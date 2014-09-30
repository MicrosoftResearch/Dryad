/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Azure;

namespace Microsoft.Research.Dryad.Channel
{
    internal class AzureReader : StreamReader
    {
        private static TimeSpan ExecutionTimeout = new TimeSpan(0, 0, 30); // 30 seconds before retrying
        private AzureDfsClient client;
        private Stream readStream;
        private long bytesToRead;
        private Uri source;
        private int blobIndex;

        public AzureReader(Uri path, int numberOfReaders, IReaderClient client, IDrLogging logger)
            : base(ComputeBufferSize(numberOfReaders), client, logger)
        {
            source = path;
        }

        private static int ComputeBufferSize(int numberOfReaders)
        {
            // for now set the buffer size to 4MB since that gives decent performance
            // with 4MB blocks
            return AzureConsts.MaxBlockSize4MB;
            /*
            int blockSize = 4 * 1024;
            int numberOfBlocksPerBuffer = 64 / numberOfReaders;
            if (numberOfBlocksPerBuffer < 16)
            {
                numberOfBlocksPerBuffer = 16;
            }
            return blockSize * numberOfBlocksPerBuffer;
            */
        }

        public override Uri Source { get { return source; } }
        public override bool IsLocal { get { return false; } }

        protected override string DescribeException(Exception e)
        {
            return null;
        }

        protected override async Task Open()
        {
            Log.LogInformation("Opening read for " + source.AbsoluteUri);
            string account, key, container, blobName;
            Utils.FromAzureUri(source, out account, out key, out container, out blobName);
            client = new AzureDfsClient(account, key, container, false, new PeloponneseLogger(Log.Logger));
            client.SetParallelThreadCount(4);

            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(source.Query);
            if (query["blobs"] == null)
            {
                blobIndex = -1;
            }
            else
            {
                blobIndex = 0;
            }

            await OpenBlob();
        }

        private async Task OpenBlob()
        {
            string account, key, container, blobName;
            Utils.FromAzureUri(source, out account, out key, out container, out blobName);

            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(source.Query);

            if (blobIndex >= 0)
            {
                string[] blobs = query["blobs"].Split(',');
                blobName += blobs[blobIndex];
                ++blobIndex;
            }

            Uri blobUri = Utils.ToAzureUri(account, container, blobName, null, key);

            Log.LogInformation("Opening read for blob " + blobUri.AbsoluteUri);
            
            readStream = (await client.GetDfsStreamReaderAsync(blobUri, ExecutionTimeout, new PeloponneseLogger(Log.Logger)));

            long offset = -1;
            if (query["offset"] != null)
            {
                offset = Int64.Parse(query["offset"]);
                readStream.Seek(offset, SeekOrigin.Begin);
            }

            bytesToRead = Int64.MaxValue;
            if (query["length"] != null)
            {
                bytesToRead = Int64.Parse(query["length"]);
            }

            if (query["seekboundaries"] != null)
            {
                if (offset == -1 || bytesToRead == Int64.MaxValue)
                {
                    throw new ApplicationException("Reading " + source.AbsoluteUri + ": Can't look for line endings without block start and end metadata");
                }

                if (query["seekboundaries"] != "Microsoft.Research.DryadLinq.LineRecord")
                {
                    throw new ApplicationException("Reading " + source.AbsoluteUri + ": Don't know how to seek for record boundaries of type " + query["seekboundaries"]);
                }

                // SeekLineRecordBoundaries updates bytesToRead
                offset = await SeekLineRecordBoundaries(offset);
                readStream.Seek(offset, SeekOrigin.Begin);
            }

            long thisLength;
            if (bytesToRead == Int64.MaxValue)
            {
                thisLength = readStream.Length;
            }
            else
            {
                thisLength = bytesToRead;
            }

            long currentLength = TotalLength;
            if (currentLength == -1)
            {
                currentLength = thisLength;
                Log.LogInformation("Setting Azure read total length to " + currentLength);
            }
            else
            {
                currentLength += thisLength;
                Log.LogInformation("Increasing Azure read total length to " + thisLength);
            }

            TotalLength = currentLength;
        }

        private int NumberOfBlobs
        {
            get
            {
                NameValueCollection query = System.Web.HttpUtility.ParseQueryString(source.Query);
                return query["blobs"].Split(',').Length;
            }
        }

        protected override async Task<ReadData> ReadBuffer(byte[] managedBuffer)
        {
            while (true)
            {
                ReadData readData = await ReadBufferInternal(managedBuffer);

                if (readData.eof && blobIndex >= 0 && blobIndex < NumberOfBlobs)
                {
                    // this is a multi-blob read; open the next one
                    await OpenBlob();

                    if (readData.nRead > 0)
                    {
                        readData.eof = false;
                        // we read some data; return it now
                        return readData;
                    }

                    // otherwise go around the loop again and read some data from the next blob
                }
                else
                {
                    return readData;
                }
            }
        }

        private async Task<ReadData> ReadBufferInternal(byte[] managedBuffer)
        {
            ReadData readData;

            if (bytesToRead == 0)
            {
                // we knew on open that there was nothing to read
                readData.nRead = 0;
                readData.eof = true;
                return readData;
            }

            int toRead = (int)Math.Min((long)managedBuffer.Length, bytesToRead);

            Log.LogInformation("About to read buffer length " + toRead);
            readData.nRead = await Utils.WrapInRetry(new PeloponneseLogger(Log.Logger), async () =>
                {
                    return await readStream.ReadAsync(managedBuffer, 0, toRead);
                });

            if (bytesToRead == Int64.MaxValue)
            {
                readData.eof = (readData.nRead < toRead);
            }
            else
            {
                if (readData.nRead == 0)
                {
                    throw new ApplicationException("Reader " + source.AbsoluteUri + " ran out of data with " + bytesToRead + " bytes remaining to read");
                }

                bytesToRead -= readData.nRead;
                readData.eof = (bytesToRead == 0);
            }

            Log.LogInformation("Read buffer length " + toRead + " got " + readData.nRead + " eof " + readData.eof);

            return readData;
        }

        protected override Task OnFinishedRead()
        {
            Log.LogInformation("Finished reading " + source.AbsoluteUri);
            return Task.FromResult(true);
        }

        private async Task<long> SeekLineRecordBoundaries(long offset)
        {
            long startOfNextBlock = offset + bytesToRead;

            if (offset != 0)
            {
                // we aren't reading the first block, so the previous reader is going to take care of the partial
                // line that begins at this block. We'll look for the start of the next line
                offset = await SeekStartOfLine(offset, startOfNextBlock);
                if (offset > startOfNextBlock)
                {
                    // We looked as far as the first character in the next block and didn't find the start of a
                    // new line. That means the preceding block reader is going to handle our entire block, so
                    // we don't need to do anything. By setting bytesToRead to 0, we ensure that the first read
                    // will return eof as desired
                    bytesToRead = 0;
                    return 0;
                }
            }

            // determine the first line that the next block reader is going to take care of; keep looking all the
            // way to the end of the stream if need be
            long startOfFirstLineAfterBlock = await SeekStartOfLine(startOfNextBlock, Int64.MaxValue);

            if (startOfFirstLineAfterBlock < offset)
            {
                throw new ApplicationException("Line boundary logic bug");
            }

            bytesToRead = startOfFirstLineAfterBlock - offset;
            return offset;
        }

        private async Task<long> SeekStartOfLine(long currentOffset, long startOfNextBlock)
        {
            byte[] buffer = new byte[4 * 1024];

            readStream.Seek(currentOffset, SeekOrigin.Begin);

            long endOffset;
            if (startOfNextBlock == Int64.MaxValue)
            {
                endOffset = Int64.MaxValue;
            }
            else
            {
                // we may need to look as far as the first character in the next block
                endOffset = startOfNextBlock + 1;
            }

            bool foundReturn = false;
            while (currentOffset < endOffset)
            {
                int toRead = (int)Math.Min(buffer.LongLength, endOffset - currentOffset);

                int nRead = await Utils.WrapInRetry(new PeloponneseLogger(Log.Logger), async () =>
                    {
                        return await readStream.ReadAsync(buffer, 0, toRead);
                    });
                if (nRead == 0)
                {
                    // we hit end of stream
                    return currentOffset;
                }

                for (int i = 0; i < nRead; ++i)
                {
                    if (buffer[i] == '\n')
                    {
                        // the next character is the first character of a line
                        return currentOffset + i + 1;
                    }
                    else if (foundReturn)
                    {
                        // there was a '\r' that wasn't followed by a '\n', i.e. this is
                        // the first character of a line
                        return currentOffset + i;
                    }
                    else if (buffer[i] == '\r')
                    {
                        // just flag this for now; if the next character is '\n' the next line
                        // starts after that, otherwise the next line starts with the next character
                        foundReturn = true;
                    }
                }

                currentOffset += nRead;
            }

            // the next line starts at some character >= startOfNextBlock+1, so it's not our problem
            return currentOffset;
        }

        private static IManagedReader OpenAzureReader(Uri uri, int numberOfReaders, IReaderClient client, IDrLogging logger)
        {
            return new AzureReader(uri, numberOfReaders, client, logger);
        }

        public static void Register()
        {
            Factory.RegisterReader("azureblob", OpenAzureReader);
        }
    }
}
