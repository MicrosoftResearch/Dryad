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
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.Channel
{
    internal class HttpReader : StreamReader
    {
        private Uri source;

        public HttpReader(Uri path, int numberOfReaders, IReaderClient client, IDrLogging logger)
            : base(ComputeBufferSize(numberOfReaders), client, logger)
        {
            source = path;
        }

        private static int ComputeBufferSize(int numberOfReaders)
        {
            int blockSize = 4 * 1024;
            int numberOfBlocksPerBuffer = 64 / numberOfReaders;
            if (numberOfBlocksPerBuffer < 16)
            {
                numberOfBlocksPerBuffer = 16;
            }
            return blockSize * numberOfBlocksPerBuffer;
        }

        public override Uri Source { get { return source; } }
        public override bool IsLocal { get { return false; } }

        protected override string DescribeException(Exception e)
        {
            if (e is NotHttpException)
            {
                var nhe = e as NotHttpException;
                IHttpResponse response = nhe.Response;
                if (response != null && response.StatusCode == HttpStatusCode.NotFound)
                {
                    return response.StatusDescription;
                }
            }

            return null;
        }

        protected override Task Open()
        {
            Log.LogInformation("Opening read for " + source.AbsoluteUri);
            return Task.FromResult(true);
        }

        protected override async Task<ReadData> ReadBuffer(byte[] managedBuffer)
        {
            Log.LogInformation("Requesting read from " + source.AbsoluteUri + " " + Offset + ":" + BytesToRead);

            string requestString = String.Format("{0}?offset={1}&length={2}", source.AbsoluteUri, Offset, BytesToRead);
            IHttpRequest request = HttpClient.Create(requestString);
            // don't need a timeout on this request since there's a timeout wrapping the entire operation

            ReadData readData;

            using (IHttpResponse response = await request.GetResponseAsync())
            {
                string lengthHeader = response.Headers["X-Dryad-StreamTotalLength"];
                if (lengthHeader != null)
                {
                    long newLength = long.Parse(lengthHeader);
                    if (TotalLength != newLength)
                    {
                        TotalLength = newLength;
                        Log.LogInformation("Got new total length " + newLength + " from " + source.AbsoluteUri);
                    }
                }

                readData.eof = (response.Headers["X-Dryad-StreamEof"] != null);

                using (Stream payload = response.GetResponseStream())
                {
                    readData.nRead = await payload.ReadAsync(managedBuffer, 0, BytesToRead);
                }

                if (!readData.eof && readData.nRead == 0)
                {
                    throw new ApplicationException("Expected " + BytesToRead + " bytes but was not able to read any.");
                }
            }

            return readData;
        }

        protected override Task OnFinishedRead()
        {
            Log.LogInformation("Finished reading " + source.AbsoluteUri);
            return Task.FromResult(true);
        }

        private static IManagedReader OpenHttpReader(Uri uri, int numberOfReaders, IReaderClient client, IDrLogging logger)
        {
            return new HttpReader(uri, numberOfReaders, client, logger);
        }

        public static void Register()
        {
            Factory.RegisterReader("http", OpenHttpReader);
        }
    }
}
