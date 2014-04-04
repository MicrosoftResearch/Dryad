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
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

namespace Microsoft.Research.Dryad.ProcessService
{
    class SpillMachine
    {
        private class SpillBlock
        {
            // data bytes corresponding to an unwritten block of a spill file
            public byte[] data;
            // prefix of bytes in the array that are valid data
            public Int64 validLength;

            public SpillBlock(Int64 size)
            {
                data = new byte[size];
            }

            public SpillBlock(byte[] d)
            {
                data = d;
            }
        }

        private class SpillFile
        {
            // the amount of data that is buffered before writing out to disk. This adjusts
            // depending on memory pressure: if a lot of spill files are open for writing
            // then the buffer size is reduced.
            private Int64 writeBlockSize;

            public Int64 WriteBlockSize { get { return writeBlockSize; } }
            public Int64 BufferedSpace { get { return writeBlockSize - bytesAppended; } }

            // name of the spill file
            private string fileName;
            // handle to the spill file, which remains open until all the data have been consumed
            // at which point the file is deleted
            private FileStream handle;
            // block of data being buffered before writing to disk, or null
            private SpillBlock bufferedData;
            // completions associated with the buffered data block, or null if bufferedData is null.
            // these will be delivered once the buffered data is written to disk
            private List<Cache.IAppendCompletion> bufferedCompletions;
            // flag recording whether or not an IDF is currently appending to the spill file. Only
            // one IDF can append at a time
            bool activeWriter;
            // the count of bytes appended so far. When a new IDF is added to the spill file, this
            // count is returned to record the offset at which the IDF begins
            private Int64 bytesAppended;
            // the number of IDFs stored in the spill file that have not yet been consumed.
            // when this drops to zero the handle can be closed and the spill file can be deleted
            private int referenceCount;

            internal SpillFile(string name)
            {
                fileName = name;
                handle = new FileStream(fileName, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
            }

            // Called at the end of an asynchronous disk write, and forwards the completions
            // for every fragment whose write was completed as a part of the buffer being written.
            private void WriteComplete(Object o)
            {
                var completions = (List<Cache.IAppendCompletion>)o;
                foreach (var c in completions)
                {
                    // XXX read the manual to find out how to discover write failures
                    c.Success();
                }
            }

            // Enqueue an async write of the data currently being buffered, then zero out the buffer in
            // preparation for more appends
            private void QueueWrite()
            {
                handle.BeginWrite(bufferedData.data, 0, (int)bufferedData.validLength, WriteComplete, bufferedCompletions);
                bufferedData = null;
                bufferedCompletions = null;
            }

            // Associate an IDF to begin appending to this spill file. Only one IDF can append at a time
            Int64 OpenIDF()
            {
                // This will be the offset into the spill file where this IDF's data begins
                Int64 startingOffset;
                lock (this)
                {
                    ++referenceCount;
                    // the current append count marks the beginning of the new IDF
                    startingOffset = bytesAppended;
                    // only one IDF can be appending at a time
                    System.Diagnostics.Debug.Assert(activeWriter == false);
                    activeWriter = true;
                }

                return startingOffset;
            }

            // Indicate that the IDF currently appending to this spill file has been completely written
            void CloseIDF()
            {
                lock (this)
                {
                    System.Diagnostics.Debug.Assert(activeWriter == true);
                    activeWriter = false;
                }
            }

            // Indicate that an IDF that was written into this spill file has been completely consumed.
            // When all IDFs have been consumed, the spill file can be deleted.
            bool DiscardIDF()
            {
                // This will be set to true if it's the last IDF to be consumed
                bool lastReference = false;

                lock (this)
                {
                    --referenceCount;
                    if (referenceCount == 0)
                    {
                        // Can't run out of references if someone is still writing
                        System.Diagnostics.Debug.Assert(activeWriter == false);
                        // Garbage collect the spilled file data
                        handle.Close();
                        System.IO.File.Delete(fileName);
                        lastReference = true;
                    }
                }

                return lastReference;
            }

            // The IDF that is adding data to this spill file calls Append to add some more. Each
            // call to Append is accompanied by a completion handler that will be called once the
            // appended data has been written to disk.
            void Append(byte[] data, Cache.IAppendCompletion completion)
            {
                // This will be a count of the bytes written out of the data array
                Int64 written = 0;
                while (written < data.LongLength)
                {
                    if (bufferedData == null) // no data is currently buffered to write
                    {
                        if (written == 0 && data.LongLength == writeBlockSize)
                        {
                            // fast path, write straight through without copying the buffer.
                            // This is used when spilling large, previously-cached IDFs as fast as
                            // possible, for which the client uses the WriteBlockSize property to match
                            // the Append calls to the buffer size.
                            bufferedData = new SpillBlock(data);
                            written = data.LongLength;
                            bufferedData.validLength = data.LongLength;
                        }
                        else
                        {
                            // make a new buffer for writing
                            bufferedData = new SpillBlock(writeBlockSize);
                        }

                        bufferedCompletions = new List<Cache.IAppendCompletion>();
                    }

                    // Copy some data into the buffer
                    Int64 space = bufferedData.data.LongLength - bufferedData.validLength;
                    Int64 needed = data.LongLength - written;
                    Int64 toWrite = Math.Max(needed, space);
                    Array.Copy(data, written, bufferedData.data, bufferedData.validLength, toWrite);
                    written += toWrite;
                    bufferedData.validLength += toWrite;

                    if (bufferedData.validLength == bufferedData.data.LongLength)
                    { // the buffer is full---time to write it to disk
                        if (written == data.LongLength)
                        {
                            // this happens to coincide with finishing the append, so attach
                            // the completion. Otherwise the completion will be attached to a
                            // subsequent buffer, so it doesn't get called until the entire 
                            // append block has been written out
                            bufferedCompletions.Add(completion);
                        }

                        // Send bufferedData to the disk, and reset it to null
                        QueueWrite();
                    }
                }

                // We have written out all the data now
                if (bufferedCompletions != null)
                {
                    // The buffer wasn't sent out at the end of the loop, so add
                    // our completion to the partially-written buffer, waiting for
                    // it to get written our in a subsequent Append or Flush.
                    bufferedCompletions.Add(completion);
                }

                // Record the amount of data we appended in this call
                bytesAppended += data.LongLength;
            }

            // Write out buffered data if any, then call Success on completion
            void Flush(Cache.IAppendCompletion completion)
            {
                if (bufferedData == null) // no buffered data being held
                {
                    completion.Success();
                }
                else
                {
                    // the completion will be called once the data has been
                    // written to disk
                    bufferedCompletions.Add(completion);
                    QueueWrite();
                }
            }
        }

        private HashSet<SpillFile> availableFiles;
        private HashSet<SpillFile> activeFiles;
        private HashSet<SpillFile> completeFiles;
        private string filePrefix;
        private int fileCounter;

        private string MintFileName()
        {
            int thisFile;
            lock (this)
            {
                thisFile = fileCounter;
                ++fileCounter;
            }

            return filePrefix + "-" + thisFile + ".idf";
        }

        SpillFile AcquireFile(Int64 sizeNeeded)
        {
            SpillFile f;

            if (sizeNeeded < 0)
            {
                f = new SpillFile(MintFileName());
            }
            else
            {
                lock (availableFiles)
                {
                    var fitFiles = availableFiles.Where(x => x.BufferedSpace >= sizeNeeded);
                    // XXX
                    f = fitFiles.First();
                }
            }

            return f;
        }
    }
}
