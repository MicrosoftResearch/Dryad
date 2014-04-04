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

namespace Microsoft.Research.Dryad.Channel
{
    internal abstract class StreamReader : IManagedReader
    {
        public struct ReadData
        {
            public bool eof;
            public int nRead;
        }

        private int SafetyTimeout = 10 * 60 * 1000; // 10 minute timeout on reads just in case

        private DryadLogger log;
        private int bufferSize;
        private long length;
        private long offset;
        private bool interrupted;
        private Task finished;
        private BufferQueue queue;
        private IReaderClient client;

        public StreamReader(int bSize, IReaderClient c, IDrLogging logger)
        {
            client = c;
            log = new DryadLogger(logger);
            bufferSize = bSize;
        }

        /// <summary>
        /// true if the stream is being read from a source on the same computer
        /// </summary>
        public abstract bool IsLocal { get; }

        /// <summary>
        /// the Uri we are reading from
        /// </summary>
        public abstract Uri Source { get; }

        /// <summary>
        /// overlap buffer reads with consumption at the client side
        /// </summary>
        public int OutstandingBuffers
        {
            get { return 4; }
        }

        /// <summary>
        /// the fixed buffer size we are using for reads
        /// </summary>
        public int BufferSize
        {
            get { return bufferSize; }
        }

        /// <summary>
        /// the number of bytes in the entire stream, or -1 if the length is not known
        /// </summary>
        public long TotalLength
        {
            get { return length; }
            protected set {
                length = value;
                client.UpdateTotalLength(length);
            }
        }

        /// <summary>
        /// the logging interface
        /// </summary>
        protected DryadLogger Log { get { return log; } }

        /// <summary>
        /// the next offset to read from
        /// </summary>
        protected long Offset { get { return offset; } }

        /// <summary>
        /// true if the stream length is known
        /// </summary>
        protected bool KnownLength
        {
            get { return (length >= 0); }
        }

        /// <summary>
        /// number of bytes remaining to read from the stream. Throws an exception
        /// if the total stream length is unknown
        /// </summary>
        protected long RemainingLength
        {
            get {
                if (!KnownLength)
                {
                    throw new ApplicationException("Unknown length");
                }
                return length - offset;
            }
        }

        /// <summary>
        /// number of bytes to request in the next block read: this is the buffer size unless
        /// we know there are fewer bytes remaining
        /// </summary>
        protected int BytesToRead
        {
            get { return (KnownLength) ? (int)Math.Min(RemainingLength, (long)bufferSize) : bufferSize; }
        }

        public void Start()
        {
            length = -1;
            offset = 0;
            interrupted = false;
            queue = new BufferQueue();
            log.LogInformation("Starting read for " + Source.AbsoluteUri);
            finished = Task.Run(async () => await Worker());
        }

        /// <summary>
        /// fill a buffer with the next data to read from the stream
        /// </summary>
        /// <param name="managedBuffer"></param>
        /// <returns></returns>
        protected abstract Task<ReadData> ReadBuffer(byte[] managedBuffer);

        protected abstract string DescribeException(Exception e);

        private void SendError(Exception e, ErrorType type)
        {
            try
            {
                string message = DescribeException(e);

                if (message == null)
                {
                    message = e.ToString();
                }

                client.SignalError(type, message);
            }
            catch (Exception ee)
            {
                client.SignalError(type, ee.ToString());
            }
        }

        protected abstract Task Open();
        protected abstract Task OnFinishedRead();

        private async Task Worker()
        {
            bool errorState = false;

            try
            {
                await Open();
            }
            catch (Exception e)
            {
                SendError(e, ErrorType.Open);
                errorState = true;
            }
            await DataLoop(errorState);

            List<Buffer> discard;
            lock (this)
            {
                discard = queue.Shutdown();
                interrupted = true;
                queue = null;
            }

            foreach (var buffer in discard.Where(b => b != null))
            {
                client.DiscardBuffer(buffer);
            }

            await OnFinishedRead();
        }

        private async Task DataLoop(bool errorState)
        {
            var managedBuffer = new byte[bufferSize];
            var readData = new ReadData { eof = false, nRead = 0 };

            while ((!readData.eof) || errorState)
            {
                log.LogInformation("Waiting for buffer");
                var buffer = await queue.Dequeue();
                if (buffer == null)
                {
                    // we were interrupted, and have returned all the necessary buffers
                    return;
                }

                if (errorState)
                {
                    client.DiscardBuffer(buffer);
                }
                else
                {
                    try
                    {
                        if (buffer.size != bufferSize)
                        {
                            throw new ApplicationException("Mismatched buffer sizes " + buffer.size + " != " + bufferSize);
                        }

                        if (buffer.offset == -1)
                        {
                            buffer.offset = offset;
                        }
                        else if (buffer.offset != offset)
                        {
                            throw new ApplicationException("Buffer offset " + buffer.offset + " expected " + offset);
                        }

                        log.LogInformation("Waiting for buffer read");

                        Task<ReadData> timeout = Task.Delay(SafetyTimeout).ContinueWith((t) => new ReadData());
                        Task<ReadData> reads = await Task.WhenAny(timeout, ReadBuffer(managedBuffer));
                        if (reads == timeout)
                        {
                            throw new ApplicationException("Excessive timeout on read operation");
                        }
                        readData = reads.Result;

                        log.LogInformation("Got buffer read " + readData.nRead);

                        buffer.size = readData.nRead;
                        Marshal.Copy(managedBuffer, 0, buffer.storage, readData.nRead);

                        log.LogInformation("Returning to client");

                        client.ReceiveData(buffer, readData.eof);

                        offset += (long) buffer.size;
                    }
                    catch (Exception e)
                    {
                        SendError(e, ErrorType.IO);
                        client.DiscardBuffer(buffer);
                        errorState = true;
                    }
                }
            }
        }

        public void SupplyBuffer(Buffer b)
        {
            lock (this)
            {
                if (interrupted)
                {
                    log.LogAssert("Got supplied buffer after being interrupted");
                }
                queue.Enqueue(b);
            }
        }

        public void Interrupt()
        {
            lock (this)
            {
                if (!interrupted)
                {
                    interrupted = true;
                    queue.Enqueue(null);
                }
            }
        }

        public void WaitForDrain()
        {
            try
            {
                finished.Wait();
            }
            catch (Exception e)
            {
                log.LogError("Caught exception from reader " + e.ToString());
            }

            log.LogInformation("Reader " + this.Source.AbsoluteUri + " finished");

            finished = null;
        }

        public void Close()
        {
            if (finished != null)
            {
                log.LogError("Close called before drain completed");
                throw new ApplicationException("Close called before drain completed");
            }
        }
    }
}
