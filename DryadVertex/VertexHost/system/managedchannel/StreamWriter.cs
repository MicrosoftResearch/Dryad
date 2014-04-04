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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Dryad.Channel
{
    internal abstract class StreamWriter : IManagedWriter
    {
        private int SafetyTimeout = 10 * 60 * 1000; // 10 minute timeout on reads just in case

        private DryadLogger log;
        private IWriterClient client;

        private ErrorType errorType;
        private string error;

        private BufferQueue writes;
        private long offset;

        private Task finished;

        public StreamWriter(IWriterClient c, IDrLogging l)
        {
            client = c;
            log = new DryadLogger(l);

            writes = new BufferQueue();
            offset = 0;

            finished = new Task(() => { });

            error = null;
            errorType = ErrorType.IO;
        }

        /// <summary>
        /// the logging interface
        /// </summary>
        protected DryadLogger Log { get { return log; } }

        abstract public int BufferSize { get; }

        abstract public long BufferAlignment { get; }

        abstract public bool BreakOnRecordBoundaries { get; }

        abstract public Task<string> Open();

        public async void Start()
        {
            try
            {
                error = await Open();
                if (error != null)
                {
                    errorType = ErrorType.Open;
                }
            }
            catch (Exception e)
            {
                log.LogError("Got exception opening stream " + e.ToString());
                error = e.Message;
                errorType = ErrorType.Open;
            }

            log.LogInformation("About to await writes");

            await WriteAll();

            log.LogInformation("Writes done: setting finish");

            finished.RunSynchronously();
        }

        public bool Write(Buffer buffer)
        {
            lock (this)
            {
                if (writes == null)
                {
                    log.LogError("Got write buffer after close");
                    throw new ApplicationException("Got write buffer after close");
                }

                int buffersInQueue = writes.Enqueue(buffer);
                return (buffersInQueue > 4);
            }
        }

        private async Task CloseInternal()
        {
            lock (this)
            {
                List<Buffer> remaining = writes.Shutdown();
                if (remaining.Count > 0)
                {
                    throw new ApplicationException("Got end write buffer with buffers still in the queue");
                }
                writes = null;
            }

            log.LogInformation("Closing write stream");

            try
            {
                error = await Close();
                if (error != null)
                {
                    errorType = ErrorType.Close;
                }
            }
            catch (Exception e)
            {
                log.LogError("Got exception closing stream " + e.ToString());
                error = e.ToString();
                errorType = ErrorType.Close;
            }
        }

        private async Task WriteInternal(Buffer buffer)
        {
            if (buffer.offset != offset)
            {
                throw new ApplicationException("Expected offset " + offset + " got " + buffer.offset);
            }

            byte[] managedBuffer = new byte[buffer.size];
            Marshal.Copy(buffer.storage, managedBuffer, 0, buffer.size);

            log.LogInformation("Copied write buffer");

            try
            {
                Task<string> timeout = Task.Delay(SafetyTimeout).ContinueWith((t) => "");
                Task<string> writes = await Task.WhenAny(timeout, WriteBuffer(managedBuffer));
                if (writes == timeout)
                {
                    throw new ApplicationException("Excessive timeout on read operation");
                }
                error = writes.Result;
            }
            catch (Exception e)
            {
                log.LogError("Got file write exception " + e.ToString());
                error = e.Message;
            }

            offset += buffer.size;
        }

        private async Task WriteAll()
        {
            Buffer buffer;

            do
            {
                buffer = await writes.Dequeue();

                log.LogInformation("Got buffer");

                if (error == null)
                {
                    try
                    {
                        if (buffer.offset < 0)
                        {
                            log.LogInformation("Got close buffer");
                            await CloseInternal();
                        }
                        else
                        {
                            log.LogInformation("Got write buffer");
                            await WriteInternal(buffer);
                        }
                    }
                    catch (Exception e)
                    {
                        log.LogError("Got exception writing stream " + e.ToString());
                        error = e.Message;
                        errorType = ErrorType.IO;
                    }
                }

                client.ReturnBuffer(buffer, errorType, error);
            } while (buffer.offset >= 0);

            log.LogInformation("Finished all writes");
        }

        abstract public Task<string> WriteBuffer(byte[] buffer);

        abstract public Task<string> Close();

        public void WaitForClose()
        {
            log.LogInformation("Waiting for writes to drain");
            finished.Wait();
            log.LogInformation("Finished waiting for writes to drain");
        }
    }
}
