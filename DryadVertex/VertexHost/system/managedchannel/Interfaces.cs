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
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.Channel
{
    public interface IDrLogging
    {
        void LogAssertion(string message, string file, string function, int line);
        void LogError(string message, string file, string function, int line);
        void LogWarning(string message, string file, string function, int line);
        void LogInformation(string message, string file, string function, int line);
    }

    internal class DryadLogger
    {
        private IDrLogging logger;

        public DryadLogger(IDrLogging l)
        {
            logger = l;
        }

        public IDrLogging Logger { get { return logger; } }

        public void LogAssert(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            logger.LogAssertion(message, file, function, line);
        }

        public void LogError(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            logger.LogError(message, file, function, line);
        }

        public void LogWarning(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            logger.LogWarning(message, file, function, line);
        }

        public void LogInformation(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            logger.LogInformation(message, file, function, line);
        }
    }

    internal class PeloponneseLogger : Microsoft.Research.Peloponnese.ILogger
    {
        private readonly IDrLogging logger;

        public PeloponneseLogger(IDrLogging l)
        {
            logger = l;
        }

        public void Log(
            string entry,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            logger.LogInformation(entry, file, function, line);
        }

        public void Stop()
        {
        }
    }

    public enum ErrorType
    {
        Open,
        IO,
        Close
    }

    internal class HttpClient
    {
        private static readonly string s_dummy;
        private static NotHttpClient s_client;

        static HttpClient()
        {
            s_dummy = "String to lock";
        }

        public static void Initialize(IDrLogging logger)
        {
            lock (s_dummy)
            {
                if (s_client == null)
                {
                    s_client = new NotHttpClient(false, 10, 30000, new PeloponneseLogger(logger));
                }
            }
        }

        public static IHttpRequest Create(string uri)
        {
            return s_client.CreateRequest(uri);
        }
    }

    public class Buffer
    {
        public long offset;
        public int size;
        public IntPtr storage;
        public IntPtr handle;
    }

    public interface IReaderClient
    {
        void UpdateTotalLength(long totalLength);
        void ReceiveData(Buffer b, bool eof);
        void SignalError(ErrorType type, string reason);
        void DiscardBuffer(Buffer b);
    }

    public interface IManagedReader
    {
        bool IsLocal { get; }
        int OutstandingBuffers { get; }
        int BufferSize { get; }
        long TotalLength { get; }

        void Start();
        void SupplyBuffer(Buffer b);
        void Interrupt();
        void WaitForDrain();
        void Close();
    }

    public interface IWriterClient
    {
        void ReturnBuffer(Buffer b, ErrorType type, string errorMessage);
    }

    public interface IManagedWriter
    {
        int BufferSize { get; }
        long BufferAlignment { get; }
        bool BreakOnRecordBoundaries { get; }

        /// <summary>
        /// open the output and start writing
        /// </summary>
        void Start();

        /// <summary>
        /// accept a buffer to be written. This can be called before Start(), in which
        /// case the buffer should be queued. (This happens when the number of open writers
        /// is being throttled.) If the buffer offset is negative, that means we should close
        /// the writer stream after all the preceding writes have been sent.
        /// </summary>
        /// <param name="buffer">The data to write</param>
        /// <returns>true if the sender should block until some buffers have been written out</returns>
        bool Write(Buffer buffer);

        /// <summary>
        /// block the calling thread until the output has been completely written and all buffers
        /// have been returned to the IWriterClient
        /// </summary>
        void WaitForClose();
    }

    public class Factory
    {
        public delegate IManagedReader ReaderFactory(Uri uri, int numberOfReaders, IReaderClient client, IDrLogging logger);
        public delegate IManagedWriter WriterFactory(Uri uri, int numberOfWriters, IWriterClient client, IDrLogging logger);

        public static Dictionary<string, ReaderFactory> readers;
        public static Dictionary<string, WriterFactory> writers;

        static Factory()
        {
            readers = new Dictionary<string,ReaderFactory>();
            writers = new Dictionary<string,WriterFactory>();

            HttpReader.Register();
            AzureReader.Register();
            AzureWriter.Register();
            //FileWriter.Register();
        }

        public static void RegisterReader(string scheme, ReaderFactory factory)
        {
            readers.Add(scheme, factory);
        }

        public static void RegisterWriter(string scheme, WriterFactory factory)
            {
            writers.Add(scheme, factory);
        }

        public static bool RecognizesReaderUri(string path)
        {
            try
            {
                var uri = new Uri(path);
                return readers.ContainsKey(uri.Scheme);
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool RecognizesWriterUri(string path)
        {
            try
            {
                var uri = new Uri(path);
                return writers.ContainsKey(uri.Scheme);
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static IManagedReader OpenReader(string path, int numberOfReaders, IReaderClient client, IDrLogging logger)
        {
            try
            {
                HttpClient.Initialize(logger);
                var uri = new Uri(path);
                return readers[uri.Scheme](uri, numberOfReaders, client, logger);
            }
            catch (Exception e)
            {
                DryadLogger l = new DryadLogger(logger);
                l.LogError("Caught exception opening reader " + e.ToString());
                return null;
            }
        }

        public static IManagedWriter OpenWriter(string path, int numberOfWriters, IWriterClient client, IDrLogging logger)
        {
            try
            {
                HttpClient.Initialize(logger);
                var uri = new Uri(path);
                return writers[uri.Scheme](uri, numberOfWriters, client, logger);
            }
            catch (Exception e)
            {
                DryadLogger l = new DryadLogger(logger);
                l.LogError("Caught exception opening writer " + e.ToString());
                return null;
            }
        }
    }
}
