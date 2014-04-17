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
using System.Text;
using System.IO;

namespace Microsoft.Research.DryadLinq.Internal
{
    // The client-log is a file produced in the current working directly
    // (next to generated .cs/.dll and queryPlan.xml)
    internal static class DryadLinqClientLog
    {
        internal const string CLIENT_LOG_FILENAME = "DryadLinqClient.log";

        public static bool IsOn = true;  // whether to log or not.
        private static bool s_IOErrorOccurred = false;
        private static TextWriter s_writer;

        static DryadLinqClientLog()
        {
            string path = DryadLinqCodeGen.GetPathForGeneratedFile(CLIENT_LOG_FILENAME, null);
            s_writer = new StreamWriter(path);
        }

        public static void Add(string msg)
        {
            Add(msg, null);
        }

        public static void Add(string msg, params object[] args)
        {
            if (!IsOn || s_IOErrorOccurred) return;

            try
            {
                if (args == null)
                {
                    s_writer.WriteLine(msg);
                }
                else
                {
                    s_writer.WriteLine(msg, args);
                }
                s_writer.Flush();
            }
            catch (IOException)
            {
                s_IOErrorOccurred = true;
                try
                {
                    s_writer.Close();
                    s_writer.Dispose();
                    s_writer = null;
                }
                catch
                {
                    // supress exceptions that occur during cleanup.
                }
                return;
            }
            catch (System.ObjectDisposedException)
            {
                s_IOErrorOccurred = true;
                try
                {
                    s_writer.Close();
                    s_writer.Dispose();
                    s_writer = null;
                }
                catch
                {
                    // supress exceptions that occur during cleanup.
                }
                return;
            }
        }
    }
}
    
namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// DryadLINQ logging API.
    /// </summary>
    public static class DryadLinqLog
    {
        /// <summary>
        /// Gets and sets the logging level of DryadLINQ.
        /// </summary>
        public static int Level = Constants.TraceErrorLevel;
        private static TextWriter s_writer = Console.Out;

        internal static void Initialize(int logLevel, string filePath)
        {
            Level = logLevel;
            if (filePath != null)
            {
                TextWriter temp = new StreamWriter(filePath, true);
                s_writer = TextWriter.Synchronized(temp);
            }
        }

        private static void Add(string prefix, int logLevel, string msg, params object[] args)
        {
            if (logLevel <= Level)
            {
                try
                {
                    s_writer.WriteLine(prefix + msg, args);
                    s_writer.Flush();
                }
                catch (ObjectDisposedException)
                {
                    // we're in a shutdown scenario, writing the log triggers an error but it's ok, 
                    // we can ignore it. Let's do the next best thing instead: write to the console.
                    Console.WriteLine(prefix + msg, args);
                }
            }
        }

        /// <summary>
        /// Adds a critical-level log entries.
        /// </summary>
        /// <param name="msg">The log message as a format string</param>
        /// <param name="args">The objects to format</param>
        public static void AddCritical(string msg, params object[] args)
        {
            Add("Critical: ", Constants.TraceCriticalLevel, msg, args);
        }

        /// <summary>
        /// Adds an error-level log entries.
        /// </summary>
        /// <param name="msg">The log message as a format string</param>
        /// <param name="args">The objects to format</param>
        public static void AddError(string msg, params object[] args)
        {
            Add("Error: ", Constants.TraceErrorLevel, msg, args);
        }

        /// <summary>
        /// Adds a warning-level log entries.
        /// </summary>
        /// <param name="msg">The log message as a format string</param>
        /// <param name="args">The objects to format</param>
        public static void AddWarning(string msg, params object[] args)
        {
            Add("Warning: ", Constants.TraceWarningLevel, msg, args);
        }

        /// <summary>
        /// Adds an information-level log entries.
        /// </summary>
        /// <param name="msg">The log message as a format string</param>
        /// <param name="args">The objects to format</param>
        public static void AddInfo(string msg, params object[] args)
        {
            Add("Info: ", Constants.TraceInfoLevel, msg, args);
        }

        /// <summary>
        /// Adds a verbose-level log entries.
        /// </summary>
        /// <param name="msg">The log message as a format string</param>
        /// <param name="args">The objects to format</param>
        public static void AddVerbose(string msg, params object[] args)
        {
            Add("Verbose: ", Constants.TraceVerboseLevel, msg, args);
        }  
    }
}
