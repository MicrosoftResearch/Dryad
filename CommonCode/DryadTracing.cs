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
using System.Diagnostics;
using System.Diagnostics.Eventing;
using System.IO;
using Microsoft.Win32;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Security.Permissions;
using System.Threading;

namespace Microsoft.Research.Dryad
{
    internal struct DryadEventDescriptor
    {
        private int m_level;
        private string m_message;

        public int Level
        {
            get { return m_level; }
        }

        public string Message
        {
            get { return m_message; }
        }

        public DryadEventDescriptor(int level, string message)
        {
            m_level = level;
            m_message = message;
        }
    }

    internal sealed class DISCTextProvider : IDisposable
    {
        private bool m_disposed = false;
        private uint m_rolloverCount = 0;
        private static readonly long m_maxFileBytes = 10 * 1024 * 1024;
        private object m_lock = new object();
        private static readonly string m_logFileFormat = "{0}{1:D6}{2}";
        private string m_baseFilePath = String.Empty;
        private StreamWriter m_provider = null;
        private Timer m_flushTimer = null;
        private static readonly int m_flushTimeout = 100;

        public DISCTextProvider(string path)
        {
            m_baseFilePath = path;
            OpenLogFile();
            m_flushTimer = new Timer(new TimerCallback(this.FlushLogTimer), null, 1000, 1000);
        }

        private void FlushLogTimer(Object state)
        {
            try
            {
                Flush();
            }
            catch
            {
            }
        }

        // Must be called while holding m_lock
        private bool ArchiveLogFile()
        {
            string archivePath = String.Format(m_logFileFormat, Path.Combine(Path.GetDirectoryName(m_baseFilePath),Path.GetFileNameWithoutExtension(m_baseFilePath)), m_rolloverCount, Path.GetExtension(m_baseFilePath));

            for (int i = 0; i < 3; i++)
            {
                try
                {
                    File.Move(m_baseFilePath, archivePath);
                    return true;
                }
                catch
                {
                }
            }

            return false;
        }

        // Must be called while holding m_lock
        private void CloseLogFile()
        {
            if (m_provider != null)
            {
                // Close the current log file
                try
                {
                    Flush();

                    // Dispose
                    m_provider.Dispose();
                    m_provider = null;
                }
                catch
                {
                    return;
                }
            }
        }

        private void OpenLogFile()
        {
            try
            {
                // Open log file
                m_provider = new StreamWriter(new FileStream(m_baseFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));
            }
            catch
            {
            }
        }

        private void ReopenLogFile()
        {
            if (m_provider != null)
            {
                // Close the current log file
                CloseLogFile();

                ArchiveLogFile();
            }

            OpenLogFile();

        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                if (disposing)
                {
                    lock (m_lock)
                    {
                        if (m_flushTimer != null)
                        {
                            m_flushTimer.Dispose();
                            m_flushTimer = null;
                        }

                        if (m_provider != null)
                        {
                            CloseLogFile();
                            m_provider = null;
                        }
                    }
                }
                m_disposed = true;
            }
        }

        public void WriteEvent(ref DryadEventDescriptor dryadEventDescriptor, string ModuleName, string Source, DateTime TimeStamp, string OperationContext, string Message)
        {
            try
            {
                StringBuilder newMessage = new StringBuilder();
                if (!String.IsNullOrEmpty(OperationContext))
                {
                    newMessage.Append(OperationContext);
                    newMessage.Append(": ");
                }
                newMessage.Append(Message);

                WriteEvent(ref dryadEventDescriptor, ModuleName, Source, TimeStamp, newMessage.ToString());
            }
            catch
            {
            }
        }

        public void WriteEvent(ref DryadEventDescriptor dryadEventDescriptor, string ModuleName, string Source, DateTime TimeStamp, string OperationContext, string MessageFormat, params object[] MessageParameters)
        {
            try
            {
                StringBuilder newMessage = new StringBuilder();
                if (!String.IsNullOrEmpty(OperationContext))
                {
                    newMessage.Append(OperationContext);
                    newMessage.Append(": ");
                }

                newMessage.Append(String.Format(MessageFormat, MessageParameters));

                WriteEvent(ref dryadEventDescriptor, ModuleName, Source, TimeStamp, newMessage.ToString());
            }
            catch
            {
            }

        }

        public void WriteEvent(ref DryadEventDescriptor dryadEventDescriptor, string ModuleName, string Source, DateTime TimeStamp, string Message)
        {
            try
            {
                lock (m_lock)
                {
                    if (m_provider != null)
                    {
                        m_provider.WriteLine(
                            String.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\"",
                            dryadEventDescriptor.Message,
                            TimeStamp.ToString("yyyy/MM/dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture),
                            System.Diagnostics.Process.GetCurrentProcess().Id,
                            System.Threading.Thread.CurrentThread.ManagedThreadId,
                            ModuleName,
                            Source,
                            String.IsNullOrEmpty(Message) ? String.Empty : Message
                            ));

                        if (m_provider.BaseStream.Position > m_maxFileBytes)
                        {
                            m_rolloverCount++;
                            ReopenLogFile();
                        }
                    }
                }
            }
            catch
            {
            }
        }

        public void WriteEvent(ref DryadEventDescriptor dryadEventDescriptor, string ModuleName, string Source, DateTime TimeStamp, Exception Exception, string Message)
        {
            try
            {
                lock (m_lock)
                {
                    if (m_provider != null)
                    {
                        m_provider.WriteLine(
                            String.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\"",
                            dryadEventDescriptor.Message,
                            TimeStamp.ToString("yyyy/MM/dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture),
                            System.Diagnostics.Process.GetCurrentProcess().Id,
                            System.Threading.Thread.CurrentThread.ManagedThreadId,
                            ModuleName,
                            Source,
                            String.IsNullOrEmpty(Message) ? String.Empty : Message,
                            (Exception == null) ? String.Empty : Exception.ToString()
                            ));

                        if (m_provider.BaseStream.Position > m_maxFileBytes)
                        {
                            m_rolloverCount++;
                            ReopenLogFile();
                        }
                    }
                }
            }
            catch
            {
            }
        }

        public void WriteEvent(ref DryadEventDescriptor dryadEventDescriptor, string ModuleName, string Source, DateTime TimeStamp, Exception Exception, string MessageFormat, params object[] MessageParameters)
        {
            try
            {
                WriteEvent(ref dryadEventDescriptor, ModuleName, Source, TimeStamp, Exception, String.Format(MessageFormat, MessageParameters));
            }
            catch
            {
            }
        }

        public void Flush()
        {
            try
            {
                if (Monitor.TryEnter(m_lock, m_flushTimeout))
                {
                    try
                    {
                        if (m_provider != null)
                        {
                            m_provider.Flush();
                        }
                    }
                    catch
                    {
                    }
                    finally
                    {
                        Monitor.Exit(m_lock);
                    }
                }
            }
            catch
            {
            }
        }

    }

    public sealed class DryadLogger
    {
        private static DISCTextProvider s_discTracer;
        // TODO: Change the default once there is better support for setting per job
        private static int s_traceLevel = Constants.traceVerboseNum;
        private static object s_syncRoot = new object();
        private static bool s_initialized = false;

        private static DryadEventDescriptor DryadMethodEntry = new DryadEventDescriptor(Constants.traceVerboseNum, "MethodEntry");
        private static DryadEventDescriptor DryadMethodExit = new DryadEventDescriptor(Constants.traceVerboseNum, "MethodExit");
        private static DryadEventDescriptor DryadError = new DryadEventDescriptor(Constants.traceErrorNum, "Error");
        private static DryadEventDescriptor DryadCritical = new DryadEventDescriptor(Constants.traceErrorNum, "Critical");
        private static DryadEventDescriptor DryadWarning = new DryadEventDescriptor(Constants.traceWarningNum, "Warning");
        private static DryadEventDescriptor DryadInformational = new DryadEventDescriptor(Constants.traceInfoNum, "Info");
        private static DryadEventDescriptor DryadVerbose = new DryadEventDescriptor(Constants.traceVerboseNum, "Verbose");


        private DryadLogger()
        {
        }

        static DryadLogger()
        {
            try
            {
                string debugLevel = Environment.GetEnvironmentVariable(Constants.traceLevelEnvVar);

                if (!String.IsNullOrEmpty(debugLevel))
                {
                    s_traceLevel = HpcQueryUtility.GetTraceLevelFromString(debugLevel);
                }

                Console.Out.WriteLine("Trace level set to {0}", HpcQueryUtility.ConvertTraceLevelToString(s_traceLevel));
            }

            catch (Exception e)
            {
                Console.Error.WriteLine("Failed to get tracing level: {0}", e);
            }
        }

        public static int TraceLevel
        {
            get { return s_traceLevel; }
            set { s_traceLevel = value; }
        }

        public static bool Start(string path)
        {
            try
            {
                if (!s_initialized)
                {
                    lock (s_syncRoot)
                    {
                        if (!s_initialized)
                        {
                            s_discTracer = new DISCTextProvider(path);
                            if (s_discTracer != null)
                            {
                                s_initialized = true;
                            }
                            else
                            {
                                Console.Error.WriteLine("Tracing initialization failed: failed to get intance of tracing provider");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Tracing initialization failed: {0}", e);
            }

            return s_initialized;
        }

        public static void Stop()
        {
            if (s_initialized)
            {
                lock (s_syncRoot)
                {
                    if (s_initialized)
                    {
                        s_initialized = false;
                        s_discTracer.Flush();
                        s_discTracer.Dispose();
                        s_discTracer = null;
                    }
                }
            }
        }

        private static bool IsEnabled(int level)
        {
            return (s_initialized) && ((level & s_traceLevel) == level);
        }

        private static string GetModuleName(StackTrace inputStack)
        {
            if (inputStack.FrameCount > 0)
            {
                return (String.Format("{0}!{1}!{2}", inputStack.GetFrame(0).GetMethod().Module, inputStack.GetFrame(0).GetMethod().ReflectedType.Name, inputStack.GetFrame(0).GetMethod().Name));
            }
            else
            {
                return String.Empty;
            }
        }

        public static void LogMethodEntry(params object[] methodParameters)
        {
            if (!IsEnabled(Constants.traceVerboseNum)) return;

            StringBuilder parameterString = new StringBuilder();

            if (methodParameters != null && methodParameters.Length > 0)
            {
                Int32 parameterCount = 0;

                foreach (object methodParameter in methodParameters)
                {
                    if (parameterCount == methodParameters.Length - 1)
                    {
                        parameterString.Append(methodParameter);
                    }
                    else
                    {
                        parameterString.Append(methodParameter);
                        parameterString.Append(", ");
                    }

                    parameterCount++;
                }
            }

            s_discTracer.WriteEvent(ref DryadMethodEntry, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, parameterString.ToString());
            
            return;
        }

        public static void LogMethodExit(params object[] methodParameters)
        {
            if (!IsEnabled(Constants.traceVerboseNum)) return;

            StringBuilder parameterString = new StringBuilder();

            if (methodParameters != null && methodParameters.Length > 0)
            {
                Int32 parameterCount = 0;

                foreach (object methodParameter in methodParameters)
                {
                    if (parameterCount == methodParameters.Length - 1)
                    {
                        parameterString.Append(methodParameter);
                    }
                    else
                    {
                        parameterString.Append(methodParameter);
                        parameterString.Append(", ");
                    }

                    parameterCount++;
                }
            }

            s_discTracer.WriteEvent(ref DryadMethodExit, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, parameterString.ToString());
           

            return;
        }

        public static void LogError(Int32 errorCode, Exception discException)
        {
            if (!IsEnabled(Constants.traceErrorNum)) return;

            StackTrace currentStack;

            if (discException != null)
            {
                currentStack = new StackTrace(discException);
            }
            else
            {
                currentStack = new StackTrace(1);
            }

            s_discTracer.WriteEvent(ref DryadError, Process.GetCurrentProcess().ProcessName, GetModuleName(currentStack), DateTime.Now, discException, String.Empty);
            
            return;
        }

        public static void LogError(Int32 errorCode, Exception discException, string messageFormat, params object[] parameterValues)
        {
            if (!IsEnabled(Constants.traceErrorNum)) return;

            StackTrace currentStack;

            if (discException != null)
            {
                currentStack = new StackTrace(discException);
            }
            else
            {
                currentStack = new StackTrace(1);
            }

            s_discTracer.WriteEvent(ref DryadError, Process.GetCurrentProcess().ProcessName, GetModuleName(currentStack), DateTime.Now, discException, messageFormat, parameterValues);
            
            return;
        }

        private static void LogCriticalToConsole(Int32 errorCode, Exception discException, string messageFormat, params object[] parameterValues)
        {
            if (discException == null && String.IsNullOrEmpty(messageFormat))
            {
                // Sadly, nothing to log
                return;
            }

            StringBuilder message = new StringBuilder();

            if (discException != null)
            {
                message.Append("Critical Exception occurred: ");
                int hr = System.Runtime.InteropServices.Marshal.GetHRForException(discException);
                if (hr != 0)
                {
                    message.Append("0x");
                    message.Append(hr.ToString("X8"));
                }
                Console.Error.WriteLine(message.ToString());
                Console.Error.WriteLine(discException.ToString());
            }
            else if (errorCode != 0)
            {
                message.Append("Critical error occured: code = ");
                message.Append(errorCode);
                Console.Error.WriteLine(message.ToString());
            }

            if (!String.IsNullOrEmpty(messageFormat))
            {
                try
                {
                    Console.Error.WriteLine(messageFormat, parameterValues);
                }
                catch
                {
                }
            }
            Console.Error.Flush();
        }

        public static void LogCritical(Int32 errorCode, Exception discException)
        {
            //
            // For LogCritical only, write message to Console.Error so that it shows up in task's output
            //
            LogCriticalToConsole(errorCode, discException, String.Empty);
            
            if (!IsEnabled(Constants.traceCriticalNum)) return;

            StackTrace currentStack;

            if (discException != null)
            {
                currentStack = new StackTrace(discException);
            }
            else
            {
                currentStack = new StackTrace(1);
            }

            s_discTracer.WriteEvent(ref DryadCritical, Process.GetCurrentProcess().ProcessName, GetModuleName(currentStack), DateTime.Now, discException, String.Empty);
            s_discTracer.Flush();
            
            return;
        }

        public static void LogCritical(Int32 errorCode, Exception discException, string messageFormat, params object[] parameterValues)
        {
            //
            // For LogCritical only, write message to Console.Error so that it shows up in task's output
            //
            LogCriticalToConsole(errorCode, discException, messageFormat, parameterValues);

            if (!IsEnabled(Constants.traceCriticalNum)) return;

            StackTrace currentStack;

            if (discException != null)
            {
                currentStack = new StackTrace(discException);
            }
            else
            {
                currentStack = new StackTrace(1);
            }

            s_discTracer.WriteEvent(ref DryadCritical, Process.GetCurrentProcess().ProcessName, GetModuleName(currentStack), DateTime.Now, discException, messageFormat, parameterValues);
            s_discTracer.Flush();
            
            return;
        }

        public static void LogWarning(string operationContext, string warningMessage)
        {
            if (!IsEnabled(Constants.traceWarningNum)) return;

            s_discTracer.WriteEvent(ref DryadWarning, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, warningMessage);
            
            return;
        }

        public static void LogWarning(string operationContext, string warningMessageFormat, params object[] parameterValues)
        {
            if (!IsEnabled(Constants.traceWarningNum)) return;

            s_discTracer.WriteEvent(ref DryadWarning, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, warningMessageFormat, parameterValues);
            
            return;
        }

        public static void LogInformation(string operationContext, string operationalMessage)
        {
            if (!IsEnabled(Constants.traceInfoNum)) return;

            s_discTracer.WriteEvent(ref DryadInformational, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, operationalMessage);
            
            return;
        }

        public static void LogInformation(string operationContext, string operationalMessageFormat, params object[] parameterValues)
        {
            if (!IsEnabled(Constants.traceInfoNum)) return;

            s_discTracer.WriteEvent(ref DryadInformational, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, operationalMessageFormat, parameterValues);
          
            return;
        }

        public static void LogDebug(string operationContext, string debugMessage)
        {
            if (!IsEnabled(Constants.traceVerboseNum)) return;

            s_discTracer.WriteEvent(ref DryadVerbose, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, debugMessage);

            return;
        }

        public static void LogDebug(string operationContext, string debugMessageFormat, params object[] parameterValues)
        {
            if (!IsEnabled(Constants.traceVerboseNum)) return;

            s_discTracer.WriteEvent(ref DryadVerbose, Process.GetCurrentProcess().ProcessName, GetModuleName(new StackTrace(1)), DateTime.Now, operationContext, debugMessageFormat, parameterValues);
            
            return;
        }  
    }
}