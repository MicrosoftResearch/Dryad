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
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using Microsoft.Research.Dryad;

namespace Microsoft.Research.Dryad.GraphManager
{
    internal class ConfigDependency : Microsoft.Research.Peloponnese.Shared.AssemblyDependencyAttribute
    {
        public ConfigDependency() : base("Microsoft.Research.Dryad.GraphManager.exe.config", false)
        {
        }
    }

    class Program
    {
        static Uri dfsDirectory;

        static void ExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            int result = unchecked((int)LinqToDryadException.E_FAIL);
            Exception e = args.ExceptionObject as Exception;
            string errorString = "Unknown exception";
            if (e != null)
            {
                result = System.Runtime.InteropServices.Marshal.GetHRForException(e);
                errorString = e.ToString();
                DryadLogger.LogCritical(errorString);
                System.Threading.Thread.Sleep(10 * 1000);
            }
            DebugHelper.StopLogging(result, errorString, DateTime.Now.ToLocalTime(), dfsDirectory);

            if (Debugger.IsAttached)
            {
                Debugger.Break();
            }
            else
            {
                DrLogging.WriteMiniDump();
            }

            // We need to Exit, since other threads in the GM
            // are likely to still be running.
            Environment.Exit(result);
        }

        static int Main(string[] args)
        {
            Console.WriteLine("{0} starting up in {1}", Environment.CommandLine, Environment.CurrentDirectory);
            
            bool waitForDebugger = false;
            List<string> tempArgs = new List<string>();

            // Record start time so we can report it once logging has been initialized
            DateTime startTime = DateTime.Now.ToLocalTime();

            // Set unhandled exception handler to catch anything thrown from 
            // Microsoft.Hpc.Query.GraphManager.dll
            AppDomain currentDomain = AppDomain.CurrentDomain;
            currentDomain.UnhandledException += new UnhandledExceptionEventHandler(ExceptionHandler);

            dfsDirectory = null;

            foreach (string arg in args)
            {
                if (String.Compare(arg, "--break", StringComparison.OrdinalIgnoreCase) == 0)
                {
                    waitForDebugger = true;
                }
                else if (arg.StartsWith("--dfs="))
                {
                    dfsDirectory = DebugHelper.AddDfsJobDirectoryFromArgs(arg);
                }
                else 
                {
                    tempArgs.Add(arg);
                }
            }
            args = tempArgs.ToArray();

            if (waitForDebugger)
            {
                DebugHelper.WaitForDebugger();
            }

            //
            // Configure tracing
            //
            DebugHelper.InitializeLogging(startTime);

            //
            // Run the Graph Manager
            //
            int retCode = 0;
            string errorString = null;
            try
            {
                LinqToDryadJM jm = new LinqToDryadJM();
                retCode = jm.Run(dfsDirectory, args, out errorString);
            }
            catch (Exception e)
            {
                retCode = System.Runtime.InteropServices.Marshal.GetHRForException(e);
                if (retCode == 0)
                {
                    errorString = e.ToString();
                    DryadLogger.LogCritical(errorString);
                    retCode = unchecked((int)LinqToDryadException.E_FAIL);
                }
                else
                {
                    errorString = String.Format("{0}: {1}", retCode, e);
                    DryadLogger.LogCritical(errorString);
                }
                System.Threading.Thread.Sleep(10 * 1000);
            }

            DebugHelper.StopLogging(retCode, errorString, DateTime.Now.ToLocalTime(), dfsDirectory);

            // NOTE: We don't want to log critical errors twice, so here we're assuming that
            // if the GM exited "gracefully" and returned an error code instead of throwing
            // an exception, that it has already logged the error.
            if (retCode != 0)
            {
                // If the Graph Manager already started executing, we need to exit the process.
                // Exiting the thread (returning from Main) will not necessarily cause the GM's 
                // worker threads to exit

                // TODO: Consider deleting temp output stream from DSC
                // requires that we have access to the URI at this point, though
                Environment.Exit(retCode);
            }

            //
            // Cleanup all vertex tasks in case any became
            // unreachable.
            //
            
            return retCode;
        }
    }
}
