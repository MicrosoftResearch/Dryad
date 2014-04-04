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
using System.Reflection;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Research.Dryad;

namespace Microsoft.Research.Dryad.GraphManager
{
    internal class DryadLogger
    {
        static public void LogCritical(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            DrLogging.LogCritical(message, file, function, line);
        }

        static public void LogWarning(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            DrLogging.LogWarning(message, file, function, line);
        }

        static public void LogInformation(
            string message,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            DrLogging.LogInformation(message, file, function, line);
        }
    }

    internal class DebugHelper
    {
        private static List<DrIReporter> reporters = new List<DrIReporter>();
        private static bool brokeInDebugger = false;
        private static bool loggingInitialized = false;
        private static object syncRoot = new object();

        public static Uri AddDfsJobDirectoryFromArgs(string arg)
        {
            string jobId = Microsoft.Research.Peloponnese.Storage.AzureUtils.ApplicationIdFromEnvironment();
            string jobDirectoryBase = Microsoft.Research.Peloponnese.Storage.AzureUtils.CmdLineDecode(arg.Substring(arg.IndexOf('=') + 1));
            string jobDirectorySpecific = jobDirectoryBase.Replace("_JOBID_", jobId);

            UriBuilder logLocation = new UriBuilder(jobDirectorySpecific);
            logLocation.Path = logLocation.Path + "calypso.log";

            DebugHelper.AddReporter(new DrCalypsoReporter(logLocation.Uri.AbsoluteUri));

            return new Uri(jobDirectorySpecific);
        }

        public static void UploadToDfs(Uri dfsDirectory, string localPath)
        {
            DryadLogger.LogInformation("Uploading " + localPath + " to " + dfsDirectory.AbsoluteUri);
            try
            {
                if (dfsDirectory.Scheme == "hdfs")
                {
                    using (var hdfs = new Microsoft.Research.Peloponnese.Hdfs.HdfsInstance(dfsDirectory))
                    {
                        string dfsPath = dfsDirectory.AbsolutePath + Path.GetFileName(localPath);
                        DryadLogger.LogInformation("Uploading " + localPath + " to " + dfsPath);
                        hdfs.UploadAll(localPath, dfsPath);
                    }
                }
                else if (dfsDirectory.Scheme == "azureblob")
                {
                    string account, key, container, blob;
                    Microsoft.Research.Peloponnese.Storage.AzureUtils.FromAzureUri(dfsDirectory, out account, out key, out container, out blob);
                    var azure = new Microsoft.Research.Peloponnese.Storage.AzureDfsClient(account, key, container);
                    string dfsPath = blob + Path.GetFileName(localPath);
                    DryadLogger.LogInformation("Uploading " + localPath + " to " + dfsPath);
                    azure.PutDfsFile(localPath, dfsPath);
                }
                else if (dfsDirectory.Scheme == "file")
                {
                    string dstPath = Path.Combine(dfsDirectory.AbsolutePath, Path.GetFileName(localPath));
                    File.Copy(localPath, dstPath);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Failed to upload query plan: " + e.ToString());
            }
        }

        public static void UploadToDfs(Uri dfsDirectory, string dfsName, string payload)
        {
            byte[] payloadBytes = System.Text.Encoding.UTF8.GetBytes(payload);

            DryadLogger.LogInformation("Uploading payload to " + dfsName + " at " + dfsDirectory.AbsoluteUri);
            try
            {
                if (dfsDirectory.Scheme == "hdfs")
                {
                    using (var hdfs = new Microsoft.Research.Peloponnese.Hdfs.HdfsInstance(dfsDirectory))
                    {
                        string dfsPath = dfsDirectory.AbsolutePath + dfsName;
                        hdfs.WriteAll(dfsPath, payloadBytes);
                    }
                }
                else if (dfsDirectory.Scheme == "azureblob")
                {
                    string account, key, container, blob;
                    Microsoft.Research.Peloponnese.Storage.AzureUtils.FromAzureUri(dfsDirectory, out account, out key, out container, out blob);
                    var azure = new Microsoft.Research.Peloponnese.Storage.AzureDfsClient(account, key, container);
                    string dfsPath = blob + dfsName;
                    azure.PutDfsFile(payloadBytes, dfsPath);
                }
                else if (dfsDirectory.Scheme == "file")
                {
                    string dstPath = Path.Combine(dfsDirectory.AbsolutePath, dfsName);
                    File.WriteAllBytes(dstPath, payloadBytes);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Failed to upload final output: " + e.ToString());
            }
        }

        public static void AddReporter(DrIReporter reporter)
        {
            reporters.Add(reporter);
        }

        public static List<DrIReporter> Reporters()
        {
            return reporters;
        }

        public static void WaitForDebugger()
        {
            if (!brokeInDebugger)
            {
                Console.Out.WriteLine("Waiting for debugger...");
                while (!Debugger.IsAttached)
                {
                    Thread.Sleep(1000);
                }
                Debugger.Break();
                brokeInDebugger = true;
            }
        }

        public static void DebugBreakOnFileExisting(string breakFileName)
        {
            if (File.Exists(breakFileName))
            {
                WaitForDebugger();
            }
        }

        public static void InitializeLogging(DateTime startTime)
        {
            if (!loggingInitialized)
            {
                lock (syncRoot)
                {
                    if (!loggingInitialized)
                    {
                        // Initialize Graph Manager's internal logging
                        string logDir = Environment.GetEnvironmentVariable("LOG_DIRS");
                        if (logDir == null)
                        {
                            DrLogging.Initialize("graphmanager", false);
                        }
                        else
                        {
                            // deal with comma-separated list
                            logDir = logDir.Split(',').First().Trim();
                            DrLogging.Initialize(Path.Combine(logDir, "graphmanager"), false);
                        }
                        DrLogging.SetLoggingLevel(DrLogTypeManaged.Info);

                        // Report start time to Artemis - must come after
                        // DrLogging is initialized so stdout is redirected
                        foreach (DrIReporter reporter in reporters)
                        {
                            reporter.ReportStart((ulong)startTime.ToFileTime());
                        }

                        loggingInitialized = true;
                    }
                }
            }
        }

        public static void StopLogging(int retCode, string errorString, DateTime startTime, Uri dfsDirectory)
        {
            if (loggingInitialized)
            {
                lock (syncRoot)
                {
                    if (loggingInitialized)
                    {
                        // Report stop time to Artemis
                        foreach (DrIReporter reporter in reporters)
                        {
                            reporter.ReportStop(unchecked((uint)retCode), errorString, (ulong)startTime.ToFileTime());
                        }

                        if (dfsDirectory != null && errorString != null)
                        {
                            UploadToDfs(dfsDirectory, "error.txt", errorString);
                        }

                        // Shutdown Graph Manager's internal logging
                        DrLogging.ShutDown(unchecked((uint)retCode));

                        loggingInitialized = false;
                    }
                }
            }
        }
    }

    public class LinqToDryadJM
    {
        internal void FinalizeExecution(Query query, DrGraph graph)
        {
            SortedDictionary<int, Vertex> queryPlan = query.queryPlan;
            foreach (KeyValuePair<int, Vertex> kvp in query.queryPlan)
            {
                /* used to do CSStream expiration time stuff here */
            }
        }

        internal bool ConsumeSingleArgument(string arg, ref string[] args)
        {
            List<string> temp = new List<string>();
            bool found = false;

            for (int index=0; index<args.Length; index++)
            {
                if (!found && (arg == args[index]))
                {
                    found = true;
                }
                else
                {
                    temp.Add(args[index]);
                }
            }
            args = temp.ToArray();
            return found;

        }
        
        //
        // Main Dryad LINQ execution stuff
        //
        public int ExecLinqToDryad(Uri dfsDirectory, string[] args, out string errorString)
        {
            //
            // must be at least two arguments (program name and query XML file name)
            //
            if (args.Length < 2)
            {
                errorString = "Must provide at least query XML file name and VertexHost executable.";
                DryadLogger.LogCritical(errorString);
                return -1;
            }

            //
            // break if --break is included in arguments (and eliminate it, as it is not known downstream)
            //
            if (ConsumeSingleArgument("--break", ref args))
            {
                DebugHelper.WaitForDebugger();
            }

            // this is where we ensure the right type of scheduler is registered
            Microsoft.Research.Dryad.LocalScheduler.Registration.Ensure();

            //
            // parse the XML input, producing a DryadLINQ Query
            //
            Query query = new Query();
            QueryPlanParser parser = new QueryPlanParser();
            if (!parser.ParseQueryXml(args[1], query))
            {
                errorString = "Invalid query plan";
                DryadLogger.LogCritical(errorString);
                return -1;
            }

            //
            // upload the query plan to the job DFS directory for the benefit of debug tools
            //
            if (dfsDirectory != null)
            {
                DebugHelper.UploadToDfs(dfsDirectory, args[1]);
            }

            //
            // build internal app arguments
            //
            List<string> internalArgs = new List<string>();

            //
            // add the XmlExecHost args to the internal app arguments
            //
            foreach (string xmlExecHostArg in query.xmlExecHostArgs)

            {
                if (xmlExecHostArg == "--break")
                {
                    DebugHelper.WaitForDebugger();
                }
                else
                {
                    internalArgs.Add(xmlExecHostArg);
                }
            }

            //
            // combine internal arguments with any additional arguments received on the command line
            // don't include argv[0] and argv[1] (program name and query XML file name)
            //

            int internalArgc = (int)internalArgs.Count;
            int externalArgc = args.Length - 2;          // don't include argv[0] and argv[1]
            int combinedArgc = internalArgc + externalArgc;
            string[] combinedArgv = new string[combinedArgc];

            string msg = "";
            // internal arguments first
            for (int i=0; i<internalArgc; i++)
            {
                combinedArgv[i] = internalArgs[i];
                msg += String.Format("{0} ", combinedArgv[i]);
            }
            
            // then external arguments
            for (int i = 0; i<externalArgc; i++)
            {
                combinedArgv[i+internalArgc] = args[i+2]; // don't include argv[0] and argv[1]

                msg += String.Format("{0} ", combinedArgv[i+internalArgc]);
            }
            DryadLogger.LogInformation(String.Format("Arguments: {0}", msg));

            string jobClass = "DryadLINQ";
            string exeName = args[0];
            
            // create app and run it
            //
            DrGraphParameters p = DrDefaultParameters.Make(exeName, jobClass, query.enableSpeculativeDuplication);

            foreach (DrIReporter reporter in DebugHelper.Reporters())
            {
                p.m_reporters.Add(reporter);
            }
            p.m_intermediateCompressionMode = query.intermediateDataCompression;
            DrGraphExecutor graphExecutor = new DrGraphExecutor();
            DrGraph graph = graphExecutor.Initialize(p);
            if (graph == null)
            {
                errorString = "Failed to initialize Graph Executor";
                DryadLogger.LogCritical(errorString);
                return -1;
            }
            DryadLINQApp app = new DryadLINQApp(graph);
            
            // Initialize with arguments
            app.SetXmlFileName(args[1]);
            if (!app.ParseCommandLineFlags(combinedArgv))
            {
                errorString = "Bad command-line options";
                DryadLogger.LogCritical(errorString);
                return -1;
            }
            // Build graph from query plan
            GraphBuilder builder = new GraphBuilder();
            builder.BuildGraphFromQuery(app, query);

            // Run the app
            DryadLogger.LogInformation("Running the app");
            
            graphExecutor.Run();
            DrError exitStatus = graphExecutor.Join();

            DryadLogger.LogInformation("Finished running the app");
            
            if (exitStatus == null || exitStatus.m_code == 0)
            {
                FinalizeExecution(query, graph);
                DryadLogger.LogInformation("Application completed successfully.");
                errorString = null;
                return 0;
            }
            else
            {
                DryadLogger.LogCritical(String.Format("Application failed with error code 0x{0:X8}.\n", exitStatus.m_code));
                errorString = exitStatus.ToFullTextNative();
                return exitStatus.m_code;
            }
        }
        
        public int Run(Uri dfsDirectory, string[] args, out string errorString)
        {
            return ExecLinqToDryad(dfsDirectory, args, out errorString);
        }

    }

}
