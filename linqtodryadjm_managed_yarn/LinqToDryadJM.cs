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
using System.Threading;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Research.Dryad;

namespace linqtodryadjm_managed
{
    internal class DebugHelper
    {
        private static bool brokeInDebugger = false;
        private static bool loggingInitialized = false;
        private static object syncRoot = new object();

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

        public static void SetLogType()
        {
            DrLogging.SetLoggingLevel((DrLogType)DryadLogger.TraceLevel);
        }

        public static void InitializeLogging(DateTime startTime)
        {
            if (!loggingInitialized)
            {
                lock (syncRoot)
                {
                    if (!loggingInitialized)
                    {
                        // Initialize text-based tracing
                        string traceFile = Path.Combine(Directory.GetCurrentDirectory(), "GraphManagerTrace.txt");
                        DryadLogger.Start(traceFile);

                        // Initialize Graph Manager's internal logging
                        DrLogging.Initialize();
                        DebugHelper.SetLogType();

                        // Report start time to Artemis - must come after
                        // DrLogging is initialized so stdout is redirected
                        DrArtemisLegacyReporter.ReportStart((ulong) startTime.Ticks);

                        loggingInitialized = true;
                    }
                }
            }
        }

        public static void StopLogging(int retCode)
        {
            if (loggingInitialized)
            {
                lock (syncRoot)
                {
                    if (loggingInitialized)
                    {
                        // Report stop time to Artemis
                        DrArtemisLegacyReporter.ReportStop(unchecked((uint)retCode));

                        // Shutdown Graph Manager's internal logging
                        DrLogging.ShutDown(unchecked((uint)retCode));

                        // Shutdown text-based tracing
                        DryadLogger.Stop();

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
        public int ExecLinqToDryad(string[] args)
        {
            //
            // must be at least two arguments (program name and query XML file name)
            //
            if (args.Length < 2)
            {
                DryadLogger.LogCritical(0, null, "Must provide at least query XML file name.");
                return -1;
            }

            //
            // break if --break is included in arguments (and eliminate it, as it is not known downstream)
            //
            if (ConsumeSingleArgument("--break", ref args))
            {
                DebugHelper.WaitForDebugger();
            }

            //
            // parse the XML input, producing a DryadLINQ Query
            //
            Query query = new Query();
            QueryPlanParser parser = new QueryPlanParser();
            if (!parser.ParseQueryXml(args[1], query))
            {
                DryadLogger.LogCritical(0, null, "Invalid query plan");
                return -1;
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
            DryadLogger.LogInformation(null, "Arguments: {0}", msg);

            string jobClass = "DryadLINQ";
            string dryadBinDir = Environment.GetEnvironmentVariable("DRYAD_HOME");
            if (String.IsNullOrEmpty(dryadBinDir))
            {
                throw new ApplicationException("DryadLINQ requires the DRYAD_HOME environment variable to be set to the Dryad binary folder.");
            }
            string exeName = Path.Combine(dryadBinDir, "VertexHost.exe"); 
            
            // create app and run it
            //
            DrGraphParameters p = DrDefaultParameters.Make(exeName, jobClass, query.enableSpeculativeDuplication);
            
            DrArtemisLegacyReporter reporter = new DrArtemisLegacyReporter();
            p.m_defaultProcessTemplate.GetListenerList().Add(reporter);
            p.m_defaultVertexTemplate.GetListenerList().Add(reporter);
            p.m_topologyReporter = reporter;
            p.m_intermediateCompressionMode = query.intermediateDataCompression;
            DrGraphExecutor graphExecutor = new DrGraphExecutor();
            DrGraph graph = graphExecutor.Initialize(p);
            if (graph == null)
            {
                DryadLogger.LogCritical(0, null, "Failed to initialize Graph Executor");
                return -1;
            }
            DryadLINQApp app = new DryadLINQApp(graph);
            
            // Initialize with arguments
            app.SetXmlFileName(args[1]);
            if (!app.ParseCommandLineFlags(combinedArgv))
            {
                DryadLogger.LogCritical(0, null, "Bad command-line options");
                return -1;
            }
            // Build graph from query plan
            GraphBuilder builder = new GraphBuilder();
            builder.BuildGraphFromQuery(app, query);

            // Run the app
            DryadLogger.LogInformation(null, "Running the app");
            
            graphExecutor.Run();
            DrError exitStatus = graphExecutor.Join();

            DryadLogger.LogInformation(null, "Finished running the app");
            
            if (exitStatus == null || exitStatus.m_code == 0)
            {
                FinalizeExecution(query, graph);
                DryadLogger.LogInformation(null, "Application completed successfully.");
                return 0;
            }
            else
            {
                DryadLogger.LogCritical(exitStatus.m_code, null, "Application failed with error code 0x{0:X8}.\n", exitStatus.m_code);
                return exitStatus.m_code;
            }
        }
        
        public int Run(string[] args)
        {
            return ExecLinqToDryad(args);
        }

    }

}
