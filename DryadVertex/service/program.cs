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

//------------------------------------------------------------------------------
// <summary>
//      The main entry point for the application.
// </summary>
//------------------------------------------------------------------------------
namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.ServiceModel;
    using System.ServiceModel.Description;
    using System.Diagnostics;
    using System.Net;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Threading;
    using Microsoft.Research.Dryad;
    using System.IO;
    using System.Security.Principal;
    using System.Security.AccessControl;

    /// <summary>
    /// Main entry point
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Number of times to retry operations before failing
        /// </summary>
        private static int numRetries = 2;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        private static int Main(string[] args)
        {
            //
            // Try to create working directory. Fail vertex service if unable to do so.
            //
            bool createdJobDir = false;
            int retryCount = 0;
            do
            {
                try
                {
                    ProcessPathHelper.CreateUserWorkingDirectory();

                    Directory.CreateDirectory(ProcessPathHelper.JobPath);

                    createdJobDir = true;
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Failed to create working directory, {0}. Error: {1}.", ProcessPathHelper.JobPath, ex.ToString());
                    retryCount++;
                }
            } while (retryCount < numRetries && !createdJobDir);

            if (!createdJobDir)
            {
                Console.Error.WriteLine("Vertex service cannot proceed because working directory could not be created.");
                return 1;
            }

            //
            // Get Task ID from environment
            //
            int taskId;
            if (Int32.TryParse(Environment.GetEnvironmentVariable("CCP_TASKID"), out taskId) == false)
            {
                Console.Error.WriteLine("Program.Main", "Failed to read CCP_TASKID from environment");
                return 1;
            }

            //
            // Initialize tracing subsystem
            //
            string traceFile = Path.Combine(ProcessPathHelper.JobPath, String.Format("VertexServiceTrace_{0}.txt", taskId));
            DryadLogger.Start(traceFile);

            //
            // Initialize scheduler helper of the correct type
            //
            ISchedulerHelper schedulerHelper;
            try
            {
                schedulerHelper = SchedulerHelperFactory.GetInstance();
            }
            catch (Exception ex)
            {
                DryadLogger.LogCritical(0, ex, "Failed to get scheduler helper");
                DryadLogger.Stop();
                Console.Error.WriteLine("Failed to contact HPC scheduler. See log for details.");
                return 1;
            }

            //
            // Step 1 of the address configuration procedure: Create a URI to serve as the base address.
            //
            string strAddress = schedulerHelper.GetVertexServiceBaseAddress("localhost", taskId);
            Uri baseAddress = new Uri(strAddress);

            //
            // Step 2 of the hosting procedure: Create ServiceHost
            //
            ServiceHost selfHost = new ServiceHost(typeof(VertexService), baseAddress);

            try
            {
                //
                // Get the service binding
                //
                NetTcpBinding binding = schedulerHelper.GetVertexServiceBinding();

                //
                // Step 3 of the hosting procedure: Add service endpoints.
                //
                ServiceEndpoint vertexEndpoint = selfHost.AddServiceEndpoint(typeof(IDryadVertexService), binding, Constants.vertexServiceName);
                DryadLogger.LogInformation("Initialize vertex service", "listening on address {0}", vertexEndpoint.Address.ToString());

                //
                // Step 4 of hosting procedure : Add a security manager
                // TODO: Fix this for local scheduler and / or Azure scheduler when supported
                //
                selfHost.Authorization.ServiceAuthorizationManager = new DryadVertexServiceAuthorizationManager();

                // Step 5 of the hosting procedure: Start (and then stop) the service.
                selfHost.Open();

                Console.WriteLine("Vertex Service up and waiting for commands");

                // Wait for the shutdown event to be set.
                VertexService.shutdownEvent.WaitOne(-1, true);

                // Check vertex service shutdown condition
                if (VertexService.internalShutdown)
                {
                    string errorMsg = string.Format("Vertex Service Task unable to continue after critical error in initialization or communication: {0}", VertexService.ShutdownReason.ToString());
                    Console.WriteLine(errorMsg);
                    DryadLogger.LogCritical(0, new Exception(errorMsg));
                    DryadLogger.Stop();
                    try
                    {
                        selfHost.Abort();
                    }
                    catch
                    {
                    }

                    return 1;
                }

                // Close the ServiceHostBase to shutdown the service.
                selfHost.Close();
            }
            catch (CommunicationException ce)
            {
                //
                // Report any errors and fail task
                //
                DryadLogger.LogCritical(0, ce, "A communication exception occurred");
                DryadLogger.Stop();
                try
                {
                    selfHost.Abort();
                }
                catch 
                { 
                }
                Console.Error.WriteLine("CommunicationException occured, aborting vertex service. See log for details.");
                return 1;
            }
            catch (Exception ex)
            {
                //
                // Report any errors and fail task
                //
                DryadLogger.LogCritical(0, ex, "An exception occurred");
                DryadLogger.Stop();
                try
                {
                    selfHost.Abort();
                }
                catch
                {
                }
                Console.Error.WriteLine("An exception occured, aborting vertex service. See log for details.");
                return 1;
            }

            DryadLogger.LogInformation("Vertex Service", "Shut down cleanly");
            DryadLogger.Stop();
            return 0;
        }
    }
}
