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
//      Handles WCF communication for vertex service
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net.Security;
    using System.ServiceModel;
    using Microsoft.Research.Dryad;

    class ReplyDispatcher
    {
        private static string graphMgrUri = String.Empty;
        private static VertexCallbackServiceClient graphMgrClient;

        private static string vertexProcUri = String.Empty;
        private static VertexCallbackServiceClient vertexProcClient;

        private static NetTcpBinding binding = null;
        private static readonly int numRetries = 6;
        private static readonly int retrySleepTime = 1000;
        private static object vertexLock = new object();
        private static object graphMgrLock = new object();
        private static object syncRoot = new object();
        private static bool shuttingDown = false;

        /// <summary>
        /// Flag to notify reply dispatcher that job is shutting down and any communication errors should be ignored
        /// </summary>
        internal static bool ShuttingDown
        {
            get
            {
                return shuttingDown;
            }

            set
            {
                shuttingDown = value;
            }
        }

        /// <summary>
        /// Initialize binding defaults
        /// </summary>
        static ReplyDispatcher()
        {
            using (ISchedulerHelper helper = SchedulerHelperFactory.GetInstance())
            {
                binding = helper.GetVertexServiceBinding();
            }
        }

        /// <summary>
        /// Check URI to see if it is the graph manager by checking for vertex id /1/
        /// </summary>
        /// <param name="uri">URI where vertex is expecting a response</param>
        /// <returns>true if graph manager</returns>
        public static bool IsGraphMrgUri(string uri)
        {
            Uri u = new Uri(uri);
            return u.AbsolutePath.StartsWith("/1/");
        }

        /// <summary>
        /// Creates a new WCF client to service listening at URI
        /// </summary>
        /// <param name="uri">wcf service endpoint</param>
        /// <returns>client to WCF service</returns>
        private static VertexCallbackServiceClient CreateClient(string uri)
        {
            VertexCallbackServiceClient client = new VertexCallbackServiceClient(binding, new EndpointAddress(uri));
            lock (syncRoot)
            {
                //
                // If the graph manager URI is specified, store this client as the GM client, otherwise assume it's a vertex host 
                //
                if (IsGraphMrgUri(uri))
                {
                    graphMgrUri = uri;
                    graphMgrClient = client;
                }
                else
                {
                    vertexProcUri = uri;
                    vertexProcClient = client;
                }
            }

            return client;
        }

        /// <summary>
        /// Close an existing client
        /// </summary>
        /// <param name="client">client to dispose</param>
        private static void DisposeClient(ref VertexCallbackServiceClient client)
        {
            if (client != null)
            {
                try
                {
                    client.Close();
                }
                catch (Exception)
                {
                    try
                    {
                        client.Abort();
                    }
                    catch (Exception)
                    {
                        // If client cannot be aborted, just finish silently.
                    }
                }

                client = null;
            }
            else
            {
                throw new ArgumentNullException("client");
            }
        }

        /// <summary>
        /// Returns client pointing to URI - create if needed
        /// </summary>
        /// <param name="uri">WCF server address</param>
        /// <returns>Client to server listening at URI</returns>
        private static VertexCallbackServiceClient GetClient(string uri)
        {
            if (graphMgrUri.Equals(uri, StringComparison.OrdinalIgnoreCase))
            {
                return graphMgrClient;
            }
            else if (vertexProcUri.Equals(uri, StringComparison.OrdinalIgnoreCase))
            {
                return vertexProcClient;
            }
            else
            {
                return CreateClient(uri);
            }
        }

        /// <summary>
        /// Try to reopen client to WCF service
        /// </summary>
        /// <param name="uri">Address of service</param>
        /// <returns>new client</returns>
        private static VertexCallbackServiceClient ReopenClient(string uri)
        {
            lock (syncRoot)
            {
                //
                // Get any existing client to this URI
                //
                VertexCallbackServiceClient client = GetClient(uri);
                if (client != null)
                {
                    //
                    // If a client exists, dispose it
                    //
                    DisposeClient(ref client);
                }

                //
                // Recreate the client 
                //
                return CreateClient(uri);
            }
        }

        /// <summary>
        /// Helper method to retry opening the client for use with state changes and property comm
        /// </summary>
        /// <param name="replyUri">URI to respond to</param>
        /// <param name="e">Reason for retry</param>
        /// <returns>new client - may be null on failures</returns>
        private static VertexCallbackServiceClient ReopenClientForRetry(string replyUri, Exception e)
        {
            VertexCallbackServiceClient client = null;
            DryadLogger.LogError(0, e);
            try
            {
                client = ReopenClient(replyUri);
            }
            catch (Exception reopenEx)
            {
                DryadLogger.LogError(0, reopenEx, "Unable to reopen client connection");
            }

            //
            // If retrying, sleep briefly
            //
            System.Threading.Thread.Sleep(retrySleepTime);

            return client;
        }

        /// <summary>
        /// Notify URI of state change
        /// </summary>
        /// <param name="replyUri">where to send state change notification</param>
        /// <param name="processId">vertex process id</param>
        /// <param name="newState">updated state</param>
        /// <returns>success/failure of state change notification</returns>
        public static bool FireStateChange(string replyUri, int processId, ProcessState newState)
        {
            DryadLogger.LogMethodEntry(replyUri, processId, newState);

            bool result = false;
            VertexCallbackServiceClient client = GetClient(replyUri);

            //
            // Try to notify GM of state change up to numRetries times 
            //
            for (int index = 0; index < numRetries; index++)
            {                
                try
                {
                    //
                    // If client is null, try reopening it
                    //
                    if (client == null)
                    {
                        client = CreateClient(replyUri);
                    }

                    //
                    // Make FireStateChange WCF call, return success
                    //
                    client.FireStateChange(processId, newState);
                    result = true;
                    break;
                }
                catch (Exception e)
                {
                    if (shuttingDown)
                    {
                        // if shutting down, just return
                        DisposeClient(ref client);
                        return true;
                    }
                    else
                    {
                        //
                        // If call failed, try reopening WCF client and calling again
                        //
                        client = ReopenClientForRetry(replyUri, e);
                    }
                }
            }

            //
            // If failure occurs after X retry attempts, report error
            //
            DryadLogger.LogMethodExit(result);
            return result;            
        }

        /// <summary>
        /// Notify GM that vertex host process exited
        /// </summary>
        /// <param name="replyUri">GM address</param>
        /// <param name="processId">vertex process id</param>
        /// <param name="exitCode">reason for vertex host exit</param>
        /// <returns>success/failure</returns>
        public static bool ProcessExited(string replyUri, int processId, int exitCode)
        {
            DryadLogger.LogMethodEntry(replyUri, processId, exitCode);

            bool result = false;

            VertexCallbackServiceClient client = GetClient(replyUri);

            //
            // Try to notify GM that the process has exited up to numRetries times 
            //
            for(int index = 0; index < numRetries; index++)
            {                
                try
                {
                    //
                    // If client is null, try reopening it
                    //
                    if(client == null)
                    {
                        client = CreateClient(replyUri);
                    }

                    //
                    // Make ProcessExited WCF call, return success
                    //
                    client.ProcessExited(processId, exitCode);
                    result = true;
                    break;
                }
                catch (Exception e)
                {
                    if (shuttingDown)
                    {
                        // if shutting down, just return
                        DisposeClient(ref client);
                        return true;
                    }
                    else
                    {
                        //
                        // If call failed, try reopening WCF client and calling again
                        //
                        client = ReopenClientForRetry(replyUri, e);
                    }
                }
            }
            
            //
            // If failure occurs after X retry attempts, report error
            //
            DryadLogger.LogMethodExit(result);
            return result;
        }

        /// <summary>
        /// Attempt to call SetGetPropsComplete on specified WCF service.
        /// </summary>
        /// <param name="replyUri">Service endpoint</param>
        /// <param name="systemProcess"></param>
        /// <param name="processId"></param>
        /// <param name="info"></param>
        /// <param name="propertyLabels"></param>
        /// <param name="propertyVersions"></param>
        /// <returns></returns>
        public static bool SetGetPropsComplete(string replyUri, Process systemProcess, int processId, ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions)
        {
            DryadLogger.LogMethodEntry(replyUri, processId);

            bool result = false;

            VertexCallbackServiceClient client = GetClient(replyUri);

            //
            // Try to set/get properties up to numRetries times
            //
            for (int index = 0; index < numRetries; index++)
            {                
                try
                {
                    //
                    // If client is null, try reopening it
                    //
                    if (client == null)
                    {
                        client = CreateClient(replyUri);
                    }

                    //
                    // Make SetGetPropsComplete WCF call, return success
                    //
                    client.SetGetPropsComplete(processId, info, propertyLabels, propertyVersions);
                    result = true;
                    break;
                }
                catch (Exception e)
                {
                    if ((IsGraphMrgUri(replyUri) == false && systemProcess.HasExited) || shuttingDown)
                    {
                        //
                        // If trying to connect to non-running vertex or job is shutting down, don't retry and report success.
                        //
                        DisposeClient(ref client);
                        return true;
                    }
                    else
                    {
                        //
                        // If call failed and talking to GM or running vertex process, try reopening WCF client and calling again
                        //
                        client = ReopenClientForRetry(replyUri, e);
                    }
                }
            }

            //
            // If failed to connect X times, report error
            //
            DryadLogger.LogMethodExit(result);
            return result;
        }
    }
}
