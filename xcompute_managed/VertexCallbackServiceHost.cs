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

namespace Microsoft.Research.Dryad
{
    using System;
    using System.ServiceModel;
    using System.ServiceModel.Description;
    using System.Net.Security;

    class VertexCallbackServiceHost
    {
        private VertexCallbackService callbackService;

        private ServiceHost selfHost;

        public VertexCallbackServiceHost(VertexScheduler vs)
        {
            callbackService = new VertexCallbackService(vs);
        }

        public bool Start(string listenUri, ISchedulerHelper schedulerHelper)
        {
            DryadLogger.LogMethodEntry(listenUri);
            Uri baseAddress = new Uri(listenUri);

            try
            {
                NetTcpBinding binding = schedulerHelper.GetVertexServiceBinding();

                selfHost = null;

                //  Retry opening the service port if address is already in use
                int maxRetryCount = 20; // Results in retrying for ~1 min
                for (int retryCount = 0; retryCount < maxRetryCount; retryCount++)
                {
                    try
                    {
                        //Step 1 of the hosting procedure: Create ServiceHost
                        selfHost = new ServiceHost(callbackService, baseAddress);

                        //Step 2 of the hosting procedure: Add service endpoints.
                        ServiceEndpoint vertexEndpoint = selfHost.AddServiceEndpoint(typeof(IDryadVertexCallback), binding, Constants.vertexCallbackServiceName);
                        ServiceThrottlingBehavior stb = new ServiceThrottlingBehavior();
                        stb.MaxConcurrentCalls = Constants.MaxConnections;
                        stb.MaxConcurrentSessions = Constants.MaxConnections;
                        selfHost.Description.Behaviors.Add(stb);

                        //Step 3 of hosting procedure : Add a security manager
                        selfHost.Authorization.ServiceAuthorizationManager = new DryadVertexServiceAuthorizationManager();

                        // Step 4 of the hosting procedure: Start the service.
                        selfHost.Open();
                        break;
                    }

                    catch (AddressAlreadyInUseException)
                    {
                        if (selfHost != null)
                        {
                            selfHost.Abort();
                            selfHost = null;
                        }

                        // If this is the last try, dont sleep. Just rethrow exception to exit.
                        if (retryCount < maxRetryCount - 1)
                        {
                            DryadLogger.LogInformation("Start Vertex Callback Service", "Address already in use. Retrying...");
                            System.Threading.Thread.Sleep(3000);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }

                DryadLogger.LogInformation("Start Vertex Callback Service", "Service Host started successfully");
                return true;
            }
            catch (CommunicationException ce)
            {
                DryadLogger.LogCritical(0, ce, "Failed to start vertex callback service");
                try
                {
                    if (selfHost != null)
                    {
                        selfHost.Abort();
                    }
                }
                catch
                {
                }
                return false;
            }
        }

        public void Stop()
        {
            if (selfHost != null)
            {
                try
                {
                    selfHost.Close(TimeSpan.FromMilliseconds(100)); 
                }
                catch (Exception)
                {
                    try
                    {
                        selfHost.Abort();
                    }
                    
                    catch { }
                }
            }
        }
    }
}
