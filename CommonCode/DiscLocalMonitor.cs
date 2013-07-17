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
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;
    using System.IO;
    using System.Threading;
    using System.Diagnostics;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.ServiceModel.Description;
    using System.Net.Security;
    using System.Runtime.Serialization;

    [ServiceContract(SessionMode = SessionMode.Allowed)]
    public interface IDiscLocalJobMonitor
    {
        [OperationContract]
        void UpdateJobProgress(string JobId, string JobMessage, double JobProgress);

        [OperationContract]
        void UpdateJobState(string JobId, string JobState);
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "3.0.0.0")]
    public interface IDiscLocalJobMonitorServiceChannel : IDiscLocalJobMonitor, System.ServiceModel.IClientChannel
    {
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "3.0.0.0")]
    public partial class DiscLocalJobMonitorClient : System.ServiceModel.ClientBase<IDiscLocalJobMonitor>, IDiscLocalJobMonitor
    {
        
        public DiscLocalJobMonitorClient()
        {
        }
        
        public DiscLocalJobMonitorClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) :
            base(binding, remoteAddress)
        {
        }
            
        public void UpdateJobProgress(string JobId, string JobMessage, double JobProgress)
        {
            base.Channel.UpdateJobProgress(JobId, JobMessage, JobProgress);
        }

        public void UpdateJobState(string JobId, string JobState)
        {
            base.Channel.UpdateJobState(JobId, JobState);
        }
    }

    public class DiscLocalMonitorHelper            
    {
        public string DiscLocalMonitorMachine = @"localhost";
        public DiscLocalMonitorHelper()
        {
        }
        ~DiscLocalMonitorHelper()
        {
            m_client = null;
        }

        public NetTcpBinding LocalMonitorBinding
        {
            get
            {
                NetTcpBinding binding = new NetTcpBinding(SecurityMode.Transport, false);
                binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
                binding.Security.Transport.ProtectionLevel = ProtectionLevel.None;
                return binding;
            }
        }
        
        public string LocalMonitorEpr
        {
            get
            {
                return String.Format("net.tcp://{0}:8042/Service/DiscLocalJobMonitor", DiscLocalMonitorMachine);
            }
        }

        private DiscLocalJobMonitorClient m_client = null;
        private bool faultedClient = false;
        public DiscLocalJobMonitorClient Client
        {
            get
            {
                if (faultedClient)
                {
                    return null;
                }            
                if (m_client != null)
                {
                    return m_client;
                }
                m_client = new DiscLocalJobMonitorClient(LocalMonitorBinding, new EndpointAddress(LocalMonitorEpr));
                return m_client;               
            }
        }
        
        public void UpdateProgress(string JobId, string JobMessage, double JobProgress)
        {
            try
            {
                if (this.Client != null)
                {
                    this.Client.UpdateJobProgress(JobId, JobMessage, JobProgress);
                }
            }
            catch (Exception e)
            {
                faultedClient = true;
                m_client = null;
                Console.WriteLine("ERROR: DiscLocalMonitorHelper '{0}'", e);
            }                    

        }

        public void UpdateJobState(string JobId, string JobState)
        {
            try
            {
                if (this.Client != null)
                {
                    this.Client.UpdateJobState(JobId, JobState);
                }
            }
            catch (Exception e)
            {
                faultedClient = true;
                m_client = null;
                Console.WriteLine("ERROR: DiscLocalMonitorHelper '{0}'", e);
            }                    

        }
        
    }
    
}
