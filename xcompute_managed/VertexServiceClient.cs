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
    using System.Runtime.Serialization;
    using System.Collections.Specialized;

    public partial interface IDryadVertexService
    {
        //
        // Shutdown
        //
        [OperationContract(AsyncPattern = true, IsOneWay = true, Action = "http://hpc.microsoft.com/dryadvertex/shutdown")]
        IAsyncResult BeginShutdown(uint ShutdownCode, AsyncCallback callback, object state);

        void EndShutdown(IAsyncResult result);

        //
        // ReleaseProcess
        //
        [OperationContract(AsyncPattern = true, IsOneWay = true, Action = "http://hpc.microsoft.com/dryadvertex/releaseprocess")]
        IAsyncResult BeginReleaseProcess(int processId, AsyncCallback callback, object state);

        void EndReleaseProcess(IAsyncResult result);

        //
        // ScheduleProcess
        //
        [OperationContract(AsyncPattern = true, Action = "http://hpc.microsoft.com/dryadvertex/scheduleprocess")]
        IAsyncResult BeginScheduleProcess(string replyUri, int processId, string commandLine, StringDictionary environment, AsyncCallback callback, object state);

        bool EndScheduleProcess(IAsyncResult result);

        //
        // CancelScheduleProcess
        //
        [OperationContract(AsyncPattern = true, IsOneWay = true, Action = "http://hpc.microsoft.com/dryadvertex/cancelscheduleprocess")]
        IAsyncResult BeginCancelScheduleProcess(int processId, AsyncCallback callback, object state);

        void EndCancelScheduleProcess(IAsyncResult result);

        //
        // SetGetProps
        //
        [OperationContract(AsyncPattern = true, Action = "http://hpc.microsoft.com/dryadvertex/setgetprops")]
        IAsyncResult BeginSetGetProps(string replyUri, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics, AsyncCallback callback, object state);

        bool EndSetGetProps(IAsyncResult result);

        //
        // CheckStatus
        //
        [OperationContract(AsyncPattern = true, Action = "http://hpc.microsoft.com/dryadvertex/checkstatus")]
        IAsyncResult BeginCheckStatus(AsyncCallback callback, object state);

        VertexStatus EndCheckStatus(IAsyncResult result);

        //
        // Initialize
        //
        [OperationContract(AsyncPattern = true, IsOneWay = true, Action = "http://hpc.microsoft.com/dryadvertex/initialize")]
        IAsyncResult BeginInitialize(StringDictionary vertexEndpointAddresses, AsyncCallback callback, object state);

        void EndInitialize(IAsyncResult result);

    }

    public class VertexServiceClient : ClientBase<IDryadVertexService>, IDryadVertexService
    {

        public VertexServiceClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) :
            base(binding, remoteAddress)
        {
        }

        public void CancelScheduleProcess(int processId)
        {
            base.Channel.CancelScheduleProcess(processId);
        }

        public IAsyncResult BeginCancelScheduleProcess(int processId, AsyncCallback callback, object state)
        {
            return base.Channel.BeginCancelScheduleProcess(processId, callback, state);
        }

        public void EndCancelScheduleProcess(IAsyncResult result)
        {
            base.Channel.EndCancelScheduleProcess(result);
        }

        public VertexStatus CheckStatus()
        {
            return base.Channel.CheckStatus();
        }

        public IAsyncResult BeginCheckStatus(AsyncCallback callback, object state)
        {
            return base.Channel.BeginCheckStatus(callback, state);
        }

        public VertexStatus EndCheckStatus(IAsyncResult result)
        {
            return base.Channel.EndCheckStatus(result);
        }

        public void Initialize(StringDictionary vertexEndpointAddresses)
        {
            base.Channel.Initialize(vertexEndpointAddresses);
        }

        public IAsyncResult BeginInitialize(StringDictionary vertexEndpointAddresses, AsyncCallback callback, object state)
        {
            return base.Channel.BeginInitialize(vertexEndpointAddresses, callback, state);
        }

        public void EndInitialize(IAsyncResult result)
        {
            base.Channel.EndInitialize(result);
        }

        public void ReleaseProcess(int processId)
        {
            base.Channel.ReleaseProcess(processId);
        }

        public IAsyncResult BeginReleaseProcess(int processId, AsyncCallback callback, object state)
        {
            return base.Channel.BeginReleaseProcess(processId, callback, state);
        }

        public void EndReleaseProcess(IAsyncResult result)
        {
            base.Channel.EndReleaseProcess(result);
        }

        public bool ScheduleProcess(string replyUri, int processId, string commandLine, System.Collections.Specialized.StringDictionary environment)
        {
            return base.Channel.ScheduleProcess(replyUri, processId, commandLine, environment);
        }

        public IAsyncResult BeginScheduleProcess(string replyUri, int processId, string commandLine, System.Collections.Specialized.StringDictionary environment, AsyncCallback callback, object state)
        {
            return base.Channel.BeginScheduleProcess(replyUri, processId, commandLine, environment, callback, state);
        }

        public bool EndScheduleProcess(IAsyncResult result)
        {
            return base.Channel.EndScheduleProcess(result);
        }

        public bool SetGetProps(string replyUri, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics)
        {
            return base.Channel.SetGetProps(replyUri, processId, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics);
        }

        public IAsyncResult BeginSetGetProps(string replyUri, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics, AsyncCallback callback, object state)
        {
            return base.Channel.BeginSetGetProps(replyUri, processId, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics, callback, state);
        }

        public bool EndSetGetProps(IAsyncResult result)
        {
            return base.Channel.EndSetGetProps(result);
        }

        public void Shutdown(uint ShutdownCode)
        {
            base.Channel.Shutdown(ShutdownCode);
        }

        public IAsyncResult BeginShutdown(uint ShutdownCode, AsyncCallback callback, object state)
        {
            return base.Channel.BeginShutdown(ShutdownCode, callback, state);
        }

        public void EndShutdown(IAsyncResult result)
        {
            base.Channel.EndShutdown(result);
        }
    }
}
