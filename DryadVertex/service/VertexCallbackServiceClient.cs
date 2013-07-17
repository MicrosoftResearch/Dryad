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
    using Microsoft.Research.Dryad;

    class VertexCallbackServiceClient : ClientBase<IDryadVertexCallback>, IDryadVertexCallback
    {
        public VertexCallbackServiceClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) :
            base(binding, remoteAddress)
        {
        }

        public void FireStateChange(int processId, ProcessState newState)
        {
            Channel.FireStateChange(processId, newState);
        }

        public void SetGetPropsComplete(int processId, ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions)
        {
            Channel.SetGetPropsComplete(processId, info, propertyLabels, propertyVersions);
        }

        public void ProcessExited(int processId, int exitCode)
        {
            Channel.ProcessExited(processId, exitCode);
        }

    }
}
