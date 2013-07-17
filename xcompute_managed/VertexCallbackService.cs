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
    using System.Collections.Generic;
    using Microsoft.Research.Dryad;

    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, InstanceContextMode = InstanceContextMode.Single, IncludeExceptionDetailInFaults = true)]
    internal class VertexCallbackService : IDryadVertexCallback
    {
        private VertexScheduler vertexScheduler;

        public VertexCallbackService(VertexScheduler scheduler)
        {
            this.vertexScheduler = scheduler;
        }


        #region IVertexCallbackService

        public void FireStateChange(int processId, ProcessState newState)
        {
            try
            {
                vertexScheduler.ProcessChangeState(processId, newState);
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Failed to change state to {0} for process {1}", newState.ToString(), processId);
            }
        }

        public void ProcessExited(int processId, int exitCode)
        {
            try
            {
                vertexScheduler.ProcessExit(processId, exitCode);
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Failed to execute process exit for process {0}", processId);
            }
        }

        public void SetGetPropsComplete(int processId, ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions)
        {
            try
            {
                vertexScheduler.SetGetPropsComplete(processId, info, propertyLabels, propertyVersions);
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Failed to complete set / get properties for process {0}", processId);
            }            
        }
        
        #endregion
    }
}
