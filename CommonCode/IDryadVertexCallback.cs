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
    using System.Collections.Generic;
    using System.ServiceModel;
    using System.Runtime.Serialization;

    [DataContract]
    public class ProcessStatistics
    {
        [DataMember]
        public uint flags;
        [DataMember]
        public long processUserTime;
        [DataMember]
        public long processKernelTime;
        [DataMember]
        public int pageFaults;
        [DataMember]
        public int totalProcessesCreated;
        [DataMember]
        public ulong peakVMUsage;
        [DataMember]
        public ulong peakMemUsage;
        [DataMember]
        public ulong memUsageSeconds;
        [DataMember]
        public ulong totalIo;    
    };

    [DataContract]
    public class ProcessInfo
    {
        [DataMember]
        public uint flags;
        [DataMember]
        public ProcessState processState;
        [DataMember]
        public uint processStatus;
        [DataMember]
        public uint exitCode;
        [DataMember]
        public ProcessPropertyInfo[] propertyInfos;
        [DataMember]
        public ProcessStatistics processStatistics;
    };
            
    [ServiceContract(SessionMode = SessionMode.Allowed)]
    public interface IDryadVertexCallback
    {
        [OperationContract]
        void FireStateChange(int processId, ProcessState newState);

        [OperationContract]
        void SetGetPropsComplete(int processId, ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions);        
       
        [OperationContract]
        void ProcessExited(int processId, int exitCode);
    }
}
