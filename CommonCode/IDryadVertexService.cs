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
//      Vertex Service contracts
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Runtime.Serialization;
    using System.ServiceModel;
    using System.Diagnostics;

    /// <summary>
    /// Holds property information
    /// </summary>
    [DataContract]
    public class ProcessPropertyInfo
    {
        [DataMember]
        public string propertyLabel;
        [DataMember]
        public ulong propertyVersion;
        [DataMember]
        public string propertyString;
        [DataMember]
        public byte[] propertyBlock;
    }

    /// <summary>
    /// Keeps track of vertex host process information
    /// </summary>
    [DataContract]
    public class VertexProcessInfo
    {
        [DataMember]
        public int DryadId;
        [DataMember]
        public string commandLine;
        [DataMember]
        public ProcessState State;        
    }
    
    /// <summary>
    /// Keeps track of CPU and memory info for vertex host process
    /// </summary>
    [DataContract]
    public class VertexStatus
    {
        [DataMember]
        public bool serviceIsAlive = false;
        [DataMember]
        public Dictionary<string,ulong> freeDiskSpaces = new Dictionary<string,ulong>();
        [DataMember]
        public ulong freeVirtualMemory = 0;
        [DataMember]
        public ulong freePhysicalMemory = 0;
        [DataMember]
        public uint runningProcessCount = 0;
        [DataMember]
        public List<VertexProcessInfo> vps = new List<VertexProcessInfo>();
    }

    [DataContract(Namespace = "http://hpc.microsoft.com/dryadvertex/")]
    [Serializable]
    public class VertexServiceError
    {
        public const string Action = "http://hpc.microsoft.com/dryadvertex/VertexServiceError";

        /// <summary>
        /// Stores the Operation
        /// </summary>
        [DataMember]
        private string operation;

        /// <summary>
        /// Stores the Reason
        /// </summary>
        [DataMember]
        private string reason;

        /// <summary>
        /// Initializes a new instance of the VertexServiceError class
        /// </summary>
        /// <param name="operation">The operation that failed</param>
        /// <param name="reason">The detailed reason for the failure (exception.ToString())</param>
        public VertexServiceError(string operation, string reason)
        {
            this.operation = operation;
            this.reason = reason;
        }

        /// <summary>
        /// The detailed reason for the failure
        /// </summary>
        public string Reason
        {
            get
            {
                return this.reason;
            }
        }

        /// <summary>
        /// The operation that failed
        /// </summary>
        public string Operation
        {
            get
            {
                return this.operation;
            }
        }
    }

    [DataContract(Namespace = "http://hpc.microsoft.com/dryadvertex/")]
    [Serializable]
    public class UnknownProcessError
    {
        public const string Action = "http://hpc.microsoft.com/dryadvertex/UnknownProcessError";

        /// <summary>
        /// Stores the ProcessId
        /// </summary>
        [DataMember]
        private int processId;

        /// <summary>
        /// Initializes a new instance of the UnknownProcessError class
        /// </summary>
        /// <param name="id">Id of the unknown process</param>
        public UnknownProcessError(int id)
        {
            this.processId = id;
        }

        /// <summary>
        /// The process id which was not found
        /// </summary>
        public int Processid
        {
            get
            {
                return this.processId;
            }
        }

    }

    /// <summary>
    /// Dryad Vertex Service Contract - allows GM to schedule vertices and VH to report status
    /// </summary>
    [ServiceContract(Name = "IDryadVertexService", Namespace = "http://hpc.microsoft.com/dryadvertex/", SessionMode = SessionMode.Allowed)]
    public partial interface IDryadVertexService
    {
        [OperationContract(IsOneWay=true, Action = "http://hpc.microsoft.com/dryadvertex/cancelscheduleprocess")]
        void CancelScheduleProcess(int processId);

        // TODO: Deprecated.
        [OperationContract(Action = "http://hpc.microsoft.com/dryadvertex/checkstatus")]
        VertexStatus CheckStatus();

        [OperationContract(IsOneWay = true, Action = "http://hpc.microsoft.com/dryadvertex/initialize")]
        void Initialize(StringDictionary vertexEndpointAddresses);

        [OperationContract(IsOneWay=true, Action = "http://hpc.microsoft.com/dryadvertex/releaseprocess")]
        void ReleaseProcess(int processId);

        [OperationContract(Action = "http://hpc.microsoft.com/dryadvertex/scheduleprocess")]
        [FaultContract(typeof(VertexServiceError), Action = VertexServiceError.Action)]
        bool ScheduleProcess(string replyUri, int processId, string commandLine, StringDictionary environment);

        [OperationContract(Action = "http://hpc.microsoft.com/dryadvertex/setgetprops")]
        [FaultContract(typeof(VertexServiceError), Action = VertexServiceError.Action)]
        [FaultContract(typeof(UnknownProcessError), Action = UnknownProcessError.Action)]
        bool SetGetProps(string replyUri, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics);        

        [OperationContract(IsOneWay=true, Action = "http://hpc.microsoft.com/dryadvertex/shutdown")]
        void Shutdown(uint ShutdownCode);
    }

}
