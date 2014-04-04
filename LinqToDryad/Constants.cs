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

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Constants used by Dryad and DryadLINQ
    /// </summary>
    internal class Constants
    {
        //
        // Constants for all WCF nettcp bindings
        //
        public const int MaxReceivedMessageSize = 16 * 1024 * 1024;
        public const int MaxBufferPoolSize = 16 * 1024 * 1024;
        public const int MaxConnections = 1024;
        public const int ListenBacklog = 256;
        public static readonly TimeSpan SendTimeout = new TimeSpan(0, 2, 0);
        public static readonly TimeSpan ReceiveTimeout = new TimeSpan(0, 10, 0);
        public static readonly TimeSpan VertexSendTimeout = new TimeSpan(0, 1, 0);

        public const string ServiceLocationString = @"ServiceLocation";

        public const String jobManager = "XCJOBMANAGER";

        public const string vertexAddrFormat = "net.tcp://{0}:8050/{1}/";  // net.tcp://<machine>:8050/<JobTaskId>/
        public const string vertexCallbackAddrFormat = "net.tcp://{0}:8051/{1}/"; // net.tcp://<machine>:8051/<DryadProcId>/
        public const string vertexCallbackServiceName = "DryadVertexCallback";
        public const string vertexServiceName = "DryadVertexService";
        public const string vertexFileServiceName = "DryadVertexFileService";
        public const int vertexFileChunkSize = 1024 * 16;

        public const string vertexCountEnvVar = "DRYAD_VERTEXCOUNT";
        public const string vertexEnvVarFormat = "DRYAD_VERTEX{0}";
        public const string vertexSvcInstanceEnvVar = "DRYAD_VERTEXSVCINST";
        public const string vertexSvcLocalAddrEnvVar = "CCP_DRYADVERTEXLOCALADDRESS";

        public const string schedulerTypeEnvVar = "CCP_SCHEDULERTYPE";
        public const string schedulerTypeLocal = "LOCAL";
        public const string schedulerTypeCluster = "CLUSTER";
        public const string schedulerTypeAzure = "AZURE";
        public const string debugAzure = "DEBUG_AZURE";

        // Recognized values are: OFF, CRITICAL, ERROR, WARN, INFO, VERBOSE
        public const string TraceLevelEnvVar = "CCP_DRYADTRACELEVEL";
        public const string TraceOff = "OFF";
        public const string TraceCritical = "CRITICAL";
        public const string TraceError = "ERROR";
        public const string TraceWarning = "WARN";
        public const string TraceInfo = "INFO";
        public const string TraceVerbose = "VERBOSE";

        public const int TraceOffLevel = 0;
        public const int TraceCriticalLevel = 1;
        public const int TraceErrorLevel = 3;
        public const int TraceWarningLevel = 7;
        public const int TraceInfoLevel = 15;
        public const int TraceVerboseLevel = 31;

        // SchedulerHelper environment variables
        public const string clusterNameEnvVar = "CCP_CLUSTER_NAME";
        public const string jobIdEnvVar = "CCP_JOBID";
        public const string taskIdEnvVar = "CCP_TASKID";
        public const string nodesEnvVar = "CCP_NODES";
        public const string jobNameEnvVar = "CCP_JOBNAME";
        public const string requiredNodesEnvVar = "CCP_REQUIREDNODES";
        public const string localProcessComputeNodesEnvVar = "CCP_LOCALPROCESSCOMPUTENODES";

        // DrError.h values used in managed code
        // need to keep this section in sync with drerror.h changes...
        public const uint DrError_VertexReceivedTermination = 0x830A0003;
        public const uint DrError_VertexCompleted = 0x830A0016;
        public const uint DrError_VertexError = 0x830A0017;
        public const uint DrError_VertexInitialization = 0x830A0019;
        public const uint DrError_ProcessingInterrupted = 0x830A001A;
        public const uint DrError_VertexHostLostCommunication = 0x830A0FFF;

        // NodeAdmin constants
        // Retain time set to one day
        // todo: this should be configurable
        public static readonly TimeSpan RetainTime = new TimeSpan(1, 0, 0, 0);
        public static readonly TimeSpan FileTimeStampMarginForGC = new TimeSpan(0, 0, 5, 0);
        public const string runningJobEnvVar = "CCP_RUNNING_JOBS";
        public const string replicaPathFormat = @"\\{0}\DscData\{1}.data";
        public const string nodeAdminMutexName = "A19A8AC1-4129-46e2-BB81-ED7EE3265B05";
        public const string nodeAdminUsage = "Syntax:\n\t" + 
                                                "DscNodeAdmin [/r] [/g] [/wd] [/e] [/v] [/u]\n\n" +
                                             "Parameters:\n\t" + 
                                                "/? \t- Display this help message.\n\t" +
                                                "/g \t- Delete files not managed by DSC from the DscData share.\n\t" +
                                                "/wd\t- Delete old job working directories from the DscTemp share.\n\t" +
                                                "/r \t- Replicate DSC files onto this node.\n\t" +
                                                "/e \t- Print full error traces.\n\t" +
                                                "/u \t- Resets DscReplication account password.\n\t" +
                                                "/v \t- Print verbose activity traces.\n";

        // Client retry period is 1 second for first retry, increasing up to 12 seconds for a total of 30 seconds
        // These timeouts are intended to ride through transient network failures
        internal const int StartRetryPeriod = 1000;
        internal const int MaxRetryPeriod = 12000;
        internal const int TotalRetryPeriod = 30000;
        internal const int ClientRetryCount = 4;

        // Runtime retry period is 10 seconds for first retry, increasing up to 60 seconds for a total of 6 minutes
        // Runtime timeouts intended to ride through a failover and more severe network disruptions with the goal 
        // of keeping running jobs alive
        internal const int RuntimeStartRetryPeriod = 10000;
        internal const int RuntimeMaxRetryPeriod = 60000;
        internal const int RuntimeTotalRetryPeriod = 360000;
        internal const int RuntimeClientRetryCount = 7;
    }
}
