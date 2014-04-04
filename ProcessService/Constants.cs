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
//      Constants used by managed code in Dryad
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad.ProcessService
{
    using System;

    internal class Constants
    {
        public const string EnvManagerJobGuid = "PELOPONNESE_JOB_GUID";
        public const string EnvManagerServerUri = "PELOPONNESE_SERVER_URI";
        public const string EnvProcessGroup = "PELOPONNESE_PROCESS_GROUP";
        public const string EnvProcessIdentifier = "PELOPONNESE_PROCESS_IDENTIFIER";
        public const string EnvProcessHostName = "PELOPONNESE_PROCESS_HOSTNAME";
        public const string EnvProcessRackName = "PELOPONNESE_PROCESS_RACKNAME";

        public const string HttpHeaderValueVersion = "X-Dryad-ValueVersion";
        public const string HttpHeaderValueStatus = "X-Dryad-ValueStatus";
        public const string HttpHeaderProcessStatus = "X-Dryad-ProcessStatus";
        public const string HttpHeaderProcessExitCode = "X-Dryad-ProcessExitCode";
        public const string HttpHeaderProcessStartTime = "X-Dryad-ProcessStartTime";
        public const string HttpHeaderProcessStopTime = "X-Dryad-ProcessEndTime";

        public const string EnvProcessServerUri = "DRYAD_PROCESS_SERVER_URI";

        public const Int32 HdfsServiceDefaultHttpPort = 50070;

        // Recognized values are: OFF, CRITICAL, ERROR, WARN, INFO, VERBOSE
        public const string traceLevelEnvVar = "DRYAD_TRACE_LEVEL";
        public const string traceOff = "OFF";
        public const string traceCritical = "CRITICAL";
        public const string traceError = "ERROR";
        public const string traceWarning = "WARN";
        public const string traceInfo = "INFO";
        public const string traceVerbose = "VERBOSE";

        public const int traceOffNum = 0;
        public const int traceCriticalNum = 1;
        public const int traceErrorNum = 3;
        public const int traceWarningNum = 7;
        public const int traceInfoNum = 15;
        public const int traceVerboseNum = 31;

        // DrError.h values used in managed code
        // need to keep this section in sync with drerror.h changes...
        public const uint DrError_VertexReceivedTermination = 0x830A0003;
        public const uint DrError_VertexCompleted = 0x830A0016;
        public const uint DrError_VertexError = 0x830A0017;
        public const uint DrError_VertexInitialization = 0x830A0019;
        public const uint DrError_ProcessingInterrupted = 0x830A001A;
        public const uint DrError_VertexHostLostCommunication = 0x830A0FFF;

        public const uint WinError_StillActive = 0x00000103;
        public const uint WinError_ConnectionAlreadyOpen = 0x80004005;

        // dummy key used to block on process state changes
        public const string NullKeyString = " _ nokey _ ";
    }
}
