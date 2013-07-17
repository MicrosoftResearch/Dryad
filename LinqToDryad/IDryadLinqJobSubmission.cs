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

//
// � Microsoft Corporation.  All rights reserved.
//
using System;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Status of a Dryad job computing a set of PartitionedTables.
    /// </summary>
    internal enum JobStatus
    {
        /// <summary>
        /// Job has not been submitted yet.
        /// </summary>
        NotSubmitted,
        /// <summary>
        /// Job is waiting in the scheduler queue.
        /// </summary>
        Waiting,
        /// <summary>
        /// Job is running on the cluster.
        /// </summary>
        Running,
        /// <summary>
        /// Job has completed successfully.
        /// </summary>
        Success,
        /// <summary>
        /// Job execution failed.
        /// </summary>
        Failure,
        /// <summary>
        /// Job has been cancelled by user.
        /// </summary>
        Cancelled
    }

    internal interface IHpcLinqJobSubmission
    {
        void AddJobOption(string fieldName, string fieldVal);
        void AddLocalFile(string fileName);
        void AddRemoteFile(string fileName);
        string ErrorMsg { get; }
        JobStatus GetStatus();
        void SubmitJob();
        JobStatus TerminateJob();
        int GetJobId();
    }
}
