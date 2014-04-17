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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Represents the current state of a set of DryadLINQ jobs that have already been
    /// submitted for execution.  A DryadLinqJobInfo object is returned after a job is
    /// submitted for execution. 
    /// </summary>
    public sealed class DryadLinqJobInfo
    {
        internal const string JOBID_NOJOB = "NoJob";
        internal const string JOBID_LOCALDEBUG = "LocalDebug";
        private ReadOnlyCollection<string> _jobIds;
        private ReadOnlyCollection<string> _headNodes;
        private ReadOnlyCollection<DryadLinqJobExecutor> _jobExecutors;

        internal DryadLinqJobInfo(string jobId,
                                  string headNode,
                                  DryadLinqJobExecutor jobExecutor)
        {
            _jobIds = Array.AsReadOnly(new string[] { jobId }); 
            _headNodes = Array.AsReadOnly(new string[] { headNode });
            _jobExecutors = Array.AsReadOnly(new DryadLinqJobExecutor[] { jobExecutor });
        }

        internal DryadLinqJobInfo(string[] jobIds,
                                  string[] headNodes,
                                  DryadLinqJobExecutor[] jobExecutors)
        {
            _jobIds = Array.AsReadOnly(jobIds);
            _headNodes = Array.AsReadOnly(headNodes);
            _jobExecutors = Array.AsReadOnly(jobExecutors);
        }

        /// <summary>
        /// Gets the job ids of the DryadLINQ jobs.
        /// </summary>
        public ReadOnlyCollection<string> JobIds
        {
            get { return _jobIds; }
        }

        internal ReadOnlyCollection<string> HeadNodes
        {
            get { return _headNodes; }
        }

        /// <summary>
        /// Blocks until all the DryadLINQ jobs terminate.
        /// </summary>
        public void Wait()
        {
            foreach (var jobExecutor in _jobExecutors)
            {
                if (jobExecutor != null)
                {
                    JobStatus finalStatus = jobExecutor.WaitForCompletion();
                    if (finalStatus != JobStatus.Success)
                    {
                        string message = SR.DidNotCompleteSuccessfully;
                        if (jobExecutor.JobSubmission.ErrorMsg != null)
                        {
                            message = "Job returned error " + jobExecutor.JobSubmission.ErrorMsg;
                        }
                        throw new DryadLinqException(DryadLinqErrorCode.DidNotCompleteSuccessfully, message);
                    }
                }
            }
        }

        /// <summary>
        /// Cancels all the unfinished jobs.
        /// </summary>
        public void CancelJob()
        {
            foreach (var jobExecutor in _jobExecutors)
            {
                if (jobExecutor != null)
                {
                    jobExecutor.Cancel();
                }
            }
        }
    }
}
