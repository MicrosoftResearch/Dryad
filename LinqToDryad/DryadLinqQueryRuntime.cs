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

        public ReadOnlyCollection<string> JobIds
        {
            get { return _jobIds; }
        }

        public ReadOnlyCollection<string> HeadNodes
        {
            get { return _headNodes; }
        }

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
    
    /// <summary>
    /// Represents a connection to a cluster service that can execute DryadLinq jobs.
    /// </summary>
    /// <remarks>
    /// A DryadLinqQueryRuntime holds an IScheduler object that is used to submit
    /// a DryadLINQ job.
    /// </remarks>
    internal sealed class DryadLinqQueryRuntime : IDisposable
    {
        private string m_headNode;
        private IScheduler m_scheduler;

        public DryadLinqQueryRuntime(string headNode)
        {
            this.m_headNode = headNode;
            this.m_scheduler = new YarnScheduler();
            this.m_scheduler.Connect(m_headNode);
        }

        public string HostName
        {
            get { return this.m_headNode; }
        }

        public void Dispose()
        {
            this.m_scheduler.Dispose();
        }

        // Return IScheduler reference for internal use
        internal IScheduler GetIScheduler()
        {
            return this.m_scheduler;
        }
    }
}
