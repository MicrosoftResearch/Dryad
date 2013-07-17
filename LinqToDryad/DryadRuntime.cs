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
using System.Linq;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    public sealed class HpcLinqJobInfo
    {
        internal const int JOBID_LOCALDEBUG = -1;
        private int _jobId;
        private string _headNode;
        private string[] _targetUris; // Test-support
        private JobExecutor _jobExecutor;

        public int JobId 
        { 
            get {return _jobId;}
        }

        public string HeadNode
        {
            get { return _headNode; }
        }

        // Test-support
        internal string[] TargetUris 
        {
            get { return _targetUris; }
        } 

        internal HpcLinqJobInfo(int jobId,
                                string headNode,
                                JobExecutor jobExecutor,
                                string[] targetUris)
        {
            _jobId = jobId; 
            _headNode = headNode;
            _jobExecutor = jobExecutor;
            _targetUris = targetUris;
        }

        public void Wait()
        {
            if (_jobExecutor != null)
            {
                JobStatus finalStatus = _jobExecutor.WaitForCompletion();
                if (finalStatus != JobStatus.Success)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.DidNotCompleteSuccessfully,
                                               SR.DidNotCompleteSuccessfully);
                }
            }
        }
    }
    
    /// <summary>
    /// Represents a connection to a HPC Server that can execute HpcLinq jobs.
    /// </summary>
    /// <remarks>
    /// A HpcQueryRuntime instance holds an open Microsoft.Hpc.Scheduler.Scheduler connection.
    /// This connection can be closed by calling Dispose()
    /// When a HpcQueryRuntime instance is passed to HpcLinqQuery.Submit(), HpcLinq will use
    /// the open connection to submit the job.
    /// </remarks>
    internal sealed class HpcQueryRuntime : IDisposable
    {
        private string m_headNode;
        private IScheduler m_scheduler;

        public string HostName { get { return m_headNode; } }

        public HpcQueryRuntime(string headNode){
            m_headNode = headNode;
            m_scheduler = new YarnScheduler();
            m_scheduler.Connect(m_headNode);
        }

        public void Dispose()
        {
            m_scheduler.Dispose();
        }

        // Return IScheduler reference for internal use
        internal IScheduler GetIScheduler()
        {
            return m_scheduler;
        }
    }
}
