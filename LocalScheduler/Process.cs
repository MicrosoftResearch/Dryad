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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;

using Microsoft.Research.Dryad;

namespace Microsoft.Research.Dryad.LocalScheduler
{
    /// </summary>
    /// This is the container class for a process as it makes its way through
    /// the scheduler lifecycle. It may start out in multiple queues, waiting
    /// for one of a set of particular computers or racks to be available.
    /// Once it gets assigned to a computer, its 'dispatched' flag is set to
    /// true, but it is left in the other queues for simplicity; when it reaches
    /// the front of those queues it will be discarded if it has already been
    /// dispatched
    /// </summary>
    internal class Process : ClusterInterface.ISchedulerProcess
    {
        /// <summary>
        /// this is called to let the cluster interface know when the schedule has succeeded or
        /// failed
        /// </summary>
        private ClusterInterface.RunProcess OnScheduledCallback;

        /// <summary>
        /// this is true until the process has been placed on all the queues it is
        /// going to be placed on. At that point, if the queueCount ever drop to
        /// zero we know it can't be scheduled, and return it to the upper layer
        /// </summary>
        private bool scheduling;

        /// <summary>
        /// the number of queues the process has been placed in. If this drops to
        /// zero after the process has been scheduled (because the computers it has
        /// been scheduled on all fail), it is returned as unschedulable to the upper
        /// layer
        /// </summary>
        private int queueCount;

        /// <summary>
        /// this is null before the process has been matched to a computer. It is
        /// assigned in TryToClaim to the first computer that claims it. Since the process
        /// may be placed in multiple scheduling queues, there may be subsequent (failed)
        /// calls to TryToClaim by other processes. If the process is canceled while it
        /// is being scheduler, this is set to a dummy (non-null) value ensuring that all queues
        /// will fail to claim it.
        /// </summary>
        private Computer owner;

        /// <summary>
        /// giud is a cluster-wide unique identifier for the process
        /// </summary>
        private string guid;

        /// <summary>
        /// construct a new object to represent the lifecycle of a process being scheduled
        /// </summary>
        public Process()
        {
            scheduling = true;
            queueCount = 0;
            owner = null;
            guid = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// a unique GUID representing the process for logging purposes
        /// </summary>
        public string Id { get { return guid; } }

        /// <summary>
        /// Called while the Process is locked, to see whether it has already been claimed
        /// </summary>
        public bool Unclaimed { get { return (owner == null); } }

        public void SetCallback(ClusterInterface.RunProcess callback)
        {
            OnScheduledCallback = callback;
        }

        public Task OnScheduled(ClusterInterface.IComputer computer, int processId, Task blocker, string errorReason)
        {
            return OnScheduledCallback(computer, processId, blocker, errorReason);
        }

        /// <summary>
        /// Called while the Process is locked: attempt to bind the process to a computer;
        /// this fails if it has already been bound to another computer. The Process may
        /// be entered into multiple scheduling queues, and the first time it reaches the head
        /// of a queue it will be claimed successfully; it will be dropped after reaching the
        /// heads of other queues
        /// </summary>
        public void Claim(Computer c)
        {
            System.Diagnostics.Debug.Assert(owner == null);
            owner = c;
        }

        /// <summary>
        /// Called while the Process is locked; increment the count of queues that the Process
        /// has been placed in for scheduling
        /// </summary>
        public void IncrementQueueCount()
        {
            ++queueCount;
        }

        /// <summary>
        /// Called while the Process is locked; decrement the count of queues that the Process
        /// has been placed in for scheduling
        /// </summary>
        public void DecrementQueueCount()
        {
            Debug.Assert(queueCount > 0);
            --queueCount;
            if (owner == null && !scheduling && queueCount == 0)
            {
                // the queue count has dropped to zero without the process being matched
                // since scheduling is false, there's no chance of adding it to any more
                // queues, so tell the watcher
                OnScheduledCallback(null, -1, null, "No computers in the cluster");
            }
        }

        /// <summary>
        /// Called while the Process is locked; signal that the Process will not be added
        /// to any more queues
        /// </summary>
        public void FinishedScheduling()
        {
            scheduling = false;
            if (owner == null && queueCount == 0)
            {
                // the queue count has dropped to zero without the process being matched
                // since scheduling is false, there's no chance of adding it to any more
                // queues, so tell the watcher
                OnScheduledCallback(null, -1, null, "No computers in the cluster");
            }
        }

        public Computer Location { get { return owner; } }
    }
}
