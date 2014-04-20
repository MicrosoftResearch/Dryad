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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Dryad.LocalScheduler
{
    /// <summary>
    /// This is a placeholder for a computer that is free. It may be entered
    /// into multiple queues, for the computer itself, its rack, and the entire
    /// cluster. Once it gets paired with a process, its 'claimed' flag is set to
    /// true, but it is left in the other queues for simplicity; when it reaches
    /// the front of those queues it will be discarded if it has already been
    /// claimed
    /// </summary>
    internal class ProcessWaiter
    {
        /// <summary>
        /// the resource that is actually waiting. It is set to null after being claimed
        /// </summary>
        private Computer computer;

        /// <summary>
        /// this task is started when the object is paired with a Process, and returns
        /// that process
        /// </summary>
        private TaskCompletionSource<Process> waiter;

        /// <summary>
        /// construct a new object to represent a free computer with no scheduled process
        /// </summary>
        public ProcessWaiter(Computer target)
        {
            computer = target;
        }

        /// <summary>
        /// Called while the ProcessWaiter is locked, to see whether it has already been claimed
        /// </summary>
        public bool Unclaimed { get { return (computer != null); } }

        /// <summary>
        /// Called while the ProcessWaiter is locked: attempt to bind the computer to a process;
        /// this fails if it has already been bound to another process. The ProcessWaiter may
        /// be entered into multiple scheduling queues, and the first time it reaches the head
        /// of a queue it will be claimed successfully; it will be dropped after reaching the
        /// heads of other queues
        /// </summary>
        public Computer Claim()
        {
            System.Diagnostics.Debug.Assert(computer != null);
            Computer ret = computer;
            computer = null;
            return ret;
        }

        /// <summary>
        /// create the waiter task that can be used to block on the process
        /// being matched to a computer
        /// </summary>
        public Task<Process> Initialize()
        {
            waiter = new TaskCompletionSource<Process>();
            // add a continuation so that SetResult won't synchronously run the continuation
            // that the client awaits
            return waiter.Task.ContinueWith((t) => t.Result);
        }

        /// <summary>
        /// match a process to the computer; any method awaiting the Task returned
        /// by Initialize will be unblocked
        /// </summary>
        /// <param name="item">the process to be matched</param>
        public void Dispatch(Process item)
        {
            waiter.SetResult(item);
        }
    }

    /// <summary>
    /// datastructure to match schedulable processes to available computers
    /// </summary>
    internal class ProcessQueue
    {
        /// <summary>
        /// this becomes false when the queue is shutting down, at which point no more processes
        /// will be scheduled. This is used to avoid a race when removing a failed Computer from
        /// the cluster; when the computer is known to have failed its queue is set to inactive
        /// before processes queued to it are discarded
        /// </summary>
        bool active;

        /// <summary>
        /// queue of processes that are waiting to be scheduled. If processQueue is non-empty
        /// then waiterQueue must be empty
        /// </summary>
        private Queue<Process> processQueue;

        /// <summary>
        /// queue of computers that are waiting to be matched to processes. If waiterQueue
        /// is non-empty then processQueue must be empty
        /// </summary>
        private Queue<ProcessWaiter> waiterQueue;

        /// <summary>
        /// create a new datastructure for matching schedulable processes to available
        /// computers
        /// </summary>
        public ProcessQueue()
        {
            processQueue = new Queue<Process>();
            waiterQueue = new Queue<ProcessWaiter>();

            // start background cleaning tasks
            CleanProcessQueue();
            CleanWaiterQueue();

            active = true;
        }

        /// <summary>
        /// prevent the queue from accepting any more processes to be matched, and discard
        /// the ones that were waiting
        /// </summary>
        public void ShutDown()
        {
            Queue<Process> remaining;

            lock (this)
            {
                active = false;
                remaining = processQueue;
                processQueue = null;
            }

            foreach (var p in remaining)
            {
                lock (p)
                {
                    if (p.Unclaimed)
                    {
                        // if this was the last remaining queue the process
                        // was scheduled on, the upper layer will get notified
                        // that it is unscheduleable
                        p.DecrementQueueCount();
                    }
                }
            }
        }

        /// <summary>
        /// background thread to periodically remove any claimed processes, so we don't
        /// hang on to memory indefinitely
        /// </summary>
        private async void CleanProcessQueue()
        {
            while (true)
            {
                lock (this)
                {
                    if (processQueue == null)
                    {
                        // we have shut down, so exit this daemon
                        return;
                    }

                    Queue<Process> cleanedQueue = new Queue<Process>();
                    foreach (Process p in processQueue)
                    {
                        lock (p)
                        {
                            if (p.Unclaimed)
                            {
                                cleanedQueue.Enqueue(p);
                            }
                        }
                    }
                    processQueue = cleanedQueue;
                }

                // clean again in a second
                await Task.Delay(1000);
            }
        }

        /// <summary>
        /// background thread to periodically remove any claimed waiters, so we don't
        /// hang on to memory indefinitely
        /// </summary>
        private async void CleanWaiterQueue()
        {
            while (true)
            {
                lock (this)
                {
                    if (processQueue == null)
                    {
                        // we have shut down, so exit this daemon
                        return;
                    }

                    Queue<ProcessWaiter> cleanedQueue = new Queue<ProcessWaiter>();
                    foreach (ProcessWaiter w in waiterQueue)
                    {
                        lock (w)
                        {
                            if (w.Unclaimed)
                            {
                                cleanedQueue.Enqueue(w);
                            }
                        }
                    }
                    waiterQueue = cleanedQueue;
                }

                // clean again in a second
                await Task.Delay(1000);
            }
        }

        /// <summary>
        /// add a schedulable process. If there is an unclaimed computer waiting, the
        /// process will be assigned to the computer and the computer's Task will be
        /// unblocked. Returns true if the process has been matched (by this call or
        /// another asynchronous event in the meantime). Returns false if the process
        /// still needs to be matched.
        /// </summary>
        public bool AddProcess(Process process)
        {
            // waiter will exit the lock holding the value of the newly-claimed waiter, if
            // any. waiter may be non-null exiting the lock even if there was no
            // unclaimed waiter; the claimed flag below disambiguates
            ProcessWaiter waiter = null;

            // claimed will exit the lock set to true if and only if there was an unclaimed
            // waiter, in which case waiter will be set to the value of the waiter
            bool claimed = false;

            // lock ordering discipline is Queue first, then waiter, then process
            lock (this)
            {
                // if we are shutting down, return immediately
                if (!active)
                {
                    return false;
                }

                // even if there are waiters, they may have been claimed by processes
                // already, so use a loop here
                while (waiterQueue.Count > 0 && !claimed)
                {
                    // get the next available waiter; don't dequeue it yet because
                    // we have to wait until we acquire the locks below to figure out
                    // if it's going to be matched to anything
                    waiter = waiterQueue.Peek();

                    // lock ordering discipline is Queue first, then waiter, then process
                    lock (waiter)
                    {
                        if (waiter.Unclaimed)
                        {
                            // lock ordering discipline is Queue first, then waiter, then process
                            lock (process)
                            {
                                // another queue might have turned up and claimed the Process
                                // while we were dithering; matching a process to a computer
                                // must be done while both are locked
                                if (process.Unclaimed)
                                {
                                    // remove the waiter from the queue and match it to the process
                                    waiterQueue.Dequeue();
                                    var computer = waiter.Claim();
                                    process.Claim(computer);

                                    // break out of the loop
                                    claimed = true;
                                }
                                else
                                {
                                    // there's no point in continuing to try to add the process
                                    // since it has been claimed by someone else.
                                    // The waiter we Peek()ed is left in the queue for the next
                                    // process to match
                                    return true;
                                }
                            }
                        }
                        else
                        {
                            // the waiter that we Peek()ed above was already claimed so discard it
                            // and go around the loop again
                            waiterQueue.Dequeue();
                        }
                    }
                }

                // there were no unclaimed waiters, so add the process to the queue of
                // schedulable items while we're holding the queue lock
                if (!claimed)
                {
                    lock (process)
                    {
                        // let the process know it has been added to another queue
                        process.IncrementQueueCount();
                    }
                    processQueue.Enqueue(process);
                }
            }

            // exit the lock before triggering the wakeup of the computer
            if (claimed)
            {
                // this pairs the process with the computer
                waiter.Dispatch(process);
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// add a waiting computer. If there is an unclaimed process waiting, the
        /// process will be assigned to the computer and the computer's Task will be
        /// unblocked. Returns true if the waiter has been matched (by this call or
        /// another asynchronous event in the meantime). Returns false if the computer
        /// still needs to be matched.
        /// </summary>
        /// <param name="waiter">holder for the waiting computer</param>
        /// <returns></returns>
        public bool AddWaiter(ProcessWaiter waiter)
        {
            // process will exit the lock holding the value of the newly-claimed process, if
            // any. process may be non-null exiting the lock even if there was no
            // unclaimed process; the claimed flag below disambiguates
            Process process = null;

            // claimed will exit the lock set to true if and only if there was an unclaimed
            // waiter, in which case waiter will be set to the value of the waiter
            bool claimed = false;

            // lock ordering discipline is Queue first, then waiter, then process
            lock (this)
            {
                if (!active)
                {
                    // the queue has been shut down so don't accept another waiter
                    lock (waiter)
                    {
                        if (waiter.Unclaimed)
                        {
                            waiter.Claim();
                            // this will make us Dispatch the waiter with a null process below
                            // which is correct since the queue has been shut down, and will cause
                            // the waiting computer's commandloop to exit if it hasn't already
                            claimed = true;
                        }
                    }
                }

                // even if there are processes, they may have been claimed by other computers
                // already, so use a loop here
                while (active && processQueue.Count > 0 && !claimed)
                {
                    // get the next available process; don't dequeue it yet because
                    // we have to wait until we acquire the locks below to figure out
                    // if it's going to be matched to anything
                    process = processQueue.Peek();

                    // lock ordering discipline is Queue first, then waiter, then process
                    lock (waiter)
                    {
                        if (waiter.Unclaimed)
                        {
                            // lock ordering discipline is Queue first, then waiter, then process
                            lock (process)
                            {
                                // another queue might have turned up and claimed the Process
                                // while we were dithering; matching a process to a computer
                                // must be done while both are locked
                                if (process.Unclaimed)
                                {
                                    // remove the process from the queue and match it to the computer
                                    processQueue.Dequeue();
                                    var computer = waiter.Claim();
                                    process.Claim(computer);

                                    // break out of the loop
                                    claimed = true;
                                }
                                else
                                {
                                    // the process that we Peek()ed above was already claimed so discard it
                                    // and go around the loop again
                                    processQueue.Dequeue();
                                }
                            }
                        }
                        else
                        {
                            // there's no point in continuing to look for a process for the waiter
                            // since it has been claimed by someone else.
                            // The process we Peek()ed is left in the queue for the next
                            // waiter to match
                            return true;
                        }
                    }
                }

                // there were no unclaimed processes, so add the waiter to the queue of
                // waiting computers
                if (!claimed)
                {
                    waiterQueue.Enqueue(waiter);
                }
            }

            // exit the lock before triggering the wakeup of the computer
            if (claimed)
            {
                // this pairs the process with the computer
                waiter.Dispatch(process);
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
