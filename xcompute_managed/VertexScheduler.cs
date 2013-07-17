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
    using System.Collections.Specialized;
    using System.Globalization;
    using System.Threading;
    using System.Diagnostics;

    using Microsoft.Research.Dryad;
    
    public class VertexScheduler
    {
        private ProcessTable processTable = null;
        private DispatcherPool dispatcherPool = new DispatcherPool();
        private DispatcherPool badDispatcherPool = new DispatcherPool();
        private RequestPool requestPool = new RequestPool();
        private ISchedulerHelper schedulerHelper = SchedulerHelperFactory.GetInstance();
        private VertexCallbackServiceHost callbackServiceHost;
        private JobStatus jobStatus = null;
        private string baseUri;
        private string replyUri;
        private int JobId = 0;
        private const int currentProcess = 1;
        private int processId = 0;
        private object dispatcherChangeLock = new object();

        #region Public Members

        public void CancelScheduleProcess(int processId)
        {
            DryadLogger.LogMethodEntry(processId);

            XComputeProcess proc = null;

            if (processTable.TryGetValue(processId, out proc) == false)
            {
                // We don't know about this process
                DryadLogger.LogWarning("Cancel process", "Attempt to cancel unknown process, id {0}", processId);
                return;
            }

            // Try to remove it from request pool (unassigned)
            if (requestPool.Cancel(processId))
            {
                DryadLogger.LogInformation("Cancel process", "Process request removed from request pool for process id {0}", processId);
                return;
            }

            // Handle already assigned processes
            proc.Cancel();
        }

        public void CloseVertexProcess(int processId)
        {
            XComputeProcess proc = null;

            if (processId == currentProcess)
            {
                // We don't maintain an entry in the process table for the current process
                return;
            }

            if (processTable.TryGetValue(processId, out proc))
            {
                // else if it's already assigned, release it at the node
                lock (proc.SyncRoot)
                {
                    if (proc.Dispatcher != null)
                    {
                        if (proc.CurrentState != ProcessState.Completed)
                        {
                            // This can happen when the GM cancels a process and closes the handle right afterward.
                            // We may not have received the state change from the cancellation yet.
                            // Note that the handle was closed by the GM, but do nothing else to avoid leaking a Dispatcher.
                            // ProcessExit will use this to know whether it also needs to close the handle.
                            DryadLogger.LogDebug("Close vertex process", "Closing handle for process id {0} in state {1} - delaying close until process exit", processId, proc.CurrentState);
                            proc.HandleClosed = true;
                        }
                        else
                        {
                            try
                            {
                                proc.Dispatcher.ReleaseProcess(processId);
                            }
                            finally
                            {
                                // Graph Manager is done with the process at this point is called so remove it from the table
                                processTable.Remove(processId);
                            }
                        }
                    }
                    else
                    {
                        DryadLogger.LogInformation("Close vertex process", "Dispatcher is null for process id {0} - it was either unscheduled or the dispatcher faulted", processId);
                    }
                }

            }
            else
            {
                DryadLogger.LogError(0, null, "Unknown process id {0}", processId);
            }

        }

        public void CreateVertexProcess(int processId)
        {
            XComputeProcess proc = new XComputeProcess(processId);
            this.processTable.Add(processId, proc);
            proc.ChangeState(ProcessState.Unscheduled);
        }

        public string CurrentProcessLocalPath
        {
            get
            {
                return ProcessPathHelper.ProcessPath(this.processId);
            }

        }

        public string CurrentProcessRemotePath
        {
            get
            {
                return GetProcessPath(this.processId, null);
            }
        }

        public string[] EnumerateProcessNodes()
        {
            return dispatcherPool.Nodes.ToArray();
        }

        public string GetAssignedNode(int processId)
        {
            // TODO: Need to fix for local executor if it's supported again
            if (processId == currentProcess)
            {
                return AzureUtils.CurrentHostName;
            }
            else if (this.processTable.ContainsKey(processId))
            {
                return this.processTable[processId].AssignedNode;
            }
            else
            {
                return null;
            }
        }

        public uint GetExitCode(int processId)
        {
            return this.processTable[processId].ExitCode;
        }

        public string GetProcessPath(int processId, string relativePath)
        {
            // TODO: Need to fix for local executor if it's supported again
            string node = GetAssignedNode(processId);
            if (String.IsNullOrEmpty(node))
            {
                return null;
            }
            else
            {
                string path = String.Format(@"\\{0}\{1}\{2}\{3}\{4}", node, Constants.DscTempShare, Environment.UserName, this.JobId, processId);

                if (relativePath != null && relativePath.Length > 0)
                {
                    path += @"\" + relativePath;
                }
                return path;
            }
        }

        public ProcessState GetProcessState(int processId)
        {
            if (processId == currentProcess)
            {
                return ProcessState.Running;
            }
            else if (this.processTable.ContainsKey(processId))
            {
                return this.processTable[processId].CurrentState;
            }
            else
            {
                return ProcessState.Completed;
            }
        }

        public bool IsGraphManager
        {
            get
            {
                return (processId == 1);
            }
        }

        public bool IsVertex
        {
            get
            {
                return (processId > 1);
            }
        }

        public bool IsVertexRerun
        {
            get
            {
                return (processId == 0);
            }
        }

        public JobStatus JobStatus
        {
            get { return this.jobStatus; }
        }

        public void NotifyStateChange(int processId, long timeoutInterval, ProcessState targetState, StateChangeEventHandler handler)
        {
            this.processTable[processId].AddStateChangeListener(targetState, timeoutInterval, handler);
        }

        public void ProcessChangeState(int processId, ProcessState newState)
        {
            XComputeProcess proc = null;
            if (this.processTable.TryGetValue(processId, out proc))
            {
                DryadLogger.LogDebug("Process Change State", "Process {0} changed to state {1}", processId, newState);
                if (newState == ProcessState.Running)
                {
                    // Need to ensure that the process transitions to AssignedToNode before
                    // transitioning to Running, or the GM gets mildly confused
                    ThreadPool.QueueUserWorkItem(new WaitCallback(proc.TransitionToRunning));
                }
                else
                {
                    proc.ChangeState(newState);
                }
            }
        }

        public void ProcessExit(int processId, int exitCode)
        {
            ProcessExit(processId, exitCode, false);
        }

        public bool ProcessCancelled(int processId)
        {
            if (processTable.ContainsKey(processId))
            {
                return processTable[processId].Cancelled;
            }
            return false;
        }

        public bool ScheduleProcess(int processId, string commandLine, List<SoftAffinity> softAffinities, string hardAffinity, StringDictionary environment)
        {
            bool retVal = false;

            processTable[processId].SetIdAndVersion(commandLine);
            DryadLogger.LogInformation("Schedule process", "Internal ID {0} corresponds to vertex {1}.{2}", processId, processTable[processId].GraphManagerId, processTable[processId].GraphManagerVersion);
            DryadLogger.LogInformation("Schedule process", "Internal ID {0} has a command line of {1}", processId,
                                       commandLine);

            if (environment == null)
            {
                environment = new StringDictionary();
            }
            environment[Constants.jobManager] = AzureUtils.CurrentHostName;
            environment["CCP_DRYADPROCID"] = processId.ToString(CultureInfo.InvariantCulture);

            ScheduleProcessRequest req = new ScheduleProcessRequest(processId, commandLine, softAffinities, hardAffinity, environment);
            Dispatcher dispatcher = null;

            // Take the request pool lock in case a ProcessExit comes in after we've looked for a node
            // but before the request has been added to the request pool.
            lock (requestPool.SyncRoot)
            {
                if (!FindNodeForRequest(req, out dispatcher))
                {
                    if (dispatcherPool.Count > 0)
                    {
                        DryadLogger.LogDebug("Schedule Process", "No nodes available, adding process {0} to request pool", processId);
                        requestPool.Add(req);
                        return true;
                    }
                    else
                    {
                        DryadLogger.LogCritical(0, null, "No available dispatchers");
                        return false;
                    }
                }
            }

            // Found a Dispatcher, schedule the request outside of the lock
            retVal = ScheduleProcess(req, dispatcher);
            if (!retVal)
            {
                processTable[processId].ChangeState(ProcessState.SchedulingFailed);
                dispatcher.Release();
            }

            return retVal;
        }

        public bool SetGetProps(int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics, GetSetPropertyEventHandler handler)
        {
            if (this.processTable.ContainsKey(processId))
            {
                if (infos != null && infos.Length > 0)
                {
                    // Only add for the first property info since we only want to fire completion once per request
                    this.processTable[processId].AddPropertyListener(infos[0].propertyLabel, infos[0].propertyVersion, handler);
                }
                else if (getPropLabel != null && getPropLabel.Length > 0)
                {
                    this.processTable[processId].AddPropertyListener(getPropLabel, 0, handler);
                }
                else
                {
                    DryadLogger.LogError(0, null, "infos and getPropLabel both empty");
                    return false;
                }

                lock (this.processTable[processId].SyncRoot)
                {
                    if (this.processTable[processId].Dispatcher != null)
                    {
                        if (this.processTable[processId].Dispatcher.SetGetProps(replyUri, processId, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics))
                        {
                            return true;
                        }
                    }
                }

                // Keep returning error to GM and let its fault-tolerance kick in
                if (dispatcherPool.Count == 0)
                {
                    DryadLogger.LogCritical(0, null, "All dispatchers are faulted.");
                }
                return false;
            }
            else
            {
                DryadLogger.LogError(0, null, "process id {0} not found in process table", processId);
                return false;
            }
        }

        public void SetGetPropsComplete(int processId, ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions)
        {
            XComputeProcess proc = null;
            if (processTable.TryGetValue(processId, out proc))
            {
                proc.SetGetPropsComplete(info, propertyLabels, propertyVersions);
            }
            else
            {
                DryadLogger.LogError(0, null, "process id {0} not found in process table", processId);
            }
        }

        /// <summary>
        /// When called from the GM, shuts down all the vertex services and closes the communication channels.
        /// When called from the vertex host, closes the communication channel to the local vertex service.
        /// </summary>
        /// <param name="ShutdownCode">Code to pass to the vertex services.  Currently unused.</param>
        public void Shutdown(uint ShutdownCode)
        {
            DryadLogger.LogMethodEntry(ShutdownCode);

            // If this is the GM, invoke Shutdown asynchronously to improve job shutdown time
            if (processId == 1)
            {
                // We no longer need to listen for task state changes
                schedulerHelper.StopTaskMonitorThread();

                lock (dispatcherPool.SyncRoot)
                {
                    foreach (Dispatcher disp in dispatcherPool)
                    {
                        DryadLogger.LogDebug("Shutdown", "Calling Shutdown on dispatcher for node {0}", disp.NodeName);
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        try
                        {
                            disp.Shutdown(0);
                            sw.Stop();
                        }
                        catch (Exception e)
                        {
                            sw.Stop();
                            DryadLogger.LogError(0, e, "Exception calling Shutdown on dispatcher for node {0}", disp.NodeName);
                        }

                        DryadLogger.LogDebug("Shutdown", "Dispatcher.Shutdown took {0} ms", sw.ElapsedMilliseconds);
                    }
                }
            }

            // Dispose the SchedulerHelper instance to clean up resources
            schedulerHelper.Dispose();
            schedulerHelper = null;

            // Clean out the dispatcher pool (this also disposes all dispatchers)
            dispatcherPool.Clear();

            // Stop the callback service
            callbackServiceHost.Stop();

            DryadLogger.LogMethodExit();
        }

        public bool WaitForStateChange(int processId, long timeoutInterval, ProcessState targetState)
        {
            DryadLogger.LogDebug("Wait for state change", "Process id: {0}, targetState: {1}", processId, targetState);
            if (this.processTable.ContainsKey(processId))
            {
                using (ManualResetEvent waitEvent = new ManualResetEvent(false))
                {
                    this.processTable[processId].AddStateChangeWaiter(targetState, waitEvent);
                    return waitEvent.WaitOne(TimeSpan.FromMilliseconds(timeoutInterval / 10), false);
                }
            }
            else
            {
                DryadLogger.LogError(0, null, "process id {0} not found in process table", processId);
                return false;
            }

        }

        #endregion

        #region Private Members

        private VertexScheduler(ProcessTable table)
        {
            this.processTable = table;
            this.jobStatus = new JobStatus(schedulerHelper);

            // These environment variables will not be set when the vertex rerun command is executed
            // Set them to 0 so we can use them later to detect that we're rerunning a vertex outside of an HPC job
            if (!Int32.TryParse(Environment.GetEnvironmentVariable("CCP_JOBID"), out JobId))
            {
                JobId = 0;
            }
            if (!Int32.TryParse(Environment.GetEnvironmentVariable("CCP_DRYADPROCID"), out processId))
            {
                processId = 0;
            }
            this.baseUri = String.Format(Constants.vertexCallbackAddrFormat, AzureUtils.CurrentHostName, processId);
            this.replyUri = this.baseUri + Constants.vertexCallbackServiceName;
            this.callbackServiceHost = new VertexCallbackServiceHost(this);
        }

        /// <summary>
        /// Create a new dispatcher and add to the good dispatcher pool.
        /// </summary>
        /// <param name="taskid">HPC Task Id</param>
        /// <param name="node">Name of node this dispatcher is for</param>
        /// <param name="state">State of task when dispatcher is created (always Running now)</param>
        /// <returns>Dispatcher that was added, or null if a dispatcher already exists in the good pool for specified node</returns>
        private Dispatcher AddDispatcher(int taskid, string node, VertexTaskState state)
        {
            VertexComputeNode cn = new VertexComputeNode();
            cn.instanceId = taskid;
            cn.ComputeNode = node;
            cn.State = state;
            Dispatcher d = new Dispatcher(schedulerHelper, cn);
            d.FaultedEvent += new DispatcherFaultedEventHandler(OnDispatcherFaulted);

            if (!dispatcherPool.Add(d))
            {
                // There's already a dispatcher for this node
                d.Dispose();
                d = null;
            }
            return d;
        }

        private bool FindRequestForNode(string node, out ScheduleProcessRequest req)
        {
            req = null;
            ulong maxAffinity = 0;
            bool result = false;
            Dispatcher dispatcher = null;
            Stopwatch swTotal = new Stopwatch();
            Stopwatch swSearch = new Stopwatch();
            Stopwatch swBlock = new Stopwatch();
            int requestCount = 0;

            swTotal.Start();
            if (dispatcherPool.TryReserveDispatcher(node, out dispatcher))
            {
                swBlock.Start();
                lock (requestPool.SyncRoot)
                {
                    swBlock.Stop();
                    swSearch.Start();
                    requestCount = requestPool.Count;
                    if (requestCount != 0)
                    {
                        foreach (ScheduleProcessRequest r in requestPool)
                        {
                            // Skip any lingering processes which have been cancelled.
                            if (processTable.ContainsKey(r.Id) && processTable[r.Id].Cancelled)
                            {
                                continue;
                            }

                            if (r.MustRunOnNode(node))
                            {
                                req = r;
                                DryadLogger.LogDebug("Find Request for Node", "process {0} has hard affinity constraint for node {1}", req.Id, node);
                                break;
                            }
                            else if (r.CanRunOnNode(node))
                            {
                                ulong thisAffinity = r.GetAffinityWeightForNode(node);
                                if (thisAffinity == 0 && req == null)
                                {
                                    req = r;
                                    DryadLogger.LogDebug("Find Request for Node", "Process {0} has 0 affinity constraint for node {1} but no other process has been selected yet", r.Id, node);
                                }
                                else if (thisAffinity > maxAffinity)
                                {
                                    maxAffinity = thisAffinity;
                                    req = r;
                                    DryadLogger.LogDebug("Find Request for Node", "Process {0} with affinity constraint {1} for node {2} larger than previous max", r.Id, thisAffinity, node);
                                }
                            }
                        }
                    }
                    swSearch.Stop();


                    if (req != null)
                    {
                        requestPool.Remove(req);
                        DryadLogger.LogDebug("Find Request for Node", "Found request {0} for node {1}", req.Id, node);
                        result = true;
                    }
                    else
                    {
                        DryadLogger.LogDebug("Find Request for Node", "Did not find any requests for node {0}", node);
                        dispatcher.Release();
                        result = false;
                    }
                }
            }
            swTotal.Stop();

            DryadLogger.LogInformation("Find Request for Node", "Searching {0} requests. Block {1} ms. Inner search {2} ms. Total elapsed time {3} ms.", 
                requestCount, swBlock.ElapsedMilliseconds, swSearch.ElapsedMilliseconds, swTotal.ElapsedMilliseconds);
            return result;
        }

        private bool FindNodeForRequest(ScheduleProcessRequest req, out Dispatcher dispatcher)
        {
            dispatcher = null;
            if (req.HardAffinity != null)
            {
                if (dispatcherPool.TryReserveDispatcher(req.HardAffinity, out dispatcher))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // First try soft affinity in decreasing order (assumes Soft Affinity list in req is sorted descending by weight)

                // Keep a map of the nodes we've already tried, because Dryad adds each affinity twice
                // once for the node and once for the "pod"
                Dictionary<string, bool> attemptedNodes = new Dictionary<string, bool>();
                int count = 0;

                for (int i = 0; i < req.AffinityCount; i++)
                {
                    if (attemptedNodes.ContainsKey(req.AffinityAt(i).Node.ToUpper()))
                    {
                        continue;
                    }
                    attemptedNodes.Add(req.AffinityAt(i).Node.ToUpper(), true);
                    count++;

                    if (dispatcherPool.TryReserveDispatcher(req.AffinityAt(i).Node, out dispatcher))
                    {
                        DryadLogger.LogDebug("Find Node For Request", "process {0} satisfied affinity constraint: node {1}, weight {2}", req.Id, req.AffinityAt(i).Node, req.AffinityAt(i).Weight);
                        return true;
                    }

                    DryadLogger.LogDebug("Find Node For Request", "process {0} did not satisfy affinity constraint: node {1}, weight {2}", req.Id, req.AffinityAt(i).Node, req.AffinityAt(i).Weight);
                }

                // If we get this far and AffinityCount > 0, then we failed to satisfy the affinity constraints
                // log a message so we can more easily detect this situation
                if (count > 0)
                {
                    DryadLogger.LogInformation("Find Node For Request", "process {0} failed to satisfy any of {1} affinity constraints", req.Id, count);
                }

                // Finally try any available node
                lock (dispatcherPool.SyncRoot)
                {
                    foreach (Dispatcher d in dispatcherPool)
                    {
                        if (req.CanRunOnNode(d.NodeName))
                        {
                            if (d.Reserve())
                            {
                                dispatcher = d;
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        private void Initialize()
        {
            if (IsVertexRerun)
            {
                // Vertex rerun command is being executed, don't create any dispatchers
                return;
            }
            else if (IsGraphManager)
            {
                VertexChangeEventHandler evtHandler = new VertexChangeEventHandler(OnVertexChanged);
                schedulerHelper.OnVertexChange += evtHandler;
                schedulerHelper.StartTaskMonitorThread();

                if (!schedulerHelper.WaitForTasksReady())
                {
                    // The graph manager will abort because we will not return any vertex nodes
                    DryadLogger.LogCritical(0, null, "Unable to begin job: too many vertex tasks failed");
                    schedulerHelper.OnVertexChange -= evtHandler;
                    dispatcherPool.Clear();
                    return;
                }

                // TODO: we need to be able to turn this off
                //ThreadPool.QueueUserWorkItem(new WaitCallback(VertexMonitorThreadFunc));
            }
            // IsVertex
            else
            {
                // On vertex nodes, create a dispatcher for the local vertex service and add an entry to the process table for the local process
                Dispatcher d = AddDispatcher(Int32.Parse(Environment.GetEnvironmentVariable(Constants.taskIdEnvVar)), "localhost", VertexTaskState.Running);
                XComputeProcess proc = new XComputeProcess(processId);
                lock (proc.SyncRoot)
                {
                    proc.Dispatcher = d;
                }

                this.processTable.Add(processId, proc);
            }
            callbackServiceHost.Start(this.baseUri, this.schedulerHelper);
        }

        /// <summary>
        /// When a dispatcher faults due to a communication error (as opposed to a task failure)
        /// it is moved to the bad dispatcher pool and a timer is set to retry the dispatcher
        /// after a predetermined interval. This method is called when that timer fires.
        /// </summary>
        /// <param name="state">The dispatcher to be retried</param>
        private void RetryFaultedDispatcher(object state)
        {
            DryadLogger.LogMethodEntry();

            Dispatcher newDispatcher = null;
            Dispatcher d = state as Dispatcher;
            if (d != null)
            {
                DryadLogger.LogDebug("Retry faulted dispatcher", "Creating new dispatcher for node {0}", d.NodeName);

                lock (dispatcherChangeLock)
                {
                    // Add a new dispatcher for this node
                    newDispatcher = AddDispatcher(d.TaskId, d.NodeName, VertexTaskState.Running);

                    // Get rid of the old dispatcher
                    badDispatcherPool.Remove(d);
                    d.Dispose();
                }

                if (newDispatcher != null)
                {
                    // Look for a request to run on this node
                    ThreadPool.QueueUserWorkItem(new WaitCallback(this.FindRequestForNodeThreadFunc), newDispatcher);
                }
            }
            else
            {
                DryadLogger.LogWarning("Retry faulted dispatcher", "state parameter not a valid dispatcher");
            }
            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// This is the event handler for the Dispatcher.FaultedEvent event.
        /// The FaultedEvent event is raised when a task transitions out of a running
        /// state, or when there is a communication error wich does not succeed after N retries.
        /// </summary>
        /// <param name="sender">The dispatcher raising the faulted event</param>
        /// <param name="e">Not used</param>
        private void OnDispatcherFaulted(object sender, EventArgs e)
        {
            Dispatcher d = sender as Dispatcher;
            if (d != null)
            {
                DryadLogger.LogWarning("Dispatcher Faulted", "Dispatcher for node '{0}' faulted due to {1}", d.NodeName, d.SchedulerTaskFailed ? "failed YARN Container" : "communication error");

                lock (dispatcherChangeLock)
                {
                    // Remove from dispatcher pool
                    dispatcherPool.Remove(d);

                    if (d.SchedulerTaskFailed)
                    {
                        // If we're faulting because the scheduler task transitioned to
                        // a non-running state, then we want to completely remove the dispatcher
                        badDispatcherPool.Remove(d);
                        d.Dispose();
                    }
                    else
                    {
                        // If we're faulting because of a communication error, then we want to
                        // add to bad dispatcher pool so that we'll retry it again
                        badDispatcherPool.Add(d);

                        // Set up a timer to move this dispatcher out of the bad pool in the future
                        d.SetRetryTimer(new TimerCallback(this.RetryFaultedDispatcher));
                    }
                }


                if (d.CurrentProcess != Dispatcher.InvalidProcessId)
                {
                    ProcessExit(d.CurrentProcess, unchecked((int)Constants.DrError_ProcessingInterrupted), true);
                }

            }
        }

        private void CheckForOutOfDispatchers()
        {
            if (badDispatcherPool.Count == 0 && dispatcherPool.Count == 0)
            {
                DryadLogger.LogError(0, null, "All vertex tasks have failed");
                lock (requestPool.SyncRoot)
                {
                    foreach (ScheduleProcessRequest r in requestPool)
                    {
                        XComputeProcess proc;
                        if (processTable.TryGetValue(r.Id, out proc))
                        {
                            DryadLogger.LogInformation("No Valid Dispatchers", "Transitioning process {0} to state {1} because all vertex tasks failed", r.Id, ProcessState.SchedulingFailed.ToString());
                            proc.ChangeState(ProcessState.SchedulingFailed);
                        }
                        else
                        {
                            DryadLogger.LogCritical(0, null, "Failed to find process {0} in process table, exiting application.", r.Id);
                            throw new ApplicationException(String.Format("All vertex tasks failed and unable to cancel pending request id {0}", r.Id));
                        }
                    }

                    requestPool.Clear();
                }
            }
        }

        /// <summary>
        /// This event handler is called from ISchedulerHelper task monitoring thread in response
        /// to an HPC Task state change.
        /// </summary>
        /// <param name="sender">Not used</param>
        /// <param name="e">Information about the task state transition</param>
        private void OnVertexChanged(object sender, VertexChangeEventArgs e)
        {
            Dispatcher oldDispatcher = null;
            Dispatcher newDispatcher = null;
            bool addNewDispatcher = false;
            bool faultOldDispatcher = false;

            lock (dispatcherChangeLock)
            {
                bool dispatcherFound = dispatcherPool.GetByTaskId(e.Id, out oldDispatcher);
                if (!dispatcherFound)
                {
                    // Check to see if this dispatcher was already faulted due to a communication error
                    dispatcherFound = badDispatcherPool.GetByTaskId(e.Id, out oldDispatcher);
                }


                // Task state change
                if (e.OldState != e.NewState)
                {
                    // Transitioning to, e.g., queued
                    if (e.NewState < VertexTaskState.Running)
                    {
                        DryadLogger.LogInformation("Vertex Task State Change", "Task {0} transitioned to waiting", e.Id);

                        // If there is a dispatcher for the task, then the task has previously been running.
                        // Now it's not, so we need to fault the dispatcher.
                        if (dispatcherFound)
                        {
                            DryadLogger.LogWarning("Vertex Task State Change", "Previously running task {0} transitioned to waiting", e.Id);
                            faultOldDispatcher = true;
                        }
                    }
                    // Transition to running
                    else if (e.NewState == VertexTaskState.Running)
                    {
                        if (!dispatcherFound)
                        {
                            // No dispatcher for task, add a new one
                            DryadLogger.LogInformation("Vertex Task State Change", "Task {0} transitioned to running", e.Id);
                            addNewDispatcher = true;
                        }
                        else if (String.Compare(e.OldNode, e.NewNode, StringComparison.OrdinalIgnoreCase) != 0)
                        {
                            // Dispatcher found, but task is now on a new node
                            // 1. Make sure old dispatcher is faulted.
                            // 2. Add a new one for the new node
                            DryadLogger.LogInformation("Vertex Task State Change", "Running task {0} assigned to new node", e.Id);

                            faultOldDispatcher = true;
                            addNewDispatcher = true;
                        }
                        else
                        {
                            // Dispatcher found, task is on same node
                            DryadLogger.LogWarning("Vertex Task State Change", "Change notification for running task {0}, but state and node are unchanged in notification", e.Id);
                        }
                    }
                    // Job is exiting, nothing to do
                    else if (e.NewState == VertexTaskState.Finished)
                    {
                        DryadLogger.LogDebug("Vertex Task State Change", "Task {0} transitioned to finished", e.Id);
                    }
                    // Failed or Cancelled
                    else
                    {
                        DryadLogger.LogWarning("Vertex Task State Change", "Task {0} transitioned to failed or cancelled", e.Id);

                        // Fault dispatcher if it isn't already
                        if (dispatcherFound)
                        {
                            faultOldDispatcher = true;
                        }
                    }
                }
                // Node change
                else if (String.Compare(e.OldNode, e.NewNode, StringComparison.OrdinalIgnoreCase) != 0)
                {
                    if (e.NewState == VertexTaskState.Running)
                    {
                        DryadLogger.LogDebug("Vertex Task State Change", "Task {0} moved from node {1} to node {2}", e.Id, e.OldNode, e.NewNode);
                        if (dispatcherFound)
                        {
                            faultOldDispatcher = true;
                            addNewDispatcher = true;
                        }
                    }
                }
                // Running -> Queued -> Running, e.g.
                else if (e.OldRequeueCount < e.NewRequeueCount)
                {
                    DryadLogger.LogDebug("Vertex Task State Change", "Task {0} node {1} state {2} unchanged from previous state: likely missed a state change notification.",
                        e.Id, e.NewNode, e.NewState.ToString());

                    // Was task running previously? If so, fault the old dispatcher.
                    if (dispatcherFound)
                    {
                        faultOldDispatcher = true;
                    }

                    // Is task running now? If so, create a new dispatcher to re-establish connection.
                    if (e.NewState == VertexTaskState.Running)
                    {
                        addNewDispatcher = true;
                    }
                }
            }

            if (faultOldDispatcher)
            {
                oldDispatcher.RaiseFaultedEvent(true);
            }

            if (addNewDispatcher)
            {
                newDispatcher = AddDispatcher(e.Id, e.NewNode, e.NewState);
                if (newDispatcher != null)
                {
                    // Look for new request for node
                    ThreadPool.QueueUserWorkItem(new WaitCallback(FindRequestForNodeThreadFunc), newDispatcher);
                }
                else
                {
                    DryadLogger.LogError(0, null, "Failed to add new dispatcher for node {0}", e.NewNode);
                }
            }

            if (faultOldDispatcher)
            {
                // Check to see if we have any dispatchers left. If not, we need to fail
                // everything in the request pool.
                CheckForOutOfDispatchers();
            }
        }

        private void ProcessExit(int processId, int exitCode, bool dispatcherFaulted)
        {
            DryadLogger.LogMethodEntry(processId, exitCode, dispatcherFaulted);
            try
            {
                XComputeProcess proc = null;
                if (processTable.TryGetValue(processId, out proc))
                {
                    DryadLogger.LogInformation("Process Exit", "found process {0} for vertex {1}.{2}", processId, proc.GraphManagerId, proc.GraphManagerVersion);

                    // Update process
                    if (proc.CurrentState < ProcessState.AssignedToNode && dispatcherFaulted)
                    {
                        // If we haven't yet reached AssignedToNode and the dispatcher faulted, then scheduling failed
                        DryadLogger.LogInformation("Process Exit", "Process {0} was in state {1}", processId, proc.CurrentState.ToString());

                        proc.ChangeState(ProcessState.SchedulingFailed);
                    }
                    else if (proc.CurrentState <= ProcessState.Running)
                    {
                        // If we're at AssignedToNode or Running, then the process either did really complete
                        // or the Vertex Service failed to start it - so this is not a scheduling error and the
                        // exit code has meaning.
                        DryadLogger.LogInformation("Process Exit", "Process {0} was in state {1}", processId, proc.CurrentState.ToString());
                        proc.ExitCode = (uint)exitCode;
                        proc.ChangeState(ProcessState.Completed);
                    }
                    else
                    {
                        // we've already reached this state previously, and this call should be idempotent
                        DryadLogger.LogInformation("Process Exit", "Process {0} was already in state {1}", processId, proc.CurrentState.ToString());
                        DryadLogger.LogMethodExit();
                        return;
                    }

                    if (proc.HandleClosed)
                    {
                        // This happens if a close handle comes from the GM
                        // before we've received notification that the process exited.
                        // For example, when the GM does:
                        // - Cancel
                        // - CloseHandle
                        // in rapid succession.
                        DryadLogger.LogDebug("Process Exit", "Delayed close handle for process {0}", processId);
                        CloseVertexProcess(processId);
                    }

                    lock (proc.SyncRoot)
                    {
                        if (dispatcherFaulted)
                        {
                            DryadLogger.LogWarning("Process Exit", "Process exiting due to faulted dispatcher");
                            proc.Dispatcher = null;
                        }
                        else if (proc.Dispatcher != null)
                        {
                            // Release dispatcher
                            DryadLogger.LogInformation("Process Exit", "Releasing dispatcher");
                            proc.Dispatcher.Release();

                            // Look for new request for node
                            ThreadPool.QueueUserWorkItem(new WaitCallback(this.FindRequestForNodeThreadFunc), proc.Dispatcher);
                        }
                    }
                }
                else
                {
                    DryadLogger.LogError(0, null, "Unknown process id {0}", processId);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Failed to transition vertex process {0} to exited gracefully", processId);
            }
            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// This is the callback method for the async ScheduleProcess operation.
        /// </summary>
        /// <param name="asyncResult">AsyncState member is the Dispatcher that initiated the operation</param>
        private void ScheduleProcessCallback(IAsyncResult asyncResult)
        {
            try
            {
                Dispatcher d = asyncResult.AsyncState as Dispatcher;
                if (d != null)
                {
                    int currentProcessId = d.CurrentProcess;
                    SchedulingResult schedulingResult = d.EndScheduleProcess(asyncResult);
                    if (schedulingResult == SchedulingResult.Failure)
                    {
                        // This indicates there was a fatal error (Exception or FaultException)

                        // Change process state to scheduling failed
                        DryadLogger.LogWarning("Schedule Process", "Async operation did not complete successfully for process {0} on node {1}", currentProcessId, d.NodeName);
                        if (currentProcessId != Dispatcher.InvalidProcessId)
                        {
                            // Since we will still be in the Unscheduled state, the return code will be ignored by
                            // ProcessExit, but we'll pass a nonzero exit code just to be sure we don't
                            // confuse the GM in case of a race condition.
                            ProcessExit(currentProcessId, unchecked((int)Constants.DrError_ProcessingInterrupted));
                        }
                        d.Release();
                    }
                    else if (schedulingResult == SchedulingResult.CommunicationError)
                    {
                        // This indicates that there was an error communicating with the node.

                        // We need to fault the dispatcher so that subsequent attemps don't try to use it again.
                        // Faulting the dispatcher will take care of exiting the current process, so no need to
                        // do it here.

                        DryadLogger.LogWarning("Schedule Process", "Async operation failed due to communication error for process {0} on node {1}", currentProcessId, d.NodeName);
                        d.RaiseFaultedEvent(false);
                    }
                    else if (schedulingResult == SchedulingResult.Pending)
                    {
                        // Nothing to do for this case -it indicates there was a problem and we're retrying
                    }
                    else if (schedulingResult == SchedulingResult.Success)
                    {
                        // Process has been scheduled, transition to AssignedToNode state
                        if (currentProcessId != Dispatcher.InvalidProcessId)
                        {
                            DryadLogger.LogInformation("Schedule Process", "Process {0} successfully scheduled on node {1}", currentProcessId, d.NodeName);
                            processTable[currentProcessId].ChangeState(ProcessState.AssignedToNode);
                        }
                    }
                }
                else
                {
                    DryadLogger.LogWarning("Schedule Process", "Dispatcher not passed correctly to callback");
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Schedule Process", "Schedule process callback threw exception: {0}", e.ToString());
            }
        }

        private bool ScheduleProcess(ScheduleProcessRequest request, Dispatcher dispatcher)
        {
            lock (processTable.SyncRoot)
            {
                lock (this.processTable[request.Id].SyncRoot)
                {
                    processTable[request.Id].Dispatcher = dispatcher;
                }
            }

            if (dispatcher.ScheduleProcess(replyUri, request, new AsyncCallback(this.ScheduleProcessCallback)))
            {
                DryadLogger.LogInformation("Schedule Process", "Began asynchronous scheduling of process {0} on node '{1}': '{2}'", request.Id, dispatcher.NodeName, request.CommandLine);
                return true;
            }
            else
            {
                DryadLogger.LogWarning("Schedule Process", "Failed to begin asynchronous scheduling of process {0} on node '{1}'", request.Id, dispatcher.NodeName);
                return false;
            }
        }

        private void FindRequestForNodeThreadFunc(Object state)
        {
            Dispatcher d = state as Dispatcher;

            ScheduleProcessRequest req = null;
            // FindRequestForNode takes a lock on the request pool
            try
            {
                if (d != null)
                {
                    if (FindRequestForNode(d.NodeName, out req))
                    {
                        if (!ScheduleProcess(req, d))
                        {
                            DryadLogger.LogWarning("Schedule Request on Node", "Failed to schedule process {0} on node {1}", req.Id, d.NodeName);
                            processTable[req.Id].ChangeState(ProcessState.SchedulingFailed);
                            d.Release();
                        }
                    }
                }
            }
            catch (NullReferenceException)
            {
                if (d == null)
                {
                    // Dispatcher has been faulted and set to null. Ignore.
                }
                else
                {
                    throw;
                }
            }
        }

#if false
        // This thread is not currently used
        private void VertexMonitorThreadFunc(Object state)
        {
            do
            {
                using (System.IO.StreamWriter sw = new System.IO.StreamWriter("vertex_health.txt", true))
                {
                    sw.AutoFlush = true;

                    List<Dispatcher> dlist = new List<Dispatcher>();
                    lock (dispatcherPool.SyncRoot)
                    {
                        foreach (Dispatcher d in dispatcherPool)
                        {
                            dlist.Add(d);
                        }
                    }

                    Process proc = Process.GetCurrentProcess();
                    
                    sw.WriteLine("<CheckPoint>");
                    sw.WriteLine("    <Timestamp>{0}</Timestamp>", DateTime.Now);
                    sw.WriteLine("    <MainModule>{0}</MainModule>", proc.MainModule);
                    sw.WriteLine("    <StartTime>{0}</StartTime>", proc.StartTime);
                    sw.WriteLine("    <VirtualMemorySize64>{0}</VirtualMemorySize64>", proc.VirtualMemorySize64);
                    sw.WriteLine("    <WorkingSet64>{0}</WorkingSet64>", proc.WorkingSet64);                    

                    foreach (Dispatcher d in dlist)
                    {
                        sw.WriteLine("    <Dispatcher>");                    
                        sw.WriteLine("        <Name>{0}</Name>", d.NodeName);                    
                        sw.WriteLine("        <Idle>{0}</Idle>", d.Idle);                    
                        sw.WriteLine("        <Faulted>{0}</Faulted>", d.Faulted);
                        sw.WriteLine("        <ConnectionAttempts>{0}</ConnectionAttempts>", d.ConnectionAttempts);
                       
                        if (!d.Faulted)
                        {
                            VertexStatus status = d.CheckStatus();
                            sw.WriteLine("        <Alive>{0}</Alive>", status.serviceIsAlive);                            
                            if (status.serviceIsAlive)
                            {
                                sw.WriteLine("        <ProcessCount>{0}</ProcessCount>", status.runningProcessCount);                    
                                sw.WriteLine("        <FreePhysMem>{0}</FreePhysMem>", status.freePhysicalMemory);                    
                                sw.WriteLine("        <FreeVirtMem>{0}</FreeVirtMem>", status.freeVirtualMemory);                    
                                foreach (KeyValuePair<string, ulong> kvp in status.freeDiskSpaces)
                                {
                                    //sw.WriteLine("    Disk: {0}, Free space = {1}", kvp.Key, kvp.Value);
                                }

                                foreach (VertexProcessInfo vpi in status.vps)
                                {
                                    sw.WriteLine("            <VertexProcess>");                    
                                    sw.WriteLine("                <CommandLine>{0}</CommandLine>", vpi.commandLine);                    
                                    sw.WriteLine("                <Id>{0}</Id>", vpi.DryadId);                    
                                    sw.WriteLine("                <State>{0}</State>", vpi.State);                    
                                    sw.WriteLine("            </VertexProcess>");                    
                                }                                
                            }
                        }                        
                        sw.WriteLine("    </Dispatcher>");
                        
                    }
                    sw.WriteLine("</CheckPoint>");

                }

                // Let the GM fault tolerance handle this
                if (dispatcherPool.Count == 0)
                {
                    DryadLogger.LogCritical(0, null, "No reachable dispatchers");
                }
                
                Thread.Sleep(1000 * 60);
            
            } while (true);
        }
#endif

        #endregion


        #region Factory Methods

        private static VertexScheduler vertexScheduler = null;
        private static Object factoryLock = new Object();

        public static VertexScheduler GetInstance()
        {
            if (vertexScheduler == null)
            {
                lock (factoryLock)
                {
                    if (vertexScheduler == null)
                    {
                        ProcessTable processTable = new ProcessTable();

                        vertexScheduler = new VertexScheduler(processTable);

                        vertexScheduler.Initialize();
                    }
                }
            }
            return vertexScheduler;
        }

        #endregion
    }
}
