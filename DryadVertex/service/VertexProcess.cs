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
//      Wrapper around vertex host
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Diagnostics;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Threading;
    using System.IO;
    using Microsoft.Research.Dryad;

    class VertexProcess : IDisposable
    {
        #region members

        private int dryadProcessId;
        private string graphManagerReplyUri;
        public  string commandLine;
        private StringDictionary environment;
        private string localAddress;
        private bool failed = false;
        private bool cancelled = false;
        private bool exited = false;
        private int exitCode = 0;

        private ManualResetEvent processStartEvent = new ManualResetEvent(false);

        private Process systemProcess;
        private ProcessState state = ProcessState.AssignedToNode;
        private ProcessInfo info = new ProcessInfo();
        private long peakWorkingSet = 0;

        private Dictionary<string, Dictionary<ulong, ManualResetEvent>> propertyWaitEvents = new Dictionary<string, Dictionary<ulong, ManualResetEvent>>();
        private int propertyWaiters = 0;

        private bool finalStatusMessageSent = false;
        private ProcessPropertyInfo latestVertexStatusReceived;
        private ProcessPropertyInfo latestVertexStatusSent;
        
        public Object syncRoot = new Object();
        private bool m_disposed = false;

        #endregion

        #region Constructors

        /// <summary>
        /// Instantiates a new instance of the VertexProcess Class. Saves parameters.
        /// </summary>
        /// <param name="uri">GM URI</param>
        /// <param name="id">Vertex ID</param>
        /// <param name="cmd">Vertex cmd line args</param>
        /// <param name="env">Environment variables for vertex host</param>
        /// <param name="localAddr">VS URI</param>
        public VertexProcess(string uri, int id, string cmd, StringDictionary env, string localAddr)
        {
            dryadProcessId = id;
            commandLine = cmd;
            environment = env;
            graphManagerReplyUri = uri;
            localAddress = localAddr;
        }

        #endregion

        #region Public methods

        //
        // Asynchronously start vertex process
        //
        public bool Start(ManualResetEvent serviceInitializedEvent)
        {
            DryadLogger.LogMethodEntry(this.DryadId);

            bool result = ThreadPool.QueueUserWorkItem(new WaitCallback(StartProcessThreadProc), serviceInitializedEvent);

            DryadLogger.LogMethodExit(result);
            return result;
        }

        /// <summary>
        /// Set process state to cancelled and stop the vertex host process if possible
        /// </summary>
        public void Cancel(bool suppressNotifications)
        {
            DryadLogger.LogMethodEntry(this.DryadId);

            lock (syncRoot)
            {
                if (state == ProcessState.Completed)
                {
                    // Process has already completed before cancelation made it here, do nothing
                    DryadLogger.LogInformation("Cancel process", "Process {0} has already exited", DryadId);
                    DryadLogger.LogMethodExit();
                    return;
                }
                DryadLogger.LogInformation("Cancel process", "Process {0} has not already exited", DryadId);
                state = ProcessState.Completed;
                this.cancelled = true;
            }

            // If the process started, kill it
            if (systemProcess != null)
            {
                try
                {
                    // Killing the process will trigger Process_Exited
                    DryadLogger.LogInformation("Cancel process", "Killing system process for process id {0}", DryadId);

                    if (suppressNotifications)
                    {
                        // Remove the Exited event handler
                        systemProcess.Exited -= this.Process_Exited;
                    }
                    systemProcess.Kill();
                    DryadLogger.LogMethodExit();
                    return;
                }
                catch (Exception e)
                {
                    //
                    // Failed to kill process - log exception
                    //
                    DryadLogger.LogError(0, e, "Failed to kill system process for process id {0}", DryadId);
                }
            }
            else
            {
                DryadLogger.LogInformation("Cancel process", "Process {0} has not started yet", DryadId);
            }

            // Process was either not running or failed to die, trigger Process_Exited ourself
            if (!suppressNotifications)
            {
                Process_Exited(this, null);
            }
            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            DryadLogger.LogMethodEntry(this.DryadId);

            Dispose(true);
            GC.SuppressFinalize(this);

            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Creates a request for property update from one vertex to another (one vertex is usually GM)
        /// </summary>
        /// <param name="replyEpr">Address to send response to</param>
        /// <param name="infos"></param>
        /// <param name="blockOnLabel"></param>
        /// <param name="blockOnVersion"></param>
        /// <param name="maxBlockTime"></param>
        /// <param name="getPropLabel"></param>
        /// <param name="ProcessStatistics"></param>
        /// <returns>Returns success/failure of thread startup</returns>
        public bool SetGetProps(string replyEpr, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics)
        {
            DryadLogger.LogMethodEntry(replyEpr, this.DryadId);

            //
            // Check if graph manager is the one calling. If so, increment propertyWaiter count
            //
            if (ReplyDispatcher.IsGraphMrgUri(replyEpr))
            {
                int n = Interlocked.Increment(ref propertyWaiters);
            }

            //
            // Create new property request with provided parameters and queue up request sending
            //
            PropertyRequest req = new PropertyRequest(replyEpr, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics);

            bool result = ThreadPool.QueueUserWorkItem(new WaitCallback(SetGetPropThreadProc), req);

            DryadLogger.LogMethodExit(result);
            return result;
        }

        #endregion

        #region Private methods

        /// <summary>
        /// Adds specified property to property wait list and waits for it.
        /// </summary>
        /// <param name="blockOnLabel">Property label to wait for</param>
        /// <param name="blockOnVersion">Version of property to wait for</param>
        /// <param name="maxBlockTime">Time to wait for property</param>
        /// <returns>False if property was requested but none was returned</returns>
        private bool BlockOnProperty(string blockOnLabel, ulong blockOnVersion, long maxBlockTime)
        {
            DryadLogger.LogMethodEntry();

            //
            // Return true if no label is provided
            //
            if (String.IsNullOrEmpty(blockOnLabel))
            {
                DryadLogger.LogMethodExit(true);
                return true;
            }

            DryadLogger.LogInformation("Block on property", "Label {0} Version {1} maxBlockTime {2}", blockOnLabel, blockOnVersion, maxBlockTime);

            ProcessPropertyInfo prop = null;

            //
            // If the process already exited, don't bother adding a wait event for
            // this property - if it's not already set it never will be.
            //

            lock (syncRoot)
            {
                if (!exited)
                {
                    //
                    // Add this label and version to the wait events list if needed
                    //
                    if (propertyWaitEvents.ContainsKey(blockOnLabel) == false)
                    {
                        propertyWaitEvents.Add(blockOnLabel, new Dictionary<ulong, ManualResetEvent>());
                    }

                    if (propertyWaitEvents[blockOnLabel].ContainsKey(blockOnVersion) == false)
                    {
                        propertyWaitEvents[blockOnLabel].Add(blockOnVersion, new ManualResetEvent(false));
                    }
                }
                else
                {
                    DryadLogger.LogInformation("Block on property", "Process {0} already exited, not adding waiter", this.DryadId);
                }
            }

            // todo: We still may want to implement timeouts to deal with deadlocks in the service / host but it hasn't been an issue yet.
            //if (propertyWaitEvents[blockOnLabel][blockOnVersion].WaitOne(new TimeSpan(maxBlockTime), false))

            //
            // Wait forever (or until process exits or is disposed) for the property to be set or interrupted
            //

            while (!exited)
            {
                try
                {
                    if (propertyWaitEvents[blockOnLabel][blockOnVersion].WaitOne(100, false))
                    {
                        break;
                    }
                }
                catch (ObjectDisposedException)
                {
                    DryadLogger.LogWarning("Block on property", "Process {0} disposed while waiting for label {1}, version {2}", DryadId, blockOnLabel, blockOnVersion);
                    DryadLogger.LogMethodExit(false);
                    return false;
                }
            }

            // Did we get the property, or did the process
            // terminate?
            int index;
            if (TryGetProperty(blockOnLabel, out prop, out index))
            {
                //
                // If a property was successfully returned, return true
                //
                if ((blockOnVersion == 0) || (prop.propertyVersion > blockOnVersion))
                {
                    DryadLogger.LogMethodExit(true);
                    return true;
                }

                if (state == ProcessState.Completed)
                {
                    DryadLogger.LogInformation("Block on property", "Vertex completed (wait) requested version:{0} returned version:{1} of label {2}", blockOnVersion, prop.propertyVersion, blockOnLabel);
                    DryadLogger.LogMethodExit(true);
                    return true;
                }
            }

            //
            // Return false if property was requested but none was found
            //
            DryadLogger.LogMethodExit(false);
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="disposing"></param>
        private void Dispose(bool disposing)
        {
            DryadLogger.LogMethodEntry(disposing);
            if (!this.m_disposed)
            {
                lock (syncRoot)
                {
                    if (!this.m_disposed)
                    {
                        if (disposing)
                        {
                            // Close start event handle
                            try
                            {
                                processStartEvent.Close();
                            }
                            catch (Exception ex)
                            {
                                DryadLogger.LogError(0, ex);
                            }

                            // Close any get/set property wait handles
                            foreach (KeyValuePair<string, Dictionary<ulong, ManualResetEvent>> label in propertyWaitEvents)
                            {
                                foreach (KeyValuePair<ulong, ManualResetEvent> version in label.Value)
                                {
                                    try
                                    {
                                        version.Value.Close();
                                    }
                                    catch (Exception ex)
                                    {
                                        DryadLogger.LogError(0, ex);
                                    }
                                }
                            }
                            propertyWaitEvents.Clear();
                        }

                        m_disposed = true;
                    }
                }
            }
            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Record memory and CPU statistics for vertex host process
        /// </summary>
        /// <param name="stats">requested statistics</param>
        /// <returns>true on success</returns>
        private bool GetStatistics(out ProcessStatistics stats)
        {
            stats = new ProcessStatistics();

            // These are the only statistics returned by the xcexec implementation
            UpdateMemoryStatistics();
            stats.processUserTime = systemProcess.UserProcessorTime.Ticks * 10 * TimeSpan.TicksPerMillisecond;
            stats.processKernelTime = (systemProcess.TotalProcessorTime.Ticks * 10 * TimeSpan.TicksPerMillisecond) - stats.processUserTime;
            stats.peakMemUsage = (ulong)peakWorkingSet;
            return true;
        }

        /// <summary>
        /// Set the property wait events for all versions of all properties
        /// </summary>
        private void SetAllPropertyWaiters()
        {
            lock (syncRoot)
            {
                // signal all threads waiting for properties
                foreach (KeyValuePair<string, Dictionary<ulong, ManualResetEvent>> label in this.propertyWaitEvents)
                {
                    foreach (KeyValuePair<ulong, ManualResetEvent> version in label.Value)
                    {
                        version.Value.Set();
                    }
                }
            }
        }
        
        /// <summary>
        /// Set all requested properties
        /// </summary>
        /// <param name="infos">Properties to set</param>
        /// <param name="labels">Output - labels of properties set</param>
        /// <param name="versions">Output - version of properties set</param>
        private void SetProperties(ProcessPropertyInfo[] infos, out string[] labels, out ulong[] versions)
        {
            //
            // Return null if infos list contains no properties
            //
            labels = null;
            versions = null;
            if (infos != null && infos.Length > 0)
            {
                versions = new ulong[infos.Length];
                labels = new string[infos.Length];

                for (int i = 0; i < infos.Length; i++)
                {
                    //
                    // Set each property and update version and label
                    //
                    ulong newVersion = 0;
                    SetProperty(infos[i], out newVersion);
                    versions[i] = newVersion;
                    labels[i] = infos[i].propertyLabel;
                }
            }
        }

        /// <summary>
        /// Set a property and record the version
        /// </summary>
        /// <param name="property"></param>
        /// <param name="newVersion"></param>
        private void SetProperty(ProcessPropertyInfo property, out ulong newVersion)
        {
            DryadLogger.LogMethodEntry(property.propertyLabel);
            ProcessPropertyInfo oldProperty = null;
            lock (syncRoot)
            {
                int index;
                if (TryGetProperty(property.propertyLabel, out oldProperty, out index))
                {
                    //
                    // If property found in local array, then we are setting a new version of existing property
                    // Copy the new property information into the array
                    //
                    oldProperty.propertyVersion++;
                    newVersion = oldProperty.propertyVersion;
                    if (property.propertyBlock != null && property.propertyBlock.Length > 0)
                    {
                        oldProperty.propertyBlock = property.propertyBlock;
                    }

                    oldProperty.propertyString = property.propertyString;
                    CopyProp(oldProperty, out info.propertyInfos[index]);
                }
                else
                {
                    //
                    // if property not found in local array, then setting a new property
                    // use version 1, unless valid value specified
                    //
                    if (property.propertyVersion == ulong.MaxValue || property.propertyVersion == 0)
                    {
                        property.propertyVersion = 1;
                    }

                    newVersion = property.propertyVersion;

                    //
                    // Create or resize the local info array as necessary and append this property
                    //
                    if (info.propertyInfos == null)
                    {
                        info.propertyInfos = new ProcessPropertyInfo[1];
                    }
                    else
                    {
                        Array.Resize(ref info.propertyInfos, info.propertyInfos.Length + 1);
                    }

                    info.propertyInfos[info.propertyInfos.Length - 1] = property;
                }

                //
                // If there was a vertex completed event, record the latest vertex status
                //
                if (StatusMessageContainsDryadError_VertexCompleted( property.propertyLabel))
                {
                    CopyProp(property, out latestVertexStatusReceived);
                    latestVertexStatusReceived.propertyVersion = newVersion;
                }
                
                //
                // Wake up anyone waiting for a property change by adding a new wait event for this property if needed
                //
                if (propertyWaitEvents.ContainsKey(property.propertyLabel) == false)
                {
                    propertyWaitEvents.Add(property.propertyLabel, new Dictionary<ulong, ManualResetEvent>());
                }
                
                //
                // Wake up anyone waiting for this version of the property
                //
                if (propertyWaitEvents[property.propertyLabel].ContainsKey(newVersion - 1))
                {
                    propertyWaitEvents[property.propertyLabel][newVersion - 1].Set();
                }
                else
                {
                    propertyWaitEvents[property.propertyLabel].Add(newVersion - 1, new ManualResetEvent(true));
                }

                //
                // Wake up anyone waiting for any version of this property
                //
                if (newVersion > 1)
                {
                    if (propertyWaitEvents[property.propertyLabel].ContainsKey(0))
                    {
                        propertyWaitEvents[property.propertyLabel][0].Set();
                    }
                }
            }
            DryadLogger.LogMethodExit();
        }
        
        /// <summary>
        /// Copy the information from one ProcessPropertyInfo object to another
        /// </summary>
        /// <param name="propertySrc">Source of info</param>
        /// <param name="propertyDst">destination of info</param>
        private void CopyProp(ProcessPropertyInfo propertySrc, out ProcessPropertyInfo propertyDst)
        {
            propertyDst = new ProcessPropertyInfo();
            propertyDst.propertyLabel = propertySrc.propertyLabel;
            propertyDst.propertyVersion = propertySrc.propertyVersion;
            propertyDst.propertyString = propertySrc.propertyString;
            if (propertySrc.propertyBlock != null)
            {
                propertyDst.propertyBlock = new byte[ propertySrc.propertyBlock.Length ];
                Array.Copy(propertySrc.propertyBlock, propertyDst.propertyBlock, propertySrc.propertyBlock.Length);
            }
        }

        /// <summary>
        /// Looks at current list of properties to get latest information
        /// </summary>
        /// <param name="getPropLabel">property label</param>
        /// <param name="property">property info output</param>
        /// <param name="index">property info index where found if found</param>
        /// <returns>found property = true</returns>
        private bool TryGetProperty(string getPropLabel, out ProcessPropertyInfo property, out int index)
        {
            index = 0;
            property = null;
            if (info.propertyInfos != null)
            {
                lock (syncRoot)
                {
                    //
                    // Look through each known property for one sharing the same label.
                    //
                    foreach (ProcessPropertyInfo p in info.propertyInfos)
                    {
                        if (String.Compare(p.propertyLabel, getPropLabel, StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            //
                            // If found, set output parameter and return true
                            //
                            CopyProp(p, out property);
                            return true;
                        }

                        index ++;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Update vertex host process information
        /// </summary>
        private void UpdateMemoryStatistics()
        {
            try
            {
                systemProcess.Refresh();
                peakWorkingSet = systemProcess.PeakWorkingSet64;
            }
            catch
            {
                // Process has exited
            }
        }

        #endregion

        #region Properties

        public int DryadId
        {
            get { return this.dryadProcessId; }
        }

        public int ProcessId
        {
            get { return this.systemProcess.Id; }
        }

        public ProcessState State
        {
            get { return this.state; }
        }

        public bool Succeeded
        {
            get { return ((this.exited) && !(this.cancelled || this.failed)); }
        }

        #endregion

        #region Event handlers

        /// <summary>
        /// Vertex host process exited event - marks process state and queues up exit process thread
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        private void Process_Exited(object sender, EventArgs args)
        {
            DryadLogger.LogMethodEntry(DryadId);

            // Ensure the process exited code can only be executed once
            lock (syncRoot)
            {
                if (exited)
                {
                    DryadLogger.LogInformation("Process exit", "Process {0} already exited", DryadId);
                    DryadLogger.LogMethodExit();
                    return;
                }
                exited = true;
            }

            if (cancelled)
            {
                DryadLogger.LogInformation("Process exit", "Process {0} was cancelled", DryadId);
                exitCode = unchecked((int)0x830A0003); // DrError_VertexReceivedTermination
            }
            else
            {
                exitCode = systemProcess.ExitCode;
                DryadLogger.LogInformation("Process exit", "Process {0} exit code {1}", DryadId, exitCode);
                if (exitCode == 0)
                {
                    lock (syncRoot)
                    {
                        state = ProcessState.Completed;
                    }
                }
                else
                {
                    lock (syncRoot)
                    {
                        state = ProcessState.Completed;
                        this.failed = true;
                    }
                }
            }

            //
            // Ensure that the vertex complete event is sent to GM and that all pending properties are handled
            //
            ThreadPool.QueueUserWorkItem(new WaitCallback(ExitProcessThreadProc));

            DryadLogger.LogMethodExit();
        }

        #endregion

        #region Thread functions

        /// <summary>
        /// Asynchronously called on start command
        /// </summary>
        /// <param name="obj"></param>
        void StartProcessThreadProc(Object obj)
        {
            ManualResetEvent serviceInitializedEvent = obj as ManualResetEvent;
            bool started = false;

            try
            {
                //
                // Wait for service initialization
                //
                serviceInitializedEvent.WaitOne();

                if (ExecutionHelper.InitializeForProcessExecution(dryadProcessId, Environment.GetEnvironmentVariable("XC_RESOURCEFILES")))
                {
                    //
                    // Vertex working directory configured successfully, start the vertex host
                    //
                    environment.Add(Constants.vertexSvcLocalAddrEnvVar, localAddress);

                    ProcessStartInfo startInfo = new ProcessStartInfo();
                    startInfo.CreateNoWindow = true;
                    startInfo.UseShellExecute = false;
                    startInfo.WorkingDirectory = ProcessPathHelper.ProcessPath(dryadProcessId);

                    //YARN Debugging
                    //var procEnvVarKeys = startInfo.EnvironmentVariables.Keys;
                    //foreach (string key in procEnvVarKeys)
                    //{
                    //    DryadLogger.LogInformation("StartProcess", "key: '{0}' value: '{1}'", key, startInfo.EnvironmentVariables[key]);
                    //}

                    string[] args = commandLine.Split(' '); 
                    string arg = "";
                    for (int i = 1; i < args.Length; i++)
                    {
                        arg += args[i] + " ";
                    }
                    
                    //
                    // Use either FQ path or path relative to job path  
                    //
                    if (Path.IsPathRooted(args[0]))
                    {
                        startInfo.FileName = args[0];
                    }
                    else
                    {
                        startInfo.FileName = Path.Combine(ProcessPathHelper.JobPath, args[0]);
                    }
                    DryadLogger.LogInformation("StartProcess", "FileName: '{0}'", startInfo.FileName);

                    //
                    // Add environment variable to vertex host process
                    //
                    startInfo.Arguments = arg;
                    foreach (DictionaryEntry entry in environment)
                    {
                        string key = entry.Key.ToString();

                        if (key == null || startInfo.EnvironmentVariables.ContainsKey(key))
                        {
                            DryadLogger.LogInformation("StartProcess", "Attempting to add existing key '{0}' with value '{1}'",
                                                          entry.Key, entry.Value);   
                        } 
                        else
                        {
                            startInfo.EnvironmentVariables.Add(key, entry.Value.ToString());
                        }
                    }

                    lock (syncRoot)
                    {
                        //
                        // After taking lock, start the vertex host process and set up exited event handler
                        //
                        if (cancelled)
                        {
                            // If we've already been canceled, don't start the process
                            DryadLogger.LogInformation("Process start", "Not starting process {0} due to receipt of cancellation", DryadId);
                            return;
                        }
                        else
                        {

                            systemProcess = new Process();
                            systemProcess.StartInfo = startInfo;
                            systemProcess.EnableRaisingEvents = true;
                            systemProcess.Exited += new EventHandler(Process_Exited);
                            Console.WriteLine("Process start - Vertex host process starting");
                            started = systemProcess.Start();
                            Console.WriteLine("Process start - Vertex host process started");
                            if (started)
                            {
                                DryadLogger.LogInformation("Process start", "Vertex host process started");
                                state = ProcessState.Running;
                            }
                            else
                            {
                                DryadLogger.LogError(0, null, "Vertex host process failed to start");
                            }
                        }
                    }
                }
                else
                {
                    DryadLogger.LogError(0, null, "Initialization failed");
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Error starting vertex");
            }

            if (started)
            {
                //
                // Notify Graph Manager that process started if successful
                //
                bool success = ReplyDispatcher.FireStateChange(this.graphManagerReplyUri, this.dryadProcessId, ProcessState.Running);
                if (!success)
                {
                    //
                    // Graph manager doesn't know we started and we have no way to tell it, so it's 
                    // best to just fail the vertex service task and let the job manager inform the graph manager
                    //
                    VertexService.Surrender(new Exception("Unable to communicate with graph manager."));
                }
            }
            else
            {
                //
                // Otherwise, notify GM that process has failed
                //
                lock (syncRoot)
                {
                    // If we've already been canceled, we don't need to change state or record the initialization failure
                    if (!cancelled)
                    {
                        state = ProcessState.Completed;
                        this.failed = true;
                        exitCode = unchecked((int)Constants.DrError_VertexInitialization); // DryadError_VertexInitialization
                    }
                }

                if (failed)  // This also means we weren't canceled
                {
                    // Notify the Graph Manager that the process failed to start
                    Process_Exited(this, null);
                }
            }

            //
            // Make sure process start event is set
            //
            processStartEvent.Set();
        }

        /// <summary>
        /// Check if the message label marks vertex as completed
        /// </summary>
        /// <param name="statusMessageLabel">Label to check</param>
        /// <returns>true if message contains DryadError_VertexCompleted</returns>
        bool StatusMessageContainsDryadError_VertexCompleted(string statusMessageLabel)
        {
            //
            // todo: This seems hacky - make sure it always works
            //
            if (statusMessageLabel.StartsWith(@"DVertexStatus-", StringComparison.OrdinalIgnoreCase) &&
                  !statusMessageLabel.EndsWith(@"update", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }
        
        /// <summary>
        /// Called in new thread in setgetproperty service operation
        /// </summary>
        /// <param name="obj"></param>
        void SetGetPropThreadProc(Object obj)
        {
            DryadLogger.LogMethodEntry(DryadId);
            PropertyRequest r = obj as PropertyRequest;

            ProcessInfo infoLocal = new ProcessInfo();
            ulong[] propertyVersions = null;
            string[] propertyLabels = null;

            //
            // Make sure process is started before continuing
            //
            if (this.State < ProcessState.Running)
            {
                try
                {
                    processStartEvent.WaitOne();
                }
                catch (ObjectDisposedException ex)
                {
                    // The process was cancelled and released before it started running, just return
                    if (exited)
                    {
                        DryadLogger.LogInformation("SetGetProp Thread", "Process {0} cancelled or exited before starting.", this.DryadId);
                    }
                    else
                    {
                        DryadLogger.LogError(0, ex);
                    }
                    DryadLogger.LogMethodExit();
                    return;
                }
            }

            //
            // Use status_pending if running, vertex initialization failure if process is failed and process exit code otherwise
            //
            infoLocal.processStatus = 0x103;  // WinNT.h STATUS_PENDING
            infoLocal.processState = state;
            if (state == ProcessState.Running)
            {
                infoLocal.exitCode = 0x103; // WinNT.h STATUS_PENDING
            }
            else if (failed)
            {
                infoLocal.exitCode = Constants.DrError_VertexError; 
            }
            else if (cancelled)
            {
                infoLocal.exitCode = Constants.DrError_VertexReceivedTermination;  // DryadError_VertexReceivedTermination
            }
            else
            {
                infoLocal.exitCode = (uint)systemProcess.ExitCode;
            }

            //
            // Record specified properties and update versions - wakes up anyone waiting for property changes
            //
            SetProperties(r.infos, out propertyLabels, out propertyVersions);
            
            //
            // Try to get property update
            //
            if (BlockOnProperty(r.blockOnLabel, r.blockOnVersion, r.maxBlockTime))
            {
                //
                // If property update was received, update the received property information
                // If received property marks vertex completed, record that
                //
                if (r.getPropLabel != null && r.getPropLabel.Length > 0)
                {
                    lock (syncRoot)
                    {
                        infoLocal.propertyInfos = new ProcessPropertyInfo[1];

                        int index;
                        if (TryGetProperty(r.getPropLabel, out infoLocal.propertyInfos[0], out index) == false)
                        {
                            DryadLogger.LogError(0, null, "Failed to get property for label {0}", r.getPropLabel);
                        }
                        
                        if (StatusMessageContainsDryadError_VertexCompleted(infoLocal.propertyInfos[0].propertyLabel))
                        {
                            CopyProp(infoLocal.propertyInfos[0], out latestVertexStatusSent);
                        }
                    }
                }
                
                //
                // If request asks for statistics on vertex process, get them
                //
                if (r.ProcessStatistics)
                {
                    if (GetStatistics(out infoLocal.processStatistics) == false)
                    {
                        DryadLogger.LogError(0, null, "Failed to get vertex statistics");
                    }
                }
            }
            
            //
            // Try to report property change, if unsuccessful, kill the running vertex host process
            //
            if (!ReplyDispatcher.SetGetPropsComplete(r.replyUri, systemProcess, dryadProcessId, infoLocal, propertyLabels, propertyVersions))
            {
                try
                {
                    systemProcess.Kill();
                }
                catch (InvalidOperationException /* unused ioe */)
                {
                    // The process has already exited
                    // -or-
                    // There is no process associated with this Process object.
                }
                catch (Exception eInner)
                {
                    //
                    // all other exceptions
                    //
                    DryadLogger.LogError(0, eInner, "Exception calling back to '{0}'", r.replyUri);
                }
            }

            //
            // If a property was handled from the graph manager, decrement the waiter count
            //
            if (ReplyDispatcher.IsGraphMrgUri(r.replyUri))
            {
                int n = Interlocked.Decrement(ref propertyWaiters);
                DryadLogger.LogInformation("SetGetProp Thread", "Process {0} propertyWaiters = {1}", DryadId, n);
            }

            lock (syncRoot)
            {
                //
                // If vertex process has exited, and sending vertex completed event, we can stop worrying
                //
                if (!finalStatusMessageSent)
                {
                    if (latestVertexStatusSent != null)
                    {
                        if (!String.IsNullOrEmpty(latestVertexStatusSent.propertyString))
                        {
                            if (latestVertexStatusSent.propertyString.Contains(string.Format(@"(0x{0:x8})", Constants.DrError_VertexCompleted)))
                            {
                                finalStatusMessageSent = true;
                            }
                        }
                    }
                }
            }
            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Make sure all pending properties get handled and vertex complete event is sent to GM
        /// </summary>
        /// <param name="obj"></param>
        void ExitProcessThreadProc(Object obj)
        {
            DryadLogger.LogMethodEntry();

            //
            // Wait until all property waiters have been notified and the final
            // status message sent, iff the process completed successfully
            //
            do
            {
                //
                // Clear any thing intended for the vertex
                //
                SetAllPropertyWaiters();

                lock (syncRoot)
                {
                    // If nobody is waiting, AND
                    if (propertyWaiters == 0)
                    {
                        // Process did not complete successfully OR 
                        // final status message has already been sent
                        if (!Succeeded || finalStatusMessageSent)
                        {
                            // Then we can send the Process Exit notification
                            break;
                        }
                    }
                }
                
                Thread.Sleep(10);
                
            } while(true);

            ReplyDispatcher.ProcessExited(this.graphManagerReplyUri, this.dryadProcessId, this.exitCode);

            //
            // This should never happen unless a property is requested after the vertex completed event is sent
            // so it's not a big deal if it does because the GM knows that the vertex is done
            //
            if (propertyWaiters > 0)
            {
                DryadLogger.LogWarning("Process exit", "Leaving thread with {0} property waiter(s).", propertyWaiters);
            }

            DryadLogger.LogMethodExit();
        }

        #endregion
    }
}
