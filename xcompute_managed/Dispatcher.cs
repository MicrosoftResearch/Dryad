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
    using System.Globalization;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Net.Security;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.Text;
    using System.Xml;
    using System.Threading;
    using Microsoft.Research.Dryad;

    internal delegate void DispatcherFaultedEventHandler(object sender, EventArgs e);

    internal enum SchedulingResult
    {
        Success = 0,
        Pending = 1,
        Failure = 2,
        CommunicationError = 3
    };

    internal sealed class Dispatcher : IDisposable
    {
        public event DispatcherFaultedEventHandler FaultedEvent;

        public static readonly int InvalidProcessId = -1;
        private static readonly int MaxRetries = 3;
        private static readonly int RetryDelayInMilliseconds = 60 * 1000; // Retry on a faulted dispatcher every 1 minute

        private NetTcpBinding m_backendBinding = null;

        private VertexServiceClient m_client = null;

        private ISchedulerHelper m_schedulerHelper;

        private int m_connectionAttempts = 0;
        private ScheduleProcessRequest m_currentProcess = null;
        private string m_currentReplyUri = null;
        private AsyncCallback m_currentAsyncCallback = null;
        private bool m_disposed = false;
        private string m_endpointAddress = String.Empty;
        private bool m_faulted = false;
        private bool m_idle = true;
        private string m_nodeName = String.Empty;
        private object m_syncRoot = new object();
        private int m_taskId = 0;
        private int m_schedulingAttempts = 0;
        private bool m_taskFailed = false;
        private Timer m_retryTimer = null;

        #region Constructors

        /// <summary>
        /// Constructor used by the vertex host
        /// </summary>
        /// <param name="name"></param>
        /// <param name="endpointAddress"></param>
        public Dispatcher(string name, string endpointAddress)
        {
            m_nodeName = name;
            m_endpointAddress = endpointAddress;
            SafeOpenConnection();
        }

        /// <summary>
        /// Constructor used by the Graph Manager
        /// </summary>
        /// <param name="m_schedulerHelper"></param>
        /// <param name="computeNode"></param>
        public Dispatcher(ISchedulerHelper schedulerHelper, VertexComputeNode computeNode)
        {
            m_schedulerHelper = schedulerHelper;
            m_taskId = computeNode.instanceId;
            m_nodeName = computeNode.ComputeNode;
            m_backendBinding = m_schedulerHelper.GetVertexServiceBinding();
            m_endpointAddress = m_schedulerHelper.GetVertexServiceBaseAddress(m_nodeName, m_taskId) + Constants.vertexServiceName;
            SafeOpenConnection();
        }

        #endregion

        #region Public Methods

        public void SetRetryTimer(TimerCallback cb)
        {
            lock (SyncRoot)
            {
                // Guard against SetRetryTimer and Dispose getting called at
                // the same time
                if (!m_disposed)
                {
                    if (m_retryTimer != null)
                    {
                        m_retryTimer.Dispose();
                        m_retryTimer = null;
                    }
                    m_retryTimer = new Timer(cb, this, RetryDelayInMilliseconds, Timeout.Infinite);
                }
            }
        }

        public void CancelScheduleProcess(int processId)
        {
            bool faultDispatcher = true;

            for (int numRetries = 0; numRetries < MaxRetries; numRetries++)
            {
                try
                {
                    if (!Faulted)
                    {
                        this.m_client.CancelScheduleProcess(processId);
                    }
                    return;
                }
                // CancelScheduleProcess is one-way
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Cancel Process", "Timeout communicating with vertex service on node {0}: {1}", this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Cancel Process", "Error communicating with vertex service on node {0}: {1}", this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Error calling CancelScheduleProcess for node {0}, process {1}", m_nodeName, processId);
                    faultDispatcher = false;
                    break;
                }
            }

            if (faultDispatcher)
            {
                RaiseFaultedEvent();
            }
        }

        public VertexStatus CheckStatus()
        {
            for (int index = 0; index < MaxRetries; index++)
            {
                try
                {
                    if (!Faulted)
                    {
                        return this.m_client.CheckStatus();
                    }
                    break;
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "node '{0}'", m_nodeName);
                    if (!SafeOpenConnection())
                    {
                        break;
                    }
                }
            }

            RaiseFaultedEvent();

            VertexStatus s = new VertexStatus();
            s.serviceIsAlive = false;
            return s;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Initialize(StringDictionary vertexEndpointAddresses)
        {
            bool faultDispatcher = true;

            for (int numRetries = 0; numRetries < MaxRetries; numRetries++)
            {
                try
                {
                    if (!Faulted)
                    {
                        this.m_client.Initialize(vertexEndpointAddresses);
                    }
                    return;
                }
                // Initialize is one-way
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Initialize", "Timeout communicating with vertex service on node {0}: {1}", this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Initialize", "Error communicating with vertex service on node {0}: {1}", this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Error calling Initialize for node {0}", m_nodeName);
                    faultDispatcher = false;
                    break;
                }
            }

            if (faultDispatcher)
            {
                RaiseFaultedEvent();
            }

        }

        private void RaiseFaultedEvent()
        {
            RaiseFaultedEvent(false);
        }

        public void RaiseFaultedEvent(bool taskFailed)
        {
            bool raiseEvent = false;

            // For SP3, we need to crash if this happens in the vertex host
            if (String.Compare(Process.GetCurrentProcess().ProcessName, "HpcQueryVertexHost", StringComparison.OrdinalIgnoreCase) == 0)
            {
                DryadLogger.LogCritical(0, null, "Vertex Host lost communication with Vertex Service while updating vertex status: Exiting vertex. Graph Manager will rerun a failed vertex up to six times.");
                Environment.Exit(unchecked((int)Constants.DrError_VertexHostLostCommunication));
            }

            lock (SyncRoot)
            {
                // We always want to raise the faulted event if the 
                // task failed, so that the dispatcher is disposed.

                // If the task did not fail, we want to ensure that
                // the event is only raised once for a given fault.
                raiseEvent = taskFailed || (!Faulted);


                // We never want to reset m_taskFailed once it's been set
                // to true, because the task isn't coming back.
                m_taskFailed = m_taskFailed || taskFailed;

                m_faulted = true;
            }

            if (raiseEvent)
            {
                DryadLogger.LogError(0, null, "Dispatcher for task {0} has faulted on node {1}, current process: {2}", m_taskId, m_nodeName, CurrentProcess == InvalidProcessId ? "<none>" : CurrentProcess.ToString());
                
                // Notice that this will keep any locks that are currently held, so refrain from calling this while enumerating the dispatchers
                FaultedEvent(this, null);
            }
        }

        public void Release()
        {
            if (!Idle)
            {
                lock (SyncRoot)
                {
                    if (!Idle)
                    {
                        this.m_idle = true;
                        // Reset the number of scheduling attempts, since they are per-process
                        m_schedulingAttempts = 0;
                    }
                }
            }
        }

        /// <summary>
        /// Notify vertex service that the Graph Manager is done
        /// with vertex process processId
        /// </summary>
        /// <param name="processId">Process Id of the process to release</param>
        public void ReleaseProcess(int processId)
        {
            bool faultDispatcher = true;

            for (int numRetries = 0; numRetries < MaxRetries; numRetries++)
            {
                try
                {
                    if (CurrentProcess == processId)
                    {
                        m_currentProcess = null;
                    }

                    if (!Faulted)
                    {
                        this.m_client.ReleaseProcess(processId);
                    }
                    return;
                }
                // ReleaseProcess is one-way
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Release Process", "Timeout communicating with vertex service on node {0}: {1}", this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Release Process", "Error communicating with vertex service on node {0}: {1}", this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Error calling ReleaseProcess for node {0}", m_nodeName);
                    faultDispatcher = false;
                    break;
                }
            }

            if (faultDispatcher)
            {
                RaiseFaultedEvent();
            }

        }

        public bool Reserve()
        {
            bool acquired = false;
            if (!Faulted && Idle)
            {
                lock (SyncRoot)
                {
                    if (!Faulted && Idle)
                    {
                        m_idle = false;
                        acquired = true;
                    }
                }
            }
            return acquired;
        }

        public bool ScheduleProcess(string replyUri, ScheduleProcessRequest req, AsyncCallback cb)
        {
            bool faultDispatcher = true;

            for (int numRetries = 0; numRetries < MaxRetries; numRetries++)
            {
                try
                {
                    // TODO: Why are we taking the lock in this particular case again?
                    lock (SyncRoot)
                    {
                        if (!Faulted && m_schedulingAttempts < MaxRetries)
                        {
                            m_schedulingAttempts++;

                            // Set the current process so that if the dispatcher faults we know
                            // which process to kill
                            m_currentProcess = req;
                            m_currentReplyUri = replyUri;
                            m_currentAsyncCallback = cb;

                            this.m_client.BeginScheduleProcess(replyUri, req.Id, req.CommandLine, req.Environment, cb, (object)this);
                            return true;
                        }
                    }
                    return false;
                }
                catch (FaultException<VertexServiceError> vse)
                {
                    DryadLogger.LogWarning("Schedule Process", "Error scheduling process {0} on node {1}: {2}", req.Id, this.m_nodeName, vse.Reason);
                    faultDispatcher = false;
                    break;
                }
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Schedule Process", "Timeout communicating with vertex service scheduling process {0} on node {1}: {2}", req.Id, this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Schedule Process", "Error communicating with vertex service scheduling process {0} on node {1}: {2}", req.Id, this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Error calling ScheduleProcess for process {0} on node {1}", req.Id, m_nodeName);
                    faultDispatcher = false;
                    break;
                }
            }

            if (faultDispatcher)
            {
                RaiseFaultedEvent();
            }
            return false;

        }

        public SchedulingResult EndScheduleProcess(IAsyncResult asyncResult)
        {
            // We don't want to retry the async end operation - if it fails retry
            // the whole scheduling operation

            try
            {
                if (!Faulted)
                {
                    if (this.m_client.EndScheduleProcess(asyncResult))
                    {
                        return SchedulingResult.Success;
                    }
                    else
                    {
                        return SchedulingResult.Failure;
                    }

                }
                else
                {
                    return SchedulingResult.Failure;
                }
            }
            catch (FaultException<VertexServiceError> vse)
            {
                DryadLogger.LogWarning("Schedule Process", "Error completing schedule process {0} on node {1}: {2}", this.m_currentProcess.Id, this.m_nodeName, vse.Reason);
                return SchedulingResult.Failure;
            }
            catch (TimeoutException te)
            {
                DryadLogger.LogWarning("Schedule Process", "Timeout communicating with vertex service for process {0} on node {1}: {2}", this.m_currentProcess.Id, this.m_nodeName, te.ToString());
            }
            catch (CommunicationException ce)
            {
                DryadLogger.LogWarning("Schedule Process", "Error communicating with vertex service for process {0} on node {1}: {2}", this.m_currentProcess.Id, this.m_nodeName, ce.ToString());
            }
            catch (Exception e)
            {
                DryadLogger.LogError(0, e, "Error calling EndScheduleProcess for process {0} on node {0}", this.m_currentProcess.Id, m_nodeName);
                return SchedulingResult.Failure;
            }

            // If we make it here, then we need to retry the scheduling operation
            if (SafeOpenConnection())
            {
                // ScheduleProcess manages the retry count and returns false if it is exceeded
                DryadLogger.LogDebug("Schedule Process", "Communication error: retrying process {0} on node {1}", this.m_currentProcess.Id, this.m_nodeName);
                if (ScheduleProcess(m_currentReplyUri, m_currentProcess, m_currentAsyncCallback))
                {
                    return SchedulingResult.Pending;
                }
            }

            // SafeOpenConnection failed or retry count exceeded - fault the dispatcher.
            DryadLogger.LogWarning("Schedule Process", "Connection failed to node {0}", this.m_nodeName);
            return SchedulingResult.CommunicationError;
        }

        public bool SetGetProps(string replyUri, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics)
        {
            bool faultDispatcher = true;

            for (int numRetries = 0; numRetries < MaxRetries; numRetries++)
            {
                try
                {
                    if (!Faulted)
                    {
                        return this.m_client.SetGetProps(replyUri, processId, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics);
                    }
                    return false;
                }
                catch (FaultException<UnknownProcessError>)
                {
                    DryadLogger.LogWarning("Set Get Process Properties", "Attempt to get or set properties for unknown process {0} on node {1}", processId, this.m_nodeName);
                    faultDispatcher = false;
                    break;
                }
                catch (FaultException<VertexServiceError> vse)
                {
                    DryadLogger.LogWarning("Set Get Process Properties", "Error setting or getting properties for process {0} on node {1}: {2}", processId, this.m_nodeName, vse.Reason);
                    faultDispatcher = false;
                    break;
                }
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Set Get Process Properties", "Timeout communicating with vertex service for process {0} on node {1}: {2}", processId, this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Set Get Process Properties", "Error communicating with vertex service for process {0} on node {1}: {2}", processId, this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        faultDispatcher = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Error calling SetGetProps for process {0} on node {1}", processId, m_nodeName);
                    faultDispatcher = false;
                    break;
                }
            }

            if (faultDispatcher)
            {
                RaiseFaultedEvent();
            }
            return false;

        }

        /// <summary>
        /// Call Shutdown method on the vertex service and close the communication channel.
        /// After this method is called, the Dispatcher is unusable.
        /// </summary>
        /// <param name="state">uint code - reserved for future use</param>
        public void Shutdown(uint code)
        {
            for (int index = 0; index < MaxRetries; index++)
            {
                try
                {
                    if (!Faulted)
                    {
                        this.m_client.Shutdown(code);
                    }
                    return;
                }
                catch (FaultException<VertexServiceError> vse)
                {
                    DryadLogger.LogWarning("Shutdown", "Error shutting down vertex service on node {0}: {1}", this.m_nodeName, vse.Reason);
                    break;
                }
                catch (TimeoutException te)
                {
                    DryadLogger.LogWarning("Shutdown", "Timeout communicating with vertex service on node {0}: {1}", this.m_nodeName, te.ToString());
                    if (!SafeOpenConnection())
                    {
                        break;
                    }
                }
                catch (CommunicationException ce)
                {
                    DryadLogger.LogWarning("Shutdown", "Error communicating with vertex service on node {0}: {1}", this.m_nodeName, ce.ToString());
                    if (!SafeOpenConnection())
                    {
                        DryadLogger.LogWarning("Shutdown", "Failed to reopen connection to node {0}", this.m_nodeName);
                        break;
                    }
                }
                catch (Exception e)
                {
                    DryadLogger.LogWarning("Shutdown", "Exception shutting down vertex service on node {0}: {1}", this.m_nodeName, e.ToString());
                    if (!SafeOpenConnection())
                    {
                        break;
                    }
                }
            }

            // Not faulting the dispatcher here, even though the WCF connection could not be closed cleanly
            // Shutdown is only called when the graphmanger is closing, so there is no need to fault the dispatchers
            // Also avoids problems around faulting dispatchers while enumerating them
        }

        #endregion

        #region Private Methods

        private void Dispose(bool disposing)
        {
            bool closeConnection = false;

            if (!this.m_disposed)
            {
                lock (SyncRoot)
                {
                    if (!this.m_disposed)
                    {
                        if (disposing)
                        {
                            closeConnection = true;
                        }

                        if (m_retryTimer != null)
                        {
                            m_retryTimer.Dispose();
                            m_retryTimer = null;
                        }
                        m_disposed = true;
                    }
                }
            }

            if (closeConnection)
            {
                SafeCloseConnection();
            }
        }

        private void SafeCloseConnection()
        {
            VertexServiceClient client = null;
            lock (SyncRoot)
            {
                if (m_client == null)
                {
                    return;
                }

                client = m_client;
                m_client = null;
            }

            try
            {
                client.Close();
            }
            catch
            {
                try
                {
                    client.Abort();
                }
                catch
                {
                }
            }
        }

        private bool SafeOpenConnection()
        {
            SafeCloseConnection();

            lock (SyncRoot)
            {
                if (!Faulted)
                {
                    m_client = new VertexServiceClient(m_backendBinding, new EndpointAddress(m_endpointAddress));
                    m_connectionAttempts++;
                    return true;
                }
                return false;
            }
        }

        #endregion

        #region Properties

        public int ConnectionAttempts
        {
            get { return this.m_connectionAttempts; }
        }

        public int CurrentProcess
        {
            get 
            {
                if (m_currentProcess != null)
                {
                    return m_currentProcess.Id;
                }
                else
                {
                    return InvalidProcessId;
                }
            }
        }

        public bool Faulted
        {
            get { return this.m_faulted; }
            set { this.m_faulted = value; }
        }

        public bool SchedulerTaskFailed
        {
            get { return this.m_taskFailed; }
            set { this.m_taskFailed = value; }
        }

        public bool Idle
        {
            get { return this.m_idle; }
        }

        public string NodeName
        {
            get { return this.m_nodeName; }
        }

        public object SyncRoot
        {
            get { return m_syncRoot; }
        }

        public int TaskId
        {
            get
            {
                return this.m_taskId;
            }
        }

        #endregion

    }
}
