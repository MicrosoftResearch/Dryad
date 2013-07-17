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
    using System.Threading;
    using Microsoft.Research.Dryad;

    public delegate void StateChangeEventHandler(object sender, XComputeProcessStateChangeEventArgs e);

    public delegate void GetSetPropertyEventHandler(object sender, XComputeProcessGetSetPropertyEventArgs e);

    internal class XComputeProcess : IDisposable
    {
        private bool m_disposed = false;
        private int m_id;
        private int m_graphManagerId = -1;
        private int m_graphManagerVersion = -1;
        private ProcessState m_currentState;
        private uint m_exitCode;
        private bool m_cancelled = false;
        private bool m_handleClosed = false;
        private Dispatcher m_dispatcher = null;
        private string m_assignedNode = String.Empty;
        private Dictionary<ProcessState, StateChangeEventHandler> m_stateChangeListeners;
        private Dictionary<ProcessState, List<ManualResetEvent>> m_stateChangeWaiters;
        private Dictionary<StateChangeEventHandler, Timer> m_stateChangeTimers;
        private ManualResetEvent m_assignedToNodeEvent = new ManualResetEvent(false);

        private Dictionary<string, Dictionary<ulong, GetSetPropertyEventHandler>> m_propertyListeners;

        private object m_syncRoot = new object();

        private static readonly char[] cmdLineSeparator = new char[] { ' ' };

        public XComputeProcess(int m_id)
        {
            this.m_id = m_id;
            this.m_currentState = ProcessState.Uninitialized;
            this.m_exitCode = 0;
            m_stateChangeListeners = new Dictionary<ProcessState, StateChangeEventHandler>();
            m_stateChangeWaiters = new Dictionary<ProcessState, List<ManualResetEvent>>();
            m_stateChangeTimers = new Dictionary<StateChangeEventHandler, Timer>();

            m_propertyListeners = new Dictionary<string, Dictionary<ulong, GetSetPropertyEventHandler>>();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                if (disposing)
                {
                    DryadLogger.LogInformation("Dispose Process", "Releasing resources for process id {0}", this.m_id);

                    this.m_assignedToNodeEvent.Close();

                    foreach (KeyValuePair<ProcessState, List<ManualResetEvent>> kvp in m_stateChangeWaiters)
                    {
                        foreach (ManualResetEvent e in kvp.Value)
                        {
                            try
                            {
                                e.Close();
                            }
                            catch (Exception ex)
                            {
                                DryadLogger.LogError(0, ex);
                            }
                        }
                    }
                }
                m_disposed = true;
            }
        }

        public void Timeout(object state)
        {
            StateChangeEventHandler handler = state as StateChangeEventHandler;
            handler(this, new XComputeProcessStateChangeEventArgs(m_id, m_currentState, true));
            lock (SyncRoot)
            {
                m_stateChangeTimers.Remove(handler);
            }
        }

        public void AddPropertyListener(string label, ulong version, GetSetPropertyEventHandler handler)
        {
            lock (SyncRoot)
            {
                // Is there an entry for this label?
                if (m_propertyListeners.ContainsKey(label) == false)
                {
                    m_propertyListeners.Add(label, new Dictionary<ulong,GetSetPropertyEventHandler>());
                }

                // Is there an entry for this version
                if (m_propertyListeners[label].ContainsKey(version))
                {
                    m_propertyListeners[label][version] += handler;
                }
                else
                {
                    m_propertyListeners[label].Add(version, handler);
                }
            }
        }

        public void AddStateChangeListener(ProcessState targetState, long timeoutInterval, StateChangeEventHandler handler)
        {
            lock (SyncRoot)
            {
                if (m_currentState >= targetState)
                {
                    handler(this, new XComputeProcessStateChangeEventArgs(m_id, m_currentState, false));
                }
                else
                {
                    if (m_stateChangeListeners.ContainsKey(targetState))
                    {
                        m_stateChangeListeners[targetState] += handler;
                    }
                    else
                    {
                        m_stateChangeListeners.Add(targetState, handler);
                    }
                    if (timeoutInterval != long.MaxValue)
                    {
                        m_stateChangeTimers[handler] = new Timer(this.Timeout, handler, timeoutInterval, 0);
                    }
                }
            }
        }

        public void AddStateChangeWaiter(ProcessState targetState, ManualResetEvent waitEvent)
        {
            lock (SyncRoot)
            {
                if (m_currentState >= targetState)
                {
                    waitEvent.Set();
                }
                else
                {
                    m_stateChangeWaiters[targetState].Add(waitEvent);
                }
            }
        }

        public void Cancel()
        {
            bool wasRunning = false;

            lock (SyncRoot)
            {
                // If the process has already been assigned to a node, then we will need to cancel it at the node
                if (this.CurrentState < ProcessState.AssignedToNode)
                {
                    this.m_cancelled = true;
                    this.ExitCode = 0x830A0003; // DrError_VertexReceivedTermination
                    DryadLogger.LogInformation("Cancel process", "Cancelation received for vertex {0}.{1} before it was assigned to a node", m_graphManagerId, m_graphManagerVersion);
                    wasRunning = false;
                }
                else if (this.CurrentState == ProcessState.Completed)
                {
                    // nothing to do for this case, process already completed
                    DryadLogger.LogInformation("Cancel process", "Cancellation received for vertex {0}.{1} after it completed", m_graphManagerId, m_graphManagerVersion);
                    return;
                }
                else if (Dispatcher != null)
                {
                    DryadLogger.LogInformation("Cancel process", "Cancellation received for vertex {0}.{1} after it was assigned to node {2}", m_graphManagerId, m_graphManagerVersion, Dispatcher.NodeName);
                    wasRunning = true;
                }
                else
                {
                    // This is an unexpected condition
                    DryadLogger.LogError(0, null, "Cancellation received for vertex {0}.{1} in state {2} with no dispatcher", m_graphManagerId, m_graphManagerVersion, CurrentState.ToString());
                    return;
                }

                if (wasRunning)
                {
                    if (Dispatcher != null)
                    {
                        Dispatcher.CancelScheduleProcess(m_id);
                    }
                }
                else
                {
                    ChangeState(ProcessState.Completed);
                }
            }
        }

        public void ChangeState(ProcessState newState)
        {
            lock (SyncRoot)
            {
                if (newState > m_currentState)
                {
                    DryadLogger.LogDebug("Change State", "Transition process {0} from state {1} to state {2}", m_id, m_currentState, newState);

                    m_currentState = newState;
                    List<ProcessState> listenersToRemove = new List<ProcessState>();
                    List<ProcessState> waitersToRemove = new List<ProcessState>();

                    // Check for listeners / waiters for earlier states, in case a state is skipped (e.g. process failed to start)
                    foreach (ProcessState s in m_stateChangeListeners.Keys)
                    {
                        if (s <= m_currentState)
                        {
                            // Notify listeners
                            if (m_stateChangeListeners[s] != null)
                            {
                                XComputeProcessStateChangeEventArgs e = new XComputeProcessStateChangeEventArgs(m_id, m_currentState, false);
                                m_stateChangeListeners[s](this, e);
                                if (m_stateChangeTimers.ContainsKey(m_stateChangeListeners[s]))
                                {
                                    m_stateChangeTimers[m_stateChangeListeners[s]].Dispose();
                                    m_stateChangeTimers.Remove(m_stateChangeListeners[s]);
                                }
                            }
                            listenersToRemove.Add(s);
                        }
                    }
                    foreach (ProcessState s in listenersToRemove)
                    {
                        m_stateChangeListeners.Remove(s);
                    }

                    foreach (ProcessState s in m_stateChangeWaiters.Keys)
                    {
                        // Signal waiters
                        if (s <= m_currentState)
                        {
                            foreach (ManualResetEvent w in m_stateChangeWaiters[s])
                            {
                                w.Set();
                            }
                            waitersToRemove.Add(s);
                        }
                    }
                    foreach (ProcessState s in waitersToRemove)
                    {
                        foreach (ManualResetEvent e in m_stateChangeWaiters[s])
                        {
                            try
                            {
                                e.Close();
                            }
                            catch (Exception ex)
                            {
                                DryadLogger.LogError(0, ex);
                            }
                        }
                        m_stateChangeWaiters.Remove(s);
                    }

                    if (m_currentState == ProcessState.AssignedToNode)
                    {
                        m_assignedToNodeEvent.Set();
                    }
                }
                else
                {
                    DryadLogger.LogWarning("Change State", "Unexpected state change attempted for process {0}: from {1} to {2}", this.m_id, this.m_currentState.ToString(), newState.ToString());
                }
            }
        }

        public void TransitionToRunning(object state)
        {
            DryadLogger.LogDebug("Change State", "Transitioning to Running with current state {0} for process {1}", this.m_currentState.ToString(), this.m_id);

            try
            {
                // In rare cases (such as a cancelled duplicate), the GM may close the handle to the process while it is transitioning to running.
                // This results in Dispose being called on this process, which closes the m_assignedToNode handle.
                // In this case, we want to catch the exception and log it, but do nothing else, since the GM is done with this process.
                if (m_assignedToNodeEvent.WaitOne(new TimeSpan(0, 0, 10), false))
                {
                    DryadLogger.LogDebug("Change State", "Successfully waited for transition to {0} for process {1}", this.m_currentState.ToString(), this.m_id);
                }
                else
                {
                    DryadLogger.LogWarning("Change State", "Timed out waiting for transition to AssignedToNode for process {0}", this.m_id);
                    // We want to fire the state change anyway or else we'll get a zombie process.
                    // The GM will handle the transition, it just may cause a delay.
                }
                ChangeState(ProcessState.Running);
            }
            catch (ObjectDisposedException ex)
            {
                DryadLogger.LogError(0, ex, "Process handle was closed while waiting for transition to assigned to node");
            }
        }

        public void SetGetPropsComplete(ProcessInfo info, string[] propertyLabels, ulong[] propertyVersions)
        {
            lock (SyncRoot)
            {
                // For the Set part
                if (propertyLabels != null && propertyVersions != null)
                {
                    for (int i = 0; i < propertyLabels.Length; i++)
                    {
                        if (m_propertyListeners.ContainsKey(propertyLabels[i]))
                        {
                            List<ulong> versionsToRemove = new List<ulong>();
                            foreach (KeyValuePair<ulong, GetSetPropertyEventHandler> entry in m_propertyListeners[propertyLabels[i]])
                            {
                                if (entry.Key <= propertyVersions[i] || entry.Key == ulong.MaxValue)
                                {
                                    DryadLogger.LogDebug("SetGetProsComplete", "Set complete - m_id: {0} state: {1}, label: {2}", m_id, info.processState, propertyLabels[i]);
                                    XComputeProcessGetSetPropertyEventArgs e = new XComputeProcessGetSetPropertyEventArgs(m_id, info, propertyVersions);
                                    entry.Value(this, e);

                                    versionsToRemove.Add(entry.Key);
                                }
                            }
                            foreach (ulong version in versionsToRemove)
                            {
                                m_propertyListeners[propertyLabels[i]].Remove(version);
                            }
                        }
                    }
                }

                // For the Get part
                if (info != null && info.propertyInfos != null)
                {
                    foreach (ProcessPropertyInfo propInfo in info.propertyInfos)
                    {
                        if (m_propertyListeners.ContainsKey(propInfo.propertyLabel))
                        {
                            List<ulong> versionsToRemove = new List<ulong>();
                            foreach (KeyValuePair<ulong, GetSetPropertyEventHandler> entry in m_propertyListeners[propInfo.propertyLabel])
                            {
                                if (entry.Key <= propInfo.propertyVersion || entry.Key == ulong.MaxValue)
                                {
                                    DryadLogger.LogDebug("SetGetProsComplete", "Get complete - m_id: {0} state: {1}, label: {2}", m_id, info.processState, propInfo.propertyLabel);

                                    XComputeProcessGetSetPropertyEventArgs e = new XComputeProcessGetSetPropertyEventArgs(m_id, info, propertyVersions);
                                    entry.Value(this, e);

                                    versionsToRemove.Add(entry.Key);
                                }
                            }
                            foreach (ulong version in versionsToRemove)
                            {
                                m_propertyListeners[propInfo.propertyLabel].Remove(version);
                            }
                        }
                    }
                }
            }
        }

        public void SetIdAndVersion(string commandLine)
        {
            bool parsed = false;
            string[] args = commandLine.Split(cmdLineSeparator, StringSplitOptions.RemoveEmptyEntries);
            if (args != null)
            {
                if (args.Length == 6)
                {
                    lock (SyncRoot)
                    {
                        if (Int32.TryParse(args[4], out m_graphManagerId))
                        {
                            if (Int32.TryParse(args[5], out m_graphManagerVersion))
                            {
                                parsed = true;
                            }
                        }
                    }
                }
            }

            if (!parsed)
            {
                DryadLogger.LogWarning("Set Vertex Id And Version", "Failed to parse vertex command line: {0}", commandLine);
            }
        }

        public string AssignedNode
        {
            get { return m_assignedNode; }
            set { m_assignedNode = value; } 
        }

        public Dispatcher Dispatcher
        {
            get { return m_dispatcher; }
            set 
            {
                lock (SyncRoot)
                {
                    m_dispatcher = value;
                    try
                    {
                        if (m_dispatcher != null)
                        {
                            m_assignedNode = m_dispatcher.NodeName;
                        }
                    }
                    catch (Exception e)
                    {
                        DryadLogger.LogError(0, e, "Failed to set assigned node from supplied dispatcher");
                    }
                }
            }
        }

        public ProcessState CurrentState
        {
            get { return m_currentState; }
        }

        public uint ExitCode
        {
            get { return m_exitCode; }
            set
            {
                lock (SyncRoot)
                {
                    m_exitCode = value;
                }
            }
        }

        public bool Cancelled
        {
            get
            {
                return this.m_cancelled;
            }
        }

        public bool HandleClosed
        {
            get { return m_handleClosed; }
            set { m_handleClosed = value; }
        }

        public int GraphManagerId
        {
            get
            {
                return m_graphManagerId;
            }
        }

        public int GraphManagerVersion
        {
            get
            {
                return m_graphManagerVersion;
            }
        }

        public object SyncRoot
        {
            get { return m_syncRoot; }
        }

    }

    public class XComputeProcessGetSetPropertyEventArgs
    {
        private int processId;
        private ProcessInfo processInfo;
        private ulong[] propertyVersions;

        public XComputeProcessGetSetPropertyEventArgs(int m_id, ProcessInfo info, ulong[] versions)
        {
            this.processId = m_id;
            this.processInfo = info;
            this.propertyVersions = versions;
        }

        public int ProcessId
        {
            get { return this.processId; }
        }

        public ProcessInfo ProcessInfo
        {
            get { return this.processInfo; }
        }

        public ulong[] PropertyVersions
        {
            get { return this.propertyVersions; }
        }
    }

    public class XComputeProcessStateChangeEventArgs
    {
        private bool timedOut;
        private int processId;
        private ProcessState m_currentState;

        public XComputeProcessStateChangeEventArgs(int m_id, ProcessState state, bool timedOut)
        {
            this.timedOut = timedOut;
            processId = m_id;
            m_currentState = state;
        }

        public int ProcessId
        {
            get { return processId; }
        }

        public ProcessState State
        {
            get { return m_currentState; }
        }

        public bool TimedOut
        {
            get { return timedOut; }
            set { timedOut = value; }
        }
    }
}
