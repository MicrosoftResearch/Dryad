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
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.Threading;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Collections.Concurrent;
    using System.Reflection;
    using System.Net.Security;
    using System.Security;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Text;
    using System.Xml;
    using System.Xml.Serialization;
    using Microsoft.Hpc;
    using Microsoft.Win32;
    using Microsoft.Research.Dryad.YarnBridge;

    [Flags]
    public enum VertexTaskState
    {
        NA = 0x00000000,
        Waiting = 0x00000010,
        Running = 0x00000020,
        Finished = 0x00000030,
        Failed = 0x00000031,
        Canceled = 0x00000032
    }

    public class VertexChangeEventArgs : EventArgs
    {
        public VertexChangeEventArgs(int id)
        {
            Id = id;
            OldState = VertexTaskState.NA;
            OldNode = String.Empty;
            OldRequeueCount = 0;
        }

        public int Id
        {
            get;
            private set;
        }

        public VertexTaskState OldState
        {
            get;
            set;
        }

        public VertexTaskState NewState
        {
            get;
            set;
        }

        public string OldNode
        {
            get;
            set;
        }

        public string NewNode
        {
            get;
            set;
        }

        public int OldRequeueCount
        {
            get;
            set;
        }

        public int NewRequeueCount
        {
            get;
            set;
        }
    }

    public class VertexComputeNode
    {
        public string ComputeNode;
        public int instanceId;
        public VertexTaskState State;
    }

    public delegate void VertexChangeEventHandler(object sender, VertexChangeEventArgs e);

    public interface ISchedulerHelper : IDisposable
    {
        event VertexChangeEventHandler OnVertexChange;

        void FinishJob();

        string GetVertexServiceBaseAddress(string nodename, int instanceId);

        NetTcpBinding GetVertexServiceBinding();

        void SetJobProgress(int n, string message);

        bool StartTaskMonitorThread();

        void StopTaskMonitorThread();

        bool WaitForTasksReady();

    }

    public class SchedulerHelperFactory
    {
        private static ISchedulerHelper m_instance = null;
        private static object m_lock = new object();

        public static ISchedulerHelper GetInstance()
        {
            if (m_instance == null)
            {
                lock (m_lock)
                {
                    if (m_instance == null)
                    {
                        string schedulerType = System.Environment.GetEnvironmentVariable(Constants.schedulerTypeEnvVar);
                        if (String.IsNullOrEmpty(schedulerType) || schedulerType == Constants.schedulerTypeYarn)
                        {
                            m_instance = new YarnSchedulerHelper();
                        }
                        else if (schedulerType == Constants.schedulerTypeLocal)
                        {
                            m_instance = new LocalSchedulerHelper();
                        }
                        else
                        {
                            throw new InvalidOperationException(String.Format("Scheduler type {0} is not supported", schedulerType));
                        }
                    }
                }
            }
            return m_instance;
       }
    }

    public class LocalSchedulerHelper : ISchedulerHelper
    {
        protected string m_EnvCcpLocalProcessComputeNodes = System.Environment.GetEnvironmentVariable(Constants.localProcessComputeNodesEnvVar);
        protected string[] m_LocalProcessComputeNodes = new string[0];
        private bool m_disposed = false;


        private VertexChangeEventHandler m_vertexChangeEvent;

        event VertexChangeEventHandler ISchedulerHelper.OnVertexChange
        {
            add
            {
                lock (m_vertexChangeEvent)
                {
                    m_vertexChangeEvent += value;
                }
            }
            remove
            {
                lock (m_vertexChangeEvent)
                {
                    m_vertexChangeEvent -= value;
                }
            }
        }

        public LocalSchedulerHelper()
        {
            if (!String.IsNullOrEmpty(m_EnvCcpLocalProcessComputeNodes))
            {
                m_LocalProcessComputeNodes = m_EnvCcpLocalProcessComputeNodes.Split(',');
            }
        }

        void IDisposable.Dispose()
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
                }
                m_disposed = true;
            }
        }

        void ISchedulerHelper.FinishJob()
        {
            
        }

        string ISchedulerHelper.GetVertexServiceBaseAddress(string nodename, int instanceId)
        {
            return String.Format(Constants.vertexAddrFormat, "localhost", nodename);
        }

        NetTcpBinding ISchedulerHelper.GetVertexServiceBinding()
        {
            NetTcpBinding binding = new NetTcpBinding(SecurityMode.Transport, false);
            binding.PortSharingEnabled = true;
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = ProtectionLevel.None;
            binding.SendTimeout = Constants.SendTimeout;
            binding.ReceiveTimeout = Constants.ReceiveTimeout;
            binding.MaxReceivedMessageSize = Constants.MaxReceivedMessageSize;
            binding.MaxBufferPoolSize = Constants.MaxBufferPoolSize;
            binding.MaxConnections = Constants.MaxConnections;
            binding.ListenBacklog = Constants.ListenBacklog;
            binding.ReaderQuotas = System.Xml.XmlDictionaryReaderQuotas.Max;

            return binding;

        }

        void ISchedulerHelper.SetJobProgress(int n, string message)
        {
        }

        bool ISchedulerHelper.StartTaskMonitorThread()
        {
            return true;
        }

        void ISchedulerHelper.StopTaskMonitorThread()
        {
            return;
        }

        bool ISchedulerHelper.WaitForTasksReady()
        {
            return true;
        }

    }

    public class YarnSchedulerHelper : ISchedulerHelper
    {
        public enum YarnTaskState
        {
            NA,
            Scheduling,
            Running,
            Completed,
            Failed
        }

        public class VertexTask
        {
            public VertexTask(int id, string node, YarnTaskState state, int requeueCount, DateTime changeTime)
            {
                Id = id;
                Node = node;
                State = state;
                RequeueCount = requeueCount;
                ChangeTime = changeTime;
            }

            public int Id
            {
                get;
                set;
            }

            public string Node
            {
                get;
                set;
            }

            public YarnTaskState State
            {
                get;
                set;
            }

            public int RequeueCount
            {
                get;
                set;
            }

            public DateTime ChangeTime
            {
                get;
                set;
            }
        }

        private VertexTask[] m_vertices = null;

        protected string m_EnvCcpClusterName = System.Environment.GetEnvironmentVariable(Constants.clusterNameEnvVar);
        protected int m_EnvCcpJobId = Convert.ToInt32(System.Environment.GetEnvironmentVariable(Constants.jobIdEnvVar));

        private object m_eventLock = new object();
        private VertexChangeEventHandler m_vertexChangeEvent;

        private int m_minNodes = -1;
        private int m_maxNodes = -1;
        private int m_startNodes = -1;
        private int m_runningTasks = 0;  // No longer start at 1 since the GM is not running under a task
        private int m_finishedTasks = 0;
        private object m_lock = new object();
        private bool m_disposed = false;
        private AutoResetEvent m_taskChangeEvt = new AutoResetEvent(false);

        private bool m_taskMonitorThreadRunning = false;
        private ManualResetEvent m_threadStopEvt = new ManualResetEvent(false);
        private Thread m_taskMonitorThread = null;
        private BlockingCollection<VertexTask> m_taskUpdateQueue;

        public AMInstance m_appMaster;  

        private const int GM_EXITCODE_CANNOT_ACCESS_SCHEDULER = 1000;

        #region Events

        event VertexChangeEventHandler ISchedulerHelper.OnVertexChange
        {
            add
            {
                lock (m_eventLock)
                {
                    m_vertexChangeEvent += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_vertexChangeEvent -= value;
                }
            }
        }

        private AMInstance GetScheduler()
        {
            return m_appMaster;
        }

        #endregion

        #region Properties

        private int JobMinNodes
        {
            get
            {
                return m_minNodes;
            }
        }

        private int JobMaxNodes
        {
            get
            {
                return m_maxNodes;
            }
        }

        private int JobStartNodes
        {
            get
            {
                return m_startNodes;
            }
        }

        #endregion

        public YarnSchedulerHelper()
        {
            // init the DryadLogger, just to make sure
            DryadLogger.Start("xcompute.log");
            m_taskUpdateQueue = new BlockingCollection<VertexTask>();

            // if we are not running in a vertex, then init the GM
            string jmString = Environment.GetEnvironmentVariable(Constants.jobManager);
            if (String.IsNullOrEmpty(jmString))
            {
                m_minNodes = int.Parse(Environment.GetEnvironmentVariable("MINIMUM_COMPUTE_NODES"));
                m_maxNodes = int.Parse(Environment.GetEnvironmentVariable("MAXIMUM_COMPUTE_NODES"));
                m_startNodes = m_minNodes;
                
                m_vertices = new VertexTask[JobMaxNodes + 2];
                DryadLogger.LogInformation("YarnSchedulerHelper()", "Initializing JAVA GM");
                DryadLogger.LogInformation("YarnSchedulerHelper()", "m_maxNodes: {0}", m_maxNodes);
                AMInstance.RegisterGMCallback(new UpdateProcessState(QueueYarnUpdate));
                ((ISchedulerHelper)this).OnVertexChange += new VertexChangeEventHandler(OnVertexChangeHandler);
                m_appMaster = new AMInstance();

            }
            else
            {
                m_vertices = new VertexTask[JobMaxNodes + 2];
                DryadLogger.LogInformation("YarnSchedulerHelper()", "Not initializing JAVA GM");
            }
            
            
        }

        #region Methods

        void IDisposable.Dispose()
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
                    if (m_appMaster != null)
                    {
                        m_appMaster.Close();
                    }
                    this.m_taskChangeEvt.Close();
                }

                m_disposed = true;
            }
        }

        void ISchedulerHelper.FinishJob()
        {
            m_appMaster.Finish();
        }

        string ISchedulerHelper.GetVertexServiceBaseAddress(string nodename, int instanceId)
        {
            return String.Format(Constants.vertexAddrFormat, nodename, instanceId);
        }

        NetTcpBinding ISchedulerHelper.GetVertexServiceBinding()
        {
            NetTcpBinding binding = new NetTcpBinding(SecurityMode.Transport, false);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = ProtectionLevel.None;
            binding.SendTimeout = Constants.VertexSendTimeout;
            binding.ReceiveTimeout = Constants.ReceiveTimeout;
            binding.MaxReceivedMessageSize = Constants.MaxReceivedMessageSize;
            binding.MaxBufferPoolSize = Constants.MaxBufferPoolSize;
            binding.MaxConnections = Constants.MaxConnections;
            binding.ListenBacklog = Constants.ListenBacklog;
            binding.ReaderQuotas = System.Xml.XmlDictionaryReaderQuotas.Max;

            return binding;

        }

        void ISchedulerHelper.SetJobProgress(int n, string message)
        {
            DryadLogger.LogWarning("SetJobProgress", "n: {0} message: {1}", n, message);
        }



        bool ISchedulerHelper.StartTaskMonitorThread()
        {
            // We only want to have one of these threads running, in case we get called more than once
            if (m_taskMonitorThreadRunning == false)
            {
                lock (m_lock)
                {
                    if (m_taskMonitorThreadRunning == false)
                    {
                        ((ISchedulerHelper)this).OnVertexChange += new VertexChangeEventHandler(OnVertexChangeHandler);
                        try
                        {
                            m_taskMonitorThread = new Thread(new ThreadStart(TaskMonitorThread));
                            m_taskMonitorThread.Start();
                            m_taskMonitorThreadRunning = true;
                            return true;
                        }
                        catch (Exception e)
                        {
                            DryadLogger.LogCritical(0, e, "Failed to start task monitoring thread");
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        void ISchedulerHelper.StopTaskMonitorThread()
        {
            DryadLogger.LogMethodEntry();
            bool wait = false;
            if (m_taskMonitorThreadRunning)
            {
                lock (m_lock)
                {
                    if (m_taskMonitorThreadRunning)
                    {
                        m_threadStopEvt.Set();
                        wait = true;
                    }
                }
            }

            if (wait)
            {
                try
                {
                    m_taskMonitorThread.Join();
                }
                catch (Exception e)
                {
                    DryadLogger.LogError(0, e, "Failed to wait for task monitor thread to stop.");
                }
            }
            DryadLogger.LogMethodExit();
        }

        private VertexTaskState YarnTaskStateToVertexTaskState(YarnTaskState ts)
        {
            VertexTaskState vts = VertexTaskState.NA;
            if (ts == YarnTaskState.NA)
            {
                vts = VertexTaskState.NA;
            }
            else if (ts < YarnTaskState.Running)
            {
                vts = VertexTaskState.Waiting;
            }
            else if (ts == YarnTaskState.Running)
            {
                vts = VertexTaskState.Running;
            }
            else
            {
                switch (ts)
                {
                    case YarnTaskState.Completed:
                        vts = VertexTaskState.Finished;
                        break;
                    case YarnTaskState.Failed:
                        vts = VertexTaskState.Failed;
                        break;
                    //case TaskState.Canceled:
                    //case TaskState.Canceling:
                    //    vts = VertexTaskState.Canceled;
                    //    break;
                }
            }
            DryadLogger.LogDebug("Task State", "Mapped ts: {0} to vts: {1}", ts, vts);
            return vts;
        }

        bool ISchedulerHelper.WaitForTasksReady()
        {
            // The basic strategy is to wait for the maximum number of vertex tasks which is 
            // practical. Start by waiting for AllocatedNodes.Count.  As tasks fail or are cancelled,
            // decrement the number of tasks to wait for until we drop below Min at which time the
            // scheduler will end the job. Also, if tasks are rerun, increment the number of tasks to wait for.
            do
            {
                // Event set by the Task Monitor Thread when it finishes processes a batch of changes.
                m_taskChangeEvt.WaitOne();

                // Don't want OnVertexChangeHandler updating these counts while we're checking them
                lock (this)
                {
                    DryadLogger.LogInformation("Wait for vertex tasks",
                                               "{0} tasks are running, waiting for at least {1} before starting",
                                               m_runningTasks, m_startNodes);
                    if (m_runningTasks >= m_startNodes)
                    {
                        // We have enough running tasks to start
                        DryadLogger.LogDebug("Wait for vertex tasks", 
                            "Sufficient number of tasks transitioned to running to begin: {0} running tasks", 
                            m_runningTasks);
                        return true;
                    }
                }

            } while (true);
        }

        public void QueueYarnUpdate(int taskId, int taskState, string nodeName)
        {
            DryadLogger.LogInformation("QueueYarnUpdate", "Task {0} on node {2} is in state {3}", taskId, nodeName,
                                       taskState);
            // Set change event arguments
            YarnTaskState yTaskState = (YarnTaskState)taskState;
            VertexTask v = new VertexTask(taskId, nodeName, yTaskState, int.MaxValue, DateTime.UtcNow);
            m_taskUpdateQueue.Add(v);
        }

        public void ProcessYarnUpdate(VertexTask v)
        {
            DryadLogger.LogInformation("ProcessYarnUpdate", "Task {0} on node {1} is in state {2}", v.Id, v.Node,
                                       v.State);
            VertexChangeEventArgs e = new VertexChangeEventArgs(v.Id);

            e.NewNode = v.Node;
            e.NewState = YarnTaskStateToVertexTaskState(v.State);
            e.NewRequeueCount = v.RequeueCount;

            if (m_vertices[v.Id] != null)
            {
                e.OldNode = m_vertices[v.Id].Node;
                e.OldState = YarnTaskStateToVertexTaskState(m_vertices[v.Id].State);
                e.OldRequeueCount = m_vertices[v.Id].RequeueCount;
            }

            if (e.NewRequeueCount != e.OldRequeueCount)
            {
                DryadLogger.LogInformation("ProcessYarnUpdate", "Task {0} requeue count changed from {1} to {2}",
                                           v.Id, e.OldRequeueCount, e.NewRequeueCount);
            }

            // Update current vertex state
            m_vertices[v.Id] = v;
            m_vertexChangeEvent(this, e);
            //m_taskChangeEvt.Set();
        }

         private void TaskMonitorThread()
        {
            TimeSpan pollInterval = TimeSpan.FromSeconds(1);
            TimeSpan maxPollInterval = TimeSpan.FromSeconds(16);

            // The main loop.  Each iteration polls for task changes.
            while (true)
            {
                bool foundUpdate = false;
                DateTime loopStartTime = DateTime.Now;
                //
                // Process change results from blocking queue
                //
                do
                {
                    VertexTask v = null;
                    if (m_taskUpdateQueue.TryTake(out v, pollInterval))
                    {
                        foundUpdate = true;
                        ProcessYarnUpdate(v);
                    }

                } while ((DateTime.Now - loopStartTime) < pollInterval);

                if (foundUpdate)
                {
                    // Notify WaitForTasksReady once for each polling cycle
                    // so that it gets all the changes in one batch
                    m_taskChangeEvt.Set();
                }

                // Check to see if we've been told to stop.
                // Timeout after pollInterval.
                // TODO: For better shutdown perf, we may want to check this at other places
                // or just kill the thread - but this provides a more graceful exit.
                if (m_threadStopEvt.WaitOne(pollInterval, true))
                {
                    m_taskMonitorThreadRunning = false;
                    DryadLogger.LogInformation("Task Monitoring Thread", "Received shutdown event");
                    return;
                }

                // Double the polling interval each iteration up to maxPollInterval
                if (pollInterval < maxPollInterval)
                {
                    double newSeconds = 2 * pollInterval.TotalSeconds;
                    if (newSeconds < maxPollInterval.TotalSeconds)
                    {
                        pollInterval = TimeSpan.FromSeconds(newSeconds);
                    }
                    else
                    {
                        pollInterval = maxPollInterval;
                    }
                }
                
            }
        }

        private void OnVertexChangeHandler(object sender, VertexChangeEventArgs ve)
        {
            if (ve.OldState != ve.NewState)
            {
                // Don't want to update counts while WaitForTasksReady is checking them
                lock (this)
                {
                    if (ve.OldState == VertexTaskState.Running)
                    {
                        m_runningTasks--;
                    }
                    else if (ve.OldState > VertexTaskState.Running)
                    {
                        m_finishedTasks--;
                        // Task transitioning from a completed state so we can increment
                        // the number of tasks to wait for at startup
                        m_startNodes++;
                    }

                    if (ve.NewState == VertexTaskState.Running)
                    {
                        m_runningTasks++;
                    }
                    else if (ve.NewState > VertexTaskState.Running)
                    {
                        m_finishedTasks++;
                        // Task transitioning to a completed state so we need to
                        // decrement the number of tasks to wait for at startup.
                        m_startNodes--;
                    }
                }
            }
        }

        #endregion
    }


}
