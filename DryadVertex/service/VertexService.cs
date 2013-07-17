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
//      Implementation of the vertex service
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Globalization;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Runtime.Serialization;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.Threading;
    using System.Configuration;
    using System.IO;
    using System.Diagnostics;
    using System.Management;
    using System.Runtime.InteropServices;
    using Microsoft.Research.Dryad;

    /// <summary>
    /// Class that holds all information needed to make a property request
    /// </summary>
    internal class PropertyRequest
    {
        public ProcessPropertyInfo[] infos;
        public string blockOnLabel;
        public ulong blockOnVersion;
        public long maxBlockTime;
        public string getPropLabel;
        public bool ProcessStatistics;
        public string replyUri;

        /// <summary>
        /// Constructor - fills in properties
        /// </summary>
        /// <param name="uri"></param>
        /// <param name="infos"></param>
        /// <param name="blockOnLabel"></param>
        /// <param name="blockOnVersion"></param>
        /// <param name="maxBlockTime"></param>
        /// <param name="getPropLabel"></param>
        /// <param name="ProcessStatistics"></param>
        public PropertyRequest(string uri, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics)
        {
            this.infos = infos;
            this.blockOnLabel = blockOnLabel;
            this.blockOnVersion = blockOnVersion;
            this.maxBlockTime = maxBlockTime;
            this.getPropLabel = getPropLabel;
            this.ProcessStatistics = ProcessStatistics;
            this.replyUri = uri;
        }
    }

    /// <summary>
    /// Implementation of the IDryadVertexService and IDryadVertexFileService
    /// </summary>
    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Reentrant, InstanceContextMode = InstanceContextMode.Single, IncludeExceptionDetailInFaults = true)]
    internal class VertexService : IDryadVertexService
    {
        #region members

        public static ManualResetEvent shutdownEvent = new ManualResetEvent(false);
        internal static bool internalShutdown = false;
        internal static Exception ShutdownReason { get; set; }
        private ManualResetEvent initializedEvent = new ManualResetEvent(false);

        // TODO: add synchronization locks as necessary
        private SynchronizedCollection<VertexProcess> vertexProcessTable;

        private StringDictionary vertexEndpointAddresses = new StringDictionary();

        #endregion

        #region Public methods

        /// <summary>
        /// Constructor - called when service first hosted
        /// </summary>
        public VertexService()
        {
            DryadLogger.LogMethodEntry();
            this.vertexProcessTable = new SynchronizedCollection<VertexProcess>();
            System.Threading.ThreadPool.QueueUserWorkItem(new WaitCallback(InitializationThreadProc));
            DryadLogger.LogMethodExit();
        }

        #endregion

        #region IDryadVertexService methods

        /// <summary>
        /// Cancels the vertex process with the provided id
        /// </summary>
        /// <param name="processId">vertex process id</param>
        void IDryadVertexService.CancelScheduleProcess(int processId)
        {
            VertexProcess vp = null;
            DryadLogger.LogMethodEntry(processId);

            try
            {
                vp = FindByDryadId(processId);
                if (vp != null)
                {
                    vp.Cancel(false);
                }
                else
                {
                    DryadLogger.LogWarning("Cancel Process", "Unknown process id {0}", processId);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Cancel Process", "Operation threw exception: {0}", e.ToString());
            }

            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Gets information about the vertex service
        /// </summary>
        /// <returns></returns>
        VertexStatus IDryadVertexService.CheckStatus()
        {
            DryadLogger.LogMethodEntry();
            VertexStatus status = new VertexStatus();
            status.serviceIsAlive = true;

            //
            // Update information about disk usage
            //
            foreach (string disk in Environment.GetLogicalDrives())
            {
                ulong freeDiskSpaceforUser;
                ulong totalDiskSpace;
                ulong freeDiskSpace;

                if (NativeMethods.GetDiskFreeSpaceEx(disk, out freeDiskSpaceforUser, out totalDiskSpace, out freeDiskSpace))
                {
                    status.freeDiskSpaces.Add(disk, freeDiskSpace);
                }
                else
                {
                    // 
                    // Report any errors as warnings, as this is a non-essential call
                    //
                    int errorCode = Marshal.GetLastWin32Error();
                    Exception lastex = Marshal.GetExceptionForHR(errorCode);
                    if (lastex != null)
                    {
                        DryadLogger.LogWarning("Unable to get disk space information", "Disk: {0} Error: {1}", disk, lastex.Message);
                    }
                    else
                    {
                        DryadLogger.LogWarning("Unable to get disk space information", "Disk: {0} Error Code: {1}", disk, errorCode);
                    }
                }
            }

            //
            // Update information about memory usage
            //
            NativeMethods.MEMORYSTATUSEX memStatus = new NativeMethods.MEMORYSTATUSEX();
            if (NativeMethods.GlobalMemoryStatusEx(memStatus))
            {
                status.freePhysicalMemory = memStatus.ullAvailPhys;
                status.freeVirtualMemory = memStatus.ullAvailVirtual;
            }
            else
            {
                // 
                // Report any errors as warnings, as this is a non-essential call
                //
                int errorCode = Marshal.GetLastWin32Error();
                Exception lastex = Marshal.GetExceptionForHR(errorCode);
                if (lastex != null)
                {
                    DryadLogger.LogWarning("Unable to get memory information", "Error: {0}", lastex.Message);
                }
                else
                {
                    DryadLogger.LogWarning("Unable to get memory information", "Error Code: {0}", errorCode);
                }
            }

            //
            // Get process info for each running vertex process
            //
            status.runningProcessCount = 0;
            lock (vertexProcessTable.SyncRoot)
            {
                foreach (VertexProcess vp in this.vertexProcessTable)
                {
                    VertexProcessInfo vpInfo = new VertexProcessInfo();
                    vpInfo.DryadId = vp.DryadId;
                    vpInfo.commandLine = vp.commandLine;
                    vpInfo.State = vp.State;

                    status.vps.Add(vpInfo);

                    if (vp.State == ProcessState.Running)
                    {
                        status.runningProcessCount++;
                    }
                }
            }

            DryadLogger.LogMethodExit(status);
            return status;
        }

        /// <summary>
        /// Initialize the endpoint addresses for each vertex host
        /// </summary>
        /// <param name="vertexEndpointAddresses">List of vertex host addresses</param>
        void IDryadVertexService.Initialize(StringDictionary vertexEndpointAddresses)
        {
            DryadLogger.LogMethodEntry(vertexEndpointAddresses.Count);

            try
            {
                this.vertexEndpointAddresses = vertexEndpointAddresses;
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Initialize", "Operation threw exception: {0}", e.ToString());
            }

            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Removes reference to a vertex process
        /// </summary>
        /// <param name="processId">process id to forget</param>
        void IDryadVertexService.ReleaseProcess(int processId)
        {
            DryadLogger.LogMethodEntry(processId);
            VertexProcess vp = null;
            
            try 
            {
                vp = FindByDryadId(processId);
                if (vp != null)
                {
                    vertexProcessTable.Remove(vp);
                    vp.Dispose();
                }
                else
                {
                    DryadLogger.LogWarning("Release Process", "Unknown process id {0}", processId);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Release Process", "Operation threw exception: {0}", e.ToString());
            }

            DryadLogger.LogMethodExit();
        }

        /// <summary>
        /// Schedule a vertex host process using the provided parameters
        /// </summary>
        /// <param name="replyUri">callback URI</param>
        /// <param name="processId">vertex process id</param>
        /// <param name="commandLine">vertex host command line</param>
        /// <param name="environment">vertex host environment variables</param>
        /// <returns>Success/Failure of starting vertex process thread</returns>
        bool IDryadVertexService.ScheduleProcess(string replyUri, int processId, string commandLine, StringDictionary environment)
        {
            DryadLogger.LogMethodEntry(processId, commandLine);
            bool startSuccess = false;
            Console.WriteLine("Starting process id {0} with commandLIne: '{1}", processId, commandLine);
            try
            {
                VertexProcess newProcess = null;

                lock (vertexProcessTable.SyncRoot)
                {
                    foreach (VertexProcess vp in vertexProcessTable)
                    {
                        if (vp.DryadId == processId)
                        {
                            // This means a previous call to Schedule process partially succeeded:
                            // the call made it to the service but something went wrong with the response
                            // so the GM's xcompute machinery retried the call. We can just return success
                            // for this case rather than tearing down the process and creating a new one.
                            return true;
                        }

                        if (vp.State <= ProcessState.Running)
                        {
                            // There should be no other processes running.
                            // If there are, it means a previous communication error
                            // cause the GM to give up on this node for a while.
                            // Kill anything that's still hanging around.
                            vp.Cancel(true);
                        }
                    }

                    newProcess = new VertexProcess(
                        replyUri,
                        processId,
                        commandLine,
                        environment,
                        OperationContext.Current.Channel.LocalAddress.Uri.ToString()
                        );
                    this.vertexProcessTable.Add(newProcess);
                }

                startSuccess = newProcess.Start(initializedEvent);
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Schedule Process", "Operation threw exception: {0}", e.ToString());
                throw new FaultException<VertexServiceError>(new VertexServiceError("ReleaseProcess", e.ToString()));
            }

            DryadLogger.LogMethodExit(startSuccess);
            return startSuccess;
        }

        /// <summary>
        /// Update properties
        /// </summary>
        /// <param name="replyEpr">callback URI</param>
        /// <param name="processId">vertex process id</param>
        /// <param name="infos">property information</param>
        /// <param name="blockOnLabel">property update label</param>
        /// <param name="blockOnVersion">property update version</param>
        /// <param name="maxBlockTime">maximum time to wait for update</param>
        /// <param name="getPropLabel">property to get</param>
        /// <param name="ProcessStatistics">vertex host process statistics</param>
        /// <returns>success/failure of property update</returns>
        bool IDryadVertexService.SetGetProps(string replyEpr, int processId, ProcessPropertyInfo[] infos, string blockOnLabel, ulong blockOnVersion, long maxBlockTime, string getPropLabel, bool ProcessStatistics)
        {
            DryadLogger.LogMethodEntry(replyEpr, processId);
            bool success = false;

            try
            {
                // Get the vertex process ID
                VertexProcess vp = FindByDryadId(processId);
                if (vp != null)
                {
                    success = vp.SetGetProps(replyEpr, infos, blockOnLabel, blockOnVersion, maxBlockTime, getPropLabel, ProcessStatistics);
                }
                else
                {
                    DryadLogger.LogError(0, null, "Failed to set / get process properties: Unknown process id {0}", processId);
                }
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Set Or Get Process Properties", "Operation threw exception: {0}", e.ToString());
                throw new FaultException<VertexServiceError>(new VertexServiceError("SetGetProps", e.ToString()));
            }

            DryadLogger.LogMethodExit(success);
            return success;
        }

        /// <summary>
        /// Shut down the vertex service
        /// </summary>
        /// <param name="ShutdownCode"></param>
        void IDryadVertexService.Shutdown(uint ShutdownCode)
        {
            DryadLogger.LogMethodEntry(ShutdownCode);

            try
            {
                ReplyDispatcher.ShuttingDown = true;
                VertexService.shutdownEvent.Set();
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Shutdown", "Operation threw exception: {0}", e.ToString());
            }

            DryadLogger.LogMethodExit();
        }

        #endregion

        #region Private methods

        /// <summary>
        /// Get vertex process cooresponding to dryad id
        /// </summary>
        /// <param name="id">dryad id</param>
        /// <returns>vertex process</returns>
        private VertexProcess FindByDryadId(int id)
        {
            lock (vertexProcessTable.SyncRoot)
            {
                foreach (VertexProcess p in vertexProcessTable)
                {
                    if (p.DryadId == id)
                    {
                        return p;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Get vertex process cooresponding to process id
        /// </summary>
        /// <param name="id">process id</param>
        /// <returns>vertex process</returns>
        private VertexProcess FindByProcessId(int id)
        {
            lock (vertexProcessTable.SyncRoot)
            {
                foreach (VertexProcess p in vertexProcessTable)
                {
                    if (p.ProcessId == id)
                    {
                        return p;
                    }
                }
            }
            return null;
        }

        #endregion

        #region Internal methods

        /// <summary>
        /// Fail the vertex service task
        /// </summary>
        internal static void Surrender(Exception ex)
        {
            DryadLogger.LogMethodEntry();
            ReplyDispatcher.ShuttingDown = true;
            VertexService.internalShutdown = true;
            VertexService.ShutdownReason = ex;
            VertexService.shutdownEvent.Set();
            DryadLogger.LogMethodExit();
        }
        #endregion

        #region Thread Functions

        /// <summary>
        /// Initialization thread - initialize job working directory if needed.
        /// </summary>
        /// <param name="state"></param>
        void InitializationThreadProc(Object state)
        {
            try
            {
                if (Environment.GetEnvironmentVariable(Constants.schedulerTypeEnvVar) == Constants.schedulerTypeLocal)
                {
                    initializedEvent.Set();
                }
                else if (ExecutionHelper.InitializeForJobExecution(Environment.GetEnvironmentVariable("XC_RESOURCEFILES")))
                {
                    DryadLogger.LogInformation("InitializationThreadProc", "InitializeForJobExecution was successful."); 
                    initializedEvent.Set();
                }
                else
                {
                    Surrender(new Exception("Failed to initialize vertex service for job execution"));
                }
            }
            catch (Exception ex)
            {
                Surrender(ex);
            }
        }

        #endregion
    }

}
