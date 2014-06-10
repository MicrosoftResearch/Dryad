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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.ClusterInterface
{
    /// <summary>
    /// this is the connection to the application's logging interface, supplied
    /// by the external application
    /// </summary>
    public interface ILogger
    {
        void Log(
            string entry,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1);
    }

    /// <summary>
    /// this is the information available to the external application about a
    /// "computer" in the cluster. This is really a slot in a cluster's resource manager;
    /// there may be multiple computers on the same host.
    /// </summary>
    public interface IComputer
    {
        /// <summary>
        /// the unique name of this resource
        /// </summary>
        string Name { get; }
        /// <summary>
        /// the URI for communicating with processes
        /// </summary>
        string ProcessServer { get; }
        /// <summary>
        /// the URI for fetching remote files
        /// </summary>
        string FileServer { get; }
        /// <summary>
        /// the directory for fetching local files
        /// </summary>
        string Directory { get; }
        /// <summary>
        /// the hostname this is running on, for data locality purposes
        /// </summary>
        string Host { get; }
        /// <summary>
        /// the cluster rack this is running on, for data locality purposes
        /// </summary>
        string RackName { get; }
    }

    /// <summary>
    /// this is the information available to the external application about a Dryad
    /// process that has been started on an IComputer. Basically just a UID to use in
    /// logging, and an interface for constructing references to files that it wrote
    /// </summary>
    public interface IProcess
    {
        /// <summary>
        /// a UID for this process
        /// </summary>
        string Id { get; }
        /// <summary>
        /// the directory for files written by this process
        /// </summary>
        string Directory { get; }
    }

    /// <summary>
    /// affinities can be described at different levels. We only use Computer and Rack
    /// for now
    /// </summary>
    public enum AffinityResourceLevel
    {
        Core = 0,
        Socket,
        Host,
        Rack,
        Cluster
    }

    /// <summary>
    /// the external application supplies affinity resources when making scheduling requests
    /// </summary>
    public class AffinityResource
    {
        /// <summary>
        /// the granularity of this affinity; for now we only use Computer and Rack
        /// </summary>
        public AffinityResourceLevel level;
        /// <summary>
        /// the identifying string for the affinity. It is a UID with respect to the level,
        /// so identifies a unique host or rack
        /// </summary>
        public string locality;

        /// <summary>
        /// create a new affinity object
        /// </summary>
        /// <param name="l">the granularity of the affinity</param>
        /// <param name="place">the locality of the affinity</param>
        public AffinityResource(AffinityResourceLevel l, string place)
        {
            level = l;
            locality = place;
        }
    }

    /// <summary>
    /// a particular affinity that a process has to be scheduled somewhere
    /// </summary>
    public class Affinity
    {
        /// <summary>
        /// if this is true, the process may not run anywhere else
        /// </summary>
        public bool isHardContraint;
        /// <summary>
        /// this is a list of equally good places to run, e.g. the locations of
        /// all the replicas of a file
        /// </summary>
        public List<AffinityResource> affinities;
        /// <summary>
        /// this is a weight, corresponding to the amount of data the process
        /// would like to read from the locality in question
        /// </summary>
        public UInt64 weight;

        /// <summary>
        /// make a new affinity descriptor
        /// </summary>
        /// <param name="hc">true if this is a hard constraint and the process may not run anywhere else</param>
        /// <param name="w">weight corresponding to the amount of data the process would like to read from here</param>
        public Affinity(bool hc, UInt64 w)
        {
            isHardContraint = hc;
            affinities = new List<AffinityResource>();
            weight = w;
        }
    }

    /// <summary>
    /// state returned when a the status of a key at a process is queried after the process has started running
    /// </summary>
    public enum ProcessState
    {
        /// <summary>
        /// the process is still running
        /// </summary>
        Running,
        /// <summary>
        /// the process has reported successful completion
        /// </summary>
        Completed,
        /// <summary>
        /// the process has exited without reporting successful completion
        /// </summary>
        Failed
    }

    /// <summary>
    /// state returned when a process has exited
    /// </summary>
    public enum ProcessExitState
    {
        /// <summary>
        /// the process was never started due to an error
        /// </summary>
        ScheduleFailed,
        /// <summary>
        /// contact was lost with the process
        /// </summary>
        StatusFailed,
        /// <summary>
        /// the process was canceled while scheduling
        /// </summary>
        ScheduleCanceled,
        /// <summary>
        /// the process had started, and has now exited
        /// </summary>
        ProcessExited
    }

    /// <summary>
    /// When the higher level of the software stack wants to schedule a process, it
    /// passes in an object that implements IProcessWatcher to receive updates in the
    /// form of callbacks as the process is queued, matched, scheduled, run, etc.
    /// </summary>
    public interface IProcessWatcher
    {
        /// <summary>
        /// OnQueued is called when the process has been placed in the scheduling queues.
        /// </summary>
        void OnQueued();

        /// <summary>
        /// OnMatched is called when the process has been matched to <param>computer</param>
        /// and is about to be scheduled there.
        /// </summary>
        /// <param name="computer">The computer the process has been scheduled on</param>
        /// <param name="timestamp">The UTC time on the local computer that the process was scheduled</param>
        void OnMatched(IComputer computer, long timestamp);

        /// <summary>
        /// OnCreated is called when the process has been created on the remote computer.
        /// </summary>
        /// <param name="timestamp">The UTC time on the local computer that the remote daemon responded to the process create request</param>
        void OnCreated(long timestamp);

        /// <summary>
        /// OnStarted is called when the process has started running on the remote computer.
        /// </summary>
        /// <param name="timestamp">The UTC time on the remote computer that the process started running</param>
        void OnStarted(long timestamp);

        /// <summary>
        /// OnExited is called when the process has finished, either because it could not be
        /// created (state=ScheduleFailed), because contact was lost with its daemon
        /// (state=StatusFailed) or because it has finished (state=ProcessExited). exitCode is
        /// the process exit code
        /// </summary>
        /// <param name="state">How far through scheduling the process got</param>
        /// <param name="timestamp">The UTC time on the remote computer that the process stopped running, or on the local computer
        /// if we lost contact with the remote daemon</param>
        /// <param name="exitCode">The exit code of the process if it was started, or 1 otherwise</param>
        /// <param name="errorText">A description of the error if the process didn't exit cleanly</param>
        void OnExited(ProcessExitState state, long timestamp, int exitCode, string errorText);
    }

    /// <summary>
    /// when the application wants to learn the status of a key on a running process, it passes in an
    /// IProcessStatus object identifying the key being queried, and including a callback method
    /// that is called when the status is known
    /// </summary>
    public interface IProcessKeyStatus
    {
        /// <summary>
        /// the key to query at the process, filled in by the application
        /// </summary>
        /// <returns>the key the application wants to query</returns>
        string GetKey();

        /// <summary>
        /// the heartbeat timeout, filled in by the application
        /// </summary>
        /// <returns>how long to block waiting for the key to change before returning its status.
        /// OnCompleted will be called either when the version increases, or the timeout expires.</returns>
        int GetTimeout();

        /// <summary>
        /// the last known version of the key, filled in by the application
        /// </summary>
        /// <returns>the last version of the key seen by the application. OnCompleted will be called either
        /// when the version increases, or the timeout expires.</returns>
        UInt64 GetVersion();

        /// <summary>
        /// called by the cluster interface when the status query completes
        /// </summary>
        /// <param name="newVersion">the new version of the key at the remote process</param>
        /// <param name="statusData">the value of the key at the remote process</param>
        /// <param name="processExitCode">the exit code if the process has finished, or 259 (STILL_ACTIVE)</param>
        /// <param name="errorMessage">a descriptive message if something went wrong</param>
        void OnCompleted(UInt64 newVersion, byte[] statusData, int processExitCode, string errorMessage);
    }

    /// <summary>
    /// when the application wants to set a command key on a running process, it passes in an
    /// IProcessCommand object identifying the key being set, and including a callback method
    /// that is called when the RPC completes
    /// </summary>
    public interface IProcessCommand
    {
        /// <summary>
        /// the key to set at the process, filled in by the application
        /// </summary>
        /// <returns>the key the application wants to set</returns>
        string GetKey();

        /// <summary>
        /// a human-friendly summary of the value being set, filled in by the application
        /// </summary>
        /// <returns>summary of the value being set</returns>
        string GetShortStatus();

        /// <summary>
        /// the value to set, filled in by the application
        /// </summary>
        /// <returns>the value being set</returns>
        byte[] GetPayload();

        /// <summary>
        /// called by the cluster interface when the command set completes
        /// </summary>
        /// <param name="reason">null on success, or a descriptive error if there was a problem</param>
        void OnCompleted(string reason);
    }

    public interface ICluster
    {
        /// <summary>
        /// starts up the cluster. Blocks until the application is ready to proceed
        /// </summary>
        /// <returns>false if the cluster startup fails</returns>
        bool Start();

        /// <summary>
        /// shuts down the connection to the cluster during application exit
        /// </summary>
        void Stop();

        /// <summary>
        /// retrieve a list of computers currently available in the cluster. The list
        /// may change as failures occur, or the cluster elastically changes the resource
        /// allocation of the application
        /// </summary>
        /// <returns>the computers currently available in the cluster</returns>
        List<IComputer> GetComputers();

        /// <summary>
        /// get a Uri to read a file from a computer running on the same host
        /// </summary>
        /// <param name="computer">the computer that wrote the file</param>
        /// <param name="fileName">the leafname of the file</param>
        /// <param name="compressionMode">an integer compression mode to put in the query part of the Uri</param>
        /// <returns>a uri that identifies the file locally</returns>
        string GetLocalFilePath(IComputer computer, string directory, string fileName, int compressionMode);

        /// <summary>
        /// get a Uri to read a file from a remote computer
        /// </summary>
        /// <param name="computer">the computer that wrote the file</param>
        /// <param name="fileName"></param>
        /// <param name="compressionMode">an integer compression mode to put in the query part of the Uri</param>
        /// <returns>a uri that identifies the file remotely</returns>
        string GetRemoteFilePath(IComputer computer, string directory, string fileName, int compressionMode);

        /// <summary>
        /// generate a new Process object that will be used to schedule a process on a cluster
        /// computer
        /// </summary>
        /// <param name="watcher">the callback handler that the cluster will use to update
        /// the application on the process' lifecycle</param>
        /// <param name="commandLine">the command line to use to start the process on the remote
        /// computer</param>
        /// <param name="commandLineArguments">arguments to provide to the remote process</param>
        /// <returns>a handle to the new process</returns>
        IProcess NewProcess(IProcessWatcher watcher, string commandLine, string commandLineArguments);

        /// <summary>
        /// request that a Process object, return from NewProcess, be scheduled according
        /// to the supplied affinity hints and constraints
        /// </summary>
        /// <param name="process">the handle to the previously-created process</param>
        /// <param name="affinities">the hints and constraints about where the process should be run</param>
        void ScheduleProcess(IProcess process, List<Affinity> affinities);

        /// <summary>
        /// request that a process, previously created using NewProcess, be canceled, either before it is
        /// scheduled or after it starts running.
        /// </summary>
        /// <param name="process">the handle to the process</param>
        void CancelProcess(IProcess process);

        /// <summary>
        /// query the status of a key at a running process
        /// </summary>
        /// <param name="process">the handle to the process</param>
        /// <param name="status">a description of the key, and a callback when the query completes</param>
        void GetProcessStatus(IProcess process, IProcessKeyStatus status);

        /// <summary>
        /// set a command key at a running process
        /// </summary>
        /// <param name="process">the handle to the process</param>
        /// <param name="command">a description of the command, and callback when the RPC completes</param>
        void SetProcessCommand(IProcess process, IProcessCommand command);
    }

    public class HttpClient
    {
        private class PLogger : Microsoft.Research.Peloponnese.ILogger
        {
            private readonly ILogger logger;

            public PLogger(ILogger l)
            {
                logger = l;
            }

            public void Log(
                string entry,
                [CallerFilePath] string file = "(nofile)",
                [CallerMemberName] string function = "(nofunction)",
                [CallerLineNumber] int line = -1)
            {
                logger.Log(entry, file, function, line);
            }

            public void Stop()
            {
            }
        }

        private static readonly string dummy;
        private static NotHttpClient client;

        static HttpClient()
        {
            dummy = "string to lock";
        }

        public static void Initialize(ILogger logger)
        {
            lock (dummy)
            {
                if (client == null)
                {
                    client = new NotHttpClient(true, 1, 30000, new PLogger(logger));
                }
            }
        }

        public static IHttpRequest Create(string uri)
        {
            return client.CreateRequest(uri);
        }

        public static IHttpRequest Create(Uri uri)
        {
            return client.CreateRequest(uri);
        }
    }

    // -----------------------
    // below are interfaces implemented by schedulers
    // -----------------------

    /// <summary>
    /// the handle for a process used internally by a scheduler
    /// </summary>
    public interface ISchedulerProcess
    {
        /// <summary>
        /// a unique ID assigned by the scheduler to the process
        /// </summary>
        string Id { get; }
    }

    /// <summary>
    /// the method called by the scheduler when a process is ready to be run on the cluster. When the returned
    /// Task completes the scheduler reclaims the resource that the process was using.
    /// </summary>
    /// <param name="computer">The location where the process has been scheduled or null if there was a scheduling error</param>
    /// <param name="processId">A unique integer ID for the process at the computer or -1 if there was a scheduling error</param>
    /// <param name="blocker">This Task completes if the computer is being shut down in which case
    /// RunProcess should return early, or null if there was a scheduling error</param>
    /// <param name="errorReason">null if the process was scheduled, otherwise an error explaining the reason it wasn't</param>
    /// <returns>a Task that completes when the process finishes. This should be immediately returned complete if there was a
    /// scheduling error</returns>
    public delegate Task RunProcess(IComputer computer, int processId, Task interrupt, string errorReason);

    /// <summary>
    /// the interface implemented by a Dryad scheduler
    /// </summary>
    public interface IScheduler
    {
        /// <summary>
        /// Start the scheduler.
        /// </summary>
        /// <returns>true if the scheduler started successfully</returns>
        bool Start();

        /// <summary>
        /// get a snapshot of the available computers in the cluster. This may change later
        /// due to failures or elastic resource allocation.
        /// </summary>
        /// <returns></returns>
        List<IComputer> GetComputers();

        /// <summary>
        /// get a computer that is currently running on a given host, or null if there isn't one
        /// </summary>
        /// <param name="host">the hostname to look up a computer on</param>
        /// <returns>computer at the host, or null if there isn't one</returns>
        IComputer GetComputerAtHost(string host);

        /// <summary>
        /// get a handle to a new process that can be scheduled later
        /// </summary>
        /// <returns>the handle</returns>
        ISchedulerProcess NewProcess();

        /// <summary>
        /// add a process to the scheduling queues, along with affinity information about where it would
        /// prefer to run and a callback that is triggered when the process has been scheduled
        /// </summary>
        /// <param name="process">a handle for the process, created earlier using NewProcess</param>
        /// <param name="affinities">a description of the hints/constraints about where the process should run</param>
        /// <param name="onScheduled">a callback that is invoked when the process has been scheduled, or if a
        /// scheduling error occurs</param>
        void ScheduleProcess(ISchedulerProcess process, List<Affinity> affinities, RunProcess onScheduled);

        /// <summary>
        /// cancel the scheduling of a process. This will trigger the onScheduled callback if it has not already
        /// been sent, otherwise it does nothing.
        /// </summary>
        /// <param name="process">a handle to the process to be canceled</param>
        void CancelProcess(ISchedulerProcess process);

        /// <summary>
        /// shut down the scheduler
        /// </summary>
        void Stop();
    }

    /// <summary>
    /// a factory managing available schedulers
    /// </summary>
    public class Factory
    {
        /// <summary>
        /// delegate to create a scheduler
        /// </summary>
        /// <param name="logger">handle to the application logging interface</param>
        /// <returns>a new scheduler</returns>
        public delegate IScheduler MakeFunction(ILogger logger);

        /// <summary>
        /// table of registered schedulers each associated with a string
        /// </summary>
        private static Dictionary<string, MakeFunction> registrations;

        /// <summary>
        /// static initializer
        /// </summary>
        static Factory()
        {
            registrations = new Dictionary<string, MakeFunction>();
        }

        /// <summary>
        /// called by a scheduler dll to register itself with the factory
        /// </summary>
        /// <param name="type">string used to identify the scheduler</param>
        /// <param name="factory">factory function to make a concrete instance of the scheduler</param>
        /// <returns>true if it was registered</returns>
        public static void Register(string type, MakeFunction factory)
        {
            registrations.Add(type, factory);
        }

        /// <summary>
        /// used internally to create a scheduler
        /// </summary>
        /// <param name="type">string that the scheduler registered with</param>
        /// <param name="logger">handle to the application logging interface</param>
        /// <returns>a concrete scheduler</returns>
        internal static IScheduler CreateScheduler(string type, ILogger logger)
        {
            MakeFunction factory;
            if (registrations.TryGetValue(type, out factory))
            {
                return factory(logger);
            }
            else
            {
                throw new ApplicationException("Unknown scheduler type " + type);
            }
        }
    }
}
