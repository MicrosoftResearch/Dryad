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
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.ClusterInterface
{
    /// </summary>
    /// This is the container class for a process once it has been scheduled
    /// </summary>
    internal class Process : IProcess
    {
        /// <summary>
        /// internal state to keep track of what we have told the higher level,
        /// and debug bad state transitions
        /// </summary>
        private enum State
        {
            Initializing,
            Queued,
            Matched,
            Created,
            Started,
            Exited
        }

        /// <summary>
        /// internal state to keep track of what we have told the higher level,
        /// and debug bad state transitions
        /// </summary>
        private State state;

        /// <summary>
        /// this is the handle that the scheduler supplies that is used to refer
        /// to the process
        /// </summary>
        private ISchedulerProcess schedulerProcess;

        /// <summary>
        /// task to start when the process is canceled
        /// </summary>
        private TaskCompletionSource<bool> cancelTask;

        /// <summary>
        /// this is the object passed down by higher levels of the software stack,
        /// that receives updates as the process is queued, matched, scheduled, run, etc.
        /// </summary>
        private IProcessWatcher watcher;

        /// <summary>
        /// this is the local directory where the process writes its outputs
        /// </summary>
        private string directory;

        /// <summary>
        /// this is the computer we are running on, once we get scheduled
        /// </summary>
        private IComputer computer;

        /// <summary>
        /// this task is started when the owning computer is shutting down
        /// </summary>
        private Task computerCancellation;

        /// <summary>
        /// statusVersion is the version number associated with the process at the remote
        /// web server, which is incremented every time the process' state changes, e.g.
        /// when it starts running or exits
        /// </summary>
        private UInt64 statusVersion;

        /// <summary>
        /// statusString is the status associated with the process at the remote web
        /// server, which can be Queued, Running, Canceling or Completed
        /// </summary>
        private string statusString;

        /// <summary>
        /// the interface to the application's logging
        /// </summary>
        private ILogger logger;

        /// <summary>
        /// construct a new object to represent the lifecycle of a process being scheduled
        /// </summary>
        public Process(ISchedulerProcess p, IProcessWatcher w, string cmd, string cmdLineArgs, ILogger l)
        {
            schedulerProcess = p;
            state = State.Initializing;
            CommandLine = cmd;
            CommandLineArguments = cmdLineArgs;
            watcher = w;
            cancelTask = new TaskCompletionSource<bool>();
            directory = null;
            statusVersion = 0;
            statusString = "";
            logger = l;
        }

        public ISchedulerProcess SchedulerProcess { get { return schedulerProcess; } }

        /// <summary>
        /// a unique GUID representing the process for logging purposes
        /// </summary>
        public string Id { get { return schedulerProcess.Id; } }

        /// <summary>
        /// the string used to start the remote process
        /// </summary>
        public string CommandLine { get; private set; }

        /// <summary>
        /// arguments provided when starting the remote process
        /// </summary>
        public string CommandLineArguments { get; private set; }

        /// <summary>
        /// the local directory of the process at the daemon's host computer
        /// </summary>
        public string Directory { get { return directory; } }

        /// <summary>
        /// set the computer where the process is running
        /// </summary>
        private void SetComputer(IComputer remote, Task remoteCancel, string suffix)
        {
            // use a lock here because computer can be accessed by other threads trying
            // to get process keys
            lock (this)
            {
                computer = remote;
                computerCancellation = remoteCancel;
                directory = suffix;
            }
        }

        private async Task<string> PostRequest(IComputer computer, string requestString, byte[] payload)
        {
            string uri = computer.ProcessServer + requestString;
            IHttpRequest request = HttpClient.Create(uri);
            request.Timeout = 30 * 1000; // this should come back quickly. If it really takes a long time, something is wrong
            request.Method = "POST";

            try
            {
                using (Stream upload = request.GetRequestStream())
                {
                    await upload.WriteAsync(payload, 0, payload.Length);
                }

                using (IHttpResponse response = await request.GetResponseAsync())
                {
                    // this succeeded but we don't care about the response: null indicates no error
                    return null;
                }
            }
            catch (NotHttpException e)
            {
                string error = "Post " + uri + " failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription;
                logger.Log(error);
                return error;
            }
            catch (Exception e)
            {
                string error = "Post " + uri + " failed message " + e.Message;
                logger.Log(error);
                return error;
            }
        }

        private async Task<bool> Schedule(IComputer computer, Task interrupt)
        {
            logger.Log("Process " + Id + " scheduling itself as " + directory + " on computer " + computer.Name + " at " + computer.Host);

            ToMatched(computer, DateTime.Now.ToFileTimeUtc());
            
            StringBuilder payload = new StringBuilder();
            payload.AppendLine(CommandLine);
            payload.AppendLine(CommandLineArguments);

            Task<string> bail = interrupt.ContinueWith((t) => "");
            Task<string> upload = PostRequest(computer, directory + "?op=create", Encoding.UTF8.GetBytes(payload.ToString()));
            Task<string> completed = await Task.WhenAny(bail, upload);

            if (completed == bail)
            {
                logger.Log("Process " + Id + " abandoned creation due to finishWaiter");
                ToExited(ProcessExitState.ScheduleFailed, DateTime.Now.ToFileTimeUtc(), 1, "Service shut down while scheduling");
                return false;
            }

            if (completed.Result == null)
            {
                logger.Log("Process " + Id + " got remote create process success for " + directory);
                ToCreated(DateTime.Now.ToFileTimeUtc());
                return true;
            }
            else
            {
                logger.Log("Proces " + Id + " got remote create process failure " + completed.Result);
                ToExited(ProcessExitState.ScheduleFailed, DateTime.Now.ToFileTimeUtc(), 1, completed.Result);
                return false;
            }
        }

        private bool UpdateProcessStatus(IHttpResponse response)
        {
            try
            {
                UInt64 newVersion = UInt64.Parse(response.Headers["X-Dryad-ValueVersion"]);
                string status = response.Headers["X-Dryad-ProcessStatus"];
                int exitCode = Int32.Parse(response.Headers["X-Dryad-ProcessExitCode"]);
                long startTime = Int64.Parse(response.Headers["X-Dryad-ProcessStartTime"]);
                long endTime = Int64.Parse(response.Headers["X-Dryad-ProcessEndTime"]);

                statusVersion = newVersion;
                if (status != statusString)
                {
                    statusString = status;
                    if (status == "Queued")
                    {
                        // don't bother to record this 'transition'
                        logger.Log("Process " + Id + " got Queued status");
                    }
                    else if (status == "Running")
                    {
                        logger.Log("Process " + Id + " got Running status");
                        ToStarted(startTime);
                    }
                    else if (status == "Completed")
                    {
                        logger.Log("Process " + Id + " got Completed status");
                        ToExited(ProcessExitState.ProcessExited, endTime, exitCode, "Process exit detected normally");
                        return true;
                    }
                    else
                    {
                        logger.Log("Process " + Id + " got unknown status " + status);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Log("Process " + Id + " got exception " + e.ToString() + " parsing status");
                // we failed to read the headers correctly, which is odd, but we'll assume the process is now dead
                ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, "Failed to read headers " + e.Message);
                return true;
            }

            return false;
        }

        private async Task<bool> GetStatus(IComputer computer, Task interrupt)
        {
            logger.Log("Process " + Id + " getting status on computer " + computer.Name + " at " + computer.Host);

            // use a 2 minute heartbeat for now
            int timeout = 120000;

            StringBuilder sb = new StringBuilder(directory);
            sb.AppendFormat("?version={0}", statusVersion);
            sb.AppendFormat("&timeout={0}", timeout);

            Task<IHttpResponse> completed;

            try
            {
                IHttpRequest request = HttpClient.Create(computer.ProcessServer + sb.ToString());
                request.Timeout = timeout + 30000;

                Task<IHttpResponse> bail = interrupt.ContinueWith((t) => null as IHttpResponse);
                completed = await Task.WhenAny(bail, request.GetResponseAsync());

                if (completed == bail)
                {
                    logger.Log("Process " + Id + " abandoned status due to finishWaiter");
                    ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, "Service stopped while waiting for status");
                    return true;
                }
            }
            catch (NotHttpException e)
            {
                string error = "Status fetch failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription;
                logger.Log("Process " + Id + " got remote process status failure " + error);
                ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, error);
                return true;
            }
            catch (Exception e)
            {
                string error = "Status fetch failed message " + e.Message;
                logger.Log("Process " + Id + " got remote process status failure " + error);
                ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, error);
                return true;
            }

            using (IHttpResponse response = completed.Result)
            {
                try
                {
                    // read the empty payload to the end to keep the protocol happy
                    using (Stream payloadStream = response.GetResponseStream())
                    {
                    }
                }
                catch (NotHttpException e)
                {
                    string error = "Status fetch failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription;
                    logger.Log("Process " + Id + " got remote process status failure " + error);
                    ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, error);
                    return true;
                }
                catch (Exception e)
                {
                    string error = "Status fetch failed message " + e.Message;
                    logger.Log("Process " + Id + " got remote process status failure " + error);
                    ToExited(ProcessExitState.StatusFailed, DateTime.Now.ToFileTimeUtc(), 1, error);
                    return true;
                }

                return UpdateProcessStatus(response);
            }
        }

        public async Task Kill(IComputer computer, Task interrupt)
        {
            logger.Log("Process " + Id + " sending remote kill to computer " + computer.Name + " on host " + computer.Host);

            Task<string> bail = interrupt.ContinueWith((t) => "");
            Task<string> upload = PostRequest(computer, directory + "?op=kill", new byte[0]);
            Task<string> completed = await Task.WhenAny(bail, upload);

            if (completed == bail)
            {
                logger.Log("Process " + Id + " abandoned kill due to finishWaiter");
                return;
            }

            if (completed.Result == null)
            {
                logger.Log("Process " + Id + " got successful response for kill");
            }
            else
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Process " + Id + " got failure response for kill " + completed.Result);
            }
        }

        private Task AsyncCancelTask
        {
            get { return cancelTask.Task.ContinueWith((t) => { }); }
        }

        public async Task Run(IComputer computer, int processId, Task computerInterrupt, string errorReason)
        {
            if (errorReason != null)
            {
                ToExited(ProcessExitState.ScheduleFailed, DateTime.Now.ToFileTimeUtc(), 1, errorReason);
                return;
            }

            // get a unique id for this process on this computer, and store the identifying
            // suffix that we will use to refer to it
            SetComputer(computer, computerInterrupt, processId.ToString());

            logger.Log("Process " + Id + " matched to computer " + computer.Name + " on " + computer.Host);

            {
                Task interrupt = Task.WhenAny(computerInterrupt, AsyncCancelTask);

                bool exited = !(await Schedule(computer, interrupt));

                while (!exited)
                {
                    logger.Log("Process " + Id + " getting status from " + computer.Name + " on " + computer.Host);
                    exited = await GetStatus(computer, interrupt);
                }
            }

            logger.Log("Process " + Id + " ensuring it is killed at " + computer.Name + " on " + computer.Host);

            // we shouldn't get here until the process has exited unless we got a cancellation, but just for belt
            // and braces we'll always try to make sure it's really dead at the other end
            await Kill(computer, computerInterrupt);

            logger.Log("Process " + Id + " finished running at " + computer.Name + " on " + computer.Host);
        }

        public void Cancel()
        {
            cancelTask.SetResult(true);
        }

        public async Task GetKeyStatus(IProcessKeyStatus status)
        {
            logger.Log("Process " + Id + " sending key/value fetch for " +
                status.GetKey() + ":" + status.GetVersion() + ":" + status.GetTimeout());

            IComputer remote;
            Task computerInterrupt;
            lock (this)
            {
                // use a lock to ensure memory safety since these are set on another thread
                remote = computer;
                computerInterrupt = computerCancellation;
            }

            Task interrupt = Task.WhenAny(computerInterrupt, AsyncCancelTask);

            StringBuilder sb = new StringBuilder(directory);
            sb.AppendFormat("?key={0}", status.GetKey());
            sb.AppendFormat("&timeout={0}", status.GetTimeout());
            sb.AppendFormat("&version={0}", status.GetVersion());

            Task<IHttpResponse> completed;

            try
            {
                IHttpRequest request = HttpClient.Create(remote.ProcessServer + sb.ToString());
                request.Timeout = status.GetTimeout() + 30000;

                Task<IHttpResponse> bail = interrupt.ContinueWith((t) => null as IHttpResponse);
                completed = await Task.WhenAny(bail, request.GetResponseAsync());

                if (completed == bail)
                {
                    logger.Log("Process " + Id + " abandoned property fetch due to interrupt");
                    return;
                }
            }
            catch (NotHttpException e)
            {
                string error = "Status fetch failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription;
                logger.Log("Process " + Id + " got remote property fetch failure from " + remote.Name +
                    " at " + remote.Host + ": " + error);
                status.OnCompleted(0, null, 1, error);
                return;
            }
            catch (Exception e)
            {
                string error = "Status fetch failed message " + e.ToString();
                logger.Log("Process " + Id + " got remote property fetch failure from " + remote.Name +
                    " at " + remote.Host + ": " + error);
                status.OnCompleted(0, null, 1, error);
                return;
            }

            using (IHttpResponse response = completed.Result)
            {
                try
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        Task payload;
                        using (Stream payloadStream = response.GetResponseStream())
                        {
                            payload = await Task.WhenAny(interrupt, payloadStream.CopyToAsync(ms));
                        }

                        if (payload == interrupt)
                        {
                            logger.Log("Process " + Id + " abandoned property fetch due to interrupt");
                            return;
                        }

                        logger.Log("Process " + Id + " completed property fetch");

                        UInt64 newVersion = UInt64.Parse(response.Headers["X-Dryad-ValueVersion"]);
                        string stateString = response.Headers["X-Dryad-ProcessStatus"];
                        int exitCode = Int32.Parse(response.Headers["X-Dryad-ProcessExitCode"]);

                        logger.Log("Process " + Id + " property fetch: " + status.GetKey() + ":" + newVersion + ":" + stateString);

                        status.OnCompleted(newVersion, ms.ToArray(), exitCode, null);
                    }
                }
                catch (NotHttpException e)
                {
                    string error = "Status fetch failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription;
                    logger.Log("Process " + Id + " got remote property fetch failure from " + remote.Name +
                        " at " + remote.Host + ": " + error);
                    status.OnCompleted(0, null, 1, error);
                }
                catch (Exception e)
                {
                    string error = "Header fetch failed message " + e.ToString() + " headers " + response.Headers;
                    logger.Log("Process " + Id + " got remote property fetch failure from " + remote.Name +
                        " at " + remote.Host + ": " + error);
                    status.OnCompleted(0, null, 1, error);
                }
            }
        }

        public async Task SetCommand(IProcessCommand command)
        {
            logger.Log("Process " + Id + " sending property command for " +
                command.GetKey() + ":" + command.GetShortStatus());

            IComputer remote;
            Task computerInterrupt;
            lock (this)
            {
                // use a lock to ensure memory safety since these are set on another thread
                remote = computer;
                computerInterrupt = computerCancellation;
            }

            Task interrupt = Task.WhenAny(computerInterrupt, AsyncCancelTask);

            StringBuilder sb = new StringBuilder(directory);
            sb.AppendFormat("?op=setstatus");
            sb.AppendFormat("&key={0}", command.GetKey());
            sb.AppendFormat("&shortstatus={0}", command.GetShortStatus());
            sb.AppendFormat("&notifywaiters=true");

            Task<string> bail = interrupt.ContinueWith((t) => "");
            Task<string> upload = PostRequest(remote, sb.ToString(), command.GetPayload());
            Task<string> completed = await Task.WhenAny(bail, upload);

            if (completed == bail)
            {
                logger.Log("Process " + Id + " abandoned property command due to interrupt");
                return;
            }

            if (completed.Result == null)
            {
                logger.Log("Process " + Id + " send property command succeeded");

                command.OnCompleted(null);
            }
            else
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Process " + Id + " got command send failure " + completed.Result);
                command.OnCompleted(completed.Result);
            }
        }

        public void ToQueued()
        {
            lock (this)
            {
                Debug.Assert(state == State.Initializing);
                state = State.Queued;
            }
            watcher.OnQueued();
        }

        private void ToMatched(IComputer computer, long timestamp)
        {
            lock (this)
            {
                Debug.Assert(state == State.Queued);
                state = State.Matched;
            }
            watcher.OnMatched(computer, timestamp);
        }

        private void ToCreated(long timestamp)
        {
            lock (this)
            {
                Debug.Assert(state == State.Matched);
                state = State.Created;
            }
            watcher.OnCreated(timestamp);
        }

        private void ToStarted(long timestamp)
        {
            lock (this)
            {
                Debug.Assert(state == State.Created);
                state = State.Started;
            }
            watcher.OnStarted(timestamp);
        }

        private void ToExited(ProcessExitState exitState, long timestamp, int exitCode, string errorText)
        {
            lock (this)
            {
                if (state == State.Exited)
                {
                    // duplicate exit; ignore it
                    return;
                }
                // this can be reached from any preceding state
                state = State.Exited;
            }
            watcher.OnExited(exitState, timestamp, exitCode, errorText);
        }
    }
}
