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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;

using Microsoft.Research.Dryad;
using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.LocalScheduler
{
    /// <summary>
    /// this represents a long-running daemon on which processes can be scheduled. It is
    /// associated with a rack.
    /// </summary>
    internal class Computer : ClusterInterface.IComputer
    {
        /// <summary>
        /// the unique string identifying this computer
        /// </summary>
        private string name;

        /// <summary>
        /// the address of this computer's process server, used to schedule and monitor processes
        /// </summary>
        private string processServer;

        /// <summary>
        /// the address of this computer's file server, used to by remote processes to fetch files
        /// </summary>
        private string fileServer;

        /// <summary>
        /// the local directory that this computer's process server is running in. If multiple
        /// process servers are running on the same host, they are assumed to be in adjacent local
        /// directories that can be accessed from ..\localDirectory
        /// </summary>
        private string localDirectory;

        /// <summary>
        /// the physical host this daemon is running on. A host may have several daemons running on
        /// it, and locality information takes into account that they are all in the same place
        /// </summary>
        private string computerName;

        /// <summary>
        /// the name of the rack the computer is situated in, if we know it
        /// </summary>
        private string rackName;

        /// <summary>
        /// a structure private to this computer, used to match processes that
        /// have a particular affinity to this computer
        /// </summary>
        private ProcessQueue localQueue;

        /// <summary>
        /// a structure shared by all computers in this rack, used to match processes that
        /// have a particular affinity to this rack
        /// </summary>
        private ProcessQueue rackQueue;

        /// <summary>
        /// a structure shared by all computers, used to match processes that
        /// have no affinity, or can't be matched to better-located computers
        /// </summary>
        private ProcessQueue clusterQueue;

        /// <summary>
        /// this blocks until it is time to shut down this computer and stop pairing it with any
        /// more processes, at which point finishWaiter is unblocked causing the main <c>CommandLoop</c> to
        /// exit.
        /// </summary>
        private TaskCompletionSource<Process> finishWaiter;

        /// <summary>
        /// this blocks until the command loop exits
        /// </summary>
        private TaskCompletionSource<bool> exited;

        /// <summary>
        /// numeric id of the next process to start on the computer
        /// </summary>
        private int nextTask;

        /// <summary>
        /// connection to the external logging subsystem
        /// </summary>
        private ClusterInterface.ILogger logger;

        /// <summary>
        /// construct a new Computer object
        /// </summary>
        /// <param name="n">the unique name of the daemon</param>
        /// <param name="host">the computer the daemon is running on</param>
        /// <param name="rn">the rack the daemon is running on</param>
        /// <param name="rack">the scheduling queue associated with the computer's rack</param>
        /// <param name="cluster">the global scheduling queue associated with the cluster</param>
        /// <param name="pServer">the address of the daemon's http server for process scheduling</param>
        /// <param name="fServer">the address of the daemon's http server for file proxying</param>
        /// <param name="directory">the daemon's local directory</param>
        /// <param name="log">connection to the logging subsystem</param>
        public Computer(string n, string host, string rn, ProcessQueue rack, ProcessQueue cluster,
                        string pServer, string fServer, string directory, ClusterInterface.ILogger log)
        {
            logger = log;
            name = n;
            localDirectory = directory;
            processServer = pServer;
            fileServer = fServer;
            computerName = host;
            rackName = rn;
            localQueue = new ProcessQueue();
            rackQueue = rack;
            clusterQueue = cluster;

            logger.Log("Created computer " + name + " on host " + computerName + ":" + rackName + ":" + localDirectory + ":" + fileServer);

            // make the Task that CommandLoop blocks on; when finishWaiter is started it returns null
            // causing CommandLoop to exit.
            finishWaiter = new TaskCompletionSource<Process>();

            // this is started when the Command Loop exits
            exited = new TaskCompletionSource<bool>();

            nextTask = 1;
        }

        /// <summary>
        /// implements IComputer.Name; get the unique name of the computer
        /// </summary>
        public string Name { get { return name; } }

        /// <summary>
        /// implements IComputer.ProcessServer; get the root Uri of the computer's
        /// remote process server
        /// </summary>
        public string ProcessServer { get { return processServer; } }

        /// <summary>
        /// implements IComputer.FileServer; get the root Uri of the computer's
        /// remote file server
        /// </summary>
        public string FileServer { get { return fileServer; } }

        /// <summary>
        /// implements IComputer.Directory; get the local directory of the computer
        /// </summary>
        public string Directory { get { return localDirectory; } }

        /// <summary>
        /// implements IComputer.Host; get a name that is the same for all
        /// Computers running on the same host
        /// </summary>
        public string Host { get { return computerName; } }

        /// <summary>
        /// implements IComputer.RackName; get the name of the rack where the
        /// Computer is located
        /// </summary>
        public string RackName { get { return rackName; } }

        /// <summary>
        /// returns the local queue so processes can be scheduled on the computer
        /// </summary>
        public ProcessQueue LocalQueue { get { return localQueue; } }

        /// <summary>
        /// discard all the processes on our local queue and unblock the finishWaiter
        /// causing the CommandLoop to exit
        /// </summary>
        public void ShutDown()
        {
            logger.Log("Computer " + name + " stopping local queue");
            // stop the local queue accepting any more processes
            localQueue.ShutDown();

            logger.Log("Computer " + name + " starting finishWaiter");
            finishWaiter.SetResult(null);
        }

        /// <summary>
        /// a task that can be awaited and will asynchronously unblock when the finishWaiter result is set
        /// </summary>
        private Task<Process> AsyncFinishWaiter { get { return finishWaiter.Task.ContinueWith((t) => t.Result); } }

        /// <summary>
        /// (asynchronously) block until there is a process available on the local queue, the rack queue
        /// or the cluster queue, then return that process. If ShutDown is called, this returns null
        /// immediately
        /// </summary>
        private async Task<Process> GetProcess()
        {
            // make a new waiter object to block on all the available queues until a Process is available
            var waiter = new ProcessWaiter(this);

            // get the actual blocker Task out of the waiter
            var blocker = waiter.Initialize();

            logger.Log("Computer " + name + " trying to find process on local queue");

            // try to match with an available Process in the local queue. If AddWaiter returns false, there
            // wasn't one, but by passing in waiter, we ensure that blocker will be unblocked if one
            // turns up. If Peek returns true there was already a waiting Process to be paired with,
            // blocker has been unblocked, and the await below will fall through immediately and return
            // the process; in this case don't bother to add the waiter to the rack and cluster queues.
            if (!localQueue.AddWaiter(waiter))
            {
                logger.Log("Computer " + name + " trying to find process on rack queue");

                // there was no local process, so try to match with an available Process in the rack queue.
                // If Peek returns false, there wasn't one, but by passing in waiter, we ensure that blocker
                // will be unblocked if one turns up. If Peek returns true then blocker is already unblocked
                // (because there was a Process in the rack queue, or by the localQueue we just put it in above)
                // and matched with a waiting process, and the await below will fall through immediately.
                if (!rackQueue.AddWaiter(waiter))
                {
                    logger.Log("Computer " + name + " trying to find process on cluster queue");

                    // there was no local or rack process, so try to match with an available Process in the
                    // cluster queue.
                    clusterQueue.AddWaiter(waiter);
                }
            }

            logger.Log("Computer " + name + " waiting for matched process");

            // we want to wait either for waiter to be matched with a Process in one of the three queues, or
            // for ShutDown to be called, so make an array of tasks and wait for the first one to be unblocked.
            var unblocked = await Task.WhenAny(blocker, AsyncFinishWaiter);

            if (unblocked.Result != null)
            {
                logger.Log("Computer " + name + " matched process " + blocker.Result.Id);
            }
            else
            {
                logger.Log("Computer " + name + " unblocked by shutdown");
            }

            return unblocked.Result;
        }

        private async Task<string> PostRequest(string requestString, byte[] payload)
        {
            string uri = processServer + requestString;
            IHttpRequest request = ClusterInterface.HttpClient.Create(uri);
            request.Timeout = 30 * 1000; // this should come back quickly. If it doesn't, something is wrong
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

        private Task<string> ShutdownRemote()
        {
            logger.Log("Computer " + name + " sending remote shutdown command");
            return PostRequest("shutdown", new byte[0]);
        }

        /// <summary>
        /// This is the main loop for the computer; it repeatedly (asynchronously) blocks until a Process is
        /// available to be Scheduled, then runs that process to completion, then looks for another one to
        /// schedule, until ShutDown is called
        /// </summary>
        private async void CommandLoop()
        {
            Process process;

            do
            {
                logger.Log("Computer " + name + " waiting for assigned process");

                // GetProcess() blocks until either there is an available Process, in which case that Process
                // is returned, or ShutDown() is called, in which case null is returned.
                process = await GetProcess();

                if (process != null)
                {
                    logger.Log("Computer " + name + " got assigned process");

                    Computer assignedComputer;

                    lock (process)
                    {
                        assignedComputer = process.Location;
                    }

                    if (assignedComputer != this)
                    {
                        // the process was canceled while it was in the queue, so there's nothing for us to do
                        logger.Log("Computer " + name + ": process " + process.Id + " was already canceled");

                        await process.OnScheduled(null, -1, null, "Process canceled while in scheduling queue");
                    }
                    else
                    {
                        logger.Log("Computer " + name + " reporting match with process " + process.Id);

                        await process.OnScheduled(this, nextTask, AsyncFinishWaiter, null);

                        logger.Log("Computer " + name + " waiting for process " + process.Id + " to complete");

                        ++nextTask;

                        logger.Log("Computer " + name + " finished running process " + process.Id);
                    }
                }

                // process is null when ShutDown is called
            } while (process != null);

            logger.Log("Computer " + name + " shutting down remote process");

            Task<string> timeout = Task.Delay(10000).ContinueWith((t) => null as string);
            Task<string> shutDown = await Task.WhenAny(timeout, ShutdownRemote());
            if (shutDown == timeout)
            {
                logger.Log("Timed out waiting for shutdown to complete");
            }

            logger.Log("Computer " + name + " setting exited");

            exited.SetResult(true);
        }

        public void Start()
        {
            Task.Run(() => CommandLoop());
        }

        public async Task WaitForExit()
        {
            logger.Log("Computer " + name + " waiting for exited");
            await exited.Task.ContinueWith((t) => { });
            logger.Log("Computer " + name + " waiting for exited completed");
        }
    }

    internal class Rack
    {
        public Rack()
        {
            computers = new HashSet<string>();
            queue = new ProcessQueue();
        }

        public HashSet<string> computers;
        public ProcessQueue queue;
    }
}
