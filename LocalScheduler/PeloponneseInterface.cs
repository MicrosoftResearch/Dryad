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
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Dryad;
using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.LocalScheduler
{
    class Constants
    {
        /// <summary>
        /// the environment variable used to communicate the job's Guid to a
        /// spawned process. This is set by the ProcessGroupManager for each
        /// process it creates
        /// </summary>
        public const string EnvJobGuid = "PELOPONNESE_JOB_GUID";

        /// <summary>
        /// the environment variable used to communicate the server's Uri to a
        /// spawned process. This is set by the ProcessGroupManager for each
        /// process it creates
        /// </summary>
        public const string EnvManagerServerUri = "PELOPONNESE_SERVER_URI";

        /// <summary>
        /// the environment variable used to communicate the name of the process group
        /// that a spawned process belongs to. This is set by the ProcessGroupManager for each
        /// process it creates. The spawned process uses this name when it registers itself
        /// with the web server
        /// </summary>
        public const string EnvProcessGroup = "PELOPONNESE_PROCESS_GROUP";

        /// <summary>
        /// the environment variable used to communicate the identifier for a spawned
        /// process. This is set by the ProcessGroupManager for each
        /// process it creates. Identifiers must be unique (within a given group) over
        /// the lifetime of the server. The spawned process uses this identifier when
        /// it registers itself with the web server
        /// </summary>
        public const string EnvProcessIdentifier = "PELOPONNESE_PROCESS_IDENTIFIER";
    }

    internal class PeloponneseInterface
    {
        private LocalScheduler parent;
        private ClusterInterface.ILogger logger;
        private string jobGuid;
        private string serverAddress;

        private UInt64 epoch;
        private UInt64 version;
        private int targetNumberOfWorkers;
        private Dictionary<string, string> knownWorkers;

        private TaskCompletionSource<bool> reasonableReached;
        private TaskCompletionSource<XContainer> shutdownTask;
        private List<Task> waitingForComputer;
        private TaskCompletionSource<bool> exited;

        public bool Initialize(LocalScheduler p, ClusterInterface.ILogger l)
        {
            parent = p;
            logger = l;
            epoch = 0;
            version = 0;
            targetNumberOfWorkers = -1;
            knownWorkers = new Dictionary<string, string>();
            reasonableReached = new TaskCompletionSource<bool>();
            shutdownTask = new TaskCompletionSource<XContainer>();
            waitingForComputer = new List<Task>();
            exited = new TaskCompletionSource<bool>();

            jobGuid = Environment.GetEnvironmentVariable(Constants.EnvJobGuid);
            if (jobGuid == null)
            {
                logger.Log("Can't find environment variable " + Constants.EnvJobGuid + ": exiting");
                return false;
            }

            serverAddress = Environment.GetEnvironmentVariable(Constants.EnvManagerServerUri);
            if (serverAddress == null)
            {
                logger.Log("Can't find environment variable " + Constants.EnvManagerServerUri + ": exiting");
                return false;
            }

            var groupName = Environment.GetEnvironmentVariable(Constants.EnvProcessGroup);
            if (groupName == null)
            {
                logger.Log("Can't find environment variable " + Constants.EnvProcessGroup + ": exiting");
                return false;
            }

            var procIdentifier = Environment.GetEnvironmentVariable(Constants.EnvProcessIdentifier);
            if (procIdentifier == null)
            {
                logger.Log("Can't find environment variable " + Constants.EnvProcessIdentifier + ": exiting");
                return false;
            }

            var element = new XElement("ProcessDetails");
            var status = element.ToString();

            string registration = String.Format("{0}register?guid={1}&group={2}&identifier={3}", serverAddress, jobGuid, groupName, procIdentifier);
            IHttpRequest request = ClusterInterface.HttpClient.Create(registration);
            request.Timeout = 30 * 1000; // if this doesn't come back quickly, we'll get an exception and quit
            request.Method = "POST";

            try
            {
                using (Stream upload = request.GetRequestStream())
                {
                    using (StreamWriter sw = new StreamWriter(upload))
                    {
                        sw.Write(status);
                    }
                }

                using (IHttpResponse response = request.GetResponse())
                {
                    logger.Log("Server registration succeeded");
                    return true;
                }
            }
            catch (NotHttpException e)
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Server registration failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription);
                return false;
            }
            catch (Exception e)
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Server registration failed message " + e.Message);
                return false;
            }
        }

        private async Task<XContainer> GetStatus()
        {
            int reasonableNumber = Math.Max(1, (targetNumberOfWorkers * 3) / 4);
            int nearlyAll = targetNumberOfWorkers - 3;

            StringBuilder sb = new StringBuilder(serverAddress);
            sb.Append("status");

            if (knownWorkers.Count >= reasonableNumber)
            {
                // we will try to set this repeatedly, so make sure it doesn't throw an exception
                // the second time
                reasonableReached.TrySetResult(true);
            }

            if (targetNumberOfWorkers < 0)
            {
                // we haven't seen any status yet. Don't add any predicates to the request,
                // so it will return immediately
            }
            else if (knownWorkers.Count < nearlyAll)
            {
                // wait until the epoch changes, or we get close to all, or a few seconds have passed
                sb.AppendFormat("?epochGreater={0}", epoch);
                sb.AppendFormat("&thresholdGreater=Worker:{0}", nearlyAll - 1);
                sb.Append("&timeout=2000");
            }
            else
            {
                // wait until the epoch changes, or any machine state changes, or a few seconds have passed
                sb.AppendFormat("?epochGreater={0}", epoch);
                sb.AppendFormat("&versionGreater={0}", version);
                sb.Append("&timeout=30000");
            }

            IHttpRequest request = ClusterInterface.HttpClient.Create(sb.ToString());
            request.Timeout = 60 * 1000; // if the query doesn't eventually return, something is wrong

            try
            {
                using (IHttpResponse status = await request.GetResponseAsync())
                {
                    using (Stream response = status.GetResponseStream())
                    {
                        using (var reader = System.Xml.XmlReader.Create(response))
                        {
                            return XDocument.Load(reader);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.Log("Failed getting status: " + e.ToString());
                return null;
            }
        }

        private bool UpdateStatus(XContainer status)
        {
            try
            {
                var processes = status.Descendants("RegisteredProcesses").Single();
                var newEpoch = UInt64.Parse(processes.Attribute("epoch").Value);
                var newVersion = UInt64.Parse(processes.Attribute("version").Value);

                bool changes = false;
                var workers = processes.Descendants("ProcessGroup").Where(pg => pg.Attribute("name").Value == "Worker").Single();
                var newTarget = int.Parse(workers.Attribute("targetNumberOfProcesses").Value);

                Debug.Assert(newEpoch >= epoch);
                if (newEpoch > epoch)
                {
                    epoch = newEpoch;
                    targetNumberOfWorkers = newTarget;
                    version = 0;
                    changes = true;
                }
                else
                {
                    Debug.Assert(newTarget == targetNumberOfWorkers);
                }

                Debug.Assert(newVersion >= version);
                if (newVersion > version)
                {
                    version = newVersion;
                    changes = true;
                }

                var newProcesses = new Dictionary<string, string>();
                foreach (var processElement in workers.Descendants("Process"))
                {
                    var id = processElement.Attribute("identifier").Value;
                    var payload = processElement.ToString();
                    newProcesses.Add(id, payload);

                    string oldPayload;
                    if (knownWorkers.TryGetValue(id, out oldPayload))
                    {
                        Debug.Assert(payload == oldPayload);
                        knownWorkers.Remove(id);
                    }
                    else
                    {
                        // there shouldn't be any new processes if the epoch and version are unchanged
                        Debug.Assert(changes);

                        var details = processElement.Descendants("ProcessDetails").Single();
                        var hostName = details.Attribute("hostname").Value;
                        var rackName = details.Attribute("rackname").Value;
                        var processServer = details.Descendants("ProcessUri").Single().Value;
                        var fileServer = details.Descendants("FileUri").Single().Value;
                        var directory = details.Descendants("LocalDirectory").Single().Value;
                        parent.AddComputer(id, hostName, rackName, processServer, fileServer, directory);
                    }
                }

                if (!changes)
                {
                    // there shouldn't be any missing processes if the epoch and version are unchanged
                    Debug.Assert(knownWorkers.Count == 0);
                }
                foreach (var formerProcess in knownWorkers)
                {
                    waitingForComputer.Add(parent.RemoveComputer(formerProcess.Key));
                }

                knownWorkers = newProcesses;

                return true;
            }
            catch (Exception e)
            {
                logger.Log("Read bad status from server: " + e.ToString());

                return false;
            }
        }

        private async void CommandLoop()
        {
            XContainer newStatus;

            do
            {
                var unblockedTask = await Task.WhenAny(shutdownTask.Task, GetStatus());
                newStatus = unblockedTask.Result;

                if (newStatus != null)
                {
                    if (!UpdateStatus(newStatus))
                    {
                        // exit the loop if the status update failed
                        newStatus = null;
                    }
                }

                // trim the list of waiting computers that have now exited
                waitingForComputer = waitingForComputer.Where(t => !t.IsCompleted).ToList();
            } while (newStatus != null);

            // we try to set this in multiple places, so be tolerant of the fact it may
            // have already been set
            reasonableReached.TrySetResult(true);

            string shutdown = String.Format("{0}startshutdown", serverAddress);
            IHttpRequest request = ClusterInterface.HttpClient.Create(shutdown);
            request.Timeout = 30 * 1000; // if this doesn't return quickly we will get an exception and fail to do the clean shutdown
            request.Method = "POST";

            try
            {
                using (Stream upload = request.GetRequestStream())
                {
                    // empty stream
                }

                using (IHttpResponse response = request.GetResponse())
                {
                    logger.Log("Server shutdown initiation succeeded");
                }
            }
            catch (NotHttpException e)
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Server shutdown initiation failed message " + e.Message + " status " + e.Response.StatusCode + ": " + e.Response.StatusDescription);
            }

            // we're shutting down: get rid of all the computers
            foreach (var c in knownWorkers)
            {
                waitingForComputer.Add(parent.RemoveComputer(c.Key));
            }

            await Task.WhenAll(waitingForComputer);

            exited.SetResult(true);
        }

        public void Start()
        {
            Task.Run(() => CommandLoop());
        }

        public void WaitForReasonableNumberOfComputers()
        {
            logger.Log("Waiting for a reasonable number of processes to start");
            reasonableReached.Task.Wait();
        }

        public void Stop()
        {
            shutdownTask.TrySetResult(null);

            exited.Task.Wait();
        }
    }
}
