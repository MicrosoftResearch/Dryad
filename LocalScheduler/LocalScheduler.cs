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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Dryad;

namespace Microsoft.Research.Dryad.LocalScheduler
{
    public class Registration
    {
        private static LocalScheduler Create(ClusterInterface.ILogger logger)
        {
            return new LocalScheduler(logger);
        }

        static public void Ensure()
        {
            ClusterInterface.Factory.Register("local", Create);
        }
    }

    public class LocalScheduler : ClusterInterface.IScheduler
    {
        private Dictionary<string, Computer> computers;
        private Dictionary<string, List<Computer>> localities;
        private Dictionary<string, Rack> racks;
        private ProcessQueue clusterQueue;

        private Task flusher;
        private const int rackDelay = 1000;
        private const int clusterDelay = 2000;

        private ClusterInterface.ILogger logger;
        private PeloponneseInterface clusterInterface;

        private Computer dummyCancelComputer;

        public LocalScheduler(ClusterInterface.ILogger l)
        {
            logger = l;

            computers = new Dictionary<string, Computer>();
            localities = new Dictionary<string, List<Computer>>();
            racks = new Dictionary<string, Rack>();
            clusterQueue = new ProcessQueue();

            flusher = new Task(() => { });

            clusterInterface = new PeloponneseInterface();

            dummyCancelComputer = new Computer("dummy for canceling", "nowhere", "no rack", null, null,
                                               "no server", "no server", "no directory", logger);

            l.Log("LocalScheduler created");
        }

        public bool Start()
        {
            if (!clusterInterface.Initialize(this, logger))
            {
                return false;
            }

            clusterInterface.Start();

            clusterInterface.WaitForReasonableNumberOfComputers();

            return true;
        }

        public void Stop()
        {
            clusterInterface.Stop();
        }

        public List<ClusterInterface.IComputer> GetComputers()
        {
            var l = new List<ClusterInterface.IComputer>();

            lock (computers)
            {
                foreach (var c in computers.Values)
                {
                    l.Add(c);
                }
            }

            return l;
        }

        public ClusterInterface.IComputer GetComputerAtHost(string host)
        {
            lock (localities)
            {
                List<Computer> computers;
                if (localities.TryGetValue(host, out computers))
                {
                    return computers.First();
                }
            }

            return null;
        }

        public ClusterInterface.ISchedulerProcess NewProcess()
        {
            return new Process();
        }

        private async void ScheduleProcessInternal(Process process, List<ClusterInterface.Affinity> affinities,
                                                   ClusterInterface.RunProcess callback)
        {
            logger.Log("Scheduling process " + process.Id);

            process.SetCallback(callback);

            Task rackBlocker;
            Task clusterBlocker;

            lock (this)
            {
                rackBlocker = Task.WhenAny(flusher, Task.Delay(rackDelay));
                clusterBlocker = Task.WhenAny(flusher, Task.Delay(clusterDelay));
            }

            bool isHardConstraint = affinities.Aggregate(false, (a, b) => a || b.isHardContraint);
            if (isHardConstraint)
            {
                // the constraint generator should have intersected the hard constraint into a single one
                Debug.Assert(affinities.Count() == 1);
                logger.Log("Process " + process.Id + " has a hard constraint");
            }

            var allAffinities = affinities.SelectMany(a => a.affinities).Distinct();
            var computerAffinities = allAffinities.Where(a => a.level == ClusterInterface.AffinityResourceLevel.Host);

            bool addedAny = false;

            // get a snapshot of available computers
            Dictionary<string, List<Computer>> localitySnapshot = new Dictionary<string,List<Computer>>();
            lock (localities)
            {
                foreach (var c in localities)
                {
                    localitySnapshot.Add(c.Key, c.Value);
                }
            }

            if (localitySnapshot.Count == 0)
            {
                await process.OnScheduled(null, -1, null, "No cluster computers available");
                return;
            }

            var racksUsed = new List<string>();
            foreach (var a in computerAffinities)
            {
                List<Computer> cl;
                if (localitySnapshot.TryGetValue(a.locality, out cl))
                {
                    addedAny = true;
                    logger.Log("Adding Process " + process.Id + " to queues for computers with locality " + a.locality);
                    foreach (var c in cl)
                    {
                        logger.Log("Adding Process " + process.Id + " to queue for computer " + c.Name);
                        if (c.LocalQueue.AddProcess(process))
                        {
                            // this returns true if p has been matched to a computer, in which case we
                            // can stop adding it to queues
                            logger.Log("Process " + process.Id + " claimed by computer " + c.Name);
                            return;
                        }
                    }
                    // remember the rack this computer was in, to include it for soft affinities below
                    racksUsed.Add(cl.First().RackName);
                }
            }

            if (addedAny)
            {
                // hacky delay scheduling; wait until the upper level has finished adding processes in
                // the current stage, or some time has passed, before relaxing affinities if the process
                // had affinities for particular computers
                logger.Log("Process " + process.Id + " delay scheduling for rack");
                await rackBlocker;
            }

            // reset flag before adding to racks
            addedAny = false;

            // get a snapshot of available racks
            Dictionary<string, Rack> rackSnapshot = new Dictionary<string, Rack>();
            lock (racks)
            {
                foreach (var r in racks)
                {
                    rackSnapshot.Add(r.Key, r.Value);
                }
            }

            var rackAffinities = allAffinities.Where(a => a.level == ClusterInterface.AffinityResourceLevel.Rack).Select(a => a.locality).Distinct();
            if (!isHardConstraint)
            {
                rackAffinities = rackAffinities.Concat(racksUsed).Distinct();
            }

            foreach (var a in rackAffinities)
            {
                Rack r;
                if (rackSnapshot.TryGetValue(a, out r))
                {
                    addedAny = true;
                    logger.Log("Adding Process " + process.Id + " to queue for rack " + a);
                    if (r.queue.AddProcess(process))
                    {
                        // this returns true if p has been matched to a computer, in which case we
                        // can stop adding it to queues
                        logger.Log("Process " + process.Id + " claimed by rack " + a);
                        return;
                    }
                }
            }

            if (isHardConstraint)
            {
                // let the process know it won't get added to any more queues. This will signal the
                // upper layer if it didn't get added to any queues
                process.FinishedScheduling();
                return;
            }

            if (addedAny)
            {
                // hacky delay scheduling; wait until the upper level has finished adding processes in
                // the current stage, or some time has passed, before relaxing affinities if the process
                // had affinities for particular racks
                logger.Log("Process " + process.Id + " delay scheduling for cluster");
                await clusterBlocker;
            }

            logger.Log("Adding Process " + process.Id + " to queue for cluster");
            clusterQueue.AddProcess(process);

            // let the process know it won't get added to any more queues
            process.FinishedScheduling();
        }

        public void ScheduleProcess(ClusterInterface.ISchedulerProcess ip,
                                    List<ClusterInterface.Affinity> affinities,
                                    ClusterInterface.RunProcess onScheduled)
        {
            Process process = ip as Process;

            Task.Run(() => ScheduleProcessInternal(process, affinities, onScheduled));
        }

        public void CancelProcess(ClusterInterface.ISchedulerProcess ip)
        {
            Process process = ip as Process;

            lock (process)
            {
                if (process.Unclaimed)
                {
                    // it is still sitting in scheduling queues; unblock it with a dummy process
                    process.Claim(dummyCancelComputer);
                }
            }
        }

        internal void AddComputer(string computerName, string hostName, string rackName,
                                  string processServer, string fileServer, string localDirectory)
        {
            Rack rack;
            lock (racks)
            {
                if (!racks.TryGetValue(rackName, out rack))
                {
                    rack = new Rack();
                    racks.Add(rackName, rack);
                }
            }

            Computer c = new Computer(computerName, hostName, rackName, rack.queue, clusterQueue,
                                      processServer, fileServer, localDirectory, logger);
            lock (computers)
            {
                computers.Add(computerName, c);
            }

            lock (localities)
            {
                List<Computer> cl;
                if (!localities.TryGetValue(hostName, out cl))
                {
                    cl = new List<Computer>();
                    localities.Add(hostName, cl);
                }

                cl.Add(c);
            }

            lock (rack)
            {
                Debug.Assert(!rack.computers.Contains(computerName));
                rack.computers.Add(computerName);
            }

            c.Start();
        }

        internal async Task RemoveComputer(string computerName)
        {
            Computer computer;

            logger.Log("Removing computer " + computerName);

            lock (computers)
            {
                computer = computers[computerName];
                computers.Remove(computerName);
            }

            computer.ShutDown();

            lock (localities)
            {
                var locality = localities[computer.Host];
                List<Computer> newList = new List<Computer>();
                foreach (var c in locality)
                {
                    if (c.Name != computerName)
                    {
                        newList.Add(c);
                    }
                }

                if (newList.Count == 0)
                {
                    localities.Remove(computer.Host);
                }
                else
                {
                    localities[computer.Host] = newList;
                }
            }

            Rack emptyRack = null;
            lock (racks)
            {
                var rack = racks[computer.RackName];
                lock (rack)
                {
                    bool removed = rack.computers.Remove(computer.Name);
                    Debug.Assert(removed);
                    if (rack.computers.Count == 0)
                    {
                        racks.Remove(computer.RackName);
                        emptyRack = rack;
                    }
                }
            }

            if (emptyRack != null)
            {
                emptyRack.queue.ShutDown();
            }

            await computer.WaitForExit();
        }
    }
}
