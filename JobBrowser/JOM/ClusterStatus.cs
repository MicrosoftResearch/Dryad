
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
using Microsoft.Research.Peloponnese.Storage;
using Microsoft.Research.Tools;

namespace Microsoft.Research.JobObjectModel
{
    /// <summary>
    /// Dynamic information of all the jobs and machines in a cluster. 
    /// </summary>
    public abstract class ClusterStatus
    {
        /// <summary>
        /// The static configuration of the cluster
        /// </summary>
        public ClusterConfiguration Config { get; protected set; }
        /// <summary>
        /// Cache here the collection of cluster jobs on the cluster; index is cluster job guid.
        /// </summary>
        protected Dictionary<string, ClusterJobInformation> clusterJobs;
        /// <summary>
        /// For each machine the pod it belongs to.
        /// </summary>
        protected Dictionary<string, string> pods;

        /// <summary>
        /// Create a cluster status.
        /// </summary>
        /// <param name="config">Cluster configuration.</param>
        protected ClusterStatus(ClusterConfiguration config)
        {
            this.Config = config;
            this.Initialize();
            if (ClusterStatuses.ContainsKey(config.Name))
                ClusterStatuses[config.Name] = this;
            else
                ClusterStatuses.Add(config.Name, this);
        }

        /// <summary>
        /// Cache each for each cluster its status.
        /// </summary>
        static Dictionary<string, ClusterStatus> ClusterStatuses = new Dictionary<string, ClusterStatus>();

        /// <summary>
        /// See if a status is already cached.
        /// </summary>
        /// <param name="clusterName">Name of cluster.</param>
        /// <returns>The cached status.</returns>
        public static ClusterStatus LookupStatus(string clusterName)
        {
            if (ClusterStatuses.ContainsKey(clusterName))
                return ClusterStatuses[clusterName];
            return null;
        }

        /// <summary>
        /// Initialize the cluster status.
        /// </summary>
        private void Initialize()
        {
            this.pods = null;
        }

        /// <summary>
        /// The cluster map: for each machine the pod it resides in.
        /// </summary>
        /// <returns>An iterator over the cluster map.</returns>
        public IEnumerable<KeyValuePair<string, string>> Pods()
        {
            if (this.pods == null)
            {
                this.pods = this.QueryClusterTopology();
            }
            return this.pods.AsEnumerable();
        }

        /// <summary>
        /// Discover the way machines are organized into racks.
        /// </summary>
        protected abstract Dictionary<string, string> QueryClusterTopology();

        /// <summary>
        /// Generate the list of machines which may be in the cluster.
        /// </summary>
        /// <returns>An iterator returning all machines in the cluster.</returns>
        public IEnumerable<string> GetClusterMachines()
        {
            if (this.pods == null)
                this.pods = this.QueryClusterTopology();
            return this.pods.Select(m => m.Key);
        }

        /// <summary>
        /// The cached of tasks on the cluster.
        /// </summary>
        /// <param name="virtualCluster">Virtual cluster selected; defined only for Scope clusters.</param>
        /// <param name="manager">Communication manager.</param>
        public IEnumerable<ClusterJobInformation> GetClusterJobList(string virtualCluster, CommManager manager)
        {
            this.RecomputeClusterJobList(virtualCluster, manager);
            return this.clusterJobs.Values.ToList();
        }

        /// <summary>
        /// Force the recomputation of the cluster job list.
        /// </summary>
        /// <param name="virtualCluster">Virtual cluster to use (defined only for some cluster types).</param>
        /// <param name="manager">Communication manager.</param>
        // ReSharper disable once UnusedParameter.Global
        protected abstract void RecomputeClusterJobList(string virtualCluster, CommManager manager);

        /// <summary>
        /// Discover the (unique) dryadlinq job corresponding to a cluster job.
        /// </summary>
        /// <param name="clusterJob">Cluster Job.</param>
        /// <returns>The job description.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public abstract DryadLinqJobSummary DiscoverDryadLinqJobFromClusterJob(ClusterJobInformation clusterJob, StatusReporter reporter);

        /// <summary>
        /// Discover the dryadlinq job given a url from the cluster scheduler.
        /// (Does not make sense for some cluster architectures, e.g., HPC.)
        /// </summary>
        /// <param name="url">URL pointing to the job.</param>
        /// <returns>The dryadlinq job summary.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        // ReSharper disable UnusedParameter.Global
        public abstract DryadLinqJobSummary DiscoverDryadLinqJobFromURL(string url, StatusReporter reporter);
        // ReSharper restore UnusedParameter.Global

        /// <summary>
        /// Discover a cluster job given its id.
        /// </summary>
        /// <param name="job">Job to discover.</param>
        /// <returns>The cluster job, or null if not found.</returns>
        /// <param name="manager">Communication manager.</param>
        public virtual ClusterJobInformation DiscoverClusterJob(DryadLinqJobSummary job, CommManager manager)
        {
            if (this.clusterJobs == null)
                this.RecomputeClusterJobList(job.VirtualCluster, manager);
            return this.clusterJobs[job.ClusterJobId];
        }

        /// <summary>
        /// Refresh the job summary status.
        /// </summary>
        /// <param name="summary">Summary to refresh.</param>
        /// <param name="manager">Communication manager.</param>
        public virtual void RefreshStatus(DryadLinqJobSummary summary, CommManager manager)
        {
            // refresh the whole list
            this.RecomputeClusterJobList(summary.VirtualCluster, manager);
            ClusterJobInformation info = this.DiscoverClusterJob(summary, manager);
            if (info == null)
            {
                summary.Status = ClusterJobInformation.ClusterJobStatus.Unknown;
                return;
            }
            summary.Status = info.Status;
        }

        /// <summary>
        /// Cancel the specified job.
        /// </summary>
        /// <param name="job">Job whose execution is cancelled.</param>
        /// <returns>True if the cancellation succeeded.</returns>
        public abstract bool CancelJob(DryadLinqJobSummary job);
    }



    /// <summary>
    /// A fake cluster keeps some information about past jobs on a local filesystem, to allow post-mortem debugging.
    /// </summary>
    public class CacheClusterStatus : ClusterStatus
    {
        /// <summary>
        /// Create a fake cluster status.
        /// </summary>
        /// <param name="config">Configuration to use for this cluster.</param>
        public CacheClusterStatus(ClusterConfiguration config)
            : base(config)
        {
            if (!(config is CacheClusterConfiguration))
                throw new ArgumentException("Expected configuration to be for a cache cluster");
        }

        /// <summary>
        /// Not implemented; return an empty topology.
        /// </summary>
        /// <returns></returns>
        protected override Dictionary<string, string> QueryClusterTopology()
        {
            return new Dictionary<string, string>();
        }

        /// <summary>
        /// Recompute the list of jobs on the cluster and add them to the clusterJobs field.
        /// </summary>
        /// <param name="virtualCluster">Unused.</param>
        /// <param name="manager">Communication manager.</param>
        protected override void RecomputeClusterJobList(string virtualCluster, CommManager manager)
        {
            this.clusterJobs = new Dictionary<string, ClusterJobInformation>();
            if (string.IsNullOrEmpty(CachedClusterResidentObject.CacheDirectory))
                return;

            string joblist = Path.Combine(CachedClusterResidentObject.CacheDirectory, "jobs");
            if (!Directory.Exists(joblist))
                Directory.CreateDirectory(joblist);

            string[] files = Directory.GetFiles(joblist, "*.xml");
            foreach (var file in files)
            {
                manager.Token.ThrowIfCancellationRequested();
                DryadLinqJobSummary job = Utilities.LoadXml<DryadLinqJobSummary>(file);
                string cjid = job.Cluster + "-" + job.ClusterJobId; // there may be two jobs with same id from different clusters
                ClusterJobInformation ci = new ClusterJobInformation(this.Config.Name, job.Cluster, cjid, job.Name, job.User, job.Date, job.EndTime - job.Date, job.Status);
                ci.SetAssociatedSummary(job);
                if (this.clusterJobs.ContainsKey(cjid))
                {
                    manager.Status("Duplicate job id, cannot insert in cache " + job.AsIdentifyingString(), StatusKind.Error);
                    continue;
                }
                this.clusterJobs.Add(cjid, ci);
            }
            manager.Progress(100);
        }

        /// <summary>
        /// Refresh the job summary status.
        /// </summary>
        /// <param name="job">Summary to refresh.</param>
        /// <param name="manager">Communication manager.</param>        
        public override void RefreshStatus(DryadLinqJobSummary job, CommManager manager)
        {
            ClusterConfiguration actual = (this.Config as CacheClusterConfiguration).ActualConfig(job);
            ClusterStatus actualStatus = actual.CreateClusterStatus();
            actualStatus.RefreshStatus(job, manager);
            ClusterJobInformation info = actualStatus.DiscoverClusterJob(job, manager);
            if (info == null)
            {
                job.Status = ClusterJobInformation.ClusterJobStatus.Unknown;
                return;
            }
            job.Status = info.Status;
        }

        /// <summary>
        /// Not needed, all summaries are already known.
        /// </summary>
        /// <param name="clusterJob">Cluster job information.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <returns>Throws an exception.</returns>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromClusterJob(ClusterJobInformation clusterJob, StatusReporter reporter)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// This functionality is not available for cached jobs.
        /// </summary>
        /// <param name="url">Job url.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <returns>Throws an exception.</returns>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromURL(string url, StatusReporter reporter)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Not needed, all summaries are already known.
        /// </summary>
        /// <param name="job">Cluster job.</param>
        /// <returns>Throws an exception.</returns>
        /// <param name="manager">Communication manager.</param>
        public override ClusterJobInformation DiscoverClusterJob(DryadLinqJobSummary job, CommManager manager)
        {
            ClusterConfiguration actual = (this.Config as CacheClusterConfiguration).ActualConfig(job);
            ClusterStatus actualStatus = actual.CreateClusterStatus();
            return actualStatus.DiscoverClusterJob(job, manager);
        }

        /// <summary>
        /// Not needed, these jobs are no longer running.
        /// </summary>
        /// <param name="job">Job to cancel.</param>
        /// <returns>Throws an exception.</returns>
        public override bool CancelJob(DryadLinqJobSummary job)
        {
            throw new InvalidOperationException();
        }
    }

    #region IDisposable Members
        #endregion

    /// <summary>
    /// Status of a cluster comprised only of the local machine.
    /// </summary>
    public class YarnEmulatedClusterStatus : ClusterStatus
    {
        private LocalEmulator config;

        /// <summary>
        /// Create a cluster containing just the local machine.
        /// </summary>
        /// <param name="config">Configuration for the local machine.</param>
        public YarnEmulatedClusterStatus(ClusterConfiguration config)
            : base(config)
        {
            if (!(config is LocalEmulator))
                throw new ArgumentException("Expected a LocalMachineConfiguration, got a " + config.GetType());
            this.config = config as LocalEmulator;
        }

        /// <summary>
        /// Discover the way machines are organized into racks.
        /// </summary>
        protected override Dictionary<string, string> QueryClusterTopology()
        {
            var result = new Dictionary<string, string>();
            result.Add("localhost", "localhost");
            return result;
        }

        /// <summary>
        /// Force the recomputation of the cluster job list.
        /// </summary>
        /// <param name="virtualCluster">Virtual cluster to use (defined only for some cluster types).</param>
        /// <param name="manager">Communication manager.</param>        
        protected override void RecomputeClusterJobList(string virtualCluster, CommManager manager)
        {
            this.clusterJobs = new Dictionary<string, ClusterJobInformation>();
            if (!Directory.Exists(this.config.JobsFolder))
                return;
            string[] subfolders = Directory.GetDirectories(this.config.JobsFolder);

            int done = 0;
            foreach (var job in subfolders)
            {
                manager.Token.ThrowIfCancellationRequested();
                string jobId = Path.GetFileName(job);
                ClusterJobInformation info = this.GetJobInfo(job, jobId);
                if (info != null)
                {
                    // ReSharper disable once AssignNullToNotNullAttribute
                    this.clusterJobs.Add(jobId, info);
                }
                manager.Progress(done++ *100/subfolders.Length);
            }
            manager.Progress(100);
        }

        /// <summary>
        /// Discover the (unique) dryadlinq job corresponding to a cluster job.
        /// </summary>
        /// <param name="clusterJob">Cluster Job.</param>
        /// <returns>The job description.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromClusterJob(ClusterJobInformation clusterJob, StatusReporter reporter)
        {
            DryadLinqJobSummary result = new DryadLinqJobSummary(
                    clusterJob.Cluster,
                    this.config.TypeOfCluster,
                    "", // virtual cluster
                    "", // machine
                    clusterJob.ClusterJobID, // jobId
                    clusterJob.ClusterJobID, // clusterJobId
                    new DryadProcessIdentifier("jm"), // jmProcessGuid
                    clusterJob.Name,
                    clusterJob.User,
                    clusterJob.Date,
                    clusterJob.Date + clusterJob.EstimatedRunningTime,
                    clusterJob.Status);
            return result;
        }

        /// <summary>
        /// Discover the dryadlinq job given a url from the cluster scheduler.
        /// (Does not make sense for some cluster architectures, e.g., HPC.)
        /// </summary>
        /// <param name="url">URL pointing to the job.</param>
        /// <returns>The dryadlinq job summary.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromURL(string url, StatusReporter reporter)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Extract the job information from a folder with logs on the local machine.
        /// </summary>
        /// <param name="jobRootFolder">Folder with logs for the specified job.</param>
        /// <returns>The job information, or null if not found.</returns>
        /// <param name="jobId">Job id.</param>
        private ClusterJobInformation GetJobInfo(string jobRootFolder, string jobId)
        {
            string jmFolder = Path.Combine(jobRootFolder, "jm");
            if (!Directory.Exists(jmFolder)) return null;

            var date = File.GetCreationTime(jmFolder);
            ClusterJobInformation info = new ClusterJobInformation(this.config.Name, "", jobId, jobId, Environment.UserName, date, TimeSpan.Zero, ClusterJobInformation.ClusterJobStatus.Unknown);
            return info;
        }

        /// <summary>
        /// Cancel the specified job.
        /// </summary>
        /// <param name="job">Job whose execution is cancelled.</param>
        /// <returns>True if the cancellation succeeded.</returns>
        public override bool CancelJob(DryadLinqJobSummary job)
        {
            throw new InvalidOperationException();
        }
    }

    /// <summary>
    /// Status of an Azure DFS cluster.
    /// </summary>
    public class AzureDfsClusterStatus : ClusterStatus
    {
        private AzureDfsClusterConfiguration config;

        /// <summary>
        /// Create a cluster containing just the local machine.
        /// </summary>
        /// <param name="config">Configuration for the local machine.</param>
        public AzureDfsClusterStatus(ClusterConfiguration config)
            : base(config)
        {
            if (!(config is AzureDfsClusterConfiguration))
                throw new ArgumentException("Expected a AzureYarnClusterConfiguration, got a " + config.GetType());
            this.config = config as AzureDfsClusterConfiguration;
        }

        /// <summary>
        /// Discover the way machines are organized into racks.
        /// </summary>
        protected override Dictionary<string, string> QueryClusterTopology()
        {
            var result = new Dictionary<string, string>();
            result.Add("localhost", "localhost");
            return result;
        }

        /// <summary>
        /// Force the recomputation of the cluster job list.
        /// </summary>
        /// <param name="virtualCluster">Virtual cluster to use (defined only for some cluster types).</param>
        /// <param name="manager">Communication manager.</param>        
        protected override void RecomputeClusterJobList(string virtualCluster, CommManager manager)
        {
            this.clusterJobs = new Dictionary<string, ClusterJobInformation>();
            var jobs = this.config.AzureClient.EnumerateDirectory("").ToList();

            int done = 0;
            foreach (var job in jobs)
            {
                manager.Token.ThrowIfCancellationRequested();
                ClusterJobInformation info = this.GetJobInfo(job);
                if (info != null)
                {
                    // ReSharper disable once AssignNullToNotNullAttribute
                    this.clusterJobs.Add(job, info);
                }
                manager.Progress(100*done++/jobs.Count);
            }
            manager.Progress(100);
        }

        /// <summary>
        /// Discover the (unique) dryadlinq job corresponding to a cluster job.
        /// </summary>
        /// <param name="clusterJob">Cluster Job.</param>
        /// <returns>The job description.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromClusterJob(ClusterJobInformation clusterJob, StatusReporter reporter)
        {
            DryadLinqJobSummary result = new DryadLinqJobSummary(
                    clusterJob.Cluster,
                    this.config.TypeOfCluster,
                    "", // virtual cluster
                    "", // machine
                    clusterJob.ClusterJobID, // jobId
                    clusterJob.ClusterJobID, // clusterJobId
                    new DryadProcessIdentifier("jm"), // jmProcessGuid
                    clusterJob.Name,
                    clusterJob.User,
                    clusterJob.Date,
                    clusterJob.Date + clusterJob.EstimatedRunningTime,
                    clusterJob.Status);
            return result;
        }

        /// <summary>
        /// Discover the dryadlinq job given a url from the cluster scheduler.
        /// (Does not make sense for some cluster architectures, e.g., HPC.)
        /// </summary>
        /// <param name="url">URL pointing to the job.</param>
        /// <returns>The dryadlinq job summary.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public override DryadLinqJobSummary DiscoverDryadLinqJobFromURL(string url, StatusReporter reporter)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Extract the job information from a folder with logs on the local machine.
        /// </summary>
        /// <param name="jobRootFolder">Folder with logs for the specified job.</param>
        /// <returns>The job information, or null if not found.</returns>
        private ClusterJobInformation GetJobInfo(string jobRootFolder)
        {
            DateTime date = DateTime.MinValue;
            DateTime lastHeartBeat = DateTime.MinValue;
            ClusterJobInformation.ClusterJobStatus status = ClusterJobInformation.ClusterJobStatus.Unknown;
            bool found = false;
            string jobName = jobRootFolder;

            var jobsFolders = this.config.AzureClient.EnumerateDirectory(jobRootFolder).ToList();
            foreach (var file in jobsFolders)
            {
                if (file.EndsWith("heartbeat"))
                {
                    var blob = this.config.AzureClient.Container.GetPageBlobReference(file);
                    blob.FetchAttributes();
                    var props = blob.Metadata;
                    if (props.ContainsKey("status"))
                    {
                        var st = props["status"];
                        switch (st)
                        {
                            case "failure":
                                status = ClusterJobInformation.ClusterJobStatus.Failed;
                                break;
                            case "success":
                                status = ClusterJobInformation.ClusterJobStatus.Succeeded;
                                break;
                            case "running":
                                status = ClusterJobInformation.ClusterJobStatus.Running;
                                break;
                            case "killed":
                                status = ClusterJobInformation.ClusterJobStatus.Cancelled;
                                break;
                            default:
                                Console.WriteLine("Unknown status " + st);
                                break;
                        }
                    }
                    if (props.ContainsKey("heartbeat"))
                    {
                        var hb = props["heartbeat"];
                        if (DateTime.TryParse(hb, out lastHeartBeat))
                        {
                            lastHeartBeat = lastHeartBeat.ToLocalTime();
                            if (status == ClusterJobInformation.ClusterJobStatus.Running &&
                                DateTime.Now - lastHeartBeat > TimeSpan.FromSeconds(40))
                                // job has in fact crashed
                                status = ClusterJobInformation.ClusterJobStatus.Failed;
                        }
                    }
                    if (props.ContainsKey("jobname"))
                    {
                        jobName = props["jobname"];
                    }
                    if (props.ContainsKey("starttime"))
                    {
                        var t = props["starttime"];
                        if (DateTime.TryParse(t, out date))
                            date = date.ToLocalTime();
                    }
                    
                    found = true;
                }
                else if (file.Contains("DryadLinqProgram__") && 
                    // newer heartbeats contain the date
                    date != DateTime.MinValue)
                {
                    var blob = this.config.AzureClient.Container.GetBlockBlobReference(file);
                    blob.FetchAttributes();
                    var props = blob.Properties;
                    if (props.LastModified.HasValue)
                    {
                        date = props.LastModified.Value.DateTime;
                        date = date.ToLocalTime();
                    }
                }
            }

            if (!found) return null;
            
            TimeSpan running = TimeSpan.Zero;
            if (date != DateTime.MinValue && lastHeartBeat != DateTime.MinValue)
                running = lastHeartBeat - date;
            var info = new ClusterJobInformation(this.config.Name, "", jobRootFolder, jobName, Environment.UserName, date, running, status);
            return info;
        }

        /// <summary>
        /// Refresh the job summary status.
        /// </summary>
        /// <param name="summary">Summary to refresh.</param>
        /// <param name="manager">Communication manager.</param>        
        public override void RefreshStatus(DryadLinqJobSummary summary, CommManager manager)
        {
            // refresh the whole list
            ClusterJobInformation info = this.GetJobInfo(summary.JobID);
            if (info == null)
            {
                summary.Status = ClusterJobInformation.ClusterJobStatus.Unknown;
                return;
            }
            summary.Status = info.Status;
        }

        /// <summary>
        /// Cancel the specified job.
        /// </summary>
        /// <param name="job">Job whose execution is cancelled.</param>
        /// <returns>True if the cancellation succeeded.</returns>
        public override bool CancelJob(DryadLinqJobSummary job)
        {
            AzureUtils.KillJob(this.config.AccountName, this.config.AccountKey, this.config.Container, job.ClusterJobId);
            return true;
        }
    }
}
