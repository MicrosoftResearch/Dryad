
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

using System.Security.Cryptography.X509Certificates;
using System.Xml.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Net;

using Microsoft.Research.Peloponnese.Storage;
using Microsoft.Research.Tools;
using Microsoft.WindowsAzure.Management.HDInsight;

namespace Microsoft.Research.JobObjectModel
{
    /// <summary>
    /// Error during conversation with cluster.
    /// </summary>
    public sealed class ClusterException : Exception
    {
        /// <summary>
        /// Create an exception about handling a cluster.
        /// </summary>
        /// <param name="message">Exception message.</param>
        public ClusterException(string message) : base(message) { }
    }

    /// <summary>
    /// Identifier for a Dryad process; for now we are using globally-unique process GUID.
    /// A Dryad Job identifier is always the same as the job manager process guid.
    /// </summary>
    [Serializable]
    public class DryadProcessIdentifier : IEquatable<DryadProcessIdentifier>
    {
        /// <summary>
        /// Process identifier.
        /// </summary>
        private string processIdentifier;

        /// <summary>
        /// Used just for XML serialization.
        /// </summary>
        public DryadProcessIdentifier()
        { }

        /// <summary>
        /// create an indentifier struct. 
        /// Be sure to initialize all fields before use;
        /// </summary>
        /// <param name="pid">The id of the process (platform-dependent).</param>
        public DryadProcessIdentifier(string pid)
        {
            this.processIdentifier = pid;
        }

        /// <summary>
        /// Process identifier; public for serialization only.
        /// </summary>
        public string ProcessIdentifier
        {
            get { return this.processIdentifier; }
            set { this.processIdentifier = value; }
        }

        /// <summary>
        /// If true the process identifier is not known.
        /// </summary>
        public bool IsUnknown { get { return this.ProcessIdentifier == null; } }

        /// <summary>
        /// Human-readable description of the process identifier.
        /// </summary>
        /// <returns>An empty string if the pid is not set.</returns>
        public override string ToString()
        {
            if (this.ProcessIdentifier != null) return this.ProcessIdentifier;
            return "";
        }

        /// <summary>
        /// Equality test.
        /// </summary>
        /// <param name="obj">Object to compare to.</param>
        /// <returns>True if both objects represent the same process id.</returns>
        public override bool Equals(object obj)
        {
            if (!(obj is DryadProcessIdentifier))
                return false;
            return this.Equals((DryadProcessIdentifier)obj);
        }

        #region IEquatable<DryadProcessIdentifier> Members
        /// <summary>
        /// Equality test.
        /// </summary>
        /// <param name="other">Process id to compare to.</param>
        /// <returns>True if the id's represent the same process.</returns>
        public bool Equals(DryadProcessIdentifier other)
        {
            if (this.IsUnknown)
                return other.IsUnknown;
            if (other.IsUnknown)
                return false;
            return this.ProcessIdentifier.Equals(other.ProcessIdentifier);
        }

        /// <summary>
        /// Overriden implementation of getHashCode.
        /// </summary>
        /// <returns>The hashcode of the process id.</returns>
        public override int GetHashCode()
        {
            // ReSharper disable once BaseObjectGetHashCodeCallInGetHashCode
            return base.GetHashCode();
        }
        #endregion
    }

    /// <summary>
    /// Brief summary of an executed DryadLINQ job.
    /// </summary>
    [Serializable]
    public sealed class DryadLinqJobSummary : IEquatable<DryadLinqJobSummary>
    {
        /// <summary>
        /// Empty constructor for XML serialization.
        /// </summary>
        public DryadLinqJobSummary()
        { }

        /// <summary>
        /// Initialize a job summary.
        /// </summary>
        /// <param name="cluster">Cluster where the job ran.</param>
        /// <param name="clusterType">A string corresponding to the type of ClusterConfiguration.</param>
        /// <param name="machine">Machine where job manager ran.</param>
        /// <param name="jobId">Id of job.</param>
        /// <param name="jmProcessGuid">Guid of job manager process.</param>
        /// <param name="clusterJobId">Id of job on the cluster.</param>
        /// <param name="friendlyname">Friendly name used.</param>
        /// <param name="username">Who ran the job.</param>
        /// <param name="date">Start date (not completion date).</param>
        /// <param name="status">Job status.</param>
        /// <param name="endTime">Estimated end running time.</param>
        /// <param name="virtualcluster">Virtual cluster where job ran.</param>
        public DryadLinqJobSummary(
            string cluster,
            ClusterConfiguration.ClusterType clusterType,
            string virtualcluster,
            string machine,
            string jobId,
            string clusterJobId,
            DryadProcessIdentifier jmProcessGuid,
            string friendlyname,
            string username,
            DateTime date,
            DateTime endTime,
            ClusterJobInformation.ClusterJobStatus status)
        {
            this.VirtualCluster = virtualcluster;
            this.Cluster = cluster;
            this.ClusterType = clusterType;
            this.Machine = machine;
            this.Name = friendlyname;
            this.User = username;
            this.Date = date;
            this.EndTime = endTime;
            this.Status = status;
            this.ManagerProcessGuid = jmProcessGuid;
            this.JobID = jobId;
            this.ClusterJobId = clusterJobId;
        }

        /// <summary>
        /// Cluster where the job ran.
        /// </summary>
        public string Cluster { get; /*private*/ set; }
        /// <summary>
        /// Id of cluster job that originated this DryadLinq job (can be used to find the cluster job from the dryadlinq job).
        /// </summary>
        public string ClusterJobId { get; /*private*/ set; }
        /// <summary>
        /// Cluster where the job ran.
        /// </summary>
        public DateTime EndTime { get; /*private*/ set; }
        /// <summary>
        /// String describing cluster type.
        /// </summary>
        public ClusterConfiguration.ClusterType ClusterType { get; /*private*/ set; }
        /// <summary>
        /// Virtual cluster where job ran.
        /// </summary>
        public string VirtualCluster { get; /*private*/ set; }
        /// <summary>
        /// (Friendly) name of the job.
        /// </summary>
        public string Name { get; /*private*/ set; }
        /// <summary>
        /// User who submitted job.
        /// </summary>
        public string User { get; /*private*/ set; }
        /// <summary>
        /// ID of job on the cluster.
        /// </summary>
        public string JobID { get; /*private*/ set; }
        /// <summary>
        /// The Guid of the job manager process.
        /// </summary>
        public DryadProcessIdentifier ManagerProcessGuid { set; get; }

        /// <summary>
        /// User who submitted job.
        /// </summary>
        public string GetAlias()
        {
            int pos = User.IndexOf(@"\");
            return User.Substring(pos + 1);
        }

        /// <summary>
        /// Date when job was submitted.
        /// </summary>
        public DateTime Date { get; /*private*/ set; }

        /// <summary>
        /// Did the job fail?
        /// </summary>
        public ClusterJobInformation.ClusterJobStatus Status { get; /*internal*/ set; }

        /// <summary>
        /// Machine where the job manager ran.
        /// </summary>
        public string Machine { get; /*private*/ set; }

        /// <summary>
        /// Get a short name for this job summary.
        /// </summary>
        /// <returns>Short name of job summary.</returns>
        public string ShortName()
        {
            // we use the starting time to uniquify the job name
            return this.Date.ToString("s") + "-" + this.Name;
        }

        /// <summary>
        /// True if these two summaries are the same.  The status and end time do not matter, since the job may still be running.
        /// </summary>
        /// <param name="other">Summary to compare against.</param>
        /// <returns>True if they are equal.</returns>
        public bool Equals(DryadLinqJobSummary other)
        {
            return this.Cluster == other.Cluster &&
                this.ClusterJobId == other.ClusterJobId &&
                this.Date == other.Date &&
                this.Machine == other.Machine &&
                this.Name == other.Name &&
                this.User == other.User;
        }

        /// <summary>
        /// Hashcode proper for the equality test.
        /// </summary>
        /// <returns>The object hashcode.</returns>
        public override int GetHashCode()
        {
            return this.ClusterJobId.GetHashCode() ^ this.ClusterJobId.GetHashCode() ^ this.Date.GetHashCode() ^ this.Machine.GetHashCode() ^ this.Name.GetHashCode() ^ this.User.GetHashCode();
        }

        /// <summary>
        /// A string describing the unique identifying part of the summary.
        /// Two different summaries may represent the same job at different times.
        /// </summary>
        /// <returns>The part common to all jobs.</returns>
        public string AsIdentifyingString()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("Cluster={0} ClusterJobID={1} Date={2} Machine={3} Name={4} User={5}",
                this.Cluster, this.ClusterJobId, this.Date, this.Machine, this.Name, this.User);
            return builder.ToString();
        }
    }

    /// <summary>
    /// This class is an abstraction of a cluster-level job, as opposed to a DryadLINQ job.
    /// In Cosmos that's called a task, in HPC that's called a Job.
    /// (In cosmos a task is a recurring job. In DryadLINQ running on top of cosmos, a task always contains exactly one job.)
    /// </summary>
    public class ClusterJobInformation : IEquatable<ClusterJobInformation>
    {
        /// <summary>
        /// Status of a cluster job.
        /// </summary>
        public enum ClusterJobStatus
        {
            /// <summary>
            /// Job is still running.
            /// </summary>
            Running,
            /// <summary>
            /// Job has finished successfully.
            /// </summary>
            Succeeded,
            /// <summary>
            /// Job has finished and has failed.
            /// </summary>
            Failed,
            /// <summary>
            /// Job has been cancelled. Not precise on cosmos clusters.
            /// </summary>
            Cancelled,
            /// <summary>
            /// Could not determine job status.
            /// </summary>
            Unknown,
        };

        /// <summary>
        /// True if job is finished, false if not, or unknown.
        /// </summary>
        /// <param name="status">Job status.</param>
        /// <returns>True if the job is no longer running.</returns>
        public static bool JobIsFinished(ClusterJobStatus status)
        {
            switch (status)
            {
                case ClusterJobInformation.ClusterJobStatus.Failed:
                case ClusterJobInformation.ClusterJobStatus.Succeeded:
                case ClusterJobInformation.ClusterJobStatus.Cancelled:
                    return true;
                case ClusterJobInformation.ClusterJobStatus.Running:
                case ClusterJobInformation.ClusterJobStatus.Unknown:
                    return false;
                default:
                    throw new InvalidDataException("Invalid job status " + status);
            }
        }

        /// <summary>
        /// Create a cluster job structure from a bunch of information.
        /// </summary>
        /// <param name="cluster">Cluster where the job is running.</param>
        /// <param name="clusterJobGuid">Cluster job guid.</param>
        /// <param name="jobName">Name of the cluster job.</param>
        /// <param name="username">User who submitted cluster job.</param>
        /// <param name="date">Last execution of cluster job.</param>
        /// <param name="status">Execution status.</param>
        /// <param name="runningTime">Time the job ran.</param>
        /// <param name="virtualCluster">Cluster where the job has run.</param>
        public ClusterJobInformation(
            string cluster, 
            string virtualCluster,
            string clusterJobGuid, 
            string jobName, 
            string username, 
            DateTime date, 
            TimeSpan runningTime,
            ClusterJobStatus status)
        {
            this.VirtualCluster = virtualCluster;
            this.Cluster = cluster;
            this.ClusterJobID = clusterJobGuid;
            this.Name = jobName;
            this.User = username;
            this.Date = date;
            this.EstimatedRunningTime = runningTime;
            this.Status = status;
            this.JobSummary = null;
        }

        /// <summary>
        /// Name of cluster job.
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// Uset who submitted cluster job.
        /// </summary>
        public string User { get; set; }
        /// <summary>
        /// Date when job was submitted.
        /// </summary>
        public DateTime Date { get; set; }
        /// <summary>
        /// ID of Job on cluster.
        /// </summary>
        public string ClusterJobID { get; set; }
        /// <summary>
        /// Status of the execution.
        /// </summary>
        public ClusterJobStatus Status { get; set; }
        /// <summary>
        /// Cluster where the job ran.
        /// </summary>
        public string Cluster { get; set; }
        /// <summary>
        /// In some installations a cluster is composed of multiple virtual clusters.
        /// </summary>
        public string VirtualCluster { get; set; }
        /// <summary>
        /// Is the cluster job information still available on the cluster?
        /// </summary>
        public bool IsUnavailable { get; set; }
        /// <summary>
        /// Cache here the associated job, if available.  Null if not cached.
        /// </summary>
        private DryadLinqJobSummary JobSummary { get; set; }
        /// <summary>
        /// Estimated time the job ran.
        /// </summary>
        public TimeSpan EstimatedRunningTime { get; set; }

        /// <summary>
        /// If known, set the associated job summary.
        /// </summary>
        /// <param name="summary">Job summary for this cluster job.</param>
        public void SetAssociatedSummary(DryadLinqJobSummary summary)
        {
            this.JobSummary = summary;
        }

        /// <summary>
        /// Discover the dryadlinq job associated with a cluster job.
        /// </summary>
        /// <param name="status">Cluster configuration.</param>
        /// <returns>The job, if any</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        public DryadLinqJobSummary DiscoverDryadLinqJob(ClusterStatus status, StatusReporter reporter)
        {
            if (this.IsUnavailable)
                return null;
            if (this.JobSummary != null)
                return this.JobSummary;

            DryadLinqJobSummary j = status.DiscoverDryadLinqJobFromClusterJob(this, reporter);
            if (j == null)
            {
                this.IsUnavailable = true;
            }
            return this.JobSummary = j;
        }

        /// <summary>
        /// Copy the content of a cluster job.
        /// </summary>
        /// <param name="refresh">The value to copy from.</param>
        internal void Copy(ClusterJobInformation refresh)
        {
            this.Name = refresh.Name;
            this.Status = refresh.Status;
            this.User = refresh.User;
            this.JobSummary = refresh.JobSummary;
            this.ClusterJobID = refresh.ClusterJobID;
            this.Date = refresh.Date;
            this.IsUnavailable = refresh.IsUnavailable;
            this.VirtualCluster = refresh.VirtualCluster;
            this.EstimatedRunningTime = refresh.EstimatedRunningTime;
        }

        /// <summary>
        /// True if these two records represent the same job.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(ClusterJobInformation other)
        {
            return 
                this.Cluster == other.Cluster &&
                this.VirtualCluster == other.VirtualCluster &&
                this.Name == other.Name &&
                this.User == other.User &&
                this.ClusterJobID == other.ClusterJobID &&
                this.Date == other.Date;
        }
    }

    /// <summary>
    /// Serializable properties key-value pairs.
    /// </summary>
    [Serializable]
    public class PropertySetting
    {
        /// <summary>
        /// Property of a configuration.
        /// </summary>
        public string Property { get; set; }
        /// <summary>
        /// Value associated with property.
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Empty constructor, for serialization.
        /// </summary>
        public PropertySetting()
        {
        }

        /// <summary>
        /// Create a property setting.
        /// </summary>
        /// <param name="prop">Property name.</param>
        /// <param name="value">Value.</param>
        public PropertySetting(String prop, string value)
        {
            this.Property = prop;
            this.Value = value;
        }
    }

    /// <summary>
    /// The serializable data part of a clusterConfiguration.
    /// </summary>
    [Serializable]
    public class ClusterConfigurationSerialization
    {
        /// <summary>
        /// Cluster name.
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// Cluster type.
        /// </summary>
        public ClusterConfiguration.ClusterType Type { get; set; }
        /// <summary>
        /// The other properties.
        /// </summary>
        public List<PropertySetting> Properties { get; set; }

        /// <summary>
        /// Create a ClusterConfiguration from its serialization.
        /// </summary>
        /// <returns>The corresponding cluster configuration.</returns>
        public ClusterConfiguration Create()
        {
            var config = ClusterConfiguration.CreateConfiguration(this.Type);
            config.Name = this.Name;
            for (int i = 0; i < this.Properties.Count; i++)
            {
                var property = config.GetType().GetProperty(this.Properties[i].Property);
                property.SetValue(config, this.Properties[i].Value);
            }

            return config;
        }
    }

    /// <summary>
    /// All configuration parameters descrbing the cluster setup should be here.
    /// </summary>
    public abstract class ClusterConfiguration
    {
        /// <summary>
        /// The type of runtime for the cluster.
        /// </summary>
        public enum ClusterType
        {
            /// <summary>
            /// Could not detect cluster version.
            /// </summary>
            Unknown,
            /// <summary>
            /// Fake cluster, used for post-mortem debugging; keeps some information about jobs in a local folder.
            /// </summary>
            Cache,
            /// <summary>
            /// Cluster emulated on a local machine
            /// </summary>
            LocalEmulator,
            /// <summary>
            /// Azure DFS client
            /// </summary>
            AzureDfs,
            /// <summary>
            /// Max type, unused; for enumerating.
            /// </summary>
            MaxUnused,
        };

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public abstract List<string> GetPropertiesToEdit();
        /// <summary>
        /// Must be called after setting all properties.
        /// <returns>Returns null if initialization succeeds, an error otherwise.</returns>
        /// </summary>
        public abstract string Initialize();

        /// <summary>
        /// Enumerate all clusters this user is subscribed to.
        /// </summary>
        /// <returns>A list of clusters.</returns>
        public static IEnumerable<ClusterConfiguration> EnumerateSubscribedClusters()
        {
            return AzureDfsClusterConfiguration.EnumerateAzureDfsSubscribedClusters();
        }

        /// <summary>
        /// Create serialization data structure for this configuration.
        /// </summary>
        /// <returns>The corresponding serialization.</returns>
        public ClusterConfigurationSerialization ExtractData()
        {
            ClusterConfigurationSerialization result = new ClusterConfigurationSerialization
            {
                Type = this.TypeOfCluster,
                Name = this.Name,
                Properties = new List<PropertySetting>()
            };

            foreach (var prop in this.GetPropertiesToEdit())
            {
                var property = this.GetType().GetProperty(prop);
                var value = property.GetValue(this);
                PropertySetting setting = new PropertySetting(prop, value != null ? value.ToString() : null);
                result.Properties.Add(setting);
            }

            return result;
        }

        /// <summary>
        /// The serialization of all known clusters.
        /// </summary>
        /// <returns>The serializations of all known clusters in a list.</returns>
        public static List<ClusterConfigurationSerialization> KnownClustersSerialization()
        {
            var result = new List<ClusterConfigurationSerialization>();
            foreach (var clus in KnownClusters.Values)
            {
                result.Add(clus.ExtractData());
            }
            return result;
        }

        /// <summary>
        /// Reconstruct the known clusters from the saved serialization.
        /// </summary>
        /// <param name="sers">Serializations for each cluster.</param>
        public static void ReconstructKnownCluster(List<ClusterConfigurationSerialization> sers)
        {
            if (sers == null) return;

            foreach (var cs in sers)
            {
                var clus = cs.Create();
                if (clus == null) continue;

                string error = clus.Initialize();
                if (error != null) continue;

                AddKnownCluster(clus);
            }
        }

        /// <summary>
        /// Create a cluster configuration of the specified type.
        /// </summary>
        /// <param name="type">Type of cluster.</param>
        protected ClusterConfiguration(ClusterConfiguration.ClusterType type)
        {
            this.TypeOfCluster = type;
        }

        /// <summary>
        /// Credentials to use for authentication.
        /// </summary>
        [NonSerialized]
        // ReSharper disable once NotAccessedField.Global
        protected NetworkCredential credentials;

        /// <summary>
        /// Set the credentials for connecting to this cluster.
        /// </summary>
        /// <param name="credential">Credentials to use.</param>
        public void SetCredential(NetworkCredential credential)
        {
            this.credentials = credential;
        }


        /// <summary>
        /// The name of this cluster.
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// The type of the cluster
        /// </summary>
        public ClusterType TypeOfCluster { get; protected set; }
        /// <summary>
        /// Base directory on all cluster machines.
        /// </summary>
        public string DryadInstallDirectory { get; protected set; }
        /// <summary>
        /// Default domain for user account to use for connecting to cluster; empty to use the current domain.
        /// </summary>
        public string UserDomain { get; protected set; }

        /// <summary>
        /// The machine where the metadata for the copied jobs is stored.
        /// </summary>
        public string MetaDataMachine { get; protected set; }
        /// <summary>
        /// Time zone of the analyzed cluster.  We assume that the cluster is in the local time zone.
        /// </summary>
        /// <returns>Timezone infomation of the cluster</returns>
        /// <param name="job">Job we are interested in.</param>
        public virtual TimeZoneInfo GetClusterTimeZone(DryadLinqJobSummary job)
        {
            return TimeZoneInfo.Local;
        }
        /// <summary>
        /// The directory where a specific process is created on the cluster.
        /// </summary>
        /// <param name="identifier">Process identifier</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>Home directory containing the process information (not working directory of vertex).</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public abstract IClusterResidentObject ProcessDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job);
        /// <summary>
        /// Work directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public abstract IClusterResidentObject ProcessWorkDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job);
        /// <summary>
        /// Given an input file identify the process that produced it.
        /// </summary>
        /// <param name="input">Input file of a process.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>The identity of the process that produced the file.</returns>
        // ReSharper disable UnusedParameter.Global
        public abstract DryadProcessIdentifier ProcessFromInputFile(IClusterResidentObject input, DryadLinqJobSummary job);
        // ReSharper restore UnusedParameter.Global
        /// <summary>
        /// File containing standard output of a process.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>The pathname to the standard output.</returns>
        /// <param name="terminated">True if vertex is terminated.</param>
        public virtual IClusterResidentObject ProcessStdoutFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            IClusterResidentObject processdir = this.ProcessDirectory(identifier, terminated, machine, job);
            IClusterResidentObject file = processdir.GetFile("stdout.txt");
            return file;
        }

        /// <summary>
        /// Create a cluster status for this cluster.
        /// </summary>
        /// <returns>The proper cluster status.</returns>
        public abstract ClusterStatus CreateClusterStatus();

        static Dictionary<string, ClusterConfiguration> KnownClusters;

        static ClusterConfiguration()
        {
            KnownClusters = new Dictionary<string, ClusterConfiguration>();
            //KnownClusters.Add("Cache", new CacheClusterConfiguration());
            //KnownClusters.Add("Local emulation", new LocalEmulator());
            //KnownClusters.Add("AzureDfs", new AzureDfsClusterConfiguration());
        }

        /// <summary>
        /// A known cluster configuration by name.
        /// </summary>
        /// <param name="name">Name of the cluster.</param>
        /// <returns>The cluster configuration for that cluster.</returns>
        public static ClusterConfiguration KnownClusterByName(string name)
        {
            if (KnownClusters.ContainsKey(name))
                return KnownClusters[name];
            return null;
        }

        /// <summary>
        /// Add a new cluster to the list of known clusters.
        /// </summary>
        /// <param name="config">New config to add.</param>
        public static void AddKnownCluster(ClusterConfiguration config)
        {
            if (KnownClusters.ContainsKey(config.Name))
                KnownClusters[config.Name] = config;
            else
                KnownClusters.Add(config.Name, config);
        }

        /// <summary>
        /// Remove a cluster form the list of known clusters.
        /// </summary>
        /// <param name="name">Name of cluster to remove.</param>
        /// <returns>The removed configuration.</returns>
        public static ClusterConfiguration RemoveKnownCluster(string name)
        {
            if (KnownClusters.ContainsKey(name))
            {
                var config = KnownClusters[name];
                KnownClusters.Remove(name);
                return config;
            }
            return null;
        }

        /// <summary>
        /// Get the list of known clusters.
        /// </summary>
        /// <returns>A list of cluster names.</returns>
        public static IEnumerable<string> GetKnownClusterNames()
        {
            return KnownClusters.Keys;
        }
        
        /// <summary>
        /// File containing standard error of a process.  Not available on all architectures.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>A reference to the standard output.</returns>
        /// <param name="terminated">Vertex state.</param>
        public virtual IClusterResidentObject ProcessStderrFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            return this.ProcessStdoutFile(identifier, terminated, machine, job);
        }

        /// <summary>
        /// Log directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">Vertex state.</param>
        public abstract IClusterResidentObject ProcessLogDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job);
        /// <summary>
        /// A shell pattern matching (just the) log files produced by a job manager process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job where the JM process belongs.</param>
        // ReSharper disable once UnusedParameter.Global
        public abstract string JMLogFilesPattern(bool error, DryadLinqJobSummary job);
        /// <summary>
        /// A shell pattern matching (just the) log files produced by a vertex process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job containing this vertex.</param>
        public abstract string VertexLogFilesPattern(bool error, DryadLinqJobSummary job);
        /// <summary>
        /// Directory where the perfmon logs are being collected, relative to local machine.
        /// </summary>
        /// <returns>A directory containing the perfmon logs.</returns>
        public virtual string PerfmonLogDirectory()
        {
            return Path.Combine(this.DryadInstallDirectory, "Perfmon");
        }

        /// <summary>
        /// Create an empty configuration of the specified type.
        /// </summary>
        /// <param name="type">Configuration type.</param>
        /// <returns>The created configuration.</returns>
        public static ClusterConfiguration CreateConfiguration(ClusterType type)
        {
            switch (type)
            {
                case ClusterType.Cache:
                    return new CacheClusterConfiguration();
                case ClusterType.LocalEmulator:
                    return new LocalEmulator();
                case ClusterType.AzureDfs:
                    return new AzureDfsClusterConfiguration();
                case ClusterType.Unknown:
                case ClusterType.MaxUnused:
                default:
                    throw new ArgumentOutOfRangeException("type");
            }
        }

        /// <summary>
        /// Convert a GUID printed by the Dryad job manager into a process-id, which is platform dependent.
        /// </summary>
        /// <param name="guid">Process guid.</param>
        /// <returns>Process id.</returns>\
        /// <param name="job">Job where guid is from.</param>
        public abstract string ExtractPidFromGuid(string guid, DryadLinqJobSummary job);

        /// <summary>
        /// Navigate to a given url and return a stream reader with the corresponding web page.
        /// </summary>
        /// <param name="url">Url to navigate to.</param>
        /// <returns>The web page.</returns>
        internal virtual StreamReader Navigate(string url)
        {
            return Utilities.Navigate(url, null);
        }

        /// <summary>
        /// The file containing the job query plan.
        /// </summary>
        /// <param name="job">Job whose plan is sought.</param>
        /// <returns>An object containing the path, or null if it cannot be found.</returns>
        public virtual IClusterResidentObject JobQueryPlan(DryadLinqJobSummary job)
        {
            try
            {
                IClusterResidentObject dir = this.ProcessWorkDirectory(job.ManagerProcessGuid, true, job.Machine, job); // immutable
                var matchingfiles = dir.GetFilesAndFolders("DryadLinqProgram__*.xml").ToList();
                if (matchingfiles.Count() != 1)
                    throw new ClusterException("Could not find query plan file; got " + matchingfiles.Count() + " possible matches");
                IClusterResidentObject result = matchingfiles.First();
                result.ShouldCacheLocally = true; // immutable
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
            }
        }
    }



    /// <summary>
    /// A cached cluster is just a set of files on a filesystem which are saved from old jobs; allows postmortem analysis.
    /// </summary>
    public sealed class CacheClusterConfiguration : ClusterConfiguration
    {
        /// <summary>
        /// Each cached job may originate from a completely different cluster; cache the actual configurations here.
        /// </summary>
        Dictionary<string, ClusterConfiguration> jobConfig;

        /// <summary>
        /// Folder where the information is saved.
        /// </summary>
        public string LocalInformationFolder { get; set; }

        /// <summary>
        /// Create a fake cluster which stores the information in the specified folder.
        /// </summary>
        public CacheClusterConfiguration() : base(ClusterConfiguration.ClusterType.Cache)
        {
            this.LocalInformationFolder = null;
            this.Name = "Cache";
            this.jobConfig = new Dictionary<string, ClusterConfiguration>();
        }

        /// <summary>
        /// Make this cluster the active cache.
        /// </summary>
        public void StartCaching()
        {
            string folder = String.IsNullOrEmpty(this.LocalInformationFolder) ? null : this.LocalInformationFolder;
            CachedClusterResidentObject.CacheDirectory = folder; // enables caching
        }

        /// <summary>
        /// Disable caching in this cluster.
        /// </summary>
        public void StopCaching()
        {
            CachedClusterResidentObject.CacheDirectory = null; // disables caching
        }

        private static List<string> props = new List<string>
        {
            "LocalInformationFolder"
        };

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return props;
        }

        /// <summary>
        /// Must be called after setting all properties.
        /// <returns>Returns null if initialization succeeds, an error otherwise.</returns>
        /// </summary>
        public override string Initialize()
        {            
            return null;
        }

        /// <summary>
        /// Create a cluster status for this cluster.
        /// </summary>
        /// <returns>The proper cluster status.</returns>
        public override ClusterStatus CreateClusterStatus()
        {
            var stat = ClusterStatus.LookupStatus(this.Name);
            if (stat != null) return stat;
            return new CacheClusterStatus(this);
        }

        /// <summary>
        /// Find the actual cluster configuration corresponding to the specified job.
        /// </summary>
        /// <param name="job">Cached job.</param>
        /// <returns>A configuration that could have been used for the cluster running the job.</returns>
        public ClusterConfiguration ActualConfig(DryadLinqJobSummary job)
        {
            if (!this.jobConfig.ContainsKey(job.Cluster))
            {
                ClusterConfiguration config = ClusterConfiguration.KnownClusterByName(job.Cluster);
                if (config == null)
                    config = ClusterConfiguration.CreateConfiguration(job.ClusterType);
                this.jobConfig.Add(job.Cluster, config);
            }
            return this.jobConfig[job.Cluster];
        }

        /// <summary>
        /// Object that can be used to access the process directory.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="terminated">True if process is terminated.</param>
        /// <param name="machine">Machine where the process ran.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>An object which can be used to access the process home directory.</returns>
        public override IClusterResidentObject ProcessDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            IClusterResidentObject pd = config.ProcessDirectory(identifier, terminated, machine, job);
            if (pd == null) return null;
            IClusterResidentObject result = new FolderInCachedCluster(pd as CachedClusterResidentObject);
            return result;
        }

        /// <summary>
        /// Object that can be used to access the process work directory.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="terminated">True if process is terminated.</param>
        /// <param name="machine">Machine where the process ran.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>An object which can be used to access the process work directory.</returns>
        public override IClusterResidentObject ProcessWorkDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            IClusterResidentObject wd = config.ProcessWorkDirectory(identifier, terminated, machine, job);
            if (wd == null) return null;
            IClusterResidentObject result = new FolderInCachedCluster(wd as CachedClusterResidentObject);
            return result;
        }

        /// <summary>
        /// Time zone of cluster.
        /// </summary>
        /// <param name="job">Job we are interested in.</param>
        /// <returns>The time zome of the cluster.</returns>
        public override TimeZoneInfo GetClusterTimeZone(DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.GetClusterTimeZone(job);
        }

        /// <summary>
        /// Given an input file identify the process that produced it.
        /// </summary>
        /// <param name="input">Input file of a process.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>The identity of the process that produced the file.</returns>
        public override DryadProcessIdentifier ProcessFromInputFile(IClusterResidentObject input, DryadLinqJobSummary job)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Object that can be used to access the process log directory.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="terminated">True if process is terminated.</param>
        /// <param name="machine">Machine where the process ran.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>An object which can be used to access the process log directory.</returns>
        public override IClusterResidentObject ProcessLogDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            IClusterResidentObject pld = config.ProcessLogDirectory(identifier, terminated, machine, job);
            if (pld == null) return null;
            IClusterResidentObject result = new FolderInCachedCluster(pld as CachedClusterResidentObject);
            return result;
        }

        /// <summary>
        /// Pattern which matches the log files.
        /// </summary>
        /// <param name="error">If true return only the log files containing errors.</param>
        /// <returns>A string that can be used to match only log files.</returns>
        /// <param name="job">Job where process belongs.</param>
        public override string JMLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.JMLogFilesPattern(error, job);
        }

        /// <summary>
        /// Pattern which matches the log files.
        /// </summary>
        /// <param name="error">If true return only the log files containing errors.</param>
        /// <returns>A string that can be used to match only log files.</returns>
        /// <param name="job">Job containing the vertex.</param>
        public override string VertexLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.VertexLogFilesPattern(error, job);
        }

        /// <summary>
        /// File containing standard output of a process.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>The pathname to the standard output.</returns>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessStdoutFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.ProcessStdoutFile(identifier, terminated, machine, job);
        }

        /// <summary>
        /// File containing standard error of a process.  Not available on all architectures.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>A reference to the standard output.</returns>
        /// <param name="terminated">Vertex state.</param>
        public override IClusterResidentObject ProcessStderrFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.ProcessStderrFile(identifier, terminated, machine, job);
        }

        /// <summary>
        /// Extract the process id from a guid.
        /// </summary>
        /// <param name="guid">Process guid.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>The process id.</returns>
        public override string ExtractPidFromGuid(string guid, DryadLinqJobSummary job)
        {
            ClusterConfiguration config = this.ActualConfig(job);
            return config.ExtractPidFromGuid(guid, job);
        }

        /// <summary>
        /// The file containing the job query plan.
        /// </summary>
        /// <param name="job">Job whose plan is sought.</param>
        /// <returns>An object containing the path, or null if it cannot be found.</returns>
        public override IClusterResidentObject JobQueryPlan(DryadLinqJobSummary job)
        {
            try
            {
                // we have to handle the xml plan differently; the cached file is renamed to always be "0".
                IClusterResidentObject dir = this.ProcessWorkDirectory(job.ManagerProcessGuid, true, job.Machine, job);
                if (dir == null) return null;
                IClusterResidentObject result = dir.GetFile("DryadLinqProgram__0.xml");
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
            }
        }
    }

    /// <summary>
    /// A local machine used to run an emulated Yarn cluster.
    /// </summary>
    public sealed class LocalEmulator : ClusterConfiguration
    {
        /// <summary>
        /// Folder where job logs are stored.
        /// </summary>
        public string JobsFolder { get; private set; }

        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        public LocalEmulator()
            : base(ClusterType.LocalEmulator)
        {
            string dryadHome = Environment.GetEnvironmentVariable("DRYAD_HOME");
            if (string.IsNullOrEmpty(dryadHome))
                throw new InvalidOperationException("Environment variable DRYAD_HOME is not set");
            this.JobsFolder = Path.Combine(dryadHome, "LocalJobs");
            this.Name = "LocalEmulator";
        }

        private static List<string> props = new List<string>();

        /// <summary>
        /// Must be called after setting all properties.
        /// <returns>Returns null if initialization succeeds, an error otherwise.</returns>
        /// </summary>
        public override string Initialize()
        {
            return null;
        }

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return props;
        }

        /// <summary>
        /// Create a cluster status for this cluster.
        /// </summary>
        /// <returns>The proper cluster status.</returns>
        public override ClusterStatus CreateClusterStatus()
        {
            var stat = ClusterStatus.LookupStatus(this.Name);
            if (stat != null) return stat;
            return new YarnEmulatedClusterStatus(this);
        }

        /// <summary>
        /// The directory where a specific process is created on the cluster.
        /// </summary>
        /// <param name="identifier">Process identifier</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>Home directory containing the process information (not working directory of vertex).</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            if (identifier.ToString() == "jm")
            {
                // The job manager process is special
                return new LocalFile(Utilities.PathCombine(this.JobsFolder, job.ClusterJobId, identifier.ProcessIdentifier, "Process.000.001"));
            }
            else
            {
                string folder = Utilities.PathCombine(this.JobsFolder, job.ClusterJobId, "Worker");
                return new LocalFile(Path.Combine(folder, identifier.ProcessIdentifier));
            }
        }

        /// <summary>
        /// Given an input file, identify the process that has produced it.
        /// </summary>
        /// <param name="input">Input file.</param>
        /// <param name="job">Job containing the process.</param>
        /// <returns>The process that has produced the file.</returns>
        public override DryadProcessIdentifier ProcessFromInputFile(IClusterResidentObject input, DryadLinqJobSummary job)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// File containing standard error of a process.  Not available on all architectures.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>A reference to the standard output.</returns>
        /// <param name="terminated">Vertex state.</param>
        public override IClusterResidentObject ProcessStderrFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            IClusterResidentObject processdir = this.ProcessDirectory(identifier, terminated, machine, job);
            IClusterResidentObject file = processdir.GetFile("stderr.txt");
            return file;
        }

        /// <summary>
        /// The file containing the job query plan.
        /// </summary>
        /// <param name="job">Job whose plan is sought.</param>
        /// <returns>An object containing the path, or null if it cannot be found.</returns>
        public override IClusterResidentObject JobQueryPlan(DryadLinqJobSummary job)
        {
            try
            {
                IClusterResidentObject dir = this.ProcessWorkDirectory(job.ManagerProcessGuid, true, job.Machine, job); // this is missing at this point
                //IClusterResidentObject dir = this.ProcessWorkDirectory(new DryadProcessIdentifier("Process.000.001"), true, job.Machine, job);
                var matchingfiles = dir.GetFilesAndFolders("DryadLinqProgram__*.xml").ToList();
                if (matchingfiles.Count() != 1)
                    throw new ClusterException("Could not find query plan file; got " + matchingfiles.Count() + " possible matches");
                IClusterResidentObject result = matchingfiles.First();
                result.ShouldCacheLocally = true; // immutable
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
            }
        }

        /// <summary>
        /// Work directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessWorkDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            return this.ProcessDirectory(identifier, terminated, machine, job);
        }

        /// <summary>
        /// Log directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="terminated">True if vertex is terminated.</param>        
        /// <param name="job">Job where the process belongs.</param>
        public override IClusterResidentObject ProcessLogDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            return this.ProcessDirectory(identifier, terminated, machine, job);
        }

        /// <summary>
        /// A shell pattern matching (just the) log files produced by a job manager process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job where process belongs.</param>
        public override string JMLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            return "stderr.txt";
        }

        /// <summary>
        /// A shell pattern matching (just the) log files produced by a vertex process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job where process belongs.</param>
        public override string VertexLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            return this.JMLogFilesPattern(error, job);
        }

        /// <summary>
        /// Convert a GUID printed by the Dryad job manager into a process-id, which is platform dependent.
        /// </summary>
        /// <param name="guid">Process guid.</param>
        /// <returns>Process id.</returns>
        /// <param name="job">Job where process belongs.</param>
        public override string ExtractPidFromGuid(string guid, DryadLinqJobSummary job)
        {
            return guid;
        }
    }

    /// <summary>
    /// Configuration for an AzureDfs cluster.
    /// </summary>
    public sealed class AzureDfsClusterConfiguration : ClusterConfiguration
    {
        /// <summary>
        /// Handle to client to enumerate logs.
        /// </summary>
        public AzureDfsClient AzureClient;

        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        public AzureDfsClusterConfiguration()
            : base(ClusterType.AzureDfs)
        {
        }

        /// <summary>
        /// Enumerate all the clusters this user is subscribed to.
        /// </summary>
        /// <returns>The list of clusters this user is subscribed to.</returns>
        public static IEnumerable<ClusterConfiguration> EnumerateAzureDfsSubscribedClusters()
        {
            var store = new X509Store();
            store.Open(OpenFlags.ReadOnly);
            var configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Windows Azure Powershell");
            var defaultFile = Path.Combine(configDir, "WindowsAzureProfile.xml");
            if (File.Exists(defaultFile))
            {
                using (FileStream s = new FileStream(defaultFile, FileMode.Open, FileAccess.Read))
                {
                    XDocument doc = XDocument.Load(s);
                    XNamespace ns = doc.Root.GetDefaultNamespace();
                    IEnumerable<XElement> subs = doc.Descendants(ns + "AzureSubscriptionData");
                    foreach (XElement sub in subs)
                    {
                        string thumbprint = sub.Descendants(ns + "ManagementCertificate").Single().Value;
                        string subId = sub.Descendants(ns + "SubscriptionId").Single().Value;
                        Guid subGuid = new Guid(subId);

                        X509Certificate2 cert = store.Certificates.Cast<X509Certificate2>().First(item => item.Thumbprint == thumbprint);

                        HDInsightCertificateCredential sCred = new HDInsightCertificateCredential(subGuid, cert);
                        IHDInsightClient sClient = HDInsightClient.Connect(sCred);
                        var clusters = sClient.ListClusters();
                        foreach (var cluster in clusters)
                        {
                            var account = cluster.DefaultStorageAccount;
                            var accountName = account.Name.Split('.').First();
                            Console.WriteLine("Cluster " + cluster.Name + " uses account " + accountName + " with key " + account.Key);

                            AzureDfsClusterConfiguration config = null;
                            try
                            {
                                config = new AzureDfsClusterConfiguration();
                                config.AzureClient = new AzureDfsClient(accountName, account.Key, "dryad-jobs");
                                config.Name = cluster.Name;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception while reconstructing cluster " + cluster.Name + ": " + ex);
                            }

                            if (config != null)
                                yield return config;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Azure account name.
        /// </summary>
        public
        string AccountName { get; set; }
        /// <summary>
        /// Azure account key.
        /// </summary>
        public string AccountKey { get; set; }
        /// <summary>
        /// Azure container.
        /// </summary>
        public string Container { get; set; }

        /// <summary>
        /// Must be called after setting all properties.
        /// Returns true if initialization succeeds.
        /// </summary>
        public override string Initialize()
        {
            try
            {
                this.AzureClient = new AzureDfsClient(
                    this.AccountName,
                    this.AccountKey,
                    this.Container);
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return ex.Message;
            }
        }

        /// <summary>
        /// The directory where a specific process is created on the cluster.
        /// </summary>
        /// <param name="identifier">Process identifier</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>Home directory containing the process information (not working directory of vertex).</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            if (identifier.ToString() == "jm")
            {
                // The job manager process is special
                var result = new AzureDfsFile(this, job, this.AzureClient, job.ClusterJobId, terminated, true);
                return result;
            }

            // vertices not supported
            return null;
        }

        /// <summary>
        /// Create a cluster status for this cluster.
        /// </summary>
        /// <returns>The proper cluster status.</returns>
        public override ClusterStatus CreateClusterStatus()
        {
            var stat = ClusterStatus.LookupStatus(this.Name);
            if (stat != null) return stat;
            return new AzureDfsClusterStatus(this);
        }

        /// <summary>
        /// Work directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessWorkDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            return this.ProcessDirectory(identifier, terminated, machine, job);
        }

        /// <summary>
        /// Given an input file identify the process that produced it.
        /// </summary>
        /// <param name="input">Input file of a process.</param>
        /// <param name="job">Job that contained the process.</param>
        /// <returns>The identity of the process that produced the file.</returns>
        // ReSharper disable UnusedParameter.Global
        public override DryadProcessIdentifier ProcessFromInputFile(IClusterResidentObject input, DryadLinqJobSummary job)
        {
            return null;
        }

        // ReSharper restore UnusedParameter.Global
        /// <summary>
        /// File containing standard output of a process.
        /// </summary>
        /// <param name="identifier">Process identifier.</param>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job containing process.</param>
        /// <returns>The pathname to the standard output.</returns>
        /// <param name="terminated">True if vertex is terminated.</param>
        public override IClusterResidentObject ProcessStdoutFile(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            if (identifier.ToString() == "jm")
            {
                IClusterResidentObject processdir = this.ProcessDirectory(identifier, terminated, machine, job);
                IClusterResidentObject file = processdir.GetFile("calypso.log");
                return file;
            }

            // vertices not supported
            return null;
        }

        /// <summary>
        /// Log directory of a process vertex.
        /// </summary>
        /// <param name="identifier">Vertex guid.</param>
        /// <returns>The path to the work directory of the vertex.</returns>
        /// <param name="machine">Machine where process ran.</param>
        /// <param name="job">Job where the process belongs.</param>
        /// <param name="terminated">Vertex state.</param>
        public override IClusterResidentObject ProcessLogDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            return this.ProcessDirectory(identifier, terminated, machine, job);
        }

        /// <summary>
        /// A shell pattern matching (just the) log files produced by a job manager process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job where the JM process belongs.</param>
        // ReSharper disable once UnusedParameter.Global
        public override string JMLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            return "*.log";
        }

        /// <summary>
        /// A shell pattern matching (just the) log files produced by a vertex process.
        /// </summary>
        /// <returns>Pattern matching the log files.</returns>
        /// <param name="error">If true, return only the error logs.</param>
        /// <param name="job">Job containing this vertex.</param>
        public override string VertexLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            return "*.log";
        }

        /// <summary>
        /// Convert a GUID printed by the Dryad job manager into a process-id, which is platform dependent.
        /// </summary>
        /// <param name="guid">Process guid.</param>
        /// <returns>Process id.</returns>\
        /// <param name="job">Job where guid is from.</param>
        public override string ExtractPidFromGuid(string guid, DryadLinqJobSummary job)
        {
            return guid;
        }

        /// <summary>
        /// The file containing the job query plan.
        /// </summary>
        /// <param name="job">Job whose plan is sought.</param>
        /// <returns>An object containing the path, or null if it cannot be found.</returns>
        public override IClusterResidentObject JobQueryPlan(DryadLinqJobSummary job)
        {
            try
            {
                IClusterResidentObject dir = this.ProcessWorkDirectory(job.ManagerProcessGuid, true, job.Machine, job); // immutable
                var matchingfiles = dir.GetFilesAndFolders("DryadLinqProgram__*.xml").ToList();
                if (matchingfiles.Count() != 1)
                    throw new ClusterException("Could not find query plan file; got " + matchingfiles.Count() + " possible matches");
                IClusterResidentObject result = matchingfiles.First();
                (result as AzureDfsFile).IsDfsStream = true;
                result.ShouldCacheLocally = true; // immutable
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
            }
        }

        private static List<string> props = new List<string> 
        {
            "AccountName",
            "AccountKey",
            "Container"
        };

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return props;
        }
    }
}
