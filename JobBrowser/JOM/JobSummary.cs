
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
using System.IO;
using System.Text;
using Microsoft.Research.Tools;

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

}
