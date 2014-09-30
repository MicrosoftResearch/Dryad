
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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.Research.Peloponnese.Azure;
using Microsoft.Research.Peloponnese.Hdfs;
using Microsoft.Research.Peloponnese.Shared;
using Microsoft.Research.Peloponnese.WebHdfs;
using Microsoft.Research.Peloponnese.Yarn;


using System.Xml.Linq;
using Microsoft.Research.Tools;
using Microsoft.WindowsAzure.Management.HDInsight;

namespace Microsoft.Research.JobObjectModel
{
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

                string stringValue = this.Properties[i].Value;
                object value;
                if (property.PropertyType == typeof (int))
                {
                    value = int.Parse(stringValue);
                }
                else if (property.PropertyType == typeof (string))
                {
                    value = stringValue;
                }
                else if (property.PropertyType == typeof (Uri))
                {
                    value = new Uri(stringValue);
                }
                else
                {
                    throw new InvalidCastException("Properties of type " + property.PropertyType + " not yet supported");
                }
                property.SetValue(config, value);
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
            /// Cluster is running on the cosmos runtime.
            /// </summary>
            Cosmos,
            /// <summary>
            /// Cluster is running on the windows high-performance computing platform released by external research.
            /// </summary>
            ExternalResearchHPC,
            /// <summary>
            /// The taiga version of HPC.
            /// </summary>
            HPC,
            /// <summary>
            /// Cosmos cluster running scope.
            /// </summary>
            Scope,
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
            /// Web-access to HDFS
            /// </summary>
            WebHdfs,
            /// <summary>
            /// Hdfs direct access.
            /// </summary>
            Hdfs,
            /// <summary>
            /// Error in creating configuration.
            /// </summary>
            Error,
            /// <summary>
            /// Max type, unused; for enumerating.
            /// </summary>
            MaxUnused,
        };

        /// <summary>
        /// Set of cluster types available.
        /// </summary>
        public static HashSet<ClusterType> Available;

        static ClusterConfiguration()
        {
            Available = new HashSet<ClusterType>();
            Available.Add(ClusterType.Cache);
            Available.Add(ClusterType.LocalEmulator);
            Available.Add(ClusterType.AzureDfs);
            Available.Add(ClusterType.WebHdfs);
            Available.Add(ClusterType.Hdfs);
        }

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
        /// True if the cluster supports diagnosis.
        /// </summary>
        public bool SupportsDiagnosis { get; protected set; }
        /// <summary>
        /// Cluster description.
        /// </summary>
        public string Description { get; set; }

        private delegate object Work();

        /// <summary>
        /// Enumerate all clusters this user is subscribed to.
        /// </summary>
        /// <returns>A list of clusters.</returns>
        public static IEnumerable<ClusterConfiguration> EnumerateSubscribedClusters()
        {
            // ReSharper disable once JoinDeclarationAndInitializer
            IEnumerable<ClusterConfiguration> list = null;

            try
            {
                Work work = AzureDfsClusterConfiguration.EnumerateAzureDfsSubscribedClusters;
                IAsyncResult result = work.BeginInvoke(null, null);
                if (result.IsCompleted == false)
                {
                    result.AsyncWaitHandle.WaitOne(3000, false);
                    if (result.IsCompleted == false)
                        throw new ApplicationException("Timeout scanning Azure clusters");
                }
                list = (List<ClusterConfiguration>)work.EndInvoke(result);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception enumerating DFS clusters: " + ex);
            }

            if (list != null)
            {
                foreach (var c in list)
                    yield return c;
            }

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
                try
                {
                    var clus = cs.Create();
                    if (clus == null) continue;

                    string error = clus.Initialize();
                    if (error != null) continue;

                    AddKnownCluster(clus);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error reconstructing saved cluster; skipping " + cs.Name + ":" + ex.Message);
                }
            }
        }

        /// <summary>
        /// Create a cluster configuration of the specified type.
        /// </summary>
        /// <param name="type">Type of cluster.</param>
        protected ClusterConfiguration(ClusterConfiguration.ClusterType type)
        {
            this.Description = "";
            this.TypeOfCluster = type;
            this.SupportsDiagnosis = true;
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
        public virtual string MetaDataMachine { get; protected set; }
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

        static Dictionary<string, ClusterConfiguration> KnownClusters = new Dictionary<string, ClusterConfiguration>();

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
            {
                KnownClusters[config.Name] = config;
            }
            else
            {
                KnownClusters.Add(config.Name, config);
            }
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
                case ClusterType.WebHdfs:
                    return new WebHdfsClusterConfiguration();
                case ClusterType.Hdfs:
                    return new HdfsClusterConfiguration();
                case ClusterType.Unknown:
                case ClusterType.MaxUnused:
                default:
                    return new ErrorConfiguration("Unsupported cluster type " + type);
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
        /// Navigate to a given url and return a stream with the corresponding web page.
        /// </summary>
        /// <param name="url">Url to navigate to.</param>
        /// <returns>The web page.</returns>
        internal virtual Stream Navigate(string url)
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
    /// Represents an error in creating a cluster configuration.
    /// </summary>
    public sealed class ErrorConfiguration : ClusterConfiguration
    {
        /// <summary>
        /// Error message.
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Create an Error Cluster.
        /// </summary>
        internal ErrorConfiguration(string message)
            : base(ClusterType.Error)
        {
            this.ErrorMessage = message;
        }

        private static List<string> properties = new List<string>();

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return properties;
        }

        /// <summary>
        /// Must be called after setting all properties.
        /// <returns>Returns null if initialization succeeds, an error otherwise.</returns>
        /// </summary>
        public override string Initialize()
        {
            return this.ErrorMessage;
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>        
        public override IClusterResidentObject ProcessDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>        
        public override IClusterResidentObject ProcessWorkDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>        
        public override DryadProcessIdentifier ProcessFromInputFile(IClusterResidentObject input, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>        
        public override ClusterStatus CreateClusterStatus()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>        
        public override IClusterResidentObject ProcessLogDirectory(DryadProcessIdentifier identifier, bool terminated, string machine, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>
        public override string JMLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>
        public override string VertexLogFilesPattern(bool error, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not used.
        /// </summary>
        /// <returns>Exception.</returns>
        public override string ExtractPidFromGuid(string guid, DryadLinqJobSummary job)
        {
            throw new NotImplementedException();
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
            var stat = ClusterStatus.LookupStatus(this);
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
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        public string JobsFolder { get; private set; }

        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        public LocalEmulator()
            : base(ClusterType.LocalEmulator)
        {
        }

        private static List<string> props = new List<string> {"JobsFolder"};

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
            var stat = ClusterStatus.LookupStatus(this);
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
    public abstract class DfsClusterConfiguration : ClusterConfiguration
    {
        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        protected DfsClusterConfiguration(ClusterType type)
            : base(type)
        {
            this.SupportsDiagnosis = false;
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
    }

    /// <summary>
    /// Configuration for an AzureDfs cluster.
    /// </summary>
    public sealed class AzureDfsClusterConfiguration : DfsClusterConfiguration
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
            this.Description = "Container is usually `dryad-jobs'";
        }

        /// <summary>
        /// Base Uri to access data in this Cluster.
        /// </summary>
        public Uri baseUri;

        /// <summary>
        /// Enumerate all the clusters this user is subscribed to.
        /// </summary>
        /// <returns>The list of clusters this user is subscribed to.</returns>
        public static List<ClusterConfiguration> EnumerateAzureDfsSubscribedClusters()
        {
            List<ClusterConfiguration> configList = new List<ClusterConfiguration>();

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
                                config.AzureClient = new AzureDfsClient(accountName, account.Key, config.Container);
                                config.Name = cluster.Name;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception while reconstructing cluster " + cluster.Name + ": " + ex);
                            }

                            if (config != null)
                                configList.Add(config);
                        }
                    }
                }
            }

            return configList;
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

        /// <summary>
        /// Azure account name.
        /// </summary>
        public string AccountName { get; set; }
        /// <summary>
        /// Azure account key.
        /// </summary>
        public string AccountKey { get; set; }
        /// <summary>
        /// Azure container.
        /// </summary>
        public string Container { get; set; }

        private static List<string> props = new List<string> 
        {
            "AccountName",
            "AccountKey",
            "Container"
        };

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
                this.baseUri = Microsoft.Research.Peloponnese.Azure.Utils.ToAzureUri(this.AccountName, this.Container, "", null, this.AccountKey);
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
            var stat = ClusterStatus.LookupStatus(this);
            if (stat != null) return stat;
            return new AzureDfsClusterStatus(this);
        }

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return props;
        }
    }

    /// <summary>
    /// Configuration for a WebHdfs cluster.
    /// </summary>
    public sealed class WebHdfsClusterConfiguration : DfsClusterConfiguration
    {
        /// <summary>
        /// Handle to client to access files.
        /// </summary>
        public HdfsClientBase DfsClient;

        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        public WebHdfsClusterConfiguration()
            : base(ClusterType.WebHdfs)
        {
            this.Description = "JobsFolderUri usually looks like hdfs://headnode:port/JobsFolder";
        }

        /// <summary>
        /// WebHdfs user name.
        /// </summary>
        public string UserName { get; set; }
        /// <summary>
        /// WebHdfs port.
        /// </summary>
        public int WebHdfsPort { get; set; }
        /// <summary>
        /// Uri to folder containing jobs.
        /// </summary>
        public Uri JobsFolderUri { get; set; }
        /// <summary>
        /// Machine that supplies job status.
        /// </summary>
        public string StatusNode { get; set; }
        /// <summary>
        /// Port of status machine.
        /// </summary>
        public int StatusNodePort { get; set; }

        private static List<string> props = new List<string> 
        {
            "UserName",
            "WebHdfsPort",
            "JobsFolderUri",
            "StatusNode",
            "StatusNodePort"
        };

        /// <summary>
        /// Must be called after setting all properties.
        /// Returns true if initialization succeeds.
        /// </summary>
        public override string Initialize()
        {
            try
            {
                this.DfsClient = new WebHdfsClient(this.UserName, this.WebHdfsPort);
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return ex.Message;
            }
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
                result.ShouldCacheLocally = true; // immutable
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
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
                var result = new DfsFile(this, this.JobsFolderUri, job, this.DfsClient, job.ClusterJobId, terminated, true);
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
            var stat = ClusterStatus.LookupStatus(this);
            if (stat != null) return stat;
            return new WebHdfsClusterStatus(this);
        }

        /// <summary>
        /// Properties that can be edited.
        /// </summary>
        /// <returns>List of properties that can be edited.</returns>
        public override List<string> GetPropertiesToEdit()
        {
            return props;
        }
    }

    /// <summary>
    /// Configuration for an Hdfs cluster.
    /// </summary>
    public sealed class HdfsClusterConfiguration : DfsClusterConfiguration
    {
        /// <summary>
        /// Handle to client to access files.
        /// </summary>
        public HdfsClientBase DfsClient;

        /// <summary>
        /// Create a cluster representing the local machine only.
        /// </summary>
        public HdfsClusterConfiguration()
            : base(ClusterType.Hdfs)
        {
            this.Description = "JobsFolderUri should look like hdfs://headnode:port/JobsFolder";
        }

        /// <summary>
        /// Path to cluster.
        /// </summary>
        public Uri JobsFolderUri { get; set; }
        /// <summary>
        /// Port to access HDFS.
        /// </summary>
        public string UserName { get; set; }
        /// <summary>
        /// Machine that supplies job status.
        /// </summary>
        public string StatusNode { get; set; }
        /// <summary>
        /// Port of status machine.
        /// </summary>
        public int StatusNodePort { get; set; }

        private static List<string> props = new List<string> 
        {
            "UserName",
            "JobsFolderUri",
            "StatusNode",
            "StatusNodePort"
        };

        /// <summary>
        /// Must be called after setting all properties.
        /// Returns true if initialization succeeds.
        /// </summary>
        public override string Initialize()
        {
            try
            {
                this.DfsClient = new HdfsClient(this.UserName);
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return ex.Message;
            }
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
                result.ShouldCacheLocally = true; // immutable
                return result;
            }
            catch (Exception e)
            {
                return new UNCFile(e);
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
                var result = new DfsFile(this, this.JobsFolderUri, job, this.DfsClient, job.ClusterJobId, terminated, true);
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
            var stat = ClusterStatus.LookupStatus(this);
            if (stat != null) return stat;
            var result = new HdfsClusterStatus(this);
            return result;
        }

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
