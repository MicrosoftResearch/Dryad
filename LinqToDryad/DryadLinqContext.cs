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
using System.Text;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.IO;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.Peloponnese.ClusterUtils;
using Microsoft.Research.Peloponnese.Storage;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// We currently support two schedulers.
    /// </summary>
    public enum ExecutorKind
    {
        DRYAD
    }

    /// <summary>
    /// The service platforms where you can run DryadLINQ.
    /// </summary>
    public enum PlatformKind
    {
        /// <summary>
        /// run on a YARN cluster (not yet implemented)
        /// </summary>
        YARN
    }

    /// <summary>
    /// Represents the context necessary to prepare and execute a DryadLinq Query,
    /// </summary>
    /// <remarks>
    /// <para>
    /// DryadLinqContext is the main entry point for the DryadLINQ framework.
    /// The context that is maintained by a DryadLinqContext instance includes
    /// configuration information.
    /// </para>
    /// <para>
    /// A DryadLinqContext may be reused by multiple queries and query executions.
    /// </para>
    /// <para>
    /// A DryadLinqContext may hold open connections to cluster services.
    /// To release these connections, call DryadLinqContext.Dispose().
    /// </para>
    /// </remarks>
    public class DryadLinqContext : IDisposable, IEquatable<DryadLinqContext>
    {
        private const int DscNameNodeDataPort = 6498;   //TODO: Read Config
        private const int HdfsNameNodeHttpPort = 8033;  //TODO: Read Config
        private const int HdfsNameNodeDataPort = 9000;  //TODO: Read Config

        private ExecutorKind _executorKind = ExecutorKind.DRYAD;
        private PlatformKind _platformKind = PlatformKind.YARN;

        private string _headNode;
        private string _dataNameNode;
        private DryadLinqQueryRuntime _runtime;
        private DscService _dscService;
        private IDfsClient _dfsClient;
        private ClusterClient _clusterClient;
        private int _dataNameNodeDataPort;
        private int _dataNameNodeHttpPort;

        private string _azureAccountName;
        private Dictionary<string, string> _azureAccountKeyDictionary;
        private string _azureContainerName;

        private Version _clientVersion;
        private Version _serverVersion;

        private CompressionScheme _intermediateDataCompressionScheme = CompressionScheme.None;
        private CompressionScheme _outputCompressionScheme = CompressionScheme.None;

        private bool _compileForVertexDebugging = false; // Ship PDBs + No optimization

        private string _jobFriendlyName;
        private int? _jobMinNodes;
        private int? _jobMaxNodes;
        private string _nodeGroup;
        private int? _jobRuntimeLimit;
        private bool _localDebug = false;
        private bool _localExecution = false;
        private string _jobUsername = null;
        private string _jobPassword = null;
        private QueryTraceLevel _runtimeTraceLevel = QueryTraceLevel.Error;
        private string _graphManagerNode;

        private bool _enableSpeculativeDuplication = true;
        private bool _selectOrderPreserving = false;

        private bool _matchClientNetFrameworkVersion = true;
        private bool _multiThreading = true;
        private string _partitionUncPath = null;
        private string _storageSetScheme = null;
        private Uri _tempDatasetDirectory = null;
        private DryadLinqStringDictionary _jobEnvironmentVariables = new DryadLinqStringDictionary();
        private DryadLinqStringList _resourcesToAdd = new DryadLinqStringList();
        private DryadLinqStringList _resourcesToRemove = new DryadLinqStringList();
        private bool _forceGC = false;
        private bool _isDisposed = false;

        private string _dryadHome;
        private string _peloponneseHome;

        /// <summary>
        /// Initializes a new instance of the DryadLinqContext class for local execution.
        /// </summary>
        /// <param name="numProcesses">The number of local worker processes that should be started.</param>
        public DryadLinqContext(int numProcesses, string storageSetScheme = null)
        {
            this._platformKind = PlatformKind.YARN;
            this._runtime = new DryadLinqQueryRuntime(this._headNode);
            this._localExecution = true;
            this._headNode = String.Empty;
            this._dataNameNode = null;
            this._storageSetScheme = storageSetScheme;
            if (String.IsNullOrEmpty(this._storageSetScheme))
            {
                this._storageSetScheme = DataPath.PARTFILE_URI_SCHEME;
            }
            DataProvider dataProvider = DataProvider.GetDataProvider(_storageSetScheme);
            this._tempDatasetDirectory = dataProvider.GetTempDirectory(this);
            this._jobMinNodes = numProcesses;
            this._dataNameNodeDataPort = HdfsNameNodeDataPort;
            this._dataNameNodeHttpPort = HdfsNameNodeHttpPort;
            CommonInit();
        }

        /// <summary>
        /// Initializes a new instance of the DryadLinqContext class for a YARN cluster.
        /// </summary>
        /// <param name="headNode">The head node of the cluster and DFS.</param>
        public DryadLinqContext(string headNode, PlatformKind platform = PlatformKind.YARN)
            : this(headNode, headNode, platform)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DryadLinqContext class for a YARN cluster.
        /// </summary>
        /// <param name="headNode">The head node of YARN cluster used to execute LINQ queries.</param>
        /// <param name="hdfsNameNode">The namenode for the HDFS.</param>
        /// <param name="platform">The cluster platform</param>
        public DryadLinqContext(string headNode, string hdfsNameNode, PlatformKind platform = PlatformKind.YARN)
        {
            // Verify that the head node is set
            if (String.IsNullOrEmpty(headNode))
            {
                throw new DryadLinqException(DryadLinqErrorCode.ClusterNameMustBeSpecified,
                                             SR.ClusterNameMustBeSpecified);
            }
            CommonInit();
            this._platformKind = platform;
            this._runtime = new DryadLinqQueryRuntime(headNode);
            this._headNode = headNode;
            this._dataNameNode = hdfsNameNode;
            this._dataNameNodeDataPort = HdfsNameNodeDataPort;
            this._dataNameNodeHttpPort = HdfsNameNodeHttpPort;
            this._storageSetScheme = DataPath.HDFS_URI_SCHEME;
            DataProvider dataProvider = DataProvider.GetDataProvider(_storageSetScheme);
            this._tempDatasetDirectory = dataProvider.GetTempDirectory(this);
            this._dfsClient = new Peloponnese.Storage.WebHdfsClient(hdfsNameNode, this._dataNameNodeDataPort, 50070);
            this._clusterClient = new Peloponnese.ClusterUtils.NativeYarnClient(
                                          hdfsNameNode, this._dataNameNodeDataPort);
        }

        /// <summary>
        /// Initializes a new instance of the DryadLinqContext class for Azure
        /// </summary>
        public DryadLinqContext(string accountName, string accountKey, string containerName,
                                string clusterName = null, string subscriptionId = null, string certificateThumbprint = null)
        {
            // Verify that the head node is set
            if (String.IsNullOrEmpty(containerName))
            {
                throw new DryadLinqException(DryadLinqErrorCode.ClusterNameMustBeSpecified,
                                             SR.ClusterNameMustBeSpecified);
            }
            CommonInit();
            this._platformKind = PlatformKind.YARN;
            this._runtime = new DryadLinqQueryRuntime(containerName);
            this._headNode = string.Empty;
            this._storageSetScheme = DataPath.AZUREBLOB_URI_SCHEME;
            this._azureAccountName = accountName;
            this._azureAccountKeyDictionary = new Dictionary<string, string>();
            this._azureAccountKeyDictionary.Add(this._azureAccountName, accountKey);
            this._azureContainerName = containerName;
            DataProvider dataProvider = DataProvider.GetDataProvider(_storageSetScheme);
            this._tempDatasetDirectory = dataProvider.GetTempDirectory(this);
            AzureDfsClient dfsClient = new Peloponnese.Storage.AzureDfsClient(accountName, accountKey, containerName);
            _dfsClient = dfsClient;
            _clusterClient = new Peloponnese.ClusterUtils.AzureYarnClient(
                                     dfsClient, this.PeloponneseHomeDirectory, clusterName,
                                     subscriptionId, certificateThumbprint);
        }

        private void CommonInit()
        {
            this._peloponneseHome = Peloponnese.ClusterUtils.ConfigHelpers.GetPPMHome(null);
            this._dryadHome = GetDryadHome();
        }

        private string GetDryadHome()
        {
            string dryadHome = Environment.GetEnvironmentVariable(StaticConfig.DryadHomeVar);

            if (dryadHome == null)
            {
                if (Microsoft.Research.Peloponnese.ClusterUtils.ConfigHelpers.RunningFromNugetPackage)
                {
                    dryadHome = Microsoft.Research.Peloponnese.ClusterUtils.ConfigHelpers.GetPPMHome(null);
                }
                else
                {
                    throw new ApplicationException("Cannot find Dryad home directory; must define " + StaticConfig.DryadHomeVar);
                }
            }

            return dryadHome;
        }

        public ExecutorKind ExecutorKind
        {
            get { return this._executorKind; }
            set { _executorKind = value; }
        }

        public PlatformKind PlatformKind
        {
            get { return this._platformKind; }
            set { _platformKind = value; }
        }

        /// <summary>
        /// Gets or sets the value specifying whether data passed between stages will be compressed.
        /// </summary>
        /// <remarks>
        /// The default is true.
        /// </remarks>
        public CompressionScheme IntermediateDataCompressionScheme
        {
            get { return this._intermediateDataCompressionScheme; }
            set { this._intermediateDataCompressionScheme = value; }
        }

        /// <summary>
        /// Gets or sets the value specifying the compression scheme for output data.
        /// </summary>
        /// <remarks>
        /// The default is <see cref="CompressionScheme.None"/>.
        /// </remarks>
        public CompressionScheme OutputDataCompressionScheme
        {
            get { return this._outputCompressionScheme; }
            set { this._outputCompressionScheme = value; }
        }

        /// <summary>
        /// Gets or sets the value specifying whether to compile code with debugging support.
        /// </summary>
        /// <remarks>
        /// If true, vertex code will be compiled with no code-level optimizations and a PDB will be generated.
        /// Also, the query execution job look for and include the PDB associated with every DLL resource 
        /// that is part of the submitted job.
        /// <para>The default is false.</para>
        /// </remarks>
        public bool CompileForVertexDebugging
        {
            get { return _compileForVertexDebugging; }
            set { _compileForVertexDebugging = value; }
        }

        /// <summary>
        /// Gets or sets the bin directory for Dryad.
        /// </summary>
        public string DryadHomeDirectory
        {
            get { return _dryadHome; }
            set { _dryadHome = value; }
        }

        /// <summary>
        /// Gets or sets the bin directory for Peloponnese.
        /// </summary>
        public string PeloponneseHomeDirectory
        {
            get { return _peloponneseHome; }
            set { _peloponneseHome = value; }
        }

        /// <summary>
        /// Gets or sets the head node for executing a DryadLinq query.
        /// </summary>
        public string HeadNode
        {
            get { return _headNode; }
            set { _headNode = value; }
        }

        /// <summary>
        /// Gets the DscService associated with this DryadLinqContext.
        /// </summary>
        public DscService DscService
        {
            get
            {
                ThrowIfDisposed();
                return _dscService;
            }
        }

        /// <summary>
        /// Gets the DfsClient associated with this HpcLinqContext.
        /// </summary>
        public IDfsClient DfsClient
        {
            get
            {
                ThrowIfDisposed();
                return _dfsClient;
            }
        }

        /// <summary>
        /// Gets the ClusterClient associated with this HpcLinqContext.
        /// </summary>
        public ClusterClient ClusterClient
        {
            get
            {
                ThrowIfDisposed();
                return _clusterClient;
            }
        }

        /// <summary>
        /// Gets or sets the namenode for the data store.
        /// </summary>
        public string DataNameNode
        {
            get { return _dataNameNode; }
            set { _dataNameNode = value; }
        }

        /// <summary>
        /// Gets or sets the HTTP port used by the namenode for the HDFS.
        /// </summary>
        public int DataNameNodeDataPort
        {
            get { return _dataNameNodeDataPort; }
            set { _dataNameNodeDataPort = value; }
        }

        /// <summary>
        /// Gets or sets the HTTP port used by the namenode for the HDFS.
        /// </summary>
        public int DataNameNodeHttpPort
        {
            get { return _dataNameNodeHttpPort; }
            set { _dataNameNodeHttpPort = value; }
        }

        /// <summary> 
        /// Gets or sets the account name for Azure. 
        /// </summary> 
        public string AzureAccountName
        {
            get { return _azureAccountName; }
            set { _azureAccountName = value; }
        }

        /// <summary> 
        /// Registers a key for an Azure account
        /// </summary> 
        public void RegisterAzureAccountKey(string accountName, string accountKey)
        {
            _azureAccountKeyDictionary[accountName] = accountKey;
        }

        /// <summary> 
        /// Retrieves the key for an azure account
        /// </summary> 
        public string AzureAccountKey(string accountName)
        {
            if (!_azureAccountKeyDictionary.ContainsKey(accountName))
            {
                return null;
            }
            return _azureAccountKeyDictionary[accountName];
        }

        /// <summary> 
        /// Gets or sets the container name for Azure. 
        /// </summary> 
        public string AzureContainerName
        {
            get { return _azureContainerName; }
            set { _azureContainerName = value; }
        }

        /// <summary>
        /// Gets or sets the partition UNC path used when constructing a partitioned table.
        /// </summary>
        public string PartitionUncPath
        {
            get { return _partitionUncPath; }
            set { _partitionUncPath = value; }
        }

        /// <summary>
        /// Gets the collection of environment variables associated with the DryadLINQ job.
        /// </summary>
        public IDictionary<string, string> JobEnvironmentVariables
        {
            get { return _jobEnvironmentVariables; }
        }

        /// <summary>
        /// Gets or sets the descriptive name used to describe the DryadLINQ job.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no name). May be overriden by cluster settings such as node templates.</para>
        /// <para>This property can be altered even when <see cref="IsReadOnly"/> is true.</para>
        /// </remarks>
        public string JobFriendlyName
        {
            get { return _jobFriendlyName; }
            set { _jobFriendlyName = value; }
        }

        /// <summary>
        /// Gets or sets the minimum number of cluster nodes for the DryadLINQ job.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no lower limit). May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public int? JobMinNodes
        {
            get { return _jobMinNodes; }
            set { _jobMinNodes = value; }
        }

        /// <summary>
        /// Gets or sets the maximum number of cluster nodes for the DryadLINQ job.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no upper limit). May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public int? JobMaxNodes
        {
            get { return _jobMaxNodes; }
            set { _jobMaxNodes = value; }
        }

        /// <summary>
        /// Gets or sets the name of the compute node group when running on the cluster.
        /// </summary>
        /// <remarks>
        /// Creation and management of nodes groups is performed using the Cluster Manager.
        /// </remarks>
        /// <remarks>
        /// <para>The default is null (no node group restriction).</para>
        /// </remarks>
        public string NodeGroup
        {
            get { return _nodeGroup; }
            set { _nodeGroup = value; }
        }

        /// <summary>
        /// Gets or sets the maximum execution time for the DryadLINQ job, in seconds.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no runtime limit).</para>
        /// </remarks>
        public int? JobRuntimeLimit
        {
            get { return _jobRuntimeLimit; }
            set { _jobRuntimeLimit = value; }
        }

        /// <summary>
        /// Enables or disables speculative duplication of vertices based on runtime performance analysis. 
        /// </summary>
        /// <remarks>
        /// <para>The default is true.</para>
        /// </remarks>
        public bool EnableSpeculativeDuplication
        {
            get { return _enableSpeculativeDuplication; }
            set { _enableSpeculativeDuplication = value; }
        }

        /// <summary>
        /// Gets or sets the value specifying whether to use Local debugging mode.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If true, the DryadLINQ query will execute in the current CLR via LINQ-to-Objects.
        /// This mode is particularly useful for debugging user-functions before attempting cluster execution.
        /// LocalDebug mode accesses input and output data as usual.
        /// </para>
        /// <para>
        /// LocalDebug mode does not perform vertex-code compilation.
        /// </para>
        /// <para>The default is false.</para>
        /// </remarks>
        public bool LocalDebug
        {
            get { return _localDebug; }
            set { _localDebug = value; }
        }

        /// <summary>
        /// Gets or sets the value specifying whether to use Local execution mode.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If true, the DryadLINQ Query will execute by forking processes on the local
        /// computer instead of using a cluster. LocalExecution mode accesses HDFS as usual for
        /// input and output data.
        /// </para>
        /// <para>The default is false.</para>
        /// </remarks>
        public bool LocalExecution
        {
            get { return _localExecution; }
            set { _localExecution = value; }
        }

        public bool DebugBreak
        {
            get { return this.JobEnvironmentVariables.ContainsKey("DLINQ_DEBUGVERTEX"); }
            set
            {
                this.JobEnvironmentVariables["DLINQ_DEBUGVERTEX"] = "BREAK";
            }
        }

        /// <summary>
        /// Gets or sets the value specifying the platform for this query.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If YARN, the query will execute on a YARN cluster.
        /// </para>
        /// <para>
        /// LOCAL mode is determined by the flag _localExecution, and is mutually exclusive 
        /// with LocalDebug. If LOCAL, the query will execute with DryadLinq on processes 
        /// spawned on the local machine. This mode is particularly useful for debugging
        /// interactions between processes.
        /// </para>
        /// <para>The default is YARN.</para>
        /// </remarks>
        public PlatformKind Platform
        {
            get { return _platformKind; }
            set { _platformKind = value; }
        }

        /// <summary>
        /// Get the list of resources to add to the DryadLINQ job.
        /// </summary>
        /// <remarks>
        /// <para>
        /// During query submission, some resources will be detected and added automatically.
        /// It is only necessary to add resources that are not detected automatically.
        /// </para>
        /// <para>
        /// Each resource should be a complete path to a file-based resource accessible
        /// from the local machine.
        /// </para>
        /// </remarks>
        public IList<string> ResourcesToAdd
        {
            get { return _resourcesToAdd; }
        }

        /// <summary>
        /// Get the list of resources to be excluded from the DryadLINQ job.
        /// </summary>
        /// <remarks>
        /// <para>
        /// During query submission, some resources will be detected and added automatically.  
        /// Remove resources that are detected automatically but that are not required for job execution.
        /// </para>
        /// <para>
        /// Each resource should be a complete path to a file-based resource accessible from the local machine.
        /// </para>
        /// </remarks>
        public IList<string> ResourcesToRemove
        {
            get { return _resourcesToRemove; }
        }

        /// <summary>
        /// Gets or sets the RunAs password for jobs submitted to the cluster.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (use the credentials of the current Thread)</para>
        /// </remarks>
        public string JobUsername
        {
            get { return _jobUsername; }
            set { _jobUsername = value; }
        }

        /// <summary>
        /// Gets or sets the RunAs password for jobs submitted to the cluster.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (use the credentials of the current Thread)</para>
        /// </remarks>
        public string JobPassword
        {
            get { return _jobPassword; }
            set { _jobPassword = value; }
        }

        /// <summary>
        /// Gets or sets the trace level to use for DryadLINQ Query jobs.
        /// </summary>
        /// <remarks>
        /// <para>The RuntimeTraceLevel affects the logs produced by all components associated with the execution
        /// of a DryadLINQ Query job.
        /// </para>
        /// <para>The default is QueryTraceLevel.Error</para>
        /// </remarks>
        public QueryTraceLevel RuntimeTraceLevel
        {
            get { return _runtimeTraceLevel; }
            set { _runtimeTraceLevel = value; }
        }

        /// <summary>
        /// Gets or sets the node that should be used for running the Dryad Graph Manager task.
        /// </summary>
        /// <remarks>
        /// If null, the Graph Manager task will run on an arbitrary machine that is allocated to the DryadLINQ job.
        /// </remarks>
        public string GraphManagerNode
        {
            get { return _graphManagerNode; }
            set { _graphManagerNode = value; }
        }

        /// <summary>
        /// Gets or sets whether certain operators will preserve item ordering.
        /// When true, the Select, SelectMany and Where operators will preserve item ordering;
        /// otherwise, they may shuffle the input items as they are processed.
        /// </summary>
        public bool SelectOrderPreserving
        {
            get { return _selectOrderPreserving; }
            set { _selectOrderPreserving = value; }
        }

        /// <summary>
        /// Configures query jobs to be launched on the cluster nodes against a .NET framework version 
        /// matching that of the client process. This should only be set if all cluster nodes are known to have
        /// the same .NET version as the client. 
        /// When set to false (default), the vertex code will be compiled and run against .NET Framework 3.5.
        /// </summary>
        public bool MatchClientNetFrameworkVersion
        {
            get { return _matchClientNetFrameworkVersion; }
            set { _matchClientNetFrameworkVersion = value; }
        }

        /// <summary>
        /// Gets or sets whether user-defined methods and custom serializers may be called on
        /// multiple threads of a single process.
        /// </summary>
        /// <remarks>
        /// This option affects the internal behavior of individual queries and applies to both the
        /// client process (for serialization and local-debug mode) and to vertex processes.
        /// This option does not have any serializing effect for queries that are submitted
        /// concurrently by one or more client processes.
        /// If true, user-defined methods may be called concurrently.
        /// If false, user-defined methods will be called without concurrency.  
        /// </remarks>
        public bool EnableMultiThreadingInVertex
        {
            get { return _multiThreading; }
            set { _multiThreading = value; }
        }

        /// <summary>
        /// Gets or sets whether to run GC after Moxie runs each task.
        /// </summary>
        /// <remarks>
        /// This only works with Moxie (for now at least).
        /// </remarks>
        public bool ForceGC
        {
            get { return _forceGC; }
            set { _forceGC = value; }
        }

        // internal: the runtime associated with this DryadLinqContext.
        internal DryadLinqQueryRuntime Runtime
        {
            get
            {
                ThrowIfDisposed();
                return _runtime;
            }
        }

        /// <summary>
        /// Version of the DryadLinq client components
        /// </summary>
        public Version ClientVersion()
        {
            ThrowIfDisposed();
            if (_clientVersion == null)
            {
                try
                {
                    Assembly asm = Assembly.GetExecutingAssembly();
                    _clientVersion = new Version(FileVersionInfo.GetVersionInfo(asm.Location).FileVersion);
                }
                catch (Exception ex)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CouldNotGetClientVersion,
                                                 SR.CouldNotGetClientVersion, ex);
                }
            }
            return _clientVersion;
        }

        /// <summary>
        /// Version of the DryadLinq server components
        /// </summary>
        public Version ServerVersion()
        {
            ThrowIfDisposed();
            if (_serverVersion == null)
            {
                try
                {
                    IServerVersion version = this.GetIScheduler().GetServerVersion();
                    _serverVersion = new Version(version.Major, version.Minor, version.Build, version.Revision);
                }
                catch (Exception ex)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CouldNotGetServerVersion,
                                                 SR.CouldNotGetServerVersion, ex);
                }
            }
            return _serverVersion;
        }

        internal DryadLinqJobExecutor MakeJobExecutor()
        {
            switch (this.ExecutorKind)
            {
                case ExecutorKind.DRYAD:
                    {
                        return new DryadLinqJobExecutor(this);
                    }
                default:
                    {
                        throw new Exception("No implementation for scheduler: " + this.ExecutorKind.ToString());
                    }
            }
        }

        public Uri MakeTemporaryStreamUri()
        {
            if (this._storageSetScheme == null)
            {
                throw new DryadLinqException("The storage scheme for temporary streams must be specified.");
            }
            return new Uri(this._tempDatasetDirectory, DryadLinqUtil.MakeUniqueName());
        }

        /// <summary>
        /// Open a dataset as a DryadLinq's IQueryable<T>.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="fileSetName">The name of the dataset. </param>
        /// <returns>An IQueryable{T} representing the data.</returns>
        public IQueryable<T> FromStore<T>(string dataSetName)
        {
            return FromStore<T>(new Uri(dataSetName));
        }

        /// <summary>
        /// Open a dataset as a DryadLinq's IQueryable<T>.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="fileSetName">The name of the dataset. </param>
        /// <returns>An IQueryable{T} representing the data.</returns>
        public IQueryable<T> FromStore<T>(Uri dataSetName)
        {
            ThrowIfDisposed();
            DryadLinqQuery<T> q = DataProvider.GetPartitionedTable<T>(this, dataSetName);
            q.CheckAndInitialize();   // force the data-info checks.
            return q;
        }

        /// <summary>
        /// Converts an IEnumerable{T} to a DryadLinq IQueryable{T}.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="data">The source data.</param>
        /// <returns>An IQueryable{T} representing the data with DryadLinq query provider.</returns>
        /// <remarks>
        /// The source data will be serialized to a temp stream.
        /// The resulting fileset has an auto-generated name and a temporary lease.
        /// </remarks>
        public IQueryable<T> FromEnumerable<T>(IEnumerable<T> data)
        {
            Uri dataSetName = this.MakeTemporaryStreamUri();
            CompressionScheme compressionScheme = this.IntermediateDataCompressionScheme;
            DryadLinqMetaData metadata = new DryadLinqMetaData(this, typeof(T), dataSetName, compressionScheme);
            return DataProvider.StoreData(this, data, dataSetName, metadata, compressionScheme, true);
        }

        internal static DryadLinqContext GetContext(IQueryProvider provider)
        {
            DryadLinqProviderBase baseProvider = provider as DryadLinqProviderBase;
            if (baseProvider == null)
            {
                throw new DryadLinqException("Must be DryadLINQ query provider.");
            }
            DryadLinqContext context = baseProvider.Context;
            context.ThrowIfDisposed();
            return context;
        }

        // Return IScheduler reference for internal use
        internal IScheduler GetIScheduler()
        {
            return this._runtime.GetIScheduler();
        }

        /// <summary>
        /// Releases all resources used by the DryadLinqContext.
        /// </summary>
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                if (_runtime != null)
                {
                    _runtime.Dispose();
                    _runtime = null;
                }
                if (_dscService != null)
                {
                    _dscService.Close();
                    _dscService = null;
                }
            }
        }

        internal void ThrowIfDisposed()
        {
            if (this._isDisposed)
            {
                throw new DryadLinqException(DryadLinqErrorCode.ContextDisposed, SR.ContextDisposed);
            }
        }

        // This is used to check if a DryadLINQ query is constructed using the same context. 
        public virtual bool Equals(DryadLinqContext context)
        {
            return (this.IntermediateDataCompressionScheme == context.IntermediateDataCompressionScheme &&
                    this.OutputDataCompressionScheme == context.OutputDataCompressionScheme &&
                    this.CompileForVertexDebugging == context.CompileForVertexDebugging &&
                    this.DryadHomeDirectory == context.DryadHomeDirectory &&
                    this.PeloponneseHomeDirectory == context.PeloponneseHomeDirectory &&
                    this.HeadNode == context.HeadNode &&
                    this.DataNameNode == context.DataNameNode &&
                    this.DataNameNodeDataPort == context.DataNameNodeDataPort &&
                    this.DataNameNodeHttpPort == context.DataNameNodeHttpPort &&
                    this.AzureAccountName == context.AzureAccountName &&
                    this.AzureContainerName == context.AzureContainerName &&
                    this.PartitionUncPath == context.PartitionUncPath &&
                    this.JobMinNodes == context.JobMinNodes &&
                    this.JobMaxNodes == context.JobMaxNodes &&
                    this.NodeGroup == context.NodeGroup &&
                    this.JobRuntimeLimit == context.JobRuntimeLimit &&
                    this.EnableSpeculativeDuplication == context.EnableSpeculativeDuplication &&
                    this.LocalDebug == context.LocalDebug &&
                    this.LocalExecution == context.LocalExecution &&
                    this.Platform == context.Platform &&
                    this.JobUsername == context.JobUsername &&
                    this.JobPassword == context.JobPassword &&
                    this.RuntimeTraceLevel == context.RuntimeTraceLevel &&
                    this.GraphManagerNode == context.GraphManagerNode &&
                    this.SelectOrderPreserving == context.SelectOrderPreserving &&
                    this.MatchClientNetFrameworkVersion == context.MatchClientNetFrameworkVersion &&
                    this.EnableMultiThreadingInVertex == context.EnableMultiThreadingInVertex &&
                    this.ForceGC == context.ForceGC);
        }
    }
}
