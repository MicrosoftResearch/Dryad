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
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
  
[assembly: InternalsVisibleTo("DistributedDandelion")]

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Configuration information for a HPC Query.
    /// </summary>
    public sealed class HpcLinqConfiguration
    {
        internal bool _isReadOnly; 

        private HpcLinqStringList _resourcesToAdd = new HpcLinqStringList();
        private HpcLinqStringList _resourcesToRemove = new HpcLinqStringList();

        private DscCompressionScheme _intermediateDataCompressionScheme = DscCompressionScheme.Gzip;
        private DscCompressionScheme _outputCompressionScheme = DscCompressionScheme.None;
        private bool _compileForVertexDebugging = false; // Ship PDBs + No optimization
        private string _headNode;
        private string _hdfsNameNode;
        private int _hdfsNameNodeHttpPort = 8033;  //TODO - Read Config
        private string _jobFriendlyName;
        private int? _jobMinNodes;
        private int? _jobMaxNodes;
        private string _nodeGroup;
        private int? _jobRuntimeLimit;
        private bool _localDebug = false;
        private bool _orderPreserving = true;
        private string _jobUsername = null;
        private string _jobPassword = null;
        private HpcQueryTraceLevel _runtimeTraceLevel = HpcQueryTraceLevel.Error;
        private string _graphManagerNode;
        private bool _enableSpeculativeDuplication = false;
        private HpcLinqStringDictionary _jobEnvironmentVariables = new HpcLinqStringDictionary();
        private bool _selectAndWherePreserveOrder = false;
        private bool _matchClientNetFrameworkVersion = false;
        private bool _multiThreading = true;

         //Set these values using YARN_HOME and DRYAD_HOME environment variables
        private string _yarnHome = Environment.GetEnvironmentVariable("YARN_HOME");
        private string _dryadHome = Environment.GetEnvironmentVariable("DRYAD_HOME");

        private void ThrowIfReadOnly()
        {
            if (_isReadOnly)
            {
                throw new NotSupportedException(SR.ConfigReadonly);
            }
        }

        /// <summary>
        /// Gets the value indicating whether the HpcLinqConfiguration is read-only.
        /// </summary>
        /// <remarks>
        /// When <see cref="IsReadOnly"/> is true, every property except JobFriendlyName will throw a <see cref="System.NotSupportedException"/>
        /// from its setter.
        /// </remarks>
        public bool IsReadOnly
        {
            get { return _isReadOnly; }
        }

        /// <summary>
        /// Gets or sets the value specifying whether data passed between stages in a HPC Query will be compressed.
        /// </summary>
        /// <remarks>
        /// The default is true.
        /// </remarks>
        public DscCompressionScheme IntermediateDataCompressionScheme
        {
            get { return _intermediateDataCompressionScheme; }
            set
            { 
                ThrowIfReadOnly(); 
                _intermediateDataCompressionScheme = value; 
            }
        }

        /// <summary>
        /// Gets or sets the value specifying the compression scheme for output data.
        /// </summary>
        /// <remarks>
        /// The default is <see cref="DscCompressionScheme.None"/>.
        /// </remarks>
        public DscCompressionScheme OutputDataCompressionScheme
        {
            get { return _outputCompressionScheme; }
            set
            {
                ThrowIfReadOnly();
                _outputCompressionScheme = value;
            }
        }

        /// <summary>
        /// Gets or sets the value specifying whether to compile code that support debugging vertex tasks that execute on a HPC Server cluster.
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
            set 
            {
                ThrowIfReadOnly(); 
                _compileForVertexDebugging = value; 
            }
        }

        /// <summary>
        /// Gets or sets the bin directory for Dryad.
        /// </summary>
        public string DryadHomeDirectory
        {
            get { return _dryadHome; }
            set
            {
                ThrowIfReadOnly();
                _dryadHome = value;
            }
        }

        /// <summary>
        /// Gets or sets the home directory for Yarn.
        /// </summary>
        public string YarnHomeDirectory
        {
            get { return _yarnHome; }
            set
            {
                ThrowIfReadOnly();
                _yarnHome = value;
            }
        }

        /// <summary>
        /// Gets or sets the head node for the HPC Server used to execute the HPC Query job.
        /// </summary>
        public string HeadNode
        {
            get { return _headNode; }
            set
            { 
                ThrowIfReadOnly();
                _headNode = value; 
            }
        }

        /// <summary>
        /// Gets or sets the namenode for the HDFS.
        /// </summary>
        public string HdfsNameNode
        {
            get { return _hdfsNameNode; }
            set
            { 
                ThrowIfReadOnly();
                _hdfsNameNode = value; 
            }
        }

        /// <summary>
        /// Gets or sets the HTTP port used by the namenode for the HDFS.
        /// </summary>
        public int HdfsNameNodeHttpPort
        {
            get { return _hdfsNameNodeHttpPort; }
            set
            {
                ThrowIfReadOnly();
                _hdfsNameNodeHttpPort = value;
            }
        }

        /// <summary>
        /// Gets the collection of environment variables associated with the HPC Query job.
        /// </summary>
        public IDictionary<string, string> JobEnvironmentVariables
        {
            get { return _jobEnvironmentVariables; }
        }

        /// <summary>
        /// Gets or sets the descriptive name used to describe the HPC Query job.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no name). May be overriden by cluster settings such as node templates.</para>
        /// <para>This property can be altered even when <see cref="IsReadOnly"/> is true.</para>
        /// </remarks>
        public string JobFriendlyName
        {
            get { return _jobFriendlyName; }
            set
            {
                _jobFriendlyName = value;
            }
        }

        /// <summary>
        /// Gets or sets the minimum number of cluster nodes that the HPC Server job will use.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no lower limit). May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public int? JobMinNodes
        {
            get { return _jobMinNodes; }
            set 
            {
                ThrowIfReadOnly();
                _jobMinNodes = value; 
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of cluster nodes that the HPC Server job will use.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no upper limit). May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public int? JobMaxNodes
        {
            get { return _jobMaxNodes; }
            set
            {
                ThrowIfReadOnly(); 
                _jobMaxNodes = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the compute node group that the HPC Server job will use.
        /// </summary>
        /// <remarks>
        /// Creation and management of nodes groups is performed using the HPC Cluster Manager.
        /// </remarks>
        /// <remarks>
        /// <para>The default is null (no node group restriction). May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public string NodeGroup
        {
            get { return _nodeGroup; }
            set 
            {
                ThrowIfReadOnly(); 
                _nodeGroup = value;
            }
        }


        /// <summary>
        /// Gets or sets the maximum execution time for the HPC Query job, in seconds.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (no runtime limit).  May be overriden by cluster settings such as node templates.</para>
        /// </remarks>
        public int? JobRuntimeLimit
        {
            get { return _jobRuntimeLimit; }
            set 
            {
                ThrowIfReadOnly(); 
                _jobRuntimeLimit = value;
            }
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
            set
            {
                ThrowIfReadOnly();
                _enableSpeculativeDuplication = value;
            }
        }

        /// <summary>
        /// Gets or sets the value specifying whether to use Local debugging mode.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If true, the HPC Query will execute in the current AppDomain via LINQ-to-Objects.
        /// This mode is particularly useful for debugging user-functions before attempting cluster execution.
        /// LocalDebug mode accesses DSC as usual for input and output data.
        /// </para>
        /// <para>
        /// LocalDebug mode does not perform vertex-code compilation, nor is a job submitted to HPC Server.  
        /// </para>
        /// <para>The default is false.</para>
        /// </remarks>
        public bool LocalDebug
        {
            get { return _localDebug; }
            set 
            {
                ThrowIfReadOnly(); 
                _localDebug = value;
            }
        }

        /// <summary>
        /// Get the list of resources to add to the HPC job used to execute a HPC Query.
        /// </summary>
        /// <remarks>
        /// <para>
        /// During query submission, some resources will be detected and added automatically.  It is only necessary
        /// to add resources that are not detected automatically.
        /// </para>
        /// <para>
        /// Each resource should be a complete path to a file-based resource accessible from the local machine.
        /// </para>
        /// </remarks>
        public IList<string> ResourcesToAdd
        {
            get { return _resourcesToAdd; }
        }

        /// <summary>
        /// Get the list of resources to remove from the HPC job used to execute a HPC Query.
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
        /// Gets or sets the RunAs password for jobs submitted to HPC Server.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (use the credentials of the current Thread)</para>
        /// </remarks>
        public string JobUsername
        {
            get { return _jobUsername; }
            set 
            {
                ThrowIfReadOnly(); 
                _jobUsername = value;
            }
        }

        /// <summary>
        /// Gets or sets the RunAs password for jobs submitted to HPC Server.
        /// </summary>
        /// <remarks>
        /// <para>The default is null (use the credentials of the current Thread)</para>
        /// </remarks>
        public string JobPassword
        {
            get { return _jobPassword; }
            set 
            {
                ThrowIfReadOnly(); 
                _jobPassword = value;
            }
        }

        /// <summary>
        /// Gets or sets the trace level to use for HPC Query jobs.
        /// </summary>
        /// <remarks>
        /// <para>The RuntimeTraceLevel affects the logs produced by all components associated with the execution
        /// of a HPC Query job.
        /// </para>
        /// <para>The default is HpcQueryTraceLevel.Error</para>
        /// </remarks>
        public HpcQueryTraceLevel RuntimeTraceLevel
        {
            get { return _runtimeTraceLevel; }
            set 
            {
                ThrowIfReadOnly(); 
                _runtimeTraceLevel = value;
            }
        }

#if YARN_MISSING_FEATURE
        /// <summary>
        /// Gets or sets the node that should be used for running the HPC Query Graph Manager task.
        /// </summary>
        /// <remarks>
        /// If null, the Graph Manager task will run on an arbitrary machine that is allocated to the HPC Query job.
        /// </remarks>
        public string GraphManagerNode
        {
            get { return _graphManagerNode; }
            set 
            {
                ThrowIfReadOnly(); 
                _graphManagerNode = value;
            }
        }
#endif

        /// <summary>
        /// Gets or sets whether certain operators will preserve item ordering.
        /// When true, the Select, SelectMany and Where operators will preserve item ordering;
        /// otherwise, they may shuffle the input items as they are processed.
        /// </summary>
        public bool SelectiveOrderPreservation
        {
            get { return _selectAndWherePreserveOrder; }
            set
            {
                ThrowIfReadOnly();
                _selectAndWherePreserveOrder = value;
            }
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
            set
            {
                ThrowIfReadOnly();
                _matchClientNetFrameworkVersion = value;
            }
        }

        /// <summary>
        /// Gets or sets whether user-defined methods and custom serializers may be called on multiple threads of a single process.
        /// </summary>
        /// <remarks>
        /// This option affects the internal behavior of individual queries and applies to both the client process (for serialization and local-debug mode) 
        /// and to vertex processes.
        /// This option does not have any serializing effect for queries that are submitted concurrently by one or more client processes.
        /// If true, user-defined methods may be called concurrently.
        /// If false, user-defined methods will be called without concurrency.  
        /// </remarks>
        public bool AllowConcurrentUserDelegatesInSingleProcess
        {
            get { return _multiThreading; }
            set
            {
                ThrowIfReadOnly();
                _multiThreading = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the HpcLinqConfiguration class.
        /// </summary>
        public HpcLinqConfiguration()
        {
            CommonInit();
        }

        /// <summary>
        /// Initializes a new instance of the HpcLinqConfiguration class.
        /// </summary>
        /// <param name="headNode">The head node for the HPC Server used to execute the HPC Query job.</param>
        public HpcLinqConfiguration(string headNode)
        {
            _headNode = headNode;
            _hdfsNameNode = headNode; //default
            CommonInit();
        }

        /// <summary>
        /// Initializes a new instance of the HpcLinqConfiguration class.
        /// </summary>
        /// <param name="headNode">The head node for the HPC Server used to execute the HPC Query job.</param>
        /// <param name="hdfsNameNode">The namenode for the HDFS.</param>
        public HpcLinqConfiguration(string headNode, string hdfsNameNode)
        {
            _headNode = headNode;
            _hdfsNameNode = hdfsNameNode;
            CommonInit();
        }

        private void CommonInit()
        {
            _yarnHome = Environment.GetEnvironmentVariable("YARN_HOME");
            _dryadHome = Environment.GetEnvironmentVariable("DRYAD_HOME");
        }

        internal HpcLinqConfiguration MakeImmutableCopy()
        {
            HpcLinqConfiguration newConfig = new HpcLinqConfiguration();

            newConfig._isReadOnly = true;

            newConfig._jobEnvironmentVariables = this._jobEnvironmentVariables.GetImmutableClone();

            newConfig._resourcesToAdd = this._resourcesToAdd.GetImmutableClone();
            newConfig._resourcesToRemove = this._resourcesToRemove.GetImmutableClone();

            newConfig._intermediateDataCompressionScheme = this._intermediateDataCompressionScheme;
            newConfig._outputCompressionScheme = this._outputCompressionScheme;
            newConfig._compileForVertexDebugging = this._compileForVertexDebugging;
            newConfig._headNode = this._headNode;
            newConfig._hdfsNameNode = this._hdfsNameNode;
            newConfig._hdfsNameNodeHttpPort = this._hdfsNameNodeHttpPort;
            newConfig._jobFriendlyName = this._jobFriendlyName;
            newConfig._jobMinNodes = this._jobMinNodes;
            newConfig._jobMaxNodes = this._jobMaxNodes;
            newConfig._nodeGroup = this._nodeGroup;
            newConfig._jobRuntimeLimit = this._jobRuntimeLimit;
            newConfig._localDebug = this._localDebug;
            newConfig._orderPreserving = this._orderPreserving;
            newConfig._jobUsername = this._jobUsername;
            newConfig._jobPassword = this.JobPassword;
            newConfig._runtimeTraceLevel = this._runtimeTraceLevel;
            newConfig._graphManagerNode = this._graphManagerNode;
            newConfig._selectAndWherePreserveOrder = this._selectAndWherePreserveOrder;
            newConfig._matchClientNetFrameworkVersion = this._matchClientNetFrameworkVersion;
            newConfig._enableSpeculativeDuplication = this._enableSpeculativeDuplication;
            newConfig._multiThreading = this._multiThreading;

            newConfig._dryadHome = this._dryadHome;
            newConfig._yarnHome = this._yarnHome;
            
            return newConfig;
        }
    }
}
