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

namespace Microsoft.Research.DryadLinq
{
    

    /// <summary>
    /// Represents the context necessary to prepare and execute a HPC LINQ Query,
    /// </summary>
    /// <remarks>
    /// <para>
    /// HpcLinqContext is the main entry point for the HPC LINQ framework.  The context
    /// that is maintained by a HpcLinqContext instance includes configuration information, a connection to a DSC Service 
    /// that can be used during execution, and a connection to a HPC Server.
    /// </para>
    /// <para>
    /// A HpcLinqContext may be reused by multiple queries and query executions.
    /// </para>
    /// <para>
    /// A HpcLinqContext may hold open connections to DSC and a HPC Server.  To release these connections, call
    /// HpcLinqContext.Dispose()
    /// </para>
    /// </remarks>
    public sealed class HpcLinqContext : IDisposable
    {
        private HpcLinqConfiguration _configuration;
        private HpcQueryRuntime _runtime;
        private DscService _dscService;
        private string _hdfsServiceNode;
        private Version _clientVersion;
        private Version _serverVersion;

        /// <summary>
        /// Gets the configuration associated with this HpcLinqContext.
        /// </summary>
        /// <remarks>
        /// The Configuration object returns will be read-only.
        /// </remarks>
        public HpcLinqConfiguration Configuration 
        { 
            get {
                ThrowIfDisposed();
                return _configuration; 
            } 
        }
        
        /// <summary>
        /// Gets the DscService associated with this HpcLinqContext.
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
        /// Gets the HdfsService associated with this HpcLinqContext.
        /// </summary>
        public string HdfsService
        {
            get
            {
                ThrowIfDisposed();
                return _hdfsServiceNode;
            }
        }
        
        // internal: the runtime associated with this HpcLinqContext.
        internal HpcQueryRuntime Runtime { 
            get 
            {
                ThrowIfDisposed();
                return _runtime; 
            }
        }

       

        /// <summary>
        /// Version of the HpcLinq client components
        /// </summary>
        public Version ClientVersion { 
            get {
                ThrowIfDisposed();
                if (_clientVersion == null)
                {
                    LoadClientVersion(); // thread-safe
                }
                return _clientVersion; 
            } 
        }

        /// <summary>
        /// Version of the HpcLinq server components
        /// </summary>
        public Version ServerVersion { 
            get 
            {
                ThrowIfDisposed();
                if (_serverVersion == null)
                {
                    LoadServerVersion(); // thread-safe
                }
                return _serverVersion; 
            } 
        }

        /// <summary>
        /// Initializes a new instance of the HpcLinqConfiguration class.
        /// </summary>
        /// <param name="configuration">Configuration information.</param>
        /// <remarks>
        /// Connections will be opened to DSC and HPC Server using configuration.HeadNode.
        /// The connections will be opened regardless of whether DSC is used and/or whether 
        /// configuration.LocalDebug is true
        /// </remarks>
        public HpcLinqContext(HpcLinqConfiguration configuration)
        {
            // Verify that the head node is set
            if (configuration.HeadNode == null)
            {
                throw new DryadLinqException(HpcLinqErrorCode.ClusterNameMustBeSpecified,
                                           SR.ClusterNameMustBeSpecified);
            }
            
            _configuration = configuration.MakeImmutableCopy();
            _runtime = new HpcQueryRuntime(_configuration.HeadNode);
            _dscService = new DscService(_configuration.HeadNode);
            _hdfsServiceNode = _configuration.HdfsNameNode;
        }

        private void LoadClientVersion()
        {
            try
            {
                Assembly asm = Assembly.GetExecutingAssembly();
                _clientVersion = new Version(FileVersionInfo.GetVersionInfo(asm.Location).FileVersion);
            }
            catch (Exception ex)
            {
                throw new DryadLinqException(HpcLinqErrorCode.CouldNotGetClientVersion,
                                           SR.CouldNotGetClientVersion, ex);
            }
        }

        private void LoadServerVersion()
        {
            try
            {
                IServerVersion version = this.GetIScheduler().GetServerVersion();
                _serverVersion = new Version(version.Major, version.Minor, version.Build, version.Revision);
            }
            catch (Exception ex)
            {
                throw new DryadLinqException(HpcLinqErrorCode.CouldNotGetServerVersion,
                                           SR.CouldNotGetServerVersion, ex);
            }
        }

        /// <summary>
        /// Open a DSC fileset as a LINQ-to-HPC IQueryable{T}.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="dscFileSetName">The name of the DSC fileset. </param>
        /// <returns>An IQueryable{T} representing the data and associated with the HPC LINQ query provider.</returns>
        public IQueryable<T> FromDsc<T>(string fileSetName)
        {
            ThrowIfDisposed();

            string fullPath = DataPath.MakeDscStreamUri(_dscService, fileSetName);

            try {
                DscFileSet fs = _dscService.GetFileSet(fileSetName);
                if (!fs.IsSealed())
                {
                    throw new DryadLinqException(HpcLinqErrorCode.FileSetMustBeSealed,
                                               SR.FileSetMustBeSealed);
                }

                int fileCount = fs.GetFiles().Count();
                if (fileCount < 1)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.FileSetMustHaveAtLeastOneFile,
                                               SR.FileSetMustHaveAtLeastOneFile);
                }

            }
            catch (DscException dscEx){
                throw new DryadLinqException(HpcLinqErrorCode.FileSetCouldNotBeOpened,
                                           SR.FileSetCouldNotBeOpened, dscEx);
            }

            DryadLinqQuery<T> q = DataProvider.GetPartitionedTable<T>(this, fullPath);
            q.CheckAndInitialize(); // force the data-info checks.
            return q;
        }

        /// <summary>
        /// Open a HDFS fileset as an IQueryable{T}.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="dscFileSetName">The name of the HDFS fileset. </param>
        /// <returns>An IQueryable{T} representing the data and associated with the HPC LINQ query provider.</returns>
        public IQueryable<T> FromHdfs<T>(string fileSetName)
        {
            ThrowIfDisposed();

            string fullPath = DataPath.MakeHdfsStreamUri(_hdfsServiceNode, fileSetName);
            return DataProvider.GetPartitionedTable<T>(this, fullPath);
        }

        /// <summary>
        /// Converts an IEnumerable{T} to a LINQ-to-HPC IQueryable{T}.
        /// </summary>
        /// <typeparam name="T">The type of the records in the table.</typeparam>
        /// <param name="data">The source data.</param>
        /// <returns>An IQueryable{T} representing the data and associated with the HPC LINQ query provider.</returns>
        /// <remarks>
        /// The source data will be serialized to a DSC fileset using the LINQ-to-HPC serialization approach.
        /// The resulting fileset will have an auto-generated name and a temporary lease.
        /// </remarks>
        public IQueryable<T> FromEnumerable<T>(IEnumerable<T> data)
        {
            string fileSetName = DataPath.MakeUniqueTemporaryDscFileSetName();
            DscCompressionScheme compressionScheme = Configuration.IntermediateDataCompressionScheme;
            DryadLinqMetaData metadata = DryadLinqMetaData.ForLocalDebug(this, typeof(T), fileSetName, compressionScheme);
            return DataProvider.IngressTemporaryDataDirectlyToDsc(this, data, fileSetName, metadata, compressionScheme);
        }

        

        /// <summary>
        /// Releases all resources used by the HpcLinqContext.
        /// </summary>
        public void Dispose()
        {
            _configuration = null;
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

        internal static HpcLinqContext GetContext(DryadLinqProviderBase provider)
        {
            HpcLinqContext context = provider.Context;
            Debug.Assert(context != null, "A context should always be associated with a HpcLinqQuery<T>");
            context.ThrowIfDisposed();
            return context;
        }

        // Return IScheduler reference for internal use
        internal IScheduler GetIScheduler()
        {
            return _runtime.GetIScheduler();
        }

        internal void ThrowIfDisposed()
        {
            if (_configuration == null)
            {
                throw new DryadLinqException(HpcLinqErrorCode.ContextDisposed,
                                           SR.ContextDisposed);
            }
        }
    }
}
