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

// 
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Research.Dryad.Hdfs;
using Microsoft.Research.DryadLinq.Internal;
using System.Globalization;

namespace Microsoft.Research.DryadLinq
{
    // The base provider for all DryadLinq queries.
    // Any IQueryable that we are handling should satisfy ((queryable.Provider is DryadLinqProviderBase) == true)
    //
    // For example:
    //    - all IQueryable extension methods ask for (queryable.Provider) and then call provider.CreateQuery(expr)
    internal abstract class DryadLinqProviderBase : IQueryProvider
    {
        private HpcLinqContext m_context;
        internal HpcLinqContext Context { get { return m_context; } }

        internal DryadLinqProviderBase(HpcLinqContext context)
        {
            m_context = context;
        }

        public abstract IQueryable<TElement> CreateQuery<TElement>(Expression expression);
        public abstract IQueryable CreateQuery(Expression expression);
        public abstract TResult Execute<TResult>(Expression expression);
        public abstract object Execute(Expression expression);
    }

    // The provider for DryadLinq queries that will be executed by the LocalDebug infrastructure.
    internal sealed class DryadLinqLocalProvider : DryadLinqProviderBase
    {
        private IQueryProvider m_linqToObjectsProvider;
        
        public DryadLinqLocalProvider(IQueryProvider linqToObjectsProvider, HpcLinqContext context)
            : base(context)
        {
            this.m_linqToObjectsProvider = linqToObjectsProvider;
        }

        //Always throw for untyped call.
        public override IQueryable CreateQuery(Expression expression)
        {
            MethodCallExpression callExpr = expression as MethodCallExpression;
            if (callExpr == null)
            {
                throw new DryadLinqException(HpcLinqErrorCode.ExpressionMustBeMethodCall, SR.ExpressionMustBeMethodCall);
            }
            string methodName = callExpr.Method.Name;
            throw new DryadLinqException(HpcLinqErrorCode.UntypedProviderMethodsNotSupported,
                                       String.Format(SR.UntypedProviderMethodsNotSupported, methodName));
        }

        //Always throw for untyped call.
        public override object Execute(Expression expression)
        {
            return this.CreateQuery(expression);
        }

        public override IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            ThrowIfUnsupported(expression);
            var localQuery = this.m_linqToObjectsProvider.CreateQuery<TElement>(expression);
            return new DryadLinqLocalQuery<TElement>(this, localQuery);
        }

        public override TResult Execute<TResult>(Expression expression)
        {
            ThrowIfUnsupported(expression);
            return this.m_linqToObjectsProvider.Execute<TResult>(expression);
        }

        internal void ThrowIfUnsupported(Expression expression)
        {
            var mcexpr = expression as MethodCallExpression;
            if (mcexpr != null)
            {
                // if (mcexpr.Method.Name == "SequenceEqual")
                // {
                //     throw new NotSupportedException(SR.SequenceEqualNotSupported);
                // }
            }
        }
    }

    // The IQueryable<T> that is used for LocalDebug queries.
    // This is much simpler than DryadLinqQuery<T> as it only has to support fallback to LINQ-to-objects.
    internal sealed class DryadLinqLocalQuery<T> : IOrderedQueryable<T>, IEnumerable<T>, IOrderedQueryable
    {
        private IQueryProvider m_queryProvider;
        private IQueryable<T> m_localQuery;

        public DryadLinqLocalQuery(IQueryProvider queryProvider, IQueryable<T> localQuery)
        {
            this.m_queryProvider = queryProvider;
            this.m_localQuery = localQuery;
        }

        public Expression Expression
        {
            get { return this.m_localQuery.Expression; }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        IQueryProvider IQueryable.Provider
        {
            get { return this.m_queryProvider; }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.m_localQuery.GetEnumerator();
        }
    }


    // The provider for DryadLinq queries that will be executed by the cluster infrastructure.
    internal sealed class DryadLinqProvider : DryadLinqProviderBase
    {
        internal DryadLinqProvider(HpcLinqContext context)
            : base(context)
        {
        }

        //It is exercised by unit test "Bug11782_LowLevelQueryableManipulation"
        // which ccn now expect to receive an exception
        // Always throw for the untyped calls.
        public override IQueryable CreateQuery(Expression expression)
        {
            MethodCallExpression callExpr = expression as MethodCallExpression;
            if (callExpr == null)
            {
                throw new DryadLinqException(HpcLinqErrorCode.ExpressionMustBeMethodCall,
                                           SR.ExpressionMustBeMethodCall);
            }
            string methodName = callExpr.Method.Name;
            throw new DryadLinqException(HpcLinqErrorCode.UntypedProviderMethodsNotSupported,
                                       String.Format(SR.UntypedProviderMethodsNotSupported, methodName));
        }

        public override IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new DryadLinqQuery<TElement>(this, expression);
        }

        //This is the IQueryProvider.Execute() method used for execution when a single value is produced (rather than an enumerable)
        //This non-generic method simply delegates to the generic method
        //Always throw for the untyped calls
        public override object Execute(Expression expression)
        {
            return this.CreateQuery(expression); // which will throw.
        }

        //This is the IQueryProvider.Execute() method used for execution when a single value is produced (rather than an enumerable)
        public override TResult Execute<TResult>(Expression expression)
        {
            MethodCallExpression callExpr = expression as MethodCallExpression;
            if (callExpr == null)
            {
                throw new ArgumentException(String.Format(SR.ExpressionMustBeMethodCall,
                                                          HpcLinqExpression.Summarize(expression)), "expression");
            }
            string methodName = callExpr.Method.Name;
            if (methodName == "FirstOrDefault" ||
                methodName == "SingleOrDefault" ||
                methodName == "LastOrDefault")
            {
                Type qType = typeof(DryadLinqQuery<>).MakeGenericType(typeof(AggregateValue<>).MakeGenericType(expression.Type));
                AggregateValue<TResult> res = ((IEnumerable<AggregateValue<TResult>>)
                                                Activator.CreateInstance(
                                                    qType,
                                                    BindingFlags.NonPublic | BindingFlags.Instance,
                                                    null,
                                                    new object[] { this, expression},
                                                    CultureInfo.CurrentCulture
                                               )).Single();
                if (res.Count == 0) return default(TResult);
                return res.Value;
            }
            else
            {
                Type qType = typeof(DryadLinqQuery<>).MakeGenericType(expression.Type);
                return ((IEnumerable<TResult>) Activator.CreateInstance(
                                                    qType,
                                                    BindingFlags.NonPublic | BindingFlags.Instance,
                                                    null,
                                                    new object[] { this, expression },
                                                    CultureInfo.CurrentCulture
                                                )).Single();
            }
        }
    }


    //Note: cannot be sub-classed by users as they cannot provide overrides for the internal abstract properties.
    internal abstract class DryadLinqQuery
    {
        protected DryadLinqProviderBase m_queryProvider;
        private DataProvider m_dataProvider;
        private bool m_isTemp;
        private JobExecutor m_queryExecutor;

        internal DryadLinqQuery(DryadLinqProviderBase queryProvider, DataProvider dataProvider)
        {
            this.m_queryProvider = queryProvider;
            this.m_dataProvider = dataProvider;
            this.m_queryExecutor = null;
        }

        //if non-null, this provided a data-backed DLQ that should be used in place of (this).
        //query-execution causes a _backingData field to be set for the DLQ nodes that were specifically "executed".
        //(used to be called _table/Table)
        internal abstract DryadLinqQuery BackingDataDLQ { set; }
        internal abstract bool IsDataBacked { get; } 
        
        internal abstract Type Type { get; }
        internal abstract string DataSourceUri { get; }
        internal abstract bool IsDynamic { get; }
        internal abstract int PartitionCount { get; }
        internal abstract DataSetInfo DataSetInfo { get; }

        internal DryadLinqProviderBase QueryProvider
        {
            get { return this.m_queryProvider; }
        }

        internal DataProvider DataProvider
        {
            get { return this.m_dataProvider; }
        }

        internal bool IsTemp
        {
            set { this.m_isTemp = value; }
        }

        internal JobExecutor QueryExecutor
        {
            get { return this.m_queryExecutor; }
            set { this.m_queryExecutor = value; }
        }

        protected void CopyToInternal(DryadLinqQuery otherQuery)
        {
            otherQuery.m_queryProvider = this.m_queryProvider;
            otherQuery.m_dataProvider = this.m_dataProvider;
            otherQuery.m_isTemp = this.m_isTemp;
            otherQuery.m_queryExecutor = this.m_queryExecutor;
        }

        internal virtual VertexCodeGen GetVertexCodeGen()
        {
            return new VertexCodeGen();
        }
    }

    // The IQueryable<T> that is used for cluster-execution queries.
    internal class DryadLinqQuery<T> : DryadLinqQuery, IOrderedQueryable<T>, IEnumerable<T>, IOrderedQueryable
    {
        // If _backingDataDLQ is set, this is a normal query node that was executed and now has a 
        // "PlainData" DLQ available with the results.  
        private DryadLinqQuery<T> m_backingDataDLQ; 
        private Expression m_queryExpression;
        private string m_dataSourceUri;
        private DataSetInfo m_dataSetInfo;
        private bool m_isDynamic;
        private DryadLinqQueryEnumerable<T> m_tableEnumerable;

        //ctor: 
        // 1. used by IQueryProvider. eg IQueryable<>.Select() 
        // 2. used by lazy queries which come in through here via 
        //      DryadLinqIQueryable.ToPartitionedTableLazy()
        //      -> DryadLinqProvider.CreateQuery()
        //        
        internal DryadLinqQuery(DryadLinqProviderBase provider, Expression expression)
            : base(provider, null)
        {
            this.m_queryExpression = expression;
            this.m_isDynamic = false;
            this.m_tableEnumerable = null;
        }

        //ctor:
        //[ML]: combined from MSR-DL ctors for PartitionedTable<> and DryadLinqQuery<>
        //      This ctor is used by DryadLinqQuery.Get(uri)
        internal DryadLinqQuery(Expression queryExpression,
                                DryadLinqProvider queryProvider,
                                DataProvider dataProvider,
                                string dataUri)
            : base(queryProvider, dataProvider)
        {
            if(!DataPath.IsDsc(dataUri) && !DataPath.IsHdfs(dataUri))
            {
                throw new DryadLinqException(HpcLinqErrorCode.UnrecognizedDataSource,
                                           String.Format(SR.UnrecognizedDataSource, dataUri));
            }

            this.m_queryExpression = queryExpression;
            this.m_dataSourceUri = DataPath.GetDataPath(dataUri);
            this.m_isDynamic = false;
            this.m_tableEnumerable = null;
        }

        internal void CopyTo(DryadLinqQuery<T> otherQuery)
        {
            this.CopyToInternal(otherQuery);
            otherQuery.m_backingDataDLQ = this.m_backingDataDLQ;
            otherQuery.m_queryExpression = this.m_queryExpression;
            otherQuery.m_dataSourceUri = this.m_dataSourceUri;
            otherQuery.m_dataSetInfo = this.m_dataSetInfo;
            otherQuery.m_isDynamic = this.m_isDynamic;
            otherQuery.m_tableEnumerable = this.m_tableEnumerable;
        }

        // returns true for DLQ that are pointing directly at plain data.
        // Note: plain-data DLQ might also have an executor associated with it.. the data wont be 
        // available unless the executor completes sucessfully.
        internal bool IsPlainData
        {
            get { return (this.m_dataSourceUri != null); }
        }

        // returns true for DLQ that are not themselves pointing directly at plain data, eg query-operators.
        internal bool IsNormalQuery
        {
            get { return (this.m_dataSourceUri == null); }
        }

        internal override DryadLinqQuery BackingDataDLQ
        {
            set { m_backingDataDLQ = (DryadLinqQuery<T>)value; }
        }

        // returns true for a normal query that was executed and now has a backing data DLQ available.
        internal override bool IsDataBacked
        {
            get { return (this.m_backingDataDLQ != null); }
        }

        // returns true if an executor is associated with the DLQ.
        internal bool HasExecutor
        {
            get { 
                bool hasExec = (this.QueryExecutor != null);
                if (hasExec && !IsPlainData)
                {
                    throw new DryadLinqException("An executor should only be associated with a DLQ that is plain data");
                }
                return hasExec; 
            }
        }

        public HpcLinqContext Context
        {
            get { return m_queryProvider.Context; }
        }

        #region IQueryable members
        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        //@@Comment-required: (bit unclear what the intended behavior is for localDebug)
        //ML: combined from PT<T> and DLQ<T>.. 
        IQueryProvider IQueryable.Provider
        {
            get
            {
                this.CheckAndInitialize();
                return this.m_queryProvider;
            }
        }
        #endregion

        // Executes a query to a named Dsc URI.  The query should _not_ be terminated with ToDsc().
        internal DryadLinqQuery<T> ToTemporaryTable(HpcLinqContext context, string targetUri)
        {
            if ((!DataPath.IsDsc(targetUri)) && (!DataPath.IsHdfs(targetUri)))
            {
                throw new ArgumentException(String.Format(SR.UnrecognizedDataSource, targetUri));
            }
            
            HpcLinqQueryGen dryadGen = null;
            string realTableUri = targetUri;
#if REMOVE_FOR_YARN
            if (IsPlainData) // was if (this.m_queryExpression is ConstantExpression)
            {
                //@@TODO: I think this is dead code. See if it can be exercised.

                // the input is a Plain-data DLQ.
                // the output-target has been set
                // We expect both to be DSC -- so we just use the DSC API to perform a copy rather than invoke dryad.
                string inputUri = DataSourceUri;

                Debug.Assert(DataPath.IsDsc(inputUri) && DataPath.IsDsc(targetUri), "both uris should be to Dsc");

                using (DscInstance inputService = new DscInstance(new Uri(inputUri)))
                using (DscInstance outputService = new DscInstance(new Uri(targetUri)))
                {
                    DscStream inputStream = inputService.GetStream(new Uri(inputUri));
                    try
                    {
                        DscStream outputStream = outputService.GetStream(new Uri(targetUri));
                        outputStream.Delete();
                    }
                    catch (DscException)
                    { }
                    inputStream.Copy(new Uri(targetUri));
                    ////this.m_table = DryadLinqQuery.Get<T>(tableUri);  
                    ////return this.m_table;
                    //// [ML] part of deleting this.m_table.  We just return (this) rather than (this.m_table)

                    DryadLinqQuery<T> databackedDLQ = DataProvider.GetPartitionedTable<T>(Context, targetUri);
                    this.m_backingDataDLQ = databackedDLQ;  //ML: we set the new table as backing data for the source. (not sure what this gains)
                    return databackedDLQ;
                }
                
            }
            else if (IsDataBacked)
            {
                // @@TODO: I think this is dead code. See if it can be exercised.
                // if taken, we should be able to just recurse with the backing data (after doing _backingDataDLQ.CheckAndInitialize())
                throw new NotImplementedException();
            }
#endif
            // Invoke Dryad
            Debug.Assert(IsNormalQuery, "execution should only occur for a normal query");
            if (dryadGen == null)
            {
                dryadGen = new HpcLinqQueryGen(context, this.GetVertexCodeGen(),  this.Expression, realTableUri, true);
            }
            DryadLinqQuery[] resultTables = dryadGen.InvokeDryad();
            this.m_backingDataDLQ = (DryadLinqQuery<T>) resultTables[0];

            return this;
        }

        // Generate the query plan as an XML file and return the file name.
        // provided for test-support. Access via reflection.
        // returns the queryPlan xml path.
        internal string ToDryadLinqProgram()
        {
            string tableUri = DataPath.DSC_URI_PREFIX + @"dummy/dummy";
            HpcLinqQueryGen dryadGen = new HpcLinqQueryGen(Context, this.GetVertexCodeGen(), this.m_queryExpression, tableUri, true);
            return dryadGen.GenerateDryadProgram(); 
        }

        // ML: complex ToString is problematic for the debugger -- eg Expression.ToString() leads to infinite recursion 
        //     for PlainData which has Expression=ConstantExpression(this) and ConstantExpression(x).ToString() ==> x.ToString()
        //     Also, a rich ToString risks leaking internal details.
        // @@TODO[P2]:  this override should not be necessary.. however the debugger was acting up without it..
        //              eg timing out when inspecting DryadLinqQuery objects in watch window etc.  
        public override string ToString()
        {
            return base.ToString();
        }

        internal void Initialize()
        {
            //Detailed initialize behavior is only for plain-old-data.
            //This was previously implicit (as only defined on the PartitionedData<> type)
            if (this.IsPlainData)
            {
                // short-circuit if already initialized
                if (this.m_tableEnumerable != null)
                {
                    return;
                }

                Int32 parCount = 0;
                Int64 estSize = -1;
                this.m_isDynamic = false;

                try
                {
                    // YY: TODO: This could just be set to -1 if the xmlexechost will create the correct number of partitions
                    // YY: We need the partition count here: it is used in plan optimization.
                    if (DataPath.IsHdfs((this.m_dataSourceUri)))
                    {
                        //hdfs
                        /*
                        using (HdfsInstance hdfs = new HdfsInstance(this.m_dataSourceUri))
                        {
                            string path = hdfs.FromInternalUri(this.m_dataSourceUri);
                            HdfsFileInfo dataStream = hdfs.GetFileInfo(path, true);
                            estSize = (long)dataStream.totalSize;
                            parCount = (Int32)dataStream.blockArray.Length;
                        }
                         */
                        WebHdfsClient.GetContentSummary(this.m_dataSourceUri, ref estSize, ref parCount);

                    }
                    else
                    {
                        //dsc
                        using (DscInstance dataService = new DscInstance(new Uri(this.m_dataSourceUri)))
                        {
                            DscStream dataStream = dataService.GetStream(new Uri(this.m_dataSourceUri));
                            estSize = (long)dataStream.Length;
                            parCount = (Int32)dataStream.PartitionCount;

                        }
                    }
                }
                catch (Exception e)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.FailedToGetStreamProps,
                                               String.Format(SR.FailedToGetStreamProps, this.m_dataSourceUri), e);
                }

                // --- start metadata processing --- //
                // Finally load any stored metadata to check settings, extract compression-setting and initialize the DataInfo for this Query.
                string streamName = DataPath.GetFilesetNameFromUri(this.m_dataSourceUri); // we converted to uri.. now must go back to stream-name.
                DryadLinqMetaData meta = null;
                if (DataPath.IsDsc(this.m_dataSourceUri))
                {
                    meta = DryadLinqMetaData.FromDscStream(Context, streamName);
                }
                if (meta != null)
                {
                    //check the record-type matches meta-data. (disabled until final API is determined)
                    //if (meta.ElemType != typeof(T))
                    //{
                    //    throw new HpcLinqException(HpcLinqErrorCode.MetadataRecordType,
                    //                               String.Format(SR.MetadataRecordType, typeof(T), meta.ElemType));
                    //}

                    //check the serialization flags match meta-data. (disabled as serialization flags are fixed. re-consider when flags become user-settable again.)
                    //if (StaticConfig.AllowNullFields != meta.AllowNullFields ||
                    //    StaticConfig.AllowNullArrayElements != meta.AllowNullArrayElements ||
                    //    StaticConfig.AllowNullRecords != meta.AllowNullRecords)
                    //{
                    //    HpcClientSideLog.Add("Warning: Table was generated with AllowNullFields=" + meta.AllowNullFields +
                    //                      ", AllowNullRecords=" + meta.AllowNullRecords +
                    //                      ", and AllowNullArrayElements=" + meta.AllowNullArrayElements);
                    //}

                }

                // Initialize the DataInfo -- currently we always initialize to the "nothing" datainfo.
                PartitionInfo pinfo = new RandomPartition(parCount);
                OrderByInfo oinfo = DataSetInfo.NoOrderBy;
                DistinctInfo dinfo = DataSetInfo.NoDistinct;
                this.m_dataSetInfo = new DataSetInfo(pinfo, oinfo, dinfo);

                // --- end metadata processing --- //

                string fileSetName = DataPath.GetFilesetNameFromUri(this.m_dataSourceUri);
                this.m_tableEnumerable = new DryadLinqQueryEnumerable<T>(this.Context, fileSetName);

                //YY: query expression and provider are at least set consistently
                if (Context.Configuration.LocalDebug)
                {
                    this.m_queryExpression = Expression.Constant(this.m_tableEnumerable.AsQueryable());
                    IQueryProvider linqToObjectProvider = this.m_tableEnumerable.AsQueryable().Provider;  // this should be an instance of "EnumerableQuery<T>"
                    this.m_queryProvider = new DryadLinqLocalProvider(linqToObjectProvider, Context);
                }
                else
                {
                    this.m_queryExpression = Expression.Constant(this);
                    this.m_queryProvider = new DryadLinqProvider(Context);
                }
            }
        }

        internal override Type Type
        {
            get { return typeof(T); }
        }

        // only legal/valid for plainData and data-backed DLQ. 
        internal override string DataSourceUri
        {
            get {

                if (this.IsPlainData)
                {
                    // no need to CheckAndInitialize() as m_dataSourceUri should already be set.
                    // also, performing checkAndInitialize causes infinite recursion due to it accessing DataSourceUri
                    return this.m_dataSourceUri;                    
                }
                else if (this.IsDataBacked)
                {
                    // as above, regarding CheckAndInitialize()
                    return (this.m_backingDataDLQ).m_dataSourceUri; 
                }

                throw new DryadLinqException(HpcLinqErrorCode.OnlyAvailableForPhysicalData,
                                           SR.OnlyAvailableForPhysicalData);
            }
        }

        //   combination of old approaches.  return either the full expression, or just an expression for the table, if available.
        // * Fundamental part of IQueryable system.
        //   Most IQueryable operators will access (source.Expression) and form a new IQueryable
        //   which is a MethodCall('method',{src.Expression,params})
        //
        //   Plain data: we create an expression to represent plain-data
        //   Data-backed query: we behave as if the IQueryable were just the backing data (ie a simple expression to plain-data)
        //   Normal query: an normal query node will already have a m_queryExpression
        public Expression Expression
        {
            get 
            {
                if (this.IsDataBacked)
                {
                    // if this is a data-backed-query, (recursively) return the expression for the backingDLQ
                    Debug.Assert(this.m_backingDataDLQ.IsPlainData, "backing data is expected to always be plain data");
                    return (this.m_backingDataDLQ).Expression;
                }
                this.CheckAndInitialize();
                return this.m_queryExpression;
            }
        }

        internal override int PartitionCount
        {
            get 
            {
                if (IsPlainData)
                {
                    this.CheckAndInitialize();
                    return this.m_dataSetInfo.partitionInfo.Count;
                }
                
                if (IsDataBacked)
                {
                    this.m_backingDataDLQ.CheckAndInitialize();
                    return this.m_backingDataDLQ.PartitionCount;
                }

                throw new DryadLinqException(HpcLinqErrorCode.OnlyAvailableForPhysicalData,
                                           SR.OnlyAvailableForPhysicalData);
            }
        }



        internal override bool IsDynamic
        {
            get {
                this.CheckAndInitialize();
                return this.m_isDynamic;  // possible issue: if(IsDataBacked) then using the value from backing data may be more appropriate.
            }
        }

        internal override DataSetInfo DataSetInfo
        {
            get
            {
                //even if data-backed, the DataSetInfo for the normal-query is the best available.
                //hence this._m_dataSetInfo is always best.

                this.CheckAndInitialize();
                return this.m_dataSetInfo;
            }
        }

        internal void CheckAndInitialize()
        {
            if (HasExecutor)
            {
                Debug.Assert(IsPlainData, "We expect a DLQ with an executor to be a plain-data DLQ");
                
                JobStatus status = this.QueryExecutor.WaitForCompletion();
                if (status == JobStatus.Failure)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.JobToCreateTableFailed,
                                               String.Format(SR.JobToCreateTableFailed, this.QueryExecutor.ErrorMsg));
                }
                if (status == JobStatus.Cancelled)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.JobToCreateTableWasCanceled,
                                               SR.JobToCreateTableWasCanceled);
                }
                if (status == JobStatus.Success)
                {
                    HpcClientSideLog.Add("Table " + this.DataSourceUri + " was created successfully.");
                }
            }
            this.Initialize();
        }

        

        #region IEnumerable and IEnumerable<T> members
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        // combined GetEnumerator from PT and DLQ... use table if present, else start query to generate anonymous output table.
        public IEnumerator<T> GetEnumerator()
        {
            // Process:
            // 1. if this is a data-backed-query, return an enumerator over the backing data
            // 2. if this is plain-data, return an enumerator over the data.
            // 2. otherwise, start an anonymous query execution (which will produce a data-backed-query), and call GetEnumerator() again to hit the first path.

            if (this.IsPlainData){
                this.CheckAndInitialize();
                return this.m_tableEnumerable.GetEnumerator();
            }
            else if (this.IsDataBacked)
            {
                m_backingDataDLQ.CheckAndInitialize();
                return m_backingDataDLQ.m_tableEnumerable.GetEnumerator();
            }
            else
            {
                Debug.Assert(IsNormalQuery);
                
                // if terminated in ToDsc, eg query.ToDsc("path").GetEnumerator();
                //    currently: treat this as an error.  We throw in both cluster and LocalDebug modes.
                //    @@TODO[P2]: we could execute the query, producing data as specified by ToDsc().  (see DryadQueryGen ctors)
                //
                // otherwise, we create a temporary stream to hold the data and get an enumerator.
                //    - cluster mode will run a dryad query.
                //    - LocalDebug mode will write the data directly into DSC.

                string hdfsPath = DataPath.MakeUniqueTemporaryHdfsFileSetUri(Context);
                return this.ToTemporaryTable(Context, hdfsPath).GetEnumerator();
            }
        }
        #endregion
    }


    //From PartitionedTableEnumerable
    internal class DryadLinqQueryEnumerable<T> : IEnumerable<T>, IEnumerable
    {
        internal string m_fileSetName;
        private HpcLinqContext m_context;

        public DryadLinqQueryEnumerable(HpcLinqContext context, string fileSetName)
        {
            m_context = context;
            m_fileSetName = fileSetName;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            List<string[]> filePathList;    // a list of dsc files, each of which is represented by an array holding the replica paths
            DscCompressionScheme compressionScheme;
            try
            {
                DscFileSet fileSet = m_context.DscService.GetFileSet(m_fileSetName);
                filePathList = fileSet.GetFiles().Select(file => file.ReadPaths).ToList();
                DryadLinqMetaData metaData = DryadLinqMetaData.FromDscStream(m_context, m_fileSetName);
                compressionScheme = metaData.CompressionScheme;
                
            }
            catch (Exception e)
            {
                throw new DryadLinqException(HpcLinqErrorCode.FailedToGetReadPathsForStream,
                                           String.Format(SR.FailedToGetReadPathsForStream, this.m_fileSetName), e);
            }

            return new TableEnumerator(m_context, filePathList, m_fileSetName,compressionScheme);
        }
    
        // Internal enumerator class
        private class TableEnumerator : IEnumerator<T>
        {
            private HpcLinqContext m_context;
            private T m_current;
            private List<string[]> m_filePathList;  // a list of dsc files, each of which is represented by an array holding the replica paths
            private string m_associatedDscStreamName; // stored here only to provide a better exception message in case of IO errors
            private DscCompressionScheme m_compressionScheme;
            private HpcLinqFactory<T> m_factory;
            private HpcRecordReader<T> m_reader;

            internal TableEnumerator(HpcLinqContext context,
                                     List<string[]> filePathList,
                                     string associatedDscStreamName,
                                     DscCompressionScheme scheme)
            {
                this.m_context = context;
                this.m_current = default(T);
                this.m_filePathList = filePathList;
                this.m_associatedDscStreamName = associatedDscStreamName;
                this.m_compressionScheme = scheme;
                this.m_factory = (HpcLinqFactory<T>)HpcLinqCodeGen.GetFactory(context, typeof(T));
                bool appendNewLinesToFiles = (typeof(T) == typeof(LineRecord));
                NativeBlockStream nativeStream = new MultiBlockStream(m_filePathList, m_associatedDscStreamName,
                                                                      FileAccess.Read, m_compressionScheme,
                                                                      appendNewLinesToFiles);
                this.m_reader = this.m_factory.MakeReader(nativeStream);
                
                if (context.Configuration.AllowConcurrentUserDelegatesInSingleProcess)
                {
                    this.m_reader.StartWorker();
                }
            }

            public bool MoveNext()
            {
                if (m_context.Configuration.AllowConcurrentUserDelegatesInSingleProcess)
                {
                    return this.m_reader.ReadRecordAsync(ref this.m_current);
                }
                else
                {
                    return this.m_reader.ReadRecordSync(ref this.m_current);
                }
            }

            object IEnumerator.Current
            {
                get { return this.m_current; }
            }

            public T Current
            {
                get { return this.m_current; }
            }

            public void Reset()
            {
                this.m_current = default(T);
                bool appendNewLineToFiles = (typeof(T) == typeof(LineRecord));
                NativeBlockStream nativeStream = new MultiBlockStream(this.m_filePathList, m_associatedDscStreamName,
                                                                      FileAccess.Read, this.m_compressionScheme,
                                                                      appendNewLineToFiles);
                this.m_reader = this.m_factory.MakeReader(nativeStream);

                if (m_context.Configuration.AllowConcurrentUserDelegatesInSingleProcess)
                {
                    this.m_reader.StartWorker();
                }
            }

            void IDisposable.Dispose()
            {
                this.m_reader.Close();
            }
        }
    }
}
