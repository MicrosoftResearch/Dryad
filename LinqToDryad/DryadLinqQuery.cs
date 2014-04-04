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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    // The base provider for all DryadLinq queries.
    // All IQueryable extension methods ask for (queryable.Provider) and then call provider.CreateQuery(expr)
    internal abstract class DryadLinqProviderBase : IQueryProvider
    {
        private DryadLinqContext m_context;

        internal DryadLinqProviderBase(DryadLinqContext context)
        {
            this.m_context = context;
        }

        internal DryadLinqContext Context { get { return this.m_context; } }

        public abstract IQueryable<TElement> CreateQuery<TElement>(Expression expression);
        public abstract IQueryable CreateQuery(Expression expression);
        public abstract TResult Execute<TResult>(Expression expression);
        public abstract object Execute(Expression expression);
    }

    // The provider for DryadLinq queries that will be executed by the LocalDebug infrastructure.
    internal sealed class DryadLinqLocalProvider : DryadLinqProviderBase
    {
        private IQueryProvider m_linqToObjectsProvider;

        public DryadLinqLocalProvider(IQueryProvider linqToObjectsProvider, DryadLinqContext context)
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
                throw new DryadLinqException(DryadLinqErrorCode.ExpressionMustBeMethodCall,
                                             SR.ExpressionMustBeMethodCall);
            }
            string methodName = callExpr.Method.Name;
            throw new DryadLinqException(DryadLinqErrorCode.UntypedProviderMethodsNotSupported,
                                         String.Format(SR.UntypedProviderMethodsNotSupported, methodName));
        }

        //Always throw for untyped call.
        public override object Execute(Expression expression)
        {
            return this.CreateQuery(expression);
        }

        public override IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            var localQuery = this.m_linqToObjectsProvider.CreateQuery<TElement>(expression);
            return new DryadLinqLocalQuery<TElement>(this, localQuery);
        }

        public override TResult Execute<TResult>(Expression expression)
        {
            return this.m_linqToObjectsProvider.Execute<TResult>(expression);
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
        
        public IEnumerator<T> GetEnumerator()
        {
            return this.m_localQuery.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    // The provider for DryadLinq queries that will be executed by the cluster infrastructure.
    internal class DryadLinqProvider : DryadLinqProviderBase
    {
        internal DryadLinqProvider(DryadLinqContext context)
            : base(context)
        {
        }

        public override IQueryable CreateQuery(Expression expression)
        {
            MethodCallExpression callExpr = expression as MethodCallExpression;
            if (callExpr == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.ExpressionMustBeMethodCall,
                                             SR.ExpressionMustBeMethodCall);
            }
            string methodName = callExpr.Method.Name;
            throw new DryadLinqException(DryadLinqErrorCode.UntypedProviderMethodsNotSupported,
                                         String.Format(SR.UntypedProviderMethodsNotSupported, methodName));
        }

        public override IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new DryadLinqQuery<TElement>(this, expression);
        }

        // This is the IQueryProvider.Execute() method used for execution
        // when a single value is produced (rather than an enumerable)
        public override object Execute(Expression expression)
        {
            return this.CreateQuery(expression); // which will throw.
        }

        // This is the IQueryProvider.Execute() method used for execution
        // when a single value is produced (rather than an enumerable)
        public override TResult Execute<TResult>(Expression expression)
        {
            MethodCallExpression callExpr = expression as MethodCallExpression;
            if (callExpr == null)
            {
                throw new ArgumentException(String.Format(SR.ExpressionMustBeMethodCall,
                                                          DryadLinqExpression.Summarize(expression)),
                                            "expression");
            }
            string methodName = callExpr.Method.Name;
            if (methodName == "FirstOrDefault" ||
                methodName == "SingleOrDefault" ||
                methodName == "LastOrDefault")
            {
                Type elemType = typeof(AggregateValue<>).MakeGenericType(expression.Type);
                Type qType = typeof(DryadLinqQuery<>).MakeGenericType(elemType);
                AggregateValue<TResult> res = ((IEnumerable<AggregateValue<TResult>>)
                                                Activator.CreateInstance(
                                                    qType,
                                                    BindingFlags.NonPublic | BindingFlags.Instance,
                                                    null,
                                                    new object[] { this, expression },
                                                    CultureInfo.CurrentCulture
                                                    )).Single();
                if (res.Count == 0) return default(TResult);
                return res.Value;
            }
            else
            {
                Type qType = typeof(DryadLinqQuery<>).MakeGenericType(expression.Type);
                return ((IEnumerable<TResult>)Activator.CreateInstance(
                                                    qType,
                                                    BindingFlags.NonPublic | BindingFlags.Instance,
                                                    null,
                                                    new object[] { this, expression },
                                                    CultureInfo.CurrentCulture
                                                    )).Single();
            }
        }
    }

    internal abstract class DryadLinqQuery : IQueryable
    {
        protected DryadLinqProviderBase m_queryProvider;
        private DataProvider m_dataProvider;
        private bool m_isTemp;
        private DryadLinqJobExecutor m_queryExecutor;

        internal DryadLinqQuery(DryadLinqProviderBase queryProvider,
                                DataProvider dataProvider)
        {
            this.m_queryProvider = queryProvider;
            this.m_dataProvider = dataProvider;
            this.m_queryExecutor = null;
        }

        //if non-null, this provided a data-backed DLQ that should be used in place of (this).
        //query-execution causes a _backingData field to be set for the DLQ nodes that were "executed".
        internal abstract DryadLinqQuery BackingData { get; set; }
        internal bool IsDataBacked
        {
            get { return this.BackingData != null; }
        }
        
        public abstract Type ElementType { get; }
        public abstract Expression Expression { get; }
        internal abstract bool IsPlainData { get; }
        internal abstract Uri DataSourceUri { get; }
        internal abstract bool IsDynamic { get; }
        internal abstract int PartitionCount { get; }
        internal abstract DataSetInfo DataSetInfo { get; }

        protected abstract IEnumerator IEnumGetEnumerator();
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.IEnumGetEnumerator();
        }

        public IQueryProvider Provider
        {
            get { return this.m_queryProvider; }
            set { this.m_queryProvider = (DryadLinqProviderBase)value; }
        }

        internal DataProvider DataProvider
        {
            get { return this.m_dataProvider; }
        }

        public DryadLinqContext Context
        {
            get { return m_queryProvider.Context; }
        }

        internal bool IsTemp
        {
            set { this.m_isTemp = value; }
        }

        internal DryadLinqJobExecutor QueryExecutor
        {
            get { return this.m_queryExecutor; }
            set { this.m_queryExecutor = value; }
        }

        protected void CloneBase(DryadLinqQuery otherQuery)
        {
            if (otherQuery.m_queryProvider == null)
            {
            otherQuery.m_queryProvider = this.m_queryProvider;
            }
            if (otherQuery.m_dataProvider == null)
            {
            otherQuery.m_dataProvider = this.m_dataProvider;
            }
            otherQuery.m_isTemp = this.m_isTemp;
            otherQuery.m_queryExecutor = this.m_queryExecutor;
        }

        internal virtual VertexCodeGen GetVertexCodeGen()
        {
            return new VertexCodeGen(this.m_queryProvider.Context);
        }
    }

    // The IQueryable<T> that is used for cluster-execution queries.
    internal class DryadLinqQuery<T>
        : DryadLinqQuery, IOrderedQueryable<T>, IEnumerable<T>, IOrderedQueryable
    {
        // If BackingData is set, this is a normal query node that was executed and now has a 
        // "PlainData" DLQ available with the results.  
        private DryadLinqQuery<T> m_backingData; 
        private Expression m_queryExpression;
        private Uri m_dataSourceUri;
        private DataSetInfo m_dataSetInfo;
        private bool m_isDynamic;
        private DryadLinqQueryEnumerable<T> m_tableEnumerable;

        // Used by IQueryProvider. e.g., IQueryable<>.Select() and IQueryable<>.ToStore()
        internal DryadLinqQuery(DryadLinqProviderBase provider, Expression expression)
            : base(provider, null)
        {
            this.m_queryExpression = expression;
            this.m_isDynamic = false;
            this.m_tableEnumerable = null;
        }

        // Used by DryadLinqContext.LoadFrom(uri)
        internal DryadLinqQuery(Expression queryExpression,
                                DryadLinqProvider queryProvider,
                                DataProvider dataProvider,
                                Uri dataSetUri)
            : base(queryProvider, dataProvider)
        {
            if (!DataPath.IsValidDataPath(dataSetUri))
            {
                throw new DryadLinqException(DryadLinqErrorCode.UnrecognizedDataSource,
                                             String.Format(SR.UnrecognizedDataSource, dataSetUri.AbsoluteUri));
            }

            this.m_queryExpression = queryExpression;
            this.m_dataSourceUri = dataSetUri;
            this.m_isDynamic = false;
            this.m_tableEnumerable = null;
        }

        internal void Clone(DryadLinqQuery<T> otherQuery)
        {
            this.CloneBase(otherQuery);
            otherQuery.m_backingData = this.m_backingData;
            otherQuery.m_queryExpression = this.m_queryExpression;
            otherQuery.m_dataSourceUri = this.m_dataSourceUri;
            otherQuery.m_dataSetInfo = this.m_dataSetInfo;
            otherQuery.m_isDynamic = this.m_isDynamic;
            otherQuery.m_tableEnumerable = this.m_tableEnumerable;
        }

        // returns true for DLQ that are pointing directly at plain data.
        // Note: plain-data DLQ might also have an executor associated with it.. the data 
        // wont be available unless the executor completes sucessfully.
        internal override bool IsPlainData
        {
            get { return (this.m_dataSourceUri != null); }
        }

        internal override DryadLinqQuery BackingData
        {
            get { return this.m_backingData; }
            set { this.m_backingData = (DryadLinqQuery<T>)value; }
        }

        public override Type ElementType
        {
            get { return typeof(T); }
        }

        // only legal/valid for plainData and data-backed DLQ. 
        internal override Uri DataSourceUri
        {
            get
            {
                if (this.IsPlainData)
                {
                    this.CheckAndInitialize();
                    return this.m_dataSourceUri;                    
                }
                else if (this.IsDataBacked)
                {
                    // as above, regarding CheckAndInitialize()
                    return this.m_backingData.DataSourceUri; 
                }
                throw new DryadLinqException(DryadLinqErrorCode.OnlyAvailableForPhysicalData,
                                             SR.OnlyAvailableForPhysicalData);
            }
        }

        // Plain data: we create an expression to represent plain-data
        // Data-backed query: we behave as if the IQueryable were just the backing data.
        public override Expression Expression
        {
            get
            {
                if (this.IsPlainData)
                {
                    this.CheckAndInitialize();
                    return this.m_queryExpression;
                }
                else if (this.IsDataBacked)
                {
                    if (this.m_backingData.QueryExecutor != null)
                    {
                        this.CheckAndInitialize();
                    }
                    return this.m_backingData.Expression;
                }
                this.CheckAndInitialize();
                return this.m_queryExpression;
            }
        }

        internal override int PartitionCount
        {
            get 
            {
                if (this.IsPlainData)
                {
                    this.CheckAndInitialize();
                    return this.m_dataSetInfo.partitionInfo.Count;
                }
                else if (this.IsDataBacked)
                {
                    return this.m_backingData.PartitionCount;
                }
                throw new DryadLinqException(DryadLinqErrorCode.OnlyAvailableForPhysicalData,
                                             SR.OnlyAvailableForPhysicalData);
            }
        }

        internal override bool IsDynamic
        {
            get
            {
                this.CheckAndInitialize();
                return this.m_isDynamic;
            }
        }
                
        internal override DataSetInfo DataSetInfo
        {
            get
            {
                if (this.IsPlainData)
                {
                    this.CheckAndInitialize();
                    return this.m_dataSetInfo;
                }
                else if (this.IsDataBacked)
                {
                    return this.m_backingData.DataSetInfo;
                }
                this.CheckAndInitialize();
                return this.m_dataSetInfo;
        }
        }

        internal void CheckAndInitialize()
        {
            if (this.QueryExecutor != null)
            {
                JobStatus status = this.QueryExecutor.WaitForCompletion();
                if (status == JobStatus.Failure)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.JobToCreateTableFailed,
                                                 String.Format(SR.JobToCreateTableFailed,
                                                               this.QueryExecutor.ErrorMsg));
                }
                if (status == JobStatus.Cancelled)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.JobToCreateTableWasCanceled,
                                                 SR.JobToCreateTableWasCanceled);
                }
                if (status == JobStatus.Success)
                {
                    DryadLinqClientLog.Add("Dataset " + this.m_dataSourceUri + " was created successfully.");
                }
            }
            this.Initialize();
        }

        internal void Initialize()
        {
            if (this.IsPlainData && this.m_tableEnumerable == null)
            {
                DryadLinqStreamInfo streamInfo = this.DataProvider.GetStreamInfo(this.Context, this.m_dataSourceUri);
                Int32 parCount = streamInfo.PartitionCount;
                Int64 estSize = streamInfo.DataSize;
                this.m_isDynamic = false;

                // Finally load any stored metadata to check settings, extract compression-setting
                // and initialize the DataInfo for this Query. It is uri.. have to convert to stream-name.
                DryadLinqMetaData meta = null;
                if (DataPath.IsDsc(this.m_dataSourceUri))
                {
                    meta = DryadLinqMetaData.Get(Context, this.m_dataSourceUri);
                }
                if (meta != null)
                {
                    //check the record-type matches meta-data. (disabled until final API is determined)
                    //if (meta.ElemType != typeof(T))
                    //{
                    //    throw new DisributedLinqException(DryadLinqErrorCode.MetadataRecordType,
                    //                                      String.Format(SR.MetadataRecordType,
                    //                                                    typeof(T), meta.ElemType));
                    //}

                    //check the serialization flags match meta-data.
                    //(disabled as serialization flags are fixed. re-consider if user-settable.)
                    //if (StaticConfig.AllowNullFields != meta.AllowNullFields ||
                    //    StaticConfig.AllowNullArrayElements != meta.AllowNullArrayElements ||
                    //    StaticConfig.AllowNullRecords != meta.AllowNullRecords)
                    //{
                    //    DryadLinqClientLog.Add("Warning: Table was generated with AllowNullFields=" +
                    //                                 meta.AllowNullFields +
                    //                                 ", AllowNullRecords=" + meta.AllowNullRecords +
                    //                                 ", and AllowNullArrayElements=" +
                    //                                 meta.AllowNullArrayElements);
                    //}
                }

                // Initialize the DataInfo -- currently we always initialize to the "nothing" datainfo.
                PartitionInfo pinfo = new RandomPartition(parCount);
                OrderByInfo oinfo = DataSetInfo.NoOrderBy;
                DistinctInfo dinfo = DataSetInfo.NoDistinct;
                this.m_dataSetInfo = new DataSetInfo(pinfo, oinfo, dinfo);

                this.m_tableEnumerable = new DryadLinqQueryEnumerable<T>(this.DataProvider, this.Context, this.m_dataSourceUri);

                // YY: query expression and provider are at least set consistently
                if (Context.LocalDebug)
                {
                    this.m_queryExpression = Expression.Constant(this.m_tableEnumerable.AsQueryable());
                    IQueryProvider linqToObjectProvider = this.m_tableEnumerable.AsQueryable().Provider;
                    this.m_queryProvider = new DryadLinqLocalProvider(linqToObjectProvider, Context);
                }
                else
                {
                    this.m_queryExpression = Expression.Constant(this);
                    if (this.m_queryProvider == null)
                    {
                        // Only set if not provided
                        this.m_queryProvider = new DryadLinqProvider(this.Context);
                    }
                }
            }
        }

        protected override IEnumerator IEnumGetEnumerator()
        {
            return this.GetEnumerator();
        }

        // Use table if present, else start query to generate anonymous output table.
        public IEnumerator<T> GetEnumerator()
        {
            // Process:
            // 1. if this is plain-data, return an enumerator over the data.
            // 2. if this is a data-backed-query, return an enumerator over the backing data
            // 3. otherwise, start an anonymous query execution (which will produce a data-backed-query),
            //    and call GetEnumerator() again to hit the first path.
            if (this.IsPlainData)
            {
                this.CheckAndInitialize();
                return this.m_tableEnumerable.GetEnumerator();
            }
            else if (this.IsDataBacked)
            {
                return this.m_backingData.GetEnumerator();
            }
            else
            {
                DryadLinqQueryable.SubmitAndWait(this);
                return this.m_backingData.GetEnumerator();
            }
        }

        // Generate the query plan as an XML file and return the file name.
        // returns the queryPlan xml path.
        internal string ToDryadLinqProgram()
        {
            Uri tableUri = this.Context.MakeTemporaryStreamUri();
            DryadLinqQueryGen dryadGen = new DryadLinqQueryGen(
                        this.Context, this.GetVertexCodeGen(), this.m_queryExpression, tableUri, true);
            return dryadGen.GenerateDryadProgram(); 
        }
    }

    internal class DryadLinqQueryEnumerable<T> : IEnumerable<T>, IEnumerable
    {
        private DryadLinqContext m_context;
        private Stream m_stream;

        public DryadLinqQueryEnumerable(DataProvider dataProvider, DryadLinqContext context, Uri dataSetUri)
        {
            this.m_context = context;
            this.m_stream = dataProvider.Egress(context, dataSetUri);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new TableEnumerator(this.m_context, this.m_stream);
        }

        // Internal enumerator class
        private class TableEnumerator : IEnumerator<T>
        {
            private T m_current;
            private DryadLinqFactory<T> m_factory;
            private DryadLinqRecordReader<T> m_reader;

            internal TableEnumerator(DryadLinqContext context, Stream stream)
            {
                this.m_current = default(T);
                this.m_factory = (DryadLinqFactory<T>)DryadLinqCodeGen.GetFactory(context, typeof(T));
                DryadLinqBlockStream nativeStream = new DryadLinqBlockStream(stream);
                this.m_reader = this.m_factory.MakeReader(nativeStream);
                // this.m_reader.StartWorker();
            }

            public bool MoveNext()
            {
                return this.m_reader.ReadRecordSync(ref this.m_current);
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
                throw new DryadLinqException("The stream doesn't support Reset");
            }

            void IDisposable.Dispose()
            {
                if (this.m_reader != null)
                {
                    this.m_reader.Close();
                }
            }
        }
    }
}
