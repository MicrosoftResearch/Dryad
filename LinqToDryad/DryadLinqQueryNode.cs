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
using System.Text;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.CodeDom;
using System.Diagnostics;
using System.Xml;
using System.Data.Linq.Mapping;
using System.Data.Linq;
using Microsoft.Research.DryadLinq.Internal;


namespace Microsoft.Research.DryadLinq
{
    internal enum QueryNodeType
    {
        InputTable,
        OutputTable,
        Aggregate,
        Select,
        SelectMany,
        Where,
        Distinct,
        BasicAggregate,
        GroupBy,
        OrderBy,
        Skip,
        SkipWhile,
        Take,
        TakeWhile,
        Contains,
        Join,
        GroupJoin,
        Union,
        Intersect,
        Except,
        Concat,
        Zip,
        Super,
        RangePartition,
        HashPartition,
        Merge,
        Apply,
        Fork,
        DoWhile,
        Tee,
        Dynamic,
        Dummy
    }

    internal enum AggregateOpType
    {
        Count,
        LongCount,
        Sum,
        Min,
        Max,
        Average,
        Any,
        All,
        First,
        FirstOrDefault,
        Single,
        SingleOrDefault,
        Last,
        LastOrDefault
    }

    internal enum ChannelType
    {
        MemoryFIFO,
        TCPPipe,
        DiskFile
    }

    internal enum ConnectionOpType
    {
        Pointwise,
        CrossProduct
    }

    internal enum AffinityConstraintType
    {
        UseDefault,
        HardConstraint,
        OptimizationConstraint,
        Preference,
        DontCare
    }

    internal abstract class DLinqQueryNode
    {
        private QueryNodeType m_nodeType;
        internal DryadLinqQueryGen m_queryGen;
        private Expression m_queryExpression;
        private List<DLinqQueryNode> m_parents;
        private DLinqQueryNode[] m_children;
        private DLinqSuperNode m_superNode;
        private bool m_isForked;
        protected internal ChannelType m_channelType;
        protected internal ConnectionOpType m_conOpType;
        protected string m_opName;
        internal int m_uniqueId;
        internal string m_vertexEntryMethod;
        protected internal int m_partitionCount;
        internal DataSetInfo m_outputDataSetInfo;
        protected internal DynamicManager m_dynamicManager;
        protected internal List<Pair<ParameterExpression, DLinqQueryNode>> m_referencedQueries;

        internal DLinqQueryNode(QueryNodeType nodeType,
                              DryadLinqQueryGen queryGen,
                              Expression queryExpr,
                              params DLinqQueryNode[] children)
        {
            this.m_nodeType = nodeType;
            this.m_queryGen = queryGen;
            this.m_queryExpression = queryExpr;
            this.m_parents = new List<DLinqQueryNode>(1);
            this.m_children = children;
            foreach (DLinqQueryNode child in children)
            {
                child.Parents.Add(this);
            }
            this.m_superNode = null;
            this.m_isForked = false;
            this.m_uniqueId = DryadLinqQueryGen.StartPhaseId;
            this.m_channelType = ChannelType.DiskFile;
            this.m_conOpType = ConnectionOpType.Pointwise;
            this.m_opName = null;
            this.m_vertexEntryMethod = null;
            this.m_outputDataSetInfo = null;
            this.m_partitionCount = -1;
            this.m_dynamicManager = null;
        }

        internal QueryNodeType NodeType
        {
            get { return this.m_nodeType; }
        }

        /// <summary>
        /// The query generator to use for this query node.
        /// </summary>
        internal DryadLinqQueryGen QueryGen
        {
            get { return this.m_queryGen; }
        }
        
        /// <summary>
        /// (sub)Query expression corresponding to this node.
        /// </summary>
        internal Expression QueryExpression
        {
            get { return this.m_queryExpression; }
        }

        /// <summary>
        /// (sub)Query expression corresponding to this node, after it has been rewritten.
        /// </summary>
        internal virtual Expression RebuildQueryExpression(Expression inputExpr)
        {
            throw new NotSupportedException(SR.CannotRebuildOptimizedQueryExpression);
        }

        /// <summary>
        /// Children of this node: data sources.
        /// </summary>
        internal DLinqQueryNode[] Children
        {
            get { return this.m_children; }
            set { this.m_children = value; }
        }

        /// <summary>
        /// Parents of this node: data consumers.
        /// </summary>
        internal List<DLinqQueryNode> Parents
        {
            get { return this.m_parents; }
        }

        /// <summary>
        /// A SuperNode contains many other elementary nodes inside.
        /// </summary>
        internal DLinqSuperNode SuperNode
        {
            get { return this.m_superNode; }
            set { this.m_superNode = value; }
        }

        /// <summary>
        /// Operation performed by node.
        /// </summary>
        internal string OpName
        {
            get { return this.m_opName; }
        }

        internal ConnectionOpType ConOpType
        {
            get { return this.m_conOpType; }
            set { this.m_conOpType = value; }
        }

        internal ChannelType ChannelType
        {
            get { return this.m_channelType; }
            set { this.m_channelType = value; }
        }

        internal bool UseLargeWriteBuffer
        {
            get {
                if (!StaticConfig.UseLargeBuffer ||
                    this.IsStateful ||
                    this.ChannelType != ChannelType.DiskFile)
                {
                    return false;
                }
                return true;
            }
        }

        /// <summary>
        /// One type for each input connection.
        /// </summary>
        internal virtual Type[] InputTypes
        {
            get {
                Type[] types = new Type[this.Children.Length];
                for (int i = 0; i < types.Length; i++)
                {
                    types[i] = this.Children[i].OutputTypes[0];
                }
                return types;
            }
        }

        /// <summary>
        /// Summary of the output data (static estimate).
        /// </summary>
        internal DataSetInfo OutputDataSetInfo
        {
            get { return this.m_outputDataSetInfo; }
            set { this.m_outputDataSetInfo = value; }
        }

        internal PartitionInfo OutputPartition
        {
            get { return this.m_outputDataSetInfo.partitionInfo; }
        }

        internal int PartitionCount
        {
            get { return this.m_partitionCount; }
        }

        internal bool IsOrderedBy(LambdaExpression expr, object comparer)
        {
            return this.m_outputDataSetInfo.orderByInfo.IsOrderedBy(expr, comparer);
        }

        /// <summary>
        /// Dynamic manager associated with first child.
        /// </summary>
        internal DynamicManager DynamicManager
        {
            get { return this.m_dynamicManager; }
            set { this.m_dynamicManager = value; }
        }

        internal DLinqQueryNode OutputNode
        {
            get {
                DLinqQueryNode node = this;
                if (node is DLinqSuperNode)
                {
                    node = ((DLinqSuperNode)node).RootNode;
                }
                return node;
            }
        }

        internal bool IsDistributeNode
        {
            get {
                DLinqQueryNode curNode = this.OutputNode;
                return (curNode.NodeType == QueryNodeType.RangePartition ||
                        curNode.NodeType == QueryNodeType.HashPartition);
            }
        }

        internal bool IsForked
        {
            get { return this.m_isForked || (this.Parents.Count > 1); }
            set { this.m_isForked = value; }
        }

        internal virtual Int32 InputArity
        {
            get {
                DLinqQueryNode node = this;
                if (node is DLinqDynamicNode)
                {
                    node = ((DLinqDynamicNode)node).GetRealNode(0);
                }
                return node.Children.Length;
            }
        }

        internal Int32 OutputArity
        {
            get {
                DLinqQueryNode node = this.OutputNode;

                if (node is DLinqForkNode)
                {
                    return node.Parents.Count;
                }
                return (node.Parents.Count == 0) ? 0 : 1; 
            }
        }

       internal virtual bool IsHomomorphic
        {
            get { return false; }
        }

        internal virtual bool CanAttachPipeline
        {
            get { return false; }
        }

        internal virtual Pipeline AttachedPipeline
        {
            set { 
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.CannotAttach);
            }
        }

        internal virtual bool ContainsMerge
        {
            get {
                return (this.DynamicManager == DynamicManager.PartialAggregator);
            }
        }

        internal virtual int[] InputPortCounts()
        {
            int[] portCountArray = new int[this.InputArity];
            for (int i = 0; i < portCountArray.Length; i++)
            {
                portCountArray[i] = 1;
            }
            return portCountArray;
        }
        
        internal virtual bool[] KeepInputPortOrders()
        {
            bool[] keepPortOrderArray = new bool[this.InputArity];
            for (int i = 0; i < keepPortOrderArray.Length; i++)
            {
                keepPortOrderArray[i] = false;
            }
            return keepPortOrderArray;
        }
        
        /// <summary>
        /// If true the node should not be pipelined with other stateful nodes.
        /// </summary>
        internal virtual bool IsStateful
        {
            get { return false; }
        }

        internal abstract Type[] OutputTypes { get; }

        // not virtual yet as only SuperNode requires special handling and we don't need generality (yet)
        // Handling for intermediate types is virtual as we anticipate more DryadQueryNodes will require this over time.
        internal void CreateCodeAndMappingsForVertexTypes(bool intermediateTypesOnly)
        {
            // process the output types for this node
            if (!intermediateTypesOnly)
            {
                for (int i = 0; i < this.OutputArity; i++)
                {
                    this.QueryGen.CodeGen.AddDryadCodeForType(OutputTypes[i]);
                }
            }

            //process the intermediate types for this node
            this.CreateCodeAndMappingsForIntermediateTypes();
        }

        //Default behavior: nothing
        //However, some nodes may need to do some codegen/mapping for types that are not their outputs.
        internal virtual void CreateCodeAndMappingsForIntermediateTypes()
        {
        }
        
        internal abstract string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames);

        internal List<Pair<ParameterExpression, DLinqQueryNode>> GetReferencedQueries()
        {
            if (this.m_referencedQueries == null)
            {
                ReferencedQuerySubst subst = new ReferencedQuerySubst(this.QueryGen.ReferencedQueryMap);
                this.GetReferencedQueries(subst);
                this.m_referencedQueries = subst.GetReferencedQueries();
            }
            return this.m_referencedQueries;
        }

        internal virtual void GetReferencedQueries(ReferencedQuerySubst subst)
        {
        }

        internal void AddSideReaders(CodeMemberMethod vertexMethod)
        {
            foreach (Pair<ParameterExpression, DLinqQueryNode> nq in this.GetReferencedQueries())
            {
                string factoryName = this.QueryGen.CodeGen.GetStaticFactoryName(nq.Value.OutputTypes[0]);
                CodeExpression
                    readerExpr = new CodeMethodInvokeExpression(
                                         new CodeArgumentReferenceExpression(DryadLinqCodeGen.VertexEnvName),
                                         "MakeReader",
                                         new CodeArgumentReferenceExpression(factoryName));
                readerExpr = new CodeMethodInvokeExpression(readerExpr, "ToArray");
                // readerExpr = new CodeMethodInvokeExpression(readerExpr, "AsQueryable");
                CodeStatement sideDecl = new CodeVariableDeclarationStatement("var", nq.Key.Name, readerExpr);
                vertexMethod.Statements.Add(sideDecl);
            }
        }
        
        /// <summary>
        /// The summary to show for this node.
        /// </summary>
        /// <param name="builder"></param>
        internal abstract void BuildString(StringBuilder builder);

        // Replace all occurences of oldNode in this.Parents by newNode.
        // Return true iff oldNode is in this.Parents.
        internal bool UpdateParent(DLinqQueryNode oldNode, DLinqQueryNode newNode)
        {
            bool found = false;
            for (int i = 0; i < this.Parents.Count; i++)
            {
                if (Object.ReferenceEquals(oldNode, this.Parents[i]))
                {
                    this.Parents[i] = newNode;
                    found = true;
                }
            }
            if (!found)
            {
                this.Parents.Add(newNode);
            }
            return found;
        }

        // Replace all occurences of oldNode in this.Children by newNode.
        // Return true iff oldNode is in this.Children.
        internal bool UpdateChildren(DLinqQueryNode oldNode, DLinqQueryNode newNode)
        {
            bool found = false;
            for (int i = 0; i < this.Children.Length; i++)
            {
                if (Object.ReferenceEquals(oldNode, this.Children[i]))
                {
                    this.Children[i] = newNode;
                    found = true;
                }
            }
            return found;
        }

        internal DLinqQueryNode InsertTee(bool isForked)
        {
            if (this.OutputArity != 1)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.CannotAddTeeToNode);
            }
            List<DLinqQueryNode> pnodes = new List<DLinqQueryNode>(this.Parents);
            this.Parents.Clear();
            DLinqTeeNode teeNode = new DLinqTeeNode(this.OutputTypes[0], isForked, this.QueryExpression, this);
            teeNode.m_uniqueId = this.m_uniqueId;
            teeNode.Parents.AddRange(pnodes);
            DLinqQueryNode oldNode = this.OutputNode;
            foreach (DLinqQueryNode pn in pnodes)
            {
                pn.UpdateChildren(oldNode, teeNode);
            }
            return teeNode;
        }

        // Return true if this node and child can't be pipelined together.
        private bool CanNotBePipelinedWith(DLinqQueryNode child)
        {
            return ((child is DLinqInputNode) ||
                    (child is DLinqConcatNode) ||
                    (child is DLinqTeeNode) || (child.IsForked) ||
                    (child is DLinqDoWhileNode) ||
                    (child is DLinqDummyNode) ||
                    ((child is DLinqApplyNode) && ((DLinqApplyNode)child).IsWriteToStream));
        }

        // Determine if the current node can be reduced with some of its children
        // into a supernode.
        private bool CanBePipelined()
        {
            if ((this is DLinqInputNode) ||
                (this is DLinqOutputNode) ||
                (this is DLinqMergeNode) ||
                (this is DLinqTeeNode) ||
                (this is DLinqConcatNode) ||
                (this is DLinqDoWhileNode) ||
                (this is DLinqDummyNode))
            {
                return false;
            }
            if ((this is DLinqHashPartitionNode) &&
                ((DLinqHashPartitionNode)this).IsDynamicDistributor)
            {
                return false;
            }
            if ((this is DLinqRangePartitionNode) &&
                ((DLinqRangePartitionNode)this).IsDynamicDistributor)
            {
                return false;
            }
            if ((this is DLinqBasicAggregateNode) && this.ContainsMerge)
            {
                return false;
            }
            if ((this is DLinqAggregateNode) && this.ContainsMerge)
            {
                return false;
            }
            if ((this is DLinqPartitionOpNode) && this.ContainsMerge &&
                (this.Children[0].IsDynamic || this.Children[0].OutputPartition.Count > 1))
            {
                return false;
            }
            if ((this is DLinqApplyNode) &&
                ((DLinqApplyNode)this).IsReadFromStream)
            {
                return false;
            }
            
            bool canBePipelined = false;
            foreach (DLinqQueryNode child in this.Children)
            {
                if (!this.CanNotBePipelinedWith(child))
                {
                    canBePipelined = true;
                    break;
                }
            }
            if (!canBePipelined) return false;

            // Not reducible if this node has a child of distribute node:
            foreach (DLinqQueryNode child in this.Children)
            {
                if (child.IsDistributeNode) return false;
            }

            // Not reducible if this node and one of its children are stateful:
            bool hasState = this.IsStateful;
            foreach (DLinqQueryNode child in this.Children)
            {
                if (child.IsStateful)
                {
                    if (hasState) return false;
                    hasState = child.IsStateful;
                }
            }
            return true;
        }

        internal DLinqQueryNode PipelineReduce()
        {
            if (!this.CanBePipelined())
            {
                return this;
            }

            DLinqQueryNode[] nodeChildren = this.Children;
            DLinqSuperNode resNode = new DLinqSuperNode(this);
            List<DLinqQueryNode> childList = new List<DLinqQueryNode>();
            for (int i = 0; i < nodeChildren.Length; i++)
            {
                DLinqQueryNode child = nodeChildren[i];
                if (this.CanNotBePipelinedWith(child))
                {
                    childList.Add(child);
                    bool found = child.UpdateParent(this, resNode);
                }
                else
                {
                    if (child is DLinqSuperNode)
                    {
                        DLinqSuperNode superChild = (DLinqSuperNode)child;
                        nodeChildren[i] = superChild.RootNode;
                        superChild.SwitchTo(resNode);
                    }
                    else
                    {
                        child.SuperNode = resNode;
                    }

                    // Fix the child's children
                    foreach (DLinqQueryNode child1 in child.Children)
                    {
                        childList.Add(child1);
                        bool found = child1.UpdateParent(child, resNode);
                    }
                }
            }

            DLinqQueryNode[] resChildren = new DLinqQueryNode[childList.Count];
            for (int i = 0; i < resChildren.Length; i++)
            {
                resChildren[i] = childList[i];
            }
            resNode.Children = resChildren;
            resNode.OutputDataSetInfo = resNode.RootNode.OutputDataSetInfo;
            return resNode;
        }

        // This can only be used before super nodes are formed.
        internal bool IsDynamic
        {
            get {
                if (this.m_dynamicManager.ManagerType == DynamicManagerType.Splitter ||
                    this.m_dynamicManager.ManagerType == DynamicManagerType.PartialAggregator ||
                    this.m_dynamicManager.ManagerType == DynamicManagerType.HashDistributor)
                {
                    return true;
                }
                if (this is DLinqMergeNode)
                {
                    DLinqQueryNode child = this.Children[0];
                    if (child is DLinqHashPartitionNode)
                    {
                        return ((DLinqHashPartitionNode)child).IsDynamicDistributor;
                    }
                    if (child is DLinqRangePartitionNode)
                    {
                        return ((DLinqRangePartitionNode)child).IsDynamicDistributor;
                    }
                }
                if (this is DLinqConcatNode)
                {
                    foreach (DLinqQueryNode child in this.Children)
                    {
                        if (child.IsDynamic) return true;
                    }
                }
                return false;
            }
        }

        // This can only be used before super nodes are formed.        
        protected DynamicManager InferDynamicManager()
        {
            DynamicManager dynamicMan = DynamicManager.None;
            DLinqQueryNode child = this.Children[0];
            if (child is DLinqInputNode)
            {
                if (((DLinqInputNode)child).Table.IsDynamic && this.Children.Length == 1)
                {
                    dynamicMan = DynamicManager.PartialAggregator;
                }
            }
            else if (child is DLinqMergeNode)
            {
                if (((DLinqMergeNode)child).IsSplitting)
                {
                    dynamicMan = DynamicManager.Splitter;
                }
            }
            else if (child is DLinqConcatNode)
            {
                foreach (DLinqQueryNode cc in child.Children)
                {
                    if (cc is DLinqInputNode)
                    {
                        if (((DLinqInputNode)cc).Table.IsDynamic)
                        {
                            dynamicMan = DynamicManager.Splitter;
                            break;
                        }
                    }
                    else
                    {
                        DynamicManager ccdm = cc.InferDynamicManager();
                        if (ccdm.ManagerType == DynamicManagerType.Splitter ||
                            ccdm.ManagerType == DynamicManagerType.PartialAggregator)
                        {
                            dynamicMan = DynamicManager.Splitter;
                            break;
                        }
                    }
                }
            }
            else
            {
                DynamicManager cdm = child.DynamicManager;
                if (cdm.ManagerType == DynamicManagerType.Splitter ||
                    cdm.ManagerType == DynamicManagerType.PartialAggregator)
                {
                    dynamicMan = DynamicManager.Splitter;
                }
            }
            return dynamicMan;
        }

        internal virtual int AddToQueryPlan(XmlDocument queryDoc,
                                            XmlElement queryPlan,
                                            HashSet<int> seen)
        {
            if (!seen.Contains(this.m_uniqueId))
            {
                var refQueries = this.GetReferencedQueries();
                int[] cids = new int[refQueries.Count + this.Children.Length];
                for (int i = 0; i < refQueries.Count; i++)
                {
                    cids[i] = refQueries[i].Value.AddToQueryPlan(queryDoc, queryPlan, seen);
                }
                for (int i = 0; i < this.Children.Length; i++)
                {
                    cids[i+refQueries.Count] = this.Children[i].AddToQueryPlan(queryDoc, queryPlan, seen);
                }
                if (!seen.Contains(this.m_uniqueId))
                {
                    seen.Add(this.m_uniqueId);
                    XmlElement vertexElem = this.CreateVertexElem(queryDoc, this.m_uniqueId, this.m_vertexEntryMethod);
                    queryPlan.AppendChild(vertexElem);

                    string dllName = this.QueryGen.CodeGen.GetDryadLinqDllName();
                    XmlElement entryElem = DryadQueryDoc.CreateVertexEntryElem(queryDoc, dllName, this.m_vertexEntryMethod);
                    vertexElem.AppendChild(entryElem);

                    XmlElement childrenElem = this.CreateVertexChildrenElem(queryDoc, cids);
                    vertexElem.AppendChild(childrenElem);

                    if (this.OutputNode is DLinqForkNode)
                    {
                        foreach (DLinqQueryNode pnode in this.Parents)
                        {
                            pnode.AddToQueryPlan(queryDoc, queryPlan, seen);
                        }
                    }
                }
            }
            return this.m_uniqueId;
        }

        protected XmlElement CreateVertexElem(XmlDocument queryDoc, int uid, string name)
        {
            XmlElement vertexElem = queryDoc.CreateElement("Vertex");

            XmlElement elem = queryDoc.CreateElement("UniqueId");
            elem.InnerText = Convert.ToString(uid);
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("Type");
            elem.InnerText = this.NodeType.ToString();
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("Name");
            elem.InnerText = name;
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("Explain");
            StringBuilder plan = new StringBuilder();
            DryadLinqQueryExplain.ExplainNode(plan, this);
            XmlCDataSection data = queryDoc.CreateCDataSection(plan.ToString());
            elem.AppendChild(data);
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("Partitions");
            elem.InnerText = Convert.ToString(this.m_partitionCount);
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("ChannelType");
            elem.InnerText = this.ChannelType.ToString();
            vertexElem.AppendChild(elem);

            elem = queryDoc.CreateElement("ConnectionOperator");
            elem.InnerText = this.m_conOpType.ToString();
            vertexElem.AppendChild(elem);

            elem = this.m_dynamicManager.CreateElem(queryDoc);
            vertexElem.AppendChild(elem);

            return vertexElem;
        }

        protected XmlElement CreateVertexChildrenElem(XmlDocument queryDoc, params int[] cids)
        {
            XmlElement childrenElem = queryDoc.CreateElement("Children");
            for (int i = 0; i < cids.Length; i++)
            {
                XmlElement childElem = queryDoc.CreateElement("Child");
                childrenElem.AppendChild(childElem);

                XmlElement elem = queryDoc.CreateElement("UniqueId");
                elem.InnerText = Convert.ToString(cids[i]);
                childElem.AppendChild(elem);

                elem = queryDoc.CreateElement("AffinityConstraint");
                elem.InnerText = AffinityConstraintType.UseDefault.ToString();
                childElem.AppendChild(elem);
            }
            return childrenElem;
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            this.BuildString(builder);
            return builder.ToString();
        }
    }

    internal class DLinqInputNode : DLinqQueryNode
    {
        private DryadLinqQuery m_table;

        internal DLinqInputNode(DryadLinqQueryGen queryGen, ConstantExpression queryExpr)
            : base(QueryNodeType.InputTable, queryGen, queryExpr)
        {
            this.m_table = queryExpr.Value as DryadLinqQuery;
            if (this.m_table == null)
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.UnknownError, SR.InputMustBeDryadLinqSource, queryExpr);
            }
            if (!queryGen.Context.Equals(this.m_table.Context))
            {
                throw new DryadLinqException("DryadLinqContext was changed while constructing this query.");
            }
            if (TypeSystem.IsTypeOrAnyGenericParamsAnonymous(queryExpr.Type.GetGenericArguments()[0]))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.InputTypeCannotBeAnonymous,
                                                SR.InputTypeCannotBeAnonymous,
                                                queryExpr);
            }
            this.m_opName = "Input";
            this.m_outputDataSetInfo = ((DryadLinqQuery)this.m_table).DataSetInfo;
            this.m_partitionCount = this.m_outputDataSetInfo.partitionInfo.Count;
            this.m_dynamicManager = DynamicManager.None;
        }

        internal override Type[] OutputTypes
        {
            get {
                Type[] typeArgs = this.QueryExpression.Type.GetGenericArguments();
                Debug.Assert(typeArgs != null && typeArgs.Length == 1);
                return new Type[] { typeArgs[0] };
            }
        }

        //@@TODO[P2]: rename to Query.  Also look for other places.
        internal DryadLinqQuery Table
        {
            get { return this.m_table; }
        }

        internal override int AddToQueryPlan(XmlDocument queryDoc,
                                             XmlElement queryPlan,
                                             HashSet<int> seen)
        {
            if (!seen.Contains(this.m_uniqueId))
            {
                XmlElement vertexElem = this.CreateVertexElem(queryDoc, this.m_uniqueId, this.m_vertexEntryMethod);
                queryPlan.AppendChild(vertexElem);

                XmlElement storageElem = queryDoc.CreateElement("StorageSet");
                vertexElem.AppendChild(storageElem);

                XmlElement elem = queryDoc.CreateElement("Type");
                Uri srcUri = this.m_table.DataSourceUri;
                elem.InnerText = DataPath.GetStorageType(srcUri);
                storageElem.AppendChild(elem);

                elem = queryDoc.CreateElement("SourceURI");
                elem.InnerText = srcUri.AbsoluteUri;
                storageElem.AppendChild(elem);
            }

            return this.m_uniqueId;
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new NotImplementedException();
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append(this.m_table.ToString());
        }
    }
    
    internal class DLinqOutputNode : DLinqQueryNode
    {
        private Uri m_outputUri;
        private Type m_outputType;
        private CompressionScheme m_outputCompressionScheme;
        private bool m_isTempOutput;
        private DryadLinqContext m_context;
        
        internal DLinqOutputNode(DryadLinqContext context,
                                 Uri outputUri,
                                 bool isTempOutput,
                                 CompressionScheme outputScheme,
                                 Expression queryExpr,
                                 DLinqQueryNode child)
            : base(QueryNodeType.OutputTable, child.QueryGen, queryExpr, child)
        {
            if (TypeSystem.IsTypeOrAnyGenericParamsAnonymous(child.OutputTypes[0]))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.OutputTypeCannotBeAnonymous,
                                                SR.OutputTypeCannotBeAnonymous,
                                                queryExpr);
            }
            this.m_opName = "Output";
            this.m_context = context;
            this.m_outputUri = outputUri;
            this.m_outputType = child.OutputTypes[0];
            this.m_outputDataSetInfo = child.OutputDataSetInfo;
            this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;
            this.m_dynamicManager = DynamicManager.Splitter;
            this.m_outputCompressionScheme = outputScheme;
            this.m_isTempOutput = isTempOutput;
        }

        internal override Type[] InputTypes
        {
            get { return new Type[] { this.m_outputType }; }
        }

        internal override Type[] OutputTypes
        {
            get { return new Type[] { this.m_outputType }; }
        }

        internal Uri OutputUri
        {
            get { return this.m_outputUri; }
        }

        internal CompressionScheme OutputCompressionScheme
        {
            get { return this.m_outputCompressionScheme; }
        }
        
        internal override int AddToQueryPlan(XmlDocument queryDoc,
                                             XmlElement queryPlan,
                                             HashSet<int> seen)
        {
            int cid = this.Children[0].AddToQueryPlan(queryDoc, queryPlan, seen);
            if (!seen.Contains(this.m_uniqueId))
            {
                seen.Add(this.m_uniqueId);
                XmlElement vertexElem = this.CreateVertexElem(queryDoc, this.m_uniqueId, this.m_vertexEntryMethod);
                queryPlan.AppendChild(vertexElem);

                XmlElement storageElem = queryDoc.CreateElement("StorageSet");
                vertexElem.AppendChild(storageElem);

                XmlElement elem = queryDoc.CreateElement("Type");
                elem.InnerText = DataPath.GetStorageType(this.m_outputUri);
                storageElem.AppendChild(elem);

                string sinkPath = this.m_outputUri.AbsoluteUri;
                if (DataPath.IsPartitionedFile(this.m_outputUri))
                {
                    string uncPath = this.m_context.PartitionUncPath;
                    if (uncPath == null)
                    {
                        uncPath = Path.GetDirectoryName(this.m_outputUri.LocalPath).TrimStart('\\', '/');
                    }
                    elem = queryDoc.CreateElement("PartitionUncPath");
                    elem.InnerText = Path.Combine(uncPath,
                                                  DryadLinqUtil.MakeUniqueName(),
                                                  Path.GetFileNameWithoutExtension(sinkPath));
                    storageElem.AppendChild(elem);
                }

                elem = queryDoc.CreateElement("SinkURI");
                elem.InnerText = sinkPath;
                storageElem.AppendChild(elem);

                elem = queryDoc.CreateElement("IsTemporary");
                elem.InnerText = this.m_isTempOutput.ToString();
                storageElem.AppendChild(elem);

                DryadLinqMetaData metaData = DryadLinqMetaData.Get(this.m_context, this);
                
                elem = queryDoc.CreateElement("OutputCompressionScheme");
                elem.InnerText = ((int)metaData.CompressionScheme).ToString();
                storageElem.AppendChild(elem);

                elem = queryDoc.CreateElement("RecordType");
                elem.InnerText = metaData.ElemType.AssemblyQualifiedName;
                storageElem.AppendChild(elem);

                XmlElement childrenElem = this.CreateVertexChildrenElem(queryDoc, cid);
                vertexElem.AppendChild(childrenElem);
            }
            return this.m_uniqueId;
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new InvalidOperationException("Internal error.");
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[[Table: " + this.m_outputUri + "], ");
            this.Children[0].BuildString(builder);
            builder.Append("]");
        }
    }

    internal class DLinqWhereNode : DLinqQueryNode
    {
        private LambdaExpression m_whereExpr;
        private LambdaExpression m_whereExpr1;

        internal DLinqWhereNode(LambdaExpression whereExpr, 
                                Expression queryExpr, 
                                DLinqQueryNode child)
            : base(QueryNodeType.Where, child.QueryGen, queryExpr, child)
        {
            this.m_whereExpr = whereExpr;

            //If indexed version and the index is a long, we will use opName=LongWhere
            if (this.m_whereExpr.Parameters.Count() == 2 &&
                this.m_whereExpr.Parameters[1].Type == typeof(long))
            {
                this.m_opName = "LongWhere";
            }
            else
            {
                this.m_opName = "Where";
            }

            this.m_partitionCount = child.OutputPartition.Count;
            this.m_outputDataSetInfo = new DataSetInfo(child.OutputDataSetInfo);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override bool IsHomomorphic
        {
            get { return this.m_whereExpr.Type.GetGenericArguments().Length == 2; }
        }
        
        internal override Expression RebuildQueryExpression(Expression inputExpr)
        {
            return Expression.Call(typeof(System.Linq.Enumerable),
                                   "Where",
                                   new Type[] { this.InputTypes[0] },
                                   inputExpr, m_whereExpr1);
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal bool OrderPreserving()
        {
            return (this.m_queryGen.Context.SelectOrderPreserving ||
                    this.OutputDataSetInfo.orderByInfo.IsOrdered);
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression whereExpr = new CodeMethodInvokeExpression(
                                                  DryadLinqCodeGen.DLVTypeExpr,
                                                  this.OpName,
                                                  new CodeVariableReferenceExpression(readerNames[0]),
                                                  this.QueryGen.CodeGen.MakeExpression(this.m_whereExpr1),
                                                  new CodePrimitiveExpression(this.OrderPreserving()));
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", whereExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_whereExpr1 == null)
            {
                this.m_whereExpr1 = (LambdaExpression)subst.Visit(this.m_whereExpr);
            }
        }
        
        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_whereExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }

        /// <summary>
        /// The expression performing the filtering.
        /// </summary>
        internal LambdaExpression WhereExpression
        {
            get { return this.m_whereExpr; }
        }

        internal LambdaExpression WhereExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_whereExpr1;
            }
        }
    }

    internal class DLinqSelectNode : DLinqQueryNode
    {
        private LambdaExpression m_selectExpr;
        private LambdaExpression m_resultSelectExpr;
        private LambdaExpression m_selectExpr1;
        private LambdaExpression m_resultSelectExpr1;

        internal DLinqSelectNode(QueryNodeType nodeType,
                                 LambdaExpression selectExpr,
                                 LambdaExpression resultSelectExpr,
                                 Expression queryExpr,
                                 DLinqQueryNode child)
            : base(nodeType, child.QueryGen, queryExpr, child)
        {
            Debug.Assert(nodeType == QueryNodeType.Select || nodeType == QueryNodeType.SelectMany);
            this.m_selectExpr = selectExpr;
            this.m_resultSelectExpr = resultSelectExpr;

            //If indexed version and the index is a long, we will use opName=DryadLong.  
            if (this.m_selectExpr.Parameters.Count() == 2 &&
                this.m_selectExpr.Parameters[1].Type == typeof(long))
            {
                this.m_opName = "Long" + nodeType;
            }
            else
            {
                this.m_opName = nodeType.ToString();
            }

            this.m_partitionCount = child.OutputPartition.Count;
            this.m_outputDataSetInfo = this.ComputeOutputDataSetInfo();

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal LambdaExpression SelectExpression 
        {
            get { return this.m_selectExpr; } 
        }

        internal LambdaExpression SelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_selectExpr1; 
            }
        }
        
        internal LambdaExpression ResultSelectExpression
        {
            get { return this.m_resultSelectExpr; } 
        }

        internal LambdaExpression ResultSelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_resultSelectExpr1; 
            }
        }

        internal override bool IsHomomorphic
        {
            get { return this.m_selectExpr.Type.GetGenericArguments().Length == 2; }
        }
        
        internal override Expression RebuildQueryExpression(Expression inputExpr)
        {
            string methodName;
            Type[] typeArgs;
            Expression[] args;
            if (this.OpName == "Select")
            {
                methodName = "Select";
                typeArgs = new Type[] { this.InputTypes[0], this.OutputTypes[0] };
                args = new Expression[] { inputExpr, this.m_selectExpr1 };
            }
            else
            {
                methodName = "SelectMany";
                if (this.m_resultSelectExpr1 == null)
                {
                    typeArgs = new Type[] { this.InputTypes[0], this.OutputTypes[0] };
                    args = new Expression[] { inputExpr, this.m_selectExpr1 };
                }
                else
                {
                    Type collectionType = this.m_resultSelectExpr1.Type.GetGenericArguments()[1];
                    typeArgs = new Type[] { this.InputTypes[0], collectionType, this.OutputTypes[0] };
                    args = new Expression[] { inputExpr, this.m_selectExpr1, this.m_resultSelectExpr1 };
                }
            }
            return Expression.Call(typeof(System.Linq.Enumerable), methodName, typeArgs, args);
        }

        internal override Type[] OutputTypes
        {
            get {
                Type resType;
                if (this.m_resultSelectExpr == null)
                {
                    Type[] argTypes = this.m_selectExpr.Type.GetGenericArguments();
                    resType = (argTypes.Length == 3) ? argTypes[2] : argTypes[1];
                    if (this.OpName == "SelectMany" || this.OpName == "LongSelectMany")
                    {
                        resType = resType.GetGenericArguments()[0];
                    }
                }
                else
                {
                    resType = this.m_resultSelectExpr.Body.Type;
                }
                return new Type[] { resType };
            }
        }

        private DataSetInfo ComputeOutputDataSetInfo()
        {
            DataSetInfo childInfo = this.Children[0].OutputDataSetInfo;

            ParameterExpression param = this.m_selectExpr.Parameters[0];
            PartitionInfo pinfo = childInfo.partitionInfo.Rewrite(this.m_selectExpr, param);
            OrderByInfo oinfo = childInfo.orderByInfo.Rewrite(this.m_selectExpr, param);
            DistinctInfo dinfo = DataSetInfo.NoDistinct;
            DistinctAttribute attrib1 = AttributeSystem.GetDistinctAttrib(this.m_selectExpr);
            if (attrib1 != null && (!attrib1.MustBeDistinct || childInfo.distinctInfo.IsDistinct()))
            {
                dinfo = DistinctInfo.Create(attrib1.Comparer, this.OutputTypes[0]);
            }

            return new DataSetInfo(pinfo, oinfo, dinfo);
        }

        internal bool OrderPreserving()
        {
            return (m_queryGen.Context.SelectOrderPreserving ||
                    this.OutputDataSetInfo.orderByInfo.IsOrdered);
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression selectorExpr = this.QueryGen.CodeGen.MakeExpression(this.m_selectExpr1);
            CodeExpression selectExpr;
            if (this.m_resultSelectExpr1 == null)
            {
                selectExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                            this.OpName,
                                                            new CodeVariableReferenceExpression(readerNames[0]),
                                                            selectorExpr,
                                                            new CodePrimitiveExpression(this.OrderPreserving()));
            }
            else
            {
                selectExpr = new CodeMethodInvokeExpression(
                                        DryadLinqCodeGen.DLVTypeExpr,
                                        this.OpName,
                                        new CodeVariableReferenceExpression(readerNames[0]),
                                        selectorExpr,
                                        this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr1),
                                        new CodePrimitiveExpression(this.OrderPreserving()));
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", selectExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_selectExpr1 == null)
            {
                this.m_selectExpr1 = (LambdaExpression)subst.Visit(this.m_selectExpr);
                if (this.m_resultSelectExpr != null)
                {
                    this.m_resultSelectExpr1 = (LambdaExpression)subst.Visit(this.m_resultSelectExpr);
                }
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_selectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }
    }

    internal class DLinqZipNode : DLinqQueryNode
    {
        private LambdaExpression m_selectExpr;
        private LambdaExpression m_selectExpr1;

        internal DLinqZipNode(LambdaExpression selectExpr,
                              Expression queryExpr,
                              DLinqQueryNode child1,
                              DLinqQueryNode child2)
            : base(QueryNodeType.Zip, child1.QueryGen, queryExpr, child1, child2)
        {
            this.m_opName = "Zip";
            this.m_selectExpr = selectExpr;
            this.m_partitionCount = child1.OutputPartition.Count;
            this.m_outputDataSetInfo = this.ComputeOutputDataSetInfo();

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal LambdaExpression SelectExpression
        {
            get { return this.m_selectExpr; }
        }

        internal LambdaExpression SelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_selectExpr1;
            }
        }

        internal override Type[] OutputTypes
        {
            get {
                Type resType = this.m_selectExpr.Body.Type;
                return new Type[] { resType };
            }
        }
        
        private DataSetInfo ComputeOutputDataSetInfo()
        {
            PartitionInfo pinfo = new RandomPartition(this.m_partitionCount);
            OrderByInfo oinfo = DataSetInfo.NoOrderBy;
            DistinctInfo dinfo = DataSetInfo.NoDistinct;
            return new DataSetInfo(pinfo, oinfo, dinfo);
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            bool orderPreserving = false;
            CodeExpression selectorExpr = this.QueryGen.CodeGen.MakeExpression(this.m_selectExpr1);
            CodeExpression selectExpr = new CodeMethodInvokeExpression(
                                                  DryadLinqCodeGen.DLVTypeExpr,
                                                  this.OpName,
                                                  new CodeVariableReferenceExpression(readerNames[0]),
                                                  new CodeVariableReferenceExpression(readerNames[1]),
                                                  selectorExpr,
                                                  new CodePrimitiveExpression(orderPreserving));
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", selectExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_selectExpr1 == null)
            {
                this.m_selectExpr1 = (LambdaExpression)subst.Visit(this.m_selectExpr);
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            this.Children[1].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_selectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }
    }

    internal class DLinqOrderByNode : DLinqQueryNode
    {
        private LambdaExpression m_keySelectExpr;
        private LambdaExpression m_keySelectExpr1;
        private Expression m_comparerExpr;
        private bool m_isDescending;
        private object m_comparer;
        private int m_comparerIdx;

        internal DLinqOrderByNode(LambdaExpression keySelectExpr,
                                  Expression comparerExpr,
                                  bool isDescending,
                                  Expression queryExpr,
                                  DLinqQueryNode child)
            : base(QueryNodeType.OrderBy, child.QueryGen, queryExpr, child)
        {
            this.m_keySelectExpr = keySelectExpr;
            this.m_comparerExpr = comparerExpr;
            this.m_isDescending = isDescending;
            this.m_opName = "Sort";

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(this.m_comparer);
            }

            this.m_partitionCount = child.OutputPartition.Count;

            this.m_outputDataSetInfo = new DataSetInfo(child.OutputDataSetInfo);
            Type[] typeArgs = this.KeySelectExpression.Type.GetGenericArguments();
            this.m_outputDataSetInfo.orderByInfo = OrderByInfo.Create(this.KeySelectExpression,
                                                                      this.m_comparer,
                                                                      this.m_isDescending,
                                                                      typeArgs[1]);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override bool IsStateful
        {
            get { return true; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal LambdaExpression KeySelectExpression
        {
            get { return this.m_keySelectExpr; }
        }

        internal LambdaExpression KeySelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_keySelectExpr1;
            }
        }
        
        internal bool IsDescending
        {
            get { return this.m_isDescending; }
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal object Comparer
        {
            get { return this.m_comparer; }
        }

        internal override void CreateCodeAndMappingsForIntermediateTypes()
        {
            // External sort uses serializers directly, so we must process this type
            // even if it appears inside a super-node and would otherwise not be serialized.
            this.QueryGen.CodeGen.AddDryadCodeForType(this.InputTypes[0]);
        }
        
        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getCall = new CodeMethodInvokeExpression(
                                                    new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                    "Get",
                                                    new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType = typeof(IComparer<>).MakeGenericType(typeArgs[0]);
                comparerArg = new CodeCastExpression(comparerType, getCall);
            }

            bool isIdentityFunc = IdentityFunction.IsIdentity(this.m_keySelectExpr1);
            string factoryName = this.QueryGen.CodeGen.GetStaticFactoryName(this.OutputTypes[0]);
            CodeExpression orderByExpr = new CodeMethodInvokeExpression(
                                                    DryadLinqCodeGen.DLVTypeExpr,
                                                    this.OpName,
                                                    new CodeVariableReferenceExpression(readerNames[0]),
                                                    this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                    comparerArg,
                                                    new CodePrimitiveExpression(this.m_isDescending),
                                                    new CodePrimitiveExpression(isIdentityFunc),
                                                    new CodeArgumentReferenceExpression(factoryName));

            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", orderByExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_keySelectExpr1 == null)
            {
                this.m_keySelectExpr1 = (LambdaExpression)subst.Visit(this.m_keySelectExpr);
            }
        }
        
        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_keySelectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            if (this.m_comparerExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqGroupByNode : DLinqQueryNode
    {
        private LambdaExpression m_keySelectExpr;
        private LambdaExpression m_elemSelectExpr;
        private LambdaExpression m_resSelectExpr;
        private LambdaExpression m_keySelectExpr1;
        private LambdaExpression m_elemSelectExpr1;
        private LambdaExpression m_resSelectExpr1;
        private LambdaExpression m_seedExpr;
        private LambdaExpression m_accumulatorExpr;
        private LambdaExpression m_recursiveAccumulatorExpr;
        private Expression m_comparerExpr;
        private object m_comparer;
        private int m_comparerIdx;
        private bool m_isPartial;

        internal DLinqGroupByNode(string opName,
                                  LambdaExpression keySelectExpr,
                                  LambdaExpression elemSelectExpr,
                                  LambdaExpression resSelectExpr,
                                  LambdaExpression seedExpr,
                                  LambdaExpression accumulateExpr,
                                  LambdaExpression recursiveAccumulatorExpr,
                                  Expression comparerExpr,
                                  bool isPartial,
                                  Expression queryExpr,
                                  DLinqQueryNode child)
            : base(QueryNodeType.GroupBy, child.QueryGen, queryExpr, child)
        {
            Debug.Assert(opName == "GroupBy" || opName == "OrderedGroupBy");
            this.m_keySelectExpr = keySelectExpr;
            this.m_elemSelectExpr = elemSelectExpr;
            this.m_resSelectExpr = resSelectExpr;
            this.m_seedExpr = seedExpr;
            this.m_accumulatorExpr = accumulateExpr;
            this.m_recursiveAccumulatorExpr = recursiveAccumulatorExpr;
            this.m_comparerExpr = comparerExpr;
            this.m_isPartial = isPartial;
            this.m_opName = opName;

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(this.m_comparer);
            }

            this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;
            this.m_outputDataSetInfo = this.ComputeOutputDataSetInfo(isPartial);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override bool IsStateful
        {
            get {
                return (this.m_opName == "GroupBy" && !this.m_isPartial);
            }
        }

        internal override Type[] OutputTypes
        {
            get {
                Type keyType = this.m_keySelectExpr.Type.GetGenericArguments()[1];
                if (this.m_seedExpr != null)
                {
                    Type elemType = this.m_seedExpr.Body.Type;
                    return new Type[] { typeof(Pair<,>).MakeGenericType(keyType, elemType) };
                }
                else if (this.m_resSelectExpr == null)
                {
                    // Note: The output type is the IGrouping interface.
                    Type elemType = this.Children[0].OutputTypes[0];
                    if (this.m_elemSelectExpr != null)
                    {
                        elemType = this.m_elemSelectExpr.Type.GetGenericArguments()[1];
                    }
                    Type groupingType = typeof(IGrouping<,>).MakeGenericType(keyType, elemType);
                    return new Type[] { groupingType };
                }
                else
                {
                    // Get the output type from the result selector expression
                    Type[] typeArgs = this.m_resSelectExpr.Type.GetGenericArguments();
                    return new Type[] { typeArgs[2] };
                }
            }
        }

        private DataSetInfo ComputeOutputDataSetInfo(bool isLocalReduce)
        {
            // TBD: could do a bit better with DistinctInfo.
            DataSetInfo childInfo = this.Children[0].OutputDataSetInfo;

            if (isLocalReduce)
            {
                // Partial aggregation node. No need to do anything.
                PartitionInfo pinfo = new RandomPartition(this.m_partitionCount);
                OrderByInfo oinfo = DataSetInfo.NoOrderBy;
                DistinctInfo dinfo = DataSetInfo.NoDistinct;
                return new DataSetInfo(pinfo, oinfo, dinfo);
            }
            else if (this.m_resSelectExpr == null || this.m_seedExpr != null)
            {
                // Build the new key selection expression (based on group key):
                ParameterExpression param = Expression.Parameter(this.OutputTypes[0], "g");
                PropertyInfo propInfo = param.Type.GetProperty("Key");
                Expression body = Expression.Property(param, propInfo);
                Type dType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                LambdaExpression keySelExpr = Expression.Lambda(dType, body, param);

                PartitionInfo pinfo = childInfo.partitionInfo.Create(keySelExpr);
                OrderByInfo oinfo = DataSetInfo.NoOrderBy;
                if (this.m_opName == "OrderedGroupBy")
                {
                    oinfo = childInfo.orderByInfo.Create(keySelExpr);
                }
                DistinctInfo dinfo = DataSetInfo.NoDistinct;
                return new DataSetInfo(pinfo, oinfo, dinfo);
            }
            else
            {
                ParameterExpression param = Expression.Parameter(this.m_keySelectExpr.Body.Type, "k");
                Type dType = typeof(Func<,>).MakeGenericType(param.Type, param.Type);
                LambdaExpression keySelExpr = Expression.Lambda(dType, param, param);
                PartitionInfo pinfo = childInfo.partitionInfo.Create(keySelExpr);

                pinfo = pinfo.Rewrite(this.m_resSelectExpr, this.m_resSelectExpr.Parameters[0]);
                OrderByInfo oinfo = DataSetInfo.NoOrderBy;
                if (this.m_opName == "OrderedGroupBy")
                {
                    oinfo = childInfo.orderByInfo.Create(keySelExpr);
                    oinfo = oinfo.Rewrite(this.m_resSelectExpr, param);
                }
                DistinctInfo dinfo = DataSetInfo.NoDistinct;
                return new DataSetInfo(pinfo, oinfo, dinfo);
            }
        }

        internal LambdaExpression KeySelectExpression
        {
            get { return this.m_keySelectExpr; }
        }

        internal LambdaExpression KeySelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_keySelectExpr1; 
            }
        }

        internal LambdaExpression ElemSelectExpression
        {
            get { return this.m_elemSelectExpr; }
        }

        internal LambdaExpression ElemSelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_elemSelectExpr1;
            }
        }

        internal LambdaExpression ResSelectExpression
        {
            get { return this.m_resSelectExpr; }
        }

        internal LambdaExpression ResSelectExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_resSelectExpr1; 
            }
        }

        internal LambdaExpression SeedExpression
        {
            get { return this.m_seedExpr; }
        }

        internal LambdaExpression AccumulatorExpression
        {
            get { return this.m_accumulatorExpr; }
        }

        internal LambdaExpression RecursiveAccumulatorExpression
        {
            get { return this.m_recursiveAccumulatorExpr; }
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            if (this.m_resSelectExpr != null)
            {
                this.QueryGen.CodeGen.AddDryadCodeForType(this.m_resSelectExpr.Body.Type);
            }

            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getCall = new CodeMethodInvokeExpression(
                                                    new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                    "Get",
                                                    new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                comparerArg = new CodeCastExpression(comparerType, getCall);
            }

            CodeExpression groupByExpr;
            if (this.m_seedExpr == null)
            {
                if (this.m_elemSelectExpr == null)
                {
                    if (this.m_resSelectExpr == null)
                    {
                        groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 comparerArg);
                    }
                    else
                    {
                        groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_resSelectExpr1),
                                                 comparerArg);
                    }
                }
                else
                {
                    if (this.m_resSelectExpr == null)
                    {
                        groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_elemSelectExpr1),
                                                 comparerArg);
                    }
                    else
                    {
                        groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_elemSelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_resSelectExpr1),
                                                 comparerArg);
                    }
                }
            }
            else
            {
                // m_seedExpr != null
                if (this.m_elemSelectExpr == null)
                {
                    groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_seedExpr),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_accumulatorExpr),
                                                 comparerArg,
                                                 new CodePrimitiveExpression(this.m_isPartial));
                }
                else
                {
                    groupByExpr = new CodeMethodInvokeExpression(
                                                 DryadLinqCodeGen.DLVTypeExpr,
                                                 this.OpName,
                                                 new CodeVariableReferenceExpression(readerNames[0]),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_elemSelectExpr1),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_seedExpr),
                                                 this.QueryGen.CodeGen.MakeExpression(this.m_accumulatorExpr),
                                                 comparerArg,
                                                 new CodePrimitiveExpression(this.m_isPartial));
                }
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", groupByExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_keySelectExpr1 == null)
            {
                this.m_keySelectExpr1 = (LambdaExpression)subst.Visit(this.m_keySelectExpr);
                if (this.m_elemSelectExpr != null)
                {
                    this.m_elemSelectExpr1 = (LambdaExpression)subst.Visit(this.m_elemSelectExpr);
                }
                if (this.m_resSelectExpr != null)
                {
                    this.m_resSelectExpr1 = (LambdaExpression)subst.Visit(this.m_resSelectExpr);
                }
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_keySelectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            if (this.m_elemSelectExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_elemSelectExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            if (this.m_comparerExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqPartitionOpNode : DLinqQueryNode
    {
        private Expression m_controlExpression;
        private bool m_isFirstStage;
        private int m_count;

        internal DLinqPartitionOpNode(string opName,
                                      QueryNodeType nodeType,
                                      Expression controlExpr,
                                      bool isFirstStage,
                                      Expression queryExpr,
                                      DLinqQueryNode child)
            : base(nodeType, child.QueryGen, queryExpr, child)
        {
            this.m_controlExpression = controlExpr;
            this.m_isFirstStage = isFirstStage;
            this.m_opName = opName;
            
            this.m_count = -1;
            if (nodeType == QueryNodeType.Take || nodeType == QueryNodeType.Skip)
            {
                ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
                this.m_count = evaluator.Eval(controlExpr);
            }

            DataSetInfo childInfo = child.OutputDataSetInfo;
            if (isFirstStage)
            {
                this.m_partitionCount = child.OutputPartition.Count;
                this.m_outputDataSetInfo = new DataSetInfo(child.OutputDataSetInfo);
                this.m_dynamicManager = this.InferDynamicManager();
            }
            else
            {
                this.m_partitionCount = 1;
                this.m_outputDataSetInfo = new DataSetInfo(childInfo);
                this.m_outputDataSetInfo.partitionInfo = DataSetInfo.OnePartition;
                if (childInfo.partitionInfo.Count > 1 &&
                    (childInfo.partitionInfo.ParType != PartitionType.Range ||
                     childInfo.orderByInfo.IsOrdered))
                {
                    this.m_outputDataSetInfo.orderByInfo = DataSetInfo.NoOrderBy;
                }
                this.m_dynamicManager = DynamicManager.None;                
            }
        }

        internal override bool ContainsMerge
        {
            get { return !this.m_isFirstStage; }
        }

        internal override int[] InputPortCounts()
        {
            int portCount = (this.m_isFirstStage) ? 1 : this.Children[0].PartitionCount;
            return new int[] { portCount };
        }
        
        internal override bool[] KeepInputPortOrders()
        {
            return new bool[] { !this.m_isFirstStage };
        }

        internal override Type[] OutputTypes
        {
            get {
                if (this.NodeType == QueryNodeType.TakeWhile)
                {
                    return new Type[] { this.m_controlExpression.Type.GetGenericArguments()[0] };
                }
                return this.Children[0].OutputTypes;
            }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression partitionExpr;
            if (this.NodeType == QueryNodeType.TakeWhile)
            {
                partitionExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               this.OpName,
                                                               new CodeVariableReferenceExpression(readerNames[0]));
            }
            else
            {
                CodeExpression controlArg;
                if (this.NodeType == QueryNodeType.Take || this.NodeType == QueryNodeType.Skip)
                {
                    controlArg = new CodePrimitiveExpression(this.m_count);
                }
                else
                {
                    controlArg = this.QueryGen.CodeGen.MakeExpression(this.m_controlExpression);
                }
                partitionExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               this.OpName,
                                                               new CodeVariableReferenceExpression(readerNames[0]),
                                                               controlArg);
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", partitionExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            if (this.NodeType == QueryNodeType.Take || this.NodeType == QueryNodeType.Skip)
            {
                builder.Append(Convert.ToString(this.m_count));
            }
            else
            {
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_controlExpression,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }

        internal Expression ControlExpression { get { return this.m_controlExpression; } }
    }

    internal class DLinqJoinNode : DLinqQueryNode
    {
        private LambdaExpression m_outerKeySelectExpr;
        private LambdaExpression m_innerKeySelectExpr;
        private LambdaExpression m_resultSelectExpr;
        private LambdaExpression m_outerKeySelectExpr1;
        private LambdaExpression m_innerKeySelectExpr1;
        private LambdaExpression m_resultSelectExpr1;
        private Expression m_comparerExpr;
        private object m_comparer;
        private int m_comparerIdx;
        private Pipeline m_attachedPipeline;

        internal DLinqJoinNode(QueryNodeType nodeType,
                               string opName,
                               LambdaExpression outerKeySelectExpr,
                               LambdaExpression innerKeySelectExpr,
                               LambdaExpression resultSelectExpr,
                               Expression comparerExpr,
                               Expression queryExpr,
                               DLinqQueryNode outerChild,
                               DLinqQueryNode innerChild)
            : base(nodeType, outerChild.QueryGen, queryExpr, outerChild, innerChild)
        {
            Debug.Assert(nodeType == QueryNodeType.Join || nodeType == QueryNodeType.GroupJoin);
            this.m_outerKeySelectExpr = outerKeySelectExpr;
            this.m_innerKeySelectExpr = innerKeySelectExpr;
            this.m_resultSelectExpr = resultSelectExpr;
            this.m_comparerExpr = comparerExpr;
            this.m_opName = opName;
            this.m_attachedPipeline = null;

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(m_comparer);
            }

            if (StaticConfig.UseMemoryFIFO)
            {
                bool isStateful = this.IsStateful;
                foreach (DLinqQueryNode child in this.Children)
                {
                    if (!(child is DLinqInputNode) &&
                        !child.IsForked &&
                        !(isStateful && child.IsStateful) &&
                        child.PartitionCount > 1)
                    {
                        child.ChannelType = ChannelType.MemoryFIFO;
                    }
                    isStateful = isStateful || child.IsStateful;
                }
            }

            this.m_partitionCount = outerChild.OutputDataSetInfo.partitionInfo.Count;
            this.m_outputDataSetInfo = this.ComputeOutputDataSetInfo();

            this.m_dynamicManager = DynamicManager.None;
        }

        internal override bool IsStateful
        {
            get { return this.m_opName.StartsWith("Hash", StringComparison.Ordinal); }
        }

        internal override Type[] OutputTypes
        {
            get {
                Type resultType = this.m_resultSelectExpr.Type.GetGenericArguments()[2];
                return new Type[] { resultType };
            }
        }

        internal override bool CanAttachPipeline
        {
            get { return true; }
        }

        internal override Pipeline AttachedPipeline
        {
            set { this.m_attachedPipeline = value; }
        }

        private DataSetInfo ComputeOutputDataSetInfo()
        {
            DataSetInfo leftChildInfo = this.Children[0].OutputDataSetInfo;
            DataSetInfo rightChildInfo = this.Children[1].OutputDataSetInfo;

            ParameterExpression leftParam = this.m_resultSelectExpr.Parameters[0];
            ParameterExpression rightParam = this.m_resultSelectExpr.Parameters[1];
            PartitionInfo pinfo = leftChildInfo.partitionInfo.Rewrite(this.m_resultSelectExpr, leftParam);
            if (pinfo is RandomPartition)
            {
                pinfo = rightChildInfo.partitionInfo.Rewrite(this.m_resultSelectExpr, rightParam);
            }
            OrderByInfo oinfo = leftChildInfo.orderByInfo.Rewrite(this.m_resultSelectExpr, leftParam);
            if (!oinfo.IsOrdered)
            {
                oinfo = rightChildInfo.orderByInfo.Rewrite(this.m_resultSelectExpr, rightParam);
            }
            DistinctInfo dinfo = DataSetInfo.NoDistinct;
            DistinctAttribute attrib1 = AttributeSystem.GetDistinctAttrib(this.m_resultSelectExpr);
            if (attrib1 != null &&
                (!attrib1.MustBeDistinct ||
                (leftChildInfo.distinctInfo.IsDistinct() && rightChildInfo.distinctInfo.IsDistinct())))
            {
                dinfo = DistinctInfo.Create(attrib1.Comparer, this.OutputTypes[0]);
            }

            return new DataSetInfo(pinfo, oinfo, dinfo);
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            bool isHashJoin = this.OpName.StartsWith("Hash", StringComparison.Ordinal);
            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getCall = new CodeMethodInvokeExpression(
                                                    new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                    "Get",
                                                    new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType;
                if (isHashJoin)
                {
                    comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                }
                else
                {
                    comparerType = typeof(IComparer<>).MakeGenericType(typeArgs[0]);
                }
                comparerArg = new CodeCastExpression(comparerType, getCall);
            }

            CodeExpression joinExpr;
            if (this.m_attachedPipeline != null && this.m_attachedPipeline.Length > 1)
            {
                Type paramType = typeof(IEnumerable<>).MakeGenericType(this.OutputTypes[0]);
                ParameterExpression param = Expression.Parameter(paramType, DryadLinqCodeGen.MakeUniqueName("x"));
                CodeExpression pipelineArg = this.m_attachedPipeline.BuildExpression(1, param, param);
                if (isHashJoin)
                {
                    joinExpr = new CodeMethodInvokeExpression(
                                      DryadLinqCodeGen.DLVTypeExpr,
                                      this.OpName,
                                      new CodeVariableReferenceExpression(readerNames[0]),
                                      new CodeVariableReferenceExpression(readerNames[1]),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_outerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_innerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr1),
                                      comparerArg,
                                      pipelineArg);
                }
                else
                {
                    bool isDescending = this.Children[0].OutputDataSetInfo.orderByInfo.IsDescending;
                    joinExpr = new CodeMethodInvokeExpression(
                                      DryadLinqCodeGen.DLVTypeExpr,
                                      this.OpName,
                                      new CodeVariableReferenceExpression(readerNames[0]),
                                      new CodeVariableReferenceExpression(readerNames[1]),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_outerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_innerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr1),
                                      comparerArg,
                                      new CodePrimitiveExpression(isDescending),
                                      pipelineArg);
                }
            }
            else
            {
                if (isHashJoin)
                {
                    joinExpr = new CodeMethodInvokeExpression(
                                      DryadLinqCodeGen.DLVTypeExpr,
                                      this.OpName,
                                      new CodeVariableReferenceExpression(readerNames[0]),
                                      new CodeVariableReferenceExpression(readerNames[1]),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_outerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_innerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr1),
                                      comparerArg);
                }
                else
                {
                    bool isDescending = this.Children[0].OutputDataSetInfo.orderByInfo.IsDescending;
                    joinExpr = new CodeMethodInvokeExpression(
                                      DryadLinqCodeGen.DLVTypeExpr,
                                      this.OpName,
                                      new CodeVariableReferenceExpression(readerNames[0]),
                                      new CodeVariableReferenceExpression(readerNames[1]),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_outerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_innerKeySelectExpr1),
                                      this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr1),
                                      comparerArg,
                                      new CodePrimitiveExpression(isDescending));
                }
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", joinExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal LambdaExpression OuterKeySelectorExpression 
        {
            get { return this.m_outerKeySelectExpr; } 
        }

        internal LambdaExpression OuterKeySelectorExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_outerKeySelectExpr1;
            }
        }

        internal LambdaExpression InnerKeySelectorExpression 
        {
            get { return this.m_innerKeySelectExpr; } 
        }

        internal LambdaExpression InnerKeySelectorExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_innerKeySelectExpr1; 
            }
        }

        internal LambdaExpression ResultSelectorExpression 
        {
            get { return this.m_innerKeySelectExpr; }
        }

        internal LambdaExpression ResultSelectorExpression1
        {
            get {
                this.GetReferencedQueries();
                return this.m_resultSelectExpr1;
            }
        }

        internal Expression ComparerExpression 
        {
            get { return this.m_comparerExpr; }
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_outerKeySelectExpr1 == null)
            {
                this.m_outerKeySelectExpr1 = (LambdaExpression)subst.Visit(this.m_outerKeySelectExpr);
                this.m_innerKeySelectExpr1 = (LambdaExpression)subst.Visit(this.m_innerKeySelectExpr);
                this.m_resultSelectExpr1 = (LambdaExpression)subst.Visit(this.m_resultSelectExpr);
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_outerKeySelectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append(", ");
            this.Children[1].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_innerKeySelectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }
    }

    internal class DLinqDistinctNode : DLinqQueryNode
    {
        private bool m_isPartial;
        private Expression m_comparerExpr;
        private object m_comparer;
        private int m_comparerIdx;
        private Pipeline m_attachedPipeline;

        internal DLinqDistinctNode(bool isPartial,
                                   Expression comparerExpr, 
                                   Expression queryExpr, 
                                   DLinqQueryNode child)
            : base(QueryNodeType.Distinct, child.QueryGen, queryExpr, child)
        {
            this.m_isPartial = isPartial;
            this.m_comparerExpr = comparerExpr;
            this.m_opName = "Distinct";
            this.m_attachedPipeline = null;

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(this.m_comparer);
            }

            this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;

            this.m_outputDataSetInfo = new DataSetInfo(child.OutputDataSetInfo);
            this.m_outputDataSetInfo.distinctInfo = DistinctInfo.Create(this.m_comparer, this.OutputTypes[0]);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal override bool IsStateful
        {
            get { return !this.m_isPartial; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal override bool CanAttachPipeline
        {
            get { return true; }
        }
        
        internal override Pipeline AttachedPipeline
        {
            set { this.m_attachedPipeline = value; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression compareArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getCall = new CodeMethodInvokeExpression(
                                                    new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                    "Get",
                                                    new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                compareArg = new CodeCastExpression(comparerType, getCall);
            }
            CodeExpression distinctExpr;
            if (this.m_attachedPipeline != null && this.m_attachedPipeline.Length > 1)
            {
                Type paramType = typeof(IEnumerable<>).MakeGenericType(this.OutputTypes[0]);
                ParameterExpression param = Expression.Parameter(paramType, DryadLinqCodeGen.MakeUniqueName("x"));
                CodeExpression pipelineArg = this.m_attachedPipeline.BuildExpression(1, param, param);
                distinctExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                              this.OpName,
                                                              new CodeVariableReferenceExpression(readerNames[0]),
                                                              compareArg,
                                                              pipelineArg,
                                                              new CodePrimitiveExpression(this.m_isPartial));
            }
            else
            {
                distinctExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                              this.OpName,
                                                              new CodeVariableReferenceExpression(readerNames[0]),
                                                              compareArg,
                                                              new CodePrimitiveExpression(this.m_isPartial));
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", distinctExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            if (this.m_comparerExpr != null)
            {
                builder.Append(",");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqContainsNode : DLinqQueryNode
    {
        private Expression m_valueExpr;
        private Expression m_comparerExpr;
        private int m_valueIdx;
        private int m_comparerIdx;

        internal DLinqContainsNode(Expression valueExpr,
                                   Expression comparerExpr,
                                   Expression queryExpr,
                                   DLinqQueryNode child)
            : base(QueryNodeType.Contains, child.QueryGen, queryExpr, child)
        {
            this.m_valueExpr = valueExpr;
            this.m_comparerExpr = comparerExpr;
            this.m_opName = "Contains";

            this.m_valueIdx = DryadLinqObjectStore.Put(ExpressionSimplifier.Evaluate(valueExpr));
            ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                this.m_comparerIdx = DryadLinqObjectStore.Put(evaluator.Eval(comparerExpr));
            }

            this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;
            this.m_outputDataSetInfo = new DataSetInfo();
            this.m_outputDataSetInfo.partitionInfo = new RandomPartition(this.m_partitionCount);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal Expression ValueExpression
        {
            get { return this.m_valueExpr; }
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal override Type[] OutputTypes
        {
            get { return new Type[] { typeof(bool) }; }
        }

        internal override Expression RebuildQueryExpression(Expression inputExpr)
        {
            MethodInfo minfo = typeof(DryadLinqObjectStore).GetMethod("Get");
            Expression getValueExpr = Expression.Call(minfo, Expression.Constant(this.m_valueIdx));
            Expression valueExpr = Expression.Convert(getValueExpr, this.m_valueExpr.Type);
            
            Expression comparerExpr;
            if (this.m_comparerExpr == null)
            {
                Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(this.InputTypes[0]);
                comparerExpr = Expression.Constant(null, comparerType);
            }
            else
            {
                getValueExpr = Expression.Call(minfo, Expression.Constant(this.m_comparerIdx));
                comparerExpr = Expression.Convert(getValueExpr, this.m_comparerExpr.Type);
            }
            Type[] typeArgs = new Type[] { this.InputTypes[0] };
            Expression[] args = new Expression[] { inputExpr, valueExpr, comparerExpr };
            Expression resExpr = Expression.Call(typeof(DryadLinqVertex), this.m_opName, typeArgs, args);
            minfo = typeof(DryadLinqVertex).GetMethod("AsEnumerable").MakeGenericMethod(resExpr.Type);
            return Expression.Call(minfo, new Expression[] { resExpr });
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression getValueCall =
                new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                               "Get",
                                               new CodePrimitiveExpression(this.m_valueIdx));
            CodeExpression valueArg = new CodeCastExpression(this.m_valueExpr.Type, getValueCall);

            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getComparerCall = new CodeMethodInvokeExpression(
                                                            new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                            "Get",
                                                            new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                comparerArg = new CodeCastExpression(comparerType, getComparerCall);
            }
            CodeExpression containsExpr = new CodeMethodInvokeExpression(
                                                     DryadLinqCodeGen.DLVTypeExpr,
                                                     this.OpName,
                                                     new CodeVariableReferenceExpression(readerNames[0]),
                                                     valueArg,
                                                     comparerArg);
            containsExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                          "AsEnumerable",
                                                          containsExpr);
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", containsExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            if (this.m_valueExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_valueExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            if (this.m_comparerExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqBasicAggregateNode : DLinqQueryNode
    {
        private LambdaExpression m_selectExpr;
        private AggregateOpType m_aggregateOpType;
        private bool m_isFirstStage;
        private bool m_isQuery;

        internal DLinqBasicAggregateNode(LambdaExpression selectExpr,
                                         AggregateOpType aggType,
                                         bool isFirstStage,
                                         bool isQuery,
                                         Expression queryExpr,
                                         DLinqQueryNode child)
            : base(QueryNodeType.BasicAggregate, child.QueryGen, queryExpr, child)
        {
            this.m_selectExpr = selectExpr;
            this.m_aggregateOpType = aggType;
            this.m_isFirstStage = isFirstStage;
            this.m_isQuery = isQuery;
            this.m_opName = aggType.ToString();

            if (isFirstStage)
            {
                this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;
                this.m_outputDataSetInfo = new DataSetInfo();
                this.m_outputDataSetInfo.partitionInfo = new RandomPartition(this.m_partitionCount);
                this.m_dynamicManager = this.InferDynamicManager();
            }
            else
            {
                this.m_partitionCount = 1;
                this.m_outputDataSetInfo = new DataSetInfo();
                this.m_dynamicManager = DynamicManager.None;
            }
        }

        internal AggregateOpType OpType
        {
            get { return this.m_aggregateOpType; }
        }

        internal LambdaExpression SelectExpression
        {
            get { return this.m_selectExpr; }
        }

        internal override bool ContainsMerge
        {
            get { return !this.m_isFirstStage; }
        }

        internal override int[] InputPortCounts()
        {
            int portCount = (this.m_isFirstStage) ? 1 : this.Children[0].PartitionCount;
            return new int[] { portCount };
        }

        internal override bool[] KeepInputPortOrders()
        {
            bool keepPortOrder = (!this.m_isFirstStage &&
                                  (this.m_aggregateOpType == AggregateOpType.First ||
                                   this.m_aggregateOpType == AggregateOpType.Last ||
                                   this.m_aggregateOpType == AggregateOpType.FirstOrDefault ||
                                   this.m_aggregateOpType == AggregateOpType.LastOrDefault));
            return new bool[] { keepPortOrder };
        }

        internal bool IsFirstStage
        {
            get { return this.m_isFirstStage; }
        }

        internal bool IsQuery
        {
            get { return this.m_isQuery; }
        }
        
        internal override Type[] OutputTypes
        {
            get {
                if (this.m_aggregateOpType == AggregateOpType.Count)
                {
                    return new Type[] { typeof(Int32) };
                }
                if (this.m_aggregateOpType == AggregateOpType.LongCount)
                {
                    return new Type[] { typeof(Int64) };
                }
                if (this.m_aggregateOpType == AggregateOpType.Any ||
                    this.m_aggregateOpType == AggregateOpType.All)
                {
                    return new Type[] { typeof(bool) };
                }
                
                Type qType = this.QueryExpression.Type;
                if (this.m_isQuery)
                {
                    qType = qType.GetGenericArguments()[0];
                }

                if (!this.m_isFirstStage)
                {
                    if (this.m_aggregateOpType == AggregateOpType.FirstOrDefault ||
                        this.m_aggregateOpType == AggregateOpType.SingleOrDefault ||
                        this.m_aggregateOpType == AggregateOpType.LastOrDefault)
                    {
                        return new Type[] { typeof(AggregateValue<>).MakeGenericType(qType) };
                    }
                    else
                    {
                        return new Type[] { qType };
                    }
                }

                switch (this.m_aggregateOpType)
                {
                    case AggregateOpType.Sum:
                    {
                        return new Type[] { qType };
                    }
                    case AggregateOpType.Min:
                    case AggregateOpType.Max:
                    {
                        if (qType == typeof(Int32?) ||
                            qType == typeof(Int64?) ||
                            qType == typeof(float?) ||
                            qType == typeof(double?) ||
                            qType == typeof(decimal?))
                        {
                            return new Type[] { qType };
                        }
                        return new Type[] { typeof(AggregateValue<>).MakeGenericType(qType) };
                    }
                    case AggregateOpType.First:
                    case AggregateOpType.Single:
                    case AggregateOpType.Last:
                    case AggregateOpType.FirstOrDefault:
                    case AggregateOpType.SingleOrDefault:
                    case AggregateOpType.LastOrDefault:
                    {
                        return new Type[] { typeof(AggregateValue<>).MakeGenericType(qType) };
                    }
                    case AggregateOpType.Average:
                    {
                        ParameterInfo[] paramInfos = ((MethodCallExpression)this.QueryExpression).Method.GetParameters();
                        Type valueType;
                        if (this.m_selectExpr == null)
                        {
                            valueType = paramInfos[0].ParameterType.GetGenericArguments()[0];
                        }
                        else
                        {
                            valueType = paramInfos[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1];
                        }

                        if (valueType == typeof(int))
                        {
                            valueType = typeof(long);
                        }
                        else if (valueType == typeof(int?))
                        {
                            valueType = typeof(long?);
                        }
                        else if (valueType == typeof(float))
                        {
                            valueType = typeof(double);
                        }
                        else if (valueType == typeof(float?))
                        {
                            valueType = typeof(double?);
                        }
                        return new Type[] { typeof(AggregateValue<>).MakeGenericType(valueType) };
                    }
                    default:
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AggregateOperatorNotSupported,
                                                     String.Format(SR.AggregateOperatorNotSupported, this.m_aggregateOpType));
                    }
                }
            }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression aggregateExpr;
            if (this.m_selectExpr == null)
            {
                aggregateExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               this.OpName,
                                                               new CodeVariableReferenceExpression(readerNames[0]));
            }
            else
            {
                aggregateExpr = new CodeMethodInvokeExpression(
                                           DryadLinqCodeGen.DLVTypeExpr,
                                           this.OpName,
                                           new CodeVariableReferenceExpression(readerNames[0]),
                                           this.QueryGen.CodeGen.MakeExpression(this.m_selectExpr));
            }
            if (!this.m_isFirstStage &&
                this.m_aggregateOpType == AggregateOpType.Average &&
                ((this.OutputTypes[0] == typeof(float)) || (this.OutputTypes[0] == typeof(float?))))
            {
                aggregateExpr = new CodeCastExpression(this.OutputTypes[0], aggregateExpr);
            }
            aggregateExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                           "AsEnumerable",
                                                           aggregateExpr);
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", aggregateExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            if (this.m_selectExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_selectExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqAggregateNode : DLinqQueryNode
    {
        // There are up to 3 levels of aggregations:
        // Stage 1: first level is pipelined with the source computation;
        // Stage 2: second level is an aggregation of first level and then a dynamic aggregation;
        // Stage 3: third level is a single partition
        private Expression m_seedExpr;
        private LambdaExpression m_funcLambda;
        private LambdaExpression m_combinerLambda;
        private LambdaExpression m_resultLambda;
        private int m_stage;
        private LambdaExpression m_seedLambda;
        private object m_seedValue;
        private int m_seedIdx;

        internal DLinqAggregateNode(string opName,
                                    Expression seedExpr,
                                    LambdaExpression funcLambda,
                                    LambdaExpression combinerLambda,
                                    LambdaExpression resultLambda,
                                    int stage,
                                    Expression queryExpr,
                                    DLinqQueryNode child,
                                    bool functionIsExpensive)
            : base(QueryNodeType.Aggregate, child.QueryGen, queryExpr, child)
        {
            this.m_seedExpr = seedExpr;
            this.m_funcLambda = funcLambda;
            this.m_combinerLambda = combinerLambda;
            this.m_resultLambda = resultLambda;
            this.m_stage = stage;
            this.m_opName = opName;

            this.m_seedValue = null;
            this.m_seedIdx = -1;
            if (seedExpr != null)
            {
                this.m_seedLambda = DryadLinqExpression.GetLambda(seedExpr);
                if (this.m_seedLambda == null)
                {
                    this.m_seedValue = ExpressionSimplifier.Evaluate(seedExpr);
                    if (!seedExpr.Type.IsPrimitive)
                    {
                        this.m_seedIdx = DryadLinqObjectStore.Put(m_seedValue);
                    }
                }
            }

            if (stage != 3)
            {
                this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;
                this.m_outputDataSetInfo = new DataSetInfo();
                this.m_outputDataSetInfo.partitionInfo = new RandomPartition(this.m_partitionCount);
                this.m_dynamicManager = this.InferDynamicManager();
            }
            else
            {
                this.m_partitionCount = 1;
                this.m_outputDataSetInfo = new DataSetInfo();
                if (functionIsExpensive)
                {
                    DLinqDynamicNode dnode = new DLinqDynamicNode(DynamicManagerType.FullAggregator, child);
                    this.m_dynamicManager = new DynamicManager(DynamicManagerType.FullAggregator, dnode);
                    this.m_dynamicManager.AggregationLevels = 2;
                }
                else
                {
                    this.m_dynamicManager = DynamicManager.None;
                }
            }
        }

        // NOTE: because some stages may consume their inputs out of order,
        // [Associative] in fact also assumes commutativity. 
        internal override bool ContainsMerge
        {
            get { return (this.m_stage == 3); }
        }

        internal override int[] InputPortCounts()
        {
            int portCount = (this.m_stage == 3) ? this.Children[0].PartitionCount : 1;
            return new int[] { portCount };
        }

        internal override bool[] KeepInputPortOrders()
        {
            return new bool[] { (this.m_stage == 3) };
        }

        internal int Stage
        {
            get { return this.m_stage; }
        }

        internal override Type[] OutputTypes
        {
            get {
                Type resultType;
                if (this.m_stage == 3 && this.m_resultLambda != null)
                {
                    resultType = this.m_resultLambda.Type.GetGenericArguments()[1];
                }
                else
                {
                    resultType = this.m_funcLambda.Type.GetGenericArguments()[0];
                }
                return new Type[] { resultType };
            }
        }

        internal override bool IsHomomorphic
        {
            get { return (this.m_stage == 1); }
        }

        internal override Expression RebuildQueryExpression(Expression inputExpr)
        {
            Type[] typeArgs;
            Expression[] args;
            if (this.m_seedExpr == null)
            {
                typeArgs = new Type[] { this.InputTypes[0] };
                args = new Expression[] { inputExpr, this.m_funcLambda};
            }
            else
            {
                Expression seedExpr = this.m_seedLambda;
                if (seedExpr == null)
                {
                    if (this.m_seedExpr.Type.IsPrimitive)
                    {
                        seedExpr = Expression.Constant(this.m_seedValue, this.m_seedExpr.Type);
                    }
                    else
                    {
                        MethodInfo minfo = typeof(DryadLinqObjectStore).GetMethod("Get");
                        Expression getValueExpr = Expression.Call(minfo, Expression.Constant(this.m_seedIdx));
                        seedExpr = Expression.Convert(getValueExpr, this.m_seedExpr.Type);
                    }
                }
                typeArgs = new Type[] { this.InputTypes[0], this.m_funcLambda.Body.Type };
                args = new Expression[] { inputExpr, seedExpr, this.m_funcLambda};
            }
            Expression resExpr = Expression.Call(typeof(DryadLinqVertex), this.m_opName, typeArgs, args);
            return Expression.Call(typeof(DryadLinqVertex).GetMethod("AsEnumerable").MakeGenericMethod(resExpr.Type),
                                   new Expression[] { resExpr });
        }

        internal LambdaExpression FuncLambda
        {
            get { return this.m_funcLambda; }
        }

        internal LambdaExpression CombinerLambda
        {
            get { return this.m_combinerLambda; }
        }

        internal LambdaExpression ResultLambda
        {
            get { return this.m_resultLambda; }
        }

        internal Expression SeedExpression
        {
            get { return this.m_seedExpr; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression seedArg = null;
            if (this.m_seedExpr != null &&
                ((this.m_stage == 1 && this.OpName == "AssocAggregate") ||
                 (this.m_stage == 3 && this.OpName == "Aggregate")))
            {
                if (this.m_seedLambda != null)
                {
                    seedArg = this.QueryGen.CodeGen.MakeExpression(this.m_seedLambda);
                }
                else if (this.m_seedExpr.Type.IsPrimitive)
                {
                    seedArg = new CodePrimitiveExpression(this.m_seedValue);
                }
                else
                {
                    CodeExpression getCall =
                        new CodeMethodInvokeExpression(
                                new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                "Get",
                                new CodePrimitiveExpression(this.m_seedIdx));
                    seedArg = new CodeCastExpression(this.m_seedExpr.Type, getCall);
                }
            }
            
            CodeExpression aggregateExpr;
            if (this.m_stage != 3)
            {
                if (seedArg == null)
                {
                    aggregateExpr = new CodeMethodInvokeExpression(
                                            DryadLinqCodeGen.DLVTypeExpr,
                                            this.m_opName,
                                            new CodeVariableReferenceExpression(readerNames[0]),
                                            this.QueryGen.CodeGen.MakeExpression(this.m_funcLambda),
                                            this.QueryGen.CodeGen.MakeExpression(this.m_combinerLambda));
                }
                else
                {
                    aggregateExpr = new CodeMethodInvokeExpression(
                                            DryadLinqCodeGen.DLVTypeExpr,
                                            this.m_opName,
                                            new CodeVariableReferenceExpression(readerNames[0]),
                                            seedArg,
                                            this.QueryGen.CodeGen.MakeExpression(this.m_funcLambda),
                                            this.QueryGen.CodeGen.MakeExpression(this.m_combinerLambda));
                }
            }
            else
            {
                if (this.m_opName.Contains("Assoc"))
                {
                    if (this.m_resultLambda == null)
                    {
                        aggregateExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.m_opName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_combinerLambda));
                    }
                    else
                    {
                        aggregateExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.m_opName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_combinerLambda),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_resultLambda));
                    }
                }
                else
                {
                    if (seedArg == null)
                    {
                        aggregateExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.m_opName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_funcLambda));
                    }
                    else if (this.m_resultLambda == null)
                    {
                        aggregateExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.m_opName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                seedArg,
                                                this.QueryGen.CodeGen.MakeExpression(this.m_funcLambda));
                    }
                    else
                    {
                        aggregateExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.m_opName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                seedArg,
                                                this.QueryGen.CodeGen.MakeExpression(this.m_funcLambda),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_resultLambda));
                    }
                }
                aggregateExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               "AsEnumerable",
                                                               aggregateExpr);
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", aggregateExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            if (this.m_seedExpr != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_seedExpr,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            if (this.m_funcLambda != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_funcLambda,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            if (this.m_resultLambda != null)
            {
                builder.Append(", ");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_resultLambda,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    internal class DLinqConcatNode : DLinqQueryNode
    {
        internal DLinqConcatNode(Expression queryExpr, params DLinqQueryNode[] children)
            : base(QueryNodeType.Concat, children[0].QueryGen, queryExpr)
        {
            this.m_opName = "Concat";
            List<DLinqQueryNode> childList = new List<DLinqQueryNode>();
            this.m_partitionCount = 0;
            foreach (DLinqQueryNode child in children)
            {
                if ((child is DLinqConcatNode) && !child.IsForked)
                {
                    foreach (DLinqQueryNode cc in child.Children)
                    {
                        cc.UpdateParent(child, this);
                        childList.Add(cc);
                    }
                }
                else
                {
                    child.Parents.Add(this);
                    childList.Add(child);
                }
                this.m_partitionCount += child.OutputDataSetInfo.partitionInfo.Count;
            }
            this.Children = childList.ToArray();

            this.m_outputDataSetInfo = new DataSetInfo();
            this.m_outputDataSetInfo.partitionInfo = new RandomPartition(this.m_partitionCount);

            this.m_dynamicManager = DynamicManager.None;
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new InvalidOperationException("Internal error");
        }

        internal void FixInputs()
        {
            for (int i = 0; i < this.Children.Length; i++)
            {
                DLinqQueryNode child = this.Children[i];
                
                if ((child is DLinqInputNode) || child.IsForked)
                {
                    // Insert a dummy Apply
                    Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                    ParameterExpression param = Expression.Parameter(paramType, "x");
                    Type type = typeof(Func<,>).MakeGenericType(paramType, paramType);
                    LambdaExpression applyExpr = Expression.Lambda(type, param, param);
                    this.Children[i] = new DLinqApplyNode(applyExpr, child.QueryExpression, child);
                    this.Children[i].OutputDataSetInfo = child.OutputDataSetInfo;
                    this.Children[i].Parents.Add(this);
                    child.Parents.Remove(this);
                }
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            for (int i = 0; i < this.Children.Length; ++i)
            {
                this.Children[i].BuildString(builder);
                if (i < this.Children.Length - 1)
                {
                    builder.Append(",");
                }
            }
            builder.Append("]");
        }
    }

    internal class DLinqSetOperationNode : DLinqQueryNode
    {
        // Inv: The children are both either ordered or unordered
        private bool m_isOrdered;
        private Expression m_comparerExpr;
        private object m_comparer;
        private int m_comparerIdx;

        internal DLinqSetOperationNode(QueryNodeType nodeType,
                                       string opName,
                                       Expression comparerExpr,
                                       Expression queryExpr,
                                       DLinqQueryNode child1,
                                       DLinqQueryNode child2)
            : base(nodeType, child1.QueryGen, queryExpr, child1, child2)
        {
            this.m_isOrdered = opName.StartsWith("Ordered", StringComparison.Ordinal);
            this.m_opName = opName;
            this.m_comparerExpr = comparerExpr;

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(m_comparer);
            }

            if (StaticConfig.UseMemoryFIFO)
            {
                bool isStateful = this.IsStateful;
                foreach (DLinqQueryNode child in this.Children)
                {
                    if (!(child is DLinqInputNode) &&
                        !child.IsForked &&
                        !(isStateful && child.IsStateful) &&
                        child.PartitionCount > 1)
                    {
                        child.ChannelType = ChannelType.MemoryFIFO;
                    }
                    isStateful = isStateful || child.IsStateful;
                }
            }

            this.m_partitionCount = child1.OutputDataSetInfo.partitionInfo.Count;
            this.m_outputDataSetInfo = new DataSetInfo(child1.OutputDataSetInfo);

            this.m_dynamicManager = DynamicManager.None;
        }

        internal override bool IsStateful
        {
            get { return !this.m_isOrdered; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerExpr != null)
            {
                CodeExpression getCall =
                    new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                   "Get",
                                                   new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                Type comparerType;
                if (m_isOrdered)
                {
                    comparerType = typeof(IComparer<>).MakeGenericType(typeArgs[0]);
                }
                else
                {
                    comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                }

                comparerArg = new CodeCastExpression(comparerType, getCall);
            }

            CodeExpression setOpExpr;
            if (this.m_isOrdered)
            {
                bool isDescending = this.Children[0].OutputDataSetInfo.orderByInfo.IsDescending;
                setOpExpr = new CodeMethodInvokeExpression(
                                          DryadLinqCodeGen.DLVTypeExpr,
                                          this.OpName,
                                          new CodeVariableReferenceExpression(readerNames[0]),
                                          new CodeVariableReferenceExpression(readerNames[1]),
                                          comparerArg,
                                          new CodePrimitiveExpression(isDescending));
            }
            else
            {
                setOpExpr = new CodeMethodInvokeExpression(
                                          DryadLinqCodeGen.DLVTypeExpr,
                                          this.OpName,
                                          new CodeVariableReferenceExpression(readerNames[0]),
                                          new CodeVariableReferenceExpression(readerNames[1]),
                                          comparerArg);
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", setOpExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            this.Children[1].BuildString(builder);
            builder.Append("]");
        }
    }

    // Merging of multiple data channels. 
    // This can operate in two principal modes:
    //    1. "arbitrary" merge .. no port-ordering and no rules about the resulting sequence.
    //    2. "sorted" merge. port-ordering and merge-sort logic.  Requires the input channels be sorted.
    internal class DLinqMergeNode : DLinqQueryNode
    {
        private LambdaExpression m_keySelectExpr;
        private LambdaExpression m_keySelectExpr1;
        private Expression m_comparerExpr;
        private bool m_isDescending;
        private bool m_keepPortOrder;
        private bool m_isSplitting;
        private object m_comparer;
        private int m_comparerIdx;

        internal DLinqMergeNode(bool keepPortOrder, Expression queryExpr, DLinqQueryNode child)
            : base(QueryNodeType.Merge, child.QueryGen, queryExpr, child)
        {
            OrderByInfo childInfo = child.OutputDataSetInfo.orderByInfo;
            LambdaExpression keySelectExpr = childInfo.KeySelector;
            Expression comparerExpr = childInfo.Comparer;
            bool isDescending = childInfo.IsDescending;
            this.m_keepPortOrder = keepPortOrder;
            this.Initialize(keySelectExpr, comparerExpr, isDescending, child, queryExpr);
        }

        internal DLinqMergeNode(LambdaExpression keySelectExpr,
                                Expression comparerExpr,
                                bool isDescending,
                                Expression queryExpr,
                                DLinqQueryNode child)
            : base(QueryNodeType.Merge, child.QueryGen, queryExpr, child)
        {
            this.m_keepPortOrder = true;
            this.Initialize(keySelectExpr, comparerExpr, isDescending, child, queryExpr);
        }

        // Used to support the Left-homomorphic binary ApplyPerPartition.
        // With a TeeNode, it does a broadcast, creating n partitions each collating the complete data.
        internal DLinqMergeNode(Int32 parCount, Expression queryExpr, DLinqQueryNode child)
            : base(QueryNodeType.Merge, child.QueryGen, queryExpr, child)
        {
            this.m_opName = "Merge";
            this.m_keySelectExpr = null;
            this.m_comparerExpr = null;
            this.m_isDescending = false;
            this.m_keepPortOrder = false;
            this.m_comparer = null;
            this.m_comparerIdx = -1;
            this.m_dynamicManager = DynamicManager.None;
            this.m_partitionCount = parCount;
            PartitionInfo pinfo = new RandomPartition(parCount);
            this.m_outputDataSetInfo = new DataSetInfo(pinfo, DataSetInfo.NoOrderBy, DataSetInfo.NoDistinct);
            this.m_isSplitting = false;
        }

        private void Initialize(LambdaExpression keySelectExpr,
                                Expression comparerExpr,
                                bool isDescending,
                                DLinqQueryNode child,
                                Expression queryExpr)
        {
            this.m_opName = (keySelectExpr == null) ? "Merge" : "MergeSort";
            this.m_keySelectExpr = keySelectExpr;
            this.m_comparerExpr = comparerExpr;
            this.m_isDescending = isDescending;

            if (keySelectExpr != null)
            {
                Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
                if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
                {
                    throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                    String.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                    queryExpr);
                }
            }

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(this.m_comparer);
            }

            this.m_dynamicManager = DynamicManager.None;
            if (this.OpName == "MergeSort" && StaticConfig.UseSMBAggregation)
            {
                DLinqDynamicNode dnode = new DLinqDynamicNode(DynamicManagerType.FullAggregator, this);
                this.m_dynamicManager = new DynamicManager(DynamicManagerType.FullAggregator, dnode);
            }

            DLinqQueryNode child1 = child;
            if ((child is DLinqHashPartitionNode) &&
                ((DLinqHashPartitionNode)child).IsDynamicDistributor)
            {
                child1 = child.Children[0];
                this.Children[0] = child1;
                bool found = child1.UpdateParent(child, this);
                this.m_dynamicManager = this.m_dynamicManager.CreateManager(DynamicManagerType.HashDistributor);
                this.m_dynamicManager.InsertVertexNode(-1, child);
            }

            DataSetInfo childInfo = child1.OutputDataSetInfo;
            PartitionInfo pinfo;
            if (child1.ConOpType == ConnectionOpType.CrossProduct)
            {
                this.m_partitionCount = childInfo.partitionInfo.Count;
                pinfo = childInfo.partitionInfo;
            }
            else
            {
                this.m_partitionCount = 1;
                pinfo = DataSetInfo.OnePartition;
            }

            DistinctInfo dinfo = childInfo.distinctInfo;
            OrderByInfo oinfo = DataSetInfo.NoOrderBy;
            if (this.OpName == "MergeSort")
            {
                Type[] typeArgs = this.m_keySelectExpr.Type.GetGenericArguments();
                oinfo = OrderByInfo.Create(this.m_keySelectExpr, this.m_comparer, this.m_isDescending, typeArgs[1]);
            }
            this.m_outputDataSetInfo = new DataSetInfo(pinfo, oinfo, dinfo);

            this.m_isSplitting = (((child is DLinqHashPartitionNode) &&
                                   ((DLinqHashPartitionNode)child).IsDynamicDistributor) ||
                                  ((child is DLinqRangePartitionNode) &&
                                   ((DLinqRangePartitionNode)child).IsDynamicDistributor));
        }

        internal Expression ComparerExpression
        {
            get { return this.m_comparerExpr; }
        }

        internal override int[] InputPortCounts()
        {
            return new int[] { this.Children[0].PartitionCount };
        }

        internal override bool[] KeepInputPortOrders()
        {
            return new bool[] { this.m_keepPortOrder };
        }

        internal bool IsSplitting
        {
            get { return this.m_isSplitting; }
        }

        internal override bool ContainsMerge
        {
            get { return true; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal void AddAggregateNode(DLinqQueryNode node)
        {
            switch (this.m_dynamicManager.ManagerType)
            {
                case DynamicManagerType.None:
                {
                    DLinqDynamicNode dnode = new DLinqDynamicNode(DynamicManagerType.FullAggregator, this);
                    this.m_dynamicManager = new DynamicManager(DynamicManagerType.FullAggregator, dnode);
                    break;
                }
                case DynamicManagerType.HashDistributor:
                {
                    DLinqQueryNode firstVertex = this.m_dynamicManager.GetVertexNode(0);
                    DLinqDynamicNode dnode = firstVertex as DLinqDynamicNode;
                    if (dnode == null || dnode.DynamicType != DynamicManagerType.FullAggregator)
                    {
                        dnode = new DLinqDynamicNode(DynamicManagerType.FullAggregator, this);
                        this.m_dynamicManager.InsertVertexNode(0, dnode);
                    }
                    break;
                }
                case DynamicManagerType.FullAggregator:
                {                    
                    break;
                }
                default:
                {
                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                 String.Format(SR.DynamicManagerType,
                                                               this.m_dynamicManager.ManagerType));
                }
            }
            ((DLinqDynamicNode)this.m_dynamicManager.GetVertexNode(0)).AddNode(node);
        }
        
        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression mergeExpr = null;
            if (this.OpName == "Merge")
            {
                mergeExpr = new CodeMethodInvokeExpression(
                                       DryadLinqCodeGen.DLVTypeExpr,
                                       this.OpName,
                                       new CodeVariableReferenceExpression(readerNames[0]));
            }
            else
            {
                CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
                if (this.m_comparerExpr != null)
                {
                    CodeExpression getCall =
                        new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                       "Get",
                                                       new CodePrimitiveExpression(this.m_comparerIdx));
                    Type[] typeArgs = this.m_comparerExpr.Type.GetGenericArguments();
                    Type comparerType = typeof(IComparer<>).MakeGenericType(typeArgs[0]);
                    comparerArg = new CodeCastExpression(comparerType, getCall);
                }

                mergeExpr = new CodeMethodInvokeExpression(
                                       DryadLinqCodeGen.DLVTypeExpr,
                                       this.OpName,
                                       new CodeVariableReferenceExpression(readerNames[0]),
                                       this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                       comparerArg,
                                       new CodePrimitiveExpression(this.m_isDescending));
            }

            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", mergeExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            this.m_keySelectExpr1 = null;
            if (this.m_keySelectExpr != null)
            {
                this.m_keySelectExpr1 = (LambdaExpression)subst.Visit(this.m_keySelectExpr);
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append("]");
        }
    }

    // Hash partition of a dataset
    internal class DLinqHashPartitionNode : DLinqQueryNode
    {
        private LambdaExpression m_keySelectExpr;
        private Expression m_keySelectExpr1;
        private LambdaExpression m_resultSelectExpr;
        private int m_parCount;
        private Expression m_comparerExpression;
        private object m_comparer;
        private int m_comparerIdx;
        private bool m_isDynamic;

        internal DLinqHashPartitionNode(LambdaExpression keySelectExpr,
                                        Expression comparerExpr,
                                        int count,
                                        Expression queryExpr,
                                        DLinqQueryNode child)
            : this(keySelectExpr, comparerExpr, count, false, queryExpr, child)
        {
        }

        internal DLinqHashPartitionNode(LambdaExpression keySelectExpr,
                                        Expression comparerExpr,
                                        int count,
                                        bool isDynamic,
                                        Expression queryExpr,
                                        DLinqQueryNode child)
            : this(keySelectExpr, null, comparerExpr, count, isDynamic, queryExpr, child)
        {
        }

        internal DLinqHashPartitionNode(LambdaExpression keySelectExpr,
                                        LambdaExpression resultSelectExpr,
                                        Expression comparerExpr,
                                        int count,
                                        bool isDynamic,
                                        Expression queryExpr,
                                        DLinqQueryNode child)
            : base(QueryNodeType.HashPartition, child.QueryGen, queryExpr, child)
        {
            this.m_keySelectExpr = keySelectExpr;
            this.m_resultSelectExpr = resultSelectExpr;
            this.m_parCount = count;
            this.m_isDynamic = isDynamic;
            this.m_comparerExpression = comparerExpr;
            this.m_opName = "HashPartition";
            this.m_conOpType = ConnectionOpType.CrossProduct;

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(this.m_comparer);
            }

            this.m_partitionCount = child.OutputDataSetInfo.partitionInfo.Count;

            Type keyType = child.OutputTypes[0];
            if (this.m_keySelectExpr != null)
            {
                keyType = this.m_keySelectExpr.Type.GetGenericArguments()[1];
            }
            DataSetInfo childInfo = child.OutputDataSetInfo;
            PartitionInfo pInfo = PartitionInfo.CreateHash(this.m_keySelectExpr,
                                                           this.m_parCount,
                                                           this.m_comparer,
                                                           keyType);
            OrderByInfo oinfo = childInfo.orderByInfo;
            DistinctInfo dinfo = childInfo.distinctInfo;
            this.m_outputDataSetInfo = new DataSetInfo(pInfo, oinfo, dinfo);

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override Type[] OutputTypes
        {
            get {
                if (this.m_resultSelectExpr != null)
                {
                    return new Type[] { this.m_resultSelectExpr.Body.Type };
                }
                return this.Children[0].OutputTypes;
            }
        }

        internal bool IsDynamicDistributor
        {
            get { return this.m_isDynamic; }
        }

        internal LambdaExpression KeySelectExpression
        {
            get { return m_keySelectExpr; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerIdx != -1)
            {
                CodeExpression getComparerCall =
                    new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                   "Get",
                                                   new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpression.Type.GetGenericArguments();
                Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(typeArgs[0]);
                comparerArg = new CodeCastExpression(comparerType, getComparerCall);
            }

            CodeExpression distributeExpr;
            if (this.m_keySelectExpr == null)
            {
                if (this.m_resultSelectExpr == null)
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                comparerArg,
                                                new CodeVariableReferenceExpression(writerNames[0]));
                }
                else
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                comparerArg,
                                                this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr),
                                                new CodeVariableReferenceExpression(writerNames[0]));
                }
            }
            else
            {
                ExpressionInfo einfo = new ExpressionInfo(this.m_keySelectExpr1);
                if (this.m_resultSelectExpr == null)
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                                new CodePrimitiveExpression(einfo.IsExpensive),
                                                comparerArg,
                                                new CodeVariableReferenceExpression(writerNames[0]));
                }
                else
                {
                distributeExpr = new CodeMethodInvokeExpression(
                                            DryadLinqCodeGen.DLVTypeExpr,
                                            this.OpName,
                                            new CodeVariableReferenceExpression(readerNames[0]),
                                            this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpr1),
                                            new CodePrimitiveExpression(einfo.IsExpensive),
                                            comparerArg,
                                            this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpr),
                                            new CodeVariableReferenceExpression(writerNames[0]));
                }
            }
            vertexMethod.Statements.Add(distributeExpr);
            return null;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_keySelectExpr != null)
            {
                this.m_keySelectExpr1 = subst.Visit(this.m_keySelectExpr);
            }
        }

        internal int NumberOfPartitions { get { return this.m_parCount; } }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[HashPartition ");
            this.Children[0].BuildString(builder);
            builder.Append(",");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_keySelectExpr,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append(",");
            builder.Append(Convert.ToString(this.m_parCount));
            if (this.m_comparerIdx != -1)
            {
                builder.Append(",");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpression,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    // Range partition of a dataset
    internal class DLinqRangePartitionNode : DLinqQueryNode
    {
        private LambdaExpression m_keySelectExpression;
        private Expression m_keySelectExpression1;
        private LambdaExpression m_resultSelectExpression;
        private Expression m_keysExpression;
        private Expression m_comparerExpression;
        private Expression m_isDescendingExpression;
        private Expression m_countExpression;
        private object m_keys;
        private int m_keysIdx;
        private object m_comparer;
        private int m_comparerIdx;
        private bool? m_isDescending;
        private int m_count;

        //Creates a "Range distribution" Node
        internal DLinqRangePartitionNode(LambdaExpression keySelectExpr,
                                         LambdaExpression resultSelectExpr,
                                         Expression keysExpr,
                                         Expression comparerExpr,
                                         Expression isDescendingExpr,
                                         Expression countExpr,
                                         Expression queryExpr,
                                         params DLinqQueryNode[] children)
            : base(QueryNodeType.RangePartition, children[0].QueryGen, queryExpr, children)
        {
            this.m_keySelectExpression = keySelectExpr;
            this.m_resultSelectExpression = resultSelectExpr;
            this.m_keysExpression = keysExpr;            
            this.m_comparerExpression = comparerExpr;
            this.m_isDescendingExpression = isDescendingExpr;
            this.m_countExpression = countExpr;            
            this.m_opName = "RangePartition";
            this.m_conOpType = ConnectionOpType.CrossProduct;

            this.m_isDescending = null;
            if (this.m_isDescendingExpression != null)
            {
                ExpressionSimplifier<bool> bevaluator = new ExpressionSimplifier<bool>();
                this.m_isDescending = bevaluator.Eval(isDescendingExpr);
            }

            ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
            this.m_keys = null;
            this.m_keysIdx = -1;
            if (keysExpr != null)
            {
                this.m_keys = evaluator.Eval(keysExpr);
                this.m_keysIdx = DryadLinqObjectStore.Put(m_keys);
            }

            this.m_comparer = null;
            this.m_comparerIdx = -1;
            if (comparerExpr != null)
            {
                this.m_comparer = evaluator.Eval(comparerExpr);
                this.m_comparerIdx = DryadLinqObjectStore.Put(m_comparer);
            }

            this.m_count = 1;
            if (countExpr != null)
            {
                ExpressionSimplifier<int> ievaluator = new ExpressionSimplifier<int>();
                this.m_count = ievaluator.Eval(countExpr);
            }
            
            this.m_partitionCount = this.Children[0].OutputDataSetInfo.partitionInfo.Count;

            DataSetInfo childInfo = this.Children[0].OutputDataSetInfo;
            Type keyType = this.m_keySelectExpression.Type.GetGenericArguments()[1];
            PartitionInfo pInfo = PartitionInfo.CreateRange(this.m_keySelectExpression,
                                                            this.m_keys,
                                                            this.m_comparer,
                                                            this.m_isDescending,
                                                            this.m_count,
                                                            keyType);
            OrderByInfo oinfo = childInfo.orderByInfo;
            DistinctInfo dinfo = childInfo.distinctInfo;
            this.m_outputDataSetInfo = new DataSetInfo(pInfo, oinfo, dinfo);
            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override Type[] OutputTypes
        {
            get {
                if (this.m_resultSelectExpression != null)
                {
                    return new Type[] { this.m_resultSelectExpression.Body.Type };
                }
                return this.Children[0].OutputTypes;
            }
        }

        internal Expression KeysExpression
        {
            get { return this.m_keysExpression; }
        }
        internal Expression ComparerExpression { get { return this.m_comparerExpression; } }
        internal Expression CountExpression { get { return this.m_countExpression; } }

        internal bool IsDynamicDistributor
        {
            get { return this.m_countExpression == null; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression rangeKeys;
            if (this.m_keys == null)
            {
                rangeKeys = new CodeVariableReferenceExpression(readerNames[1]);
            }
            else
            {
                rangeKeys = new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                           "Get",
                                                           new CodePrimitiveExpression(this.m_keysIdx));
                rangeKeys = new CodeCastExpression(this.m_keysExpression.Type, rangeKeys);
            }
            CodeExpression sinkExpr = new CodeVariableReferenceExpression(writerNames[0]);
            CodeExpression comparerArg = DryadLinqCodeGen.NullExpr;
            if (this.m_comparerIdx != -1)
            {
                CodeExpression getComparerCall =
                    new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                   "Get",
                                                   new CodePrimitiveExpression(this.m_comparerIdx));
                Type[] typeArgs = this.m_comparerExpression.Type.GetGenericArguments();
                Type comparerType = typeof(IComparer<>).MakeGenericType(typeArgs[0]);
                comparerArg = new CodeCastExpression(comparerType, getComparerCall);
            }
            CodeExpression isDescendingArg = new CodePrimitiveExpression(this.OutputPartition.IsDescending);
            CodeExpression distributeExpr;
            if (this.m_keySelectExpression == null)
            {
                if (this.m_resultSelectExpression == null)
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                rangeKeys,
                                                comparerArg,
                                                isDescendingArg,
                                                sinkExpr);
                }
                else
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                rangeKeys,
                                                comparerArg,
                                                isDescendingArg,
                                                this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpression),
                                                sinkExpr);
                }
            }
            else
            {
                if (this.m_resultSelectExpression == null)
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpression1),
                                                rangeKeys,
                                                comparerArg,
                                                isDescendingArg,
                                                sinkExpr);
                }
                else
                {
                    distributeExpr = new CodeMethodInvokeExpression(
                                                DryadLinqCodeGen.DLVTypeExpr,
                                                this.OpName,
                                                new CodeVariableReferenceExpression(readerNames[0]),
                                                this.QueryGen.CodeGen.MakeExpression(this.m_keySelectExpression1),
                                                rangeKeys,
                                                comparerArg,
                                                isDescendingArg,
                                                this.QueryGen.CodeGen.MakeExpression(this.m_resultSelectExpression),
                                                sinkExpr);
                }
            }
            vertexMethod.Statements.Add(distributeExpr);
            return null;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            if (this.m_keySelectExpression != null)
            {
                this.m_keySelectExpression1 = subst.Visit(this.m_keySelectExpression);
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[RangePartition ");
            this.Children[0].BuildString(builder);
            builder.Append(",");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_keySelectExpression,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append(",");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_keysExpression,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            if (this.m_comparerIdx != -1)
            {
                builder.Append(",");
                builder.Append(DryadLinqExpression.ToCSharpString(this.m_comparerExpression,
                                                                this.QueryGen.CodeGen.AnonymousTypeToName));
            }
            builder.Append("]");
        }
    }

    // A super node encapsulates a subtree of the query tree into a single
    // vertex. It could have arbitrary number of inputs and outputs.
    internal class DLinqSuperNode : DLinqQueryNode
    {
        private DLinqQueryNode m_rootNode;
        private bool m_isStateful;
        private bool m_containsMerge;

        internal DLinqSuperNode(DLinqQueryNode root)
            : base(QueryNodeType.Super, root.QueryGen, root.QueryExpression)
        {
            this.ChannelType = root.ChannelType;
            this.m_conOpType = root.ConOpType;
            this.m_rootNode = root;
            this.IsForked = root.IsForked;
            root.SuperNode = this;
            this.m_isStateful = root.IsStateful;
            this.m_containsMerge = false;
            foreach (DLinqQueryNode child in root.Children)
            {
                if (!(child is DLinqInputNode))
                {
                    this.m_isStateful = this.m_isStateful || child.IsStateful;
                    this.m_containsMerge = this.m_containsMerge || child.ContainsMerge;
                }
            }

            this.Parents.AddRange(root.Parents);
            this.m_partitionCount = root.PartitionCount;
            this.m_outputDataSetInfo = root.OutputDataSetInfo;
            this.m_dynamicManager = root.Children[0].DynamicManager;
        }

        internal DLinqQueryNode RootNode
        {
            get { return this.m_rootNode; }
        }

        internal void SwitchTo(DLinqSuperNode node)
        {
            this.SwitchTo(this.m_rootNode, node);
        }

        private void SwitchTo(DLinqQueryNode curNode, DLinqSuperNode node)
        {
            if (curNode.SuperNode == this)
            {
                curNode.SuperNode = node;
                foreach (DLinqQueryNode child in curNode.Children)
                {
                    this.SwitchTo(child, node);
                }
            }
        }

        internal override bool ContainsMerge
        {
            get { return this.m_containsMerge; }
        }

        internal override int[] InputPortCounts()
        {
            int[] inputPortCountArray = new int[this.InputArity];
            this.InputPortCounts(this.m_rootNode, inputPortCountArray, 0);
            return inputPortCountArray;
        }

        private int InputPortCounts(DLinqQueryNode curNode,
                                    int[] inputPortCountArray,
                                    int inputIndex)
        {
            int[] curInputPortCountArray = curNode.InputPortCounts();
            for (int i = 0; i < curNode.Children.Length; i++)
            {
                DLinqQueryNode child = curNode.Children[i];
                if (this.Contains(child))
                {
                    inputIndex = this.InputPortCounts(child, inputPortCountArray, inputIndex);
                }
                else
                {
                    inputPortCountArray[inputIndex] = curInputPortCountArray[i];
                    inputIndex++;
                }
            }
            return inputIndex;
        }

        internal override bool[] KeepInputPortOrders()
        {
            bool[] keepPortOrderArray = new bool[this.InputArity];
            this.KeepInputPortOrders(this.m_rootNode, keepPortOrderArray, 0);
            return keepPortOrderArray;
        }

        private int KeepInputPortOrders(DLinqQueryNode curNode,
                                        bool[] keepPortOrderArray,
                                        int inputIndex)
        {
            bool[] curKeepPortOrderArray = curNode.KeepInputPortOrders();
            for (int i = 0; i < curNode.Children.Length; i++)
            {
                DLinqQueryNode child = curNode.Children[i];
                if (this.Contains(child))
                {
                    inputIndex = this.KeepInputPortOrders(child, keepPortOrderArray, inputIndex);
                }
                else
                {
                    keepPortOrderArray[inputIndex] = curKeepPortOrderArray[i];
                    inputIndex++;
                }
            }
            return inputIndex;
        }

        internal override bool IsStateful
        {
            get { return this.m_isStateful; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.m_rootNode.OutputTypes; }
        }

        internal bool Contains(DLinqQueryNode node)
        {
            return (node.SuperNode == this);
        }

        internal override void CreateCodeAndMappingsForIntermediateTypes()
        {
            this.CreateCodeAndMappingsForIntermediateTypes(this.m_rootNode);
        }

        private void CreateCodeAndMappingsForIntermediateTypes(DLinqQueryNode curNode)
        {
            if (curNode.SuperNode == this)
            {
                foreach (DLinqQueryNode child in curNode.Children)
                {
                    child.CreateCodeAndMappingsForIntermediateTypes();
                    this.CreateCodeAndMappingsForIntermediateTypes(child);
                }
            }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            Pipeline pipeline = new Pipeline(vertexMethod, this.QueryGen.CodeGen, writerNames);
            this.MakeSuperBody(vertexMethod, this.m_rootNode, writerNames, pipeline);
            return this.QueryGen.CodeGen.AddVertexCode(vertexMethod, pipeline);
        }

        private void MakeSuperBody(CodeMemberMethod vertexMethod,
                                   DLinqQueryNode curNode,
                                   string[] writerNames,
                                   Pipeline pipeline)
        {
            bool isHomomorphic = curNode.IsHomomorphic;
            DLinqQueryNode[] curChildren = curNode.Children;
            string[] curSources = new string[curChildren.Length];

            for (int i = 0; i < curChildren.Length; i++)
            {
                DLinqQueryNode child = curChildren[i];
                if (this.Contains(child))
                {
                    this.MakeSuperBody(vertexMethod, child, writerNames, pipeline);
                    if (!isHomomorphic)
                    {
                        curSources[i] = this.QueryGen.CodeGen.AddVertexCode(vertexMethod, pipeline);
                    }
                }
                else
                {
                    Type inputType = child.OutputTypes[0];
                    string factoryName = this.QueryGen.CodeGen.GetStaticFactoryName(inputType);
                    CodeVariableDeclarationStatement
                        readerDecl = this.QueryGen.CodeGen.MakeVertexReaderDecl(inputType, factoryName);
                    vertexMethod.Statements.Add(readerDecl);
                    curSources[i] = readerDecl.Name;
                    pipeline.Reset(new string[] { readerDecl.Name });
                }
            }

            if (!isHomomorphic)
            {
                pipeline.Reset(curSources);
            }
            pipeline.Add(curNode);
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            this.GetReferencedQueries(this.m_rootNode, subst);
        }
        
        private void GetReferencedQueries(DLinqQueryNode curNode, ReferencedQuerySubst subst)
        {
            curNode.GetReferencedQueries(subst);
            DLinqQueryNode[] curChildren = curNode.Children;
            for (int i = 0; i < curChildren.Length; i++)
            {
                DLinqQueryNode child = curChildren[i];
                if (this.Contains(child))
                {
                    this.GetReferencedQueries(child, subst);
                }
            }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            for (int i = 0; i < this.Children.Length; i++)
            {
                if (i != 0)
                {
                    builder.Append(", ");
                }
                this.Children[i].BuildString(builder);
            }
            builder.Append("]");
        }
    }

    internal class DLinqApplyNode : DLinqQueryNode
    {
        private LambdaExpression m_procLambda;
        private Expression m_procLambda1;
        private bool m_isMultiSources;

        internal DLinqApplyNode(LambdaExpression procLambda,
                                bool isMultiSources,
                                Expression queryExpr,
                                params DLinqQueryNode[] children)
            : base(QueryNodeType.Apply, children[0].QueryGen, queryExpr, children)
        {
            this.m_procLambda = procLambda;
            this.m_isMultiSources = isMultiSources;
            this.m_opName = "Apply";

            if (StaticConfig.UseMemoryFIFO && children.Length > 1)
            {
                bool isStateful = this.IsStateful;
                foreach (DLinqQueryNode child in this.Children)
                {
                    if (!(child is DLinqInputNode) &&
                        !child.IsForked &&
                        !(isStateful && child.IsStateful) &&
                        child.PartitionCount > 1)
                    {
                        child.ChannelType = ChannelType.MemoryFIFO;
                    }
                    isStateful = isStateful || child.IsStateful;
                }
            }

            this.m_partitionCount = this.Children[0].OutputPartition.Count;
            this.m_outputDataSetInfo = this.ComputeOutputDataSetInfo();

            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal DLinqApplyNode(LambdaExpression procLambda,
                                Expression queryExpr,
                                params DLinqQueryNode[] children)
            : this(procLambda, false, queryExpr, children)
        {
        }

        // This operator is not stateful iff
        //   1. procLambda is of form: (args) => Method(args) and
        //   2. Method has a DryadStatefulAttribute specifying it not stateful
        internal override bool IsStateful
        {
            get {
                ExpressionInfo einfo = new ExpressionInfo(this.m_procLambda);
                return einfo.IsExpensive;
            }
        }

        internal override Type[] OutputTypes
        {
            get {
                Type[] procArgTypes = m_procLambda.Type.GetGenericArguments();
                Int32 idx = (this.IsWriteToStream) ? 0 : (procArgTypes.Length - 1);
                Type procReturnType = procArgTypes[idx].GetGenericArguments()[0];
                return new Type[] { procReturnType };
            }
        }

        internal bool IsReadFromStream
        {
            get {
                Type[] procArgTypes = m_procLambda.Type.GetGenericArguments();
                return typeof(Stream).IsAssignableFrom(procArgTypes[0]);
            }
        }
        
        internal bool IsWriteToStream
        {
            get {
                Type[] procArgTypes = m_procLambda.Type.GetGenericArguments();
                return typeof(Stream).IsAssignableFrom(procArgTypes[1]);
            }
        }

        private DataSetInfo ComputeOutputDataSetInfo()
        {
            PartitionInfo pinfo = new RandomPartition(this.m_partitionCount);
            OrderByInfo oinfo = DataSetInfo.NoOrderBy;
            DistinctInfo dinfo = DataSetInfo.NoDistinct;
            return new DataSetInfo(pinfo, oinfo, dinfo);
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression procArg = this.QueryGen.CodeGen.MakeExpression(this.m_procLambda1);
            CodeExpression applyExpr = null;
            if (this.IsWriteToStream)
            {
                applyExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                           this.OpName,
                                                           new CodeVariableReferenceExpression(readerNames[0]),
                                                           procArg,
                                                           new CodeVariableReferenceExpression(writerNames[0]));
                vertexMethod.Statements.Add(new CodeExpressionStatement(applyExpr));
                return null;                
            }
            if (this.m_isMultiSources)
            {
                if (this.m_procLambda.Parameters.Count == 1)
                {
                    CodeExpression[] sourceExprs = new CodeExpression[readerNames.Length];
                    for (int i = 0; i < readerNames.Length; ++i)
                    {
                        sourceExprs[i] = new CodeVariableReferenceExpression(readerNames[i]);
                    }
                    var arrayExpr = new CodeArrayCreateExpression(typeof(IEnumerable<>).MakeGenericType(OutputTypes[0]),
                                                                  sourceExprs);
                    var arrayDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "sourceArray", arrayExpr);
                    vertexMethod.Statements.Add(arrayDecl);
                    applyExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               this.OpName,
                                                               new CodeVariableReferenceExpression(arrayDecl.Name),
                                                               procArg);
                }
                else
                {
                    CodeExpression[] sourceExprs = new CodeExpression[readerNames.Length - 1];
                    for (int i = 1; i < readerNames.Length; i++)
                    {
                        sourceExprs[i - 1] = new CodeVariableReferenceExpression(readerNames[i]);
                    }
                    var arrayExpr = new CodeArrayCreateExpression(typeof(IEnumerable), sourceExprs);
                    var arrayDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "sourceArray", arrayExpr);
                    vertexMethod.Statements.Add(arrayDecl);
                    applyExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                               this.OpName,
                                                               new CodeVariableReferenceExpression(readerNames[0]),
                                                               new CodeVariableReferenceExpression(arrayDecl.Name),
                                                               procArg);
                }
            }
            else if (readerNames.Length == 1)
            {
                applyExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                           this.OpName,
                                                           new CodeVariableReferenceExpression(readerNames[0]),
                                                           procArg);
            }
            else
            {
                applyExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr,
                                                           this.OpName,
                                                           new CodeVariableReferenceExpression(readerNames[0]),
                                                           new CodeVariableReferenceExpression(readerNames[1]),
                                                           procArg);
            }

            CodeVariableDeclarationStatement
                sourceDecl = this.QueryGen.CodeGen.MakeVarDeclStatement("var", "source", applyExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        internal override void GetReferencedQueries(ReferencedQuerySubst subst)
        {
            this.m_procLambda1 = subst.Visit(this.m_procLambda);
        }

        internal Expression LambdaExpression { get { return this.m_procLambda; } }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            foreach (DLinqQueryNode child in this.Children)
            {
                child.BuildString(builder);
                builder.Append("  ");
            }
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_procLambda,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }
    }

    internal class DLinqForkNode : DLinqQueryNode
    {
        private LambdaExpression m_forkLambda;
        private Expression m_keysExpression;
        private object m_keys;
        private int m_keysIdx;
        private Type[] m_outputTypes;
    
        internal DLinqForkNode(LambdaExpression fork,
                               Expression keysExpr,
                               Expression queryExpr,
                               DLinqQueryNode child)
            : base(QueryNodeType.Fork, child.QueryGen, queryExpr, child)
        {
            this.m_forkLambda = fork;
            this.m_keysExpression = keysExpr;
            this.m_opName = "Fork";
    
            ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
            this.m_keys = null;
            this.m_keysIdx = -1;
            if (keysExpr != null)
            {
                this.m_keys = evaluator.Eval(keysExpr);
                this.m_keysIdx = DryadLinqObjectStore.Put(m_keys);
            }
            
            this.m_partitionCount = child.OutputPartition.Count;
            PartitionInfo pinfo = new RandomPartition(child.OutputDataSetInfo.partitionInfo.Count);
            this.m_outputDataSetInfo = new DataSetInfo(pinfo, DataSetInfo.NoOrderBy, DataSetInfo.NoDistinct);
    
            this.m_dynamicManager = this.InferDynamicManager();
    
            // Finally, create all the children of this:
            if (keysExpr == null)
            {
                Type forkTupleType = fork.Type.GetGenericArguments()[1];
                if (forkTupleType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    forkTupleType = forkTupleType.GetGenericArguments()[0];
                }
                Type[] queryTypeArgs = forkTupleType.GetGenericArguments();
                this.m_outputTypes = new Type[queryTypeArgs.Length];
                for (int i = 0; i < queryTypeArgs.Length; i++)
                {
                    this.m_outputTypes[i] = queryTypeArgs[i];
                    DLinqQueryNode parentNode = new DLinqTeeNode(queryTypeArgs[i], true, queryExpr, this);
                }
            }
            else
            {
                int forkCnt = ((Array)m_keys).Length;
                Type forkType = fork.Type.GetGenericArguments()[0];
                this.m_outputTypes = new Type[forkCnt];
                for (int i = 0; i < forkCnt; i++)
                {
                    this.m_outputTypes[i] = forkType;
                    DLinqQueryNode parentNode = new DLinqTeeNode(forkType, true, queryExpr, this);
                }
            }
        }
    
        internal override bool IsStateful
        {
            get {
                if (this.KeysExpression != null) return false;
                if (m_forkLambda.Type.GetGenericArguments()[1].GetGenericTypeDefinition() == typeof(ForkTuple<,>))
                {
                    return false;
                }
                ResourceAttribute attrib = AttributeSystem.GetResourceAttrib(this.m_forkLambda);
                return (attrib == null || attrib.IsStateful);
            }
        }
    
        internal override Type[] OutputTypes
        {
            get { return this.m_outputTypes; }
        }
    
        internal Expression KeysExpression
        {
            get { return this.m_keysExpression; }
        }
    
        internal Expression ForkLambda 
        { 
            get { return this.m_forkLambda; } 
        }
    
        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            CodeExpression[] args;
            bool orderPreserving = (m_queryGen.Context.SelectOrderPreserving ||
                                    this.OutputDataSetInfo.orderByInfo.IsOrdered);
            if (this.KeysExpression != null)
            {
                args = new CodeExpression[readerNames.Length + writerNames.Length + 3];
                args[0] = new CodeVariableReferenceExpression(readerNames[0]);
                args[1] = this.QueryGen.CodeGen.MakeExpression(this.m_forkLambda);
                CodeExpression rangeKeys = new CodeMethodInvokeExpression(
                                                      new CodeTypeReferenceExpression("DryadLinqObjectStore"),
                                                      "Get",
                                                      new CodePrimitiveExpression(this.m_keysIdx));
                args[2] = new CodeCastExpression(this.m_keysExpression.Type, rangeKeys);
                args[3] = new CodePrimitiveExpression(orderPreserving);
                for (int i = 0; i < writerNames.Length; i++)
                {
                    args[i+4] = new CodeVariableReferenceExpression(writerNames[i]);
                }
            }
            else
            {
                args = new CodeExpression[readerNames.Length + writerNames.Length + 2];
                args[0] = new CodeVariableReferenceExpression(readerNames[0]);
                args[1] = this.QueryGen.CodeGen.MakeExpression(this.m_forkLambda);
                args[2] = new CodePrimitiveExpression(orderPreserving);
                for (int i = 0; i < writerNames.Length; i++)
                {
                    args[i+3] = new CodeVariableReferenceExpression(writerNames[i]);
                }
            }
                
            CodeExpression forkExpr = new CodeMethodInvokeExpression(DryadLinqCodeGen.DLVTypeExpr, this.OpName, args);
            vertexMethod.Statements.Add(forkExpr);
            return null;
        }
    
        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            builder.Append(DryadLinqExpression.ToCSharpString(this.m_forkLambda,
                                                            this.QueryGen.CodeGen.AnonymousTypeToName));
            builder.Append("]");
        }
    }

    internal class DLinqDoWhileNode: DLinqQueryNode
    {
        private DLinqQueryNode m_body;         // ending vertex of body
        private DLinqQueryNode m_cond;         // ending vertex of cond
        private DLinqQueryNode m_bodySource;
        private DLinqQueryNode m_condSource1;
        private DLinqQueryNode m_condSource2;

        internal DLinqDoWhileNode(DLinqQueryNode body,
                                  DLinqQueryNode cond,
                                  DLinqQueryNode bodySource,
                                  DLinqQueryNode condSource1,
                                  DLinqQueryNode condSource2,
                                  Expression queryExpr,
                                  DLinqQueryNode child)
            : base(QueryNodeType.DoWhile, child.QueryGen, queryExpr, child)
        {
            this.m_opName = "DoWhile";
            this.m_body = body;
            this.m_cond = cond;
            this.m_bodySource = bodySource;
            this.m_condSource1 = condSource1;
            this.m_condSource2 = condSource2;
            
            this.m_partitionCount = body.PartitionCount;
            this.m_outputDataSetInfo = body.OutputDataSetInfo;
            this.m_dynamicManager = DynamicManager.None;
        }

        internal override Type[] OutputTypes
        {
            get { return this.Children[0].OutputTypes; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new InvalidOperationException("Internal error");
        }

        internal override int AddToQueryPlan(XmlDocument queryDoc,
                                             XmlElement queryPlan,
                                             HashSet<int> seen)
        {
            if (!seen.Contains(this.m_uniqueId))
            {
                seen.Add(this.m_uniqueId);
                XmlElement vertexElem = this.CreateVertexElem(queryDoc, this.m_uniqueId, this.m_vertexEntryMethod);

                int cid = this.Children[0].AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement childrenElem = this.CreateVertexChildrenElem(queryDoc, cid);
                vertexElem.AppendChild(childrenElem);

                int bodyId = this.m_body.AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement bodyElem = queryDoc.CreateElement("Body");
                bodyElem.InnerText = Convert.ToString(bodyId);
                vertexElem.AppendChild(bodyElem);

                int bodySourceId = this.m_bodySource.AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement bodySourceElem = queryDoc.CreateElement("BodySource");
                bodySourceElem.InnerText = Convert.ToString(bodySourceId);
                vertexElem.AppendChild(bodySourceElem);

                int condId = this.m_cond.AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement condElem = queryDoc.CreateElement("Cond");
                condElem.InnerText = Convert.ToString(condId);
                vertexElem.AppendChild(condElem);

                int condSourceId1 = this.m_condSource1.AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement condSourceElem1 = queryDoc.CreateElement("CondSource");
                condSourceElem1.InnerText = Convert.ToString(condSourceId1);
                vertexElem.AppendChild(condSourceElem1);

                int condSourceId2 = this.m_condSource2.AddToQueryPlan(queryDoc, queryPlan, seen);
                XmlElement condSourceElem2 = queryDoc.CreateElement("CondSource");
                condSourceElem2.InnerText = Convert.ToString(condSourceId2);
                vertexElem.AppendChild(condSourceElem2);

                queryPlan.AppendChild(vertexElem);
            }
            return this.m_uniqueId;
        }

        public DLinqQueryNode Body
        {
            get { return this.m_body; }
            set { this.m_body = value; }            
        }

        public DLinqQueryNode Cond
        {
            get { return this.m_cond; }
            set { this.m_cond = value; }
        }

        public DLinqQueryNode BodySource
        {
            get { return this.m_bodySource; }
        }

        public DLinqQueryNode CondSource1
        {
            get { return this.m_condSource1; }
        }

        public DLinqQueryNode CondSource2
        {
            get { return this.m_condSource2; }
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append(", ");
            this.m_body.BuildString(builder);
            builder.Append(", ");
            this.m_cond.BuildString(builder);
            builder.Append("]");
        }
    }

    internal class DLinqTeeNode : DLinqQueryNode
    {
        private Type m_outputType;

        internal DLinqTeeNode(Type outputType, bool isForked, Expression queryExpr, DLinqQueryNode child)
            : base(QueryNodeType.Tee, child.QueryGen, queryExpr, child)
        {
            this.m_outputType = outputType;
            this.m_opName = "Tee";
            this.IsForked = isForked;

            this.m_partitionCount = child.OutputPartition.Count;
            PartitionInfo pinfo = new RandomPartition(child.OutputDataSetInfo.partitionInfo.Count);
            this.m_outputDataSetInfo = new DataSetInfo(pinfo, DataSetInfo.NoOrderBy, DataSetInfo.NoDistinct);
            this.m_dynamicManager = this.InferDynamicManager();
        }

        internal override Type[] InputTypes
        {
            get { return new Type[] { this.m_outputType }; }
        }

        internal override Type[] OutputTypes
        {
            get { return new Type[] { this.m_outputType }; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new InvalidOperationException("Internal error");
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ");
            this.Children[0].BuildString(builder);
            builder.Append("]");
        }
    }

    internal class DLinqDynamicNode : DLinqQueryNode
    {
        private DynamicManagerType m_dmType;
        private List<DLinqQueryNode> m_realNodes;

        internal DLinqDynamicNode(DynamicManagerType dmType, DLinqQueryNode node)
            : base(QueryNodeType.Dynamic, node.QueryGen, node.QueryExpression)
        {
            switch (dmType)
            {
                case DynamicManagerType.FullAggregator:
                case DynamicManagerType.Broadcast:
                {
                    this.m_dmType = dmType;
                    this.m_realNodes = new List<DLinqQueryNode>(1);
                    this.m_realNodes.Add(node);
                    break;
                }
                default:
                {
                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                 SR.IllegalDynamicManagerType);
                }
            }
        }

        internal override Type[] InputTypes
        {
            get { return this.m_realNodes[0].InputTypes; }
        }

        internal override Type[] OutputTypes
        {
            get { return this.m_realNodes[this.m_realNodes.Count-1].OutputTypes; }
        }

        internal DynamicManagerType DynamicType
        {
            get { return this.m_dmType; }
        }

        internal List<DLinqQueryNode> RealNodes
        {
            get { return this.m_realNodes; }
        }

        internal DLinqQueryNode GetRealNode(int index)
        {
            return this.m_realNodes[index];
        }

        internal void AddNode(DLinqQueryNode node)
        {
            this.m_realNodes.Add(node);
        }
        
        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            string source = readerNames[0];
            foreach (DLinqQueryNode node in this.m_realNodes)
            {
                source = node.AddVertexCode(vertexMethod, new string[] { source }, null);
            }
            return source;
        }
        
        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[" + this.NodeType + " ]");
        }
    }

    internal class DLinqDummyNode : DLinqQueryNode
    {
        private Type m_outputType;

        /// <summary>
        /// Create a dummy node with a specific code generator.
        /// </summary>
        /// <param name="queryGen">Query generator to instantiate.</param>
        /// <param name="outputType">Type of the single output.</param>
        /// <param name="children">The upstream nodes</param>
        internal DLinqDummyNode(DryadLinqQueryGen queryGen,
                                Type outputType,
                                int partitionCount,
                                params DLinqQueryNode[] children)
            : base(QueryNodeType.Dummy, queryGen, null, children)
        {
            this.m_opName = "Dummy";
            this.m_outputType = outputType;
            this.m_outputDataSetInfo = new DataSetInfo();
            this.m_partitionCount = partitionCount;

            this.DynamicManager = DynamicManager.None;
        }

        internal DLinqDummyNode(Expression queryExpr,
                                params DLinqQueryNode[] children)
            : base(QueryNodeType.Dummy, children[0].QueryGen, null)
        {
            this.m_opName = "Dummy";
            this.m_outputType = children[0].OutputTypes[0];
            this.m_outputDataSetInfo = children[0].OutputDataSetInfo;
            this.m_partitionCount = children[0].PartitionCount;

            this.DynamicManager = DynamicManager.None;            
        }

        internal override Type[] OutputTypes
        {
            get { return new Type[] { this.m_outputType }; }
        }

        internal override string AddVertexCode(CodeMemberMethod vertexMethod,
                                               string[] readerNames,
                                               string[] writerNames)
        {
            throw new InvalidOperationException("Internal error");
        }

        internal override void BuildString(StringBuilder builder)
        {
            builder.Append("[Dummy]");
        }
    }

    internal class Pipeline
    {
        private CodeMemberMethod m_vertexMethod;
        private DryadLinqCodeGen m_codeGen;
        private string[] m_readerNames;
        private string[] m_writerNames;
        private List<DLinqQueryNode> m_nodes;

        internal Pipeline(CodeMemberMethod vertexMethod, DryadLinqCodeGen codeGen, string[] writerNames)
        {
            this.m_vertexMethod = vertexMethod;
            this.m_codeGen = codeGen;
            this.m_readerNames = null;
            this.m_writerNames = writerNames;
            this.m_nodes = new List<DLinqQueryNode>();
        }

        internal string[] ReaderNames
        {
            get { return this.m_readerNames; }
        }

        internal string[] WriterNames
        {
            get { return this.m_writerNames; }
        }

        internal Type InputType
        {
            get { return this.m_nodes[0].InputTypes[0]; }
        }

        internal Type OutputType
        {
            get { return this.m_nodes[this.Length - 1].OutputTypes[0]; }
        }

        internal int Length
        {
            get { return this.m_nodes.Count; }
        }

        internal DLinqQueryNode this[int index]
        {
            get { return this.m_nodes[index]; }
        }

        internal void Add(DLinqQueryNode node)
        {
            this.m_nodes.Add(node);
        }

        internal CodeExpression BuildExpression(int idx,
                                                Expression inputExpr,
                                                params ParameterExpression[] paramList)
        {
            Expression bodyExpr = inputExpr;
            for (int i = idx; i < this.Length; i++)
            {
                bodyExpr = this.m_nodes[i].RebuildQueryExpression(bodyExpr);
            }
            Type type = typeof(Func<,>).MakeGenericType(inputExpr.Type, bodyExpr.Type);
            return this.m_codeGen.MakeExpression(Expression.Lambda(type, bodyExpr, paramList));
        }

        internal void Reset(string[] readerNames)
        {
            this.m_readerNames = readerNames;
            this.m_nodes.Clear();
        }
    }
}
