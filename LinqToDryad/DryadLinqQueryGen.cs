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
using System.Collections.ObjectModel;
using System.Text;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Linq;
using System.Linq.Expressions;
using System.CodeDom;
using System.Xml;
using System.Diagnostics;

using Microsoft.Research.DryadLinq.Internal;
using System.Globalization;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// This class handles code generation for multiple queries and execution invocation.
    /// </summary>
    internal class DryadLinqQueryGen
    {
        internal const int StartPhaseId = -4;
        private const string DryadLinqProgram = "DryadLinqProgram__.xml";
        private const string QueryGraph = "QueryGraph__.txt";
        private const string VertexHostExe = "VertexHost.exe";

        private static int s_uniqueProgId = -1;
        private static object s_queryGenLock = new Object();

        private int m_currentPhaseId = StartPhaseId;
        private int m_nextVertexId = 0;
        private bool m_codeGenDone = false;
        private DryadLinqCodeGen m_codeGen;
        private Expression[] m_queryExprs;
        private DLinqQueryNode[] m_queryPlan1;
        private DLinqQueryNode[] m_queryPlan2;
        private DLinqQueryNode[] m_queryPlan3;
        private Uri[] m_outputTableUris;
        private Expression[] m_serializers;
        private Expression[] m_deserializers;
        private bool[] m_isTempOutput;
        private Type[] m_outputTypes;
        private QueryNodeInfo[] m_queryNodeInfos;
        private DryadLinqQuery[] m_outputTables;
        private string m_DryadLinqProgram;
        private string m_queryGraph;
        private Dictionary<Expression, QueryNodeInfo> m_exprNodeInfoMap;
        private Dictionary<Expression, QueryNodeInfo> m_referencedQueryMap;
        private Dictionary<string, DLinqInputNode> m_inputUriMap;
        private Dictionary<string, DLinqOutputNode> m_outputUriMap;
        private DryadLinqJobExecutor m_queryExecutor;
        private DryadLinqContext m_context;

        // This constructor is specifically to support enumeration of a query.
        // It assumes that the Expressions all terminate with a ToStore node.        
        internal DryadLinqQueryGen(DryadLinqContext context,
                                   VertexCodeGen vertexCodeGen,
                                   Expression queryExpr,
                                   Uri tableUri,
                                   bool isTempOutput)
        {
            this.m_queryExprs = new Expression[] { queryExpr };
            this.m_outputTableUris = new Uri[] { tableUri };
            this.m_serializers = new Expression[] { null };
            this.m_deserializers = new Expression[] { null };
            this.m_isTempOutput = new bool[] { isTempOutput };
            this.m_context = context;
            this.Initialize(vertexCodeGen);
        }

        // This constructor is specifically to support SubmitAndWait() calls.
        // It assumes that the Expressions all terminate with a ToStore node.
        internal DryadLinqQueryGen(DryadLinqContext context,
                                   VertexCodeGen vertexCodeGen,
                                   Expression[] qlist)
        {
            this.m_queryExprs = new Expression[qlist.Length];
            this.m_outputTableUris = new Uri[qlist.Length];
            this.m_serializers = new Expression[qlist.Length];
            this.m_deserializers = new Expression[qlist.Length];
            this.m_isTempOutput = new bool[qlist.Length];
            this.m_context = context;
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                MethodCallExpression mcExpr = qlist[i] as MethodCallExpression;
                
                if (mcExpr != null && mcExpr.Method.Name == ReflectedNames.DLQ_ToStoreInternal)
                {
                    this.m_queryExprs[i] = mcExpr.Arguments[0];
                    ExpressionSimplifier<Uri> e2 = new ExpressionSimplifier<Uri>();
                    this.m_outputTableUris[i] = e2.Eval(mcExpr.Arguments[1]);
                    ExpressionSimplifier<bool> e3 = new ExpressionSimplifier<bool>();
                    this.m_isTempOutput[i] = e3.Eval(mcExpr.Arguments[2]);
                    this.m_serializers[i] = mcExpr.Arguments[3];
                    this.m_deserializers[i] = mcExpr.Arguments[4];
                }
                else if (mcExpr != null && mcExpr.Method.Name == ReflectedNames.DLQ_ToStoreInternalAux)
                {
                    this.m_queryExprs[i] = mcExpr.Arguments[0];
                    ExpressionSimplifier<Uri> e2 = new ExpressionSimplifier<Uri>();
                    this.m_outputTableUris[i] = e2.Eval(mcExpr.Arguments[1]);
                    ExpressionSimplifier<bool> e3 = new ExpressionSimplifier<bool>();
                    this.m_isTempOutput[i] = e3.Eval(mcExpr.Arguments[2]);
                }
                else
                {
                    throw new DryadLinqException("Internal error: The method must be " + ReflectedNames.DLQ_ToStoreInternal);
                }
            }
            this.Initialize(vertexCodeGen);
        }

        private void Initialize(VertexCodeGen vertexCodeGen)
        {
            this.m_codeGen = new DryadLinqCodeGen(this.m_context, vertexCodeGen);
            this.m_queryPlan1 = null;
            this.m_queryPlan2 = null;
            this.m_queryPlan3 = null;
            this.m_DryadLinqProgram = null;
            this.m_queryPlan1 = null;
            this.m_exprNodeInfoMap = new Dictionary<Expression, QueryNodeInfo>();
            this.m_referencedQueryMap = new Dictionary<Expression, QueryNodeInfo>();
            this.m_inputUriMap = new Dictionary<string, DLinqInputNode>();
            this.m_outputUriMap = new Dictionary<string, DLinqOutputNode>();
            this.m_queryExecutor = new DryadLinqJobExecutor(this.m_context);

            // Initialize the data structures for the output tables
            this.m_outputTypes = new Type[this.m_queryExprs.Length];
            this.m_queryNodeInfos = new QueryNodeInfo[this.m_queryExprs.Length];
            
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                this.m_queryNodeInfos[i] = this.BuildNodeInfoGraph(this.m_queryExprs[i]);
                this.m_queryNodeInfos[i] = new DummyQueryNodeInfo(this.m_queryExprs[i], false, this.m_queryNodeInfos[i]);

                if (!DataPath.IsValidDataPath(this.m_outputTableUris[i]))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.UnrecognizedDataSource,
                                                 String.Format(SR.UnrecognizedDataSource,
                                                               this.m_outputTableUris[i].AbsoluteUri));
                }
            }
        }

        internal DryadLinqContext Context
        {
            get { return this.m_context; }
        }

        internal DryadLinqCodeGen CodeGen
        {
            get { return this.m_codeGen; }
        }
        
        internal Dictionary<Expression, QueryNodeInfo> ReferencedQueryMap
        {
            get { return this.m_referencedQueryMap; }
        }

        /// <summary>
        /// Probes the running assembly and its dependencies, and throws an exception
        /// if any of them is targetted to x86. Returns silently if all managed assemblies
        /// in the list are x64 or AnyCPU. Native or unloadable binaries are ignored.
        /// </summary>
        private void CheckAssemblyArchitectures()
        {
            // First create the list of assemblies to probe
            // We use a stripped down version of the resource discovery logic in GenerateDryadProgram.
            //   i) We start with the same set of currently loaded binaries
            //      (== client app + its dependencies + dynamically loaded assemblies)
            //  ii) We take out user specified resource exclusions (this enables a workaround
            //      for x86 assemblies that must be loaded on the client side, think UI plugins,
            //      but aren't needed by the vertex code)
            //
            // The difference is we don't add the vertex DLL, or user resources.
            List<string> resourcesToExclude = new List<string>();
            resourcesToExclude.AddRange(this.m_context.ResourcesToRemove.Select(x => x.ToLower(CultureInfo.InvariantCulture)));

            IEnumerable<string> loadedAssemblyPaths
                = TypeSystem.GetLoadedNonSystemAssemblyPaths().Select(x => x.ToLower(CultureInfo.InvariantCulture));
            var asembliesToCheck = loadedAssemblyPaths.Where(path => !resourcesToExclude.Contains(path));
            foreach (string path in asembliesToCheck)
            {
                Assembly asm = null;
                try
                {
                    asm = Assembly.ReflectionOnlyLoadFrom(path);
                }
                catch
                {
                    // silently ignore load errors
                }

                if (asm != null)
                {
                    PortableExecutableKinds peKind;
                    ImageFileMachine machine;
                    asm.ManifestModule.GetPEKind(out peKind, out machine);

                    // machine will always be reported as "I386" for both true x86 and AnyCPU assemblies
                    // peKind will have the "Required32Bit" flag set only for x86 binaries. Therefore we
                    // use peKind to make our decision.
                    if ((peKind & PortableExecutableKinds.Required32Bit) != 0)
                    {
                        string offendingAssemblyName = Path.GetFileName(path);
                        throw new DryadLinqException(DryadLinqErrorCode.Binaries32BitNotSupported,
                                                     String.Format(SR.Binaries32BitNotSupported, offendingAssemblyName));
                    }
                }
            }
        }

        internal DryadLinqQuery[] Execute()
        {
            lock (s_queryGenLock)
            {
                this.GenerateDryadProgram();

                this.CheckAssemblyArchitectures();

                // Invoke the background execution
                this.m_queryExecutor.ExecuteAsync(this.m_DryadLinqProgram);
                
                // Create the resulting partitioned tables
                this.m_outputTables = new DryadLinqQuery[this.m_outputTableUris.Length];
                MethodInfo minfo = typeof(Microsoft.Research.DryadLinq.DataProvider).GetMethod(
                                              ReflectedNames.DataProvider_GetPartitionedTable,
                                              BindingFlags.NonPublic | BindingFlags.Static);
                for (int i = 0; i < this.m_outputTableUris.Length; i++)
                {
                    MethodInfo minfo1 = minfo.MakeGenericMethod(this.m_outputTypes[i]);
                    LambdaExpression deserializer = null;
                    if (this.m_deserializers[i] != null)
                    {
                        deserializer = DryadLinqExpression.GetLambda(this.m_deserializers[i]);
                    }
                    object[] args = new object[] { this.m_context, this.m_outputTableUris[i], deserializer };
                    this.m_outputTables[i] = (DryadLinqQuery)minfo1.Invoke(null, args);
                    this.m_outputTables[i].QueryExecutor = this.m_queryExecutor;
                }
                return this.m_outputTables;
            }
        }

        // Phase 1 of the query optimization
        internal void GenerateQueryPlanPhase1()
        {
            if (this.m_queryPlan1 != null) return;

            // Apply some simple rewrite rules
            SimpleRewriter rewriter = new SimpleRewriter(this.m_exprNodeInfoMap.Values.ToList());
            rewriter.Rewrite();
            
            // Generate the query plan of phase1
            var referencedNodes = this.m_referencedQueryMap.Values;
            this.m_queryPlan1 = new DLinqQueryNode[this.m_queryExprs.Length + referencedNodes.Count];
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                this.m_queryPlan1[i] = this.Visit(this.m_queryNodeInfos[i].Children[0].Child);
            }
            int idx = this.m_queryExprs.Length;
            foreach (DummyQueryNodeInfo nodeInfo in referencedNodes)
            {
                if (nodeInfo.NeedsMerge)
                {
                    // Add a Tee'd Merge
                    this.m_queryPlan1[idx] = this.Visit(nodeInfo.Children[0].Child);
                    DLinqQueryNode mergeNode = new DLinqMergeNode(true,
                                                                  nodeInfo.QueryExpression,
                                                                  this.m_queryPlan1[idx]);
                    this.m_queryPlan1[idx] = new DLinqTeeNode(mergeNode.OutputTypes[0], true,
                                                              mergeNode.QueryExpression, mergeNode);
                }
                else
                {
                    this.m_queryPlan1[idx] = this.Visit(nodeInfo.Children[0].Child);
                }
                nodeInfo.QueryNode = this.m_queryPlan1[idx];
                idx++;
            }

            // Finally, add the output nodes.
            Dictionary<DLinqQueryNode, int> forkCounts = new Dictionary<DLinqQueryNode, int>();
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                DLinqQueryNode queryNode = this.m_queryPlan1[i];
                int cnt;
                if (!forkCounts.TryGetValue(queryNode, out cnt))
                {
                    cnt = queryNode.Parents.Count;
                }
                forkCounts[queryNode] = cnt + 1;
            }

            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                DryadLinqClientLog.Add("Query " + i + " Output: " + this.m_outputTableUris[i]);

                DLinqQueryNode queryNode = this.m_queryPlan1[i];
                if (TypeSystem.IsAnonymousType(queryNode.OutputTypes[0]))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.OutputTypeCannotBeAnonymous,
                                                 SR.OutputTypeCannotBeAnonymous);
                }

                if (this.m_serializers[i] != null)
                {
                    // Add an Apply for the serializer if it is not null
                    LambdaExpression serializer = DryadLinqExpression.GetLambda(this.m_serializers[i]);
                    DLinqQueryNode applyNode = new DLinqApplyNode(serializer, this.m_queryExprs[i], queryNode);
                    applyNode.OutputDataSetInfo = queryNode.OutputDataSetInfo;
                    queryNode = applyNode;
                }
                else
                {
                    // Add dummy Apply to make Dryad happy (it doesn't like to hook inputs straight to outputs)
                    if ((queryNode is DLinqInputNode) || (forkCounts[queryNode] > 1))
                    {
                        // Add a dummy Apply
                        Type paramType = typeof(IEnumerable<>).MakeGenericType(queryNode.OutputTypes[0]);
                        ParameterExpression param = Expression.Parameter(paramType, "x");
                        Type type = typeof(Func<,>).MakeGenericType(paramType, paramType);
                        LambdaExpression applyExpr = Expression.Lambda(type, param, param);
                        DLinqQueryNode applyNode = new DLinqApplyNode(applyExpr, this.m_queryExprs[i], queryNode);
                        applyNode.OutputDataSetInfo = queryNode.OutputDataSetInfo;
                        queryNode = applyNode;
                    }

                    if (queryNode is DLinqConcatNode)
                    {
                        // Again, we add dummy Apply in certain cases to make Dryad happy
                        ((DLinqConcatNode)queryNode).FixInputs();
                    }
                }

                // Add the output node                
                CompressionScheme outputScheme = this.m_context.OutputDataCompressionScheme;
                DLinqOutputNode outputNode = new DLinqOutputNode(this.m_context,
                                                                 this.m_outputTableUris[i],
                                                                 this.m_isTempOutput[i],
                                                                 outputScheme,
                                                                 this.m_queryExprs[i],
                                                                 queryNode);

                this.m_queryPlan1[i] = outputNode;

                string outputUri = this.m_outputTableUris[i].AbsoluteUri.ToLower();
                if (this.m_outputUriMap.ContainsKey(outputUri))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.MultipleOutputsWithSameDscUri,
                                                 String.Format(SR.MultipleOutputsWithSameUri, this.m_outputTableUris[i]));
                }

                this.m_outputUriMap.Add(outputUri, outputNode);
                this.m_outputTypes[i] = this.m_queryPlan1[i].OutputTypes[0];
                    
                // Remove useless Tees to make Dryad happy                
                if ((queryNode is DLinqTeeNode) && (forkCounts[queryNode] == 1))
                {
                    DLinqQueryNode teeChild = queryNode.Children[0];
                    teeChild.UpdateParent(queryNode, outputNode);
                    outputNode.UpdateChildren(queryNode, teeChild);
                }
            }
        }

        // Phase 2 of the query optimization
        internal DLinqQueryNode[] GenerateQueryPlanPhase2()
        {
            if (this.m_queryPlan2 == null)
            {
                this.GenerateQueryPlanPhase1();
                this.m_queryPlan2 = new DLinqQueryNode[this.m_queryPlan1.Length];
                for (int i = 0; i < this.m_queryPlan1.Length; i++)
                {
                    this.m_queryPlan2[i] = this.VisitPhase2(this.m_queryPlan1[i]);
                }
                this.m_currentPhaseId++;
            }
            return this.m_queryPlan2;
        }

        private DLinqQueryNode VisitPhase2(DLinqQueryNode node)
        {
            DLinqQueryNode resNode = node;
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                if (node is DLinqForkNode)
                {
                    // For now, we require every branch of a fork be used:
                    DLinqForkNode forkNode = (DLinqForkNode)node;
                    for (int i = 0; i < forkNode.Parents.Count; i++)
                    {
                        if ((forkNode.Parents[i] is DLinqTeeNode) &&
                            (forkNode.Parents[i].Parents.Count == 0))
                        {
                            throw DryadLinqException.Create(DryadLinqErrorCode.BranchOfForkNotUsed,
                                                            string.Format(SR.BranchOfForkNotUsed, i),
                                                            node.QueryExpression);
                        }
                    }
                }

                resNode = node.SuperNode;
                if (resNode == null)
                {
                    for (int i = 0; i < node.Children.Length; i++)
                    {
                        node.Children[i] = this.VisitPhase2(node.Children[i]);
                    }
                    resNode = node.PipelineReduce();
                    resNode.m_uniqueId++;

                    // Special treatment for DoWhile
                    DLinqDoWhileNode doWhile = resNode as DLinqDoWhileNode;
                    if (doWhile != null)
                    {
                        doWhile.Body = this.VisitPhase2(doWhile.Body);
                        doWhile.Cond = this.VisitPhase2(doWhile.Cond);
                    }

                    // Insert a Tee node if needed:
                    DLinqQueryNode outputNode = resNode.OutputNode;
                    if (outputNode.IsForked &&
                        !(outputNode is DLinqForkNode) &&
                        !(outputNode is DLinqTeeNode))
                    {
                        resNode = resNode.InsertTee(true);
                    }
                }
            }
            return resNode;
        }

        // Phase 3 of the query optimization
        internal DLinqQueryNode[] GenerateQueryPlanPhase3()
        {
            if (this.m_queryPlan3 == null)
            {
                this.GenerateQueryPlanPhase2();
                this.m_queryPlan3 = this.m_queryPlan2;
                for (int i = 0; i < this.m_queryPlan2.Length; i++)
                {
                    this.VisitPhase3(this.m_queryPlan2[i]);
                }
                this.m_currentPhaseId++;
            }
            return this.m_queryPlan3;
        }

        private void VisitPhase3(DLinqQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                node.m_uniqueId++;

                // Remove some useless Tee nodes
                foreach (DLinqQueryNode child in node.Children)
                {
                    if ((child is DLinqTeeNode) && !child.IsForked)
                    {
                        DLinqQueryNode teeChild = child.Children[0];
                        teeChild.UpdateParent(child, node);
                        node.UpdateChildren(child, teeChild);
                    }
                }

                // Remove some useless Merge nodes
                if ((node is DLinqMergeNode) &&
                    !node.IsForked &&
                    !(node.Parents[0] is DLinqOutputNode) &&
                    !node.Children[0].IsDynamic &&
                    node.Children[0].PartitionCount == 1)
                {
                    node.Children[0].UpdateParent(node, node.Parents[0]);
                    node.Parents[0].UpdateChildren(node, node.Children[0]);
                }

                // Add dynamic managers for tee nodes.
                if ((StaticConfig.DynamicOptLevel & StaticConfig.NoDynamicOpt) != 0 && 
                    node is DLinqTeeNode &&
                    node.DynamicManager.ManagerType == DynamicManagerType.None)
                {
                    // insert a dynamic broadcast manager on Tee
                    node.DynamicManager = DynamicManager.Broadcast;
                }

                // Recurse on the children of node
                foreach (DLinqQueryNode child in node.Children)
                {
                    this.VisitPhase3(child);
                }
                if (node is DLinqDoWhileNode)
                {
                    this.VisitPhase3(((DLinqDoWhileNode)node).Body);
                    this.VisitPhase3(((DLinqDoWhileNode)node).Cond);
                }
            }
        }
        
        // The main method that generates the query plan.
        internal DLinqQueryNode[] QueryPlan()
        {
            return this.GenerateQueryPlanPhase3();
        }

        // Generate the vertex code for all the query nodes.
        internal void CodeGenVisit()
        {
            if (!this.m_codeGenDone)
            {
                DLinqQueryNode[] optimizedPlan = this.QueryPlan();

                // make sure none of the outputs share a URI with inputs
                foreach (var kvp in this.m_outputUriMap)
                {
                    string outputPath = kvp.Key;
                    if (m_inputUriMap.ContainsKey(outputPath))
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.OutputUriAlsoQueryInput,
                                                     String.Format(SR.OutputUriAlsoQueryInput, outputPath));
                    }
                }

                foreach (DLinqQueryNode node in optimizedPlan) 
                {
                    this.CodeGenVisit(node);
                }
                this.m_currentPhaseId++;
                this.m_codeGenDone = true;
            }
        }

        private void CodeGenVisit(DLinqQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                node.m_uniqueId++;

                // We process the types first so that children will also know about all 
                // proxies/mappings that should be used.
                node.CreateCodeAndMappingsForVertexTypes(false);

                // Recurse on the children
                foreach (DLinqQueryNode child in node.Children)
                {
                    this.CodeGenVisit(child);
                }

                switch (node.NodeType)
                {
                    case QueryNodeType.InputTable:
                    {
                        // vertex with no code
                        string t = ((DLinqInputNode)node).Table.DataSourceUri.AbsolutePath;
                        int index = t.LastIndexOf('/');
                        int bk = t.LastIndexOf('\\');
                        if (index < bk) index = bk;
                        node.m_vertexEntryMethod = t.Substring(index + 1);
                        break;
                    }
                    case QueryNodeType.OutputTable:
                    {
                        // vertex with no code
                        string t = ((DLinqOutputNode)node).OutputUri.AbsolutePath;
                        int index = t.LastIndexOf('/');
                        int bk = t.LastIndexOf('\\');
                        if (index < bk) index = bk;
                        int len = Math.Min(8, t.Length - index - 1);
                        node.m_vertexEntryMethod = t.Substring(index + 1, len);
                        break;
                    }
                    case QueryNodeType.Tee:
                    {
                        // vertex with no code
                        node.m_vertexEntryMethod = DryadLinqCodeGen.MakeUniqueName("Tee");
                        // broadcast manager code generation
                        if (node.DynamicManager.ManagerType != DynamicManagerType.None)
                        {
                            node.DynamicManager.CreateVertexCode();
                        }
                        break;
                    }
                    case QueryNodeType.Concat:
                    case QueryNodeType.Dummy:
                    {
                        // vertex with no code
                        node.m_vertexEntryMethod = DryadLinqCodeGen.MakeUniqueName(node.NodeType.ToString());
                        break;
                    }
                    case QueryNodeType.DoWhile:
                    {
                        // vertex with no code
                        node.m_vertexEntryMethod = DryadLinqCodeGen.MakeUniqueName(node.NodeType.ToString());
                        this.CodeGenVisit(((DLinqDoWhileNode)node).Body);
                        this.CodeGenVisit(((DLinqDoWhileNode)node).Cond);
                        break;
                    }
                    default:
                    {
                        CodeMemberMethod vertexMethod = this.m_codeGen.AddVertexMethod(node);
                        node.m_vertexEntryMethod = vertexMethod.Name;
                        node.DynamicManager.CreateVertexCode();
                        break;
                    }
                }
            }
        }

        // Assign unique ids to all the query nodes
        private void AssignUniqueId()
        {
            if (this.m_currentPhaseId != -1)
            {
                //@@TODO: this should not be reachable. could change to Assert/InvalidOpEx
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             "Internal error: Optimization phase should be -1, not " +
                                             this.m_currentPhaseId);
            }
            foreach (DLinqQueryNode node in this.QueryPlan())
            {
                this.AssignUniqueId(node);
            }
        }

        private void AssignUniqueId(DLinqQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                foreach (Pair<ParameterExpression, DLinqQueryNode> refChild in node.GetReferencedQueries())
                {
                    this.AssignUniqueId(refChild.Value);
                }
                foreach (DLinqQueryNode child in node.Children)
                {
                    this.AssignUniqueId(child);
                }
                if (node.m_uniqueId == this.m_currentPhaseId)
                {
                    node.m_uniqueId = this.m_nextVertexId++;

                    
                    // Special treatment for DoWhile
                    DLinqDoWhileNode doWhileNode = node as DLinqDoWhileNode;
                    if (doWhileNode != null)
                    {
                        this.AssignUniqueId(doWhileNode.Body);
                        this.AssignUniqueId(doWhileNode.Cond);
                        this.AssignUniqueId(doWhileNode.BodySource);
                        this.AssignUniqueId(doWhileNode.CondSource1);
                        this.AssignUniqueId(doWhileNode.CondSource2);
                    }

                    // Special treatment for Fork
                    if (node.OutputNode is DLinqForkNode)
                    {
                        foreach (DLinqQueryNode pnode in node.Parents)
                        {
                            if (pnode.m_uniqueId == this.m_currentPhaseId)
                            {
                                pnode.m_uniqueId = this.m_nextVertexId++;
                            }
                        }
                    }
                }
            }
        }

        private void CreateQueryPlan(XmlDocument queryDoc, XmlElement queryPlan)
        {
            this.AssignUniqueId();

            HashSet<int> seen = new HashSet<int>();
            foreach (DLinqQueryNode node in this.QueryPlan())
            {    
                node.AddToQueryPlan(queryDoc, queryPlan, seen);
            }
        }

        /// <summary>
        /// Find the pdb file associated with a given filename.
        /// </summary>
        /// <param name="filename">Filename with debugging information.</param>
        /// <returns>The associated pdb.</returns>
        internal static string FindPDB(string filename)
        {
            string basename = Path.GetFileNameWithoutExtension(filename);
            string directory = Path.GetDirectoryName(filename);
            string pdbname = directory + Path.DirectorySeparatorChar + basename + ".pdb";
            return (File.Exists(pdbname)) ? pdbname : null;
        }

        /// <summary>
        /// Add a resource to the Xml plan.
        /// </summary>
        /// <param name="queryDoc">Document holding the xml plan.</param>
        /// <param name="parent">Parent node.</param>
        /// <param name="resource">Resource to add.</param>
        /// <param name="resourcesToExclude">The resources should be excluded.</param>
        private void AddResourceToPlan(XmlDocument queryDoc,
                                       XmlElement parent,
                                       string resource,
                                       IEnumerable<string> resourcesToExclude)
        {
            AddResourceToPlan_Core(queryDoc, parent, resource, resourcesToExclude);

            if (this.m_context.CompileForVertexDebugging)
            {
                string pdb = FindPDB(resource);
                if (pdb != null)
                {
                    AddResourceToPlan_Core(queryDoc, parent, pdb, resourcesToExclude);
                }
            }
        }

        // Add a resource to the plan unless it was specifically in the exclusions list.
        private void AddResourceToPlan_Core(XmlDocument queryDoc,
                                            XmlElement parent,
                                            string resource,
                                            IEnumerable<string> resourcesToExclude)
        {
            if (resourcesToExclude.Contains(resource, StringComparer.OrdinalIgnoreCase))
            {
                return;
            }
            XmlElement resourceElem = queryDoc.CreateElement("Resource");
            resourceElem.InnerText = m_queryExecutor.AddResource(resource);
            parent.AppendChild(resourceElem);
        }

        private void GenerateAppConfigResource(string appConfigPath)
        {
            // Generates an app config XML for the VertexHost which 
            // 
            // 1) specifies the server GC mode
            //
            // 2) requests a .NET runtime version equal to that of the client
            //    submitting the job (if the client has a .NET version higher than 3.5)
            //    also specifies <supportedRuntime version="v2.0.50727"/> as a fallback
            //    in case the cluster nodes don't have the client's newer .Net version
            //    (v2.0.50727 corresponds to .Net 3.5, both Sp1 and Sp2 pointing to this
            //    version ID, since it's actually the underlying CLR's version). This rule
            //    only kicks in if the query config has the MatchClientNetFrameworkVersion flag set.
            //
            // 3) disables Authenticode checks for the VH by specifying
            //    <generatePublisherEvidence enabled="false"/>. This is necessary
            //    because some cluster nodes don't have an Internet connection, in which
            //    case which the Authenticode check leads to a 20sec delay during process startup.
            //
            // NOTE: useLegacyV2RuntimeActivationPolicy="true" is needed becuase the VH binary is mixed mode.
            string clientVersionString = "";
            if (Environment.Version.Major > 2 && this.m_context.MatchClientNetFrameworkVersion)
            {
                clientVersionString = String.Format(CultureInfo.InvariantCulture,
                                                    @"      <supportedRuntime version=""v{0}.{1}""/>",
                                                    Environment.Version.Major,
                                                    Environment.Version.Minor);
                // NOTE: We use the "v4.0" syntax instead of the longer "v4.0.30319" because as of .NET4
                // the format of this app config tag has been simplified to "major.minor"
            }

            string appConfigBody =
@"<?xml version =""1.0""?>
<configuration>
    <runtime>
        <gcServer enabled=""true""/>
        <gcConcurrent enabled=""false""/>
        <generatePublisherEvidence enabled=""false""/>
        <gcAllowVeryLargeObjects enabled=""true"" />
    </runtime>

    <startup useLegacyV2RuntimeActivationPolicy=""true"">
";

            appConfigBody += clientVersionString;   // add the client specific version string if we generated one.
            appConfigBody +=            
@"
      <supportedRuntime version=""v2.0.50727""/>
    </startup>
</configuration>    
";

            File.WriteAllText(appConfigPath, appConfigBody);
        }

        /// <summary>
        /// Generate the executable code.
        /// </summary>
        /// <returns>The path to the queryPlanXml location</returns>
        internal string GenerateDryadProgram()
        {
            DryadLinqObjectStore.Clear();

            // Any resource that we try to add will be tested against this list.
            IEnumerable<string> resourcesToExclude = this.m_context.ResourcesToRemove;

            // BuildDryadLinqAssembly:
            //   1. Performs query optimizations
            //   2. Generate vertex code for all the nodes in the query plan
            this.m_codeGen.BuildDryadLinqAssembly(this);

            // DryadQueryExplain explain = new DryadQueryExplain();
            // DryadLinqClientLog.Add("{0}", explain.Explain(this));

            // Finally, write out the query plan.
            if (this.m_DryadLinqProgram == null)
            {
                int progId = Interlocked.Increment(ref s_uniqueProgId);
                this.m_DryadLinqProgram = DryadLinqCodeGen.GetPathForGeneratedFile(DryadLinqProgram, progId);
                this.m_queryGraph = DryadLinqCodeGen.GetPathForGeneratedFile(QueryGraph, progId); 
            }

            XmlDocument queryDoc = new XmlDocument();
            queryDoc.LoadXml("<Query></Query>");

            // Write the assembly version information
            Assembly dryadlinqassembly = Assembly.GetExecutingAssembly();
            AssemblyName asmName = dryadlinqassembly.GetName();
            XmlElement elem = queryDoc.CreateElement("DryadLinqVersion");
            elem.InnerText = asmName.Version.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the cluster name element
            elem = queryDoc.CreateElement("ClusterName");
            elem.InnerText = this.m_context.HeadNode;
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the minimum number of nodes
            int minComputeNodes = 1; 
            if (this.m_context.JobMinNodes.HasValue)
            {
                minComputeNodes = this.m_context.JobMinNodes.Value;
            }
            elem = queryDoc.CreateElement("MinimumComputeNodes");
            elem.InnerText = minComputeNodes.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the maximum number of nodes
            int maxComputeNodes = -1;
            if (this.m_context.JobMaxNodes.HasValue)
            {
               maxComputeNodes = this.m_context.JobMaxNodes.Value;
            }
            elem = queryDoc.CreateElement("MaximumComputeNodes");
            elem.InnerText = maxComputeNodes.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // intermediate data compression
            elem = queryDoc.CreateElement("IntermediateDataCompression");
            elem.InnerText = ((int)this.m_context.IntermediateDataCompressionScheme).ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Speculative Duplication Node
            elem = queryDoc.CreateElement("EnableSpeculativeDuplication");
            elem.InnerText = this.m_context.EnableSpeculativeDuplication.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the visualization element
            elem = queryDoc.CreateElement("Visualization");
            elem.InnerText = "none";
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the query name element
            elem = queryDoc.CreateElement("QueryName");
            if (String.IsNullOrEmpty(this.m_context.JobFriendlyName))
            {
                elem.InnerText = asmName.Name; 
            }
            else
            {
                elem.InnerText = this.m_context.JobFriendlyName;
            }
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the XmlExecHostArgs element
            elem = queryDoc.CreateElement("XmlExecHostArgs");
            queryDoc.DocumentElement.AppendChild(elem);

            // Add an element for each argument
            string[] args = StaticConfig.XmlExecHostArgs.Split(new char[] {' '}, StringSplitOptions.RemoveEmptyEntries);
            List<string> xmlExecResources = new List<string>();
            for (int i = 0; i < args.Length; ++i)
            {
                string arg = args[i];
                XmlElement argElem = queryDoc.CreateElement("Argument");
                argElem.InnerText = arg;
                elem.AppendChild(argElem);
                if (arg.Equals("-bw") || arg.Equals("-r"))
                {
                    xmlExecResources.Add(args[i+1]);
                }
            }

            // Add the resources element
            elem = queryDoc.CreateElement("Resources");
            queryDoc.DocumentElement.AppendChild(elem);

            // Add resource item for this LINQ DLL
            AddResourceToPlan(queryDoc, elem, this.m_codeGen.GetTargetLocation(), resourcesToExclude);
            
            // Add resource item for each loaded DLL that isn't a system DLL 
            IEnumerable<string> loadedAssemblyPaths = TypeSystem.GetLoadedNonSystemAssemblyPaths();
            foreach(string assemblyPath in loadedAssemblyPaths)
            {
                AddResourceToPlan(queryDoc, elem, assemblyPath, resourcesToExclude);
            }

            // Add the xmlExec resources
            foreach (string resourcePath in xmlExecResources)
            {
                AddResourceToPlan(queryDoc, elem, resourcePath, resourcesToExclude);
            }

            // Add codegen resources
            foreach (string resourcePath in this.m_codeGen.VertexCodeGen.ResourcesToAdd())
            {
                AddResourceToPlan(queryDoc, elem, resourcePath, resourcesToExclude);
            }

            // Create an app config file for the VertexHost process, and add it to the resources
            // don't generate this since we're shipping a real vertexhost.exe.config now
            //string vertexHostAppConfigPath = DryadLinqCodeGen.GetPathForGeneratedFile(Path.ChangeExtension(VertexHostExe, "exe.config"), null);
            //GenerateAppConfigResource(vertexHostAppConfigPath);
            //AddResourceToPlan(queryDoc, elem, vertexHostAppConfigPath, resourcesToExclude);

            // Save and add the object store as a resource
            if (!DryadLinqObjectStore.IsEmpty)
            {
                DryadLinqObjectStore.Save();
                AddResourceToPlan(queryDoc, elem, DryadLinqObjectStore.GetClientSideObjectStorePath(), resourcesToExclude);
            }

            // Add resource item for user-added resources
            foreach (string userResource in this.m_context.ResourcesToAdd.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                AddResourceToPlan(queryDoc, elem, userResource, resourcesToExclude);
            }

            // Add the query plan element
            XmlElement queryPlanElem = queryDoc.CreateElement("QueryPlan");
            queryDoc.DocumentElement.AppendChild(queryPlanElem);

            // Add the query tree as a sequence of nodes
            this.CreateQueryPlan(queryDoc, queryPlanElem);

            // Finally, save the DryadQuery doc to a file
            queryDoc.Save(this.m_DryadLinqProgram);

            return this.m_DryadLinqProgram;
        }

        private void BuildReferencedQuery(Expression expr)
        {
            ExpressionQuerySet querySet = new ExpressionQuerySet();
            querySet.Visit(expr);
            foreach (Expression qexpr in querySet.QuerySet)
            {
                QueryNodeInfo nodeInfo = this.BuildNodeInfoGraph(qexpr);
                this.m_referencedQueryMap[qexpr] = new DummyQueryNodeInfo(qexpr, true, nodeInfo);
            }
        }

        private void BuildReferencedQuery(int startIdx, ReadOnlyCollection<Expression> exprs)
        {
            ExpressionQuerySet querySet = new ExpressionQuerySet();
            for (int i = startIdx; i < exprs.Count; i++)
            {
                querySet.Visit(exprs[i]);
            }
            foreach (Expression qexpr in querySet.QuerySet)
            {
                QueryNodeInfo nodeInfo = this.BuildNodeInfoGraph(qexpr);
                this.m_referencedQueryMap[qexpr] = new DummyQueryNodeInfo(qexpr, true, nodeInfo);
            }
        }

        // Document what the 'NodeInfo' and 'ReferencedQuery' system does
        private QueryNodeInfo BuildNodeInfoGraph(Expression expression)
        {
            QueryNodeInfo resNodeInfo = null;
            if (this.m_exprNodeInfoMap.TryGetValue(expression, out resNodeInfo))
            {
                return resNodeInfo;
            }
            MethodCallExpression mcExpr = expression as MethodCallExpression;
            if (mcExpr != null && mcExpr.Method.IsStatic && TypeSystem.IsQueryOperatorCall(mcExpr))
            {
                switch (mcExpr.Method.Name)
                {
                    case "Join":
                    case "GroupJoin":
                    case "Union":
                    case "Intersect":
                    case "Except":
                    case "Zip":
                    case "SequenceEqual":
                    case "SequenceEqualAsQuery":
                    {
                        QueryNodeInfo child1 = this.BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        QueryNodeInfo child2 = this.BuildNodeInfoGraph(mcExpr.Arguments[1]);
                        resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        this.BuildReferencedQuery(2, mcExpr.Arguments);
                        break;
                    }
                    case "Concat":                                                
                    {
                        NewArrayExpression others = mcExpr.Arguments[1] as NewArrayExpression;
                        if (others == null)
                        {
                            QueryNodeInfo child1 = this.BuildNodeInfoGraph(mcExpr.Arguments[0]);
                            QueryNodeInfo child2 = this.BuildNodeInfoGraph(mcExpr.Arguments[1]);
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        }
                        else
                        {
                            QueryNodeInfo[] infos = new QueryNodeInfo[others.Expressions.Count + 1];
                            infos[0] = this.BuildNodeInfoGraph(mcExpr.Arguments[0]);
                            for (int i = 0; i < others.Expressions.Count; ++i)
                            {
                                infos[i + 1] = this.BuildNodeInfoGraph(others.Expressions[i]);
                            }
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, infos);
                        }
                        break;
                    }
                    case "Apply":
                    case "ApplyPerPartition":
                    {
                        QueryNodeInfo child1 = this.BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        if (mcExpr.Arguments.Count == 2)
                        {
                            // Apply with only one input source
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1);
                            this.BuildReferencedQuery(mcExpr.Arguments[1]);
                        }
                        else
                        {
                            LambdaExpression lambda = DryadLinqExpression.GetLambda(mcExpr.Arguments[2]);
                            NewArrayExpression others = mcExpr.Arguments[1] as NewArrayExpression;
                            if (lambda.Parameters.Count == 2 && others == null)
                            {
                                // Apply with two input sources
                                QueryNodeInfo child2 = this.BuildNodeInfoGraph(mcExpr.Arguments[1]);
                                resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                            }
                            else
                            {
                                // Apply with multiple sources
                                QueryNodeInfo[] infos = new QueryNodeInfo[others.Expressions.Count + 1];
                                infos[0] = child1;
                                for (int i = 0; i < others.Expressions.Count; ++i)
                                {
                                    infos[i + 1] = this.BuildNodeInfoGraph(others.Expressions[i]);
                                }
                                resNodeInfo = new QueryNodeInfo(mcExpr, true, infos);
                            }
                            this.BuildReferencedQuery(mcExpr.Arguments[2]);
                        }
                        break;
                    }
                    case "RangePartition":
                    {
                        QueryNodeInfo child1 = this.BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        if (mcExpr.Arguments.Count == 6)
                        {
                            // This is a key part of handling for RangePartition( ... , IQueryable<> keysQuery, ...)
                            // The keys expression is established as a child[1] nodeInfo for the rangePartition node.
                            QueryNodeInfo child2 = this.BuildNodeInfoGraph(mcExpr.Arguments[2]);
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        }
                        else
                        {
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1);
                        }
                        this.BuildReferencedQuery(mcExpr.Arguments[1]);
                        break;
                    }
                    case "DoWhile":
                    {
                        QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        Expression bodyExpr = mcExpr.Arguments[1];
                        QueryNodeInfo bodyNodeInfo = this.BuildNodeInfoGraph(bodyExpr);
                        Expression condExpr = mcExpr.Arguments[2];
                        QueryNodeInfo condNodeInfo = this.BuildNodeInfoGraph(condExpr);
                        Expression bodySourceExpr = mcExpr.Arguments[3];
                        QueryNodeInfo bodySourceNodeInfo = this.BuildNodeInfoGraph(bodySourceExpr);
                        Expression condSourceExpr1 = mcExpr.Arguments[4];
                        QueryNodeInfo condSourceNodeInfo1 = this.BuildNodeInfoGraph(condSourceExpr1);
                        Expression condSourceExpr2 = mcExpr.Arguments[5];
                        QueryNodeInfo condSourceNodeInfo2 = this.BuildNodeInfoGraph(condSourceExpr2);
                        resNodeInfo = new DoWhileQueryNodeInfo(mcExpr, bodyNodeInfo,
                                                               condNodeInfo,
                                                               bodySourceNodeInfo,
                                                               condSourceNodeInfo1, condSourceNodeInfo2,
                                                               child1);
                        break;
                    }
                    case "Dummy":
                    {
                        // Just return a QueryNodeInfo with no child.
                        resNodeInfo = new QueryNodeInfo(mcExpr, true);
                        break;
                    }
                    default:
                    {
                        QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        resNodeInfo = new QueryNodeInfo(mcExpr, true, child1);
                        this.BuildReferencedQuery(1, mcExpr.Arguments);
                        break;
                    }
                }
            }
            if (resNodeInfo == null)
            {
                resNodeInfo = new QueryNodeInfo(expression, false);
            }
            this.m_exprNodeInfoMap.Add(expression, resNodeInfo);
            return resNodeInfo;
        }

        private static bool IsMergeNodeNeeded(DLinqQueryNode node)
        {
            return node.IsDynamic || node.PartitionCount > 1;
        }
        
        internal DLinqQueryNode Visit(QueryNodeInfo nodeInfo)
        {
            Expression expression = nodeInfo.QueryExpression;
            if (expression.NodeType == ExpressionType.Call)
            {
                MethodCallExpression mcExpr = (MethodCallExpression)expression;
                if (mcExpr.Method.IsStatic && TypeSystem.IsQueryOperatorCall(mcExpr))
                {
                    return this.VisitQueryOperatorCall(nodeInfo);
                }
                
                throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                                String.Format(SR.OperatorNotSupported, mcExpr.Method.Name),
                                                expression);
            }
            else if (expression.NodeType == ExpressionType.Constant)
            {
                DLinqInputNode inputNode = new DLinqInputNode(this, (ConstantExpression)expression);
                string inputUri = inputNode.Table.DataSourceUri.AbsoluteUri.ToLower();
                if (!this.m_inputUriMap.ContainsKey(inputUri))
                {
                    this.m_inputUriMap.Add(inputUri, inputNode);
                }
                DLinqQueryNode resNode = inputNode;
                if (inputNode.Table.Deserializer != null)
                {
                    // Add an Apply for the deserializer
                    resNode = new DLinqApplyNode(inputNode.Table.Deserializer, expression, inputNode);
                }
                return resNode;
            }
            else
            {
                string errMsg = "Can't handle expression of type " + expression.NodeType;
                throw DryadLinqException.Create(DryadLinqErrorCode.UnsupportedExpressionsType,
                                                String.Format(SR.UnsupportedExpressionsType,expression.NodeType),
                                                expression);
            }
        }

        private static bool IsLambda(Expression expr, int n)
        {
            LambdaExpression lambdaExpr = DryadLinqExpression.GetLambda(expr);
            return (lambdaExpr != null && lambdaExpr.Parameters.Count == n);
        }

        // Checks if the child of the source node is a groupby node without any result selectors.
        private static bool IsGroupByWithoutResultSelector(Expression source)
        {
            bool isGroupBy = false;
            if (source.NodeType == ExpressionType.Call)
            {
                MethodCallExpression expression = (MethodCallExpression)source;
                if (expression.Method.IsStatic &&
                    TypeSystem.IsQueryOperatorCall(expression) &&
                    expression.Method.Name == "GroupBy")
                {
                    if (expression.Arguments.Count == 2)
                    {
                        isGroupBy = true;
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        isGroupBy = !IsLambda(expression.Arguments[2], 2);
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        isGroupBy = !(IsLambda(expression.Arguments[2], 2) ||
                                      IsLambda(expression.Arguments[3], 2));
                    }
                }
            }
            return isGroupBy;
        }

        private DLinqQueryNode CreateOffset(bool isLong, Expression queryExpr, DLinqQueryNode child)
        {
            // Count node
            DLinqQueryNode countNode = new DLinqBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                   true, false, queryExpr, child);

            // Apply node for x => Offsets(x)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(typeof(long));
            ParameterExpression param = Expression.Parameter(paramType, "x");
            MethodInfo minfo = typeof(DryadLinqEnumerable).GetMethod("Offsets");
            Expression body = Expression.Call(minfo, param, Expression.Constant(isLong, typeof(bool)));
            Type type = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression procFunc = Expression.Lambda(type, body, param);
            DLinqQueryNode mergeCountNode = new DLinqMergeNode(true, queryExpr, countNode);
            DLinqQueryNode offsetsNode = new DLinqApplyNode(procFunc, queryExpr, mergeCountNode);

            // HashPartition
            LambdaExpression keySelectExpr = IdentityFunction.Instance(typeof(IndexedValue<long>));
            int pcount = child.OutputPartition.Count;
            DLinqQueryNode hdistNode = new DLinqHashPartitionNode(keySelectExpr, null, null, pcount,
                                                                  false, queryExpr, offsetsNode);
            DLinqQueryNode resNode = new DLinqMergeNode(false, queryExpr, hdistNode);
            return resNode;
        }

        private DLinqQueryNode PromoteConcat(QueryNodeInfo source,
                                             DLinqQueryNode sourceNode,
                                             Func<DLinqQueryNode, DLinqQueryNode> func)
        {
            DLinqQueryNode resNode = sourceNode;
            if ((resNode is DLinqConcatNode) && !source.IsForked)
            {
                DLinqQueryNode[] children = resNode.Children;
                DLinqQueryNode[] newChildren = new DLinqQueryNode[children.Length];
                for (int i = 0; i < children.Length; i++)
                {
                    children[i].Parents.Remove(resNode);
                    newChildren[i] = func(children[i]);
                }
                resNode = new DLinqConcatNode(source.QueryExpression, newChildren);
            }
            else
            {
                resNode = func(resNode);
            }
            return resNode;
        }
  
        private DLinqQueryNode VisitSelect(QueryNodeInfo source,
                                           QueryNodeType nodeType,
                                           LambdaExpression selector,
                                           LambdaExpression resultSelector,
                                           MethodCallExpression queryExpr)
        {
            DLinqQueryNode selectNode;
            if (selector.Type.GetGenericArguments().Length == 2)
            {
                // If this select's child is a groupby node, push this select into its child, if
                //   1. The groupby node is not tee'd, and
                //   2. The groupby node has no result selector, and
                //   3. The selector is decomposable
                if (!source.IsForked &&
                    IsGroupByWithoutResultSelector(source.QueryExpression) &&
                    Decomposition.GetDecompositionInfoList(selector, m_codeGen) != null)
                {
                    MethodCallExpression expr = (MethodCallExpression)source.QueryExpression;
                    LambdaExpression keySelectExpr = DryadLinqExpression.GetLambda(expr.Arguments[1]);

                    // Figure out elemSelectExpr and comparerExpr
                    LambdaExpression elemSelectExpr = null;
                    Expression comparerExpr = null;
                    if (expr.Arguments.Count == 3)
                    {
                        elemSelectExpr = DryadLinqExpression.GetLambda(expr.Arguments[2]);
                        if (elemSelectExpr == null)
                        {
                            comparerExpr = expr.Arguments[2];
                        }
                    }
                    else if (expr.Arguments.Count == 4)
                    {
                        elemSelectExpr = DryadLinqExpression.GetLambda(expr.Arguments[2]);
                        comparerExpr = expr.Arguments[3];
                    }

                    // Construct new query expression by building result selector expression
                    // and pushing it to groupby node.
                    selectNode = VisitGroupBy(source.Children[0].Child, keySelectExpr,
                                              elemSelectExpr, selector, comparerExpr, queryExpr);
                    if (nodeType == QueryNodeType.SelectMany)
                    {
                        Type selectorRetType = selector.Type.GetGenericArguments()[1];
                        LambdaExpression id = IdentityFunction.Instance(selectorRetType);
                        selectNode = new DLinqSelectNode(nodeType, id, resultSelector, queryExpr, selectNode);
                    }
                }
                else
                {
                    DLinqQueryNode child = this.Visit(source);
                    selectNode = this.PromoteConcat(
                                         source, child,
                                         x => new DLinqSelectNode(nodeType, selector, resultSelector, queryExpr, x));
                }
            }
            else
            {
                // The "indexed" version
                DLinqQueryNode child = this.Visit(source);
                if (!child.IsDynamic && child.OutputPartition.Count == 1)
                {
                    selectNode = this.PromoteConcat(
                                         source, child,
                                         x => new DLinqSelectNode(nodeType, selector, resultSelector, queryExpr, x));
                }
                else
                {
                    child.IsForked = true;

                    // Create (x, y) => Select(x, y, selector)
                    Type ptype1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                    Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                    ParameterExpression param1 = Expression.Parameter(ptype1, DryadLinqCodeGen.MakeUniqueName("x"));
                    ParameterExpression param2 = Expression.Parameter(ptype2, DryadLinqCodeGen.MakeUniqueName("y"));
                    
                    string methodName = queryExpr.Method.Name;
                    Type[] selectorTypeArgs = selector.Type.GetGenericArguments();
                    Type typeArg2 = selectorTypeArgs[selectorTypeArgs.Length - 1];
                    if (nodeType == QueryNodeType.SelectMany)
                    {
                        if (resultSelector != null)
                        {
                            methodName += "Result";
                        }
                        typeArg2 = typeArg2.GetGenericArguments()[0];
                    }

                    string targetMethodName = methodName + "WithStartIndex";
                    MethodInfo minfo = typeof(DryadLinqEnumerable).GetMethod(targetMethodName);
                    Expression body;
                    if (resultSelector == null)
                    {
                        minfo = minfo.MakeGenericMethod(child.OutputTypes[0], typeArg2);
                        body = Expression.Call(minfo, param1, param2, selector);
                    }
                    else
                    {
                        minfo = minfo.MakeGenericMethod(child.OutputTypes[0], typeArg2, resultSelector.Body.Type);
                        body = Expression.Call(minfo, param1, param2, selector, resultSelector);
                    }
                    Type type = typeof(Func<,,>).MakeGenericType(ptype1, ptype2, body.Type);
                    LambdaExpression procFunc = Expression.Lambda(type, body, param1, param2);

                    bool isLong = methodName.StartsWith("Long", StringComparison.Ordinal);
                    DLinqQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, child);
                    selectNode = new DLinqApplyNode(procFunc, queryExpr, child, offsetNode);
                }
            }
            return selectNode;
        }

        private DLinqQueryNode VisitWhere(QueryNodeInfo source,
                                          LambdaExpression predicate,
                                          MethodCallExpression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            DLinqQueryNode whereNode;            
            if (predicate.Type.GetGenericArguments().Length == 2 ||
                (!child.IsDynamic && child.OutputPartition.Count == 1))
            {
                whereNode = this.PromoteConcat(source, child, x => new DLinqWhereNode(predicate, queryExpr, x));
            }
            else
            {
                // The "indexed" version
                // Create (x, y) => DryadWhere(x, y, predicate)
                Type ptype1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                ParameterExpression param1 = Expression.Parameter(ptype1, DryadLinqCodeGen.MakeUniqueName("x"));
                ParameterExpression param2 = Expression.Parameter(ptype2, DryadLinqCodeGen.MakeUniqueName("y"));
                string targetMethod = queryExpr.Method.Name + "WithStartIndex";
                MethodInfo minfo = typeof(DryadLinqEnumerable).GetMethod(targetMethod);
                minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param1, param2, predicate);
                Type type = typeof(Func<,,>).MakeGenericType(ptype1, ptype2, body.Type);
                LambdaExpression procFunc = Expression.Lambda(type, body, param1, param2);

                child.IsForked = true;
                bool isLong = (queryExpr.Method.Name == "LongWhere");
                DLinqQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, child);
                whereNode = new DLinqApplyNode(procFunc, queryExpr, child, offsetNode);
            }
            return whereNode;
        }

        private DLinqQueryNode VisitJoin(QueryNodeInfo outerSource,
                                         QueryNodeInfo innerSource,
                                         QueryNodeType nodeType,
                                         LambdaExpression outerKeySelector,
                                         LambdaExpression innerKeySelector,
                                         LambdaExpression resultSelector,
                                         Expression comparerExpr,
                                         Expression queryExpr)
        {
            DLinqQueryNode outerChild = this.Visit(outerSource);
            DLinqQueryNode innerChild = this.Visit(innerSource);
            DLinqQueryNode joinNode = null;

            Type keyType = outerKeySelector.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }

            // The comparer object:
            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            if (outerChild.IsDynamic || innerChild.IsDynamic)
            {
                // Well, let us do the simplest thing for now
                outerChild = new DLinqHashPartitionNode(outerKeySelector,
                                                        comparerExpr,
                                                        StaticConfig.DefaultPartitionCount,
                                                        queryExpr,
                                                        outerChild);
                if (IsMergeNodeNeeded(outerChild))
                {
                    outerChild = new DLinqMergeNode(false, queryExpr, outerChild);
                }
                
                innerChild = new DLinqHashPartitionNode(innerKeySelector,
                                                        comparerExpr,
                                                        StaticConfig.DefaultPartitionCount,
                                                        queryExpr,
                                                        innerChild);
                if (IsMergeNodeNeeded(innerChild))
                {
                    innerChild = new DLinqMergeNode(false, queryExpr, innerChild);
                }

                joinNode = new DLinqJoinNode(nodeType,
                                             "Hash" + nodeType,
                                             outerKeySelector,
                                             innerKeySelector,
                                             resultSelector,
                                             comparerExpr,
                                             queryExpr,
                                             outerChild,
                                             innerChild);
                return joinNode;
            }


            bool isOuterDescending = outerChild.OutputDataSetInfo.orderByInfo.IsDescending;
            bool isInnerDescending = innerChild.OutputDataSetInfo.orderByInfo.IsDescending;

            // Partition outer and inner if needed
            if (outerChild.OutputPartition.ParType == PartitionType.Range &&
                outerChild.OutputPartition.HasKeys &&
                outerChild.OutputPartition.IsPartitionedBy(outerKeySelector, comparer))
            {
                if (innerChild.OutputPartition.ParType != PartitionType.Range ||
                    !innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer) ||
                    !outerChild.OutputPartition.IsSamePartition(innerChild.OutputPartition))
                {
                    // Range distribute inner using outer's partition.
                    innerChild = outerChild.OutputPartition.CreatePartitionNode(innerKeySelector, innerChild);
                    if (IsMergeNodeNeeded(innerChild))
                    {
                        innerChild = new DLinqMergeNode(false, queryExpr, innerChild);
                    }
                }
            }
            else if (innerChild.OutputPartition.ParType == PartitionType.Range &&
                     innerChild.OutputPartition.HasKeys &&
                     innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer))
            {
                // Range distribute outer using inner's partition.
                outerChild = innerChild.OutputPartition.CreatePartitionNode(outerKeySelector, outerChild);
                if (IsMergeNodeNeeded(outerChild))
                {
                    outerChild = new DLinqMergeNode(false, queryExpr, outerChild);
                }
            }
            else if (outerChild.OutputPartition.ParType == PartitionType.Hash &&
                     outerChild.OutputPartition.IsPartitionedBy(outerKeySelector, comparer))
            {
                if (innerChild.OutputPartition.ParType != PartitionType.Hash ||
                    !innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer) ||
                    !outerChild.OutputPartition.IsSamePartition(innerChild.OutputPartition))
                {
                    innerChild = new DLinqHashPartitionNode(innerKeySelector,
                                                            comparerExpr,
                                                            outerChild.OutputPartition.Count,
                                                            queryExpr,
                                                            innerChild);
                    if (IsMergeNodeNeeded(innerChild))
                    {
                        innerChild = new DLinqMergeNode(false, queryExpr, innerChild);
                    }
                }
            }
            else if (innerChild.OutputPartition.ParType == PartitionType.Hash &&
                     innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer))
            {
                outerChild = new DLinqHashPartitionNode(outerKeySelector,
                                                        comparerExpr,
                                                        innerChild.OutputPartition.Count,
                                                        queryExpr,
                                                        outerChild);
                if (IsMergeNodeNeeded(outerChild))
                {
                    outerChild = new DLinqMergeNode(false, queryExpr, outerChild);
                }
            }
            else
            {
                // No luck. Hash partition both outer and inner
                int parCnt = Math.Max(outerChild.OutputPartition.Count, innerChild.OutputPartition.Count);
                if (parCnt > 1)
                {
                    outerChild = new DLinqHashPartitionNode(outerKeySelector,
                                                            comparerExpr,
                                                            parCnt,
                                                            queryExpr,
                                                            outerChild);
                    if (IsMergeNodeNeeded(outerChild))
                    {
                        outerChild = new DLinqMergeNode(false, queryExpr, outerChild);
                    }

                    innerChild = new DLinqHashPartitionNode(innerKeySelector,
                                                            comparerExpr,
                                                            parCnt,
                                                            queryExpr,
                                                            innerChild);
                    if (IsMergeNodeNeeded(innerChild))
                    {
                        innerChild = new DLinqMergeNode(false, queryExpr, innerChild);
                    }
                }
            }

            // Perform either merge or hash join
            string opName = "Hash";
            if (outerChild.IsOrderedBy(outerKeySelector, comparer))
            {
                if (!innerChild.IsOrderedBy(innerKeySelector, comparer) ||
                    isOuterDescending != isInnerDescending)
                {
                    // Sort inner if unsorted                    
                    innerChild = new DLinqOrderByNode(innerKeySelector, comparerExpr,
                                                      isOuterDescending, queryExpr, innerChild);
                }
                opName = "Merge";
            }
            else if (innerChild.IsOrderedBy(innerKeySelector, comparer))
            {
                if (!outerChild.IsOrderedBy(outerKeySelector, comparer) ||
                    isOuterDescending != isInnerDescending)
                {
                    // Sort outer if unsorted
                    outerChild = new DLinqOrderByNode(outerKeySelector, comparerExpr,
                                                      isInnerDescending, queryExpr, outerChild);
                }
                opName = "Merge";                
            }
                    
            joinNode = new DLinqJoinNode(nodeType,
                                         opName + nodeType,
                                         outerKeySelector,
                                         innerKeySelector,
                                         resultSelector,
                                         comparerExpr,
                                         queryExpr,
                                         outerChild,
                                         innerChild);
            return joinNode;
        }

        private DLinqQueryNode VisitDistinct(QueryNodeInfo source,
                                             Expression comparerExpr,
                                             Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = child.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }

            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            LambdaExpression keySelectExpr = IdentityFunction.Instance(keyType);
            if (!child.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                if (child.IsDynamic || child.OutputPartition.Count > 1)
                {
                    child = new DLinqDistinctNode(true, comparerExpr, queryExpr, child);
                    bool isDynamic = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicHashPartitionLevel) != 0;
                    child = new DLinqHashPartitionNode(keySelectExpr,
                                                       comparerExpr,
                                                       child.OutputPartition.Count,
                                                       isDynamic,
                                                       queryExpr,
                                                       child);
                    child = new DLinqMergeNode(false, queryExpr, child);
                }
            }
            DLinqQueryNode resNode = new DLinqDistinctNode(false, comparerExpr, queryExpr, child);
            return resNode;
        }

        private DLinqQueryNode VisitConcat(QueryNodeInfo source, MethodCallExpression queryExpr)
        {
            DLinqQueryNode[] childs = new DLinqQueryNode[source.Children.Count];
            for (int i = 0; i < source.Children.Count; ++i)
            {
                childs[i] = this.Visit(source.Children[i].Child);
            }
            DLinqQueryNode resNode = new DLinqConcatNode(queryExpr, childs);

            int parCount = resNode.OutputPartition.Count;
            if (!resNode.IsDynamic && parCount > StaticConfig.MaxPartitionCount)
            {
                // Too many partitions, need to repartition
                int newParCount = parCount / 2;
                DLinqQueryNode countNode = new DLinqBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                       true, false, queryExpr, resNode);
                DLinqQueryNode mergeCountNode = new DLinqMergeNode(true, queryExpr, countNode);

                // Apply node for s => IndexedCount(s)
                Type paramType = typeof(IEnumerable<>).MakeGenericType(typeof(long));
                ParameterExpression param = Expression.Parameter(paramType, "s");
                MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("IndexedCount");
                minfo = minfo.MakeGenericMethod(typeof(long));
                Expression body = Expression.Call(minfo, param);
                Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
                LambdaExpression indexedCountFunc = Expression.Lambda(funcType, body, param);
                DLinqQueryNode indexedCountNode = new DLinqApplyNode(indexedCountFunc, queryExpr, mergeCountNode);

                // HashPartition(x => x.index, parCount)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                Expression keySelectBody = Expression.Property(param, "Index");                
                funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
                LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
                DLinqQueryNode distCountNode = new DLinqHashPartitionNode(keySelectExpr,
                                                                          null,
                                                                          parCount,
                                                                          queryExpr,
                                                                          indexedCountNode);

                // Apply node for (x, y) => AddPartitionIndex(x, y, newParCount)
                ParameterExpression param1 = Expression.Parameter(body.Type, "x");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(resNode.OutputTypes[0]);
                ParameterExpression param2 = Expression.Parameter(paramType2, "y");
                minfo = typeof(DryadLinqHelper).GetMethod("AddPartitionIndex");
                minfo = minfo.MakeGenericMethod(resNode.OutputTypes[0]);
                body = Expression.Call(minfo, param1, param2, Expression.Constant(newParCount));
                funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression addIndexFunc = Expression.Lambda(funcType, body, param1, param2);
                DLinqQueryNode addIndexNode = new DLinqApplyNode(addIndexFunc, queryExpr, distCountNode, resNode);

                // HashPartition(x => x.index, x => x.value, newParCount)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                body = Expression.Property(param, "Index");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                keySelectExpr = Expression.Lambda(funcType, body, param);
                body = Expression.Property(param, "Value");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
                resNode = new DLinqHashPartitionNode(keySelectExpr,
                                                     resultSelectExpr,
                                                     null,
                                                     newParCount,
                                                     false,
                                                     queryExpr,
                                                     addIndexNode);
                resNode = new DLinqMergeNode(true, queryExpr, resNode);
            }
            return resNode;
        }

        private DLinqQueryNode VisitSetOperation(QueryNodeInfo source1,
                                                 QueryNodeInfo source2,
                                                 QueryNodeType nodeType,
                                                 Expression comparerExpr,
                                                 Expression queryExpr)
        {
            DLinqQueryNode child1 = this.Visit(source1);
            DLinqQueryNode child2 = this.Visit(source2);
            DLinqQueryNode resNode = null;

            Type keyType = child1.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }

            // The comparer object:
            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            LambdaExpression keySelectExpr = IdentityFunction.Instance(keyType);
            if (child1.IsDynamic || child2.IsDynamic)
            {
                // Well, let us do the simplest thing for now
                child1 = new DLinqHashPartitionNode(keySelectExpr,
                                                    null,
                                                    StaticConfig.DefaultPartitionCount,
                                                    queryExpr,
                                                    child1);
                if (IsMergeNodeNeeded(child1))
                {
                    child1 = new DLinqMergeNode(false, queryExpr, child1);
                }

                child2 = new DLinqHashPartitionNode(keySelectExpr,
                                                    null,
                                                    StaticConfig.DefaultPartitionCount,
                                                    queryExpr,
                                                    child2);
                if (IsMergeNodeNeeded(child2))
                {
                    child2 = new DLinqMergeNode(false, queryExpr, child2);
                }

                resNode = new DLinqSetOperationNode(nodeType, nodeType.ToString(), comparerExpr,
                                                    queryExpr, child1, child2);
                return resNode;
            }

            bool isDescending1 = child1.OutputDataSetInfo.orderByInfo.IsDescending;
            bool isDescending2 = child2.OutputDataSetInfo.orderByInfo.IsDescending;

            // Partition child1 and child2 if needed
            if (child1.OutputPartition.ParType == PartitionType.Range &&
                child1.OutputPartition.HasKeys &&
                child1.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                if (child2.OutputPartition.ParType != PartitionType.Range ||
                    !child2.OutputPartition.IsPartitionedBy(keySelectExpr, comparer) ||
                    child1.OutputPartition.IsSamePartition(child2.OutputPartition))
                {
                    // Range distribute child2 using child1's partition
                    child2 = child1.OutputPartition.CreatePartitionNode(keySelectExpr, child2);
                    if (IsMergeNodeNeeded(child2))
                    {
                        child2 = new DLinqMergeNode(false, queryExpr, child2);
                    }
                }
            }
            else if (child2.OutputPartition.ParType == PartitionType.Range &&
                     child2.OutputPartition.HasKeys &&
                     child2.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                // Range distribute child1 using child2's partition
                child1 = child2.OutputPartition.CreatePartitionNode(keySelectExpr, child1);
                if (IsMergeNodeNeeded(child1))
                {
                    child1 = new DLinqMergeNode(false, queryExpr, child1);
                }
            }
            else if (child1.OutputPartition.ParType == PartitionType.Hash &&
                     child1.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                if (child2.OutputPartition.ParType != PartitionType.Hash ||
                    !child2.OutputPartition.IsPartitionedBy(keySelectExpr, comparer) ||
                    !child1.OutputPartition.IsSamePartition(child2.OutputPartition))
                {
                    // Hash distribute child2:
                    child2 = new DLinqHashPartitionNode(keySelectExpr,
                                                        comparerExpr,
                                                        child1.OutputPartition.Count,
                                                        queryExpr,
                                                        child2);
                    if (IsMergeNodeNeeded(child2))
                    {
                        child2 = new DLinqMergeNode(false, queryExpr, child2);
                    }
                }
            }
            else if (child2.OutputPartition.ParType == PartitionType.Hash &&
                     child2.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                child1 = new DLinqHashPartitionNode(keySelectExpr,
                                                    comparerExpr,
                                                    child2.OutputPartition.Count,
                                                    queryExpr,
                                                    child1);
                if (IsMergeNodeNeeded(child1))
                {
                    child1 = new DLinqMergeNode(false, queryExpr, child1);
                }
            }
            else
            {
                // No luck. Hash distribute both child1 and child2, then perform hash operation
                int parCnt = Math.Max(child1.OutputPartition.Count, child2.OutputPartition.Count);
                if (parCnt > 1)
                {
                    child1 = new DLinqHashPartitionNode(keySelectExpr, comparerExpr, parCnt, queryExpr, child1);
                    if (IsMergeNodeNeeded(child1))
                    {
                        child1 = new DLinqMergeNode(false, queryExpr, child1);
                    }

                    child2 = new DLinqHashPartitionNode(keySelectExpr, comparerExpr, parCnt, queryExpr, child2);
                    if (IsMergeNodeNeeded(child2))
                    {
                        child2 = new DLinqMergeNode(false, queryExpr, child2);
                    }
                }
            }

            // Perform either hash or ordered operation
            string opName = "";
            if (child1.IsOrderedBy(keySelectExpr, comparer))
            {
                if (!child1.IsOrderedBy(keySelectExpr, comparer) ||
                    isDescending1 != isDescending2)
                {
                    // Sort inner if unsorted                    
                    child2 = new DLinqOrderByNode(keySelectExpr, comparerExpr, isDescending1, queryExpr, child2);
                }
                opName = "Ordered";
            }
            else if (child2.IsOrderedBy(keySelectExpr, comparer))
            {
                if (!child1.IsOrderedBy(keySelectExpr, comparer) ||
                    isDescending1 != isDescending2)
                {
                    // Sort outer if unsorted
                    child1 = new DLinqOrderByNode(keySelectExpr, comparerExpr, isDescending2, queryExpr, child1);
                }
                opName = "Ordered";
            }

            resNode = new DLinqSetOperationNode(nodeType, opName + nodeType, comparerExpr, queryExpr, child1, child2);
            return resNode;
        }

        private DLinqQueryNode VisitContains(QueryNodeInfo source,
                                             Expression valueExpr,
                                             Expression comparerExpr,
                                             bool isQuery,
                                             Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = child.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }

            DLinqQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DLinqContainsNode(valueExpr, comparerExpr, queryExpr, x));
            resNode = new DLinqBasicAggregateNode(null, AggregateOpType.Any, false, isQuery, queryExpr, resNode);
            return resNode;
        }

        private DLinqQueryNode VisitQuantifier(QueryNodeInfo source,
                                               LambdaExpression lambda,
                                               AggregateOpType aggType,
                                               bool isQuery,
                                               Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            DLinqQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DLinqBasicAggregateNode(
                                                         lambda, aggType, true, isQuery, queryExpr, x));
            resNode = new DLinqBasicAggregateNode(null, aggType, false, isQuery, queryExpr, resNode);
            return resNode;
        }

        private DLinqQueryNode VisitAggregate(QueryNodeInfo source,
                                              Expression seed,
                                              LambdaExpression funcLambda,
                                              LambdaExpression resultLambda,
                                              Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            DLinqQueryNode resNode = child;
            if (DryadLinqExpression.IsAssociative(funcLambda))
            {
                LambdaExpression combinerLambda = DryadLinqExpression.GetAssociativeCombiner(funcLambda);
                ResourceAttribute attrib = AttributeSystem.GetResourceAttrib(funcLambda);
                bool funcIsExpensive = (attrib != null && attrib.IsExpensive); 
                resNode = this.PromoteConcat(
                                  source, child,
                                  x => new DLinqAggregateNode("AssocAggregate", seed,
                                                              funcLambda, combinerLambda, null,
                                                              1, queryExpr, x, false));
                resNode = new DLinqAggregateNode("AssocAggregate", null, combinerLambda, combinerLambda, resultLambda,
                                                 3, queryExpr, resNode, funcIsExpensive);
            }
            else
            {
                resNode = new DLinqAggregateNode("Aggregate", seed, funcLambda, null, resultLambda, 3,
                                                 queryExpr, child, false);
            }
            return resNode;
        }

        private DLinqQueryNode VisitBasicAggregate(QueryNodeInfo source,
                                                   LambdaExpression lambda,
                                                   AggregateOpType aggType,
                                                   bool isQuery,
                                                   Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            if (aggType == AggregateOpType.Min || aggType == AggregateOpType.Max)
            {
                Type elemType = child.OutputTypes[0];
                if (lambda != null)
                {
                    elemType = lambda.Body.Type;
                }
                if (!TypeSystem.HasDefaultComparer(elemType))
                {
                    throw DryadLinqException.Create(DryadLinqErrorCode.AggregationOperatorRequiresIComparable,
                                                    String.Format(SR.AggregationOperatorRequiresIComparable, aggType ),
                                                    queryExpr);
                }
            }
            DLinqQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DLinqBasicAggregateNode(lambda, aggType, true, isQuery, queryExpr, x));
            
            switch (aggType)
            {
                case AggregateOpType.Count:
                case AggregateOpType.LongCount:
                {
                    resNode = new DLinqBasicAggregateNode(null, AggregateOpType.Sum, false,
                                                          isQuery, queryExpr, resNode);
                    break;
                }
                case AggregateOpType.Sum:
                case AggregateOpType.Min:
                case AggregateOpType.Max:
                case AggregateOpType.Average:
                {
                    resNode = new DLinqBasicAggregateNode(null, aggType, false,
                                                          isQuery, queryExpr, resNode);
                    break;
                }
                default:
                {
                    throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                                    String.Format(SR.OperatorNotSupported, aggType),
                                                    queryExpr);
                }
            }
            return resNode;
        }

        private DLinqQueryNode VisitGroupBy(QueryNodeInfo source,
                                            LambdaExpression keySelectExpr,
                                            LambdaExpression elemSelectExpr,
                                            LambdaExpression resultSelectExpr,
                                            Expression comparerExpr,
                                            Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            
            ExpressionInfo einfo = new ExpressionInfo(keySelectExpr);
            if (einfo.IsExpensive)
            {
                // Any method call that is not tagged as "expensive=false" will be deemed expensive.
                // if the keySelector is expensive, we rewrite the query so that the key-function is invoked only once
                // and the record key passed around via a Pair<TKey,TRecord>.
                // keyFunc becomes pair=>pair.Key
                // elementSelector must be rewritten so that references to (record) become (pair.Value)

                Type[] vkTypes = keySelectExpr.Type.GetGenericArguments();
                Type pairType = typeof(Pair<,>).MakeGenericType(vkTypes[1], vkTypes[0]);
                ParameterExpression pairParam = Expression.Parameter(pairType, "e");

                // Add Select(x => new Pair<K,S>(key(x), x))
                ParameterExpression valueParam = keySelectExpr.Parameters[0];
                Expression body = Expression.New(pairType.GetConstructors()[0], keySelectExpr.Body, valueParam);
                Type delegateType = typeof(Func<,>).MakeGenericType(valueParam.Type, body.Type);
                LambdaExpression selectExpr = Expression.Lambda(delegateType, body, valueParam);
                child = new DLinqSelectNode(QueryNodeType.Select, selectExpr, null, queryExpr, child);
                
                // Change keySelector to e => e.Key
                PropertyInfo keyInfo = pairParam.Type.GetProperty("Key");
                body = Expression.Property(pairParam, keyInfo);
                delegateType = typeof(Func<,>).MakeGenericType(pairParam.Type, body.Type);
                keySelectExpr = Expression.Lambda(delegateType, body, pairParam);

                // Add or change elementSelector with e.Value
                PropertyInfo valueInfo = pairParam.Type.GetProperty("Value");
                body = Expression.Property(pairParam, valueInfo);
                if (elemSelectExpr != null)
                {
                    ParameterSubst subst = new ParameterSubst(elemSelectExpr.Parameters[0], body);
                    body = subst.Visit(elemSelectExpr.Body);
                }
                delegateType = typeof(Func<,>).MakeGenericType(pairParam.Type, body.Type);
                elemSelectExpr = Expression.Lambda(delegateType, body, pairParam);
            }

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }
            Type elemType;
            if (elemSelectExpr == null)
            {
                elemType = keySelectExpr.Type.GetGenericArguments()[0];
            }
            else
            {
                elemType = elemSelectExpr.Type.GetGenericArguments()[1];
            }
                 
            // The comparer object:
            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            LambdaExpression keySelectExpr1 = keySelectExpr;
            LambdaExpression elemSelectExpr1 = elemSelectExpr;
            LambdaExpression resultSelectExpr1 = resultSelectExpr;
            LambdaExpression seedExpr1 = null;
            LambdaExpression accumulateExpr1 = null;
            
            List<DecompositionInfo> dInfoList = null;
            if (resultSelectExpr != null)
            {
                dInfoList = Decomposition.GetDecompositionInfoList(resultSelectExpr, this.m_codeGen);
            }

            String groupByOpName = "GroupBy";
            DLinqQueryNode groupByNode = child;
            bool isPartitioned = child.OutputPartition.IsPartitionedBy(keySelectExpr, comparer);
            if (dInfoList != null)
            {
                // ** Decomposable GroupBy-Reduce
                // This block creates the first GroupByNode and does some preparation for subsequent nodes.
                if (child.IsOrderedBy(keySelectExpr, comparer))
                {
                    groupByOpName = "OrderedGroupBy";
                }

                int dcnt = dInfoList.Count;
                ParameterExpression keyParam;
                if (resultSelectExpr.Parameters.Count == 1)
                {
                    keyParam = Expression.Parameter(keyType, DryadLinqCodeGen.MakeUniqueName("k"));
                }
                else
                {
                    keyParam = resultSelectExpr.Parameters[0];
                }

                // Seed:
                ParameterExpression param2 = Expression.Parameter(
                                                 elemType, DryadLinqCodeGen.MakeUniqueName("e"));
                Expression zeroExpr = Expression.Constant(0, typeof(int));
                Expression seedBody = zeroExpr;
                if (dcnt != 0)
                {
                    LambdaExpression seed = dInfoList[dcnt-1].Seed;
                    ParameterSubst subst = new ParameterSubst(seed.Parameters[0], param2);
                    seedBody = subst.Visit(seed.Body);
                    for (int i = dcnt - 2; i >= 0; i--)
                    {
                        seed = dInfoList[i].Seed;
                        subst = new ParameterSubst(seed.Parameters[0], param2);
                        Expression firstExpr = subst.Visit(seed.Body);
                        Type newPairType = typeof(Pair<,>).MakeGenericType(firstExpr.Type, seedBody.Type);
                        seedBody = Expression.New(newPairType.GetConstructors()[0], firstExpr, seedBody);
                    }
                }
                LambdaExpression seedExpr = Expression.Lambda(seedBody, param2);

                // Accumulate:
                ParameterExpression param1 = Expression.Parameter(
                                                 seedBody.Type, DryadLinqCodeGen.MakeUniqueName("a"));
                Expression accumulateBody = zeroExpr;
                if (dcnt != 0)
                {
                    accumulateBody = Decomposition.AccumulateList(param1, param2, dInfoList, 0);
                }
                LambdaExpression accumulateExpr = Expression.Lambda(accumulateBody, param1, param2);

                // Now prepare for the merge-aggregator and/or in the secondary group-by.
                // keySelectExpr1: e => e.Key
                Type reducerResType = typeof(Pair<,>).MakeGenericType(keyParam.Type, accumulateBody.Type);                
                ParameterExpression reducerResParam = Expression.Parameter(reducerResType, "e");
                PropertyInfo keyInfo = reducerResParam.Type.GetProperty("Key");
                Expression body = Expression.Property(reducerResParam, keyInfo);
                Type delegateType = typeof(Func<,>).MakeGenericType(reducerResParam.Type, body.Type);
                keySelectExpr1 = Expression.Lambda(delegateType, body, reducerResParam);

                // elemSelectExpr1: e => e.Value
                PropertyInfo valueInfo = reducerResParam.Type.GetProperty("Value");
                body = Expression.Property(reducerResParam, valueInfo);
                delegateType = typeof(Func<,>).MakeGenericType(reducerResParam.Type, body.Type);
                elemSelectExpr1 = Expression.Lambda(delegateType, body, reducerResParam);

                // SeedExpr1
                param2 = Expression.Parameter(elemSelectExpr1.Body.Type,
                                              DryadLinqCodeGen.MakeUniqueName("e"));
                seedExpr1 = Expression.Lambda(param2, param2);
                
                // AccumulateExpr1
                Expression recursiveAccumulateBody = zeroExpr;
                if (dcnt != 0)
                {
                    recursiveAccumulateBody = Decomposition.RecursiveAccumulateList(param1, param2, dInfoList, 0);
                }
                accumulateExpr1 = Expression.Lambda(recursiveAccumulateBody, param1, param2);
                
                // resultSelectExpr1
                resultSelectExpr1 = null;

                // The first groupByNode.  
                // If the input was already correctly partitioned, this will be the only groupByNode.
                bool isPartial = StaticConfig.GroupByLocalAggregationIsPartial && !isPartitioned;
                groupByNode = new DLinqGroupByNode(
                                       groupByOpName, keySelectExpr, elemSelectExpr, null,
                                       seedExpr, accumulateExpr, accumulateExpr1, comparerExpr,
                                       isPartial, queryExpr, child);
            }
            else
            {
                // Can't do partial aggregation.
                // Use sort, mergesort, and ordered groupby, if TKey implements IComparable.
                if ((comparer != null && TypeSystem.IsComparer(comparer, keyType)) ||
                    (comparer == null && TypeSystem.HasDefaultComparer(keyType)))
                {
                    if (!child.IsOrderedBy(keySelectExpr, comparer))
                    {
                        groupByNode = new DLinqOrderByNode(keySelectExpr, comparerExpr, true, queryExpr, child);
                    }
                    groupByOpName = "OrderedGroupBy";
                }

                // Add a GroupByNode if it is partitioned or has elementSelector.
                // If the input was already correctly partitioned, this will be the only groupByNode.
                if (isPartitioned)
                {
                    groupByNode = new DLinqGroupByNode(groupByOpName,
                                                       keySelectExpr,
                                                       elemSelectExpr,
                                                       resultSelectExpr,
                                                       null,  // seed
                                                       null,  // accumulate
                                                       null,  // recursiveAccumulate
                                                       comparerExpr,
                                                       false, // isPartial
                                                       queryExpr,
                                                       groupByNode);
                }
                else if (elemSelectExpr != null)
                {
                    // Local GroupBy without resultSelector:
                    groupByNode = new DLinqGroupByNode(groupByOpName,
                                                       keySelectExpr,
                                                       elemSelectExpr,
                                                       null,  // resultSelect
                                                       null,  // seed
                                                       null,  // accumulate
                                                       null,  // recursiveAccumulate
                                                       comparerExpr,
                                                       StaticConfig.GroupByLocalAggregationIsPartial,  // isPartial
                                                       queryExpr,
                                                       groupByNode);
                    
                    // keySelectExpr1: g => g.Key
                    ParameterExpression groupParam = Expression.Parameter(groupByNode.OutputTypes[0], "g");
                    PropertyInfo keyInfo = groupParam.Type.GetProperty("Key");
                    Expression body = Expression.Property(groupParam, keyInfo);
                    Type delegateType = typeof(Func<,>).MakeGenericType(groupParam.Type, body.Type);
                    keySelectExpr1 = Expression.Lambda(delegateType, body, groupParam);

                    // No elementSelector
                    elemSelectExpr1 = null;

                    // resultSelectExpr1
                    ParameterExpression keyParam;
                    Type groupType = typeof(IEnumerable<>).MakeGenericType(groupByNode.OutputTypes[0]);
                    groupParam = Expression.Parameter(groupType, DryadLinqCodeGen.MakeUniqueName("g"));
                    if (resultSelectExpr == null)
                    {
                        // resultSelectExpr1: (k, g) => MakeDryadLinqGroup(k, g)
                        keyParam = Expression.Parameter(keySelectExpr1.Body.Type, DryadLinqCodeGen.MakeUniqueName("k"));
                        MethodInfo groupingInfo = typeof(DryadLinqEnumerable).GetMethod("MakeDryadLinqGroup");
                        groupingInfo = groupingInfo.MakeGenericMethod(keyParam.Type, elemType);
                        body = Expression.Call(groupingInfo, keyParam, groupParam);
                    }
                    else
                    {
                        // resultSelectExpr1: (k, g) => resultSelectExpr(k, FlattenGroups(g))
                        keyParam = resultSelectExpr.Parameters[0];
                        MethodInfo flattenInfo = typeof(DryadLinqEnumerable).GetMethod("FlattenGroups");
                        flattenInfo = flattenInfo.MakeGenericMethod(keyParam.Type, elemType);
                        Expression groupExpr = Expression.Call(flattenInfo, groupParam);
                        ParameterSubst subst = new ParameterSubst(resultSelectExpr.Parameters[1], groupExpr);
                        body = subst.Visit(resultSelectExpr.Body);
                    }
                    delegateType = typeof(Func<,,>).MakeGenericType(keyParam.Type, groupParam.Type, body.Type);
                    resultSelectExpr1 = Expression.Lambda(delegateType, body, keyParam, groupParam);
                }
            }

            // At this point, the first GroupByNode has been created.
            DLinqQueryNode groupByNode1 = groupByNode;
            DLinqMergeNode mergeNode = null;
            if (!isPartitioned)
            {
                // Create HashPartitionNode, MergeNode, and second GroupByNode

                // Note, if we are doing decomposable-GroupByReduce, there is still some work to go after this
                //   - attach the combiner to the first merge-node
                //   - attach the combiner to the merge-node
                //   - attach finalizer to second GroupBy
                int parCount = (groupByNode.IsDynamic) ? StaticConfig.DefaultPartitionCount : groupByNode.OutputPartition.Count;
                bool isDynamic = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicHashPartitionLevel) != 0;
                DLinqQueryNode hdistNode = new DLinqHashPartitionNode(keySelectExpr1, comparerExpr, parCount,
                                                                      isDynamic, queryExpr, groupByNode);

                // Create the Merge Node
                if (groupByOpName == "OrderedGroupBy")
                {
                    // Mergesort with the same keySelector of the hash partition 
                    mergeNode = new DLinqMergeNode(keySelectExpr1, comparerExpr, true, queryExpr, hdistNode);
                }
                else
                {
                    // Random merge
                    mergeNode = new DLinqMergeNode(false, queryExpr, hdistNode);
                }
                groupByNode1 = new DLinqGroupByNode(groupByOpName, keySelectExpr1, elemSelectExpr1,
                                                    resultSelectExpr1, seedExpr1, accumulateExpr1,
                                                    accumulateExpr1, comparerExpr, false, queryExpr,
                                                    mergeNode);
            }

            // Final tidy-up for decomposable GroupBy-Reduce pattern.
            //   - attach combiner to first GroupByNode
            //   - attache combiner to MergeNode as an aggregator
            //   - build a SelectNode to project out results and call finalizer on them.
            if (dInfoList != null)
            {
                // Add dynamic aggregator to the merge-node, if applicable
                if (StaticConfig.GroupByDynamicReduce && !isPartitioned)
                {
                    mergeNode.AddAggregateNode(groupByNode1);
                }
                
                // Add the final Select node
                Type keyResultPairType = typeof(Pair<,>).MakeGenericType(keyType, seedExpr1.Body.Type);
                ParameterExpression keyResultPairParam = Expression.Parameter(keyResultPairType,
                                                                              DryadLinqCodeGen.MakeUniqueName("e"));
                PropertyInfo valuePropInfo_1 = keyResultPairType.GetProperty("Value");
                Expression combinedValueExpr = Expression.Property(keyResultPairParam, valuePropInfo_1);

                // First, build the combinerList
                int dcnt = dInfoList.Count;
                Expression[] combinerList = new Expression[dcnt];
                for (int i = 0; i < dcnt; i++)
                {
                    if (i + 1 == dcnt)
                    {
                        combinerList[i] = combinedValueExpr;
                    }
                    else
                    {
                        PropertyInfo keyPropInfo = combinedValueExpr.Type.GetProperty("Key");
                        combinerList[i] = Expression.Property(combinedValueExpr, keyPropInfo);
                        PropertyInfo valuePropInfo = combinedValueExpr.Type.GetProperty("Value");
                        combinedValueExpr = Expression.Property(combinedValueExpr, valuePropInfo);
                    }
                    LambdaExpression finalizerExpr = dInfoList[i].FinalReducer;
                    if (finalizerExpr != null)
                    {
                        ParameterSubst subst = new ParameterSubst(finalizerExpr.Parameters[0], combinerList[i]);
                        combinerList[i] = subst.Visit(finalizerExpr.Body);
                    }
                }

                // Build the funcList
                Expression[] funcList = new Expression[dcnt];
                for (int i = 0; i < dcnt; i++)
                {
                    funcList[i] = dInfoList[i].Func;
                }

                // Apply the substitutions
                CombinerSubst combinerSubst = new CombinerSubst(resultSelectExpr, keyResultPairParam, funcList, combinerList);
                Expression finalizerSelectBody = combinerSubst.Visit();

                // Finally, the Select node
                Type delegateType = typeof(Func<,>).MakeGenericType(keyResultPairType, finalizerSelectBody.Type);
                LambdaExpression selectExpr = Expression.Lambda(delegateType, finalizerSelectBody, keyResultPairParam);
                groupByNode1 = new DLinqSelectNode(QueryNodeType.Select, selectExpr, null, queryExpr, groupByNode1);
            }
            return groupByNode1;
        }

        // Creates an "auto-sampling range-partition sub-query"
        private DLinqQueryNode CreateRangePartition(bool isDynamic,
                                                    LambdaExpression keySelectExpr,
                                                    LambdaExpression resultSelectExpr,
                                                    Expression comparerExpr,
                                                    Expression isDescendingExpr,
                                                    Expression queryExpr,
                                                    Expression partitionCountExpr,
                                                    DLinqQueryNode child)
        {
            // Make child a Tee node
            child.IsForked = true;

            // The partition count
            Expression countExpr = null;
            if (isDescendingExpr == null)
            {
                isDescendingExpr = Expression.Constant(false, typeof(bool));  //default for isDescending is false.
            }

            // NOTE: for MayRTM, isDynamic should never be true
            if (!isDynamic)
            {
                if (partitionCountExpr != null)
                {
                    countExpr = partitionCountExpr;
                }
                else
                {
                    // If partitionCount was not explicitly set, use the child's partition count.
                    countExpr = Expression.Constant(child.OutputPartition.Count);
                }
            }
            
            Type recordType = child.OutputTypes[0];
            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];

            // Create x => Phase1Sampling(x_1, keySelector, denv)
            Type lambdaParamType1 = typeof(IEnumerable<>).MakeGenericType(recordType);
            ParameterExpression lambdaParam1 = Expression.Parameter(lambdaParamType1, "x_1");
            
            ParameterExpression denvParam = Expression.Parameter(typeof(VertexEnv), "denv");

            MethodInfo minfo = typeof(DryadLinqSampler).GetMethod("Phase1Sampling");
            Expression body = Expression.Call(minfo.MakeGenericMethod(recordType, keyType),
                                              lambdaParam1, keySelectExpr, denvParam);
            Type type = typeof(Func<,>).MakeGenericType(lambdaParam1.Type, body.Type);
            LambdaExpression samplingExpr = Expression.Lambda(type, body, lambdaParam1);

            // Create the Sampling node
            DLinqApplyNode samplingNode = new DLinqApplyNode(samplingExpr, queryExpr, child);

            // Create x => RangeSampler(x, keySelectExpr, comparer, isDescendingExpr)
            Type lambdaParamType = typeof(IEnumerable<>).MakeGenericType(keyType);
            ParameterExpression lambdaParam = Expression.Parameter(lambdaParamType, "x_2");
            
            //For RTM, isDynamic should never be true.
            //string methodName = (isDynamic) ? "RangeSampler_Dynamic" : "RangeSampler_Static";
            Debug.Assert(isDynamic == false, "Internal error: isDynamic is true.");
            string methodName = "RangeSampler_Static";
            
            minfo = typeof(DryadLinqSampler).GetMethod(methodName);
            minfo = minfo.MakeGenericMethod(keyType);
            Expression comparerArgExpr = comparerExpr;
            if (comparerExpr == null)
            {
                if (!TypeSystem.HasDefaultComparer(keyType))
                {
                    throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                    string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                    queryExpr);
                }
                comparerArgExpr = Expression.Constant(null, typeof(IComparer<>).MakeGenericType(keyType));
            }

            Expression lastArg;
            if (isDynamic)
            {
                lastArg = denvParam;
            }
            else
            {
                lastArg = countExpr;
            }

            body = Expression.Call(minfo, lambdaParam, comparerArgExpr, isDescendingExpr, lastArg);
            type = typeof(Func<,>).MakeGenericType(lambdaParam.Type, body.Type);
            LambdaExpression samplerExpr = Expression.Lambda(type, body, lambdaParam);

            // Create the sample node
            DLinqQueryNode sampleDataNode = new DLinqMergeNode(false, queryExpr, samplingNode);
            DLinqQueryNode sampleNode = new DLinqApplyNode(samplerExpr, queryExpr, sampleDataNode);
            sampleNode.IsForked = true;

            // Create the range distribute node
            DLinqQueryNode resNode = new DLinqRangePartitionNode(keySelectExpr,
                                                                 resultSelectExpr,
                                                                 null,
                                                                 comparerExpr,
                                                                 isDescendingExpr,
                                                                 countExpr,
                                                                 queryExpr,
                                                                 child,
                                                                 sampleNode);
            resNode = new DLinqMergeNode(false, queryExpr, resNode);

            // Set the dynamic manager for sampleNode
            if (isDynamic)
            {
                sampleDataNode.DynamicManager = new DynamicRangeDistributor(resNode);
            }

            return resNode;
        }
        
        private DLinqQueryNode VisitOrderBy(QueryNodeInfo source,
                                            LambdaExpression keySelectExpr,
                                            Expression comparerExpr,
                                            bool isDescending,
                                            Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                queryExpr);
            }

            DLinqQueryNode resNode = child;
            if (child.OutputPartition.ParType == PartitionType.Range &&
                child.OutputPartition.IsPartitionedBy(keySelectExpr, comparerExpr, isDescending))
            {
                // Only need to sort each partition
                resNode = new DLinqOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, child);
            }
            else
            {
                Expression isDescendingExpr = Expression.Constant(isDescending);
                bool dynamicOptEnabled = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicRangePartitionLevel) != 0;
                if (dynamicOptEnabled || child.IsDynamic)
                {
                    // NOTE: for MayRTM, this path should not be taken.
                    resNode = this.CreateRangePartition(true, keySelectExpr, null, comparerExpr,
                                                        isDescendingExpr, queryExpr, null, child);
                    resNode = new DLinqOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, resNode);
                }
                else
                {
                    if (child.OutputPartition.Count > 1)
                    {
                        resNode = this.CreateRangePartition(false, keySelectExpr, null, comparerExpr,
                                                            isDescendingExpr, queryExpr, null, child);
                    }
                    resNode = new DLinqOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, resNode);
                }
            }
            return resNode;
        }

        private DLinqQueryNode FirstStagePartitionOp(string opName,
                                                     QueryNodeType nodeType,
                                                     Expression controlExpr,
                                                     MethodCallExpression queryExpr,
                                                     DLinqQueryNode child)
        {
            if (nodeType == QueryNodeType.TakeWhile)
            {
                Type ptype = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                ParameterExpression param = Expression.Parameter(ptype, DryadLinqCodeGen.MakeUniqueName("x"));
                MethodInfo minfo = typeof(DryadLinqEnumerable).GetMethod("GroupTakeWhile");
                minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param, controlExpr);
                Type type = typeof(Func<,>).MakeGenericType(ptype, body.Type);
                LambdaExpression procFunc = Expression.Lambda(type, body, param);
                return new DLinqApplyNode(procFunc, queryExpr, child);
            }
            else
            {
                return new DLinqPartitionOpNode(opName, nodeType, controlExpr, true, queryExpr, child);
            }
        }
        
        private DLinqQueryNode VisitPartitionOp(string opName,
                                                QueryNodeInfo source,
                                                QueryNodeType nodeType,
                                                Expression controlExpr,
                                                MethodCallExpression queryExpr)
        {
            DLinqQueryNode resNode;
            if (nodeType == QueryNodeType.TakeWhile &&
                controlExpr.Type.GetGenericArguments().Length != 2)
            {
                // The "indexed" version.
                resNode = this.Visit(source);

                // The following block used to be skipped for resNode.OutputPartition.Count == 1,
                // which causes compilation error (bug 13593)
                // @@TODO[p3] : implement a working optimization for nPartition==1 that calls
                //              directly to Linq TakeWhile.
                // Note: the test is: if (resNode.IsDynamic || resNode.OutputPartition.Count > 1)
                {
                    resNode.IsForked = true;

                    bool isLong = (queryExpr.Method.Name == "LongTakeWhile");
                    DLinqQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, resNode);

                    // Create (x, y) => GroupIndexedTakeWhile(x, y, controlExpr)
                    Type ptype1 = typeof(IEnumerable<>).MakeGenericType(resNode.OutputTypes[0]);
                    Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                    ParameterExpression param1 = Expression.Parameter(ptype1, DryadLinqCodeGen.MakeUniqueName("x"));
                    ParameterExpression param2 = Expression.Parameter(ptype2, DryadLinqCodeGen.MakeUniqueName("y"));
                    string methodName = "GroupIndexed" + queryExpr.Method.Name;
                    MethodInfo minfo = typeof(DryadLinqEnumerable).GetMethod(methodName);
                    minfo = minfo.MakeGenericMethod(resNode.OutputTypes[0]);
                    Expression body = Expression.Call(minfo, param1, param2, controlExpr);
                    Type type = typeof(Func<,,>).MakeGenericType(ptype1, ptype2, body.Type);
                    LambdaExpression procFunc = Expression.Lambda(type, body, param1, param2);
                    resNode = new DLinqApplyNode(procFunc, queryExpr, resNode, offsetNode);
                }
            }
            else if (!source.IsForked &&
                     (nodeType == QueryNodeType.Take || nodeType == QueryNodeType.TakeWhile) &&
                     (source.OperatorName == "OrderBy" || source.OperatorName == "OrderByDescending"))
            {
                resNode = this.Visit(source.Children[0].Child);
                
                bool isDescending = (source.OperatorName == "OrderByDescending");
                MethodCallExpression sourceQueryExpr = (MethodCallExpression)source.QueryExpression;
                LambdaExpression keySelectExpr = DryadLinqExpression.GetLambda(sourceQueryExpr.Arguments[1]);
                Expression comparerExpr = null;
                if (sourceQueryExpr.Arguments.Count == 3)
                {
                    comparerExpr = sourceQueryExpr.Arguments[2];
                }
                resNode = this.PromoteConcat(
                                  source.Children[0].Child,
                                  resNode,
                                  delegate(DLinqQueryNode x) {
                                      DLinqQueryNode y = new DLinqOrderByNode(keySelectExpr, comparerExpr,
                                                                              isDescending, sourceQueryExpr, x);
                                      return FirstStagePartitionOp(opName, nodeType, controlExpr, queryExpr, y);
                                  });
                if (resNode.IsDynamic || resNode.OutputPartition.Count > 1)
                {
                    // Need a mergesort
                    resNode = new DLinqMergeNode(keySelectExpr, comparerExpr, isDescending, sourceQueryExpr, resNode);
                }
            }
            else
            {
                resNode = this.Visit(source);
                if (nodeType == QueryNodeType.Take || nodeType == QueryNodeType.TakeWhile)
                {
                    resNode = this.PromoteConcat(
                                      source, resNode,
                                      x => FirstStagePartitionOp(opName, nodeType, controlExpr, queryExpr, x));
                }
            }
            resNode = new DLinqPartitionOpNode(opName, nodeType, controlExpr, false, queryExpr, resNode);
            return resNode;
        }

        private DLinqQueryNode VisitZip(QueryNodeInfo first,
                                        QueryNodeInfo second,
                                        LambdaExpression resultSelector,
                                        MethodCallExpression queryExpr)
        {
            DLinqQueryNode child1 = this.Visit(first);
            DLinqQueryNode child2 = this.Visit(second);

            if (child1.IsDynamic || child2.IsDynamic)
            {
                // Well, let us for now do it on a single machine
                child1 = new DLinqMergeNode(true, queryExpr, child1);
                child2 = new DLinqMergeNode(true, queryExpr, child2);

                // Apply node for (x, y) => Zip(x, y, resultSelector)
                Type paramType1 = typeof(IEnumerable<>).MakeGenericType(child1.OutputTypes[0]);
                ParameterExpression param1 = Expression.Parameter(paramType1, "s1");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child2.OutputTypes[0]);
                ParameterExpression param2 = Expression.Parameter(paramType2, "s2");
                MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("Zip");
                minfo = minfo.MakeGenericMethod(child1.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param1, param2, resultSelector);
                Type funcType = typeof(Func<,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression procFunc = Expression.Lambda(funcType, body, param1, param2);
                return new DLinqApplyNode(procFunc, queryExpr, child1, child2);
            }
            else
            {
                int parCount1 = child1.OutputPartition.Count;
                int parCount2 = child2.OutputPartition.Count;
                
                // Count nodes
                DLinqQueryNode countNode1 = new DLinqBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                        true, false, queryExpr, child1);
                DLinqQueryNode countNode2 = new DLinqBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                        true, false, queryExpr, child2);
                countNode1 = new DLinqMergeNode(true, queryExpr, countNode1);
                countNode2 = new DLinqMergeNode(true, queryExpr, countNode2);

                // Apply node for (x, y) => ZipCount(x, y)
                Type paramType1 = typeof(IEnumerable<>).MakeGenericType(typeof(long));
                ParameterExpression param1 = Expression.Parameter(paramType1, "x");
                ParameterExpression param2 = Expression.Parameter(paramType1, "y");                
                MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("ZipCount");
                Expression body = Expression.Call(minfo, param1, param2);
                Type funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression zipCount = Expression.Lambda(funcType, body, param1, param2);
                DLinqQueryNode indexedCountNode = new DLinqApplyNode(zipCount, queryExpr, countNode1, countNode2);

                // HashPartition(x => x.index, parCount2)
                ParameterExpression param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                Expression keySelectBody = Expression.Property(param, "Index");                
                funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
                LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
                DLinqQueryNode distCountNode = new DLinqHashPartitionNode(keySelectExpr,
                                                                          null,
                                                                          parCount2,
                                                                          queryExpr,
                                                                          indexedCountNode);

                // Apply node for (x, y) => AssignPartitionIndex(x, y)
                param1 = Expression.Parameter(body.Type, "x");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child2.OutputTypes[0]);
                param2 = Expression.Parameter(paramType2, "y");
                minfo = typeof(DryadLinqHelper).GetMethod("AssignPartitionIndex");
                minfo = minfo.MakeGenericMethod(child2.OutputTypes[0]);
                body = Expression.Call(minfo, param1, param2);
                funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression assignIndex = Expression.Lambda(funcType, body, param1, param2);
                DLinqQueryNode addIndexNode = new DLinqApplyNode(assignIndex, queryExpr, distCountNode, child2);

                // HashPartition(x => x.index, x => x.value, parCount1)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                body = Expression.Property(param, "Index");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                keySelectExpr = Expression.Lambda(funcType, body, param);
                body = Expression.Property(param, "Value");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
                DLinqQueryNode newChild2 = new DLinqHashPartitionNode(keySelectExpr,
                                                                      resultSelectExpr,
                                                                      null,
                                                                      parCount1,
                                                                      false,
                                                                      queryExpr,
                                                                      addIndexNode);
                newChild2 = new DLinqMergeNode(true, queryExpr, newChild2);

                // Finally the zip node
                return new DLinqZipNode(resultSelector, queryExpr, child1, newChild2);
            }
        }
        
        // Basic plan: (reverse all partitions) then (reverse data in each partition)
        // The main complication is to perform the first step.
        // Approach: 
        //   - tee the input.
        //   - have a dummy apply node that produces the singleton {0} at each partition
        //   - merge to get a seq {0,0,..} whose length = nPartition.
        //   - convert that seq to { (0,n), (1,n), ...}
        //   - hash-partition to send one item to each of the n workers.
        //   - use binary-apply to attach targetIndex to each source item
        //              Apply( seq1 = indexCountPair, seq2 = original data) => ({tgt, item0}, {tgt, item1}, .. )
        //   - hash-partition to move items to target partition.
        //   - use local LINQ reverse to do the local data reversal.
        private DLinqQueryNode VisitReverse(QueryNodeInfo source, Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("Reverse is only supported for static partition count");
            }

            child.IsForked = true;

            // Apply node for s => ValueZero(s)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, "s");
            MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("ValueZero");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode valueZeroNode = new DLinqApplyNode(procFunc, queryExpr, child);

            // Apply node for s => ReverseIndex(s)
            paramType = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            param = Expression.Parameter(paramType, "s");
            minfo = typeof(DryadLinqHelper).GetMethod("MakeIndexCountPairs");
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode mergeZeroNode = new DLinqMergeNode(true, queryExpr, valueZeroNode); 
            DLinqQueryNode indexCountNode = new DLinqApplyNode(procFunc, queryExpr, mergeZeroNode);
            
            // HashPartition to distribute the indexCounts -- one to each partition.
            // each partition will receive (myPartitionID, pcount).
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            Expression keySelectBody = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
            DLinqQueryNode hdistNode = new DLinqHashPartitionNode(keySelectExpr,
                                                                  null,
                                                                  pcount,
                                                                  queryExpr,
                                                                  indexCountNode);

            // Apply node for (x, y) => AddIndexForReverse(x, y)
            ParameterExpression param1 = Expression.Parameter(body.Type, "x");
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param2 = Expression.Parameter(paramType2, "y");
            minfo = typeof(DryadLinqHelper).GetMethod("AddIndexForReverse");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param1, param2);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
            LambdaExpression addIndexFunc = Expression.Lambda(funcType, body, param1, param2);
            DLinqQueryNode addIndexNode = new DLinqApplyNode(addIndexFunc, queryExpr, hdistNode, child);

            // HashPartition(x => x.index, x => x.value, pcount)
            // Moves all data to correct target partition.  (each worker will direct all its items to one target partition)
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            body = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            keySelectExpr = Expression.Lambda(funcType, body, param);
            body = Expression.Property(param, "Value");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
            DLinqQueryNode reversePartitionNode = new DLinqHashPartitionNode(
                                                            keySelectExpr, resultSelectExpr, null,
                                                            pcount, false, queryExpr, addIndexNode);

            // Reverse node
            paramType = typeof(IEnumerable<>).MakeGenericType(reversePartitionNode.OutputTypes[0]);
            param = Expression.Parameter(paramType, "x");
            minfo = typeof(DryadLinqVertex).GetMethod("Reverse");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode resNode = new DLinqMergeNode(true, queryExpr, reversePartitionNode);
            resNode = new DLinqApplyNode(procFunc, queryExpr, resNode);

            return resNode;
        }

        private DLinqQueryNode VisitSequenceEqual(QueryNodeInfo source1,
                                                  QueryNodeInfo source2,
                                                  Expression comparerExpr,
                                                  Expression queryExpr)
        {
            DLinqQueryNode child1 = this.Visit(source1);
            DLinqQueryNode child2 = this.Visit(source2);

            Type elemType = child1.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(elemType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable,
                                                String.Format(SR.ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable, elemType),
                                                queryExpr);
            }

            // Well, let us do it on a single machine for now
            child1 = new DLinqMergeNode(true, queryExpr, child1);
            child2 = new DLinqMergeNode(true, queryExpr, child2);

            // Apply node for (x, y) => SequenceEqual(x, y, c)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(elemType);
            ParameterExpression param1 = Expression.Parameter(paramType, "s1");
            ParameterExpression param2 = Expression.Parameter(paramType, "s2");
            MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("SequenceEqual");
            minfo = minfo.MakeGenericMethod(elemType);
            if (comparerExpr == null)
            {
                comparerExpr = Expression.Constant(null, typeof(IEqualityComparer<>).MakeGenericType(elemType));
            }
            Expression body = Expression.Call(minfo, param1, param2, comparerExpr);
            Type funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DLinqApplyNode(procFunc, queryExpr, child1, child2);
        }

        private DLinqQueryNode VisitHashPartition(QueryNodeInfo source,
                                                  LambdaExpression keySelectExpr,
                                                  LambdaExpression resultSelectExpr,
                                                  Expression comparerExpr,
                                                  Expression countExpr,
                                                  Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                                queryExpr);
            }

            bool isDynamic = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicHashPartitionLevel) != 0;
            int nOutputPartitions;
            if (countExpr != null)
            {
                ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
                nOutputPartitions = evaluator.Eval(countExpr);
                isDynamic = false;
            }
            else
            {
                // Note: For MayRTM, isDynamic will never be true.
                nOutputPartitions = (isDynamic) ? 1 : child.OutputPartition.Count;
            }

            DLinqQueryNode resNode = new DLinqHashPartitionNode(
                                              keySelectExpr, resultSelectExpr, comparerExpr, nOutputPartitions,
                                              isDynamic, queryExpr, child);
            resNode = new DLinqMergeNode(false, queryExpr, resNode);
            return resNode;
        }

        private DLinqQueryNode VisitRangePartition(QueryNodeInfo source,
                                                   LambdaExpression keySelectExpr,
                                                   Expression keysExpr,
                                                   Expression comparerExpr,
                                                   Expression isDescendingExpr,
                                                   Expression partitionCountExpr,
                                                   Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];

            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                queryExpr);
            }

            DLinqQueryNode resNode;
            if (keysExpr == null)
            {
                // case: no keys are provided -- create range partitioner with auto-separator-selection
                bool dynamicOptEnabled = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicRangePartitionLevel) != 0;
                bool useDynamic = (dynamicOptEnabled || child.IsDynamic);

                // NOTE: for MayRTM, useDynamic should always be false
                resNode = this.CreateRangePartition(useDynamic, keySelectExpr, null, comparerExpr, isDescendingExpr,
                                                    queryExpr, partitionCountExpr, child);
            }
            else
            {
                // case: keys are local enum (eg an array) -- create range partitioner with keys input via Object-store.
                resNode = new DLinqRangePartitionNode(keySelectExpr, null, keysExpr, comparerExpr,
                                                      isDescendingExpr, null, queryExpr, child);
                resNode = new DLinqMergeNode(false, queryExpr, resNode);
            }
            return resNode;
        }

        private DLinqQueryNode VisitApply(QueryNodeInfo source1,
                                          QueryNodeInfo source2,
                                          LambdaExpression procLambda,
                                          bool perPartition,
                                          bool isFirstOnly,
                                          Expression queryExpr)
        {
            DLinqQueryNode child1 = this.Visit(source1);

            DLinqQueryNode applyNode;
            if (source2 == null)
            {
                // Unary-apply case:
                if (perPartition)
                {
                    // homomorphic
                    applyNode = this.PromoteConcat(source1, child1, x => new DLinqApplyNode(procLambda, queryExpr, x));
                }
                else
                {
                    // non-homomorphic
                    if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                    {
                        child1 = new DLinqMergeNode(true, queryExpr, child1);
                    }
                    applyNode = new DLinqApplyNode(procLambda, queryExpr, child1);
                }
            }
            else
            {
                // Binary-apply case:
                DLinqQueryNode child2 = this.Visit(source2);
                if (perPartition && isFirstOnly)
                {
                    // The function is left homomorphic:
                    if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                    {
                        // The normal cases..
                        if (IsMergeNodeNeeded(child2))
                        {
                            if (child1.IsDynamic)
                            {
                                child2 = new DLinqMergeNode(true, queryExpr, child2);
                                child2.IsForked = true;
                            }
                            else
                            {
                                // Rather than do full merge and broadcast, which has lots of data-movement  
                                //   1. Tee output2 with output cross-product
                                //   2. Do a merge-stage which will have input1.nPartition nodes each performing a merge.
                                // This acheives a distribution of the entire input2 to the Apply nodes with least data-movement.
                                child2 = new DLinqTeeNode(child2.OutputTypes[0], true, queryExpr, child2);
                                child2.ConOpType = ConnectionOpType.CrossProduct;
                                child2 = new DLinqMergeNode(child1.OutputPartition.Count, queryExpr, child2);
                            }
                        }
                        else
                        {
                            // the right-data is alread a single partition, so just tee it.
                            // this will provide a copy to each of the apply nodes.
                            child2 = new DLinqTeeNode(child2.OutputTypes[0], true, queryExpr, child2);
                        }
                    }
                    else
                    {
                        // a full merge of the right-data may be necessary.
                        if (child2.IsDynamic || child2.OutputPartition.Count > 1)
                        {
                            child2 = new DLinqMergeNode(true, queryExpr, child2);
                            if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                            {
                                child2.IsForked = true;
                            }
                        }
                    }
                    applyNode = new DLinqApplyNode(procLambda, queryExpr, child1, child2);
                }
                else if (perPartition && !isFirstOnly && !child1.IsDynamic && !child2.IsDynamic)
                {
                    // Full homomorphic
                    // No merging occurs.
                    // NOTE: We generally expect that both the left and right datasets have matching partitionCount.
                    //       however, we don't test for it yet as users might know what they are doing, and it makes
                    //       LocalDebug inconsistent as LocalDebug doesn't throw in that situation.
                    applyNode = new DLinqApplyNode(procLambda, queryExpr, child1, child2);
                }
                else
                {
                    // Non-homomorphic
                    // Full merges of both data sets is necessary.
                    if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                    {
                        child1 = new DLinqMergeNode(true, queryExpr, child1);
                    }
                    if (child2.IsDynamic || child2.OutputPartition.Count > 1)
                    {
                        child2 = new DLinqMergeNode(true, queryExpr, child2);
                    }
                    applyNode = new DLinqApplyNode(procLambda, queryExpr, child1, child2);
                }
            }
            return applyNode;
        }

        private DLinqQueryNode VisitMultiApply(QueryNodeInfo source,
                                               LambdaExpression procLambda,
                                               bool perPartition,
                                               bool isFirstOnly,
                                               MethodCallExpression queryExpr)
        {
            DLinqQueryNode[] childs = new DLinqQueryNode[source.Children.Count];
            for (int i = 0; i < source.Children.Count; ++i)
            {
                childs[i] = this.Visit(source.Children[i].Child);
            }

            bool isDynamic = childs.Any(x => x.IsDynamic);
            if (perPartition && !isDynamic)
            {
                // Homomorphic case.
                if (isFirstOnly)
                {
                    for (int i = 1; i < childs.Length; ++i)
                    {
                        childs[i] = new DLinqTeeNode(childs[i].OutputTypes[0], true, queryExpr, childs[i]);
                        childs[i].ConOpType = ConnectionOpType.CrossProduct;
                        childs[i] = new DLinqMergeNode(childs[0].OutputPartition.Count, queryExpr, childs[i]);
                    }
                }
                else
                {
                    int count = childs[0].OutputPartition.Count;
                    for (int i = 1; i < childs.Length; ++i)
                    {
                        if (childs[i].OutputPartition.Count != count)
                        {
                            throw DryadLinqException.Create(DryadLinqErrorCode.HomomorphicApplyNeedsSamePartitionCount,
                                                            SR.HomomorphicApplyNeedsSamePartitionCount,
                                                            queryExpr);
                        }
                    }
                }
            }
            else
            {
                // Non-homomorphic case.
                for (int i = 0; i < childs.Length; ++i)
                {
                    if (childs[i].IsDynamic || childs[i].OutputPartition.Count > 1)
                    {
                        childs[i] = new DLinqMergeNode(true, queryExpr, childs[i]);
                    }
                }
            }
            DLinqQueryNode applyNode = new DLinqApplyNode(procLambda, true, queryExpr, childs);
            return applyNode;
        }

        private DLinqQueryNode VisitFork(QueryNodeInfo source,
                                         LambdaExpression forkLambda,
                                         Expression keysExpr,
                                         Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            return new DLinqForkNode(forkLambda, keysExpr, queryExpr, child);
        }

        private DLinqQueryNode VisitForkChoose(QueryNodeInfo source,
                                               Expression indexExpr,
                                               Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
            int index = evaluator.Eval(indexExpr);
            return ((DLinqForkNode)child).Parents[index];
        }

        private DLinqQueryNode VisitAssumeHashPartition(QueryNodeInfo source,
                                                        LambdaExpression keySelectExpr,
                                                        Expression keysExpr,
                                                        Expression comparerExpr,
                                                        Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                                String.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType.FullName),
                                                queryExpr);
            }

            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }
            DataSetInfo outputInfo = new DataSetInfo(child.OutputDataSetInfo);
            outputInfo.partitionInfo = PartitionInfo.CreateHash(keySelectExpr,
                                                                child.OutputPartition.Count,
                                                                comparer,
                                                                keyType);
            child.OutputDataSetInfo = outputInfo;
            return child;
        }

        private DLinqQueryNode VisitAssumeRangePartition(QueryNodeInfo source,
                                                         LambdaExpression keySelectExpr,
                                                         Expression keysExpr,
                                                         Expression comparerExpr,
                                                         Expression isDescendingExpr,
                                                         Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                String.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                queryExpr);
            }
            
            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            object keys = null;
            if (keysExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                keys = evaluator.Eval(keysExpr);
            }

            //count the number of keys provided.
            if (keys != null)
            {
                int nSeparators = 0;
                var ie = ((IEnumerable)keys).GetEnumerator();
                while (ie.MoveNext())
                {
                    nSeparators++;
                }

                if (!child.IsDynamic && nSeparators != child.PartitionCount - 1)
                {
                    throw DryadLinqException.Create(
                                 DryadLinqErrorCode.BadSeparatorCount,
                                 String.Format(SR.BadSeparatorCount, nSeparators, child.PartitionCount - 1),
                                 queryExpr);
                }
            }


            bool? isDescending = null;
            if (isDescendingExpr != null)
            {
                ExpressionSimplifier<bool> evaluator = new ExpressionSimplifier<bool>();
                isDescending = evaluator.Eval(isDescendingExpr);
            }

            DataSetInfo outputInfo = new DataSetInfo(child.OutputDataSetInfo);
            outputInfo.partitionInfo = PartitionInfo.CreateRange(keySelectExpr,
                                                                 keys,
                                                                 comparer,
                                                                 isDescending,
                                                                 child.OutputPartition.Count,
                                                                 keyType);
            child.OutputDataSetInfo = outputInfo;
            return child;
        }

        private DLinqQueryNode VisitAssumeOrderBy(QueryNodeInfo source,
                                                  LambdaExpression keySelectExpr,
                                                  Expression comparerExpr,
                                                  Expression isDescendingExpr,
                                                  Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                                String.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                                queryExpr);
            }

            object comparer = null;
            if (comparerExpr != null)
            {
                ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
                comparer = evaluator.Eval(comparerExpr);
            }

            ExpressionSimplifier<bool> bevaluator = new ExpressionSimplifier<bool>();
            bool isDescending = bevaluator.Eval(isDescendingExpr);

            DataSetInfo outputInfo = new DataSetInfo(child.OutputDataSetInfo);
            outputInfo.orderByInfo = OrderByInfo.Create(keySelectExpr, comparer, isDescending, keyType);
            child.OutputDataSetInfo = outputInfo;
            return child;
        }

        private DLinqQueryNode VisitSlidingWindow(QueryNodeInfo source,
                                                  LambdaExpression procLambda,
                                                  Expression windowSizeExpr,
                                                  Expression queryExpr)
        {
            // var windows = source.Apply(s => DryadLinqHelper.Last(s, windowSize));
            // var slided = windows.Apply(s => DryadLinqHelper.Slide(s)).HashPartition(x => x.Index);
            // slided.Apply(source, (x, y) => DryadLinqHelper.ProcessWindows(x, y, procFunc, windowSize));
            DLinqQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("SlidingWindow is only supported for static partition count");
            }

            ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
            Expression windowSize = Expression.Constant(evaluator.Eval(windowSizeExpr), typeof(int));
            
            child.IsForked = true;

            // Apply node for s => Last(s, windowSize)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, DryadLinqCodeGen.MakeUniqueName("s"));
            MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("Last");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param, windowSize);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode lastNode = new DLinqApplyNode(procFunc, queryExpr, child);
            lastNode = new DLinqMergeNode(true, queryExpr, lastNode);

            // Apply node for s => Slide(s)
            param = Expression.Parameter(body.Type, DryadLinqCodeGen.MakeUniqueName("s"));
            minfo = typeof(DryadLinqHelper).GetMethod("Slide");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode slideNode = new DLinqApplyNode(procFunc, queryExpr, lastNode);

            // Hash partition to distribute from partition i to i+1
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            Expression keySelectBody = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
            DLinqQueryNode hdistNode 
                = new DLinqHashPartitionNode(keySelectExpr, null, pcount, queryExpr, slideNode);

            // Apply node for (x, y) => ProcessWindows(x, y, proclambda, windowSize)
            Type paramType1 = typeof(IEnumerable<>).MakeGenericType(body.Type);
            ParameterExpression param1 = Expression.Parameter(paramType1, DryadLinqCodeGen.MakeUniqueName("x"));
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param2 = Expression.Parameter(paramType2, DryadLinqCodeGen.MakeUniqueName("y"));
            minfo = typeof(DryadLinqHelper).GetMethod("ProcessWindows");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0], procLambda.Body.Type);
            body = Expression.Call(minfo, param1, param2, procLambda, windowSize);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DLinqApplyNode(procFunc, queryExpr, hdistNode, child);
        }

        private DLinqQueryNode
            VisitApplyWithPartitionIndex(QueryNodeInfo source,
                                         LambdaExpression procLambda,
                                         Expression queryExpr)
        {
            // The computation looks like this:
            //     var indices = source.Apply(s => ValueZero(s)).Apply(s => AssignIndex(s))
            //                         .HashPartition(x => x)
            //     indices.Apply(source, (x, y) => ApplyWithPartitionIndex(x, y, procFunc));
            DLinqQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("ApplyWithPartitionIndex is only supported for static partition count");
            }

            child.IsForked = true;

            // Apply node for s => ValueZero(s)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, "s");
            MethodInfo minfo = typeof(DryadLinqHelper).GetMethod("ValueZero");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode valueZeroNode = new DLinqApplyNode(procFunc, queryExpr, child);
            valueZeroNode = new DLinqMergeNode(true, queryExpr, valueZeroNode); 

            // Apply node for s => AssignIndex(s)
            paramType = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            param = Expression.Parameter(paramType, "s");
            minfo = typeof(DryadLinqHelper).GetMethod("AssignIndex");
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DLinqQueryNode assignIndexNode = new DLinqApplyNode(procFunc, queryExpr, valueZeroNode);

            // HashPartition to distribute the indices -- one to each partition.
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type, "x");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, param.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, param, param);
            DLinqQueryNode hdistNode 
                = new DLinqHashPartitionNode(keySelectExpr, null, pcount, queryExpr, assignIndexNode);

            // Apply node for (x, y) => ApplyWithPartitionIndex(x, y, procLambda));
            Type paramType1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param1 = Expression.Parameter(paramType1, DryadLinqCodeGen.MakeUniqueName("x"));
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            ParameterExpression param2 = Expression.Parameter(paramType2, DryadLinqCodeGen.MakeUniqueName("y"));
            minfo = typeof(DryadLinqHelper).GetMethod("ProcessWithIndex");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0], procLambda.Body.Type.GetGenericArguments()[0]);
            body = Expression.Call(minfo, param1, param2, procLambda);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DLinqApplyNode(procFunc, queryExpr, child, hdistNode);
        }

        private DLinqQueryNode VisitDoWhile(QueryNodeInfo source,
                                            QueryNodeInfo body,
                                            QueryNodeInfo cond,
                                            QueryNodeInfo bodySource,
                                            QueryNodeInfo condSource1,
                                            QueryNodeInfo condSource2,
                                            Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            DLinqQueryNode bodyNode = this.Visit(body);
            DLinqQueryNode condNode = this.Visit(cond);
            DLinqQueryNode bodySourceNode = this.Visit(bodySource);
            DLinqQueryNode condSourceNode1 = this.Visit(condSource1);
            DLinqQueryNode condSourceNode2 = this.Visit(condSource2);

            return new DLinqDoWhileNode(bodyNode, condNode,
                                        bodySourceNode,
                                        condSourceNode1, condSourceNode2,
                                        queryExpr, child);
        }

        private DLinqQueryNode VisitDummy(Expression queryExpr)
        {
            MethodCallExpression mcExpr = queryExpr as MethodCallExpression;
            Expression sourceExpr = mcExpr.Arguments[0];
            QueryNodeInfo source = this.m_exprNodeInfoMap[sourceExpr];
            DLinqQueryNode child = this.Visit(source);

            return new DLinqDummyNode(queryExpr, child);
        }

        private DLinqQueryNode VisitFirst(QueryNodeInfo source,
                                          LambdaExpression lambda,
                                          AggregateOpType aggType,
                                          bool isQuery,
                                          Expression queryExpr)
        {
            DLinqQueryNode child = this.Visit(source);
            DLinqQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DLinqBasicAggregateNode(lambda, aggType, true, isQuery, queryExpr, x));
            return new DLinqBasicAggregateNode(null, aggType, false, isQuery, queryExpr, resNode);
        }

        private DLinqQueryNode VisitThenBy(QueryNodeInfo source,
                                           LambdaExpression keySelectExpr,
                                           bool isDescending,
                                           Expression queryExpr)
        {
            // YY: This makes it hard to maintain OrderByInfo.
            throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                            String.Format(SR.OperatorNotSupported, "ThenBy"),
                                            queryExpr);
        }

        private DLinqQueryNode VisitDefaultIfEmpty(QueryNodeInfo source,
                                                   Expression defaultValueExpr,
                                                   MethodCallExpression queryExpr)
        {
            // YY: Not very useful. We could add it later.
            throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, "DefaultIfEmpty"),
                                          queryExpr);
        }

        private DLinqQueryNode VisitElementAt(string opName,
                                              QueryNodeInfo source,
                                              Expression indexExpr,
                                              Expression queryExpr)
        {
            // YY: Not very useful. We could add it later.
            throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, opName),
                                          queryExpr);
        }
                                              
        private DLinqQueryNode VisitOfType(QueryNodeInfo source,
                                           Type ofType,
                                           Expression queryExpr)
        {
            // YY: Not very useful.
            throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, "OfType"),
                                          queryExpr);
        }

        private DLinqQueryNode VisitQueryOperatorCall(QueryNodeInfo nodeInfo)
        {
            DLinqQueryNode resNode = nodeInfo.QueryNode;
            if (resNode != null) return resNode;

            MethodCallExpression expression = (MethodCallExpression)nodeInfo.QueryExpression;
            string methodName = expression.Method.Name;
            
            #region LINQMETHODS
            switch (methodName)
            {
                case "Aggregate":
                case "AggregateAsQuery":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression funcLambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitAggregate(nodeInfo.Children[0].Child,
                                                          null,
                                                          funcLambda,
                                                          null,
                                                          expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression funcLambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitAggregate(nodeInfo.Children[0].Child,
                                                          expression.Arguments[1],
                                                          funcLambda,
                                                          null,
                                                          expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression funcLambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression resultLambda = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2 &&
                            resultLambda != null && resultLambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAggregate(nodeInfo.Children[0].Child,
                                                          expression.Arguments[1],
                                                          funcLambda,
                                                          resultLambda,
                                                          expression);
                        }
                    }
                    break;
                }
                case "Select":
                case "LongSelect":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitSelect(nodeInfo.Children[0].Child,
                                                       QueryNodeType.Select,
                                                       lambda,
                                                       null,
                                                       expression);
                        }
                    }
                    break;
                }
                case "SelectMany":
                case "LongSelectMany":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count <= 2)
                        {
                            resNode = this.VisitSelect(nodeInfo.Children[0].Child,
                                                       QueryNodeType.SelectMany,
                                                       lambda,
                                                       null,
                                                       expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda1 = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda1 != null && (lambda1.Parameters.Count == 1 || lambda1.Parameters.Count == 2) &&
                            lambda2 != null && lambda2.Parameters.Count == 2)
                        {
                            resNode = this.VisitSelect(nodeInfo.Children[0].Child,
                                                       QueryNodeType.SelectMany,
                                                       lambda1,
                                                       lambda2,
                                                       expression);
                        }
                    }
                    break;
                }
                case "Join":
                case "GroupJoin":
                {
                    QueryNodeType nodeType = (methodName == "Join") ? QueryNodeType.Join : QueryNodeType.GroupJoin;
                    if (expression.Arguments.Count == 5)
                    {
                        LambdaExpression lambda2 = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                        LambdaExpression lambda4 = DryadLinqExpression.GetLambda(expression.Arguments[4]);
                        if (lambda2 != null && lambda2.Parameters.Count == 1 &&
                            lambda3 != null && lambda3.Parameters.Count == 1 &&
                            lambda4 != null && lambda4.Parameters.Count == 2)
                        {
                            resNode = this.VisitJoin(nodeInfo.Children[0].Child,
                                                     nodeInfo.Children[1].Child,
                                                     nodeType,
                                                     lambda2,
                                                     lambda3,
                                                     lambda4,
                                                     null,
                                                     expression);
                        }
                    }
                    else if (expression.Arguments.Count == 6)
                    {
                        LambdaExpression lambda2 = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                        LambdaExpression lambda4 = DryadLinqExpression.GetLambda(expression.Arguments[4]);
                        if (lambda2 != null && lambda2.Parameters.Count == 1 &&
                            lambda3 != null && lambda3.Parameters.Count == 1 &&
                            lambda4 != null && lambda4.Parameters.Count == 2)
                        {
                            resNode = this.VisitJoin(nodeInfo.Children[0].Child,
                                                     nodeInfo.Children[1].Child,
                                                     nodeType,
                                                     lambda2,
                                                     lambda3,
                                                     lambda4,
                                                     expression.Arguments[5],
                                                     expression);
                        }
                    }
                    break;
                }
                case "OfType":
                {
                    if (expression.Arguments.Count == 1)
                    {
                        Type ofType = expression.Method.GetGenericArguments()[0];
                        resNode = this.VisitOfType(nodeInfo.Children[0].Child, ofType, expression);
                    }
                    break;
                }
                case "Where":
                case "LongWhere":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitWhere(nodeInfo.Children[0].Child,
                                                      lambda,
                                                      expression);
                        }
                    }
                    break;
                }
                case "First":
                case "FirstOrDefault":
                case "FirstAsQuery":
                {
                    AggregateOpType aggType = (methodName == "FirstOrDefault") ? AggregateOpType.FirstOrDefault : AggregateOpType.First;
                    bool isQuery = (methodName == "FirstAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitFirst(nodeInfo.Children[0].Child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.Children[0].Child,
                                                      lambda,
                                                      aggType,
                                                      isQuery,
                                                      expression);
                        }
                    }
                    break;
                }
                case "Single":
                case "SingleOrDefault":
                case "SingleAsQuery":
                {
                    AggregateOpType aggType = (methodName == "SingleOrDefault") ? AggregateOpType.SingleOrDefault : AggregateOpType.Single;
                    bool isQuery = (methodName == "SingleAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitFirst(nodeInfo.Children[0].Child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.Children[0].Child,
                                                      lambda,
                                                      aggType,
                                                      isQuery,
                                                      expression);
                        }
                    }
                    break;
                }
                case "Last":
                case "LastOrDefault":
                case "LastAsQuery":
                {
                    AggregateOpType aggType = (methodName == "LastOrDefault") ? AggregateOpType.LastOrDefault : AggregateOpType.Last;
                    bool isQuery = (methodName == "LastAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitFirst(nodeInfo.Children[0].Child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.Children[0].Child,
                                                      lambda,
                                                      aggType,
                                                      isQuery,
                                                      expression);
                        }
                    }
                    break;
                }
                case "Distinct":
                {
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitDistinct(nodeInfo.Children[0].Child, null, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitDistinct(nodeInfo.Children[0].Child,
                                                     expression.Arguments[1],
                                                     expression);
                    }
                    break;
                }
                case "DefaultIfEmpty":
                {
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitDefaultIfEmpty(nodeInfo.Children[0].Child,
                                                           null,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitDefaultIfEmpty(nodeInfo.Children[0].Child,
                                                           expression.Arguments[1],
                                                           expression);
                    }
                    break;
                }
                case "Concat":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitConcat(nodeInfo, expression);
                    }
                    break;
                }
                case "Union":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Union,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Union,
                                                         expression.Arguments[2],
                                                         expression);
                    }
                    break;
                }
                case "Intersect":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Intersect,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Intersect,
                                                         expression.Arguments[2],
                                                         expression);
                    }
                    break;
                }
                case "Except":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Except,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.Children[0].Child,
                                                         nodeInfo.Children[1].Child,
                                                         QueryNodeType.Except,
                                                         expression.Arguments[2],
                                                         expression);
                    }
                    break;
                }
            case "Any":
            case "AnyAsQuery":
                {
                    bool isQuery = (methodName == "AnyAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        Type type = expression.Method.GetGenericArguments()[0];
                        ParameterExpression param = Expression.Parameter(type, "x");
                        Type delegateType = typeof(Func<,>).MakeGenericType(type, typeof(bool));
                        Expression body = Expression.Constant(true);
                        LambdaExpression lambda = Expression.Lambda(delegateType, body, param);
                        resNode = this.VisitQuantifier(nodeInfo.Children[0].Child,
                                                       lambda,
                                                       AggregateOpType.Any,
                                                       isQuery,
                                                       expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitQuantifier(nodeInfo.Children[0].Child,
                                                           lambda,
                                                           AggregateOpType.Any,
                                                           isQuery,
                                                           expression);
                        }
                    }
                    break;
                }
                case "All":
                case "AllAsQuery":
                {
                    bool isQuery = (methodName == "AllAsQuery");
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitQuantifier(nodeInfo.Children[0].Child,
                                                           lambda,
                                                           AggregateOpType.All,
                                                           isQuery,
                                                           expression);
                        }
                    }
                    break;
                }
                case "Count":
                case "CountAsQuery":
                {
                    bool isQuery = (methodName == "CountAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.Count,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.Count,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "LongCount":
                case "LongCountAsQuery":
                {
                    bool isQuery = (methodName == "LongCountAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.LongCount,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.LongCount,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "Sum":
                case "SumAsQuery":
                {
                    bool isQuery = (methodName == "SumAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.Sum,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.Sum,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "Min":
                case "MinAsQuery":
                {
                    bool isQuery = (methodName == "MinAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.Min,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.Min,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "Max":
                case "MaxAsQuery":
                {
                    bool isQuery = (methodName == "MaxAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.Max,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.Max,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "Average":
                case "AverageAsQuery":
                {
                    bool isQuery = (methodName == "AverageAsQuery");
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                           null,
                                                           AggregateOpType.Average,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.Children[0].Child,
                                                               lambda,
                                                               AggregateOpType.Average,
                                                               isQuery,
                                                               expression);
                        }
                    }
                    break;
                }
                case "GroupBy":
                {
                    // groupby can take 2, 3, 4, or 5 arguments. 
                    if (expression.Arguments.Count == 2)
                    {
                        //Supplied arguments are as follows:(source, key selector)
                        LambdaExpression keySelExpr = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (keySelExpr != null && keySelExpr.Parameters.Count == 1)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        null,
                                                        null,
                                                        null,
                                                        expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        //Supplied arguments are as follows:(source, key selector, element selector/result selector/comparer)
                        LambdaExpression keySelExpr = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        if (keySelExpr != null && lambda2 == null)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        null,
                                                        null,
                                                        expression.Arguments[2],
                                                        expression);
                        }
                        else if (keySelExpr != null && keySelExpr.Parameters.Count == 1 && lambda2 != null)
                        {
                            LambdaExpression elemSelExpr = null;
                            LambdaExpression resSelExpr = null;
                            if (lambda2.Parameters.Count == 1)
                            {
                                elemSelExpr = lambda2;
                            }
                            else if (lambda2.Parameters.Count == 2)
                            {
                                resSelExpr = lambda2;
                            }
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        elemSelExpr,
                                                        resSelExpr,
                                                        null,
                                                        expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        //Argument-0 is source and Argument-1 is key selector expression
                        LambdaExpression keySelExpr = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                        if (keySelExpr != null && keySelExpr.Parameters.Count == 1 && lambda3 == null)
                        {
                            //Argument-2 can be either result selector, element selector and argument-3 is comparer
                            LambdaExpression elemSelExpr = null;
                            LambdaExpression resSelExpr = null;
                            if (lambda2.Parameters.Count == 1)
                            {
                                elemSelExpr = lambda2;
                            }
                            else if (lambda2.Parameters.Count == 2)
                            {
                                resSelExpr = lambda2;
                            }
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        elemSelExpr,
                                                        resSelExpr,
                                                        expression.Arguments[3],
                                                        expression);
                        }
                        else if (keySelExpr != null && keySelExpr.Parameters.Count == 1 &&
                                 lambda2 != null && lambda2.Parameters.Count == 1 &&
                                 lambda3 != null && lambda3.Parameters.Count == 2)
                        {
                            //Argument-2 is element selector and argument-3 is result selector
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        lambda2,
                                                        lambda3,
                                                        null,
                                                        expression);
                        }
                    }
                    else if (expression.Arguments.Count == 5)
                    {
                        //Supplied arguments are as follows:(source, key selector, element selector, result selector, comparer)
                        LambdaExpression keySelExpr = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression elemSelExpr = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression resSelExpr = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                        if (keySelExpr != null && keySelExpr.Parameters.Count == 1 &&
                            elemSelExpr != null && elemSelExpr.Parameters.Count == 1 &&
                            resSelExpr != null && resSelExpr.Parameters.Count == 2)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.Children[0].Child,
                                                        keySelExpr,
                                                        elemSelExpr,
                                                        resSelExpr,
                                                        expression.Arguments[4],
                                                        expression);
                        }
                    }
                    break;
                }
                case "OrderBy":
                case "OrderByDescending":
                {
                    bool isDescending = (methodName == "OrderByDescending");
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitOrderBy(nodeInfo.Children[0].Child,
                                                        lambda,
                                                        null,
                                                        isDescending,
                                                        expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitOrderBy(nodeInfo.Children[0].Child,
                                                        lambda,
                                                        expression.Arguments[2],
                                                        isDescending,
                                                        expression);
                        }
                    }
                    break;
                }
                case "ThenBy":
                case "ThenByDescending":
                {
                    bool isDescending = (methodName == "ThenByDescending");
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitThenBy(nodeInfo.Children[0].Child,
                                                       lambda,
                                                       isDescending,
                                                       expression);
                        }
                    }
                    break;
                }
                case "ElementAt":
                case "ElementAtOrDefault":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitElementAt(methodName,
                                                      nodeInfo.Children[0].Child,
                                                      expression.Arguments[1],
                                                      expression);
                    }
                    break;
                }
                case "Take":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitPartitionOp(methodName,
                                                        nodeInfo.Children[0].Child,
                                                        QueryNodeType.Take,
                                                        expression.Arguments[1],
                                                        expression);
                    }
                    break;
                }
                case "TakeWhile":
                case "LongTakeWhile":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitPartitionOp("TakeWhile",
                                                            nodeInfo.Children[0].Child,
                                                            QueryNodeType.TakeWhile,
                                                            lambda,
                                                            expression);
                        }
                    }
                    break;
                }
                case "Skip":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitPartitionOp(methodName,
                                                        nodeInfo.Children[0].Child,
                                                        QueryNodeType.Skip,
                                                        expression.Arguments[1],
                                                        expression);
                    }
                    break;
                }
                case "SkipWhile":
                case "LongSkipWhile":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitPartitionOp(methodName,
                                                            nodeInfo.Children[0].Child,
                                                            QueryNodeType.SkipWhile,
                                                            lambda,
                                                            expression);
                        }
                    }
                    break;
                }
                case "Contains":
                case "ContainsAsQuery":
                {
                    bool isQuery = (methodName == "ContainsAsQuery");
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitContains(nodeInfo.Children[0].Child,
                                                     expression.Arguments[1],
                                                     null,
                                                     isQuery,
                                                     expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitContains(nodeInfo.Children[0].Child,
                                                     expression.Arguments[1],
                                                     expression.Arguments[2],
                                                     isQuery,
                                                     expression);
                    }
                    break;
                }
                case "Reverse":
                {
                    resNode = this.VisitReverse(nodeInfo.Children[0].Child, expression);
                    break;
                }
                case "SequenceEqual":
                case "SequenceEqualAsQuery":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitSequenceEqual(nodeInfo.Children[0].Child,
                                                          nodeInfo.Children[1].Child,
                                                          null,
                                                          expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSequenceEqual(nodeInfo.Children[0].Child,
                                                          nodeInfo.Children[1].Child,
                                                          expression.Arguments[2],
                                                          expression);
                    }
                    break;
                }
                case "Zip":
                {
                    if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda != null && lambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitZip(nodeInfo.Children[0].Child,
                                                    nodeInfo.Children[1].Child,
                                                    lambda,
                                                    expression);
                        }
                    }
                    break;
                }
                case "HashPartition":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                              lambda,
                                                              null,
                                                              null,
                                                              null,
                                                              expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(int))
                            {
                                resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                                  lambda,
                                                                  null,
                                                                  null,
                                                                  expression.Arguments[2],
                                                                  expression);
                            }
                            else
                            {
                                LambdaExpression resLambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                                if (resLambda == null)
                                {
                                    resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                                      lambda,
                                                                      null,
                                                                      expression.Arguments[2],
                                                                      null,
                                                                      expression);
                                }
                                else
                                {
                                    resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                                      lambda,
                                                                      resLambda,
                                                                      null,
                                                                      null,
                                                                      expression);
                                }
                            }
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[3].Type == typeof(int))
                            {
                                resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                                  lambda,
                                                                  null,
                                                                  expression.Arguments[2],
                                                                  expression.Arguments[3],
                                                                  expression);
                            }
                            else
                            {
                                LambdaExpression resLambda = DryadLinqExpression.GetLambda(expression.Arguments[3]);
                                resNode = this.VisitHashPartition(nodeInfo.Children[0].Child,
                                                                  lambda,
                                                                  resLambda,
                                                                  expression.Arguments[2],
                                                                  null,
                                                                  expression);
                            }
                        }
                    }
                    break;
                }
                case "RangePartition":
                {
                    //overloads:
                    //
                    //  2-param:
                    //    (source, keySelector)
                    //
                    //  3-param:
                    //    (source, keySelector, pcount) 
                    //    (source, keySelector, isDescending)
                    //    (source, keySelector, rangeSeparators)
                    //
                    //  4-param:
                    //    (source, keySelector, isDescending, pcount)
                    //    (source, keySelector, keyComparer, isDescending)
                    //    (source, keySelector, rangeSeparators, keyComparer)
                    //
                    //  5-param:
                    //    (source, keySelector, keyComparer, isDescending, pcount)
                    //    (source, keySelector, rangeSeparators, keyComparer, isDescending)
                    if (expression.Arguments.Count == 2)
                    {
                        // Case: (source, keySelector)  
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                           lambda,
                                                           null,
                                                           null,
                                                           null,
                                                           null,
                                                           expression);
                    }

                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(int))
                            {
                                // Case: (source, keySelector, pcount)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   null,
                                                                   null,
                                                                   null,
                                                                   expression.Arguments[2],
                                                                   expression);
                            }
                            if (expression.Arguments[2].Type == typeof(bool))
                            {
                                // Case: (source, keySelector, isDescending)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   null,
                                                                   null,
                                                                   expression.Arguments[2],
                                                                   null,
                                                                   expression);
                            }
                            else if (expression.Arguments[2].Type.IsArray)
                            {
                                // Case: RangePartition(keySelector, TKey[] keys)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   expression.Arguments[2],
                                                                   null,
                                                                   null,
                                                                   null,
                                                                   expression);
                            }
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(bool))
                            {
                                //case: (source, keySelector, isDescending, pcount) 
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   null,
                                                                   null,
                                                                   expression.Arguments[2],
                                                                   expression.Arguments[3],
                                                                   expression);
                            }
                            else if (expression.Arguments[3].Type == typeof(bool))
                            {
                                //case: (source, keySelector, keyComparer, isDescending)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   null,
                                                                   expression.Arguments[2],
                                                                   expression.Arguments[3],
                                                                   null,
                                                                   expression);
                            }
                            else if (expression.Arguments[2].Type.IsArray)
                            {
                                //case: (source, keySelector, rangeSeparators, keyComparer)
                                
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   expression.Arguments[2],
                                                                   expression.Arguments[3],
                                                                   null,
                                                                   null,
                                                                   expression);
                            }
                        }
                    }
                    else if (expression.Arguments.Count == 5)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[3].Type == typeof(bool))
                            {
                                // case: (source, keySelector, keyComparer, isDescending, pcount)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                  lambda,
                                                                  null,
                                                                  expression.Arguments[2],
                                                                  expression.Arguments[3],
                                                                  expression.Arguments[4],
                                                                  expression);
                            }
                            else if (expression.Arguments[4].Type == typeof(bool))
                            {
                                // case: (source, keySelector, rangeSeparators, keyComparer, isDescending)
                                resNode = this.VisitRangePartition(nodeInfo.Children[0].Child,
                                                                   lambda,
                                                                   expression.Arguments[2],
                                                                   expression.Arguments[3],
                                                                   expression.Arguments[4],
                                                                   null,
                                                                   expression);
                            }
                            
                        }
                    }
                    break;
                }
                case "Apply":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        // Apply with only one input source
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitApply(nodeInfo.Children[0].Child,
                                                      null,
                                                      lambda,
                                                      false,
                                                      false,
                                                      expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda != null)
                        {
                            if (nodeInfo.Children.Count == 2)
                            {
                                // Apply with two input sources
                                resNode = this.VisitApply(nodeInfo.Children[0].Child,
                                                          nodeInfo.Children[1].Child,
                                                          lambda,
                                                          false,
                                                          false,
                                                          expression);
                            }
                            else
                            {
                                // Apply with multiple sources
                                resNode = this.VisitMultiApply(nodeInfo, lambda, false, false, expression);
                            }
                        }
                    }
                    break;
                }
                case "ApplyPerPartition":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitApply(nodeInfo.Children[0].Child,
                                                      null,
                                                      lambda,
                                                      true,
                                                      false,
                                                      expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[2]);
                        ExpressionSimplifier<bool> evaluator = new ExpressionSimplifier<bool>();
                        bool isFirstOnly = evaluator.Eval(expression.Arguments[3]);
                        if (lambda != null)
                        {
                            if (nodeInfo.Children.Count == 2)
                            {
                                resNode = this.VisitApply(nodeInfo.Children[0].Child,
                                                          nodeInfo.Children[1].Child,
                                                          lambda,
                                                          true,
                                                          isFirstOnly,
                                                          expression);
                            }
                            else
                            {
                                // Apply with multiple sources of the same type
                                resNode = this.VisitMultiApply(nodeInfo, lambda, true, isFirstOnly, expression);
                            }
                        }
                    }
                    break;
                }
                case "Fork":
                {
                    if (expression.Arguments.Count == 2) // ForkSelect and ForkApply
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFork(nodeInfo.Children[0].Child,
                                                     lambda,
                                                     null,
                                                     expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3) // ForkByKey
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFork(nodeInfo.Children[0].Child,
                                                     lambda,
                                                     expression.Arguments[2],
                                                     expression);
                        }
                    }
                    break;
                }
                case "ForkChoose":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitForkChoose(nodeInfo.Children[0].Child,
                                                       expression.Arguments[1],
                                                       expression);
                    }
                    break;
                }
                case "AssumeHashPartition":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeHashPartition(nodeInfo.Children[0].Child,
                                                                    lambda,
                                                                    null,
                                                                    null,
                                                                    expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeHashPartition(nodeInfo.Children[0].Child,
                                                                    lambda,
                                                                    null,
                                                                    expression.Arguments[2],
                                                                    expression);
                        }
                    }
                    break;
                }
                case "AssumeRangePartition":
                {
                    if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type.IsArray)
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.Children[0].Child,
                                                                         lambda,
                                                                         expression.Arguments[2],
                                                                         null,
                                                                         null,
                                                                         expression);
                            }
                            else
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.Children[0].Child,
                                                                         lambda,
                                                                         null,
                                                                         null,
                                                                         expression.Arguments[2],
                                                                         expression);
                            }
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type.IsArray)
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.Children[0].Child,
                                                                         lambda,
                                                                         expression.Arguments[2],
                                                                         expression.Arguments[3],
                                                                         null,
                                                                         expression);
                            }
                            else
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.Children[0].Child,
                                                                         lambda,
                                                                         null,
                                                                         expression.Arguments[2],
                                                                         expression.Arguments[3],
                                                                         expression);
                            }
                        }
                    }
                    break;
                }
                case "AssumeOrderBy":
                {
                    if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeOrderBy(nodeInfo.Children[0].Child,
                                                              lambda,
                                                              null,
                                                              expression.Arguments[2],
                                                              expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeOrderBy(nodeInfo.Children[0].Child,
                                                              lambda,
                                                              expression.Arguments[2],
                                                              expression.Arguments[3],
                                                              expression);
                        }
                    }
                    break;
                }
                case "SlidingWindow":
                {
                    LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                    if (lambda != null && lambda.Parameters.Count == 1)
                    {
                        resNode = this.VisitSlidingWindow(nodeInfo.Children[0].Child,
                                                          lambda,
                                                          expression.Arguments[2],
                                                          expression);
                    }
                    break;
                }
                case "ApplyWithPartitionIndex":
                {
                    LambdaExpression lambda = DryadLinqExpression.GetLambda(expression.Arguments[1]);
                    if (lambda != null && lambda.Parameters.Count == 2)
                    {
                        resNode = this.VisitApplyWithPartitionIndex(nodeInfo.Children[0].Child,
                                                                    lambda,
                                                                    expression);
                    }
                    break;
                }
                case "DoWhile":
                {
                    DoWhileQueryNodeInfo doWhileNodeInfo = (DoWhileQueryNodeInfo)nodeInfo;
                    resNode = this.VisitDoWhile(nodeInfo.Children[0].Child,
                                                doWhileNodeInfo.Body,
                                                doWhileNodeInfo.Cond,
                                                doWhileNodeInfo.BodySource,
                                                doWhileNodeInfo.CondSource1,
                                                doWhileNodeInfo.CondSource2,
                                                expression);
                    break;
                }
                case "Dummy":
                {
                    resNode = this.VisitDummy(expression);
                    break;
                }
                case ReflectedNames.DLQ_ToStoreInternal:
                case ReflectedNames.DLQ_ToStoreInternalAux:
                {
                    //SHOULD NOT VISIT..
                    //Later if we do allow ToStore in the middle of query chain, then we need to either
                    //    1. update the source node with an outputeTableUri
                    // OR 2. create an actual node and handle it later on  (tee, etc)
                    throw new DryadLinqException("Internal error: Should never visit an expression of " + methodName);
                }
            }
            #endregion

            if (resNode == null)
            {
                throw DryadLinqException.Create(DryadLinqErrorCode.OperatorNotSupported,
                                                String.Format(SR.OperatorNotSupported, methodName),
                                                expression);
            }
            resNode.IsForked = resNode.IsForked || nodeInfo.IsForked;
            nodeInfo.QueryNode = resNode;
            return resNode;
        }
    }
}
