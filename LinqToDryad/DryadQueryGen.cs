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
using Microsoft.Hpc.Dryad;
using System.Globalization;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// This class handles code generation for multiple queries and execution invocation.
    /// </summary>
    internal class HpcLinqQueryGen
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
        private HpcLinqCodeGen m_codeGen;
        private Expression[] m_queryExprs;
        private DryadQueryNode[] m_queryPlan1;
        private DryadQueryNode[] m_queryPlan2;
        private DryadQueryNode[] m_queryPlan3;
        private string[] m_outputTableUris;
        private bool[] m_isTempOutput;
        private string[] m_outputDatapaths;
        private Type[] m_outputTypes;
        private QueryNodeInfo[] m_queryNodeInfos;
        private DryadLinqQuery[] m_outputTables;
        private string m_DryadLinqProgram;
        private string m_queryGraph;
        private Dictionary<Expression, QueryNodeInfo> m_exprNodeInfoMap;
        private Dictionary<Expression, QueryNodeInfo> m_referencedQueryMap;
        private Dictionary<string, DryadInputNode> m_inputUriMap;
        private Dictionary<string, DryadOutputNode> m_outputUriMap;
        private JobExecutor queryExecutor;
        private HpcLinqContext m_context;

        internal HpcLinqQueryGen(HpcLinqContext context,
                                 VertexCodeGen vertexCodeGen,
                                 Expression queryExpr,
                                 string tableUri,
                                 bool isTempOutput)
        {
            this.m_queryExprs = new Expression[] { queryExpr };
            string fullTableUri = tableUri;
            this.m_outputTableUris = new string[] { fullTableUri };
            this.m_isTempOutput = new bool[] { isTempOutput };
            this.m_context = context;
            this.Initialize(vertexCodeGen);
        }

        // This constructor is specifically to support Materialize() calls.
        // it assumes that the Expressions all terminate with a ToDsc node.
        internal HpcLinqQueryGen(HpcLinqContext context,
                                 VertexCodeGen vertexCodeGen,
                                 Expression[] qlist)
        {
            this.m_queryExprs = new Expression[qlist.Length];
            this.m_outputTableUris = new string[qlist.Length];
            this.m_isTempOutput = new bool[qlist.Length];
            this.m_context = context;
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                MethodCallExpression mcExpr = (MethodCallExpression)qlist[i];
                string tableUri;
                this.m_queryExprs[i] = mcExpr.Arguments[0];


                //this block supports the scenario: q-nonToDsc
                if (mcExpr.Method.Name == ReflectedNames.DryadLinqIQueryable_AnonymousDscPlaceholder)
                {
                    ExpressionSimplifier<string> e1 = new ExpressionSimplifier<string>();
                    tableUri = e1.Eval(mcExpr.Arguments[1]);
                    this.m_isTempOutput[i] = true;
                }

                //this block supports the scenario: q.ToDsc()
                else if (mcExpr.Method.Name == ReflectedNames.DryadLinqIQueryable_ToDscWorker)
                {
                    DscService dsc = context.DscService;
                    ExpressionSimplifier<string> e2 = new ExpressionSimplifier<string>();
                    string streamName = e2.Eval(mcExpr.Arguments[2]);

                    tableUri = DataPath.MakeDscStreamUri(dsc, streamName);
                    this.m_isTempOutput[i] = false;
                }

                //this block supports the scenario: q.ToHdfs()
                else if (mcExpr.Method.Name == ReflectedNames.DryadLinqIQueryable_ToHdfsWorker)
                {
                    string hdfsHeadNode = context.HdfsService;
                    ExpressionSimplifier<string> e2 = new ExpressionSimplifier<string>();
                    string streamName = e2.Eval(mcExpr.Arguments[2]);

                    tableUri = DataPath.MakeHdfsStreamUri(hdfsHeadNode, streamName);
                    this.m_isTempOutput[i] = false;
                }
                else {
                    throw new InvalidOperationException(); // should not occur.
                }

                this.m_outputTableUris[i] = tableUri;
                
            }
            this.Initialize(vertexCodeGen);
        }

        private void Initialize(VertexCodeGen vertexCodeGen)
        {
            this.m_codeGen = new HpcLinqCodeGen(this.m_context, vertexCodeGen);
            this.m_queryPlan1 = null;
            this.m_queryPlan2 = null;
            this.m_queryPlan3 = null;
            this.m_DryadLinqProgram = null;
            this.m_queryPlan1 = null;
            this.m_exprNodeInfoMap = new Dictionary<Expression, QueryNodeInfo>();
            this.m_referencedQueryMap = new Dictionary<Expression, QueryNodeInfo>();
            this.m_inputUriMap = new Dictionary<string, DryadInputNode>();
            this.m_outputUriMap = new Dictionary<string, DryadOutputNode>();
            this.queryExecutor = new JobExecutor(this.m_context);

            // Initialize the data structures for the output tables
            this.m_outputTypes = new Type[this.m_queryExprs.Length];
            this.m_outputDatapaths = new string[this.m_queryExprs.Length];
            this.m_queryNodeInfos = new QueryNodeInfo[this.m_queryExprs.Length];
            
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                this.m_queryNodeInfos[i] = this.BuildNodeInfoGraph(this.m_queryExprs[i]);
                this.m_queryNodeInfos[i] = new QueryNodeInfo(this.m_queryExprs[i], false, this.m_queryNodeInfos[i]);

                this.m_outputDatapaths[i] = DataPath.GetDataPath(this.m_outputTableUris[i]);
                
                Dictionary<string, string> args = DataPath.GetArguments(this.m_outputTableUris[i]);

                if (!(DataPath.IsDsc(this.m_outputDatapaths[i]) || DataPath.IsHdfs(this.m_outputDatapaths[i])))
                {
                    throw new DryadLinqException(HpcLinqErrorCode.UnrecognizedDataSource,
                                               String.Format(SR.UnrecognizedDataSource, this.m_outputTableUris[i]));
                }
            }
        }

        internal HpcLinqContext Context
        {
            get { return this.m_context; }
        }

        internal HpcLinqCodeGen CodeGen
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
            resourcesToExclude.AddRange(this.m_context.Configuration.ResourcesToRemove.Select(x => x.ToLower(CultureInfo.InvariantCulture)));

            IEnumerable<string> loadedAssemblyPaths = TypeSystem.GetLoadedNonSystemAssemblyPaths().Select(x => x.ToLower(CultureInfo.InvariantCulture));
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
                    // peKind will have the "Required32Bit" flag set only for x86 binaries. Therefore we use peKind to make our decision.
                    if ((peKind & PortableExecutableKinds.Required32Bit) != 0)
                    {
                        string offendingAssemblyName = Path.GetFileName(path);
                        throw new DryadLinqException(HpcLinqErrorCode.Binaries32BitNotSupported,
                                                   String.Format(SR.Binaries32BitNotSupported, offendingAssemblyName));
                    }
                }
            }
        }

        internal DryadLinqQuery[] InvokeDryad()
        {
            lock (s_queryGenLock)
            {
                this.GenerateDryadProgram();

                CheckAssemblyArchitectures();

                // Invoke the background execution
                this.queryExecutor.ExecuteAsync(this.m_DryadLinqProgram);
                
                // Create the resulting partitioned tables
                this.m_outputTables = new DryadLinqQuery[this.m_outputTableUris.Length];
                MethodInfo minfo = typeof(Microsoft.Research.DryadLinq.DataProvider).GetMethod(
                                              ReflectedNames.DataProvider_GetPartitionedTable,
                                              BindingFlags.NonPublic | BindingFlags.Static);
                for (int i = 0; i < this.m_outputTableUris.Length; i++)
                {
                    MethodInfo minfo1 = minfo.MakeGenericMethod(this.m_outputTypes[i]);
                    object[] args = new object[] { this.m_context, this.m_outputTableUris[i] };
                    this.m_outputTables[i] = (DryadLinqQuery)minfo1.Invoke(null, args);
                    this.m_outputTables[i].QueryExecutor = this.queryExecutor;
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
            this.m_queryPlan1 = new DryadQueryNode[this.m_queryExprs.Length + referencedNodes.Count];
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                this.m_queryPlan1[i] = this.Visit(this.m_queryNodeInfos[i].children[0].child);
            }
            int idx = this.m_queryExprs.Length;
            foreach (QueryNodeInfo nodeInfo in referencedNodes)
            {
                // Add a Tee'd Merge
                this.m_queryPlan1[idx] = this.Visit(nodeInfo.children[0].child);
                DryadQueryNode mergeNode = new DryadMergeNode(true, false, nodeInfo.queryExpression,
                                                              this.m_queryPlan1[idx]);
                this.m_queryPlan1[idx] =  new DryadTeeNode(mergeNode.OutputTypes[0], true,
                                                           mergeNode.QueryExpression, mergeNode);
                nodeInfo.queryNode = this.m_queryPlan1[idx];
                idx++;
            }

            // Finally, add the output nodes.
            Dictionary<DryadQueryNode, int> forkCounts = new Dictionary<DryadQueryNode, int>();
            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                DryadQueryNode queryNode = this.m_queryPlan1[i];
                int cnt;
                if (!forkCounts.TryGetValue(queryNode, out cnt))
                {
                    cnt = queryNode.Parents.Count;
                }
                forkCounts[queryNode] = cnt + 1;
            }

            for (int i = 0; i < this.m_queryExprs.Length; i++)
            {
                HpcClientSideLog.Add("Query " + i + " Output: " + this.m_outputDatapaths[i]);

                DryadQueryNode queryNode = this.m_queryPlan1[i];
                if (TypeSystem.IsAnonymousType(queryNode.OutputTypes[0]))
                {
                    throw new DryadLinqException(HpcLinqErrorCode.OutputTypeCannotBeAnonymous,
                                               SR.OutputTypeCannotBeAnonymous);
                }

                // Add dummy Apply to make Dryad happy (it doesn't like to hook inputs straight to outputs)
                if ((queryNode is DryadInputNode) || (forkCounts[queryNode] > 1))
                {
                    // Add a dummy Apply
                    Type paramType = typeof(IEnumerable<>).MakeGenericType(queryNode.OutputTypes[0]);
                    ParameterExpression param = Expression.Parameter(paramType, "x");
                    Type type = typeof(Func<,>).MakeGenericType(paramType, paramType);
                    LambdaExpression applyExpr = Expression.Lambda(type, param, param);
                    DryadQueryNode applyNode = new DryadApplyNode(applyExpr, this.m_queryExprs[i], queryNode);
                    applyNode.OutputDataSetInfo = queryNode.OutputDataSetInfo;
                    queryNode = applyNode;
                }

                if (queryNode is DryadConcatNode)
                {
                    // Again, we add dummy Apply in certain cases to make Dryad happy
                    ((DryadConcatNode)queryNode).FixInputs();
                }

                // Add the output node                
                DscCompressionScheme outputScheme = this.m_context.Configuration.OutputDataCompressionScheme;
                DryadOutputNode outputNode = new DryadOutputNode(this.m_context,
                                                                 this.m_outputDatapaths[i],
                                                                 this.m_isTempOutput[i],
                                                                 outputScheme,
                                                                 this.m_queryExprs[i],
                                                                 queryNode);

                this.m_queryPlan1[i] = outputNode;

                if (this.m_outputUriMap.ContainsKey(this.m_outputDatapaths[i].ToLower()))
                {
                    throw new DryadLinqException(HpcLinqErrorCode.MultipleOutputsWithSameDscUri,
                                               String.Format(SR.MultipleOutputsWithSameDscUri, this.m_outputDatapaths[i]));
                }

                this.m_outputUriMap.Add(this.m_outputDatapaths[i].ToLower(), outputNode);
                this.m_outputTypes[i] = this.m_queryPlan1[i].OutputTypes[0];
                    
                // Remove useless Tees to make Dryad happy                
                if ((queryNode is DryadTeeNode) && (forkCounts[queryNode] == 1))
                {
                    DryadQueryNode teeChild = queryNode.Children[0];
                    teeChild.UpdateParent(queryNode, outputNode);
                    outputNode.UpdateChildren(queryNode, teeChild);
                }
            }
        }

        // Phase 2 of the query optimization
        internal DryadQueryNode[] GenerateQueryPlanPhase2()
        {
            if (this.m_queryPlan2 == null)
            {
                this.GenerateQueryPlanPhase1();
                this.m_queryPlan2 = new DryadQueryNode[this.m_queryPlan1.Length];
                for (int i = 0; i < this.m_queryPlan1.Length; i++)
                {
                    this.m_queryPlan2[i] = this.VisitPhase2(this.m_queryPlan1[i]);
                }
                this.m_currentPhaseId++;
            }
            return this.m_queryPlan2;
        }

        private DryadQueryNode VisitPhase2(DryadQueryNode node)
        {
            DryadQueryNode resNode = node;
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                if (node is DryadForkNode)
                {
                    // For now, we require every branch of a fork be used:
                    DryadForkNode forkNode = (DryadForkNode)node;
                    for (int i = 0; i < forkNode.Parents.Count; i++)
                    {
                        if ((forkNode.Parents[i] is DryadTeeNode) &&
                            (forkNode.Parents[i].Parents.Count == 0))
                        {
                            throw DryadLinqException.Create(HpcLinqErrorCode.BranchOfForkNotUsed,
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

                    // Insert a Tee node if needed:
                    DryadQueryNode outputNode = resNode.OutputNode;
                    if (outputNode.IsForked &&
                        !(outputNode is DryadForkNode) &&
                        !(outputNode is DryadTeeNode))
                    {
                        resNode = resNode.InsertTee(true);
                    }
                }
            }
            return resNode;
        }

        // Phase 3 of the query optimization
        internal DryadQueryNode[] GenerateQueryPlanPhase3()
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

        private void VisitPhase3(DryadQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                node.m_uniqueId++;

                // Remove some useless Tee nodes
                foreach (DryadQueryNode child in node.Children)
                {
                    if ((child is DryadTeeNode) && !child.IsForked)
                    {
                        DryadQueryNode teeChild = child.Children[0];
                        teeChild.UpdateParent(child, node);
                        node.UpdateChildren(child, teeChild);
                    }
                }

                // Remove some useless Merge nodes
                if ((node is DryadMergeNode) &&
                    !node.IsForked &&
                    !(node.Parents[0] is DryadOutputNode) &&
                    !node.Children[0].IsDynamic &&
                    node.Children[0].PartitionCount == 1)
                {
                    node.Children[0].UpdateParent(node, node.Parents[0]);
                    node.Parents[0].UpdateChildren(node, node.Children[0]);
                }

                // Add dynamic managers for tee nodes.
                if ((StaticConfig.DynamicOptLevel & StaticConfig.NoDynamicOpt) != 0 && 
                    node is DryadTeeNode &&
                    node.DynamicManager.ManagerType == DynamicManagerType.None)
                {
                    // insert a dynamic broadcast manager on Tee
                    node.DynamicManager = DynamicManager.Broadcast;
                }

                // Recurse on the children of node
                foreach (DryadQueryNode child in node.Children)
                {
                    this.VisitPhase3(child);
                }
            }
        }
        
        // The main method that generates the query plan.
        internal DryadQueryNode[] QueryPlan()
        {
            return this.GenerateQueryPlanPhase3();
        }

        // Generate the vertex code for all the query nodes.
        internal void CodeGenVisit()
        {
            if (!this.m_codeGenDone)
            {
                DryadQueryNode[] optimizedPlan = this.QueryPlan();

                // make sure none of the outputs share a URI with inputs
                foreach (var kvp in this.m_outputUriMap)
                {
                    string outputPath = kvp.Key;
                    if(m_inputUriMap.ContainsKey(outputPath))
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.OutputUriAlsoQueryInput,
                                                   String.Format(SR.OutputUriAlsoQueryInput, outputPath));
                    }
                }

                foreach (DryadQueryNode node in optimizedPlan) 
                {
                    this.CodeGenVisit(node);
                }
                this.m_currentPhaseId++;
                this.m_codeGenDone = true;
            }
        }

        private void CodeGenVisit(DryadQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                node.m_uniqueId++;

                // We process the types first so that children will also know about all 
                // proxies/mappings that should be used.
                node.CreateCodeAndMappingsForVertexTypes(false);

                // Recurse on the children
                foreach (DryadQueryNode child in node.Children)
                {
                    this.CodeGenVisit(child);
                }

                if (node.NodeType == QueryNodeType.InputTable)
                {
                    // not used as a vertex
                    string t = ((DryadInputNode)node).Table.DataSourceUri;
                    int index = t.LastIndexOf('/');
                    int bk = t.LastIndexOf('\\');
                    if (index < bk) index = bk;
                    node.m_vertexEntryMethod = t.Substring(index + 1);
                }
                else if (node.NodeType == QueryNodeType.OutputTable)
                {
                    // not used as a vertex
                    string t = ((DryadOutputNode)node).MetaDataUri;
                    int index = t.LastIndexOf('/');
                    int bk = t.LastIndexOf('\\');
                    if (index < bk) index = bk;
                    int len = Math.Min(8, t.Length - index - 1);
                    node.m_vertexEntryMethod = t.Substring(index + 1, len);
                }
                else if (node.NodeType == QueryNodeType.Tee)
                {
                    // not used as a vertex
                    node.m_vertexEntryMethod = HpcLinqCodeGen.MakeUniqueName("Tee");
                    // broadcast manager code generation
                    if (node.DynamicManager.ManagerType != DynamicManagerType.None)
                    {
                        node.DynamicManager.CreateVertexCode();
                    }
                }
                else if (node.NodeType == QueryNodeType.Concat)
                {
                    // not used as a vertex
                    node.m_vertexEntryMethod = HpcLinqCodeGen.MakeUniqueName("Concat");
                }
                else
                {
                    CodeMemberMethod vertexMethod = this.m_codeGen.AddVertexMethod(node);
                    node.m_vertexEntryMethod = vertexMethod.Name;
                    node.DynamicManager.CreateVertexCode();
                }
            }
        }

        // Assign unique ids to all the query nodes
        private void AssignUniqueId()
        {
            if (this.m_currentPhaseId != -1)
            {
                //@@TODO: this should not be reachable. could change to Assert/InvalidOpEx
                throw new DryadLinqException(HpcLinqErrorCode.Internal,
                                           "Internal error: Optimization phase should be -1, not " +
                                           this.m_currentPhaseId);
            }
            foreach (DryadQueryNode node in this.QueryPlan())
            {
                this.AssignUniqueId(node);
            }
        }

        private void AssignUniqueId(DryadQueryNode node)
        {
            if (node.m_uniqueId == this.m_currentPhaseId)
            {
                foreach (Pair<string, DryadQueryNode> refChild in node.GetReferencedQueries())
                {
                    this.AssignUniqueId(refChild.Value);
                }
                foreach (DryadQueryNode child in node.Children)
                {
                    this.AssignUniqueId(child);
                }
                if (node.m_uniqueId == this.m_currentPhaseId)
                {
                    node.m_uniqueId = this.m_nextVertexId++;

                    if (node.OutputNode is DryadForkNode)
                    {
                        foreach (DryadQueryNode pnode in node.Parents)
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
            foreach (DryadQueryNode node in this.QueryPlan())
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
        /// <returns>Handle to the inserted node.</returns>
        private void AddResourceToPlan(XmlDocument queryDoc,
                                       XmlElement parent,
                                       string resource,
                                       IEnumerable<string> resourcesToExclude)
        {
            AddResourceToPlan_Core(queryDoc, parent, resource, resourcesToExclude);

            if (this.m_context.Configuration.CompileForVertexDebugging)
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
            resourceElem.InnerText = queryExecutor.AddResource(resource);
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
            if (Environment.Version.Major > 2 && this.m_context.Configuration.MatchClientNetFrameworkVersion)
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
    </runtime>

    <startup useLegacyV2RuntimeActivationPolicy=""true"">
";

            appConfigBody += clientVersionString;   // add the client specific version string if we generated one.
            appConfigBody +=            
@"      <supportedRuntime version=""v2.0.50727""/>
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
            HpcLinqObjectStore.Clear();

            // Any resource that we try to add will be tested against this list.
            IEnumerable<string> resourcesToExclude = this.m_context.Configuration.ResourcesToRemove;

            // BuildDryadLinqAssembly:
            //   1. Performs query optimizations
            //   2. Generate vertex code for all the nodes in the query plan
            this.m_codeGen.BuildDryadLinqAssembly(this);

            DryadQueryExplain explain = new DryadQueryExplain();
            HpcClientSideLog.Add("{0}", explain.Explain(this));

            // Finally, write out the query plan.
            if (this.m_DryadLinqProgram == null)
            {
                int progId = Interlocked.Increment(ref s_uniqueProgId);
                this.m_DryadLinqProgram = HpcLinqCodeGen.GetPathForGeneratedFile(DryadLinqProgram, progId);
                this.m_queryGraph = HpcLinqCodeGen.GetPathForGeneratedFile(QueryGraph, progId); 
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
            elem.InnerText = this.m_context.Configuration.HeadNode;
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the minimum number of nodes
            int minComputeNodes = 1; 
            if (this.m_context.Configuration.JobMinNodes.HasValue)
            {
                minComputeNodes = this.m_context.Configuration.JobMinNodes.Value;
            }
            elem = queryDoc.CreateElement("MinimumComputeNodes");
            elem.InnerText = minComputeNodes.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the maximum number of nodes
            int maxComputeNodes = -1;
            if (this.m_context.Configuration.JobMaxNodes.HasValue)
            {
               maxComputeNodes = this.m_context.Configuration.JobMaxNodes.Value;
            }
            elem = queryDoc.CreateElement("MaximumComputeNodes");
            elem.InnerText = maxComputeNodes.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // intermediate data compression
            elem = queryDoc.CreateElement("IntermediateDataCompression");
            elem.InnerText = ((int)this.m_context.Configuration.IntermediateDataCompressionScheme).ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Speculative Duplication Node
            elem = queryDoc.CreateElement("EnableSpeculativeDuplication");
            elem.InnerText = this.m_context.Configuration.EnableSpeculativeDuplication.ToString();
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the visualization element
            //@@TODO[p2]: remove this element from the queryXML.
            elem = queryDoc.CreateElement("Visualization");
            elem.InnerText = "none";
            queryDoc.DocumentElement.AppendChild(elem);

            // Add the query name element
            elem = queryDoc.CreateElement("QueryName");
            if (String.IsNullOrEmpty(this.m_context.Configuration.JobFriendlyName))
            {
                elem.InnerText = asmName.Name; 
            }
            else
            {
                elem.InnerText = this.m_context.Configuration.JobFriendlyName;
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
            foreach (string resourcePath in this.m_codeGen.VertexCodeGen.GetResources())
            {
                AddResourceToPlan(queryDoc, elem, resourcePath, resourcesToExclude);
            }

            // Create an app config file for the VertexHost process, and add it to the resources
            string vertexHostAppConfigPath = HpcLinqCodeGen.GetPathForGeneratedFile(Path.ChangeExtension(VertexHostExe, "exe.config"), null);
            GenerateAppConfigResource(vertexHostAppConfigPath);
            AddResourceToPlan(queryDoc, elem, vertexHostAppConfigPath, resourcesToExclude);

            // Save and add the object store as a resource
            if (!HpcLinqObjectStore.IsEmpty)
            {
                HpcLinqObjectStore.Save();
                AddResourceToPlan(queryDoc, elem, HpcLinqObjectStore.GetClientSideObjectStorePath(), resourcesToExclude);
            }

            // Add resource item for user-added resources
            foreach (string userResource in this.m_context.Configuration.ResourcesToAdd.Distinct(StringComparer.OrdinalIgnoreCase))
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
                QueryNodeInfo nodeInfo = BuildNodeInfoGraph(qexpr);
                this.m_referencedQueryMap[qexpr] = new QueryNodeInfo(qexpr, false, nodeInfo);
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
                QueryNodeInfo nodeInfo = BuildNodeInfoGraph(qexpr);
                this.m_referencedQueryMap[qexpr] = new QueryNodeInfo(qexpr, false, nodeInfo);
            }
        }

        //@@TODO: document what the 'NodeInfo' and 'ReferencedQuery' system does
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
                        QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        QueryNodeInfo child2 = BuildNodeInfoGraph(mcExpr.Arguments[1]);
                        resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        this.BuildReferencedQuery(2, mcExpr.Arguments);
                        break;
                    }
                    case "Concat":                                                
                    {
                        NewArrayExpression others = mcExpr.Arguments[1] as NewArrayExpression;
                        if (others == null)
                        {
                            QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                            QueryNodeInfo child2 = BuildNodeInfoGraph(mcExpr.Arguments[1]);
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        }
                        else
                        {
                            QueryNodeInfo[] infos = new QueryNodeInfo[others.Expressions.Count + 1];
                            infos[0] = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                            for (int i = 0; i < others.Expressions.Count; ++i)
                            {
                                infos[i + 1] = BuildNodeInfoGraph(others.Expressions[i]);
                            }
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, infos);
                        }
                        break;
                    }
                    case "Apply":
                    case "ApplyPerPartition":
                    {
                        QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        if (mcExpr.Arguments.Count == 2)
                        {
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1);
                            this.BuildReferencedQuery(mcExpr.Arguments[1]);
                        }
                        else
                        {
                            LambdaExpression lambda = HpcLinqExpression.GetLambda(mcExpr.Arguments[2]);
                            if (lambda.Parameters.Count == 2)
                            {
                                // Apply with two sources
                                QueryNodeInfo child2 = BuildNodeInfoGraph(mcExpr.Arguments[1]);
                                resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                                this.BuildReferencedQuery(mcExpr.Arguments[2]);
                            }
                            else
                            {
                                // Apply with multiple sources of the same type
                                NewArrayExpression others = (NewArrayExpression)mcExpr.Arguments[1];
                                QueryNodeInfo[] infos = new QueryNodeInfo[others.Expressions.Count + 1];
                                infos[0] = child1;
                                for (int i = 0; i < others.Expressions.Count; ++i)
                                {
                                    infos[i + 1] = BuildNodeInfoGraph(others.Expressions[i]);
                                }
                                resNodeInfo = new QueryNodeInfo(mcExpr, true, infos);
                                this.BuildReferencedQuery(mcExpr.Arguments[2]);
                            }
                        }
                        break;
                    }
                    case "RangePartition":
                    {
                        QueryNodeInfo child1 = BuildNodeInfoGraph(mcExpr.Arguments[0]);
                        if (mcExpr.Arguments.Count == 6)
                        {
                            // This is a key part of handling for RangePartition( ... , IQueryable<> keysQuery, ...)
                            // The keys expression is established as a child[1] nodeInfo for the rangePartition node.
                            QueryNodeInfo child2 = BuildNodeInfoGraph(mcExpr.Arguments[2]);
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1, child2);
                        }
                        else
                        {
                            resNodeInfo = new QueryNodeInfo(mcExpr, true, child1);
                        }
                        this.BuildReferencedQuery(mcExpr.Arguments[1]);
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

        private static bool IsMergeNodeNeeded(DryadQueryNode node)
        {
            return node.IsDynamic || node.PartitionCount > 1;
        }
        
        internal DryadQueryNode Visit(QueryNodeInfo nodeInfo)
        {
            Expression expression = nodeInfo.queryExpression;
            if (expression.NodeType == ExpressionType.Call)
            {
                MethodCallExpression mcExpr = (MethodCallExpression)expression;
                if (mcExpr.Method.IsStatic && TypeSystem.IsQueryOperatorCall(mcExpr))
                {
                    return this.VisitQueryOperatorCall(nodeInfo);
                }
                
                throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                              String.Format(SR.OperatorNotSupported, mcExpr.Method.Name),
                                              expression);
            }
            else if (expression.NodeType == ExpressionType.Constant)
            {
                DryadInputNode inputNode = new DryadInputNode(this, (ConstantExpression)expression);
                if (!this.m_inputUriMap.ContainsKey(inputNode.Table.DataSourceUri.ToLower()))
                {
                    this.m_inputUriMap.Add(inputNode.Table.DataSourceUri.ToLower(), inputNode);
                }
                return inputNode;
            }
            else
            {
                string errMsg = "Can't handle expression of type " + expression.NodeType;
                throw DryadLinqException.Create(HpcLinqErrorCode.UnsupportedExpressionsType,
                                              String.Format(SR.UnsupportedExpressionsType,expression.NodeType),
                                              expression);
            }
        }

        private static bool IsLambda(Expression expr, int n)
        {
            LambdaExpression lambdaExpr = HpcLinqExpression.GetLambda(expr);
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

        private DryadQueryNode CreateOffset(bool isLong, Expression queryExpr, DryadQueryNode child)
        {
            // Count node
            DryadQueryNode countNode = new DryadBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                   true, false, queryExpr, child);

            // Apply node for x => Offsets(x)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(typeof(long));
            ParameterExpression param = Expression.Parameter(paramType, "x");
            MethodInfo minfo = typeof(HpcLinqEnumerable).GetMethod("Offsets");
            Expression body = Expression.Call(minfo, param, Expression.Constant(isLong, typeof(bool)));
            Type type = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression procFunc = Expression.Lambda(type, body, param);
            DryadQueryNode mergeCountNode = new DryadMergeNode(true, true, queryExpr, countNode);
            DryadQueryNode offsetsNode = new DryadApplyNode(procFunc, queryExpr, mergeCountNode);

            // HashPartition
            LambdaExpression keySelectExpr = IdentityFunction.Instance(typeof(IndexedValue<long>));
            int pcount = child.OutputPartition.Count;
            DryadQueryNode hdistNode = new DryadHashPartitionNode(keySelectExpr, null, null, pcount,
                                                                  false, queryExpr, offsetsNode);
            DryadQueryNode resNode = new DryadMergeNode(false, true, queryExpr, hdistNode);
            return resNode;
        }

        private DryadQueryNode PromoteConcat(QueryNodeInfo source,
                                             DryadQueryNode sourceNode,
                                             Func<DryadQueryNode, DryadQueryNode> func)
        {
            DryadQueryNode resNode = sourceNode;
            if ((resNode is DryadConcatNode) && !source.IsForked)
            {
                DryadQueryNode[] children = resNode.Children;
                DryadQueryNode[] newChildren = new DryadQueryNode[children.Length];
                for (int i = 0; i < children.Length; i++)
                {
                    children[i].Parents.Remove(resNode);
                    newChildren[i] = func(children[i]);
                }
                resNode = new DryadConcatNode(source.queryExpression, newChildren);
            }
            else
            {
                resNode = func(resNode);
            }
            return resNode;
        }
  
        private DryadQueryNode VisitSelect(QueryNodeInfo source,
                                           QueryNodeType nodeType,
                                           LambdaExpression selector,
                                           LambdaExpression resultSelector,
                                           MethodCallExpression queryExpr)
        {
            DryadQueryNode selectNode;
            if (selector.Type.GetGenericArguments().Length == 2)
            {
                // If this select's child is a groupby node, push this select into its child, if
                //   1. The groupby node is not tee'd, and
                //   2. The groupby node has no result selector, and
                //   3. The selector is decomposable
                if (!source.IsForked &&
                    IsGroupByWithoutResultSelector(source.queryExpression) &&
                    Decomposition.GetDecompositionInfoList(selector, m_codeGen) != null)
                {
                    MethodCallExpression expr = (MethodCallExpression)source.queryExpression;
                    LambdaExpression keySelectExpr = HpcLinqExpression.GetLambda(expr.Arguments[1]);

                    // Figure out elemSelectExpr and comparerExpr
                    LambdaExpression elemSelectExpr = null;
                    Expression comparerExpr = null;
                    if (expr.Arguments.Count == 3)
                    {
                        elemSelectExpr = HpcLinqExpression.GetLambda(expr.Arguments[2]);
                        if (elemSelectExpr == null)
                        {
                            comparerExpr = expr.Arguments[2];
                        }
                    }
                    else if (expr.Arguments.Count == 4)
                    {
                        elemSelectExpr = HpcLinqExpression.GetLambda(expr.Arguments[2]);
                        comparerExpr = expr.Arguments[3];
                    }

                    // Construct new query expression by building result selector expression
                    // and pushing it to groupby node.
                    selectNode = VisitGroupBy(source.children[0].child, keySelectExpr,
                                              elemSelectExpr, selector, comparerExpr, queryExpr);
                    if (nodeType == QueryNodeType.SelectMany)
                    {
                        Type selectorRetType = selector.Type.GetGenericArguments()[1];
                        LambdaExpression id = IdentityFunction.Instance(selectorRetType);
                        selectNode = new DryadSelectNode(nodeType, id, resultSelector, queryExpr, selectNode);
                    }
                }
                else
                {
                    DryadQueryNode child = this.Visit(source);
                    selectNode = this.PromoteConcat(
                                         source, child,
                                         x => new DryadSelectNode(nodeType, selector, resultSelector, queryExpr, x));
                }
            }
            else
            {
                // The "indexed" version
                DryadQueryNode child = this.Visit(source);
                if (!child.IsDynamic && child.OutputPartition.Count == 1)
                {
                    selectNode = this.PromoteConcat(
                                         source, child,
                                         x => new DryadSelectNode(nodeType, selector, resultSelector, queryExpr, x));
                }
                else
                {
                    child.IsForked = true;

                    // Create (x, y) => Select(x, y, selector)
                    Type ptype1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                    Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                    ParameterExpression param1 = Expression.Parameter(ptype1, HpcLinqCodeGen.MakeUniqueName("x"));
                    ParameterExpression param2 = Expression.Parameter(ptype2, HpcLinqCodeGen.MakeUniqueName("y"));
                    
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
                    MethodInfo minfo = typeof(HpcLinqEnumerable).GetMethod(targetMethodName);
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
                    DryadQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, child);
                    selectNode = new DryadApplyNode(procFunc, queryExpr, child, offsetNode);
                }
            }
            return selectNode;
        }

        private DryadQueryNode VisitWhere(QueryNodeInfo source,
                                          LambdaExpression predicate,
                                          MethodCallExpression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            DryadQueryNode whereNode;            
            if (predicate.Type.GetGenericArguments().Length == 2 ||
                (!child.IsDynamic && child.OutputPartition.Count == 1))
            {
                whereNode = this.PromoteConcat(source, child, x => new DryadWhereNode(predicate, queryExpr, x));
            }
            else
            {
                // The "indexed" version
                // Create (x, y) => DryadWhere(x, y, predicate)
                Type ptype1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                ParameterExpression param1 = Expression.Parameter(ptype1, HpcLinqCodeGen.MakeUniqueName("x"));
                ParameterExpression param2 = Expression.Parameter(ptype2, HpcLinqCodeGen.MakeUniqueName("y"));
                string targetMethod = queryExpr.Method.Name + "WithStartIndex";
                MethodInfo minfo = typeof(HpcLinqEnumerable).GetMethod(targetMethod);
                minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param1, param2, predicate);
                Type type = typeof(Func<,,>).MakeGenericType(ptype1, ptype2, body.Type);
                LambdaExpression procFunc = Expression.Lambda(type, body, param1, param2);

                child.IsForked = true;
                bool isLong = (queryExpr.Method.Name == "LongWhere");
                DryadQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, child);
                whereNode = new DryadApplyNode(procFunc, queryExpr, child, offsetNode);
            }
            return whereNode;
        }

        private DryadQueryNode VisitJoin(QueryNodeInfo outerSource,
                                         QueryNodeInfo innerSource,
                                         QueryNodeType nodeType,
                                         LambdaExpression outerKeySelector,
                                         LambdaExpression innerKeySelector,
                                         LambdaExpression resultSelector,
                                         Expression comparerExpr,
                                         Expression queryExpr)
        {
            DryadQueryNode outerChild = this.Visit(outerSource);
            DryadQueryNode innerChild = this.Visit(innerSource);
            DryadQueryNode joinNode = null;

            Type keyType = outerKeySelector.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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
                outerChild = new DryadHashPartitionNode(outerKeySelector,
                                                        comparerExpr,
                                                        StaticConfig.DefaultPartitionCount,
                                                        queryExpr,
                                                        outerChild);
                if (IsMergeNodeNeeded(outerChild))
                {
                    outerChild = new DryadMergeNode(false, false, queryExpr, outerChild);
                }
                
                innerChild = new DryadHashPartitionNode(innerKeySelector,
                                                        comparerExpr,
                                                        StaticConfig.DefaultPartitionCount,
                                                        queryExpr,
                                                        innerChild);
                if (IsMergeNodeNeeded(innerChild))
                {
                    innerChild = new DryadMergeNode(false, false, queryExpr, innerChild);
                }

                joinNode = new DryadJoinNode(nodeType,
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
                        innerChild = new DryadMergeNode(false, false, queryExpr, innerChild);
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
                    outerChild = new DryadMergeNode(false, false, queryExpr, outerChild);
                }
            }
            else if (outerChild.OutputPartition.ParType == PartitionType.Hash &&
                     outerChild.OutputPartition.IsPartitionedBy(outerKeySelector, comparer))
            {
                if (innerChild.OutputPartition.ParType != PartitionType.Hash ||
                    !innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer) ||
                    !outerChild.OutputPartition.IsSamePartition(innerChild.OutputPartition))
                {
                    innerChild = new DryadHashPartitionNode(innerKeySelector,
                                                            comparerExpr,
                                                            outerChild.OutputPartition.Count,
                                                            queryExpr,
                                                            innerChild);
                    if (IsMergeNodeNeeded(innerChild))
                    {
                        innerChild = new DryadMergeNode(false, false, queryExpr, innerChild);
                    }
                }
            }
            else if (innerChild.OutputPartition.ParType == PartitionType.Hash &&
                     innerChild.OutputPartition.IsPartitionedBy(innerKeySelector, comparer))
            {
                outerChild = new DryadHashPartitionNode(outerKeySelector,
                                                        comparerExpr,
                                                        innerChild.OutputPartition.Count,
                                                        queryExpr,
                                                        outerChild);
                if (IsMergeNodeNeeded(outerChild))
                {
                    outerChild = new DryadMergeNode(false, false, queryExpr, outerChild);
                }
            }
            else
            {
                // No luck. Hash partition both outer and inner
                int parCnt = Math.Max(outerChild.OutputPartition.Count, innerChild.OutputPartition.Count);
                if (parCnt > 1)
                {
                    outerChild = new DryadHashPartitionNode(outerKeySelector,
                                                            comparerExpr,
                                                            parCnt,
                                                            queryExpr,
                                                            outerChild);
                    if (IsMergeNodeNeeded(outerChild))
                    {
                        outerChild = new DryadMergeNode(false, false, queryExpr, outerChild);
                    }

                    innerChild = new DryadHashPartitionNode(innerKeySelector,
                                                            comparerExpr,
                                                            parCnt,
                                                            queryExpr,
                                                            innerChild);
                    if (IsMergeNodeNeeded(innerChild))
                    {
                        innerChild = new DryadMergeNode(false, false, queryExpr, innerChild);
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
                    innerChild = new DryadOrderByNode(innerKeySelector, comparerExpr,
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
                    outerChild = new DryadOrderByNode(outerKeySelector, comparerExpr,
                                                      isInnerDescending, queryExpr, outerChild);
                }
                opName = "Merge";                
            }
                    
            joinNode = new DryadJoinNode(nodeType,
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

        private DryadQueryNode VisitDistinct(QueryNodeInfo source,
                                             Expression comparerExpr,
                                             Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = child.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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
                    child = new DryadDistinctNode(true, comparerExpr, queryExpr, child);
                    bool isDynamic = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicHashPartitionLevel) != 0;
                    child = new DryadHashPartitionNode(keySelectExpr,
                                                       comparerExpr,
                                                       child.OutputPartition.Count,
                                                       isDynamic,
                                                       queryExpr,
                                                       child);
                    child = new DryadMergeNode(false, false, queryExpr, child);
                }
            }
            DryadQueryNode resNode = new DryadDistinctNode(false, comparerExpr, queryExpr, child);
            return resNode;
        }

        private DryadQueryNode VisitConcat(QueryNodeInfo source, MethodCallExpression queryExpr)
        {
            DryadQueryNode[] childs = new DryadQueryNode[source.children.Count];
            for (int i = 0; i < source.children.Count; ++i)
            {
                childs[i] = this.Visit(source.children[i].child);
            }
            DryadQueryNode resNode = new DryadConcatNode(queryExpr, childs);

            int parCount = resNode.OutputPartition.Count;
            if (!resNode.IsDynamic && parCount > StaticConfig.MaxPartitionCount)
            {
                // Too many partitions, need to repartition
                int newParCount = parCount / 2;
                DryadQueryNode countNode = new DryadBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                       true, false, queryExpr, resNode);
                DryadQueryNode mergeCountNode = new DryadMergeNode(true, false, queryExpr, countNode);

                // Apply node for s => IndexedCount(s)
                Type paramType = typeof(IEnumerable<>).MakeGenericType(typeof(long));
                ParameterExpression param = Expression.Parameter(paramType, "s");
                MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("IndexedCount");
                minfo = minfo.MakeGenericMethod(typeof(long));
                Expression body = Expression.Call(minfo, param);
                Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
                LambdaExpression indexedCountFunc = Expression.Lambda(funcType, body, param);
                DryadQueryNode indexedCountNode = new DryadApplyNode(indexedCountFunc, queryExpr, mergeCountNode);

                // HashPartition(x => x.index, parCount)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                Expression keySelectBody = Expression.Property(param, "Index");                
                funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
                LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
                DryadQueryNode distCountNode = new DryadHashPartitionNode(keySelectExpr,
                                                                          null,
                                                                          parCount,
                                                                          queryExpr,
                                                                          indexedCountNode);

                // Apply node for (x, y) => AddPartitionIndex(x, y, newParCount)
                ParameterExpression param1 = Expression.Parameter(body.Type, "x");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(resNode.OutputTypes[0]);
                ParameterExpression param2 = Expression.Parameter(paramType2, "y");
                minfo = typeof(HpcLinqHelper).GetMethod("AddPartitionIndex");
                minfo = minfo.MakeGenericMethod(resNode.OutputTypes[0]);
                body = Expression.Call(minfo, param1, param2, Expression.Constant(newParCount));
                funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression addIndexFunc = Expression.Lambda(funcType, body, param1, param2);
                DryadQueryNode addIndexNode = new DryadApplyNode(addIndexFunc, queryExpr, distCountNode, resNode);

                // HashPartition(x => x.index, x => x.value, newParCount)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                body = Expression.Property(param, "Index");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                keySelectExpr = Expression.Lambda(funcType, body, param);
                body = Expression.Property(param, "Value");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
                resNode = new DryadHashPartitionNode(keySelectExpr,
                                                     resultSelectExpr,
                                                     null,
                                                     newParCount,
                                                     false,
                                                     queryExpr,
                                                     addIndexNode);
                resNode = new DryadMergeNode(true, true, queryExpr, resNode);
            }
            return resNode;
        }

        private DryadQueryNode VisitSetOperation(QueryNodeInfo source1,
                                                 QueryNodeInfo source2,
                                                 QueryNodeType nodeType,
                                                 Expression comparerExpr,
                                                 Expression queryExpr)
        {
            DryadQueryNode child1 = this.Visit(source1);
            DryadQueryNode child2 = this.Visit(source2);
            DryadQueryNode resNode = null;

            Type keyType = child1.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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
                child1 = new DryadHashPartitionNode(keySelectExpr,
                                                    null,
                                                    StaticConfig.DefaultPartitionCount,
                                                    queryExpr,
                                                    child1);
                if (IsMergeNodeNeeded(child1))
                {
                    child1 = new DryadMergeNode(false, false, queryExpr, child1);
                }

                child2 = new DryadHashPartitionNode(keySelectExpr,
                                                    null,
                                                    StaticConfig.DefaultPartitionCount,
                                                    queryExpr,
                                                    child2);
                if (IsMergeNodeNeeded(child2))
                {
                    child2 = new DryadMergeNode(false, false, queryExpr, child2);
                }

                resNode = new DryadSetOperationNode(nodeType, nodeType.ToString(), comparerExpr,
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
                        child2 = new DryadMergeNode(false, false, queryExpr, child2);
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
                    child1 = new DryadMergeNode(false, false, queryExpr, child1);
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
                    child2 = new DryadHashPartitionNode(keySelectExpr,
                                                        comparerExpr,
                                                        child1.OutputPartition.Count,
                                                        queryExpr,
                                                        child2);
                    if (IsMergeNodeNeeded(child2))
                    {
                        child2 = new DryadMergeNode(false, false, queryExpr, child2);
                    }
                }
            }
            else if (child2.OutputPartition.ParType == PartitionType.Hash &&
                     child2.OutputPartition.IsPartitionedBy(keySelectExpr, comparer))
            {
                child1 = new DryadHashPartitionNode(keySelectExpr,
                                                    comparerExpr,
                                                    child2.OutputPartition.Count,
                                                    queryExpr,
                                                    child1);
                if (IsMergeNodeNeeded(child1))
                {
                    child1 = new DryadMergeNode(false, false, queryExpr, child1);
                }
            }
            else
            {
                // No luck. Hash distribute both child1 and child2, then perform hash operation
                int parCnt = Math.Max(child1.OutputPartition.Count, child2.OutputPartition.Count);
                if (parCnt > 1)
                {
                    child1 = new DryadHashPartitionNode(keySelectExpr, comparerExpr, parCnt, queryExpr, child1);
                    if (IsMergeNodeNeeded(child1))
                    {
                        child1 = new DryadMergeNode(false, false, queryExpr, child1);
                    }

                    child2 = new DryadHashPartitionNode(keySelectExpr, comparerExpr, parCnt, queryExpr, child2);
                    if (IsMergeNodeNeeded(child2))
                    {
                        child2 = new DryadMergeNode(false, false, queryExpr, child2);
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
                    child2 = new DryadOrderByNode(keySelectExpr, comparerExpr, isDescending1, queryExpr, child2);
                }
                opName = "Ordered";
            }
            else if (child2.IsOrderedBy(keySelectExpr, comparer))
            {
                if (!child1.IsOrderedBy(keySelectExpr, comparer) ||
                    isDescending1 != isDescending2)
                {
                    // Sort outer if unsorted
                    child1 = new DryadOrderByNode(keySelectExpr, comparerExpr, isDescending2, queryExpr, child1);
                }
                opName = "Ordered";
            }

            resNode = new DryadSetOperationNode(nodeType, opName + nodeType, comparerExpr, queryExpr, child1, child2);
            return resNode;
        }

        private DryadQueryNode VisitContains(QueryNodeInfo source,
                                             Expression valueExpr,
                                             Expression comparerExpr,
                                             bool isQuery,
                                             Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = child.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
                                              string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable, keyType),
                                              queryExpr);
            }

            DryadQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DryadContainsNode(valueExpr, comparerExpr, queryExpr, x));
            resNode = new DryadBasicAggregateNode(null, AggregateOpType.Any, false, isQuery, queryExpr, resNode);
            return resNode;
        }

        private DryadQueryNode VisitQuantifier(QueryNodeInfo source,
                                               LambdaExpression lambda,
                                               AggregateOpType aggType,
                                               bool isQuery,
                                               Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            DryadQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DryadBasicAggregateNode(
                                                         lambda, aggType, true, isQuery, queryExpr, x));
            resNode = new DryadBasicAggregateNode(null, aggType, false, isQuery, queryExpr, resNode);
            return resNode;
        }

        private DryadQueryNode VisitAggregate(QueryNodeInfo source,
                                              Expression seed,
                                              LambdaExpression funcLambda,
                                              LambdaExpression resultLambda,
                                              bool isQuery,
                                              Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            DryadQueryNode resNode = child;
            if (HpcLinqExpression.IsAssociative(funcLambda))
            {
                LambdaExpression combinerLambda = HpcLinqExpression.GetAssociativeCombiner(funcLambda);
                ResourceAttribute attrib = AttributeSystem.GetResourceAttrib(funcLambda);
                bool funcIsExpensive = (attrib != null && attrib.IsExpensive); 
                resNode = this.PromoteConcat(
                                  source, child,
                                  delegate(DryadQueryNode x) {
                                      DryadQueryNode y = new DryadAggregateNode("AssocAggregate", seed,
                                                                                funcLambda, combinerLambda, resultLambda,
                                                                                1, isQuery, queryExpr, x, false);
                                      return new DryadAggregateNode("AssocTreeAggregate", seed,
                                                                    funcLambda, combinerLambda, resultLambda,
                                                                    2, isQuery, queryExpr, y, false);
                                  });
                resNode = new DryadAggregateNode("AssocAggregate", seed, funcLambda, combinerLambda, resultLambda,
                                                 3, isQuery, queryExpr, resNode, funcIsExpensive);
            }
            else
            {
                resNode = new DryadAggregateNode("Aggregate", seed, funcLambda, null, resultLambda, 3,
                                                 isQuery, queryExpr, child, false);
            }
            return resNode;
        }

        private DryadQueryNode VisitBasicAggregate(QueryNodeInfo source,
                                                   LambdaExpression lambda,
                                                   AggregateOpType aggType,
                                                   bool isQuery,
                                                   Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            if (aggType == AggregateOpType.Min || aggType == AggregateOpType.Max)
            {
                Type elemType = child.OutputTypes[0];
                if (lambda != null)
                {
                    elemType = lambda.Body.Type;
                }
                if (!TypeSystem.HasDefaultComparer(elemType))
                {
                    throw DryadLinqException.Create(HpcLinqErrorCode.AggregationOperatorRequiresIComparable,
                                                  String.Format(SR.AggregationOperatorRequiresIComparable, aggType ),
                                                  queryExpr);
                }
            }
            DryadQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DryadBasicAggregateNode(lambda, aggType, true, isQuery, queryExpr, x));
            
            switch (aggType)
            {
                case AggregateOpType.Count:
                case AggregateOpType.LongCount:
                {
                    resNode = new DryadBasicAggregateNode(null, AggregateOpType.Sum, false,
                                                          isQuery, queryExpr, resNode);
                    break;
                }
                case AggregateOpType.Sum:
                case AggregateOpType.Min:
                case AggregateOpType.Max:
                case AggregateOpType.Average:
                {
                    resNode = new DryadBasicAggregateNode(null, aggType, false,
                                                          isQuery, queryExpr, resNode);
                    break;
                }
                default:
                {
                    throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                                  String.Format(SR.OperatorNotSupported, aggType),
                                                  queryExpr);
                }
            }
            return resNode;
        }

        private DryadQueryNode VisitGroupBy(QueryNodeInfo source,
                                            LambdaExpression keySelectExpr,
                                            LambdaExpression elemSelectExpr,
                                            LambdaExpression resultSelectExpr,
                                            Expression comparerExpr,
                                            Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            
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
                child = new DryadSelectNode(QueryNodeType.Select, selectExpr, null, queryExpr, child);
                
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
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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
            DryadQueryNode groupByNode = child;
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
                    keyParam = Expression.Parameter(keyType, HpcLinqCodeGen.MakeUniqueName("k"));
                }
                else
                {
                    keyParam = resultSelectExpr.Parameters[0];
                }

                // Seed:
                ParameterExpression param2 = Expression.Parameter(
                                                 elemType, HpcLinqCodeGen.MakeUniqueName("e"));
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
                                                 seedBody.Type, HpcLinqCodeGen.MakeUniqueName("a"));
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
                                              HpcLinqCodeGen.MakeUniqueName("e"));
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
                groupByNode = new DryadGroupByNode(
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
                        groupByNode = new DryadOrderByNode(keySelectExpr, comparerExpr, true, queryExpr, child);
                    }
                    groupByOpName = "OrderedGroupBy";
                }

                // Add a GroupByNode if it is partitioned or has elementSelector.
                // If the input was already correctly partitioned, this will be the only groupByNode.
                if (isPartitioned)
                {
                    groupByNode = new DryadGroupByNode(groupByOpName,
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
                    groupByNode = new DryadGroupByNode(groupByOpName,
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
                    groupParam = Expression.Parameter(groupType, HpcLinqCodeGen.MakeUniqueName("g"));
                    if (resultSelectExpr == null)
                    {
                        // resultSelectExpr1: (k, g) => MakeDryadLinqGroup(k, g)
                        keyParam = Expression.Parameter(keySelectExpr1.Body.Type, HpcLinqCodeGen.MakeUniqueName("k"));
                        MethodInfo groupingInfo = typeof(HpcLinqEnumerable).GetMethod("MakeHpcLinqGroup");
                        groupingInfo = groupingInfo.MakeGenericMethod(keyParam.Type, elemType);
                        body = Expression.Call(groupingInfo, keyParam, groupParam);
                    }
                    else
                    {
                        // resultSelectExpr1: (k, g) => resultSelectExpr(k, FlattenGroups(g))
                        keyParam = resultSelectExpr.Parameters[0];
                        MethodInfo flattenInfo = typeof(HpcLinqEnumerable).GetMethod("FlattenGroups");
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
            DryadMergeNode mergeNode = null;
            DryadQueryNode groupByNode1 = groupByNode;
            if (!isPartitioned)
            {
                // Create HashPartitionNode, MergeNode, and second GroupByNode
                
                // Note, if we are doing decomposable-GroupByReduce, there is still some work to go after this
                //   - attach the combiner to the first merge-node
                //   - attach the combiner to the merge-node
                //   - attach finalizer to second GroupBy
                int parCount = (groupByNode.IsDynamic) ? StaticConfig.DefaultPartitionCount : groupByNode.OutputPartition.Count;
                bool isDynamic = (StaticConfig.DynamicOptLevel & StaticConfig.DynamicHashPartitionLevel) != 0;
                DryadQueryNode hdistNode = new DryadHashPartitionNode(keySelectExpr1, comparerExpr, parCount,
                                                                      isDynamic, queryExpr, groupByNode);

                // Create the Merge Node
                if (groupByOpName == "OrderedGroupBy")
                {
                    // Mergesort with the same keySelector of the hash partition 
                    mergeNode = new DryadMergeNode(keySelectExpr1, comparerExpr, true, false, queryExpr, hdistNode);
                }
                else
                {
                    // Random merge
                    mergeNode = new DryadMergeNode(false, false, queryExpr, hdistNode);
                }
                groupByNode1 = new DryadGroupByNode(groupByOpName, keySelectExpr1, elemSelectExpr1,
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
                                                                              HpcLinqCodeGen.MakeUniqueName("e"));
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
                groupByNode1 = new DryadSelectNode(QueryNodeType.Select, selectExpr, null, queryExpr, groupByNode1);
            }
            return groupByNode1;
        }

        // Creates an "auto-sampling range-partition sub-query"
        private DryadQueryNode CreateRangePartition(bool isDynamic,
                                                    LambdaExpression keySelectExpr,
                                                    LambdaExpression resultSelectExpr,
                                                    Expression comparerExpr,
                                                    Expression isDescendingExpr,
                                                    Expression queryExpr,
                                                    Expression partitionCountExpr,
                                                    DryadQueryNode child)
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
            
            ParameterExpression denvParam = Expression.Parameter(typeof(HpcLinqVertexEnv), "denv");

            MethodInfo minfo = typeof(HpcLinqSampler).GetMethod("Phase1Sampling");
            Expression body = Expression.Call(minfo.MakeGenericMethod(recordType, keyType),
                                              lambdaParam1, keySelectExpr, denvParam);
            Type type = typeof(Func<,>).MakeGenericType(lambdaParam1.Type, body.Type);
            LambdaExpression samplingExpr = Expression.Lambda(type, body, lambdaParam1);

            // Create the Sampling node
            DryadApplyNode samplingNode = new DryadApplyNode(samplingExpr, queryExpr, child);

            // Create x => RangeSampler(x, keySelectExpr, comparer, isDescendingExpr)
            Type lambdaParamType = typeof(IEnumerable<>).MakeGenericType(keyType);
            ParameterExpression lambdaParam = Expression.Parameter(lambdaParamType, "x_2");
            
            //For RTM, isDynamic should never be true.
            //string methodName = (isDynamic) ? "RangeSampler_Dynamic" : "RangeSampler_Static";
            Debug.Assert(isDynamic == false, "Internal error: isDynamic is true.");
            string methodName = "RangeSampler_Static";
            
            minfo = typeof(HpcLinqSampler).GetMethod(methodName);
            minfo = minfo.MakeGenericMethod(keyType);
            Expression comparerArgExpr = comparerExpr;
            if (comparerExpr == null)
            {
                if (!TypeSystem.HasDefaultComparer(keyType))
                {
                    throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
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
            DryadQueryNode sampleDataNode = new DryadMergeNode(false, true, queryExpr, samplingNode);
            DryadQueryNode sampleNode = new DryadApplyNode(samplerExpr, queryExpr, sampleDataNode);
            sampleNode.IsForked = true;

            // Create the range distribute node
            DryadQueryNode resNode = new DryadRangePartitionNode(keySelectExpr,
                                                                 resultSelectExpr,
                                                                 null,
                                                                 comparerExpr,
                                                                 isDescendingExpr,
                                                                 countExpr,
                                                                 queryExpr,
                                                                 child,
                                                                 sampleNode);
            resNode = new DryadMergeNode(false, true, queryExpr, resNode);

            // Set the dynamic manager for sampleNode
            if (isDynamic)
            {
                sampleDataNode.DynamicManager = new DynamicRangeDistributor(resNode);
            }

            return resNode;
        }
        
        private DryadQueryNode VisitOrderBy(QueryNodeInfo source,
                                            LambdaExpression keySelectExpr,
                                            Expression comparerExpr,
                                            bool isDescending,
                                            Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                              string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                              queryExpr);
            }

            DryadQueryNode resNode = child;
            if (child.OutputPartition.ParType == PartitionType.Range &&
                child.OutputPartition.IsPartitionedBy(keySelectExpr, comparerExpr, isDescending))
            {
                // Only need to sort each partition
                resNode = new DryadOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, child);
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
                    resNode = new DryadOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, resNode);
                }
                else
                {
                    if (child.OutputPartition.Count > 1)
                    {
                        resNode = this.CreateRangePartition(false, keySelectExpr, null, comparerExpr,
                                                            isDescendingExpr, queryExpr, null, child);
                    }
                    resNode = new DryadOrderByNode(keySelectExpr, comparerExpr, isDescending, queryExpr, resNode);
                }
            }
            return resNode;
        }

        private DryadQueryNode FirstStagePartitionOp(string opName,
                                                     QueryNodeType nodeType,
                                                     Expression controlExpr,
                                                     MethodCallExpression queryExpr,
                                                     DryadQueryNode child)
        {
            if (nodeType == QueryNodeType.TakeWhile)
            {
                Type ptype = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
                ParameterExpression param = Expression.Parameter(ptype, HpcLinqCodeGen.MakeUniqueName("x"));
                MethodInfo minfo = typeof(HpcLinqEnumerable).GetMethod("GroupTakeWhile");
                minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param, controlExpr);
                Type type = typeof(Func<,>).MakeGenericType(ptype, body.Type);
                LambdaExpression procFunc = Expression.Lambda(type, body, param);
                return new DryadApplyNode(procFunc, queryExpr, child);
            }
            else
            {
                return new DryadPartitionOpNode(opName, nodeType, controlExpr, true, queryExpr, child);
            }
        }
        
        private DryadQueryNode VisitPartitionOp(string opName,
                                                QueryNodeInfo source,
                                                QueryNodeType nodeType,
                                                Expression controlExpr,
                                                MethodCallExpression queryExpr)
        {
            DryadQueryNode resNode;
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
                    DryadQueryNode offsetNode = this.CreateOffset(isLong, queryExpr, resNode);

                    // Create (x, y) => GroupIndexedTakeWhile(x, y, controlExpr)
                    Type ptype1 = typeof(IEnumerable<>).MakeGenericType(resNode.OutputTypes[0]);
                    Type ptype2 = typeof(IEnumerable<>).MakeGenericType(typeof(IndexedValue<long>));
                    ParameterExpression param1 = Expression.Parameter(ptype1, HpcLinqCodeGen.MakeUniqueName("x"));
                    ParameterExpression param2 = Expression.Parameter(ptype2, HpcLinqCodeGen.MakeUniqueName("y"));
                    string methodName = "GroupIndexed" + queryExpr.Method.Name;
                    MethodInfo minfo = typeof(HpcLinqEnumerable).GetMethod(methodName);
                    minfo = minfo.MakeGenericMethod(resNode.OutputTypes[0]);
                    Expression body = Expression.Call(minfo, param1, param2, controlExpr);
                    Type type = typeof(Func<,,>).MakeGenericType(ptype1, ptype2, body.Type);
                    LambdaExpression procFunc = Expression.Lambda(type, body, param1, param2);
                    resNode = new DryadApplyNode(procFunc, queryExpr, resNode, offsetNode);
                }
            }
            else if (!source.IsForked &&
                     (nodeType == QueryNodeType.Take || nodeType == QueryNodeType.TakeWhile) &&
                     (source.OperatorName == "OrderBy" || source.OperatorName == "OrderByDescending"))
            {
                resNode = this.Visit(source.children[0].child);
                
                bool isDescending = (source.OperatorName == "OrderByDescending");
                MethodCallExpression sourceQueryExpr = (MethodCallExpression)source.queryExpression;
                LambdaExpression keySelectExpr = HpcLinqExpression.GetLambda(sourceQueryExpr.Arguments[1]);
                Expression comparerExpr = null;
                if (sourceQueryExpr.Arguments.Count == 3)
                {
                    comparerExpr = sourceQueryExpr.Arguments[2];
                }
                resNode = this.PromoteConcat(
                                  source.children[0].child,
                                  resNode,
                                  delegate(DryadQueryNode x) {
                                      DryadQueryNode y = new DryadOrderByNode(keySelectExpr, comparerExpr,
                                                                              isDescending, sourceQueryExpr, x);
                                      return FirstStagePartitionOp(opName, nodeType, controlExpr, queryExpr, y);
                                  });
                if (resNode.IsDynamic || resNode.OutputPartition.Count > 1)
                {
                    // Need a mergesort
                    resNode = new DryadMergeNode(keySelectExpr, comparerExpr, isDescending, false, sourceQueryExpr, resNode);
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
            resNode = new DryadPartitionOpNode(opName, nodeType, controlExpr, false, queryExpr, resNode);
            return resNode;
        }

        private DryadQueryNode VisitZip(QueryNodeInfo first,
                                        QueryNodeInfo second,
                                        LambdaExpression resultSelector,
                                        MethodCallExpression queryExpr)
        {
            DryadQueryNode child1 = this.Visit(first);
            DryadQueryNode child2 = this.Visit(second);

            if (child1.IsDynamic || child2.IsDynamic)
            {
                // Well, let us for now do it on a single machine
                child1 = new DryadMergeNode(true, false, queryExpr, child1);
                child2 = new DryadMergeNode(true, false, queryExpr, child2);

                // Apply node for (x, y) => Zip(x, y, resultSelector)
                Type paramType1 = typeof(IEnumerable<>).MakeGenericType(child1.OutputTypes[0]);
                ParameterExpression param1 = Expression.Parameter(paramType1, "s1");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child2.OutputTypes[0]);
                ParameterExpression param2 = Expression.Parameter(paramType2, "s2");
                MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("Zip");
                minfo = minfo.MakeGenericMethod(child1.OutputTypes[0]);
                Expression body = Expression.Call(minfo, param1, param2, resultSelector);
                Type funcType = typeof(Func<,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression procFunc = Expression.Lambda(funcType, body, param1, param2);
                return new DryadApplyNode(procFunc, queryExpr, child1, child2);
            }
            else
            {
                int parCount1 = child1.OutputPartition.Count;
                int parCount2 = child2.OutputPartition.Count;
                
                // Count nodes
                DryadQueryNode countNode1 = new DryadBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                        true, false, queryExpr, child1);
                DryadQueryNode countNode2 = new DryadBasicAggregateNode(null, AggregateOpType.LongCount,
                                                                        true, false, queryExpr, child2);
                countNode1 = new DryadMergeNode(true, false, queryExpr, countNode1);
                countNode2 = new DryadMergeNode(true, false, queryExpr, countNode2);

                // Apply node for (x, y) => ZipCount(x, y)
                Type paramType1 = typeof(IEnumerable<>).MakeGenericType(typeof(long));
                ParameterExpression param1 = Expression.Parameter(paramType1, "x");
                ParameterExpression param2 = Expression.Parameter(paramType1, "y");                
                MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("ZipCount");
                Expression body = Expression.Call(minfo, param1, param2);
                Type funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression zipCount = Expression.Lambda(funcType, body, param1, param2);
                DryadQueryNode indexedCountNode = new DryadApplyNode(zipCount, queryExpr, countNode1, countNode2);

                // HashPartition(x => x.index, parCount2)
                ParameterExpression param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                Expression keySelectBody = Expression.Property(param, "Index");                
                funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
                LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
                DryadQueryNode distCountNode = new DryadHashPartitionNode(keySelectExpr,
                                                                          null,
                                                                          parCount2,
                                                                          queryExpr,
                                                                          indexedCountNode);

                // Apply node for (x, y) => AssignPartitionIndex(x, y)
                param1 = Expression.Parameter(body.Type, "x");
                Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child2.OutputTypes[0]);
                param2 = Expression.Parameter(paramType2, "y");
                minfo = typeof(HpcLinqHelper).GetMethod("AssignPartitionIndex");
                minfo = minfo.MakeGenericMethod(child2.OutputTypes[0]);
                body = Expression.Call(minfo, param1, param2);
                funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
                LambdaExpression assignIndex = Expression.Lambda(funcType, body, param1, param2);
                DryadQueryNode addIndexNode = new DryadApplyNode(assignIndex, queryExpr, distCountNode, child2);

                // HashPartition(x => x.index, x => x.value, parCount1)
                param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
                body = Expression.Property(param, "Index");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                keySelectExpr = Expression.Lambda(funcType, body, param);
                body = Expression.Property(param, "Value");
                funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
                LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
                DryadQueryNode newChild2 = new DryadHashPartitionNode(keySelectExpr,
                                                                      resultSelectExpr,
                                                                      null,
                                                                      parCount1,
                                                                      false,
                                                                      queryExpr,
                                                                      addIndexNode);
                newChild2 = new DryadMergeNode(true, true, queryExpr, newChild2);

                // Finally the zip node
                return new DryadZipNode(resultSelector, queryExpr, child1, newChild2);
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
        private DryadQueryNode VisitReverse(QueryNodeInfo source, Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("Reverse is only supported for static partition count");
            }

            child.IsForked = true;

            // Apply node for s => ValueZero(s)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, "s");
            MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("ValueZero");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode valueZeroNode = new DryadApplyNode(procFunc, queryExpr, child);

            // Apply node for s => ReverseIndex(s)
            paramType = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            param = Expression.Parameter(paramType, "s");
            minfo = typeof(HpcLinqHelper).GetMethod("MakeIndexCountPairs");
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode mergeZeroNode = new DryadMergeNode(true, true, queryExpr, valueZeroNode); 
            DryadQueryNode indexCountNode = new DryadApplyNode(procFunc, queryExpr, mergeZeroNode);
            
            // HashPartition to distribute the indexCounts -- one to each partition.
            // each partition will receive (myPartitionID, pcount).
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            Expression keySelectBody = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
            DryadQueryNode hdistNode = new DryadHashPartitionNode(keySelectExpr,
                                                                  null,
                                                                  pcount,
                                                                  queryExpr,
                                                                  indexCountNode);

            // Apply node for (x, y) => AddIndexForReverse(x, y)
            ParameterExpression param1 = Expression.Parameter(body.Type, "x");
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param2 = Expression.Parameter(paramType2, "y");
            minfo = typeof(HpcLinqHelper).GetMethod("AddIndexForReverse");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param1, param2);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
            LambdaExpression addIndexFunc = Expression.Lambda(funcType, body, param1, param2);
            DryadQueryNode addIndexNode = new DryadApplyNode(addIndexFunc, queryExpr, hdistNode, child);

            // HashPartition(x => x.index, x => x.value, pcount)
            // Moves all data to correct target partition.  (each worker will direct all its items to one target partition)
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            body = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            keySelectExpr = Expression.Lambda(funcType, body, param);
            body = Expression.Property(param, "Value");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression resultSelectExpr = Expression.Lambda(funcType, body, param);
            DryadQueryNode reversePartitionNode = new DryadHashPartitionNode(
                                                            keySelectExpr, resultSelectExpr, null,
                                                            pcount, false, queryExpr, addIndexNode);

            // Reverse node
            paramType = typeof(IEnumerable<>).MakeGenericType(reversePartitionNode.OutputTypes[0]);
            param = Expression.Parameter(paramType, "x");
            minfo = typeof(HpcLinqVertex).GetMethod("Reverse");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode resNode = new DryadMergeNode(true, true, queryExpr, reversePartitionNode);
            resNode = new DryadApplyNode(procFunc, queryExpr, resNode);

            return resNode;
        }

        private DryadQueryNode VisitSequenceEqual(QueryNodeInfo source1,
                                                  QueryNodeInfo source2,
                                                  Expression comparerExpr,
                                                  Expression queryExpr)
        {
            DryadQueryNode child1 = this.Visit(source1);
            DryadQueryNode child2 = this.Visit(source2);

            Type elemType = child1.OutputTypes[0];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(elemType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable,
                                              String.Format(SR.ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable, elemType),
                                              queryExpr);
            }

            // Well, let us do it on a single machine for now
            child1 = new DryadMergeNode(true, false, queryExpr, child1);
            child2 = new DryadMergeNode(true, false, queryExpr, child2);

            // Apply node for (x, y) => SequenceEqual(x, y, c)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(elemType);
            ParameterExpression param1 = Expression.Parameter(paramType, "s1");
            ParameterExpression param2 = Expression.Parameter(paramType, "s2");
            MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("SequenceEqual");
            minfo = minfo.MakeGenericMethod(elemType);
            if (comparerExpr == null)
            {
                comparerExpr = Expression.Constant(null, typeof(IEqualityComparer<>).MakeGenericType(elemType));
            }
            Expression body = Expression.Call(minfo, param1, param2, comparerExpr);
            Type funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DryadApplyNode(procFunc, queryExpr, child1, child2);
        }

        private DryadQueryNode VisitHashPartition(QueryNodeInfo source,
                                                  LambdaExpression keySelectExpr,
                                                  Expression comparerExpr,
                                                  Expression countExpr,
                                                  Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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

            DryadQueryNode resNode = new DryadHashPartitionNode(
                                              keySelectExpr, null, comparerExpr, nOutputPartitions,
                                              isDynamic, queryExpr, child);
            resNode = new DryadMergeNode(false, true, queryExpr, resNode);
            return resNode;
        }

        private DryadQueryNode VisitRangePartition(QueryNodeInfo source,
                                                   LambdaExpression keySelectExpr,
                                                   Expression keysExpr,
                                                   Expression comparerExpr,
                                                   Expression isDescendingExpr,
                                                   Expression partitionCountExpr,
                                                   Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];

            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                              string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, keyType),
                                              queryExpr);
            }

            DryadQueryNode resNode;
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
                resNode = new DryadRangePartitionNode(keySelectExpr, null, keysExpr, comparerExpr,
                                                      isDescendingExpr, null, queryExpr, child);
                resNode = new DryadMergeNode(false, true, queryExpr, resNode);
            }
            return resNode;
        }

        private DryadQueryNode VisitMultiApply(QueryNodeInfo source,
                                               LambdaExpression procLambda,
                                               bool perPartition,
                                               bool isFirstOnly,
                                               MethodCallExpression queryExpr)
        {
            DryadQueryNode[] childs = new DryadQueryNode[source.children.Count];
            for (int i = 0; i < source.children.Count; ++i)
            {
                childs[i] = this.Visit(source.children[i].child);
            }

            bool isDynamic = childs.Any(x => x.IsDynamic);
            if (perPartition && !isDynamic)
            {
                // Homomorphic case.
                if (isFirstOnly)
                {
                    for (int i = 1; i < childs.Length; ++i)
                    {
                        childs[i] = new DryadTeeNode(childs[i].OutputTypes[0], true, queryExpr, childs[i]);
                        childs[i].ConOpType = ConnectionOpType.CrossProduct;
                        childs[i] = new DryadMergeNode(childs[0].OutputPartition.Count, queryExpr, childs[i]);
                    }
                }
                else
                {
                    int count = childs[0].OutputPartition.Count;
                    for (int i = 1; i < childs.Length; ++i)
                    {
                        if (childs[i].OutputPartition.Count != count)
                        {
                            throw DryadLinqException.Create(HpcLinqErrorCode.HomomorphicApplyNeedsSamePartitionCount,
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
                        childs[i] = new DryadMergeNode(true, false, queryExpr, childs[i]);
                    }
                }
            }
            DryadQueryNode applyNode = new DryadApplyNode(procLambda, true, queryExpr, childs);
            return applyNode;
        }

        private DryadQueryNode VisitApply(QueryNodeInfo source1,
                                          QueryNodeInfo source2,
                                          LambdaExpression procLambda,
                                          bool perPartition,
                                          bool isFirstOnly,
                                          Expression queryExpr)
        {
            DryadQueryNode child1 = this.Visit(source1);

            DryadQueryNode applyNode;
            if (source2 == null)
            {
                // Unary-apply case:
                if (perPartition)
                {
                    //homomorphic
                    applyNode = this.PromoteConcat(source1, child1, x => new DryadApplyNode(procLambda, queryExpr, x));
                }
                else
                {
                    //non-homomorphic
                    if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                    {
                        child1 = new DryadMergeNode(true, false, queryExpr, child1);
                    }
                    applyNode = new DryadApplyNode(procLambda, queryExpr, child1);
                }
            }
            else
            {
                // Binary-apply case:
                DryadQueryNode child2 = this.Visit(source2);

                if (perPartition && isFirstOnly)
                {
                    // The function is left homomorphic:
                    if (!child2.IsForked && (child1.IsDynamic || child1.OutputPartition.Count > 1))
                    {
                        // The normal cases..
                        if (IsMergeNodeNeeded(child2))
                        {
                            if (child1.IsDynamic)
                            {
                                child2 = new DryadMergeNode(true, false, queryExpr, child2);
                                child2.IsForked = true;
                            }
                            else
                            {
                                // Rather than do full merge and broadcast, which has lots of data-movement  
                                //   1. Tee output2 with output cross-product
                                //   2. Do a merge-stage which will have input1.nPartition nodes each performing a merge.
                                //  This acheives a distribution of the entire input2 to the Apply nodes with least data-movement.
                                child2 = new DryadTeeNode(child2.OutputTypes[0], true, queryExpr, child2);
                                child2.ConOpType = ConnectionOpType.CrossProduct;
                                child2 = new DryadMergeNode(child1.OutputPartition.Count, queryExpr, child2);
                            }
                        }
                        else
                        {
                            // the right-data is alread a single partition, so just tee it.
                            // this will provide a copy to each of the apply nodes.
                            child2 = new DryadTeeNode(child2.OutputTypes[0], true, queryExpr, child2);
                        }
                    }
                    else
                    {
                        // Less common cases..
                        // a full merge of the right-data may be necessary.
                        if (child2.IsDynamic || child2.OutputPartition.Count > 1)
                        {
                            child2 = new DryadMergeNode(true, false, queryExpr, child2);
                            if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                            {
                                child2.IsForked = true;
                            }
                        }
                    }
                    applyNode = new DryadApplyNode(procLambda, queryExpr, child1, child2);
                }
                else if (perPartition && !isFirstOnly && !child1.IsDynamic && !child2.IsDynamic)
                {
                    // Full homomorphic
                    // No merging occurs.
                    // NOTE: We generally expect that both the left and right datasets have matching partitionCount.
                    //       however, we don't test for it yet as users might know what they are doing, and it makes
                    //       LocalDebug inconsistent as LocalDebug doesn't throw in that situation.
                    applyNode = new DryadApplyNode(procLambda, queryExpr, child1, child2);
                }
                else
                {
                    // Non-homomorphic
                    // Full merges of both data sets is necessary.
                    if (child1.IsDynamic || child1.OutputPartition.Count > 1)
                    {
                        child1 = new DryadMergeNode(true, false, queryExpr, child1);
                    }
                    if (child2.IsDynamic || child2.OutputPartition.Count > 1)
                    {
                        child2 = new DryadMergeNode(true, false, queryExpr, child2);
                    }
                    applyNode = new DryadApplyNode(procLambda, queryExpr, child1, child2);
                }
            }
            return applyNode;
        }

        private DryadQueryNode VisitFork(QueryNodeInfo source,
                                         LambdaExpression forkLambda,
                                         Expression keysExpr,
                                         Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            return new DryadForkNode(forkLambda, keysExpr, queryExpr, child);
        }

        private DryadQueryNode VisitForkChoose(QueryNodeInfo source,
                                               Expression indexExpr,
                                               Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
            int index = evaluator.Eval(indexExpr);
            return ((DryadForkNode)child).Parents[index];
        }

        private DryadQueryNode VisitAssumeHashPartition(QueryNodeInfo source,
                                                        LambdaExpression keySelectExpr,
                                                        Expression keysExpr,
                                                        Expression comparerExpr,
                                                        Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultEqualityComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable,
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

        private DryadQueryNode VisitAssumeRangePartition(QueryNodeInfo source,
                                                         LambdaExpression keySelectExpr,
                                                         Expression keysExpr,
                                                         Expression comparerExpr,
                                                         Expression isDescendingExpr,
                                                         Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
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
                        HpcLinqErrorCode.BadSeparatorCount,
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

        private DryadQueryNode VisitAssumeOrderBy(QueryNodeInfo source,
                                                  LambdaExpression keySelectExpr,
                                                  Expression comparerExpr,
                                                  Expression isDescendingExpr,
                                                  Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);

            Type keyType = keySelectExpr.Type.GetGenericArguments()[1];
            if (comparerExpr == null && !TypeSystem.HasDefaultComparer(keyType))
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
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

        private DryadQueryNode VisitSlidingWindow(QueryNodeInfo source,
                                                  LambdaExpression procLambda,
                                                  Expression windowSizeExpr,
                                                  Expression queryExpr)
        {
            // var windows = source.Apply(s => HpcLinqHelper.Last(s, windowSize));
            // var slided = windows.Apply(s => HpcLinqHelper.Slide(s)).HashPartition(x => x.Index);
            // slided.Apply(source, (x, y) => HpcLinqHelper.ProcessWindows(x, y, procFunc, windowSize));
            DryadQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("SlidingWindow is only supported for static partition count");
            }

            ExpressionSimplifier<int> evaluator = new ExpressionSimplifier<int>();
            Expression windowSize = Expression.Constant(evaluator.Eval(windowSizeExpr), typeof(int));
            
            child.IsForked = true;

            // Apply node for s => Last(s, windowSize)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, HpcLinqCodeGen.MakeUniqueName("s"));
            MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("Last");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param, windowSize);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode lastNode = new DryadApplyNode(procFunc, queryExpr, child);
            lastNode = new DryadMergeNode(true, true, queryExpr, lastNode);

            // Apply node for s => Slide(s)
            param = Expression.Parameter(body.Type, HpcLinqCodeGen.MakeUniqueName("s"));
            minfo = typeof(HpcLinqHelper).GetMethod("Slide");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode slideNode = new DryadApplyNode(procFunc, queryExpr, lastNode);

            // Hash partition to distribute from partition i to i+1
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type.GetGenericArguments()[0], "x");
            Expression keySelectBody = Expression.Property(param, "Index");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, keySelectBody.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, keySelectBody, param);
            DryadQueryNode hdistNode = new DryadHashPartitionNode(keySelectExpr,
                                                                  null,
                                                                  pcount,
                                                                  queryExpr,
                                                                  slideNode);

            // Apply node for (x, y) => ProcessWindows(x, y, proclambda, windowSize)
            Type paramType1 = typeof(IEnumerable<>).MakeGenericType(body.Type);
            ParameterExpression param1 = Expression.Parameter(paramType1, HpcLinqCodeGen.MakeUniqueName("x"));
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param2 = Expression.Parameter(paramType2, HpcLinqCodeGen.MakeUniqueName("y"));
            minfo = typeof(HpcLinqHelper).GetMethod("ProcessWindows");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0], procLambda.Body.Type);
            body = Expression.Call(minfo, param1, param2, procLambda, windowSize);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DryadApplyNode(procFunc, queryExpr, hdistNode, child);
        }

        private DryadQueryNode VisitApplyWithPartitionIndex(
                                    QueryNodeInfo source,
                                    LambdaExpression procLambda,
                                    Expression queryExpr)
        {
            // var indices = source.Apply(s => ValueZero(s)).Apply(s => AssignIndex(s));
            // indices.Apply(source, (x, y) => ProcessWithIndex(x, y, procFunc));
            DryadQueryNode child = this.Visit(source);
            if (child.IsDynamic)
            {
                throw new DryadLinqException("ApplyWithPartitionIndex is only supported for static partition count");
            }

            child.IsForked = true;

            // Apply node for s => ValueZero(s)
            Type paramType = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param = Expression.Parameter(paramType, "s");
            MethodInfo minfo = typeof(HpcLinqHelper).GetMethod("ValueZero");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0]);
            Expression body = Expression.Call(minfo, param);
            Type funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);            
            LambdaExpression procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode valueZeroNode = new DryadApplyNode(procFunc, queryExpr, child);
            valueZeroNode = new DryadMergeNode(true, true, queryExpr, valueZeroNode); 

            // Apply node for s => AssignIndex(s)
            paramType = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            param = Expression.Parameter(paramType, "s");
            minfo = typeof(HpcLinqHelper).GetMethod("AssignIndex");
            body = Expression.Call(minfo, param);
            funcType = typeof(Func<,>).MakeGenericType(param.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param);
            DryadQueryNode assignIndexNode = new DryadApplyNode(procFunc, queryExpr, valueZeroNode);

            // HashPartition to distribute the indices -- one to each partition.
            int pcount = child.OutputPartition.Count;
            param = Expression.Parameter(body.Type, "x");
            funcType = typeof(Func<,>).MakeGenericType(param.Type, param.Type);
            LambdaExpression keySelectExpr = Expression.Lambda(funcType, param, param);
            DryadQueryNode hdistNode = new DryadHashPartitionNode(keySelectExpr,
                                                                  null,
                                                                  pcount,
                                                                  queryExpr,
                                                                  assignIndexNode);

            // Apply node for (x, y) => ProcessWithIndex(x, y, procLambda));
            Type paramType1 = typeof(IEnumerable<>).MakeGenericType(child.OutputTypes[0]);
            ParameterExpression param1 = Expression.Parameter(paramType1, HpcLinqCodeGen.MakeUniqueName("x"));
            Type paramType2 = typeof(IEnumerable<>).MakeGenericType(typeof(int));
            ParameterExpression param2 = Expression.Parameter(paramType2, HpcLinqCodeGen.MakeUniqueName("y"));
            minfo = typeof(HpcLinqHelper).GetMethod("ProcessWithIndex");
            minfo = minfo.MakeGenericMethod(child.OutputTypes[0], procLambda.Body.Type);
            body = Expression.Call(minfo, param1, param2, procLambda);
            funcType = typeof(Func<,,>).MakeGenericType(param1.Type, param2.Type, body.Type);
            procFunc = Expression.Lambda(funcType, body, param1, param2);
            return new DryadApplyNode(procFunc, queryExpr, child, hdistNode);
        }

        private DryadQueryNode VisitFirst(QueryNodeInfo source,
                                          LambdaExpression lambda,
                                          AggregateOpType aggType,
                                          bool isQuery,
                                          Expression queryExpr)
        {
            DryadQueryNode child = this.Visit(source);
            DryadQueryNode resNode = this.PromoteConcat(
                                             source, child,
                                             x => new DryadBasicAggregateNode(lambda, aggType, true, isQuery, queryExpr, x));
            return new DryadBasicAggregateNode(null, aggType, false, isQuery, queryExpr, resNode);
        }

        private DryadQueryNode VisitThenBy(QueryNodeInfo source,
                                           LambdaExpression keySelectExpr,
                                           bool isDescending,
                                           Expression queryExpr)
        {
            // YY: This makes it hard to maintain OrderByInfo.
            throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, "ThenBy"),
                                          queryExpr);
        }

        private DryadQueryNode VisitDefaultIfEmpty(QueryNodeInfo source,
                                                   Expression defaultValueExpr,
                                                   MethodCallExpression queryExpr)
        {
            // YY: Not very useful. We could add it later.
            throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, "DefaultIfEmpty"),
                                          queryExpr);
        }

        private DryadQueryNode VisitElementAt(string opName,
                                              QueryNodeInfo source,
                                              Expression indexExpr,
                                              Expression queryExpr)
        {
            // YY: Not very useful. We could add it later.
            throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, opName),
                                          queryExpr);
        }
                                              
        private DryadQueryNode VisitOfType(QueryNodeInfo source,
                                           Type ofType,
                                           Expression queryExpr)
        {
            // YY: Not very useful.
            throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                          String.Format(SR.OperatorNotSupported, "OfType"),
                                          queryExpr);
        }

        private DryadQueryNode VisitQueryOperatorCall(QueryNodeInfo nodeInfo)
        {
            DryadQueryNode resNode = nodeInfo.queryNode;
            if (resNode != null) return resNode;

            MethodCallExpression expression = (MethodCallExpression)nodeInfo.queryExpression;
            string methodName = expression.Method.Name;
            
            #region LINQMETHODS
            switch (methodName)
            {
                case "Aggregate":
                case "AggregateAsQuery":
                {
                    bool isQuery = (methodName == "AggregateAsQuery");
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression funcLambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitAggregate(nodeInfo.children[0].child,
                                                          null,
                                                          funcLambda,
                                                          null,
                                                          isQuery,
                                                          expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression funcLambda = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitAggregate(nodeInfo.children[0].child,
                                                          expression.Arguments[1],
                                                          funcLambda,
                                                          null,
                                                          isQuery,
                                                          expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression funcLambda = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression resultLambda = HpcLinqExpression.GetLambda(expression.Arguments[3]);
                        if (funcLambda != null && funcLambda.Parameters.Count == 2 &&
                            resultLambda != null && resultLambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAggregate(nodeInfo.children[0].child,
                                                          expression.Arguments[1],
                                                          funcLambda,
                                                          resultLambda,
                                                          isQuery,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitSelect(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count <= 2)
                        {
                            resNode = this.VisitSelect(nodeInfo.children[0].child,
                                                       QueryNodeType.SelectMany,
                                                       lambda,
                                                       null,
                                                       expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda1 = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda1 != null && (lambda1.Parameters.Count == 1 || lambda1.Parameters.Count == 2) &&
                            lambda2 != null && lambda2.Parameters.Count == 2)
                        {
                            resNode = this.VisitSelect(nodeInfo.children[0].child,
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
                        LambdaExpression lambda2 = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = HpcLinqExpression.GetLambda(expression.Arguments[3]);
                        LambdaExpression lambda4 = HpcLinqExpression.GetLambda(expression.Arguments[4]);
                        if (lambda2 != null && lambda2.Parameters.Count == 1 &&
                            lambda3 != null && lambda3.Parameters.Count == 1 &&
                            lambda4 != null && lambda4.Parameters.Count == 2)
                        {
                            resNode = this.VisitJoin(nodeInfo.children[0].child,
                                                     nodeInfo.children[1].child,
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
                        LambdaExpression lambda2 = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = HpcLinqExpression.GetLambda(expression.Arguments[3]);
                        LambdaExpression lambda4 = HpcLinqExpression.GetLambda(expression.Arguments[4]);
                        if (lambda2 != null && lambda2.Parameters.Count == 1 &&
                            lambda3 != null && lambda3.Parameters.Count == 1 &&
                            lambda4 != null && lambda4.Parameters.Count == 2)
                        {
                            resNode = this.VisitJoin(nodeInfo.children[0].child,
                                                     nodeInfo.children[1].child,
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
                        resNode = this.VisitOfType(nodeInfo.children[0].child, ofType, expression);
                    }
                    break;
                }
                case "Where":
                case "LongWhere":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitWhere(nodeInfo.children[0].child,
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
                        resNode = this.VisitFirst(nodeInfo.children[0].child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.children[0].child,
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
                        resNode = this.VisitFirst(nodeInfo.children[0].child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.children[0].child,
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
                        resNode = this.VisitFirst(nodeInfo.children[0].child, null, aggType, isQuery, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFirst(nodeInfo.children[0].child,
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
                        resNode = this.VisitDistinct(nodeInfo.children[0].child, null, expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitDistinct(nodeInfo.children[0].child,
                                                     expression.Arguments[1],
                                                     expression);
                    }
                    break;
                }
                case "DefaultIfEmpty":
                {
                    if (expression.Arguments.Count == 1)
                    {
                        resNode = this.VisitDefaultIfEmpty(nodeInfo.children[0].child,
                                                           null,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitDefaultIfEmpty(nodeInfo.children[0].child,
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
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
                                                         QueryNodeType.Union,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
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
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
                                                         QueryNodeType.Intersect,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
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
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
                                                         QueryNodeType.Except,
                                                         null,
                                                         expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSetOperation(nodeInfo.children[0].child,
                                                         nodeInfo.children[1].child,
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
                        resNode = this.VisitQuantifier(nodeInfo.children[0].child,
                                                       lambda,
                                                       AggregateOpType.Any,
                                                       isQuery,
                                                       expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitQuantifier(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitQuantifier(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.Count,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.LongCount,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.Sum,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.Min,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.Max,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
                                                           null,
                                                           AggregateOpType.Average,
                                                           isQuery,
                                                           expression);
                    }
                    else if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitBasicAggregate(nodeInfo.children[0].child,
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
                        LambdaExpression keySelExpr = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (keySelExpr != null && keySelExpr.Parameters.Count == 1)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                        LambdaExpression keySelExpr = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        if (keySelExpr != null && lambda2 == null)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                        LambdaExpression keySelExpr = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression lambda2 = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression lambda3 = HpcLinqExpression.GetLambda(expression.Arguments[3]);
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
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                        LambdaExpression keySelExpr = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        LambdaExpression elemSelExpr = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        LambdaExpression resSelExpr = HpcLinqExpression.GetLambda(expression.Arguments[3]);
                        if (keySelExpr != null && keySelExpr.Parameters.Count == 1 &&
                            elemSelExpr != null && elemSelExpr.Parameters.Count == 1 &&
                            resSelExpr != null && resSelExpr.Parameters.Count == 2)
                        {
                            resNode = this.VisitGroupBy(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitOrderBy(nodeInfo.children[0].child,
                                                        lambda,
                                                        null,
                                                        isDescending,
                                                        expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitOrderBy(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitThenBy(nodeInfo.children[0].child,
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
                                                      nodeInfo.children[0].child,
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
                                                        nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitPartitionOp("TakeWhile",
                                                            nodeInfo.children[0].child,
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
                                                        nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitPartitionOp(methodName,
                                                            nodeInfo.children[0].child,
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
                        resNode = this.VisitContains(nodeInfo.children[0].child,
                                                     expression.Arguments[1],
                                                     null,
                                                     isQuery,
                                                     expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitContains(nodeInfo.children[0].child,
                                                     expression.Arguments[1],
                                                     expression.Arguments[2],
                                                     isQuery,
                                                     expression);
                    }
                    break;
                }
                case "Reverse":
                {
                    resNode = this.VisitReverse(nodeInfo.children[0].child, expression);
                    break;
                }
                case "SequenceEqual":
                case "SequenceEqualAsQuery":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        resNode = this.VisitSequenceEqual(nodeInfo.children[0].child,
                                                          nodeInfo.children[1].child,
                                                          null,
                                                          expression);
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        resNode = this.VisitSequenceEqual(nodeInfo.children[0].child,
                                                          nodeInfo.children[1].child,
                                                          expression.Arguments[2],
                                                          expression);
                    }
                    break;
                }
                case "Zip":
                {
                    if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda != null && lambda.Parameters.Count == 2)
                        {
                            resNode = this.VisitZip(nodeInfo.children[0].child,
                                                    nodeInfo.children[1].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitHashPartition(nodeInfo.children[0].child,
                                                              lambda,
                                                              null,
                                                              null,
                                                              expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(int))
                            {
                                resNode = this.VisitHashPartition(nodeInfo.children[0].child,
                                                                  lambda,
                                                                  null,
                                                                  expression.Arguments[2],
                                                                  expression);
                            }
                            else
                            {
                                resNode = this.VisitHashPartition(nodeInfo.children[0].child,
                                                                  lambda,
                                                                  expression.Arguments[2],
                                                                  null,
                                                                  expression);
                            }
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitHashPartition(nodeInfo.children[0].child,
                                                              lambda,
                                                              expression.Arguments[2],
                                                              expression.Arguments[3],
                                                              expression);
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        resNode = this.VisitRangePartition(nodeInfo.children[0].child,
                                                           lambda,
                                                           null,
                                                           null,
                                                           null,
                                                           null,
                                                           expression);
                    }

                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(int))
                            {
                                // Case: (source, keySelector, pcount)
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type == typeof(bool))
                            {
                                //case: (source, keySelector, isDescending, pcount) 
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                                
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[3].Type == typeof(bool))
                            {
                                // case: (source, keySelector, keyComparer, isDescending, pcount)
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                                resNode = this.VisitRangePartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitApply(nodeInfo.children[0].child,
                                                      null,
                                                      lambda,
                                                      false,
                                                      false,
                                                      expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        if (lambda != null)
                        {
                            if (lambda.Parameters.Count == 2)
                            {
                                resNode = this.VisitApply(nodeInfo.children[0].child,
                                                          nodeInfo.children[1].child,
                                                          lambda,
                                                          false,
                                                          false,
                                                          expression);
                            }
                            else
                            {
                                // Apply with multiple sources of the same type
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null &&
                            (lambda.Parameters.Count == 1 || lambda.Parameters.Count == 2))
                        {
                            resNode = this.VisitApply(nodeInfo.children[0].child,
                                                      null,
                                                      lambda,
                                                      true,
                                                      false,
                                                      expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[2]);
                        ExpressionSimplifier<bool> evaluator = new ExpressionSimplifier<bool>();
                        bool isFirstOnly = evaluator.Eval(expression.Arguments[3]);
                        if (lambda != null)
                        {
                            if (lambda.Parameters.Count == 2)
                            {
                                resNode = this.VisitApply(nodeInfo.children[0].child,
                                                          nodeInfo.children[1].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFork(nodeInfo.children[0].child,
                                                     lambda,
                                                     null,
                                                     expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3) // ForkByKey
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitFork(nodeInfo.children[0].child,
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
                        resNode = this.VisitForkChoose(nodeInfo.children[0].child,
                                                       expression.Arguments[1],
                                                       expression);
                    }
                    break;
                }
                case "AssumeHashPartition":
                {
                    if (expression.Arguments.Count == 2)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeHashPartition(nodeInfo.children[0].child,
                                                                    lambda,
                                                                    null,
                                                                    null,
                                                                    expression);
                        }
                    }
                    else if (expression.Arguments.Count == 3)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeHashPartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type.IsArray)
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.children[0].child,
                                                                         lambda,
                                                                         expression.Arguments[2],
                                                                         null,
                                                                         null,
                                                                         expression);
                            }
                            else
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            if (expression.Arguments[2].Type.IsArray)
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.children[0].child,
                                                                         lambda,
                                                                         expression.Arguments[2],
                                                                         expression.Arguments[3],
                                                                         null,
                                                                         expression);
                            }
                            else
                            {
                                resNode = this.VisitAssumeRangePartition(nodeInfo.children[0].child,
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
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeOrderBy(nodeInfo.children[0].child,
                                                              lambda,
                                                              null,
                                                              expression.Arguments[2],
                                                              expression);
                        }
                    }
                    else if (expression.Arguments.Count == 4)
                    {
                        LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                        if (lambda != null && lambda.Parameters.Count == 1)
                        {
                            resNode = this.VisitAssumeOrderBy(nodeInfo.children[0].child,
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
                    LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                    if (lambda != null && lambda.Parameters.Count == 1)
                    {
                        resNode = this.VisitSlidingWindow(nodeInfo.children[0].child,
                                                          lambda,
                                                          expression.Arguments[2],
                                                          expression);
                    }
                    break;
                }
                case "SelectWithPartitionIndex":
                case "ApplyWithPartitionIndex":
                {
                    LambdaExpression lambda = HpcLinqExpression.GetLambda(expression.Arguments[1]);
                    if (lambda != null && lambda.Parameters.Count == 1)
                    {
                        resNode = this.VisitApplyWithPartitionIndex(nodeInfo.children[0].child,
                                                                    lambda,
                                                                    expression);
                    }
                    break;
                }
                case ReflectedNames.DryadLinqIQueryable_ToDscWorker: // was case "ToPartitionedTableLazy":
                {
                    //SHOULD NOT VISIT.. The DryadQueryGen ctors should be interrogating ToDsc nodes directly.
                    //Later if we do allow ToDsc in the middle of query chain, then we need to either
                    //    1. update the source node with an outputeTableUri
                    // OR 2. create an actual node and handle it later on  (tee, etc)

                    throw DryadLinqException.Create(HpcLinqErrorCode.ToDscUsedIncorrectly,
                                                  String.Format(SR.ToDscUsedIncorrectly),
                                                  expression);
                }
                case ReflectedNames.DryadLinqIQueryable_ToHdfsWorker: // was case "ToPartitionedTableLazy":
                {
                    //SHOULD NOT VISIT.. The DryadQueryGen ctors should be interrogating ToDsc nodes directly.
                    //Later if we do allow ToDsc in the middle of query chain, then we need to either
                    //    1. update the source node with an outputeTableUri
                    // OR 2. create an actual node and handle it later on  (tee, etc)

                    throw DryadLinqException.Create(HpcLinqErrorCode.ToHdfsUsedIncorrectly, String.Format(SR.ToHdfsUsedIncorrectly), expression);
                }
            }
            #endregion

            if (resNode == null)
            {
                throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                              String.Format(SR.OperatorNotSupported, methodName),
                                              expression);
            }
            resNode.IsForked = resNode.IsForked || nodeInfo.IsForked;
            nodeInfo.queryNode = resNode;
            return resNode;
        }
    }
}
