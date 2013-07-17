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
using System.Text;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml;
using System.Diagnostics;
using System.Xml.Linq;
using System.Drawing;
using System.Drawing.Drawing2D;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    //@@TODO[P1]: update for new APIs.

    /// <summary>
    /// This class explains in detail the generated plan.
    /// </summary>
    internal sealed class DryadQueryExplain
    {
        /// <summary>
        /// Visit the set of nodes in the query plan and build an explanation of the plan.
        /// </summary>
        /// <param name="plan">Return plan description here.</param>
        /// <param name="nodes">Nodes to explain.</param>
        internal void CodeShowVisit(StringBuilder plan, DryadQueryNode[] nodes)
        {
            HashSet<DryadQueryNode> visited = new HashSet<DryadQueryNode>();
            foreach (DryadQueryNode n in nodes)
            {
                CodeShowVisit(plan, n, visited);
            }
        }

        /// <summary>
        /// Helper for CodeShowVisit: do not revisit a node twice.
        /// </summary>
        /// <param name="plan">Return plan here.</param>
        /// <param name="n">Node to explain.</param>
        /// <param name="visited">Set of nodes already visited.</param>
        private void CodeShowVisit(StringBuilder plan, DryadQueryNode n, HashSet<DryadQueryNode> visited)
        {
            if (visited.Contains(n)) return;
            visited.Add(n);

            foreach (DryadQueryNode c in n.Children)
            {
                CodeShowVisit(plan, c, visited);
            }

            ExplainNode(plan, n);
        }

        /// <summary>
        /// Explain one query node.
        /// </summary>
        /// <param name="plan">Return plan here.</param>
        /// <param name="n">Node to explain.</param>
        internal static void ExplainNode(StringBuilder plan, DryadQueryNode n)
        {
            if (n is DryadTeeNode || n is DryadOutputNode)
            {
                return;
            }
            else if (n is DryadInputNode)
            {
                plan.AppendLine("Input:");
                plan.Append("\t");
                n.BuildString(plan);
                plan.AppendLine();
                return;
            }

            plan.Append(n.m_vertexEntryMethod);
            plan.AppendLine(":");

            HashSet<DryadQueryNode> allchildren = new HashSet<DryadQueryNode>();

            if (n is DryadSuperNode)
            {
                DryadSuperNode sn = n as DryadSuperNode;
                List<DryadQueryNode> tovisit = new List<DryadQueryNode>();

                tovisit.Add(sn.RootNode);

                while (tovisit.Count > 0)
                {
                    DryadQueryNode t = tovisit[0];
                    tovisit.RemoveAt(0);
                    if (!(t is DryadSuperNode))
                        allchildren.Add(t);
                    foreach (DryadQueryNode tc in t.Children)
                    {
                        if (!allchildren.Contains(tc) && sn.Contains(tc))
                            tovisit.Add(tc);
                    }
                }
            }
            else
                allchildren.Add(n);

            foreach (DryadQueryNode nc in allchildren.Reverse())
            {
                Expression expression = null; // expression to print
                List<string> additional = new List<string>(); // additional arguments to print
                int argsToSkip = 0;
                string methodname = nc.OpName;

                plan.Append("\t");

                if (nc is DryadMergeNode)
                {
                    expression = ((DryadMergeNode)nc).ComparerExpression;
                }
                else if (nc is DryadHashPartitionNode)
                {
                    DryadHashPartitionNode hp = (DryadHashPartitionNode)nc;
                    expression = hp.KeySelectExpression;
                    additional.Add(hp.NumberOfPartitions.ToString());
                }
                else if (nc is DryadGroupByNode)
                {
                    DryadGroupByNode gb = (DryadGroupByNode)nc;
                    expression = gb.KeySelectExpression;
                    if (gb.ElemSelectExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(gb.ElemSelectExpression));
                    if (gb.ResSelectExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(gb.ResSelectExpression));
                    if (gb.ComparerExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(gb.ComparerExpression));
                    if (gb.SeedExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(gb.SeedExpression));
                    if (gb.AccumulatorExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(gb.AccumulatorExpression));
                }
                else if (nc is DryadOrderByNode)
                {
                    DryadOrderByNode ob = (DryadOrderByNode)nc;
                    expression = ob.KeySelectExpression;
                    if (ob.ComparerExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(ob.ComparerExpression));
                }
                else if (nc is DryadWhereNode) {
                    expression = ((DryadWhereNode)nc).WhereExpression;
                }
                else if (nc is DryadSelectNode) {
                    DryadSelectNode s = (DryadSelectNode)nc;
                    expression = s.SelectExpression;
                    if (s.ResultSelectExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(s.ResultSelectExpression));
                }
                else if (nc is DryadAggregateNode)
                {
                    DryadAggregateNode a = (DryadAggregateNode)nc;
                    expression = a.FuncLambda;
                    if (a.SeedExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(a.SeedExpression));
                    if (a.ResultLambda != null)
                        additional.Add(HpcLinqExpression.Summarize(a.ResultLambda));
                }
                else if (nc is DryadPartitionOpNode) {
                    expression = ((DryadPartitionOpNode)nc).ControlExpression;
                }
                else if (nc is DryadJoinNode)
                {
                    DryadJoinNode j = (DryadJoinNode)nc;
                    expression = j.OuterKeySelectorExpression;
                    additional.Add(HpcLinqExpression.Summarize(j.InnerKeySelectorExpression));
                    additional.Add(HpcLinqExpression.Summarize(j.ResultSelectorExpression));
                    if (j.ComparerExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(j.ComparerExpression));
                }
                else if (nc is DryadDistinctNode)
                {
                    expression = ((DryadDistinctNode)nc).ComparerExpression;
                }
                else if (nc is DryadContainsNode)
                {
                    DryadContainsNode c = (DryadContainsNode)nc;
                    expression = c.ValueExpression;
                    if (c.ComparerExpression != null)
                        additional.Add(HpcLinqExpression.Summarize(c.ComparerExpression));
                }
                else if (nc is DryadBasicAggregateNode)
                {
                    expression = ((DryadBasicAggregateNode)nc).SelectExpression;
                }
                else if (nc is DryadConcatNode)
                    // nothing to do
                {
                }
                else if (nc is DryadSetOperationNode) 
                {
                    expression = ((DryadSetOperationNode)nc).ComparerExpression;
                }
                else if (nc is DryadRangePartitionNode)
                {   
                    DryadRangePartitionNode r = (DryadRangePartitionNode)nc;
                    expression = r.CountExpression;
                    // TODO: there's some other possible interesting info
                }
                else if (nc is DryadApplyNode)
                {
                    expression = ((DryadApplyNode)nc).LambdaExpression;
                }

                else if (nc is DryadForkNode)
                {
                    expression = ((DryadForkNode)nc).ForkLambda;
                }
                
                else if (nc is DryadTeeNode)
                {
                    // nothing
                }
                else if (nc is DryadDynamicNode)
                {
                    // nothing
                }
                else
                {
                    expression = nc.QueryExpression;
                }

                if (expression is MethodCallExpression)
                {
                    MethodCallExpression mc = (MethodCallExpression)expression;
                    methodname = mc.Method.Name;  // overwrite methodname

                    // determine which arguments to skip
                    #region LINQMETHODS
                    switch (mc.Method.Name)
                    {
                        case "Aggregate":
                        case "AggregateAsQuery":
                        case "Select":
                        case "LongSelect":
                        case "SelectMany":
                        case "LongSelectMany":
                        case "OfType":
                        case "Where":
                        case "LongWhere":
                        case "First":
                        case "FirstOrDefault":
                        case "FirstAsQuery":
                        case "Single":
                        case "SingleOrDefault":
                        case "SingleAsQuery":
                        case "Last":
                        case "LastOrDefault":
                        case "LastAsQuery":
                        case "Distinct":
                        case "Any":
                        case "AnyAsQuery":
                        case "All":
                        case "AllAsQuery":
                        case "Count":
                        case "CountAsQuery":
                        case "LongCount":
                        case "LongCountAsQuery":
                        case "Sum":
                        case "SumAsQuery":
                        case "Min":
                        case "MinAsQuery":
                        case "Max":
                        case "MaxAsQuery":
                        case "Average":
                        case "AverageAsQuery":
                        case "GroupBy":
                        case "OrderBy":
                        case "OrderByDescending":
                        case "ThenBy":
                        case "ThenByDescending":
                        case "Take":
                        case "TakeWhile":
                        case "LongTakeWhile":
                        case "Skip":
                        case "SkipWhile":
                        case "LongSkipWhile":
                        case "Contains":
                        case "ContainsAsQuery":
                        case "Reverse":
                        case "Merge":
                        case "HashPartition":
                        case "RangePartition":
                        case "Fork":
                        case "ForkChoose":
                        case "AssumeHashPartition":
                        case "AssumeRangePartition":
                        case "AssumeOrderBy":
                        case "ToPartitionedTableLazy":
                        case "AddCacheEntry":
                        case "SlidingWindow":
                        case "SelectWithPartitionIndex":
                        case "ApplyWithPartitionIndex":
                            argsToSkip = 1;
                            break;
                        case "Join":
                        case "GroupJoin":
                        case "Concat":
                        case "MultiConcat":
                        case "Union":
                        case "Intersect":
                        case "Except":
                        case "SequenceEqual":
                        case "SequenceEqualAsQuery":
                        case "Zip":
                            argsToSkip = 2;
                            break;
                        case "Apply":
                        case "ApplyPerPartition":
                            if (mc.Arguments.Count < 3)
                                argsToSkip = 1;
                            else
                                argsToSkip = 2;
                            break;
                        default:
                            throw DryadLinqException.Create(HpcLinqErrorCode.OperatorNotSupported,
                                                          String.Format(SR.OperatorNotSupported, mc.Method.Name),
                                                          expression);
                    }
                    #endregion

                    plan.Append(methodname);
                    plan.Append("(");

                    int argno = 0;
                    foreach (var arg in mc.Arguments)
                    {
                        argno++;
                        if (argno <= argsToSkip) continue;
                        if (argno > argsToSkip + 1)
                        {
                            plan.Append(",");
                        }
                        plan.Append(HpcLinqExpression.Summarize(arg));
                    }
                    plan.AppendLine(")");
                }
                else
                {
                    // expression is not methodcall
                    plan.Append(methodname);
                    plan.Append("(");
                    if (expression != null)
                    {
                        plan.Append(HpcLinqExpression.Summarize(expression));
                    }
                    foreach (string e in additional)
                    {
                        plan.Append(",");
                        plan.Append(e);
                    }
                    plan.AppendLine(")");
                }
            }
        }

        /// <summary>
        /// Explain a query plan in terms of elementary operations.
        /// </summary>
        /// <param name="gen">Query generator.</param>
        /// <returns>A string explaining the plan.</returns>
        internal string Explain(HpcLinqQueryGen gen)
        {
            StringBuilder plan = new StringBuilder();
            gen.CodeGenVisit();
            this.CodeShowVisit(plan, gen.QueryPlan());
            return plan.ToString();
        }
    }

    /// <summary>
    /// Summary information about a job query plan.
    /// </summary>
    internal class DryadLinqJobStaticPlan
    {
        /// <summary>
        /// Connection between two stages.
        /// </summary>
        public class Connection
        {
            /// <summary>
            /// Arity of connection.
            /// </summary>
            public enum ConnectionType
            {
                /// <summary>
                /// Point-to-point connection between two stages.
                /// </summary>
                PointToPoint,
                /// <summary>
                /// Cross-product connection between two stages.
                /// </summary>
                AllToAll
            };

            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public enum ChannelType
            {
                /// <summary>
                /// Persistent file.
                /// </summary>
                DiskFile,
                /// <summary>
                /// In-memory fifo.
                /// </summary>
                Fifo,
                /// <summary>
                /// TCP pipe.
                /// </summary>
                TCP
            }

            /// <summary>
            /// Stage originating the connection.
            /// </summary>
            public Stage From { internal set; get; }
            /// <summary>
            /// Stage terminating the connection.
            /// </summary>
            public Stage To { internal set; get; }
            /// <summary>
            /// Type of connection.
            /// </summary>
            public ConnectionType Arity { get; internal set; }
            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public ChannelType ChannelKind { get; internal set; }
            /// <summary>
            /// Dynamic manager associated with the connection.
            /// </summary>
            public string ConnectionManager { get; internal set; }

            /// <summary>
            /// Color used to represent the connection.
            /// </summary>
            /// <returns>A string describing the color.</returns>
            public string Color()
            {
                switch (this.ChannelKind)
                {
                    case ChannelType.DiskFile:
                        return "black";
                    case ChannelType.Fifo:
                        return "red";
                    case ChannelType.TCP:
                        return "yellow";
                    default:
                        throw new Exception(String.Format(SR.UnknownChannelType, this.ChannelKind.ToString()));
                }
            }
        }

        /// <summary>
        /// Per-node connection information (should be per-edge...)
        /// </summary>
        struct ConnectionInformation
        {
            /// <summary>
            /// Type of connection.
            /// </summary>
            public Connection.ConnectionType Arity { get; internal set; }
            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public Connection.ChannelType ChannelKind { get; internal set; }
            /// <summary>
            /// Dynamic manager associated with the connection.
            /// </summary>
            public string ConnectionManager { get; internal set; }
        }

        /// <summary>
        /// Information about a stage.
        /// </summary>
        public class Stage
        {
            /// <summary>
            /// Stage name.
            /// </summary>
            public string Name { get; internal set; }
            /// <summary>
            /// Code executed in the stage.
            /// </summary>
            public string[] Code { get; internal set; }
            /// <summary>
            /// DryadLINQ operator implemented by the stage.
            /// </summary>
            public string Operator { get; internal set; }
            /// <summary>
            /// Number of vertices in stage.
            /// </summary>
            public int Replication { get; internal set; }
            /// <summary>
            /// Unique identifier.
            /// </summary>
            public int Id { get; set; }

            /// <summary>
            /// True if the stage is an input.
            /// </summary>
            public bool IsInput { get; internal set; }
            /// <summary>
            /// True if the stage is an output.
            /// </summary>
            public bool IsOutput { get; internal set; }
            /// <summary>
            /// True if the stage is a tee.
            /// </summary>
            public bool IsTee { get; internal set; }
            /// <summary>
            /// True if the stage is a concatenation.
            /// </summary>
            public bool IsConcat { get; internal set; }
            /// <summary>
            /// True if the stage is virtual (no real vertices synthesized).
            /// </summary>
            public bool IsVirtual { get { return this.IsInput || this.IsOutput || this.IsTee || this.IsConcat; } }
            /// <summary>
            /// Only defined for tables.
            /// </summary>
            public string Uri { get; internal set; }
            /// <summary>
            /// Only defined for tables.
            /// </summary>
            public string UriType { get; internal set; }
        }

        /// <summary>
        /// File containing the plan.
        /// </summary>
        string xmlPlanFile;
        /// <summary>
        /// Map from stage id to stage.
        /// </summary>
        Dictionary<int, Stage> stages;
        /// <summary>
        /// List of inter-stage connections in the plan.
        /// </summary>
        List<Connection> connections;
        /// <summary>
        /// Store here per-node connection information (map from node id).
        /// </summary>
        Dictionary<int, ConnectionInformation> perNodeConnectionInfo;

        /// <summary>
        /// Create a dryadlinq job plan starting from an xml plan file.
        /// </summary>
        /// <param name="xmlPlanFile">Plan file to parse.</param>
        public DryadLinqJobStaticPlan(string xmlPlanFile)
        {
            this.stages = new Dictionary<int, Stage>();
            this.connections = new List<Connection>();
            this.perNodeConnectionInfo = new Dictionary<int, ConnectionInformation>();
            this.xmlPlanFile = xmlPlanFile;
            this.ParseQueryPlan();
        }

        /// <summary>
        /// Parse an XML query plan and represent that information.
        /// </summary>
        private void ParseQueryPlan()
        {
            if (!File.Exists(this.xmlPlanFile))
                throw new Exception(String.Format( SR.CannotReadQueryPlan , this.xmlPlanFile));

            XDocument plan = XDocument.Load(this.xmlPlanFile);
            XElement query = plan.Root.Elements().Where(e => e.Name == "QueryPlan").First();
            IEnumerable<XElement> vertices = query.Elements().Where(e => e.Name == "Vertex");

            foreach (XElement v in vertices)
            {
                Stage stage = new Stage();
                stage.Id = int.Parse(v.Element("UniqueId").Value);
                stage.Replication = int.Parse(v.Element("Partitions").Value);
                stage.Operator = v.Element("Type").Value;
                stage.Name = v.Element("Name").Value;
                {
                    string code = v.Element("Explain").Value;
                    stage.Code = code.Split('\n').
                        Skip(1). // drop stage name
                        Select(l => l.Trim()). // remove leading tab
                        ToArray();
                }
                this.stages.Add(stage.Id, stage);

                {
                    // These should be connection attributes, not stage attributes.
                    string cht = v.Element("ChannelType").Value;
                    string connectionManager = v.Element("DynamicManager").Element("Type").Value;
                    string connection = v.Element("ConnectionOperator").Value;
                    ConnectionInformation info = new ConnectionInformation();
                    info.ConnectionManager = connectionManager;
                    switch (connection)
                    {
                        case "Pointwise":
                            info.Arity = Connection.ConnectionType.PointToPoint;
                            break;
                        case "CrossProduct":
                            info.Arity = Connection.ConnectionType.AllToAll;
                            break;
                        default:
                            throw new Exception(String.Format( SR.UnknownConnectionType , connection));
                    }
                    switch (cht)
                    {
                        case "DiskFile":
                            info.ChannelKind = Connection.ChannelType.DiskFile;
                            break;
                        case "TCPPipe":
                            info.ChannelKind = Connection.ChannelType.TCP;
                            break;
                        case "MemoryFIFO":
                            info.ChannelKind = Connection.ChannelType.Fifo;
                            break;
                        default:
                            throw new Exception(String.Format( SR.UnknownChannelType2 , cht));
                    }
                    this.perNodeConnectionInfo.Add(stage.Id, info);
                }

                switch (stage.Operator)
                {
                    case "InputTable":
                        stage.IsInput = true;
                        stage.UriType = v.Element("StorageSet").Element("Type").Value;
                        stage.Uri = v.Element("StorageSet").Element("SourceURI").Value;
                        break;
                    case "OutputTable":
                        stage.IsOutput = true;
                        stage.UriType = v.Element("StorageSet").Element("Type").Value;
                        stage.Uri = v.Element("StorageSet").Element("SinkURI").Value;
                        break;
                    case "Tee":
                        stage.IsTee = true;
                        break;
                    case "Concat":
                        stage.IsConcat = true;
                        break;
                    default:
                        break;
                }

                if (v.Elements("Children").Count() == 0)
                    continue;

                bool first = true;
                IEnumerable<XElement> children = v.Element("Children").Elements().Where(e => e.Name == "Child");
                foreach (XElement child in children)
                {
                    // This code parallels the graphbuilder.cpp for XmlExecHost
                    Connection conn = new Connection();
                    int fromid = int.Parse(child.Element("UniqueId").Value);
                    ConnectionInformation fromConnectionInformation = this.perNodeConnectionInfo[fromid];
                    Stage from = this.stages[fromid];
                    conn.From = from;
                    conn.To = stage;
                    conn.ChannelKind = fromConnectionInformation.ChannelKind;

                    switch (fromConnectionInformation.ConnectionManager)
                    {
                        case "FullAggregator":
                        case "HashDistributor":
                        case "RangeDistributor":
                            // Ignore except first child
                            if (first)
                            {
                                first = false;
                                conn.ConnectionManager = fromConnectionInformation.ConnectionManager;
                            }
                            else
                            {
                                conn.ConnectionManager = "";
                            }
                            break;
                        case "PartialAggregator":
                        case "Broadcast":
                            // All children have the same connection manager
                            conn.ConnectionManager = fromConnectionInformation.ConnectionManager;
                            break;
                        case "Splitter":
                            // The connection manager depends on the number of children
                            if (first)
                            {
                                first = false;
                                if (children.Count() == 1)
                                    conn.ConnectionManager = fromConnectionInformation.ConnectionManager;
                                else
                                    conn.ConnectionManager = "SemiSplitter";
                            }
                            else
                            {
                                conn.ConnectionManager = "";
                            }
                            break;
                        case "None":
                        case "":
                            break;
                    }


                    conn.Arity = fromConnectionInformation.Arity;

                    this.connections.Add(conn);
                }
            }
        }

        /// <summary>
        /// Find the stage given the stage id as a string.
        /// </summary>
        /// <param name="stageId">Stage id.</param>
        /// <returns>A handle to the stage with the specified static Id.</returns>
        public Stage GetStageByStaticId(string stageId)
        {
            int id = int.Parse(stageId);
            return this.stages[id];
        }

        /// <summary>
        /// Find the stage given the stage name.
        /// </summary>
        /// <param name="name">Name of stage to return.</param>
        /// <returns>The stage with the given name or null.</returns>
        public Stage GetStageByName(string name)
        {
            foreach (Stage s in this.stages.Values)
            {
                if (s.Name.Equals(name))
                    return s;
            }

            return null;
        }

        /// <summary>
        /// The list of all stages in the plan.
        /// </summary>
        /// <returns>An iterator over the list of stages.</returns>
        public IEnumerable<Stage> GetAllStages()
        {
            return this.stages.Values;
        }

        /// <summary>
        /// The list of all connections in the plan.
        /// </summary>
        /// <returns>An iterator over a list of connections.</returns>
        public IEnumerable<Connection> GetAllConnections()
        {
            return this.connections;
        }
    }
}
