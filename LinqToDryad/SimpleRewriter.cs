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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.CodeDom;
using System.Xml;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    internal class NodeInfoEdge
    {
        public QueryNodeInfo Parent;
        public QueryNodeInfo Child;

        public NodeInfoEdge(QueryNodeInfo parent, QueryNodeInfo child)
        {
            this.Parent = parent;
            this.Child = child;
        }

        // Replace all occurences of oldEdge in edges by newEdge.
        public static bool UpdateEdge(List<NodeInfoEdge> edges, NodeInfoEdge oldEdge, NodeInfoEdge newEdge)
        {
            for (int i = 0; i < edges.Count; i++)
            {
                if (Object.ReferenceEquals(oldEdge, edges[i]))
                {
                    edges[i] = newEdge;
                    return true;
                }
            }
            return false;
        }

        // Insert a node info on this edge.
        public void Insert(QueryNodeInfo nextInfo)
        {
            Debug.Assert(nextInfo.Children.Count == 0 && nextInfo.Parents.Count == 0);
            NodeInfoEdge edge1 = new NodeInfoEdge(this.Parent, nextInfo);
            NodeInfoEdge edge2 = new NodeInfoEdge(nextInfo, this.Child);
            UpdateEdge(this.Parent.Children, this, edge1);
            nextInfo.Parents.Add(edge1);
            UpdateEdge(this.Child.Parents, this, edge2);
            nextInfo.Children.Add(edge2);
        }
    }
    
    internal class QueryNodeInfo
    {
        public Expression QueryExpression;
        public List<NodeInfoEdge> Children;
        public List<NodeInfoEdge> Parents;
        public bool IsQueryOperator;
        public DLinqQueryNode QueryNode;

        public QueryNodeInfo(Expression queryExpression,
                             bool isQueryOperator,
                             params QueryNodeInfo[] children)
        {
            this.QueryExpression = queryExpression;
            this.IsQueryOperator = isQueryOperator;
            this.Children = new List<NodeInfoEdge>(children.Length);
            foreach (QueryNodeInfo childInfo in children)
            {
                NodeInfoEdge edge = new NodeInfoEdge(this, childInfo);
                this.Children.Add(edge);
                childInfo.Parents.Add(edge);
            }
            this.Parents = new List<NodeInfoEdge>();
            this.QueryNode = null;
        }

        public string OperatorName
        {
            get {
                if (!this.IsQueryOperator) return null;
                return ((MethodCallExpression)this.QueryExpression).Method.Name;
            }
        }

        public Type Type
        {
            get { return this.QueryExpression.Type; }
        }

        public bool IsForked
        {
            get { return this.Parents.Count > 1; }
        }

        public virtual QueryNodeInfo Clone()
        {
            return new QueryNodeInfo(this.QueryExpression, this.IsQueryOperator);
        }
        
        // Delete this NodeInfo.
        // Precondition: this.children.Count < 2
        public void Delete()
        {
            Debug.Assert(this.Children.Count < 2);
            if (this.Children.Count == 0)
            {
                foreach (NodeInfoEdge edge in this.Parents)
                {
                    edge.Parent.Children.Remove(edge);
                }
            }
            else
            {
                QueryNodeInfo child = this.Children[0].Child;
                child.Parents.Remove(this.Children[0]);
                foreach (NodeInfoEdge edge in this.Parents)
                {
                    NodeInfoEdge newEdge = new NodeInfoEdge(edge.Parent, child);
                    NodeInfoEdge.UpdateEdge(edge.Parent.Children, edge, newEdge);
                    child.Parents.Add(newEdge);
                }
            }
            
            this.Parents.Clear();
            this.Children.Clear();
        }

        // Return true if this is not in the NodeInfo graph.
        public bool IsOrphaned
        {
            get { return (this.Children.Count == 0 && this.Parents.Count == 0); }
        }

        public void Swap(QueryNodeInfo other)
        {
            Debug.Assert(this.IsQueryOperator && other.IsQueryOperator);
            Debug.Assert(this.QueryNode == null && other.QueryNode == null);

            Expression queryExpr = this.QueryExpression;
            this.QueryExpression = other.QueryExpression;
            other.QueryExpression = queryExpr;
        }
    }

    internal class DummyQueryNodeInfo : QueryNodeInfo
    {
        public bool NeedsMerge;

        public DummyQueryNodeInfo(Expression queryExpression,
                                  bool needsMerge,
                                  params QueryNodeInfo[] children)
            : base(queryExpression, false, children)
        {
            NeedsMerge = needsMerge;
        }

        public override QueryNodeInfo Clone()
        {
            throw new InvalidOperationException("Internal error.");
        }
    }

    internal class DoWhileQueryNodeInfo : QueryNodeInfo
    {
        public QueryNodeInfo Body;
        public QueryNodeInfo Cond;
        public QueryNodeInfo BodySource;
        public QueryNodeInfo CondSource1;
        public QueryNodeInfo CondSource2;

        public DoWhileQueryNodeInfo(Expression queryExpression,
                                    QueryNodeInfo body,
                                    QueryNodeInfo cond,
                                    QueryNodeInfo bodySource,
                                    QueryNodeInfo condSource1,
                                    QueryNodeInfo condSource2,
                                    params QueryNodeInfo[] children)
            : base(queryExpression, false, children)
        {
            this.Body = body;
            this.Cond = cond;
            this.BodySource = bodySource;
            this.CondSource1 = condSource1;
            this.CondSource2 = condSource2;
        }
    }

    internal class SimpleRewriter
    {
        private List<QueryNodeInfo> m_nodeInfos;
        
        public SimpleRewriter(List<QueryNodeInfo> nodeInfos)
        {
            this.m_nodeInfos = nodeInfos;
        }

        public void Rewrite()
        {
            bool isDone = false;
            while (!isDone)
            {
                isDone = true;
                int idx = 0;
                while (idx < this.m_nodeInfos.Count)
                {
                    if (this.m_nodeInfos[idx].IsOrphaned)
                    {
                        this.m_nodeInfos[idx] = this.m_nodeInfos[this.m_nodeInfos.Count - 1];
                        this.m_nodeInfos.RemoveAt(this.m_nodeInfos.Count - 1);
                    }
                    else
                    {
                        bool changed = this.RewriteOne(idx);
                        isDone = isDone && !changed;
                        idx++;
                    }
                }
            }
        }

        // Return true iff EPG is modified by this method.
        public bool RewriteOne(int idx)
        {
            QueryNodeInfo curNode = this.m_nodeInfos[idx];
            if (curNode.OperatorName == "Where" && !curNode.Children[0].Child.IsForked)
            {
                LambdaExpression lambda = DryadLinqExpression.GetLambda(((MethodCallExpression)curNode.QueryExpression).Arguments[1]);
                if (lambda.Type.GetGenericArguments().Length == 2)
                {
                    QueryNodeInfo child = curNode.Children[0].Child;
                    string[] names = new string[] { "OrderBy", "Distinct", "RangePartition", "HashPartition" };
                    if (names.Contains(child.OperatorName))
                    {
                        curNode.Swap(child);
                        return true;
                    }
                    if (child.OperatorName == "Concat")
                    {
                        curNode.Delete();
                        for (int i = 0; i < child.Children.Count; i++)
                        {
                            NodeInfoEdge edge = child.Children[i];
                            QueryNodeInfo node = curNode.Clone();
                            this.m_nodeInfos.Add(node);
                            edge.Insert(node);
                        }
                        return true;
                    }
                }
            }
            else if ((curNode.OperatorName == "Select" || curNode.OperatorName == "SelectMany") &&
                     !curNode.Children[0].Child.IsForked)
            {
                LambdaExpression lambda = DryadLinqExpression.GetLambda(((MethodCallExpression)curNode.QueryExpression).Arguments[1]);
                if (lambda.Type.GetGenericArguments().Length == 2)
                {
                    QueryNodeInfo child = curNode.Children[0].Child;
                    if (child.OperatorName == "Concat")
                    {
                        curNode.Delete();
                        for (int i = 0; i < child.Children.Count; i++)
                        {
                            NodeInfoEdge edge = child.Children[i];
                            QueryNodeInfo node = curNode.Clone();
                            this.m_nodeInfos.Add(node);
                            edge.Insert(node);
                        }
                        return true;
                    }
                }
            }
            else if (curNode.OperatorName == "Take" && !curNode.Children[0].Child.IsForked)
            {
                QueryNodeInfo child = curNode.Children[0].Child;
                if (child.OperatorName == "Select")
                {
                    QueryNodeInfo cchild = child.Children[0].Child;
                    if (cchild.OperatorName != "GroupBy")
                    {
                        curNode.Swap(child);
                        return true;
                    }
                }
            }
            else if ((curNode.OperatorName == "Contains" ||
                      curNode.OperatorName == "ContainsAsQuery" ||
                      curNode.OperatorName == "All" ||
                      curNode.OperatorName == "AllAsQuery" ||
                      curNode.OperatorName == "Any" ||
                      curNode.OperatorName == "AnyAsQuery") &&
                     !curNode.Children[0].Child.IsForked)
            {
                QueryNodeInfo child = curNode.Children[0].Child;
                string[] names = new string[] { "OrderBy", "Distinct", "RangePartition", "HashPartition" };
                if (names.Contains(child.OperatorName))
                {
                    child.Delete();
                    return true;
                }
            }
            return false;
        }
    }
}
