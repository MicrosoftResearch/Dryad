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
        public QueryNodeInfo parent;
        public QueryNodeInfo child;

        public NodeInfoEdge(QueryNodeInfo parent, QueryNodeInfo child)
        {
            this.parent = parent;
            this.child = child;
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
            Debug.Assert(nextInfo.children.Count == 0 && nextInfo.parents.Count == 0);
            NodeInfoEdge edge1 = new NodeInfoEdge(this.parent, nextInfo);
            NodeInfoEdge edge2 = new NodeInfoEdge(nextInfo, this.child);
            UpdateEdge(this.parent.children, this, edge1);
            nextInfo.parents.Add(edge1);
            UpdateEdge(this.child.parents, this, edge2);
            nextInfo.children.Add(edge2);
        }
    }
    
    internal class QueryNodeInfo
    {
        public Expression queryExpression;
        public List<NodeInfoEdge> children;
        public List<NodeInfoEdge> parents;
        public bool isQueryOperator;
        public DryadQueryNode queryNode;

        public QueryNodeInfo(Expression queryExpression,
                             bool isQueryOperator,
                             params QueryNodeInfo[] children)
        {
            this.queryExpression = queryExpression;
            this.isQueryOperator = isQueryOperator;
            this.children = new List<NodeInfoEdge>(children.Length);
            foreach (QueryNodeInfo childInfo in children)
            {
                NodeInfoEdge edge = new NodeInfoEdge(this, childInfo);
                this.children.Add(edge);
                childInfo.parents.Add(edge);
            }
            this.parents = new List<NodeInfoEdge>();
            this.queryNode = null;
        }

        public string OperatorName
        {
            get {
                if (!this.isQueryOperator) return null;
                return ((MethodCallExpression)this.queryExpression).Method.Name;
            }
        }

        public Type Type
        {
            get { return this.queryExpression.Type; }
        }

        public bool IsForked
        {
            get { return this.parents.Count > 1; }
        }

        public QueryNodeInfo Clone()
        {
            return new QueryNodeInfo(this.queryExpression, this.isQueryOperator);
        }
        
        // Delete this NodeInfo.
        // Precondition: this.children.Count < 2
        public void Delete()
        {
            Debug.Assert(this.children.Count < 2);
            if (this.children.Count == 0)
            {
                foreach (NodeInfoEdge edge in this.parents)
                {
                    edge.parent.children.Remove(edge);
                }
            }
            else
            {
                QueryNodeInfo child = this.children[0].child;
                child.parents.Remove(this.children[0]);
                foreach (NodeInfoEdge edge in this.parents)
                {
                    NodeInfoEdge newEdge = new NodeInfoEdge(edge.parent, child);
                    NodeInfoEdge.UpdateEdge(edge.parent.children, edge, newEdge);
                    child.parents.Add(newEdge);
                }
            }
            
            this.parents.Clear();
            this.children.Clear();
        }

        // Return true if this is not in the NodeInfo graph.
        public bool IsOrphaned
        {
            get { return (this.children.Count == 0 && this.parents.Count == 0); }
        }

        public void Swap(QueryNodeInfo other)
        {
            Debug.Assert(this.isQueryOperator && other.isQueryOperator);
            Debug.Assert(this.queryNode == null && other.queryNode == null);

            Expression queryExpr = this.queryExpression;
            this.queryExpression = other.queryExpression;
            other.queryExpression = queryExpr;
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
            if (curNode.OperatorName == "Where" && !curNode.children[0].child.IsForked)
            {
                LambdaExpression lambda = HpcLinqExpression.GetLambda(((MethodCallExpression)curNode.queryExpression).Arguments[1]);
                if (lambda.Type.GetGenericArguments().Length == 2)
                {
                    QueryNodeInfo child = curNode.children[0].child;
                    string[] names = new string[] { "OrderBy", "Distinct", "RangePartition", "HashPartition" };
                    if (names.Contains(child.OperatorName))
                    {
                        curNode.Swap(child);
                        return true;
                    }
                    if (child.OperatorName == "Concat")
                    {
                        curNode.Delete();
                        for (int i = 0; i < child.children.Count; i++)
                        {
                            NodeInfoEdge edge = child.children[i];
                            QueryNodeInfo node = curNode.Clone();
                            this.m_nodeInfos.Add(node);
                            edge.Insert(node);
                        }
                        return true;
                    }
                }
            }
            else if ((curNode.OperatorName == "Select" || curNode.OperatorName == "SelectMany") &&
                     !curNode.children[0].child.IsForked)
            {
                LambdaExpression lambda = HpcLinqExpression.GetLambda(((MethodCallExpression)curNode.queryExpression).Arguments[1]);
                if (lambda.Type.GetGenericArguments().Length == 2)
                {
                    QueryNodeInfo child = curNode.children[0].child;
                    if (child.OperatorName == "Concat")
                    {
                        curNode.Delete();
                        for (int i = 0; i < child.children.Count; i++)
                        {
                            NodeInfoEdge edge = child.children[i];
                            QueryNodeInfo node = curNode.Clone();
                            this.m_nodeInfos.Add(node);
                            edge.Insert(node);
                        }
                        return true;
                    }
                }
            }
            else if (curNode.OperatorName == "Take" && !curNode.children[0].child.IsForked)
            {
                QueryNodeInfo child = curNode.children[0].child;
                if (child.OperatorName == "Select")
                {
                    QueryNodeInfo cchild = child.children[0].child;
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
                     !curNode.children[0].child.IsForked)
            {
                QueryNodeInfo child = curNode.children[0].child;
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
