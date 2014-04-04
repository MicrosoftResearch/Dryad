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
    internal class VertexCodeGen
    {
        protected DryadLinqContext m_context;

        internal VertexCodeGen(DryadLinqContext context)
        {
            this.m_context = context;
        }

        internal virtual IEnumerable<string> ResourcesToAdd()
        {
            return Enumerable.Empty<string>();
        }

        internal virtual IEnumerable<string> ResourcesToRemove()
        {
            return Enumerable.Empty<string>();
        }

        internal virtual IEnumerable<string> GetReferencedAssemblies()
        {
            return Enumerable.Empty<string>();
        }

        internal virtual IEnumerable<string> GetGeneratedSources()
        {
            return Enumerable.Empty<string>();
        }

        internal virtual string AddVertexCode(DLinqQueryNode node,
                                              CodeMemberMethod vertexMethod,
                                              string[] readerNames,
                                              string[] writerNames)
        {
            switch (node.NodeType)
            {
                case QueryNodeType.InputTable:
                {
                    return this.Visit((DLinqInputNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.OutputTable:
                {   
                    return this.Visit((DLinqOutputNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Aggregate:
                {    
                    return this.Visit((DLinqAggregateNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Select:
                case QueryNodeType.SelectMany:
                {
                    return this.Visit((DLinqSelectNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Where:
                {
                    return this.Visit((DLinqWhereNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Distinct:
                {
                    return this.Visit((DLinqDistinctNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.BasicAggregate:
                {
                    return this.Visit((DLinqBasicAggregateNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.GroupBy:
                {
                    return this.Visit((DLinqGroupByNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.OrderBy:
                {
                    return this.Visit((DLinqOrderByNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Skip:
                case QueryNodeType.SkipWhile:
                case QueryNodeType.Take:
                case QueryNodeType.TakeWhile:
                {
                    return this.Visit((DLinqPartitionOpNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Contains:
                {
                    return this.Visit((DLinqContainsNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Join:
                case QueryNodeType.GroupJoin:
                {
                    return this.Visit((DLinqJoinNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Union:
                case QueryNodeType.Intersect:
                case QueryNodeType.Except:
                {
                    return this.Visit((DLinqSetOperationNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Concat:
                {
                    return this.Visit((DLinqConcatNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Zip:
                {
                    return this.Visit((DLinqZipNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Super:
                {
                    return this.Visit((DLinqSuperNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.RangePartition:
                {
                    return this.Visit((DLinqRangePartitionNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.HashPartition:
                {
                    return this.Visit((DLinqHashPartitionNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Merge:
                {
                    return this.Visit((DLinqMergeNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Apply:
                {
                    return this.Visit((DLinqApplyNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Fork:
                {
                    return this.Visit((DLinqForkNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Tee:
                {
                    return this.Visit((DLinqTeeNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Dynamic:
                {
                    return this.Visit((DLinqDynamicNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Dummy:
                {
                    return this.Visit((DLinqDummyNode)node, vertexMethod, readerNames, writerNames);
                }
                default:
                {
                    throw new DryadLinqException("Internal error: unhandled node type " + node.NodeType);
                }
            }
        }

        internal virtual string Visit(DLinqInputNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqOutputNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqWhereNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqSelectNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqZipNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqOrderByNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqGroupByNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqPartitionOpNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqJoinNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqDistinctNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqContainsNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqBasicAggregateNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqAggregateNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqConcatNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqSetOperationNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqMergeNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqHashPartitionNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqRangePartitionNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqSuperNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqApplyNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqForkNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqTeeNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqDynamicNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DLinqDummyNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

    }
}
