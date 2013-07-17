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
        internal virtual IEnumerable<string> GetResources()
        {
            return new string[0];
        }

        internal virtual string AddVertexCode(DryadQueryNode node,
                                              CodeMemberMethod vertexMethod,
                                              string[] readerNames,
                                              string[] writerNames)
        {
            switch (node.NodeType)
            {
                case QueryNodeType.InputTable:
                {
                    return this.Visit((DryadInputNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.OutputTable:
                {   
                    return this.Visit((DryadOutputNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Aggregate:
                {    
                    return this.Visit((DryadAggregateNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Select:
                case QueryNodeType.SelectMany:
                {
                    return this.Visit((DryadSelectNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Where:
                {
                    return this.Visit((DryadWhereNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Distinct:
                {
                    return this.Visit((DryadDistinctNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.BasicAggregate:
                {
                    return this.Visit((DryadBasicAggregateNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.GroupBy:
                {
                    return this.Visit((DryadGroupByNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.OrderBy:
                {
                    return this.Visit((DryadOrderByNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Skip:
                case QueryNodeType.SkipWhile:
                case QueryNodeType.Take:
                case QueryNodeType.TakeWhile:
                {
                    return this.Visit((DryadPartitionOpNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Contains:
                {
                    return this.Visit((DryadContainsNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Join:
                case QueryNodeType.GroupJoin:
                {
                    return this.Visit((DryadJoinNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Union:
                case QueryNodeType.Intersect:
                case QueryNodeType.Except:
                {
                    return this.Visit((DryadSetOperationNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Concat:
                {
                    return this.Visit((DryadConcatNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Zip:
                {
                    return this.Visit((DryadZipNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Super:
                {
                    return this.Visit((DryadSuperNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.RangePartition:
                {
                    return this.Visit((DryadRangePartitionNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.HashPartition:
                {
                    return this.Visit((DryadHashPartitionNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Merge:
                {
                    return this.Visit((DryadMergeNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Apply:
                {
                    return this.Visit((DryadApplyNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Fork:
                {
                    return this.Visit((DryadForkNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Tee:
                {
                    return this.Visit((DryadTeeNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Dynamic:
                {
                    return this.Visit((DryadDynamicNode)node, vertexMethod, readerNames, writerNames);
                }
                case QueryNodeType.Dummy:
                {
                    return this.Visit((DryadDummyNode)node, vertexMethod, readerNames, writerNames);
                }
                default:
                    throw new DryadLinqException("Internal error: unhandled node type " + node.NodeType);
            }
        }

        internal virtual string Visit(DryadInputNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadOutputNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadWhereNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadSelectNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadZipNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadOrderByNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadGroupByNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadPartitionOpNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadJoinNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadDistinctNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadContainsNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadBasicAggregateNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadAggregateNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadConcatNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadSetOperationNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadMergeNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadHashPartitionNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadRangePartitionNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadSuperNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadApplyNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadForkNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadTeeNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadDynamicNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

        internal virtual string Visit(DryadDummyNode node,
                                      CodeMemberMethod vertexMethod,
                                      string[] readerNames,
                                      string[] writerNames)
        {
            return node.AddVertexCode(vertexMethod, readerNames, writerNames);
        }

    }
}
