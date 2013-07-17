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
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.CodeDom;
using System.Diagnostics;
using System.Xml;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    internal enum DynamicManagerType
    {
        None,
        Splitter,
        PartialAggregator,
        FullAggregator,
        HashDistributor,
        RangeDistributor,
        Broadcast
    }

    internal class DynamicManager
    {
        internal static DynamicManager None = new DynamicManager(DynamicManagerType.None);
        internal static DynamicManager Splitter = new DynamicManager(DynamicManagerType.Splitter);
        internal static DynamicManager PartialAggregator = new DynamicManager(DynamicManagerType.PartialAggregator);
        internal static DynamicManager Broadcast = new DynamicManager(DynamicManagerType.Broadcast);

        private DynamicManagerType m_managerType;
        internal protected List<DryadQueryNode> m_vertexNodes;
        private string[] m_vertexNames;

        private DynamicManager(DynamicManagerType type)
        {
            this.m_managerType = type;
            this.m_vertexNodes = new List<DryadQueryNode>();
            this.m_vertexNames = null;
            this.AggregationLevels = 0;
            // default aggregation has 1 level
            if (type == DynamicManagerType.FullAggregator)
            {
                AggregationLevels = 1;
            }
        }

        /// <summary>
        /// Create a dynamic manager (a Dryad policy manager) with a list of parameter nodes.
        /// </summary>
        /// <param name="type">Type of dynamic manager to create.</param>
        /// <param name="nodes">Nodes that the manager depends on.</param>
        internal DynamicManager(DynamicManagerType type, List<DryadQueryNode> nodes)
            : this(type)
        {
            this.m_vertexNodes.AddRange(nodes);
        }
        
        /// <summary>
        /// Create a dynamic manager with a single parameter node.
        /// </summary>
        /// <param name="type">Type of manager to create.</param>
        /// <param name="node">Node that the manager depends on.</param>
        internal DynamicManager(DynamicManagerType type, DryadQueryNode node)
            : this(type)
        {
            this.m_vertexNodes.Add(node);
        }

        internal DynamicManagerType ManagerType
        {
            get { return this.m_managerType; }
        }

        internal DynamicManager CreateManager(DynamicManagerType type)
        {
            return new DynamicManager(type, this.m_vertexNodes);
        }

        /// <summary>
        /// The aggregation level of the dynamic manager (used for aggregations only).
        /// </summary>
        internal int AggregationLevels { get; set; }

        internal DryadQueryNode GetVertexNode(int index)
        {
            return this.m_vertexNodes[index];
        }
        
        internal void InsertVertexNode(int index, DryadQueryNode node)
        {
            if (index == -1)
            {
                this.m_vertexNodes.Add(node);
            }
            else
            {
                this.m_vertexNodes.Insert(index, node);
            }
        }

        internal virtual void CreateVertexCode()
        {
            if (this.m_vertexNodes.Count != 0)
            {
                this.m_vertexNames = new string[this.m_vertexNodes.Count];
                for (int i = 0; i < this.m_vertexNames.Length; i++)
                {
                    CodeMemberMethod vertexMethod = this.m_vertexNodes[i].QueryGen.CodeGen.AddVertexMethod(this.m_vertexNodes[i]);
                    this.m_vertexNames[i] = vertexMethod.Name;
                }
            }
        }

        internal virtual XmlElement CreateElem(XmlDocument queryDoc)
        {
            XmlElement managerElem = queryDoc.CreateElement("DynamicManager");
            XmlElement elem = queryDoc.CreateElement("Type");
            elem.InnerText = Convert.ToString(this.ManagerType.ToString());
            managerElem.AppendChild(elem);

            if (AggregationLevels != 0)
            {
                XmlElement agg = queryDoc.CreateElement("AggregationLevels");
                agg.InnerText = AggregationLevels.ToString();
                managerElem.AppendChild(agg);
            }

            if (this.m_vertexNames != null)
            {
                for (int i = 0; i < this.m_vertexNames.Length; i++)
                {
                    string dllName = this.m_vertexNodes[i].QueryGen.CodeGen.GetDryadLinqDllName();
                    XmlElement entryElem = DryadQueryDoc.CreateVertexEntryElem(queryDoc, dllName, this.m_vertexNames[i]);
                    managerElem.AppendChild(entryElem);
                }
            }
            return managerElem;
        }
    }

    internal class DynamicRangeDistributor : DynamicManager
    {
        private double m_sampleRate;

        internal DynamicRangeDistributor(DryadQueryNode node)
            : base(DynamicManagerType.RangeDistributor, node)
        {
            //@@TODO[P2]: This sample rate used here should really be its own constant.
            this.m_sampleRate = HpcLinqSampler.SAMPLE_RATE;
        }

        internal override void CreateVertexCode()
        {
        }

        internal override XmlElement CreateElem(XmlDocument queryDoc)
        {
            XmlElement managerElem = queryDoc.CreateElement("DynamicManager");
            XmlElement elem = queryDoc.CreateElement("Type");
            elem.InnerText = Convert.ToString(this.ManagerType.ToString());
            managerElem.AppendChild(elem);

            elem = queryDoc.CreateElement("SampleRate");
            elem.InnerText = Convert.ToString(this.m_sampleRate);
            managerElem.AppendChild(elem);

            elem = queryDoc.CreateElement("VertexId");
            DryadQueryNode node = this.m_vertexNodes[0];
            if (node.SuperNode != null)
            {
                node = node.SuperNode;
            }
            elem.InnerText = Convert.ToString(node.m_uniqueId);
            managerElem.AppendChild(elem);

            return managerElem;
        }
    }
}
