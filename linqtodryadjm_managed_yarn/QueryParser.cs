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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Research.Dryad;

namespace linqtodryadjm_managed
{
    public class QueryPlanParser
    {
        public VertexInfo.IOType GetIoType(string type)
        {
            if (type == "File") return VertexInfo.IOType.FILELIST;
            if (type == "PartitionedFile") return VertexInfo.IOType.PARTITIONEDFILE;
            if (type == "FileDirectory") return VertexInfo.IOType.FILEDIRECTORY;
            if (type == "FileWildcard") return VertexInfo.IOType.FILEWILDCARD;
            if (type == "TidyFS") return VertexInfo.IOType.STREAM;
            if (type == "Dsc") return VertexInfo.IOType.STREAM;
            if (type == "Hdfs") return VertexInfo.IOType.HDFS_STREAM;
            if (type == "FilePrefix") return VertexInfo.IOType.FILEPREFIX;
            throw new LinqToDryadException(String.Format("Unknown IoType: {0}", type));
        }

        public Vertex.Type GetVertexType(string type)
        {
            if (type == "InputTable") return Vertex.Type.INPUTTABLE;
            if (type == "OutputTable") return Vertex.Type.OUTPUTTABLE;
            if (type == "Where") return Vertex.Type.WHERE;
            if (type == "Join") return Vertex.Type.JOIN;
            if (type == "Fork") return Vertex.Type.FORK;
            if (type == "Tee") return Vertex.Type.TEE;
            if (type == "Concat") return Vertex.Type.CONCAT;
            if (type == "Super") return Vertex.Type.SUPER;
            if (type == "Apply") return Vertex.Type.SUPER;
            return Vertex.Type.UNKNOWN;
        }

        public Predecessor.ChannelType GetChannelType(string type)
        {
            if (type == "DiskFile") return Predecessor.ChannelType.DISKFILE;
            if (type == "TCPPipe") return Predecessor.ChannelType.TCPPIPE;
            if (type == "MemoryFIFO") return Predecessor.ChannelType.MEMORYFIFO;
            throw new LinqToDryadException(String.Format("Unknown ChannelType: {0}", type));
        }

        public Predecessor.ConnectionOperator GetConnectionOperator(string type)
        {
            if (type == "Pointwise") return Predecessor.ConnectionOperator.POINTWISE;
            if (type == "CrossProduct") return Predecessor.ConnectionOperator.CROSSPRODUCT;
            throw new LinqToDryadException(String.Format("Unknown ConnectionOperator: {0}", type));
        }

        public Predecessor.AffinityConstraint GetAffinityConstraint(string type)
        {
            if (type == "UseDefault") return Predecessor.AffinityConstraint.UseDefault;
            if (type == "HardConstraint") return Predecessor.AffinityConstraint.HardConstraint;
            if (type == "OptimizationConstraint") return Predecessor.AffinityConstraint.OptimizationConstraint;
            if (type == "Preference") return Predecessor.AffinityConstraint.Preference;
            if (type == "DontCare") return Predecessor.AffinityConstraint.DontCare;
            throw new LinqToDryadException(String.Format("Unknown AffinityConstraint: {0}", type));
        }

        public DynamicManager.Type GetDynamicManagerType(string type)
        {
            if (type == "None") return DynamicManager.Type.NONE;    
            if (type == "Splitter") return DynamicManager.Type.SPLITTER;
            if (type == "PartialAggregator") return DynamicManager.Type.PARTIALAGGR;    
            if (type == "FullAggregator") return DynamicManager.Type.FULLAGGR;
            if (type == "HashDistributor") return DynamicManager.Type.HASHDISTRIBUTOR;
            if (type == "RangeDistributor") return DynamicManager.Type.RANGEDISTRIBUTOR;        
            if (type == "Broadcast") return DynamicManager.Type.BROADCAST;
            throw new LinqToDryadException(String.Format("Unknown DynamicManager: {0}", type));
        }

        public static bool SplitEntryIntoAssemblyClassMethod(string entry, out string _assembly, out string _class, out string _method)
        {
            _assembly = "";
            _class = "";
            _method = "";

            int indexBang = entry.IndexOf("!");
            int indexPeriod = entry.LastIndexOf(".");

            if (indexBang == -1 || indexPeriod == -1 || indexPeriod <= indexBang)
            {
                return false;
            }

            _assembly = entry.Substring(0, indexBang);
            _class = entry.Substring(indexBang + 1, indexPeriod - indexBang - 1);
            _method = entry.Substring(indexPeriod + 1);
            return true;

        }

        private void ParseQueryXmlLinqToDryad(XmlDocument queryPlanDoc, Query query)
        {
            XmlElement root = queryPlanDoc.DocumentElement;

            //
            // Query globals
            //
            query.queryPlan = new SortedDictionary<int, Vertex>();
            query.compilerversion = root.SelectSingleNode("DryadLinqVersion").InnerText;
            query.clusterName = root.SelectSingleNode("ClusterName").InnerText;
            query.visualization = root.SelectSingleNode("Visualization").InnerText;

            // Compression scheme for intermediate data
            XmlNode compressionNode = root.SelectSingleNode("IntermediateDataCompression");
            if (compressionNode != null)
            {
                query.intermediateDataCompression = Convert.ToInt32(compressionNode.InnerText);
            }

            //
            // XmlExecHost arguments
            //
            XmlNodeList nodes = root.SelectSingleNode("XmlExecHostArgs").ChildNodes;
            query.xmlExecHostArgs = new string[nodes.Count];
            for (int index=0; index<nodes.Count; index++)
            {
                query.xmlExecHostArgs[index] = nodes[index].InnerText;
            }
            
            // 
            // Get Speculative duplication flag - default is enabled (true)
            //
            XmlNode duplicationNode = root.SelectSingleNode("EnableSpeculativeDuplication");
            if (duplicationNode != null)
            {
                bool dupFlag;
                if (bool.TryParse(duplicationNode.InnerText, out dupFlag))
                {
                    query.enableSpeculativeDuplication = dupFlag;
                }
            }

            nodes = root.SelectSingleNode("QueryPlan").ChildNodes; 

            //
            // Need to remember the conection operator for use when the
            // predecessors are being created.
            //            
            string[] vertexConnectionOperator = new string[nodes.Count];
            for (int index=0; index<nodes.Count; index++)
            {
                vertexConnectionOperator[index] = "";                
            }
            
            for (int index=0; index<nodes.Count; index++)
            {
                Vertex vertex = new Vertex();

                //
                // Vertex globals
                //
                string uniqueId = nodes[index].SelectSingleNode("UniqueId").InnerText;
                string vertexType = nodes[index].SelectSingleNode("Type").InnerText;
                string name = nodes[index].SelectSingleNode("Name").InnerText;
                string partitions = nodes[index].SelectSingleNode("Partitions").InnerText;
                string channelType = nodes[index].SelectSingleNode("ChannelType").InnerText;

                //
                // Need to remember the conection operator for use when the
                // predecessors are being created.
                //            
                vertexConnectionOperator[index] = nodes[index].SelectSingleNode("ConnectionOperator").InnerText;

                vertex.uniqueId = Convert.ToInt32(uniqueId);
                vertex.name = name;
                vertex.type = GetVertexType(vertexType);
                vertex.partitions = Convert.ToInt32(partitions);

                XmlNode dynamicManager = nodes[index].SelectSingleNode("DynamicManager");
                string dmType = dynamicManager.SelectSingleNode("Type").InnerText;
                
                vertex.dynamicManager = new DynamicManager();
                vertex.dynamicManager.type = GetDynamicManagerType(dmType);

                if (vertex.dynamicManager.type == DynamicManager.Type.FULLAGGR) 
                {
                    string levels = dynamicManager.SelectSingleNode("AggregationLevels").InnerText;
                    vertex.dynamicManager.aggregationLevels = Convert.ToInt32(levels);
                }         
       
                if (vertex.dynamicManager.type == DynamicManager.Type.RANGEDISTRIBUTOR)
                {
                    string sampleRate = dynamicManager.SelectSingleNode("SampleRate").InnerText;
                    string vertexId = dynamicManager.SelectSingleNode("VertexId").InnerText;
                    vertex.dynamicManager.sampleRate = Convert.ToDouble(sampleRate);
                    vertex.dynamicManager.splitVertexId = Convert.ToInt32(vertexId);
                }
                else
                {
                    XmlNodeList entries = dynamicManager.SelectNodes("Entry");
                    vertex.dynamicManager.assemblyNames = new string[entries.Count];
                    vertex.dynamicManager.classNames = new string[entries.Count];
                    vertex.dynamicManager.methodNames = new string[entries.Count];
                    for (int entryIndex = 0; entryIndex < entries.Count; entryIndex++)
                    {
                        vertex.dynamicManager.assemblyNames[entryIndex] = entries[entryIndex].SelectSingleNode("AssemblyName").InnerText;
                        vertex.dynamicManager.classNames[entryIndex] = entries[entryIndex].SelectSingleNode("ClassName").InnerText;
                        vertex.dynamicManager.methodNames[entryIndex] = entries[entryIndex].SelectSingleNode("MethodName").InnerText;
                    }
                }
                  
                if (vertex.type == Vertex.Type.INPUTTABLE)
                {
                    XmlNode storageSet = nodes[index].SelectSingleNode("StorageSet");
                    string ioType = storageSet.SelectSingleNode("Type").InnerText;

                    vertex.info = new VertexInfo();
                    vertex.info.ioType = GetIoType(ioType);

                    XmlNodeList storageUris = storageSet.SelectNodes("SourceURI");
                    vertex.info.sources = new string[storageUris.Count];
                    for (int indexStorageUri=0; indexStorageUri<storageUris.Count; indexStorageUri++)
                    {
                        vertex.info.sources[indexStorageUri] = storageUris[indexStorageUri].InnerText;
                    }
                }
                else if ( vertex.type == Vertex.Type.OUTPUTTABLE )
                {
                    XmlNode storageSet = nodes[index].SelectSingleNode("StorageSet");
                    
                    string ioType = storageSet.SelectSingleNode("Type").InnerText;
                    
                    vertex.info = new VertexInfo();
                    vertex.info.ioType = GetIoType(ioType);

                    string source = storageSet.SelectSingleNode("SinkURI").InnerText;    
                    vertex.info.sources  = new string[1] { source };
                    
                    if (vertex.info.ioType == VertexInfo.IOType.PARTITIONEDFILE )
                    {
                        vertex.info.partitionUncPath = storageSet.SelectSingleNode("PartitionUncPath").InnerText;
                    }

                    XmlNode temporary = storageSet.SelectSingleNode("IsTemporary");
                    if (temporary != null)
                    {
                        if (bool.TryParse(temporary.InnerXml, out vertex.info.isTemporary) == false)
                        {
                            throw new LinqToDryadException(String.Format("Invalid value for IsTemporary: {0}", temporary.InnerXml));
                        }
                    }
                    else
                    {
                        vertex.info.isTemporary = false;
                    }

                    XmlNode recordType = storageSet.SelectSingleNode("RecordType");
                    if (recordType != null)
                    {
                        vertex.info.recordType = recordType.InnerXml;
                    }

                    XmlNode outputCompressionScheme = storageSet.SelectSingleNode("OutputCompressionScheme");
                    if (outputCompressionScheme != null)
                    {
                        if (int.TryParse(outputCompressionScheme.InnerXml, out vertex.info.compressionScheme) == false)
                        {
                            throw new LinqToDryadException(String.Format("Invalid value for OutputCompressionScheme: {0}", outputCompressionScheme.InnerXml));
                        }
                    }
                    else
                    {
                        vertex.info.compressionScheme = 0;  // TODO: Change to Enum
                    }
                } 
                else /* JOIN etc. */
                {
                    XmlNode entry = nodes[index].SelectSingleNode("Entry");
                    
                    vertex.info = new VertexInfo();
                    vertex.info.assemblyName = entry.SelectSingleNode("AssemblyName").InnerText;
                    vertex.info.className = entry.SelectSingleNode("ClassName").InnerText;
                    vertex.info.methodName = entry.SelectSingleNode("MethodName").InnerText;        
                }
                
                //
                // everybody except inputs have children
                //
                if (vertex.type != Vertex.Type.INPUTTABLE)
                {
                    XmlNodeList children = nodes[index].SelectSingleNode("Children").ChildNodes;

                    vertex.info.predecessors = new Predecessor[children.Count];

                    for (int indexChild = 0; indexChild < children.Count; indexChild++)
                    {
                        int childId = Convert.ToInt32(children[indexChild].SelectSingleNode("UniqueId").InnerText);
                        string childConstraint = children[indexChild].SelectSingleNode("AffinityConstraint").InnerText;
                        
                        vertex.info.predecessors[indexChild] = new Predecessor();
                        vertex.info.predecessors[indexChild].uniqueId = childId;
                        vertex.info.predecessors[indexChild].connectionOperator = GetConnectionOperator(vertexConnectionOperator[childId]);
                        vertex.info.predecessors[indexChild].channelType = GetChannelType(channelType);
                        vertex.info.predecessors[indexChild].constraint = GetAffinityConstraint(childConstraint);
                    }
                } 

                //
                // In this parser the only way to have an optional Tag is immediately before consuming
                // a close of an outer tag.  So we have to look for machines tag here.
                //
                if ((vertex.type != Vertex.Type.INPUTTABLE) && (vertex.type != Vertex.Type.OUTPUTTABLE))
                {
                    XmlNode machinesRoot = nodes[index].SelectSingleNode("Machines");
                    if (machinesRoot != null)
                    {
                        XmlNodeList machines = machinesRoot.ChildNodes;
                        vertex.machines = new string[machines.Count];
                        for (int indexMachine = 0; indexMachine < machines.Count; indexMachine++)
                        {
                            vertex.machines[indexMachine] = machines[indexMachine].SelectSingleNode("Machine").InnerText;

                        }
                    }
                }
  
                //
                // Add the vertex
                //
                query.queryPlan.Add(vertex.uniqueId, vertex);       
            }            
        }
        
        public bool ParseQueryXml(string queryPlanFileName, Query query)
        {
            XmlNode version = null;
            XmlDocument queryPlanDoc = new XmlDocument();

            //
            // Load query plan document
            //
            try
            {
                queryPlanDoc.Load(queryPlanFileName);
            }
            catch (Exception e)
            {
                DryadLogger.LogCritical(0, e, "Failed to load query plan: {0}", queryPlanFileName);
                return false;
            }

            //
            // Get DryadLinqVersion - absence used to indicate Argentia query plan
            //
            try
            {
                version = queryPlanDoc.DocumentElement.SelectSingleNode("DryadLinqVersion");
            }
            catch (System.Xml.XPath.XPathException e)
            {
                DryadLogger.LogCritical(0, e, "Failed to select node DryadLinqVersion from query plan: {0}", queryPlanFileName);
                return false;
            }

            if (version == null)
            {
                DryadLogger.LogCritical(0, null, "Missing element 'DryadLinqVersion' in query plan: {0}", queryPlanFileName);
                return false;
            }

            //
            // Parse query plan XML doc into Query
            //
            try
            {
                ParseQueryXmlLinqToDryad(queryPlanDoc, query);
            }
            catch (Exception e)
            {
                DryadLogger.LogCritical(0, e, "Failed to parse query plan: {0}", queryPlanFileName);
                return false;
            }
            
#if REMOVE_ARGENTIA
            else // If (version == null), Argentia query plan
            {
                // Add the namespace.
                XmlNamespaceManager nsmgr = new XmlNamespaceManager(queryPlanDoc.NameTable);
                nsmgr.AddNamespace("qp", "http://microsoft.com/PCP/Argentia/QueryPlan.xsd");

                version = queryPlanDoc.DocumentElement.SelectSingleNode("qp:RuntimeVersion", nsmgr);
                if (version != null)
                {
                    ParseQueryXmlArgentia(queryPlanDoc, query);
                }
                else
                {
                    DryadLogger.LogCritical(0, null, "Unknown query plan format.");
                    return false;
                }
            }
#endif

            return true;
        }

    }

}
