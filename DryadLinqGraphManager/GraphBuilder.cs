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

namespace Microsoft.Research.Dryad.GraphManager
{
    public class GraphBuilder
    {
        enum ChannelConnectorType
        {
            DCT_File,
            DCT_Output,
            DCT_Pipe,
            DCT_Fifo,
            DCT_FifoNonBlocking,
            DCT_Tombstone
        };

        public class DrVertexSet : List<DrVertex>
        {
        };

        public class GraphStageInfo
        {
            public DrVertexSet members;
            public Vertex vertex;
            public DrStageManager stageManager;

            public GraphStageInfo()
            {
                stageManager = null;
            }

            public GraphStageInfo(DrVertexSet m, Vertex v, DrStageManager mgr)
            {
                members = m;
                vertex = v;
                stageManager = mgr;
            }
        }

        // GraphStageMap is used to store the vertices as they are created, grouped by query plan stage.
        // Once all the stages are added, the map is iterated over to connect the appropriate vertices 
        // and produce the full graph.
        
        internal bool SUCCEEDED(int err)
        {
            if (err == 0)
            {
                return true;
            }
            return false;
        }

        private DrInputStreamManager CreateInputNode(DryadLINQApp app, VertexInfo info, string inputName) 
        {
            DrInputStreamManager s;
            int err = 0;

            DryadLogger.LogInformation("methodEntry: " + inputName);

            if (info.ioType == VertexInfo.IOType.PARTITIONEDFILE )
            {
                DrPartitionInputStream input = new DrPartitionInputStream();

                err = input.Open(app.GetUniverse(), new Uri(info.sources[0]).AbsolutePath);
                if (!SUCCEEDED(err))
                {
                    string msg = String.Format("Could not read DSC input file {0}", info.sources[0]);
                    throw new LinqToDryadException(msg, err);
                }
                
                DrManagerBase inputStage = new DrManagerBase(app.GetGraph(), inputName);
                DrInputStreamManager inputManager = new DrInputStreamManager(input, inputStage);
                
                s = inputManager;
            }
            //else if ( info.ioType == VertexInfo.IOType.STREAM )
            //{   
            //    DrDscInputStream input = new DrDscInputStream();

            //    DryadLogger.LogInformation("Create input node", "Opening DSC input fileset");

            //    err = input.Open(app.GetUniverse(), info.sources[0]);
            //    if (!SUCCEEDED(err))
            //    {
            //        string msg = String.Format("Could not read DSC input fileset {0}", info.sources[0]);
            //        throw new LinqToDryadException(msg, err);
            //    }

            //    DryadLogger.LogInformation("Create input node", "Opened DSC input fileset");

            //    DrManagerBase inputStage = new DrManagerBase(app.GetGraph(), inputName);
            //    DrInputStreamManager inputManager = new DrInputStreamManager(input, inputStage);

            //    s = inputManager;
            //}
            else if (info.ioType == VertexInfo.IOType.HDFS_STREAM)
            {
                DrHdfsInputStream input = new DrHdfsInputStream();

                DryadLogger.LogInformation("Create input node", "Opening HDFS input fileset");

                err = input.Open(app.GetUniverse(), info.sources[0]);
                if (!SUCCEEDED(err))
                {
                    string msg = String.Format("Could not read HDFS input fileset {0}: {1}", info.sources[0], input.GetError());
                    throw new LinqToDryadException(msg, err);
                }

                DryadLogger.LogInformation("Create input node", "Opened HDFS input fileset");

                DrManagerBase inputStage = new DrManagerBase(app.GetGraph(), inputName);
                DrInputStreamManager inputManager = new DrInputStreamManager(input, inputStage);

                s = inputManager;
            }
            else if (info.ioType == VertexInfo.IOType.AZUREBLOB)
            {
                DrAzureInputStream input = new DrAzureInputStream();

                DryadLogger.LogInformation("Create input node", "Opening Azure input fileset");

                try
                {
                    input.Open(info.sources[0]);
                }
                catch (Exception e)
                {
                    string msg = String.Format("Could not read Azure input fileset {0}: {1}", info.sources[0], e.ToString());
                    throw new LinqToDryadException(msg);
                }

                DryadLogger.LogInformation("Create input node", "Opened Azure input fileset");

                DrManagerBase inputStage = new DrManagerBase(app.GetGraph(), inputName);
                DrInputStreamManager inputManager = new DrInputStreamManager(input, inputStage);

                s = inputManager;
            }
            else
            {
                string msg = String.Format("Unknown input type {0}", info.ioType);
                throw new LinqToDryadException(msg);
            }

            DryadLogger.LogInformation("methodExit");
            return s;
        }

        private DrOutputStreamManager CreateOutputNode(DryadLINQApp app, VertexInfo info, string outputName) 
        {
            DryadLogger.LogInformation("methodEntry: " + outputName);

            DrOutputStreamManager s;
            if ( info.ioType == VertexInfo.IOType.PARTITIONEDFILE )
            {
                DrPartitionOutputStream output = new DrPartitionOutputStream();
                int err = output.Open(new Uri(info.sources[0]).AbsolutePath, info.partitionUncPath);
                if (!SUCCEEDED(err))
                {
                    string msg = String.Format("Could not open DSC output fileset {0}", info.sources[0]);
                    throw new LinqToDryadException(msg, err);
                }
                
                DrManagerBase outputStage = new DrManagerBase(app.GetGraph(), outputName);
                DrOutputStreamManager outputManager = new DrOutputStreamManager(output, outputStage);
                app.GetGraph().AddPartitionGenerator(outputManager);
                                
                s = outputManager;
            }
            //else if ( info.ioType == VertexInfo.IOType.STREAM )
            //{   
            //    DrDscOutputStream output = new DrDscOutputStream(info.compressionScheme, info.isTemporary);
            //    int err = 0;
            //    if (info.recordType == "")
            //    {
            //        err = output.Open(info.sources[0], info.partitionUncPath);
            //    }
            //    else
            //    {
            //        err = output.OpenWithRecordType(info.sources[0], info.partitionUncPath, info.recordType);
            //    }

            //    if (!SUCCEEDED(err))
            //    {
            //        string msg = String.Format("Could not open DSC output fileset {0}", info.sources[0]);
            //        throw new LinqToDryadException(msg, err);
            //    }
                
            //    DrManagerBase outputStage = new DrManagerBase(app.GetGraph(), outputName);
            //    DrOutputStreamManager outputManager = new DrOutputStreamManager(output, outputStage);
            //    app.GetGraph().AddPartitionGenerator(outputManager);

            //    s = outputManager;
            //}
            else if (info.ioType == VertexInfo.IOType.HDFS_STREAM)
            {
                DrHdfsOutputStream output = new DrHdfsOutputStream();
                int err = output.Open(info.sources[0]);

                if (!SUCCEEDED(err))
                {
                    string msg = String.Format("Could not open HDFS output fileset {0}: {1}", info.sources[0], output.GetError());
                    throw new LinqToDryadException(msg, err);
                }

                DrManagerBase outputStage = new DrManagerBase(app.GetGraph(), outputName);
                DrOutputStreamManager outputManager = new DrOutputStreamManager(output, outputStage);
                app.GetGraph().AddPartitionGenerator(outputManager);

                s = outputManager;
            }
            else if (info.ioType == VertexInfo.IOType.AZUREBLOB)
            {
                DrAzureOutputStream output = new DrAzureOutputStream();

                try
                {
                    output.Open(info.sources[0]);
                }
                catch (Exception e)
                {
                    string msg = String.Format("Could not open Azure output fileset {0}: {1}", info.sources[0], e.ToString());
                    throw new LinqToDryadException(msg);
                }

                DrManagerBase outputStage = new DrManagerBase(app.GetGraph(), outputName);
                DrOutputStreamManager outputManager = new DrOutputStreamManager(output, outputStage);
                app.GetGraph().AddPartitionGenerator(outputManager);

                s = outputManager;
            }
            else
            {
                string msg = String.Format("Unknown output type {0}", info.ioType);
                throw new LinqToDryadException(msg);
            }
            
            return s;
        }

        private DrVertexSet CreateVertexSet(DrGraph graph, DrVertex prototype, int copies)
        {
            DrVertexSet result = new DrVertexSet();

            for (int i = 0; i < copies; i++)
            {
                DrVertex v = prototype.MakeCopy(i);
                result.Add(v);
            }
            
            return result;
        }

        private DrVertexSet CreateVertexSet(DrGraph graph, DrInputStreamManager inputStream)
        {
            DrVertexSet result = new DrVertexSet();

            List<DrStorageVertex> vertices = inputStream.GetVertices();
            for (int i = 0; i < vertices.Count; i++)
            {
                DrVertex v = vertices[i];
                result.Add(v);
            }

            return result;
        }

        private DrVertexSet CreateVertexSet(DrGraph graph, DrOutputStreamManager outputStream, int parts)
        {
            DrVertexSet result = new DrVertexSet();

            outputStream.SetNumberOfParts(parts);

            List<DrOutputVertex> vertices = outputStream.GetVertices();
            for (int i = 0; i < vertices.Count; i++)
            {
                DrVertex v = vertices[i];
                result.Add(v);
            }

            return result;
        }

        private void CreateVertexSet(Vertex v, DryadLINQApp app, Query query, Dictionary<int, GraphStageInfo> graphStageMap)
        {
            SortedDictionary<int, Vertex> queryPlan = query.queryPlan;
            DrVertexSet nodes = null;
            DrStageManager newManager = null;
            
            app.GetGraph().GetParameters();
            
            DrGraphParameters parameters = app.GetGraph().GetParameters();
            string stdVertexName = "MW";
            
            if ( v.type == Vertex.Type.INPUTTABLE)
            {
                DrInputStreamManager input = CreateInputNode(app, v.info, v.name);
                newManager = input.GetStageManager();
                nodes = CreateVertexSet(app.GetGraph(), input);
            }
            else if ( v.type == Vertex.Type.OUTPUTTABLE )
            {
                DrOutputStreamManager output = CreateOutputNode(app, v.info, v.name);
                newManager = output.GetStageManager();
                nodes = CreateVertexSet(app.GetGraph(), output, v.partitions);
            }
            else if ( v.type == Vertex.Type.CONCAT )
            {
                newManager = new DrManagerBase(app.GetGraph(), v.name);

                // the set of nodes in a concat is just the set of nodes in the predecessor stages concatenated
                nodes = new DrVertexSet();
                foreach (Predecessor p in v.info.predecessors)
                {
                    GraphStageInfo value = null;
                    if (graphStageMap.TryGetValue(p.uniqueId, out value))
                    {
                        nodes.InsertRange(nodes.Count, value.members);
                    }
                    else
                    {
                        throw new LinqToDryadException(String.Format("Concat: Failed to find predecessor {0} in graph stage map", p.uniqueId));
                    }
                }
            }
            else
            {
                newManager = new DrManagerBase(app.GetGraph(), v.name);
                
                DrVertex vertex;
                if (v.type == Vertex.Type.TEE)
                {
                    DrTeeVertex teeVertex = new DrTeeVertex(newManager);                    
                    vertex = teeVertex;
                }
                else
                {
                    DrActiveVertex activeVertex = new DrActiveVertex(newManager, parameters.m_defaultProcessTemplate, parameters.m_defaultVertexTemplate);

                    activeVertex.AddArgument(stdVertexName);                    
                    activeVertex.AddArgument(v.info.assemblyName);
                    activeVertex.AddArgument(v.info.className);
                    activeVertex.AddArgument(v.info.methodName);
                    
                    vertex = activeVertex;
                }

                nodes = CreateVertexSet(app.GetGraph(), vertex, v.partitions);
                
                if (v.machines != null && v.machines.Length != 0 && v.type != Vertex.Type.TEE)
                {
                    for (int i=0; i<v.partitions; i++)
                    {
                        DrResource r = app.GetUniverse().LookUpResource(v.machines[i]);
                        
                        if (r != null)
                        {
                            DrVertex baseVertex = nodes[i];
                            DrActiveVertex activeVertex = baseVertex as DrActiveVertex;
                            
                            activeVertex.GetAffinity().SetHardConstraint(true);
                            activeVertex.GetAffinity().AddLocality(r);
                        }
                    }
                }

                FileStream mapfile = app.GetIdentityMapFile();
                if (mapfile != null)
                {
                    for (int i = 0; i < v.partitions; i++)
                    {
                        DrVertex jmv = nodes[i];
                        string message = String.Format("Mapping {0}[{1}] to {2}\n", v.uniqueId, i, jmv.GetId());
                        System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
                        mapfile.Seek(0, SeekOrigin.End);
                        mapfile.Write(encoding.GetBytes(message), 0, message.Length);
                    }
                }
            }

            graphStageMap.Add( v.uniqueId, new GraphStageInfo(nodes, v, newManager));
        }

        private void ConnectPointwise(DrVertexSet source, DrVertexSet destination, ChannelConnectorType type)
        {
            int numberOfSrc = source.Count;
            int numberOfDst = destination.Count;

            if (numberOfDst == 1)
            {
                DrVertex ov = destination[0];
                int inputs = ov.GetInputs().GetNumberOfEdges();

                ov.GetInputs().GrowNumberOfEdges(inputs + numberOfSrc);

                for (int i = 0; i < numberOfSrc; i++)
                {
                    DrVertex iv = source[i];

                    int outputs = iv.GetOutputs().GetNumberOfEdges();
                    iv.GetOutputs().GrowNumberOfEdges(outputs + 1);

                    iv.ConnectOutput(outputs, ov, inputs + i, (DrConnectorType)type);
                }
            }
            else if (numberOfSrc == 1)
            {
                DrVertex iv = source[0];
                int outputs = iv.GetOutputs().GetNumberOfEdges();

                iv.GetOutputs().GrowNumberOfEdges(outputs + numberOfDst);

                for (int i = 0; i < numberOfDst; i++)
                {
                    DrVertex ov = destination[i];

                    int inputs = ov.GetInputs().GetNumberOfEdges();
                    ov.GetInputs().GrowNumberOfEdges(inputs + 1);

                    iv.ConnectOutput(outputs + i, ov, inputs, (DrConnectorType)type);
                }
            }
            else if (numberOfSrc == numberOfDst)
            {
                for (int i = 0; i < numberOfDst; i++)
                {
                    DrVertex iv = source[i];
                    DrVertex ov = destination[i];

                    int outputs = iv.GetOutputs().GetNumberOfEdges();
                    int inputs = ov.GetInputs().GetNumberOfEdges();

                    iv.GetOutputs().GrowNumberOfEdges(outputs + 1);
                    ov.GetInputs().GrowNumberOfEdges(inputs + 1);

                    iv.ConnectOutput(outputs, ov, inputs, (DrConnectorType)type);
                }
            }
            else
            {
                throw new LinqToDryadException(String.Format("Source and destination do not match in pointwise connection. Source = {0}, destination = {1}", numberOfSrc, numberOfDst));
            }
        }

        private void ConnectCrossProduct(DrVertexSet source, DrVertexSet destination, ChannelConnectorType type)
        {
            int numberOfSrc = source.Count;
            int numberOfDst = destination.Count;

            // Allocate ports
            for (int i = 0; i < numberOfSrc; i++)
            {
                source[i].GetOutputs().SetNumberOfEdges(numberOfDst);
            }

            // Connect them up
            for (int destinationNumber = 0; destinationNumber < numberOfDst; destinationNumber++)
            {
                DrVertex dv = destination[destinationNumber];
                int inputs = dv.GetInputs().GetNumberOfEdges();
                dv.GetInputs().GrowNumberOfEdges(inputs + numberOfSrc);

                for (int sourceNumber = 0; sourceNumber < numberOfSrc; sourceNumber++)
                {
                    source[sourceNumber].ConnectOutput(destinationNumber, dv, inputs + sourceNumber, (DrConnectorType)type);
                }
            }
        }

        // Add the edges to nodes
        // The input ordering of each node is very important.
        private void AddEdges(GraphStageInfo destInfo, Dictionary<int, GraphStageInfo> graphStageMap)
        {
            Vertex destVertex = destInfo.vertex;
            int destId = destVertex.uniqueId;

            DrVertexSet destNodes = graphStageMap[ destId ].members;
            foreach (Predecessor p in destVertex.info.predecessors)
            {
                GraphStageInfo info = graphStageMap[ p.uniqueId ];
                Vertex sourceVertex = info.vertex; 
                
                if (destVertex.type != Vertex.Type.CONCAT)
                {
                    ChannelConnectorType channelType = ChannelConnectorType.DCT_File;
                    if ( p.channelType == Predecessor.ChannelType.DISKFILE )
                    {
                        if (destVertex.type == Vertex.Type.OUTPUTTABLE)
                        {
                            channelType = ChannelConnectorType.DCT_Output;
                        }
                        else
                        {
                            channelType = ChannelConnectorType.DCT_File;
                        }
                    }
                    else if ( p.channelType == Predecessor.ChannelType.MEMORYFIFO )
                    {
                        channelType = ChannelConnectorType.DCT_Fifo;
                    }
                    else if ( p.channelType == Predecessor.ChannelType.TCPPIPE )
                    {
                        channelType = ChannelConnectorType.DCT_Pipe;
                    }
                    else
                    {
                        string msg = String.Format("Unknown channel type {0}", p.channelType);
                        throw new LinqToDryadException(msg);
                    }
                    
                    DrVertexSet sourceNodes = info.members;
                    switch (p.connectionOperator)
                    {
                        case Predecessor.ConnectionOperator.CROSSPRODUCT:
                            ConnectCrossProduct(sourceNodes, destNodes, channelType);
                            break;
                        case Predecessor.ConnectionOperator.POINTWISE:
                            ConnectPointwise(sourceNodes, destNodes, channelType);
                            break;
                        default:
                            break;
                    }

                }
            }
        }
        
        public void BuildGraphFromQuery(DryadLINQApp app, Query query)
        {
            // set configurable properties
            int highThreshold = app.GetMaxAggregateInputs();
            int lowThreshold = 16;
            UInt64 highDataThreshold = (UInt64)app.GetAggregateThreshold();
            UInt64 lowDataThreshold = (3*highDataThreshold)/4;
            UInt64 maxSingleDataThreshold = highDataThreshold/2;
            int aggFilterThreshold = app.GetMaxAggregateFilterInputs();
            
            // use a graph stage map to store the vertices as they are created, grouped by stage.
            Dictionary<int, GraphStageInfo> graphStageMap = new Dictionary<int,GraphStageInfo>();

            DryadLogger.LogInformation("Building graph");

            //
            // Create a set of vertices for each vertex (stage) in the query plan
            //
            DryadLogger.LogInformation("Adding vertices");
            foreach (KeyValuePair<int, Vertex> kvp in query.queryPlan)
            {
                Vertex v = kvp.Value;
                GraphStageInfo value = null;

                if (!graphStageMap.TryGetValue(v.uniqueId, out value))
                {
                    DryadLogger.LogInformation(String.Format("Adding vertices for stage {0}", v.name));
                    CreateVertexSet(v, app, query, graphStageMap);
                }
            }

            //
            // Add dynamic stage managers
            //
            DryadLogger.LogInformation("Build Graph From Query", "Adding stage managers");
            foreach (KeyValuePair<int, GraphStageInfo> kvp in graphStageMap)
            {
                Vertex v = kvp.Value.vertex;

                //
                //There are no dynamic managers
                //
                if (v.dynamicManager == null)
                {
                    continue;
                }

                DrStageManager newManager = kvp.Value.stageManager;	   // newManager
                
                DrGraphParameters parameters = app.GetGraph().GetParameters();

                string stdVertexName = "MW";
                string cpyVertexName = "CP";
                
                if (v.type != Vertex.Type.INPUTTABLE && v.type != Vertex.Type.CONCAT)
                {
                    if (v.dynamicManager.type == DynamicManager.Type.SPLITTER)
                    {
                        if (v.info.predecessors.Length == 1)
                        {
                            DrPipelineSplitManager splitter = new DrPipelineSplitManager();
                            newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager, splitter);
                        }
                        else
                        {
                            DrSemiPipelineSplitManager splitter = new DrSemiPipelineSplitManager();
                            newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager, splitter);
                        }
                    }
                    else if (v.dynamicManager.type == DynamicManager.Type.PARTIALAGGR)
                    {
                        DrDynamicAggregateManager dynamicMerge = new DrDynamicAggregateManager();
                        
                        dynamicMerge.SetGroupingSettings(0, 0);
                        dynamicMerge.SetMachineGroupingSettings(2, aggFilterThreshold);
                        dynamicMerge.SetDataGroupingSettings(lowDataThreshold, highDataThreshold, maxSingleDataThreshold);
                        dynamicMerge.SetSplitAfterGrouping(true);

                        foreach (Predecessor p in v.info.predecessors)
                        {
                            newManager.AddDynamicConnectionManager(graphStageMap[p.uniqueId].stageManager, dynamicMerge);
                        }
                    }
                    else if (v.dynamicManager.type == DynamicManager.Type.FULLAGGR ||
                             v.dynamicManager.type == DynamicManager.Type.HASHDISTRIBUTOR)
                    {
                        int idx = 0;
                        int sz = v.dynamicManager.assemblyNames == null ? 0 : v.dynamicManager.assemblyNames.Length;
                        DrDynamicAggregateManager dynamicMerge = new DrDynamicAggregateManager();
                        
                        if (v.dynamicManager.type == DynamicManager.Type.FULLAGGR || sz > 1)
                        {
                            dynamicMerge = new DrDynamicAggregateManager();
                            
                            string name = v.dynamicManager.methodNames[idx];
                            DrManagerBase newStage = new DrManagerBase(app.GetGraph(), name);
                            
                            DrActiveVertex mergeVertex = new DrActiveVertex(newStage, parameters.m_defaultProcessTemplate, parameters.m_defaultVertexTemplate);
                            mergeVertex.AddArgument(stdVertexName);
                            
                            mergeVertex.AddArgument(v.dynamicManager.assemblyNames[idx]);
                            mergeVertex.AddArgument(v.dynamicManager.classNames[idx]);
                            mergeVertex.AddArgument(v.dynamicManager.methodNames[idx]);
                            
                            idx++;						  
                            dynamicMerge.SetInternalVertex(mergeVertex);
                            
                            dynamicMerge.SetGroupingSettings(0, 0);
                            dynamicMerge.SetPodGroupingSettings(lowThreshold, highThreshold);
                            dynamicMerge.SetDataGroupingSettings(lowDataThreshold,
                                highDataThreshold,
                                maxSingleDataThreshold);
                            dynamicMerge.SetMaxAggregationLevel(v.dynamicManager.aggregationLevels);						 
                        }
                        
                        if (v.dynamicManager.type == DynamicManager.Type.FULLAGGR)
                        {
                            newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager, dynamicMerge);
                        }
                        else
                        {
                            string name = v.dynamicManager.methodNames[idx];
                            DrManagerBase newStage = new DrManagerBase(app.GetGraph(), name);
                            
                            DrActiveVertex distributeVertex = new DrActiveVertex(newStage, parameters.m_defaultProcessTemplate, parameters.m_defaultVertexTemplate);
                            distributeVertex.AddArgument(stdVertexName);
                            
                            distributeVertex.AddArgument(v.dynamicManager.assemblyNames[idx]);
                            distributeVertex.AddArgument(v.dynamicManager.classNames[idx]);
                            distributeVertex.AddArgument(v.dynamicManager.methodNames[idx]);
                            
                            idx++;
                            
                            DrDynamicDistributionManager dynamicHashDistribute =
                                new DrDynamicDistributionManager(distributeVertex, dynamicMerge);
                            dynamicHashDistribute.SetDataPerVertex(highDataThreshold*2);  // 2GB
                            
                            newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager, dynamicHashDistribute);						   
                        }						 
                    }
                    else if (v.dynamicManager.type == DynamicManager.Type.RANGEDISTRIBUTOR)
                    {
                        DrStageManager splitManager = graphStageMap[v.dynamicManager.splitVertexId].stageManager;                        
                        DrDynamicRangeDistributionManager drdm = new DrDynamicRangeDistributionManager(splitManager, v.dynamicManager.sampleRate);
                        drdm.SetDataPerVertex(highDataThreshold*2);   // 2GB
                        newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager,drdm);
                    }
                    else if (v.dynamicManager.type == DynamicManager.Type.BROADCAST)
                    {
                        // the copy vertex
                        int bcastNumber = 0;
                        string nameString = String.Format("CP__{0}", bcastNumber++);                        
                        DrManagerBase newStage = new DrManagerBase(app.GetGraph(), nameString);
                        
                        DrActiveVertex copyVertex =
                            new DrActiveVertex(newStage,
                                               parameters.m_defaultProcessTemplate,
                                               parameters.m_defaultVertexTemplate);
                        copyVertex.AddArgument(cpyVertexName);
                        
                        DrDynamicBroadcastManager bcast = new DrDynamicBroadcastManager(copyVertex);
                        newManager.AddDynamicConnectionManager(graphStageMap[v.info.predecessors[0].uniqueId].stageManager, bcast);
                    }
                    else if (v.dynamicManager.type != DynamicManager.Type.NONE)
                    {
                        DryadLogger.LogWarning(String.Format("Dynamic manager type {0} not supported yet", v.dynamicManager.type));
                    }
                }
            }
            

            //
            // Add all the edges
            //
            DryadLogger.LogInformation("Build Graph From Query", "Adding edges");
            foreach (KeyValuePair<int, GraphStageInfo> kvp in graphStageMap)
            {
                AddEdges(kvp.Value, graphStageMap);
            }

            //
            // Register the actual created vertices with the graph
            //
            MaterializeToManagers(graphStageMap);
        }

        public void PrintGraph(DrGraph graph)
        {
            using (StreamWriter sw = new StreamWriter(new FileStream("toplogy.txt", FileMode.Create)))
            {
                List<DrStageManager> stages = graph.GetStages();

                foreach (DrStageManager s in stages)
                {
                    List<DrVertex> vertices = s.GetVertexVector();
                    foreach (DrVertex v in vertices)
                    {
                        sw.Write("{0} <= ", v.GetId());

                        DrEdgeHolder eh = v.GetInputs();
                        int n = eh.GetNumberOfEdges();

                        if (n == 0)
                        {
                            sw.Write("DSC");
                        }
                        else
                        {
                            for (int i = 0; i < n; i++)
                            {
                                DrVertex vin = eh.GetEdge(i).m_remoteVertex;
                                sw.Write("{0} ", vin.GetId());
                            }
                        }
                        sw.WriteLine();
                    }
                }
            }
        }

        public void MaterializeToManagers(Dictionary<int, GraphStageInfo> graphStageMap)
        {
            foreach (KeyValuePair<int, GraphStageInfo> kvp in graphStageMap)
            {
                // Skip CONCAT - it is just a collection of inputs from previous stages
                // and those vertices have already been registered
                if (kvp.Value.vertex.type != Vertex.Type.CONCAT)
                {
                    foreach (DrVertex v in kvp.Value.members)
                    {
                        v.GetStageManager().RegisterVertex(v);
                    }
                }
            }
        }
    }
    
} // namespace DryadLINQ
