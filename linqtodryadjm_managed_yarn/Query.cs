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

namespace linqtodryadjm_managed
{
    public class DynamicManager
    {
        public enum Type
        {
            NONE = 0,
            SPLITTER,
            PARTIALAGGR,
            FULLAGGR,
            HASHDISTRIBUTOR,
            RANGEDISTRIBUTOR,
            BROADCAST,
        };
        public Type type;
        
        public string[] assemblyNames; // dll file name of vertex entry code
        public string[] classNames;    // class name of vertex entry code 
        public string[] methodNames;   // method name of vertex entry code
         
        public double sampleRate;      // For range distributor only
        public int splitVertexId;      // For range distributor only
        public int aggregationLevels;  // For aggregators
    };

    
    public class Predecessor
    {
        public enum ConnectionOperator
        {
            POINTWISE = 0,
            CROSSPRODUCT
        };

        public enum ChannelType
        {
            DISKFILE = 0,
            TCPPIPE,
            MEMORYFIFO
        };

        public enum AffinityConstraint
        {
            UseDefault = 0,
            HardConstraint,
            OptimizationConstraint,
            Preference,
            DontCare
        };

        public int uniqueId;
        public ConnectionOperator connectionOperator;
        public ChannelType channelType; 
        public AffinityConstraint constraint; 

    };

    
    public class VertexInfo
    {
        public enum IOType
        {
            FILELIST = 0,  // always just one file
            FILEDIRECTORY, // only for input
            FILEWILDCARD,  // only for input
            STREAM,
            HDFS_STREAM,
            PARTITIONEDFILE, 
            FILEPREFIX     // only for output            
        };

        public IOType ioType;
        public Predecessor[] predecessors = new Predecessor[0];
        
        // for tables-type vertices only
        public string[] sources;           // fully qualified URI of output

        // for partitioned output table type vertices only
        public string partitionUncPath;  // fully-qualified network URI path
        
        // True iff the output is a temp dataset
        public bool isTemporary;
        
        // for general vertices
        public string assemblyName;      // dll file name of vertex entry code
        public string className;         // class name of vertex entry code 
        public string methodName;        // method name of vertex entry code

        // for OUTPUTTABLE storage set, this is the record type
        public string recordType;
        // for OUTPUTTABLE storage set, this is the compresssion mode
        public int compressionScheme;
        
    };
    

    public class Vertex
    {
        public enum Type
        {
            UNKNOWN = -1,
            UNUSED = 0,
            INPUTTABLE,
            OUTPUTTABLE,
            WHERE,
            JOIN,
            FORK,
            TEE,
            CONCAT,
            SUPER
        };
        public Type type;

        public int uniqueId;       // id number (and position in plan - starts from zero)
        public string name;           // pretty-printing name
        public int partitions;     // partitions 
        public VertexInfo info;           // (vertex-type specific)additional info about this vertex
        public DynamicManager dynamicManager;
        public string[] machines;
    };

    public class Query
    {
        public string compilerversion = "";    // version of DryadLinq compiler
        public string clusterName = "";        // name of dryad cluster to run on
        public string[] xmlExecHostArgs = new string[0];    // app defined command-line args for XmlExecHost
        public string visualization = "";      // control of visualization
        public int intermediateDataCompression = 0;  //YARN       // compression scheme for intermediate data
        public SortedDictionary<int, Vertex> queryPlan = new SortedDictionary<int, Vertex>();          // DAG of numbered vertices
        public bool enableSpeculativeDuplication = true;
    };

} // namespace DryadLINQ
