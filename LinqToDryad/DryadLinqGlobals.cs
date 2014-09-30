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
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Reflection;
using System.Linq;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// This class consists of static properties and fields that control the
    /// details of the DryadLinq configuration. 
    /// </summary>
    internal class StaticConfig
    {
        internal const string DryadHomeVar = "DRYAD_HOME";

        internal static bool UseLargeBuffer = false;

        internal const int NoDynamicOpt = 0;
        internal const int DynamicBroadcastLevel = 0x1;
        internal const int DynamicAggregateLevel = 0x2;
        internal const int DynamicRangePartitionLevel = 0x4;
        internal const int DynamicHashPartitionLevel = 0x8;
        internal const int DynamicCoRangePartitionLevel = 0x10;

        /// <summary>
        /// Runtime optimization: mostly affects dynamic resource management.  
        /// </summary>  
        internal static int DynamicOptLevel = DynamicBroadcastLevel;

        /// <summary>
        /// Number of partitions to use when doing static resource management.
        /// </summary>
        //@@TODO: dynamic re-partitioning of multiple inputs should use CoHashPartition or CoRangePartition
        //        both are in MSR private repositories, and Yuan recommends the use of CoRangePartition
        //        as it produces more balanced partitioning.
        //        If/when this work is done, DefaultPartitionCount should be retired.
        internal static int DefaultPartitionCount = 8;

        /// <summary>
        /// Used by concat to determine whether to repartition the data down to fewer partitions.
        /// </summary>
        //@@TODO: review and probably choose a more sensible value for this.
        //        Choosing a smaller value should break anything.. just forces repartitioning rather 
        //        than a plain-old concatenation of the source partitions.
        internal static int MaxPartitionCount = 20000;

        /// <summary>
        /// Use in memory FIFOs where appropriate.
        /// </summary>  
        internal static bool UseMemoryFIFO = false;

        /// <summary>
        /// Stop execution of vertices in a debugger.
        /// </summary>
        internal static bool LaunchDebugger = false;

        // Specifies whether object fields can have null values. 
        // Setting AllowNullFields to true allows all object fields to have
        // null values. A field can be specified as non-nullable by [Nullable(false)].
        // If AllowNullFields is false, all object fields are treated as non-nullable.
        // A field can be specified as nullable by [Nullable(true)].
        internal static bool AllowNullFields = false;
        internal static bool AllowNullArrayElements = false;

        /// <summary>
        /// Specifies whether records can have null values. 
        /// </summary>
        /// <remarks>
        /// Setting AllowNullRecords to true allows all records to have null values.
        /// A class can be specified as non-nullable by [Nullable(false)].
        /// If AllowNullRecords is false, all records are treated as non-nullable.
        /// A class can be specified as nullable by [Nullable(true)].
        /// </remarks>
        internal static bool AllowNullRecords = false;

        /// <summary>
        /// Allows records to be serialized and retain their concrete type identity
        /// </summary>
        /// <remarks>
        /// If false, records in an IQueryable{T} will be treated as records of type T regardless of their concrete type.
        /// </remarks>
        internal static bool AllowAutoTypeInference = false;

        /// <summary>                
        /// Specifies whether to use aggregation tree to work around some SMB limitation.
        /// </summary>
        internal static bool UseSMBAggregation = false;

        /// <summary>
        /// Specifies whether to use partial reduction for Distinct.
        /// </summary>
        internal static bool UsePartialDistinct = false;

        /// <summary>
        /// The maximum number of seconds to wait between polling the cluster to see if a job has completed.
        /// </summary>
        internal static int JobCompletionMaxPollInterval = 20;
        
        // The local reduction strategy used by GroupBy.
        internal static CombinerKind GroupByReduceStrategy = CombinerKind.PartialHash;
        internal static bool GroupByDynamicReduce = false;
        internal static bool GroupByLocalAggregationIsPartial = true;

        /// <summary>
        /// Additional DryadLinq related command line arguments for XmlExecHost.exe.
        /// </summary>
        private static string XmlExecHostArgsAdditional = String.Empty;
        internal static string XmlExecHostArgs
        {
            get {
                return XmlExecHostArgsAdditional;
            }
        }
        internal static void AddXmlExecHostArg(string arg)
        {
            XmlExecHostArgsAdditional += arg;
        }

        internal static TimeSpan LeaseDurationForTempFiles = new TimeSpan(1, 0, 0, 0); // 1 day.
    }

    /// <summary>
    /// Specifies a reduction strategy for GroupBy.
    /// </summary>
    internal enum CombinerKind
    {
        /// <summary>
        /// Partial sort GroupBy.
        /// </summary>
        PartialSort,
        /// <summary>
        /// Partial hash GroupBy.
        /// </summary>
        PartialHash,
        /// <summary>
        /// Full hash GroupBy.
        /// </summary>
        FullHash
    }

    /// <summary>
    /// Contains references to class/method/field names that are referenced via reflection.
    /// This is intended to assist with refactoring that may break reflection.
    ///
    /// NOTE: this list will probably never be complete. 
    ///       - A method mentioned here is definitely accessed via reflection
    ///       - Do not assume that methods not listed here are not accessed via Reflection.
    /// </summary>
    internal class ReflectedNames
    {
        internal const string DataProvider_GetPartitionedTable = "GetPartitionedTable";
        internal const string DLQ_ToStoreInternal = "ToStoreInternal";
        internal const string DLQ_ToStoreInternalAux = "ToStoreInternalAux";
    }
}
