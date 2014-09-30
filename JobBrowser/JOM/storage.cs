
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
using System.Linq;
using Microsoft.Research.Tools;

namespace Microsoft.Research.JobObjectModel
{

    /// <summary>
    /// Information descoverable statically by inspecting a partitioned table.
    /// </summary>
    public class StaticPartitionedTableInformation
    {
        /// <summary>
        /// File name.
        /// </summary>
        public string Name { get; protected set; }
        /// <summary>
        /// Provider that hosts this partitioned table.
        /// </summary>
        public string UriType { get; protected set; }
        /// <summary>
        /// URI used to reach the partitioned table; contains provider and options.
        /// </summary>
        public string Options { get; protected set; }
        /// <summary>
        /// Basic URI, except provider and options.
        /// </summary>
        public string Uri { get; protected set; }
        /// <summary>
        /// Number of partitions.  Could be -1 if the number of partitions is unknown.
        /// </summary>
        public long PartitionCount { get; internal set; }
        /// <summary>
        /// Estimated size of the contents; could be -1 if the size is unknown.
        /// </summary>
        public long EstimatedSize { get; internal set; }
        /// <summary>
        /// In case of error this is the message.
        /// </summary>
        public string Error { get; internal set; }
        /// <summary>
        /// Short name of the table.
        /// </summary>
        public string Header { get; protected set; }

        /// <summary>
        /// Cluster where the job accessing the stream resides.
        /// </summary>
        ClusterConfiguration Config { get; set; }

        private List<StaticPartitionInformation> partitions;

        /// <summary>
        /// Static information about each partition.
        /// </summary>
        public struct StaticPartitionInformation
        {
            /// <summary>
            /// Number of the partition.
            /// </summary>
            public int PartitionNumber { get; private set; }
            /// <summary>
            /// Size of partition.
            /// </summary>
            public long PartitionSize { get; private set; }
            /// <summary>
            /// Copies of the replica: either a number or one machine name.
            /// </summary>
            public string Copies { get; private set; }

            /// <summary>
            /// Crate information about a partition.
            /// </summary>
            /// <param name="size">Partition size.</param>
            /// <param name="number">Partition number.</param>
            /// <param name="replicas">Number of replicas.</param>
            public StaticPartitionInformation(int number, long size, int replicas)
                : this()
            {
                this.PartitionNumber = number;
                this.PartitionSize = size;
                this.Copies = replicas.ToString();
            }

            /// <summary>
            /// Crate information about a partition.
            /// </summary>
            /// <param name="size">Partition size.</param>
            /// <param name="number">Partition number.</param>
            /// <param name="location">Location of the unique replica.</param>
            public StaticPartitionInformation(int number, long size, string location)
                : this()
            {
                this.PartitionNumber = number;
                this.PartitionSize = size;
                this.Copies = location;
            }
        }
        /// <summary>
        /// Information about the partitions.
        /// </summary>
        public IEnumerable<StaticPartitionInformation> Partitions { get { return this.partitions; } }

        /// <summary>
        /// Add a new partition.
        /// </summary>
        /// <param name="spi">Partition to add.</param>
        public void AddPartition(StaticPartitionInformation spi)
        {
            this.partitions.Add(spi);
        }

        /// <summary>
        /// Save here information about the constructor invocation arguments; used by Refresh().
        /// </summary>
        private class SaveConstructorArguments
        {
            public string[] code;
            public DryadJobStaticPlan plan;
            public DryadJobStaticPlan.Stage source;
        }

        /// <summary>
        /// Save here information about the constructor invocation arguments.
        /// </summary>
        SaveConstructorArguments constructorArguments;

        /// <summary>
        /// Code attached to the stage (if any).
        /// </summary>
        public string[] Code { get { return this.constructorArguments.code; } }

        /// <summary>
        /// Empty information about a partitioned table.
        /// </summary>
        private StaticPartitionedTableInformation()
        {
            this.constructorArguments = null;
            this.Config = null;
            this.UriType = "";
            this.Error = "";
            this.Name = "";
            this.Header = "";

            this.PartitionCount = -1;
            this.partitions = new List<StaticPartitionInformation>();
            this.EstimatedSize = -1;
        }

        /// <summary>
        /// Create a class representing the set of edges between two stages in the job plan.
        /// These collectively look like a partitioned table.
        /// If the source stage has multiple outputs there is not enough information to return meaningful information.
        /// </summary>
        /// <param name="job">Job whose slice we are displaying.</param>
        /// <param name="plan">Static plan of the job.</param>
        /// <param name="source">Stage in the job which produces the data.</param>
        /// <param name="status">Delegate used to report errors.</param>
        /// <param name="showCancelled">If true include cancelled vertices.</param>
        public static StaticPartitionedTableInformation StageOutput(
            DryadLinqJobInfo job,
            DryadJobStaticPlan plan,
            DryadJobStaticPlan.Stage source,
            StatusReporter status,
            bool showCancelled)
        {
            string header = "Output of " + source.Name;

            // First check whether in the static plan this is virtual
            while (source.IsTee)
            {
                var sourceInputs = plan.GetStageConnections(source, true).ToList();
                if (sourceInputs.Count() != 1)
                    throw new DryadException("Unexpected number of inputs for stage " + source.Name);
                source = sourceInputs.First().From;
            }

            // If we reached the input return information about that input
            if (source.IsInput)
            {
                status("Scanning " + source.Name, StatusKind.LongOp);
                StaticPartitionedTableInformation result = new StaticPartitionedTableInformation(job.ClusterConfiguration, source.UriType, source.Uri, source.Code, status);
                result.Header = "Output of " + header;
                result.constructorArguments = new SaveConstructorArguments
                {
                    code = null,
                    source = source,
                    plan = plan
                };
                return result;
            }
            else
            {
                StaticPartitionedTableInformation result = new StaticPartitionedTableInformation();
                result.Name = "Output of vertices in stage " + source.Name;
                result.Header = "Output of " + header;
                result.constructorArguments = new SaveConstructorArguments
                {
                    code = null,
                    source = source,
                    plan = plan
                };

                // Check whether this stage has multiple outputs; this can only happen for 'Fork' operators.
                var destinations = plan.GetStageConnections(source, false);
                if (destinations.Count() > 1)
                {
                    result.Error = "Cannot provide information about one of multiple outputs of a stage.";
                    return result;
                }

                DryadLinqJobStage stage = job.GetStage(source.Name);
                if (stage == null)
                {
                    result.Error = "There is no information about the output of stage " + source.Name;
                    return result;
                }

                result.EstimatedSize = 0;
                result.PartitionCount = stage.TotalInitiatedVertices;
                int count = 0;
                foreach (ExecutedVertexInstance vi in stage.Vertices)
                {
                    if (vi.State == ExecutedVertexInstance.VertexState.Successful ||
                        vi.State == ExecutedVertexInstance.VertexState.Failed ||
                        (showCancelled && vi.State == ExecutedVertexInstance.VertexState.Cancelled))
                    {
                        StaticPartitionInformation spi = new StaticPartitionInformation(count++, vi.DataWritten, vi.Name + " v." + vi.Version);
                        result.AddPartition(spi);
                        if (vi.DataWritten != -1)
                            result.EstimatedSize += vi.DataWritten;
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Create a class to discover the information about a partitioned table given its uri.
        /// </summary>
        /// <param name="uri">Partitioned table uri.</param>
        /// <param name="uriType">Type of URI.</param>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        /// <param name="code">Code associated to the stage (the uri does not contain the options, they may still be in the code).</param>
        /// <param name="config">Cluster where the job accessing the stream resides.</param>
        public StaticPartitionedTableInformation(ClusterConfiguration config, string uriType, string uri, string[] code, StatusReporter statusReporter)
        {
            this.Config = config;
            this.UriType = uriType;
            this.Error = "";
            this.constructorArguments = new SaveConstructorArguments
            {
                // this is all we need
                code = code
            };

            // really ugly, but the uri in the table does not longer contain the options; they were stripped by the DryadLINQ compiler.
            if (code.Length > 0)
            {
                string firstline = code[0];
                firstline = firstline.Trim('[', ']');
                if (firstline.StartsWith("PartitionedTable:"))
                    firstline = firstline.Substring("PartitionedTable:".Length).Trim();
                if (firstline.StartsWith(uri))
                    uri = firstline; // this may contain the options.
            }

            int indexoptions = uri.IndexOf("?");
            if (indexoptions > 0)
            {
                this.Uri = uri.Substring(0, indexoptions);
                this.Options = uri.Substring(indexoptions + 1);
            }
            else
            {
                this.Uri = uri;
                this.Options = "";
            }

            this.Name = Path.GetFileName(this.Uri);

            // default values
            this.PartitionCount = -1;
            this.partitions = new List<StaticPartitionInformation>();
            this.EstimatedSize = -1;
            this.Header = Path.GetFileName(this.Uri);

            switch (uriType)
            {
                case "PartitionedFile":
                    this.ParsePartitionedFile(statusReporter);
                    break;
            }
        }

        /// <summary>
        /// Generate a new view of the same partitioned table.
        /// </summary>
        /// <returns>The new view.</returns>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        /// <param name="job">Job containing the table.</param>
        /// <param name="showCancelled">Show the cancelled vertices.</param>
        public StaticPartitionedTableInformation Refresh(DryadLinqJobInfo job, StatusReporter statusReporter, bool showCancelled)
        {
            if (this.constructorArguments.code != null)
                return new StaticPartitionedTableInformation(this.Config, this.UriType, this.Uri, this.constructorArguments.code, statusReporter);
            else
                return StageOutput(job, this.constructorArguments.plan, this.constructorArguments.source, statusReporter, showCancelled);
        }



        /// <summary>
        /// The partitioned table is a partitioned file.
        /// </summary>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        private void ParsePartitionedFile(StatusReporter statusReporter)
        {
            this.EstimatedSize = 0;
            try
            {
                if (!File.Exists(this.Uri))
                {
                    this.Error = "File not found";
                    statusReporter("Cannot find file " + this.Uri, StatusKind.Error);
                    return;
                }

                PartitionedFileMetadata pfi = new PartitionedFileMetadata(new UNCPathname(this.Uri));
                this.PartitionCount = pfi.NumberOfPartitions;
                foreach (var p in pfi.Partitions)
                {
                    StaticPartitionInformation spi = new StaticPartitionInformation(p.Number, p.Size, p.NumberOfReplicas);
                    this.partitions.Add(spi);
                    if (spi.PartitionSize >= 0)
                        this.EstimatedSize += spi.PartitionSize;
                }
            }
            catch (Exception ex)
            {
                this.Error = ex.Message;
            }
        }
    }

}
