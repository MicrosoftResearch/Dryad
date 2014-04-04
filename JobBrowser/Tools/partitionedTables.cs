
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

namespace Microsoft.Research.Calypso.Tools
{
    using System.Collections.Generic;
    using System.IO;
    using System;
    using System.Text;
    using System.Linq;

    /// <summary>
    /// Exceptions thrown during operations on partitioned files.
    /// </summary>
    public sealed class PartitionedFileException : Exception
    {
        /// <summary>
        /// Create an exception for processing partitioned files.
        /// </summary>
        /// <param name="message">Message to throw.</param>
        public PartitionedFileException(string message)
            : base(message) { }
    }

    /// <summary>
    /// Representation for the metadata of a partitioned file (provider file://).
    /// </summary>
    public class PartitionedFileMetadata
    {
        private List<Partition> partitions;

        /// <summary>
        /// How many partitions does the table have?
        /// </summary>
        public int NumberOfPartitions { get { return this.partitions.Count; } }

        /// <summary>
        /// The partitions that compose the file.
        /// </summary>
        public IEnumerable<Partition> Partitions { get { return this.partitions; } }

        /// <summary>
        /// One partition of a replicated partitioned file.
        /// </summary>
        public class Partition
        {
            int partitionNumber;
            long size;
            List<UNCPathname> replicas;

            /// <summary>
            /// Copy information from another partition.
            /// </summary>
            /// <param name="other">Partition to copy.</param>
            public Partition(Partition other)
            {
                this.partitionNumber = other.partitionNumber;
                this.size = other.size;
                this.replicas = new List<UNCPathname>();
                this.replicas.AddRange(other.replicas);
            }

            /// <summary>
            /// Create a Partition object by parsing a PartitionedTable metadata file line.
            /// </summary>
            /// <param name="metadataline">A line from a metadata for a partitionedTable.</param>
            /// <param name="defaultprefix">Prefix for partition from metadata.</param>
            internal Partition(string metadataline, string defaultprefix)
            {
                string[] parts = metadataline.Split(',');
                if (parts.Length < 3)
                    throw new PartitionedFileException("Cannot parse metadata line " + metadataline);
                partitionNumber = int.Parse(parts[0]);
                size = long.Parse(parts[1]);
                replicas = new List<UNCPathname>(parts.Length - 2);
                for (int i = 2; i < parts.Length; i++)
                {
                    string[] md = parts[i].Split(':');
                    string machine = md[0];
                    string path;
                    if (md.Length == 2)
                        path = md[1];
                    else
                        path = string.Format("{0}.{1:X8}", defaultprefix, partitionNumber);
                    if (md.Length > 2)
                        throw new PartitionedFileException("Cannot understand pathname to file partition in " + metadataline);
                    this.AddReplica(new UNCPathname(machine, path));
                }
            }

            /// <summary>
            /// Create a partition from basic information.
            /// </summary>
            /// <param name="partitionNo">Partition number.</param>
            /// <param name="partitionSize">Size of partition.</param>
            /// <param name="machine">Machine storing the partition.</param>
            /// <param name="prefix">Prefix of the partition (global per partitioned file)</param>
            public Partition(int partitionNo, long partitionSize, string machine, string prefix)
            {
                this.partitionNumber = partitionNo;
                this.size = partitionSize;
                this.replicas = new List<UNCPathname>();
                this.replicas.Add(new UNCPathname(machine, string.Format("{0}.{1:X8}", prefix, partitionNo)));
            }

            /// <summary>
            /// The unique machine for a single-replica partition.
            /// </summary>
            public string Machine
            {
                get
                {
                    if (replicas.Count > 1)
                        throw new ArgumentException("query for machine in replicated partition");
                    return replicas[0].Machine;
                }
            }

            /// <summary>
            /// Add a new replica to a partition.
            /// </summary>
            /// <param name="path">Pathname to the replica to add.</param>
            public void AddReplica(UNCPathname path)
            {
                replicas.Add(path);
            }

            /// <summary>
            /// Representation of the partition as used in a metadata file.
            /// </summary>
            /// <returns>A string suitable for insertion in a metadata file.</returns>
            public override string ToString()
            {
                StringBuilder builder = new StringBuilder();
                builder.AppendFormat("{0},{1}", partitionNumber, size);
                foreach (UNCPathname p in replicas)
                {
                    builder.AppendFormat(",{0}:{1}", p.Machine, p.DirectoryAndFilename);
                }
                return builder.ToString();
            }

            /// <summary>
            /// The prefix of the partition; this only makes sense if the partition is in the canonical form.
            /// </summary>
            public string Prefix
            {
                get
                {
                    string filename = this.replicas[0].DirectoryAndFilename;
                    // remove file extension
                    int index = filename.LastIndexOf('.');
                    return filename.Substring(0, index);
                }
            }

            /// <summary>
            /// This form is used when all partitions have the same prefix.
            /// </summary>
            /// <returns>A short description of the list of replicas.</returns>
            public string ToShortString()
            {
                StringBuilder builder = new StringBuilder();
                builder.AppendFormat("{0},{1}", partitionNumber, size);
                foreach (UNCPathname p in replicas)
                {
                    builder.AppendFormat(",{0}", p.Machine);
                }
                return builder.ToString();
            }

            /// <summary>
            /// One of the replicas.
            /// </summary>
            /// <param name="index">The index of the replica.</param>
            /// <returns>The index-th replica.</returns>
            public UNCPathname Replica(int index)
            {
                return replicas[index];
            }

            /// <summary>
            /// Partition number.
            /// </summary>
            public int Number { get { return this.partitionNumber; } }

            /// <summary>
            /// Partition size.
            /// </summary>
            public long Size { get { return this.size; } }

            /// <summary>
            /// Number of replicas of this partition.
            /// </summary>
            public int NumberOfReplicas { get { return this.replicas.Count; } }
        }

        /// <summary>
        /// Create a partitioned table to represent a list of partitions.
        /// </summary>
        /// <param name="partitions">List of partitions to represent.</param>
        public PartitionedFileMetadata(IEnumerable<Partition> partitions)
        {
            this.partitions = partitions.ToList();
        }

        /// <summary>
        /// Create an empty metadata.
        /// </summary>
        public PartitionedFileMetadata()
        {
            this.partitions = new List<Partition>();
        }

        /// <summary>
        /// Add a new partition to the file.
        /// </summary>
        /// <param name="partition">Partition to add.</param>
        public void Add(Partition partition)
        {
            this.partitions.Add(partition);
        }

        /// <summary>
        /// Create a PartitionedTable metadata file from a list of partitions.
        /// </summary>
        /// <param name="metadataFile">Pathname for file containing the metadata.</param>
        /// <returns>The URI to use to read this metadata file.</returns>
        public string CreateMetadataFile(UNCPathname metadataFile)
        {
            // compute the prefix of the first partition
            string prefix = partitions.First().Prefix;

            Utilities.EnsureDirectoryExistsForFile(metadataFile.ToString());
            StreamWriter sw = new StreamWriter(metadataFile.ToString());
            sw.WriteLine(prefix);
            sw.WriteLine(this.partitions.Count);
            foreach (var p in this.partitions)
                sw.WriteLine(p.ToShortString());
            sw.Close();

            return Uri.UriSchemeFile + @"://" + metadataFile;
        }

        /// <summary>
        /// Read the metadata from a partitioned file.
        /// </summary>
        /// <param name="metadataFile">Metadata file to read.</param>
        public PartitionedFileMetadata(UNCPathname metadataFile)
        {
            StreamReader sr = new StreamReader(metadataFile.ToString());
            string defaultPrefix = sr.ReadLine();
            string str = sr.ReadLine();
            if (string.IsNullOrEmpty(str))
                throw new InvalidDataException("Expected a partitioned count, found none");
            int partitionCount = int.Parse(str);
            partitions = new List<Partition>(partitionCount);

            for (int i = 0; i < partitionCount; i++)
                this.Add(new Partition(sr.ReadLine(), defaultPrefix));
        }

        /// <summary>
        /// All the machines referred in the partitioned file.
        /// </summary>
        /// <returns>The list of partitions.</returns>
        public IEnumerable<string> AllMachines()
        {
            return partitions.Select(part => part.Machine).Distinct().OrderBy(machine => machine);
        }
    }
}
