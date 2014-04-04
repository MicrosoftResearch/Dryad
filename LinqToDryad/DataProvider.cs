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
using System.Collections.Specialized;
using System.Text;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Xml;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;
using System.IO.Compression;

using Microsoft.Research.Peloponnese.Storage;

namespace Microsoft.Research.DryadLinq
{
    // DataProvider is an abstraction for different data backends.
    public abstract class DataProvider
    {
        private static Dictionary<string, DataProvider> s_providers;

        static DataProvider()
        {
            s_providers = new Dictionary<string, DataProvider>();
            s_providers.Add(DataPath.HDFS_URI_SCHEME, new HdfsDataProvider());
            s_providers.Add(DataPath.PARTFILE_URI_SCHEME, new PartitionedFileDataProvider());
            s_providers.Add(DataPath.WASB_URI_SCHEME, new WasbDataProvider());
            s_providers.Add(DataPath.AZUREBLOB_URI_SCHEME, new AzureBlobDataProvider());
        }

        /// <summary>
        /// The prefix of this data provider.
        /// </summary>
        public abstract string Scheme { get; }
        
        public abstract DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri);
        public abstract DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri);
        public abstract Uri GetTempDirectory(DryadLinqContext context);

        private class DummyHiddenType { }

        public Uri RewriteUri(DryadLinqContext context, Uri dataSetUri)
        {
            return RewriteUri<DummyHiddenType>(context, dataSetUri);
        }

        public virtual Uri RewriteUri<T>(DryadLinqContext context, Uri dataSetUri)
        {
            return dataSetUri;
        }

        public abstract void Ingress<T>(DryadLinqContext context,
                                        IEnumerable<T> source,
                                        Uri dataSetName,
                                        DryadLinqMetaData metaData,
                                        CompressionScheme outputScheme,
                                        bool isTemp = false);

        public abstract Stream Egress(DryadLinqContext context, Uri dataSetUri);

        public abstract void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists);

        /// <summary>
        /// The path separator of this data provider.
        /// </summary>
        public virtual char PathSeparator
        {
            get { return '/'; }
        }

        public static void Register(string scheme, DataProvider provider)
        {
            if (s_providers.ContainsKey(scheme))
            {
                throw new DryadLinqException("Data provider for " + scheme + " has already existed.");
            }
            s_providers[scheme] = provider;
        }

        /// <summary>
        /// Get the data provider associated with a prefix.
        /// </summary>
        /// <param name="scheme">The data provider scheme</param>
        /// <returns>The data provider</returns>
        internal static DataProvider GetDataProvider(string scheme)
        {
            DataProvider provider;
            if (!s_providers.TryGetValue(scheme, out provider))
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             String.Format(SR.UnknownProvider, scheme));
            }
            return provider;
        }

        /// <summary>
        /// Get the dataset specified by a URI.
        /// </summary>
        /// <typeparam name="T">The record type of the dataset.</typeparam>
        /// <param name="dataSetUri">The URI of the dataset</param>
        /// <returns>A query object representing the dsc file set data.</returns>
        internal static DryadLinqQuery<T> GetPartitionedTable<T>(DryadLinqContext context, Uri dataSetUri)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            DryadLinqProvider queryProvider = new DryadLinqProvider(context);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            return new DryadLinqQuery<T>(null, queryProvider, dataProvider, dataSetUri);
        }

        // Egress data from store to client.
        public static IEnumerable<T> ReadData<T>(DryadLinqContext context, Uri dataSetUri)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            return new DryadLinqQueryEnumerable<T>(dataProvider, context, dataSetUri);
        }

        // Ingress any IEnumerable data.  Set the lease if it is temporary
        internal static DryadLinqQuery<T> StoreData<T>(DryadLinqContext context,
                                                       IEnumerable<T> source,
                                                       Uri dataSetUri,
                                                       DryadLinqMetaData metaData,
                                                       CompressionScheme outputScheme,
                                                       bool isTemp = false)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            dataProvider.Ingress(context, source, dataSetUri, metaData, outputScheme, isTemp);
            return DataProvider.GetPartitionedTable<T>(context, dataSetUri);
        }
    }

    public class DryadLinqStreamInfo
    {
        public Int32 PartitionCount { get; private set; }
        public Int64 DataSize { get; private set; }

        public DryadLinqStreamInfo(Int32 parCnt, Int64 size)
        {
            this.PartitionCount = parCnt;
            this.DataSize = size;
        }
    }

    internal class HdfsDataProvider : DataProvider
    {
        public override string Scheme
        {
            get { return DataPath.HDFS_URI_SCHEME; }
        }

        public override Uri GetTempDirectory(DryadLinqContext context)
        {
            UriBuilder builder = new UriBuilder(this.Scheme, context.DataNameNode, context.DataNameNodeDataPort);
            builder.Path = DataPath.TEMPORARY_STREAM_NAME_PREFIX;
            return builder.Uri;
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            throw new DryadLinqException("TBA");
        }

        public override DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri)
        {
            Int32 parCnt = 0;
            Int64 size = -1;
            context.DfsClient.GetContentSummary(dataSetUri.AbsolutePath, ref size, ref parCnt);
            if (parCnt == 0)
            {
                throw new DryadLinqException("Got 0 partition count for " + dataSetUri.AbsoluteUri);
            }
            return new DryadLinqStreamInfo(parCnt, size);
        }

        public override void Ingress<T>(DryadLinqContext context,
                                        IEnumerable<T> source,
                                        Uri dataSetUri,
                                        DryadLinqMetaData metaData,
                                        CompressionScheme outputScheme,
                                        bool isTemp = false)
        {
            throw new DryadLinqException("TBA");
        }

        public override Stream Egress(DryadLinqContext context, Uri dataSetUri)
        {
            throw new DryadLinqException("TBA");
        }

        public override void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists)
        {
            WebHdfsClient client = new WebHdfsClient(dataSetUri.Host, 8033, 50070);
            if (client.IsFileExists(dataSetUri.AbsolutePath))
            {
                if (deleteIfExists)
                {
                    client.DeleteDfsFile(dataSetUri.AbsolutePath);
                }
                else
                {
                    throw new DryadLinqException("Can't output to existing HDFS collection " + dataSetUri.AbsoluteUri);
                }
            }
        }
    }

    internal class PartitionedFileDataProvider : DataProvider
    {
        public override string Scheme
        {
            get { return DataPath.PARTFILE_URI_SCHEME; }
        }

        public override Uri GetTempDirectory(DryadLinqContext context)
        {
            UriBuilder builder = new UriBuilder();
            builder.Scheme = this.Scheme;
            string dataNameNode = context.DataNameNode;
            if (String.IsNullOrEmpty(dataNameNode))
            {
                dataNameNode = Environment.MachineName;
            }
            builder.Host = dataNameNode;
            builder.Path = DataPath.TEMPORARY_STREAM_NAME_PREFIX;
            return builder.Uri;
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            throw new DryadLinqException("TBA");
        }

        public override DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri)
        {
            string fileName = dataSetUri.LocalPath;
            var lines = File.ReadAllLines(fileName);
            if (lines.Length < 3)
            {
                throw new DryadLinqException("The partition file " + dataSetUri + " is malformed.");
            }
            Int32 parCnt = int.Parse(lines[1].Trim());
            Int64 size = 0;
            for (int i = 2; i < lines.Length; i++)
            {
                string[] fields = lines[i].Split(',');
                size += Int64.Parse(fields[1]);
            }
            return new DryadLinqStreamInfo(parCnt, size);
        }

        public override void Ingress<T>(DryadLinqContext context,
                                        IEnumerable<T> source,
                                        Uri dataSetUri,
                                        DryadLinqMetaData metaData,
                                        CompressionScheme compressionScheme,
                                        bool isTemp = false)
        {
            // Write the partition:
            string partPath = context.PartitionUncPath;
            if (partPath == null)
            {
                partPath = Path.GetDirectoryName(dataSetUri.LocalPath);
            }
            
            if (!Path.IsPathRooted(partPath))
            {
                partPath = Path.Combine("/", partPath);
            }
            partPath = Path.Combine(partPath, DryadLinqUtil.MakeUniqueName());
            Directory.CreateDirectory(partPath);
            partPath = Path.Combine(partPath, "Part.00000000");
            DryadLinqFactory<T> factory = (DryadLinqFactory<T>)DryadLinqCodeGen.GetFactory(context, typeof(T));
            using (FileStream fstream = new FileStream(partPath, FileMode.CreateNew, FileAccess.Write))
            {
                DryadLinqFileBlockStream nativeStream = new DryadLinqFileBlockStream(fstream, compressionScheme);
                DryadLinqRecordWriter<T> writer = factory.MakeWriter(nativeStream);
                foreach (T rec in source)
                {
                    writer.WriteRecordSync(rec);
                }
                writer.Close();
            }

            // Write the partfile:
            FileInfo finfo = new FileInfo(partPath);
            using (StreamWriter writer = File.CreateText(dataSetUri.LocalPath))
            {
                writer.WriteLine("thislineisignoredbecauseoftheoverride");
                writer.WriteLine("1");
                writer.WriteLine("{0},{1},{2}:{3}", 0, finfo.Length, Environment.MachineName, partPath.TrimStart('\\', '/'));
            }
        }

        public override Stream Egress(DryadLinqContext context, Uri dataSetUri)
        {
            string fileName = dataSetUri.LocalPath;
            var lines = File.ReadAllLines(fileName);
            if (lines.Length < 3)
            {
                throw new DryadLinqException("The partition file " + dataSetUri + " is malformed.");
            }
            bool isLocalPath = lines[0].Contains(':');
            string[] filePathArray = new string[lines.Length - 2];
            for (int i = 2; i < lines.Length; i++)
            {
                int idx = i - 2;
                string[] fields = lines[i].Split(',');
                if (fields[2].Contains(':'))
                {
                    string[] parts = fields[2].Split(':');
                    filePathArray[idx] = String.Format(@"\\{0}\{1}", parts[0], parts[1]);
                }
                else if (isLocalPath)
                {
                    filePathArray[idx] = String.Format("{0}.{1:X8}", lines[0], idx);
                }
                else
                {
                    filePathArray[idx] = String.Format(@"\\{0}\{1}.{2:X8}", fields[2], lines[0], idx);
                }
            }
            return new DryadLinqMultiFileStream(filePathArray, CompressionScheme.None);
        }

        public override void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists)
        {
            string fileName = dataSetUri.LocalPath;
            if (File.Exists(fileName))
            {
                if (deleteIfExists)
                {
                    File.Delete(fileName);
                }
                else
                {
                    throw new DryadLinqException("Can't output to existing Partitioned File collection " + dataSetUri.AbsoluteUri);
                }
            }
        }
    }

    internal class WasbDataProvider : HdfsDataProvider
    {
        public override string Scheme
        {
            get { return DataPath.WASB_URI_SCHEME; }
        }
    }

    internal class AzureBlobDataProvider : DataProvider
    {
        public override string Scheme
        {
            get { return DataPath.AZUREBLOB_URI_SCHEME; }
        }

        public override Uri GetTempDirectory(DryadLinqContext context)
        {
            return AzureUtils.ToAzureUri(context.AzureAccountName, 
                                         context.AzureAccountKey(context.AzureAccountName),
                                         context.AzureContainerName, 
                                         DataPath.TEMPORARY_STREAM_NAME_PREFIX);
        }

        public override Uri RewriteUri<T>(DryadLinqContext context, Uri dataSetUri)
        {
            string account, key, container, blob;
            AzureUtils.FromAzureUri(dataSetUri, out account, out key, out container, out blob);

            UriBuilder builder = new UriBuilder(dataSetUri);
            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(builder.Query);

            if (key == null)
            {
                query["key"] = context.AzureAccountKey(account);
            }

            if (typeof(T) == typeof(Microsoft.Research.DryadLinq.LineRecord))
            {
                query["seekBoundaries"] = "Microsoft.Research.DryadLinq.LineRecord";
            }

            builder.Query = query.ToString();

            return builder.Uri;
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            throw new DryadLinqException("TBA");
        }

        public override DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri)
        {
            Int32 parCnt = 1;
            Int64 size = -1;

            try
            {
                AzureCollectionPartition partition = new AzureCollectionPartition(dataSetUri);

                if (!partition.IsCollectionExists())
                {
                    throw new DryadLinqException("Input collection " + dataSetUri + " does not exist");
                }

                parCnt = partition.GetPartition().Count();
                size = partition.TotalLength;
            }
            catch (Exception e)
            {
                throw new DryadLinqException("Can't get Azure stream info for " + dataSetUri, e);
            }
 
            return new DryadLinqStreamInfo(parCnt, size);
        }

        public override void Ingress<T>(DryadLinqContext context,
                                        IEnumerable<T> source,
                                        Uri dataSetUri,
                                        DryadLinqMetaData metaData,
                                        CompressionScheme outputScheme,
                                        bool isTemp = false)
        {
            throw new DryadLinqException("TBA");
        }

        public override Stream Egress(DryadLinqContext context, Uri dataSetUri)
        {
            try
            {
                AzureCollectionPartition partition = new AzureCollectionPartition(dataSetUri);

                if (!partition.IsCollectionExists())
                {
                    throw new DryadLinqException("Input collection " + dataSetUri + " does not exist");
                }

                Stream dataSetStream = partition.GetReadStream();
                return dataSetStream;
            }
            catch (Exception e)
            {
                throw new DryadLinqException("Can't get Azure stream info for " + dataSetUri, e);
            }
        }

        public override void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists)
        {
            AzureCollectionPartition partition = new AzureCollectionPartition(dataSetUri);
            if (partition.IsCollectionExists())
            {
                if (deleteIfExists)
                {
                    partition.DeleteCollection();
                }
                else
                {
                    throw new DryadLinqException("Can't output to existing Azure Blob collection " + dataSetUri.AbsoluteUri);
                }
            }
        }
    }   
}
