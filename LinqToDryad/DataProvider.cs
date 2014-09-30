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

using Microsoft.Research.Peloponnese.Azure;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Specifies whether to compress the output of a DryadLINQ vertex.
    /// </summary>
    public enum CompressionScheme
    {
        /// <summary>
        /// No compression.
        /// </summary>
        None,

        /// <summary>
        /// Compression using gzip.
        /// </summary>
        Gzip
    }

    /// <summary>
    /// DataProvider provides an abstraction for different data backends. New data storage backends
    /// could be added by subclassing this class.
    /// </summary>
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
        /// The scheme of this data provider.
        /// </summary>
        public abstract string Scheme { get; }

        /// <summary>
        /// Gets the metadata of a specified dataset.
        /// </summary>
        /// <param name="context">A DryadLinqConext object.</param>
        /// <param name="dataSetUri">The URI of the dataset.</param>
        /// <returns>The metadata. Returns null if metadats is not present.</returns>
        public abstract DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri);

        /// <summary>
        /// Gets information of a specified dataset.
        /// </summary>
        /// <param name="context">A DryadLinqContext object.</param>
        /// <param name="dataSetUri">The URI of the dataset.</param>
        /// <returns>Information about a dataset.</returns>
        public abstract DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri);

        /// <summary>
        /// Gets a URI to store a temporary dataset.
        /// </summary>
        /// <param name="context">A DryadLinqContext object.</param>
        /// <param name="path">A local path.</param>
        /// <returns>The URI of a temporary directory.</returns>
        public abstract Uri GetTemporaryStreamUri(DryadLinqContext context, string path);

        /// <summary>
        /// Rewrites the URI of a dataset. Allows DataProvider specific rewriting.
        /// </summary>
        /// <typeparam name="T">The element type of the specified dataset</typeparam>
        /// <param name="context">The current DryadLinqContext.</param>
        /// <param name="dataSetUri">The URI of the dataset.</param>
        /// <param name="access">The intended access to the dataset.</param>
        /// <returns>The rewritten URI of the dataset.</returns>
        public virtual Uri RewriteUri<T>(DryadLinqContext context, 
                                         Uri dataSetUri, 
                                         FileAccess access = FileAccess.Read)
        {
            return dataSetUri;
        }

        /// <summary>
        /// Ingress a .NET collection to a specified store location.
        /// </summary>
        /// <typeparam name="T">The record type of the collection.</typeparam>
        /// <param name="context">An instance of DryadLinqContext.</param>
        /// <param name="source">The collection to be ingressed.</param>
        /// <param name="dataSetUri">The URI to store the collection.</param>
        /// <param name="metaData">The metadata for the collection.</param>
        /// <param name="outputScheme">The compression scheme used to store the collection.</param>
        /// <param name="isTemp">true to only store the collection temporarily with a time lease.</param>
        /// <param name="serializer">A stream-based serializer.</param>
        public abstract void Ingress<T>(DryadLinqContext context,
                                        IEnumerable<T> source,
                                        Uri dataSetUri,
                                        DryadLinqMetaData metaData,
                                        CompressionScheme outputScheme,
                                        bool isTemp,
                                        Expression<Action<IEnumerable<T>, Stream>> serializer);

        /// <summary>
        /// Creates an instance of Stream for a dataset at a specified location. This is
        /// used by DryadLINQ to read a .NET collection from a store.
        /// </summary>
        /// <param name="context">An instance of DryadLinqContext.</param>
        /// <param name="dataSetUri">The URI of a dataset.</param>
        /// <returns>An instance of Stream.</returns>
        public abstract Stream Egress(DryadLinqContext context, Uri dataSetUri);

        /// <summary>
        /// Checks the existence of a specified dataset.
        /// </summary>
        /// <param name="context">The current DryadLinqContext.</param>
        /// <param name="dataSetUri">The URI of the dataset.</param>
        /// <param name="deleteIfExists">True to delete if the dataset exists.</param>
        public abstract void CheckExistence(DryadLinqContext context, 
                                            Uri dataSetUri, 
                                            bool deleteIfExists);

        /// <summary>
        /// The path separator of this data provider.
        /// </summary>
        public virtual char PathSeparator
        {
            get { return '/'; }
        }

        /// <summary>
        /// Registers a new <see cref="DataProvider"/>. This can be used to extend DryadLINQ to
        /// interact with a new kind of data store.
        /// </summary>
        /// <param name="provider">A new DataProvider</param>
        public static void Register(DataProvider provider)
        {
            string scheme = provider.Scheme;
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
        /// <param name="context">An instance of <see cref="DryadLinqContext"/></param>
        /// <param name="dataSetUri">The URI of the dataset</param>
        /// <param name="deserializer">A stream-based deserializer</param>
        /// <returns>A query object representing the specified dataset.</returns>
        internal static DryadLinqQuery<T> 
            GetPartitionedTable<T>(DryadLinqContext context, 
                                   Uri dataSetUri,
                                   Expression<Func<Stream, IEnumerable<T>>> deserializer)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            return new DryadLinqQuery<T>(context, dataProvider, dataSetUri, deserializer);
        }

        /// <summary>
        /// Reads the dataset specified by a URI.
        /// </summary>
        /// <typeparam name="T">The record type of the dataset</typeparam>
        /// <param name="context">An instance of <see cref="DryadLinqContext"/></param>
        /// <param name="dataSetUri">The URI of the dataset</param>
        /// <param name="deserializer">A stream-based deserializer</param>
        /// <returns>A sequence of records as IEnumerable{T}</returns>
        internal static IEnumerable<T> ReadData<T>(DryadLinqContext context, 
                                                   Uri dataSetUri,
                                                   Expression<Func<Stream, IEnumerable<T>>> deserializer)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            return new DryadLinqQueryEnumerable<T>(context, dataProvider, dataSetUri, deserializer);
        }

        /// <summary>
        /// Stores an IEnumerable{T} at a specified location.
        /// </summary>
        /// <typeparam name="T">The record type of the data.</typeparam>
        /// <param name="context">An instance of <see cref="DryadLinqContext"/></param>
        /// <param name="source">The data to store.</param>
        /// <param name="dataSetUri">The URI of the store location.</param>
        /// <param name="metaData">The metadata of the data.</param>
        /// <param name="outputScheme">The compression scheme.</param>
        /// <param name="isTemp">true if the data is only stored temporarily.</param>
        /// <param name="serializer">A stream-based serializer</param>
        /// <param name="deserializer">A stream-based deserializer</param>
        /// <returns>An instance of IQueryable{T} for the data.</returns>
        internal static DryadLinqQuery<T> StoreData<T>(DryadLinqContext context,
                                                       IEnumerable<T> source,
                                                       Uri dataSetUri,
                                                       DryadLinqMetaData metaData,
                                                       CompressionScheme outputScheme,
                                                       bool isTemp,
                                                       Expression<Action<IEnumerable<T>, Stream>> serializer,
                                                       Expression<Func<Stream, IEnumerable<T>>> deserializer)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            dataSetUri = dataProvider.RewriteUri<T>(context, dataSetUri);
            dataProvider.Ingress(context, source, dataSetUri, metaData, outputScheme, isTemp, serializer);
            DryadLinqQuery<T> res = DataProvider.GetPartitionedTable<T>(context, dataSetUri, deserializer);
            res.CheckAndInitialize();    // must initialize
            return res;
        }
    }

    /// <summary>
    /// Basic information of a dataset.
    /// </summary>
    public class DryadLinqStreamInfo
    {
        /// <summary>
        /// The number of partitions of the dataset. Returns -1 if unknown.
        /// </summary>
        public Int32 PartitionCount { get; private set; }

        /// <summary>
        /// The size in bytes of the dataset. Returns -1 if unknown.
        /// </summary>
        public Int64 DataSize { get; private set; }

        /// <summary>
        /// Initializes an instance of DryadLinqStreamInfo.
        /// </summary>
        /// <param name="parCnt">The number of partitions.</param>
        /// <param name="size">The size in bytes.</param>
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

        public override Uri GetTemporaryStreamUri(DryadLinqContext context, string path)
        {
            return context.Cluster.MakeInternalClusterUri("tmp", DataPath.TEMPORARY_STREAM_NAME_PREFIX, path);
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            // TBD
            return null;
        }

        public override Uri RewriteUri<T>(DryadLinqContext context, Uri dataSetUri, FileAccess access)
        {
            UriBuilder builder = new UriBuilder(dataSetUri);
            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(builder.Query);

            if (access != FileAccess.Write &&
                typeof(T) == typeof(Microsoft.Research.DryadLinq.LineRecord))
            {
                query["seekBoundaries"] = "Microsoft.Research.DryadLinq.LineRecord";
            }

            builder.Query = query.ToString();
            return builder.Uri;
        }

        public override DryadLinqStreamInfo GetStreamInfo(DryadLinqContext context, Uri dataSetUri)
        {
            Int32 parCnt = 0;
            Int64 size = -1;
            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(dataSetUri.Query);
            bool expandBlocks = (query["seekboundaries"] == "Microsoft.Research.DryadLinq.LineRecord");
            context.GetHdfsClient.GetDirectoryContentSummary(dataSetUri, expandBlocks, ref size, ref parCnt);
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
                                        bool isTemp,
                                        Expression<Action<IEnumerable<T>, Stream>> serializer)
        {
            DryadLinqFactory<T> factory = (DryadLinqFactory<T>)DryadLinqCodeGen.GetFactory(context, typeof(T));
            using (Stream stream = context.GetHdfsClient.GetDfsStreamWriter(dataSetUri))
            {
                DryadLinqBlockStream nativeStream = new DryadLinqBlockStream(stream);
                DryadLinqRecordWriter<T> writer = factory.MakeWriter(nativeStream);
                foreach (T rec in source)
                {
                    writer.WriteRecordSync(rec);
                }
                writer.Close();
            }
        }

        public override Stream Egress(DryadLinqContext context, Uri dataSetUri)
        {
            return context.GetHdfsClient.GetDfsDirectoryStreamReader(dataSetUri);
        }

        public override void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists)
        {
            if (context.GetHdfsClient.IsFileExists(dataSetUri))
            {
                if (!deleteIfExists)
                {
                    throw new DryadLinqException("Can't output to existing HDFS collection " + dataSetUri.AbsoluteUri);
                }
                context.GetHdfsClient.DeleteDfsFile(dataSetUri, true);
            }
        }
    }

    internal class PartitionedFileDataProvider : DataProvider
    {
        public override string Scheme
        {
            get { return DataPath.PARTFILE_URI_SCHEME; }
        }

        public override Uri GetTemporaryStreamUri(DryadLinqContext context, string path)
        {
            string wd = Directory.GetCurrentDirectory();
            path = Path.Combine(Path.GetPathRoot(wd), DataPath.TEMPORARY_STREAM_NAME_PREFIX, path);
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            Uri uri = new Uri(this.Scheme + ":///" + path);
            return uri;
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            // TBD
            return null;
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
                                        bool isTemp,
                                        Expression<Action<IEnumerable<T>, Stream>> serializer)
        {
            string fileName = dataSetUri.LocalPath;
            if (!String.IsNullOrEmpty(dataSetUri.Host))
            {
                fileName = @"\\" + dataSetUri.Host + fileName;
            }

            // Write the partition:
            string partDir = Path.GetDirectoryName(fileName);
            partDir = Path.Combine(partDir, DryadLinqUtil.MakeUniqueName());
            Directory.CreateDirectory(partDir);
            string uncPath = Path.Combine(partDir, "Part");
            string partitionPath = uncPath + ".00000000";
            DryadLinqFactory<T> factory = (DryadLinqFactory<T>)DryadLinqCodeGen.GetFactory(context, typeof(T));
            using (FileStream fstream = new FileStream(partitionPath, FileMode.CreateNew, FileAccess.Write))
            {
                if (serializer == null)
                {
                    DryadLinqFileBlockStream nativeStream = new DryadLinqFileBlockStream(fstream, compressionScheme);
                    DryadLinqRecordWriter<T> writer = factory.MakeWriter(nativeStream);
                    foreach (T rec in source)
                    {
                        writer.WriteRecordSync(rec);
                    }
                    writer.Close();
                }
                else
                {
                    Action<IEnumerable<T>, Stream> serializerFunc = serializer.Compile();
                    serializerFunc(source, fstream);
                }
            }

            // Write the partfile:
            long partSize = new FileInfo(partitionPath).Length;
            using (StreamWriter writer = File.CreateText(fileName))
            {
                writer.WriteLine(uncPath);
                writer.WriteLine("1");
                writer.WriteLine("{0},{1}", 0, partSize);
            }
        }

        public override Stream Egress(DryadLinqContext context, Uri dataSetUri)
        {
            string fileName = dataSetUri.LocalPath;
            if (!String.IsNullOrEmpty(dataSetUri.Host))
            {
                fileName = @"\\" + dataSetUri.Host + fileName;
            }
            var lines = File.ReadAllLines(fileName);
            if (lines.Length < 3)
            {
                throw new DryadLinqException("The partition file " + dataSetUri + " is malformed.");
            }
            string[] filePathArray = this.GetPartitionPaths(lines);
            return new DryadLinqMultiFileStream(filePathArray, CompressionScheme.None);
        }

        public override void CheckExistence(DryadLinqContext context, Uri dataSetUri, bool deleteIfExists)
        {
            string fileName = dataSetUri.LocalPath;
            if (!String.IsNullOrEmpty(dataSetUri.Host))
            {
                fileName = @"\\" + dataSetUri.Host + fileName;
            }
            if (File.Exists(fileName))
            {
                if (!deleteIfExists)
                {
                    throw new DryadLinqException("Can't output to existing Partitioned File collection " + dataSetUri.AbsoluteUri);
                }

                // Note: We delete all the partitions!
                var lines = File.ReadAllLines(fileName);
                try
                {
                    foreach (string path in this.GetPartitionPaths(lines))
                    {
                        if (File.Exists(path))
                        {
                            File.Delete(path);
                        }
                    }
                }
                catch (Exception) { /*skip*/ }
                File.Delete(fileName);
            }
        }

        private string[] GetPartitionPaths(string[] lines)
        {
            string[] filePathArray = new string[lines.Length - 2];
            for (int i = 2; i < lines.Length; i++)
            {
                int idx = i - 2;
                string[] fields = lines[i].Split(',');
                if (fields.Length > 2 && fields[2].Contains(':'))
                {
                    string[] parts = fields[2].Split(':');
                    filePathArray[idx] = String.Format(@"\\{0}\{1}", parts[0], parts[1]);
                }
                else if (Path.IsPathRooted(lines[0]))
                {
                    filePathArray[idx] = String.Format("{0}.{1:X8}", lines[0], idx);
                }
                else
                {
                    filePathArray[idx] = String.Format(@"\\{0}\{1}.{2:X8}", fields[2], lines[0], idx);
                }
            }
            return filePathArray;
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

        public override Uri GetTemporaryStreamUri(DryadLinqContext context, string path)
        {
            return context.Cluster.MakeInternalClusterUri(DataPath.TEMPORARY_STREAM_NAME_PREFIX, path);
        }

        public override Uri RewriteUri<T>(DryadLinqContext context, Uri dataSetUri, FileAccess access)
        {
            string account, key, container, blob;
            Microsoft.Research.Peloponnese.Azure.Utils.FromAzureUri(dataSetUri, out account, out key, out container, out blob);

            UriBuilder builder = new UriBuilder(dataSetUri);
            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(builder.Query);

            if (key == null)
            {
                query["key"] = context.AzureAccountKey(account);
            }

            if (access != FileAccess.Write && 
                typeof(T) == typeof(Microsoft.Research.DryadLinq.LineRecord))
            {
                query["seekBoundaries"] = "Microsoft.Research.DryadLinq.LineRecord";
            }

            builder.Query = query.ToString();
            return builder.Uri;
        }

        public override DryadLinqMetaData GetMetaData(DryadLinqContext context, Uri dataSetUri)
        {
            // TBD
            return null;
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
                                        CompressionScheme compressionScheme,
                                        bool isTemp,
                                        Expression<Action<IEnumerable<T>, Stream>> serializer)
        {
            string account, key, container, blob;
            Utils.FromAzureUri(dataSetUri, out account, out key, out container, out blob);
            if (compressionScheme != CompressionScheme.None)
            {
                throw new DryadLinqException("Not implemented: writing to Azure temporary storage with compression enabled");
            }
            AzureDfsClient client = new AzureDfsClient(account, key, container);
            DryadLinqFactory<T> factory = (DryadLinqFactory<T>)DryadLinqCodeGen.GetFactory(context, typeof(T));
            using (Stream stream = client.GetDfsStreamWriterAsync(dataSetUri).Result)
            {
                if (serializer == null)
                {
                    DryadLinqBlockStream nativeStream = new DryadLinqBlockStream(stream);
                    DryadLinqRecordWriter<T> writer = factory.MakeWriter(nativeStream);
                    foreach (T rec in source)
                    {
                        writer.WriteRecordSync(rec);
                    }
                    writer.Close();
                }
                else
                {
                    Action<IEnumerable<T>, Stream> serializerFunc = serializer.Compile();
                    serializerFunc(source, stream);
                }
            }
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
                if (!deleteIfExists)
                {
                    throw new DryadLinqException("Can't output to existing Azure Blob collection " + dataSetUri.AbsoluteUri);
                }
                partition.DeleteCollection();
            }
        }
    }   
}
