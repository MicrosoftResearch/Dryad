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

namespace Microsoft.Research.DryadLinq
{
    //DataProvider is an abstraction for different data backends.
    //Currently this class isn't strictly necessary (DSC is only data-store) but other data sources could come back.
    internal class DataProvider
    {
        private static Dictionary<string, DataProvider> s_providers;

        static DataProvider()
        {
            s_providers = new Dictionary<string, DataProvider>();
            s_providers.Add(DataPath.DSC_URI_PREFIX, new DataProvider());
        }

        // dead code.
        ///// <summary>
        ///// Register a data provider so that it can be used.
        ///// </summary>
        ///// <param name="prefix">The prefix name of the data provider</param>
        ///// <param name="dp">the data provider to be registered</param>
        //internal static void Register(string prefix, DataProvider dp)
        //{
        //    if (s_providers.ContainsKey(prefix))
        //    {
        //        throw new HpcLinqException(HpcLinqErrorCode.Internal_PrefixAlreadyUsedForOtherProvider,
        //                                   String.Format(SR.PrefixAlreadyUsedForOtherProvider, prefix));
        //    }
        //    s_providers.Add(prefix, dp);
        //}

        /// <summary>
        /// Get the data provider associated with a prefix.
        /// </summary>
        /// <param name="prefix">The data provider prefix</param>
        /// <returns>The data provider</returns>
        internal static DataProvider GetDataProvider(string prefix)
        {
            if (!s_providers.ContainsKey(prefix))
            {
                throw new DryadLinqException(HpcLinqErrorCode.Internal,
                                           String.Format(SR.UnknownProvier, prefix));
            }
            return s_providers[prefix];
        }
        
        /// <summary>
        /// The prefix of this data provider.
        /// </summary>
        internal string Prefix
        {
            get { return DataPath.DSC_URI_PREFIX; }
        }

        /// <summary>
        /// The path separator of this data provider.
        /// </summary>
        internal char PathSeparator
        {
            get { return '/'; }
        }

        /// <summary>
        /// Get the DSC file set specified by a URI.
        /// </summary>
        /// <typeparam name="T">The record type of the table.</typeparam>
        /// <param name="dscFileSetUri">The URI of a DscFileSet.</param>
        /// <returns>A query object representing the dsc file set data.</returns>
        
        
        internal static DryadLinqQuery<T> GetPartitionedTable<T>(HpcLinqContext context, string dscFileSetUri)
        {
            Dictionary<string, string> args = DataPath.GetArguments(dscFileSetUri);

            DataProvider dataProvider = new DataProvider();
            DryadLinqProvider queryProvider = new DryadLinqProvider(context);
            return new DryadLinqQuery<T>(null, queryProvider, dataProvider, dscFileSetUri);
        }

        // ingresses data, and also sets the temporary-length lease.
        internal static DryadLinqQuery<T> IngressTemporaryDataDirectlyToDsc<T>(HpcLinqContext context, IEnumerable<T> source, string dscFileSetName, DryadLinqMetaData metaData, DscCompressionScheme outputScheme)
        {
            DryadLinqQuery<T> result = IngressDataDirectlyToDsc(context, source, dscFileSetName, metaData, outputScheme);

            // try to set a temporary lease on the resulting fileset
            try
            {
                DscFileSet fs = context.DscService.GetFileSet(dscFileSetName);
                fs.SetLeaseEndTime(DateTime.Now.Add(StaticConfig.LeaseDurationForTempFiles));
            }
            catch (DscException)
            {
                // suppress
            }

            return result;
        }

        //* streams plain enumerable data directly to DSC 
        internal static DryadLinqQuery<T> IngressDataDirectlyToDsc<T>(HpcLinqContext context, 
                                                                      IEnumerable<T> source, 
                                                                      string dscFileSetName, 
                                                                      DryadLinqMetaData metaData, 
                                                                      DscCompressionScheme outputScheme)
        {
            try
            {
                string dscFileSetUri = DataPath.MakeDscStreamUri(context.DscService.HostName, dscFileSetName);
                if (source.Take(1).Count() == 0)
                {
                    //there is no data.. we must create a FileSet with an empty file
                    //(the factory/stream approach opens files lazily and thus never opens a file if there is no data)


                    if (context.DscService.FileSetExists(dscFileSetName))
                    {
                        context.DscService.DeleteFileSet(dscFileSetName);
                    }
                    DscFileSet fileSet = context.DscService.CreateFileSet(dscFileSetName, outputScheme);
                    DscFile file = fileSet.AddNewFile(0);
                    string writePath = file.WritePath;

                    
                    if (outputScheme == DscCompressionScheme.Gzip)
                    {
                        //even zero-byte file must go through the gzip-compressor (for headers etc).
                        using (Stream s = new FileStream(writePath, FileMode.Create))
                        {
                            var gzipStream = new GZipStream(s, CompressionMode.Compress);
                            gzipStream.Close();
                        }
                    }
                    else
                    {
                        StreamWriter sw = new StreamWriter(writePath, false);
                        sw.Close();
                    }
                    fileSet.Seal();

                }
                else
                {
                    HpcLinqFactory<T> factory = (HpcLinqFactory<T>)HpcLinqCodeGen.GetFactory(context, typeof(T));
                    
                    // new DscBlockStream(uri,Create,Write,compress) provides a DSC stream with one partition.
                    NativeBlockStream nativeStream = new DscBlockStream(dscFileSetUri, FileMode.Create, FileAccess.Write, outputScheme);
                    HpcRecordWriter<T> writer = factory.MakeWriter(nativeStream);
                    try
                    {
                        if (context.Configuration.AllowConcurrentUserDelegatesInSingleProcess)
                        {
                            foreach (T item in source)
                            {
                                writer.WriteRecordAsync(item);
                            }
                        }
                        else
                        {
                            foreach (T item in source)
                            {
                                writer.WriteRecordSync(item);
                            }
                        }
                    }
                    finally
                    {
                        writer.Close(); // closes the NativeBlockStream, which seals the dsc stream.
                    }
                }

                if (metaData != null)
                {
                    DscFileSet fileSet = context.DscService.GetFileSet(dscFileSetName);
                    fileSet.SetMetadata(DryadLinqMetaData.RECORD_TYPE_NAME, Encoding.UTF8.GetBytes(metaData.ElemType.AssemblyQualifiedName));
                }

                return DataProvider.GetPartitionedTable<T>(context, dscFileSetUri);
            }
            catch
            {
                // if we had a problem creating the empty fileset, try to delete it to avoid cruft being left in DSC.
                try
                {
                    context.DscService.DeleteFileSet(dscFileSetName);
                }
                catch
                {
                    // suppress error during delete
                }

                throw; // rethrow the original exception.
            }
        }
    }
}
