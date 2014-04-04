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
using System.Text;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Globalization;

namespace Microsoft.Research.DryadLinq
{
    internal static class DataPath
    {
        internal const string TEMPORARY_STREAM_NAME_PREFIX = "DryadLinqTemp/";

        internal const string DSC_URI_SCHEME = "hpcdsc";
        internal const string HDFS_URI_SCHEME = "hdfs";
        internal const string PARTFILE_URI_SCHEME = "partfile";
        internal const string WASB_URI_SCHEME = "wasb";
        internal const string AZUREBLOB_URI_SCHEME = "azureblob";

        internal const string PrefixSeparator = "://";

        internal const string DSC_STORAGE_SET_TYPE = "Dsc"; // used to be enum StorageSetType {Dsc}
        internal const string HDFS_STORAGE_SET_TYPE = "Hdfs"; // used to be enum StorageSetType {Hdfs}
        internal const string PT_STORAGE_SET_TYPE = "PartitionedFile";
        internal const string AZUREBLOB_STORAGE_SET_TYPE = "AzureBlob";

        internal static Dictionary<string, string> storageSetSchemeToType = new Dictionary<string,string>
        {
            { DSC_URI_SCHEME, DSC_STORAGE_SET_TYPE },
            { HDFS_URI_SCHEME, HDFS_STORAGE_SET_TYPE },
            { PARTFILE_URI_SCHEME, PT_STORAGE_SET_TYPE },
            { WASB_URI_SCHEME, HDFS_STORAGE_SET_TYPE },
            { AZUREBLOB_URI_SCHEME, AZUREBLOB_STORAGE_SET_TYPE }
        };

        // An example of URI: 
        // hdfs://machine:port/folder/folder/file?arg=value?arg2=value2
        // |------------- data path -------------|---- arguments -----|
        // |prefix|----------- path -------------|
        //                    |---- filepath ----|
        public static bool IsDsc(Uri uri)
        {
            return uri.Scheme == DSC_URI_SCHEME;
        }

        public static bool IsHdfs(Uri uri)
        {
            return uri.Scheme == HDFS_URI_SCHEME;
        }

        public static bool IsPartitionedFile(Uri uri)
        {
            return uri.Scheme == PARTFILE_URI_SCHEME;
        }

        public static bool IsWasb(Uri uri)
        {
            return uri.Scheme == WASB_URI_SCHEME;
        }

        public static bool IsAzureBlob(Uri uri)
        {
            return uri.Scheme == AZUREBLOB_URI_SCHEME;
        }

        public static bool IsValidDataPath(Uri uri)
        {
            return (IsDsc(uri) || IsHdfs(uri) || IsPartitionedFile(uri) || IsWasb(uri) || IsAzureBlob(uri));
        }

        internal static string GetScheme(Uri dataPath)
        {
            return dataPath.Scheme;
        }

        internal static string GetPath(string dataPath)
        {
            int idx = dataPath.IndexOf(PrefixSeparator, StringComparison.Ordinal);
            if (idx <= 0) return dataPath;
            return dataPath.Substring(idx + PrefixSeparator.Length);
        }

        internal static string GetStorageType(Uri dataPath)
        {
            string prefix = GetScheme(dataPath);
            string storageType;
            if (!storageSetSchemeToType.TryGetValue(prefix, out storageType))
            {
                storageType = prefix;
            }
            return storageType;
        }

        internal static string GetStreamNameFromUri(Uri uri)
        {
            return uri.AbsolutePath.TrimStart('/');
        }
    }
}
