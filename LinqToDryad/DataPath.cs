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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Globalization;

namespace Microsoft.Research.DryadLinq
{
    internal static class DataPath
    {
        const string TEMPORARY_STREAM_NAME_PREFIX = "/__DryadLinq_Temp/";
        internal const string DSC_URI_PREFIX = "hpcdsc://"; 
        internal const string HDFS_URI_PREFIX = "hpchdfs://"; 

        internal const string PrefixSeparator = "://";
        internal const string QuerySeparator = "?";
        internal const char ArgumentSeparator = '&';
        internal const char KeyValueSeparator = '=';

        internal const string DSC_STORAGE_SET_TYPE = "Dsc"; // used to be enum StorageSetType {Dsc}
        internal const string HDFS_STORAGE_SET_TYPE = "Hdfs"; // used to be enum StorageSetType {Hdfs}

        public static bool IsDsc(string uri)
        {
            return uri.StartsWith(DSC_URI_PREFIX, StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsHdfs(string dataPath)
        {
            return dataPath.StartsWith(HDFS_URI_PREFIX);
        }

        internal static string GetDataPath(string tableUri)
        {
            int idx = tableUri.IndexOf(QuerySeparator, StringComparison.Ordinal);
            string dataPath = tableUri;
            if (idx > 0)
            {
                dataPath = tableUri.Substring(0, idx);
            }
            return dataPath;   
        }

        internal static string GetPrefix(string dataPath)
        {
            int idx = dataPath.IndexOf(PrefixSeparator, StringComparison.Ordinal);
            if (idx <= 0) return "";
            return dataPath.Substring(0, idx+3);
        }

        internal static string GetPath(string dataPath)
        {
            int idx = dataPath.IndexOf(PrefixSeparator, StringComparison.Ordinal);
            if (idx <= 0) return dataPath;
            return dataPath.Substring(idx+3);
        }

        internal static Dictionary<string, string> GetArguments(string dscFileSetUri)
        {
            Dictionary<string, string> args = new Dictionary<string, string>();
            int idx = dscFileSetUri.IndexOf(QuerySeparator, StringComparison.Ordinal);
            while (idx >= 0)
            {
                idx++;
                int idx1 = dscFileSetUri.IndexOf(KeyValueSeparator, idx);
                if (idx1 < 0)
                {
                    throw new ArgumentException(String.Format(SR.IllFormedUri, dscFileSetUri), "dscFileSetUri");
                }
                string key = dscFileSetUri.Substring(idx, idx1 - idx).ToLower(CultureInfo.InvariantCulture);
                idx1++;
                idx = dscFileSetUri.IndexOf(ArgumentSeparator, idx1);
                string value;
                if (idx < 0)
                {
                    value = dscFileSetUri.Substring(idx1);
                }
                else
                {
                    value = dscFileSetUri.Substring(idx1, idx - idx1);
                }
                args.Add(key, value);
            }
            return args;
        }
        
        internal static string PathCombine(string dataPath1, string dataPath2)
        {
            string prefix = GetPrefix(dataPath1);
            if (prefix == "")
            {
                return Path.Combine(dataPath1, dataPath2);
            }
            if (prefix == dataPath1)
            {
                return dataPath1 + dataPath2;
            }
            DataProvider dp = DataProvider.GetDataProvider(prefix);
            dataPath1 = dataPath1.TrimEnd(dp.PathSeparator);
            dataPath2 = dataPath2.TrimStart(dp.PathSeparator);
            return dataPath1 + dp.PathSeparator + dataPath2;
        }

        /// <summary>
        /// Split a path into a directory name and a filename.
        /// </summary>
        /// <param name="dataPath">Path to split.</param>
        /// <param name="dir">Directory part of pathname.  May be empty if there are no slashes.</param>
        /// <param name="file">File part of pathname.</param>
        internal static void PathSplit(string dataPath, out string dir, out string file)
        {
            string prefix = GetPrefix(dataPath);
            int slash;
            if (prefix == "")
            {
                slash = dataPath.LastIndexOf(Path.DirectorySeparatorChar);
            }
            else
            {
                DataProvider dp = DataProvider.GetDataProvider(prefix);
                slash = dataPath.LastIndexOf(dp.PathSeparator);
                if (slash < 0) slash = dataPath.LastIndexOf('/');
            }
            
            if (slash >= 0)
            {
                file = dataPath.Substring(slash + 1);
                dir = dataPath.Substring(0, slash);
            }
            else
            {
                file = dataPath;
                dir = "";
            }
        }

        /// <summary>
        /// Extract the directory part of a path.
        /// </summary>
        /// <param name="dataPath">Path to split.</param>
        /// <returns>Directory name (may be empty).</returns>
        internal static string GetDir(string dataPath)
        {
            string dir, file;
            PathSplit(dataPath, out dir, out file);
            return dir;
        }

        /// <summary>
        /// Extract just the filename from a path.
        /// </summary>
        /// <param name="dataPath">Path to split.</param>
        /// <returns>Just the filename part.</returns>
        public static string GetFile(string dataPath)
        {
            string dir, file;
            PathSplit(dataPath, out dir, out file);
            return file;
        }

        internal static string MakeDscStreamUri(DscService dsc, string streamName)
        {
            string serviceNodeName = dsc.HostName;
            return MakeDscStreamUri(serviceNodeName, streamName);
        }

        internal static string MakeDscStreamUri(string serviceNodeName, string streamName)
        {
            Uri uri = new Uri(new Uri("hpcdsc://" + serviceNodeName + ":6498/"), streamName);  // use the Uri class to do combining/escaping.
            return uri.AbsoluteUri;
        }

        internal static string MakeHdfsStreamUri(string serviceNodeName, string streamName)
        {
            return "hpchdfs://" + serviceNodeName + ":9000/" + streamName; // TODO: Parse config files.
        }

        //@@TODO[P2]: some overlap in how unique DSC stream names are made. Cleanup.
        //  1. string tableUri = DryadLinqUtil.MakeUniqueName();
        //     string fullTableUri = DataPath.GetFullUri(tableUri);
        //
        //  2. string tableName = DataPath.MakeUniqueDscStreamUri();
        //
        // Note: both base their root path on DryadLinq.DryadOutputDir.


        internal static string MakeUniqueTemporaryDscFileSetName()
        {
            return TEMPORARY_STREAM_NAME_PREFIX + HpcLinqUtil.MakeUniqueName();
        }

        internal static string MakeUniqueTemporaryDscFileSetUri(HpcLinqContext context)
        {
            string uri = DataPath.MakeDscStreamUri(context.DscService.HostName, MakeUniqueTemporaryDscFileSetName());
            return uri;
        }

        internal static string MakeUniqueTemporaryHdfsFileSetUri(HpcLinqContext context)
        {
            string uri = DataPath.MakeHdfsStreamUri(context.Configuration.HdfsNameNode, MakeUniqueTemporaryDscFileSetName());
            return uri;
        }

        internal static string GetFilesetNameFromUri(string uriString)
        {
            Uri uri = new Uri(uriString);
            return uri.AbsolutePath.TrimStart('/');
        }
    }
}
