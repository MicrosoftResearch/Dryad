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

using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace Microsoft.Research.DryadLinq
{
    internal class WebHdfsClient
    {

        internal void GetHdfsFile(string hdfsDir, string fileName)
        {
            if(!hdfsDir.EndsWith("/"))
            {
                hdfsDir = hdfsDir + "/";
            }
            var hdfsDirUri = new Uri(hdfsDir, UriKind.Absolute);
            var hdfsFileUri = new Uri(hdfsDirUri, fileName);
            var builder = new UriBuilder();
            builder.Host = hdfsFileUri.DnsSafeHost;
            builder.Port = 50070; //hdfsFileUri.Port;  // ipc port is 9000, http port is 50070  TODO
            builder.Path = "webhdfs/v1/" + hdfsFileUri.AbsolutePath.TrimStart('/');
            builder.Query = "op=OPEN";
            Console.WriteLine(builder.Uri);
            var wc = new WebClient();
            wc.DownloadFile(builder.Uri, fileName);
            
        }

        internal static void GetContentSummary(string path, ref long estSize, ref int parCount)
        {
            // TODO: Move this to a sensible JSON parser.
            var pathUri = new Uri(path, UriKind.Absolute);
            var builder = new UriBuilder();
            builder.Host = pathUri.DnsSafeHost;
            builder.Port = 50070; // pathUri.Port; // ipc port is 9000, http port is 50070  TODO
            builder.Path = "webhdfs/v1/" + pathUri.AbsolutePath.TrimStart('/');
            builder.Query = "op=GETCONTENTSUMMARY";
            bool foundParCount = false;
            bool foundEstSize = false;

            var wc = new WebClient();
            var data = wc.DownloadString(builder.Uri);

            var matches = Regex.Matches(data, "\"([^\"]+)\":([^,]+)");
            foreach(Match match in matches)
            {
                for(int ctr = 1; ctr <= match.Groups.Count - 1; ctr++)
                {
                    if(match.Groups[ctr].Value == "fileCount")
                    {
                        parCount = int.Parse(match.Groups[ctr + 1].Value);
                        foundParCount = true;
                        ctr++;
                    }
                    else if(match.Groups[ctr].Value == "length")
                    {
                        estSize = long.Parse(match.Groups[ctr + 1].Value);
                        foundEstSize = true;
                        ctr++;
                    }
                    
                }
            }
            if(!foundParCount || !foundEstSize)
            {
                throw new DryadLinqException("Unable to parse WebHdfs reponse.");
            }
        }
    }
}
