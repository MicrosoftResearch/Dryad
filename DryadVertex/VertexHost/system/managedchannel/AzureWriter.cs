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
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Azure;

namespace Microsoft.Research.Dryad.Channel
{
    internal class AzureWriter : StreamWriter
    {
        private static TimeSpan ExecutionTimeout = new TimeSpan(0, 0, 30); // 30 seconds before retrying
        private readonly Uri baseUri;
        private AzureBlockBlobWriter writer;
        private int blobIndex;
        private int bytesWritten;
        private bool exceededBlockSize;

        public AzureWriter(Uri uri, int numberOfWriters, IWriterClient client, IDrLogging logger)
            : base(client, logger)
        {
            baseUri = uri;
            blobIndex = 0;
            bytesWritten = 0;
            exceededBlockSize = false;
        }

        public override int BufferSize
        {
            get {
                NameValueCollection query = System.Web.HttpUtility.ParseQueryString(baseUri.Query);
                if (query["targetBlockSize"] != null)
                {
                    int target = Int32.Parse(query["targetBlockSize"]);
                    return Math.Min(target, AzureConsts.MaxBlockSize4MB);
                }
                else
                {
                    return AzureConsts.MaxBlockSize4MB;
                }
            }
        }

        public override long BufferAlignment
        {
            get { return 0; }
        }

        public override bool BreakOnRecordBoundaries
        {
            get { return true; }
        }

        private Uri CurrentUri
        {
            get
            {
                UriBuilder builder = new UriBuilder(baseUri);
                builder.Path += "-" + blobIndex.ToString("D4");
                return builder.Uri;
            }
        }


        public override async Task<string> Open()
        {
            await Task.Run(() => { writer = new AzureBlockBlobWriter(CurrentUri, ExecutionTimeout, false, new PeloponneseLogger(Log.Logger)); });
            bytesWritten = 0;
            return null;
        }

        public override async Task<string> WriteBuffer(byte[] managedBuffer)
        {
            Log.LogInformation("writing buffer " + managedBuffer.Length);
            if (managedBuffer.LongLength > Int32.MaxValue)
            {
                throw new ApplicationException("Can't write a record larger than 2GB, attempting " + managedBuffer.LongLength);
            }

            if (managedBuffer.Length > AzureConsts.MaxBlockSize4MB)
            {
                exceededBlockSize = true;
            }

            if (!exceededBlockSize && bytesWritten + managedBuffer.Length > AzureConsts.Size256MB)
            {
                Log.LogInformation("opening new stream");
                await Close();
                ++blobIndex;
                await Open();
            }

            int offset = 0;
            while (offset < managedBuffer.Length)
            {
                int toWrite = (int)Math.Min((long)AzureConsts.MaxBlockSize4MB, managedBuffer.Length - offset);
                Log.LogInformation("writing block " + offset + " " + toWrite);
                await writer.WriteBlockAsync(managedBuffer, offset, toWrite);
                Log.LogInformation("wrote block " + offset + " " + toWrite);
                offset += toWrite;
                bytesWritten += toWrite;
            }

            return null;
        }

        public override async Task<string> Close()
        {
            await writer.CommitBlocksAsync();
            if (!exceededBlockSize)
            {
                await writer.SetMetadataAsync("recordsRespectBlockBoundaries", "true");
            }
            writer = null;
            return null;
        }

        private static IManagedWriter OpenAzureWriter(Uri uri, int numberOfWriters, IWriterClient client, IDrLogging logger)
        {
            return new AzureWriter(uri, numberOfWriters, client, logger);
        }

        public static void Register()
        {
            Factory.RegisterWriter("azureblob", OpenAzureWriter);
        }
    }
}
