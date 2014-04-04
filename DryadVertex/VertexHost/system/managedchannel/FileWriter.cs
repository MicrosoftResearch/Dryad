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
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Dryad.Channel
{
    internal class FileWriter : StreamWriter
    {
        private string fileName;
        private FileStream file;

        public FileWriter(Uri uri, int numberOfWriters, IWriterClient client, IDrLogging logger) : base(client, logger)
        {
            fileName = uri.AbsolutePath;
        }

        public override int BufferSize
        {
            get { return 64 * 1024; }
        }

        public override long BufferAlignment
        {
            get { return 0; }
        }

        public override bool BreakOnRecordBoundaries
        {
            get { return false; }
        }

        public override Task<string> Open()
        {
            file = new FileStream(fileName, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
            return Task.FromResult<string>(null);
        }

        public override async Task<string> WriteBuffer(byte[] managedBuffer)
        {
            await file.WriteAsync(managedBuffer, 0, managedBuffer.Length);
            return null;
        }

        public override Task<string> Close()
        {
            file.Dispose();
            file = null;
            return Task.FromResult<string>(null);
        }

        private static IManagedWriter OpenFileWriter(Uri uri, int numberOfWriters, IWriterClient client, IDrLogging logger)
        {
            return new FileWriter(uri, numberOfWriters, client, logger);
        }

        public static void Register()
        {
            Factory.RegisterWriter("file", OpenFileWriter);
        }
    }
}
