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

//--------------------------------------------------------------------------
//
// <summary>
//      Fileset compression modes supported by DSC.
// </summary>
//--------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Research.DryadLinq
{
    //YARN 
    public enum DscCompressionScheme
    {
        None,
        Gzip
    }

    public class DscService
    {
        private string m_headNode;
        public DscService(string headNode)
        {
            m_headNode = headNode;
        }

        internal DscFileSet GetFileSet(string streamName)
        {
            throw new NotImplementedException();
        }

        internal bool FileSetExists(string dscFileSetName)
        {
            throw new NotImplementedException();
        }

        internal void DeleteFileSet(string dscFileSetName)
        {
            throw new NotImplementedException();
        }

        internal DscFileSet CreateFileSet(string streamName, DscCompressionScheme compressionScheme)
        {
            throw new NotImplementedException();
        }

        internal void Close()
        {
            throw new NotImplementedException();
        }

        public string HostName { get; set; }
    }

    public class DscFileSet
    {

        internal DscFile AddNewFile(int p)
        {
            throw new NotImplementedException();
        }

        public DscCompressionScheme CompressionScheme { get; set; }

        internal byte[] GetMetadata(string p)
        {
            throw new NotImplementedException();
        }

        internal void Seal()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<DscFile> GetFiles()
        {
            throw new NotImplementedException();
        }

        internal bool IsSealed()
        {
            throw new NotImplementedException();
        }

        internal void SetLeaseEndTime(DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        internal void SetMetadata(string p1, byte[] p2)
        {
            throw new NotImplementedException();
        }
    }

    public class DscFile
    {
        public string[] ReadPaths { get; set; }
        public string WritePath { get; set; }
    }

    internal class DscInstance: IDisposable
    {

        public DscInstance(Uri uri)
        {
            throw new NotImplementedException();
        }


        internal DscStream GetStream(Uri uri)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    internal class DscStream
    {

        public long Length { get; set; }

        public int PartitionCount { get; set; }
    }

    public class DscException : Exception
    {

    }

    public interface IScheduler
    {

        void Connect(string headNode);

        void Dispose();

        IServerVersion GetServerVersion();
    }

    public interface IServerVersion
    {

        int Major { get; set; }

        int Minor { get; set; }

        int Build { get; set; }

        int Revision { get; set; }
    }
}
