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
using System.IO;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    // The class directly talks to a list of NTFS/Cosmos files. 
    internal unsafe class MultiBlockStream : NativeBlockStream
    {
        private List<string[]> m_srcList;   // each source is represented by an array of alternative paths (i.e. replica paths)
        private string m_associatedDscStreamName; // stored here only to provide a better exception message in case of IO errors
        private CompressionScheme m_compressionScheme;
        private int m_curIdx;
        private NativeBlockStream m_curStream;

        internal MultiBlockStream(List<string[]> srcList,
                                  string associatedDscStreamName,
                                  FileAccess access,
                                  CompressionScheme scheme)
        {
            this.m_srcList = srcList;
            this.m_associatedDscStreamName = associatedDscStreamName;
            if (srcList.Count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MultiBlockEmptyPartitionList,
                                             SR.MultiBlockEmptyPartitionList);
            }
            this.m_compressionScheme = scheme;
            this.m_curIdx = 0;
            this.m_curStream = this.GetStream(this.m_curIdx++, access);
        }

        private NativeBlockStream GetStream(int idx, FileAccess access)
        {
            DryadLinqFileBlockStream fileStream = null;
            string[] pathAlternatives = this.m_srcList[idx];

            for (int i = 0; i < pathAlternatives.Length; i++)
            {
                bool bLastIter = (i == pathAlternatives.Length - 1);
                string curSrcPath = pathAlternatives[i];
                try
                {
                    fileStream = new DryadLinqFileBlockStream(curSrcPath, access, this.m_compressionScheme);
                }
                catch(Exception e)
                {
                    // if we have more path alternatives to try we will continue,
                    // otherwise we'll propagate the exception from this last attempt
                    if (bLastIter)
                    {
                        // if we caught the DryadLinqException thrown by DryadLinqFileStream.Initialize,
                        // we want to propagate its inner exception (which contains the actual IO error)
                        // otherwise we'll attach the exception we caught as is
                        Exception innerException = (e is DryadLinqException) ? e.InnerException : e;
                        throw new DryadLinqException(DryadLinqErrorCode.MultiBlockCannotAccesFilePath,
                                                   String.Format(SR.MultiBlockCannotAccesFilePath,
                                                                 curSrcPath, m_associatedDscStreamName),
                                                   innerException);
                    }
                }

                // if the attempt to initialize an DryadLinqFileStream with this path succeeded we'll return the object
                if (fileStream != null) break;
            }
            
            return fileStream;
        }

        internal override unsafe Int64 GetTotalLength()
        {
            Int64 totalLen = 0;
            for (int i = 0; i < this.m_srcList.Count; i++)
            {
                NativeBlockStream ns = this.GetStream(i, FileAccess.Read);
                totalLen += ns.GetTotalLength();
                ns.Close();
            }
            return totalLen;
        }

        internal override unsafe DataBlockInfo ReadDataBlock()
        {
            while (true)
            {
                DataBlockInfo dataBlockInfo = this.m_curStream.ReadDataBlock();
                if (dataBlockInfo.BlockSize == 0 && this.m_curIdx == m_srcList.Count)
                {
                    // data has been exhausted.  We return the empty block and the caller knows what to do.
                    return dataBlockInfo;
                }
                
                // normal case.. record the last two bytes for newline tracking, and return the block.
                if (dataBlockInfo.BlockSize > 0)
                {
                    return dataBlockInfo;
                }

                this.m_curStream.ReleaseDataBlock(dataBlockInfo.ItemHandle);
                this.m_curStream.Close();
                this.m_curStream = this.GetStream(this.m_curIdx++, FileAccess.Read);
            }
        }

        internal override unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite)
        {
            return this.m_curStream.WriteDataBlock(itemHandle, numBytesToWrite);
        }

        internal override void Flush()
        {
            this.m_curStream.Flush();
        }

        internal override void Close()
        {
            this.m_curStream.Close();
        }

        internal override unsafe DataBlockInfo AllocateDataBlock(Int32 size)
        {
            return this.m_curStream.AllocateDataBlock(size);
        }

        internal override unsafe void ReleaseDataBlock(IntPtr itemHandle)
        {
            this.m_curStream.ReleaseDataBlock(itemHandle);
        }

        internal override unsafe string GetURI()
        {
            // the only use of this is to be put in an error msg..
            // We don't have an appropriate URI, so we just use the empty string. (bug 13970)
            return "";
        }
    }
}
