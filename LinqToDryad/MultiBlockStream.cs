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
        private DscCompressionScheme m_compressionScheme;
        private int m_curIdx;
        private NativeBlockStream m_curStream;
        private static byte* s_newlineByteBlock; // holds a dummy block comprising ['\r', '\n'], lazily initialized.

        //re bug: 16011, initialize trailingByes as a newline pair so that we dont add a newline if the first file (or files) are empty.
        byte[] m_trailingBytesOfData = new byte[2] { (byte)'\r', (byte)'\n' };
        private bool m_appendNewLinesToFiles;

        internal MultiBlockStream(List<string[]> srcList,
                                  string associatedDscStreamName,
                                  FileAccess access,
                                  DscCompressionScheme scheme,
                                  bool appendNewLinesToFiles)
        {
            this.m_srcList = srcList;
            m_associatedDscStreamName = associatedDscStreamName;
            if (srcList.Count == 0)
            {
                throw new DryadLinqException(HpcLinqErrorCode.MultiBlockEmptyPartitionList,
                                           SR.MultiBlockEmptyPartitionList);
            }
            this.m_compressionScheme = scheme;
            this.m_curIdx = 0;
            this.m_curStream = this.GetStream(this.m_curIdx++, access);
            this.m_appendNewLinesToFiles = appendNewLinesToFiles;
        }

        private NativeBlockStream GetStream(int idx, FileAccess access)
        {
            HpcLinqFileStream fileStream = null;
            string[] pathAlternatives = this.m_srcList[idx];

            for (int i = 0; i < pathAlternatives.Length; i++)
            {
                bool bLastIter = (i == pathAlternatives.Length - 1);
                string curSrcPath = pathAlternatives[i];
                try
                {
                    fileStream = new HpcLinqFileStream(curSrcPath, access, this.m_compressionScheme);
                }
                catch(Exception exp)
                {
                    
                    // if we have more path alternatives to try we will continue,
                    // otherwise we'll propagate the exception from this last attempt
                    if (bLastIter)
                    {
                        // if we caught the HpcLinqException thrown by HpcLinqFileStream.Initialize,
                        // we want to propagate its inner exception (which contains the actual IO error)
                        // otherwise we'll attach the exception we caught as is
                        Exception innerException = exp is DryadLinqException ? innerException = exp.InnerException : exp;
                        throw new DryadLinqException(HpcLinqErrorCode.MultiBlockCannotAccesFilePath,
                                                   String.Format(SR.MultiBlockCannotAccesFilePath,
                                                                 curSrcPath, m_associatedDscStreamName),
                                                   innerException);
                    }
                }

                // if the attempt to initialize an HpcLinqFileStream with this path succeeded we'll return the object
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
            // free the dummy block if it was allocated.
            if (s_newlineByteBlock != null)
            {
                Marshal.FreeHGlobal((IntPtr) s_newlineByteBlock);
                s_newlineByteBlock = null;
            }

            while (true)
            {
                DataBlockInfo dataBlockInfo = this.m_curStream.ReadDataBlock();

                if (dataBlockInfo.blockSize == 0 && this.m_curIdx == m_srcList.Count)
                {
                    // data has been exhausted.  We return the empty block and the caller knows what to do.
                    return dataBlockInfo;
                }
                
                // normal case.. record the last two bytes for newline tracking, and return the block.
                if (dataBlockInfo.blockSize > 0)
                {
                    if (m_appendNewLinesToFiles)
                    {
                        if (dataBlockInfo.blockSize >= 2)
                        {
                            m_trailingBytesOfData[0] = dataBlockInfo.dataBlock[dataBlockInfo.blockSize - 2];
                            m_trailingBytesOfData[1] = dataBlockInfo.dataBlock[dataBlockInfo.blockSize - 1];
                        }
                        else
                        {
                            Debug.Assert(dataBlockInfo.blockSize == 1);
                            // CASE: dataBlockInfo.blockSize == 1
                            // shift left.
                            // We must do this otherwise the following data could fail to be identified
                            //    Blocks = [.........\r] [\n] 
                            m_trailingBytesOfData[0] = m_trailingBytesOfData[1]; //shift
                            m_trailingBytesOfData[1] = dataBlockInfo.dataBlock[0]; // record the single element.
                        }
                    }

                    return dataBlockInfo;
                }

                this.m_curStream.ReleaseDataBlock(dataBlockInfo.itemHandle);
                this.m_curStream.Close();
                this.m_curStream = this.GetStream(this.m_curIdx++, FileAccess.Read);
                
                // we only get here when a file is fully consumed and it wasn't the last file in the set.
                // if the data stream didn't end with a newline-pair, emit one so that
                // LineRecord-parsing will work correctly.
                // the next time we enter this method, we will free the unmanaged data and the
                // next real block of data will be read and consumed.

                if (m_appendNewLinesToFiles)
                {
                    //@@TODO[p3]: we currently only observe and insert \r\n pairs.
                    //            Unicode may have other types of newline to consider.
                    if (m_trailingBytesOfData[0] != '\r' || m_trailingBytesOfData[1] != '\n')
                    {
                        // create the dummy block.
                        if (s_newlineByteBlock == null)
                        {
                            s_newlineByteBlock = (byte*)Marshal.AllocHGlobal(2);
                            s_newlineByteBlock[0] = (byte)'\r';
                            s_newlineByteBlock[1] = (byte)'\n';
                        }
                        DataBlockInfo dummyblock = new DataBlockInfo();
                        dummyblock.blockSize = 2;
                        dummyblock.dataBlock = s_newlineByteBlock;
                        dummyblock.itemHandle = IntPtr.Zero;
                        
                        return dummyblock;
                    }
                }
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
            return ""; // the only use of this is to be put in an error msg.. We don't have an appropriate URI, so we just use the empty string. (bug 13970)
        }
    }
}
