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
using System.IO.Compression;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using Microsoft.Research.DryadLinq;


namespace Microsoft.Research.DryadLinq.Internal
{
    // This class directly talks to NTFS files. 
    internal unsafe class HpcLinqFileStream : NativeBlockStream
    {
        private const int DefaultBuffSize = 8192*32;

        private FileStream m_fstream;
        private SafeFileHandle m_fhandle;
        private DscCompressionScheme m_compressionScheme;
        private bool m_isClosed;
        private Stream m_compressStream;
        
        internal HpcLinqFileStream(FileStream fstream, DscCompressionScheme scheme)
        {
            this.m_fstream = fstream;
            this.m_fhandle = fstream.SafeFileHandle;
            this.m_compressionScheme = scheme;
            this.m_isClosed = false;
            this.m_compressStream = null;
        }

        private void Initialize(string filePath, FileMode mode, FileAccess access, DscCompressionScheme scheme)
        {
            try
            {
                this.m_fstream = new FileStream(filePath, mode, access);
            }
            catch(Exception e)
            {
                throw new DryadLinqException(HpcLinqErrorCode.CannotAccesFilePath,
                                           String.Format(SR.CannotAccesFilePath , filePath),e);
            }
            this.m_fhandle = m_fstream.SafeFileHandle;
            this.m_isClosed = false;
            this.m_compressionScheme = scheme;
            this.m_compressStream = null;
        }

        internal HpcLinqFileStream(string filePath, FileAccess access, DscCompressionScheme scheme)
        {
            FileMode mode = (access == FileAccess.Read) ? FileMode.Open : FileMode.OpenOrCreate;
            Initialize(filePath, mode, access, scheme);
        }

        internal HpcLinqFileStream(string filePath, FileAccess access)
            : this(filePath, access, DscCompressionScheme.None)
        {
        }

        internal HpcLinqFileStream(string filePath, FileMode mode, FileAccess access, DscCompressionScheme scheme)
        {
            Initialize(filePath, mode, access, scheme);
        }

        internal HpcLinqFileStream(string filePath, FileMode mode, FileAccess access)
            : this(filePath, mode, access, DscCompressionScheme.None)
        {
        }

        internal override unsafe Int64 GetTotalLength()
        {
            Int64 totalLen;
            bool success = HpcLinqNative.GetFileSizeEx(this.m_fhandle, out totalLen);
            if (!success)
            {
                throw new DryadLinqException(HpcLinqErrorCode.GetFileSizeError,
                                           String.Format(SR.GetFileSizeError,
                                                         Marshal.GetLastWin32Error()));
            }
            return totalLen;
        }
        
        internal override unsafe DataBlockInfo ReadDataBlock()
        {
            DataBlockInfo blockInfo;
            blockInfo.dataBlock = (byte*)Marshal.AllocHGlobal(DefaultBuffSize);
            blockInfo.itemHandle = (IntPtr)blockInfo.dataBlock;
            if (this.m_compressionScheme == DscCompressionScheme.None)
            {
                Int32* pBlockSize = &blockInfo.blockSize;
                bool success = HpcLinqNative.ReadFile(this.m_fhandle,
                                                        blockInfo.dataBlock,
                                                        DefaultBuffSize,
                                                        (IntPtr)pBlockSize,
                                                        null);
                if (!success)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.ReadFileError,
                                               String.Format(SR.ReadFileError,
                                                             Marshal.GetLastWin32Error()));
                }
            }
            else
            {
                if (this.m_compressStream == null)
                {
                    if (this.m_compressionScheme == DscCompressionScheme.Gzip)
                    {
                        this.m_compressStream = new GZipStream(this.m_fstream,
                                                               CompressionMode.Decompress);
                    }
                    else
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.UnknownCompressionScheme,
                                                   SR.UnknownCompressionScheme);
                    }
                }
                // YY: Made an extra copy here. Could do better.
                byte[] buffer = new byte[DefaultBuffSize];
                blockInfo.blockSize = this.m_compressStream.Read(buffer, 0, DefaultBuffSize);
                fixed (byte* pBuffer = buffer)
                {
                    HpcLinqUtil.memcpy(pBuffer, blockInfo.dataBlock, blockInfo.blockSize);
                }
            }
            
            return blockInfo;
        }

        internal override unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite)
        {
            byte* dataBlock = (byte*)itemHandle;
            if (this.m_compressionScheme == DscCompressionScheme.None)
            {
                Int32 numBytesWritten = 0;
                Int32 remainingBytes = numBytesToWrite;

                while (remainingBytes > 0)
                {
                    Int32* pNumBytesWritten = &numBytesWritten;
                    bool success = HpcLinqNative.WriteFile(this.m_fhandle,
                                                             dataBlock,
                                                             (UInt32)remainingBytes,
                                                             (IntPtr)pNumBytesWritten,
                                                             null);
                    if (!success)
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.WriteFileError,
                                                   String.Format(SR.WriteFileError,
                                                                 Marshal.GetLastWin32Error()));
                    }

                    dataBlock += numBytesWritten;
                    remainingBytes -= numBytesWritten;
                }
            }
            else
            {
                if (this.m_compressStream == null)
                {
                    if (this.m_compressionScheme == DscCompressionScheme.Gzip)
                    {
                        this.m_compressStream = new GZipStream(this.m_fstream,
                                                               CompressionMode.Compress);
                    }
                    else
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.UnknownCompressionScheme,
                                                   SR.UnknownCompressionScheme);
                    }
                }
                // YY: Made an extra copy here. Could do better.
                byte[] buffer = new byte[numBytesToWrite];
                fixed (byte* pBuffer = buffer)
                {
                    HpcLinqUtil.memcpy(dataBlock, pBuffer, numBytesToWrite);
                }
                this.m_compressStream.Write(buffer, 0, numBytesToWrite);
            }
            return true;
        }

        internal override void Flush()
        {
           if (this.m_compressStream != null)
           {
               this.m_compressStream.Flush();
           }
           this.m_fstream.Flush();
        }

        internal override void Close()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                if (this.m_compressStream != null)
                {
                    this.m_compressStream.Close();
                }
                this.m_fstream.Close();
            }
        }

        internal override unsafe DataBlockInfo AllocateDataBlock(Int32 size)
        {
            DataBlockInfo blockInfo;
            blockInfo.itemHandle = Marshal.AllocHGlobal((IntPtr)size);
            blockInfo.dataBlock = (byte*)blockInfo.itemHandle;
            blockInfo.blockSize = size;
            return blockInfo;
        }

        internal override unsafe void ReleaseDataBlock(IntPtr itemHandle)
        {
            if (itemHandle != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(itemHandle);
            }
        }
    }
}
