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
using System.IO.Compression;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    internal unsafe class DryadLinqBlockStream : NativeBlockStream, IDisposable
    {
        private const int DefaultBuffSize = 8192 * 128;

        private Stream m_stream;

        internal DryadLinqBlockStream(Stream stream)
        {
            this.m_stream = stream;
        }

        internal override Int64 GetTotalLength()
        {
            return this.m_stream.Length;
        }

        internal override unsafe DataBlockInfo ReadDataBlock()
        {
            DataBlockInfo blockInfo;
            blockInfo.DataBlock = (byte*)Marshal.AllocHGlobal(DefaultBuffSize);
            blockInfo.ItemHandle = (IntPtr)blockInfo.DataBlock;
            byte[] buffer = new byte[DefaultBuffSize];
            blockInfo.BlockSize = this.m_stream.Read(buffer, 0, DefaultBuffSize);
            fixed (byte* pBuffer = buffer)
            {
                DryadLinqUtil.memcpy(pBuffer, blockInfo.DataBlock, blockInfo.BlockSize);
            }
            return blockInfo;
        }

        internal override unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite)
        {
            byte* dataBlock = (byte*)itemHandle;
            byte[] buffer = new byte[numBytesToWrite];
            fixed (byte* pBuffer = buffer)
            {
                DryadLinqUtil.memcpy(dataBlock, pBuffer, numBytesToWrite);
            }
            this.m_stream.Write(buffer, 0, numBytesToWrite);
            return true;
        }

        internal override unsafe DataBlockInfo AllocateDataBlock(Int32 size)
        {
            DataBlockInfo blockInfo;
            blockInfo.ItemHandle = Marshal.AllocHGlobal((IntPtr)size);
            blockInfo.DataBlock = (byte*)blockInfo.ItemHandle;
            blockInfo.BlockSize = size;
            return blockInfo;
        }

        internal override unsafe void ReleaseDataBlock(IntPtr itemHandle)
        {
            if (itemHandle != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(itemHandle);
            }
        }

        internal override void Flush()
        {
            this.m_stream.Flush();
        }

        internal override void Close()
        {
            this.m_stream.Close();
        }

        public void Dispose()
        {
            this.m_stream.Dispose();
        }
    }

    // This class directly talks to NTFS files. 
    internal unsafe class DryadLinqFileBlockStream : NativeBlockStream
    {
        private const int DefaultBuffSize = 8192*128;
        private const int FILE_FLAG_NO_BUFFERING = 0x20000000;

        private FileStream m_fstream;
        private SafeFileHandle m_fhandle;
        private CompressionScheme m_compressionScheme;
        private bool m_isClosed;
        private Stream m_compressStream;
        
        internal DryadLinqFileBlockStream(FileStream fstream, CompressionScheme scheme)
        {
            this.m_fstream = fstream;
            this.m_fhandle = fstream.SafeFileHandle;
            this.m_compressionScheme = scheme;
            this.m_isClosed = false;
            this.m_compressStream = null;
        }

        private void Initialize(string filePath, FileMode mode, FileAccess access, CompressionScheme scheme)
        {
            try
            {
                FileOptions options = FileOptions.None;
                if (access == FileAccess.Read)
                {
                    options |= FileOptions.SequentialScan;
                    // options |= (FileOptions)FILE_FLAG_NO_BUFFERING;
                }
                else
                {
                    // options |= FileOptions.WriteThrough;
                }
                // options |= FileOptions.Asynchronous;
                this.m_fstream = new FileStream(filePath, mode, access, FileShare.Read, DefaultBuffSize, options);
            }
            catch(Exception e)
            {
                throw new DryadLinqException(DryadLinqErrorCode.CannotAccesFilePath,
                                             String.Format(SR.CannotAccesFilePath , filePath),e);
            }
            this.m_fhandle = m_fstream.SafeFileHandle;
            this.m_isClosed = false;
            this.m_compressionScheme = scheme;
            this.m_compressStream = null;
        }

        internal DryadLinqFileBlockStream(string filePath, FileAccess access, CompressionScheme scheme)
        {
            FileMode mode = (access == FileAccess.Read) ? FileMode.Open : FileMode.OpenOrCreate;
            this.Initialize(filePath, mode, access, scheme);
        }

        internal DryadLinqFileBlockStream(string filePath, FileAccess access)
            : this(filePath, access, CompressionScheme.None)
        {
        }

        internal DryadLinqFileBlockStream(string filePath, FileMode mode, FileAccess access, CompressionScheme scheme)
        {
            this.Initialize(filePath, mode, access, scheme);
        }

        internal DryadLinqFileBlockStream(string filePath, FileMode mode, FileAccess access)
            : this(filePath, mode, access, CompressionScheme.None)
        {
        }

        internal override unsafe Int64 GetTotalLength()
        {
            Int64 totalLen;
            bool success = DryadLinqNative.GetFileSizeEx(this.m_fhandle, out totalLen);
            if (!success)
            {
                throw new DryadLinqException(DryadLinqErrorCode.GetFileSizeError,
                                             String.Format(SR.GetFileSizeError,
                                                           Marshal.GetLastWin32Error()));
            }
            return totalLen;
        }
        
        internal override unsafe DataBlockInfo ReadDataBlock()
        {
            DataBlockInfo blockInfo;
            blockInfo.DataBlock = (byte*)Marshal.AllocHGlobal(DefaultBuffSize);
            blockInfo.ItemHandle = (IntPtr)blockInfo.DataBlock;
            if (this.m_compressionScheme == CompressionScheme.None)
            {
                Int32* pBlockSize = &blockInfo.BlockSize;
                bool success = DryadLinqNative.ReadFile(this.m_fhandle,
                                                      blockInfo.DataBlock,
                                                      DefaultBuffSize,
                                                      (IntPtr)pBlockSize,
                                                      null);
                if (!success)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.ReadFileError,
                                                 String.Format(SR.ReadFileError,
                                                               Marshal.GetLastWin32Error()));
                }
            }
            else
            {
                if (this.m_compressStream == null)
                {
                    if (this.m_compressionScheme == CompressionScheme.Gzip)
                    {
                        this.m_compressStream = new GZipStream(this.m_fstream,
                                                               CompressionMode.Decompress);
                    }
                    else
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.UnknownCompressionScheme,
                                                     SR.UnknownCompressionScheme);
                    }
                }
                // YY: Made an extra copy here. Could do better.
                byte[] buffer = new byte[DefaultBuffSize];
                blockInfo.BlockSize = this.m_compressStream.Read(buffer, 0, DefaultBuffSize);
                fixed (byte* pBuffer = buffer)
                {
                    DryadLinqUtil.memcpy(pBuffer, blockInfo.DataBlock, blockInfo.BlockSize);
                }
            }
            return blockInfo;
        }

        internal override unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite)
        {
            byte* dataBlock = (byte*)itemHandle;
            if (this.m_compressionScheme == CompressionScheme.None)
            {
                Int32 numBytesWritten = 0;
                Int32 remainingBytes = numBytesToWrite;

                while (remainingBytes > 0)
                {
                    Int32* pNumBytesWritten = &numBytesWritten;
                    bool success = DryadLinqNative.WriteFile(this.m_fhandle,
                                                           dataBlock,
                                                           (UInt32)remainingBytes,
                                                           (IntPtr)pNumBytesWritten,
                                                           null);
                    if (!success)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.WriteFileError,
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
                    if (this.m_compressionScheme == CompressionScheme.Gzip)
                    {
                        this.m_compressStream = new GZipStream(this.m_fstream,
                                                               CompressionMode.Compress);
                    }
                    else
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.UnknownCompressionScheme,
                                                     SR.UnknownCompressionScheme);
                    }
                }
                // YY: Made an extra copy here. Could do better.
                byte[] buffer = new byte[numBytesToWrite];
                fixed (byte* pBuffer = buffer)
                {
                    DryadLinqUtil.memcpy(dataBlock, pBuffer, numBytesToWrite);
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
            blockInfo.ItemHandle = Marshal.AllocHGlobal((IntPtr)size);
            blockInfo.DataBlock = (byte*)blockInfo.ItemHandle;
            blockInfo.BlockSize = size;
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
