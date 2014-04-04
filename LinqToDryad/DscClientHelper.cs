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
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.IO.Compression;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Win32.SafeHandles;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    internal class DscIOStream : System.IO.Stream
    {
        private DscService m_dscClient;
        private string m_fileSetName;
        private FileAccess m_mode;
        private FileStream m_fstream;
        private DscFileSet m_dscFileSet;
        private IEnumerator<DscFile> m_dscFileEnumerator;
        private ulong size = 0;
        private bool m_atEOF;
        private CompressionScheme m_compressionScheme;

        public DscIOStream(string streamName, FileAccess access, CompressionScheme compressionScheme)
        {
            if (String.IsNullOrEmpty(streamName))
            {
                throw new ArgumentNullException("streamName");
            }

            Uri streamUri = new Uri(streamName);
            this.m_dscClient = new DscService(streamUri.Host);
            this.m_fileSetName = streamUri.LocalPath;
            this.m_mode = access;
            this.m_fstream = null;
            this.m_atEOF = false;
            this.m_compressionScheme = compressionScheme;
            
            if (access == FileAccess.Read)
            {
                this.m_dscFileSet = this.m_dscClient.GetFileSet(streamName);
                this.m_dscFileEnumerator = this.m_dscFileSet.GetFiles().GetEnumerator();
            }
            else if (access == FileAccess.Write)
            {
                this.m_dscFileSet = this.m_dscClient.CreateFileSet(streamName, compressionScheme);
            }
            else
            {
                throw new ArgumentException(SR.ReadWriteNotSupported, "access");
            }
        }

        public DscIOStream(string streamName,
                           FileAccess access,
                           FileMode createMode,
                           CompressionScheme compressionScheme)
        {
            if (String.IsNullOrEmpty(streamName))
            {
                throw new ArgumentNullException("streamName");
            }

            Uri streamUri = new Uri(streamName);
            this.m_dscClient = new DscService(streamUri.Host);
            this.m_fileSetName = streamUri.LocalPath.TrimStart('/');
            this.m_mode = access;
            this.m_compressionScheme = compressionScheme;

            bool streamExists = this.m_dscClient.FileSetExists(this.m_fileSetName);

            if (access == FileAccess.Read)
            {
                switch (createMode)
                {
                    case FileMode.Open:
                    case FileMode.OpenOrCreate:
                    {
                        if (!streamExists)
                        {
                            throw new FileNotFoundException(String.Format(SR.StreamDoesNotExist , streamName));
                        }
                        break;
                    }
                    case FileMode.Append:
                    case FileMode.Create:
                    case FileMode.CreateNew:
                    case FileMode.Truncate:
                    {
                        throw new NotSupportedException();
                    }
                    default:
                    {
                        throw new NotSupportedException();
                    }
                }
                
                this.m_dscFileSet = this.m_dscClient.GetFileSet(streamName);
                this.m_dscFileEnumerator = this.m_dscFileSet.GetFiles().GetEnumerator();
            }
            else if (access == FileAccess.Write)
            {
                switch (createMode)
                {
                    case FileMode.Append:
                        if (!streamExists)
                        {
                            this.m_dscFileSet = this.m_dscClient.CreateFileSet(this.m_fileSetName, this.m_compressionScheme);
                        }
                        break;
                    case FileMode.Create:
                        if (streamExists)
                        {
                            this.m_dscClient.DeleteFileSet(this.m_fileSetName);
                        }
                        this.m_dscFileSet = this.m_dscClient.CreateFileSet(this.m_fileSetName, this.m_compressionScheme);
                        break;
                    case FileMode.CreateNew:
                        if (streamExists)
                        {
                            throw new IOException(String.Format(SR.StreamAlreadyExists, streamName));
                        }
                        break;
                    case FileMode.Truncate:
                        if (streamExists)
                        {
                            this.m_dscClient.DeleteFileSet(this.m_fileSetName);
                        }
                        this.m_dscFileSet = this.m_dscClient.CreateFileSet(this.m_fileSetName, this.m_compressionScheme);
                        break;
                    case FileMode.Open:
                    case FileMode.OpenOrCreate: // TODO: this should be dealt with correctly, 
                                                // although it's not obvious what open should do 
                        throw new NotSupportedException("'" + createMode.ToString() + "' not supported");
                }
            }
            else
            {
                throw new ArgumentException(SR.ReadWriteNotSupported, "access");
            }

            this.m_fstream = null;
            this.m_atEOF = false;
        }

        public override bool CanRead
        {
            get {
                return this.m_mode == FileAccess.Read;
            }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get {
                return this.m_mode == FileAccess.Write;
            }
        }

        public override void Close()
        {
            try
            {
                if (this.m_fstream != null && this.m_mode == FileAccess.Write)
                {
                    this.SealPartition();
                }
            }
            finally
            {
                this.m_dscClient.Close();
            }
        }

        public override void Flush()
        {
            if (this.m_fstream != null)
            {
                this.m_fstream.Flush();
            }
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        private void OpenForRead()
        {
            Debug.Assert(this.m_fstream == null);

            if (this.m_dscFileEnumerator.MoveNext())
            {
                // TODO(bug 15879): Should failover to other readpath on failure if available
                string path = this.m_dscFileEnumerator.Current.ReadPaths[0];
                this.m_fstream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4 * 65536, false);
            }
            else
            {
                this.m_atEOF = true;
            }
        }

        private void OpenForWrite(bool synchronously)
        {
            if (this.m_fstream != null)
            {
                throw new InvalidOperationException();
            }

            // @@TODO: Should try to estimate size
            DscFile dscFile = this.m_dscFileSet.AddNewFile(1);
            this.m_fstream = new FileStream(dscFile.WritePath, FileMode.Create, FileAccess.Write,
                                            FileShare.None, 4 * 65536, synchronously);
        }

        internal void SealPartition()
        {
            if (this.m_fstream != null)
            {
                this.m_fstream.Close();
                this.m_fstream = null;

                this.m_dscFileSet.Seal();
            }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (this.m_mode == FileAccess.Write)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AttemptToReadFromAWriteStream,
                                             SR.AttemptToReadFromAWriteStream);
            }
            int totalBytesRead = 0;
            while (totalBytesRead < count && !this.m_atEOF)
            {
                if (this.m_fstream == null)
                {
                    this.OpenForRead();
                    if (this.m_atEOF)
                    {
                        break; // we hit EOF (EOS, really), so fall out of the loop
                    }
                }
                int bytesRead = this.m_fstream.Read(buffer, offset + totalBytesRead, count - totalBytesRead);
                totalBytesRead += bytesRead;
                if (bytesRead == 0)
                {
                    this.m_fstream.Close();
                    this.m_fstream = null;
                }
            }
            return totalBytesRead;
        }

        internal unsafe int Read(byte* buffer, int bufferSize)
        {
            int totalBytesRead = 0;
            do
            {
                SafeFileHandle handle;
                if (this.m_fstream == null)
                {
                    this.OpenForRead();
                }
                if (this.m_atEOF) break;

                handle = this.m_fstream.SafeFileHandle;
                int size = 0;
                Int32* pBlockSize = &size;
                bool success = DryadLinqNative.ReadFile(handle, buffer, (UInt32)bufferSize, (IntPtr)pBlockSize, null);
                if (!success)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.ReadFileError,
                                                 String.Format(SR.ReadFileError, Marshal.GetLastWin32Error()));
                }
                totalBytesRead += size;

                if (size == 0)
                {
                    this.m_fstream.Close();
                    this.m_fstream = null;
                }
            } while (totalBytesRead == 0 && !this.m_atEOF);

            return totalBytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public unsafe int Write(byte* buffer, int offset, int count)
        {
            if (this.m_mode == FileAccess.Read)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AttemptToReadFromAWriteStream,
                                             SR.AttemptToReadFromAWriteStream);
            }
            if (this.m_fstream == null)
            {
                this.OpenForWrite(false);
            }

            SafeFileHandle handle = this.m_fstream.SafeFileHandle;
            int size;
            Int32* pBlockSize = &size;
            
            bool success = DryadLinqNative.WriteFile(handle, buffer, (UInt32)count, (IntPtr)pBlockSize, null);
            if (!success)
            {
                throw new DryadLinqException(DryadLinqErrorCode.WriteFileError,
                                             String.Format(SR.WriteFileError, Marshal.GetLastWin32Error()));
            }

            this.size += (ulong)size;
            return size;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.m_mode == FileAccess.Read)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AttemptToReadFromAWriteStream,
                                             SR.AttemptToReadFromAWriteStream);
            }
            if (this.m_fstream == null)
            {
                this.OpenForWrite(true);
            }
            this.size += (ulong)count;
            this.m_fstream.Write(buffer, offset, count);
        }
    }

    /// <summary>
    /// Handle interaction between DryadLINQ serialization and DSC streams.
    /// </summary>
    internal unsafe class DscBlockStream : NativeBlockStream
    {
        private const int DefaultBuffSize = 8192*32;

        private DscIOStream m_dscStream;
        private CompressionScheme m_compressionScheme;
        private bool m_isClosed;
        private Stream m_compressStream;

        public DscBlockStream(DscIOStream dscStream, CompressionScheme scheme)
        {
            this.m_dscStream = dscStream;
            this.m_compressionScheme = scheme;
            this.m_isClosed = false;
            this.m_compressStream = null;
        }

        private void Initialize(string filePath,
                                FileMode mode,
                                FileAccess access,
                                CompressionScheme scheme)
        {
            try
            {
                this.m_dscStream = new DscIOStream(filePath, access, mode, scheme);
            }
            catch (Exception e)
            {
                throw new DryadLinqException(DryadLinqErrorCode.FailedToCreateStream,
                                             String.Format(SR.FailedToCreateStream, filePath), e);
            }
            this.m_isClosed = false;
            this.m_compressionScheme = scheme;
            this.m_compressStream = null;
        }

        public DscBlockStream(string filePath, FileAccess access, CompressionScheme scheme)
        {
            FileMode mode = (access == FileAccess.Read) ? FileMode.Open : FileMode.OpenOrCreate;
            this.Initialize(filePath, mode, access, scheme);
        }

        public DscBlockStream(string filePath, FileMode mode, FileAccess access, CompressionScheme scheme)
        {
            this.Initialize(filePath, mode, access, scheme);
        }

        internal override Int64 GetTotalLength()
        {
            return (Int64)this.m_dscStream.Length;
        }

        internal override DataBlockInfo ReadDataBlock()
        {
            DataBlockInfo blockInfo;
            blockInfo.DataBlock = (byte*)Marshal.AllocHGlobal(DefaultBuffSize);
            blockInfo.ItemHandle = (IntPtr)blockInfo.DataBlock;
            if (this.m_compressionScheme == CompressionScheme.None)
            {
                blockInfo.BlockSize = this.m_dscStream.Read(blockInfo.DataBlock, DefaultBuffSize);
            }
            else
            {
                if (this.m_compressStream == null)
                {
                    if (this.m_compressionScheme == CompressionScheme.Gzip)
                    {
                        this.m_compressStream = new GZipStream(this.m_dscStream, CompressionMode.Decompress);
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
                    numBytesWritten = this.m_dscStream.Write(dataBlock, 0, remainingBytes);
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
                        this.m_compressStream = new GZipStream(this.m_dscStream, CompressionMode.Compress);
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
            this.m_dscStream.Flush();
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
                this.m_dscStream.Close();
                this.m_compressStream = null;
                this.m_dscStream = null;
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
