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
using System.Text;
using System.IO;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// DryadLinqBinaryWriter is the main interface for user provided custom serializers
    /// or DL-internal autoserialization codepaths to write primitive types from a partition file.
    /// </summary>
    public unsafe sealed class DryadLinqBinaryWriter
    {
        private const Int32 DefaultBlockSize = 256 * 1024;

        private NativeBlockStream m_nativeStream;
        private Encoding m_encoding;
        private Int32 m_nextBlockSize;
        private Int32 m_bufferSizeHint;

        private DataBlockInfo m_curDataBlockInfo;
        private byte* m_curDataBlock;   // The current write buffer. This is allocated from the native stream, 
        // individual WriteXXX methods serialize primitives into this buffer,
        // and it gets written out when it's full
        private Int32 m_curBlockSize;   // Size of the current write buffer.
        private Int32 m_curRecordStart;
        private Int32 m_curRecordEnd;
        private Int32 m_charMaxByteCount;
        private bool m_isClosed;
        private Int64 m_numBytesWritten;
        private bool m_calcFP;
        private BinaryFormatter m_bfm;

        internal DryadLinqBinaryWriter(NativeBlockStream stream)
            : this(stream, Encoding.UTF8)
        {
        }

        internal DryadLinqBinaryWriter(NativeBlockStream stream, Encoding encoding)
            : this(stream, encoding, DefaultBlockSize)
        {
        }

        internal DryadLinqBinaryWriter(NativeBlockStream stream, Encoding encoding, Int32 buffSize)
        {
            this.m_nativeStream = stream;
            this.m_encoding = encoding;
            this.m_nextBlockSize = Math.Max(DefaultBlockSize, buffSize / 2);
            this.m_bufferSizeHint = buffSize;
            this.m_curDataBlockInfo.DataBlock = null;
            this.m_curDataBlockInfo.BlockSize = 0;
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curDataBlock = this.m_curDataBlockInfo.DataBlock;
            this.m_curBlockSize = this.m_curDataBlockInfo.BlockSize;
            this.m_curRecordStart = 0;
            this.m_curRecordEnd = 0;
            this.m_charMaxByteCount = this.m_encoding.GetMaxByteCount(1);
            this.m_isClosed = false;
            this.m_numBytesWritten = 0;
            this.m_calcFP = false;
            this.m_bfm = new BinaryFormatter();
        }

        internal DryadLinqBinaryWriter(IntPtr vertexInfo, UInt32 portNum, Int32 buffSize)
            : this(new DryadLinqChannel(vertexInfo, portNum, false), Encoding.UTF8, buffSize)
        {
        }

        internal DryadLinqBinaryWriter(IntPtr vertexInfo, UInt32 portNum, Encoding encoding, Int32 buffSize)
            : this(new DryadLinqChannel(vertexInfo, portNum, false), encoding, buffSize)
        {
        }

        ~DryadLinqBinaryWriter()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.Flush();
                if (this.m_curBlockSize > 0)
                {
                    this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                }
                this.m_nativeStream.Close();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        //
        // Internal methods
        //
        internal Int32 BufferSizeHint
        {
            get { return this.m_bufferSizeHint; }
        }

        internal void CompleteWriteRecord()
        {
            this.m_curRecordStart = this.m_curRecordEnd;
        }

        internal bool CalcFP
        {
            get { return this.m_calcFP; }
            set
            {
                this.m_nativeStream.SetCalcFP();
                this.m_calcFP = value;
            }
        }

        internal string GetChannelURI()
        {
            return this.m_nativeStream.GetURI();
        }

        internal Int64 GetTotalLength()
        {
            return this.m_nativeStream.GetTotalLength();
        }

        internal UInt64 GetFingerPrint()
        {
            if (!this.m_calcFP)
            {
                throw new DryadLinqException(DryadLinqErrorCode.FingerprintDisabled,
                                             SR.FingerprintDisabled);
            }
            return this.m_nativeStream.GetFingerPrint();
        }

        /// <summary>
        /// Writes out the current data buffer (equivalent of FlushDataBlock), and calls
        /// Flush on the native stream to ensure all the data makes its way to the disk
        /// </summary>
        internal void Flush()
        {
            if (this.m_curRecordEnd > 0)
            {
                this.m_nativeStream.WriteDataBlock(this.m_curDataBlockInfo.ItemHandle, this.m_curRecordEnd);
                this.m_numBytesWritten += this.m_curRecordEnd;
                this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
                this.m_curDataBlockInfo = this.m_nativeStream.AllocateDataBlock(this.m_curBlockSize);
                this.m_curDataBlock = this.m_curDataBlockInfo.DataBlock;
                this.m_curBlockSize = this.m_curDataBlockInfo.BlockSize;
                this.m_curRecordStart = 0;
                this.m_curRecordEnd = 0;
            }
            this.m_nativeStream.Flush();
        }

        /// <summary>
        /// Internal entry point to flush and close the writer. This is called by the record writer
        /// </summary>
        internal void Close()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.Flush();
                if (this.m_curBlockSize > 0)
                {
                    this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                }
                this.m_nativeStream.Close();
            }

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Private helper to write the current block out to the native stream.
        ///  - it writes out the current data buffer up to the point it was filled
        ///  - it releases the current data block back to the native stream code (which owns the lifecycle of read buffers),
        ///  - then allocated a new buffer from the native stream
        ///  - and updates the internal read buffer pointer and position members
        /// </summary>
        private void FlushDataBlock()
        {
            DataBlockInfo newDataBlockInfo;
            if (this.m_curRecordStart <= 16)
            {
                // The current block is too small for a single record, augment it
                if (this.m_curBlockSize == this.m_nextBlockSize)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.RecordSizeMax2GB, SR.RecordSizeMax2GB);
                }
                newDataBlockInfo = this.m_nativeStream.AllocateDataBlock(this.m_nextBlockSize);
                this.m_nextBlockSize = this.m_nextBlockSize * 2;
                if (this.m_nextBlockSize < 0)
                {
                    this.m_nextBlockSize = 0x7FFFFFF8;
                }
                DryadLinqUtil.memcpy(this.m_curDataBlock, newDataBlockInfo.DataBlock, this.m_curRecordEnd);
            }
            else
            {
                // Write all the complete records in the block, put the partial record in the new block
                newDataBlockInfo = this.m_nativeStream.AllocateDataBlock(this.m_curBlockSize);
                DryadLinqUtil.memcpy(this.m_curDataBlock + this.m_curRecordStart,
                                   newDataBlockInfo.DataBlock,
                                   this.m_curRecordEnd - this.m_curRecordStart);
                this.m_nativeStream.WriteDataBlock(this.m_curDataBlockInfo.ItemHandle, this.m_curRecordStart);
                this.m_numBytesWritten += this.m_curRecordStart;
                this.m_curRecordEnd -= this.m_curRecordStart;
                this.m_curRecordStart = 0;
            }
            this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curDataBlockInfo = newDataBlockInfo;
            this.m_curDataBlock = newDataBlockInfo.DataBlock;
            this.m_curBlockSize = newDataBlockInfo.BlockSize;
        }

        internal Int64 Length
        {
            get
            {
                return this.m_numBytesWritten + this.m_curRecordEnd;
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        //
        // Public methods
        //
        public void Write(byte b)
        {
            if (this.m_curRecordEnd == this.m_curBlockSize)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = b;
        }

        public void Write(sbyte b)
        {
            if (this.m_curRecordEnd == this.m_curBlockSize)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)b;
        }

        public void Write(bool b)
        {
            if (this.m_curRecordEnd == this.m_curBlockSize)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(b ? 1 : 0);
        }

        public void Write(char ch)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < this.m_charMaxByteCount)
            {
                this.FlushDataBlock();
            }

            int numBytes = this.m_encoding.GetBytes(&ch, 1, this.m_curDataBlock + this.m_curRecordEnd, this.m_charMaxByteCount);
            this.m_curRecordEnd += numBytes;
        }

        public void Write(short val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 2)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
        }

        public void Write(ushort val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 2)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
        }

        public void Write(int val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 4)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 16);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 24);
        }

        public void WriteCompact(int val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 4)
            {
                this.FlushDataBlock();
            }
            if (val < 0x80)
            {
                this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            }
            else
            {
                this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 24 | 0x80);
                this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 16);
                this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
                this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            }
        }

        internal static int CompactSize(int val)
        {
            return (val < 0x80) ? 1 : 4;
        }

        private void Write(int val, int loc)
        {
            this.m_curDataBlock[loc++] = (byte)val;
            this.m_curDataBlock[loc++] = (byte)(val >> 8);
            this.m_curDataBlock[loc++] = (byte)(val >> 16);
            this.m_curDataBlock[loc++] = (byte)(val >> 24);
        }

        private void WriteCompact(int val, int compactSize, int loc)
        {
            if (compactSize == 1)
            {
                this.m_curDataBlock[loc++] = (byte)val;
            }
            else
            {
                this.m_curDataBlock[loc++] = (byte)(val >> 24 | 0x80);
                this.m_curDataBlock[loc++] = (byte)(val >> 16);
                this.m_curDataBlock[loc++] = (byte)(val >> 8);
                this.m_curDataBlock[loc++] = (byte)val;
            }
        }

        public void Write(uint val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 4)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 16);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 24);
        }

        public void Write(long val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 8)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 16);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 24);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 32);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 40);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 48);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 56);
        }

        public void Write(ulong val)
        {
            if (this.m_curBlockSize - this.m_curRecordEnd < 8)
            {
                this.FlushDataBlock();
            }
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)val;
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 8);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 16);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 24);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 32);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 40);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 48);
            this.m_curDataBlock[this.m_curRecordEnd++] = (byte)(val >> 56);
        }

        public void Write(decimal val)
        {
            this.WriteRawBytes((byte*)&val, sizeof(decimal));
        }

        public void Write(float val)
        {
            uint tmpVal = *(uint*)&val;
            this.Write(tmpVal);
        }

        public void Write(double val)
        {
            ulong tmpVal = *(ulong*)&val;
            this.Write(tmpVal);
        }

        private const Int32 KindShift = 62;

        public void Write(DateTime val)
        {
            UInt64 tempVal = (UInt64)val.Ticks | (((UInt64)val.Kind) << KindShift);
            this.Write(tempVal);
        }

        public void Write(SqlDateTime val)
        {
            this.Write(val.DayTicks);
            this.Write(val.TimeTicks);
        }

        public void Write(Guid guid)
        {
            WriteRawBytes((byte*)&guid, sizeof(Guid));
        }

        public void Write(string val)
        {
            Int32 len = val.Length;
            Int32 maxByteCount = this.m_encoding.GetMaxByteCount(len);
            Int32 compactSize = CompactSize(maxByteCount);

            while (this.m_curBlockSize - this.m_curRecordEnd < (maxByteCount + 8))
            {
                this.FlushDataBlock();
            }
            this.WriteCompact(len);
            int buffLoc = this.m_curRecordEnd;
            this.m_curRecordEnd += compactSize;
            int numBytes;
            fixed (char* pVal = val)
            {
                numBytes = this.m_encoding.GetBytes(pVal,
                                                    len,
                                                    this.m_curDataBlock + this.m_curRecordEnd,
                                                    this.m_curBlockSize - this.m_curRecordEnd);
            }
            this.m_curRecordEnd += numBytes;
            this.WriteCompact(numBytes, compactSize, buffLoc);
        }

        public void WriteChars(char[] charBuffer, int offset, int charCount)
        {
            if (charBuffer == null)
            {
                throw new ArgumentNullException("charBuffer");
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (charCount < 0)
            {
                throw new ArgumentOutOfRangeException("charCount");
            }
            if (charBuffer.Length < (offset + charCount))
            {
                throw new ArgumentOutOfRangeException("charBuffer",
                                                      String.Format(SR.ArrayLengthVsCountAndOffset,
                                                                    "charBuffer", offset + charCount,
                                                                    "offset", "charCount"));
            }

            Int32 maxByteCount = this.m_encoding.GetMaxByteCount(charCount);

            // if current block doesn't have enough space flush it and allocate a new one
            while (this.m_curBlockSize - this.m_curRecordEnd < maxByteCount )
            {
                this.FlushDataBlock();
            }

            int buffLoc = this.m_curRecordEnd;
            int numBytes;
            fixed (char* pVal = charBuffer )
            {
                numBytes = this.m_encoding.GetBytes(pVal + offset,
                                                    charCount,
                                                    this.m_curDataBlock + this.m_curRecordEnd,
                                                    this.m_curBlockSize - this.m_curRecordEnd);
            }

            this.m_curRecordEnd += numBytes;
        }

        public void WriteBytes(byte[] byteBuffer, int offset, int byteCount)
        {
            if (byteBuffer == null)
            {
                throw new ArgumentNullException("byteBuffer");
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (byteCount < 0)
            {
                throw new ArgumentOutOfRangeException("byteCount");
            }
            if (byteBuffer.Length < (offset + byteCount))
            {
                throw new ArgumentOutOfRangeException("byteBuffer",
                                                      String.Format(SR.ArrayLengthVsCountAndOffset,
                                                                    "byteBuffer", offset + byteCount,
                                                                    "offset", "byteCount"));
            }
            while (this.m_curBlockSize - this.m_curRecordEnd < byteCount)
            {
                this.FlushDataBlock();
            }
            fixed (byte* pBytes = byteBuffer)
            {
                DryadLinqUtil.memcpy(pBytes + offset, this.m_curDataBlock + this.m_curRecordEnd, byteCount);
            }
            this.m_curRecordEnd += byteCount;
        }

        /// <summary>
        /// Public helper to write from a caller provided byte* to the output stream.
        /// This is mainly used to read preallocated fixed size, non-integer types (Guid, decimal etc).
        /// </summary>
        public void WriteRawBytes(byte* pBytes, Int32 numBytes)
        {
            while (this.m_curBlockSize - this.m_curRecordEnd < numBytes)
            {
                this.FlushDataBlock();
            }
            DryadLinqUtil.memcpy(pBytes, this.m_curDataBlock + this.m_curRecordEnd, numBytes);
            this.m_curRecordEnd += numBytes;
        }

        public override string ToString()
        {
            return this.m_nativeStream.ToString();
        }
    }
}

namespace Microsoft.Research.DryadLinq.Internal
{
    // internal adapter class to make a DryadLinqBinaryWriter work as a Stream
    // this is needed to reuse Stream-based serialization code.
    internal class DryadLinqBinaryWriterToStreamAdapter : Stream
    {
        private DryadLinqBinaryWriter m_dbw;

        internal DryadLinqBinaryWriterToStreamAdapter(DryadLinqBinaryWriter dbw)
        {
            m_dbw = dbw;
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            m_dbw.Flush();
        }

        public override long Length
        {
            get { return m_dbw.Length; }
        }

        public override long Position
        {
            get { return m_dbw.Length; }
            set { throw new DryadLinqException(DryadLinqErrorCode.SettingPositionNotSupported,
                                               SR.SettingPositionNotSupported); }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new DryadLinqException(DryadLinqErrorCode.ReadNotAllowed, SR.ReadNotAllowed);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SeekNotSupported, SR.SeekNotSupported);
        }

        public override void SetLength(long value)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SetLengthNotSupported,
                                         SR.SetLengthNotSupported);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            m_dbw.WriteBytes(buffer, offset, count);
        }

        public override void WriteByte(byte value)
        {
            m_dbw.Write(value);
        }

        public override void Close()
        {
            try
            {
                m_dbw.Close();
            }
            finally
            {
                base.Dispose(true);
            }
        }
    }
}
