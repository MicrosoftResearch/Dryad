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
using System.Runtime.InteropServices;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// DryadLinqBinaryReader is the main interface for user provided custom serializers
    /// or DL-internal autoserialization codepaths to read primitive types from a partition file.
    /// </summary>
    public unsafe sealed class DryadLinqBinaryReader
    {
        private NativeBlockStream m_nativeStream;
        private Encoding m_encoding;
        private Decoder m_decoder;
        
        private DataBlockInfo m_curDataBlockInfo;
        private byte* m_curDataBlock;   // The current read buffer. This is requested from the native stream as it depletes, 
                                        // and individual ReadXXX methods deserialize primitives out of this buffer
        private Int32 m_curBlockSize;   // Size of the current read buffer.
        private Int32 m_curBlockPos;    // Current position on the read buffer. This is updated by individual
                                        // ReadXXX methods as they pull bytes for primitives they are reading
        
        private bool m_isClosed;
        
        internal DryadLinqBinaryReader(NativeBlockStream stream)
            : this(stream, Encoding.UTF8)
        {
        }

        internal DryadLinqBinaryReader(NativeBlockStream stream, Encoding encoding)
        {
            this.m_nativeStream = stream;
            this.m_encoding = encoding;
            this.m_decoder = encoding.GetDecoder();
            this.m_curDataBlockInfo.DataBlock = null;
            this.m_curDataBlockInfo.BlockSize = -1;
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curDataBlock = this.m_curDataBlockInfo.DataBlock;
            this.m_curBlockSize = this.m_curDataBlockInfo.BlockSize;
            this.m_curBlockPos = -1;
            this.m_isClosed = false;
        }

        internal DryadLinqBinaryReader(IntPtr vertexInfo, UInt32 portNum)
            : this(new DryadLinqChannel(vertexInfo, portNum, true), Encoding.UTF8)
        {
        }

        internal DryadLinqBinaryReader(IntPtr vertexInfo, UInt32 portNum, Encoding encoding)
            : this(new DryadLinqChannel(vertexInfo, portNum, true), encoding)
        {
        }

        ~DryadLinqBinaryReader()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                this.m_nativeStream.Close();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        //
        // Internal methods
        //
        internal Int64 GetTotalLength()
        {
            return this.m_nativeStream.GetTotalLength();
        }

        internal long Length
        {
            get { return this.m_curBlockSize; }
        }

        internal void Close()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                this.m_nativeStream.Close();
            }
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Private helper to request a new data block from the native stream.
        ///  - it releases the current data block back to the native stream code
        ///    (which owns the lifecycle of read buffers),
        ///  - then requests a new buffer from the native stream
        ///  - and updates the internal read buffer pointer (m_curDataBlock), size (m_curDataSize)
        ///    and position, all of which are needed by subsequent Read*() calls coming from the user
        /// </summary>
        private unsafe void GetNextDataBlock()
        {
            this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curDataBlockInfo = this.m_nativeStream.ReadDataBlock();
            this.m_curDataBlock = this.m_curDataBlockInfo.DataBlock;
            this.m_curBlockSize = this.m_curDataBlockInfo.BlockSize;
            this.m_curBlockPos = 0;
        }

        internal string GetChannelURI()
        {
            return this.m_nativeStream.GetURI();
        }

        ////////////////////////////////////////////////////////////////////////////////
        //
        // Public methods
        //
        public override string ToString()
        {
            return this.m_nativeStream.ToString();
        }

        /// <summary>
        /// Helper used by DryadLinqRecordReader and generated vertex code to check whether the
        /// reader reached the end of stream.
        /// Returns true if the reader is at the end of the stream, and false is there is
        /// more data to read.
        /// This check may cause a new data block to be fetched if the call happens while
        /// we're at the end of the current read buffer.
        /// </summary>
        internal bool EndOfStream()
        {
            if (this.m_curBlockPos < this.m_curBlockSize)
            {
                return false;
            }

            if (this.m_curBlockSize == 0)
            {
                return true;
            }

            this.GetNextDataBlock();
            return (this.m_curBlockSize <= 0);
        }
        
        /// <summary>
        /// Read a byte from the current reader and advances the current position of the
        /// reader by one byte.
        /// </summary>
        /// <returns>The next byte read from the current reader.</returns>
        public byte ReadUByte()
        {
            if (this.m_curBlockPos == this.m_curBlockSize)
            {
                this.GetNextDataBlock();
                if (this.m_curBlockSize <= 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                 String.Format(SR.EndOfStreamEncountered,
                                                               GetChannelURI()));
                }
            }
            return this.m_curDataBlock[this.m_curBlockPos++];
        }

        /// <summary>
        /// Read a signed byte from the current reader and advances the current
        /// position of the reader by one byte.
        /// </summary>
        /// <returns>The next signed byte read from the current reader.</returns>
        public sbyte ReadSByte()
        {
            if (this.m_curBlockPos == this.m_curBlockSize)
            {
                this.GetNextDataBlock();
                if (this.m_curBlockSize <= 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                 String.Format(SR.EndOfStreamEncountered,
                                                               GetChannelURI()));
                }
            }
            return (sbyte)this.m_curDataBlock[this.m_curBlockPos++];
        }

        /// <summary>
        /// Read a boolean value from the current reader and advances the current
        /// position of the reader by one byte.
        /// </summary>
        /// <returns>true iff the byte is nonzero.</returns>
        public unsafe bool ReadBool()
        {
            if (this.m_curBlockPos == this.m_curBlockSize)
            {
                this.GetNextDataBlock();
                if (this.m_curBlockSize <= 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                 String.Format(SR.EndOfStreamEncountered,
                                                               GetChannelURI()));
                }
            }
            byte res = this.m_curDataBlock[this.m_curBlockPos++];
            return (res == 0) ? false : true;
        }

        /// <summary>
        /// Read a character from the current reader and advances the current position of the reader
        /// according to the encoding and the character.
        /// </summary>
        /// <returns>A character read from the current reader.</returns>
        public char ReadChar()
        {
            char ch;
            char *pCh = &ch;

            while (true)
            {
                // request a new buffer from the native stream if we're at the end of the current one
                if (this.m_curBlockPos == this.m_curBlockSize)
                {
                    this.GetNextDataBlock();
                    if (this.m_curBlockSize <= 0)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                     String.Format(SR.EndOfStreamEncountered,
                                                                   GetChannelURI()));
                    }
                }

                // decode a character and update current position
                int numChars = this.m_decoder.GetChars(this.m_curDataBlock + this.m_curBlockPos, 1, pCh, 1, false);
                this.m_curBlockPos++;
                if (numChars == 1) return ch;
            }
        }
        
        public short ReadInt16()
        {
            ushort low, high;
            if (this.m_curBlockSize < this.m_curBlockPos + 2)
            {
                low = this.ReadUByte();
                high = this.ReadUByte();
            }
            else
            {
                low = this.m_curDataBlock[this.m_curBlockPos++];
                high = this.m_curDataBlock[this.m_curBlockPos++];
            }
            return (short)(low | (high << 8));
        }

        public ushort ReadUInt16()
        {
            ushort low, high;
            if (this.m_curBlockSize < this.m_curBlockPos + 2)
            {
                low = this.ReadUByte();
                high = this.ReadUByte();
            }
            else
            {
                low = this.m_curDataBlock[this.m_curBlockPos++];
                high = this.m_curDataBlock[this.m_curBlockPos++];
            }
            return (ushort)(low | (high << 8));
        }

        public int ReadInt32()
        {
            int b1, b2, b3, b4;
            if (this.m_curBlockSize < this.m_curBlockPos + 4)
            {
                b1 = this.ReadUByte();
                b2 = this.ReadUByte() << 8;
                b3 = this.ReadUByte() << 16;
                b4 = this.ReadUByte() << 24;
            }
            else
            {
                b1 = this.m_curDataBlock[this.m_curBlockPos++];
                b2 = this.m_curDataBlock[this.m_curBlockPos++] << 8;
                b3 = this.m_curDataBlock[this.m_curBlockPos++] << 16;
                b4 = this.m_curDataBlock[this.m_curBlockPos++] << 24;
            }
            return (int)(b1 | b2 | b3 | b4);
        }

        public unsafe int ReadCompactInt32()
        {
            int b1, b2, b3, b4;
            b1 = this.ReadUByte();
            if (b1 < 0x80)
            {
                return b1;
            }
            else
            {
                b1 = (b1 & 0x7F) << 24;
                if (this.m_curBlockSize < this.m_curBlockPos + 3)
                {
                    b2 = this.ReadUByte() << 16;
                    b3 = this.ReadUByte() << 8;
                    b4 = this.ReadUByte();
                }
                else
                {
                    b2 = this.m_curDataBlock[this.m_curBlockPos++] << 16;
                    b3 = this.m_curDataBlock[this.m_curBlockPos++] << 8;
                    b4 = this.m_curDataBlock[this.m_curBlockPos++];
                }
                return (int)(b1 | b2 | b3 | b4);
            }
        }
        
        public uint ReadUInt32()
        {
            int b1, b2, b3, b4;
            if (this.m_curBlockSize < this.m_curBlockPos + 4)
            {
                b1 = this.ReadUByte();
                b2 = this.ReadUByte() << 8;
                b3 = this.ReadUByte() << 16;
                b4 = this.ReadUByte() << 24;
            }
            else
            {
                b1 = this.m_curDataBlock[this.m_curBlockPos++];
                b2 = this.m_curDataBlock[this.m_curBlockPos++] << 8;
                b3 = this.m_curDataBlock[this.m_curBlockPos++] << 16;
                b4 = this.m_curDataBlock[this.m_curBlockPos++] << 24;
            }
            return (uint)(b1 | b2 | b3 | b4);
        }

        public long ReadInt64()
        {
            uint lo, hi;
            if (this.m_curBlockSize < this.m_curBlockPos + 8)
            {
                lo = this.ReadUInt32();
                hi = this.ReadUInt32();
            }
            else
            {
                lo = (uint)(this.m_curDataBlock[this.m_curBlockPos++] |
                            this.m_curDataBlock[this.m_curBlockPos++] << 8 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 16 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 24);
                hi = (uint)(this.m_curDataBlock[this.m_curBlockPos++] |
                            this.m_curDataBlock[this.m_curBlockPos++] << 8 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 16 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 24);
            }
            return (long)(((ulong)hi) << 32 | lo);
        }

        public ulong ReadUInt64()
        {
            uint lo, hi;
            if (this.m_curBlockSize < this.m_curBlockPos + 8)
            {
                lo = this.ReadUInt32();
                hi = this.ReadUInt32();
            }
            else
            {
                lo = (uint)(this.m_curDataBlock[this.m_curBlockPos++] |
                            this.m_curDataBlock[this.m_curBlockPos++] << 8 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 16 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 24);
                hi = (uint)(this.m_curDataBlock[this.m_curBlockPos++] |
                            this.m_curDataBlock[this.m_curBlockPos++] << 8 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 16 |
                            this.m_curDataBlock[this.m_curBlockPos++] << 24);
            }
            return ((ulong)hi) << 32 | lo;
        }

        public float ReadSingle()
        {
            int tmp = this.ReadInt32();
            return *((float*)&tmp);
        }
        
        public decimal ReadDecimal()
        {
            decimal val;
            this.ReadRawBytes((byte*)&val, sizeof(decimal));
            return val;
        }

        public double ReadDouble()
        {
            ulong tmp = this.ReadUInt64();
            return *((double*)&tmp);
        }

        private const Int64 TicksMask = 0x3FFFFFFFFFFFFFFF;
        private const Int32 KindShift  = 62;    

        public DateTime ReadDateTime()
        {
            UInt64 value = this.ReadUInt64();
            return new DateTime((Int64)(value & TicksMask), (DateTimeKind)(value >> KindShift));
        }

        public SqlDateTime ReadSqlDateTime()
        {
            int dayTicks = this.ReadInt32();
            int timeTicks = this.ReadInt32();
            return new SqlDateTime(dayTicks, timeTicks);
        }

        public Guid ReadGuid()
        {
            Guid guid;
            ReadRawBytes((byte*)&guid, sizeof(Guid));
            return guid;
        }


        /// <summary>
        /// Reads <paramref name="charCount"/> chars into <paramref name="destBuffer"/> starting at <paramref name="offset"/>.        
        /// </summary>
        /// <param name="destBuffer">The pre-allocated char array to read data into.</param>
        /// <param name="offset">The starting offset at which to begin reading chars into <paramref name="destBuffer"/>.</param>
        /// <param name="charCount">The maximum number of chars to read. Must be smaller than or equal to (<paramref name="destArray.Length"/> - <paramref name="offset"/>). </param>
        /// <returns>The number of chars that was actually read.</returns>
        public unsafe int ReadChars(char[] destBuffer, int offset, int charCount)
        {
            if (destBuffer == null)
            {
                throw new ArgumentNullException("destBuffer");
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (charCount < 0)
            {
                throw new ArgumentOutOfRangeException("charCount");
            }
            if (destBuffer.Length < (offset + charCount))
            {
                throw new ArgumentOutOfRangeException("destBuffer",
                                                      String.Format(SR.ArrayLengthVsCountAndOffset,
                                                                    "destBuffer", offset + charCount,
                                                                    "offset", "charCount"));
            }

            Int32 numChars = charCount;
            Int32 numMaxBytes = m_encoding.GetMaxByteCount(charCount);  // note numMaxBytes may not always equal the actual bytes consumed in the conversion
            int numCharsDecoded = 0;
            
            while (numChars > 0)
            {
                // check if there's enough data in the read buffer to finish the conversion
                // if so do it in a single step, adjust read buffer location and return
                int numAvailBytes = this.m_curBlockSize - this.m_curBlockPos;
                if (numAvailBytes >= numMaxBytes)
                {
                    int bytesUsed;
                    int charsConverted;
                    bool completed;

                    fixed (char *pChars = destBuffer)
                    {                       
                        this.m_decoder.Convert(this.m_curDataBlock + this.m_curBlockPos,
                            numMaxBytes,
                            pChars + offset + numCharsDecoded,
                            numChars,
                            false,
                            out bytesUsed,
                            out charsConverted,
                            out completed);
                    }

                    this.m_curBlockPos += bytesUsed;
                    numCharsDecoded = charCount;
                    return numCharsDecoded;
                }

                // The remaining bytes in the read buffer don't *seem to be* enough to satisfy
                // the request, but attempt to convert all the remaining bytes, and adjust
                // current pos etc. before requesting a new buffer
                if (numAvailBytes != 0)
                {
                    int bytesUsed;
                    int charsConverted;
                    bool completed;

                    fixed (char *pChars = destBuffer)
                    {
                        this.m_decoder.Convert(this.m_curDataBlock + this.m_curBlockPos,
                                               numAvailBytes,
                                               pChars + offset + numCharsDecoded,
                                               numChars,
                                               false,
                                               out bytesUsed,
                                               out charsConverted,
                                               out completed);
                    }
                    numChars -= charsConverted;     // update the number of remaining chars to convert
                    numMaxBytes -= bytesUsed;       // adjust the max bytes estimate
                    numAvailBytes -= bytesUsed;
                    numCharsDecoded += charsConverted;

                    // Even though it seemed like the remaining # of bytes wouldn't be enough,
                    // there's still a chance we decoded all the chars we needed
                    // (this can happen if the decoding used less bytes / char than the max estimate)
                    // So we need to check for this and return if it's indeed the case
                    if (numChars == 0)
                    {
                        this.m_curBlockPos += bytesUsed;
                        break;
                    }
                }

                // if we've reached this line there must be 0 bytes remaining, if not there's a mismatch in the math above
                Debug.Assert(numAvailBytes == 0);
                
                // if we are here it means we've depleted all the bytes
                this.GetNextDataBlock();
                if (this.m_curBlockSize <= 0)
                {
                    // this means we're at the end of the file. simply break so that we
                    // return the number of chars read so far
                    break;
                }
            }

            return numCharsDecoded;
        }

        public string ReadString()
        {
            // First read the length of the string and the number of bytes needed
            Int32 numChars = this.ReadCompactInt32();
            Int32 numBytes = this.ReadCompactInt32();
            
            // allocate the string
            string str = new String('a', numChars);
            int numCharsDecoded = 0;

            while (numChars > 0)
            {
                int numAvailBytes = this.m_curBlockSize - this.m_curBlockPos;

                // Check whether current read buffer has enough data to fill the string
                // buffer to the end. If so, invoke decoder to copy from bytes to the
                // destination chars, update m_curBlockPos and return
                if (numAvailBytes >= numBytes)
                {
                    fixed (char *pChars = str)
                    {
                        this.m_decoder.GetChars(this.m_curDataBlock + this.m_curBlockPos,
                                                numBytes,
                                                pChars + numCharsDecoded,
                                                numChars,
                                                false);
                    }
                    this.m_curBlockPos += numBytes;
                    break;
                }


                // If there wasn't enough data in the read buffer convert the remaining bytes to chars
                // and request a new data block from the stream.
                if (numAvailBytes != 0)
                {
                    Int32 num = 0;
                    fixed (char *pChars = str)
                    {
                        num = this.m_decoder.GetChars(this.m_curDataBlock + this.m_curBlockPos,
                                                      numAvailBytes,
                                                      pChars + numCharsDecoded,
                                                      numChars,
                                                      false);
                    }
                    numChars -= num;
                    numBytes -= numAvailBytes;
                    numCharsDecoded += num;
                }

                this.GetNextDataBlock();
                if (this.m_curBlockSize <= 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                 String.Format(SR.EndOfStreamEncountered,
                                                               GetChannelURI()));
                }
            }
            return str;
        }

        /// <summary>
        /// Reads <paramref name="byteCount"/> bytes into <paramref name="destBuffer"/> starting at <paramref name="offset"/>.        
        /// </summary>
        /// <param name="destBuffer">The pre-allocated byte array to read data into.</param>
        /// <param name="offset">The starting offset at which to begin reading bytes into <paramref name="destBuffer"/>.</param>
        /// <param name="byteCount">The maximum number of bytes to read. Must be smaller than or equal to (<paramref name="destBuffer.Length"/> - <paramref name="offset"/>). </param>
        /// <returns>The number of bytes that was actually read.</returns>
        public int ReadBytes(byte[] destBuffer, int offset, int byteCount)
        {
            if (destBuffer == null)
            {
                throw new ArgumentNullException("destBuffer");
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (byteCount < 0)
            {
                throw new ArgumentOutOfRangeException("byteCount");
            }
            if (destBuffer.Length < (offset + byteCount))
            {
                throw new ArgumentOutOfRangeException("destBuffer",
                                                      String.Format(SR.ArrayLengthVsCountAndOffset,
                                                                    "destBuffer", offset + byteCount,
                                                                    "offset", "byteCount"));
            }
            
            int numBytes = byteCount;
            int numBytesRead = 0;
            fixed (byte *pBytes = &destBuffer[offset])
            {                
                while (numBytes > 0)
                {                    
                    int numAvailBytes = this.m_curBlockSize - this.m_curBlockPos;

                    // Check if there are enough bytes in the read buffer to satisfy the
                    // caller's request. If so, do the copy, update m_curBlockPos and return
                    if (numAvailBytes >= numBytes)
                    {
                        DryadLinqUtil.memcpy(this.m_curDataBlock + this.m_curBlockPos,
                                           pBytes + numBytesRead,
                                           numBytes);
                        this.m_curBlockPos += numBytes;
                        numBytesRead = byteCount;
                        break;
                    }

                    // The remaining data in the read buffer isn't enough to fill the user's request...
                    // Copy the all the remaining bytes to the destination buffer, and request a
                    // new read buffer from the stream.
                    // Note that we don't need to update m_curBlockPos here because the
                    // GetNextDataBlock call will reset it.
                    DryadLinqUtil.memcpy(this.m_curDataBlock + this.m_curBlockPos,
                                       pBytes + numBytesRead,
                                       numAvailBytes);
                    
                    // update numBytes/numBytesRead
                    numBytes -= numAvailBytes;
                    numBytesRead += numAvailBytes;

                    this.GetNextDataBlock();        
                    if (this.m_curBlockSize <= 0)
                    {
                        // if the file stream returned an empty buffer it means we are at the
                        // end of the file. Just return the total number of bytes read, and the
                        // caller will decide how to handle it.
                        break;
                    }

                    // continue with the loop to keep filling the remaining parts of the
                    // destination buffer
                }
            }
            return numBytesRead;
        }

        /// <summary>
        /// public helper to read into a byte*, mainly used to read preallocated fixed size,
        /// non-integer types (Array, Guid, decimal etc)
        /// </summary>
        public void ReadRawBytes(byte* pBytes, int numBytes)
        {
            int numBytesRead = 0;
            while (numBytes > 0)
            {
                int numAvailBytes = this.m_curBlockSize - this.m_curBlockPos;

                // if m_curDataBlock has enough bytes to fill the remainder of the user's request,
                // simply copy and exit.
                if (numAvailBytes >= numBytes)
                {
                    DryadLinqUtil.memcpy(this.m_curDataBlock + this.m_curBlockPos,
                                       pBytes + numBytesRead,
                                       numBytes);
                    this.m_curBlockPos += numBytes;
                    break;
                }

                // if m_curDataBlock has less data than required, copy all the remaining bytes
                // to user's buffer, update BytesRead counter, and request a new data block from
                // the native stream now that m_curDataBlock is depleted
                // Note that we don't need to update m_curBlockPos after memcpy() becase the
                // subsequent GetNextDataBlock() call will reset it
                DryadLinqUtil.memcpy(this.m_curDataBlock + this.m_curBlockPos,
                                     pBytes + numBytesRead,
                                     numAvailBytes);
                this.GetNextDataBlock();    
                if (this.m_curBlockSize <= 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.EndOfStreamEncountered,
                                                 String.Format(SR.EndOfStreamEncountered,
                                                               GetChannelURI()));
                }
                numBytes -= numAvailBytes;
                numBytesRead += numAvailBytes;

                // here we go on to do another loop, as we can only reach when the user's request
                // isn't fulfilled.
            }
        }
    }
}
