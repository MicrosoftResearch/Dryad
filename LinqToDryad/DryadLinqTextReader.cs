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
using System.Text;
using System.Reflection;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// The DryadLINQ class to read texts from a native stream.
    /// </summary>
    /// <remarks>A DryadLINQ user should not need to use this class directly.</remarks>
    public unsafe sealed class DryadLinqTextReader
    {
        // The number of bytes we attempt to decode each time
        private const int DecodeUnitByteSize = 8192 * 16;

        private NativeBlockStream m_nativeStream;  // source stream
        private Encoding m_encoding;               // character encoding
        private Decoder m_decoder;                 // class decoding bytes to chars
        private DataBlockInfo m_curDataBlockInfo;  // unsafe class describing a memory buffer read from stream
        private Int32 m_curBlockPos;               // pointer in input buffer holding first char to decode
        private Int32 m_decodeUnitCharSize;        // how many characters are decoded in one call
        private char[] m_charBuff;                 // temporary buffer holding decoded characters; grown dynamically
        private Int32 m_charBuffEnd;               // offset of last character in charBuff
        private Int32 m_curLineStart;              // offset of line start in charBuff
        private Int32 m_curLineEnd;                // offset of line end in charBuff
        private bool m_isClosed;

        /// <summary>
        /// Initializes an instance of DryadLinqTextReader with encoding UTF8.
        /// </summary>
        /// <param name="stream">A native stream to read from.</param>
        public DryadLinqTextReader(NativeBlockStream stream)
            : this(stream, Encoding.UTF8)
        {
        }

        /// <summary>
        /// Initializes an instance of DryadLinqTextReader.
        /// </summary>
        /// <param name="stream">A native stream to read from.</param>
        /// <param name="encoding">The text encoding.</param>
        public DryadLinqTextReader(NativeBlockStream stream, Encoding encoding)
        {
            this.m_nativeStream = stream;
            this.m_encoding = encoding;
            this.m_decoder = encoding.GetDecoder();
            this.m_curDataBlockInfo.DataBlock = null;
            this.m_curDataBlockInfo.BlockSize = -1;
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curBlockPos = 0;
            this.m_decodeUnitCharSize = this.m_encoding.GetMaxCharCount(DecodeUnitByteSize);
            this.m_charBuff = new char[this.m_decodeUnitCharSize + 2];  //allow 2 bytes for trailing newline
            this.m_charBuffEnd = 0;
            this.m_curLineStart = 0;
            this.m_curLineEnd = 0;
            this.m_isClosed = false;
        }

        /// <summary>
        /// Initializes an instance of DryadLiqnTextReader with encoding UTF8.
        /// </summary>
        /// <param name="vertexInfo">A native handle for Dryad vertex.</param>
        /// <param name="portNum">A port number that specifies a Dryad channel.</param>
        public DryadLinqTextReader(IntPtr vertexInfo, UInt32 portNum)
            : this(new DryadLinqChannel(vertexInfo, portNum, true), Encoding.UTF8)
        {
        }

        /// <summary>
        /// Initializes an instance of DryadLiqnTextReader.
        /// </summary>
        /// <param name="vertexInfo">A native handle for Dryad vertex.</param>
        /// <param name="portNum">A port number that specifies a Dryad channel.</param>
        /// <param name="encoding">The text encoding.</param>
        public DryadLinqTextReader(IntPtr vertexInfo, UInt32 portNum, Encoding encoding)
            : this(new DryadLinqChannel(vertexInfo, portNum, true), encoding)
        {
        }

        /// <summary>
        /// The finalizer that frees native resources.
        /// </summary>
        ~DryadLinqTextReader()
        {
            // Only release native resoure here
            if (this.m_curDataBlockInfo.ItemHandle != IntPtr.Zero)
            {
                this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            }
        }

        /// <summary>
        /// Gets the length in bytes of the native stream.
        /// </summary>
        /// <returns>A 64-bit integer.</returns>
        public Int64 GetTotalLength()
        {
            return this.m_nativeStream.GetTotalLength();
        }

        // Fill this.charBuff by decoding more from data block.
        private int FillCharBuffer()
        {
            if (this.m_curDataBlockInfo.BlockSize <= 0)
            {
                if (this.m_curDataBlockInfo.BlockSize == -1)
                {
                    this.GetNextDataBlock();
                }
                if (this.m_curDataBlockInfo.BlockSize == 0) return 0;
            }

            Int32 curLineLen = this.m_curLineEnd - this.m_curLineStart;
            if (curLineLen + this.m_decodeUnitCharSize + 2 > this.m_charBuff.Length) //allow 2 bytes for trailing newline
            {
                // The current charBuff is too small, augment
                char[] newCharBuff = new char[curLineLen + this.m_decodeUnitCharSize + 2];  //allow 2 bytes for trailing newline
                Array.Copy(this.m_charBuff, this.m_curLineStart, newCharBuff, 0, curLineLen);
                this.m_charBuff = newCharBuff;
            }
            else
            {
                // Shift the current line to the beginning
                Array.Copy(this.m_charBuff, this.m_curLineStart, this.m_charBuff, 0, curLineLen);
            }
            this.m_curLineStart = 0;
            this.m_curLineEnd = curLineLen;
            this.m_charBuffEnd = curLineLen;

            // Decode DecodeUnitByteSize bytes unless EOF
            Int32 numChars = 0;
            Int32 numBytesDesired = DecodeUnitByteSize - 2;
            while (numBytesDesired > 0)
            {
                Int32 numBytesRemainingInBlock = this.m_curDataBlockInfo.BlockSize - this.m_curBlockPos;
                if (numBytesRemainingInBlock > numBytesDesired)
                {
                    fixed (char* pChars = this.m_charBuff)
                    {
                        numChars += this.m_decoder.GetChars(this.m_curDataBlockInfo.DataBlock + this.m_curBlockPos,
                                                            numBytesDesired,
                                                            pChars + curLineLen + numChars,
                                                            this.m_decodeUnitCharSize - numChars,
                                                            false);
                    }
                    this.m_curBlockPos += numBytesDesired;
                    break;
                }
                else
                {
                    fixed (char* pChars = this.m_charBuff)
                    {
                        numChars += this.m_decoder.GetChars(this.m_curDataBlockInfo.DataBlock + this.m_curBlockPos,
                                                            numBytesRemainingInBlock,
                                                            pChars + curLineLen + numChars,
                                                            this.m_decodeUnitCharSize - numChars,
                                                            false);
                    }
                    numBytesDesired -= numBytesRemainingInBlock;

                    this.GetNextDataBlock();
                    if (this.m_curDataBlockInfo.BlockSize <= 0) break;
                }
            }

            this.m_charBuffEnd += numChars;
            return numChars;
        }

        // Get the next data block
        private unsafe void GetNextDataBlock()
        {
            this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
            this.m_curDataBlockInfo.ItemHandle = IntPtr.Zero;
            this.m_curDataBlockInfo = this.m_nativeStream.ReadDataBlock();
            this.m_curBlockPos = 0;
        }

        /// <summary>
        /// Moves to the next line.
        /// </summary>
        /// <returns>True if there is a next line.</returns>
        public bool MoveNext()
        {
            return (this.m_curLineEnd < this.m_charBuffEnd || this.FillCharBuffer() > 0);
        }
        
        /// <summary>
        /// Reads a line of text from the reader.
        /// </summary>
        /// <returns>A string. Returns null if EOF.</returns>
        public string ReadLine()
        {
            Debug.Assert(this.m_curLineStart == this.m_curLineEnd);
            while (this.m_curLineEnd < this.m_charBuffEnd || this.FillCharBuffer() > 0)
            {
                char ch = this.m_charBuff[m_curLineEnd];
                if (ch == '\r' || ch == '\n')
                {
                    Int32 lineLen = this.m_curLineEnd - this.m_curLineStart;
                    this.m_curLineEnd++;
                    if (ch == '\r' && (this.m_curLineEnd < this.m_charBuffEnd || this.FillCharBuffer() > 0))
                    {
                        if (this.m_charBuff[this.m_curLineEnd] == '\n')
                        {
                            this.m_curLineEnd++;
                        }
                    }
                    Int32 lineStart = this.m_curLineStart;
                    this.m_curLineStart = this.m_curLineEnd;
                    return new String(this.m_charBuff, lineStart, lineLen);
                }
                else
                {
                    this.m_curLineEnd++;
                }
            }

            // This is for the last line:
            Int32 lastLineLen = this.m_curLineEnd - this.m_curLineStart;
            if (lastLineLen == 0) return null;
            String lastLine = new String(this.m_charBuff, this.m_curLineStart, lastLineLen);
            this.m_curLineStart = this.m_curLineEnd;
            return lastLine;
        }

        /// <summary>
        /// Closes the native stream and frees native resources.
        /// </summary>
        public void Close()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.m_nativeStream.ReleaseDataBlock(this.m_curDataBlockInfo.ItemHandle);
                this.m_nativeStream.Close();
            }
            GC.SuppressFinalize(this);
        }

        internal string GetChannelURI()
        {
            return this.m_nativeStream.GetURI();
        }

        /// <summary>
        /// Returns a string that represents this DryadLinqTextReader object.
        /// </summary>
        /// <returns>The string representation.</returns>
        public override string ToString()
        {
            return this.m_nativeStream.ToString();
        }
    }
}
