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
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    internal class DryadLinqMultiReaderStream : Stream
    {
        private DryadLinqBinaryReader[] m_inputStreamArray;
        private DryadLinqBinaryReader m_curStream;
        private Int32 m_nextStreamIdx;

        public DryadLinqMultiReaderStream(DryadLinqBinaryReader[] streamArray)
        {
            this.m_inputStreamArray = streamArray;
            this.m_curStream = streamArray[0];
            this.m_nextStreamIdx = 1;
        }

        ~DryadLinqMultiReaderStream()
        {
            this.Close();
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }
        
        public override bool CanSeek
        {
            get { return false; }
        }

        public override long Length
        {
            get {
                long len = 0;
                for (int i = 0; i < this.m_inputStreamArray.Length; i++)
                {
                    len += this.m_inputStreamArray[i].Length;
                }
                return len;
            }
        }

        public override long Position
        {
            get { throw new DryadLinqException(DryadLinqErrorCode.PositionNotSupported,
                                               SR.PositionNotSupported); }
            set { throw new DryadLinqException(DryadLinqErrorCode.PositionNotSupported,
                                               SR.PositionNotSupported); }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                foreach (DryadLinqBinaryReader s in this.m_inputStreamArray)
                {
                    s.Close();
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            while (true)
            {
                int n = this.m_curStream.ReadBytes(buffer, offset, count);
                if (n != 0) return n;
                if (this.m_nextStreamIdx == this.m_inputStreamArray.Length) return 0;
                this.m_curStream = this.m_inputStreamArray[this.m_nextStreamIdx++];
            }
        }

        public override int ReadByte()
        {
            while (true)
            {
                int b = this.m_curStream.ReadUByte();
                if (b != -1) return b;
                if (this.m_nextStreamIdx == this.m_inputStreamArray.Length) return -1;
                this.m_curStream = this.m_inputStreamArray[this.m_nextStreamIdx++];
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SeekNotSupported,
                                         SR.SeekNotSupported);
        }

        public override void SetLength(long value)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SetLengthNotSupported,
                                         SR.SetLengthNotSupported);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new DryadLinqException(DryadLinqErrorCode.WriteNotSupported,
                                         SR.WriteNotSupported);
        }

        public override void WriteByte(byte value)
        {
            throw new DryadLinqException(DryadLinqErrorCode.WriteByteNotSupported,
                                         SR.WriteByteNotSupported);
        }
    }

    internal class DryadLinqMultiFileStream : Stream
    {
        private const int DefaultBuffSize = 8192 * 128;

        private string[] m_filePathArray;
        private CompressionScheme m_compressionScheme;
        private Stream m_curStream;
        private int m_nextIndex;

        internal DryadLinqMultiFileStream(string[] filePathArray, CompressionScheme scheme)
        {
            this.m_filePathArray = filePathArray;
            this.m_compressionScheme = scheme;
            this.m_nextIndex = 0;
            this.InitNextStream();
        }

        private void InitNextStream()
        {
            this.m_curStream = null;
            if (this.m_nextIndex < this.m_filePathArray.Length)
            {
                FileOptions options = FileOptions.SequentialScan;
                this.m_curStream = new FileStream(this.m_filePathArray[this.m_nextIndex], 
                                                  FileMode.Open, FileAccess.Read, FileShare.Read, 
                                                  DefaultBuffSize, options);
                if (this.m_compressionScheme != CompressionScheme.None)
                {
                    if (this.m_compressionScheme == CompressionScheme.Gzip)
                    {
                        this.m_curStream = new GZipStream(this.m_curStream, CompressionMode.Decompress);
                    }
                    else
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.UnknownCompressionScheme,
                                                     SR.UnknownCompressionScheme);
                    }
                }
                this.m_nextIndex++;
            }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override long Length
        {
            get
            {
                long len = 0;
                for (int i = 0; i < this.m_filePathArray.Length; i++)
                {
                    len += new FileInfo(this.m_filePathArray[i]).Length;
                }
                return len;
            }
        }

        public override long Position
        {
            get {
                throw new DryadLinqException(DryadLinqErrorCode.PositionNotSupported,
                                             SR.PositionNotSupported);
            }
            set {
                throw new DryadLinqException(DryadLinqErrorCode.PositionNotSupported,
                                             SR.PositionNotSupported);
            }
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            while (true)
            {
                int n = this.m_curStream.Read(buffer, offset, count);
                if (n != 0) return n;
                this.InitNextStream();
                if (this.m_curStream == null) return 0;
            }
        }

        public override int ReadByte()
        {
            while (true)
            {
                int b = this.m_curStream.ReadByte();
                if (b != -1) return b;
                this.InitNextStream();
                if (this.m_curStream == null) return 0;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SeekNotSupported,
                                         SR.SeekNotSupported);
        }

        public override void SetLength(long value)
        {
            throw new DryadLinqException(DryadLinqErrorCode.SetLengthNotSupported,
                                         SR.SetLengthNotSupported);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new DryadLinqException(DryadLinqErrorCode.WriteNotSupported,
                                         SR.WriteNotSupported);
        }

        public override void WriteByte(byte value)
        {
            throw new DryadLinqException(DryadLinqErrorCode.WriteByteNotSupported,
                                         SR.WriteByteNotSupported);
        }
    }
}
