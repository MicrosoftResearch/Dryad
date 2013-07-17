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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    internal class HpcLinqMultiInputStream : Stream
    {
        private HpcBinaryReader[] m_inputStreamArray;
        private HpcBinaryReader m_curStream;
        private Int32 m_nextStreamIdx;

        public HpcLinqMultiInputStream(HpcBinaryReader[] streamArray)
        {
            this.m_inputStreamArray = streamArray;
            this.m_curStream = streamArray[0];
            this.m_nextStreamIdx = 1;
        }

        ~HpcLinqMultiInputStream()
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
            get { throw new DryadLinqException(HpcLinqErrorCode.PositionNotSupported,
                                             SR.PositionNotSupported); }
            set { throw new DryadLinqException(HpcLinqErrorCode.PositionNotSupported,
                                             SR.PositionNotSupported); }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                foreach (HpcBinaryReader s in this.m_inputStreamArray)
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
            throw new DryadLinqException(HpcLinqErrorCode.SeekNotSupported,
                                       SR.SeekNotSupported);
        }

        public override void SetLength(long value)
        {
            throw new DryadLinqException(HpcLinqErrorCode.SetLengthNotSupported,
                                       SR.SetLengthNotSupported);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new DryadLinqException(HpcLinqErrorCode.WriteNotSupported,
                                       SR.WriteNotSupported);
        }

        public override void WriteByte(byte value)
        {
            throw new DryadLinqException(HpcLinqErrorCode.WriteByteNotSupported,
                                       SR.WriteByteNotSupported);
        }
    }
}
