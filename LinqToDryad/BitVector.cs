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
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    public struct BitVector
    {
        private byte[] m_array;
        
        public BitVector(int length)
        {
            this.m_array = new byte[(length + 7) / 8];
        }
    
        public BitVector(bool[] values)
        {
            this.m_array = new byte[(values.Length + 7) / 8];
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i])
                {
                    m_array[i / 8] |= (byte)(1 << (i % 8));
                }
            }
        }
    
        private BitVector(byte[] values)
        {
            this.m_array = values;
        }
    
        public bool this[int index]
        {
            get { return this.Get(index); }
        }
    
        public bool Get(int index)
        {
            int idx = index / 8;
            return ((idx < this.m_array.Length) &&
                    (this.m_array[idx] & (1 << (index % 8))) != 0);
        }

        public void Set(int index)
        {
            m_array[index / 8] |= (byte)(1 << (index % 8));
        }
    
        public void SetAll(bool value)
        {
            byte fillValue = 0;
            if (value) fillValue = 0xff;
            for (int i = 0; i < this.m_array.Length; i++)
            {
                this.m_array[i] = fillValue;
            }
        }
        
        private void WriteInner(DryadLinqBinaryWriter writer)
        {
            int len;
            for (len = this.m_array.Length - 1; len >= 0; len--)
            {
                if (this.m_array[len] != 0) break;
            }
            len++;
            writer.WriteCompact(len);
            for (int i = 0; i < len; i++)
            {
                writer.Write(this.m_array[i]);
            }
        }
        
        public static BitVector Read(DryadLinqBinaryReader reader)
        {
            Int32 len = reader.ReadCompactInt32();
            byte[] values = new byte[len];
            for (int i = 0; i < len; i++)
            {
                values[i] = reader.ReadUByte();
            }
            return new BitVector(values);
        }

        public static void Write(DryadLinqBinaryWriter writer, BitVector bv)
        {
            bv.WriteInner(writer);
        }
    }

}
