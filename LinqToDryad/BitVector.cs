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
    /// <summary>
    /// A simple BitVector implementation.
    /// </summary>
    public struct BitVector
    {
        private byte[] m_array;
        
        /// <summary>
        /// Initializes a new instance of the BitVector class.
        /// </summary>
        /// <param name="length">The number of bits for the bit vector.</param>
        public BitVector(int length)
        {
            this.m_array = new byte[(length + 7) / 8];
        }

        /// <summary>
        /// Initializes a new instance of the BitVector class from an array of boolean values.  
        /// </summary>
        /// <param name="values">An array of boolean values representing a bit vector.</param>
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
    
        /// <summary>
        /// Gets the bit at the specified index.
        /// </summary>
        /// <param name="index">An index into the bit vector.</param>
        /// <returns>The value of the bit as a boolean.</returns>
        public bool this[int index]
        {
            get { return this.Get(index); }
        }
    
        /// <summary>
        /// Gets the bit at the specified index.
        /// </summary>
        /// <param name="index">An index into the bit vector.</param>
        /// <returns>The value of the bit as a boolean.</returns>
        public bool Get(int index)
        {
            int idx = index / 8;
            return ((idx < this.m_array.Length) &&
                    (this.m_array[idx] & (1 << (index % 8))) != 0);
        }

        /// <summary>
        /// Sets the bit at the specified index.
        /// </summary>
        /// <param name="index">An index into the bit vector.</param>
        public void Set(int index)
        {
            this.m_array[index / 8] |= (byte)(1 << (index % 8));
        }

        /// <summary>
        /// Sets all the bits to the specified value.
        /// </summary>
        /// <param name="value">The value to be set for all bits.</param>
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
        
        /// <summary>
        /// Reads a BitVector from the specified DryadLinqBinaryReader. 
        /// </summary>
        /// <param name="reader">The DryadLinqBinaryReader to read from.</param>
        /// <returns>A BitVector</returns>
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

        /// <summary>
        /// Writes a BitVector to the specified DryadLinqBinaryWriter.
        /// </summary>
        /// <param name="writer">The DryadLinqBinaryWriter to write to.</param>
        /// <param name="bv">The BitVector to write</param>
        public static void Write(DryadLinqBinaryWriter writer, BitVector bv)
        {
            bv.WriteInner(writer);
        }
    }

}
