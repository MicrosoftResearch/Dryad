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
using System.IO;

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// A class to compute 64 bit Rabin fingerprints.
    /// </summary>        
    internal class Hash64
    {
        private const int LOGZEROBLOCK = 8;
        private const int ZEROBLOCK = 1 << LOGZEROBLOCK;

        internal const UInt64 Empty = 0x911498ae0e66bad6UL;
        internal static readonly Hash64 Hasher = new Hash64(Empty, 8);
        
        // poly[0] = 0; poly[1] = polynomial
        private UInt64[] poly = new UInt64[2];        

        // bybyte[b,i] is i*X^(64+8*b) mod poly[1]
        private UInt64[,] bybyte = new UInt64[8,256]; 

        // extend[i] is X^(8*2^i) mod poly[1]
        private UInt64[] powers = new UInt64[64];    

        private byte[] zeroes = new byte[ZEROBLOCK];

        // bybyte[b,i] is i*X^(64+8*(b+span)) mod poly[1]
        private UInt64[,] bybyte_out = new UInt64[8,256];     
        
        private int span;

        /// <summary>
        /// Computes the tables needed for fingerprint manipulations.
        /// Requires that "poly" be the binary representation
        /// of an irreducible polynomial in GF(2) of degree 64.   The X^64 term
        /// is not represented.  The X^0 term is the high order bit, and the
        /// X^63 term is the low-order bit.
        /// span is used in later calls to SlideWord(). If SlideWord() 
        /// is not to be called, span should be set to zero.
        /// </summary>        
        internal Hash64(UInt64 poly, int span)
        {
            this.poly[0] = 0;
            this.poly[1] = poly;        // This must be initialized early on
            this.span = span;
            // bybyte[][] must be initialized before powers[]
            this.InitByByte(this.bybyte, poly);
            // zeroes must be initialized before powers[]
            for (int i = 0; i < this.zeroes.Length; i++) this.zeroes[i] = 0;
            // The initialization of powers[] must happen after bybyte[][] 
            // and zeroes are initialized because concat uses all of 
            // bybyte[][], zeroes and the prefix of powers[] internally.
            this.powers[0] = 1ul << 55;
            uint l = 1;
            for (int i = 1; i < this.powers.Length; i++, l <<= 1)
            {
                this.powers[i] = this.Concat(this.powers[i-1] ^ poly, 0, l);
            }
            if (span != 0)
            {
                this.InitByByte(this.bybyte_out, this.Concat(0, 0, (uint)(span-1) * 8));
            }
        }

        private void InitByByte(UInt64[,] bybyte, UInt64 f)
        {
            for (int b = 0; b != 8; b++)
            {
                bybyte[b,0] = 0;
                for (int i = 0x80; i != 0; i >>= 1)
                {
                    bybyte[b,i] = f;
                    f = this.poly[f & 1] ^ (f >> 1);
                }
                for (int i = 1; i != 256; i <<= 1)
                {
                    UInt64 xf = bybyte[b,i];
                    for (int k = 1; k != i; k++)
                    {
                        bybyte[b,i+k] = xf ^ bybyte[b,k];
                    }
                }
            }
        }

        /// <summary>
        /// If fp was generated with polynomial P, "a" is the fingerprint under 
        /// P of string A, and 64-bit words "data[0, ..., len-1]" contain string
        /// B, return the fingerprint under P of the concatenation of A and B.
        /// Arrays of words are treated as polynomials.  The low-order bit in 
        /// the first word is the highest degree coefficient in the polynomial. 
        /// This routine differs from Extend() on bigendian machines, where the 
        /// byte order within each word is backwards.
        /// </summary>        
        internal UInt64 ExtendWord(UInt64 fpa, UInt64[] data, int start, int len) 
        {
            for (int i = start; i != start+len; i++)
            {
                fpa ^= data[i];
                fpa = this.bybyte[7, fpa & 0xff] ^
                      this.bybyte[6, (fpa >> 8) & 0xff] ^
                      this.bybyte[5, (fpa >> 16) & 0xff] ^
                      this.bybyte[4, (fpa >> 24) & 0xff] ^
                      this.bybyte[3, (fpa >> 32) & 0xff] ^
                      this.bybyte[2, (fpa >> 40) & 0xff] ^
                      this.bybyte[1, (fpa >> 48) & 0xff] ^
                      this.bybyte[0, fpa >> 56];
            }
            return fpa;
        }

        /// <summary>        
        /// If fp was generated with polynomial P, "a" is the fingerprint under 
        /// P of string A, and "b" is the fingerprint under P of string B, which
        /// has length "blen" bytes, return the fingerprint under P of the 
        /// concatenation of A and B.
        /// </summary>
        internal UInt64 Concat(UInt64 a, UInt64 b, UInt64 blen)
        {
            UInt64 x = blen;
            int low = (int)(x & ((1 << LOGZEROBLOCK)-1));
            a ^= this.poly[1];
            if (low != 0)
            {
                a = this.Extend(a, this.zeroes, 0, low);
            }
            x >>= LOGZEROBLOCK;
            for (int i = LOGZEROBLOCK; x != 0; i++)
            {
                if ((x & 1) != 0)
                {
                    UInt64 m = 0;
                    UInt64 e = this.powers[i];
                    for (UInt64 bit = 1ul << 63; bit != 0; bit >>= 1)
                    {
                        if ((e & bit) != 0)
                        {
                            m ^= a;
                        }
                        a = (a >> 1) ^ this.poly[a & 1];
                    }
                    a = m;
                }
                x >>= 1;
            }
            return a ^ b;
        }

        /// <summary>
        /// if "fp" was generated with polynomial P, X is some string of length 
        /// "(span-1)*8" bytes (see the FingerPrint constructor), "fpa" is the 
        /// fingerprint under P of word "a" concatenated with X,  return the
        /// fingerprint under P of X concatenated with word "b". The words "a"
        /// and "b" represent polynomials whose X^0 term is in the high-order bit,
        /// and whose X^63 term is in the low order bit.
        /// </summary>        
        internal UInt64 SlideWord(UInt64 fp, UInt64 a, UInt64 b)
        {
            a ^= this.poly[1] ^ (1ul << 63);
            fp ^= this.bybyte_out[7,a & 0xff] ^
                  this.bybyte_out[6,(a >> 8) & 0xff] ^
                  this.bybyte_out[5,(a >> 16) & 0xff] ^
                  this.bybyte_out[4,(a >> 24) & 0xff] ^
                  this.bybyte_out[3,(a >> 32) & 0xff] ^
                  this.bybyte_out[2,(a >> 40) & 0xff] ^
                  this.bybyte_out[1,(a >> 48) & 0xff] ^
                  this.bybyte_out[0,a >> 56];
            fp ^= b;
            fp = this.bybyte[7,fp & 0xff] ^
                 this.bybyte[6,(fp >> 8) & 0xff] ^
                 this.bybyte[5,(fp >> 16) & 0xff] ^
                 this.bybyte[4,(fp >> 24) & 0xff] ^
                 this.bybyte[3,(fp >> 32) & 0xff] ^
                 this.bybyte[2,(fp >> 40) & 0xff] ^
                 this.bybyte[1,(fp >> 48) & 0xff] ^
                 this.bybyte[0,fp >> 56];
            return fp;
        }

        /// <summary>
        /// if fp was generated with polynomial P, "fpa" is the fingerprint under 
        /// P of string A, and bytes "data[start, ..., start+len-1]" contain 
        /// string B, return the fingerprint under P of the concatenation of A 
        /// and B.  Strings are treated as polynomials.  The low-order bit in 
        /// the first byte is the highest degree coefficient in the polynomial.
        /// This routine differs from ExtendWord() in that it will read bytes 
        /// in increasing address order, regardless of the endianness of the 
        /// machine.
        /// </summary>
        internal UInt64 Extend(UInt64 fpa, byte[] data, int start, int len) 
        {
            for (int i = 0; i < len; i++)
            {
                fpa = (fpa >> 8) ^ this.bybyte[0,(fpa & 0xff) ^ data[start++]];
            }
            return fpa;
        }

        internal unsafe UInt64 Extend(UInt64 fpa, byte* data, int start, int len) 
        {
            for (int i = 0; i < len; i++)
            {
                fpa = (fpa >> 8) ^ this.bybyte[0,(fpa & 0xff) ^ data[start++]];
            }
            return fpa;
        }

        internal UInt64 Extend(UInt64 fp, byte b)
        {
            return (fp >> 8) ^ this.bybyte[0,(fp & 0xff) ^ b];
        }

        internal UInt64 Extend(UInt64 fp, sbyte b)
        {
            return this.Extend(fp, (byte)b);
        }

        internal UInt64 Extend(UInt64 fp, bool b)
        {
            byte b1 = (byte)((b) ? 1 : 0);
            return (fp >> 8) ^ this.bybyte[0,(fp & 0xff) ^ b1];
        }

        internal UInt64 Extend(UInt64 fp, char c)
        {
            return this.Extend(fp, (ushort)c);
        }

        internal UInt64 Extend(UInt64 fp, short v)
        {
            return this.Extend(fp, (ushort)v);
        }

        internal UInt64 Extend(UInt64 fp, ushort v)
        {
            fp ^= v;
            return ((fp >> 16) ^
                    this.bybyte[1, fp & 0xff] ^
                    this.bybyte[0, (fp >> 8) & 0xff]);
            
        }

        internal UInt64 Extend(UInt64 fp, int v)
        {
            return this.Extend(fp, (uint)v);
        }

        internal UInt64 Extend(UInt64 fp, uint v)
        {
            fp ^= v;
            return ((fp >> 32) ^
                    (this.bybyte[3, fp & 0xff] ^
                     this.bybyte[2, (fp >> 8) & 0xff] ^
                     this.bybyte[1, (fp >> 16) & 0xff] ^                
                     this.bybyte[0, (fp >> 24) & 0xff]));
        }

        internal UInt64 Extend(UInt64 fp, long v)
        {
            return this.Extend(fp, (UInt64)v);            
        }

        internal UInt64 Extend(UInt64 fp, UInt64 v)
        {
            fp ^= v;
            return (this.bybyte[7, fp & 0xff] ^
                    this.bybyte[6, (fp >> 8) & 0xff] ^
                    this.bybyte[5, (fp >> 16) & 0xff] ^
                    this.bybyte[4, (fp >> 24) & 0xff] ^
                    this.bybyte[3, (fp >> 32) & 0xff] ^
                    this.bybyte[2, (fp >> 40) & 0xff] ^
                    this.bybyte[1, (fp >> 48) & 0xff] ^
                    this.bybyte[0, (fp >> 56) & 0xff]);

        }

        internal unsafe UInt64 Extend(UInt64 fp, float v)
        {
            uint v1 = *(uint*)&v;
            return this.Extend(fp, v1);
        }

        internal unsafe UInt64 Extend(UInt64 fp, decimal v)
        {
            UInt64* vals = (UInt64*)&v;
            fp = this.Extend(fp, *vals);
            return this.Extend(fp, *(vals + 1));
        }

        internal unsafe UInt64 Extend(UInt64 fp, double v)
        {
            UInt64 v1 = *(UInt64*)&v;
            return this.Extend(fp, v1);
        }

        internal UInt64 Extend(UInt64 fp, string s)
        {
            byte[] bytes = new byte[s.Length];
            for (int i = 0; i < s.Length; i++)
            {
                bytes[i] = (byte)(s[i] & 0xff);
            }
            return this.Extend(fp, bytes, 0, bytes.Length);
        }

        internal UInt64 ExtendFile(UInt64 fp, string filename)
        {
            int size = 65536 * 4;
            byte[] readBuf = new byte[size];
            byte[] fpBuf = new byte[size];

            ulong fileFP = fp;
            using (Stream ifs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, size, FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                IAsyncResult readResult = ifs.BeginRead(readBuf, 0, readBuf.Length, null, null);
                while (true)
                {
                    int bytesRead = ifs.EndRead(readResult);
                    if (bytesRead == 0) break;

                    byte[] tmpBuf = fpBuf;
                    fpBuf = readBuf;
                    readBuf = tmpBuf;
                    readResult = ifs.BeginRead(readBuf, 0, readBuf.Length, null, null);
                    fileFP = this.Extend(fileFP, fpBuf, 0, bytesRead);
                }
            }
            return fileFP;
        }
    }
}
