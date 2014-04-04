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
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Reflection;
using System.Linq;
using System.Diagnostics;
using Microsoft.CSharp;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    // This class contains some useful utility functions.
    internal static class DryadLinqUtil
    {
        private static Regex s_CSharpIdRegex = new Regex(@"[^\p{Ll}\p{Lu}\p{Lt}\p{Lo}\p{Nd}\p{Nl}\p{Mn}\p{Mc}\p{Cf}\p{Pc}\p{Lm}]");
        private static CSharpCodeProvider s_CSharpCodeProvider = new CSharpCodeProvider();

        // Check if name is a valid identifier.
        internal static bool IsValidId(string name)
        {
            return s_CSharpCodeProvider.IsValidIdentifier(name);
        }
        
        // Make name to be a valid identifier.
        internal static string MakeValidId(string name)
        {
            return s_CSharpIdRegex.Replace(name, "_");
        }

        // Note: this is used in various places, including vertex-code for naming files.
        internal static string MakeUniqueName()
        {
            return System.Guid.NewGuid().ToString();
        }

        internal static int GetTaskIndex(int hcode, int count)
        {
            int x1 = hcode & 0xFF;
            int x2 = (hcode >> 8) & 0xFF;
            int x3 = (hcode >> 16) & 0xFFFF;
            return (x1 ^ x2 ^ x3) % count;
        }

        // Check if the array is ordered.
        internal static bool IsOrdered<T>(T[] array, IComparer<T> comparer, bool isDescending)
        {
            comparer = TypeSystem.GetComparer<T>(comparer);
            if (array.Length < 2) return true;
            T elem = array[0];
            for (int i = 1; i < array.Length; i++)
            {
                int cmp = comparer.Compare(elem, array[i]);
                int cmpRes = (isDescending) ? -cmp : cmp;
                if (cmpRes > 0) return false;
                elem = array[i];
            }
            return true;
        }

        /// <summary>
        /// Binary search. The array must be ordered. Assume that comparer != null
        /// The input is an array {a_i} of n items and a value to search for.
        /// The output is a value in the range [0..n].
        /// The output can be directly used as the port-number to emit the item to for range-distribution.
        /// </summary>
        /// <typeparam name="T">The type of the elements in the array.</typeparam>
        /// <param name="array">The elements.</param>
        /// <param name="value">The value to be found.</param>
        /// <param name="comparer">A comparer to compare the elements of type T.</param>
        /// <param name="isDescending">Whether the array is ordered descending (true) or ascending (false).</param>
        /// <returns>The left-most value that is greaterthan-or-equal to value  (lessthan-or-equal for descending).</returns>

        // retVal is the idx of the item which satisfies
        //
        // Ascending:   arr[idx-1] < x <= arr[idx]  arr[-1] = -inf  arr[n] = +inf
        // idx is the index of the left-most element which is greater-or-equal than x
        // The smallest retVal is 0
        // The largest retVal is n
        //
        // Descending:  arr[idx-1] > x >= arr[idx]  arr[-1] = +inf  arr[n] = -inf
        // idx is the index of the left-most element which is less-or-equal than x.
        // The smallest retVal is 0
        // The largest retVal is n
        //
        // Although this isn't a strictly 'symmetrical rule', the "always left-most" is simple and consistent.
        internal static int BinarySearch<T>(T[] array, T value, IComparer<T> comparer, bool isDescending)
        {
            int lo = 0;
            int hi = array.Length - 1;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);
                int cmp = comparer.Compare(array[i], value);
                
                if (cmp == 0){
                    // ensure we are on the left-most matching item.
                    // Note: linear-search isn't ideal if there are many equal values, but that should not be common.
                    while (i > 0 && comparer.Compare(array[i-1], value) == 0) 
                        i--;
                }
                cmp = (isDescending) ? -cmp : cmp;
                if (cmp < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }

            return lo;
        }

        /// <summary>
        /// Mergesort a list of sorted datasets.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the input datasets</typeparam>
        /// <typeparam name="TKey">The type of the keys to sort</typeparam>
        /// <param name="sources">An array of input datasets</param>
        /// <param name="keySelector">The key extraction function</param>
        /// <param name="comparer">A Comparer on TKey to compare keys</param>
        /// <param name="isDescending">True if the mergesort is descending</param>
        /// <returns>The mergesort result</returns>
        internal static IEnumerable<TSource>
            MergeSort<TSource, TKey>(IEnumerable<TSource>[] sources,
                                     Func<TSource, TKey> keySelector,
                                     IComparer<TKey> comparer,
                                     bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TKey>(comparer);
            IEnumerable<TSource>[] currentLayer = sources;
            int currentLayerCount = sources.Length;
            IEnumerable<TSource>[] nextLayer = new IEnumerable<TSource>[currentLayerCount / 2 + 1];
            while (currentLayerCount != 1)
            {
                int nextLayerCount = currentLayerCount / 2;
                int idx = 0;
                for (int i = 0; i < nextLayerCount; i++)
                {
                    nextLayer[i] = BinaryMergeSort<TSource, TKey>(currentLayer[idx],
                                                                  currentLayer[idx + 1],
                                                                  keySelector,
                                                                  comparer,
                                                                  isDescending);
                    idx += 2;
                }
                if (idx < currentLayerCount)
                {
                    nextLayer[nextLayerCount] = currentLayer[idx];
                    nextLayerCount++;
                }
                currentLayer = nextLayer;
                currentLayerCount = nextLayerCount;
            }
            return currentLayer[0];
        }

        /// <summary>
        /// Mergesort two input sorted datasets.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the datasets</typeparam>
        /// <typeparam name="TKey">The type of the keys to sort</typeparam>
        /// <param name="source1">The first input dataset</param>
        /// <param name="source2">The second input dataset</param>
        /// <param name="keySelector">The key extraction function</param>
        /// <param name="comparer">A Comparer on TKey to compare keys</param>
        /// <param name="isDescending">True if the sort is descending</param>
        /// <returns>The mergesort result</returns>
        internal static IEnumerable<TSource>
            BinaryMergeSort<TSource, TKey>(IEnumerable<TSource> source1,
                                           IEnumerable<TSource> source2,
                                           Func<TSource, TKey> keySelector,
                                           IComparer<TKey> comparer,
                                           bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TKey>(comparer);
            
            IEnumerator<TSource> leftElems = source1.GetEnumerator();
            IEnumerator<TSource> rightElems = source2.GetEnumerator();

            if (leftElems.MoveNext())
            {
                if (rightElems.MoveNext())
                {
                    TKey leftKey = keySelector(leftElems.Current);
                    TKey rightKey = keySelector(rightElems.Current);
                    while (true)
                    {
                        int cmp = comparer.Compare(leftKey, rightKey);
                        int cmpRes = (isDescending) ? -cmp : cmp;
                        if (cmpRes > 0)
                        {
                            yield return rightElems.Current;
                            if (!rightElems.MoveNext())
                            {
                                yield return leftElems.Current;
                                break;
                            }
                            rightKey = keySelector(rightElems.Current);
                        }
                        else
                        {
                            yield return leftElems.Current;
                            if (!leftElems.MoveNext())
                            {
                                yield return rightElems.Current;
                                leftElems = rightElems;
                                break;
                            }
                            leftKey = keySelector(leftElems.Current);
                        }
                    }
                }
            }
            else
            {
                leftElems = rightElems;
            }

            while (leftElems.MoveNext())
            {
                yield return leftElems.Current;
            }
        }

        // Swap the bytes
        internal static UInt64 ByteSwap(UInt64 x)
        {
            return (x << 56)  
                 | (x >> 56) 
                 | ((x & 0x0000ff00UL) << 40) 
                 | ((x >> 40) & 0x0000ff00UL) 
                 | ((x & 0x00ff0000UL) << 24) 
                 | ((x >> 24) & 0x00ff0000UL) 
                 | ((x & 0xff000000UL) << 8) 
                 | ((x >> 8) & 0xff000000UL);
        }

        private static byte[] ObjectToByteArray(Object objectToSerialize)
        {
            MemoryStream fs = new MemoryStream();
            BinaryFormatter formatter = new BinaryFormatter();
            try
            {
                formatter.Serialize(fs, objectToSerialize);
                return fs.ToArray();
            }
            catch (SerializationException se)
            {
                DryadLinqClientLog.Add("Error occured during serialization. Message: " + se.Message);
                throw;
            }
            finally
            {
                fs.Close();
            }
        }

        private static Object ByteArrayToObject(byte[] rep)
        {
            MemoryStream fs = new MemoryStream(rep);
            BinaryFormatter bfm = new BinaryFormatter();
            try
            {
                Object o = bfm.Deserialize(fs);
                return o;
            }
            catch (SerializationException e)
            {
                throw new DryadLinqException(DryadLinqErrorCode.FailedToDeserialize, SR.FailedToDeserialize, e);
            }
            finally
            {
                fs.Close();
            }
        }

        internal static string MD5(Object ob)
        {
            byte[] payload = ObjectToByteArray(ob);
            MD5 md5 = new MD5CryptoServiceProvider();
            byte[] result = md5.ComputeHash(payload);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < result.Length; i++)
            {
                sb.Append(result[i].ToString("X2"));
            }
            return sb.ToString();
        }

        internal static string ReplaceWithLast(Match m)
        {
            return m.Groups[1].Value;
        }

        // matches each simple type name in a nested template type.
        private static Regex dotted = new Regex(@"[^<>,]*\.([^\.<>,]*)", RegexOptions.Compiled);
        internal static string SimpleName(string name)
        {
            // the typename may contain nested templates; simplify every component to keep the text after the last dot
            string result = dotted.Replace(name, ReplaceWithLast);
            return result;
        }

        internal static string MapToString<T>(Dictionary<T, T[]> map)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('[');
            bool isFirst = true;
            foreach (KeyValuePair<T, T[]> kv in map)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    sb.AppendLine(",");
                    sb.Append(' ');
                }
                sb.Append(kv.Key);
                sb.Append(" -> <");
                bool isFirst1 = true;
                foreach (T v in kv.Value)
                {
                    if (isFirst1)
                    {
                        isFirst1 = false;
                    }
                    sb.Append(v);
                }
                sb.Append('>');
            }
            sb.Append(']');
            return sb.ToString();
        }

        // Unsafe memcpy from System.Buffer
        internal unsafe static void memcpy(byte* src, byte* dest, int len)
        {
            if (len < 0)
            {
                throw new ArgumentException("len < 0", "len");
            }

#if FEATURE_PAL
            // Portable naive implementation
            while (len-- > 0)
                *dest++ = *src++;
#else

#if IA64
            // IA64 implementation
            long dstAlign = 8 - (((long)dest) & 7); // number of bytes to copy before dest is 8-byte aligned
            
            while ((dstAlign > 0) && (len > 0))
            {
                *dest++ = *src++;
                
                len--;
                dstAlign--;
            }

            long srcAlign = 8 - (((long)src) & 7);

            if (len > 0)
            {
                if (srcAlign != 8)
                {
                    if (4 == srcAlign)
                    {
                        while (len >= 4)
                        {
                            ((int*)dest)[0] = ((int*)src)[0];
                            dest += 4;
                            src  += 4;
                            len  -= 4;
                        }

                        srcAlign = 2;   // fall through to 2-byte copies
                    }

                    if ((2 == srcAlign) || (6 == srcAlign))
                    {
                        while (len >= 2)
                        {
                            ((short*)dest)[0] = ((short*)src)[0];
                            dest += 2;
                            src  += 2;
                            len  -= 2;
                        }
                    }

                    while (len-- > 0)
                    {
                        *dest++ = *src++;
                    }
                }
                else
                {
                    if (len >= 16) 
                    {
                        do 
                        {
                            ((long*)dest)[0] = ((long*)src)[0];
                            ((long*)dest)[1] = ((long*)src)[1];
                            dest += 16;
                            src += 16;
                        } while ((len -= 16) >= 16);
                    }
                    if (len > 0)  // protection against negative len and optimization for len==16*N
                    {
                       if ((len & 8) != 0) 
                       {
                           ((long*)dest)[0] = ((long*)src)[0];
                           dest += 8;
                           src += 8;
                       }
                       if ((len & 4) != 0) 
                       {
                           ((int*)dest)[0] = ((int*)src)[0];
                           dest += 4;
                           src += 4;
                       }
                       if ((len & 2) != 0) 
                       {
                           ((short*)dest)[0] = ((short*)src)[0];
                           dest += 2;
                           src += 2;
                       }
                       if ((len & 1) != 0)
                       {
                           *dest++ = *src++;
                       }
                    }
                }
            }

#else
            // AMD64 implementation uses longs instead of ints where possible
            if (len >= 16) 
            {
                do 
                {
#if AMD64
                    ((long*)dest)[0] = ((long*)src)[0];
                    ((long*)dest)[1] = ((long*)src)[1];
#else                    
                    ((int*)dest)[0] = ((int*)src)[0];
                    ((int*)dest)[1] = ((int*)src)[1];
                    ((int*)dest)[2] = ((int*)src)[2];
                    ((int*)dest)[3] = ((int*)src)[3];
#endif
                    dest += 16;
                    src += 16;
                } while ((len -= 16) >= 16);
            }
            if (len > 0)  // protection against negative len and optimization for len==16*N
            {
                if ((len & 8) != 0) 
                {
#if AMD64
                    ((long*)dest)[0] = ((long*)src)[0];
#else
                    ((int*)dest)[0] = ((int*)src)[0];
                    ((int*)dest)[1] = ((int*)src)[1];
#endif
                    dest += 8;
                    src += 8;
               }
               if ((len & 4) != 0) 
               {
                    ((int*)dest)[0] = ((int*)src)[0];
                    dest += 4;
                    src += 4;
               }
               if ((len & 2) != 0) 
               {
                    ((short*)dest)[0] = ((short*)src)[0];
                    dest += 2;
                    src += 2;
               }
               if ((len & 1) != 0)
                    *dest++ = *src++;
            }

#endif // IA64
#endif // FEATURE_PAL
        }


        //Utility function to determine if a sequence of partition-keys is ascending or descending consistently
        // if the sequence is neither ascending or descending, an exception is thrown.
        // if the sequence is single element or a series of equal values, the return is null == inconclusive/both.
        // if the return value == false, the value of isDescending is meaningless.
        internal static bool ComputeIsDescending<TKey>(TKey[] partitionKeys, IComparer<TKey> comparer, out bool? isDescending)
        {
            if (partitionKeys.Length == 0 || partitionKeys.Length == 1)
            {
                isDescending = null; // neither specifically ascending nor descending.
                return true; // everything is OK.
            }

            // Determine if the keys are ascending/descending and whether they are consistent
            isDescending = null; // initially we don't know (and equal keys may delay identification)

            TKey curr = partitionKeys[0];
            for (int i = 1; i < partitionKeys.Length; i++)
            {
                int cmp = comparer.Compare(curr, partitionKeys[i]);

                if (cmp == 0)
                {
                    // do nothing 
                }
                if (cmp < 0)
                {
                    if (isDescending == null)
                    {
                        isDescending = false; // the sequence appears to be ascending
                    }

                    if (isDescending == true)
                    {
                        return false; // there was an inconsistency.
                    }
                }
                if (cmp > 0)
                {
                    if (isDescending == null)
                    {
                        isDescending = true; // the sequence appears to be descending
                    }

                    if (isDescending == false)
                    {
                        return false; // there was an inconsistency.
                    }
                        
                }
                curr = partitionKeys[i];
            }

            return true;
        }
    }

    internal class FList<T>
    {
        public T elem;
        public FList<T> next;

        internal static FList<T> Empty = new FList<T>(default(T), null);

        internal FList(T elem, FList<T> next)
        {
            this.elem = elem;
            this.next = next;
        }

        internal FList<T> Cons(T elem, FList<T> next)
        {
            return new FList<T>(elem, next);
        }

        internal bool Find(T x)
        {
            FList<T> curr = this;
            while (curr != Empty)
            {
                if (curr.elem.Equals(x)) return true;
                curr = curr.next;
            }
            return false;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("< ");
            if (this != Empty)
            {
                sb.Append(this.elem);
                FList<T> curr = this.next;
                while (curr != Empty)
                {
                    sb.Append(", " + curr.elem.ToString());
                    curr = curr.next;
                }
                sb.Append(" >");
            }
            return sb.ToString();
        }
    }

    internal class Wrapper<T>
    {
        internal T item;

        internal Wrapper(T item)
        {
            this.item = item;
        }
    }
}
