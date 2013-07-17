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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq.Expressions;
using System.Linq;

namespace Microsoft.Research.DryadLinq.Internal
{
    public static class HpcLinqHelper
    {
        [Resource(IsStateful = false)]
        public static IEnumerable<TSource>
            CheckSort<TSource, TKey>(IEnumerable<TSource> source,
                                     Expression<Func<TSource, TKey>> keySelector,
                                     IComparer<TKey> comparer,
                                     bool isDescending)
        {
            Func<TSource, TKey> keySel = keySelector.Compile();
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            IEnumerator<TSource> elems = source.GetEnumerator();
            if (elems.MoveNext())
            {
                TSource curElem = elems.Current;
                yield return curElem;

                TKey curKey = keySel(curElem);
                while (elems.MoveNext())
                {
                    TSource nextElem = elems.Current;
                    yield return nextElem;

                    TKey nextKey = keySel(nextElem);
                    int cmp = comparer.Compare(curKey, nextKey);
                    int cmpRes = (isDescending) ? -cmp : cmp;
                    if (cmpRes > 0)
                    {
                        throw new DryadLinqException(SR.SourceNotOrdered);
                    }
                    curKey = nextKey;
                }
            }
        }
        
        public static IEnumerable<T3> Cross<T1, T2, T3>(IEnumerable<T1> s1,
                                                        IEnumerable<T2> s2,
                                                        Expression<Func<T1, T2, T3>> procFunc)
        {
            Func<T1, T2, T3> proc = procFunc.Compile();
            bool useRight = true;
            if ((s1 is HpcVertexReader<T1>) && (s2 is HpcVertexReader<T2>))
            {
                Int64 leftLen = ((HpcVertexReader<T1>)s1).GetTotalLength();
                Int64 rightLen = ((HpcVertexReader<T2>)s2).GetTotalLength();
                if (leftLen >= 0 && rightLen >= 0)
                {
                    useRight = rightLen <= leftLen;
                }
            }
            if (useRight)
            {
                List<T2> elems2 = s2.ToList();
                foreach (var elem1 in s1)
                {
                    foreach (var elem2 in elems2)
                    {
                        yield return proc(elem1, elem2);
                    }
                }
            }
            else
            {
                List<T1> elems1 = s1.ToList();
                foreach (var elem2 in s2)
                {
                    foreach (var elem1 in elems1)
                    {
                        yield return proc(elem1, elem2);
                    }
                }
            }
        }

        public static IEnumerable<T2> SelectSecond<T1, T2>(IEnumerable<T1> s1, IEnumerable<T2> s2)
        {
            return s2;
        }

        // Used in SequenceEqual()
        public static IEnumerable<bool> SequenceEqual<T>(IEnumerable<T> s1,
                                                         IEnumerable<T> s2,
                                                         IEqualityComparer<T> comparer)
        {
            return HpcLinqVertex.AsEnumerable(System.Linq.Enumerable.SequenceEqual(s1, s2, comparer));
            
        }

        // Used in SlidingWindow()
        [Resource(IsStateful = false)]
        public static IEnumerable<T[]> Last<T>(IEnumerable<T> source,
                                               int windowSize)
        {
            int count = windowSize - 1;
            T[] buffer = new T[count];
            long total = 0;
            foreach (var x in source)
            {
                buffer[total % count] = x;
                total++;
            }

            if (total < count)
            {
                throw new DryadLinqException(String.Format(SR.PartitionTooSmallForSlidingWindow, count));
            }
            
            T[] last = new T[count];
            int startIdx = (int)total % count;
            Array.Copy(buffer, startIdx, last, 0, count - startIdx);
            Array.Copy(buffer, 0, last, count - startIdx, startIdx);
            yield return last;
        }

        public static IEnumerable<IndexedValue<T[]>> Slide<T>(IEnumerable<T[]> source)
        {
            using (IEnumerator<T[]> sourceEnum = source.GetEnumerator())
            {
                if (sourceEnum.MoveNext())
                {
                    yield return new IndexedValue<T[]>(0, new T[0]);

                    int index = 1;
                    T[] lastVal = sourceEnum.Current;
                    while (sourceEnum.MoveNext())
                    {
                        yield return new IndexedValue<T[]>(index, lastVal);
                        index++;
                        lastVal = sourceEnum.Current;
                    }
                }
            }
        }

        [Resource(IsStateful = false)]
        public static IEnumerable<T2>
            ProcessWindows<T1, T2>(IEnumerable<IndexedValue<T1[]>> source1,
                                   IEnumerable<T1> source2,
                                   Func<IEnumerable<T1>, T2> procFunc,
                                   Int32 windowSize)
        {
            Window<T1> window = new Window<T1>(windowSize);
            T1[] slided = source1.Single().Value;
            for (int i = 0; i < slided.Length; i++)
            {
                window.Add(slided[i]);
            }

            using (IEnumerator<T1> sourceEnum = source2.GetEnumerator())
            {
                while (window.Count() < windowSize)
                {
                    if (!sourceEnum.MoveNext()) break;
                    window.Add(sourceEnum.Current);
                }
                if (window.Count() == windowSize)
                {
                    yield return procFunc(window);
                    while (sourceEnum.MoveNext())
                    {
                        window.Add(sourceEnum.Current);
                        yield return procFunc(window);
                    }
                }
            }
        }

        // Calculate the sizes of the partitions. Used for example to implement Concat.
        public static IEnumerable<IndexedValue<T[]>> IndexedCount<T>(IEnumerable<T> source)
        {
            T[] elems = source.ToArray();
            for (int i = 0; i < elems.Length; i++)
            {
                yield return new IndexedValue<T[]>(i, elems);
            }
        }

        [Resource(IsStateful = false)]
        public static IEnumerable<IndexedValue<T>>
            AddPartitionIndex<T>(IEnumerable<IndexedValue<long[]>> source1, IEnumerable<T> source2, Int32 pcount)
        {
            IndexedValue<long[]> s1 = source1.Single();
            long averageCount = s1.Value.Sum() / pcount;
            long partialCount = 0;
            for (int i = 0; i < s1.Index; i++)
            {
                partialCount += s1.Value[i];
            }
            int partIndex = (int)(partialCount / averageCount);
            long indexInPart = partialCount % averageCount;
            foreach (T elem in source2)
            {
                if (indexInPart >= averageCount && partIndex != pcount-1)
                {
                    partIndex++;
                    indexInPart = 0;
                }
                yield return new IndexedValue<T>(partIndex, elem);
                indexInPart++;
            }                    
        }

        // Produces one dummy item per partition. Used for example to implement Reverse().
        [Resource(IsStateful = false)]        
        public static IEnumerable<int> ValueZero<T>(IEnumerable<T> source)
        {
            yield return 0;
        }

        //Used for Reverse()
        //input: a sequence of n dummy items.  eg {0,0,0...  } x n
        //output: { {(0,n), (1,n), (2,n), .., (n-1, n)} }
        //        item.Index = index
        //        item.Value = nPartitions
        public static IEnumerable<IndexedValue<int>> MakeIndexCountPairs(IEnumerable<int> source)
        {
            int count = source.Count();
            for (int i = 0; i < count; i++)
            {
                yield return new IndexedValue<int>(i, count);
            }
        }
        
        // Used for Reverse()
        // receives a pair (myIndex, nPartitions) as source1, and a normal sequence as source2.
        // targetIdx = nPartition-myIndex-1
        // produces {(targetIdx, item), (targetIdx, item), ...}
        public static IEnumerable<IndexedValue<T>>
            AddIndexForReverse<T>(IEnumerable<IndexedValue<int>> source1, IEnumerable<T> source2)
        {
            IndexedValue<int> item = source1.Single();
            int myIndex = item.Index;
            int pcount = item.Value;
            int targetIndex = pcount - myIndex - 1;
            foreach (T elem in source2)
            {
                yield return new IndexedValue<T>(targetIndex, elem);
            }
        }

        // Used in Zip()
        public static IEnumerable<IndexedValue<Pair<long[], long[]>>>
            ZipCount(IEnumerable<long> source1, IEnumerable<long> source2)
        {
            long[] elems1 = source1.ToArray();
            long[] elems2 = source2.ToArray();
            Pair<long[], long[]> pair = new Pair<long[], long[]>(elems1, elems2);
            for (int i = 0; i < elems2.Length; i++)
            {
                yield return new IndexedValue<Pair<long[], long[]>>(i, pair);
            }
        }
        
        public static IEnumerable<IndexedValue<T>>
            AssignPartitionIndex<T>(IEnumerable<IndexedValue<Pair<long[], long[]>>> source1,
                                    IEnumerable<T> source2)
        {
            IndexedValue<Pair<long[], long[]>> s1 = source1.Single();
            long[] elems1 = s1.Value.Key;
            long[] elems2 = s1.Value.Value;

            long partialCount = 0;
            for (int i = 0; i < s1.Index; i++)
            {
                partialCount += elems2[i];
            }
            int partIndex = 0;
            for (partIndex = 0; partIndex < elems1.Length; partIndex++)
            {
                partialCount -= elems1[partIndex];
                if (partialCount < 0) break;
            }
            if (partialCount < 0)
            {
                foreach (T elem in source2)
                {
                    yield return new IndexedValue<T>(partIndex, elem);
                    partialCount++;
                    if (partialCount == 0)
                    {
                        for (partIndex = partIndex + 1; partIndex < elems1.Length; partIndex++)
                        {
                            partialCount = -elems1[partIndex];
                            if (partialCount < 0) break;
                        }
                        if (partialCount == 0) break;
                    }
                }
            }
        }

        // Used in SelectWithPartitionIndex()
        public static IEnumerable<int> AssignIndex(IEnumerable<int> source)
        {
            int index = 0;
            foreach (int elem in source)
            {
                yield return index;
                index++;
            }
        }
        
        public static IEnumerable<T2>
            ProcessWithIndex<T1, T2>(IEnumerable<T1> source1,
                                     IEnumerable<int> source2,
                                     Func<IEnumerable<T1>, int, IEnumerable<T2>> procFunc)
        {
            int index = source2.Single();
            return procFunc(source1, index);
        }

        public static IEnumerable<T2>
            ProcessWithIndex<T1, T2>(IEnumerable<T1> source1,
                                     IEnumerable<int> source2,
                                     Func<T1, int, T2> procFunc)
        {
            int index = source2.Single();
            return HpcLinqVertex.Select(source1, x => procFunc(x, index), true);
        }
    }

    internal class Window<T> : IEnumerable<T>
    {
        private T[] m_elems;
        private int m_startIdx;
        private int m_count;
        
        public Window(int len)
        {
            this.m_elems = new T[len];
            this.m_startIdx = 0;
            this.m_count = 0;
        }

        public void Add(T elem)
        {
            int nextIdx = this.m_startIdx + this.m_count;
            if (nextIdx >= this.m_elems.Length)
            {
                nextIdx -= this.m_elems.Length;
            }
            this.m_elems[nextIdx] = elem;
            if (this.m_count < this.m_elems.Length)
            {
                this.m_count++;
            }
            else
            {
                this.m_startIdx++;
                if (this.m_startIdx == this.m_elems.Length)
                {
                    this.m_startIdx = 0;
                }
            }
        }

        public int Count()
        {
            return this.m_count;
        }

        #region IEnumerable and IEnumerable<T> members
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            int idx = this.m_startIdx;
            for (int i = 0; i < this.m_count; i++)
            {
                yield return this.m_elems[idx];
                idx++;
                if (idx == this.m_elems.Length) idx = 0;
            }
        }
        #endregion
    }
}

