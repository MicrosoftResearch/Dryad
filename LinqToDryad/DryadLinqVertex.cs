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
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Data.Linq;
using System.Xml;
using System.Data.Linq.Mapping;
using System.Diagnostics;
using System.Threading;
using System.Data;
using System.Data.SqlClient;
using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Research.DryadLinq;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// This class provides the generic vertex runtime for the query operators
    /// supported by DryadLINQ. The auto-generated vertex code uses the methods
    /// in this class extensively.
    /// </summary>
    /// <remarks>A DryadLINQ user should not need to use DryadLinqVertex directly.</remarks>
    public static class DryadLinqVertex
    {
        public static bool s_multiThreading = true; //vertex code will set this at runtime. 

        internal static IParallelPipeline<TResult>
            ExtendParallelPipeline<TSource, TResult>(this IEnumerable<TSource> source,
                                                     Func<IEnumerable<TSource>, IEnumerable<TResult>> func,
                                                     bool orderPreserving)
        {
            IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
            IParallelPipeline<TResult> result;
            if (pipe == null)
            {
                result = new ParallelApply<TSource, TResult>(source, func, orderPreserving);
            }
            else
            {
                result = pipe.Extend(func, orderPreserving);
            }
            return result;
        }

        // Operator: Where        
        public static IEnumerable<TSource> Where<TSource>(IEnumerable<TSource> source,
                                                          Func<TSource, bool> predicate,
                                                          bool orderPreserving)
        {
            if (s_multiThreading)
            {
                return source.ExtendParallelPipeline(s => s.Where(predicate), orderPreserving);
            }
            else
            {
                return System.Linq.Enumerable.Where(source, predicate);
            }
        }

        public static IEnumerable<TSource> Where<TSource>(IEnumerable<TSource> source,
                                                          Func<TSource, int, bool> predicate,
                                                          bool orderPreserving)
        {
            return System.Linq.Enumerable.Where(source, predicate);
        }

        public static IEnumerable<TSource> LongWhere<TSource>(IEnumerable<TSource> source,
                                                              Func<TSource, long, bool> predicate,
                                                              bool orderPreserving)
        {
            return DryadLinqEnumerable.LongWhere(source, predicate);
        }

        // Operator: Select        
        public static IEnumerable<TResult>
            Select<TSource, TResult>(IEnumerable<TSource> source,
                                     Func<TSource, TResult> selector,
                                     bool orderPreserving)
        {
            if (s_multiThreading)
            {
                return source.ExtendParallelPipeline(s => s.Select(selector), orderPreserving);
            }
            else
            {
                return System.Linq.Enumerable.Select(source, selector);
            }
        }

        public static IEnumerable<TResult>
            Select<TSource, TResult>(IEnumerable<TSource> source,
                                     Func<TSource, int, TResult> selector,
                                     bool orderPreserving)
        {
            return System.Linq.Enumerable.Select(source, selector);
        }

        public static IEnumerable<TResult>
            LongSelect<TSource, TResult>(IEnumerable<TSource> source,
                                         Func<TSource, long, TResult> selector,
                                         bool orderPreserving)
        {
            return DryadLinqEnumerable.LongSelect(source, selector);
        }

        // Operator: SelectMany
        public static IEnumerable<TResult>
            SelectMany<TSource, TResult>(IEnumerable<TSource> source,
                                         Func<TSource, IEnumerable<TResult>> selector,
                                         bool orderPreserving)
        {
            if (s_multiThreading)
            {
                return source.ExtendParallelPipeline(s => s.SelectMany(selector), orderPreserving);
            }
            else
            {
                return System.Linq.Enumerable.SelectMany(source, selector);
            }
        }

        public static IEnumerable<TResult>
            SelectMany<TSource, TResult>(IEnumerable<TSource> source,
                                         Func<TSource, int, IEnumerable<TResult>> selector,
                                         bool orderPreserving)
        {
            return System.Linq.Enumerable.SelectMany(source, selector);
        }

        public static IEnumerable<TResult>
            SelectMany<TSource, TCollection, TResult>(IEnumerable<TSource> source,
                                                      Func<TSource, IEnumerable<TCollection>> collectionSelector,
                                                      Func<TSource, TCollection, TResult> resultSelector,
                                                      bool orderPreserving)
        {
            if (s_multiThreading)
            {
                return source.ExtendParallelPipeline(s => s.SelectMany(collectionSelector, resultSelector), orderPreserving);
            }
            else
            {
                return System.Linq.Enumerable.SelectMany(source, collectionSelector, resultSelector);
            }
        }

        public static IEnumerable<TResult>
            SelectMany<TSource, TCollection, TResult>(IEnumerable<TSource> source,
                                                      Func<TSource, int, IEnumerable<TCollection>> collectionSelector,
                                                      Func<TSource, TCollection, TResult> resultSelector,
                                                      bool orderPreserving)
        {
            return System.Linq.Enumerable.SelectMany(source, collectionSelector, resultSelector);
        }

        public static IEnumerable<TResult>
            LongSelectMany<TSource, TResult>(IEnumerable<TSource> source,
                                             Func<TSource, long, IEnumerable<TResult>> collectionSelector,
                                             bool orderPreserving)
        {
            return DryadLinqEnumerable.LongSelectMany(source, collectionSelector);
        }

        public static IEnumerable<TResult>
            LongSelectMany<TSource, TResult>(IEnumerable<TSource> source,
                                             Func<TSource, long, IEnumerable<TResult>> collectionSelector,
                                             Func<TSource, TResult, TResult> resultSelector,
                                             bool orderPreserving)
        {
            return DryadLinqEnumerable.LongSelectMany(source, collectionSelector, resultSelector);
        }

        // Operator: Zip
        private static IEnumerable<Pair<T1, T2>> ZipToPairs<T1, T2>(IEnumerable<T1> s1,
                                                                    IEnumerable<T2> s2)
        {
            IEnumerator<T1> elems1 = s1.GetEnumerator();
            IEnumerator<T2> elems2 = s2.GetEnumerator();
            while (elems1.MoveNext() && elems2.MoveNext())
            {
                yield return new Pair<T1, T2>(elems1.Current, elems2.Current);
            }
        }

        public static IEnumerable<T3> Zip<T1, T2, T3>(IEnumerable<T1> s1, 
                                                      IEnumerable<T2> s2, 
                                                      Func<T1, T2, T3> zipper,
                                                      bool orderPreserving)
        {
            var pairs = ZipToPairs(s1, s2);
            return Select(pairs, x => zipper(x.Key, x.Value), orderPreserving);
        }

        // Operator: Take
        public static IEnumerable<TSource>
            Take<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Take(count);
        }

        // Operator: TakeWhile
        public static IEnumerable<TSource>
            TakeWhile<TSource>(IEnumerable<Pair<List<TSource>, bool>> source)
        {
            foreach (Pair<List<TSource>, bool> group in source)
            {
                foreach (TSource elem in group.Key)
                {
                    yield return elem;
                }
                if (!group.Value) yield break;
            }
        }
                                    
        // Operator: Skip
        public static IEnumerable<TSource>
            Skip<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Skip(count);
        }

        // Operator: SkipWhile
        public static IEnumerable<TSource>
            SkipWhile<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return source.SkipWhile(predicate);
        }

        public static IEnumerable<TSource>
            SkipWhile<TSource>(IEnumerable<TSource> source, Func<TSource, int, bool> predicate)
        {
            return source.SkipWhile(predicate);
        }

        public static IEnumerable<TSource>
            LongSkipWhile<TSource>(IEnumerable<TSource> source,
                                   Func<TSource, long, bool> predicate)
        {
            long index = -1;
            bool yielding = false;
            using (IEnumerator<TSource> sourceEnum = source.GetEnumerator())
            {
                while (sourceEnum.MoveNext())
                {
                    checked { index++; }
                    if (!predicate(sourceEnum.Current, index))
                    {
                        yielding = true;
                        break;
                    }
                }

                if (yielding)
                {
                    do
                    {
                        yield return sourceEnum.Current;
                    }
                    while (sourceEnum.MoveNext());
                }
            }
        }
        
        // Operator: OrderBy
        public static IEnumerable<TSource>
            Sort<TSource, TKey>(IEnumerable<TSource> source,
                                Func<TSource, TKey> keySelector,
                                IComparer<TKey> comparer,
                                bool isDescending,
                                bool isIdKeySelector,
                                DryadLinqFactory<TSource> factory)
        {
            if (s_multiThreading)
            {
                return new ParallelSort<TSource, TKey>(
                         source, keySelector, comparer, isDescending, isIdKeySelector, factory);
            }
            else
            {
                if (isDescending)
                {
                    return Enumerable.OrderByDescending(source, keySelector, comparer);
                }
                else
                {
                    return Enumerable.OrderBy(source, keySelector, comparer);
                }
            }
        }

        public static IEnumerable<TSource>
            MergeSort<TSource, TKey>(this IEnumerable<TSource> source,
                                     Func<TSource, TKey> keySelector,
                                     IComparer<TKey> comparer,
                                     bool isDescending)
        {
            IMultiEnumerable<TSource> msource = source as IMultiEnumerable<TSource>;
            if (msource == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.SourceOfMergesortMustBeMultiEnumerable,
                                             SR.SourceOfMergesortMustBeMultiEnumerable);
            }
            if (msource.NumberOfInputs == 1)
            {
                return source;
            }

            if (s_multiThreading)
            {
                return new ParallelMergeSort<TSource, TKey>(msource, keySelector, comparer, isDescending);
            }
            else
            {
                return SequentialMergeSort(msource, keySelector, comparer, isDescending);
            }
        }

        private static IEnumerable<TSource>
            SequentialMergeSort<TSource, TKey>(IMultiEnumerable<TSource> source,
                                               Func<TSource, TKey> keySelector,
                                               IComparer<TKey> comparer,
                                               bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            // Initialize
            IEnumerator<TSource>[] readers = new IEnumerator<TSource>[source.NumberOfInputs];
            for (int i = 0; i < readers.Length; i++)
            {
                readers[i] = source[i].GetEnumerator();
            }

            DryadLinqLog.AddInfo("Sequential MergeSort started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

            TSource[] elems = new TSource[readers.Length];
            TKey[] keys = new TKey[readers.Length];
            int lastIdx = readers.Length - 1;
            int readerCnt = 0;
            while (readerCnt <= lastIdx)
            {
                elems[readerCnt] = default(TSource);
                if (readers[readerCnt].MoveNext())
                {
                    elems[readerCnt] = readers[readerCnt].Current;
                    keys[readerCnt] = keySelector(elems[readerCnt]);
                    readerCnt++;
                }
                else
                {
                    readers[readerCnt].Dispose();
                    if (readerCnt == lastIdx) break;
                    readers[readerCnt] = readers[lastIdx];
                    lastIdx--;
                }
            }

            // Merge sort
            while (readerCnt > 0)
            {
                TKey key = keys[0];
                int idx = 0;
                for (int i = 1; i < readerCnt; i++)
                {
                    int cmp = comparer.Compare(key, keys[i]);
                    int cmpRes = (isDescending) ? -cmp : cmp;
                    if (cmpRes > 0)
                    {
                        key = keys[i];
                        idx = i;
                    }
                }

                yield return elems[idx];

                if (readers[idx].MoveNext())
                {
                    elems[idx] = readers[idx].Current;
                    keys[idx] = keySelector(elems[idx]);
                }
                else
                {
                    readers[idx].Dispose();
                    readerCnt--;
                    if (idx < readerCnt)
                    {
                        readers[idx] = readers[readerCnt];
                        elems[idx] = elems[readerCnt];
                        keys[idx] = keys[readerCnt];
                    }
                }
            }
            
            DryadLinqLog.AddInfo("Sequential MergeSort ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
        }

        // Operator: ThenBy        
        public static IEnumerable<TSource>
            ThenBy<TSource, TKey>(IEnumerable<TSource> source,
                                  Func<TSource, TKey> keySelector,
                                  IComparer<TKey> comparer,
                                  bool isDescending)
        {
            throw new DryadLinqException(DryadLinqErrorCode.ThenByNotSupported, SR.ThenByNotSupported);
        }

        // Operator: GroupBy
        public static IEnumerable<Pair<TKey, TResult>>
            GroupBy<TSource, TKey, TResult>(
                              IEnumerable<TSource> source,
                              Func<TSource, TKey> keySelector,
                              Func<TSource, TResult> seed,
                              Func<TResult, TSource, TResult> accumulator,
                              IEqualityComparer<TKey> comparer,
                              bool isPartial)
        {
            return GroupBy(source, keySelector, x => x, seed, accumulator, comparer, isPartial);
        }

        public static IEnumerable<Pair<TKey, TResult>>
            GroupBy<TSource, TKey, TElement, TResult>(
                              IEnumerable<TSource> source,
                              Func<TSource, TKey> keySelector,
                              Func<TSource, TElement> elementSelector,
                              Func<TElement, TResult> seed,
                              Func<TResult, TElement, TResult> accumulator,
                              IEqualityComparer<TKey> comparer,
                              bool isPartial)
        {
            if (s_multiThreading)
            {
                if (isPartial)
                {
                    if (source is IParallelApply<TSource>)
                    {
                        IParallelApply<TSource> parSource = (IParallelApply<TSource>)source;
                        return parSource.ExtendGroupBy(keySelector, elementSelector, seed, accumulator, comparer);
                    }
                    else
                    {
                        return new ParallelHashGroupByPartialAccumulate<TSource, TSource, TKey, TElement, TResult>(
                                         source, null, keySelector, elementSelector, seed, accumulator, comparer);
                    }
                }
                else
                {
                    return new ParallelHashGroupByFullAccumulate<TSource, TKey, TElement, TResult, Pair<TKey, TResult>>(
                                     source, keySelector, elementSelector, seed, accumulator, comparer, null);
                }
            }
            else
            {
                return SequentialHashGroupBy(source, keySelector, elementSelector, seed, accumulator, comparer);
            }
        }

        private static IEnumerable<Pair<TKey, TResult>>
            SequentialHashGroupBy<TSource, TKey, TElement, TResult>(
                              IEnumerable<TSource> source,
                              Func<TSource, TKey> keySelector,
                              Func<TSource, TElement> elementSelector,
                              Func<TElement, TResult> seed,
                              Func<TResult, TElement, TResult> accumulator,
                              IEqualityComparer<TKey> comparer)
        {
            DryadLinqLog.AddInfo("Sequential HashGroupBy (Acc) started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
            
            AccumulateDictionary<TKey, TElement, TResult>
                groups = new AccumulateDictionary<TKey, TElement, TResult>(comparer, 16411, seed, accumulator);
            foreach (TSource item in source)
            {
                groups.Add(keySelector(item), elementSelector(item));
            }

            DryadLinqLog.AddInfo("Sequential HashGroupBy (Acc) ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

            return groups;
        }

        public static IEnumerable<IGrouping<TKey, TSource>>
            GroupBy<TSource, TKey>(IEnumerable<TSource> source,
                                        Func<TSource, TKey> keySelector,
                                        IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            GroupingHashSet<TSource, TKey> groupings = new GroupingHashSet<TSource, TKey>(comparer, 16411);
            foreach (TSource item in source)
            {
                groupings.AddItem(keySelector(item), item);
            }
            return groupings;
        }

        public static IEnumerable<TResult>
            GroupBy<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                 Func<TSource, TKey> keySelector,
                                                 Func<TKey, IEnumerable<TSource>, TResult> resultSelector,
                                                 IEqualityComparer<TKey> comparer)
        {
            var groupings = GroupBy(source, keySelector, comparer);
            if (s_multiThreading)
            {
                return new ParallelApply<IGrouping<TKey, TSource>, TResult>(
                                 groupings, s => s.Select(g => resultSelector(g.Key, g)), false);
            }
            else
            {
                return Apply(groupings, s => s.Select(g => resultSelector(g.Key, g)));
            }
        }

        public static IEnumerable<IGrouping<TKey, TElement>>
            GroupBy<TSource, TKey, TElement>(IEnumerable<TSource> source,
                                             Func<TSource, TKey> keySelector,
                                             Func<TSource, TElement> elementSelector,
                                             IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            GroupingHashSet<TElement, TKey> groupings = new GroupingHashSet<TElement, TKey>(comparer, 16411);
            foreach (TSource item in source)
            {
                groupings.AddItem(keySelector(item), elementSelector(item));
            }
            return groupings;
        }

        public static IEnumerable<TResult>
            GroupBy<TSource, TKey, TElement, TResult>(IEnumerable<TSource> source,
                                                      Func<TSource, TKey> keySelector,
                                                      Func<TSource, TElement> elementSelector,
                                                      Func<TKey, IEnumerable<TElement>, TResult> resultSelector,
                                                      IEqualityComparer<TKey> comparer)
        {
            var groupings = GroupBy(source, keySelector, elementSelector, comparer);
            if (s_multiThreading)
            {
                return new ParallelApply<IGrouping<TKey, TElement>, TResult>(
                                 groupings, s => s.Select(g => resultSelector(g.Key, g)), false);
            }
            else
            {
                return Apply(groupings, s => s.Select(g => resultSelector(g.Key, g)));
            }
        }

        // Operator: OrderedGroupBy        
        public static IEnumerable<Pair<TKey, TResult>>
            OrderedGroupBy<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                   Func<TSource, TKey> keySelector,
                                                   Func<TSource, TResult> seed,
                                                   Func<TResult, TSource, TResult> accumulator,
                                                   IEqualityComparer<TKey> comparer,
                                                   bool isPartial)
        {
            return OrderedGroupBy(source, keySelector, x => x, seed, accumulator, comparer, isPartial);
        }

        public static IEnumerable<Pair<TKey, TResult>>
            OrderedGroupBy<TSource, TKey, TElement, TResult>(IEnumerable<TSource> source,
                                                             Func<TSource, TKey> keySelector,
                                                             Func<TSource, TElement> elementSelector,
                                                             Func<TElement, TResult> seed,
                                                             Func<TResult, TElement, TResult> accumulator,
                                                             IEqualityComparer<TKey> comparer,
                                                             bool isPartial)
        {
            if (s_multiThreading)
            {
                return new ParallelOrderedGroupByAccumulate<TSource, TKey, TElement, TResult, Pair<TKey, TResult>>(
                                 source, keySelector, elementSelector, seed, accumulator, comparer, null);
            }
            else
            {
                return SequentialOrderedGroupBy(source, keySelector, elementSelector, seed, accumulator, comparer);
            }
        }

        private static IEnumerable<Pair<TKey, TResult>>
            SequentialOrderedGroupBy<TSource, TKey, TElement, TResult>(
                                 IEnumerable<TSource> source,
                                 Func<TSource, TKey> keySelector,
                                 Func<TSource, TElement> elementSelector,
                                 Func<TElement, TResult> seed,
                                 Func<TResult, TElement, TResult> accumulator,
                                 IEqualityComparer<TKey> comparer)
        {
            DryadLinqLog.AddInfo("Sequential OrderedGroupBy (Acc) started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

            using (IEnumerator<TSource> elems = source.GetEnumerator())
            {
                if (elems.MoveNext())
                {
                    TKey curKey = keySelector(elems.Current);
                    TResult curValue = seed(elementSelector(elems.Current));

                    while (elems.MoveNext())
                    {
                        if (comparer.Equals(curKey, keySelector(elems.Current)))
                        {
                            curValue = accumulator(curValue, elementSelector(elems.Current));
                        }
                        else
                        {
                            yield return new Pair<TKey, TResult>(curKey, curValue);
                            curKey = keySelector(elems.Current);
                            curValue = seed(elementSelector(elems.Current));
                        }
                    }

                    yield return new Pair<TKey, TResult>(curKey, curValue);
                }
            }
            
            DryadLinqLog.AddInfo("Sequential OrderedGroupBy (Acc) ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff")); 
        }

        public static IEnumerable<IGrouping<TKey, TSource>>
            OrderedGroupBy<TSource, TKey>(IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            DryadLinqLog.AddInfo("Sequential OrderedGroupBy started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            

            using (IEnumerator<TSource> elems = source.GetEnumerator())
            {
                Grouping<TKey, TSource> curGroup;
                if (elems.MoveNext())
                {
                    curGroup = new Grouping<TKey, TSource>(keySelector(elems.Current));
                    curGroup.AddItem(elems.Current);

                    while (elems.MoveNext())
                    {
                        if (!comparer.Equals(curGroup.Key, keySelector(elems.Current)))
                        {
                            yield return curGroup;
                            curGroup = new Grouping<TKey, TSource>(keySelector(elems.Current));
                        }
                        curGroup.AddItem(elems.Current);
                    }
                    yield return curGroup;
                }
            }

            DryadLinqLog.AddInfo("Sequential OrderedGroupBy ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            
        }

        public static IEnumerable<IGrouping<TKey, TElement>>
            OrderedGroupBy<TSource, TKey, TElement>(IEnumerable<TSource> source,
                                                    Func<TSource, TKey> keySelector,
                                                    Func<TSource, TElement> elementSelector,
                                                    IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            DryadLinqLog.AddInfo("Sequential OrderedGroupBy started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            

            using (IEnumerator<TSource> elems = source.GetEnumerator())
            {
                Grouping<TKey, TElement> curGroup;
                if (elems.MoveNext())
                {
                    curGroup = new Grouping<TKey, TElement>(keySelector(elems.Current));
                    curGroup.AddItem(elementSelector(elems.Current));

                    while (elems.MoveNext())
                    {
                        if (!comparer.Equals(curGroup.Key, keySelector(elems.Current)))
                        {
                            yield return curGroup;
                            curGroup = new Grouping<TKey, TElement>(keySelector(elems.Current));
                        }
                        curGroup.AddItem(elementSelector(elems.Current));
                    }
                    yield return curGroup;
                }
            }

            DryadLinqLog.AddInfo("Sequential OrderedGroupBy ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            
        }

        public static IEnumerable<TResult>
           OrderedGroupBy<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                  Func<TSource, TKey> keySelector,
                                                  Func<TKey, IEnumerable<TSource>, TResult> resultSelector,
                                                  IEqualityComparer<TKey> comparer)
        {
            var groupings = OrderedGroupBy(source, keySelector, comparer);
            if (s_multiThreading)
            {
                return new ParallelApply<IGrouping<TKey, TSource>, TResult>(
                                 groupings, s => s.Select(g => resultSelector(g.Key, g)), true);
            }
            else
            {
                return Apply(groupings, s => s.Select(g => resultSelector(g.Key, g)));
            }

        }

        public static IEnumerable<TResult>
           OrderedGroupBy<TSource, TKey, TElement, TResult>(
                                   IEnumerable<TSource> source,
                                   Func<TSource, TKey> keySelector,
                                   Func<TSource, TElement> elementSelector,
                                   Func<TKey, IEnumerable<TElement>, TResult> resultSelector,
                                   IEqualityComparer<TKey> comparer)
        {
            var groupings = OrderedGroupBy(source, keySelector, elementSelector, comparer);
            if (s_multiThreading)
            {
                return new ParallelApply<IGrouping<TKey, TElement>, TResult>(
                                 groupings, s => s.Select(g => resultSelector(g.Key, g)), true);
            }
            else
            {
                return Apply(groupings, s => s.Select(g => resultSelector(g.Key, g)));
            }
        }

        // Operator: Join
        internal static IEnumerable<TResult>
            SequentialHashJoin<TOuter, TInner, TKey, TResult>(
                                         IEnumerable<TOuter> outer,
                                         IEnumerable<TInner> inner,
                                         Func<TOuter, TKey> outerKeySelector,
                                         Func<TInner, TKey> innerKeySelector,
                                         Func<TOuter, TInner, TResult> resultSelector,
                                         IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            DryadLinqLog.AddInfo("Sequential HashJoin started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
            
            bool hashInner = true;
            if ((outer is DryadLinqVertexReader<TOuter>) && (inner is DryadLinqVertexReader<TInner>))
            {
                Int64 outerLen = ((DryadLinqVertexReader<TOuter>)outer).GetTotalLength();
                Int64 innerLen = ((DryadLinqVertexReader<TInner>)inner).GetTotalLength();
                if (innerLen >= 0 && outerLen >= 0)
                {
                    hashInner = innerLen <= outerLen;
                }
            }

            if (hashInner)
            {
                // Create a hash lookup table using inner
                GroupingHashSet<TInner, TKey> innerGroupings = new GroupingHashSet<TInner, TKey>(comparer);
                foreach (TInner innerItem in inner)
                {
                    innerGroupings.AddItem(innerKeySelector(innerItem), innerItem);
                }
                foreach (TOuter outerItem in outer)
                {
                    Grouping<TKey, TInner> innerGroup = innerGroupings.GetGroup(outerKeySelector(outerItem));
                    if (innerGroup != null)
                    {
                        TInner[] items = innerGroup.Elements;
                        for (int i = 0; i < innerGroup.Count(); i++)
                        {
                            yield return resultSelector(outerItem, items[i]);
                        }
                    }
                }
            }
            else
            {
                // Create a hash lookup table using outer
                GroupingHashSet<TOuter, TKey> outerGroupings = new GroupingHashSet<TOuter, TKey>(comparer);
                foreach (TOuter outerItem in outer)
                {
                    outerGroupings.AddItem(outerKeySelector(outerItem), outerItem);
                }

                foreach (TInner innerItem in inner)
                {
                    Grouping<TKey, TOuter> outerGroup = outerGroupings.GetGroup(innerKeySelector(innerItem));
                    if (outerGroup != null)
                    {
                        TOuter[] items = outerGroup.Elements;
                        for (int i = 0; i < outerGroup.Count(); i++)
                        {
                            yield return resultSelector(items[i], innerItem);
                        }
                    }
                }
            }

            DryadLinqLog.AddInfo("Sequential HashJoin ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            
        }
        
        // Perform a hash join.
        public static IEnumerable<TResult>
            HashJoin<TOuter, TInner, TKey, TResult>(IEnumerable<TOuter> outer,
                                                    IEnumerable<TInner> inner,
                                                    Func<TOuter, TKey> outerKeySelector,
                                                    Func<TInner, TKey> innerKeySelector,
                                                    Func<TOuter, TInner, TResult> resultSelector,
                                                    IEqualityComparer<TKey> comparer)
        {
            if (s_multiThreading)
            {
                return new ParallelHashJoin<TOuter, TInner, TKey, TResult, TResult>(
                                 outer, inner, outerKeySelector, innerKeySelector,
                                 resultSelector, comparer, null);
            }
            else
            {
                return SequentialHashJoin(outer, inner, outerKeySelector, innerKeySelector,
                                          resultSelector, comparer);
            }
        }

        public static IEnumerable<TFinal>
            HashJoin<TOuter, TInner, TKey, TResult, TFinal>(IEnumerable<TOuter> outer,
                                                            IEnumerable<TInner> inner,
                                                            Func<TOuter, TKey> outerKeySelector,
                                                            Func<TInner, TKey> innerKeySelector,
                                                            Func<TOuter, TInner, TResult> resultSelector,
                                                            IEqualityComparer<TKey> comparer,
                                                            Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            if (s_multiThreading)
            {
                return new ParallelHashJoin<TOuter, TInner, TKey, TResult, TFinal>(
                                 outer, inner, outerKeySelector, innerKeySelector,
                                 resultSelector, comparer, applyFunc);
            }
            else
            {
                var results = SequentialHashJoin(outer, inner, outerKeySelector, innerKeySelector,
                                                 resultSelector, comparer);
                return applyFunc(results);
            }
        }

        // Perform a merge join
        // Precondition: both outer and inner inputs are sorted based on TKey
        public static IEnumerable<TResult>
            MergeJoin<TOuter, TInner, TKey, TResult>(IEnumerable<TOuter> outer,
                                                     IEnumerable<TInner> inner,
                                                     Func<TOuter, TKey> outerKeySelector,
                                                     Func<TInner, TKey> innerKeySelector,
                                                     Func<TOuter, TInner, TResult> resultSelector,
                                                     IComparer<TKey> comparer,
                                                     bool isDescending)
        {
            var joinPairs = MergeJoin(outer, inner, outerKeySelector, innerKeySelector, comparer, isDescending);

            if (s_multiThreading)
            {
                return new ParallelApply<Pair<TOuter, TInner>, TResult>(
                                 joinPairs, s => s.Select(x => resultSelector(x.Key, x.Value)), true);
            }
            else
            {
                return Apply(joinPairs, s => s.Select(x => resultSelector(x.Key, x.Value)));
            }
        }

        public static IEnumerable<TFinal>
            MergeJoin<TOuter, TInner, TKey, TResult, TFinal>(
                                    IEnumerable<TOuter> outer,
                                    IEnumerable<TInner> inner,
                                    Func<TOuter, TKey> outerKeySelector,
                                    Func<TInner, TKey> innerKeySelector,
                                    Func<TOuter, TInner, TResult> resultSelector,
                                    IComparer<TKey> comparer,
                                    bool isDescending,
                                    Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            var joinPairs = MergeJoin(outer, inner, outerKeySelector, innerKeySelector, comparer, isDescending);
            if (s_multiThreading)
            {
                return new ParallelApply<Pair<TOuter, TInner>, TFinal>(
                                 joinPairs, s => applyFunc(s.Select(x => resultSelector(x.Key, x.Value))), true);
            }
            else
            {
                return Apply(joinPairs, s => applyFunc(s.Select(x => resultSelector(x.Key, x.Value))));
            }
        }

        public static IEnumerable<Pair<TOuter, TInner>>
            MergeJoin<TOuter, TInner, TKey>(IEnumerable<TOuter> outer,
                                            IEnumerable<TInner> inner,
                                            Func<TOuter, TKey> outerKeySelector,
                                            Func<TInner, TKey> innerKeySelector,
                                            IComparer<TKey> comparer,
                                            bool isDescending)
        {        
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            DryadLinqLog.AddInfo("Sequential MergeJoin started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            

            IEnumerator<TOuter> outerEnum = outer.GetEnumerator();
            IEnumerator<TInner> innerEnum = inner.GetEnumerator();
            TOuter[] outerItemArray = new TOuter[4];
            TInner[] innerItemArray = new TInner[4];
            bool outerHasMoreWork = outerEnum.MoveNext();
            bool innerHasMoreWork = innerEnum.MoveNext();
            while (outerHasMoreWork && innerHasMoreWork)
            {
                TOuter outerItem = outerEnum.Current;
                TInner innerItem = innerEnum.Current;
                TKey outerKey = outerKeySelector(outerItem);
                TKey innerKey = innerKeySelector(innerItem);

                int cmpResult = comparer.Compare(outerKey, innerKey);
                cmpResult = (isDescending) ? -cmpResult : cmpResult;
                if (cmpResult < 0)
                {
                    outerHasMoreWork = outerEnum.MoveNext();
                }
                else if (cmpResult > 0)
                {
                    innerHasMoreWork = innerEnum.MoveNext();
                }
                else
                {
                    // Get all the outer items with the same key:
                    outerItemArray[0] = outerItem;
                    int outerCnt = 1;
                    while (true)
                    {
                        outerHasMoreWork = outerEnum.MoveNext();
                        if (!outerHasMoreWork ||
                            comparer.Compare(outerKey, outerKeySelector(outerEnum.Current)) != 0)
                        {
                            break;
                        }
                        if (outerCnt == outerItemArray.Length)
                        {
                            TOuter[] newOuterItemArray = new TOuter[outerItemArray.Length * 2];
                            Array.Copy(outerItemArray, 0, newOuterItemArray, 0, outerItemArray.Length);
                            outerItemArray = newOuterItemArray;
                        }
                        outerItemArray[outerCnt++] = outerEnum.Current;
                    }

                    // Get all the inner items with the same key:
                    innerItemArray[0] = innerItem;
                    int innerCnt = 1;
                    while (true)
                    {
                        innerHasMoreWork = innerEnum.MoveNext();
                        if (!innerHasMoreWork ||
                            comparer.Compare(innerKey, innerKeySelector(innerEnum.Current)) != 0)
                        {
                            break;
                        }
                        if (innerCnt == innerItemArray.Length)
                        {
                            TInner[] newInnerItemArray = new TInner[innerItemArray.Length * 2];
                            Array.Copy(innerItemArray, 0, newInnerItemArray, 0, innerItemArray.Length);
                            innerItemArray = newInnerItemArray;
                        }
                        innerItemArray[innerCnt++] = innerEnum.Current;
                    }

                    // Yield items:
                    for (int i = 0; i < outerCnt; i++)
                    {
                        for (int j = 0; j < innerCnt; j++)
                        {
                            yield return new Pair<TOuter, TInner>(outerItemArray[i], innerItemArray[j]);
                        }
                    }
                }
            }

            DryadLinqLog.AddInfo("Sequential MergeJoin ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            
        }

        internal static IEnumerable<TResult>
            SequentialHashGroupJoin<TOuter, TInner, TKey, TResult>(
                                              IEnumerable<TOuter> outer,
                                              IEnumerable<TInner> inner,
                                              Func<TOuter, TKey> outerKeySelector,
                                              Func<TInner, TKey> innerKeySelector,
                                              Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                              IEqualityComparer<TKey> comparer)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            // Create a hash lookup table using inner. It is hard to do the same
            // optimization as Join, because resultSelector is not symemtric.
            GroupingHashSet<TInner, TKey> innerGroupings = new GroupingHashSet<TInner, TKey>(comparer);
            foreach (TInner innerItem in inner)
            {
                innerGroupings.AddItem(innerKeySelector(innerItem), innerItem);
            }

            TInner[] emptyGroup = new TInner[0];
            foreach (TOuter outerItem in outer)
            {
                IEnumerable<TInner> innerGroup = innerGroupings.GetGroup(outerKeySelector(outerItem));
                if (innerGroup == null)
                {
                    innerGroup = emptyGroup;
                }
                yield return resultSelector(outerItem, innerGroup);
            }
        }


        public static IEnumerable<TResult>
            HashGroupJoin<TOuter, TInner, TKey, TResult>(IEnumerable<TOuter> outer,
                                                         IEnumerable<TInner> inner,
                                                         Func<TOuter, TKey> outerKeySelector,
                                                         Func<TInner, TKey> innerKeySelector,
                                                         Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                                         IEqualityComparer<TKey> comparer)
        {
            if (s_multiThreading)
            {
                return new ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TResult>(
                                 outer, inner, outerKeySelector, innerKeySelector,
                                 resultSelector, comparer, null);
            }
            else
            {
                return SequentialHashGroupJoin(outer, inner, outerKeySelector, innerKeySelector,
                                               resultSelector, comparer);
            }
        }

        public static IEnumerable<TFinal>
            HashGroupJoin<TOuter, TInner, TKey, TResult, TFinal>(
                                          IEnumerable<TOuter> outer,
                                          IEnumerable<TInner> inner,
                                          Func<TOuter, TKey> outerKeySelector,
                                          Func<TInner, TKey> innerKeySelector,
                                          Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                          IEqualityComparer<TKey> comparer,
                                          Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            if (s_multiThreading)
            {
                return new ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TFinal>(
                                 outer, inner, outerKeySelector, innerKeySelector,
                                 resultSelector, comparer, applyFunc);
            }
            else
            {
                var results = SequentialHashGroupJoin(outer, inner, outerKeySelector, innerKeySelector,
                                                      resultSelector, comparer);
                return applyFunc(results);
            }
        }
        
        // Precondition: both outer and inner inputs are sorted based on TKey        
        public static IEnumerable<TResult>
            MergeGroupJoin<TOuter, TInner, TKey, TResult>(IEnumerable<TOuter> outer,
                                                          IEnumerable<TInner> inner,
                                                          Func<TOuter, TKey> outerKeySelector,
                                                          Func<TInner, TKey> innerKeySelector,
                                                          Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                                          IComparer<TKey> comparer,
                                                          bool isDescending)
        {
            var joinPairs = MergeGroupJoin(outer, inner, outerKeySelector, innerKeySelector, comparer, isDescending);

            if (s_multiThreading)
            {
                return new ParallelApply<Pair<TOuter, List<TInner>>, TResult>(
                                 joinPairs, s => s.Select(x => resultSelector(x.Key, x.Value)), true);
            }
            else
            {
                return Apply(joinPairs, s => s.Select(x => resultSelector(x.Key, x.Value)));
            }
        }

        public static IEnumerable<TFinal>
            MergeGroupJoin<TOuter, TInner, TKey, TResult, TFinal>(IEnumerable<TOuter> outer,
                                                                  IEnumerable<TInner> inner,
                                                                  Func<TOuter, TKey> outerKeySelector,
                                                                  Func<TInner, TKey> innerKeySelector,
                                                                  Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                                                  IComparer<TKey> comparer,
                                                                  bool isDescending,
                                                                  Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            var joinPairs = MergeGroupJoin(outer, inner, outerKeySelector, innerKeySelector, comparer, isDescending);

            if (s_multiThreading)
            {
                return new ParallelApply<Pair<TOuter, List<TInner>>, TFinal>(
                                 joinPairs, s => applyFunc(s.Select(x => resultSelector(x.Key, x.Value))), true);
            }
            else
            {
                return Apply(joinPairs, s => applyFunc(s.Select(x => resultSelector(x.Key, x.Value))));
            }
        }

        public static IEnumerable<Pair<TOuter, List<TInner>>>
            MergeGroupJoin<TOuter, TInner, TKey>(IEnumerable<TOuter> outer,
                                                 IEnumerable<TInner> inner,
                                                 Func<TOuter, TKey> outerKeySelector,
                                                 Func<TInner, TKey> innerKeySelector,
                                                 IComparer<TKey> comparer,
                                                 bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            DryadLinqLog.AddInfo("Sequential MergeGroupJoin started reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            

            IEnumerator<TOuter> outerEnum = outer.GetEnumerator();
            IEnumerator<TInner> innerEnum = inner.GetEnumerator();
            List<TInner> innerItemList = new List<TInner>(8);
            bool hasMoreWork = outerEnum.MoveNext() && innerEnum.MoveNext();
            while (hasMoreWork)
            {
                TOuter outerItem = outerEnum.Current;
                TInner innerItem = innerEnum.Current;
                TKey outerKey = outerKeySelector(outerItem);
                TKey innerKey = innerKeySelector(innerItem);
                int cmpResult = comparer.Compare(outerKey, innerKey);
                cmpResult = (isDescending) ? -cmpResult : cmpResult;
                if (cmpResult < 0)
                {
                    hasMoreWork = outerEnum.MoveNext();
                }
                else if (cmpResult > 0)
                {
                    hasMoreWork = innerEnum.MoveNext();
                }
                else
                {
                    // Get all the inner items with the same key:
                    innerItemList.Add(innerItem);
                    while (true)
                    {
                        hasMoreWork = innerEnum.MoveNext();
                        if (!hasMoreWork ||
                            comparer.Compare(innerKey, innerKeySelector(innerEnum.Current)) != 0)
                        {
                            break;
                        }
                        innerItemList.Add(innerEnum.Current);
                    }

                    // Yield items:
                    while (true)
                    {
                        yield return new Pair<TOuter, List<TInner>>(outerItem, innerItemList);
                        hasMoreWork = outerEnum.MoveNext();
                        if (!hasMoreWork ||
                            comparer.Compare(outerKey, outerKeySelector(outerEnum.Current)) != 0)
                        {
                            break;
                        }
                        outerItem = outerEnum.Current;
                    }

                    innerItemList.Clear();
                }
            }

            DryadLinqLog.AddInfo("Sequential MergeGroupJoin ended reading at {0}",
                                 DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));            
        }

        // Operator: Concat
        public static IEnumerable<TSource>
            Concat<TSource>(IEnumerable<TSource> source1, IEnumerable<TSource> source2)
        {
            return System.Linq.Enumerable.Concat(source1, source2);
        }

        // Operator: Distinct        
        public static IEnumerable<TResult>
            Distinct<TSource, TResult>(IEnumerable<TSource> source,
                                       IEqualityComparer<TSource> comparer,
                                       Func<IEnumerable<TSource>, IEnumerable<TResult>> applyFunc,
                                       bool isPartial)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TResult>(
                                 "Distinct", source, null, comparer, applyFunc, isPartial);
            }
            else
            {
                var results = Enumerable.Distinct(source, comparer);
                return applyFunc(results);
            }
        }

        public static IEnumerable<TSource>
            Distinct<TSource>(IEnumerable<TSource> source,
                              IEqualityComparer<TSource> comparer,
                              bool isPartial)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Distinct", source, null, comparer, null, isPartial);
            }
            else
            {
                return Enumerable.Distinct(source, comparer);
            }
        }

        // Operator: Union        
        public static IEnumerable<TSource>
            Union<TSource>(IEnumerable<TSource> source1,
                           IEnumerable<TSource> source2)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Union", source1, source2, null, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Union(source1, source2);
            }
        }

        public static IEnumerable<TSource>
            Union<TSource>(IEnumerable<TSource> source1,
                           IEnumerable<TSource> source2,
                           IEqualityComparer<TSource> comparer)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Union", source1, source2, comparer, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Union(source1, source2, comparer);
            }
        }

        /// <summary>
        /// Performs the union of two ordered sources.  It is like mergesort, but removes duplicates.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to union.</typeparam>
        /// <param name="source1">Left sorted stream to union.</param>
        /// <param name="source2">Right sorted stream to union.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise they are in ascending order.</param>
        /// <returns>The union of all elements, in sorted order.</returns>
        public static IEnumerable<TSource>
            OrderedUnion<TSource>(IEnumerable<TSource> source1,
                                  IEnumerable<TSource> source2,
                                  bool isDescending)
        {
            return OrderedUnion(source1, source2, null, isDescending);
        }
        
        /// <summary>
        /// Performs the union of two ordered sources.  It is like mergesort, but removes duplicates.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to union.</typeparam>
        /// <param name="source1">Left sorted stream to union.</param>
        /// <param name="source2">Right sorted stream to union.</param>
        /// <param name="comparer">Comparison function to use for TSource.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise they are in ascending order.</param>
        /// <returns>The union of all elements, in sorted order.</returns>
        public static IEnumerable<TSource>
            OrderedUnion<TSource>(IEnumerable<TSource> source1,
                                  IEnumerable<TSource> source2,
                                  IComparer<TSource> comparer,
                                  bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TSource>(comparer);
            IEnumerator<TSource> enum1 = source1.GetEnumerator();
            IEnumerator<TSource> enum2 = source2.GetEnumerator();
            bool hasMoreWork1 = enum1.MoveNext();
            bool hasMoreWork2 = enum2.MoveNext();
            TSource item1 = default(TSource);
            TSource item2 = default(TSource);
            
            if (hasMoreWork1)
            {
                item1 = enum1.Current;
            }

            if (hasMoreWork2)
            {
                item2 = enum2.Current;
            }

            while (hasMoreWork1 && hasMoreWork2)
            {
                int cmpResult = comparer.Compare(item1, item2);
                cmpResult = (isDescending) ? -cmpResult : cmpResult;
                if (cmpResult <= 0)
                {
                    yield return item1;

                    // skip duplicates:
                    TSource item3 = item1;
                    while (hasMoreWork1 = enum1.MoveNext())
                    {
                        item1 = enum1.Current;
                        if (comparer.Compare(item1, item3) != 0) break;
                    }
                    if (cmpResult == 0)
                    {
                        while (hasMoreWork2 = enum2.MoveNext())
                        {
                            item2 = enum2.Current;
                            if (comparer.Compare(item2, item3) != 0) break;
                        }
                    }
                }
                else
                {
                    yield return item2;

                    // skip duplicates:
                    TSource item3 = item2;
                    while (hasMoreWork2 = enum2.MoveNext())
                    {
                        item2 = enum2.Current;
                        if (comparer.Compare(item2, item3) != 0) break;
                    }
                }
            }

            // yield the remaining items:
            if (hasMoreWork2)
            {
                //switch enum2 over to enum1 to simplify the next block.
                hasMoreWork1 = true;
                enum1 = enum2;
                item1 = item2;
            }

            if (hasMoreWork1)
            {
                while (true)
                {
                    yield return item1;

                    // skip duplicates:
                    item2 = item1;
                    while (true)
                    {
                        if (!enum1.MoveNext()) yield break;
                        item1 = enum1.Current;
                        if (comparer.Compare(item1, item2) != 0) break;
                    }
                }
            }
        }

        // Operator: Intersect
        public static IEnumerable<TSource>
            Intersect<TSource>(IEnumerable<TSource> source1,
                               IEnumerable<TSource> source2)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Intersect", source1, source2, null, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Intersect(source1, source2);
            }
        }

        public static IEnumerable<TSource>
            Intersect<TSource>(IEnumerable<TSource> source1,
                               IEnumerable<TSource> source2,
                               IEqualityComparer<TSource> comparer)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Intersect", source1, source2, comparer, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Intersect(source1, source2, comparer);
            }
        }

        /// <summary>
        /// Compute the intersection of two ordered sources.  Like mergesort, but only keeps common values.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to intersect.</typeparam>
        /// <param name="source1">Left sorted stream of values.</param>
        /// <param name="source2">Right sorted stream of values.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise they are in ascending order.</param>
        /// <returns></returns>
        public static IEnumerable<TSource>
            OrderedIntersect<TSource>(IEnumerable<TSource> source1,
                                      IEnumerable<TSource> source2,
                                      bool isDescending)
        {
            return OrderedIntersect(source1, source2, null, isDescending);
        }

        /// <summary>
        /// Compute the intersection between two ordered sets.  Like mergesort, but only keeps common values.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to intersect.</typeparam>
        /// <param name="source1">Left sorted stream of values.</param>
        /// <param name="source2">Right sorted stream of values.</param>
        /// <param name="comparer">Comparison function to use.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise they are in ascending order.</param>
        /// <returns></returns>
        public static IEnumerable<TSource>
            OrderedIntersect<TSource>(IEnumerable<TSource> source1,
                                      IEnumerable<TSource> source2,
                                      IComparer<TSource> comparer,
                                      bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TSource>(comparer);

            IEnumerator<TSource> enum1 = source1.GetEnumerator();
            IEnumerator<TSource> enum2 = source2.GetEnumerator();
            bool hasMoreWork = enum1.MoveNext() && enum2.MoveNext();
            if (hasMoreWork)
            {
                TSource item1 = enum1.Current;
                TSource item2 = enum2.Current;
                do
                {
                    int cmpResult = comparer.Compare(item1, item2);
                    if (cmpResult == 0)
                    {
                        yield return item1;

                        // skip duplicates:
                        TSource item3 = item1;
                        while (true)
                        {
                            hasMoreWork = enum1.MoveNext();
                            if (!hasMoreWork) yield break;
                            item1 = enum1.Current;
                            if (comparer.Compare(item1, item3) != 0) break;
                        }
                        while (true)
                        {
                            hasMoreWork = enum2.MoveNext();
                            if (!hasMoreWork) yield break;
                            item2 = enum2.Current;
                            if (comparer.Compare(item3, item2) != 0) break;
                        }
                    }
                    else
                    {
                        cmpResult = (isDescending) ? -cmpResult : cmpResult;
                        if (cmpResult < 0)
                        {
                            hasMoreWork = enum1.MoveNext();
                            item1 = enum1.Current;
                        }
                        else
                        {
                            hasMoreWork = enum2.MoveNext();
                            item2 = enum2.Current;
                        }
                    }
                }
                while (hasMoreWork);
            }
        }

        // Operator: Except        
        public static IEnumerable<TSource>
            Except<TSource>(IEnumerable<TSource> source1,
                            IEnumerable<TSource> source2)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Except", source1, source2, null, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Except(source1, source2);
            }
        }

        public static IEnumerable<TSource>
            Except<TSource>(IEnumerable<TSource> source1,
                            IEnumerable<TSource> source2,
                            IEqualityComparer<TSource> comparer)
        {
            if (s_multiThreading)
            {
                return new ParallelSetOperation<TSource, TSource>(
                                 "Except", source1, source2, comparer, null, false);
            }
            else
            {
                return System.Linq.Enumerable.Except(source1, source2, comparer);
            }
        }

        /// <summary>
        /// Perform a set difference between two ordered sources.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to compare.</typeparam>
        /// <param name="source1">Sorted stream from which subtraction occurs.</param>
        /// <param name="source2">Subtracted sorted stream.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise, they are in ascending order. </param>
        /// <returns>Elements in left steram not ocurring in right stream.</returns>
        public static IEnumerable<TSource>
            OrderedExcept<TSource>(IEnumerable<TSource> source1,
                                   IEnumerable<TSource> source2,
                                   bool isDescending)
        {
            return OrderedExcept(source1, source2, null, isDescending);
        }

        /// <summary>
        /// Perform a set difference between two ordered sources.
        /// </summary>
        /// <typeparam name="TSource">Type of elements to compare.</typeparam>
        /// <param name="source1">Sorted stream from which subtraction occurs.</param>
        /// <param name="source2">Subtracted sorted stream.</param>
        /// <param name="comparer">Function to use for comparison testing.</param>
        /// <param name="isDescending">true if both streams are ordered in descending order; 
        /// otherwise, they are in ascending order. </param>
        /// <returns>Elements in left steram not ocurring in right stream.</returns>
        public static IEnumerable<TSource>
            OrderedExcept<TSource>(IEnumerable<TSource> source1,
                                   IEnumerable<TSource> source2,
                                   IComparer<TSource> comparer,
                                   bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TSource>(comparer);

            IEnumerator<TSource> enum1 = source1.GetEnumerator();
            IEnumerator<TSource> enum2 = source2.GetEnumerator();
            bool hasMoreWork1 = enum1.MoveNext();
            bool hasMoreWork2 = enum2.MoveNext();
            if (hasMoreWork1)
            {
                TSource item1 = enum1.Current;
                while (hasMoreWork2)
                {
                    TSource item2 = enum2.Current;
                    int cmpResult = comparer.Compare(item1, item2);
                    if (cmpResult == 0)
                    {
                        // skip duplicates:
                        TSource item3 = item1;
                        while (true)
                        {
                            if (!enum1.MoveNext()) yield break;
                            item1 = enum1.Current;
                            if (comparer.Compare(item1, item3) != 0) break;
                        }
                        while (hasMoreWork2 = enum2.MoveNext())
                        {
                            item2 = enum2.Current;
                            if (comparer.Compare(item2, item3) != 0) break;
                        }
                    }
                    else
                    {
                        cmpResult = (isDescending) ? -cmpResult : cmpResult;
                        if (cmpResult < 0)
                        {
                            yield return item1;

                            // skip duplicates:
                            TSource item3 = item1;
                            while (true)
                            {
                                if (!enum1.MoveNext()) yield break;
                                item1 = enum1.Current;
                                if (comparer.Compare(item1, item3) != 0) break;
                            }
                        }
                        else
                        {
                            hasMoreWork2 = enum2.MoveNext();
                        }
                    }
                }

                // yield the remaining items:
                while (true)
                {
                    yield return item1;

                    // skip duplicates:
                    TSource item2 = item1;
                    while (true)
                    {
                        if (!enum1.MoveNext()) yield break;
                        item1 = enum1.Current;
                        if (comparer.Compare(item1, item2) != 0) break;
                    }
                }
            }
        }

        // Operator: Count        
        // This one could be implemented in native  for better performance.
        public static int Count<TSource>(IEnumerable<TSource> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Count(source);
                }
                int count = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Count()), false);
                foreach (int item in partialResults)
                {
                    checked { count += item; }
                }
                return count;
            }
            else
            {
                return Enumerable.Count(source);
            }
        }

        public static int Count<TSource>(IEnumerable<TSource> source,
                                         Func<TSource, bool> predicate)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Count(source, predicate);
                }
                int count = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Count(predicate)), false);
                foreach (int item in partialResults)
                {
                    checked { count += item; }
                }
                return count;
            }
            else
            {
                return Enumerable.Count(source, predicate);
            }
        }

        // Operator: LongCount        
        // This one could be implemented in native  for better performance.
        public static long LongCount<TSource>(IEnumerable<TSource> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.LongCount(source);
                }
                long count = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.LongCount()), false);
                foreach (long item in partialResults)
                {
                    checked { count += item; }
                }
                return count;
            }
            else
            {
                return Enumerable.LongCount(source);
            }
        }

        public static long LongCount<TSource>(IEnumerable<TSource> source,
                                              Func<TSource, bool> predicate)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.LongCount(source, predicate);
                }
                long count = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.LongCount(predicate)), false);
                foreach (long item in partialResults)
                {
                    checked { count += item; }
                }
                return count;
            }
            else
            {
                return Enumerable.LongCount(source, predicate);
            }
        }

        // Operator: Contains        
        public static bool Contains<TSource>(IEnumerable<TSource> source,
                                             TSource value,
                                             IEqualityComparer<TSource> comparer)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Contains(source, value, comparer);
                }

                var partialResults = pipe.Extend(x => AsEnumerable(x.Contains(value, comparer)), false);
                foreach (bool item in partialResults)
                {
                    if (item) return true;
                }
                return false;
            }
            else
            {
                return Enumerable.Contains(source, value, comparer);
            }
        }

        // Operator: Aggregate        
        public static TSource
            Aggregate<TSource>(IEnumerable<TSource> source,
                               Func<TSource, TSource, TSource> aggregator)
        {
            return System.Linq.Enumerable.Aggregate(source, aggregator);
        }

        public static TAccumulate
            Aggregate<TSource, TAccumulate>(IEnumerable<TSource> source,
                                            TAccumulate seed,
                                            Func<TAccumulate, TSource, TAccumulate> aggregator)
        {
            return System.Linq.Enumerable.Aggregate(source, seed, aggregator);
        }

        public static TAccumulate
            Aggregate<TSource, TAccumulate>(IEnumerable<TSource> source,
                                            Func<TAccumulate> seedFunc,
                                            Func<TAccumulate, TSource, TAccumulate> aggregator)
        {
            return System.Linq.Enumerable.Aggregate(source, seedFunc(), aggregator);
        }

        public static TResult
            Aggregate<TSource, TAccumulate, TResult>(IEnumerable<TSource> source,
                                                     TAccumulate seed,
                                                     Func<TAccumulate, TSource, TAccumulate> aggregator,
                                                     Func<TAccumulate, TResult> resultSelector)
        {
            return System.Linq.Enumerable.Aggregate(source, seed, aggregator, resultSelector);
        }

        public static TResult
            Aggregate<TSource, TAccumulate, TResult>(IEnumerable<TSource> source,
                                                     Func<TAccumulate> seedFunc,
                                                     Func<TAccumulate, TSource, TAccumulate> aggregator,
                                                     Func<TAccumulate, TResult> resultSelector)
        {
            return System.Linq.Enumerable.Aggregate(source, seedFunc(), aggregator, resultSelector);
        }

        // If the aggregate function is associative, AssocAggregate is the first/second stage
        // of the aggregation.
        public static IEnumerable<TSource>
            AssocAggregate<TSource>(IEnumerable<TSource> source,
                                    Func<TSource, TSource, TSource> aggregator,
                                    Func<TSource, TSource, TSource> combiner)
        {
            TSource result = default(TSource);
            var partialResults = source.ExtendParallelPipeline(s => AggregateInner(s, aggregator), false);
            using (IEnumerator<TSource> elems = partialResults.GetEnumerator())
            {
                bool hasElem = elems.MoveNext();
                if (hasElem)
                {
                    result = elems.Current;
                    while (elems.MoveNext())
                    {
                        result = combiner(result, elems.Current);
                    }
                }
                if (hasElem) yield return result;
            }
        }

        private static IEnumerable<TSource>
            AggregateInner<TSource>(IEnumerable<TSource> source,
                                    Func<TSource, TSource, TSource> aggregator)
        {
            TSource result = default(TSource);
            using (IEnumerator<TSource> elems = source.GetEnumerator())
            {
                bool hasElem = elems.MoveNext();
                if (hasElem)
                {
                    result = elems.Current;
                    while (elems.MoveNext())
                    {
                        result = aggregator(result, elems.Current);
                    }
                }
                if (hasElem) yield return result;
            }
        }
        
        public static IEnumerable<TAccumulate>
            AssocAggregate<TSource, TAccumulate>(IEnumerable<TSource> source,
                                                 TAccumulate seed,
                                                 Func<TAccumulate, TSource, TAccumulate> aggregator,
                                                 Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            TAccumulate result = seed;
            var partialResults = source.ExtendParallelPipeline(s => AggregateInner(s, seed, aggregator), false);
            foreach (var elem in partialResults)
            {
                result = combiner(result, elem);
            }
            yield return result;
        }

        private static IEnumerable<TAccumulate>
            AggregateInner<TSource, TAccumulate>(IEnumerable<TSource> source,
                                                 TAccumulate seed,
                                                 Func<TAccumulate, TSource, TAccumulate> aggregator)
        {
            TAccumulate result = seed;
            foreach (TSource elem in source)
            {
                result = aggregator(result, elem);
            }
            yield return result;
        }
        
        public static IEnumerable<TAccumulate>
            AssocAggregate<TSource, TAccumulate>(IEnumerable<TSource> source,
                                                 Func<TAccumulate> seedFunc,
                                                 Func<TAccumulate, TSource, TAccumulate> aggregator,
                                                 Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            TAccumulate result = seedFunc();
            var partialResults = source.ExtendParallelPipeline(s => AggregateInner(s, seedFunc, aggregator), false);
            foreach (var elem in partialResults)
            {
                result = combiner(result, elem);
            }
            yield return result;
        }

        private static IEnumerable<TAccumulate>
            AggregateInner<TSource, TAccumulate>(IEnumerable<TSource> source,
                                                 Func<TAccumulate> seedFunc,
                                                 Func<TAccumulate, TSource, TAccumulate> aggregator)
        {
            TAccumulate result = seedFunc();
            foreach (TSource elem in source)
            {
                result = aggregator(result, elem);
            }
            yield return result;
        }

        // If the aggregate function is associative, we use the following two operators for
        // the final aggregation.
        public static TSource
            AssocAggregate<TSource>(IEnumerable<TSource> source,
                                    Func<TSource, TSource, TSource> combiner)
        {
            IEnumerable<TSource> result = AssocAggregate(source, combiner, combiner);
            using (IEnumerator<TSource> elems = result.GetEnumerator())
            {
                bool hasElem = elems.MoveNext();
                if (hasElem) return elems.Current;
                throw new DryadLinqException(DryadLinqErrorCode.AggregateNoElements, SR.AggregateNoElements);
            }
        }

        public static TResult
            AssocAggregate<TSource, TResult>(IEnumerable<TSource> source,
                                             Func<TSource, TSource, TSource> combiner,
                                             Func<TSource, TResult> resultSelector)
        {
            return resultSelector(AssocAggregate(source, combiner));
        }

        // Operator: First
        public static AggregateValue<TSource> First<TSource>(IEnumerable<TSource> source)
        {
            using (IEnumerator<TSource> e = source.GetEnumerator())
            {
                if (e.MoveNext())
                {
                    return new AggregateValue<TSource>(e.Current, 1);
                }
            }
            return new AggregateValue<TSource>(default(TSource), 0);
        }

        public static AggregateValue<TSource>
            First<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            foreach (TSource elem in source)
            {
                if (predicate(elem))
                {
                    return new AggregateValue<TSource>(elem, 1);
                }
            }
            return new AggregateValue<TSource>(default(TSource), 0);
        }

        public static TSource First<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            foreach (var elem in source)
            {
                if (elem.Count > 0) return elem.Value;
            }
            throw new DryadLinqException(DryadLinqErrorCode.FirstNoElementsFirst, SR.FirstNoElementsFirst);
        }

        // Operator: FirstOrDefault
        public static AggregateValue<TSource> FirstOrDefault<TSource>(IEnumerable<TSource> source)
        {
            return First(source);
        }

        public static AggregateValue<TSource>
            FirstOrDefault<TSource>(IEnumerable<TSource> source,
                                         Func<TSource, bool> predicate)
        {
            return First(source, predicate);
        }

        public static AggregateValue<TSource>
            FirstOrDefault<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            foreach (var elem in source)
            {
                if (elem.Count > 0) return elem;
            }
            return new AggregateValue<TSource>(default(TSource), 0);
        }
        
        // Operator: Single
        public static AggregateValue<TSource> Single<TSource>(IEnumerable<TSource> source)
        {
            using (IEnumerator<TSource> e = source.GetEnumerator())
            {
                if (!e.MoveNext())
                {
                    return new AggregateValue<TSource>(default(TSource), 0);
                }
                TSource val = e.Current;
                if (!e.MoveNext())
                {
                    return new AggregateValue<TSource>(val, 1);
                }
            }
            throw new DryadLinqException(DryadLinqErrorCode.SingleMoreThanOneElement,
                                         SR.SingleMoreThanOneElement);
        }

        public static AggregateValue<TSource>
            Single<TSource>(IEnumerable<TSource> source,
                                 Func<TSource, bool> predicate)
        {
            IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
            IEnumerable<AggregateValue<TSource>> partialResults;
            if (pipe == null)
            {
                partialResults = source.PApply(s => AsEnumerable(SingleInner(s, predicate)), false);
            }
            else
            {
                partialResults = pipe.Extend(s => AsEnumerable(SingleInner(s, predicate)), false);
            }

            TSource theValue = default(TSource);
            long count = 0;
            foreach (var elem in partialResults)
            {
                if (elem.Count > 0)
                {
                    if (count > 0)
                    {
                        new DryadLinqException(DryadLinqErrorCode.SingleMoreThanOneElement,
                                               SR.SingleMoreThanOneElement);
                    }
                    count = 1;
                    theValue = elem.Value;
                }
            }
            return new AggregateValue<TSource>(theValue, count);
        }

        private static AggregateValue<TSource>
            SingleInner<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            long count = 0;
            TSource theValue = default(TSource);
            foreach (TSource elem in source)
            {
                if (predicate(elem))
                {
                    if (count > 0)
                    {
                        new DryadLinqException(DryadLinqErrorCode.SingleMoreThanOneElement,
                                               SR.SingleMoreThanOneElement);
                    }
                    count = 1;
                    theValue = elem;
                }
            }
            return new AggregateValue<TSource>(theValue, count);
        }

        public static TSource Single<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            AggregateValue<TSource> result = new AggregateValue<TSource>(default(TSource), 0);
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (result.Count > 0)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.SingleMoreThanOneElement,
                                                     SR.SingleMoreThanOneElement);
                    }
                    result = elem;
                }
            }
            if (result.Count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.SingleNoElements, SR.SingleNoElements);
            }
            return result.Value;
        }

        // Operator: SingleOrDefault
        public static AggregateValue<TSource> SingleOrDefault<TSource>(IEnumerable<TSource> source)
        {
            return Single(source);
        }

        public static AggregateValue<TSource>
            SingleOrDefault<TSource>(IEnumerable<TSource> source,
                                          Func<TSource, bool> predicate)
        {
            return Single(source, predicate);
        }

        public static AggregateValue<TSource>
            SingleOrDefault<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            AggregateValue<TSource> result = new AggregateValue<TSource>(default(TSource), 0);
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (result.Count > 0)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.SingleMoreThanOneElement,
                                                     SR.SingleMoreThanOneElement);
                    }
                    result = elem;
                }
            }
            return result;
        }
        
        // Operator: Last
        public static AggregateValue<TSource> Last<TSource>(IEnumerable<TSource> source)
        {
            using (IEnumerator<TSource> e = source.GetEnumerator())
            {
                if (e.MoveNext())
                {
                    TSource result = e.Current;
                    while (e.MoveNext())
                    {
                        result = e.Current;
                    }
                    return new AggregateValue<TSource>(result, 1);
                }
            }
            return new AggregateValue<TSource>(default(TSource), 0);
        }

        public static AggregateValue<TSource>
            Last<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
            IEnumerable<AggregateValue<TSource>> partialResults;
            if (pipe == null)
            {
                partialResults = source.PApply(s => AsEnumerable(LastInner(s, predicate)), true);
            }
            else
            {
                partialResults = pipe.Extend(s => AsEnumerable(LastInner(s, predicate)), true);
            }

            TSource lastValue = default(TSource);
            long count = 0;
            foreach (var elem in partialResults)
            {
                if (elem.Count > 0)
                {
                    count = 1;
                    lastValue = elem.Value;
                }
            }
            return new AggregateValue<TSource>(lastValue, count);
        }

        private static AggregateValue<TSource>
            LastInner<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            long count = 0;
            TSource lastValue = default(TSource);
            foreach (TSource elem in source)
            {
                if (predicate(elem))
                {
                    count = 1;
                    lastValue = elem;
                }
            }
            return new AggregateValue<TSource>(lastValue, count);
        }

        public static TSource Last<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            AggregateValue<TSource> result = new AggregateValue<TSource>(default(TSource), 0);
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    result = elem;
                }
            }
            if (result.Count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.LastNoElements, SR.LastNoElements);
            }
            return result.Value;
        }

        // Operator: LastOrDefault
        public static AggregateValue<TSource> LastOrDefault<TSource>(IEnumerable<TSource> source)
        {
            return Last(source);
        }

        public static AggregateValue<TSource>
            LastOrDefault<TSource>(IEnumerable<TSource> source,
                                        Func<TSource, bool> predicate)
        {
            return Last(source, predicate);
        }

        public static AggregateValue<TSource>
            LastOrDefault<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            AggregateValue<TSource> result = new AggregateValue<TSource>(default(TSource), 0);
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    result = elem;
                }
            }
            return result;
        }

        // Operator: Sum        
        public static int Sum(IEnumerable<int> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int> pipe = source as IParallelPipeline<int>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                int sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (int item in partialResults)
                {
                    checked { sum += item; }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static int? Sum(IEnumerable<int?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int?> pipe = source as IParallelPipeline<int?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                int sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (int? item in partialResults)
                {
                    if (item != null)
                    {
                        checked { sum += item.GetValueOrDefault(); }
                    }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static long Sum(IEnumerable<long> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long> pipe = source as IParallelPipeline<long>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                long sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (long item in partialResults)
                {
                    checked { sum += item; }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source);
            }
        }

        public static long? Sum(IEnumerable<long?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long?> pipe = source as IParallelPipeline<long?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                long sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (long? item in partialResults)
                {
                    if (item != null)
                    {
                        checked { sum += item.GetValueOrDefault(); }
                    }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static float Sum(IEnumerable<float> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float> pipe = source as IParallelPipeline<float>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                float sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (float item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source);
            }
        }

        public static float? Sum(IEnumerable<float?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float?> pipe = source as IParallelPipeline<float?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                float sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (float? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static double Sum(IEnumerable<double> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double> pipe = source as IParallelPipeline<double>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                double sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (double item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source);
            }
        }

        public static double? Sum(IEnumerable<double?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double?> pipe = source as IParallelPipeline<double?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                double sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (double? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static decimal Sum(IEnumerable<decimal> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal> pipe = source as IParallelPipeline<decimal>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                decimal sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (decimal item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source);
            }
        }

        public static decimal? Sum(IEnumerable<decimal?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal?> pipe = source as IParallelPipeline<decimal?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Sum(source);
                }
                decimal sum = 0;
                var partialResults = pipe.Extend(x => AsEnumerable(x.Sum()), false);
                foreach (decimal? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return System.Linq.Enumerable.Sum(source);
            }
        }

        public static int Sum<TSource>(IEnumerable<TSource> source,
                                       Func<TSource, int> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                int sum = 0;
                foreach (int item in partialResults)
                {
                    checked { sum += item; }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static int? Sum<TSource>(IEnumerable<TSource> source,
                                        Func<TSource, int?> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                int sum = 0;
                foreach (int? item in partialResults)
                {
                    if (item != null)
                    {
                        checked { sum += item.GetValueOrDefault(); }
                    }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static long Sum<TSource>(IEnumerable<TSource> source,
                                        Func<TSource, long> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                long sum = 0;
                foreach (long item in partialResults)
                {
                    checked { sum += item; }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static long? Sum<TSource>(IEnumerable<TSource> source,
                                         Func<TSource, long?> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                long sum = 0;
                foreach (long? item in partialResults)
                {
                    if (item != null)
                    {
                        checked { sum += item.GetValueOrDefault(); }
                    }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static float Sum<TSource>(IEnumerable<TSource> source,
                                         Func<TSource, float> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                float sum = 0;
                foreach (float item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static float? Sum<TSource>(IEnumerable<TSource> source,
                                          Func<TSource, float?> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                float sum = 0;
                foreach (float? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static double Sum<TSource>(IEnumerable<TSource> source,
                                          Func<TSource, double> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                double sum = 0;
                foreach (double item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static double? Sum<TSource>(IEnumerable<TSource> source,
                                           Func<TSource, double?> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                double sum = 0;
                foreach (double? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static decimal Sum<TSource>(IEnumerable<TSource> source,
                                           Func<TSource, decimal> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                decimal sum = 0;
                foreach (decimal item in partialResults)
                {
                    sum += item;
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static decimal? Sum<TSource>(IEnumerable<TSource> source,
                                            Func<TSource, decimal?> selector)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Sum(selector)), false);
                decimal sum = 0;
                foreach (decimal? item in partialResults)
                {
                    if (item != null)
                    {
                        sum += item.GetValueOrDefault();
                    }
                }
                return sum;
            }
            else
            {
                return Enumerable.Sum(source, selector);
            }
        }

        public static int SumAccumulate(int a, int? x)
        {
            return (x == null) ? a : checked(a + x.GetValueOrDefault());
        }

        public static long SumAccumulate(long a, long? x)
        {
            return (x == null) ? a : checked(a + x.GetValueOrDefault());
        }
        
        public static float SumAccumulate(float a, float? x)
        {
            return (x == null) ? a : (a + x.GetValueOrDefault());
        }

        public static double SumAccumulate(double a, double? x)
        {
            return (x == null) ? a : (a + x.GetValueOrDefault());
        }

        public static decimal SumAccumulate(decimal a, decimal? x)
        {
            return (x == null) ? a : (a + x.GetValueOrDefault());
        }

        // Operator: Min
        public static AggregateValue<int> Min(IEnumerable<int> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int> pipe = source as IParallelPipeline<int>;
                if (pipe == null)
                {
                    return MinInner(source);
                }

                IEnumerable<AggregateValue<int>> partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                int value = Int32.MaxValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value < value) value = elem.Value;
                    count += elem.Count;
                }
                return new AggregateValue<int>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }

        private static AggregateValue<int> MinInner(IEnumerable<int> source)
        {
            int value = Int32.MaxValue;
            long count = 0;

            foreach (int elem in source)
            {
                if (elem < value) value = elem;
                count = 1;
            }
            return new AggregateValue<int>(value, count);
        }

        public static int Min(IEnumerable<AggregateValue<int>> source)
        {
            int value = Int32.MaxValue;
            bool hasValue = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    hasValue = true;
                }
            }
            if (!hasValue)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static int? Min(IEnumerable<int?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int?> pipe = source as IParallelPipeline<int?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Min(source);
                }

                IEnumerable<int?> partialResults = pipe.Extend(s => AsEnumerable(s.Min()), false);
                int? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem < value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Min(source);
            }
        }

        public static AggregateValue<long> Min(IEnumerable<long> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long> pipe = source as IParallelPipeline<long>;
                if (pipe == null)
                {
                    return MinInner(source);
                }

                IEnumerable<AggregateValue<long>> partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                long value = Int64.MaxValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    count = 1;
                }
                return new AggregateValue<long>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }

        private static AggregateValue<long> MinInner(IEnumerable<long> source)
        {
            long value = Int64.MaxValue;
            long count = 0;

            foreach (long elem in source)
            {
                if (elem < value) value = elem;
                count = 1;
            }
            return new AggregateValue<long>(value, count);
        }

        public static long Min(IEnumerable<AggregateValue<long>> source)
        {
            long value = Int64.MaxValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static long? Min(IEnumerable<long?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long?> pipe = source as IParallelPipeline<long?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Min(source);
                }

                IEnumerable<long?> partialResults = pipe.Extend(s => AsEnumerable(s.Min()), false);
                long? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem < value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Min(source);
            }
        }

        public static AggregateValue<float> Min(IEnumerable<float> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float> pipe = source as IParallelPipeline<float>;
                if (pipe == null)
                {
                    return MinInner(source);
                }

                IEnumerable<AggregateValue<float>> partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                float value = System.Single.MaxValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    count = 1;
                }
                return new AggregateValue<float>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }

        private static AggregateValue<float> MinInner(IEnumerable<float> source)
        {
            float value = System.Single.MaxValue;
            long count = 0;

            foreach (float elem in source)
            {
                if (elem < value) value = elem;
                count = 1;
            }
            return new AggregateValue<float>(value, count);
        }

        public static float Min(IEnumerable<AggregateValue<float>> source)
        {
            float value = System.Single.MaxValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static float? Min(IEnumerable<float?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float?> pipe = source as IParallelPipeline<float?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Min(source);
                }

                IEnumerable<float?> partialResults = pipe.Extend(s => AsEnumerable(s.Min()), false);
                float? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem < value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Min(source);
            }
        }

        public static AggregateValue<double> Min(IEnumerable<double> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double> pipe = source as IParallelPipeline<double>;
                if (pipe == null)
                {
                    return MinInner(source);
                }

                IEnumerable<AggregateValue<double>> partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                double value = Double.MaxValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    count = 1;
                }
                return new AggregateValue<double>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }

        private static AggregateValue<double> MinInner(IEnumerable<double> source)
        {
            double value = Double.MaxValue;
            long count = 0;

            foreach (double elem in source)
            {
                if (elem < value) value = elem;
                count = 1;
            }
            return new AggregateValue<double>(value, count);
        }

        public static double Min(IEnumerable<AggregateValue<double>> source)
        {
            double value = Double.MaxValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static double? Min(IEnumerable<double?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double?> pipe = source as IParallelPipeline<double?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Min(source);
                }

                IEnumerable<double?> partialResults = pipe.Extend(s => AsEnumerable(s.Min()), false);
                double? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem < value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Min(source);
            }
        }

        public static AggregateValue<decimal> Min(IEnumerable<decimal> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal> pipe = source as IParallelPipeline<decimal>;
                if (pipe == null)
                {
                    return MinInner(source);
                }

                IEnumerable<AggregateValue<decimal>> partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                decimal value = Decimal.MaxValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    count = 1;
                }
                return new AggregateValue<decimal>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }
        
        private static AggregateValue<decimal> MinInner(IEnumerable<decimal> source)
        {
            decimal value = Decimal.MaxValue;
            long count = 0;

            foreach (decimal x in source)
            {
                if (x < value) value = x;
                count = 1;
            }
            return new AggregateValue<decimal>(value, count);
        }

        public static decimal Min(IEnumerable<AggregateValue<decimal>> source)
        {
            decimal value = Decimal.MaxValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value < value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static decimal? Min(IEnumerable<decimal?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal?> pipe = source as IParallelPipeline<decimal?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Min(source);
                }

                IEnumerable<decimal?> partialResults = pipe.Extend(s => AsEnumerable(s.Min()), false);
                decimal? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem < value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Min(source);
            }
        }

        public static AggregateValue<TSource> Min<TSource>(IEnumerable<TSource> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                IEnumerable<AggregateValue<TSource>> partialResults;
                if (pipe == null)
                {
                    partialResults = source.PApply(s => AsEnumerable(MinInner(s)), false);
                }
                else
                {
                    partialResults = pipe.Extend(s => AsEnumerable(MinInner(s)), false);
                }

                IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
                TSource value = default(TSource);
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Count > 0)
                    {
                        if (count == 0 || comparer.Compare(elem.Value, value) < 0)
                        {
                            value = elem.Value;
                        }
                        count = 1;
                    }
                }
                return new AggregateValue<TSource>(value, count);
            }
            else
            {
                return MinInner(source);
            }
        }

        private static AggregateValue<TSource> MinInner<TSource>(IEnumerable<TSource> source)
        {
            IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
            TSource value = default(TSource);
            long count = 0;

            foreach (TSource x in source)
            {
                if (x != null)
                {
                    if (count == 0 || comparer.Compare(x, value) < 0)
                    {
                        value = x;
                        count = 1;
                    }
                }
            }
            return new AggregateValue<TSource>(value, count);
        }
        
        public static TSource Min<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
            TSource value = default(TSource);
            bool hasElem = false;

            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (!hasElem || comparer.Compare(elem.Value, value) < 0)
                    {
                        value = elem.Value;
                        hasElem = true;
                    }
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MinNoElements, SR.MinNoElements);
            }
            return value;
        }

        public static AggregateValue<int>
            Min<TSource>(IEnumerable<TSource> source, Func<TSource, int> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static int? Min<TSource>(IEnumerable<TSource> source,
                                             Func<TSource, int?> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static AggregateValue<long> Min<TSource>(IEnumerable<TSource> source,
                                                             Func<TSource, long> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static long? Min<TSource>(IEnumerable<TSource> source,
                                              Func<TSource, long?> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static AggregateValue<float> Min<TSource>(IEnumerable<TSource> source,
                                                              Func<TSource, float> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static float? Min<TSource>(IEnumerable<TSource> source,
                                               Func<TSource, float?> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static AggregateValue<double> Min<TSource>(IEnumerable<TSource> source,
                                                               Func<TSource, double> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static double? Min<TSource>(IEnumerable<TSource> source,
                                                Func<TSource, double?> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static AggregateValue<decimal> Min<TSource>(IEnumerable<TSource> source,
                                                                Func<TSource, decimal> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static decimal? Min<TSource>(IEnumerable<TSource> source,
                                                 Func<TSource, decimal?> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static AggregateValue<TResult> Min<TSource, TResult>(IEnumerable<TSource> source,
                                                                         Func<TSource, TResult> selector)
        {
            return Min(Select(source, selector, false));
        }

        public static int MinAccumulate(int a, int x)
        {
            return (x < a) ? x : a;
        }

        public static int? MinAccumulate(int? a, int? x)
        {
            return (a == null || x < a) ? x : a;
        }

        public static long MinAccumulate(long a, long x)
        {
            return (x < a) ? x : a;
        }

        public static long? MinAccumulate(long? a, long? x)
        {
            return (a == null || x < a) ? x : a;
        }

        public static float MinAccumulate(float a, float x)
        {
            return (x < a) ? x : a;
        }

        public static float? MinAccumulate(float? a, float? x)
        {
            return (a == null || x < a) ? x : a;
        }

        public static double MinAccumulate(double a, double x)
        {
            return (x < a) ? x : a;
        }

        public static double? MinAccumulate(double? a, double? x)
        {
            return (a == null || x < a) ? x : a;
        }

        public static decimal MinAccumulate(decimal a, decimal x)
        {
            return (x < a) ? x : a;
        }

        public static decimal? MinAccumulate(decimal? a, decimal? x)
        {
            return (a == null || x < a) ? x : a;
        }

        public static TSource MinAccumulateGeneric<TSource>(TSource a, TSource x)
        {
            return (x != null && (a == null || Comparer<TSource>.Default.Compare(x, a) < 0)) ? x : a;
        }

        // Operator: Max
        public static AggregateValue<int> Max(IEnumerable<int> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int> pipe = source as IParallelPipeline<int>;
                if (pipe == null)
                {
                    return MaxInner(source);
                }

                IEnumerable<AggregateValue<int>> partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                int value = Int32.MinValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value > value) value = elem.Value;
                    count = 1;
                }
                return new AggregateValue<int>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }

        private static AggregateValue<int> MaxInner(IEnumerable<int> source)
        {
            int value = Int32.MinValue;
            long count = 0;

            foreach (int elem in source)
            {
                if (elem > value) value = elem;
                count = 1;
            }
            return new AggregateValue<int>(value, count);
        }

        public static int Max(IEnumerable<AggregateValue<int>> source)
        {
            int value = Int32.MinValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value > value) value = elem.Value;
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static int? Max(IEnumerable<int?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int?> pipe = source as IParallelPipeline<int?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Max(source);
                }

                IEnumerable<int?> partialResults = pipe.Extend(s => AsEnumerable(s.Max()), false);
                int? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem > value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Max(source);
            }
        }

        public static AggregateValue<long> Max(IEnumerable<long> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long> pipe = source as IParallelPipeline<long>;
                if (pipe == null)
                {
                    return MaxInner(source);
                }

                IEnumerable<AggregateValue<long>> partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                long value = Int64.MinValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value > value) value = elem.Value;
                    count = 1;
                }
                return new AggregateValue<long>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }

        private static AggregateValue<long> MaxInner(IEnumerable<long> source)
        {
            long value = Int64.MinValue;
            long count = 0;

            foreach (long elem in source)
            {
                if (elem > value) value = elem;
                count = 1;
            }
            return new AggregateValue<long>(value, count);
        }

        public static long Max(IEnumerable<AggregateValue<long>> source)
        {
            long value = Int64.MinValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value > value) value = elem.Value;
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static long? Max(IEnumerable<long?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long?> pipe = source as IParallelPipeline<long?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Max(source);
                }

                IEnumerable<long?> partialResults = pipe.Extend(s => AsEnumerable(s.Max()), false);
                long? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem > value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Max(source);
            }
        }

        public static AggregateValue<double> Max(IEnumerable<double> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double> pipe = source as IParallelPipeline<double>;
                if (pipe == null)
                {
                    return MaxInner(source);
                }

                IEnumerable<AggregateValue<double>> partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                double value = Double.MinValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value > value) value = elem.Value;
                    count = 1;
                }
                return new AggregateValue<double>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }

        private static AggregateValue<double> MaxInner(IEnumerable<double> source)
        {
            double value = Double.MinValue;
            long count = 0;

            foreach (double elem in source)
            {
                if (elem > value) value = elem;
                count = 1;
            }
            return new AggregateValue<double>(value, count);
        }

        public static double Max(IEnumerable<AggregateValue<double>> source)
        {
            double value = Double.MinValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value > value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static double? Max(IEnumerable<double?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double?> pipe = source as IParallelPipeline<double?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Max(source);
                }

                IEnumerable<double?> partialResults = pipe.Extend(s => AsEnumerable(s.Max()), false);
                double? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem > value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Max(source);
            }
        }

        public static AggregateValue<float> Max(IEnumerable<float> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float> pipe = source as IParallelPipeline<float>;
                if (pipe == null)
                {
                    return MaxInner(source);
                }

                IEnumerable<AggregateValue<float>> partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                float value = System.Single.MinValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value > value) value = elem.Value;
                    count = 1;
                }
                return new AggregateValue<float>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }
        
        private static AggregateValue<float> MaxInner(IEnumerable<float> source)
        {
            float value = System.Single.MinValue;
            long count = 0;

            foreach (float x in source)
            {
                if (x > value) value = x;
                count = 1;
            }
            return new AggregateValue<float>(value, count);
        }

        public static float Max(IEnumerable<AggregateValue<float>> source)
        {
            float value = System.Single.MinValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value > value)
                    {
                        value = elem.Value;
                    }
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static float? Max(IEnumerable<float?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float?> pipe = source as IParallelPipeline<float?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Max(source);
                }

                IEnumerable<float?> partialResults = pipe.Extend(s => AsEnumerable(s.Max()), false);
                float? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem > value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Max(source);
            }
        }

        public static AggregateValue<decimal> Max(IEnumerable<decimal> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal> pipe = source as IParallelPipeline<decimal>;
                if (pipe == null)
                {
                    return MaxInner(source);
                }

                IEnumerable<AggregateValue<decimal>> partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                decimal value = Decimal.MinValue;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Value > value) value = elem.Value;
                    count = 1;
                }
                return new AggregateValue<decimal>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }

        private static AggregateValue<decimal> MaxInner(IEnumerable<decimal> source)
        {
            decimal value = Decimal.MinValue;
            long count = 0;

            foreach (decimal x in source)
            {
                if (x > value) value = x;
                count = 1;
            }
            return new AggregateValue<decimal>(value, count);
        }

        public static decimal Max(IEnumerable<AggregateValue<decimal>> source)
        {
            decimal value = Decimal.MinValue;
            bool hasElem = false;
            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (elem.Value > value) value = elem.Value;
                    hasElem = true;
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static decimal? Max(IEnumerable<decimal?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal?> pipe = source as IParallelPipeline<decimal?>;
                if (pipe == null)
                {
                    return System.Linq.Enumerable.Max(source);
                }

                IEnumerable<decimal?> partialResults = pipe.Extend(s => AsEnumerable(s.Max()), false);
                decimal? value = null;
                foreach (var elem in partialResults)
                {
                    if (value == null || elem > value)
                    {
                        value = elem;
                    }
                }
                return value;
            }
            else
            {
                return System.Linq.Enumerable.Max(source);
            }
        }

        public static AggregateValue<TSource> Max<TSource>(IEnumerable<TSource> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<TSource> pipe = source as IParallelPipeline<TSource>;
                IEnumerable<AggregateValue<TSource>> partialResults;
                if (pipe == null)
                {
                    partialResults = source.PApply(s => AsEnumerable(MaxInner(s)), false);
                }
                else
                {
                    partialResults = pipe.Extend(s => AsEnumerable(MaxInner(s)), false);
                }

                IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
                TSource value = default(TSource);
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Count > 0)
                    {
                        if (count == 0 || comparer.Compare(elem.Value, value) > 0)
                        {
                            value = elem.Value;
                        }
                        count = 1;
                    }
                }
                return new AggregateValue<TSource>(value, count);
            }
            else
            {
                return MaxInner(source);
            }
        }

        private static AggregateValue<TSource> MaxInner<TSource>(IEnumerable<TSource> source)
        {
            IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
            TSource value = default(TSource);
            long count = 0;

            foreach (TSource x in source)
            {
                if (x != null)
                {
                    if (count == 0 || comparer.Compare(x, value) > 0)
                    {
                        value = x;
                        count = 1;
                    }
                }
            }
            return new AggregateValue<TSource>(value, count);
        }
        
        public static TSource Max<TSource>(IEnumerable<AggregateValue<TSource>> source)
        {
            IComparer<TSource> comparer = TypeSystem.GetComparer<TSource>(null);
            TSource value = default(TSource);
            bool hasElem = false;

            foreach (var elem in source)
            {
                if (elem.Count > 0)
                {
                    if (!hasElem || comparer.Compare(elem.Value, value) > 0)
                    {
                        value = elem.Value;
                        hasElem = true;
                    }
                }
            }
            if (!hasElem)
            {
                throw new DryadLinqException(DryadLinqErrorCode.MaxNoElements, SR.MaxNoElements);
            }
            return value;
        }

        public static AggregateValue<int> Max<TSource>(IEnumerable<TSource> source,
                                                            Func<TSource, int> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static int? Max<TSource>(IEnumerable<TSource> source,
                                             Func<TSource, int?> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static AggregateValue<long> Max<TSource>(IEnumerable<TSource> source,
                                                             Func<TSource, long> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static long? Max<TSource>(IEnumerable<TSource> source,
                                              Func<TSource, long?> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static AggregateValue<float> Max<TSource>(IEnumerable<TSource> source,
                                                              Func<TSource, float> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static float? Max<TSource>(IEnumerable<TSource> source,
                                               Func<TSource, float?> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static AggregateValue<double> Max<TSource>(IEnumerable<TSource> source,
                                                               Func<TSource, double> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static double? Max<TSource>(IEnumerable<TSource> source,
                                                Func<TSource, double?> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static AggregateValue<decimal> Max<TSource>(IEnumerable<TSource> source,
                                                                Func<TSource, decimal> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static decimal? Max<TSource>(IEnumerable<TSource> source,
                                                 Func<TSource, decimal?> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static AggregateValue<TResult> Max<TSource, TResult>(IEnumerable<TSource> source,
                                                                         Func<TSource, TResult> selector)
        {
            return Max(Select(source, selector, false));
        }

        public static int? MaxAccumulate(int? a, int? x)
        {
            return (a == null || x > a) ? x : a;
        }

        public static int MaxAccumulate(int a, int x)
        {
            return (x > a) ? x : a;
        }

        public static long MaxAccumulate(long a, long x)
        {
            return (x > a) ? x : a;
        }

        public static long? MaxAccumulate(long? a, long? x)
        {
            return (a == null || x > a) ? x : a;
        }

        public static float MaxAccumulate(float a, float x)
        {
            return (x > a) ? x : a;
        }
        
        public static float? MaxAccumulate(float? a, float? x)
        {
            return (a == null || x > a) ? x : a;
        }

        public static double MaxAccumulate(double a, double x)
        {
            return (x > a) ? x : a;
        }

        public static double? MaxAccumulate(double? a, double? x)
        {
            return (a == null || x > a) ? x : a;
        }

        public static decimal MaxAccumulate(decimal a, decimal x)
        {
            return (x > a) ? x : a;
        }

        public static decimal? MaxAccumulate(decimal? a, decimal? x)
        {
            return (a == null || x > a) ? x : a;
        }

        public static TSource MaxAccumulateGeneric<TSource>(TSource a, TSource x)
        {
            return (x != null && (a == null || Comparer<TSource>.Default.Compare(x, a) > 0)) ? x : a;
        }

        // Operator: Average        
        public static AggregateValue<long> Average(IEnumerable<int> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int> pipe = source as IParallelPipeline<int>;
                if (pipe == null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<long>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                long sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    checked {
                        sum += elem.Value;
                        count += elem.Count;
                    }
                }
                return new AggregateValue<long>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<long> AverageInner(IEnumerable<int> source)
        {
            long sum = 0;
            long count = 0;
            foreach (int x in source)
            {
                checked {
                    sum += x;
                    count++;
                }
            }
            return new AggregateValue<long>(sum, count);
        }

        public static double Average(IEnumerable<AggregateValue<long>> source)
        {
            long sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                checked {
                    sum += x.Value;
                    count += x.Count;
                }
            }

            if (count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AverageNoElements, SR.AverageNoElements);
            }
            return (double)sum / count;
        }

        public static AggregateValue<long?> Average(IEnumerable<int?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<int?> pipe = source as IParallelPipeline<int?>;
                if (pipe != null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<long?>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                long sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    checked {
                        sum += elem.Value.GetValueOrDefault();
                        count += elem.Count;
                    }
                }
                return new AggregateValue<long?>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<long?> AverageInner(IEnumerable<int?> source)
        {
            long sum = 0;
            long count = 0;
            foreach (int? x in source)
            {
                if (x != null)
                {
                    checked {
                        sum += x.GetValueOrDefault();
                        count++;
                    }
                }
            }
            return new AggregateValue<long?>(sum, count);
        }

        public static double? Average(IEnumerable<AggregateValue<long?>> source)
        {
            long sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                checked {
                    sum += x.Value.GetValueOrDefault();
                    count += x.Count;
                }
            }

            if (count == 0) return null;
            return (double)sum / count;
        }
        
        public static AggregateValue<long> Average(IEnumerable<long> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long> pipe = source as IParallelPipeline<long>;
                if (pipe == null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<long>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                long sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    checked {
                        sum += elem.Value;
                        count += elem.Count;
                    }
                }
                return new AggregateValue<long>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<long> AverageInner(IEnumerable<long> source)
        {
            long sum = 0;
            long count = 0;
            foreach (long x in source)
            {
                checked {
                    sum += x;
                    count++;
                }
            }
            return new AggregateValue<long>(sum, count);
        }
        
        public static AggregateValue<long?> Average(IEnumerable<long?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<long?> pipe = source as IParallelPipeline<long?>;
                if (pipe != null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<long?>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                long sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    checked {
                        sum += elem.Value.GetValueOrDefault();
                        count += elem.Count;
                    }
                }
                return new AggregateValue<long?>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }            
        }

        private static AggregateValue<long?> AverageInner(IEnumerable<long?> source)
        {
            long sum = 0;
            long count = 0;
            foreach (long? x in source)
            {
                if (x != null)
                {
                    checked {
                        sum += x.GetValueOrDefault();
                        count++;
                    }
                }
            }
            return new AggregateValue<long?>(sum, count);
        }

        public static AggregateValue<double> Average(IEnumerable<float> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float> pipe = source as IParallelPipeline<float>;
                if (pipe == null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<double>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                double sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    sum += elem.Value;
                    checked { count += elem.Count; }
                }
                return new AggregateValue<double>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }
        
        private static AggregateValue<double> AverageInner(IEnumerable<float> source)
        {
            double sum = 0;
            long count = 0;
            foreach (float v in source)
            {
                sum += v;
                checked { count++; }
            }
            return new AggregateValue<double>(sum, count);
        }

        public static AggregateValue<double?> Average(IEnumerable<float?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<float?> pipe = source as IParallelPipeline<float?>;
                if (pipe != null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<double?>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                double sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    if (elem.Count != 0)
                    {
                        sum += elem.Value.GetValueOrDefault();
                        checked { count += elem.Count; }
                    }
                }
                return new AggregateValue<double?>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }
        
        private static AggregateValue<double?> AverageInner(IEnumerable<float?> source)
        {
            double sum = 0;
            long count = 0;
            foreach (float? x in source)
            {
                if (x != null)
                {
                    sum += x.GetValueOrDefault();
                    checked { count++; }
                }
            }
            return new AggregateValue<double?>(sum, count);
        }

        public static AggregateValue<double> Average(IEnumerable<double> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double> pipe = source as IParallelPipeline<double>;
                if (pipe == null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<double>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                double sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    sum += elem.Value;
                    checked { count += elem.Count; }
                }
                return new AggregateValue<double>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<double> AverageInner(IEnumerable<double> source)
        {
            double sum = 0;
            long count = 0;
            foreach (double x in source)
            {
                sum += x;
                checked { count++; }
            }
            return new AggregateValue<double>(sum, count);
        }

        public static double Average(IEnumerable<AggregateValue<double>> source)
        {
            double sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                sum += x.Value;
                checked { count += x.Count; }
            }

            if (count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AverageNoElements, SR.AverageNoElements);
            }
            return sum / count;
        }

        public static AggregateValue<double?> Average(IEnumerable<double?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<double?> pipe = source as IParallelPipeline<double?>;
                if (pipe != null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<double?>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                double sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    sum += elem.Value.GetValueOrDefault();
                    checked { count += elem.Count; }
                }
                return new AggregateValue<double?>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }
        
        private static AggregateValue<double?> AverageInner(IEnumerable<double?> source)
        {
            double sum = 0;
            long count = 0;
            foreach (double? x in source)
            {
                if (x != null)
                {
                    sum += x.GetValueOrDefault();
                    checked { count++; }
                }
            }
            return new AggregateValue<double?>(sum, count);
        }

        public static double? Average(IEnumerable<AggregateValue<double?>> source)
        {
            double sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                sum += x.Value.GetValueOrDefault();
                checked { count += x.Count; }
            }

            if (count == 0) return null;
            return sum / count;
        }

        public static AggregateValue<decimal> Average(IEnumerable<decimal> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal> pipe = source as IParallelPipeline<decimal>;
                if (pipe == null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<decimal>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                decimal sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    sum += elem.Value;
                    checked { count += elem.Count; }
                }
                return new AggregateValue<decimal>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<decimal> AverageInner(IEnumerable<decimal> source)
        {
            decimal sum = 0;
            long count = 0;
            foreach (decimal x in source)
            {
                sum += x;
                checked { count++; }
            }
            return new AggregateValue<decimal>(sum, count);
        }

        public static decimal Average(IEnumerable<AggregateValue<decimal>> source)
        {
            decimal sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                sum += x.Value;
                checked { count += x.Count; }
            }

            if (count == 0)
            {
                throw new DryadLinqException(DryadLinqErrorCode.AverageNoElements, SR.AverageNoElements);
            }
            return sum / count;
        }

        public static AggregateValue<decimal?> Average(IEnumerable<decimal?> source)
        {
            if (s_multiThreading)
            {
                IParallelPipeline<decimal?> pipe = source as IParallelPipeline<decimal?>;
                if (pipe != null)
                {
                    return AverageInner(source);
                }

                IEnumerable<AggregateValue<decimal?>> partialResults = pipe.Extend(s => AsEnumerable(AverageInner(s)), false);
                decimal sum = 0;
                long count = 0;
                foreach (var elem in partialResults)
                {
                    sum += elem.Value.GetValueOrDefault();
                    checked { count += elem.Count; }
                }
                return new AggregateValue<decimal?>(sum, count);
            }
            else
            {
                return AverageInner(source);
            }
        }

        private static AggregateValue<decimal?> AverageInner(IEnumerable<decimal?> source)
        {
            decimal sum = 0;
            long count = 0;
            foreach (decimal? x in source)
            {
                if (x != null)
                {
                    sum += x.GetValueOrDefault();
                    checked { count++; }
                }
            }
            return new AggregateValue<decimal?>(sum, count);
        }

        public static decimal? Average(IEnumerable<AggregateValue<decimal?>> source)
        {
            decimal sum = 0;
            long count = 0;
            foreach (var x in source)
            {
                sum += x.Value.GetValueOrDefault();
                checked { count += x.Count; }
            }

            if (count == 0) return null;
            return sum / count;
        }
        
        public static AggregateValue<long> Average<TSource>(IEnumerable<TSource> source,
                                                            Func<TSource, int> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<long?> Average<TSource>(IEnumerable<TSource> source,
                                                             Func<TSource, int?> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<long> Average<TSource>(IEnumerable<TSource> source,
                                                            Func<TSource, long> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<long?> Average<TSource>(IEnumerable<TSource> source,
                                                             Func<TSource, long?> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<double> Average<TSource>(IEnumerable<TSource> source,
                                                              Func<TSource, float> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<double?> Average<TSource>(IEnumerable<TSource> source,
                                                               Func<TSource, float?> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<double> Average<TSource>(IEnumerable<TSource> source,
                                                              Func<TSource, double> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<double?> Average<TSource>(IEnumerable<TSource> source,
                                                               Func<TSource, double?> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<decimal> Average<TSource>(IEnumerable<TSource> source,
                                                               Func<TSource, decimal> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<decimal?> Average<TSource>(IEnumerable<TSource> source,
                                                                Func<TSource, decimal?> selector)
        {
            return Average(Select(source, selector, false));
        }

        public static AggregateValue<long> AverageAccumulate(AggregateValue<long> a, int x)
        {
            return new AggregateValue<long>(checked(a.Value + x), checked(a.Count + 1));
        }

        public static AggregateValue<long> AverageAccumulate(AggregateValue<long> a, int? x)
        {
            if (x == null) return a;
            return new AggregateValue<long>(checked(a.Value + x.GetValueOrDefault()),
                                            checked(a.Count + 1));
        }
        
        public static AggregateValue<long> AverageAccumulate(AggregateValue<long> a, long x)
        {
            return new AggregateValue<long>(checked(a.Value + x), checked(a.Count + 1));
        }

        public static AggregateValue<long> AverageAccumulate(AggregateValue<long> a, long? x)
        {
            if (x == null) return a;
            return new AggregateValue<long>(checked(a.Value + x.GetValueOrDefault()),
                                            checked(a.Count + 1));
        }

        public static AggregateValue<double> AverageAccumulate(AggregateValue<double> a, float x)
        {
            return new AggregateValue<double>(a.Value + x,
                                              checked(a.Count + 1));
        }

        public static AggregateValue<double> AverageAccumulate(AggregateValue<double> a, float? x)
        {
            if (x == null) return a;
            return new AggregateValue<double>(a.Value + x.GetValueOrDefault(),
                                              checked(a.Count + 1));
        }

        public static AggregateValue<double> AverageAccumulate(AggregateValue<double> a, double x)
        {
            return new AggregateValue<double>(a.Value + x,
                                              checked(a.Count + 1));
        }

        public static AggregateValue<double> AverageAccumulate(AggregateValue<double> a, double? x)
        {
            if (x == null) return a;
            return new AggregateValue<double>(a.Value + x.GetValueOrDefault(),
                                              checked(a.Count + 1));
        }

        public static AggregateValue<decimal> AverageAccumulate(AggregateValue<decimal> a, decimal x)
        {
            return new AggregateValue<decimal>(a.Value + x,
                                               checked(a.Count + 1));
        }

        public static AggregateValue<decimal> AverageAccumulate(AggregateValue<decimal> a, decimal? x)
        {
            if (x == null) return a;
            return new AggregateValue<decimal>(a.Value + x.GetValueOrDefault(),
                                               checked(a.Count + 1));
        }
        
        // Operator: Any        
        public static bool Any<TSource>(IEnumerable<TSource> source,
                                             Func<TSource, bool> predicate)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.Any(predicate)), false);
                foreach (bool item in partialResults)
                {
                    if (item) return true;
                }
                return false;
            }
            else
            {
                return Enumerable.Any(source, predicate);
            }
        }

        public static bool Any(IEnumerable<bool> source)
        {
            foreach (bool item in source)
            {
                if (item) return true;
            }
            return false;
        }

        // Operator: All        
        public static bool All<TSource>(IEnumerable<TSource> source,
                                             Func<TSource, bool> predicate)
        {
            if (s_multiThreading)
            {
                var partialResults = source.ExtendParallelPipeline(x => AsEnumerable(x.All(predicate)), false);
                foreach (bool item in partialResults)
                {
                    if (!item) return false;
                }
                return true;
            }
            else
            {
                return Enumerable.All(source, predicate);
            }
        }

        public static bool All(IEnumerable<bool> source)
        {
            foreach (bool item in source)
            {
                if (!item) return false;
            }
            return true;
        }

        // Operator: Reverse
        public static IEnumerable<TSource> Reverse<TSource>(IEnumerable<TSource> source)
        {
            BigCollection<TSource> buffer = new BigCollection<TSource>();
            foreach (var elem in source)
            {
                buffer.Add(elem);
            }
            return buffer.Reverse();
        }

        // Operator: Merge
        // When not pipelined, this should be implemented in native  for better performance.
        public static IEnumerable<TSource> Merge<TSource>(IEnumerable<TSource> source)
        {
            return source;
        }

        // Operator: Apply
        public static IEnumerable<TResult>
            Apply<TSource, TResult>(IEnumerable<TSource> source,
                                    Func<IEnumerable<TSource>, IEnumerable<TResult>> procFunc)
        {
            return procFunc(source);
        }

        public static IEnumerable<TResult>
            Apply<TSource1, TSource2, TResult>(IEnumerable<TSource1> source1,
                                               IEnumerable<TSource2> source2,
                                               Func<IEnumerable<TSource1>, IEnumerable<TSource2>, IEnumerable<TResult>> procFunc)
        {
            return procFunc(source1, source2);
        }

        public static IEnumerable<TSource>
            Apply<TSource>(IEnumerable<TSource>[] sources, 
                           Func<IEnumerable<TSource>[], IEnumerable<TSource>> procFunc)
        {
            return procFunc(sources);
        }

        public static IEnumerable<TResult>
            Apply<TSource, TResult>(IEnumerable<TSource> source,
                                    IEnumerable[] otherSources,
                                    Func<IEnumerable<TSource>, IEnumerable[], IEnumerable<TResult>> procFunc)
        {
            return procFunc(source, otherSources);
        }

        public static IEnumerable<TResult>
            PApply<TSource, TResult>(this IEnumerable<TSource> source,
                                     Func<IEnumerable<TSource>, IEnumerable<TResult>> procFunc,
                                     bool orderPreserving)
        {
            return source.ExtendParallelPipeline(procFunc, orderPreserving);
        }

        // Operator: HashPartition
        public static void
            HashPartition<TSource>(IEnumerable<TSource> source,
                                   IEqualityComparer<TSource> comparer,
                                   DryadLinqVertexWriter<TSource> sink)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TSource>.Default;
            }

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            foreach (TSource item in source)
            {
                Int32 hashCode = comparer.GetHashCode(item) & 0x7FFFFFFF;
                Int32 portNum = hashCode % numOfPorts;
                sink.WriteItem(item, portNum);
            }
            sink.CloseWriters();
        }

        public static void
            HashPartition<TSource, TResult>(IEnumerable<TSource> source,
                                            IEqualityComparer<TSource> comparer,
                                            Func<TSource, TResult> resultSelector,
                                            DryadLinqVertexWriter<TResult> sink)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TSource>.Default;
            }

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            foreach (TSource item in source)
            {
                Int32 hashCode = comparer.GetHashCode(item) & 0x7FFFFFFF;
                Int32 portNum = hashCode % numOfPorts;
                sink.WriteItem(resultSelector(item), portNum);
            }
            sink.CloseWriters();
        }

        public static void
            HashPartition<TSource, TKey>(IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         bool isExpensive,
                                         IEqualityComparer<TKey> comparer,
                                         DryadLinqVertexWriter<TSource> sink)
        {
            if (s_multiThreading && isExpensive)
            {
                var source1 = source.ExtendParallelPipeline(s => s.Select(x => new Pair<TKey, TSource>(keySelector(x), x)), true);
                HashPartition(source1, x => x.Key, comparer, x => x.Value, sink);
            }
            else
            {
                HashPartition(source, keySelector, comparer, sink);
            }
        }

        private static void
            HashPartition<TSource, TKey>(IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         IEqualityComparer<TKey> comparer,
                                         DryadLinqVertexWriter<TSource> sink)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            foreach (TSource item in source)
            {
                Int32 hashCode = comparer.GetHashCode(keySelector(item)) & 0x7FFFFFFF;
                Int32 portNum = hashCode % numOfPorts;
                sink.WriteItem(item, portNum);
            }
            sink.CloseWriters();
        }

        public static void
            HashPartition<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                  Func<TSource, TKey> keySelector,
                                                  bool isExpensive,
                                                  IEqualityComparer<TKey> comparer,
                                                  Func<TSource, TResult> resultSelector,
                                                  DryadLinqVertexWriter<TResult> sink)
        {
            if (s_multiThreading && isExpensive)
            {
                var source1 = source.ExtendParallelPipeline(s => s.Select(x => new Pair<TKey, TResult>(keySelector(x), resultSelector(x))), true);
                HashPartition(source1, x => x.Key, comparer, x => x.Value, sink);
            }
            else
            {
                HashPartition(source, keySelector, comparer, resultSelector, sink);                
            }
        }

        private static void
            HashPartition<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                  Func<TSource, TKey> keySelector,
                                                  IEqualityComparer<TKey> comparer,
                                                  Func<TSource, TResult> resultSelector,
                                                  DryadLinqVertexWriter<TResult> sink)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            foreach (TSource item in source)
            {
                Int32 hashCode = comparer.GetHashCode(keySelector(item)) & 0x7FFFFFFF;
                Int32 portNum = hashCode % numOfPorts;
                sink.WriteItem(resultSelector(item), portNum);
            }
            sink.CloseWriters();
        }
        
        // Operator: RangePartition
        // special case for keySelector=identityFunction
        public static void
            RangePartition<TSource>(IEnumerable<TSource> source,
                                    IEnumerable<TSource> partitionKeys,
                                    IComparer<TSource> comparer,
                                    bool isDescending,
                                    DryadLinqVertexWriter<TSource> sink)
        {
            if (partitionKeys == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionKeysMissing,
                                             SR.RangePartitionKeysMissing);
            }
            comparer = TypeSystem.GetComparer<TSource>(comparer);
            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            TSource[] keys = new TSource[numOfPorts - 1];
            int idx = 0;
            foreach (TSource key in partitionKeys)
            {
                if (idx < keys.Length)
                {
                    keys[idx] = key;
                }
                idx++;
            }
            if (idx > keys.Length)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionInputOutputMismatch,
                                             String.Format(SR.RangePartitionInputOutputMismatch,
                                                           idx, numOfPorts));
            }
            if (idx < keys.Length)
            {
                TSource[] keys1 = new TSource[idx];
                Array.Copy(keys, keys1, idx);
                keys = keys1;
            }
            if (s_multiThreading)
            {
                var source1 = source.ExtendParallelPipeline(
                                    s => s.Select(x => new Pair<int, TSource>(DryadLinqUtil.BinarySearch(keys, x, comparer, isDescending), x)), false);
                foreach (var item in source1)
                {
                    sink.WriteItem(item.Value, item.Key);
                }
            }
            else
            {
                foreach (TSource item in source)
                {
                    int portNum = DryadLinqUtil.BinarySearch(keys, item, comparer, isDescending);
                    sink.WriteItem(item, portNum);
                }
            }
            sink.CloseWriters();
        }

        // special case for keySelector=identityFunction and resultSelector
        public static void
            RangePartition<TSource, TResult>(IEnumerable<TSource> source,
                                             IEnumerable<TSource> partitionKeys,
                                             IComparer<TSource> comparer,
                                             bool isDescending,
                                             Func<TSource, TResult> resultSelector,
                                             DryadLinqVertexWriter<TResult> sink)
        {
            if (partitionKeys == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionKeysMissing,
                                             SR.RangePartitionKeysMissing);
            }
            comparer = TypeSystem.GetComparer<TSource>(comparer);
            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            TSource[] keys = new TSource[numOfPorts - 1];
            int idx = 0;
            foreach (TSource key in partitionKeys)
            {
                if (idx < keys.Length)
                {
                    keys[idx] = key;
                }
                idx++;
            }
            if (idx > keys.Length)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionInputOutputMismatch,
                                             String.Format(SR.RangePartitionInputOutputMismatch,
                                                           idx, numOfPorts));
            }
            if (idx < keys.Length)
            {
                TSource[] keys1 = new TSource[idx];
                Array.Copy(keys, keys1, idx);
                keys = keys1;
            }
            if (s_multiThreading)
            {
                var source1 = source.ExtendParallelPipeline(
                                    s => s.Select(x => new Pair<int, TResult>(
                                                         DryadLinqUtil.BinarySearch(keys, x, comparer, isDescending),
                                                         resultSelector(x))), 
                                    false);
                foreach (var item in source1)
                {
                    sink.WriteItem(item.Value, item.Key);
                }
            }
            else
            {
                foreach (TSource item in source)
                {
                    int portNum = DryadLinqUtil.BinarySearch(keys, item, comparer, isDescending);
                    sink.WriteItem(resultSelector(item), portNum);
                }
            }
            sink.CloseWriters();
        }

        // general case
        public static void
            RangePartition<TSource, TKey>(IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          IEnumerable<TKey> partitionKeys,
                                          IComparer<TKey> comparer,
                                          bool isDescending,
                                          DryadLinqVertexWriter<TSource> sink)
        {
            if (partitionKeys == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionKeysMissing,
                                             SR.RangePartitionKeysMissing);
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            TKey[] keys = new TKey[numOfPorts - 1];
            int idx = 0;
            foreach (TKey key in partitionKeys)
            {
                if (idx < keys.Length)
                {
                    keys[idx] = key;
                }
                idx++;
            }
            if (idx > keys.Length)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionInputOutputMismatch,
                                             String.Format(SR.RangePartitionInputOutputMismatch,
                                                           idx, numOfPorts));
            }
            if (idx < keys.Length)
            {
                TKey[] keys1 = new TKey[idx];
                Array.Copy(keys, keys1, idx);
                keys = keys1;
            }
            if (s_multiThreading)
            {
                var source1 = source.ExtendParallelPipeline(
                                    s => s.Select(x => new Pair<int, TSource>(
                                                         DryadLinqUtil.BinarySearch(keys, keySelector(x), comparer, isDescending),
                                                         x)),
                                    false);
                foreach (var item in source1)
                {
                    sink.WriteItem(item.Value, item.Key);
                }
            }
            else
            {
                foreach (TSource item in source)
                {
                    int portNum = DryadLinqUtil.BinarySearch(keys, keySelector(item), comparer, isDescending);
                    sink.WriteItem(item, portNum);
                }
            }
            sink.CloseWriters();
        }

        // general case with result-selector
        public static void
            RangePartition<TSource, TKey, TResult>(IEnumerable<TSource> source,
                                                   Func<TSource, TKey> keySelector,
                                                   IEnumerable<TKey> partitionKeys,
                                                   IComparer<TKey> comparer,
                                                   bool isDescending,
                                                   Func<TSource, TResult> resultSelector,
                                                   DryadLinqVertexWriter<TResult> sink)
        {
            if (partitionKeys == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionKeysMissing,
                                             SR.RangePartitionKeysMissing);
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            Int32 numOfPorts = (Int32)sink.NumberOfOutputs;
            TKey[] keys = new TKey[numOfPorts - 1];
            int idx = 0;
            foreach (TKey key in partitionKeys)
            {
                if (idx < keys.Length)
                {
                    keys[idx] = key;
                }
                idx++;
            }
            if (idx > keys.Length)
            {
                throw new DryadLinqException(DryadLinqErrorCode.RangePartitionInputOutputMismatch,
                                             String.Format(SR.RangePartitionInputOutputMismatch,
                                                           idx, numOfPorts));
            }
            if (idx < keys.Length)
            {
                TKey[] keys1 = new TKey[idx];
                Array.Copy(keys, keys1, idx);
                keys = keys1;
            }
            if (s_multiThreading)
            {
                var source1 = source.ExtendParallelPipeline(
                                    s => s.Select(x => new Pair<int, TResult>(
                                                         DryadLinqUtil.BinarySearch(keys, keySelector(x), comparer, isDescending), 
                                                         resultSelector(x))),
                                    false);
                foreach (var item in source1)
                {
                    sink.WriteItem(item.Value, item.Key);
                }
            }
            else
            {
                foreach (TSource item in source)
                {
                    int portNum = DryadLinqUtil.BinarySearch(keys, keySelector(item), comparer, isDescending);
                    sink.WriteItem(resultSelector(item), portNum);
                }
            }
            sink.CloseWriters();
        }

        // Operator: Fork
        public static void Fork<T, R1, R2>(IEnumerable<T> source,
                                           Func<IEnumerable<T>, IEnumerable<ForkTuple<R1, R2>>> mapper,
                                           bool orderPreserving,
                                           DryadLinqVertexWriter<R1> sink1,
                                           DryadLinqVertexWriter<R2> sink2)
        {
            DryadLinqRecordWriter<R1> writer1 = sink1.GetWriter(0);
            DryadLinqRecordWriter<R2> writer2 = sink2.GetWriter(0);
        
            IEnumerable<ForkTuple<R1, R2>> result = mapper(source);
            foreach (ForkTuple<R1, R2> val in result)
            {
                if (val.HasFirst)
                {
                    writer1.WriteRecordAsync(val.First);
                }
                if (val.HasSecond)
                {
                    writer2.WriteRecordAsync(val.Second);
                }
            }
            sink1.CloseWriters();
            sink2.CloseWriters();
        }
        
        public static void Fork<T, R1, R2, R3>(IEnumerable<T> source,
                                               Func<IEnumerable<T>, IEnumerable<ForkTuple<R1, R2, R3>>> mapper,
                                               bool orderPreserving,
                                               DryadLinqVertexWriter<R1> sink1,
                                               DryadLinqVertexWriter<R2> sink2,
                                               DryadLinqVertexWriter<R3> sink3)
        {
            DryadLinqRecordWriter<R1> writer1 = sink1.GetWriter(0);
            DryadLinqRecordWriter<R2> writer2 = sink2.GetWriter(0);
            DryadLinqRecordWriter<R3> writer3 = sink3.GetWriter(0);
        
            IEnumerable<ForkTuple<R1, R2, R3>> result = mapper(source);
            foreach (ForkTuple<R1, R2, R3> val in result)
            {
                if (val.HasFirst)
                {
                    writer1.WriteRecordAsync(val.First);
                }
                if (val.HasSecond)
                {
                    writer2.WriteRecordAsync(val.Second);
                }
                if (val.HasThird)
                {
                    writer3.WriteRecordAsync(val.Third);
                }
            }
            sink1.CloseWriters();
            sink2.CloseWriters();
            sink3.CloseWriters();
        }
        
        public static void Fork<T, R1, R2>(IEnumerable<T> source,
                                           Func<T, ForkTuple<R1, R2>> mapper,
                                           bool orderPreserving,
                                           DryadLinqVertexWriter<R1> sink1,
                                           DryadLinqVertexWriter<R2> sink2)
        {
            DryadLinqRecordWriter<R1> writer1 = sink1.GetWriter(0);
            DryadLinqRecordWriter<R2> writer2 = sink2.GetWriter(0);
        
            IEnumerable<ForkTuple<R1, R2>>
                result = source.ExtendParallelPipeline(s => s.Select(mapper), orderPreserving);
        
            foreach (ForkTuple<R1, R2> val in result)
            {
                if (val.HasFirst)
                {
                    writer1.WriteRecordAsync(val.First);
                }
                if (val.HasSecond)
                {
                    writer2.WriteRecordAsync(val.Second);
                }
            }
            sink1.CloseWriters();
            sink2.CloseWriters();
        }
        
        public static void Fork<T, R1, R2, R3>(IEnumerable<T> source,
                                               Func<T, ForkTuple<R1, R2, R3>> mapper,
                                               bool orderPreserving,
                                               DryadLinqVertexWriter<R1> sink1,
                                               DryadLinqVertexWriter<R2> sink2,
                                               DryadLinqVertexWriter<R3> sink3)
        {
            DryadLinqRecordWriter<R1> writer1 = sink1.GetWriter(0);
            DryadLinqRecordWriter<R2> writer2 = sink2.GetWriter(0);
            DryadLinqRecordWriter<R3> writer3 = sink3.GetWriter(0);
        
            IEnumerable<ForkTuple<R1, R2, R3>>
                result = source.ExtendParallelPipeline(s => s.Select(mapper), orderPreserving);
        
            foreach (ForkTuple<R1, R2, R3> val in result)
            {
                if (val.HasFirst)
                {
                    writer1.WriteRecordAsync(val.First);
                }
                if (val.HasSecond)
                {
                    writer2.WriteRecordAsync(val.Second);
                }
                if (val.HasThird)
                {
                    writer3.WriteRecordAsync(val.Third);
                }
            }
            sink1.CloseWriters();
            sink2.CloseWriters();
            sink3.CloseWriters();            
        }
        
        public static void Fork<T, K>(IEnumerable<T> source,
                                      Func<T, K> keySelector,
                                      K[] keys,
                                      bool orderPreserving,
                                      params DryadLinqVertexWriter<T>[] sinks)
        {
            if (keys.Length != sinks.Length)
            {
                throw new DryadLinqException(SR.NumberOfKeysMustEqualNumOutputPorts);
            }
            Dictionary<K, int> keyMap = new Dictionary<K, int>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                keyMap.Add(keys[i], i);
            }
            DryadLinqRecordWriter<T>[] writers = new DryadLinqRecordWriter<T>[keys.Length];
            for (int i = 0; i < writers.Length; i++)
            {
                writers[i] = sinks[i].GetWriter(0);
            }
            foreach (T item in source)
            {
                int portNum;
                if (keyMap.TryGetValue(keySelector(item), out portNum))
                {
                    writers[portNum].WriteRecordAsync(item);
                }
                // item is dropped silently if not present in the keyMap
            }
            foreach (var sink in sinks)
            {
                sink.CloseWriters();
            }
        }

        public static IEnumerable<T> AsEnumerable<T>(T value)
        {
            yield return value;
        }
    }

    public struct AggregateValue<T>
    {
        private T _value;
        private long _count;

        public AggregateValue(T val, long count)
        {
            _value = val;
            _count = count;
        }

        public T Value
        {
            get { return _value; }
            set { _value = value; }
        }

        public long Count
        {
            get { return _count; }
            set { _count = value; }
        }

        public override string ToString()
        {
            return "<" + Value + ", " + Count + ">";
        }
    }

    internal class ParallelHashGroupBy<TSource, TKey, TElement, TResult, TFinal> : IParallelPipeline<TFinal>
    {
        private IEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private Func<TSource, TElement> m_elementSelector;
        private Func<TKey, IEnumerable<TElement>, TResult> m_resultSelector;
        private IEqualityComparer<TKey> m_comparer;
        private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;

        public ParallelHashGroupBy(IEnumerable<TSource> source,
                                   Func<TSource, TKey> keySelector,
                                   Func<TSource, TElement> elementSelector,
                                   Func<TKey, IEnumerable<TElement>, TResult> resultSelector,
                                   IEqualityComparer<TKey> comparer,
                                   Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            if (resultSelector == null || elementSelector == null)
            {
                throw new DryadLinqException("Internal error: The accumulator and element selector can't be null");
            }
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_resultSelector = resultSelector;
            this.m_applyFunc = applyFunc;
            this.m_comparer = comparer;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<TResult>, IEnumerable<TLast>>)(object)func;
                return new ParallelHashGroupBy<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_resultSelector, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelHashGroupBy<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_resultSelector, this.m_comparer, s => func(this.m_applyFunc(s)));
            }
        }
        
        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 HashSetCapacity = 16411;
            private const Int32 BufferSize = 2048;

            private IEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private Func<TSource, TElement> m_elementSelector;
            private Func<TKey, IEnumerable<TElement>, TResult> m_resultSelector;
            private IEqualityComparer<TKey> m_comparer;
            private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TSource[]>[] m_queues;
            private BlockingCollection<TFinal[]> m_resultSet;
            private BlockingCollection<TResult[]> m_stealingQueue;
            private int m_stealingWorkerCnt;
            private bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TFinal[]> m_resultEnum;
            private TFinal[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelHashGroupBy<TSource, TKey, TElement, TResult, TFinal> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_resultSelector = parent.m_resultSelector;
                this.m_comparer = parent.m_comparer;
                this.m_applyFunc = parent.m_applyFunc;

                this.m_isDone = false;
                this.m_stealingWorkerCnt = 0;
                this.m_stealingQueue = new BlockingCollection<TResult[]>();
                this.m_workerException = null;
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_queues = new BlockingCollection<TSource[]>[this.m_workers.Length];
                for (int i = 0; i < this.m_queues.Length; i++)
                {
                    this.m_queues[i] = new BlockingCollection<TSource[]>(2);
                }
                this.m_resultSet = new BlockingCollection<TFinal[]>(4);

                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HGB.ProcessAllItem";
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TFinal[0];
                this.m_index = -1;
                this.m_disposed = false;
            }
            
            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the queues
                    foreach (var queue in this.m_queues)
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                        {
                        }
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (Hash) started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Read all the items
                    TSource[][] buffers = new TSource[this.m_workers.Length][];
                    Int32[] counts = new Int32[this.m_workers.Length];
                    for (int i = 0; i < buffers.Length; i++)
                    {
                        buffers[i] = new TSource[BufferSize];
                        counts[i] = 0;
                    }
                    foreach (TSource item in this.m_source)
                    {
                        Int32 hashCode = this.m_comparer.GetHashCode(m_keySelector(item));
                        Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                        if (counts[idx] == BufferSize)
                        {
                            if (this.m_isDone) break;

                            this.m_queues[idx].Add(buffers[idx]);
                            buffers[idx] = new TSource[BufferSize];
                            counts[idx] = 0;
                        }
                        buffers[idx][counts[idx]] = item;
                        counts[idx]++;
                    }

                    // Add the final buffers to the queues and declare adding is complete
                    for (int i = 0; i < counts.Length; i++)
                    {
                        if (!this.m_isDone && counts[i] > 0)
                        {
                            TSource[] buffer = new TSource[counts[i]];
                            Array.Copy(buffers[i], buffer, counts[i]);
                            this.m_queues[i].Add(buffer);
                        }
                        this.m_queues[i].CompleteAdding();
                    }

                    DryadLinqLog.AddInfo("Parallel GroupBy (Hash) ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_queues.Length; i++)
                    {
                        this.m_queues[i].CompleteAdding();
                    }
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                return Task.Factory.StartNew(delegate { this.HashGroupBy(idx); }, TaskCreationOptions.LongRunning);
            }

            private void HashGroupBy(Int32 idx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (Hash) worker {0} started at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                    
                    BlockingCollection<TSource[]> queue = this.m_queues[idx];
                    Int32 wlen = this.m_workers.Length;
                    GroupingHashSet<TElement, TKey>
                        groups = new GroupingHashSet<TElement, TKey>(this.m_comparer, HashSetCapacity);
                    TResult[] resultBuffer = new TResult[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = this.m_stealingWorkerCnt;

                    foreach (var buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) break;
                        
                        for (int i = 0; i < buffer.Length; i++)
                        {
                            TSource item = buffer[i];
                            groups.AddItem(this.m_keySelector(item), this.m_elementSelector(item));
                        }
                    }

                    foreach (IGrouping<TKey, TElement> g in groups)
                    {
                        if (count == BufferSize)
                        {
                            if (this.m_isDone) break;

                            if (this.m_applyFunc == null)
                            {
                                this.m_resultSet.Add((TFinal[])(object)resultBuffer);
                                resultBuffer = new TResult[BufferSize];
                            }
                            else
                            {
                                int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                if (newStealingWorkerCnt > stealingWorkerCnt)
                                {
                                    myWorkCnt = 0;
                                    otherWorkCnt = 0;
                                    stealingWorkerCnt = newStealingWorkerCnt;
                                }
                                if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                {
                                    this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                    myWorkCnt++;
                                }
                                else
                                {
                                    this.m_stealingQueue.Add(resultBuffer);
                                    resultBuffer = new TResult[BufferSize];
                                    otherWorkCnt++;
                                }
                            }
                            count = 0;
                        }
                        resultBuffer[count++] = this.m_resultSelector(g.Key, g);
                    }

                    // Add the last buffer
                    if (!this.m_isDone && count > 0)
                    {
                        TResult[] lastResultBuffer = new TResult[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TFinal[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TResult[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInHashGroupBy,
                                                                    SR.FailureInHashGroupBy, e);
                    this.m_resultSet.Add(new TFinal[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (Hash) worker {0} ended at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }

    internal class ParallelHashGroupByPartialAccumulate<TSource, TRecord, TKey, TElement, TResult>
        : IEnumerable<Pair<TKey, TResult>>
    {
        private IEnumerable<TSource> m_source;
        private Func<IEnumerable<TSource>, IEnumerable<TRecord>> m_preApply;
        private Func<TRecord, TKey> m_keySelector;
        private Func<TRecord, TElement> m_elementSelector;
        private Func<TElement, TResult> m_seed;
        private Func<TResult, TElement, TResult> m_accumulator;
        private IEqualityComparer<TKey> m_comparer;

        public ParallelHashGroupByPartialAccumulate(IEnumerable<TSource> source,
                                                    Func<IEnumerable<TSource>, IEnumerable<TRecord>> preApply,
                                                    Func<TRecord, TKey> keySelector,
                                                    Func<TRecord, TElement> elementSelector,
                                                    Func<TElement, TResult> seed,
                                                    Func<TResult, TElement, TResult> accumulator,
                                                    IEqualityComparer<TKey> comparer)
        {
            if (seed == null || accumulator == null || elementSelector == null)
            {
                throw new DryadLinqException("Internal error: The accumulator and element selector can't be null");
            }
            this.m_source = source;
            this.m_preApply = preApply;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_seed = seed;
            this.m_accumulator = accumulator;
            this.m_comparer = comparer;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<Pair<TKey, TResult>> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }
        
        private class InnerEnumerator : IEnumerator<Pair<TKey, TResult>>
        {
            private const Int32 HashSetCapacity = 16411;
            private const Int32 BufferSize = 2048;

            private IEnumerable<TSource> m_source;
            private Func<IEnumerable<TSource>, IEnumerable<TRecord>> m_preApply;
            private Func<TRecord, TKey> m_keySelector;
            private Func<TRecord, TElement> m_elementSelector;
            private Func<TElement, TResult> m_seed;
            private Func<TResult, TElement, TResult> m_accumulator;
            private IEqualityComparer<TKey> m_comparer;
            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TSource[]>[] m_queues;
            private BlockingCollection<Pair<TKey, TResult>[]> m_resultSet;
            private bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<Pair<TKey, TResult>[]> m_resultEnum;
            private Pair<TKey, TResult>[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelHashGroupByPartialAccumulate<TSource, TRecord, TKey, TElement, TResult> parent)
            {
                this.m_source = parent.m_source;
                this.m_preApply = parent.m_preApply;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_seed = parent.m_seed;
                this.m_accumulator = parent.m_accumulator;
                this.m_comparer = parent.m_comparer;

                this.m_isDone = false;
                this.m_workerException = null;
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_queues = new BlockingCollection<TSource[]>[this.m_workers.Length];
                for (int i = 0; i < this.m_queues.Length; i++)
                {
                    this.m_queues[i] = new BlockingCollection<TSource[]>(2);
                }
                this.m_resultSet = new BlockingCollection<Pair<TKey, TResult>[]>(4);

                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HGBPA.ProcessAllItem";
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new Pair<TKey, TResult>[0];
                this.m_index = -1;
                this.m_disposed = false;
            }
            
            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public Pair<TKey, TResult> Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the queues
                    foreach (var queue in this.m_queues)
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                        {
                        }
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashPartialAcc) started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Read all the items
                    TSource[] buffer = new TSource[BufferSize];
                    Int32 count = 0;
                    foreach (TSource item in this.m_source)
                    {
                        if (count == BufferSize)
                        {
                            if (this.m_isDone) break;

                            BlockingCollection<TSource[]>.AddToAny(this.m_queues, buffer);
                            buffer = new TSource[BufferSize];
                            count = 0;
                        }
                        buffer[count++] = item;
                    }

                    // Add the final buffers to the queues and declare adding is complete
                    if (!this.m_isDone && count > 0)
                    {
                        TSource[] lastBuffer = new TSource[count];
                        Array.Copy(buffer, lastBuffer, count);
                        buffer = null;
                        BlockingCollection<TSource[]>.AddToAny(this.m_queues, lastBuffer);
                    }

                    for (int i = 0; i < this.m_queues.Length; i++)
                    {
                        this.m_queues[i].CompleteAdding();
                    }

                    DryadLinqLog.AddInfo("Parallel GroupBy (HashPartialAcc) ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_queues.Length; i++)
                    {
                        this.m_queues[i].CompleteAdding();
                    }
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                return Task.Factory.StartNew(delegate { this.HashGroupBy(idx); }, TaskCreationOptions.LongRunning);
            }

            private void HashGroupBy(Int32 idx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashPartialAcc) worker {0} started at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    BlockingCollection<TSource[]> queue = this.m_queues[idx];
                    AccumulateDictionary<TKey, TElement, TResult>
                        groups = new AccumulateDictionary<TKey, TElement, TResult>(
                                          this.m_comparer, HashSetCapacity, this.m_seed, this.m_accumulator);

                    foreach (var buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) break;

                        if (this.m_preApply == null)
                        {
                            TRecord[] recBuffer = (TRecord[])(object)buffer;
                            for (int i = 0; i < recBuffer.Length; i++)
                            {
                                TRecord item = recBuffer[i];
                                groups.Add(this.m_keySelector(item), this.m_elementSelector(item));
                            }
                        }
                        else
                        {
                            IEnumerable<TRecord> recBuffer = this.m_preApply(buffer);
                            foreach (TRecord item in recBuffer)
                            {
                                groups.Add(this.m_keySelector(item), this.m_elementSelector(item));
                            }
                        }
                    }

                    Int32 wlen = this.m_workers.Length;
                    Pair<TKey, TResult>[] resultBuffer = new Pair<TKey, TResult>[BufferSize];
                    Int32 count = 0;
                    foreach (Pair<TKey, TResult> g in groups)
                    {
                        if (count == BufferSize)
                        {
                            if (this.m_isDone) break;

                            this.m_resultSet.Add(resultBuffer);
                            resultBuffer = new Pair<TKey, TResult>[BufferSize];
                            count = 0;
                        }
                        resultBuffer[count++] = g;
                    }

                    if (!this.m_isDone && count > 0)
                    {
                        Pair<TKey, TResult>[] lastResultBuffer = new Pair<TKey, TResult>[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        this.m_resultSet.Add(lastResultBuffer);
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInHashGroupBy,
                                                                    SR.FailureInHashGroupBy, e);
                    this.m_resultSet.Add(new Pair<TKey, TResult>[0]);
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashPartialAcc) worker {0} ended at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }

    internal class ParallelHashGroupByFullAccumulate<TSource, TKey, TElement, TResult, TFinal>
        : IParallelPipeline<TFinal>
    {
        private IEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private Func<TSource, TElement> m_elementSelector;
        private Func<TElement, TResult> m_seed;
        private Func<TResult, TElement, TResult> m_accumulator;
        private IEqualityComparer<TKey> m_comparer;
        private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_postApply;

        public ParallelHashGroupByFullAccumulate(IEnumerable<TSource> source,
                                                 Func<TSource, TKey> keySelector,
                                                 Func<TSource, TElement> elementSelector,
                                                 Func<TElement, TResult> seed,
                                                 Func<TResult, TElement, TResult> accumulator,
                                                 IEqualityComparer<TKey> comparer,
                                                 Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> postApply)
        {
            if (seed == null || accumulator == null || elementSelector == null)
            {
                throw new DryadLinqException("Internal error: The accumulator and element selector can't be null");
            }
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_seed = seed;
            this.m_accumulator = accumulator;
            this.m_postApply = postApply;
            this.m_comparer = comparer;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }
        
        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_postApply == null)
            {
                var applyFunc = (Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TLast>>)(object)func;
                return new ParallelHashGroupByFullAccumulate<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector, this.m_seed,
                                 this.m_accumulator, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelHashGroupByFullAccumulate<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector, this.m_seed,
                                 this.m_accumulator, this.m_comparer, s => func(this.m_postApply(s)));
            }
        }
        
        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 HashSetCapacity = 16411;
            private const Int32 BufferSize = 2048;

            private IEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private Func<TSource, TElement> m_elementSelector;
            private Func<TElement, TResult> m_seed;
            private Func<TResult, TElement, TResult> m_accumulator;
            private IEqualityComparer<TKey> m_comparer;
            private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_postApply;
            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TSource[]>[] m_queues;
            private BlockingCollection<TFinal[]> m_resultSet;
            private BlockingCollection<Pair<TKey, TResult>[]> m_stealingQueue;
            private int m_stealingWorkerCnt;
            private bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TFinal[]> m_resultEnum;
            private TFinal[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelHashGroupByFullAccumulate<TSource, TKey, TElement, TResult, TFinal> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_seed = parent.m_seed;
                this.m_accumulator = parent.m_accumulator;
                this.m_comparer = parent.m_comparer;
                this.m_postApply = parent.m_postApply;

                this.m_isDone = false;
                this.m_stealingWorkerCnt = 0;
                this.m_stealingQueue = new BlockingCollection<Pair<TKey, TResult>[]>();
                this.m_workerException = null;
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_queues = new BlockingCollection<TSource[]>[this.m_workers.Length];
                for (int i = 0; i < this.m_queues.Length; i++)
                {
                    this.m_queues[i] = new BlockingCollection<TSource[]>(2);
                }
                this.m_resultSet = new BlockingCollection<TFinal[]>(4);

                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HGBFA.ProcessAllItem";
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TFinal[0];
                this.m_index = -1;
                this.m_disposed = false;
            }
            
            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the queues
                    foreach (var queue in this.m_queues)
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                        {
                        }
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashFullAcc) started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Read all the items
                    TSource[][] buffers = new TSource[this.m_workers.Length][];
                    Int32[] counts = new Int32[this.m_workers.Length];
                    for (int i = 0; i < buffers.Length; i++)
                    {
                        buffers[i] = new TSource[BufferSize];
                        counts[i] = 0;
                    }
                    foreach (TSource item in this.m_source)
                    {
                        Int32 hashCode = this.m_comparer.GetHashCode(m_keySelector(item));
                        Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                        if (counts[idx] == BufferSize)
                        {
                            if (this.m_isDone) break;

                            this.m_queues[idx].Add(buffers[idx]);
                            buffers[idx] = new TSource[BufferSize];
                            counts[idx] = 0;
                        }
                        buffers[idx][counts[idx]] = item;
                        counts[idx]++;
                    }

                    // Add the final buffers to the queues and declare adding is complete
                    for (int i = 0; i < counts.Length; i++)
                    {
                        if (!this.m_isDone && counts[i] > 0)
                        {
                            TSource[] buffer = new TSource[counts[i]];
                            Array.Copy(buffers[i], buffer, counts[i]);
                            this.m_queues[i].Add(buffer);
                        }
                        this.m_queues[i].CompleteAdding();
                    }

                    DryadLinqLog.AddInfo("Parallel GroupBy (HashFullAcc) ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_queues.Length; i++)
                    {
                        this.m_queues[i].CompleteAdding();
                    }
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                return Task.Factory.StartNew(delegate { this.HashGroupBy(idx); }, TaskCreationOptions.LongRunning);
            }

            private void HashGroupBy(Int32 idx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashFullAcc) worker {0} started at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    BlockingCollection<TSource[]> queue = this.m_queues[idx];
                    AccumulateDictionary<TKey, TElement, TResult>
                        groups = new AccumulateDictionary<TKey, TElement, TResult>(
                                          this.m_comparer, HashSetCapacity, this.m_seed, this.m_accumulator);

                    foreach (var buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) break;

                        for (int i = 0; i < buffer.Length; i++)
                        {
                            TSource item = buffer[i];
                            groups.Add(this.m_keySelector(item), this.m_elementSelector(item));
                        }
                    }

                    Int32 wlen = this.m_workers.Length;
                    Pair<TKey, TResult>[] resultBuffer = new Pair<TKey, TResult>[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = this.m_stealingWorkerCnt;
                    
                    foreach (Pair<TKey, TResult> g in groups)
                    {
                        if (count == BufferSize)
                        {
                            if (this.m_isDone) break;

                            if (this.m_postApply == null)
                            {
                                this.m_resultSet.Add((TFinal[])(object)resultBuffer);
                                resultBuffer = new Pair<TKey, TResult>[BufferSize];
                            }
                            else
                            {
                                int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                if (newStealingWorkerCnt > stealingWorkerCnt)
                                {
                                    myWorkCnt = 0;
                                    otherWorkCnt = 0;
                                    stealingWorkerCnt = newStealingWorkerCnt;
                                }
                                if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                {
                                    this.m_resultSet.Add(this.m_postApply(resultBuffer).ToArray());
                                    myWorkCnt++;
                                }
                                else
                                {
                                    this.m_stealingQueue.Add(resultBuffer);
                                    resultBuffer = new Pair<TKey, TResult>[BufferSize];
                                    otherWorkCnt++;
                                }
                            }
                            count = 0;
                        }
                        resultBuffer[count++] = g;
                    }

                    if (!this.m_isDone && count > 0)
                    {
                        Pair<TKey, TResult>[] lastResultBuffer = new Pair<TKey, TResult>[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_postApply == null)
                        {
                            this.m_resultSet.Add((TFinal[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_postApply(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_postApply != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (Pair<TKey, TResult>[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_postApply(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInHashGroupBy,
                                                                    SR.FailureInHashGroupBy, e);
                    this.m_resultSet.Add(new TFinal[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (HashFullAcc) worker {0} ended at {1}",
                                         idx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }

    // This is really partial groupby.
    internal class ParallelSortGroupBy<TSource, TKey, TElement, TResult, TFinal> : IEnumerable<TFinal>
    {
        private IEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private Func<TSource, TElement> m_elementSelector;
        private Func<IEnumerable<TElement>, TResult> m_resultSelector;
        private IComparer<TKey> m_comparer;
        private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_applyFunc;

        public ParallelSortGroupBy(IEnumerable<TSource> source,
                                   Func<TSource, TKey> keySelector,
                                   Func<TSource, TElement> elementSelector,
                                   Func<IEnumerable<TElement>, TResult> resultSelector,
                                   IComparer<TKey> comparer,
                                   Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> applyFunc)
        {
            if (resultSelector == null || elementSelector == null)
            {
                throw new DryadLinqException("Internal error: The result and element selectors can't be null");
            }
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_resultSelector = resultSelector;
            this.m_comparer = comparer;
            this.m_applyFunc = applyFunc;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 ChunkSize = (1 << 22);
            private const Int32 ResultChunkSize = 2048;

            private IEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private Func<TSource, TElement> m_elementSelector;
            private Func<IEnumerable<TElement>, TResult> m_resultSelector;
            private IComparer<TKey> m_comparer;
            private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_applyFunc;
            
            private Thread m_mainWorker;
            private List<Task> m_workers;
            private BlockingCollection<TFinal[]> m_resultSet;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TFinal[]> m_resultEnum;
            private TFinal[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelSortGroupBy<TSource, TKey, TElement, TResult, TFinal> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_resultSelector = parent.m_resultSelector;
                this.m_comparer = parent.m_comparer;
                this.m_applyFunc = parent.m_applyFunc;

                this.m_isDone = false;
                this.m_workerException = null;
                this.m_workers = new List<Task>(16);
                this.m_resultSet = new BlockingCollection<TFinal[]>();
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "SGB.ProcessAllItem";
                this.m_mainWorker.Start();
            
                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TFinal[0];
                this.m_index = -1;
                this.m_disposed = false;
            }
            
            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel GroupBy (PartialSort) started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    TSource[] itemArray = new TSource[ChunkSize];
                    Int32 itemCnt = 0;
                    foreach (TSource item in this.m_source)
                    {
                        if (itemCnt == ChunkSize)
                        {
                            if (this.m_isDone) break;

                            this.ProcessItemArray(itemArray, itemCnt);
                            itemArray = new TSource[ChunkSize];
                            itemCnt = 0;
                        }
                        itemArray[itemCnt++] = item;
                    }

                    if (!this.m_isDone && itemCnt > 0)
                    {
                        this.ProcessItemArray(itemArray, itemCnt);
                    }

                    DryadLinqLog.AddInfo("Parallel GroupBy (PartialSort) ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    foreach (Task task in this.m_workers)
                    {
                        task.Wait();
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    this.m_resultSet.CompleteAdding();
                }
            }

            private void ProcessItemArray(TSource[] itemArray, Int32 itemCnt)
            {
                Wrapper<TSource[]> wrappedItemArray = new Wrapper<TSource[]>(itemArray);

                // NOT using the TCO.LongRunning option for this task, because it's spawned an
                // arbitrary # of times for potentially shorter work which means it's best to
                // leave this to the TP load balancing algorithm
                Task task = Task.Factory.StartNew(delegate { this.SortGroupByItemArray(wrappedItemArray, itemCnt); });
                this.m_workers.Add(task);
            }

            private void SortGroupByItemArray(Wrapper<TSource[]> wrappedItemArray, Int32 itemCnt)
            {
                try
                {
                    TSource[] itemArray = wrappedItemArray.item;
                    TKey[] keyArray = new TKey[itemCnt];
                    for (int i = 0; i < itemCnt; i++)
                    {
                        keyArray[i] = this.m_keySelector(itemArray[i]);
                    }
                    Array.Sort(keyArray, itemArray, 0, itemCnt, this.m_comparer);

                    Pair<TKey, TResult>[] resultChunk = new Pair<TKey, TResult>[ResultChunkSize];
                    Int32 count = 0;
                    if (itemCnt > 0)
                    {
                        Grouping<TKey, TElement> curGroup = new Grouping<TKey, TElement>(keyArray[0]);
                        curGroup.AddItem(this.m_elementSelector(itemArray[0]));
                        for (int i = 1; i < itemCnt; i++)
                        {
                            if (this.m_comparer.Compare(curGroup.Key, keyArray[i]) != 0)
                            {
                                if (count == ResultChunkSize)
                                {
                                    if (this.m_isDone) break;

                                    if (this.m_applyFunc == null)
                                    {
                                        this.m_resultSet.Add((TFinal[])(object)resultChunk);
                                        resultChunk = new Pair<TKey, TResult>[ResultChunkSize];
                                    }
                                    else
                                    {
                                        this.m_resultSet.Add(this.m_applyFunc(resultChunk).ToArray());
                                    }
                                    count = 0;
                                }
                                resultChunk[count++] = new Pair<TKey, TResult>(curGroup.Key,
                                                                               this.m_resultSelector(curGroup));
                                curGroup = new Grouping<TKey, TElement>(keyArray[i]);
                            }
                            curGroup.AddItem(this.m_elementSelector(itemArray[i]));
                        }

                        // Add the last group
                        if (count == ResultChunkSize)
                        {
                            Pair<TKey, TResult>[] lastResultChunk = new Pair<TKey, TResult>[count + 1];
                            Array.Copy(resultChunk, lastResultChunk, count);
                            resultChunk = lastResultChunk;
                        }
                        resultChunk[count++] = new Pair<TKey, TResult>(curGroup.Key,
                                                                       this.m_resultSelector(curGroup));

                        // Add the last chunk
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TFinal[])(object)resultChunk);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(resultChunk).ToArray());
                        }
                    }
                    wrappedItemArray.item = null;
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInSortGroupBy,
                                                                    SR.FailureInSortGroupBy, e);
                    throw this.m_workerException;
                }
            }
        }
    }

    internal class ParallelHashJoin<TOuter, TInner, TKey, TResult, TFinal> : IParallelPipeline<TFinal>
    {
        private IEnumerable<TOuter> m_outer;
        private IEnumerable<TInner> m_inner;
        private Func<TOuter, TKey> m_outerKeySelector;
        private Func<TInner, TKey> m_innerKeySelector;
        private Func<TOuter, TInner, TResult> m_resultSelector;
        private IEqualityComparer<TKey> m_comparer;
        private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;

        public ParallelHashJoin(IEnumerable<TOuter> outer,
                                IEnumerable<TInner> inner,
                                Func<TOuter, TKey> outerKeySelector,
                                Func<TInner, TKey> innerKeySelector,
                                Func<TOuter, TInner, TResult> resultSelector,
                                IEqualityComparer<TKey> comparer,
                                Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            this.m_outer = outer;
            this.m_inner = inner;
            this.m_outerKeySelector = outerKeySelector;
            this.m_innerKeySelector = innerKeySelector;
            this.m_resultSelector = resultSelector;
            this.m_applyFunc = applyFunc;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<TResult>, IEnumerable<TLast>>)(object)func;
                return new ParallelHashJoin<TOuter, TInner, TKey, TResult, TLast>(
                                 this.m_outer, this.m_inner, this.m_outerKeySelector, this.m_innerKeySelector,
                                 this.m_resultSelector, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelHashJoin<TOuter, TInner, TKey, TResult, TLast>(
                                 this.m_outer, this.m_inner, this.m_outerKeySelector, this.m_innerKeySelector,
                                 this.m_resultSelector, this.m_comparer, s => func(this.m_applyFunc(s)));
            }
        }
        
        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 BufferSize = 2048;
            private const Int32 QueueMaxSize = 4;
            private const Int32 ResultQueueMaxSize = 8;
            
            private IEnumerable<TOuter> m_outer;
            private IEnumerable<TInner> m_inner;
            private Func<TOuter, TKey> m_outerKeySelector;
            private Func<TInner, TKey> m_innerKeySelector;
            private Func<TOuter, TInner, TResult> m_resultSelector;
            private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
            private IEqualityComparer<TKey> m_comparer;
            private bool m_hashInner;

            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TInner[]>[] m_innerQueues;
            private BlockingCollection<TOuter[]>[] m_outerQueues;            
            private BlockingCollection<TFinal[]> m_resultSet;
            private BlockingCollection<TResult[]> m_stealingQueue;
            private Int32 m_stealingWorkerCnt;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TFinal[]> m_resultEnum;
            private TFinal[] m_currentItems;
            private Int32 m_index;
            private bool m_disposed;
            
            public InnerEnumerator(ParallelHashJoin<TOuter, TInner, TKey, TResult, TFinal> parent)
            {
                this.m_outer = parent.m_outer;
                this.m_inner = parent.m_inner;
                this.m_outerKeySelector = parent.m_outerKeySelector;
                this.m_innerKeySelector = parent.m_innerKeySelector;
                this.m_resultSelector = parent.m_resultSelector;
                this.m_applyFunc = parent.m_applyFunc;
                this.m_comparer = parent.m_comparer;

                this.m_hashInner = true;
                if ((this.m_outer is DryadLinqVertexReader<TOuter>) &&
                    (this.m_inner is DryadLinqVertexReader<TInner>))
                {
                    Int64 outerLen = ((DryadLinqVertexReader<TOuter>)this.m_outer).GetTotalLength();
                    Int64 innerLen = ((DryadLinqVertexReader<TInner>)this.m_inner).GetTotalLength();
                    if (innerLen >= 0 && outerLen >= 0)
                    {
                        this.m_hashInner = innerLen <= outerLen;
                    }
                    DryadLinqLog.AddInfo("Parallel HashJoin: outerLen={0}, innerLen={1}", outerLen, innerLen);                    
                }

                this.m_isDone = false;
                this.m_stealingWorkerCnt = 0;
                this.m_stealingQueue = new BlockingCollection<TResult[]>();
                this.m_workerException = null;
                Int32 wlen = Environment.ProcessorCount;
                this.m_workers = new Task[wlen];
                this.m_outerQueues = new BlockingCollection<TOuter[]>[wlen];
                this.m_innerQueues = new BlockingCollection<TInner[]>[wlen];
                for (int i = 0; i < wlen; i++)
                {
                    this.m_outerQueues[i] = new BlockingCollection<TOuter[]>(QueueMaxSize);
                    this.m_innerQueues[i] = new BlockingCollection<TInner[]>(QueueMaxSize);
                }

                this.m_resultSet = new BlockingCollection<TFinal[]>(ResultQueueMaxSize);
                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HJ.ProcessAllItem";                
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TFinal[0];
                this.m_index = -1;
                this.m_disposed = false;
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new InvalidOperationException();
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the outer queues
                    foreach (var outerQueue in this.m_outerQueues)
                    {
                        foreach (var item in outerQueue.GetConsumingEnumerable())
                        {
                        }
                    }
                    // Always drain the inner queues
                    foreach (var innerQueue in this.m_innerQueues)
                    {
                        foreach (var item in innerQueue.GetConsumingEnumerable())
                        {
                        }
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel HashJoin started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    Int32 wlen = this.m_workers.Length;
                    Int32[] counts = new Int32[wlen];
                    if (this.m_hashInner)
                    {
                        // Send the inner to the workers
                        TInner[][] innerBuffers = new TInner[wlen][];
                        for (int i = 0; i < wlen; i++)
                        {
                            innerBuffers[i] = new TInner[BufferSize];
                            counts[i] = 0;
                        }

                        foreach (TInner innerItem in this.m_inner)
                        {
                            TKey innerKey = this.m_innerKeySelector(innerItem);
                            Int32 hashCode = this.m_comparer.GetHashCode(innerKey);
                            Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                            if (counts[idx] == BufferSize)
                            {
                                if (this.m_isDone) break;

                                this.m_innerQueues[idx].Add(innerBuffers[idx]);
                                innerBuffers[idx] = new TInner[BufferSize];
                                counts[idx] = 0;
                            }
                            innerBuffers[idx][counts[idx]] = innerItem;
                            counts[idx]++;
                        }

                        // Add the final buffers to the queues and declare adding is complete
                        for (int i = 0; i < wlen; i++)
                        {
                            if (!this.m_isDone && counts[i] > 0)
                            {
                                TInner[] lastBuffer = new TInner[counts[i]];
                                Array.Copy(innerBuffers[i], lastBuffer, counts[i]);
                                this.m_innerQueues[i].Add(lastBuffer);
                            }
                            this.m_innerQueues[i].CompleteAdding();
                        }
                        innerBuffers = null;
                        
                        // Send the outer to the workers
                        TOuter[][] outerBuffers = new TOuter[wlen][];
                        for (int i = 0; i < wlen; i++)
                        {
                            outerBuffers[i] = new TOuter[BufferSize];
                            counts[i] = 0;
                        }
                        foreach (TOuter outerItem in this.m_outer)
                        {
                            TKey outerKey = this.m_outerKeySelector(outerItem);
                            Int32 hashCode = this.m_comparer.GetHashCode(outerKey);
                            Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                            if (counts[idx] == BufferSize)
                            {
                                if (this.m_isDone) break;

                                this.m_outerQueues[idx].Add(outerBuffers[idx]);
                                outerBuffers[idx] = new TOuter[BufferSize];
                                counts[idx] = 0;
                            }
                            outerBuffers[idx][counts[idx]] = outerItem;
                            counts[idx]++;
                        }

                        // Add the final buffers to the queues and declare adding is complete
                        for (int i = 0; i < wlen; i++)
                        {
                            if (!this.m_isDone && counts[i] > 0)
                            {
                                TOuter[] lastBuffer = new TOuter[counts[i]];
                                Array.Copy(outerBuffers[i], lastBuffer, counts[i]);
                                this.m_outerQueues[i].Add(lastBuffer);
                            }
                            this.m_outerQueues[i].CompleteAdding();
                        }
                        outerBuffers = null;                        
                    }
                    else
                    {
                        // Send outer to the workers
                        TOuter[][] outerBuffers = new TOuter[wlen][];
                        for (int i = 0; i < wlen; i++)
                        {
                            outerBuffers[i] = new TOuter[BufferSize];
                            counts[i] = 0;
                        }
                        foreach (TOuter outerItem in this.m_outer)
                        {
                            TKey outerKey = this.m_outerKeySelector(outerItem);
                            Int32 hashCode = this.m_comparer.GetHashCode(outerKey);
                            Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                            if (counts[idx] == BufferSize)
                            {
                                if (this.m_isDone) break;

                                this.m_outerQueues[idx].Add(outerBuffers[idx]);
                                outerBuffers[idx] = new TOuter[BufferSize];
                                counts[idx] = 0;
                            }
                            outerBuffers[idx][counts[idx]] = outerItem;
                            counts[idx]++;
                        }

                        // Add the final buffers to the queues and declare adding is complete
                        for (int i = 0; i < wlen; i++)
                        {
                            if (!this.m_isDone && counts[i] > 0)
                            {
                                TOuter[] lastBuffer = new TOuter[counts[i]];
                                Array.Copy(outerBuffers[i], lastBuffer, counts[i]);
                                this.m_outerQueues[i].Add(lastBuffer);
                            }
                            this.m_outerQueues[i].CompleteAdding();
                        }
                        outerBuffers = null;

                        // Send the inner to the workers
                        TInner[][] innerBuffers = new TInner[wlen][];
                        for (int i = 0; i < wlen; i++)
                        {
                            innerBuffers[i] = new TInner[BufferSize];
                            counts[i] = 0;
                        }

                        foreach (TInner innerItem in this.m_inner)
                        {
                            TKey innerKey = this.m_innerKeySelector(innerItem);
                            Int32 hashCode = this.m_comparer.GetHashCode(innerKey);
                            Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                            if (counts[idx] == BufferSize)
                            {
                                if (this.m_isDone) break;

                                this.m_innerQueues[idx].Add(innerBuffers[idx]);
                                innerBuffers[idx] = new TInner[BufferSize];
                                counts[idx] = 0;
                            }
                            innerBuffers[idx][counts[idx]] = innerItem;
                            counts[idx]++;
                        }

                        // Add the final buffers to the queues and declare adding is complete
                        for (int i = 0; i < wlen; i++)
                        {
                            if (!this.m_isDone && counts[i] > 0)
                            {
                                TInner[] lastBuffer = new TInner[counts[i]];
                                Array.Copy(innerBuffers[i], lastBuffer, counts[i]);
                                this.m_innerQueues[i].Add(lastBuffer);
                            }
                            this.m_innerQueues[i].CompleteAdding();
                        }
                        innerBuffers = null;
                    }

                    DryadLinqLog.AddInfo("Parallel HashJoin ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_outerQueues.Length; i++)
                    {
                        this.m_outerQueues[i].CompleteAdding();
                    }
                    for (int i = 0; i < this.m_innerQueues.Length; i++)
                    {
                        this.m_innerQueues[i].CompleteAdding();
                    }
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                // using the TCO.LongRunning option, because this method is called a fix number of times to spawn worker tasks
                // which means it is safe to request a decicated thread for this task
                return Task.Factory.StartNew(delegate { this.DoHashJoin(idx); },
                                             TaskCreationOptions.LongRunning);
            }

            private void DoHashJoin(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel HashJoin worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                    
                    Int32 wlen = this.m_workers.Length;
                    TResult[] resultBuffer = new TResult[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = this.m_stealingWorkerCnt;

                    if (this.m_hashInner)
                    {
                        GroupingHashSet<TInner, TKey> innerGroups = new GroupingHashSet<TInner, TKey>(this.m_comparer);
                        foreach (var buffer in this.m_innerQueues[qidx].GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int bidx = 0; bidx < buffer.Length; bidx++)
                            {
                                TInner innerItem = buffer[bidx];
                                TKey innerKey = this.m_innerKeySelector(innerItem);
                                innerGroups.AddItem(innerKey, innerItem);
                            }
                        }

                        DryadLinqLog.AddInfo("Parallel HashJoin: In-memory hashtable using inner created at {0}",
                                             DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                                
                        foreach (var buffer in this.m_outerQueues[qidx].GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int bidx = 0; bidx < buffer.Length; bidx++)
                            {
                                TOuter outerItem = buffer[bidx];
                                Grouping<TKey, TInner> innerGroup = innerGroups.GetGroup(this.m_outerKeySelector(outerItem));
                                if (innerGroup != null)
                                {
                                    foreach (TInner item in innerGroup)
                                    {
                                        if (count == BufferSize)
                                        {
                                            if (this.m_applyFunc == null)
                                            {
                                                this.m_resultSet.Add((TFinal[])(object)resultBuffer);
                                                resultBuffer = new TResult[BufferSize];
                                            }
                                            else
                                            {
                                                int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                                if (newStealingWorkerCnt > stealingWorkerCnt)
                                                {
                                                    myWorkCnt = 0;
                                                    otherWorkCnt = 0;
                                                    stealingWorkerCnt = newStealingWorkerCnt;
                                                }
                                                if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                                {
                                                    this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                    myWorkCnt++;
                                                }
                                                else
                                                {
                                                    this.m_stealingQueue.Add(resultBuffer);
                                                    resultBuffer = new TResult[BufferSize];
                                                    otherWorkCnt++;
                                                }
                                            }
                                            count = 0;
                                        }
                                        resultBuffer[count++] = this.m_resultSelector(outerItem, item);
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        GroupingHashSet<TOuter, TKey> outerGroups = new GroupingHashSet<TOuter, TKey>(this.m_comparer);
                        foreach (var buffer in this.m_outerQueues[qidx].GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int bidx = 0; bidx < buffer.Length; bidx++)
                            {
                                TOuter outerItem = buffer[bidx];
                                TKey outerKey = this.m_outerKeySelector(outerItem);
                                outerGroups.AddItem(outerKey, outerItem);
                            }
                        }

                        DryadLinqLog.AddInfo("Parallel HashJoin: In-memory hashtable using outer created at {0}",
                                             DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                        
                        foreach (var buffer in this.m_innerQueues[qidx].GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int bidx = 0; bidx < buffer.Length; bidx++)
                            {
                                TInner innerItem = buffer[bidx];
                                Grouping<TKey, TOuter> outerGroup = outerGroups.GetGroup(this.m_innerKeySelector(innerItem));
                                if (outerGroup != null)
                                {
                                    foreach (TOuter item in outerGroup)
                                    {
                                        if (count == BufferSize)
                                        {
                                            if (this.m_applyFunc == null)
                                            {
                                                this.m_resultSet.Add((TFinal[])(object)resultBuffer);
                                                resultBuffer = new TResult[BufferSize];
                                            }
                                            else
                                            {
                                                int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                                if (newStealingWorkerCnt > stealingWorkerCnt)
                                                {
                                                    myWorkCnt = 0;
                                                    otherWorkCnt = 0;
                                                    stealingWorkerCnt = newStealingWorkerCnt;
                                                }
                                                if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                                {
                                                    this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                    myWorkCnt++;
                                                }
                                                else
                                                {
                                                    this.m_stealingQueue.Add(resultBuffer);
                                                    resultBuffer = new TResult[BufferSize];
                                                    otherWorkCnt++;
                                                }
                                            }
                                            count = 0;
                                        }
                                        resultBuffer[count++] = this.m_resultSelector(item, innerItem);
                                    }
                                }
                            }
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TResult[] lastResultBuffer = new TResult[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TFinal[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TResult[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInHashJoin,
                                                                    SR.FailureInHashJoin, e);
                    this.m_resultSet.Add(new TFinal[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel HashJoin worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }
    
    internal class ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TFinal> : IParallelPipeline<TFinal>
    {
        private IEnumerable<TOuter> m_outer;
        private IEnumerable<TInner> m_inner;
        private Func<TOuter, TKey> m_outerKeySelector;
        private Func<TInner, TKey> m_innerKeySelector;
        private Func<TOuter, IEnumerable<TInner>, TResult> m_resultSelector;
        private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
        private IEqualityComparer<TKey> m_comparer;

        public ParallelHashGroupJoin(IEnumerable<TOuter> outer,
                                     IEnumerable<TInner> inner,
                                     Func<TOuter, TKey> outerKeySelector,
                                     Func<TInner, TKey> innerKeySelector,
                                     Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                                     IEqualityComparer<TKey> comparer,
                                     Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            this.m_outer = outer;
            this.m_inner = inner;
            this.m_outerKeySelector = outerKeySelector;
            this.m_innerKeySelector = innerKeySelector;
            this.m_resultSelector = resultSelector;
            this.m_applyFunc = applyFunc;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<TResult>, IEnumerable<TLast>>)(object)func;
                return new ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TLast>(
                                 this.m_outer, this.m_inner, this.m_outerKeySelector, this.m_innerKeySelector,
                                 this.m_resultSelector, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TLast>(
                                 this.m_outer, this.m_inner, this.m_outerKeySelector, this.m_innerKeySelector,
                                 this.m_resultSelector, this.m_comparer, s => func(this.m_applyFunc(s)));
            }
        }
        
        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 BufferSize = 2048;
            private const Int32 QueueMaxSize = 4;
            private const Int32 ResultQueueMaxSize = 8;

            private IEnumerable<TOuter> m_outer;
            private IEnumerable<TInner> m_inner;
            private Func<TOuter, TKey> m_outerKeySelector;
            private Func<TInner, TKey> m_innerKeySelector;
            private Func<TOuter, IEnumerable<TInner>, TResult> m_resultSelector;
            private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
            private IEqualityComparer<TKey> m_comparer;

            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TOuter[]>[] m_outerQueues;
            private BlockingCollection<TInner[]>[] m_innerQueues;
            private BlockingCollection<TFinal[]> m_resultSet;
            private BlockingCollection<TResult[]> m_stealingQueue;
            private int m_stealingWorkerCnt;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TFinal[]> m_resultEnum;
            private TFinal[] m_currentItems;
            private int m_index;
            private bool m_disposed;
            
            public InnerEnumerator(ParallelHashGroupJoin<TOuter, TInner, TKey, TResult, TFinal> parent)
            {
                this.m_outer = parent.m_outer;
                this.m_inner = parent.m_inner;
                this.m_outerKeySelector = parent.m_outerKeySelector;
                this.m_innerKeySelector = parent.m_innerKeySelector;
                this.m_resultSelector = parent.m_resultSelector;
                this.m_applyFunc = parent.m_applyFunc;
                this.m_comparer = parent.m_comparer;

                this.m_isDone = false;
                this.m_stealingWorkerCnt = 0;
                this.m_stealingQueue = new BlockingCollection<TResult[]>();
                this.m_workerException = null;
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_outerQueues = new BlockingCollection<TOuter[]>[this.m_workers.Length];
                this.m_innerQueues = new BlockingCollection<TInner[]>[this.m_workers.Length];
                for (int i = 0; i < this.m_outerQueues.Length; i++)
                {
                    this.m_outerQueues[i] = new BlockingCollection<TOuter[]>(QueueMaxSize);
                    this.m_innerQueues[i] = new BlockingCollection<TInner[]>(QueueMaxSize);
                }
                this.m_resultSet = new BlockingCollection<TFinal[]>(4);
                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HGJ.ProcessAllItem";                
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TFinal[0];
                this.m_index = -1;
                this.m_disposed = false;
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;
                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the outer queues
                    foreach (var outerQueue in this.m_outerQueues)
                    {
                        foreach (var item in outerQueue.GetConsumingEnumerable())
                        {
                        }
                    }
                    // Always drain the inner queues
                    foreach (var innerQueue in this.m_innerQueues)
                    {
                        foreach (var item in innerQueue.GetConsumingEnumerable())
                        {
                        }
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel HashGroupJoin started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                                        Int32 wlen = this.m_workers.Length;
                    Int32[] counts = new Int32[wlen];

                    // Send the inner to the workers
                    TInner[][] innerBuffers = new TInner[wlen][];
                    for (int i = 0; i < wlen; i++)
                    {
                        innerBuffers[i] = new TInner[BufferSize];
                        counts[i] = 0;
                    }

                    foreach (TInner innerItem in this.m_inner)
                    {
                        TKey innerKey = this.m_innerKeySelector(innerItem);
                        Int32 hashCode = this.m_comparer.GetHashCode(innerKey);
                        Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, counts.Length);
                        if (counts[idx] == BufferSize)
                        {
                            if (this.m_isDone) break;
                            
                            this.m_innerQueues[idx].Add(innerBuffers[idx]);
                            innerBuffers[idx] = new TInner[BufferSize];
                            counts[idx] = 0;
                        }
                        innerBuffers[idx][counts[idx]] = innerItem;
                        counts[idx]++;
                    }

                    // Add the final buffers to the queues and declare adding is complete
                    for (int i = 0; i < wlen; i++)
                    {
                        if (!this.m_isDone && counts[i] > 0)
                        {
                            TInner[] lastBuffer = new TInner[counts[i]];
                            Array.Copy(innerBuffers[i], lastBuffer, counts[i]);
                            this.m_innerQueues[i].Add(lastBuffer);
                        }
                        this.m_innerQueues[i].CompleteAdding();
                    }
                    innerBuffers = null;
                        
                    // Send outer to the workers
                    TOuter[][] outerBuffers = new TOuter[wlen][];
                    for (int i = 0; i < outerBuffers.Length; i++)
                    {
                        outerBuffers[i] = new TOuter[BufferSize];
                        counts[i] = 0;
                    }
                    foreach (TOuter outerItem in this.m_outer)
                    {
                        TKey outerKey = this.m_outerKeySelector(outerItem);
                        Int32 hashCode = this.m_comparer.GetHashCode(outerKey);
                        Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, wlen);
                        if (counts[idx] == BufferSize)
                        {
                            if (this.m_isDone) break;

                            this.m_outerQueues[idx].Add(outerBuffers[idx]);
                            outerBuffers[idx] = new TOuter[BufferSize];
                            counts[idx] = 0;
                        }
                        outerBuffers[idx][counts[idx]] = outerItem;
                        counts[idx]++;
                    }
                    // Add the final buffers to the queues and declare adding is complete
                    for (int i = 0; i < wlen; i++)
                    {
                        if (!this.m_isDone && counts[i] > 0)
                        {
                            TOuter[] lastBuffer = new TOuter[counts[i]];
                            Array.Copy(outerBuffers[i], lastBuffer, counts[i]);
                            this.m_outerQueues[i].Add(lastBuffer);
                        }
                        this.m_outerQueues[i].CompleteAdding();
                    }
                    outerBuffers = null;
                    
                    DryadLinqLog.AddInfo("Parallel HashGroupJoin ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_outerQueues.Length; i++)
                    {
                        this.m_outerQueues[i].CompleteAdding();
                    }
                    for (int i = 0; i < this.m_innerQueues.Length; i++)
                    {
                        this.m_innerQueues[i].CompleteAdding();
                    }                    
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                // using the TCO.LongRunning option, because this method is called a fix number of times to spawn worker tasks
                // which means it is safe to request a decicated thread for this task
                return Task.Factory.StartNew(delegate { this.DoGroupJoin(idx); },
                                             TaskCreationOptions.LongRunning);
            }

            private void DoGroupJoin(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel HashGroupJoin worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    Int32 wlen = this.m_workers.Length;
                    TResult[] resultBuffer = new TResult[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = this.m_stealingWorkerCnt;

                    GroupingHashSet<TInner, TKey> innerGroups = new GroupingHashSet<TInner, TKey>(this.m_comparer);
                    foreach (var buffer in this.m_innerQueues[qidx].GetConsumingEnumerable())
                    {
                        if (this.m_isDone) return;
                        
                        for (int bidx = 0; bidx < buffer.Length; bidx++)
                        {
                            TInner innerItem = buffer[bidx];
                            TKey innerKey = this.m_innerKeySelector(innerItem);
                            innerGroups.AddItem(innerKey, innerItem);
                        }
                    }

                    DryadLinqLog.AddInfo("Parallel HashGroupJoin: In-memory hashtable using inner created at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    TInner[] emptyGroup = new TInner[0];
                    foreach (var buffer in this.m_outerQueues[qidx].GetConsumingEnumerable())
                    {
                        if (this.m_isDone) return;

                        for (int i = 0; i < buffer.Length; i++)
                        {
                            TOuter outerItem = buffer[i];
                            IEnumerable<TInner> innerGroup = innerGroups.GetGroup(this.m_outerKeySelector(outerItem));
                            if (innerGroup == null)
                            {
                                innerGroup = emptyGroup;
                            }
                            if (count == BufferSize)
                            {
                                if (this.m_applyFunc == null)
                                {
                                    this.m_resultSet.Add((TFinal[])(object)resultBuffer);
                                    resultBuffer = new TResult[BufferSize];
                                }
                                else
                                {
                                    int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                    if (newStealingWorkerCnt > stealingWorkerCnt)
                                    {
                                        myWorkCnt = 0;
                                        otherWorkCnt = 0;
                                        stealingWorkerCnt = newStealingWorkerCnt;
                                    }
                                    if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                    {
                                        this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                        myWorkCnt++;
                                    }
                                    else
                                    {
                                        this.m_stealingQueue.Add(resultBuffer);
                                        resultBuffer = new TResult[BufferSize];
                                        otherWorkCnt++;
                                    }
                                }
                                count = 0;
                            }
                            resultBuffer[count++] = this.m_resultSelector(outerItem, innerGroup);
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TResult[] lastResultBuffer = new TResult[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TFinal[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TResult[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInHashGroupJoin,
                                                                    SR.FailureInHashGroupJoin, e);
                    this.m_resultSet.Add(new TFinal[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel HashGroupJoin worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }
        
    internal class ParallelSetOperation<TSource, TResult> : IParallelPipeline<TResult>
    {
        private string m_opName;
        private IEnumerable<TSource> m_source;
        private IEnumerable<TSource> m_otherSource;
        private Func<IEnumerable<TSource>, IEnumerable<TResult>> m_applyFunc;
        private IEqualityComparer<TSource> m_comparer;
        private bool m_isPartial;

        public ParallelSetOperation(string opName,
				    IEnumerable<TSource> source,
				    IEnumerable<TSource> otherSource,
				    IEqualityComparer<TSource> comparer,
				    Func<IEnumerable<TSource>, IEnumerable<TResult>> applyFunc,
				    bool isPartial)
        {
            this.m_opName = opName;
            this.m_source = source;
            this.m_otherSource = otherSource;
            this.m_comparer = comparer;
            this.m_applyFunc = applyFunc;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TSource>.Default;
            }
            this.m_isPartial = isPartial;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TResult> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TFinal>
            Extend<TFinal>(Func<IEnumerable<TResult>, IEnumerable<TFinal>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<TSource>, IEnumerable<TFinal>>)(object)func;
                return new ParallelSetOperation<TSource, TFinal>(
                                 this.m_opName, this.m_source, this.m_otherSource, this.m_comparer, 
                                 applyFunc, this.m_isPartial);
            }
            else
            {
                return new ParallelSetOperation<TSource, TFinal>(
                                 this.m_opName, this.m_source, this.m_otherSource, this.m_comparer,
                                 s => func(this.m_applyFunc(s)), this.m_isPartial);
            }
        }
        
        private class InnerEnumerator : IEnumerator<TResult>
        {
            private const Int32 SetSize = (1 << 23);  // 8388608
            private const Int32 BufferSize = 2048;
            private const Int32 QueueMaxSize = 4;
            private const Int32 ResultQueueMaxSize = 8;
        
            private string m_opName;
            private IEnumerable<TSource> m_source;
            private IEnumerable<TSource> m_otherSource;
            private IEqualityComparer<TSource> m_comparer;
            private Func<IEnumerable<TSource>, IEnumerable<TResult>> m_applyFunc;
            private bool m_isPartial;
            
            private Thread m_mainWorker;
            private Task[] m_workers;
            private BlockingCollection<TSource[]>[] m_queues;
            private BlockingCollection<TResult[]> m_resultSet;
            private BlockingCollection<TSource[]> m_stealingQueue;
            private int m_stealingWorkerCnt;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<TResult[]> m_resultEnum;
            private TResult[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelSetOperation<TSource, TResult> parent)
            {
                this.m_opName = parent.m_opName;
                this.m_source = parent.m_source;
                this.m_otherSource = parent.m_otherSource;

                if (this.m_opName == "Except")
                {
                    this.m_source = parent.m_otherSource;
                    this.m_otherSource = parent.m_source;
                }
                else if (this.m_opName == "Intersect")
                {
                    if ((parent.m_source is DryadLinqVertexReader<TSource>) &&
                        (parent.m_otherSource is DryadLinqVertexReader<TSource>))
                    {
                        Int64 len1 = ((DryadLinqVertexReader<TSource>)parent.m_source).GetTotalLength();
                        Int64 len2 = ((DryadLinqVertexReader<TSource>)parent.m_otherSource).GetTotalLength();
                        if (len2 >= 0 && len1 > len2)
                        {
                            this.m_source = parent.m_otherSource;
                            this.m_otherSource = parent.m_source;
                        }
                        DryadLinqLog.AddInfo("Parallel " + this.m_opName + ": len1={0}, len2={1}", len1, len2);
                    }
                }

                this.m_comparer = parent.m_comparer;
                this.m_applyFunc = parent.m_applyFunc;
                this.m_isPartial = parent.m_isPartial;

                this.m_isDone = false;
                this.m_stealingWorkerCnt = 0;
                this.m_stealingQueue = new BlockingCollection<TSource[]>();
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_queues = new BlockingCollection<TSource[]>[this.m_workers.Length];
                for (int i = 0; i < this.m_queues.Length; i++)
                {
                    this.m_queues[i] = new BlockingCollection<TSource[]>(QueueMaxSize);
                }
                this.m_resultSet = new BlockingCollection<TResult[]>(ResultQueueMaxSize);
                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    this.m_workers[i] = this.CreateTask(i); 
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "SO.ProcessAllItem";
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultSet.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TResult[0];
                this.m_index = -1;
                this.m_disposed = false;
            }
            
            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;

                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TResult Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    foreach (var queue in this.m_queues)
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                        {
                            // Always drain the queues
                        }
                    }
                    while (this.m_resultEnum.MoveNext())
                    {
                        // Always drain the result queue
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel " + this.m_opName + " started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    Int32 wlen = this.m_workers.Length;
                    TSource[][] buffers = new TSource[wlen][];
                    Int32[] counts = new Int32[wlen];
                    for (int i = 0; i < wlen; i++)
                    {
                        buffers[i] = new TSource[BufferSize];
                        counts[i] = 0;
                    }
                    foreach (TSource item in this.m_source)
                    {
                        Int32 hashCode = this.m_comparer.GetHashCode(item);
                        Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, wlen);
                        if (counts[idx] == BufferSize)
                        {
                            if (this.m_isDone) break;

                            this.m_queues[idx].Add(buffers[idx]);
                            buffers[idx] = new TSource[BufferSize];
                            counts[idx] = 0;
                        }
                        buffers[idx][counts[idx]] = item;
                        counts[idx]++;
                    }

                    if (this.m_opName == "Intersect" || this.m_opName == "Except")
                    {
                        // Add the final buffers of m_source to queues
                        for (int i = 0; i < wlen; i++)
                        {
                            if (!this.m_isDone && counts[i] > 0)
                            {
                                TSource[] lastBuffer = new TSource[counts[i]];
                                Array.Copy(buffers[i], lastBuffer, counts[i]);
                                buffers[i] = null;
                                this.m_queues[i].Add(lastBuffer);
                                counts[i] = 0;
                            }
                            this.m_queues[i].Add(new TSource[0]);
                        }
                    }
                    if (this.m_otherSource != null)
                    {
                        foreach (TSource item in this.m_otherSource)
                        {
                            Int32 hashCode = this.m_comparer.GetHashCode(item);
                            Int32 idx = DryadLinqUtil.GetTaskIndex(hashCode, wlen);
                            if (counts[idx] == BufferSize)
                            {
                                if (this.m_isDone) break;

                                this.m_queues[idx].Add(buffers[idx]);
                                buffers[idx] = new TSource[BufferSize];
                                counts[idx] = 0;
                            }
                            buffers[idx][counts[idx]] = item;
                            counts[idx]++;
                        }
                    }
                    // Add the final buffers to the queues and declare adding is complete
                    for (int i = 0; i < wlen; i++)
                    {
                        if (!this.m_isDone && counts[i] > 0)
                        {
                            TSource[] lastBuffer = new TSource[counts[i]];
                            Array.Copy(buffers[i], lastBuffer, counts[i]);
                            buffers[i] = null;
                            this.m_queues[i].Add(lastBuffer);
                        }
                        this.m_queues[i].CompleteAdding();
                    }

                    DryadLinqLog.AddInfo("Parallel " + this.m_opName + " ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    for (int i = 0; i < this.m_queues.Length; i++)
                    {
                        this.m_queues[i].CompleteAdding();
                    }
                    this.m_resultSet.CompleteAdding();
                }
            }

            private Task CreateTask(Int32 idx)
            {
                if (this.m_opName == "Intersect")
                {
                    // using the TCO.LongRunning option, because this method is
                    // called a fix number of times to spawn worker tasks
                    return Task.Factory.StartNew(delegate { this.DoIntersect(idx); },
                                                 TaskCreationOptions.LongRunning);
                }
                else if (this.m_opName == "Except")
                {
                    // using the TCO.LongRunning option, because this method is
                    // called a fix number of times to spawn worker tasks
                    return Task.Factory.StartNew(delegate { this.DoExcept(idx); },
                                                 TaskCreationOptions.LongRunning);
                }
                else
                {
                    if (this.m_isPartial)
                    {
                        // using the TCO.LongRunning option, because this method is
                        // called a fix number of times to spawn worker tasks
                        return Task.Factory.StartNew(delegate { this.DoPartialDistinct(idx); },
                                                     TaskCreationOptions.LongRunning);
                    }
                    else
                    {
                        // using the TCO.LongRunning option, because this method is
                        // called a fix number of times to spawn worker tasks
                        return Task.Factory.StartNew(delegate { this.DoFullDistinct(idx); },
                                                     TaskCreationOptions.LongRunning);
                    }
                }
            }

            private void DoPartialDistinct(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel Distinct (partial) worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    BlockingCollection<TSource[]> queue = this.m_queues[qidx];
                    Int32 wlen = this.m_workers.Length;
                    TSource[] resultBuffer = new TSource[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = 0;
                    
                    if (typeof(TSource).IsValueType)
                    {
                        Pair<TSource, bool>[] seenSet = new Pair<TSource, bool>[SetSize];
                        foreach (var buffer in queue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int i = 0; i < buffer.Length; i++)
                            {
                                TSource item = buffer[i];
                                Int32 idx = (this.m_comparer.GetHashCode(item) & 0x7fffffff) % SetSize;
                                Pair<TSource, bool> seenItem = seenSet[idx];
                                if (!seenItem.Value || !this.m_comparer.Equals(item, seenItem.Key))
                                {
                                    seenSet[idx] = new Pair<TSource, bool>(item, true);
                                    if (count == BufferSize)
                                    {
                                        if (this.m_applyFunc == null)
                                        {
                                            this.m_resultSet.Add((TResult[])(object)resultBuffer);
                                            resultBuffer = new TSource[BufferSize];
                                        }
                                        else
                                        {
                                            int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                            if (newStealingWorkerCnt > stealingWorkerCnt)
                                            {
                                                myWorkCnt = 0;
                                                otherWorkCnt = 0;
                                                stealingWorkerCnt = newStealingWorkerCnt;
                                            }
                                            if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                            {
                                                this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                myWorkCnt++;
                                            }
                                            else
                                            {
                                                this.m_stealingQueue.Add(resultBuffer);
                                                resultBuffer = new TSource[BufferSize];
                                                otherWorkCnt++;
                                            }
                                        }
                                        count = 0;
                                    }
                                    resultBuffer[count++] = item;
                                }
                            }
                        }
                    }
                    else
                    {
                        TSource[] seenSet = new TSource[SetSize];
                        foreach (var buffer in queue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;

                            for (int i = 0; i < buffer.Length; i++)
                            {
                                TSource item = buffer[i];
                                Int32 idx = (this.m_comparer.GetHashCode(item) & 0x7fffffff) % SetSize;
                                TSource seenItem = seenSet[idx];
                                if (seenItem == null || !this.m_comparer.Equals(item, seenItem))
                                {
                                    seenSet[idx] = item;
                                    if (count == BufferSize)
                                    {
                                        if (this.m_applyFunc == null)
                                        {
                                            this.m_resultSet.Add((TResult[])(object)resultBuffer);
                                            resultBuffer = new TSource[BufferSize];
                                        }
                                        else
                                        {
                                            int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                            if (newStealingWorkerCnt > stealingWorkerCnt)
                                            {
                                                myWorkCnt = 0;
                                                otherWorkCnt = 0;
                                                stealingWorkerCnt = newStealingWorkerCnt;
                                            }
                                            if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                            {
                                                this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                myWorkCnt++;
                                            }
                                            else
                                            {
                                                this.m_stealingQueue.Add(resultBuffer);
                                                resultBuffer = new TSource[BufferSize];
                                                otherWorkCnt++;
                                            }
                                        }
                                        count = 0;
                                    }
                                    resultBuffer[count++] = item;
                                }
                            }
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TSource[] lastResultBuffer = new TSource[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TResult[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TSource[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInDistinct,
                                                                    SR.FailureInDistinct, e);
                    this.m_resultSet.Add(new TResult[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel Distinct (partial) worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }

            private void DoFullDistinct(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel " + this.m_opName + " worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                    
                    BlockingCollection<TSource[]> queue = this.m_queues[qidx];
                    BigHashSet<TSource> seenSet = new BigHashSet<TSource>(this.m_comparer);
                    // HashSet<TSource> seenSet = new HashSet<TSource>(this.m_comparer);

                    Int32 wlen = this.m_workers.Length;
                    TSource[] resultBuffer = new TSource[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = 0;

                    foreach (TSource[] buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) return;

                        for (int i = 0; i < buffer.Length; i++)
                        {
                            TSource item = buffer[i];
                            if (seenSet.Add(item))
                            {
                                if (count == BufferSize)
                                {
                                    if (this.m_applyFunc == null)
                                    {
                                        this.m_resultSet.Add((TResult[])(object)resultBuffer);
                                        resultBuffer = new TSource[BufferSize];
                                    }
                                    else
                                    {
                                        int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                        if (newStealingWorkerCnt > stealingWorkerCnt)
                                        {
                                            myWorkCnt = 0;
                                            otherWorkCnt = 0;
                                            stealingWorkerCnt = newStealingWorkerCnt;
                                        }
                                        if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                        {
                                            this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                            myWorkCnt++;
                                        }
                                        else
                                        {
                                            this.m_stealingQueue.Add(resultBuffer);
                                            resultBuffer = new TSource[BufferSize];
                                            otherWorkCnt++;
                                        }
                                    }
                                    count = 0;
                                }
                                resultBuffer[count++] = item;
                            }
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TSource[] lastResultBuffer = new TSource[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TResult[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TSource[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInOperator,
                                                                    String.Format(SR.FailureInOperator, this.m_opName), e);
                    this.m_resultSet.Add(new TResult[0]);
                    this.m_stealingQueue.CompleteAdding();
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel " + this.m_opName + " worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }

            private void DoExcept(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel Except worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                    
                    BlockingCollection<TSource[]> queue = this.m_queues[qidx];
                    BigHashSet<TSource> seenSet = new BigHashSet<TSource>(this.m_comparer);

                    Int32 wlen = this.m_workers.Length;
                    TSource[] resultBuffer = new TSource[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = 0;
                    bool isFirst = false;

                    foreach (TSource[] buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) return;

                        if (isFirst)
                        {
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                TSource item = buffer[i];
                                if (seenSet.Add(item))
                                {
                                    if (count == BufferSize)
                                    {
                                        if (this.m_applyFunc == null)
                                        {
                                            this.m_resultSet.Add((TResult[])(object)resultBuffer);
                                            resultBuffer = new TSource[BufferSize];
                                        }
                                        else
                                        {
                                            int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                            if (newStealingWorkerCnt > stealingWorkerCnt)
                                            {
                                                myWorkCnt = 0;
                                                otherWorkCnt = 0;
                                                stealingWorkerCnt = newStealingWorkerCnt;
                                            }
                                            if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                            {
                                                this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                myWorkCnt++;
                                            }
                                            else
                                            {
                                                this.m_stealingQueue.Add(resultBuffer);
                                                resultBuffer = new TSource[BufferSize];
                                                otherWorkCnt++;
                                            }
                                        }
                                        count = 0;
                                    }
                                    resultBuffer[count++] = item;
                                }
                            }
                        }
                        else
                        {
                            isFirst = (buffer.Length == 0);
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                seenSet.Add(buffer[i]);
                            }
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TSource[] lastResultBuffer = new TSource[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TResult[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TSource[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_stealingQueue.CompleteAdding();
                    throw new DryadLinqException(DryadLinqErrorCode.FailureInExcept,
                                                 String.Format(SR.FailureInExcept), e);
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel Except worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }

            private void DoIntersect(Int32 qidx)
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel Intersect worker {0} started at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    BlockingCollection<TSource[]> queue = this.m_queues[qidx];
                    BigHashSet<TSource> leftSet = new BigHashSet<TSource>(this.m_comparer);

                    Int32 wlen = this.m_workers.Length;
                    TSource[] resultBuffer = new TSource[BufferSize];
                    Int32 count = 0;
                    Int32 myWorkCnt = 0;
                    Int32 otherWorkCnt = 0;
                    Int32 stealingWorkerCnt = 0;
                    bool isFirst = true;

                    foreach (TSource[] buffer in queue.GetConsumingEnumerable())
                    {
                        if (this.m_isDone) return;

                        if (isFirst)
                        {
                            isFirst = (buffer.Length != 0);
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                leftSet.Add(buffer[i]);
                            }
                        }
                        else
                        {
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                TSource item = buffer[i];
                                if (leftSet.Remove(item))
                                {
                                    if (count == BufferSize)
                                    {
                                        if (this.m_applyFunc == null)
                                        {
                                            this.m_resultSet.Add((TResult[])(object)resultBuffer);
                                            resultBuffer = new TSource[BufferSize];
                                        }
                                        else
                                        {
                                            int newStealingWorkerCnt = this.m_stealingWorkerCnt;
                                            if (newStealingWorkerCnt > stealingWorkerCnt)
                                            {
                                                myWorkCnt = 0;
                                                otherWorkCnt = 0;
                                                stealingWorkerCnt = newStealingWorkerCnt;
                                            }
                                            if ((stealingWorkerCnt * myWorkCnt) <= ((wlen - stealingWorkerCnt) * otherWorkCnt))
                                            {
                                                this.m_resultSet.Add(this.m_applyFunc(resultBuffer).ToArray());
                                                myWorkCnt++;
                                            }
                                            else
                                            {
                                                this.m_stealingQueue.Add(resultBuffer);
                                                resultBuffer = new TSource[BufferSize];
                                                otherWorkCnt++;
                                            }
                                        }
                                        count = 0;
                                    }
                                    resultBuffer[count++] = item;
                                }
                            }
                        }
                    }

                    // Add the final buffer:
                    if (!this.m_isDone && count > 0)
                    {
                        TSource[] lastResultBuffer = new TSource[count];
                        Array.Copy(resultBuffer, lastResultBuffer, count);
                        resultBuffer = null;
                        if (this.m_applyFunc == null)
                        {
                            this.m_resultSet.Add((TResult[])(object)lastResultBuffer);
                        }
                        else
                        {
                            this.m_resultSet.Add(this.m_applyFunc(lastResultBuffer).ToArray());
                        }
                    }

                    // Stealing work
                    if (this.m_applyFunc != null)
                    {
                        stealingWorkerCnt = Interlocked.Increment(ref this.m_stealingWorkerCnt);
                        if (stealingWorkerCnt == wlen)
                        {
                            this.m_stealingQueue.CompleteAdding();
                        }
                        foreach (TSource[] buffer in this.m_stealingQueue.GetConsumingEnumerable())
                        {
                            if (this.m_isDone) return;
                            this.m_resultSet.Add(this.m_applyFunc(buffer).ToArray());
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_stealingQueue.CompleteAdding();
                    throw new DryadLinqException(DryadLinqErrorCode.FailureInIntersect,
                                                 String.Format(SR.FailureInIntersect), e);
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel Intersect worker {0} ended at {1}",
                                         qidx, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                }
            }
        }
    }

    internal class ParallelOrderedGroupBy<TSource, TKey, TElement, TResult, TFinal> : IParallelPipeline<TFinal>
    {
        private IEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private Func<TSource, TElement> m_elementSelector;
        private Func<TKey, IEnumerable<TElement>, TResult> m_resultSelector;
        private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
        private IEqualityComparer<TKey> m_comparer;
        
        public ParallelOrderedGroupBy(IEnumerable<TSource> source,
                                      Func<TSource, TKey> keySelector,
                                      Func<TSource, TElement> elementSelector,
                                      Func<TKey, IEnumerable<TElement>, TResult> resultSelector,
                                      IEqualityComparer<TKey> comparer,
                                      Func<IEnumerable<TResult>, IEnumerable<TFinal>> applyFunc)
        {
            if (resultSelector == null || elementSelector == null)
            {
                throw new InvalidOperationException();
            }
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_resultSelector = resultSelector;
            this.m_applyFunc = applyFunc;
            this.m_comparer = comparer;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<TResult>, IEnumerable<TLast>>)(object)func;
                return new ParallelOrderedGroupBy<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_resultSelector, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelOrderedGroupBy<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_resultSelector, this.m_comparer,
                                 s => func(this.m_applyFunc(s)));
            }
        }

        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 BufferSize = 16384;  // (1 << 14);

            private IEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private Func<TSource, TElement> m_elementSelector;
            private Func<TKey, IEnumerable<TElement>, TResult> m_resultSelector;
            private Func<IEnumerable<TResult>, IEnumerable<TFinal>> m_applyFunc;
            private IEqualityComparer<TKey> m_comparer;

            private Thread m_mainWorker;
            private Task[] m_workers;
            private EventWaitHandle[] m_events;
            private List<TFinal>[] m_workerResLists;
            private BlockingCollection<List<TFinal>> m_resultQueue;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<List<TFinal>> m_resultEnum;
            private List<TFinal> m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelOrderedGroupBy<TSource, TKey, TElement, TResult, TFinal> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_resultSelector = parent.m_resultSelector;
                this.m_applyFunc = parent.m_applyFunc;
                this.m_comparer = parent.m_comparer;

                this.m_isDone = false;
                this.m_workerException = null;
                this.m_resultQueue = new BlockingCollection<List<TFinal>>(2);
                this.m_workers = new Task[2 * Environment.ProcessorCount];
                this.m_events = new ManualResetEvent[this.m_workers.Length];
                this.m_workerResLists = new List<TFinal>[this.m_workers.Length];
                for (int i = 0; i < this.m_events.Length; i++)
                {
                    this.m_events[i] = new ManualResetEvent(true);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "OGB.ProcessAllItem";                
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultQueue.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new List<TFinal>();
                this.m_index = -1;
                this.m_disposed = false;
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Count)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;

                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Count > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new InvalidOperationException();
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;
                    while (this.m_resultEnum.MoveNext())
                    {
                        // Always drain the result queue
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    // Read all the items
                    DryadLinqLog.AddInfo("Parallel OrderedGroupBy started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                    Int32 wlen = this.m_workers.Length;
                    TSource[] elemBuffer = new TSource[BufferSize];
                    Int32 taskIdx = 0;
                    Int32 elemCnt = 0;
                    IEnumerator<TSource> sourceEnum = this.m_source.GetEnumerator();
                    while (sourceEnum.MoveNext())
                    {
                        TSource item = sourceEnum.Current;
                        if (elemCnt < BufferSize)
                        {
                            elemBuffer[elemCnt++] = item;
                        }
                        else
                        {
                            if (this.m_isDone) break;

                            TKey lastKey = this.m_keySelector(elemBuffer[BufferSize - 1]);
                            bool hasMoreItems = true;
                            if (this.m_comparer.Equals(lastKey, this.m_keySelector(item)))
                            {
                                List<TSource> lastItems = new List<TSource>(8);
                                lastItems.Add(item);
                                while (hasMoreItems = sourceEnum.MoveNext())
                                {
                                    item = sourceEnum.Current;
                                    if (!this.m_comparer.Equals(lastKey, this.m_keySelector(item)))
                                    {
                                        break;
                                    }
                                    lastItems.Add(item);
                                }
                                elemCnt = BufferSize + lastItems.Count();
                                TSource[] newElemBuffer = new TSource[elemCnt];
                                Array.Copy(elemBuffer, 0, newElemBuffer, 0, BufferSize);
                                lastItems.CopyTo(newElemBuffer, BufferSize);
                                elemBuffer = newElemBuffer;
                            }

                            this.CreateTask(taskIdx, elemBuffer, elemCnt);
                            taskIdx = (taskIdx + 1) % wlen;
                            elemCnt = 0;
                            if (!hasMoreItems) break;
                            elemBuffer = new TSource[BufferSize];
                            elemBuffer[elemCnt++] = item;
                        }
                    }
                            
                    // Create a task for the last buffer
                    if (!this.m_isDone && elemCnt > 0)
                    {
                        this.CreateTask(taskIdx, elemBuffer, elemCnt);
                        taskIdx = (taskIdx + 1) % wlen;
                    }
                    DryadLinqLog.AddInfo("Parallel OrderedGroupBy ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    for (int i = 0; i < this.m_workers.Length; i++)
                    {
                        this.m_events[taskIdx].WaitOne();
                        if (this.m_workerResLists[taskIdx] != null)
                        {
                            this.m_resultQueue.Add(this.m_workerResLists[taskIdx]);
                            this.m_workerResLists[taskIdx] = null;
                        }
                        taskIdx = (taskIdx + 1) % wlen;
                    }
                    foreach (Task task in this.m_workers)
                    {
                        if (task != null) task.Wait();
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    this.m_resultQueue.CompleteAdding();
                }
            }

            private void CreateTask(Int32 taskIdx, TSource[] elems, Int32 cnt)
            {
                this.m_events[taskIdx].WaitOne();
                if (this.m_workerResLists[taskIdx] != null)
                {
                    this.m_resultQueue.Add(this.m_workerResLists[taskIdx]);
                    this.m_workerResLists[taskIdx] = null;
                }
                this.m_events[taskIdx].Reset();

                // NOT using the TCO.LongRunning option for this task, because it's spawned
                // an arbitrary # of times for potentially shorter work which means it's best
                // to leave this to the TP load balancing algorithm
                this.m_workers[taskIdx] = Task.Factory.StartNew(
                                             delegate { this.OrderedGroupBy(taskIdx, elems, cnt); });
            }

            private void OrderedGroupBy(Int32 taskIdx, TSource[] elems, Int32 cnt)
            {
                try
                {
                    List<TResult> resList = new List<TResult>(cnt / 8);
                    Grouping<TKey, TElement> curGroup = new Grouping<TKey, TElement>(this.m_keySelector(elems[0]));
                    curGroup.AddItem(this.m_elementSelector(elems[0]));
                    Int32 idx = 1;
                    while (idx < cnt)
                    {
                        if (this.m_comparer.Equals(curGroup.Key, this.m_keySelector(elems[idx])))
                        {
                            curGroup.AddItem(this.m_elementSelector(elems[idx]));
                        }
                        else
                        {
                            resList.Add(this.m_resultSelector(curGroup.Key, curGroup));
                            curGroup = new Grouping<TKey, TElement>(this.m_keySelector(elems[idx]));
                            curGroup.AddItem(this.m_elementSelector(elems[idx]));
                        }
                        idx++;
                    }
                    resList.Add(this.m_resultSelector(curGroup.Key, curGroup));
                    if (this.m_applyFunc == null)
                    {
                        this.m_workerResLists[taskIdx] = (List<TFinal>)(object)resList;
                    }
                    else
                    {
                        List<TFinal> finalResList = new List<TFinal>(cnt/8);
                        foreach (var elem in this.m_applyFunc(resList))
                        {
                            finalResList.Add(elem);
                        }
                        this.m_workerResLists[taskIdx] = finalResList;
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInOrderedGroupBy,
                                                                    SR.FailureInOrderedGroupBy, e);
                    throw this.m_workerException;
                }
                finally
                {
                    this.m_events[taskIdx].Set();
                }
            }
        }
    }

    internal class ParallelOrderedGroupByAccumulate<TSource, TKey, TElement, TResult, TFinal>
        : IParallelPipeline<TFinal>
    {
        private IEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private Func<TSource, TElement> m_elementSelector;
        private Func<TElement, TResult> m_seed;
        private Func<TResult, TElement, TResult> m_accumulator;
        private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_applyFunc;
        private IEqualityComparer<TKey> m_comparer;
        
        public ParallelOrderedGroupByAccumulate(
                              IEnumerable<TSource> source,
                              Func<TSource, TKey> keySelector,
                              Func<TSource, TElement> elementSelector,
                              Func<TElement, TResult> seed,
                              Func<TResult, TElement, TResult> accumulator,
                              IEqualityComparer<TKey> comparer,
                              Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> applyFunc)
        {
            if (seed == null || accumulator == null || elementSelector == null)
            {
                throw new DryadLinqException("Internal error: The accumulator and element selector can't be null");
            }
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_elementSelector = elementSelector;
            this.m_seed = seed;
            this.m_accumulator = accumulator;
            this.m_applyFunc = applyFunc;
            this.m_comparer = comparer;
            if (this.m_comparer == null)
            {
                this.m_comparer = EqualityComparer<TKey>.Default;
            }
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TFinal> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TLast>
            Extend<TLast>(Func<IEnumerable<TFinal>, IEnumerable<TLast>> func, bool orderPreserving)
        {
            if (this.m_applyFunc == null)
            {
                var applyFunc = (Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TLast>>)(object)func;
                return new ParallelOrderedGroupByAccumulate<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_seed, this.m_accumulator, this.m_comparer, applyFunc);
            }
            else
            {
                return new ParallelOrderedGroupByAccumulate<TSource, TKey, TElement, TResult, TLast>(
                                 this.m_source, this.m_keySelector, this.m_elementSelector,
                                 this.m_seed, this.m_accumulator, this.m_comparer,
                                 s => func(this.m_applyFunc(s)));
            }
        }

        private class InnerEnumerator : IEnumerator<TFinal>
        {
            private const Int32 BufferSize = 16384;  // (1 << 14);

            private IEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private Func<TSource, TElement> m_elementSelector;
            private Func<TElement, TResult> m_seed;
            private Func<TResult, TElement, TResult> m_accumulator;
            private Func<IEnumerable<Pair<TKey, TResult>>, IEnumerable<TFinal>> m_applyFunc;
            private IEqualityComparer<TKey> m_comparer;

            private Thread m_mainWorker;
            private Task[] m_workers;
            private EventWaitHandle[] m_events;
            private List<TFinal>[] m_workerResLists;
            private BlockingCollection<List<TFinal>> m_resultQueue;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<List<TFinal>> m_resultEnum;
            private List<TFinal> m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelOrderedGroupByAccumulate<TSource, TKey, TElement, TResult, TFinal> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_elementSelector = parent.m_elementSelector;
                this.m_seed = parent.m_seed;
                this.m_accumulator = parent.m_accumulator;
                this.m_applyFunc = parent.m_applyFunc;
                this.m_comparer = parent.m_comparer;

                this.m_isDone = false;
                this.m_workerException = null;
                this.m_resultQueue = new BlockingCollection<List<TFinal>>(2);
                this.m_workers = new Task[2 * Environment.ProcessorCount];
                this.m_events = new ManualResetEvent[this.m_workers.Length];
                this.m_workerResLists = new List<TFinal>[this.m_workers.Length];
                for (int i = 0; i < this.m_events.Length; i++)
                {
                    this.m_events[i] = new ManualResetEvent(true);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "OGB.ProcessAllItem";                
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultQueue.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new List<TFinal>();
                this.m_index = -1;
                this.m_disposed = false;
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Count)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {
                    if (this.m_workerException != null) break;

                    this.m_currentItems = this.m_resultEnum.Current;
                    if (this.m_currentItems.Count > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("Failed while enumerating.", this.m_workerException);
                }
                return false;
            }

            public TFinal Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }
            
            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;
                    while (this.m_resultEnum.MoveNext())
                    {
                        // Always drain the result queue
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    // Read all the items
                    DryadLinqLog.AddInfo("Parallel OrderedGroupBy (Acc) started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    Int32 wlen = this.m_workers.Length;
                    TSource[] elemBuffer = new TSource[BufferSize];
                    Int32 taskIdx = 0;
                    Int32 elemCnt = 0;
                    IEnumerator<TSource> sourceEnum = this.m_source.GetEnumerator();
                    while (sourceEnum.MoveNext())
                    {
                        TSource item = sourceEnum.Current;
                        if (elemCnt < BufferSize)
                        {
                            elemBuffer[elemCnt++] = item;
                        }
                        else
                        {
                            if (this.m_isDone) break;

                            TKey lastKey = this.m_keySelector(elemBuffer[BufferSize - 1]);
                            TResult lastValue = default(TResult);
                            bool moreLast = this.m_comparer.Equals(lastKey, this.m_keySelector(item));
                            bool hasMoreItems = true;
                            if (moreLast)
                            {
                                Int32 idx = BufferSize - 2;
                                while (idx >= 0 && this.m_comparer.Equals(lastKey, this.m_keySelector(elemBuffer[idx])))
                                {
                                    idx--;
                                }
                                elemCnt = idx++;
                                lastValue = this.m_seed(this.m_elementSelector(elemBuffer[elemCnt]));
                                for (int i = elemCnt + 1; i < BufferSize; i++)
                                {
                                    lastValue = this.m_accumulator(lastValue, this.m_elementSelector(elemBuffer[i]));
                                }
                                while (hasMoreItems = sourceEnum.MoveNext())
                                {
                                    item = sourceEnum.Current;
                                    if (!this.m_comparer.Equals(lastKey, this.m_keySelector(item)))
                                    {
                                        break;
                                    }
                                    lastValue = this.m_accumulator(lastValue, this.m_elementSelector(item));
                                }
                            }

                            Pair<TKey, TResult> last = new Pair<TKey, TResult>(lastKey, lastValue);
                            this.CreateTask(taskIdx, elemBuffer, elemCnt, moreLast, last);
                            taskIdx = (taskIdx + 1) % wlen;
                            elemCnt = 0;
                            if (!hasMoreItems) break;
                            elemBuffer = new TSource[BufferSize];
                            elemBuffer[elemCnt++] = item;
                        }
                    }
                            
                    // Create a task for the last buffer
                    if (!this.m_isDone && elemCnt > 0)
                    {
                        this.CreateTask(taskIdx, elemBuffer, elemCnt, false,
                                        new Pair<TKey, TResult>(default(TKey), default(TResult)));
                        taskIdx = (taskIdx + 1) % wlen;
                    }

                    DryadLinqLog.AddInfo("Parallel OrderedGroupBy (Acc) ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    for (int i = 0; i < this.m_workers.Length; i++)
                    {
                        this.m_events[taskIdx].WaitOne();
                        if (this.m_workerResLists[taskIdx] != null)
                        {
                            this.m_resultQueue.Add(this.m_workerResLists[taskIdx]);
                            this.m_workerResLists[taskIdx] = null;
                        }
                        taskIdx = (taskIdx + 1) % wlen;
                    }
                    foreach (Task task in this.m_workers)
                    {
                        if (task != null) task.Wait();
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    this.m_resultQueue.CompleteAdding();
                }
            }

            private void CreateTask(Int32 taskIdx, TSource[] elems, Int32 cnt, bool moreLast, Pair<TKey, TResult> last)
            {
                this.m_events[taskIdx].WaitOne();
                if (this.m_workerResLists[taskIdx] != null)
                {
                    this.m_resultQueue.Add(this.m_workerResLists[taskIdx]);
                    this.m_workerResLists[taskIdx] = null;
                }
                this.m_events[taskIdx].Reset();

                // NOT using the TCO.LongRunning option for this task, because it's spawned
                // an arbitrary # of times for potentially shorter work which means it's best
                // to leave this to the TP load balancing algorithm
                this.m_workers[taskIdx] = Task.Factory.StartNew(
                                             delegate { this.OrderedGroupBy(taskIdx, elems, cnt, moreLast, last); });
            }

            private void OrderedGroupBy(Int32 taskIdx, TSource[] elems, Int32 cnt, bool moreLast, Pair<TKey, TResult> last)
            {
                try
                {
                    List<Pair<TKey, TResult>> resList = new List<Pair<TKey, TResult>>(cnt / 8);
                    if (cnt > 0)
                    {
                        TKey curKey = this.m_keySelector(elems[0]);
                        TResult curValue = this.m_seed(this.m_elementSelector(elems[0]));
                        Int32 idx = 1;
                        while (idx < cnt)
                        {
                            if (this.m_comparer.Equals(curKey, this.m_keySelector(elems[idx])))
                            {
                                curValue = this.m_accumulator(curValue, this.m_elementSelector(elems[idx]));
                            }
                            else
                            {
                                resList.Add(new Pair<TKey, TResult>(curKey, curValue));
                                curKey = this.m_keySelector(elems[idx]);
                                curValue = this.m_seed(this.m_elementSelector(elems[idx]));
                            }
                            idx++;
                        }
                        resList.Add(new Pair<TKey, TResult>(curKey, curValue));
                    }

                    // Add the last value:
                    if (moreLast)
                    {
                        resList.Add(new Pair<TKey, TResult>(last.Key, last.Value));
                    }

                    // Apply applyFunc:
                    if (this.m_applyFunc == null)
                    {
                        this.m_workerResLists[taskIdx] = (List<TFinal>)(object)resList;
                    }
                    else
                    {
                        List<TFinal> finalResList = new List<TFinal>(cnt/8);
                        foreach (var elem in this.m_applyFunc(resList))
                        {
                            finalResList.Add(elem);
                        }
                        this.m_workerResLists[taskIdx] = finalResList;
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInOrderedGroupBy,
                                                                    SR.FailureInOrderedGroupBy, e);
                    throw this.m_workerException;
                }
                finally
                {
                    this.m_events[taskIdx].Set();
                }
            }
        }
    }
    
    internal class ParallelSort<TElement, TKey> : IEnumerable<TElement>
    {
        private IEnumerable<TElement> m_source;
        private Func<TElement, TKey> m_keySelector;
        private IComparer<TKey> m_comparer;
        private bool m_isDescending;
        private bool m_isIdKeySelector;
        private DryadLinqFactory<TElement> m_elemFactory;

        public ParallelSort(IEnumerable<TElement> source,
                            Func<TElement, TKey> keySelector,
                            IComparer<TKey> comparer,
                            bool isDescending,
                            bool isIdKeySelector,
                            DryadLinqFactory<TElement> elemFactory)                                     
        {
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_comparer = TypeSystem.GetComparer<TKey>(comparer);
            if (isDescending)
            {
                this.m_comparer = MinusComparer<TKey>.Make(this.m_comparer);
                this.m_isDescending = false;
            }
            if (this.m_comparer is MinusComparer<TKey>)
            {
                this.m_isDescending = !this.m_isDescending;
                this.m_comparer = ((MinusComparer<TKey>)this.m_comparer).InnerComparer;
            }
            this.m_isIdKeySelector = isIdKeySelector;
            this.m_elemFactory = elemFactory;
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        private class InnerEnumerator : IEnumerator<TElement>
        {
            private const Int32 ChunkSize = (1 << 21);
            private const Int32 MergeSize = 16;

            private IEnumerable<TElement> m_source;
            private Func<TElement, TKey> m_keySelector;
            private IComparer<TKey> m_comparer;
            private bool m_isDescending;
            private bool m_isIdKeySelector;
            private DryadLinqFactory<TElement> m_elemFactory;

            private BlockingCollection<TElement[]> m_sourceQueue;
            private Thread m_mainWorker;
            private Task[] m_sortWorkers;

            private List<Task> m_mergeSortWorkers;
            private SortedChunkList m_chunkList;
            private List<SortedChunkList> m_mergeList;
            private volatile bool m_isDone;
            private Exception m_sorterException;

            private IEnumerator<TElement[]>[] m_enumArray;
            private IEnumerator<TElement> m_resultEnum;
            private bool m_disposed;

            public InnerEnumerator(ParallelSort<TElement, TKey> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_isDescending = parent.m_isDescending;
                this.m_comparer = parent.m_comparer;
                this.m_isIdKeySelector = parent.m_isIdKeySelector;
                this.m_elemFactory = parent.m_elemFactory;

                this.m_disposed = false;

                this.m_isDone = false;
                this.m_sorterException = null;
                this.m_sortWorkers = new Task[Environment.ProcessorCount];
                this.m_sourceQueue = new BlockingCollection<TElement[]>(4);

                this.m_mergeSortWorkers = new List<Task>(8);
                this.m_chunkList = new SortedChunkList(this);
                this.m_mergeList = new List<SortedChunkList>(8);

                // Start all the workers:
                for (int i = 0; i < this.m_sortWorkers.Length; i++)
                {
                    // using the TCO.LongRunning option, because we spawn a fixed number of tasks
                    // which means it is safe to request a decicated thread for each task
                    this.m_sortWorkers[i] = Task.Factory.StartNew(delegate { this.SortItemArray(); },
                                                                  TaskCreationOptions.LongRunning);
                }

                // Start main worker, and wait for it
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "S.ProcessAllItem";
                this.m_mainWorker.Start();
                this.m_mainWorker.Join();

                DryadLinqLog.AddInfo("Parallel mergesort workers all started at {0}, number of workers is {1}",
                                     DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"), this.m_mergeList.Count);

                this.m_enumArray = new IEnumerator<TElement[]>[this.m_mergeList.Count];
                for (int i = 0; i < this.m_enumArray.Length; i++)
                {
                    this.m_enumArray[i] = this.m_mergeList[i].GetEnumerator();
                }
                this.m_resultEnum = SortHelper.MergeSort(this.m_enumArray, this.m_keySelector,
                                                         this.m_comparer, this.m_isDescending);
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                return this.m_resultEnum.MoveNext();
            }

            public TElement Current
            {
                get { return this.m_resultEnum.Current; }
            }

            object IEnumerator.Current
            {
                get { return this.m_resultEnum.Current; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }

            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }

            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;
                }

                foreach (var item in this.m_sourceQueue.GetConsumingEnumerable())
                {
                    // Always drain the source queue
                }
                for (int i = 0; i < this.m_enumArray.Length; i++)
                {
                    // Always drain the queues of mergesort workers
                    this.m_enumArray[i].MoveNext();
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel sort started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    TElement[] itemArray = new TElement[ChunkSize];
                    Int32 itemCnt = 0;
                    Int32 chunkCnt = 0;
                    foreach (TElement item in this.m_source)
                    {
                        if (itemCnt == ChunkSize)
                        {
                            if (this.m_isDone) break;

                            this.m_sourceQueue.Add(itemArray);
                            chunkCnt++;
                            itemArray = new TElement[ChunkSize];
                            itemCnt = 0;
                        }
                        itemArray[itemCnt++] = item;
                    }

                    // Process the final buffer
                    if (!this.m_isDone && itemCnt > 0)
                    {
                        if (itemCnt != itemArray.Length)
                        {
                            TElement[] newItemArray = new TElement[itemCnt];
                            Array.Copy(itemArray, 0, newItemArray, 0, itemCnt);
                            itemArray = newItemArray;
                        }
                        this.m_sourceQueue.Add(itemArray);
                        chunkCnt++;
                    }
                    this.m_sourceQueue.CompleteAdding();

                    DryadLinqLog.AddInfo("Parallel sort ended reading at {0}, number of sorters is {1}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"), chunkCnt);
                    
                    // Wait for all the sort workers to complete
                    Task.WaitAll(this.m_sortWorkers);

                    // Mergesort the final chunk list
                    if (!this.m_isDone && this.m_chunkList.Count > 0)
                    {
                        this.MergeSortChunks(this.m_chunkList);
                        this.m_mergeList.Add(this.m_chunkList); // This is happening without a lock because all sort workers have now completed
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_sorterException = e;
                    lock (this.m_mergeList)
                    {
                        SortedChunkList dummyChunkList = new SortedChunkList(this);
                        this.m_mergeList.Add(dummyChunkList);
                        dummyChunkList.MergeSort();
                    }
                }
                finally
                {
                    this.m_sourceQueue.CompleteAdding();
                }
            }

            private unsafe void SortItemArray()
            {
                try
                {
                    DryadLinqLog.AddInfo("Parallel sort worker started at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    foreach (var itemArray in this.m_sourceQueue.GetConsumingEnumerable())
                    {
                        if (this.m_isIdKeySelector)
                        {
                            // TElement and TKey must be equal
                            Array.Sort(itemArray, 0, itemArray.Length, (IComparer<TElement>)this.m_comparer);
                        }
                        else
                        {
                            Int32 itemCnt = itemArray.Length;
                            TKey[] keyArray = new TKey[itemCnt];
                            for (int i = 0; i < itemCnt; i++)
                            {
                                keyArray[i] = this.m_keySelector(itemArray[i]);
                            }
                            Array.Sort(keyArray, itemArray, 0, itemArray.Length, this.m_comparer);
                        }
                        if (this.m_isDescending)
                        {
                            Array.Reverse(itemArray);
                        }

                        // Flush the current chunk to disk if memory is low
                        IEnumerable<TElement> sortedChunk = itemArray;
                        MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
                        memStatus.dwLength = (UInt32)sizeof(MEMORYSTATUSEX);
                        DryadLinqNative.GlobalMemoryStatusEx(ref memStatus);
                        if (this.m_elemFactory != null &&
                            DryadLinqNative.GlobalMemoryStatusEx(ref memStatus) &&
                            memStatus.ullAvailPhys < 1 * 1024 * 1024 * 1024UL)
                        {
                            sortedChunk = new FileEnumerable<TElement>(itemArray, this.m_elemFactory);

                            // It may be a while until we move on to the next foreach iteartion.
                            // Until that happens itemArray and all its entries remain rooted due
                            // to the loop scope and therfore won't be available for GC. However
                            // we've already backed them up in a file and no one else will refer
                            // to itemArray[*] any more, so we can reduce memory pressure by
                            // cleaning up the array
                            Array.Clear(itemArray, 0, itemArray.Length);
                        }

                        if (this.m_isDone) return;
                        
                        lock (this.m_mergeList)
                        {
                            if (this.m_chunkList.Count == MergeSize)
                            {
                                this.MergeSortChunks(this.m_chunkList);
                                this.m_mergeList.Add(this.m_chunkList);
                                this.m_chunkList = new SortedChunkList(this);
                            }
                            this.m_chunkList.Add(sortedChunk);
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    throw new DryadLinqException(DryadLinqErrorCode.FailureInSort,
                                                 String.Format(SR.FailureInSort), e);
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel sort worker ended at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }

            private void MergeSortChunks(SortedChunkList chunkList)
            {
                // NOT using the TCO.LongRunning option for this task, because it's
                // spawned an arbitrary # of times for potentially shorter work
                // which means it's best to leave this to the TP load balancing algorithm
                Task task = Task.Factory.StartNew(delegate { chunkList.MergeSort(); });
                this.m_mergeSortWorkers.Add(task);
            }

            private class SortedChunkList : IEnumerable<TElement[]>
            {
                private const Int32 ResultChunkSize = 4096;  // (1 << 12)

                private InnerEnumerator m_parent;
                private Func<TElement, TKey> m_keySelector;
                private IComparer<TKey> m_comparer;
                private bool m_isDescending;
                private IEnumerable<TElement>[] m_itemArrays;
                private Int32 m_count;
                private Exception m_mergerException;
                private BlockingCollection<TElement[]> m_resultQueue;

                public SortedChunkList(InnerEnumerator parent)
                {
                    this.m_parent = parent;
                    this.m_keySelector = parent.m_keySelector;
                    this.m_comparer = parent.m_comparer;
                    this.m_isDescending = parent.m_isDescending;
                    this.m_itemArrays = new IEnumerable<TElement>[MergeSize];
                    this.m_count = 0;
                    this.m_mergerException = null;
                    this.m_resultQueue = new BlockingCollection<TElement[]>(2);
                }

                public void Add(IEnumerable<TElement> itemArray)
                {
                    if (this.m_count == this.m_itemArrays.Length)
                    {
                        TElement[][] newItemArrays = new TElement[this.m_count * 2][];
                        Array.Copy(this.m_itemArrays, 0, newItemArrays, 0, this.m_count);
                        this.m_itemArrays = newItemArrays;
                    }
                    this.m_itemArrays[this.m_count++] = itemArray;
                }

                public Int32 Count
                {
                    get { return this.m_count; }
                }

                public void MergeSort()
                {
                    try
                    {
                        if (this.m_parent.m_sorterException != null)
                        {
                            this.m_mergerException = this.m_parent.m_sorterException;
                            this.m_resultQueue.Add(new TElement[0]);
                        }
                        else
                        {
                            IEnumerator<TElement>[] enumArray = new IEnumerator<TElement>[this.m_count];
                            TKey[] keys = new TKey[this.m_count];
                            Int32 mergeCnt = this.m_count;
                            for (int i = 0; i < this.m_count; i++)
                            {
                                IEnumerator<TElement> itemArrayEnum = this.m_itemArrays[i].GetEnumerator();
                                if (!itemArrayEnum.MoveNext())
                                {
                                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                                 SR.SortedChunkCannotBeEmpty);
                                }
                                enumArray[i] = itemArrayEnum;
                                keys[i] = this.m_keySelector(itemArrayEnum.Current);
                            }

                            TElement[] resultChunk = new TElement[ResultChunkSize];
                            Int32 resultChunkIdx = 0;
                            while (mergeCnt > 1)
                            {
                                TKey key = keys[0];
                                int idx = 0;
                                for (int i = 1; i < mergeCnt; i++)
                                {
                                    int cmp = this.m_comparer.Compare(key, keys[i]);
                                    cmp = (this.m_isDescending) ? -cmp : cmp;
                                    if (cmp > 0)
                                    {
                                        key = keys[i];
                                        idx = i;
                                    }
                                }

                                if (resultChunkIdx == ResultChunkSize)
                                {
                                    if (this.m_parent.m_isDone) break;

                                    this.m_resultQueue.Add(resultChunk);
                                    resultChunk = new TElement[ResultChunkSize];
                                    resultChunkIdx = 0;
                                }
                                resultChunk[resultChunkIdx++] = enumArray[idx].Current;

                                if (enumArray[idx].MoveNext())
                                {
                                    keys[idx] = this.m_keySelector(enumArray[idx].Current);
                                }
                                else
                                {
                                    mergeCnt--;
                                    if (idx < mergeCnt)
                                    {
                                        enumArray[idx] = enumArray[mergeCnt];
                                        keys[idx] = keys[mergeCnt];
                                    }
                                }
                            }

                            if (mergeCnt == 1)
                            {
                                IEnumerator<TElement> enum0 = enumArray[0];
                                do
                                {
                                    if (resultChunkIdx == ResultChunkSize)
                                    {
                                        if (this.m_parent.m_isDone) break;

                                        this.m_resultQueue.Add(resultChunk);
                                        resultChunk = new TElement[ResultChunkSize];
                                        resultChunkIdx = 0;
                                    }
                                    resultChunk[resultChunkIdx++] = enum0.Current;
                                }
                                while (enum0.MoveNext());
                            }

                            // Get rid of those item arrays
                            for (int i = 0; i < this.m_count; i++)
                            {
                                this.m_itemArrays[i] = null;
                            }

                            // Add the final chunk
                            if (!this.m_parent.m_isDone && resultChunkIdx > 0)
                            {
                                if (resultChunkIdx != ResultChunkSize)
                                {
                                    TElement[] lastResultChunk = new TElement[resultChunkIdx];
                                    Array.Copy(resultChunk, 0, lastResultChunk, 0, resultChunkIdx);
                                    resultChunk = lastResultChunk;
                                }
                                this.m_resultQueue.Add(resultChunk);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        this.m_mergerException = e;
                        this.m_resultQueue.Add(new TElement[0]);
                    }
                    finally
                    {
                        // Always declare the adding is complete.
                        this.m_resultQueue.CompleteAdding();
                    }
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return this.GetEnumerator();
                }

                public IEnumerator<TElement[]> GetEnumerator()
                {
                    foreach (var res in this.m_resultQueue.GetConsumingEnumerable())
                    {
                        if (res.Length == 0)
                        {
                            this.m_parent.m_isDone = true;
                            throw new DryadLinqException("ParallelSort.SortedChunkList failed.", this.m_mergerException);
                        }
                        yield return res;
                    }
                }
            }
        }
    }

    internal class ParallelMergeSort<TSource, TKey> : IEnumerable<TSource>
    {
        private IMultiEnumerable<TSource> m_source;
        private Func<TSource, TKey> m_keySelector;
        private IComparer<TKey> m_comparer;
        private bool m_isDescending;

        public ParallelMergeSort(IMultiEnumerable<TSource> source,
                                 Func<TSource, TKey> keySelector,
                                 IComparer<TKey> comparer,
                                 bool isDescending)
        {
            this.m_source = source;
            this.m_keySelector = keySelector;
            this.m_comparer = TypeSystem.GetComparer<TKey>(comparer);
            if (isDescending)
            {
                this.m_comparer = MinusComparer<TKey>.Make(this.m_comparer);
                this.m_isDescending = false;
            }
            if (this.m_comparer is MinusComparer<TKey>)
            {
                this.m_isDescending = !this.m_isDescending;
                this.m_comparer = ((MinusComparer<TKey>)this.m_comparer).InnerComparer;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TSource> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        private class InnerEnumerator : IEnumerator<TSource>
        {
            // This looks a lot like what I did in SkyServerQ18.
            private const Int32 MergeSize = 16;

            private IMultiEnumerable<TSource> m_source;
            private Func<TSource, TKey> m_keySelector;
            private IComparer<TKey> m_comparer;
            private bool m_isDescending;
            private DryadLinqRecordReader<TSource>[] m_readers;
            private List<Task> m_mergeSortWorkers;
            private List<SubrangeReader> m_mergeList;

            private volatile bool m_isDone;
            private IEnumerator<TSource[]>[] m_enumArray;
            private IEnumerator<TSource> m_resultEnum;
            private bool m_disposed;

            public InnerEnumerator(ParallelMergeSort<TSource, TKey> parent)
            {
                this.m_source = parent.m_source;
                this.m_keySelector = parent.m_keySelector;
                this.m_comparer = parent.m_comparer;
                this.m_isDescending = parent.m_isDescending;

                this.m_readers = new DryadLinqRecordReader<TSource>[this.m_source.NumberOfInputs];
                for (int i = 0; i < this.m_readers.Length; i++)
                {
                    this.m_readers[i] = (DryadLinqRecordReader<TSource>)this.m_source[i];
                }
                this.m_mergeSortWorkers = new List<Task>();
                this.m_mergeList = new List<SubrangeReader>();

                // Start mergesort workers:
                Int32 readerCnt = this.m_readers.Length;
                Int32 startIdx = 0;
                while (startIdx < readerCnt)
                {
                    Int32 size = Math.Min(MergeSize, readerCnt - startIdx);
                    SubrangeReader subReaders = new SubrangeReader(this, startIdx, size);
                    this.m_mergeList.Add(subReaders);
                    this.StartMergeSortWorker(subReaders);
                    startIdx += size;
                }

                this.m_enumArray = new IEnumerator<TSource[]>[this.m_mergeList.Count];
                for (int i = 0; i < this.m_enumArray.Length; i++)
                {
                    this.m_enumArray[i] = this.m_mergeList[i].GetEnumerator();
                }
                this.m_resultEnum = SortHelper.MergeSort(this.m_enumArray, this.m_keySelector,
                                                         this.m_comparer, this.m_isDescending);
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                return this.m_resultEnum.MoveNext();
            }

            public TSource Current
            {
                get {
                    return this.m_resultEnum.Current;
                }
            }

            object IEnumerator.Current
            {
                get {
                    return this.m_resultEnum.Current;
                }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }

            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }

            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;
                }
                // Always drain the queues of mergesort workers
                for (int i = 0; i < this.m_enumArray.Length; i++)
                {
                    this.m_enumArray[i].MoveNext();
                }
            }

            private void StartMergeSortWorker(SubrangeReader subReaders)
            {
                // NOT using the TCO.LongRunning option for this task, because it's
                // spawned an arbitrary # of times for potentially shorter work
                // which means it's best to leave this to the TP load balancing algorithm
                Task task = Task.Factory.StartNew(delegate { subReaders.MergeSort(); });
                this.m_mergeSortWorkers.Add(task);
            }

            private class SubrangeReader : IEnumerable<TSource[]>
            {
                private const Int32 ChunkSize = 4096;  // (1 << 12)

                private InnerEnumerator m_parent;
                private Int32 m_startIdx;
                private IEnumerator<TSource>[] m_enumerators;
                private Func<TSource, TKey> m_keySelector;
                private IComparer<TKey> m_comparer;
                private bool m_isDescending;
                private Exception m_mergerException;
                private BlockingCollection<TSource[]> m_resultQueue;

                public SubrangeReader(InnerEnumerator parent,
                                      Int32 startIdx,
                                      Int32 len)
                {
                    this.m_parent = parent;
                    this.m_startIdx = startIdx;
                    this.m_enumerators= new IEnumerator<TSource>[len];
                    for (int i = 0; i < len; i++)
                    {
                        this.m_enumerators[i] = parent.m_readers[startIdx + i].GetEnumerator();
                    }
                    this.m_keySelector = parent.m_keySelector;
                    this.m_comparer = parent.m_comparer;
                    this.m_isDescending = parent.m_isDescending;
                    this.m_resultQueue = new BlockingCollection<TSource[]>(2);
                }

                public void MergeSort()
                {
                    try
                    {
                        DryadLinqLog.AddInfo("ParallelMergeSort.SubrangeReader({0}) started at {1}",
                                             this.m_startIdx,
                                             DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                        TSource[] elems = new TSource[this.m_enumerators.Length];
                        TKey[] keys = new TKey[this.m_enumerators.Length];
                        int lastIdx = m_enumerators.Length - 1;
                        int readerCnt = 0;
                        while (readerCnt <= lastIdx)
                        {
                            
                            if (this.m_enumerators[readerCnt].MoveNext())
                            {
                                elems[readerCnt] = this.m_enumerators[readerCnt].Current;
                                keys[readerCnt] = this.m_keySelector(elems[readerCnt]);
                                readerCnt++;
                            }
                            else
                            {
                                this.m_enumerators[readerCnt].Dispose();
                                
                                if (readerCnt == lastIdx) break;
                                this.m_enumerators[readerCnt] = this.m_enumerators[lastIdx];
                                lastIdx--;
                            }
                        }

                        TSource[] resultChunk = new TSource[ChunkSize];
                        Int32 resultChunkIdx = 0;
                        while (readerCnt > 1)
                        {
                            TKey key = keys[0];
                            int idx = 0;
                            for (int i = 1; i < readerCnt; i++)
                            {
                                int cmp = this.m_comparer.Compare(key, keys[i]);
                                cmp = (this.m_isDescending) ? -cmp : cmp;
                                if (cmp > 0)
                                {
                                    key = keys[i];
                                    idx = i;
                                }
                            }

                            if (resultChunkIdx == ChunkSize)
                            {
                                if (this.m_parent.m_isDone) break;

                                this.m_resultQueue.Add(resultChunk);
                                resultChunk = new TSource[ChunkSize];
                                resultChunkIdx = 0;
                            }
                            resultChunk[resultChunkIdx++] = elems[idx];

                            if (this.m_enumerators[idx].MoveNext())
                            {
                                elems[idx] = this.m_enumerators[idx].Current;
                                keys[idx] = this.m_keySelector(elems[idx]);
                            }
                            else
                            {
                                this.m_enumerators[idx].Dispose();
                                
                                readerCnt--;
                                if (idx < readerCnt)
                                {
                                    this.m_enumerators[idx] = this.m_enumerators[readerCnt];
                                    elems[idx] = elems[readerCnt];
                                    keys[idx] = keys[readerCnt];
                                }
                            }
                        }

                        if (!this.m_parent.m_isDone && readerCnt == 1)
                        {
                            TSource elem = elems[0];
                            IEnumerator<TSource> enumerator = this.m_enumerators[0];
                            do
                            {
                                if (resultChunkIdx == ChunkSize)
                                {
                                    this.m_resultQueue.Add(resultChunk);
                                    resultChunk = new TSource[ChunkSize];
                                    resultChunkIdx = 0;
                                }
                                resultChunk[resultChunkIdx++] = elem;
                                if(!enumerator.MoveNext()){
                                    break;
                                }
                                elem = enumerator.Current;
                            }
                            while (true);
                            enumerator.Dispose();
                        }
                        
                        // Add the final chunk
                        if (!this.m_parent.m_isDone && resultChunkIdx > 0)
                        {
                            if (resultChunkIdx != ChunkSize)
                            {
                                TSource[] lastResultChunk = new TSource[resultChunkIdx];
                                Array.Copy(resultChunk, 0, lastResultChunk, 0, resultChunkIdx);
                                resultChunk = lastResultChunk;
                            }
                            this.m_resultQueue.Add(resultChunk);
                        }
                    }
                    catch (Exception e)
                    {
                        this.m_mergerException = e;
                        this.m_resultQueue.Add(new TSource[0]);
                    }
                    finally
                    {
                        DryadLinqLog.AddInfo("ParallelMergeSort.SubrangeReader({0}) ended at {1}",
                                             this.m_startIdx,
                                             DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                        
                        // Always declare the adding is complete.
                        this.m_resultQueue.CompleteAdding();
                    }
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return this.GetEnumerator();
                }

                public IEnumerator<TSource[]> GetEnumerator()
                {
                    foreach (var res in this.m_resultQueue.GetConsumingEnumerable())
                    {
                        if (res.Length == 0)
                        {
                            this.m_parent.m_isDone = true;
                            throw new DryadLinqException("ParallelMergeSort failed.", this.m_mergerException);
                        }
                        yield return res;
                    }
                }
            }
        }
    }

    internal interface IParallelPipeline<TResult> : IEnumerable<TResult>
    {
        IParallelPipeline<TFinal> Extend<TFinal>(Func<IEnumerable<TResult>, IEnumerable<TFinal>> func,
                                                 bool orderPreserving);
    }

    internal interface IParallelApply<TResult>
    {
        IEnumerable<Pair<TKey, TFinal>> ExtendGroupBy<TKey, TElement, TFinal>(
                                               Func<TResult, TKey> keySelector,
                                               Func<TResult, TElement> elementSelector,
                                               Func<TElement, TFinal> seed,
                                               Func<TFinal, TElement, TFinal> accumulator,
                                               IEqualityComparer<TKey> comparer);
    }

    internal class ParallelApply<TSource, TResult> : IParallelApply<TResult>, IParallelPipeline<TResult>
    {
        private IEnumerable<TSource> m_source;
        private Func<IEnumerable<TSource>, IEnumerable<TResult>> m_procFunc;
        private bool m_orderPreserving;
        
        public ParallelApply(IEnumerable<TSource> source,
                             Func<IEnumerable<TSource>, IEnumerable<TResult>> procFunc,
                             bool orderPreserving)
        {
            this.m_source = source;
            this.m_procFunc = procFunc;
            this.m_orderPreserving = orderPreserving;
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TResult> GetEnumerator()
        {
            return new InnerEnumerator(this);
        }

        public IParallelPipeline<TFinal>
            Extend<TFinal>(Func<IEnumerable<TResult>, IEnumerable<TFinal>> func, bool orderPreserving)
        {
            return new ParallelApply<TSource, TFinal>(this.m_source,
                                                      s => func(this.m_procFunc(s)),
                                                      this.m_orderPreserving && orderPreserving);
        }

        public IEnumerable<Pair<TKey, TFinal>>
            ExtendGroupBy<TKey, TElement, TFinal>(
                                  Func<TResult, TKey> keySelector,
                                  Func<TResult, TElement> elementSelector,
                                  Func<TElement, TFinal> seed,
                                  Func<TFinal, TElement, TFinal> accumulator,
                                  IEqualityComparer<TKey> comparer)
        {
            return new ParallelHashGroupByPartialAccumulate<TSource, TResult, TKey, TElement, TFinal>(
                              this.m_source, this.m_procFunc, keySelector, elementSelector, seed, accumulator, comparer);

        }

        private class InnerEnumerator : IEnumerator<TResult>
        {
            private static Int32[] ChunkSizes = new Int32[] { 2, 4, 4, 8, 8, 64, 512, 1024, 2048, 4096 };
            private static Int32 ResultBufferSize = 4096;

            private IEnumerable<TSource> m_source;
            private Func<IEnumerable<TSource>, IEnumerable<TResult>> m_procFunc;
            private bool m_orderPreserving;

            private int m_maxQueueSize;
            private BlockingCollection<Pair<TSource[], Wrapper<TResult[]>>> m_sourceQueue;
            private BlockingCollection<Wrapper<TResult[]>> m_resultQueue;
            private Thread m_mainWorker;
            private Task[] m_workers;
            private volatile bool m_isDone;
            private Exception m_workerException;

            private IEnumerator<Wrapper<TResult[]>> m_resultEnum;
            private TResult[] m_currentItems;
            private int m_index;
            private bool m_disposed;

            public InnerEnumerator(ParallelApply<TSource, TResult> parent)
            {
                this.m_source = parent.m_source;
                this.m_procFunc = parent.m_procFunc;
                this.m_orderPreserving = parent.m_orderPreserving;
                this.m_isDone = false;
                this.m_workerException = null;
                this.m_workers = new Task[Environment.ProcessorCount];
                this.m_maxQueueSize = Math.Max(4, this.m_workers.Length);
                this.m_sourceQueue = new BlockingCollection<Pair<TSource[], Wrapper<TResult[]>>>(this.m_maxQueueSize);
                this.m_resultQueue = new BlockingCollection<Wrapper<TResult[]>>(this.m_workers.Length*2);

                // Start all the workers:
                for (int i = 0; i < this.m_workers.Length; i++)
                {
                    // using the TCO.LongRunning option, because we spawn a fixed number of tasks
                    // which means it is safe to request a decicated thread for each task
                    this.m_workers[i] = Task.Factory.StartNew(delegate { this.ApplyFunc(); },
                                                              TaskCreationOptions.LongRunning);
                }
                this.m_mainWorker = new Thread(this.ProcessAllItems);
                this.m_mainWorker.Name = "HA.ProcessAllItem";
                this.m_mainWorker.Start();

                this.m_resultEnum = this.m_resultQueue.GetConsumingEnumerable().GetEnumerator();
                this.m_currentItems = new TResult[0];
                this.m_index = -1;
                this.m_disposed = false;
            }

            ~InnerEnumerator()
            {
                this.Dispose(false);
            }

            public bool MoveNext()
            {
                this.m_index++;
                if (this.m_index < this.m_currentItems.Length)
                {
                    return true;
                }

                while (this.m_resultEnum.MoveNext())
                {       
                    var wrapperItem = this.m_resultEnum.Current;
                    if (wrapperItem.item == null)
                    {
                        lock (wrapperItem)
                        {
                            while (wrapperItem.item == null)
                            {
                                Monitor.Wait(wrapperItem);
                            }
                        }
                    }

                    if (this.m_workerException != null) break;

                    this.m_currentItems = wrapperItem.item;
                    if (this.m_currentItems.Length > 0)
                    {
                        this.m_index = 0;
                        return true;
                    }
                }
                if (this.m_workerException != null)
                {
                    throw new DryadLinqException("ParallelApply failed.", this.m_workerException);
                }
                return false;
            }

            public TResult Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            object IEnumerator.Current
            {
                get { return this.m_currentItems[this.m_index]; }
            }

            public void Reset()
            {
                throw new DryadLinqException("Internal error: Cannot reset this IEnumerator.");
            }

            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }
            
            private void Dispose(bool disposing)
            {
                if (!this.m_disposed)
                {
                    this.m_disposed = true;
                    this.m_isDone = true;

                    // Always drain the source queue
                    Pair<TSource[], Wrapper<TResult[]>> sourceElem;                    
                    for (int i = 0; i < this.m_maxQueueSize; i++)
                    {
                        if (!this.m_sourceQueue.TryTake(out sourceElem)) break;
                    }

                    // Always drain the result queue
                    while (this.m_resultEnum.MoveNext())
                    {
                    }
                }
            }

            private void ProcessAllItems()
            {
                try
                {
                    DryadLinqLog.AddInfo("ParallelApply started reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Read all the items
                    Int32 wlen = this.m_workers.Length;
                    Int32 bufferSize = ChunkSizes[0] * wlen;
                    Int32 rateIdx = 1;
                    TSource[] elemBuffer = new TSource[bufferSize];
                    Int32 elemIdx = 0;
                    foreach (TSource elem in this.m_source)
                    {
                        if (elemIdx == bufferSize)
                        {
                            if (this.m_isDone) break;

                            Int32 chunkSize = bufferSize / wlen;
                            for (int i = 0; i < wlen; i++)
                            {
                                TSource[] chunk = new TSource[chunkSize];
                                Array.Copy(elemBuffer, chunkSize * i, chunk, 0, chunkSize);

                                if (this.m_orderPreserving)
                                {
                                    Wrapper<TResult[]> res = new Wrapper<TResult[]>(null);
                                    this.m_sourceQueue.Add(new Pair<TSource[], Wrapper<TResult[]>>(chunk, res));
                                    this.m_resultQueue.Add(res);
                                }
                                else
                                {
                                    this.m_sourceQueue.Add(new Pair<TSource[], Wrapper<TResult[]>>(chunk, null));
                                }
                            }
                            if (rateIdx < ChunkSizes.Length)
                            {
                                bufferSize = ChunkSizes[rateIdx] * wlen;
                                elemBuffer = new TSource[bufferSize];
                                rateIdx++;
                            }
                            elemIdx = 0;
                        }
                        elemBuffer[elemIdx++] = elem;
                    }

                    // Add the last buffer.
                    if (!this.m_isDone && elemIdx > 0)
                    {
                        Int32 chunkSize = elemIdx / wlen;
                        Int32 remainingCnt = elemIdx % wlen;
                        elemIdx = 0;
                        for (int i = 0; i < wlen; i++)
                        {
                            Int32 realChunkSize = (i < remainingCnt) ? chunkSize + 1 : chunkSize;
                            if (realChunkSize == 0) break;

                            TSource[] chunk = new TSource[realChunkSize];
                            Array.Copy(elemBuffer, elemIdx, chunk, 0, realChunkSize);
                            elemIdx += realChunkSize;

                            if (this.m_orderPreserving)
                            {
                                Wrapper<TResult[]> res = new Wrapper<TResult[]>(null);
                                this.m_sourceQueue.Add(new Pair<TSource[], Wrapper<TResult[]>>(chunk, res));
                                this.m_resultQueue.Add(res);
                            }
                            else
                            {
                                this.m_sourceQueue.Add(new Pair<TSource[], Wrapper<TResult[]>>(chunk, null));
                            }
                        }
                    }

                    // Now, the adding is complete.            
                    this.m_sourceQueue.CompleteAdding();

                    DryadLinqLog.AddInfo("ParallelApply ended reading at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    // Wait for all the workers to complete
                    Task.WaitAll(this.m_workers);
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = e;
                }
                finally
                {
                    this.m_sourceQueue.CompleteAdding();
                    this.m_resultQueue.CompleteAdding();
                }
            }

            private void ApplyFunc()
            {
                Wrapper<TResult[]> wrapperItem = null;
                try
                {
                    DryadLinqLog.AddInfo("Parallel Apply worker started at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));

                    foreach (var item in this.m_sourceQueue.GetConsumingEnumerable())
                    {
                        wrapperItem = item.Value;
                        IEnumerable<TResult> res = this.m_procFunc(item.Key);
                        TResult[] res1 = res as TResult[];

                        if (this.m_orderPreserving)
                        {
                            if (res1 == null)
                            {
                                res1 = res.ToArray();
                            }
                            lock (wrapperItem)
                            {
                                wrapperItem.item = res1;
                                Monitor.Pulse(wrapperItem);
                            }
                        }
                        else
                        {
                            if (res1 == null)
                            {
                                TResult[] buffer = new TResult[ResultBufferSize];
                                int cnt = 0;
                                foreach (var elem in res)
                                {
                                    if (cnt == ResultBufferSize)
                                    {
                                        this.m_resultQueue.Add(new Wrapper<TResult[]>(buffer));
                                        buffer = new TResult[ResultBufferSize];
                                        cnt = 0;
                                    }
                                    buffer[cnt++] = elem;
                                }
                                if (cnt > 0)
                                {
                                    if (cnt != ResultBufferSize)
                                    {
                                        TResult[] buffer1 = new TResult[cnt];
                                        Array.Copy(buffer, buffer1, cnt);
                                        buffer = buffer1;
                                    }
                                    this.m_resultQueue.Add(new Wrapper<TResult[]>(buffer));
                                }
                            }
                            else
                            {
                                this.m_resultQueue.Add(new Wrapper<TResult[]>(res1));
                            }
                        }
                        if (this.m_isDone) return;
                    }
                }
                catch (Exception e)
                {
                    this.m_isDone = true;
                    this.m_workerException = new DryadLinqException(DryadLinqErrorCode.FailureInUserApplyFunction,
                                                                    SR.FailureInUserApplyFunction, e);
                    if (this.m_orderPreserving)
                    {
                        if (wrapperItem.item == null)
                        {
                            lock (wrapperItem)
                            {
                                wrapperItem.item = new TResult[0];
                                Monitor.Pulse(wrapperItem);
                            }
                        }
                    }
                    else
                    {
                        this.m_resultQueue.Add(new Wrapper<TResult[]>(new TResult[0]));
                    }
                }
                finally
                {
                    DryadLinqLog.AddInfo("Parallel Apply worker ended at {0}",
                                         DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"));
                }
            }
        }
    }

    internal class SortHelper
    {
        internal static IEnumerator<TElement>
            MergeSort<TElement, TKey>(List<TKey[]> keyArrayList,
                                      List<TElement[]> itemArrayList,
                                      List<Int32> itemArraySizeList,
                                      IComparer<TKey> comparer)
        {
            Int32 itemArrayCnt = itemArrayList.Count;
            TKey[][] keyArrays = new TKey[itemArrayCnt][];
            TElement[][] itemArrays = new TElement[itemArrayCnt][];
            TKey[] keys = new TKey[itemArrayCnt];
            Int32[] indexArray = new Int32[itemArrayCnt];
            for (int i = 0; i < itemArrayCnt; i++)
            {
                keyArrays[i] = keyArrayList[i];
                itemArrays[i] = itemArrayList[i];
                keys[i] = keyArrayList[i][0];
                indexArray[i] = 0;
            }

            while (itemArrayCnt > 0)
            {
                TKey key = keys[0];
                int idx = 0;
                for (int i = 1; i < itemArrayCnt; i++)
                {
                    if (comparer.Compare(key, keys[i]) > 0)
                    {
                        key = keys[i];
                        idx = i;
                    }
                }

                yield return itemArrays[idx][indexArray[idx]++];

                if (indexArray[idx] < itemArraySizeList[idx])
                {
                    keys[idx] = keyArrays[idx][indexArray[idx]];
                }
                else
                {
                    itemArrayCnt--;
                    if (idx < itemArrayCnt)
                    {
                        itemArrays[idx] = itemArrays[itemArrayCnt];
                        keyArrays[idx] = keyArrays[itemArrayCnt];
                        itemArraySizeList[idx] = itemArraySizeList[itemArrayCnt];
                        indexArray[idx] = indexArray[itemArrayCnt];
                        keys[idx] = keys[itemArrayCnt];
                    }
                }
            }
        }

        internal static IEnumerable<TElement>
            HeapMergeSort<TElement, TKey>(IMultiEnumerable<TElement> source,
                                          Func<TElement, TKey> keySelector,
                                          IComparer<TKey> comparer,
                                          bool isDescending)
        {
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            // Initialize
            IEnumerable<TElement>[] readers = new IEnumerable<TElement>[source.NumberOfInputs];
            for (int i = 0; i < readers.Length; i++)
            {
                readers[i] = source[i];
            }
            return DryadLinqUtil.MergeSort(readers, keySelector, comparer, isDescending);
        }

        internal static IEnumerator<TElement>
            MergeSort<TElement, TKey>(IEnumerator<TElement[]>[] enumArray,
                                      Func<TElement, TKey> keySelector,
                                      IComparer<TKey> comparer,
                                      bool isDescending)
        {
            TElement[][] itemArrays = new TElement[enumArray.Length][];
            TKey[] keys = new TKey[enumArray.Length];
            Int32[] indexArray = new Int32[enumArray.Length];
            Int32 mergeCnt = 0;
            
            for (int i = 0; i < enumArray.Length; i++)
            {
                if (enumArray[i].MoveNext())
                {
                    enumArray[mergeCnt] = enumArray[i];
                    itemArrays[mergeCnt] = enumArray[mergeCnt].Current;
                    keys[mergeCnt] = keySelector(itemArrays[mergeCnt][0]);
                    indexArray[mergeCnt] = 0;
                    mergeCnt++;
                }
            }

            while (mergeCnt > 1)
            {
                TKey key = keys[0];
                Int32 idx = 0;
                for (int i = 1; i < mergeCnt; i++)
                {
                    int cmp = comparer.Compare(key, keys[i]);
                    cmp = (isDescending) ? -cmp : cmp;
                    if (cmp > 0)
                    {
                        key = keys[i];
                        idx = i;
                    }
                }

                yield return itemArrays[idx][indexArray[idx]++];

                if (indexArray[idx] < itemArrays[idx].Length)
                {
                    keys[idx] = keySelector(itemArrays[idx][indexArray[idx]]);
                }
                else if (enumArray[idx].MoveNext())
                {
                    itemArrays[idx] = enumArray[idx].Current;
                    keys[idx] = keySelector(itemArrays[idx][0]);
                    indexArray[idx] = 0;
                }
                else
                {
                    mergeCnt--;
                    if (idx < mergeCnt)
                    {
                        enumArray[idx] = enumArray[mergeCnt];
                        itemArrays[idx] = itemArrays[mergeCnt];
                        indexArray[idx] = indexArray[mergeCnt];
                        keys[idx] = keys[mergeCnt];
                    }                    
                }
            }

            if (mergeCnt == 1)
            {
                IEnumerator<TElement[]> elems = enumArray[0];
                TElement[] itemArray = itemArrays[0];
                Int32 index = indexArray[0];
                while (true)
                {
                    for (int i = index; i < itemArray.Length; i++)
                    {
                        yield return itemArray[i];
                    }
                    if (!elems.MoveNext()) break;
                    itemArray = elems.Current;
                    index = 0;
                }
            }
        }
    }
    
    internal struct MinusComparer<T> : IComparer<T>, IEquatable<MinusComparer<T>>
    {
        private IComparer<T> m_comparer;

        internal MinusComparer(IComparer<T> comparer)
        {
            this.m_comparer = comparer;
        }

        internal static IComparer<T> Make(IComparer<T> comparer)
        {
            if (comparer is MinusComparer<T>)
            {
                return ((MinusComparer<T>)comparer).InnerComparer;
            }
            return new MinusComparer<T>(comparer);
        }

        internal IComparer<T> InnerComparer
        {
            get { return this.m_comparer; }
        }

        public int Compare(T x, T y)
        {
            return this.m_comparer.Compare(y, x);
        }

        public bool Equals(MinusComparer<T> val)
        {
            return this.InnerComparer.Equals(val.InnerComparer);
        }
    }

    internal struct ReferenceEqualityComparer<T> : IEqualityComparer<T>
    {
        public bool Equals(T x, T y)
        {
            return Object.ReferenceEquals(x, y);
        }

        public int GetHashCode(T x)
        {
            return x.GetHashCode();
        }
    }

    public class FileEnumerable<T> : IEnumerable<T>
    {
        private DryadLinqFactory<T> m_factory;
        private string m_fileName;
        private DryadLinqRecordReader<T> m_reader;
            
        internal FileEnumerable(T[] elems, DryadLinqFactory<T> factory)
        {
            this.m_factory = factory;
            this.m_fileName = DryadLinqUtil.MakeUniqueName();
            this.m_reader = null;

            //YY: could potentially use compression to reduce I/O
            NativeBlockStream ns = new DryadLinqFileBlockStream(this.m_fileName, FileAccess.Write);
            DryadLinqRecordWriter<T> writer = this.m_factory.MakeWriter(ns);
            try
            {
                for (int i = 0; i < elems.Length; i++)
                {
                    writer.WriteRecordSync(elems[i]);
                }
            }
            finally
            {
                writer.Close();
            }
        }

        ~FileEnumerable()
        {
            this.Dispose(false);
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (this.m_reader != null)
            {
                this.m_reader.Close();
            }
            File.Delete(this.m_fileName);
        }
        
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private IEnumerator<T> GetEnumerator()
        {
            NativeBlockStream ns = new DryadLinqFileBlockStream(this.m_fileName, FileAccess.Read);
            this.m_reader = this.m_factory.MakeReader(ns);

            T rec = default(T);
            try
            {
                if (DryadLinqVertex.s_multiThreading)
                {
                    this.m_reader.StartWorker();
                    while (this.m_reader.ReadRecordAsync(ref rec))
                    {
                        yield return rec;
                    }
                }
                else
                {
                    while (this.m_reader.ReadRecordSync(ref rec))
                    {
                        yield return rec;
                    }
                }
            }
            finally
            {
                this.m_reader.Close();
            }
        }
    }
}
