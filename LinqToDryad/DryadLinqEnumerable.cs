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
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq;
using Microsoft.Research.DryadLinq.Internal;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// This class provides the IEnumerable implementation of the operators
    /// we introduced in DryadLINQ. This is needed to implement LocalDebug.
    /// </summary>
    /// <remarks>A DryadLINQ user should not need to use this class directly.</remarks>
    public static class DryadLinqEnumerable
    {
        // Operator: HashPartition
        public static IEnumerable<TSource>
            HashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         IEqualityComparer<TKey> comparer,
                                         int count)
        {
            return source;
        }

        public static IEnumerable<TSource>
            HashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         int count)
        {
            return source;
        }

        public static IEnumerable<TSource>
            HashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector)
        {
            return source;
        }
        
        public static IEnumerable<TSource>
            HashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         IEqualityComparer<TKey> comparer)
        {
            return source;
        }

        public static IEnumerable<TResult>
            HashPartition<TSource, TKey, TResult>(this IEnumerable<TSource> source,
                                                  Func<TSource, TKey> keySelector,
                                                  Func<TSource, TResult> resultSelector)
        {
            foreach (TSource x in source)
            {
                yield return resultSelector(x);
            }
        }

        public static IEnumerable<TResult>
            HashPartition<TSource, TKey, TResult>(this IEnumerable<TSource> source,
                                                  Func<TSource, TKey> keySelector,
                                                  IEqualityComparer<TKey> comparer,
                                                  Func<TSource, TResult> resultSelector)
        {
            foreach (TSource x in source)
            {
                yield return resultSelector(x);
            }
        }

        // Operator: RangePartition
        public static IEnumerable<TSource>
            RangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          bool isDescending)
        {
            return source;
        }

        public static IEnumerable<TSource>
            RangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          IComparer<TKey> comparer,
                                          bool isDescending)
        {
            return source;
        }

        public static IEnumerable<TSource>
            RangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          TKey[] rangeSeparators)
        {
            return source;
        }

        public static IEnumerable<TSource>
            RangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                          Func<TSource, TKey> keySelector,
                                          TKey[] rangeSeparators,
                                          IComparer<TKey> comparer)
        {
            return source;
        }

        // Operator: Apply
        public static IEnumerable<TResult>
            Apply<TSource, TResult>(this IEnumerable<TSource> source,
                                    Func<IEnumerable<TSource>, IEnumerable<TResult>> procFunc)
        {
            return procFunc(source);
        }

        public static IEnumerable<TResult>
            Apply<TSource1, TSource2, TResult>(this IEnumerable<TSource1> source1,
                                               IEnumerable<TSource2> source2,
                                               Func<IEnumerable<TSource1>, IEnumerable<TSource2>, IEnumerable<TResult>> procFunc)
        {
            return procFunc(source1, source2);
        }

        public static IEnumerable<TResult>
            Apply<TSource, TResult>(this IEnumerable<TSource> source,
                                    IEnumerable<TSource>[] otherSources,
                                    Func<IEnumerable<TSource>[], IEnumerable<TResult>> procFunc)
        {
            IEnumerable<TSource>[] allSources = new IEnumerable<TSource>[otherSources.Length + 1];
            allSources[0] = source;
            for (int i = 0; i < otherSources.Length; i++)
            {
                allSources[i+1] = otherSources[i];
            }
            return procFunc(allSources);
        }

        public static IEnumerable<TResult>
            Apply<TSource, TResult>(this IEnumerable<TSource> source,
                                    IEnumerable[] otherSources,
                                    Func<IEnumerable<TSource>, IEnumerable[], IEnumerable<TResult>> procFunc)
        {
            return procFunc(source, otherSources);
        }
        
        // Operator: DoWhile
        public static IEnumerable<T>
            DoWhile<T>(this IEnumerable<T> source,
                       Func<IQueryable<T>, IQueryable<T>> body,
                       Func<IQueryable<T>, IQueryable<T>, IQueryable<bool>> cond)
        {
            IEnumerable<T> before = source;
            while (true)
            {
                IEnumerable<T> after = before;
                after = body(after.AsQueryable());
                var more = cond(before.AsQueryable(), after.AsQueryable()).Single();
                if (!more) return after;
                before = after;
            }
        }

        // Operator: SlidingWindow
        public static IEnumerable<T2>
            SlidingWindow<T1, T2>(this IEnumerable<T1> source,
                                  Func<IEnumerable<T1>, T2> procFunc,
                                  Int32 windowSize)
        {
            Window<T1> window = new Window<T1>(windowSize);
            using (IEnumerator<T1> sourceEnum = source.GetEnumerator())
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

        // Operator: ApplyWithPartitionIndex
        public static IEnumerable<T2>
            ApplyWithPartitionIndex<T1, T2>(this IEnumerable<T1> source,
                                            Func<IEnumerable<T1>, int, IEnumerable<T2>> procFunc)
        {
            return procFunc(source, 0);
        }

        public static IEnumerable<bool> AnyAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Any());
        }

        public static IEnumerable<bool>
            AnyAsQuery<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.Any(predicate));
        }

        public static IEnumerable<bool>
            AllAsQuery<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.All(predicate));
        }

        public static IEnumerable<int>
            CountAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Count());
        }
        
        public static IEnumerable<int>
            CountAsQuery<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.Count(predicate));
        }

        public static IEnumerable<long>
            LongCountAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.LongCount());
        }

        public static IEnumerable<long>
            LongCountAsQuery<TSource>(this IEnumerable<TSource> source,
                                      Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.LongCount(predicate));
        }

        public static IEnumerable<bool>
            ContainsAsQuery<TSource>(this IEnumerable<TSource> source, TSource value)
        {
            return DryadLinqVertex.AsEnumerable(source.Contains(value));
        }

        public static IEnumerable<bool>
            ContainsAsQuery<TSource>(this IEnumerable<TSource> source,
                                     TSource value,
                                     IEqualityComparer<TSource> comparer)
        {
            return DryadLinqVertex.AsEnumerable(source.Contains(value, comparer));
        }

        public static IEnumerable<bool>
            SequenceEqualAsQuery<TSource>(this IEnumerable<TSource> first,
                                          IEnumerable<TSource> second)
        {
            return DryadLinqVertex.AsEnumerable(first.SequenceEqual(second));
        }
        
        public static IEnumerable<bool>
            SequenceEqualAsQuery<TSource>(this IEnumerable<TSource> first,
                                          IEnumerable<TSource> second,
                                          IEqualityComparer<TSource> comparer)
        {
            return DryadLinqVertex.AsEnumerable(first.SequenceEqual(second, comparer));
        }
        
        public static IEnumerable<TSource> FirstAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.First());
        }

        public static IEnumerable<TSource>
            FirstAsQuery<TSource>(this IEnumerable<TSource> source,
                                  Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.First(predicate));
        }

        public static IEnumerable<TSource> LastAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Last());
        }

        public static IEnumerable<TSource>
            LastAsQuery<TSource>(this IEnumerable<TSource> source,
                                 Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.Last(predicate));
        }

        public static IEnumerable<TSource> SingleAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Single());
        }

        public static IEnumerable<TSource>
            SingleAsQuery<TSource>(this IEnumerable<TSource> source,
                                   Func<TSource, bool> predicate)
        {
            return DryadLinqVertex.AsEnumerable(source.Single(predicate));
        }

        public static IEnumerable<int> SumAsQuery(this IEnumerable<int> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<int?> SumAsQuery(this IEnumerable<int?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<long> SumAsQuery(this IEnumerable<long> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<long?> SumAsQuery(this IEnumerable<long?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<float> SumAsQuery(this IEnumerable<float> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<float?> SumAsQuery(this IEnumerable<float?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<double> SumAsQuery(this IEnumerable<double> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<double?> SumAsQuery(this IEnumerable<double?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<decimal> SumAsQuery(this IEnumerable<decimal> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<decimal?> SumAsQuery(this IEnumerable<decimal?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum());
        }

        public static IEnumerable<int>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<int?>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<long>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<long?>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<float>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<float?>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<double>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<double?>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<decimal>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<decimal?>
            SumAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Sum(selector));
        }

        public static IEnumerable<int> MinAsQuery(this IEnumerable<int> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<int?> MinAsQuery(this IEnumerable<int?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<long> MinAsQuery(this IEnumerable<long> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<long?> MinAsQuery(this IEnumerable<long?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<float> MinAsQuery(this IEnumerable<float> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<float?> MinAsQuery(this IEnumerable<float?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<double> MinAsQuery(this IEnumerable<double> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<double?> MinAsQuery(this IEnumerable<double?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<decimal> MinAsQuery(this IEnumerable<decimal> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<decimal?> MinAsQuery(this IEnumerable<decimal?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<TSource> MinAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Min());
        }

        public static IEnumerable<int>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<int?>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<long>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<long?>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<float>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<float?>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<double>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<double?>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<decimal>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<decimal?>
            MinAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<TResult>
            MinAsQuery<TSource, TResult>(this IEnumerable<TSource> source,
                                         Func<TSource, TResult> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Min(selector));
        }

        public static IEnumerable<int> MaxAsQuery(this IEnumerable<int> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<int?> MaxAsQuery(this IEnumerable<int?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<long> MaxAsQuery(this IEnumerable<long> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<long?> MaxAsQuery(this IEnumerable<long?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<double> MaxAsQuery(this IEnumerable<double> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<double?> MaxAsQuery(this IEnumerable<double?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<float> MaxAsQuery(this IEnumerable<float> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<float?> MaxAsQuery(this IEnumerable<float?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<decimal> MaxAsQuery(this IEnumerable<decimal> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<decimal?> MaxAsQuery(this IEnumerable<decimal?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<TSource> MaxAsQuery<TSource>(this IEnumerable<TSource> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Max());
        }

        public static IEnumerable<int>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<int?>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, int?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<long>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<long?>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, long?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<float>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<float?>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, float?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<double>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<double?>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, double?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<decimal>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<decimal?>
            MaxAsQuery<TSource>(this IEnumerable<TSource> source,
                                Func<TSource, decimal?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<TResult>
            MaxAsQuery<TSource, TResult>(this IEnumerable<TSource> source,
                                         Func<TSource, TResult> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Max(selector));
        }

        public static IEnumerable<double> AverageAsQuery(this IEnumerable<int> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double?> AverageAsQuery(this IEnumerable<int?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double> AverageAsQuery(this IEnumerable<long> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double?> AverageAsQuery(this IEnumerable<long?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<float> AverageAsQuery(this IEnumerable<float> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<float?> AverageAsQuery(this IEnumerable<float?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double> AverageAsQuery(this IEnumerable<double> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double?> AverageAsQuery(this IEnumerable<double?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<decimal> AverageAsQuery(this IEnumerable<decimal> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<decimal?> AverageAsQuery(this IEnumerable<decimal?> source)
        {
            return DryadLinqVertex.AsEnumerable(source.Average());
        }

        public static IEnumerable<double>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, int> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<double?>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, int?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<double>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, long> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<double?>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, long?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<float>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, float> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<float?>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, float?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<double>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, double> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<double?>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, double?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<decimal>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, decimal> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        public static IEnumerable<decimal?>
            AverageAsQuery<TSource>(this IEnumerable<TSource> source,
                                    Func<TSource, decimal?> selector)
        {
            return DryadLinqVertex.AsEnumerable(source.Average(selector));
        }

        // Aggregate operator
        public static TAccumulate
            Aggregate<TSource, TAccumulate>(this IEnumerable<TSource> source,
                                            Func<TAccumulate> seedFunc,
                                            Func<TAccumulate, TSource, TAccumulate> func)
        {
            return source.Aggregate(seedFunc(), func);
        }

        public static TResult
            Aggregate<TSource, TAccumulate, TResult>(this IEnumerable<TSource> source,
                                                     Func<TAccumulate> seedFunc,
                                                     Func<TAccumulate, TSource, TAccumulate> func,
                                                     Func<TAccumulate, TResult> resultSelector)
        {
            return resultSelector(source.Aggregate(seedFunc, func));
        }

        public static IEnumerable<TSource>
            AggregateAsQuery<TSource>(this IEnumerable<TSource> source,
                                      Func<TSource, TSource, TSource> func)
        {
            return DryadLinqVertex.AsEnumerable(source.Aggregate(func));
        }

        public static IEnumerable<TAccumulate>
            AggregateAsQuery<TSource, TAccumulate>(this IEnumerable<TSource> source,
                                                   TAccumulate seed,
                                                   Func<TAccumulate, TSource, TAccumulate> func)
        {
            return DryadLinqVertex.AsEnumerable(source.Aggregate(seed, func));
        }

        public static IEnumerable<TResult>
            AggregateAsQuery<TSource, TAccumulate, TResult>(this IEnumerable<TSource> source,
                                                            TAccumulate seed,
                                                            Func<TAccumulate, TSource, TAccumulate> func,
                                                            Func<TAccumulate, TResult> resultSelector)
        {
            return DryadLinqVertex.AsEnumerable(source.Aggregate(seed, func, resultSelector));
        }

        public static IEnumerable<TAccumulate>
            AggregateAsQuery<TSource, TAccumulate>(this IEnumerable<TSource> source,
                                                   Func<TAccumulate> seedFunc,
                                                   Func<TAccumulate, TSource, TAccumulate> func)
        {
            return DryadLinqVertex.AsEnumerable(source.Aggregate(seedFunc(), func));
        }

        public static IEnumerable<TResult>
            AggregateAsQuery<TSource, TAccumulate, TResult>(this IEnumerable<TSource> source,
                                                            Func<TAccumulate> seedFunc,
                                                            Func<TAccumulate, TSource, TAccumulate> func,
                                                            Func<TAccumulate, TResult> resultSelector)
        {
            return DryadLinqVertex.AsEnumerable(source.Aggregate(seedFunc, func, resultSelector));
        }
        
        public static IEnumerable<TSource>
            AssumeHashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                               Func<TSource, TKey> keySelector)
        {
            return source;
        }

        // Operator: AssumeHashPartition
        public static IEnumerable<TSource>
            AssumeHashPartition<TSource, TKey>(this IEnumerable<TSource> source,
                                               Func<TSource, TKey> keySelector,
                                               IEqualityComparer<TKey> comparer)
        {
            return source;
        }

        public static IEnumerable<TSource>
            AssumeRangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                                Func<TSource, TKey> keySelector,
                                                bool isDescending)
        {
            return source;
        }

        // Operator: AssumeRangePartition
        public static IEnumerable<TSource>
            AssumeRangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                                Func<TSource, TKey> keySelector,
                                                IComparer<TKey> comparer,
                                                bool isDescending)
        {
            return source;
        }

        public static IEnumerable<TSource>
            AssumeRangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                                Func<TSource, TKey> keySelector,
                                                TKey[] rangeSeparators)
        {
            return source;
        }

        public static IEnumerable<TSource>
            AssumeRangePartition<TSource, TKey>(this IEnumerable<TSource> source,
                                                Func<TSource, TKey> keySelector,
                                                TKey[] rangeSeparators,
                                                IComparer<TKey> comparer)
        {
            return source;
        }

        // Operator: AssumeOrderBy
        public static IEnumerable<TSource>
            AssumeOrderBy<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         bool isDescending)
        {
            return source;
        }

        public static IEnumerable<TSource>
            AssumeOrderBy<TSource, TKey>(this IEnumerable<TSource> source,
                                         Func<TSource, TKey> keySelector,
                                         IComparer<TKey> comparer,
                                         bool isDescending)
        {
            return source;
        }

        public static IMultiEnumerable<R1, R2>
            Fork<TSource, R1, R2>(this IEnumerable<TSource> source,
                                  Func<IEnumerable<TSource>, IEnumerable<ForkTuple<R1, R2>>> mapper)
        {
            List<R1> resX = new List<R1>();
            List<R2> resY = new List<R2>();

            IEnumerable<ForkTuple<R1, R2>> result = mapper(source);
            foreach (ForkTuple<R1, R2> item in result)
            {
                if (item.HasFirst) resX.Add(item.First);
                if (item.HasSecond) resY.Add(item.Second);
            }
            return new MultiEnumerable<R1, R2>(resX, resY);
        }
        
        public static IMultiEnumerable<R1, R2, R3>
            Fork<TSource, R1, R2, R3>(this IEnumerable<TSource> source,
                                      Func<IEnumerable<TSource>, IEnumerable<ForkTuple<R1, R2, R3>>> mapper)
        {
            List<R1> resX = new List<R1>();
            List<R2> resY = new List<R2>();
            List<R3> resZ = new List<R3>();

            IEnumerable<ForkTuple<R1, R2, R3>> result = mapper(source);
            foreach (ForkTuple<R1, R2, R3> item in result)
            {
                if (item.HasFirst) resX.Add(item.First);
                if (item.HasSecond) resY.Add(item.Second);
                if (item.HasThird) resZ.Add(item.Third);                
            }
            return new MultiEnumerable<R1, R2, R3>(resX, resY, resZ);
        }
        
        public static IMultiEnumerable<R1, R2>
            Fork<TSource, R1, R2>(this IEnumerable<TSource> source,
                                  Func<TSource, ForkTuple<R1, R2>> mapper)
        {
            List<R1> resX = new List<R1>();
            List<R2> resY = new List<R2>();
        
            foreach (TSource elem in source)
            {
                ForkTuple<R1, R2> item = mapper(elem);
                if (item.HasFirst) resX.Add(item.First);
                if (item.HasSecond) resY.Add(item.Second);
            }
            return new MultiEnumerable<R1, R2>(resX, resY);
        }
        
        public static IMultiEnumerable<R1, R2, R3>
            Fork<TSource, R1, R2, R3>(this IEnumerable<TSource> source,
                                      Func<TSource, ForkTuple<R1, R2, R3>> mapper)
        {
            List<R1> resX = new List<R1>();
            List<R2> resY = new List<R2>();
            List<R3> resZ = new List<R3>();
        
            foreach (TSource elem in source)
            {
                ForkTuple<R1, R2, R3> item = mapper(elem);
                if (item.HasFirst) resX.Add(item.First);
                if (item.HasSecond) resY.Add(item.Second);
                if (item.HasThird) resZ.Add(item.Third);
            }
            return new MultiEnumerable<R1, R2, R3>(resX, resY, resZ);
        }
        
        public static IMultiEnumerable<TSource>
            Fork<TSource, TKey>(this IEnumerable<TSource> source,
                                Func<TSource, TKey> keySelector,
                                TKey[] keys)
        {
            List<TSource>[] enumList = new List<TSource>[keys.Length];
            Dictionary<TKey, int> keyMap = new Dictionary<TKey, int>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                enumList[i] = new List<TSource>();
                keyMap.Add(keys[i], i);
            }
            foreach (TSource item in source)
            {
                int index;
                if (keyMap.TryGetValue(keySelector(item), out index))
                {
                    enumList[index].Add(item);
                }
            }
            return new MultiEnumerable<TSource>(enumList);
        }

        public static IEnumerable<TSource>
            LongTakeWhile<TSource>(this IEnumerable<TSource> source,
                                   Func<TSource, long, bool> predicate)
        {
            long index = 0;
            foreach (TSource element in source)
            {
                if (!predicate(element, index))
                {
                    yield break;
                }
                yield return element;
                checked { index++; }
            }
        }

        public static IEnumerable<TSource>
            LongSkipWhile<TSource>(this IEnumerable<TSource> source,
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

        public static IEnumerable<T> FlattenGroups<K, T>(IEnumerable<IGrouping<K, T>> groups)
        {
            foreach (var g in groups)
            {
                foreach (var x in g)
                {
                    yield return x;
                }
            }
        }

        [FieldMapping("key", "Key")]
        public static IGrouping<K, T>
            MakeDryadLinqGroup<K, T>(K key, IEnumerable<IGrouping<K, T>> groups)
        {
            return new DryadLinqGrouping<K, T>(key, FlattenGroups(groups));
        }
        
        public static IEnumerable<T> Flatten<T>(IEnumerable<IEnumerable<T>> groups)
        {
            foreach (var g in groups)
            {
                foreach (var x in g)
                {
                    yield return x;
                }
            }
        }

        public static IEnumerable<T> FlattenDistinct<T>(IEnumerable<IEnumerable<T>> groups,
                                                        IEqualityComparer<T> comparer)
        {
            return Flatten(groups).Distinct(comparer).ToArray();
        }

        public static IEnumerable<IndexedValue<long>> Offsets(IEnumerable<long> counts, bool isLong)
        {
            int index = 0;
            long offset = 0;
            foreach (long count in counts)
            {
                yield return new IndexedValue<long>(index, offset);
                index++;
                checked { offset += count; }
            }
            if (!isLong && (offset > Int32.MaxValue))
            {
                throw new OverflowException(SR.IndexTooSmall);
            }
        }

        public static IEnumerable<TSource>
            WhereWithStartIndex<TSource>(IEnumerable<TSource> source,
                           IEnumerable<IndexedValue<long>> startIndex,
                           Func<TSource, int, bool> predicate)
        {
            int index = (int)startIndex.Single().Value;
            foreach (TSource element in source)
            {
                if (predicate(element, index))
                {
                    yield return element;
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TSource>
           LongWhere<TSource>(this IEnumerable<TSource> source,
                              Func<TSource, long, bool> predicate)
        {
            long index = 0;
            foreach (TSource element in source)
            {
                if (predicate(element, index))
                {
                    yield return element;
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TSource>
            LongWhereWithStartIndex<TSource>(IEnumerable<TSource> source,
                               IEnumerable<IndexedValue<long>> startIndex,
                               Func<TSource, long, bool> predicate)
        {
            long index = startIndex.Single().Value;
            foreach (TSource element in source)
            {
                if (predicate(element, index))
                {
                    yield return element;
                }
                checked { index++; }
            }
        }
        
        public static IEnumerable<TResult>
            SelectWithStartIndex<TSource, TResult>(IEnumerable<TSource> source,
                                     IEnumerable<IndexedValue<long>> startIndex,
                                     Func<TSource, int, TResult> selector)
        {
            int index = (int)startIndex.Single().Value;
            foreach (TSource element in source)
            {
                yield return selector(element, index);
                checked { index++; }
            }
        }

        public static IEnumerable<TResult>
            LongSelect<TSource, TResult>(this IEnumerable<TSource> source,
                                        Func<TSource, long, TResult> selector)
        {
            long index = 0;
            foreach (TSource element in source)
            {
                yield return selector(element, index);
                checked { index++; }
            }
        }

        public static IEnumerable<TResult>
            LongSelectWithStartIndex<TSource, TResult>(IEnumerable<TSource> source,
                                         IEnumerable<IndexedValue<long>> startIndex,
                                         Func<TSource, long, TResult> selector)
        {
            long index = startIndex.Single().Value;
            foreach (TSource element in source)
            {
                yield return selector(element, index);
                checked { index++; }
            }
        }
        
        public static IEnumerable<TResult>
            SelectManyWithStartIndex<TSource, TResult>(IEnumerable<TSource> source,
                                         IEnumerable<IndexedValue<long>> startIndex,
                                         Func<TSource, int, IEnumerable<TResult>> selector)
        {
            int index = (int)startIndex.Single().Value;
            foreach (TSource element in source)
            {
                foreach (TResult result in selector(element, index))
                {
                    yield return result;
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TResult>
            LongSelectManyWithStartIndex<TSource, TResult>(
                                 IEnumerable<TSource> source,
                                 IEnumerable<IndexedValue<long>> startIndex,
                                 Func<TSource, long, IEnumerable<TResult>> selector)
        {
            long index = startIndex.Single().Value;
            foreach (TSource element in source)
            {
                foreach (TResult result in selector(element, index))
                {
                    yield return result;
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TResult>
            SelectManyResultWithStartIndex<TSource, TCollection, TResult>(
                                 IEnumerable<TSource> source,
                                 IEnumerable<IndexedValue<long>> startIndex,
                                 Func<TSource, int, IEnumerable<TCollection>> collectionSelector,
                                 Func<TSource, TCollection, TResult> resultSelector)
        {
            int index = (int)startIndex.Single().Value;
            foreach (TSource element in source)
            {
                foreach (TCollection result in collectionSelector(element, index))
                {
                    yield return resultSelector(element, result);
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TResult> LongSelectMany<TSource, TResult>(
                                 this IEnumerable<TSource> source,
                                 Func<TSource, long, IEnumerable<TResult>> selector)
        {
            long index = 0;
            foreach (TSource element in source)
            {
                foreach (TResult result in selector(element, index))
                {
                    yield return result;
                }
                checked { index++; }
            }
        }

        public static IEnumerable<TResult>
            LongSelectMany<TSource, TCollection, TResult>(
                                 this IEnumerable<TSource> source,
                                 Func<TSource, long, IEnumerable<TCollection>> selector,
                                 Func<TSource, TCollection, TResult> resultSelector)
        {
            long index = 0;
            foreach (TSource element in source)
            {
                foreach (TCollection result in selector(element, index))
                {
                    yield return resultSelector(element, result);
                }
                checked { index++; }
            }
        }


        public static IEnumerable<TResult>
            LongSelectManyResultWithStartIndex<TSource, TCollection, TResult>(
                                 IEnumerable<TSource> source,
                                 IEnumerable<IndexedValue<long>> startIndex,
                                 Func<TSource, long, IEnumerable<TCollection>> collectionSelector,
                                 Func<TSource, TCollection, TResult> resultSelector)
        {
            long index = startIndex.Single().Value;
            foreach (TSource element in source)
            {
                foreach (TCollection result in collectionSelector(element, index))
                {
                    yield return resultSelector(element, result);
                }
                checked { index++; }
            }
        }
        
        private const int GroupSize = 1000;
        public static IEnumerable<Pair<List<T>, bool>>
            GroupTakeWhile<T>(IEnumerable<T> source, Func<T, bool> pred)
        {
            List<T> group = new List<T>(GroupSize);
            foreach (T elem in source)
            {
                if (pred(elem))
                {
                    if (group.Count == GroupSize)
                    {
                        yield return new Pair<List<T>, bool>(group, true);
                        group = new List<T>(GroupSize);
                    }
                    group.Add(elem);
                }
                else
                {
                    yield return new Pair<List<T>, bool>(group, false);
                    yield break;
                }
            }

            yield return new Pair<List<T>, bool>(group, true);
        }

        public static IEnumerable<Pair<List<T>, bool>>
            GroupIndexedTakeWhile<T>(IEnumerable<T> source,
                                     IEnumerable<IndexedValue<long>> startIndex,
                                     Func<T, int, bool> pred)
        {
            int currIdx = (int)startIndex.Single().Value;
            List<T> group = new List<T>(GroupSize);
            foreach (T elem in source)
            {
                if (pred(elem, currIdx))
                {
                    if (group.Count == GroupSize)
                    {
                        yield return new Pair<List<T>, bool>(group, true);
                        group = new List<T>(GroupSize);
                    }
                    group.Add(elem);
                    checked { currIdx++; }
                }
                else
                {
                    yield return new Pair<List<T>, bool>(group, false);
                    yield break;
                }
            }

            yield return new Pair<List<T>, bool>(group, true);
        }

        public static IEnumerable<Pair<List<T>, bool>>
            GroupIndexedLongTakeWhile<T>(IEnumerable<T> source,
                                         IEnumerable<IndexedValue<long>> startIndex,
                                         Func<T, long, bool> pred)
        {
            long currIdx = startIndex.Single().Value;
            List<T> group = new List<T>(GroupSize);
            foreach (T elem in source)
            {
                if (pred(elem, currIdx))
                {
                    if (group.Count == GroupSize)
                    {
                        yield return new Pair<List<T>, bool>(group, true);
                        group = new List<T>(GroupSize);
                    }
                    group.Add(elem);
                    currIdx++;
                }
                else
                {
                    yield return new Pair<List<T>, bool>(group, false);
                    yield break;
                }
            }
            
            yield return new Pair<List<T>, bool>(group, true);
        }

        public static IEnumerable<T> ToStore<T>(IEnumerable<T> source)
        {
            return source;
        }
    }
}
