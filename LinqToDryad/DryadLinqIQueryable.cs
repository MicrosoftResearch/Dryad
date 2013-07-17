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
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.Dryad.Hdfs;


namespace Microsoft.Research.DryadLinq
{   
    // This class introduces some new operators into the expression tree. So far,
    // there are two classes of new operators:
    //     1. HashPartition, RangePartition, Merge
    //     2. Apply
    public static class HpcLinqQueryable
    {
        internal static bool IsLocalDebugSource(IQueryable source)
        {
            return !(source.Provider is DryadLinqProvider);
        }

        public static IQueryable<TSource>
            LongWhere<TSource>(this IQueryable<TSource> source,
                               Expression<Func<TSource, long, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongWhere(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TSource>( 
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<TResult>
            LongSelect<TSource,TResult>(this IQueryable<TSource> source,
                                        Expression<Func<TSource, long, TResult>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongSelect(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            } 

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<TResult>
            LongSelectMany<TSource,TResult>(this IQueryable<TSource> source,
                                            Expression<Func<TSource, long, IEnumerable<TResult>>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongSelectMany(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<TResult>
            LongSelectMany<TSource, TCollection, TResult>(this IQueryable<TSource> source,
                                                          Expression<Func<TSource, long, IEnumerable<TCollection>>> selector,
                                                          Expression<Func<TSource, TCollection, TResult>> resultSelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (resultSelector == null)
            {
                throw new ArgumentNullException("resultSelector");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongSelectMany(source, selector.Compile(), resultSelector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TCollection), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector), Expression.Quote(resultSelector) }
                    ));
        }
        
        public static IQueryable<TSource>
            LongTakeWhile<TSource>(this IQueryable<TSource> source,
                                   Expression<Func<TSource, long, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongTakeWhile(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TSource>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<TSource>
            LongSkipWhile<TSource>(this IQueryable<TSource> source,
                                   Expression<Func<TSource, long, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var result = HpcLinqEnumerable.LongSkipWhile(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TSource>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Hash partition a dataset.  
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An EqualityComparer on TKey to compare keys</param>
        /// <param name="partitionCount">The number of partitions to create</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            HashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector,
                                         IEqualityComparer<TKey> comparer,
                                         int partitionCount)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (partitionCount <= 0)
            {
                throw new ArgumentOutOfRangeException("partitionCount");
            }

            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IEqualityComparer<TKey>)),
                                       Expression.Constant(partitionCount, typeof(int)) }
                    ));
        }

        /// <summary>
        /// Hash partition a dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">the dataset to be partitioned</param>
        /// <param name="keySelector">The funtion to extract the key from a record</param>
        /// <param name="partitionCount">The number of partitioned to create</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            HashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector,
                                         int partitionCount)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (partitionCount <= 0)
            {
                throw new ArgumentOutOfRangeException("partitionCount");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(partitionCount, typeof(int)) }
                    ));
        }

        /// <summary>
        /// Hash partition a dataset. The number of resulting partitions is dynamically determined
        /// at the runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">the dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <returns>An IQueryable partitioned according to a key </returns>
        public static IQueryable<TSource>
            HashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector) }
                    ));
        }

        /// <summary>
        /// Hash partition a dataset. The number of resulting partitions is dynamically determined
        /// at the runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            HashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector,
                                         IEqualityComparer<TKey> comparer)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IEqualityComparer<TKey>)) }
                    ));
        }

        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at 
        /// runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector) }
                    ));
        }

        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at 
        /// runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="partitionCount">Number of partitions in the output dataset</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          int partitionCount)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (partitionCount <= 0)
            {
                throw new ArgumentOutOfRangeException("partitionCount");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(partitionCount, typeof(int)) }
                    ));
        }

        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at 
        /// runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The funtion to extract the key from a record</param>
        /// <param name="isDescending">true if the partition keys are descending</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(isDescending, typeof(bool)) }
                    ));
        }

        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          TKey[] rangeSeparators)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (rangeSeparators == null)
                throw new ArgumentNullException("rangeSeparators");

            // check that the range-keys are consistent.
            bool? dummy;
            if (!HpcLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, Comparer<TKey>.Default, out dummy))
            {
                throw new ArgumentException(SR.PartitionKeysAreNotConsistentlyOrdered, "rangeSeparators");
            }
            
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(rangeSeparators, typeof(TKey[])) }
                    ));
        }

        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at 
        /// runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The funtion to extract the key from a record</param>
        /// <param name="isDescending">true if the partition keys are descending</param>
        /// <param name="partitionCount">Number of partitions in the output dataset</param>
        /// <returns>An IQueryable partitioned according to a key</returns>
        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          bool isDescending,
                                          int partitionCount )
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (partitionCount <= 0)
            {
                throw new ArgumentOutOfRangeException("partitionCount");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(isDescending, typeof(bool)),
                                       Expression.Constant(partitionCount, typeof(int)) }
                    ));
        }
        
        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          IComparer<TKey> comparer,
                                          bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            } 
            if (IsLocalDebugSource(source))
            {
                return source;
            }           
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)),
                                       Expression.Constant(isDescending, typeof(bool)) }
                    ));
        }

        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          TKey[] rangeSeparators,
                                          IComparer<TKey> comparer)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (rangeSeparators == null)
                throw new ArgumentNullException("rangeSeparators");

            if (comparer == null && !TypeSystem.HasDefaultComparer(typeof(TKey)))
            {
                throw new DryadLinqException(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                           string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, typeof(TKey)));
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            // check that the range-keys are consistent.
            bool? dummy;
            if (!HpcLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, comparer, out dummy))
            {
                throw new ArgumentException(SR.PartitionKeysAreNotConsistentlyOrdered, "rangeSeparators");
            }

            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(rangeSeparators, typeof(TKey[])),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)) }
                    ));
        }

        public static IQueryable<TSource>
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          IComparer<TKey> comparer,
                                          bool isDescending,
                                          int partitionCount)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (partitionCount <= 0)
            {
                throw new ArgumentOutOfRangeException("partitionCount");
            }
            
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)),
                                       Expression.Constant(isDescending, typeof(bool)),
                                       Expression.Constant(partitionCount, typeof(int))}
                    ));
        }

        public static IQueryable<TSource> 
            RangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                          Expression<Func<TSource, TKey>> keySelector,
                                          TKey[] rangeSeparators, 
                                          IComparer<TKey> comparer,
                                          bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }

            if (rangeSeparators == null)
                throw new ArgumentNullException("rangeSeparators");

            if (comparer == null && !TypeSystem.HasDefaultComparer(typeof(TKey)))
            {
                throw new DryadLinqException(HpcLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                           string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, typeof(TKey)));
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);
            
            // check that the range-keys are consistent.
            bool? detectedDescending;
            bool keysAreConsistent = HpcLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, comparer, out detectedDescending);
            // Note: detectedDescending==null implies that we couldn't precisely tell (single element, repeated elements, etc).
            
            if (!keysAreConsistent)
            {
                throw new ArgumentException(SR.PartitionKeysAreNotConsistentlyOrdered, "rangeSeparators");
            }

            // and check that the actual direction of keys matches what the user said they wanted.
            if (detectedDescending != null && detectedDescending != isDescending)
            {
                throw new ArgumentException(SR.IsDescendingIsInconsistent);
            }

            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(rangeSeparators, typeof(TKey[])),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)),
                                       Expression.Constant(isDescending, typeof(bool)) }
                    ));

        }

        /// <summary>
        /// Compute applyFunc (source)
        /// </summary>
        /// <typeparam name="T1">The type of the records of the input dataset</typeparam>
        /// <typeparam name="T2">The type of the records of the output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="applyFunc ">The function to be applied to the input dataset</param>
        /// <returns>The result of computing applyFunc(source)</returns>
        public static IQueryable<T2>
            Apply<T1, T2>(this IQueryable<T1> source,
                          Expression<Func<IEnumerable<T1>, IEnumerable<T2>>> applyFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = applyFunc.Compile()(source).AsQueryable();
                return new DryadLinqLocalQuery<T2>(source.Provider, q);
            }            
            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(applyFunc) }
                    ));
        }

        /// <summary>
        /// Compute applyFunc(source1, source2)
        /// </summary>
        /// <typeparam name="T1">The type of the records of the first input dataset</typeparam>
        /// <typeparam name="T2">The type of the records of the second input dataset</typeparam>
        /// <typeparam name="T3">he type of the records of the output dataset</typeparam>
        /// <param name="source1">The first input dataset</param>
        /// <param name="source2">The second input dataset</param>
        /// <param name="applyFunc ">The function to be applied to the input datasets</param>
        /// <returns>The result of computing applyFunc(source1, source2)</returns>
        public static IQueryable<T3>
            Apply<T1, T2, T3>(this IQueryable<T1> source1,
                              IQueryable<T2> source2,
                              Expression<Func<IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>>> applyFunc)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException("source1");
            }
            if (source2 == null)
            {
                throw new ArgumentNullException("source2");
            }
            if (IsLocalDebugSource(source1))
            {
                var q = applyFunc.Compile()(source1, source2).AsQueryable();
                return new DryadLinqLocalQuery<T3>(source1.Provider, q);
            }
            return source1.Provider.CreateQuery<T3>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2), typeof(T3)), 
                    new Expression[] { source1.Expression,
                                       source2.Expression,
                                       Expression.Quote(applyFunc) }
                    ));
        }    

        /// <summary>
        /// Compute applyFunc on multiple sources
        /// </summary>
        /// <typeparam name="T1">The type of the records of input</typeparam>
        /// <typeparam name="T2">The type of the records of output</typeparam>
        /// <param name="source">The first input dataset</param>
        /// <param name="otherSources">Other input datasets</param>  
        /// <param name="applyFunc">The function to be applied to the input datasets</param>
        /// <returns>The result of computing applyFunc(source,pieces)</returns>
        public static IQueryable<T2>
            Apply<T1, T2>(this IQueryable<T1> source,
                          IQueryable<T1>[] otherSources,
                          Expression<Func<IEnumerable<T1>[], IEnumerable<T2>>> applyFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IQueryable<T1>[] allSources = new IQueryable<T1>[otherSources.Length + 1];
                allSources[0] = source;
                for (int i = 0; i < otherSources.Length; ++i)
                {
                    allSources[i + 1] = otherSources[i];
                }
                var q = applyFunc.Compile()(allSources).AsQueryable();
                return new DryadLinqLocalQuery<T2>(source.Provider, q);
            }

            Expression[] others = new Expression[otherSources.Length];
            for (int i = 0; i < otherSources.Length; i++)
            {
                others[i] = otherSources[i].Expression;
            }

            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.NewArrayInit(typeof(IQueryable<T1>), others),
                                       Expression.Quote(applyFunc) }
                    ));
        }

        /// <summary>
        /// Compute applyFunc (source)
        /// </summary>
        /// <typeparam name="T1">The type of the records of the input dataset</typeparam>
        /// <typeparam name="T2">The type of the records of the output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="applyFunc ">The function to be applied to the input dataset</param>
        /// <returns>The result of computing applyFunc(source)</returns>
        public static IQueryable<T2>
            ApplyPerPartition<T1, T2>(
                        this IQueryable<T1> source,
                        Expression<Func<IEnumerable<T1>, IEnumerable<T2>>> applyFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = applyFunc.Compile()(source).AsQueryable();
                return new DryadLinqLocalQuery<T2>(source.Provider, q);
            }
            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(applyFunc) }
                    ));
        }

        /// <summary>
        /// Compute applyFunc(source1, source2)
        /// </summary>
        /// <typeparam name="T1">The type of the records of the first input dataset</typeparam>
        /// <typeparam name="T2">The type of the records of the second input dataset</typeparam>
        /// <typeparam name="T3">he type of the records of the output dataset</typeparam>
        /// <param name="source1">The first input dataset</param>
        /// <param name="source2">The second input dataset</param>
        /// <param name="applyFunc ">The function to be applied to the input datasets</param>
        /// <param name="isFirstOnly">True if only distributive over the first dataset</param>
        /// <returns>The result of computing applyFunc(source1, source2)</returns>
        public static IQueryable<T3>
            ApplyPerPartition<T1, T2, T3>(
                        this IQueryable<T1> source1,
                        IQueryable<T2> source2,
                        Expression<Func<IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>>> applyFunc,
                        bool isFirstOnly)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException("source1");
            }
            if (source2 == null)
            {
                throw new ArgumentNullException("source2");
            }
            if (IsLocalDebugSource(source1))
            {
                var q = applyFunc.Compile()(source1, source2).AsQueryable();
                return new DryadLinqLocalQuery<T3>(source1.Provider, q);
            }
            return source1.Provider.CreateQuery<T3>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2), typeof(T3)),
                    new Expression[] { source1.Expression,
                                       source2.Expression,
                                       Expression.Quote(applyFunc),
                                       Expression.Constant(isFirstOnly) }
                    ));
        }

        /// <summary>
        /// Compute applyFunc on multiple sources
        /// </summary>
        /// <typeparam name="T1">The type of the records of input</typeparam>
        /// <typeparam name="T2">The type of the records of output</typeparam>
        /// <param name="source">The first input dataset</param>
        /// <param name="otherSources">Other input datasets</param>  
        /// <param name="applyFunc">The function to be applied to the input datasets</param>
        /// <param name="isFirstOnly">True if only distributive over the first dataset</param>
        /// <returns>The result of computing applyFunc(source,pieces)</returns>
        public static IQueryable<T2>
            ApplyPerPartition<T1, T2>(
                          this IQueryable<T1> source,
                          IQueryable<T1>[] otherSources,
                          Expression<Func<IEnumerable<T1>[], IEnumerable<T2>>> applyFunc,
                          bool isFirstOnly)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IQueryable<T1>[] allSources = new IQueryable<T1>[otherSources.Length + 1];
                allSources[0] = source;
                for (int i = 0; i < otherSources.Length; ++i)
                {
                    allSources[i + 1] = otherSources[i];
                }
                var q = applyFunc.Compile()(allSources).AsQueryable();
                return new DryadLinqLocalQuery<T2>(source.Provider, q);
            }

            Expression[] others = new Expression[otherSources.Length];
            for (int i = 0; i < otherSources.Length; i++)
            {
                others[i] = otherSources[i].Expression;
            }

            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.NewArrayInit(typeof(IQueryable<T1>), others),
                                       Expression.Quote(applyFunc),
                                       Expression.Constant(isFirstOnly) }
                    ));
        }

        /// <summary>
        /// Apply a function on every sliding window on the input sequence of records.
        /// </summary>
        /// <typeparam name="T1">The type of the input records</typeparam>
        /// <typeparam name="T2">The type of the output records</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="procFunc">The function to apply to every sliding window</param>
        /// <param name="windowSize">The size of the window</param>
        /// <returns></returns>
        public static IQueryable<T2>
            SlidingWindow<T1, T2>(this IQueryable<T1> source,
                                  Expression<Func<IEnumerable<T1>, T2>> procFunc,
                                  Int32 windowSize)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (windowSize < 2)
            {
                throw new DryadLinqException(SR.WindowSizeMustyBeGTOne);
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SlidingWindow(source, procFunc.Compile(), windowSize);
                return new DryadLinqLocalQuery<T2>(source.Provider, q.AsQueryable());
            }
            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(procFunc),
                                       Expression.Constant(windowSize, typeof(int))}
                    ));
        }

        public static IQueryable<T2>
            SelectWithPartitionIndex<T1, T2>(this IQueryable<T1> source,
                                             Expression<Func<T1, int, T2>> procFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SelectWithPartitionIndex(source, procFunc.Compile());
                return new DryadLinqLocalQuery<T2>(source.Provider, q.AsQueryable());
            }
            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(procFunc) }
                    ));
        }
        
        public static IQueryable<T2>
            ApplyWithPartitionIndex<T1, T2>(this IQueryable<T1> source,
                                            Expression<Func<IEnumerable<T1>, int, IEnumerable<T2>>> procFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = procFunc.Compile()(source, 0);
                return new DryadLinqLocalQuery<T2>(source.Provider, q.AsQueryable());
            }
            return source.Provider.CreateQuery<T2>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T1), typeof(T2)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(procFunc) }
                    ));
        }

        public static IQueryable<bool> AnyAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AnyAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<bool> AnyAsQuery<TSource>(this IQueryable<TSource> source,
                                                           Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AnyAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<bool> AllAsQuery<TSource>(this IQueryable<TSource> source,
                                                           Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AllAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<int> CountAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.CountAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<int> CountAsQuery<TSource>(this IQueryable<TSource> source,
                                                            Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.CountAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<long> LongCountAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.LongCountAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<long> LongCountAsQuery<TSource>(this IQueryable<TSource> source,
                                                                 Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.LongCountAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<bool>
            ContainsAsQuery<TSource>(this IQueryable<TSource> source, TSource item)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.ContainsAsQuery(source, item).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Constant(item, typeof(TSource)) }
                    ));
        }

        public static IQueryable<bool>
            ContainsAsQuery<TSource>(this IQueryable<TSource> source,
                                     TSource item,
                                     IEqualityComparer<TSource> comparer) {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.ContainsAsQuery(source, item, comparer).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }            
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression,
                                       Expression.Constant(item, typeof(TSource)),
                                       Expression.Constant(comparer, typeof(IEqualityComparer<TSource>)) }
                    ));
        }

        private static Expression GetSourceExpression<TSource>(IEnumerable<TSource> source)
        {
            IQueryable<TSource> q = source as IQueryable<TSource>;
            if (q != null) return q.Expression;
            return Expression.Constant(source.ToArray(), typeof(TSource[]));
        }

        public static IQueryable<bool>
            SequenceEqualAsQuery<TSource>(this IQueryable<TSource> source1,
                                          IEnumerable<TSource> source2)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException("source1");
            }
            if (source2 == null)
            {
                throw new ArgumentNullException("source2");
            }
            if (IsLocalDebugSource(source1))
            {
                var q = HpcLinqEnumerable.SequenceEqualAsQuery(source1, source2).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source1.Provider, q);
            }
            return source1.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source1.Expression, GetSourceExpression(source2) }
                    ));
        }
        
        public static IQueryable<bool>
            SequenceEqualAsQuery<TSource>(this IQueryable<TSource> source1,
                                          IEnumerable<TSource> source2,
                                          IEqualityComparer<TSource> comparer)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException("source1");
            }
            if (source2 == null)
            {
                throw new ArgumentNullException("source2");
            }
            if (IsLocalDebugSource(source1))
            {
                var q = HpcLinqEnumerable.SequenceEqualAsQuery(source1, source2, comparer).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source1.Provider, q);
            }            
            return source1.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { 
                        source1.Expression, 
                        GetSourceExpression(source2),
                        Expression.Constant(comparer, typeof(IEqualityComparer<TSource>)) 
                        }
                    ));
        }
        
        public static IQueryable<TSource> FirstAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.FirstAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<TSource>
            FirstAsQuery<TSource>(this IQueryable<TSource> source,
                                  Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.FirstAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<TSource> LastAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.LastAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<TSource>
            LastAsQuery<TSource>(this IQueryable<TSource> source,
                                 Expression<Func<TSource, bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.LastAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<TSource> SingleAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SingleAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<TSource>
            SingleAsQuery<TSource>(this IQueryable<TSource> source,
                                   Expression<Func<TSource,bool>> predicate)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SingleAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        public static IQueryable<TSource> MinAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.MinAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<TResult>
            MinAsQuery<TSource,TResult>(this IQueryable<TSource> source, Expression<Func<TSource,TResult>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.MinAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<TSource> MaxAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.MaxAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<TResult>
            MaxAsQuery<TSource,TResult>(this IQueryable<TSource> source,
                                        Expression<Func<TSource,TResult>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.MaxAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<int> SumAsQuery(this IQueryable<int> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<int?> SumAsQuery(this IQueryable<int?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<long> SumAsQuery(this IQueryable<long> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<long?> SumAsQuery(this IQueryable<long?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<float> SumAsQuery(this IQueryable<float> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<float?> SumAsQuery(this IQueryable<float?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double> SumAsQuery(this IQueryable<double> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double?> SumAsQuery(this IQueryable<double?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<decimal> SumAsQuery(this IQueryable<decimal> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<decimal?> SumAsQuery(this IQueryable<decimal?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<int>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,int>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<int?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,int?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<long>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,long>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<long?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,long?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<float>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,float>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<float?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,float?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,double>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,double?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<decimal>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,decimal>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<decimal?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource,decimal?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double> AverageAsQuery(this IQueryable<int> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double?> AverageAsQuery(this IQueryable<int?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double> AverageAsQuery(this IQueryable<long> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double?> AverageAsQuery(this IQueryable<long?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<float> AverageAsQuery(this IQueryable<float> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()),
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<float?> AverageAsQuery(this IQueryable<float?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()),
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double> AverageAsQuery(this IQueryable<double> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double?> AverageAsQuery(this IQueryable<double?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<decimal> AverageAsQuery(this IQueryable<decimal> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<decimal?> AverageAsQuery(this IQueryable<decimal?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        public static IQueryable<double>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,int>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,int?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<float>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource, float>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<float?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource, float?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,long>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,long?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,double>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<double?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,double?>> selector) {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<decimal>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,decimal>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<decimal?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource,decimal?>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        public static IQueryable<TSource>
            AggregateAsQuery<TSource>(this IQueryable<TSource> source,
                                      Expression<Func<TSource,TSource,TSource>> func)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (func == null)
            {
                throw new ArgumentNullException("func");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AggregateAsQuery(source, func.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(func) }
                    ));
        }

        public static IQueryable<TAccumulate>
            AggregateAsQuery<TSource,TAccumulate>(this IQueryable<TSource> source,
                                                  TAccumulate seed,
                                                  Expression<Func<TAccumulate,TSource,TAccumulate>> func)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (func == null)
            {
                throw new ArgumentNullException("func");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AggregateAsQuery(source, seed, func.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TAccumulate>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TAccumulate>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TAccumulate)), 
                    new Expression[] { source.Expression, Expression.Constant(seed), Expression.Quote(func) }
                    ));
        }

        public static IQueryable<TResult>
            AggregateAsQuery<TSource,TAccumulate,TResult>(this IQueryable<TSource> source,
                                                          TAccumulate seed,
                                                          Expression<Func<TAccumulate,TSource,TAccumulate>> func,
                                                          Expression<Func<TAccumulate,TResult>> selector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (func == null)
            {
                throw new ArgumentNullException("func");
            }
            if (selector == null)
            {
                throw new ArgumentNullException("selector");
            }
            if (IsLocalDebugSource(source))
            {
                var q = HpcLinqEnumerable.AggregateAsQuery(source, seed, func.Compile(), selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TAccumulate), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Constant(seed), Expression.Quote(func), Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Instruct DryadLINQ to assume that the dataset is hash partitioned.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the keys on which the partition is based</typeparam>
        /// <param name="source">The dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <returns>The same dataset as input</returns>
        public static IQueryable<TSource>
            AssumeHashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                               Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector) }
                    ));
        }

        public static IQueryable<TSource>
            AssumeHashPartition<TSource, TKey>(this IQueryable<TSource> source,
                                               Expression<Func<TSource, TKey>> keySelector,
                                               IEqualityComparer<TKey> comparer)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IEqualityComparer<TKey>)) }
                    ));
        }

        /// <summary>
        /// Instruct DryadLINQ to assume that the dataset is range partitioned.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="isDescending">true if the partition keys are ordered descendingly</param>
        /// <returns>The same dataset as input</returns>
        public static IQueryable<TSource>
            AssumeRangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                                Expression<Func<TSource, TKey>> keySelector,
                                                bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(isDescending) }
                    ));
        }
        
        public static IQueryable<TSource>
            AssumeRangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                                Expression<Func<TSource, TKey>> keySelector,
                                                IComparer<TKey> comparer,
                                                bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)),
                                       Expression.Constant(isDescending) }
                    ));
        }

        public static IQueryable<TSource>
            AssumeRangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                                Expression<Func<TSource, TKey>> keySelector,
                                                TKey[] rangeSeparators)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (rangeSeparators == null)
            {
                throw new ArgumentNullException("rangeSeparators");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(rangeSeparators) }
                    ));
        }

        public static IQueryable<TSource>
            AssumeRangePartition<TSource, TKey>(this IQueryable<TSource> source,
                                                Expression<Func<TSource, TKey>> keySelector,
                                                TKey[] rangeSeparators,
                                                IComparer<TKey> comparer)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (rangeSeparators == null)
            {
                throw new ArgumentNullException("rangeSeparators");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(rangeSeparators),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)) }
                    ));
        }
        
        /// <summary>
        /// Instruct DryadLINQ to assume that each partition of the dataset is ordered. A dataset
        /// is ordered if it is range partitioned and each partition of it is ordered on the same
        /// key. 
        /// </summary>
        /// <typeparam name="TSource">The type of the recrods of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="isDescending">true if the order is descending</param>
        /// <returns>The same dataset as input</returns>
        public static IQueryable<TSource>
            AssumeOrderBy<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector,
                                         bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(isDescending) }
                    ));
        }
        
        public static IQueryable<TSource>
            AssumeOrderBy<TSource, TKey>(this IQueryable<TSource> source,
                                         Expression<Func<TSource, TKey>> keySelector,
                                         IComparer<TKey> comparer,
                                         bool isDescending)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (IsLocalDebugSource(source))
            {
                return source;
            }            
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IComparer<TKey>)),
                                       Expression.Constant(isDescending) }
                    ));
        }

        public static IMultiQueryable<R1, R2>
            Fork<T, R1, R2>(this IQueryable<T> source,
                            Expression<Func<IEnumerable<T>, IEnumerable<ForkTuple<R1, R2>>>> mapper)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IMultiEnumerable<R1, R2> enumerables = HpcLinqEnumerable.Fork(source, mapper.Compile());
                return new MultiQueryable<T, R1, R2>(source, enumerables);
            }
        
            Expression expr = Expression.Call(
                null,
                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(R1), typeof(R2)),
                new Expression[] { source.Expression,
                                   Expression.Quote(mapper) }
                );
            return new MultiQueryable<T, R1, R2>(source, expr);
        }
        
        public static IMultiQueryable<R1, R2, R3>
            Fork<T, R1, R2, R3>(this IQueryable<T> source,
                                Expression<Func<IEnumerable<T>, IEnumerable<ForkTuple<R1, R2, R3>>>> mapper)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IMultiEnumerable<R1, R2, R3> enumerables = HpcLinqEnumerable.Fork(source, mapper.Compile());
                return new MultiQueryable<T, R1, R2, R3>(source, enumerables);
            }
          
            Expression expr = Expression.Call(
                null,
                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(R1), typeof(R2), typeof(R3)),
                new Expression[] { source.Expression,
                                   Expression.Quote(mapper) }
                );
            return new MultiQueryable<T, R1, R2, R3>(source, expr);
        }
        
        /// <summary>
        /// Compute two output datasets from one input dataset.
        /// </summary>
        /// <typeparam name="T">The type of records of input dataset</typeparam>
        /// <typeparam name="R1">The type of records of first output dataset</typeparam>
        /// <typeparam name="R2">The type of records of second output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="mapper">The function applied to each record of the input</param>
        /// <returns>An IMultiQueryable for the two output dataset</returns>
        public static IMultiQueryable<R1, R2>
            Fork<T, R1, R2>(this IQueryable<T> source,
                                  Expression<Func<T, ForkTuple<R1, R2>>> mapper)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IMultiEnumerable<R1, R2> enumerables = HpcLinqEnumerable.Fork(source, mapper.Compile());
                return new MultiQueryable<T, R1, R2>(source, enumerables);
            }
            
            Expression expr = Expression.Call(
                null,
                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(R1), typeof(R2)),
                new Expression[] { source.Expression,
                                   Expression.Quote(mapper) }
                );
            return new MultiQueryable<T, R1, R2>(source, expr);
        }
        
        public static IMultiQueryable<R1, R2, R3>
            Fork<T, R1, R2, R3>(this IQueryable<T> source,
                                Expression<Func<T, ForkTuple<R1, R2, R3>>> mapper)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                IMultiEnumerable<R1, R2, R3> enumerables = HpcLinqEnumerable.Fork(source, mapper.Compile());
                return new MultiQueryable<T, R1, R2, R3>(source, enumerables);
            }
            
            Expression expr = Expression.Call(
                null,
                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(R1), typeof(R2), typeof(R3)),
                new Expression[] { source.Expression,
                                   Expression.Quote(mapper) }
                );
            return new MultiQueryable<T, R1, R2, R3>(source, expr);
        }
        
        public static IKeyedMultiQueryable<TSource, TKey>
            Fork<TSource, TKey>(this IQueryable<TSource> source,
                                Expression<Func<TSource, TKey>> keySelector,
                                TKey[] keys)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (keys == null)
            {
                throw new ArgumentNullException("keys");
            }
            if (IsLocalDebugSource(source))
            {
                IMultiEnumerable<TSource> enumerables = HpcLinqEnumerable.Fork(source, keySelector.Compile(), keys);
                return new MultiQueryable<TSource, TKey>(source, keys, enumerables);
            }
        
            Expression expr = Expression.Call(
                null,
                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                new Expression[] { source.Expression,
                                   Expression.Quote(keySelector),
                                   Expression.Constant(keys, typeof(TKey[])) }
                );
            return new MultiQueryable<TSource, TKey>(source, keys, expr);
        }
        
        internal static IQueryable<T> ForkChoose<T>(this IMultiQueryable source, int index)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            return source.Provider.CreateQuery<T>(
                Expression.Call(null,
                                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)),
                                new Expression[] { source.Expression, 
                                                   Expression.Constant(index) }));
        }

        /// <summary>
        /// Specifies a DSC stream to be populated with data during query execution. 
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="dscService">A DSC service</param>
        /// <param name="streamName">A DSC stream name</param>
        /// <returns>A query representing the output data.</returns>
        
        // Note: for both cluster&LocalDebug, we add a node to the query-tree
        //       Submit/Materialize will process the ToDsc call in both cases.
        //       This is good for consistency of LocalDebug & Cluser modes -- ie ToDsc is lazy in both cases.
        public static IQueryable<TSource>
            ToDsc<TSource>(this IQueryable<TSource> source, string streamName)
        {  
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (streamName == null)
            {
                throw new ArgumentNullException("streamName");
            }
            if (!(source.Provider is DryadLinqProviderBase))
            {
                //@@TODO[p2]: a "single-input" resource string should be used
                throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, 0), "source");
            }
            HpcLinqContext context = HpcLinqContext.GetContext(source.Provider as DryadLinqProviderBase);

            MethodInfo nongenericMethod = 
                typeof(HpcLinqQueryable)
                .GetMethod(ReflectedNames.DryadLinqIQueryable_ToDscWorker, BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo targetMethod = nongenericMethod.MakeGenericMethod(typeof(TSource));

            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    targetMethod,
                    //((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression,
                                       Expression.Constant(context , typeof(HpcLinqContext)),
                                       Expression.Constant(streamName, typeof(string)) }
                    ));
        }

        /// <summary>
        /// Specifies a HDFS stream to be populated with data during query execution. 
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="dscService">A HDFS service</param>
        /// <param name="streamName">A HDFS stream name</param>
        /// <returns>A query representing the output data.</returns>

        // Note: for both cluster&LocalDebug, we add a node to the query-tree
        //       Submit/Materialize will process the ToHdfs call in both cases.
        //       This is good for consistency of LocalDebug & Cluser modes -- ie ToHdfs is lazy in both cases.
        public static IQueryable<TSource> ToHdfs<TSource>(this IQueryable<TSource> source, string streamName)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (streamName == null)
            {
                throw new ArgumentNullException("streamName");
            }
            if (!(source.Provider is DryadLinqProviderBase))
            {
                //@@TODO[p2]: a "single-input" resource string should be used
                throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, 0), "source");
            }
            HpcLinqContext context = HpcLinqContext.GetContext(source.Provider as DryadLinqProviderBase);

            MethodInfo nongenericMethod = 
                typeof(HpcLinqQueryable)
                .GetMethod(ReflectedNames.DryadLinqIQueryable_ToHdfsWorker, BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo targetMethod = nongenericMethod.MakeGenericMethod(typeof(TSource));


            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    targetMethod,
                    //((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression,
                                       Expression.Constant(context , typeof(HpcLinqContext)),
                                       Expression.Constant(streamName, typeof(string)) }
                    ));
        }

        /// <summary>
        /// Specifies a DSC stream to be populated with data during query execution. 
        /// </summary>
        /// <remarks>
        /// This method is not intended for direct use.  To prepare a plain enumerable for 
        /// use with DryadLinq, call AsDryadQuery or AsDryadQueryPartitions.  
        /// </remarks>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="dscService">A DSC service</param>
        /// <param name="streamName">A DSC stream name</param>
        /// <returns>A query representing the output data.</returns>
        // @@TODO[P1]:  The above comments require that AsDryadQuery/AsDryadQueryPartitions exists.. that is a P1 work item.
        //              In the meantime, a basic version of those methods is available in DryadLinqUnitTests.
        //
        // *visited by "LocalDebug-mode GetEnumerator()"
        //      -- in cluster mode, we never enter this method.. DryadQueryGen just inspects the MethodCallExpression
        //      -- in localDebug mode, we do enter this method, particularly via Linq-to-objects GetEnumerator.
        //
        // Behavior:
        // (LocalDebug mode) q.ToDsc().GetEnumerator() -> throws, as execution never occurred, so the output was not created.
        // (LocalDebug mode) var q1 = q.ToDsc();q1.Submit(); foreach(var x in q1) -> succeeds (as submit creates the data)
        //
        internal static IQueryable<TSource>
            ToDscWorker<TSource>(this IEnumerable<TSource> source, HpcLinqContext context, string streamName)
        {
            // We want the following query to succeed reliably for both cluster and LocalDebug :
            //    var q1 = q.ToDsc("..");
            //    q1.Submit(runtime);
            //    foreach(var x in q1){ ... } // calls q1.GetEnumerator()
            // 
            // For cluster execution, q1 becomes data-backed and so the call to GetEnumerator succeeds.
            // For local execution, we don't attach a "databacking DLQ" to the source 
            //   - if we did find a "data-backing DLQ" for the DSC node we would return that node.
            //   - but we don't, so we fake it by looking for the existence of the output and if it exists, we 
            //     assume that we created it and treat it as though we had tracked it as the data-backing.
            //
            // @@BUG(low-severity): This logic could be spoofed if the dsc output already exists but wasn't created via 
            //        the expected call to Submit()/Materialize().. A possible cause is a previous run of almost exactly the same query.
            //        Mitigation: User-education to not rely on auto-delete of output streams will avoid the issue and is a good idea anyway.
            //
            // (fix idea) reimplement all the Linq-to-objects operators ourseleves so that we have more control
            //            during LocalDebug-mode.
            //
            // (fix idea) Introduce a new class DryadLinqQueryLocal. It uses AsQueryable.Provider as query provider 
            //            and has a data-backing field.  May only be feasible if DryadLinqQuery<T> is internal.
            IQueryable<TSource> fakedDataBackingDLQ = null;
            try
            {
                if (context.DscService.FileSetExists(streamName))
                {
                    // if so, try to make a DLQ<TSource> from it.
                    fakedDataBackingDLQ = context.FromDsc<TSource>(streamName);
                }
            }
            catch (DryadLinqException)
            {
                //suppress.. we expect this to occur if the dsc stream does exist, but cannot be loaded as a DLQ<TSource>
            }

            if (fakedDataBackingDLQ != null)
                return fakedDataBackingDLQ;

            throw new DryadLinqException(HpcLinqErrorCode.ToDscUsedIncorrectly, String.Format(SR.ToDscUsedIncorrectly));
        }


        /// <summary>
        /// Specifies a HDFS stream to be populated with data during query execution. 
        /// </summary>
        /// <remarks>
        /// This method is not intended for direct use.  To prepare a plain enumerable for 
        /// use with DryadLinq, call AsDryadQuery or AsDryadQueryPartitions.  
        /// </remarks>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="dscService">A HDFS service</param>
        /// <param name="streamName">A HDFS stream name</param>
        /// <returns>A query representing the output data.</returns>
        // @@TODO[P1]:  The above comments require that AsDryadQuery/AsDryadQueryPartitions exists.. that is a P1 work item.
        //              In the meantime, a basic version of those methods is available in DryadLinqUnitTests.
        //
        // *visited by "LocalDebug-mode GetEnumerator()"
        //      -- in cluster mode, we never enter this method.. DryadQueryGen just inspects the MethodCallExpression
        //      -- in localDebug mode, we do enter this method, particularly via Linq-to-objects GetEnumerator.
        //
        // Behavior:
        // (LocalDebug mode) q.ToHdfs().GetEnumerator() -> throws, as execution never occurred, so the output was not created.
        // (LocalDebug mode) var q1 = q.ToHdfs();q1.Submit(); foreach(var x in q1) -> succeeds (as submit creates the data)
        //
        internal static IQueryable<TSource> ToHdfsWorker<TSource>(this IEnumerable<TSource> source, HpcLinqContext context, string streamName)
        {
            // We want the following query to succeed reliably for both cluster and LocalDebug :
            //    var q1 = q.ToHdfs("..");
            //    q1.Submit(runtime);
            //    foreach(var x in q1){ ... } // calls q1.GetEnumerator()
            // 
            // For cluster execution, q1 becomes data-backed and so the call to GetEnumerator succeeds.
            // For local execution, we don't attach a "databacking DLQ" to the source 
            //   - if we did find a "data-backing DLQ" for the DSC node we would return that node.
            //   - but we don't, so we fake it by looking for the existence of the output and if it exists, we 
            //     assume that we created it and treat it as though we had tracked it as the data-backing.
            //
            // @@BUG(low-severity): This logic could be spoofed if the dsc output already exists but wasn't created via 
            //        the expected call to Submit()/Materialize().. A possible cause is a previous run of almost exactly the same query.
            //        Mitigation: User-education to not rely on auto-delete of output streams will avoid the issue and is a good idea anyway.
            //
            // (fix idea) reimplement all the Linq-to-objects operators ourseleves so that we have more control
            //            during LocalDebug-mode.
            //
            // (fix idea) Introduce a new class DryadLinqQueryLocal. It uses AsQueryable.Provider as query provider 
            //            and has a data-backing field.  May only be feasible if DryadLinqQuery<T> is internal.
            IQueryable<TSource> fakedDataBackingDLQ = null;
            try
            {
                bool exists;
                using (HdfsInstance hdfs = new HdfsInstance(context.HdfsService))
                {
                    exists = hdfs.IsFileExists(streamName);
                }

                if (exists)
                {
                    // if so, try to make a DLQ<TSource> from it.
                    fakedDataBackingDLQ = context.FromHdfs<TSource>(streamName);
                }
            }
            catch (DryadLinqException)
            {
                //suppress.. we expect this to occur if the dsc stream does exist, but cannot be loaded as a DLQ<TSource>
            }

            if (fakedDataBackingDLQ != null)
                return fakedDataBackingDLQ;

            throw new DryadLinqException(HpcLinqErrorCode.ToHdfsUsedIncorrectly, String.Format(SR.ToHdfsUsedIncorrectly));
        }



        /// <summary>
        /// Submits the query and then waits for the job to complete
        /// </summary>
        /// <exception cref="DryadLinqException">If the job completes in error or is cancelled.</exception>
        /// <exception cref="DryadLinqException">If repeated errors occur while polling for status.</exception>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <returns>Information about the execution job.</returns>
        public static HpcLinqJobInfo SubmitAndWait<TSource>(this IQueryable<TSource> source)
        {
            HpcLinqJobInfo info = source.Submit();
            info.Wait();
            return info;
        }

        /// <summary>
        /// Submits the query for asynchronous execution.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="dscFileSetUri">The URI of the DSC file set to be created</param>
        /// <returns>Information about the execution job.</returns>
        // @@TODO[P1]. Try to unify q.Submit() and the static Submit.  They duplicate a fair bit of code. 
        //             Changing this method to work with untyped IQueryable (and not mention TSource) should 
        //             make it easy to forward the call to submit( new [] {source} )
        public static HpcLinqJobInfo Submit<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            //Extract the context.
            if (!(source.Provider is DryadLinqProviderBase))
            {
                //@@TODO[p2]: a "single-input" resource string should be used
                throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, 0), "source");
            }
            HpcLinqContext context = HpcLinqContext.GetContext(source.Provider as DryadLinqProviderBase);

            if (IsLocalDebugSource(source))
            {
                try
                {
                    // if ToDsc is present, extract out the target, otherwise make an anonymous target
                    // then ingress the data directly to DSC.

                    MethodCallExpression mcExpr1 = source.Expression as MethodCallExpression;
                    if (mcExpr1 != null && mcExpr1.Method.Name == ReflectedNames.DryadLinqIQueryable_ToDscWorker)
                    {
                        // visted by LocalDebug:: q2.ToDsc(...).Submit(...), eg Test3.
                        LocalDebug_ProcessToDscExpression<TSource>(context, mcExpr1);
                    }
                    else
                    {
                        // visited by (LocalDebug mode) q-nonToDsc.Submit();
                        string fileSetName = DataPath.MakeUniqueTemporaryDscFileSetName();
                        
                        DscCompressionScheme outputScheme = context.Configuration.IntermediateDataCompressionScheme;
                        DryadLinqMetaData metadata = DryadLinqMetaData.ForLocalDebug(context, typeof(TSource), fileSetName, outputScheme );
                        DataProvider.IngressTemporaryDataDirectlyToDsc(context, source, fileSetName, metadata, outputScheme);
                    }

                    return new HpcLinqJobInfo(HpcLinqJobInfo.JOBID_LOCALDEBUG, null, null, null);
                }
                catch (Exception e)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.CreatingDscDataFromLocalDebugFailed,
                                               String.Format(SR.CreatingDscDataFromLocalDebugFailed), e);
                }
            }

            if (!(source is DryadLinqQuery))
            {
                //@@TODO[p2]: a "single-input" resource string should be used
                throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, 0), "source");
            }

            // handle repeat submissions.
            if (((DryadLinqQuery)source).IsDataBacked)
            {
                // this query has already been submitted.
                throw new ArgumentException(string.Format(SR.AlreadySubmitted), "source");
            }

            // sanity check that we have a normal DryadLinqQuery
            MethodCallExpression mcExpr = source.Expression as MethodCallExpression;
            if (mcExpr == null)
            {
                throw new ArgumentException(String.Format(SR.AtLeastOneOperatorRequired, 0), "source");
            }

            if (!mcExpr.Method.IsStatic ||
               !TypeSystem.IsQueryOperatorCall(mcExpr))
            {
                //@@TODO[p2]: a "single-input" resource string should be used
                throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, 0), "source");
            }

            if (mcExpr.Method.Name != ReflectedNames.DryadLinqIQueryable_ToDscWorker && mcExpr.Method.Name != ReflectedNames.DryadLinqIQueryable_ToHdfsWorker)
            {
                // Support for non-ToDscQuery.Submit()
                string tableName = DataPath.MakeUniqueTemporaryDscFileSetUri(context);
                mcExpr = (MethodCallExpression) source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    typeof(HpcLinqQueryable).GetMethod(ReflectedNames.DryadLinqIQueryable_AnonymousDscPlaceholder,
                                                       BindingFlags.Static | BindingFlags.NonPublic).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression,
                                       Expression.Constant(tableName, typeof(string))}
                    )).Expression;
            }

            // Execute the queries
            HpcLinqQueryGen dryadGen = new HpcLinqQueryGen(context,
                                                           ((DryadLinqQuery)source).GetVertexCodeGen(),
                                                           new Expression[] { mcExpr });
            DryadLinqQuery[] tables = dryadGen.InvokeDryad();


            tables[0].IsTemp = false;
            ((DryadLinqQuery)source).BackingDataDLQ = tables[0];

            int jobId = tables[0].QueryExecutor.JobSubmission.GetJobId();
            string[] targetUris = new [] {tables[0].DataSourceUri};
            return new HpcLinqJobInfo(jobId, context.Configuration.HeadNode, tables[0].QueryExecutor, targetUris);
        }
        
        // inspect and process the trailing ToDsc() node in local-debug scenarios.
        private static void LocalDebug_ProcessToDscExpression<TSource>(HpcLinqContext context, MethodCallExpression mcExpr1)
        {
            // get the dsc target out of the ToDsc node.
            DscService dsc = context.DscService;
            string fileSetName = (string)((ConstantExpression)mcExpr1.Arguments[2]).Value;

            // evaluate the source query before the ToDsc node.
            // WAS MethodCallExpression mce = ((MethodCallExpression)mcExpr1.Arguments[0]);
            // DIDN't handle constant expression for plain-data.
            Expression e = mcExpr1.Arguments[0];

            DscCompressionScheme compressionScheme = context.Configuration.OutputDataCompressionScheme;
            ExecuteLocalExpressionAndIngressToDsc<TSource>(context, e, fileSetName, compressionScheme);
        }

        // process the main data and push into DSC, used in local-debug scenarios.
        private static void ExecuteLocalExpressionAndIngressToDsc<TSource>(HpcLinqContext context,
                                                                           Expression mce,
                                                                           string fileSetName,
                                                                           DscCompressionScheme compressionScheme)
        {
            ExpressionSimplifier<IEnumerable<TSource>> simplifier = new ExpressionSimplifier<IEnumerable<TSource>>();
            IEnumerable<TSource> sourceData = simplifier.Eval(mce);

            // stuff the data into DSC
            DryadLinqMetaData metadata = DryadLinqMetaData.ForLocalDebug(context, typeof(TSource), fileSetName, compressionScheme);
            DataProvider.IngressDataDirectlyToDsc(context, sourceData, fileSetName, metadata, compressionScheme);
        }

        // this method is never directly executed
        // it's only use is as a placeholder in an expression tree for the scenario: query-nonDsc.Submit()
        // This appears in expression trees in the same place that "ToDscWorker" would otherwise appear.
        internal static DryadLinqQuery<TSource>
            AnonymousDscTarget__Placeholder<TSource>(this IEnumerable<TSource> source, string dscFileSetUri)
        {
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Submits a collection of HPC LINQ queries for execution.  
        /// </summary>
        /// <param name="sources">Queries to execute.</param>
        /// <returns>Job information for tracking the execution.</returns>
        /// <remarks>
        /// Every item in sources must be an HPC LINQ IQueryable object that terminates with ToDsc()
        /// Only one job will be executed, but the job will produce the output associated with each item in sources.
        /// </remarks>
        public static HpcLinqJobInfo Submit(params IQueryable[] sources)
        {
            if (sources == null)
            {
                throw new ArgumentNullException("sources");
            }
            
            if(sources.Length == 0)
            {
                //@@TODO[p2]: localize message.
                throw new ArgumentException("sources is empty", "sources");
            }

            HpcLinqContext commonContext = CheckSourcesAndGetCommonContext(sources);
            return HpcLinqQueryable.Materialize(commonContext, sources);
        }

        /// <summary>
        /// Submits a collection of HPC LINQ queries for execution and waits for the job to complete/
        /// </summary>
        /// <exception cref="DryadLinqException">If the job completes in error or is cancelled.</exception>
        /// <exception cref="DryadLinqException">If repeated errors occur while polling for status.</exception>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <returns>Information about the execution job.</returns>
        /// <remarks>
        /// Every item in sources must be an HPC LINQ IQueryable object that terminates with ToDsc()
        /// Only one job will be executed, but the job will produce the output associated with each item in sources.
        /// </remarks>
        public static HpcLinqJobInfo SubmitAndWait(params IQueryable[] sources)
        {
            if (sources == null)
            {
                throw new ArgumentNullException("sources");
            }

            if (sources.Length == 0)
            {
                throw new ArgumentException("sources is empty", "sources");
            }

            HpcLinqContext commonContext = CheckSourcesAndGetCommonContext(sources);
            HpcLinqJobInfo info = HpcLinqQueryable.Materialize(commonContext, sources);
            info.Wait();
            return info;
        }

        private static HpcLinqContext CheckSourcesAndGetCommonContext(IQueryable[] sources)
        {
            Debug.Assert(sources != null && sources.Length > 0);
            for (int i = 0; i < sources.Length; i++)
            {
                if (sources[i] == null)
                {
                    //@@TODO[p1]: localize
                    throw new ArgumentException(string.Format("An item in sources[] was null. sources[{0}]", i), "sources"); 
                }
                
                // sanity check that we have normal DryadLinqQuery objects
                if (!(sources[i].Provider is DryadLinqProviderBase))
                {
                    throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, i), "sources");
                }
            }

            //check for duplicate query objects 
            HashSet<IQueryable> repeatedQueryDetector = new HashSet<IQueryable>();
            for (int i = 0; i < sources.Length; i++)
            {
                var q = sources[i];
                if (repeatedQueryDetector.Contains(q))
                {
                    throw new ArgumentException(string.Format(SR.SameQuerySubmittedMultipleTimesInMaterialize),
                                                string.Format("sources[{0}]", i));
                }
                repeatedQueryDetector.Add(q);
            }

            //Check the queries all use the same context
            HpcLinqContext[] contexts = sources.Select(src => HpcLinqContext.GetContext(src.Provider as DryadLinqProviderBase)).ToArray();
            if (contexts.Distinct().Count() != 1)
            {
                //@@TODO[p1]: localize message
                throw new ArgumentException("Each query must be created from the same HpcLinqContext object", "sources");
            }

            HpcLinqContext commonContext = contexts[0];
            return commonContext;
        }

        /// <summary>
        /// Force the execution of a list of queries and create the partitioned tables for the
        /// results of the queries.
        /// </summary>
        /// <param name="sources">The list of lazy tables to be materialized</param>
        /// <returns>A list of partitioned tables</returns>
        internal static HpcLinqJobInfo Materialize(HpcLinqContext context, params IQueryable[] sources)
        {
            if (context.Configuration.LocalDebug) 
            {
                //@@TODO[P1]: force streams to be be temporary if automatically named. Not doing this might be considered a bug.
                foreach (var source in sources)
                {
                    MethodCallExpression mcExpr1 = source.Expression as MethodCallExpression;
                    if (mcExpr1 == null)
                    {
                        //normally we have a method call, but IDryadLinqQueryable.HashPartition() will just return source in
                        //localDebug mode.  hence ctx.Submit(src.HashPartition()) will just present src here.
                        Debug.Assert(source.Expression is ConstantExpression,
                                     "mcExpr1 should be MethodCallExpression or ConstantExpression");
                    }
                    
                    if (mcExpr1 != null && mcExpr1.Method.Name == ReflectedNames.DryadLinqIQueryable_ToDscWorker)
                    {
                        // visted by LocalDebug:: Materialize( {q2.ToDsc(...)} ), eg Test5.
                        var method = 
                            typeof(HpcLinqQueryable)
                                .GetMethod(ReflectedNames.HpcLinqQueryable_LocalDebug_ProcessToDscExpression,
                                           BindingFlags.NonPublic | BindingFlags.Static)
                                .MakeGenericMethod(source.ElementType);
                        try
                        {
                            method.Invoke(null, new object[] { context, mcExpr1 });
                        }
                        catch (TargetInvocationException tie)
                        {
                            if (tie.InnerException != null)
                                throw tie.InnerException; // unwrap and rethrow original exception
                            else 
                                throw; // this shouldn't occur.. but just in case.
                        }
                    }
                    else
                    {
                        // visted by LocalDebug:: Materialize( {q2.Non-ToDsc(...)} ), eg Test6.
                        string tableName = DataPath.MakeUniqueTemporaryDscFileSetName();
                        DscCompressionScheme compressionScheme = context.Configuration.IntermediateDataCompressionScheme;
                        var method = 
                            typeof(HpcLinqQueryable)
                               .GetMethod(ReflectedNames.HpcLinqQueryable_ExecuteLocalExpressionAndIngressToDsc,
                                          BindingFlags.NonPublic | BindingFlags.Static)
                               .MakeGenericMethod(source.ElementType);

                        try
                        {
                            method.Invoke(null, new object[] { context, source.Expression, tableName, compressionScheme });
                        }
                        catch (TargetInvocationException tie)
                        {
                            if (tie.InnerException != null)
                                throw tie.InnerException; // unwrap and rethrow original exception
                            else
                                throw; // this shouldn't occur.. but just in case.
                        }
                    }
                }

                return new HpcLinqJobInfo(HpcLinqJobInfo.JOBID_LOCALDEBUG, null, null, null);
            }
            else
            {
                // If the sources were not terminated with ToDsc(), do this now and provide a temporary output name
                Expression[] qList = new Expression[sources.Length];
                bool[] isTemps = new bool[sources.Length];
                for (int i = 0; i < sources.Length; i++)
                {
                    isTemps[i] = false;
                    MethodCallExpression mcExpr = sources[i].Expression as MethodCallExpression;

                    // sanity check that we have a normal DryadLinqQuery
                    if (!(sources[i] is DryadLinqQuery))
                    {
                        throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, i),
                                                    string.Format("sources[{0}]", i));
                    }

                    // check that the query has not already been submitted.
                    if (((DryadLinqQuery)sources[i]).IsDataBacked)
                    {
                        throw new ArgumentException(string.Format(SR.AlreadySubmittedInMaterialize),
                                                    string.Format("sources[{0}]", i));
                    }

                    // more checks that we have a normal, unsubmitted, non-trivial DryadLinqQuery
                    if (!(sources[i] is DryadLinqQuery) ||
                        mcExpr == null ||
                        !mcExpr.Method.IsStatic ||
                        !TypeSystem.IsQueryOperatorCall(mcExpr))
                    {
                        throw new ArgumentException(String.Format(SR.NotAHpcLinqQuery, i),
                                                    string.Format("sources[{0}]", i));
                    }

                    if (mcExpr.Method.Name != ReflectedNames.DryadLinqIQueryable_ToDscWorker)
                    {
                        isTemps[i] = true;
                        string tableName = DataPath.MakeUniqueTemporaryDscFileSetUri(context);
                        MethodInfo minfo = 
                            typeof(HpcLinqQueryable)
                            .GetMethod(ReflectedNames.DryadLinqIQueryable_AnonymousDscPlaceholder,
                                       BindingFlags.Static | BindingFlags.NonPublic);
                        Type elemType = mcExpr.Type.GetGenericArguments()[0];
                        minfo = minfo.MakeGenericMethod(elemType);
                        mcExpr = Expression.Call(minfo, mcExpr, Expression.Constant(tableName));
                    }
                    qList[i] = mcExpr;
                }

                // Normal cluster execution -- prepare and submit the query to Dryad.
                // Execute the queries, which are all now terminated by ToDsc()
                VertexCodeGen vertexCodeGen = ((DryadLinqQuery)sources[0]).GetVertexCodeGen();
                HpcLinqQueryGen queryGen = new HpcLinqQueryGen(context, vertexCodeGen, qList);
                DryadLinqQuery[] tables = queryGen.InvokeDryad();

                // Store the results in the queries
                for (int j = 0; j < sources.Length; j++)
                {
                    tables[j].IsTemp = isTemps[j];
                    ((DryadLinqQuery)sources[j]).BackingDataDLQ = tables[j];
                }

                // return a runtimeInfo.
                int jobId = tables[0].QueryExecutor.JobSubmission.GetJobId();
                string[] targetUris = new string[tables.Length];
                for (int i = 0; i < tables.Length; i++)
                {
                    targetUris[i] = tables[i].DataSourceUri;
                }
                return new HpcLinqJobInfo(jobId, context.Configuration.HeadNode, tables[0].QueryExecutor, targetUris);
            }
        }
    }
}
