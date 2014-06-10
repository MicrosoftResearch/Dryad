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
using System.Linq.Expressions;
using System.Linq;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{   
    /// <summary>
    /// This class extends LINQ with a set of new operators that are specific to DryadLINQ.
    /// The new operators includes partitioning operators (HashPartition and RangePartition)
    /// and the Apply operator that enables stateful transformations on datasets.
    /// </summary>
    public static class DryadLinqQueryable
    {
        internal static bool IsLocalDebugSource(IQueryable source)
        {
            return !(source.Provider is DryadLinqProvider);
        }

        /// <summary>
        /// Filters a sequence of values based on a predicate. Each element's index is used in 
        /// the logic of the predicate function.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The sequence of elements to filter</param>
        /// <param name="predicate">The filter predicate.</param>
        /// <returns>The elements in the input that satisfy the predicate</returns>
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
                var result = DryadLinqEnumerable.LongWhere(source, predicate.Compile()).AsQueryable();
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
        /// Transforms each element of a sequence into a new form by applying a function of 
        /// the element and its index.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TResult">The type of the elements of the result</typeparam>
        /// <param name="source">The sequence of input elements</param>
        /// <param name="selector">A transform function to apply to each source element; 
        /// the second parameter of the function represents the index of the source element.</param>
        /// <returns>The sequence resulting from applying the transformation function on each input element</returns>
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
                var result = DryadLinqEnumerable.LongSelect(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            } 

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Transforms each element of a sequence into an IEnumerable{T} by applying a function to the 
        /// element and its index, and then flattens the resulting sequences into one sequence. 
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TResult">The type of the elements of the result</typeparam>
        /// <param name="source">The sequence of input elements</param>
        /// <param name="selector">A transform function to apply to each source element; 
        /// the second parameter of the function represents the index of the source element.</param>
        /// <returns>The sequence resulting from applying the function on each input element and 
        /// flattening the results</returns>
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
                var result = DryadLinqEnumerable.LongSelectMany(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Transforms each element of a sequence into an IEnumerable{T} by applying a function to the 
        /// element and its index, and then flattens the resulting sequences into one sequence. 
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TCollection">The type of the element in the intermediate IEnumerable sequences</typeparam>
        /// <typeparam name="TResult">The type of the elements of the result</typeparam>
        /// <param name="source">The sequence of input elements</param>
        /// <param name="selector">A transform function to apply to each source element; 
        /// the second parameter of the function represents the index of the source element.</param>
        /// <param name="resultSelector">A transformation function to apply to each intermediate element</param>
        /// <returns>The sequence resulting from applying <code>selector</code> to each input element and 
        /// flattening and transforming the elements in the intermediate sequences</returns>
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
                var result = DryadLinqEnumerable.LongSelectMany(source, selector.Compile(), resultSelector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, result);
            }

            return source.Provider.CreateQuery<TResult>( 
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TCollection), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector), Expression.Quote(resultSelector) }
                    ));
        }
        
        /// <summary>
        /// Returns the largest prefix of a sequence such that the elements satisfy a specified predicate.
        /// </summary>
        /// <typeparam name="TSource">The element type of the input sequence</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The largest prefix satisfying the predicate</returns>
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
                var result = DryadLinqEnumerable.LongTakeWhile(source, predicate.Compile()).AsQueryable();
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
        /// Skips elements in a sequence as long as a specified condition is true and then returns the 
        /// remaining elements. The predicate is a function of an element and its index.
        /// </summary>
        /// <typeparam name="TSource">The element type of the input sequence</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The remaining sequence by skipping the elements in the head that satisfy the predicate</returns>
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
                var result = DryadLinqEnumerable.LongSkipWhile(source, predicate.Compile()).AsQueryable();
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
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
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
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
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
        /// <returns>An IQueryable hash-partitioned according to a key </returns>
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
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
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
        /// Hash partition a dataset. The number of resulting partitions is dynamically determined
        /// at the runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <typeparam name="TResult">The type of the records in the result dataset</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="resultSelector">The function to compute output record</param>
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
        public static IQueryable<TResult>
            HashPartition<TSource, TKey, TResult>(this IQueryable<TSource> source,
                                                  Expression<Func<TSource, TKey>> keySelector,
                                                  Expression<Func<TSource, TResult>> resultSelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (resultSelector == null)
            {
                throw new ArgumentNullException("resultSelector");
            }
            if (IsLocalDebugSource(source))
            {
                Func<TSource, TResult> resultSelect = resultSelector.Compile();
                return source.Select(resultSelect).AsQueryable();
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey), typeof(TResult)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Quote(resultSelector) }
                    ));
        }

        /// <summary>
        /// Hash partition a dataset. The number of resulting partitions is dynamically determined
        /// at the runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <typeparam name="TResult">The type of the records in the result dataset</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="resultSelector">The function to compute output record</param>
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
        public static IQueryable<TResult>
            HashPartition<TSource, TKey, TResult>(this IQueryable<TSource> source,
                                                  Expression<Func<TSource, TKey>> keySelector,
                                                  IEqualityComparer<TKey> comparer,
                                                  Expression<Func<TSource, TResult>> resultSelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException("keySelector");
            }
            if (resultSelector == null)
            {
                throw new ArgumentNullException("resultSelector");
            }
            if (IsLocalDebugSource(source))
            {
                Func<TSource, TResult> resultSelect = resultSelector.Compile();
                return source.Select(resultSelect).AsQueryable();
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey), typeof(TResult)),
                    new Expression[] { source.Expression,
                                       Expression.Quote(keySelector),
                                       Expression.Constant(comparer, typeof(IEqualityComparer<TKey>)),
                                       Expression.Quote(resultSelector) }
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
        /// <returns>An IQueryable hash-partitioned according to a key</returns>
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
        /// <param name="partitionCount">The number of partitions in the output dataset</param>
        /// <returns>An IQueryable partitioned according to a list of range keys determined at runtime</returns>
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
        /// <returns>An IQueryable partitioned according to a list of range keys determined at runtime</returns>
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

        /// <summary>
        /// Range partition a dataset using an array of partition keys.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The dataset to be partitioned</param>
        /// <param name="keySelector">The funtion to extract the key from a record</param>
        /// <param name="rangeSeparators">An array of partition keys, either in ascending or descending order</param>
        /// <returns>An IQueryable partitioned according to the specified range keys</returns>
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
            if (!DryadLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, Comparer<TKey>.Default, out dummy))
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
        /// <returns>An IQueryable partitioned according to a list of keys determined at runtime</returns>
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
        
        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the input dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The input dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="isDescending">true if the generated keys must be descending; otherwise ascending</param>
        /// <returns>An IQueryable partitioned according to a list of keys determined at runtime</returns>
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

        /// <summary>
        /// Range partition a dataset using a specified list of keys.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the input dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The input dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="rangeSeparators">The list of range keys</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <returns>An IQueryable partitioned according to a specified list of keys.</returns>
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
                throw new DryadLinqException(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                             string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, typeof(TKey)));
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);

            // check that the range-keys are consistent.
            bool? dummy;
            if (!DryadLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, comparer, out dummy))
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

        /// <summary>
        /// Range partition a dataset. The list of range keys are determined dynamically at runtime.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the input dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The input dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="isDescending">true if the generated keys must be descending; otherwise ascending</param>
        /// <param name="partitionCount">The number of partitions in the output dataset</param>
        /// <returns>An IQueryable partitioned according to a list of keys determined at runtime</returns>
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

        /// <summary>
        /// Range partition a dataset using a specified list of keys.
        /// </summary>
        /// <typeparam name="TSource">The type of the records in the input dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which the partition is based</typeparam>
        /// <param name="source">The input dataset to be partitioned</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="rangeSeparators">The list of range keys</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="isDescending">true if the keys must be in descending order; otherwise false</param>
        /// <returns>An IQueryable partitioned according to a specified list of keys</returns>
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
                throw new DryadLinqException(DryadLinqErrorCode.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable,
                                             string.Format(SR.ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable, typeof(TKey)));
            }
            comparer = TypeSystem.GetComparer<TKey>(comparer);
            
            // check that the range-keys are consistent.
            bool? detectedDescending;
            bool keysAreConsistent = DryadLinqUtil.ComputeIsDescending<TKey>(rangeSeparators, comparer, out detectedDescending);
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
        /// <param name="applyFunc">The function to be applied to the input dataset</param>
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
        /// <param name="applyFunc">The function to be applied to the input datasets</param>
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
                          IQueryable[] otherSources,
                          Expression<Func<IEnumerable<T1>, IEnumerable[], IEnumerable<T2>>> applyFunc)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = applyFunc.Compile()(source, otherSources).AsQueryable();
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
                                       Expression.NewArrayInit(typeof(IQueryable), others),
                                       Expression.Quote(applyFunc) }
                    ));
        }
        
        /// <summary>
        /// Compute applyFunc(source)
        /// </summary>
        /// <typeparam name="T1">The type of the records of the input dataset</typeparam>
        /// <typeparam name="T2">The type of the records of the output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="applyFunc">The function to be applied to the input dataset</param>
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
        /// <param name="applyFunc">The function to be applied to the input datasets</param>
        /// <param name="isFirstOnly">True if only distributive over the first dataset</param>
        /// <returns>The result of computing applyFunc(source1, source2)</returns>
        public static IQueryable<T3>
            ApplyPerPartition<T1, T2, T3>(
                        this IQueryable<T1> source1,
                        IQueryable<T2> source2,
                        Expression<Func<IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>>> applyFunc,
                        bool isFirstOnly = false)
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
        /// <returns>The result of computing applyFunc(source, otherSources)</returns>
        public static IQueryable<T2>
            ApplyPerPartition<T1, T2>(
                          this IQueryable<T1> source,
                          IQueryable<T1>[] otherSources,
                          Expression<Func<IEnumerable<T1>[], IEnumerable<T2>>> applyFunc,
                          bool isFirstOnly = false)
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
                          IQueryable[] otherSources,
                          Expression<Func<IEnumerable<T1>, IEnumerable[], IEnumerable<T2>>> applyFunc,
                          bool isFirstOnly = false)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = applyFunc.Compile()(source, otherSources).AsQueryable();
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
                                       Expression.NewArrayInit(typeof(IQueryable), others),
                                       Expression.Quote(applyFunc),
                                       Expression.Constant(isFirstOnly) }
                    ));
        }

        private static IQueryable<T> Dummy<T>(IQueryable<T> source)
        {
            return source.Provider.CreateQuery<T>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)),
                    new Expression[] { source.Expression }
                    ));
        }

        private static IQueryable<T> DoWhile<T>(IQueryable<T> source,
                                                IQueryable<T> body,
                                                IQueryable<bool> cond,
                                                IQueryable<T> bodySource,
                                                IQueryable<T> condSource1,
                                                IQueryable<T> condSource2)
        {
            return source.Provider.CreateQuery<T>(
                    Expression.Call(
                        null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)),
                        new Expression[] { source.Expression,
                                           body.Expression,
                                           cond.Expression,
                                           bodySource.Expression,
                                           condSource1.Expression,
                                           condSource2.Expression }
                        ));
        }

        /// <summary>
        /// Conditional DoWhile loop.
        /// </summary>
        /// <typeparam name="T">The type of the input records</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="body">The code body of the DoWhile loop</param>
        /// <param name="cond">The termination condition of the DoWhile loop</param>
        /// <returns>The output dataset when the loop terminates</returns>
        public static IQueryable<T>
            DoWhile<T>(this IQueryable<T> source,
                       Func<IQueryable<T>, IQueryable<T>> body,
                       Func<IQueryable<T>, IQueryable<T>, IQueryable<bool>> cond)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.DoWhile(source, body, cond);
                return new DryadLinqLocalQuery<T>(source.Provider, q.AsQueryable());
            }

            DryadLinqContext context = DryadLinqContext.GetContext(source.Provider);
            IQueryable<T> before = source;
            while (true)
            {
                IQueryable<T> after = before;
                after = body(after);
                var more = cond(before, after);
                DryadLinqQueryable.SubmitAndWait(after, more);
                if (!more.Single()) return after;
                before = after;
            }
        }

        /// <summary>
        /// Apply a function on every sliding window on the input sequence of records.
        /// </summary>
        /// <typeparam name="T1">The type of the input records</typeparam>
        /// <typeparam name="T2">The type of the output records</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="procFunc">The function to apply to every sliding window</param>
        /// <param name="windowSize">The size of the window</param>
        /// <returns>An IQueryable formed by the results for each sliding window</returns>
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
                var q = DryadLinqEnumerable.SlidingWindow(source, procFunc.Compile(), windowSize);
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

        /// <summary>
        /// Computes a user-defined function on each partition of the input. The function takes a 
        /// partition and its partition index as arguments.
        /// </summary>
        /// <typeparam name="T1">The type of the input records</typeparam>
        /// <typeparam name="T2">The type of the output records </typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="procFunc">The function to apply to each partition</param>
        /// <returns>An IQueryable formed by concatenating the results of applying the function
        /// to each partition</returns>
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

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Any``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>true iff the input sequence contains at least one element</returns>
        public static IQueryable<bool> AnyAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AnyAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Any``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition.</param>
        /// <returns>true iff the input sequence contains at least one element that satisfies 
        /// the predicate</returns>
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
                var q = DryadLinqEnumerable.AnyAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.All``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>true iff every element in the input sequence satisfies the predicate</returns>
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
                var q = DryadLinqEnumerable.AllAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Count``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int32"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>The number of elements in the input sequence</returns>
        public static IQueryable<int> CountAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.CountAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Count``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int32"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The number of elements in the input sequence satisfying the predicate</returns>
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
                var q = DryadLinqEnumerable.CountAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.LongCount``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int64"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>The number of elements in the input sequence</returns>
        public static IQueryable<long> LongCountAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.LongCountAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.LongCount``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int64"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The number of elements in the input sequence satisfying the predicate</returns>
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
                var q = DryadLinqEnumerable.LongCountAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Contains``1(System.Linq.IQueryable{``0},``0)"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="item">The value to locate in the sequence</param>
        /// <returns>true iff the source sequence contains an element of the specified value</returns>
        public static IQueryable<bool>
            ContainsAsQuery<TSource>(this IQueryable<TSource> source, TSource item)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.ContainsAsQuery(source, item).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source.Provider, q);
            }
            return source.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Constant(item, typeof(TSource)) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Contains``1(System.Linq.IQueryable{``0},``0,System.Collections.Generic.IEqualityComparer{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="item">The value to locate in the sequence</param>
        /// <param name="comparer">The equality comparer to use</param>
        /// <returns>true iff the source sequence contains an element of the specified value</returns>
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
                var q = DryadLinqEnumerable.ContainsAsQuery(source, item, comparer).AsQueryable();
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

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.SequenceEqual``1(System.Linq.IQueryable{``0},System.Collections.Generic.IEnumerable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source1">The first input sequence</param>
        /// <param name="source2">The second input sequence</param>
        /// <returns>true iff the two input sequences are equal</returns>
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
                var q = DryadLinqEnumerable.SequenceEqualAsQuery(source1, source2).AsQueryable();
                return new DryadLinqLocalQuery<bool>(source1.Provider, q);
            }
            return source1.Provider.CreateQuery<bool>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source1.Expression, GetSourceExpression(source2) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.SequenceEqual``1(System.Linq.IQueryable{``0},System.Collections.Generic.IEnumerable{``0},System.Collections.Generic.IEqualityComparer{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Boolean"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source1">The first input sequence</param>
        /// <param name="source2">The second input sequence</param>
        /// <param name="comparer">The equality comparer to use</param>
        /// <returns>true iff the two input sequences are equal</returns>
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
                var q = DryadLinqEnumerable.SequenceEqualAsQuery(source1, source2, comparer).AsQueryable();
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

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.First``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>The first element in the input sequence</returns>
        /// <exception cref="DryadLinqException">The input sequence is empty</exception>
        public static IQueryable<TSource> FirstAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.FirstAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.First``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The first element in the input sequence that satisfies the predicate</returns>
        /// <exception cref="DryadLinqException">No element in the input sequence satisfies the predicate</exception>
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
                var q = DryadLinqEnumerable.FirstAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Last``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>The last element in the input sequence</returns>
        public static IQueryable<TSource> LastAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.LastAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Last``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The last element in the input sequence that satisfies the predicate</returns>
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
                var q = DryadLinqEnumerable.LastAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Single``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <returns>The single element of the input sequence</returns>
        public static IQueryable<TSource> SingleAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SingleAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Single``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Boolean}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="predicate">A predicate to test each element for a condition</param>
        /// <returns>The single element of the input sequence that satisfies the predicate</returns>
        public static IQueryable<TSource>
            SingleAsQuery<TSource>(this IQueryable<TSource> source,
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
                var q = DryadLinqEnumerable.SingleAsQuery(source, predicate.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Min``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <returns>The minimum value in the input dataset</returns>
        public static IQueryable<TSource> MinAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.MinAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Min``2(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,``1}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TResult"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TResult">The type of the result value</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The minimum value in the transformed values</returns>
        public static IQueryable<TResult>
            MinAsQuery<TSource, TResult>(this IQueryable<TSource> source, 
                                         Expression<Func<TSource, TResult>> selector)
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
                var q = DryadLinqEnumerable.MinAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Max``1(System.Linq.IQueryable{``0})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <returns>The maximum value in the input dataset</returns>
        public static IQueryable<TSource> MaxAsQuery<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.MaxAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Max``2(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,``1}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TResult"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TResult">The type of the result value</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The maximum value in the transformed values</returns>
        public static IQueryable<TResult>
            MaxAsQuery<TSource, TResult>(this IQueryable<TSource> source,
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
                var q = DryadLinqEnumerable.MaxAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TResult>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Int32})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int32"/>&gt;
        /// containing a single element. Computes the sum of a set of Int32 values.
        /// </summary>
        /// <param name="source">A dataset of Int32 values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<int> SumAsQuery(this IQueryable<int> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Nullable{System.Int32}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Int32"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable Int32 values.
        /// </summary>
        /// <param name="source">A dataset of nullable Int32 values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<int?> SumAsQuery(this IQueryable<int?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<int?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Int64})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int64"/>&gt;
        /// containing a single element. Computes the sum of a set of Int64 values.
        /// </summary>
        /// <param name="source">A dataset of Int64 values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<long> SumAsQuery(this IQueryable<long> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Nullable{System.Int64}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Int64"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable Int64 values.
        /// </summary>
        /// <param name="source">A dataset of nullable Int64 values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<long?> SumAsQuery(this IQueryable<long?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<long?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Single})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Single"/>&gt;
        /// containing a single element. Computes the sum of a set of float values.
        /// </summary>
        /// <param name="source">A dataset of float values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<float> SumAsQuery(this IQueryable<float> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Nullable{System.Single}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Single"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable float values.
        /// </summary>
        /// <param name="source">A dataset of nullable float values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<float?> SumAsQuery(this IQueryable<float?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Double})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the sum of a set of double values.
        /// </summary>
        /// <param name="source">A dataset of double values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<double> SumAsQuery(this IQueryable<double> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Nullable{System.Double}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable double values.
        /// </summary>
        /// <param name="source">A dataset of nullable double values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<double?> SumAsQuery(this IQueryable<double?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Decimal})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Decimal"/>&gt;
        /// containing a single element. Computes the sum of a set of decimal values.
        /// </summary>
        /// <param name="source">A dataset of decimal values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<decimal> SumAsQuery(this IQueryable<decimal> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum(System.Linq.IQueryable{System.Nullable{System.Decimal}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Decimal"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable decimal values.
        /// </summary>
        /// <param name="source">A dataset of nullable decimal values to calculate the sum of</param>
        /// <returns>The sum of the values in the dataset</returns>
        public static IQueryable<decimal?> SumAsQuery(this IQueryable<decimal?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.SumAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Int32}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int32"/>&gt;
        /// containing a single element. Computes the sum of a set of Int32 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<int>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource, int>> selector)
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Int32}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Int32"/>&gt;&gt;
        /// containing a single element. Computes the sum of a set of nullable Int32 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<int?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<int?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Int64}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int64"/>&gt;
        /// containing a single element. Computes the sum of a set of Int64 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<long>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource, long>> selector)
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Int64}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Int64"/>&gt;
        /// containing a single element. Computes the sum of a set of nullable Int64 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<long?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<long?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Single}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Single"/>&gt;
        /// containing a single element. Computes the sum of a set of float values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<float>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Single}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Single"/>&gt;
        /// containing a single element. Computes the sum of a set of nullable float values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Double}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the sum of a set of double values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<double>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource, double>> selector)
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Double}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the sum of a set of nullable double values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Decimal}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Decimal"/>&gt;
        /// containing a single element. Computes the sum of a set of decimal values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<decimal>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource, decimal>> selector)
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Sum``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Decimal}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Decimal"/>&gt;
        /// containing a single element. Computes the sum of a set of nullable decimal values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The sum of the values after applying the transformation function</returns>
        public static IQueryable<decimal?>
            SumAsQuery<TSource>(this IQueryable<TSource> source,
                                Expression<Func<TSource, decimal?>> selector)
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
                var q = DryadLinqEnumerable.SumAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Int32})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of Int32 values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of Int32 values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double> AverageAsQuery(this IQueryable<int> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Nullable{System.Int32}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable Int32 values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of nullable Int32 values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double?> AverageAsQuery(this IQueryable<int?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Int64})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of Int64 values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of Int64 values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double> AverageAsQuery(this IQueryable<long> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Nullable{System.Int64}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable Int64 values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of nullable Int64 values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double?> AverageAsQuery(this IQueryable<long?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Single})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Single"/>&gt;
        /// containing a single element. Computes the average of a set of float values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of float values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<float> AverageAsQuery(this IQueryable<float> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()),
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Nullable{System.Single}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Single"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable float values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of nullable float values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<float?> AverageAsQuery(this IQueryable<float?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()),
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Double})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of double values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of double values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double> AverageAsQuery(this IQueryable<double> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Nullable{System.Double}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable double values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of nullable double values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<double?> AverageAsQuery(this IQueryable<double?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Decimal})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Decimal"/>&gt;
        /// containing a single element. Computes the average of a set of decimal values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of decimal values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<decimal> AverageAsQuery(this IQueryable<decimal> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average(System.Linq.IQueryable{System.Nullable{System.Decimal}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Decimal"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable decimal values in the input
        /// dataset.
        /// </summary>
        /// <param name="source">A set of nullable decimal values to calculate the average of</param>
        /// <returns>The average of the values in the input dataset</returns>
        public static IQueryable<decimal?> AverageAsQuery(this IQueryable<decimal?> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (IsLocalDebugSource(source))
            {
                var q = DryadLinqEnumerable.AverageAsQuery(source).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()), 
                    new Expression[] { source.Expression }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Int32}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of Int32 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
        public static IQueryable<double>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource, int>> selector)
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Int32}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable Int32 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Single}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Single"/>&gt;
        /// containing a single element. Computes the average of a set of float values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Single}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Single"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable float values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<float?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<float?>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Int64}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of Int64 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
        public static IQueryable<double>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource, long>> selector)
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Int64}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable Int64 values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>        
        public static IQueryable<double?>
            AverageAsQuery<TSource>(this IQueryable<TSource> source,
                                    Expression<Func<TSource, long?>> selector)
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Double}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Double"/>&gt;
        /// containing a single element. Computes the average of a set of double values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Double}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Double"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable double values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>        
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<double?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<double?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Decimal}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Decimal"/>&gt;
        /// containing a single element. Computes the average of a set of decimal values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Average``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,System.Nullable{System.Decimal}}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<see cref="T:System.Nullable"/>&lt;<see cref="T:System.Decimal"/>&gt;&gt;
        /// containing a single element. Computes the average of a set of nullable decimal values that
        /// is obtained by applying a function to each element of the input dataset.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="selector">A transformation function to apply to each element</param>
        /// <returns>The average of the values after applying the transformation function</returns>
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
                var q = DryadLinqEnumerable.AverageAsQuery(source, selector.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<decimal?>(source.Provider, q);
            }
            return source.Provider.CreateQuery<decimal?>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Applies an aggregator function over a sequence.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="seedFunc">The function that creates the initial accumulator value </param>
        /// <param name="func">An accumualator function to apply to each element</param>
        /// <returns>The final accumulator value</returns>
        public static TAccumulate
            Aggregate<TSource, TAccumulate>(this IQueryable<TSource> source,
                                            Expression<Func<TAccumulate>> seedFunc,
                                            Expression<Func<TAccumulate, TSource, TAccumulate>> func)
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
                return DryadLinqEnumerable.Aggregate(source, seedFunc.Compile(), func.Compile());
            }
            return source.Provider.Execute<TAccumulate>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TAccumulate)),
                    new Expression[] { source.Expression, Expression.Quote(seedFunc), Expression.Quote(func) }
                    ));
        }

        /// <summary>
        /// Applies an aggregator function over a sequence.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value</typeparam>
        /// <typeparam name="TResult">The type of final result</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="seedFunc">The function that creates the initial accumulator value </param>
        /// <param name="func">An accumualator function to apply to each element</param>
        /// <param name="selector">A function to transform the final accumulator value into the result value</param>
        /// <returns>The result of applying selector to the accumulator value</returns>
        public static TResult
            Aggregate<TSource, TAccumulate, TResult>(
                        this IQueryable<TSource> source,
                        Expression<Func<TAccumulate>> seedFunc,
                        Expression<Func<TAccumulate, TSource, TAccumulate>> func,
                        Expression<Func<TAccumulate, TResult>> selector)
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
                return DryadLinqEnumerable.Aggregate(source, seedFunc.Compile(), func.Compile(), selector.Compile());
            }
            return source.Provider.Execute<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TAccumulate), typeof(TResult)),
                    new Expression[] { source.Expression, Expression.Quote(seedFunc), Expression.Quote(func), Expression.Quote(selector) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Aggregate``1(System.Linq.IQueryable{``0},System.Linq.Expressions.Expression{System.Func{``0,``0,``0}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TSource"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="func">An accumualator function to apply to each element</param>
        /// <returns>The final accumulator value</returns>
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
                var q = DryadLinqEnumerable.AggregateAsQuery(source, func.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TSource>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)), 
                    new Expression[] { source.Expression, Expression.Quote(func) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Aggregate``2(System.Linq.IQueryable{``0},``1,System.Linq.Expressions.Expression{System.Func{``1,``0,``1}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TAccumulate"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="seed">The initial accumulator value</param>
        /// <param name="func">An accumualator function to apply to each element</param>
        /// <returns>The final accumulator value</returns>
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
                var q = DryadLinqEnumerable.AggregateAsQuery(source, seed, func.Compile()).AsQueryable();
                return new DryadLinqLocalQuery<TAccumulate>(source.Provider, q);
            }
            return source.Provider.CreateQuery<TAccumulate>(
                Expression.Call(
                    null, 
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TAccumulate)), 
                    new Expression[] { source.Expression, Expression.Constant(seed), Expression.Quote(func) }
                    ));
        }

        /// <summary>
        /// Same as <see cref="M:System.Linq.Queryable.Aggregate``3(System.Linq.IQueryable{``0},``1,System.Linq.Expressions.Expression{System.Func{``1,``0,``1}},System.Linq.Expressions.Expression{System.Func{``1,``2}})"/>, but returns an <see cref="T:System.Linq.IQueryable"/>&lt;<typeparamref name="TResult"/>&gt;
        /// containing a single element.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value</typeparam>
        /// <typeparam name="TResult">The type of the final result</typeparam>
        /// <param name="source">The input sequence</param>
        /// <param name="seed">The initial accumulator value</param>
        /// <param name="func">An accumualator function to apply to each element</param>
        /// <param name="selector">A function to transform the final accumulator value into the result value</param>
        /// <returns>The result of applying selector to the accumulator value</returns>
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
                var q = DryadLinqEnumerable.AggregateAsQuery(source, seed, func.Compile(), selector.Compile()).AsQueryable();
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
        /// <param name="source">The input dataset</param>
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

        /// <summary>
        /// Instructs DryadLINQ to assume that the dataset is hash partitioned.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An equality comparer to compute the hash code of a key</param>
        /// <returns>The same dataset as input</returns>
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
        /// Instructs DryadLINQ to assume that the dataset is range partitioned.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="isDescending">true to assume the partition keys are ordered descendingly</param>
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

        /// <summary>
        /// Instructs DryadLINQ to assume that the dataset is range partitioned.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="isDescending">true to assume that the partition keys are descending</param>
        /// <returns>The same dataset as input</returns>
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

        /// <summary>
        /// Instructs DryadLINQ to assume that the dataset is range partitioned by a specified list of keys.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="rangeSeparators">A list of partition keys</param>
        /// <returns>The same dataset as input</returns>
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

        /// <summary>
        /// Instructs DryadLINQ to assume that the dataset is range partitioned by a specified list of keys.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="rangeSeparators">A list of partition keys</param>
        /// <returns>The same dataset as input</returns>
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
        /// Instructs DryadLINQ to assume that each partition of the dataset is ordered. A dataset
        /// is ordered if it is range partitioned and each partition of it is ordered on the same
        /// key. 
        /// </summary>
        /// <typeparam name="TSource">The type of the recrods of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="isDescending">true to assume the order is descending</param>
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

        /// <summary>
        /// Instructs DryadLINQ to assume that each partition of the dataset is ordered. A dataset
        /// is ordered if it is range partitioned and each partition of it is ordered on the same
        /// key. 
        /// </summary>
        /// <typeparam name="TSource">The type of the recrods of the dataset</typeparam>
        /// <typeparam name="TKey">The type of the key on which partition is based</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="comparer">An IComparer on TKey to compare keys</param>
        /// <param name="isDescending">true to assume the order is descending</param>
        /// <returns>The same dataset as input</returns>
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

        /// <summary>
        /// Forks a specified input dataset into two datasets. A specified user-defined function is
        /// applied to each partition of the input dataset to produce a sequence of ForkTuples.
        /// </summary>
        /// <typeparam name="T">The type of the elements of source</typeparam>
        /// <typeparam name="R1">The element type of the first output dataset</typeparam>
        /// <typeparam name="R2">The element type of the second output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="mapper">The function to apply to each partition of the input dataset</param>
        /// <returns>An IMultiQueryable exposing two output datasets</returns>
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
                IMultiEnumerable<R1, R2> enumerables = DryadLinqEnumerable.Fork(source, mapper.Compile());
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

        /// <summary>
        /// Forks a specified input dataset into three datasets. A specified user-defined function is
        /// applied to each partition of the input dataset to produce a sequence of ForkTuples.
        /// </summary>
        /// <typeparam name="T">The type of the elements of source</typeparam>
        /// <typeparam name="R1">The element type of the first output dataset</typeparam>
        /// <typeparam name="R2">The element type of the second output dataset</typeparam>
        /// <typeparam name="R3">The element type of the third output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="mapper">The function to apply to each partition of the input dataset</param>
        /// <returns>An IMultiQueryable exposing three output datasets</returns>
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
                IMultiEnumerable<R1, R2, R3> enumerables = DryadLinqEnumerable.Fork(source, mapper.Compile());
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
        /// Compute two output datasets from one input dataset.  A specified user-defined function is
        /// applied to each input element to produce zero or one element for each output dataset.
        /// </summary>
        /// <typeparam name="T">The type of records of input dataset</typeparam>
        /// <typeparam name="R1">The type of records of first output dataset</typeparam>
        /// <typeparam name="R2">The type of records of second output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="mapper">The function applied to each record of the input</param>
        /// <returns>An IMultiQueryable for the two output datasets</returns>
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
                IMultiEnumerable<R1, R2> enumerables = DryadLinqEnumerable.Fork(source, mapper.Compile());
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

        /// <summary>
        /// Forks one input dataset into three output datasets.  A specified user-defined function is
        /// applied to each input element to produce zero or one element for each output dataset.
        /// </summary>
        /// <typeparam name="T">The type of records of input dataset</typeparam>
        /// <typeparam name="R1">The type of records of the first output dataset</typeparam>
        /// <typeparam name="R2">The type of records of the second output dataset</typeparam>
        /// <typeparam name="R3">The type of records of the third output dataset</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="mapper">The function applied to each record of the input</param>
        /// <returns>An IMultiQueryable for the three output datasets</returns>
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
                IMultiEnumerable<R1, R2, R3> enumerables = DryadLinqEnumerable.Fork(source, mapper.Compile());
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
        /// Divides the input dataset into a collection of datasets based on the keys of the records.
        /// The method produces one output dataset for each key in the specified key array. Input 
        /// records that don't match any of the keys are dropped.
        /// </summary>
        /// <typeparam name="TSource">The type of records of input dataset</typeparam>
        /// <typeparam name="TKey">The type of the keys of the input records</typeparam>
        /// <param name="source">The input dataset</param>
        /// <param name="keySelector">The function to extract the key from a record</param>
        /// <param name="keys">A list of the partition keys</param>
        /// <returns>An IKeyedMultiQueryable for the output datasets.</returns>
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
                IMultiEnumerable<TSource> enumerables = DryadLinqEnumerable.Fork(source, keySelector.Compile(), keys);
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
        /// Specifies a stream URI to be populated with the result of a specified DryadLINQ query. 
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="streamName">A stream name</param>
        /// <param name="deleteIfExists">If this flag is true, delete the output stream 
        /// if it already exisit before execution</param>
        /// <returns>A query representing the output data.</returns>
        public static IQueryable<TSource>
            ToStore<TSource>(this IQueryable<TSource> source, string streamName, bool deleteIfExists = false)
        {  
            return ToStore(source, new Uri(streamName), deleteIfExists);
        }

        /// <summary>
        /// Specifies a stream URI to be populated with the result of a specified DryadLINQ query.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <param name="streamName">The stream name to store the result</param>
        /// <param name="deleteIfExists">If this flag is true, delete the output stream 
        /// if it already exisit before execution</param>
        /// <returns>A query representing the output data.</returns>
        public static IQueryable<TSource>
            ToStore<TSource>(this IQueryable<TSource> source, Uri streamName, bool deleteIfExists = false)
        {
            DryadLinqContext context = DryadLinqContext.GetContext(source.Provider);
            DataProvider dataProvider = DataProvider.GetDataProvider(streamName.Scheme);
            streamName = dataProvider.RewriteUri<TSource>(context, streamName, FileAccess.Write);
            dataProvider.CheckExistence(context, streamName, deleteIfExists);
            return ToStoreInternal(source, streamName, false);
        }

        internal static IQueryable<TSource>
            ToStoreInternal<TSource>(this IQueryable<TSource> source, Uri streamName, bool isTemp)
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
                throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, 0), "source");
            }

            // Strip out ToStore if source.Expression has one.
            Expression expr = source.Expression;
            MethodCallExpression mcExpr = expr as MethodCallExpression;
            if (mcExpr != null && mcExpr.Method.Name == ReflectedNames.DLQ_ToStore)
            {
                expr = mcExpr.Arguments[0];
            }

            DryadLinqContext context = DryadLinqContext.GetContext(source.Provider);
            IQueryable<TSource> result = source.Provider.CreateQuery<TSource>(
                                            Expression.Call(
                                                null,
                                                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                                                new Expression[] { expr,
                                                                   Expression.Constant(streamName, typeof(Uri)),
                                                                   Expression.Constant(isTemp, typeof(bool)) }));
            ((DryadLinqQuery)source).BackingData = (DryadLinqQuery)result;
            return result;
        }

        /// <summary>
        /// Submits a specified query for asynchronous execution.
        /// </summary>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <returns>Information about the execution job.</returns>
        public static DryadLinqJobInfo Submit<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            // Extract the context.
            if (!(source.Provider is DryadLinqProviderBase))
            {
                throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, 0), "source");
            }
            DryadLinqContext context = DryadLinqContext.GetContext(source.Provider);

            if (IsLocalDebugSource(source))
            {
                try
                {
                    // If ToStore is present, extract out the target, otherwise make an anonymous target.
                    // then ingress the data directly to the store.
                    MethodCallExpression mcExpr1 = source.Expression as MethodCallExpression;
                    if (mcExpr1 != null && mcExpr1.Method.Name == ReflectedNames.DLQ_ToStore)
                    {
                        // visited by LocalDebug: q2.ToStore(...).Submit(...)
                        Uri dataSetUri = (Uri)((ConstantExpression)mcExpr1.Arguments[1]).Value;
                        CompressionScheme compressionScheme = context.OutputDataCompressionScheme;
                        DryadLinqMetaData metadata
                            = new DryadLinqMetaData(context, typeof(TSource), dataSetUri, compressionScheme);
                        DataProvider.StoreData(context, source, dataSetUri, metadata, compressionScheme);
                    }
                    else
                    {
                        // visited by LocalDebug: q-nonToStore.Submit();
                        Uri dataSetUri = context.MakeTemporaryStreamUri();
                        CompressionScheme compressionScheme = context.IntermediateDataCompressionScheme;
                        DryadLinqMetaData metadata
                            = new DryadLinqMetaData(context, typeof(TSource), dataSetUri, compressionScheme);
                        DataProvider.StoreData(context, source, dataSetUri, metadata, compressionScheme, true);
                    }

                    return new DryadLinqJobInfo(DryadLinqJobInfo.JOBID_LOCALDEBUG, null, null);
                }
                catch (Exception e)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CreatingDscDataFromLocalDebugFailed,
                                                 String.Format(SR.CreatingDscDataFromLocalDebugFailed), e);
                }
            }

            // Now we are not LocalDebug mode:
            DryadLinqQuery sourceQuery = source as DryadLinqQuery;
            if (sourceQuery == null)
            {
                throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, 0), "source");
            }

            // Handle repeat submissions.
            if (sourceQuery.IsPlainData)
            {
                string jobId = DryadLinqJobInfo.JOBID_NOJOB;
                if (sourceQuery.QueryExecutor != null)
                {
                    jobId = sourceQuery.QueryExecutor.GetJobId();
                }
                return new DryadLinqJobInfo(jobId, context.HeadNode, sourceQuery.QueryExecutor);
            }
            else if (sourceQuery.IsDataBacked)
            {
                // This query has already been submitted.
                DryadLinqQuery backingData = sourceQuery.BackingData;
                while (backingData.IsDataBacked)
                {
                    backingData = backingData.BackingData;
                }
                string jobId = backingData.QueryExecutor.GetJobId();
                return new DryadLinqJobInfo(jobId, context.HeadNode, backingData.QueryExecutor);
            }
            else
            {
                // Sanity check that we have a DryadLinqQuery
                MethodCallExpression mcExpr = source.Expression as MethodCallExpression;
                if (mcExpr == null)
                {
                    throw new ArgumentException(String.Format(SR.AtLeastOneOperatorRequired, 0), "source");
                }

                if (!mcExpr.Method.IsStatic || !TypeSystem.IsQueryOperatorCall(mcExpr))
                {
                    throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, 0), "source");
                }

                bool isTemp = false;
                if (mcExpr.Method.Name != ReflectedNames.DLQ_ToStore)
                {
                    // Support for non-ToStoreQuery.Submit()
                    isTemp = true;
                    Uri tableUri = context.MakeTemporaryStreamUri();
                    mcExpr = Expression.Call(
                                    null,
                                    typeof(DryadLinqQueryable).GetMethod(ReflectedNames.DLQ_ToStore,
                                                                         BindingFlags.Static | BindingFlags.NonPublic)
                                                              .MakeGenericMethod(typeof(TSource)),
                                    new Expression[] { source.Expression,
                                                       Expression.Constant(tableUri, typeof(Uri)),
                                                       Expression.Constant(true, typeof(bool)) });
                }

                // Execute the queries
                DryadLinqQueryGen dryadGen 
                    = new DryadLinqQueryGen(context,
                                            ((DryadLinqQuery)source).GetVertexCodeGen(),
                                            new Expression[] { mcExpr });
                DryadLinqQuery[] tables = dryadGen.Execute();

                tables[0].IsTemp = isTemp;
                ((DryadLinqQuery)source).BackingData = tables[0];

                string jobId = tables[0].QueryExecutor.GetJobId();
                return new DryadLinqJobInfo(jobId, context.HeadNode, tables[0].QueryExecutor);
            }
        }
        
        /// <summary>
        /// Submits a specified query and then waits for the job to complete
        /// </summary>
        /// <exception cref="DryadLinqException">If the job completes in error or is cancelled.</exception>
        /// <exception cref="DryadLinqException">If repeated errors occur while polling for status.</exception>
        /// <typeparam name="TSource">The type of the records of the table</typeparam>
        /// <param name="source">The data source</param>
        /// <returns>Information about the execution job.</returns>
        public static DryadLinqJobInfo SubmitAndWait<TSource>(this IQueryable<TSource> source)
        {
            DryadLinqJobInfo info = source.Submit();
            info.Wait();
            return info;
        }

        /// <summary>
        /// Submits a list of DryadLINQ queries for asynchronous execution.
        /// </summary>
        /// <param name="sources">Queries to execute.</param>
        /// <returns>Job information for tracking the execution.</returns>
        public static DryadLinqJobInfo Submit(params IQueryable[] sources)
        {
            if (sources == null)
            {
                throw new ArgumentNullException("sources");
            }
            if (sources.Length == 0)
            {
                throw new ArgumentException("sources is empty", "sources");
            }

            DryadLinqContext context = CheckSourcesAndGetCommonContext(sources);
            if (IsLocalDebugSource(sources[0]))
            {
                foreach (var source in sources)
                {
                    var method = typeof(DataProvider).GetMethod(ReflectedNames.DataProvider_Ingress,
                                                                BindingFlags.NonPublic | BindingFlags.Static)
                                                     .MakeGenericMethod(source.ElementType);
                    MethodCallExpression mcExpr1 = source.Expression as MethodCallExpression;
                    Uri dataSetUri;
                    CompressionScheme compressionScheme;
                    bool isTemp;
                    if (mcExpr1 != null && mcExpr1.Method.Name == ReflectedNames.DLQ_ToStore)
                    {
                        dataSetUri = (Uri)((ConstantExpression)mcExpr1.Arguments[1]).Value;
                        compressionScheme = context.OutputDataCompressionScheme;
                        isTemp = false;
                    }
                    else
                    {
                        dataSetUri = context.MakeTemporaryStreamUri();
                        compressionScheme = context.IntermediateDataCompressionScheme;
                        isTemp = true;
                    }
                    DryadLinqMetaData metadata
                        = new DryadLinqMetaData(context, source.ElementType, dataSetUri, compressionScheme);
                    try
                    {
                        method.Invoke(
                             null, new object[] { context, source, dataSetUri, metadata, compressionScheme, isTemp });
                    }
                    catch (TargetInvocationException tie)
                    {
                        if (tie.InnerException != null)
                        {
                            throw tie.InnerException; // unwrap and rethrow original exception
                        }
                        else
                        {
                            throw; // this shouldn't occur.. but just in case.
                        }
                    }

                    return new DryadLinqJobInfo(DryadLinqJobInfo.JOBID_LOCALDEBUG, null, null);
                }
            }

            // Not LocalDebug mode:
            List<Expression> qList = new List<Expression>();
            List<bool> isTempList = new List<bool>();
            string[] jobIds = new string[sources.Length];
            string[] headNodes = new string[sources.Length];
            DryadLinqJobExecutor[] jobExecutors = new DryadLinqJobExecutor[sources.Length];

            for (int i = 0; i < sources.Length; i++)
            {
                DryadLinqQuery sourceQuery = sources[i] as DryadLinqQuery;
                if (sourceQuery == null)
                {
                    throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, i), string.Format("sources[{0}]", i));
                }

                if (sourceQuery.IsPlainData)
                {
                    jobIds[i] = DryadLinqJobInfo.JOBID_NOJOB;
                    if (sourceQuery.QueryExecutor != null)
                    {
                        jobIds[i] = sourceQuery.QueryExecutor.GetJobId();
                    }
                    headNodes[i] = context.HeadNode;
                    jobExecutors[i] = sourceQuery.QueryExecutor;
                }
                else if (sourceQuery.IsDataBacked)
                {
                    // This query has already been submitted.
                    sourceQuery = sourceQuery.BackingData;
                    while (sourceQuery.IsDataBacked)
                    {
                        sourceQuery = sourceQuery.BackingData;
                    }
                    jobIds[i] = sourceQuery.QueryExecutor.GetJobId();
                    headNodes[i] = context.HeadNode;
                    jobExecutors[i] = sourceQuery.QueryExecutor;
                }
                else
                {
                    // Sanity check that we have a normal DryadLinqQuery
                    MethodCallExpression mcExpr = sources[i].Expression as MethodCallExpression;
                    if (mcExpr == null)
                    {
                        throw new ArgumentException(String.Format(SR.AtLeastOneOperatorRequired, 0), "source");
                    }

                    if (!mcExpr.Method.IsStatic || !TypeSystem.IsQueryOperatorCall(mcExpr))
                    {
                        throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, i), string.Format("sources[{0}]", i));
                    }

                    bool isTemp = false; ;
                    if (mcExpr.Method.Name != ReflectedNames.DLQ_ToStore)
                    {
                        isTemp = true;
                        Uri tableUri = context.MakeTemporaryStreamUri();
                        MethodInfo minfo = typeof(DryadLinqQueryable)
                                                   .GetMethod(ReflectedNames.DLQ_ToStore,
                                                              BindingFlags.Static | BindingFlags.NonPublic);
                        Type elemType = mcExpr.Type.GetGenericArguments()[0];
                        minfo = minfo.MakeGenericMethod(elemType);
                        mcExpr = Expression.Call(minfo,
                                                 mcExpr,
                                                 Expression.Constant(tableUri, typeof(Uri)),
                                                 Expression.Constant(isTemp, typeof(bool)));
                    }
                    qList.Add(mcExpr);
                    isTempList.Add(isTemp);
                }
            }

            // Execute the queries on the cluster:
            VertexCodeGen vertexCodeGen = ((DryadLinqQuery)sources[0]).GetVertexCodeGen();
            DryadLinqQueryGen queryGen = new DryadLinqQueryGen(context, vertexCodeGen, qList.ToArray());
            DryadLinqQuery[] tables = queryGen.Execute();

            // Store the results in the queries
            int idx = 0;
            for (int i = 0; i < tables.Length; i++)
            {
                tables[i].IsTemp = isTempList[i];
                while (headNodes[idx] != null) idx++;
                ((DryadLinqQuery)sources[idx]).BackingData = tables[i];
                headNodes[idx] = context.HeadNode;
                jobExecutors[idx] = tables[0].QueryExecutor;
            }
            return new DryadLinqJobInfo(jobIds, headNodes, jobExecutors);
        }

        /// <summary>
        /// Submits a list of DryadLinq queries for execution and waits for the job to complete
        /// </summary>
        /// <exception cref="DryadLinqException">If the job completes in error or is cancelled.</exception>
        /// <exception cref="DryadLinqException">If repeated errors occur while polling for status.</exception>
        /// <param name="sources">A set of DryadLINQ queries to execute</param>
        /// <returns>Information about the job being submitted for execution.</returns>
        /// <remarks>
        /// Every item in sources must be an DryadLinq IQueryable object that terminates with ToStore()
        /// Only one job will be executed, but the job will produce the output associated with each item in sources.
        /// </remarks>
        public static DryadLinqJobInfo SubmitAndWait(params IQueryable[] sources)
        {
            DryadLinqJobInfo info = DryadLinqQueryable.Submit(sources);
            info.Wait();
            return info;
        }

        private static DryadLinqContext CheckSourcesAndGetCommonContext(IQueryable[] sources)
        {
            if (sources == null)
            {
                throw new ArgumentNullException("sources");
            }
            if (sources.Length == 0)
            {
                throw new ArgumentException("sources is empty", "sources");
            }
            
            for (int i = 0; i < sources.Length; i++)
            {
                if (sources[i] == null)
                {
                    throw new ArgumentException(string.Format("sources[{0}] was null.", i), "sources"); 
                }
                
                // Sanity check that we have normal DryadLinqQuery objects
                if (!(sources[i].Provider is DryadLinqProviderBase))
                {
                    throw new ArgumentException(String.Format(SR.NotADryadLinqQuery, i), "sources");
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

            // Check the queries all use the same context
            DryadLinqContext commonContext = DryadLinqContext.GetContext(sources[0].Provider);
            for (int i = 1; i < sources.Length; i++)
            {
                if (commonContext != DryadLinqContext.GetContext(sources[i].Provider))
                {
                    throw new DryadLinqException("Each query must be created from the same DryadLinqContext object");
                }
            }
            return commonContext;
        }
    }
}
