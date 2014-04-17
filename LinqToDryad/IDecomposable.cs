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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Defines the DryadLINQ interface for decomposable functions. It allows a function to be 
    /// decomposed into the composition of several functions that can be executed more efficiently.
    /// </summary>
    /// <typeparam name="TSource">The record type of the original input.</typeparam>
    /// <typeparam name="TAccumulate">The record type of an intermediate result.</typeparam>
    /// <typeparam name="TResult">The record type of the final result.</typeparam>
    public interface IDecomposable<TSource, TAccumulate, TResult>
    {
        /// <summary>
        /// Initializes the state of this IDecomposable object.
        /// </summary>
        /// <param name="state">The state.</param>
        void Initialize(object state);

        /// <summary>
        /// Converts an input record to an intermediate value.
        /// </summary>
        /// <param name="val">An input record.</param>
        /// <returns>An intermediate result.</returns>
        TAccumulate Seed(TSource val);

        /// <summary>
        /// Adds a new input record into the intermediate value.
        /// </summary>
        /// <param name="acc">The current intermediate value.</param>
        /// <param name="val">A new input record.</param>
        /// <returns>The new intermediate value. </returns>
        TAccumulate Accumulate(TAccumulate acc, TSource val);

        /// <summary>
        /// Combines two intermediate values into a new intermediate value.
        /// </summary>
        /// <param name="acc">The first intermediate value.</param>
        /// <param name="val">The second intermediate value.</param>
        /// <returns>The new intermediate value.</returns>
        TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val);

        /// <summary>
        /// Computes the final result from the current intermediate value.
        /// </summary>
        /// <param name="val">An intermediate value.</param>
        /// <returns>The final result.</returns>
        TResult FinalReduce(TAccumulate val);
    }

    /// <summary>
    /// A helper class for calling IDecomposable methods more efficiently. It is used in 
    /// auto-generated vertex code.  A DryadLINQ user should not need to use this class directly.
    /// </summary>
    /// <typeparam name="TDecomposable">The type that implements the IDecomposable interface</typeparam>
    /// <typeparam name="TSource">The element type of the input sequence</typeparam>
    /// <typeparam name="TAccumulate">The element type of an intermediate result</typeparam>
    /// <typeparam name="TResult">The element type of the final result</typeparam>
    public static class GenericDecomposable<TDecomposable, TSource, TAccumulate, TResult>
        where TDecomposable : IDecomposable<TSource, TAccumulate, TResult>, new()
    {
        private static TDecomposable d = new TDecomposable();

        /// <summary>
        /// Initializes the initial state of the IDecomposable object.
        /// </summary>
        /// <param name="state">The initial state of this IDecomposable object</param>
        public static void Initialize(object state)
        {
            d.Initialize(state);
        }

        /// <summary>
        /// Converts an input element to an intermediate accumulator value.
        /// </summary>
        /// <param name="val">An input element</param>
        /// <returns>An accumulator value</returns>
        public static TAccumulate Seed(TSource val)
        {
            return d.Seed(val);
        }

        /// <summary>
        /// Accumulates an input element into the accumulator value.
        /// </summary>
        /// <param name="acc">An accumulator value</param>
        /// <param name="val">An input element</param>
        /// <returns>An accumulator value resulting from applying this Accumulate method
        /// on the two arguments</returns>
        public static TAccumulate Accumulate(TAccumulate acc, TSource val)
        {
            return d.Accumulate(acc, val);
        }

        /// <summary>
        /// Combines two accumulator values into one.
        /// </summary>
        /// <param name="acc">The first accumulator value</param>
        /// <param name="val">The second accumulator value</param>
        /// <returns>An accumulator value resulting from combining two accumulator values</returns>
        public static TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val)
        {
            return d.RecursiveAccumulate(acc, val);
        }

        /// <summary>
        /// Produces the final value from an accumulator value.
        /// </summary>
        /// <param name="val">An accumulator value</param>
        /// <returns>The value of the final result</returns>
        public static TResult FinalReduce(TAccumulate val)
        {
            return d.FinalReduce(val);
        }
    }
}
