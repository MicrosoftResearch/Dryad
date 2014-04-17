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
    /// Defines the DryadLINQ interface for associative accumulator. 
    /// </summary>
    /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
    public interface IAssociative<TAccumulate>
    {
        /// <summary>
        /// Provides the initial value for the accumulator.
        /// </summary>
        /// <returns>The initial value of the accumulator</returns>
        TAccumulate Seed();

        /// <summary>
        /// Combines two accumulator values into one.
        /// </summary>
        /// <param name="acc">The value of the accumulator</param>
        /// <param name="val">A value to be accumulated</param>
        /// <returns>The result of combining two accumulator values into one</returns>
        TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val);
    }

    /// <summary>
    /// A helper class for calling IAssociative methods more efficiently. It is used in 
    /// auto-generated vertex code.  A DryadLINQ user should not need to use this class directly.
    /// </summary>
    /// <typeparam name="TAssoc">The type that implements the IAssociative{T} interface</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulator value.</typeparam>
    public static class GenericAssociative<TAssoc, TAccumulate>
        where TAssoc : IAssociative<TAccumulate>, new()
    {
        private static TAssoc a = new TAssoc();

        /// <summary>
        /// Provides the initial value for the accumulator.
        /// </summary>
        /// <returns>The initial value of the accumulator</returns>
        public static TAccumulate Seed()
        {
            return a.Seed();
        }

        /// <summary>
        /// Combines two accumulator values into one.
        /// </summary>
        /// <param name="acc">The value of the accumulator</param>
        /// <param name="val">A value to be accumulated</param>
        /// <returns>The result of combining two accumulator values into one</returns>
        public static TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val)
        {
            return a.RecursiveAccumulate(acc, val);
        }
    }
}
