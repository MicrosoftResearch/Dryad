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
    public interface IDecomposable<TSource, TAccumulate, TResult>
    {
        void Initialize(object state);
        TAccumulate Seed(TSource val);
        TAccumulate Accumulate(TAccumulate acc, TSource val);
        TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val);
        TResult FinalReduce(TAccumulate val);
    }

    public static class GenericDecomposable<TDecomposable, TSource, TAccumulate, TResult>
        where TDecomposable : IDecomposable<TSource, TAccumulate, TResult>, new()
    {
        private static TDecomposable d = new TDecomposable();

        public static void Initialize(object state)
        {
            d.Initialize(state);
        }

        public static TAccumulate Seed(TSource val)
        {
            return d.Seed(val);
        }

        public static TAccumulate Accumulate(TAccumulate acc, TSource val)
        {
            return d.Accumulate(acc, val);
        }

        public static TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val)
        {
            return d.RecursiveAccumulate(acc, val);
        }

        public static TResult FinalReduce(TAccumulate val)
        {
            return d.FinalReduce(val);
        }
    }
}
