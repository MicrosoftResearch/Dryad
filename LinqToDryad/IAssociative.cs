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
    public interface IAssociative<TAccumulate>
    {
        TAccumulate Seed();
        TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val);
    }

    public static class GenericAssociative<TAssoc, TAccumulate>
        where TAssoc : IAssociative<TAccumulate>, new()
    {
        private static TAssoc a = new TAssoc();

        public static TAccumulate Seed()
        {
            return a.Seed();
        }

        public static TAccumulate RecursiveAccumulate(TAccumulate acc, TAccumulate val)
        {
            return a.RecursiveAccumulate(acc, val);
        }
    }
}
