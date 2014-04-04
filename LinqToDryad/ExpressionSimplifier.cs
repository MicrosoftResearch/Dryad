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
using System.Text;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    // This class implements a simple evaluator for expression. It may
    // evolve into a better and more useful thing: a simplifier that
    // goes through an expression to remove all dependency to the
    // current local execution context. This is necessary to ensure
    // that expressions can be remotely executed. Even better, this
    // simplifier could detect potential "impure" expressions. Again,
    // it would be nice if this is a functionality provided by C# and
    // LINQ. But unfortunately it is not.
    internal abstract class ExpressionSimplifier
    {
        internal abstract object EvalBoxed(Expression expr);
        
        // Evaluate the expression in the current local execution context.
        internal static object Evaluate(Expression expr)
        {
            Type qType = typeof(ExpressionSimplifier<>).MakeGenericType(expr.Type);
            ExpressionSimplifier evaluator = (ExpressionSimplifier)Activator.CreateInstance(qType);
            return evaluator.EvalBoxed(expr);
        }
    }
    
    internal class ExpressionSimplifier<T> : ExpressionSimplifier
    {
        internal override object EvalBoxed(Expression expr)
        {
            return this.Eval(expr);
        }

        internal T Eval(Expression expr)
        {
            Expression<Func<T>> lambda = Expression.Lambda<Func<T>>(expr);
            Func<T> func = lambda.Compile();
            return func();
        }
    }
}
