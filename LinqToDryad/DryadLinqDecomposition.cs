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
using System.Collections.ObjectModel;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    internal class DecompositionInfo
    {
        private Expression m_func;                       // The original function call
        private LambdaExpression m_seed;                 // (TSource) => TAccumulate
        private LambdaExpression m_accumulator;          // (TAccumulate, TSource) => TAccumulate
        private LambdaExpression m_recursiveAccumulator; // (TAccumulate, TAccumulate) => TAccumulate
        private LambdaExpression m_finalReducer;         // (TAccumulate) => TResult

        public DecompositionInfo(Expression func,
                                 LambdaExpression seed,
                                 LambdaExpression accumulator,
                                 LambdaExpression recursiveAccumulator,
                                 LambdaExpression finalReducer)
        {
            this.m_func = func;
            this.m_seed = seed;
            this.m_accumulator = accumulator;
            this.m_recursiveAccumulator = recursiveAccumulator;
            this.m_finalReducer = finalReducer;
        }

        public Expression Func
        {
            get { return this.m_func; }
        }

        public LambdaExpression Seed
        {
            get { return this.m_seed; }
        }
        
        public LambdaExpression Accumulator
        {
            get { return this.m_accumulator; }
        }

        public LambdaExpression RecursiveAccumulator
        {
            get { return this.m_recursiveAccumulator; }
        }

        public LambdaExpression FinalReducer
        {
            get { return this.m_finalReducer; }
        }
    }

    internal class Decomposition
    {
        internal static List<DecompositionInfo>
            GetDecompositionInfoList(LambdaExpression resultSelectExpr, HpcLinqCodeGen codeGen)
        {
            ParameterExpression keyParam;
            ParameterExpression groupParam;
            if (resultSelectExpr.Parameters.Count == 1)
            {
                keyParam = null;
                groupParam = resultSelectExpr.Parameters[0];
            }
            else
            {
                Debug.Assert(resultSelectExpr.Parameters.Count == 2);
                keyParam = resultSelectExpr.Parameters[0];
                groupParam = resultSelectExpr.Parameters[1];
            }

            List<DecompositionInfo> infoList = new List<DecompositionInfo>(1);
            bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam,
                                                         resultSelectExpr.Body,
                                                         infoList, codeGen);
            if (isDecomposed)
            {
                return infoList;
            }
            return null;
        }

        private static bool GetDecompositionInfoList(ParameterExpression keyParam,
                                                     ParameterExpression groupParam,
                                                     MemberBinding mbinding,
                                                     List<DecompositionInfo> infoList,
                                                     HpcLinqCodeGen codeGen)
        {
            if (mbinding is MemberAssignment)
            {
                Expression expr = ((MemberAssignment)mbinding).Expression;
                return GetDecompositionInfoList(keyParam, groupParam, expr, infoList, codeGen);
            }
            else if (mbinding is MemberMemberBinding)
            {
                foreach (MemberBinding mb in ((MemberMemberBinding)mbinding).Bindings)
                {
                    bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam, mb, infoList, codeGen);
                    if (!isDecomposed) return false;
                }
            }
            else if (mbinding is MemberListBinding)
            {
                foreach (ElementInit ei in ((MemberListBinding)mbinding).Initializers)
                {
                    foreach (Expression arg in ei.Arguments)
                    {
                        bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam, arg, infoList, codeGen);
                        if (!isDecomposed) return false;
                    }
                }
            }
            return true;
        }

        private static bool GetDecompositionInfoList(ParameterExpression keyParam,
                                                     ParameterExpression groupParam,
                                                     Expression expr,
                                                     List<DecompositionInfo> infoList,
                                                     HpcLinqCodeGen codeGen)
        {
            IEnumerable<Expression> argList = null;
            if (HpcLinqExpression.IsConstant(expr))
            {
                return true;
            }
            else if (expr is BinaryExpression)
            {
                BinaryExpression be = (BinaryExpression)expr;
                argList = new Expression[] { be.Left, be.Right };
            }
            else if (expr is UnaryExpression)
            {
                UnaryExpression ue = (UnaryExpression)expr;
                return GetDecompositionInfoList(keyParam, groupParam, ue.Operand, infoList, codeGen);
            }
            else if (expr is ConditionalExpression)
            {
                ConditionalExpression ce = (ConditionalExpression)expr;
                argList = new Expression[] { ce.Test, ce.IfTrue, ce.IfFalse };
            }
            else if (expr is MethodCallExpression)
            {
                MethodCallExpression mcExpr = (MethodCallExpression)expr;
                DecompositionInfo dinfo = GetDecompositionInfo(groupParam, mcExpr, codeGen);
                if (dinfo != null)
                {
                    infoList.Add(dinfo);
                    return true;
                }
                if (mcExpr.Object != null)
                {
                    bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam,
                                                                 mcExpr.Object,
                                                                 infoList, codeGen);
                    if (!isDecomposed) return false;
                }
                argList = mcExpr.Arguments;
            }
            else if (expr is NewExpression)
            {
                argList = ((NewExpression)expr).Arguments;
            }
            else if (expr is NewArrayExpression)
            {
                argList = ((NewArrayExpression)expr).Expressions;
            }
            else if (expr is ListInitExpression)
            {
                ListInitExpression li = (ListInitExpression)expr;
                bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam,
                                                             li.NewExpression,
                                                             infoList, codeGen);
                for (int i = 0, n = li.Initializers.Count; i < n; i++)
                {
                    ElementInit ei = li.Initializers[i];
                    foreach (Expression arg in ei.Arguments)
                    {
                        isDecomposed = GetDecompositionInfoList(keyParam, groupParam, arg, infoList, codeGen);
                        if (!isDecomposed) return false;
                    }
                }
                return true;
            }
            else if (expr is MemberInitExpression)
            {
                MemberInitExpression mi = (MemberInitExpression)expr;
                bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam,
                                                             mi.NewExpression,
                                                             infoList, codeGen);
                if (!isDecomposed) return false;
                foreach (MemberBinding mb in mi.Bindings)
                {
                    isDecomposed = GetDecompositionInfoList(keyParam, groupParam, mb, infoList, codeGen);
                    if (!isDecomposed) return false;
                }
                return true;
            }
            else if (keyParam == null)
            {
                while (expr is MemberExpression)
                {
                    MemberExpression me = (MemberExpression)expr;
                    if (me.Expression == groupParam &&
                        me.Member.Name == "Key")
                    {
                        return true;
                    }
                    expr = me.Expression;
                }
                return false;
            }
            else
            {
                while (expr is MemberExpression)
                {
                    expr = ((MemberExpression)expr).Expression;
                }
                return (expr == keyParam);
            }

            foreach (var argExpr in argList)
            {
                bool isDecomposed = GetDecompositionInfoList(keyParam, groupParam, argExpr, infoList, codeGen);
                if (!isDecomposed) return false;
            }
            return true;
        }

        private static DecompositionInfo GetDecompositionInfo(ParameterExpression groupParam,
                                                              MethodCallExpression mcExpr,
                                                              HpcLinqCodeGen codeGen)
        {
            if (mcExpr.Arguments.Count == 0 || mcExpr.Arguments[0] != groupParam)
            {
                return null;
            }
            for (int i = 1; i < mcExpr.Arguments.Count; i++)
            {
                if (HpcLinqExpression.Contains(groupParam, mcExpr.Arguments[i]))
                {
                    return null;
                }
            }

            ExpressionSimplifier<object> evaluator = new ExpressionSimplifier<object>();
            Type[] paramTypeArgs = groupParam.Type.GetGenericArguments();
            Type sourceElemType = paramTypeArgs[paramTypeArgs.Length - 1];
            Type resultType = mcExpr.Type;
            Type decomposerType = null;
            
            DecomposableAttribute decomposableAttrib = AttributeSystem.GetDecomposableAttrib(mcExpr);
            if (decomposableAttrib != null)
            {
                decomposerType = decomposableAttrib.DecompositionType;
            }
            else
            {
                MethodInfo mInfo = mcExpr.Method;
                if (mInfo.DeclaringType == typeof(System.Linq.Enumerable) ||
                    mInfo.DeclaringType == typeof(System.Linq.Queryable))
                {
                    // For built-in decomposable operators.
                    switch (mInfo.Name)
                    {
                        case "Count":
                        case "LongCount":
                        {
                            Type outputType;
                            Expression body;
                            if (mInfo.Name == "Count")
                            {
                                outputType = typeof(Int32);
                                body = Expression.Constant(1, outputType);
                            }
                            else
                            {
                                outputType = typeof(Int64);
                                body = Expression.Constant((long)1, outputType);
                            }
                            ParameterExpression param1 = Expression.Parameter(outputType, "a");
                            ParameterExpression param2 = Expression.Parameter(sourceElemType, "e");
                            LambdaExpression seedExpr = Expression.Lambda(body, param2);
                            body = Expression.AddChecked(param1, body);
                            LambdaExpression accumulateExpr = Expression.Lambda(body, param1, param2);
                            param2 = Expression.Parameter(outputType, "b");
                            body = Expression.AddChecked(param1, param2);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(body, param1, param2);
                            
                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "Any":
                        {
                            ParameterExpression param1 = Expression.Parameter(typeof(bool), "a");
                            ParameterExpression param2;
                            Expression body;
                            if (mcExpr.Arguments.Count == 1)
                            {
                                param2 = Expression.Parameter(sourceElemType, "e");
                                body = Expression.Constant(true, typeof(bool));
                            }
                            else
                            {
                                LambdaExpression predExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                                param2 = predExpr.Parameters[0];
                                body = predExpr.Body;
                            }

                            LambdaExpression seedExpr = Expression.Lambda(body, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(Expression.Or(param1, body), param1, param2);
                            param2 = Expression.Parameter(typeof(bool), "b");
                            body = Expression.Or(param1, param2);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(body, param1, param2);

                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "All":
                        {
                            ParameterExpression param1 = Expression.Parameter(typeof(bool), "a");
                            LambdaExpression predExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                            ParameterExpression param2 = predExpr.Parameters[0];

                            Expression body = predExpr.Body;
                            LambdaExpression seedExpr = Expression.Lambda(body, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(Expression.And(param1, body), param1, param2);
                            param2 = Expression.Parameter(typeof(bool), "b");
                            body = Expression.And(param1, param2);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(body, param1, param2);

                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "First":
                        {
                            ParameterExpression param1 = Expression.Parameter(sourceElemType, "a");
                            ParameterExpression param2 = Expression.Parameter(sourceElemType, "e");

                            LambdaExpression seedExpr = Expression.Lambda(param2, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(param1, param1, param2);
                            LambdaExpression recursiveAccumulateExpr = accumulateExpr;

                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "Last":
                        {
                            ParameterExpression param1 = Expression.Parameter(sourceElemType, "a");
                            ParameterExpression param2 = Expression.Parameter(sourceElemType, "e");

                            LambdaExpression seedExpr = Expression.Lambda(param2, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(param2, param1, param2);
                            LambdaExpression recursiveAccumulateExpr = accumulateExpr;

                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "Sum":
                        {
                            ParameterExpression param1;
                            ParameterExpression param2;
                            Expression arg2;
                            if (mInfo.GetParameters().Length == 1)
                            {
                                param2 = Expression.Parameter(sourceElemType, "e");
                                arg2 = param2;
                            }
                            else
                            {
                                LambdaExpression selectExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                                param2 = selectExpr.Parameters[0];
                                arg2 = selectExpr.Body;
                            }

                            Expression abody, sbody;
                            if (arg2.Type.IsGenericType)
                            {
                                param1 = Expression.Parameter(arg2.Type.GetGenericArguments()[0], "a");
                                MethodInfo accumulateInfo = typeof(HpcLinqVertex).GetMethod(
                                                                     "SumAccumulate",
                                                                     new Type[] { param1.Type, arg2.Type });
                                sbody = Expression.Constant(0, param1.Type);
                                sbody = Expression.Call(accumulateInfo, sbody, arg2);
                                abody = Expression.Call(accumulateInfo, param1, arg2);
                            }
                            else
                            {
                                param1 = Expression.Parameter(arg2.Type, "a");
                                sbody = arg2;
                                abody = Expression.AddChecked(param1, arg2);
                            }

                            LambdaExpression seedExpr = Expression.Lambda(sbody, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(abody, param1, param2);
                            param2 = Expression.Parameter(param1.Type, "b");
                            Expression rbody = Expression.AddChecked(param1, param2);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(rbody, param1, param2);
                            Expression fbody = Expression.Convert(param1, arg2.Type);
                            LambdaExpression finalReduceExpr = Expression.Lambda(fbody, param1);
                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr,
                                                         recursiveAccumulateExpr, finalReduceExpr);
                        }
                        case "Max":
                        case "Min":
                        {
                            ParameterExpression param2;
                            Expression abody;
                            if (mInfo.GetParameters().Length == 1)
                            {
                                param2 = Expression.Parameter(sourceElemType, "e");
                                abody = param2;
                            }
                            else
                            {
                                LambdaExpression selectExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                                param2 = selectExpr.Parameters[0];
                                abody = selectExpr.Body;
                            }

                            ParameterExpression param1 = Expression.Parameter(abody.Type, "a");
                            Expression sbody = abody;
                            MethodInfo accumulateInfo;
                            string methodName = (mInfo.Name == "Max") ? "MaxAccumulate" : "MinAccumulate";
                            if (mInfo.IsGenericMethod && (mInfo.GetParameters().Length == 1))
                            {
                                accumulateInfo = typeof(HpcLinqVertex).GetMethod(methodName + "Generic");
                                accumulateInfo = accumulateInfo.MakeGenericMethod(sourceElemType);
                            }
                            else
                            {
                                accumulateInfo = typeof(HpcLinqVertex).GetMethod(
                                                               methodName,
                                                               new Type[] { param1.Type, abody.Type });
                            }
                            abody = Expression.Call(accumulateInfo, param1, abody);

                            LambdaExpression seedExpr = Expression.Lambda(sbody, param2);
                            LambdaExpression accumulateExpr = Expression.Lambda(abody, param1, param2);
                            param2 = Expression.Parameter(param1.Type, "b");
                            Expression rbody = Expression.Call(accumulateInfo, param1, param2);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(rbody, param1, param2);
                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "Aggregate":
                        {
                            ParameterExpression elemParam = Expression.Parameter(sourceElemType, "e");
                            LambdaExpression accumulateExpr;
                            LambdaExpression seedExpr;
                            if (mcExpr.Arguments.Count == 2)
                            {
                                accumulateExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                                seedExpr = Expression.Lambda(elemParam, elemParam);
                            }
                            else
                            {
                                accumulateExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[2]);
                                object seedVal = evaluator.Eval(mcExpr.Arguments[1]);
                                Expression body = Expression.Constant(seedVal, seedVal.GetType());
                                ParameterSubst subst = new ParameterSubst(accumulateExpr.Parameters[0], body);
                                body = subst.Visit(accumulateExpr.Body);
                                seedExpr = Expression.Lambda(body, accumulateExpr.Parameters[1]);
                            }
                            if (!HpcLinqExpression.IsAssociative(accumulateExpr))
                            {
                                return null;
                            }
                            LambdaExpression recursiveAccumulateExpr = HpcLinqExpression.GetAssociativeCombiner(accumulateExpr);
                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, null);
                        }
                        case "Average":
                        {
                            ParameterExpression param2;
                            Expression abody;
                            if (mInfo.GetParameters().Length == 1)
                            {
                                param2 = Expression.Parameter(sourceElemType, "e");
                                abody = param2;
                            }
                            else
                            {
                                LambdaExpression selectExpr = HpcLinqExpression.GetLambda(mcExpr.Arguments[1]);
                                param2 = selectExpr.Parameters[0];
                                abody = selectExpr.Body;
                            }
                            Type aggValueType = abody.Type;
                            if (aggValueType == typeof(int) ||
                                aggValueType == typeof(int?))
                            {
                                aggValueType = typeof(long);
                            }
                            else if (aggValueType == typeof(long?))
                            {
                                aggValueType = typeof(long);
                            }
                            else if (aggValueType == typeof(float) ||
                                     aggValueType == typeof(float?))
                            {
                                aggValueType = typeof(double);
                            }
                            else if (aggValueType == typeof(double?))
                            {
                                aggValueType = typeof(double);
                            }
                            else if (aggValueType == typeof(decimal?))
                            {
                                aggValueType = typeof(decimal);
                            }

                            Type sumAndCountType = typeof(AggregateValue<>).MakeGenericType(aggValueType);
                            ParameterExpression param1 = Expression.Parameter(sumAndCountType, "a");
                            MethodInfo accumulateInfo = typeof(HpcLinqVertex).GetMethod(
                                                               "AverageAccumulate",
                                                               new Type[] { sumAndCountType, abody.Type });

                            // Seed:
                            Expression sbody = Expression.New(sumAndCountType);
                            sbody = Expression.Call(accumulateInfo, sbody, abody);
                            LambdaExpression seedExpr = Expression.Lambda(sbody, param2);
                            
                            // Accumulate:
                            abody = Expression.Call(accumulateInfo, param1, abody);
                            LambdaExpression accumulateExpr = Expression.Lambda(abody, param1, param2);

                            // RecursiveAccumulate:
                            param2 = Expression.Parameter(param1.Type, "b");
                            PropertyInfo valueInfo = sumAndCountType.GetProperty("Value");
                            PropertyInfo countInfo = sumAndCountType.GetProperty("Count");
                            Expression sumExpr1 = Expression.Property(param1, valueInfo);
                            Expression countExpr1 = Expression.Property(param1, countInfo);
                            Expression sumExpr2 = Expression.Property(param2, valueInfo);
                            Expression countExpr2 = Expression.Property(param2, countInfo);
                            Expression sumExpr = Expression.AddChecked(sumExpr1, sumExpr2);
                            Expression countExpr = Expression.AddChecked(countExpr1, countExpr2);
                            ConstructorInfo cinfo = sumAndCountType.GetConstructor(new Type[] { sumExpr.Type, countExpr.Type });
                            Expression rbody = Expression.New(cinfo, sumExpr, countExpr);
                            LambdaExpression recursiveAccumulateExpr = Expression.Lambda(rbody, param1, param2);

                            // FinalReduce:
                            if (sumExpr1.Type == typeof(long))
                            {
                                sumExpr1 = Expression.Convert(sumExpr1, typeof(double));
                            }
                            Expression fbody = Expression.Divide(sumExpr1, countExpr1);
                            fbody = Expression.Convert(fbody, resultType);
                            if (resultType.IsGenericType)
                            {
                                Expression zeroExpr = Expression.Constant(0, typeof(long));
                                Expression condExpr = Expression.GreaterThan(countExpr1, zeroExpr);
                                Expression nullExpr = Expression.Constant(null, resultType);
                                fbody = Expression.Condition(condExpr, fbody, nullExpr);
                            }
                            LambdaExpression finalReduceExpr = Expression.Lambda(fbody, param1);
                            return new DecompositionInfo(mcExpr, seedExpr, accumulateExpr, recursiveAccumulateExpr, finalReduceExpr);
                        }
                        case "Contains":
                        {
                            decomposerType = typeof(ContainsDecomposition<>).MakeGenericType(sourceElemType);
                            break;
                        }
                        case "Distinct":
                        {
                            decomposerType = typeof(DistinctDecomposition<>).MakeGenericType(sourceElemType);
                            break;
                        }
                        default:
                        {
                            return null;
                        }
                    }
                }
            }

            if (decomposerType == null) return null;

            Type implementedInterface = null;
            Type[] interfaces = decomposerType.GetInterfaces();
            foreach (Type intf in interfaces)
            {
                if (intf.GetGenericTypeDefinition() == typeof(IDecomposable<,,>))
                {
                    if (implementedInterface != null)
                    {
                        throw new DryadLinqException("Decomposition class can implement only one decomposable interface.");
                    }
                    implementedInterface = intf;
                }
            }

            if (implementedInterface == null ||
                implementedInterface.GetGenericArguments().Length != 3)
            {
                throw new DryadLinqException("Decomposition class " + decomposerType.FullName +
                                           "must implement IDecomposable<,,>");
            }

            // The second type of the implemented interface definition is the accumulatorType.
            Type accumulatorType = implementedInterface.GetGenericArguments()[1];
            
            // Now check that all the types match up.
            Type decomposerInterface = typeof(IDecomposable<,,>).MakeGenericType(
                                                    sourceElemType, accumulatorType, resultType);
            if (!decomposerInterface.IsAssignableFrom(decomposerType))
            {
                throw new DryadLinqException("Decomposition class must match the function that it decorates.");
            }
            if (decomposerType.ContainsGenericParameters)
            {
                if (decomposerType.GetGenericArguments().Length != 1 ||
                    !decomposerType.GetGenericArguments()[0].IsGenericParameter)
                {
                    throw new DryadLinqException(decomposerType.Name + " must match the function it annotates.");
                }
                decomposerType = decomposerType.MakeGenericType(sourceElemType);
            }
            if (decomposerType.GetConstructor(Type.EmptyTypes) == null)
            {
                throw new DryadLinqException("Decomposition class must have a default constructor.");
            }

            // Add to the codegen a call of the static Initializer of decomposerType
            Expression[] args = new Expression[mcExpr.Arguments.Count-1];
            for (int i = 0; i < args.Length; i++)
            {
                args[i] = Expression.Convert(mcExpr.Arguments[i+1], typeof(object));
            }
            Expression stateExpr = Expression.NewArrayInit(typeof(object), args);
            string decomposerName = codeGen.AddDecompositionInitializer(decomposerType, stateExpr);
            ParameterExpression decomposer = Expression.Parameter(decomposerType, decomposerName);
                
            // Seed: TSource => TAccumulate                
            MethodInfo seedInfo1 = decomposerType.GetMethod("Seed");
            ParameterExpression p2 = Expression.Parameter(sourceElemType, "e");
            Expression sbody1 = Expression.Call(decomposer, seedInfo1, p2);
            LambdaExpression seedExpr1 = Expression.Lambda(sbody1, p2);

            // Accumulate: (TAccumulate, TSource) => TAccumulate
            MethodInfo accumulateInfo1 = decomposerType.GetMethod("Accumulate");
            ParameterExpression p1 = Expression.Parameter(accumulatorType, "a");
            Expression abody1 = Expression.Call(decomposer, accumulateInfo1, p1, p2);
            LambdaExpression accumulateExpr1 = Expression.Lambda(abody1, p1, p2);
            
            // RecursiveAccumulate: (TAccumulate, TAccumulate) => TAccumulate
            MethodInfo recursiveAccumulateInfo1 = decomposerType.GetMethod("RecursiveAccumulate");
            p2 = Expression.Parameter(accumulatorType, "e");
            Expression rbody1 = Expression.Call(decomposer, recursiveAccumulateInfo1, p1, p2);
            LambdaExpression recursiveAccumulateExpr1 = Expression.Lambda(rbody1, p1, p2);
                
            // FinalReduce: TAccumulate => TResult
            MethodInfo finalReduceInfo1 = decomposerType.GetMethod("FinalReduce");
            Expression fbody1 = Expression.Call(decomposer, finalReduceInfo1, p1);
            LambdaExpression finalReduceExpr1 = Expression.Lambda(fbody1, p1);

            return new DecompositionInfo(mcExpr, seedExpr1, accumulateExpr1, recursiveAccumulateExpr1, finalReduceExpr1);
        }

        // Precondition: idx < dInfoList.Count
        internal static Expression AccumulateList(Expression valueExpr,
                                                  ParameterExpression elemParam,
                                                  List<DecompositionInfo> dInfoList,
                                                  int idx)
        {
            LambdaExpression accumulateExpr = dInfoList[idx].Accumulator;
            if (dInfoList.Count == idx + 1)
            {
                ParameterSubst subst = new ParameterSubst(accumulateExpr.Parameters[0], valueExpr);
                Expression resultExpr = subst.Visit(accumulateExpr.Body);
                subst = new ParameterSubst(accumulateExpr.Parameters[1], elemParam);
                return subst.Visit(resultExpr);
            }
            else
            {
                PropertyInfo keyPropInfo = valueExpr.Type.GetProperty("Key");
                Expression keyValueExpr = Expression.Property(valueExpr, keyPropInfo);
                ParameterSubst subst = new ParameterSubst(accumulateExpr.Parameters[0], keyValueExpr);
                Expression expr1 = subst.Visit(accumulateExpr.Body);
                subst = new ParameterSubst(accumulateExpr.Parameters[1], elemParam);
                expr1 = subst.Visit(expr1);

                PropertyInfo valuePropInfo = valueExpr.Type.GetProperty("Value");
                Expression valueValueExpr = Expression.Property(valueExpr, valuePropInfo);
                Expression expr2 = AccumulateList(valueValueExpr, elemParam, dInfoList, idx + 1);

                Type pairType = typeof(Pair<,>).MakeGenericType(expr1.Type, expr2.Type);
                return Expression.New(pairType.GetConstructors()[0], expr1, expr2);
            }
        }

        // Precondition: idx < dInfoList.Count
        internal static Expression RecursiveAccumulateList(Expression valueExpr1,
                                                           Expression valueExpr2,
                                                           List<DecompositionInfo> dInfoList,
                                                           int idx)
        {
            LambdaExpression recursiveAccumulateExpr = dInfoList[idx].RecursiveAccumulator;
            if (dInfoList.Count == idx + 1)
            {
                ParameterSubst subst = new ParameterSubst(recursiveAccumulateExpr.Parameters[0], valueExpr1);
                Expression resultExpr = subst.Visit(recursiveAccumulateExpr.Body);
                subst = new ParameterSubst(recursiveAccumulateExpr.Parameters[1], valueExpr2);
                return subst.Visit(resultExpr);
            }
            else
            {
                PropertyInfo keyPropInfo1 = valueExpr1.Type.GetProperty("Key");
                Expression keyValueExpr1 = Expression.Property(valueExpr1, keyPropInfo1);
                PropertyInfo keyPropInfo2 = valueExpr2.Type.GetProperty("Key");
                Expression keyValueExpr2 = Expression.Property(valueExpr2, keyPropInfo2);
                ParameterSubst subst = new ParameterSubst(recursiveAccumulateExpr.Parameters[0], keyValueExpr1);
                Expression expr1 = subst.Visit(recursiveAccumulateExpr.Body);
                subst = new ParameterSubst(recursiveAccumulateExpr.Parameters[1], keyValueExpr2);
                expr1 = subst.Visit(expr1);

                PropertyInfo valuePropInfo1 = valueExpr1.Type.GetProperty("Value");
                Expression valueValueExpr1 = Expression.Property(valueExpr1, valuePropInfo1);
                PropertyInfo valuePropInfo2 = valueExpr2.Type.GetProperty("Value");
                Expression valueValueExpr2 = Expression.Property(valueExpr2, valuePropInfo2);
                Expression expr2 = RecursiveAccumulateList(valueValueExpr1, valueValueExpr2, dInfoList, idx + 1);

                Type pairType = typeof(Pair<,>).MakeGenericType(expr1.Type, expr2.Type);
                return Expression.New(pairType.GetConstructors()[0], expr1, expr2);
            }
        }
    }

    public class ContainsDecomposition<TSource> : IDecomposable<TSource, bool, bool>
    {
        private TSource m_value;
        private IEqualityComparer<TSource> m_comparer;
        
        public void Initialize(object state)
        {
            object[] args = state as object[];
            this.m_value = (TSource)args[0];
            if (args.Length > 1)
            {
                this.m_comparer = (IEqualityComparer<TSource>)args[1];
            }
            else
            {
                this.m_comparer = EqualityComparer<TSource>.Default;
            }
        }

        public bool Seed(TSource val)
        {
            return this.m_comparer.Equals(this.m_value, val);
        }
        
        public bool Accumulate(bool acc, TSource val)
        {
            return acc || this.m_comparer.Equals(this.m_value, val);
        }
        
        public bool RecursiveAccumulate(bool acc, bool val)
        {
            return acc || val;
        }
        
        public bool FinalReduce(bool val)
        {
            return val;
        }
    }

    public class DistinctDecomposition<TSource>
        : IDecomposable<TSource, DistinctSet<TSource>, IEnumerable<TSource>>
    {
        private IEqualityComparer<TSource> m_comparer;
        
        public void Initialize(object state)
        {
            object[] args = state as object[];
            if (args.Length > 0)
            {
                this.m_comparer = (IEqualityComparer<TSource>)args[0];
            }
            else
            {
                this.m_comparer = EqualityComparer<TSource>.Default;
            }
        }
        
        public DistinctSet<TSource> Seed(TSource val)
        {
            DistinctSet<TSource> set = new DistinctSet<TSource>();
            set.Add(val, this.m_comparer);
            return set;
        }
        
        public DistinctSet<TSource> Accumulate(DistinctSet<TSource> acc, TSource val)
        {
            acc.Add(val, this.m_comparer);
            return acc;
        }
        
        public DistinctSet<TSource> RecursiveAccumulate(DistinctSet<TSource> acc,
                                                        DistinctSet<TSource> val)
        {
            foreach (TSource x in val.GetElems(this.m_comparer))
            {
                acc.Add(x, this.m_comparer);
            }
            return acc;
        }
        
        public IEnumerable<TSource> FinalReduce(DistinctSet<TSource> val)
        {
            return val.ToArray(this.m_comparer);
        }
    }

    public class DistinctSet<TSource>
    {
        private const Int32 MaxCount = 32;
        private static readonly TSource[] Empty = new TSource[0];

        private TSource[] m_distinctElems;
        private TSource[] m_elems;
        private Int32 m_count;
        
        public DistinctSet()
        {
            this.m_distinctElems = Empty;
            this.m_elems = new TSource[1];
            this.m_count = 0;
        }

        public void Add(TSource elem, IEqualityComparer<TSource> comparer)
        {
            if (this.m_count == this.m_elems.Length)
            {
                if (this.m_count < MaxCount)
                {
                    TSource[] newElems = new TSource[this.m_count * 2];
                    Array.Copy(this.m_elems, 0, newElems, 0, this.m_count);
                    this.m_elems = newElems;
                }
                else
                {
                    this.m_distinctElems = this.ToArray(comparer);
                    this.m_elems = new TSource[2];
                    this.m_count = 0;
                }
            }
            this.m_elems[this.m_count++] = elem;
        }

        public IEnumerable<TSource> GetElems(IEqualityComparer<TSource> comparer)
        {
            HashSet<TSource> set = new HashSet<TSource>(comparer);
            for (int i = 0; i < this.m_count; i++)
            {
                if (set.Add(this.m_elems[i]))
                {
                    yield return this.m_elems[i];
                }
            }
            foreach (var elem in this.m_distinctElems)
            {
                if (!set.Contains(elem))
                {
                    yield return elem;
                }
            }
        }

        public TSource[] ToArray(IEqualityComparer<TSource> comparer)
        {
            HashSet<TSource> set = new HashSet<TSource>(comparer);
            for (int i = 0; i < this.m_count; i++)
            {
                set.Add(this.m_elems[i]);
            }
            Int32 idx = 0;
            for (int i = 0; i < this.m_distinctElems.Length; i++)
            {
                if (!set.Contains(this.m_distinctElems[i]))
                {
                    this.m_distinctElems[idx++] = this.m_distinctElems[i];
                }
            }
            TSource[] distinctElems = new TSource[idx + set.Count];
            Array.Copy(this.m_distinctElems, 0, distinctElems, 0, idx);
            foreach (var x in set)
            {
                distinctElems[idx++] = x;
            }
            return distinctElems;
        }
    }
}
