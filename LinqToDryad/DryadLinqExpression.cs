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
using System.Collections.ObjectModel;
using System.Text;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    // Various methods to support Expression manipulation.
    internal static class DryadLinqExpression
    {
        #region TOCSHARPSTRING
        public static ParameterExpression GetParameterMemberAccess(Expression expr)
        {
            while (expr is MemberExpression)
            {
                if (!(((MemberExpression)expr).Member is FieldInfo) &&
                    !(((MemberExpression)expr).Member is PropertyInfo))
                {
                    return null;
                }
                expr = ((MemberExpression)expr).Expression;
            }
            return (expr as ParameterExpression);
        }

        public static Expression CreateMemberAccess(Expression expr, params string[] fieldNames)
        {
            Expression resultExpr = expr;
            foreach (string name in fieldNames)
            {
                if (expr.Type.GetField(name) != null)
                {
                    resultExpr = Expression.Field(resultExpr, name);
                }
                else if (expr.Type.GetProperty(name) != null)
                {
                    resultExpr = Expression.Property(resultExpr, name);
                }
                else
                {
                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                 String.Format(SR.TypeDoesNotContainMember, expr.Type, name));
                }
            }
            return resultExpr;
        }

        // TBD: Not quite complete
        public static bool IsConstant(Expression expr)
        {
            if (expr is ConstantExpression) return true;
            if (expr is MemberExpression)
            {
                Expression expr1 = ((MemberExpression)expr).Expression;
                return expr1 == null || IsConstant(expr1);
            }
            return false;
        }
        
        public static LambdaExpression GetLambda(Expression expr)
        {
            while (expr.NodeType == ExpressionType.Quote)
            {
                expr = ((UnaryExpression)expr).Operand;
            }
            return expr as LambdaExpression;
        }
        
        public static bool Contains(ParameterExpression param, Expression expr)
        {
            FreeParameters freeParams = new FreeParameters();
            freeParams.Visit(expr);
            return freeParams.Parameters.Contains(param);
        }

        internal static bool IsAssociative(LambdaExpression expr)
        {
            if (AttributeSystem.GetAssociativeAttrib(expr) != null)
            {
                return true;
            }
            ParameterExpression param1 = expr.Parameters[0];
            ParameterExpression param2 = expr.Parameters[1];
            Expression operand1 = null;
            Expression operand2 = null;
            BinaryExpression body = expr.Body as BinaryExpression;
            if (body == null)
            {
                MethodCallExpression mcExpr = expr.Body as MethodCallExpression;
                if (mcExpr != null && mcExpr.Method.DeclaringType == typeof(System.Math))
                {
                    if (mcExpr.Method.Name == "Max" || mcExpr.Method.Name == "Min")
                    {
                        operand1 = mcExpr.Arguments[0];
                        operand2 = mcExpr.Arguments[1];
                    }
                }
            }
            else if (body.NodeType == ExpressionType.Add ||
                     body.NodeType == ExpressionType.AddChecked ||
                     body.NodeType == ExpressionType.Multiply ||
                     body.NodeType == ExpressionType.MultiplyChecked ||
                     body.NodeType == ExpressionType.And ||
                     body.NodeType == ExpressionType.Or ||
                     body.NodeType == ExpressionType.ExclusiveOr)
            {
                if (body.Method == null)
                {
                    operand1 = body.Left;
                    operand2 = body.Right;
                }
            }

            if (operand1 != null)
            {
                if (operand1 == param1)
                {
                    return !Contains(param1, operand2);
                }
                if (operand2 == param1)
                {
                    return !Contains(param1, operand1);
                }
            }
            return false;
        }

        internal static ExpressionType GetNodeType(string opName)
        {
            switch (opName)
            {
                case "op_Addition":
                    return ExpressionType.Add;
                case "op_Subtraction":
                    return ExpressionType.Subtract;
                case "op_Multiply":
                    return ExpressionType.Multiply;
                case "op_Division":
                    return ExpressionType.Divide;
                case "op_Modulus":
                    return ExpressionType.Modulo;
                case "op_BitwiseAnd":
                    return ExpressionType.And;
                case "op_BitwiseOr":
                    return ExpressionType.Or;
                case "op_ExclusiveOr":
                    return ExpressionType.ExclusiveOr;
                case "op_LeftShift":
                    return ExpressionType.LeftShift;
                case "op_RightShift":
                    return ExpressionType.RightShift;
                case "op_Equality":
                    return ExpressionType.Equal;
                case "op_Inequality":
                    return ExpressionType.NotEqual;
                case "op_LessThan":
                    return ExpressionType.LessThan;
                case "op_GreaterThan":
                    return ExpressionType.GreaterThan;
                case "op_LessThanOrEqual":
                    return ExpressionType.LessThanOrEqual;
                case "op_GreaterThanOrEqual":
                    return ExpressionType.GreaterThanOrEqual;
                case "op_UnaryPlus":
                    return ExpressionType.UnaryPlus;
                case "op_UnaryNegation":
                    return ExpressionType.Negate;
                case "op_LogicalNot":
                    return ExpressionType.Not;
                default:
                    throw new DryadLinqException(DryadLinqErrorCode.UnrecognizedOperatorName,
                                                 String.Format(SR.UnrecognizedOperatorName , opName));
            }
        }

        internal static LambdaExpression GetAssociativeCombiner(LambdaExpression expr)
        {
            AssociativeAttribute attrib = AttributeSystem.GetAssociativeAttrib(expr);
            if (attrib == null)
            {
                BinaryExpression bexpr = expr.Body as BinaryExpression;
                if (bexpr == null)
                {
                    MethodCallExpression mcExpr = expr.Body as MethodCallExpression;
                    ParameterExpression px = Expression.Parameter(mcExpr.Arguments[0].Type, "x");
                    ParameterExpression py = Expression.Parameter(mcExpr.Arguments[1].Type, "y");
                    Expression body = Expression.Call(mcExpr.Method, px, py);
                    return Expression.Lambda(body, px, py);
                }
                else
                {
                    ParameterExpression px = Expression.Parameter(bexpr.Left.Type, "x");
                    ParameterExpression py = Expression.Parameter(bexpr.Right.Type, "y");
                    Expression body = Expression.MakeBinary(bexpr.NodeType, px, py, bexpr.IsLiftedToNull, bexpr.Method);
                    return Expression.Lambda(body, px, py);
                }
            }
            else
            {
                Type[] funcTypeArgs = expr.Type.GetGenericArguments();
                MethodInfo cInfo = null;
                Type associativeType = attrib.AssociativeType;
                if (associativeType == null)
                {
                    if (expr.Body is MethodCallExpression)
                    {
                        cInfo = ((MethodCallExpression)expr.Body).Method;
                    }
                    else if (expr.Body is BinaryExpression)
                    {
                        cInfo = ((BinaryExpression)expr.Body).Method;
                    }
                    ParameterInfo[] pInfos = cInfo.GetParameters();
                    if (cInfo == null || pInfos.Length != 2)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeMethodHasWrongForm,
                                                     string.Format(SR.AssociativeMethodHasWrongForm, cInfo.Name));
                    }
                    if (funcTypeArgs[0] != pInfos[0].ParameterType || pInfos[0].ParameterType != pInfos[1].ParameterType)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeMethodHasWrongForm,
                                                     string.Format(SR.AssociativeMethodHasWrongForm, cInfo.Name));
                    }
                }
                else
                {
                    // determine if the attribute specifies an IAssociative.
                    if (associativeType.ContainsGenericParameters)
                    {
                        if (associativeType.GetGenericArguments().Length != 1)
                        {
                            throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypeDoesNotImplementInterface,
                                                         String.Format(SR.AssociativeTypeDoesNotImplementInterface,
                                                                       associativeType.FullName));
                        }
                        if (associativeType.GetGenericArguments()[0].IsGenericParameter)
                        {
                            associativeType = associativeType.MakeGenericType(funcTypeArgs[0]);
                        }
                    }
                    
                    Type implementedInterface = null;
                    Type[] interfaces = associativeType.GetInterfaces();
                    foreach (Type inter in interfaces)
                    {
                        if (inter.GetGenericTypeDefinition() == typeof(IAssociative<>))
                        {
                            if (implementedInterface != null)
                            {
                                throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypeImplementsTooManyInterfaces,
                                                             String.Format(SR.AssociativeTypeImplementsTooManyInterfaces,
                                                                           associativeType.FullName));
                            }
                            implementedInterface = inter;
                        }
                    }
                    if (implementedInterface == null)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypeDoesNotImplementInterface,
                                                     String.Format(SR.AssociativeTypeDoesNotImplementInterface,
                                                                   associativeType.FullName));
                    }
                    if (implementedInterface.GetGenericArguments()[0] != funcTypeArgs[0])
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypesDoNotMatch,
                                                   String.Format(SR.AssociativeTypesDoNotMatch,
                                                                 associativeType.FullName));
                    }
                    if (!associativeType.IsPublic && !associativeType.IsNestedPublic)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypeMustBePublic,
                                                   String.Format(SR.AssociativeTypeMustBePublic,
                                                                 associativeType.FullName));
                    }
                    try
                    {
                        associativeType = typeof(GenericAssociative<,>).MakeGenericType(associativeType, funcTypeArgs[0]);
                    }
                    catch (Exception)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.AssociativeTypeDoesNotHavePublicDefaultCtor,
                                                   String.Format(SR.AssociativeTypeDoesNotHavePublicDefaultCtor,
                                                                 associativeType.FullName));
                    }
                    cInfo = associativeType.GetMethod("Accumulate", new Type[] { funcTypeArgs[0], funcTypeArgs[0] });
                    Debug.Assert(cInfo != null, "problem finding method on associativeType");
                }

                ParameterExpression px = Expression.Parameter(funcTypeArgs[0], "x");
                ParameterExpression py = Expression.Parameter(funcTypeArgs[0], "y");
                Expression body;
                if (cInfo.Name.StartsWith("op_", StringComparison.Ordinal))
                {
                    body = Expression.MakeBinary(GetNodeType(cInfo.Name), px, py, false, cInfo); 
                }
                else
                {
                    body = Expression.Call(cInfo, px, py);
                }
                return Expression.Lambda(body, px, py);
            }
        }

        public static LambdaExpression Rewrite(LambdaExpression expr, LambdaExpression selector, Substitution pSubst)
        {
            if (expr != null)
            {
                Type resultType = selector.Body.Type;
                ParameterExpression resultParam = Expression.Parameter(resultType, "key_1");

                // Perform substitutions
                ExpressionSubst subst = new ExpressionSubst(pSubst);
                subst.AddSubst(selector.Body, resultParam);
                if (selector.Body is NewExpression)
                {
                    NewExpression newBody = (NewExpression)selector.Body;
                    if (newBody.Constructor != null)
                    {
                        resultType = newBody.Constructor.DeclaringType;
                    }
                    if (TypeSystem.IsAnonymousType(resultType))
                    {
                        PropertyInfo[] props = resultType.GetProperties();

                        //the following test is never expected to occur, and an assert would most likely suffice.
                        if (props.Length != newBody.Arguments.Count)
                        {
                            throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.BugInHandlingAnonymousClass);
                        }

                        for (int i = 0; i < props.Length; i++)
                        {
                            Expression leftExpr = newBody.Arguments[i];
                            Expression rightExpr = CreateMemberAccess(resultParam, props[i].Name);
                            subst.AddSubst(leftExpr, rightExpr);
                        }
                    }
                }
                if (selector.Body is MemberInitExpression)
                {
                    ReadOnlyCollection<MemberBinding> bindings = ((MemberInitExpression)selector.Body).Bindings;
                    for (int i = 0; i < bindings.Count; i++)
                    {
                        if (bindings[i] is MemberAssignment)
                        {
                            Expression leftExpr = ((MemberAssignment)bindings[i]).Expression;
                            Expression rightExpr = CreateMemberAccess(resultParam, ((MemberAssignment)bindings[i]).Member.Name);
                            subst.AddSubst(leftExpr, rightExpr);
                        }
                    }
                }
                else
                {
                    FieldMappingAttribute[] attribs = AttributeSystem.GetFieldMappingAttribs(selector);
                    if (attribs != null)
                    {
                        foreach (FieldMappingAttribute attrib in attribs)
                        {
                            string[] srcFieldNames = attrib.Source.Split('.');
                            string paramName = srcFieldNames[0];

                            ParameterInfo[] paramInfos = null;
                            if (selector.Body is MethodCallExpression)
                            {
                                paramInfos = ((MethodCallExpression)selector.Body).Method.GetParameters();
                            }
                            else if (selector.Body is NewExpression)
                            {
                                paramInfos = ((NewExpression)selector.Body).Constructor.GetParameters();
                            }

                            if (paramInfos != null)
                            {
                                int argIdx = -1;
                                for (int i = 0; i < paramInfos.Length; i++)
                                {
                                    if (paramInfos[i].Name == paramName)
                                    {
                                        argIdx = i;
                                        break;
                                    }
                                }

                                Expression leftExpr = null;
                                if (argIdx != -1)
                                {
                                    if (selector.Body is MethodCallExpression)
                                    {
                                        leftExpr = ((MethodCallExpression)selector.Body).Arguments[argIdx];
                                    }
                                    else if (selector.Body is NewExpression)
                                    {
                                        leftExpr = ((NewExpression)selector.Body).Arguments[argIdx];
                                    }
                                }
                                if (leftExpr == null)
                                {
                                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                               "The source of the FieldMapping annotation was wrong. " +
                                                               paramName + " is not a formal parameter.");
                                }

                                string[] fieldNames = new string[srcFieldNames.Length - 1];
                                for (int i = 1; i < srcFieldNames.Length; i++)
                                {
                                    fieldNames[i] = srcFieldNames[i-1];
                                }
                                leftExpr = CreateMemberAccess(leftExpr, fieldNames);
                                Expression rightExpr = CreateMemberAccess(resultParam, attrib.Destination.Split('.'));
                                subst.AddSubst(leftExpr, rightExpr);
                            }
                        }
                    }
                }
                Expression resultBody = subst.Visit(expr.Body);

                // Check if the substitutions are complete
                FreeParameters freeParams = new FreeParameters();
                freeParams.Visit(resultBody);
                if (freeParams.Parameters.Count == 1 && freeParams.Parameters.Contains(resultParam))
                {
                    Type funcType = typeof(Func<,>).MakeGenericType(resultType, expr.Body.Type);
                    return Expression.Lambda(funcType, resultBody, resultParam);
                }
            }
            return null;
        }

        public static string EscapeString(char ch)
        {
            switch (ch)
            {
                case '\'':
                case '\"':
                case '\\':
                    return "\\" + ch;
                case '\0':
                    return "\\0";
                case '\a':
                    return "\\a";
                case '\b':
                    return "\\b";
                case '\f':
                    return "\\f";
                case '\n':
                    return "\\n";
                case '\r':
                    return "\\r";
                case '\t':
                    return "\\t";
                case '\v':
                    return "\\v";
               default:
                   return null;
            }
        }
        
        private static Dictionary<string, string> s_transIdMap = new Dictionary<string, string>();
        public static string ToCSharpString(Expression expr)
        {
            return ToCSharpString(expr, new Dictionary<Type, string>());
        }

        public static string ToCSharpString(Expression expr, Dictionary<Type, string> typeNames)
        {
            StringBuilder builder = new StringBuilder();
            BuildExpression(builder, expr, typeNames);
            return builder.ToString();
        }

        private static void BuildExpression(StringBuilder builder,
                                            Expression expr,
                                            Dictionary<Type, string> typeNames)
        {
            if (IsConstant(expr))
            {
                object val = ExpressionSimplifier.Evaluate(expr);
                if (val == null)
                {
                    builder.Append("(" + TypeSystem.TypeName(expr.Type, typeNames) + ")");
                    builder.Append("null");
                }
                else
                {
                    Type valType = val.GetType();
                    if (valType.IsPrimitive)
                    {
                        TypeCode tcode = Type.GetTypeCode(expr.Type);
                        if (tcode == TypeCode.Boolean)
                        {
                            builder.Append(((bool)val) ? "true" : "false");
                        }
                        else if (tcode == TypeCode.Char)
                        {
                            string escapeStr = EscapeString((char)val);
                            if (escapeStr == null)
                            {
                                builder.Append("'" + val + "'");
                            }
                            else
                            {
                                builder.Append("'" + escapeStr + "'");
                            }
                        }
                        else
                        {
                            builder.Append("((" + TypeSystem.TypeName(valType, typeNames) + ")(");
                            builder.Append(val + "))");
                        }
                    }
                    else if (valType.IsEnum)
                    {
                        builder.Append(TypeSystem.TypeName(valType, typeNames) + "." + val);
                    }
                    else if (val is string)
                    {
                        builder.Append("@\"");
                        builder.Append(((string)val).Replace("\"", "\"\""));
                        builder.Append("\"");
                    }
                    else if (val is Expression)
                    {
                        BuildExpression(builder, (Expression)val, typeNames);
                    }
                    else
                    {
                        int valIdx = DryadLinqObjectStore.Put(val);
                        builder.Append("((" + TypeSystem.TypeName(expr.Type, typeNames) + ")");
                        builder.Append("DryadLinqObjectStore.Get(" + valIdx + "))");
                    }
                }
            }
            else if (expr is BinaryExpression)
            {
                BuildBinaryExpression(builder, (BinaryExpression)expr, typeNames);
            }
            else if (expr is ConditionalExpression)
            {
                BuildConditionalExpression(builder, (ConditionalExpression)expr, typeNames);
            }
            else if (expr is ConstantExpression)
            {
                BuildConstantExpression(builder, (ConstantExpression)expr, typeNames);
            }
            else if (expr is InvocationExpression)
            {
                BuildInvocationExpression(builder, (InvocationExpression)expr, typeNames);
            }
            else if (expr is LambdaExpression)
            {
                BuildLambdaExpression(builder, (LambdaExpression)expr, typeNames);
            }
            else if (expr is MemberExpression)
            {
                BuildMemberExpression(builder, (MemberExpression)expr, typeNames);
            }
            else if (expr is MethodCallExpression)
            {
                BuildMethodCallExpression(builder, (MethodCallExpression)expr, typeNames);
            }
            else if (expr is NewExpression)
            {
                BuildNewExpression(builder, (NewExpression)expr, typeNames);
            }
            else if (expr is NewArrayExpression)
            {
                BuildNewArrayExpression(builder, (NewArrayExpression)expr, typeNames);
            }
            else if (expr is MemberInitExpression)
            {
                BuildMemberInitExpression(builder, (MemberInitExpression)expr, typeNames);
            }
            else if (expr is ListInitExpression)
            {
                BuildListInitExpression(builder, (ListInitExpression)expr, typeNames);
            }
            else if (expr is ParameterExpression)
            {
                BuildParameterExpression(builder, (ParameterExpression)expr, typeNames);
            }
            else if (expr is TypeBinaryExpression)
            {
                BuildTypeBinaryExpression(builder, (TypeBinaryExpression)expr, typeNames);
            }
            else if (expr is UnaryExpression)
            {
                BuildUnaryExpression(builder, (UnaryExpression)expr, typeNames);
            }
            else
            {
                throw new DryadLinqException(DryadLinqErrorCode.UnsupportedExpressionsType,
                                             String.Format(SR.UnsupportedExpressionsType, expr.NodeType));
            }
        }

        private static void BuildInvocationExpression(StringBuilder builder,
                                                      InvocationExpression expr,
                                                      Dictionary<Type, string> typeNames)
        {
            builder.Append("(");
            builder.Append("(");

            // type cast to method
            builder.Append("(");
            builder.Append(TypeSystem.TypeName(expr.Expression.Type, typeNames));
            builder.Append(")");
            // method name
            builder.Append("(");
            BuildExpression(builder, expr.Expression, typeNames);
            builder.Append(")");

            builder.Append(")");

            // method invocation
            builder.Append("(");
            bool isFirst = true;
            foreach (Expression arg in expr.Arguments)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    builder.Append(", ");
                }
                BuildExpression(builder, arg, typeNames);
            }
            builder.Append(")");
            builder.Append(")");
        }

        private static void BuildBinaryExpression(StringBuilder builder,
                                                  BinaryExpression expr,
                                                  Dictionary<Type, string> typeNames)
        {
            if (expr.NodeType == ExpressionType.ArrayIndex)
            {
                BuildExpression(builder, expr.Left, typeNames);
                builder.Append("[");
                BuildExpression(builder, expr.Right, typeNames);
                builder.Append("]");
            }
            else
            {
                string op = GetBinaryOperator(expr);
                if (op != null)
                {
                    bool isChecked = (expr.NodeType == ExpressionType.AddChecked ||
                                      expr.NodeType == ExpressionType.SubtractChecked ||
                                      expr.NodeType == ExpressionType.MultiplyChecked);
                    if (isChecked) builder.Append("checked(");
                    builder.Append("(");
                    BuildExpression(builder, expr.Left, typeNames);
                    builder.Append(" ");
                    builder.Append(op);
                    builder.Append(" ");
                    BuildExpression(builder, expr.Right, typeNames);
                    builder.Append(")");
                    if (isChecked) builder.Append(")");
                }
                else {
                    builder.Append(expr.NodeType);
                    builder.Append("(");
                    BuildExpression(builder, expr.Left, typeNames);
                    builder.Append(", ");
                    BuildExpression(builder, expr.Right, typeNames);
                    builder.Append(")");
                }
            }
        }

        internal static string GetBinaryOperator(BinaryExpression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return "+";
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return "-";
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "%";
                case ExpressionType.And:
                    return (expr.Type == typeof(bool) || expr.Type == typeof(bool?)) ? "&&" : "&";
                case ExpressionType.AndAlso:
                    return "&&";
                case ExpressionType.Or:
                    return (expr.Type == typeof(bool) || expr.Type == typeof(bool?)) ? "||" : "|";
                case ExpressionType.OrElse:
                    return "||";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.Equal:
                    return "==";
                case ExpressionType.NotEqual:
                    return "!=";
                case ExpressionType.Coalesce:
                    return "??";
                case ExpressionType.RightShift:
                    return ">>";
                case ExpressionType.LeftShift:
                    return "<<";
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Power:
                    return "^";
            }
            return null;
        }

        private static void BuildConditionalExpression(StringBuilder builder,
                                                       ConditionalExpression expr,
                                                       Dictionary<Type, string> typeNames)
        {
            builder.Append("(");
            builder.Append("(");
            BuildExpression(builder, expr.Test, typeNames);
            builder.Append(") ? (");
            BuildExpression(builder, expr.IfTrue, typeNames);
            builder.Append(") : (");
            BuildExpression(builder, expr.IfFalse, typeNames);
            builder.Append(")");
            builder.Append(")");
        }
        
        private static void BuildConstantExpression(StringBuilder builder,
                                                    ConstantExpression expr,
                                                    Dictionary<Type, string> typeNames)
        {
            if (expr.Value == null)
            {
                builder.Append("null");
            }
            else
            {
                if (expr.Value is string)
                {
                    builder.Append("@\"");
                    builder.Append(expr.Value);
                    builder.Append("\"");
                }
                else if (expr.Value.ToString() == expr.Value.GetType().ToString())
                {
                    builder.Append("value(");
                    builder.Append(expr.Value);
                    builder.Append(")");
                }
                else
                {
                    builder.Append(expr.Value);
                }
            }
        }

        private static void BuildLambdaExpression(StringBuilder builder,
                                                  LambdaExpression expr,
                                                  Dictionary<Type, string> typeNames)
        {
            foreach (ParameterExpression param in expr.Parameters)
            {
                if (TypeSystem.IsTransparentIdentifier(param.Name))
                {
                    string newName = DryadLinqCodeGen.MakeUniqueName("h__TransparentIdentifier");
                    s_transIdMap[param.Name] = newName;
                }
            }
            if (expr.Parameters.Count == 1)
            {
                BuildExpression(builder, expr.Parameters[0], typeNames);
            }
            else
            {
                builder.Append("(");
                for (int i = 0, n = expr.Parameters.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    BuildExpression(builder, expr.Parameters[i], typeNames);
                }
                builder.Append(")");
            }
            builder.Append(" => ");
            BuildExpression(builder, expr.Body, typeNames);
        }

        private static void BuildMemberExpression(StringBuilder builder,
                                                  MemberExpression expr,
                                                  Dictionary<Type, string> typeNames)
        {
            if (expr.Expression == null)
            {
                builder.Append(TypeSystem.TypeName(expr.Member.DeclaringType, typeNames));
            }
            else
            {
                ParameterExpression param = expr.Expression as ParameterExpression;
                if (param != null)
                {
                    BuildExpression(builder, param, typeNames);
                }
                else
                {
                    BuildExpression(builder, expr.Expression, typeNames);
                }
            }
            builder.Append(".");
            string memberName = expr.Member.Name;
            if (TypeSystem.IsTransparentIdentifier(memberName))
            {
                if (s_transIdMap.ContainsKey(memberName))
                {
                    memberName = s_transIdMap[memberName];
                }
                else
                {
                    string newName = DryadLinqCodeGen.MakeUniqueName("h__TransparentIdentifier");
                    s_transIdMap[memberName] = newName;
                    memberName = newName;
                }
            }
            builder.Append(memberName);
        }

        private static void BuildMethodCallExpression(StringBuilder builder,
                                                      MethodCallExpression expr,
                                                      Dictionary<Type, string> typeNames)
        {
            Expression obj = expr.Object;
            int start = 0;
            if (Attribute.GetCustomAttribute(expr.Method, typeof(System.Runtime.CompilerServices.ExtensionAttribute)) != null)
            {
                start = 1;
                obj = expr.Arguments[0];
            }
            bool desugar = expr.Method.IsStatic && !TypeSystem.IsQueryOperatorCall(expr);
            if (obj == null || desugar)
            {
                Type type = expr.Method.DeclaringType;
                builder.Append(TypeSystem.TypeName(type, typeNames));
            }
            else
            {
                BuildExpression(builder, obj, typeNames);
            }

            if (TypeSystem.IsProperty(expr.Method))
            {
                // Special case: an indexer property
                builder.Append("[");
                for (int i = start, n = expr.Arguments.Count; i < n; i++)
                {
                    if (i > start) builder.Append(", ");
                    BuildExpression(builder, expr.Arguments[i], typeNames);
                }
                builder.Append("]");
            }
            else
            {
                bool isArrayIndexer = (expr.Method.DeclaringType.IsArray && expr.Method.Name == "Get");
                if (isArrayIndexer)
                {
                    builder.Append("[");
                }
                else
                {
                    builder.Append(".");
                    builder.Append(expr.Method.Name);
                    if (expr.Method.IsGenericMethod &&
                        !TypeSystem.ContainsAnonymousType(expr.Method.GetGenericArguments()))
                    {
                        builder.Append("<");
                        bool first = true;
                        foreach (Type t in expr.Method.GetGenericArguments())
                        {
                            if (first)
                            {
                                first = false;
                            }
                            else
                            {
                                builder.Append(",");
                            }
                            builder.Append(TypeSystem.TypeName(t, typeNames));
                        }
                        builder.Append(">");
                    }
                    builder.Append("(");
                }

                bool isFirst = true;
                if (obj != null && desugar)
                {
                    isFirst = false;
                    BuildExpression(builder, obj, typeNames);
                }
                for (int i = start, n = expr.Arguments.Count; i < n; i++)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        builder.Append(", ");
                    }
                    BuildExpression(builder, expr.Arguments[i], typeNames);
                }

                builder.Append((isArrayIndexer) ? "]" : ")");
            }
        }

        private static void BuildNewExpression(StringBuilder builder,
                                               NewExpression expr,
                                               Dictionary<Type, string> typeNames)
        {
            Type type = (expr.Constructor == null) ? expr.Type : expr.Constructor.DeclaringType;
            builder.Append("new ");
            string typeName = null;
            if (TypeSystem.IsAnonymousType(type))
            {
                if (!typeNames.TryGetValue(type, out typeName))
                {
                    PropertyInfo[] props = type.GetProperties();
                    System.Array.Sort(props, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));                    
                    builder.Append("{");
                    for (int i = 0; i < props.Length; i++)
                    {
                        if (i > 0) builder.Append(", ");
                        string propName = props[i].Name;
                        if (TypeSystem.IsTransparentIdentifier(propName))
                        {
                            if (s_transIdMap.ContainsKey(propName))
                            {
                                propName = s_transIdMap[propName];
                            }
                            else
                            {
                                string newName = DryadLinqCodeGen.MakeUniqueName("h__TransparentIdentifier");
                                s_transIdMap.Add(propName, newName);
                                propName = newName;
                            }
                        }
                        builder.Append(propName + " = ");
                        BuildExpression(builder, expr.Arguments[i], typeNames);
                    }
                    builder.Append("}");
                    return;
                }
            }
            else
            {
                typeName = TypeSystem.TypeName(type, typeNames);
            }

            builder.Append(typeName);
            builder.Append("(");
            for (int i = 0; i < expr.Arguments.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                BuildExpression(builder, expr.Arguments[i], typeNames);
            }
            builder.Append(")");
        }

        private static void BuildNewArrayExpression(StringBuilder builder,
                                                    NewArrayExpression expr,
                                                    Dictionary<Type, string> typeNames)
        {
            builder.Append("new ");
            if (expr.NodeType == ExpressionType.NewArrayBounds)
            {
                Type baseType = expr.Type.GetElementType();
                while (baseType.IsArray)
                {
                    baseType = baseType.GetElementType();
                }
                builder.Append(TypeSystem.TypeName(baseType, typeNames));
                builder.Append("[");
                for (int i = 0, n = expr.Expressions.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    BuildExpression(builder, expr.Expressions[i], typeNames);
                }
                builder.Append("]");

                Type elemType = expr.Type.GetElementType();
                while (elemType.IsArray)
                {
                    builder.Append("[");
                    int rank = elemType.GetArrayRank();
                    for (int i = 1; i < rank; i++)
                    {
                        builder.Append(',');
                    }
                    builder.Append("]");
                    elemType = elemType.GetElementType();
                }
            }
            else
            {
                Debug.Assert(expr.NodeType == ExpressionType.NewArrayInit);
                builder.Append(TypeSystem.TypeName(expr.Type, typeNames));
                builder.Append(" {");
                for (int i = 0, n = expr.Expressions.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    BuildExpression(builder, expr.Expressions[i], typeNames);
                }
                builder.Append("}");
            }
        }

        private static void BuildMemberInitExpression(StringBuilder builder,
                                                      MemberInitExpression expr,
                                                      Dictionary<Type, string> typeNames)
        {
            if (expr.NewExpression.Arguments.Count == 0 &&
                expr.NewExpression.Type.Name.Contains("<"))
            {
                // anonymous type constructor
                builder.Append("new");
            }
            else
            {
                BuildExpression(builder, expr.NewExpression, typeNames);
            }
            builder.Append(" {");
            for (int i = 0, n = expr.Bindings.Count; i < n; i++)
            {
                if (i > 0) builder.Append(", ");
                BuildMemberBinding(builder, expr.Bindings[i], typeNames);
            }
            builder.Append("}");
        }

        private static void BuildListInitExpression(StringBuilder builder,
                                                    ListInitExpression expr,
                                                    Dictionary<Type, string> typeNames)
        {
            BuildExpression(builder, expr.NewExpression, typeNames);
            builder.Append(" {");
            for (int i = 0, n = expr.Initializers.Count; i < n; i++)
            {
                if (i > 0) builder.Append(", ");
                BuildElementInit(builder, expr.Initializers[i], typeNames);
            }
            builder.Append("}");
        }

        private static void BuildParameterExpression(StringBuilder builder,
                                                     ParameterExpression expr,
                                                     Dictionary<Type, string> typeNames)
        {
            if (expr.Name == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.UnnamedParameterExpression);
            }
            string paramName = expr.Name;
            if (s_transIdMap.ContainsKey(paramName))
            {
                paramName = s_transIdMap[paramName];
            }
            builder.Append(paramName);
        }

        private static void BuildTypeBinaryExpression(StringBuilder builder,
                                                      TypeBinaryExpression expr,
                                                      Dictionary<Type, string> typeNames)
        {
            Debug.Assert(expr.NodeType == ExpressionType.TypeIs);
            builder.Append("(");
            BuildExpression(builder, expr.Expression, typeNames);
            builder.Append(" is ");
            builder.Append(TypeSystem.TypeName(expr.TypeOperand, typeNames));
            builder.Append(")");
        }

        private static void BuildUnaryExpression(StringBuilder builder,
                                                 UnaryExpression expr,
                                                 Dictionary<Type, string> typeNames)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.ArrayLength:
                {
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append(".Length");
                    break;
                }
                case ExpressionType.Convert:
                {
                    bool isChecked = (expr.NodeType == ExpressionType.ConvertChecked);
                    if (isChecked) builder.Append("checked(");

                    builder.Append("((");
                    builder.Append(TypeSystem.TypeName(expr.Type, typeNames));
                    builder.Append(")");
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append(")");

                    if (isChecked) builder.Append(")");
                    break;
                }
                case ExpressionType.TypeAs:
                {
                    builder.Append("(");
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append(" as ");
                    builder.Append(TypeSystem.TypeName(expr.Type, typeNames));
                    builder.Append(")");
                    break;
                }
                case ExpressionType.Not:
                {
                    //bug 15050.. Not is represented in C# two ways, depending on operand type.
                    // see http://msdn.microsoft.com/en-us/library/bb361179.aspx
                    if (expr.Operand.Type == typeof(bool) || expr.Operand.Type == typeof(bool?))
                    {
                        builder.Append("(!(");
                        BuildExpression(builder, expr.Operand, typeNames);
                        builder.Append("))");
                    }
                    else
                    {
                        builder.Append("(~(");
                        BuildExpression(builder, expr.Operand, typeNames);
                        builder.Append("))");
                    }

                    break;
                }
                case ExpressionType.Negate:
                { 
                    builder.Append("(-(");
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append("))");
                    break;
                }
                case ExpressionType.NegateChecked:
                {
                    builder.Append("checked(-(");
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append("))");
                    break;
                }
                case ExpressionType.Quote:
                {
                    BuildExpression(builder, expr.Operand, typeNames);
                    break;
                }
                default:
                {
                    builder.Append(expr.NodeType);
                    builder.Append("(");
                    BuildExpression(builder, expr.Operand, typeNames);
                    builder.Append(")");
                    break;
                }
            }
        }

        private static void BuildMemberBinding(StringBuilder builder,
                                               MemberBinding binding,
                                               Dictionary<Type, string> typeNames)
        {
            if (binding is MemberAssignment)
            {
                builder.Append(binding.Member.Name);
                builder.Append(" = ");
                BuildExpression(builder, ((MemberAssignment)binding).Expression, typeNames);
            }
            else if (binding is MemberMemberBinding)
            {
                builder.Append(binding.Member.Name);
                builder.Append(" = {");

                MemberMemberBinding mmBinding = (MemberMemberBinding)binding;
                for (int i = 0, n = mmBinding.Bindings.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    BuildMemberBinding(builder, mmBinding.Bindings[i], typeNames);
                }
                builder.Append("}");
            }
            else
            {
                Debug.Assert(binding is MemberListBinding);
                builder.Append(binding.Member.Name);
                builder.Append(" = {");

                MemberListBinding mlBinding = (MemberListBinding)binding;
                for (int i = 0, n = mlBinding.Initializers.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    BuildElementInit(builder, mlBinding.Initializers[i], typeNames);
                }
                builder.Append("}");            
            }
        }

        private static void BuildElementInit(StringBuilder builder,
                                             ElementInit elemInit,
                                             Dictionary<Type, string> typeNames)
        {
            // An ElementInitExpression is something like: "Add(arg1, arg2..)"
            // which corresponds to C# syntax such as:
            //      1. new Dictionary<T> { {arg1, arg2} }; 
            //      AND/OR
            //      2. Dictionary<T> x = new Dictionary<T>(); x.Add(arg1,arg2);
            //
            // The caller of BuildElementInit looks after the multiple *items* added to a collection, but this method must cope with
            // Add() methods that take one or more arguments.
            //
            // The main example for multi-argument Add() methods is Dictionary.Add(key,val), but user-defined classes 
            // can also participate.  C# uses duck-typing for this and similar language extensions.

            // Bug 15049: when emitting inline (syntax #1) don't emit elemInit.AddMethod, but do emit braces to demarcate the parameters
            builder.Append("{");
            bool isFirst = true;
            foreach (Expression argument in elemInit.Arguments)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    builder.Append(",");
                }
                BuildExpression(builder, argument, typeNames);
            }
            builder.Append("}");
        }
        #endregion

        #region SUMMARIZE
        // summarizing expressions: like ToString, but more compact
        public static string Summarize(Expression expr)
        {
            return Summarize(expr, 0);
        }

        public static string Summarize(Expression expr, int level)
        {
            StringBuilder builder = new StringBuilder();
            Summarize(builder, expr, level);
            return builder.ToString();
        }

        private static void Summarize(StringBuilder builder, Expression expr, int level)
        {
            if (IsConstant(expr))
            {
                object val = ExpressionSimplifier.Evaluate(expr);
                if (val == null)
                {
                    builder.Append("null");
                }
                else
                {
                    Type valType = val.GetType();
                    if (valType.IsPrimitive)
                    {
                        TypeCode tcode = Type.GetTypeCode(expr.Type);
                        if (tcode == TypeCode.Char)
                        {
                            string escapeStr = EscapeString((char)val);
                            if (escapeStr == null)
                            {
                                builder.Append("'" + val + "'");
                            }
                            else
                            {
                                builder.Append("'" + escapeStr + "'");
                            }
                        }
                        else
                        {
                            builder.Append(val.ToString());
                        }
                    }
                    else if (val is Expression)
                    {
                        Summarize(builder, (Expression)val, level);
                    }
                    else if (val is Delegate)
                    {
                        builder.Append(((Delegate)val).Method.Name);
                    }
                    else
                    {
                        builder.Append('_');
                    }
                }
            }
            else if (expr is BinaryExpression)
            {
                SummarizeBinaryExpression(builder, (BinaryExpression)expr, level);
            }
            else if (expr is ConditionalExpression)
            {
                SummarizeConditionalExpression(builder, (ConditionalExpression)expr, level);
            }
            else if (expr is ConstantExpression)
            {
                SummarizeConstantExpression(builder, (ConstantExpression)expr);
            }
            else if (expr is InvocationExpression)
            {
                SummarizeInvocationExpression(builder, (InvocationExpression)expr, level);
            }
            else if (expr is LambdaExpression)
            {
                SummarizeLambdaExpression(builder, (LambdaExpression)expr, level);
            }
            else if (expr is MemberExpression)
            {
                SummarizeMemberExpression(builder, (MemberExpression)expr, level);
            }
            else if (expr is MethodCallExpression)
            {
                SummarizeMethodCallExpression(builder, (MethodCallExpression)expr, level);
            }
            else if (expr is NewExpression)
            {
                SummarizeNewExpression(builder, (NewExpression)expr, level);
            }
            else if (expr is NewArrayExpression)
            {
                SummarizeNewArrayExpression(builder, (NewArrayExpression)expr, level);
            }
            else if (expr is MemberInitExpression)
            {
                SummarizeMemberInitExpression(builder, (MemberInitExpression)expr, level);
            }
            else if (expr is ListInitExpression)
            {
                SummarizeListInitExpression(builder, (ListInitExpression)expr, level);
            }
            else if (expr is ParameterExpression)
            {
                SummarizeParameterExpression(builder, (ParameterExpression)expr);
            }
            else if (expr is TypeBinaryExpression)
            {
                SummarizeTypeBinaryExpression(builder, (TypeBinaryExpression)expr, level);
            }
            else if (expr is UnaryExpression)
            {
                SummarizeUnaryExpression(builder, (UnaryExpression)expr, level);
            }
            else
            {
                throw new DryadLinqException(DryadLinqErrorCode.UnsupportedExpressionType,
                                             String.Format(SR.UnsupportedExpressionType, expr.NodeType));
            }
        }

        private static void SummarizeInvocationExpression(StringBuilder builder, InvocationExpression expr, int level)
        {
            bool isConstant = expr.Expression is ConstantExpression;
            if (!isConstant)
                builder.Append("(");
            Summarize(builder, expr.Expression, level);
            if (!isConstant)
                builder.Append(")");

            // method invocation
            builder.Append("(");
            bool isFirst = true;
            foreach (Expression arg in expr.Arguments)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    builder.Append(", ");
                }
                Summarize(builder, arg, level);
            }
            builder.Append(")");
        }

        private static void SummarizeBinaryExpression(StringBuilder builder, BinaryExpression expr, int level)
        {
            if (expr.NodeType == ExpressionType.ArrayIndex)
            {
                Summarize(builder, expr.Left, level);
                builder.Append("[");
                Summarize(builder, expr.Right, level);
                builder.Append("]");
            }
            else
            {
                string op = GetBinaryOperator(expr);
                if (op != null)
                {
                    builder.Append("(");
                    Summarize(builder, expr.Left, level);
                    builder.Append(" ");
                    builder.Append(op);
                    builder.Append(" ");
                    Summarize(builder, expr.Right, level);
                    builder.Append(")");
                }
                else
                {
                    builder.Append(expr.NodeType);
                    builder.Append("(");
                    Summarize(builder, expr.Left, level);
                    builder.Append(", ");
                    Summarize(builder, expr.Right, level);
                    builder.Append(")");
                }
            }
        }

        private static void SummarizeConditionalExpression(StringBuilder builder, ConditionalExpression expr, int level)
        {
            builder.Append("(");
            Summarize(builder, expr.Test, level);
            builder.Append(") ? (");
            Summarize(builder, expr.IfTrue, level);
            builder.Append(") : (");
            Summarize(builder, expr.IfFalse, level);
            builder.Append(")");
        }

        private static void SummarizeConstantExpression(StringBuilder builder, ConstantExpression expr)
        {
            builder.Append('_');
        }

        private static void SummarizeLambdaExpression(StringBuilder builder, LambdaExpression expr, int level)
        {
            if (expr.Parameters.Count == 1)
            {
                Summarize(builder, expr.Parameters[0], level);
            }
            else
            {
                builder.Append("(");
                for (int i = 0, n = expr.Parameters.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    Summarize(builder, expr.Parameters[i], level);
                }
                builder.Append(")");
            }
            builder.Append(" => ");
            Summarize(builder, expr.Body, level);
        }

        private static void SummarizeMemberExpression(StringBuilder builder, MemberExpression expr, int level)
        {
            if (expr.Expression == null)
            {
                builder.Append(TypeSystem.TypeName(expr.Member.DeclaringType));
            }
            else
            {
                Summarize(builder, expr.Expression, level);
            }
            builder.Append(".");
            builder.Append(expr.Member.Name);
        }

        private static void SummarizeMethodCallExpression(StringBuilder builder, MethodCallExpression expr, int level)
        {
            Expression obj = expr.Object;
            int start = 0;
            if (Attribute.GetCustomAttribute(expr.Method, typeof(System.Runtime.CompilerServices.ExtensionAttribute)) != null)
            {
                start = 1;
                obj = expr.Arguments[0];
            }
            if (obj == null)
            {
                Type type = expr.Method.DeclaringType;
                builder.Append(DryadLinqUtil.SimpleName(TypeSystem.TypeName(type)));
            }
            else if (level > 0)
            {
                builder.Append('_');
            }
            else
            {
                Summarize(builder, obj, level);
            }

            if (TypeSystem.IsProperty(expr.Method))
            {
                // Special case: an indexer property
                builder.Append("[");
                for (int i = start, n = expr.Arguments.Count; i < n; i++)
                {
                    if (i > start) builder.Append(", ");
                    Summarize(builder, expr.Arguments[i], level);
                }
                builder.Append("]");
            }
            else
            {
                builder.Append(".");
                builder.Append(DryadLinqUtil.SimpleName(expr.Method.Name));
                builder.Append("(");
                for (int i = start, n = expr.Arguments.Count; i < n; i++)
                {
                    if (i > start) builder.Append(", ");
                    Summarize(builder, expr.Arguments[i], level);
                }
                builder.Append(")");
            }
        }

        private static string SummarizeType(string typename)
        {
            // drop common 'System.*' prefixes
            string result = Regex.Replace(typename, @"System[a-zA-Z\.]*\.([a-zA-Z]+)", "$1");
            return result;
        }

        private static void SummarizeNewExpression(StringBuilder builder, NewExpression expr, int level)
        {
            Type type = (expr.Constructor == null) ? expr.Type : expr.Constructor.DeclaringType;
            builder.Append("new ");
            builder.Append(SummarizeType(TypeSystem.TypeName(type)));

            int n = expr.Arguments.Count;
            builder.Append("(");
            for (int i = 0; i < n; i++)
            {
                if (i > 0) builder.Append(", ");
                Summarize(builder, expr.Arguments[i], level);
            }
            builder.Append(")");
        }

        private static void SummarizeNewArrayExpression(StringBuilder builder, NewArrayExpression expr, int level)
        {
            if (expr.NodeType == ExpressionType.NewArrayBounds)
            {
                builder.Append("new ");
                builder.Append(expr.Type.ToString());
                builder.Append("(");
                for (int i = 0, n = expr.Expressions.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    Summarize(builder, expr.Expressions[i], level);
                }
                builder.Append(")");
            }
            else
            {
                Debug.Assert(expr.NodeType == ExpressionType.NewArrayInit);
                builder.Append("new ");
                builder.Append("[] {");
                for (int i = 0, n = expr.Expressions.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    Summarize(builder, expr.Expressions[i], level);
                }
                builder.Append("}");
            }
        }

        private static void SummarizeMemberInitExpression(StringBuilder builder, MemberInitExpression expr, int level)
        {
            if (expr.NewExpression.Arguments.Count == 0 &&
                expr.NewExpression.Type.Name.Contains("<"))
            {
                // anonymous type constructor
                builder.Append("new");
            }
            else
            {
                Summarize(builder, expr.NewExpression, level);
            }
            builder.Append(" {");
            for (int i = 0, n = expr.Bindings.Count; i < n; i++)
            {
                if (i > 0) builder.Append(", ");
                SummarizeMemberBinding(builder, expr.Bindings[i], level);
            }
            builder.Append("}");
        }

        private static void SummarizeListInitExpression(StringBuilder builder, ListInitExpression expr, int level)
        {
            Summarize(builder, expr.NewExpression, level);
            builder.Append(" {");
            for (int i = 0, n = expr.Initializers.Count; i < n; i++)
            {
                if (i > 0) builder.Append(", ");
                SummarizeElementInit(builder, expr.Initializers[i], level);
            }
            builder.Append("}");
        }

        private static void SummarizeParameterExpression(StringBuilder builder, ParameterExpression expr)
        {
            if (expr.Name != null)
            {
                builder.Append(expr.Name);
            }
            else
            {
                builder.Append('_');
            }
        }

        private static void SummarizeTypeBinaryExpression(StringBuilder builder, TypeBinaryExpression expr, int level)
        {
            Debug.Assert(expr.NodeType == ExpressionType.TypeIs);
            builder.Append("(");
            Summarize(builder, expr.Expression, level);
            builder.Append(" is ");
            builder.Append(TypeSystem.TypeName(expr.TypeOperand));
            builder.Append(")");
        }

        private static void SummarizeUnaryExpression(StringBuilder builder, UnaryExpression expr, int level)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.ArrayLength:
                    {
                        Summarize(builder, expr.Operand, level);
                        builder.Append(".Length");
                        break;
                    }
                case ExpressionType.Convert:
                    {
                        // do not show type casts
                        Summarize(builder, expr.Operand, level);
                        break;
                    }
                case ExpressionType.TypeAs:
                    {
                        // do not show type casts
                        Summarize(builder, expr.Operand, level);
                        break;
                    }
                case ExpressionType.Not:
                    {
                        builder.Append("!(");
                        Summarize(builder, expr.Operand, level);
                        builder.Append(")");
                        break;
                    }
                case ExpressionType.Negate:
                    {
                        builder.Append("-(");
                        Summarize(builder, expr.Operand, level);
                        builder.Append(")");
                        break;
                    }
                case ExpressionType.Quote:
                    {
                        Summarize(builder, expr.Operand, level);
                        break;
                    }
                default:
                    {
                        builder.Append(expr.NodeType);
                        builder.Append("(");
                        Summarize(builder, expr.Operand, level);
                        builder.Append(")");
                        break;
                    }
            }
        }

        private static void SummarizeMemberBinding(StringBuilder builder, MemberBinding binding, int level)
        {
            if (binding is MemberAssignment)
            {
                builder.Append(binding.Member.Name);
                builder.Append(" = ");
                Summarize(builder, ((MemberAssignment)binding).Expression, level);
            }
            else if (binding is MemberMemberBinding)
            {
                builder.Append(binding.Member.Name);
                builder.Append(" = {");

                MemberMemberBinding mmBinding = (MemberMemberBinding)binding;
                for (int i = 0, n = mmBinding.Bindings.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    SummarizeMemberBinding(builder, mmBinding.Bindings[i], level);
                }
                builder.Append("}");
            }
            else
            {
                Debug.Assert(binding is MemberListBinding);
                builder.Append(binding.Member.Name);
                builder.Append(" = {");

                MemberListBinding mlBinding = (MemberListBinding)binding;
                for (int i = 0, n = mlBinding.Initializers.Count; i < n; i++)
                {
                    if (i > 0) builder.Append(", ");
                    SummarizeElementInit(builder, mlBinding.Initializers[i], level);
                }
                builder.Append("}");
            }
        }

        private static void SummarizeElementInit(StringBuilder builder, ElementInit elemInit, int level)
        {
            builder.Append(elemInit.AddMethod.Name);
            builder.Append("(");
            bool isFirst = true;
            foreach (Expression argument in elemInit.Arguments)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    builder.Append(",");
                }
                Summarize(builder, argument, level);
            }
            builder.Append(")");
        }
        #endregion        
    }
}
