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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    // This class implements an expression matcher. This is useful for
    // many interesting static analysis. Again, it would be nice if this
    // is a functionality provided by C# and LINQ. But unfortunately it
    // is not.
    internal class ExpressionMatcher
    {
        public static bool Match(Expression e1, Expression e2)
        {
            return Match(e1, e2, Substitution.Empty);
        }

        // return true if e1 subsumes e2 in terms of member access
        internal static bool MemberAccessSubsumes(Expression e1, Expression e2)
        {
            if (Match(e1, e2)) return true;
            Expression e = e2;
            while (e is MemberExpression)
            {
                e = ((MemberExpression)e).Expression;
                if (Match(e1, e)) return true;
            }
            return false;
        }

        internal static bool Match(Expression e1, Expression e2, Substitution subst)
        {
            if (!e1.Type.Equals(e2.Type)) return false;
            
            if (e1 is BinaryExpression)
            {
                return ((e2 is BinaryExpression) &&
                        MatchBinary((BinaryExpression)e1, (BinaryExpression)e2, subst));
            }
            else if (e1 is ConditionalExpression)
            {
                return ((e2 is ConditionalExpression) &&
                        MatchConditional((ConditionalExpression)e1, (ConditionalExpression)e2, subst));
            }
            else if (e1 is ConstantExpression)
            {
                return ((e2 is ConstantExpression) &&
                        MatchConstant((ConstantExpression)e1, (ConstantExpression)e2, subst));
            }
            else if (e1 is InvocationExpression)
            {
                return ((e2 is InvocationExpression) &&
                        MatchInvocation((InvocationExpression)e1, (InvocationExpression)e2, subst));
            }
            else if (e1 is LambdaExpression)
            {
                return ((e2 is LambdaExpression) &&
                        MatchLambda((LambdaExpression)e1, (LambdaExpression)e2, subst));
            }
            else if (e1 is MemberExpression)
            {
                return ((e2 is MemberExpression) &&
                        MatchMember((MemberExpression)e1, (MemberExpression)e2, subst));
            }
            else if (e1 is MethodCallExpression)
            {
                return ((e2 is MethodCallExpression) &&
                        MatchMethodCall((MethodCallExpression)e1, (MethodCallExpression)e2, subst));
            }
            else if (e1 is NewExpression)
            {
                return ((e2 is NewExpression) &&
                        MatchNew((NewExpression)e1, (NewExpression)e2, subst));
            }
            else if (e1 is NewArrayExpression)
            {
                return ((e2 is NewArrayExpression) &&
                        MatchNewArray((NewArrayExpression)e1, (NewArrayExpression)e2, subst));
            }
            else if (e1 is MemberInitExpression)
            {
                return ((e2 is MemberInitExpression) &&
                        MatchMemberInit((MemberInitExpression)e1, (MemberInitExpression)e2, subst));
            }
            else if (e1 is ListInitExpression)
            {
                return ((e2 is ListInitExpression) &&
                        MatchListInit((ListInitExpression)e1, (ListInitExpression)e2, subst));
            }
            else if (e1 is ParameterExpression)
            {
                return ((e2 is ParameterExpression) &&
                        MatchParameter((ParameterExpression)e1, (ParameterExpression)e2, subst));
            }
            else if (e1 is TypeBinaryExpression)
            {
                return ((e2 is TypeBinaryExpression) &&
                        MatchTypeBinary((TypeBinaryExpression)e1, (TypeBinaryExpression)e2, subst));
            }
            else if (e1 is UnaryExpression)
            {
                return ((e2 is UnaryExpression) &&
                        MatchUnary((UnaryExpression)e1, (UnaryExpression)e2, subst));
            }

            throw new DryadLinqException(HpcLinqErrorCode.ExpressionTypeNotHandled,
                                       String.Format(SR.ExpressionTypeNotHandled,
                                                     "ExpressionMatcher", e1.NodeType));
        }

        private static bool MatchInvocation(InvocationExpression e1,
                                            InvocationExpression e2,
                                            Substitution subst)
        {
            ReadOnlyCollection<Expression> args1 = e1.Arguments;
            ReadOnlyCollection<Expression> args2 = e2.Arguments;
            if (!Match(e1.Expression, e2.Expression, subst) || args1.Count != args2.Count)
            {
                return false;
            }
            for (int i = 0; i < args1.Count; i++)
            {
                if (!Match(args1[i], args2[i], subst))
                {
                    return false;
                }
            }
            return true;
        }

        private static bool MatchBinary(BinaryExpression e1,
                                        BinaryExpression e2,
                                        Substitution subst)
        {
            return (e1.NodeType == e2.NodeType &&
                    Match(e1.Left, e2.Left, subst) &&
                    Match(e1.Right, e2.Right, subst));
        }

        private static bool MatchConditional(ConditionalExpression e1,
                                             ConditionalExpression e2,
                                             Substitution subst)
        {
            return (Match(e1.Test, e2.Test, subst) &&
                    Match(e1.IfTrue, e2.IfTrue, subst) &&
                    Match(e1.IfFalse, e2.IfFalse, subst));
        }

        private static bool MatchConstant(ConstantExpression e1,
                                          ConstantExpression e2,
                                          Substitution subst)
        {
            if (e1.Value == null)
            {
                return (e2.Value == null);
            }
            else
            {
                return e1.Value.Equals(e2.Value);
            }
        }

        private static bool MatchLambda(LambdaExpression e1,
                                        LambdaExpression e2,
                                        Substitution subst)
        {
            if (e1.Parameters.Count != e2.Parameters.Count)
            {
                return false;
            }
            Substitution subst1 = subst;
            for (int i = 0, n = e1.Parameters.Count; i < n; i++)
            {
                if (!e1.Parameters[i].Equals(e2.Parameters[i]))
                {
                    subst1 = subst1.Cons(e1.Parameters[i], e2.Parameters[i]);
                }
            }
            return Match(e1.Body, e2.Body, subst1);
        }

        private static bool MatchMember(MemberExpression e1,
                                        MemberExpression e2,
                                        Substitution subst)
        {
            if (e1.Expression == null)
            {
                if (e2.Expression != null) return false;
            }
            else
            {
                if (e2.Expression == null ||
                    !Match(e1.Expression, e2.Expression, subst))
                {
                    return false;
                }
            }
            return e1.Member.Equals(e2.Member);
        }

        private static bool MatchMethodCall(MethodCallExpression e1,
                                            MethodCallExpression e2,
                                            Substitution subst)
        {
            if (e1.Method != e2.Method) return false;

            if (e1.Object == null || e2.Object == null)
            {
                if (e1.Object != e2.Object) return false;
            }
            else if (!Match(e1.Object, e2.Object, subst) ||
                     e1.Arguments.Count != e2.Arguments.Count)
            {
                return false;
            }
            for (int i = 0, n = e1.Arguments.Count; i < n; i++)
            {
                if (!Match(e1.Arguments[i], e2.Arguments[i], subst))
                {
                    return false;
                }
            }
            return true;
        }

        private static bool MatchNew(NewExpression e1, NewExpression e2, Substitution subst)
        {
            if (e1.Arguments.Count != e2.Arguments.Count)
            {
                return false;
            }
            for (int i = 0, n = e1.Arguments.Count; i < n; i++)
            {
                if (!Match(e1.Arguments[i], e2.Arguments[i], subst))
                {
                    return false;
                }
            }
            return true;
        }

        public static bool MatchNewArray(NewArrayExpression e1,
                                         NewArrayExpression e2,
                                         Substitution subst)
        {
            if (e1.NodeType != e2.NodeType ||
                e1.Expressions.Count != e2.Expressions.Count)
            {
                return false;
            }
            for (int i = 0, n = e1.Expressions.Count; i < n; i++)
            {
                if (!Match(e1.Expressions[i], e2.Expressions[i], subst))
                {
                    return false;
                }
            }
            return true;
        }

        public static bool MatchMemberInit(MemberInitExpression e1,
                                           MemberInitExpression e2,
                                           Substitution subst)
        {
            if (!Match(e1.NewExpression, e2.NewExpression, subst) ||
                e1.Bindings.Count != e2.Bindings.Count)
            {
                return false;
            }
            for (int i = 0, n = e1.Bindings.Count; i < n; i++)
	    {
                if (!MatchMemberBinding(e1.Bindings[i], e2.Bindings[i], subst))
                {
                    return false;
                }
            }
            return true;
        }

        public static bool MatchListInit(ListInitExpression e1,
                                         ListInitExpression e2,
                                         Substitution subst)
        {
            if (!Match(e1.NewExpression, e2.NewExpression, subst))
            {
                return false;
            }
            if (e1.Initializers.Count != e2.Initializers.Count)
            {
                return false;
            }
            for (int i = 0, n = e1.Initializers.Count; i < n; i++)
	    {
                ElementInit init1 = e1.Initializers[i];
                ElementInit init2 = e2.Initializers[i];
                if (!MatchElementInit(init1, init2, subst))
                {
                    return false;
                }
            }
            return true;
        }

        public static bool MatchParameter(ParameterExpression e1,
                                          ParameterExpression e2,
                                          Substitution subst)
        {
            if (e1.Equals(e2)) return true;
            ParameterExpression e = subst.Find(e1);
            return (e != null && e.Equals(e2));
        }

        public static bool MatchTypeBinary(TypeBinaryExpression e1,
                                           TypeBinaryExpression e2,
                                           Substitution subst)
        {
            return (e1.NodeType == ExpressionType.TypeIs &&
                    e2.NodeType == ExpressionType.TypeIs &&
                    e1.TypeOperand.Equals(e2.TypeOperand) &&
                    Match(e1.Expression, e2.Expression, subst));
        }

        public static bool MatchUnary(UnaryExpression e1, UnaryExpression e2, Substitution subst)
        {
            return (e1.NodeType == e2.NodeType &&
                    Match(e1.Operand, e2.Operand, subst));
        }

        private static bool MatchMemberBinding(MemberBinding b1, MemberBinding b2, Substitution subst)
        {
            if (b1.BindingType != b2.BindingType ||
                !b1.Member.Equals(b2.Member))
            {
                return false;
            }
            if (b1 is MemberAssignment)
            {
                return Match(((MemberAssignment)b1).Expression,
                             ((MemberAssignment)b2).Expression,
                             subst);
            }
            else if (b1 is MemberMemberBinding)
            {
                MemberMemberBinding mmb1 = (MemberMemberBinding)b1;
                MemberMemberBinding mmb2 = (MemberMemberBinding)b2;
                if (mmb1.Bindings.Count != mmb2.Bindings.Count)
                {
                    return false;
                }
                for (int i = 0, n = mmb1.Bindings.Count; i < n; i++)
                {
                    if (!MatchMemberBinding(mmb1.Bindings[i], mmb2.Bindings[i], subst))
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                MemberListBinding mlb1 = (MemberListBinding)b1;
                MemberListBinding mlb2 = (MemberListBinding)b2;
                if (mlb1.Initializers.Count != mlb2.Initializers.Count)
                {
                    return false;
                }
                for (int i = 0, n = mlb1.Initializers.Count; i < n; i++)
                {
                    if (!MatchElementInit(mlb1.Initializers[i], mlb2.Initializers[i], subst))
                    {
                        return false;
                    }
                }
                return true;
            }
        }

        private static bool MatchElementInit(ElementInit init1,
                                             ElementInit init2,
                                             Substitution subst)
        {
            if (init1.AddMethod != init2.AddMethod ||
                init1.Arguments.Count != init2.Arguments.Count)
            {
                return false;
            }
            for (int i = 0; i < init1.Arguments.Count; i++)
            {
                if (!Match(init1.Arguments[i], init2.Arguments[i], subst))
                {
                    return false;
                }
            }
            return true;
        }
    }

    internal class Substitution
    {
        private ParameterExpression x;
        private ParameterExpression y;
        private Substitution next;

        public static Substitution Empty = new Substitution(null, null, null);
        
        private Substitution(ParameterExpression x, ParameterExpression y, Substitution s)
        {
            this.x = x;
            this.y = y;
            this.next = s;
        }

        public Substitution Cons(ParameterExpression a, ParameterExpression b)
        {
            return new Substitution(a, b, this);
        }

        public ParameterExpression Find(ParameterExpression a)
        {
            Substitution curSubst = this;
            while (curSubst != Empty)
            {
                if (curSubst.x.Equals(a)) return y;
                curSubst = curSubst.next;
            }
            return null;
        }
    }
}
