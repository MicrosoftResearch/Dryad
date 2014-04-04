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
using System.Collections.ObjectModel;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    // This class implements a generic expression visitor. The code was stolen
    // from LINQ source code.
    internal abstract class ExpressionVisitor
    {
        internal ExpressionVisitor()
        {
        }

        internal virtual Expression Visit(Expression exp)
        {
            if (exp == null) return exp;
            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                {
                    return this.VisitUnary((UnaryExpression)exp);
                }
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                {
                    return this.VisitBinary((BinaryExpression)exp);
                }
                case ExpressionType.TypeIs:
                {
                    return this.VisitTypeIs((TypeBinaryExpression)exp);
                }
                case ExpressionType.Conditional:
                {
                    return this.VisitConditional((ConditionalExpression)exp);
                }
                case ExpressionType.Constant:
                {
                    return this.VisitConstant((ConstantExpression)exp);
                }
                case ExpressionType.Parameter:
                {
                    return this.VisitParameter((ParameterExpression)exp);
                }
                case ExpressionType.MemberAccess:
                {
                    return this.VisitMemberAccess((MemberExpression)exp);
                }
                case ExpressionType.Call:
                {
                    return this.VisitMethodCall((MethodCallExpression)exp);
                }
                case ExpressionType.Lambda:
                {
                    return this.VisitLambda((LambdaExpression)exp);
                }
                case ExpressionType.New:
                {
                    return this.VisitNew((NewExpression)exp);
                }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                {
                    return this.VisitNewArray((NewArrayExpression)exp);
                }
                case ExpressionType.Invoke:
                {
                    return this.VisitInvocation((InvocationExpression)exp);
                }
                case ExpressionType.MemberInit:
                {
                    return this.VisitMemberInit((MemberInitExpression)exp);
                }
                case ExpressionType.ListInit:
                {
                    return this.VisitListInit((ListInitExpression)exp);
                }
                default:
                {
                    throw new DryadLinqException(DryadLinqErrorCode.ExpressionTypeNotHandled,
                                                 String.Format(SR.ExpressionTypeNotHandled,
                                                               "ExpressionVisitor", exp.NodeType));
                }
            }
        }

        internal virtual MemberBinding VisitBinding(MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                {
                    return this.VisitMemberAssignment((MemberAssignment)binding);
                }
                case MemberBindingType.MemberBinding:
                {
                    return this.VisitMemberMemberBinding((MemberMemberBinding)binding);
                }
                case MemberBindingType.ListBinding:
                {
                    return this.VisitMemberListBinding((MemberListBinding)binding);
                }
                default:
                {
                    throw new DryadLinqException(DryadLinqErrorCode.ExpressionTypeNotHandled,
                                                 String.Format(SR.ExpressionTypeNotHandled,
                                                               "ExpressionVisitor", binding.BindingType));
                }
            }
        }

        internal virtual ElementInit VisitElementInitializer(ElementInit initializer)
        {
            ReadOnlyCollection<Expression> arguments = this.VisitExpressionList(initializer.Arguments);
            if (arguments != initializer.Arguments)
            {
                return Expression.ElementInit(initializer.AddMethod, arguments);
            }
            return initializer;
        }
        
        internal virtual Expression VisitUnary(UnaryExpression u)
        {
            Expression operand = this.Visit(u.Operand);
            if (operand != u.Operand)
            {
                return Expression.MakeUnary(u.NodeType, operand, u.Type, u.Method);
            }
            return u;
        }

        internal virtual Expression VisitBinary(BinaryExpression b)
        {
            Expression left = this.Visit(b.Left);
            Expression right = this.Visit(b.Right);
            if (left != b.Left || right != b.Right)
            {
                return Expression.MakeBinary(b.NodeType, left, right, b.IsLiftedToNull, b.Method);
            }
            return b;
        }

        internal virtual Expression VisitTypeIs(TypeBinaryExpression b)
        {
            Expression expr = this.Visit(b.Expression);
            if (expr != b.Expression)
            {
                return Expression.TypeIs(expr, b.TypeOperand);
            }
            return b;
        }

        internal virtual Expression VisitConstant(ConstantExpression c)
        {
            return c;
        }

        internal virtual Expression VisitConditional(ConditionalExpression c)
        {
            Expression test = this.Visit(c.Test);
            Expression ifTrue = this.Visit(c.IfTrue);
            Expression ifFalse = this.Visit(c.IfFalse);
            if (test != c.Test || ifTrue != c.IfTrue || ifFalse != c.IfFalse)
            {
                return Expression.Condition(test, ifTrue, ifFalse);
            }
            return c;
        }

        internal virtual Expression VisitParameter(ParameterExpression p)
        {
            return p;
        }

        internal virtual Expression VisitMemberAccess(MemberExpression m)
        {
            Expression exp = this.Visit(m.Expression);
            if (exp != m.Expression)
            {
                return Expression.MakeMemberAccess(exp, m.Member);
            }
            return m;
        }

        internal virtual Expression VisitMethodCall(MethodCallExpression m)
        {
            Expression obj = this.Visit(m.Object);
            IEnumerable<Expression> args = this.VisitExpressionList(m.Arguments);
            if (obj != m.Object || args != m.Arguments)
            {
                return Expression.Call(obj, m.Method, args);
            }
            return m;
        }

        internal virtual ReadOnlyCollection<Expression> VisitExpressionList(ReadOnlyCollection<Expression> original)
        {
            List<Expression> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                Expression p = this.Visit(original[i]);
                if (list != null)
                {
                    list.Add(p);
                }
                else if (p != original[i])
                {
                    list = new List<Expression>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(p);
                }
            }
            if (list != null)
            {
                return new ReadOnlyCollection<Expression>(list);
            }
            return original;
        }

        internal virtual MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            Expression e = this.Visit(assignment.Expression);
            if (e != assignment.Expression)
            {
                return Expression.Bind(assignment.Member, e);
            }
            return assignment;
        }

        internal virtual MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            IEnumerable<MemberBinding> bindings = this.VisitBindingList(binding.Bindings);
            if (bindings != binding.Bindings)
            {
                return Expression.MemberBind(binding.Member, bindings);
            }
            return binding;
        }

        internal virtual MemberListBinding VisitMemberListBinding(MemberListBinding binding)
        {
            IEnumerable<ElementInit> initializers = this.VisitElementInitializerList(binding.Initializers);
            if (initializers != binding.Initializers)
            {
                return Expression.ListBind(binding.Member, initializers);
            }
            return binding;
        }

        internal virtual IEnumerable<MemberBinding> VisitBindingList(ReadOnlyCollection<MemberBinding> original)
        {
            List<MemberBinding> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                MemberBinding b = this.VisitBinding(original[i]);
                if (list != null)
                {
                    list.Add(b);
                }
                else if (b != original[i])
                {
                    list = new List<MemberBinding>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(b);
                }
            }
            if (list != null)
            {
                return list;
            }
            return original;
        }

        internal virtual IEnumerable<ElementInit> VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
        {
            List<ElementInit> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                ElementInit init = this.VisitElementInitializer(original[i]);
                if (list != null)
                {
                    list.Add(init);
                }
                else if (init != original[i])
                {
                    list = new List<ElementInit>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(init);
                }
            }
            if (list != null)
            {
                return list;
            }
            return original;
        }
        
        internal virtual Expression VisitLambda(LambdaExpression lambda)
        {
            Expression body = this.Visit(lambda.Body);
            if (body != lambda.Body)
            {
                return Expression.Lambda(lambda.Type, body, lambda.Parameters);
            }
            return lambda;
        }

        internal virtual NewExpression VisitNew(NewExpression nex)
        {
            IEnumerable<Expression> args = this.VisitExpressionList(nex.Arguments);
            if (args != nex.Arguments)
            {
                return Expression.New(nex.Constructor, args);
            }
            return nex;
        }

        internal virtual Expression VisitMemberInit(MemberInitExpression init)
        {
            NewExpression n = this.VisitNew(init.NewExpression);
            IEnumerable<MemberBinding> bindings = this.VisitBindingList(init.Bindings);
            if (n != init.NewExpression || bindings != init.Bindings)
            {
                return Expression.MemberInit(n, bindings);
            }
            return init;
        }

        internal virtual Expression VisitListInit(ListInitExpression init)
        {
            NewExpression n = this.VisitNew(init.NewExpression);
            IEnumerable<ElementInit> initializers = this.VisitElementInitializerList(init.Initializers);
            if (n != init.NewExpression || initializers != init.Initializers)
            {
                return Expression.ListInit(n, initializers);
            }
            return init;
        }

        internal virtual Expression VisitNewArray(NewArrayExpression na)
        {
            IEnumerable<Expression> exprs = this.VisitExpressionList(na.Expressions);
            if (exprs != na.Expressions)
            {
                if (na.NodeType == ExpressionType.NewArrayInit)
                {
                    return Expression.NewArrayInit(na.Type.GetElementType(), exprs);
                }
                else
                {
                    return Expression.NewArrayBounds(na.Type.GetElementType(), exprs);
                }
            }
            return na;
        }

        internal virtual Expression VisitInvocation(InvocationExpression iv)
        {
            IEnumerable<Expression> args = this.VisitExpressionList(iv.Arguments);
            Expression expr = this.Visit(iv.Expression);
            if (args != iv.Arguments || expr != iv.Expression)
            {
                return Expression.Invoke(expr, args);
            }
            return iv;
        }
    }

    internal sealed class ParameterSubst : ExpressionVisitor
    {
        private ParameterExpression m_pexpr;
        private Expression m_aexpr;
        
        internal ParameterSubst(ParameterExpression pexpr, Expression aexpr)
        {
            this.m_pexpr = pexpr;
            this.m_aexpr = aexpr;
        }

        internal override Expression VisitParameter(ParameterExpression p)
        {
            return (p == this.m_pexpr) ? this.m_aexpr : p;
        }
    }

    internal sealed class ExpressionSubst : ExpressionVisitor
    {
        private Substitution m_paramSubst;
        private List<Expression> m_leftExprList;
        private List<Expression> m_rightExprList;
        
        internal ExpressionSubst(Substitution paramSubst)
        {
            this.m_paramSubst = paramSubst;
            this.m_leftExprList = new List<Expression>(2);
            this.m_rightExprList = new List<Expression>(2);
        }

        internal void AddSubst(Expression left, Expression right)
        {
            foreach (Expression expr in this.m_leftExprList)
            {
                if (ExpressionMatcher.MemberAccessSubsumes(expr, left))
                {
                    return;
                }
            }
            this.m_leftExprList.Add(left);
            this.m_rightExprList.Add(right);
        }

        internal override Expression Visit(Expression expr)
        {
            if (expr == null) return expr;
            for (int i = 0; i < this.m_leftExprList.Count; i++)
            {
                if (ExpressionMatcher.Match(expr, this.m_leftExprList[i], this.m_paramSubst))
                {
                    return this.m_rightExprList[i];
                }
            }
            return base.Visit(expr);
        }
    }

    internal sealed class CombinerSubst : ExpressionVisitor
    {
        private Expression m_expr;
        private ParameterExpression m_keyParam;
        private ParameterExpression m_groupParam;
        private Expression m_keyExpr;
        private Expression[] m_fromExprs;
        private Expression[] m_toExprs;
        private int m_idx = 0;

        internal CombinerSubst(LambdaExpression lambdaExpr,
                               ParameterExpression keyValueParam,
                               Expression[] fromExprs,
                               Expression[] toExprs)
        {
            this.m_expr = lambdaExpr.Body;
            if (lambdaExpr.Parameters.Count == 1)
            {
                this.m_keyParam = null;
                this.m_groupParam = lambdaExpr.Parameters[0];
            }
            else
            {
                this.m_keyParam = lambdaExpr.Parameters[0];
                this.m_groupParam = lambdaExpr.Parameters[1];
            }
            PropertyInfo keyPropInfo = keyValueParam.Type.GetProperty("Key");
            this.m_keyExpr = Expression.Property(keyValueParam, keyPropInfo);

            this.m_fromExprs = fromExprs;
            this.m_toExprs = toExprs;
            this.m_idx = 0;
        }

        internal Expression Visit()
        {
            return this.Visit(this.m_expr);
        }
        
        internal override Expression VisitMethodCall(MethodCallExpression mcExpr)
        {
            if (this.m_idx < this.m_fromExprs.Length &&
                this.m_fromExprs[this.m_idx] == mcExpr)
            {
                return this.m_toExprs[this.m_idx++];
            }            
            return base.VisitMethodCall(mcExpr);
        }

        internal override Expression VisitMemberAccess(MemberExpression m)
        {
            if (this.m_keyParam == null &&
                m.Expression == this.m_groupParam &&
                m.Member.Name == "Key")
            {
                return this.m_keyExpr;
            }
            return base.VisitMemberAccess(m);
        }

        internal override Expression VisitParameter(ParameterExpression p)
        {
            if (this.m_keyParam == p)
            {
                return this.m_keyExpr;
            }
            return base.VisitParameter(p);
        }
    }

    internal sealed class FreeParameters : ExpressionVisitor
    {
        private HashSet<ParameterExpression> freeParameters;

        internal FreeParameters()
        {
            this.freeParameters = new HashSet<ParameterExpression>();
        }

        internal HashSet<ParameterExpression> Parameters
        {
            get { return this.freeParameters; }
        }
        
        internal override Expression VisitParameter(ParameterExpression p)
        {
            this.freeParameters.Add(p);
            return p;
        }

        internal override Expression VisitLambda(LambdaExpression lambda)
        {
            Expression body = this.Visit(lambda.Body);
            foreach (ParameterExpression param in lambda.Parameters)
            {
                this.freeParameters.Remove(param);
            }
            return lambda;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{ ");
            bool isFirst = true;
            foreach (ParameterExpression p in this.freeParameters)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    sb.Append(",");
                }
                if (p.Name != null)
                {
                    sb.Append(p.Name);
                }
                else
                {
                    sb.Append("<param>");
                }
            }
            sb.Append(" }");
            return sb.ToString();
        }
    }

    internal sealed class ExpressionQuerySet : ExpressionVisitor
    {
        private HashSet<Expression> m_querySet;

        public ExpressionQuerySet()
        {
            this.m_querySet = new HashSet<Expression>();
        }

        internal HashSet<Expression> QuerySet
        {
            get { return this.m_querySet; }
        }
        
        internal override Expression Visit(Expression expr)
        {
            if (expr == null) return expr;
            if (DryadLinqExpression.IsConstant(expr))
            {
                object val = ExpressionSimplifier.Evaluate(expr);
                if (val is IQueryable)
                {
                    this.m_querySet.Add(((IQueryable)val).Expression);
                }
                return expr;
            }
            return base.Visit(expr);
        }
    }

    internal sealed class ReferencedQuerySubst : ExpressionVisitor
    {
        private Dictionary<Expression, QueryNodeInfo> m_referencedQueryMap;
        private int m_idx;
        private List<Pair<ParameterExpression, DLinqQueryNode>> m_referencedQueries;

        public ReferencedQuerySubst(Dictionary<Expression, QueryNodeInfo> referencedQueryMap)
        {
            this.m_referencedQueryMap = referencedQueryMap;
            this.m_idx = 0;
            this.m_referencedQueries = new List<Pair<ParameterExpression, DLinqQueryNode>>();
        }

        internal override Expression Visit(Expression expr)
        {
            if (expr == null) return expr;
            if (DryadLinqExpression.IsConstant(expr))
            {
                object val = ExpressionSimplifier.Evaluate(expr);
                if (val is IQueryable)
                {
                    QueryNodeInfo nodeInfo;
                    if (this.m_referencedQueryMap.TryGetValue(((IQueryable)val).Expression, out nodeInfo) &&
                        nodeInfo.QueryNode != null)
                    {
                        string name = "side__" + this.m_idx;
                        this.m_idx++;
                        ParameterExpression paramExpr = Expression.Parameter(expr.Type, name);
                        this.m_referencedQueries.Add(new Pair<ParameterExpression, DLinqQueryNode>(paramExpr, nodeInfo.QueryNode));
                        return paramExpr;
                    }
                    throw new DryadLinqException(DryadLinqErrorCode.UnhandledQuery,
                                                 String.Format(SR.UnhandledQuery, DryadLinqExpression.Summarize(expr)));
                }
                return expr;
            }
            return base.Visit(expr);
        }

        public List<Pair<ParameterExpression, DLinqQueryNode>> GetReferencedQueries()
        {
            return this.m_referencedQueries;
        }
    }

    internal sealed class ExpressionInfo : ExpressionVisitor
    {
        private bool m_isExpensive;
        private bool m_isStateful;

        public ExpressionInfo(Expression expr)
        {
            this.m_isExpensive = false;
            this.m_isStateful = false;
            this.Visit(expr);
        }

        internal override Expression VisitMethodCall(MethodCallExpression mcExpr)
        {
            Attribute resourceAttrib = AttributeSystem.GetAttrib(mcExpr, typeof(ResourceAttribute));
            if (resourceAttrib == null)
            {
                this.m_isExpensive = true;
                this.m_isStateful = true;
            }
            else
            {
                this.m_isExpensive = this.m_isExpensive || ((ResourceAttribute)resourceAttrib).IsExpensive;
                this.m_isStateful = this.m_isStateful || ((ResourceAttribute)resourceAttrib).IsStateful;
            }
            return mcExpr;
        }

        internal override Expression VisitBinary(BinaryExpression b)
        {
            if (b.Method == null)
            {
                return base.VisitBinary(b);
            }
            else
            {
                Attribute resourceAttrib = AttributeSystem.GetAttrib(b, typeof(ResourceAttribute));
                if (resourceAttrib == null)
                {
                    this.m_isExpensive = true;
                    this.m_isStateful = true;
                }
                else
                {
                    this.m_isExpensive = this.m_isExpensive || ((ResourceAttribute)resourceAttrib).IsExpensive;
                    this.m_isStateful = this.m_isStateful || ((ResourceAttribute)resourceAttrib).IsStateful;
                }
                return b;
            }
        }

        internal override Expression VisitUnary(UnaryExpression u)
        {
            if (u.Method == null)
            {
                return base.VisitUnary(u);
            }
            else
            {
                Attribute resourceAttrib = AttributeSystem.GetAttrib(u, typeof(ResourceAttribute));
                if (resourceAttrib == null)
                {
                    this.m_isExpensive = true;
                    this.m_isStateful = true;
                }
                else
                {
                    this.m_isExpensive = this.m_isExpensive || ((ResourceAttribute)resourceAttrib).IsExpensive;
                    this.m_isStateful = this.m_isStateful || ((ResourceAttribute)resourceAttrib).IsStateful;
                }
                return u;
            }
        }

        public bool IsExpensive
        {
            get { return this.m_isExpensive; }
        }
    }
}
