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
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    // The information we know about the dataset at each stage of the
    // computation. For each operator, we try to compute this from the
    // DataSetInfo of its input datasets and the semantics of the operator.
    [Serializable]
    internal class DataSetInfo
    {
        internal static PartitionInfo OnePartition = new RandomPartition(1);
        internal static OrderByInfo NoOrderBy = new OrderByInfo();
        internal static DistinctInfo NoDistinct = new DistinctInfo();

        internal PartitionInfo partitionInfo;
        internal OrderByInfo orderByInfo;
        internal DistinctInfo distinctInfo;

        internal DataSetInfo()
        {
            this.partitionInfo = OnePartition;
            this.orderByInfo = NoOrderBy;
            this.distinctInfo = NoDistinct;
        }

        internal DataSetInfo(PartitionInfo pinfo, OrderByInfo oinfo, DistinctInfo dinfo)
        {
            this.partitionInfo = pinfo;
            this.orderByInfo = oinfo;
            this.distinctInfo = dinfo;
        }

        internal DataSetInfo(DataSetInfo info)
        {
            this.partitionInfo = info.partitionInfo;
            this.orderByInfo = info.orderByInfo;
            this.distinctInfo = info.distinctInfo;
        }

        // Return true iff the entire dataset is ordered.
        internal bool IsOrderedBy(LambdaExpression keySel, object comparer)
        {
            return (this.partitionInfo.ParType == PartitionType.Range &&
                    this.partitionInfo.IsPartitionedBy(keySel, comparer) &&
                    this.orderByInfo.IsOrderedBy(keySel, comparer) &&
                    this.orderByInfo.IsSameMonotoncity(this.partitionInfo));
        }

        internal static DataSetInfo Read(Stream fstream)
        {
            BinaryFormatter bfm = new BinaryFormatter();
            return (DataSetInfo)bfm.Deserialize(fstream);
        }

        internal static void Write(DataSetInfo dsInfo, Stream fstream)
        {
            BinaryFormatter bfm = new BinaryFormatter();
            bfm.Serialize(fstream, dsInfo);
        }
    }

    internal enum PartitionType
    {
        Random      = 0x0000,
        Hash        = 0x0001,
        Range       = 0x0002,
        HashOrRange = 0x0003
    }

    internal abstract class PartitionInfo
    {
        private PartitionType m_partitionType;

        protected PartitionInfo(PartitionType parType)
        {
            this.m_partitionType = parType;
        }

        internal PartitionType ParType
        {
            get { return this.m_partitionType; }
        }

        internal virtual bool IsDescending
        {
            get {
                throw new InvalidOperationException();
            }
        }

        internal virtual bool HasKeys
        {
            get {
                throw new InvalidOperationException();
            }
        }

        internal virtual PartitionInfo Concat(PartitionInfo p)
        {
            return new RandomPartition(this.Count + p.Count);
        }

        internal abstract int Count { get; set; }
        internal abstract bool IsPartitionedBy(LambdaExpression keySel);
        internal abstract bool IsPartitionedBy(LambdaExpression keySel, object comparer);
        internal abstract bool IsPartitionedBy(LambdaExpression keySel, object comparer, bool isDescending);
        internal abstract bool IsSamePartition(PartitionInfo p);
        internal abstract DLinqQueryNode CreatePartitionNode(LambdaExpression keySelector, DLinqQueryNode child);
        internal abstract PartitionInfo Create(LambdaExpression keySel);
        internal abstract PartitionInfo Rewrite(LambdaExpression keySel, ParameterExpression param);

        internal static PartitionInfo CreateHash(LambdaExpression keySel, int count, object comparer, Type keyType)
        {
            Type hashType = typeof(HashPartition<>).MakeGenericType(keyType);
            object[] args = new object[] { keySel, count, comparer };
            return (PartitionInfo)Activator.CreateInstance(hashType, BindingFlags.NonPublic | BindingFlags.Instance, null ,args, null);
        }

        internal static PartitionInfo
            CreateRange(LambdaExpression keySel, object keys, object comparer, bool? isDescending, Int32 parCnt, Type keyType)
        {
            Type parType = typeof(RangePartition<>).MakeGenericType(keyType);
            object[] args = new object[] { keySel, keys, comparer, isDescending, parCnt };
            try
            {
                return (PartitionInfo)Activator.CreateInstance(parType, BindingFlags.NonPublic | BindingFlags.Instance, null, args, null);
            }
            catch (TargetInvocationException tie)
            {
                // The ctor for RangePartition<> can throw.. we trap and rethrow the useful exception here.
                if (tie.InnerException != null)
                    throw tie.InnerException;
                else
                    throw;
            }
        }

        internal virtual Pair<MethodInfo, Expression[]> GetOperator()
        {
            throw new InvalidOperationException();
        }
    }

    internal class RandomPartition : PartitionInfo
    {
        private int m_count;

        internal RandomPartition(int count)
            : base(PartitionType.Random)
        {
            this.m_count = count;
        }

        internal override int Count
        {
            get { return this.m_count; }
            set { this.m_count = value; }
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel)
        {
            return false;
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comparer)
        {
            return false;
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comparer, bool isDescending)
        {
            return false;
        }

        internal override bool IsSamePartition(PartitionInfo p)
        {
            return false;
        }

        internal override DLinqQueryNode CreatePartitionNode(LambdaExpression keySel, DLinqQueryNode child)
        {
            throw new DryadLinqException(DryadLinqErrorCode.CannotCreatePartitionNodeRandom,
                                         SR.CannotCreatePartitionNodeRandom);
        }

        internal override PartitionInfo Create(LambdaExpression keySel)
        {
            return this;
        }

        internal override PartitionInfo Rewrite(LambdaExpression resultSel, ParameterExpression param)
        {
            return this;
        }
    }

    internal class RangePartition<TKey> : PartitionInfo 
    {
        private int m_count;
        private LambdaExpression m_keySelector;
        private TKey[] m_partitionKeys;
        private IComparer<TKey> m_comparer;
        private bool m_isDescending; 

        internal RangePartition(LambdaExpression keySelector, TKey[] partitionKeys, IComparer<TKey> comparer)
            : this(keySelector, partitionKeys, comparer, null, 1)
        {
        }

        internal RangePartition(LambdaExpression keySelector,
                                TKey[] partitionKeys,
                                IComparer<TKey> comparer,
                                bool? isDescending,
                                Int32 parCnt)
            : base(PartitionType.Range)
        {
            this.m_count = (partitionKeys == null) ? parCnt : (partitionKeys.Length + 1);
            this.m_keySelector = keySelector;
            this.m_partitionKeys = partitionKeys;
            this.m_comparer = TypeSystem.GetComparer<TKey>(comparer);
            if (isDescending == null)
            {
                if (partitionKeys == null)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.PartitionKeysNotProvided,
                                                 SR.PartitionKeysNotProvided);
                }

                bool? detectedIsDescending;
                if (!DryadLinqUtil.ComputeIsDescending(partitionKeys, m_comparer, out detectedIsDescending))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.PartitionKeysAreNotConsistentlyOrdered,
                                                 SR.PartitionKeysAreNotConsistentlyOrdered);
                }

                this.m_isDescending = detectedIsDescending ?? false;
            }
            else
            {
                this.m_isDescending = isDescending.GetValueOrDefault();
                if (partitionKeys != null &&
                    !DryadLinqUtil.IsOrdered(partitionKeys, this.m_comparer, this.m_isDescending))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.IsDescendingIsInconsistent,
                                                 SR.IsDescendingIsInconsistent);
                }
            }
        }

        internal RangePartition(LambdaExpression keySelector,
                                IComparer<TKey> comparer,
                                bool isDescending,
                                Int32 parCnt)
            : base(PartitionType.Range)
        {
            this.m_count = parCnt;
            this.m_keySelector = keySelector;
            this.m_partitionKeys = null;
            this.m_comparer = TypeSystem.GetComparer<TKey>(comparer);
        }
        
        internal TKey[] Keys
        {
            get { return this.m_partitionKeys; }
        }

        internal Expression KeysExpression
        {
            get {
                return Expression.Constant(this.m_partitionKeys);
            }
        }

        internal Expression KeySelector
        {
            get { return this.m_keySelector; }
        }

        internal IComparer<TKey> Comparer
        {
            get { return this.m_comparer; }
        }

        internal override bool IsDescending
        {
            get { return this.m_isDescending; }
        }

        internal override bool HasKeys
        {
            get { return this.m_partitionKeys != null; }
        }

        internal override int Count
        {
            get { return this.m_count; }
            set { this.m_count = value; }
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel)
        {
            // Match the key selector functions:
            if (this.m_keySelector == null)
            {
                return (keySel == null);
            }
            if (keySel == null) return false;
            return ExpressionMatcher.Match(this.m_keySelector, keySel);
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comp)
        {
            // Match the key selector functions:
            if (!this.IsPartitionedBy(keySel))
            {
                return false;
            }

            // Check the comparers:
            IComparer<TKey> comp1 = TypeSystem.GetComparer<TKey>(comp);
            if (comp1 == null) return false;
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comp, bool isDescending)
        {
            // Match the key selector functions:
            if (!this.IsPartitionedBy(keySel))
            {
                return false;
            }

            // Check the comparers:
            IComparer<TKey> comp1 = TypeSystem.GetComparer<TKey>(comp);
            if (comp1 == null) return false;
            if (this.m_isDescending != isDescending)
            {
                comp1 = new MinusComparer<TKey>(comp1);
            }
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsSamePartition(PartitionInfo p)
        {
            RangePartition<TKey> p1 =  p as RangePartition<TKey>;
            if (p1 == null) return false;

            // Check the keys:
            if (this.Keys == null ||
                p1.Keys == null || 
                this.Keys.Length != p1.Keys.Length)
            {
                return false;
            }

            IComparer<TKey> comp1 = TypeSystem.GetComparer<TKey>(p1.m_comparer);
            if (comp1 == null) return false;
            if (this.IsDescending != p1.IsDescending)
            {
                comp1 = new MinusComparer<TKey>(comp1);
            }
            for (int i = 0; i < this.Keys.Length; i++)
            {
                if (this.m_comparer.Compare(this.Keys[i], p1.Keys[i]) != 0)
                {
                    return false;
                }
            }

            // Check the comparers:
            return this.m_comparer.Equals(p1.m_comparer);
        }

        internal override DLinqQueryNode CreatePartitionNode(LambdaExpression keySel, DLinqQueryNode child)
        {
            Expression keysExpr = Expression.Constant(this.m_partitionKeys);
            Expression comparerExpr = Expression.Constant(this.m_comparer, typeof(IComparer<TKey>));
            Expression isDescendingExpr = Expression.Constant(this.m_isDescending);
            return new DLinqRangePartitionNode(keySel, null, keysExpr, comparerExpr, isDescendingExpr, null, child.QueryExpression, child);
        }

        internal override PartitionInfo Create(LambdaExpression keySel)
        {
            Type keyType = keySel.Body.Type;
            return PartitionInfo.CreateRange(keySel, this.Keys, this.m_comparer, this.m_isDescending, this.Count, keyType);
        }

        internal override PartitionInfo Rewrite(LambdaExpression resultSel, ParameterExpression param)
        {
            ParameterExpression a = this.m_keySelector.Parameters[0];
            Substitution pSubst = Substitution.Empty.Cons(a, param);
            LambdaExpression newKeySel = DryadLinqExpression.Rewrite(this.m_keySelector, resultSel, pSubst);
            if (newKeySel == null)
            {
                return new RandomPartition(this.m_count);
            }
            return this.Create(newKeySel);
        }

        internal override Pair<MethodInfo, Expression[]> GetOperator()
        {
            Type sourceType = this.m_keySelector.Parameters[0].Type;
            MethodInfo operation = TypeSystem.FindStaticMethod(
                                       typeof(Microsoft.Research.DryadLinq.DryadLinqQueryable),
                                       "RangePartition",
                                       new Type[] { typeof(IQueryable<>).MakeGenericType(sourceType),
                                                    m_keySelector.GetType(), 
                                                    m_partitionKeys.GetType(),
                                                    m_comparer.GetType(),
                                                    typeof(bool) },
                                       new Type[] { sourceType, typeof(TKey) });            
            Expression[] arguments = new Expression[] {
                this.m_keySelector,
                Expression.Constant(this.m_partitionKeys),
                Expression.Constant(this.m_comparer, typeof(IComparer<TKey>)),
                Expression.Constant(this.m_isDescending) };

            return new Pair<MethodInfo, Expression[]>(operation, arguments);
        }
    }

    internal class HashPartition<TKey> : PartitionInfo 
    {
        private int m_count;
        private LambdaExpression m_keySelector;
        private IEqualityComparer<TKey> m_comparer;

        internal HashPartition(LambdaExpression keySelector, int count)
            : this(keySelector, count, null)
        {
        }

        internal HashPartition(LambdaExpression keySelector, int count, IEqualityComparer<TKey> eqComparer)                             
            : base(PartitionType.Hash)
        {
            this.m_count = count;
            this.m_keySelector = keySelector;
            this.m_comparer = (eqComparer == null) ? EqualityComparer<TKey>.Default : eqComparer;
        }

        internal Expression KeySelector
        {
            get { return this.m_keySelector; }
        }

        internal IEqualityComparer<TKey> EqualityComparer
        {
            get { return this.m_comparer; }
        }

        internal override int Count
        {
            get { return this.m_count; }
            set { this.m_count = value; }
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel)
        {
            // Match the key selector functions:
            if (this.m_keySelector == null)
            {
                return (keySel == null);
            }
            if (keySel == null) return false;
            return ExpressionMatcher.Match(this.m_keySelector, keySel);
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comp)
        {
            // Match the key selector functions:
            if (!this.IsPartitionedBy(keySel))
            {
                return false;
            }

            // Check the comparers:
            IEqualityComparer<TKey> comp1 = TypeSystem.GetEqualityComparer<TKey>(comp);
            if (comp1 == null) return false;
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsPartitionedBy(LambdaExpression keySel, object comparer, bool isDescending)
        {
            return this.IsPartitionedBy(keySel, comparer);
        }

        internal override bool IsSamePartition(PartitionInfo p)
        {
            HashPartition<TKey> p1 = p as HashPartition<TKey>;
            if (p1 == null || this.Count != p1.Count)
            {
                return false;
            }
            // Check the comparers:
            return this.m_comparer.Equals(p1.m_comparer);
        }

        internal override DLinqQueryNode CreatePartitionNode(LambdaExpression keySel, DLinqQueryNode child)
        {
            Expression comparerExpr = Expression.Constant(this.m_comparer, typeof(IEqualityComparer<TKey>));
            return new DLinqHashPartitionNode(keySel, comparerExpr, this.Count, child.QueryExpression, child);
        }

        internal override PartitionInfo Create(LambdaExpression keySel)
        {
            Type keyType = keySel.Body.Type;
            return PartitionInfo.CreateHash(keySel, this.Count, this.m_comparer, keyType);
        }

        internal override PartitionInfo Rewrite(LambdaExpression resultSel, ParameterExpression param)
        {
            ParameterExpression a = this.m_keySelector.Parameters[0];
            Substitution pSubst = Substitution.Empty.Cons(a, param);
            LambdaExpression newKeySel = DryadLinqExpression.Rewrite(this.m_keySelector, resultSel, pSubst);
            if (newKeySel == null)
            {
                return new RandomPartition(this.m_count);
            }
            return this.Create(newKeySel);
        }

        internal override Pair<MethodInfo, Expression[]> GetOperator()
        {
            Type sourceType = this.m_keySelector.Parameters[0].Type;
            MethodInfo operation = TypeSystem.FindStaticMethod(
                                       typeof(Microsoft.Research.DryadLinq.DryadLinqQueryable),
                                       "HashPartition",
                                       new Type[] { typeof(IQueryable<>).MakeGenericType(sourceType),
                                                    m_keySelector.GetType(),
                                                    m_comparer.GetType(),
                                                    typeof(int) },
                                       new Type[] { sourceType, typeof(TKey) });

            Expression[] arguments = new Expression[] {
                    m_keySelector,
                    Expression.Constant(this.m_comparer, typeof(IEqualityComparer<TKey>)),
                    Expression.Constant(this.Count) };

            return new Pair<MethodInfo, Expression[]>(operation, arguments);
        }
    }

    internal class OrderByInfo
    {
        internal virtual bool IsOrdered
        {
            get { return false; }
        }

        internal virtual LambdaExpression KeySelector
        {
            get { return null; }
        }

        internal virtual Expression Comparer
        {
            get { return null; }
        }

        internal virtual bool IsDescending
        {
            get { return false; }
        }

        internal virtual bool IsOrderedBy(LambdaExpression keySel)
        {
            return false;
        }

        internal virtual bool IsOrderedBy(LambdaExpression keySel, object comparer)
        {
            return false;
        }

        internal virtual bool IsOrderedBy(LambdaExpression keySel, object comparer, bool isDescending)
        {
            return false;
        }

        internal virtual bool IsSameMonotoncity(PartitionInfo pinfo)
        {
            return false;
        }

        internal static OrderByInfo Create(Expression keySel, object comparer, bool isDescending, Type keyType)
        {
            Type infoType = typeof(OrderByInfo<>).MakeGenericType(keyType);
            object[] args = new object[] { keySel, comparer, isDescending };
            return (OrderByInfo)Activator.CreateInstance(infoType, BindingFlags.NonPublic | BindingFlags.Instance, null, args, null);
        }

        internal virtual OrderByInfo Create(LambdaExpression keySel)
        {
            return DataSetInfo.NoOrderBy;
        }

        internal virtual OrderByInfo Rewrite(LambdaExpression resultSel, ParameterExpression param)
        {
            return DataSetInfo.NoOrderBy;
        }
    }
    
    internal class OrderByInfo<TKey> : OrderByInfo
    {
        private LambdaExpression m_keySelector;
        private IComparer<TKey> m_comparer;
        private bool m_isDescending;

        internal OrderByInfo(LambdaExpression keySelector, IComparer<TKey> comparer, bool isDescending)
        {
            this.m_keySelector = keySelector;
            this.m_comparer = TypeSystem.GetComparer<TKey>(comparer);
            this.m_isDescending = isDescending;
        }

        internal override LambdaExpression KeySelector
        {
            get { return this.m_keySelector; }
        }

        internal override Expression Comparer
        {
            get {
                return Expression.Constant(this.m_comparer, typeof(IComparer<TKey>));
            }
        }

        internal override bool IsDescending
        {
            get { return this.m_isDescending; }
        }

        internal override bool IsOrdered
        {
            get { return true; }
        }

        internal override bool IsOrderedBy(LambdaExpression keySel)
        {
            if (this.m_keySelector == null)
            {
                return (keySel == null);
            }
            if (keySel == null) return false;
            return ExpressionMatcher.Match(this.m_keySelector, keySel);
        }

        internal override bool IsOrderedBy(LambdaExpression keySel, object comp)
        {
            // Match the key selector functions:
            if (!this.IsOrderedBy(keySel))
            {
                return false;
            }

            // Check the comparers:
            IComparer<TKey> comp1 = TypeSystem.GetComparer<TKey>(comp);
            if (comp1 == null) return false;
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsOrderedBy(LambdaExpression keySel, object comp, bool isDescending)
        {
            // Match the key selector functions:
            if (!this.IsOrderedBy(keySel))
            {
                return false;
            }

            // Check the comparers:
            IComparer<TKey> comp1 = TypeSystem.GetComparer<TKey>(comp);
            if (comp1 == null) return false;
            if (this.IsDescending != isDescending)
            {
                comp1 = new MinusComparer<TKey>(comp1);
            }
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsSameMonotoncity(PartitionInfo pinfo)
        {
            RangePartition<TKey> pinfo1 = pinfo as RangePartition<TKey>;
            if (pinfo1 == null) return false;

            IComparer<TKey> comp1 = pinfo1.Comparer;
            if (this.m_isDescending != pinfo1.IsDescending)
            {
                comp1 = new MinusComparer<TKey>(comp1);
            }
            return this.m_comparer.Equals(comp1);
        }

        internal override OrderByInfo Create(LambdaExpression keySel)
        {
            Type keyType = keySel.Body.Type;
            return OrderByInfo.Create(keySel, this.m_comparer, this.m_isDescending, keyType);
        }

        internal override OrderByInfo Rewrite(LambdaExpression resultSel, ParameterExpression param)
        {
            ParameterExpression a = this.m_keySelector.Parameters[0];
            Substitution pSubst = Substitution.Empty.Cons(a, param);
            LambdaExpression newKeySel = DryadLinqExpression.Rewrite(this.m_keySelector, resultSel, pSubst);
            if (newKeySel == null)
            {
                return DataSetInfo.NoOrderBy;
            }
            return this.Create(newKeySel);
        }
    }

    internal class DistinctInfo
    {
        internal virtual bool IsDistinct()
        {
            return false;
        }

        internal virtual bool IsDistinct(object comp)
        {
            return false;
        }

        internal virtual bool IsSameDistinct(DistinctInfo dist)
        {
            return false;
        }

        internal static DistinctInfo Create(object comparer, Type type)
        {
            Type infoType = typeof(DistinctInfo<>).MakeGenericType(type);
            object[] args = new object[] { comparer };
            return (DistinctInfo)Activator.CreateInstance(infoType, BindingFlags.NonPublic | BindingFlags.Instance, null ,args, null);
        }
    }
    
    internal class DistinctInfo<TKey> : DistinctInfo
    {
        private IEqualityComparer<TKey> m_comparer;

        internal Expression Comparer
        {
            get { return Expression.Constant(this.m_comparer, typeof(IEqualityComparer<TKey>)); }
        }

        internal DistinctInfo(IEqualityComparer<TKey> comparer)
        {
            this.m_comparer = (comparer == null) ? EqualityComparer<TKey>.Default : comparer;
        }

        internal override bool IsDistinct()
        {
            return true;
        }

        internal override bool IsDistinct(object comp)
        {
            IEqualityComparer<TKey> comp1 = TypeSystem.GetEqualityComparer<TKey>(comp);
            if (comp1 == null) return false;
            return this.m_comparer.Equals(comp1);
        }

        internal override bool IsSameDistinct(DistinctInfo dist)
        {
            DistinctInfo<TKey> info = dist as DistinctInfo<TKey>;
            if (info == null) return false;
            else return IsDistinct(info.Comparer);
        }
    }
}
