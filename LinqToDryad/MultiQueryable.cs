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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq.Expressions;
using System.Linq;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    public interface IMultiQueryable
    {
        Type ElementType(int index);
        Expression Expression { get; }
        IQueryProvider Provider { get; }
        UInt32 NumberOfInputs { get; }
    }

    public interface IKeyedMultiQueryable<T, K> : IMultiQueryable
    {
        IQueryable<T> this[K key] { get; }
        K[] Keys { get; }
    }

    public interface IMultiQueryable<T1, T2> : IMultiQueryable
    {
        IQueryable<T1> First { get; }
        IQueryable<T2> Second { get; }
    }

    public interface IMultiQueryable<T1, T2, T3> : IMultiQueryable
    {
        IQueryable<T1> First { get; }
       IQueryable<T2> Second { get; }
        IQueryable<T3> Third { get; }
    }

    public class MultiQueryable<T, K> : IKeyedMultiQueryable<T, K>
    {
        private IQueryable<T> m_source;
        private Expression m_queryExpression;
        private IMultiEnumerable<T> m_enumerables;
        private K[] m_keys;
        private Dictionary<K, int> m_keyMap;

        public MultiQueryable(IQueryable<T> source, K[] keys, IMultiEnumerable<T> enumerables)
        {
            this.m_source = source;
            this.m_queryExpression = null;
            this.m_enumerables = enumerables;
            this.m_keys = new K[keys.Length];
            this.m_keyMap = new Dictionary<K, int>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                this.m_keys[i] = keys[i];
                this.m_keyMap.Add(keys[i], i);
            }
        }

        public MultiQueryable(IQueryable<T> source, K[] keys, Expression queryExpr)
        {
            this.m_source = source;
            this.m_queryExpression = queryExpr;
            this.m_enumerables = null;
            this.m_keys = new K[keys.Length];
            this.m_keyMap = new Dictionary<K, int>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                this.m_keys[i] = keys[i];
                this.m_keyMap.Add(keys[i], i);
            }
        }

        public Type ElementType(int index)
        {
           return typeof(T);
        }

        public Expression Expression
        {
            get { return this.m_queryExpression; }
        }

        public IQueryProvider Provider
        {
            get { return this.m_source.Provider; }
        }

        public UInt32 NumberOfInputs
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    return (UInt32)this.m_enumerables.NumberOfInputs;
                }
                return (UInt32)this.m_keys.Length;
            }
        }

        public K[] Keys
        {
            get { return this.m_keys; }
        }

        public IQueryable<T> this[K key]
        {
            get
            {
                int index;
                if (this.m_keyMap.TryGetValue(key, out index))
                {
                    if (this.m_enumerables != null)
                    {
                        var q = this.m_enumerables[index].AsQueryable();
                        return new DryadLinqLocalQuery<T>(this.Provider, q);
                    }
                    return this.ForkChoose<T>(index);
                }

                //@@TODO: throw ArgumentOutOfRangeException?
                throw new DryadLinqException(DryadLinqErrorCode.MultiQueryableKeyOutOfRange,
                                             SR.MultiQueryableKeyOutOfRange);
            }
        }
    }

    public class MultiQueryable<T, R1, R2> : IMultiQueryable<R1, R2>
    {
        private IQueryable<T> m_source;
        private Expression m_queryExpression;
        private IMultiEnumerable<R1, R2> m_enumerables;

        public MultiQueryable(IQueryable<T> source, IMultiEnumerable<R1, R2> enumerables)
        {
            this.m_source = source;
            this.m_queryExpression = null;
            this.m_enumerables = enumerables;
        }

        public MultiQueryable(IQueryable<T> source, Expression queryExpr)
        {
            this.m_source = source;
            this.m_queryExpression = queryExpr;
            this.m_enumerables = null;
        }

        public Type ElementType(int index)
        {
            if (index == 0)
            {
                return typeof(R1);
            }
            else if (index == 1)
            {
                return typeof(R2);
            }
            else
            {
                //@@TODO: throw ArgumentOutOfRangeException?
                throw new DryadLinqException(DryadLinqErrorCode.IndexOutOfRange,
                                             SR.IndexOutOfRange);
            }
        }

        public Expression Expression
        {
            get { return this.m_queryExpression; }
        }

        public IQueryProvider Provider
        {
            get { return this.m_source.Provider; }
        }

        public UInt32 NumberOfInputs
        {
            get { return 2; }
        }

        public IQueryable<R1> First
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    var q = this.m_enumerables.First.AsQueryable();
                    return new DryadLinqLocalQuery<R1>(this.Provider, q);
                }
                return this.ForkChoose<R1>(0);
            }
        }

        public IQueryable<R2> Second
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    var q = this.m_enumerables.Second.AsQueryable();
                    return new DryadLinqLocalQuery<R2>(this.Provider, q);
                }
                return this.ForkChoose<R2>(1);
            }
        }
    }

    public class MultiQueryable<T, R1, R2, R3> : IMultiQueryable<R1, R2, R3>
    {
        private IQueryable<T> m_source;
        private Expression m_queryExpression;
        private IMultiEnumerable<R1, R2, R3> m_enumerables;

        public MultiQueryable(IQueryable<T> source, Expression queryExpr)
        {
            this.m_source = source;
            this.m_queryExpression = queryExpr;
            this.m_enumerables = null;
        }

        public MultiQueryable(IQueryable<T> source, IMultiEnumerable<R1, R2, R3> enumerables)
        {
            this.m_source = source;
            this.m_queryExpression = null;
            this.m_enumerables = enumerables;
        }

        public Type ElementType(int index)
        {
            if (index == 0)
            {
                return typeof(R1);
            }
            else if (index == 1)
            {
                return typeof(R2);
            }
            else if (index == 2)
            {
                return typeof(R3);
            }
            else
            {
                //@@TODO: throw ArgumentOutOfRangeException?
                throw new DryadLinqException(DryadLinqErrorCode.IndexOutOfRange,
                                             SR.IndexOutOfRange);
            }
        }

        public Expression Expression
        {
            get { return this.m_queryExpression; }
        }

        public IQueryProvider Provider
        {
            get { return this.m_source.Provider; }
        }

        public UInt32 NumberOfInputs
        {
            get { return 3; }
        }

        public IQueryable<R1> First
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    var q = this.m_enumerables.First.AsQueryable();
                    return new DryadLinqLocalQuery<R1>(this.Provider, q);
                }
                return this.ForkChoose<R1>(0);
            }
        }

        public IQueryable<R2> Second
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    var q = this.m_enumerables.Second.AsQueryable();
                    return new DryadLinqLocalQuery<R2>(this.Provider, q);
                }
                return this.ForkChoose<R2>(1);
            }
        }

        public IQueryable<R3> Third
        {
            get
            {
                if (this.m_enumerables != null)
                {
                    var q = this.m_enumerables.Third.AsQueryable();
                    return new DryadLinqLocalQuery<R3>(this.Provider, q);
                }
                return this.ForkChoose<R3>(2);
            }
        }
    }
}
