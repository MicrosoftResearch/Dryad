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
    /// <summary>
    /// The base interface to access a collection of IQueryable instances. The 
    /// DryadLINQ Fork operator returns a value that implements this interface.
    /// </summary>
    public interface IMultiQueryable
    {
        /// <summary>
        /// Gets the element type of the query at a specified index.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>A Type that represents the type of the elements</returns>
        Type ElementType(int index);

        /// <summary>
        /// Gets the expression tree that is associated with this instance of IMultiQueryable
        /// </summary>
        Expression Expression { get; }

        /// <summary>
        /// Gets the query provider that is associated with this instance of IMultiQueryable
        /// </summary>
        IQueryProvider Provider { get; }

        /// <summary>
        /// Gets the number of queries in this instance of IMultiQueryable
        /// </summary>
        UInt32 NumberOfInputs { get; }
    }

    /// <summary>
    /// The interface to access a collection of two IQueryable{T} instances.
    /// </summary>
    /// <typeparam name="T1">The element type of the first IQueryable{T}</typeparam>
    /// <typeparam name="T2">The element type of the second IQueryable{T}</typeparam>
    public interface IMultiQueryable<T1, T2> : IMultiQueryable
    {
        /// <summary>
        /// Gets the first IQueryable{T}
        /// </summary>
        IQueryable<T1> First { get; }

        /// <summary>
        /// Gets the second IQueryable{T}
        /// </summary>
        IQueryable<T2> Second { get; }
    }

    /// <summary>
    /// The interface to access a collection of three IQueryable{T} instances.
    /// </summary>
    /// <typeparam name="T1">The element type of the first IQueryable{T}</typeparam>
    /// <typeparam name="T2">The element type of the second IQueryable{T}</typeparam>
    /// <typeparam name="T3">The element type of the third IQueryable{T}</typeparam>
    public interface IMultiQueryable<T1, T2, T3> : IMultiQueryable
    {
        /// <summary>
        /// Gets the first IQueryable{T}
        /// </summary>
        IQueryable<T1> First { get; }

        /// <summary>
        /// Gets the second IQueryable{T}
        /// </summary>
        IQueryable<T2> Second { get; }

        /// <summary>
        /// Gets the third IQueryable{T}
        /// </summary>
        IQueryable<T3> Third { get; }
    }

    /// <summary>
    /// The interface to access a collection of IQueryable{T} instances. Each IQueryable{T}
    /// contains only elements of the same key. The IQueryable{T}s are indexed by a set of keys.
    /// </summary>
    /// <typeparam name="T">The element type of IQueryable{T}s</typeparam>
    /// <typeparam name="K">The key type</typeparam>
    public interface IKeyedMultiQueryable<T, K> : IMultiQueryable
    {
        /// <summary>
        /// Gets the IQueryable{T} associated with a specified key.
        /// </summary>
        /// <param name="key">A key</param>
        /// <returns>The IQueryable{T} associated with the key</returns>
        IQueryable<T> this[K key] { get; }

        /// <summary>
        /// Gets the keys.
        /// </summary>
        K[] Keys { get; }
    }

    internal class MultiQueryable<T, K> : IKeyedMultiQueryable<T, K>
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

                throw new DryadLinqException(DryadLinqErrorCode.MultiQueryableKeyOutOfRange,
                                             SR.MultiQueryableKeyOutOfRange);
            }
        }
    }

    internal class MultiQueryable<T, R1, R2> : IMultiQueryable<R1, R2>
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

    internal class MultiQueryable<T, R1, R2, R3> : IMultiQueryable<R1, R2, R3>
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
