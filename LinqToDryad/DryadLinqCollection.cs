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
using System.Linq;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    [Serializable]
    public struct IndexedValue<T> : IEquatable<IndexedValue<T>>, IComparable<IndexedValue<T>>
    {
        private int _index;
        private T _value;

        public int Index
        {
            get { return _index; }
            set { _index = value; }
        }

        public T Value
        {
            get { return _value; }
            set { _value = value; }
        }

        public IndexedValue(int index, T value)
        {
            _index = index;
            _value = value;
        }

        public bool Equals(IndexedValue<T> val)
        {
            return this.Index == val.Index;
        }

        public int CompareTo(IndexedValue<T> val)
        {
            return this.Index - val.Index;
        }

        public override int GetHashCode()
        {
            return this.Index;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is IndexedValue<T>))
            {
                return false;
            }
            return this.Equals((IndexedValue<T>)obj);
        }

        public static bool operator ==(IndexedValue<T> a, IndexedValue<T> b)
        {
            return a.Equals(b);
        }

        public static bool operator !=(IndexedValue<T> a, IndexedValue<T> b)
        {
            return !a.Equals(b);
        }    
    }

    public struct DryadLinqGrouping<K, T> : IGrouping<K, T>
    {
        private K m_key;
        private IEnumerable<T> m_elems;

        public DryadLinqGrouping(K key, IEnumerable<T> elems)
        {
            this.m_key = key;
            this.m_elems = elems;
        }

        public K Key
        {
            get { return this.m_key; }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.m_elems.GetEnumerator();
        }
    }

    public class BigCollection<TElement> : IEnumerable<TElement>
    {
        protected const int ChunkSize = (1 << 21);

        private TElement[][] m_elements;
        private TElement[] m_curChunk;
        private Int32 m_pos1;
        private Int32 m_pos2;

        public BigCollection()
        {
            this.m_elements = new TElement[4][];
            this.m_elements[0] = new TElement[256];
            this.m_curChunk = this.m_elements[0];
            this.m_pos1 = 0;
            this.m_pos2 = 0;
        }

        public long Count()
        {
            return (ChunkSize * (long)this.m_pos1) + this.m_pos2;
        }

        public TElement this[long index]
        {
            get { return this.m_elements[index/ChunkSize][index%ChunkSize]; }
            set { this.m_elements[index/ChunkSize][index%ChunkSize] = value; }
        }
        
        public void Add(TElement elem)
        {
            if (this.m_pos2 == this.m_curChunk.Length)
            {
                if (this.m_pos2 == ChunkSize)
                {
                    this.m_pos1++;
                    this.m_pos2 = 0;
                    if (this.m_pos1 == this.m_elements.Length)
                    {
                        TElement[][] elems = new TElement[this.m_pos1 * 2][];
                        Array.Copy(this.m_elements, 0, elems, 0, this.m_pos1);
                        this.m_elements = elems;
                    }
                    this.m_elements[this.m_pos1] = new TElement[ChunkSize];
                }
                else
                {
                    TElement[] newElems = new TElement[this.m_pos2 * 2];
                    Array.Copy(this.m_elements[this.m_pos1], 0, newElems, 0, this.m_pos2);
                    this.m_elements[this.m_pos1] = newElems;
                }
                this.m_curChunk = this.m_elements[this.m_pos1];
            }

            this.m_curChunk[this.m_pos2] = elem;
            this.m_pos2++;
        }

        public IEnumerable<TElement> Reverse()
        {
            for (int i = this.m_pos2 - 1; i >= 0; i--)
            {
                yield return this.m_elements[this.m_pos1][i];
            }
            for (int i = this.m_pos1 - 1; i >= 0; i--)
            {
                TElement[] elems = this.m_elements[i];
                for (int j = elems.Length - 1; j >= 0; j--)
                {
                    yield return elems[j];
                }
            }
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            for (int i = 0; i < this.m_pos1; i++)
            {
                TElement[] chunk = this.m_elements[i];
                for (int j = 0; j < chunk.Length; j++)
                {
                    yield return chunk[j];
                }
            }
            TElement[] lastChunk = this.m_elements[this.m_pos1];
            for (int i = 0; i < this.m_pos2; i++)
            {
                yield return lastChunk[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    public class BigDictionary<TKey, TValue> : IEnumerable<Pair<TKey, TValue>>
    {
        private const int Ratio = 2;
        private const int LogChunkSize = 21;
        private const int ChunkSize = (1 << LogChunkSize);
        private const int ChunkMask = ChunkSize - 1;
        private const uint MaxCount = UInt32.MaxValue - ChunkSize;
        
        private IEqualityComparer<TKey> m_comparer;
        private UInt32[] m_buckets;
        private Entry[][] m_entries;
        private UInt32 m_pos1;
        private UInt32 m_pos2;
        private Int64 m_count;
        private UInt32 m_freeList;

        public BigDictionary()
            : this(null, 1024)
        {
        }

        public BigDictionary(IEqualityComparer<TKey> comparer)
            : this(comparer, 1024)
        {
        }

        public BigDictionary(IEqualityComparer<TKey> comparer, int initialCapacity)
        {
            this.m_comparer = (comparer == null) ? EqualityComparer<TKey>.Default : comparer;
            this.m_buckets = new uint[CollectionHelper.GetNextPrime(initialCapacity)];
            this.m_entries = new Entry[4][];
            this.m_entries[0] = new Entry[ChunkSize];
            this.m_pos1 = 0;
            this.m_pos2 = 1;
            this.m_count = 0;
            this.m_freeList = 0;
        }

        public Int64 Count
        {
            get { return this.m_count; }
        }

        public TValue this[TKey key]
        {
            get {
                TValue value;
                if (!this.TryGetValue(key, out value))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.KeyNotFound, SR.KeyNotFound);
                }
                return value;
            }
            set {
                this.Add(key, value);
            }
        }
        
        public bool TryGetValue(TKey key, out TValue value)
        {
            int hashCode = this.m_comparer.GetHashCode(key) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_key, key))
                {
                    value = entry.m_value;
                    return true;
                }
                index = entry.m_next;
            }
            value = default(TValue);
            return false;
        }

        public bool ContainsKey(TKey key)
        {
            TValue value;
            return this.TryGetValue(key, out value);
        }

        public bool Add(TKey key, TValue value)
        {
            int hashCode = this.m_comparer.GetHashCode(key) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_key, key))
                {
                    return false;
                }
                index = entry.m_next;
            }

            // <key, value> is not in the dictionary, so add it
            if (this.m_freeList > 0)
            {
                index = this.m_freeList;
                this.m_freeList = this.m_entries[index >> LogChunkSize][index & ChunkMask].m_next;
                Entry newEntry = new Entry(key, value, this.m_buckets[bucket]);
                this.m_entries[index >> LogChunkSize][index & ChunkMask] = newEntry;
            }
            else
            {
                if (this.m_count == (this.m_buckets.Length * Ratio))
                {
                    this.Resize();
                    bucket = hashCode % this.m_buckets.Length;
                }
                Entry newEntry = new Entry(key, value, this.m_buckets[bucket]);
                if (this.m_pos2 == ChunkSize)
                {
                    if (this.m_count >= MaxCount)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.TooManyItems, SR.TooManyItems);
                    }
                    this.m_pos1++;
                    this.m_pos2 = 0;
                    if (this.m_pos1 == this.m_entries.Length)
                    {
                        Entry[][] newEntries = new Entry[this.m_pos1 * 2][];
                        Array.Copy(this.m_entries, 0, newEntries, 0, this.m_pos1);
                        this.m_entries = newEntries;
                    }
                    this.m_entries[this.m_pos1] = new Entry[ChunkSize];
                }

                this.m_entries[this.m_pos1][this.m_pos2] = newEntry;
                index = ((this.m_pos1 << LogChunkSize) | this.m_pos2);
                this.m_pos2++;
            }

            this.m_buckets[bucket] = index;
            this.m_count++;
            return true;
        }

        // Remove an item from the set. Return true iff the item is in the set and
        // is removed successfully from the set.
        public bool Remove(TKey key)
        {
            int hashCode = this.m_comparer.GetHashCode(key) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;

            uint pidx = 0;
            uint cidx = this.m_buckets[bucket];
            while (cidx > 0)
            {
                Entry entry = this.m_entries[cidx >> LogChunkSize][cidx & ChunkMask];
                if (this.m_comparer.Equals(entry.m_key, key))
                {
                    if (pidx == 0)
                    {
                        this.m_buckets[bucket] = entry.m_next;
                    }
                    else
                    {
                        this.m_entries[pidx >> LogChunkSize][pidx & ChunkMask].m_next = entry.m_next;
                    }
                    this.m_entries[cidx >> LogChunkSize][cidx & ChunkMask].m_next = this.m_freeList;
                    this.m_freeList = cidx;
                    this.m_count--;
                    return true;
                }
                pidx = cidx;
                cidx = entry.m_next;
            }
            return false;
        }

        private void Resize()
        {
            int oldSize = this.m_buckets.Length;
            int newSize = CollectionHelper.GetNextPrime(oldSize);
            if (newSize > oldSize)
            {
                this.m_buckets = new uint[newSize];
                for (uint i = 1; i <= this.m_count; i++)
                {
                    Entry entry = this.m_entries[i >> LogChunkSize][i & ChunkMask];
                    int bucket = (this.m_comparer.GetHashCode(entry.m_key) & 0x7FFFFFFF) % newSize;
                    this.m_entries[i >> LogChunkSize][i & ChunkMask].m_next = this.m_buckets[bucket];
                    this.m_buckets[bucket] = i;
                }
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return entry.m_key;
                    index = entry.m_next;
                }
            }
        }

        public IEnumerable<TValue> GetValues()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return entry.m_value;
                    index = entry.m_next;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<Pair<TKey, TValue>> GetEnumerator()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return new Pair<TKey, TValue>(entry.m_key, entry.m_value);
                    index = entry.m_next;
                }
            }
        }

        private struct Entry
        {
            public TKey m_key;
            public TValue m_value;
            public UInt32 m_next;

            public Entry(TKey key, TValue value, UInt32 next)
            {
                this.m_key = key;
                this.m_value = value;
                this.m_next = next;
            }
        }
    }

    public class AccumulateDictionary<TKey, TSource, TValue> : IEnumerable<Pair<TKey, TValue>>
    {
        private const int Ratio = 2;
        private const int LogChunkSize = 21;
        private const int ChunkSize = (1 << LogChunkSize);
        private const int ChunkMask = ChunkSize - 1;
        private const uint MaxCount = UInt32.MaxValue - ChunkSize;
        
        private IEqualityComparer<TKey> m_comparer;
        private Func<TSource, TValue> m_seed;
        private Func<TValue, TSource, TValue> m_accumulator;
        private UInt32[] m_buckets;
        private Entry[][] m_entries;
        private UInt32 m_pos1;
        private UInt32 m_pos2;
        private Int64 m_count;

        public AccumulateDictionary(Func<TSource, TValue> seed,
                                    Func<TValue, TSource, TValue> accumulator)
            : this(null, 1024, seed, accumulator)
        {
        }

        public AccumulateDictionary(IEqualityComparer<TKey> comparer,
                                    Func<TSource, TValue> seed,
                                    Func<TValue, TSource, TValue> accumulator)
            : this(comparer, 1024, seed, accumulator)
        {
        }

        public AccumulateDictionary(IEqualityComparer<TKey> comparer,
                                    int initialCapacity,
                                    Func<TSource, TValue> seed,
                                    Func<TValue, TSource, TValue> accumulator)
        {
            this.m_comparer = (comparer == null) ? EqualityComparer<TKey>.Default : comparer;
            this.m_seed = seed;
            this.m_accumulator = accumulator;
            this.m_buckets = new uint[CollectionHelper.GetNextPrime(initialCapacity)];
            this.m_entries = new Entry[4][];
            this.m_entries[0] = new Entry[ChunkSize];
            this.m_pos1 = 0;
            this.m_pos2 = 1;
            this.m_count = 0;
        }

        public Int64 Count
        {
            get { return this.m_count; }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            int hashCode = this.m_comparer.GetHashCode(key) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_key, key))
                {
                    value = entry.m_value;
                    return true;
                }
                index = entry.m_next;
            }
            value = default(TValue);
            return false;
        }

        public bool ContainsKey(TKey key)
        {
            TValue value;
            return this.TryGetValue(key, out value);
        }

        public void Add(TKey key, TSource elem)
        {
            int hashCode = this.m_comparer.GetHashCode(key) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_key, key))
                {
                    this.m_entries[index >> LogChunkSize][index & ChunkMask].m_value
                        = this.m_accumulator(entry.m_value, elem);
                    return;
                }
                index = entry.m_next;
            }

            // <key, elem> is not in the dictionary, so add it
            if (this.m_count == (this.m_buckets.Length * Ratio))
            {
                this.Resize();
                bucket = hashCode % this.m_buckets.Length;
            }
            TValue val = this.m_seed(elem);
            Entry newEntry = new Entry(key, val, this.m_buckets[bucket]);
            if (this.m_pos2 == ChunkSize)
            {
                if (this.m_count >= MaxCount)
                {
                    throw new DryadLinqException("Too many items");
                }
                this.m_pos1++;
                this.m_pos2 = 0;
                if (this.m_pos1 == this.m_entries.Length)
                {
                    Entry[][] newEntries = new Entry[this.m_pos1 * 2][];
                    Array.Copy(this.m_entries, 0, newEntries, 0, this.m_pos1);
                    this.m_entries = newEntries;
                }
                this.m_entries[this.m_pos1] = new Entry[ChunkSize];
            }

            this.m_entries[this.m_pos1][this.m_pos2] = newEntry;
            index = ((this.m_pos1 << LogChunkSize) | this.m_pos2);
            this.m_pos2++;
        
            this.m_buckets[bucket] = index;
            this.m_count++;
        }

        private void Resize()
        {
            int oldSize = this.m_buckets.Length;
            int newSize = CollectionHelper.GetNextPrime(oldSize);
            if (newSize > oldSize)
            {
                this.m_buckets = new uint[newSize];
                for (uint i = 1; i <= this.m_count; i++)
                {
                    Entry entry = this.m_entries[i >> LogChunkSize][i & ChunkMask];
                    int bucket = (this.m_comparer.GetHashCode(entry.m_key) & 0x7FFFFFFF) % newSize;
                    this.m_entries[i >> LogChunkSize][i & ChunkMask].m_next = this.m_buckets[bucket];
                    this.m_buckets[bucket] = i;
                }
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return entry.m_key;
                    index = entry.m_next;
                }
            }
        }

        public IEnumerable<TValue> GetValues()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return entry.m_value;
                    index = entry.m_next;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<Pair<TKey, TValue>> GetEnumerator()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return new Pair<TKey, TValue>(entry.m_key, entry.m_value);
                    index = entry.m_next;
                }
            }
        }

        private struct Entry
        {
            public TKey m_key;
            public TValue m_value;
            public UInt32 m_next;

            public Entry(TKey key, TValue value, UInt32 next)
            {
                this.m_key = key;
                this.m_value = value;
                this.m_next = next;
            }
        }
    }

    public class BigHashSet<TElement> : IEnumerable<TElement>
    {
        private const int Ratio = 2;
        private const int LogChunkSize = 21;
        private const int ChunkSize = (1 << LogChunkSize);
        private const int ChunkMask = ChunkSize - 1;
        private const uint MaxCount = UInt32.MaxValue - ChunkSize;
        
        private IEqualityComparer<TElement> m_comparer;
        private UInt32[] m_buckets;
        private Entry[][] m_entries;
        private UInt32 m_pos1;
        private UInt32 m_pos2;
        private Int64 m_count;
        private UInt32 m_freeList;

        public BigHashSet()
            : this(null, 1024)
        {
        }

        public BigHashSet(IEqualityComparer<TElement> comparer)
            : this(comparer, 1024)
        {
        }

        public BigHashSet(IEqualityComparer<TElement> comparer, int initialCapacity)
        {
            this.m_comparer = (comparer == null) ? EqualityComparer<TElement>.Default : comparer;
            this.m_buckets = new uint[CollectionHelper.GetNextPrime(initialCapacity)];
            this.m_entries = new Entry[4][];
            this.m_entries[0] = new Entry[ChunkSize];
            this.m_pos1 = 0;
            this.m_pos2 = 1;
            this.m_count = 0;
            this.m_freeList = 0;
        }

        public Int64 Count
        {
            get { return this.m_count; }
        }
        
        public bool Contains(TElement item)
        {
            int hashCode = this.m_comparer.GetHashCode(item) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_item, item))
                {
                    return true;
                }
                index = entry.m_next;
            }
            return false;
        }

        // Add an item into the set. Return true iff the item is not in the set and
        // is added successfully into the set.
        public bool Add(TElement item)
        {
            int hashCode = this.m_comparer.GetHashCode(item) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;
            UInt32 index = this.m_buckets[bucket];
            while (index > 0)
            {
                Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                if (this.m_comparer.Equals(entry.m_item, item))
                {
                    return false;
                }
                index = entry.m_next;
            }

            // item is not in the set, so add it
            if (this.m_freeList > 0)
            {
                index = this.m_freeList;
                this.m_freeList = this.m_entries[index >> LogChunkSize][index & ChunkMask].m_next;
                Entry newEntry = new Entry(item, this.m_buckets[bucket]);
                this.m_entries[index >> LogChunkSize][index & ChunkMask] = newEntry;
            }
            else
            {
                if (this.m_count == (this.m_buckets.Length * Ratio))
                {
                    this.Resize();
                    bucket = hashCode % this.m_buckets.Length;
                }
                Entry newEntry = new Entry(item, this.m_buckets[bucket]);
                if (this.m_pos2 == ChunkSize)
                {
                    if (this.m_count >= MaxCount)
                    {
                        throw new DryadLinqException(DryadLinqErrorCode.TooManyItems, SR.TooManyItems);
                    }
                    this.m_pos1++;
                    this.m_pos2 = 0;
                    if (this.m_pos1 == this.m_entries.Length)
                    {
                        Entry[][] newEntries = new Entry[this.m_pos1 * 2][];
                        Array.Copy(this.m_entries, 0, newEntries, 0, this.m_pos1);
                        this.m_entries = newEntries;
                    }
                    this.m_entries[this.m_pos1] = new Entry[ChunkSize];
                }

                this.m_entries[this.m_pos1][this.m_pos2] = newEntry;
                index = ((this.m_pos1 << LogChunkSize) | this.m_pos2);
                this.m_pos2++;
            }

            this.m_buckets[bucket] = index;
            this.m_count++;
            return true;
        }

        // Remove an item from the set. Return true iff the item is in the set and
        // is removed successfully from the set.
        public bool Remove(TElement item)
        {
            int hashCode = this.m_comparer.GetHashCode(item) & 0x7FFFFFFF;
            int bucket = hashCode % this.m_buckets.Length;

            uint pidx = 0;
            uint cidx = this.m_buckets[bucket];
            while (cidx > 0)
            {
                Entry entry = this.m_entries[cidx >> LogChunkSize][cidx & ChunkMask];
                if (this.m_comparer.Equals(entry.m_item, item))
                {
                    if (pidx == 0)
                    {
                        this.m_buckets[bucket] = entry.m_next;
                    }
                    else
                    {
                        this.m_entries[pidx >> LogChunkSize][pidx & ChunkMask].m_next = entry.m_next;
                    }
                    this.m_entries[cidx >> LogChunkSize][cidx & ChunkMask].m_next = this.m_freeList;
                    this.m_freeList = cidx;
                    this.m_count--;
                    return true;
                }
                pidx = cidx;
                cidx = entry.m_next;
            }
            return false;
        }

        private void Resize()
        {
            int oldSize = this.m_buckets.Length;
            int newSize = CollectionHelper.GetNextPrime(oldSize);
            if (newSize > oldSize)
            {
                this.m_buckets = new uint[newSize];
                for (uint i = 1; i <= this.m_count; i++)
                {
                    Entry entry = this.m_entries[i >> LogChunkSize][i & ChunkMask];
                    int bucket = (this.m_comparer.GetHashCode(entry.m_item) & 0x7FFFFFFF) % newSize;
                    this.m_entries[i >> LogChunkSize][i & ChunkMask].m_next = this.m_buckets[bucket];
                    this.m_buckets[bucket] = i;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                uint index = this.m_buckets[i];
                while (index > 0)
                {
                    Entry entry = this.m_entries[index >> LogChunkSize][index & ChunkMask];
                    yield return entry.m_item;
                    index = entry.m_next;
                }
            }
        }

        private struct Entry
        {
            public TElement m_item;
            public UInt32 m_next;

            public Entry(TElement item, UInt32 next)
            {
                this.m_item = item;
                this.m_next = next;
            }
        }
    }

    public class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private static int MaxCount = Int32.MaxValue / TypeSystem.GetInMemSize(typeof(TElement));

        private TKey m_key;
        private TElement[] m_elements;
        private int m_count;
        private Grouping<TKey, TElement> m_next;

        public Grouping(TKey key)
            : this(key, 2)
        {
        }

        internal Grouping(TKey key, int len)
        {
            this.m_key = key;
            this.m_elements = new TElement[len];
            this.m_count = 0;
            this.m_next = null;
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            for (int i = 0; i < this.m_count; i++)
            {
                yield return this.m_elements[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public TKey Key
        {
            get { return this.m_key; }
        }

        public int Count()
        {
            return this.m_count;
        }

        internal TElement[] Elements
        {
            get { return m_elements; }
        }
        
        internal Grouping<TKey, TElement> Next
        {
            get { return this.m_next; }
            set { this.m_next = value; }
        }
        
        public void AddItem(TElement elem)
        {
            if (this.m_elements.Length == this.m_count)
            {
                if (this.m_count >= MaxCount)
                {
                    throw new DryadLinqException("Too many elements in a single group.");
                }
                int newSize = this.m_count * 2;
                if (newSize > MaxCount) newSize = MaxCount;
                TElement[] newElements = new TElement[newSize];
                Array.Copy(this.m_elements, 0, newElements, 0, this.m_count);
                this.m_elements = newElements;
            }
            this.m_elements[this.m_count++] = elem;
        }

        public override string ToString()
        {
            return "Grouping[" + this.Key + "]";
        }
    }

    public class BigGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        protected const int ChunkSize = (1 << 21);
        
        private TKey m_key;
        private TElement[][] m_elements;
        private int m_pos1;
        private int m_pos2;
        private BigGrouping<TKey, TElement> m_next;    

        public BigGrouping(TKey key)
        {
            this.m_key = key;
            this.m_elements = new TElement[2][];
            this.m_elements[0] = new TElement[2];
            this.m_pos1 = 0;
            this.m_pos2 = 0;
            this.m_next = null;        
        }

        public TKey Key
        {
            get { return this.m_key; }
        }

        public long Count()
        {
            return (ChunkSize * (long)this.m_pos1) + this.m_pos2;
        }

        internal BigGrouping<TKey, TElement> Next
        {
            get { return this.m_next; }
            set { this.m_next = value; }
        }

        public void AddItem(TElement elem)
        {
            if (this.m_pos2 == this.m_elements[this.m_pos1].Length)
            {
                if (this.m_pos2 == ChunkSize)
                {
                    this.m_pos1++;
                    this.m_pos2 = 0;
                    if (this.m_pos1 == this.m_elements.Length)
                    {
                        TElement[][] elems = new TElement[this.m_pos1 * 2][];
                        Array.Copy(this.m_elements, 0, elems, 0, this.m_pos1);
                        this.m_elements = elems;
                    }
                    this.m_elements[this.m_pos1] = new TElement[ChunkSize];
                }
                else
                {
                    TElement[] newElems = new TElement[this.m_pos2 * 2];
                    Array.Copy(this.m_elements[this.m_pos1], 0, newElems, 0, this.m_pos2);
                    this.m_elements[this.m_pos1] = newElems;
                }
            }

            this.m_elements[this.m_pos1][this.m_pos2] = elem;
            this.m_pos2++;
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            for (int i = 0; i < this.m_pos1; i++)
            {
                TElement[] elems = this.m_elements[i];
                for (int j = 0; j < elems.Length; j++)
                {
                    yield return elems[j];
                }
            }
            for (int j = 0; j < this.m_pos2; j++)
            {
                yield return this.m_elements[this.m_pos1][j];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public override string ToString()
        {
            return "Grouping[" + this.Key + "]";
        }
    }

    public class Grouping<TKey, TElement, TResult>
    {
        private TKey m_key;
        private TElement[] m_elems;
        private TResult[] m_results;
        private int m_count;
        private Grouping<TKey, TElement, TResult> m_next;

        public Grouping(TKey key)
            : this(key, 2)
        {
        }

        internal Grouping(TKey key, int len)
        {
            this.m_key = key;
            this.m_elems = new TElement[len];
            this.m_count = 0;
            this.m_results = new TResult[2];
            this.m_next = null;
        }

        public TKey Key
        {
            get { return this.m_key; }
        }

        public int Count()
        {
            return this.ElemCount;
        }

        private int ElemCount
        {
            get { return (this.m_count & 0xFFFFFF); }
        }

        private int ResCount
        {
            get { return (this.m_count >> 24); }
        }
            
        internal Grouping<TKey, TElement, TResult> Next
        {
            get { return this.m_next; }
            set { this.m_next = value; }
        }
        
        public void Reduce(Func<IEnumerable<TElement>, TResult> resultSelector,
                           Func<IEnumerable<TResult>, TResult> combiner)
        {
            int elemCnt = this.ElemCount;
            if (elemCnt > 0)
            {
                TElement[] curElems = this.m_elems;
                if (elemCnt < this.m_elems.Length)
                {
                    curElems = new TElement[elemCnt];
                    Array.Copy(this.m_elems, 0, curElems, 0, elemCnt);
                }
                int resCount = this.ResCount;
                this.m_results[resCount] = resultSelector(curElems);
                if (resCount == 1)
                {
                    this.m_results[0] = combiner(this.m_results);
                }

                this.m_elems = new TElement[2];
                this.m_count = 0x1000000;
            }
        }

        public TResult GetResult(Func<IEnumerable<TElement>, TResult> resultSelector,
                                 Func<IEnumerable<TResult>, TResult> combiner)
        {
            int elemCnt = this.ElemCount;
            if (elemCnt > 0)
            {
                TElement[] curElems = this.m_elems;
                if (elemCnt < this.m_elems.Length)
                {
                    curElems = new TElement[elemCnt];
                    Array.Copy(this.m_elems, 0, curElems, 0, elemCnt);
                }
                int resCount = this.ResCount;
                this.m_results[resCount] = resultSelector(curElems);
                if (resCount == 1)
                {
                    this.m_results[0] = combiner(this.m_results);
                }
            }
            return this.m_results[0];
        }
        
        public void AddItem(TElement elem)
        {
            int elemCnt = this.ElemCount;
            if (this.m_elems.Length == elemCnt)
            {
                if (elemCnt >= 0x800000)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.TooManyElementsBeforeReduction,
                                               SR.TooManyElementsBeforeReduction);
                }
                TElement[] newElems = new TElement[elemCnt * 2];
                Array.Copy(this.m_elems, 0, newElems, 0, elemCnt);
                this.m_elems = newElems;
            }
            this.m_elems[elemCnt] = elem;
            this.m_count++;
        }

        public override string ToString()
        {
            return "Grouping[" + this.Key + "]";
        }
    }

    public class GroupingHashSet<TElement, TKey> : IEnumerable<IGrouping<TKey, TElement>>
    {
        private const int Ratio = 2;
        private const int MaxGroupSize = 64;
        
        private Grouping<TKey, TElement>[] m_buckets;
        private IEqualityComparer<TKey> m_comparer;
        private long m_count;
        private int m_maxGroupSize;

        public GroupingHashSet(IEqualityComparer<TKey> comparer)
            : this(comparer, 1024, MaxGroupSize)
        {
        }

        internal GroupingHashSet(IEqualityComparer<TKey> comparer, int capacity)
            : this(comparer, capacity, MaxGroupSize)
        {
        }
        
        internal GroupingHashSet(IEqualityComparer<TKey> comparer, int capacity, int maxGroupSize)
        {
            int size = CollectionHelper.GetNextPrime(capacity);
            this.m_buckets = new Grouping<TKey, TElement>[size];
            this.m_comparer = comparer;
            this.m_count = 0;
            this.m_maxGroupSize = maxGroupSize;
        }

        internal Grouping<TKey, TElement> GetGroup(TKey key)
        {
            int hashCode = this.m_comparer.GetHashCode(key);
            int startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            for (Grouping<TKey, TElement> g = this.m_buckets[startIdx]; g != null; g = g.Next)
            {
                if (hashCode == this.m_comparer.GetHashCode(g.Key) &&
                    this.m_comparer.Equals(key, g.Key))
                {
                    return g;
                }
            }
            return null;
        }

        public Grouping<TKey, TElement> AddItem(TKey key, TElement elem)
        {
            int hashCode = this.m_comparer.GetHashCode(key);
            int startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            for (Grouping<TKey, TElement> g = this.m_buckets[startIdx]; g != null; g = g.Next)
            {
                if (hashCode == this.m_comparer.GetHashCode(g.Key) &&
                    this.m_comparer.Equals(key, g.Key))
                {
                    g.AddItem(elem);
                    return g;
                }
            }

            // Add a new group for the element:
            if (this.m_count == (this.m_buckets.Length * Ratio))
            {
                this.Resize();
                startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            }
            Grouping<TKey, TElement> newGroup = new Grouping<TKey, TElement>(key);
            newGroup.AddItem(elem);
            newGroup.Next = this.m_buckets[startIdx];
            this.m_buckets[startIdx] = newGroup;
            this.m_count++;
            return newGroup;
        }

        internal Grouping<TKey, TElement> AddItemPartial(TKey key, TElement elem)
        {
            int hashCode = this.m_comparer.GetHashCode(key);
            int startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            Grouping<TKey, TElement> g = this.m_buckets[startIdx];
            if (g != null &&
                hashCode == this.m_comparer.GetHashCode(g.Key) &&
                this.m_comparer.Equals(key, g.Key) &&
                g.Count() < this.m_maxGroupSize)
            {
                g.AddItem(elem);
                return null;
            }
            Grouping<TKey, TElement> newGroup = new Grouping<TKey, TElement>(key);
            newGroup.AddItem(elem);
            this.m_buckets[startIdx] = newGroup;
            if (g == null) this.m_count++;
            return g;
        }

        private void Resize()
        {
            int oldSize = this.m_buckets.Length;
            int newSize = CollectionHelper.GetNextPrime(oldSize);
            if (newSize > oldSize)
            {
                Grouping<TKey, TElement>[] oldBuckets = this.m_buckets;
                this.m_buckets = new Grouping<TKey, TElement>[newSize];
                for (int i = 0; i < oldBuckets.Length; i++)
                {
                    Grouping<TKey, TElement> curGroup = oldBuckets[i];
                    while (curGroup != null)
                    {
                        // Add the group:
                        Grouping<TKey, TElement> nextGroup = curGroup.Next;
                        int hashCode = this.m_comparer.GetHashCode(curGroup.Key);
                        int startIdx = (hashCode & 0x7FFFFFFF) % newSize;
                        curGroup.Next = this.m_buckets[startIdx];
                        this.m_buckets[startIdx] = curGroup;
                        curGroup = nextGroup;
                    }
                }
            }
        }
        
        public IEnumerator<IGrouping<TKey, TElement>> GetEnumerator()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                Grouping<TKey, TElement> g = this.m_buckets[i];
                while (g != null)
                {
                    yield return g;
                    g = g.Next;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    internal class GroupingHashSet<TResult, TElement, TKey> : IEnumerable<Grouping<TKey, TElement, TResult>>
    {
        private const int Ratio = 2;
        private const int MaxGroupSize = 64;
        
        private Grouping<TKey, TElement, TResult>[] m_buckets;
        private IEqualityComparer<TKey> m_comparer;
        private long m_count;
        private long m_elemCount;
        private int m_maxGroupSize;

        public GroupingHashSet(IEqualityComparer<TKey> comparer)
            : this(comparer, 1024, MaxGroupSize)
        {
        }

        internal GroupingHashSet(IEqualityComparer<TKey> comparer, int capacity)
            : this(comparer, capacity, MaxGroupSize)
        {
        }
        
        internal GroupingHashSet(IEqualityComparer<TKey> comparer, int capacity, int maxGroupSize)
        {
            int size = CollectionHelper.GetNextPrime(capacity);
            this.m_buckets = new Grouping<TKey, TElement, TResult>[size];
            this.m_comparer = comparer;
            this.m_count = 0;
            this.m_elemCount = 0;
            this.m_maxGroupSize = maxGroupSize;
        }

        public long GroupCount
        {
            get { return this.m_count; }
        }

        public long ElemCount
        {
            get { return this.m_elemCount; }
        }

        public Grouping<TKey, TElement, TResult> AddItem(TKey key, TElement elem)
        {
            this.m_elemCount++;
            int hashCode = this.m_comparer.GetHashCode(key);
            int startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            for (Grouping<TKey, TElement, TResult> g = this.m_buckets[startIdx]; g != null; g = g.Next)
            {
                if (hashCode == this.m_comparer.GetHashCode(g.Key) &&
                    this.m_comparer.Equals(key, g.Key))
                {
                    g.AddItem(elem);
                    return g;
                }
            }

            // Add a new group for the element:
            if (this.m_count == (this.m_buckets.Length * Ratio))
            {
                this.Resize();
                startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            }
            Grouping<TKey, TElement, TResult> newGroup = new Grouping<TKey, TElement, TResult>(key);
            newGroup.AddItem(elem);
            newGroup.Next = this.m_buckets[startIdx];
            this.m_buckets[startIdx] = newGroup;
            this.m_count++;
            return newGroup;
        }

        internal Grouping<TKey, TElement, TResult> AddItemPartial(TKey key, TElement elem)
        {
            this.m_elemCount++;
            int hashCode = this.m_comparer.GetHashCode(key);
            int startIdx = (hashCode & 0x7FFFFFFF) % this.m_buckets.Length;
            Grouping<TKey, TElement, TResult> g = this.m_buckets[startIdx];
            if (g != null &&
                hashCode == this.m_comparer.GetHashCode(g.Key) &&
                this.m_comparer.Equals(key, g.Key) &&
                g.Count() < this.m_maxGroupSize)
            {
                g.AddItem(elem);
                return null;
            }
            Grouping<TKey, TElement, TResult> g1 = new Grouping<TKey, TElement, TResult>(key);
            g1.AddItem(elem);
            this.m_buckets[startIdx] = g1;
            if (g == null)
            {
                this.m_count++;
            }
            else
            {
                this.m_elemCount -= g.Count();
            }
            return g;
        }

        internal void Reduce(Func<IEnumerable<TElement>, TResult> resultSelector,
                             Func<IEnumerable<TResult>, TResult> combiner)
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                Grouping<TKey, TElement, TResult> curGroup = this.m_buckets[i];
                while (curGroup != null)
                {
                    curGroup.Reduce(resultSelector, combiner);
                    curGroup = curGroup.Next;
                }
            }
            this.m_elemCount = 0;
        }

        private void Resize()
        {
            int oldSize = this.m_buckets.Length;
            int newSize = CollectionHelper.GetNextPrime(oldSize);
            if (newSize > oldSize)
            {
                Grouping<TKey, TElement, TResult>[] oldBuckets = this.m_buckets;
                this.m_buckets = new Grouping<TKey, TElement, TResult>[newSize];
                for (int i = 0; i < oldBuckets.Length; i++)
                {
                    Grouping<TKey, TElement, TResult> curGroup = oldBuckets[i];
                    while (curGroup != null)
                    {
                        // Add the group:
                        Grouping<TKey, TElement, TResult> nextGroup = curGroup.Next;
                        int hashCode = this.m_comparer.GetHashCode(curGroup.Key);
                        int startIdx = (hashCode & 0x7FFFFFFF) % newSize;
                        curGroup.Next = this.m_buckets[startIdx];
                        this.m_buckets[startIdx] = curGroup;
                        curGroup = nextGroup;
                    }
                }
            }
        }

        public IEnumerator<Grouping<TKey, TElement, TResult>> GetEnumerator()
        {
            for (int i = 0; i < this.m_buckets.Length; i++)
            {
                Grouping<TKey, TElement, TResult> g = this.m_buckets[i];
                while (g != null)
                {
                    yield return g;
                    g = g.Next;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    internal static class CollectionHelper
    {
        private static readonly int[] primes = new int[] { 2053, 16411, 1048583, 8388617, 16777259, 33554467, 67108879 };

        internal static int GetNextPrime(int p)
        {
            int len = primes.Length;
            for (int i = 0; i < len; i++)
            {
                if (primes[i] > p) return primes[i];
            }
            return primes[len-1];
        }
    }
}
