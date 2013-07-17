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
using System.Text;
using System.Reflection;
using System.Linq;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq
{
    // If a class T implements HpcSerializer<T>, HpcLinq will use the
    // read and write methods of the class to do serialization.
    public interface IHpcSerializer<T>
    {
        T Read(HpcBinaryReader reader);
        void Write(HpcBinaryWriter writer, T x);
    }
}

namespace Microsoft.Research.DryadLinq.Internal
{
    public abstract class HpcSerializer<T> : IHpcSerializer<T>
    {
        public HpcSerializer() { }
        public abstract T Read(HpcBinaryReader reader);
        public abstract void Write(HpcBinaryWriter writer, T x);
    }

    internal struct HpcLinqSequence<T> : IEnumerable<T>
    {
        private T[] elements;

        internal HpcLinqSequence(T[] elems)
        {
            this.elements = elems;
        }

        internal int Count()
        {
            return this.elements.Length;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        internal IEnumerator<T> GetEnumerator()
        {
            foreach (T x in this.elements)
            {
                yield return x;
            }
        }
    }

    // The only use is to handle Nullable<T>.
    public static class StructHpcSerialization<T, S>
        where T : struct
        where S : HpcSerializer<T>, new()
    {
        private static S serializer = new S();
        
        public static void Read(HpcBinaryReader reader, out Nullable<T> val)
        {
            bool hasValue = reader.ReadBool();
            if (hasValue)
            {
                val = serializer.Read(reader);
            }
            else
            {
                val = null;
            }
        }

        public static void Write(HpcBinaryWriter writer, Nullable<T> val)
        {
            writer.Write(val.HasValue);
            if (val.HasValue)
            {
                serializer.Write(writer, val.Value);
            }
        }
    }

    public static class StructHpcSerialization<T1, T2, S1, S2>
        where T1 : struct
        where T2 : struct        
        where S1 : HpcSerializer<T1>, new()
        where S2 : HpcSerializer<T2>, new()        
    {
        private static S1 serializer1 = new S1();
        private static S2 serializer2 = new S2();
    }

    // A workaround to deal with some limitation of C# generics
    public static class HpcSerialization<T, S>
        where S : HpcSerializer<T>, new()
    {
        private static S serializer = new S();
        
        public static void Read(HpcBinaryReader reader, out List<T> list)
        {
            int cnt = reader.ReadInt32();
            list = new List<T>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                list.Add(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, List<T> list)
        {
            writer.Write(list.Count);         
            foreach (T elem in list)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out LinkedList<T> list)
        {
            int cnt = reader.ReadInt32();
            list = new LinkedList<T>();
            for (int i = 0; i < cnt; i++)
            {
                list.AddLast(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, LinkedList<T> list)
        {
            writer.Write(list.Count);
            foreach (T elem in list)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out Queue<T> queue)
        {
            int cnt = reader.ReadInt32();
            queue = new Queue<T>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                queue.Enqueue(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, Queue<T> queue)
        {
            writer.Write(queue.Count);
            foreach (T elem in queue)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out Stack<T> stack)
        {
            int cnt = reader.ReadInt32();
            stack = new Stack<T>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                stack.Push(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, Stack<T> stack)
        {
            writer.Write(stack.Count);
            foreach (T elem in stack)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out HashSet<T> set)
        {
            int cnt = reader.ReadInt32();
            set = new HashSet<T>();
            for (int i = 0; i < cnt; i++)
            {
                set.Add(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, HashSet<T> set)
        {
            writer.Write(set.Count);
            foreach (T elem in set)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out Collection<T> set)
        {
            int cnt = reader.ReadInt32();
            set = new Collection<T>();
            for (int i = 0; i < cnt; i++)
            {
                set.Add(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, Collection<T> set)
        {
            writer.Write(set.Count);
            foreach (T elem in set)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out ReadOnlyCollection<T> set)
        {
            int cnt = reader.ReadInt32();
            List<T> lst = new List<T>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                lst.Add(serializer.Read(reader));
            }
            set = new ReadOnlyCollection<T>(lst);
        }

        public static void Write(HpcBinaryWriter writer, ReadOnlyCollection<T> set)
        {
            writer.Write(set.Count);
            foreach (T elem in set)
            {
                serializer.Write(writer, elem);
            }
        }
        
        public static void Read(HpcBinaryReader reader, out IEnumerable<T> seq)
        {
            int cnt = reader.ReadInt32();
            T[] elems = new T[cnt];
            for (int i = 0; i < cnt; i++)
            {
                elems[i] = serializer.Read(reader);
            }
            seq = new HpcLinqSequence<T>(elems);
        }

        public static void Write(HpcBinaryWriter writer, IEnumerable<T> seq)
        {
            writer.Write(seq.Count());
            foreach (T elem in seq)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out IList<T> seq)
        {
            int cnt = reader.ReadInt32();
            seq = new List<T>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                seq.Add(serializer.Read(reader));
            }
        }

        public static void Write(HpcBinaryWriter writer, IList<T> seq)
        {
            writer.Write(seq.Count);
            foreach (T elem in seq)
            {
                serializer.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out ForkValue<T> val)
        {
            val = new ForkValue<T>();
            if (reader.ReadBool())
            {
                val.Value = serializer.Read(reader);
            }
        }

        public static void Write(HpcBinaryWriter writer, ForkValue<T> val)
        {
            writer.Write(val.HasValue);
            if (val.HasValue)
            {
                serializer.Write(writer, val.Value);
            }
        }

        public static void Read(HpcBinaryReader reader, out AggregateValue<T> aggVal)
        {
            long cnt = reader.ReadInt64();
            T val = default(T);
            if (cnt > 0)
            {
                val = serializer.Read(reader);
            }
            aggVal = new AggregateValue<T>(val, cnt);
        }

        public static void Write(HpcBinaryWriter writer, AggregateValue<T> aggVal)
        {
            writer.Write(aggVal.Count);
            if (aggVal.Count > 0)
            {
                serializer.Write(writer, aggVal.Value);
            }
        }

        public static void Read(HpcBinaryReader reader, out IndexedValue<T> indexedVal)
        {
            int index = reader.ReadInt32();
            T val = serializer.Read(reader);
            indexedVal = new IndexedValue<T>(index, val);
        }

        public static void Write(HpcBinaryWriter writer, IndexedValue<T> indexedVal)
        {
            writer.Write(indexedVal.Index);
            serializer.Write(writer, indexedVal.Value);
        }
    }

    public static class HpcSerialization<T1, T2, S1, S2>
        where S1 : HpcSerializer<T1>, new()
        where S2 : HpcSerializer<T2>, new()
    {
        private static S1 serializer1 = new S1();
        private static S2 serializer2 = new S2();
        
        public static void Read(HpcBinaryReader reader, out Dictionary<T1, T2> dict)
        {
            int cnt = reader.ReadInt32();
            dict = new Dictionary<T1, T2>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                T1 key = serializer1.Read(reader);
                T2 val = serializer2.Read(reader);
                dict.Add(key, val);
            }
        }

        public static void Write(HpcBinaryWriter writer, Dictionary<T1, T2> dict)
        {
            writer.Write(dict.Count);
            foreach (KeyValuePair<T1, T2> elem in dict)
            {
                serializer1.Write(writer, elem.Key);
                serializer2.Write(writer, elem.Value);                
            }
        }

        public static void Read(HpcBinaryReader reader, out SortedDictionary<T1, T2> dict)
        {
            int cnt = reader.ReadInt32();
            dict = new SortedDictionary<T1, T2>();
            for (int i = 0; i < cnt; i++)
            {
                T1 key = serializer1.Read(reader);
                T2 val = serializer2.Read(reader);
                dict.Add(key, val);
            }
        }

        public static void Write(HpcBinaryWriter writer, SortedDictionary<T1, T2> dict)
        {
            writer.Write(dict.Count);
            foreach (KeyValuePair<T1, T2> elem in dict)
            {
                serializer1.Write(writer, elem.Key);
                serializer2.Write(writer, elem.Value);
            }
        }

        public static void Read(HpcBinaryReader reader, out SortedList<T1, T2> list)
        {
            int cnt = reader.ReadInt32();
            list = new SortedList<T1, T2>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                T1 key = serializer1.Read(reader);
                T2 value = serializer2.Read(reader);
                list.Add(key, value);
            }
        }

        public static void Write(HpcBinaryWriter writer, SortedList<T1, T2> list)
        {
            writer.Write(list.Count);
            foreach (KeyValuePair<T1, T2> elem in list)
            {
                serializer1.Write(writer, elem.Key);
                serializer2.Write(writer, elem.Value);
            }
        }

        public static void Read(HpcBinaryReader reader, out IGrouping<T1, T2> group)
        {
            T1 key = serializer1.Read(reader);
            int len = reader.ReadInt32();
            Grouping<T1, T2> realGroup = new Grouping<T1, T2>(key, len);

            for (int i = 0; i < len; i++)
            {
                realGroup.AddItem(serializer2.Read(reader));
            }
            group = realGroup;
        }

        public static void Write(HpcBinaryWriter writer, IGrouping<T1, T2> group)
        {
            serializer1.Write(writer, group.Key);
            writer.Write(group.Count());

            foreach (T2 elem in group)
            {
                serializer2.Write(writer, elem);
            }
        }

        public static void Read(HpcBinaryReader reader, out KeyValuePair<T1, T2> kv)
        {
            T1 key = serializer1.Read(reader);
            T2 val = serializer2.Read(reader);
            kv = new KeyValuePair<T1, T2>(key, val);
        }

        public static void Write(HpcBinaryWriter writer, KeyValuePair<T1, T2> kv)
        {
            serializer1.Write(writer, kv.Key);
            serializer2.Write(writer, kv.Value);
        }

        public static void Read(HpcBinaryReader reader, out Pair<T1, T2> pair)
        {
            T1 x = serializer1.Read(reader);
            T2 y = serializer2.Read(reader);
            pair = new Pair<T1, T2>(x, y);
        }

        public static void Write(HpcBinaryWriter writer, Pair<T1, T2> pair)
        {
            serializer1.Write(writer, pair.Key);
            serializer2.Write(writer, pair.Value);
        }

        public static void Read(HpcBinaryReader reader, out ForkTuple<T1, T2> val)
        {
            val = new ForkTuple<T1, T2>();
            if (reader.ReadBool())
            {
                val.First = serializer1.Read(reader);
            }
            if (reader.ReadBool())
            {
                val.Second = serializer2.Read(reader);
            }
        }
        
        public static void Write(HpcBinaryWriter writer, ForkTuple<T1, T2> val)
        {
            writer.Write(val.HasFirst);
            if (val.HasFirst)
            {
                serializer1.Write(writer, val.First);
            }
        
            writer.Write(val.HasSecond);
            if (val.HasSecond)
            {
                serializer2.Write(writer, val.Second);
            }
        }

        public static void Read(HpcBinaryReader reader, out HpcLinqGrouping<T1, T2> group)
        {
            T1 key = serializer1.Read(reader);
            int cnt = reader.ReadInt32();
            T2[] elems = new T2[cnt];
            for (int i = 0; i < cnt; i++)
            {
                elems[i] = serializer2.Read(reader);
            }
            group = new HpcLinqGrouping<T1, T2>(key, elems);
        }

        public static void Write(HpcBinaryWriter writer, HpcLinqGrouping<T1, T2> group)
        {
            serializer1.Write(writer, group.Key);
            writer.Write(group.Count());
            foreach (T2 elem in group)
            {
                serializer2.Write(writer, elem);
            }
        }
    }

    public sealed class ByteHpcSerializer : HpcSerializer<byte>
    {
        public override byte Read(HpcBinaryReader reader)
        {
            return reader.ReadUByte();
        }

        public override void Write(HpcBinaryWriter writer, byte x)
        {
            writer.Write(x);
        }
    }

    public sealed class SByteHpcSerializer : HpcSerializer<sbyte>
    {
        public override sbyte Read(HpcBinaryReader reader)
        {
            return reader.ReadSByte();
        }

        public override void Write(HpcBinaryWriter writer, sbyte x)
        {
            writer.Write(x);
        }
    }

    public sealed class BoolHpcSerializer : HpcSerializer<bool>
    {
        public override bool Read(HpcBinaryReader reader)
        {
            return reader.ReadBool();
        }

        public override void Write(HpcBinaryWriter writer, bool x)
        {
            writer.Write(x);
        }
    }

    public sealed class CharHpcSerializer : HpcSerializer<char>
    {
        public override char Read(HpcBinaryReader reader)
        {
            return reader.ReadChar();
        }

        public override void Write(HpcBinaryWriter writer, char x)
        {
            writer.Write(x);
        }
    }

    public sealed class Int16HpcSerializer : HpcSerializer<Int16>
    {
        public override Int16 Read(HpcBinaryReader reader)
        {
            return reader.ReadInt16();
        }

        public override void Write(HpcBinaryWriter writer, Int16 x)
        {
            writer.Write(x);
        }
    }

    public sealed class UInt16HpcSerializer : HpcSerializer<UInt16>
    {
        public override UInt16 Read(HpcBinaryReader reader)
        {
            return reader.ReadUInt16();
        }

        public override void Write(HpcBinaryWriter writer, UInt16 x)
        {
            writer.Write(x);
        }
    }

    public sealed class Int32HpcSerializer : HpcSerializer<Int32>
    {
        public override Int32 Read(HpcBinaryReader reader)
        {
            return reader.ReadInt32();
        }

        public override void Write(HpcBinaryWriter writer, Int32 x)
        {
            writer.Write(x);
        }
    }

    public sealed class UInt32HpcSerializer : HpcSerializer<UInt32>
    {
        public override UInt32 Read(HpcBinaryReader reader)
        {
            return reader.ReadUInt32();
        }

        public override void Write(HpcBinaryWriter writer, UInt32 x)
        {
            writer.Write(x);
        }
    }

    public sealed class Int64HpcSerializer : HpcSerializer<Int64>
    {
        public override Int64 Read(HpcBinaryReader reader)
        {
            return reader.ReadInt64();
        }

        public override void Write(HpcBinaryWriter writer, Int64 x)
        {
            writer.Write(x);
        }
    }

    public sealed class UInt64HpcSerializer : HpcSerializer<UInt64>
    {
        public override UInt64 Read(HpcBinaryReader reader)
        {
            return reader.ReadUInt64();
        }

        public override void Write(HpcBinaryWriter writer, UInt64 x)
        {
            writer.Write(x);
        }
    }

    public sealed class SingleHpcSerializer : HpcSerializer<float>
    {
        public override float Read(HpcBinaryReader reader)
        {
            return reader.ReadSingle();
        }

        public override void Write(HpcBinaryWriter writer, float x)
        {
            writer.Write(x);
        }
    }

    public sealed class DoubleHpcSerializer : HpcSerializer<double>
    {
        public override double Read(HpcBinaryReader reader)
        {
            return reader.ReadDouble();
        }

        public override void Write(HpcBinaryWriter writer, double x)
        {
            writer.Write(x);
        }
    }

    public sealed class DecimalHpcSerializer : HpcSerializer<decimal>
    {
        public override decimal Read(HpcBinaryReader reader)
        {
            return reader.ReadDecimal();
        }

        public override void Write(HpcBinaryWriter writer, decimal x)
        {
            writer.Write(x);
        }
    }

    public sealed class DateTimeHpcSerializer : HpcSerializer<DateTime>
    {
        public override DateTime Read(HpcBinaryReader reader)
        {
            return reader.ReadDateTime();
        }

        public override void Write(HpcBinaryWriter writer, DateTime x)
        {
            writer.Write(x);
        }
    }

    public sealed class StringHpcSerializer : HpcSerializer<string>
    {
        public override string Read(HpcBinaryReader reader)
        {
            return reader.ReadString();
        }

        public override void Write(HpcBinaryWriter writer, string x)
        {
            writer.Write(x);
        }
    }

    public sealed class GuidHpcSerializer : HpcSerializer<Guid>
    {
        public override Guid Read(HpcBinaryReader reader)
        {
            return reader.ReadGuid();
        }

        public override void Write(HpcBinaryWriter writer, Guid x)
        {
            writer.Write(x);
        }
    }


    public sealed class SqlDateTimeHpcSerializer : HpcSerializer<SqlDateTime>
    {
        public override SqlDateTime Read(HpcBinaryReader reader)
        {
            return reader.ReadSqlDateTime();
        }

        public override void Write(HpcBinaryWriter writer, SqlDateTime x)
        {
            writer.Write(x);
        }
    }
}
