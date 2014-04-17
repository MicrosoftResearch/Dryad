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
using System.Text;
using System.Reflection;
using System.Threading;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    // This class defines the abstraction of reading Dryad records.
    public abstract class DryadLinqRecordReader<T> : IEnumerable<T>
    {
        private const int BufferMaxSize = 1024;
        
        private T[] m_buffer1;
        private T[] m_buffer2;
        private int m_count1;
        private int m_count2;
        private int m_index1;
        private object m_bufLck = new object();
        private Thread m_worker;
        private Exception m_workerException;
        private long m_numRecordsRead;
        protected bool m_isUsed;

        public DryadLinqRecordReader()
        {
            this.m_isUsed = false;
            this.m_numRecordsRead = 0;
            this.FirstReadTime = DateTime.Now;
            this.LastReadTime = this.FirstReadTime;
        }

        protected abstract bool ReadRecord(ref T rec);
        public abstract Int64 GetTotalLength();
        public abstract string GetChannelURI();

        public virtual void Close()
        {
            if (this.m_worker != null)
            {
                lock (this.m_bufLck)
                {
                    this.m_count2 = -2;
                    Monitor.Pulse(this.m_bufLck);
                }
            }
        }

        /// <summary>
        /// Time when first record was read from the channel.
        /// </summary>
        public DateTime FirstReadTime { get; protected set; }

        /// <summary>
        /// Time when last record was read from the channel.
        /// </summary>
        public DateTime LastReadTime { get; protected set; }

        /// <summary>
        /// The number of records read from this channel.
        /// </summary>
        public long RecordsRead { get { return this.m_numRecordsRead; } }

        // Note: direct use of this method (rather than going through the enumerator)
        //      will miss the checks we do to prevent repeat enumeration.
        //      Either add those manually or go through the enumerator.
        public bool ReadRecordSync(ref T rec)
        {
            bool isRead = this.ReadRecord(ref rec);
            if (isRead)
            {
                this.m_numRecordsRead++;
            }
            else
            {
                this.LastReadTime = DateTime.Now;
            }
            return isRead;
        }

        internal void StartWorker()
        {
            if (this.m_worker == null)
            {
                this.m_buffer1 = new T[BufferMaxSize];
                this.m_buffer2 = new T[BufferMaxSize];
                this.m_count1 = 0;
                this.m_index1 = this.m_buffer1.Length;
                this.m_count2 = -1;
                this.m_worker = new Thread(this.FillBuffer);
                this.m_worker.Name = "DryadLinqRecordReader";
                this.m_worker.Start();
            }
        }

        private void FillBuffer()
        {
            DryadLinqLog.AddInfo("DryadLinqRecordReader reader thread started.  ThreadId=" +
                                 Thread.CurrentThread.ManagedThreadId);
            lock (this.m_bufLck)
            {
                while (true)
                {
                    try
                    {
                        while (this.m_count2 > 0)
                        {
                            Monitor.Wait(this.m_bufLck);
                        }
                        if (this.m_count2 == -2) return;
                        this.m_count2 = 0;
                        while (this.m_count2 < this.m_buffer2.Length &&
                               this.ReadRecord(ref this.m_buffer2[this.m_count2]))
                        {
                            this.m_count2++;
                        }
                        Monitor.Pulse(this.m_bufLck);
                        if (this.m_count2 < this.m_buffer2.Length)
                        {
                            return;
                        }
                    }
                    catch (Exception e)
                    {
                        this.m_workerException = e;
                        Monitor.Pulse(this.m_bufLck);
                        return;
                    }
                }
            }
        }

        internal bool ReadRecordAsync(ref T rec)
        {
            if (this.m_index1 < this.m_count1)
            {
                rec = this.m_buffer1[this.m_index1++];
                this.m_numRecordsRead++;
                return true;
            }
            if (this.m_index1 < this.m_buffer1.Length)
            {
                this.LastReadTime = DateTime.Now;
                return false;
            }
            lock (this.m_bufLck)
            {
                while (this.m_count2 == -1)
                {
                    Monitor.Wait(this.m_bufLck);
                }
                if (this.m_count2 == -2) return false;
                if (this.m_workerException != null)
                {
                    throw this.m_workerException;
                }
                T[] temp = this.m_buffer1;
                this.m_buffer1 = this.m_buffer2;
                this.m_buffer2 = temp;
                this.m_count1 = this.m_count2;
                this.m_index1 = 0;
                this.m_count2 = -1;
                Monitor.Pulse(this.m_bufLck);
            }
            return this.ReadRecordAsync(ref rec);
        }

        internal void AddLogEntry()
        {
            if (this.LastReadTime == this.FirstReadTime)
            {
                this.LastReadTime = DateTime.Now;
            }
            DryadLinqLog.AddInfo("Read {0} records from {1} from {2} to {3} ",
                                 this.RecordsRead,
                                 this.ToString(),
                                 this.FirstReadTime.ToString("MM/dd/yyyy HH:mm:ss.fff"),
                                 this.LastReadTime.ToString("MM/dd/yyyy HH:mm:ss.fff"));
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (this.m_isUsed)
            {
                throw new DryadLinqException(DryadLinqErrorCode.ChannelCannotBeReadMoreThanOnce,
                                             SR.ChannelCannotBeReadMoreThanOnce);
            }
            this.m_isUsed = true;

            return new RecordEnumerator(this);
        }

        // Internal enumerator class
        private class RecordEnumerator : IEnumerator<T>
        {
            private DryadLinqRecordReader<T> m_reader;
            private T m_current;
            
            public RecordEnumerator(DryadLinqRecordReader<T> reader)
            {
                this.m_reader = reader;
                this.m_current = default(T);
            }

            public bool MoveNext()
            {
                this.m_reader.m_numRecordsRead++;
                return this.m_reader.ReadRecord(ref this.m_current);
            }

            object IEnumerator.Current
            {
                get { return this.m_current; }
            }
            
            public T Current
            {
                get { return this.m_current; }
            }

            public void Reset()
            {
                throw new InvalidOperationException();
            }

            void IDisposable.Dispose()
            {
            }
        }
    }

    public sealed class DryadLinqRecordTextReader : DryadLinqRecordReader<LineRecord>
    {
        private DryadLinqTextReader m_reader;

        public DryadLinqRecordTextReader(DryadLinqTextReader reader)
        {
            this.m_reader = reader;
        }
        
        protected override bool ReadRecord(ref LineRecord rec)
        {
            string line = this.m_reader.ReadLine();
            if (line != null)
            {
                rec.Line = line;
                return true;
            }
            return false;
        }

        public override Int64 GetTotalLength()
        {
            return this.m_reader.GetTotalLength();
        }

        public override string GetChannelURI()
        {
            return this.m_reader.GetChannelURI();
        }
        
        public override void Close()
        {
            this.AddLogEntry();
            base.Close();
            this.m_reader.Close();
        }

        public override string ToString()
        {
            return this.m_reader.ToString();
        }
    }
    
    public unsafe abstract class DryadLinqRecordBinaryReader<T> : DryadLinqRecordReader<T>
    {
        protected DryadLinqBinaryReader m_reader;

        public DryadLinqRecordBinaryReader(DryadLinqBinaryReader reader)
        {
            this.m_reader = reader;
        }

        // entry point needed for generated vertex code
        public bool IsReaderAtEndOfStream()
        {
            return m_reader.EndOfStream();
        }

        public override Int64 GetTotalLength()
        {
            return this.m_reader.GetTotalLength();
        }

        public override string GetChannelURI()
        {
            return this.m_reader.GetChannelURI();
        }

        public override void Close()
        {
            this.AddLogEntry();
            base.Close();
            this.m_reader.Close();
        }

        public override string ToString()
        {
            return this.m_reader.ToString();
        }
    }

    public sealed class DryadLinqRecordByteReader : DryadLinqRecordBinaryReader<byte>
    {
        public DryadLinqRecordByteReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref byte rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadUByte();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordSByteReader : DryadLinqRecordBinaryReader<sbyte>
    {
        public DryadLinqRecordSByteReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref sbyte rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadSByte();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordBoolReader : DryadLinqRecordBinaryReader<bool>
    {
        public DryadLinqRecordBoolReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref bool rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadBool();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordCharReader : DryadLinqRecordBinaryReader<char>
    {
        public DryadLinqRecordCharReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref char rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadChar();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordShortReader : DryadLinqRecordBinaryReader<short>
    {
        public DryadLinqRecordShortReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref short rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadInt16();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordUShortReader : DryadLinqRecordBinaryReader<ushort>
    {
        public DryadLinqRecordUShortReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref ushort rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadUInt16();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordInt32Reader : DryadLinqRecordBinaryReader<int>
    {
        public DryadLinqRecordInt32Reader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref int rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadInt32();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordUInt32Reader : DryadLinqRecordBinaryReader<uint>
    {
        public DryadLinqRecordUInt32Reader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref uint rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadUInt32();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordInt64Reader : DryadLinqRecordBinaryReader<long>
    {
        public DryadLinqRecordInt64Reader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref long rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadInt64();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordUInt64Reader : DryadLinqRecordBinaryReader<ulong>
    {
        public DryadLinqRecordUInt64Reader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref ulong rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadUInt64();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordFloatReader : DryadLinqRecordBinaryReader<float>
    {
        public DryadLinqRecordFloatReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref float rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadSingle();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordDecimalReader : DryadLinqRecordBinaryReader<decimal>
    {
        public DryadLinqRecordDecimalReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref decimal rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadDecimal();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordDoubleReader : DryadLinqRecordBinaryReader<double>
    {
        public DryadLinqRecordDoubleReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref double rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadDouble();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordDateTimeReader : DryadLinqRecordBinaryReader<DateTime>
    {
        public DryadLinqRecordDateTimeReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref DateTime rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadDateTime();
                return true;
            }
            return false;
        }
    }
    
    public sealed class DryadLinqRecordStringReader : DryadLinqRecordBinaryReader<string>
    {
        public DryadLinqRecordStringReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref string rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadString();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadLinqRecordSqlDateTimeReader : DryadLinqRecordBinaryReader<SqlDateTime>
    {
        public DryadLinqRecordSqlDateTimeReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref SqlDateTime rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadSqlDateTime();
                return true;
            }
            return false;
        }
    }

    public sealed class DryadRecordGuidReader : DryadLinqRecordBinaryReader<Guid>
    {
        public DryadRecordGuidReader(DryadLinqBinaryReader reader)
            : base(reader)
        {
        }

        protected override bool ReadRecord(ref Guid rec)
        {
            if (!this.m_reader.EndOfStream())
            {
                rec = this.m_reader.ReadGuid();
                return true;
            }
            return false;
        }
    }
}
