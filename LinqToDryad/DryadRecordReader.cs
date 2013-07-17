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
using System.Text;
using System.Reflection;
using System.Threading;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    // This class defines the abstraction of reading Dryad records.
    public abstract class HpcRecordReader<T> : IEnumerable<T>
    {
        private const int bufferMaxSize = 1024; //@@TODO: it would be good to choose a buffersize based on record-sizes.. as done in HpcRecordWriter.
        private T[] m_buffer1;
        private T[] m_buffer2;
        private int m_count1;
        private int m_count2;
        private int m_index1;
        private Thread m_worker;
        private Exception m_workerException;
        private long m_numRecordsRead;
        protected bool m_isUsed;

        public HpcRecordReader()
        {
            this.m_isUsed = false;
            this.m_numRecordsRead = 0;
            this.FirstReadTime = DateTime.Now;
            this.LastReadTime = this.FirstReadTime;            
        }

        protected abstract bool ReadRecord(ref T rec); // simple synchronous non-buffering read of a record
        public abstract Int64 GetTotalLength();
        public abstract string GetChannelURI();

        public virtual void Close()
        {
            if (this.m_worker != null)
            {
                lock (this)
                {
                    this.m_count2 = -2;
                    Monitor.Pulse(this);
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
        public long RecordsRead { get { return m_numRecordsRead; } }

        //Note: direct use of this method (rather than going through the enumerator)
        //      will miss the checks we do to prevent repeat enumeration.  Either add those manually
        //      or go through the enumerator.
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
                this.m_buffer1 = new T[bufferMaxSize];
                this.m_buffer2 = new T[bufferMaxSize];
                this.m_count1 = 0;
                this.m_index1 = this.m_buffer1.Length;
                this.m_count2 = -1;
                this.m_worker = new Thread(this.FillBuffer);
                this.m_worker.Start();
            }
        }
        
        private void FillBuffer()
        {
            DryadLinqLog.Add("HpcRecordReader reader thread started.  ThreadId=" + Thread.CurrentThread.ManagedThreadId);
            lock (this)
            {
                while (true)
                {
                    try
                    {
                        while (this.m_count2 > 0)
                        {
                            Monitor.Wait(this);
                        }
                        if (this.m_count2 == -2) return;
                        this.m_count2 = 0;
                        while (this.m_count2 < this.m_buffer2.Length &&
                               this.ReadRecord(ref this.m_buffer2[this.m_count2]))
                        {
                            this.m_count2++;
                        }
                        Monitor.Pulse(this);
                        if (this.m_count2 < this.m_buffer2.Length) return;
                    }
                    catch (Exception e)
                    {
                        this.m_workerException = e;
                        Monitor.Pulse(this);
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
            lock (this)
            {
                while (this.m_count2 == -1)
                {
                    Monitor.Wait(this);
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
                Monitor.Pulse(this);
            }
            return this.ReadRecordAsync(ref rec);
        }

        internal void AddLogEntry()
        {
            if (this.LastReadTime == this.FirstReadTime)
            {
                this.LastReadTime = DateTime.Now;
            }
            DryadLinqLog.Add("Read {0} records from {1} from {2} to {3} ",
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
                throw new DryadLinqException(HpcLinqErrorCode.ChannelCannotBeReadMoreThanOnce,
                                           SR.ChannelCannotBeReadMoreThanOnce);
            }
            this.m_isUsed = true;

            return new RecordEnumerator(this);
        }

        // Internal enumerator class
        private class RecordEnumerator : IEnumerator<T>
        {
            private HpcRecordReader<T> m_reader;
            private T m_current;
            
            public RecordEnumerator(HpcRecordReader<T> reader)
            {
                this.m_reader = reader;
                this.m_current = default(T);
            }

            public bool MoveNext()
            {
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

    public sealed class HpcRecordTextReader : HpcRecordReader<LineRecord>
    {
        private HpcTextReader m_reader;

        public HpcRecordTextReader(HpcTextReader reader)
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
    
    public unsafe abstract class HpcRecordBinaryReader<T> : HpcRecordReader<T>
    {
        protected HpcBinaryReader m_reader;

        public HpcRecordBinaryReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordByteReader : HpcRecordBinaryReader<byte>
    {
        public HpcRecordByteReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordSByteReader : HpcRecordBinaryReader<sbyte>
    {
        public HpcRecordSByteReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordBoolReader : HpcRecordBinaryReader<bool>
    {
        public HpcRecordBoolReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordCharReader : HpcRecordBinaryReader<char>
    {
        public HpcRecordCharReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordShortReader : HpcRecordBinaryReader<short>
    {
        public HpcRecordShortReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordUShortReader : HpcRecordBinaryReader<ushort>
    {
        public HpcRecordUShortReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordInt32Reader : HpcRecordBinaryReader<int>
    {
        public HpcRecordInt32Reader(HpcBinaryReader reader)
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

    public sealed class HpcRecordUInt32Reader : HpcRecordBinaryReader<uint>
    {
        public HpcRecordUInt32Reader(HpcBinaryReader reader)
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

    public sealed class HpcRecordInt64Reader : HpcRecordBinaryReader<long>
    {
        public HpcRecordInt64Reader(HpcBinaryReader reader)
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

    public sealed class HpcRecordUInt64Reader : HpcRecordBinaryReader<ulong>
    {
        public HpcRecordUInt64Reader(HpcBinaryReader reader)
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

    public sealed class HpcRecordFloatReader : HpcRecordBinaryReader<float>
    {
        public HpcRecordFloatReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordDecimalReader : HpcRecordBinaryReader<decimal>
    {
        public HpcRecordDecimalReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordDoubleReader : HpcRecordBinaryReader<double>
    {
        public HpcRecordDoubleReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordDateTimeReader : HpcRecordBinaryReader<DateTime>
    {
        public HpcRecordDateTimeReader(HpcBinaryReader reader)
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
    
    public sealed class HpcRecordStringReader : HpcRecordBinaryReader<string>
    {
        public HpcRecordStringReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordSqlDateTimeReader : HpcRecordBinaryReader<SqlDateTime>
    {
        public HpcRecordSqlDateTimeReader(HpcBinaryReader reader)
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

    public sealed class HpcRecordGuidReader : HpcRecordBinaryReader<Guid>
    {
        public HpcRecordGuidReader(HpcBinaryReader reader)
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
