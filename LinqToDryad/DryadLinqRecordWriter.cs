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
    // This class defines the abstraction of writing DryadLinq records.
    public unsafe abstract class DryadLinqRecordWriter<T>
    {
        private const int BufferMaxSize = 1024;
        private const int InitRecords = 100;

        private long m_numRecordsWritten;
        private T[] m_buffer1;
        private T[] m_buffer2;
        private int m_index1;
        private int m_count2;
        private bool m_isClosed;
        private Thread m_worker;
        private object m_lockObj = new Object();

        protected DryadLinqRecordWriter()
        {
            this.m_numRecordsWritten = 0;
            this.m_buffer1 = null;
            this.m_buffer2 = null;
            this.m_index1 = 0;
            this.m_count2 = -1;
            this.m_isClosed = false;
            this.m_worker = null;
        }

        protected abstract void WriteRecord(T rec);
        protected abstract void FlushInternal();
        protected abstract void CloseInternal();

        public abstract Int64 Length { get; }
        public abstract string GetChannelURI();
        public abstract Int64 GetTotalLength();
        public abstract UInt64 GetFingerPrint();
        public abstract bool CalcFP { get; set; }
        public abstract Int32 BufferSizeHint { get; }

        public void WriteRecordSync(T rec)
        {
            this.WriteRecord(rec);
            this.m_numRecordsWritten++;
        }

        // Called by DryadLinqVertexWrite.WriteItemSequence, DataProvider.IngressDirectlyToDsc etc.
        // Note: async writer thread will only be started after nRecords>InitRecords (default=100)
        public void WriteRecordAsync(T rec)
        {
            if (this.m_worker == null)
            {
                this.WriteRecord(rec);
                this.m_numRecordsWritten++;
                if (this.m_numRecordsWritten == InitRecords)
                {
                    // Decide if we want to use async and the buffer size   
                    Int32 bsize = (this.BufferSizeHint / (4 * (Int32)this.Length)) * InitRecords;
                    if (this.BufferSizeHint > (64 * BufferMaxSize) && bsize > 1)
                    {
                        bsize = Math.Min(bsize, BufferMaxSize);
                        this.m_buffer1 = new T[bsize];
                        this.m_buffer2 = new T[bsize];
                        this.m_index1 = 0;
                        this.m_count2 = -1;
                        this.m_isClosed = false;
                        this.m_worker = new Thread(this.WriteBuffer);
                        this.m_worker.Start();
                        DryadLinqLog.AddInfo("Async writer with buffer size {0}", bsize);
                    }
                }
            }
            else
            {
                if (this.m_index1 == this.m_buffer1.Length)
                {
                    lock (this.m_lockObj)
                    {
                        while (this.m_count2 != -1)
                        {
                            Monitor.Wait(this.m_lockObj);
                        }
                        T[] temp = this.m_buffer1;
                        this.m_buffer1 = this.m_buffer2;
                        this.m_buffer2 = temp;
                        this.m_count2 = this.m_index1;
                        this.m_index1 = 0;
                        Monitor.Pulse(this.m_lockObj);
                    }
                }
                this.m_buffer1[this.m_index1++] = rec;
            }
        }

        private void WriteBuffer()
        {
            try
            {
                while (true)
                {
                    lock (this.m_lockObj)
                    {
                        while (this.m_count2 == -1)
                        {
                            Monitor.Wait(this.m_lockObj);
                        }
                    }

                    // Write the records
                    for (int i = 0; i < this.m_count2; i++)
                    {
                        this.WriteRecord(this.m_buffer2[i]);
                    }
                    this.m_numRecordsWritten += this.m_count2;

                    lock (this.m_lockObj)
                    {
                        this.m_count2 = -1;
                        Monitor.Pulse(this.m_lockObj);

                        if (this.m_isClosed) break;
                    }
                }
            }
            catch (Exception e)
            {
                DryadLinqLog.AddInfo(e.ToString());
                throw;
            }
        }
        
        private void Flush(bool closeIt)
        {
            if (this.m_worker != null)
            {
                lock (this.m_lockObj)
                {
                    while (this.m_count2 != -1)
                    {
                        Monitor.Wait(this.m_lockObj);
                    }
                    T[] temp = this.m_buffer1;
                    this.m_buffer1 = this.m_buffer2;
                    this.m_buffer2 = temp;
                    this.m_count2 = this.m_index1;
                    this.m_index1 = 0;
                    this.m_isClosed = closeIt;
                    Monitor.Pulse(this.m_lockObj);

                    // Again, wait for the worker to complete
                    while (this.m_count2 != -1)
                    {
                        Monitor.Wait(this.m_lockObj);
                    }
                }
            }
            this.FlushInternal();
        }

        public void Flush()
        {
            this.Flush(false);
        }
        
        public void Close()
        {
            this.Flush(true);
            this.CloseInternal();
            DryadLinqLog.AddInfo("Wrote {0} records to {1}", this.m_numRecordsWritten, this.ToString());
        }
    }

    public sealed class DryadLinqRecordTextWriter : DryadLinqRecordWriter<LineRecord>
    {
        private DryadLinqTextWriter m_writer;

        public DryadLinqRecordTextWriter(DryadLinqTextWriter writer)
        {
            this.m_writer = writer;
        }

        protected override void WriteRecord(LineRecord rec)
        {
            this.m_writer.WriteLine(rec.Line);
        }

        public override Int64 Length
        {
            get { return this.m_writer.Length; }
        }
        
        public override string GetChannelURI()
        {
            return this.m_writer.GetChannelURI();
        }
        
        public override long GetTotalLength()
        {
            return this.m_writer.GetTotalLength();
        }

        public override UInt64 GetFingerPrint()
        {
            return this.m_writer.GetFingerPrint();
        }

        public override bool CalcFP
        {
            get { return this.m_writer.CalcFP; }
            set { this.m_writer.CalcFP = value; }
        }
        
        public override Int32 BufferSizeHint
        {
            get { return this.m_writer.BufferSizeHint; }
        }

        protected override void FlushInternal()
        {
            this.m_writer.Flush();
        }

        protected override void CloseInternal()
        {
            this.m_writer.Close();
        }

        public override string ToString()
        {
            return this.m_writer.ToString();
        }
    }

    public unsafe abstract class DryadLinqRecordBinaryWriter<T> : DryadLinqRecordWriter<T>
    {
        protected DryadLinqBinaryWriter m_writer;

        public DryadLinqRecordBinaryWriter(DryadLinqBinaryWriter writer)
        {
            this.m_writer = writer;
        }

        public DryadLinqBinaryWriter BinaryWriter
        {
            get { return this.m_writer; }
        }

        public override Int64 Length
        {
            get { return this.m_writer.Length; }
        }

        public override string GetChannelURI()
        {
            return this.m_writer.GetChannelURI();
        }
        
        public override long GetTotalLength()
        {
            return this.m_writer.GetTotalLength();
        }

        public override UInt64 GetFingerPrint()
        {
            return this.m_writer.GetFingerPrint();
        }

        public override bool CalcFP
        {
            get { return this.m_writer.CalcFP; }
            set { this.m_writer.CalcFP = value; }
        }

        public override Int32 BufferSizeHint
        {
            get { return this.m_writer.BufferSizeHint; }
        }

        // helper for generated vertex code to call m_writer.CompleteWriteRecord()
        public void CompleteWriteRecord()
        {
            this.m_writer.CompleteWriteRecord();
        }

        protected override void FlushInternal()
        {
            this.m_writer.Flush();
        }

        protected override void CloseInternal()
        {
            this.m_writer.Close();
        }

        public override string ToString()
        {
            return this.m_writer.ToString();
        }
    }

    public sealed class DryadLinqRecordByteWriter : DryadLinqRecordBinaryWriter<byte>
    {
        public DryadLinqRecordByteWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(byte rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }
    
    public sealed class DryadLinqRecordSByteWriter : DryadLinqRecordBinaryWriter<sbyte>
    {
        public DryadLinqRecordSByteWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(sbyte rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordBoolWriter : DryadLinqRecordBinaryWriter<bool>
    {
        public DryadLinqRecordBoolWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(bool rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordCharWriter : DryadLinqRecordBinaryWriter<char>
    {
        public DryadLinqRecordCharWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(char rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordShortWriter : DryadLinqRecordBinaryWriter<short>
    {
        public DryadLinqRecordShortWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(short rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordUShortWriter : DryadLinqRecordBinaryWriter<ushort>
    {
        public DryadLinqRecordUShortWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(ushort rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordInt32Writer : DryadLinqRecordBinaryWriter<int>
    {
        public DryadLinqRecordInt32Writer(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(int rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordUInt32Writer : DryadLinqRecordBinaryWriter<uint>
    {
        public DryadLinqRecordUInt32Writer(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(uint rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordInt64Writer : DryadLinqRecordBinaryWriter<long>
    {
        public DryadLinqRecordInt64Writer(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(long rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordUInt64Writer : DryadLinqRecordBinaryWriter<ulong>
    {
        public DryadLinqRecordUInt64Writer(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(ulong rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordDecimalWriter : DryadLinqRecordBinaryWriter<decimal>
    {
        public DryadLinqRecordDecimalWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(decimal rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordFloatWriter : DryadLinqRecordBinaryWriter<float>
    {
        public DryadLinqRecordFloatWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(float rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordDoubleWriter : DryadLinqRecordBinaryWriter<double>
    {
        public DryadLinqRecordDoubleWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(double rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordDateTimeWriter : DryadLinqRecordBinaryWriter<DateTime>
    {
        public DryadLinqRecordDateTimeWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(DateTime rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }
    
    public sealed class DryadLinqRecordStringWriter : DryadLinqRecordBinaryWriter<string>
    {
        public DryadLinqRecordStringWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(string rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordSqlDateTimeWriter : DryadLinqRecordBinaryWriter<SqlDateTime>
    {
        public DryadLinqRecordSqlDateTimeWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(SqlDateTime rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class DryadLinqRecordGuidWriter : DryadLinqRecordBinaryWriter<Guid>
    {
        public DryadLinqRecordGuidWriter(DryadLinqBinaryWriter writer)
            : base(writer)
        {
        }

        protected override void WriteRecord(Guid rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }
}
