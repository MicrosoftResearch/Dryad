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
    // This class defines the abstraction of writing HcLinq records.
    public unsafe abstract class HpcRecordWriter<T>
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
        
        public HpcRecordWriter()
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

        //Called by HpcVertexWrite.WriteItemSequence, DataProvider.IngressDirectlyToDsc etc.
        //Note: async writer thread will only be started after nRecords>InitRecords (default=100)
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
                        DryadLinqLog.Add("Async writer with buffer size {0}", bsize);
                    }
                }
            }
            else
            {
                if (this.m_index1 == this.m_buffer1.Length)
                {
                    lock (this)
                    {
                        while (this.m_count2 != -1)
                        {
                            Monitor.Wait(this);
                        }
                        T[] temp = this.m_buffer1;
                        this.m_buffer1 = this.m_buffer2;
                        this.m_buffer2 = temp;
                        this.m_count2 = this.m_index1;
                        this.m_index1 = 0;
                        Monitor.Pulse(this); 
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
                    lock (this)
                    {
                        while (this.m_count2 == -1)
                        {
                            Monitor.Wait(this);
                        }
                    }

                    // Write the records
                    for (int i = 0; i < this.m_count2; i++)
                    {
                        this.WriteRecord(this.m_buffer2[i]);
                    }
                    this.m_numRecordsWritten += this.m_count2;

                    lock (this)
                    {
                        this.m_count2 = -1;
                        Monitor.Pulse(this);

                        if (this.m_isClosed) break;
                    }
                }
            }
            catch (Exception e)
            {
                DryadLinqLog.Add(true, e.ToString());
                throw;
            }
        }
        
        private void Flush(bool closeIt)
        {
            if (this.m_worker != null)
            {
                lock (this)
                {
                    while (this.m_count2 != -1)
                    {
                        Monitor.Wait(this);
                    }
                    T[] temp = this.m_buffer1;
                    this.m_buffer1 = this.m_buffer2;
                    this.m_buffer2 = temp;
                    this.m_count2 = this.m_index1;
                    this.m_index1 = 0;
                    this.m_isClosed = closeIt;
                    Monitor.Pulse(this);

                    // Again, wait for the worker to complete
                    while (this.m_count2 != -1)
                    {
                        Monitor.Wait(this);
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
            DryadLinqLog.Add("Wrote {0} records to {1}", this.m_numRecordsWritten, this.ToString());
        }
    }

    public sealed class HpcRecordTextWriter : HpcRecordWriter<LineRecord>
    {
        private HpcTextWriter m_writer;

        public HpcRecordTextWriter(HpcTextWriter writer)
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

    public unsafe abstract class HpcRecordBinaryWriter<T> : HpcRecordWriter<T>
    {
        protected HpcBinaryWriter m_writer;

        public HpcRecordBinaryWriter(HpcBinaryWriter writer)
        {
            this.m_writer = writer;
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
            m_writer.CompleteWriteRecord();
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

    public sealed class HpcRecordByteWriter : HpcRecordBinaryWriter<byte>
    {
        public HpcRecordByteWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(byte rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }
    
    public sealed class HpcRecordSByteWriter : HpcRecordBinaryWriter<sbyte>
    {
        public HpcRecordSByteWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(sbyte rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordBoolWriter : HpcRecordBinaryWriter<bool>
    {
        public HpcRecordBoolWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(bool rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordCharWriter : HpcRecordBinaryWriter<char>
    {
        public HpcRecordCharWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(char rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordShortWriter : HpcRecordBinaryWriter<short>
    {
        public HpcRecordShortWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(short rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordUShortWriter : HpcRecordBinaryWriter<ushort>
    {
        public HpcRecordUShortWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(ushort rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordInt32Writer : HpcRecordBinaryWriter<int>
    {
        public HpcRecordInt32Writer(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(int rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordUInt32Writer : HpcRecordBinaryWriter<uint>
    {
        public HpcRecordUInt32Writer(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(uint rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordInt64Writer : HpcRecordBinaryWriter<long>
    {
        public HpcRecordInt64Writer(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(long rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordUInt64Writer : HpcRecordBinaryWriter<ulong>
    {
        public HpcRecordUInt64Writer(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(ulong rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordDecimalWriter : HpcRecordBinaryWriter<decimal>
    {
        public HpcRecordDecimalWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(decimal rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordFloatWriter : HpcRecordBinaryWriter<float>
    {
        public HpcRecordFloatWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(float rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordDoubleWriter : HpcRecordBinaryWriter<double>
    {
        public HpcRecordDoubleWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(double rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordDateTimeWriter : HpcRecordBinaryWriter<DateTime>
    {
        public HpcRecordDateTimeWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(DateTime rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }
    
    public sealed class HpcRecordStringWriter : HpcRecordBinaryWriter<string>
    {
        public HpcRecordStringWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(string rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordSqlDateTimeWriter : HpcRecordBinaryWriter<SqlDateTime>
    {
        public HpcRecordSqlDateTimeWriter(HpcBinaryWriter writer)
            : base(writer)
        {
        }
        
        protected override void WriteRecord(SqlDateTime rec)
        {
            this.m_writer.Write(rec);
            this.m_writer.CompleteWriteRecord();
        }
    }

    public sealed class HpcRecordGuidWriter : HpcRecordBinaryWriter<Guid>
    {
        public HpcRecordGuidWriter(HpcBinaryWriter writer)
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
