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
using System.Globalization;
using System.Reflection;
using System.Linq.Expressions;
using System.Linq;
using Microsoft.Research.DryadLinq;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// The class encapsulates the external environment in which a managed
    /// DryadLINQ vertex writes to output channels.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <remarks>A DryadLINQ user should not use DryadLinqVertexWriter directly.</remarks>
    public class DryadLinqVertexWriter<T>
    {
        private VertexEnv m_dvertexEnv;
        private IntPtr m_nativeHandle;
        private UInt32 m_startPort;
        private UInt32 m_numberOfOutputs;
        private DryadLinqFactory<T> m_writerFactory;
        private DryadLinqRecordWriter<T>[] m_writers;

        public DryadLinqVertexWriter(VertexEnv denv, DryadLinqFactory<T> writerFactory, UInt32 startPort, UInt32 endPort)
        {
            this.m_dvertexEnv = denv;
            this.m_nativeHandle = denv.NativeHandle;
            this.m_startPort = startPort;
            this.m_numberOfOutputs = endPort - startPort;
            this.m_writerFactory = writerFactory;
            this.m_writers = new DryadLinqRecordWriter<T>[this.m_numberOfOutputs];
            Int32 buffSize = this.m_dvertexEnv.GetWriteBuffSize();            
            for (UInt32 i = 0; i < this.m_numberOfOutputs; i++)
            {
                this.m_writers[i] = writerFactory.MakeWriter(this.m_nativeHandle, i + startPort, buffSize);
            }
        }

        public DryadLinqVertexWriter(VertexEnv denv, DryadLinqFactory<T> writerFactory, UInt32 portNum)
        {
            this.m_dvertexEnv = denv;
            this.m_nativeHandle = denv.NativeHandle;
            this.m_startPort = portNum;
            this.m_numberOfOutputs = 1;
            this.m_writerFactory = writerFactory;
            Int32 buffSize = this.m_dvertexEnv.GetWriteBuffSize();            
            DryadLinqRecordWriter<T> writer = writerFactory.MakeWriter(this.m_nativeHandle, portNum, buffSize);
            this.m_writers = new DryadLinqRecordWriter<T>[] { writer };
        }

        public VertexEnv VertexEnv
        {
            get { return this.m_dvertexEnv; }
        }

        public IntPtr NativeHandle
        {
            get { return this.m_nativeHandle; }
        }
        
        public UInt32 NumberOfOutputs
        {
            get { return this.m_numberOfOutputs; }
        }

        internal DryadLinqRecordWriter<T> GetWriter(UInt32 portNum)
        {
            return this.m_writers[portNum];
        }

        public void WriteItemSequence(IEnumerable<T> source)
        {
            DryadLinqRecordWriter<T> writer = this.m_writers[0];
            if (m_dvertexEnv.MultiThreading)
            {
                foreach (T item in source)
                {
                    writer.WriteRecordAsync(item);
                }
            }
            else
            {
                foreach (T item in source)
                {
                    writer.WriteRecordSync(item);
                }
            }
            this.CloseWriters();
        }

        // Write a single item to the output channel. Use sync write.
        internal void WriteItem(T item, Int32 portNum)
        {
            this.m_writers[portNum].WriteRecordSync(item);
        }

        public string GetChannelURI(int idx)
        {
            return this.m_writers[idx].GetChannelURI();
        }

        public Int64 GetChannelLength(int idx)
        {
            return this.m_writers[idx].GetTotalLength();
        }

        public UInt64 GetChannelFP(int idx)
        {
            return this.m_writers[idx].GetFingerPrint();
        }

        public void SetCalcFP(int idx)
        {
            this.m_writers[idx].CalcFP = true;
        }
        
        public Int64 GetTotalLength()
        {
            Int64 totalLen = 0;
            for (UInt32 i = 0; i < this.NumberOfOutputs; i++)
            {
                Int64 chLen = this.m_writers[i].GetTotalLength();
                if (chLen < 0) return -1;
                totalLen += chLen;
            }
            return totalLen;
        }
        
        internal Stream OutputStream
        {
            get {
                if (this.m_numberOfOutputs != 1)
                {
                    throw new InvalidOperationException();
                }
                NativeBlockStream nativeStream = new DryadLinqChannel(this.m_nativeHandle, this.m_startPort, false);
                return new DryadLinqBinaryWriterToStreamAdapter(new DryadLinqBinaryWriter(nativeStream));
            }
        }

        public void FlushWriters()
        {
            for (UInt32 i = 0; i < this.NumberOfOutputs; i++)
            {
                this.m_writers[i].Flush();
            }
        }

        public void CloseWriters()
        {
            for (UInt32 i = 0; i < this.NumberOfOutputs; i++)
            {
                this.m_writers[i].Close();
            }
        }
    }    
}
