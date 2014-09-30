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
using Microsoft.Research.DryadLinq;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// The class encapsulates the external environment in which a managed
    /// DryadLINQ vertex reads from input channels.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <remarks>A DryadLINQ user should not use DryadLinqVertexWriter directly.</remarks>
    public class DryadLinqVertexReader<T> : IMultiEnumerable<T>
    {
        private VertexEnv m_dvertexEnv;
        private IntPtr m_nativeHandle;
        private DryadLinqFactory<T> m_readerFactory;
        private UInt32 m_startPort;
        private UInt32 m_numberOfInputs;
        private bool m_keepInputPortOrder;
        internal DryadLinqRecordReader<T>[] m_readers;
        internal UInt32[] m_portPermArray;
        private bool m_isUsed;
        
        public DryadLinqVertexReader(VertexEnv denv,
                                     DryadLinqFactory<T> readerFactory,
                                     UInt32 startPort,
                                     UInt32 endPort,
                                     bool keepInputPortOrder)
        {
            this.m_dvertexEnv = denv;
            this.m_nativeHandle = denv.NativeHandle;
            this.m_readerFactory = readerFactory;
            this.m_startPort = startPort;
            this.m_numberOfInputs = endPort - startPort;
            this.m_keepInputPortOrder = keepInputPortOrder;

            this.m_portPermArray = new UInt32[this.NumberOfInputs];
            for (UInt32 i = 0; i < this.NumberOfInputs; i++)
            {
                this.m_portPermArray[i] = i;
            }
            if (!keepInputPortOrder)
            {
                Random rdm = new Random(System.Diagnostics.Process.GetCurrentProcess().Id);
                Int32 max = (Int32)this.NumberOfInputs;
                for (UInt32 i = 1; i < this.NumberOfInputs; i++)
                {
                    int idx = rdm.Next(max);
                    UInt32 n = this.m_portPermArray[max - 1];
                    this.m_portPermArray[max - 1] = this.m_portPermArray[idx];
                    this.m_portPermArray[idx] = n;
                    max--;
                }
            }

            this.m_readers = new DryadLinqRecordReader<T>[this.NumberOfInputs];
            for (UInt32 i = 0; i < this.NumberOfInputs; i++)
            {
                this.m_readers[i] = this.m_readerFactory.MakeReader(this.m_nativeHandle, startPort + i);
            }
            this.m_isUsed = false;
        }
        
        public DryadLinqVertexReader(VertexEnv denv, DryadLinqFactory<T> readerFactory, UInt32 portNum)
        {
            this.m_dvertexEnv = denv;
            this.m_nativeHandle = denv.NativeHandle;
            this.m_readerFactory = readerFactory;
            this.m_startPort = portNum;
            this.m_numberOfInputs = 1;
            this.m_keepInputPortOrder = false;
            this.m_portPermArray = new UInt32[] { 0 };
            DryadLinqRecordReader<T> reader = readerFactory.MakeReader(this.m_nativeHandle, portNum);            
            this.m_readers = new DryadLinqRecordReader<T>[] { reader };
            this.m_isUsed = false;
        }

        public VertexEnv VertexEnv
        {
            get { return this.m_dvertexEnv; }
        }
        
        public IntPtr NativeHandle
        {
            get { return this.m_nativeHandle; }
        }

        public UInt32 NumberOfInputs
        {
            get { return this.m_numberOfInputs; }
        }

        public IEnumerable<T> this[int idx]
        {
            get { return this.m_readers[idx]; }
        }

        public Int64 GetTotalLength()
        {
            Int64 totalLen = 0;
            for (UInt32 i = 0; i < this.NumberOfInputs; i++)
            {
                Int64 chLen = this.m_readers[i].GetTotalLength();
                if (chLen < 0) return -1;
                totalLen += chLen;
            }
            return totalLen;
        }

        public string GetChannelURI(int idx)
        {
            return this.m_readers[idx].GetChannelURI();
        }

        // Close the internal Dryad readers.
        public void CloseReaders()
        {
            for (UInt32 i = 0; i < this.NumberOfInputs; i++)
            {
                this.m_readers[i].Close();
            }
        }

        // Make this reader into a System.IO.Stream.
        internal Stream InputStream
        {
            get {
                if (this.m_isUsed)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.ChannelCannotBeReadMoreThanOnce,
                                                 SR.ChannelCannotBeReadMoreThanOnce);
                }
                this.m_isUsed = true;

                DryadLinqBinaryReader[] inputStreamArray = new DryadLinqBinaryReader[this.NumberOfInputs];
                for (int i = 0; i < this.NumberOfInputs; i++)
                {
                    DryadLinqRecordBinaryReader<T> recordReader = this.m_readers[i] as DryadLinqRecordBinaryReader<T>;
                    if (recordReader == null)
                    {
                        throw new DryadLinqException("Internal error: Input channel was not of type BinaryRecordReader.");
                    }
                    inputStreamArray[i] = recordReader.BinaryReader;
                
                }
                return new DryadLinqMultiReaderStream(inputStreamArray);
            }
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

        // Internal enumerator class (sync)
        private class RecordEnumerator : IEnumerator<T>
        {
            private DryadLinqVertexReader<T> m_vertexReader;
            private DryadLinqRecordReader<T> m_curReader;
            private UInt32 m_nextPortIdx;
            private T m_current;

            public RecordEnumerator(DryadLinqVertexReader<T> reader)
            {
                this.m_vertexReader = reader;
                this.m_nextPortIdx = 0;
                UInt32 curPortIdx = this.GetNextPortIdx();
                this.m_curReader = this.m_vertexReader.m_readers[curPortIdx];
                this.m_current = default(T);
            }

            private UInt32 GetNextPortIdx()
            {
                return this.m_vertexReader.m_portPermArray[this.m_nextPortIdx++];
            }

            public bool MoveNext()
            {
                while (true)
                {
                    if (this.m_curReader.ReadRecordSync(ref this.m_current))
                    {
                        return true;
                    }
                    if (this.m_nextPortIdx >= this.m_vertexReader.m_readers.Length)
                    {
                        return false;
                    }

                    UInt32 curPortNum = this.GetNextPortIdx();
                    this.m_curReader = this.m_vertexReader.m_readers[curPortNum];
                }
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
                this.m_vertexReader.CloseReaders();
            }
        }
    }
}
