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
using System.Runtime;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    // The class encapsulates the external environment in which a
    // managed query operator executes.
    public class VertexEnv
    {
        private const string VERTEX_EXCEPTION_FILENAME = @"VertexException.txt";

        private IntPtr m_nativeHandle;
        private UInt32 m_numberOfInputs;
        private UInt32 m_numberOfOutputs;
        private UInt32 m_nextInput;
        private UInt32 m_nextInputPort;
        private UInt32 m_nextOutputPort;
        private string[] m_argList;
        private DryadLinqVertexParams m_vertexParams;
        private bool m_useLargeBuffer;
        private bool m_multiThreading;

        public VertexEnv(string args, DryadLinqVertexParams vertexParams)
        {
            this.m_argList = args.Split('|');
            this.m_nativeHandle = new IntPtr(Int64.Parse(this.m_argList[0], NumberStyles.HexNumber));
            this.m_numberOfInputs = DryadLinqNative.GetNumOfInputs(this.m_nativeHandle);
            this.m_numberOfOutputs = DryadLinqNative.GetNumOfOutputs(this.m_nativeHandle);
            this.m_nextInput = 0;
            this.m_nextInputPort = 0;
            this.m_nextOutputPort = 0;
            this.m_vertexParams = vertexParams;
            this.m_useLargeBuffer = vertexParams.UseLargeBuffer;
            this.m_multiThreading = vertexParams.MultiThreading;
            if (this.m_numberOfOutputs > 0)
            {
                this.SetInitialWriteSizeHint();
            }

            Debug.Assert(vertexParams.InputArity <= this.m_numberOfInputs);
            Debug.Assert(vertexParams.OutputArity <= this.m_numberOfOutputs);
        }

        public bool MultiThreading
        {
            get { return m_multiThreading; }
            set { m_multiThreading = value; }
        }

        internal IntPtr NativeHandle
        {
            get { return this.m_nativeHandle; }
        }

        public UInt32 NumberOfInputs
        {
            get { return this.m_numberOfInputs; }
        }        

        public UInt32 NumberOfOutputs
        {
            get { return this.m_numberOfOutputs; }
        }

        public Int32 NumberOfArguments
        {
            get { return this.m_argList.Length; }
        }
        
        public string GetArgument(Int32 idx)
        {
            return this.m_argList[idx];
        }

        private bool UseLargeBuffer
        {
            get { return this.m_useLargeBuffer; }
        }

        public Int64 VertexId
        {
            get {
                return DryadLinqNative.GetVertexId(this.m_nativeHandle);
            }
        }
        
        public DryadLinqVertexReader<T> MakeReader<T>(DryadLinqFactory<T> readerFactory)
        {
            bool keepPortOrder = this.m_vertexParams.KeepInputPortOrder(this.m_nextInput);
            UInt32 startPort = this.m_nextInputPort;
            this.m_nextInputPort += this.m_vertexParams.InputPortCount(this.m_nextInput);
            UInt32 endPort = this.m_nextInputPort;
            this.m_nextInput++;
            return new DryadLinqVertexReader<T>(this, readerFactory, startPort, endPort, keepPortOrder);
        }

        public DryadLinqVertexWriter<T> MakeWriter<T>(DryadLinqFactory<T> writerFactory)
        {
            if (this.m_nextOutputPort + 1 < this.m_vertexParams.OutputArity)
            {
                UInt32 portNum = this.m_nextOutputPort++;
                return new DryadLinqVertexWriter<T>(this, writerFactory, portNum);
            }
            else
            {
                UInt32 startPort = this.m_nextOutputPort;
                UInt32 endPort = this.NumberOfOutputs;
                return new DryadLinqVertexWriter<T>(this, writerFactory, startPort, endPort);                
            }
        }

        public static DryadLinqBinaryReader MakeBinaryReader(NativeBlockStream nativeStream)
        {
            return new DryadLinqBinaryReader(nativeStream);
        }

        public static DryadLinqBinaryReader MakeBinaryReader(IntPtr handle, UInt32 port)
        {
            return new DryadLinqBinaryReader(handle, port);
        }

        public static DryadLinqBinaryWriter MakeBinaryWriter(NativeBlockStream nativeStream)
        {
            return new DryadLinqBinaryWriter(nativeStream);
        }

        public static DryadLinqBinaryWriter MakeBinaryWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new DryadLinqBinaryWriter(handle, port, buffSize);
        }

        private static Exception s_lastReportedException;
        internal static int ErrorCode { get; set; }

        // This method gets called by the generated vertex code, as well as VertexBridge
        // to report exceptions. The exception will be dumped to "VertexException.txt"
        // in the working directory.
        public static void ReportVertexError(Exception e)
        {
            // We first need to check whether the same exception object was already
            // reported recently, and ignore the second call.
            //
            // This will be the case for most vertex exceptions because 1) the generated
            // vertex code catches the exceptions, calls ReportVertexError and rethrows,
            // and right after that 2) VertexBridge will receive the same exception
            // wrapped in a TargetInvocationException, and call ReportVertexError again
            // after extracting the inner exception.
            //
            // The second call from the VertexBridge is necessary because some exceptions
            // (particularly TypeLoadException due to static ctors) happen in the vertex DLL,
            // but just before the try/catch blocks in the vertex entry point (therefore
            // are missed by 1).
            if (s_lastReportedException == e) return;
                        
            s_lastReportedException = e;
                        
            // add to DryadLinqLog
            DryadLinqLog.AddInfo("Vertex failed with the following exception:");
            DryadLinqLog.AddInfo("{0}", e.ToString());

            // also write out to the standalone vertex exception file in the working directory
            using (StreamWriter exceptionFile = new StreamWriter(VERTEX_EXCEPTION_FILENAME))
            {
                exceptionFile.WriteLine(e.ToString());
            }
            if (ErrorCode == 0) throw e;
        }

        internal unsafe Int32 GetWriteBuffSize()
        {
            MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
            memStatus.dwLength = (UInt32)sizeof(MEMORYSTATUSEX);
            UInt64 maxSize = 512 * 1024 * 1024UL;
            if (DryadLinqNative.GlobalMemoryStatusEx(ref memStatus))
            {
                maxSize = memStatus.ullAvailPhys / 4;
            }
            if (this.m_vertexParams.RemoteArch == "i386")
            {
                maxSize = Math.Min(maxSize, 1024 * 1024 * 1024UL);
            }
            if (this.NumberOfOutputs > 0)
            {
                maxSize = maxSize / this.NumberOfOutputs;
            }

            UInt64 buffSize = (this.UseLargeBuffer) ? (256 * 1024 * 1024UL) : (8 * 1024 * 1024UL);
            if (buffSize > maxSize) buffSize = maxSize;
            if (buffSize < (16 * 1024UL)) buffSize = 16 * 1024;
            return (Int32)buffSize;
        }

        internal Int64 GetInputSize()
        {
            Int64 totalSize = 0;
            for (UInt32 i = 0; i < this.m_numberOfInputs; i++)
            {
                Int64 channelSize = DryadLinqNative.GetExpectedLength(this.NativeHandle, i);
                if (channelSize == -1) return -1;
                totalSize += channelSize;
            }
            return totalSize;
        }

        internal void SetInitialWriteSizeHint()
        {
            Int64 inputSize = this.GetInputSize();
            UInt64 hsize = (inputSize == -1) ? (5 * 1024 * 1024 * 1024UL) : (UInt64)inputSize;
            hsize /= this.NumberOfOutputs;
            for (UInt32 i = 0; i < this.NumberOfOutputs; i++)
            {
                DryadLinqNative.SetInitialSizeHint(this.m_nativeHandle, i, hsize);
            }
        }

        // The Vertex Host native layer will use this bridge method to invoke the vertex
        // entry point instead of invoking it directly through the CLR host.
        // This has the advantage of doing all the assembly load and invoke work for the
        // generated vertex assembly to happen in a managed context, so that any type or
        // assembly load exceptions can be caught and reported in full detail.
        private static void VertexBridge(string logFileName, string vertexBridgeArgs)
        {
            DryadLinqLog.Initialize(Constants.TraceInfoLevel, logFileName);
            DryadLinqLog.AddInfo(".NET runtime version = v{0}.{1}.{2}",
                                 Environment.Version.Major,
                                 Environment.Version.Minor,
                                 Environment.Version.Build);
            DryadLinqLog.AddInfo(".NET runtime GC = {0}({1})",
                                 (GCSettings.IsServerGC) ? "ServerGC" : "WorkstationGC",
                                 GCSettings.LatencyMode);

            try
            {
                string[] splitArgs = vertexBridgeArgs.Split(',');
                if (splitArgs.Length != 4)
                {
                    throw new ArgumentException(string.Format(SR.VertexBridgeBadArgs, vertexBridgeArgs),
                                                "vertexBridgeArgs");
                }

                // We assume that the vertex DLL is in the job dir (currently always one level up from the WD).
                string moduleName = Path.Combine("..", splitArgs[0]);
                string className = splitArgs[1];
                string methodName = splitArgs[2];
                string nativeChannelString = splitArgs[3];

                Assembly vertexAssembly = Assembly.LoadFrom(moduleName);
                DryadLinqLog.AddInfo("Vertex Bridge loaded assembly {0}", vertexAssembly.Location);

                MethodInfo vertexMethod = vertexAssembly.GetType(className)
                                                        .GetMethod(methodName, BindingFlags.Static | BindingFlags.Public);
                vertexMethod.Invoke(null, new object[] { nativeChannelString });
            }
            catch (Exception e)
            {
                // Any exception that happens in the vertex code will come wrapped in a
                // TargetInvocationException since we're using Invoke(). We only want to
                // report the inner exception in this case. If the exception is of another
                // type (most likely one coming from the Assembly.LoadFrom() call), then
                // we will report it as is.
                if (e is TargetInvocationException && e.InnerException != null)
                {
                    ReportVertexError(e.InnerException);
                    if (ErrorCode == 0) throw e.InnerException;
                }
                else
                {
                    ReportVertexError(e);
                    if (ErrorCode == 0) throw;
                }
            }
        }

    }
}
