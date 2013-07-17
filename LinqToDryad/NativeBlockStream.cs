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
using System.Security;
using System.Threading;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using System.Diagnostics;
using System.IO;
using Microsoft.Research.DryadLinq;
using Microsoft.Research.DryadLinq.Internal;


namespace Microsoft.Research.DryadLinq.Internal
{
    internal unsafe struct DataBlockInfo
    {
        internal byte* dataBlock;
        internal Int32 blockSize;        
        internal IntPtr itemHandle;
    }

    // this type is public on the outside but all its members are marked internal
    // because generated vertex code needs to pass around references to it but
    // doesn't call any methods, nor should client code.
    public abstract class NativeBlockStream
    {
        internal abstract Int64 GetTotalLength();
        
        internal abstract unsafe DataBlockInfo ReadDataBlock();

        internal abstract unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite);

        internal abstract unsafe DataBlockInfo AllocateDataBlock(Int32 size);

        internal abstract unsafe void ReleaseDataBlock(IntPtr itemHandle);

        internal abstract void Flush();

        internal abstract void Close();

        internal virtual string GetURI()
        {
            throw new DryadLinqException(HpcLinqErrorCode.GetURINotSupported,
                                       SR.GetURINotSupported);
        }

        internal virtual void SetCalcFP()
        {
            throw new DryadLinqException(HpcLinqErrorCode.SetCalcFPNotSupported,
                                       SR.SetCalcFPNotSupported);
        }

        internal virtual UInt64 GetFingerPrint()
        {
            throw new DryadLinqException(HpcLinqErrorCode.GetFPNotSupported,
                                       SR.GetFPNotSupported);
        }
    }

    internal sealed class HpcLinqChannel : NativeBlockStream
    {
        private IntPtr m_vertexInfo;
        private UInt32 m_portNum;
        private bool m_isInput;
        private bool m_isClosed;

        internal HpcLinqChannel(IntPtr vertexInfo, UInt32 portNum, bool isInput)
        {
            this.m_vertexInfo = vertexInfo;
            this.m_portNum = portNum;
            this.m_isInput = isInput;
            this.m_isClosed = false;
        }

        ~HpcLinqChannel()
        {
            this.Close();
        }

        internal IntPtr NativeHandle
        {
            get { return this.m_vertexInfo; }
        }

        internal UInt32 PortNumber
        {
            get { return this.m_portNum; }
        }

        internal override unsafe Int64 GetTotalLength()
        {
            if (this.m_isInput)
            {
                return HpcLinqNative.GetExpectedLength(this.m_vertexInfo, this.m_portNum);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal override unsafe DataBlockInfo AllocateDataBlock(Int32 size)
        {
            DataBlockInfo blockInfo;
            blockInfo.itemHandle =
                HpcLinqNative.AllocateDataBlock(this.m_vertexInfo, size, &blockInfo.dataBlock);
            blockInfo.blockSize = size;
            if (blockInfo.itemHandle == IntPtr.Zero)
            {
                throw new DryadLinqException(HpcLinqErrorCode.FailedToAllocateNewNativeBuffer,
                                           String.Format(SR.FailedToAllocateNewNativeBuffer, size));
            }
            // DryadLinqLog.Add("Allocated data block {0} of {1} bytes.", blockInfo.itemHandle, size);
            return blockInfo;
        }

        internal override unsafe void ReleaseDataBlock(IntPtr itemHandle)
        {
            if (itemHandle != IntPtr.Zero)
            {
                HpcLinqNative.ReleaseDataBlock(this.m_vertexInfo, itemHandle);
            }
            // DryadLinqLog.Add("Released data block {0}.", itemHandle);
        }

        internal override unsafe DataBlockInfo ReadDataBlock()
        {
            DataBlockInfo blockInfo;
            Int32 errorCode = 0;
            blockInfo.itemHandle = HpcLinqNative.ReadDataBlock(this.m_vertexInfo,
                                                                 this.m_portNum,
                                                                 &blockInfo.dataBlock,
                                                                 &blockInfo.blockSize,
                                                                 &errorCode);
            if (errorCode != 0)
            {
                HpcLinqVertexEnv.ErrorCode = errorCode;
                throw new DryadLinqException(HpcLinqErrorCode.FailedToReadFromInputChannel,
                                           String.Format(SR.FailedToReadFromInputChannel,
                                                         this.m_portNum, errorCode));
            }
            return blockInfo;
        }

        internal override unsafe bool WriteDataBlock(IntPtr itemHandle, Int32 numBytesToWrite)
        {
            bool success = true;
            if (numBytesToWrite > 0)
            {
                success = HpcLinqNative.WriteDataBlock(this.m_vertexInfo,
                                                         this.m_portNum,
                                                         itemHandle,
                                                         numBytesToWrite);

                if (!success)
                {

                    throw new DryadLinqException(HpcLinqErrorCode.FailedToWriteToOutputChannel,
                                               String.Format(SR.FailedToWriteToOutputChannel,
                                                             this.m_portNum));
                }
            }
            return success;
        }

        internal override void SetCalcFP()
        {
            throw new DryadLinqException(HpcLinqErrorCode.SetCalcFPNotSupported,
                                       SR.SetCalcFPNotSupported);
        }

        internal override UInt64 GetFingerPrint()
        {
            throw new DryadLinqException(HpcLinqErrorCode.GetFPNotSupported,
                                       SR.GetFPNotSupported);
        }

        internal override unsafe string GetURI()
        {
            IntPtr uriPtr;
            if (this.m_isInput)
            {
                uriPtr = HpcLinqNative.GetInputChannelURI(this.m_vertexInfo, this.m_portNum);
            }
            else
            {
                uriPtr = HpcLinqNative.GetOutputChannelURI(this.m_vertexInfo, this.m_portNum);
            }
            return Marshal.PtrToStringAnsi(uriPtr);
        }

        internal override void Flush()
        {
            HpcLinqNative.Flush(this.m_vertexInfo, this.m_portNum);
        }

        internal override void Close()
        {
            if (!this.m_isClosed)
            {
                this.m_isClosed = true;
                this.Flush();
                HpcLinqNative.Close(this.m_vertexInfo, this.m_portNum);
                string ctype = (this.m_isInput) ? "Input" : "Output";
                DryadLinqLog.Add(ctype + " channel {0} was closed.", this.m_portNum);
            }
            GC.SuppressFinalize(this);
        }

        public override string ToString()
        {
            return "DryadChannel[" + PortNumber + "]";
        }
    }
}
