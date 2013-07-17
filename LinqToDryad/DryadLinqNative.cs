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

namespace Microsoft.Research.DryadLinq.Internal
{       
    internal struct MEMORYSTATUSEX
    {
        public UInt32  dwLength;
        public UInt32 dwMemoryLoad;  
        public UInt64 ullTotalPhys;
        public UInt64 ullAvailPhys;
        public UInt64 ullTotalPageFile;
        public UInt64 ullAvailPageFile;
        public UInt64 ullTotalVirtual;
        public UInt64 ullAvailVirtual;
        public UInt64 ullAvailExtendedVirtual;
    }

    // This class contains the Win32 and Dryad native API.
    // Security Warning: We suppressed unmanaged code secuirty check,
    // which saves a stack walk for each call into unmanaged code.
    [SuppressUnmanagedCodeSecurity]
    internal static class HpcLinqNative
    {
        /* Win32 native API */
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal unsafe static extern bool ReadFile(SafeFileHandle handle,
                                                  byte* pBuffer, 
                                                  UInt32 numBytesToRead, 
                                                  IntPtr pNumBytesRead, 
                                                  NativeOverlapped* overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal unsafe static extern bool WriteFile(SafeFileHandle handle,
                                                   byte* pBuffer,
                                                   UInt32 numBytesToWrite,
                                                   IntPtr pNumBytesWritten,
                                                   NativeOverlapped* overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool GetFileSizeEx(SafeFileHandle handle, out Int64 fsize);

        [DllImport("kernel32.dll", SetLastError=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

        /* Dryad native API */
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern UInt32 GetNumOfInputs(IntPtr vertexInfo);

        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern UInt32 GetNumOfOutputs(IntPtr vertexInfo);

        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern void Flush(IntPtr vertexInfo, UInt32 portNum);

        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern void Close(IntPtr vertexInfo, UInt32 portNum);

        // Get the expected size in bytes of the input channel of the given port. 
        // It returns -1 if the size is unknown.        
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern Int64 GetExpectedLength(IntPtr vertexInfo, UInt32 portNum);

        // Get the global vertex id which is unique.
        [DllImport("DryadLINQNativeChannels.dll", SetLastError = true)]
        internal static extern Int64 GetVertexId(IntPtr vertexInfo);
       
        // Set the hint size for the output channel of the given port.        
        [DllImport("DryadLINQNativeChannels.dll", SetLastError = true)]
        internal static extern void SetInitialSizeHint(IntPtr vertexInfo, UInt32 portNum, UInt64 hint);

        // Get the URI of the input channel of the given port. 
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern IntPtr GetInputChannelURI(IntPtr vertexInfo, UInt32 portNum);

        // Get the URI of the output channel of the given port. 
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal static extern IntPtr GetOutputChannelURI(IntPtr vertexInfo, UInt32 portNum);
        
        // Read the data block from the channel of the specified port number.
        // *pDataBlockSize is the number of bytes read, and return 0 if the
        // channel reaches the end. In this case, *pDataBlock should be null.
        //
        // The caller is considered to be the exclusive owner of this data
        // block. This data block will not be reclaimed until the caller
        // explicitly releases it.
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal unsafe static extern IntPtr ReadDataBlock(IntPtr vertexInfo,
                                                         UInt32 portNum,
                                                         byte** pDataBlock,
                                                         Int32* pDataBlockSize,
                                                         Int32* pErrorCode);

        // Write the data block on the channel with the specified port number.
        //
        // The data block should be considered read-only after WriteDataBlock
        // has been called. This data block will not be reclaimed until the
        // client explicitly releases it.
        [DllImport("DryadLINQNativeChannels.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal unsafe static extern bool WriteDataBlock(IntPtr vertexInfo,
                                                        UInt32 portNum,
                                                        IntPtr itemHandle,
                                                        Int32 numBytesToWrite);

        // Allocate a native Dryad data block with specified size. This data
        // block will not be reclaimed until the client explicitly releases it.
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal unsafe static extern IntPtr AllocateDataBlock(IntPtr vertexInfo, Int32 size,
                                                             byte** pDataBlock);

        // Release the data block. The client should not access it again after releasing.
        [DllImport("DryadLINQNativeChannels.dll", SetLastError=true)]
        internal unsafe static extern void ReleaseDataBlock(IntPtr vertexInfo, IntPtr itemHandle);      
    }
}
