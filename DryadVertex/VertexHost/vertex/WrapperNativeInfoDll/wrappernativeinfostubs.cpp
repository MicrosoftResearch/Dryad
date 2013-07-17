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

#include "stdafx.h"

#include <wrappernativeinfo.h>
#ifdef TIDYFS
#include <mdclient.h>
#endif

#pragma unmanaged 

Int64 GetTotalLength(WrapperNativeInfoBase *info, UInt32 portNum)
{
    return info->GetTotalLength(portNum);
}

Int64 GetExpectedLength(WrapperNativeInfoBase *info, UInt32 portNum)
{
    return info->GetExpectedLength(portNum);
}

Int64 GetVertexId(WrapperNativeInfoBase *info)
{
    return info->GetVertexId();
}

void SetInitialSizeHint(WrapperNativeInfoBase *info, UInt32 portNum, UInt64 hint)
{
    return info->SetInitialSizeHint(portNum, hint);
}

UInt32 GetNumOfInputs(WrapperNativeInfoBase *info)
{
    return info->GetNumOfInputs();
}

UInt32 GetNumOfOutputs(WrapperNativeInfoBase *info)
{
    return info->GetNumOfOutputs();
}

const char* GetInputChannelURI(WrapperNativeInfoBase *info, UInt32 portNum)
{
    return info->GetInputChannelURI(portNum);
}

const char* GetOutputChannelURI(WrapperNativeInfoBase *info, UInt32 portNum)
{
    return info->GetOutputChannelURI(portNum);
}

DataBlockItem* AllocateDataBlock(WrapperNativeInfoBase *info,
                                 Int32 dataBlockSize,
                                 byte **pDataBlock)
{
    return info->AllocateDataBlock(dataBlockSize, pDataBlock);
}

void ReleaseDataBlock(WrapperNativeInfoBase *info,
                      DataBlockItem *pItem)
{
    info->ReleaseDataBlock(pItem);
}

DataBlockItem* ReadDataBlock(WrapperNativeInfoBase *info, 
                             UInt32 portNum,
                             byte **ppDataBlock,
                             Int32 *ppDataBlockSize,
                             Int32 *pErrorCode)
{
    return info->ReadDataBlock(portNum, ppDataBlock, ppDataBlockSize, pErrorCode);
}

BOOL WriteDataBlock(WrapperNativeInfoBase *info, 
                    UInt32 portNum, 
                    DataBlockItem *pItem,
                    Int32 numBytesToWrite)
{
#ifdef VERBOSE
    fprintf(stdout, "Writing %d bytes to channel %u.\n", 
            numBytesToWrite, portNum);
    fflush(stdout);
#endif

    return info->WriteDataBlock(portNum, pItem, numBytesToWrite);
}

void Flush(WrapperNativeInfoBase *info, UInt32 portNum)
{
  // NYI
}

void Close(WrapperNativeInfoBase *info, UInt32 portNum)
{
  // NYI
}

/* JC
bool GetDryadStreamInfo(char *streamName,
                         UInt32 *numExtents, 
                         UInt64 *streamLength)
{
#ifdef VERBOSE
    fprintf(stderr, "Getting Stream Info for stream '%s'\n", streamName);
#endif
   // initialize the cosmos libraries
    DrError err = DrInitialize(DR_CURRENT_VERSION, NULL);
    if (err != DrError_OK)
    {
        return false;
    }

    DR_STREAM *pStreamInfo;
    err = DrGetStreamInformation(streamName, 0, 0, DR_ALL_EXTENTS,
                                 &pStreamInfo, NULL);
    if (err != DrError_OK)
    {
        return false;
    }
    
    *numExtents = pStreamInfo->TotalNumberOfExtents;
    *streamLength = pStreamInfo->Length;

    err = DrFreeMemory(pStreamInfo);
    LogAssert(err == DrError_OK);

    err = DrUninitialize();
    LogAssert(err == DrError_OK);

#ifdef VERBOSE
    fprintf(stderr, "Returning stream info for stream '%s' of %u extents and %I64u bytes.\n",
            streamName, *numExtents, *streamLength);
#endif
    return true;
}
*/

void EnableFifoInputChannel(WrapperNativeInfo *info, 
                            Int32 compresionScheme,
                            UInt32 channel)
{
    info->EnableFifoInputChannel(compresionScheme, channel);
}

void EnableFifoOutputChannel(WrapperNativeInfo *info, 
                             Int32 compressionScheme,
                             UInt32 channel) 
{
    info->EnableFifoOutputChannel(compressionScheme, channel);
}
