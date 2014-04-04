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

#include "managedchannelhelpers.h"

#pragma managed

#include <vcclr.h>

void MCritSec::Enter()
{
    System::Threading::Monitor::Enter(this);
}

void MCritSec::Leave()
{
    System::Threading::Monitor::Exit(this);
}


MAutoCriticalSection::MAutoCriticalSection(MCritSec^ critSec)
{
    m_critSec = critSec;
    m_critSec->Enter();
}

MAutoCriticalSection::~MAutoCriticalSection()
{
    m_critSec->Leave();
}


static ManagedHandleStore::ManagedHandleStore()
{
    s_cs = gcnew MCritSec();
    s_store = gcnew Dictionary<MCHandle, Object^>();
    s_next = 0;
}

MCHandle ManagedHandleStore::Create(Object^ o)
{
    MAutoCriticalSection acs(s_cs);
        
    s_store->Add(s_next, o);

    MCHandle toReturn = s_next;
    ++s_next;
    return toReturn;
}

System::Object^ ManagedHandleStore::Get(MCHandle h)
{
    MAutoCriticalSection acs(s_cs);

    return s_store[h];
}

void ManagedHandleStore::Free(MCHandle h)
{
    MAutoCriticalSection acs(s_cs);

    bool removed = s_store->Remove(h);
    if (!removed)
    {
        throw gcnew System::ApplicationException("Freed non-existent handle " + h);
    }
}


void ManagedChannelBridge::LogAssertion(System::String^ message, System::String^ file, System::String^ function, int line)
{
    LogWithType(LogLevel_Assert, message, file, function, line);
}

void ManagedChannelBridge::LogError(System::String^ message, System::String^ file, System::String^ function, int line)
{
    LogWithType(LogLevel_Error, message, file, function, line);
}

void ManagedChannelBridge::LogWarning(System::String^ message, System::String^ file, System::String^ function, int line)
{
    LogWithType(LogLevel_Warning, message, file, function, line);
}

void ManagedChannelBridge::LogInformation(System::String^ message, System::String^ file, System::String^ function, int line)
{
    LogWithType(LogLevel_Info, message, file, function, line);
}

void ManagedChannelBridge::LogWithType(LogLevel type, System::String^ message, System::String^ file, System::String^ function, int line)
{
    if (DrLogging::Enabled(type))
    {
        DrString sMessage(message);
        DrString sFile(file);
        DrString sFunction(function);
        DrLogHelper(type, sFile.GetChars(), sFunction.GetChars(), line)("%s", sMessage.GetChars());
    }
}


RChannelItem* ManagedHelpers::MakeErrorItem(DrError errorCode, const char* description)
{
    RChannelItem* item =
        RChannelMarkerItem::Create(RChannelItem_Abort, true);
    DryadMetaData* metaData = item->GetMetaData();
    metaData->AddErrorWithDescription(errorCode, description);

    return item;
}

RChannelBuffer* ManagedHelpers::MakeErrorBuffer(DrError errorCode, const char* description,
                                                RChannelBufferDefaultHandler* handler)
{
    RChannelItem* item = MakeErrorItem(errorCode, description);

    RChannelBuffer* errorBuffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_Abort,
                                            item,
                                            handler);
    DryadMetaData* metaData = errorBuffer->GetMetaData();
    metaData->AddErrorWithDescription(errorCode, description);

    return errorBuffer;
}

//
// Make an end-of-stream buffer
//
RChannelItem* ManagedHelpers::MakeEndOfStreamItem()
{
    return RChannelMarkerItem::Create(RChannelItem_EndOfStream, false);
}

RChannelBuffer* ManagedHelpers::MakeEndOfStreamBuffer(RChannelBufferDefaultHandler* handler)
{
    RChannelItem* item = MakeEndOfStreamItem();

    RChannelBuffer* buffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_EndOfStream,
                                            item,
                                            handler);

    return buffer;
}

RChannelBufferData* ManagedHelpers::MakeDataBuffer(UInt64 streamOffset, size_t blockSize,
                                                   RChannelBufferDefaultHandler* handler)
{
    DryadAlignedReadBlock* block =
        new DryadAlignedReadBlock(blockSize, 0);
    RChannelBufferDataSettableOffset* dataBuffer =
        RChannelBufferDataSettableOffset::Create(block,
                                                 handler);
    dataBuffer->SetOffset(streamOffset);

    DryadMetaData* metaData = dataBuffer->GetMetaData();
    DryadMTagRef tag;
    tag.Attach(DryadMTagUInt64::Create(Prop_Dryad_BufferLength,
                                       block->GetAvailableSize()));
    metaData->Append(tag, false);

    return dataBuffer;
}
