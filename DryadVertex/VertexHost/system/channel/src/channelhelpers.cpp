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

#include "channelparser.h"
#include "channelhelpers.h"
#include "channelreader.h"
#include "channelbuffer.h"
#include "channelbufferqueue.h"
#include "channelfifo.h"

#pragma unmanaged


RChannelUnit::RChannelUnit(RChannelUnitType type)
{
    m_type = type;
}

RChannelUnit::~RChannelUnit()
{
}

RChannelUnitType RChannelUnit::GetType()
{
    return m_type;
}

RChannelItemUnit::RChannelItemUnit(RChannelItemArray* payload,
                                   RChannelItem* terminationItem) :
    RChannelUnit(RChannelUnit_Item)
{
    m_payload = payload;
    m_terminationItem = terminationItem;
    m_numberOfSubItems = 0;
    m_dataSize = 0;
}

RChannelItemUnit::~RChannelItemUnit()
{
}

RChannelItemArray* RChannelItemUnit::GetItemArray()
{
    return m_payload;
}

RChannelItem* RChannelItemUnit::GetTerminationItem()
{
    return m_terminationItem;
}

void RChannelItemUnit::DiscardItems()
{
    m_payload = NULL;
    m_terminationItem = NULL;
}

void RChannelItemUnit::SetSizes(UInt64 numberOfSubItems,
                                UInt64 dataSize)
{
    m_numberOfSubItems = numberOfSubItems;
    m_dataSize = dataSize;
}

UInt64 RChannelItemUnit::GetNumberOfSubItems()
{
    return m_numberOfSubItems;
}

UInt64 RChannelItemUnit::GetDataSize()
{
    return m_dataSize;
}


RChannelSerializedUnit::RChannelSerializedUnit(RChannelBufferQueue* parent,
                                               RChannelItemArray* payload,
                                               RChannelItem* terminationItem) :
    RChannelItemUnit(payload, terminationItem)
{
    LogAssert(m_payload != NULL);
    m_parent = parent;
}

void RChannelSerializedUnit::ReturnToSupplier()
{
    /* we should have already transferred away the array before getting
       here, and don't need to do anything more */
    LogAssert(m_payload == NULL);
    m_parent->NotifyUnitConsumption();
    delete this;
}


RChannelFifoUnit::RChannelFifoUnit(RChannelFifoWriterBase* parent) :
    RChannelItemUnit(NULL, NULL)
{
    m_parent = parent;
    m_handler = NULL;
    m_statusCode = RChannelItem_Data;
}

RChannelFifoUnit::~RChannelFifoUnit()
{
    LogAssert(m_handler == NULL);
}

void RChannelFifoUnit::SetPayload(RChannelItemArray* itemArray,
                                  RChannelItemArrayWriterHandler* handler,
                                  RChannelItemType statusCode)
{
    LogAssert(itemArray != NULL);
    LogAssert(m_payload == NULL);
    LogAssert(m_terminationItem == NULL);
    LogAssert(m_handler == NULL);

    m_payload = itemArray;
    m_handler = handler;
    m_statusCode = statusCode;
}

void RChannelFifoUnit::SetTerminationItem(RChannelItem* terminationItem)
{
    LogAssert(RChannelItem::IsTerminationItem(terminationItem->GetType()));
    m_terminationItem = terminationItem;
}

void RChannelFifoUnit::Disgorge(RChannelItemArrayWriterHandler** pHandler,
                                RChannelItemType* pStatusCode)
{
    LogAssert(m_payload == NULL);

    *pHandler = m_handler;
    m_handler = NULL;

    *pStatusCode = m_statusCode;
    m_statusCode = RChannelItem_Data;
}

void RChannelFifoUnit::SetStatusCode(RChannelItemType statusCode)
{
    LogAssert(m_handler != NULL);
    m_statusCode = statusCode;
}

RChannelItemType RChannelFifoUnit::GetStatusCode()
{
    return m_statusCode;
}

void RChannelFifoUnit::ReturnToSupplier()
{
    LogAssert(m_parent != NULL);
    LogAssert(m_payload == NULL);
    m_parent->AcceptReturningUnit(this);
}

RChannelBufferBoundaryUnit::
    RChannelBufferBoundaryUnit(RChannelBuffer* buffer,
                               RChannelBufferPrefetchInfo* prefetchCookie) :
        RChannelUnit(RChannelUnit_BufferBoundary)
{
    LogAssert(buffer != NULL);
    m_buffer = buffer;
    m_prefetchCookie = prefetchCookie;
}

RChannelBufferBoundaryUnit::~RChannelBufferBoundaryUnit()
{
    LogAssert(m_buffer == NULL);
}

void RChannelBufferBoundaryUnit::ReturnToSupplier()
{
    LogAssert(m_buffer != NULL);
    m_buffer->ProcessingComplete(m_prefetchCookie);
    m_buffer = NULL;
    m_prefetchCookie = NULL;
    delete this;
}


RChannelProcessRequest::
    RChannelProcessRequest(RChannelReaderImpl* parent,
                           RChannelItemArrayReaderHandler* handler,
                           void* cancelCookie)
{
    m_aborted = 0;
    m_parent = parent;
    m_handler = handler;
    m_cookie = cancelCookie;
}

RChannelProcessRequest::~RChannelProcessRequest()
{
}

//
// Have request parent process the item
//
void RChannelProcessRequest::Process()
{
    m_parent->ProcessItemArrayRequest(this);
    m_itemArray = NULL;
}

bool RChannelProcessRequest::ShouldAbort()
{
    return (::InterlockedExchangeAdd(&m_aborted, 0) != 0);
}

void RChannelProcessRequest::SetItemArray(RChannelItemArray* itemArray)
{
    m_itemArray = itemArray;
}

RChannelItemArray* RChannelProcessRequest::GetItemArray()
{
    return m_itemArray;
}

RChannelItemArrayReaderHandler* RChannelProcessRequest::GetHandler()
{
    LogAssert(m_handler != NULL);
    return m_handler;
}

void* RChannelProcessRequest::GetCookie()
{
    return m_cookie;
}


void RChannelProcessRequest::Cancel()
{
    ::InterlockedIncrement(&m_aborted);
}


RChannelParseRequest::
    RChannelParseRequest(RChannelBufferQueue* parent,
                         bool useNewBuffer)
{
    m_parent = parent;
    m_useNewBuffer = useNewBuffer;
}

void RChannelParseRequest::Process()
{
    m_parent->ParseRequest(m_useNewBuffer);
}

bool RChannelParseRequest::ShouldAbort()
{
    return m_parent->ShutDownRequested();
}


RChannelMarshalRequest::
    RChannelMarshalRequest(RChannelSerializedWriter* parent)
{
    m_parent = parent;
}

void RChannelMarshalRequest::Process()
{
    m_parent->MarshalItems();
}

bool RChannelMarshalRequest::ShouldAbort()
{
    return false;
}


RChannelReaderSyncWaiter::
    RChannelReaderSyncWaiter(RChannelReaderImpl* parent,
                             HANDLE event,
                             RChannelItemArrayRef* itemDstArray)
{
    m_parent = parent;
    m_event = event;
    m_itemDstArray = itemDstArray;
}

RChannelReaderSyncWaiter::~RChannelReaderSyncWaiter()
{
    LogAssert(m_event == INVALID_HANDLE_VALUE);
}

void RChannelReaderSyncWaiter::ProcessItemArray(RChannelItemArray* itemArray)
{
    LogAssert(m_event != INVALID_HANDLE_VALUE);
    m_parent->ThreadSafeSetItemArray(m_itemDstArray, itemArray);
    HANDLE event = m_event;
    m_event = INVALID_HANDLE_VALUE;
    BOOL bRet = ::SetEvent(event);
    LogAssert(bRet != 0);
}
