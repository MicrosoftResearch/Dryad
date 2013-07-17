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

#pragma once

class RChannelBuffer;
class RChannelBufferQueue;
class RChannelFifoWriterBase;

#include "channelparser.h"
#include "channelreader.h"
#include "channelwriter.h"
#include <workqueue.h>

enum RChannelUnitType {
    RChannelUnit_Item,
    RChannelUnit_BufferBoundary
};

class RChannelUnit
{
public:
    virtual ~RChannelUnit();

    RChannelUnitType GetType();

    virtual void ReturnToSupplier() = 0;

protected:
    RChannelUnit(RChannelUnitType type);

private:
    RChannelUnitType  m_type;
    DrBListEntry      m_listPtr;
    friend class DryadBList<RChannelUnit>;
};

typedef class DryadBList<RChannelUnit> ChannelUnitList;

class RChannelItemUnit : public RChannelUnit
{
public:
    virtual ~RChannelItemUnit();

    RChannelItemArray* GetItemArray();
    RChannelItem* GetTerminationItem();

    void DiscardItems();

    void SetSizes(UInt64 numberOfSubItems, UInt64 dataSize);
    UInt64 GetNumberOfSubItems();
    UInt64 GetDataSize();

protected:
    RChannelItemUnit(RChannelItemArray* payload,
                     RChannelItem* terminationItem);

    RChannelItemArrayRef  m_payload;
    RChannelItemRef       m_terminationItem;
    UInt64                m_numberOfSubItems;
    UInt64                m_dataSize;
};

class RChannelSerializedUnit : public RChannelItemUnit
{
public:
    RChannelSerializedUnit(RChannelBufferQueue* parent,
                           RChannelItemArray* payload,
                           RChannelItem* terminationItem);
    void ReturnToSupplier();

private:
    RChannelBufferQueue*  m_parent;
};

class RChannelFifoUnit : public RChannelItemUnit
{
public:
    RChannelFifoUnit(RChannelFifoWriterBase* parent);
    ~RChannelFifoUnit();

    void SetPayload(RChannelItemArray* itemArray,
                    RChannelItemArrayWriterHandler* handler,
                    RChannelItemType statusCode);

    void SetTerminationItem(RChannelItem* terminationItem);


    void ReturnToSupplier();

    void Disgorge(RChannelItemArrayWriterHandler** pHandler,
                  RChannelItemType* pStatusCode);

    void SetStatusCode(RChannelItemType statusCode);
    RChannelItemType GetStatusCode();

private:
    RChannelFifoWriterBase*            m_parent;
    RChannelItemArrayWriterHandler*    m_handler;
    RChannelItemType                   m_statusCode;
};

class RChannelBufferBoundaryUnit : public RChannelUnit
{
public:
    RChannelBufferBoundaryUnit(RChannelBuffer* buffer,
                               RChannelBufferPrefetchInfo* prefetchCookie);
    ~RChannelBufferBoundaryUnit();

    void ReturnToSupplier();

private:
    RChannelBuffer*                    m_buffer;
    RChannelBufferPrefetchInfo*        m_prefetchCookie;
};

class RChannelProcessRequest : public WorkRequest
{
public:
    RChannelProcessRequest(RChannelReaderImpl* parent,
                           RChannelItemArrayReaderHandler* handler,
                           void* cancelCookie);
    ~RChannelProcessRequest();

    void Process();
    bool ShouldAbort();

    void SetItemArray(RChannelItemArray* itemArray);
    RChannelItemArray* GetItemArray();
    RChannelItemArrayReaderHandler* GetHandler();
    void* GetCookie();

    void Cancel();

private:
    LONG                             m_aborted;
    RChannelReaderImpl*              m_parent;

    RChannelItemArrayRef             m_itemArray;
    RChannelItemArrayReaderHandler*  m_handler;
    void*                            m_cookie;
};

class RChannelMarshalRequest : public WorkRequest
{
public:
    RChannelMarshalRequest(RChannelSerializedWriter* parent);

    void Process();
    bool ShouldAbort();

private:
    RChannelSerializedWriter*                m_parent;
};

class RChannelParseRequest : public WorkRequest
{
public:
    RChannelParseRequest(RChannelBufferQueue* parent,
                         bool useNewBuffer);

    void Process();
    bool ShouldAbort();

private:
    RChannelBufferQueue*                m_parent;
    bool                                m_useNewBuffer;
};

class RChannelReaderSyncWaiter : public RChannelItemArrayReaderHandlerImmediate
{
public:
    RChannelReaderSyncWaiter(RChannelReaderImpl* parent,
                             HANDLE event,
                             RChannelItemArrayRef* itemDstArray);
    ~RChannelReaderSyncWaiter();

    void ProcessItemArray(RChannelItemArray* itemArray);

private:
    RChannelReaderImpl*       m_parent;
    HANDLE                    m_event;
    RChannelItemArrayRef*     m_itemDstArray;
};
