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

#include <channelreader.h>
#include <channelwriter.h>
#include <dryadeventcache.h>

class RChannelFifo;
class RChannelFifoUnit;
typedef DryadBListDerived<RChannelFifoUnit,RChannelUnit> ChannelFifoUnitList;

class RChannelFifoWriterBase :
    public RChannelWriter,
    public RChannelReaderSupplier
{
public:
    RChannelFifo* GetParent();

    /* partial RChannelWriter interface */
    void WriteItemArray(RChannelItemArrayRef& itemArray,
                        bool flushAfter,
                        RChannelItemArrayWriterHandler* handler);
    RChannelItemType WriteItemArraySync(RChannelItemArrayRef& itemArray,
                                        bool flush,
                                        RChannelItemArrayRef* failureArray);
    void Close();

    /* partial RChannelReaderSupplier interface */
    void CloseSupplier();

    UInt64 GetDataSizeWritten();

    /* Get/Set the URI of the channel. */
    const char* GetURI();
    void SetURI(const char* uri);

    /* these are just dummies for a fifo: the hint is always returned
       as zero */
    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);

protected:
    enum WriterState {
        WS_Closed,
        WS_Running,
        WS_Draining,
        WS_Stopped
    };

    enum SupplierState {
        SS_Closed,
        SS_Running,
        SS_Interrupted,
        SS_Draining,
        SS_Stopped
    };

    class SyncHandler : public RChannelItemArrayWriterHandler
    {
    public:
        SyncHandler();
        ~SyncHandler();

        void ProcessWriteArrayCompleted(RChannelItemType status,
                                        RChannelItemArray* failureArray);

        void UseEvent(DryadHandleListEntry* event);
        bool UsingEvent();
        RChannelItemType GetStatusCode();
        void Wait();
        DryadHandleListEntry* GetEvent();

    private:
        DryadHandleListEntry*       m_event;
        LONG volatile               m_usingEvent;
        RChannelItemType            m_statusCode;
    };

    RChannelFifoWriterBase(RChannelFifo* parent);
    virtual ~RChannelFifoWriterBase();

    virtual void AcceptReturningUnit(RChannelFifoUnit* unit) = 0;
    virtual void EnqueueItemArray(RChannelItemArrayRef& itemArray,
                                  RChannelItemArrayWriterHandler* handler,
                                  SyncHandler* SyncHandler) = 0;

    void SetReader(RChannelReaderImpl* reader);

    void ReturnHandlers(ChannelFifoUnitList* returnList);
    void SendUnits(ChannelUnitList* unitList);
    RChannelFifoUnit* DuplicateUnit(RChannelFifoUnit* unit,
                                    RChannelItemArray* itemArray);

    RChannelFifo*                             m_parent;
    WriterState                               m_writerState;
    SupplierState                             m_supplierState;
    ChannelFifoUnitList                       m_blockedList;
    DryadOrderedSendLatch<ChannelUnitList>    m_sendLatch;
    DryadOrderedSendLatch<ChannelFifoUnitList>    m_returnLatch;
    RChannelReaderImpl*                       m_reader;
    UInt64                                    m_nextDataSequenceNumber;
    UInt64                                    m_nextDeliverySequenceNumber;
    UInt64                                    m_numberOfSubItemsWritten;
    UInt64                                    m_dataSizeWritten;
    UInt32                                    m_outstandingUnits;
    RChannelItemRef                           m_writerTerminationItem;
    RChannelItemRef                           m_readerTerminationItem;
    UInt32                                    m_writerEpoch;
    UInt32                                    m_supplierEpoch;
    HANDLE                                    m_supplierDrainEvent;
    DryadEventCache                           m_eventCache;

    DrStr128                                   m_uri;

    CRITSEC                                   m_baseDR;

    friend class RChannelFifoUnit;
    friend class RChannelFifo;
};

class RChannelFifoWriter :
    public RChannelFifoWriterBase
{
public:
    /* RChannelWriter interface */
    void Start();
    void Drain(DrTimeInterval timeOut, RChannelItemRef* pReturnItem);
    void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                             RChannelItemRef* pReaderDrainItem);

    /* RChannelReaderSupplier interface */
    void StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie);
    void InterruptSupplier();
    void DrainSupplier(RChannelItem* drainItem);

private:
    RChannelFifoWriter(RChannelFifo* parent, UInt32 fifoLength);
    ~RChannelFifoWriter();

    bool ReWriteBlockedListForEarlyReturn(RChannelItemType drainType);
    bool CheckForTerminationItem(RChannelFifoUnit* unit);
    void StartBlocking(RChannelFifoUnit* unit,
                       SyncHandler* SyncHandler);
    void EnqueueItemArray(RChannelItemArrayRef& itemArray,
                          RChannelItemArrayWriterHandler* handler,
                          SyncHandler* SyncHandler);
    void AcceptReturningUnit(RChannelFifoUnit* unit);

    UInt32                                    m_availableUnits;
    UInt32                                    m_fifoLength;
    RChannelFifoUnit*                         m_terminationUnit;
    HANDLE                                    m_writerDrainEvent;

    friend class RChannelFifoUnit;
    friend class RChannelFifo;
};

class RChannelFifoNBWriter :
    public RChannelFifoWriterBase
{
public:
    /* RChannelWriter interface */
    void Start();
    void Drain(DrTimeInterval timeOut, RChannelItemRef* pReturnItem);
    void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                             RChannelItemRef* pReaderDrainItem);

    /* RChannelReaderSupplier interface */
    void StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie);
    void InterruptSupplier();
    void DrainSupplier(RChannelItem* drainItem);

private:
    RChannelFifoNBWriter(RChannelFifo* parent);

    bool CheckForTerminationItem(RChannelFifoUnit* unit);
    void EnqueueItemArray(RChannelItemArrayRef& itemArray,
                          RChannelItemArrayWriterHandler* handler,
                          SyncHandler* SyncHandler);
    void AcceptReturningUnit(RChannelFifoUnit* unit);

    friend class RChannelFifoUnit;
    friend class RChannelFifo;
};

class RChannelFifoReader : public RChannelReaderImpl
{
public:
    RChannelFifo* GetParent();

    bool GetTotalLength(UInt64* pLen);
    bool GetExpectedLength(UInt64* pLen);
    void SetExpectedLength(UInt64 expectedLength);

private:
    RChannelFifoReader(RChannelFifo* parent,
                       WorkQueue* workQueue);
    ~RChannelFifoReader();

    RChannelFifo*   m_parent;

    friend class RChannelFifo;
};

class RChannelFifo
{
public:
    RChannelFifo(const char* name,
                 UInt32 fifoLength, WorkQueue* workQueue);
    ~RChannelFifo();

    const char* GetName();
    RChannelFifoReader* GetReader();
    RChannelFifoWriterBase* GetWriter();

    static const UInt32 s_infiniteBuffer = (UInt32) -1;

private:
    DrStr64                   m_name;
    RChannelFifoReader*       m_reader;
    RChannelFifoWriterBase*   m_writer;
};
