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
#pragma warning(disable:4512)  // KLUDGE -- build for now, fix later.

#include <channelitem.h>
#include <orderedsendlatch.h>
#include <dryadlisthelper.h>
#include <dryadeventcache.h>
#include <channelinterface.h>
#include <channelparser.h>
#pragma warning(disable:4995)
#include <map>

class WorkQueue;
class WorkRequest;

class RChannelUnit;

class RChannelParseRequest;
class RChannelProcessRequest;
class RChannelBufferBoundaryUnit;
class RChannelItemUnit;
class RChannelBuffer;
class RChannelBufferReader;
class RChannelBufferQueue;
class RChannelBufferPrefetchInfo;

typedef DryadBList<RChannelUnit> ChannelUnitList;

typedef DryadBListDerived<RChannelProcessRequest,WorkRequest>
    ChannelProcessRequestList;

class RChannelReaderSupplier
{
public:
    virtual ~RChannelReaderSupplier();
    virtual void StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie) = 0;
    virtual void InterruptSupplier() = 0;
    virtual void DrainSupplier(RChannelItem* drainItem) = 0;
    virtual void CloseSupplier() = 0;
};

class RChannelReaderImpl : public RChannelReader
{
public:
    virtual ~RChannelReaderImpl();

    /* a client must call Start to cause the channel to start
       generating items for the first time, or after a Drain has
       completed before the channel is restarted.

       prefetchCookie is passed to the byte-oriented buffer layer and
       should be NULL for now.
    */
    void Start(RChannelBufferPrefetchInfo* prefetchCookie);

    /* SupplyHandler passes a handler which will be "returned" via a
       matching call to handler->ProcessItem. Multiple handlers may be
       outstanding at a given time. The channel blocks when there are
       no handlers available, so this is the primary flow control
       mechanism to allow the reader to exert back-pressure on a
       channel.

       If the channel has not been started or has delivered a
       termination item to the application since the last call to
       Start, handler->ProcessItem will be called with value NULL on
       the calling thread before SupplyHandler returns.

       If there is an item waiting to be delivered and handler is an
       RChannelItemReaderHandlerImmediate then handler will be called
       back with the item on the calling thread before SupplyHandler
       returns.

       If handler is an RChannelItemReaderHandlerQueued it will never
       be called back on the calling thread. If there is an item
       waiting to be delivered, a processing request will be queued
       for the item with handler, otherwise handler will be queued
       waiting for the next item to be ready.

       If thread B submits an RChannelItemReaderHandlerImmediate with
       a call to SupplyHandler that is overlapped with thread A's call
       to SupplyHandler, B's immediate handler may be called on A's
       calling thread before A's call to SupplyHandler returns.

       After a handler has been submitted to SupplyHandler with a
       given value of cancelCookie, that handler is guaranteed to be
       returned before any matching call to Cancel with the same value
       of cancelCookie returns. If there is no item available at the
       time the handler is cancelled it will be called with
       NULL. cancelCookie may take any value, including NULL, however
       it is safest to use NULL or the address of an object owned by
       the caller. The cancellation mechanism is used internally by
       the synchronous FetchNextItem call which uses an allocated heap
       address as its cancelCookie. Using the heap address of an
       object which has not been freed before the call to Cancel will
       avoid any danger of a cookie collision.

       For any item A which has already been returned via an async
       handler or call to FetchNextItem when SupplyHandler is called
       it is guaranteed that A's sequence number is less than or equal
       to the sequence number of the item eventually returned on
       handler. Beyond this constraint, if multiple handlers are
       outstanding at once, or while calls to FetchNextItem are in
       progress, the order of delivered items is undefined.
     */
    void SupplyHandler(RChannelItemArrayReaderHandler* handler,
                       void* cancelCookie);

    /* any handler passed to SupplyHandler with value cancelCookie is
       guaranteed to be returned before a call to Cancel completes. */
    void Cancel(void* cancelCookie);

    /* FetchNextItem blocks waiting until an item is available, a
       termination item is delivered on another thread, or the timeOut
       interval has elapsed (timeOut can be DrTimeInterval_Infinite in
       which case FetchNextItem will block indefinitely).

       If the timeout expires FetchNextItem returns false and *outItem
       is NULL. Otherwise FetchNextItem returns true and the returned
       item is stored in *outItem. *outItem may be NULL even if
       FetchNextItem returns true: this will happen if Start has not
       been called or a termination item has already been delivered on
       the channel since the last call to Start.
     */
    bool FetchNextItemArray(UInt32 maxListSize,
                            RChannelItemArrayRef* itemArray,
                            DrTimeInterval timeOut);

    /* Instruct the channel to return all outstanding handlers via
       their handler->ProcessItem callbacks in preparation for either
       closing or restarting the channel. Once the Drain method
       returns all outstanding handler callbacks will have completed
       and all waiting calls to FetchNextItem will have been unblocked
       (though of course they may not have returned to the calling
       thread). For obvious reasons, Drain may not be called from a
       handler's ProcessItem callback.

       The drainItem may be of type RChannelItem_Abort,
       RChannelItem_Restart, RChannelItem_EndOfStream or
       RChannelItem_ParseError. If the channel is a pipe which has not
       broken, the drainItem will be delivered to the remote end as
       part of the shutdown procedure.
     */
    void Interrupt(RChannelItem* interruptItem);
    void Drain();

    /* return the drain item passed to the most recent call to Drain,
       or NULL if Drain has not been called since the last call to
       Start. The return value is undefined if this call overlaps
       with a call to Start or Drain, and it is illegal to call
       GetTerminationItems after Close has been called. */
    void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                             RChannelItemRef* pReaderDrainItem);

    UInt64 GetDataSizeRead();

    /* Close may only be called if Start has never been called, or if
       Drain has completed since the last call to Start. Close must be
       called before the RChannelReader is destroyed. After Close has
       been called no other methods may be called on RChannelReader.
    */
    void Close();

    /* Get/Set the URI of the channel. */
    const char* GetURI();
    void SetURI(const char* uri);

protected:
    RChannelReaderImpl();
    void Initialize(RChannelReaderSupplier* supplier,
                    WorkQueue* workQueue,
                    bool lazyStart);

private:
    typedef std::multimap<void *, RChannelProcessRequest *>
        CookieHandlerMap;
    typedef std::map<void *, HANDLE>
        CookieEventMap;

    enum ReaderState {
        RS_Stopped,
        RS_Running,
        RS_InterruptingSupplier,
        RS_Stopping,
        RS_Closed
    };

    bool IsRunning();

    void FillEmptyHandlers(ChannelProcessRequestList* handlerDispatch);
    void TransferWaitingItems(const char* caller,
                              ChannelProcessRequestList* requestList,
                              ChannelUnitList* returnUnitList);
    void ReturnUnits(ChannelUnitList* unitList);
    void DispatchRequests(const char* caller,
                          ChannelProcessRequestList* requestList,
                          ChannelUnitList* returnUnitList);
    void AddUnitToQueue(const char* caller,
                        RChannelUnit* unit,
                        ChannelProcessRequestList* requestList,
                        ChannelUnitList* returnUnitList);
    void AddUnitList(ChannelUnitList* unitList);
    void AddHandlerToQueue(const char* caller,
                           RChannelProcessRequest* request,
                           ChannelProcessRequestList* requestList,
                           ChannelUnitList* returnUnitList);
    void RemoveFromCancelMap(RChannelProcessRequest* request,
                             void* cancelCookie);
    void MaybeTriggerCancelEvent(void* cancelCookie);
    void ProcessItemArrayRequest(RChannelProcessRequest* handler);
    void AlertApplication(RChannelItem* item);
    void ThreadSafeSetItemArray(RChannelItemArrayRef* dstItemArray,
                                RChannelItemArray* srcItemArray);

    WorkQueue*                      m_workQueue;

    CookieHandlerMap                m_cookieMap;
    CookieEventMap                  m_eventMap;
    ChannelUnitList                 m_unitList;
    ChannelProcessRequestList       m_handlerList;
    RChannelInterruptHandler*       m_interruptHandler;

    ReaderState                     m_state;
    DryadOrderedSendLatch<ChannelProcessRequestList> m_sendLatch;
    DryadOrderedSendLatch<ChannelUnitList> m_unitLatch;
    HANDLE                          m_drainEvent;
    HANDLE                          m_interruptEvent;
    HANDLE                          m_startedSupplierEvent;
    bool                            m_lazyStart;
    bool                            m_startedSupplier;
    DryadEventCache                 m_eventCache;
    RChannelItemRef                 m_readerTerminationItem;
    RChannelItemRef                 m_writerTerminationItem;
    UInt64                          m_numberOfSubItemsRead;
    UInt64                          m_dataSizeRead;

    RChannelBufferPrefetchInfo*     m_prefetchCookie;
    RChannelReaderSupplier*         m_supplier;

    DrStr128                        m_uri;

    CRITSEC                         m_baseDR;

    friend class RChannelParseRequest;
    friend class RChannelProcessRequest;
    friend class RChannelBufferBoundaryRequest;
    friend class RChannelReaderSyncWaiter;
    friend class RChannelItemUnit;
    friend class RChannelBufferQueue;
    friend class RChannelFifoWriterBase;
};

class RChannelSerializedReader : public RChannelReaderImpl
{
public:
    RChannelSerializedReader(RChannelBufferReader* bufferReader,
                             RChannelItemParserBase* parser,
                             UInt32 maxParseBatchSize,
                             UInt32 maxOutstandingUnits,
                             bool lazyStart,
                             WorkQueue* workQueue);
    ~RChannelSerializedReader();

    bool GetTotalLength(UInt64* pLen);
    bool GetExpectedLength(UInt64* pLen);
    void SetExpectedLength(UInt64 expectedLength);
    
private:
    UInt64                 m_expectedLength;
    RChannelBufferQueue*   m_bufferQueue;
};

/* interface used to signal that a buffer has arrived and is ready for
   reading */
class RChannelBufferReaderHandler
{
public:
    virtual ~RChannelBufferReaderHandler();

    /* When an i/o completes on a buffer reader, the buffer is
       delivered via this callback to the consumer.
       buffer->ProcessingComplete should be called when the consumer
       has finished using the data in the buffer. Buffers are
       delivered in order, and to enforce this there will be at most
       one call to ProcessBuffer in progress at a time on any given
       channel.

       If buffer->GetType() is a termination type
       (RChannelBuffer::IsTerminationBuffer returns true) then no more
       calls to ProcessBuffer will be made before
       RChannelBufferReader::Drain has completed.

       The completion callback mechanism is used to implement flow
       control, as the buffer-oriented i/o will only allow the
       consumer to hold a bounded number of outstanding buffers before
       blocking further reads.
    */
    virtual void ProcessBuffer(RChannelBuffer* buffer) = 0;
};

/* base class to wrap byte-oriented read implementations */
class RChannelBufferReader
{
public:
    virtual ~RChannelBufferReader();

    /* Instruct the i/o reader to start fetching buffers from the
       start of the Channel and delivering them via handler.

       prefetchCookie is an optional hint which may be used to
       influence initial read buffer sizes or prefetching
       behaviour. It is dependent on the implementation of the
       underlying buffer-oriented i/o class and should be NULL for
       now.
     */
    virtual void Start(RChannelBufferPrefetchInfo* prefetchCookie,
                       RChannelBufferReaderHandler* handler) = 0;

    /* Instruct the i/o reader to prepare to Drain the Channel of
       buffers. After a call to Interrupt returns, the BufferReader
       will not make any more calls to handler unless the Channel is
       Drained and restarted.

       A client which wants to abort or restart the Channel should
       first call Interrupt (which guarantees no new buffers will be
       delivered), then ensure that any outstanding buffers have been
       returned via their completion handlers, then call Drain.
     */
    virtual void Interrupt() = 0;

    virtual void FillInStatus(DryadChannelDescription* status);

    /* Complete the synchronisation in the case of restarting or
       closing the stream. Drain will not return until all outstanding
       buffer completion handlers have been called.

       drainItem will have type RChannelItem_Abort,
       RChannelItem_Restart, RChannelItem_EndOfStream or
       RChannelItem_ParseError and if the channel is a pipe it should
       be communicated to the process at the remote end.

       After Drain returns, Start and Close are the only legal method
       calls.
     */
    virtual void Drain(RChannelItem* drainItem) = 0;

    /* shut down the channel. After Close returns no further calls can
       be made to this interface.
    */
    virtual void Close() = 0;

    virtual bool GetTotalLength(UInt64* pLen) = 0;
};

class RChannelNullBufferReader :
    public RChannelBufferReader, public RChannelBufferDefaultHandler
{
public:
    void Start(RChannelBufferPrefetchInfo* prefetchCookie,
               RChannelBufferReaderHandler* handler);
    void Interrupt();
    void Drain(RChannelItem* drainItem);
    void Close();
    void ReturnBuffer(RChannelBuffer* buffer);
    bool GetTotalLength(UInt64* pLen);
};
