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

#include <channelitem.h>

class RChannelBufferPrefetchInfo;
enum TransformType;

/* this is a stub object that is e.g. passed to parsers and marshalers
   when they are initialized. For example, DryadVertexProgramBase
   inherits from this interface and is set as the context for all
   parsers and marshalers in that vertex so that they can access
   configuration parameters, etc. */
class RChannelContext : public IDrRefCounter
{
public:
    virtual ~RChannelContext();
};

typedef DrRef<RChannelContext> RChannelContextRef;

class RChannelInterruptHandler
{
public:
    /* When the byte-oriented layer from which a channel is reading
       signals an error condition by delivering a Restart or Abort
       buffer, the application may wish to learn this immediately
       rather than waiting for the error item to be delivered (for
       example, the application may be blocking reads from the channel
       indefinitely, yet still want timely error notifications.) In
       this case, the application can register an interrupt handler
       which will be called back when an error occurs, or during Drain
       if no error occurs.

       interruptItem is the terminatino item which will eventually be
       delivered. The callee of ProcessInterrupt should call IncRef on
       interruptItem if it wants to store a reference to the item.
     */
    virtual void ProcessInterrupt(RChannelItem* interruptItem) = 0;
};


class RChannelItemArrayReaderHandler
{
public:
    RChannelItemArrayReaderHandler();
    virtual ~RChannelItemArrayReaderHandler();

    /* This sets the maximum number of items which will be delivered
       via the ProcessItemArray callback. The default value is 1. */
    void SetMaximumArraySize(UInt32 maximumArraySize);
    UInt32 GetMaximumArraySize();

    /* When an asynchronous item is ready to be delivered on a channel
       this method is called on the handler object passed to the
       associated call to RChannelReader::SupplyHandler().

       If deliveredArray is empty and the asynchronous read was not
       cancelled, then the channel has completed, i.e. a termination
       item has been delivered to some other synchronous or
       asynchronous read method. If there are multiple outstanding
       reads the order of delivery is undefined so an empty array may
       appear on one handler before the termination item has actually
       been delivered.

       If an asynchronous read is cancelled ProcessItem is called
       before the Cancel call completes. If the array is empty in this
       case it is impossible to determine from the ProcessItem
       callback whether it occurred because of successful cancellation
       or because a termination item has been delivered to the
       application.
     */
    virtual void ProcessItemArray(RChannelItemArray* deliveredArray) = 0;

    /* there are two types of asynchronous handler: queued and
       immediate. If the handler is queued, then it is called on a
       worker thread which is specifically assigned for processing
       items, and the application should perform as much computation
       as necessary on the calling thread before returning.

       If the handler is immediate then it may be called on a variety
       of different threads, including I/O processing threads, and the
       application should return as soon as possible. Immediate
       handlers are suitable, for example, for waking up a synchronous
       thread which has been blocking on a read, or for queueing a
       work item to an application's private thread pool if the
       standard thread pool is for some reason inappropriate.

       In general rather than implementing the ImmediateDispatch
       method the application should just inherit from
       RChannelItemReaderHandlerImmediate or
       RChannelItemReaderHandlerQueued.
    */
    virtual bool ImmediateDispatch() = 0;

private:
    UInt32    m_maximumArraySize;
};

/* the base class for an immediate item array handler, see
   RChannelItemArrayReaderHandler::ImmediateDispatch above. */
class RChannelItemArrayReaderHandlerQueued :
    public RChannelItemArrayReaderHandler
{
public:
    bool ImmediateDispatch();
};

/* the base class for a queued item array handler, see
   RChannelItemArrayReaderHandler::ImmediateDispatch above. */
class RChannelItemArrayReaderHandlerImmediate :
    public RChannelItemArrayReaderHandler
{
public:
    bool ImmediateDispatch();
};

class RChannelItemReaderHandler :
    public RChannelItemArrayReaderHandler
{
public:
    virtual ~RChannelItemReaderHandler();

    /* When an asynchronous item is ready to be delivered on a channel
       this method is called on the handler object passed to the
       associated call to RChannelReader::SupplyHandler().

       If deliveredItem is NULL and the asynchronous read was not
       cancelled, then the channel has completed, i.e. a termination
       item has been delivered to some synchronous or asynchronous
       read method. If there are multiple outstanding reads the order
       of delivery is undefined so a NULL may appear on one handler
       before the termination item has actually been delivered.

       If an asynchronous read is cancelled ProcessItem is called with
       NULL before the Cancel call completes. In this case it is
       impossible to determine from the ProcessItem callback whether
       it occurred because of successful cancellation or because a
       termination item has been delivered to the application.

       If deliveredItem is non-NULL then the callee now owns a
       reference to deliveredItem.
     */
    virtual void ProcessItem(RChannelItem* deliveredItem) = 0;

    /* there are two types of asynchronous handler: queued and
       immediate. If the handler is queued, then it is called on a
       worker thread which is specifically assigned for processing
       items, and the application should perform as much computation
       as necessary on the calling thread before returning.

       If the handler is immediate then it may be called on a variety
       of different threads, including I/O processing threads, and the
       application should return as soon as possible. Immediate
       handlers are suitable, for example, for waking up a synchronous
       thread which has been blocking on a read, or for queueing a
       work item to an application's private thread pool if the
       standard thread pool is for some reason inappropriate.

       In general rather than implementing the ImmediateDispatch
       method the application should just inherit from
       RChannelItemReaderHandlerImmediate or
       RChannelItemReaderHandlerQueued.
    */
    virtual bool ImmediateDispatch() = 0;

    /* implementation of RChannelItemArrayReaderHandler interface */
    void ProcessItemArray(RChannelItemArray* deliveredArray);
};

/* the base class for an immediate item handler, see
   RChannelItemReaderHandler::ImmediateDispatch above. */
class RChannelItemReaderHandlerQueued : public RChannelItemReaderHandler
{
public:
    bool ImmediateDispatch();
};

/* the base class for a queued item handler, see
   RChannelItemReaderHandler::ImmediateDispatch above. */
class RChannelItemReaderHandlerImmediate : public RChannelItemReaderHandler
{
public:
    bool ImmediateDispatch();
};

class SyncItemReaderBase
{
public:
    virtual DrError ReadItemSync(RChannelItemRef* item /* out */) = 0;
};

class RChannelReader : public SyncItemReaderBase
{
public:
    virtual ~RChannelReader();

    /* a client must call Start to cause the channel to start
       generating items for the first time, or after a Drain has
       completed before the channel is restarted.

       prefetchCookie is passed to the byte-oriented buffer layer and
       should be NULL for now.
    */
    virtual void Start(RChannelBufferPrefetchInfo* prefetchCookie) = 0;

    /* SupplyHandler passes a handler which will be "returned" via a
       matching call to handler->ProcessItemArray. Multiple handlers
       may be outstanding at a given time. The channel blocks when
       there are no handlers available, so this is the primary flow
       control mechanism to allow the reader to exert back-pressure on
       a channel.

       If the channel has not been started or has delivered a
       termination item to the application since the last call to
       Start, handler->ProcessItemArray will be called with an empty
       array on the calling thread before SupplyHandler returns.

       If there is an item waiting to be delivered and handler is an
       RChannelItemReaderHandlerImmediate then handler will be called
       back with the items on the calling thread before SupplyHandler
       returns.

       If handler is an RChannelItemReaderHandlerQueued it will never
       be called back on the calling thread. If there are items
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
       time the handler is cancelled it will be called with an empty
       array. cancelCookie may take any value, including NULL, however
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
    virtual void SupplyHandler(RChannelItemArrayReaderHandler* handler,
                               void* cancelCookie) = 0;

    /* any handler passed to SupplyHandler with value cancelCookie is
       guaranteed to be returned before a call to Cancel completes. */
    virtual void Cancel(void* cancelCookie) = 0;

    /* FetchNextItemArray blocks waiting until an item is available, a
       termination item is delivered on another thread, or the timeOut
       interval has elapsed (timeOut can be DrTimeInterval_Infinite in
       which case FetchNextItem will block indefinitely).

       If the timeout expires FetchNextItemArray returns false and
       *pItemArray is an empty array. Otherwise FetchNextItem returns
       true and the any returned items are stored in
       *pItemArray. *pItemArray may be empty even if
       FetchNextItemArray returns true: this will happen if Start has
       not been called or a termination item has already been
       delivered on the channel since the last call to Start. At most
       maxArraySize items will be delivered in *pItemArray, however
       fewer may be delivered.
     */
    virtual bool FetchNextItemArray(UInt32 maxArraySize,
                                    RChannelItemArrayRef* pItemArray,
                                    DrTimeInterval timeOut) = 0;

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
    bool FetchNextItem(RChannelItemRef* pOutItem,
                       DrTimeInterval timeOut);
    DrError ReadItemSync(RChannelItemRef* pOutItem /* out */);

    /* Instruct the channel to stop reading items and prepare for
       Drain. The interruptItem must be of type RChannelItem_Abort,
       RChannelItem_Restart, RChannelItem_EndOfStream or
       RChannelItem_ParseError. If the channel is an in-process FIFO
       or a pipe which has not broken, the interruptItem will be
       delivered to the remote end and the remote writer will
       therefore be made aware of the channel close request. Interrupt
       may be called multiple times after a channel has been Started
       and before it has been Drained. The first call will have its
       interruptItem delivered; subsequent interruptItems will be
       discarded. If multiple calls to Interrupt are overlapped, the
       call whose interruptItem gets delivered is undefined. The
       caller's reference to interruptItem is not modified by this
       call.
     */
    virtual void Interrupt(RChannelItem* interruptItem) = 0;

    /* Instruct the channel to return all outstanding handlers via
       their handler->ProcessItem callbacks in preparation for either
       closing or restarting the channel. Once the Drain method
       returns all outstanding handler callbacks will have completed
       and all waiting calls to FetchNextItem will have been unblocked
       (though of course they may not have returned to the calling
       thread). For obvious reasons, Drain may not be called from a
       handler's ProcessItem callback.

       If the channel is an in-process FIFO or a pipe which has not
       broken, and Interrupt has not previously been called, Drain
       will send an appropriate termination item to the write end. If
       the writer has delivered a termination item to the reader
       implementation, an item of the same type will be returned to
       the writer. Otherwise an item of type RChannel_Abort will be
       returned to the writer. If the RChannelReader client has not
       read a ternmination item, then the item delivered by a call to
       Drain which is not preceded by Interrupt is undefined, since
       the implementation may have received a termination item which
       has not yet been forwarded to the client.
     */
    virtual void Drain() = 0;

    /* If the writer has sent a termination item down the channel, it
       is returned in pWriterDrainItem. If the reader has sent a
       termination item to the writer (via Interrupt or drain) it is
       returned in pReaderDrainItem. These termination items are set
       to NULL when a channel is restarted. The returned items are
       undefined if this call overlaps with a call to Start or Drain,
       and it is illegal to call GetTerminationItem after Close has
       been called. */
    virtual void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                                     RChannelItemRef* pReaderDrainItem) = 0;

    /* After a call to Drain, this returns a status code and
       optionally error metadata corresponding to the final status of
       the channel. If the writer sent an error item,
       GetTerminationStatus returns the error associated with that
       item. Otherwise if the reader sent an Interrupt with an
       EndOfStream item or called Drain after receiving an EndOfStream
       item from the writer, GetTerminationStatus will return
       DrError_EndOfStream (signalling clean termination). Otherwise
       GetTerminationStatus returns DryadError_ProcessingInterrupted
       which signifies that there was no error reported on the
       channel, but the reader requested early termination. */
    DrError GetTerminationStatus(DryadMetaDataRef* pErrorData);

    /* Close may only be called if Start has never been called, or if
       Drain has completed since the last call to Start. Close must be
       called before the RChannelReader is destroyed. After Close has
       been called no other methods may be called on RChannelReader.
    */
    virtual void Close() = 0;

    /* Get the total length in bytes of the channel. This is known
       only after the channel has been opened. */
    virtual bool GetTotalLength(UInt64* pLen) = 0;

    /* Get the total expected length in bytes of the channel. This may
       be known even before the channel is opened. If
       GetExpectedLength returns false, *pLen is undefined. */
    virtual bool GetExpectedLength(UInt64* pLen) = 0;
    virtual void SetExpectedLength(UInt64 expectedLength) = 0;

    /* Get the URI of the channel. */
    virtual const char* GetURI() = 0;

    void SetTransformType(TransformType t) { m_transform = t; }
    TransformType GetTransformType() { return m_transform; }

private:
     TransformType m_transform;
};


class RChannelItemArrayWriterHandler
{
public:
    virtual ~RChannelItemArrayWriterHandler();

    /* this interface is called whenever an async write (started by
       RChannelWriter::WriteItemList) completes. If WriteItemList was
       called with the flush flag set, this handler will not be
       returned until the underlying channel has reported that the
       write has completed. Otherwise this handler may be called
       before the item has been written to the channel.

       Normally status is RChannelItem_Data, however if the underlying
       channel implementation has requested a restart or abort, status
       will be RChannelItem_Restart or RChannelItem_Abort
       respectively. If WriteItem was called before the channel was
       started, or after a termination item had been written, then
       status will be RChannelItem_EndOfStream. If status is not
       RChannelItem_Data the item may or may not have actually been
       written to the output, regardless of whether a flush was
       requested (if status is RChannelItem_EndOfStream the item has
       definitely not been written to the output).

       If the item marshaler encountered any errors the error
       descriptions will be returned in failureArray. This is an array
       of items: each element is non-NULL if and only if the
       corresponding item in the original list had a marshal error. If
       there were no marshaling errors, failureArray will be NULL.
    */
    virtual void ProcessWriteArrayCompleted(RChannelItemType status,
                                            RChannelItemArray*
                                            failureArray) = 0;
};

class RChannelItemWriterHandler :
    public RChannelItemArrayWriterHandler
{
public:
    virtual ~RChannelItemWriterHandler();

    /* this interface is called whenever an async write (started by
       RChannelWriter::WriteItem) completes. If WriteItem was called
       with the flush flag set, this handler will not be returned
       until the underlying channel has reported that the write has
       completed. Otherwise this handler may be called before the item
       has been written to the channel.

       Normally status is RChannelItem_Data, however if the underlying
       channel implementation has requested a restart or abort, status
       will be RChannelItem_Restart or RChannelItem_Abort
       respectively. If WriteItem was called before the channel was
       started, or after a termination item had been written, then
       status will be RChannelItem_EndOfStream. If status is not
       RChannelItem_Data the item may or may not have actually been
       written to the output, regardless of whether a flush was
       requested (if status is RChannelItem_EndOfStream the item has
       definitely not been written to the output).

       If the item marshaler encountered an error the error
       description will be returned in marshalFailureItem. The callee
       does not own a reference to marshalFailureItem.
    */
    virtual void ProcessWriteCompleted(RChannelItemType status,
                                       RChannelItem* marshalFailureItem) = 0;

    /* implementation of RChannelItemArrayWriterHandler interface */
    void ProcessWriteArrayCompleted(RChannelItemType status,
                                    RChannelItemArray* failureArray);
};

class SyncItemWriterBase
{
public:
    /* A client should call WriteMapOutputItem or
       WriteMapOutputItemConsumingReference whenever it wants to send
       an item to the output channel. The latter consumes a reference
       to item, the former does not. */
    virtual void WriteItemSyncConsumingReference(RChannelItemRef& item) = 0;
    void WriteItemSyncConsumingFreeReference(RChannelItem* item);
    void WriteItemSync(RChannelItem* item);
    virtual DrError GetWriterStatus() = 0;
};

/*
  The RChannelWriter is the primary mechanism for writing
  application-specific structured items to an underlying byte-oriented
  channel.

  The application first calls Start, then sends a series of items,
  primarily of type RChannelItem_Data, though markers may be
  interspersed, followed by a termination item of type
  RChannelItem_Restart, RChannelItem_Abort or
  RChannelItem_EndOfStream. In the case of a pipe which has not
  broken, the termination item will be sent to the remote end so that
  the consuming process learns the reason for the pipe closure.

  After sending a termination item no further items will be sent on
  the channel until a call to Drain has completed. After this point
  the channel may call Start again in the case of a restart, and start
  sending items again.

  Each item is marshaled into the byte-stream using an
  application-specific marshaler object which must co-operate with the
  application so that bare RChannelItem objects can be cast by it into
  objects containing meaningful data.
 */
class RChannelWriter : public SyncItemWriterBase
{
public:
    virtual ~RChannelWriter();

    /* a client must call Start before writing items to the channel
       the first time, or after a Drain has completed before writing
       to a restarted channel.
    */
    virtual void Start() = 0;

    /* WriteItem queues item for async write to the channel.
       handler->ProcessWriteCompleted will be called when the async
       write "completes."

       If flushAfter is false the handler may be returned before the
       item has actually been written on the underlying channel. If
       flushAfter is true the handler will not be returned until the
       underlying channel has reported that the item has been
       written. Setting flushAfter potentially causes the underlying
       channel to write a partial buffer and should be used sparingly
       where performance is important.

       WriteItem transfers the caller's reference to item to the
       channel and it is guaranteed that handler will be called before
       the next call to Drain completes. If Start has not yet been
       called or a termination item has been queued since the last
       call to Start, handler->ProcessWriteCompleted will be called
       with status RChannelItem_EndOfStream on the caller's stack
       before WriteItem returns.

       Flow control is exercised by having the channel refrain from
       calling handler->ProcessWriteCompleted if the underlying
       stream has blocked. Therefore the application should limit the
       total number of outstanding calls to WriteItem in flight whose
       handlers have not yet been returned. Async items are serialized
       to a queue and marshaled on a worker thread, and handler is
       returned after this marshaling completes, so in the common case
       where the underlying channel has not blocked, the application
       should allow enough outstanding handlers to account for this
       delay in order to avoid "bubbles" in the processing pipeline.

       Once a call to WriteItem passing item A has completed, then any
       items which are subsequently submitted to the channel are
       guaranteed to appear after A. Beyond this constraint, if calls
       to WriteItem and/or WriteItemSync are overlapped the order of
       serialization to the channel is undefined.
    */
    virtual void WriteItemArray(RChannelItemArrayRef& itemArray,
                                bool flushAfter,
                                RChannelItemArrayWriterHandler* handler) = 0;
    void WriteItem(RChannelItem* item,
                   bool flushAfter,
                   RChannelItemArrayWriterHandler* handler);

    /* WriteItemArraySync submits item to be written to the channel and
       may block indefinitely if the underlying channel has blocked.

       If flush is false, WriteItemArraySync may return before the array
       has actually been sent to the underlying channel. If flush is
       true WriteItemListSync will not return until the underlying
       channel reports that item has been written. Setting flush
       potentially causes the underlying channel to write a partial
       buffer and should be used sparingly where efficiency is
       crucial.

       If the marshaler encounters an error when marshaling any item
       in the list, and pFailureArray is non-NULL, then pFailureArray
       will be filled in with an array of the same size as the
       original itemList. This array will contain non-NULL elements
       corresponding to any item in itemList which had a marshal
       error. If pFailureArray is non-NULL and there are no marshaling
       errors, *pFailureArray=NULL on return.

       WriteItemListSync always consunes the caller's reference to
       each item in the list and leaves itemList empty. If Start has
       not yet been called or a termination item has been queued since
       the last call to Start WriteItemListSync returns
       RChannelItem_EndOfStream, no item is written to the channel,
       and *pFailureArray is guaranteed to be NULL if pFailureArray is
       non-NULL. Otherwise, normally status is RChannelItem_Data,
       however if the underlying channel implementation has requested
       a restart or abort, status will be RChannelItem_Restart or
       RChannelItem_Abort respectively. If status is
       RChannelItem_Restart or RChannelItem_Abort the item in the list
       may or may not have actually been written to the output,
       regardless of whether a flush was requested.

       Once a call to WriteItemListSync passing item A has completed,
       then any items which are subsequently submitted to the channel
       are guaranteed to appear after A. Beyond this constraint, if
       calls to WriteItem and/or WriteItemSync are overlapped the
       order of serialization to the channel is undefined.
    */
    virtual RChannelItemType
        WriteItemArraySync(RChannelItemArrayRef& itemArray,
                           bool flush,
                           RChannelItemArrayRef* pFailureArray) = 0;

    /* WriteItemSync submits item to be written to the channel and may
       block indefinitely if the underlying channel has blocked.

       If flush is false, WriteItemSync may return before item has
       actually been sent to the underlying channel. If flush is true
       WriteItemSync will not return until the underlying channel
       reports that item has been written. Setting flush potentially
       causes the underlying channel to write a partial buffer and
       should be used sparingly where efficiency is crucial.

       If the marshaler encounters an error when marshaling item and
       pMarshalFailureItem is non-NULL, *pMarshalFailureItem will
       contain an item describing that error. In this case item has
       not been marshaled to the channel, however *pMarshalFailureItem
       has been. If there is no marshaling error and
       pMarshalFailureItem is non-NULL, *pMarshalFailureItem is NULL.

       WriteItemSync does not consume the caller's reference to
       item. If Start has not yet been called or a termination item
       has been queued since the last call to Start WriteItemSync
       returns RChannelItem_EndOfStream, item is not written to the
       channel, and *pMarshalFailureItem is guaranteed to be NULL if
       pMarshalFailureItem is non-NULL. Otherwise, normally status is
       RChannelItem_Data, however if the underlying channel
       implementation has requested a restart or abort, status will be
       RChannelItem_Restart or RChannelItem_Abort respectively. If
       status is RChannelItem_Restart or RChannelItem_Abort the item
       may or may not have actually been written to the output,
       regardless of whether a flush was requested.

       Once a call to WriteItemSync passing item A has completed, then
       any items which are subsequently submitted to the channel are
       guaranteed to appear after A. Beyond this constraint, if calls
       to WriteItem and/or WriteItemSync are overlapped the order of
       serialization to the channel is undefined.
    */
    RChannelItemType
        WriteItemSync(RChannelItem* item,
                      bool flush,
                      RChannelItemRef* pMarshalFailureItem);
    void WriteItemSyncConsumingReference(RChannelItemRef& item);

    /* Drain may not be called until after a termination item has been
       written to the channel using a call to WriteItemListSync,
       WriteItemSync, WriteItemList or WriteItem which has
       return. Drain will not return until the underlying channel has
       drained and all outstanding async handlers submitted to
       WriteItem or WriteItemList have been returned. At this point
       any calls to WriteItemSync or WriteItemListSync will be
       unblocked, though of course they may not have returned to the
       caller.

       If the application does not wish to block waiting for Drain it
       should wait until the handler submitted with the termination
       item has returned (or use a blocking call to WriteItemSync or
       WriteItemListSync). This handler will not be returned until the
       underlying channel has drained, and it is always the last
       handler to be returned, so Drain will not block after it has
       been returned.

       The call to Drain will block waiting for the "remote end" of
       the channel to signal that the channel has been shut down. In
       the case of a file or stream this happens immediately. In the
       case of a pipe or FIFO this happens only when the reader end
       calls Interrupt or Drain, and the item sent to the reader's
       Interrupt command is delivered by the writer's Drain and stored
       in *pRemoteStatus if pRemoteStatus is non-NULL. The item will
       be type RChannelItem_Abort, RChannelItem_Restart,
       RChannelItem_ParseError or RChannelItem_EndOfStream. The
       writer's call to Drain will block for at most a time interval
       of timeOut before returning. If the timeout period expires
       before the remote end's drain item is received and
       pRemoteStatus is non-NULL, *pRemoteStatus will be set to NULL.
    */
    virtual void Drain(DrTimeInterval timeOut,
                       RChannelItemRef* pRemoteStatus) = 0;

    /* (*pWriterDrainItem) is set to the termination item written to
       the channel, or NULL if no termination item has been written
       since the most recent call to Start.  *pReaderDrainItem is set
       to the remote status item returned by the most recent call to
       Drain, or NULL if Drain has not been called since the last call
       to Start. The return values are undefined if this call overlaps
       with a call to Start or Drain, and it is illegal to call
       GetTerminationItems after Close has been called.
    */
    virtual void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                                     RChannelItemRef* pReaderDrainItem) = 0;

    /* This returns an error corresponding to the most recent
       termination item written to the channel. It is illegal to call
       GetTerminationStatus before sending a termination item. */
    DrError GetTerminationStatus(DryadMetaDataRef* pErrorData);

    DrError GetWriterStatus();

    /* Close may not be called unless Start has never been called or
       Drain has completed since the last call to Start. After Close
       is called the channel may not be restarted. */
    virtual void Close() = 0;

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    virtual UInt64 GetInitialSizeHint() = 0;
    virtual void SetInitialSizeHint(UInt64 hint) = 0;

    /* Get the URI of the channel. */
    virtual const char* GetURI() = 0;

    void SetTransformType(TransformType t) { m_transform = t; }
    TransformType GetTransformType() { return m_transform; }

private:
     TransformType m_transform;

};
