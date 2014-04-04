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

#include "channelbufferqueue.h"
#include "channelhelpers.h"
#include "channelreader.h"

#pragma unmanaged


RChannelBufferQueue::RChannelBufferQueue(RChannelReaderImpl* parent,
                                         RChannelBufferReader* bufferReader,
                                         RChannelItemParserBase* parser,
                                         UInt32 maxParseBatchSize,
                                         UInt32 maxOutstandingUnits,
                                         WorkQueue* workQueue)
{
    m_parent = parent;
    m_bufferReader = bufferReader;
    m_parser = parser;
    m_workQueue = workQueue;

    m_maxParseBatchSize = m_parser->GetMaxParseBatchSize();
    if (m_maxParseBatchSize == 0)
    {
        m_maxParseBatchSize = maxParseBatchSize;
    }

    m_maxOutstandingUnits = maxOutstandingUnits;
    m_outstandingUnits = 0;

    m_state = BQ_Stopped;
    m_shutDownRequested = false;
    m_shutDownEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_shutDownEvent != NULL);
    /* alertCompleteEvent starts out signaled, and only gets reset
       transiently during the period that ProcessAfterParsing may be
       calling AlertApplication */
    m_alertCompleteEvent = ::CreateEvent(NULL, TRUE, TRUE, NULL);
    LogAssert(m_alertCompleteEvent != NULL);

    m_currentBuffer = NULL;
    m_firstBuffer = true;
    m_nextDataSequenceNumber = 0;
    m_nextDeliverySequenceNumber = 0;
}

RChannelBufferQueue::~RChannelBufferQueue()
{
    LogAssert(m_state == BQ_Stopped);
    LogAssert(m_currentBuffer == NULL);
    LogAssert(m_pendingList.IsEmpty());
    BOOL bRet = ::CloseHandle(m_shutDownEvent);
    LogAssert(bRet != 0);
    bRet = ::CloseHandle(m_alertCompleteEvent);
    LogAssert(bRet != 0);
}

void RChannelBufferQueue::
    StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_bufferReader != NULL);
        LogAssert(m_parser != NULL);
        LogAssert(m_state == BQ_Stopped);
        LogAssert(m_currentBuffer == NULL);
        LogAssert(m_pendingList.IsEmpty());
        m_sendLatch.Start();

        m_shutDownRequested = false;
        m_firstBuffer = true;
        m_state = BQ_Empty;

        m_nextDataSequenceNumber = 0;
        m_nextDeliverySequenceNumber = 0;
    }

    m_bufferReader->Start(prefetchCookie, this);
}

//
// Return whether queue is shutting down or has been requested
// to shut down
//
bool RChannelBufferQueue::ShutDownRequested()
{
    bool retval;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state != BQ_Stopped);
        retval = (m_shutDownRequested || m_state == BQ_Stopping);
    }

    return retval;
}

//
// Return all buffers in list and queue a work request if supplied
//
void RChannelBufferQueue::DispatchEvents(ChannelBufferList* bufferList,
                                         WorkRequest* workRequest)
{
    DrBListEntry* listEntry = bufferList->GetHead();
    while (listEntry != NULL)
    {
        //
        // Return all buffers
        //
        RChannelBuffer* buffer = bufferList->CastOut(listEntry);
        listEntry = bufferList->GetNext(listEntry);
        bufferList->Remove(bufferList->CastIn(buffer));
//         DrLogD(
//             "RChannelBufferQueue::RequestNextParse returning buffer");
        buffer->ProcessingComplete(NULL);
    }

    if (workRequest != NULL)
    {
        //
        // Queue up work request if specified
        //
        bool bRet = m_workQueue->EnQueue(workRequest);
        LogAssert(bRet == true);
    }
}

//
// Add current and pending buffers to buffer list and forget about them
// called with RChannelBufferQueue::m_baseCS locked 
//
void RChannelBufferQueue::ShutDownBufferQueue(ChannelBufferList* bufferList)
{
    LogAssert(m_currentBuffer != NULL);

    bufferList->InsertAsTail(bufferList->CastIn(m_currentBuffer));
    m_currentBuffer = NULL;
    bufferList->TransitionToTail(&m_pendingList);
}

//
// Queue for processing or clean up if buffer queue is shutting down
//
void RChannelBufferQueue::ProcessBuffer(RChannelBuffer* buffer)
{
    WorkRequest* workRequest = NULL;
    bool returnBuffer = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_shutDownRequested == false);

        if (m_state == BQ_Empty)
        {
            //
            // If queue is currently empty, create a new work request for this buffer
            //
            m_state = BQ_InWorkQueue;
            LogAssert(m_currentBuffer == NULL);
            m_currentBuffer = buffer;
            // todo: remove comment if not logging
//             DrLogD( "scheduling parse");
            workRequest = new RChannelParseRequest(this, true);
        }
        else if (m_state == BQ_Stopping)
        {
            //
            // If queue is currently stopping, log and return
            //
            LogAssert(m_currentBuffer == NULL);
            LogAssert(m_pendingList.IsEmpty());
            returnBuffer = true;
            DrLogD( "stopping: returning buffer");
        }
        else
        {
            //
            // If queue is neither empty nor stopping, add buffer to tail of queue 
            // to be processed later
            //
            LogAssert(m_state == BQ_BlockingItem ||
                      m_state == BQ_InWorkQueue ||
                      m_state == BQ_Locked);
            LogAssert(m_currentBuffer != NULL);
            m_pendingList.InsertAsTail(m_pendingList.CastIn(buffer));
            // todo: remove comment if not logging
//             DrLogD( "queueing buffer");
        }
    }

    //
    // make all calls into other components with no locks held 
    //

    //
    // If stopping and buffer not being processed, return the buffer to the pool
    //
    if (returnBuffer)
    {
        buffer->ProcessingComplete(NULL);
    }

    //
    // If new work request was created, enqueue it in work queue
    //
    if (workRequest != NULL)
    {
        bool bRet = m_workQueue->EnQueue(workRequest);
        LogAssert(bRet == true);
    }
}

//
// Shut down queue if already requested, otherwise, mark it as locked
//
RChannelBuffer* RChannelBufferQueue::GetAndLockCurrentBuffer(bool* outReset)
{
    RChannelBuffer* buffer;
    ChannelBufferList bufferList;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_currentBuffer != NULL);
        LogAssert(m_state == BQ_InWorkQueue);

        if (m_shutDownRequested)
        {
            //
            // If shut down is requested, do it now
            //
            ShutDownBufferQueue(&bufferList);
            m_state = BQ_Stopping;
            BOOL bRet = ::SetEvent(m_shutDownEvent);
            LogAssert(bRet != 0);
            *outReset = false;
            LogAssert(m_currentBuffer == NULL);
        }
        else
        {
            //
            // If not shutting down, mark queue as locked 
            //
            m_state = BQ_Locked;
            *outReset = m_firstBuffer;
            m_firstBuffer = false;
            LogAssert(m_currentBuffer != NULL);
        }

        //
        // m_currentBuffer is NULL if and only if m_shutDownRequested
        // was true 
        //
        buffer = m_currentBuffer;
    }

    //
    // Return all buffers in list if queue is shutting down
    //
    DispatchEvents(&bufferList, NULL);

    return buffer;
}

void RChannelBufferQueue::NotifyUnitConsumption()
{
    WorkRequest* workRequest = NULL;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_outstandingUnits > 0);
        --m_outstandingUnits;

        if (m_state == BQ_BlockingItem &&
            m_outstandingUnits < m_maxOutstandingUnits)
        {
            workRequest = new RChannelParseRequest(this, false);
            m_state = BQ_InWorkQueue;
        }
    }

    if (workRequest != NULL)
    {
        bool bRet = m_workQueue->EnQueue(workRequest);
        LogAssert(bRet == true);
    }
}

void RChannelBufferQueue::
    ProcessAfterParsing(NextParseAction nextParseAction,
                        RChannelItemArray* itemArray,
                        UInt64 numberOfSubItemsRead,
                        UInt64 dataSizeRead,
                        RChannelBufferPrefetchInfo* prefetchCookie)
{
    ChannelUnitList unitList;
    ChannelBufferList bufferList;
    WorkRequest* workRequest = NULL;
    RChannelItemType lastItemType = RChannelItem_ItemHole;
    bool mustWakeUp = false;
    RChannelItem* lastItem = NULL;
    RChannelItemRef alertItem = NULL;

    if (itemArray->GetNumberOfItems() > 0)
    {
        lastItem =
            itemArray->GetItemArray()[itemArray->GetNumberOfItems()-1];
        lastItemType = lastItem->GetType();
    }

    if (lastItemType == RChannelItem_ParseError)
    {
        /* call into other components with no locks held.

          Make sure no more buffers will be delivered since we are
          going to stop parsing. When this returns we are sure no more
          calls to ProcessBuffer will come in so it's safe to call
          ShutDownBufferQueue below */
        m_bufferReader->Interrupt();
    }

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == BQ_Locked);
        LogAssert(m_currentBuffer != NULL);

        if (m_shutDownRequested)
        {
            /* a shutdown request came in from the application while
               we were in the parser, so we don't want to do anything
               except throw out all pending buffers and return. */
            ShutDownBufferQueue(&bufferList);
            m_state = BQ_Stopping;
            mustWakeUp = true;
        }
        else
        {
            if (lastItemType == RChannelItem_ParseError)
            {
                /* ditch any buffers we haven't parsed yet along with
                   the current buffer */
                ShutDownBufferQueue(&bufferList);
                m_state = BQ_Stopping;
                mustWakeUp = true;
            }
            else if (nextParseAction != NPA_RequestItem)
            {
                /* we're done with the current buffer; put it in the
                   unit queue for completion when the items in it have
                   been processed */
                RChannelUnit* unit =
                    new RChannelBufferBoundaryUnit(m_currentBuffer,
                                                   prefetchCookie);
                unitList.InsertAsTail(unitList.CastIn(unit));
                m_currentBuffer = NULL;
            }

            if (itemArray->GetNumberOfItems() > 0)
            {
                RChannelItemRef terminationItem;
                if (RChannelItem::IsTerminationItem(lastItemType))
                {
                    terminationItem = lastItem;
                }

                RChannelItemUnit* unit =
                    new RChannelSerializedUnit(this,
                                               itemArray, terminationItem);
                unit->SetSizes(numberOfSubItemsRead, dataSizeRead);
                ++m_outstandingUnits;

                unitList.InsertAsTail(unitList.CastIn(unit));
                if (nextParseAction == NPA_StopParsing)
                {
                    alertItem = lastItem;
                    if (alertItem != NULL)
                    {
                        /* reset the event so InterruptSupplier won't
                           complete until the application has been
                           alerted */
                        BOOL bRet = ::ResetEvent(m_alertCompleteEvent);
                        LogAssert(bRet != 0);
                    }
                }
            }

            switch (nextParseAction)
            {
            case NPA_RequestBuffer:
                LogAssert(m_currentBuffer == NULL);
                /* we would like to start processing the next buffer,
                   so put it in the queue if there is one */
                if (m_pendingList.IsEmpty())
                {
                    m_state = BQ_Empty;
                }
                else
                {
                    m_currentBuffer =
                        m_pendingList.CastOut(m_pendingList.RemoveHead());
                    workRequest = new RChannelParseRequest(this, true);
                    m_state = BQ_InWorkQueue;
                }
                break;

            case NPA_RequestItem:
                LogAssert(m_currentBuffer != NULL);
                if (m_outstandingUnits >= m_maxOutstandingUnits)
                {
                    m_state = BQ_BlockingItem;
                }
                else
                {
                    workRequest = new RChannelParseRequest(this, false);
                    m_state = BQ_InWorkQueue;
                }
                break;

            case NPA_EndOfStream:
                LogAssert(m_pendingList.IsEmpty());
                LogAssert(m_currentBuffer == NULL);
                LogAssert(lastItemType == RChannelItem_EndOfStream);
                m_state = BQ_Stopping;
                break;

            case NPA_StopParsing:
                LogAssert(m_pendingList.IsEmpty());
                LogAssert(m_currentBuffer == NULL);
                LogAssert(lastItemType == RChannelItem_ParseError ||
                          lastItemType == RChannelItem_Abort ||
                          lastItemType == RChannelItem_Restart);
                m_state = BQ_Stopping;
                break;

            default:
                LogAssert(false);
            }
        }

        m_sendLatch.AcceptList(&unitList);
    }

    /* make all calls into other components with no locks held */

    DispatchEvents(&bufferList, workRequest);

    while (unitList.IsEmpty() == false)
    {
        m_parent->AddUnitList(&unitList);

        {
            AutoCriticalSection acs(&m_baseCS);

            m_sendLatch.TransferList(&unitList);
        }
    }

    if (nextParseAction == NPA_StopParsing)
    {
        if (alertItem != NULL)
        {
            m_parent->AlertApplication(alertItem);
            /* set the event so InterruptSupplier can complete now
               that the application has been alerted */
            BOOL bRet = ::SetEvent(m_alertCompleteEvent);
            LogAssert(bRet != 0);
        }
    }
    else
    {
        LogAssert(alertItem == NULL);
    }

    if (mustWakeUp)
    {
        BOOL bRet = ::SetEvent(m_shutDownEvent);
        LogAssert(bRet != 0);
    }
}

//
// Called when RChannelParseRequest.Process is called in work queue
//
void RChannelBufferQueue::ParseRequest(bool useNewBuffer)
{
//     DrLogD(
//         "RChannelBufferQueue::ParseRequest entered",
//         "useNewBuffer: %s", (useNewBuffer) ? "true" : "false");

    RChannelBuffer* buffer;
    bool resetParser = false;
    UInt64 numberOfSubItemsRead = 0;
    UInt64 dataSizeRead = 0;

    //
    // Handle shutdown logic if necessary or lock the queue
    //
    buffer = GetAndLockCurrentBuffer(&resetParser);
    if (buffer == NULL)
    {
        //
        // there was a shutdown request while this work item was in
        // the queue, which has now been dealt with, so just exit 
        //
        return;
    }

    RChannelBufferType bufferType = buffer->GetType();
    RChannelItemArrayRef itemArray;
    itemArray.Attach(new RChannelItemArray());
    RChannelBufferPrefetchInfo* prefetchCookie = NULL;
    NextParseAction nextParseAction;

    if (bufferType == RChannelBuffer_Data ||
        bufferType == RChannelBuffer_Hole ||
        bufferType == RChannelBuffer_EndOfStream)
    {
        /* get the parser to give us the next item, if any */
        RChannelBuffer* parseBuffer = (useNewBuffer) ? buffer : NULL;

        UInt32 parsedItemCount = 0;
        itemArray->SetNumberOfItems(m_maxParseBatchSize);
        RChannelItemRef* items = itemArray->GetItemArray();

        do
        {
            RChannelItem* item =
                m_parser->RawParseItem(resetParser, parseBuffer,
                                       &prefetchCookie);
            resetParser = false;
            parseBuffer = NULL;

            if (item == NULL)
            {
                /* the parser should only ask for another buffer if
                   there's more data and it didn't give back an item */
                LogAssert(bufferType != RChannelBuffer_EndOfStream);
                nextParseAction = NPA_RequestBuffer;
            }
            else
            {
                LogAssert(prefetchCookie == NULL);

                RChannelItemType itemType = item->GetType();
                if (itemType == RChannelItem_EndOfStream)
                {
                    LogAssert(bufferType == RChannelBuffer_EndOfStream);
                    nextParseAction = NPA_EndOfStream;
                }
                else if (itemType == RChannelItem_ParseError)
                {
                    nextParseAction = NPA_StopParsing;
                }
                else
                {
                    LogAssert(itemType == RChannelItem_Data ||
                              itemType == RChannelItem_ItemHole ||
                              itemType == RChannelItem_BufferHole);
                    nextParseAction = NPA_RequestItem;
                }

                item->SetDeliverySequenceNumber(m_nextDeliverySequenceNumber);
                ++m_nextDeliverySequenceNumber;
                /* Only data items merit a new sequence number; everything
                   else gets the sequence number of the next data item. */
                item->SetDataSequenceNumber(m_nextDataSequenceNumber);
                if (itemType == RChannelItem_Data)
                {
                    ++m_nextDataSequenceNumber;
                }

                numberOfSubItemsRead += item->GetNumberOfSubItems();
                dataSizeRead += item->GetItemSize();

                items[parsedItemCount].Attach(item);
                ++parsedItemCount;
            }
        } while (nextParseAction == NPA_RequestItem &&
                 parsedItemCount < m_maxParseBatchSize);

        itemArray->TruncateToSize(parsedItemCount);
    }
    else
    {
        LogAssert(useNewBuffer == true);
        RChannelBufferMarker* markerBuffer = (RChannelBufferMarker *) buffer;
        RChannelItem* item = markerBuffer->GetItem();

        item->SetDeliverySequenceNumber(m_nextDeliverySequenceNumber);
        ++m_nextDeliverySequenceNumber;
        /* Only data items merit a new sequence number; everything
           else gets the sequence number of the next data item. */
        item->SetDataSequenceNumber(m_nextDataSequenceNumber);

        itemArray->SetNumberOfItems(1);
        itemArray->GetItemArray()[0] = item;
        LogAssert(bufferType == RChannelBuffer_Restart ||
                  bufferType == RChannelBuffer_Abort);
        nextParseAction = NPA_StopParsing;
    }

    ProcessAfterParsing(nextParseAction,
                        itemArray, numberOfSubItemsRead, dataSizeRead,
                        prefetchCookie);
}

//
// Stop creation of new buffers
//
void RChannelBufferQueue::InterruptSupplier()
{
    bool waitForSend = false;
    bool waitForLock = false;

    //
    // Interrupt the reader associated with this queue
    // call into other components with no locks held. When this
    // returns we are sure no more calls to ProcessBuffer will come in
    //
    m_bufferReader->Interrupt();

    WorkRequest* workRequest = NULL;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_shutDownRequested == false);

        if (m_state == BQ_BlockingItem)
        {
            //
            // the easiest thing to do is to transition into the
            // InWorkQueue state and continue interrupting using the
            // normal path 
            //
            workRequest = new RChannelParseRequest(this, false);
            m_state = BQ_InWorkQueue;
        }

        if (m_state == BQ_Locked ||
            m_state == BQ_InWorkQueue)
        {
            //
            // If queue is locked or working, we're going to
            // request a shutdown but wait for the lock
            //
            m_shutDownRequested = true;
            waitForLock = true;
            BOOL bRet = ::ResetEvent(m_shutDownEvent);
            LogAssert(bRet != 0);
        }
        else
        {
            if (m_state == BQ_Empty)
            {
                m_state = BQ_Stopping;
            }

            LogAssert(m_state == BQ_Stopping);
            LogAssert(m_currentBuffer == NULL);
            LogAssert(m_pendingList.IsEmpty());
        }

        //
        // Interrupt the send latch. waitForSend = true if currently sending
        //
        waitForSend = m_sendLatch.Interrupt();
    }

    if (workRequest != NULL)
    {
        //
        // If blocking, add request to queue to call this.ParseRequest, which will shut down the queue
        //
        bool bRet = m_workQueue->EnQueue(workRequest);
        LogAssert(bRet == true);
    }

    if (waitForSend)
    {
        //
        // Wait on send latch to stop sending if necessary
        //
        m_sendLatch.Wait();
    }

    //
    // Wait until any alerts have been delivered to the application 
    //
    BOOL bRet = ::WaitForSingleObject(m_alertCompleteEvent, INFINITE);
    LogAssert(bRet == WAIT_OBJECT_0);

    if (waitForLock)
    {
        //
        // Wait for shut down if work queue is still processing
        //
        bRet = ::WaitForSingleObject(m_shutDownEvent, INFINITE);
        LogAssert(bRet == WAIT_OBJECT_0);
    }

    {
        //
        // Ensure that everything is shut down correctly
        //
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == BQ_Stopping);
        LogAssert(m_currentBuffer == NULL);
        LogAssert(m_pendingList.IsEmpty());
        m_shutDownRequested = false;
    }
}

void RChannelBufferQueue::DrainSupplier(RChannelItem* drainItem)
{
    LogAssert(RChannelItem::IsTerminationItem(drainItem->GetType()));

    InterruptSupplier();

    /* this won't return until all outstanding buffers have had their
       completion handlers called. By the time this happens everything
       must have drained out of the work queue and the parser */
    m_bufferReader->Drain(drainItem);

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == BQ_Stopping);
        LogAssert(m_outstandingUnits == 0);
        LogAssert(m_currentBuffer == NULL);
        LogAssert(m_pendingList.IsEmpty());
        m_state = BQ_Stopped;
        m_sendLatch.Stop();
    }
}

void RChannelBufferQueue::CloseSupplier()
{
    m_bufferReader->Close();

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == BQ_Stopped);
        m_bufferReader = NULL;
        m_parser = NULL;
    }
}

bool RChannelBufferQueue::GetTotalLength(UInt64* pLen)
{
    return m_bufferReader->GetTotalLength(pLen);
}
