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

#include <DrExecution.h>
#include <channelfifo.h>
#include <channelhelpers.h>

#pragma unmanaged


RChannelFifoWriterBase::SyncHandler::SyncHandler()
{
    m_event = NULL;
    m_statusCode = RChannelItem_Data;
    m_usingEvent = 0;
}

RChannelFifoWriterBase::SyncHandler::~SyncHandler()
{
    LogAssert(m_event == NULL);
}

void RChannelFifoWriterBase::SyncHandler::UseEvent(DryadHandleListEntry* event)
{
    LONG postIncrement = ::InterlockedIncrement(&m_usingEvent);
    LogAssert(postIncrement == 1);
    LogAssert(m_event == NULL);
    m_event = event;
}

void RChannelFifoWriterBase::SyncHandler::
    ProcessWriteArrayCompleted(RChannelItemType statusCode,
                               RChannelItemArray* failureArray)
{
    LogAssert(failureArray == NULL);
    m_statusCode = statusCode;
    LONG postIncrement = ::InterlockedIncrement(&m_usingEvent);
    if (postIncrement == 2)
    {
        LogAssert(m_event != NULL);
        BOOL bRet = ::SetEvent(m_event->GetHandle());
        LogAssert(bRet != 0);
    }
    else
    {
        LogAssert(postIncrement == 1);
        LogAssert(m_event == NULL);
    }
}

bool RChannelFifoWriterBase::SyncHandler::UsingEvent()
{
    return (m_event != NULL);
}

RChannelItemType RChannelFifoWriterBase::SyncHandler::GetStatusCode()
{
    return m_statusCode;
}

void RChannelFifoWriterBase::SyncHandler::Wait()
{
    LogAssert(m_event != NULL);
    DWORD dRet = ::WaitForSingleObject(m_event->GetHandle(), INFINITE);
    LogAssert(dRet == WAIT_OBJECT_0);
}

DryadHandleListEntry* RChannelFifoWriterBase::SyncHandler::GetEvent()
{
    LogAssert(m_event != NULL);
    DryadHandleListEntry* retVal = m_event;
    m_event = NULL;
    return retVal;
}

RChannelFifoWriterBase::RChannelFifoWriterBase(RChannelFifo* parent)
{
    m_parent = parent;
    m_reader = NULL;
    m_writerState = WS_Closed;
    m_supplierState = SS_Closed;
    m_outstandingUnits = 0;
    m_nextDataSequenceNumber = 0;
    m_nextDeliverySequenceNumber = 0;
    m_numberOfSubItemsWritten = 0;
    m_dataSizeWritten = 0;
    m_supplierDrainEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_supplierDrainEvent != NULL);
    m_supplierEpoch = 0;
    m_writerEpoch = 0;
}

RChannelFifoWriterBase::~RChannelFifoWriterBase()
{
    BOOL bRet = ::CloseHandle(m_supplierDrainEvent);
    LogAssert(bRet != 0);
}

void RChannelFifoWriterBase::SetURI(const char* uri)
{
    m_uri = uri;
}

const char* RChannelFifoWriterBase::GetURI()
{
    return m_uri;
}

UInt64 RChannelFifoWriterBase::GetInitialSizeHint()
{
    return 0;
}

void RChannelFifoWriterBase::SetInitialSizeHint(UInt64 /*hint*/)
{
}

void RChannelFifoWriterBase::SetReader(RChannelReaderImpl* reader)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_reader == NULL);
        LogAssert(reader != NULL);
        m_reader = reader;
        LogAssert(m_writerState == WS_Closed);
        LogAssert(m_supplierState == SS_Closed);
        LogAssert(m_readerTerminationItem == NULL);
        LogAssert(m_writerTerminationItem == NULL);
        m_writerState = WS_Stopped;
        m_supplierState = SS_Stopped;
    }
}

RChannelFifo* RChannelFifoWriterBase::GetParent()
{
    return m_parent;
}

void RChannelFifoWriterBase::ReturnHandlers(ChannelFifoUnitList* returnList)
{
    while (returnList->IsEmpty() == false)
    {
        RChannelFifoUnit* returnUnit =
            returnList->CastOut(returnList->RemoveHead());
        while (returnUnit != NULL)
        {
            RChannelItemArrayWriterHandler* handler;
            RChannelItemType statusCode;
            returnUnit->Disgorge(&handler, &statusCode);
            if (handler != NULL)
            {
                handler->ProcessWriteArrayCompleted(statusCode, NULL);
            }
            delete returnUnit;
            returnUnit =
                returnList->CastOut(returnList->RemoveHead());
        }

        {
            AutoCriticalSection acs(&m_baseDR);

            m_returnLatch.TransferList(returnList);
        }
    }
}

void RChannelFifoWriterBase::SendUnits(ChannelUnitList* unitList)
{
    while (unitList->IsEmpty() == false)
    {
        m_reader->AddUnitList(unitList);

        {
            AutoCriticalSection acs(&m_baseDR);

            m_sendLatch.TransferList(unitList);
        }
    }
}

RChannelFifoUnit*
    RChannelFifoWriterBase::DuplicateUnit(RChannelFifoUnit* unit,
                                          RChannelItemArray* itemArray)
{
    RChannelFifoUnit* sendUnit = new RChannelFifoUnit(this);
    sendUnit->SetSizes(unit->GetNumberOfSubItems(),
                       unit->GetDataSize());
    sendUnit->SetPayload(itemArray, NULL, RChannelItem_Data);
    RChannelItem* t = unit->GetTerminationItem();
    if (t != NULL)
    {
        sendUnit->SetTerminationItem(t);
    }
    unit->DiscardItems();
    return sendUnit;
}

void RChannelFifoWriterBase::
    WriteItemArray(RChannelItemArrayRef& itemArray,
                   bool flushAfter,
                   RChannelItemArrayWriterHandler* handler)
{
    EnqueueItemArray(itemArray, handler, NULL);
}

RChannelItemType RChannelFifoWriterBase::
    WriteItemArraySync(RChannelItemArrayRef& itemArray,
                       bool flush,
                       RChannelItemArrayRef* pFailureArray)
{
    SyncHandler syncHandler;

    if (pFailureArray != NULL)
    {
        *pFailureArray = NULL;
    }

    EnqueueItemArray(itemArray, &syncHandler, &syncHandler);

    if (syncHandler.UsingEvent())
    {
        syncHandler.Wait();

        {
            AutoCriticalSection acs(&m_baseDR);

            m_eventCache.ReturnEvent(syncHandler.GetEvent());
        }
    }

    return syncHandler.GetStatusCode();
}

UInt64 RChannelFifoWriterBase::GetDataSizeWritten()
{
    return m_dataSizeWritten;
}

void RChannelFifoWriterBase::CloseSupplier()
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Stopped);
        m_supplierState = SS_Closed;
    }
}

void RChannelFifoWriterBase::Close()
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_writerState == WS_Stopped);
        m_writerState = WS_Closed;
        m_writerTerminationItem = NULL;
        m_readerTerminationItem = NULL;
    }
}


RChannelFifoWriter::RChannelFifoWriter(RChannelFifo* parent,
                                       UInt32 fifoLength) :
    RChannelFifoWriterBase(parent)
{
    m_fifoLength = fifoLength;
    m_availableUnits = 0;
    m_terminationUnit = NULL;
    m_writerDrainEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_writerDrainEvent != NULL);
}

RChannelFifoWriter::~RChannelFifoWriter()
{
    BOOL bRet = ::CloseHandle(m_writerDrainEvent);
    LogAssert(bRet != 0);
}

void RChannelFifoWriter::Start()
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_writerState == WS_Stopped);
        LogAssert(m_availableUnits == 0);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_reader != NULL);
        LogAssert(m_nextDataSequenceNumber == 0);
        LogAssert(m_nextDeliverySequenceNumber == 0);
        LogAssert(m_outstandingUnits == 0);
        LogAssert(m_terminationUnit == NULL);

        m_sendLatch.Start();
        m_returnLatch.Start();

        m_writerTerminationItem = NULL;
        m_readerTerminationItem = NULL;
        m_numberOfSubItemsWritten = 0;
        m_dataSizeWritten = 0;

        LogAssert(m_supplierEpoch == m_writerEpoch);

        BOOL bRet = ::ResetEvent(m_writerDrainEvent);
        LogAssert(bRet != 0);

        if (m_supplierState == SS_Running)
        {
            m_availableUnits = m_fifoLength;
        }
        m_writerState = WS_Running;
    }
}

void RChannelFifoWriter::
    StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie)
{
    ChannelUnitList unblockedList;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Stopped);

        LogAssert(m_writerState != WS_Closed);
        LogAssert(m_availableUnits == 0);
        LogAssert(m_outstandingUnits == 0);

        if (m_writerState == WS_Running)
        {
            m_availableUnits = m_fifoLength;
        }
        else
        {
            if (m_writerState == WS_Draining)
            {
                LogAssert(m_blockedList.IsEmpty() == false);
                m_availableUnits = m_fifoLength;
            }
            else
            {
                LogAssert(m_writerState == WS_Stopped);
                LogAssert(m_blockedList.IsEmpty());
            }
        }

        BOOL bRet = ::ResetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);

        m_supplierState = SS_Running;

        while (m_availableUnits > 0 && m_blockedList.IsEmpty() == false)
        {
            --m_availableUnits;
            ++m_outstandingUnits;

            RChannelFifoUnit* blockedUnit =
                m_blockedList.CastOut(m_blockedList.GetHead());

            unblockedList.TransitionToTail(unblockedList.
                                           CastIn(blockedUnit));
        }

        if (m_writerTerminationItem != NULL)
        {
            if (m_terminationUnit == NULL)
            {
                LogAssert(m_writerState == WS_Stopped);
                LogAssert(m_writerEpoch == m_supplierEpoch);
            }
            m_availableUnits = 0;
        }

        m_sendLatch.AcceptList(&unblockedList);
    }

    SendUnits(&unblockedList);
}

/* called with m_baseDR held */
bool RChannelFifoWriter::CheckForTerminationItem(RChannelFifoUnit* unit)
{
    if (m_writerTerminationItem != NULL || m_writerState == WS_Stopped)
    {
        LogAssert(m_availableUnits == 0);
        unit->DiscardItems();
        return true;
    }
    else
    {
        /* we can't be in the draining state if we haven't seen a
           termination item */
        LogAssert(m_writerState != WS_Draining);

        RChannelItemArray* itemArray = unit->GetItemArray();
        UInt32 nItems = itemArray->GetNumberOfItems();
        LogAssert(nItems > 0);
        RChannelItemRef* items = itemArray->GetItemArray();

        UInt64 unitSubItems = 0;
        UInt64 unitDataSize = 0;
        UInt32 i;
        for (i=0; i<nItems; ++i)
        {
            RChannelItem* item = items[i];

            RChannelItemType itemType = item->GetType();
            if (RChannelItem::IsTerminationItem(itemType))
            {
                m_writerTerminationItem = item;
                LogAssert(m_terminationUnit == NULL);
                m_terminationUnit = unit;
                itemArray->TruncateToSize(i+1);
                nItems = i+1;
                unit->SetTerminationItem(item);
            }

            item->SetDeliverySequenceNumber(m_nextDeliverySequenceNumber);
            ++m_nextDeliverySequenceNumber;
            /* Only data items merit a new sequence number;
               everything else gets the sequence number of the
               next data item. */
            item->SetDataSequenceNumber(m_nextDataSequenceNumber);
            if (itemType == RChannelItem_Data)
            {
                ++m_nextDataSequenceNumber;
            }
            m_numberOfSubItemsWritten += item->GetNumberOfSubItems();
            m_dataSizeWritten += item->GetItemSize();
            unitSubItems += item->GetNumberOfSubItems();
            unitDataSize += item->GetItemSize();
        }

        unit->SetSizes(unitSubItems, unitDataSize);

        return false;
    }
}

void RChannelFifoWriter::StartBlocking(RChannelFifoUnit* unit,
                                       SyncHandler* handler)
{
    if (handler != NULL)
    {
        /* if we have been called from WriteItemSync then we want to
           use an event inside the handler. Since resetting an event
           is expensive we don't want to do it on every call to
           WriteItemSync, and here inside this lock is the only place
           we can get it on demand. */
        handler->UseEvent(m_eventCache.GetEvent(true));
    }

    m_blockedList.InsertAsTail(m_blockedList.CastIn(unit));
}

void RChannelFifoWriter::
    EnqueueItemArray(RChannelItemArrayRef& itemArrayIn,
                     RChannelItemArrayWriterHandler* handler,
                     SyncHandler* syncHandler)
{
    ChannelUnitList sendList;
    ChannelFifoUnitList returnList;
    bool writerHasTerminated = false;
    RChannelFifoUnit* unit = new RChannelFifoUnit(this);

    RChannelItemArrayRef itemArray;
    /* ensure that the caller no longer holds a reference to this array */
    itemArray.TransferFrom(itemArrayIn);

    LogAssert(itemArray->GetNumberOfItems() > 0);

    {
        AutoCriticalSection acs(&m_baseDR);

        unit->SetPayload(itemArray, handler, RChannelItem_MarshalError);

        LogAssert(m_writerState != WS_Closed);

        writerHasTerminated = CheckForTerminationItem(unit);

        if (writerHasTerminated)
        {
            /* some previous invocation (perhaps is another thread)
               has already sent a termination item so signal that we
               aren't accepting any more items by just returning the
               handler immediately with this status code */
            unit->SetStatusCode(RChannelItem_EndOfStream);
        }
        else
        {
            LogAssert(m_writerState == WS_Running);

            if (m_supplierState == SS_Interrupted)
            {
                /* the writer has not terminated, but the reader is
                   about to request a drain. Queue up the handler to
                   be returned with the appropriate code once the
                   drain actually arrives.  */
                LogAssert(m_readerTerminationItem == NULL);
                LogAssert(m_availableUnits == 0);
                LogAssert(unit->GetItemArray()->GetNumberOfItems() > 0);

                StartBlocking(unit, syncHandler);
                unit = NULL;
            }
            else if (m_supplierState == SS_Draining)
            {
                /* the writer has not terminated, but the reader has
                   requested a drain. */
                LogAssert(m_readerTerminationItem != NULL);
                RChannelItemType drainType =
                    m_readerTerminationItem->GetType();
                LogAssert(drainType != RChannelItem_MarshalError);
                LogAssert(m_availableUnits == 0);

                if (m_outstandingUnits > 0)
                {
                    /* queue up this item to be returned after the
                       in-flight items have come back. At that point
                       the extra item reference will be removed, so
                       leave it there for now. */
                    LogAssert(unit->GetItemArray()->GetNumberOfItems() > 0);
                    StartBlocking(unit, syncHandler);
                    unit = NULL;
                }
                else
                {
                    /* if there are no in-flight items we should have
                       returned the handlers from any previous items
                       already so we can return this handler
                       immediately */
                    LogAssert(m_blockedList.IsEmpty());
                    unit->SetStatusCode(drainType);
                    unit->DiscardItems();

                    if (m_terminationUnit == unit)
                    {
                        m_terminationUnit = NULL;
                    }
                }
            }
            else if (m_supplierState == SS_Stopped)
            {
                LogAssert(m_availableUnits == 0);

                if (m_supplierEpoch == m_writerEpoch)
                {
                    /* the supplier hasn't yet been started so queue
                       up the unit for when it is */
                    unit->SetStatusCode(RChannelItem_Data);
                    LogAssert(unit->GetItemArray()->GetNumberOfItems() > 0);

                    StartBlocking(unit, syncHandler);
                    unit = NULL;
                }
                else
                {
                    /* the supplier has already shut down so just
                       return this immediately */
                    LogAssert(m_supplierEpoch == m_writerEpoch+1);
                    LogAssert(m_readerTerminationItem != NULL);
                    RChannelItemType drainType =
                        m_readerTerminationItem->GetType();
                    LogAssert(drainType != RChannelItem_MarshalError);
                    LogAssert(m_blockedList.IsEmpty());
                    unit->DiscardItems();
                    unit->SetStatusCode(drainType);

                    if (m_terminationUnit == unit)
                    {
                        m_terminationUnit = NULL;
                    }
                }
            }
            else
            {
                LogAssert(m_supplierState == SS_Running);

                LogAssert(unit->GetItemArray()->GetNumberOfItems() > 0);
                unit->SetStatusCode(RChannelItem_Data);

                if (m_availableUnits == 0)
                {
                    /* we have no spare slots in the FIFO so add the
                       item to the blocked list: this has the
                       side-effect of preventing the handler being
                       returned immediately. */
                    StartBlocking(unit, syncHandler);
                    unit = NULL;
                }
                else
                {
                    LogAssert(m_blockedList.IsEmpty());

                    /* we're ready to send this on to the reader: use
                       the send latch to ensure the ordering is
                       preserved. We will return the handler
                       immediately since the FIFO was not full and it
                       is OK to send more items. */

                    --m_availableUnits;

                    RChannelFifoUnit* sendUnit;
                    if (unit == m_terminationUnit)
                    {
                        /* always block on the termination item to
                           ensure its handler gets delivered last */
                        StartBlocking(unit, syncHandler);
                        unit = NULL;

                        /* cheat by stealing this back off the end of
                           the blocking queue and send it
                           immediately */
                        sendUnit = m_blockedList.CastOut(m_blockedList.
                                                         RemoveTail());

                        /* throw away any spare FIFO units since we
                           won't be writing any more items */
                        m_availableUnits = 0;
                    }
                    else
                    {
                        /* return the handler to the user immediately
                           while also forwarding the unit to the
                           reader */
                        sendUnit = DuplicateUnit(unit, itemArray);
                    }

                    sendList.InsertAsTail(sendList.CastIn(sendUnit));
                    m_sendLatch.AcceptList(&sendList);

                    ++m_outstandingUnits;
                }
            }
        }

        if (unit != NULL)
        {
            LogAssert(unit->GetStatusCode() != RChannelItem_MarshalError);
            returnList.InsertAsTail(returnList.CastIn(unit));
            m_returnLatch.AcceptList(&returnList);
            if (returnList.IsEmpty() && syncHandler != NULL)
            {
                /* another thread is returning the handler for us, so
                   we need to wait for its event */
                syncHandler->UseEvent(m_eventCache.GetEvent(true));
            }
        }
    }

    if (writerHasTerminated)
    {
        LogAssert(sendList.IsEmpty());
        LogAssert(unit != NULL);
    }

    SendUnits(&sendList);

    ReturnHandlers(&returnList);
}

/* called with m_baseDR held */
bool RChannelFifoWriter::ReWriteBlockedListForEarlyReturn(RChannelItemType
                                                          drainType)
{
    /* rewrite any blocked items to have the termination code correct
       before sending them back to the writer after a reader
       Drain. Also, discard the items, since the reader is never going
       to see them. */
    bool wakeUpWriterDrain = false;
    DrBListEntry* listEntry = m_blockedList.GetHead();
    while (listEntry != NULL)
    {
        RChannelFifoUnit* unit = m_blockedList.CastOut(listEntry);
        listEntry = m_blockedList.GetNext(listEntry);
        unit->SetStatusCode(drainType);
        unit->DiscardItems();
        if (unit == m_terminationUnit)
        {
            LogAssert(listEntry == NULL);
            m_terminationUnit = NULL;
            if (m_writerState == WS_Draining)
            {
                wakeUpWriterDrain = true;
            }
        }
    }

    return wakeUpWriterDrain;
}

void RChannelFifoWriter::AcceptReturningUnit(RChannelFifoUnit* unit)
{
    ChannelUnitList unblockedList;
    ChannelFifoUnitList returnList;
    bool wakeUpWriterDrain = false;
    bool wakeUpSupplierDrain = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_outstandingUnits > 0);
        --m_outstandingUnits;

        if (unit == m_terminationUnit)
        {
            LogAssert(m_outstandingUnits == 0);
            LogAssert(m_blockedList.IsEmpty());
            m_terminationUnit = NULL;

            if (m_writerState == WS_Draining)
            {
                wakeUpWriterDrain = true;
            }
        }

        returnList.InsertAsTail(returnList.CastIn(unit));
        unit = NULL;

        if (m_supplierState == SS_Draining)
        {
            /* the supplier has requested a drain */
            if (m_outstandingUnits == 0)
            {
                /* we have been building up written items in the
                   blocked list while waiting for the outstanding
                   units to be returned. We can send them all back
                   now. */
                LogAssert(m_readerTerminationItem != NULL);
                RChannelItemType drainType =
                    m_readerTerminationItem->GetType();
                wakeUpWriterDrain =
                    ReWriteBlockedListForEarlyReturn(drainType) ||
                    wakeUpWriterDrain;
                returnList.TransitionToTail(&m_blockedList);
                m_supplierState = SS_Stopped;
                ++m_supplierEpoch;
                wakeUpSupplierDrain = true;
            }

            LogAssert(m_availableUnits == 0);
        }
        else if (m_supplierState != SS_Interrupted)
        {
            LogAssert(m_supplierState == SS_Running);

            if (m_blockedList.IsEmpty())
            {
                if (m_writerTerminationItem == NULL)
                {
                    /* there's nothing blocked, so just add a unit to
                       the available list and return without doing
                       anything else */
                    ++m_availableUnits;
                }
            }
            else
            {
                /* there was a blocked request waiting to be sent to
                   the FIFO: put it in now that we have a space
                   available. */
                LogAssert(m_supplierState == SS_Running);

                RChannelFifoUnit* blockedUnit =
                    m_blockedList.CastOut(m_blockedList.GetHead());

                unblockedList.TransitionToTail(unblockedList.
                                               CastIn(blockedUnit));
                ++m_outstandingUnits;

                m_sendLatch.AcceptList(&unblockedList);
            }
        }

        m_returnLatch.AcceptList(&returnList);
    }

    if (wakeUpWriterDrain)
    {
        LogAssert(unblockedList.IsEmpty());
        BOOL bRet = ::SetEvent(m_writerDrainEvent);
        LogAssert(bRet != 0);
    }
    else
    {
        SendUnits(&unblockedList);
    }

    ReturnHandlers(&returnList);

    if (wakeUpSupplierDrain)
    {
        BOOL bRet = ::SetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);
    }
}

void RChannelFifoWriter::InterruptSupplier()
{
    bool waitForLatch;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Running);

        /* remove any spare FIFO slots since we aren't going to send
           anything more to the supplier */
        m_availableUnits = 0;

        m_supplierState = SS_Interrupted;

        waitForLatch = m_sendLatch.Interrupt();
    }

    if (waitForLatch)
    {
        /* make sure all sends which were proceeding outside a lock
           have completed */
        m_sendLatch.Wait();
    }
}

void RChannelFifoWriter::DrainSupplier(RChannelItem* drainItem)
{
    ChannelFifoUnitList returnList;

    bool wakeUpWriterDrain = false;
    bool wakeUpSupplierDrain = false;
    bool waitForSupplierDrain = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Interrupted);
        LogAssert(m_availableUnits == 0);

        LogAssert(drainItem != NULL);
        RChannelItemType drainType = drainItem->GetType();

        m_readerTerminationItem = drainItem;

        if (m_outstandingUnits > 0)
        {
            m_supplierState = SS_Draining;
            waitForSupplierDrain = true;
        }
        else
        {
            wakeUpWriterDrain = ReWriteBlockedListForEarlyReturn(drainType);
            returnList.TransitionToTail(&m_blockedList);
            m_returnLatch.AcceptList(&returnList);
            m_supplierState = SS_Stopped;
            ++m_supplierEpoch;
            wakeUpSupplierDrain = true;
        }
    }

    if (wakeUpWriterDrain)
    {
        BOOL bRet = ::SetEvent(m_writerDrainEvent);
        LogAssert(bRet != 0);
    }

    if (wakeUpSupplierDrain)
    {
        LogAssert(waitForSupplierDrain == false);
        BOOL bRet = ::SetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);
    }
    else if (waitForSupplierDrain)
    {
        DWORD dRet = ::WaitForSingleObject(m_supplierDrainEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    ReturnHandlers(&returnList);
}

void RChannelFifoWriter::Drain(DrTimeInterval csTimeOut,
                               RChannelItemRef* pReturnItem)
{
    DrTimeStamp startTime = DrGetCurrentTimeStamp();

    bool waitForWriterDrain = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_writerState == WS_Running);
        LogAssert(m_writerTerminationItem != NULL);
        LogAssert(m_availableUnits == 0);

        if (m_supplierState == SS_Running && m_outstandingUnits == 0)
        {
            LogAssert(m_blockedList.IsEmpty());
        }

        if (m_outstandingUnits > 0 || m_blockedList.IsEmpty() == false)
        {
            LogAssert(m_terminationUnit != NULL);
            m_writerState = WS_Draining;
            waitForWriterDrain = true;
        }
    }

    if (waitForWriterDrain)
    {
        DWORD dRet = ::WaitForSingleObject(m_writerDrainEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_outstandingUnits == 0);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_availableUnits == 0);
        LogAssert(m_reader != NULL);
        LogAssert(m_outstandingUnits == 0);
        LogAssert(m_terminationUnit == NULL);

        m_writerState = WS_Stopped;
        ++m_writerEpoch;
        m_nextDeliverySequenceNumber = 0;
        m_nextDataSequenceNumber = 0;
    }

    DrTimeStamp currentTime = DrGetCurrentTimeStamp();
    DrTimeInterval elapsed = DrGetElapsedTime(startTime, currentTime);
    if (elapsed < csTimeOut)
    {
        DWORD timeOut = DrGetTimerMsFromInterval(csTimeOut - elapsed);
        DWORD dRet = ::WaitForSingleObject(m_supplierDrainEvent, timeOut);
        LogAssert(dRet == WAIT_TIMEOUT || dRet == WAIT_OBJECT_0);
    }

    if (pReturnItem != NULL)
    {
        AutoCriticalSection acs(&m_baseDR);

        *pReturnItem = m_readerTerminationItem;
    }
}

void RChannelFifoWriter::GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                                             RChannelItemRef* pReaderDrainItem)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        *pWriterDrainItem = m_writerTerminationItem;
        *pReaderDrainItem = m_readerTerminationItem;
    }
}


RChannelFifoNBWriter::RChannelFifoNBWriter(RChannelFifo* parent) :
    RChannelFifoWriterBase(parent)
{
}

void RChannelFifoNBWriter::Start()
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_writerState == WS_Stopped);
        LogAssert(m_outstandingUnits == 0);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_reader != NULL);
        LogAssert(m_nextDataSequenceNumber == 0);
        LogAssert(m_nextDeliverySequenceNumber == 0);

        m_sendLatch.Start();
        m_returnLatch.Start();

        m_writerTerminationItem = NULL;
        m_readerTerminationItem = NULL;
        m_numberOfSubItemsWritten = 0;
        m_dataSizeWritten = 0;

        LogAssert(m_supplierEpoch == m_writerEpoch);

        m_writerState = WS_Running;
    }
}

void RChannelFifoNBWriter::
    StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie)
{
    ChannelUnitList unblockedList;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Stopped);

        LogAssert(m_writerState != WS_Closed);
        LogAssert(m_outstandingUnits == 0);

        BOOL bRet = ::ResetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);

        m_supplierState = SS_Running;

        while (m_blockedList.IsEmpty() == false)
        {
            ++m_outstandingUnits;

            RChannelFifoUnit* blockedUnit =
                m_blockedList.CastOut(m_blockedList.GetHead());

            unblockedList.TransitionToTail(unblockedList.
                                           CastIn(blockedUnit));
        }

        m_sendLatch.AcceptList(&unblockedList);
    }

    SendUnits(&unblockedList);
}

/* called with m_baseDR held */
bool RChannelFifoNBWriter::CheckForTerminationItem(RChannelFifoUnit* unit)
{
    if (m_writerTerminationItem != NULL || m_writerState == WS_Stopped)
    {
        unit->DiscardItems();
        return true;
    }
    else
    {
        RChannelItemArray* itemArray = unit->GetItemArray();
        UInt32 nItems = itemArray->GetNumberOfItems();
        LogAssert(nItems > 0);
        RChannelItemRef* items = itemArray->GetItemArray();

        UInt64 unitSubItems = 0;
        UInt64 unitDataSize = 0;
        UInt32 i;
        for (i=0; i<nItems; ++i)
        {
            RChannelItem* item = items[i];

            RChannelItemType itemType = item->GetType();
            if (RChannelItem::IsTerminationItem(itemType))
            {
                m_writerTerminationItem = item;
                itemArray->TruncateToSize(i+1);
                nItems = i+1;
                unit->SetTerminationItem(item);
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
            m_numberOfSubItemsWritten += item->GetNumberOfSubItems();
            m_dataSizeWritten += item->GetItemSize();
            unitSubItems += item->GetNumberOfSubItems();
            unitDataSize += item->GetItemSize();
        }

        unit->SetSizes(unitSubItems, unitDataSize);

        return false;
    }
}

void RChannelFifoNBWriter::
    EnqueueItemArray(RChannelItemArrayRef& itemArrayIn,
                     RChannelItemArrayWriterHandler* handler,
                     SyncHandler* syncHandler)
{
    ChannelUnitList sendList;
    ChannelFifoUnitList returnList;
    bool writerHasTerminated = false;
    RChannelFifoUnit* unit = new RChannelFifoUnit(this);

    RChannelItemArrayRef itemArray;
    /* ensure that the caller no longer holds a reference to this array */
    itemArray.TransferFrom(itemArrayIn);

    LogAssert(itemArray->GetNumberOfItems() > 0);

    {
        AutoCriticalSection acs(&m_baseDR);

        unit->SetPayload(itemArray, handler, RChannelItem_MarshalError);

        LogAssert(m_writerState != WS_Closed);

        writerHasTerminated = CheckForTerminationItem(unit);

        if (writerHasTerminated)
        {
            /* some previous invocation (perhaps is another thread)
               has already sent a termination item so signal that we
               aren't accepting any more items by just returning the
               handler immediately with this status code */
            unit->SetStatusCode(RChannelItem_EndOfStream);
        }
        else
        {
            LogAssert(m_writerState == WS_Running);

            if (m_supplierState == SS_Stopped)
            {
                if (m_supplierEpoch == m_writerEpoch)
                {
                    /* the supplier hasn't started yet so return the
                       handler to the user and queue up the data */
                    unit->SetStatusCode(RChannelItem_Data);

                    RChannelFifoUnit* blockedUnit =
                        DuplicateUnit(unit, itemArray);
                    m_blockedList.
                        InsertAsTail(m_blockedList.CastIn(blockedUnit));
                }
                else
                {
                    /* the supplier has already shut down so just
                       return this immediately */
                    LogAssert(m_supplierEpoch == m_writerEpoch+1);
                    LogAssert(m_readerTerminationItem != NULL);
                    RChannelItemType drainType =
                        m_readerTerminationItem->GetType();
                    LogAssert(drainType != RChannelItem_MarshalError);
                    LogAssert(m_blockedList.IsEmpty());
                    unit->DiscardItems();
                    unit->SetStatusCode(drainType);
                }
            }
            else if (m_supplierState == SS_Draining)
            {
                /* the supplier has sent a drain item, so return the
                   handler to the user with the appropriate code and
                   throw the data away */
                LogAssert(m_supplierEpoch == m_writerEpoch);
                LogAssert(m_readerTerminationItem != NULL);
                RChannelItemType drainType =
                    m_readerTerminationItem->GetType();
                LogAssert(drainType != RChannelItem_MarshalError);
                LogAssert(m_blockedList.IsEmpty());
                unit->DiscardItems();
                unit->SetStatusCode(drainType);
            }
            else if (m_supplierState == SS_Interrupted)
            {
                /* the supplier wants to drain but hasn't yet, so just
                   return the handler to the user and throw the data
                   away */
                LogAssert(m_supplierEpoch == m_writerEpoch);
                LogAssert(m_readerTerminationItem == NULL);
                LogAssert(m_blockedList.IsEmpty());
                unit->DiscardItems();
                unit->SetStatusCode(RChannelItem_Data);
            }
            else
            {
                LogAssert(m_supplierState == SS_Running);

                unit->SetStatusCode(RChannelItem_Data);

                LogAssert(m_blockedList.IsEmpty());

                /* return the handler to the user immediately
                   while also forwarding the unit to the
                   reader */
                RChannelFifoUnit* sendUnit =
                    DuplicateUnit(unit, itemArray);
                sendList.InsertAsTail(sendList.CastIn(sendUnit));
                m_sendLatch.AcceptList(&sendList);

                ++m_outstandingUnits;
            }
        }

        LogAssert(unit->GetStatusCode() != RChannelItem_MarshalError);
        returnList.InsertAsTail(returnList.CastIn(unit));
        m_returnLatch.AcceptList(&returnList);
        if (returnList.IsEmpty() && syncHandler != NULL)
        {
            /* another thread is returning the handler for us, so
               we need to wait for its event */
            syncHandler->UseEvent(m_eventCache.GetEvent(true));
        }
    }

    SendUnits(&sendList);

    ReturnHandlers(&returnList);
}

void RChannelFifoNBWriter::AcceptReturningUnit(RChannelFifoUnit* unit)
{
    bool wakeUpSupplierDrain = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_outstandingUnits > 0);
        --m_outstandingUnits;

        if (m_supplierState == SS_Draining)
        {
            /* the supplier has requested a drain */
            if (m_outstandingUnits == 0)
            {
                m_supplierState = SS_Stopped;
                ++m_supplierEpoch;
                wakeUpSupplierDrain = true;
            }
        }
    }

    if (wakeUpSupplierDrain)
    {
        BOOL bRet = ::SetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);
    }

    delete unit;
}

void RChannelFifoNBWriter::InterruptSupplier()
{
    bool waitForLatch;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Running);

        m_supplierState = SS_Interrupted;

        waitForLatch = m_sendLatch.Interrupt();
    }

    if (waitForLatch)
    {
        /* make sure all sends which were proceeding outside a lock
           have completed */
        m_sendLatch.Wait();
    }
}

void RChannelFifoNBWriter::DrainSupplier(RChannelItem* drainItem)
{
    ChannelFifoUnitList returnList;

    bool wakeUpSupplierDrain = false;
    bool waitForSupplierDrain = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplierState == SS_Interrupted);

        LogAssert(drainItem != NULL);

        m_readerTerminationItem = drainItem;

        if (m_outstandingUnits > 0)
        {
            m_supplierState = SS_Draining;
            waitForSupplierDrain = true;
        }
        else
        {
            m_supplierState = SS_Stopped;
            ++m_supplierEpoch;
            wakeUpSupplierDrain = true;
        }
    }

    if (wakeUpSupplierDrain)
    {
        LogAssert(waitForSupplierDrain == false);
        BOOL bRet = ::SetEvent(m_supplierDrainEvent);
        LogAssert(bRet != 0);
    }
    else if (waitForSupplierDrain)
    {
        DWORD dRet = ::WaitForSingleObject(m_supplierDrainEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    ReturnHandlers(&returnList);
}

void RChannelFifoNBWriter::Drain(DrTimeInterval csTimeOut,
                                 RChannelItemRef* pReturnItem)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_writerState == WS_Running);
        LogAssert(m_writerTerminationItem != NULL);

        m_writerState = WS_Stopped;
        ++m_writerEpoch;
        m_nextDeliverySequenceNumber = 0;
        m_nextDataSequenceNumber = 0;

        if (pReturnItem != NULL)
        {
            *pReturnItem = m_readerTerminationItem;
        }
    }
}

void RChannelFifoNBWriter::
    GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                        RChannelItemRef* pReaderDrainItem)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        *pWriterDrainItem = m_writerTerminationItem;
        *pReaderDrainItem = m_readerTerminationItem;
    }
}


RChannelFifoReader::RChannelFifoReader(RChannelFifo* parent,
                                       WorkQueue* workQueue)
{
    m_parent = parent;
    Initialize(m_parent->GetWriter(), workQueue, false);
}

RChannelFifoReader::~RChannelFifoReader()
{
}

RChannelFifo* RChannelFifoReader::GetParent()
{
    return m_parent;
}

bool RChannelFifoReader::GetTotalLength(UInt64* pLen)
{
    *pLen = 0;
    return false;
}

bool RChannelFifoReader::GetExpectedLength(UInt64* pLen)
{
    *pLen = 0;
    return false;
}

void RChannelFifoReader::SetExpectedLength(UInt64 expectedLength)
{
}

RChannelFifo::RChannelFifo(const char* name,
                           UInt32 fifoLength, WorkQueue* workQueue)
{
    m_name = name;
    if (fifoLength == s_infiniteBuffer)
    {
        m_writer = new RChannelFifoNBWriter(this);
    }
    else
    {
        m_writer = new RChannelFifoWriter(this, fifoLength);
    }
    m_writer->SetURI(name);

    m_reader = new RChannelFifoReader(this, workQueue);
    m_reader->SetURI(name);

    m_writer->SetReader(m_reader);
}

RChannelFifo::~RChannelFifo()
{
    DrLogD( "Deleting fifo. Name %s", m_name.GetString());
    delete m_reader;
    delete m_writer;
}

const char* RChannelFifo::GetName()
{
    return m_name;
}

RChannelFifoReader* RChannelFifo::GetReader()
{
    return m_reader;
}

RChannelFifoWriterBase* RChannelFifo::GetWriter()
{
    return m_writer;
}
