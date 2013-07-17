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
#include "channelreader.h"
#include "channelparser.h"
#include "channelbufferqueue.h"
#include "channelhelpers.h"
#include "workqueue.h"
#include "dryaderrordef.h"

#pragma unmanaged


/* put this here for want of a better source file */
RChannelContext::~RChannelContext()
{
}


RChannelReader::~RChannelReader()
{
}

bool RChannelReader::FetchNextItem(RChannelItemRef* pOutItem,
                                   DrTimeInterval timeOut)
{
    RChannelItemArrayRef itemArray;
    bool delivered = FetchNextItemArray(1, &itemArray, timeOut);
    LogAssert(itemArray != NULL);
    if (delivered)
    {
        if (itemArray->GetNumberOfItems() == 1)
        {
            RChannelItemRef* a = itemArray->GetItemArray();
            pOutItem->TransferFrom(a[0]);
        }
        else
        {
            LogAssert(itemArray->GetNumberOfItems() == 0);
            *pOutItem = NULL;
        }
    }
    else
    {
        LogAssert(itemArray->GetNumberOfItems() == 0);
        *pOutItem = NULL;
    }

    return delivered;
}

DrError RChannelReader::ReadItemSync(RChannelItemRef* pOutItem)
{
    FetchNextItem(pOutItem, DrTimeInterval_Infinite);

    if ((*pOutItem) == NULL)
    {
        RChannelItemRef writerTermination;
        RChannelItemRef readerTermination;
        GetTerminationItems(&writerTermination,
                            &readerTermination);

        if (writerTermination == NULL)
        {
            LogAssert(readerTermination != NULL);
            *pOutItem = readerTermination;
        }
        else
        {
            *pOutItem = writerTermination;
        }
    }

    return DrError_OK;
}

DrError RChannelReader::GetTerminationStatus(DryadMetaDataRef* pErrorData)
{
    DrError status;

    RChannelItemRef writerTermination;
    RChannelItemRef readerTermination;
    GetTerminationItems(&writerTermination,
                        &readerTermination);

    LogAssert(readerTermination.Ptr() != NULL);
    if (writerTermination.Ptr() != NULL &&
        writerTermination->GetType() != RChannelItem_EndOfStream)
    {
        status = writerTermination->GetErrorFromItem();
        *pErrorData = writerTermination->GetMetaData();
    }
    else if (readerTermination->GetType() == RChannelItem_EndOfStream)
    {
        status = DrError_EndOfStream;
        *pErrorData = readerTermination->GetMetaData();
    }
    else
    {
        status = DryadError_ProcessingInterrupted;
        *pErrorData = readerTermination->GetMetaData();
    }

    return status;
}

RChannelReaderSupplier::~RChannelReaderSupplier()
{
}

RChannelItemArrayReaderHandler::RChannelItemArrayReaderHandler()
{
    m_maximumArraySize = 1;
}

RChannelItemArrayReaderHandler::~RChannelItemArrayReaderHandler()
{
}

void RChannelItemArrayReaderHandler::SetMaximumArraySize(UInt32 maximumArraySize)
{
    m_maximumArraySize = maximumArraySize;
}

UInt32 RChannelItemArrayReaderHandler::GetMaximumArraySize()
{
    return m_maximumArraySize;
}

bool RChannelItemArrayReaderHandlerQueued::ImmediateDispatch()
{
    return false;
}

bool RChannelItemArrayReaderHandlerImmediate::ImmediateDispatch()
{
    return true;
}

RChannelItemReaderHandler::~RChannelItemReaderHandler()
{
}

void RChannelItemReaderHandler::
    ProcessItemArray(RChannelItemArray* deliveredArray)
{
    if (deliveredArray->GetNumberOfItems() == 1)
    {
        RChannelItemRef* a = deliveredArray->GetItemArray();
        ProcessItem(a[0]);
    }
    else
    {
        LogAssert(deliveredArray->GetNumberOfItems() == 0);
        ProcessItem(NULL);
    }
}

bool RChannelItemReaderHandlerQueued::ImmediateDispatch()
{
    return false;
}

bool RChannelItemReaderHandlerImmediate::ImmediateDispatch()
{
    return true;
}


RChannelReaderImpl::RChannelReaderImpl()
{
    m_workQueue = NULL;
    m_interruptHandler = NULL;
    m_supplier = NULL;
    m_lazyStart = false;

    m_state = RS_Stopped;
    m_drainEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_drainEvent != NULL);
    m_interruptEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_interruptEvent != NULL);
    m_startedSupplierEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_startedSupplierEvent != NULL);
    m_startedSupplier = false;
    m_numberOfSubItemsRead = 0;
    m_dataSizeRead = 0;
}

RChannelReaderImpl::~RChannelReaderImpl()
{
    LogAssert(m_state == RS_Closed);
    LogAssert(m_cookieMap.empty());
    LogAssert(m_eventMap.empty());
    LogAssert(m_unitList.IsEmpty());
    LogAssert(m_handlerList.IsEmpty());
    LogAssert(m_interruptHandler == NULL);
    LogAssert(m_readerTerminationItem == NULL);
    LogAssert(m_writerTerminationItem == NULL);

    BOOL bRet = ::CloseHandle(m_drainEvent);
    LogAssert(bRet != 0);
    bRet = ::CloseHandle(m_interruptEvent);
    LogAssert(bRet != 0);
    bRet = ::CloseHandle(m_startedSupplierEvent);
    LogAssert(bRet != 0);
}

void RChannelReaderImpl::Initialize(RChannelReaderSupplier* supplier,
                                    WorkQueue* workQueue, bool lazyStart)
{
    m_supplier = supplier;
    m_workQueue = workQueue;
    m_lazyStart = lazyStart;
}

void RChannelReaderImpl::SetURI(const char* uri)
{
    m_uri = uri;
}

const char* RChannelReaderImpl::GetURI()
{
    return m_uri;
}

void RChannelReaderImpl::Start(RChannelBufferPrefetchInfo* prefetchCookie)
{
    bool startSupplier = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supplier != NULL);
        LogAssert(m_state == RS_Stopped);
        LogAssert(m_cookieMap.empty());
        LogAssert(m_eventMap.empty());
        LogAssert(m_unitList.IsEmpty());
        LogAssert(m_handlerList.IsEmpty());
        LogAssert(m_interruptHandler == NULL);
        LogAssert(m_startedSupplier == false);
        m_sendLatch.Start();
        m_unitLatch.Start();

        m_state = RS_Running;
        m_readerTerminationItem = NULL;
        m_writerTerminationItem = NULL;
        m_numberOfSubItemsRead = 0;
        m_dataSizeRead = 0;
        m_prefetchCookie = prefetchCookie;

        if (m_lazyStart == false)
        {
            /* when we exit the lock, we will start the
               supplier. After the supplier has been started we'll set
               m_startedSupplierEvent to prevent a race condition in
               Interrupt */
            startSupplier = true;
            BOOL bRet = ::ResetEvent(m_startedSupplierEvent);
            LogAssert(bRet != 0);
            m_startedSupplier = true;
        }
    }

    if (startSupplier)
    {
        //todo: remove comments if not logging
//         DrLogI( "Starting Supplier");
        m_supplier->StartSupplier(m_prefetchCookie);
//         DrLogI( "Started Supplier");
        BOOL bRet = ::SetEvent(m_startedSupplierEvent);
        LogAssert(bRet != 0);
    }
}    

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::
    FillEmptyHandlers(ChannelProcessRequestList* handlerDispatch)
{
    while (m_handlerList.IsEmpty() == false)
    {
        RChannelProcessRequest* request =
            m_handlerList.CastOut(m_handlerList.GetHead());

        LogAssert(request->GetItemArray() == NULL);
        RChannelItemArrayRef emptyArray;
        emptyArray.Attach(new RChannelItemArray());
        request->SetItemArray(emptyArray);

        handlerDispatch->TransitionToTail(handlerDispatch->
                                          CastIn(request));
    }
}

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::
    TransferWaitingItems(const char* caller,
                         ChannelProcessRequestList* requestList,
                         ChannelUnitList* returnUnitList)
{
    LogAssert(m_handlerList.IsEmpty() == false);
    LogAssert(m_unitList.IsEmpty() == false);

    do
    {
        RChannelProcessRequest* request =
            m_handlerList.CastOut(m_handlerList.GetHead());

        LogAssert(m_writerTerminationItem == NULL);

        /* the request needs to be taken off the handler list now,
           since if we hit a termination item below, the remains of
           the handler list will be dumped on to the end of the
           request list */
        requestList->TransitionToTail(requestList->CastIn(request));

        UInt32 maxArraySize = request->GetHandler()->GetMaximumArraySize();
        LogAssert(request->GetItemArray() == NULL);

        RChannelUnit* unit = m_unitList.CastOut(m_unitList.GetHead());
        LogAssert(unit->GetType() == RChannelUnit_Item);
        RChannelItemUnit* itemUnit = (RChannelItemUnit *) unit;

        RChannelItemArray* srcArray = itemUnit->GetItemArray();
        UInt32 nItems = srcArray->GetNumberOfItems();

        if (nItems <= maxArraySize)
        {
            //todo: remove comments if not logging
//             DrLogD( "Transferring bulk item array",
//                 "Size %u max acceptable %u", nItems, maxArraySize);

            m_numberOfSubItemsRead += itemUnit->GetNumberOfSubItems();
            m_dataSizeRead += itemUnit->GetDataSize();

            request->SetItemArray(srcArray);

            m_writerTerminationItem = itemUnit->GetTerminationItem();
            if (m_writerTerminationItem != NULL)
            {
                FillEmptyHandlers(requestList);
            }

            itemUnit->DiscardItems();

            returnUnitList->TransitionToTail(returnUnitList->CastIn(unit));

            /* send off any blocked buffer boundary units */
            while (!m_unitList.IsEmpty() &&
                   (unit = m_unitList.CastOut(m_unitList.GetHead()))->
                   GetType() != RChannelUnit_Item)
            {
                LogAssert(unit->GetType() == RChannelUnit_BufferBoundary);
                returnUnitList->TransitionToTail(returnUnitList->
                                                 CastIn(unit));
                // todo: remove comments if not logging
//                 DrLogD(
//                     "adding buffer boundary dispatch request",
//                     "caller: %s", caller);
            }
        }
        else
        {
//             DrLogD( "Transferring partial item array",
//                 "Size %u max acceptable %u", nItems, maxArraySize);

            RChannelItemArrayRef dstArray;
            dstArray.Attach(new RChannelItemArray());
            dstArray->SetNumberOfItems(maxArraySize);
            RChannelItemRef* dstItems = dstArray->GetItemArray();
            RChannelItemRef* srcItems = srcArray->GetItemArray();

            UInt64 subItemsRead = 0;
            UInt64 dataSizeRead = 0;
            UInt32 i;
            for (i=0; i<maxArraySize; ++i)
            {
                dstItems[i].TransferFrom(srcItems[i]);
                subItemsRead += dstItems[i]->GetNumberOfSubItems();
                dataSizeRead += dstItems[i]->GetItemSize();
            }

            LogAssert(subItemsRead <= itemUnit->GetNumberOfSubItems());
            LogAssert(dataSizeRead <= itemUnit->GetDataSize());

            m_numberOfSubItemsRead += subItemsRead;
            m_dataSizeRead += dataSizeRead;
            itemUnit->SetSizes(itemUnit->GetNumberOfSubItems() - subItemsRead,
                               itemUnit->GetDataSize() - dataSizeRead);

            request->SetItemArray(dstArray);
            srcArray->DiscardPrefix(maxArraySize);
        }
    } while (m_handlerList.IsEmpty() == false &&
             m_unitList.IsEmpty() == false);
}

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::AddUnitToQueue(const char* caller,
                                        RChannelUnit* unit,
                                        ChannelProcessRequestList* requestList,
                                        ChannelUnitList* returnUnitList)
{
    LogAssert(m_state == RS_Running || m_state == RS_InterruptingSupplier);

    if (unit->GetType() == RChannelUnit_Item)
    {
        /* make sure we haven't already sent out a termination item for
           processing */
        LogAssert(m_writerTerminationItem == NULL &&
                  m_readerTerminationItem == NULL);

        if (m_handlerList.IsEmpty())
        {
//             DrLogD(
//                 "queueing item list",
//                 "caller: %s", caller);

            m_unitList.InsertAsTail(m_unitList.CastIn(unit));
            unit = NULL;
        }
        else
        {
//             DrLogD(
//                 "adding process work request",
//                 "caller: %s", caller);

            LogAssert(m_unitList.IsEmpty());

            /* put this in the unit list since that's where
               TransferWaitingItems expects to find it */
            m_unitList.InsertAsTail(m_unitList.CastIn(unit));

            TransferWaitingItems(caller,
                                 requestList,
                                 returnUnitList);
        }
    }
    else
    {
        LogAssert(unit->GetType() == RChannelUnit_BufferBoundary);
        if (m_unitList.IsEmpty())
        {
            returnUnitList->InsertAsTail(returnUnitList->CastIn(unit));
//             DrLogD(
//                 "adding buffer boundary dispatch request",
//                 "caller: %s", caller);
        }
        else
        {
            m_unitList.InsertAsTail(m_unitList.CastIn(unit));
//             DrLogD(
//                 "queueing buffer boundary dispatch request",
//                 "caller: %s", caller);
        }
    }
}

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::
    AddHandlerToQueue(const char* caller,
                      RChannelProcessRequest* request,
                      ChannelProcessRequestList* requestList,
                      ChannelUnitList* returnUnitList)
{
    LogAssert(m_state == RS_Running);

    if (m_unitList.IsEmpty())
    {
//         DrLogD(
//             "queueing handler",
//             "caller: %s", caller);
        m_handlerList.InsertAsTail(m_handlerList.CastIn(request));
    }
    else
    {
//         DrLogD(
//             "adding process work request",
//             "caller: %s", caller);

        LogAssert(m_handlerList.IsEmpty());

        /* put this in the handler list since that's where
           TransferWaitingItems expects to find it */
        m_handlerList.InsertAsTail(m_handlerList.CastIn(request));

        TransferWaitingItems(caller,
                             requestList,
                             returnUnitList);
    }
}

void RChannelReaderImpl::ReturnUnits(ChannelUnitList* unitList)
{
    while (unitList->IsEmpty() == false)
    {
        DrBListEntry* listEntry = unitList->RemoveHead();
        while (listEntry != NULL)
        {
            RChannelUnit* unit = unitList->CastOut(listEntry);
//             DrLogD( "returning unit");
            unit->ReturnToSupplier();
            listEntry = unitList->RemoveHead();
        }

        {
            AutoCriticalSection acs(&m_baseDR);

            m_unitLatch.TransferList(unitList);
        }
    }
}

void RChannelReaderImpl::
    DispatchRequests(const char* caller,
                     ChannelProcessRequestList* requestList,
                     ChannelUnitList* unitList)
{
    ReturnUnits(unitList);

    while (requestList->IsEmpty() == false)
    {
        DrBListEntry* listEntry = requestList->RemoveHead();
        while (listEntry != NULL)
        {
            RChannelProcessRequest* request = requestList->CastOut(listEntry);

            if (request->GetHandler()->ImmediateDispatch())
            {
                request->Process();
                delete request;
            }
            else
            {
//                 DrLogD( caller,
//                     "adding work request");
                bool bRet = m_workQueue->EnQueue(request);
                LogAssert(bRet == true);
            }

            listEntry = requestList->RemoveHead();
        }

        {
            AutoCriticalSection acs(&m_baseDR);

            m_sendLatch.TransferList(requestList);
        }
    }
}

static void FOO(ChannelProcessRequestList* requestList)
{
    RChannelProcessRequest* request =
        requestList->CastOut(requestList->GetHead());
    while (request != NULL)
    {
        LogAssert(request->GetItemArray() != NULL);
        request = requestList->GetNextTyped(request);
    }
}

void RChannelReaderImpl::AddUnitList(ChannelUnitList* unitList)
{
    ChannelProcessRequestList requestList;
    ChannelUnitList returnUnitList;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == RS_Running ||
                  m_state == RS_InterruptingSupplier);

        DrBListEntry* listEntry = unitList->RemoveHead();
        while (listEntry != NULL)
        {
            RChannelUnit* unit = unitList->CastOut(listEntry);
            AddUnitToQueue("RChannelReaderImpl::AddUnitList",
                           unit, &requestList, &returnUnitList);
            listEntry = unitList->RemoveHead();
        }

        FOO(&requestList);
        m_sendLatch.AcceptList(&requestList);
        m_unitLatch.AcceptList(&returnUnitList);
    }

    DispatchRequests("RChannelReaderImpl::AddUnitList",
                     &requestList, &returnUnitList);
}

void RChannelReaderImpl::SupplyHandler(RChannelItemArrayReaderHandler* handler,
                                       void* cancelCookie)
{
    ChannelProcessRequestList requestList;
    ChannelUnitList returnUnitList;
    bool queuedHandler = false;
    bool startSupplier = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_state == RS_Running)
        {
            if (m_writerTerminationItem == NULL &&
                m_readerTerminationItem == NULL)
            {
                if (m_startedSupplier == false)
                {
                    /* when we exit the lock, we will start the
                       supplier since this is the first read we have
                       received. After the supplier has been started
                       we'll set m_startedSupplierEvent to prevent a
                       race condition in Interrupt */
                    startSupplier = true;
                    BOOL bRet = ::ResetEvent(m_startedSupplierEvent);
                    LogAssert(bRet != 0);
                    m_startedSupplier = true;
                }

                /* we haven't yet sent a termination item for
                   processing, so there's going to be at least one
                   more item coming from the parser */
                RChannelProcessRequest* request =
                    new RChannelProcessRequest(this, handler, cancelCookie);
                m_cookieMap.insert(std::make_pair(cancelCookie, request));
                AddHandlerToQueue("RChannelReaderImpl::SupplyHandler",
                                  request, &requestList, &returnUnitList);
                queuedHandler = true;

                FOO(&requestList);
                m_sendLatch.AcceptList(&requestList);
                m_unitLatch.AcceptList(&returnUnitList);
            }
        }
        else
        {
            LogAssert(m_state != RS_Closed);
        }
    }

    if (startSupplier)
    {
//         DrLogI( "Starting Supplier");
        m_supplier->StartSupplier(m_prefetchCookie);
//         DrLogI( "Started Supplier");
        BOOL bRet = ::SetEvent(m_startedSupplierEvent);
        LogAssert(bRet != 0);
    }

    if (queuedHandler)
    {
        DispatchRequests("RChannelReaderImpl::SupplyHandler",
                         &requestList, &returnUnitList);
    }
    else
    {
        LogAssert(requestList.IsEmpty());
        LogAssert(returnUnitList.IsEmpty());
        /* we've already sent out a termination item or started to
           drain so just return the handler immediately as there will
           never be any more items to send */
        RChannelItemArrayRef emptyArray;
        emptyArray.Attach(new RChannelItemArray());
        handler->ProcessItemArray(emptyArray);
    }
}

void RChannelReaderImpl::
    ThreadSafeSetItemArray(RChannelItemArrayRef* dstItemArray,
                           RChannelItemArray* srcItemArray)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        dstItemArray->Set(srcItemArray);
    }
}

bool RChannelReaderImpl::FetchNextItemArray(UInt32 maxArraySize,
                                            RChannelItemArrayRef* pItemArray,
                                            DrTimeInterval csTimeOut)
{
    ChannelProcessRequestList requestList;
    ChannelUnitList returnUnitList;
    DryadHandleListEntry* event = NULL;
    bool timedOut = false;
    bool mustBlock = false;
    bool startSupplier = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_state != RS_Running)
        {
            LogAssert(m_state != RS_Closed);
            /* return an empty array */
            pItemArray->Attach(new RChannelItemArray());
            return true;
        }

        if (!m_unitList.IsEmpty())
        {
            LogAssert(m_startedSupplier == true);
            LogAssert(m_handlerList.IsEmpty());

            RChannelReaderSyncWaiter dummyHandler(NULL,
                                                  INVALID_HANDLE_VALUE,
                                                  NULL);
            dummyHandler.SetMaximumArraySize(maxArraySize);
            RChannelProcessRequest dummyRequest(NULL, &dummyHandler, NULL);
            m_handlerList.InsertAsTail(m_handlerList.CastIn(&dummyRequest));

            TransferWaitingItems("FetchNextItemArray",
                                 &requestList, &returnUnitList);

            RChannelProcessRequest* dummyReturn =
                requestList.CastOut(requestList.RemoveHead());
            LogAssert(dummyReturn == &dummyRequest);

            pItemArray->Set(dummyRequest.GetItemArray());
            LogAssert((*pItemArray)->GetNumberOfItems() > 0);
        }
        else if (m_writerTerminationItem != NULL ||
                 m_readerTerminationItem != NULL)
        {
            /* we have already sent a termination item for processing
               so there aren't going to be any more arriving on the
               queue and we can return an empty list immediately */
            pItemArray->Attach(new RChannelItemArray());
        }
        else if (csTimeOut > DrTimeInterval_Zero)
        {
            if (m_startedSupplier == false)
            {
                /* when we exit the lock, we will start the supplier
                   since this is the first read we have
                   received. After the supplier has been started we'll
                   set m_startedSupplierEvent to prevent a race
                   condition in Interrupt */
                startSupplier = true;
                BOOL bRet = ::ResetEvent(m_startedSupplierEvent);
                LogAssert(bRet != 0);
                m_startedSupplier = true;
            }

            /* we are going to block */
            mustBlock = true;
            event = m_eventCache.GetEvent(true);
        }
        else
        {
            /* there's no item available and a zero timeout --- return
               an empty list immediately */
            pItemArray->Attach(new RChannelItemArray());
            LogAssert(csTimeOut == DrTimeInterval_Zero);
            timedOut = true;
        }

        m_unitLatch.AcceptList(&returnUnitList);
        FOO(&requestList);
        m_sendLatch.AcceptList(&requestList);
    }

    if (startSupplier)
    {
//         DrLogI( "Starting Supplier");
        m_supplier->StartSupplier(m_prefetchCookie);
//         DrLogI( "Started Supplier");
        BOOL bRet = ::SetEvent(m_startedSupplierEvent);
        LogAssert(bRet != 0);
    }

    DispatchRequests("FetchNextItemArray", &requestList, &returnUnitList);

    if (mustBlock == false)
    {
        LogAssert(event == NULL);
    }
    else
    {
        LogAssert(event != NULL);

        *pItemArray = NULL;

        RChannelReaderSyncWaiter* waiter =
            new RChannelReaderSyncWaiter(this, event->GetHandle(), pItemArray);
        this->SupplyHandler(waiter, waiter);

        DWORD timeOut = DrGetTimerMsFromInterval(csTimeOut);
        DWORD dRet = ::WaitForSingleObject(event->GetHandle(), timeOut);

        if (dRet == WAIT_TIMEOUT)
        {
            /* when cancel returns it's guaranteed the handler has
               been called */
            this->Cancel(waiter);
        }
        else
        {
            LogAssert(dRet == WAIT_OBJECT_0);
        }

        LogAssert(*pItemArray != NULL);

        delete waiter;

        {
            AutoCriticalSection acs(&m_baseDR);

            /* save this event in case we need one again in the
               future */
            m_eventCache.ReturnEvent(event);

            /* even if we actually timed out the wait, the handler may
               have supplied an item immediately afterwards or during
               the cancel. We only return a timed out value if there's
               no item ready but we haven't processed a termination
               item, so it's worth waiting for another one. */
            timedOut = ((*pItemArray)->GetNumberOfItems() == 0 &&
                        m_writerTerminationItem == NULL &&
                        m_readerTerminationItem == NULL);
        }
    }

    return (!timedOut);
}

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::RemoveFromCancelMap(RChannelProcessRequest* request,
                                             void* cancelCookie)
{
    /* get the first occurrence of this cookie in the multimap */
    CookieHandlerMap::iterator hIter = m_cookieMap.find(cancelCookie);
    /* then look for the actual matching request */
    while (hIter != m_cookieMap.end() &&
           hIter->first == cancelCookie &&
           hIter->second != request)
    {
        ++hIter;
    }
    LogAssert(hIter != m_cookieMap.end() &&
              hIter->first == cancelCookie);
    m_cookieMap.erase(hIter);
}

/* called with RChannelReaderImpl::m_baseDR held */
void RChannelReaderImpl::MaybeTriggerCancelEvent(void* cancelCookie)
{
    /* there is an event in the event map if somebody is blocked on
       cancelling this cookie */
    CookieEventMap::iterator eIter = m_eventMap.find(cancelCookie);
    if (eIter != m_eventMap.end())
    {
        /* check to see if there are any remaining requests with this
           cookie which haven't been processed yet */
        CookieHandlerMap::iterator hIter = m_cookieMap.find(cancelCookie);
        if (hIter == m_cookieMap.end())
        {
            /* there's nothing left so signal the event and remove the
               cookie from the map */
            BOOL bRet = ::SetEvent(eIter->second);
            LogAssert(bRet != 0);
            m_eventMap.erase(eIter);
        }
    }
}

void RChannelReaderImpl::
    ProcessItemArrayRequest(RChannelProcessRequest* request)
{
    RChannelItemArray* itemArray = request->GetItemArray();
    RChannelItemArrayReaderHandler* handler = request->GetHandler();
    LogAssert(handler != NULL);
    LogAssert(itemArray != NULL);

    void* cancelCookie = request->GetCookie();

    handler->ProcessItemArray(itemArray);

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state != RS_Stopped);

        /* now that the request has been processed remove it from our
           accounting */
        RemoveFromCancelMap(request, cancelCookie);
        /* if it was the last request with this cookie and somebody
           was blocked cancelling the cookie, wake them up */
        MaybeTriggerCancelEvent(cancelCookie);

        /* if it was the last request in the system and we are
           draining, wake up the drain thread */
        if (m_state == RS_Stopping && m_cookieMap.empty())
        {
            LogAssert(m_eventMap.empty());
            BOOL bRet = ::SetEvent(m_drainEvent);
            LogAssert(bRet != 0);
        }
    }
}

void RChannelReaderImpl::AlertApplication(RChannelItem* item)
{
    RChannelInterruptHandler* interruptHandler = NULL;

    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_interruptHandler != NULL)
        {
            interruptHandler = m_interruptHandler;
            m_interruptHandler = NULL;
        }
    }

    if (interruptHandler != NULL)
    {
        interruptHandler->ProcessInterrupt(item);
    }
}

bool RChannelReaderImpl::IsRunning()
{
    bool retval;

    {
        AutoCriticalSection acs(&m_baseDR);

        retval = (m_state == RS_Running);
    }

    return retval;
}

void RChannelReaderImpl::Cancel(void* cancelCookie)
{
    BOOL bRet;
    bool mustClean = false;
    ChannelProcessRequestList handlerDispatch;
    DrBListEntry* listEntry;
    CookieHandlerMap::iterator cIter;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == RS_Running || m_state == RS_InterruptingSupplier);

        /* first find any handlers with this cookie which haven't yet
           been dispatched with an item, and put them on the
           handlerDispatch list */
        listEntry = m_handlerList.GetHead();
        while (listEntry != NULL)
        {
            RChannelProcessRequest* request = m_handlerList.CastOut(listEntry);
            listEntry = m_handlerList.GetNext(listEntry);

            if (request->GetCookie() == cancelCookie)
            {
                LogAssert(request->GetItemArray() == NULL);
                handlerDispatch.TransitionToTail(handlerDispatch.
                                                 CastIn(request));
                RemoveFromCancelMap(request, cancelCookie);
            }
        }

        /* now find any handlers with this cookie which are still
           around (i.e. have already been put on the work queue but
           not yet processed) and try to cancel them, which will
           spring them from the work queue early when we clean it
           below */
        cIter = m_cookieMap.find(cancelCookie);
        if (cIter != m_cookieMap.end())
        {
            while (cIter != m_cookieMap.end() &&
                   cIter->first == cancelCookie)
            {
                (cIter->second)->Cancel();
                ++cIter;
            }
            mustClean = true;
        }
    }

    /* these handlers had been submitted but not assigned an item, so
       we can send them back immediately */
    listEntry = handlerDispatch.GetHead();
    while (listEntry != NULL)
    {
        RChannelProcessRequest* request = handlerDispatch.CastOut(listEntry);
        listEntry = handlerDispatch.GetNext(listEntry);
        handlerDispatch.Remove(handlerDispatch.CastIn(request));
        RChannelItemArrayRef emptyArray;
        emptyArray.Attach(new RChannelItemArray());
        request->GetHandler()->ProcessItemArray(emptyArray);
        delete request;
    }

    if (mustClean)
    {
        /* at least one handler had already been sent for processing:
           get the queue to trigger it if it's still hanging around
           there */
        m_workQueue->Clean();
    }

    DryadHandleListEntry* event = NULL;

    {
        AutoCriticalSection acs(&m_baseDR);

        /* see if there are still any handlers around after the work
           queue cleaning which haven't triggered yet. If so, we'll
           have to add an event to the event map and wait for it */
        cIter = m_cookieMap.find(cancelCookie);
        if (cIter != m_cookieMap.end())
        {
            event = m_eventCache.GetEvent(true);

            std::pair<CookieEventMap::iterator, bool> retval;
            retval = m_eventMap.insert(std::make_pair(cancelCookie,
                                                      event->GetHandle()));
            /* it's not legal to overlap two calls to cancel the same
               cookie */
            LogAssert(retval.second == true);
        }
    }

    if (event != NULL)
    {
        /* we decided we had to wait for an event */
        bRet = ::WaitForSingleObject(event->GetHandle(), INFINITE);
        LogAssert(bRet == WAIT_OBJECT_0);

        {
            AutoCriticalSection acs(&m_baseDR);

            /* sanity check that there really aren't any handlers with
               this cookie still hanging around */
            cIter = m_cookieMap.find(cancelCookie);
            LogAssert(cIter == m_cookieMap.end());

            /* sanity check that it got removed from the event map at
               the same time */
            CookieEventMap::iterator eIter = m_eventMap.find(cancelCookie);
            LogAssert(eIter == m_eventMap.end());

            /* save the event in case we want to use it again later */
            m_eventCache.ReturnEvent(event);
        }
    }
}

//
// Interrupt an input channel for provided reason
//
void RChannelReaderImpl::Interrupt(RChannelItem* interruptItemBase)
{
    RChannelItemRef interruptItem = interruptItemBase;
    bool doInterrupt;
    bool startedSupplier = false;
    BOOL bRet;

    //
    // Enter a critical section and update state from "Running" to "Interrupting"
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_state == RS_Running)
        {
            doInterrupt = true;
            bRet = ::ResetEvent(m_interruptEvent);
            LogAssert(bRet != 0);
            m_state = RS_InterruptingSupplier;
            startedSupplier = m_startedSupplier;
        }
        else
        {
            doInterrupt = false;
        }
    }

    //
    // If already interrupting, wait for interrupt event to be handled
    //
    if (doInterrupt == false)
    {
        bRet = ::WaitForSingleObject(m_interruptEvent, INFINITE);
        LogAssert(bRet == WAIT_OBJECT_0);
        return;
    }

    ChannelProcessRequestList handlerDispatch;
    ChannelUnitList unitDispatch;
    DrBListEntry* listEntry;
    RChannelInterruptHandler* interruptHandler = NULL;

    if (startedSupplier)
    {
        //
        // startedSupplier just means that we (potentially in another
        // thread) have called or are about to call StartSupplier,
        // outside the lock. We'll wait here until it really gets
        // called before calling InterruptSupplier 
        //
        bRet = ::WaitForSingleObject(m_startedSupplierEvent, INFINITE);
        LogAssert(bRet == WAIT_OBJECT_0);

        //
        // when this returns the buffer reader will not be generating
        // any new buffers and the parser will not be generating any
        // new items.  
        //
        m_supplier->InterruptSupplier();
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        if (interruptItem == NULL)
        {
            if (m_writerTerminationItem == NULL)
            {
                interruptItem.Attach(RChannelMarkerItem::
                                     Create(RChannelItem_Abort, false));
            }
            else
            {
                RChannelItemType interruptType =
                    m_writerTerminationItem->GetType();
                interruptItem.Attach(RChannelMarkerItem::
                                     Create(interruptType, false));
                interruptItem->ReplaceMetaData(m_writerTerminationItem->
                                               GetMetaData());
            }
        }
        else
        {
            RChannelItemType interruptType = interruptItem->GetType();
            LogAssert(RChannelItem::IsTerminationItem(interruptType));
        }

        LogAssert(m_state == RS_InterruptingSupplier);
        /* sanity check that nobody accidentally started the supplier
           while we were getting here */
        LogAssert(startedSupplier == m_startedSupplier);

        m_state = RS_Stopping;

        if (m_unitList.IsEmpty())
        {
            /* gather up any handlers which haven't been given an item
               yet */
            FillEmptyHandlers(&handlerDispatch);
        }
        else
        {
            LogAssert(m_handlerList.IsEmpty());
            /* gather up any units which have been put on the queue
               but don't have a handler waiting */
            bool gotTermination = (m_writerTerminationItem != NULL);
            while (m_unitList.IsEmpty() == false)
            {
                RChannelUnit* unit = m_unitList.CastOut(m_unitList.GetHead());
                if (unit->GetType() == RChannelUnit_Item)
                {
                    /* sanity check that the item sequence is
                       correct */
                    RChannelItemUnit* itemUnit = (RChannelItemUnit *) unit;
                    RChannelItemArray* itemArray = itemUnit->GetItemArray();

                    UInt32 nItems = itemArray->GetNumberOfItems();
                    LogAssert(nItems > 0);
                    RChannelItemRef* a = itemArray->GetItemArray();

                    UInt32 i;
                    for (i=0; i<nItems; ++i)
                    {
                        RChannelItem* item = a[i];
                        RChannelItemType itemType = item->GetType();
                        LogAssert(gotTermination == false);
                        if (RChannelItem::IsTerminationItem(itemType))
                        {
                            gotTermination = true;
                        }
                    }

                    itemUnit->DiscardItems();
                }
                unitDispatch.TransitionToTail(unitDispatch.CastIn(unit));
            }
        }

        /* prepare to trigger any interrupt handler the application
           sent in earlier */
        if (m_interruptHandler != NULL)
        {
            interruptHandler = m_interruptHandler;
            m_interruptHandler = NULL;
        }

        m_unitLatch.AcceptList(&unitDispatch);
    }

    /* these are handlers which had been submitted but not assigned an
       item */
    listEntry = handlerDispatch.GetHead();
    while (listEntry != NULL)
    {
        RChannelProcessRequest* request = handlerDispatch.CastOut(listEntry);
        listEntry = handlerDispatch.GetNext(listEntry);
        handlerDispatch.Remove(handlerDispatch.CastIn(request));
        /* call the process event (which calls back into
           RChannelReaderImpl::ProcessUnit) instead of just calling the
           handler in case there is a thread blocked on a cancellation
           which needs to be woken up. */
        request->Process();
        delete request;
    }

    ReturnUnits(&unitDispatch);

    if (startedSupplier)
    {
        /* assuming we ever started it, wait for the buffer reader to
           process all its returned buffers and tell us whether we can
           restart or not */
        m_supplier->DrainSupplier(interruptItem);
    }

    if (interruptHandler != NULL)
    {
        interruptHandler->ProcessInterrupt(interruptItem);
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        /* sanity check that nobody accidentally started the supplier
           while we were getting here */
        LogAssert(startedSupplier == m_startedSupplier);

        LogAssert(m_unitList.IsEmpty());
        LogAssert(m_handlerList.IsEmpty());
        LogAssert(m_interruptHandler == NULL);
        LogAssert(m_readerTerminationItem == NULL);
        m_readerTerminationItem = interruptItem;
        m_startedSupplier = false;
    }

    bRet = ::SetEvent(m_interruptEvent);
    LogAssert(bRet != 0);
}

void RChannelReaderImpl::Drain()
{
    bool waitForCookie = false;
    bool waitForLatch = false;
    BOOL bRet;

    Interrupt(NULL);

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == RS_Stopping);
        LogAssert(m_startedSupplier == false);
        LogAssert(m_unitList.IsEmpty());
        LogAssert(m_handlerList.IsEmpty());

        if (!m_cookieMap.empty())
        {
            /* there are handlers which have been submitted and sent
               for processing, but haven't triggered yet. Mark them
               for cancellation so they will get evicted from the work
               queue in the clean below. */
            CookieHandlerMap::iterator cookieIter;
            for (cookieIter = m_cookieMap.begin();
                 cookieIter != m_cookieMap.end();
                 ++cookieIter)
            {
                (cookieIter->second)->Cancel();
            }
            waitForCookie = true;
            bRet = ::ResetEvent(m_drainEvent);
            LogAssert(bRet != 0);
        }

        waitForLatch = m_sendLatch.Interrupt();
    }

    if (waitForLatch)
    {
        m_sendLatch.Wait();
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        waitForLatch = m_unitLatch.Interrupt();
    }

    if (waitForLatch)
    {
        m_unitLatch.Wait();
    }

    if (waitForCookie)
    {
        m_workQueue->Clean();

        /* wait until all handlers which had been sent to the queue
           have returned */
        bRet = ::WaitForSingleObject(m_drainEvent, INFINITE);
        LogAssert(bRet == WAIT_OBJECT_0);
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_cookieMap.empty());
        LogAssert(m_eventMap.empty());
        LogAssert(m_unitList.IsEmpty());
        LogAssert(m_handlerList.IsEmpty());
        LogAssert(m_interruptHandler == NULL);
        LogAssert(m_startedSupplier == false);
        m_sendLatch.Stop();
        m_unitLatch.Stop();

        m_state = RS_Stopped;
    }
}

void RChannelReaderImpl::
    GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                        RChannelItemRef* pReaderDrainItem)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        *pWriterDrainItem = m_writerTerminationItem;
        *pReaderDrainItem = m_readerTerminationItem;
    }
}

UInt64 RChannelReaderImpl::GetDataSizeRead()
{
    return m_dataSizeRead;
}

void RChannelReaderImpl::Close()
{
    m_supplier->CloseSupplier();

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_cookieMap.empty());
        LogAssert(m_eventMap.empty());
        LogAssert(m_unitList.IsEmpty());
        LogAssert(m_handlerList.IsEmpty());
        LogAssert(m_interruptHandler == NULL);
        LogAssert(m_state == RS_Stopped);
        m_state = RS_Closed;
        m_readerTerminationItem = NULL;
        m_writerTerminationItem = NULL;
    }
}

RChannelSerializedReader::
    RChannelSerializedReader(RChannelBufferReader* bufferReader,
                             RChannelItemParserBase* parser,
                             UInt32 maxParseBatchSize,
                             UInt32 maxOutstandingUnits,
                             bool lazyStart,
                             WorkQueue* workQueue)
{
    m_bufferQueue = new RChannelBufferQueue(this,
                                            bufferReader,
                                            parser,
                                            maxParseBatchSize,
                                            maxOutstandingUnits,
                                            workQueue);
    RChannelReaderImpl::Initialize(m_bufferQueue, workQueue, lazyStart);

    m_expectedLength = (UInt64) -1;
}

RChannelSerializedReader::~RChannelSerializedReader()
{
    delete m_bufferQueue;
}

bool RChannelSerializedReader::GetTotalLength(UInt64* pLen)
{
    return this->m_bufferQueue->GetTotalLength(pLen);
}

bool RChannelSerializedReader::GetExpectedLength(UInt64* pLen)
{
    if (m_expectedLength == (UInt64) -1)
    {
        *pLen = 0;
        return false;
    }
    else
    {
        *pLen = m_expectedLength;
        return true;
    }
}

void RChannelSerializedReader::SetExpectedLength(UInt64 expectedLength)
{
    m_expectedLength = expectedLength;
}

RChannelBufferReaderHandler::~RChannelBufferReaderHandler()
{
}

RChannelBufferReader::~RChannelBufferReader()
{
}

void RChannelBufferReader::FillInStatus(DryadChannelDescription* status)
{
}

void RChannelNullBufferReader::
    Start(RChannelBufferPrefetchInfo* prefetchCookie,
          RChannelBufferReaderHandler* handler)
{
    RChannelItem* item =
        RChannelMarkerItem::Create(RChannelItem_EndOfStream, false);

    RChannelBuffer* buffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_EndOfStream,
                                            item,
                                            this);

    handler->ProcessBuffer(buffer);
}

void RChannelNullBufferReader::Interrupt()
{
}

void RChannelNullBufferReader::Drain(RChannelItem* drainItem)
{
}

void RChannelNullBufferReader::Close()
{
}

void RChannelNullBufferReader::ReturnBuffer(RChannelBuffer* buffer)
{
    buffer->DecRef();
}

bool RChannelNullBufferReader::GetTotalLength(UInt64* pLen)
{
    *pLen = 0;
    return true;
}
