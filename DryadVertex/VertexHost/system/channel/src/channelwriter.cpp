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
#include <channelwriter.h>
#include <channelmarshaler.h>
#include <dryaderrordef.h>
#include <workqueue.h>
#include "channelhelpers.h"
#include "channelmemorybuffers.h"

#pragma unmanaged


void SyncItemWriterBase::
    WriteItemSyncConsumingFreeReference(RChannelItem* item)
{
    RChannelItemRef refItem;
    refItem.Attach(item);
    WriteItemSyncConsumingReference(refItem);
}

void SyncItemWriterBase::WriteItemSync(RChannelItem* item)
{
    RChannelItemRef refItem = item;
    WriteItemSyncConsumingReference(refItem);
}

RChannelWriter::~RChannelWriter()
{
}

void RChannelWriter::WriteItem(RChannelItem* item,
                               bool flushAfter,
                               RChannelItemArrayWriterHandler* handler)
{
    RChannelItemArrayRef singleArray;
    singleArray.Attach(new RChannelItemArray());
    singleArray->SetNumberOfItems(1);
    singleArray->GetItemArray()[0] = item;
    WriteItemArray(singleArray, flushAfter, handler);
}

RChannelItemType RChannelWriter::
    WriteItemSync(RChannelItem* item,
                  bool flush,
                  RChannelItemRef* pMarshalFailureItem)
{
    RChannelItemArrayRef singleArray;
    singleArray.Attach(new RChannelItemArray());
    singleArray->SetNumberOfItems(1);
    singleArray->GetItemArray()[0] = item;

    RChannelItemArrayRef failureArray;

    RChannelItemType retVal =
        WriteItemArraySync(singleArray, flush, &failureArray);

    if (pMarshalFailureItem != NULL)
    {
        if (failureArray == NULL)
        {
            *pMarshalFailureItem = NULL;
        }
        else
        {
            LogAssert(failureArray->GetNumberOfItems() == 1);
            LogAssert(failureArray->GetItemArray()[0] != NULL);
            *pMarshalFailureItem = failureArray->GetItemArray()[0];
        }
    }

    return retVal;
}

void RChannelWriter::WriteItemSyncConsumingReference(RChannelItemRef& item)
{
    WriteItemSync(item, false, NULL);
    item = NULL;
}

DrError RChannelWriter::GetTerminationStatus(DryadMetaDataRef* pErrorData)
{
    DrError status;

    RChannelItemRef writerTermination;
    RChannelItemRef readerTermination;
    GetTerminationItems(&writerTermination, &readerTermination);

    LogAssert(writerTermination != NULL);

    *pErrorData = writerTermination->GetMetaData();

    if (writerTermination->GetType() == RChannelItem_EndOfStream)
    {
        if (readerTermination != NULL &&
            readerTermination->GetType() != RChannelItem_EndOfStream)
        {
            status = readerTermination->GetErrorFromItem();
            *pErrorData = readerTermination->GetMetaData();
        }
        else
        {
            status = DrError_EndOfStream;
        }
    }
    else
    {
        status = writerTermination->GetErrorFromItem();
    }

    return status;
}

DrError RChannelWriter::GetWriterStatus()
{
    DrError status = DrError_OK;

    RChannelItemRef writerTermination;
    RChannelItemRef readerTermination;
    GetTerminationItems(&writerTermination, &readerTermination);

    if (readerTermination != NULL)
    {
        if (readerTermination->GetType() == RChannelItem_EndOfStream)
        {
            status = DrError_EndOfStream;
        }
        else
        {
            status = readerTermination->GetErrorFromItem();
        }
    }

    return status;
}

RChannelItemArrayWriterHandler::~RChannelItemArrayWriterHandler()
{
}

RChannelItemWriterHandler::~RChannelItemWriterHandler()
{
}

void RChannelItemWriterHandler::
    ProcessWriteArrayCompleted(RChannelItemType returnCode,
                               RChannelItemArray* failureArray)
{
    if (failureArray != NULL)
    {
        LogAssert(failureArray->GetNumberOfItems() == 1);
        ProcessWriteCompleted(returnCode, failureArray->GetItemArray()[0]);
    }
    else
    {
        ProcessWriteCompleted(returnCode, NULL);
    }
}

void RChannelSerializedWriter::DummyItemHandler::
    ProcessWriteArrayCompleted(RChannelItemType returnCode,
                               RChannelItemArray* failureArray)
{
    LogAssert(failureArray == NULL);
    delete this;
}

RChannelSerializedWriter::WriteRequest::
    WriteRequest(RChannelItemArray* itemArray,
                 bool flushAfter,
                 RChannelItemArrayWriterHandler* handler)
{
    LogAssert(itemArray != NULL);
    m_itemArray = itemArray;
    m_flushAfter = flushAfter;
    m_handler = handler;
    m_currentItem = 0;
    m_aborted = false;
}

RChannelSerializedWriter::WriteRequest::~WriteRequest()
{
    LogAssert(m_itemArray == NULL);
    LogAssert(m_handler == NULL);
    LogAssert(m_failureArray == NULL);
}

void RChannelSerializedWriter::WriteRequest::
    SetHandler(RChannelItemArrayWriterHandler* handler)
{
    LogAssert(m_handler == NULL);
    m_handler = handler;
}

RChannelItem* RChannelSerializedWriter::WriteRequest::GetNextItem()
{
    LogAssert(m_currentItem < m_itemArray->GetNumberOfItems() &&
              m_aborted == false);
    return m_itemArray->GetItemArray()[m_currentItem];
}

void RChannelSerializedWriter::WriteRequest::SetSuccessItem()
{
    LogAssert(m_aborted == false);
    if (m_failureArray != NULL)
    {
        LogAssert(m_currentItem < m_failureArray->GetNumberOfItems());
        LogAssert(m_failureArray->GetItemArray()[m_currentItem] == NULL);
    }
    ++m_currentItem;
}

void RChannelSerializedWriter::WriteRequest::
    SetFailureItem(RChannelItem* marshalFailureItem, bool abort)
{
    LogAssert(m_aborted == false);
    m_aborted = abort;

    if (m_failureArray == NULL)
    {
        m_failureArray.Attach(new RChannelItemArray());
        m_failureArray->SetNumberOfItems(m_itemArray->GetNumberOfItems());
    }

    LogAssert(m_currentItem < m_failureArray->GetNumberOfItems());
    LogAssert(m_failureArray->GetItemArray()[m_currentItem] == NULL);
    m_failureArray->GetItemArray()[m_currentItem] = marshalFailureItem;

    ++m_currentItem;
}

bool RChannelSerializedWriter::WriteRequest::ShouldFlush()
{
    return m_flushAfter;
}

bool RChannelSerializedWriter::WriteRequest::LastItem()
{
    LogAssert(m_aborted == false);
    return (m_currentItem == m_itemArray->GetNumberOfItems()-1);
}

bool RChannelSerializedWriter::WriteRequest::Completed()
{
    return (m_currentItem == m_itemArray->GetNumberOfItems() ||
            m_aborted == true);
}

void RChannelSerializedWriter::WriteRequest::
    ProcessMarshalCompleted(RChannelItemType returnCode)
{
    LogAssert(m_handler != NULL);

    if (m_aborted == false)
    {
        LogAssert(m_currentItem == m_itemArray->GetNumberOfItems());
    }

    m_handler->ProcessWriteArrayCompleted(returnCode, m_failureArray);

    m_itemArray = NULL;
    m_failureArray = NULL;
    m_handler = NULL;
}


RChannelSerializedWriter::SyncHandler::SyncHandler()
{
    m_event = NULL;
    m_statusCode = RChannelItem_EndOfStream;
    m_usingEvent = 0;
}

RChannelSerializedWriter::SyncHandler::~SyncHandler()
{
    LogAssert(m_event == NULL);
}

void RChannelSerializedWriter::SyncHandler::
    UseEvent(DryadHandleListEntry* event)
{
    LONG postIncrement = ::InterlockedIncrement(&m_usingEvent);
    LogAssert(postIncrement == 1);
    LogAssert(m_event == NULL);
    m_event = event;
}

void RChannelSerializedWriter::SyncHandler::
    ProcessWriteArrayCompleted(RChannelItemType statusCode,
                               RChannelItemArray* failureArray)
{
    m_statusCode = statusCode;
    m_failureArray = failureArray;

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

bool RChannelSerializedWriter::SyncHandler::UsingEvent()
{
    return (m_event != NULL);
}

RChannelItemType RChannelSerializedWriter::SyncHandler::GetStatusCode()
{
    return m_statusCode;
}

void RChannelSerializedWriter::SyncHandler::
    GetFailureItemArray(RChannelItemArrayRef* pFailureArray)
{
    *pFailureArray = m_failureArray;
}

void RChannelSerializedWriter::SyncHandler::Wait()
{
    LogAssert(m_event != NULL);
    DWORD dRet = ::WaitForSingleObject(m_event->GetHandle(), INFINITE);
    LogAssert(dRet == WAIT_OBJECT_0);
}

DryadHandleListEntry* RChannelSerializedWriter::SyncHandler::GetEvent()
{
    LogAssert(m_event != NULL);
    DryadHandleListEntry* retVal = m_event;
    m_event = NULL;
    return retVal;
}


RChannelSerializedWriter::
    RChannelSerializedWriter(RChannelBufferWriter* writer,
                             RChannelItemMarshalerBase* marshaler,
                             bool breakBufferOnRecordBoundaries,
                             UInt32 maxMarshalBatchSize,
                             WorkQueue* workQueue)
{
    m_breakBufferOnRecordBoundaries = breakBufferOnRecordBoundaries;
    m_writer = writer;
    m_marshaler = marshaler;
    m_workQueue = workQueue;

    m_maxMarshalBatchSize = m_marshaler->GetMaxMarshalBatchSize();
    if (m_maxMarshalBatchSize == 0)
    {
        m_maxMarshalBatchSize = maxMarshalBatchSize;
    }

    m_state = CW_Stopped;
    m_outstandingBuffers = 0;
    m_outstandingHandlers = 0;
    m_marshaledTerminationItem = false;
    m_channelTermination = RChannelItem_Data;
    m_cachedWriter = NULL;

    m_handlerReturnEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_handlerReturnEvent != NULL);
    m_marshaledLastItemEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_marshaledLastItemEvent != NULL);
}

RChannelSerializedWriter::~RChannelSerializedWriter()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        DrLogD("Uninitializing RChannelSerializedWriter, uri = %s", m_uri != NULL ? m_uri.GetString() : "");

        LogAssert(m_state == CW_Closed);
        LogAssert(m_writerTerminationItem == NULL);
        LogAssert(m_readerTerminationItem == NULL);
        LogAssert(m_cachedWriter == NULL);

        BOOL bRet = ::CloseHandle(m_handlerReturnEvent);
        LogAssert(bRet != 0);
        m_handlerReturnEvent = INVALID_HANDLE_VALUE;

        bRet = ::CloseHandle(m_marshaledLastItemEvent);
        LogAssert(bRet != 0);
        m_marshaledLastItemEvent = INVALID_HANDLE_VALUE;
    }
}


void RChannelSerializedWriter::SetURI(const char* uri)
{
    m_uri = uri;
}

const char* RChannelSerializedWriter::GetURI()
{
    return m_uri;
}

UInt64 RChannelSerializedWriter::GetInitialSizeHint()
{
    return m_writer->GetInitialSizeHint();
}

void RChannelSerializedWriter::SetInitialSizeHint(UInt64 hint)
{
    m_writer->SetInitialSizeHint(hint);
}

void RChannelSerializedWriter::Start()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == CW_Stopped);
        LogAssert(m_pendingList.IsEmpty());
        LogAssert(m_blockedHandlerList.IsEmpty());
        LogAssert(m_bufferList.IsEmpty());
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingHandlers == 0);
        LogAssert(m_marshaledTerminationItem == false);
        LogAssert(m_cachedWriter == NULL);

        m_writerTerminationItem = NULL;
        m_readerTerminationItem = NULL;
        m_state = CW_Empty;
        m_channelTermination = RChannelItem_Data;
        m_returnLatch.Start();
    }

    m_marshaler->Reset();
    m_writer->Start();
}

void RChannelSerializedWriter::
    GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                        RChannelItemRef* pReaderDrainItem)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        *pWriterDrainItem = m_writerTerminationItem;
        *pReaderDrainItem = m_readerTerminationItem;
    }
}

void RChannelSerializedWriter::Close()
{
    m_writer->Close();

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == CW_Stopped);
        LogAssert(m_pendingList.IsEmpty());
        LogAssert(m_blockedHandlerList.IsEmpty());
        LogAssert(m_bufferList.IsEmpty());
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingHandlers == 0);
        LogAssert(m_marshaledTerminationItem == false);
        LogAssert(m_cachedWriter == NULL);

        m_writerTerminationItem = NULL;
        m_readerTerminationItem = NULL;
        m_state = CW_Closed;
    }
}

void RChannelSerializedWriter::MakeCachedWriter()
{
    if (m_cachedWriter == NULL)
    {
        DrRef<DrMemoryBuffer> buffer;
        buffer.Attach(new RChannelWriterBuffer(m_writer, &m_bufferList));
        m_cachedWriter = new ChannelMemoryBufferWriter(buffer, &m_bufferList);
    }
}

Size_t RChannelSerializedWriter::DisposeOfCachedWriter()
{
    Size_t preMarshalAvailableSize = 0;
    if (m_cachedWriter != NULL)
    {
        preMarshalAvailableSize = m_cachedWriter->GetLastRecordBoundary();
        DrError err = m_cachedWriter->CloseMemoryWriter();
        LogAssert(err == DrError_OK);
        delete m_cachedWriter;
        m_cachedWriter = NULL;
    }
    return preMarshalAvailableSize;
}

/* called with m_baseCS held */
void RChannelSerializedWriter::AcceptReturningHandlers(UInt32 handlerCount)
{
    LogAssert(m_outstandingHandlers >= handlerCount);
    m_outstandingHandlers -= handlerCount;

    if (m_state == CW_Stopping && m_outstandingHandlers == 0)
    {
        BOOL bRet = ::SetEvent(m_handlerReturnEvent);
        LogAssert(bRet != 0);
    }
}

void RChannelSerializedWriter::ReturnHandlers(WriteRequestList* completedList,
                                              RChannelItemType returnCode)
{
    while (completedList->IsEmpty() == false)
    {
        UInt32 returnedHandlerCount = 0;

        WriteRequest* returnRequest =
            completedList->CastOut(completedList->RemoveHead());

        do
        {
            ++returnedHandlerCount;
            returnRequest->ProcessMarshalCompleted(returnCode);
            delete returnRequest;
            returnRequest =
                completedList->CastOut(completedList->RemoveHead());
        } while (returnRequest != NULL);

        {
            AutoCriticalSection acs(&m_baseCS);

            m_returnLatch.TransferList(completedList);

            AcceptReturningHandlers(returnedHandlerCount);
        }
    }
}

/* called with m_baseCS held */
bool RChannelSerializedWriter::
    CheckForTerminationItem(RChannelItemArray* itemArray)
{
    if (m_writerTerminationItem != NULL || m_state == CW_Stopped)
    {
        return true;
    }
    else
    {
        UInt32 nItems = itemArray->GetNumberOfItems();
        LogAssert(nItems > 0);
        RChannelItemRef* items = itemArray->GetItemArray();

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
            }
        }

        return false;
    }
}

/* called **without** m_baseCS held but this is OK since we are in the
   marshaling state which means no other thread is touching
   m_bufferList */
void RChannelSerializedWriter::
    RestorePreMarshalBuffers(Size_t preMarshalAvailableSize)
{
    LogAssert(m_state == CW_Marshaling);
    LogAssert(m_cachedWriter == NULL);

    if (m_bufferList.IsEmpty())
    {
        LogAssert(preMarshalAvailableSize == 0);
        return;
    }

    DrBListEntry* listEntry = m_bufferList.GetTail();
    while (listEntry != NULL &&
           (m_bufferList.GetPrev(listEntry) != NULL ||
            preMarshalAvailableSize == 0))
    {
        DryadFixedMemoryBuffer* buffer = m_bufferList.CastOut(listEntry);
        listEntry = m_bufferList.GetPrev(listEntry);
        m_bufferList.Remove(m_bufferList.CastIn(buffer));
        m_writer->ReturnUnusedBuffer(buffer);
    }

    if (preMarshalAvailableSize > 0)
    {
        LogAssert(m_bufferList.CountLinks() == 1);
        DryadFixedMemoryBuffer* buffer =
            m_bufferList.CastOut(m_bufferList.GetHead());
        LogAssert(buffer->GetAllocatedSize() > preMarshalAvailableSize);
        LogAssert(buffer->GetAvailableSize() >= preMarshalAvailableSize);
        buffer->SetAvailableSize(preMarshalAvailableSize);
    }
}

bool RChannelSerializedWriter::
    SendCompletedBuffers(bool shouldFlush, RChannelItemType terminationType)
{
    Size_t preMarshalAvailableSize = DisposeOfCachedWriter();

    if (m_breakBufferOnRecordBoundaries)
    {
        ShuffleBuffersOnRecordBoundaries(preMarshalAvailableSize);
    }

    UInt32 bufferCount = m_bufferList.CountLinks();
    DryadFixedBufferList sendList;
    sendList.TransitionToTail(&m_bufferList);

//     DrLogE( "SendCompletedBuffers",
//         "count %u", bufferCount);

    if (shouldFlush == false && terminationType == RChannelItem_Data)
    {
        /* we're only here because we filled a buffer, and we may not
           want to send the partial one at the end of the list */
        DryadFixedMemoryBuffer* lastBuffer =
            sendList.CastOut(sendList.GetTail());
        if (lastBuffer->GetAvailableSize() != lastBuffer->GetAllocatedSize())
        {
            m_bufferList.TransitionToTail(m_bufferList.CastIn(lastBuffer));
            --bufferCount;
        }
    }

    {
        AutoCriticalSection acs(&m_baseCS);

        if (terminationType != RChannelItem_Data)
        {
            LogAssert(m_writerTerminationItem != NULL);
            /* the additional "buffer" accounts for the call to
               writer->WriteTermination below */
            ++bufferCount;
        }

        m_outstandingBuffers += bufferCount;
    }

    bool shouldBlock = false;

    DryadFixedMemoryBuffer* nextBuffer =
        sendList.CastOut(sendList.RemoveHead());
    while (nextBuffer != NULL)
    {
        shouldBlock =
            m_writer->WriteBuffer(nextBuffer, shouldFlush, this);
        nextBuffer = sendList.CastOut(sendList.RemoveHead());
    }

    if (terminationType != RChannelItem_Data)
    {
        shouldBlock = true;
        m_writer->WriteTermination(terminationType, this);
    }

    return shouldBlock;
}

RChannelItemType RChannelSerializedWriter::
    PerformSingleMarshal(WriteRequest* writeRequest)
{
    LogAssert(m_cachedWriter != NULL);

    RChannelItem* item = writeRequest->GetNextItem();
    RChannelItemType itemType = item->GetType();
    if (RChannelItem::IsTerminationItem(itemType) == false)
    {
        itemType = RChannelItem_Data;
    }

    bool shouldFlush = (writeRequest->ShouldFlush() &&
                        writeRequest->LastItem());

    RChannelItemRef marshalFailure;
    DrError marshalStatus =
        m_marshaler->MarshalItem(m_cachedWriter, item, shouldFlush,
                                 &marshalFailure);
    DrError errTmp = m_cachedWriter->FlushMemoryWriter();
    LogAssert(errTmp == DrError_OK);

    if (marshalStatus == DrError_IncompleteOperation)
    {
        /* do nothing, we will call MarshalItem again on the same item
           next time around */
        LogAssert(itemType == RChannelItem_Data);
    }
    else if (marshalStatus == DrError_OK)
    {
        writeRequest->SetSuccessItem();
    }
    else
    {
        if (marshalFailure == NULL)
        {
            marshalFailure.Attach(RChannelMarkerItem::
                                  Create(RChannelItem_MarshalError, false));
        }

        Size_t preMarshalAvailableSize = DisposeOfCachedWriter();

        RestorePreMarshalBuffers(preMarshalAvailableSize);

        MakeCachedWriter();

        RChannelItemRef secondFailure;
        m_marshaler->MarshalItem(m_cachedWriter, marshalFailure, shouldFlush,
                                 &secondFailure);
        errTmp = m_cachedWriter->FlushMemoryWriter();
        LogAssert(errTmp == DrError_OK);

        bool aborted = (marshalStatus == DryadError_ChannelAbort ||
                        marshalStatus == DryadError_ChannelRestart);

        writeRequest->SetFailureItem(marshalFailure, aborted);

        if (aborted)
        {
            itemType = (marshalStatus == DryadError_ChannelAbort) ?
                (RChannelItem_Abort) : (RChannelItem_Restart);
            RChannelItemRef terminationItem;
            terminationItem.
                Attach(RChannelMarkerItem::
                       CreateErrorItemWithDescription(itemType,
                                                      marshalStatus,
                                                      "Marshal failure caused "
                                                      "termination"));
            m_marshaler->MarshalItem(m_cachedWriter,
                                     terminationItem, shouldFlush,
                                     &secondFailure);
            errTmp = m_cachedWriter->FlushMemoryWriter();
            LogAssert(errTmp == DrError_OK);
        }
    }

    return itemType;
}

void RChannelSerializedWriter::CollapseToSingleBuffer()
{
    Size_t totalSize = 0;
    DryadFixedMemoryBuffer* buffer =
        m_bufferList.CastOut(m_bufferList.GetHead());
    while (buffer != NULL)
    {
        totalSize += buffer->GetAvailableSize();
        buffer = m_bufferList.GetNextTyped(buffer);
    }

    LogAssert(totalSize > 0);

    DryadFixedMemoryBuffer* combinedBuffer =
        m_writer->GetCustomWriteBuffer(totalSize);

    totalSize = 0;
    buffer = m_bufferList.CastOut(m_bufferList.RemoveHead());
    while (buffer != NULL)
    {
        Size_t contiguous;

        void* dstPtr =
            combinedBuffer->GetWriteAddress(totalSize,
                                            buffer->GetAvailableSize(),
                                            &contiguous);
        LogAssert(contiguous >= buffer->GetAvailableSize());

        const void* srcPtr =
            buffer->GetReadAddress(0, &contiguous);
        LogAssert(contiguous >= buffer->GetAvailableSize());

        ::memcpy(dstPtr, srcPtr, buffer->GetAvailableSize());

        totalSize += buffer->GetAvailableSize();

        m_writer->ReturnUnusedBuffer(buffer);

        buffer = m_bufferList.CastOut(m_bufferList.RemoveHead());
    }

    LogAssert(totalSize == combinedBuffer->GetAllocatedSize());
    combinedBuffer->SetAvailableSize(totalSize);

    m_bufferList.InsertAsTail(m_bufferList.CastIn(combinedBuffer));
}

void RChannelSerializedWriter::
    ShuffleBuffersOnRecordBoundaries(Size_t preMarshalAvailableSize)
{
    UInt32 bufferCount = m_bufferList.CountLinks();

    if (bufferCount < 2)
    {
        /* there is nothing to write or the record boundary coincides
           with the buffer boundary, so we don't need to move anything
           around */
        DrLogD( "ShuffleBuffers taking no action");
        return;
    }

    DryadFixedMemoryBuffer* headBuffer =
        m_bufferList.CastOut(m_bufferList.GetHead());
    LogAssert(headBuffer != NULL);

    LogAssert(headBuffer->GetAvailableSize() ==
              headBuffer->GetAllocatedSize());
    LogAssert(preMarshalAvailableSize < headBuffer->GetAvailableSize());
    Size_t overhang = headBuffer->GetAvailableSize() - preMarshalAvailableSize;

    DryadFixedMemoryBuffer* nextBuffer =
        m_bufferList.GetNextTyped(headBuffer);
    LogAssert(nextBuffer != NULL);

    Size_t remainingSpace =
        nextBuffer->GetAllocatedSize() - nextBuffer->GetAvailableSize();

    if (preMarshalAvailableSize == 0 || bufferCount > 2 ||
        remainingSpace < overhang)
    {
        /* there isn't space to just shift the overhang into the next
           buffer (the common case) so collapse everything written up
           to now into a single buffer to be written as a unit */
        CollapseToSingleBuffer();
    }
    else
    {
        /* shift up the overhang into the next buffer, copying the
           data that is already there out of the way first */

        Size_t copySize = nextBuffer->GetAvailableSize();
        Size_t availableSize;
        const void* srcPtr = nextBuffer->GetReadAddress(0, &availableSize);
        LogAssert(availableSize >= copySize);
        void* dstPtr = nextBuffer->GetWriteAddress(overhang,
                                                   copySize,
                                                   &availableSize);
        LogAssert(availableSize >= copySize);

        DrLogD( "ShuffleBuffers shifting data. Overhang size %Iu/%Iu copy size %Iu/%Iu",
            overhang, headBuffer->GetAvailableSize(),
            copySize, nextBuffer->GetAllocatedSize());

        ::memmove(dstPtr, srcPtr, copySize);

        dstPtr = nextBuffer->GetWriteAddress(0, overhang, &availableSize);
        LogAssert(availableSize >= overhang);
        srcPtr = headBuffer->GetReadAddress(preMarshalAvailableSize,
                                            &availableSize);
        LogAssert(availableSize >= overhang);
        ::memcpy(dstPtr, srcPtr, overhang);

        headBuffer->SetAvailableSize(preMarshalAvailableSize);
        nextBuffer->SetAvailableSize(copySize + overhang);
    }
}

bool RChannelSerializedWriter::
    PerformMarshal(WriteRequestList* pendingRequestList,
                   WriteRequestList* completedRequestList)
{
    LogAssert(pendingRequestList->IsEmpty() == false);

    MakeCachedWriter();

    UInt32 marshaledItemCount = 0;

    bool filledBuffer = m_cachedWriter->MarkRecordBoundary();
    LogAssert(filledBuffer == false);
    bool shouldFlush = false;
    RChannelItemType terminationType = RChannelItem_Data;

    do
    {
        WriteRequest* writeRequest =
            pendingRequestList->CastOut(pendingRequestList->GetHead());

        LogAssert(writeRequest->Completed() == false);

        do
        {
            ++marshaledItemCount;

            terminationType = PerformSingleMarshal(writeRequest);

            filledBuffer = m_cachedWriter->MarkRecordBoundary();

            if (terminationType != RChannelItem_Data)
            {
                LogAssert(writeRequest->Completed());
            }
        } while (filledBuffer == false &&
                 writeRequest->Completed() == false &&
                 marshaledItemCount < m_maxMarshalBatchSize);

        if (writeRequest->Completed())
        {
            completedRequestList->
                TransitionToTail(completedRequestList->CastIn(writeRequest));
            shouldFlush = writeRequest->ShouldFlush();
        }
    } while (filledBuffer == false &&
             shouldFlush == false &&
             terminationType == RChannelItem_Data &&
             pendingRequestList->IsEmpty() == false &&
             marshaledItemCount < m_maxMarshalBatchSize);

    bool shouldBlock = false;

    if (filledBuffer || shouldFlush || terminationType != RChannelItem_Data)
    {
        shouldBlock =
            SendCompletedBuffers(shouldFlush, terminationType) ||
            shouldFlush || terminationType != RChannelItem_Data;
    }

    return shouldBlock;
}

void RChannelSerializedWriter::
    PostMarshal(WriteRequestList* pendingRequestList,
                WriteRequestList* completedRequestList,
                bool shouldBlock,
                SyncHandler* syncHandler)
{
    bool marshaledLast = false;
    WorkRequest* nextRequest = NULL;
    RChannelItemType returnCode;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == CW_Marshaling);

        /* stick any newly arrived pending requests on the end of our
           list */
        pendingRequestList->TransitionToTail(&m_pendingList);
        /* then move the whole thing back where it belongs */
        m_pendingList.TransitionToTail(pendingRequestList);

        /* block when requested unless the outstanding buffers got
           returned on another thread between the call to
           SendCompletedBuffers and here */
        if (shouldBlock && m_outstandingBuffers > 0)
        {
            m_blockedHandlerList.TransitionToTail(completedRequestList);
            m_state = CW_Blocking;
        }
        else if (m_pendingList.IsEmpty())
        {
            if (m_writerTerminationItem != NULL)
            {
                LogAssert(m_outstandingHandlers > 0);
                m_state = CW_Stopping;
            }
            else
            {
                m_state = CW_Empty;
            }
        }
        else
        {
            m_state = CW_InWorkQueue;
            nextRequest = new RChannelMarshalRequest(this);
        }

        returnCode = m_channelTermination;

        if (m_state != CW_InWorkQueue &&
            m_writerTerminationItem != NULL &&
            m_pendingList.IsEmpty())
        {
            LogAssert(m_marshaledTerminationItem == false);
            m_marshaledTerminationItem = true;
            marshaledLast = true;
        }

        m_returnLatch.AcceptList(completedRequestList);

        if (completedRequestList->IsEmpty() && syncHandler != NULL)
        {
            syncHandler->UseEvent(m_eventCache.GetEvent(true));
        }
    }

    if (marshaledLast)
    {
        DrLogD("Writer marshalled last item.");
        BOOL bRet = ::SetEvent(m_marshaledLastItemEvent);
        LogAssert(bRet != 0, "SetEvent() failed with error code = %d", GetLastError());
    }

    ReturnHandlers(completedRequestList, returnCode);

    if (nextRequest != NULL)
    {
        m_workQueue->EnQueue(nextRequest);
    }
}

void RChannelSerializedWriter::MarshalItems()
{
    WriteRequestList pendingRequestList;
    WriteRequestList completedRequestList;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == CW_InWorkQueue);

        m_state = CW_Marshaling;

        pendingRequestList.TransitionToTail(&m_pendingList);

        LogAssert(pendingRequestList.IsEmpty() == false);
    }

    bool shouldBlock = PerformMarshal(&pendingRequestList,
                                      &completedRequestList);

    PostMarshal(&pendingRequestList, &completedRequestList,
                shouldBlock, NULL);

    LogAssert(pendingRequestList.IsEmpty());
    LogAssert(completedRequestList.IsEmpty());
}

void RChannelSerializedWriter::ProcessWriteCompleted(RChannelItemType status)
{
    WriteRequestList unblockList;
    WorkRequest* nextRequest = NULL;
    RChannelItemType returnCode;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_outstandingBuffers > 0);
        --m_outstandingBuffers;

        if (m_outstandingBuffers <= 2 &&
            m_state == CW_Blocking &&
            m_writerTerminationItem == NULL)
        {
            if (m_pendingList.IsEmpty())
            {
                m_state = CW_Empty;
            }
            else
            {
                m_state = CW_InWorkQueue;
                nextRequest = new RChannelMarshalRequest(this);
            }

            unblockList.TransitionToHead(&m_blockedHandlerList);
        }

        if (m_outstandingBuffers == 0)
        {
            if (m_state == CW_Blocking)
            {
                if (m_pendingList.IsEmpty())
                {
                    if (m_writerTerminationItem != NULL)
                    {
                        m_state = CW_Stopping;
                        /* if we are unblocking there must be at least
                           one handler queued to be released after the
                           block */
                        LogAssert(m_blockedHandlerList.IsEmpty() == false);
                        LogAssert(m_outstandingHandlers > 0);
                    }
                    else
                    {
                        m_state = CW_Empty;
                    }
                }
                else
                {
                    m_state = CW_InWorkQueue;
                    nextRequest = new RChannelMarshalRequest(this);
                }

                unblockList.TransitionToHead(&m_blockedHandlerList);
            }
            else
            {
                LogAssert(m_state == CW_InWorkQueue ||
                          m_state == CW_Marshaling ||
                          m_state == CW_Empty);
            }
        }

        if (RChannelItem::IsTerminationItem(status))
        {
            if (status != RChannelItem_EndOfStream)
            {
                m_channelTermination = status;
            }
        }

        returnCode = m_channelTermination;

        m_returnLatch.AcceptList(&unblockList);
    }

    ReturnHandlers(&unblockList, returnCode);

    if (nextRequest != NULL)
    {
        m_workQueue->EnQueue(nextRequest);
    }
}

void RChannelSerializedWriter::
    WriteItemArray(RChannelItemArrayRef& itemArrayIn,
                   bool flushAfter,
                   RChannelItemArrayWriterHandler* handler)
{
    RChannelItemArrayRef itemArray;
    /* ensure that the caller no longer holds a reference to this array */
    itemArray.TransferFrom(itemArrayIn);

    LogAssert(itemArray->GetNumberOfItems() > 0);
    LogAssert(handler != NULL);

    WorkRequest* workRequest = NULL;

    bool alreadyTerminated;

    {
        AutoCriticalSection acs(&m_baseCS);

        alreadyTerminated = CheckForTerminationItem(itemArray);

        if (alreadyTerminated)
        {
            LogAssert(m_state == CW_InWorkQueue ||
                      m_state == CW_Marshaling ||
                      m_state == CW_Blocking ||
                      m_state == CW_Stopping ||
                      m_state == CW_Stopped);
        }
        else
        {
            if (m_state == CW_Empty)
            {
                LogAssert(m_pendingList.IsEmpty());
                m_state = CW_InWorkQueue;
                workRequest = new RChannelMarshalRequest(this);
            }
            else
            {
                LogAssert(m_state == CW_InWorkQueue ||
                          m_state == CW_Marshaling ||
                          m_state == CW_Blocking);
            }

            WriteRequest* writeRequest =
                new WriteRequest(itemArray, flushAfter, handler);
            m_pendingList.InsertAsTail(m_pendingList.CastIn(writeRequest));

            ++m_outstandingHandlers;
        }
    }

    if (alreadyTerminated)
    {
        LogAssert(workRequest == NULL);
        handler->ProcessWriteArrayCompleted(RChannelItem_EndOfStream, NULL);
    }
    else
    {
        if (workRequest != NULL)
        {
            m_workQueue->EnQueue(workRequest);
        }
    }
}

RChannelItemType RChannelSerializedWriter::
    WriteItemArraySync(RChannelItemArrayRef& itemArrayIn,
                       bool flush,
                       RChannelItemArrayRef* pFailureArray)
{
    RChannelItemArrayRef itemArray;
    /* ensure that the caller no longer holds a reference to this array */
    itemArray.TransferFrom(itemArrayIn);

    LogAssert(itemArray->GetNumberOfItems() > 0);

    bool alreadyTerminated;
    bool shouldMarshal = false;

    SyncHandler* handler = NULL;
    WriteRequestList pendingRequestList;

    {
        AutoCriticalSection acs(&m_baseCS);

        alreadyTerminated = CheckForTerminationItem(itemArray);

        if (alreadyTerminated)
        {
            LogAssert(m_state == CW_InWorkQueue ||
                      m_state == CW_Marshaling ||
                      m_state == CW_Blocking ||
                      m_state == CW_Stopping ||
                      m_state == CW_Stopped);
        }
        else
        {
            handler = new SyncHandler();
            WriteRequest* writeRequest =
                new WriteRequest(itemArray, flush, handler);

            if (m_state == CW_Empty)
            {
                m_state = CW_Marshaling;

                pendingRequestList.InsertAsTail(pendingRequestList.
                                                CastIn(writeRequest));
                shouldMarshal = true;
            }
            else
            {
                LogAssert(m_state == CW_InWorkQueue ||
                          m_state == CW_Marshaling ||
                          m_state == CW_Blocking);

                m_pendingList.InsertAsTail(m_pendingList.CastIn(writeRequest));
                writeRequest = NULL;
                handler->UseEvent(m_eventCache.GetEvent(true));
            }

            ++m_outstandingHandlers;
        }
    }

    if (alreadyTerminated)
    {
        LogAssert(handler == NULL);
        LogAssert(pendingRequestList.IsEmpty());
        if (pFailureArray != NULL)
        {
            *pFailureArray = NULL;
        }
        return RChannelItem_EndOfStream;
    }

    if (shouldMarshal)
    {
        LogAssert(pendingRequestList.IsEmpty() == false);
        WriteRequestList completedRequestList;
        bool shouldBlock = false;
        do
        {
            /* keep marshaling until we have done everything in this
               request */
            shouldBlock = PerformMarshal(&pendingRequestList,
                                         &completedRequestList) ||
                shouldBlock;
        } while (pendingRequestList.IsEmpty() == false);

        PostMarshal(&pendingRequestList, &completedRequestList,
                    shouldBlock, handler);

        LogAssert(pendingRequestList.IsEmpty());
        LogAssert(completedRequestList.IsEmpty());
    }

    if (handler->UsingEvent())
    {
//         DrLogE( "Waiting for handler");
        handler->Wait();
//         DrLogE( "Waited for handler");

        {
            AutoCriticalSection acs(&m_baseCS);

            m_eventCache.ReturnEvent(handler->GetEvent());
        }
    }

    RChannelItemType returnCode = handler->GetStatusCode();
    if (pFailureArray != NULL)
    {
        handler->GetFailureItemArray(pFailureArray);
    }
    delete handler;

    LogAssert(returnCode != RChannelItem_EndOfStream);

    return returnCode;
}

void RChannelSerializedWriter::Drain(DrTimeInterval csTimeOut,
                                     RChannelItemRef* pRemoteStatus)
{
    bool mustWaitForHandler = false;
    bool mustWaitForMarshal = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_writerTerminationItem != NULL);

        if (m_outstandingHandlers > 0 || m_state != CW_Stopping)
        {
            BOOL bRet = ::ResetEvent(m_handlerReturnEvent);
            LogAssert(bRet != 0);
            mustWaitForHandler = true;
        }

        if (m_marshaledTerminationItem == false)
        {
            BOOL bRet = ::ResetEvent(m_marshaledLastItemEvent);
            LogAssert(bRet != 0);
            mustWaitForMarshal = true;
        }
    }

    if (mustWaitForMarshal)
    {
        DWORD dRet = ::WaitForSingleObject(m_marshaledLastItemEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    RChannelItemRef returnItem;
    m_writer->Drain(&returnItem);
    LogAssert(returnItem != NULL);

    if (mustWaitForHandler)
    {
        DWORD dRet = ::WaitForSingleObject(m_handlerReturnEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == CW_Stopping);

        if (m_writerTerminationItem->GetType() == RChannelItem_EndOfStream &&
            returnItem->GetType() != RChannelItem_EndOfStream)
        {
            // there was a close error
            m_writerTerminationItem = returnItem;
        }

        LogAssert(m_pendingList.IsEmpty());
        LogAssert(m_blockedHandlerList.IsEmpty());
        LogAssert(m_bufferList.IsEmpty());
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingHandlers == 0);
        LogAssert(m_writerTerminationItem != NULL);
        LogAssert(m_marshaledTerminationItem == true);
        LogAssert(m_readerTerminationItem == NULL);
        LogAssert(m_cachedWriter == NULL);

        m_readerTerminationItem = returnItem;

        m_returnLatch.Stop();
        m_marshaledTerminationItem = false;
        m_state = CW_Stopped;
    }

    if (pRemoteStatus != NULL)
    {
        *pRemoteStatus = returnItem;
    }
}

RChannelBufferWriterHandler::~RChannelBufferWriterHandler()
{
}

RChannelBufferWriter::~RChannelBufferWriter()
{
}

void RChannelBufferWriter::FillInStatus(DryadChannelDescription* status)
{
}


RChannelNullWriter::RChannelNullWriter(const char* uri)
{
    m_uri = uri;
    m_started = false;
}

const char* RChannelNullWriter::GetURI()
{
    return m_uri;
}

UInt64 RChannelNullWriter::GetInitialSizeHint()
{
    return 0;
}

void RChannelNullWriter::SetInitialSizeHint(UInt64 /*hint*/)
{
}

void RChannelNullWriter::Start()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        m_writeTerminationItem = NULL;
        m_started = true;
    }
}

void RChannelNullWriter::
    WriteItemArray(RChannelItemArrayRef& itemArray,
                   bool flushAfter,
                   RChannelItemArrayWriterHandler* handler)
{
    RChannelItemType status = WriteItemArraySync(itemArray, flushAfter, NULL);
    handler->ProcessWriteArrayCompleted(status, NULL);
}

RChannelItemType RChannelNullWriter::
    WriteItemArraySync(RChannelItemArrayRef& itemArray,
                       bool flush,
                       RChannelItemArrayRef* pFailureArray)
{
    RChannelItemType status;

    {
        AutoCriticalSection acs(&m_baseCS);

        if (m_started)
        {
            UInt32 numberOfItems = itemArray->GetNumberOfItems();
            RChannelItemRef* array = itemArray->GetItemArray();
            UInt32 i;
            for (i=0; i<numberOfItems; ++i)
            {
                RChannelItemRef item;
                item.TransferFrom(array[i]);
                if (m_writeTerminationItem == NULL &&
                    RChannelItem::IsTerminationItem(item->GetType()))
                {
                    m_writeTerminationItem = item;
                }
            }

            if (m_writeTerminationItem == NULL)
            {
                status = RChannelItem_Data;
            }
            else
            {
                status = RChannelItem_EndOfStream;
            }
        }
        else
        {
            status = RChannelItem_EndOfStream;
        }
    }

    itemArray = NULL;

    return status;
}

void RChannelNullWriter::Drain(DrTimeInterval timeOut,
                               RChannelItemRef* pRemoteStatus)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_started);
        LogAssert(m_writeTerminationItem != NULL);

        *pRemoteStatus = m_writeTerminationItem;
        m_started = false;
    }
}

void RChannelNullWriter::GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                                             RChannelItemRef* pReaderDrainItem)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        *pWriterDrainItem = m_writeTerminationItem;
        *pReaderDrainItem = m_writeTerminationItem;
    }
}

void RChannelNullWriter::Close()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_started == false);
    }
}
