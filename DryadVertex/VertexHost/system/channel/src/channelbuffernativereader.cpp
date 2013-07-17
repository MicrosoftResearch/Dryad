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

#define _CRT_RAND_S
#include <stdlib.h>

#include <DrExecution.h>
#include <channelbuffernativereader.h>
#include <dryadmetadata.h>
#include <concreterchannel.h>
#include <workqueue.h>
#include <dryadstandaloneini.h>

#pragma unmanaged


//
// Create a new generic read handle
//
RChannelBufferReaderNative::ReadHandler::ReadHandler(UInt64 requestOffset,
                                                     UInt64 streamOffset,
                                                     UInt32 dataSize,
                                                     size_t dataAlignment)
    
{
    //
    // Initialize the handler with the offset and data size
    //
    this->DryadNativePort::Handler::InitializeInternal(dataSize,
                                                       requestOffset);

    m_streamOffset = streamOffset;
    m_buffer = NULL;
    
    //
    // Create a fixed size buffer if there is any data to read
    //
    if (dataSize > 0)
    {
        m_block = new DryadAlignedReadBlock(dataSize, dataAlignment);
    }
    else
    {
        m_block = NULL;
    }

    m_isLastDataBuffer = false;
}

RChannelBufferReaderNative::ReadHandler::~ReadHandler()
{
    LogAssert(m_buffer == NULL);
    if (m_block != NULL)
    {
        m_block->DecRef();
    }
}

UInt64 RChannelBufferReaderNative::ReadHandler::GetStreamOffset()
{
    return m_streamOffset;
}

void* RChannelBufferReaderNative::ReadHandler::GetData()
{
    LogAssert(m_block != NULL);
    return m_block->GetData();
}

DryadAlignedReadBlock* RChannelBufferReaderNative::ReadHandler::GetBlock()
{
    LogAssert(m_block != NULL);
    return m_block;
}

void RChannelBufferReaderNative::ReadHandler::
    SetChannelBuffer(RChannelBuffer* buffer)
{
    LogAssert(m_buffer == NULL);
    m_buffer = buffer;
}

//
// Return pointer to current buffer and forget about it
//
RChannelBuffer* RChannelBufferReaderNative::ReadHandler::
    TransferChannelBuffer()
{
    LogAssert(m_buffer != NULL);
    RChannelBuffer* retval = m_buffer;
    m_buffer = NULL;
    return retval;
}

//
// Mark last data buffer
//
void RChannelBufferReaderNative::ReadHandler::SignalLastDataBuffer()
{
    LogAssert(m_isLastDataBuffer == false);
    LogAssert(m_buffer != NULL);
    LogAssert(m_buffer->GetType() == RChannelBuffer_Data);
    m_isLastDataBuffer = true;
}

bool RChannelBufferReaderNative::ReadHandler::IsLastDataBuffer()
{
    return m_isLastDataBuffer;
}

RChannelBufferReaderNative::
    RChannelBufferReaderNative(UInt32 prefetchBuffers,
                               DryadNativePort* port,
                               WorkQueue* workQueue,
                               RChannelOpenThrottler* openThrottler,
                               bool supportsLazyOpen)
{
    m_prefetchBuffers = prefetchBuffers;
    m_port = port;
    m_workQueue = workQueue;
    m_openThrottler = openThrottler;
    m_supportsLazyOpen = supportsLazyOpen;

    m_state = S_Closed;
    m_openState = OS_Closed;
    m_drainingOpenQueue = false;
    m_sentLastBuffer = false;
    m_outstandingHandlers = 0;
    m_outstandingBuffers = 0;

    m_totalLength = 0;
    m_nextStreamOffsetToProcess = 0;

    m_handlerReturnEvent = INVALID_HANDLE_VALUE;
    m_handler = NULL;
    m_bufferReturnEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_bufferReturnEvent != NULL);
}

RChannelBufferReaderNative::~RChannelBufferReaderNative()
{
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == S_Closed);
        LogAssert(m_openState == OS_Closed);
        LogAssert(m_drainingOpenQueue == false);
        BOOL bRet = ::CloseHandle(m_bufferReturnEvent);
        LogAssert(bRet != 0);
        LogAssert(m_errorBuffer == NULL);
    }
}

CRITSEC* RChannelBufferReaderNative::GetBaseDR()
{
    return &m_baseDR;
}

void RChannelBufferReaderNative::SetErrorBuffer(RChannelBuffer* buffer)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_errorBuffer == NULL)
        {
            m_errorBuffer = buffer;
        }
    }
}

void RChannelBufferReaderNative::
    FillInStatus(DryadChannelDescription* status)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        status->SetChannelTotalLength(m_totalLength);
        status->SetChannelProcessedLength(m_nextStreamOffsetToProcess);
    }
}

void RChannelBufferReaderNative::AssociateHandleWithPort(HANDLE h)
{
    m_port->AssociateHandle(h);
}

void RChannelBufferReaderNative::SetTotalLength(UInt64 totalLength)
{
    m_totalLength = totalLength;
}

bool RChannelBufferReaderNative::GetTotalLength(UInt64* pLen)
{
    *pLen = m_totalLength;
    return true;
}

/* called with baseDR held */
void RChannelBufferReaderNative::OpenNativeReader()
{
    LogAssert(m_state == S_Closed);
    LogAssert(m_openState == OS_Closed);
    LogAssert(m_drainingOpenQueue == false);
    m_state = S_Stopped;
    m_openState = OS_Stopped;
}

/* called with baseDR held */
RChannelBuffer* RChannelBufferReaderNative::CloseNativeReader()
{
    LogAssert(m_state == S_Stopped);
    LogAssert(m_openState == OS_Stopped);
    LogAssert(m_drainingOpenQueue == false);
    m_state = S_Closed;
    m_openState = OS_Closed;

    RChannelBuffer* errorBuffer = m_errorBuffer.Detach();

    return errorBuffer;
}

void RChannelBufferReaderNative::
    StartNativeReader(RChannelBufferReaderHandler* handler)
{
    HandlerList requestList;
    ChannelBufferList sendErrorBufferList;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == S_Stopped);
        LogAssert(m_openState == OS_Stopped);
        LogAssert(m_drainingOpenQueue == false);
        LogAssert(m_sentLastBuffer == false);
        LogAssert(m_outstandingHandlers == 0);
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_reorderMap.empty());
        LogAssert(m_handlerReturnEvent == INVALID_HANDLE_VALUE);
        m_handlerReturnEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
        LogAssert(m_handlerReturnEvent != NULL);
        LogAssert(m_handler == NULL);

        m_handler = handler;

        m_latch.Start();
        m_nextStreamOffsetToProcess = 0;

        m_fetching = true;

        bool lazyOpenDone = false;

        if (m_supportsLazyOpen)
        {
            m_openState = OS_NotOpened;
        }
        else
        {
            m_openState = OS_Opened;
            lazyOpenDone = true;
        }

        if (m_errorBuffer == NULL)
        {
            LogAssert(m_prefetchBuffers > 0);
            UInt32 i;
            for (i=0; i<m_prefetchBuffers; ++i)
            {
                ReadHandler* h = GetNextReadHandler(lazyOpenDone);
                requestList.InsertAsTail(requestList.CastIn(h));
            }
            m_outstandingHandlers = m_prefetchBuffers;
        }
        else
        {
            m_errorBuffer->IncRef();
            sendErrorBufferList.InsertAsTail(sendErrorBufferList.
                                             CastIn(m_errorBuffer));
            m_latch.AcceptList(&sendErrorBufferList);

            m_outstandingBuffers = 1;
            m_sentLastBuffer = true;
        }

        m_state = S_Running;
    }

    DrBListEntry* listEntry = requestList.GetHead();
    while (listEntry != NULL)
    {
        ReadHandler* requestHandler = requestList.CastOut(listEntry);
        listEntry = requestList.GetNext(listEntry);
        requestList.Remove(requestList.CastIn(requestHandler));
        requestHandler->QueueRead(m_port);
    }

    while (sendErrorBufferList.IsEmpty() == false)
    {
        listEntry = sendErrorBufferList.GetHead();
        while (listEntry != NULL)
        {
            RChannelBuffer* buffer = sendErrorBufferList.CastOut(listEntry);
            listEntry = sendErrorBufferList.GetNext(listEntry);
            sendErrorBufferList.Remove(sendErrorBufferList.CastIn(buffer));
            m_handler->ProcessBuffer(buffer);
        }

        {
            AutoCriticalSection acs(&m_baseDR);

            m_latch.TransferList(&sendErrorBufferList);
        }
    }
}

//
// Return true if currently open for reading and caller should wait to use
//
bool RChannelBufferReaderNative::EnsureOpenForRead(ReadHandler* handler)
{
    bool consumedHandler = false;
    bool queueOpen = false;

    //
    // Enter a critical section and 
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_supportsLazyOpen);

        if (m_openState == OS_NotOpened)
        {
            //
            // Either this is the first read ever queued on this channel 
            // or the throttler will call us back when the file is ready
            // to be opened
            //
            if (m_openThrottler == NULL ||
                m_openThrottler->QueueOpen(this))
            {
                queueOpen = true;
            }

            m_openState = OS_Waiting;
        }

        if (m_openState == OS_Waiting || m_drainingOpenQueue)
        {
            //
            // the open has been queued but hasn't yet completed, so
            // add this handler to a list of waiters that will get
            // sent once the file is open 
            //
            m_openWaitingList.InsertAsTail(m_openWaitingList.
                                           CastIn(handler));

            //
            // true return means the caller shouldn't do anything with
            // the handler right now since it has been queued. 
            //
            consumedHandler = true;
        }
        else
        {
            LogAssert(m_openState == OS_Opened || m_openState == OS_OpenError);
            
            // 
            // the file is successfully opened or had an open error:
            // either way, it's as open as it's going to get: fill in
            // the appropriate handles
            //
            FillInOpenedDetails(handler);
        }
    }

    if (queueOpen)
    {
        //
        // if queue is open, open the file now; queue it up so we make
        // sure it's on one of our threads. This causes
        // OpenAfterThrottle to be called on a worker thread 
        //
        RChannelOpenThrottler::Dispatch* dispatch =
            new RChannelOpenThrottler::Dispatch(this);
        m_workQueue->EnQueue(dispatch);
    }

    return consumedHandler;
}

//
// Called when all handlers are done to close up reader
// we return true if the file was open when we entered, and was
// closed during the progress of the function. This value is
// needed so that the caller can notify the throttler outside the
// lock when a file gets closed 
//
bool RChannelBufferReaderNative::FinishUsingFile()
{
    //
    // we shouldn't get to this routine if we are waiting for a
    // blocked open from the throttling mechanism 
    //
    LogAssert(m_openState != OS_Waiting);
    if (m_drainingOpenQueue)
    {
        LogAssert(m_openWaitingList.IsEmpty());
    }

    if (m_openState == OS_NotOpened)
    {
        //
        // if we never opened the file; transition straight to closed 
        //
        m_openState = OS_Stopped;
        return false;
    }
    else if (m_openState == OS_Opened)
    {
        //
        // If opened, close file and stop
        //
        EagerCloseFile();
        m_openState = OS_Stopped;
        return true;
    }
    else
    {
        //
        // If state isn't opened or not opened, it should be in error
        // ie should not have been stopped already
        //
        LogAssert(m_openState == OS_OpenError);
        return false;
    }
}

void RChannelBufferReaderNative::OpenAfterThrottle()
{
    bool openSuccess = LazyOpenFile();
    if (!openSuccess && m_openThrottler != NULL)
    {
        /* this means we tried and failed to open the file. Let the
           throttler know (outside the lock) so it can queue up
           the next one */
        m_openThrottler->NotifyFileCompleted();
    }

    bool firstTime = true;

    while (true)
    {
        HandlerList sendList;

        {
            AutoCriticalSection acs(&m_baseDR);

            LogAssert(m_supportsLazyOpen);

            if (m_openState == OS_Waiting)
            {
                LogAssert(firstTime);
                LogAssert(m_drainingOpenQueue == false);
                m_drainingOpenQueue = true;
            }

            if (m_openWaitingList.IsEmpty())
            {
                /* there's supposed to be something waiting when we
                   get opened */
                LogAssert(firstTime == false);
                LogAssert(m_drainingOpenQueue == true);
                m_drainingOpenQueue = false;

                /* this drain loop is the last thing we were waiting
                   for */
                if (m_outstandingBuffers == 0 && m_state == S_Stopping)
                {
                    BOOL bRet = ::SetEvent(m_bufferReturnEvent);
                    LogAssert(bRet != 0);
                }

                return;
            }
            else
            {
                if (firstTime == true)
                {
                    LogAssert(m_openState == OS_Waiting);

                    if (openSuccess)
                    {
                        m_openState = OS_Opened;
                    }
                    else
                    {
                        m_openState = OS_OpenError;
                    }

                    firstTime = false;
                }

                LogAssert(m_openState == OS_Opened ||
                          m_openState == OS_OpenError);

                /* copy out everything that's currently waiting */

                while (m_openWaitingList.IsEmpty() == false)
                {
                    ReadHandler* nextRequest =
                        m_openWaitingList.
                        CastOut(m_openWaitingList.RemoveHead());
                    FillInOpenedDetails(nextRequest);
                    sendList.InsertAsTail(sendList.CastIn(nextRequest));
                }
            }

            /* the openWaitingList is now empty but we're going to
               leave the lock while remaining in the
               m_drainingOpenQueue state and send off the
               handlers. Then we'll go around the loop again in case
               somebody added more handlers to the openWaitingList
               while we were doing the send. */
        }

        while (sendList.IsEmpty() == false)
        {
            ReadHandler* nextRequest =
                sendList.CastOut(sendList.RemoveHead());
            nextRequest->QueueRead(m_port);
        }
    }
}

//
// Add all buffers waiting to be sent to sending queue and return any finished buffers
// to pool. Close the file if everything is done.
//
void RChannelBufferReaderNative::DispatchBuffer(ReadHandler* handler,
                                                bool makeNewHandler,
                                                ReadHandler* fillInReadHandler)
{
    ChannelBufferList sendBufferList;
    ChannelBufferList returnBufferList;
    ReadHandler* newHandler = NULL;
    bool performedClose = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_state == S_Running)
        {
            if (m_sentLastBuffer)
            {
                //
                // If running and already send last buffer, add read handler buffer to return buffer list
                //
                LogAssert(m_reorderMap.empty());
                RChannelBuffer* buffer = handler->TransferChannelBuffer();
                returnBufferList.InsertAsTail(returnBufferList.
                                              CastIn(buffer));
                delete handler;
                handler = NULL;
            }
            else
            {
                //
                // If running and have not already sent last buffer, 
                //
                std::pair<OffsetHandlerMap::iterator, bool> retval;
                UInt64 streamOffset = handler->GetStreamOffset();
                retval = m_reorderMap.insert(std::make_pair(streamOffset,
                                                            handler));
                LogAssert(retval.second == true);
                handler = NULL;

                //
                // Foreach entry in the reorder map
                //
                OffsetHandlerMap::iterator iter;
                for (iter = m_reorderMap.begin();
                     m_sentLastBuffer == false &&
                     iter != m_reorderMap.end() &&
                         iter->first == m_nextStreamOffsetToProcess;
                     iter = m_reorderMap.erase(iter))
                {
                    //
                    // Get the read handler from the reorder map, and add it's
                    // buffer to the send buffer list
                    //
                    ReadHandler* nextHandler = iter->second;
                    RChannelBuffer* nextBuffer =
                        nextHandler->TransferChannelBuffer();

                    sendBufferList.InsertAsTail(sendBufferList.
                                                CastIn(nextBuffer));

                    RChannelBufferType t = nextBuffer->GetType();
                    if (RChannelBuffer::IsTerminationBuffer(t))
                    {
                        //
                        // If buffer is to be used for termination, it's the last one
                        //
                        m_sentLastBuffer = true;
                    }
                    else
                    {
                        //
                        // If the buffer is not used for termination, update the stream offset by
                        // the size of the buffer for next processing step
                        //
                        m_nextStreamOffsetToProcess +=
                            (UInt64)
                            nextHandler->GetBlock()->GetAvailableSize();

                        if (nextHandler->IsLastDataBuffer())
                        {
                            //
                            // If this is the last data buffer, then add a new end-of-stream buffer
                            // and mark last buffer as sent
                            //
                            UInt64 endOfStreamOffset =
                                m_nextStreamOffsetToProcess;
                            RChannelBuffer* endBuffer =
                                MakeEndOfStreamBuffer(endOfStreamOffset);
                            sendBufferList.InsertAsTail(sendBufferList.
                                                        CastIn(endBuffer));

                            m_sentLastBuffer = true;
                        }
                    }

                    delete nextHandler;
                }

                //
                // If the last buffer has been put in the send buffer list
                // (either termination or end-of-stream), take the read handler
                // and add it's buffer to the return buffer list
                //
                if (m_sentLastBuffer)
                {
                    for (iter = m_reorderMap.begin();
                         iter != m_reorderMap.end();
                         iter = m_reorderMap.erase(iter))
                    {
                        ReadHandler* nextHandler = iter->second;
                        RChannelBuffer* nextBuffer =
                            nextHandler->TransferChannelBuffer();
                        returnBufferList.InsertAsTail(returnBufferList.
                                                      CastIn(nextBuffer));
                        delete nextHandler;
                    }
                }
            }

            //
            // Update count of buffers currently waiting to be processed
            //
            m_outstandingBuffers += sendBufferList.CountLinks();
            m_outstandingBuffers += returnBufferList.CountLinks();

            //
            // Add send buffer list to send latch for processing
            //
            m_latch.AcceptList(&sendBufferList);

            LogAssert(m_outstandingHandlers > 0);
            --m_outstandingHandlers;

            //
            // If the last buffer has been send and no more handlers are using it, 
            // mark it as finished
            //
            if (m_sentLastBuffer == true && m_outstandingHandlers == 0)
            {
                performedClose = FinishUsingFile();
            }

            UInt32 buffersInFlight =
                m_outstandingBuffers + m_outstandingHandlers;
            if (makeNewHandler)
            {
                //
                // If the number of buffers used is less than prefetch count
                // and currently fetching, create another one if requested
                //
                if (m_fetching == true &&
                    buffersInFlight < m_prefetchBuffers)
                {
                    LogAssert(m_state == S_Running);

                    bool lazyOpenDone = ((m_openState == OS_Opened ||
                                          m_openState == OS_OpenError) &&
                                         !m_drainingOpenQueue);

                    newHandler = GetNextReadHandler(lazyOpenDone);
                    ++m_outstandingHandlers;
                }
            }
            else if (fillInReadHandler == NULL)
            {
                //
                // we've got all the buffers out of the stream now 
                //
                m_fetching = false;
            }

            if (fillInReadHandler != NULL)
            {
                //
                // If a new read handler was provided, increment the cound
                //
                ++m_outstandingHandlers;
            }
        }
        else
        {
            //
            // If reader is not it OK state, make sure it's stopping
            //
            LogAssert(m_state == S_Stopping);

            //
            // Transfer the read handler buffer into the return buffer list and update the count of 
            // outstanding buffers
            //
            RChannelBuffer* buffer = handler->TransferChannelBuffer();
            returnBufferList.InsertAsTail(returnBufferList.CastIn(buffer));
            delete handler;
            handler = NULL;

            m_outstandingBuffers += returnBufferList.CountLinks();

            //
            // In this case, clear up the replacement read handler
            //
            if (fillInReadHandler != NULL)
            {
                delete fillInReadHandler;
                fillInReadHandler = NULL;
            }

            LogAssert(m_outstandingHandlers > 0);
            --m_outstandingHandlers;

            //
            // If no more handlers remaining, close the file and set handler return event
            //
            if (m_outstandingHandlers == 0)
            {
                performedClose = FinishUsingFile();
                BOOL bRet = ::SetEvent(m_handlerReturnEvent);
                LogAssert(bRet != 0);
            }

            LogAssert(newHandler == NULL);
        }

        //
        // Leave critical section
        //
    }

    //
    // For each entry is the return buffer list, return it to the buffer pool
    //
    DrBListEntry* listEntry;
    listEntry = returnBufferList.GetHead();
    while (listEntry != NULL)
    {
        RChannelBuffer* buffer = returnBufferList.CastOut(listEntry);
        listEntry = returnBufferList.GetNext(listEntry);
        returnBufferList.Remove(returnBufferList.CastIn(buffer));
        ReturnBuffer(buffer);
    }

    //
    // todo: why do this more than once?
    //
    while (sendBufferList.IsEmpty() == false)
    {
        //
        // If there are any buffers in the sendBuffer list, queue them up for processing
        //
        listEntry = sendBufferList.GetHead();
        while (listEntry != NULL)
        {
            RChannelBuffer* buffer = sendBufferList.CastOut(listEntry);
            listEntry = sendBufferList.GetNext(listEntry);
            sendBufferList.Remove(sendBufferList.CastIn(buffer));
            m_handler->ProcessBuffer(buffer);
        }

        {
            //
            // Take a lock and transfer all send buffer entries to the latch
            //

            AutoCriticalSection acs(&m_baseDR);
            m_latch.TransferList(&sendBufferList);
        }
    }

    //
    // If a new read handler has been created, have it start reading
    //
    if (newHandler != NULL)
    {
        newHandler->QueueRead(m_port);
    }

    //
    // If a new read handler was supplied, have it start reading
    //
    if (fillInReadHandler != NULL)
    {
        fillInReadHandler->QueueRead(m_port);
    }

    //
    // The file was open previously, but is now closed. If we're
    // being throttled, let the throttler know so it can open the
    // next file in the queue
    //
    if (performedClose && m_openThrottler != NULL)
    {
        m_openThrottler->NotifyFileCompleted();
    }
}

//
// Stop reader. Blocks for all outstanding buffers to be processed.
//
void RChannelBufferReaderNative::Interrupt()
{
    bool mustWaitForLatch = false;
    HANDLE handlerEvent = INVALID_HANDLE_VALUE;
    ChannelBufferList returnBufferList;

    //
    // Enter a critical section and 
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        //
        // Interrupt latch if sending and see if we need to wait for it to block
        //
        mustWaitForLatch = m_latch.Interrupt();

        //
        // If any read handlers are still outstanding, get the handler return event handle
        //
        if (m_outstandingHandlers > 0)
        {
            LogAssert(m_handlerReturnEvent != INVALID_HANDLE_VALUE);
            BOOL bRet = ::ResetEvent(m_handlerReturnEvent);
            LogAssert(bRet != 0);
            bRet = ::DuplicateHandle(GetCurrentProcess(),
                                     m_handlerReturnEvent,
                                     GetCurrentProcess(),
                                     &handlerEvent,
                                     0,
                                     FALSE,
                                     DUPLICATE_SAME_ACCESS);
            LogAssert(bRet != 0);
            LogAssert(handlerEvent != INVALID_HANDLE_VALUE);
        }

        if (m_state == S_Stopped)
        {
            //
            // If this is stopped, the latch and outstanding handler handle must both 
            // be "stopped" as well
            //
            LogAssert(mustWaitForLatch == false);
            LogAssert(handlerEvent == INVALID_HANDLE_VALUE);
        }
        else
        {
            //
            // If not stopped, must be stopping
            //
            LogAssert(m_state == S_Running || m_state == S_Stopping);
            m_state = S_Stopping;
            m_fetching = false;

            //
            // For each read handler, transfer a pointer to the read buffer into a list and clean up handler
            //
            OffsetHandlerMap::iterator iter;
            for (iter = m_reorderMap.begin();
                 iter != m_reorderMap.end();
                 iter = m_reorderMap.erase(iter))
            {
                ReadHandler* nextHandler = iter->second;
                RChannelBuffer* nextBuffer =
                    nextHandler->TransferChannelBuffer();
                returnBufferList.InsertAsTail(returnBufferList.
                                              CastIn(nextBuffer));
                delete nextHandler;
            }

            //
            // Remember number of links (number of buffers - 1)
            //
            m_outstandingBuffers += returnBufferList.CountLinks();
        }
    }

    //
    // For each return buffer, start up new reader if possible
    //
    DrBListEntry* listEntry = returnBufferList.GetHead();
    while (listEntry != NULL)
    {
        RChannelBuffer* buffer = returnBufferList.CastOut(listEntry);
        listEntry = returnBufferList.GetNext(listEntry);
        returnBufferList.Remove(returnBufferList.CastIn(buffer));
        ReturnBuffer(buffer);
    }

    //
    // Wait for latch to reset if needed
    //
    if (mustWaitForLatch)
    {
        m_latch.Wait();
    }

    //
    // Wait for read handler to return if needed
    //
    if (handlerEvent != INVALID_HANDLE_VALUE)
    {
        DWORD dRet = ::WaitForSingleObject(handlerEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
        BOOL bRet = ::CloseHandle(handlerEvent);
        LogAssert(bRet != 0);
    }
}

/* this needs to be overwritten by derived classes that implement lazy
   open/eager close (i.e. files but not pipes) */
bool RChannelBufferReaderNative::LazyOpenFile()
{
    LogAssert(false);
    return true;
}

/* this is only ever called as part of the lazy open mechanism, to
   fill in details of handlers after a deferred open has
   completed. Therefore if it's not overwritten, it asserts */
void RChannelBufferReaderNative::FillInOpenedDetails(ReadHandler* handler)
{
    LogAssert(false);
}

//
// this does nothing by default: it needs to be overwritten by derived
// classes that implement lazy open/eager close (i.e. files but not
// pipes) 
//
void RChannelBufferReaderNative::EagerCloseFile()
{
}

//
// Stop reader but allow outstanding reads to finish 
//
void RChannelBufferReaderNative::DrainNativeReader()
{
    //
    // Interrupt the reader to avoid additional reads
    //
    Interrupt();

    bool mustWaitForBuffers = false;
    bool performedClose = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == S_Stopping);

        if (m_outstandingBuffers > 0 || m_drainingOpenQueue)
        {
            //
            // Need to wait for outstanding buffers to be handled
            //
            mustWaitForBuffers = true;
            BOOL bRet = ::ResetEvent(m_bufferReturnEvent);
            LogAssert(bRet != 0);
        }
        else if (m_openState != OS_Stopped)
        {
            //
            // If everything done and stopped, close file
            //
            performedClose = FinishUsingFile();
        }
    }

    if (performedClose && m_openThrottler != NULL)
    {
        //
        // the file was open previously, and is now closed. If we're
        // being throttled, let the throttler know so it can open the
        // next file in the queue 
        //
        m_openThrottler->NotifyFileCompleted();
    }

    if (mustWaitForBuffers)
    {
        //
        // If there were outstanding buffers or the queue was still draining, wait
        // for all buffers to be returned
        //
        DWORD dRet = ::WaitForSingleObject(m_bufferReturnEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    {
        AutoCriticalSection acs(&m_baseDR);

        //
        // Ensure that everything was shut down correctly and
        // clean up all additional resources (handles and latch)
        //
        LogAssert(m_outstandingHandlers == 0);
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_drainingOpenQueue == false);
        LogAssert(m_reorderMap.empty());
        m_latch.Stop();
        BOOL bRet = ::CloseHandle(m_handlerReturnEvent);
        LogAssert(bRet != 0);
        m_handlerReturnEvent = INVALID_HANDLE_VALUE;
        m_handler = NULL;

        LogAssert(m_state == S_Stopping);
        LogAssert(m_openState == OS_OpenError ||
                  m_openState == OS_Stopped);
        m_state = S_Stopped;
        m_openState = OS_Stopped;
        m_sentLastBuffer = false;
    }
}

void RChannelBufferReaderNative::SetPrefetchBufferCount(UInt32 numberOfBuffers)
{
    m_prefetchBuffers = numberOfBuffers;
}

UInt32 RChannelBufferReaderNative::GetPrefetchBufferCount()
{
    return m_prefetchBuffers;
}

//
// Make an end-of-stream buffer
//
RChannelBuffer* RChannelBufferReaderNative::
    MakeEndOfStreamBuffer(UInt64 streamOffset)
{
    RChannelItem* item =
        RChannelMarkerItem::Create(RChannelItem_EndOfStream, true);
    DryadMetaData* metaData = item->GetMetaData();
    MakeFileMetaData(metaData, streamOffset);

    RChannelBuffer* buffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_EndOfStream,
                                            item,
                                            this);
    metaData = buffer->GetMetaData();
    MakeFileMetaData(metaData, streamOffset);

    return buffer;
}

RChannelBuffer* RChannelBufferReaderNative::
    MakeErrorBuffer(UInt64 streamOffset, DrError errorCode)
{
    RChannelItem* item =
        RChannelMarkerItem::Create(RChannelItem_Abort, true);
    DryadMetaData* metaData = item->GetMetaData();
    MakeFileMetaData(metaData, streamOffset);
    metaData->AddError(errorCode);

    RChannelBuffer* errorBuffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_Abort,
                                            item,
                                            this);
    metaData = errorBuffer->GetMetaData();
    MakeFileMetaData(metaData, streamOffset);
    metaData->AddError(errorCode);

    SetErrorBuffer(errorBuffer);
    return errorBuffer;
}

RChannelBuffer* RChannelBufferReaderNative::
    MakeOpenErrorBuffer(DrError errorCode, const char* description)
{
    RChannelItem* item =
        RChannelMarkerItem::Create(RChannelItem_Abort, true);
    DryadMetaData* metaData = item->GetMetaData();
    metaData->AddErrorWithDescription(errorCode, description);

    RChannelBuffer* errorBuffer =
        RChannelBufferMarkerDefault::Create(RChannelBuffer_Abort,
                                            item,
                                            this);
    metaData = errorBuffer->GetMetaData();
    metaData->AddErrorWithDescription(errorCode, description);

    return errorBuffer;
}

//
// Create a data buffer for the reader to use
//
RChannelBuffer* RChannelBufferReaderNative::
    MakeDataBuffer(UInt64 streamOffset, DryadLockedMemoryBuffer* block)
{
    RChannelBuffer* dataBuffer =
        RChannelBufferDataDefault::Create(block,
                                          streamOffset,
                                          this);

    DryadMetaData* metaData = dataBuffer->GetMetaData();
    MakeFileMetaData(metaData, streamOffset);
    DryadMTagRef tag;
    tag.Attach(DryadMTagUInt64::Create(Prop_Dryad_BufferLength,
                                       block->GetAvailableSize()));
    metaData->Append(tag, false);

    block->IncRef();

    return dataBuffer;
}

//
// Create a new read handler if needed and do the accounting for returning a buffer
//
void RChannelBufferReaderNative::ReturnBuffer(RChannelBuffer* buffer)
{
    //
    // Decrement reference count to buffer
    //
    buffer->DecRef();

    //
    // Enter critical section and decrement number of outstanding buffers
    // If this is the last buffer and everything else done, set buffer return event
    //
    ReadHandler* newHandler = NULL;
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_state == S_Running || m_state == S_Stopping);

        //
        // Decrement outstand buffer count
        //
        LogAssert(m_outstandingBuffers > 0);
        --m_outstandingBuffers;

        if (m_outstandingBuffers == 0 &&
            m_drainingOpenQueue == false &&
            m_state == S_Stopping)
        {
            //
            // If no more outstanding buffers, done with open queue, and stopping,
            // then set buffer return event
            //
            BOOL bRet = ::SetEvent(m_bufferReturnEvent);
            LogAssert(bRet != 0);
        }
        else
        {
            //
            // If not done, enumerate buffers still being processed and create a read handle if
            // number of buffers currently working is less than number of prefetch buffers allowed
            //
            UInt32 buffersInFlight =
                m_outstandingBuffers + m_outstandingHandlers;

            if (m_fetching == true &&
                buffersInFlight < m_prefetchBuffers)
            {
                LogAssert(m_state == S_Running);

                bool lazyOpenDone = ((m_openState == OS_Opened ||
                                      m_openState == OS_OpenError) &&
                                     !m_drainingOpenQueue);

                newHandler = GetNextReadHandler(lazyOpenDone);
                ++m_outstandingHandlers;
            }
        }
    }

    //
    // If a new read handler was created, have it read from the port
    //
    if (newHandler != NULL)
    {
        newHandler->QueueRead(m_port);
    }
}


//
// Create a new file read handler and initialize buffer
//
RChannelBufferReaderNativeFile::FileReadHandler::
    FileReadHandler(HANDLE fileHandle,
                    bool detailsPresent,
                    UInt64 streamOffset,
                    UInt32 dataSize,
                    size_t dataAlignment,
                    RChannelBufferReaderNativeFile* parent) :
        RChannelBufferReaderNative::ReadHandler(streamOffset, streamOffset,
                                                dataSize, dataAlignment)
{
    m_parent = parent;
    m_fileHandle = fileHandle;
    m_detailsPresent = detailsPresent;
}

void RChannelBufferReaderNativeFile::FileReadHandler::SetFileHandle(HANDLE h)
{
    m_fileHandle = h;
    LogAssert(m_detailsPresent == false);
    m_detailsPresent = true;
}

HANDLE RChannelBufferReaderNativeFile::FileReadHandler::GetFileHandle()
{
    return m_fileHandle;
}

//
// Under normal circumstances, create a read buffer and queue the file read into it
// Also deals with completion logic (normal or error)
//
void RChannelBufferReaderNativeFile::FileReadHandler::
    QueueRead(DryadNativePort* port)
{
    if (m_detailsPresent == false)
    {
        //
        // this handler was created before the file was opened. Let's
        // see if we're the first handler to come by, and if so try to
        // open the file 
        //
        bool waitForThrottledOpen = m_parent->EnsureOpenForRead(this);
        if (waitForThrottledOpen)
        {
            //
            // If we have to wait for opening,
            // do nothing right now. This will be sent to the port
            // eventually (on another thread) when the file is finally
            // opened. 
            //
            return;
        }
        else
        {
            LogAssert(m_detailsPresent);
        }
    }

    
    if (m_fileHandle == INVALID_HANDLE_VALUE)
    {
        //
        // If file handle is invalid, then file open has failed.
        // report error and 0 bytes read
        //
        ProcessIO(DrError_EndOfStream, 0);
    }
    else
    {
        //
        // If file is valid, queue up a read
        //
        port->QueueNativeRead(GetFileHandle(), this);
    }
}

//
// Create data or error buffer and handle it depending on the provided status
//
void RChannelBufferReaderNativeFile::FileReadHandler::
    ProcessIO(DrError cse, UInt32 numBytes)
{
    bool makeNewHandler = false;

    // todo: decide if we want remove commented code
//     DrLogI(
//         "Native read completed",
//         "name = %s handle=%p, offset=%I64u, numBytes=%u, err=%s",
//         m_parent->m_fileNameA,
//         m_fileHandle, GetStreamOffset(), numBytes, DRERRORSTRING(cse));

    if (m_parent->m_fileIsPipe &&
        cse == DrErrorFromWin32(ERROR_BROKEN_PIPE))
    {
        //
        // if pipe read fails with ERROR_BROKEN_PIPE => assume pipe is closed
        //
        cse = DrError_EndOfStream;
    }

    
    if (cse == DrError_EndOfStream)
    {
        //
        // If there is an end of stream message, no bytes should be processed
        // and final buffer should be created
        //
        LogAssert(numBytes == 0);

        SetChannelBuffer(m_parent->MakeFinalBuffer(GetStreamOffset()));
    }
    else
    {
        LogAssert(m_fileHandle != INVALID_HANDLE_VALUE);

        if (cse == DrError_OK)
        {
            //
            // If the processing under normal circumstances,
            // create a channel buffer to use for IO
            //
            DryadAlignedReadBlock* block = GetBlock();
            LogAssert(numBytes > 0);
            LogAssert(numBytes <= block->GetAllocatedSize());
            block->Trim(numBytes);

            SetChannelBuffer(m_parent->
                             MakeDataBuffer(GetStreamOffset(), block));

            //
            // todo: I don't understand why this is known to be the last data buffer when numBytes != allocated block size
            //         but a new handler should be made when numBytes == allocated block size
            //         ask JC
            //
            if (numBytes == block->GetAllocatedSize())
            {
                makeNewHandler = true;
            }
            else
            {
                SignalLastDataBuffer();
            }
        }
        else
        {
            //
            // If reporting error other than end of stream, no bytes should be processed
            // and final buffer should be created
            //
            SetChannelBuffer(m_parent->
                             MakeErrorBuffer(GetStreamOffset(), cse));
        }
    }

    //
    // Cause all buffers to be read and handle completion logic
    //
    m_parent->DispatchBuffer(this, makeNewHandler, NULL);
}


RChannelBufferReaderNativeFile::
    RChannelBufferReaderNativeFile(UInt32 bufferSize,
                                   size_t bufferAlignment,
                                   UInt32 prefetchBuffers,
                                   DryadNativePort* port,
                                   WorkQueue* workQueue,
                                   RChannelOpenThrottler* openThrottler) :
        RChannelBufferReaderNative(prefetchBuffers, port,
                                   workQueue, openThrottler,
                                   true)
{
    m_bufferSize = bufferSize;
    m_bufferAlignment = bufferAlignment;
    m_nextOffsetToRequest = 0;
    m_fileHandle = INVALID_HANDLE_VALUE;
    m_fileNameA = new char[MAX_PATH];
    m_fileNameW = new wchar_t[MAX_PATH];
    m_wideFileName = false;
    m_fileIsPipe = false;
}

RChannelBufferReaderNativeFile::~RChannelBufferReaderNativeFile()
{
    LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);
    delete [] m_fileNameA;
    delete [] m_fileNameW;
}

void RChannelBufferReaderNativeFile::FillInStatus(DryadChannelDescription* status)
{
    RChannelBufferReaderNative::FillInStatus(status);
    {
        AutoCriticalSection acs(&m_baseDR);
        status->SetChannelURI(m_fileNameA);
    }
}

bool RChannelBufferReaderNativeFile::LazyOpenFile()
{
    DWORD flags = 0;

    if (m_fileIsPipe)
    {
        flags = FILE_FLAG_OVERLAPPED;
    }
    else
    {
        flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED;
    }

    HANDLE h = INVALID_HANDLE_VALUE;
    int attemptCount = 0;

    DrLogI( "Opening native file. Filename %s (%swide-char)", m_fileNameA,
        m_wideFileName ? "" : "not ");

    while (h == INVALID_HANDLE_VALUE && attemptCount < 3)
    {
        attemptCount++;

        if (m_wideFileName)
        {
            h = ::CreateFileW(m_fileNameW,
                GENERIC_READ,
                FILE_SHARE_READ,
                NULL,
                OPEN_EXISTING,
                flags,
                NULL);
        }
        else
        {
            h = ::CreateFileA(m_fileNameA,
                GENERIC_READ,
                FILE_SHARE_READ,
                NULL,
                OPEN_EXISTING,
                flags,
                NULL);
        }

        if (h == INVALID_HANDLE_VALUE)
        {
            // sleep for attemptCount * (3000 + rand value in the range 0 < x < 2000) milliseconds
            unsigned int sleepTime = 0;
            errno_t err = rand_s(&sleepTime); 
            if (err == 0)
            {
                sleepTime = (unsigned int) ((double)sleepTime /
                    (double) UINT_MAX * 2000.0);
            }
            else 
            {
                DrLogE("rand_s call failed, adding 2000 ms.");
                sleepTime = 2000;
            }
            sleepTime = attemptCount * (3000 + sleepTime);
            DrLogI("Native file open failed",
                "Filename %s (%swide-char) Retrying after %d ms", m_fileNameA,
                m_wideFileName ? "" : "not ", sleepTime);
            Sleep(sleepTime);
        }

    } 

    {
        AutoCriticalSection acs(GetBaseDR());

        if (h == INVALID_HANDLE_VALUE)
        {
            DrLogI( "Native file open failed. Filename %s (%swide-char)", m_fileNameA,
                m_wideFileName ? "" : "not ");

            DrError err = DrGetLastError();
            DrStr64 description;
            description.SetF("Can't open native file '%s' to read",
                             m_fileNameA);
            m_openErrorBuffer.Attach(MakeOpenErrorBuffer(err, description));

            return false;
        }
        else
        {
            // todo: remove comments if we're not logging this
//         DrLogI( "Native file open succeeded",
//             "Filename %s (%swide-char)", m_fileNameA,
//             m_wideFileName ? "" : "not ");

            m_fileHandle = h;

            LARGE_INTEGER fileSize;
            BOOL bRet = ::GetFileSizeEx(m_fileHandle, &fileSize);
            LogAssert(bRet != 0);

            AssociateHandleWithPort(h);
            SetTotalLength(fileSize.QuadPart);

            return true;
        }
    }
}

//
// Close file when done
//
void RChannelBufferReaderNativeFile::EagerCloseFile()
{
    LogAssert(m_fileHandle != INVALID_HANDLE_VALUE);
    LogAssert(m_openErrorBuffer == NULL);

    DrLogI( "Closing native file. Name %s (%swide-char)", m_fileNameA,
        m_wideFileName ? "" : "not ");

    BOOL bRet = ::CloseHandle(m_fileHandle);
    LogAssert(bRet != 0);
    m_fileHandle = INVALID_HANDLE_VALUE;
}

bool RChannelBufferReaderNativeFile::OpenA(const char* pathName)
{
    {
        AutoCriticalSection acs(GetBaseDR());
        DrStr128 mappedPath;

        LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_nextOffsetToRequest == 0);

        /* DRYADONLY DrNetworkToLocal(0, pathName, mappedPath); */
        HRESULT hr = ::StringCbCopyA(m_fileNameA, MAX_PATH, pathName);
        LogAssert(SUCCEEDED(hr));
        hr = ::StringCbLengthA(m_fileNameA, MAX_PATH, &m_fileNameLength);
        LogAssert(SUCCEEDED(hr));
        m_wideFileName = false;
        m_openErrorBuffer == NULL;

        if (ConcreteRChannel::IsNamedPipe(m_fileNameA))
        {
            m_fileIsPipe = true;

            // Resize the buffer and change open
            SetPrefetchBufferCount(1); // to make pipe data access sequential
            UInt32 bufferAlignment = 1024;
            UInt32 buffSize = 64*bufferAlignment;
            if (m_bufferSize > buffSize)
            {
                m_bufferSize = buffSize;
                m_bufferAlignment = bufferAlignment;
                DrLogI("Reduced input buffer size for pipe. Size now %u", m_bufferSize);
            }
        }

        OpenNativeReader();
    }

    return true;
}

/* JC
bool RChannelBufferReaderNativeFile::OpenW(const wchar_t* pathName)
{
    {
        AutoCriticalSection acs(GetBaseDR());
        DrStr128 strPathName;

        LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_nextOffsetToRequest == 0);

        // DRYADONLY DrNetworkToLocal(0, DRWSTRINGTOUTF8(pathName), strPathName)
        HRESULT hr = ::StringCbCopyW(m_fileNameW, MAX_PATH, DRUTF8TOWSTRING(strPathName));
        LogAssert(SUCCEEDED(hr));
        m_wideFileName = true;

        LogAssert(strPathName.GetString() != NULL);
        LogAssert(strPathName.GetLength() < MAX_PATH-1);
        hr = ::StringCbCopyA(m_fileNameA, MAX_PATH, strPathName.GetString());
        LogAssert(SUCCEEDED(hr));
        hr = ::StringCbLengthA(m_fileNameA, MAX_PATH, &m_fileNameLength);
        LogAssert(SUCCEEDED(hr));
        m_openErrorBuffer == NULL;

        if (ConcreteRChannel::IsNamedPipe(m_fileNameA))
        {
            m_fileIsPipe = true;

            // Resize the buffer and change open
            SetPrefetchBufferCount(1); // to make pipe data access sequential
            UInt32 bufferAlignment = 1024;
            UInt32 buffSize = 64*bufferAlignment;
            if (m_bufferSize > buffSize)
            {
                m_bufferSize = buffSize;
                m_bufferAlignment = bufferAlignment;
                DrLogI(
                    "Reduced input buffer size for pipe",
                    "Size now %u", m_bufferSize);
            }
        }

        OpenNativeReader();
    }

    return true;
}
*/

void RChannelBufferReaderNativeFile::
    Start(RChannelBufferPrefetchInfo* /*prefetchCookie*/,
          RChannelBufferReaderHandler* handler)
{
    {
        AutoCriticalSection acs(GetBaseDR());

        LogAssert(m_nextOffsetToRequest == 0);
    }

    StartNativeReader(handler);
}

//
// Drain read channel
//
void RChannelBufferReaderNativeFile::Drain(RChannelItem* drainItem)
{
    DrainNativeReader();

    {
        AutoCriticalSection acs(GetBaseDR());

        m_nextOffsetToRequest = 0;
    }
}

void RChannelBufferReaderNativeFile::Close()
{
    DrRef<RChannelBuffer> errorBuffer;

    {
        AutoCriticalSection acs(GetBaseDR());

        LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);

        errorBuffer.Attach(CloseNativeReader());
    }
}

void RChannelBufferReaderNativeFile::
    MakeFileMetaData(DryadMetaData* metaData, UInt64 streamOffset)
{
    DryadMTagRef tag;
    tag.Attach(DryadMTagString::Create(Prop_Dryad_ChannelURI,
                                       m_fileNameA));
    metaData->Append(tag, false);

    tag.Attach(DryadMTagUInt64::Create(Prop_Dryad_ChannelBufferOffset,
                                       streamOffset));
    metaData->Append(tag, false);
}

//
// Return new read handler
//
RChannelBufferReaderNative::ReadHandler*
    RChannelBufferReaderNativeFile::GetNextReadHandler(bool lazyOpenDone)
{
    RChannelBufferReaderNative::ReadHandler* handler;

    {
        AutoCriticalSection acs(GetBaseDR());

        //
        // Create a read handler 
        //
        handler = new FileReadHandler(m_fileHandle,
                                      lazyOpenDone,
                                      m_nextOffsetToRequest,
                                      m_bufferSize,
                                      m_bufferAlignment,
                                      this);
        m_nextOffsetToRequest += m_bufferSize;
    }

    return handler;
}

//
// Make an error buffer or return one if it already exists
//
RChannelBuffer* RChannelBufferReaderNativeFile::
    MakeFinalBuffer(UInt64 streamOffset)
{
    {
        AutoCriticalSection acs(GetBaseDR());

        if (m_openErrorBuffer == NULL)
        {
            return MakeEndOfStreamBuffer(streamOffset);
        }
        else
        {
            m_openErrorBuffer->IncRef();
            return m_openErrorBuffer;
        }
    }
}

void RChannelBufferReaderNativeFile::FillInOpenedDetails(ReadHandler* h)
{
    FileReadHandler* handler = dynamic_cast<FileReadHandler*>(h);

    if (m_openErrorBuffer == NULL)
    {
        LogAssert(m_fileHandle != INVALID_HANDLE_VALUE);
    }
    else
    {
        LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);
    }

    handler->SetFileHandle(m_fileHandle);
}
