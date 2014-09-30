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
#include <channelbuffernativewriter.h>
#include <channelmemorybuffers.h>
#include <dryaderrordef.h>
#include <dryadmetadata.h>
#include <concreterchannel.h>
//YARN #include <DscNativeClient.h>

#include <aclapi.h>
#include <Sddl.h>
#include <stdint.h>

#pragma unmanaged

static const UInt64 s_fileExtendChunk = 256 * 1024 * 1024;

static const char* s_dscPartitionPrefix = "hpcdscpt://";

// This is used in the call to DscGetWritePath to make sure the
// current node isn't completely full. We don't know the initial
// size hint from the vertex code at the time we call DscGetWritePath,
// so can't use a better estimate without major code refactoring.
static const UInt64 c_estimatedInitialFileSize = 1;

//JCstatic DryadStreamPropertyUpdater g_streamUpdater;
bool RChannelBufferWriterNativeFile::s_triedToSetPrivilege = false;
bool RChannelBufferWriterNativeFile::s_setPrivilege = false;
CRITSEC RChannelBufferWriterNativeFile::s_privilegeDR;

RChannelBufferWriterNative::WriteHandler::
    WriteHandler(DryadFixedMemoryBuffer* block,
                 UInt64 streamOffset,
                 bool flushAfter,
                 RChannelBufferWriterHandler* handler,
                 RChannelBufferWriterNative* parent)
{
    m_block = block;
    if (m_block != NULL)
    {
        Size_t availableLength = m_block->GetAvailableSize();
        LogAssert(availableLength < 0x100000000);
        m_writeLength = (UInt32) availableLength;
        m_data = m_block->GetWriteAddress(0, m_writeLength, &availableLength);
        LogAssert(availableLength >= (Size_t) m_writeLength);
    }
    else
    {
        m_writeLength = 0;
        m_data = NULL;
    }

    m_streamOffset = streamOffset;
    m_flush = flushAfter;
    m_handler = handler;
    m_parent = parent;
}

RChannelBufferWriterNative::WriteHandler::~WriteHandler()
{
    if (m_block != NULL)
    {
        m_block->DecRef();
    }
}

UInt32 RChannelBufferWriterNative::WriteHandler::GetWriteLength()
{
    return m_writeLength;
}

UInt64 RChannelBufferWriterNative::WriteHandler::GetStreamOffset()
{
    return m_streamOffset;
}

DryadFixedMemoryBuffer* RChannelBufferWriterNative::WriteHandler::GetBlock()
{
    return m_block;
}

RChannelBufferWriterNative* RChannelBufferWriterNative::WriteHandler::
    GetParent()
{
    return m_parent;
}

void RChannelBufferWriterNative::WriteHandler::
    ProcessingComplete(RChannelItemType statusCode)
{
    LogAssert(m_handler != NULL);
    m_handler->ProcessWriteCompleted(statusCode);
    m_handler = NULL;
}

void* RChannelBufferWriterNative::WriteHandler::GetData()
{
    return m_data;
}

bool RChannelBufferWriterNative::WriteHandler::IsFlush()
{
    return m_flush;
}


RChannelBufferWriterNative::
    RChannelBufferWriterNative(UInt32 outstandingWritesLowWatermark,
                               UInt32 outstandingWritesHighWatermark,
                               DryadNativePort* port,
                               RChannelOpenThrottler* openThrottler,
                               bool supportsLazyOpen)
{
    m_lowWatermark = outstandingWritesLowWatermark;
    m_highWatermark = outstandingWritesHighWatermark;
    m_port = port;
    m_openThrottler = openThrottler;
    m_supportsLazyOpen = supportsLazyOpen;
    if (!m_supportsLazyOpen)
    {
        LogAssert(m_openThrottler == NULL);
    }

    LogAssert(m_highWatermark > 0);

    m_outstandingBuffers = 0;
    m_outstandingIOs = 0;
    m_outstandingHandlerSends = 0;
    m_outstandingTermination = false;
    m_blocking = false;
    m_flushing = false;
    m_terminationHandler = NULL;
    m_terminationEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_terminationEvent != NULL);

    m_processedLength = 0;
    m_initialSizeHint = 0;

    m_state = S_Closed;
    m_openState = OS_Closed;
    m_drainingOpenQueue = false;
}

RChannelBufferWriterNative::~RChannelBufferWriterNative()
{
    LogAssert(m_state == S_Closed);
    LogAssert(m_openState == OS_Closed);
    LogAssert(m_drainingOpenQueue == false);
    BOOL bRet = ::CloseHandle(m_terminationEvent);
    LogAssert(bRet != 0);
}

void RChannelBufferWriterNative::SetInitialSizeHint(UInt64 hint)
{
    m_initialSizeHint = hint;
}

UInt64 RChannelBufferWriterNative::GetInitialSizeHint()
{
    return m_initialSizeHint;
}

void RChannelBufferWriterNative::Start()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == S_Stopped);
        LogAssert(m_openState == OS_Stopped);
        LogAssert(m_drainingOpenQueue == false);
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingIOs == 0);
        LogAssert(m_outstandingHandlerSends == 0);
        LogAssert(m_outstandingTermination == false);
        LogAssert(m_blocking == false);
        LogAssert(m_flushing == false);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_pendingWriteSet.empty());
        LogAssert(m_pendingUnblockSet.empty());
        LogAssert(m_terminationHandler == NULL);

        m_openErrorItem = NULL;

        StartConcreteWriter(&m_completionItem);

        LogAssert(m_completionItem == NULL ||
                  m_completionItem->GetType() != RChannelItem_EndOfStream);

        m_processedLength = 0;

        m_state = S_Running;
        if (m_supportsLazyOpen)
        {
            m_openState = OS_NotOpened;
        }
        else
        {
            m_openState = OS_Opened;
        }

        // todo: remove comment if not logging
//         DrLogD( "Started",
//             "this=%p", this);
    }
}

CRITSEC* RChannelBufferWriterNative::GetBaseDR()
{
    return &m_baseCS;
}

DryadNativePort* RChannelBufferWriterNative::GetPort()
{
    return m_port;
}

void RChannelBufferWriterNative::SetOpenErrorItem(RChannelItem* errorItem)
{
    LogAssert(m_openErrorItem == NULL);
    m_openErrorItem = errorItem;
}

bool RChannelBufferWriterNative::OpenError()
{
    return (m_openErrorItem == NULL) ? false : true;
}

void RChannelBufferWriterNative::OpenInternal()
{
    LogAssert(m_state == S_Closed);
    LogAssert(m_openState == OS_Closed);
    LogAssert(m_drainingOpenQueue == false);
    m_state = S_Stopped;
    m_openState = OS_Stopped;
}

bool RChannelBufferWriterNative::EnsureOpenForWrite(WriteHandler* handler)
{
    bool consumedHandler = false;
    bool openFailed = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_supportsLazyOpen);

        if (m_openState == OS_NotOpened)
        {
            /* this is the first write ever queued on this channel */
            if (m_openThrottler == NULL ||
                m_openThrottler->QueueOpen(this))
            {
                /* we aren't throttling opens or it's ready to open
                   now, so open the file now */
                if (LazyOpenFile())
                {
                    m_openState = OS_Opened;
                }
                else
                {
                    m_openState = OS_OpenError;
                    openFailed = true;
                }
            }
            else
            {
                /* the throttler will call us back (outside all
                   locks) when the file gets opened. */
                m_openState = OS_Waiting;
            }
        }

        if (m_openState == OS_Waiting || m_drainingOpenQueue)
        {
            /* the open has been queued but hasn't yet completed, so
               add this handler to a list of waiters that will get
               sent once the file is open */
            m_openWaitingList.InsertAsTail(m_openWaitingList.
                                           CastIn(handler));

            /* true return means the caller shouldn't do anything with
               the handler right now since it has been queued. */
            consumedHandler = true;
        }
        else
        {
            LogAssert(m_openState == OS_Opened || m_openState == OS_OpenError);
            /* the file is successfully opened or had an open error:
               either way, it's as open as it's going to get: fill in
               the appropriate handles */
            FillInOpenedDetails(handler);
        }
    }

    if (openFailed && m_openThrottler != NULL)
    {
        /* the throttler said we could open the file, and the open
           failed, so tell it the file isn't open. Call into the
           throttler outside the lock since this may cause another
           file to be opened */
        m_openThrottler->NotifyFileCompleted();
    }

    return consumedHandler;
}

bool RChannelBufferWriterNative::FinishUsingFile()
{
    /* we shouldn't get to this routine if we are waiting for a
       blocked open from the throttling mechanism */
    LogAssert(m_openState == OS_NotOpened ||
              m_openState == OS_OpenError ||
              m_openState == OS_Opened);

    if (m_drainingOpenQueue)
    {
        LogAssert(m_openWaitingList.IsEmpty());
    }

    /* this is the value that will be returned, and it is true if the
       file was open when we entered, and was closed during the
       progress of the function. This value is needed so that the
       caller can notify the throttler outside the lock when a file
       gets closed */
    bool performedClose = (m_openState == OS_Opened);

    if (m_openState == OS_NotOpened)
    {
        /* this means nothing was written before we were asked to
           close, so we never did the lazy open */
        LogAssert(OpenError() == false);

        /* open now and close immediately. This writes a 0-length
           output otherwise people downstream that try to read the
           output will be sorely disappointed. Ignore throttling since
           it makes life so much easier. */
        if (LazyOpenFile())
        {
            m_openState = OS_Opened;
        }
        else
        {
            m_openState = OS_OpenError;
        }
    }

    if (m_openState == OS_Opened)
    {
        LogAssert(OpenError() == false);
        EagerCloseFile();
        m_openState = OS_Stopped;
    }
    else
    {
        LogAssert(m_openState == OS_OpenError);
    }

    return performedClose;
}

void RChannelBufferWriterNative::OpenAfterThrottle()
{
    bool firstTime = true;
    bool drained = false;

    while (!drained)
    {
        bool openFailure = false;
        WriteHandlerList sendList;

        {
            AutoCriticalSection acs(&m_baseCS);

            if (firstTime)
            {
                /* hold a reference to make sure the channel can't
                   finish before we exit this drain loop */
                ++m_outstandingHandlerSends;
            }

            LogAssert(m_supportsLazyOpen);
            LogAssert(m_openThrottler != NULL);

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

                drained = true;
                /* now we will exit the while loop and decrement
                   m_outstandingHandlerSends again */
            }
            else
            {
                if (firstTime == true)
                {
                    LogAssert(m_openState == OS_Waiting);

                    /* try to actually open the file now. If we try
                       and fail to open the file here, record the fact
                       so we can tell the throttler about it once we
                       exit the lock. */
                    if (LazyOpenFile())
                    {
                        m_openState = OS_Opened;
                    }
                    else
                    {
                        m_openState = OS_OpenError;
                        openFailure = true;
                    }

                    firstTime = false;
                }

                LogAssert(m_openState == OS_Opened ||
                          m_openState == OS_OpenError);

                /* copy out everything that's currently waiting */

                while (m_openWaitingList.IsEmpty() == false)
                {
                    WriteHandler* nextRequest =
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
            WriteHandler* nextRequest =
                sendList.CastOut(sendList.RemoveHead());
            nextRequest->QueueWrite(m_port);
        }

        if (openFailure)
        {
            /* this means we tried and failed to open the file. Let
               the throttler know (outside the lock) so it can queue
               up the next one */
            m_openThrottler->NotifyFileCompleted();
        }
    }

    /* return our reference on m_outstandingHandlerSends. In the event
       that a write we queued was the termination write, this may
       cause the channel to give up its last reference and close */
    DecrementOutstandingHandlers();
}

/* this needs to be overwritten by derived classes that implement lazy
   open/eager close (i.e. files but not pipes) */
bool RChannelBufferWriterNative::LazyOpenFile()
{
    LogAssert(false);
    return true;
}

//
// this is only ever called as part of the lazy open mechanism, to
// fill in details of handlers after a deferred open has
// completed. Therefore if it's not overwritten, it asserts 
//
void RChannelBufferWriterNative::FillInOpenedDetails(WriteHandler* handler)
{
    LogAssert(false);
}

/* this does nothing by default: it needs to be overwritten by derived
   classes that implement lazy open/eager close (i.e. files but not
   pipes) */
void RChannelBufferWriterNative::EagerCloseFile()
{
}

/* this does nothing by default: it needs to be overwritten by derived
   classes that don't implement lazy open/eager close (i.e. pipes) */
void RChannelBufferWriterNative::FinalCloseFile()
{
}

void RChannelBufferWriterNative::Close()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        FinalCloseFile();

        LogAssert(m_state == S_Stopped);
        LogAssert(m_openState == OS_OpenError ||
                  m_openState == OS_Stopped);
        LogAssert(m_drainingOpenQueue == false);
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingIOs == 0);
        LogAssert(m_outstandingHandlerSends == 0);
        LogAssert(m_outstandingTermination == false);
        LogAssert(m_blocking == false);
        LogAssert(m_flushing == false);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_pendingWriteSet.empty());
        LogAssert(m_pendingUnblockSet.empty());
        LogAssert(m_terminationHandler == NULL);
        m_completionItem = NULL;
        m_openErrorItem = NULL;

        m_state = S_Closed;
        m_openState = OS_Closed;

        //todo: remove comment if not logging
//         DrLogD( "Closed",
//             "this=%p", this);
    }
}

void RChannelBufferWriterNative::
    FillInStatus(DryadChannelDescription* status)
{
    {
        AutoCriticalSection acs (&m_baseCS);

        status->SetChannelTotalLength(0);
        status->SetChannelProcessedLength(m_processedLength);
    }
}

/* called with baseDR held */
void RChannelBufferWriterNative::SetProcessedLength(UInt64 processedLength)
{
    m_processedLength = processedLength;
}

DryadFixedMemoryBuffer* RChannelBufferWriterNative::GetNextWriteBuffer()
{
    DryadFixedMemoryBuffer* block;

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == S_Running);
        LogAssert(m_terminationHandler == NULL);
        LogAssert(m_outstandingTermination == false);
        ++m_outstandingBuffers;

        block = GetNextWriteBufferInternal();
    }

    return block;
}

void RChannelBufferWriterNative::
    SetLowAndHighWaterMark(UInt32 outstandingWritesLowWatermark,
                           UInt32 outstandingWritesHighWatermark)
{
    m_lowWatermark = outstandingWritesLowWatermark;
    m_highWatermark = outstandingWritesHighWatermark;
}

DryadFixedMemoryBuffer*
    RChannelBufferWriterNative::GetCustomWriteBuffer(Size_t bufferSize)
{
    DryadFixedMemoryBuffer* block;

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == S_Running);
        LogAssert(m_terminationHandler == NULL);
        LogAssert(m_outstandingTermination == false);
        ++m_outstandingBuffers;

        block = GetCustomWriteBufferInternal(bufferSize);
    }

    return block;
}

void RChannelBufferWriterNative::
    ReturnUnusedBuffer(DryadFixedMemoryBuffer* block)
{
    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == S_Running);
        LogAssert(m_outstandingBuffers > 0);
        --m_outstandingBuffers;

        ReturnUnusedBufferInternal(block);
    }
}

/* called with baseDR held */
bool RChannelBufferWriterNative::AddToWriteQueue(WriteHandler* writeRequest,
                                                 WriteHandlerList* writeQueue,
                                                 bool extendFile)
{
    bool shouldBlock = false;

    // todo: remove comment if not logging
//     DrLogD( "Adding to write queue",
//         "blocking %s flushing %s ios %u",
//         (m_blocking) ? "true" : "false", (m_flushing) ? "true" : "false",
//         m_outstandingIOs);

    if (extendFile)
    {
        m_blockingForFileExtension = true;
    }

    if (m_blocking)
    {
        m_blockedList.InsertAsTail(m_blockedList.CastIn(writeRequest));
        shouldBlock = true;
    }
    else
    {
        LogAssert(m_outstandingIOs < m_highWatermark);
        LogAssert(m_flushing == false);

        ++m_outstandingIOs;
        if (m_outstandingIOs == m_highWatermark || writeRequest->IsFlush() ||
            m_blockingForFileExtension)
        {
            //todo: remove comment if not logging
//             DrLogE( "Blocking");
            m_blocking = true;
            m_flushing = writeRequest->IsFlush();
            shouldBlock = true;
        }

        writeQueue->InsertAsTail(writeQueue->CastIn(writeRequest));

        writeRequest->IncRef();
        std::pair<WriteHandlerSet::iterator,bool> retval;
        retval = m_pendingWriteSet.insert(writeRequest);
        LogAssert(retval.second == true);
    }

    // todo: remove comment if not logging
//     DrLogD( "Added to write queue",
//         "blocking %s flushing %s shouldBlock %s ios %u",
//         (m_blocking) ? "true" : "false", (m_flushing) ? "true" : "false",
//         (shouldBlock) ? "true" : "false", m_outstandingIOs);

    return shouldBlock;
}

bool RChannelBufferWriterNative::
    WriteBuffer(DryadFixedMemoryBuffer* buffer,
                bool flushAfter,
                RChannelBufferWriterHandler* handler)
{
    LogAssert(buffer->GetAvailableSize() <= buffer->GetAllocatedSize());

    bool shouldBlock = false;
    RChannelItemType returnCode = RChannelItem_Data;
    WriteHandlerList processList;

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == S_Running);
        LogAssert(m_terminationHandler == NULL);

        if (m_completionItem == NULL)
        {
            bool lazyOpenDone = ((m_openState == OS_Opened ||
                                  m_openState == OS_OpenError) &&
                                 !m_drainingOpenQueue);
            bool extendFile = false;
            WriteHandler* writeHandler =
                MakeWriteHandler(buffer, flushAfter, handler, lazyOpenDone,
                                 &extendFile);
            if (!lazyOpenDone && extendFile)
            {
                DrLogW("Not extending file since lazyOpen not done");
                extendFile = false;
            }
            if (extendFile)
            {
                DrLogW("Considering extending file");
                /* we need to extend the valid length, but if there
                   are outstanding IOs we'll have to drain them first,
                   and that process will be initiated by passing
                   extendFile=true to AddToWriteQueue */
                if (m_outstandingIOs == 0)
                {
                    DrLogW("Extending file since no IOs in flight");
                    ExtendFileValidLength();
                    extendFile = false;
                }
            }
            shouldBlock = AddToWriteQueue(writeHandler, &processList,
                                          extendFile);
        }
        else
        {
            returnCode = m_completionItem->GetType();
            LogAssert(returnCode != RChannelItem_Data);
            buffer->DecRef();
        }

        LogAssert(m_outstandingBuffers > 0);
        --m_outstandingBuffers;
    }

    if (returnCode != RChannelItem_Data)
    {
        LogAssert(shouldBlock == false);
        LogAssert(processList.IsEmpty());
        handler->ProcessWriteCompleted(returnCode);
    }
    else
    {
        DrBListEntry* listEntry = processList.GetHead();
        while (listEntry != NULL)
        {
            WriteHandler* processHandler = processList.CastOut(listEntry);
            listEntry = processList.GetNext(listEntry);
            processList.Remove(processList.CastIn(processHandler));
            processHandler->QueueWrite(m_port);
        }
    }

    return shouldBlock;
}

void RChannelBufferWriterNative::
    WriteTermination(RChannelItemType reasonCode,
                     RChannelBufferWriterHandler* handler)
{
    bool sendImmediately = false;
    RChannelItemType statusCode = RChannelItem_Data;

    // Open channel if it was not opened before -- we can get errors here and should report them
    if (m_openState == OS_NotOpened)
    {
        /* this means nothing was written before we were asked to
           close, so we never did the lazy open */
        LogAssert(OpenError() == false);

        /* open now and close immediately. This writes a 0-length
           output otherwise people downstream that try to read the
           output will be sorely disappointed. Ignore throttling since
           it makes life so much easier. */
        if (LazyOpenFile())
        {
            m_openState = OS_Opened;
        }
        else
        {
            m_openState = OS_OpenError;
        }
    }

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == S_Running);
        LogAssert(m_terminationHandler == NULL);
        LogAssert(m_outstandingTermination == false);

        bool lazyOpenDone = ((m_openState == OS_Opened ||
                              m_openState == OS_OpenError) &&
                             !m_drainingOpenQueue);
        bool extendFile = false;
        m_terminationHandler =
            MakeWriteHandler(NULL, false, handler, lazyOpenDone, &extendFile);

        m_outstandingTermination = true;

        if (m_outstandingHandlerSends == 0 &&
            m_outstandingIOs == 0)
        {
            LogAssert(m_blockedList.IsEmpty());
            LogAssert(m_outstandingBuffers == 0);

            sendImmediately = true;

            if (m_completionItem == NULL && m_openErrorItem != NULL)
            {
                m_completionItem = m_openErrorItem;
            }

            if (m_completionItem == NULL)
            {
                statusCode = RChannelItem_EndOfStream;
            }
            else
            {
                statusCode = m_completionItem->GetType();
            }
        }
    }

    if (sendImmediately)
    {
        m_terminationHandler->ProcessingComplete(statusCode);

        bool performedClose;

        {
            AutoCriticalSection acs(&m_baseCS);

            LogAssert(m_outstandingBuffers == 0);
            LogAssert(m_blockedList.IsEmpty());
            LogAssert(m_outstandingIOs == 0);
            LogAssert(m_outstandingHandlerSends == 0);
            LogAssert(m_outstandingTermination == true);
            /* hold on to the outstanding termination until after
               we've set the event to avoid a race with the drain
               thread */

            if (m_completionItem == NULL)
            {
                LogAssert(m_openErrorItem == NULL);

                m_completionItem.Attach(RChannelMarkerItem::Create(reasonCode,
                                                                   true));
                DryadMetaData* metaData = m_completionItem->GetMetaData();

                DrError errorCode = DrError_OK;
                switch (reasonCode)
                {
                case RChannelItem_EndOfStream:
                    errorCode = DrError_EndOfStream;
                    break;

                case RChannelItem_Restart:
                    errorCode = DryadError_ChannelRestart;
                    break;

                case RChannelItem_MarshalError:
                case RChannelItem_Abort:
                    errorCode = DryadError_ChannelAbort;
                    break;

                default:
                    LogAssert(false);
                };

                metaData->AddErrorWithDescription(errorCode,
                                                  "Writer Sent Termination");
            }

            performedClose = FinishUsingFile();
        }

        if (performedClose && m_openThrottler != NULL)
        {
            /* the file was open previously, and is now closed. If
               we're being throttled, let the throttler know so it can
               open the next file in the queue */
            m_openThrottler->NotifyFileCompleted();
        }

        {
            AutoCriticalSection acs(&m_baseCS);

            BOOL bRet = ::SetEvent(m_terminationEvent);
            LogAssert(bRet != 0);

            LogAssert(m_outstandingTermination == true);
            m_outstandingTermination = false;
        }
    }
}

//
// Drain write channel
//
void RChannelBufferWriterNative::Drain(RChannelItemRef* pReturnItem)
{
    bool mustWait = false;

    //todo: remove comment if not logging
//     DrLogD( "Draining",
//         "this=%p", this);

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == S_Running);

        LogAssert(m_terminationHandler != NULL);
        if (m_outstandingIOs > 0 ||
            m_outstandingHandlerSends > 0 ||
            m_outstandingTermination)
        {
            //
            // If anything outstanding, wait for it to complete
            //
            mustWait = true;
            BOOL bRet = ::ResetEvent(m_terminationEvent);
            LogAssert(bRet != 0);
        }

        m_state = S_Stopping;
    }

    if (mustWait)
    {
        //
        // wait for all writes to complete
        //

        // todo: remove comment if not logging
//         DrLogD( "Drain: waiting",
//             "this=%p", this);
        DWORD dRet = ::WaitForSingleObject(m_terminationEvent, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

    {
        AutoCriticalSection acs(&m_baseCS);

        //
        // Verify everything is shutdown as expected
        //
        LogAssert(m_state == S_Stopping);
        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_outstandingIOs == 0);
        LogAssert(m_outstandingHandlerSends == 0);
        LogAssert(m_outstandingTermination == false);
        LogAssert(m_blocking == false);
        LogAssert(m_flushing == false);
        LogAssert(m_terminationHandler != NULL);
        LogAssert(m_blockedList.IsEmpty());
        LogAssert(m_openWaitingList.IsEmpty());
        LogAssert(m_pendingWriteSet.empty());
        LogAssert(m_pendingUnblockSet.empty());
        LogAssert(m_completionItem != NULL);

        //
        // Clean up resources
        //
        m_terminationHandler->DecRef();
        m_terminationHandler = NULL;

        *pReturnItem = m_completionItem;

        DrainConcreteWriter();

        m_state = S_Stopped;

// todo: remove comment if not logging
//         DrLogD( "Drained",
//             "this=%p", this);
    }
}

/* called with baseDR held */
void RChannelBufferWriterNative::Unblock(WriteHandlerList* returnList,
                                         WriteHandlerList* processList)
{
// todo: remove comment if not logging
//     DrLogE( "UnBlocking");
    WriteHandlerSet::iterator iter;
    for (iter = m_pendingUnblockSet.begin(); iter != m_pendingUnblockSet.end();
         iter = m_pendingUnblockSet.erase(iter))
    {
        returnList->InsertAsTail(returnList->CastIn(*iter));
    }

    if (m_blockingForFileExtension)
    {
        DrLogW("Drained IOs to extend file length");
        ExtendFileValidLength();
    }

    m_blocking = false;
    m_flushing = false;
    m_blockingForFileExtension = false;

    while (m_blocking == false && m_blockedList.IsEmpty() == false)
    {
        WriteHandler* nextRequest =
            m_blockedList.CastOut(m_blockedList.RemoveHead());
        AddToWriteQueue(nextRequest, processList, false);
    }
}

void RChannelBufferWriterNative::
    ConsumeErrorBuffer(DrError errorCode,
                       WriteHandlerList* pReturnList,
                       WriteHandlerList* pProcessList)
{
    if (m_completionItem == NULL)
    {
        if (m_openErrorItem != NULL)
        {
            m_completionItem = m_openErrorItem;
        }
        else
        {
            m_completionItem.Attach(RChannelMarkerItem::
                                    CreateErrorItem(RChannelItem_Abort,
                                                    errorCode));
        }

        pReturnList->TransitionToTail(&m_blockedList);
        Unblock(pReturnList, pProcessList);
        LogAssert(pProcessList->IsEmpty());
    }
    else
    {
        LogAssert(m_completionItem->GetType() != RChannelItem_EndOfStream);
        LogAssert(m_blocking == false);
        LogAssert(m_flushing == false);
    }

    LogAssert(m_blockedList.IsEmpty()); 
    LogAssert(m_pendingUnblockSet.empty());
}

RChannelItemType RChannelBufferWriterNative::
    ConsumeBufferCompletion(WriteHandler* writeHandler,
                            DrError errorCode,
                            WriteHandlerList* pReturnList,
                            WriteHandlerList* pProcessList)
{
    RChannelItemType statusCode;

    {
        AutoCriticalSection acs(&m_baseCS);

// todo: remove comment if not logging
//         DrLogE( "Consuming buffer",
//             "state %u sends %u ios %u blocking %s flushing %s",
//             m_state, m_outstandingHandlerSends, m_outstandingIOs,
//             (m_blocking) ? "true" : "false", (m_flushing) ? "true" : "false");

        LogAssert(m_state == S_Running || m_state == S_Stopping);
        LogAssert(m_outstandingIOs > 0);
        --m_outstandingIOs;

        size_t nRemoved = m_pendingWriteSet.erase(writeHandler);
        LogAssert(nRemoved == 1);

        if (errorCode != DrError_OK)
        {
            ConsumeErrorBuffer(errorCode, pReturnList, pProcessList);
        }

        if (m_blocking == false)
        {
            LogAssert(m_flushing == false);
            LogAssert(m_blockedList.IsEmpty());
            pReturnList->InsertAsTail(pReturnList->CastIn(writeHandler));
        }
        else
        {
            LogAssert(m_completionItem == NULL);

            std::pair<WriteHandlerSet::iterator,bool> retval;
            retval = m_pendingUnblockSet.insert(writeHandler);
            LogAssert(retval.second == true);

            bool flushAll = (m_flushing || m_blockingForFileExtension);
            if ((flushAll == false && m_outstandingIOs <= m_lowWatermark) ||
                (flushAll == true && m_outstandingIOs == 0))
            {
                Unblock(pReturnList, pProcessList);
            }
        }

        if (m_outstandingIOs == 0)
        {
            LogAssert(m_blockedList.IsEmpty());
        }

        if (m_completionItem == NULL ||
            m_completionItem->GetType() == RChannelItem_EndOfStream)
        {
            statusCode = RChannelItem_Data;
        }
        else
        {
            statusCode = m_completionItem->GetType();
        }

        ++m_outstandingHandlerSends;
    }

    return statusCode;
}

RChannelItemType RChannelBufferWriterNative::
    DealWithReturnHandlers(WriteHandler* writeHandler, DrError errorCode)
{
    RChannelItemType statusCode;
    WriteHandlerList returnList;
    WriteHandlerList processList;

    statusCode = ConsumeBufferCompletion(writeHandler, errorCode,
                                         &returnList, &processList);

    writeHandler->DecRef();
    writeHandler = NULL;

    DrBListEntry* listEntry = returnList.GetHead();
    while (listEntry != NULL)
    {
        WriteHandler* handler = returnList.CastOut(listEntry);
        listEntry = returnList.GetNext(listEntry);
        returnList.Remove(returnList.CastIn(handler));
        handler->ProcessingComplete(statusCode);
        handler->DecRef();
    }

    listEntry = processList.GetHead();
    while (listEntry != NULL)
    {
        WriteHandler* handler = processList.CastOut(listEntry);
        listEntry = processList.GetNext(listEntry);
        processList.Remove(processList.CastIn(handler));
        handler->QueueWrite(m_port);
    }

    return statusCode;
}

void RChannelBufferWriterNative::
    ReceiveBufferInternal(WriteHandler* writeHandler,
                          DrError errorCode)
{
    /* the following increments m_outstandingHandlerSends */
    DealWithReturnHandlers(writeHandler, errorCode);

    DecrementOutstandingHandlers();
}

void RChannelBufferWriterNative::DecrementOutstandingHandlers()
{
    WriteHandler* returnTermination = NULL;

    RChannelItemType statusCode = RChannelItem_Data;

    {
        AutoCriticalSection acs(&m_baseCS);

// todo: remove comment if not logging
//         DrLogD( "Decrementing outstanding",
//             "ios %u outstandinghandlers %u outstandingtermination %s",
//             m_outstandingIOs, m_outstandingHandlerSends,
//             (m_outstandingTermination) ? "true" : "false");

        LogAssert(m_outstandingHandlerSends > 0);
        --m_outstandingHandlerSends;

        if (m_outstandingIOs == 0 &&
            m_outstandingHandlerSends == 0 &&
            m_outstandingTermination == true)
        {
            LogAssert(m_outstandingBuffers == 0);
            LogAssert(m_terminationHandler != NULL);
            returnTermination = m_terminationHandler;

            if (m_completionItem == NULL)
            {
                statusCode = RChannelItem_EndOfStream;
            }
            else
            {
                statusCode = m_completionItem->GetType();
            }
        }
    }

    if (returnTermination != NULL)
    {
// todo: remove comment if not logging
//         DrLogD( "Calling processing complete",
//             "status %s",
//             DRERRORSTRING(statusCode));

        returnTermination->ProcessingComplete(statusCode);

        bool performedClose;

        {
            AutoCriticalSection acs(&m_baseCS);

            LogAssert(m_outstandingBuffers == 0);
            LogAssert(m_outstandingIOs == 0);
            LogAssert(m_outstandingHandlerSends == 0);
            LogAssert(m_blockedList.IsEmpty());
            LogAssert(m_outstandingTermination == true);
            /* hold on to the outstanding termination until after
               we've set the event to avoid a race with the drain
               thread */
            if (m_completionItem == NULL)
            {
                LogAssert(m_openErrorItem == NULL);

                m_completionItem.Attach(RChannelMarkerItem::
                                        Create(RChannelItem_EndOfStream,
                                               true));
            }

// todo: remove comment if not logging
//             DrLogD( "Calling finish using file");

            performedClose = FinishUsingFile();
        }

        if (performedClose && m_openThrottler != NULL)
        {
            /* the file was open previously, and is now closed. If
               we're being throttled, let the throttler know so it can
               open the next file in the queue */
            m_openThrottler->NotifyFileCompleted();
        }

        {
            AutoCriticalSection acs(&m_baseCS);

            //todo: remove comment if not logging
//             DrLogD( "Setting event");

            BOOL bRet = ::SetEvent(m_terminationEvent);
            LogAssert(bRet != 0);

            LogAssert(m_outstandingTermination == true);
            m_outstandingTermination = false;
        }
    }
}


RChannelBufferWriterNativeFile::
    RChannelBufferWriterNativeFile(UInt32 bufferSize,
                                   size_t bufferAlignment,
                                   UInt32 outstandingWritesLowWatermark,
                                   UInt32 outstandingWritesHighWatermark,
                                   DryadNativePort* port,
                                   RChannelOpenThrottler* openThrottler) :
        RChannelBufferWriterNative(outstandingWritesLowWatermark,
                                   outstandingWritesHighWatermark,
                                   port, openThrottler, true)
{
    m_bufferSize = bufferSize;
    m_bufferAlignment = bufferAlignment;
    LogAssert(m_bufferSize >= m_bufferAlignment);

    m_rawFileHandle = INVALID_HANDLE_VALUE;
    m_bufferedFileHandle = INVALID_HANDLE_VALUE;
    m_fileNameA = new char[MAX_PATH];
//JC    m_fileNameW = new wchar_t[MAX_PATH];
    m_fileNameLength = 0;
//JC    m_wideFileName = false;

    m_tryToCreatePath = false;

    m_nextOffsetToWrite = 0;
    m_realignmentSize = 0;
    m_fileIsPipe = false;
    m_calcFP = false;
    m_fp = 0;
    m_fpDataLength = UINT64_MAX;
}

RChannelBufferWriterNativeFile::~RChannelBufferWriterNativeFile()
{
    LogAssert(m_rawFileHandle == INVALID_HANDLE_VALUE);
    LogAssert(m_bufferedFileHandle == INVALID_HANDLE_VALUE);
    delete [] m_fileNameA;
//JC    delete [] m_fileNameW;
    if (m_calcFP)
    {
        Dryad_dupelim_fprint_close(m_fpo);
        m_calcFP = false;
    }
}

DrError RChannelBufferWriterNativeFile::SetMetaData(DryadMetaData* metaData)
{
    if (metaData == NULL)
    {
        return DrError_OK;
    }

    if (metaData->LookUpVoidTag(Prop_Dryad_TryToCreateChannelPath) != NULL)
    {
        m_tryToCreatePath = true;
    }

    UInt64 initialSize;
    if (metaData->LookUpUInt64(Prop_Dryad_InitialChannelWriteSize,
                               &initialSize) == DrError_OK)
    {
        SetInitialSizeHint(initialSize);
    }

    return DrError_OK;
}

DrError RChannelBufferWriterNativeFile::TryToCreatePathA()
{
    DrStr128 fileName(m_fileNameA);
    size_t separator = 0;

    if (fileName.StartsWith("\\\\", 2))
    {
        /* skip the machine name */
        separator = fileName.IndexOfChar('\\', 2);
        if (separator == DrStr_InvalidIndex)
        {
            return DrErrorFromWin32(ERROR_PATH_NOT_FOUND);
        }
        /* skip the share name */
        separator = fileName.IndexOfChar('\\', separator+1);
    }
    else
    {
        /* skip the drive letter */
        separator = fileName.IndexOfChar('\\', 0);
    }

    if (separator == DrStr_InvalidIndex)
    {
        /* this isn't a fully qualified path so punt */
        return DrErrorFromWin32(ERROR_PATH_NOT_FOUND);
    }

    while (separator != DrStr_InvalidIndex)
    {
        /* find the next path component */
        separator = fileName.IndexOfChar('\\', separator+1);
        if (separator != DrStr_InvalidIndex)
        {
            /* try to create this directory */
            DrStr128 pathName;
            pathName.Set(fileName, separator);
            BOOL b = ::CreateDirectoryA(pathName, NULL);
            if (!b)
            {
                DrError err = DrGetLastError();
                if (err != DrErrorFromWin32(ERROR_ALREADY_EXISTS))
                {
                    return err;
                }
            }
        }
    }

    return DrError_OK;
}

//JC
#if 0
DrError RChannelBufferWriterNativeFile::TryToCreatePathW()
{
    DrWStr128 fileName(m_fileNameW);
    size_t separator = 0;

    if (fileName.StartsWith(L"\\\\", 2))
    {
        /* skip the machine name */
        separator = fileName.IndexOfChar(L'\\', 2);
        if (separator == DrStr_InvalidIndex)
        {
            return DrErrorFromWin32(ERROR_PATH_NOT_FOUND);
        }
        /* skip the share name */
        separator = fileName.IndexOfChar(L'\\', separator+1);
    }
    else
    {
        /* skip the drive letter */
        separator = fileName.IndexOfChar(L'\\', 0);
    }

    if (separator == DrStr_InvalidIndex)
    {
        /* this isn't a fully qualified path so punt */
        return DrErrorFromWin32(ERROR_PATH_NOT_FOUND);
    }

    while (separator != DrStr_InvalidIndex)
    {
        /* find the next path component */
        separator = fileName.IndexOfChar(L'\\', separator+1);
        if (separator != DrStr_InvalidIndex)
        {
            /* try to create this directory */
            DrWStr128 pathName;
            pathName.Set(fileName, separator);
            BOOL b = ::CreateDirectoryW(pathName, NULL);
            if (!b)
            {
                DrError err = DrGetLastError();
                if (err != DrErrorFromWin32(ERROR_ALREADY_EXISTS))
                {
                    return err;
                }
            }
        }
    }

    return DrError_OK;
}
#endif

HANDLE RChannelBufferWriterNativeFile::CreateFileAndPath(DrError* pErr, SECURITY_ATTRIBUTES *sa)
{
    HANDLE h;
    DrError err = DrError_OK;

/* JC
    if (m_wideFileName)
    {
        h = ::CreateFileW(m_fileNameW,
                          GENERIC_WRITE,
                          FILE_SHARE_WRITE | FILE_SHARE_READ,
                          NULL,
                          CREATE_ALWAYS,
                          FILE_FLAG_OVERLAPPED,
                          NULL);
    }
    else
    {*/
        h = ::CreateFileA(m_fileNameA,
                          GENERIC_WRITE,
                          FILE_SHARE_WRITE | FILE_SHARE_READ,
                          sa,
                          CREATE_ALWAYS,
                          FILE_FLAG_OVERLAPPED,
                          NULL);
//JC    }

    if (h == INVALID_HANDLE_VALUE)
    {
        err = DrGetLastError();
        if (err == DrErrorFromWin32(ERROR_PATH_NOT_FOUND) && m_tryToCreatePath)
        {
/*JC            if (m_wideFileName)
            {
                err = TryToCreatePathW();
            }
            else
            {*/
                err = TryToCreatePathA();
//JC            }

            if (err == DrError_OK)
            {
/*JC                if (m_wideFileName)
                {
                    h = ::CreateFileW(m_fileNameW,
                                      GENERIC_WRITE,
                                      FILE_SHARE_WRITE | FILE_SHARE_READ,
                                      NULL,
                                      CREATE_ALWAYS,
                                      FILE_FLAG_OVERLAPPED,
                                      NULL);
                }
                else
                {*/
                    h = ::CreateFileA(m_fileNameA,
                                      GENERIC_WRITE,
                                      FILE_SHARE_WRITE | FILE_SHARE_READ,
                                      sa,
                                      CREATE_ALWAYS,
                                      FILE_FLAG_OVERLAPPED,
                                      NULL);
//JC                }

                if (h == INVALID_HANDLE_VALUE)
                {
                    err = DrGetLastError();
                }
            }
        }
    }

    *pErr = err;
    return h;
}

//
// Helper to amplify privileges
//
static 
BOOL 
SetCurrentPrivilege (
    IN LPCTSTR Privilege,      // Privilege to enable/disable
    IN OUT BOOL *bEnablePrivilege  // to enable or disable privilege
    )
/*

    If successful, *bEnablePrivlege is set to the new state.
    If NOT successful, bEnablePrivlege is invalid

    Returns:
        TRUE - success
        FALSE - failure
 */
{
    HANDLE hToken;
    TOKEN_PRIVILEGES tp;
    LUID luid;
    TOKEN_PRIVILEGES tpPrevious;
    DWORD cbPrevious = sizeof(TOKEN_PRIVILEGES);
    BOOL bSuccess;
    BOOL bEnableIt;

    bEnableIt = *bEnablePrivilege;

    if (!LookupPrivilegeValue(NULL, Privilege, &luid)) {
        return FALSE;
    }

    if(!OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_QUERY | TOKEN_ADJUST_PRIVILEGES,
            &hToken
            )) {
        return FALSE;
    }

    //
    // first pass.  get current privilege setting
    //
    tp.PrivilegeCount           = 1;
    tp.Privileges[0].Luid       = luid;
    tp.Privileges[0].Attributes = 0;

    AdjustTokenPrivileges(
            hToken,
            FALSE,
            &tp,
            sizeof(TOKEN_PRIVILEGES),
            &tpPrevious,
            &cbPrevious
            );

    bSuccess = FALSE;

    if(GetLastError() == ERROR_SUCCESS) {
        //
        // second pass.  set privilege based on previous setting
        //
        tpPrevious.PrivilegeCount     = 1;
        tpPrevious.Privileges[0].Luid = luid;

        *bEnablePrivilege = tpPrevious.Privileges[0].Attributes | (SE_PRIVILEGE_ENABLED);

        if(bEnableIt) {
            tpPrevious.Privileges[0].Attributes |= (SE_PRIVILEGE_ENABLED);
        }
        else {
            tpPrevious.Privileges[0].Attributes ^= (SE_PRIVILEGE_ENABLED &
                tpPrevious.Privileges[0].Attributes);
        }

        AdjustTokenPrivileges(
                hToken,
                FALSE,
                &tpPrevious,
                cbPrevious,
                NULL,
                NULL
                );

        if (GetLastError() == ERROR_SUCCESS) {
            bSuccess=TRUE;
        }
    }

    CloseHandle(hToken);

    return bSuccess;
}

static void SetInitialFileLength(HANDLE fp, UInt64 initialLength,
                                 bool setPrivilegeForValidLength)
{
    LARGE_INTEGER fPointer;
    fPointer.QuadPart = initialLength;
    DWORD status = SetFilePointerEx(fp, fPointer, NULL, FILE_BEGIN);
    if (status == INVALID_SET_FILE_POINTER)
    {
        DrLogW( "Failed to set initial length. Length %I64u error %s", initialLength,
            DRERRORSTRING(DrGetLastError()));
        return;
    }
    else
    {
        DrLogI( "Set initial length. Length %I64u", initialLength);
    }

    BOOL ok = SetEndOfFile(fp);
    if (!ok)
    {
        DrLogW( "Failed to set end of file. Length %I64u error %s", initialLength,
            DRERRORSTRING(DrGetLastError()));
    }
    else
    {
        DrLogI( "Set end of file. Length %I64u", initialLength);
    }

    if (setPrivilegeForValidLength)
    {
        ok = SetFileValidData(fp, initialLength);
        if (!ok)
        {
            DrLogW( "Failed to set file valid data. Length %I64u error %s", initialLength,
                DRERRORSTRING(DrGetLastError()));
        }
        else
        {
            DrLogI( "Set file valid data. Length %I64u", initialLength);
        }
    }
    else
    {
        DrLogW("Not setting initial valid length: didn't get privileges");
    }

    status = SetFilePointer(fp, 0, 0, FILE_BEGIN);
    if (status == INVALID_SET_FILE_POINTER)
    {
        /* assert here, since we really do need to start writing from
           byte zero... */
        DrLogA( "Failed to reset initial length. Length %I64u error %s", initialLength,
            DRERRORSTRING(DrGetLastError()));
    }
    else
    {
        DrLogI( "Reset file pointer");
    }
}

bool RChannelBufferWriterNativeFile::TryToSetPrivilege()
{
    {
        AutoCriticalSection acs(&s_privilegeDR);

        if (s_triedToSetPrivilege == false)
        {
            LogAssert(s_setPrivilege == false);
            s_triedToSetPrivilege = true;

            BOOL bEnabled = TRUE;
            if (SetCurrentPrivilege(SE_MANAGE_VOLUME_NAME, &bEnabled))
            {
                s_setPrivilege = true;
                DrLogI("Set SE_MANAGE_VOLUME_NAME privilege");
            }
            else
            {
                DrLogI("Failed to set SE_MANAGE_VOLUME_NAME privilege");
            }
        }

        return s_setPrivilege;
    }
}

//
// Release the security descriptor if it has been created
//
void CleanUpSecurityDescriptor(SECURITY_ATTRIBUTES* sa)
{
    if (NULL != sa)
    {
        if(NULL != sa->lpSecurityDescriptor)
        {
            HeapFree(GetProcessHeap(), HEAP_ZERO_MEMORY, sa->lpSecurityDescriptor);
            sa->lpSecurityDescriptor = NULL;
        }
    }
}

//
// Build a security descriptor for the job owner
//
DrError GenerateSecurityDescriptor(SECURITY_ATTRIBUTES *sa)
{
    PSID pHpcReplicationSID = NULL, pAdminSID = NULL, pOwnerSID = NULL, pRunAsSID = NULL;
    PACL pACL = NULL;
    PSECURITY_DESCRIPTOR pSD = NULL;
    sa->lpSecurityDescriptor = pSD;
    int accessCount = 4;
    EXPLICIT_ACCESS ea[4];
    SID_IDENTIFIER_AUTHORITY SIDAuthNT = SECURITY_NT_AUTHORITY;

    
    DrError e = DrError_Fail;

    // Prep Explicit Access Structure
    ZeroMemory(&ea, accessCount * sizeof(EXPLICIT_ACCESS));

    // Get the SID for runas user
    e = DrGetSidForUser(L"HpcReplication", &pHpcReplicationSID);
    if(e != DrError_OK)
    {
        DrLogE("Unable to get SID for HpcReplication user: %u", GetLastError());
        goto Cleanup;
    }

    // If successful, provide read access to HpcReplication user
    ea[0].grfAccessPermissions = GENERIC_READ;
    ea[0].grfAccessMode = SET_ACCESS;
    ea[0].grfInheritance= NO_INHERITANCE;
    ZeroMemory(&ea[0].Trustee, sizeof(TRUSTEE));
    ea[0].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    ea[0].Trustee.TrusteeType = TRUSTEE_IS_GROUP;
    ea[0].Trustee.ptstrName  = (LPTSTR) pHpcReplicationSID;

    // Create a SID for the administrators group.
    if(! AllocateAndInitializeSid(&SIDAuthNT, 2,
                     SECURITY_BUILTIN_DOMAIN_RID,
                     DOMAIN_ALIAS_RID_ADMINS,
                     0, 0, 0, 0, 0, 0,
                     &pAdminSID)) 
    {
        DrLogE("Unable to allocate SID for Administrators group: %u", GetLastError());
        goto Cleanup; 
    }

    // If successful, provide full control to administrators group
    ea[1].grfAccessPermissions = GENERIC_ALL;
    ea[1].grfAccessMode = SET_ACCESS;
    ea[1].grfInheritance= NO_INHERITANCE;
    ZeroMemory(&ea[1].Trustee, sizeof(TRUSTEE));
    ea[1].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    ea[1].Trustee.TrusteeType = TRUSTEE_IS_GROUP;
    ea[1].Trustee.ptstrName  = (LPTSTR) pAdminSID;

    // Get username of job runas user from the environment
    WCHAR userName[MAX_PATH+1] = {0};
    HRESULT hr = DrGetEnvironmentVariable(L"USERNAME",userName);
    if(hr != 0)
    {
        DrLogE("Unable to get job runas user name: 0x%08x", hr);
        goto Cleanup;
    }

    // Get domain of job runas user from the environment
    WCHAR domain[MAX_PATH+1] = {0};
    hr = DrGetEnvironmentVariable(L"USERDOMAIN",domain);
    if(hr != 0)
    {
        DrLogE("Unable to get job runas user domain: 0x%08x", hr);
        goto Cleanup;
    }

    // Build fully qualified user name in the form domain\username
    WCHAR domainUser[MAX_PATH] = {0};
    wsprintf(domainUser, L"%s\\%s", domain, userName);

    // Get the SID for runas user
    e = DrGetSidForUser(domainUser, &pRunAsSID);
    if(e != DrError_OK)
    {
        DrLogE("Unable to get SID for job runas user, %ls.", domainUser);
        goto Cleanup;
    }

    // If successful, give runas user full permissions
    ea[2].grfAccessPermissions = GENERIC_ALL;
    ea[2].grfAccessMode = SET_ACCESS;
    ea[2].grfInheritance= NO_INHERITANCE;
    ZeroMemory(&ea[2].Trustee, sizeof(TRUSTEE));
    ea[2].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    ea[2].Trustee.TrusteeType = TRUSTEE_IS_USER;
    ea[2].Trustee.ptstrName  = (LPTSTR) pRunAsSID;

    // Get SID of job owner from the environment
    WCHAR sidEnv[MAX_PATH+1] = {0};
    hr = DrGetEnvironmentVariable(L"CCP_OWNER_SID",sidEnv);
    if(hr != 0)
    {
        DrLogW("Unable to get job owner SID: %ld.", hr);
        accessCount = 3;
    }
    else
    {
        // Converts SID string into functional SID
        if(!ConvertStringSidToSidW(sidEnv, &pOwnerSID))
        {
            DrLogW("Unable to convert job owner SID into functional SID: %u", GetLastError());
            accessCount = 3;
        }
        else
        {
            // If successful, give full control to job owner
            ea[3].grfAccessPermissions = GENERIC_ALL;
            ea[3].grfAccessMode = SET_ACCESS;
            ea[3].grfInheritance= NO_INHERITANCE;
            ZeroMemory(&ea[3].Trustee, sizeof(TRUSTEE));
            ea[3].Trustee.TrusteeForm = TRUSTEE_IS_SID;
            ea[3].Trustee.TrusteeType = TRUSTEE_IS_USER;
            ea[3].Trustee.ptstrName  = (LPTSTR) pOwnerSID;
        }
    }
    
    // Create a new ACL that contains all the entries
    hr = SetEntriesInAcl(accessCount, ea, NULL, &pACL);
    if (ERROR_SUCCESS != hr) 
    {
        DrLogE("Unable to create new access control list: %u", GetLastError());
        goto Cleanup;
    }

    // Initialize a security descriptor.  
    pSD = (PSECURITY_DESCRIPTOR) HeapAlloc(
                       GetProcessHeap(),
                       HEAP_ZERO_MEMORY,
                       SECURITY_DESCRIPTOR_MIN_LENGTH); 
    if (NULL == pSD) 
    { 
        printf("LocalAlloc Error %u\n", GetLastError());
        goto Cleanup; 
    } 
 
    if (!InitializeSecurityDescriptor(pSD, SECURITY_DESCRIPTOR_REVISION)) 
    {  
        DrLogE("Unable to initialize a Security Descriptor: Error %u", GetLastError());
        goto Cleanup; 
    } 
 
    // Add the ACL to the security descriptor. 
    if (!SetSecurityDescriptorDacl(pSD, TRUE, pACL, FALSE)) 
    {  
        DrLogE("Unable to set the DACL in the Security Descriptor: Error %u", GetLastError());
        goto Cleanup; 
    } 

    // Initialize the security attributes structure with created security descriptor
    sa->nLength = sizeof (SECURITY_ATTRIBUTES);
    sa->lpSecurityDescriptor = pSD;
    sa->bInheritHandle = FALSE;

    DrLogI("Successfully generated security descriptor");
    // Successful if everything worked as planned
    return DrError_OK;

Cleanup:
    // Free security descriptor pointer
    sa->lpSecurityDescriptor = pSD; 
    CleanUpSecurityDescriptor(sa);

    return DrError_Fail;
}

// YARN Skip security descriptor version for now but leave the code in place in case we want it in the future
#if 0 
bool RChannelBufferWriterNativeFile::LazyOpenFile()
{
    HANDLE hBuffered;
    SECURITY_ATTRIBUTES sa;

    DrLogI( "Opening native file. Filename %s", m_fileNameA);

    /* always try to extend the length of the file even if we didn't
       get a length hint, since it helps write speeds so much */
    if (m_fileIsPipe)
    {
        m_canExtendFileLength = false;
    }
    else
    {
        m_canExtendFileLength = TryToSetPrivilege();
    }

	

    // Generate the security descriptor for output files
	// YARN - Ignore the security descriptor for now, 
	//but leave the code in place in case we want it in the future
    DrError err = GenerateSecurityDescriptor(&sa);
    if(DrError_OK == err)
    {
        // If successful, create the file
        hBuffered = CreateFileAndPath(&err, &sa);

        if (hBuffered == INVALID_HANDLE_VALUE)
        {
            // If the file cannot be opened, report failure
            DrLogI( "Buffered native file open failed. Filename %s", m_fileNameA);

            RChannelItemRef errorItem;
            DrStr64 description;
            description.SetF("Can't open buffered native file '%s' to write",
                             m_fileNameA);
            errorItem.Attach(RChannelMarkerItem::
                             CreateErrorItemWithDescription(RChannelItem_Abort,
                                                            err,
                                                            description));
            SetOpenErrorItem(errorItem);
        }
        else
        {
            LogAssert(err == DrError_OK);

            DrLogI( "Buffered native file open succeeded. Filename %s", m_fileNameA);

            HANDLE h = INVALID_HANDLE_VALUE;
            if (!m_fileIsPipe)
            {
    /*JC            if (m_wideFileName)
                {
                    h = ::CreateFileW(m_fileNameW,
                                      GENERIC_WRITE,
                                      FILE_SHARE_WRITE | FILE_SHARE_READ,
                                      NULL,
                                      OPEN_EXISTING,
                                      FILE_FLAG_NO_BUFFERING |
                                      FILE_FLAG_OVERLAPPED,
                                      NULL);
                }
                else
                {*/
                    h = ::CreateFileA(m_fileNameA,
                                      GENERIC_WRITE,
                                      FILE_SHARE_WRITE | FILE_SHARE_READ,
                                      &sa,
                                      OPEN_EXISTING,
                                      FILE_FLAG_NO_BUFFERING |
                                      FILE_FLAG_OVERLAPPED,
                                      NULL);
    //JC            }

                if (h == INVALID_HANDLE_VALUE)
                {
                    // If file cannot be opened, report failure
                    DrLogI( "Native file open failed. Filename %s", m_fileNameA);

                    RChannelItemRef errorItem;
                    DrError err = DrGetLastError();
                    DrStr64 description;
                    description.SetF("Can't open native file '%s' to write",
                                     m_fileNameA);

                    errorItem.Attach(RChannelMarkerItem::
                                     CreateErrorItemWithDescription(RChannelItem_Abort,
                                                                    err,
                                                                    description));
                    SetOpenErrorItem(errorItem);

                    BOOL bRet = ::CloseHandle(hBuffered);
                    LogAssert(bRet != 0);
                    hBuffered = INVALID_HANDLE_VALUE;
                }
                else
                {
                    DrLogI(
                        "Native file open succeeded. Filename %s, rawHandle=%u, bufferedHandle=%u",
                        m_fileNameA, 
                        h, hBuffered);
                }
            }

            if (hBuffered != INVALID_HANDLE_VALUE)
            {
                m_rawFileHandle = h;
                m_bufferedFileHandle = hBuffered;

                if (h != INVALID_HANDLE_VALUE)
                {
                    GetPort()->AssociateHandle(h);
                }
                GetPort()->AssociateHandle(hBuffered);

                m_fileLengthSet = GetInitialSizeHint();
                if (m_fileLengthSet == 0 && m_canExtendFileLength)
                {
                    DrLogW("No initial size hint: extending to 0x%I64x",
                           m_fileLengthSet);
                    m_fileLengthSet = s_fileExtendChunk;
                }
                if (m_fileLengthSet != 0)
                {
                    LogAssert(!m_fileIsPipe);

                    DrLogW("Setting initial size hint: 0x%I64x",
                           m_fileLengthSet);
                    SetInitialFileLength(h, m_fileLengthSet,
                                         m_canExtendFileLength);
                }

                // Release the security descriptor
                CleanUpSecurityDescriptor(&sa);

                return true;
            }
        }
    }
    else
    {
        // Failed to build security descriptor. Report failure
        RChannelItemRef errorItem;
        DrStr64 description;
        description.SetF("Can't create security descriptor for %s", m_fileNameA);
        errorItem.Attach(RChannelMarkerItem::CreateErrorItemWithDescription(RChannelItem_Abort, err, description));
        SetOpenErrorItem(errorItem);
    }


    // Release the security descriptor
    CleanUpSecurityDescriptor(&sa);

    // we get here if the open failed 
    LogAssert(OpenError());

    return false;
}
#endif

bool RChannelBufferWriterNativeFile::LazyOpenFile()
{
    HANDLE hBuffered;
    DrLogI( "Opening native file. Filename %s", m_fileNameA);

    /* always try to extend the length of the file even if we didn't
       get a length hint, since it helps write speeds so much */
    if (m_fileIsPipe)
    {
        m_canExtendFileLength = false;
    }
    else
    {
        m_canExtendFileLength = TryToSetPrivilege();
	}

	DrError err = DrError_OK;
	// If successful, create the file
	hBuffered = CreateFileAndPath(&err, NULL);

	if (hBuffered == INVALID_HANDLE_VALUE)
	{
		// If the file cannot be opened, report failure
		DrLogI( "Buffered native file open failed. Filename %s", m_fileNameA);

		RChannelItemRef errorItem;
		DrStr64 description;
		description.SetF("Can't open buffered native file '%s' to write",
			m_fileNameA);
		errorItem.Attach(RChannelMarkerItem::
			CreateErrorItemWithDescription(RChannelItem_Abort,
			err,
			description));
		SetOpenErrorItem(errorItem);
	}
	else
	{
		LogAssert(err == DrError_OK);

		DrLogI( "Buffered native file open succeeded. Filename %s", m_fileNameA);

		HANDLE h = INVALID_HANDLE_VALUE;
		if (!m_fileIsPipe)
		{
			/*JC            if (m_wideFileName)
			{
			h = ::CreateFileW(m_fileNameW,
			GENERIC_WRITE,
			FILE_SHARE_WRITE | FILE_SHARE_READ,
			NULL,
			OPEN_EXISTING,
			FILE_FLAG_NO_BUFFERING |
			FILE_FLAG_OVERLAPPED,
			NULL);
			}
			else
			{*/
			h = ::CreateFileA(m_fileNameA,
				GENERIC_WRITE,
				FILE_SHARE_WRITE | FILE_SHARE_READ,
				NULL,
				OPEN_EXISTING,
				FILE_FLAG_NO_BUFFERING |
				FILE_FLAG_OVERLAPPED,
				NULL);
			//JC            }

			if (h == INVALID_HANDLE_VALUE)
			{
				// If file cannot be opened, report failure
				DrLogI( "Native file open failed. Filename %s", m_fileNameA);

				RChannelItemRef errorItem;
				DrError err = DrGetLastError();
				DrStr64 description;
				description.SetF("Can't open native file '%s' to write",
					m_fileNameA);

				errorItem.Attach(RChannelMarkerItem::
					CreateErrorItemWithDescription(RChannelItem_Abort,
					err,
					description));
				SetOpenErrorItem(errorItem);

				BOOL bRet = ::CloseHandle(hBuffered);
				LogAssert(bRet != 0);
				hBuffered = INVALID_HANDLE_VALUE;
			}
			else
			{
				DrLogI(
					"Native file open succeeded. Filename %s, rawHandle=%u, bufferedHandle=%u",
					m_fileNameA, 
					h, hBuffered);
			}
		}

		if (hBuffered != INVALID_HANDLE_VALUE)
		{
			m_rawFileHandle = h;
			m_bufferedFileHandle = hBuffered;

			if (h != INVALID_HANDLE_VALUE)
			{
				GetPort()->AssociateHandle(h);
			}
			GetPort()->AssociateHandle(hBuffered);

			m_fileLengthSet = GetInitialSizeHint();
			if (m_fileLengthSet == 0 && m_canExtendFileLength)
			{
				DrLogW("No initial size hint: extending to 0x%I64x",
					m_fileLengthSet);
				m_fileLengthSet = s_fileExtendChunk;
			}
			if (m_fileLengthSet != 0)
			{
				LogAssert(!m_fileIsPipe);

				DrLogW("Setting initial size hint: 0x%I64x",
					m_fileLengthSet);
				SetInitialFileLength(h, m_fileLengthSet,
					m_canExtendFileLength);
			}

			return true;
		}
	}
	// we get here if the open failed 
	LogAssert(OpenError());

	return false;
}

void RChannelBufferWriterNativeFile::EagerCloseFile()
{
    LogAssert(m_bufferedFileHandle != INVALID_HANDLE_VALUE);

    DrLogI( "Closing native file. File %s", m_fileNameA);

    m_fpDataLength = m_nextOffsetToWrite;

    if (!m_fileIsPipe)
    {
        LARGE_INTEGER finalLength;
        finalLength.QuadPart = m_nextOffsetToWrite;
        BOOL ok = SetFilePointerEx(m_bufferedFileHandle, finalLength,
                                   NULL, FILE_BEGIN);
        if (!ok)
        {
            DrLogA(
                "Couldn't set file pointer to end. Length %I64u Error: %s",
                finalLength.QuadPart, DRERRORSTRING(DrGetLastError()));
        }
        else
        {
            DrLogI( "Set final file pointer. Length %I64u", finalLength.QuadPart);
        }

        ok = SetEndOfFile(m_bufferedFileHandle);
        if (!ok)
        {
            DrLogA( "Couldn't truncate file. Error: %s", DRERRORSTRING(DrGetLastError()));
        }
        else
        {
            DrLogI( "Truncated file");
        }
    }

    BOOL bRet;

    if (m_rawFileHandle == INVALID_HANDLE_VALUE)
    {
        LogAssert(m_fileIsPipe);
    }
    else
    {
        LogAssert(!m_fileIsPipe);
        bRet = ::CloseHandle(m_rawFileHandle);
        LogAssert(bRet != 0);
        m_rawFileHandle = INVALID_HANDLE_VALUE;
    }

    bRet = ::CloseHandle(m_bufferedFileHandle);
    LogAssert(bRet != 0);
    m_bufferedFileHandle = INVALID_HANDLE_VALUE;
}

bool RChannelBufferWriterNativeFile::OpenA(const char* pathName)
{
    {
        AutoCriticalSection acs(GetBaseDR());
        DrStr128 mappedPath;

        LogAssert(m_rawFileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_bufferedFileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_nextOffsetToWrite == 0);

        /* DRYADONLY DrNetworkToLocal(0, pathName, mappedPath); */
        HRESULT hr = ::StringCbCopyA(m_fileNameA, MAX_PATH, pathName);
        LogAssert(SUCCEEDED(hr));
//JC        m_wideFileName = false;
        hr = ::StringCbLengthA(m_fileNameA, MAX_PATH, &m_fileNameLength);
        LogAssert(SUCCEEDED(hr));

        if (ConcreteRChannel::IsNamedPipe(m_fileNameA))
        {
            UInt32 bufferAlignment = 1024;
            UInt32 buffSize = 64*bufferAlignment;
            if (m_bufferSize > buffSize)
            {
                m_bufferSize = buffSize;
                m_bufferAlignment = bufferAlignment;
                DrLogI(
                    "Reduced output buffer size for pipe. Size now %u", m_bufferSize);
            }
            SetLowAndHighWaterMark(0,1); // make the pipe writes sequential
            m_fileIsPipe = true;
        }

        OpenInternal();
    }

    return true;
}

/* JC
bool RChannelBufferWriterNativeFile::OpenW(const wchar_t* pathName)
{
    {
        AutoCriticalSection acs(GetBaseDR());
        DrStr128 strPathName;

        LogAssert(m_rawFileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_bufferedFileHandle == INVALID_HANDLE_VALUE);
        LogAssert(m_nextOffsetToWrite == 0);

        // DRYADONLY DrNetworkToLocal(0, DRWSTRINGTOUTF8(pathName), strPathName);
        HRESULT hr = ::StringCbCopyW(m_fileNameW, MAX_PATH, DRUTF8TOWSTRING(strPathName));
        LogAssert(SUCCEEDED(hr));
        m_wideFileName = true;

        LogAssert(strPathName.GetString() != NULL);
        LogAssert(strPathName.GetLength() < MAX_PATH-1);
        hr = ::StringCbCopyA(m_fileNameA, MAX_PATH, strPathName.GetString());
        LogAssert(SUCCEEDED(hr));
        hr = ::StringCbLengthA(m_fileNameA, MAX_PATH, &m_fileNameLength);
        LogAssert(SUCCEEDED(hr));

        if (ConcreteRChannel::IsNamedPipe(m_fileNameA))
        {
            UInt32 bufferAlignment = 1024;
            UInt32 buffSize = 64*bufferAlignment;
            if (m_bufferSize > buffSize)
            {
                m_bufferSize = buffSize;
                m_bufferAlignment = bufferAlignment;
                DrLogI(
                    "Reduced output buffer size for pipe",
                    "Size now %u", m_bufferSize);
            }
            SetLowAndHighWaterMark(0,1); // make the pipe writes sequential
            m_fileIsPipe = true;
        }

        OpenInternal();
    }

    return true;
}
*/

/* called with baseDR held */
void RChannelBufferWriterNativeFile::
    StartConcreteWriter(RChannelItemRef* pCompletionItem)
{
    LogAssert(m_realignmentSize == 0);
    if (*pCompletionItem != NULL &&
        (*pCompletionItem)->GetType() == RChannelItem_EndOfStream)
    {
        RChannelItem* error =
            RChannelMarkerItem::
            CreateErrorItemWithDescription(RChannelItem_Abort,
                                           DryadError_ChannelRestartError,
                                           "Can't restart channel after "
                                           "sending EOF");
        pCompletionItem->Attach(error);
    }
}

/* called with baseDR held */
void RChannelBufferWriterNativeFile::DrainConcreteWriter()
{
    m_realignmentSize = 0;
    m_nextOffsetToWrite = 0;
}

/* called with baseDR held */
DryadFixedMemoryBuffer* RChannelBufferWriterNativeFile::
    GetNextWriteBufferInternal()
{
    if (m_realignmentSize == 0)
    {
        return GetCustomWriteBufferInternal(m_bufferSize);
    }
    else
    {
        return GetCustomWriteBufferInternal(m_realignmentSize);
    }
}

/* called with baseDR held */
DryadFixedMemoryBuffer* RChannelBufferWriterNativeFile::
    GetCustomWriteBufferInternal(Size_t bufferSize)
{
    DryadFixedMemoryBuffer* buffer;

    if (m_realignmentSize == 0 && AlignmentGap(bufferSize) == 0)
    {
        buffer =
            new DryadAlignedWriteBlock(bufferSize, m_bufferAlignment);
    }
    else
    {
        LogAssert(m_realignmentSize < m_bufferSize);
        buffer = new DryadAlignedWriteBlock(bufferSize, 0);
    }

    if (bufferSize <= m_realignmentSize)
    {
        m_realignmentSize -= (UInt32) bufferSize;
    }
    else
    {
        /* figure out new realignment offset. First re-base bufferSize
           to the start of an alignment block */
        bufferSize -= m_realignmentSize;
        /* then figure out the unaligned overhang we wrote */
        UInt32 downAlignment = AlignmentGap(bufferSize);
        if (downAlignment != 0)
        {
            /* and if there is any, adjust to get back to the regular
               buffer size later */
            LogAssert(downAlignment < m_bufferSize);
            m_realignmentSize = m_bufferSize - downAlignment;
        }
    }

    return buffer;
}

/* called with baseDR held */
void RChannelBufferWriterNativeFile::
    ReturnUnusedBufferInternal(DryadFixedMemoryBuffer* block)
{
    block->DecRef();
}

void RChannelBufferWriterNativeFile::ReceiveBuffer(WriteHandler* writeHandler,
                                                   DrError errorCode)
{
    ReceiveBufferInternal(writeHandler, errorCode);
}

bool RChannelBufferWriterNativeFile::IsAligned(UInt64 offset)
{
    return ((offset & ((UInt64) m_bufferAlignment - 1)) == 0);
}

UInt32 RChannelBufferWriterNativeFile::AlignmentGap(UInt64 offset)
{
    UInt64 gap = (offset & ((UInt64) (m_bufferAlignment - 1)));
    LogAssert(gap < 0x100000000);
    return (UInt32) gap;
}

/* called with BaseDR set */
void RChannelBufferWriterNativeFile::ExtendFileValidLength()
{
    m_fileLengthSet += s_fileExtendChunk;
    SetInitialFileLength(m_rawFileHandle, m_fileLengthSet,
                         m_canExtendFileLength);
}

/* called with baseDR held */
RChannelBufferWriterNative::WriteHandler* RChannelBufferWriterNativeFile::
    MakeWriteHandler(DryadFixedMemoryBuffer* block,
                     bool flushAfter,
                     RChannelBufferWriterHandler* handler,
                     bool lazyOpenDone,
                     bool* extendFile)
{
    UInt32 writeLength;
    HANDLE handleToUse;
    bool isBuffered = false;

    if (block == NULL)
    {
        writeLength = 0;
        handleToUse = INVALID_HANDLE_VALUE;
    }
    else
    {
        Size_t availableLength = block->GetAvailableSize();
        LogAssert(availableLength < 0x100000000);
        writeLength = (UInt32) availableLength;

        if (!m_fileIsPipe &&
            IsAligned(m_nextOffsetToWrite) && IsAligned(writeLength))
        {
            handleToUse = m_rawFileHandle;
        }
        else
        {
            handleToUse = m_bufferedFileHandle;
            isBuffered = true;
        }
        if (m_calcFP)
        {
            size_t dataSize;
            void *dataAddr = block->GetDataAddress(0, &dataSize, NULL);
            //LogAssert(dataSize == availableLength);
            m_fp = Dryad_dupelim_fprint_extend (m_fpo, m_fp,
                (const unsigned char *) dataAddr, writeLength);
        }
 
    }

    FileWriteHandler* writeHandler =
        new FileWriteHandler(handleToUse,
                             lazyOpenDone,
                             isBuffered,
                             block,
                             m_nextOffsetToWrite,
                             flushAfter,
                             handler, this);

    m_nextOffsetToWrite += writeLength;
    UInt32 downAlignment = AlignmentGap(m_nextOffsetToWrite);
    if (downAlignment != 0)
    {
        LogAssert(downAlignment < m_bufferSize);
        m_realignmentSize = m_bufferSize - downAlignment;
    }

    SetProcessedLength(m_nextOffsetToWrite);

    if (!m_fileIsPipe && m_canExtendFileLength &&
        m_nextOffsetToWrite >= m_fileLengthSet)
    {
        *extendFile = true;
    }

    return writeHandler;
}

void RChannelBufferWriterNativeFile::FillInOpenedDetails(WriteHandler* h)
{
    HANDLE handleToUse;

    FileWriteHandler* handler = dynamic_cast<FileWriteHandler*>(h);

    if (handler->IsBuffered())
    {
        handleToUse = m_bufferedFileHandle;
    }
    else
    {
        handleToUse = m_rawFileHandle;
    }

    if (OpenError())
    {
        LogAssert(handleToUse == INVALID_HANDLE_VALUE);
    }
    else
    {
        LogAssert(handleToUse != INVALID_HANDLE_VALUE);
    }

    handler->SetFileHandle(handleToUse);
}

RChannelBufferWriterNativeFile::FileWriteHandler::
    FileWriteHandler(HANDLE handle,
                     bool detailsPresent,
                     bool isBuffered,
                     DryadFixedMemoryBuffer* block,
                     UInt64 streamOffset,
                     bool flushAfter,
                     RChannelBufferWriterHandler* handler,
                     RChannelBufferWriterNativeFile* parent) :
        RChannelBufferWriterNative::WriteHandler(block, streamOffset,
                                                 flushAfter,
                                                 handler, parent)
{
    m_fileHandle = handle;
    m_detailsPresent = detailsPresent;
    if (!m_detailsPresent)
    {
        LogAssert(m_fileHandle == INVALID_HANDLE_VALUE);
    }

    m_isBuffered = isBuffered;
    if (block != NULL)
    {
        Size_t availableLength = block->GetAvailableSize();
        LogAssert(availableLength < 0x100000000);
        InitializeInternal((UInt32) availableLength, streamOffset);
    }
}

void RChannelBufferWriterNativeFile::FileWriteHandler::SetFileHandle(HANDLE h)
{
    m_fileHandle = h;
    LogAssert(m_detailsPresent == false);
    m_detailsPresent = true;
}

HANDLE RChannelBufferWriterNativeFile::FileWriteHandler::GetFileHandle()
{
    return m_fileHandle;
}

bool RChannelBufferWriterNativeFile::FileWriteHandler::IsBuffered()
{
    return m_isBuffered;
}

void RChannelBufferWriterNativeFile::FileWriteHandler::
    QueueWrite(DryadNativePort* port)
{
    if (m_detailsPresent == false)
    {
        /* we get here if the write was queued before the file was
           opened. This happens in the case of lazy open. Either this
           is the first write (that will trigger the lazy open) or
           opens have been throttled and we will queue this up until
           the open gets to the front of the queue */
        LogAssert(GetFileHandle() == INVALID_HANDLE_VALUE);

        RChannelBufferWriterNative* parent = GetParent();

        bool waitForThrottledOpen = parent->EnsureOpenForWrite(this);
        if (waitForThrottledOpen)
        {
            /* do nothing right now. This will be sent to the port
               eventually (perhaps on another thread) when the file is
               finally opened. */
            return;
        }
        else
        {
            LogAssert(m_detailsPresent);
        }
    }
            
    if (GetFileHandle() == INVALID_HANDLE_VALUE)
    {
        /* we will get here if there was an open error, so just pass
           it straight back into the machinery without (obviously)
           trying to actually write anything. The writer class will
           fill in the appropriate error details. */
        ProcessIO(DrError_EndOfStream, 0);
    }
    else
    {
        // todo: remove comment if not logging
//         DrLogE( "queue native write",
//             "offset %I64u length %u", GetStreamOffset(), GetWriteLength());
        port->QueueNativeWrite(GetFileHandle(), this);
    }
}

void RChannelBufferWriterNativeFile::FileWriteHandler::
    ProcessIO(DrError errorCode, UInt32 numBytes)
{
    LogAssert(m_detailsPresent);

    if (errorCode == DrError_OK)
    {
        UInt32 requested = (UInt32) (*GetNumberOfBytesToTransferPtr());
        LogAssert(numBytes == requested);
    }
    else
    {
        LogAssert(numBytes == 0);
        DrLogE(
            "Native file write failed. handle=%u, err=%s",
            GetFileHandle(), DRERRORSTRING(errorCode));
    }

    RChannelBufferWriterNative* baseParent = GetParent();
    RChannelBufferWriterNativeFile* parent =
        (RChannelBufferWriterNativeFile *) baseParent;
    parent->ReceiveBuffer(this, errorCode);
}

#ifdef TIDYFS
RChannelBufferWriterNativeTidyFSStream::
    RChannelBufferWriterNativeTidyFSStream(UInt32 bufferSize,
                                           size_t bufferAlignment,
                                           UInt32 outstandingWritesLowWatermark,
                                           UInt32 outstandingWritesHighWatermark,
                                           DryadNativePort* port,
                                           RChannelOpenThrottler* openThrottler) :
        RChannelBufferWriterNativeFile(bufferSize, bufferAlignment,
                                       outstandingWritesLowWatermark,
                                       outstandingWritesHighWatermark,
                                       port, openThrottler)
{
    DrLogD( "RChannelBufferWriterNativeTidyFSStream");
    m_client = new MDClient();
    LogAssert(m_client != NULL);   
    m_partId = 0;

    m_fpo = Dryad_dupelim_fprint_new (0x911498ae0e66bad6, 0);
    m_fp = Dryad_dupelim_fprint_empty(m_fpo);
    m_calcFP = true;
}

RChannelBufferWriterNativeTidyFSStream::~RChannelBufferWriterNativeTidyFSStream()
{
    DrLogD( "~RChannelBufferWriterNativeTidyFSStream");
  // check if stream properly closed    
    delete m_client;
    m_client = NULL;
    m_partId = 0;
    m_hostname = NULL;
    DrLogD( "~RChannelBufferWriterNativeTidyFSStream Done");
}

DrError RChannelBufferWriterNativeTidyFSStream::OpenA(const char* streamName, DryadMetaData *metaData)
{
    
    DrError result = m_client->Initialize("rsl.ini");
    if (result != DrError_OK) 
    {
        DrLogE( 
            "Error initializing TidyFS client", "ErrorCode: %u ErrorString: %s",
            result, GetDrErrorDescription(result));

        return result;
    }

    m_client->SetKeepAlive(true);

    const char *hostname = Configuration::GetRawMachineName();
    DrLogI( "CreateTidyFSStreamWriter", "Stream: %s, Host: %s", streamName, hostname);

    FILETIME currFileTime;
    GetSystemTimeAsFileTime(&currFileTime);
    UINT64 currTime = FiletimeToUInt64(currFileTime);
    DrTimeInterval leaseInterval;
    result = metaData->LookUpTimeInterval(Prop_Dryad_StreamExpireTimeWhileClosed, &leaseInterval);
    if (result != DrError_OK) 
    {
        const char* text = metaData->GetText();
         DrLogE(
            "Can't read stream metadata"
            "%s --- Stream: %s ErrorString: %s",
            text, streamName, GetDrErrorDescription(result));

        delete [] text;
        return result;
    }

    result= m_client->CreateStream(streamName, currTime + leaseInterval, 1, &m_partId, true);
    if (result != DrError_OK) 
    {
        DrLogE( 
            "Can't create temporary TidyFS Stream", "Stream: '%s' ErrorCode: %u ErrorString: %s",
            streamName, result, GetDrErrorDescription(result));

        return result;
    }

    char path[2048]; 
    int hostLen = 1024;
    m_hostname = new char[hostLen];
    LogAssert(m_hostname != NULL);
    result = m_client->GetWritePath(path, 2048, m_hostname, hostLen, m_partId, hostname);
    if (result != DrError_OK) 
    {
        DrLogE( "Error in GetWritePath", 
            "Partition: %I64x Node: %s ErrorCode: %u ErrorString: %s",
            m_partId, hostname, result, GetDrErrorDescription(result));
        return result;
    }
    m_client->SetKeepAlive(false);

    bool ok = RChannelBufferWriterNativeFile::OpenA(path);
    if (!ok)
    {
        return DrError_Fail;
    }
    return DrError_OK;
}

void RChannelBufferWriterNativeTidyFSStream::Close()   
{
    // properly close stream
    const char *hostname = Configuration::GetRawMachineName();
    DrLogI( "Closing TidyFS Stream", "Id: %I64x Size: %I64d FP: %I64x", 
        m_partId, m_fpDataLength, m_fp);
    PartitionInfo *pi = new PartitionInfo(m_partId, m_fpDataLength, m_fp, m_hostname);    
    DrError result = m_client->AddPartitionInformation(&pi, 1); 
    if (result != DrError_OK)
    {
        DrLogE( "Error in AddPartitionInformation", 
            "Partition: %I64x Node: %s ErrorCode: %u ErrorString: %s",
            m_partId, hostname, result, GetDrErrorDescription(result));
    }
    delete m_hostname;
    m_hostname = NULL;
    delete pi;
    DrLogI( "TidyFS Writer Close Calling parent Close");
    RChannelBufferWriterNativeFile::Close();    
    
}
#endif
