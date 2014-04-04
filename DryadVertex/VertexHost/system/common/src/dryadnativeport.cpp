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

#include <dryadnativeport.h>
#include <process.h>
#include <strsafe.h>
#include <dryadstandaloneini.h>


#pragma unmanaged

//
// No cleanup required for handlerbase
//
DryadNativePort::HandlerBase::~HandlerBase()
{
}

OVERLAPPED* DryadNativePort::HandlerBase::GetOverlapped()
{
    return this;
}

//
// Create uninitialized handler
//
DryadNativePort::Handler::Handler()
{
    hEvent = 0;
    Offset = 0;
    OffsetHigh = 0;
    m_bytesToTransfer = 0;

    // todo: remove cosmos code
    //JCm_cosmosPosition.ExtentIndex = DR_INVALID_EXTENT_INDEX;
    //JCm_cosmosPosition.Offset      = DR_UNKNOWN_OFFSET;
}

//
// No cleanup required for handler
//
DryadNativePort::Handler::~Handler()
{
}

//
// Initialize handler offset and number of bytes to transfer
//
void DryadNativePort::Handler::InitializeInternal(UInt32 bytesToTransfer,
                                                  UInt64 requestOffset)
{
    hEvent = 0;
    Offset = (DWORD) (requestOffset & 0xffffffff);
    OffsetHigh = (DWORD) (requestOffset >> 32);
    m_bytesToTransfer = (SIZE_T) bytesToTransfer;

    // todo: remove cosmost code
    //JCm_cosmosPosition.ExtentIndex = 0;
    //JCm_cosmosPosition.Offset      = requestOffset;
}

//
// Return number of bytes to tranfer
//
PSIZE_T DryadNativePort::Handler::GetNumberOfBytesToTransferPtr()
{
    return &m_bytesToTransfer;
}

// todo: remove commented code
/*JC
DR_STREAM_POSITION *DryadNativePort::Handler::GetDryadPositionPtr()
{
    return &m_cosmosPosition;
}*/

//
// Constructor. Create a pool of worker thread handles for later use.
//
DryadNativePort::DryadNativePort(DWORD numWorkerThreads,
                                 DWORD numConcurrentThreads)
{
    m_state = BPS_Stopped;
    m_outstandingRequests = 0;

    m_numWorkerThreads = numWorkerThreads;
    m_numConcurrentThreads = numConcurrentThreads;
    m_completionPort = INVALID_HANDLE_VALUE;

    m_threadHandle = new HANDLE[m_numWorkerThreads];
    LogAssert(m_threadHandle != NULL);
    DWORD i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        m_threadHandle[i] = INVALID_HANDLE_VALUE;
    }

    m_writeFileHandle = INVALID_HANDLE_VALUE;
    m_writeFileEvent = INVALID_HANDLE_VALUE;
}

//
// Delete any worker thread handles
//
DryadNativePort::~DryadNativePort()
{
    LogAssert(m_completionPort == INVALID_HANDLE_VALUE);
    delete [] m_threadHandle;
}

unsigned __stdcall DryadNativePort::WriteFileThreadBase(void* arg)
{
    DryadNativePort* self = static_cast<DryadNativePort*>(arg);
    self->WriteFileThread();
    return 0;
}

void DryadNativePort::WriteFileThread()
{
    bool mustStop = false;

    do
    {
        WaitForSingleObject(m_writeFileEvent, INFINITE);

        bool mustBreak = false;
        do
        {
            DryadWriteFileList writeList;
            {
                AutoCriticalSection acs(&m_writeFileCS);

                writeList.TransitionToTail(&m_writeFileList);
                mustStop = m_writeFileFinished;

                if (writeList.IsEmpty())
                {
                    ResetEvent(m_writeFileEvent);
                    mustBreak = true;
                }
            }

            while (writeList.IsEmpty() == false)
            {
                WriteFileRequest* wfr =
                    writeList.CastOut(writeList.RemoveHead());

                DWORD bytesToTransfer =
                    (DWORD) *(wfr->m_request->GetNumberOfBytesToTransferPtr());
                BOOL bRet = ::WriteFile(wfr->m_fileHandle,
                                        wfr->m_request->GetData(),
                                        bytesToTransfer,
                                        NULL,
                                        wfr->m_request->GetOverlapped());

                if (bRet == 0)
                {
                    DWORD dErr = ::GetLastError();

                    DrError cse;
                    if (dErr == ERROR_HANDLE_EOF)
                    {
                        cse = DrError_EndOfStream;
                    }
                    else
                    {
                        cse = DrErrorFromWin32(dErr);
                    }

                    if (dErr != ERROR_IO_PENDING)
                    {
                        wfr->m_request->ProcessIO(cse, 0);

                        {
                            AutoCriticalSection acs (&m_baseCS);

                            LogAssert(m_outstandingRequests > 0);
                            --m_outstandingRequests;
                        }
                    }
                }

                delete wfr;
            }
        } while (mustBreak == false);
    } while (mustStop == false);
}

//
// Worker threads' "main"
//
unsigned __stdcall DryadNativePort::ThreadFunc(void* arg)
{
    //
    // Get port reference and validate initialization 
    //
    DryadNativePort* self = (DryadNativePort *) arg;
    LogAssert(self->m_completionPort != INVALID_HANDLE_VALUE);

    DrLogI("DryadNativePort::ThreadFunc starting thread");

    //
    // Get I/O completion events until shutdown event received
    //
    bool finished = false;
    do
    {
        DWORD numBytes;
        ULONG_PTR completionKey;
        LPOVERLAPPED overlapped;

        // todo: do we want this log?
//         DrLogD(
//             "DryadNativePort::ThreadFunc waiting for completion event");

        //
        // Attempt to dequeue a completion packet
        //
        BOOL retval = ::GetQueuedCompletionStatus(self->m_completionPort,
                                                  &numBytes,
                                                  &completionKey,
                                                  &overlapped,
                                                  INFINITE);

        // todo: do we want this log?
//         DrLogD(
//             "DryadNativePort::ThreadFunc received completion event",
//             "retval: %d", retval);

        if (completionKey == (ULONG_PTR) self)
        {
            //
            // If completionkey is the native port, finish or fail depending on return code
            //
            if (retval != 0)
            {
                //
                // If GetQueuedCompletionStatus succeeded, make sure nothing was 
                // transfered and stop waiting
                //
                LogAssert(numBytes == 0);
                LogAssert(overlapped == NULL);
                finished = true;

                DrLogI("DryadNativePort::ThreadFunc received shutdown event");
            }
            else
            {
                //
                // If GetQueuedCompletitionStatus failed, log error and fail
                //
                DWORD errCode = GetLastError();
                DrLogA("DryadNativePort::GetQueuedCompletionStatus - error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
            }
        }
        else
        {
            //
            // If completionkey is not the native port, validate results and 
            // handle any errors
            //
            LogAssert(completionKey == NULL);
            LogAssert(overlapped != NULL);
            HandlerBase* handler = (HandlerBase *) overlapped;
            DrError cse;
            if (retval != 0)
            {
                //
                // If success, then everything's ok
                //
                cse = DrError_OK;
            }
            else
            {
                //
                // If failure, set reason.
                //
                DWORD errCode = GetLastError();
                if (errCode == ERROR_HANDLE_EOF)
                {
                    cse = DrError_EndOfStream;
                    if (numBytes > 0)
                    {
                        //
                        // If end of file with bytes remaining, log unexpected
                        // completion.
                        //
                        DrLogD("Unexpected non-zero byte count on EOF. numbytes %u", (UInt32) numBytes);
                        numBytes = 0;
                    }
                }
                else
                {
                    cse = DrErrorFromWin32(errCode);
                }
            }

            // todo: do we want this log?
//             DrLogD(
//                 "Forwarding completed IO to client",
//                 "error %s; numbytes %u",
//                 DRERRORSTRING(cse), (UInt32) numBytes);

            //
            // 
            // todo: figure out which polymorphic ProcessIO this goes to
            //
            handler->ProcessIO(cse, (UInt32) numBytes);

            //
            // Enter critical section and decrement number of outstanding requests
            //
            {
                AutoCriticalSection acs(&(self->m_baseCS));
                LogAssert(self->m_outstandingRequests > 0);
                --(self->m_outstandingRequests);
            }
        }
    } while (!finished);

    //
    // Exit cleanly once shutdown event received
    //
    DrLogI("DryadNativePort::ThreadFunc exiting thread");
    return 0;
}

void DryadNativePort::AssociateHandle(HANDLE fileHandle)
{
    {
        AutoCriticalSection acs(&m_baseCS);
        LogAssert(m_state == BPS_Running);

        HANDLE completionPort = ::CreateIoCompletionPort(fileHandle,
                                                         m_completionPort,
                                                         NULL,
                                                         0);
        if (completionPort == NULL)
        {
            DrLogA("CreateIoCompletionPort failed. error: %u", GetLastError());
        }
        else
        {
            LogAssert(completionPort == m_completionPort);
        }
    }
}

//
// Begin waiting for events on a completion port
//
void DryadNativePort::Start()
{
    //
    // Enter a critical section to create the completion port and start worker threads
    //
    {
        AutoCriticalSection acs(&m_baseCS);

        //
        // Ensure that port is in initialized, but stopped state
        //
        LogAssert(m_state == BPS_Stopped);
        LogAssert(m_completionPort == INVALID_HANDLE_VALUE);
        LogAssert(m_outstandingRequests == 0);

        DrLogI("DryadNativePort::Start entered");

        //
        // Create an io completion port without associating it with a file
        // Assert that it is correctly created - this is probably safe
        // because of the lack of associated file
        //
        m_completionPort = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE,
                                                    NULL,
                                                    NULL,
                                                    m_numConcurrentThreads);
        LogAssert(m_completionPort != NULL);

        DrLogI("DryadNativePort::Start created completion port");

        //
        // Create each worker thread and have them wait for IO completion events
        //
        DWORD i;
        for (i=0; i<m_numWorkerThreads; ++i)
        {
            unsigned threadAddr;
            m_threadHandle[i] =
                (HANDLE) ::_beginthreadex(NULL,
                                          0,
                                          DryadNativePort::ThreadFunc,
                                          this,
                                          0,
                                          &threadAddr);
            LogAssert(m_threadHandle[i] != 0);
        }

        {
            AutoCriticalSection acs(&m_writeFileCS);

            m_writeFileFinished = false;
            LogAssert(m_writeFileList.IsEmpty());
        }

        m_writeFileEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);

        unsigned wfThreadAddr;
        m_writeFileHandle =
            (HANDLE) ::_beginthreadex(NULL,
                                      0,
                                      DryadNativePort::WriteFileThreadBase,
                                      this,
                                      0,
                                      &wfThreadAddr);
        LogAssert(m_writeFileHandle != 0);

        //
        // Record running state
        //
        m_state = BPS_Running;
        DrLogI("DryadNativePort::Start created threads");
    }
}

void DryadNativePort::Stop()
{
    DrLogI("DryadNativePort::Stop entered");

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == BPS_Running);
        LogAssert(m_outstandingRequests == 0);

        m_state = BPS_Stopping;
    }

    DWORD i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                                   0,
                                                   (ULONG_PTR) this,
                                                   NULL);
        if (retval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("DryadNativePort::Stop post completion status. error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }
    }

    DrLogI("DryadNativePort::Stop sent completion events");

    DWORD waitRet = ::WaitForMultipleObjects(m_numWorkerThreads,
                                             m_threadHandle,
                                             TRUE,
                                             INFINITE);
    LogAssert(/*waitRet >= WAIT_OBJECT_0 &&*/
              waitRet < (WAIT_OBJECT_0 + m_numWorkerThreads));

    DrLogI("DryadNativePort::Stop all worker threads have terminated");

    {
        AutoCriticalSection acs(&m_writeFileCS);

        m_writeFileFinished = true;
    }
    
    waitRet = ::WaitForSingleObject(m_writeFileHandle, INFINITE);
    LogAssert(waitRet == WAIT_OBJECT_0);

    {
        AutoCriticalSection acs(&m_writeFileCS);

        LogAssert(m_writeFileList.IsEmpty());
    }

    DrLogI("DryadNativePort::Stop all threads have terminated");

    {
        AutoCriticalSection acs(&m_baseCS);

        BOOL bRetval;

        for (i=0; i<m_numWorkerThreads; ++i)
        {
            bRetval = ::CloseHandle(m_threadHandle[i]);
            if (bRetval == 0)
            {
                DWORD errCode = GetLastError();
                DrLogA("DryadNativePort::Stop close thread handle - error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
            }
            m_threadHandle[i] = INVALID_HANDLE_VALUE;
        }

        bRetval = ::CloseHandle(m_writeFileHandle);
        if (bRetval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("DryadNativePort::Stop close writefile handle - error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }
        m_writeFileHandle = INVALID_HANDLE_VALUE;

        bRetval = ::CloseHandle(m_writeFileEvent);
        if (bRetval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("DryadNativePort::Stop close writefile event - error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }
        m_writeFileEvent = INVALID_HANDLE_VALUE;

        bRetval = ::CloseHandle(m_completionPort);
        if (bRetval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA(
                "DryadNativePort::Stop close completion port handle - error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }

        m_completionPort = INVALID_HANDLE_VALUE;
        m_state = BPS_Stopped;
    }

    DrLogI("DryadNativePort::Stop exiting");
}

//
// Do asynchronous readfile
//
void DryadNativePort::QueueNativeRead(HANDLE fileHandle, Handler* request)
{
    LogAssert(request != NULL);

    {
        //
        // Enter critical section and increment outstanding requests
        //
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == BPS_Running);
        ++m_outstandingRequests;
    }

    DWORD bytesToTransfer =
        (DWORD) *(request->GetNumberOfBytesToTransferPtr());

    //
    // Read requested number of bytes from a file asynchronously
    //
    BOOL bRet = ::ReadFile(fileHandle,
                           request->GetData(),
                           bytesToTransfer,
                           NULL,
                           request->GetOverlapped());

    if (bRet == 0)
    {
        DWORD dErr = ::GetLastError();

        DrError cse;
        if (dErr == ERROR_HANDLE_EOF)
        {
            //
            // If reached EOF, report EOS error
            //
            cse = DrError_EndOfStream;
        }
        else
        {
            cse = DrErrorFromWin32(dErr);
        }

        if (dErr != ERROR_IO_PENDING)
        {
            //
            // If not IO pending, asynchronous function has failed running, so process the error
            //
            request->ProcessIO(cse, 0);

            {
                AutoCriticalSection acs (&m_baseCS);

                LogAssert(m_outstandingRequests > 0);
                --m_outstandingRequests;
            }
        }
    }
}

void DryadNativePort::QueueNativeWrite(HANDLE fileHandle, Handler* request)
{
    LogAssert(request != NULL);

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == BPS_Running);
        ++m_outstandingRequests;
    }

    WriteFileRequest* wfr = new
        WriteFileRequest(fileHandle, request);
    {
        AutoCriticalSection acs(&m_writeFileCS);

        BOOL mustWake = m_writeFileList.IsEmpty();
        m_writeFileList.InsertAsTail(m_writeFileList.CastIn(wfr));
        if (mustWake)
        {
            SetEvent(m_writeFileEvent);
        }
    }

#if 0
    DWORD bytesToTransfer =
        (DWORD) *(request->GetNumberOfBytesToTransferPtr());

    BOOL bRet = ::WriteFile(fileHandle,
                            request->GetData(),
                            bytesToTransfer,
                            NULL,
                            request->GetOverlapped());

    if (bRet == 0)
    {
        DWORD dErr = ::GetLastError();

        DrError cse;
        if (dErr == ERROR_HANDLE_EOF)
        {
            cse = DrError_EndOfStream;
        }
        else
        {
            cse = DrErrorFromWin32(dErr);
        }

        if (dErr != ERROR_IO_PENDING)
        {
            request->ProcessIO(cse, 0);

            {
                AutoCriticalSection acs (&m_baseCS);

                LogAssert(m_outstandingRequests > 0);
                --m_outstandingRequests;
            }
        }
    }
#endif
}

/*JCvoid DryadNativePort::QueueDryadWrite(DRHANDLE streamHandle,
                                       DrError* pendingStatePtr,
                                       UInt64 streamOffset,
                                       Handler* request)
{
    LogAssert(request != NULL);

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == BPS_Running);
        ++m_outstandingRequests;
    }

    DR_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.cbSize = sizeof(asyncInfo);
    asyncInfo.IOCP = m_completionPort;
    asyncInfo.pOperationState = pendingStatePtr;
    asyncInfo.pOverlapped = request->GetOverlapped();

    DR_STREAM_POSITION *pAppendPosition;
    pAppendPosition = request->GetDryadPositionPtr();
    pAppendPosition->ExtentIndex = 0;
    pAppendPosition->Offset = streamOffset;
    
    DrLogI(
        "Queueing Dryad append",
        "streamhandle=%p, offset=%I64u, numBytes=%I64u",
        streamHandle, streamOffset,
        (UInt64) *(request->GetNumberOfBytesToTransferPtr()));
    DrError err = ::DrAppendStream(streamHandle,
                                   request->GetData(),
                                   (*request->GetNumberOfBytesToTransferPtr()),
                                   DR_FIXED_OFFSET_APPEND,
                                   pAppendPosition,
                                   request->GetNumberOfBytesToTransferPtr(),
                                   &asyncInfo);

    LogAssert(err != DrError_OK);

    if (err == DrErrorFromWin32( ERROR_IO_PENDING ) ) {
        err = DrError_OK;
    }

    if (err != DrError_OK)
    {
        DrLogI(
            "Dryad append failed immediately",
            "streamhandle=%p, offset=%I64u, err=%s",
            streamHandle, streamOffset, DRERRORSTRING(err));

        *pendingStatePtr = err;

        request->ProcessIO(DrError_OK, 0);

        {
            AutoCriticalSection acs (&m_baseCS);

            LogAssert(m_outstandingRequests > 0);
            --m_outstandingRequests;
        }
    }
}

void DryadNativePort::QueueDryadSetStreamProperties(const char* uri,
                                                     DrError* pendingStatePtr,
                                                     Handler* request)
{
    LogAssert(request != NULL);

    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == BPS_Running);
        ++m_outstandingRequests;
    }

    DR_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.cbSize = sizeof(asyncInfo);
    asyncInfo.IOCP = m_completionPort;
    asyncInfo.pOperationState = pendingStatePtr;
    asyncInfo.pOverlapped = request->GetOverlapped();

    PCDR_STREAM_PROPERTIES properties =
        (PCDR_STREAM_PROPERTIES) request->GetData();

    DrTimeInterval expireInterval =
        properties->ExpirePeriod * DrTimeInterval_100ns;
    DrLogI(
        "Queueing Dryad set stream properties",
        "stream=%s expirePeriod=%s",
        uri, DRTIMEINTERVALSTRING(expireInterval));

    DrError err = ::DrSetStreamProperties(uri,
                                          properties,
                                          &asyncInfo);

    LogAssert(err != DrError_OK);

    if (err == DrErrorFromWin32( ERROR_IO_PENDING ) ) {
        err = DrError_OK;
    }

    if (err != DrError_OK)
    {
        DrLogI(
            "Dryad set stream properties failed immediately",
            "stream=%s, err=%s",
            uri, DRERRORSTRING(err));

        *pendingStatePtr = err;

        request->ProcessIO(DrError_OK, 0);

        {
            AutoCriticalSection acs (&m_baseCS);

            LogAssert(m_outstandingRequests > 0);
            --m_outstandingRequests;
        }
    }
}*/

void DryadNativePort::IncrementOutstandingRequests()
{
    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_state == BPS_Running);
        ++m_outstandingRequests;
    }
}

void DryadNativePort::DecrementOutstandingRequests()
{
    {
        AutoCriticalSection acs (&m_baseCS);

        LogAssert(m_outstandingRequests > 0);
        --m_outstandingRequests;
    }
}

HANDLE DryadNativePort::GetCompletionPort()
{
    return m_completionPort;
}
