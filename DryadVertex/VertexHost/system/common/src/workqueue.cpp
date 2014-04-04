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

#include "workqueue.h"
#include <process.h>
#include <strsafe.h>
//JC#include "logging.h"

#pragma unmanaged

#define DWORKQUEUE_CONTINUE   (0)
#define DWORKQUEUE_EXIT       (1)

WorkRequest::~WorkRequest()
{
}

//
// Create work queue using provided numbers of threads
//
WorkQueue::WorkQueue(DWORD numWorkerThreads,
                     DWORD numConcurrentThreads)
{
    m_state = WQS_Stopped;
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

    m_numQueuedWakeUps = 0;
}

//
// Delete each thread handle
//
WorkQueue::~WorkQueue()
{
    LogAssert(m_completionPort == INVALID_HANDLE_VALUE);
    delete [] m_threadHandle;
}

unsigned __stdcall WorkQueue::ThreadFunc(void* arg)
{
    WorkQueue* self = (WorkQueue *) arg;

    LogAssert(self->m_completionPort != INVALID_HANDLE_VALUE);

    DrLogI("WorkQueue::ThreadFunc starting thread");

    bool finished = false;

    do
    {
        DWORD numBytes;
        ULONG_PTR completionKey;
        LPOVERLAPPED overlapped;

//         DrLogD(
//             "WorkQueue::ThreadFunc waiting for completion event");

        BOOL retval = ::GetQueuedCompletionStatus(self->m_completionPort,
                                                  &numBytes,
                                                  &completionKey,
                                                  &overlapped,
                                                  INFINITE);

//         DrLogD(
//             "WorkQueue::ThreadFunc received completion event",
//             "retval: %d", retval);

        if (retval != 0)
        {
            finished = (numBytes == DWORKQUEUE_EXIT);

            if (finished)
            {
                DrLogI("WorkQueue::ThreadFunc received shutdown event");
            }
        }
        else
        {
            DWORD errCode = GetLastError();
            DrLogA("WorkQueue::GetQueuedCompletionStatus. error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }

        bool queueDrained = false;
        bool decrementedCount = false;
        do
        {
            WorkRequest* request = NULL;
            {
                AutoCriticalSection acs(&(self->m_baseCS));

                if (!decrementedCount)
                {
                    decrementedCount = true;
                    LogAssert(self->m_numQueuedWakeUps > 0);
                    --(self->m_numQueuedWakeUps);
//                     DrLogD(
//                         "WorkQueue::ThreadFunc decremented queued wakeups",
//                         "new val: %d", self->m_numQueuedWakeUps);
                }

                if (self->m_list.IsEmpty())
                {
                    queueDrained = true;
//                     DrLogD(
//                         "WorkQueue::ThreadFunc found empty work queue");
                }
                else
                {
                    request = self->m_list.CastOut(self->m_list.RemoveHead());
                    LogAssert(request != NULL);
//                     DrLogD(
//                         "WorkQueue::ThreadFunc removed work item to process");
                }
            }

            if (!queueDrained)
            {
                request->Process();
                delete request;
                request = NULL;
            }
        } while (!queueDrained);
    } while (!finished);

    DrLogI("WorkQueue::ThreadFunc exiting thread");

    return 0;
}

void WorkQueue::Start()
{
    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == WQS_Stopped);
        LogAssert(m_completionPort == INVALID_HANDLE_VALUE);

        DrLogI("WorkQueue::Start entered");

        m_completionPort = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE,
                                                    NULL,
                                                    NULL,
                                                    m_numConcurrentThreads);
        LogAssert(m_completionPort != NULL);

        DrLogI("WorkQueue::Start created completion port");

        DWORD i;
        for (i=0; i<m_numWorkerThreads; ++i)
        {
            unsigned threadAddr;
            m_threadHandle[i] =
                (HANDLE) ::_beginthreadex(NULL,
                                          0,
                                          WorkQueue::ThreadFunc,
                                          this,
                                          0,
                                          &threadAddr);
            LogAssert(m_threadHandle[i] != 0);
        }

        m_state = WQS_Running;

        DrLogI("WorkQueue::Start created threads");
    }
}

void WorkQueue::Stop()
{
    DrLogI("WorkQueue::Stop entered");

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_state == WQS_Running);

        m_state = WQS_Stopping;

        m_numQueuedWakeUps += m_numWorkerThreads;
    }

    DWORD i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                                   DWORKQUEUE_EXIT,
                                                   NULL,
                                                   NULL);
        if (retval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("WorkQueue::Stop post completion status. error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }
    }

    DrLogI("WorkQueue::Stop sent completion events");

    DWORD waitRet = ::WaitForMultipleObjects(m_numWorkerThreads,
                                             m_threadHandle,
                                             TRUE,
                                             INFINITE);
    LogAssert(/*waitRet >= WAIT_OBJECT_0 &&*/
              waitRet < (WAIT_OBJECT_0 + m_numWorkerThreads));

    DrLogI("WorkQueue::Stop all threads have terminated");

    {
        AutoCriticalSection acs(&m_baseCS);

        BOOL bRetval;

        LogAssert(m_numQueuedWakeUps == 0);
        LogAssert(m_list.IsEmpty());

        for (i=0; i<m_numWorkerThreads; ++i)
        {
            bRetval = ::CloseHandle(m_threadHandle[i]);
            if (bRetval == 0)
            {
                DWORD errCode = GetLastError();
                DrLogA("WorkQueue::Stop close thread handle. error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
            }
            m_threadHandle[i] = INVALID_HANDLE_VALUE;
        }

        bRetval = ::CloseHandle(m_completionPort);
        if (bRetval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("WorkQueue::Stop close completion port handle. error code: 0x%08x", HRESULT_FROM_WIN32(errCode));
        }

        m_completionPort = INVALID_HANDLE_VALUE;
        m_state = WQS_Stopped;
    }

    DrLogI("WorkQueue::Stop exiting");
}

//
// Put a work item in the queue
//
bool WorkQueue::EnQueue(WorkRequest* item)
{
    LogAssert(item != NULL);

    //
    // Enter a critical section to add work item to queue and notify any waiting worker threads
    //
    {
        AutoCriticalSection acs (&m_baseCS);

        if (m_state == WQS_Stopping)
        {
            //
            // If stopping, log rejection
            //
            DrLogI("WorkQueue::EnQueue rejecting stopping item");
            return false;
        }
        else
        {
            LogAssert(m_state == WQS_Running);
        }

        //
        // If item shouldn't be aborted, add it to list of work and make sure
        // worker threads are awake
        //
        if (!item->ShouldAbort())
        {
            m_list.InsertAsTail(m_list.CastIn(item));
            item = NULL;

            if (m_numQueuedWakeUps < m_numWorkerThreads)
            {
                //
                // If additional worker threads are availble, post queued work
                //
                ++m_numQueuedWakeUps;
                BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                                           DWORKQUEUE_CONTINUE,
                                                           NULL,
                                                           NULL);
                if (retval == 0)
                {
                    //
                    // Log any failure posting queued work item
                    //
                    DWORD errCode = GetLastError();
                    DrLogA("WorkQueue::EnQueue post completion status. error code:0x%08x", HRESULT_FROM_WIN32(errCode));
                }
            }
        }
    }

    //
    // If item is non-null, then ShouldAbort returned true above.
    // In this case, log, abort, and clean up
    //
    if (item != NULL)
    {
        DrLogD("WorkQueue::EnQueue processing aborting work item");

        item->Process();
        delete item;
    }

    return true;
}

void WorkQueue::Clean()
{
    WorkRequestList cleanedList;
    DrBListEntry* listEntry;

    {
        AutoCriticalSection acs (&m_baseCS);

        listEntry = m_list.GetHead();
        while (listEntry != NULL)
        {
            WorkRequest* request = m_list.CastOut(listEntry);
            listEntry = m_list.GetNext(listEntry);

            if (request->ShouldAbort())
            {
                DrLogD("WorkQueue::Clean removing work item from list");
                cleanedList.TransitionToTail(cleanedList.CastIn(request));
            }
        }
    }

    listEntry = cleanedList.GetHead();
    while (listEntry != NULL)
    {
        WorkRequest* request = cleanedList.CastOut(listEntry);
        listEntry = cleanedList.GetNext(listEntry);

        DrLogD("WorkQueue::Clean processing removed work item");

        request->Process();
        cleanedList.Remove(cleanedList.CastIn(request));
        delete request;
    }
    LogAssert(cleanedList.IsEmpty());
}
