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

#include <DrKernel.h>

#define DRMESSAGEPUMP_CONTINUE   (0)
#define DRMESSAGEPUMP_EXIT       (1)

DrMessageBase::~DrMessageBase()
{
}

DRCLASS(DrMessageDummy) : public DrMessageBase
{
public:
    virtual void Deliver() DROVERRIDE
    {
        DrLogA("Dummy message asked to deliver");
    }

    virtual DrCritSecPtr GetBaseLock() DROVERRIDE
    {
        DrLogA("Dummy message lock requested");
        return DrNull;
    }
};


DrOverlapped::~DrOverlapped()
{
}

HRESULT* DrOverlapped::GetOperationStatePtr()
{
    return &m_operationState;
}


DrMessagePump::DrMessagePump(int numWorkerThreads,
                             int numConcurrentThreads)
{
    m_state = MPS_Stopped;
    m_numWorkerThreads = numWorkerThreads;
    m_numConcurrentThreads = numConcurrentThreads;
    m_completionPort = INVALID_HANDLE_VALUE;
    m_threadHandle = DrNew ThreadArray(m_numWorkerThreads+1);
#ifndef _MANAGED
    ThreadArrayR thArray = *m_threadHandle;
    int i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        thArray[i] = INVALID_HANDLE_VALUE;
    }
#endif

    m_currentListener = DrNew CSArray(m_numWorkerThreads);

    m_numQueuedWakeUps = 0;
    m_listDummy = DrNew DrMessageDummy();
    m_listDummy->m_nextMessage = m_listDummy;
    m_listDummy->m_prevMessage = m_listDummy;
    m_listLength = 0;

    m_pendingMessages = DrNew MessageQueue();
    m_submittedOverlapped = DrNew OverlappedSet();
}

DrMessagePump::~DrMessagePump()
{
    DrAssert(m_completionPort == INVALID_HANDLE_VALUE);
    /* free the circular references */
    m_listDummy->m_nextMessage = DrNull;
    m_listDummy->m_prevMessage = DrNull;
}

static DrDateTime GetSystemTimeStamp()
{
    union {
        FILETIME    ft;
        DrDateTime  ts;
    };
    GetSystemTimeAsFileTime(&ft);
    return ts;
}

DrDateTime DrMessagePump::GetCurrentTimeStamp()
{
    return GetSystemTimeStamp();
}

void DrMessagePump::AddToListTail(DrMessageBasePtr message)
{
    DrAssert(message->m_nextMessage == DrNull);
    DrAssert(message->m_prevMessage == DrNull);

    message->m_nextMessage = m_listDummy;
    message->m_prevMessage = m_listDummy->m_prevMessage;
    message->m_prevMessage->m_nextMessage = message;
    message->m_nextMessage->m_prevMessage = message;

    ++m_listLength;
}

void DrMessagePump::RemoveFromList(DrMessageBasePtr message)
{
    DrAssert(message != m_listDummy);
    DrAssert(m_listLength > 0);

    --m_listLength;

    message->m_nextMessage->m_prevMessage = message->m_prevMessage;
    message->m_prevMessage->m_nextMessage = message->m_nextMessage;

    message->m_nextMessage = DrNull;
    message->m_prevMessage = DrNull;
}

bool DrMessagePump::ListEmpty()
{
    if (m_listDummy->m_nextMessage == m_listDummy)
    {
        DrAssert(m_listDummy->m_prevMessage == m_listDummy);
        DrAssert(m_listLength == 0);
        return true;
    }
    else
    {
        DrAssert(m_listDummy->m_prevMessage != m_listDummy);
        DrAssert(m_listLength > 0);
        return false;
    }
}

void DrMessagePump::TimerThread()
{
    DrLogI("starting timer thread");

    bool finished = false;

    do
    {
        Sleep(1000);

        {
            DrAutoCriticalSection acs(this);

            DrDateTime currentTime = GetCurrentTimeStamp();

            if (m_state == MPS_Running)
            {
                MessageQueue::Iter iter = m_pendingMessages->Begin();
                while (iter != m_pendingMessages->End() && iter->first <= currentTime)
                {
                    EnQueueInternal(iter->second);
                    iter = m_pendingMessages->Erase(iter);
                }
            }
            else
            {
                finished = true;
            }
        }
    } while (!finished);

    DrLogI("exiting timer thread");
}

void DrMessagePump::ThreadMain(int threadId)
{
    DrAssert(m_completionPort != INVALID_HANDLE_VALUE);

    DrLogI("starting thread %d", threadId);

    bool finished = false;

    do
    {
        DWORD numBytes;
        ULONG_PTR completionKey;
        LPOVERLAPPED overlapped;

        BOOL retval = ::GetQueuedCompletionStatus(m_completionPort,
                                                  &numBytes,
                                                  &completionKey,
                                                  &overlapped,
                                                  INFINITE);

        bool mustDecrementCount = false;
        if (retval != 0)
        {
            if (overlapped == NULL)
            {
                /* This is a queue wakeup event */
                mustDecrementCount = true;
                finished = (numBytes == DRMESSAGEPUMP_EXIT);

                if (finished)
                {
                    DrLogI("received shutdown event");
                }
                else
                {
                    //DrLogI("Received queued wakeup");
                }
            }
            else
            {
                /* This is an async completion event from xcompute */
                DrAssert(numBytes == 0);
                DrAssert(completionKey == NULL);
                DrOverlapped* messageWrapper = (DrOverlapped *) overlapped;

                {
                    DrAutoCriticalSection acs(this);
#ifdef _MANAGED              
                    System::IntPtr messagePtr(messageWrapper);
                    bool removed = m_submittedOverlapped->Remove(messagePtr);
                    DrAssert(removed);
#else 
                    bool removed = m_submittedOverlapped->Remove(messageWrapper);
                    DrAssert(removed);
#endif
                }

                messageWrapper->Process();
                delete messageWrapper;
            }
        }
        else
        {
            DWORD errCode = GetLastError();
            DrLogA("error code", "%d", errCode);
        }

        bool foundMessage = false;
        do
        {
            DrMessageBaseRef message = DrNull;

            {
                DrAutoCriticalSection acs(this);

                message = m_listDummy->m_nextMessage;

                if (finished)
                {
                    /* Received a shutdown message - verify that the message queue is now empty */
                    DrAssert(message == m_listDummy);
                }

                foundMessage = false;
                while (!foundMessage && message != m_listDummy)
                {
                    int i;
                    /* Check whether another thread is holding the same 
                    lock that this message wants to acquire. If so, skip
                    this message so we don't block waiting to acquire
                    the lock. */
                    for (i=0; i<m_numWorkerThreads; ++i)
                    {
                        if (m_currentListener[i] == message->GetBaseLock())
                        {
                            message = message->m_nextMessage;
                            break;
                        }
                    }

                    if (i == m_numWorkerThreads)
                    {
                        /* Found a message - no other thread is holding the lock */
                        foundMessage = true;
                        RemoveFromList(message);
                        m_currentListener[threadId] = message->GetBaseLock();
                    }
                }

                if (!foundMessage && mustDecrementCount)
                {
                    mustDecrementCount = false;
                    DrAssert(m_numQueuedWakeUps > 0);
                    --m_numQueuedWakeUps;
                }
            }

            if (foundMessage)
            {
                /* this acquires the lock and sends the message */
                message->Deliver();

                {
                    DrAutoCriticalSection acs(this);

                    if (m_state == MPS_Stopping)
                    {
                        /* If the message pump is stopping - verify that there
                        are no more messages left on the queue */
                        DrAssert(m_listLength == 0);
                    }

                    m_currentListener[threadId] = DrNull;

                    /* If we didn't receive a shutdown message, check whether
                    there are any more messages in the queue, and
                    wake up any free threads to help process them */
                    if (!finished)
                    {
                        int numberOfSpareMessages = m_listLength;
                        int numberOfFreeThreads = 0;
                        int i;
                        for (i=0; i<m_numWorkerThreads && numberOfSpareMessages > 0; ++i)
                        {
                            if (m_currentListener[i] == DrNull)
                            {
                                ++numberOfFreeThreads;
                                --numberOfSpareMessages;
                            }
                        }

                        /* we are free by construction: if anyone else is, wake them up */
                        for (i=m_numQueuedWakeUps; i<numberOfFreeThreads; ++i)
                        {
                            ++m_numQueuedWakeUps;
                            BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                DRMESSAGEPUMP_CONTINUE,
                                NULL,
                                NULL);
                            if (retval == 0)
                            {
                                DWORD errCode = GetLastError();
                                DrLogA("post completion status", "error code: %d", errCode);
                            }
                        }
                    }
                }
            }

            if (!foundMessage && !finished)
            {
                //DrLogI("Sleeping");
            }
        } while (foundMessage);
    } while (!finished);

    DrLogI("exiting thread %d", threadId);
}

#ifdef _MANAGED

void DrMessagePump::TimerFunc()
{
    TimerThread();
}

void DrMessagePump::ThreadFunc(Object^ parameter)
{
    int threadId = (int) parameter;
    ThreadMain(threadId);
}

void DrMessagePump::StartThreads()
{
    int i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        m_threadHandle[i] = DrNew System::Threading::Thread(
            DrNew System::Threading::ParameterizedThreadStart(this, &DrMessagePump::ThreadFunc));
        m_threadHandle[i]->Start((int) i);
    }

    m_threadHandle[i] = DrNew System::Threading::Thread(
        DrNew System::Threading::ThreadStart(this, &DrMessagePump::TimerFunc));
    m_threadHandle[i]->Start();
}

void DrMessagePump::WaitForThreads()
{
    int i;
    for (i=0; i<m_numWorkerThreads+1; ++i)
    {
        m_threadHandle[i]->Join();
    }
}

#else

#include <process.h>

struct threadBlock
{
    DrMessagePumpRef    m_pump;
    int                 m_threadId;
};

unsigned __stdcall DrMessagePump::TimerFunc(void* arg)
{
    threadBlock* tb = (threadBlock *) arg;
    tb->m_pump->TimerThread();
    delete tb;
    return 0;
}

unsigned __stdcall DrMessagePump::ThreadFunc(void* arg)
{
    threadBlock* tb = (threadBlock *) arg;
    tb->m_pump->ThreadMain(tb->m_threadId);
    delete tb;
    return 0;
}

void DrMessagePump::StartThreads()
{
    ThreadArrayR thArray = *m_threadHandle;
    threadBlock* tb;
    unsigned threadAddr;
    int i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        tb = new threadBlock;
        tb->m_pump = this;
        tb->m_threadId = i;
        thArray[i] =
            (HANDLE) ::_beginthreadex(NULL,
                                      0,
                                      DrMessagePump::ThreadFunc,
                                      tb,
                                      0,
                                      &threadAddr);
        DrAssert(thArray[i] != 0);
    }

    tb = new threadBlock;
    tb->m_pump = this;
    tb->m_threadId = -1;
    thArray[i] =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  DrMessagePump::TimerFunc,
                                  tb,
                                  0,
                                  &threadAddr);
    DrAssert(thArray[i] != 0);
}

void DrMessagePump::WaitForThreads()
{
    ThreadArrayR thArray = *m_threadHandle;
    DWORD waitRet = ::WaitForMultipleObjects(m_numWorkerThreads + 1,
                                             &(thArray[0]),
                                             TRUE,
                                             INFINITE);
    DrAssert(waitRet < (WAIT_OBJECT_0 + m_numWorkerThreads + 1));

    {
        DrAutoCriticalSection acs(this);
        BOOL bRetval;

        int i;
        for (i=0; i<m_numWorkerThreads + 1; ++i)
        {
            bRetval = ::CloseHandle(thArray[i]);
            if (bRetval == 0)
            {
                DWORD errCode = GetLastError();
                DrLogA("close thread handle", "error code: %d", errCode);
            }
            thArray[i] = INVALID_HANDLE_VALUE;
        }
    }
}

#endif

HANDLE DrMessagePump::GetCompletionPort()
{
    return m_completionPort;
}

void DrMessagePump::Start()
{
    {
        DrAutoCriticalSection acs(this);

        DrAssert(m_state == MPS_Stopped);
        DrAssert(m_completionPort == INVALID_HANDLE_VALUE);

        DrLogI("entered");

        m_completionPort = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE,
                                                    NULL,
                                                    NULL,
                                                    m_numConcurrentThreads);
        DrAssert(m_completionPort != NULL);

        DrLogI("created completion port");

        StartThreads();

        m_state = MPS_Running;

        DrLogI("created threads");
    }
}

void DrMessagePump::Stop()
{
    DrLogI("entered");

    {
        DrAutoCriticalSection acs(this);

        DrAssert(m_state == MPS_Running);

        m_state = MPS_Stopping;

        while (ListEmpty() == false)
        {
            RemoveFromList(m_listDummy->m_nextMessage);
        }

        m_numQueuedWakeUps += m_numWorkerThreads;
    }

    int i;
    for (i=0; i<m_numWorkerThreads; ++i)
    {
        BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                                   DRMESSAGEPUMP_EXIT,
                                                   NULL,
                                                   NULL);
        if (retval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("post completion status",
                   "error code: %d", errCode);
        }
    }

    DrLogI("sent completion events");

    WaitForThreads();

    DrLogI("all threads have terminated");

    {
        DrAutoCriticalSection acs(this);

        DrAssert(m_numQueuedWakeUps == 0);

        BOOL bRetval = ::CloseHandle(m_completionPort);
        if (bRetval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("close completion port handle",
                   "error code: %d", errCode);
        }

        OverlappedSet::DrEnumerator e = m_submittedOverlapped->GetDrEnumerator();
        while (e.MoveNext())
        {
            DrOverlapped* element;
#ifdef _MANAGED
            element = (DrOverlapped*) e.GetElement().ToPointer();
#else
            element = e.GetElement();
#endif
            element->Discard();
        }
        m_submittedOverlapped = DrNew OverlappedSet();

        DrAssert(ListEmpty());

        m_pendingMessages = DrNew MessageQueue();

        m_completionPort = INVALID_HANDLE_VALUE;
        m_state = MPS_Stopped;
    }

    DrLogI("exiting");
}

void DrMessagePump::EnQueueInternal(DrMessageBasePtr message)
{
    AddToListTail(message);

    if (m_numQueuedWakeUps < m_numWorkerThreads)
    {
        ++m_numQueuedWakeUps;
        BOOL retval = ::PostQueuedCompletionStatus(m_completionPort,
                                                   DRMESSAGEPUMP_CONTINUE,
                                                   NULL,
                                                   NULL);
        if (retval == 0)
        {
            DWORD errCode = GetLastError();
            DrLogA("post completion status", "error code: %d", errCode);
        }
    }
}

bool DrMessagePump::EnQueue(DrMessageBasePtr message)
{
    {
        DrAutoCriticalSection acs(this);

        if (m_state == MPS_Stopping)
        {
            DrLogI("rejecting stopping item");
            return false;
        }
        else
        {
            DrAssert(m_state == MPS_Running);
        }

        EnQueueInternal(message);
    }

    return true;
}

bool DrMessagePump::EnQueueDelayed(DrTimeInterval delay, DrMessageBasePtr message)
{
    DrAssert(delay > 0);

    {
        DrAutoCriticalSection acs(this);

        DrDateTime currentTime = GetCurrentTimeStamp();

        if (m_state == MPS_Stopping)
        {
            DrLogI("rejecting stopping item");
            return false;
        }
        else
        {
            DrAssert(m_state == MPS_Running);
        }

        m_pendingMessages->Insert(currentTime + delay, message);
    }

    return true;
}

void DrMessagePump::NotifySubmissionToCompletionPort(DrOverlapped* overlapped)
{

    DrAutoCriticalSection acs(this);
    
#ifdef _MANAGED
    System::IntPtr ptr(overlapped);
    m_submittedOverlapped->Add(ptr);
#else
    m_submittedOverlapped->Add(overlapped);
#endif
}