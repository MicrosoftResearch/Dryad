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

#include "DrCommon.h"

#pragma unmanaged

volatile LONG DrJobHash::s_nextHash = 0L;

DrJobHash DrNoJobHash;

//Period in msec we wait for all worker threads to close
static const DWORD c_CloseThreadsTimeout=30000;

DrThreadPool *g_pDrClientThreads = NULL;

DrTlsPtr<DrThread> *t_ppThread = NULL;


//Slop period in msec we allow on timers so we don't wake the timer thread up to much
//e.g. If a timer expires at t=100 and we've another timer at t=120 we'll expire
//both of them rather than sleeping for the 20 msec
static const DWORD c_TimerExpirySlopPeriod=500;

DrThread *DrGenerateCurrentThread()
{
    DrRef<DrGenericThread> pThread;
    pThread.Attach(new DrGenericThread());
    LogAssert(pThread != NULL);
    pThread->AttachToCurrentThread();
    DrThread *pReturn = pThread;
    // DecRef is OK here because current thread now holds a reference
    return pReturn;
}

DrThread::DrThread(const char *pszThreadClass, const char *pszShortClass, DrThreadPool *pPool, int iBucket)
{
    DrLogD( "DrThread contructed. %s (%s), pThread=%p, pool=%p, bucket=%d", pszThreadClass, pszShortClass, this, pPool, iBucket);
    m_strClass = pszThreadClass;
    m_strShortClass = pszShortClass;
    m_pPool = pPool;
    m_iBucket = iBucket;
    m_pCurrentJob = NULL;
    m_hThread = NULL;
    m_dwThreadId = 0;
    m_hIocp = NULL;
    m_strTag = "INVL";
    m_strDescription = "Unattached Thread";
}

DrThread::~DrThread()
{
    DrLogD( "DrThread destructed. %s, pThread=%p", m_strClass.GetString(), this);
    if (m_hThread != NULL) {
        CloseHandle(m_hThread);
    }
}

void DrThread::AttachToCurrentThread()
{
    LogAssert(t_pThread.IsNull());
    t_pThread = this;
    m_dwThreadId = GetCurrentThreadId();
    IncRef();
    UpdateTagAndDescription();
    DrLogD( "DrThread::AttachToCurrentThread. %s, pThread=%p", m_strClass.GetString(), this);
}

void DrThread::DetachFromCurrentThread()
{
    LogAssert(t_pThread == this);
    t_pThread = NULL;
    DrLogD( "DrThread::AttachToCurrentThread. %s, pThread=%p", m_strClass.GetString(), this);
    m_dwThreadId = 0;
    LogAssert(m_hThread == NULL);
    m_strTag = "INVL";
    m_strDescription = "Unattached Thread";
    DecRef();
    // "this" may no longer exist
}

void DrThread::UpdateTagAndDescription()
{
    m_strDescription.SetF("%s %8u", m_strClass.GetString(), m_dwThreadId);
    m_strTag.SetF("%s %8u", m_strShortClass.GetString(), m_dwThreadId);
}

DrError DrThread::Start(
    LPSECURITY_ATTRIBUTES lpThreadAttributes,
    SIZE_T dwStackSize,
    DWORD dwCreationFlags
)
{
    LogAssert(m_hThread == NULL);
    DWORD dwThreadId;

    HANDLE h = CreateThread(
        lpThreadAttributes,
        dwStackSize,
        ThreadEntryStatic,
        this,
        dwCreationFlags | CREATE_SUSPENDED,
        &dwThreadId);
    if (h == NULL) {
        return DrGetLastError();
    }
    m_hThread = h;
    m_dwThreadId = dwThreadId;

    UpdateTagAndDescription();
    IncRef();  // The win32 thread owns one reference
    if ((dwCreationFlags & CREATE_SUSPENDED) == 0) {
        DWORD ret = ResumeThread(h);
        LogAssert(ret != (DWORD)-1);
    }
    return DrError_OK;
}

// param points to the DrThread, already IncRef'd
DWORD WINAPI DrThread::ThreadEntryStatic(void * param)
{
    DrThread *pThread = (DrThread *)param;

    // We save a pointer to this thread object in thread-local storage. That way,
    // You can always find your current thread object with DrGetCurrentThread().
    //
    LogAssert(t_pThread.IsNull());
    t_pThread = pThread;  // save pointer to ourselves in thread local storage

    DrLogI( "Dryad thread starting. %s, pThread=%p", pThread->m_strDescription.GetString(), pThread);

    DWORD ret = pThread->ThreadEntry();

    DrLogI( "Dryad thread exiting. %s, pThread=%p, exitcode=%08x", pThread->m_strDescription.GetString(), pThread, ret);

    LogAssert(t_pThread == pThread);

    // no longer under our control...
    t_pThread = NULL;

    pThread->DecRef(); // The DrThread will be freed when noone is interested in it anymoe

    return ret;
}

DrTimerThread::DrTimerThread(DrThreadPool *pPool) : DrThread("DrTimerThread", "TIMR", pPool)
{
    m_timerThreadWakesAt=0;
    m_dwTimerThreadSleepPeriod=INFINITE;
    m_timerEventHandle=CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_timerEventHandle != NULL);
}

DrTimerThread::~DrTimerThread()
{
    if (m_timerEventHandle != NULL) {
        CloseHandle(m_timerEventHandle);
    }
}


DrThreadPool::DrThreadPool()
{
    memset(&m_ov, 0, sizeof(m_ov));
    m_completionPortHandle=NULL;
    m_createdThreadCount=0;
    m_rgWorkerThreads = NULL;
    m_threadsShouldQuit=false;
    m_hashedThreadCount = 0;
    m_rgHashedThreads = NULL;
    m_nextRandomHash = 0L;
    m_numHashBuckets = 0;
}


DrThreadPool::~DrThreadPool()
{
    LogAssert(m_completionPortHandle==NULL);
    LogAssert(m_createdThreadCount == 0);
    LogAssert(m_hashedThreadCount == 0);

    if (m_rgWorkerThreads != NULL)
    {
        delete[] m_rgWorkerThreads;
    }

    if (m_rgHashedThreads != NULL)
    {
        delete[] m_rgHashedThreads;
    }
}


DrError DrThreadPool::Initialize(int initialThreadCount, int numHashedThreads)
{
    DrError err = DrError_OK;

    LogAssert(m_rgWorkerThreads==NULL);
    LogAssert(m_rgHashedThreads==NULL);
    LogAssert(m_createdThreadCount==0);
    LogAssert(m_hashedThreadCount==0);
    LogAssert(m_completionPortHandle==NULL);
    LogAssert(m_threadsShouldQuit==false);

/* JC LogAsserts below will catch
    if (initialThreadCount < 0) {
        initialThreadCount = (int)g_pDryadConfig->GetNumProcessors();
    }
    if (numHashedThreads < 0) {
       numHashedThreads= (int)g_pDryadConfig->GetNumProcessors();
    }
*/

    m_numHashBuckets = numHashedThreads;

    LogAssert(initialThreadCount >=0 && initialThreadCount < 1000000);
    LogAssert(numHashedThreads >=0 && numHashedThreads < 1000000);
    LogAssert(initialThreadCount + numHashedThreads > 0);

    if (initialThreadCount != 0) {
        m_completionPortHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, NULL, 0);
        if (m_completionPortHandle==NULL)
        {
            err = DrGetLastError();
            goto ExitError;
        }
    }

    if (numHashedThreads != 0) {
        m_rgHashedThreads = new DrHashThread *[(Size_t)numHashedThreads];
        LogAssert(m_rgHashedThreads != NULL);
        memset(m_rgHashedThreads, 0, sizeof(DrHashThread *) * numHashedThreads);
        for (int i = 0; i < numHashedThreads; i++) {
            m_rgHashedThreads[i] = new DrHashThread(this, i);
            LogAssert(m_rgHashedThreads[i]  != NULL);
            m_rgHashedThreads[i] ->m_hIocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, NULL, 0);
            if (m_rgHashedThreads[i]->m_hIocp == NULL)
            {
                err = DrGetLastError();
                goto ExitError;
            }
        }
    }

    //Spin up the timer thread
    m_pTimerThread.Attach(new DrTimerThread(this));
    LogAssert(m_pTimerThread != NULL);

    // TODO: We should be able to write the threadpool so this timer isn't needed.
    //Clever version would have a single threadpool thread nominating itself as a timing thread
    //if no other threads are and sleeping for the appropriate period before firing timers
    //Simpler version would have a single threadpool thread permanently being the timer thread
    //and waking up every second or so to fire off timers.
    //Come back to this when we've got better usage patterns for timers
    err = m_pTimerThread->Start();
    if (err != DrError_OK)
    {
        goto ExitError;
    }

    //Spin up all the worker threads we were asked to

    if (initialThreadCount != 0) {
        m_rgWorkerThreads=new DrPoolThread *[(size_t) initialThreadCount];
        LogAssert(m_rgWorkerThreads != NULL);
        memset(m_rgWorkerThreads, 0, sizeof(DrPoolThread *) * initialThreadCount);

        for ( ; m_createdThreadCount < initialThreadCount; m_createdThreadCount++)
        {
            m_rgWorkerThreads[m_createdThreadCount] = new DrPoolThread(this);
            LogAssert(m_rgWorkerThreads[m_createdThreadCount]  != NULL);
            m_rgWorkerThreads[m_createdThreadCount]->m_hIocp = m_completionPortHandle;
            err = m_rgWorkerThreads[m_createdThreadCount]->Start();
            if (err != DrError_OK) {
                break;
            }
        }

        //If we couldn't spin up all required threads then treat that as an error
        //and tear everything down
        if (err != DrError_OK) {
            goto ExitError;
        }
    }


    if (numHashedThreads!= 0) {
        for ( ; m_hashedThreadCount < numHashedThreads; m_hashedThreadCount++) {
            err = m_rgHashedThreads[m_hashedThreadCount]->Start();
            if (err != DrError_OK) {
                break;
            }
        }

        //If we couldn't spin up all required threads then treat that as an error
        //and tear everything down
        if (err != DrError_OK) {
            goto ExitError;
        }
    }

    //Looks like everything is good!
    return DrError_OK;

ExitError:

    //Flag all threads read to test for quit
    m_threadsShouldQuit=true;

    //If we managed to spin up any worker threads then tell them to quit
    CloseWorkerThreads();

    if (m_pTimerThread != NULL) {
        m_pTimerThread->Signal();
        m_pTimerThread->WaitForTermination();
        m_pTimerThread = NULL;
    }

    //Tear down the completition port and any allocated memory
    if (m_completionPortHandle != NULL)
    {
        CloseHandle(m_completionPortHandle);
        m_completionPortHandle=NULL;
    }

    m_threadsShouldQuit=false;
    return err;
}


DrError DrThreadPool::Deinitialize()
{
    //Should only be called if initialize succeeded, so assert that
    LogAssert(m_createdThreadCount+ m_hashedThreadCount != 0);
    LogAssert(m_pTimerThread != NULL);

    m_threadsShouldQuit=true;

    m_pTimerThread->Signal();
    m_pTimerThread->WaitForTermination();
    m_pTimerThread = NULL;

    DrError closeResult=CloseWorkerThreads();

    if (m_completionPortHandle != NULL) {
        CloseHandle(m_completionPortHandle);
        m_completionPortHandle=NULL;
    }

    m_threadsShouldQuit=false;

    return closeResult;
}


DrError DrThreadPool::CloseWorkerThreads()
{
    LogAssert(m_threadsShouldQuit==true);

    DrError closeResult=DrError_OK;

    // To make threads quit we send them a completition with 'this' as the overlapped
    // pointer. That causes them to inspect the thread pool state which will
    // allow them to spot the fact we want them to quit

    if (m_createdThreadCount != 0) {
        //Wake all started threads up once each
        for (int i=0; i<m_createdThreadCount; i++)
        {
            PostQueuedCompletionStatus(m_completionPortHandle, 0, 0, &m_ov);
        }

        //Now wait for all threads
        for (int i=0; i<m_createdThreadCount; i++)
        {
            if (!m_rgWorkerThreads[i]->WaitForTermination(c_CloseThreadsTimeout)) {
                closeResult=DrError_Fail;
            }
            m_rgWorkerThreads[i]->DecRef();
            m_rgWorkerThreads[i]=NULL;
        }

        m_createdThreadCount = 0;
    }

    if (m_rgWorkerThreads != NULL) {
        delete[] m_rgWorkerThreads;
        m_rgWorkerThreads = NULL;
    }

    if (m_hashedThreadCount != 0) {
        //Wake all started threads up once each
        for (int i=0; i<m_hashedThreadCount; i++)
        {
            PostQueuedCompletionStatus(m_rgHashedThreads[i]->GetThreadIocp(), 0, 0,&m_ov);
        }

        for (int i=0; i<m_hashedThreadCount; i++)
        {
            if (!m_rgHashedThreads[i]->WaitForTermination(c_CloseThreadsTimeout)) {
                closeResult=DrError_Fail;
            }
        }

        m_hashedThreadCount = 0;
    }

    if (m_rgHashedThreads != NULL) {
        delete[] m_rgHashedThreads;
        m_rgHashedThreads = NULL;
    }

    return closeResult;
}


bool DrThreadPool::EnqueueJobWithStatus(DrJob *job, DWORD numBytes, ULONG_PTR key, DrError err)
{

    LogAssert(job);
    LogAssert(m_completionPortHandle);

    job->m_postedError = err;
    HANDLE h = GetCompletionHandleForBucket(GetBucketOfJob(job));
    if (h == NULL) {
        SetLastError(ERROR_NOT_SUPPORTED);
        return false;
    }
    job->SetDefaultThreadPool(this);
    return (PostQueuedCompletionStatus(
        h,
        numBytes,
        key,
        job->GetOverlapped()) != 0);
}


bool DrThreadPool::AssociateHandleWithPool(HANDLE fileHandle, ULONG_PTR key)
{
    LogAssert(fileHandle!=NULL && fileHandle!=INVALID_HANDLE_VALUE);
    HANDLE h = GetCompletionHandleForBucket(-1);
    if (h == NULL) {
        SetLastError(ERROR_NOT_SUPPORTED);
        return false;
    }

    return (CreateIoCompletionPort(fileHandle, h, key, 0) == h);
}

bool DrThreadPool::AssociateHandleWithPoolAndHash(HANDLE fileHandle, ULONG_PTR key, const DrJobHash& jobHash)
{
    LogAssert(fileHandle!=NULL && fileHandle!=INVALID_HANDLE_VALUE);
    HANDLE h = GetCompletionHandleForBucket(GetBucketOfHash(jobHash));
    if (h == NULL) {
        SetLastError(ERROR_NOT_SUPPORTED);
        return false;
    }

    return (CreateIoCompletionPort(fileHandle, h, key, 0) == h);
}

HANDLE DrThreadPool::GetCompletionHandleForBucket(int iBucket)
{
    HANDLE h = NULL;
    if (iBucket < 0) {
        if (m_completionPortHandle != NULL) {
            h = m_completionPortHandle;
        } else {
            // This pool has no non-hashed threads. Pick a random hashed thread to run the job in.
            DrJobHash jobHash;
            jobHash.SetSequentialHash();
            iBucket = GetBucketOfHash(jobHash);
            h = m_rgHashedThreads[iBucket]->GetThreadIocp();
        }
    } else if (iBucket < m_numHashBuckets) {
        h = m_rgHashedThreads[iBucket]->GetThreadIocp();
    }
    return h;
}

 //Schedule a timer to run a specific number of msec from now
void DrThreadPool::ScheduleTimerMs(DrJob * timer, DWORD delay)
 {
    LogAssert(m_pTimerThread != NULL);
    m_pTimerThread->ScheduleTimerMs(timer, delay);
 }

 //Schedule a timer to run a specific number of msec from now
void DrTimerThread::ScheduleTimerMs(DrJob * timer, DWORD delay)
{
    bool wakeTimerThread=false;
    DWORD currentTime=GetTickCount();

    //Make sure we don't clash with the timer thread
    Lock();

    LogAssert(timer->m_isActiveTimer==false);

    //Set state of timer and insert it into heap
    timer->m_isActiveTimer=true;
    timer->m_expiryTime=currentTime+delay;
    timer->SetDefaultThreadPool(m_pPool);
    m_timerHeap.InsertHeapEntry(timer);

    //Now if we just inserted that at the root of the heap (i.e. New first timer) AND
    //the timer thread is currently sleeping to infinity OR
    //the time before it wakes up is too long then give it a kick
    if ((DrJob * ) m_timerHeap.PeekHeapRoot()==timer &&
        (m_dwTimerThreadSleepPeriod==INFINITE ||
        ((int ) (m_timerThreadWakesAt-currentTime))>c_TimerExpirySlopPeriod))
    {
        wakeTimerThread=true;
    }

    Unlock();

    if (wakeTimerThread)
    {
        SetEvent(m_timerEventHandle);
    }
}


bool DrTimerThread::CancelTimer(DrJob * timer)
{
    bool cancelledOK=false;

    //Make sure we don't clash with the timer thread
    Lock();

    //If the timer isn't active then its already being processed
    //Can't do much about that case
    //Otherwise we should pull it from heap

    if (timer->m_isActiveTimer)
    {
        timer->m_isActiveTimer=false;
        m_timerHeap.RemoveHeapEntry(timer->m_heapIndex);
        cancelledOK=true;
    }

    Unlock();

    return cancelledOK;

}

bool DrThreadPool::CancelTimer(DrJob * timer)
{
    return m_pTimerThread->CancelTimer(timer);
}


DWORD DrPoolWorkerThread::PoolWorkerEntry()
{
    int iBucket = GetJobHashBucket();
    HANDLE iocp = GetThreadIocp();
    LogAssert(iocp != NULL);

    while (true)
    {
        m_pCurrentJob = NULL;
        OVERLAPPED *overlapped = NULL;
        ULONG_PTR   completionKey = NULL;
        DWORD       bytesTransferred = 0;

        //TODO: We need to integrate timer functionality here.
        //Threads should check if there are any timers scheduled and
        //if no other thread is going to service them set their
        //timeout so they can do it

        BOOL success = GetQueuedCompletionStatus(
            iocp,
            &bytesTransferred,
            &completionKey,
            &overlapped,
            INFINITE
        );

        //If the overlapped structure is this object then thats our internal
        //signal rather than an externally submitted job
        if (overlapped == m_pPool->GetCommonOv())
        {
            //We should check the thread pool state
            if (m_pPool->ShouldQuit())
            {
                return 0;
            }
        }
        else
        {
            if (success)
            {
                //Looks like we got a valid job completed
                LogAssert(overlapped != NULL);

                DrJob *pJob = DrJob::MapOverlappedToJob(overlapped);
                int iJobBucket = m_pPool->GetBucketOfJob(pJob);
                if (iJobBucket >= 0 && iJobBucket != iBucket) {
                    // This job completed in the wrong thread (probably because the job specified
                    // a different hashcode than the hashcode that the I/O handle was bound to).
                    // Simply resubmit the job to complete in the proper thread.
                    m_pPool->EnqueueJobWithStatus(pJob, bytesTransferred, completionKey, pJob->m_postedError);
                } else {
                    m_pCurrentJob = pJob;
                    m_currentJobHash = pJob->GetJobHash();
                    if (pJob->m_postedError == DrError_OK) {
                        pJob->JobReady(bytesTransferred, completionKey);
                    } else {
                        pJob->JobFailed(bytesTransferred, completionKey, pJob->m_postedError);
                    }
                    m_pCurrentJob = NULL;
                }
            }
            else
            {
                //Couldn't get a good completition
                DrError lastError=DrGetLastError();
                LogAssert(lastError != DrError_OK);

                //If we've got an associated job then tell it
                if (overlapped != NULL)
                {
                    DrJob *pJob = DrJob::MapOverlappedToJob(overlapped);
                    int iJobBucket = m_pPool->GetBucketOfJob(pJob);
                    if (iJobBucket >= 0 && iJobBucket != iBucket) {
                        // This job completed in the wrong thread (probably because the job specified
                        // a different hashcode than the hashcode that the I/O handle was bound to).
                        // Simply resubmit the job to complete in the proper thread.
                        m_pPool->EnqueueJobWithStatus(pJob, bytesTransferred, completionKey, lastError);
                    } else {
                        m_pCurrentJob = pJob;
                        m_currentJobHash = pJob->GetJobHash();
                        pJob->JobFailed(bytesTransferred, completionKey, lastError);
                        m_pCurrentJob = NULL;
                    }
                }
                else
                {
                    //TODO: What can we do sensibly in this scenario?
                    //Are there specific cases we should be able to handle?
                    LogAssert(overlapped != overlapped);
                }
            }

        }
    }
}

DWORD DrPoolThread::ThreadEntry()
{
    return PoolWorkerEntry();
}

void DrHashThread::UpdateTagAndDescription()
{
    m_strDescription.SetF("%s %8u(%d)", m_strClass.GetString(), m_dwThreadId, m_iBucket);
    m_strTag.SetF("%s %8u(%d)", m_strShortClass.GetString(), m_dwThreadId, m_iBucket);
}

DWORD DrHashThread::ThreadEntry()
{
    return PoolWorkerEntry();
}

DWORD DrTimerThread::ThreadEntry()
{
    DWORD currentTime;
    int timeToExpire;
    DrJob * timer;

    while (true)
    {
        //Analyse the current state of the timer heap
        Lock();

        currentTime=GetTickCount();
        while (true)
        {
            timer = (DrJob*) m_timerHeap.PeekHeapRoot();

            //If there are no more timers on the heap then our timeout on our event
            //is infinite and we're done checking the heap
            if (timer==NULL)
            {
                m_dwTimerThreadSleepPeriod=INFINITE;
                break;
            }

            //Looks like we've got a timer. Work out when it'll expire relative to now
            LogAssert(timer->m_isActiveTimer);
            timeToExpire=(int ) (timer->m_expiryTime-currentTime);

            //If it hasn't expired yet then that should be our timeout and we're done
            //checking the heap
            if (timeToExpire>0)
            {
                m_dwTimerThreadSleepPeriod=(DWORD)timeToExpire;
                break;
            }

            //Looks like we've got an expired timer. Mark is as no longer active, pop it from heap
            //and pass it to a worker thread for processing
            timer->m_isActiveTimer=false;
            m_timerHeap.DequeueHeapRoot();
            BOOL fSuccess = m_pPool->EnqueueJob(timer, currentTime, NULL);
            LogAssert(fSuccess);
        }

        //Little wrinkle here. We don't want to wake up too often and we don't care
        //to be all that accurate for expiring timers. Therefore, if timeout is
        //too small we'll push it out in the hope of expiring more timers at once
        if (m_dwTimerThreadSleepPeriod<c_TimerExpirySlopPeriod)
        {
            m_dwTimerThreadSleepPeriod=c_TimerExpirySlopPeriod;
        }
        m_timerThreadWakesAt=currentTime+m_dwTimerThreadSleepPeriod;

        ResetEvent(m_timerEventHandle);

        Unlock();

        //Above should have set timeout for an appropriate value. Sleep waiting for that time
        //to expire or to be told we have a new head of the heap timer
        WaitForSingleObject(m_timerEventHandle, m_dwTimerThreadSleepPeriod);

        //Possible we were woken because the thread pool is being shut down so check that case
        if (m_pPool->ShouldQuit())
        {
            break;
        }

    }

    //Treat any timers still scheduled as if they were cancelled
    Lock();

    while (true)
    {
        timer = (DrJob*) m_timerHeap.DequeueHeapRoot();
        if (timer==NULL)
        {
            break;
        }
        LogAssert(timer->m_isActiveTimer==true);
        timer->m_isActiveTimer=false;
    }

    Unlock();

    return 0;
}
