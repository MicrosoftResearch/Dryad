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

#ifndef __DRYADTHREAD_H__
#define __DRYADTHREAD_H__

#pragma once

//JC#include "XsAutoActivity.h"

//Handy macro for mapping from memember of struct/class to its container object
//TODO: Isn't there already a standard MACRO defined for this? Replace if so.
//TODO: This should probably be in a common types/macros header rather than this one
#ifndef DR_GET_CONTAINER
#define DR_GET_CONTAINER(type, address, field) ((type *)( \
                                                  (PCHAR)(address) - \
                                                  (UINT_PTR)(&((type *)0)->field)))
#endif // if not defined DR_GET_CONTAINER


// Abstraction of a Thread-local storage pointer. These should be created sparingly, because they are a scarce resource.
class DrTlsVoidPointer
{
public:
    DrTlsVoidPointer()
    {
        m_tlsSlot = TlsAlloc();
        LogAssert(m_tlsSlot != TLS_OUT_OF_INDEXES);
    }
    
    ~DrTlsVoidPointer()
    {
        TlsFree(m_tlsSlot);
    }

    const void *GetConstVoidPtr() const
    {
        const void *p = TlsGetValue(m_tlsSlot);
        LogAssert(p != NULL || GetLastError() == ERROR_SUCCESS);
        return p;
    }

    void *GetVoidPtr()
    {
        return (void *)GetConstVoidPtr();
    }

    void SetVoidPtr(const void *p)
    {
        BOOL fSuccess = TlsSetValue(m_tlsSlot, (void *)p);
        LogAssert(fSuccess);
    }
    
    bool IsNull() const
    {
        return GetConstVoidPtr() == NULL;
    }
    
private:
    DWORD m_tlsSlot;
};

template<class t> class DrTlsPtr : public DrTlsVoidPointer
{
public:
    DrTlsPtr()
    {
    }

    operator t *()
    {
        return (t *)GetVoidPtr();
    }

    operator const t *() const
    {
        return (const t *)GetConstVoidPtr();
    }
    
    DrTlsPtr& operator =(const t *p)
    {
        SetVoidPtr(p);
        return *this;
    }

};

class DrThreadPool;

extern DrThreadPool *g_pDrClientThreads;

class DrThread;
class DrJob;

// we keep a refcounted pointer to our own thread in TLS

extern DrTlsPtr<DrThread> *t_ppThread;

__inline DrTlsPtr<DrThread>& DrGetThreadTlsPtr()
{
    if (t_ppThread == NULL) {
        t_ppThread = new DrTlsPtr<DrThread>();
        LogAssert(t_ppThread != NULL);
    }

    return *t_ppThread;
}

void StartDumpCountersThread(const char *componentName);

#define t_pThread (DrGetThreadTlsPtr())

// A DrJobHash is a wrapper for an optiional 32-bit hashcode that can be used to serialize
// jobs. If a hashcode is associated with a job, the job will be serialized with other jobs that
// have the same hashcode.
class DrJobHash
{
public:
    DrJobHash()
    {
        m_hashCode = 0;
        m_fHasHash = false;
    }

    DrJobHash(const DrJobHash& other)
    {
        m_hashCode = other.m_hashCode;
        m_fHasHash = other.m_fHasHash;
    }

    DrJobHash(UInt32 hash)
    {
        m_hashCode = hash;
        m_fHasHash = true;
    }

    DrJobHash& operator=(const DrJobHash& other)
    {
        m_hashCode = other.m_hashCode;
        m_fHasHash = other.m_fHasHash;
        return *this;
    }

    DrJobHash& operator=(UInt32 hash)
    {
        m_hashCode = hash;
        m_fHasHash = true;
        return *this;
    }

    operator UInt32() const
    {
        return m_hashCode;
    }

    bool HasHash() const
    {
        return m_fHasHash;
    }

    UInt32 GetHashcode() const
    {
        return m_hashCode;
    }

    void SetHasHash(bool fHasHash = true)
    {
        m_fHasHash = fHasHash;
        if (!fHasHash) {
            m_hashCode = 0;
        }
    }

    void SetNoHash()
    {
        m_hashCode = 0;
        m_fHasHash = false;
    }

    void SetHandleHash(HANDLE h)
    {
        m_fHasHash = true;
        m_hashCode = (UInt32)((ULONG_PTR)h >> 3);
    }

    void SetSocketHash(SOCKET h)
    {
        m_fHasHash = true;
        m_hashCode = (UInt32)((ULONG_PTR)h >> 3);
    }

    void SetHeapItemHash(const void *p)
    {
        m_fHasHash = true;
        m_hashCode = (UInt32)((ULONG_PTR)p >> 4);
    }

    void SetUlongPtrHash(ULONG_PTR p)
    {
        SetHeapItemHash((const void *)p);
    }
    
    static UInt32 GetSequentialHashcode()
    {
        LONG val = InterlockedIncrement(&s_nextHash);
        return (UInt32)val;
    }

    void SetSequentialHash()
    {
        m_fHasHash = true;
        m_hashCode = GetSequentialHashcode();
    }

    // returns -1 if there is no hash code
    int GetBucket(UInt32 numBuckets) const
    {
        if (!m_fHasHash) {
            return -1;
        }
        LogAssert(numBuckets != 0);
        return (int)(m_hashCode % numBuckets);
    }

private:
    UInt32 m_hashCode;
    bool m_fHasHash;
    static volatile LONG s_nextHash;
};

extern DrJobHash DrNoJobHash;

class DrThread : public DrRefCounter, public DrLockable
{
    friend class DrThreadPool;
    
public:
    DrThread(const char *pszThreadClass, const char *pszShortClass, DrThreadPool *pPool = NULL, int iBucket = -1);
    virtual ~DrThread();

    HANDLE GetThreadIocp()
    {
        return m_hIocp;
    }
    
    DrThreadPool *GetThreadPool()
    {
        return m_pPool;
    }

    int GetJobHashBucket()
    {
        return m_iBucket;
    }

    // may be corrupted if called from outside this thread
    // This version creates a copy for you
    DrJobHash GetCurrentJobHash()
    {
        return m_currentJobHash;
    }

    // may become invalid if called from outside this thread, or if
    // the job deletes itself.
    DrJob *GetCurrentJob()
    {
        return m_pCurrentJob;
    }

    HANDLE GetThreadHandle()
    {
        return m_hThread;
    }

    DWORD GetThreadId()
    {
        return m_dwThreadId;
    }

    const char *GetDescription()
    {
        return m_strDescription;
    }

    const char *GetShortTag()
    {
        return m_strTag;
    }

    const char *GetShortThreadClass()
    {
        return m_strShortClass;
    }

    const char *GetThreadClass()
    {
        return m_strClass;
    }
    
    DrError Start(
        LPSECURITY_ATTRIBUTES lpThreadAttributes = NULL,
        SIZE_T dwStackSize = 0,
        DWORD dwCreationFlags = 0
    );

    bool WaitForTermination(DWORD dwMilliseconds = INFINITE)
    {
        DWORD ret = WaitForSingleObject(m_hThread, dwMilliseconds);
        //m_e2eTransfer.ProcessReceive();
        return (ret == WAIT_OBJECT_0);
    }

    // param points to the DrThread.
    static DWORD WINAPI ThreadEntryStatic(void * param);

    void AttachToCurrentThread();

    // This thread object may be deleted if the current thread is the only reference
    void DetachFromCurrentThread();

    // called whenever thread metadata is updated that might affect the description (e.g., threadid)
    // by default, builds a simple description and tag from from the long and short class name and the thread id.
    virtual void UpdateTagAndDescription();

protected:
    // subclass gets called here when thread starts
    virtual DWORD ThreadEntry() = 0;

protected:
    DrThreadPool *m_pPool; // NULL if not owned by a thread pool
    int m_iBucket;
    DrJobHash m_currentJobHash;
    DrJob *m_pCurrentJob;   // May become invalid as the job runs if it deletes itself
    HANDLE m_hThread;
    DWORD m_dwThreadId;
    DrStr64 m_strDescription;
    DrStr32 m_strTag;
    DrStr16 m_strShortClass;
    DrStr32 m_strClass;
    HANDLE m_hIocp;  // I/O completion port used by this thread. The thread doesn't automatically close this.
};

// An unmanaged thread class that can be attached to a running win32 thread
// The ThreadEntry is never called.
class DrGenericThread : public DrThread
{
public:
    DrGenericThread(const char *pszThreadClass = "DrGenericThread", const char *pszShortClass = "????") : DrThread(pszThreadClass, pszShortClass)
    {
    }

    virtual DWORD ThreadEntry()
    {
        LogAssert(false);
        return (DWORD)-1;
    }
};

DrThread *DrGenerateCurrentThread();

// creates a DrGenericThread object  if this thread is not yet managed by cosmos libraries
inline DrThread *DrGetCurrentThread()
{
    DrThread *pThread = t_pThread;
    if (pThread == NULL) {
        pThread = DrGenerateCurrentThread();
    }
    return pThread;
}

/*
 * DrJob
 *
 * This is an abstract base class for a Dryad job.
 * User must implement the JobReady and JobFailed methods.
 * See DrEmbeddedJob below for an alternative usage pattern
 */
 
class DrJob : protected DryadHeapItem
{
    friend class DrThreadPool;
    friend class DrTimerThread;
    friend class DrPoolWorkerThread;
    
public:
    DrJob()
    {
        ZeroMemory(&m_overlapped, sizeof(m_overlapped));
        m_pDefaultThreadPool = NULL;
        m_postedError = DrError_OK;
        m_isActiveTimer=false;
        m_expiryTime = 0;
    }

    virtual ~DrJob()
    {
    }

    void SetJobHash(const DrJobHash& jobHash)
    {
        m_jobHash = jobHash;
    }

    void SetJobHash(UInt32 hash)
    {
        m_jobHash = hash;
    }
    
    const DrJobHash& GetJobHash()
    {
        return m_jobHash;
    }
    
    //Return the overlapped structure associated with this job
    //Use this when submitting the job as an IO operation
    OVERLAPPED * GetOverlapped()
    {
        return &m_overlapped;
    }

    HANDLE GetOverlappedEvent()
    {
        return m_overlapped.hEvent;
    }

    void SetOverlappedEvent(HANDLE hEvent)
    {
        m_overlapped.hEvent = hEvent;
    }

    UInt64 GetOverlappedOffset()
    {
        // Assumes little endian
        return *(UInt64 *)&(m_overlapped.Offset);
    }

    void SetOverlappedOffset(UInt64 offset)
    {
        // Assumes little endian
        *(UInt64 *)&(m_overlapped.Offset) = offset;
    }

    //Return the job object associated with a specific overlapped structure
    static DrJob * MapOverlappedToJob(OVERLAPPED * pOverlapped)
    {
        return DR_GET_CONTAINER(DrJob, pOverlapped, m_overlapped); 
    }

    DrThreadPool *GetDefaultThreadPool()
    {
        return (m_pDefaultThreadPool != NULL) ? m_pDefaultThreadPool : g_pDrClientThreads;
    }

    void SetDefaultThreadPool(DrThreadPool *pThreadPool)
    {
        m_pDefaultThreadPool = pThreadPool;
    }

    //Called when a job is completed OK.
    virtual void JobReady(DWORD numBytes, ULONG_PTR key)=0;

    //Called when a job fails. errorCode gives the reason
    virtual void JobFailed(DWORD numBytes, ULONG_PTR key, DrError errorCode)=0;

private:

    bool IsHigherPriorityThan(DryadHeapItem *other)
    {
        DrJob * otherTimer = (DrJob*) other;

        return (((int ) (otherTimer->m_expiryTime - m_expiryTime)) > 0);
    }

protected:
    OVERLAPPED m_overlapped;
    DrError m_postedError;
    DrJobHash m_jobHash;
    bool m_isActiveTimer;
    DWORD m_expiryTime;
    DrThreadPool *m_pDefaultThreadPool;
};


/*
 * DrTimer
 *
 * This is an abstract base class for a Dryad timer.
 * User must implement the TimerFired method.
 * See DrEmbeddedTimer below for an alternative usage pattern
 */


class DrTimer :
        public DrJob
{
    friend class DrThreadPool;

public:


    DrTimer()
    {
    }

    virtual ~DrTimer()
    {
    }

    DWORD GetExpiryTime()
    {
        return m_expiryTime;   
    }    

    //Implement this to do work when the timer goes off
    virtual void TimerFired(DWORD firedTime)=0;
    
    //Called when a job is completed OK.
    virtual void JobReady(DWORD numBytes, ULONG_PTR key)
    {
        TimerFired(numBytes);
    }

    //Called when a job fails. errorCode gives the reason
    virtual void JobFailed(DWORD numBytes, ULONG_PTR key, DrError errorCode)
    {
        LogAssert(false);
    }

};    


/*
 * DrThreadPoolUser, DrEmbeddedJob and DrEmbeddedTimer
 *
 * These classes provide an alternative approach to using the timer and job classes.
 * Rather than having to implement a new class for each timer and/or job, simply
 * make the embedded job/timer a member of your class and inherit from the ThreadPoolUser
 * interface class. This is useful when you've got a single object that wants to handle
 * multiple timers and jobs running at once.
 */

class DrThreadPoolUser
{
public:

    virtual void JobReady(DrJob * job, DWORD numBytes, ULONG_PTR key)=0;

    virtual void JobFailed(DrJob * job, DWORD numBytes, ULONG_PTR key, DrError errorCode)=0;

    virtual void TimerFired(DrTimer * timer, DWORD firedTime)=0;

};    

class DrEmbeddedJob : public DrJob
{
public:

    DrEmbeddedJob() : DrJob()
        {   m_user=NULL;  };

    //Call this before using it to assign the user object
    void Initialize(DrThreadPoolUser * user)
        {   m_user=user;  };

    void JobReady(DWORD numBytes, ULONG_PTR key)
        {   m_user->JobReady(this, numBytes, key);  };

    virtual void JobFailed(DWORD numBytes, ULONG_PTR key, DrError errorCode)
        {   m_user->JobFailed(this, numBytes, key, errorCode);  };

private:

    DrThreadPoolUser * m_user;
};


class DrEmbeddedTimer : public DrTimer
{
public:

    DrEmbeddedTimer() : DrTimer()
        {   m_user=NULL;  };

    //Call this before using it to assign the user object
    void Initialize(DrThreadPoolUser * user)
        {   m_user=user;  };

    void TimerFired(DWORD firedTime)
        {   m_user->TimerFired(this, firedTime);  };

private:

    DrThreadPoolUser * m_user;
};


class DrTimerThread : public DrThread
{
public:
    DrTimerThread(DrThreadPool *pPool);
    virtual ~DrTimerThread();

    virtual DWORD ThreadEntry();

    void ScheduleTimerMs(DrJob * timer, DWORD delay);
    bool CancelTimer(DrJob * timer);

    void Signal()
    {
        BOOL fSuccess = SetEvent(m_timerEventHandle);
        LogAssert(fSuccess);
    }

private:
    //Heap of timers
    DryadHeap m_timerHeap;
    
    //Event used to signal to timer thread that new timers have been queued
    HANDLE m_timerEventHandle;

    //Time the timer thread is due to wake at
    DWORD m_timerThreadWakesAt;

    //Set to the period the timer thread is sleeping for
    //INFINITY if its never planning to wake
    DWORD m_dwTimerThreadSleepPeriod;
};

class DrPoolWorkerThread : public DrThread
{
public:
    DrPoolWorkerThread(const char *pszThreadClass, const char *pszShortClass, DrThreadPool *pPool = NULL, int iBucket = -1)
        : DrThread(pszThreadClass, pszShortClass, pPool, iBucket)
    {
    }

    DWORD PoolWorkerEntry();
    
    virtual DWORD ThreadEntry() = 0;

private:
};


class DrPoolThread : public DrPoolWorkerThread
{
public:
    DrPoolThread(DrThreadPool *pPool) : DrPoolWorkerThread("DrPoolThread", "POOL", pPool)
    {
    }

    virtual DWORD ThreadEntry();

private:
};

class DrHashThread : public DrPoolWorkerThread
{
public:
    DrHashThread(DrThreadPool *pPool, int iBucket) : DrPoolWorkerThread("DrHashThread", "HASH", pPool, iBucket)
    {
    }

    virtual DWORD ThreadEntry();
    virtual void UpdateTagAndDescription();
    
private:
};


/*
 * DrThreadPool
 *
 * Encapsulates a pool of running Dryad threads
 * Allows IO jobs and timers to be queued to the pool
 */

class DrThreadPool
{
public:
    DrThreadPool();
    ~DrThreadPool();

    // Create the thread pool
    // The initialThreadCount defines the number of threads used for running non-hashed jobs. It can be 0. If it is -1, the number of processors
    // on this machine is used. If it is 0, The hashed threads will be used to run non-hashed jobs (a random hash will be assigned to each job).
    // numHashedJobs defines the number of threads used for running hashed jobs. It can be 0. If it is -1, the number of processors
    // on this machine is used. If it is 0, attempts to schedule hashed jobs on this pool will fail.
    DrError Initialize(int initialThreadCount = -1, int numHashedThreads = -1);

    // Tells all threads to quit and returns when they have.
    // Returns DrError_Fail if any threads appeared deadlocked and unresponsive
    DrError Deinitialize();

    // Callable on any thread.  Enqueue the given job on some thread in this thread pool.
    // If err is not DrError_OK, the Job will be called back through JobFailed; ptherwise, it will be called through JobReady.
    bool EnqueueJobWithStatus(DrJob *job, DWORD numBytes, ULONG_PTR key, DrError err);

    // Callable on any thread.  Enqueue the given job on some thread in this thread pool
    inline bool EnqueueJob(DrJob *job, DWORD numBytes, ULONG_PTR key)
    {
        return EnqueueJobWithStatus(job, numBytes, key, DrError_OK);
    }

    // Associate a specified handle with the thread pool. This will cause threads
    // from the pool to pick up the completitions of overlapped IO submitted
    // on the specified handle. 'key' will be passed to each job that does overlapped
    // IO on the file handle via the JobReady method
    bool AssociateHandleWithPool(HANDLE fileHandle, ULONG_PTR key);

    // Associate a specified handle with the thread pool, with a specific hash affinity. This will cause the specified hash thread
    // from the pool to pick up the completitions of overlapped IO submitted
    // on the specified handle. 'key' will be passed to each job that does overlapped
    // IO on the file handle via the JobReady method
    bool AssociateHandleWithPoolAndHash(HANDLE fileHandle, ULONG_PTR key, const DrJobHash& jobHash);

    //Schedule a job to run a specific number of msec from now
    void ScheduleTimerMs(DrJob * timer, DWORD delay);

    //Schedule a job to run a specific time interval from now
    void ScheduleTimerInterval(DrJob * timer, DrTimeInterval timeInterval)
    {
        ScheduleTimerMs(timer, DrGetTimerMsFromInterval(timeInterval));
    }

    //Cancel a job that was scheduled with ScheduleTimer. Note this can fail and caller MUST handle that case.
    //Failure means the timer has already fired (even if caller hasn't got
    //the callback yet), and owner of the timer must stick around untill
    //the callback is processed
    bool CancelTimer(DrJob * timer);

    // Returns the completion port handle associated with the thread pool. This can be used to schedule
    // completions of asynchronous operations.
    HANDLE GetCompletionPortHandle()
    {
        return m_completionPortHandle;
    }

    UInt32 GenerateRandomHash()
    {
        LONG n = InterlockedIncrement(&m_nextRandomHash);
        return (UInt32)n;
    }

    // It is a fatal error to call this with a specified hash when m_numHashBuckets is 0.
    // returns -1 if there is no hash.
    int GetBucketOfHash(const DrJobHash& jobHash)
    {
        return jobHash.GetBucket((UInt32)m_numHashBuckets);
    }

    // Returns -1 if the job is not hashed
    int GetBucketOfJob(DrJob *pJob)
    {
        int i = GetBucketOfHash(pJob->GetJobHash());
        return i;
    }

    // returns NULL if the specified bucket is not supported.
    // If iBucket is -1, uses the general completion pool handle. If there is no genera completion pool, picks a random hashed thread.
    HANDLE GetCompletionHandleForBucket(int iBucket);

    // Returns true if the currently executing thread is an appropriate thread
    // for the specified bucket.
    bool CurrentThreadIsBucketThread(int iBucket)
    {
        bool f = true;
        if (iBucket < 0) {
            // no specific bucket, so we can be in any thread
        } else if (iBucket < m_numHashBuckets) {
            f = ((DrThread *)m_rgHashedThreads[iBucket] == DrGetCurrentThread());
        } else {
            // bucket out of range, so can't be in the bucket
            f = false;
        }
        return f;
    }
    
    // Returns true if the currently executing thread is an appropriate thread
    // for the specified job hash.
    bool CurrentThreadIsHashThread(const DrJobHash& jobHash)
    {
        return CurrentThreadIsBucketThread(GetBucketOfHash(jobHash));
    }

    // Returns true if the currently executing thread is an appropriate thread
    // for the specified job.
    bool CurrentThreadIsJobThread(DrJob *pJob)
    {
        return CurrentThreadIsHashThread(GetBucketOfHash(pJob->GetJobHash()));
    }

    bool ShouldQuit()
    {
        return m_threadsShouldQuit;
    }

    OVERLAPPED *GetCommonOv()
    {
        return &m_ov;
    }
    
private:
    OVERLAPPED m_ov;

    // This is the io completion port handle tied to this thread pool for non-hashed jobs
    // Currently this value is given to all cosmos threads in the thread pool
    HANDLE m_completionPortHandle;
    
    // Number of worker threads we've spun up for non-hashed jobs
    int m_createdThreadCount;

    // Array of running thread handles for non-hashed jobs. Length is m_createdThreadCount.
    DrPoolThread **m_rgWorkerThreads;  // refcounted pointers

    int m_numHashBuckets;
    
    // Number of worker threads we've spun up for hashed jobs
    int m_hashedThreadCount;

    // Array of thread info for hashed threads. Length is m_hashedThreadCount.
    DrHashThread **m_rgHashedThreads;
    
    //Set to true when all threads should exit
    volatile bool m_threadsShouldQuit;

    //Thread that watches the timer heap and farms expired timers off to worker threads
    DrRef<DrTimerThread> m_pTimerThread;

    volatile LONG m_nextRandomHash;

    //Tells all worker threads to quit, waits for them to do so and
    //closes our handles to them. Returns DrError_Fail if it thinks
    //threads are deadlocked and not responding
    DrError CloseWorkerThreads();
};

// this version does not implement IDrRefCounter; use DrRefCountedJob if you do not need multiple inheritance
class IDrRefCountedJob : public DrJob, public IDrRefCounter
{
public:
    IDrRefCountedJob()
    {
        m_pThreadPool = NULL;
    }
    
    virtual ~IDrRefCountedJob()
    {
    }

    void SetThreadPool(DrThreadPool *pThreadPool)
    {
        m_pThreadPool = pThreadPool;
    }
    
    void ScheduleJob(DrThreadPool *pThreadPool, DrTimeInterval t)
    {
        if (pThreadPool != NULL) {
            m_pThreadPool = pThreadPool;
        } else {
            if (m_pThreadPool == NULL) {
                m_pThreadPool = g_pDrClientThreads;
            }
        }
        IncRef();
        m_pThreadPool->ScheduleTimerInterval(this, t);
    }

    void ScheduleJob(DrTimeInterval t)
    {
        ScheduleJob(NULL, t);
    }

    bool EnqueueJobWithStatus(DrThreadPool *pThreadPool, DWORD numBytes, ULONG_PTR key, DrError err)
    {
        if (pThreadPool != NULL) {
            m_pThreadPool = pThreadPool;
        } else {
            if (m_pThreadPool == NULL) {
                m_pThreadPool = g_pDrClientThreads;
            }
        }
        IncRef();
        bool fok = m_pThreadPool->EnqueueJobWithStatus(this, numBytes, key, err);
        if (!fok) {
            DecRef();
        }
        return fok;
    }

    bool EnqueueJobWithStatus(DWORD numBytes, ULONG_PTR key, DrError err)
    {
        return EnqueueJobWithStatus(NULL, numBytes, key, err);
    }

    bool EnqueueJobWithStatus(DrError err)
    {
        return EnqueueJobWithStatus(NULL, 0, 0, err);
    }

    bool EnqueueJobWithStatus(DrThreadPool *pThreadPool, DrError err)
    {
        return EnqueueJobWithStatus(pThreadPool, 0, 0, err);
    }

    bool EnqueueJob(DrThreadPool *pThreadPool, DWORD numBytes, ULONG_PTR key)
    {
        return EnqueueJobWithStatus(pThreadPool, numBytes, key, DrError_OK);
    }

    bool EnqueueJob(DWORD numBytes, ULONG_PTR key)
    {
        return EnqueueJobWithStatus(NULL, numBytes, key, DrError_OK);
    }

    bool EnqueueJob()
    {
        return EnqueueJob(NULL, 0, 0);
    }

    bool EnqueueJob(DrThreadPool *pThreadPool)
    {
        return EnqueueJob(pThreadPool, 0, 0);
    }

    // subclasses implement this method to provide job behavior
    virtual void ExecuteJob(DrError err, DWORD numBytes, ULONG_PTR key) = 0;
    
    //Called when a job is completed OK.
    virtual void JobReady(DWORD numBytes, ULONG_PTR key)
    {
        ExecuteJob(DrError_OK, numBytes, key);
        DecRef();
    }

    //Called when a job fails. errorCode gives the reason.
    virtual void JobFailed(DWORD numBytes, ULONG_PTR key, DrError errorCode)
    {
        ExecuteJob(errorCode, numBytes, key);
        DecRef();
    }


private:
    DrThreadPool *m_pThreadPool;
};

class DrRefCountedJob : public IDrRefCountedJob
{
public:
    DrRefCountedJob()
    {
    }
    
    virtual ~DrRefCountedJob()
    {
    }

public:
    DRREFCOUNTIMPL;
};


// An easy-to-use refcounted timer base class.
class DrRefCountedTimer : public DrTimer, public DrRefCounter
{
public:
    DrRefCountedTimer()
    {
        m_pThreadPool = NULL;
    }
    
    virtual ~DrRefCountedTimer()
    {
    }

    void Schedule(DrThreadPool *pThreadPool, DrTimeInterval t)
    {
        LogAssert(m_pThreadPool == NULL);
        LogAssert(pThreadPool != NULL);
        IncRef();
        m_pThreadPool = pThreadPool;
        pThreadPool->ScheduleTimerInterval(this, t);
    }

    // Either OnTimerFired or OnTimerCancelled is guarranteed to be called after
    // Schedule.
    virtual void OnTimerFired() = 0;
    virtual void OnTimerCancelled()
    {
    }
    
    // Don't overload this method. Use OnTimerFired instead.
    virtual void TimerFired(DWORD firedTime)
    {
        OnTimerFired();
        DecRef();
    }

    // Causes either OnTimerFired or OnTimerCancelled to be called as soon as possible if
    // the timer has been scheduled but one has not already been called.
    void Cancel()
    {
        if (m_pThreadPool != NULL && m_pThreadPool->CancelTimer(this)) {
            OnTimerCancelled();
            DecRef();
        }
    }

    bool HasBeenScheduled()
    {
        return (m_pThreadPool != NULL);
    }

private:
    DrThreadPool *m_pThreadPool;
};

#endif  //end if not defined __DRYADTHREAD_H__
