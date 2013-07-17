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

#ifndef __DRYADENHANCEDTIMER_H__
#define __DRYADENHANCEDTIMER_H__

/*
 * DrEnhancedTimer
 *
 * The enhanced timer class improves the standard timer class by guaranteeing
 * that it can always be cancelled and that it can always be rescheduled whilst
 * its still running.
 *
 * To use this you must supply an owner object type that supports the methods:
 * void IncRef()
 * void DecRef() both for reference counting
 * void EnhancedTimerFired(DrEnhancedTimer<T> * ,DWORD ) for receiving timer fired events
 *
 * Locking conventions
 * The owner of an enhanced timer must supply a critical section lock that guards
 * its timing state.
 * This lock must be held by the owner when it schedules/cancels the timer
 * This lock will be taken by the timer prior to calling back into the owner
 *
 * Reference counting
 * The timer will take and hold references to the owner while it has outstanding
 * threadpool operations in progress.
 */

//Disable warning about non standard extentions
//We're using nameless structs/unions and compiler gets whiny about them
#pragma warning (push)
#pragma warning (disable:4201)


template <class Owner>
class DrEnhancedTimer : public DrTimer
{
public:

    //Standard c'tor. Must call Initialize prior to using timer if you use this
    inline DrEnhancedTimer();

    //Construct and initialize timer
    inline DrEnhancedTimer(Owner * pOwner, DrThreadPool * pThreadPool, DrCriticalSection * pLock);

    //Initialize timer state if standard c'tor was used
    inline void Initialize(Owner * pOwner, DrThreadPool * pThreadPool, DrCriticalSection * pLock);

    //Schedule the timer to fire in dwPeriod msec from now.
    //This will cancel and reschedule the timer if its already running
    //Lock supplied to c'tor/Initialize must be held on this call
    inline void Schedule(DWORD dwPeriod);

    //Cancel the timer
    //Lock supplied to c'tor/Initialize must be held on this call
    inline void Cancel();


private:

    inline void TimerFired(DWORD dwFiredTime);


    //Time that Owner wants the timer to fire at
    DWORD m_dwExpiryTime;

    //Thread pool timer is currently scheduled against
    DrThreadPool * m_pThreadPool;

    //Object using this timer
    Owner * m_pOwner;

    //Lock created by owner to guard its timing state
    DrCriticalSection * m_pLock;

    union
    {
        DWORD m_dwFlags;
        struct
        {
            //Set to TRUE if the timer is currently running
            DWORD m_fTimerRunning : 1;
            //Set to TRUE if Owner really wants the timer callback
            DWORD m_fTimerRequired : 1;
        };
    };

};


/*
 * Inline methods from DrEnhancedTimer
 */

template <class Owner>
DrEnhancedTimer<Owner>::DrEnhancedTimer()
{
    m_pThreadPool=NULL;
    m_pOwner=NULL;
    m_pLock=NULL;
    m_dwFlags=0;
}


template <class Owner>
DrEnhancedTimer<Owner>::DrEnhancedTimer(Owner * pOwner, DrThreadPool * pThreadPool, DrCriticalSection * pLock)
{
    m_pThreadPool=pThreadPool;
    m_pOwner=pOwner;
    m_pLock=pLock;
    m_dwFlags=0;
}

template <class Owner>
void DrEnhancedTimer<Owner>::Initialize(Owner * pOwner, DrThreadPool * pThreadPool, DrCriticalSection * pLock)
{
    LogAssert(m_pOwner==NULL);
    LogAssert(m_pThreadPool==NULL);
    LogAssert(m_dwFlags==0);

    m_pThreadPool=pThreadPool;
    m_pOwner=pOwner;
    m_pLock=pLock;
}


template <class Owner>
void DrEnhancedTimer<Owner>::Schedule(DWORD dwPeriod)
{
    DebugLogAssert( m_pLock->Aquired() );

    //Whatever happens we want a timer to fire
    m_fTimerRequired=TRUE;

    //Compute when we want the timer to expire
    m_dwExpiryTime=GetTickCount()+dwPeriod;

    //If the timer is already scheduled then try and cancel it
    if (m_fTimerRunning)
    {
        if (m_pThreadPool->CancelTimer(this)==FALSE)
        {
            //Let it fire and detect the bogus expiry time at that point
            return;
        }
    }
    else
    {
        //Timer wasn't previously running so store fact it now will be
        //and that owner needs to stick around
        m_fTimerRunning=TRUE;
        m_pOwner->IncRef();
    }

    m_pThreadPool->ScheduleTimerMs(this, dwPeriod);
}


template <class Owner>
void DrEnhancedTimer<Owner>::Cancel()
{
    DebugLogAssert( m_pLock->Aquired() );

     //Whatever happens owner doesn't want a timer running
    m_fTimerRequired=FALSE;

    //If its not scheduled then we're done, OR
    //If we fail to cancel it then we'll just have to let it fire and
    //spot that its no longer requested then
    if (m_fTimerRunning==FALSE || m_pThreadPool->CancelTimer(this)==FALSE)
    {
        return;
    }

    //Looks like we cancelled it OK
    m_fTimerRunning=FALSE;
    m_pOwner->DecRef();
}


template <class Owner>
void DrEnhancedTimer<Owner>::TimerFired(DWORD dwFiredTime)
{
    m_pLock->Enter();

    LogAssert(m_fTimerRunning);

    m_fTimerRunning=FALSE;

    //If owner didn't currently want a timer then we're done
    if (m_fTimerRequired==FALSE)
    {
        m_pLock->Leave();
        m_pOwner->DecRef();
        return;
    }

    //If this isn't the right time for the timer to fire then reschedule it
    //for the right time
    int iTimeRemaining=(int ) (m_dwExpiryTime-dwFiredTime);
    if (iTimeRemaining>0)
    {
        m_pThreadPool->ScheduleTimerMs(this, iTimeRemaining);
        m_fTimerRunning=TRUE;
        m_pLock->Leave();
        return;
    }

    //Looks like we've got a good timer that we want to process
    m_fTimerRequired=FALSE;

    //Tell the owner a timer has expired
    m_pOwner->EnhancedTimerFired(this, dwFiredTime);

    m_pLock->Leave();
    m_pOwner->DecRef();

}

#pragma warning (pop)

#endif  //end if not defined __DRYADENHANCEDTIMER_H__

