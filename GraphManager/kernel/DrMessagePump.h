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

#pragma once

class WorkQueue;

DRDECLARECLASS(DrMessageBase);
DRREF(DrMessageBase);

DRBASECLASS(DrMessageBase abstract)
{
public:
    virtual ~DrMessageBase();
    virtual void Deliver() = 0;
    virtual DrCritSecPtr GetBaseLock() = 0;

    DrMessageBaseRef    m_nextMessage;
    DrMessageBaseRef    m_prevMessage;
};

template <class Notification> DRINTERFACE(DrListener)
{
public:
    virtual void ReceiveMessage(Notification message) DRABSTRACT;
};

template <class Notification> DRCLASS(DrMessage) : public DrMessageBase
{
    typedef DrListener<Notification> Listener;
    DRIREF(Listener);

public:
    DrMessage(ListenerPtr listener, Notification notification)
    {
        DrAssert(listener != DrNull);
        DrICritSecPtr lCritSec = dynamic_cast<DrICritSecPtr>(listener);
        if (lCritSec == DrNull)
        {
            DrLogA("All listeners must inherit from DrICritSec");
        }
        m_listener = lCritSec;
        m_payload = notification;
    }

    DrMessage(DrLockBox<DrICritSec> listener, Notification notification)
    {
        /* we are going to use this forbidden interface, because nobody actually gets
           the listener out of here again except the pump, and that only delivers
           it which takes place under the lock */
        m_listener = listener.DoNotUse();
        m_payload = notification;
    }

    virtual ~DrMessage()
    {
    }

    virtual DrCritSecPtr GetBaseLock() DROVERRIDE
    {
        return m_listener->GetBaseLock();
    }

    virtual void Deliver() DROVERRIDE
    {
        DrAutoCriticalSection acs(m_listener);

        ListenerPtr listener = dynamic_cast<ListenerPtr>((DrICritSecPtr) m_listener);
        DrAssert(listener != DrNull);

        listener->ReceiveMessage(m_payload);
    }

    void SetPayload(Notification payload)
    {
        m_payload = payload;
    }

    Notification GetPayload()
    {
        return m_payload;
    }

private:
    DrICritSecIRef   m_listener;
    Notification     m_payload;
};

class DrOverlapped : public OVERLAPPED
{
public:
    virtual ~DrOverlapped();

    HRESULT* GetOperationStatePtr();

    virtual void Process() = 0;
    virtual void Discard() = 0;

private:
    HRESULT                 m_operationState;
};

DRENUM(MessagePumpState)
{
    MPS_Stopped,
    MPS_Running,
    MPS_Stopping
};

/* stop spurious compiler warning by exercising template machinery */
template ref class DrMultiMap<DrDateTime, DrMessageBaseRef>;

DRCLASS(DrMessagePump) : public DrCritSec
{
 public:
    DrMessagePump(int numWorkerThreads,
                  int numConcurrentThreads);
    ~DrMessagePump();

    void Start();
    void Stop();

    DrDateTime GetCurrentTimeStamp();

    bool EnQueue(DrMessageBasePtr request);
    bool EnQueueDelayed(DrTimeInterval delay, DrMessageBasePtr request);

    HANDLE GetCompletionPort();
    void NotifySubmissionToCompletionPort(DrOverlapped* overlapped);

private:
    typedef DrArray<DrCritSecRef> CSArray;
    DRAREF(CSArray,DrCritSecRef);
    typedef DrMultiMap<DrDateTime, DrMessageBaseRef> MessageQueue;
    DRREF(MessageQueue);

#ifdef _MANAGED
    typedef DrSet<System::IntPtr> OverlappedSet;
    DRREF(OverlappedSet);

    
    typedef DrArray<System::Threading::Thread^> ThreadArray;
    DRAREF(ThreadArray,System::Threading::Thread^);
    void TimerFunc();
    void ThreadFunc(Object^ parameter);

#else
    typedef DrSet<DrOverlapped*> OverlappedSet;
    DRREF(OverlappedSet);

    /* stop spurious compiler warning by exercising template machinery */
    template class DrMultiMap<DrDateTime, DrMessageBaseRef>;
    typedef DrArray<HANDLE> ThreadArray;
    DRAREF(ThreadArray,HANDLE);
    static unsigned __stdcall TimerFunc(void* parameter);
    static unsigned __stdcall ThreadFunc(void* parameter);

#endif

    void AddToListTail(DrMessageBasePtr message);
    void RemoveFromList(DrMessageBasePtr message);
    bool ListEmpty();

    void StartThreads();
    void WaitForThreads();

    void TimerThread();
    void ThreadMain(int threadId);

    void EnQueueInternal(DrMessageBasePtr message);

    DrMessageBaseRef  m_listDummy; /* list of messages */
    int               m_listLength;
    MessageQueueRef   m_pendingMessages; /* list of delayed messages */
    OverlappedSetRef  m_submittedOverlapped; /* set of overlapped objects in the completion port */

    MessagePumpState  m_state;

    int               m_numWorkerThreads;
    int               m_numConcurrentThreads;
    HANDLE            m_completionPort;
    ThreadArrayRef    m_threadHandle;
    CSArrayRef        m_currentListener;
    int               m_numQueuedWakeUps;
};
DRREF(DrMessagePump);

template <class Notification> DRCLASS(DrNotifier) : public DrSharedCritSec
{
    typedef DrMessage<Notification> Message;
    DRREF(Message);
    typedef DrListener<Notification> Listener;
    DRIREF(Listener);
    typedef DrArrayList<ListenerIRef> LArray;
    DRAREF(LArray,ListenerIRef);

public:
    DrNotifier(DrMessagePumpPtr pump) : DrSharedCritSec(DrNew DrCritSec())
    {
        m_pump = pump;
        m_listener = DrNew LArray();
    }

    DrNotifier(DrMessagePumpPtr pump, DrICritSecPtr cs) : DrSharedCritSec(cs)
    {
        m_pump = pump;
        m_listener = DrNew LArray();
    }

    bool AddListener(ListenerPtr listener)
    {
        int nListeners = m_listener->Size();
        int i;
        for (i=0; i<nListeners; ++i)
        {
            if (m_listener[i] == listener)
            {
                /* this listener is already on the list */
                return false;
            }
        }

        m_listener->Add(listener);

        return true;
    }

    bool CancelListener(ListenerPtr listener)
    {
        return m_listener->Remove(listener);
    }

protected:
    void DeliverNotification(Notification notification)
    {
        int nListeners = m_listener->Size();
        int i;
        for (i=0; i<nListeners; ++i)
        {
            ListenerPtr listener = m_listener[i];
            MessageRef message = DrNew Message(listener, notification);
            m_pump->EnQueue(message);
        }
    }

    void DeliverDelayedNotification(DrTimeInterval delay, Notification notification)
    {
        int nListeners = m_listener->Size();
        int i;
        for (i=0; i<nListeners; ++i)
        {
            ListenerPtr listener = m_listener[i];
            MessageRef message = DrNew Message(listener, notification);
            m_pump->EnQueueDelayed(delay, message);
        }
    }

    void DeliverMessage(DrMessageBasePtr message)
    {
        m_pump->EnQueue(message);
    }

    void DeliverDelayedMessage(DrTimeInterval delay, DrMessageBasePtr message)
    {
        m_pump->EnQueueDelayed(delay, message);
    }

private:
    DrMessagePumpRef      m_pump;
    LArrayRef             m_listener;
};



typedef DrListener<DrErrorRef> DrErrorListener;
DRIREF(DrErrorListener);

typedef DrMessage<DrErrorRef> DrErrorMessage;
DRREF(DrErrorMessage);

typedef DrNotifier<DrErrorRef> DrErrorNotifier;
DRREF(DrErrorNotifier);

/* Output stream lease extension */
typedef bool DrLeaseExtender;

typedef DrListener<DrLeaseExtender> DrLeaseListener;
DRIREF(DrLeaseListener);

typedef DrMessage<DrLeaseExtender> DrLeaseMessage;
DRREF(DrLeaseMessage);

/* Graph shutdown */
typedef HRESULT DrExitStatus;

typedef DrListener<DrExitStatus> DrShutdownListener;
DRIREF(DrShutdownListener);

typedef DrMessage<DrExitStatus> DrShutdownMessage;
DRREF(DrShutdownMessage);
