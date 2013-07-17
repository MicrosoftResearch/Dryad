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

#include "DrRef.h"

DRDECLARECLASS(DrCritSec);
DRREF(DrCritSec);

DRINTERFACE(DrICritSec)
{
public:
    virtual void Enter() DRABSTRACT;
    virtual void Leave() DRABSTRACT;
    virtual DrCritSecPtr GetBaseLock() DRABSTRACT;
};
DRIREF(DrICritSec);

#ifdef _MANAGED

DRBASECLASS(DrCritSec),  public DrICritSec
{
public:
    virtual void Enter()
    {
        System::Threading::Monitor::Enter(this);
    }

    virtual void Leave()
    {
        System::Threading::Monitor::Exit(this);
    }

    virtual DrCritSecPtr GetBaseLock()
    {
        return this;
    }
};

ref class DrAutoCriticalSection
{
public:
    DrAutoCriticalSection(DrICritSecPtr critSec)
    {
        m_critSec = critSec;
        m_critSec->Enter();
    }
    ~DrAutoCriticalSection()
    {
        m_critSec->Leave();
    }

private:
    DrICritSecIRef m_critSec;
};

template <class T> ref class DrLockBox
{
public:
    DrLockBox()
    {
    }

    DrLockBox(T^ t)
    {
        m_obj = t;
    }

    DrLockBox(DrLockBox<T>% t)
    {
        m_obj = t.m_obj;
    }

    DrLockBox<T>% operator=(T^ t)
    {
        return Set(t);
    }

    DrLockBox<T>% operator=(DrLockBox<T>% t)
    {
        return Set(t.m_obj);
    }

    bool operator== (DrLockBox<T>% t)
    {
        return (m_obj == t.m_obj);
    }

    bool IsNull()
    {
        return (m_obj == DrNull);
    }

    DrLockBox<T>% Set(T^ t)
    {
        m_obj = t;
        return *this;
    }

    operator DrLockBox<DrICritSec>()
    {
        return DrLockBox<DrICritSec>(m_obj);
    }

    bool IsEmpty()
    {
        return (m_obj == nullptr);
    }

    /* this is only to be used by the DrLockBoxKey class, and if there were friend classes
       in managed code this would be private */
    T^ DoNotUse()
    {
        return dynamic_cast<T^>(m_obj);
    }

private:
    T^   m_obj;
};

template <class T> ref class DrLockBoxKey
{
public:
    DrLockBoxKey(DrLockBox<T> box)
    {
        p = box.DoNotUse();
        this->Enter();
    }

    ~DrLockBoxKey()
    {
        this->Leave();
    }

    operator T^()
    {
        return p;
    }

    T^ operator->()
    {
        return p;
    }

    bool operator!()
    {
        return (p == nullptr);
    }

    bool operator==(T^ pT)
    {
        return (p == pT);
    }

    bool operator!=(T^ pT)
    {
        return (p != pT);
    }

private:
    T^ p;
};

#else

//#include <stdio.h>

DRBASECLASS(DrCritSec), public DrICritSec
{
public:
    DrCritSec()
    {
        InitializeCriticalSection(&m_critsec);
    }

    ~DrCritSec()
    {
        DeleteCriticalSection(&m_critsec);
    }

    virtual void Enter()
    {
//        printf("thread %u acquiring lock %p\n", GetCurrentThreadId(), this);
        EnterCriticalSection(&m_critsec);
//        printf("thread %u acquired lock %p\n", GetCurrentThreadId(), this);
    }

    virtual void Leave()
    {
        LeaveCriticalSection(&m_critsec);
//        printf("thread %u released lock %p\n", GetCurrentThreadId(), this);
    }

    virtual DrCritSecPtr GetBaseLock()
    {
        return this;
    }

private:
    CRITICAL_SECTION m_critsec;
};
DRREF(DrCritSec);

class DrAutoCriticalSection
{
public:
    DrAutoCriticalSection(DrICritSecPtr critSec)
    {
        m_critSec = critSec;
        m_critSec->Enter();
    }
    ~DrAutoCriticalSection()
    {
        m_critSec->Leave();
    }

private:
    /* ensure this can't be allocated on the heap: stack only! */
    void* operator new(size_t);

    DrICritSecIRef m_critSec;
};

template <class T> class DrLockBox
{
public:
    DrLockBox()
    {
    }

    DrLockBox(const DrLockBox<T>& t)
    {
        m_obj = t.m_obj;
    }

    DrLockBox(T* t)
    {
        m_obj = t;
    }

    DrLockBox<T>& operator=(T* t)
    {
        return Set(t);
    }

    DrLockBox<T>& operator=(const DrLockBox<T>& t)
    {
        return Set((T*) (t.m_obj));
    }

    bool operator== (const DrLockBox<T>& t)
    {
        return (m_obj == t.m_obj);
    }

    bool IsNull()
    {
        return (m_obj == DrNull);
    }

    DrLockBox<T>& Set(T* t)
    {
        m_obj = t;
        return *this;
    }

    operator DrLockBox<DrICritSec>()
    {
        return DrLockBox<DrICritSec>(m_obj);
    }

    bool IsEmpty()
    {
        return (m_obj == NULL);
    }

    /* this is only to be used by the DrLockBoxKey class, and if there were friend classes
       in managed code this would be private */
    T* DoNotUse()
    {
        return dynamic_cast<T*>((DrICritSecPtr) m_obj);
    }

private:
    DrInterfaceRef<T>   m_obj;
};

template <class T> class DrLockBoxKey
{
public:
    DrLockBoxKey(DrLockBox<T> box)
    {
        p = box.DoNotUse();
        p->Enter();
    }
    ~DrLockBoxKey()
    {
        p->Leave();
    }

    operator T*()
    {
        return p;
    }

    T* operator->()
    {
        return p;
    }

    bool operator!()
    {
        return (p == NULL);
    }

    bool operator==(T* pT)
    {
        return (p == pT);
    }

    bool operator!=(T* pT)
    {
        return (p != pT);
    }

private:
    /* ensure this can't be allocated on the heap: stack only! */
    void* operator new(size_t);

    DrInterfaceRef<T> p;
};

#endif

DRBASECLASS(DrSharedCritSec), public DrICritSec
{
public:
    DrSharedCritSec(DrICritSecPtr parent)
    {
        m_cs = parent;
    }

    virtual void Enter()
    {
        m_cs->Enter();
    }

    virtual void Leave()
    {
        m_cs->Leave();
    }

    virtual DrCritSecPtr GetBaseLock()
    {
        return m_cs->GetBaseLock();
    }

private:
    DrICritSecIRef    m_cs;
};
DRREF(DrSharedCritSec);
