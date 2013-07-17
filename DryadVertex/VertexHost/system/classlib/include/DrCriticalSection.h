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

/*
 * DrCriticalSection
 *
 * Defines a utility class for wrapping a critical section
 */
#pragma once


#define DEFAULT_SPIN_COUNT 4000u
#define DEFAULT_DR_LOG_HELD_TOO_LONG_TIMEOUT (500 * DrTimeInterval_Millisecond)

class DrCriticalSectionBase : public CRITICAL_SECTION
{
public:
    void Init(
        __in PCSTR name = NULL,
        DWORD spinCount = DEFAULT_SPIN_COUNT,
        bool logUsage = false,
        DrTimeInterval logHeldTooLongTimeout = DEFAULT_DR_LOG_HELD_TOO_LONG_TIMEOUT
        );

    void Uninit()
    {
        DebugLogAssert( RecursionCount==0 && OwningThread == 0);
        DeleteCriticalSection( this );
    }

    void Enter( PCSTR functionName = NULL, PCSTR fileName = NULL, UINT lineNumber = 0 );

    void Leave( PCSTR functionName = NULL, PCSTR fileName = NULL, UINT lineNumber = 0 );

    bool Acquired() const
    {
        return RecursionCount > 0 && (DWORD)(DWORD_PTR)OwningThread == GetCurrentThreadId();
    }

    // TODO: deprecate this, misspelled
    bool Aquired() const
    {
        return RecursionCount > 0 && (DWORD)(DWORD_PTR)OwningThread == GetCurrentThreadId();
    }

    // Returns 0 if this thread does not own the critical section
    UInt32 GetRecursionCountIfAcquired()
    {
        return (RecursionCount > 0 && (DWORD)(DWORD_PTR)OwningThread == GetCurrentThreadId()) ? (UInt32)RecursionCount : 0 ;
    }

    // also returns true if n is 0 and the thread does not own the critical section
    bool AcquiredExactNumberOfTimes(UInt32 n)
    {
        return (GetRecursionCountIfAcquired() == n);
    }

    bool AcquiredExactlyOnce()
    {
        return RecursionCount == 1 && (DWORD)(DWORD_PTR)OwningThread == GetCurrentThreadId();
    }
    
    bool NotAcquired() const
    {
        return (DWORD)(DWORD_PTR)OwningThread != GetCurrentThreadId();
    }

    // TODO: deprecate this, misspelled
    bool NotAquired() const
    {
        return (DWORD)(DWORD_PTR)OwningThread != GetCurrentThreadId();
    }

    // returns the old spin count
    DWORD SetSpinCount(DWORD spinCount)
    {
        return SetCriticalSectionSpinCount(this, spinCount);
    }

    // Could cause inconsistent settings if called concurrently. If you might be calling this
    // method concurrently, you should claim the lock before calling. Since in most cases
    // this is called before the lock is actually used, this precaution is
    // not taken by default.
    void SetCriticalSectionLoggingParameters(
        bool logUsage = true,
        DrTimeInterval logHeldTooLongTimeout = DEFAULT_DR_LOG_HELD_TOO_LONG_TIMEOUT
    );

    void SetCriticalSectionLogging(
        bool logUsage = true
    );

    void SetCriticalSectionLogHeldTooLongTimeout(
        DrTimeInterval logHeldTooLongTimeout = DEFAULT_DR_LOG_HELD_TOO_LONG_TIMEOUT
    );


    void SetCriticalSectionName(__in   PCSTR name)
    {
        _name = name;
    }
    
    bool TryEnter( PCSTR functionName = NULL, PCSTR fileName = NULL, UINT lineNumber = 0)
    {
        BOOL entered = TryEnterCriticalSection(this);

        if ( entered )
        {
            _lastFunctionName = functionName;
            _lastFileName = fileName;
            _lastLineNumber = lineNumber;
        }

        if ( _logUsage )
        {
            if ( entered )
            {
                if (fileName != NULL) {
                    DrLogD( "CritSect TRY ENTER OK, %s at %s %s(%u), addr=%08Ix",
                        _name, functionName, fileName, lineNumber, this );
                } else {
                    DrLogD( "CritSect TRY ENTER OK, %s, addr=%08Ix",
                        _name, this );
                }
            }
            else
            {
                if (fileName != NULL) {
                    DrLogW( "CritSect TRY ENTER FAILED, %s at %s %s(%u), addr=%08Ix",
                        _name, functionName, fileName, lineNumber, this );
                } else {
                    DrLogW( "CritSect TRY ENTER FAILED, %s, addr=%08Ix",
                        _name, this );
                }
            }
        }

        return entered != FALSE;
    }

    PCSTR   _name;
    bool    _logUsage;
    DWORD   _logHeldTooLongTimeoutMs;
    PCSTR   _lastFunctionName;
    PCSTR   _lastFileName;
    UINT    _lastLineNumber;
    DWORD   _enterTimeMs;
};


class DrCriticalSection : public DrCriticalSectionBase
{
public:

    DrCriticalSection(
        PCSTR name = NULL,
        DWORD spinCount = DEFAULT_SPIN_COUNT,
        bool logUsage = false,
        UINT logHeldTooLongTimeout = DEFAULT_DR_LOG_HELD_TOO_LONG_TIMEOUT
        )
    {
        Init( name, spinCount, logUsage, logHeldTooLongTimeout );
    }

    ~DrCriticalSection()
    {
        Uninit();
    }

};


class AutoLock
{
    friend class AutoLockLogged;

public:

    AutoLock()
    {
        _lock = NULL;
    }

    AutoLock( DrCriticalSectionBase* lock )
    {
        _lock = lock;
        _lock->Enter();
    }

    AutoLock( DrCriticalSectionBase& lock )
    {
        _lock = &lock;
        _lock->Enter();
    }

    AutoLock( DrCriticalSectionBase* lock, PCSTR functionName, PCSTR fileName, UINT lineCount )
    {
        _lock = lock;
        _lock->Enter( functionName, fileName, lineCount );
    }

    ~AutoLock()
    {
        if ( _lock != NULL )
        {
            _lock->Leave();
        }
    }

    void Enter( DrCriticalSectionBase* newLock = NULL )
    {
        if ( newLock != NULL )
        {
            LogAssert( _lock == NULL );
            _lock = newLock;
        }
        LogAssert( _lock != NULL );
        _lock->Enter();
    }

    void Leave()
    {
        LogAssert( _lock != NULL );
        _lock->Leave();
        _lock = NULL;
    }

private:

    AutoLock( const AutoLock& );
    AutoLock& operator=( const AutoLock& );

    DrCriticalSectionBase* _lock;

};

typedef AutoLock DrScopedCritSec;
typedef AutoLock DrAutoCriticalSection;

#define DR_ENTER(lock) (lock).Enter( __FUNCTION__, __FILE__, __LINE__ )
#define DR_LEAVE(lock) (lock).Leave( __FUNCTION__, __FILE__, __LINE__ )
#define DR_ENTER_NOLOG(lock) (lock).Enter()
#define DR_LEAVE_NOLOG(lock) (lock).Leave()

#define EnterAndLog() Enter( __FUNCTION__, __FILE__, __LINE__ )
#define LeaveAndLog() Leave( __FUNCTION__, __FILE__, __LINE__ )

#define LOCK_JOIN_NAME2(a, b, c) a##_line_##b##_counter_##c
#define LOCK_JOIN_NAME(a, b, c) LOCK_JOIN_NAME2(a, b, c)
#define LOCK(lock) AutoLock LOCK_JOIN_NAME(autoLock, __LINE__, __COUNTER__) (&lock, __FUNCTION__, __FILE__, __LINE__);
#define LOCK_NOLOG(lock) AutoLock LOCK_JOIN_NAME(autoLock, __LINE__, __COUNTER__) (&lock);

#define DR_IN_CRITSEC(cs) LOCK(cs)

#define DRLOCKABLENOIMPL \
    public: \
        virtual void Lock() override = 0; \
        virtual void Lock() const override = 0; \
        virtual void Unlock() override = 0; \
        virtual void Unlock() const override = 0; \

#define DRLOCKABLEIMPL_NO_LOCK \
    public: \
        virtual void Lock() override\
        { \
        } \
        void Lock() const override \
        { \
        } \
        virtual void Unlock() override \
        { \
        } \
        virtual void Unlock() const override \
        { \
        }

#define DRLOCKABLEIMPL_PROTO \
    public: \
        virtual void Lock(); \
        virtual void Lock() const; \
        virtual void Unlock(); \
        virtual void Unlock() const; \

#define DRLOCKABLEIMPL_BASE(InterfaceClass) \
    protected: \
        mutable DrCriticalSection m_cs; \
    public: \
        virtual void Lock() override \
        { \
            m_cs.Enter(); \
        } \
        virtual void Lock() const override \
        { \
            m_cs.Enter(); \
        } \
        virtual void Unlock() override \
        { \
            m_cs.Leave(); \
        } \
        virtual void Unlock() const override \
        { \
            m_cs.Leave(); \
        }

#define DRLOCKABLEIMPL_DELEGATE(pImpl) \
    public: \
        virtual void Lock() override \
        { \
            pImpl->Lock(); \
        } \
        virtual void Lock() const override \
        { \
            pImpl->Lock(); \
        } \
        virtual void Unlock() override \
        { \
            pImpl->Unlock(); \
        } \
        virtual void Unlock() const override \
        { \
            pImpl->Unlock(); \
        }

#define DRLOCKABLEIMPL_DELEGATE_OUTSIDE_CLASS(classname, pImpl) \
    void classname::Lock() \
    { \
        pImpl->Lock(); \
    } \
    void classname::Lock() const \
    { \
        pImpl->Lock(); \
    } \
    void classname::Unlock() \
    { \
        pImpl->Unlock(); \
    } \
    void classname::Unlock() const \
    { \
        pImpl->Unlock(); \
    }


#define DRLOCKABLEIMPL DRLOCKABLEIMPL_BASE(IDrLockable)

class IDrLockable
{
public:
    virtual void Lock() = 0;
    virtual void Lock() const = 0;
    virtual void Unlock() = 0;
    virtual void Unlock() const = 0;
    virtual ~IDrLockable()
    {
    }
};

class DrLockable : public IDrLockable
{
    DRLOCKABLEIMPL
};

template<class T> class DrScopedLock
{
public:
    DrScopedLock(T *pT = NULL)
    {
        m_pT = pT;
        if (pT != NULL) {
            pT->Lock();
        }
    }

    ~DrScopedLock()
    {
        if (m_pT != NULL) {
            m_pT->Unlock();
        }
    }

private:
    T *m_pT;

};

typedef DrScopedLock<const IDrLockable> DrScopedLockable;

extern DrCriticalSection *g_pDrGlobalCritSec; // A general shared critical section that is intialized on first use or at static constructor time

extern DrCriticalSection *DrInitializeGlobalCritSec();

__inline DrCriticalSection *DrGetGlobalCritSec()
{
    if (g_pDrGlobalCritSec == NULL) {
        DrInitializeGlobalCritSec();
    }
    return g_pDrGlobalCritSec;
}

__inline void DrEnterGlobalCritSec()
{
    if (g_pDrGlobalCritSec == NULL) {
        DrInitializeGlobalCritSec();
    }
#pragma prefast(push)
#pragma prefast(disable:6011)
    g_pDrGlobalCritSec->Enter();
#pragma prefast(pop)    
}

__inline void DrLeaveGlobalCritSec()
{
    g_pDrGlobalCritSec->Leave();
}

// A scoped lock of the singleton global cosmos critical section
class DrScopedGlobalLock
{
public:
    DrScopedGlobalLock()
    {
        DrEnterGlobalCritSec();
    }

    ~DrScopedGlobalLock()
    {
        DrLeaveGlobalCritSec();
    }
};

// put this in your header file to declare a static singleton lock that is never destructed and can be
// used within static initializers
#define DECLARE_STATIC_LOCK(name) \
    class name \
    { \
    public: \
        static DrCriticalSection *InitialGetCritSec() \
        { \
            DrScopedGlobalLock glock; \
            if (s_pCritSec == NULL) { \
                s_pCritSec = new DrCriticalSection(); \
            } \
            return s_pCritSec; \
        } \
        __forceinline static DrCriticalSection *GetCritSec() \
        { \
            if (s_pCritSec == NULL) { \
                InitialGetCritSec(); \
            } \
            return s_pCritSec; \
        } \
        static void Lock() \
        { \
            GetCritSec()->Enter(); \
        } \
        static void Unlock() \
        { \
            GetCritSec()->Enter(); \
        } \
    private: \
        static DrCriticalSection *s_pCritSec; \
    }; \

// Put this in your cpp file to provide a definition for a static singleton lock that is never destructed and can be
// used within static initializers.
#define DEFINE_STATIC_LOCK(name) \
    DrCriticalSection * name::s_pCritSec = NULL;

// A scoped lock of a singleton static critical section that is never destructed and can be
// used within static initializers.
template<class LockName> class DrScopedStaticLock
{
public:
    DrScopedStaticLock()
    {
        LockName::Lock();
    }

    ~DrScopedStaticLock()
    {
        LockName::Unlock();
    }
};

#define DRLOCKABLEIMPL_DELEGATE_TO_STATIC(LockName) \
    public: \
        virtual void Lock() override \
        { \
            LockName::Lock(); \
        } \
        virtual void Lock() const override \
        { \
            LockName::Lock(); \
        } \
        virtual void Unlock() override \
        { \
            LockName::Unlock(); \
        } \
        virtual void Unlock() const override \
        { \
            LockName::Unlock(); \
        }


