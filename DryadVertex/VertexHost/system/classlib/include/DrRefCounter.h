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

#ifndef __DRYADREFCOUNTER_H__
#define __DRYADREFCOUNTER_H__

#pragma once

template<class Ty> __forceinline Ty& DrRemoveConst(const Ty& x)
{
    return const_cast<Ty&>(x);
}

template<class Ty> __forceinline Ty *DrRemovePtrConst(const Ty *pX)
{
    return const_cast<Ty *>(pX);
}

UInt64 GetUniqueObjectID();

// a type for automatically assigning a unique object ID for static initializers
class DrObjID
{
public:
    DrObjID() { _val = GetUniqueObjectID();}
    UInt64 Value() const { return _val; };
private:
    UInt64 _val;
};

/*
 * DrRefCounter
 *
 * Implements simple reference count functionality.
 * Derive any ref counted objects from this class.
 */

class DrRefCounter;

static const LONG k_lDecommissionedRefCount = (LONG)-1234;

class DrRefCountMonitor
{
public:
    virtual void OnRefCountChanged(const DrRefCounter *pCounter, void *pContext, LONG newCount, const char *pszReason) = 0;
};

class IDrRefCounter
{
public:
    virtual LONG IncRef() = 0;
    LONG IncRef() const
    {
        // IncRef/DecRef are allowed even for const instances
        return DrRemovePtrConst(this)->IncRef();
    }

    // called by weak reference pointers (under the shared lock) to IncRef this object.
    // returns TRUE if the IncRef could be performed, false if the object is already unreferenceable
    // The default implementation succeeds unless the current refcount is 0, on the assumption that the implementation
    // will clear all existing weak references after the refcount becomes 0 and before the object is deleted.
    virtual bool IncRefFromWeakReferenceLocked() = 0;
    bool IncRefFromWeakReferenceLocked() const
    {
        // IncRef/DecRef are allowed even for const instances
        return DrRemovePtrConst(this)->IncRefFromWeakReferenceLocked();
    }

    // called by weak reference Freememory implementations (under the shared lock) to get the refcount on this object.
    virtual LONG GetRefCountLocked() const = 0;

    virtual LONG DecRef() = 0;
    LONG DecRef() const
    {
        // IncRef/DecRef are allowed even for const instances
        return DrRemovePtrConst(this)->DecRef();
    }
    virtual void FreeMemory() = 0;

    void FreeMemory() const
    {
        // FreeMemory is allowed even for const instances
        DrRemovePtrConst(this)->FreeMemory();
    }
    
    virtual ~IDrRefCounter()
    {
    }
    virtual UInt64 GetOID() const = 0;  // Get object-id, an (almost) unique identifier that can be used for logging (this way we can see related log lines for a given object)
};


class DrOneInitializedVolatileLong
{
public:
   volatile LONG m_long;

   DrOneInitializedVolatileLong()
   {
        m_long = 1;
   }
};

class DrOneInitializedZeroDestroyedVolatileLong : public DrOneInitializedVolatileLong
{
public:
    ~DrOneInitializedZeroDestroyedVolatileLong()
    {
        LogAssert(m_long == 0 || m_long == k_lDecommissionedRefCount);
    }
};

class DrOneInitializedOneDestroyedVolatileLong : public DrOneInitializedVolatileLong
{
public:
    ~DrOneInitializedOneDestroyedVolatileLong()
    {
        LogAssert(m_long == 1 || m_long == k_lDecommissionedRefCount);
    }
};


#define DRREFCOUNTNOIMPL \
    public: \
        virtual LONG IncRef() override = 0; \
        virtual LONG DecRef() override = 0; \
        virtual UInt64 GetOID() const override = 0; \
        virtual bool IncRefFromWeakReferenceLocked() override = 0; \
        virtual LONG GetRefCountLocked() const override = 0; \
        virtual void FreeMemory() override = 0; \


// NOTE: the following could use DrOneInitializedZeroDestroyedVolativeLong, but some people create static instances of this...
#define DRREFCOUNTIMPL_NOFREEMEMORY_BASE(InterfaceClass) \
    protected: \
        mutable DrOneInitializedVolatileLong m_iRefCount; \
        DrObjID m_oid; \
    public: \
        virtual LONG IncRef() override\
        { \
            LONG i; \
            i = InterlockedIncrement (&(m_iRefCount.m_long)); \
            LogAssert (i > 1); \
            return i; \
        } \
        virtual LONG DecRef() override\
        { \
            LONG i; \
            i = InterlockedDecrement (&(m_iRefCount.m_long)); \
            if (i <= 0) \
            { \
                LogAssert (i == 0); \
                FreeMemory (); \
            } \
            return i; \
        } \
        virtual UInt64 GetOID() const override{ return m_oid.Value(); } \
        virtual bool IncRefFromWeakReferenceLocked() override\
        { \
            if (m_iRefCount.m_long == 0) { \
                return false; \
            } \
            LONG i = InterlockedIncrement (&(m_iRefCount.m_long)); \
            LogAssert (i > 1); \
            return true; \
        } \
        virtual LONG GetRefCountLocked() const override\
        { \
            return m_iRefCount.m_long; \
        } \


#define DRREFCOUNTIMPL_STATIC_BASE(InterfaceClass) \
    private: \
        static void * operator new( size_t) { LogAssert(false); return NULL; } \
        static void operator delete( void *) { LogAssert(false); } \
    protected: \
        mutable DrOneInitializedOneDestroyedVolatileLong m_iRefCount; \
        DrObjID m_oid; \
    public: \
        virtual LONG IncRef() override\
        { \
            LONG i; \
            i = InterlockedIncrement (&(m_iRefCount.m_long)); \
            LogAssert (i > 1); \
            return i; \
        } \
        virtual LONG DecRef() override\
        { \
            LONG i; \
            i = InterlockedDecrement (&(m_iRefCount.m_long)); \
            LogAssert(i > 0); \
            return i; \
        } \
        virtual UInt64 GetOID() const override{ return m_oid.Value(); } \
        virtual bool IncRefFromWeakReferenceLocked() override \
        { \
            LONG i = InterlockedIncrement (&(m_iRefCount.m_long)); \
            LogAssert (i > 1); \
            return true; \
        } \
        virtual LONG GetRefCountLocked() const override\
        { \
            return m_iRefCount.m_long; \
        } \


#define DRREFCOUNTIMPL_NOFREEMEMORY DRREFCOUNTIMPL_NOFREEMEMORY_BASE(IDrRefCounter)

#define DRREFCOUNTIMPL_BASE(InterfaceClass) \
        DRREFCOUNTIMPL_NOFREEMEMORY_BASE(InterfaceClass) \
    protected: \
        virtual void FreeMemory() override\
        { \
            delete this; \
        }


#define DRREFCOUNTIMPL DRREFCOUNTIMPL_BASE(IDrRefCounter)
#define DRREFCOUNTIMPL_STATIC DRREFCOUNTIMPL_STATIC_BASE(IDrRefCounter)

class DrRefCounter : public IDrRefCounter
{
protected:
    mutable volatile LONG m_iRefCount;
    DrRefCountMonitor *m_pMonitor;
    void *m_pMonitorContext;
    UInt64  m_oid;

    DrRefCounter()
    {
        m_iRefCount = 1;
        m_pMonitor = NULL;
        m_pMonitorContext = NULL;
        m_oid = GetUniqueObjectID();
    }


    virtual ~DrRefCounter()
    {
        LogAssert (m_iRefCount == 0 || m_iRefCount == k_lDecommissionedRefCount);
    }

    // Called when the refcount becomes zero.
    virtual void FreeMemory() override
    {
        delete this;
    }

public:

    // not threadsafe
    void SetRefCountMonitor(DrRefCountMonitor *pMonitor, void *pContext = NULL)
    {
        m_pMonitor = pMonitor;
        m_pMonitorContext = pContext;
        if (m_pMonitor != NULL) {
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, m_iRefCount, "SetMonitor");
        }
    }

    virtual LONG IncRef() override
    {
        LONG i;
        i = InterlockedIncrement (&m_iRefCount);
        LogAssert (i > 1);
        if (m_pMonitor != NULL) {
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, i, "IncRef");
        }
        return i;
    }

    // Increments without checking that the previous refcount was > 0. Used for special circumstances where we
    // may temporarily increment from a 0 refcount
    LONG IncRefNoCheck()
    {
        LONG i;
        i = InterlockedIncrement (&m_iRefCount);
        if (m_pMonitor != NULL) {
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, i, "IncRefNoCheck");
        }
        return i;
    }

    // called by weak reference pointers (under the shared lock) to IncRef this object.
    // returns TRUE if the IncRef could be performed, false if the object is already unreferenceable
    // The default implementation succeeds unless the current refcount is 0, on the assumption that the implementation
    // will clear all existing weak references after the refcount becomes 0 and before the object is deleted.
    virtual bool IncRefFromWeakReferenceLocked() override
    {
        if (m_iRefCount == 0) {
            return false;
        }
        LONG i = InterlockedIncrement (&m_iRefCount);
        LogAssert (i > 1); \
        if (m_pMonitor != NULL) {
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, i, "IncRefFromWeakReferenceLocked");
        }
        return true;
    }

    virtual LONG GetRefCountLocked() const override
    {
        return m_iRefCount;
    }

    virtual LONG DecRef() override
    {
        LONG i;
        if (m_pMonitor != NULL) {
            // HACK: the refcount may be wrong, but it will work
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, m_iRefCount-1L, "DecRef");
        }
        i = InterlockedDecrement (&m_iRefCount);
        if (i <= 0)
        {
            LogAssert (i == 0);
            FreeMemory ();
        }
        return i;
    }

    // Decrements without special handling when the refcount becomes zero. Used for special circumstances
    // where the refcount has been temporarily incremented from zero and another context will be
    // calling FreeMemory().
    LONG DecRefNoFree()
    {
        LONG i;
        if (m_pMonitor != NULL) {
            // HACK: the refcount may be wrong, but it will work
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, m_iRefCount-1L, "DecRefNoFree");
        }
        i = InterlockedDecrement (&m_iRefCount);
        LogAssert(i >= 0);
        return i;
    }

    virtual UInt64 GetOID() const override
    {
        return m_oid;
    }


    void ResetRefCounter()
    {
        LONG i;
        i = InterlockedExchange (&m_iRefCount, 1);
        if ( i != 0 )
        {
            DrLogE( "DecRef error - refCount=%ld, oid=%I64x",  i, GetOID() );
            LogAssert (i == 0);
        }
        if (m_pMonitor != NULL) {
            m_pMonitor->OnRefCountChanged(this, m_pMonitorContext, 1, "ResetRefCounter");
        }
    }

    // Abandons the refcounter. Used when a subclass overrides the implementation. Prevents the refcounter from being used, and
    // prevents an assertion failure when the object is destructed.
    void DecommissionRefCounter()
    {
        LONG i;
        // We set a magic number to indicate that it is decommissioned. Any call to IncRef or DecRef will assert, and destruction will succeed
        i = InterlockedExchange (&m_iRefCount, k_lDecommissionedRefCount);
        LogAssert(i == 1 || i == k_lDecommissionedRefCount);
    }

    // The value returned by this method is not stable unless the caller provides another mechanism for guaranteeing that
    // noone calls IncRef or DecRef.
    LONG GetRefCount() const
    {
        return m_iRefCount;
    }
};

template <class T> class DrRef
{

public:
    DrRef()
    {
        p = NULL;
    }

    DrRef(T* lp)
    {
        p = lp;

        if (p != NULL) {
            DrRemovePtrConst(p)->IncRef();
        }
    }

    explicit DrRef(const DrRef<T>& ref)
    {
        p = ref.p;

        if (p != NULL) {
            DrRemovePtrConst(p)->IncRef();
        }
    }

    ~DrRef()
    {
        if (p != NULL) {
            DrRemovePtrConst(p)->DecRef();
            p = NULL;   // Make sure we AV in case someone is using DrRef after DecRef
        }
    }

    operator T*() const
    {
        return p;
    }

    T& operator*() const
    {
        return *p;
    }

    T* operator->() const
    {
        return p;
    }

    bool operator!() const
    {
        return (p == NULL);
    }

    bool operator<(T* pT) const
    {
        return (p < pT);
    }

    bool operator>(T* pT) const
    {
        return (p < pT);
    }

    bool operator<=(T* pT) const
    {
        return (p <= pT);
    }

    bool operator>=(T* pT) const
    {
        return (p < pT);
    }

    bool operator==(T* pT) const
    {
        return (p == pT);
    }

    bool operator!=(T* pT) const
    {
        return (p != pT);
    }

    DrRef& Set(T* lp)
    {
        if (p != lp) {
            if (lp != NULL) {
                DrRemovePtrConst(lp)->IncRef();
            }

            if (p != NULL) {
                DrRemovePtrConst(p)->DecRef();
            }

            p = lp;
        }

        return *this;
    }

    DrRef& operator=(T* lp)
    {
        return Set(lp);
    }

    DrRef& operator=(const DrRef<T>& ref)
    {
        return Set(ref.p);
    }

    // Release the interface and set to NULL
    void Release()
    {
        T* pTemp = p;
        if (pTemp != NULL) {
            p = NULL;
            DrRemovePtrConst(pTemp)->DecRef();
        }
    }

    //
    // Attach to an existing interface (does not IncRef)
    //
    void Attach(T* p2)
    {
        //
        // Remove reference to previous interface
        //
        if (p != NULL) 
        {
            DrRemovePtrConst(p)->DecRef();
        }

        //
        // Update current interface
        //
        p = p2;
    }

    // Detach the interface (does not DecRef)
    T* Detach()
    {
        T* pt = p;
        p = NULL;
        return pt;
    }

    void TransferFrom( DrRef<T>& source)
    {
        Attach(source.Detach());
    }

    template<class T2> void TransferFrom( DrRef<T2>& source)
    {
        T2 *p2 = source.Detach();
        if (p != NULL) {
            DrRemovePtrConst(p)->DecRef();
        }
        if (p2== NULL) {
            p = NULL;
        } else {
            p = dynamic_cast<T *>(p2);
            LogAssert(p != NULL);
        }
    }

    T* Ptr() const
    {
        return p;
    }

private:
    T* p;
};

// Growable vector of DrRef smart pointers to arbitrary refcounted typed items
// Insertions and deletions can be performed both at the head and at the tail of the list, making it suitable for queues.
template<class t> class DrRefList
{
public:    
    DrRefList()
    {
        m_nEntries = 0;
        m_nAllocated = 0;
        m_prgEntries = NULL;
    }

    ~DrRefList()
    {
        if (m_prgEntries != NULL) {
            delete[] m_prgEntries;
            m_prgEntries = NULL;
        }
        m_nEntries = 0;
        m_nAllocated = 0;
    }

    // forces the buffer to be reallocated with the given size, even if it
    // is the same as the current size.
    // the requested size must be big enough to hold the valid entries.
    // On exit, the valid entries are always contiguous starting at offset 0
    // if n==0, frees the buffer
    void ForceRealloc(::UInt32 n)
    {
        LogAssert(n >= m_nEntries);
        DrRef<t> *pnew = NULL;
        if (n != 0) {
            pnew = new DrRef<t>[n];
            LogAssert(pnew != NULL);
            ::UInt32 uFrom = m_uFirstEntry;
            for (::UInt32 i = 0; i < m_nEntries; i++) {
                pnew[i].TransferFrom(m_prgEntries[uFrom++]);
                if (uFrom >= m_nAllocated) {
                    uFrom = 0;
                }
            }
        }
        if (m_prgEntries != NULL) {
            delete[] m_prgEntries;
        }
        m_prgEntries = pnew;
        m_nAllocated = n;
        m_uFirstEntry = 0;
    }

    // reallocates the buffer if there are not at least n elements allocated    
    void GrowTo(::UInt32 n)
    {
        if (n > m_nAllocated) {
            if (n < 2 * m_nAllocated) {
                n = 2 * m_nAllocated;
            }
            if (n < 20) {
                n = 20;
            }
            ForceRealloc(n);
        }
    }

    // Converts a potentially wrapped list (if you have moved the head) into a
    // contiguous list, and returns a pointer to the first item in the contiguous list
    // If possible, nothing is moved. If the list is wrapped, a new buffer is allocated (easier than moving everything in a full list)
    // This operation is always cheap if you never remove from or add to the head.
    DrRef<t> *MakeContiguous()
    {
        if (m_uFirstEntry + m_nEntries > m_nAllocated) {
            ForceRealloc(m_nEntries);
        }

        return m_prgEntries + m_uFirstEntry;
    }

    UInt32 NumEntries() const
    {
        return m_nEntries;
    }

    UInt32 NumAllocated() const
    {
        return m_nAllocated;
    }
    
    DrRef<t>& EntryAt(UInt32 index)
    {
        LogAssert(index < m_nEntries);
        return m_prgEntries[NormalizeEntryIndex(index)];
    }

    const t *ConstEntryAt(UInt32 index) const
    {
        LogAssert(index < m_nEntries);
        return m_prgEntries[NormalizeEntryIndex(index)];
    }

    const t *EntryAt(UInt32 index) const
    {
        ConstEntryAt(index);
    }

    DrRef<t>& operator[](UInt32 index)
    {
        return EntryAt(index);
    }

    const t *operator[](UInt32 index) const
    {
        return ConstEntryAt(index);
    }

    // returns NULL if list is empty
    t *Head()
    {
        if (m_nEntries == 0) {
            return NULL;
        }
        return m_prgEntries[m_uFirstEntry];
    }

    // returns NULL if list is empty
    const t *Head() const
    {
        if (m_nEntries == 0) {
            return NULL;
        }
        return m_prgEntries[m_uFirstEntry];
    }

    // returns NULL if list is empty
    t *Tail()
    {
        if (m_nEntries == 0) {
            return NULL;
        }
        return EntryAt(m_nEntries-1);
    }

    // returns NULL if list is empty
    const t *Tail() const
    {
        if (m_nEntries == 0) {
            return NULL;
        }
        return EntryAt(m_nEntries-1);
    }

    // Invalidates all entry references and pointers previously returned
    DrRef<t>& AddEntryToTail(t *pNewEntry = NULL)
    {
        GrowTo(m_nEntries+1);
        DrRef<t>& newEntry = m_prgEntries[NormalizeEntryIndex(m_nEntries++)];
        newEntry = pNewEntry;
        return newEntry;
    }

    // Invalidates all entry references and pointers previously returned
    DrRef<t>& AddEntryToHead(t *pNewEntry = NULL)
    {
        GrowTo(m_nEntries+1);
        if (m_uFirstEntry == 0) {
            m_uFirstEntry = m_nAllocated - 1;
        } else {
            m_uFirstEntry--;
        }
        m_nEntries++;
        DrRef<t>& newEntry = m_prgEntries[m_uFirstEntry];
        newEntry = pNewEntry;
        return newEntry;
    }

    // Invalidates all entry references and pointers previously returned
    DrRef<t>& AddEntry(t *pNewEntry = NULL)
    {
        return AddEntryToTail(pNewEntry);
    }

    void RemoveEntryFromTail(DrRef<t> *pValOut)
    {
        LogAssert(m_nEntries != 0);
        pValOut->TransferFrom(m_prgEntries[NormalizeEntryIndex(--m_nEntries)]);
    }

    void RemoveEntryFromHead(DrRef<t> *pValOut)
    {
        LogAssert(m_nEntries != 0);
        pValOut->TransferFrom(m_prgEntries[m_uFirstEntry++]);
        if (m_uFirstEntry >= m_nAllocated) {
            m_uFirstEntry = 0;
        }
        m_nEntries--;
    }


    void Clear()
    {
        ::UInt32 uIndex = m_uFirstEntry;
        for (::UInt32 i = 0; i < m_nEntries; i++) {
            m_prgEntries[uIndex++] = NULL;
            if (uIndex >= m_nAllocated) {
                uIndex = 0;
            }
        }
        m_nEntries = 0;
        m_uFirstEntry = 0; // might as well reset to the beginning of the array
    }

    void AddNullEntriesToTail(UInt32 numNulls)
    {
        if (numNulls != 0)
        {
            // Depends on the fact that all unused entries are NULL
            LogAssert(m_nEntries + numNulls > m_nEntries);   // Check for overflow
            GrowTo(m_nEntries + numNulls);
            m_nEntries += numNulls;
        }
    }

    typedef int (__cdecl *PDRREF_COMPARE_FUNCTION)(void *context, t *p1, t *p2);

    typedef struct {
        PDRREF_COMPARE_FUNCTION compare;
        void *context;
    } DrRefSortContext;

    static int __cdecl InternalDrRefCompare(void *context, const void *p1, const void *p2)
    {
        DrRefSortContext *pSortContext = (DrRefSortContext *)context;
        return (*pSortContext->compare)(pSortContext->context, *(DrRef<t> *)p1, *(DrRef<t> *)p2);
    }
    
    // performs a quicksort on the list, with a user-provided compare function
    // Invalidates all entry references and pointers previously returned
    void Sort(PDRREF_COMPARE_FUNCTION compare, void * context = NULL)
    {
        if (m_nEntries > 1) {
            DrRef<t> *pFirst = MakeContiguous();
            DrRefSortContext ctx;
            ctx.compare = compare;
            ctx.context = context;
            qsort_s(pFirst, (size_t)m_nEntries, sizeof(*pFirst), InternalDrRefCompare, &ctx);
        }
    }

protected:
    UInt32 NormalizeEntryIndex(UInt32 index) const
    {
        LogAssert(index < m_nAllocated);
        if (m_uFirstEntry != 0) {
            index += m_uFirstEntry;
            if (index >= m_nAllocated) {
                index -= m_nAllocated;
            }
        }
        return index;
    }

protected:    
    UInt32 m_uFirstEntry;             // Normally 0, the index of the first entry (for circular buffers)
    
private:
    UInt32 m_nEntries;
    UInt32 m_nAllocated;
    DrRef<t> *m_prgEntries;
};

#endif  //end if not defined __DRYADREFCOUNTER_H__
