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

#include <DrCommon.h>

class DObjPoolBase;

class DObjPoolCache;

/* the DObjPoolThreadPrivateBlock is only ever read or modified by the
   thread which owns it, so needs no synchronization on any of its
   members */
class DObjPoolThreadPrivateBlock
{
public:
    DObjPoolThreadPrivateBlock();
    ~DObjPoolThreadPrivateBlock();

    DObjPoolCache* LookUpPoolCache(DObjPoolBase* pool, LONGLONG poolKey);
    void AddPoolCache(DObjPoolBase* pool, LONGLONG poolKey,
                      DObjPoolCache* cache);
    void GarbageCollectCaches();

private:
    class Entry
    {
    public:
        Entry();
        ~Entry();
        void Initialize(DObjPoolBase* pool, LONGLONG poolKey,
                        DObjPoolCache* cache);

        DObjPoolBase*       m_pool;
        LONGLONG            m_key;
        DObjPoolCache*      m_cache;
    };

    UInt32       m_entryArraySize;
    UInt32       m_numberOfEntries;
    Entry*       m_entry;
};

/* the DObjPoolCache is a thread-local pool of cached objects from the
   pool. It is generally only accessed by the local thread, so most
   methods need no synchronization. During pool cleanup it can be
   accessed by another thread which is holding the pool's lock.
   Correctly written code will ensure that pool cleanup will never
   occur unless all outstanding pool objects have been handed back,
   and this handback must be communicated to the cleanup thread using
   correctly synchronized operations. This ensures that no object can
   be added to or removed from the cache once cleanup begins. After
   cleanup, Abandoned() will return true, and the thread-local code
   may subsequently garbage-collect the cache.
*/
class DObjPoolCache
{
public:
    DObjPoolCache(UInt32 maxEntries, UInt32 keepEntryCount,
                  DObjPoolBase* pool);
    ~DObjPoolCache();

    bool Abandoned();

    void InsertObject(void* o);
    void* RemoveObject();

private:
    /* this is the only method which can be called on a thread other
       than the local owner thread, and this only happens during
       cleanup */
    void ReturnToPool(bool finalCleanup);

    bool            m_abandoned;
    DObjPoolBase*   m_pool;
    UInt32          m_maxEntries;
    UInt32          m_keepEntryCount;
    UInt32          m_numberOfEntries;
    void**          m_array;

    friend class DObjPoolBase;
};

class DObjFactoryBase : public IDrRefCounter
{
public:
    virtual void* AllocateObjectUntyped() = 0;
    virtual void FreeObjectUntyped(void* object) = 0;
};

typedef DrRef<DObjFactoryBase> DObjFactoryRef;

class DObjPoolBase : public DObjFactoryBase
{
public:
    DObjPoolBase(DObjFactoryBase* factory,
                 UInt32 maxCentralEntries,
                 UInt32 maxLocalEntries, UInt32 localKeepEntryCount);
    ~DObjPoolBase();

    void RemoveObjects(void** dst, UInt32 countNeeded);
    void AcceptObjects(void** src, UInt32 count);

    void* AllocateObjectUntyped();
    void FreeObjectUntyped(void* object);

protected:
    DObjFactoryBase* DetachFactory();

private:
    DObjPoolCache* MakeCache();
    DObjPoolCache* FetchPrivateCache();

    DrRef<DObjFactoryBase>   m_factory;
    UInt32                   m_maxCentralEntries;
    UInt32                   m_maxLocalEntries;
    UInt32                   m_localKeepEntryCount;

    UInt32                   m_numberOfCentralEntries;
    void**                   m_array;

    UInt32                   m_cacheArraySize;
    UInt32                   m_numberOfCaches;
    DObjPoolCache**          m_cache;

    UInt64                   m_totalGivenOut;
    UInt64                   m_totalReturned;
    UInt64                   m_totalAllocated;
    UInt64                   m_totalFreed;

    LONGLONG                 m_key;
    CRITSEC                  m_atomic;
};

template< class T_ > class DrRefFactory : public DObjFactoryBase
{
public:
    virtual ~DrRefFactory() {}
    virtual void AllocateObject(DrRef<T_>* pObject) = 0;
    virtual void FreeObject(DrRef<T_>& object)
    {
        object = NULL;
    }

private:
    void* AllocateObjectUntyped()
    {
        DrRef<T_> typedObject;
        AllocateObject(&typedObject);
        return typedObject.Detach();
    }

    void FreeObjectUntyped(void* object)
    {
        DrRef<T_> typedObject;
        typedObject.Attach((T_ *) object);
        FreeObject(typedObject);
    }
};

template< class T_ > class StdRefPoolFactory : public DrRefFactory<T_>
{
public:
    StdRefPoolFactory() {}
    StdRefPoolFactory(DObjPoolBase* pool)
    {
        InitializePoolFactory(pool);
    }

    void InitializePoolFactory(DObjPoolBase* pool)
    {
        m_pool = pool;
    }

    void AllocateObject(DrRef<T_>* pObject)
    {
        pObject->Attach(new T_(m_pool));
    }

    void FreeObject(DrRef<T_>& object)
    {
        object.Detach()->PoolFreeMemory();
    }

private:
    DrRef<DObjPoolBase>  m_pool;

    DRREFCOUNTIMPL
};

template< class T_ > class DrRefPool : public DObjPoolBase
{
public:
    DrRefPool(DrRefFactory<T_>* factory,
              UInt32 maxCentralEntries,
              UInt32 maxLocalEntries, UInt32 localKeepEntryCount) :
        DObjPoolBase(factory, maxCentralEntries,
                     maxLocalEntries, localKeepEntryCount)
    {
    }

    void InsertObject(DrRef<T_>& object)
    {
        FreeObjectUntyped(object.Detach());
    }

    void RemoveObject(DrRef<T_>* pObject)
    {
        pObject->Attach((T_ *) AllocateObjectUntyped());
    }

    DRREFCOUNTIMPL
};

template< class T_ > class StdRefPool : public DObjPoolBase
{
public:
    StdRefPool(UInt32 maxCentralEntries,
               UInt32 maxLocalEntries, UInt32 localKeepEntryCount) :
        DObjPoolBase(new StdRefPoolFactory<T_>(), maxCentralEntries,
                     maxLocalEntries, localKeepEntryCount)
    {
    }

    ~StdRefPool()
    {
        DObjFactoryBase* factory = DetachFactory();
        delete factory;
    }

    void InsertObject(DrRef<T_>& object)
    {
        FreeObjectUntyped(object.Detach());
    }

    void RemoveObject(DrRef<T_>* pObject)
    {
        pObject->Attach((T_ *) AllocateObjectUntyped());
    }

    DRREFCOUNTIMPL
};
