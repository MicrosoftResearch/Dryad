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

#include "DObjPool.h"

DObjPoolThreadPrivateBlock::Entry::Entry()
{
    m_pool = NULL;
    m_key = 0;
    m_cache = NULL;
}

DObjPoolThreadPrivateBlock::Entry::~Entry()
{
    if (m_cache != NULL)
    {
        LogAssert(m_cache->Abandoned());
        delete m_cache;
    }
}

void DObjPoolThreadPrivateBlock::Entry::Initialize(DObjPoolBase* pool,
                                                   LONGLONG poolKey,
                                                   DObjPoolCache* cache)
{
    LogAssert(pool != NULL);
    LogAssert(cache != NULL);

    m_pool = pool;
    m_key = poolKey;
    m_cache = cache;
}

DObjPoolThreadPrivateBlock::DObjPoolThreadPrivateBlock()
{
    m_entryArraySize = 16;
    m_entry = new Entry[m_entryArraySize];
    m_numberOfEntries = 0;
}

DObjPoolThreadPrivateBlock::~DObjPoolThreadPrivateBlock()
{
    delete [] m_entry;
}

DObjPoolCache* DObjPoolThreadPrivateBlock::LookUpPoolCache(DObjPoolBase* pool,
                                                           LONGLONG key)
{
    UInt32 i;
    for (i=0; i<m_numberOfEntries; ++i)
    {
        Entry* e = &(m_entry[i]);
        if (e->m_pool == pool && e->m_key == key)
        {
            return e->m_cache;
        }
    }

    return NULL;
}

void DObjPoolThreadPrivateBlock::AddPoolCache(DObjPoolBase* pool,
                                              LONGLONG poolKey,
                                              DObjPoolCache* cache)
{
    LogAssert(LookUpPoolCache(pool, poolKey) == NULL);

    if (m_numberOfEntries == m_entryArraySize)
    {
        m_entryArraySize *= 2;
        DrLogI( "Growing pool cache array. Was %u entries, now %u", m_numberOfEntries, m_entryArraySize);
        Entry* newArray = new Entry[m_entryArraySize];
        LogAssert(newArray != NULL);
        ::memcpy(newArray, m_entry, m_numberOfEntries*sizeof(m_entry[0]));
        delete [] m_entry;
        m_entry = newArray;
    }

    LogAssert(m_numberOfEntries < m_entryArraySize);

    DrLogI( "Inserting pool cache entry. Entry %u pool %p key %I64d", m_numberOfEntries, pool, poolKey);

    m_entry[m_numberOfEntries].Initialize(pool, poolKey, cache);
    ++m_numberOfEntries;
}

void DObjPoolThreadPrivateBlock::GarbageCollectCaches()
{
    DrLogI( "Inspecting pool cache for stale entries");

    UInt32 newTotal = 0;
    UInt32 i;
    for (i=0; i<m_numberOfEntries; ++i)
    {
        if (m_entry[i].m_cache->Abandoned())
        {
            DrLogI( "Discarding pool cache entry. Entry %u pool %p key %I64d",
                i, m_entry[i].m_pool, m_entry[i].m_key);
            delete m_entry[i].m_cache;
        }
        else
        {
            m_entry[newTotal].Initialize(m_entry[i].m_pool,
                                         m_entry[i].m_key,
                                         m_entry[i].m_cache);
            ++newTotal;
        }
    }

    LogAssert(newTotal <= m_numberOfEntries);

    DrLogI( "After pool cache collection. Old size %u new size %u", m_numberOfEntries, newTotal);

    m_numberOfEntries = newTotal;
}


DObjPoolCache::DObjPoolCache(UInt32 maxEntries,
                             UInt32 keepEntryCount,
                             DObjPoolBase* pool)
{
    LogAssert(keepEntryCount < maxEntries);
    m_abandoned = false;
    m_pool = pool;
    m_maxEntries = maxEntries;
    m_keepEntryCount = keepEntryCount;
    m_array = new void* [m_maxEntries];
    m_numberOfEntries = 0;
}

DObjPoolCache::~DObjPoolCache()
{
    LogAssert(m_abandoned == true);
    delete [] m_array;
}

bool DObjPoolCache::Abandoned()
{
    return m_abandoned;
}

void DObjPoolCache::InsertObject(void* e)
{
    LogAssert(m_abandoned == false);

    if (m_numberOfEntries == m_maxEntries)
    {
        ReturnToPool(false);
    }

    LogAssert(m_numberOfEntries < m_maxEntries);
    m_array[m_numberOfEntries] = e;
    ++m_numberOfEntries;
}

void* DObjPoolCache::RemoveObject()
{
    LogAssert(m_abandoned == false);

    if (m_numberOfEntries == 0)
    {
        m_pool->RemoveObjects(m_array, m_keepEntryCount);
        m_numberOfEntries = m_keepEntryCount;
    }

    LogAssert(m_numberOfEntries > 0);
    --m_numberOfEntries;
    return m_array[m_numberOfEntries];
}

void DObjPoolCache::ReturnToPool(bool finalCleanup)
{
    UInt32 keepCount;
    if (m_keepEntryCount == 0 || finalCleanup)
    {
        keepCount = 0;
    }
    else
    {
        /* we're about to insert something, so get rid of one
           extra */
        keepCount = m_keepEntryCount - 1;
    }

    m_pool->AcceptObjects(m_array + keepCount,
                          m_numberOfEntries - keepCount);
    m_numberOfEntries = keepCount;

    if (finalCleanup)
    {
        /* make sure we don't reference any member variables after
           setting m_abandoned to true, since we may be spontaneously
           deleted by another thread at any time after this action */
        m_abandoned = true;
    }
}


static CRITSEC* s_refPoolGlobalCritSec;
static DrTlsPtr<DObjPoolThreadPrivateBlock>* t_privateCacheBlock;

class DObjPoolCritSecInitializer
{
public:
    DObjPoolCritSecInitializer(CRITSEC** critSec);
};

DObjPoolCritSecInitializer::DObjPoolCritSecInitializer(CRITSEC** critSec)
{
    *critSec = new CRITSEC();
}

/* make sure s_refPoolGlobalCritSec is initialized by the time all
   static constructors have run */
static DObjPoolCritSecInitializer s_initCritSec(&s_refPoolGlobalCritSec);

DObjPoolBase::DObjPoolBase(DObjFactoryBase* factory,
                           UInt32 maxCentralEntries,
                           UInt32 maxLocalEntries,
                           UInt32 localKeepEntryCount)
{
    /* make a UID to distinguish us from any other pool with the same
       heap address (e.g. if memory gets recycled). We don't have to
       worry about a race here, since another pool being created at
       exactly the same time on another processor must have a
       different address. */
    LARGE_INTEGER keyLI;
    ::QueryPerformanceCounter(&keyLI);
    m_key = keyLI.QuadPart;

    LogAssert(factory != NULL);
    m_factory = factory;
    m_maxCentralEntries = maxCentralEntries;
    m_maxLocalEntries = maxLocalEntries;
    m_localKeepEntryCount = localKeepEntryCount;

    m_array = new void* [m_maxCentralEntries];
    m_numberOfCentralEntries = 0;

    m_cacheArraySize = 32;
    m_cache = new DObjPoolCache* [m_cacheArraySize];
    m_numberOfCaches = 0;

    m_totalGivenOut = 0;
    m_totalReturned = 0;
    m_totalAllocated = 0;
    m_totalFreed = 0;

    /* make sure we have exactly one TLS entry for all pools */
    if (s_refPoolGlobalCritSec == NULL)
    {
        /* a static constructor is calling, so we don't need to worry
           about thread safety */
        if (t_privateCacheBlock == NULL)
        {
            t_privateCacheBlock = new DrTlsPtr<DObjPoolThreadPrivateBlock>;
        }
    }
    else
    {
        AutoCriticalSection acs(s_refPoolGlobalCritSec);

        if (t_privateCacheBlock == NULL)
        {
            t_privateCacheBlock = new DrTlsPtr<DObjPoolThreadPrivateBlock>;
        }
    }
}

DObjPoolBase::~DObjPoolBase()
{
    UInt32 i;
    for (i=0; i<m_numberOfCaches; ++i)
    {
        m_cache[i]->ReturnToPool(true);
        /* we must not delete m_cache[i] here since it is still
           referenced in the private block of its thread. That thread
           will delete the cache later during a garbage collection */
    }
    delete [] m_cache;

    LogAssert(m_totalGivenOut == m_totalReturned);
    LogAssert(m_numberOfCentralEntries + m_totalFreed == m_totalAllocated);

    for (i=0; i<m_numberOfCentralEntries; ++i)
    {
        m_factory->FreeObjectUntyped(m_array[i]);
    }
    delete [] m_array;
}

DObjFactoryBase* DObjPoolBase::DetachFactory()
{
    LogAssert(m_factory != NULL);
    return m_factory.Detach();
}

void DObjPoolBase::AcceptObjects(void** src, UInt32 count)
{
    UInt32 i;
    {
        AutoCriticalSection acs(&m_atomic);

        UInt32 freeSpace = m_maxCentralEntries - m_numberOfCentralEntries;
        if (freeSpace > count)
        {
            freeSpace = count;
        }

        for (i=0; i<freeSpace; ++i)
        {
            LogAssert(src[i] != NULL);
            m_array[m_numberOfCentralEntries + i] = src[i];
        }
        m_numberOfCentralEntries += freeSpace;

        m_totalReturned += count;
        m_totalFreed += (count - i);

        DrLogI( "Bulk transfer into pool. Pool %p, count %u, deleting %u, hOut: %I64u ret: %I64u all: %I64u free: %I64u",
            this, count, count-i,
            m_totalGivenOut, m_totalReturned,
            m_totalAllocated, m_totalFreed);
    }

    for (; i<count; ++i)
    {
        m_factory->FreeObjectUntyped(src[i]);
    }
}

void DObjPoolBase::RemoveObjects(void** dst, UInt32 count)
{
    UInt32 i;
    {
        AutoCriticalSection acs(&m_atomic);

        UInt32 existing = m_numberOfCentralEntries;
        if (existing > count)
        {
            existing = count;
        }
        m_numberOfCentralEntries -= existing;

        for (i=0; i<existing; ++i)
        {
            dst[i] = m_array[m_numberOfCentralEntries + i];
            LogAssert(dst[i] != NULL);
        }

        m_totalGivenOut += count;
        m_totalAllocated += (count - i);

        DrLogI( "Bulk transfer out of pool. Pool %p, count %u, allocating %u, hOut: %I64u ret: %I64u all: %I64u free: %I64u",
            this, count, count-i,
            m_totalGivenOut, m_totalReturned,
            m_totalAllocated, m_totalFreed);
    }

    for (; i<count; ++i)
    {
        dst[i] = m_factory->AllocateObjectUntyped();
    }
}

DObjPoolCache* DObjPoolBase::MakeCache()
{
    AutoCriticalSection acs(&m_atomic);

    if (m_numberOfCaches == m_cacheArraySize)
    {
        m_cacheArraySize *= 2;
        DObjPoolCache** newArray = new DObjPoolCache* [m_cacheArraySize];
        LogAssert(newArray != NULL);
        ::memcpy(newArray, m_cache, m_numberOfCaches * sizeof(m_cache[0]));
        delete m_cache;
        m_cache = newArray;
    }

    LogAssert(m_numberOfCaches < m_cacheArraySize);

    DObjPoolCache* cache = new DObjPoolCache(m_maxLocalEntries,
                                             m_localKeepEntryCount,
                                             this);
    m_cache[m_numberOfCaches] = cache;
    ++m_numberOfCaches;

    return cache;
}

DObjPoolCache* DObjPoolBase::FetchPrivateCache()
{
    DObjPoolThreadPrivateBlock* privateCacheBlock = *t_privateCacheBlock;

    if (privateCacheBlock == NULL)
    {
        /* this is the first time any pool has been referenced on this
           thread, so make a new empty private cache block. This will
           never be garbage-collected. */
        privateCacheBlock = new DObjPoolThreadPrivateBlock();
        LogAssert(privateCacheBlock != NULL);
        *t_privateCacheBlock = privateCacheBlock;
    }

    DObjPoolCache* cache = privateCacheBlock->LookUpPoolCache(this, m_key);
    if (cache == NULL)
    {
        /* this is the first time this particular pool has been
           referenced on this thread, so make a new empty entry
           cache. This will be garbage-collected by the
           privateCacheBlock some time after the pool is freed. */
        cache = MakeCache();
        privateCacheBlock->AddPoolCache(this, m_key, cache);
    }

    return cache;
}

void* DObjPoolBase::AllocateObjectUntyped()
{
    return FetchPrivateCache()->RemoveObject();
}

void DObjPoolBase::FreeObjectUntyped(void* item)
{
    FetchPrivateCache()->InsertObject(item);
}
