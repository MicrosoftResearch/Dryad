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

#include "DrShared.h"

#ifdef _MANAGED

#else

#define DRREF_MAGIC_CONSTRUCTOR_VALUE (-666)

#ifdef _DEBUG_DRREF
std::set<DrRefCounter*> DrRefCounter::s_refsAllocated;
std::map<void*,DrRefCounter*> DrRefCounter::s_arrayStorage;
CRITICAL_SECTION DrRefCounter::s_debugCS;
#endif

DrRefCounter::DrRefCounter() : m_iRefCount( DRREF_MAGIC_CONSTRUCTOR_VALUE )
{
#ifdef _DEBUG_DRREF
    EnterCriticalSection(&s_debugCS);
    bool inserted = s_refsAllocated.insert(this).second;
    DrAssert(inserted);
    LeaveCriticalSection(&s_debugCS);
#endif
}

DrRefCounter::~DrRefCounter()
{
    DrAssert(m_iRefCount == 0);
#ifdef _DEBUG_DRREF
    DrAssert(m_holders.empty());
    EnterCriticalSection(&s_debugCS);
    size_t nRemoved = s_refsAllocated.erase(this);
    DrAssert(nRemoved == 1);
    LeaveCriticalSection(&s_debugCS);
#endif
}

void DrRefCounter::FreeMemory()      // Called when the refcount becomes zero.
{
    delete this;
}


#ifdef _DEBUG_DRREF
void DrRefCounter::IncRef(void* h)
{
    EnterCriticalSection(&s_debugCS);
    ++m_iRefCount;
    if (m_iRefCount <= 1)
    {
        // this is the first assignment of a newly constructed object */
        DrAssert(m_iRefCount == (DRREF_MAGIC_CONSTRUCTOR_VALUE + 1));
        m_iRefCount = 1;
    }
    bool inserted = m_holders.insert(h).second;
    DrAssert(inserted);
    DrAssert(m_holders.size() == (size_t) m_iRefCount);
    LeaveCriticalSection(&s_debugCS);
}
#else
void DrRefCounter::IncRef()
{
    LONG i;
    i = InterlockedIncrement(&m_iRefCount);
    if (i <= 1)
    {
        // this is the first assignment of a newly constructed object */
        DrAssert(i == (DRREF_MAGIC_CONSTRUCTOR_VALUE + 1));
        m_iRefCount = 1;
    }
}
#endif

#ifdef _DEBUG_DRREF
void DrRefCounter::DecRef(void* h)
{
    EnterCriticalSection(&s_debugCS);
    DrAssert(m_holders.size() == (size_t) m_iRefCount);
    size_t nRemoved = m_holders.erase(h);
    DrAssert(nRemoved == 1);
    --m_iRefCount;
    if (m_iRefCount <= 0)
    {
        DrAssert(m_iRefCount == 0);
        FreeMemory();
    }
    LeaveCriticalSection(&s_debugCS);
}
#else
void DrRefCounter::DecRef()
{
    LONG i;
    i = InterlockedDecrement(&m_iRefCount);
    if (i <= 0)
    {
        DrAssert(i == 0);
        FreeMemory();
    }
}
#endif

void DrInterfaceRefBase::AssertTypeCast()
{
    DrLogA("Type cast failed");
}

#endif