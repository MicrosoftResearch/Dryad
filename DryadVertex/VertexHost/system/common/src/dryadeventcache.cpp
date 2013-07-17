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

#include "dryadeventcache.h"


#pragma unmanaged

DryadHandleListEntry::DryadHandleListEntry(HANDLE handle)
{
    m_handle = handle;
}

HANDLE DryadHandleListEntry::GetHandle()
{
    return m_handle;
}

DryadEventCache::DryadEventCache()
{
}

DryadEventCache::~DryadEventCache()
{
    BOOL bRet;
    DrBListEntry* listEntry = m_eventCache.GetHead();
    while (listEntry != NULL)
    {
        DryadHandleListEntry* h = m_eventCache.CastOut(listEntry);
        listEntry = m_eventCache.GetNext(listEntry);
        m_eventCache.Remove(m_eventCache.CastIn(h));
        bRet = ::CloseHandle(h->GetHandle());
        LogAssert(bRet != 0);
        delete h;
    }
}

DryadHandleListEntry* DryadEventCache::GetEvent(bool reset)
{
    DryadHandleListEntry* event;

    if (m_eventCache.IsEmpty())
    {
        HANDLE h = ::CreateEvent(NULL, TRUE, FALSE, NULL);
        LogAssert(h != NULL);
        event = new DryadHandleListEntry(h);
    }
    else
    {
        event = m_eventCache.CastOut(m_eventCache.RemoveHead());
        if (reset)
        {
            BOOL bRet = ::ResetEvent(event->GetHandle());
            LogAssert(bRet != 0);
        }
    }

    return event;
}

void DryadEventCache::ReturnEvent(DryadHandleListEntry* event)
{
    m_eventCache.InsertAsHead(m_eventCache.CastIn(event));
}
