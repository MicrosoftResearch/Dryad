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
#include <DrBList.h>

template< class _T > class DryadBList : public DrBList
{
public:
    typedef _T EntryType;

    static EntryType* CastOut(DrBListEntry* item)
    {
        return (item == NULL) ? NULL :
            (DR_GET_CONTAINER(EntryType, item, m_listPtr));
    }

    static DrBListEntry* CastIn(EntryType* item)
    {
        return &(item->m_listPtr);
    }

    EntryType* GetNextTyped(EntryType* item)
    {
        return CastOut(GetNext(CastIn(item)));
    }
};

template< class _T, class _B > class DryadBListDerived : public DrBList
{
public:
    typedef _T EntryType;
    typedef _B BaseType;
    typedef DryadBList< BaseType > BaseListType;

    static EntryType* CastOut(DrBListEntry* item)
    {
        return (EntryType *) BaseListType::CastOut(item);
    }

    static DrBListEntry* CastIn(EntryType* item)
    {
        return BaseListType::CastIn(item);
    }

    EntryType* GetNextTyped(EntryType* item)
    {
        return CastOut(GetNext(CastIn(item)));
    }
};



