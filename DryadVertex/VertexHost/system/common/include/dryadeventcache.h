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

#include <dryadlisthelper.h>

class DryadHandleListEntry
{
public:
    DryadHandleListEntry(HANDLE handle);
    HANDLE GetHandle();

private:
    HANDLE          m_handle;
    DrBListEntry    m_listPtr;
    friend class DryadBList<DryadHandleListEntry>;
};

class DryadEventCache
{
public:
    DryadEventCache();
    ~DryadEventCache();

    DryadHandleListEntry* GetEvent(bool reset);
    void ReturnEvent(DryadHandleListEntry* event);

private:
    typedef DryadBList<DryadHandleListEntry> HandleList;

    HandleList   m_eventCache;
};
