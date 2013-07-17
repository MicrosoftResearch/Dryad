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

#include "RefCount.h"

//JCnamespace apsdk
//JC{

class MSMutex : public RefCount {
public:
  CRITICAL_SECTION m_Section;

  MSMutex() {
    InitializeCriticalSection(&m_Section);
  }
  ~MSMutex() {
    DeleteCriticalSection(&m_Section);
  }

  void Acquire() {
    EnterCriticalSection(&m_Section);
  }

  void Release() {
    LeaveCriticalSection(&m_Section);
  }
  
  BOOL TryAcquire() {
    return TryEnterCriticalSection(&m_Section);
  }
  
};

struct MutexLock {
  Ptr<MSMutex>        m_Lock;
  MutexLock(MSMutex *am) : m_Lock(am) {
    LogAssert (m_Lock);
    m_Lock->Acquire();
  }
  ~MutexLock() {
    Release();
  }
  void Release() {
    if (m_Lock) {
      m_Lock->Release();
      m_Lock = NULL;
    }
  }
};
    
    
  
struct MutexTryLock {
  Ptr<MSMutex>        m_Lock;
  MutexTryLock(MSMutex * am) {
    BOOL lockAcquired = am->TryAcquire(); 
    if (lockAcquired)
      m_Lock = am;
  }
  
  ~MutexTryLock() {
    Release();
  }
  void Release() { 
    if (m_Lock) {
      m_Lock->Release();
      m_Lock = NULL;
    }
  }
  bool operator!() { return (m_Lock == NULL); }
  operator BOOL () { return (m_Lock != NULL); }
};

#define MUTEX_LOCK(_l, _m) MutexLock _l(_m)
#define MUTEX_TRY_LOCK(_l,_m) MutexTryLock _l(_m)
#define MUTEX_RELEASE(_l) (_l).Release()

//JC} // namespace apsdk

#ifdef USING_APSDK_NAMESPACE
using namespace apsdk;
#endif
