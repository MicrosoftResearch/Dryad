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

class WorkQueue;

class WorkRequest
{
public:
    virtual ~WorkRequest();
    virtual void Process() = 0;
    virtual bool ShouldAbort() = 0;

private:
    DrBListEntry   m_listPtr;
    friend class DryadBList<WorkRequest>;
};

typedef DryadBList<WorkRequest> WorkRequestList;

class WorkQueue {
 public:
    WorkQueue(DWORD numWorkerThreads,
              DWORD numConcurrentThreads);
    ~WorkQueue();

    void Start();
    bool EnQueue(WorkRequest* request);

    void Clean();
    void Stop();

private:
    enum WorkQueueState {
        WQS_Stopped,
        WQS_Running,
        WQS_Stopping
    };

    static unsigned __stdcall ThreadFunc(void* arg);

    WorkRequestList   m_list; /* list of WorkRequest items */

    WorkQueueState    m_state;

    DWORD             m_numWorkerThreads;
    DWORD             m_numConcurrentThreads;
    HANDLE            m_completionPort;
    HANDLE*           m_threadHandle;
    DWORD             m_numQueuedWakeUps;

    CRITSEC           m_baseCS;
};
