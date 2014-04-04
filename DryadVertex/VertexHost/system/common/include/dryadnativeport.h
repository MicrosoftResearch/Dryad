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

#include <orderedsendlatch.h>
#include <DrCommon.h>
#include <dryadlisthelper.h>

class DryadNativePort
{
public:
    class HandlerBase : protected OVERLAPPED
    {
    public:
        virtual ~HandlerBase();

        OVERLAPPED* GetOverlapped();

    private:
        virtual void ProcessIO(DrError retval, UInt32 numBytes) = 0;

        friend class DryadNativePort;
    };

    class Handler : public HandlerBase
    {
    public:
        Handler();
        virtual ~Handler();

        PSIZE_T GetNumberOfBytesToTransferPtr();
        virtual void* GetData() = 0;

    protected:
        void InitializeInternal(UInt32 bytesToTransfer,
                                UInt64 requestOffset);

    private:
        SIZE_T       m_bytesToTransfer;
    };

    DryadNativePort(DWORD numWorkerThreads,
                    DWORD numConcurrentThreads);
    ~DryadNativePort();

    void Start();

    void AssociateHandle(HANDLE fileHandle);

    void QueueNativeRead(HANDLE fileHandle, Handler* request);

    void QueueNativeWrite(HANDLE fileHandle, Handler* request);
/*JC    void QueueDryadWrite(DRHANDLE streamHandle,
                          DrError* pendingStatePtr,
                          UInt64 streamOffset,
                          Handler* request);*/

/*JC    void QueueDryadSetStreamProperties(const char* uri,
                                        DrError* pendingStatePtr,
                                        Handler* request);*/

    void IncrementOutstandingRequests();
    void DecrementOutstandingRequests();
    
    //
    // Return the number of outstanding requests
    //
    UInt32 GetOutstandingRequests() 
    { 
        return m_outstandingRequests; 
    }

    //
    // Return a the completition port handle
    //
    HANDLE GetCompletionPort();

    void Stop();

private:
    enum BufferPortState {
        BPS_Stopped,
        BPS_Running,
        BPS_Stopping
    };

    static unsigned __stdcall ThreadFunc(void* arg);

    static unsigned __stdcall WriteFileThreadBase(void* arg);
    void WriteFileThread();

    BufferPortState   m_state;

    DWORD             m_numWorkerThreads;
    DWORD             m_numConcurrentThreads;
    HANDLE            m_completionPort;
    HANDLE*           m_threadHandle;
    UInt32            m_outstandingRequests;

    class WriteFileRequest
    {
    public:
        WriteFileRequest(HANDLE h, Handler* hh)
        {
            m_fileHandle = h;
            m_request = hh;
        }

        HANDLE               m_fileHandle;
        Handler*             m_request;
        DrBListEntry         m_listPtr;
    };
    typedef DryadBList<WriteFileRequest> DryadWriteFileList;

    HANDLE              m_writeFileEvent;
    HANDLE              m_writeFileHandle;
    DryadWriteFileList  m_writeFileList;
    bool                m_writeFileFinished;
    CRITSEC             m_writeFileCS;

    CRITSEC             m_baseCS;
};


