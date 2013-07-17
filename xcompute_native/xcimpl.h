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

/*++
Module Name:

    xcimpl.h

Abstract:

    This module contains the private prototypes and definitions for 
    the xcompute API implementation on top of the HPC scheduler.

--*/

using namespace Microsoft::Research::Dryad;


//
// timing infrastructure
//
extern DWORD g_StartTime;
#define TS ((::GetTickCount() - g_StartTime)/1000.0)


//
//
//

#define INVALID_PROCESS_ID  ((XCPROCESSHANDLE)0)
#define CURRENT_PROCESS_ID  ((XCPROCESSHANDLE)1)

DWORD GetNextProcessId();

//
// Utility routines in async.cpp for managing the asynchronous completion
// notification functionality of xcompute
//

//
// The copy of the async structure we keep around until the 
// operation completes.
//
class ASYNC 
{
public:
    ASYNC(PCXC_ASYNC_INFO pAsyncInfo);
    virtual ~ASYNC();
    static ASYNC *Capture(PCXC_ASYNC_INFO pAsyncInfo);
    HRESULT Complete(HRESULT hr);
private:
    HRESULT *pOperationState;
    HANDLE hEvent;
    HANDLE hIOCP;
    LPOVERLAPPED pOverlapped;
    UINT_PTR CompletionKey;
};
typedef ASYNC *PASYNC;


ref class AsyncWrapper 
{
    PASYNC ap;
public:
    AsyncWrapper(PASYNC ap):ap(ap) {}

    void StateChangeHandler(System::Object ^sender,Microsoft::Research::Dryad::XComputeProcessStateChangeEventArgs ^e)
    {
        if (e->TimedOut)
        {
            ap->Complete(HRESULT_FROM_WIN32(ERROR_TIMEOUT));
        }
        else
        {
            ap->Complete(S_OK);
        }
    }

    void GetSetPropertyHandler(System::Object ^sender, Microsoft::Research::Dryad::XComputeProcessGetSetPropertyEventArgs ^e)
    {
        ap->Complete(S_OK);
    }
};


//
// N.B. This macro will RETURN HRESULT on failure, so be sure no additional
//      error cleanup is required. 
//
#define CAPTURE_ASYNC(_pasync_) \
    {                                                              \
        if (pAsyncInfo) {                                          \
            _pasync_ = new ASYNC(pAsyncInfo);                    \
            if (_pasync_ == NULL)                                  \
            {                                                      \
                return E_OUTOFMEMORY;                              \
            }                                                      \
        } else {                                                   \
            _pasync_ = NULL;                                       \
        }                                                          \
    }

#define COMPLETE_ASYNC(_pasync_, _hr_) ((_pasync_) ? _pasync_->Complete(_hr_) : _hr_)

//
// Translate from managed ProcessState to native XCPROCESSSTATE
//
XCPROCESSSTATE TranslateProcessState(
    IN ProcessState     fromState 
);
