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

    process.cpp

Abstract:

    This module contains the public interface and support routines for
    the xcompute process functionality on top of the HPC scheduler.

    Note that "process" in the xcompute terminology maps to "task" in
    the HPC terminology.

--*/
#include "stdafx.h"
#include <map>

static CComAutoCriticalSection g_idLock;
static LONG g_dwNextProcessId = 1;

DWORD GetNextProcessId()
{
    return InterlockedIncrement(&g_dwNextProcessId);
}

static CComAutoCriticalSection g_referenceLock;
typedef std::map<DWORD,DWORD> HandleMapT;
HandleMapT g_handleRefs;

void AddProcessHandleReference(DWORD dwId)
{
    CComCritSecLock<CComAutoCriticalSection> lock(g_referenceLock);

    HandleMapT::const_iterator iter = g_handleRefs.find(dwId);
    if (iter == g_handleRefs.end())
    {
        g_handleRefs[dwId] = 1;
    }
    else
    {
        g_handleRefs[dwId] = ++(g_handleRefs[dwId]);
    }
}

bool ReleaseProcessHandleReference(DWORD dwId)
{
    bool bCanFree = true;

    CComCritSecLock<CComAutoCriticalSection> lock(g_referenceLock);

    HandleMapT::iterator iter = g_handleRefs.find(dwId);
    if (iter != g_handleRefs.end())
    {
        DWORD dwCount = iter->second;
        if (--dwCount == 0)
        {
            g_handleRefs.erase(iter);
        }
        else
        {
            g_handleRefs[dwId] = dwCount;
            bCanFree = false;
        }
    }

    return bCanFree;
}


XCPROCESSSTATE TranslateProcessState(
    IN ProcessState     fromState 
    )
{
    XCPROCESSSTATE toState = XCPROCESSSTATE_UNSCHEDULED;

    if (fromState == ProcessState::Unscheduled)
    {
        toState = XCPROCESSSTATE_UNSCHEDULED;
    }
    else if (fromState == ProcessState::SchedulingFailed)
    {
        toState = XCPROCESSSTATE_SCHEDULINGFAILED;
    }
    else if (fromState == ProcessState::AssignedToNode)
    {
        toState = XCPROCESSSTATE_ASSIGNEDTONODE;
    }
    else if (fromState == ProcessState::Running)
    {
        toState = XCPROCESSSTATE_RUNNING;
    }
    else if (fromState == ProcessState::Completed)
    {
        toState = XCPROCESSSTATE_COMPLETED;
    }

    return toState;
}


/*++

XcCreateNewProcessHandle API

Description:

Creates a new process handle for a new process in the
given Job. 

This call is synchronous and does not cross
machine boundaries/process boundaries.

Note: 
1.  
    This method just creates the handle to 
    the XCompute process. It does not schedule the process itself.
    Use the XcScheduleProcessAPI to schedule the XCompute process.

2.  
    Use the XcCloseProcessHandle() to free the handle

3.
    Do not copy handle using the simple assignment operator.Use the 
    DuplicateProcessHandle() API. Each handle variable needs to be 
    freed using the XcCloseProcessHandle().

Arguments:

    hSession            
                Handle to a session associated with this call                

    pJobId   
                The Id of the job under which the process will
                be created. A NULL value will cause the current
                processes JobId to be automatically picked up.
                NOTE: 
                This parameter is only interesting to the Task Scheduler.
                For all other cases, it should be assined to NULL

    phProcessHandle
                The handle to the process.


Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCreateNewProcessHandle(
    IN  XCSESSIONHANDLE     hSession,  
    IN  PCXC_JOBID          pJobId,  
    OUT PXCPROCESSHANDLE    phProcessHandle
)
{
    DWORD dwId = GetNextProcessId();

    VertexScheduler::GetInstance()->CreateVertexProcess(dwId);

    *phProcessHandle = (XCPROCESSHANDLE)dwId;
    AddProcessHandleReference(*((LPDWORD)phProcessHandle));

    return S_OK;
}


/*++

XcOpenCurrentProcessHandle API

Description:

Opens the current processes handle 

This call is synchronous and does not cross
machine boundaries/process boundaries.

Note: 
1.  
    This method creates the handle to the current process and assigns
    it to the session on that process.

2.  
    Use the XcCloseProcessHandle() to free the handle

3.
    Do not copy handle using the simple assignment operator.Use the 
    DuplicateProcessHandle() API. Each handle variable needs to be 
    freed using the XcCloseProcessHandle().

Arguments:

    hSession            
                Handle to a session associated with this call.

    phProcessHandle
                The handle to the process. This must be closed using the 
                XcClosePorcessHandle()

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcOpenCurrentProcessHandle(
    IN  XCSESSIONHANDLE     hSession,  
    OUT PXCPROCESSHANDLE    phProcessHandle
)
{
    *phProcessHandle = CURRENT_PROCESS_ID;

    return S_OK;
}


/*++

XcCloseProcessHandle API

Description:

Closes a process handle created either by a call to 
XcCreateNewProcessHandle() or XcDupProcessHandle().

This call is synchronous and does not cross
machine boundaries/process boundaries.


NOTE: 
Every call to the XcCreateNewProcessHandle() or
DupProcessHandle() should ultimately
result in a call to XcCloseProcessHandle() to deallocated the handle.

Arguments:

    hProcessHandle
                Process handle to be closed

Return Value:
  
    XCERROR_OK  
                The call succeded


--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseProcessHandle (
    IN  XCPROCESSHANDLE     hProcessHandle
)
{

    if (hProcessHandle != CURRENT_PROCESS_ID)
    {
        DWORD dwId = (DWORD)hProcessHandle;
        if (ReleaseProcessHandleReference(dwId))
        {
            // No more references, free associated resources in XComputeLib.dll
            VertexScheduler::GetInstance()->CloseVertexProcess(dwId);
        }
    }

    return S_OK;
}



/*++

XcDupProcessHandle API

Description:

Duplicates a process handle. Use this api, if a copy of the 
process handle is needed.

This call is synchronous and does not cross
machine boundaries/process boundaries.

NOTE: 
    a. Every call to the DupProcessHandle should ultimately result
        in a call to XcCloseProcessHandle() to deallocated the handle.

Arguments:

    hProcessHandle
                Process handle to be duplicated

    phDupProcessHandle
                The duplicated process handle

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcDupProcessHandle(
    IN  XCPROCESSHANDLE     hProcessHandle,
    OUT PXCPROCESSHANDLE    phDupProcessHandle
)
{
    AddProcessHandleReference((DWORD)hProcessHandle);
    *phDupProcessHandle = hProcessHandle;

    return S_OK;
}



/*++

XcSerializeProcessHandle API

Description:

Creates a serialized process handle. A XCompute process can serialize a 
process handle, and pass it to another XCompute process where the other
XCompute process can use the XcUnSerializeProcessHandle() API, to
recreate the process handle. Then it can use that process handle to 
communicate with the process. e.g by using XcSetAndGetProcessInfo() API

Arguments:

    hProcessHandle
        The handle to the process to serialize.

    ppXcSerializedHandleBlock
        The serialized process handle. Use the XcFreeMemory() API 
        to de-allocated the pXcSerializedHandleBlock.
    
    pcbBlockLength
        The length in bytes of the serialized process handle block

NOTE:
    The UserContext assiciated with the process handle will *NOT*
    be serialzed.
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcSerializeProcessHandle (
    IN  XCPROCESSHANDLE hProcessHandle,
    OUT PCVOID*         ppXcSerializedHandleBlock,    
    OUT PSIZE_T         pcbBlockLength
)
{
    *ppXcSerializedHandleBlock = (PCVOID)hProcessHandle;
    *pcbBlockLength = sizeof(DWORD);

    return S_OK;
}


/*++

XcUnSerializeProcessHandle API

Description:

Un-serializes a serialized process handle. See XcSerializeProcessHandle() API
for more details

Arguments:

    hSession
        The session to which to associate the un-serialized process handle with.

    pXcSerializedHandleBlock
        The serialized process handle.
    
    pcbBlockLength
        The length in bytes of the serialized process handle block

    phProcessHandle
        The un-serialized process handle.
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcUnSerializeProcessHandle (
    IN  XCSESSIONHANDLE     hSession,
    IN  PCVOID              pXcSerializedHandleBlock,
    IN  SIZE_T              cbBlockLength,
    OUT PXCPROCESSHANDLE    phProcessHandle
)
{
    *phProcessHandle = (XCPROCESSHANDLE)pXcSerializedHandleBlock;

    return S_OK;
}


/*++

XcGetProcessState API

Description:

Gets the process state information. If Schedule process is not
yet been called, the API will return error.

This call is synchronous and does not cross
machine boundaries/process boundaries.

Arguments:

    hProcessHandle
                Process handle 
    
    pProcessState
                Describes the process state. The different states
                are described in XComputeTypes.h

    pProcessSchedulingError
                if process state is XCPROCESSSTATE_COMPLETED
                then the error code indicates reson.
                S_OK means process compeleted without errors.
                Other error codes indicate reasons for failed completion.

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessState(
    IN  XCPROCESSHANDLE hProcessHandle,
    OUT PXCPROCESSSTATE pProcessState,
    OUT XCERROR*        pProcessSchedulingError
    )
{
    HRESULT hr = S_OK;

    if (pProcessState == NULL)
    {
        hr = E_INVALIDARG;
        goto Exit;
    }

    if (pProcessSchedulingError == NULL)
    {
        hr = E_INVALIDARG;
        goto Exit;
    }

    try 
    {
        ProcessState state = VertexScheduler::GetInstance()->GetProcessState((int)hProcessHandle);

        *pProcessState = TranslateProcessState(state);

        switch (*pProcessState)
        {
        case XCPROCESSSTATE_SCHEDULINGFAILED:
            *pProcessSchedulingError = E_FAIL;
            break;
        case XCPROCESSSTATE_COMPLETED:
            if (VertexScheduler::GetInstance()->ProcessCancelled((int)hProcessHandle))
            {
                *pProcessSchedulingError = E_FAIL;
            }
            break;
        default:
            *pProcessSchedulingError = S_OK;
            break;
        }
    }
    catch (System::Exception ^e)
    {
        hr = System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }

Exit:
    return hr;
}

/*++

XcGetProcessId API

Description:

Gets the process Id of the process associated with the process handle. 
If the process state is anything less than XCPROCESSSTATE_ASSIGNEDTOPN
an error is returned.

Arguments:

    hProcessHandle
                Process handle 
    
    
    pProcessId   
                The id of the process
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessId(
    IN  XCPROCESSHANDLE hProcessHandle,
    OUT XC_PROCESSID*   pProcessId   
)
{
    // TODO: anything else needed here?
    pProcessId->Data1 = (DWORD)hProcessHandle;

    return S_OK;
}

/*++

XcWaitForStateChange API

Description:

The API allows users to get async completion status for 
XCompute process when it reaches a desired state. (see XCPROCESSSTATE) 
When the desired state is reached the async completion is dispatched.

NOTE: 
1.  If the process gets cancelled, then completion is dispatched immediately
2.  The pOperationState of the AsyncInfo will have the error code.

Arguments:    

    hProcessHandle        
                Handle to an XCompute process for which the 
                state change event is needed

    waitForState
                The state to wait for the XCompute to be in, so
                that completion can be dispatched

    tiMaxWaitInterval
                The maximum amount of time (not including network 
                request latencies) that the API should wait for a 
                change in the process list before completing. If 
                XCTIMEINTERVAL_ZERO, the API will return changes
                that can be immediately determined without  
                communication with the process scheduler. If 
                XCTIMEINTERVAL_INFINITE, the API will wait until a 
                change occurs or the process is cancelled.

    pAsyncInfo  
                The async info structure. Its an alias to the 
                CS_ASYNC_INFO defined in Cosmos.h. If this 
                parameter is NULL, then the function completes in 
                synchronous manner and error code is returned as 
                return value.

                If parameter is not NULL then the operation is carried
                on in asynchronous manner. If an asynchronous 
                operation has been successfully started then 
                this function terminates immediately with 
                an HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
                Any other return value indicates that it was 
                impossible to start the asynchronous operation.

    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcWaitForStateChange(        
    IN  XCPROCESSHANDLE hProcessHandle,  
    IN  XCPROCESSSTATE  waitForState,
    IN  XCTIMEINTERVAL  tiMaxWaitInterval,
    IN  PCXC_ASYNC_INFO pAsyncInfo
)
{
    ProcessState targetState;

    PASYNC async;

    //
    // Map the requested state into the HPC task state
    //
    switch (waitForState)
    {
    case XCPROCESSSTATE_UNSCHEDULED:
        targetState = ProcessState::Unscheduled;
        break;

    case XCPROCESSSTATE_SCHEDULINGFAILED:
        targetState = ProcessState::SchedulingFailed;
        break;

    case XCPROCESSSTATE_ASSIGNEDTONODE:
        targetState = ProcessState::AssignedToNode;
        break;

    case XCPROCESSSTATE_RUNNING:
        targetState = ProcessState::Running;
        break;

    case XCPROCESSSTATE_COMPLETED:
        targetState = ProcessState::Completed;
        break;

    default:
        return E_NOTIMPL;
    }

    CAPTURE_ASYNC(async);

    try 
    {
    
       VertexScheduler ^client = VertexScheduler::GetInstance();
       ProcessState currentState = client->GetProcessState((int)hProcessHandle);

        if (currentState >= targetState) 
        {
            return COMPLETE_ASYNC(async, S_OK);
        }
    
        //
        // If our timeout has elapsed, return timeout
        //
        if (tiMaxWaitInterval == XCTIMEINTERVAL_ZERO)
        {
            return COMPLETE_ASYNC(async, HRESULT_FROM_WIN32(ERROR_TIMEOUT));
        }

        if (async == NULL) 
        {
            if (client->WaitForStateChange((int)hProcessHandle, tiMaxWaitInterval, targetState))
            {
                return S_OK;
            }
            else
            {
                return HRESULT_FROM_WIN32(ERROR_TIMEOUT);
            }
        } 
        else 
        {
            AsyncWrapper ^wrapper = gcnew AsyncWrapper(async);
            StateChangeEventHandler ^handler = gcnew StateChangeEventHandler(wrapper, &AsyncWrapper::StateChangeHandler);
            client->NotifyStateChange((int)hProcessHandle, tiMaxWaitInterval, targetState, handler);
            return HRESULT_FROM_WIN32(ERROR_IO_PENDING);
        }
    } 
    catch (System::Exception ^e) 
    {
        return COMPLETE_ASYNC(async,System::Runtime::InteropServices::Marshal::GetHRForException(e));
    }
}



