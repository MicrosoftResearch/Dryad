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

    context.cpp

Abstract:

    This module contains the public interface and support routines for
    managing process context for xcompute on the HPC scheduler

--*/
#include "stdafx.h"
#include <map>

std::map<DWORD, ULONG_PTR> g_Context;


/*++

XcSetProcessUserContext API

Description:

Associates API user related data with process 
identified by the Process handle.

The API user can associate any data with the XCompute process 
and get back the data, using the XcGetProcessUserContext API.

This call is synchronous and does not cross
machine boundaries/process boundaries.

NOTE:   
a.  The XcCloseProcessHandle() will not deallocate the user 
    context data. It is the API users responsibilty to 
    deallocated data associated with UserContext.

b. The user context is associated with a process and not with a
    ProcessHandle. So if multiple handles identify the same 
    XCompute process, they will return the same user context.

Arguments:

    hProcessHandle
                Process handle to identify process to which user context 
                is being associated

    userContext
                The user context data

    pPreviousUserContext
                If there was a previously associated user context
                with the XCompute process , then returns that context data. 
                Otherwise NULL is returned. If the caller supplies NULL input, 
                the previous value is not returned


Return Value:
  
    XCERROR_OK  
                The call succeded


--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcSetProcessUserContext(
    IN  XCPROCESSHANDLE hProcessHandle,
    IN  ULONG_PTR       userContext,    
    OUT ULONG_PTR*      pPreviousUserContext
    )
{
    *pPreviousUserContext = g_Context[(DWORD)hProcessHandle];
    g_Context[(DWORD)hProcessHandle] = userContext;

    return S_OK;
}



/*++

XcGetProcessUserContext API

Description:

Gets the API user related data associated with the XCompute Process.
The API user can associate any data with the XCompute process via the 
XcAddUserContextToHandle API.

This call is synchronous and does not cross
machine boundaries/process boundaries.

Arguments:

    hProcessHandle
                Process handle 

    pUserContext
                The user context data associated with the XCompute Process. 
                If no user context is associated, then 
                NULL is returned.

Return Value:
  
    XCERROR_OK  
                The call succeded


--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessUserContext(
    IN  XCPROCESSHANDLE hProcessHandle,
    OUT ULONG_PTR*      pUserContext
    )
{

    *pUserContext = g_Context[(DWORD)hProcessHandle];

    return S_OK;
}


