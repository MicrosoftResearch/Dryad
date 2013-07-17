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

    session.cpp

Abstract:

    This module contains the support routines for the xcompute
    session functionality on top of the HPC scheduler.

--*/

#include "stdafx.h"

using namespace System::Runtime::InteropServices;
using namespace Microsoft::Research::Dryad;



/*++

XcOpenSession API

Description:

Opens an XCompute session for a given cluster. Each session 
is associated with a cluster and is independent of other sessiosn. 
The session (apart from other things) is associated with
user credientials.

It is possible to create multiple sessions for the same cluster and 
these multiple sessions will behave independent of each other. This
is particularly useful for applications like WebServer which will run
multiple sessions, one per user.

Use the XcCloseSession to close the handle returned as a result of
XcOpenSession call.

Arguments:

    pOpenSessionParams
        The Open Session Parameters. Passes info about cluster to
        establish session with, clientId, etc.
        See XC_OPEN_SESSION_PARAMS for details. 

        Pass NULL for defaults - Default cluster and a default cliend id.
        
	phSessionHandle
        Handle to session

    pAsyncInfo  
        The async info structure. Its an alias to the 
        CS_ASYNC_INFO defined in Cosmos.h. IF this 
        parameter is NULL, then function completes in 
        synchronous manner and error code is returned as 
        return value.

        If parameter is not NULL then operation is carried
        on in asynchronous manner. If asynchronous 
        operation has been successfully started then 
        function terminates immediately with 
        HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
        Any other return value indicates that it was 
        impossible to start asynchronous operation.


    Return Value:

        if pAsyncInfo is NULL
        XCERROR_OK  indicates call succeeded

        Any other error code, indicates the failure reason.


        if pAsyncInfo != NULL
        HRESULT_FROM_WIN32(ERROR_IO_PENDING) indicates the async 
        operation was successfully started    

        Any other return value indicates it was impossible to start
        asynchronous operation

--*/    
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcOpenSession(
    IN    PCXC_OPEN_SESSION_PARAMS pOpenSessionParams,
    OUT   PXCSESSIONHANDLE pSessionHandle,
    IN    PCXC_ASYNC_INFO  pAsyncInfo
)
{
    HRESULT hr = S_OK;

    *pSessionHandle = (XCSESSIONHANDLE)1;

    PASYNC async;
    CAPTURE_ASYNC(async);

    return COMPLETE_ASYNC(async, hr);
}

/*++

XcCloseSession API

Description:

Closes the session.

Arguments:

    hSessionHandle
        Handle to session to close

Return Value:
  
    XCERROR_OK  
        Call succeeded.

--*/
    
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseSession(    
    IN XCSESSIONHANDLE hSessionHandle
)
{
    VertexScheduler::GetInstance()->Shutdown(0);
    return S_OK;
}
