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

    async.cpp

Abstract:

    This module contains support routines for implementing the 
    the xcompute async completion notification.

--*/
#include "stdafx.h"


//
// ValidateAsync(PCCS_ASYNC_INFO asyncInfo)
//      Does some basic validation of the CS_ASYNC_INFO structure
//
//      returns S_OK if it's well-formed, E_INVALIDARG otherwise
//
HRESULT
ValidateAsync(
    PCXC_ASYNC_INFO async
    )
{
    if (async==NULL) {
        return S_OK;
    }
    if (async->Size < sizeof(XC_ASYNC_INFO)) {
        return E_INVALIDARG;
    }
    if (async->pOperationState == NULL) {
        return E_INVALIDARG;
    }

    //
    // If an IOCP was supplied, there better be a pOverlapped and CompletionKey
    // to post to it
    //
    if (async->IOCP != NULL) 
    {
        if  ((async->pOverlapped == NULL) || (async->CompletionKey == NULL)) {
            return E_INVALIDARG;
        }
    } else {

        //
        // If no IOCP was supplied, there also must not be a pOverlapped.
        //
        if (async->pOverlapped != NULL) {
            return E_INVALIDARG;
        }
    }
    return S_OK;
}

ASYNC::ASYNC(
    PCXC_ASYNC_INFO pAsyncInfo
    )
{


    //
    // Capture the supplied completion information
    //
    this->pOperationState = pAsyncInfo->pOperationState;
    this->hEvent = pAsyncInfo->Event;
    this->hIOCP = pAsyncInfo->IOCP;
    this->pOverlapped = pAsyncInfo->pOverlapped;
    this->CompletionKey = pAsyncInfo->CompletionKey;
}

ASYNC::~ASYNC()
{
}

HRESULT 
ASYNC::Complete(
    HRESULT hr
    )
{


    //
    // Indicate status
    //
    *pOperationState = hr;

    //
    // Set the event (if present)
    //
    if (hEvent) {
        ::SetEvent(hEvent);
    }

    //
    // Post to the IOCP (if present)
    //
    if (hIOCP) {
        PostQueuedCompletionStatus(hIOCP,
                                   0,
                                   CompletionKey,
                                   pOverlapped);
    }

    delete this;

    //
    // Always return pending
    //
    return HRESULT_FROM_WIN32(ERROR_IO_PENDING);
}
