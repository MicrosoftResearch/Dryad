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

    locality.cpp

Abstract:

    This module contains the public interface and support routines for
    managing locality for xcompute on the HPC scheduler
--*/
#include "stdafx.h"


/*++

XcGetNetworkLocalityPathOfProcessNode

Description:

This API translates a set of process node IDs into 
network locality paths.

Arguments:

    hSession       
                        Handle to a session associated with 
                        this call

    pProcessNodeId     
                        The Process Node for which the 
                        path is required
    
    ppNetworkLocalityPath
                        Returned network locality path for the ProcessNode.
                        The pNetworkLocalityPath vector should be freed with 
                        XcFreeMemory(ppNetworkLocalityPath)

    pNetworkLocalityParam
                        The affinity param to be used to get the locality path.
                        The affinity param lets the user identify the affinity
                        level relative to the given ProcessNodeId, 
                        which is reflected in the returned ppNetworkLocalityPath.
                        Thus given a ProcessNodeId, the user might say,
                        L2Switch as the NetworkLocalityParam, which means
                        the affinity is to all process nodes under that L2Switch.

                        Different affinity params are defined in the 
                        XComputeTypes.h. See Network Locality Params for
                        more details.
  
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
XcGetNetworkLocalityPathOfProcessNode(    
    IN  XCSESSIONHANDLE     hSession,      
    IN  XCPROCESSNODEID     processNodeId,    
    IN  PSTR                pNetworkLocalityParam,
    OUT PCSTR*              ppNetworkLocalityPath
)
{
    //
    //		
    //      This is a bit of a hack - we define each machine to be its own
    //      distinct "pod". Need better locality hints in the HPC scheduler
    //      to do anything more clever.
    //
    if ((pNetworkLocalityParam == NULL) ||
        (lstrcmpiA(pNetworkLocalityParam, XCLOCALITYPARAM_POD) == 0)) {
        PCSTR pszNodeName;
        XCERROR hr = XcProcessNodeNameFromId(hSession,processNodeId,&pszNodeName);
        if (FAILED(hr)) {
            return hr;
        }
        *ppNetworkLocalityPath = ::StrDupA(pszNodeName);
    } else {
        *ppNetworkLocalityPath = ::StrDupA("E_NOTIMPL");
    }
    return S_OK;
}

