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

    xcompute.cpp

Abstract:

    This module contains initialization, cleanup, and some
    utility functions for xcompute on top of the HPC scheduler.

--*/

#include "stdafx.h"

/*++

XcInitialize API

Description:

Call this function at the start to initialize the various internal
data structures of the XCompute SDK library.

Arguments:

    configFile 
        Name of the config file

    componentName
        The name of the component

Return Value:
  
    XCERROR_OK  
        Call succeeded.
    NOTE:   
        S_FALSE will be returned if the initialize 
        has already been called. 

--*/
    
DWORD g_StartTime;

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcInitialize(
    IN  PCSTR            configFile,
    IN  PCSTR            componentName
)
{
    g_StartTime = ::GetTickCount();
	return S_OK;
}


/*++

XcFreeMemory API

Description:

Frees the memory allocated by the XCompute API.
All the memory returned as a result of call to 
the XCompute API should use the XcFreeMemory to 
deallocate the memory

Arguments:

    prt 
        Pointer to the memory

Return Value:
  
    XCERROR_OK  
        Memory was successfully deallocated

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcFreeMemory(
    IN  PCVOID   ptr
)
{
    if (ptr)
    {
        LocalFree((HLOCAL)ptr);
    }
	return S_OK;
}
