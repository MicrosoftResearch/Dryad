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

    file.cpp

Abstract:

    This module contains the public interface and support routines for
    the xcompute process file functionality on top of the HPC scheduler.

--*/
#include "stdafx.h"

/*++

XcOpenProcessFile API

Description:

Opens a handle to a remote XCompute processes working File. 
Using this handle, an application can read remote files 
written by a XCompute process on a given Node.
Writing of files is not supported. Local files can be written 
using Ordinary Windows file I/O (restricted to the working 
directory and Its children).

Arguments:

    hSession        
                    Handle to an XCompute session associated with 
                    this call.

    fileUri
                the fully qualified file Uri (UTF-8) obtained by calling the 
                XcGetProcessFileUri API.

    Flags     
                Reserved. Must be 0.

    phFileHandle
                The returned handle to the opened file. 
                Set to NULL if error

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
  
    if pAsyncInfo is NULL
    CsError_OK  indicates call succeeded

    Any other error code, indicates the failure reason.


    if pAsyncInfo != NULL
    HRESULT_FROM_WIN32(ERROR_IO_PENDING) indicates the async 
    operation was successfully started    

    Any other return value indicates it was impossible to start
    asynchronous operation (a SUCCESS HRESULT will never
    be returned if pAsyncInfo is not NULL).

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcOpenProcessFile(    
    IN  XCSESSIONHANDLE         hSession,
    IN  PCSTR                   fileUri,
    IN  DWORD                   Flags,    
    OUT PXCPROCESSFILEHANDLE    phFileHandle,
    IN  PCXC_ASYNC_INFO         pAsyncInfo
)
{
    PASYNC async;
    CAPTURE_ASYNC(async);

    //
    // Our file URI is a UNC path so we can just open it directly
    //
    HANDLE hFile = ::CreateFileA(fileUri,
                                GENERIC_READ,
                                FILE_SHARE_READ | FILE_SHARE_WRITE,
                                NULL,
                                OPEN_EXISTING,
                                0,
                                NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        return COMPLETE_ASYNC(async,HRESULT_FROM_WIN32(GetLastError()));
    }

    *phFileHandle = (XCPROCESSFILEHANDLE)hFile;

    return COMPLETE_ASYNC(async, S_OK);
}



/*++

XcCloseProcessFile API

Description:

Closes the file opened by the XcOpenProcessFile

Arguments:

    hFileHandle
                The handle to the opened file. 
  
    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseProcessFile(    
    IN  XCPROCESSFILEHANDLE hFileHandle
)
{
    ::CloseHandle((HANDLE)hFileHandle);
    return S_OK;
}




/*++

XcReadProcessFile API

Description:

Reads the content of the file opened by the XcOpenProcessFile

Arguments:
    

    phFileHandle
                The handle to the opened file. 

    pBuffer     
                Pointer to the buffer that receives the data read.

    pBytesRead  
                Pointer to variable containing size of the buffer 
                on input. On return this variable receives number 
                of bytes read.

    pReadPosition  
                The offset from the beginning of the file at 
                which to read.
    
    pAsyncInfo  
                The async info structure. Its an alias to the 
                CS_ASYNC_INFO defined in Cosmos.h. If this 
                parameter is NULL, then the function completes in 
                synchronous manner and error code is returned as 
                return value.

                If parameter is not NULL then the operation is 
                carried on in asynchronous manner. If an asynchronous 
                operation has been successfully started then 
                this function terminates immediately with 
                an HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
                Any other return value indicates that it was 
                impossible to start the asynchronous operation.

  
    Return Value:
  
    if pAsyncInfo is NULL
    CsError_OK  indicates call succeeded

    Any other error code, indicates the failure reason.


    if pAsyncInfo != NULL
    HRESULT_FROM_WIN32(ERROR_IO_PENDING) indicates the async 
    operation was successfully started    

    Any other return value indicates it was impossible to start
    asynchronous operation (a SUCCESS HRESULT will never
    be returned if pAsyncInfo is not NULL).

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcReadProcessFile(        
    IN      XCPROCESSFILEHANDLE      phFileHandle,
    OUT     PVOID                    pBuffer,
    IN OUT  PSIZE_T                  pBytesRead,
    IN OUT  XCPROCESSFILEPOSITION*   pReadPosition,
    IN      PCXC_ASYNC_INFO          pAsyncInfo    
)
{
    OVERLAPPED ov = {0};
    PASYNC async;

    if (*pBytesRead > (DWORD)-1) 
    {
        return E_NOTIMPL;
    }

    CAPTURE_ASYNC(async);

    ov.Offset = (DWORD)(*pReadPosition);
    ov.OffsetHigh = (DWORD)(*pReadPosition >> 32);
    if (!::ReadFile((HANDLE)phFileHandle,
                    pBuffer,
                    (DWORD)*pBytesRead,
                    (LPDWORD)pBytesRead,
                    &ov)) {

        return COMPLETE_ASYNC(async,HRESULT_FROM_WIN32(GetLastError()));
    }
    return COMPLETE_ASYNC(async, S_OK);
}


