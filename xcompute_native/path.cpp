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

    path.cpp

Abstract:

    This module contains the public interface and support routines for
    mapping between global URIs and local paths for xcompute on the 
    HPC scheduler

--*/
#include "stdafx.h"
using namespace System;
using namespace System::Runtime::InteropServices;

/*++

XcGetProcessUri API

Description:

Gets the Uri to a file or directory local to XCompute process. 
The returned Uri is a fully qualified and can be used to 
create paths for file URI's  in the processes root directory or 
another directory under the root by appending path/s relative 
to the initial working directory. 

NOTE: 

The Job does not have access to directories above the 
Process's Root Directory. 
All directories e.g. Process Working Directory, Data directory
are sub directories under the Process's Root directory


Arguments:

    hProcessHandle
                The process handle for which to get the 
                Process File Uri. 

    pszRelativePath
                The path relative to process's working directory
                that will be appended to the working directory.
                NOTE: 
                If relative path is NULL. or '.' or '/', then the working directory 
                path is returned.
                
    ppszProcessRootDirUri
                The Processes Root directory Uri.
                Use the XcFreeMemory() API to free this buffer.
  
    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessUri(
    IN  XCPROCESSHANDLE hProcessHandle,
    IN  PCSTR           pszRelativePath,
    OUT PSTR*           ppszProcessRootDirUri
)
{
    PSTR FilePath = NULL;
    PSTR UriPath = NULL;

    try
    {
        PSTR prefix="file://";
        HRESULT hr = XcGetProcessPath(hProcessHandle, pszRelativePath, &FilePath);

        if (FAILED(hr))
        {
            throw System::Runtime::InteropServices::Marshal::GetExceptionForHR( hr );
        }

        size_t cbLen = strlen(prefix)+strlen(FilePath)+2;
        UriPath = (PSTR)::LocalAlloc(LMEM_FIXED, cbLen);
        if (UriPath == NULL) 
        {
            throw System::Runtime::InteropServices::Marshal::GetExceptionForHR( E_OUTOFMEMORY );
        }
        strcpy_s(UriPath, cbLen, prefix);
        strcat_s(UriPath, cbLen, FilePath);

        *ppszProcessRootDirUri = UriPath;
    }
    catch (System::Exception ^e)
    {
        // UriPath is freed on error
        if (UriPath != NULL)
        {
            XcFreeMemory(UriPath);
            UriPath = NULL;
        }

        return System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }
    finally
    {
        // FilePath is always freed
        if (FilePath != NULL)
        {
            XcFreeMemory(FilePath);
            FilePath = NULL;
        }
    }

    return S_OK;
}

/*++

XcGetProcessPath API

Description:

Gets the path to a file or directory local to XCompute process. 
The returned path is fully qualified and can be used to 
create paths for files  in the processes root directory or 
another directory under the root by appending path/s relative 
to the initial working directory. 

The returned path is suitable for passing to OS APIs like CreateFile()

NOTE: 

The Job does not have access to directories above the 
Process's Root Directory. 
All directories e.g. Process Working Directory, Data directory
are sub directories under the Process's Root directory


Arguments:

    hProcessHandle
                The process handle for which to get the 
                Process File Uri. 

    pszRelativePath
                The path relative to process's working directory
                that will be appended to the working directory.
                NOTE: 
                If relative path is NULL. or '.' or '/', then the working directory 
                path is returned.
                
    ppszProcessRootDirPath
                The Processes Root directory Uri.
                Use the XcFreeMemory() API to free this buffer.
  
    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessPath(
    IN  XCPROCESSHANDLE hProcessHandle,
    IN  PCSTR           pszRelativePath,
    OUT PSTR*           ppszProcessRootDirUri
)
{
    String ^path = Microsoft::Research::Dryad::VertexScheduler::GetInstance()->GetProcessPath((int)hProcessHandle, gcnew String(pszRelativePath));

    if (System::String::IsNullOrEmpty(path))
    {
        return E_FAIL;
    }

    *ppszProcessRootDirUri = (PSTR)Marshal::StringToHGlobalAnsi(path).ToPointer();

    return S_OK;
}
