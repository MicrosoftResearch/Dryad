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

    property.cpp

Abstract:

    This module contains the public interface and support routines for
    process properties for xcompute on the HPC scheduler

--*/
#include "stdafx.h"
//#include <scoped_any.h>

using namespace System;
using namespace System::Threading;
using namespace System::Runtime::InteropServices;

//
// Prototypes for functions internal to this module
//
HRESULT
SetGetProps(
            XCPROCESSHANDLE hProcessHandle,
            long cPropCount, 
            PXC_PROCESSPROPERTY_INFO setProp[], 
            LPCSTR blockOnLabel,
            UINT64 blockOnVersion,
            XCTIMEINTERVAL maxBlockTime,
            LPCSTR getPropLabel,
            DWORD fetchOptions,
            PXC_SETANDGETPROCESSINFO_REQRESULTS *presults,
            PCXC_ASYNC_INFO pAsyncInfo
            );

static void ConvertManagedProcessInfoToNative(ProcessInfo ^managedInfo, PXC_PROCESS_INFO &pNativeInfo)
{
    char *strTemp = NULL;

    pNativeInfo = (PXC_PROCESS_INFO)::LocalAlloc(LMEM_FIXED, sizeof(XC_PROCESS_INFO));
    ZeroMemory(pNativeInfo, sizeof(XC_PROCESS_INFO));

    pNativeInfo->Size = sizeof(XC_PROCESS_INFO);
    pNativeInfo->Flags = managedInfo->flags;
    pNativeInfo->ProcessState = TranslateProcessState(managedInfo->processState);
    pNativeInfo->ProcessStatus = managedInfo->processStatus;
    pNativeInfo->ExitCode = managedInfo->exitCode;

    if (managedInfo->propertyInfos != nullptr)
    {
        pNativeInfo->ppProperties = (PXC_PROCESSPROPERTY_INFO*)LocalAlloc(LMEM_FIXED, sizeof(PXC_PROCESSPROPERTY_INFO) * managedInfo->propertyInfos->Length);
        pNativeInfo->NumberofProcessProperties = managedInfo->propertyInfos->Length;

        for (int i = 0; i < managedInfo->propertyInfos->Length; i ++)
        {
            PXC_PROCESSPROPERTY_INFO processProp = (XC_PROCESSPROPERTY_INFO*)LocalAlloc(LMEM_FIXED, sizeof(XC_PROCESSPROPERTY_INFO));
            ZeroMemory(processProp, sizeof(XC_PROCESSPROPERTY_INFO));
            processProp->Size = sizeof(XC_PROCESSPROPERTY_INFO);

            size_t len = managedInfo->propertyInfos[i]->propertyLabel->Length + 1;
            strTemp = (char*)(void*)Marshal::StringToHGlobalAnsi(managedInfo->propertyInfos[i]->propertyLabel);
            processProp->pPropertyLabel = (char*)LocalAlloc(LMEM_FIXED, sizeof(char) * len);
            strncpy(processProp->pPropertyLabel, strTemp, len);
            Marshal::FreeHGlobal((IntPtr)strTemp);
            strTemp = NULL;

            len = managedInfo->propertyInfos[i]->propertyString->Length + 1;
            strTemp = (char*)(void*)Marshal::StringToHGlobalAnsi(managedInfo->propertyInfos[i]->propertyString);
            processProp->pPropertyString = (char*)LocalAlloc(LMEM_FIXED, sizeof(char) * len);
            strncpy(processProp->pPropertyString, strTemp, len);
            Marshal::FreeHGlobal((IntPtr)strTemp);
            strTemp = NULL;

            processProp->PropertyVersion = managedInfo->propertyInfos[i]->propertyVersion;

            if (managedInfo->propertyInfos[i]->propertyBlock != nullptr && managedInfo->propertyInfos[i]->propertyBlock->Length > 0)
            {
                processProp->PropertyBlockSize = managedInfo->propertyInfos[i]->propertyBlock->Length;
                processProp->pPropertyBlock = (char*)LocalAlloc(LMEM_FIXED, sizeof(char) * processProp->PropertyBlockSize);
                Marshal::Copy(managedInfo->propertyInfos[i]->propertyBlock, 
                    0, 
                    (IntPtr)processProp->pPropertyBlock, 
                    processProp->PropertyBlockSize);
            }

            pNativeInfo->ppProperties[i] = processProp;
        }
    }

    if (managedInfo->processStatistics != nullptr)
    {
        PXC_PROCESS_STATISTICS stats = (XC_PROCESS_STATISTICS*)LocalAlloc(LMEM_FIXED, sizeof(XC_PROCESS_STATISTICS));
        ZeroMemory(stats, sizeof(XC_PROCESS_STATISTICS));

        stats->Size = sizeof(XC_PROCESS_STATISTICS);
        stats->Flags = managedInfo->processStatistics->flags;
        stats->ProcessUserTime = managedInfo->processStatistics->processUserTime;
        stats->ProcessKernelTime = managedInfo->processStatistics->processKernelTime;
        stats->PageFaults = managedInfo->processStatistics->pageFaults;
        stats->TotalProcessesCreated = managedInfo->processStatistics->totalProcessesCreated;
        stats->PeakVMUsage = managedInfo->processStatistics->peakVMUsage;
        stats->PeakMemUsage = managedInfo->processStatistics->peakMemUsage;
        stats->MemUsageSeconds = managedInfo->processStatistics->memUsageSeconds;
        stats->TotalIo = managedInfo->processStatistics->totalIo;

        pNativeInfo->pProcessStatistics = stats;
    }

    if (strTemp != NULL)
    {
        Marshal::FreeHGlobal((IntPtr)strTemp);
        strTemp = NULL;
    }
}


ref class AsyncPropWrapper 
{
public:
    ASYNC *m_pASYNC;
    ManualResetEvent ^m_event;
    XCPROCESSHANDLE m_process;
    PXC_SETANDGETPROCESSINFO_REQRESULTS *m_ppResults;
    HRESULT m_hResult;

    AsyncPropWrapper(ASYNC *pAsync, XCPROCESSHANDLE hProc, PXC_SETANDGETPROCESSINFO_REQRESULTS *ppResults)
    {
        m_pASYNC = pAsync;
        m_event = nullptr;
        if (m_pASYNC == NULL)
        {
            m_event = gcnew ManualResetEvent(false);
        }
        m_process = hProc;
        m_ppResults = ppResults;
        m_hResult = HRESULT_FROM_WIN32(ERROR_IO_PENDING);
    }

    ~AsyncPropWrapper()
    {
        if (m_pASYNC)
        {
            delete m_pASYNC;
        }
    }

    void GetSetPropertyHandler(System::Object ^sender, Microsoft::Research::Dryad::XComputeProcessGetSetPropertyEventArgs ^e)
    {
        if (m_ppResults != NULL)
        {
            PXC_SETANDGETPROCESSINFO_REQRESULTS pResults = (PXC_SETANDGETPROCESSINFO_REQRESULTS)::LocalAlloc(LMEM_FIXED, sizeof(XC_SETANDGETPROCESSINFO_REQRESULTS));
            if (pResults == NULL  )
            {
                m_hResult = E_OUTOFMEMORY;
                goto Exit;
            }
            ZeroMemory(pResults, sizeof(XC_SETANDGETPROCESSINFO_REQRESULTS));

            try
            {
                if (e->ProcessInfo != nullptr)
                {
                    ConvertManagedProcessInfoToNative(e->ProcessInfo, pResults->pProcessInfo);
                }

                if (e->PropertyVersions != nullptr && e->PropertyVersions->Length > 0)
                {
                    pResults->NumberOfPropertyVersions = e->PropertyVersions->Length;
                    pResults->pPropertyVersions = (UINT64 *)LocalAlloc(LMEM_FIXED, sizeof(UINT64) * e->PropertyVersions->Length);
                    for (UINT32 i = 0; i < pResults->NumberOfPropertyVersions; i++)
                    {
                        pResults->pPropertyVersions[i] = e->PropertyVersions[i];
                    }
                }
            }
            catch(Exception ^e)
            {
                m_hResult = Marshal::GetHRForException(e);
                Console::WriteLine("[XComputeNative.GetSetPropertyHandler] Exception: {0}", e->Message);
                goto Exit;
            }

            *m_ppResults = pResults;
        }

        m_hResult = S_OK;

Exit:
        if (m_pASYNC)
        {
            m_pASYNC->Complete(m_hResult);
        }
        else
        {
            m_event->Set();
        }
    }
};


HRESULT
SetAppProcessConstraints(
    IN XCPROCESSHANDLE hProcess,
    IN PCXC_PROCESS_CONSTRAINTS Constraints
    )
{
    // TODO implement this! (return success for now because Dryad tries to dink with the runtime
    return S_OK;
}

// #pragma unmanaged
// push managed state on to stack and set unmanaged state
#pragma managed(push, off)
/*++

XcSetAndGetProcessInfo API

Description:

Gets the process related information from the Process Node.
JobManager (e.g. Dryad Job manager), will use this API to get
information about a given XCompute process, of a job.
Various bit flags (explained below) control the amount of data
retreived for a given process
It also provides the user with the ability to block on a 
particular property, for maxBlockTime amount of time, before the
API finishes (synchronously or asynchronously). Dryad uses this
to extend the lease period for a given process

Arguments:

    hProcessHandle
                        Handle to the process. 
                        Use the XcCreateNewProcessHandle () API
                        to get obtain the handle to the process

    pXcRequestInputs    
                        Pointer to the 
                        XC_SETANDGETPROCESSINFO_REQINPUT struct.
                        It contains the various inputs to the API 
                        clubbed together. This structure needs to
                        be preserverd by the user till the Async
                        call is completed  

    ppXcRequestResults   
                        The results structure.The user should use 
                        the XcFreeMemory(ppXcPnProcessInfo) to free 
                        the memory after the results have been 
                        consumed. 
                        See PXC_SETANDGETPROCESSINFO_REQRESULTS for
                        more info.                    
  
    pAsyncInfo    
                        The async info structure. Its an alias to 
                        the CS_ASYNC_INFO defined in Cosmos.h. If
                        this parameter is NULL, then function 
                        completes in synchronous manner and error 
                        code is returned as return value.

                        If parameter is not NULL then operation is 
                        carried on in asynchronous manner. If 
                        asynchronous operation has been successfully 
                        started then  function terminates 
                        immediately with 
                        HRESULT_FROM_WIN32(ERROR_IO_PENDING) return 
                        value.

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
XcSetAndGetProcessInfo(    
    IN  XCPROCESSHANDLE                         hProcessHandle,  
    IN  PXC_SETANDGETPROCESSINFO_REQINPUT       pXcRequestInputs,    
    OUT PXC_SETANDGETPROCESSINFO_REQRESULTS*    ppXcRequestResults,
    IN  PCXC_ASYNC_INFO                         pAsyncInfo
)
{
    HRESULT hr;
    printf("In native XcSetAndGetProcessInfo\n");
    // check for malformed input
    if (pXcRequestInputs->Size < sizeof(XC_SETANDGETPROCESSINFO_REQINPUT))
    {
        return E_NOTIMPL;
    }

    // no support for process constraints
    if (pXcRequestInputs->pAppProcessConstraints != NULL)
    {
        hr = SetAppProcessConstraints(hProcessHandle, pXcRequestInputs->pAppProcessConstraints);
        if (FAILED(hr))
        {
            return hr;
        }
    }
    *ppXcRequestResults = NULL;

    if (hProcessHandle == CURRENT_PROCESS_ID)
    {
        // We need to use the real dryad process id so the vertex service host can find it
        CStringA procId;
        procId.GetEnvironmentVariable("CCP_DRYADPROCID");
        hProcessHandle = (XCPROCESSHANDLE)atoi((LPCSTR)procId);
    }

    hr = SetGetProps(
        hProcessHandle,
        pXcRequestInputs->NumberOfProcessPropertiesToSet,
        pXcRequestInputs->ppPropertiesToSet,
        pXcRequestInputs->pBlockOnPropertyLabel,
        pXcRequestInputs->BlockOnPropertyversionLastSeen,
        pXcRequestInputs->MaxBlockTime,
        pXcRequestInputs->pPropertyFetchTemplate,
        pXcRequestInputs->ProcessInfoFetchOptions,
        ppXcRequestResults,
        pAsyncInfo);

    return hr;
}
// #pragma managed
#pragma managed(pop)


HRESULT
SetGetProps(
            XCPROCESSHANDLE hProcessHandle,
            long cPropCount, 
            PXC_PROCESSPROPERTY_INFO setProp[], 
            LPCSTR blockOnLabel,
            UINT64 blockOnVersion,
            XCTIMEINTERVAL maxBlockTime,
            LPCSTR getPropLabel,
            DWORD fetchOptions,
            PXC_SETANDGETPROCESSINFO_REQRESULTS *ppResults,
            PCXC_ASYNC_INFO pAsyncInfo
            )
{
    PASYNC async;
    CAPTURE_ASYNC(async);
    HRESULT hr = S_OK;

    // Build a managed ProcessPropertyInfo array from the setProp[] array
    array<ProcessPropertyInfo ^> ^infos = gcnew array<ProcessPropertyInfo ^>(cPropCount);
    for (int i = 0; i < cPropCount; i++)
    {
        ProcessPropertyInfo ^info = gcnew ProcessPropertyInfo();
        info->propertyLabel = gcnew String(setProp[i]->pPropertyLabel);
        info->propertyVersion = setProp[i]->PropertyVersion;
        info->propertyString = gcnew String(setProp[i]->pPropertyString);
        if (setProp[i]->PropertyBlockSize > 0)
        {
            info->propertyBlock = gcnew array<byte>(setProp[i]->PropertyBlockSize);
            Marshal::Copy((IntPtr)setProp[i]->pPropertyBlock, info->propertyBlock, 0, setProp[i]->PropertyBlockSize);
        }

        infos[i] = info;
    }


    AsyncPropWrapper ^wrapper = gcnew AsyncPropWrapper(async, hProcessHandle, ppResults);
    GetSetPropertyEventHandler ^handler = gcnew GetSetPropertyEventHandler(wrapper, &AsyncPropWrapper::GetSetPropertyHandler);

    VertexScheduler ^vs = VertexScheduler::GetInstance();
    bool bRetVal = vs->SetGetProps(
        (int)hProcessHandle,
        infos,
        gcnew String(blockOnLabel),
        blockOnVersion,
        maxBlockTime,
        gcnew String(getPropLabel),
        (fetchOptions & XCPROCESSINFOOPTION_PROCESSSTAT) != 0,
        handler);

    if (pAsyncInfo == NULL)
    {
        wrapper->m_event->WaitOne();
        return wrapper->m_hResult;
    }
    else
    {
        return HRESULT_FROM_WIN32(ERROR_IO_PENDING);
    }

}
