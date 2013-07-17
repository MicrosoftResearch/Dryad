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

    scheduler.cpp

Abstract:

    This module contains the public interface and support routines for
    the xcompute scheduling functionality on top of the HPC scheduler.

--*/
#include "stdafx.h"
using namespace System;
using namespace System::Threading;
using namespace System::Collections::Generic;
using namespace System::Collections::Specialized;
using namespace System::Runtime::InteropServices;
using namespace Microsoft::Research::Dryad;

/*++

XcScheduleProcess API

Description:

Contacts the Process Scheduler to schedule an XCompute Process.
Any XCompute Process in a Job may schedule additional 
XCompute Processes in the same Job by requesting their creation 
through the XCompute Process Scheduler, using this API.

NOTE: 
This call always returns immediately.
A successful return code from the API indicates that the 
XcScheduleProcess request was added to the local scheduleProcess queue.
The user should use the XcWaitForStateChange(XCPROCESSSTATE_ASSIGNEDTOPN)
API to see when the process actually gets scheduled to the Process Scheduler

Arguments:

    hProcessHandle


                Handle to the process. 
                Use the XcCreateNewProcessHandle () API
                to get obtain the handle to the process

    pScheduleProcessDescriptor
                See PCXC_SCHEDULEPROCESS_DESCRIPTOR in 
                XComputeTypes.h. This datastructure is 
                copied before the function returns and
                so it is not necessary for the caller
                to preserve the contents during a 
                async call
  
    Return Value:
  
    S_OK indicating the operation was successfully started.     

    Any other return value indicates the scheduleprocess request 
    could not be started

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcScheduleProcess(    
    IN  XCPROCESSHANDLE                 hProcessHandle,  
    IN  PCXC_SCHEDULEPROCESS_DESCRIPTOR pScheduleProcessDescriptor
)
{
    HRESULT hr = S_OK;

    try 
    {
        if (pScheduleProcessDescriptor->Size != sizeof(XC_SCHEDULEPROCESS_DESCRIPTOR)) 
        {
            return E_INVALIDARG;
        }
        PCXC_CREATEPROCESS_DESCRIPTOR proc = pScheduleProcessDescriptor->pCreateProcessDescriptor;
        if (proc->Size != sizeof(XC_CREATEPROCESS_DESCRIPTOR)) 
        {
            return E_INVALIDARG;
        }

        //
        // Set command line and friendly name
        //
        StringDictionary ^env = gcnew StringDictionary();

        String ^cmdLine = gcnew String(proc->pCommandLine);
        String ^name = gcnew String(proc->pProcessFriendlyName);

        // 
        // Walk through the list of environment strings and set each one
        //
        PCSTR envValue = NULL;
        PCSTR envName = proc->pEnvironmentStrings;
        
        while (envName && *envName) 
        {
            envValue = envName + strlen(envName) + 1;

            env->Add(gcnew System::String(envName), gcnew System::String(envValue));
            envName = envValue + strlen(envValue) + 1;
        }


        //
        // If the caller asked for a single machine affinity, and it's a hard affinity,
        // then put the machine requirement on the task. Otherwise let the job scheduler
        // pick whatever it likes.
        //

        List<SoftAffinity^> ^requestedNodes = gcnew List<SoftAffinity^>();
        String ^requiredNode = nullptr;

        if (pScheduleProcessDescriptor->pLocalityDescriptor)
        {
            if (pScheduleProcessDescriptor->pLocalityDescriptor->NumberOfAffinities == 1 &&
                (pScheduleProcessDescriptor->pLocalityDescriptor->pAffinities[0].Flags & XCAFFINITY_HARD))
            {
                requiredNode = gcnew String(pScheduleProcessDescriptor->pLocalityDescriptor->pAffinities->pNetworkLocalityPaths[0]);
            }
            else
            {
                for (unsigned int i = 0; i < pScheduleProcessDescriptor->pLocalityDescriptor->NumberOfAffinities; i++)
                {
                    if (pScheduleProcessDescriptor->pLocalityDescriptor->pAffinities[i].NumberOfNetworkLocalityPaths > 0)
                    {
                        String ^machineName = gcnew String(pScheduleProcessDescriptor->pLocalityDescriptor->pAffinities[i].pNetworkLocalityPaths[0]);

                        if (!machineName->Equals(Microsoft::Research::Dryad::AzureUtils::CurrentHostName, StringComparison::OrdinalIgnoreCase))
                        {
                            SoftAffinity ^affinity = gcnew SoftAffinity(
                                machineName, 
                                pScheduleProcessDescriptor->pLocalityDescriptor->pAffinities[i].Weight
                                );
                            requestedNodes->Add(affinity);
                        }
                    }
                }
            }
        }


        VertexScheduler ^client = VertexScheduler::GetInstance();

        if ( client->ScheduleProcess((int)hProcessHandle, cmdLine, requestedNodes, requiredNode, env))
        {
            hr = S_OK;
        }
        else
        {
            hr = E_FAIL;
        }
    } 
    catch (Exception ^e) 
    {
        hr = System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }

    return hr;
}




/*++

XcCancelScheduleProcess API

Description:

Contacts the Process Scheduler to cancel the scheduled 
XCompute Process. This API is used by the Parent XCompute process
that originally scheduled the XCompute process to cancel its 
creation.
NOTE: The XCompute process will get cancelled, only if has not
already been created on a process node. The returned error code
indicates whether the process was successfully cancelled or not.

Arguments:

    hProcessHandle
                    Handle to the process. 

    pProcessId   
                The processId that needs to be cancelled    

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
XcCancelScheduleProcess(    
    IN  XCPROCESSHANDLE hProcessHandle,  
    IN  PCXC_ASYNC_INFO pAsyncInfo
)
{
    PASYNC async;
    CAPTURE_ASYNC(async);

    Microsoft::Research::Dryad::VertexScheduler::GetInstance()->CancelScheduleProcess((int)hProcessHandle);

    return COMPLETE_ASYNC(async, S_OK);
}
