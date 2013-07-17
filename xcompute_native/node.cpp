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

    node.cpp

Abstract:

    This module contains the public interface and support routines for
    the xcompute node functionality on top of the HPC scheduler.
--*/
#include "stdafx.h"
using namespace System;
using namespace System::Collections::Generic;
using namespace System::Collections::Specialized;
using namespace System::Runtime::InteropServices;

gcroot<System::Collections::Specialized::StringCollection ^> g_NodeList = gcnew System::Collections::Specialized::StringCollection();
CComAutoCriticalSection g_NodeListLock;

XCPROCESSNODEID
NodeNameToID(System::String ^name)
{
    String ^N = name->ToUpper();
    CComCritSecLock<CComAutoCriticalSection> lock(g_NodeListLock);
    int index = g_NodeList->IndexOf(N);
    if (index == -1) 
    {
        index = g_NodeList->Add(N);
    }
    return (XCPROCESSNODEID)(index+1);
}

System::String ^
NodeIDToName(XCPROCESSNODEID nodeID)
{
    int i = (int)nodeID;
    CComCritSecLock<CComAutoCriticalSection> lock(g_NodeListLock);
    return (String ^)g_NodeList->default[i-1];
}

/*++

XcGetProcessNodeId API

Description:

Gets the process node on which the process has been assigned.
If the process state anything other than XCPROCESSSTATE_ASSIGNEDTOPN
an error is returned.

Arguments:

    hProcessHandle
                Process handle 
    
    pProcessNodeId
                Pointer to process node Id
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessNodeId(
    IN  XCPROCESSHANDLE     hProcessHandle,
    OUT PXCPROCESSNODEID    pProcessNodeId   
)
{

    try 
    {
        if (hProcessHandle == CURRENT_PROCESS_ID)
        {
            *pProcessNodeId = NodeNameToID(Microsoft::Research::Dryad::AzureUtils::CurrentHostName);
        }
        else
        {
            System::String ^nodeName = Microsoft::Research::Dryad::VertexScheduler::GetInstance()->GetAssignedNode((int)hProcessHandle);
            if (System::String::IsNullOrEmpty(nodeName))
            {
                return E_FAIL;
            }
            *pProcessNodeId = NodeNameToID(nodeName);
        }
    } 
    catch (System::Exception ^e) 
    {
        return System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }
    return S_OK;
}

/*++

XcEnumerateProcessNodes


Description:

This API enumerates all the process nodes that are controlled
by the Process scheduler and returns an array of processNodeIds

Arguments:

    hSession       
                        Handle to a session associated with 
                        this call

    pNumNodeIds
                        Pointer to a int which gets filled with the 
                        number of process Node Ids in the 
                        ppProcessNodeIds array

    ppProcessNodeIds      
                        Pointer to array of processNode Ids. Use the
                        XcFreeMemory() API to deallocate.
  
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
XcEnumerateProcessNodes(    
    IN  XCSESSIONHANDLE     hSession,  
    OUT UINT32*             pNumNodeIds,              
    OUT PXCPROCESSNODEID*   ppProcessNodeIds,
    IN  PCXC_ASYNC_INFO     pAsyncInfo
)
{
    PASYNC async;
    CAPTURE_ASYNC(async);

    if (pNumNodeIds == NULL || ppProcessNodeIds == NULL)
    {
        return COMPLETE_ASYNC(async, E_INVALIDARG);
    }

    array<String ^> ^nodes =Microsoft::Research::Dryad::VertexScheduler::GetInstance()->EnumerateProcessNodes();

    if (nodes == nullptr)
    {
        return COMPLETE_ASYNC(async, E_UNEXPECTED);
    }

    //
    // Now get the list of allocated nodes.
    //
    int numNodes = nodes->Length;

    //
    // Allocate a buffer large enough to hold all the node IDs
    // 
    XCPROCESSNODEID *nodeIds = (XCPROCESSNODEID *)::LocalAlloc(LMEM_FIXED, numNodes * sizeof(XCPROCESSNODEID));
    if (nodeIds == NULL) 
    {
        return COMPLETE_ASYNC(async, E_OUTOFMEMORY);
    }

    int i=0;
    for each (String ^node in nodes)
    {
        nodeIds[i++] = NodeNameToID(node);
    }

    *ppProcessNodeIds = nodeIds;
    *pNumNodeIds = i;

    return COMPLETE_ASYNC(async, S_OK);
}



/*++

XcFetchProcessNodeMetaData

Description:

This API fetches the process node related metadata. This
call can result in a call to the Process Scheduler, if the 
metadata for a given process node is missing.

Arguments:

    hSession       
                        Handle to a session associated with 
                        this call

    pProcessNodeIds     
                        Array of IDs of the nodes for which the 
                        metadata is required

    numNodeIds
                        Number of node ids in the 
                        pProcessNodeIds array

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
XcFetchProcessNodeMetaData(  
    IN  XCSESSIONHANDLE     hSession,    
    IN  UINT32              numNodeIds,              
    IN  PXCPROCESSNODEID    pProcessNodeIds,    
    IN  PCXC_ASYNC_INFO     pAsyncInfo
)
{
    PASYNC async;
    CAPTURE_ASYNC(async);

    return COMPLETE_ASYNC(async, E_NOTIMPL);
}

/*++

XcGetCurrentProcessNodeId API

Description:

Gets the current Process Node Id. The Process Node Id to 
the node name map is maintained internally.

Arguments:
    
    hSession        
                    Handle to an XCompute session associated with 
                    this call.

    pProcessNodeId   
                    Pointer to Pointer of the Id of the node
    
    Return Value:
    
    CsError_OK  
                    indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetCurrentProcessNodeId(   
    IN  XCSESSIONHANDLE     hSession,     
    OUT PXCPROCESSNODEID    pProcessNodeId   
)
{
    *pProcessNodeId = NodeNameToID(Microsoft::Research::Dryad::AzureUtils::CurrentHostName);
    return S_OK;
}



/*++

XcProcessNodeIdFromName API

Description:

Gets the Process Node Id for a node given the node name. The 
Process Node Id to the node name map is maintained internally.
If a node name is not found in the internal map, then a new
entry is created and the corrosponding id is returned back

Arguments:
    
    hSession    
                Handle to an XCompute session associated with this 
                call. Reserved for future use. Must be NULL.


    pszProcessNodeName 
                Name of the process node for which Id is needed

    pProcessNodeId   
                Pointer to Pointer of the Id of the node
    
    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcProcessNodeIdFromName(    
    IN  XCSESSIONHANDLE     hSession,     
    IN  PCSTR               pszProcessNodeName,
    OUT PXCPROCESSNODEID    pProcessNodeId
)
{
    *pProcessNodeId = NodeNameToID(gcnew String(pszProcessNodeName));
    return S_OK;
}



/*++

XcProcessNodeNameFromId API

Description:

Gets the Process Node name from the given Process Node Id.The 
Process Node Id to the node name map is maintained internally.

Arguments:    

    hSession        
                    Handle to an XCompute session associated with this 
                    call. 


    processNodeId   
                    The process Node Id for which the node name 
                    is needed

    ppszProcessNodeName
                    Name of the process node corrosponding to Id 
                    Note: the returned process node name string 
                    is permanently allocated and will remain 
                    valid for the life of the process. There
                    is no need to make a copy of this string.

    Return Value:
    
    CsError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcProcessNodeNameFromId(        
    IN  XCSESSIONHANDLE     hSession,     
    IN  XCPROCESSNODEID     processNodeId,
    OUT PCSTR*              ppszProcessNodeName
)
{
    //
    // If we've returned this name before, return the
    // same pointer again.
    //
    String ^s = NodeIDToName(processNodeId);

    *ppszProcessNodeName = (PCSTR)Marshal::StringToHGlobalAnsi(s).ToPointer();
    return S_OK;
}

