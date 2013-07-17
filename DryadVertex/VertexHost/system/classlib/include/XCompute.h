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

#pragma once

#pragma warning( push )
/* 'X' bytes padding added after member 'Y' */
#pragma warning( disable: 4820 )


#pragma pack( push, 8 )


#if !defined(_PCVOID_DEFINED)
typedef const void* PCVOID;
#define _PCVOID_DEFINED
#endif


#include <XComputeTypes.h>


#if defined(__cplusplus)
extern "C" {
#endif



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
        
    pSessionHandle
        Handle to session

    pAsyncInfo  
        The async info structure. Its an alias to the 
        DR_ASYNC_INFO defined in Dryad.h. IF this 
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
    OUT   PXDRESSIONHANDLE pSessionHandle,
    IN    PCXC_ASYNC_INFO  pAsyncInfo
);



/*++

XcCloseSession API

Description:

Closes the session.

Arguments:

    SessionHandle
        Handle to session to close

Return Value:
  
    XCERROR_OK  
        Call succeeded.

--*/
    
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseSession(    
    IN XDRESSIONHANDLE SessionHandle
);



/*++

XcInitialize API

Description:

Call this function at the start to initialize the various internal
data structures of the XCompute SDK library.

Arguments:

    ConfigFile 
        Name of the config file

    ComponentName
        The name of the component

Return Value:
  
    XCERROR_OK  
        Call succeeded.
    NOTE:   
        S_FALSE will be returned if the initialize 
        has already been called. 

--*/
    
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcInitialize(
    IN  PCSTR            ConfigFileName,
    IN  PCSTR            ComponentName
);



/*++

XcFreeMemory API

Description:

Frees the memory allocated by the XCompute API.
All the memory returned as a result of call to 
the XCompute API should use the XcFreeMemory to 
deallocate the memory

Arguments:

    pMem 
        Pointer to the memory

Return Value:
  
    XCERROR_OK  
        Memory was successfully deallocated

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcFreeMemory(
    IN  PCVOID   pMem
);



/*++

XcCreateNewProcessHandle API

Description:

Creates a new process handle for a new process in the
given Job. 

This call is synchronous and does not cross
machine boundaries/process boundaries.

Note: 
1.  
    This method just creates the handle to 
    the XCompute process. It does not schedule the process itself.
    Use the XcScheduleProcessAPI to schedule the XCompute process.

2.  
    Use the XcCloseProcessHandle() to free the handle

3.
    Do not copy handle using the simple assignment operator.Use the 
    DuplicateProcessHandle() API. Each handle variable needs to be 
    freed using the XcCloseProcessHandle().

Arguments:

    SessionHandle            
                Handle to a session associated with this call                

    pJobId   
                The Id of the job under which the process will
                be created. A NULL value will cause the current
                processes JobId to be automatically picked up.
                NOTE: 
                This parameter is only interesting to the Task Scheduler.
                For all other cases, it should be assined to NULL

    pProcessHandle
                The handle to the process.


Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCreateNewProcessHandle(
    IN  XDRESSIONHANDLE     SessionHandle,  
    IN  const GUID*         pJobId,  
    OUT PXCPROCESSHANDLE    pProcessHandle
);



/*++

XcOpenCurrentProcessHandle API

Description:

Opens the current processes handle 

This call is synchronous and does not cross
machine boundaries/process boundaries.

Note: 
1.  
    This method creates the handle to the current process and assigns
    it to the session on that process.

2.  
    Use the XcCloseProcessHandle() to free the handle

3.
    Do not copy handle using the simple assignment operator.Use the 
    DuplicateProcessHandle() API. Each handle variable needs to be 
    freed using the XcCloseProcessHandle().

Arguments:

    SessionHandle            
                Handle to a session associated with this call.

    pProcessHandle
                The handle to the process. This must be closed using the 
                XcClosePorcessHandle()

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcOpenCurrentProcessHandle(
    IN  XDRESSIONHANDLE     SessionHandle,  
    OUT PXCPROCESSHANDLE    pProcessHandle
);



/*++

XcCloseProcessHandle API

Description:

Closes a process handle created either by a call to 
XcCreateNewProcessHandle() or XcDupProcessHandle().

This call is synchronous and does not cross
machine boundaries/process boundaries.


NOTE: 
Every call to the XcCreateNewProcessHandle() or
DupProcessHandle() should ultimately
result in a call to XcCloseProcessHandle() to deallocated the handle.

Arguments:

    ProcessHandle
                Process handle to be closed

Return Value:
  
    XCERROR_OK  
                The call succeded


--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseProcessHandle (
    IN  XCPROCESSHANDLE     ProcessHandle
);



/*++

XcDupProcessHandle API

Description:

Duplicates a process handle. Use this api, if a copy of the 
process handle is needed.

This call is synchronous and does not cross
machine boundaries/process boundaries.

NOTE: 
    a. Every call to the DupProcessHandle should ultimately result
        in a call to XcCloseProcessHandle() to deallocated the handle.

Arguments:

    ProcessHandle
                Process handle to be duplicated

    pDupProcessHandle
                The duplicated process handle

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcDupProcessHandle(
    IN  XCPROCESSHANDLE     ProcessHandle,
    OUT PXCPROCESSHANDLE    pDupProcessHandle
);



/*++

XcSerializeProcessHandle API

Description:

Creates a serialized process handle. A XCompute process can serialize a 
process handle, and pass it to another XCompute process where the other
XCompute process can use the XcUnSerializeProcessHandle() API, to
recreate the process handle. Then it can use that process handle to 
communicate with the process. e.g by using XcSetAndGetProcessInfo() API

Arguments:

    ProcessHandle
        The handle to the process to serialize.

    ppXcSerializedHandleBlock
        The serialized process handle. Use the XcFreeMemory() API 
        to de-allocated the pXcSerializedHandleBlock.
    
    pBlockLength
        The length in bytes of the serialized process handle block

NOTE:
    The UserContext assiciated with the process handle will *NOT*
    be serialzed.
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcSerializeProcessHandle (
    IN  XCPROCESSHANDLE ProcessHandle,
    OUT PCVOID*         ppXcSerializedHandleBlock,    
    OUT PSIZE_T         pBlockLength
);



/*++

XcUnSerializeProcessHandle API

Description:

Un-serializes a serialized process handle. See XcSerializeProcessHandle() API
for more details

Arguments:

    SessionHandle
        The session to which to associate the un-serialized process handle with.

    pXcSerializedHandleBlock
        The serialized process handle.
    
    pBlockLength
        The length in bytes of the serialized process handle block

    pProcessHandle
        The un-serialized process handle.
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcUnSerializeProcessHandle (
    IN  XDRESSIONHANDLE     SessionHandle,
    IN  PCVOID              pXcSerializedHandleBlock,
    IN  SIZE_T              BlockLength,
    OUT PXCPROCESSHANDLE    pProcessHandle
);



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

    ProcessHandle
                Process handle to identify process to which user context 
                is being associated

    pUserContext
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
    IN  XCPROCESSHANDLE ProcessHandle,
    IN  ULONG_PTR       pUserContext,    
    OUT ULONG_PTR*      pPreviousUserContext
);



/*++

XcGetProcessUserContext API

Description:

Gets the API user related data associated with the XCompute Process.
The API user can associate any data with the XCompute process via the 
XcAddUserContextToHandle API.

This call is synchronous and does not cross
machine boundaries/process boundaries.

Arguments:

    ProcessHandle
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
    IN  XCPROCESSHANDLE ProcessHandle,
    OUT ULONG_PTR*      pUserContext
);



/*++

XcGetProcessState API

Description:

Gets the process state information. If Schedule process is not
yet been called, the API will return error.

This call is synchronous and does not cross
machine boundaries/process boundaries.

Arguments:

    ProcessHandle
                Process handle 
    
    pProcessState
                Describes the process state. The different states
                are described in XComputeTypes.h

    pProcessSchedulingError
                if process state is XCPROCESSSTATE_COMPLETED
                then the error code indicates reson.
                S_OK means process compeleted without errors.
                Other error codes indicate reasons for failed completion.

Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessState(
    IN  XCPROCESSHANDLE ProcessHandle,
    OUT PXCPROCESSSTATE pProcessState,
    OUT XCERROR*        pProcessSchedulingError
);



/*++

XcGetProcessId API

Description:

Gets the process Id of the process associated with the process handle. 
If the process state is anything less than XCPROCESSSTATE_ASSIGNEDTOPN
an error is returned.

Arguments:

    ProcessHandle
                Process handle 
    
    
    pProcessId   
                The id of the process
    
Return Value:
  
    XCERROR_OK  
                The call succeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessId(
    IN  XCPROCESSHANDLE ProcessHandle,
    OUT GUID*           pProcessId   
);



/*++

XcGetProcessNodeId API

Description:

Gets the process node on which the process has been assigned.
If the process state anything other than XCPROCESSSTATE_ASSIGNEDTOPN
an error is returned.

Arguments:

    ProcessHandle
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
    IN  XCPROCESSHANDLE     ProcessHandle,
    OUT PXCPROCESSNODEID    pProcessNodeId   
);



/*++

ProcessScheduler API

--*/



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

    ProcessHandle
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
    IN  XCPROCESSHANDLE                 ProcessHandle,  
    IN  PCXC_SCHEDULEPROCESS_DESCRIPTOR pScheduleProcessDescriptor
);




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

    ProcessHandle
                    Handle to the process. 
    
    pAsyncInfo  
                The async info structure. Its an alias to the 
                DR_ASYNC_INFO defined in Dryad.h. IF this 
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
    IN  XCPROCESSHANDLE ProcessHandle,  
    IN  PCXC_ASYNC_INFO pAsyncInfo
);



/*++

PN API

--*/

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

    ProcessHandle
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
                        the DR_ASYNC_INFO defined in Dryad.h. If
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
    IN  XCPROCESSHANDLE                         ProcessHandle,  
    IN  PXC_SETANDGETPROCESSINFO_REQINPUT       pXcRequestInputs,    
    OUT PXC_SETANDGETPROCESSINFO_REQRESULTS*    ppXcRequestResults,
    IN  PCXC_ASYNC_INFO                         pAsyncInfo
);



/*++

XcGetNetworkLocalityPathOfProcessNode

Description:

This API translates a set of process node IDs into 
network locality paths.

Arguments:

    SessionHandle       
                        Handle to a session associated with 
                        this call

    ProcessNodeId     
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
    IN  XDRESSIONHANDLE     SessionHandle,      
    IN  XCPROCESSNODEID     ProcessNodeId,    
    IN  PSTR                pNetworkLocalityParam,
    OUT PCSTR*              ppNetworkLocalityPath
);



/*++

XcEnumerateProcessNodes


Description:

This API enumerates all the process nodes that are controlled
by the Process scheduler and returns an array of processNodeIds

Arguments:

    SessionHandle       
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
                        the DR_ASYNC_INFO defined in Dryad.h. If
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
    IN  XDRESSIONHANDLE     SessionHandle,  
    OUT UINT32*             pNumNodeIds,              
    OUT PXCPROCESSNODEID*   ppProcessNodeIds,
    IN  PCXC_ASYNC_INFO     pAsyncInfo
);



/*++

XcFetchProcessNodeMetaData

Description:

This API fetches the process node related metadata. This
call can result in a call to the Process Scheduler, if the 
metadata for a given process node is missing.

Arguments:

    SessionHandle
                        Handle to a session associated with 
                        this call

    pProcessNodeIds     
                        Array of IDs of the nodes for which the 
                        metadata is required

    NumNodeIds
                        Number of node ids in the 
                        pProcessNodeIds array

    pAsyncInfo    
                        The async info structure. Its an alias to 
                        the DR_ASYNC_INFO defined in Dryad.h. If
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
    IN  XDRESSIONHANDLE     SessionHandle,    
    IN  UINT32              NumNodeIds,              
    IN  PXCPROCESSNODEID    pProcessNodeIds,    
    IN  PCXC_ASYNC_INFO     pAsyncInfo
);



/*++

    Notification/Sync API

--*/



/*++

XcWaitForStateChange API

Description:

The API allows users to get async completion status for 
XCompute process when it reaches a desired state. (see XCPROCESSSTATE) 
When the desired state is reached the async completion is dispatched.

NOTE: 
1.  If the process gets cancelled, then completion is dispatched immediately
2.  The pOperationState of the AsyncInfo will have the error code.

Arguments:    

    ProcessHandle        
                Handle to an XCompute process for which the 
                state change event is needed

    WaitForState
                The state to wait for the XCompute to be in, so
                that completion can be dispatched

    MaxWaitInterval
                The maximum amount of time (not including network 
                request latencies) that the API should wait for a 
                change in the process list before completing. If 
                XCTIMEINTERVAL_ZERO, the API will return changes
                that can be immediately determined without  
                communication with the process scheduler. If 
                XC_TIMEINTERVAL_INFINITE, the API will wait until a 
                change occurs or the process is cancelled.

    pAsyncInfo  
                The async info structure. Its an alias to the 
                DR_ASYNC_INFO defined in Dryad.h. If this 
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
    
    DrError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcWaitForStateChange(        
    IN  XCPROCESSHANDLE ProcessHandle,  
    IN  XCPROCESSSTATE  WaitForState,
    IN  XCTIMEINTERVAL  MaxWaitInterval,
    IN  PCXC_ASYNC_INFO pAsyncInfo
);



/*++

    XCompute File access API.

--*/



/*++

XcGetWorkingDirectoryProcessUri API

Description:

Gets a URI to a file or directory within an XCompute process's initial 
working directory.
The returned Uri is fully qualified and can be used to 
create paths for file URI's  in the process's working directory,
by appending file names to the WorkingDirectory Uri.

Arguments:

    ProcessHandle
                The process handle for which to get the 
                Process Working directory Uri
    
    pRelativePath
                The path relative to process's working directory
                that will be appended to the working directory.
                NOTE: 
                If relative path is NULL. or '.' or'/', then the working directory 
                path is returned.

    ppProcessWdUri
                The fully qualified working directory Uri,
                Use the XcFreeMemory() API to free this buffer
 
    Return Value:
    
    DrError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetWorkingDirectoryProcessUri(
    IN  XCPROCESSHANDLE ProcessHandle,
    IN  PCSTR           pRelativePath,
    OUT PSTR*           ppUri
);



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

    ProcessHandle
                The process handle for which to get the 
                Process File Uri. 

    pRelativePath
                The path relative to process's working directory
                that will be appended to the working directory.
                NOTE: 
                If relative path is NULL. or '.' or '/', then the working directory 
                path is returned.
                
    ppProcessRootDirUri
                The Processes Root directory Uri.
                Use the XcFreeMemory() API to free this buffer.
  
    Return Value:
    
    DrError_OK  indicates call succeeded

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetProcessUri(
    IN  XCPROCESSHANDLE ProcessHandle,
    IN  PCSTR           pRelativePath,
    OUT PSTR*           ppProcessRootDirUri
);



/*++

XcTranslateLocalPathToProcessUri API

Description:

Translates the local path to a process Uri. This translation is necessary
in various scenarios. The local process will interact with files using the 
standard file system API's. Once it is done it will convert the local 
file names to process Uri's to be passed to other XCompute processes, which
will use the XCompute file SDK API's to access those files across machines.


Arguments:

    ProcessHandle
                The process handle. At present it has to be the current process handle.
                In future we will allow this handle to be for any process, that belongs 
                to the same job and is running on the same Process Node.

    pLocalPath
                The local path to be translated to the process Uri format
    
    
    ppTranslatedUri
                The Translated Uri of the given local path. Use the XcFreeMemory()
                API to free this buffer
  
    Return Value:
    
    DrError_OK  
    indicates call succeeded

    DrError_UnknownProcess 
    if not under PN.

    HRESULT_FROM_WIN32(ERROR_INVALID_PATH) 
    if path is not in a process root directory on the current PN.


--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcTranslateLocalPathToProcessUri(    
    IN  XCPROCESSHANDLE ProcessHandle,
    IN  PCSTR           pLocalPath,
    OUT PSTR*           ppTranslatedUri
);



/*++

XcTranslateProcessUriToLocalPath API

Description:

Tanslates the File Uri to local path. This translation is necessary
in various scenarios. The local process will interact with files using the 
standard file system API's. The local process will use the XCompute API's
to get the process Uri's and then would want to convert them to local paths so 
as to be able to use the standard File System API's to interact with files locally.

Arguments:

    ProcessHandle
                The process handle.

    pUri
                The Uri to translate to local path

    
    
    ppLocalFilePath
                The Translated local path for the above Uri.
                Use the XcFreeMemory() API to free this buffer
  
    Return Value:
    
    DrError_OK  
    indicates call succeeded

    DrError_UnknownProcess 
    if not under a PN

    DrError_InvalidPathname 
    if the provided URI is not withing a process root directory on the current PN.

--*/

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcTranslateProcessUriToLocalPath(
    IN  XCPROCESSHANDLE ProcessHandle,
    IN  PCSTR           pUri,
    OUT PSTR*           ppLocalFilePath
);



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

    SessionHandle        
                    Handle to an XCompute session associated with 
                    this call.

    pFileUri
                the fully qualified file Uri (UTF-8) obtained by calling the 
                XcGetProcessFileUri API.

    Flags     
                Reserved. Must be 0.

    pFileHandle
                The returned handle to the opened file. 
                Set to NULL if error

    pAsyncInfo  
                The async info structure. Its an alias to the 
                DR_ASYNC_INFO defined in Dryad.h. If this 
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
    DrError_OK  indicates call succeeded

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
    IN  XDRESSIONHANDLE         SessionHandle,
    IN  PCSTR                   pFileUri,
    IN  DWORD                   Flags,    
    OUT PXCPROCESSFILEHANDLE    pFileHandle,
    IN  PCXC_ASYNC_INFO         pAsyncInfo
);



/*++

XcGetProcessFileSize API

Description:

Gets the fileSize of the given process file handle.

Arguments:

    FileHandle
                The handle to the opened file.   

    Flags
                The options for fetching the size. These option flags are mutually exclusive
                One of the following is permissible:
                XC_REFRESH_AGGRESSIVE (default)
                    - visit server to find out latest known length
                XC_REFRESH_PASSIVE
                    - return length from local cache if available otherwise
                       visit server to find out latest known length
                XC_REFRESH_FROM_CACHE
                    - return length from local cache. 
                       Fail if not available. This is a non blocking call.
                       
    pSize
                Pointer to the output size variable.
                Must not be NULL.
                The memory pointed to by this variable must remain valid and writable
                for the duration of the asynchronous operation.

    pAsyncInfo  
                The async info structure. Its an alias to the 
                DR_ASYNC_INFO defined in Dryad.h. If this 
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

    E_NOTIMPL is returned if the underlyning file does not support GetFileSize.

    if pAsyncInfo is NULL
    DrError_OK  indicates call succeeded

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
XcGetProcessFileSize(    
    IN  XCPROCESSFILEHANDLE FileHandle,
    IN  UINT                Flags,
    OUT PUINT64             pSize,
    IN  PCXC_ASYNC_INFO     pAsyncInfo
);



/*++

XcCloseProcessFile API

Description:

Closes the file opened by the XcOpenProcessFile

Arguments:

    FileHandle
                The handle to the opened file. 
  
    Return Value:
    
    DrError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCloseProcessFile(    
    IN  XCPROCESSFILEHANDLE FileHandle
);



/*++

XcReadProcessFile API

Description:

Reads the content of the file opened by the XcOpenProcessFile

Arguments:
    

    FileHandle
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
                DR_ASYNC_INFO defined in Dryad.h. If this 
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
    DrError_OK  indicates call succeeded

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
    IN      XCPROCESSFILEHANDLE      FileHandle,
    OUT     PVOID                    pBuffer,
    IN OUT  PSIZE_T                  pBytesRead,
    IN OUT  XCPROCESSFILEPOSITION*   pReadPosition,
    IN      PCXC_ASYNC_INFO          pAsyncInfo    
);



/*++

XcGetCurrentProcessNodeId API

Description:

Gets the current Process Node Id. The Process Node Id to 
the node name map is maintained internally.

Arguments:
    
    SessionHandle        
                    Handle to an XCompute session associated with 
                    this call.

    pProcessNodeId   
                    Pointer to Pointer of the Id of the node
    
    Return Value:
    
    DrError_OK  
                    indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcGetCurrentProcessNodeId(   
    IN  XDRESSIONHANDLE     SessionHandle,     
    OUT PXCPROCESSNODEID    pProcessNodeId   
);



/*++

XcProcessNodeIdFromName API

Description:

Gets the Process Node Id for a node given the node name. The 
Process Node Id to the node name map is maintained internally.
If a node name is not found in the internal map, then a new
entry is created and the corrosponding id is returned back

Arguments:
    
    SessionHandle    
                Handle to an XCompute session associated with this 
                call. Reserved for future use. Must be NULL.


    pProcessNodeName 
                Name of the process node for which Id is needed

    pProcessNodeId   
                Pointer to Pointer of the Id of the node
    
    Return Value:
    
    DrError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcProcessNodeIdFromName(    
    IN  XDRESSIONHANDLE     SessionHandle,     
    IN  PCSTR               pProcessNodeName,
    OUT PXCPROCESSNODEID    pProcessNodeId
);



/*++

XcProcessNodeNameFromId API

Description:

Gets the Process Node name from the given Process Node Id.The 
Process Node Id to the node name map is maintained internally.

Arguments:    

    SessionHandle        
                    Handle to an XCompute session associated with this 
                    call. 


    ProcessNodeId   
                    The process Node Id for which the node name 
                    is needed

    ppProcessNodeName
                    Name of the process node corrosponding to Id 
                    Note: the returned process node name string 
                    is permanently allocated and will remain 
                    valid for the life of the process. There
                    is no need to make a copy of this string.

    Return Value:
    
    DrError_OK  indicates call succeeded

--*/
XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcProcessNodeNameFromId(        
    IN  XDRESSIONHANDLE     SessionHandle,     
    IN  XCPROCESSNODEID     ProcessNodeId,
    OUT PCSTR*              ppProcessNodeName
);



#pragma pack( pop )

#pragma warning( pop )

#if defined(__cplusplus)
}
#endif
