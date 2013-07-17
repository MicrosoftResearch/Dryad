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



#if !defined(_PCVOID_DEFINED)
typedef const void* PCVOID;
#define _PCVOID_DEFINED
#endif

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(XCOMPUTE_EXPORTS)
#define XCOMPUTEAPI_EXT __declspec(dllexport)
#else
#define XCOMPUTEAPI_EXT __declspec(dllimport)
#endif
#define XCOMPUTEAPI __stdcall


/*++

Error codes and exit code typedefs

--*/
typedef DWORD           XCEXITCODE;
typedef HRESULT         XCERROR;



/*

Process state related typedefs. Used by the sync 
API, to depict process state 

*/
typedef DWORD           XCPROCESSSTATE;
typedef XCPROCESSSTATE* PXCPROCESSSTATE;



/*

Various process states.

State transictions are shown below
Each state is explained in detail in the section below this

    XCPROCESSSTATE_INVALID -------------> XCPROCESSSTATE_COMPLETED        
                         |                                                         
                         |                                                         
                         \/                          
    XCPROCESSSTATE_UNSCHEDULED -------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                          

    XCPROCESSSTATE_SCHEDULING-------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                          

    XCPROCESSSTATE_SCHEDULED-------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                 

    XCPROCESSSTATE_ASSIGNEDTONODE-------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                 

    XCPROCESSSTATE_BINDING-------------> XCPROCESSSTATE_COMPLETED        
                        /\
                        |
                        |
                        \/                 

    XCPROCESSSTATE_BINDCOMPLETED-------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                 

    XCPROCESSSTATE_LAUNCHING-------------> XCPROCESSSTATE_COMPLETED        
                        |
                        |
                        \/                 

    XCPROCESSSTATE_RUNNING-------------> XCPROCESSSTATE_COMPLETED                  
                        |
                        |
                        \/                 
    XCPROCESSSTATE_TERMINATING                        
                        |
                        |
                        \/                 
    XCPROCESSSTATE_COMPLETED        
    
                        |
                        |
                        \/                 
    XCPROCESSSTATE_STATEDELETED
                        |
                        |
                        \/                 
    XCPROCESSSTATE_DELETED 

                                    


XCPROCESSSTATE_INVALID
        The process state is invalid. This will be returned, if a call to 
        the XcGetProcessState() api is made, even before 
        XcScheduleProcess() has been called.

XCPROCESSSTATE_UNSCHEDULED 
        The process has NOT been scheduled on the Process Scheduler.
        It is possible to XcCancelScheduleProcess() in this state

XCPROCESSSTATE_SCHEDULING
        The scheduling is in flight to the Process Scheduler. But it is
        not yet assigned to a process node.
        It is possible to XcCancelScheduleProcess() in this state

XCPROCESSSTATE_SCHEDULED 
        The process has been scheduled on the Process Scheduler.
        It is possible to XcCancelScheduleProcess() in this state

XCPROCESSSTATE_ASSIGNEDTONODE
        The process has been assinged to a process Node. The user can 
        use XcGetProcessNode() api to get PN related info for the process
        After this point it is possible to interact with process node state for 
        the process (update process constraints, request resource bindings, 
        launch the process, get and set properties, open and read process 
        files, etc.)

XCPROCESSSTATE_BINDING
        The process is assigned to a PN and resource binding (copying) is
        in progress. All required resources need to be copied before a 
        process can be launched. While in this state, it is possible to interact 
        with process node state for the process (update process constraints, 
        request additional resource bindings, get and set properties, open 
        and read process files, etc.)
        
XCPROCESSSTATE_BINDCOMPLETED
        The resources copying is compelted. NOTE: This might signal 
        completion of binding for a batch of resources. The state can
        jump back to XCPROCESSSTATE_BINDING. if further bindings are 
        requested at this point. If the process was originally scheduled 
        without the XC_CREATEPROCESS_DESCRIPTOR_LATEBOUNDRESOURCES 
        option set, or if process flag 
        XCPROCESS_FLAG_LAUNCH_AFTER_RESOURCE_BIND (TBD) has been set 
        on the process, then the state will automatically proceed to 
        XCPROCESSSTATE_LAUNCHING.       
        
XCPROCESSSTATE_LAUNCHING
        All resources binding finished and the process is being launched.

XCPROCESSSTATE_RUNNING
        The corresponding win32 process has been created on the PN 
        node and is currently running

XCPROCESSSTATE_TERMINATING
        The jobobject for the corresponding win32 process is being terminated/
        GC'ed. 

XCPROCESSSTATE_COMPLETED
        The XComputeProcess completed.
        If the process successfully reached XCPROCESSSTATE_ASSIGNEDTONODE 
        before completing, then it is still possible to interact with process node state 
        for the process (e.g., you can still open and read process files).

        NOTE: The process can complete for various reasons. The error
                  code associated with the state explains the exact reason.                              

XCPROCESSSTATE_STATEDELETED
        The XCompute process is completed. Its state like directories etc have 
        been garbage collected. But the ProcessNode still hold information about
        the XCompute process statistics. 

XCPROCESSSTATE_DELETEDFROMNODE
        The XComputeProcess has been garbage collected and no info
        about it exists at the process node. Only locally cached status 
        information is available.

*/
#define XCPROCESSSTATE_ZERO                         ((XCPROCESSSTATE)0x00000000)
#define XCPROCESSSTATE_INVALID                      XCPROCESSSTATE_ZERO

// The folowing states are managed on the Process Scheduler before the process has been allocated on the Process Node
#define XCPROCESSSTATE_UNSCHEDULED                  ((XCPROCESSSTATE)0x40000000)
#define XCPROCESSSTATE_SCHEDULING                   ((XCPROCESSSTATE)0x40100000)
#define XCPROCESSSTATE_SCHEDULED                    ((XCPROCESSSTATE)0x40200000)
#define XCPROCESSSTATE_SCHEDULINGFAILED             ((XCPROCESSSTATE)0x40300000)

// The following states are managed on the Process Node
#define XCPROCESSSTATE_NODE_UNINITIALIZED           ((XCPROCESSSTATE)0x80000000)
#define XCPROCESSSTATE_NODE_PREINITIALIZE           ((XCPROCESSSTATE)0x80100000)
#define XCPROCESSSTATE_NODE_ALLOCATED               ((XCPROCESSSTATE)0x80200000)
#define XCPROCESSSTATE_ASSIGNEDTONODE               XCPROCESSSTATE_NODE_ALLOCATED
#define XCPROCESSSTATE_NODE_READYTOBINDRESOURCES    ((XCPROCESSSTATE)0x80300000)
#define XCPROCESSSTATE_NODE_BINDINGRESOURCES        ((XCPROCESSSTATE)0x80400000)
#define XCPROCESSSTATE_BINDING                      XCPROCESSSTATE_NODE_BINDINGRESOURCES
#define XCPROCESSSTATE_NODE_RESOURCEBINDINGCOMPLETE ((XCPROCESSSTATE)0x80500000)
#define XCPROCESSSTATE_BINDCOMPLETED                XCPROCESSSTATE_NODE_RESOURCEBINDINGCOMPLETE
#define XCPROCESSSTATE_NODE_LOADPENDING             ((XCPROCESSSTATE)0x80600000)
#define XCPROCESSSTATE_LAUNCHING                    XCPROCESSSTATE_NODE_LOADPENDING
#define XCPROCESSSTATE_NODE_LOADING                 ((XCPROCESSSTATE)0x80700000)
#define XCPROCESSSTATE_NODE_LOADED                  ((XCPROCESSSTATE)0x80800000)
#define XCPROCESSSTATE_NODE_APPINITIALIZATION       ((XCPROCESSSTATE)0x80900000)
#define XCPROCESSSTATE_NODE_APPRUNPENDING           ((XCPROCESSSTATE)0x80A00000)
#define XCPROCESSSTATE_NODE_APPRUNNING              ((XCPROCESSSTATE)0x80B00000)
#define XCPROCESSSTATE_RUNNING                      XCPROCESSSTATE_NODE_APPRUNNING
#define XCPROCESSSTATE_NODE_TERMINATING             ((XCPROCESSSTATE)0x80c00000)
#define XCPROCESSSTATE_TERMINATING                  XCPROCESSSTATE_NODE_TERMINATING
#define XCPROCESSSTATE_NODE_COMPLETE                ((XCPROCESSSTATE)0x80d00000)
#define XCPROCESSSTATE_COMPLETED                    XCPROCESSSTATE_NODE_COMPLETE
#define XCPROCESSSTATE_NODE_DELETINGSTATE           ((XCPROCESSSTATE)0x80e00000)
#define XCPROCESSSTATE_NODE_STATEDELETED            ((XCPROCESSSTATE)0x80f00000)
#define XCPROCESSSTATE_STATEDELETED                 XCPROCESSSTATE_NODE_STATEDELETED
#define XCPROCESSSTATE_NODE_ZOMBIE                  ((XCPROCESSSTATE)0x8fffffff)

// The following states are maintained by the client SDK after the Process Node has forgotten about the process

#define XCPROCESSSTATE_DELETEDFROMNODE              ((XCPROCESSSTATE)0xc0000000)

// End Process States

#define XCPROCESSSTATE_NEVER                        ((XCPROCESSSTATE)0xffffffff)




/*++

NOTE: All byte strings in this .h file are UTF8 strings
unless otherwise noted.

--*/



/*++

XCPROCESSHANDLE
Handle to a XCompute Process. Used in the API's that require a 
process identifier as its input.
Some e.g. API's are: XcScheduleProcess(), XcSetAndGetProcessInfo().

This is used to assist API users in following ways:
    a.  Pass this identifier instead of generating their own unique 
        process GUIDs

    b.  If using Async API, then there is no need to keep track of /
        or lookup GUID to call back function/object map. The user
        context associated with the handle can be used instead.

    c.  All operations related to a process, e.g. Process states, etc
        will ultimately get tied to the handle, thus making it easy
        for users to collect/modify process related information 

--*/
typedef struct tagXCPROCESSHANDLE
{
    ULONG_PTR       Unused;
} *XCPROCESSHANDLE, **PXCPROCESSHANDLE;

const XCPROCESSHANDLE INVALID_XCPROCESSHANDLE = NULL;



/*++

XCPROCESSNODEID
Identifies a XCompute Node. Used in the ProcessNodeIdFromName()
and the ProcessNodeNameFromId() API's. All XCompute API's take
the XCPROCESSNODEID as input where ever a node related entry
is needed. This is used to assist users to be able to pass
this identifier instead of passing strings around and having to 
go through allocating/deallocating/Copying/Comparing them. 

Fields:
--*/
typedef struct tagXCPROCESSNODEID
{
    ULONG_PTR       Unused;
} *XCPROCESSNODEID, **PXCPROCESSNODEID;

const XCPROCESSNODEID INVALID_XCPROCESSNODEID = NULL;


/*++

XC_RESOURCE_SOURCE_TYPE enum

Determines the type of resource passed in the XC_RESOURCEFILE_DESCRIPTOR

Fields:

    XC_RESOURCE_SOURCE_UTF8_PATHNAME        
            Indicates XC_RESOURCEFILE_DESCRIPTOR's pResourceSource 
            points to a UTF-8share path. 
            The pathname may be an xstream URI or a path to a 
            working file in another XCompute Process in the same Job.
            The resource will be copied as a binary image from this
            location

    XC_RESOURCE_SOURCE_EMBEDDED_CONTENT 
            Indicates XC_RESOURCEFILE_DESCRIPTOR's pResourceSource
            points to a embedded resource. The contents of the buffer
            pointed to by pResource are written to the resource file 
            as a binary image.


--*/
typedef enum tagXC_RESOURCE_SOURCE_TYPE {
    XC_RESOURCE_SOURCE_UTF8_PATHNAME = 0,
    XC_RESOURCE_SOURCE_EMBEDDED_CONTENT
} XC_RESOURCE_SOURCE_TYPE;




/*++

XC_RESOURCEFILE_DESCRIPTOR structure

Holds information about a single resource file. 

Fields:

    Size              
                    sizeof(XC_RESOURCEFILE_DESCRIPTOR)

    Flags     
                    Reserved for future use

    pFileName         
                    Name of the file on the destination.
                    The name is a relative path to the
                    target processl's working directory

    ResourceSourceType  
                    See XC_RESOURCE_SOURCE_TYPE above

    NumberOfResourceSourceBytes 
                    Length of the pResourceSource buffer. 

    pResourceSource     
                    Depending on resourceSourceType, either
                    points to an embedded resource or to a share
                    path from where the resource will be copied

--*/
typedef struct tagXC_RESOURCEFILE_DESCRIPTOR{
    SIZE_T                  Size;
    DWORD                   Flags;
    PCSTR                   pFileName; 
    XC_RESOURCE_SOURCE_TYPE ResourceSourceType;
    SIZE_T                  NumberOfResourceSourceBytes;
    PVOID                   pResourceSource;
} XC_RESOURCEFILE_DESCRIPTOR, *PXC_RESOURCEFILE_DESCRIPTOR;
typedef const XC_RESOURCEFILE_DESCRIPTOR* PCXC_RESOURCEFILE_DESCRIPTOR;



/*++

This bit flag indicates that the resources for a process will
be late bound. This is used in the Flags parameter in the 
XC_CREATEPROCESS_DESCRIPTOR. See below

--*/
#define XCCREATEPROCESSDESCRIPTOR_LATEBOUNDRESOURCES  0x00000002



/*++

Typedefs/various aliases
--*/
typedef GUID                 XC_PROCESSID;
typedef const XC_PROCESSID*  PCXC_PROCESSID;
typedef GUID                 XC_TASKID_USER;
typedef GUID                 XC_TASKID;
typedef GUID                 XC_JOBID;
typedef const XC_JOBID*      PCXC_JOBID;

/*++

XC_ASYNC_INFO structure

Each function that may be executed asynchronously takes pointer
to XC_ASYNC_INFO structure as last parameter.

If NULL is passed then function completes in synchronous manner
and error code is returned as return value.

If parameter is not NULL then operation is carried on in asynchronous manner.
If asynchronous operation has been successfully started then function terminates
immediately with HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
Any other return value indicates that it was impossible to start asynchronous operation.

Fields:

    Size          Size of structure in bytes. Set to sizeof(XC_ASYNC_INFO).

    pOperationState Pointer to error code returned by completed operation.
                    While operation is in progress value is set to HRESULT_FROM_WIN32(ERROR_IO_PENDING).
                    Before completion is reported value is set to an error code of completed operation.
                    Cannot be NULL.

    Event           Handle to event. Event is set once operation is completed. May be NULL.

    IOCP            Handle to IO completion port. If not NULL then upon completion status is posted
                    to specified completion port.

    pOverlapped     Pointer to OVERLAPPED structure. Used in conjunction with IOCP parameter
                    to post status to IOCP.
                    Should be null if IOCP is NULL, cannot be NULL if IOCP is not NULL.

    CompletionKey   Used in conjunction with IOCP parameter to post status to IOCP.
                    Should be 0 if IOCP is NULL.

    unusedX         Fields reserved for future use.

Note that XC_ASYNC_INFO structure is not required to be available for the duration of asynchronous call
(for example, this allows to allocate XC_ASYNC_INFO structure on stack).
In contrast variable specified by pOperationState pointer is required to be available
for the duration of the asynchronous call.

--*/
typedef struct tagXC_ASYNC_INFO {
    SIZE_T          Size;

    HRESULT*        pOperationState;

    HANDLE          Event;

    HANDLE          IOCP;
    LPOVERLAPPED    pOverlapped;
    UINT_PTR        CompletionKey;

    UINT64          unused0;
    UINT64          unused1;
} XC_ASYNC_INFO, *PXC_ASYNC_INFO;
typedef const XC_ASYNC_INFO* PCXC_ASYNC_INFO;


/*++

A session handle will contain user related information that 
can be used to determine what part of information the user
can access.

--*/
typedef struct tagXCSESSIONHANDLE
{
    ULONG_PTR       Unused;
} *XCSESSIONHANDLE, **PXCSESSIONHANDLE;
const XCSESSIONHANDLE INVALID_XCSESSIONHANDLE = NULL;



/*++

XC_OPEN_SESSION_PARAMS structure

Session related information

Fields:

    Size              
                    sizeof(XC_OPEN_SESSION_PARAMS)

    Flags     
                    Reserved for future use. Must be 0.

    pCluster        
                    Name of the cluster to connect to.
                    If this filed is NULL then default cluster 
                    will be used.                     

    ClientId 
                   The unique ID to use for syncing process
                   related information with the Process Scheduler.
                   If a NULL GUID, a default ID will be generated.

                   Use this field, when implementing failover.
                   For e.g if JobManager wants to provide failover,
                   it can use a well known clientId, between various
                   redundant JobManagers. When the failover happens,
                   the new Job Manager can use the same client Id to 
                   sync process states with the Process Scheduler.

--*/

typedef struct tagXC_OPEN_SESSION_PARAMS{
    SIZE_T  Size;
    DWORD   Flags;
    PCSTR   pCluster;
    GUID    ClientId;
}XC_OPEN_SESSION_PARAMS, *PXC_OPEN_SESSION_PARAMS;
typedef const XC_OPEN_SESSION_PARAMS* PCXC_OPEN_SESSION_PARAMS;



/*++
 
An XCDATETIME is defined as the number of 100-nanosecond intervals 
that have elapsed since 12:00 A.M. January 1, 1601 (UTC). It is 
the representation of choice whenever an absolute date/time must 
be used.

XCDATETIME is equivalent to a windows FILETIME value without local
time zone adjustment.

--*/
typedef UINT64  XCDATETIME;

#define XCDATETIME_NEVER    _UI64_MAX
#define XCDATETIME_LONGAGO  0



/*++

XCTIMEINTERVAL represents a measurement of elapsed time in 100ns
Intervals. It is a signed entity (elapsed time may be negative).
It is the natural type for the result of subtracting two XCDATETIME
Values.
--*/
typedef INT64   XCTIMEINTERVAL;



/*++

The below #defines help define commonly used time intervals

--*/
#define XCTIMEINTERVAL_INFINITE             0X7FFFFFFFFFFFFFFF // TODO: Use _I64_MAX after upgrading to VS2010
#define XCTIMEINTERVAL_NEGATIVEINFINITE     _I64_MIN
#define XCTIMEINTERVAL_ZERO                 0
#define XCTIMEINTERVAL_QUANTUM              1
#define XCTIMEINTERVAL_100NS                ( (XCTIMEINTERVAL) (XCTIMEINTERVAL_QUANTUM) )
#define XCTIMEINTERVAL_MICROSECOND          ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_100NS * 10 ) )
#define XCTIMEINTERVAL_MILLISECOND          ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_MICROSECOND * 1000 ) )
#define XCTIMEINTERVAL_SECOND               ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_MILLISECOND * 1000 ) )
#define XCTIMEINTERVAL_MINUTE               ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_SECOND * 60 ) )
#define XCTIMEINTERVAL_HOUR                 ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_MINUTE * 60 ) )
#define XCTIMEINTERVAL_DAY                  ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_HOUR * 24 ) )
#define XCTIMEINTERVAL_WEEK                 ( (XCTIMEINTERVAL) ( XCTIMEINTERVAL_DAY * 7 ) )



/*++

Flags for the various options set in the XC_PROCESS_CONSTRAINTS  structure

--*/
#define XCPROCESSCONSTRAINTOPTION_SETMAXREMAININGELAPSEDEXECUTIONTIME     0x1
#define XCPROCESSCONSTRAINTOPTION_SETMAXREMAININGRETAINAFTERTERMINATETIME 0x2
#define XCPROCESSCONSTRAINTOPTION_SETMAXPERWIN32PROCESSUSERMODETIME       0x4
#define XCPROCESSCONSTRAINTOPTION_SETMAXREMAININGUSERMODETIME             0x8
#define XCPROCESSCONSTRAINTOPTION_SETMAXWORKINGSETSIZE                    0x10
#define XCPROCESSCONSTRAINTOPTION_SETMAXNUMWIN32PROCESSES                 0x20
#define XCPROCESSCONSTRAINTOPTION_SETMAXPERWIN32PROCESSMEMORYSIZE         0x40
#define XCPROCESSCONSTRAINTOPTION_SETMAXMEMORYSIZE                        0x80



/*++

Default process priority

--*/
#define XCPROCESSPRIORITY_DEFAULT     0x80000000



/*++

XC_PROCESS_CONSTRAINTS structure

Constraints that will be applied to the process that gets started
on a given node

Fields:

    Size                      
                                sizeof(XC_PROCESS_CONSTRAINTS)

    ProcessConstraintOptions    
                                Bit flag indicating what options have
                                been set

    MaxRemainingElapsedExecutionTime  
                                Maximum amount of time process can 
                                continue to run without terminating.

    MaxRemainingRetainAfterTerminateTime  
                                Amount of time after process 
                                termination before the process 
                                persistent state is discarded

    MaxPerWin32ProcessUserModeTime  
                                Max amount of user-mode CPU time for
                                each Windows process associated with 
                                the XCompute process

    MaxRemainingUserModeTime    
                                Max amount of total user-mode CPU 
                                time for XCompute process

    MaxWorkingSetSize           
                                Maximum working set size for 
                                Windows processes

    MaxNumWin32Processes        
                                Maximum number of Windows processes 
                                that can be running

    MaxPerWin32ProcessMemorySize
                                Maximum amount of memory per win32 
                                process

    MaxMemorySize               
                                Max total memory for the XCompute 
                                process   

--*/
typedef struct tagXC_PROCESS_CONSTRAINTS{
    SIZE_T          Size;
    DWORD           ProcessConstraintOptions;
    XCTIMEINTERVAL  MaxRemainingElapsedExecutionTime; 
    XCTIMEINTERVAL  MaxRemainingRetainAfterTerminateTime;  
    XCTIMEINTERVAL  MaxPerWin32ProcessUserModeTime; 
    XCTIMEINTERVAL  MaxRemainingUserModeTime; 
    UINT64          MaxWorkingSetSize; 
    UINT32          MaxNumWin32Processes; 
    UINT64          MaxPerWin32ProcessMemorySize; 
    UINT64          MaxMemorySize;
} XC_PROCESS_CONSTRAINTS, *PXC_PROCESS_CONSTRAINTS;
typedef const XC_PROCESS_CONSTRAINTS* PCXC_PROCESS_CONSTRAINTS;



/*++

XC_CREATEPROCESS_DESCRIPTOR structure

Used in ScheduleProcess API. Has all the information needed to
launch a process on a particular node

Fields:
    
    Size                  
                        Sizeof(XC_CREATEPROCESS_DESCRIPTOR)

    Flags         
                        Option bit flags

                        XC_CREATEPROCESS_DESCRIPTOR_LATEBOUNDRESOURCES
                        indicates that the resources are late 
                        bound. When the process gets created
                        on a node, the process is set to 
                        UnInitialized state, and waits for the
                        parent process to contact the PN to bind
                        the resources  

    pCommandLine          
                        Command line that will launch the process     

    pProcessClass         
                        Process class name. User-defined.

    pProcessFriendlyName      
                        The process friendly name.User-defined

    pEnvironmentStrings   
                        The environment strings that will be
                        set before launching the process on a node
                        The environment strings are represented as
                        a series of null-terminated UTF8 strings 
                        with an extra NULL at the end
    
    pAppProcessConstraints         
                        See PCXC_PROCESS_CONSTRAINTS above

    NumberOfResourceFileDescriptors
                        The number of resource file descriptors in 
                        the pResourceFileDescriptors array

    pResourceFileDescriptors
                        Pointer to array of 
                        PCXC_RESOURCEFILE_DESCRIPTOR's. These 
                        resources will be copied to the process
                        working directory before launching the 
                        process using the commandline

--*/
typedef struct tagXC_CREATEPROCESS_DESCRIPTOR{
    SIZE_T                          Size;
    DWORD                           Flags;    
    PCSTR                           pCommandLine;
    PCSTR                           pProcessClass;
    PCSTR                           pProcessFriendlyName;
    PCSTR                           pEnvironmentStrings;
    PCXC_PROCESS_CONSTRAINTS        pAppProcessConstraints;
    SIZE_T                          NumberOfResourceFileDescriptors;
    PCXC_RESOURCEFILE_DESCRIPTOR    pResourceFileDescriptors;
} XC_CREATEPROCESS_DESCRIPTOR, *PXC_CREATEPROCESS_DESCRIPTOR;
typedef const XC_CREATEPROCESS_DESCRIPTOR* PCXC_CREATEPROCESS_DESCRIPTOR;



/*++

Defines the Network Locality Params used in the 
XcGetNetworkLocalityPathOfProcessNode() API.
These params are passed to the API, to identify, the 
Affinity level. The resulting NetworkLocalityParam returned
from the API, can then be passed cia the XC_AFFINITY struct
(defined below), to the Process Scheduler, to help the
Process Scheduler in making decisions about the choice of 
Process Node to pick to run a given XCompute process.

NOTE:   The special Network Locality Param ".." can be combined
            with other locality params to represent one level up from
            the current level.
            E.g.    XCLOCALITYPARAM_POD/.. (NOTE the forward slash)
--*/
#define  XCLOCALITYPARAM_ONELEVELUP    ".."
#define  XCLOCALITYPARAM_POD           "POD"
#define  XCLOCALITYPARAM_L2SWITCH      "L2"
#define  XCLOCALITYPARAM_L3SWITCH      "L3"
#define  XCLOCALITYPARAM_VLAN          "VLAN"
#define  XCLOCALITYPARAM_CLUSTER       "CLUSTER"
#define  XCLOCALITYPARAM_DATACENTER    "DC"



/*++

Defines the bit flag used in the XC_AFFINITY structure.
If XCAFFINITY_HARD of the Flags in the XC_AFFINITY structure is
set, then the affinity is considered to have hard affinity
to the NetworkNodePath/s. See below for details.

--*/
#define XCAFFINITY_HARD    0x01



/*++

XC_AFFINITY structure

Each Affinity is comprised of list of network locality paths, 
an associated weight and a flag for hard affinity. 
A network locality can refer to a data center, a top/middle 
level switch, POD, or a specific host machine.

Fields:

    Size              
                        Sizeof(XC_AFFINITY)

    Flags         
                        Bit flags. XC_AFFINITY_HARD indicates that
                        affinity is hard affinity. 

    Weight          
                        The Process Scheduler will give preference to 
                        the Affinity (list of Nodes) that have higher 
                        weight, while picking up the Node on which to 
                        run the XCompute Process.
                        The intended units for Weight are 
                        "estimated bytes of I/O"

    NumberOfNetworkLocalityPaths
                        Number of Nodes in 
                        pNetworkLocalityPaths array.

    pNetworkLocalityPaths   
                        Pointer to the network locality paths array.
                        A network locality path is represented as a
                        string and is an opaque format. The caller 
                        gets the locality path information by calling the
                        XcGetNetworkLocalityPath API

--*/
typedef struct tagXC_AFFINITY{
    SIZE_T  Size;
    DWORD   Flags;    
    UINT64  Weight;
    SIZE_T  NumberOfNetworkLocalityPaths;
    PCSTR*  pNetworkLocalityPaths;
} XC_AFFINITY, *PXC_AFFINITY;
typedef const XC_AFFINITY* PCXC_AFFINITY;



/*++

XC_LOCALITY_DESCRIPTOR structure

Locality is represented as a collection of Affinities. 

Fields:

    Size                
                        sizeof(XC_LOCALITY_DESCRIPTOR)

    Flags         
                        Reserved. Must be 0.

    NumberOfAffinities    
                        Number of XC_AFFINITY'es

    pAffinities           
                        Pointer to Array of Affinities

--*/
typedef struct tagXC_LOCALITY_DESCRIPTOR{
    SIZE_T          Size;
    DWORD           Flags;
    SIZE_T          NumberOfAffinities;
    PXC_AFFINITY    pAffinities;
} XC_LOCALITY_DESCRIPTOR, *PXC_LOCALITY_DESCRIPTOR;
typedef const XC_LOCALITY_DESCRIPTOR* PCXC_LOCALITY_DESCRIPTOR;



/*++

XC_SCHEDULEPROCESS_DESCRIPTOR

The descriptor that has all the information about the process
to be scheduled

Fields:

    Size           
                        sizeof(XC_SCHEDULEPROCESS_DESCRIPTOR)

    Flags         Reserved for later use. Must be 0.

    ProcessPriority     
                        The priority of the process. The priority
                        is process priority, within all the 
                        processes for a given job. This is 
                        different from job priority. 

    pLocalityDescriptor         
                        See XC_LOCALITY_DESCRIPTOR above

    pCreateProcessDescriptor    
                        See XC_CREATEPROCESS_DESCRIPTOR above    

--*/
typedef struct tagXC_SCHEDULEPROCESS_DESCRIPTOR{
    SIZE_T                          Size;
    DWORD                           Flags;
    UINT32                          ProcessPriority;
    PCXC_LOCALITY_DESCRIPTOR        pLocalityDescriptor;
    PCXC_CREATEPROCESS_DESCRIPTOR   pCreateProcessDescriptor;    
} XC_SCHEDULEPROCESS_DESCRIPTOR, *PXC_SCHEDULEPROCESS_DESCRIPTOR;

typedef 
const XC_SCHEDULEPROCESS_DESCRIPTOR* PCXC_SCHEDULEPROCESS_DESCRIPTOR;



/*++

XC_PROCESSPROPERTY_INFO

The structure is embedded in the XC_POCESS_INFO struct explained 
below. It has all the information related to a particular property

Fields:

    Size              
                                sizeof(XC_SCHEDULE_PROCESS_RESULTS)

    pPropertyLabel    
                                The property label

    propertyVersion     
                                The property version

    pPropertyString   
                                The property string value

    PropertyBlockSize   
                                Memory block size of property

    pPropertyBlock      
                                Pointer to memory block related to property

--*/
typedef struct tagXC_PROCESSPROPERTY_INFO{
    SIZE_T              Size;
    PSTR                pPropertyLabel;    
    UINT64              PropertyVersion;
#if __midl
    [string]
#endif
    PSTR                pPropertyString;
    UINT32              PropertyBlockSize;
    UINT32              bugbugPAD;
#if __midl
    [size_is(PropertyBlockSize)]
#endif
    char *              pPropertyBlock;
} XC_PROCESSPROPERTY_INFO, *PXC_PROCESSPROPERTY_INFO;
typedef const XC_PROCESSPROPERTY_INFO* PCXC_PROCESSPROPERTY_INFO;



/*++

XC_PROCESS_STATISTICS

Contains all the statistics related to a given process/job

Fields:

    Size              
                        sizeof(XC_PROCESS_STATISTICS)
    
    Flags         
                        Reserved for later use

    ProcessUserTime     
                        Total user time the whole process
                        consumed in 100 nanosec

    ProcessKernelTime   
                        Total kernel time the whole process
                        consumed in 100 nanosec

    PageFaults         
                        Total #page faults for the whole process

    TotalProcessesCreated  
                        Total #win32 processes the process ever 
                        created
    
    PeakVMUsage         
                        The peak Virtual memory usage
    
    PeakMemUsage        
                        The peak working set memory usage

    MemUsageSeconds     
                        Working set memory usage * time used

    TotalIo             
                        Total IO transferred    

--*/
typedef struct tagXC_PROCESS_STATISTICS{
    SIZE_T          Size;
    DWORD           Flags;
    XCTIMEINTERVAL  ProcessUserTime;
    XCTIMEINTERVAL  ProcessKernelTime;
    INT32           PageFaults;
    INT32           TotalProcessesCreated;
    UINT64          PeakVMUsage;    
    UINT64          PeakMemUsage;
    UINT64          MemUsageSeconds;
    UINT64          TotalIo;    
} XC_PROCESS_STATISTICS, *PXC_PROCESS_STATISTICS;
typedef const XC_PROCESS_STATISTICS* PCXC_PROCESS_STATISTICS;



/*++

Bit flag definitions for XC_PROCESSINFO structure that is used
in the GetProcessProperty API

--*/
#define    XCPROCESSINFOOPTION_STATICINFO                 (0x01)
#define    XCPROCESSINFOOPTION_TIMINGINFO                 (0x02)
#define    XCPROCESSINFOOPTION_EFFECTIVECONSTRAINTS       (0x04)
#define    XCPROCESSINFOOPTION_EXTENDEDPROCESSDESCRIPTOR  (0x08)
#define    XCPROCESSINFOOPTION_EXTENDEDJOBDESCRIPTOR      (0x10)
#define    XCPROCESSINFOOPTION_PROCESSSTAT                (0x20)
#define    XCPROCESSINFOOPTION_APPCONSTRAINTS             (0x40)
#define    XCPROCESSINFOOPTION_SYSTEMCONSTRAINTS          (0x80)

#define    XCPROCESSINFOOPTION_All                                \
                XCPROCESSINFOOPTION_STATICINFO |                  \
                XCPROCESSINFOOPTION_TIMINGINFO |                  \
                XCPROCESSINFOOPTION_EFFECTIVECONSTRAINTS          \
                XCPROCESSINFOOPTION_EXTENDEDPROCESSDESCRIPTOR |   \
                XCPROCESSINFOOPTION_EXTENDEDJOBDESCRIPTOR |       \
                XCPROCESSINFOOPTION_PROCESSSTAT |                 \
                XCPROCESSINFOOPTION_APPCONSTRAINTS |              \
                XCPROCESSINFOOPTION_SYSTEMCONSTRAINTS



/*++

XC_SETANDGETPROCESSINFO_REQINPUT

The structure is used to make the XcPnSetAndGetProcessInfo call.
It contains the various inputs to the API clubbed together.

Fields:

    Size              
                        sizeof(XC_SETANDGETPROCESSINFO_REQINPUT)

    pAppProcessConstraints 
                        The process constraints to be set for the 
                        process. The user will need to preserve this
                        structure till the async call is completed

    NumberOfProcessPropertiesToSet
                        The number of properties to set in the 
                        pPropertiesToSet array

    ppPropertiesToSet    Pointer to property info array. These are the
                        properties that will be set in this call.                        

    pBlockOnPropertyLabel
                        Name of the property on which to block.The 
                        request finishes, when either the process
                        terminates, or the property is changed or
                        after timeout amount of time.                        

    BlockOnPropertyversionLastSeen
                        The latest known version number of property 
                        on which to block

    MaxBlockTime        Time to wait for property to change or pricess
                        to terminste before returning with unchanged
                        property version. If 0, API returns 
                        immediately with current values.

    pPropertyFetchTemplate
                        The property fetch template. It support the 
                        * wild card. A set of properties, whose             
                        labels match the propertyFetchTemplate are
                        returned. If NULL, no properties are returned

    ProcessInfoFetchOptions
                        bit flag indicating the different 
                        processInfo fields to fetch.

--*/
typedef struct tagXC_SETANDGETPROCESSINFO_REQINPUT{
    DWORD                       Size;
    PXC_PROCESS_CONSTRAINTS     pAppProcessConstraints;
    UINT32                      NumberOfProcessPropertiesToSet;    
    PXC_PROCESSPROPERTY_INFO*   ppPropertiesToSet;
    PCSTR                       pBlockOnPropertyLabel;
    UINT64                      BlockOnPropertyversionLastSeen;
    XCTIMEINTERVAL              MaxBlockTime;
    PCSTR                       pPropertyFetchTemplate;
    DWORD                       ProcessInfoFetchOptions;
} XC_SETANDGETPROCESSINFO_REQINPUT, 
  *PXC_SETANDGETPROCESSINFO_REQINPUT;

typedef 
const XC_SETANDGETPROCESSINFO_REQINPUT* PCXC_SETANDGETPROCESSINFO_REQINPUT;



/*++

XC_PROCESS_INFO

The structure gets returned as a result of the XcPnGetProcessProperty
call. Use the XcFreeMemory API to release memory for this structure

Fields:

    Size       
                    sizeof(XC_SCHEDULE_PROCESS_RESULTS)

    Flags         
                    Bit flag that indicates which fields in the
                    data structure have valid information. The 
                    bit flags are defined above 

    ProcessState   
                    The current state of the process from the PN's point of view.
                    This field is always sent.

    ProcessStatus   
                    The process status. Indicates whether the 
                    process is running or exited, and the reason.
                    This field is always sent.
                    
    ExitCode        
                    The process exit code.
                    This field is always sent.

    Win32Pid        The Windows processId of the process
                    This field is always sent.

    NumberofProcessProperties
                    Number of XC_PROCESSPROPERTY_INFO's returned
    
    ppProperties     
                    Array of XC_PROCESSPROPERTY_INFO structs

    CurrentPnTime 
                    Always sent. This is the time on PN

    CreatedTime       
                    Time when Win32 CreateProcess was 
                    initiated (XCDATETIME_NEVER if not yet created)
                    Bit flag:XCPROCESSINFOOPTION_TIMINGINFO

    BeginExecutionTime
                    Time when Win32 process was first resumed 
                    (XCDATETIME_NEVER if not yet resumed)
                    Bit flag:XCPROCESSINFOOPTION_TIMINGINFO

    TerminatedTime
                    Time when Win32 Process terminated 
                    (XCDATETIME_NEVER if not yet terminated)
                    Bit flag:XCPROCESSINFOOPTION_TIMINGINFO

    LastPropertyUpdateTime
                    Most recent time when any property was set                     

    pEffectiveProcessConstraints
                    Effective constraints for the process (combined constraints
                    from application and system)
                    pointer to XC_PROCESS_CONSTRAINTS struct
                    Bit flag:XCPROCESSINFOOPTION_EFFECTIVECONSTRAINTS

    pAppProcessConstraints
                    Application constraints for the process
                    pointer to XC_PROCESS_CONSTRAINTS struct
                    Bit flag:XCPROCESSINFOOPTION_APPCONSTRAINTS

    pSystemProcessConstraints
                    System constraints for the process
                    pointer to XC_PROCESS_CONSTRAINTS struct
                    Bit flag:XCPROCESSINFOOPTION_SYSTEMCONSTRAINTS

    pCommandLine  
                    The command line for the process

    pProcessStatistics                        
                    Pointer to the XC_PROCESS_STATISTICS struct
                    Bit flag:XCPROCESSINFOOPTION_STAT
        
--*/
typedef struct tagXC_PROCESS_INFO{
    SIZE_T                      Size;
    DWORD                       Flags;
    XCPROCESSSTATE              ProcessState;
    XCERROR                     ProcessStatus;
    XCEXITCODE                  ExitCode;    
    UINT32                      Win32Pid;
    UINT32                      NumberofProcessProperties;    
    PXC_PROCESSPROPERTY_INFO    *ppProperties;
    XCDATETIME                  CurrentPnTime;
    XCDATETIME                  CreatedTime; 
    XCDATETIME                  BeginExecutionTime; 
    XCDATETIME                  TerminatedTime; 
    XCDATETIME                  LastPropertyUpdateTime; 
    PXC_PROCESS_CONSTRAINTS     pEffectiveProcessConstraints;
    PXC_PROCESS_CONSTRAINTS     pAppProcessConstraints;
    PXC_PROCESS_CONSTRAINTS     pSystemProcessConstraints;
    PSTR                        pCommandLine;
    PXC_PROCESS_STATISTICS      pProcessStatistics;
} XC_PROCESS_INFO, *PXC_PROCESS_INFO;
typedef const XC_PROCESS_INFO* PCXC_PROCESS_INFO;



/*++

XC_SETANDGETPROCESSINFO_REQRESULTS

The structure is gets returned as a result of call to the 
XcPnSetAndGetProcessInfo API.
It contains the results that match the ProcessInfoFetchOptions
and the PropertyFetchTemplate passed to the API via the 
XC_SETANDGETPROCESSINFO_REQINPUT struct

Fields:

    Size           
                        sizeof(XC_SETANDGETPROCESSINFO_REQINPUT)

    pProcessInfo        
                        The process info that has information about
                        all the properties for which information 
                        was asked to be retreived (using the 
                        PropertyFetchTemplate). It also has all
                        the information that was asked to be 
                        retreived using the ProcessInfoFetchOptions.

    NumberOfPropertyVersions
                        The number of property versions in the 
                        pPropertyVersions array

    pPropertyVersions 
                        Pointer to array of property versions. 
                        Note: The indexes of version numbers in the
                        pPropertyVersions array corrosponds 1:1 with the 
                        pPropertiesToSet array in the 
                        XC_SETANDGETPROCESSINFO_REQINPUT that gets 
                        passed to the XcPnSetAndGetProcessInfo() API.
--*/
typedef struct tagXC_SETANDGETPROCESSINFO_REQRESULTS{
    DWORD               Size;
    PXC_PROCESS_INFO    pProcessInfo;
    UINT32              NumberOfPropertyVersions;
#if __midl
    [size_is(NumberOfPropertyVersions)]
#endif
    UINT64*             pPropertyVersions;
} XC_SETANDGETPROCESSINFO_REQRESULTS, 
  *PXC_SETANDGETPROCESSINFO_REQRESULTS;

typedef 
const XC_SETANDGETPROCESSINFO_REQRESULTS* 
    PCXC_SETANDGETPROCESSINFO_REQRESULTS;



/*++

XCPROCESSFILEHANDLE
A handle to represent an open XCompute Process File.
This is used in the XCompute Process File API, which gives
the ability to read remote files written by a XComputeProcess
into its working directory

Fields:
--*/
typedef struct tagXCPROCESSFILEHANDLE
{
    ULONG_PTR       Unused;
} *XCPROCESSFILEHANDLE, **PXCPROCESSFILEHANDLE;

const XCPROCESSFILEHANDLE INVALID_XCPROCESSFILEHANDLE = NULL;



/* File offset value for XCompute files */
typedef UINT64 XCPROCESSFILEPOSITION, *PXCPROCESSFILEPOSITION;



/*

Various XcGetProcessFileSize options

    XCREFRESH_AGGRESSIVE (default)
        - visit server to find out latest known length
        
    XCREFRESH_PASSIVE
        - return length from local cache if available otherwise
           visit server to find out latest known length
           
    XCREFRESH_FROM_CACHE
        - return length from local cache 
           fail if not available

*/
#define XCREFRESH_AGGRESSIVE           0x10000000u
#define XCREFRESH_PASSIVE              0x20000000u
#define XCREFRESH_FROM_CACHE           0x30000000u



#pragma warning( pop )

#if defined(__cplusplus)
}
#endif
