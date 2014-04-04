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

#include <DrKernel.h>
#include <DrXComputeInternal.h>

DrXComputeResource::DrXComputeResource(DrResourceLevel level,
                                       DrString name, DrString locality,
                                       DrResourcePtr parent, XCPROCESSNODEID node)
: DrResource(level, name, locality, parent)
{
    m_node = node;
}

XCPROCESSNODEID DrXComputeResource::GetNode()
{
    return m_node;
}


static const UINT8* GuidWriteWord(char *pDst, const UINT8 *pSrc, int uBytes)
{
    static const char Hex[] = "0123456789ABCDEF";
    int c;

    pDst += uBytes * 2;
    *pDst = '-';
    do
    {
        c = *pSrc++;
        pDst -= 2;
        pDst[1] = Hex[c & 15];
        pDst[0] = Hex[c >> 4];
    }
    while (--uBytes);

    return pSrc;
}

static void GuidWrite(char *pDst, const GUID *pGuid)
{
    const UINT8 *p;

    p = (const UINT8 *) pGuid;
    
    p = GuidWriteWord(pDst, p, 4);
    pDst += 9;

    p = GuidWriteWord(pDst, p, 2);
    pDst += 5;

    p = GuidWriteWord(pDst, p, 2);
    pDst += 5;

    GuidWriteWord(pDst, p, 1);
    GuidWriteWord(pDst + 2, p + 1, 1);
    pDst += 5;
    p += 2;

    GuidWriteWord(pDst + 0*2, p + 0, 1);
    GuidWriteWord(pDst + 1*2, p + 1, 1);
    GuidWriteWord(pDst + 2*2, p + 2, 1);
    GuidWriteWord(pDst + 3*2, p + 3, 1);
    GuidWriteWord(pDst + 4*2, p + 4, 1);
    GuidWriteWord(pDst + 5*2, p + 5, 1);

    pDst += 6*2;
    *pDst = 0;
}

void DrXComputeProcessHandle::SetAssignedNode(DrResourcePtr node)
{
    m_node = node;
}

DrResourcePtr DrXComputeProcessHandle::GetAssignedNode()
{
    return m_node;
}

void DrXComputeProcessHandle::CloseHandle()
{
    if (m_handle != DrNull)
    {
		DrLogI("Calling close handle");
        XcCloseProcessHandle(m_handle);
		DrLogI("Closed handle");
        m_handle = DrNull;
    }
}

DrString DrXComputeProcessHandle::GetHandleIdAsString()
{
    DrString s;

    GUID id;
    XCERROR err = XcGetProcessId(m_handle, &id);

    if (SUCCEEDED(err))
    {
        char guidString[37];
        GuidWrite(guidString, &id);
        s.SetF("%s", guidString);
    }
    else
    {
        s.Set("not yet assigned");
    }

    return s;
}

static DrProcessState TranslateXcState(XCPROCESSSTATE xcState, HRESULT errorReason)
{
    if (xcState < XCPROCESSSTATE_ASSIGNEDTONODE)
    {
		if (xcState == XCPROCESSSTATE_SCHEDULINGFAILED)
		{
			DrAssert(!SUCCEEDED(errorReason));
			return DPS_Failed;
		}
		else
		{
			return DPS_Scheduling;
		}
    }
    else if (xcState < XCPROCESSSTATE_RUNNING)
    {
        return DPS_Starting;
    }
    else if (xcState < XCPROCESSSTATE_COMPLETED)
    {
        return DPS_Running;
    }
    else if (xcState == XCPROCESSSTATE_COMPLETED)
    {
        if (SUCCEEDED(errorReason))
        {
            return DPS_Completed;
        }
        else
        {
            return DPS_Failed;
        }
    }
    else /* xcState > XCPROCESSSTATE_COMPLETED */
    {
        return DPS_Zombie;
    }
}

DrProcessState DrXComputeProcessHandle::GetState(HRESULT& errorReason)
{
    XCPROCESSSTATE xcState;
    XCERROR schedulingError;
    XCERROR err = XcGetProcessState(m_handle, &xcState, &schedulingError);
    DrAssert(SUCCEEDED(err));

    if (xcState == XCPROCESSSTATE_COMPLETED || xcState == XCPROCESSSTATE_SCHEDULINGFAILED)
    {
        errorReason = schedulingError;
    }
    else
    {
        errorReason = S_OK;
    }

    return TranslateXcState(xcState, errorReason);
}

DrString DrXComputeProcessHandle::GetFileURIBase()
{
    char* path = NULL;

    XCERROR err = XcGetProcessUri(m_handle, "", &path);
    DrAssert(SUCCEEDED(err));

    DrString uriBase;
    uriBase.SetF("%s", path);

    XcFreeMemory(path);

    return uriBase;
}

DrXComputeOverlapped::DrXComputeOverlapped(DrXComputePtr parent, DrMessageBasePtr message)
{
    m_parent.Store(parent);
    if (message != DrNull)
    {
        m_message.Store(message);
    }
}


DrXComputeOverlapped::~DrXComputeOverlapped()
{
}

DrXComputeRef DrXComputeOverlapped::ExtractParent()
{
    return m_parent.Extract();
}

DrMessageBaseRef DrXComputeOverlapped::ExtractMessage()
{
    return m_message.Extract();
}

void DrXComputeOverlapped::Discard()
{
    DrXComputeRef x = m_parent.Extract();
    x = DrNull;
    DrMessageBaseRef m = m_message.Extract();
    m = DrNull;
}


DrXComputeWaitForStateChangeOverlapped::DrXComputeWaitForStateChangeOverlapped(DrXComputePtr parent,
                                                                               DrPSRMessagePtr message)
    : DrXComputeOverlapped(parent, message)
{
}

void DrXComputeWaitForStateChangeOverlapped::Process()
{
    HRESULT status = *GetOperationStatePtr();

    DrXComputeRef p = ExtractParent();
    DrXComputeInternalRef parent = dynamic_cast<DrXComputeInternalPtr>((DrXComputePtr) p);
    DrAssert(parent != DrNull);

    DrMessageBaseRef m = ExtractMessage();
    DrPSRMessageRef message = dynamic_cast<DrPSRMessagePtr>((DrMessageBasePtr) m);
    DrAssert(message != DrNull);

    parent->ProcessStateChange(status, message);
}


DrXComputeCancelScheduleProcessOverlapped::DrXComputeCancelScheduleProcessOverlapped(DrXComputePtr parent)
    : DrXComputeOverlapped(parent, DrNull)
{
}

/* we don't do anything with the result, since somebody else is already waiting for the state change */
void DrXComputeCancelScheduleProcessOverlapped::Process()
{
    HRESULT status = *GetOperationStatePtr();

    DrLogI("Cancel schedule process returned with status %s", DRERRORSTRING(status));
}


DrXComputeGetSetOverlapped::DrXComputeGetSetOverlapped(DrXComputePtr parent, DrMessageBasePtr message)
    : DrXComputeOverlapped(parent, message)
{
    m_results = NULL;
}

DrXComputeGetSetOverlapped::~DrXComputeGetSetOverlapped()
{
    if (m_results != NULL)
    {
        PXC_PROCESS_INFO pProcessInfo = m_results->pProcessInfo;
        if (pProcessInfo)
        {
            for (unsigned int i=0; i<pProcessInfo->NumberofProcessProperties; i++)
            {
                PXC_PROCESSPROPERTY_INFO pprop = pProcessInfo->ppProperties[i];
                XcFreeMemory(pprop->pPropertyLabel);
                XcFreeMemory(pprop->pPropertyString);
                XcFreeMemory(pprop->pPropertyBlock);                                
                XcFreeMemory(pprop);
            }
            XcFreeMemory(pProcessInfo->ppProperties);
            XcFreeMemory(pProcessInfo->pProcessStatistics);
        }
        
        XcFreeMemory(m_results->pProcessInfo);
        XcFreeMemory(m_results->pPropertyVersions);
        XcFreeMemory(m_results);
    }
}

PXC_SETANDGETPROCESSINFO_REQRESULTS* DrXComputeGetSetOverlapped::GetResultsPointer()
{
    return &m_results;
}

DrXComputeGetPropertyOverlapped::DrXComputeGetPropertyOverlapped(DrXComputePtr parent, DrString propertyName,
                                                                 DrPPSMessagePtr message)
    : DrXComputeGetSetOverlapped(parent, message)
{
    DrHeapStringRef hs = DrNew DrHeapString();
    hs->m_payload = propertyName;
    m_propertyName.Store(hs);
}

void DrXComputeGetPropertyOverlapped::Process()
{
    HRESULT status = *GetOperationStatePtr();

    DrXComputeRef p = ExtractParent();
    DrXComputeInternalRef parent = dynamic_cast<DrXComputeInternalPtr>((DrXComputePtr) p);
    DrAssert(parent != DrNull);

    DrMessageBaseRef m = ExtractMessage();
    DrPPSMessageRef message = dynamic_cast<DrPPSMessagePtr>((DrMessageBasePtr) m);
    DrAssert(message != DrNull);

    DrHeapStringRef hs = m_propertyName.Extract();
    DrString propertyName = hs->m_payload;

    parent->ProcessPropertyFetch(status, propertyName, m_results, message);
}

DrXComputeSetCommandOverlapped::DrXComputeSetCommandOverlapped(DrXComputePtr parent, DrErrorMessagePtr message)
    : DrXComputeGetSetOverlapped(parent, message)
{
}

void DrXComputeSetCommandOverlapped::Process()
{
    HRESULT status = *GetOperationStatePtr();

    DrXComputeRef p = ExtractParent();
    DrXComputeInternalRef parent = dynamic_cast<DrXComputeInternalPtr>((DrXComputePtr) p);
    DrAssert(parent != DrNull);

    DrMessageBaseRef m = ExtractMessage();
    DrErrorMessageRef message = dynamic_cast<DrErrorMessagePtr>((DrMessageBasePtr) m);
    DrAssert(message != DrNull);

    parent->ProcessCommandResult(status, message);
}

// remove these in favor of DrScheduler.cpp
#if 0
DrXCompute::~DrXCompute()
{
}

DrXComputeRef DrXCompute::Create()
{
    //return DrNew DrXComputeYarn();
    return DrNew DrXComputeInternal();
}
#endif

DrXComputeInternal::DrXComputeInternal()
{
    m_session = INVALID_XCSESSIONHANDLE;
    m_localProcess = INVALID_XCPROCESSHANDLE;
}

DrXComputeInternal::~DrXComputeInternal()
{
    Shutdown();
}

DrUniversePtr DrXComputeInternal::GetUniverse()
{
    return m_universe;
}

DrMessagePumpPtr DrXComputeInternal::GetMessagePump()
{
    return m_messagePump;
}

DrDateTime DrXComputeInternal::GetCurrentTimeStamp()
{
    return m_messagePump->GetCurrentTimeStamp();
}


HRESULT DrXComputeInternal::Initialize(DrUniversePtr universe, DrMessagePumpPtr pump)
{
    m_universe = universe;
    m_messagePump = pump;

    DrLogI("Initializing XCompute");

    XCERROR err;

    err = XcInitialize(NULL, "Dryad");
    if (!SUCCEEDED(err))
    {
        DrLogE("Failed to initialize XCompute", "error: %s", DRERRORSTRING(err));
        return err;
    }

    {
        DRPIN(XCSESSIONHANDLE) sessionPtr = &m_session;
        err = XcOpenSession(NULL,  sessionPtr, NULL);
    }
    if (!SUCCEEDED(err))
    {
        DrLogE("Failed to open XCompute session", "error: %s", DRERRORSTRING(err));
        return err;
    }

    {
        DRPIN(XCPROCESSHANDLE) processPtr = &m_localProcess;
        err = XcOpenCurrentProcessHandle(m_session, processPtr);
    }
    if (!SUCCEEDED(err))
    {
        DrLogE("Failed to open local process handle", "error: %s", DRERRORSTRING(err));
        return err;
    }

    return FetchListOfComputers();
}

void DrXComputeInternal::Shutdown()
{
    if (m_session!= INVALID_XCSESSIONHANDLE)
    {
        XcCloseSession(m_session);
        m_session = INVALID_XCSESSIONHANDLE;
    }

    if (m_localProcess != INVALID_XCPROCESSHANDLE)
    {
        XcCloseProcessHandle(m_localProcess);
        m_localProcess = INVALID_XCPROCESSHANDLE;
    }
}

void DrXComputeInternal::AddNodeToUniverse(XCPROCESSNODEID nodeId)
{
    PCSTR nodeName;
    XCERROR err = XcProcessNodeNameFromId(m_session,
                                          nodeId,
                                          &nodeName);
    DrAssert(SUCCEEDED(err));

    DrString name;
    name.SetF("%s", nodeName);
    XcFreeMemory(nodeName);

    PCSTR nodeLS;
    err = XcGetNetworkLocalityPathOfProcessNode(m_session,
                                                nodeId,
                                                NULL,
                                                &nodeLS);
    DrAssert(SUCCEEDED(err));

    DrString nodeLocality;
    nodeLocality.SetF("%s", nodeLS);
    XcFreeMemory(nodeLS);

    PCSTR podLS;
    err = XcGetNetworkLocalityPathOfProcessNode(m_session,
                                                nodeId,
                                                XCLOCALITYPARAM_POD,
                                                &podLS);
    DrAssert(SUCCEEDED(err));

    DrString podName;
    podName.SetF("POD-%s", podLS);
    DrString podLocality;
    podLocality.SetF("%s", podLS);
    XcFreeMemory(podLS);

    DrResourceRef pod = m_universe->LookUpResourceInternal(podName);
    if (pod == DrNull)
    {
        pod = DrNew DrXComputeResource(DRL_Rack, podName, podLocality, DrNull, INVALID_XCPROCESSNODEID);
        m_universe->AddResource(pod);
        DrLogI("Found pod %s", podName.GetChars());
    }

    DrResourceRef node = m_universe->LookUpResourceInternal(name);
    DrAssert(node == DrNull);
    node = DrNew DrXComputeResource(DRL_Computer, name, nodeLocality, pod, nodeId);

    m_universe->AddResource(node);
    DrLogI("Found computer %s in pod %s", name.GetChars(), podName.GetChars());
}

HRESULT DrXComputeInternal::FetchListOfComputers()
{
    UINT32 numberOfNodes;
    PXCPROCESSNODEID nodeArray = NULL;

    XCERROR err = XcEnumerateProcessNodes(m_session,
                                          &numberOfNodes,
                                          &nodeArray,
                                          NULL);

    if (!SUCCEEDED(err))
    {
        DrLogE("Failed to enumerate process nodes error: %s", DRERRORSTRING(err));

        if (nodeArray != NULL)
        {
            XcFreeMemory(nodeArray);
        }

        return err;
    }

    if (numberOfNodes == 0)
    {
        DrLogI("No process nodes returned");

        if (nodeArray != NULL)
        {
            XcFreeMemory(nodeArray);
        }

        return HRESULT_FROM_WIN32(ERROR_INVALID_DATA);
    }

    DrLogI("Found %d process nodes", numberOfNodes);

    {
        DrAutoCriticalSection acs(m_universe->GetResourceLock());

        UINT32 i;
        for (i=0; i<numberOfNodes; ++i)
        {
            AddNodeToUniverse(nodeArray[i]);
        }
    }

    return S_OK;
}

void DrXComputeInternal::ScheduleProcess(DrAffinityListRef affinities,
                                         DrString name, DrString commandLine,
                                         DrProcessTemplatePtr processTemplate,
                                         DrPSRListenerPtr listener)
{
    DrLogI("Scheduling process with %d affinities", affinities->Size());

    PXC_AFFINITY affinityArray = new XC_AFFINITY[affinities->Size()];
    int i;
    for (i=0; i<affinities->Size(); ++i)
    {
        DrAffinityPtr a = affinities[i];

        PCSTR* pathArray = new PCSTR[a->GetLocalityArray()->Size()];
        int j;
        for (j=0; j<a->GetLocalityArray()->Size(); ++j)
        {
            pathArray[j] = a->GetLocalityArray()[j]->GetLocality().GetChars();
            DrLogI("Added affinity path %s", pathArray[j]);
        }

        XC_AFFINITY& affinity = affinityArray[i];
        memset(&affinity, 0, sizeof(affinity));
        affinity.Size = sizeof(affinity);
        affinity.NumberOfNetworkLocalityPaths = a->GetLocalityArray()->Size();
        affinity.pNetworkLocalityPaths = pathArray;
        affinity.Weight = a->GetWeight();

        DrLogI("Added affinity with weight %I64u", affinity.Weight);
    }

    XC_LOCALITY_DESCRIPTOR locality;
    memset(&locality, 0, sizeof(locality));
    locality.Size = sizeof(locality);
    locality.NumberOfAffinities = affinities->Size();
    locality.pAffinities = affinityArray;

    XC_CREATEPROCESS_DESCRIPTOR createProcess;
    memset(&createProcess, 0, sizeof(createProcess));
    createProcess.Size = sizeof(createProcess);
    createProcess.pCommandLine = commandLine.GetChars();
    createProcess.pProcessClass = processTemplate->GetProcessClass().GetChars();
    createProcess.pProcessFriendlyName = name.GetChars();
    createProcess.pAppProcessConstraints = NULL;
    createProcess.NumberOfResourceFileDescriptors = 0;
    createProcess.pResourceFileDescriptors = NULL;

    XC_SCHEDULEPROCESS_DESCRIPTOR scheduleDescriptor;
    memset(&scheduleDescriptor, 0, sizeof(scheduleDescriptor));
    scheduleDescriptor.Size = sizeof(scheduleDescriptor);
    scheduleDescriptor.pLocalityDescriptor = &locality;
    scheduleDescriptor.pCreateProcessDescriptor = &createProcess;

    DrLogI("Starting schedule process for %s.%s",
           processTemplate->GetProcessClass().GetChars(), name.GetChars());

    DrProcessState state;
    DrString reason;
    XCERROR err;

    DrXComputeProcessHandleRef process = DrNew DrXComputeProcessHandle();

    {
        DRPIN(XCPROCESSHANDLE) processHandlePtr = &process->m_handle;
        err = XcCreateNewProcessHandle(m_session, NULL, processHandlePtr);
    }

    if (SUCCEEDED(err))
    {
        err = XcScheduleProcess(process->m_handle, &scheduleDescriptor);
        if (SUCCEEDED(err))
        {
            DrLogI("Schedule process succeeded for %s.%s",
                   processTemplate->GetProcessClass().GetChars(), name.GetChars());
            state = DPS_Scheduling;
            reason = "Schedule process in progress";
        }
        else
        {
            DrLogW("Schedule process failed immediately for %s.%s error %s",
                   processTemplate->GetProcessClass().GetChars(), name.GetChars(), DRERRORSTRING(err));
            state = DPS_Zombie;
            reason.SetF("Schedule process failed immediately error %s", DRERRORSTRING(err));
        }
    }
    else
    {
        DrLogW("Create process failed for %s.%s error %s",
               processTemplate->GetProcessClass().GetChars(), name.GetChars(), DRERRORSTRING(err));
        state = DPS_Zombie;
        reason.SetF("Create process handle failed error %s", DRERRORSTRING(err));
    }

    for (i=0; i<affinities->Size(); ++i)
    {
        XC_AFFINITY& affinity = affinityArray[i];
        delete [] affinity.pNetworkLocalityPaths;
    }
    delete [] affinityArray;

    /* send the listener notification of the change of state */
    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_state = state;
    notification->m_exitCode = STILL_ACTIVE;
    notification->m_process = process;
    if (err != S_OK)
    {
        /* this can be true in principle if SUCCEEDED(err) but only S_OK corresponds to a null
           error object */
        notification->m_status = DrNew DrError(err, "XCompute", reason);
    }

    DrPSRMessageRef message = DrNew DrPSRMessage(listener, notification);
    m_messagePump->EnQueue(message);

    if (SUCCEEDED(err))
    {
        WaitForStateChange(process, XCPROCESSSTATE_ASSIGNEDTONODE, listener);
    }
}

void DrXComputeInternal::WaitUntilStart(DrProcessHandlePtr p, DrPSRListenerPtr listener)
{
    DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    WaitForStateChange(process, XCPROCESSSTATE_RUNNING, listener);
}

void DrXComputeInternal::WaitUntilCompleted(DrProcessHandlePtr p, DrPSRListenerPtr listener)
{
    DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    WaitForStateChange(process, XCPROCESSSTATE_COMPLETED, listener);
}

void DrXComputeInternal::WaitForStateChange(DrXComputeProcessHandlePtr process, XCPROCESSSTATE targetState,
                                            DrPSRListenerPtr listener)
{
    /* make a new message now to hold on to the reference to listener */
    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_exitCode = STILL_ACTIVE;
    notification->m_process = process;
    DrPSRMessageRef message = DrNew DrPSRMessage(listener, notification);

    DrXComputeOverlapped* overlapped = new DrXComputeWaitForStateChangeOverlapped(this, message);

    XC_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.Size = sizeof(asyncInfo);
    asyncInfo.pOperationState = overlapped->GetOperationStatePtr();
    asyncInfo.IOCP = m_messagePump->GetCompletionPort();
    asyncInfo.pOverlapped = overlapped;

    DrLogI("Waiting for state change to %x", targetState);

    m_messagePump->NotifySubmissionToCompletionPort(overlapped);

    XCERROR err = XcWaitForStateChange(process->m_handle, targetState,
                                       XCTIMEINTERVAL_INFINITE, &asyncInfo);

    if (err != HRESULT_FROM_WIN32(ERROR_IO_PENDING))
    {
        DrLogA("Wait for state change failed synchronously error %s", DRERRORSTRING(err));
    }
}

void DrXComputeInternal::ProcessStateChange(HRESULT status, DrPSRMessagePtr message)
{
    DrProcessStateRecordPtr notification = message->GetPayload();
    DrString reason;

    if (!SUCCEEDED(status))
    {
        /* we really don't want to add cascading retries all through the stack,
           so we are going to take the bold stance that if XCompute says there
           was an error here, it has diligently retried in as sensible a way as
           we would have, and really, things are not going to improve. So the
           process is now dead to us. */

        /* We use the Failed state here instead of the Zombie state because we
           are relying on XCompute returning success here, but setting the process
           state to some zombie value, if the process has been garbage collected */
        notification->m_state = DPS_Failed;
        reason.SetF("Wait for state change RPC failed with error %s", DRERRORSTRING(status));
        notification->m_status = DrNew DrError(status, "XCompute", reason);
        notification->m_exitCode = 1;
    }
    else
    {
        DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(notification->m_process);
        DrAssert(process != DrNull);
        HRESULT schedulingStatus;
        notification->m_state = process->GetState(schedulingStatus);

        DrLogI("Processing state change internal state %d status %s",
               notification->m_state, DRERRORSTRING(schedulingStatus));

        if (!SUCCEEDED(schedulingStatus))
        {
            reason.SetF("Process in failed state with error %s", DRERRORSTRING(schedulingStatus));
            notification->m_status = DrNew DrError(schedulingStatus, "XCompute", reason);
            notification->m_exitCode = 1;
        }
        else if (schedulingStatus != S_OK)
        {
            reason.SetF("Process in non-error state %s", DRERRORSTRING(schedulingStatus));
            notification->m_status = DrNew DrError(schedulingStatus, "XCompute", reason);
        }

        if (process->GetAssignedNode() == DrNull)
        {
            DrXComputeProcessHandlePtr xcProcess = dynamic_cast<DrXComputeProcessHandlePtr>(process);
            DrAssert(xcProcess != DrNull);

            DrLogI("Looking up assigned node");

            XCPROCESSNODEID nodeId;
            XCERROR err = XcGetProcessNodeId(xcProcess->m_handle, &nodeId);

            DrLogI("Found assigned node id err %s", DRERRORSTRING(err));

            if (SUCCEEDED(err))
            {
                DrAutoCriticalSection acs(m_universe->GetResourceLock());

                PCSTR nodeName;
                XCERROR err = XcProcessNodeNameFromId(m_session,
                                                      nodeId,
                                                      &nodeName);
                DrAssert(SUCCEEDED(err));

                DrString name;
                name.SetF("%s", nodeName);
                XcFreeMemory(nodeName);

                DrLogI("Found node name %s", name.GetChars());

                DrResourceRef resource = m_universe->LookUpResourceInternal(name);
                if (resource == DrNull)
                {
                    /* the scheduler may have added new resources we didn't know about originally, so be
                       ready for that */
                    AddNodeToUniverse(nodeId);
                    resource = m_universe->LookUpResourceInternal(name);
                    DrAssert(resource != DrNull);
                }

                process->SetAssignedNode(resource);
            }
            else if (notification->m_state >= DPS_Starting && notification->m_state != DPS_Zombie)
            {
                /* HACK. XCompute did not fill in the location, so we mustn't call in subsequently.
                   Setting the state to zombie will ensure everyone abandons it */
                DrLogW("Process didn't get assigned a node: setting to zombie");
                notification->m_state = DPS_Zombie;
                reason = "Process didn't get assigned a node";
                notification->m_status = DrNew DrError(DrError_XComputeError, "XCompute", reason);
            }
        }
    }

    m_messagePump->EnQueue(message);
}

void DrXComputeInternal::CancelScheduleProcess(DrProcessHandlePtr p)
{
    DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    DrXComputeOverlapped* overlapped = new DrXComputeCancelScheduleProcessOverlapped(this);

    XC_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.Size = sizeof(asyncInfo);
    asyncInfo.pOperationState = overlapped->GetOperationStatePtr();
    asyncInfo.IOCP = m_messagePump->GetCompletionPort();
    asyncInfo.pOverlapped = overlapped;

    DrLogI("Sending cancellation for scheduled process");

    m_messagePump->NotifySubmissionToCompletionPort(overlapped);

    XCERROR err =
        XcCancelScheduleProcess(process->m_handle, &asyncInfo);

    if (err != HRESULT_FROM_WIN32(ERROR_IO_PENDING))
    {
        DrLogA("Cancel schedule process failed synchronously error %s", DRERRORSTRING(err));
    }
}

void DrXComputeInternal::GetProcessProperty(DrProcessHandlePtr p, 
                                            UINT64 lastSeenVersion, DrString propertyName,
                                            DrTimeInterval maxBlockTime,
                                            DrPPSListenerPtr processListener,
                                            DrPropertyListenerPtr propertyListener)
{
    DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    DrLogI("Requesting property %s lastSeen %I64u maxBlock %lf", propertyName.GetChars(),
           lastSeenVersion, (double) maxBlockTime / (double) DrTimeInterval_Second);

    DrProcessPropertyStatusRef notification = DrNew DrProcessPropertyStatus();
    notification->m_process = process;

    DrPropertyStatusRef propertyNotification = DrNew DrPropertyStatus(DPBS_Running, STILL_ACTIVE, DrNull);
    DrPropertyMessageRef propertyMessage = DrNew DrPropertyMessage(propertyListener, propertyNotification);
    notification->m_message = propertyMessage;

    DrPPSMessageRef message = DrNew DrPPSMessage(processListener, notification);

    DrXComputeGetPropertyOverlapped* overlapped = new DrXComputeGetPropertyOverlapped(this, propertyName, message);

    XC_SETANDGETPROCESSINFO_REQINPUT requestInputs;
    memset(&requestInputs, 0, sizeof(requestInputs));
    requestInputs.Size = sizeof(requestInputs);
    requestInputs.pAppProcessConstraints = NULL;
    requestInputs.NumberOfProcessPropertiesToSet = 0;
    requestInputs.ppPropertiesToSet = NULL;
    requestInputs.pBlockOnPropertyLabel = propertyName.GetChars();
    requestInputs.BlockOnPropertyversionLastSeen = lastSeenVersion;
    requestInputs.MaxBlockTime = maxBlockTime;
    requestInputs.pPropertyFetchTemplate = propertyName.GetChars();
    requestInputs.ProcessInfoFetchOptions |=
        XCPROCESSINFOOPTION_STATICINFO |
        XCPROCESSINFOOPTION_TIMINGINFO |
        XCPROCESSINFOOPTION_PROCESSSTAT;

    XC_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.Size = sizeof(asyncInfo);
    asyncInfo.pOperationState = overlapped->GetOperationStatePtr();
    asyncInfo.IOCP = m_messagePump->GetCompletionPort();
    asyncInfo.pOverlapped = overlapped;

    m_messagePump->NotifySubmissionToCompletionPort(overlapped);

    XCERROR err = XcSetAndGetProcessInfo(process->m_handle, &requestInputs,
                                         overlapped->GetResultsPointer(), &asyncInfo);
    if (err != HRESULT_FROM_WIN32(ERROR_IO_PENDING))
    {
        DrLogA("XcSetAndGetProcessInfo failed with error %s", DRERRORSTRING(err));
    }
}

void DrXComputeInternal::ProcessPropertyFetch(HRESULT status, DrString propertyName,
                                              PXC_SETANDGETPROCESSINFO_REQRESULTS xcStatus,
                                              DrPPSMessagePtr message)
{
    /* Payload for process */
    DrProcessPropertyStatusPtr ppProcessStatus = message->GetPayload();

    /* Payload for vertex record */
    DrPropertyStatusPtr pVertexRecordStatus = ppProcessStatus->m_message->GetPayload();

    DrLogI("Process property fetch %s status %s", propertyName.GetChars(), DRERRORSTRING(status));

    if (!SUCCEEDED(status))
    {
        /* we really don't want to add cascading retries all through the stack,
           so we are going to take the bold stance that if XCompute says there
           was an error here, it has diligently retried in as sensible a way as
           we would have, and really, things are not going to improve. So the
           process is now dead to us. */

        pVertexRecordStatus->m_processState = DPBS_Failed;
        pVertexRecordStatus->m_exitCode = 1;
        DrString reason = "Property fetch failed";
        pVertexRecordStatus->m_status = DrNew DrError(status, "XCompute", reason);
    }
    else
    {
        PXC_PROCESS_INFO xcInfo = xcStatus->pProcessInfo;
        if (xcInfo == NULL)
        {
            /* we'll say this is Failed rather than Zombie which attempts to keep the process
               directory alive instead of just letting its lease expire */
            pVertexRecordStatus->m_processState = DPBS_Failed;
            pVertexRecordStatus->m_exitCode = 1;
            DrString reason = "Property fetch had no info";
            pVertexRecordStatus->m_status = DrNew DrError(DrError_Unexpected, "XCompute", reason);
        }
        else
        {
            DrProcessStatsRef stats = DrNew DrProcessStats();
            ppProcessStatus->m_statistics = stats;

            stats->m_exitCode = xcInfo->ExitCode;
            stats->m_pid = xcInfo->Win32Pid;

            pVertexRecordStatus->m_exitCode = xcInfo->ExitCode;
            /* TODO this seems very broken on the XCompute side: fake it up for now */
            DrProcessState state = TranslateXcState(xcInfo->ProcessState, xcInfo->ProcessStatus);
            if (pVertexRecordStatus->m_exitCode == STILL_ACTIVE)
            {
                DrLogI("Property fetch came back with process exitcode STILL_ACTIVE state %d", state);
                pVertexRecordStatus->m_processState = DPBS_Running;
            }
            else
            {
                DrLogI("Property fetch came back with process exitcode %u state %d status %s",
                    xcInfo->ExitCode, state, DRERRORSTRING(xcInfo->ProcessStatus));
                if (pVertexRecordStatus->m_exitCode == 0)
                {
                    pVertexRecordStatus->m_processState = DPBS_Completed;
                }
                else
                {
                    pVertexRecordStatus->m_processState = DPBS_Failed;
                    DrString reason = "Property fetch said process has failed";
                    pVertexRecordStatus->m_status = DrNew DrError(xcInfo->ProcessStatus, "DrXCompute", reason);
                }
            }

            if ((xcInfo->Flags & XCPROCESSINFOOPTION_TIMINGINFO) == XCPROCESSINFOOPTION_TIMINGINFO)
            {
                stats->m_createdTime = xcInfo->CreatedTime;
                stats->m_beginExecutionTime = xcInfo->BeginExecutionTime;
                stats->m_terminatedTime = xcInfo->TerminatedTime;
            }

            if ((xcInfo->Flags & XCPROCESSINFOOPTION_PROCESSSTAT) == XCPROCESSINFOOPTION_PROCESSSTAT)
            {
                PXC_PROCESS_STATISTICS xcStat = xcInfo->pProcessStatistics;
                DrAssert(xcStat != NULL);
                stats->m_userTime = xcStat->ProcessUserTime;
                stats->m_kernelTime = xcStat->ProcessKernelTime;
                stats->m_pageFaults = xcStat->PageFaults;
                stats->m_peakVMUsage = xcStat->PeakVMUsage;
                stats->m_peakMemUsage = xcStat->PeakMemUsage;
                stats->m_memUsageSeconds = xcStat->MemUsageSeconds;
                stats->m_totalIO = xcStat->TotalIo;
            }

            if (xcInfo->NumberofProcessProperties > 0)
            {
                PXC_PROCESSPROPERTY_INFO prop = xcInfo->ppProperties[0];
                if (strcmp(prop->pPropertyLabel, propertyName.GetChars()) == 0)
                {
                    DrLogI("Received new process property %s version %I64u",
                           propertyName.GetChars(), prop->PropertyVersion);
                    pVertexRecordStatus->m_statusVersion = prop->PropertyVersion;
                    if (prop->PropertyBlockSize > 0 && prop->PropertyBlockSize < 0x80000000)
                    {
                        pVertexRecordStatus->m_statusBlock = DrNew DrByteArray((int) prop->PropertyBlockSize);
                        DRPIN(BYTE) arrayDst = &(pVertexRecordStatus->m_statusBlock[0]);
                        memcpy(arrayDst, prop->pPropertyBlock, prop->PropertyBlockSize);
                    }
                }
                else
                {
                    DrLogW("Process fetch returned unexpected property %s", prop->pPropertyLabel);
                }
            }
        }
    }

    m_messagePump->EnQueue(message);
}

void DrXComputeInternal::SetProcessCommand(DrProcessHandlePtr p,
                                           UINT64 newVersion, DrString propertyName,
                                           DrString propertyDescription,
                                           DrByteArrayRef propertyBlock,
                                           DrErrorListenerPtr listener)
{
    DrXComputeProcessHandlePtr process = dynamic_cast<DrXComputeProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    DrErrorMessageRef message = DrNew DrErrorMessage(listener, DrNull);

    DrXComputeSetCommandOverlapped* overlapped = new DrXComputeSetCommandOverlapped(this, message);

    XC_PROCESSPROPERTY_INFO prop;
    memset(&prop, 0, sizeof(prop));
    prop.Size = sizeof(prop);
    prop.PropertyVersion = newVersion;
    prop.pPropertyLabel = (PSTR) propertyName.GetChars();
    prop.pPropertyString = (PSTR) propertyDescription.GetChars();
    DRPIN(BYTE) blockArray = &(propertyBlock[0]);
    prop.pPropertyBlock = (char *) blockArray;
    prop.PropertyBlockSize = propertyBlock->Allocated();
    PXC_PROCESSPROPERTY_INFO pProp = &prop;

    XC_SETANDGETPROCESSINFO_REQINPUT requestInputs;
    memset(&requestInputs, 0, sizeof(requestInputs));
    requestInputs.Size = sizeof(requestInputs);
    requestInputs.pAppProcessConstraints = NULL;
    requestInputs.NumberOfProcessPropertiesToSet = 1;
    requestInputs.ppPropertiesToSet = &pProp;

    XC_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.Size = sizeof(asyncInfo);
    asyncInfo.pOperationState = overlapped->GetOperationStatePtr();
    asyncInfo.IOCP = m_messagePump->GetCompletionPort();
    asyncInfo.pOverlapped = overlapped;

    m_messagePump->NotifySubmissionToCompletionPort(overlapped);

    XCERROR err = XcSetAndGetProcessInfo(process->m_handle, &requestInputs,
                                         overlapped->GetResultsPointer(), &asyncInfo);
    if (err != HRESULT_FROM_WIN32(ERROR_IO_PENDING))
    {
        DrLogA("XcSetAndGetProcessInfo failed with error %s", DRERRORSTRING(err));
    }
}

void DrXComputeInternal::ProcessCommandResult(HRESULT status, DrErrorMessagePtr message)
{
    if (!SUCCEEDED(status))
    {
        /* we really don't want to add cascading retries all through the stack,
           so we are going to take the bold stance that if XCompute says there
           was an error here, it has diligently retried in as sensible a way as
           we would have, and really, things are not going to improve. So the
           process is now dead to us. */

        /* We use the Failed state here instead of the Zombie state because we
           are relying on XCompute returning success here, but setting the process
           state to some zombie value, if the process has been garbage collected */
        DrString reason;
        reason.SetF("Set command RPC failed with error %s", DRERRORSTRING(status));
        message->SetPayload(DrNew DrError(status, "XCompute", reason));
    }
    else if (status != S_OK)
    {
        DrString reason;
        reason.SetF("Process command send succeeded with non-error status %s", DRERRORSTRING(status));
        message->SetPayload(DrNew DrError(status, "XCompute", reason));
    }

    m_messagePump->EnQueue(message);
}

void DrXComputeInternal::TerminateProcess(DrProcessHandlePtr /* unused p*/,
                                          DrErrorListenerPtr /* unused listener */)
{
    /* we have no way to do this right now */
}

void DrXComputeInternal::ResetProgress(UINT32 totalSteps, bool update)
{
    XcResetProgress(m_session, totalSteps, update);
}

void DrXComputeInternal::IncrementTotalSteps(bool update)
{
    XcIncrementTotalSteps(m_session, update);
}

void DrXComputeInternal::DecrementTotalSteps(bool update)
{
    XcDecrementTotalSteps(m_session, update);
}

void DrXComputeInternal::IncrementProgress(PCSTR message)
{
    XcIncrementProgress(m_session, message);
}

void DrXComputeInternal::CompleteProgress(PCSTR message)
{
    XcCompleteProgress(m_session, message);
}

