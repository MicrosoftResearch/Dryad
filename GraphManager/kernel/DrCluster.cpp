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
#include <DrClusterInternal.h>

using namespace System::Collections::Generic;

DrClusterResource::DrClusterResource(DrClusterPtr cluster,
                                     DrResourceLevel level,
                                     DrString name, DrString locality,
                                     DrResourcePtr parent, IComputer^ computer)
: DrResource(cluster, level, name, locality, parent)
{
    m_node = computer;
}

IComputer^ DrClusterResource::GetNode()
{
    return m_node;
}

DrClusterProcessHandle::DrClusterProcessHandle(DrClusterInternalRef p)
{
    m_parent = p;
    m_state = DPS_Initializing;
    m_id.Set("(not yet assigned)");
    m_exitCode = STILL_ACTIVE;
    m_lock = DrNew DrCritSec();
}

void DrClusterProcessHandle::CloseHandle()
{
    DrAutoCriticalSection acs(m_lock);

    if (m_processInternal != DrNull)
    {
		DrLogI("Closing handle");
        m_processInternal = DrNull;
    }
}

void DrClusterProcessHandle::SetProcess(IProcess^ process)
{
    DrAutoCriticalSection acs(m_lock);

    m_state = DPS_Scheduling;
    m_processInternal = process;
    m_id.Set(process->Id);
}

DrResourcePtr DrClusterProcessHandle::GetAssignedNode()
{
    DrAutoCriticalSection acs(m_lock);

    return m_node;
}

DrString DrClusterProcessHandle::GetHandleIdAsString()
{
    DrAutoCriticalSection acs(m_lock);

    DrString s(m_id);

    return s;
}

DrString DrClusterProcessHandle::GetDirectory()
{
    DrString directory;
    if (m_processInternal == DrNull)
    {
        directory.Set("(handle closed)");
    }
    else
    {
        directory.Set(m_processInternal->Directory);
    }
    return directory;
}

void DrClusterProcessHandle::WaitForStateChange(DrPSRListenerPtr listener)
{
    DrProcessStateRecordPtr notification;

    DrLogI("Waiting for state change");

    {
        DrAutoCriticalSection acs(m_lock);

        DrAssert(m_listener == DrNull);

        if (m_notification == DrNull)
        {
            m_listener = listener;
            return;
        }
        else
        {
            notification = m_notification;
            m_notification = DrNull;
        }
    }

    DrLogI("State change notification already present");

    DrPSRMessageRef message = DrNew DrPSRMessage(listener, notification);
    m_parent->GetMessagePump()->EnQueue(message);
}

void DrClusterProcessHandle::RecordNewState(DrProcessStateRecordPtr notification)
{
    DrPSRListenerIRef listener;

    DrLogI("Recording new state");

    {
        DrAutoCriticalSection acs(m_lock);

        if (m_listener == DrNull)
        {
            // replace previous notification if it didn't get matched to a listener
            // in time
            m_notification = notification;
            return;
        }
        else
        {
            listener = m_listener;
            m_listener = DrNull;
            m_notification = DrNull;
        }
    }

    DrLogI("Listener already present; dispatching new state");

    DrPSRMessageRef message = DrNew DrPSRMessage(listener, notification);
    m_parent->GetMessagePump()->EnQueue(message);
}

void DrClusterProcessHandle::Cancel(ICluster^ cluster)
{
    cluster->CancelProcess(m_processInternal);
}

void DrClusterProcessHandle::GetProperty(ICluster^ cluster, DrClusterProcessStatus^ status)
{
    cluster->GetProcessStatus(m_processInternal, status);
}

void DrClusterProcessHandle::SetProperty(ICluster^ cluster, DrClusterProcessCommand^ command)
{
    cluster->SetProcessCommand(m_processInternal, command);
}

void DrClusterProcessHandle::OnQueued()
{
    // nothing to report up the stack here
    DrString pid(m_processInternal->Id);
    DrLogI("Process %s got queued for scheduling", pid.GetChars());
}

void DrClusterProcessHandle::OnMatched(IComputer^ computer, INT64 timestamp)
{
    DrResourceRef resource = m_parent->GetOrAddResource(computer);

    DrString pid(m_processInternal->Id);
    DrLogI("Process %s got assigned node %s", pid.GetChars(), resource->GetName().GetChars());

    {
        DrAutoCriticalSection acs(m_lock);
        
        m_state = DPS_Starting;
        m_node = resource;
    }

    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_state = DPS_Starting;
    notification->m_process = this;
    notification->m_exitCode = STILL_ACTIVE;

    notification->m_creatingTime = DrDateTime_Never;
    notification->m_createdTime = DrDateTime_Never;
    notification->m_beginExecutionTime = DrDateTime_Never;
    notification->m_terminatedTime = DrDateTime_Never;

    RecordNewState(notification);
}

void DrClusterProcessHandle::OnCreated(INT64 timestamp)
{
    // nothing to report up the stack here
    DrString pid(m_processInternal->Id);
    DrLogI("Process %s got created on remote node", pid.GetChars());

    {
        DrAutoCriticalSection acs(m_lock);
        
        m_state = DPS_Created;
    }

    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_state = DPS_Created;
    notification->m_process = this;
    notification->m_exitCode = STILL_ACTIVE;

    notification->m_creatingTime = DrDateTime_LongAgo;
    notification->m_createdTime = (DrDateTime)timestamp;
    notification->m_beginExecutionTime = DrDateTime_Never;
    notification->m_terminatedTime = DrDateTime_Never;

    RecordNewState(notification);
}

void DrClusterProcessHandle::OnStarted(INT64 timestamp)
{
    DrString pid(m_processInternal->Id);
    DrLogI("Process %s started running on remote node", pid.GetChars());

    {
        DrAutoCriticalSection acs(m_lock);
        
        m_state = DPS_Running;
    }

    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_state = DPS_Running;
    notification->m_process = this;
    notification->m_exitCode = STILL_ACTIVE;

    notification->m_creatingTime = DrDateTime_LongAgo;
    notification->m_createdTime = DrDateTime_LongAgo;
    notification->m_beginExecutionTime = (DrDateTime)timestamp;
    notification->m_terminatedTime = DrDateTime_Never;

    RecordNewState(notification);
}

void DrClusterProcessHandle::OnExited(ProcessExitState state, INT64 timestamp, int exitCode, System::String^ errorText)
{
    DrString stateDescription(state.ToString());
    DrString reason(errorText);
    DrString pid(m_processInternal->Id);
    DrLogI("Process %s exited %d state %s reason %s", pid.GetChars(), exitCode, stateDescription.GetChars(), reason.GetChars());

    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();

    {
        DrAutoCriticalSection acs(m_lock);

        if (state == ProcessExitState::ProcessExited)
        {
            m_state = DPS_Completed;
        }
        else
        {
            m_state = DPS_Failed;

            DrErrorRef err = DrNew DrError(DrError_ClusterError, "Cluster", reason);
            notification->m_status = err;
        }

        m_exitCode = exitCode;
    }

    notification->m_process = this;
    notification->m_state = m_state;
    notification->m_exitCode = m_exitCode;

    notification->m_creatingTime = DrDateTime_LongAgo;
    notification->m_createdTime = DrDateTime_LongAgo;
    notification->m_beginExecutionTime = DrDateTime_LongAgo;
    notification->m_terminatedTime = (DrDateTime)timestamp;

    RecordNewState(notification);
}

DrClusterProcessStatus::DrClusterProcessStatus(DrString key, int timeout, UINT64 version,
                                               DrPropertyMessagePtr message, DrMessagePumpPtr pump,
                                               DrClusterInternalPtr cluster)
{
    m_key = key.GetString();
    m_timeout = timeout;
    m_version = version;
    m_message = message;
    m_pump = pump;
    m_cluster = cluster;
}

System::String^ DrClusterProcessStatus::GetKey()
{
    return m_key;
}

int DrClusterProcessStatus::GetTimeout()
{
    return m_timeout;
}

UINT64 DrClusterProcessStatus::GetVersion()
{
    return m_version;
}

void DrClusterProcessStatus::OnCompleted(UINT64 newVersion, array<unsigned char>^ statusBlock, int exitCode, System::String^ errorMessage)
{
    m_cluster->DecrementOutstandingPropertyRequests();

    DrString propertyName(m_key);

    /* Payload for vertex record */
    DrPropertyStatusPtr pVertexRecordStatus = m_message->GetPayload();

    pVertexRecordStatus->m_exitCode = exitCode;

    switch (exitCode)
    {
    case STILL_ACTIVE:
        DrLogI("Property fetch came back with process running");
        pVertexRecordStatus->m_processState = DPBS_Running;
        break;

    case 0:
        DrLogI("Property fetch came back with process completed");
        pVertexRecordStatus->m_processState = DPBS_Completed;
        break;

    default:
        DrLogI("Property fetch came back with process failed %d", exitCode);
        pVertexRecordStatus->m_processState = DPBS_Failed;
        pVertexRecordStatus->m_status = DrNew DrError(E_FAIL, "DrCluster", DrString("Already failed"));
        break;
    }

    if (errorMessage == DrNull)
    {
        DrLogI("Received new process property %s version %I64u", propertyName.GetChars(), newVersion);
        pVertexRecordStatus->m_statusVersion = newVersion;
        pVertexRecordStatus->m_statusBlock = DrNew DrByteArray();
        pVertexRecordStatus->m_statusBlock->SetArray(statusBlock);
    }
    else
    {
        DrString reason(errorMessage);
        DrLogI("Process property fetch %s failed %s", propertyName.GetChars(), reason.GetChars());
        pVertexRecordStatus->m_status = DrNew DrError(DrError_ClusterError, "DrCluster", reason);
    }

    m_pump->EnQueue(m_message);
}


DrClusterProcessCommand::DrClusterProcessCommand(DrString key, DrString shortStatus, array<unsigned char>^ payload,
                                                 DrErrorListenerPtr listener, DrMessagePumpPtr pump)
{
    m_key = key.GetString();
    m_shortStatus = shortStatus.GetString();
    m_payload = payload;
    m_pump = pump;
    m_listener = listener;
}

System::String^ DrClusterProcessCommand::GetKey()
{
    return m_key;
}

System::String^ DrClusterProcessCommand::GetShortStatus()
{
    return m_shortStatus;
}

array<unsigned char>^ DrClusterProcessCommand::GetPayload()
{
    return m_payload;
}

void DrClusterProcessCommand::OnCompleted(System::String^ errorMessage)
{
    DrString propertyName(m_key);

    DrErrorRef err;
    DrString reason;
    if (errorMessage == DrNull)
    {
        DrLogI("Process property set command %s succeeded", propertyName.GetChars());
        reason.SetF("Process command send succeeded");
        err = DrNew DrError(S_OK, "Cluster", reason);
    }
    else
    {
        DrString msg(errorMessage);
        DrLogI("Process property set command %s failed %s", propertyName.GetChars(), msg.GetChars());
        reason.SetF("Process command send failed with error %s", msg.GetChars());
        err = DrNew DrError(DrError_ClusterError, "Cluster", reason);
    }

    DrErrorMessageRef message = DrNew DrErrorMessage(m_listener, err);

    m_pump->EnQueue(message);
}


DrCluster::~DrCluster()
{
}

DrClusterRef DrCluster::Create()
{
    return DrNew DrClusterInternal();
}


DrClusterInternal::DrClusterInternal()
{
    m_cluster = DrNull;
    m_critSec = DrNew DrCritSec();
    m_outstandingPropertyRequests = 0;
    m_random = DrNew System::Random();
}

DrClusterInternal::~DrClusterInternal()
{
    Shutdown();
}

DrUniversePtr DrClusterInternal::GetUniverse()
{
    return m_universe;
}

DrMessagePumpPtr DrClusterInternal::GetMessagePump()
{
    return m_messagePump;
}

DrDateTime DrClusterInternal::GetCurrentTimeStamp()
{
    return m_messagePump->GetCurrentTimeStamp();
}

void DrClusterInternal::Log(
    System::String^ entry,
    [System::Runtime::CompilerServices::CallerFilePathAttribute] System::String^ file,
    [System::Runtime::CompilerServices::CallerMemberNameAttribute] System::String^ function,
    [System::Runtime::CompilerServices::CallerLineNumberAttribute] int line)
{
    DrString fileS(file);
    DrString functionS(function);
    DrString s(entry);
    if (DrLogging::Enabled(DrLog_Info)) DrLogHelper(DrLog_Info, fileS.GetChars(), functionS.GetChars(), line)("%s", s.GetChars());
}

HRESULT DrClusterInternal::Initialize(DrUniversePtr universe, DrMessagePumpPtr pump, DrTimeInterval updateInterval)
{
    m_universe = universe;
    m_messagePump = pump;
    m_propertyUpdateInterval = updateInterval;

    DrLogI("Initializing Scheduler");

    m_cluster = gcnew HttpCluster(this);

    if (m_cluster->Start())
    {
        FetchListOfComputers();
        return S_OK;
    }
    else
    {
        return DrError_ClusterError;
    }
}

void DrClusterInternal::Shutdown()
{
    m_cluster->Stop();
}

void DrClusterInternal::AddNodeToUniverse(IComputer^ computer)
{
    DrAutoCriticalSection acs(m_universe->GetResourceLock());

    AddNodeToUniverseUnderLock(computer);
}

DrResourcePtr DrClusterInternal::GetOrAddResource(IComputer^ computer)
{
    DrAutoCriticalSection acs(m_universe->GetResourceLock());

    DrString name(computer->Name);
    DrResourcePtr node = m_universe->LookUpResourceInternal(name);
    if (node == DrNull)
    {
        node = AddNodeToUniverseUnderLock(computer);
    }

    return node;
}

DrResourcePtr DrClusterInternal::AddNodeToUniverseUnderLock(IComputer^ computer)
{
    DrString name(computer->Name);
    DrString locality(computer->Host);
    DrString podName(computer->RackName);
    DrString podLocality(computer->RackName);

    DrResourceRef pod = m_universe->LookUpResourceInternal(podName);
    if (pod == DrNull)
    {
        pod = DrNew DrClusterResource(this, DRL_Rack, podName, podLocality, DrNull, DrNull);
        m_universe->AddResource(pod);
        DrLogI("Found pod %s", podName.GetChars());
    }

    DrResourceRef node = m_universe->LookUpResourceInternal(name);
    DrAssert(node == DrNull);
    node = DrNew DrClusterResource(this, DRL_Computer, name, locality, pod, computer);
    m_universe->AddResource(node);

    DrLogI("Found computer %s in pod %s", name.GetChars(), podName.GetChars());

    return node;
}

void DrClusterInternal::FetchListOfComputers()
{
    List<IComputer^>^ computers = m_cluster->GetComputers();

    for (int i=0; i<computers->Count; ++i)
    {
        AddNodeToUniverse(computers[i]);
    }
}

DrString DrClusterInternal::TranslateFileToURI(DrString leafName, DrString directory,
                                               DrResourcePtr srcResource, DrResourcePtr dstResource, int compressionMode)
{
    DrClusterResourcePtr src = dynamic_cast<DrClusterResourcePtr>(srcResource);
    DrClusterResourcePtr dst = dynamic_cast<DrClusterResourcePtr>(dstResource);

    IComputer^ srcComputer = src->GetNode();
    IComputer^ dstComputer = dst->GetNode();

    if (srcComputer->Host == dstComputer->Host)
    {
        return DrString(m_cluster->GetLocalFilePath(srcComputer, directory.GetString(), leafName.GetString(), compressionMode));
    }
    else
    {
        return DrString(m_cluster->GetRemoteFilePath(srcComputer, directory.GetString(), leafName.GetString(), compressionMode));
    }
}

void DrClusterInternal::ScheduleProcess(DrAffinityListRef affinities,
                                        DrString name, DrString commandLineArgs,
                                        DrProcessTemplatePtr processTemplate,
                                        DrPSRListenerPtr listener)
{
    DrLogI("Scheduling process with %d affinities", affinities->Size());

    List<Affinity^>^ affinityList = gcnew List<Affinity^>();
    int i;
    for (i=0; i<affinities->Size(); ++i)
    {
        DrAffinityPtr a = affinities[i];
        Affinity^ aa = gcnew Affinity(a->GetHardConstraint(), a->GetWeight());

        int j;
        for (j=0; j<a->GetLocalityArray()->Size(); ++j)
        {
            DrResourcePtr r = a->GetLocalityArray()[j];
            AffinityResource^ rr = gcnew AffinityResource((AffinityResourceLevel) r->GetLevel(), r->GetLocality().GetString());
            aa->affinities->Add(rr);
            DrLogI("Added affinity path %s", a->GetLocalityArray()[j]->GetLocality().GetChars());
        }

        aa->weight = a->GetWeight();

        affinityList->Add(aa);

        DrLogI("Added affinity with weight %I64u", aa->weight);
    }

    DrClusterProcessHandleRef process = DrNew DrClusterProcessHandle(this);

    DrLogI("Starting schedule process for %s.%s",
           processTemplate->GetProcessClass().GetChars(), name.GetChars());
	
    IProcess^ rawProcess = m_cluster->NewProcess(process, processTemplate->GetCommandLineBase().GetString(), commandLineArgs.GetString());
    process->SetProcess(rawProcess);

    DrLogI("Assigned GUID %s to process for %s.%s",
            process->GetHandleIdAsString().GetChars(),
           processTemplate->GetProcessClass().GetChars(), name.GetChars());

    DrProcessState state;
    DrString reason;
    m_cluster->ScheduleProcess(rawProcess, affinityList);

    DrLogI("Scheduled process for %s.%s",
           processTemplate->GetProcessClass().GetChars(), name.GetChars());
    state = DPS_Scheduling;
    reason = "Schedule process in progress";

    /* send the listener notification of the change of state */
    DrProcessStateRecordRef notification = DrNew DrProcessStateRecord();
    notification->m_state = state;
    notification->m_exitCode = STILL_ACTIVE;
    notification->m_process = process;

    DrPSRMessageRef message = DrNew DrPSRMessage(listener, notification);
    m_messagePump->EnQueue(message);

    process->WaitForStateChange(listener);
}

void DrClusterInternal::CancelScheduleProcess(DrProcessHandlePtr p)
{
    DrClusterProcessHandlePtr process = dynamic_cast<DrClusterProcessHandlePtr>(p);
    DrAssert(process != DrNull);
    process->Cancel(m_cluster);
}

void DrClusterInternal::DecrementOutstandingPropertyRequests()
{
        DrAutoCriticalSection acs(m_critSec);

        DrAssert(m_outstandingPropertyRequests > 0);
        --m_outstandingPropertyRequests;
}

void DrClusterInternal::GetProcessProperty(DrProcessHandlePtr p, 
                                           UINT64 lastSeenVersion, DrString propertyName,
                                           DrPropertyListenerPtr propertyListener)
{
    DrTimeInterval maxBlockTime;

    {
        DrAutoCriticalSection acs(m_critSec);

        ++m_outstandingPropertyRequests;
        DrAssert(m_outstandingPropertyRequests > 0);

        maxBlockTime = (DrTimeInterval)m_random->Next((int) (m_propertyUpdateInterval * m_outstandingPropertyRequests));
    }

    DrLogI("Requesting property %s lastSeen %I64u maxBlock %lf %d outstanding", propertyName.GetChars(),
        lastSeenVersion, (double) maxBlockTime / (double) DrTimeInterval_Second, m_outstandingPropertyRequests);

    DrPropertyStatusRef notification = DrNew DrPropertyStatus(DPBS_Running, STILL_ACTIVE, DrNull);
    DrPropertyMessageRef message = DrNew DrPropertyMessage(propertyListener, notification);

    DrClusterProcessStatusRef status =
        DrNew DrClusterProcessStatus(propertyName, (int) (maxBlockTime / DrTimeInterval_Millisecond), lastSeenVersion,
                                     message, m_messagePump, this);

    DrClusterProcessHandlePtr process = dynamic_cast<DrClusterProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    process->GetProperty(m_cluster, status);
}

void DrClusterInternal::SetProcessCommand(DrProcessHandlePtr p,
                                          DrString propertyName,
                                          DrString propertyDescription,
                                          DrByteArrayRef propertyBlock,
                                          DrErrorListenerPtr listener)
{
    DrLogI("Setting property %s %s", propertyName.GetChars(), propertyDescription.GetChars());

    DrClusterProcessHandlePtr process = dynamic_cast<DrClusterProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    DrClusterProcessCommandRef command =
        DrNew DrClusterProcessCommand(propertyName, propertyDescription, propertyBlock->GetArray(),
                                        listener, m_messagePump);

    process->SetProperty(m_cluster, command);
}

void DrClusterInternal::WaitForStateChange(DrProcessHandlePtr p, DrPSRListenerPtr listener)
{
    DrClusterProcessHandlePtr process = dynamic_cast<DrClusterProcessHandlePtr>(p);
    DrAssert(process != DrNull);

    process->WaitForStateChange(listener);
}

void DrClusterInternal::ResetProgress(UINT32 totalSteps, bool update)
{
    // Not yet implemented
}

void DrClusterInternal::IncrementTotalSteps(bool update)
{
    // Not yet implemented
}

void DrClusterInternal::DecrementTotalSteps(bool update)
{
    // Not yet implemented
}

void DrClusterInternal::IncrementProgress(PCSTR message)
{
    // Not yet implemented
}

void DrClusterInternal::CompleteProgress(PCSTR message)
{
    // Not yet implemented
}