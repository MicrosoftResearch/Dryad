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

#include <DrVertexHeaders.h>

DrVertexTemplate::DrVertexTemplate()
{
    m_statusBlockTime = 0;
    m_listenerList = DrNew DrVertexListenerIRefList();
}

void DrVertexTemplate::SetStatusBlockTime(DrTimeInterval statusBlockTime)
{
    m_statusBlockTime = statusBlockTime;
}

DrTimeInterval DrVertexTemplate::GetStatusBlockTime()
{
    return m_statusBlockTime;
}

DrVertexListenerIRefListPtr DrVertexTemplate::GetListenerList()
{
    return m_listenerList;
}


DrInputChannelExecutionStatistics::DrInputChannelExecutionStatistics()
{
    m_dataRead = 0;
    m_tempDataRead = 0;
    m_tempDataReadCrossMachine = 0;
    m_tempDataReadCrossPod = 0;
}

DrOutputChannelExecutionStatistics::DrOutputChannelExecutionStatistics()
{
    Clear();
}

void DrOutputChannelExecutionStatistics::Clear()
{
    m_dataWritten = 0;
    m_dataIntraPod = 0;
    m_dataCrossPod = 0;
}

DrVertexExecutionStatistics::DrVertexExecutionStatistics()
{
    m_totalLocalInputData = 0UL;

    m_creationTime = DrDateTime_Never;
    m_startTime = DrDateTime_Never;
    m_runningTime = DrDateTime_Never;
    m_completionTime = DrDateTime_Never;

    m_exitCode = STILL_ACTIVE;
    m_exitStatus = S_OK;
}

void DrVertexExecutionStatistics::SetNumberOfChannels(int numberOfInputs, int numberOfOutputs)
{
    m_inputData = DrNew DrInputChannelStatsArray(numberOfInputs);
    m_outputData = DrNew DrOutputChannelStatsArray(numberOfOutputs);

    int i;
    for (i=0; i<numberOfInputs; ++i)
    {
        m_inputData[i] = DrNew DrInputChannelExecutionStatistics();
    }

    for (i=0; i<numberOfOutputs; ++i)
    {
        m_outputData[i] = DrNew DrOutputChannelExecutionStatistics();
    }
}


DrVertexVersionGenerator::DrVertexVersionGenerator(int version, int numberOfInputs)
{
    m_version = version;
    m_unfilledCount = numberOfInputs;
    m_generator = DrNew DrGeneratorArray(numberOfInputs);
}

int DrVertexVersionGenerator::GetNumberOfInputs()
{
    return m_generator->Allocated();
}

void DrVertexVersionGenerator::ResetVersion(int version)
{
    DrAssert(m_version == 0);
    m_version = version;
}

int DrVertexVersionGenerator::GetVersion()
{
    return m_version;
}

void DrVertexVersionGenerator::SetGenerator(int inputIndex, DrVertexOutputGeneratorPtr generator)
{
    if (generator == DrNull)
    {
        DrAssert(m_generator[inputIndex] != DrNull);
        m_generator[inputIndex] = DrNull;
        ++m_unfilledCount;
    }
    else
    {
        DrAssert(m_generator[inputIndex] == DrNull);
        m_generator[inputIndex] = generator;
        DrAssert(m_unfilledCount > 0);
        --m_unfilledCount;
    }
}

DrVertexOutputGeneratorPtr DrVertexVersionGenerator::GetGenerator(int inputIndex)
{
    return m_generator[inputIndex];
}

bool DrVertexVersionGenerator::Ready()
{
    return (m_unfilledCount == 0);
}


void DrVertexVersionGenerator::Compact(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction)
{
    DrAssert(m_generator->Allocated() == edgesBeingCompacted->GetNumberOfEdges());
    
    DrGeneratorArrayRef newArray = DrNew DrGeneratorArray(numberOfEdgesAfterCompaction);
    
    int nextCompactedLocation = 0;
    int i;
    for (i=0; i<m_generator->Allocated(); ++i)
    {
        DrEdge e = edgesBeingCompacted->GetEdge(i);
        if (e.m_type == DCT_Tombstone)
        {
            if (m_generator[i] == DrNull)
            {
                /* we were waiting for this edge before being able to run,
                but we can stop waiting for it since it has been pruned */
                DrAssert(m_unfilledCount > 0);
                --m_unfilledCount;
            }
        }
        else
        {
            newArray[nextCompactedLocation] = m_generator[i];
            ++nextCompactedLocation;
        }
    }
    DrAssert(nextCompactedLocation == numberOfEdgesAfterCompaction);    
    m_generator = newArray;
    
}


void DrVertexVersionGenerator::Grow(int numberOfEdgesToGrow)
{    
    int numberExisiting = m_generator->Allocated();
    DrGeneratorArrayRef newArray = DrNew DrGeneratorArray(numberExisiting+ numberOfEdgesToGrow);
    
    int i;
    for (i=0; i<numberExisiting; ++i)
    {
        newArray[i] = m_generator[i];
    }
    for (i=0; i<numberOfEdgesToGrow; ++i)
    {
        newArray[i + numberExisiting ] = DrNull;
        ++m_unfilledCount;
    }    
    m_generator = newArray;
}


DrVertexRecord::DrVertexRecord(DrXComputePtr xcompute, DrActiveVertexPtr parent,
                               DrCohortProcessPtr cohort, DrVertexVersionGeneratorPtr inputs,
                               DrVertexTemplatePtr vertexTemplate)
    : DrVertexNotifier(xcompute->GetMessagePump(), parent)
{
    m_parent = parent;
    m_inputs = inputs;
    m_vertexTemplate = vertexTemplate;
    m_cohort = cohort;
    m_state = DVS_NotStarted;
    m_lastSeenVersion = 0;

    m_creationTime = xcompute->GetCurrentTimeStamp();
    m_startTime = DrDateTime_Never;
    m_runningTime = DrDateTime_Never;
    m_completionTime = DrDateTime_Never;

    DrVertexListenerIRefListRef listeners = vertexTemplate->GetListenerList();
    int i;
    for (i=0; i<listeners->Size(); ++i)
    {
        AddListener(listeners[i]);
    }
}

void DrVertexRecord::Discard()
{
    m_parent = DrNull;
    m_generator = DrNull;
    m_inputs = DrNull;
    m_cohort = DrNull;
    m_process = DrNull;
}

void DrVertexRecord::MakeGenerator()
{
    DrAssert(m_generator == DrNull);
    m_generator = DrNew DrActiveVertexOutputGenerator();

    if (m_process.IsEmpty() == false)
    {
        DrLockBoxKey<DrProcess> process(m_process);

        m_generator->SetProcess(process->GetInfo()->m_state->m_process,
                                m_parent->GetId(), m_inputs->GetVersion());
    }
    else
    {
        m_generator->SetProcess(DrNull, m_parent->GetId(), m_inputs->GetVersion());
    }
}

DrActiveVertexOutputGeneratorPtr DrVertexRecord::GetGenerator()
{
    return m_generator;
}

int DrVertexRecord::GetVersion()
{
    return m_inputs->GetVersion();
}

DrVertexStatusRef DrVertexRecord::TryToParseProperty(DrPropertyStatusPtr prop)
{
    if (prop->m_statusBlock == DrNull)
    {
        return DrNull;
    }

    if (prop->m_statusVersion < m_lastSeenVersion)
    {
        DrLogW("Vertex record got out of order property %I64u < %I64u: ignoring",
               prop->m_statusVersion, m_lastSeenVersion);
        return DrNull;
    }

    /* update this even if the parsing fails below so we don't keep requesting the same
       unparseable property in a tight loop */
    m_lastSeenVersion = prop->m_statusVersion;

    DrVertexStatusRef status = DrNew DrVertexStatus();
    DrPropertyReaderRef parser = DrNew DrPropertyReader(prop->m_statusBlock);
    HRESULT err = parser->ReadAggregate(DrTag_VertexStatus, status);
    if (SUCCEEDED(err))
    {
        DrLogI("Vertex %d.%d parsed property new version is %I64u",
               m_parent->GetId(), GetVersion(), m_lastSeenVersion);
    }
    else
    {
        DrLogW("Vertex record got unparseable property version %I64u error %s: ignoring",
               prop->m_statusVersion, DRERRORSTRING(err));
        return DrNull;
    }

    return status;
}

bool IsLocalPath(DrString machineName, DrString channelURI)
{
    if ((machineName.GetCharsLength() == 0) ||
        (channelURI.GetCharsLength() == 0))
    {
        return false;
    }

    if (channelURI.Compare("\\\\", 2) != 0)
    {
        return true;
    } 
    else {
        
        int firstIndexOfSlash = channelURI.IndexOfChar('\\', 2);
        if (firstIndexOfSlash < 0)
        {
            return false;
        }

        if(firstIndexOfSlash - 2 == machineName.GetCharsLength())
        {
#ifdef _MANAGED
            if (System::String::Compare(channelURI.GetString(), 2, machineName.GetString(), 0, machineName.GetCharsLength(), 
                true) == 0)
            {
                //
                // If length of host name is same and strings match, then file is local
                //
                return true;
            }
#else 
            const char* uriSuffix = channelURI.GetChars() + 2;
            if (machineName.Compare(uriSuffix, machineName.GetCharsLength(), false) == 0)
            {
                return true;
            }
#endif
        }
    }
    return false;
}

DrVertexExecutionStatisticsRef DrVertexRecord::MakeExecutionStatistics(DrVertexStatusPtr status,
                                                                       UINT32 exitCode, HRESULT exitStatus)
{
    DrVertexExecutionStatisticsRef stats = DrNew DrVertexExecutionStatistics();

    stats->m_creationTime = m_creationTime;
    stats->m_startTime = m_startTime;
    stats->m_runningTime = m_runningTime;
    stats->m_completionTime = m_completionTime;

    stats->m_exitCode = exitCode;
    stats->m_exitStatus = exitStatus;

    if (status != DrNull)
    {
        DrString machineName = "(no computer)";

        if (m_process.IsEmpty() == false)
        {
            DrLockBoxKey<DrProcess> process(m_process);
            DrProcessHandlePtr handle = process->GetInfo()->m_state->m_process;
            if (handle != DrNull)
            {
                if (handle->GetAssignedNode() != DrNull)
                {
                    machineName = handle->GetAssignedNode()->GetName();
                }
            }
        }
        
        DrVertexProcessStatusPtr ps = status->GetProcessStatus();

        stats->SetNumberOfChannels(ps->GetInputChannels()->Allocated(), ps->GetOutputChannels()->Allocated());

        int i;
        if (ps->GetInputChannels()->Allocated() == m_inputs->GetNumberOfInputs())
        {
            stats->m_totalInputData = DrNew DrInputChannelExecutionStatistics();
            for (i=0; i<ps->GetInputChannels()->Allocated(); ++i)
            {
                DrChannelDescriptionPtr c = ps->GetInputChannels()[i];

                UINT64 dataRead = c->GetChannelProcessedLength();
                UINT64 tempData, inPod, crossPod;

                DrResourcePtr tempSource = m_inputs->GetGenerator(i)->GetResource();
                if (tempSource == DrNull)
                {
                    tempData = 0;
                    inPod = 0;
                    crossPod = 0;
                    if (IsLocalPath(machineName, c->GetChannelURI()))
                    {
                        stats->m_totalLocalInputData += dataRead;
                    }
                }
                else if (tempSource == m_generator->GetResource())
                {
                    tempData = dataRead;
                    inPod = 0;
                    crossPod = 0;
                }
                else if (tempSource->GetParent() == m_generator->GetResource()->GetParent())
                {
                    tempData = 0;
                    inPod = dataRead;
                    crossPod = 0;
                }
                else
                {
                    tempData = 0;
                    inPod = 0;
                    crossPod = dataRead;
                }

                stats->m_totalInputData->m_dataRead += dataRead;
                stats->m_totalInputData->m_tempDataRead += tempData;
                stats->m_totalInputData->m_tempDataReadCrossMachine += inPod;
                stats->m_totalInputData->m_tempDataReadCrossPod += crossPod;

                DrInputChannelExecutionStatisticsPtr cs = stats->m_inputData[i];
                cs->m_remoteMachine = tempSource;
                cs->m_dataRead = dataRead;
                cs->m_tempDataRead = tempData;
                cs->m_tempDataReadCrossMachine = inPod;
                cs->m_tempDataReadCrossPod = crossPod;
            }
        }

        stats->m_totalOutputData = DrNew DrOutputChannelExecutionStatistics();
        for (i=0; i<ps->GetOutputChannels()->Allocated(); ++i)
        {
            DrChannelDescriptionPtr c = ps->GetOutputChannels()[i];

            UINT64 dataWritten = c->GetChannelProcessedLength();

            stats->m_totalOutputData->m_dataWritten += dataWritten;

            DrOutputChannelExecutionStatisticsPtr cs = stats->m_outputData[i];
            cs->m_dataWritten = dataWritten;
        }
    }

    return stats;
}

void DrVertexRecord::TriggerFailure(DrErrorPtr error)
{
    /* fake a failed state message to ourselves */
    ReceiveMessage(DrNew DrPropertyStatus(DPBS_Failed, STILL_ACTIVE, error));
}

DrVertexVersionGeneratorPtr DrVertexRecord::NotifyProcessHasStarted(DrLockBox<DrProcess> process)
{
    DrAssert(m_state == DVS_NotStarted);

    /* this is the first sign that we have started running: save the process */
    DrAssert(process.IsEmpty() == false);
    m_process = process;

    /* we now need a generator record for upstream vertices connected using active
       edges to use */
    MakeGenerator();

    return m_inputs;
}

void DrVertexRecord::SetActiveInput(int inputPort, DrVertexOutputGeneratorPtr generator)
{
    DrAssert(m_inputs->GetGenerator(inputPort) == DrNull);
    m_inputs->SetGenerator(inputPort, generator);

    if (m_inputs->Ready())
    {
        StartRunning();
    }
}

void DrVertexRecord::StartRunning()
{
    DrAssert(m_state == DVS_NotStarted);

    /* send the command to start the vertex running */
    SendStartCommand();

    m_state = DVS_Starting;

    DrLogI("Vertex %d.%d transition to starting", m_parent->GetId(), GetVersion());

    /* we have changed state so tell our listeners */
    DrVertexInfoRef info = DrNew DrVertexInfo();
    info->m_name = m_parent->GetName();
    info->m_state = m_state;
    info->m_info = DrNull;
    info->m_statistics = DrNull;
    info->m_process = m_process;

    DeliverNotification(info);

    /* now that there is a process waiting, ask it for a property status */
    RequestStatus();
}

/* there are two places a message could come from: the cohort or the process. The cohort sends a Running state
   message when the process starts up, after which we are responsible for sending the process status requests
   which are the source of the process' responses. The cohort may also send a Failed state message, which we
   ignore if we have already seen successful completion from the process status, and believe otherwise */
void DrVertexRecord::ReceiveMessage(DrPropertyStatusRef prop)
{
    DrLogI("Vertex %d.%d receiving message while in state %d", m_parent->GetId(), GetVersion(), (int) m_state);

    if (m_state > DVS_RunningStatus)
    {
        /* we don't really care: we've already determined that we are completed or failed */
        return;
    }

    DrErrorRef error;
    DrString reason;

    bool requestStatus = false;
    bool sendNotification = false;
    DrVertexStatusRef status = TryToParseProperty(prop);
    if (status != DrNull)
    {
        /* tell the listeners every time there's a new status block */
        sendNotification = true;
    }

    HRESULT exitStatus = DrError_VertexRunning;

    if (status == DrNull)
    {
        DrLogI("Vertex %d.%d no status block in message", m_parent->GetId(), GetVersion());

        /* there was no status property block */
        switch (prop->m_processState)
        {
        case DPBS_NotStarted:
            DrLogA("Vertex record unexpectedly received message in NotStarted state");
            break;

        case DPBS_Running:
            DrAssert(m_state > DVS_NotStarted);

            if (m_state > DVS_Starting)
            {
                DrLogW("Vertex %d.%d sent empty status after it started running",
                       m_parent->GetId(), GetVersion());

                reason = "Process sent status with no parseable block";
                error = DrNew DrError(DrError_Unexpected, "DrVertexRecord", reason);

                m_state = DVS_Failed;
                exitStatus = error->m_code;
                /* we are changing state, so tell the listeners */
                sendNotification = true;

                DrLogI("Vertex %d.%d transition to failed", m_parent->GetId(), GetVersion());
            }
            /* else we're in the starting state, so it's ok to get a message without status
               since the remote vertex may not have written a property yet */
            break;

        case DPBS_Completed:
        case DPBS_Failed:
            DrLogI("Vertex %d.%d received message with processState %d", m_parent->GetId(), GetVersion(), prop->m_processState);
            error = prop->m_status;
            if (error == DrNull)
            {
                reason = "Process ended with no error status";
                error = DrNew DrError(DrError_Unexpected, "DrVertexRecord", reason);
            }

            if (error->m_code == DrError_CohortShutdown && m_process.IsEmpty() == false)
			{
				/* this is the cohort or graph killing us, so tell the process to terminate us */
                DrLogI("Vertex %d.%d this is the cohort or graph killing us, tell process to terminate us", m_parent->GetId(), GetVersion()); 
				SendTerminateCommand(m_parent->GetId(), m_inputs->GetVersion(), m_process);
			}

            m_state = DVS_Failed;
            exitStatus = error->m_code;
            /* we are changing state, so tell the listeners */
            sendNotification = true;

            DrLogI("Vertex %d.%d transition to failed", m_parent->GetId(), GetVersion());
            break;
        }
    }
    else
    {
        DrLogI("Vertex %d.%d has status block", m_parent->GetId(), GetVersion());

        if (m_state == DVS_Starting)
        {
            DrLogI("Vertex %d.%d transition to running", m_parent->GetId(), GetVersion());

            m_state = DVS_Running;
            m_runningTime = m_parent->GetStageManager()->GetGraph()->GetXCompute()->GetCurrentTimeStamp();

            DrVertexExecutionStatisticsRef startStats =
                MakeExecutionStatistics(status, prop->m_exitCode, exitStatus);
            /* we started running at some point: tell our parent. It may be that we have already
               completed or failed, in which case the state will be updated again below and the parent
               will get another message */
            m_parent->ReactToStartedVertex(this, startStats);

            DrVertexInfoRef info = DrNew DrVertexInfo();
            info->m_name = m_parent->GetName();
            info->m_state = m_state;
            info->m_info = status;
            info->m_statistics = startStats;
            info->m_process = m_process;
            DeliverNotification(info);

            /* we only send the DVS_Running state to listeners once: after that we repeatedly send
               DVS_RunningStatus messages until the vertex finishes */
            m_state = DVS_RunningStatus;
        }

        DrAssert(m_state == DVS_RunningStatus);
        DrAssert(m_process.IsEmpty() == false);

        exitStatus = status->GetVertexState();

        DrLogI("Vertex %d.%d processState %d vertexState %s", m_parent->GetId(), GetVersion(),
               prop->m_processState, DRERRORSTRING(exitStatus));

        switch (status->GetVertexState())
        {
        case DrError_VertexRunning:
            switch (prop->m_processState)
            {
            case DPBS_Running:
                /* no change of state, but request the next status message and send the status to
                   our listeners */
                DrLogI("Vertex %d.%d process running, request next status message and send status to listeners", m_parent->GetId(), GetVersion());
                requestStatus = true;
                sendNotification = true;
                break;

            case DPBS_Completed:
            case DPBS_Failed:
                /* the process stopped without the property being updated, so
                   fail the vertex */
                DrLogI("Vertex %d.%d process stopped without property being updated, fail the vertex", m_parent->GetId(), GetVersion());
                error = prop->m_status;
                if (error == DrNull)
                {
                    error = DrNew DrError(DrError_Unexpected, "DrVertexRecord",
                                          DrString("Process ended with no error status"));
                }

                if (error->m_code == DrError_CohortShutdown)
                {
                    /* this is the cohort or graph killing us, so tell the process to terminate us */
                    DrLogI("Vertex %d.%d killed by cohort or graph, tell process to terminate us", m_parent->GetId(), GetVersion());
                    SendTerminateCommand(m_parent->GetId(), m_inputs->GetVersion(), m_process);
                }
                m_state = DVS_Failed;
                exitStatus = error->m_code;
                /* we are changing state, so tell the listeners */
                sendNotification = true;

                DrLogI("Vertex %d.%d transition to failed unexpectedly", m_parent->GetId(), GetVersion());
                break;

            default:
                DrLogA("Unknown state");
            }
            break;

        case DrError_VertexCompleted:
            DrLogI("Vertex %d.%d transition to completed, old m_state %d", m_parent->GetId(), GetVersion(), m_state);
            m_state = DVS_Completed;

            DrAssert(m_generator != DrNull);
            m_generator->StoreOutputLengths(status->GetProcessStatus(),
                                            m_parent->GetStageManager()->GetGraph()->
                                            GetXCompute()->GetCurrentTimeStamp() -
                                            m_runningTime);

            /* we are changing state, so tell the listeners */
            sendNotification = true;

            DrLogI("Vertex %d.%d transition to completed, new m_state %d", m_parent->GetId(), GetVersion(), m_state);
            break;

        default:
            /* any error state */
            DrLogI("Vertex %d.%d transition to completed, old m_state %d", m_parent->GetId(), GetVersion(), m_state);
            m_state = DVS_Failed;

            reason.SetF("Vertex ended cleanly reporting error %s", DRERRORSTRING(status->GetVertexState()));
            error = DrNew DrError(status->GetVertexState(), "DrVertexRecord", DrString());

            if (status->GetProcessStatus()->GetVertexMetaData() != DrNull)
            {
                DrMetaDataPtr md = status->GetProcessStatus()->GetVertexMetaData();
                DrMTagHRESULTPtr code = dynamic_cast<DrMTagHRESULTPtr>(md->LookUp(DrProp_ErrorCode));
                DrMTagStringPtr stringTag = dynamic_cast<DrMTagStringPtr>(md->LookUp(DrProp_ErrorString));

                if (code != DrNull)
                {
                    DrString eString;
                    if (stringTag == DrNull || stringTag->GetValue().GetString() == DrNull)
                    {
                        eString = "No reason supplied";
                    }
                    else
                    {
                        eString = stringTag->GetValue();
                    }

                    DrErrorRef subReason = DrNew DrError(code->GetValue(), "RemoteVertex", eString);
                    error->AddProvenance(subReason);

                    DrString txt = DrError::ToShortText(subReason);
                    DrLogI("Vertex reported error '%s' as failure reason", txt.GetChars());
                }
            }

            /* we are changing state, so tell the listeners */
            sendNotification = true;

            DrLogI("Vertex %d.%d transition to failed neatly, m_state = %d", m_parent->GetId(), GetVersion(), m_state);
            break;
        }
    }

    if (m_state > DVS_Running)
    {
        m_completionTime = m_parent->GetStageManager()->GetGraph()->GetXCompute()->GetCurrentTimeStamp();
    }

    DrVertexExecutionStatisticsRef stats = MakeExecutionStatistics(status, prop->m_exitCode, exitStatus);

    if (sendNotification)
    {
        if (status == DrNull)
        {
            /* make sure the receivers of our notification have some idea what's going on */
            DrLogI("Vertex %d.%d status is null, creating one for receivers of notification", m_parent->GetId(), GetVersion());

            status = DrNew DrVertexStatus();
            status->GetProcessStatus()->SetVertexId(m_parent->GetId());
            status->GetProcessStatus()->SetVertexInstanceVersion(GetVersion());
            status->GetProcessStatus()->SetInputChannelCount(0);
            status->GetProcessStatus()->SetOutputChannelCount(0);
        }

        DrVertexInfoRef info = DrNew DrVertexInfo();
        info->m_name = m_parent->GetName();
        info->m_state = m_state;
        info->m_info = status;
        info->m_statistics = stats;
        info->m_process = m_process;
        DrLogI("Vertex %d.%d delivering notification", m_parent->GetId(), GetVersion());
        DeliverNotification(info);
    }

    if (requestStatus)
    {
        DrLogI("Vertex %d.%d requesting status", m_parent->GetId(), GetVersion());
        RequestStatus();
    }

    switch (m_state)
    {
    case DVS_NotStarted:
    case DVS_Starting:
        /* do nothing */
        break;

    case DVS_Running:
        DrLogA("Vertex %d.%d logic wrongly ended up in DVS_Running state", m_parent->GetId(), GetVersion());
        break;

    case DVS_RunningStatus:
        /* we have updated status: tell our parent */
        m_parent->ReactToRunningVertexUpdate(this, status->GetVertexState(), status->GetProcessStatus());
        break;

    case DVS_Completed:
    case DVS_Failed:
        DrLogI("Vertex %d.%d completed, state is now %d", m_parent->GetId(), GetVersion(), m_state);
        m_cohort->NotifyVertexCompletion();
        DrLogI("Vertex %d.%d notified cohort of completion, state is now %d", m_parent->GetId(), GetVersion(), m_state);

        m_cohort = DrNull;

        if (m_state == DVS_Completed)
        {
            DrLogI("Vertex %d.%d completed, calling ReactToCompletedVertex", m_parent->GetId(), GetVersion());
            m_parent->ReactToCompletedVertex(this, stats);
        }
        else
        {
            DrLogI("Vertex %d.%d failed, calling ReactToFailedVertex", m_parent->GetId(), GetVersion());
            DrAssert(error != DrNull);
            DrVertexProcessStatusPtr ps = DrNull;
            if (status != DrNull)
            {
                ps = status->GetProcessStatus();
            }

            if (m_generator == DrNull)
            {
                /* make a fake generator to send to the failure machinery */
                MakeGenerator();
            }

            m_parent->ReactToFailedVertex(m_generator, m_inputs, stats, ps, error);
        }
    }
}

void DrVertexRecord::RequestStatus()
{
    DrString label = DrVertexStatus::GetPropertyLabel(m_parent->GetId(), m_inputs->GetVersion());

    DrAssert(m_process.IsEmpty() == false);
    {
        DrLockBoxKey<DrProcess> process(m_process);
        process->RequestProperty(m_lastSeenVersion, label,
                                 m_vertexTemplate->GetStatusBlockTime(), this);
    }
}

void DrVertexRecord::SendStartCommand()
{
    DrString label = DrVertexCommandBlock::GetPropertyLabel(m_parent->GetId(), m_inputs->GetVersion());
    DrString description;
    description.SetF("Start command for vertex %d.%d", m_parent->GetId(), m_inputs->GetVersion());

    DrVertexCommandBlockRef startCommand =
        m_parent->MakeVertexStartCommand(m_inputs, m_generator->GetResource());

    DrPropertyWriterRef writer = DrNew DrPropertyWriter();
    startCommand->Serialize(writer);
    DrByteArrayRef block = writer->GetBuffer();

    DrAssert(m_process.IsEmpty() == false);
    {
        DrLockBoxKey<DrProcess> process(m_process);
        process->SendCommand(1, label, description, block);
    }

    m_startTime = m_parent->GetStageManager()->GetGraph()->GetXCompute()->GetCurrentTimeStamp();
}

void DrVertexRecord::SendTerminateCommand(int id, int version, DrLockBox<DrProcess> process)
{
    DrString label = DrVertexCommandBlock::GetPropertyLabel(id, version);
    DrString description;
    description.SetF("Terminate command for vertex %d.%d", id, version);

    DrVertexCommandBlockRef cmd = DrNew DrVertexCommandBlock();

    cmd->SetVertexCommand(DrVC_Terminate);
    cmd->GetProcessStatus()->SetVertexId(id);
    cmd->GetProcessStatus()->SetVertexInstanceVersion(version);

    DrPropertyWriterRef writer = DrNew DrPropertyWriter();
    cmd->Serialize(writer);
    DrByteArrayRef block = writer->GetBuffer();

    DrAssert(process.IsEmpty() == false);
    {
        DrLockBoxKey<DrProcess> p(process);
        p->SendCommand(2, label, description, block);
    }
}
