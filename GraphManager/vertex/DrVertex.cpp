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

#ifndef _MANAGED
int DrVertexIdSource::s_nextId = 0;
#endif

int DrVertexIdSource::GetNextId()
{
    ++s_nextId;
    return s_nextId;
}

DrVertex::DrVertex(DrStageManagerPtr stage) : DrSharedCritSec(stage)
{
    m_stage = stage;
    m_partitionId = -1;

    m_id = DrVertexIdSource::GetNextId();
    m_name = stage->GetStageName();
    m_inputEdges = DrNew DrEdgeHolder(true);
    m_outputEdges = DrNew DrEdgeHolder(false);

    m_numberOfSubgraphInputs = 1;
    m_numberOfSubgraphOutputs = 1;
}

void DrVertex::InitializeFromOther(DrVertexPtr other, int suffix)
{
    if (suffix >= 0 && other->m_name.GetString() != DrNull)
    {
        m_name.SetF("%s[%d]", other->m_name.GetChars(), suffix);
        m_partitionId = suffix;
    }
    else
    {
        m_name = other->m_name;
    }

    m_inputEdges->SetNumberOfEdges(other->m_inputEdges->GetNumberOfEdges());
    m_outputEdges->SetNumberOfEdges(other->m_outputEdges->GetNumberOfEdges());

    m_numberOfSubgraphInputs = other->m_numberOfSubgraphInputs;
    m_numberOfSubgraphOutputs = other->m_numberOfSubgraphOutputs;
}

void DrVertex::Discard()
{
    DiscardDerived();

    m_stage = DrNull;
    m_inputEdges = DrNull;
    m_outputEdges = DrNull;
}

DrVertexRef DrVertex::MakeCopy(int suffix)
{
    return MakeCopy(suffix, m_stage);
}

DrStageManagerPtr DrVertex::GetStageManager()
{
    return m_stage;
}

int DrVertex::GetId()
{
    return m_id;
}

int DrVertex::GetPartitionId()
{
    return m_partitionId;
}

void DrVertex::SetName(DrString name)
{
    m_name = name;
}

DrString DrVertex::GetName()
{
    return m_name;
}

void DrVertex::SetNumberOfSubgraphInputs(int numberOfInputs)
{
    m_numberOfSubgraphInputs = numberOfInputs;
}

int DrVertex::GetNumberOfSubgraphInputs()
{
    return m_numberOfSubgraphInputs;
}

void DrVertex::SetNumberOfSubgraphOutputs(int numberOfOutputs)
{
    m_numberOfSubgraphOutputs = numberOfOutputs;
}

int DrVertex::GetNumberOfSubgraphOutputs()
{
    return m_numberOfSubgraphOutputs;
}

DrEdgeHolderPtr DrVertex::GetInputs()
{
    return m_inputEdges;
}

DrEdgeHolderPtr DrVertex::GetOutputs()
{
    return m_outputEdges;
}

DrVertexPtr DrVertex::RemoteInputVertex(int localPort)
{
    return m_inputEdges->GetEdge(localPort).m_remoteVertex;
}

int DrVertex::RemoteInputPort(int localPort)
{
    return m_inputEdges->GetEdge(localPort).m_remotePort;
}

DrVertexPtr DrVertex::RemoteOutputVertex(int localPort)
{
    return m_outputEdges->GetEdge(localPort).m_remoteVertex;
}

int DrVertex::RemoteOutputPort(int localPort)
{
    return m_outputEdges->GetEdge(localPort).m_remotePort;
}

void DrVertex::ConnectOutput(int localPort, DrVertexPtr remoteVertex, int remotePort, DrConnectorType type)
{
    DrEdge e;
    e.m_remoteVertex = remoteVertex;
    e.m_remotePort = remotePort;
    e.m_type = type;
    m_outputEdges->SetEdge(localPort, e);

    e.m_remoteVertex = this;
    e.m_remotePort = localPort;
    e.m_type = type;
    remoteVertex->GetInputs()->SetEdge(remotePort, e);
}

void DrVertex::DisconnectOutput(int localPort, bool disconnectRemote)
{
    DrEdge e;
    e.m_type = DCT_Tombstone;

    if (disconnectRemote)
    {
        RemoteOutputVertex(localPort)->GetInputs()->SetEdge(RemoteOutputPort(localPort), e);
    }    
    m_outputEdges->SetEdge(localPort, e);
}

void DrVertex::DisconnectInput(int localPort, bool disconnectRemote)
{
    DrEdge e;
    e.m_type = DCT_Tombstone;

    if (disconnectRemote)
    {
        RemoteInputVertex(localPort)->GetOutputs()->SetEdge(RemoteInputPort(localPort), e);
    }
    m_inputEdges->SetEdge(localPort, e);
}



DrActiveVertex::DrActiveVertex(DrStageManagerPtr stage, DrProcessTemplatePtr processTemplate,
                               DrVertexTemplatePtr vertexTemplate) : DrVertex(stage)
{
    m_vertexTemplate = vertexTemplate;
    m_processTemplate = processTemplate;

    m_affinity = DrNew DrAffinity();

    m_argument = DrNew DrStringList();

    m_totalOutputSizeHint = 0;
    m_maxOpenInputChannelCount = 0;
    m_maxOpenOutputChannelCount = 0;

    m_numberOfReportedCompletions = 0;
    m_registeredWithGraph = false;

    m_runningVertex = DrNew DrVertexRecordList();
    m_spareCompletedRecord = DrNew DrCompletedVertexList();
}

void DrActiveVertex::DiscardDerived()
{
    if (m_cohort != DrNull)
    {
        m_cohort->Discard();
    }
    m_cohort = DrNull;

    if (m_startClique != DrNull)
    {
        m_startClique->Discard();
    }
    m_startClique = DrNull;

    m_pendingVersion = DrNull;

    if (m_runningVertex != DrNull)
    {
        int i;
        for (i=0; i<m_runningVertex->Size(); ++i)
        {
            m_runningVertex[i]->Discard();
        }
    }
    m_runningVertex = DrNull;

    m_completedRecord = DrNull;

	m_spareCompletedRecord = DrNull;
}

DrVertexRef DrActiveVertex::MakeCopy(int suffix, DrStageManagerPtr stage)
{
    DrActiveVertexRef other = DrNew DrActiveVertex(stage, m_processTemplate, m_vertexTemplate);
    other->InitializeFromOther(this, suffix);

    int i;
    for (i=0; i<m_argument->Size(); ++i)
    {
        other->m_argument->Add(m_argument[i]);
    }

    other->m_affinity = m_affinity;

    other->m_outputSizeHint = m_outputSizeHint;
    other->m_totalOutputSizeHint = m_totalOutputSizeHint;
    other->m_maxOpenInputChannelCount = m_maxOpenInputChannelCount;
    other->m_maxOpenOutputChannelCount = m_maxOpenOutputChannelCount;

    DrVertexRef vo = other;
    return vo;
}

void DrActiveVertex::SetStartClique(DrStartCliquePtr startClique)
{
    m_startClique = startClique;
}

DrStartCliquePtr DrActiveVertex::GetStartClique()
{
    return m_startClique;
}

void DrActiveVertex::SetCohort(DrCohortPtr cohort)
{
    m_cohort = cohort;
}

DrCohortPtr DrActiveVertex::GetCohort()
{
    return m_cohort;
}


void DrActiveVertex::AddArgument(DrNativeString argument)
{
    AddArgumentInternal(DrString(argument));
}

void DrActiveVertex::AddArgumentInternal(DrString argument)
{
    m_argument->Add(argument);
}

DrAffinityPtr DrActiveVertex::GetAffinity()
{
    return m_affinity;
}

DrVertexOutputGeneratorPtr DrActiveVertex::GetOutputGenerator(int edgeIndex, DrConnectorType type, int version)
{
    DrEdge e = m_outputEdges->GetEdge(edgeIndex);
    DrAssert(e.m_type == type);

    int i;
    switch (type)
    {
    case DCT_File:
        /* doesn't matter what downstream version wants to run, just give it the completed file,
           if there is one */
        return m_completedRecord;

    case DCT_Output:
        /* nobody should be asking to read from this port */
        DrLogA("Someone is trying to get a generator for the source of an output edge");
        break;

    case DCT_Pipe:
    case DCT_Fifo:
    case DCT_FifoNonBlocking:
        /* the downstream version can only connect to the matching upstream version */
        for (i=0; i<m_runningVertex->Size(); ++i)
        {
            DrActiveVertexOutputGeneratorPtr g = m_runningVertex[i]->GetGenerator();
            if (g != DrNull)
            {
                /* the vertex has finished waiting for an available process and actually started running */
                if (g->GetVersion() == version)
                {
                    return g;
                }
            }
        }
        return DrNull;
    }

    return DrNull;
}

DrString DrActiveVertex::GetURIForWrite(int port,
                                        int /* unused id */, int /* unused version */,
                                        int /* unused outputPort */,
                                        DrResourcePtr /* unused runningResource */,
                                        DrMetaDataRef /* unused metaData */)
{
    /* only output vertices can supply this */
    DrLogA("Someone is trying to get a URI for write on active vertex %d port %d", m_id, port);
    return DrString();
}

DrVertexCommandBlockRef DrActiveVertex::MakeVertexStartCommand(DrVertexVersionGeneratorPtr inputGenerators,
                                                               DrActiveVertexOutputGeneratorPtr selfGenerator)
{
    DrVertexCommandBlockRef cmd = DrNew DrVertexCommandBlock();
    cmd->SetVertexCommand(DrVC_Start);

    cmd->SetArgumentCount(m_argument->Size());
    int i;
    for (i=0; i<m_argument->Size(); ++i)
    {
        cmd->GetArgumentVector()[i] = m_argument[i];
    }

    DrVertexProcessStatusPtr ps = cmd->GetProcessStatus();

    ps->SetVertexId(m_id);
    ps->SetVertexInstanceVersion(inputGenerators->GetVersion());

    DrAssert(m_inputEdges->GetNumberOfEdges() == inputGenerators->GetNumberOfInputs());

    ps->SetInputChannelCount(m_inputEdges->GetNumberOfEdges());
    ps->SetMaxOpenInputChannelCount(m_maxOpenInputChannelCount);
    for (i=0; i<m_inputEdges->GetNumberOfEdges(); ++i)
    {
        DrVertexOutputGeneratorPtr g = inputGenerators->GetGenerator(i);
        DrEdge e = m_inputEdges->GetEdge(i);
        DrChannelDescriptionPtr c = ps->GetInputChannels()[i];

        DrString uri = g->GetURIForRead(e.m_remotePort, e.m_type, selfGenerator->GetResource());
        DrAffinityRef affinity = g->GetOutputAffinity(e.m_remotePort);

        c->SetChannelState(S_OK);
        c->SetChannelURI(uri);
        c->SetChannelTotalLength(affinity->GetWeight());
    }

    ps->SetOutputChannelCount(m_outputEdges->GetNumberOfEdges());
    ps->SetMaxOpenOutputChannelCount(m_maxOpenOutputChannelCount);

    if (m_outputSizeHint != DrNull)
    {
        DrAssert(m_outputSizeHint->Allocated() == m_outputEdges->GetNumberOfEdges());
    }

    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrMetaDataRef metaData = DrNew DrMetaData();
        if (m_outputSizeHint != DrNull || m_totalOutputSizeHint > 0)
        {
            UINT64 sizeHint;
            if (m_outputSizeHint == DrNull)
            {
                sizeHint = m_totalOutputSizeHint / (UINT64) (m_outputEdges->GetNumberOfEdges());
            }
            else
            {
                sizeHint = m_outputSizeHint[i];
            }

            metaData->Append(DrNew DrMTagUInt64(DrProp_InitialChannelWriteSize, sizeHint));
        }

        DrConnectorType t = m_outputEdges->GetEdge(i).m_type;

        DrString uri = selfGenerator->GetURIForWrite(m_outputEdges, i, t, metaData);

        DrChannelDescriptionPtr c = ps->GetOutputChannels()[i];
        c->SetChannelState(S_OK);
        c->SetChannelURI(uri);
        c->SetChannelMetaData(metaData);
    }

    return cmd;
}

void DrActiveVertex::KickStateMachine()
{
    if (m_stage->GetGraph()->IsRunning() == false)
    {
        /* the graph is aborting so no new versions are desired */
        return;
    }

	/* Get the gang to set up a new pending version for us and everyone else
	   in the gang if necessary. */
	m_cohort->GetGang()->EnsurePendingVersion(0);
}

void DrActiveVertex::RequestDuplicate(int versionToDuplicate)
{
    if (m_stage->GetGraph()->IsRunning() == false)
    {
        /* the graph is aborting so no new versions are desired */
        return;
    }

	/* Get the gang to set up a new pending version for us and everyone else
	   in the gang if necessary. */
	m_cohort->GetGang()->EnsurePendingVersion(versionToDuplicate);
}

void DrActiveVertex::NotifyVertexIsReady()
{
    DrAssert(m_stage->VertexIsReady(this));
    m_cohort->GetGang()->DecrementUnreadyVertices();
}

void DrActiveVertex::InitializeForGraphExecution()
{
    DrLogI("Vertex %d(%s) %s", m_id, m_name.GetChars(),
           (m_registeredWithGraph) ? "already initialized" : "initializing now");

    if (!m_registeredWithGraph)
    {
        m_stage->GetGraph()->IncrementActiveVertexCount();
        m_registeredWithGraph = true;

        m_cohort = DrNew DrCohort(m_processTemplate, this);
        m_startClique = DrNew DrStartClique(this);
        /* this sets the gang in the run clique and cohort, so we don't need to keep a reference afterwards */
        DrGangRef gang = DrNew DrGang(m_cohort, m_startClique);
        m_cohort->SetGang(gang);
        m_startClique->SetGang(gang);

        if (m_stage->VertexIsReady(this) == false)
        {
            m_cohort->GetGang()->IncrementUnreadyVertices();
        }
    }
}

void DrActiveVertex::RemoveFromGraphExecution()
{
    DrAssert(m_registeredWithGraph);

    DrLogI("Vertex %d(%s)", m_id, m_name.GetChars());
    m_stage->GetGraph()->DecrementActiveVertexCount();

    m_registeredWithGraph = false;
}

void DrActiveVertex::ReportFinalTopology(DrVertexTopologyReporterPtr reporter)
{
    DrResourcePtr location = DrNull;
    DrTimeInterval time = 0;
    if (m_completedRecord != DrNull)
    {
        location = m_completedRecord->GetResource();
        time = m_completedRecord->GetRunningTime();
    }
    reporter->ReportFinalTopology(this, location, time);
}

DrVertexRecordPtr DrActiveVertex::GetRunningVersion(int version)
{
    int i;
    for (i=0; i<m_runningVertex->Size(); ++i)
    {
        if (m_runningVertex[i]->GetVersion() == version)
        {
            return m_runningVertex[i];
        }
    }
    return DrNull;
}

void DrActiveVertex::GrowPendingVersion(int numberOfEdgesToGrow)
{
	if (m_pendingVersion != DrNull)
	{
		m_pendingVersion->Grow(numberOfEdgesToGrow);
    }
}

/* !!! This is only ever called via DrGang::EnsurePendingVersion */
void DrActiveVertex::InstantiateVersion(int version)
{
	DrAssert(m_pendingVersion == DrNull);

	DrVertexVersionGeneratorRef p = DrNew DrVertexVersionGenerator(version, m_inputEdges->GetNumberOfEdges());

    int externalReadyInputs = 0;
    int i;
    for (i=0; i<m_inputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_inputEdges->GetEdge(i);
        DrVertexOutputGeneratorPtr g = e.m_remoteVertex->GetOutputGenerator(e.m_remotePort, e.m_type,
                                                                            version);
        if (g != DrNull)
        {
            p->SetGenerator(i, g);
            if (e.IsStartCliqueEdge() == false)
            {
                ++externalReadyInputs;
            }
        }
    }

    m_pendingVersion = p;

    if (externalReadyInputs > 0)
    {
        /* this just decrements the count of ready inputs in the start clique: it won't actually trigger
           anything to happen even if all the inputs are ready */
        m_startClique->NotifyExternalInputsReady(version, externalReadyInputs);
    }
}

void DrActiveVertex::AddCurrentAffinitiesToList(int version, DrAffinityListPtr list)
{
    /* add our vertex affinity if any */
    if (m_affinity != DrNull)
    {
        list->Add(m_affinity);
    }

    if (m_pendingVersion == DrNull || m_pendingVersion->GetVersion() != version)
    {
        DrLogA("Requested version %d doesn't exist in vertex %d(%s)",
               version, GetId(), GetName().GetChars());
    }

	DrAssert(m_pendingVersion->GetNumberOfInputs() == m_inputEdges->GetNumberOfEdges());

    int i;
    for (i=0; i<m_pendingVersion->GetNumberOfInputs(); ++i)
    {
        /* now add the affinities from all the generators we have managed to accumulate at
           this point. Some of them may be null if we aren't ready to run yet: this can happen
           if we are being called because someone else in the cohort is ready */
        DrVertexOutputGeneratorPtr inputGenerator = m_pendingVersion->GetGenerator(i);
        if (inputGenerator != DrNull)
        {
            DrEdge e = m_inputEdges->GetEdge(i);
            if (e.IsGangEdge())
            {
                int inputVersion = inputGenerator->GetVersion();
                /* gang edges should be paired to have both ends running the same version */
                DrAssert(inputVersion == version);
            }

            DrAffinityRef inputAffinity = inputGenerator->GetOutputAffinity(e.m_remotePort);
            if (inputAffinity != DrNull)
            {
                list->Add(inputAffinity);
            }
        }
    }
}

void DrActiveVertex::StartProcess(int version)
{
    DrAssert(m_stage->VertexIsReady(this));

    /* this will start the process if we are the first vertex in the cohort to want to start */
    DrCohortProcessRef cohortProcess = m_cohort->EnsureProcess(m_stage->GetGraph(), version);

    DrVertexVersionGeneratorRef generator = m_pendingVersion;
	m_pendingVersion = DrNull;
	DrAssert(generator != DrNull && generator->GetVersion() == version);

    DrVertexRecordRef execution = DrNew DrVertexRecord(m_stage->GetGraph()->GetCluster(), this,
                                                       cohortProcess, generator, m_vertexTemplate);

    m_runningVertex->Add(execution);
}

void DrActiveVertex::CheckForProcessAlreadyStarted(int version)
{
   DrCohortProcessRef cohortProcess = m_cohort->GetProcessForVersion(version);
   DrAssert(cohortProcess != DrNull);

   if (cohortProcess->ProcessHasStarted())
   {
        ReactToStartedProcess(version, cohortProcess->GetProcess());
   }
}

void DrActiveVertex::ReactToStartedProcess(int version, DrLockBox<DrProcess> process)
{
    DrVertexRecordPtr record = GetRunningVersion(version);
    if (record == DrNull)
    {
        if (m_pendingVersion == DrNull || m_pendingVersion->GetVersion() != version)
        {
            DrLogA("Vertex %d(%s) has no pending or running version %d", m_id, m_name.GetChars(), version);
        }
        return;
    }

    DrVertexVersionGeneratorPtr inputs = record->NotifyProcessHasStarted(process);
    if (inputs->Ready())
    {
        record->StartRunning();
    }

    int i;
    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_outputEdges->GetEdge(i);
        e.m_remoteVertex->ReactToUpStreamRunningProcess(e.m_remotePort, e.m_type,
                                                        record->GetGenerator());
    }
}

void DrActiveVertex::ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                                   DrVertexOutputGeneratorPtr generator)
{
    DrEdge e = m_inputEdges->GetEdge(inputPort);
    DrAssert(e.m_type == type);

    switch (type)
    {
    case DCT_File:
    case DCT_FifoNonBlocking:
        /* we don't care when these processes start running. The non blocking active
           edges are taken care of when the vertices start running. */
        return;

    case DCT_Output:
        /* there shouldn't be an edge of this type leading to an active vertex */
        DrLogA("Output edge leading to active vertex %d on port %d", m_id, inputPort);
        break;

    case DCT_Pipe:
    case DCT_Fifo:
        break;
    }

    /* this won't be called until everyone in the start clique has had its StartProcess
       method called, so there ought to be a running version */
    DrVertexRecordPtr record = GetRunningVersion(generator->GetVersion());
    DrAssert(record != DrNull);

    /* if this was the last active input it was waiting for, it will start itself running */
    record->SetActiveInput(inputPort, generator);
}

void DrActiveVertex::ReactToStartedVertex(DrVertexRecordPtr record, DrVertexExecutionStatisticsPtr stats)
{
    DrActiveVertexOutputGeneratorPtr generator = record->GetGenerator();
    DrAssert(generator != DrNull);

    m_stage->NotifyVertexRunning(this, record->GetVersion(), generator->GetResource(), stats);

    int i;
    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_outputEdges->GetEdge(i);
        e.m_remoteVertex->ReactToUpStreamRunningVertex(e.m_remotePort, e.m_type, generator);
    }
}

void DrActiveVertex::ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                                  DrVertexOutputGeneratorPtr generator)
{
    DrEdge e = m_inputEdges->GetEdge(inputPort);
    DrAssert(e.m_type == type);

    switch (type)
    {
    case DCT_File:
    case DCT_Pipe:
    case DCT_Fifo:
        /* we don't care when these vertices start running. The blocking active edges
           were taken care of when the process started running. */
        return;

    case DCT_Output:
        /* there shouldn't be an edge of this type leading to an active vertex */
        DrLogA("Output edge leading to active vertex %d on port %d", m_id, inputPort);
        break;

    case DCT_FifoNonBlocking:
        break;
    }

    /* nobody in the gang should be running unless we're all ready */
    DrAssert(m_stage->VertexIsReady(this));

    /* the versions have to match up at the ends of an active edge */
	DrAssert(m_pendingVersion != DrNull && m_pendingVersion->GetVersion() == generator->GetVersion());

    DrAssert(m_pendingVersion->GetGenerator(inputPort) == DrNull);
    m_pendingVersion->SetGenerator(inputPort, generator);

    /* this just decrements the count of ready inputs in the start clique: it won't actually trigger
       anything to happen even if all the inputs are ready */
    m_startClique->NotifyExternalInputsReady(m_pendingVersion->GetVersion(), 1);

    /* now actually start everyone in our clique if this was the last edge we were waiting for */
    m_startClique->StartVersionIfReady(m_pendingVersion->GetVersion());
}

void DrActiveVertex::ReactToRunningVertexUpdate(DrVertexRecordPtr /* unused record */,
                                                HRESULT exitStatus, DrVertexProcessStatusPtr status)
{
    m_stage->NotifyVertexStatus(this, exitStatus, status);
}

void DrActiveVertex::ReactToCompletedVertex(DrVertexRecordPtr record, DrVertexExecutionStatisticsPtr stats)
{
    DrLogI("Reacting to completed vertex %d.%d", this->m_id, record->GetVersion());
	DrActiveVertexOutputGeneratorRef newCompletedRecord = record->GetGenerator();
	DrAssert(newCompletedRecord != DrNull);
    
	bool becomingComplete = false;
    if (m_completedRecord == DrNull)
    {
        DrLogI("Becoming complete");
        becomingComplete = true;
		m_completedRecord = newCompletedRecord;
    }
	else
	{
        DrLogI("Adding spare completed record");
		m_spareCompletedRecord->Add(newCompletedRecord);
	}
    
    //
    // go down output edges and let the start clique notify external
    // inputs ready, this just does bookkeeping on the invariants for each
	// downstream vertex about whether its inupts are ready or not
    //
    int i;
    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_outputEdges->GetEdge(i);
        e.m_remoteVertex->NotifyUpStreamCompletedVertex(e.m_remotePort, e.m_type, newCompletedRecord);
    }
    
	//
	// during this call the graph may be rewritten!!! The set of output edges may be different,
	// in particular
	//
    DrLogI("Notifying stage of vertex %d.%d completion", this->m_id, record->GetVersion());
    m_stage->NotifyVertexCompleted(this, record->GetVersion(), newCompletedRecord->GetResource(), stats);
    ++m_numberOfReportedCompletions;

    DrString message;
    message.SetF("completed vertex %s", (PCSTR)m_name.GetChars());
    m_stage->GetGraph()->GetCluster()->IncrementProgress(message.GetChars());
    
    //
    // go down output edges and actually prod the vertices to start running if
    // their external inputs are ready. This is separated from the bookkeeping above
	// because invariant-checking is done during graph rewrites and it makes things simpler
	// to have the number of ready inputs correct before the rewrite. However we can't
	// actually start things running (in the following loop) until after the rewrite, obviously
    //
    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_outputEdges->GetEdge(i);
        e.m_remoteVertex->ReactToUpStreamCompletedVertex(e.m_remotePort, e.m_type, newCompletedRecord, stats);
    }

    m_runningVertex->Remove(record);

    if (becomingComplete)
    {
        DrLogI("Notifying graph of vertex %d.%d completion", this->m_id, record->GetVersion());
        m_stage->GetGraph()->NotifyActiveVertexComplete();
    }

	//
	// The gang keeps track of the number of vertices in any given version that have successfully
	// completed. If everyone in the gang manages to get a completed vertex with the same version
	// then we declare success and, for example, kill of any duplicate executions within the gang
	//
    DrLogI("Calling ReactToCompletedVertex for gang");
	m_cohort->GetGang()->ReactToCompletedVertex(newCompletedRecord->GetVersion());
}

void DrActiveVertex::NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator)
{
    DrEdge e = m_inputEdges->GetEdge(inputPort);
    DrAssert(e.m_type == type);
    
    if (type != DCT_File)
    {
        /* we don't care when these complete */
        return;
    }
    
    if (m_pendingVersion != DrNull)
    {
        /* there shouldn't be pending versions if we aren't ready to run */
        DrAssert(m_stage->VertexIsReady(this));
            
        if (m_pendingVersion->GetGenerator(inputPort) == DrNull)
        {
            m_pendingVersion->SetGenerator(inputPort, generator);
        }
        else
        {
            DrAssert(m_pendingVersion->Ready() == false);
        }

        /* this just decrements the count of ready inputs in the start clique: it
           won't actually trigger anything to happen even if all the inputs are ready */
        m_startClique->NotifyExternalInputsReady(m_pendingVersion->GetVersion(), 1);
    }
}

void DrActiveVertex::ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                    DrVertexOutputGeneratorPtr /* unused generator */,
                                                    DrVertexExecutionStatisticsPtr /* unused stats */)
{
    DrEdge e = m_inputEdges->GetEdge(inputPort);
    DrAssert(e.m_type == type);

    if (type != DCT_File)
    {
        /* we don't care when these complete */
        return;
    }

    if (m_pendingVersion != DrNull)
    {
        /* someone's waiting to run: let them all know we at least are ready */

		/* there shouldn't be pending versions if we aren't ready to run */
        DrAssert(m_stage->VertexIsReady(this));

        /* now actually start everyone in our clique if this was the last edge
           we were waiting for. If the version is ready to start then m_pendingVersion will
		   be DrNull when the call returns */
        m_startClique->StartVersionIfReady(m_pendingVersion->GetVersion());
    }
}

bool DrActiveVertex::HasPendingVersion()
{
	return (m_pendingVersion != DrNull);
}

bool DrActiveVertex::HasRunningVersion(int version)
{
	int i;
	for (i=0; i<m_runningVertex->Size(); ++i)
	{
		if (m_runningVertex[i]->GetVersion() == version)
		{
			return true;
		}
	}

	return false;
}

bool DrActiveVertex::HasCompletedVersion(int version)
{
	if (m_completedRecord == DrNull)
	{
		DrAssert(m_spareCompletedRecord->Size() == 0);
		return false;
	}

	if (m_completedRecord->GetVersion() == version)
	{
		return true;
	}

	int i;
	for (i=0; i<m_spareCompletedRecord->Size(); ++i)
	{
		if (m_spareCompletedRecord[i]->GetVersion() == version)
		{
			return true;
		}
	}

	return false;
}

/* !!! This is only called from the Gang's CancelVersion method. It should stay that way, so that
   the gang's invariants about which version is completed etc. track the state of the member
   vertices */
void DrActiveVertex::CancelVersion(int version, DrErrorPtr error, DrCohortProcessPtr cohortProcess)
{
    DrLogI("Canceling version %d for vertex %d", version, m_id);
    if (m_pendingVersion != DrNull && m_pendingVersion->GetVersion() == version)
    {
        /* we weren't ready to start so we haven't got a vertex record */
        DrLogI("We weren't ready to start so we haven't got a vertex record");
        if (cohortProcess != DrNull)
        {
            if (cohortProcess->GetProcess().IsNull() == false)
            {
                /* however the process did already start, so we have to tell it we're never going
                   to run */
                DrLogI("However the process did already start, so we have to tell it we're never going to run");
                DrVertexRecord::SendTerminateCommand(m_id, version, cohortProcess->GetProcess());
            }
            cohortProcess->NotifyVertexCompletion();
        }
        m_pendingVersion = DrNull;

        return;
    }

	int i;
    for (i=0; i<m_runningVertex->Size(); ++i)
    {
        if (m_runningVertex[i]->GetVersion() == version)
        {
            /* this will handle all the cleanup, killing the vertex record and calling back in
			   to ReactToFailedVertex, all on this callstack, so nothing more to do */
            DrLogI("Found version %d in running vertices, triggering failure", version);
            m_runningVertex[i]->TriggerFailure(error);

			return;
        }
    }

	if (m_completedRecord != DrNull && m_completedRecord->GetVersion() == version)
	{
        /* this will handle the cleanup including setting the completed record to
		   NULL, managing the spare completed records, etc., so nothing more to do */
        DrLogI("Found version %d in completed record, calling ReactToFailedVertex", version);
		ReactToFailedVertex(m_completedRecord, DrNull, DrNull, DrNull, error);

		return;
	}

	for (i=0; i<m_spareCompletedRecord->Size(); ++i)
	{
		if (m_spareCompletedRecord[i]->GetVersion() == version)
		{
	        /* this will handle the cleanup including managing the spare completed records,
			   etc., so nothing more to do */
            DrLogI("Found version %d in spare completed records, calling ReactToFailedVertex", version);
			ReactToFailedVertex(m_spareCompletedRecord[i], DrNull, DrNull, DrNull, error);

			return;
		}
	}

    /* we had already stopped: nothing to do */
    DrLogI("Version %d already stopped, nothing to do", version);
}

void DrActiveVertex::ReactToDownStreamFailure(int port,
                                              DrConnectorType type,
                                              int /* unused downStreamVersion */)
{
    DrEdge e = m_outputEdges->GetEdge(port);
    DrAssert(e.m_type == type);

    /* a downstream output should not be failing */
    DrAssert(type != DCT_Output);

    /* nothing to do: vertices connected by active edges will be failed by the gang anyway */
}

void DrActiveVertex::ReactToUpStreamFailure(int port, DrConnectorType type,
                                            DrVertexOutputGeneratorPtr /* unused generator*/,
                                            int /* unused downStreamVersion */)
{
    DrEdge e = m_inputEdges->GetEdge(port);
    DrAssert(e.m_type == type);

    DrAssert(type != DCT_Output);

    if (m_pendingVersion != DrNull && e.m_type == DCT_File)
    {
        /* oldGenerator is the one we used to have from the vertex, if any */
        DrVertexOutputGeneratorRef oldGenerator = m_pendingVersion->GetGenerator(port);
        if (oldGenerator != DrNull)
        {
            DrVertexPtr remoteVertex = e.m_remoteVertex;
            int remotePort = e.m_remotePort;
            DrVertexOutputGeneratorRef failedVertexGenerator =
                remoteVertex->GetOutputGenerator(remotePort, e.m_type, 0);
            if (failedVertexGenerator == DrNull)
            {
                /* we were holding a generator from the upstream vertex but it has now
                been revoked */

                m_pendingVersion->SetGenerator(port, DrNull);
                m_startClique->GrowExternalInputs(1);
            }
        }
    }
}

void DrActiveVertex::ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                         DrVertexVersionGeneratorPtr inputs,
                                         DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                         DrErrorPtr originalReason)
{
    int version = failedGenerator->GetVersion();

	DrLogI("Vertex %d.%d", m_id, version);

    m_stage->NotifyVertexFailed(this, version, failedGenerator->GetResource(), stats);

    bool foundVersion = false;

    int i;
    for (i=0; i<m_runningVertex->Size(); ++i)
    {
        DrVertexRecordRef record = m_runningVertex[i];
        if (record->GetVersion() == version)
        {
            m_runningVertex->RemoveAt(i);

            int j;
            for (j=0; j<m_inputEdges->GetNumberOfEdges(); ++j)
            {
                DrEdge e = m_inputEdges->GetEdge(j);
                e.m_remoteVertex->ReactToDownStreamFailure(e.m_remotePort, e.m_type, version);
            }

            DrActiveVertexOutputGeneratorPtr generator = record->GetGenerator();
            if (generator != DrNull)
            {
                /* we have actually started running so tell any downstream guys that might care */
                for (j=0; j<m_outputEdges->GetNumberOfEdges(); ++j)
                {
                    DrEdge e = m_outputEdges->GetEdge(j);

                    e.m_remoteVertex->ReactToUpStreamFailure(e.m_remotePort, e.m_type,
                                                             generator, version);
                }
            }

            foundVersion = true;
			break;
        }
    }

    if (m_completedRecord != DrNull && m_completedRecord->GetVersion() == version)
    {
        DrActiveVertexOutputGeneratorRef generator = m_completedRecord;
        m_completedRecord = DrNull;

        for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
        {
            DrEdge e = m_outputEdges->GetEdge(i);

            e.m_remoteVertex->ReactToUpStreamFailure(e.m_remotePort, e.m_type, generator, version);
        }

		int nSpares = m_spareCompletedRecord->Size();
		if (nSpares > 0)
		{
			m_completedRecord = m_spareCompletedRecord[nSpares-1];
			m_spareCompletedRecord->RemoveAt(nSpares-1);
		}
		else
		{
	        m_stage->GetGraph()->NotifyActiveVertexRevoked();
		}

        foundVersion = true;
    }

	for (i=0; i<m_spareCompletedRecord->Size(); ++i)
	{
		if (m_spareCompletedRecord[i]->GetVersion() == version)
		{
	        DrActiveVertexOutputGeneratorRef generator = m_spareCompletedRecord[i];
			m_spareCompletedRecord->RemoveAt(i);

	        for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
		    {
			    DrEdge e = m_outputEdges->GetEdge(i);

	            e.m_remoteVertex->ReactToUpStreamFailure(e.m_remotePort, e.m_type, generator, version);
		    }

            foundVersion = true;
			break;
		}
	}

    if (foundVersion)
    {
        int inputToKill = m_stage->GetGraph()->ReportFailure(this, version, status, originalReason);

        if (inputToKill >= 0)
        {
            DrAssert(inputs != DrNull);

            /* if the failure manager thinks we should be killing one of our upstream vertices,
            tell it so */
            DrEdge inputEdgeToKill = m_inputEdges->GetEdge(inputToKill);
            DrVertexPtr vertex = inputEdgeToKill.m_remoteVertex;
            DrVertexOutputGeneratorPtr g = inputs->GetGenerator(inputToKill);

            DrString reason = "Downstream vertex reported a read error: invalidating completed version";
            DrErrorRef error = DrNew DrError(DrError_BadOutputReported, "DrActiveVertex", reason);
            error->AddProvenance(originalReason);

            DrErrorRef channelError = DrNew DrError(status->GetInputChannels()[inputToKill]->GetChannelErrorCode(),
                                                    "RemoteVertex",
                                                    status->GetInputChannels()[inputToKill]->GetChannelErrorString());
            error->AddProvenance(channelError);

            DrLogI("Vertex %d.%d: original %s", this->m_id, version, originalReason->ToShortText().GetChars());
            DrLogI("Vertex %d.%d: %s calling ReactToFailedVertex", this->m_id, version, reason.GetChars());
            vertex->ReactToFailedVertex(g, DrNull, DrNull, DrNull, error);
        }

        if (originalReason->m_code != DrError_CohortShutdown)
        {
            /* send out a failed state to everyone. anyone who has already completed successfully will ignore it */
            DrString reason = "Cohort being cancelled";
            DrErrorRef error = DrNew DrError(DrError_CohortShutdown, "DrCohort", reason);
            error->AddProvenance(originalReason);

            m_cohort->GetGang()->CancelVersion(version, error);
        }
    }
}


void DrActiveVertex::CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction)
{
    // a) assert that the  m_runningVertex list is empty and m_completedRecord is NULL
    //    (to make sure nobody is changing the vertex after it has
    //    started running)      
    DrAssert(m_runningVertex->Size() == 0);
	DrAssert(m_completedRecord == DrNull);

    // b) compact the m_pendingVersion if any
	if (m_pendingVersion != DrNull)
	{
        m_pendingVersion->Compact(edgesBeingCompacted, numberOfEdgesAfterCompaction);
    }
}

void DrActiveVertex::CancelAllVersions(DrErrorPtr reason)
{
	m_cohort->GetGang()->CancelAllVersions(reason);
}

int DrActiveVertex::GetNumberOfReportedCompletions()
{
    return m_numberOfReportedCompletions;
}

DrString DrActiveVertex::GetDescription()
{
    DrString description;
    description.SetF("%d (%s)", GetId(), GetName().GetChars());
    return description;
}


DrStorageVertex::DrStorageVertex(DrStageManagerPtr stage, int partitionIndex,
                                 DrIInputPartitionReaderPtr reader) : DrVertex(stage)
{
    m_generator = DrNew DrStorageVertexOutputGenerator(partitionIndex, reader);
    m_registeredInputReady = false;
    m_partitionId = partitionIndex;
}

DrStorageVertex::DrStorageVertex(DrStageManagerPtr stage, DrStorageVertexOutputGeneratorPtr generator)
    : DrVertex(stage)
{
    m_generator = generator;
    m_partitionId = generator->GetPartitionIndex();
    m_registeredInputReady = false;
}

void DrStorageVertex::DiscardDerived()
{
    m_generator = DrNull;
}

DrVertexRef DrStorageVertex::MakeCopy(int suffix, DrStageManagerPtr stage)
{
    DrStorageVertexRef other = DrNew DrStorageVertex(stage, m_generator);
    other->InitializeFromOther(this, suffix);

    DrVertexRef vo = other;
    return vo;
}

DrVertexOutputGeneratorPtr DrStorageVertex::GetOutputGenerator(int /* unused edgeIndex */,
                                                               DrConnectorType type,
                                                               int /* unused version */)
{
    DrAssert(type == DCT_File);
    return m_generator;
}

DrString DrStorageVertex::GetURIForWrite(int port,
                                         int /* unused id */, int /* unused version */,
                                         int /* unused outputPort */,
                                         DrResourcePtr /* unused runningResource */,
                                         DrMetaDataRef /* unused metaData */)
{
    /* only output vertices can supply this */
    DrLogA("Storage vertex %d asked for write URI on port %d", m_id, port);
    return DrString();
}

void DrStorageVertex::KickStateMachine()
{
    /* nothing to do */
}

void DrStorageVertex::InitializeForGraphExecution()
{
    DrAssert(m_registeredInputReady == false);
    m_registeredInputReady = true;

    DrAffinityRef affinity = m_generator->GetOutputAffinity(0);
    m_stage->NotifyInputReady(this, affinity);
}

void DrStorageVertex::RemoveFromGraphExecution()
{
    /* nothing to do */
}

void DrStorageVertex::ReportFinalTopology(DrVertexTopologyReporterPtr reporter)
{
    reporter->ReportFinalTopology(this, DrNull, 0);
}

void DrStorageVertex::ReactToUpStreamRunningProcess(int inputPort,
                                                   DrConnectorType /* unused type */,
                                                   DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* for now a storage vertex shouldn't have any upstream neighbours */
    DrLogA("Storage vertex %d has upstream neighbor on port %d", m_id, inputPort);
}

void DrStorageVertex::ReactToUpStreamRunningVertex(int inputPort,
                                                   DrConnectorType /* unused type */,
                                                   DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* for now a storage vertex shouldn't have any upstream neighbours */
    DrLogA("Storage vertex %d has upstream neighbor on port %d", m_id, inputPort);
}

void DrStorageVertex::NotifyUpStreamCompletedVertex(int /*unused inputPort*/, DrConnectorType /* unused type */, DrVertexOutputGeneratorPtr /* unused generator */)
{
}

void DrStorageVertex::ReactToUpStreamCompletedVertex(int inputPort,
                                                     DrConnectorType /* unused type */,
                                                     DrVertexOutputGeneratorPtr /* unused generator */,
                                                     DrVertexExecutionStatisticsPtr /* unused stats */)
{
    /* for now a storage vertex shouldn't have any upstream neighbours */
    DrLogA("Storage vertex %d has upstream neighbor on port %d", m_id, inputPort);
}

void DrStorageVertex::ReactToDownStreamFailure(int /* unused port */,
                                               DrConnectorType /* unused type */,
                                               int /* unused downStreamVersion */)
{
    /* we don't care */
}

void DrStorageVertex::ReactToUpStreamFailure(int inputPort,
                                             DrConnectorType /* unused type */,
                                             DrVertexOutputGeneratorPtr /* unused generator */,
                                             int /* unused downStreamVersion */)
{
    /* for now a storage vertex shouldn't have any upstream neighbours */
    DrLogA("Storage vertex %d has upstream neighbor on port %d", m_id, inputPort);
}

void DrStorageVertex::ReactToFailedVertex(DrVertexOutputGeneratorPtr /* unused failedGenerator */,
                                          DrVertexVersionGeneratorPtr /* unused inputs */,
                                          DrVertexExecutionStatisticsPtr /* unused stats */,
                                          DrVertexProcessStatusPtr /* unused status */,
                                          DrErrorPtr originalReason)
{
    DrLogI("Failed vertex %s", originalReason->m_explanation.GetChars());
    m_stage->GetGraph()->ReportStorageFailure(this, originalReason);
}


void DrStorageVertex::CompactPendingVersion(DrEdgeHolderPtr /* unused edgesBeingCompacted*/, int /* unused numberOfEdgesAfterCompaction*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}

void DrStorageVertex::CancelAllVersions(DrErrorPtr /* unused reason*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}


bool DrOutputPartition::operator==(DrOutputPartitionR other)
{
    return
        (m_id == other.m_id &&
         m_version == other.m_version &&
         m_outputPort == other.m_outputPort &&
         m_resource == other.m_resource &&
         m_size == other.m_size);
}


DrOutputVertex::DrOutputVertex(DrStageManagerPtr stage, int partitionIndex,
                               DrIOutputPartitionGeneratorPtr generator) : DrVertex(stage)
{
    m_generator = generator;
    m_partitionIndex = partitionIndex;
    m_runningVersion = DrNew DrOutputPartitionList();
    m_successfulVersion = DrNew DrOutputPartitionList();
}

void DrOutputVertex::DiscardDerived()
{
    m_generator = DrNull;
    m_runningVersion = DrNull;
    m_successfulVersion = DrNull;
}

DrVertexRef DrOutputVertex::MakeCopy(int suffix, DrStageManagerPtr stage)
{
    DrOutputVertexRef other = DrNew DrOutputVertex(stage, suffix, m_generator);
    other->InitializeFromOther(this, suffix);

    m_generator->AddDynamicSplitVertex(this);

    DrVertexRef vo = other;
    return vo;
}

DrOutputPartition DrOutputVertex::FinalizeVersions(bool jobSuccess)
{
    /* abandon all but one successful version if the job succeeded, all versions otherwise */
    int i = (jobSuccess) ? 1 : 0;

    for (i=1; i<m_successfulVersion->Size(); ++i)
    {
        m_generator->AbandonVersion(m_partitionIndex,
                                    m_successfulVersion[i].m_id,
                                    m_successfulVersion[i].m_version,
                                    m_successfulVersion[i].m_outputPort,
                                    m_successfulVersion[i].m_resource,
                                    jobSuccess);
    }

    /* abandon all versions that are 'still running' */
    for (i=0; i<m_runningVersion->Size(); ++i)
    {
        m_generator->AbandonVersion(m_partitionIndex,
                                    m_runningVersion[i].m_id,
                                    m_runningVersion[i].m_version,
                                    m_runningVersion[i].m_outputPort,
                                    m_runningVersion[i].m_resource,
                                    jobSuccess);
    }

    if (jobSuccess)
    {
        return m_successfulVersion[0];
    }
    else
    {
        // it doesn't matter what we return here; it isn't used
        return DrOutputPartition();
    }
}

DrVertexOutputGeneratorPtr DrOutputVertex::GetOutputGenerator(int edgeIndex,
                                                              DrConnectorType /* unused type */,
                                                              int /* unused version */)
{
    /* we never run, and we're never upstream of anyone, so this shouldn't get called */
    DrLogA("Output generator called for output vertex %d port %d", m_id, edgeIndex);
    return DrNull;
}

DrString DrOutputVertex::GetURIForWrite(int port, int id, int version, int outputPort,
                                        DrResourcePtr runningResource, DrMetaDataRef metaData)
{
    DrLogI("Output vertex %d(%s) making URI for write %d %d %d %d %d",
           m_id, m_name.GetChars(), port, id, version, outputPort, m_partitionIndex);

    int i;
    for (i=0; i<m_runningVersion->Size(); ++i)
    {
        DrAssert(m_runningVersion[i].m_version != version);
    }
    for (i=0; i<m_successfulVersion->Size(); ++i)
    {
        DrAssert(m_successfulVersion[i].m_version != version);
    }

    DrOutputPartition p;
    p.m_id = id;
    p.m_version = version;
    p.m_outputPort = outputPort;
    p.m_resource = runningResource;
    p.m_size = 0;
    m_runningVersion->Add(p);

    return m_generator->GetURIForWrite(m_partitionIndex, id, version, port, runningResource, metaData);
}

void DrOutputVertex::KickStateMachine()
{
    /* nothing to do */
}

void DrOutputVertex::InitializeForGraphExecution()
{
    /* nothing to do */
}

void DrOutputVertex::RemoveFromGraphExecution()
{
    /* nothing to do */
}

void DrOutputVertex::ReportFinalTopology(DrVertexTopologyReporterPtr reporter)
{
    reporter->ReportFinalTopology(this, DrNull, 0);
}

void DrOutputVertex::ReactToUpStreamRunningProcess(int /* unused inputPort */,
                                                   DrConnectorType /* unused type */,
                                                   DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* nothing to do */
}

void DrOutputVertex::ReactToUpStreamRunningVertex(int /* unused inputPort */,
                                                  DrConnectorType /* unused type */,
                                                  DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* nothing to do */
}

void DrOutputVertex::NotifyUpStreamCompletedVertex(int /*unused inputPort*/, DrConnectorType /*unused type*/, DrVertexOutputGeneratorPtr /*unused generator*/)
{
}

void DrOutputVertex::ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                    DrVertexOutputGeneratorPtr generator,
                                                    DrVertexExecutionStatisticsPtr stats)
{
    DrEdge e = m_inputEdges->GetEdge(inputPort);
    DrAssert(e.m_type == type);

    int i;
    for (i=0; i<m_runningVersion->Size(); ++i)
    {
        DrOutputPartition p = m_runningVersion[i];
        if (p.m_version == generator->GetVersion())
        {
            p.m_size = stats->m_outputData[e.m_remotePort]->m_dataWritten;
            m_successfulVersion->Add(p);
            m_runningVersion->RemoveAt(i);
            return;
        }
    }
    DrLogA("Output vertex %d couldn't find matching running record for completion version %d",
           m_id, generator->GetVersion());
}

void DrOutputVertex::ReactToDownStreamFailure(int port,
                                              DrConnectorType /* unused type */,
                                              int /* unused downStreamVersion */)
{
    /* for now we shouldn't have any downstream neighbours */
    DrLogA("Output vertex %d has downstream neighbor on port %d", m_id, port);
}

void DrOutputVertex::ReactToUpStreamFailure(int /* unused port */,
                                            DrConnectorType /* unused type */,
                                            DrVertexOutputGeneratorPtr generator,
                                            int /* unused downStreamVersion */)
{
    int i;
    for (i=0; i<m_runningVersion->Size(); ++i)
    {
        if (m_runningVersion[i].m_version == generator->GetVersion())
        {
            /* discard the partition. jobSuccess=true here, because we don't
               know that the job has failed */
            m_generator->AbandonVersion(m_partitionIndex,
                                        m_runningVersion[i].m_id,
                                        m_runningVersion[i].m_version,
                                        m_runningVersion[i].m_outputPort,
                                        m_runningVersion[i].m_resource,
                                        true);

            m_runningVersion->RemoveAt(i);

            return;
        }
    }

    /* there's no particular reason to invalidate a successful version we've already got */
}

void DrOutputVertex::ReactToFailedVertex(DrVertexOutputGeneratorPtr /* unused failedGenerator */,
                                         DrVertexVersionGeneratorPtr /* unused inputs */,
                                         DrVertexExecutionStatisticsPtr /* unused stats */,
                                         DrVertexProcessStatusPtr /* unused status */,
                                         DrErrorPtr /* unused originalReason */)
{
    /* we should never appear to fail */
    DrLogA("Output vertex %d claiming to fail", m_id);
}


void DrOutputVertex::CompactPendingVersion(DrEdgeHolderPtr /* unused edgesBeingCompacted*/, int /* unused numberOfEdgesAfterCompaction*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}

void DrOutputVertex::CancelAllVersions(DrErrorPtr /* unused reason*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}


DrTeeVertex::DrTeeVertex(DrStageManagerPtr stage) : DrVertex(stage)
{
}

void DrTeeVertex::DiscardDerived()
{
    m_generator = DrNull;
}

DrVertexRef DrTeeVertex::MakeCopy(int suffix, DrStageManagerPtr stage)
{
    DrTeeVertexRef other = DrNew DrTeeVertex(stage);
    other->InitializeFromOther(this, suffix);

    DrVertexRef vo = other;
    return vo;
}

DrVertexOutputGeneratorPtr DrTeeVertex::GetOutputGenerator(int /* unused edgeIndex */,
                                                           DrConnectorType type,
                                                           int /* unused version */)
{
    DrAssert(type == DCT_File);
    return m_generator;
}

DrString DrTeeVertex::GetURIForWrite(int /* unused port */,
                                     int /* unused id */,
                                     int /* unused version */,
                                     int /* unused outputPort */,
                                     DrResourcePtr /* unused runningResource */,
                                     DrMetaDataRef /* unused metaData */)
{
    /* only output vertices can supply this */
    DrLogA("Tee vertex %d asked for write URI", m_id);
    return DrString();
}

void DrTeeVertex::KickStateMachine()
{
    /* nothing to do */
}

void DrTeeVertex::InitializeForGraphExecution()
{
    DrAssert(m_inputEdges->GetNumberOfEdges() == 1);
    DrEdge e = m_inputEdges->GetEdge(0);

    /* fill in the generator if it's already there */
    DrVertexOutputGeneratorRef generator =
        e.m_remoteVertex->GetOutputGenerator(e.m_remotePort, e.m_type, 0);
    if (generator != DrNull)
    {
        ReactToUpStreamCompletedVertex(0, e.m_type, generator, DrNull);
    }
}

void DrTeeVertex::RemoveFromGraphExecution()
{
    /* nothing to do */
}

void DrTeeVertex::ReportFinalTopology(DrVertexTopologyReporterPtr reporter)
{
    reporter->ReportFinalTopology(this, DrNull, 0);
}

void DrTeeVertex::ReactToUpStreamRunningProcess(int /* unused inputPort */,
                                                DrConnectorType /* unused type */,
                                                DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* we don't care */
}

void DrTeeVertex::ReactToUpStreamRunningVertex(int /* unused inputPort */,
                                               DrConnectorType /* unused type */,
                                               DrVertexOutputGeneratorPtr /* unused generator */)
{
    /* we don't care */
}

void DrTeeVertex::NotifyUpStreamCompletedVertex(int /* unused inputPort*/, DrConnectorType /* unused type*/, DrVertexOutputGeneratorPtr /* unused generator*/)
{
}

void DrTeeVertex::ReactToUpStreamCompletedVertex(int inputPort,
                                                 DrConnectorType /* unused type */,
                                                 DrVertexOutputGeneratorPtr generator,
                                                 DrVertexExecutionStatisticsPtr stats)
{
    /* we'll steal their generator to use as our own, but wrap it to return the value for
       inputPort 0 on all requests */
    DrAssert(inputPort == 0);
    m_generator = DrNew DrTeeVertexOutputGenerator(generator);

    int i;
    for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
    {
        DrEdge e = m_outputEdges->GetEdge(i);
        e.m_remoteVertex->NotifyUpStreamCompletedVertex(e.m_remotePort, e.m_type, m_generator);        
        e.m_remoteVertex->ReactToUpStreamCompletedVertex(e.m_remotePort, e.m_type, m_generator, stats);
    }
}

void DrTeeVertex::ReactToDownStreamFailure(int /* unused port */,
                                           DrConnectorType /* unused type */,
                                           int /* unused downStreamVersion */)
{
    /* we don't care */
}

void DrTeeVertex::ReactToUpStreamFailure(int port, DrConnectorType type,
                                         DrVertexOutputGeneratorPtr generator, int /* unused downStreamVersion */)
{
    DrAssert(port == 0);
    DrAssert(type == DCT_File);

    if (m_generator && (generator == m_generator->GetWrappedGenerator()))
    {
        DrVertexOutputGeneratorRef oldGenerator = m_generator;

        /* propagate the fact that this file is unreadable */
        m_generator = DrNull;

        int i;
        for (i=0; i<m_outputEdges->GetNumberOfEdges(); ++i)
        {
            DrEdge e = m_outputEdges->GetEdge(i);
            e.m_remoteVertex->ReactToUpStreamFailure(e.m_remotePort, e.m_type, oldGenerator, 0);
        }
    }
}


void DrTeeVertex::ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                      DrVertexVersionGeneratorPtr inputs,
                                      DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                      DrErrorPtr originalReason)
{
    DrAssert(failedGenerator != DrNull);
    DrAssert(inputs == DrNull);
    DrAssert(stats == DrNull);
    DrAssert(status == DrNull);

    /* the upstream vertex has called this because its read failed: propagate it down to the source */
    if (m_generator == failedGenerator)
    {
        m_generator = DrNull;
    }

    DrAssert(m_inputEdges->GetNumberOfEdges() == 1);

    DrEdge e = m_inputEdges->GetEdge(0);
    DrLogI("Tee vertex %d.%d: calling ReactToFailedVertex on remote edge", this->m_id, GetVersion());
    e.m_remoteVertex->ReactToFailedVertex(failedGenerator, DrNull, DrNull, DrNull, originalReason);

    /* fill in a new generator if it's already there, e.g. if the upstream vertex is a DrStorageVertex */
    DrVertexOutputGeneratorRef newGenerator =
        e.m_remoteVertex->GetOutputGenerator(e.m_remotePort, e.m_type, 0);
    if (newGenerator != DrNull)
    {
        /* don�t call ReactToUpStreamCompletedVertex here because we don�t actually want to start
           calling our downstream neighbors with ReactToUpStreamCompletedVertex just at the moment
           they called into us with ReactToFailedVertex. */
        m_generator = DrNew DrTeeVertexOutputGenerator(newGenerator);
    }
}


void DrTeeVertex::CompactPendingVersion(DrEdgeHolderPtr /* unused edgesBeingCompacted*/, int /* unused numberOfEdgesAfterCompaction*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}

void DrTeeVertex::CancelAllVersions(DrErrorPtr /* unused reason*/)
{
    //
    // Only DrActiveVertex can do this but when this method is called
    // we only have a DrVertex so this needs to be part of the
    // interface.
    //
}
