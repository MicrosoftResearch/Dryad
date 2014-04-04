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

#include <DrStageHeaders.h>

DrStageManager::DrStageManager(DrGraphPtr graph) : DrSharedCritSec(graph)
{
}

DrStageManager::~DrStageManager()
{
}


DrConnectionManager::DrConnectionManager(bool manageVerticesIndividually)
{
    m_manageVerticesIndividually = manageVerticesIndividually;
}

DrConnectionManager::~DrConnectionManager()
{
}

bool DrConnectionManager::ManageVerticesIndividually()
{
    return m_manageVerticesIndividually;
}

void DrConnectionManager::SetParent(DrManagerBasePtr parent)
{
    m_parent = parent;
}

DrManagerBasePtr DrConnectionManager::GetParent()
{
    return m_parent;
}

void DrConnectionManager::AddUpstreamStage(DrManagerBasePtr /* unused upstreamStage */)
{
}

DrConnectionManagerRef DrConnectionManager::MakeManagerForVertex(DrVertexPtr /* unused vertex */,
                                                                 bool /* ununsed splitting */)
{
    return this;
}

void DrConnectionManager::RegisterVertex(DrVertexPtr /* unused vertex */,
                                         bool /* unused splitting */)
{
}

void DrConnectionManager::UnRegisterVertex(DrVertexPtr /* unused vertex */)
{
}

void DrConnectionManager::NotifyUpstreamSplit(DrVertexPtr upstreamVertex,
                                              DrVertexPtr baseNewVertexSplitFrom,
                                              int outputPortOfSplitBase,
                                              int /* unused upstreamSplitIndex */)
{
    /* the default behaviour is to add a new edge between
       upstreamVertex and localVertex. */
    DefaultDealWithUpstreamSplit(upstreamVertex, baseNewVertexSplitFrom,
                                 outputPortOfSplitBase, DCT_File);
}

void DrConnectionManager::NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                                      int outputPortOfRemovedVertex)
{
    /* the default behaviour is to remove the old edge between
       upstreamVertex and localVertex but not modify any
       vertices. */
    DefaultDealWithUpstreamRemoval(upstreamVertex,
                                   outputPortOfRemovedVertex);
}

void DrConnectionManager::NotifyUpstreamVertexCompleted(DrActiveVertexPtr /* unused vertex */,
                                                        int /* unused outputPort */,
                                                        int /* unused executionVersion */,
                                                        DrResourcePtr /* unused machine */,
                                                        DrVertexExecutionStatisticsPtr /* unused statistics */)
{
}

void DrConnectionManager::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr /* unused upstreamStage */)
{
}

void DrConnectionManager::NotifyUpstreamInputReady(DrStorageVertexPtr /* unused vertex */,
                                                   int /* unused outputPort */,
                                                   DrAffinityPtr /* unused affinity */)
{
}

void DrConnectionManager::DefaultDealWithUpstreamSplit(DrVertexPtr upstreamVertex,
                                                       DrVertexPtr baseNewVertexSplitFrom,
                                                       int outputPortOfSplitBase,
                                                       DrConnectorType type)
{
    DrVertexPtr localVertex = baseNewVertexSplitFrom->RemoteOutputVertex(outputPortOfSplitBase);

    int upstreamVertexBasePort = upstreamVertex->GetOutputs()->GetNumberOfEdges();
    int localVertexBasePort = localVertex->GetInputs()->GetNumberOfEdges();

    upstreamVertex->ConnectOutput(upstreamVertexBasePort,
                                  localVertex, localVertexBasePort, type);
}

void DrConnectionManager::DefaultDealWithUpstreamRemoval(DrVertexPtr upstreamVertex,
                                                         int outputPortOfRemovedVertex)
{
    DrVertexPtr localVertex = upstreamVertex->RemoteOutputVertex(outputPortOfRemovedVertex);
    upstreamVertex->DisconnectOutput(outputPortOfRemovedVertex, true);

    /* shrink the local edge list to get rid of the empty slot we just
       left. The upstream vertex will be dealt with by its own stage
       manager */
    localVertex->GetInputs()->Compact(localVertex);
}


bool RTIter::operator==(RTIterR other)
{
    return
        (m_version == other.m_version &&
         m_iter == other.m_iter);
}


DrManagerBase::Holder::Holder(DrConnectionManagerPtr manager)
{
    m_manager = manager;
    m_stageSet = DrNew DrStageSet();
}

DrConnectionManagerPtr DrManagerBase::Holder::GetConnectionManager()
{
    return m_manager;
}

void DrManagerBase::Holder::AddUpstreamStage(DrManagerBasePtr upstreamStage)
{
    m_stageSet->Add(upstreamStage);
    m_manager->AddUpstreamStage(upstreamStage);
}

bool DrManagerBase::Holder::IsManagingUpstreamStage(DrManagerBasePtr upstreamStage)
{
    return m_stageSet->Contains(upstreamStage);
}

DrConnectionManagerPtr DrManagerBase::Holder::GetManagerForVertex(DrVertexPtr /* unused vertex */)
{
    return m_manager;
}

void DrManagerBase::Holder::AddManagedVertex(DrVertexPtr vertex, bool splitting)
{
    m_manager->RegisterVertex(vertex, splitting);
}

void DrManagerBase::Holder::RemoveManagedVertex(DrVertexPtr vertex)
{
    m_manager->UnRegisterVertex(vertex);
}

void DrManagerBase::Holder::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    m_manager->NotifyUpstreamLastVertexCompleted(upstreamStage);
}

DrManagerBase::IndividualHolder::IndividualHolder(DrConnectionManagerPtr manager)
    : DrManagerBase::Holder(manager)
{
    DrAssert(manager->ManageVerticesIndividually());
    m_map = DrNew Map();
}

DrConnectionManagerPtr DrManagerBase::IndividualHolder::GetManagerForVertex(DrVertexPtr vertex)
{
    DrConnectionManagerRef manager;
    if (m_map->TryGetValue(vertex, manager))
    {
        return manager;
    }
    else
    {
        return DrNull;
    }
}

void DrManagerBase::IndividualHolder::AddManagedVertex(DrVertexPtr vertex, bool splitting)
{
    DrConnectionManagerRef manager = GetConnectionManager()->MakeManagerForVertex(vertex, splitting);
    m_map->Add(vertex, manager);
    manager->RegisterVertex(vertex, splitting);
}

void DrManagerBase::IndividualHolder::RemoveManagedVertex(DrVertexPtr vertex)
{
    DrConnectionManagerRef manager;
    if (m_map->TryGetValue(vertex, manager))
    {
        manager->UnRegisterVertex(vertex);
        m_map->Remove(vertex);
    }
}

void DrManagerBase::IndividualHolder::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    Map::DrEnumerator i = m_map->GetDrEnumerator();
    while (i.MoveNext())
    {
        i.GetValue()->NotifyUpstreamLastVertexCompleted(upstreamStage);
    }
}


DrManagerBase::DrManagerBase(DrGraphPtr graph, DrNativeString stageName) : DrStageManager(graph)
{
    m_graph = graph;
    m_graph->AddStage(this);

    m_includeInJobStageList = true;
    m_stillAddingVertices = false;
    m_verticesNotYetCompleted = 0;
    m_weHaveCompleted = false;
    m_stageStatistics = DrNew DrStageStatistics();
    m_downStreamStages = DrNew DrStageSet();

    m_vertices = DrNew DrVertexList();
    m_holder = DrNew HolderList();
    m_runningMap = DrNew RunningMap();
    m_runningTimeMap = DrNew RunningTimeMap();

    m_stageName = stageName;
    m_stageStatistics->SetName(m_stageName);
}

DrManagerBase::~DrManagerBase()
{
}

void DrManagerBase::Discard()
{
    if (m_runningMap != DrNull)
    {
        DrAssert(m_runningMap->GetSize() == 0);
        DrAssert(m_runningTimeMap->GetSize() == 0);
        
    }

    m_graph = DrNull;
    m_stageStatistics = DrNull;

    if (m_vertices != DrNull)
    {
        int i;
        for (i=0; i<m_vertices->Size(); ++i)
        {
            m_vertices[i]->Discard();
        }
    }
    m_vertices = DrNull;

    m_holder = DrNull;
    m_downStreamStages = DrNull;
    m_runningMap = DrNull;
    m_runningTimeMap = DrNull;
}

DrGraphPtr DrManagerBase::GetGraph()
{
    return m_graph;
}

void DrManagerBase::InitializeForGraphExecution()
{
    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        m_vertices[i]->InitializeForGraphExecution();
    }
}

void DrManagerBase::KickStateMachine()
{
    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        m_vertices[i]->KickStateMachine();
    }
}

DrString DrManagerBase::GetStageName()
{
    return m_stageName;
}

DrStageStatisticsPtr DrManagerBase::GetStageStatistics()
{
    return m_stageStatistics;
}

void DrManagerBase::SetStageStatistics(DrStageStatisticsPtr statistics)
{
    m_stageStatistics = statistics;
}

bool DrManagerBase::GetIncludeInJobStageList()
{
    return m_includeInJobStageList;
}

void DrManagerBase::SetIncludeInJobStageList(bool includeInJobStageList)
{
    m_includeInJobStageList = includeInJobStageList;
}

DrVertexListPtr DrManagerBase::GetVertexVector()
{
    return m_vertices;
}


DrManagerBase::HolderPtr DrManagerBase::AddDynamicConnectionManagerInternal(DrManagerBasePtr upstreamStage,
                                                                            DrConnectionManagerPtr manager)
{
    if (manager->GetParent() == DrNull)
    {
        manager->SetParent(this);
    }

    DrAssert(manager->GetParent() == this);

    int i;
    for (i=0; i<m_holder->Size(); ++i)
    {
        if (m_holder[i]->GetConnectionManager() == manager)
        {
            /* we've seen this connection manager before */
            break;
        }
    }

    if (i == m_holder->Size())
    {
        if (manager->ManageVerticesIndividually())
        {
            m_holder->Add(DrNew IndividualHolder(manager));
        }
        else
        {
            m_holder->Add(DrNew Holder(manager));
        }
    }

    m_holder[i]->AddUpstreamStage(upstreamStage);
    return m_holder[i];
}

void DrManagerBase::AddDynamicConnectionManager(DrStageManagerPtr upstreamStage,
                                                DrConnectionManagerPtr manager)
{
    AddDynamicConnectionManagerInternal(dynamic_cast<DrManagerBasePtr>(upstreamStage), manager);
}

void DrManagerBase::AddDynamicConnectionManagerAtRuntime(DrStageManagerPtr upstreamStage,
                                                         DrConnectionManagerPtr connector)
{
    HolderPtr b = AddDynamicConnectionManagerInternal(dynamic_cast<DrManagerBasePtr>(upstreamStage), connector);
    DrAssert(b != DrNull);

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        b->AddManagedVertex(m_vertices[i], false);
    }
}

DrManagerBase::HolderPtr DrManagerBase::LookUpConnectionHolder(DrManagerBasePtr upstreamStage)
{
    int i;
    for (i=0; i<m_holder->Size(); ++i)
    {
        if (m_holder[i]->IsManagingUpstreamStage(upstreamStage))
        {
            return m_holder[i];
        }
    }

    return DrNull;
}

DrConnectionManagerPtr DrManagerBase::LookUpConnectionManager(DrVertexPtr vertex,
                                                              DrManagerBasePtr upstreamStage)
{
    HolderPtr holder = LookUpConnectionHolder(upstreamStage);

    if (holder == DrNull)
    {
        /* there's no default connector, and no specific connector for
           this stage */
        return DrNull;
    }
    else
    {
        return holder->GetManagerForVertex(vertex);
    }
}

void DrManagerBase::RegisterVertex(DrVertexPtr vertex)
{
    RegisterVertexInternal(vertex, false);
}

void DrManagerBase::RegisterVertexInternal(DrVertexPtr vertex, bool registerSplit)
{
    DrAssert(m_weHaveCompleted == false);
    ++m_verticesNotYetCompleted;

    /* make sure all necessary connection managers are in place for
       this vertex */
    int b;
    for (b=0; b<m_holder->Size(); ++b)
    {
        m_holder[b]->AddManagedVertex(vertex, registerSplit);
    }

    m_vertices->Add(vertex);
    m_stageStatistics->IncrementSampleSize();

    /* pass this vertex on to any derived classes */
    RegisterVertexDerived(vertex);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::RegisterVertexDerived(DrVertexPtr /* unused vertex */)
{
}

void DrManagerBase::RegisterVertexSplit(DrVertexPtr vertex, DrVertexPtr baseToSplitFrom, int splitIndex)
{
    RegisterVertexInternal(vertex, true);

    /* tell all the downstream stage managers that we are splitting a
       vertex here */
    int numberOfOutputs = baseToSplitFrom->GetOutputs()->GetNumberOfEdges();
    int i;
    for (i=0; i<numberOfOutputs; ++i)
    {
        DrVertexPtr remote = baseToSplitFrom->RemoteOutputVertex(i);
        DrManagerBasePtr remoteManager = dynamic_cast<DrManagerBasePtr>(remote->GetStageManager());
        remoteManager->NotifyUpstreamSplit(vertex, baseToSplitFrom, i, splitIndex);
    }

    RegisterVertexSplitDerived(vertex, baseToSplitFrom, splitIndex);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::RegisterVertexSplitDerived(DrVertexPtr /* unused vertex */,
                                               DrVertexPtr /* unused baseToSplitFrom */,
                                               int /* unused splitIndex */)
{
}

void DrManagerBase::NotifyUpstreamSplit(DrVertexPtr upstreamVertex, DrVertexPtr baseNewVertexSplitFrom,
                                        int outputPortOfSplitBase, int upstreamSplitIndex)
{
    DrVertexPtr localVertex = baseNewVertexSplitFrom->RemoteOutputVertex(outputPortOfSplitBase);
    DrManagerBasePtr upstreamStage = dynamic_cast<DrManagerBasePtr>(baseNewVertexSplitFrom->GetStageManager());

    DrConnectionManagerPtr manager = LookUpConnectionManager(localVertex, upstreamStage);
    if (manager != DrNull)
    {
        /* if we have a connection manager for this upstream stage,
           tell it about the split */
        manager->NotifyUpstreamSplit(upstreamVertex, baseNewVertexSplitFrom,
                                     outputPortOfSplitBase, upstreamSplitIndex);
    }
    else
    {
        /* the default behaviour is to add a new edge between
           upstreamVertex and localVertex. The DrNull below says to use
           the graph's default channel constructor for the new edge */
        DrConnectionManager::DefaultDealWithUpstreamSplit(upstreamVertex, baseNewVertexSplitFrom,
                                                          outputPortOfSplitBase, DCT_File);
    }

    /* pass this split on to any derived classes */
    NotifyUpstreamSplitDerived(upstreamVertex, baseNewVertexSplitFrom,
                               outputPortOfSplitBase, upstreamSplitIndex);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyUpstreamSplitDerived(DrVertexPtr /* unused upstreamVertex */,
                                               DrVertexPtr /* unused baseNewVertexSplitFrom */,
                                               int /* unused outputPortOfSplitBase */,
                                               int /* unused upstreamSplitIndex */)
{
}

void DrManagerBase::SetStillAddingVertices(bool stillAddingVertices)
{
    DrAssert(m_weHaveCompleted == false);
    m_stillAddingVertices = stillAddingVertices;
    CheckIfWeHaveCompleted();
}

void DrManagerBase::CheckIfWeHaveCompleted()
{
    if (m_verticesNotYetCompleted == 0 && m_stillAddingVertices == false)
    {
        DrAssert(m_weHaveCompleted == false);
        m_weHaveCompleted = true;

        DrStageSet::DrEnumerator s = m_downStreamStages->GetDrEnumerator();
        /* tell all the downstream vertices that the last vertex in
           our stage has completed, so they can tell their connection
           managers */
        while (s.MoveNext())
        {
            s.GetElement()->NotifyUpstreamLastVertexCompleted(this);
        }

        NotifyLastVertexCompletedDerived();
    }
}

void DrManagerBase::UnRegisterVertex(DrVertexPtr vertex)
{
    DrAssert(m_verticesNotYetCompleted > 0);

    /* pass this removal on to any derived classes */
    UnRegisterVertexDerived(vertex);

    bool removed = m_vertices->Remove(vertex);
    DrAssert(removed);

    m_stageStatistics->DecrementSampleSize();

    /* tell the downstream stage managers that something is being removed */
    int numberOfOutputs = vertex->GetOutputs()->GetNumberOfEdges();
    int i;
    for (i=0; i<numberOfOutputs; ++i)
    {
        DrVertexPtr remote = vertex->RemoteOutputVertex(i);
        DrManagerBasePtr remoteManager = dynamic_cast<DrManagerBasePtr>(remote->GetStageManager());
        remoteManager->NotifyUpstreamVertexRemoval(vertex, i);
    }
    vertex->GetOutputs()->Compact(DrNull);
    DrAssert(vertex->GetOutputs()->GetNumberOfEdges() == 0);

    for (i=0; i<m_holder->Size(); ++i)
    {
        m_holder[i]->RemoveManagedVertex(vertex);
    }

    DrAssert(m_verticesNotYetCompleted > 0);
    --m_verticesNotYetCompleted;

    CheckIfWeHaveCompleted();
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::UnRegisterVertexDerived(DrVertexPtr /* unused vertex */)
{
}

void DrManagerBase::NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex, int outputPortOfRemovedVertex)
{
    DrVertexPtr localVertex = upstreamVertex->RemoteOutputVertex(outputPortOfRemovedVertex);
    DrManagerBasePtr upstreamStage = dynamic_cast<DrManagerBasePtr>(upstreamVertex->GetStageManager());

    DrConnectionManagerPtr manager = LookUpConnectionManager(localVertex, upstreamStage);
    if (manager != DrNull)
    {
        /* if we have a connection manager for this upstream stage,
           tell it about the split */
        manager->NotifyUpstreamVertexRemoval(upstreamVertex, outputPortOfRemovedVertex);
    }
    else
    {
        /* the default behaviour is to remove the old edge between
           upstreamVertex and localVertex but not modify any
           vertices. */
        DrConnectionManager::DefaultDealWithUpstreamRemoval(upstreamVertex, outputPortOfRemovedVertex);
    }

    /* pass this split on to any derived classes */
    NotifyUpstreamVertexRemovalDerived(upstreamVertex, outputPortOfRemovedVertex);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyUpstreamVertexRemovalDerived(DrVertexPtr /* unused upstreamVertex */,
                                                       int /* unused outputPortOfRemovedVertex */)
{
}

void DrManagerBase::AddToRunningMap(DrActiveVertexPtr vertex, int version, DrDateTime runningTime)
{
    VertexAndVersion vv;
    vv.m_vertex = vertex;
    vv.m_version = version;
    RunningTimeMap::Iter i = m_runningTimeMap->Insert(runningTime, vv);

    RTIterListRef list;
    if (m_runningMap->TryGetValue(vertex, list) == false)
    {
        list = DrNew RTIterList();
        m_runningMap->Add(vertex, list);
    }

    RTIter rt;
    rt.m_version = version;
    rt.m_iter = i;
    list->Add(rt);
}

void DrManagerBase::RemoveFromRunningMap(DrActiveVertexPtr vertex, int version)
{
    RTIterListRef list;
    if (m_runningMap->TryGetValue(vertex, list))
    {
        int i;
        for (i=0; i<list->Size(); ++i)
        {
            if (list[i].m_version == version)
            {
                DrLogI("Removing vertex from running map %d.%d (%s)",
                       vertex->GetId(), version, vertex->GetName().GetChars());
                m_runningTimeMap->Erase(list[i].m_iter);
                list->RemoveAt(i);

                if (list->Size() == 0)
                {
                    m_runningMap->Remove(vertex);
                }
                return;
            }
        }
    }
}

void DrManagerBase::CheckForDuplicates()
{
    CheckForDuplicatesDerived();

    DrTimeInterval threshold = m_stageStatistics->GetOutlierThreshold(m_graph->GetParameters());

    if (threshold == DrTimeInterval_Infinite || m_runningTimeMap->GetSize() == 0)
    {
        /* there's not enough data yet to set a threshold, or nothing
           running, so just leave */
        return;
    }

    DrLogI("Checking for duplicates Stage %s threshold %lf potential duplicate count %d",
		   GetStageName().GetChars(), (double) threshold / (double) DrTimeInterval_Second,
		   m_runningTimeMap->GetSize());

    DrDateTime now = m_graph->GetCluster()->GetCurrentTimeStamp();

    while (m_runningTimeMap->GetSize() > 0)
    {
        RunningTimeMap::Iter i = m_runningTimeMap->Begin();
        DrDateTime runningTime = i->first;
        DrAssert(runningTime <= now);

        if (runningTime + threshold > now)
        {
            DrLogI("Exiting with vertices still running Stage %s threshold %lf potential duplicate count %d",
				   GetStageName().GetChars(), (double) threshold / (double) DrTimeInterval_Second,
                   m_runningTimeMap->GetSize());
            return;
        }

        /* the oldest vertex has been running for longer than the
           outlier threshold, so take it off the list and try to start
           it as a duplicate */
        DrActiveVertexPtr v = i->second.m_vertex;
        int version = i->second.m_version;

        DrLogI("Considering vertex for duplication Stage %s threshold %lf vertex %d.%d (%s) running for %lf",
			   GetStageName().GetChars(), (double) threshold / (double) DrTimeInterval_Second,
               v->GetId(), version, v->GetName().GetChars(),
               (double) (now - runningTime) / (double) DrTimeInterval_Second);

        v->RequestDuplicate(version+1);

        int oldSize = m_runningTimeMap->GetSize();
        RemoveFromRunningMap(v, version);
        DrAssert(m_runningTimeMap->GetSize() + 1 == oldSize);
    }
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::CheckForDuplicatesDerived()
{
}

void DrManagerBase::NotifyVertexRunning(DrActiveVertexPtr vertex, int executionVersion,
                                        DrResourcePtr machine, DrVertexExecutionStatisticsPtr statistics)
{
    AddToRunningMap(vertex, executionVersion, statistics->m_runningTime);

    m_stageStatistics->IncrementStartedCount();

    NotifyVertexRunningDerived(vertex, executionVersion, machine, statistics);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyVertexRunningDerived(DrActiveVertexPtr /* unused vertex */,
                                               int /* unused executionVersion */,
                                               DrResourcePtr /* unused machine */,
                                               DrVertexExecutionStatisticsPtr /* unused statistics */)
{
}

void DrManagerBase::NotifyVertexStatus(DrActiveVertexPtr /* unused vertex */,
                                       HRESULT /* unused completionStatus */,
                                       DrVertexProcessStatusPtr /* unused status */)
{
}

void DrManagerBase::NotifyVertexCompleted(DrActiveVertexPtr vertex, int executionVersion,
                                          DrResourcePtr machine,
                                          DrVertexExecutionStatisticsPtr statistics)
{
    if (vertex->GetNumberOfReportedCompletions() == 0)
    {
        DrAssert(m_verticesNotYetCompleted > 0);
        --m_verticesNotYetCompleted;
    }

    RemoveFromRunningMap(vertex, executionVersion);

    /* machine is DrNull if this is a dummy vertex continuing after
       failure */
    if (machine != DrNull)
    {
        m_stageStatistics->AddMeasurement(m_graph->GetParameters(), machine, statistics);
    }

    /* we're keeping track of all the downstream stage managers so we
       know who to tell when all the inputs have notified. If there
       are lots of outputs, the common case will be that they all
       connect to the same stage manager, so special-case this to
       avoid hammering on the stl map */
    DrManagerBasePtr previousDownstreamStage = DrNull;

    /* make sure all necessary connection managers know about this
       vertex completion */
    int numberOfOutputs = vertex->GetOutputs()->GetNumberOfEdges();
    int i;
    for (i=0; i<numberOfOutputs; ++i)
    {
        DrVertexPtr remote = vertex->RemoteOutputVertex(i);
        DrManagerBasePtr remoteManager = dynamic_cast<DrManagerBasePtr>(remote->GetStageManager());
        remoteManager->NotifyUpstreamVertexCompleted(vertex, i, executionVersion,
                                                     machine, statistics);
        if (remoteManager != previousDownstreamStage)
        {
            if (m_downStreamStages->Contains(remoteManager) == false)
            {
                m_downStreamStages->Add(remoteManager);
            }
            previousDownstreamStage = remoteManager;
        }
    }

    /* pass this vertex on to any derived classes */
    NotifyVertexCompletedDerived(vertex, executionVersion, machine, statistics);

    if (vertex->GetNumberOfReportedCompletions() == 0)
    {
        CheckIfWeHaveCompleted();
    }
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyVertexCompletedDerived(DrActiveVertexPtr /* unused vertex */,
                                                 int /* unused executionVersion */,
                                                 DrResourcePtr /* unused machine */,
                                                 DrVertexExecutionStatisticsPtr /* unused statistics */)
{
}

void DrManagerBase::NotifyVertexFailed(DrActiveVertexPtr vertex, int executionVersion,
                                       DrResourcePtr machine, DrVertexExecutionStatisticsPtr statistics)
{
    if ((statistics != DrNull) && (statistics->m_runningTime != DrDateTime_Never))
    {
        RemoveFromRunningMap(vertex, executionVersion);

        m_stageStatistics->DecrementStartedCount();
    }

    NotifyVertexFailedDerived(vertex, executionVersion, machine, statistics);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyVertexFailedDerived(DrActiveVertexPtr /* unused vertex */,
                                              int /* unused executionVersion */,
                                              DrResourcePtr /* unused machine */,
                                              DrVertexExecutionStatisticsPtr /* unused statistics */)
{
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyLastVertexCompletedDerived()
{
}

void DrManagerBase::NotifyUpstreamVertexCompleted(DrActiveVertexPtr upstreamVertex,
                                                  int outputPortOfCompletedVertex,
                                                  int executionVersion,
                                                  DrResourcePtr machine,
                                                  DrVertexExecutionStatisticsPtr statistics)
{
    DrVertexPtr localVertex = upstreamVertex->RemoteOutputVertex(outputPortOfCompletedVertex);
    DrManagerBasePtr upstreamStage = dynamic_cast<DrManagerBasePtr>(upstreamVertex->GetStageManager());

    DrConnectionManagerPtr manager = LookUpConnectionManager(localVertex, upstreamStage);
    if (manager != DrNull)
    {
        /* if we have a connection manager for this upstream stage,
           tell it about the completion */
        manager->NotifyUpstreamVertexCompleted(upstreamVertex, outputPortOfCompletedVertex,
                                               executionVersion, machine, statistics);
    }

    /* pass this completion on to any derived classes */
    NotifyUpstreamVertexCompletedDerived(upstreamVertex, outputPortOfCompletedVertex,
                                         executionVersion, machine, statistics);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyUpstreamVertexCompletedDerived(DrActiveVertexPtr /* unused upstreamVertex */,
                                                         int /* unused outputPortOfCompletedVertex */,
                                                         int /* unused executionVersion */,
                                                         DrResourcePtr /* unused machine */,
                                                         DrVertexExecutionStatisticsPtr /* unused statistics */)
{
}

void DrManagerBase::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    HolderPtr holder = LookUpConnectionHolder(upstreamStage);
    if (holder != DrNull)
    {
        /* if we have a connection manager for this upstream stage,
           tell it about the completion */
        holder->NotifyUpstreamLastVertexCompleted(upstreamStage);
    }

    /* pass this on to any derived classes */
    NotifyUpstreamLastVertexCompletedDerived(upstreamStage);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyUpstreamLastVertexCompletedDerived(DrManagerBasePtr /* unused upstreamStage */)
{
}

void DrManagerBase::NotifyInputReady(DrStorageVertexPtr vertex, DrAffinityPtr affinity)
{
    DrAssert(m_verticesNotYetCompleted > 0);
    --m_verticesNotYetCompleted;

    /* we're keeping track of all the downstream stage managers so we
       know who to tell when all the inputs have notified. If there
       are lots of outputs, the common case will be that they all
       connect to the same stage manager, so special-case this to
       avoid hammering on the stl map */
    DrManagerBasePtr previousDownstreamStage = DrNull;

    /* make sure all necessary connection managers know about this
       input */
    int numberOfOutputs = vertex->GetOutputs()->GetNumberOfEdges();
    int i;
    for (i=0; i<numberOfOutputs; ++i)
    {
        DrVertexPtr remote = vertex->RemoteOutputVertex(i);
        DrManagerBasePtr remoteManager = dynamic_cast<DrManagerBasePtr>(remote->GetStageManager());
        remoteManager->NotifyUpstreamInputReady(vertex, i, affinity);
        if (remoteManager != previousDownstreamStage)
        {
            if (m_downStreamStages->Contains(remoteManager) == false)
            {
                m_downStreamStages->Add(remoteManager);
            }
            previousDownstreamStage = remoteManager;
        }
    }

    /* pass this vertex on to any derived classes */
    NotifyInputReadyDerived(vertex, affinity);

    CheckIfWeHaveCompleted();
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyInputReadyDerived(DrStorageVertexPtr /* unused vertex */,
                                            DrAffinityPtr /* unused affinity */)
{
}

void DrManagerBase::NotifyUpstreamInputReady(DrStorageVertexPtr upstreamVertex, int upstreamVertexOutputPort,
                                             DrAffinityPtr affinity)
{
    DrVertexPtr localVertex = upstreamVertex->RemoteOutputVertex(upstreamVertexOutputPort);
    DrManagerBasePtr upstreamStage = dynamic_cast<DrManagerBasePtr>(upstreamVertex->GetStageManager());

    DrConnectionManagerPtr manager = LookUpConnectionManager(localVertex, upstreamStage);
    if (manager != DrNull)
    {
        /* if we have a connection manager for this upstream stage,
           tell it about the completion */
        manager->NotifyUpstreamInputReady(upstreamVertex, upstreamVertexOutputPort, affinity);
    }

    /* pass this split on to any derived classes */
    NotifyUpstreamInputReadyDerived(upstreamVertex, upstreamVertexOutputPort, affinity);
}

/* this is a virtual method and the default does nothing */
void DrManagerBase::NotifyUpstreamInputReadyDerived(DrStorageVertexPtr /* unused upstreamVertex */,
                                                    int /* unused upstreamVertexOutputPort */,
                                                    DrAffinityPtr /* unused affinity */)
{
}

/* the is a virtual method and the default always returns true */
bool DrManagerBase::VertexIsReady(DrActiveVertexPtr vertex)
{
    return m_graph->IsRunning() && VertexIsReadyDerived(vertex);
}

/* the is a virtual method and the default always returns true */
bool DrManagerBase::VertexIsReadyDerived(DrActiveVertexPtr /* unused vertex */)
{
    return true;
}

void DrManagerBase::CancelAllVertices(DrErrorPtr reason)
{
	int i;

	for (i=0; i<m_vertices->Size(); ++i)
	{
		m_vertices[i]->CancelAllVersions(reason);
	}
}