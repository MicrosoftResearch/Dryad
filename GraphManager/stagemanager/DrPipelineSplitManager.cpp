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

DrPipelineSplitManager::DrPipelineSplitManager() : DrConnectionManager(false)
{
    m_stageSet = DrNew DrStageSet();
    m_splitMap = DrNew DrVertexVListMap();
}

void DrPipelineSplitManager::AddUpstreamStage(DrManagerBasePtr stage)
{
    m_stageSet->Add(stage);
}

void DrPipelineSplitManager::
    NotifyUpstreamSplit(DrVertexPtr upstreamVertex,
                        DrVertexPtr baseNewVertexSplitFrom,
                        int outputPortOfSplitBase,
                        int upstreamSplitIndex)
{
    /* find the vertex in our stage that is connected to the upstream
       base that is splitting */
    DrEdge e = baseNewVertexSplitFrom->GetOutputs()->GetEdge(outputPortOfSplitBase);

    DrVertexPtr localBaseVertex = e.m_remoteVertex;
    int localBasePort = e.m_remotePort;
    DrConnectorType originalType = e.m_type;

    /* see if the local vertex has split before */
    DrVertexListRef splitList;
    if (m_splitMap->TryGetValue(localBaseVertex, splitList) == false)
    {
        splitList = DrNew DrVertexList();
        m_splitMap->Add(localBaseVertex, splitList);
    }

    /* this is going to be the new vertex in our stage */
    DrVertexPtr newVertex;

    /* either some other upstream stage has already split with this
       index number, or it's the next index to split */
    DrAssert(upstreamSplitIndex <= splitList->Size());
    if (upstreamSplitIndex == splitList->Size())
    {
        /* it's the next index to split, so we actually have to create
           a new vertex */
        newVertex = localBaseVertex->MakeCopy(upstreamSplitIndex);
        newVertex->GetInputs()->SetNumberOfEdges(localBaseVertex->GetInputs()->GetNumberOfEdges());

        /* register this new vertex with ourselves: this will attach
           up its output edges based on the policies of the vertices
           upstream of localBaseVertex, perhaps propagating the
           pipeline split forwards */
        GetParent()->RegisterVertexSplit(newVertex, localBaseVertex,
                                         upstreamSplitIndex);

        splitList->Add(newVertex);
    }
    else
    {
        /* some other upstream vertex already triggered the creation
           of this vertex */
        newVertex = splitList[upstreamSplitIndex];
    }

    DrAssert(newVertex != DrNull);

    if (upstreamVertex->GetOutputs()->GetNumberOfEdges() <= outputPortOfSplitBase)
    {
        upstreamVertex->GetOutputs()->GrowNumberOfEdges(outputPortOfSplitBase+1);
    }

    int nInputs = newVertex->GetInputs()->GetNumberOfEdges();
    DrAssert(localBasePort < nInputs);
    upstreamVertex->ConnectOutput(outputPortOfSplitBase,
                                  newVertex, localBasePort,
                                  originalType);

    int i;
    for (i=0; i<nInputs; ++i)
    {
        if (newVertex->GetInputs()->GetEdge(i).m_type == DCT_Tombstone)
        {
            /* this edge isn't connected to anyone yet */
            return;
        }
    }

    /* All the input edges are connected, so this vertex is ready to run */
    newVertex->InitializeForGraphExecution();
    newVertex->KickStateMachine();

    /* remove it from the split vector so we won't check it again in
       NotifyUpstreamLastVertexCompleted below */
    splitList[upstreamSplitIndex] = DrNull;
}

void DrPipelineSplitManager::
    NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                int outputPortOfRemovedVertex)
{
    /* find the vertex in our stage that is connected to the upstream
       base that is splitting */
    DrVertexRef localBaseVertex =
        upstreamVertex->RemoteOutputVertex(outputPortOfRemovedVertex);

    upstreamVertex->DisconnectOutput(outputPortOfRemovedVertex, true);

    /* shrink the local edge list to get rid of the empty slot we just
       left. The upstream vertex will be dealt with by its own stage
       manager */
    localBaseVertex->GetInputs()->Compact(localBaseVertex);

    /* if all the upstream edges have been removed, propagate the
       deletion forwards then remove the vertex */
    if (localBaseVertex->GetInputs()->GetNumberOfEdges() == 0)
    {
        GetParent()->UnRegisterVertex(localBaseVertex);
        DrAssert(localBaseVertex->GetOutputs()->GetNumberOfEdges() == 0);
        localBaseVertex->RemoveFromGraphExecution();
    }
}

void DrPipelineSplitManager::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    bool removed = m_stageSet->Remove(upstreamStage);
    DrAssert(removed == 1);

    if (m_stageSet->GetSize() == 0)
    {
        /* all our upstream stages have completed. Now go through and
           dispatch any split vertices that were still waiting for an
           edge */
        DrVertexVListMap::DrEnumerator e = m_splitMap->GetDrEnumerator();
        while (e.MoveNext())
        {
            DrVertexListRef list = e.GetValue();
            int i;
            for (i=0; i<list->Size(); ++i)
            {
                DrVertexPtr vertex = list[i];
                if (vertex != DrNull)
                {
                    int nInputs = vertex->GetInputs()->GetNumberOfEdges();
                    vertex->GetInputs()->Compact(vertex);
                    DrAssert(vertex->GetInputs()->GetNumberOfEdges() < nInputs);
                    vertex->InitializeForGraphExecution();
                    vertex->KickStateMachine();
                    list[i] = DrNull;
                }
            }
        }
    }
}



DrSemiPipelineSplitManager::DrSemiPipelineSplitManager() : DrConnectionManager(false)
{
    m_stageSet = DrNew DrStageSet();
    m_splitMap = DrNew DrVertexVListMap();
}

void DrSemiPipelineSplitManager::AddUpstreamStage(DrManagerBasePtr stage)
{
    m_stageSet->Add(stage);
}

void DrSemiPipelineSplitManager::
    NotifyUpstreamSplit(DrVertexPtr upstreamVertex,
                        DrVertexPtr baseNewVertexSplitFrom,
                        int outputPortOfSplitBase,
                        int upstreamSplitIndex)
{
    /* find the vertex in our stage that is connected to the upstream
       base that is splitting */
    DrEdge e = baseNewVertexSplitFrom->GetOutputs()->GetEdge(outputPortOfSplitBase);

    DrVertexPtr localBaseVertex = e.m_remoteVertex;
    int localBasePort = e.m_remotePort;
    DrConnectorType originalType = e.m_type;

    /* see if the local vertex has split before */
    DrVertexListRef splitList;
    if (m_splitMap->TryGetValue(localBaseVertex, splitList) == false)
    {
        splitList = DrNew DrVertexList();
        m_splitMap->Add(localBaseVertex, splitList);
    }

    /* this is going to be the new vertex in our stage */
    DrVertexPtr newVertex;

    /* either some other upstream stage has already split with this
       index number, or it's the next index to split */
    DrAssert(upstreamSplitIndex <= splitList->Size());
    if (upstreamSplitIndex == splitList->Size())
    {
        /* it's the next index to split, so we actually have to create
           a new vertex */
        newVertex = localBaseVertex->MakeCopy(upstreamSplitIndex);
        newVertex->GetInputs()->SetNumberOfEdges(localBaseVertex->GetInputs()->GetNumberOfEdges());

        /* register this new vertex with ourselves: this will attach
           up its output edges based on the policies of the vertices
           upstream of localBaseVertex, perhaps propagating the
           pipeline split forwards */
        GetParent()->RegisterVertexSplit(newVertex, localBaseVertex,
                                         upstreamSplitIndex);

        splitList->Add(newVertex);
    }
    else
    {
        /* some other upstream vertex already triggered the creation
           of this vertex */
        newVertex = splitList[upstreamSplitIndex];
    }

    DrAssert(newVertex != DrNull);

    if (upstreamVertex->GetOutputs()->GetNumberOfEdges() <= outputPortOfSplitBase)
    {
        upstreamVertex->GetOutputs()->GrowNumberOfEdges(outputPortOfSplitBase+1);
    }

    int nInputs = newVertex->GetInputs()->GetNumberOfEdges();
    DrAssert(localBasePort < nInputs);
    upstreamVertex->ConnectOutput(outputPortOfSplitBase,
                                  newVertex, localBasePort,
                                  originalType);

    int i;
    for (i=0; i<nInputs; ++i)
    {
        if (i == localBasePort) continue;

        DrEdge e = localBaseVertex->GetInputs()->GetEdge(i);
        DrVertexPtr source = e.m_remoteVertex;
        int outs = source->GetOutputs()->GetNumberOfEdges();
        source->GetOutputs()->GrowNumberOfEdges(outs + 1);
        source->ConnectOutput(outs, newVertex, i, originalType);
    }

    /* All the input edges are connected, so this vertex is ready to run */
    newVertex->InitializeForGraphExecution();
    newVertex->KickStateMachine();

    /* remove it from the split vector so we won't check it again in
       NotifyUpstreamLastVertexCompleted below */
    splitList[upstreamSplitIndex] = DrNull;
}

void DrSemiPipelineSplitManager::
    NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                int outputPortOfRemovedVertex)
{
    /* disconnect ALL the inputs to this vertex */
    DrVertexRef localBaseVertex = upstreamVertex->RemoteOutputVertex(outputPortOfRemovedVertex);
    int nInputs = localBaseVertex->GetInputs()->GetNumberOfEdges();

    int i;
    for (i=0; i<nInputs; ++i)
    {
        DrEdge e = localBaseVertex->GetInputs()->GetEdge(i);
        DrVertexPtr source = e.m_remoteVertex;
        source->DisconnectOutput(e.m_remotePort, true);

        if (source != upstreamVertex)
        {
            source->GetOutputs()->Compact(DrNull);
        }
    }

    /* shrink the local edge list to get rid of the empty slot we just
       left. The upstream vertex will be dealt with by its own stage
       manager */
    localBaseVertex->GetInputs()->Compact(localBaseVertex);

    DrAssert(localBaseVertex->GetInputs()->GetNumberOfEdges() == 0);

    GetParent()->UnRegisterVertex(localBaseVertex);

    DrAssert(localBaseVertex->GetOutputs()->GetNumberOfEdges() == 0);

    localBaseVertex->RemoveFromGraphExecution();
}

void DrSemiPipelineSplitManager::
    NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    bool removed = m_stageSet->Remove(upstreamStage);
    DrAssert(removed == 1);

    if (m_stageSet->GetSize() == 0)
    {
        /* all our upstream stages have completed. Now go through and
           dispatch any split vertices that were still waiting for an
           edge */
        DrVertexVListMap::DrEnumerator e = m_splitMap->GetDrEnumerator();
        while (e.MoveNext())
        {
            DrVertexListRef list = e.GetValue();
            int i;
            for (i=0; i<list->Size(); ++i)
            {
                DrVertexPtr vertex = list[i];
                if (vertex != DrNull)
                {
                    int nInputs = vertex->GetInputs()->GetNumberOfEdges();
                    vertex->GetInputs()->Compact(vertex);
                    DrAssert(vertex->GetInputs()->GetNumberOfEdges() < nInputs);
                    vertex->InitializeForGraphExecution();
                    vertex->KickStateMachine();
                    list[i] = DrNull;
                }
            }
        }
    }
}
