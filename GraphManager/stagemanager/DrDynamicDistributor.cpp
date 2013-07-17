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

/* dynamicdistributor.cpp:
   create dynamically a distribution layer
 */

#include <DrStageHeaders.h>

DrDynamicDistributionManager::DrDynamicDistributionManager(DrVertexPtr internalVertex,
                                                           DrConnectionManagerPtr newConnectionManager)
    : DrConnectionManager(false)
{
    SetDataPerVertex(s_dataPerVertex);
    m_combinedOutputSize = 0;
    m_internalVertex = internalVertex;
    m_newConnectionManager = newConnectionManager;

    m_stageSet = DrNew DrStageSet();
    m_sourcesSet = DrNew SourcesSet();
}

void DrDynamicDistributionManager::RegisterVertex(DrVertexPtr vertex, bool splitting)
{
    if (m_dstVertex)
    {
        // a newly created replica: nothing to do
        DrAssert(splitting);
    }
    else
    {
        m_dstVertex = vertex;
    }
}

void DrDynamicDistributionManager::SetDataPerVertex(UINT64 dataPerNode)
{
    DrAssert(dataPerNode > 0);
    m_dataPerVertex = dataPerNode;
}

UINT64 DrDynamicDistributionManager::GetDataPerVertex()
{
    return m_dataPerVertex;
}

void DrDynamicDistributionManager::AddUpstreamStage(DrManagerBasePtr upstreamStage)
{
    m_stageSet->Add(upstreamStage);
}

void DrDynamicDistributionManager::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    bool removed = m_stageSet->Remove(upstreamStage);
    DrAssert(removed);

    if (m_stageSet->GetSize() > 0)
    {
        return;
    }

    /* all our upstream stages have completed. */

    int copies = (int)((m_combinedOutputSize + m_dataPerVertex - 1) / m_dataPerVertex); 
    // how many nodes to expand this stage to
    if (copies == 0)
    {
        copies = 1;
    }

    DrVertexSetRef destCopies = DrNew DrVertexSet();
    DrVertexSetRef distrCopies = DrNew DrVertexSet();

    DrLogI("Resizing stage for dynamic distribution, new size: %d\n", copies);

    // resize this stage to 'copies' copies
    int copy;
    for (copy=0; copy<copies; ++copy)
    {
        DrVertexRef newVertex = m_dstVertex->MakeCopy(copy+1);
        destCopies->Add(newVertex);

        newVertex->GetInputs()->SetNumberOfEdges(m_dstVertex->GetInputs()->GetNumberOfEdges());

        /* split from one stage to another */
        newVertex->GetStageManager()->RegisterVertexSplit(newVertex, m_dstVertex, copy);
    }

    DrConnectorType edgeType = DCT_Tombstone;
    int distcopy = 0;
    SourcesSet::DrEnumerator e = m_sourcesSet->GetDrEnumerator();
    // disconnect the vertices in m_sourcesSet from m_dstVertex;
    while (e.MoveNext())
    {
        DrVertexPtr vertex = e.GetKey();
        int outputPort = e.GetValue();
        DrEdge edge = vertex->GetOutputs()->GetEdge(outputPort);

        if (edgeType == DCT_Tombstone)
        {
            edgeType = edge.m_type;
            /* for now we only deal with non-active edges */
            DrAssert(edgeType == DCT_File || edgeType == DCT_Output);
        }
        else
        {
            DrAssert(edgeType == edge.m_type);
        }

        int inputPort = edge.m_remotePort;
        DrVertexPtr inThisStage = edge.m_remoteVertex;

        DrAssert(inThisStage == m_dstVertex);
        inThisStage->DisconnectInput(inputPort, true);

        // create a new distributor vertex for the deleted edge
        DrVertexRef distributor = m_internalVertex->MakeCopy(distcopy);
        m_internalVertex->GetStageManager()->RegisterVertex(distributor);

        distributor->GetInputs()->SetNumberOfEdges(1);

        distributor->GetOutputs()->SetNumberOfEdges(copies);

        vertex->ConnectOutput(outputPort, distributor, 0, DCT_File);
        distrCopies->Add(distributor);

        ++distcopy;
    }

    distcopy = 0;
    // connect the cross-product from distributors to destCopies
    // and start the distributors
    DrVertexSet::DrEnumerator ve = distrCopies->GetDrEnumerator();
    while (ve.MoveNext())
    {
        DrVertexPtr distributor = ve.GetElement();

        int dest=0;
        DrVertexSet::DrEnumerator de = destCopies->GetDrEnumerator();
        while (de.MoveNext())
        {
            DrVertexPtr newVertex = de.GetElement();

            distributor->ConnectOutput(dest, newVertex, distcopy, edgeType);
            ++dest;
        }

        distributor->InitializeForGraphExecution();
        distributor->KickStateMachine();
        distcopy++;
    }

    if (m_newConnectionManager)
    {
        m_dstVertex->GetStageManager()->AddDynamicConnectionManagerAtRuntime(m_internalVertex->GetStageManager(),
                                                                             m_newConnectionManager);
    }

    m_dstVertex->GetInputs()->Compact(m_dstVertex);
    m_dstVertex->GetOutputs()->Compact(DrNull);
    DrAssert(m_dstVertex->GetInputs()->GetNumberOfEdges() == 0);
    GetParent()->UnRegisterVertex(m_dstVertex);// disconnects it
    DrAssert(m_dstVertex->GetOutputs()->GetNumberOfEdges() == 0);

    m_dstVertex->RemoveFromGraphExecution();

    m_internalVertex->GetStageManager()->SetStillAddingVertices(false);

    DrVertexSet::DrEnumerator startDest = destCopies->GetDrEnumerator();
    while (startDest.MoveNext())
    {
        DrVertexPtr newVertex = startDest.GetElement();
        newVertex->InitializeForGraphExecution();
        newVertex->KickStateMachine();
    }
}


void DrDynamicDistributionManager::NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                                                 int /* unused executionVersion */,
                                                                 DrResourcePtr /* unused machine */,
                                                                 DrVertexExecutionStatisticsPtr statistics)
{
    UINT64 outputSize = statistics->m_outputData[outputPort]->m_dataWritten;
    m_combinedOutputSize += outputSize;

    m_sourcesSet->Add(vertex, outputPort);
}

void DrDynamicDistributionManager::NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort,
                                                            DrAffinityPtr affinity)
{
    m_combinedOutputSize += affinity->GetWeight();
    m_sourcesSet->Add(vertex, outputPort);
}

/*******************************************************/
/* Alternative version of hash distributor starts here */
/* This distributor allocates extra edges, and redistributes just the edges at runtime */

DrDynamicHashDistributionManager::DrDynamicHashDistributionManager()
    : DrConnectionManager(false)
{
    SetDataPerVertex(s_dataPerVertex);
    m_combinedOutputSize = 0;
    m_edgesInBundle = 0;

    m_stageSet = DrNew DrStageSet();
    m_sources = DrNew DrVertexSet();
}

void DrDynamicHashDistributionManager::RegisterVertex(DrVertexPtr vertex, bool splitting)
{
    if (m_dstVertex)
    {
        // a newly created replica: nothing to do
        DrAssert(splitting);
    }
    else
    {
        m_dstVertex = vertex;
    }
}

void DrDynamicHashDistributionManager::SetDataPerVertex(UINT64 dataPerNode)
{
    DrAssert(dataPerNode);
    m_dataPerVertex = dataPerNode;
}

UINT64 DrDynamicHashDistributionManager::GetDataPerVertex()
{
    return m_dataPerVertex;
}

void DrDynamicHashDistributionManager::AddUpstreamStage(DrManagerBasePtr upstreamStage)
{
    m_stageSet->Add(upstreamStage);
}


void DrDynamicHashDistributionManager::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    bool removed = m_stageSet->Remove(upstreamStage);
    DrAssert(removed);
        
    if (m_stageSet->GetSize() > 0)
    {
        return;
    }

    /* all our upstream stages have completed. */

    int copies = (int)((m_combinedOutputSize + m_dataPerVertex - 1) / m_dataPerVertex); 

    // how many nodes to expand this stage to
    if (copies == 0)
    {
        copies = 1;
    }

    DrAssert(m_dstVertex);
    DrAssert(m_edgesInBundle);

    int numberOfInputs = m_dstVertex->GetInputs()->GetNumberOfEdges();
    DrAssert(numberOfInputs == m_edgesInBundle * m_sources->GetSize());

    // can't replicate more than the number of inputs
    if (m_edgesInBundle < copies)
    {
        copies = m_edgesInBundle;
    }

    DrLogI("Resizing stage for dynamic hash distribution, new size: %d\n", copies);

    int inputsPerVertex = m_edgesInBundle / copies;
    int verticesWithExtraInputs = m_edgesInBundle % copies;

    DrVertexListRef destCopies = DrNew DrVertexList();

    // resize this stage to 'copies' copies
    DrStageManagerPtr parent = m_dstVertex->GetStageManager();
    int copy;
    for (copy=0; copy<copies; ++copy)
    {
        DrVertexRef newVertex = m_dstVertex->MakeCopy(copy);
        destCopies->Add(newVertex);

        int numberOfInputs = inputsPerVertex + (copy < verticesWithExtraInputs);
        numberOfInputs = numberOfInputs * m_sources->GetSize();
        newVertex->GetInputs()->SetNumberOfEdges(numberOfInputs);
        parent->RegisterVertexSplit(newVertex, m_dstVertex, copy);
    }

    // disconnect the vertices in m_sources from m_dstVertex;
    // and connect them to the proper replica
    int sourceNo = 0;
    DrVertexSet::DrEnumerator ve = m_sources->GetDrEnumerator();
    while (ve.MoveNext())
    {
        // see if this upstream vertex has a correspondent in the new stage
        DrVertexPtr vertex = ve.GetElement();
        DrAssert(m_edgesInBundle == vertex->GetOutputs()->GetNumberOfEdges());

        int outputPort;
        for (outputPort=0; outputPort<m_edgesInBundle; ++outputPort)
        {
            DrEdge edge = vertex->GetOutputs()->GetEdge(outputPort);

            DrVertexPtr inThisStage = edge.m_remoteVertex;
            DrAssert(inThisStage == m_dstVertex);

            vertex->DisconnectOutput(outputPort, true);

            // connect to the proper replica
            int copyNo = outputPort % copies;
            int inputsPerSource = m_edgesInBundle / copies + (copyNo < verticesWithExtraInputs);
            int portNo = outputPort / copies + sourceNo * inputsPerSource;

            DrVertexPtr copy = destCopies[copyNo];

            vertex->ConnectOutput(outputPort, copy, portNo, edge.m_type);
        }

        ++sourceNo;
    }

    m_dstVertex->GetInputs()->Compact(m_dstVertex);
    DrAssert(m_dstVertex->GetInputs()->GetNumberOfEdges() == 0);

    m_dstVertex->GetOutputs()->Compact(DrNull);
    GetParent()->UnRegisterVertex(m_dstVertex); // disconnects it
    DrAssert(m_dstVertex->GetOutputs()->GetNumberOfEdges() == 0);

    m_dstVertex->RemoveFromGraphExecution();

    for (copy=0; copy<copies; ++copy)
    {
        destCopies[copy]->InitializeForGraphExecution();
        destCopies[copy]->KickStateMachine();
    }
}


void DrDynamicHashDistributionManager::NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                                                     int /* unused executionVersion */,
                                                                     DrResourcePtr /* unused machine */,
                                                                     DrVertexExecutionStatisticsPtr statistics)
{
    UINT64 outputSize = statistics->m_outputData[outputPort]->m_dataWritten;

    m_combinedOutputSize += outputSize;
    m_sources->Add(vertex);

    if (m_edgesInBundle)
    {
        DrAssert(m_edgesInBundle == vertex->GetOutputs()->GetNumberOfEdges());
    }
    else
    {
        m_edgesInBundle = vertex->GetOutputs()->GetNumberOfEdges();
    }
}


void DrDynamicHashDistributionManager::NotifyUpstreamInputReady(DrStorageVertexPtr /* ununsed vertex */,
                                                                int /* unused outputPort */,
                                                                DrAffinityPtr affinity)
{
    m_combinedOutputSize += affinity->GetWeight();
}
