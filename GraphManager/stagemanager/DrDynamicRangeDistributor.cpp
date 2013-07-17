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

DrDynamicRangeDistributionManager::DrDynamicRangeDistributionManager(DrStageManagerPtr dataConsumer,
                                                                     double samplingRate)
    : DrConnectionManager(false)
{
    SetDataPerVertex(s_dataPerVertex);
    m_combinedOutputSize = 0;
    m_dataConsumer = dataConsumer;
    DrAssert(m_dataConsumer != DrNull);

    m_samplingRate = samplingRate;
    DrAssert(samplingRate > 0 && samplingRate <= 1);

    m_stageSet = DrNew DrStageSet();
}

void DrDynamicRangeDistributionManager::SetDataPerVertex(UINT64 dataPerNode)
{
    DrAssert(dataPerNode);
    m_dataPerVertex = dataPerNode;
}

UINT64 DrDynamicRangeDistributionManager::GetDataPerVertex()
{
    return m_dataPerVertex;
}

void DrDynamicRangeDistributionManager::AddUpstreamStage(DrManagerBasePtr upstreamStage)
{
    m_stageSet->Add(upstreamStage);
}

void DrDynamicRangeDistributionManager::NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    bool removed = m_stageSet->Remove(upstreamStage);
    DrAssert(removed);

    if (m_stageSet->GetSize() > 0)
    {
        return;
    }

    /* all our upstream stages have completed. */

    int copies = (int)((m_combinedOutputSize/m_samplingRate + m_dataPerVertex - 1) / m_dataPerVertex); 
    // how many nodes to expand the M stage to

    DrLogI("Resizing stage for dynamic range distribution, new size: %d\n", copies);

    if (copies > 1)
    {
        DrVertexListRef consumers = m_dataConsumer->GetVertexVector();
        DrAssert(consumers->Size() == 1);

        DrVertexRef dataConsumerVertex = consumers[0];

        int ins = dataConsumerVertex->GetInputs()->GetNumberOfEdges();

        DrVertexListRef distributors = DrNew DrVertexList();      // keep track of the sources here

        int i;
        for (i=0; i<ins; ++i)
        {
            DrVertexPtr distributor = dataConsumerVertex->RemoteInputVertex(i);

            distributor->GetOutputs()->GrowNumberOfEdges(copies);
            distributors->Add(distributor);

            dataConsumerVertex->DisconnectInput(i, true);
        }

        int copy;
        for (copy=0; copy<copies; ++copy)
        {
            DrVertexRef newVertex = dataConsumerVertex->MakeCopy(copy);

            // connect all of the inputs of the dataconsumer to the copy
            newVertex->GetInputs()->SetNumberOfEdges(ins);

            for (i=0; i<ins; ++i)
            {
                DrVertexPtr distributor = distributors[i];
                distributor->ConnectOutput(copy, newVertex, i, DCT_File);
            }

            /* this connects up the outputs */
            m_dataConsumer->RegisterVertexSplit(newVertex, dataConsumerVertex, copy);

            newVertex->InitializeForGraphExecution();
            newVertex->KickStateMachine();
        }

        dataConsumerVertex->GetInputs()->Compact(dataConsumerVertex);
        DrAssert(dataConsumerVertex->GetInputs()->GetNumberOfEdges() == 0);

        m_dataConsumer->UnRegisterVertex(dataConsumerVertex);
        DrAssert(dataConsumerVertex->GetOutputs()->GetNumberOfEdges() == 0);

        dataConsumerVertex->RemoveFromGraphExecution();
    }

    DrString arg;
    arg.SetF("%d", copies);
    m_bucketizer->AddArgumentInternal(arg);
}

void DrDynamicRangeDistributionManager::RegisterVertex(DrVertexPtr vertex, bool splitting)
{
    DrAssert(!splitting);

    // there should be only one vertex in this stage
    DrAssert(m_bucketizer == DrNull);

    DrActiveVertexPtr activeVertex = dynamic_cast<DrActiveVertexPtr>(vertex);
    DrAssert(activeVertex != DrNull);

    m_bucketizer = activeVertex;
}

void DrDynamicRangeDistributionManager::NotifyUpstreamVertexCompleted(DrActiveVertexPtr /* unused vertex */,
                                                                      int outputPort,
                                                                      int /* unused executionVersion */,
                                                                      DrResourcePtr /* unused machine */,
                                                                      DrVertexExecutionStatisticsPtr statistics)
{
    UINT64 outputSize = statistics->m_outputData[outputPort]->m_dataWritten;
    m_combinedOutputSize += outputSize;
}

void DrDynamicRangeDistributionManager::NotifyUpstreamInputReady(DrStorageVertexPtr /* unused vertex */,
                                                                 int /* unused outputPort */,
                                                                 DrAffinityPtr affinity)
{
    m_combinedOutputSize += affinity->GetWeight();
}
