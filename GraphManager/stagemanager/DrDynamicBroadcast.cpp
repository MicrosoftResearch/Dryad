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

#include <math.h>

DrDynamicBroadcastManager::DrDynamicBroadcastManager(DrActiveVertexPtr copyVertex)
    : DrConnectionManager(false)
{
    m_copyVertex = copyVertex;
    m_teeNumber = 0;
}

void DrDynamicBroadcastManager::RegisterVertex(DrVertexPtr vertex, bool splitting)
{
    DrAssert(!splitting);

    if (m_baseTee == DrNull)
    {
        m_baseTee = dynamic_cast<DrTeeVertexPtr>(vertex);
        DrAssert(m_baseTee != DrNull);
    }
}

void DrDynamicBroadcastManager::MaybeMakeRoundRobinPodMachines()
// reorder the machines and return a list
{
    if (m_roundRobinMachines == DrNull)
    {
        m_roundRobinMachines = DrNew DrResourceList();

        /* we have to look down a long chain to find who's in the cluster, but it's there somewhere... */
        DrUniversePtr universe = m_copyVertex->GetStageManager()->GetGraph()->GetXCompute()->GetUniverse();

        {
            DrAutoCriticalSection acs(universe->GetResourceLock());

            DrResourceListRef pods = universe->GetResources(DRL_Rack);
            DrIntArrayListRef podIndex = DrNew DrIntArrayList();

            int i;
            for (i=0; i<pods->Size(); ++i)
            {
                podIndex->Add(0);
            }

            int podsToFinish = pods->Size();
            DrAssert(podsToFinish > 0);

            int currentPod = 0;
            do
            {
                int currentIndex = podIndex[currentPod];
                if (currentIndex == -1)
                {
                    /* we have already previously exhausted this pod, so just keep going */
                }
                else
                {
                    DrResourceListRef podChildren = pods[currentPod]->GetChildren();
                    if (currentIndex == podChildren->Size())
                    {
                        /* we have used all the machines from this pod */
                        DrAssert(podsToFinish > 0);
                        --podsToFinish;
                    }
                    else
                    {
                        m_roundRobinMachines->Add(podChildren[currentIndex]);
                        podIndex[currentPod] = currentIndex+1;
                    }
                }

                ++currentPod;
                if (currentPod == pods->Size())
                {
                    currentPod = 0;
                }
            }
            while (podsToFinish > 0);

            DrAssert(m_roundRobinMachines->Size() == universe->GetResources(DRL_Computer)->Size());
        }
    }
}


void DrDynamicBroadcastManager::NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                                              int /* unused executionVersion */,
                                                              DrResourcePtr machine,
                                                              DrVertexExecutionStatisticsPtr statistics)
// a node upstream of a tee has terminated, expand the tee into a tree
{
    DrEdge oe = vertex->GetOutputs()->GetEdge(outputPort);
    DrTeeVertexPtr sourceTee = dynamic_cast<DrTeeVertexPtr>((DrVertexPtr) oe.m_remoteVertex);
    DrAssert(sourceTee != DrNull);

    UINT64 dataWritten = statistics->m_outputData[0]->m_dataWritten;

    ExpandTee(sourceTee, dataWritten, machine);
}

void DrDynamicBroadcastManager::ExpandTee(DrTeeVertexPtr sourceTee, UINT64 dataWritten, DrResourcePtr machine)
{
    // how many nodes to expand this stage to
    int destinations = sourceTee->GetOutputs()->GetNumberOfEdges();
    if (destinations < s_minConsumers)
    {
        return;
    }

    int copies = (int)(sqrt((double)destinations)); 
    
    // find the pods lazily
    MaybeMakeRoundRobinPodMachines();

    int machines = m_roundRobinMachines->Size();
    DrAssert(machines > 0);

    // If there is only one machine don't ExpandTee
    if (machines == 1)
    {
        return;
    }

    if (copies > machines)
    {
        copies = machines;
    }

    int currentMachine;
    for (currentMachine=0; currentMachine<machines; ++currentMachine)
    {
        if (m_roundRobinMachines[currentMachine] == machine)
        {
            /* this is the machine the upstream vertex just ran on */
            break;
        }
    }
    DrAssert(currentMachine < machines);

    DrLogI("Inserting dynamic broadcast tree source: %d size: %d\n", sourceTee->GetId(), copies);

    int edgesPerNode = destinations / copies;
    int nodesWithExtraDestination = destinations % copies;

    DrTeeVertexRef newTee;
    DrVertexListRef newVertices = DrNew DrVertexList();

    int currentDestination = 0;
    int copy;
    // insert 'copies' broadcast nodes
    for (copy=0; copy<copies; ++copy)
    {
        DrVertexPtr downstream = DrNull;

        DrVertexRef t = m_baseTee->DrVertex::MakeCopy(m_teeNumber);
        DrTeeVertexPtr tee = dynamic_cast<DrTeeVertexPtr>((DrVertexPtr) t);
        tee->GetStageManager()->RegisterVertex(tee);

        tee->GetInputs()->SetNumberOfEdges(1);
        int edges = edgesPerNode + (copy < nodesWithExtraDestination);
        tee->GetOutputs()->SetNumberOfEdges(edges);

        if (copy == 0)
        {
            /* the first 'copy' is just another tee without a copier, since the data is already on this machine */
            downstream = tee;
            newTee = tee;
        }
        else
        {
            DrVertexRef v = m_copyVertex->DrVertex::MakeCopy(m_teeNumber);
            DrActiveVertexPtr newVertex = dynamic_cast<DrActiveVertexPtr>((DrVertexPtr) v);
            DrAssert(newVertex != DrNull);

            newVertex->GetStageManager()->RegisterVertex(newVertex);

            newVertex->GetInputs()->SetNumberOfEdges(1);
            newVertex->GetOutputs()->SetNumberOfEdges(1);

            /* make it prefer the new machine more than the one where the data lives */
            newVertex->GetAffinity()->AddLocality(m_roundRobinMachines[currentMachine]);
            newVertex->GetAffinity()->SetWeight(10 * dataWritten);

            newVertex->ConnectOutput(0, tee, 0, DCT_File);

            downstream = newVertex;
        }

        newVertices->Add(downstream);

        int i;
        for (i=0; i<edges; ++i, ++currentDestination)
        {
            DrAssert(currentDestination < destinations);

            DrEdge e = sourceTee->GetOutputs()->GetEdge(currentDestination);
            sourceTee->DisconnectOutput(currentDestination, true);

            tee->ConnectOutput(i, e.m_remoteVertex, e.m_remotePort, DCT_File);
        }

        sourceTee->ConnectOutput(copy, downstream, 0, DCT_File);

        ++m_teeNumber;
    }

    DrAssert(currentDestination == destinations);

    sourceTee->GetOutputs()->Compact(DrNull);

    /* kick all the copy vertices to start them going */
    int kick;
    for (kick=0; kick<newVertices->Size(); ++kick)
    {
        newVertices[kick]->InitializeForGraphExecution();
        newVertices[kick]->KickStateMachine();
    }

    /* now recurse down with the new tee we just created. Since there are a logarithmic number of levels,
       I'm not worried about exhausting the stack unless somebody builds a *really* big cluster */
    ExpandTee(newTee, dataWritten, machine);
}
