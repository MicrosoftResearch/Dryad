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

DrDamCompletedVertex::DrDamCompletedVertex(DrVertexPtr vertex, DrResourcePtr machine, UINT64 outputSize,
                                           int outputPort)
{
    m_vertex = vertex;
    m_outputPort = outputPort;

    m_affinity = DrNew DrAffinity();
    m_affinity->SetWeight(outputSize);
    m_affinity->GetLocalityArray()->Add(machine);

    m_numberOfLocations = 1;

    m_machine = DrNew DrResourceArray(1);
    m_group = DrNew DrDamVertexGroupArray(1);
    m_groupIndex = DrNew DrIntArray(1);

    m_machine[0] = machine;
    m_group[0] = DrNull;
    m_groupIndex[0] = -1;
}

DrDamCompletedVertex::DrDamCompletedVertex(DrVertexPtr /* ununsed vertex */,
                                           DrAffinityPtr affinity,
                                           int /* ununsed outputPort */)
{
    m_affinity = affinity;

    m_numberOfLocations = m_affinity->GetLocalityArray()->Size();
    if (m_numberOfLocations == 0)
    {
        m_numberOfLocations = 1;
    }

    m_machine = DrNew DrResourceArray(m_numberOfLocations);
    m_group = DrNew DrDamVertexGroupArray(m_numberOfLocations);
    m_groupIndex = DrNew DrIntArray(m_numberOfLocations);

    if (m_affinity->GetLocalityArray()->Size() == 0)
    {
        DrAssert(m_numberOfLocations == 1);
        m_machine[0] = DrNull;
        m_group[0] = DrNull;
        m_groupIndex[0] = -1;
    }
    else
    {
        int i;
        for (i=0; i<m_numberOfLocations; ++i)
        {
            m_machine[i] = m_affinity->GetLocalityArray()[i];
            m_group[i] = DrNull;
            m_groupIndex[i] = -1;
        }
    }
}

void DrDamCompletedVertex::RemovePodDuplicates()
{
    int newNumberOfLocations = 0;
    int i;
    for (i=0; i<m_numberOfLocations; ++i)
    {
        DrAssert(m_group[i] == DrNull);
        DrAssert(m_groupIndex[i] == -1);

        int j;
        for (j=0; j<newNumberOfLocations; ++j)
        {
            if (m_machine[j]->GetParent() == m_machine[i]->GetParent())
            {
                break;
            }
        }
        if (j == newNumberOfLocations)
        {
            m_machine[j] = m_machine[i];
            ++newNumberOfLocations;
        }
    }

    m_numberOfLocations = newNumberOfLocations;
}

DrDamCompletedVertexRef DrDamCompletedVertex::MakeCopy()
{
    return DrNew DrDamCompletedVertex(m_vertex, m_affinity, m_outputPort);
}

DrVertexPtr DrDamCompletedVertex::GetVertex()
{
    return m_vertex;
}

int DrDamCompletedVertex::GetOutputPort()
{
    return m_outputPort;
}

UINT64 DrDamCompletedVertex::GetOutputSize()
{
    return m_affinity->GetWeight();
}

DrResourceArrayRef DrDamCompletedVertex::GetMachineArray()
{
    return m_machine;
}

DrDamVertexGroupArrayRef DrDamCompletedVertex::GetGroupArray()
{
    return m_group;
}

DrIntArrayRef DrDamCompletedVertex::GetGroupIndexArray()
{
    return m_groupIndex;
}

int DrDamCompletedVertex::GetNumberOfLocations()
{
    return m_numberOfLocations;
}

void DrDamCompletedVertex::SetGroup(int locationIndex,
                                    DrDamVertexGroupPtr group,
                                    int groupIndex)
{
    DrAssert(locationIndex < m_numberOfLocations);
    m_group[locationIndex] = group;
    m_groupIndex[locationIndex] = groupIndex;
}

int DrDamCompletedVertex::GetLocationIndex(DrDamVertexGroupPtr group, int groupIndex)
{
    int locationIndex;

    for (locationIndex = m_numberOfLocations; locationIndex-- > 0; )
    {
        if ((m_group[locationIndex] == group) &&
            (m_groupIndex[locationIndex] == groupIndex))
        {
            return locationIndex;
        }
    }

    return MAX_INT32;
}

void DrDamCompletedVertex::MoveGroupIndex(DrDamVertexGroupPtr group,
                                          int newIndex, int oldIndex)
{
    int i;

    for (i=0; i<m_numberOfLocations; ++i)
    {
        if (group == m_group[i])
        {
            DrAssert(m_groupIndex[i] == oldIndex);
            m_groupIndex[i] = newIndex;
            return;
        }
    }

    DrLogA("Move group index didn't find a match");
}

void DrDamCompletedVertex::RemoveDuplicateVerticesFromGroups(DrDamVertexGroupPtr keepGroup)
{
    bool foundMatching = false;

    int i;
    for (i=0; i<m_numberOfLocations; ++i)
    {
        if (m_groupIndex[i] != -1)
        {
            if (keepGroup == m_group[i])
            {
                DrAssert(foundMatching == false);
                foundMatching = true;
            }
            else
            {
                m_group[i]->RemoveVertex(this, m_groupIndex[i]);
                m_group[i] = DrNull;
                m_groupIndex[i] = ((int) -1);
            }
        }
    }
}


DrDamVertexGroup::DrDamVertexGroup(int maxVerticesInGroup)
{
    m_maxVertices = maxVerticesInGroup;
    m_vertex = DrNew DrDamCompletedVertexList(s_initialArraySize);
    m_numberOfVertices = 0;
    m_combinedOutputSize = 0;
    m_maxNumberOfLocations = 0;
}

void DrDamVertexGroup::Discard()
{
    ClaimVertices();

    m_vertex = DrNew DrDamCompletedVertexList(s_initialArraySize);
    m_numberOfVertices = 0;
    m_combinedOutputSize = 0;
    m_maxNumberOfLocations = 0;
}

void DrDamVertexGroup::AddVertex(DrDamCompletedVertexPtr v,
                                 int locationIndex)
{
    DrAssert(m_numberOfVertices < m_maxVertices);

    m_vertex->Add(v);
    v->SetGroup(locationIndex, this, m_vertex->Size() - 1);

    ++m_numberOfVertices;

    m_combinedOutputSize += v->GetOutputSize();
    if (v->GetNumberOfLocations() > m_maxNumberOfLocations)
    {
        m_maxNumberOfLocations = v->GetNumberOfLocations();
    }

    if (m_vertex->Size() == m_vertex->Allocated())
    {
        Compact();
    }
}

void DrDamVertexGroup::RemoveVertex(DrDamCompletedVertexPtr v,
                                    int groupIndex)
{
    DrAssert(m_numberOfVertices > 0);
    DrAssert(groupIndex < m_vertex->Size());
    DrAssert(m_vertex[groupIndex] == v);
    DrAssert(m_combinedOutputSize >= v->GetOutputSize());
    m_combinedOutputSize -= v->GetOutputSize();
    m_vertex[groupIndex] = DrNull;
    --m_numberOfVertices;
    if (m_numberOfVertices == 0)
    {
        m_maxNumberOfLocations = 0;
    }
}

void DrDamVertexGroup::ClaimVertices()
{
    int i;
    for (i=0; i<m_vertex->Size(); ++i)
    {
        if (m_vertex[i] != DrNull)
        {
            m_vertex[i]->RemoveDuplicateVerticesFromGroups(this);
        }
    }
    m_maxNumberOfLocations = 1;
}

int DrDamVertexGroup::GetGroupSize()
{
    return m_numberOfVertices;
}

DrDamCompletedVertexListPtr DrDamVertexGroup::GetGroupArray()
{
    return m_vertex;
}

UINT64 DrDamVertexGroup::GetCombinedOutputSize()
{
    return m_combinedOutputSize;
}

int DrDamVertexGroup::GetMaxNumberOfLocations()
{
    return m_maxNumberOfLocations;
}

void DrDamVertexGroup::Compact()
{
    int newSlotsUsed = 0;
    int i;
    for (i=0; i<m_vertex->Size(); ++i)
    {
        if (m_vertex[i] != DrNull)
        {
            if (i > newSlotsUsed)
            {
                m_vertex[newSlotsUsed] = m_vertex[i];
                m_vertex[i] = DrNull;
                m_vertex[newSlotsUsed]->MoveGroupIndex(this, newSlotsUsed, i);
            }
            ++newSlotsUsed;
        }
    }

    DrAssert(newSlotsUsed == m_numberOfVertices);
    while (m_vertex->Size() > m_numberOfVertices)
    {
        DrAssert(m_vertex[m_vertex->Size() - 1] == DrNull);
        m_vertex->RemoveAt(m_vertex->Size() - 1);
    }
}

void DrDamVertexGroup::DisconnectFromSuccessor(DrVertexPtr successor)
{
    DrAssert(m_numberOfVertices > 0);

    int i;
    for (i=0; i<m_vertex->Size(); ++i)
    {
        if (m_vertex[i] != DrNull)
        {
            DrVertexPtr base = m_vertex[i]->GetVertex();
            int basePort = m_vertex[i]->GetOutputPort();

            DrEdge edge = base->GetOutputs()->GetEdge(basePort);
            DrAssert(edge.m_remoteVertex == successor);
            int successorPort = edge.m_remotePort;
            successor->DisconnectInput(successorPort, false);
        }
    }

    /* leave the removed edges as DrNull in the successor to avoid doing
       n^2 operations by compacting every time we remove a group. As
       long as we compact out the DrNull edges by the time the successor
       is ready to execute, things will be fine */
}

void DrDamVertexGroup::ConnectToSuccessor(DrVertexPtr successor)
{
    DrAssert(m_numberOfVertices > 0);
    successor->GetInputs()->SetNumberOfEdges(m_numberOfVertices);

    int i;
    int usedBases = 0;
    for (i=0; i<m_vertex->Size(); ++i)
    {
        if (m_vertex[i] != DrNull)
        {
            DrVertexPtr base = m_vertex[i]->GetVertex();
            int basePort = m_vertex[i]->GetOutputPort();

            DrEdge e = base->GetOutputs()->GetEdge(basePort);
            DrEdge re;
            re.m_remoteVertex = base;
            re.m_remotePort = basePort;
            re.m_type = e.m_type;
            successor->GetInputs()->SetEdge(usedBases, re);

            e.m_remoteVertex = successor;
            e.m_remotePort = usedBases;
            base->GetOutputs()->SetEdge(basePort, e);

            ++usedBases;

            m_vertex[i] = DrNull;
        }
    }

    DrAssert(usedBases == m_numberOfVertices);

    m_numberOfVertices = 0;
    m_vertex = DrNew DrDamCompletedVertexList();
    m_combinedOutputSize = 0;
    m_maxNumberOfLocations = 0;
}

DrDamPartiallyGroupedLayer::
    DrDamPartiallyGroupedLayer(DrGraphPtr graph, DrStageStatisticsPtr statistics,
                               DrDynamicAggregateManagerPtr parent,
                               DrVertexPtr internalVertex,
                               int aggregationLevel, DrString name)
{
    m_name = name;
    m_parent = parent;
    m_aggregationLevel = aggregationLevel;
    m_numberOfInternalCreated = 0;
    m_delayGrouping = false;

    if (m_aggregationLevel > 0)
    {
        DrAssert(internalVertex != DrNull);

        m_stageManager = DrNew DrManagerBase(graph, name.GetString());
        m_internalVertex = internalVertex;

        /* all the sub-managers should share the same statistics as
           the parent internal vertex's manager, since they are all
           running vertices of the same class */
        m_stageManager->SetStageStatistics(statistics);

        m_stageManager->SetStillAddingVertices(true);
    }
    else
    {
        DrAssert(internalVertex == DrNull);
    }

    m_machineGroup = DrNew GroupMap();
    m_podGroup = DrNew GroupMap();
}

void DrDamPartiallyGroupedLayer::Discard()
{
    GroupMap::DrEnumerator e = m_machineGroup->GetDrEnumerator();
    while (e.MoveNext())
    {
        e.GetValue()->Discard();
    }

    e = m_podGroup->GetDrEnumerator();
    while (e.MoveNext())
    {
        e.GetValue()->Discard();
    }

    if (m_overallGroup != DrNull)
    {
        m_overallGroup->Discard();
    }
}

void DrDamPartiallyGroupedLayer::SetDelayGrouping(bool delayGrouping)
{
    m_delayGrouping = delayGrouping;
}

DrString DrDamPartiallyGroupedLayer::GetName()
{
    return m_name;
}

DrDynamicAggregateManagerPtr DrDamPartiallyGroupedLayer::GetParent()
{
    return m_parent;
}

DrManagerBasePtr DrDamPartiallyGroupedLayer::GetStageManager()
{
    return m_stageManager;
}

void DrDamPartiallyGroupedLayer::
    ConsiderSending(DrDamVertexGroupPtr group,
                    int maxVertices, UINT64 additionalSize)
{
    if (m_delayGrouping)
    {
        return;
    }

    /* if adding this new vertex to group would cause it to
       overflow, send the group off and create a new empty one to
       put this vertex in */
    if (group->GetGroupSize() == maxVertices ||
        (group->GetCombinedOutputSize() + additionalSize) >
        GetParent()->GetMaxDataPerGroup())
    {
        group->ClaimVertices();
        GetParent()->AcceptCompletedGroup(group, m_aggregationLevel);
    }

    return;
}


// This function combines all input vertices assinged to the group into
// processing vertices by verifying grouping conditions.
//
// Input vertices which are left after such grouping still left in the group,
// so further steps can handle them appropriately.
//
// When nothing left, it destorys a group and returns DrNull, otherwise it
// returns a pointer to a group with unprocessed input vertices.

DrDamVertexGroupPtr DrDamPartiallyGroupedLayer::SendMinimum(DrDamVertexGroupPtr group,
                                                            int minVertices, int maxVertices)
{
    // If source group is DrNull, return it as is -- nothing to do
    if (group == DrNull)
    {
        return group;
    }

    int vertexIdx = 0;
    DrDamVertexGroupRef tmp_group = DrNull;
    DrDamCompletedVertexListRef vertex_array = group->GetGroupArray();

    // Move input vertices from group to tmp_group while it have at least
    // minimum number of vertices OR data size of a vertex is not less than
    // required minimum amount (minimum threshold).
    while (group->GetGroupSize() >= minVertices ||
           group->GetCombinedOutputSize() >= GetParent()->GetMinDataPerGroup())
    {
        // Allocate temporary group if not done before
        if (tmp_group == DrNull)
        {
            tmp_group = DrNew DrDamVertexGroup(maxVertices);
        }

        // Move up to maximum limit of inputs or maximum limit of data per
        // single processing vertex
        for (; group->GetGroupSize() > 0; ++vertexIdx)
        {
            DrDamCompletedVertexRef v = vertex_array[vertexIdx];
            if (v == DrNull)
            {
                continue;
            }

            // If group goes out maximum threshold, create new vertex immediately
            if ((tmp_group->GetGroupSize() == maxVertices) ||
                ((tmp_group->GetCombinedOutputSize() + v->GetOutputSize()) > GetParent()->GetMaxDataPerGroup()))
            {
                break;
            }

            int locationIdx = v->GetLocationIndex(group, vertexIdx);
            if (v->GetNumberOfLocations() > 1)
            {
                v->RemoveDuplicateVerticesFromGroups(DrNull);
            }
            else
            {
                group->RemoveVertex(v, vertexIdx);
            }
            tmp_group->AddVertex(v, locationIdx);
        }

        // Generate processing vertex and clean temporary group
        GetParent()->AcceptCompletedGroup(tmp_group, m_aggregationLevel);
    }

    // If we do not have any vertices in source group -- destroy it too
    if (group->GetGroupSize() == 0)
    {
        group = DrNull;
    }

    return group;
}

void DrDamPartiallyGroupedLayer::ReturnUnGrouped(DrDamVertexGroupPtr group,
                                                 DrDamGroupingLevel level)
{
    if (group != DrNull)
    {
        int groupSize = group->GetGroupSize();

        group->ClaimVertices();
        DrDamCompletedVertexListRef groupArray = group->GetGroupArray();
        int nSlots = groupArray->Size();

        int moved = 0;
        int i;
        for (i=0; i<nSlots; ++i)
        {
            if (groupArray[i] != DrNull)
            {
                DrDamCompletedVertexRef cv = groupArray[i];
                cv->RemoveDuplicateVerticesFromGroups(DrNull);

                switch (level)
                {
                case DDGL_Machine:
                    if (GetParent()->GetMaxPerPod() > 0)
                    {
                        AddVertexToPodGroup(cv);
                        break;
                    }

                case DDGL_Pod:
                    if (GetParent()->GetMaxOverall() > 0)
                    {
                        AddVertexToOverallGroup(cv);
                        break;
                    }

                default:
                    /* we aren't going to group it at this layer so
                       send it back to the parent to add to the next
                       aggregation layer */
                    GetParent()->ReturnUnGrouped(cv, m_aggregationLevel);
                }
                ++moved;

                groupArray[i] = DrNull;
            }
        }

        DrAssert(moved == groupSize);
    }
}

void DrDamPartiallyGroupedLayer::AddVertexToMachineGroup(DrDamCompletedVertexPtr vertex)
{
    int maxVertices = GetParent()->GetMaxPerMachine();

    if (m_delayGrouping)
    {
        maxVertices = MAX_INT32;
    }

    int nLocations = vertex->GetNumberOfLocations();
    DrResourceArrayRef machine = vertex->GetMachineArray();
    int i;
    for (i=0; i<nLocations; ++i)
    {
        DrDamVertexGroupRef group;

        if (m_machineGroup->TryGetValue(machine[i], group) == false)
        {
            group = DrNew DrDamVertexGroup(maxVertices);
            m_machineGroup->Add(machine[i], group);
        }

        ConsiderSending(group, maxVertices, vertex->GetOutputSize());
        group->AddVertex(vertex, i);
    }
}

void DrDamPartiallyGroupedLayer::AddVertexToPodGroup(DrDamCompletedVertexPtr vertex)
{
    int maxVertices = GetParent()->GetMaxPerPod();

    if (m_delayGrouping)
    {
        maxVertices = MAX_INT32;
    }

    vertex->RemovePodDuplicates();

    int nLocations = vertex->GetNumberOfLocations();
    DrResourceArrayRef machine = vertex->GetMachineArray();
    int i;
    for (i=0; i<nLocations; ++i)
    {
        DrResourcePtr podPtr;
        if (machine[i] == DrNull)
        {
            podPtr = DrNull;
        }
        else
        {
            podPtr = machine[i]->GetParent();
        }

        DrDamVertexGroupRef group;
        if (m_podGroup->TryGetValue(podPtr, group) == false)
        {
            group = DrNew DrDamVertexGroup(maxVertices);
            m_podGroup->Add(podPtr, group);
        }

        ConsiderSending(group, maxVertices, vertex->GetOutputSize());
        group->AddVertex(vertex, i);
    }
}

void DrDamPartiallyGroupedLayer::
    AddVertexToOverallGroup(DrDamCompletedVertexPtr vertex)
{
    int maxVertices = GetParent()->GetMaxOverall();

    if (m_delayGrouping)
    {
        maxVertices = MAX_INT32;
    }

    if (m_overallGroup == DrNull)
    {
        m_overallGroup = DrNew DrDamVertexGroup(maxVertices);
    }

    ConsiderSending(m_overallGroup, maxVertices, vertex->GetOutputSize());
    m_overallGroup->AddVertex(vertex, 0);
}

// Moves one "topmost" vertex from source group to destination one.
bool DrDamPartiallyGroupedLayer::MoveOneVertex(MachineGroupR grpStruct)
{
    DrDamVertexGroupPtr srcGroup = grpStruct.group;
    DrDamVertexGroupPtr dstGroup = grpStruct.outputGroup;
    DrDamCompletedVertexListRef vertices = srcGroup->GetGroupArray();
    int numOfSlots = vertices->Size();

    int vertexIdx;
    for(vertexIdx = grpStruct.nextVertex; vertexIdx < numOfSlots; ++vertexIdx)
    {
        if (vertices[vertexIdx] == DrNull)
        {
            continue;
        }

        DrDamCompletedVertexPtr v = vertices[vertexIdx];

        int locationIdx = v->GetLocationIndex(srcGroup, vertexIdx);
        if (v->GetNumberOfLocations() > 1)
        {
            v->RemoveDuplicateVerticesFromGroups(DrNull);
        }
        else
        {
            srcGroup->RemoveVertex(v, vertexIdx);
        }
        dstGroup->AddVertex(v, locationIdx);
        ++vertexIdx;
        break;
    }

    // If source group becomes empty -- destroy it, otherwise remember next
    // vertex index we need to look at.
    if (srcGroup->GetGroupSize() == 0)
    {
        grpStruct.group = DrNull;
        return true;
    }
    else
    {
        grpStruct.nextVertex = vertexIdx;
    }

    return false;
}

// Compares two group of machines to follow defined ordering
//
// For small clusters (or large datasets, when number of instances is much more
// than number of machines where those instances are distributed), we need to
// take care about amount of data allocated to particular machine.
//
// TODO: Weights for particular data distributions should be different. We need
// to figure out almost optimal algorithm, which is lightweight and suitable for
// most cases.
//
// TODO: We need to figure out a way when and how we need to switch between
// different lightweight algorithms for individual cases depending on how
// much data we have and how this data is distributed across machines.
int DrDamPartiallyGroupedLayer::MachineGroupCmp(MachineGroupR left, MachineGroupR right)
{
    UINT64 leftSize, rightSize;
    UINT64 leftOutput, rightOutput;

    if (right.group == DrNull)
    {
        if (left.group == DrNull)
        {
            return 0;
        }
        return -1;
    }

    if (left.group == DrNull)
    {
        return 1;
    }

    // Do not try to assign anything to unknown machines
    if (right.machine == DrNull)
    {
        return -1;
    }

    if (left.machine == DrNull)
    {
        return 1;
    }

    leftOutput = left.outputGroup->GetCombinedOutputSize();
    rightOutput = right.outputGroup->GetCombinedOutputSize();

    if (leftOutput < rightOutput)
    {
        return -1;
    }

    if (leftOutput > rightOutput)
    {
        return 1;
    }

    leftSize = left.group->GetCombinedOutputSize();
    rightSize = right.group->GetCombinedOutputSize();

    if (leftSize < rightSize)
    {
        return -1;
    }

    if (leftSize > rightSize)
    {
        return 1;
    }

    return 0;
}

void DrDamPartiallyGroupedLayer::GatherRemainingMachineGroups()
{
    /* first pull out any groups which are big enough. Since removing
       a group can cull machines out of a subsequent group (if a
       machine has multiple locations), do this full pass before
       gathering up the remainders below */
    if (m_delayGrouping)
    {
        MachineGroupArrayRef weightedMap = DrNew MachineGroupArray(m_machineGroup->GetSize());

        GroupMapRef newMap = DrNew GroupMap();

        int numMachines = 0;
        GroupMap::DrEnumerator m = m_machineGroup->GetDrEnumerator();
        while (m.MoveNext())
        {
            if (m.GetValue()->GetMaxNumberOfLocations() > 1)
            {
                DrDamVertexGroupRef newGroup = DrNew DrDamVertexGroup(m.GetValue()->GetGroupSize());

                weightedMap[numMachines].machine = m.GetKey();
                weightedMap[numMachines].group = m.GetValue();
                weightedMap[numMachines].outputGroup = newGroup;
                weightedMap[numMachines].nextVertex = 0;

                newMap->Add(m.GetKey(), newGroup);

                ++numMachines;
            }
            else
            {
                newMap->Add(m.GetKey(), m.GetValue());
            }
        }

        m_machineGroup = newMap;

        // If we have machines with multi-instance vertices, process them to produce almost flat distribution
        if (numMachines > 0)
        {
            for(;;)
            {
                // Find top-most machine by MachineGroupCmp() sorting condition
                MachineGroup topMachine = weightedMap[0];
                int i;
                for(i=1; i<numMachines; ++i)
                {
                    if (MachineGroupCmp(topMachine, weightedMap[i]) > 0)
                    {
                        topMachine = weightedMap[i];
                    }
                }

                // If top-most machine does not have associated source group, this signals that we
                // already allocated all inputs, nothing more to do.
                if (topMachine.group == DrNull)
                {
                    break;
                }

                // Move a vertex to target group
                MoveOneVertex(topMachine);
            }

            // Verify that we processed all machines
            int unassignedMachinesCount = 0;
            int i;
            for(i=0; i<numMachines; ++i)
            {
                if (weightedMap[i].group != DrNull)
                {
                    printf("!!! Source map for machine %s is not empty\n",
                           weightedMap[i].machine->GetName().GetChars());
                    ++unassignedMachinesCount;
                }
            }

            if (unassignedMachinesCount != 0)
            {
                fflush(stdout);
            }

            DrAssert(unassignedMachinesCount == 0);
        }

        // Report generated allocation for debugging purposes
        m = m_machineGroup->GetDrEnumerator();
        numMachines = 0;
        while (m.MoveNext())
        {
            if (m.GetValue() != DrNull)
            {
                printf("%s: assigned %d inputs, %I64u bytes\n",
                       (m.GetKey() ? m.GetKey()->GetName().GetChars() : "<missing>"),
                       m.GetValue()->GetGroupSize(), m.GetValue()->GetCombinedOutputSize());
                ++numMachines;
            }
        }
        printf("TOTAL %d machines in use\n", numMachines);
        fflush(stdout);
    }

    GroupMap::DrEnumerator mm = m_machineGroup->GetDrEnumerator();
    while (mm.MoveNext())
    {
        if (mm.GetValue() != DrNull)
        {
            SendMinimum(mm.GetValue(), GetParent()->GetMinPerMachine(), GetParent()->GetMaxPerMachine());
        }
    }

    mm = m_machineGroup->GetDrEnumerator();
    while (mm.MoveNext())
    {
        if (mm.GetValue() != DrNull)
        {
            ReturnUnGrouped(mm.GetValue(), DDGL_Machine);
        }
    }

    m_machineGroup = DrNew GroupMap();
}

void DrDamPartiallyGroupedLayer::GatherRemainingPodGroups()
{
    /* first pull out any groups which are big enough. Since removing
       a group can cull machines out of a subsequent group (if a
       machine has multiple locations), do this full pass before
       gathering up the remainders below */
    GroupMap::DrEnumerator p = m_podGroup->GetDrEnumerator();
    while (p.MoveNext())
    {
        SendMinimum(p.GetValue(), GetParent()->GetMinPerPod(), GetParent()->GetMaxPerPod());
    }

    p = m_podGroup->GetDrEnumerator();
    while (p.MoveNext())
    {
        ReturnUnGrouped(p.GetValue(), DDGL_Pod);
    }

    m_podGroup = DrNew GroupMap();
}

void DrDamPartiallyGroupedLayer::GatherRemainingOverallGroups()
{
    if (m_overallGroup != DrNull)
    {
        SendMinimum(m_overallGroup, GetParent()->GetMinOverall(), GetParent()->GetMaxOverall());
        ReturnUnGrouped(m_overallGroup, DDGL_Overall);
        m_overallGroup = DrNull;
    }
}

void DrDamPartiallyGroupedLayer::LastVertexHasCompleted()
{
    GatherRemainingMachineGroups();
    GatherRemainingPodGroups();
    GatherRemainingOverallGroups();
}

void DrDamPartiallyGroupedLayer::MakeInternalGroup(DrDamVertexGroupPtr group,
                                                   DrVertexPtr successor)
{
    DrLogI("creating internal vertex %d level %d with %d inputs 1 output",
           m_numberOfInternalCreated, m_aggregationLevel, group->GetGroupSize());
    
    /* we only make new vertices in internal levels */
    DrAssert(m_aggregationLevel > 0);
    
    group->DisconnectFromSuccessor(successor);
    successor->GetInputs()->Compact(successor);
    
    DrVertexRef vertex = m_internalVertex->MakeCopy(m_numberOfInternalCreated, m_stageManager);
    ++m_numberOfInternalCreated;
    m_parent->RegisterCreatedVertex(vertex, m_aggregationLevel);
    m_stageManager->RegisterVertex(vertex);
    
    group->ConnectToSuccessor(vertex);
    
    int successorInputsAfterGroup = successor->GetInputs()->GetNumberOfEdges();
    successor->GetInputs()->GrowNumberOfEdges(successorInputsAfterGroup+1);
    
    DrActiveVertexPtr av = (DrActiveVertexPtr)successor;
    av->GrowPendingVersion(1);
    av->GetStartClique()->GrowExternalInputs(1);
    
    vertex->GetOutputs()->SetNumberOfEdges(1);
    vertex->ConnectOutput(0, successor, successorInputsAfterGroup, DCT_File);
    
    vertex->InitializeForGraphExecution();
    vertex->KickStateMachine();
}

DrDynamicAggregateManager::DrDynamicAggregateManager() : DrConnectionManager(true)
{
    InitializeEmpty();
}

DrDynamicAggregateManager::DrDynamicAggregateManager(DrVertexPtr dstVertex,
                                                     DrManagerBasePtr parent) :
    DrConnectionManager(false)
{
    InitializeEmpty();
    m_dstVertex = dstVertex;
    SetParent(parent);

    DrDamPartiallyGroupedLayerRef newLayer =
        DrNew DrDamPartiallyGroupedLayer(parent->GetGraph(), parent->GetStageStatistics(),
                                         this, DrNull, 0, DrString());
    m_grouping->Add(newLayer);
}

DrDynamicAggregateManager::~DrDynamicAggregateManager()
{
    int i;
    for (i=0; i<m_grouping->Size(); ++i)
    {
        m_grouping[i]->Discard();
    }
}

void DrDynamicAggregateManager::InitializeEmpty()
{
    SetMaxAggregationLevel(s_maxAggregationLevel);
    SetGroupingSettings(s_minGroupSize, s_maxGroupSize);
    SetDataGroupingSettings(s_minDataSize, s_maxDataSize,
                            s_maxDataSizeToConsider);

    m_splitAfterGrouping = false;
    m_numberOfSplitCreated = 0;
    m_numberOfManagersCreated = 0;
    m_delayGrouping = false;

    m_upstreamStage = DrNew DrDefaultStageList();
    m_grouping = DrNew LayerList();
    m_createdMap = DrNew CreatedMap();
}

void DrDynamicAggregateManager::CopySettings(DrDynamicAggregateManagerPtr src,
                                           int nameIndex)
{
    m_maxAggregationLevel = src->m_maxAggregationLevel;
    m_minPerMachine = src->m_minPerMachine;
    m_maxPerMachine = src->m_maxPerMachine;
    m_minPerPod = src->m_minPerPod;
    m_maxPerPod = src->m_maxPerPod;
    m_minOverall = src->m_minOverall;
    m_maxOverall = src->m_maxOverall;
    m_minDataPerGroup = src->m_minDataPerGroup;
    m_maxDataPerGroup = src->m_maxDataPerGroup;
    m_maxDataToConsiderGrouping = src->m_maxDataToConsiderGrouping;

    if (src->m_internalVertex == DrNull)
    {
        m_internalVertex = DrNull;
    }
    else
    {
        m_internalVertex = src->m_internalVertex->MakeCopy(nameIndex);
    }

    m_splitAfterGrouping = src->m_splitAfterGrouping;
    SetDelayGrouping(src->m_delayGrouping);

    DrAssert(src->m_createdMap->GetSize() == 0);
    DrAssert(src->m_grouping->Size() == 0);

    int i;
    for (i=0; i<src->m_upstreamStage->Size(); ++i)
    {
        m_upstreamStage->Add(src->m_upstreamStage[i]);
    }
}

void DrDynamicAggregateManager::AddUpstreamStage(DrManagerBasePtr upstreamStage)
{
    int i;
    for (i=0; i<m_grouping->Size(); ++i)
    {
        if (m_grouping[i]->GetStageManager() == upstreamStage)
        {
            /* we only record this if it's not one of the internal
               stages we have created */
            return;
        }
    }

    /* nobody is supposed to be adding more stages once any groups
       have been formed */
    DrAssert(m_grouping->Size() < 2);

    m_upstreamStage->Add(upstreamStage);
}

DrConnectionManagerRef
    DrDynamicAggregateManager::MakeManagerForVertex(DrVertexPtr vertex,
                                                    bool splitting)
{
    if (splitting)
    {
        /* if we've just created a new vertex by splitting, then that
           new vertex gets a (newly reference counted) the base
           manager with no dstVertex */
        return this;
    }
    else
    {
        DrDynamicAggregateManagerRef newManager =
            DrNew DrDynamicAggregateManager(vertex, GetParent());
        newManager->CopySettings(this, m_numberOfManagersCreated);
        ++m_numberOfManagersCreated;
        DrConnectionManagerPtr cm = newManager;
        return cm;
    }
}

void DrDynamicAggregateManager::SetDelayGrouping(bool delayGrouping)
{
    m_delayGrouping = delayGrouping;
    int i;
    for (i=0; i<m_grouping->Size(); ++i)
    {
        m_grouping[i]->SetDelayGrouping(delayGrouping);
    }
}

bool DrDynamicAggregateManager::GetDelayGrouping()
{
    return m_delayGrouping;
}

void DrDynamicAggregateManager::SetInternalVertex(DrVertexPtr internalVertex)
{
    m_internalVertex = internalVertex;
}

void DrDynamicAggregateManager::SetMaxAggregationLevel(int maxAggregation)
{
    m_maxAggregationLevel = maxAggregation;
}

int DrDynamicAggregateManager::GetMaxAggregationLevel()
{
    return m_maxAggregationLevel;
}

void DrDynamicAggregateManager::SetGroupingSettings(int minGroupSize,
                                                    int maxGroupSize)
{
    SetMachineGroupingSettings(minGroupSize, maxGroupSize);
    SetPodGroupingSettings(minGroupSize, maxGroupSize);
    SetOverallGroupingSettings(minGroupSize, maxGroupSize);
}

void DrDynamicAggregateManager::SetMachineGroupingSettings(int minPerMachine,
                                                           int maxPerMachine)
{
    m_minPerMachine = minPerMachine;
    m_maxPerMachine = maxPerMachine;
    if (m_maxPerMachine > 0)
    {
        DrAssert(m_minPerMachine > 1);
    }
    DrAssert(m_minPerMachine <= m_maxPerMachine);
}

int DrDynamicAggregateManager::GetMinPerMachine()
{
    return m_minPerMachine;
}

int DrDynamicAggregateManager::GetMaxPerMachine()
{
    return m_maxPerMachine;
}

void DrDynamicAggregateManager::SetPodGroupingSettings(int minPerPod,
                                                       int maxPerPod)
{
    m_minPerPod = minPerPod;
    m_maxPerPod = maxPerPod;
    if (m_maxPerPod > 0)
    {
        DrAssert(m_minPerPod > 1);
    }
    DrAssert(m_minPerPod <= m_maxPerPod);
}

int DrDynamicAggregateManager::GetMinPerPod()
{
    return m_minPerPod;
}

int DrDynamicAggregateManager::GetMaxPerPod()
{
    return m_maxPerPod;
}

void DrDynamicAggregateManager::SetOverallGroupingSettings(int minOverall,
                                                           int maxOverall)
{
    m_minOverall = minOverall;
    m_maxOverall = maxOverall;
    if (m_maxOverall > 0)
    {
        DrAssert(m_minOverall > 1);
    }
    DrAssert(m_minOverall <= m_maxOverall);
}

int DrDynamicAggregateManager::GetMinOverall()
{
    return m_minOverall;
}

int DrDynamicAggregateManager::GetMaxOverall()
{
    return m_maxOverall;
}

void DrDynamicAggregateManager::SetDataGroupingSettings(UINT64 minDataSize,
                                                        UINT64 maxDataSize,
                                                        UINT64 maxDataToConsider)
{
    m_minDataPerGroup = minDataSize;
    m_maxDataPerGroup = maxDataSize;
    m_maxDataToConsiderGrouping = maxDataToConsider;

    DrAssert(m_maxDataPerGroup > 0);
    DrAssert(m_minDataPerGroup <= m_maxDataPerGroup);
    DrAssert(m_maxDataToConsiderGrouping <= m_maxDataPerGroup);
}

UINT64 DrDynamicAggregateManager::GetMinDataPerGroup()
{
    return m_minDataPerGroup;
}

UINT64 DrDynamicAggregateManager::GetMaxDataPerGroup()
{
    return m_maxDataPerGroup;
}

UINT64 DrDynamicAggregateManager::GetMaxDataToConsiderGrouping()
{
    return m_maxDataToConsiderGrouping;
}

void DrDynamicAggregateManager::SetSplitAfterGrouping(bool splitAfterGrouping)
{
    m_splitAfterGrouping = splitAfterGrouping;
}

bool DrDynamicAggregateManager::GetSplitAfterGrouping()
{
    return m_splitAfterGrouping;
}

DrVertexRef DrDynamicAggregateManager::AddSplitVertex()
{
    DrAssert(m_dstVertex != DrNull);

    DrVertexRef vertex = m_dstVertex->MakeCopy(m_numberOfSplitCreated, GetParent());

    GetParent()->RegisterVertexSplit(vertex, m_dstVertex,
                                     m_numberOfSplitCreated);

    ++m_numberOfSplitCreated;

    return vertex;
}

void DrDynamicAggregateManager::AcceptCompletedGroup(DrDamVertexGroupPtr group,
                                                     int aggregationLevel)
{
    int maxAggregationLevel = m_maxAggregationLevel;
    if (m_internalVertex == DrNull)
    {
        maxAggregationLevel = 0;
    }

    if (aggregationLevel == maxAggregationLevel)
    {
        /* we're already at the maximum level for aggregation, so
           create a new split vertex and connect this group to it */
        DrAssert(m_splitAfterGrouping);

        DrVertexRef newSplit = AddSplitVertex();
        group->DisconnectFromSuccessor(m_dstVertex);
        group->ConnectToSuccessor(newSplit);

        /* the new split vertex should be ready to run, so let it go */
        newSplit->InitializeForGraphExecution();
        newSplit->KickStateMachine();
    }
    else
    {
        DrAssert(aggregationLevel < maxAggregationLevel);
        DrAssert(m_internalVertex != DrNull);

        ++aggregationLevel;
        if (m_grouping->Size() == aggregationLevel)
        {
            DrLogI("Adding new aggregation level %d", aggregationLevel);

            DrString newName;
            if (aggregationLevel == 1)
            {
                newName.SetF("%s+", m_internalVertex->GetName().GetChars());
            }
            else
            {
                newName.SetF("%s+", m_grouping[aggregationLevel-1]->GetName().GetChars());
            }

            DrDamPartiallyGroupedLayerRef newLayer =
                DrNew DrDamPartiallyGroupedLayer(GetParent()->GetGraph(), GetParent()->GetStageStatistics(),
                                                 this, m_internalVertex,
                                                 aggregationLevel, newName);
            DrAssert(newLayer != DrNull);
            newLayer->SetDelayGrouping(m_delayGrouping);
            m_grouping->Add(newLayer);

            GetParent()->
                AddDynamicConnectionManager(newLayer->GetStageManager(),
                                            this);
        }

        DrAssert(m_grouping->Size() > aggregationLevel);
        m_grouping[aggregationLevel]->MakeInternalGroup(group, m_dstVertex);
    }
}

void DrDynamicAggregateManager::
    DealWithUngroupableVertex(DrDamCompletedVertexPtr vertex)
{
    if (m_splitAfterGrouping)
    {
        /* this is a singleton vertex that gets its own new split
           vertex */
        DrVertexRef newSplit = AddSplitVertex();
        DrVertexPtr base = vertex->GetVertex();
        int basePort = vertex->GetOutputPort();

        DrEdge edge = base->GetOutputs()->GetEdge(basePort);
        DrAssert(edge.m_remoteVertex == m_dstVertex);
        int successorPort = edge.m_remotePort;
        m_dstVertex->DisconnectInput(successorPort, false);

        DrEdge re;
        re.m_remoteVertex = base;
        re.m_remotePort = basePort;
        re.m_type = edge.m_type;

        newSplit->GetInputs()->SetNumberOfEdges(1);
        newSplit->GetInputs()->SetEdge(0, re);
        newSplit->InitializeForGraphExecution();
        newSplit->KickStateMachine();
    }
    else
    {
        /* do nothing and leave this vertex connected to the normal
           successor */
    }
}

/* this is called by a DamPartiallyGroupedLayer to return a vertex that
   doesn't fit into any of its groups */
void DrDynamicAggregateManager::ReturnUnGrouped(DrDamCompletedVertexPtr vertex,
                                                int aggLevel)
{
    ++aggLevel;
    if (aggLevel == m_grouping->Size())
    {
        /* none of the vertices at this level were grouped, so no next
           layer has been created */
        DealWithUngroupableVertex(vertex);
    }
    else
    {
        /* pass it on to the next layer in case it fits into one of
           the next set of groups */
        AddCompletedVertex(vertex, aggLevel);
    }
}

void DrDynamicAggregateManager::AddCompletedVertex(DrDamCompletedVertexPtr vertex,
                                                   int aggLevel)
{
    DrAssert(m_grouping->Size() > aggLevel);

    DrDamPartiallyGroupedLayerPtr layer = m_grouping[aggLevel];

    int maxAggregationLevel = m_maxAggregationLevel;
    if (m_internalVertex == DrNull)
    {
        maxAggregationLevel = 0;
    }

    /* if a vertex output is greater than m_maxDataToConsiderGrouping don't
       bother to try to group it. The greedy algorithm we are using is
       really only good at grouping together outputs substantially
       smaller than m_maxGroupDataSize. If we're at the final
       aggregation level and we're not splitting, don't bother to find
       the groups, since we're not going to do anything based on
       them */
    if (vertex->GetOutputSize() < m_maxDataToConsiderGrouping &&
        !(aggLevel == maxAggregationLevel && !m_splitAfterGrouping))
    {
        /* when we add a vertex to a layer, it will eventually return
           to us either as part of a group in a call to
           AcceptCompletedGroup() or as a singleton in a call to
           ReturnUnGrouped() */
        if (m_maxPerMachine > 0)
        {
            layer->AddVertexToMachineGroup(vertex);
        }
        else if (m_maxPerPod > 0)
        {
            layer->AddVertexToPodGroup(vertex);
        }
        else
        {
            DrAssert(m_maxOverall > 0);
            layer->AddVertexToOverallGroup(vertex);
        }
    }
    else
    {
        /* this vertex is going to remain a singleton */
        DealWithUngroupableVertex(vertex);
    }
}

void DrDynamicAggregateManager::RegisterCreatedVertex(DrVertexPtr vertex,
                                                      int aggLevel)
{
    m_createdMap->Add(vertex, aggLevel);
}

void DrDynamicAggregateManager::
    NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                  int /* unused executionVersion */,
                                  DrResourcePtr machine,
                                  DrVertexExecutionStatisticsPtr statistics)
{
    if (vertex->GetNumberOfReportedCompletions() > 0)
    {
        DrLogI("Ignoring completion since vertex has previously completed %d times",
               vertex->GetNumberOfReportedCompletions());
        return;
    }

    int aggLevel = 0;
    /* look up to see if this is an internal vertex created by us */
    m_createdMap->TryGetValue(vertex, aggLevel);

    UINT64 outputSize = statistics->m_outputData[outputPort]->m_dataWritten;
    AddCompletedVertex(DrNew DrDamCompletedVertex(vertex, machine, outputSize, outputPort),
                       aggLevel);
}

void DrDynamicAggregateManager::
    NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort, DrAffinityPtr affinity)
{
    AddCompletedVertex(DrNew DrDamCompletedVertex(vertex, affinity, outputPort),
                       0); /* the aggregation level is always 0 for
                              inputs */
}

void DrDynamicAggregateManager::CleanUp()
{
    /* that's the end, we won't see any more action. Clean up. */

    /* first, get rid of any DrNull edges we left lying around that used
       to belong to inputs we have now grouped */
    m_dstVertex->GetInputs()->Compact(m_dstVertex);

    if (m_splitAfterGrouping)
    {
        /* the original "dummy" destination vertex should have been
           replaced by some number of clones, one for each group we
           ended up with */
        DrAssert(m_dstVertex->GetInputs()->GetNumberOfEdges() == 0);
        GetParent()->UnRegisterVertex(m_dstVertex);
        DrAssert(m_dstVertex->GetOutputs()->GetNumberOfEdges() == 0);
        m_dstVertex->RemoveFromGraphExecution();
    }
    else
    {
        /* the successor should be ready to run: let it rip */
        m_dstVertex->InitializeForGraphExecution();
        m_dstVertex->KickStateMachine();
    }
}

/* this is called by any upstream stage manager when it isn't going to
   send any more vertex completion calls. This includes the internal
   aggregation layer stage managers that we created */
void DrDynamicAggregateManager::
    NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage)
{
    if (m_dstVertex == DrNull)
    {
        /* we are being called by a split-created vertex, and we are
           the base manager that doesn't need to do anything */
        DrAssert(m_grouping->Size() == 0);
        return;
    }

    int layer = 0;
    bool foundLayer = false;

    /* first see if it's one of the "real" upstream stages, i.e. not
       created by us */
    int i;
    for (i=0; i<m_upstreamStage->Size(); ++i)
    {
        if (m_upstreamStage[i] == upstreamStage)
        {
            /* remove this from the set of stages we expect to hear from */
            m_upstreamStage->RemoveAt(i);
            if (m_upstreamStage->Size() > 0)
            {
                /* there are still vertices to come from other upstream
                   stages, so layer 0 is not finished */
                return;
            }
            foundLayer = true;
            break;
        }
    }

    /* at this point layer = 0 */

    if (!foundLayer)
    {
        /* we didn't find anything in the list of registered stages so
           look in the ones we created */
        for (layer=1; layer < m_grouping->Size(); ++layer)
        {
            if (m_grouping[layer]->GetStageManager() == upstreamStage)
            {
                foundLayer = true;
                break;
            }
        }
    }

    /* this is supposed to be some stage we've heard about
       before */
    DrAssert(foundLayer);
    DrAssert(layer < m_grouping->Size());

    m_grouping[layer]->LastVertexHasCompleted();

    if (layer+1 == m_grouping->Size())
    {
        /* this means the last vertex we're ever going to see has
           completed, since the last vertex in the highest stage has
           completed without generating any new stages */
        CleanUp();
    }
    else
    {
        /* this layer isn't going to add any more vertices to the next
           layer down */
        m_grouping[layer+1]->GetStageManager()->SetStillAddingVertices(false);
    }
}
