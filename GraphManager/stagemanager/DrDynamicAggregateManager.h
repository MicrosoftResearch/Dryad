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

#pragma once

DRDECLARECLASS(DrDynamicAggregateManager);
DRREF(DrDynamicAggregateManager);

DRDECLARECLASS(DrDamCompletedVertex);
DRREF(DrDamCompletedVertex);

DRDECLARECLASS(DrDamVertexGroup);
DRREF(DrDamVertexGroup);

typedef DrArray<DrDamVertexGroupRef> DrDamVertexGroupArray;
DRAREF(DrDamVertexGroupArray,DrDamVertexGroupRef);

DRBASECLASS(DrDamCompletedVertex)
{
public:
    DrDamCompletedVertex(DrVertexPtr vertex, DrResourcePtr machine, UINT64 outputSize, int outputPort);
    DrDamCompletedVertex(DrVertexPtr vertex, DrAffinityPtr affinity, int outputPort);

    DrDamCompletedVertexRef MakeCopy();
    void RemovePodDuplicates();

    DrVertexPtr GetVertex();
    int GetOutputPort();
    UINT64 GetOutputSize();
    DrResourceArrayRef GetMachineArray();
    DrDamVertexGroupArrayRef GetGroupArray();
    DrIntArrayRef GetGroupIndexArray();
    int GetNumberOfLocations();

    void SetGroup(int locationIndex, DrDamVertexGroupPtr group, int groupIndex);
    void MoveGroupIndex(DrDamVertexGroupPtr group, int newIndex, int oldIndex);
    void RemoveDuplicateVerticesFromGroups(DrDamVertexGroupPtr keepGroup);
    int GetLocationIndex(DrDamVertexGroupPtr group, int groupIndex);

private:
    DrVertexRef               m_vertex;
    int                       m_outputPort;
    int                       m_numberOfLocations;
    DrAffinityRef             m_affinity;
    DrResourceArrayRef        m_machine;
    DrDamVertexGroupArrayRef  m_group;
    DrIntArrayRef             m_groupIndex;
};
DRREF(DrDamCompletedVertex);

typedef DrArrayList<DrDamCompletedVertexRef> DrDamCompletedVertexList;
DRAREF(DrDamCompletedVertexList,DrDamCompletedVertexRef);

DRBASECLASS(DrDamVertexGroup)
{
public:
    DrDamVertexGroup(int maxVerticesInGroup);

    void AddVertex(DrDamCompletedVertexPtr v, int locationIndex);
    void RemoveVertex(DrDamCompletedVertexPtr v, int groupIndex);
    void ClaimVertices();
    void Discard();
    void DisconnectFromSuccessor(DrVertexPtr successor);
    void ConnectToSuccessor(DrVertexPtr successor);

    int GetGroupSize();
    DrDamCompletedVertexListPtr GetGroupArray();
    UINT64 GetCombinedOutputSize();
    int GetMaxNumberOfLocations();

private:
    static const int   s_initialArraySize = 4;
    void Compact();

    UINT64                       m_combinedOutputSize;
    DrDamCompletedVertexListRef  m_vertex;
    int                          m_numberOfVertices;
    int                          m_maxVertices;
    int                          m_maxNumberOfLocations;
};

enum DrDamGroupingLevel
{
    DDGL_Machine,
    DDGL_Pod,
    DDGL_Overall
};

DRVALUECLASS(MachineGroup)
{
    public:
        DrResourceRef        machine;
        DrDamVertexGroupRef  group;
        DrDamVertexGroupRef  outputGroup;
        int                  nextVertex;
};
DRRREF(MachineGroup);
DRMAKEARRAY(MachineGroup);

DRBASECLASS(DrDamPartiallyGroupedLayer)
{
public:
    DrDamPartiallyGroupedLayer(DrGraphPtr graph, DrStageStatisticsPtr statistics,
                               DrDynamicAggregateManagerPtr parent,
                               DrVertexPtr internalVertex,
                               int aggregationLevel, DrString name);

    void Discard();

    DrString GetName();
    DrDynamicAggregateManagerPtr GetParent();
    DrManagerBasePtr GetStageManager();

    void AddVertexToMachineGroup(DrDamCompletedVertexPtr vertex);
    void AddVertexToPodGroup(DrDamCompletedVertexPtr vertex);
    void AddVertexToOverallGroup(DrDamCompletedVertexPtr vertex);
    void GatherRemainingMachineGroups();
    void GatherRemainingPodGroups();
    void GatherRemainingOverallGroups();
    void LastVertexHasCompleted();
    void SetDelayGrouping(bool delayGrouping);

    void MakeInternalGroup(DrDamVertexGroupPtr group, DrVertexPtr successor);

private:
    typedef DrDictionary<DrResourceRef, DrDamVertexGroupRef> GroupMap;
    DRREF(GroupMap);



    void ConsiderSending(DrDamVertexGroupPtr group, int maxVertices, UINT64 additionalSize);
    DrDamVertexGroupPtr SendMinimum(DrDamVertexGroupPtr group, int minVertices, int maxVertices);
    void ReturnUnGrouped(DrDamVertexGroupPtr group, DrDamGroupingLevel level);
    static int MachineGroupCmp(MachineGroupR left, MachineGroupR right);
    bool MoveOneVertex(MachineGroupR grpStruct);

    DrString                     m_name;
    DrDynamicAggregateManagerPtr m_parent;
    DrManagerBaseRef             m_stageManager;
    DrVertexRef                  m_internalVertex;
    int                          m_numberOfInternalCreated;
    int                          m_aggregationLevel;
    GroupMapRef                  m_machineGroup;
    GroupMapRef                  m_podGroup;
    DrDamVertexGroupRef          m_overallGroup;
    bool                         m_delayGrouping;
};
DRREF(DrDamPartiallyGroupedLayer);


DRCLASS(DrDynamicAggregateManager) : public DrConnectionManager
{
public:
    DrDynamicAggregateManager();
    ~DrDynamicAggregateManager();

    static const int    s_minGroupSize = 16;
    static const int    s_maxGroupSize = 128;
    static const int    s_maxAggregationLevel = 256;
    static const UINT64 s_minDataSize = 896*1024*1024;
    static const UINT64 s_maxDataSize = 1024*1024*1024; /* 1 GByte */
    static const UINT64 s_maxDataSizeToConsider = 512*1024*1024;

    /* if no internal vertex is set, then the maxAggregationLevel will
       be taken to be zero regardless of what is set in
       SetMaxAggregationLevel */
    void SetInternalVertex(DrVertexPtr internalVertex);

    void SetMaxAggregationLevel(int maxAggregation);
    int GetMaxAggregationLevel();

    void SetGroupingSettings(int minGroupSize,
                             int maxGroupSize);

    void SetMachineGroupingSettings(int minPerMachine,
                                    int maxPerMachine);
    int GetMinPerMachine();
    int GetMaxPerMachine();

    void SetPodGroupingSettings(int minPerPod,
                                int maxPerPod);
    int GetMinPerPod();
    int GetMaxPerPod();

    void SetOverallGroupingSettings(int minOverall,
                                    int maxOverall);
    int GetMinOverall();
    int GetMaxOverall();

    void SetDataGroupingSettings(UINT64 minDataSize, UINT64 maxDataSize,
                                 UINT64 maxDataToConsider);
    UINT64 GetMinDataPerGroup();
    UINT64 GetMaxDataPerGroup();
    UINT64 GetMaxDataToConsiderGrouping();

    void SetSplitAfterGrouping(bool splitAfterGrouping);
    bool GetSplitAfterGrouping();

    void SetDelayGrouping(bool delayGrouping);
    bool GetDelayGrouping();

    virtual DrConnectionManagerRef MakeManagerForVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;
    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;

    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex,
                                               int outputPort, int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort,
                                          DrAffinityPtr affinity) DROVERRIDE;

    typedef DrArrayList<DrDamPartiallyGroupedLayerRef> LayerList;
    DRAREF(LayerList,DrDamPartiallyGroupedLayerRef);
    typedef DrDictionary<DrVertexRef, int> CreatedMap;
    DRREF(CreatedMap);

    DrDynamicAggregateManager(DrVertexPtr dstVertex, DrManagerBasePtr parent);

    void InitializeEmpty();
    void CopySettings(DrDynamicAggregateManagerPtr src, int nameIndex);
    DrVertexRef AddSplitVertex();
    void AcceptCompletedGroup(DrDamVertexGroupPtr group, int aggregationLevel);
    void DealWithUngroupableVertex(DrDamCompletedVertexPtr vertex);
    void ReturnUnGrouped(DrDamCompletedVertexPtr vertex, int aggLevel);
    void AddCompletedVertex(DrDamCompletedVertexPtr vertex, int aggLevel);
    void RegisterCreatedVertex(DrVertexPtr vertex, int aggLevel);
    void CleanUp();

    int                             m_maxAggregationLevel;
    int                             m_minPerMachine;
    int                             m_maxPerMachine;
    int                             m_minPerPod;
    int                             m_maxPerPod;
    int                             m_minOverall;
    int                             m_maxOverall;
    UINT64                          m_minDataPerGroup;
    UINT64                          m_maxDataPerGroup;
    UINT64                          m_maxDataToConsiderGrouping;
    DrVertexRef                     m_internalVertex;
    DrVertexRef                     m_dstVertex;
    bool                            m_splitAfterGrouping;
    int                             m_numberOfSplitCreated;
    int                             m_numberOfManagersCreated;

    DrDefaultStageListRef           m_upstreamStage;
    LayerListRef                    m_grouping;
    CreatedMapRef                   m_createdMap;
    bool                            m_delayGrouping;
};
