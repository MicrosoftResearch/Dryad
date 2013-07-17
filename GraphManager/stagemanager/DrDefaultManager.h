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

DRDECLARECLASS(DrManagerBase);
DRREF(DrManagerBase);

DRDECLARECLASS(DrConnectionManager);
DRREF(DrConnectionManager);

DRBASECLASS(DrConnectionManager)
{
public:
    DrConnectionManager(bool manageVerticesIndividually);
    virtual ~DrConnectionManager();

    void SetParent(DrManagerBasePtr parent);
    DrManagerBasePtr GetParent();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage);

    bool ManageVerticesIndividually();

    virtual DrConnectionManagerRef MakeManagerForVertex(DrVertexPtr vertex, bool splitting);
    virtual void RegisterVertex(DrVertexPtr vertex, bool splitting);
    virtual void UnRegisterVertex(DrVertexPtr vertex);
    virtual void NotifyUpstreamSplit(DrVertexPtr upstreamVertex, DrVertexPtr baseNewVertexSplitFrom,
                                     int outputPortOfSplitBase, int upstreamSplitIndex);
    virtual void NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                             int outputPortOfRemovedVertex);

    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort, int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics);
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage);
    virtual void NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort, DrAffinityPtr affinity);

    static void DefaultDealWithUpstreamSplit(DrVertexPtr upstreamVertex,
                                             DrVertexPtr baseNewVertexSplitFrom,
                                             int outputPortOfSplitBase, DrConnectorType type);
    static void DefaultDealWithUpstreamRemoval(DrVertexPtr upstreamVertex,
                                               int outputPortOfRemovedVertex);

private:
    bool                    m_manageVerticesIndividually;
    DrManagerBaseRef       m_parent;
};
DRREF(DrConnectionManager);

typedef DrSet<DrManagerBaseRef> DrStageSet;
DRREF(DrStageSet);

typedef DrArrayList<DrManagerBaseRef> DrDefaultStageList;
DRAREF(DrDefaultStageList,DrManagerBaseRef);

DRVALUECLASS(VertexAndVersion)
{
    public:
        DrActiveVertexRef   m_vertex;
        int                 m_version;
};
typedef DrMultiMap<DrDateTime, VertexAndVersion> RunningTimeMap;
DRREF(RunningTimeMap);


DRDECLAREVALUECLASS(RTIter);
DRRREF(RTIter);
DRVALUECLASS(RTIter)
{
    public:
        bool operator==(RTIterR other);

        int                   m_version;
        RunningTimeMap::Iter  m_iter;
};
DRMAKEARRAYLIST(RTIter);
typedef DrDictionary<DrVertexRef, RTIterListRef> RunningMap;
DRREF(RunningMap);


DRCLASS(DrManagerBase) : public DrStageManager
{
public:
    DrManagerBase(DrGraphPtr graph, DrNativeString stageName);
    virtual ~DrManagerBase();

    virtual void Discard() DROVERRIDE DRSEALED;

    virtual DrGraphPtr GetGraph() DROVERRIDE DRSEALED;

    virtual void InitializeForGraphExecution() DROVERRIDE DRSEALED;
    virtual void KickStateMachine() DROVERRIDE DRSEALED;

    /* the stage name is the friendly name that is used in job
       monitoring summaries. When a stage manager is created as a side
       effect of creating a vertex, it inherits that vertex's name by
       default. */
    virtual DrString GetStageName() DROVERRIDE DRSEALED;

    /* the stage statistics gathers running-time statistics about all
       vertices in the stage, and may be used when trying to detect
       outliers. */
    DrStageStatisticsPtr GetStageStatistics();
    void SetStageStatistics(DrStageStatisticsPtr statistics);

    /* the stage will only be included in job monitoring summaries if
       includeInJobStageList is true. By convention, stages that are
       not active (e.g. input or output streams) are not included in
       monitoring, since there's not much to say about them */
    virtual bool GetIncludeInJobStageList() DROVERRIDE DRSEALED;
    virtual void SetIncludeInJobStageList(bool includeInJobStageList) DROVERRIDE DRSEALED;

    /* assign connector to manage dynamic modifications to the
       subgraph edges connecting this stage from upstreamStage, for
       example to manage a dynamic merge tree */
    virtual void AddDynamicConnectionManager(DrStageManagerPtr upStreamStage,
                                             DrConnectionManagerPtr connector) DROVERRIDE DRSEALED;

    /* similar to the above, but do it not at graph-build time, but during runtime.
       The difference is that the vertices have to be registered */
    virtual void AddDynamicConnectionManagerAtRuntime(DrStageManagerPtr upstreamStage,
                                                      DrConnectionManagerPtr connector) DROVERRIDE DRSEALED;

    /* RegisterVertex should be called once for each vertex that is
       added to the stage. RegisterVertexDerived is a virtual method
       that is called automatically after other actions in
       RegisterVertex so that derived classes can keep track of what
       is happening, and the base class implementation does nothing. A
       client wishing to let the stage know that a vertex has been
       added should call RegisterVertex. RegisterVertex should not be
       called if RegisterVertexSplit is also called on the new
       vertex. */
    virtual void RegisterVertex(DrVertexPtr vertex) DROVERRIDE DRSEALED;
    virtual void RegisterVertexDerived(DrVertexPtr vertex);

    /* Some dynamic graph modifications increase the size of a stage
       by "splitting" new vertices off from a base vertex, and they
       should call RegisterVertexSplit which will notify any relevant
       downstream vertex stage managers about the split. In this case
       RegisterVertex will automatically be called, and the client
       should not call it as well. Different downstream stage managers
       will generally want to deal differently with a split vertex,
       for example they may also choose to split. Consequently the
       split vertex should not be connected to any downstream vertices
       before this method is called, and the downstream manager will
       add edges as appropriate. By default it will add a new edge
       between the new vertex and every downstream vertex that the
       baseToSplitFrom is currently connected
       to. RegisterVertexSplitDerived is a virtual method that is
       called after other actions in RegisterVertexSplit so that
       derived classes can keep track of what is happening, and the
       base class implementation does nothing. A client wishing to let
       the stage know that a vertex has been added should call
       RegisterVertexSplit. */
    virtual void RegisterVertexSplit(DrVertexPtr vertex, DrVertexPtr baseToSplitFrom,
                                     int splitIndex) DROVERRIDE DRSEALED;
    virtual void RegisterVertexSplitDerived(DrVertexPtr vertex, DrVertexPtr baseToSplitFrom,
                                            int splitIndex);

    /* Some dynamic graph modifications remove vertices from
       stages. UnRegisterVertex should be called before a vertex is
       removed, and it will automatically call connected downstream
       managers to notify them that the vertex is being
       removed. UnRegisterVertexDerived is a virtual method that is
       called automatically after other actions in UnRegisterVertex so
       that derived classes can keep track of what is happening, and
       the base class implementation does nothing. A client wishing to
       let the stage know that a vertex has been added should call
       UnRegisterVertex. */
    virtual void UnRegisterVertex(DrVertexPtr vertex) DROVERRIDE DRSEALED;
    virtual void UnRegisterVertexDerived(DrVertexPtr vertex);

    /* NotifyUpstreamSplitDerived is called automatically whenever a
       vertex split is registered that is connected upstream of any
       vertex with this stage manager. Stage managers can keep track
       of upstream stages that are splitting dynamically this way, and
       for example propagate the split forwards if they are in a
       pipeline. This is a virtual method that is included so that
       derived classes can keep track of what is happening, and the
       base class implementation does nothing. */
    virtual void NotifyUpstreamSplitDerived(DrVertexPtr upStreamVertex,
                                            DrVertexPtr baseNewVertexSplitFrom,
                                            int outputPortOfSplitBase,
                                            int upstreamSplitIndex);

    /* NotifyUpstreamVertexRemovalDerived is called automatically
       whenever a vertex is unregistered that is connected upstream of
       any vertex with this stage manager. Stage managers can keep
       track of upstream stages that are splitting dynamically this
       way, and for example propagate the split forwards if they are
       in a pipeline. This is a virtual method that is included so
       that derived classes can keep track of what is happening, and
       the base class implementation does nothing. */
    virtual void NotifyUpstreamVertexRemovalDerived(DrVertexPtr upstreamVertex,
                                                    int outputPortOfRemovedVertex);

    /* VertexIsReady is called by the job manager before it attempts
       to run any vertex. If VertexIsReady returns false then the job
       manager will not start the vertex, otherwise it will proceed as
       normal and run the vertex when it sees fit. VertexIsReady may
       be called many times for a given vertex. If it ever returns
       false, then the application must subsequently call
       vertex->NotifyVertexIsReady() once the vertex is ready
       to run, otherwise the job may never make progress. The default
       implementation of this method always returns true but it can be
       overridden. */
    virtual bool VertexIsReady(DrActiveVertexPtr vertex) DROVERRIDE DRSEALED;
    virtual bool VertexIsReadyDerived(DrActiveVertexPtr vertex);

    virtual void SetStillAddingVertices(bool stillAddingVertices) DROVERRIDE DRSEALED;

    /* NotifyVertexStatus is called every time the job manager
       receives an update on the vertex, which happens periodically
       while the vertex is running, and once when it completes. If
       completionStatus is DryadError_VertexRunning the vertex has not
       yet completed. If completionStatus is
       DryadError_VertexCompleted the vertex has successfully
       completed and NotifyVertexStatus will not be called again for
       this version of the vertex. Otherwise the vertex has exited
       with an error.

       DVertexProcessStatus is defined in
       dryad/system/common/include/dvertexcommand.h and it includes
       the version of the vertex (with GetVertexInstanceVersion()),
       metadata including any error information (with
       GetVertexMetaData()) and information about all of its input and
       output channels.

       The default implementation does nothing.
    */
    virtual void NotifyVertexStatus(DrActiveVertexPtr vertex,
                                    HRESULT completionStatus,
                                    DrVertexProcessStatusPtr status) DROVERRIDE DRSEALED;

    virtual void NotifyVertexRunning(DrActiveVertexPtr vertex,
                                     int executionVersion,
                                     DrResourcePtr machine,
                                     DrVertexExecutionStatisticsPtr statistics) DROVERRIDE DRSEALED;
    virtual void NotifyVertexRunningDerived(DrActiveVertexPtr vertex,
                                            int executionVersion,
                                            DrResourcePtr machine,
                                            DrVertexExecutionStatisticsPtr statistics);
    virtual void NotifyVertexCompleted(DrActiveVertexPtr vertex,
                                       int executionVersion,
                                       DrResourcePtr machine,
                                       DrVertexExecutionStatisticsPtr statistics) DROVERRIDE DRSEALED;
    virtual void NotifyVertexCompletedDerived(DrActiveVertexPtr vertex,
                                              int executionVersion,
                                              DrResourcePtr machine,
                                              DrVertexExecutionStatisticsPtr statistics);
    virtual void NotifyVertexFailed(DrActiveVertexPtr vertex, int executionVersion,
                                    DrResourcePtr machine, DrVertexExecutionStatisticsPtr statistics) DROVERRIDE DRSEALED;
    virtual void NotifyVertexFailedDerived(DrActiveVertexPtr vertex, int executionVersion,
                                           DrResourcePtr machine, DrVertexExecutionStatisticsPtr statistics);

    virtual void CheckForDuplicates() DROVERRIDE DRSEALED;
    virtual void CheckForDuplicatesDerived();

    virtual void NotifyLastVertexCompletedDerived();
    virtual void NotifyUpstreamVertexCompletedDerived(DrActiveVertexPtr upstreamVertex,
                                                      int upstreamVertexOutputPort,
                                                      int executionVersion,
                                                      DrResourcePtr machine,
                                                      DrVertexExecutionStatisticsPtr statistics);
    void NotifyUpstreamLastVertexCompletedDerived(DrManagerBasePtr upstreamStage);

    virtual void NotifyInputReady(DrStorageVertexPtr vertex, DrAffinityPtr affinity) DROVERRIDE DRSEALED;
    virtual void NotifyInputReadyDerived(DrStorageVertexPtr vertex, DrAffinityPtr affinity);
    virtual void NotifyUpstreamInputReadyDerived(DrStorageVertexPtr vertex, int upstreamVertexOutputPort,
                         DrAffinityPtr affinity);

    /* this returns a set containing all the vertices that have been
       registered with this stage */
    virtual DrVertexListPtr GetVertexVector() DROVERRIDE;

	/* this tells all pending and running versions to abort */
	virtual void CancelAllVertices(DrErrorPtr reason) DROVERRIDE;

    /* this adds self's monitoring information to stats, which is a
       container for the statistics of all the stages in the job. */
    //void FillInStageStatistics(CsJobExecutionStatistics* stats);

    DRINTERNALBASECLASS(Holder)
    {
    public:
        Holder(DrConnectionManagerPtr manager);

        DrConnectionManagerPtr GetConnectionManager();
        void AddUpstreamStage(DrManagerBasePtr upstreamStage);
        bool IsManagingUpstreamStage(DrManagerBasePtr upstreamStage);
        virtual DrConnectionManagerPtr GetManagerForVertex(DrVertexPtr vertex);
        virtual void AddManagedVertex(DrVertexPtr vertex, bool splitting);
        virtual void RemoveManagedVertex(DrVertexPtr vertex);
        virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage);

    private:
        DrConnectionManagerRef    m_manager;
        DrStageSetRef             m_stageSet;
    };
    DRREF(Holder);

    typedef DrArrayList<HolderRef> HolderList;
    DRAREF(HolderList,HolderRef);

    DRINTERNALCLASS(IndividualHolder) : public Holder
    {
    public:
        IndividualHolder(DrConnectionManagerPtr manager);

        virtual DrConnectionManagerPtr GetManagerForVertex(DrVertexPtr vertex) DROVERRIDE;
        virtual void AddManagedVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;
        virtual void RemoveManagedVertex(DrVertexPtr vertex) DROVERRIDE;
        virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;

    private:
        typedef DrDictionary<DrVertexRef, DrConnectionManagerRef> Map;
        DRREF(Map);

        MapRef    m_map;
    };

private:
    void RegisterVertexInternal(DrVertexPtr vertex, bool registerSplit);
    void NotifyUpstreamSplit(DrVertexPtr upstreamVertex, DrVertexPtr baseNewVertexSplitFrom,
                             int outputPortOfSplitBase, int upstreamSplitIndex);
    void NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex, int outputPortOfRemovedVertex);
    void NotifyUpstreamVertexCompleted(DrActiveVertexPtr upstreamVertex, int upstreamVertexOutputPort,
                                       int executionVersion, DrResourcePtr machine,
                                       DrVertexExecutionStatisticsPtr statistics);
    void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage);
    void NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int upstreamVertexOutputPort, DrAffinityPtr affinity);

    void CheckIfWeHaveCompleted();

    void AddToRunningMap(DrActiveVertexPtr vertex, int version, DrDateTime runningTime);
    void RemoveFromRunningMap(DrActiveVertexPtr vertex, int version);
    HolderPtr AddDynamicConnectionManagerInternal(DrManagerBasePtr upstreamStage,
                                                  DrConnectionManagerPtr connector);

    HolderPtr LookUpConnectionHolder(DrManagerBasePtr upstreamStage);
    DrConnectionManagerPtr LookUpConnectionManager(DrVertexPtr vertex, DrManagerBasePtr upstreamStage);

    DrGraphRef                   m_graph;
    DrString                     m_stageName;
    bool                         m_includeInJobStageList;
    bool                         m_stillAddingVertices;
    int                          m_verticesNotYetCompleted;
    bool                         m_weHaveCompleted;

    DrStageStatisticsRef         m_stageStatistics;
    DrVertexListRef              m_vertices;
    HolderListRef                m_holder;
    DrStageSetRef                m_downStreamStages;
    RunningMapRef                m_runningMap;
    RunningTimeMapRef            m_runningTimeMap;
};
DRREF(DrManagerBase);
