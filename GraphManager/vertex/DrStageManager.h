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

DRDECLARECLASS(DrStageManager);
DRREF(DrStageManager);

DRDECLARECLASS(DrConnectionManager);
DRREF(DrConnectionManager);

DRDECLARECLASS(DrGraph);
DRREF(DrGraph);

DRCLASS(DrStageManager abstract) : public DrSharedCritSec
{
public:
    DrStageManager(DrGraphPtr graph);
    virtual ~DrStageManager();

    virtual void Discard() = 0;

    virtual DrGraphPtr GetGraph() = 0;

    virtual DrString GetStageName() = 0;

    virtual void InitializeForGraphExecution() = 0;
    virtual void KickStateMachine() = 0;

    /* the stage will only be included in job monitoring summaries if
       includeInJobStageList is true. By convention, stages that are
       not active (e.g. input or output streams) are not included in
       monitoring, since there's not much to say about them */
    virtual bool GetIncludeInJobStageList() = 0;
    virtual void SetIncludeInJobStageList(bool includeInJobStageList) = 0;

    /* assign connector to manage dynamic modifications to the
       subgraph edges connecting this stage from upstreamStage, for
       example to manage a dynamic merge tree */
    virtual void AddDynamicConnectionManager(DrStageManagerPtr upStreamStage,
                                             DrConnectionManagerPtr connector) = 0;

    /* similar to the above, but do it not at graph-build time, but during runtime.
       The difference is that the vertices have to be registered */
    virtual void AddDynamicConnectionManagerAtRuntime(DrStageManagerPtr upstreamStage,
                                                      DrConnectionManagerPtr connector) = 0;

    /* RegisterVertex should be called once for each vertex that is
       added to the stage. RegisterVertexDerived is a virtual method
       that is called automatically after other actions in
       RegisterVertex so that derived classes can keep track of what
       is happening, and the base class implementation does nothing. A
       client wishing to let the stage know that a vertex has been
       added should call RegisterVertex. RegisterVertex should not be
       called if RegisterVertexSplit is also called on the new
       vertex. */
    virtual void RegisterVertex(DrVertexPtr vertex) = 0;

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
                                     int splitIndex) = 0;

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
    virtual void UnRegisterVertex(DrVertexPtr vertex) = 0;

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
    virtual bool VertexIsReady(DrActiveVertexPtr vertex) = 0;

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
                                    DrVertexProcessStatusPtr status) = 0;

    virtual void NotifyVertexRunning(DrActiveVertexPtr vertex,
                                     int executionVersion,
                                     DrResourcePtr machine,
                                     DrVertexExecutionStatisticsPtr statistics) = 0;
    virtual void NotifyVertexCompleted(DrActiveVertexPtr vertex,
                                       int executionVersion,
                                       DrResourcePtr machine,
                                       DrVertexExecutionStatisticsPtr statistics) = 0;
    virtual void NotifyVertexFailed(DrActiveVertexPtr vertex, int executionVersion,
                                    DrResourcePtr machine, DrVertexExecutionStatisticsPtr statistics) = 0;

    virtual void CheckForDuplicates() = 0;

    virtual void NotifyInputReady(DrStorageVertexPtr vertex, DrAffinityPtr affinity) = 0;

    virtual void SetStillAddingVertices(bool stillAddingVertices) = 0;

    /* this returns a set containing all the vertices that have been
       registered with this stage */
    virtual DrVertexListPtr GetVertexVector() = 0;

	/* this tells all pending and running versions of all vertices to abort */
	virtual void CancelAllVertices(DrErrorPtr reason) = 0;

    /* this adds self's monitoring information to stats, which is a
       container for the statistics of all the stages in the job. */
    //void FillInStageStatistics(CsJobExecutionStatistics* stats);
};
DRREF(DrStageManager);

typedef DrArrayList<DrStageManagerRef> DrStageList;
DRAREF(DrStageList,DrStageManagerRef);
