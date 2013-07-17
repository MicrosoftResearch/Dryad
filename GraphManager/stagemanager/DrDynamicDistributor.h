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

DRDECLARECLASS(DrDynamicDistributionManager);
DRREF(DrDynamicDistributionManager);

DRCLASS(DrDynamicDistributionManager) : public DrConnectionManager
{
    /*
      If internal vertex is A, the source vertices are B, and the
      contents of the parent stage is C, the graph is changed as
      follows when all B's complete:

      From:
      (B,B,B) >> C

      To:
      ((B>=A),(B>=A),(B>=A)) >> (C,C)
    */

public:
    // the internal vertex is the distributor
    // the newConnectionManager is the manager that will handle the C layer after expansion
    // (replacing this)
    DrDynamicDistributionManager(DrVertexPtr internalVertex,
                                 DrConnectionManagerPtr newConnectionManager);

    void SetDataPerVertex(UINT64 dataPerVertex);
    UINT64 GetDataPerVertex();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                               int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics) DROVERRIDE;
    virtual void NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort,
                                          DrAffinityPtr affinity) DROVERRIDE;
    virtual void RegisterVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;

 private:
    typedef DrDictionary<DrVertexRef,int> SourcesSet;
    DRREF(SourcesSet);

    static const UINT64               s_dataPerVertex = 1024 * 1024 * 1024; /* create one new vertex downstream 
                                                                               for each 1G by default */
    DrStageSetRef                     m_stageSet;
    SourcesSetRef                     m_sourcesSet;
    DrVertexRef                       m_dstVertex;          // the vertex managed by this connection manager
    UINT64                            m_dataPerVertex;
    UINT64                            m_combinedOutputSize;
    DrVertexRef                       m_internalVertex; // actual distributor vertex
    DrConnectionManagerRef            m_newConnectionManager;
};


DRDECLARECLASS(DrDynamicHashDistributionManager);
DRREF(DrDynamicHashDistributionManager);

DRCLASS(DrDynamicHashDistributionManager) : public DrConnectionManager
{
    /*
      The Source vertices are B, and the contents of the parent stage
      is C, the graph is changed as follows when all B's complete:

      From:
      (B,B,B) >=^n C
      (n parallel connections from B to C)

      To:
      (B,B,B) >=^(n/2) (C,C)

      I.e., C is replicated, and its n inputs are redistributed among
      the copies round-robin.

      Each B vertex must have the same number of connections to C.
      There must be no outputs of B going to some other vertex than C.
    */

public:
    DrDynamicHashDistributionManager();

    void SetDataPerVertex(UINT64 dataPerVertex);
    UINT64 GetDataPerVertex();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                               int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics) DROVERRIDE;
    virtual void NotifyUpstreamInputReady(DrStorageVertexPtr vertex, int outputPort,
                                          DrAffinityPtr affinity) DROVERRIDE;
    virtual void RegisterVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;

private:
    static const UINT64               s_dataPerVertex = 1024 * 1024 * 1024; /* create one new vertex downstream 
                                                                               for each 1G by default */
    DrStageSetRef                     m_stageSet;
    DrVertexSetRef                    m_sources;
    DrVertexRef                       m_dstVertex;          // the vertex managed by this connection manager
    UINT64                            m_dataPerVertex;
    UINT64                            m_combinedOutputSize;
    int                               m_edgesInBundle;      // edges coming from each input
};
