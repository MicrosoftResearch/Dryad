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

DRCLASS(DrDynamicRangeDistributionManager) : public DrConnectionManager
{
    /*
      The graph evolves as follows when all S's complete:

      Initial graph: (S,S,S) >= B >= Tee >= (D,D,D) >> M || (S,S,S) >= (D,D,D)

      Final graph: (S,S,S) >= B >= Tee >= (D,D,D) >> (M,M,M,M) || (S,S,S) >= (D,D,D)

      (i.e., M's are expanded)

      S = data source; each has 2 outputs: one for B and one for D.
          The B output contains a sample of the values from the D output.
      B = computes the buckets for distribution based on the 
          samples from all S vertices
      D = distributor; input 0 reads the bucket boundaries from B, 
          input 1 reads the data to distribute to the outputs
      M = actual data consumer.  The dynamic range distributor will replicate
          M to the correct number of instances, and will pass this information 
          as a command-line argument to B

          The DynamicRangeDistributionManager is placed on the edges S>=B.
    */

public:
    // The connection manager will only see the sampled data.  To
    // correctly estimate the data at the distributors, it needs to
    // know the sampling rate as well.  The sampling rate is the
    // fraction of data that goes through the bucketizer node
    // (0 < samplingrate <= 1)
    DrDynamicRangeDistributionManager(DrStageManagerPtr dataConsumer /* M */,
                                      double samplingRate);

    void SetDataPerVertex(UINT64 dataPerVertex);
    UINT64 GetDataPerVertex();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                               int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics) DROVERRIDE;
    virtual void NotifyUpstreamInputReady(DrStorageVertexPtr vertex,
                                          int outputPort, DrAffinityPtr affinity) DROVERRIDE;
    virtual void RegisterVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;

private:
    static const UINT64               s_dataPerVertex = 1024 * 1024 * 1024; /* create one new vertex downstream 
                                                                               for each 1G by default */
    DrStageSetRef                     m_stageSet;
    double                            m_samplingRate;
    UINT64                            m_dataPerVertex;
    UINT64                            m_combinedOutputSize;
    DrStageManagerRef                 m_dataConsumer;
    DrActiveVertexRef                 m_bucketizer; // stage B, on which the current manager is placed
};
DRREF(DrDynamicRangeDistributionManager);