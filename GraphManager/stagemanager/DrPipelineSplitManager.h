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

DRCLASS(DrPipelineSplitManager) : public DrConnectionManager
{
public:
   DrPipelineSplitManager();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamSplit(DrVertexPtr upstreamVertex,
                                     DrVertexPtr baseNewVertexSplitFrom,
                                     int outputPortOfSplitBase,
                                     int upstreamSplitIndex) DROVERRIDE;
    virtual void NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                             int outputPortOfRemovedVertex) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;

private:
    DrStageSetRef          m_stageSet;
    DrVertexVListMapRef    m_splitMap;
};
DRREF(DrPipelineSplitManager);


/* 
   semipipelinesplitter.h
   
   A connection manager which looks almost like a pipelinesplitter.
   However, the vertices may have inputs coming from other stages as
   well.  These inputs are replicated instead of creating additional
   copies.

   I.e.

   (A >= B) || (C >= B) || (A >= C)

   The (A >= C) edge is required so that C does not yet execute when B is being rewritten.
   With a semipipelinesplitter on the edge (A >= B) the following occurs:

   When A is expanded to (A,A,A) the graph becomes:

   (A,A,A) >= (B,B,B) || C >= (B,B,B) || (A,A,A) => C

   The simple pipelinesplitter does not handle the C => (B,B,B) connection.
*/

DRCLASS(DrSemiPipelineSplitManager) : public DrConnectionManager
{
public:
    DrSemiPipelineSplitManager();

    virtual void AddUpstreamStage(DrManagerBasePtr upstreamStage) DROVERRIDE;
    virtual void NotifyUpstreamSplit(DrVertexPtr upstreamVertex,
                                     DrVertexPtr baseNewVertexSplitFrom,
                                     int outputPortOfSplitBase,
                                     int upstreamSplitIndex) DROVERRIDE;
    virtual void NotifyUpstreamVertexRemoval(DrVertexPtr upstreamVertex,
                                             int outputPortOfRemovedVertex) DROVERRIDE;
    virtual void NotifyUpstreamLastVertexCompleted(DrManagerBasePtr upstreamStage) DROVERRIDE;

private:
    DrStageSetRef          m_stageSet;
    DrVertexVListMapRef    m_splitMap;
};
DRREF(DrSemiPipelineSplitManager);