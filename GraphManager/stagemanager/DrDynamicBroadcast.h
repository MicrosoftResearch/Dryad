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

DRCLASS(DrDynamicBroadcastManager) : public DrConnectionManager
{
    /*
      This connection manager should have always a Tee as a destination.
      I.e., it is placed on the S >= T edge below.  (T is a Tee.)

      (Ideally it should have been placed on the T >= C edge, but Tee
      vertices don't emit stage events right now.)

      From 

      (S >= T)^k >=^k (C^n)

      it builds something like:

      (S >= T >= (copy >= T)^(sqrt(n)))^k >=^k (C ^ n)

      Where the operator >=^k is >= applied k times.
    */

public:
    DrDynamicBroadcastManager(DrActiveVertexPtr copyVertex);

    virtual void NotifyUpstreamVertexCompleted(DrActiveVertexPtr vertex, int outputPort,
                                               int executionVersion,
                                               DrResourcePtr machine,
                                               DrVertexExecutionStatisticsPtr statistics) DROVERRIDE;
    virtual void RegisterVertex(DrVertexPtr vertex, bool splitting) DROVERRIDE;

private:
    void MaybeMakeRoundRobinPodMachines();
    void ExpandTee(DrTeeVertexPtr sourceTee, UINT64 dataWritten, DrResourcePtr machine);

    DrTeeVertexRef                    m_baseTee;
    DrActiveVertexRef                 m_copyVertex;     // inserted as a layer
    DrResourceListRef                 m_roundRobinMachines; // machines ordered to repeat pods as rarely as possible
    int                               m_teeNumber;       // used to renumber the copies
    static const int                  s_minConsumers = 5; // do not create broadcast copies if there are fewer than this many consumers
};
DRREF(DrDynamicBroadcastManager);