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

DRDECLARECLASS(DrCluster);
DRREF(DrCluster);

/* DrCluster abstracts away all the internal types used by the concrete cluster interface.
DrClusterInternal.h defines the concrete implementation that talks to the (currently managed-only)
interface */

DRCLASS(DrCluster abstract) : public DrCritSec
{
public:
    /* this returns an object of the concrete type */
    static DrClusterRef Create();

    virtual ~DrCluster();

    virtual HRESULT Initialize(DrUniversePtr universe, DrMessagePumpPtr pump, DrTimeInterval propertyUpdateInterval) = 0;
    virtual void Shutdown() = 0;
    virtual DrUniversePtr GetUniverse() = 0;
    virtual DrMessagePumpPtr GetMessagePump() = 0;
    virtual DrDateTime GetCurrentTimeStamp() = 0;

    virtual DrString TranslateFileToURI(DrString fileName, DrString directory,
                                        DrResourcePtr srcResource, DrResourcePtr dstResource, int compressionMode) = 0;

    virtual void ScheduleProcess(DrAffinityListRef affinities,
                                 DrString name, DrString commandLineArgs,
                                 DrProcessTemplatePtr processTemplate,
                                 DrPSRListenerPtr listener) = 0;

    virtual void CancelScheduleProcess(DrProcessHandlePtr process) = 0;

    virtual void WaitForStateChange(DrProcessHandlePtr process, DrPSRListenerPtr listener) = 0;

    virtual void GetProcessProperty(DrProcessHandlePtr process,
                                    UINT64 lastSeenVersion, DrString propertyName,
                                    DrPropertyListenerPtr propertyListener) = 0;

    virtual void SetProcessCommand(DrProcessHandlePtr p,
                                   DrString propertyName,
                                   DrString propertyDescription,
                                   DrByteArrayRef propertyBlock,
                                   DrErrorListenerPtr listener) = 0;

    virtual void ResetProgress(UINT32 totalSteps, bool update) = 0;
    virtual void IncrementTotalSteps(bool update) = 0;
    virtual void DecrementTotalSteps(bool update) = 0;
    virtual void IncrementProgress(PCSTR message) = 0;
    virtual void CompleteProgress(PCSTR message) = 0;

};
DRREF(DrCluster);