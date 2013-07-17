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

#include <xcompute.h>

DRCLASS(DrXComputeResource) : public DrResource
{
public:
    DrXComputeResource(DrResourceLevel level, DrString name, DrString locality, DrResourcePtr parent,
                       XCPROCESSNODEID node);

    XCPROCESSNODEID GetNode();

private:
    XCPROCESSNODEID   m_node;
};

DRCLASS(DrXComputeProcessHandle) : public DrProcessHandle
{
public:
    virtual void CloseHandle() DROVERRIDE;
    virtual DrString GetHandleIdAsString() DROVERRIDE;
    virtual DrProcessState GetState(HRESULT& reason) DROVERRIDE;
    virtual DrString GetFileURIBase() DROVERRIDE;

    /* this is public so the managed DrXComputeInternal class can make a pin_ptr out of it */
    XCPROCESSHANDLE   m_handle;
};
DRREF(DrXComputeProcessHandle);

class DrXComputeWaitForStateChangeOverlapped : public DrXComputeOverlapped
{
public:
    DrXComputeWaitForStateChangeOverlapped(DrXComputePtr parent, DrPSRMessagePtr message);

    void Process();
};

class DrXComputeCancelScheduleProcessOverlapped : public DrXComputeOverlapped
{
public:
    DrXComputeCancelScheduleProcessOverlapped(DrXComputePtr parent);

    void Process();
};

class DrXComputeGetSetOverlapped : public DrXComputeOverlapped
{
public:
    DrXComputeGetSetOverlapped(DrXComputePtr parent, DrMessageBasePtr message);
    virtual ~DrXComputeGetSetOverlapped();

    PXC_SETANDGETPROCESSINFO_REQRESULTS* GetResultsPointer();

protected:
    PXC_SETANDGETPROCESSINFO_REQRESULTS   m_results;
};

DRBASECLASS(DrHeapString)
{
public:
    DrString m_payload;
};
DRREF(DrHeapString);

class DrXComputeGetPropertyOverlapped : public DrXComputeGetSetOverlapped
{
public:
    DrXComputeGetPropertyOverlapped(DrXComputePtr parent, DrString propertyName, DrPPSMessagePtr message);

    void Process();

private:
    DrRefHolder<DrHeapString>    m_propertyName;
};

class DrXComputeSetCommandOverlapped : public DrXComputeGetSetOverlapped
{
public:
    DrXComputeSetCommandOverlapped(DrXComputePtr parent, DrErrorMessagePtr message);

    void Process();
};

DRCLASS(DrXComputeInternal) : public DrXCompute
{
public:
    DrXComputeInternal();
    ~DrXComputeInternal();

    virtual HRESULT Initialize(DrUniversePtr universe, DrMessagePumpPtr pump) DROVERRIDE;
    virtual void Shutdown() DROVERRIDE;

    virtual DrUniversePtr GetUniverse() DROVERRIDE;
    virtual DrMessagePumpPtr GetMessagePump() DROVERRIDE;
    virtual DrDateTime GetCurrentTimeStamp() DROVERRIDE;

    virtual void ScheduleProcess(DrAffinityListRef affinities,
                                 DrString name, DrString commandLine,
                                 DrProcessTemplatePtr processTemplate,
                                 DrPSRListenerPtr listener) DROVERRIDE;
    virtual void CancelScheduleProcess(DrProcessHandlePtr process) DROVERRIDE;

    virtual void WaitUntilStart(DrProcessHandlePtr process, DrPSRListenerPtr listener) DROVERRIDE;
    virtual void WaitUntilCompleted(DrProcessHandlePtr process, DrPSRListenerPtr listener) DROVERRIDE;

    virtual void GetProcessProperty(DrProcessHandlePtr process,
                                    UINT64 lastSeenVersion, DrString propertyName,
                                    DrTimeInterval maxBlockTime,
                                    DrPPSListenerPtr processListener,
                                    DrPropertyListenerPtr propertyListener) DROVERRIDE;

    virtual void SetProcessCommand(DrProcessHandlePtr p,
                                   UINT64 newVersion, DrString propertyName,
                                   DrString propertyDescription,
                                   DrByteArrayRef propertyBlock,
                                   DrErrorListenerPtr listener) DROVERRIDE;

    virtual void TerminateProcess(DrProcessHandlePtr p,
                                  DrErrorListenerPtr listener) DROVERRIDE;

    void ProcessStateChange(HRESULT status, DrPSRMessagePtr message);
    void ProcessPropertyFetch(HRESULT status, DrString propertyName,
                              PXC_SETANDGETPROCESSINFO_REQRESULTS xcStatus, DrPPSMessagePtr message);
    void ProcessCommandResult(HRESULT status, DrErrorMessagePtr message);

    virtual void ResetProgress(UINT32 totalSteps, bool update) DROVERRIDE;
    virtual void IncrementTotalSteps(bool update) DROVERRIDE;
    virtual void DecrementTotalSteps(bool update) DROVERRIDE;
    virtual void IncrementProgress(PCSTR message) DROVERRIDE;
    virtual void CompleteProgress(PCSTR message) DROVERRIDE;


private:
    void AddNodeToUniverse(XCPROCESSNODEID node);
    HRESULT FetchListOfComputers();
    void WaitForStateChange(DrXComputeProcessHandlePtr p, XCPROCESSSTATE targetState,
                            DrPSRListenerPtr listener);

    DrUniverseRef      m_universe;
    DrMessagePumpRef   m_messagePump;
    XCSESSIONHANDLE    m_session;
    XCPROCESSHANDLE    m_localProcess;
};
DRREF(DrXComputeInternal);