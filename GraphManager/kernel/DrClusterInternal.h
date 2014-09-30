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

using namespace Microsoft::Research::Dryad::ClusterInterface;

DRDECLARECLASS(DrClusterInternal);
DRREF(DrClusterInternal);

DRCLASS(DrClusterResource) : public DrResource
{
public:
    DrClusterResource(DrClusterPtr cluster,
                      DrResourceLevel level, DrString name, DrString locality, DrResourcePtr parent,
                      IComputer^ computer);

    IComputer^ GetNode();

private:
    IComputer^    m_node;
};
DRREF(DrClusterResource);

DRDECLARECLASS(DrClusterProcessHandle);
DRREF(DrClusterProcessHandle);

DRCLASS(DrClusterProcessStatus) : public IProcessKeyStatus
{
public:
    DrClusterProcessStatus(DrString key, int timeout, UINT64 version, DrPropertyMessagePtr message, DrMessagePumpPtr pump, DrClusterInternalPtr cluster);

    virtual System::String^ GetKey();
    virtual int GetTimeout();
    virtual UINT64 GetVersion();
    virtual void OnCompleted(UINT64 newVersion, array<unsigned char>^ statusBlock, int processExitCode, System::String^ errorMessage);

private:
    System::String^         m_key;
    int                     m_timeout;
    UINT64                  m_version;

    DrClusterInternalRef    m_cluster;
    DrMessagePumpRef        m_pump;
    DrPropertyMessageRef    m_message;
};
DRREF(DrClusterProcessStatus);

DRCLASS(DrClusterProcessCommand) : public IProcessCommand
{
public:
    DrClusterProcessCommand(DrString key, DrString shortStatus, array<unsigned char>^ payload,
                            DrErrorListenerPtr listener, DrMessagePumpPtr pump);

    virtual System::String^ GetKey();
    virtual System::String^ GetShortStatus();
    virtual array<unsigned char>^ GetPayload();
    virtual void OnCompleted(System::String^ error);

private:
    System::String^              m_key;
    System::String^              m_shortStatus;
    array<unsigned char>^        m_payload;

    DrMessagePumpRef             m_pump;
    DrErrorListenerIRef          m_listener;
};
DRREF(DrClusterProcessCommand);

DRCLASS(DrClusterProcessHandle) : public DrProcessHandle, public IProcessWatcher
{
public:
    DrClusterProcessHandle(DrClusterInternalRef scheduler);

    virtual void CloseHandle() DROVERRIDE;
    virtual DrString GetHandleIdAsString() DROVERRIDE;
    virtual DrString GetDirectory() DROVERRIDE;
    virtual DrResourcePtr GetAssignedNode() DROVERRIDE;

    // <summary>
    // OnQueued is called when the process has been placed in the scheduling queues.
    // </summary>
    virtual void OnQueued();

    // <summary>
    // OnMatched is called when the process has been matched to <param>computer</param>
    // and is about to be scheduled there.
    // </summary>
    virtual void OnMatched(IComputer^ computer, INT64 timestamp);

    // <summary>
    // OnCreated is called when the process has been created on the remote computer.
    // </summary>
    virtual void OnCreated(INT64 timestamp);

    // <summary>
    // OnStarted is called when the process has started running on the remote computer.
    // </summary>
    virtual void OnStarted(INT64 timestamp);

    // <summary>
    // OnExited is called when the process has finished, either because it could not be
    // created (state=ScheduleFailed), because contact was lost with its daemon
    // (state=StatusFailed) or because it has finished (state=ProcessExited). exitCode is
    // the process exit code
    // </summary>
    virtual void OnExited(ProcessExitState state, INT64 timestamp, int exitCode, System::String^ errorText);

    void SetProcess(IProcess^ process);
    void WaitForStateChange(DrPSRListenerIRef listener);
    void GetProperty(ICluster^ scheduler, DrClusterProcessStatus^ status);
    void SetProperty(ICluster^ scheduler, DrClusterProcessCommand^ command);
    void Cancel(ICluster^ scheduler);

private:
    void RecordNewState(DrProcessStateRecordPtr notification);

    System::String^         m_commandLine;

    DrClusterInternalRef    m_parent;

    DrProcessState          m_state;
    int                     m_exitCode;
    DrString                m_id;
    IProcess^               m_processInternal;
    DrResourceRef           m_node;

    DrPSRListenerIRef       m_listener;
    DrProcessStateRecordRef m_notification;

    DrCritSecRef            m_lock;
};

DRCLASS(DrClusterInternal) : public DrCluster, public ILogger
{
public:
    DrClusterInternal();
    ~DrClusterInternal();

    virtual HRESULT Initialize(DrUniversePtr universe, DrMessagePumpPtr pump, DrTimeInterval propertyUpdateInterval) DROVERRIDE;
    virtual void Shutdown() DROVERRIDE;

    virtual DrUniversePtr GetUniverse() DROVERRIDE;
    virtual DrMessagePumpPtr GetMessagePump() DROVERRIDE;
    virtual DrDateTime GetCurrentTimeStamp() DROVERRIDE;

    virtual DrString TranslateFileToURI(DrString leafName, DrString directory,
                                        DrResourcePtr srcResource, DrResourcePtr dstResource, int compressionMode) DROVERRIDE;

    virtual void ScheduleProcess(DrAffinityListRef affinities,
                                 DrString name, DrString commandLineArgs,
                                 DrProcessTemplatePtr processTemplate,
                                 DrPSRListenerPtr listener) DROVERRIDE;

    virtual void CancelScheduleProcess(DrProcessHandlePtr process) DROVERRIDE;

    virtual void WaitForStateChange(DrProcessHandlePtr process, DrPSRListenerPtr listener) DROVERRIDE;

    virtual void GetProcessProperty(DrProcessHandlePtr process,
                                    UINT64 lastSeenVersion, DrString propertyName,
                                    DrPropertyListenerPtr propertyListener) DROVERRIDE;

    virtual void SetProcessCommand(DrProcessHandlePtr p,
                                   DrString propertyName,
                                   DrString propertyDescription,
                                   DrByteArrayRef propertyBlock,
                                   DrErrorListenerPtr listener) DROVERRIDE;

    virtual void ResetProgress(UINT32 totalSteps, bool update) DROVERRIDE;
    virtual void IncrementTotalSteps(bool update) DROVERRIDE;
    virtual void DecrementTotalSteps(bool update) DROVERRIDE;
    virtual void IncrementProgress(PCSTR message) DROVERRIDE;
    virtual void CompleteProgress(PCSTR message) DROVERRIDE;

    virtual void Log(System::String^ entry, System::String^ file, System::String^ function, int line);

    DrResourcePtr GetOrAddResource(IComputer^ computer);

    void DecrementOutstandingPropertyRequests();

private:
    void FetchListOfComputers();
    void AddNodeToUniverse(IComputer^ computer);
    DrResourcePtr AddNodeToUniverseUnderLock(IComputer^ computer);

    ICluster^          m_cluster;
    DrUniverseRef      m_universe;
    DrMessagePumpRef   m_messagePump;
    DrCritSecRef       m_critSec;
    DrTimeInterval     m_propertyUpdateInterval;
    int                m_outstandingPropertyRequests;
    System::Random^    m_random;
};
