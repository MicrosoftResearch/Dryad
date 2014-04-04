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

DRCLASS(DrVertexIdSource)
{
public:
    static int GetNextId();

private:
    static int s_nextId;
};

DRDECLARECLASS(DrVertex);
DRREF(DrVertex);

typedef DrSet<DrActiveVertexRef> DrActiveVertexSet;
DRREF(DrActiveVertexSet);

DRDECLARECLASS(DrStageManager);
DRREF(DrStageManager);

DRINTERFACE(DrVertexTopologyReporter)
{
public:
    virtual void ReportFinalTopology(DrVertexPtr vertex, DrResourcePtr runningMachine,
                                     DrTimeInterval runningTime) = 0;
};
DRIREF(DrVertexTopologyReporter);

DRCLASS(DrVertex abstract) : public DrSharedCritSec
{
public:
    DrVertex(DrStageManagerPtr stage);

    void Discard();

    DrVertexRef MakeCopy(int suffix);
    virtual DrVertexRef MakeCopy(int suffix, DrStageManagerPtr stage) = 0;

    DrStageManagerPtr GetStageManager();
    int GetId();
    int GetPartitionId();

    void SetName(DrString name);
    DrString GetName();

    DrEdgeHolderPtr GetInputs();
    DrEdgeHolderPtr GetOutputs();

    void SetNumberOfSubgraphInputs(int numberOfInputs);
    int GetNumberOfSubgraphInputs();
    void SetNumberOfSubgraphOutputs(int numberOfOutputs);
    int GetNumberOfSubgraphOutputs();

    DrVertexPtr RemoteInputVertex(int localPort);
    int RemoteInputPort(int localPort);
    DrVertexPtr RemoteOutputVertex(int localPort);
    int RemoteOutputPort(int localPort);

    void ConnectOutput(int localPort, DrVertexPtr remoteVertex, int remotePort, DrConnectorType type);
    void DisconnectInput(int localPort, bool disconnectRemote);
    void DisconnectOutput(int localPort, bool disconnectRemote);

    virtual void InitializeForGraphExecution() = 0;
    virtual void KickStateMachine() = 0;
    virtual void RemoveFromGraphExecution() = 0;
    virtual void ReportFinalTopology(DrVertexTopologyReporterPtr reporter) = 0;

    virtual DrVertexOutputGeneratorPtr GetOutputGenerator(int edgeIndex, DrConnectorType type,
                                                          int version) = 0;
    virtual DrString GetURIForWrite(int port, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) = 0;

    virtual void ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                               DrVertexOutputGeneratorPtr generator) = 0;
    virtual void ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                              DrVertexOutputGeneratorPtr generator) = 0;
    virtual void NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator) = 0;
    virtual void ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                DrVertexOutputGeneratorPtr generator,
                                                DrVertexExecutionStatisticsPtr stats) = 0;
    virtual void ReactToDownStreamFailure(int port, DrConnectorType type,
                                          int downStreamVersion) = 0;
    virtual void ReactToUpStreamFailure(int port, DrConnectorType type,
                                        DrVertexOutputGeneratorPtr generator, int downStreamVersion) = 0;
    virtual void ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                     DrVertexVersionGeneratorPtr inputs,
                                     DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                     DrErrorPtr originalReason) = 0;
    virtual void CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction) = 0;

	virtual void CancelAllVersions(DrErrorPtr reason) = 0;

protected:
    void InitializeFromOther(DrVertexPtr other, int suffix);
    virtual void DiscardDerived() = 0;

    DrStageManagerRef             m_stage;
    DrString                      m_name;
    int                           m_id;
    int                           m_partitionId;

    DrEdgeHolderRef               m_inputEdges;
    DrEdgeHolderRef               m_outputEdges;
    int                           m_numberOfSubgraphInputs;
    int                           m_numberOfSubgraphOutputs;
};

typedef DrArrayList<DrVertexRef> DrVertexList;
DRAREF(DrVertexList,DrVertexRef);

typedef DrSet<DrVertexRef> DrVertexSet;
DRREF(DrVertexSet);

typedef DrDictionary<DrVertexRef,DrVertexListRef> DrVertexVListMap;
DRREF(DrVertexVListMap);

typedef DrArrayList<DrActiveVertexOutputGeneratorRef> DrCompletedVertexList;
DRAREF(DrCompletedVertexList,DrActiveVertexOutputGeneratorRef);

DRDECLARECLASS(DrStartClique);
DRREF(DrStartClique);

DRDECLARECLASS(DrCohort);
DRREF(DrCohort);

DRCLASS(DrActiveVertex) : public DrVertex
{
public:
    DrActiveVertex(DrStageManagerPtr stage, DrProcessTemplatePtr processTemplate,
                   DrVertexTemplatePtr vertexTemplate);

    virtual void DiscardDerived() DROVERRIDE;

    virtual DrVertexRef MakeCopy(int suffix, DrStageManagerPtr stage) DROVERRIDE;

    void AddArgument(DrNativeString argument);
    void AddArgumentInternal(DrString argument);

    DrAffinityPtr GetAffinity();

    virtual void InitializeForGraphExecution() DROVERRIDE;
    virtual void KickStateMachine() DROVERRIDE;
    virtual void RemoveFromGraphExecution() DROVERRIDE;
    virtual void ReportFinalTopology(DrVertexTopologyReporterPtr reporter) DROVERRIDE;

    virtual DrVertexOutputGeneratorPtr GetOutputGenerator(int edgeIndex, DrConnectorType type,
                                                          int version) DROVERRIDE;
    virtual DrString GetURIForWrite(int port, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) DROVERRIDE;

    virtual void ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                               DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                              DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                DrVertexOutputGeneratorPtr generator,
                                                DrVertexExecutionStatisticsPtr stats) DROVERRIDE;
    virtual void ReactToDownStreamFailure(int port, DrConnectorType type,
                                          int downStreamVersion) DROVERRIDE;
    virtual void ReactToUpStreamFailure(int port, DrConnectorType type,
                                        DrVertexOutputGeneratorPtr generator,
                                        int downStreamVersion) DROVERRIDE;
    virtual void ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                     DrVertexVersionGeneratorPtr inputs,
                                     DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                     DrErrorPtr originalReason) DROVERRIDE;
    virtual void CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction) DROVERRIDE;

	virtual void CancelAllVersions(DrErrorPtr reason) DROVERRIDE;
    
    DrVertexCommandBlockRef MakeVertexStartCommand(DrVertexVersionGeneratorPtr inputGenerators,
                                                   DrActiveVertexOutputGeneratorPtr selfGenerator);

	void RequestDuplicate(int versionToDuplicate);

    void NotifyVertexIsReady();
	bool HasPendingVersion();
	bool HasRunningVersion(int i);
	bool HasCompletedVersion(int i);
	void GrowPendingVersion(int numberOfEdgesToGrow);
    void InstantiateVersion(int version);
    void AddCurrentAffinitiesToList(int version, DrAffinityListPtr list);
    void StartProcess(int version);
    void CheckForProcessAlreadyStarted(int version);
    void ReactToStartedProcess(int version, DrLockBox<DrProcess> process);
    void ReactToStartedVertex(DrVertexRecordPtr record, DrVertexExecutionStatisticsPtr stats);
    void ReactToRunningVertexUpdate(DrVertexRecordPtr record,
                                    HRESULT exitStatus, DrVertexProcessStatusPtr status);
    void ReactToCompletedVertex(DrVertexRecordPtr record, DrVertexExecutionStatisticsPtr stats);
    void CancelVersion(int version, DrErrorPtr error, DrCohortProcessPtr cohortProcess);

    int GetNumberOfReportedCompletions();

    virtual DrString GetDescription();

    void SetStartClique(DrStartCliquePtr cohort);
    virtual DrStartCliquePtr GetStartClique();

    void SetCohort(DrCohortPtr cohort);
    virtual DrCohortPtr GetCohort();

private:
    DrVertexRecordPtr GetRunningVersion(int version);

    DrCohortRef                       m_cohort;
    DrStartCliqueRef                  m_startClique;
    DrVertexTemplateRef               m_vertexTemplate;
    DrProcessTemplateRef              m_processTemplate;
    DrAffinityRef                     m_affinity;

    DrStringListRef                   m_argument;
    DrUINT64ArrayRef                  m_outputSizeHint;
    UINT64                            m_totalOutputSizeHint;
    int                               m_maxOpenInputChannelCount;
    int                               m_maxOpenOutputChannelCount;

    int                               m_numberOfReportedCompletions;
    bool                              m_registeredWithGraph;

    DrVertexVersionGeneratorRef       m_pendingVersion;
    DrVertexRecordListRef             m_runningVertex;
    DrActiveVertexOutputGeneratorRef  m_completedRecord;
	DrCompletedVertexListRef          m_spareCompletedRecord;
};

typedef DrArrayList<DrActiveVertexRef> DrActiveVertexList;
DRAREF(DrActiveVertexList,DrActiveVertexRef);


DRCLASS(DrStorageVertex) : public DrVertex
{
public:
    DrStorageVertex(DrStageManagerPtr stage, int partitionIndex, DrIInputPartitionReaderPtr reader);

    virtual void DiscardDerived() DROVERRIDE;

    virtual DrVertexRef MakeCopy(int suffix, DrStageManagerPtr stage) DROVERRIDE;

    virtual void InitializeForGraphExecution() DROVERRIDE;
    virtual void KickStateMachine() DROVERRIDE;
    virtual void RemoveFromGraphExecution() DROVERRIDE;
    virtual void ReportFinalTopology(DrVertexTopologyReporterPtr reporter) DROVERRIDE;

    virtual DrVertexOutputGeneratorPtr GetOutputGenerator(int edgeIndex, DrConnectorType type,
                                                          int version) DROVERRIDE;
    virtual DrString GetURIForWrite(int port, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) DROVERRIDE;

    virtual void ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                               DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                              DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                DrVertexOutputGeneratorPtr generator,
                                                DrVertexExecutionStatisticsPtr stats) DROVERRIDE;
    virtual void ReactToDownStreamFailure(int port, DrConnectorType type,
                                          int downStreamVersion) DROVERRIDE;
    virtual void ReactToUpStreamFailure(int port, DrConnectorType type,
                                        DrVertexOutputGeneratorPtr generator,
                                        int downStreamVersion) DROVERRIDE;
    virtual void ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                     DrVertexVersionGeneratorPtr inputs,
                                     DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                     DrErrorPtr originalReason) DROVERRIDE;
    virtual void CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction) DROVERRIDE;

	virtual void CancelAllVersions(DrErrorPtr reason) DROVERRIDE;

private:
    DrStorageVertex(DrStageManagerPtr stage, DrStorageVertexOutputGeneratorPtr generator);

    DrStorageVertexOutputGeneratorRef    m_generator;
    bool                                 m_registeredInputReady;
};
DRREF(DrStorageVertex);

typedef DrArrayList<DrStorageVertexRef> DrStorageVertexList;
DRAREF(DrStorageVertexList, DrStorageVertexRef);


DRDECLARECLASS(DrOutputVertex);
DRREF(DrOutputVertex);

DRDECLAREVALUECLASS(DrOutputPartition);
DRRREF(DrOutputPartition);

DRVALUECLASS(DrOutputPartition)
{
public:
    bool operator==(DrOutputPartitionR other);

    int             m_id;
    int             m_version;
    int             m_outputPort;
    DrResourceRef   m_resource;
    UINT64          m_size;
};

DRMAKEARRAY(DrOutputPartition);
DRMAKEARRAYLIST(DrOutputPartition);


DRINTERFACE(DrIOutputPartitionGenerator)
{
public:
    virtual void AddDynamicSplitVertex(DrOutputVertexPtr newVertex) DRABSTRACT;
    virtual HRESULT FinalizeSuccessfulParts(bool jobSuccess, DrStringR errorText) DRABSTRACT;
    virtual DrString GetURIForWrite(int partitionIndex, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) DRABSTRACT;
    virtual void AbandonVersion(int partitionIndex, int id, int version, int outputPort,
                                DrResourcePtr runningResource, bool jobSuccess) DRABSTRACT;
    virtual void ExtendLease(DrTimeInterval) DRABSTRACT;
};
DRIREF(DrIOutputPartitionGenerator);

typedef DrArrayList<DrIOutputPartitionGeneratorIRef> DrPartitionGeneratorList;
DRAREF(DrPartitionGeneratorList,DrIOutputPartitionGeneratorIRef);


DRCLASS(DrOutputVertex) : public DrVertex
{
public:
    DrOutputVertex(DrStageManagerPtr stage, int partitionIndex, DrIOutputPartitionGeneratorPtr generator);

    virtual void DiscardDerived() DROVERRIDE;

    virtual DrVertexRef MakeCopy(int suffix, DrStageManagerPtr stage) DROVERRIDE;

    DrOutputPartition FinalizeVersions(bool jobSuccess);

    virtual void InitializeForGraphExecution() DROVERRIDE;
    virtual void KickStateMachine() DROVERRIDE;
    virtual void RemoveFromGraphExecution() DROVERRIDE;
    virtual void ReportFinalTopology(DrVertexTopologyReporterPtr reporter) DROVERRIDE;

    virtual DrVertexOutputGeneratorPtr GetOutputGenerator(int edgeIndex, DrConnectorType type,
                                                          int version) DROVERRIDE;
    virtual DrString GetURIForWrite(int port, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) DROVERRIDE;

    virtual void ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                               DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                              DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                DrVertexOutputGeneratorPtr generator,
                                                DrVertexExecutionStatisticsPtr stats) DROVERRIDE;
    virtual void ReactToDownStreamFailure(int port, DrConnectorType type,
                                          int downStreamVersion) DROVERRIDE;
    virtual void ReactToUpStreamFailure(int port, DrConnectorType type,
                                        DrVertexOutputGeneratorPtr generator,
                                        int downStreamVersion) DROVERRIDE;
    virtual void ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                     DrVertexVersionGeneratorPtr inputs,
                                     DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                     DrErrorPtr originalReason) DROVERRIDE;
    virtual void CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction) DROVERRIDE;

	virtual void CancelAllVersions(DrErrorPtr reason) DROVERRIDE;

private:
    DrIOutputPartitionGeneratorIRef   m_generator;
    int                               m_partitionIndex;
    DrOutputPartitionListRef          m_runningVersion;
    DrOutputPartitionListRef          m_successfulVersion;
};

typedef DrArrayList<DrOutputVertexRef> DrOutputVertexList;
DRAREF(DrOutputVertexList,DrOutputVertexRef);


DRCLASS(DrTeeVertex) : public DrVertex
{
public:
    DrTeeVertex(DrStageManagerPtr stage);

    virtual void DiscardDerived() DROVERRIDE;

    virtual DrVertexRef MakeCopy(int suffix, DrStageManagerPtr stage) DROVERRIDE;

    virtual void InitializeForGraphExecution() DROVERRIDE;
    virtual void KickStateMachine() DROVERRIDE;
    virtual void RemoveFromGraphExecution() DROVERRIDE;
    virtual void ReportFinalTopology(DrVertexTopologyReporterPtr reporter) DROVERRIDE;

    virtual DrVertexOutputGeneratorPtr GetOutputGenerator(int edgeIndex, DrConnectorType type,
                                                          int version) DROVERRIDE;
    virtual DrString GetURIForWrite(int port, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData) DROVERRIDE;

    virtual void ReactToUpStreamRunningProcess(int inputPort, DrConnectorType type,
                                               DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamRunningVertex(int inputPort, DrConnectorType type,
                                              DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void NotifyUpStreamCompletedVertex(int inputPort, DrConnectorType type, DrVertexOutputGeneratorPtr generator) DROVERRIDE;
    virtual void ReactToUpStreamCompletedVertex(int inputPort, DrConnectorType type,
                                                DrVertexOutputGeneratorPtr generator,
                                                DrVertexExecutionStatisticsPtr stats) DROVERRIDE;
    virtual void ReactToDownStreamFailure(int port, DrConnectorType type,
                                          int downStreamVersion) DROVERRIDE;
    virtual void ReactToUpStreamFailure(int port, DrConnectorType type,
                                        DrVertexOutputGeneratorPtr generator,
                                        int downStreamVersion) DROVERRIDE;
    virtual void ReactToFailedVertex(DrVertexOutputGeneratorPtr failedGenerator,
                                     DrVertexVersionGeneratorPtr inputs,
                                     DrVertexExecutionStatisticsPtr stats, DrVertexProcessStatusPtr status,
                                     DrErrorPtr originalReason) DROVERRIDE;
    virtual void CompactPendingVersion(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction) DROVERRIDE;

	virtual void CancelAllVersions(DrErrorPtr reason) DROVERRIDE;

private:
    DrTeeVertexOutputGeneratorRef    m_generator;
};
DRREF(DrTeeVertex);
