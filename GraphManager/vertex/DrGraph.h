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

DRBASECLASS(DrFailureInfo)
{
public:
    DrFailureInfo();

    int     m_numberOfFailures;
};
DRREF(DrFailureInfo);

typedef DrDictionary<DrVertexRef, DrFailureInfoRef> DrFailureDictionary;
DRREF(DrFailureDictionary);

DRBASECLASS(DrGraphParameters)
{
public:
    DrGraphParameters();

    DrTimeInterval                m_processAbortTimeOut;
    DrTimeInterval                m_propertyUpdateInterval;
    int                           m_maxActiveFailureCount;

    int                           m_duplicateEverythingThreshold;
    DrTimeInterval                m_defaultOutlierThreshold;
    DrTimeInterval                m_minOutlierThreshold;
    double                        m_nonParametricThresholdFraction;

    int                           m_intermediateCompressionMode;

    DrProcessTemplateRef          m_defaultProcessTemplate;
    DrVertexTemplateRef           m_defaultVertexTemplate;

    DrIReporterRefListRef         m_reporters;
};
DRREF(DrGraphParameters);

/* Duplicate check timer message */
typedef int DrDuplicateChecker;

typedef DrListener<DrDuplicateChecker> DrDuplicateListener;
DRIREF(DrDuplicateListener);

typedef DrMessage<DrDuplicateChecker> DrDuplicateMessage;
DRREF(DrDuplicateMessage);

DRENUM(DrGraphState)
{
    DGS_NotStarted,
    DGS_Running,
    DGS_Stopping,
    DGS_Stopped
};

DRCLASS(DrGraph) : public DrErrorNotifier, public DrErrorListener, public DrLeaseListener, public DrDuplicateListener, public DrShutdownListener
{
public:
    DrGraph(DrClusterPtr cluster, DrGraphParametersPtr parameters);
    void Discard();

    DrClusterPtr GetCluster();
    DrGraphParametersPtr GetParameters();

    void AddStage(DrStageManagerPtr stage);
    DrStageListPtr GetStages();

    void AddPartitionGenerator(DrIOutputPartitionGeneratorPtr partitionGenerator);

    bool IsRunning();
    void StartRunning();
    void TriggerShutdown(DrErrorRef status);
    /* implements the DrErrorListener interface */
    virtual void ReceiveMessage(DrErrorRef abortError);
    /* implements the DrLeaseListener interface */
    virtual void ReceiveMessage(DrLeaseExtender leaseMessage);
    /* implements the DrDuplicateListener interface */
	virtual void ReceiveMessage(DrDuplicateChecker duplicateCheck);
    /* implements the DrShutdownListener interface */
    virtual void ReceiveMessage(DrExitStatus exitStatus);

    void IncrementActiveVertexCount();
    void DecrementActiveVertexCount();
    void NotifyActiveVertexComplete();
    void NotifyActiveVertexRevoked();

	void IncrementInFlightProcesses();
	void DecrementInFlightProcesses();

    int ReportFailure(DrActiveVertexPtr vertex, int version, DrVertexProcessStatusPtr status, DrErrorPtr error);
    void ReportStorageFailure(DrStorageVertexPtr vertex, DrErrorPtr error);

private:
	void FinalizeGraph();

	DrGraphState                  m_state;
    DrErrorRef                    m_exitStatus;

    DrClusterRef                  m_cluster;
    DrGraphParametersRef          m_parameters;

    DrStageListRef                m_stageList;
    DrPartitionGeneratorListRef   m_partitionGeneratorList;
    DrFailureDictionaryRef        m_dictionary;
    int                           m_activeVertexCount;
    int                           m_activeVertexCompleteCount;
	int                           m_inFlightProcessCount;
};
DRREF(DrGraph);
