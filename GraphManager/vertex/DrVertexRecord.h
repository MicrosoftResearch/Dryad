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


DRENUM(DrVertexState)
{
    DVS_NotStarted,
    DVS_Starting,
    DVS_Running,
    DVS_RunningStatus,
    DVS_Completed,
    DVS_Failed
};

DRBASECLASS(DrInputChannelExecutionStatistics)
{
public:
    /* the constructor zeros all the fields */
    DrInputChannelExecutionStatistics();

    /* The data read on an input channel */
    UINT64         m_dataRead;

    /* The remote machine if any of the temp data values below is
       non-zero. Otherwise, DrNull. */ 
    DrResourceRef  m_remoteMachine;

    /* The temp data read on an input channel. This is zero if the
       channel is reading from a stable storage vertex, e.g. an input
       stream. */
    UINT64         m_tempDataRead;

    /* The temp data read across machines in the same pod. This is
       equal to m_tempDataRead if the temp data was read from
       another machine in the same pod, zero otherwise. */
    UINT64         m_tempDataReadCrossMachine;

    /* The temp data read across machines in different pods. This is
       equal to m_tempDataRead if the temp data was read from
       another machine in a different pod, zero otherwise. */
    UINT64         m_tempDataReadCrossPod;
};
DRREF(DrInputChannelExecutionStatistics);

typedef DrArray<DrInputChannelExecutionStatisticsRef> DrInputChannelStatsArray;
DRAREF(DrInputChannelStatsArray,DrInputChannelExecutionStatisticsRef);

DRBASECLASS(DrOutputChannelExecutionStatistics)
{
public:
    /* the constructor zeros all the fields */
    DrOutputChannelExecutionStatistics();

    /* zero all fields */
    void Clear();

    /* The output data written */
    UINT64       m_dataWritten;

    /* data written within pod machines */
    UINT64       m_dataIntraPod;

    /* data written across pods */
    UINT64       m_dataCrossPod;
};
DRREF(DrOutputChannelExecutionStatistics);

typedef DrArray<DrOutputChannelExecutionStatisticsRef> DrOutputChannelStatsArray;
DRAREF(DrOutputChannelStatsArray,DrOutputChannelExecutionStatisticsRef);

DRBASECLASS(DrVertexExecutionStatistics)
{
public:
    DrVertexExecutionStatistics();

    void SetNumberOfChannels(int numberOfInputs, int numberOfOutputs);

    /* The total data read on all input channels */
    DrInputChannelExecutionStatisticsRef    m_totalInputData;
    /* The total local input data read on all input channels */
    UINT64                                  m_totalLocalInputData;

    /* The total data written on all output channels */
    DrOutputChannelExecutionStatisticsRef   m_totalOutputData;



    /* the data read on each input channel broken down by channel */
    DrInputChannelStatsArrayRef             m_inputData;

    /* the data written on each output channel broken down by channel */
    DrOutputChannelStatsArrayRef            m_outputData;

    /* the time the record was created */
    DrDateTime                              m_creationTime;
    /* the time the vertex start command was sent */
    DrDateTime                              m_startTime;
    /* the time we first learned the vertex was running */
    DrDateTime                              m_runningTime;
    /* the time we first learned the vertex had finished */
    DrDateTime                              m_completionTime;

    UINT32                                  m_exitCode;
    HRESULT                                 m_exitStatus;
    DrMetaDataRef                           m_metaData;
};
DRREF(DrVertexExecutionStatistics);

DRBASECLASS(DrVertexInfo)
{
public:
    DrString                         m_name;
    DrString                         m_stageName;
    int                              m_partInStage;
    DrVertexState                    m_state;
    DrVertexStatusRef                m_info;
    DrVertexExecutionStatisticsRef   m_statistics;
    DrLockBox<DrProcess>             m_process;
};
DRREF(DrVertexInfo);

typedef DrListener<DrVertexInfoRef> DrVertexListener;
DRIREF(DrVertexListener);

DRMAKEARRAYLIST(DrVertexListenerIRef);

typedef DrMessage<DrVertexInfoRef> DrVertexMessage;
DRREF(DrVertexMessage);

typedef DrNotifier<DrVertexInfoRef> DrVertexNotifier;

DRBASECLASS(DrVertexTemplate)
{
public:
    DrVertexTemplate();

    DrVertexListenerIRefListPtr GetListenerList();

private:
    DrVertexListenerIRefListRef   m_listenerList;
};
DRREF(DrVertexTemplate);


DRBASECLASS(DrVertexVersionGenerator)
{
public:
    DrVertexVersionGenerator(int version, int numberOfInputs);

    int GetNumberOfInputs();
    void ResetVersion(int version);
    int GetVersion();

    void SetGenerator(int inputIndex, DrVertexOutputGeneratorPtr generator);
    DrVertexOutputGeneratorPtr GetGenerator(int inputIndex);
    
    bool Ready();
    void Compact(DrEdgeHolderPtr edgesBeingCompacted, int numberOfEdgesAfterCompaction);
    void Grow(int numberOfEdgesToGrow);
    
private:
    int                   m_version;
    int                   m_unfilledCount;
    DrGeneratorArrayRef   m_generator;
};
DRREF(DrVertexVersionGenerator);


DRDECLARECLASS(DrCohortProcess);
DRREF(DrCohortProcess);

DRDECLARECLASS(DrActiveVertex);
DRREF(DrActiveVertex);

DRCLASS(DrVertexRecord) : public DrVertexNotifier, public DrPropertyListener
{
public:
    DrVertexRecord(DrClusterPtr cluster, DrActiveVertexPtr parent, DrCohortProcessPtr cohort,
                   DrVertexVersionGeneratorPtr generator, DrVertexTemplatePtr vertexTemplate);

    void Discard();

    int GetVersion();
    DrActiveVertexOutputGeneratorPtr GetGenerator();

    DrVertexVersionGeneratorPtr NotifyProcessHasStarted(DrLockBox<DrProcess> process);
    void SetActiveInput(int inputPort, DrVertexOutputGeneratorPtr generator);
    void StartRunning();
    void TriggerFailure(DrErrorPtr originalReason);

    virtual void ReceiveMessage(DrPropertyStatusRef prop);

    static void SendTerminateCommand(int id, int version, DrLockBox<DrProcess> process);

private:
    DrVertexStatusRef TryToParseProperty(DrPropertyStatusPtr prop);
    DrVertexExecutionStatisticsRef MakeExecutionStatistics(DrVertexStatusPtr status,
                                                           UINT32 exitCode, HRESULT exitStatus);

    void MakeGenerator();

    void SendStartCommand();
    void RequestStatus();

    DrActiveVertexRef                  m_parent;
    DrActiveVertexOutputGeneratorRef   m_generator;
    DrVertexVersionGeneratorRef        m_inputs;
    DrVertexTemplateRef                m_vertexTemplate;
    DrCohortProcessRef                 m_cohort;
    DrLockBox<DrProcess>               m_process;

    DrVertexState                      m_state;
    UINT64                             m_lastSeenVersion;

    /* the time the record was created */
    DrDateTime                         m_creationTime;
    /* the time the vertex start command was sent */
    DrDateTime                         m_startTime;
    /* the time we first learned the vertex was running */
    DrDateTime                         m_runningTime;
    /* the time we first learned the vertex had finished */
    DrDateTime                         m_completionTime;
};
DRREF(DrVertexRecord);

typedef DrArrayList<DrVertexRecordRef> DrVertexRecordList;
DRAREF(DrVertexRecordList,DrVertexRecordRef);