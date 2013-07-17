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
class DryadVertexFactoryBase;
class DVertexProcessStatus;
class DryadChannelDescription;
class WorkQueue;

#include <DrCommon.h>
#include <errorreporter.h>
#include <channelparser.h>
#include <channelmarshaler.h>
#include <concreterchannel.h>

enum TransformType {
    TT_NullTransform = 0,
    TT_GzipCompression,
    TT_GzipFastCompression,
    TT_GzipDecompression,
    TT_DeflateCompression,
    TT_DeflateDecompression,
    TT_DeflateFastCompression

	/* Xpress removed, but left in comments as an example of 
	 * supporting an alternate compression scheme.
	TT_XpressCompression,
    TT_XpressDecompression,
    TT_XpressFastCompression
	*/
};


enum VertexAffinityConstraint {
    VAC_HardConstraint = 0,
    VAC_OptimizationConstraint,
    VAC_Preference,
    VAC_DontCare
};

class DryadVertexController
{
public:
    virtual void AssimilateNewStatus(DVertexProcessStatus* status,
                                     bool sendUpdate, bool notifyWaiters) = 0;
};

class DryadVertexBase : public IDrRefCounter
{
public:
    DryadVertexBase();
    virtual ~DryadVertexBase();

    /* initialize is called once when the process starts up */
    void Initialize(DryadVertexController* controller);


    /* PrepareDryadVertex is called by the controller before a call to
       RunDryadVertex. It will never overlap with RunDryadVertex or
       ReOpenChannels. The initial state includes the vertex id and
       version, and the initial channel information. */
    virtual void PrepareDryadVertex(DVertexProcessStatus* initialState,
                                    DryadVertexFactoryBase* factory,
                                    UInt32 argumentCount,
                                    DrStr64* argumentList,
                                    UInt32 serializedBlockLength,
                                    const void* serializedBlock) = 0;

    /* RunDryadVertex is the main body of the vertex
       execution. initialState is guaranteed to be the same object as
       was passed to the preceding call to PrepareDryadVertex. When
       RunDryadVertex returns the vertex is considered to have
       completed. If the return value is DrExitCode_StillActive then the
       controller may re-use the process for a subsequent execution
       (i.e. call PrepareDryadVertex again). The vertex may only call
       ReportStatus while it is within an executing call to
       RunDryadVertex, though it is permissible for RunDryadVertex to
       return while there is an outstanding call to ReportStatus.
     */
    virtual DrError RunDryadVertex(DVertexProcessStatus* initialState,
                                   UInt32 argumentCount,
                                   DrStr64* argumentList) = 0;

    /* ReOpenChannels may be called by the controller at any time
       after a call to PrepareDryadVertex has returned and before the
       next call to PrepareDryadVertex. */
    virtual void ReOpenChannels(DVertexProcessStatus* newChannelStatus) = 0;

    void ReportStatus(DVertexProcessStatus* status,
                      bool sendUpdate, bool notifyWaiters);

private:
    DryadVertexController*       m_controller;
};

typedef DrRef<DryadVertexBase> DryadVertexRef;

class DryadVertex : public DryadVertexBase
{
public:
    virtual ~DryadVertex();
    DRREFCOUNTIMPL
};

class DryadVertexProgramCompletionHandler
{
public:
    virtual void ProgramCompleted() = 0;
};

/* this is a container for a resource which will be copied by the PN
   to the vertex process's local working directory before it is executed */
class JobCopiedResource : public DrRefCounter
{
public:
    JobCopiedResource(const char* localResourceName,
                      const char* remoteResourceName,
                      void *content, size_t contentLen);
    ~JobCopiedResource();

    const char* GetLocalResourceName();
    const char* GetRemoteResourceName();
    const void* GetContent();
    size_t GetContentLen();
    UInt64 GetContentFingerprint();

private:
    DrStr64          m_localResourceName;
    DrStr64          m_remoteResourceName;
    // new[] array of embedded resource content, or NULL if not embedded
    unsigned char*   m_content;
    size_t           m_contentLen;
    UInt64           m_contentFingerprint;
};

typedef DrRef<JobCopiedResource> JobCopiedResourceRef;

class VertexInvocationBase : public RChannelContext
{
public:
    VertexInvocationBase();
    virtual ~VertexInvocationBase();

    /* The CommandLine is the string sent to Windows' CreateProcess
       when the vertex is remotely executed. This can be set
       explicitly at the Job Manager, though usually it is done
       automatically by a DryadJointApp which is in charge of making
       sure the executable is present remotely as a resource and
       therefore knows its name. It can be read by a running vertex,
       but is probably not of much use (and it may be NULL, e.g. for a
       subgraph vertex). */
    const char* GetCommandLine();
    void SetCommandLine(const char* commandLine);

    /* The argument list is the array of strings passed to the vertex
       in its Main routine. It can be set at the Job Manager, and is
       transmitted by the vertex Start command and deserialized when
       the vertex is created remotely, it can be read by the vertex at
       any time. argument[0] is reserved for a string which uniquely
       identifies the type of the vertex: this is set by all vertex
       factories, and is used by the remote executable to decide which
       vertex to create. */
    UInt32 GetArgumentCount();
    DrStr64* GetArgumentList();
    const char* GetArgument(UInt32 whichArgument);
    void AddArgument(const char* argument);

    /* If these are set to non-zero values, the number of
       files/streams that the vertex can hold open at any given time
       is throttled. This will lead to deadlock if a vertex blocks on
       reads or writes to more than the allowed number of channels,
       however in cases where read order is unimportant (such as
       non-deterministic merge) this will automatically block the
       first read of some channels until the last read of others has
       completed. */
    UInt32 GetMaxOpenInputChannelCount();
    void SetMaxOpenInputChannelCount(UInt32 channelCount);
    UInt32 GetMaxOpenOutputChannelCount();
    void SetMaxOpenOutputChannelCount(UInt32 channelCount);

    /* The metadata is analogous to the argument list but allows more
       complex structured data to be sent from the job manager to the
       running vertex. For example this is used by a subgraph vertex
       to serialize an arbitrary graph */
    DryadMetaData* GetMetaData();
    void SetMetaData(DryadMetaData* metaData);

    /* There is a raw serialized block containing opaque data sent
       from the job manager to the running vertex. It is not typically
       accessed directly. Instead a vertex program writer will
       override the Serialize method which is called at the job
       manager before a vertex is executed, and the DeSerialize method
       which is called at the remote process to restore the
       state. Serialize/DeSerialize are also used by the graph builder
       when cloning vertices. If an error occurs during
       deserialization the method should call ReportError to report
       it. */
    virtual void Serialize(DrMemoryBufferWriter* writer);
    virtual void DeSerialize(DrMemoryBufferReader* reader);

    /* The resources are files which the PN ensures will be available
       to the vertex when it is run remotely. They can be set
       explicitly at the Job Manager but are usually handled
       automatically by a DryadJointApp. They do not appear at the
       running vertex (GetResourceCount() will always return 0). */
    UInt32 GetResourceCount();
    JobCopiedResourceRef* GetResourceList();
    JobCopiedResource* GetResource(UInt32 whichResource);
    void AddResource(JobCopiedResource* resource);
    void AttachResource(JobCopiedResource* resource);

    /* This flag can be set at the job manager and will cause the
       specified vertex to break into the debugger on startup. It will
       always return false at the running vertex. */
    bool GetDebugBreak();
    void SetDebugBreak(bool debugBreak);

    /* This flag can be set at the job manager and will cause the
       specified vertex to simulate failure after every execution, for
       testing purposes. It will always return false at the running
       vertex. */
    bool GetFakeVertexFailure();
    void SetFakeVertexFailure(bool fakeVertexFailure);

    /* This flag can be set at the job manager and will cause the
       specified vertex to simulate failure of its inputs after every
       execution, for testing purposes. It will always return false at
       the running vertex. */
    bool GetFakeVertexInputFailure();
    void SetFakeVertexInputFailure(bool fakeVertexInputFailure);

    /* This enumeration specifies where a vertex would like to run. By
       default it is VAC_DontCare. If it is anything else, the
       location list describes which machines are required or
       preferred. */
    VertexAffinityConstraint GetAffinityConstraint();
    void SetAffinityConstraint(VertexAffinityConstraint constraint);
    std::list<std::string>* GetAffinityLocationList();

    /* This flag can be set at the job manager and will cause the
       specified vertex to allow itself to be run in a subgraph on a
       shared work queue. It is generally set automatically by the
       vertex implementation. */
    void SetCanShareWorkQueue(bool canShareWorkQueue);
    bool GetCanShareWorkQueue();

    void SetDisplayName(const char* displayName);
    const char* GetDisplayName();

    void SetCpuUsage(UInt32 cpu);
    UInt32 GetCpuUsage();

    void SetMemoryUsage(UInt64 memory);
    UInt64 GetMemoryUsage();

    void SetDiskUsage(UInt32 disk);
    UInt32 GetDiskUsage();

    /* if an output size hint vector is set, this consists of one
       number per output channel that may be used by the channel
       writing code to pre-allocate files, increasing performance and
       reducing fragmentation. If it has not been set, then
       GetOutputSizeHintVector returns NULL. If the number of outputs
       changes when the hint vector is non-NULL, there is an assertion
       failure. If a total output size hint is set, the number of
       outputs can vary and the system will estimate that each output
       will get the same amount of data. */
    UInt32 GetOutputSizeHintVectorLength() const;
    UInt64* GetOutputSizeHintVector() const;
    void SetOutputSizeHintVector(UInt32 numberOfOutputs, UInt64* hints);
    UInt64 GetOutputTotalSizeHint() const;
    void SetOutputTotalSizeHint(UInt64 hint);

    VertexInvocationBase* CloneInvocation();

private:
    DrStr64                      m_commandLine;
    UInt32                       m_argumentArraySize;
    UInt32                       m_numberOfArguments;
    DrStr64*                     m_argument;
    UInt32                       m_maxInputChannels;
    UInt32                       m_maxOutputChannels;
    DryadMetaDataRef             m_metaData;
    UInt32                       m_resourceArraySize;
    UInt32                       m_numberOfResources;
    JobCopiedResourceRef*        m_resource;
    bool                         m_debugBreak;
    bool                         m_fakeVertexFailure;
    bool                         m_fakeVertexInputFailure;;
    VertexAffinityConstraint     m_affinityConstraint;
    std::list<std::string>       m_affinityLocations;
    bool                         m_canShareWorkQueue;
    DrStr64                      m_displayName;
    UInt32                       m_cpuUsage;
    UInt64                       m_memoryUsage;
    UInt32                       m_diskUsage;
    UInt64                       m_outputTotalSizeHint;
    UInt32                       m_outputSizeHintVectorLength;
    UInt64*                      m_outputSizeHintVector;
};

class VertexInvocationRecord : public VertexInvocationBase
{
public:
    virtual ~VertexInvocationRecord();

    DRREFCOUNTIMPL
};

class DryadVertexProgramBase :
    public VertexInvocationRecord, public DVErrorReporter
{
public:
    DryadVertexProgramBase();
    virtual ~DryadVertexProgramBase();

    virtual void Usage(FILE* f);

    virtual void Initialize(UInt32 numberOfInputChannels,
                            UInt32 numberOfOutputChannels);

    UInt32 GetVertexId();
    void SetVertexId(UInt32 vertexId);

    UInt32 GetVertexVersion();
    void SetVertexVersion(UInt32 vertexVersion);

    UInt64 GetExpectedInputLength(UInt32 inputChannel);
    void SetExpectedInputLength(UInt32 numberOfChannels,
                                UInt64* expectedLength);

    void SetNumberOfParserFactories(UInt32 numberOfFactories);
    void SetParserFactory(UInt32 whichFactory,
                          DryadParserFactoryBase* factory);
    void SetCommonParserFactory(DryadParserFactoryBase* factory);
    DryadParserFactoryBase* GetCommonParserFactory();
    DryadParserFactoryBase* GetParserFactory(UInt32 whichFactory);

    void SetNumberOfMarshalerFactories(UInt32 numberOfFactories);
    void SetMarshalerFactory(UInt32 whichFactory,
                             DryadMarshalerFactoryBase* factory);
    void SetCommonMarshalerFactory(DryadMarshalerFactoryBase* factory);
    DryadMarshalerFactoryBase* GetCommonMarshalerFactory();
    DryadMarshalerFactoryBase* GetMarshalerFactory(UInt32 whichFactory);

    virtual void MakeInputParser(UInt32 whichInput,
                                 RChannelItemParserRef* pParser);

    virtual void MakeOutputMarshaler(UInt32 whichOutput,
                                     RChannelItemMarshalerRef* pMarshaler);

    /* a vertex program must override at least one of the Main or
       MainAsync methods. By default, the Main method calls MainAsync
       and waits on an event for the completion handler to be
       called. By default the MainAsync method creates a thread which
       calls Main and triggers the handler when Main completes. */
    virtual void Main(WorkQueue* workQueue,
                      UInt32 numberOfInputChannels,
                      RChannelReader** inputChannel,
                      UInt32 numberOfOutputChannels,
                      RChannelWriter** outputChannel);
    virtual void MainAsync(WorkQueue* workQueue,
                           UInt32 numberOfInputChannels,
                           RChannelReader** inputChannel,
                           UInt32 numberOfOutputChannels,
                           RChannelWriter** outputChannel,
                           DryadVertexProgramCompletionHandler* handler);
    /* After MainAsync has triggered its handler, AsyncPostCompletion
       is called to allow the vertex to do any required cleanup on the
       main calling thread (i.e. not within any handlers). The current
       status when the competion handler was called can be read using
       GetError amd GetMetaData and overridden using ReportError. */
    virtual void AsyncPostCompletion();

    void NotifyChannelsOfCompletion(UInt32 numberOfInputChannels,
                                    RChannelReader** inputChannel,
                                    UInt32 numberOfOutputChannels,
                                    RChannelWriter** outputChannel);
    void DrainChannels(UInt32 numberOfInputChannels,
                       RChannelReader** inputChannel,
                       UInt32 numberOfOutputChannels,
                       RChannelWriter** outputChannel);

private:
    class ThreadBlock
    {
    public:
        ThreadBlock(DryadVertexProgramBase* parent,
                    WorkQueue* workQueue,
                    UInt32 numberOfInputChannels,
                    RChannelReader** inputChannel,
                    UInt32 numberOfOutputChannels,
                    RChannelWriter** outputChannel,
                    DryadVertexProgramCompletionHandler* handler);
        void Run();

    private:
        DryadVertexProgramBase*                m_parent;
        WorkQueue*                             m_workQueue;
        UInt32                                 m_numberOfInputChannels;
        RChannelReader**                       m_inputChannel;
        UInt32                                 m_numberOfOutputChannels;
        RChannelWriter**                       m_outputChannel;
        DryadVertexProgramCompletionHandler*   m_handler;
    };

    class DefaultHandler : public DryadVertexProgramCompletionHandler
    {
    public:
        DefaultHandler(HANDLE completionEvent);

        void ProgramCompleted();

    private:
        HANDLE            m_completionEvent;
    };

    static unsigned MainThreadFunc(void* arg);

    UInt32                      m_vertexId;
    UInt32                      m_vertexVersion;
    UInt32                      m_expectedSizeArrayLength;
    UInt64*                     m_expectedSizeArray;
    UInt32                      m_numberOfParserFactories;
    DryadParserFactoryRef*      m_parserFactoryArray;
    UInt32                      m_numberOfMarshalerFactories;
    DryadMarshalerFactoryRef*   m_marshalerFactoryArray;
    bool                        m_defaultMainCalled;
    bool                        m_defaultMainAsyncCalled;
};

typedef DrRef<DryadVertexProgramBase> DryadVertexProgramRef;

class DryadVertexProgram : public DryadVertexProgramBase
{
public:
    virtual ~DryadVertexProgram();
    DRREFCOUNTIMPL
};

class DryadSimpleChannelVertexBase :
    public DryadVertexBase, DryadVertexProgramCompletionHandler
{
public:
    DryadSimpleChannelVertexBase();
    virtual ~DryadSimpleChannelVertexBase();

    void SetStatusInterval(DrTimeInterval interval);
    DrTimeInterval GetStatusInterval();

    void PrepareDryadVertex(DVertexProcessStatus* initialState,
                            DryadVertexFactoryBase* factory,
                            UInt32 argumentCount,
                            DrStr64* argumentList,
                            UInt32 serializedBlockLength,
                            const void* serializedBlock);
    DrError RunDryadVertex(DVertexProcessStatus* initialState,
                           UInt32 argumentCount,
                           DrStr64* argumentList);
    void ReOpenChannels(DVertexProcessStatus* newChannelStatus);

    void ProgramCompleted();

private:    
    class ReaderData
    {
    public:
        ReaderData();

        RChannelItemParserRef       m_parser;
        RChannelBufferReader*       m_bufferReader;
        RChannelReader*             m_reader;
        bool                        m_isFifo;
    };

    class WriterData
    {
    public:
        WriterData();

        RChannelItemMarshalerRef    m_marshaler;
        RChannelBufferWriter*       m_bufferWriter;
        RChannelWriter*             m_writer;
        bool                        m_isFifo;
    };

    void UpdateChannelProgress(DVertexProcessStatus* status,
                               UInt32 inputChannelCount,
                               RChannelReaderHolderRef* rData,
                               UInt32 outputChannelCount,
                               RChannelWriterHolderRef* wData);
    void RunProgram(DVertexProcessStatus* status,
                    WorkQueue* workQueue,
                    UInt32 inputChannelCount,
                    RChannelReaderHolderRef* rData,
                    UInt32 outputChannelCount,
                    RChannelWriterHolderRef* wData);

    DrError                          m_initializationError;
    HANDLE                           m_programCompleted;
    DrTimeInterval                   m_statusInterval;
public:
    DryadVertexProgramRef            m_vertexProgram;
    UInt32                           m_maxParseBatchSize;
    UInt32                           m_maxMarshalBatchSize;
};

class DryadSimpleChannelVertex : public DryadSimpleChannelVertexBase
{
public:
    virtual ~DryadSimpleChannelVertex();
    DRREFCOUNTIMPL
};

//
// Remove argentia vertex
//
#if 0
class DryadArgentiaVertexBase :
    public DryadVertexBase, DryadVertexProgramCompletionHandler
{
public:
    DryadArgentiaVertexBase();
    virtual ~DryadArgentiaVertexBase();

    void SetStatusInterval(DrTimeInterval interval);
    DrTimeInterval GetStatusInterval();

    void PrepareDryadVertex(
        DVertexProcessStatus* initialState,
        DryadVertexFactoryBase* factory,
        UInt32 argumentCount,
        DrStr64* argumentList,
        UInt32 serializedBlockLength,
        const void* serializedBlock
        );

    DrError RunDryadVertex(
        DVertexProcessStatus* initialState,
        UInt32 argumentCount,
        DrStr64* argumentList
        );

    void ReOpenChannels(DVertexProcessStatus* newChannelStatus);

    void ProgramCompleted();

private:

    DrError WriteVertexPlan(
        DVertexProcessStatus* initialState,
        UInt32 argumentCount,
        DrStr64* argumentList,
        CString & sFileName
        );

    DrError RunProgram(LPCWSTR pszVertexPlanPath, LPCWSTR pszExecutablePath);

    UInt32                           m_maxParseBatchSize;
    UInt32                           m_maxMarshalBatchSize;
    DrError                          m_initializationError;
    DryadVertexProgramRef            m_vertexProgram;
    HANDLE                           m_programCompleted;
    DrTimeInterval                   m_statusInterval;
};

class DryadArgentiaVertex : public DryadArgentiaVertexBase
{
public:
    virtual ~DryadArgentiaVertex();
    DRREFCOUNTIMPL
};
#endif
