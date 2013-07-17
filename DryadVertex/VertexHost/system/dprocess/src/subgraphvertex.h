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

#include <dryadvertex.h>
#include <vertexfactory.h>
#include <concreterchannel.h>

class RChannelFifo;
class WorkQueue;

class DryadSubGraphVertex : public DryadVertexProgramBase
{
public:
    DryadSubGraphVertex();
    virtual ~DryadSubGraphVertex();

    void Usage(FILE* f);

    void SetWorkQueueThreads(UInt32 workQueueThreads);
    void SetWorkQueueConcurrentThreads(UInt32 workQueueConcurrentThreads);
    void SetInternalFifoLength(UInt32 fifoLength);

    /* DryadVertexProgramBase interface */
    void Initialize(UInt32 numberOfInputChannels,
                    UInt32 numberOfOutputChannels);
    void MainAsync(WorkQueue* workQueue,
                   UInt32 numberOfInputChannels,
                   RChannelReader** inputChannel,
                   UInt32 numberOfOutputChannels,
                   RChannelWriter** outputChannel,
                   DryadVertexProgramCompletionHandler* handler);
    void MakeInputParser(UInt32 whichInput,
                         RChannelItemParserRef* pParser);
    void MakeOutputMarshaler(UInt32 whichOutput,
                             RChannelItemMarshalerRef* pMarshaler);
    void AsyncPostCompletion();

private:
    class EdgeInfo
    {
    public:
        EdgeInfo();
        ~EdgeInfo();

        void SetInfo(UInt32 sourceVertex, UInt32 sourcePort,
                     UInt32 destinationVertex, UInt32 destinationPort);
        UInt32 GetSourceVertex();
        UInt32 GetSourcePort();
        UInt32 GetDestinationVertex();
        UInt32 GetDestinationPort();

        void MakeFifo(UInt32 fifoLength, WorkQueue* workQueue);
        void DiscardFifo();
        RChannelReader* GetReader();
        RChannelWriter* GetWriter();

    private:
        UInt32                    m_sourceVertex;
        UInt32                    m_sourcePort;
        UInt32                    m_destinationVertex;
        UInt32                    m_destinationPort;
        RChannelReaderHolderRef   m_reader;
        RChannelWriterHolderRef   m_writer;
    };

    class VertexInfo : public DryadVertexProgramCompletionHandler
    {
    public:
        VertexInfo();
        ~VertexInfo();

        DrError MakeProgram(UInt32 numberOfWorkQueueThreads,
                            UInt32 concurrentWorkQueueThreads,
                            DryadMetaDataRef* pErrorData);
        DryadVertexProgramBase* GetVertexProgram();
        WorkQueue* GetWorkQueue();

        void SetId(UInt32 id);
        UInt32 GetId();

        void SetVersion(UInt32 id);
        UInt32 GetVersion();

        void SetInputPortCount(UInt32 inputPortCount);
        UInt32 GetInputPortCount();
        bool SetInputChannel(UInt32 inputPort, bool isExternal,
                             RChannelReader* reader);
        void SetInputEdge(UInt32 inputPort, EdgeInfo* edge);

        void SetOutputPortCount(UInt32 outputPortCount);
        UInt32 GetOutputPortCount();
        bool SetOutputChannel(UInt32 outputPort, bool isExternal,
                              RChannelWriter* writer);
        void SetOutputEdge(UInt32 outputPort,
                           EdgeInfo* edge);

        void SetArgumentCount(UInt32 outputPortCount);
        UInt32 GetArgumentCount();
        void SetArgumentValue(UInt32 argument, const char* value);
        const char* GetArgumentValue(UInt32 argument);

        void* GetRawSerializedBlock();
        UInt32 GetRawSerializedBlockLength();
        void SetRawSerializedBlock(UInt32 length,
                                   const void* data);
        
        void SetMetaData(DryadMetaData* metaData);
        DryadMetaData* GetMetaData();

        void SetCanShareWorkQueue(bool canShareWorkQueue);
        bool GetCanShareWorkQueue();

        void SetVirtual();

        bool Verify();

        void Run(DryadSubGraphVertex* parent,
                 WorkQueue* sharedWorkQueue);

        /* DryadVertexProgramCompletionHandler interface */
        void ProgramCompleted();

        void NotifyChannelsOfCompletion();
        void DrainChannels();

    private:
        DryadSubGraphVertex*             m_parent;
        bool                             m_virtual;
        DryadVertexProgramRef            m_vertexProgram;
        WorkQueue*                       m_workQueue;
        UInt32                           m_id;
        UInt32                           m_version;
        UInt32                           m_inputPortCount;
        EdgeInfo**                       m_inputEdge;
        bool*                            m_inputExternal;
        RChannelReader**                 m_inputChannel;
        UInt32                           m_outputPortCount;
        EdgeInfo**                       m_outputEdge;
        bool*                            m_outputExternal;
        RChannelWriter**                 m_outputChannel;
        UInt32                           m_argumentCount;
        DrStr64*                         m_argument;
        DryadMetaDataRef                 m_metaData;
        UInt32                           m_serializedBlockLength;
        char*                            m_serializedBlock;
        bool                             m_canShareWorkQueue;
    };

    void ReadVertexInfo(DryadMetaData* vertexData, UInt32 vertexIndex);
    void ReadVertices(DryadMetaData* graphData);
    void ReadEdgeInfo(DryadMetaData* edgeData, UInt32 edgeIndex);
    void ReadEdges(DryadMetaData* graphData);
    bool SetUpChannels(WorkQueue* sharedWorkQueue,
                       RChannelReader** inputChannel,
                       RChannelWriter** outputChannel);
    void LaunchSubGraph(WorkQueue* sharedWorkQueue);
    void VertexCompleted(DrError status,
                         DryadMetaData* errorData);
    void ShutDownFifos();


    UInt32                                m_internalWorkQueueThreads;
    UInt32                                m_internalWorkQueueConcurrentThreads;
    UInt32                                m_internalFifoLength;

    UInt32                                m_virtualInput;
    UInt32                                m_virtualOutput;

    UInt32                                m_numberOfVertices;
    VertexInfo*                           m_vertex;
    UInt32                                m_numberOfEdges;
    EdgeInfo*                             m_edge;
    UInt32                                m_numberOfInputEdges;
    EdgeInfo**                            m_inputEdge;
    UInt32                                m_numberOfOutputEdges;
    EdgeInfo**                            m_outputEdge;

    DryadVertexProgramCompletionHandler*  m_handler;
    UInt32                                m_outstandingVertices;

    CRITSEC                               m_baseDR;
    DRREFCOUNTIMPL_BASE(DryadVertexProgramBase)
};
