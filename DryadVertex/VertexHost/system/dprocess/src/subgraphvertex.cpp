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

#include <DrExecution.h>
#include <subgraphvertex.h>
#include <workqueue.h>
#include <dryaderrordef.h>
#include <dryadpropertiesdef.h>
#include <dryadtagsdef.h>
#include <dvertexcommand.h>
#include <vertexfactory.h>

static const UInt32 s_invalidVertex = (UInt32) ((Int32) -1);

static const UInt32 s_internalWorkQueueThreads = 4;
static const UInt32 s_internalWorkQueueConcurrentThreads = 2;
static const UInt32 s_internalFifoLength = 20;

class SGVDummyHandler : public RChannelItemWriterHandler
{
public:
    void ProcessWriteCompleted(RChannelItemType status,
                               RChannelItem* marshalFailureItemArray);
};

void SGVDummyHandler::ProcessWriteCompleted(RChannelItemType /*status*/,
                                            RChannelItem* marshalFailureItem)
{
    if (marshalFailureItem != NULL)
    {
        marshalFailureItem->DecRef();
    }
}



DryadSubGraphVertex::EdgeInfo::EdgeInfo()
{
    m_sourceVertex = 0;
    m_sourcePort = 0;
    m_destinationVertex = 0;
    m_destinationPort = 0;
}

DryadSubGraphVertex::EdgeInfo::~EdgeInfo()
{
}

void DryadSubGraphVertex::EdgeInfo::SetInfo(UInt32 sourceVertex,
                                            UInt32 sourcePort,
                                            UInt32 destinationVertex,
                                            UInt32 destinationPort)
{
    m_sourceVertex = sourceVertex;
    m_sourcePort = sourcePort;
    m_destinationVertex = destinationVertex;
    m_destinationPort = destinationPort;
}

UInt32 DryadSubGraphVertex::EdgeInfo::GetSourceVertex()
{
    return m_sourceVertex;
}

UInt32 DryadSubGraphVertex::EdgeInfo::GetSourcePort()
{
    return m_sourcePort;
}

UInt32 DryadSubGraphVertex::EdgeInfo::GetDestinationVertex()
{
    return m_destinationVertex;
}

UInt32 DryadSubGraphVertex::EdgeInfo::GetDestinationPort()
{
    return m_destinationPort;
}

void DryadSubGraphVertex::EdgeInfo::
    MakeFifo(UInt32 fifoLength, WorkQueue* workQueue)
{
    LogAssert(m_reader == NULL);
    LogAssert(m_writer == NULL);

    UInt32 uniquifier = RChannelFactory::GetUniqueFifoId();

    DrStr64 fifoName;
    fifoName.SetF("fifo://%u/internal-%u-%u.%u--%u.%u",
                  fifoLength, uniquifier,
                  m_sourceVertex, m_sourcePort,
                  m_destinationVertex, m_destinationPort);

    DVErrorReporter errorReporter;
    RChannelFactory::OpenReader(fifoName, NULL, NULL, 1, NULL, 0, 0, workQueue,
                                &errorReporter, &m_reader, NULL);
    LogAssert(errorReporter.NoError());
    RChannelFactory::OpenWriter(fifoName, NULL, NULL, 1, NULL, 0, NULL,
                                &errorReporter, &m_writer);
    LogAssert(errorReporter.NoError());

    m_reader->GetReader()->Start(NULL);
    m_writer->GetWriter()->Start();
}

RChannelReader* DryadSubGraphVertex::EdgeInfo::GetReader()
{
    return m_reader->GetReader();
}

RChannelWriter* DryadSubGraphVertex::EdgeInfo::GetWriter()
{
    return m_writer->GetWriter();
}

void DryadSubGraphVertex::EdgeInfo::DiscardFifo()
{
    m_reader = NULL;
    m_writer = NULL;
}


DryadSubGraphVertex::VertexInfo::VertexInfo()
{
    m_parent = NULL;
    m_inputPortCount = 0;
    m_inputEdge = NULL;
    m_inputExternal = NULL;
    m_inputChannel = NULL;
    m_outputPortCount = 0;
    m_outputEdge = NULL;
    m_outputExternal = NULL;
    m_outputChannel = NULL;
    m_argumentCount = 0;
    m_argument = NULL;
    m_serializedBlockLength = 0;
    m_serializedBlock = NULL;
    m_canShareWorkQueue = false;
    m_workQueue = NULL;
    m_virtual = false;
}

DryadSubGraphVertex::VertexInfo::~VertexInfo()
{
    delete [] m_inputEdge;
    delete [] m_inputExternal;
    delete [] m_inputChannel;
    delete [] m_outputEdge;
    delete [] m_outputExternal;
    delete [] m_outputChannel;
    delete [] m_argument;
    delete [] m_serializedBlock;
    if (m_workQueue != NULL)
    {
        m_workQueue->Stop();
        delete m_workQueue;
    }
}

DrError DryadSubGraphVertex::VertexInfo::
    MakeProgram(UInt32 numberOfWorkQueueThreads,
                UInt32 concurrentWorkQueueThreads,
                DryadMetaDataRef* pErrorData)
{
    LogAssert(m_vertexProgram == NULL);

    DrError err = VertexFactoryRegistry::MakeVertex(0,
                                                    GetVersion(),
                                                    m_inputPortCount,
                                                    m_outputPortCount,
                                                    NULL,
                                                    NULL,
                                                    m_metaData,
                                                    0, 0,
                                                    m_argumentCount,
                                                    m_argument,
                                                    m_serializedBlockLength,
                                                    m_serializedBlock,
                                                    pErrorData,
                                                    &m_vertexProgram);

    if (err != DrError_OK)
    {
        return err;
    }

    DrLogI( "Made vertex program. Vertex ID %u", m_id);

    LogAssert(m_workQueue == NULL);
    if (m_canShareWorkQueue == false)
    {
        m_workQueue = new WorkQueue(numberOfWorkQueueThreads,
                                    concurrentWorkQueueThreads);
        m_workQueue->Start();

        DrLogI( "Added private work queue. Vertex ID %u", m_id);
    }
    else
    {
        DrLogI( "Using shared work queue. Vertex ID %u", m_id);
    }

    return err;
}

DryadVertexProgramBase*
     DryadSubGraphVertex::VertexInfo::GetVertexProgram()
{
    return m_vertexProgram;
}

WorkQueue* DryadSubGraphVertex::VertexInfo::GetWorkQueue()
{
    return m_workQueue;
}

void DryadSubGraphVertex::VertexInfo::
    SetInputPortCount(UInt32 inputPortCount)
{
    LogAssert(m_inputEdge == NULL);
    LogAssert(m_inputExternal == NULL);
    LogAssert(m_inputChannel == NULL);
    m_inputPortCount = inputPortCount;
    m_inputEdge = new EdgeInfo* [m_inputPortCount];
    LogAssert(m_inputEdge != NULL);
    m_inputExternal = new bool [m_inputPortCount];
    LogAssert(m_inputExternal != NULL);
    m_inputChannel = new RChannelReader* [m_inputPortCount];
    LogAssert(m_inputChannel != NULL);
    UInt32 i;
    for (i=0; i<m_inputPortCount; ++i)
    {
        m_inputEdge[i] = NULL;
        m_inputExternal[i] = false;
        m_inputChannel[i] = NULL;
    }
}

void DryadSubGraphVertex::VertexInfo::SetId(UInt32 id)
{
    m_id = id;
}

UInt32 DryadSubGraphVertex::VertexInfo::GetId()
{
    return m_id;
}

void DryadSubGraphVertex::VertexInfo::SetVersion(UInt32 version)
{
    m_version = version;
}

UInt32 DryadSubGraphVertex::VertexInfo::GetVersion()
{
    return m_version;
}

UInt32 DryadSubGraphVertex::VertexInfo::GetInputPortCount()
{
    return m_inputPortCount;
}

bool DryadSubGraphVertex::VertexInfo::
    SetInputChannel(UInt32 inputPort,
                    bool isExternal,
                    RChannelReader* reader)
{
    LogAssert(inputPort < m_inputPortCount);
    if (m_inputChannel[inputPort] == NULL)
    {
        m_inputExternal[inputPort] = isExternal;
        m_inputChannel[inputPort] = reader;
        return true;
    }
    else
    {
        return false;
    }
}

void DryadSubGraphVertex::VertexInfo::SetInputEdge(UInt32 inputPort,
                                                   EdgeInfo* edge)
{
    LogAssert(inputPort < m_inputPortCount);
    m_inputEdge[inputPort] = edge;
}

void DryadSubGraphVertex::VertexInfo::
    SetOutputPortCount(UInt32 outputPortCount)
{
    LogAssert(m_outputEdge == NULL);
    LogAssert(m_outputExternal == NULL);
    LogAssert(m_outputChannel == NULL);
    m_outputPortCount = outputPortCount;
    m_outputEdge = new EdgeInfo* [m_outputPortCount];
    LogAssert(m_outputEdge != NULL);
    m_outputExternal = new bool [m_outputPortCount];
    LogAssert(m_outputExternal != NULL);
    m_outputChannel = new RChannelWriter* [m_outputPortCount];
    LogAssert(m_outputChannel != NULL);
    UInt32 i;
    for (i=0; i<m_outputPortCount; ++i)
    {
        m_outputEdge[i] = NULL;
        m_outputExternal[i] = false;
        m_outputChannel[i] = NULL;
    }
}

UInt32 DryadSubGraphVertex::VertexInfo::GetOutputPortCount()
{
    return m_outputPortCount;
}

bool DryadSubGraphVertex::VertexInfo::
    SetOutputChannel(UInt32 outputPort,
                     bool isExternal,
                     RChannelWriter* writer)
{
    LogAssert(outputPort < m_outputPortCount);
    if (m_outputChannel[outputPort] == NULL)
    {
        m_outputExternal[outputPort] = isExternal;
        m_outputChannel[outputPort] = writer;
        return true;
    }
    else
    {
        return false;
    }
}

void DryadSubGraphVertex::VertexInfo::SetOutputEdge(UInt32 outputPort,
                                                    EdgeInfo* edge)
{
    LogAssert(outputPort < m_outputPortCount);
    m_outputEdge[outputPort] = edge;
}

void DryadSubGraphVertex::VertexInfo::
    SetArgumentCount(UInt32 argumentCount)
{
    LogAssert(m_argument == NULL);
    m_argumentCount = argumentCount;
    m_argument = new DrStr64[m_argumentCount];
}

UInt32 DryadSubGraphVertex::VertexInfo::GetArgumentCount()
{
    return m_argumentCount;
}

void DryadSubGraphVertex::VertexInfo::SetArgumentValue(UInt32 argument,
                                                       const char* value)
{
    LogAssert(argument < m_argumentCount);
    m_argument[argument].Set(value);
}

const char* DryadSubGraphVertex::VertexInfo::
    GetArgumentValue(UInt32 argument)
{
    return m_argument[argument];
}

void* DryadSubGraphVertex::VertexInfo::GetRawSerializedBlock()
{
    return m_serializedBlock;
}

UInt32 DryadSubGraphVertex::VertexInfo::GetRawSerializedBlockLength()
{
    return m_serializedBlockLength;
}

void DryadSubGraphVertex::VertexInfo::SetRawSerializedBlock(UInt32 length,
                                                            const void* data)
{
    delete [] m_serializedBlock;
    m_serializedBlockLength = length;
    m_serializedBlock = new char[m_serializedBlockLength];
    LogAssert(m_serializedBlock != NULL);
    ::memcpy(m_serializedBlock, data, m_serializedBlockLength);
}

void DryadSubGraphVertex::VertexInfo::SetMetaData(DryadMetaData* metaData)
{
    m_metaData = metaData;
}

DryadMetaData* DryadSubGraphVertex::VertexInfo::GetMetaData()
{
    return m_metaData;
}

void DryadSubGraphVertex::VertexInfo::SetCanShareWorkQueue(bool canShareWorkQueue)
{
    m_canShareWorkQueue = canShareWorkQueue;
}

bool DryadSubGraphVertex::VertexInfo::GetCanShareWorkQueue()
{
    return m_canShareWorkQueue;
}

void DryadSubGraphVertex::VertexInfo::SetVirtual()
{
    m_virtual = true;
}

bool DryadSubGraphVertex::VertexInfo::Verify()
{
    UInt32 i;

    for (i=0; i<m_inputPortCount; ++i)
    {
        if ((m_virtual && m_inputChannel[i] != NULL) ||
            (!m_virtual && m_inputChannel[i] == NULL))
        {
            return false;
        }
    }

    for (i=0; i<m_outputPortCount; ++i)
    {
        if ((m_virtual && m_outputChannel[i] != NULL) ||
            (!m_virtual && m_outputChannel[i] == NULL))
        {
            return false;
        }
    }

    for (i=0; i<m_argumentCount; ++i)
    {
        LogAssert(m_argument[i] != NULL);
    }

    return true;
}

void DryadSubGraphVertex::VertexInfo::Run(DryadSubGraphVertex* parent,
                                          WorkQueue* sharedWorkQueue)
{
    m_parent = parent;

    if (m_virtual)
    {
        DrLogI(
            "Virtual vertex completing immediately. Vertex ID %u", m_id);
        m_parent->VertexCompleted(DryadError_VertexCompleted, NULL);
    }
    else
    {
        WorkQueue* workQueue = (m_workQueue == NULL) ?
            (sharedWorkQueue) : (m_workQueue);

        const char** argv = new const char* [m_argumentCount];
        LogAssert(argv != NULL);
        UInt32 i;
        for (i=0; i<m_argumentCount; ++i)
        {
            argv[i] = m_argument[i];
        }

        DrLogI(
            "Vertex MainAsync starting. Vertex ID %u", m_id);

        m_vertexProgram->MainAsync(workQueue,
                                   m_inputPortCount,
                                   m_inputChannel,
                                   m_outputPortCount,
                                   m_outputChannel,
                                   this);

        delete [] argv;
    }
}

void DryadSubGraphVertex::VertexInfo::NotifyChannelsOfCompletion()
{
    if (m_virtual)
    {
        return;
    }

    LogAssert(m_vertexProgram != NULL);

    DrLogI(
        "Vertex telling channels about completion. Vertex ID %u status %s", m_id,
        DRERRORSTRING(m_vertexProgram->GetErrorCode()));

    m_vertexProgram->NotifyChannelsOfCompletion(m_inputPortCount,
                                                m_inputChannel,
                                                m_outputPortCount,
                                                m_outputChannel);
}

void DryadSubGraphVertex::VertexInfo::DrainChannels()
{
    if (m_virtual || m_vertexProgram == NULL)
    {
        return;
    }

    DrLogI(
        "Vertex draining channels. Vertex ID %u", m_id);

    UInt32 i;
    for (i=0; i<m_inputPortCount; ++i)
    {
        if (m_inputExternal[i])
        {
            DrLogI(
                "Vertex not draining external input. Vertex ID %u port %u", m_id, i);
        }
        else if (m_inputChannel[i] != NULL)
        {
            m_inputChannel[i]->Drain();
            DrLogI(
                "Vertex drained internal input. Vertex ID %u port %u", m_id, i);
        }
    }

    bool errorCondition = false;
    for (i=0; i<m_outputPortCount; ++i)
    {
        if (m_outputExternal[i])
        {
            DrLogI(
                "Vertex not draining external output. Vertex ID %u port %u", m_id, i);
        }
        else if (m_outputChannel[i] != NULL)
        {
            RChannelItemRef writeTermination;
            m_outputChannel[i]->Drain(DrTimeInterval_Zero, &writeTermination);
            if (writeTermination != NULL)
            {
                if (writeTermination->GetType() != RChannelItem_EndOfStream)
                {
                    DrLogI(
                        "Vertex internal output write error. Vertex ID %u port %u type %u", m_id, i,
                        writeTermination->GetType());
                    errorCondition = true;
                }
            }
            DrLogI(
                "Vertex drained internal output. Vertex ID %u port %u", m_id, i);
        }
    }

    if (errorCondition && m_vertexProgram->NoError())
    {
        m_vertexProgram->ReportError(DryadError_ChannelWriteError);
    }
}

void DryadSubGraphVertex::VertexInfo::ProgramCompleted()
{
    LogAssert(m_vertexProgram != NULL);

    DrLogI(
        "Vertex completed. Vertex ID %u, status %s", m_id,
        DRERRORSTRING(m_vertexProgram->GetErrorCode()));

    NotifyChannelsOfCompletion();

    DrError err = m_vertexProgram->GetErrorCode();
    LogAssert(err != DryadError_VertexCompleted);
    if (err == DrError_OK)
    {
        err = DryadError_VertexCompleted;
    }

    m_parent->VertexCompleted(err,
                              m_vertexProgram->GetErrorMetaData());
}

DryadSubGraphVertex::DryadSubGraphVertex()
{
    m_virtualInput = s_invalidVertex;
    m_virtualOutput = s_invalidVertex;

    m_numberOfVertices = 0;
    m_vertex = NULL;
    m_numberOfEdges = 0;
    m_edge = NULL;
    m_numberOfInputEdges = 0;
    m_inputEdge = NULL;
    m_numberOfOutputEdges = 0;
    m_outputEdge = NULL;

    m_internalWorkQueueThreads =
        s_internalWorkQueueThreads;
    m_internalWorkQueueConcurrentThreads =
        s_internalWorkQueueConcurrentThreads;
    m_internalFifoLength = s_internalFifoLength;
}

DryadSubGraphVertex::~DryadSubGraphVertex()
{
    delete [] m_edge;
    delete [] m_vertex;
    delete [] m_inputEdge;
    delete [] m_outputEdge;
}

//
// Prints out vertex identifier and warns about internal use
// todo: check if argument length >= 1 like in vertex.cpp
//
void DryadSubGraphVertex::Usage(FILE* f)
{
    fprintf(f, "%s: for internal use only\n\n", GetArgument(0));
}

void DryadSubGraphVertex::SetWorkQueueThreads(UInt32 workQueueThreads)
{
    m_internalWorkQueueThreads = workQueueThreads;
}

void DryadSubGraphVertex::
    SetWorkQueueConcurrentThreads(UInt32 workQueueConcurrentThreads)
{
    m_internalWorkQueueConcurrentThreads = workQueueConcurrentThreads;
}

void DryadSubGraphVertex::SetInternalFifoLength(UInt32 fifoLength)
{
    m_internalFifoLength = fifoLength;
}

void DryadSubGraphVertex::ReadVertexInfo(DryadMetaData* vertexData,
                                         UInt32 vertexIndex)
{
    LogAssert(NoError());

    VertexInfo* vertex = &(m_vertex[vertexIndex]);

    UInt32 id;
    if (vertexData->LookUpUInt32(Prop_Dryad_VertexId, &id) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No VertexId Tag");
        return;
    }
    vertex->SetId(id);

    UInt32 version;
    if (vertexData->LookUpUInt32(Prop_Dryad_VertexVersion,
                                 &version) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No VertexVersion Tag");
        return;
    }
    vertex->SetVersion(version);

    bool canShareWorkQueue;
    if (vertexData->LookUpBoolean(Prop_Dryad_CanShareWorkQueue,
                                  &canShareWorkQueue) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No CanShareWorkQueue Tag");
        return;
    }
    vertex->SetCanShareWorkQueue(canShareWorkQueue);

    UInt32 inputCount;
    if (vertexData->LookUpUInt32(Prop_Dryad_InputPortCount,
                                 &inputCount) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No InputPortCount Tag");
        return;
    }
    vertex->SetInputPortCount(inputCount);

    UInt32 outputCount;
    if (vertexData->LookUpUInt32(Prop_Dryad_OutputPortCount,
                                 &outputCount) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No OutputPortCount Tag");
        return;
    }
    vertex->SetOutputPortCount(outputCount);

    DryadMetaDataRef metaData;
    if (vertexData->LookUpMetaData(DryadTag_VertexMetaData,
                                   &metaData) == DrError_OK)
    {
        vertex->SetMetaData(metaData);
    }

    UInt32 argc;
    if (vertexData->LookUpUInt32(Prop_Dryad_VertexArgumentCount,
                                 &argc) != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No ArgumentCount Tag");
        return;
    }
    vertex->SetArgumentCount(argc);

    DryadMetaDataRef argvArray;
    DrError err = vertexData->LookUpMetaData(DryadTag_ArgumentArray,
                                             &argvArray);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No ArgumentArray Tag");
        return;
    }

    DryadMTagUnknown* uTag =
        vertexData->LookUpUnknownTag(Prop_Dryad_VertexSerializedBlock);
    if (uTag == NULL)
    {
        ReportError(DryadError_VertexInitialization,
                    "No VertexSerializedBlock Tag");
        return;
    }
    else
    {
        vertex->SetRawSerializedBlock(uTag->GetDataLength(), uTag->GetData());
    }

    DryadMetaData::TagListIter endArray;
    DryadMetaData::TagListIter iter =
        argvArray->LookUpInSequence(NULL, &endArray);
    UInt32 argIndex = 0;
    while (iter != endArray)
    {
        if (argIndex == vertex->GetArgumentCount())
        {
            ReportError(DryadError_VertexInitialization,
                        "Too many arguments in array");
            return;
        }
        DryadMTag* tag = *iter;
        if (tag->GetTagValue() != Prop_Dryad_VertexArgument)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag in array");
            return;
        }
        if (tag->GetType() != DrPropertyTagType_String)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag type in array");
            return;
        }
        DryadMTagString* argString = (DryadMTagString *) tag;
        vertex->SetArgumentValue(argIndex, argString->GetString());
        ++iter;
        ++argIndex;
    }

    if (argIndex != vertex->GetArgumentCount())
    {
        ReportError(DryadError_VertexInitialization,
                    "Too few arguments in array");
        return;
    }

    if (vertex->GetArgumentCount() > 0 &&
        ::strcmp(vertex->GetArgumentValue(0), "__INPUT__") == 0)
    {
        if (m_virtualInput != s_invalidVertex)
        {
            ReportError(DryadError_VertexInitialization,
                        "Multiple virtual input vertices");
            return;
        }

        if (vertex->GetInputPortCount() != 0)
        {
            ReportError(DryadError_VertexInitialization,
                        "Virtual input has input channels");
            return;
        }

        if (vertex->GetArgumentCount() != 1)
        {
            ReportError(DryadError_VertexInitialization,
                        "Virtual input has extra arguments");
            return;
        }

        m_virtualInput = vertexIndex;
        m_numberOfInputEdges = vertex->GetOutputPortCount();
        m_inputEdge = new EdgeInfo* [m_numberOfInputEdges];
        LogAssert(m_inputEdge != NULL);
        UInt32 i;
        for (i=0; i<m_numberOfInputEdges; ++i)
        {
            m_inputEdge[i] = NULL;
        }
        vertex->SetVirtual();
    }
    else if (vertex->GetArgumentCount() > 0 &&
             ::strcmp(vertex->GetArgumentValue(0), "__OUTPUT__") == 0)
    {
        if (m_virtualOutput != s_invalidVertex)
        {
            ReportError(DryadError_VertexInitialization,
                        "Multiple virtual output vertices");
            return;
        }

        if (vertex->GetOutputPortCount() != 0)
        {
            ReportError(DryadError_VertexInitialization,
                        "Virtual output has output channels");
            return;
        }

        if (vertex->GetArgumentCount() != 1)
        {
            ReportError(DryadError_VertexInitialization,
                        "Virtual output has extra arguments");
            return;
        }

        m_virtualOutput = vertexIndex;
        m_numberOfOutputEdges = vertex->GetInputPortCount();
        m_outputEdge = new EdgeInfo* [m_numberOfOutputEdges];
        UInt32 i;
        for (i=0; i<m_numberOfOutputEdges; ++i)
        {
            m_outputEdge[i] = NULL;
        }
        vertex->SetVirtual();
    }
    else
    {
        DryadMetaDataRef errorData;
        err = vertex->MakeProgram(m_internalWorkQueueThreads,
                                  m_internalWorkQueueConcurrentThreads,
                                  &errorData);
        ReportError(err, errorData);
    }
}

void DryadSubGraphVertex::ReadVertices(DryadMetaData* graphData)
{
    LogAssert(m_virtualInput == s_invalidVertex);
    LogAssert(m_virtualOutput == s_invalidVertex);

    LogAssert(NoError());

    DrError err = graphData->LookUpUInt32(Prop_Dryad_NumberOfVertices,
                                          &m_numberOfVertices);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No NumberOfVertices Tag");
        return;
    }

    m_vertex = new VertexInfo [m_numberOfVertices];
    LogAssert(m_vertex != NULL);

    DryadMetaDataRef vertexArray;
    err = graphData->LookUpMetaData(DryadTag_VertexArray, &vertexArray);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No VertexArray Tag");
        return;
    }

    DryadMetaData::TagListIter endArray;
    DryadMetaData::TagListIter iter =
        vertexArray->LookUpInSequence(NULL, &endArray);
    UInt32 vertexIndex = 0;
    while (iter != endArray)
    {
        if (vertexIndex == m_numberOfVertices)
        {
            ReportError(DryadError_VertexInitialization,
                        "Too many vertices in array");
            return;
        }
        DryadMTag* tag = *iter;
        if (tag->GetTagValue() != DryadTag_VertexInfo)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag in array");
            return;
        }
        if (tag->GetType() != DryadPropertyTagType_MetaData)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag type in array");
            return;
        }
        DryadMTagMetaData* vertexData = (DryadMTagMetaData *) tag;
        ReadVertexInfo(vertexData->GetMetaData(), vertexIndex);

        if (NoError() == false)
        {
            return;
        }

        ++iter;
        ++vertexIndex;
    }

    if (vertexIndex != m_numberOfVertices)
    {
        ReportError(DryadError_VertexInitialization,
                    "Too few vertices in array");
        return;
    }

    if (m_virtualInput == s_invalidVertex)
    {
        ReportError(DryadError_VertexInitialization,
                    "No virtual input vertex");
        return;
    }

    if (m_virtualOutput == s_invalidVertex)
    {
        ReportError(DryadError_VertexInitialization,
                    "No virtual output vertex");
        return;
    }

    LogAssert(NoError());
}

void DryadSubGraphVertex::ReadEdgeInfo(DryadMetaData* edgeData,
                                       UInt32 edgeIndex)
{
    EdgeInfo* edge = &(m_edge[edgeIndex]);

    LogAssert(NoError());

    UInt32 sourceVertex;
    DrError err = edgeData->LookUpUInt32(Prop_Dryad_SourceVertex,
                                         &sourceVertex);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No SourceVertex Tag");
        return;
    }

    if (sourceVertex >= m_numberOfVertices)
    {
        ReportError(DryadError_VertexInitialization,
                    "Bad Source Vertex Index");
        return;
    }

    UInt32 sourcePort;
    err = edgeData->LookUpUInt32(Prop_Dryad_SourcePort,
                                 &sourcePort);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No SourcePort Tag");
        return;
    }

    if (sourcePort >= m_vertex[sourceVertex].GetOutputPortCount())
    {
        ReportError(DryadError_VertexInitialization, "Bad Source Port Index");
        return;
    }

    if (sourceVertex == m_virtualInput)
    {
        LogAssert(sourcePort < m_numberOfInputEdges);
        if (m_inputEdge[sourcePort] != NULL)
        {
            ReportError(DryadError_VertexInitialization,
                        "Duplicate InputEdge Port");
            return;
        }
        m_inputEdge[sourcePort] = edge;
    }

    UInt32 destinationVertex;
    err = edgeData->LookUpUInt32(Prop_Dryad_DestinationVertex,
                                 &destinationVertex);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No DestinationVertex Tag");
        return;
    }

    if (destinationVertex >= m_numberOfVertices)
    {
        ReportError(DryadError_VertexInitialization,
                    "Bad Destination Vertex Index");
        return;
    }

    UInt32 destinationPort;
    err = edgeData->LookUpUInt32(Prop_Dryad_DestinationPort,
                                 &destinationPort);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No DestinationPort Tag");
        return;
    }

    if (destinationPort >= m_vertex[destinationVertex].GetInputPortCount())
    {
        ReportError(DryadError_VertexInitialization,
                    "Bad Destination Port Index");
        return;
    }

    if (destinationVertex == m_virtualOutput)
    {
        LogAssert(destinationPort < m_numberOfOutputEdges);
        if (m_outputEdge[destinationPort] != NULL)
        {
            ReportError(DryadError_VertexInitialization,
                        "Duplicate OutputEdge Port");
            return;
        }
        m_outputEdge[destinationPort] = edge;
    }

    edge->SetInfo(sourceVertex, sourcePort,
                  destinationVertex, destinationPort);

    LogAssert(NoError());
}

void DryadSubGraphVertex::ReadEdges(DryadMetaData* graphData)
{
    LogAssert(NoError());

    DrError err = graphData->LookUpUInt32(Prop_Dryad_NumberOfEdges,
                                          &m_numberOfEdges);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No NumberOfEdges Tag");
        return;
    }

    m_edge = new EdgeInfo [m_numberOfEdges];
    LogAssert(m_edge != NULL);

    DryadMetaDataRef edgeArray;
    err = graphData->LookUpMetaData(DryadTag_EdgeArray, &edgeArray);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization, "No EdgeArray Tag");
        return;
    }

    DryadMetaData::TagListIter endArray;
    DryadMetaData::TagListIter iter =
        edgeArray->LookUpInSequence(NULL, &endArray);
    UInt32 edgeIndex = 0;
    while (iter != endArray)
    {
        if (edgeIndex == m_numberOfEdges)
        {
            ReportError(DryadError_VertexInitialization,
                        "Too many edges in array");
            return;
        }
        DryadMTag* tag = *iter;
        if (tag->GetTagValue() != DryadTag_EdgeInfo)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag in array");
            return;
        }
        if (tag->GetType() != DryadPropertyTagType_MetaData)
        {
            ReportError(DryadError_VertexInitialization,
                        "Unexpected tag type in array");
            return;
        }
        DryadMTagMetaData* edgeData = (DryadMTagMetaData *) tag;
        ReadEdgeInfo(edgeData->GetMetaData(), edgeIndex);

        if (NoError() == false)
        {
            return;
        }
        ++iter;
        ++edgeIndex;
    }

    if (edgeIndex != m_numberOfEdges)
    {
        ReportError(DryadError_VertexInitialization,
                    "Too few edges in array");
        return;
    }

    LogAssert(NoError());
}

void DryadSubGraphVertex::Initialize(UInt32 numberOfInputChannels,
                                     UInt32 numberOfOutputChannels)
{
    DrError err;

    DryadMetaData* metaData = GetMetaData();

    if (metaData == NULL)
    {
        ReportError(DryadError_VertexInitialization,
                    "No MetaData In Start Command");
        return;
    }

    DryadMetaDataRef graphData;
    err = metaData->LookUpMetaData(DryadTag_GraphDescription, &graphData);
    if (err != DrError_OK)
    {
        ReportError(DryadError_VertexInitialization,
                    "No GraphDescription Tag");
        return;
    }

    ReadVertices(graphData);

    if (NoError() == false)
    {
        return;
    }

    ReadEdges(graphData);
}

void DryadSubGraphVertex::MakeInputParser(UInt32 whichInput,
                                          RChannelItemParserRef* pParser)
{
    if (m_virtualInput == s_invalidVertex)
    {
        ReportError(DryadError_VertexInitialization,
                    "MakeInputParser called before initialization");
        return;
    }

    if (whichInput >= m_numberOfInputEdges)
    {
        ReportError(DryadError_VertexInitialization,
                    "MakeInputParser called with illegal port");
        return;
    }

    EdgeInfo* edge = m_inputEdge[whichInput];
    LogAssert(edge->GetSourceVertex() == m_virtualInput);
    LogAssert(edge->GetSourcePort() == whichInput);

    UInt32 inputVertex = edge->GetDestinationVertex();
    LogAssert(inputVertex < m_numberOfVertices);

    VertexInfo* input = &(m_vertex[inputVertex]);
    UInt32 inputPort = edge->GetDestinationPort();
    LogAssert(inputPort < input->GetInputPortCount());

    DryadVertexProgramBase* subProgram = input->GetVertexProgram();

    subProgram->MakeInputParser(inputPort, pParser);

    ReportError(subProgram->GetErrorCode(),
                subProgram->GetErrorMetaData());
}

void DryadSubGraphVertex::
    MakeOutputMarshaler(UInt32 whichOutput,
                        RChannelItemMarshalerRef* pMarshaler)
{
    if (m_virtualOutput == s_invalidVertex)
    {
        ReportError(DryadError_VertexInitialization,
                    "MakeOutputMarshaler called before initialization");
        return;
    }

    if (whichOutput >= m_numberOfOutputEdges)
    {
        ReportError(DryadError_VertexInitialization,
                    "MakeOutputMarshaler called will illegal port");
        return;
    }

    EdgeInfo* edge = m_outputEdge[whichOutput];
    LogAssert(edge->GetDestinationVertex() == m_virtualOutput);
    LogAssert(edge->GetDestinationPort() == whichOutput);

    UInt32 outputVertex = edge->GetSourceVertex();
    LogAssert(outputVertex < m_numberOfVertices);

    VertexInfo* output = &(m_vertex[outputVertex]);
    UInt32 outputPort = edge->GetSourcePort();
    LogAssert(outputPort < output->GetOutputPortCount());

    DryadVertexProgramBase* subProgram = output->GetVertexProgram();

    subProgram->MakeOutputMarshaler(outputPort, pMarshaler);

    ReportError(subProgram->GetErrorCode(),
                subProgram->GetErrorMetaData());
}

bool DryadSubGraphVertex::SetUpChannels(WorkQueue* sharedWorkQueue,
                                        RChannelReader** inputChannel,
                                        RChannelWriter** outputChannel)
{
    UInt32 i;
    for (i=0; i<m_numberOfEdges; ++i)
    {
        EdgeInfo* edge = &(m_edge[i]);
        UInt32 srcVertex = edge->GetSourceVertex();
        UInt32 dstVertex = edge->GetDestinationVertex();
        UInt32 srcPort = edge->GetSourcePort();
        UInt32 dstPort = edge->GetDestinationPort();

        m_vertex[dstVertex].SetInputEdge(dstPort, edge);
        m_vertex[srcVertex].SetOutputEdge(srcPort, edge);

        if (srcVertex == m_virtualInput)
        {
            LogAssert(dstVertex != m_virtualOutput);
            m_vertex[dstVertex].SetInputChannel(dstPort, true,
                                                inputChannel[srcPort]);
        }
        else if (dstVertex == m_virtualOutput)
        {
            m_vertex[srcVertex].SetOutputChannel(srcPort, true,
                                                 outputChannel[dstPort]);
        }
        else
        {
            WorkQueue* workQueue = m_vertex[dstVertex].GetWorkQueue();
            if (workQueue == NULL)
            {
                workQueue = sharedWorkQueue;
            }
            edge->MakeFifo(m_internalFifoLength, workQueue);
            m_vertex[dstVertex].SetInputChannel(dstPort, false,
                                                edge->GetReader());
            m_vertex[srcVertex].SetOutputChannel(srcPort, false,
                                                 edge->GetWriter());
        }
    }

    for (i=0; i<m_numberOfVertices; ++i)
    {
        if (!m_vertex[i].Verify())
        {
            return false;
        }
    }

    return true;
}

void DryadSubGraphVertex::LaunchSubGraph(WorkQueue* sharedWorkQueue)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        m_outstandingVertices = m_numberOfVertices;
    }

    UInt32 i;
    /* get this on the stack since we can be deleted by another thread
       as soon as we have launched the last vertex */
    UInt32 numberOfVertices = m_numberOfVertices;
    for (i=0; i<numberOfVertices; ++i)
    {
        m_vertex[i].Run(this, sharedWorkQueue);
    }
}

void DryadSubGraphVertex::ShutDownFifos()
{
    UInt32 i;
    for (i=0; i<m_numberOfVertices; ++i)
    {
        m_vertex[i].DrainChannels();
    }

    for (i=0; i<m_numberOfEdges; ++i)
    {
        m_edge[i].DiscardFifo();
    }
}

void DryadSubGraphVertex::VertexCompleted(DrError status,
                                          DryadMetaData* errorData)
{
    bool finished = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(GetErrorCode() != DryadError_VertexCompleted);
        if (status == DryadError_VertexCompleted)
        {
            status = DrError_OK;
        }

        if (NoError())
        {
            ReportError(status, errorData);
        }

        LogAssert(m_outstandingVertices > 0);
        --m_outstandingVertices;

        if (m_outstandingVertices == 0)
        {
            finished = true;
        }
    }

    if (finished)
    {
        /* the real status will be returned in AsyncPostCompletion
           after we have exited all handlers. */
        m_handler->ProgramCompleted();
    }
}

void DryadSubGraphVertex::
    MainAsync(WorkQueue* workQueue,
              UInt32 numberOfInputChannels,
              RChannelReader** inputChannel,
              UInt32 numberOfOutputChannels,
              RChannelWriter** outputChannel,
              DryadVertexProgramCompletionHandler* handler)
{
    m_handler = handler;

    if (m_virtualInput == s_invalidVertex ||
        m_virtualOutput == s_invalidVertex)
    {
        ReportError(DryadError_VertexInitialization,
                    "MainAsync called before initialization");
        m_handler->ProgramCompleted();
        return;
    }

    VertexInfo* virtualInput = &(m_vertex[m_virtualInput]);
    if (virtualInput->GetOutputPortCount() != numberOfInputChannels)
    {
        ReportError(DryadError_VertexInitialization,
                    "Wrong number of input channels for subgraph");
        m_handler->ProgramCompleted();
        return;
    }

    VertexInfo* virtualOutput = &(m_vertex[m_virtualOutput]);
    if (virtualOutput->GetInputPortCount() != numberOfOutputChannels)
    {
        ReportError(DryadError_VertexInitialization,
                    "Wrong number of output channels for subgraph");
        m_handler->ProgramCompleted();
        return;
    }

    if (SetUpChannels(workQueue, inputChannel, outputChannel) == false)
    {
        ReportError(DryadError_VertexInitialization,
                    "Missing edge or bad virtual vertices in subgraph");
        /* the real status will be returned in AsyncPostCompletion
           after we have exited all handlers. */
        m_handler->ProgramCompleted();
        return;
    }

    LaunchSubGraph(workQueue);
}

void DryadSubGraphVertex::AsyncPostCompletion()
{
    ShutDownFifos();

    if (GetErrorCode() == DrError_OK)
    {
        ReportError(DryadError_VertexCompleted);
    }
}

static StdTypedVertexFactory<DryadSubGraphVertex>
    s_subgraphFactory("Dryad.Core.Subgraph");

DryadVertexFactoryBase* g_subgraphFactory = &s_subgraphFactory;
