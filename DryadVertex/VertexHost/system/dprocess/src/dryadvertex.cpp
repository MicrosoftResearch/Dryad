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

//
// Includes
//
#include <process.h>
#include "DrExecution.h"
#include "dryadvertex.h"
#include "dvertexcommand.h"
#include "dvertexpncontrol.h"
#include "workqueue.h"
#include "concreterchannel.h"
#include "dryadmetadata.h"
#include "dryaderrordef.h"
#include "vertexfactory.h"
#include "subgraphvertex.h"
#include "fingerprint.h"

#pragma managed

//
// Use a short update period since the consumer polls at its own rate, so this only generates local traffic
//
static const DrTimeInterval s_defaultStatusInterval = DrTimeInterval_Millisecond * 500;

static TransformType StripCompressionModeFromUri(char *uri)
{
    TransformType mode = TT_NullTransform;
    char *start = strstr(uri, "?c=");
    if (start != NULL)
    {
        int modeInt = atoi(start + 3);
        switch (modeInt)
        {
        // Microsoft.Hpc.Dsc.Internal.DscCompressionSchemeInternal.None
        case 0:
            mode = TT_NullTransform;
            break;
        // Microsoft.Hpc.Dsc.Internal.DscCompressionSchemeInternal.Gzip
        case 1:
            mode = TT_GzipFastCompression;
            break;
        default:
            DrLogA("Invalid compression scheme %d specified in URI: %s", modeInt, uri);
            break;
        }
        *start = 0;
    }

    return mode;
}

//
// Constructor. No associated controller
//
DryadVertexBase::DryadVertexBase()
{
    m_controller = NULL;
}

//
// Destructor. Does nothing.
//
DryadVertexBase::~DryadVertexBase()
{
}

//
// Associate controller with vertex
//
void DryadVertexBase::Initialize(DryadVertexController* controller)
{
    LogAssert(controller != NULL);

    m_controller = controller;
}

void DryadVertexBase::ReportStatus(DVertexProcessStatus* status,
                                   bool sendUpdate, bool notifyWaiters)
{
    LogAssert(m_controller != NULL);

    m_controller->AssimilateNewStatus(status, sendUpdate, notifyWaiters);
}

//
// Destructor. Does Nothing.
//
DryadVertex::~DryadVertex()
{
}


JobCopiedResource::JobCopiedResource(const char* localResourceName,
                                     const char* remoteResourceName,
                                     void *content,
                                     size_t contentLen)
{
    LogAssert(localResourceName != NULL);
    m_localResourceName.Set(localResourceName);
    m_remoteResourceName.Set(remoteResourceName);
    m_content = (unsigned char *) content;
    m_contentLen = contentLen;
    if (m_content == NULL)
    {
        LogAssert(m_contentLen == 0);
        m_contentFingerprint = 0;
    }
    else
    {
        m_contentFingerprint = FingerPrint64::GetInstance()->
            GetFingerPrint(m_content, m_contentLen);
    }
}

JobCopiedResource::~JobCopiedResource()
{
    delete [] m_content;
}

const char* JobCopiedResource::GetLocalResourceName()
{
    return m_localResourceName;
}

const char* JobCopiedResource::GetRemoteResourceName()
{
    return m_remoteResourceName;
}

const void* JobCopiedResource::GetContent()
{
    return m_content;
}
    
size_t JobCopiedResource::GetContentLen()
{
    return m_contentLen;
}

UInt64 JobCopiedResource::GetContentFingerprint()
{
    return m_contentFingerprint;
}


VertexInvocationBase::VertexInvocationBase()
{
    m_argumentArraySize = 8;
    m_argument = new DrStr64[m_argumentArraySize];
    m_numberOfArguments = 0;

    m_maxInputChannels = 0;
    m_maxOutputChannels = 0;

    m_resourceArraySize = 8;
    m_resource = new JobCopiedResourceRef[m_resourceArraySize];
    m_numberOfResources = 0;

    m_debugBreak = false;
    m_fakeVertexFailure = false;
    m_fakeVertexInputFailure = false;

    m_affinityConstraint = VAC_DontCare;

    m_canShareWorkQueue = false;

    m_cpuUsage = MAX_UINT32;
    m_memoryUsage = MAX_UINT64;
    m_diskUsage = MAX_UINT32;

    m_outputTotalSizeHint = 0;
    m_outputSizeHintVectorLength = 0;
    m_outputSizeHintVector = NULL;
}

VertexInvocationBase::~VertexInvocationBase()
{
    delete [] m_argument;
    delete [] m_resource;
    delete [] m_outputSizeHintVector;
}

const char* VertexInvocationBase::GetCommandLine()
{
    return m_commandLine;
}

void VertexInvocationBase::SetCommandLine(const char* commandLine)
{
    m_commandLine = commandLine;
}

UInt32 VertexInvocationBase::GetMaxOpenInputChannelCount()
{
    return m_maxInputChannels;
}

void VertexInvocationBase::SetMaxOpenInputChannelCount(UInt32 channelCount)
{
    m_maxInputChannels = channelCount;
}

UInt32 VertexInvocationBase::GetMaxOpenOutputChannelCount()
{
    return m_maxOutputChannels;
}

void VertexInvocationBase::SetMaxOpenOutputChannelCount(UInt32 channelCount)
{
    m_maxOutputChannels = channelCount;
}

UInt32 VertexInvocationBase::GetArgumentCount()
{
    return m_numberOfArguments;
}

DrStr64* VertexInvocationBase::GetArgumentList()
{
    return m_argument;
}

//
// Return an argument if in valid range
//
const char* VertexInvocationBase::GetArgument(UInt32 whichArgument)
{
    LogAssert(whichArgument < m_numberOfArguments);
    return m_argument[whichArgument];
}

//
// Add an argument to a vertex
//
void VertexInvocationBase::AddArgument(const char* argument)
{
    //
    // If argument list is full, grow it by factor of two
    //
    if (m_numberOfArguments == m_argumentArraySize)
    {
        //
        // Create new array twice as big
        //
        m_argumentArraySize *= 2;
        DrStr64* newArray = new DrStr64[m_argumentArraySize];
        LogAssert(newArray != NULL);
        
        //
        // Copy each element in old array into new array
        //
        UInt32 i;
        for (i=0; i<m_numberOfArguments; ++i)
        {
            newArray[i] = m_argument[i];
        }
        
        //
        // Clean up the old array
        //
        delete [] m_argument;
        m_argument = newArray;
    }

    //
    // Add new argument to list
    //
    LogAssert(m_numberOfArguments < m_argumentArraySize);
    m_argument[m_numberOfArguments] = argument;
    ++m_numberOfArguments;
}

//
// Return the metadata associated with a vertex
//
DryadMetaData* VertexInvocationBase::GetMetaData()
{
    return m_metaData;
}

//
// Update the metadata associated with a vertex
//
void VertexInvocationBase::SetMetaData(DryadMetaData* metaData)
{
    m_metaData = metaData;
}

//
// Define base behavior for serialization (none)
//
void VertexInvocationBase::Serialize(DrMemoryBufferWriter* writer)
{
}

//
// Define base behavior for deserialization (none)
//
void VertexInvocationBase::DeSerialize(DrMemoryBufferReader* reader)
{
}

//
// Get the number of resources this vertex has
//
UInt32 VertexInvocationBase::GetResourceCount()
{
    return m_numberOfResources;
}

//
// Get the list of resources this vertex has
//
JobCopiedResourceRef* VertexInvocationBase::GetResourceList()
{
    return m_resource;
}

JobCopiedResource* VertexInvocationBase::GetResource(UInt32 whichResource)
{
    LogAssert(whichResource < m_numberOfResources);
    return m_resource[whichResource];
}

void VertexInvocationBase::AddResource(JobCopiedResource* resource)
{
    const char *localResourceName;

    if (m_numberOfResources == m_resourceArraySize)
    {
        m_resourceArraySize *= 2;
        JobCopiedResourceRef* newArray =
            new JobCopiedResourceRef[m_resourceArraySize];
        LogAssert(newArray != NULL);
        UInt32 i;
        for (i=0; i<m_numberOfResources; ++i)
        {
            newArray[i] = m_resource[i];
        }
        delete [] m_resource;
        m_resource = newArray;
    }

    LogAssert(m_numberOfResources < m_resourceArraySize);

    // Check for duplicates
    localResourceName = resource->GetLocalResourceName();
    for(UInt32 i = 0; i < m_numberOfResources; ++i)
    {
        if (m_resource[i] == NULL)
            continue;
        if(_stricmp(m_resource[i]->GetLocalResourceName(), localResourceName) == 0)
        {
            if((m_resource[i]->GetContentLen() != resource->GetContentLen()) ||
               (m_resource[i]->GetContentFingerprint() != resource->GetContentFingerprint()) ||
               (memcmp(m_resource[i]->GetContent(), resource->GetContent(), resource->GetContentLen()) != 0))
            {
                DrLogE(  "Existing resource", "RemoteName=%s, LocalName=%s, Len=%u", m_resource[i]->GetRemoteResourceName(), m_resource[i]->GetLocalResourceName(), m_resource[i]->GetContentLen());
                DrLogE(  "New resource", "RemoteName=%s, LocalName=%s, Len=%u", resource->GetRemoteResourceName(), resource->GetLocalResourceName(), resource->GetContentLen());
                DrLogA( "Duplicated embedded resource with different content");
            }
            else
            {
                DrLogW( "Duplicated embedded resource ignored", "RemoteName=%s, LocalName=%s, Len=%u", resource->GetRemoteResourceName(), resource->GetLocalResourceName(), resource->GetContentLen());
                // Resource will be free'd automatically
                return;
            }
        }
    }
    m_resource[m_numberOfResources] = resource;
    ++m_numberOfResources;
}

void VertexInvocationBase::AttachResource(JobCopiedResource* resource)
{
    AddResource(resource);
    resource->DecRef();
}

bool VertexInvocationBase::GetDebugBreak()
{
    return m_debugBreak;
}

void VertexInvocationBase::SetDebugBreak(bool debugBreak)
{
    m_debugBreak = debugBreak;
}

bool VertexInvocationBase::GetFakeVertexFailure()
{
    return m_fakeVertexFailure;
}

void VertexInvocationBase::SetFakeVertexFailure(bool fakeVertexFailure)
{
    m_fakeVertexFailure = fakeVertexFailure;
}

bool VertexInvocationBase::GetFakeVertexInputFailure()
{
    return m_fakeVertexInputFailure;
}

void VertexInvocationBase::SetFakeVertexInputFailure(bool fakeVertexFailure)
{
    m_fakeVertexInputFailure = fakeVertexFailure;
}

void VertexInvocationBase::
    SetAffinityConstraint(VertexAffinityConstraint constraint)
{
    m_affinityConstraint = constraint;
}

VertexAffinityConstraint VertexInvocationBase::GetAffinityConstraint()
{
    return m_affinityConstraint;
}

std::list<std::string>* VertexInvocationBase::GetAffinityLocationList()
{
    return &m_affinityLocations;
}

void VertexInvocationBase::SetCanShareWorkQueue(bool canShareWorkQueue)
{
    m_canShareWorkQueue = canShareWorkQueue;
}

bool VertexInvocationBase::GetCanShareWorkQueue()
{
    return m_canShareWorkQueue;
}

void VertexInvocationBase::SetDisplayName(const char* displayName)
{
    m_displayName = displayName;
}

const char* VertexInvocationBase::GetDisplayName()
{
    return m_displayName;
}

void VertexInvocationBase::SetCpuUsage(UInt32 cpu)
{
    m_cpuUsage = cpu;
}

UInt32 VertexInvocationBase::GetCpuUsage()
{
    return m_cpuUsage;
}

void VertexInvocationBase::SetMemoryUsage(UInt64 memory)
{
    m_memoryUsage = memory;
}

UInt64 VertexInvocationBase::GetMemoryUsage()
{
    return m_memoryUsage;
}

void VertexInvocationBase::SetDiskUsage(UInt32 disk)
{
    m_diskUsage = disk;
}

UInt32 VertexInvocationBase::GetDiskUsage()
{
    return m_diskUsage;
}

UInt64 VertexInvocationBase::GetOutputTotalSizeHint() const
{
    return m_outputTotalSizeHint;
}

void VertexInvocationBase::SetOutputTotalSizeHint(UInt64 hint)
{
    if (hint != 0)
    {
        LogAssert(m_outputSizeHintVector == NULL);
    }
    m_outputTotalSizeHint = hint;
}

UInt32 VertexInvocationBase::GetOutputSizeHintVectorLength() const
{
    return m_outputSizeHintVectorLength;
}

UInt64* VertexInvocationBase::GetOutputSizeHintVector() const
{
    return m_outputSizeHintVector;
}

void VertexInvocationBase::SetOutputSizeHintVector(UInt32 numberOfOutputs,
                                                   UInt64* hints)
{
    if (numberOfOutputs != 0)
    {
        LogAssert(m_outputTotalSizeHint == 0);
    }

    delete [] m_outputSizeHintVector;

    m_outputSizeHintVectorLength = numberOfOutputs;
    if (numberOfOutputs == 0)
    {
        m_outputSizeHintVector = NULL;
    }
    else
    {
        m_outputSizeHintVector = new UInt64[m_outputSizeHintVectorLength];

        UInt32 i;
        for (i=0; i<m_outputSizeHintVectorLength; ++i)
        {
            m_outputSizeHintVector[i] = hints[i];
        }
    }
}

VertexInvocationBase* VertexInvocationBase::CloneInvocation()
{
    LogAssert(m_numberOfArguments > 0);

    DryadVertexFactoryBase* factory =
        VertexFactoryRegistry::LookUpFactory(m_argument[0]);

    LogAssert(factory != NULL, "Clone called on invalid vertex: %s", m_argument[0].GetString());

    DryadVertexProgramBase* clone = factory->NewUntyped();

    clone->SetCommandLine(m_commandLine);

    UInt32 i;

    for (i=1; i<m_numberOfArguments; ++i)
    {
        clone->AddArgument(m_argument[i]);
    }

    clone->SetMetaData(m_metaData);

    DrRef<DrMemoryBuffer> buffer;
    buffer.Attach(new DrSimpleHeapBuffer());
    {
        DrMemoryBufferWriter writer(buffer);
        Serialize(&writer);
    }

    {
        DrMemoryBufferReader reader(buffer);
        clone->DeSerialize(&reader);
        if (clone->GetErrorCode() != DrError_OK)
        {
            DrLogA( "Deserialize clone failed. Error %s", DRERRORSTRING(clone->GetErrorCode()));
        }
    }

    for (i=0; i<m_numberOfResources; ++i)
    {
        clone->AddResource(m_resource[i]);
    }

    clone->SetMaxOpenInputChannelCount(m_maxInputChannels);
    clone->SetMaxOpenOutputChannelCount(m_maxOutputChannels);

    clone->SetCanShareWorkQueue(m_canShareWorkQueue);
    
    clone->SetDisplayName(m_displayName);
    clone->SetDebugBreak(m_debugBreak);
    clone->SetFakeVertexFailure(m_fakeVertexFailure);
    clone->SetFakeVertexInputFailure(m_fakeVertexInputFailure);
    clone->SetAffinityConstraint(m_affinityConstraint);
    clone->GetAffinityLocationList()->assign(m_affinityLocations.begin(),
                                             m_affinityLocations.end());
    clone->SetCpuUsage(m_cpuUsage);
    clone->SetMemoryUsage(m_memoryUsage);
    clone->SetDiskUsage(m_diskUsage);

    clone->SetOutputTotalSizeHint(m_outputTotalSizeHint);
    clone->SetOutputSizeHintVector(m_outputSizeHintVectorLength,
                                   m_outputSizeHintVector);

    return clone;
}

VertexInvocationRecord::~VertexInvocationRecord()
{
}

class DVPDummyHandler : public RChannelItemWriterHandler
{
public:
    void ProcessWriteCompleted(RChannelItemType status,
                               RChannelItem* marshalFailureItem);
};

void DVPDummyHandler::ProcessWriteCompleted(RChannelItemType /*status*/,
                                            RChannelItem* /*failureItem*/)
{
    delete this;
}


DryadVertexProgramBase::ThreadBlock::
    ThreadBlock(DryadVertexProgramBase* parent,
                WorkQueue* workQueue,
                UInt32 numberOfInputChannels,
                RChannelReader** inputChannel,
                UInt32 numberOfOutputChannels,
                RChannelWriter** outputChannel,
                DryadVertexProgramCompletionHandler* handler)
{
    m_parent = parent;
    m_workQueue = workQueue;
    m_numberOfInputChannels = numberOfInputChannels;
    m_inputChannel = inputChannel;
    m_numberOfOutputChannels = numberOfOutputChannels;
    m_outputChannel = outputChannel;
    m_handler = handler;
}

//
// Run dryad vertex program and report completion
//
void DryadVertexProgramBase::ThreadBlock::Run()
{
    //
    // Execute main function
    // parent is of type ManagedWrapperVertex
    //
    m_parent->Main(m_workQueue,
                   m_numberOfInputChannels, m_inputChannel,
                   m_numberOfOutputChannels, m_outputChannel);
    
    //
    // Report completion
    //
    m_handler->ProgramCompleted();
}


DryadVertexProgramBase::DefaultHandler::DefaultHandler(HANDLE completionEvent)
{
    m_completionEvent = completionEvent;
}

void DryadVertexProgramBase::DefaultHandler::ProgramCompleted()
{
    BOOL bRet = ::SetEvent(m_completionEvent);
    LogAssert(bRet != 0);
}


DryadVertexProgramBase::DryadVertexProgramBase()
{
    m_defaultMainCalled = false;
    m_defaultMainAsyncCalled = false;
    m_parserFactoryArray = NULL;
    m_numberOfParserFactories = 0;
    m_marshalerFactoryArray = NULL;
    m_numberOfMarshalerFactories = 0;
    m_expectedSizeArrayLength = 0;
    m_expectedSizeArray = NULL;
}

DryadVertexProgramBase::~DryadVertexProgramBase()
{
    delete [] m_parserFactoryArray;
    delete [] m_marshalerFactoryArray;
    delete [] m_expectedSizeArray;
}

//
// Print out usage information to file
// If no arguments, vertex wasn't created
// If vertex was created, there's no info because this is just the base vertex class
//
void DryadVertexProgramBase::Usage(FILE* f)
{
    if (GetArgumentCount() < 1)
    {
        fprintf(f, "usage called before vertex creation completed\n\n");
        return;
    }

    fprintf(f, "vertex %s: no usage information\n\n",
            GetArgument(0));
}

UInt32 DryadVertexProgramBase::GetVertexId()
{
    return m_vertexId;
}

void DryadVertexProgramBase::SetVertexId(UInt32 vertexId)
{
    m_vertexId = vertexId;
}

UInt32 DryadVertexProgramBase::GetVertexVersion()
{
    return m_vertexVersion;
}

void DryadVertexProgramBase::SetVertexVersion(UInt32 vertexVersion)
{
    m_vertexVersion = vertexVersion;
}

UInt64 DryadVertexProgramBase::GetExpectedInputLength(UInt32 inputChannel)
{
    if (m_expectedSizeArray == NULL)
    {
        return (UInt64) -1;
    }

    LogAssert(inputChannel < m_expectedSizeArrayLength);
    return m_expectedSizeArray[inputChannel];
}

//
// Foreach input channel, set the expected length of that channel
//
void DryadVertexProgramBase::SetExpectedInputLength(UInt32 numberOfChannels,
                                                    UInt64* expectedLength)
{
    //
    // Clear current array and create new one
    //
    delete [] m_expectedSizeArray;
    m_expectedSizeArray = new UInt64[numberOfChannels];
    m_expectedSizeArrayLength = numberOfChannels;

    //
    // Foreach channel, remember length
    //
    UInt32 i;
    for (i=0; i<numberOfChannels; ++i)
    {
        m_expectedSizeArray[i] = expectedLength[i];
    }
}

void DryadVertexProgramBase::
    SetNumberOfParserFactories(UInt32 numberOfFactories)
{
    delete [] m_parserFactoryArray;
    m_numberOfParserFactories = numberOfFactories;
    m_parserFactoryArray = new DryadParserFactoryRef[m_numberOfParserFactories];
}

void DryadVertexProgramBase::SetParserFactory(UInt32 whichFactory,
                                              DryadParserFactoryBase* factory)
{
    LogAssert(whichFactory < m_numberOfParserFactories);
    m_parserFactoryArray[whichFactory] = factory;
}

void DryadVertexProgramBase::
    SetCommonParserFactory(DryadParserFactoryBase* factory)
{
    SetNumberOfParserFactories(1);
    SetParserFactory(0, factory);
    m_numberOfParserFactories = 0;
}

DryadParserFactoryBase* DryadVertexProgramBase::GetCommonParserFactory()
{
    if (m_parserFactoryArray != NULL && m_numberOfParserFactories == 0)
    {
        return m_parserFactoryArray[0];
    }
    else
    {
        return NULL;
    }
}

DryadParserFactoryBase* DryadVertexProgramBase::
    GetParserFactory(UInt32 whichFactory)
{
    if (m_parserFactoryArray == NULL)
    {
        return NULL;
    }

    if (m_numberOfParserFactories == 0)
    {
        return m_parserFactoryArray[0];
    }

    if (whichFactory >= m_numberOfParserFactories)
    {
        return NULL;
    }

    return m_parserFactoryArray[whichFactory];
}

void DryadVertexProgramBase::
    SetNumberOfMarshalerFactories(UInt32 numberOfFactories)
{
    delete [] m_marshalerFactoryArray;
    m_numberOfMarshalerFactories = numberOfFactories;
    m_marshalerFactoryArray =
        new DryadMarshalerFactoryRef[m_numberOfMarshalerFactories];
}

void DryadVertexProgramBase::
    SetMarshalerFactory(UInt32 whichFactory,
                        DryadMarshalerFactoryBase* factory)
{
    LogAssert(whichFactory < m_numberOfMarshalerFactories);
    m_marshalerFactoryArray[whichFactory] = factory;
}

void DryadVertexProgramBase::
    SetCommonMarshalerFactory(DryadMarshalerFactoryBase* factory)
{
    SetNumberOfMarshalerFactories(1);
    SetMarshalerFactory(0, factory);
    m_numberOfMarshalerFactories = 0;
}

DryadMarshalerFactoryBase* DryadVertexProgramBase::GetCommonMarshalerFactory()
{
    if (m_marshalerFactoryArray != NULL && m_numberOfMarshalerFactories == 0)
    {
        return m_marshalerFactoryArray[0];
    }
    else
    {
        return NULL;
    }
}

DryadMarshalerFactoryBase* DryadVertexProgramBase::
    GetMarshalerFactory(UInt32 whichFactory)
{
    if (m_marshalerFactoryArray == NULL)
    {
        return NULL;
    }

    if (m_numberOfMarshalerFactories == 0)
    {
        return m_marshalerFactoryArray[0];
    }

    if (whichFactory >= m_numberOfMarshalerFactories)
    {
        return NULL;
    }

    return m_marshalerFactoryArray[whichFactory];
}

void DryadVertexProgramBase::MakeInputParser(UInt32 whichInput,
                                             RChannelItemParserRef* pParser)
{
    if (m_parserFactoryArray != NULL)
    {
        if (m_numberOfParserFactories == 0)
        {
//             DrLogI(
//                 "Attaching parser from common factory",
//                 "Input channel %u", whichInput);
            m_parserFactoryArray[0]->MakeParser(pParser, this);
        }
        else if (whichInput >= m_numberOfParserFactories)
        {
            ReportError(DryadError_VertexInitialization,
                        "Parser for channel %u requested but "
                        "only %u factories supplied",
                        whichInput, m_numberOfParserFactories);
        }
        else if (m_parserFactoryArray[whichInput] == NULL)
        {
            ReportError(DryadError_VertexInitialization,
                        "Parser for channel %u requested but "
                        "no factory supplied for that channel",
                        whichInput);
        }
        else
        {
//             DrLogI(
//                 "Attaching parser from individual factory",
//                 "Input channel %u", whichInput);
            m_parserFactoryArray[whichInput]->MakeParser(pParser, this);
        }

        (*pParser)->SetParserIndex(whichInput);
        (*pParser)->SetParserContext(this);
    }
    else
    {
        ReportError(DrError_NotImplemented, "No factory implemented");
    }
}

//
// Make a marshaler for an output channel
//
void DryadVertexProgramBase::
    MakeOutputMarshaler(UInt32 whichOutput,
                        RChannelItemMarshalerRef* pMarshaler)
{
    if (m_marshalerFactoryArray != NULL)
    {
        if (m_numberOfMarshalerFactories == 0)
        {
//             DrLogI(
//                 "Attaching marshaler from common factory",
//                 "Output channel %u", whichOutput);
            //
            // Make a marshaller using the existing factory
            //
            m_marshalerFactoryArray[0]->MakeMarshaler(pMarshaler, this);
        }
        else if (whichOutput >= m_numberOfMarshalerFactories)
        {
            //
            // If output index doesn't have it's own factory, report error
            // todo: why does this matter
            //
            ReportError(DryadError_VertexInitialization,
                        "Marshaler for channel %u requested but "
                        "only %u factories supplied",
                        whichOutput, m_numberOfMarshalerFactories);
        }
        else if (m_marshalerFactoryArray[whichOutput] == NULL)
        {
            //
            // If factory for this output channel is null, report error
            //
            ReportError(DryadError_VertexInitialization,
                        "Marshaler for channel %u requested but "
                        "no factory supplied for that channel",
                        whichOutput);
        }
        else
        {
//             DrLogI(
//                 "Attaching marshaler from individual factory",
//                 "Input channel %u", whichOutput);
            //
            // If factory exists for this output channel, use it to create a marshaler
            //
            m_marshalerFactoryArray[whichOutput]->
                MakeMarshaler(pMarshaler, this);
        }

        //
        // Set marshaler index and context
        // todo: figure out what happens in middle two cases above
        //
        (*pMarshaler)->SetMarshalerIndex(whichOutput);
        (*pMarshaler)->SetMarshalerContext(this);
    }
    else
    {
//         DrLogI( "Attaching default marshaler",
//             "Output channel %u", whichOutput);
        //
        // If there is no factories, just use generic marshaler
        //
        pMarshaler->Attach(new RChannelStdItemMarshaler());
    }
}

//
// No initialization in base class
//
void DryadVertexProgramBase::Initialize(UInt32 numberOfInputChannels,
                                        UInt32 numberOfOutputChannels)
{
}

void DryadVertexProgramBase::Main(WorkQueue* workQueue,
                                  UInt32 numberOfInputChannels,
                                  RChannelReader** inputChannel,
                                  UInt32 numberOfOutputChannels,
                                  RChannelWriter** outputChannel)
{
    LogAssert(m_defaultMainAsyncCalled == false);
    m_defaultMainCalled = true;

    HANDLE completionEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(completionEvent != NULL);

    DefaultHandler handler(completionEvent);

    MainAsync(workQueue,
              numberOfInputChannels, inputChannel,
              numberOfOutputChannels, outputChannel, &handler);

    DWORD dRet  = ::WaitForSingleObject(completionEvent, INFINITE);
    LogAssert(dRet == WAIT_OBJECT_0);

    BOOL bRet = ::CloseHandle(completionEvent);
    LogAssert(bRet != 0);

    AsyncPostCompletion();
}

//
// Executes thread block containing reference to user code
//
unsigned DryadVertexProgramBase::MainThreadFunc(void* arg)
{
    //
    // Get thread block and run it. Blocking.
    //
    ThreadBlock* threadBlock = (ThreadBlock *) arg;
    threadBlock->Run();

    //
    // Clean up and return
    //
    delete threadBlock;
    return 0;
}

//
// Create a new thread to run user vertex code. This returns so that status can be reported periodically to GM
//
void DryadVertexProgramBase::MainAsync(WorkQueue* workQueue,
                                       UInt32 numberOfInputChannels,
                                       RChannelReader** inputChannel,
                                       UInt32 numberOfOutputChannels,
                                       RChannelWriter** outputChannel,
                                       DryadVertexProgramCompletionHandler*
                                       handler)
{
    LogAssert(m_defaultMainCalled == false);
    m_defaultMainAsyncCalled = true;

    //
    // Creat a new thread block with worker threads and data channels
    //
    ThreadBlock* threadBlock =
        new ThreadBlock(this, workQueue,
                        numberOfInputChannels, inputChannel,
                        numberOfOutputChannels, outputChannel, handler);

    //
    // Start main method using thread block
    //
    unsigned threadAddr;
    HANDLE threadHandle =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  MainThreadFunc,
                                  threadBlock,
                                  0,
                                  &threadAddr);
    LogAssert(threadHandle != 0);

    //
    // Close the thread handle and return
    //
    BOOL bRet = ::CloseHandle(threadHandle);
    LogAssert(bRet != 0);
}

//
// Nothing to do post-completion
//
void DryadVertexProgramBase::AsyncPostCompletion()
{
}

//
// Notify each I/O channel that they are done and/or not needed
//
void DryadVertexProgramBase::
    NotifyChannelsOfCompletion(UInt32 numberOfInputChannels,
                               RChannelReader** inputChannel,
                               UInt32 numberOfOutputChannels,
                               RChannelWriter** outputChannel)
{
    RChannelItemRef terminationItem;

    //
    // Create different channel completion codes depending on reason for ending
    //
    DrError status = GetErrorCode();                             
    switch (status)
    {
    case DrError_OK:
    case DryadError_ProcessingInterrupted:
        //
        // EndOfStream used in successful completion or user-interuption
        //
        terminationItem.
            Attach(RChannelMarkerItem::Create(RChannelItem_EndOfStream,
                                              false));
        break;

    case DryadError_ChannelRestart:
        //
        // Restart used when requested
        //
        terminationItem.
            Attach(RChannelMarkerItem::Create(RChannelItem_Restart,
                                              false));
        terminationItem->ReplaceMetaData(GetErrorMetaData());
        break;

    default:
        //
        // Use ProcessingInterrupted for all other reasons
        //
        terminationItem.
            Attach(RChannelMarkerItem::
                   CreateErrorItem(RChannelItem_Abort,
                                   DryadError_ProcessingInterrupted));
        break;
    }

    //
    // Interrupt each input channel with completion reason
    //
    UInt32 i;
    for (i=0; i<numberOfInputChannels; ++i)
    {
        RChannelItemRef clone;
        terminationItem->Clone(&clone);
        inputChannel[i]->Interrupt(clone);
    }

    //
    // Write completion reason to each output channel
    //
    for (i=0; i<numberOfOutputChannels; ++i)
    {
        DVPDummyHandler* dummyHandler = new DVPDummyHandler();
        RChannelItemRef clone;
        terminationItem->Clone(&clone);
        outputChannel[i]->WriteItem(clone, false, dummyHandler);
    }
}

//
// Drain all I/O channels
//
void DryadVertexProgramBase::DrainChannels(UInt32 numberOfInputChannels,
                                           RChannelReader** inputChannel,
                                           UInt32 numberOfOutputChannels,
                                           RChannelWriter** outputChannel)
{
    //
    // Drain all input channels
    //
    UInt32 i;
    for (i=0; i<numberOfInputChannels; ++i)
    {
        inputChannel[i]->Drain();
    }

    bool errorCondition = false;
    DryadMetaDataRef errorData;

    //
    // Drain all output channels
    //
    for (i=0; i<numberOfOutputChannels; ++i)
    {
        RChannelItemRef writeCompletion;
        outputChannel[i]->Drain(DrTimeInterval_Zero, &writeCompletion);
        if (writeCompletion != NULL)
        {
            if (writeCompletion->GetType() != RChannelItem_EndOfStream && errorCondition == false)
            {
                errorCondition = true;
                errorData = writeCompletion->GetMetaData();
            }
        }
    }

    if (GetErrorCode() == DrError_OK && errorCondition)
    {
        //
        // If completed writing but did not reach end of stream, this is an error
        //
        ReportError(DryadError_ChannelWriteError, errorData);
    }
}


DryadVertexProgram::~DryadVertexProgram()
{
}


DryadSimpleChannelVertexBase::ReaderData::ReaderData()
{
    m_bufferReader = NULL;
    m_reader = NULL;
    m_isFifo = false;
}

DryadSimpleChannelVertexBase::WriterData::WriterData()
{
    m_bufferWriter = NULL;
    m_writer = NULL;
    m_isFifo = false;
}

//
// Constructor - creates simple channel vertex
//
DryadSimpleChannelVertexBase::DryadSimpleChannelVertexBase()
{
    //
    // Use defaults
    //
    m_maxParseBatchSize = RChannelItem::s_defaultItemBatchSize;
    m_maxMarshalBatchSize = RChannelItem::s_defaultItemBatchSize;
    m_statusInterval = s_defaultStatusInterval;
    m_programCompleted = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_programCompleted != NULL);
    DrLogI( "Ensuring subgraph factory is registered. factory name %s", g_subgraphFactory->GetName());
}

//
// Destructor. Frees program completed handle
//
DryadSimpleChannelVertexBase::~DryadSimpleChannelVertexBase()
{
    BOOL bRet = ::CloseHandle(m_programCompleted);
    LogAssert(bRet != 0);
}

//
// Sets status interval
//
void DryadSimpleChannelVertexBase::SetStatusInterval(DrTimeInterval interval)
{
    m_statusInterval = interval;
}

//
// Gets status interval
//
DrTimeInterval DryadSimpleChannelVertexBase::GetStatusInterval()
{
    return m_statusInterval;
}

//
// Initialize m_vertexProgram with arguments and channels
//
void DryadSimpleChannelVertexBase::
    PrepareDryadVertex(DVertexProcessStatus* initialState,
                       DryadVertexFactoryBase* factory,
                       UInt32 argumentCount,
                       DrStr64* argumentList,
                       UInt32 serializedBlockLength,
                       const void* serializedBlock)
{
    DryadMetaDataRef errorData;

    //
    // Get input channels' length
    //
    UInt64* expectedLength = new UInt64[initialState->GetInputChannelCount()];
    UInt32 i;
    for (i=0; i<initialState->GetInputChannelCount(); ++i)
    {
        expectedLength[i] =
            initialState->GetInputChannels()[i].GetChannelTotalLength();
        initialState->GetInputChannels()[i].SetChannelTotalLength(0);
    }

    //
    // Get vertex program and initialize it with parameters and channels
    //
    m_initializationError = VertexFactoryRegistry::
        MakeVertex(initialState->GetVertexId(),
                   initialState->GetVertexInstanceVersion(),
                   initialState->GetInputChannelCount(),
                   initialState->GetOutputChannelCount(),
                   expectedLength,
                   factory,
                   initialState->GetVertexMetaData(),
                   initialState->GetMaxOpenInputChannelCount(),
                   initialState->GetMaxOpenOutputChannelCount(),
                   argumentCount,
                   argumentList,
                   serializedBlockLength,
                   serializedBlock,
                   &errorData,
                   &m_vertexProgram);

    //
    // Clean up channel length array
    //
    delete [] expectedLength;

    if (m_initializationError != DrError_OK)
    {
        //
        // Remember error data and report failure if unsuccessful
        //
        initialState->SetVertexMetaData(errorData, true);
        ReportStatus(initialState, false, false);
    }
}

//
// Set event that program has completed so that vertex host knows
//
void DryadSimpleChannelVertexBase::ProgramCompleted()
{
    BOOL bRet = ::SetEvent(m_programCompleted);
    LogAssert(bRet != 0);
}

//
// Update the progress based on the data channels
//
void DryadSimpleChannelVertexBase::
    UpdateChannelProgress(DVertexProcessStatus* status,
                          UInt32 inputChannelCount,
                          RChannelReaderHolderRef* rData,
                          UInt32 outputChannelCount,
                          RChannelWriterHolderRef* wData)
{
    UInt32 i;

    UInt64 processedLength = 0;
    UInt64 totalLength = 0, fifoLength = 0;

    //
    // Foreach input channel, get the processed length
    //
    for (i=0; i<inputChannelCount; ++i)
    {
        //
        // Get processed length of input channel
        //
        DryadInputChannelDescription* input =
            &(status->GetInputChannels()[i]);
        rData[i]->FillInStatus(input);
        UInt64 len = input->GetChannelProcessedLength();

        //
        // If fewer than 64 input channels, log each progress
        // todo: this seems arbitrary. Probably should be debug feature.
        //
        if (inputChannelCount < 64)
        {
            DrLogD( "Input channel progress. %u:%s %I64u/%I64u", i, input->GetChannelURI(),
                len,
                input->GetChannelTotalLength());
        }

        //
        // If fifo channel, add processed length to fifo count, 
        // otherwise add it to general processed count
        //
        if (!strncmp(input->GetChannelURI(), "fifo://", 7))
        {
            fifoLength += len;
        }
        else
        {
            processedLength += len;
        }
        
        //
        // Increment total channel length for this channel
        //
        totalLength += input->GetChannelTotalLength();
    }

    //
    // Report the processed length in the log
    //
    DrLogD( "Aggregated input progress. vertex %u.%u fifo=%I64u ext=%I64u total=%I64u",
        status->GetVertexId(), status->GetVertexInstanceVersion(),
        fifoLength, processedLength, totalLength);

    //
    // Foreach output channel, get the processed length
    //
    processedLength = fifoLength = 0;
    for (i=0; i<outputChannelCount; ++i)
    {
        //
        // Get processed length of output channel
        //
        DryadOutputChannelDescription* output =
            &(status->GetOutputChannels()[i]);
        wData[i]->FillInStatus(output);
        UInt64 len = output->GetChannelProcessedLength();
        
        //
        // If fewer than 64 output channels, log each progress
        // todo: this seems arbitrary. Probably should be debug feature.
        //
        if (outputChannelCount < 64)
        {
            DrLogD( "Output channel progress. %u:%s %I64u", i, output->GetChannelURI(), len);
        }
        
        //
        // If fifo channel, add processed length to fifo count, 
        // otherwise add it to general processed count
        //
        if (!strncmp(output->GetChannelURI(), "fifo://", 7))
        {
            fifoLength += len;
        }
        else
        {
            processedLength += len;
        }
    }
    
    //
    // Report the processed length in the log
    //
    DrLogD( "Aggregated output progress. vertex %u.%u fifo=%I64u ext=%I64u", status->GetVertexId(),
        status->GetVertexInstanceVersion(), fifoLength, processedLength);
}

//
// Start Async thread to call vertex code and periodically reports status to GM
// (called from RunDryadVertex)
//
void DryadSimpleChannelVertexBase::RunProgram(DVertexProcessStatus* status,
                                              WorkQueue* workQueue,
                                              UInt32 inputChannelCount,
                                              RChannelReaderHolderRef* rData,
                                              UInt32 outputChannelCount,
                                              RChannelWriterHolderRef* wData)
{
    UInt32 i;

    //
    // Get channel reader for each channel from reader holder
    //
    RChannelReader** rArray = new RChannelReader* [inputChannelCount];
    LogAssert(rArray != NULL);
    for (i=0; i<inputChannelCount; ++i)
    {
        rData[i]->GetReader()->Start(NULL);
        rArray[i] = rData[i]->GetReader();
        rArray[i]->SetExpectedLength(m_vertexProgram->
                                     GetExpectedInputLength(i));
    }

    //
    // Get channel writer for each channel from writer holder
    //
    RChannelWriter** wArray = new RChannelWriter* [outputChannelCount];
    LogAssert(wArray != NULL);
    for (i=0; i<outputChannelCount; ++i)
    {
        wData[i]->GetWriter()->Start();
        wArray[i] = wData[i]->GetWriter();
    }

    //
    // Clear the program completed event and report status is good
    //
    BOOL bRet = ::ResetEvent(m_programCompleted);
    LogAssert(bRet != 0);
    m_vertexProgram->ReportError(DrError_OK, (DryadMetaData *) NULL);

    //
    // Run the "main" method in the vertex program asynchronously
    //
    m_vertexProgram->MainAsync(workQueue,
                               inputChannelCount, rArray,
                               outputChannelCount, wArray,
                               this);

    //
    // While waiting for program completed event, Update Vertex service about progress. 
    //
    DWORD dRet;
    do
    {
        //
        // Wait for configurable timeout
        //
        DWORD waitTimeout = DrGetTimerMsFromInterval(m_statusInterval);
        dRet = ::WaitForSingleObject(m_programCompleted, waitTimeout);
        LogAssert(dRet == WAIT_OBJECT_0 || dRet == WAIT_TIMEOUT);
        
        //
        // Report data progress to log and ok/not_ok to GM
        //
        UpdateChannelProgress(status,
                              inputChannelCount, rData,
                              outputChannelCount, wData);
        ReportStatus(status, true, false);
    } while (dRet == WAIT_TIMEOUT);

    //
    // After completing vertex execution, perform any cleanup steps
    //
    m_vertexProgram->AsyncPostCompletion();

    if (status->GetVertexErrorCode() != DrError_OK)
    {
        DrLogI("Vertex reported error %08x %s", status->GetVertexErrorCode(), status->GetVertexErrorString());
    }

    //
    // Notify all I/O channels that they're done
    //
    m_vertexProgram->NotifyChannelsOfCompletion(inputChannelCount, rArray,
                                                outputChannelCount, wArray);

    //
    // If vertex status shows that it was interrupted, mark it as ok
    // todo: this looks like a hack to avoid reporting an error that can be set as
    //       vertex program stops. is it?
    //
    if (m_vertexProgram->GetErrorCode() == DryadError_ProcessingInterrupted)
    {
        m_vertexProgram->ReportError(DrError_OK);
    }

    //
    // Drain all I/O channels 
    //
    m_vertexProgram->DrainChannels(inputChannelCount, rArray,
                                   outputChannelCount, wArray);

    //
    // Record any error information in status
    //
    status->SetVertexMetaData(m_vertexProgram->GetErrorMetaData(), true);

    //
    // Update input channels with correct status of completion
    //
    for (i=0; i<inputChannelCount; ++i)
    {
        DryadMetaDataRef channelErrorData;
        DrError channelStatus =
            rArray[i]->GetTerminationStatus(&channelErrorData);
        status->GetInputChannels()[i].SetChannelState(channelStatus);
        status->GetInputChannels()[i].SetChannelMetaData(channelErrorData, true);
    }

    //
    // Update output channels with correct status of completion
    //
    for (i=0; i<outputChannelCount; ++i)
    {
        DryadMetaDataRef channelErrorData;
        DrError channelStatus =
            wArray[i]->GetTerminationStatus(&channelErrorData);
        status->GetOutputChannels()[i].SetChannelState(channelStatus);
        status->GetOutputChannels()[i].SetChannelMetaData(channelErrorData, true);
    }

    //
    // Write out I/O channel status to logs
    //
    UpdateChannelProgress(status,
                          inputChannelCount, rData,
                          outputChannelCount, wData);

    //
    // Clean up I/O channels
    //
    delete [] rArray;
    delete [] wArray;
}

//
// Prepare the channel readers, writers, and work queue for processing I/O
// Then calls RunProgram to continue quest to invoke user code
//
DrError DryadSimpleChannelVertexBase::
    RunDryadVertex(DVertexProcessStatus* initialState,
                   UInt32 argumentCount,
                   DrStr64* argumentList)
{
    //
    // Return initialization error if there is already a problem
    //
    if (m_initializationError != DrError_OK)
    {
        return m_initializationError;
    }

    DrLogI( "Opening vertex channels. Name %s",
        (argumentCount > 0) ? argumentList[0].GetString() : "unknown");

    if(initialState == NULL)
    {
        DrLogE("RunDryadVertex invoked with NULL initialState");
        return DrError_Fail;
    }

    UInt32 i;
    UInt32 iCC = initialState->GetInputChannelCount();
    UInt32 oCC = initialState->GetOutputChannelCount();

    //
    // Create a work queue that has two threads per core, 
    // but only one thread per core able to run concurrently
    // start the work queue
    //
    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);
    WorkQueue* workQueue = new WorkQueue(systemInfo.dwNumberOfProcessors*2,
                                         systemInfo.dwNumberOfProcessors);
    workQueue->Start();

    //
    // Create readers for input channels and writers for output channels
    //
    RChannelReaderHolderRef* rData = new RChannelReaderHolderRef[iCC];
    RChannelWriterHolderRef* wData = new RChannelWriterHolderRef[oCC];

    bool failed = false;
    DrStr128 failureReason;

    //
    // Throttle input connections to maximum 
    //
    RChannelOpenThrottler* readThrottler = NULL;
    UInt32 maxReaders = m_vertexProgram->GetMaxOpenInputChannelCount();
    if (maxReaders > 0)
    {
        readThrottler = RChannelFactory::MakeOpenThrottler(maxReaders,
                                                           workQueue);
    }

    //
    // Set maximum parsing batch size to the total maximum over the number of 
    // input channels, but allow at least 4.
    // Done because: if there are lots of input channels, scale back on the 
    //               batching so we don't use so much memory 
    //
    UInt32 maxParseBatchSize = 1;
    if (iCC > 0)
    {
        maxParseBatchSize = m_maxParseBatchSize / iCC;
    }
    if (maxParseBatchSize < 4)
    {
        maxParseBatchSize = 4;
    }

    UInt32 maxParseUnitsInFlight = maxParseBatchSize * 4;

    //
    // Create locality monitoring stuff
    //
    DWORD localInputChannels = 0;
    bool channelLocalCreated = true;
    bool* channelLocal = (bool*)malloc(sizeof(bool)*iCC);
    if(channelLocal == NULL)
    {
        channelLocalCreated = false;
        DrLogW("Channel locality list could not be created. Logging detail reduced.");
    }

    //
    // Foreach input channel, open a reader and record any errors
    //
    for (i=0; i<iCC; ++i)
    {
        //
        // Get the channel description and URI
        //
        DVErrorReporter errorReporter;
        DryadChannelDescription* input =
            &(initialState->GetInputChannels()[i]);
        const char* uri = input->GetChannelURI();

        // Get compression mode parameter
        DrLogD("Original URI: %s", uri);
        TransformType mode = StripCompressionModeFromUri(const_cast<char*>(uri));
        DrLogD("Transform type: %d. New URI: %s", mode, uri);

        //
        // Assume channel is remote unless proven otherwise
        //
        if(channelLocalCreated)
        {
            channelLocal[i] = false;
        }
        
        //
        // Make an input parser
        //
        RChannelItemParserRef parser;
        m_vertexProgram->MakeInputParser(i, &parser);
        if (m_vertexProgram->NoError())
        {
            DWORD localInputChannelsSnapshot = localInputChannels;

            //
            // Open the channel reader if parser successfully created
            //
            DrError err = RChannelFactory::OpenReader(uri, input->GetChannelMetaData(),
                                                     parser,
                                                     iCC, readThrottler,
                                                     maxParseBatchSize,
                                                     maxParseUnitsInFlight,
                                                     workQueue, &errorReporter,
                                                     &(rData[i]), 
                                                     &localInputChannels);
            if(err != DrError_OK)
            {
                DrLogE("RChannelFactory::OpenReader failed for %s", uri);
            }
            else if(rData[i] == NULL)
            {
                DrLogE("RChannelFactory::OpenReader returned a NULL RChannelReaderHolder object");
            }
            else
            {
                //
                // Set the transform (compression) type
                //
                rData[i]->GetReader()->SetTransformType(mode);
            
                //
                // If number of local input channels increased, this one was local
                //
                if(channelLocalCreated && (localInputChannelsSnapshot < localInputChannels))
                {
                    channelLocal[i] = true;
                }
            }
        }
        else
        {
            //
            // Report errors making parse
            //
            errorReporter.ReportError(m_vertexProgram->GetErrorCode(),
                            m_vertexProgram->GetErrorMetaData());
        }

        //
        // Remember failure if anything goes wrong and update channel
        // state with any errors.
        //
        if (errorReporter.GetErrorCode() != DrError_OK)
        {
            failed = true;

            input->SetChannelState(errorReporter.GetErrorCode());
            input->SetChannelMetaData(errorReporter.GetErrorMetaData(), true);

            if (failureReason.GetString() == NULL)
            {
                failureReason.SetF("Input channel %d open failed: %s", i, input->GetChannelErrorString());
            }
        }
    }
    
    //
    // Record number of local input files
    //
    fprintf(stdout, "DryadVertex: Reading %lu input file(s) from local disk and %lu input file(s) over network\n", localInputChannels, iCC - localInputChannels);
    
    //
    // Record list of channel inputs if list was created successfully
    //
    if(channelLocalCreated)
    {
        DrStr localChannelString("");
        bool firstChannelDone = false;
        for(i = 0; i < iCC; ++i)
        {
            if (channelLocal[i])
            {
                if(firstChannelDone)
                {
                    localChannelString.AppendF(", %u", i);
                }
                else
                {
                    localChannelString.SetF("%u", i);
                    firstChannelDone = true;
                }
            }
        }

        fprintf(stdout, "DryadVertex: Channels reading from local input files - {%s}\n", localChannelString.GetString());
        free(channelLocal);
    }
    
    //
    // Flush stdout before linq starts using it
    //
    fflush(stdout);

    //
    // Create an output channel throttler
    //
    RChannelOpenThrottler* writeThrottler = NULL;
    UInt32 maxWriters = m_vertexProgram->GetMaxOpenOutputChannelCount();
    if (maxWriters > 0)
    {
        writeThrottler = RChannelFactory::MakeOpenThrottler(maxWriters,
                                                            workQueue);
    }

    //
    // Set maximum parsing batch size to the total maximum over the number of 
    // input channels, but allow at least 4.
    // Done because: if there are lots of output channels, scale back on the 
    //               batching so we don't use so much memory 
    //
    UInt32 maxMarshalBatchSize = 1;
    if (oCC > 0)
    {
        maxMarshalBatchSize = m_maxMarshalBatchSize / oCC;
    }
    if (maxMarshalBatchSize < 4)
    {
        maxMarshalBatchSize = 4;
    }

    //
    // Foreach input channel, open a reader and record any errors
    //
    for (i=0; i<oCC; ++i)
    {
        DVErrorReporter errorReporter;
        
        //
        // Get the channel description and URI
        //
        DryadChannelDescription* output =
            &(initialState->GetOutputChannels()[i]);
        const char* uri = output->GetChannelURI();

        // Get the compression mode parameter
        DrLogD("Original URI: %s", uri);
        TransformType mode = StripCompressionModeFromUri(const_cast<char*>(uri));
        DrLogD("Transform type: %d. New URI: %s", mode, uri);
        
        //
        // Create marshaller for writing output to this channel
        //
        RChannelItemMarshalerRef marshaler;
        m_vertexProgram->MakeOutputMarshaler(i, &marshaler);
        if (m_vertexProgram->NoError())
        {
            //
            // If marshaler successfully created, open a writer to the output channel
            //
            RChannelFactory::OpenWriter(uri, output->GetChannelMetaData(),
                                        marshaler,
                                        oCC, writeThrottler,
                                        maxMarshalBatchSize,
                                        workQueue, &errorReporter,
                                        &(wData[i]));

            //
            // Set the transform (compression) type if writer was created (no error)
            //
            if(errorReporter.GetErrorCode() == DrError_OK)
            {
                wData[i]->GetWriter()->SetTransformType(mode);
            }
        }
        else
        {
            //
            // If marshaler not created successfully, report error
            //
            errorReporter.
                ReportError(m_vertexProgram->GetErrorCode(),
                            m_vertexProgram->GetErrorMetaData());
        }


        //
        // Remember failure if anything goes wrong and update channel
        // state with any errors.
        //
        if (errorReporter.GetErrorCode() != DrError_OK)
        {
            failed = true;

            output->SetChannelState(errorReporter.GetErrorCode());
            output->SetChannelMetaData(errorReporter.GetErrorMetaData(), true);

            if (failureReason.GetString() == NULL)
            {
                failureReason.SetF("Output channel %d open failed: %s", i, output->GetChannelErrorString());
            }
        }
    }

    //
    // If any failures, record vertex initialization failure
    //
    DrError err;
    if (failed)
    {
        err = DryadError_VertexInitialization;
        initialState->SetVertexErrorCode(err);

        DrStr128 reason;
        reason.SetF("Vertex was unable to initialize: %s", failureReason.GetString());
        initialState->SetVertexErrorString(reason);
    }
    else
    {
        //
        // Log start of vertex execution
        //
        DrLogI( "Starting vertex. Name %s",
            (argumentCount > 0) ? argumentList[0].GetString() : "unknown");
        
        //
        // Move forward in process of invoking user vertex - blocking
        //
        RunProgram(initialState, workQueue, iCC, rData, oCC, wData);

        //
        // Get any error code, record, it and mark it as completed if successful
        //
        err = m_vertexProgram->GetErrorCode();
        DrLogI( "Vertex complete. Name %s status %s",
            (argumentCount > 0) ? argumentList[0].GetString() : "unknown",
            DRERRORSTRING(err));
        if (err == DrError_OK)
        {
            err = DryadError_VertexCompleted;
        }
    }

    DrLogI( "Closing vertex channels. Name %s",
        (argumentCount > 0) ? argumentList[0].GetString() : "unknown");

    //
    // Close any channel readers and writers that have been opened  
    //
    delete [] rData;
    delete [] wData;

    //
    // Clean up read and write throttlers
    //
    if (readThrottler != NULL)
    {
        RChannelFactory::DiscardOpenThrottler(readThrottler);
        readThrottler = NULL;
    }

    if (writeThrottler != NULL)
    {
        RChannelFactory::DiscardOpenThrottler(writeThrottler);
        writeThrottler = NULL;
    }

    //
    // Stop and clean up thread pool for vertex
    //
    DrLogI( "Stopping vertex work queue. Name %s",
        (argumentCount > 0) ? argumentList[0].GetString() : "unknown");
    workQueue->Stop();
    delete workQueue;

    //
    // Report the updated job status and exit
    //
    ReportStatus(initialState, false, false);
    DrLogI( "Exiting vertex. Name %s",
        (argumentCount > 0) ? argumentList[0].GetString() : "unknown");
    return err;
}

//
// Report error if "ReOpenChannels" command ever received, as it is not supported
//
void DryadSimpleChannelVertexBase::
    ReOpenChannels(DVertexProcessStatus* /*newChannelStatus*/)
{
    //
    // since a simple channel vertex never requests channels to be
    // reopened, the job manager should never call here 
    //
    LogAssert(false);
}

//
// Do nothing to clean up the vertex.
//
DryadSimpleChannelVertex::~DryadSimpleChannelVertex()
{
}
