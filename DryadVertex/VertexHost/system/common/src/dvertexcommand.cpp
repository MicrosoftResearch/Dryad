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

#include <dvertexcommand.h>
#include <dryadpropertiesdef.h>
#include <dryadtagsdef.h>
#include <dryaderrordef.h>
#include <errorreporter.h>

#pragma unmanaged

static const char* s_StatusPropertyLabel = "DVertexStatus";
static const char* s_CommandPropertyLabel = "DVertexCommand";

DryadPnProcessPropertyRequest::~DryadPnProcessPropertyRequest()
{
}

DryadPnProcessPropertyResponse::~DryadPnProcessPropertyResponse()
{
}

DryadChannelDescription::DryadChannelDescription(bool isInputChannel)
{
    m_state = DryadError_ChannelAbort;
    m_errorCode = DrError_OK;
    m_totalLength = 0;
    m_processedLength = 0;
    m_isInputChannel = isInputChannel;
}

DryadChannelDescription::~DryadChannelDescription()
{
}

DrError DryadChannelDescription::GetChannelState() const
{
    return m_state;
}

void DryadChannelDescription::SetChannelState(DrError state)
{
    m_state = state;
}

const char* DryadChannelDescription::GetChannelURI() const
{
    return m_URI;
}

void DryadChannelDescription::SetChannelURI(const char* channelURI)
{
    m_URI.Set(channelURI);
}

DryadMetaData* DryadChannelDescription::GetChannelMetaData() const
{
    return m_metaData;
}

void DryadChannelDescription::SetChannelMetaData(DryadMetaData* metaData, bool updateErrorInfo)
{
    m_metaData.Set(metaData);

    if (updateErrorInfo)
    {
        DrStr128 errorDescription;
        DrError errorCode = DVErrorReporter::GetFormattedErrorFromMetaData(metaData, &errorDescription);
        SetChannelErrorCode(errorCode);
        SetChannelErrorString(errorDescription);
    }
}

DrError DryadChannelDescription::GetChannelErrorCode() const
{
    return m_errorCode;
}

void DryadChannelDescription::SetChannelErrorCode(DrError errorCode)
{
    m_errorCode = errorCode;
}

const char* DryadChannelDescription::GetChannelErrorString() const
{
    return m_errorString;
}

void DryadChannelDescription::SetChannelErrorString(const char* errorString)
{
    m_errorString.Set(errorString);
}

UInt64 DryadChannelDescription::GetChannelTotalLength() const
{
    return m_totalLength;
}

void DryadChannelDescription::SetChannelTotalLength(UInt64 totalLength)
{
    m_totalLength = totalLength;
}

UInt64 DryadChannelDescription::GetChannelProcessedLength() const
{
    return m_processedLength;
}

void DryadChannelDescription::
    SetChannelProcessedLength(UInt64 processedLength)
{
    m_processedLength = processedLength;
}

DrError DryadChannelDescription::Serialize(DrMemoryWriter* writer)
{
    UInt16 tagValue = (m_isInputChannel) ?
        DryadTag_InputChannelDescription :
        DryadTag_OutputChannelDescription;

    writer->WriteUInt16Property(Prop_Dryad_BeginTag, tagValue);

    writer->WriteDrErrorProperty(Prop_Dryad_ChannelState, m_state);
    writer->WriteLongDrStrProperty(Prop_Dryad_ChannelURI, m_URI);
    writer->WriteUInt64Property(Prop_Dryad_ChannelTotalLength, m_totalLength);
    writer->WriteUInt64Property(Prop_Dryad_ChannelProcessedLength,
                                m_processedLength);
    if (m_metaData.Ptr() != NULL)
    {
        m_metaData.Ptr()->WriteAsAggregate(writer,
                                           DryadTag_ChannelMetaData, false);
    }
    if (m_errorCode != DrError_OK)
    {
        writer->WriteDrErrorProperty(Prop_Dryad_ChannelErrorCode, m_errorCode);
    }
    if (m_errorString != NULL)
    {
        writer->WriteLongDrStrProperty(Prop_Dryad_ChannelErrorString, m_errorString);
    }

    writer->WriteUInt16Property(Prop_Dryad_EndTag, tagValue);

    return writer->GetStatus();
}

DrError DryadChannelDescription::OnParseProperty(DrMemoryReader *reader,
                                                 UInt16 enumID,
                                                 UInt32 dataLen,
                                                 void *cookie)
{
    DrError err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in channel description. enumID %u", (DWORD ) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case Prop_Dryad_ChannelState:
        err = reader->ReadNextDrErrorProperty(enumID, &m_state);
        break;

    case Prop_Dryad_ChannelURI:
        {
            const char* URI;
            err = reader->ReadNextStringProperty(enumID, &URI);
            if (err == DrError_OK)
            {
                SetChannelURI(URI);
            }
        }
        break;

    case Prop_Dryad_ChannelTotalLength:
        err = reader->ReadNextUInt64Property(enumID, &m_totalLength);
        break;

    case Prop_Dryad_ChannelProcessedLength:
        err = reader->ReadNextUInt64Property(enumID, &m_processedLength);
        break;

    case Prop_Dryad_ChannelErrorCode:
        err = reader->ReadNextDrErrorProperty(enumID, &m_errorCode);
        break;

    case Prop_Dryad_ChannelErrorString:
        {
            const char* errorString;
            err = reader->ReadNextStringProperty(enumID, &errorString);
            if (err == DrError_OK)
            {
                SetChannelErrorString(errorString);
            }
        }
        break;

    case Prop_Dryad_BeginTag:
        {
            UInt16 tagID;
            err = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagID);
            if (err == DrError_OK)
            {
                if (tagID == DryadTag_ChannelMetaData)
                {
                    DryadMetaDataParser parser;
                    err = reader->ReadAggregate(tagID, &parser, NULL);
                    if (err == DrError_OK)
                    {
                        SetChannelMetaData(parser.GetMetaData(), false);
                    }
                }
                else
                {
                    DrLogW("Unknown aggregate in channel description. tagID %u", (DWORD) tagID);
                }
            }
        }
        break;
    }

    return err;
}

void DryadChannelDescription::CopyFrom(DryadChannelDescription* src,
                                       bool includeLengths)
{
    LogAssert(m_isInputChannel == src->m_isInputChannel);

    SetChannelURI(src->GetChannelURI());
    SetChannelState(src->GetChannelState());
    SetChannelMetaData(src->GetChannelMetaData(), false);
    SetChannelErrorCode(src->GetChannelErrorCode());
    SetChannelErrorString(src->GetChannelErrorString());
    if (includeLengths)
    {
        SetChannelProcessedLength(src->GetChannelProcessedLength());
        SetChannelTotalLength(src->GetChannelTotalLength());
    }
}


DryadInputChannelDescription::DryadInputChannelDescription() :
    DryadChannelDescription(true)
{
}


DryadOutputChannelDescription::DryadOutputChannelDescription() :
    DryadChannelDescription(false)
{
}


//
// Create new Process status with default property values
//
DVertexProcessStatus::DVertexProcessStatus()
{
    m_id = 0;
    m_version = 0;
    m_errorCode = DrError_OK;
    m_nInputChannels = 0;
    m_inputChannel = NULL;
    m_maxInputChannels = 0;
    m_nOutputChannels = 0;
    m_outputChannel = NULL;
    m_maxOutputChannels = 0;
    m_canShareWorkQueue = false;

    m_nextInputChannelToRead = 0;
    m_nextOutputChannelToRead = 0;
}

DVertexProcessStatus::~DVertexProcessStatus()
{
    delete [] m_inputChannel;
    delete [] m_outputChannel;
}

UInt32 DVertexProcessStatus::GetVertexId()
{
    return m_id;
}

void DVertexProcessStatus::SetVertexId(UInt32 channelId)
{
    m_id = channelId;
}

UInt32 DVertexProcessStatus::GetVertexInstanceVersion()
{
    return m_version;
}

void DVertexProcessStatus::SetVertexInstanceVersion(UInt32 version)
{
    m_version = version;
}

DryadMetaData* DVertexProcessStatus::GetVertexMetaData()
{
    return m_metaData.Ptr();
}

void DVertexProcessStatus::SetVertexMetaData(DryadMetaData* metaData, bool updateErrorInfo)
{
    m_metaData.Set(metaData);

    if (updateErrorInfo)
    {
        DrStr128 errorDescription;
        DrError errorCode = DVErrorReporter::GetFormattedErrorFromMetaData(metaData, &errorDescription);
        SetVertexErrorCode(errorCode);
        SetVertexErrorString(errorDescription);
    }
}

DrError DVertexProcessStatus::GetVertexErrorCode() const
{
    return m_errorCode;
}

void DVertexProcessStatus::SetVertexErrorCode(DrError errorCode)
{
    m_errorCode = errorCode;
}

const char* DVertexProcessStatus::GetVertexErrorString() const
{
    return m_errorString;
}

void DVertexProcessStatus::SetVertexErrorString(const char* errorString)
{
    m_errorString.Set(errorString);
}

UInt32 DVertexProcessStatus::GetInputChannelCount()
{
    return m_nInputChannels;
}

DryadInputChannelDescription* DVertexProcessStatus::GetInputChannels()
{
    return m_inputChannel;
}

void DVertexProcessStatus::SetInputChannelCount(UInt32 nInputChannels)
{
    delete [] m_inputChannel;
    m_inputChannel = new DryadInputChannelDescription[nInputChannels];
    m_nInputChannels = nInputChannels;
    m_nextInputChannelToRead = 0;
}

UInt32 DVertexProcessStatus::GetMaxOpenInputChannelCount()
{
    return m_maxInputChannels;
}

void DVertexProcessStatus::SetMaxOpenInputChannelCount(UInt32 channelCount)
{
    m_maxInputChannels = channelCount;
}

UInt32 DVertexProcessStatus::GetOutputChannelCount()
{
    return m_nOutputChannels;
}

DryadOutputChannelDescription* DVertexProcessStatus::GetOutputChannels()
{
    return m_outputChannel;
}

void DVertexProcessStatus::SetOutputChannelCount(UInt32 nOutputChannels)
{
    delete [] m_outputChannel;
    m_outputChannel = new DryadOutputChannelDescription[nOutputChannels];
    m_nOutputChannels = nOutputChannels;
    m_nextOutputChannelToRead = 0;
}

UInt32 DVertexProcessStatus::GetMaxOpenOutputChannelCount()
{
    return m_maxOutputChannels;
}

void DVertexProcessStatus::SetMaxOpenOutputChannelCount(UInt32 channelCount)
{
    m_maxOutputChannels = channelCount;
}

bool DVertexProcessStatus::GetCanShareWorkQueue()
{
    return m_canShareWorkQueue;
}

void DVertexProcessStatus::SetCanShareWorkQueue(bool canShareWorkQueue)
{
    m_canShareWorkQueue = canShareWorkQueue;
}

DrError DVertexProcessStatus::Serialize(DrMemoryWriter* writer)
{
    UInt32 i;

    writer->WriteUInt16Property(Prop_Dryad_BeginTag,
                                DryadTag_VertexProcessStatus);

    writer->WriteUInt32Property(Prop_Dryad_VertexId, m_id);
    writer->WriteUInt32Property(Prop_Dryad_VertexVersion, m_version);
    if (m_metaData.Ptr() != NULL)
    {
        m_metaData.Ptr()->WriteAsAggregate(writer,
                                           DryadTag_VertexMetaData, false);
    }
    if (m_errorCode != DrError_OK)
    {
        writer->WriteDrErrorProperty(Prop_Dryad_VertexErrorCode, m_errorCode);
    }
    if (m_errorString != NULL)
    {
        writer->WriteLongDrStrProperty(Prop_Dryad_VertexErrorString, m_errorString);
    }

    writer->WriteUInt32Property(Prop_Dryad_VertexInputChannelCount,
                                m_nInputChannels);
    for (i=0; i<m_nInputChannels; ++i)
    {
        DrError err = m_inputChannel[i].Serialize(writer);
        if (err != DrError_OK && writer->GetStatus() == DrError_OK)
        {
            writer->SetStatus(err);
        }
    }
    writer->WriteUInt32Property(Prop_Dryad_VertexMaxOpenInputChannelCount,
                                m_maxInputChannels);

    writer->WriteUInt32Property(Prop_Dryad_VertexOutputChannelCount,
                                m_nOutputChannels);
    for (i=0; i<m_nOutputChannels; ++i)
    {
        DrError err = m_outputChannel[i].Serialize(writer);
        if (err != DrError_OK && writer->GetStatus() == DrError_OK)
        {
            writer->SetStatus(err);
        }
    }
    writer->WriteUInt32Property(Prop_Dryad_VertexMaxOpenOutputChannelCount,
                                m_maxOutputChannels);

    writer->WriteBoolProperty(Prop_Dryad_CanShareWorkQueue,
                              m_canShareWorkQueue);

    writer->WriteUInt16Property(Prop_Dryad_EndTag,
                                DryadTag_VertexProcessStatus);

    return writer->GetStatus();
}

DrError DVertexProcessStatus::OnParseProperty(DrMemoryReader *reader,
                                              UInt16 enumID,
                                              UInt32 dataLen,
                                              void *cookie)
{
    DrError err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in vertex status message. enumID %u", (DWORD ) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case Prop_Dryad_VertexId:
        err = reader->ReadNextUInt32Property(enumID, &m_id);
        break;

    case Prop_Dryad_VertexVersion:
        err = reader->ReadNextUInt32Property(enumID, &m_version);
        break;

    case Prop_Dryad_VertexErrorCode:
        err = reader->ReadNextDrErrorProperty(enumID, &m_errorCode);
        break;

    case Prop_Dryad_VertexErrorString:
        {
            const char* errorString;
            err = reader->ReadNextStringProperty(enumID, &errorString);
            if (err == DrError_OK)
            {
                SetVertexErrorString(errorString);
            }
        }
        break;

    case Prop_Dryad_VertexInputChannelCount:
        UInt32 nInputChannels;
        err = reader->ReadNextUInt32Property(enumID, &nInputChannels);
        if (err == DrError_OK)
        {
            SetInputChannelCount(nInputChannels);
        }
        break;

    case Prop_Dryad_VertexMaxOpenInputChannelCount:
        UInt32 maxInputChannels;
        err = reader->ReadNextUInt32Property(enumID, &maxInputChannels);
        if (err == DrError_OK)
        {
            SetMaxOpenInputChannelCount(maxInputChannels);
        }
        break;

    case Prop_Dryad_VertexOutputChannelCount:
        UInt32 nOutputChannels;
        err = reader->ReadNextUInt32Property(enumID, &nOutputChannels);
        if (err == DrError_OK)
        {
            SetOutputChannelCount(nOutputChannels);
        }
        break;

    case Prop_Dryad_VertexMaxOpenOutputChannelCount:
        UInt32 maxOutputChannels;
        err = reader->ReadNextUInt32Property(enumID, &maxOutputChannels);
        if (err == DrError_OK)
        {
            SetMaxOpenOutputChannelCount(maxOutputChannels);
        }
        break;

    case Prop_Dryad_CanShareWorkQueue:
        err = reader->ReadNextBoolProperty(enumID, &m_canShareWorkQueue);
        break;

    case Prop_Dryad_BeginTag:
        UInt16 tagValue;
        err = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
        if (err != DrError_OK)
        {
            DrLogE("Error reading Prop_Dryad_BeginTag - 0x%08x", err);
        } else {
            switch (tagValue)
            {
            case DryadTag_InputChannelDescription:
                if (m_nextInputChannelToRead >= m_nInputChannels)
                {
                    DrLogE("Too many input channel descriptions. nextInputChannelToRead=%u, nInputChannels=%u",
                        m_nextInputChannelToRead, m_nInputChannels);
                    err = DrError_InvalidParameter;
                }
                else
                {
                    DryadInputChannelDescription* channel =
                        &(m_inputChannel[m_nextInputChannelToRead]);
                    err = reader->ReadAggregate(tagValue, channel, NULL);
                    if (err == DrError_OK)
                    {
                        ++m_nextInputChannelToRead;
                    }
                }
                break;

            case DryadTag_OutputChannelDescription:
                if (m_nextOutputChannelToRead >= m_nOutputChannels)
                {
                    DrLogE(
                        "Too many output channel descriptions. nextOutputChannelToRead=%u, nOutputChannels=%u",
                        m_nextOutputChannelToRead, m_nOutputChannels);
                    err = DrError_InvalidParameter;
                }
                else
                {
                    DryadOutputChannelDescription* channel =
                        &(m_outputChannel[m_nextOutputChannelToRead]);
                    err = reader->ReadAggregate(tagValue, channel, NULL);
                    if (err == DrError_OK)
                    {
                        ++m_nextOutputChannelToRead;
                    }
                }
                break;

            case DryadTag_VertexMetaData:
                {
                    DryadMetaDataParser parser;
                    err = reader->ReadAggregate(tagValue, &parser, NULL);
                    if (err == DrError_OK)
                    {
                        SetVertexMetaData(parser.GetMetaData(), false);
                    }
                }
                break;

            default:
                DrLogW(
                    "Unexpected tag - %hu", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

void DVertexProcessStatus::CopyFrom(DVertexProcessStatus* src,
                                    bool includeLengths)
{
    UInt32 i;

    SetVertexId(src->GetVertexId());
    SetVertexInstanceVersion(src->GetVertexInstanceVersion());
    SetVertexMetaData(src->GetVertexMetaData(), false);
    SetVertexErrorCode(src->GetVertexErrorCode());
    SetVertexErrorString(src->GetVertexErrorString());

    SetInputChannelCount(src->GetInputChannelCount());
    DryadInputChannelDescription* srcInputs = src->GetInputChannels();
    for (i=0; i<m_nInputChannels; ++i)
    {
        m_inputChannel[i].CopyFrom(&(srcInputs[i]), includeLengths);
    }
    SetMaxOpenInputChannelCount(src->GetMaxOpenInputChannelCount());

    SetOutputChannelCount(src->GetOutputChannelCount());
    DryadOutputChannelDescription* srcOutputs = src->GetOutputChannels();
    for (i=0; i<m_nOutputChannels; ++i)
    {
        m_outputChannel[i].CopyFrom(&(srcOutputs[i]), includeLengths);
    }
    SetMaxOpenOutputChannelCount(src->GetMaxOpenOutputChannelCount());
}


DVertexStatus::DVertexStatus()
{
    m_state = DrError_OK;
    m_processStatus = new DVertexProcessStatus();
}

DrError DVertexStatus::GetVertexState()
{
    return m_state;
}

//
// Update vertex state
//
void DVertexStatus::SetVertexState(DrError state)
{
    m_state = state;
}

DVertexProcessStatus* DVertexStatus::GetProcessStatus()
{
    return m_processStatus;
}

void DVertexStatus::SetProcessStatus(DVertexProcessStatus* processStatus)
{
    m_processStatus = processStatus;
}

DrError DVertexStatus::Serialize(DrMemoryWriter* writer)
{
    writer->WriteUInt16Property(Prop_Dryad_BeginTag, DryadTag_VertexStatus);

    writer->WriteDrErrorProperty(Prop_Dryad_VertexState, m_state);
    DrError err = m_processStatus->Serialize(writer);
    if (err != DrError_OK && writer->GetStatus() == DrError_OK)
    {
        writer->SetStatus(err);
    }

    writer->WriteUInt16Property(Prop_Dryad_EndTag, DryadTag_VertexStatus);

    return writer->GetStatus();
}

DrError DVertexStatus::OnParseProperty(DrMemoryReader *reader,
                                       UInt16 enumID,
                                       UInt32 dataLen,
                                       void *cookie)
{
    DrError err;

    switch (enumID)
    {
    default:
        DrLogW(
            "Unknown property in vertex status message. enumID %u", (DWORD ) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case Prop_Dryad_VertexState:
        err = reader->ReadNextDrErrorProperty(enumID, &m_state);
        break;

    case Prop_Dryad_BeginTag:
        UInt16 tagValue;
        err = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
        if (err != DrError_OK)
        {
            DrLogE("Error reading Prop_Dryad_BeginTag - 0x08x", err);
        } else {
            switch (tagValue)
            {
            case DryadTag_VertexProcessStatus:
                err = reader->ReadAggregate(tagValue, m_processStatus, NULL);
                break;

            default:
                DrLogW("Unexpected tag - %hu", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

void DVertexStatus::
    StoreInRequestMessage(DryadPnProcessPropertyRequest* request)
{
    DrStr64 label;
    GetPnPropertyLabel(&label,
                       m_processStatus->GetVertexId(),
                       m_processStatus->GetVertexInstanceVersion(),
                       false);

    DrStr64 controlLabel;
    GetPnPropertyLabel(&controlLabel,
                       m_processStatus->GetVertexId(),
                       m_processStatus->GetVertexInstanceVersion(),
                       true);

    DrLogI( "Storing status update property. Label: %s", label.GetString());

    request->SetPropertyLabel(label, controlLabel);
    request->SetPropertyString(DRERRORSTRING(m_state));

    DrMemoryBuffer* block = request->GetPropertyBlock();

    {
        DrMemoryBufferWriter writer(block);
        DrError err = Serialize(&writer);
        LogAssert(err == DrError_OK);
        err = writer.FlushMemoryWriter();
        LogAssert(err == DrError_OK);
    }
}

DrError DVertexStatus::
    ReadFromResponseMessage(DryadPnProcessPropertyResponse* response,
                            UInt32 vertexId, UInt32 vertexVersion)
{
    DrStr64 label;
    GetPnPropertyLabel(&label, vertexId, vertexVersion, false);

    response->RetrievePropertyLabel(label);
    DrMemoryBuffer* block = response->GetPropertyBlock();
    if (block != NULL)
    {
        DrMemoryBufferReader reader(block);
        return reader.ReadAggregate(DryadTag_VertexStatus, this, NULL);
    }
    else
    {
        return DrError_InvalidProperty;
    }
}

void DVertexStatus::GetPnPropertyLabel(DrStr* pDstString,
                                       UInt32 vertexId, UInt32 vertexVersion,
                                       bool notifyWaiters)
{
    pDstString->SetF("%s-%u.%u%s",
                     s_StatusPropertyLabel, vertexId, vertexVersion,
                     (notifyWaiters) ? "-update" : "");
}


//
// Create new command block with default properties
//
DVertexCommandBlock::DVertexCommandBlock()
{
    m_command = DVertexCommand_Terminate;
    m_processStatus = new DVertexProcessStatus();
    m_nArguments = 0;
    m_argument = NULL;
    m_serializedBlockLength = 0;
    m_serializedBlock = NULL;
    m_setBreakpointOnCommandArrival = false;
    m_nextArgumentToRead = 0;
}

DVertexCommandBlock::~DVertexCommandBlock()
{
    delete [] m_argument;
}

DVertexCommand DVertexCommandBlock::GetVertexCommand()
{
    return m_command;
}

void DVertexCommandBlock::SetVertexCommand(DVertexCommand command)
{
    m_command = command;
}

DVertexProcessStatus* DVertexCommandBlock::GetProcessStatus()
{
    return m_processStatus;
}

void DVertexCommandBlock::SetProcessStatus(DVertexProcessStatus* processStatus)
{
    m_processStatus = processStatus;
}

UInt32 DVertexCommandBlock::GetArgumentCount()
{
    return m_nArguments;
}

void DVertexCommandBlock::SetArgumentCount(UInt32 nArguments)
{
    delete [] m_argument;
    m_nArguments = nArguments;
    m_argument = new DrStr64[m_nArguments];
    m_nextArgumentToRead = 0;
}

DrStr64* DVertexCommandBlock::GetArgumentVector()
{
    return m_argument;
}

void DVertexCommandBlock::SetArgument(UInt32 argumentIndex,
                                      const char* argument)
{
    LogAssert(argumentIndex < m_nArguments);
    m_argument[argumentIndex].Set(argument);
}

void* DVertexCommandBlock::GetRawSerializedBlock()
{
    return m_serializedBlock;
}

UInt32 DVertexCommandBlock::GetRawSerializedBlockLength()
{
    return m_serializedBlockLength;
}

void DVertexCommandBlock::SetRawSerializedBlock(UInt32 length,
                                                const void* data)
{
    delete [] m_serializedBlock;
    m_serializedBlockLength = length;
    m_serializedBlock = new char[m_serializedBlockLength];
    LogAssert(m_serializedBlock != NULL);
    ::memcpy(m_serializedBlock, data, m_serializedBlockLength);
}

void DVertexCommandBlock::SetDebugBreak(bool setBreakpointOnCommandArrival)
{
    m_setBreakpointOnCommandArrival = setBreakpointOnCommandArrival;
}

bool DVertexCommandBlock::GetDebugBreak()
{
    return m_setBreakpointOnCommandArrival;
}

DrError DVertexCommandBlock::Serialize(DrMemoryWriter* writer)
{
    DrError err;
    UInt32 i;

    writer->WriteUInt16Property(Prop_Dryad_BeginTag, DryadTag_VertexCommand);

    writer->WriteUInt32Property(Prop_Dryad_VertexCommand, m_command);

    err = m_processStatus->Serialize(writer);
    if (err != DrError_OK && writer->GetStatus() == DrError_OK)
    {
        writer->SetStatus(err);
    }

    writer->WriteUInt32Property(Prop_Dryad_VertexArgumentCount,
                                m_nArguments);
    for (i=0; i<m_nArguments; ++i)
    {
        writer->WriteLongDrStrProperty(Prop_Dryad_VertexArgument,
                                       m_argument[i]);
    }

    writer->WriteLongBlobProperty(Prop_Dryad_VertexSerializedBlock,
                                  m_serializedBlockLength, m_serializedBlock);

    writer->WriteBoolProperty(Prop_Dryad_DebugBreak,
                              m_setBreakpointOnCommandArrival);

    writer->WriteUInt16Property(Prop_Dryad_EndTag, DryadTag_VertexCommand);

    return writer->GetStatus();
}

DrError DVertexCommandBlock::OnParseProperty(DrMemoryReader *reader,
                                             UInt16 enumID,
                                             UInt32 dataLen,
                                             void *cookie)
{
    DrError err;

    switch (enumID)
    {
    default:
        DrLogW(
            "Unknown property in vertex command message. enumID %u", (DWORD ) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case Prop_Dryad_VertexCommand:
        UInt32 marshaledCommand;
        err = reader->ReadNextUInt32Property(enumID, &marshaledCommand);
        if (err == DrError_OK)
        {
            if (m_command < DVertexCommand_Max)
            {
                m_command = (DVertexCommand) marshaledCommand;
            }
            else
            {
                err = DrError_InvalidProperty;
            }
        }
        break;

    case Prop_Dryad_VertexArgumentCount:
        UInt32 nArguments;
        err = reader->ReadNextUInt32Property(enumID, &nArguments);
        if (err == DrError_OK)
        {
            SetArgumentCount(nArguments);
        }
        break;

    case Prop_Dryad_VertexArgument:
        if (m_nextArgumentToRead >= m_nArguments)
        {
            DrLogE(
                "Too many arguments. nextArgumentToRead=%u, nArguments=%u",
                m_nextArgumentToRead, m_nArguments);
            err = DrError_InvalidParameter;
        }
        else
        {
            const char* arg;
            err = reader->ReadNextStringProperty(enumID, &arg);
            if (err == DrError_OK)
            {
                SetArgument(m_nextArgumentToRead, arg);
                ++m_nextArgumentToRead;
            }
        }
        break;

    case Prop_Dryad_VertexSerializedBlock:
        UInt32 blockLength;
        const void* blockData;
        err = reader->ReadNextProperty(enumID, &blockLength, &blockData);
        if (err == DrError_OK)
        {
            SetRawSerializedBlock(blockLength, blockData);
        }
        break;

    case Prop_Dryad_DebugBreak:
        bool debugBreak;
        err = reader->ReadNextBoolProperty(enumID, &debugBreak);
        if (err == DrError_OK)
        {
            SetDebugBreak(debugBreak);
        }
        break;

    case Prop_Dryad_BeginTag:
        UInt16  tagValue;
        err = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
        if (err != DrError_OK)
        {
            DrLogE("Error reading Prop_Dryad_BeginTag - 0x08x", err);
        } else {
            switch (tagValue)
            {
            case DryadTag_VertexProcessStatus:
                err = reader->ReadAggregate(tagValue, m_processStatus, NULL);
                break;

            default:
                DrLogW("Unexpected tag - %hu", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

void DVertexCommandBlock::
    StoreInRequestMessage(DryadPnProcessPropertyRequest* request)
{
    DrStr64 label;
    GetPnPropertyLabel(&label,
                       m_processStatus->GetVertexId(),
                       m_processStatus->GetVertexInstanceVersion());

    DrLogI( "Storing command property. Label: %s", label.GetString());

    request->SetPropertyLabel(label, NULL);

    LogAssert(m_command < DVertexCommand_Max);
    request->SetPropertyString(g_dVertexCommandText[m_command]);

    DrMemoryBuffer* block = request->GetPropertyBlock();

    {
        DrMemoryBufferWriter writer(block);
        DrError err = Serialize(&writer);
        LogAssert(err == DrError_OK);
        err = writer.FlushMemoryWriter();
        LogAssert(err == DrError_OK);
    }
}

//
// Get response from message
//
DrError DVertexCommandBlock::
    ReadFromResponseMessage(DryadPnProcessPropertyResponse* response,
                            UInt32 vertexId, UInt32 vertexVersion)
{
    //
    // Get the property associated with this vertex
    //
    DrStr64 label;
    GetPnPropertyLabel(&label, vertexId, vertexVersion);
    response->RetrievePropertyLabel(label);

    //
    // Get the property contents
    //
    DrMemoryBuffer* block = response->GetPropertyBlock();
    if (block != NULL)
    {
        //
        // If non-null return property contents
        //
        DrMemoryBufferReader reader(block);
        return reader.ReadAggregate(DryadTag_VertexCommand, this, NULL);
    }
    else
    {
        //
        // If nothing there, return invalid property
        //
        return DrError_InvalidProperty;
    }
}

void DVertexCommandBlock::GetPnPropertyLabel(DrStr* pDstString,
                                             UInt32 vertexId,
                                             UInt32 vertexVersion)
{
    pDstString->SetF("%s-%u.%u",
                     s_CommandPropertyLabel, vertexId, vertexVersion);
}
