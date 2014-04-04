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

#include <DrVertexHeaders.h>

static const char* s_StatusPropertyLabel = "DVertexStatus";
static const char* s_CommandPropertyLabel = "DVertexCommand";

DrChannelDescription::DrChannelDescription(bool isInputChannel)
{
    m_state = DrError_ChannelAbort;
    m_errorCode = S_OK;
    m_totalLength = 0;
    m_processedLength = 0;
    m_isInputChannel = isInputChannel;
}

DrChannelDescription::~DrChannelDescription()
{
}

HRESULT DrChannelDescription::GetChannelState()
{
    return m_state;
}

void DrChannelDescription::SetChannelState(HRESULT state)
{
    m_state = state;
}

DrString DrChannelDescription::GetChannelURI()
{
    return m_URI;
}

void DrChannelDescription::SetChannelURI(DrString channelURI)
{
    m_URI = channelURI;
}

DrMetaDataPtr DrChannelDescription::GetChannelMetaData()
{
    return m_metaData;
}

void DrChannelDescription::SetChannelMetaData(DrMetaDataPtr metaData)
{
    m_metaData = metaData;
}

HRESULT DrChannelDescription::GetChannelErrorCode()
{
    return m_errorCode;
}

void DrChannelDescription::SetChannelErrorCode(HRESULT errorCode)
{
    m_errorCode = errorCode;
}

DrString DrChannelDescription::GetChannelErrorString()
{
    return m_errorString;
}

void DrChannelDescription::SetChannelErrorString(DrString errorString)
{
    m_errorString = errorString;
}

UINT64 DrChannelDescription::GetChannelTotalLength()
{
    return m_totalLength;
}

void DrChannelDescription::SetChannelTotalLength(UINT64 totalLength)
{
    m_totalLength = totalLength;
}

UINT64 DrChannelDescription::GetChannelProcessedLength()
{
    return m_processedLength;
}

void DrChannelDescription::SetChannelProcessedLength(UINT64 processedLength)
{
    m_processedLength = processedLength;
}

void DrChannelDescription::Serialize(DrPropertyWriterPtr writer)
{
    UINT16 tagValue = (m_isInputChannel) ?
        DrTag_InputChannelDescription :
        DrTag_OutputChannelDescription;

    writer->WriteProperty(DrProp_BeginTag, tagValue);

    writer->WriteProperty(DrProp_ChannelState, m_state);
    writer->WriteProperty(DrProp_ChannelURI, m_URI);
    writer->WriteProperty(DrProp_ChannelTotalLength, m_totalLength);
    writer->WriteProperty(DrProp_ChannelProcessedLength, m_processedLength);
    if (m_metaData != DrNull)
    {
        writer->WriteProperty(DrProp_BeginTag, DrTag_ChannelMetaData);
        m_metaData->Serialize(writer);
        writer->WriteProperty(DrProp_EndTag, DrTag_ChannelMetaData);
    }
    if (m_errorCode != S_OK)
    {
        writer->WriteProperty(DrProp_ChannelErrorCode, m_errorCode);
    }
    if (m_errorString.GetChars() != NULL)
    {
        writer->WriteProperty(DrProp_ChannelErrorString, m_errorString);
    }

    writer->WriteProperty(DrProp_EndTag, tagValue);
}

HRESULT DrChannelDescription::ParseProperty(DrPropertyReaderPtr reader,
                                            UINT16 enumID, UINT32 /* unused dataLen */)
{
    HRESULT err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in channel description enumID %u", (UINT32) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case DrProp_ChannelState:
        err = reader->ReadNextProperty(enumID, m_state);
        break;

    case DrProp_ChannelURI:
        {
            DrString URI;
            err = reader->ReadNextProperty(enumID, URI);
            if (err == S_OK)
            {
                SetChannelURI(URI);
            }
        }
        break;

    case DrProp_ChannelErrorCode:
        err = reader->ReadNextProperty(enumID, m_errorCode);
        break;

    case DrProp_ChannelErrorString:
        {
            DrString errorString;
            err = reader->ReadNextProperty(enumID, errorString);
            if (err == S_OK)
            {
                SetChannelErrorString(errorString);
            }
        }
        break;

    case DrProp_ChannelTotalLength:
        err = reader->ReadNextProperty(enumID, m_totalLength);
        break;

    case DrProp_ChannelProcessedLength:
        err = reader->ReadNextProperty(enumID, m_processedLength);
        break;

    case DrProp_BeginTag:
        {
            UINT16 tagID;
            err = reader->PeekNextAggregateTag(&tagID);
            if (err == S_OK)
            {
                if (tagID == DrTag_ChannelMetaData)
                {
                    DrMetaDataRef mData = DrNew DrMetaData();
                    err = reader->ReadAggregate(tagID, mData);
                    if (err == S_OK)
                    {
                        SetChannelMetaData(mData);
                    }
                }
                else
                {
                    DrLogW("Unknown aggregate in channel description tagID %u", (UINT32) tagID);
                }
            }
        }
        break;
    }

    return err;
}

void DrChannelDescription::CopyFrom(DrChannelDescriptionPtr src, bool includeLengths)
{
    DrAssert(m_isInputChannel == src->m_isInputChannel);

    SetChannelURI(src->GetChannelURI());
    SetChannelState(src->GetChannelState());
    SetChannelMetaData(src->GetChannelMetaData());
    SetChannelErrorCode(src->GetChannelErrorCode());
    SetChannelErrorString(src->GetChannelErrorString());
    if (includeLengths)
    {
        SetChannelProcessedLength(src->GetChannelProcessedLength());
        SetChannelTotalLength(src->GetChannelTotalLength());
    }
}


DrInputChannelDescription::DrInputChannelDescription() :
    DrChannelDescription(true)
{
}


DrOutputChannelDescription::DrOutputChannelDescription() :
    DrChannelDescription(false)
{
}


DrVertexProcessStatus::DrVertexProcessStatus()
{
    m_id = 0;
    m_version = 0;
    m_errorCode = S_OK;
    m_maxInputChannels = 0;
    m_maxOutputChannels = 0;
    m_canShareWorkQueue = false;

    m_nextInputChannelToRead = 0;
    m_nextOutputChannelToRead = 0;
}

int DrVertexProcessStatus::GetVertexId()
{
    return m_id;
}

void DrVertexProcessStatus::SetVertexId(int vertexId)
{
    m_id = vertexId;
}

int DrVertexProcessStatus::GetVertexInstanceVersion()
{
    return m_version;
}

void DrVertexProcessStatus::SetVertexInstanceVersion(int version)
{
    m_version = version;
}

DrMetaDataPtr DrVertexProcessStatus::GetVertexMetaData()
{
    return m_metaData;
}

void DrVertexProcessStatus::SetVertexMetaData(DrMetaDataPtr metaData)
{
    m_metaData = metaData;
}

HRESULT DrVertexProcessStatus::GetVertexErrorCode()
{
    return m_errorCode;
}

void DrVertexProcessStatus::SetVertexErrorCode(HRESULT errorCode)
{
    m_errorCode = errorCode;
}

DrString DrVertexProcessStatus::GetVertexErrorString()
{
    return m_errorString;
}

void DrVertexProcessStatus::SetVertexErrorString(DrString errorString)
{
    m_errorString = errorString;
}

DrInputChannelArrayRef DrVertexProcessStatus::GetInputChannels()
{
    return m_inputChannel;
}

void DrVertexProcessStatus::SetInputChannelCount(int nInputChannels)
{
    DrAssert(nInputChannels >= 0);
    m_inputChannel = DrNew DrInputChannelArray(nInputChannels);
    int i;
    for (i=0; i<nInputChannels; ++i)
    {
        m_inputChannel[i] = DrNew DrInputChannelDescription();
    }
    m_nextInputChannelToRead = 0;
}

int DrVertexProcessStatus::GetMaxOpenInputChannelCount()
{
    return m_maxInputChannels;
}

void DrVertexProcessStatus::SetMaxOpenInputChannelCount(int channelCount)
{
    DrAssert(channelCount >= 0);
    m_maxInputChannels = channelCount;
}

DrOutputChannelArrayRef DrVertexProcessStatus::GetOutputChannels()
{
    return m_outputChannel;
}

void DrVertexProcessStatus::SetOutputChannelCount(int nOutputChannels)
{
    DrAssert(nOutputChannels >= 0);
    m_outputChannel = DrNew DrOutputChannelArray(nOutputChannels);
    int i;
    for (i=0; i<nOutputChannels; ++i)
    {
        m_outputChannel[i] = DrNew DrOutputChannelDescription();
    }
    m_nextOutputChannelToRead = 0;
}

int DrVertexProcessStatus::GetMaxOpenOutputChannelCount()
{
    return m_maxOutputChannels;
}

void DrVertexProcessStatus::SetMaxOpenOutputChannelCount(int channelCount)
{
    DrAssert(channelCount >= 0);
    m_maxOutputChannels = channelCount;
}

bool DrVertexProcessStatus::GetCanShareWorkQueue()
{
    return m_canShareWorkQueue;
}

void DrVertexProcessStatus::SetCanShareWorkQueue(bool canShareWorkQueue)
{
    m_canShareWorkQueue = canShareWorkQueue;
}

void DrVertexProcessStatus::Serialize(DrPropertyWriterPtr writer)
{
    int i;

    writer->WriteProperty(DrProp_BeginTag, DrTag_VertexProcessStatus);

    writer->WriteProperty(DrProp_VertexId, (UINT32) m_id);
    writer->WriteProperty(DrProp_VertexVersion, (UINT32) m_version);
    if (m_metaData != DrNull)
    {
        writer->WriteProperty(DrProp_BeginTag, DrTag_VertexMetaData);
        m_metaData->Serialize(writer);
        writer->WriteProperty(DrProp_EndTag, DrTag_VertexMetaData);
    }
    if (m_errorCode != S_OK)
    {
        writer->WriteProperty(DrProp_VertexErrorCode, m_errorCode);
    }
    if (m_errorString.GetChars() != NULL)
    {
        writer->WriteProperty(DrProp_VertexErrorString, m_errorString);
    }

    if (m_inputChannel == DrNull)
    {
        writer->WriteProperty(DrProp_VertexInputChannelCount, (UINT32) 0);
    }
    else
    {
        writer->WriteProperty(DrProp_VertexInputChannelCount, (UINT32) m_inputChannel->Allocated());
        for (i=0; i<m_inputChannel->Allocated(); ++i)
        {
            m_inputChannel[i]->Serialize(writer);
        }
    }
    writer->WriteProperty(DrProp_VertexMaxOpenInputChannelCount, (UINT32) m_maxInputChannels);

    if (m_outputChannel == DrNull)
    {
        writer->WriteProperty(DrProp_VertexOutputChannelCount, (UINT32) 0);
    }
    else
    {
        writer->WriteProperty(DrProp_VertexOutputChannelCount, (UINT32) m_outputChannel->Allocated());
        for (i=0; i<m_outputChannel->Allocated(); ++i)
        {
            m_outputChannel[i]->Serialize(writer);
        }
    }
    writer->WriteProperty(DrProp_VertexMaxOpenOutputChannelCount, (UINT32) m_maxOutputChannels);

    writer->WriteProperty(DrProp_CanShareWorkQueue, m_canShareWorkQueue);

    writer->WriteProperty(DrProp_EndTag, DrTag_VertexProcessStatus);
}

HRESULT DrVertexProcessStatus::ParseProperty(DrPropertyReaderPtr reader,
                                             UINT16 enumID, UINT32 /* unused dataLen */)
{
    HRESULT err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in vertex status message enumID %u", (UINT32) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case DrProp_VertexId:
        UINT32 id;
        err = reader->ReadNextProperty(enumID, id);
        if (err == S_OK)
        {
            if (id >= 0x80000000)
            {
                DrLogW("Vertex ID out of range %u", id);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                m_id = id;
            }
        }
        break;

    case DrProp_VertexVersion:
        UINT32 version;
        err = reader->ReadNextProperty(enumID, version);
        if (err == S_OK)
        {
            if (version >= 0x80000000)
            {
                DrLogW("Vertex version out of range %u", version);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                m_version = version;
            }
        }
        break;

    case DrProp_VertexErrorCode:
        err = reader->ReadNextProperty(enumID, m_errorCode);
        break;

    case DrProp_VertexErrorString:
        {
            DrString errorString;
            err = reader->ReadNextProperty(enumID, errorString);
            if (err == S_OK)
            {
                SetVertexErrorString(errorString);
            }
        }
        break;

    case DrProp_VertexInputChannelCount:
        UINT32 nInputChannels;
        err = reader->ReadNextProperty(enumID, nInputChannels);
        if (err == S_OK)
        {
            if (nInputChannels >= 0x80000000)
            {
                DrLogW("Too many input channels %u", nInputChannels);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                SetInputChannelCount((int) nInputChannels);
            }
        }
        break;

    case DrProp_VertexMaxOpenInputChannelCount:
        UINT32 maxInputChannels;
        err = reader->ReadNextProperty(enumID, maxInputChannels);
        if (err == S_OK)
        {
            if (maxInputChannels >= 0x80000000)
            {
                DrLogW("Too many max input channels %u", maxInputChannels);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                m_maxInputChannels = (int) maxInputChannels;
            }
        }
        break;

    case DrProp_VertexOutputChannelCount:
        UINT32 nOutputChannels;
        err = reader->ReadNextProperty(enumID, nOutputChannels);
        if (err == S_OK)
        {
            if (nOutputChannels >= 0x80000000)
            {
                DrLogW("Too many output channels %u", nOutputChannels);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                SetOutputChannelCount((int) nOutputChannels);
            }
        }
        break;

    case DrProp_VertexMaxOpenOutputChannelCount:
        UINT32 maxOutputChannels;
        err = reader->ReadNextProperty(enumID, maxOutputChannels);
        if (err == S_OK)
        {
            if (maxOutputChannels >= 0x80000000)
            {
                DrLogW("Too many max output channels %d", maxOutputChannels);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
            else
            {
                m_maxOutputChannels = (int) maxOutputChannels;
            }
        }
        break;

    case DrProp_CanShareWorkQueue:
        err = reader->ReadNextProperty(enumID, m_canShareWorkQueue);
        break;

    case DrProp_BeginTag:
        UINT16 tagValue;
        err = reader->PeekNextAggregateTag(&tagValue);
        if (err != S_OK)
        {
            DrLogW("Error reading DrProp_BeginTag %d", err);
        }
        else
        {
            switch (tagValue)
            {
            case DrTag_InputChannelDescription:
                if (m_nextInputChannelToRead >= m_inputChannel->Allocated())
                {
                    DrLogW("Too many input channel descriptions nextInputChannelToRead=%d, nInputChannels=%d",
                           m_nextInputChannelToRead, m_inputChannel->Allocated());
                    err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
                }
                else
                {
                    err = reader->ReadAggregate(tagValue, m_inputChannel[m_nextInputChannelToRead]);
                    if (err == S_OK)
                    {
                        ++m_nextInputChannelToRead;
                    }
                }
                break;

            case DrTag_OutputChannelDescription:
                if (m_nextOutputChannelToRead >= m_outputChannel->Allocated())
                {
                    DrLogW("Too many output channel descriptions nextOutputChannelToRead=%d, nOutputChannels=%d",
                           m_nextOutputChannelToRead, m_outputChannel->Allocated());
                    err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
                }
                else
                {
                    err = reader->ReadAggregate(tagValue, m_outputChannel[m_nextOutputChannelToRead]);
                    if (err == S_OK)
                    {
                        ++m_nextOutputChannelToRead;
                    }
                }
                break;

            case DrTag_VertexMetaData:
                {
                    DrMetaDataRef metaData = DrNew DrMetaData();
                    err = reader->ReadAggregate(tagValue, metaData);
                    if (err == S_OK)
                    {
                        m_metaData = metaData;
                    }
                }
                break;

            default:
                DrLogW("Unexpected tag %d", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

void DrVertexProcessStatus::CopyFrom(DrVertexProcessStatusPtr src,
                                     bool includeLengths)
{
    int i;

    SetVertexId(src->GetVertexId());
    SetVertexInstanceVersion(src->GetVertexInstanceVersion());
    SetVertexMetaData(src->GetVertexMetaData());
    SetVertexErrorCode(src->GetVertexErrorCode());
    SetVertexErrorString(src->GetVertexErrorString());

    SetInputChannelCount(src->GetInputChannels()->Allocated());
    DrInputChannelArrayRef srcInputs = src->GetInputChannels();
    for (i=0; i<m_inputChannel->Allocated(); ++i)
    {
        m_inputChannel[i]->CopyFrom(srcInputs[i], includeLengths);
    }
    SetMaxOpenInputChannelCount(src->GetMaxOpenInputChannelCount());

    SetOutputChannelCount(src->GetOutputChannels()->Allocated());
    DrOutputChannelArrayRef srcOutputs = src->GetOutputChannels();
    for (i=0; i<m_outputChannel->Allocated(); ++i)
    {
        m_outputChannel[i]->CopyFrom(srcOutputs[i], includeLengths);
    }
    SetMaxOpenOutputChannelCount(src->GetMaxOpenOutputChannelCount());
}


DrVertexStatus::DrVertexStatus()
{
    m_state = S_OK;
    m_processStatus = DrNew DrVertexProcessStatus();
}

HRESULT DrVertexStatus::GetVertexState()
{
    return m_state;
}

void DrVertexStatus::SetVertexState(HRESULT state)
{
    m_state = state;
}

DrVertexProcessStatusPtr DrVertexStatus::GetProcessStatus()
{
    return m_processStatus;
}

void DrVertexStatus::SetProcessStatus(DrVertexProcessStatusPtr processStatus)
{
    m_processStatus = processStatus;
}

void DrVertexStatus::Serialize(DrPropertyWriterPtr writer)
{
    writer->WriteProperty(DrProp_BeginTag, DrTag_VertexStatus);

    writer->WriteProperty(DrProp_VertexState, m_state);
    m_processStatus->Serialize(writer);

    writer->WriteProperty(DrProp_EndTag, DrTag_VertexStatus);
}

HRESULT DrVertexStatus::ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID,
                                      UINT32 /* unused dataLen */)
{
    HRESULT err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in vertex status message enumID %u", (UINT32) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case DrProp_VertexState:
        err = reader->ReadNextProperty(enumID, m_state);
        break;

    case DrProp_BeginTag:
        UINT16 tagValue;
        err = reader->PeekNextAggregateTag(&tagValue);
        if (err != S_OK)
        {
            DrLogW("Error reading DrProp_BeginTag %d", err);
        }
        else
        {
            switch (tagValue)
            {
            case DrTag_VertexProcessStatus:
                err = reader->ReadAggregate(tagValue, m_processStatus);
                break;

            default:
                DrLogW("Unexpected tag %d", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

DrString DrVertexStatus::GetPropertyLabel(int vertexId, int vertexVersion)
{
    DrString s;
    s.SetF("%s-%d.%d", s_StatusPropertyLabel, vertexId, vertexVersion);
    return s;
}


DrVertexCommandBlock::DrVertexCommandBlock()
{
    m_command = DrVC_Terminate;
    m_processStatus = DrNew DrVertexProcessStatus();
    m_setBreakpointOnCommandArrival = false;
    m_nextArgumentToRead = 0;
}

DrVertexCommand DrVertexCommandBlock::GetVertexCommand()
{
    return m_command;
}

void DrVertexCommandBlock::SetVertexCommand(DrVertexCommand command)
{
    m_command = command;
}

DrVertexProcessStatusPtr DrVertexCommandBlock::GetProcessStatus()
{
    return m_processStatus;
}

void DrVertexCommandBlock::SetProcessStatus(DrVertexProcessStatusPtr processStatus)
{
    m_processStatus = processStatus;
}

void DrVertexCommandBlock::SetArgumentCount(int nArguments)
{
    m_argument = DrNew DrStringArray(nArguments);
    m_nextArgumentToRead = 0;
}

DrStringArrayRef DrVertexCommandBlock::GetArgumentVector()
{
    return m_argument;
}

DrByteArrayPtr DrVertexCommandBlock::GetRawSerializedBlock()
{
    return m_serializedBlock;
}

void DrVertexCommandBlock::SetRawSerializedBlock(DrByteArrayPtr block)
{
    m_serializedBlock = block;
}

void DrVertexCommandBlock::SetDebugBreak(bool setBreakpointOnCommandArrival)
{
    m_setBreakpointOnCommandArrival = setBreakpointOnCommandArrival;
}

bool DrVertexCommandBlock::GetDebugBreak()
{
    return m_setBreakpointOnCommandArrival;
}

void DrVertexCommandBlock::Serialize(DrPropertyWriterPtr writer)
{
    int i;

    writer->WriteProperty(DrProp_BeginTag, DrTag_VertexCommand);

    writer->WriteProperty(DrProp_VertexCommand, (UINT32) m_command);

    m_processStatus->Serialize(writer);

	if (m_argument == DrNull)
	{
		writer->WriteProperty(DrProp_VertexArgumentCount, 0);
	}
	else
	{
		writer->WriteProperty(DrProp_VertexArgumentCount, (UINT32) m_argument->Allocated());
		for (i=0; i<m_argument->Allocated(); ++i)
		{
			writer->WriteProperty(DrProp_VertexArgument, m_argument[i]);
		}
	}

	if (m_serializedBlock != DrNull)
	{
		DRPIN(BYTE) block = &(m_serializedBlock[0]);
		writer->WriteProperty(DrProp_VertexSerializedBlock, m_serializedBlock->Allocated(), block);
	}

    writer->WriteProperty(DrProp_DebugBreak, m_setBreakpointOnCommandArrival);

    writer->WriteProperty(DrProp_EndTag, DrTag_VertexCommand);
}

HRESULT DrVertexCommandBlock::ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID,
                                            UINT32 /* unused dataLen */)
{
    HRESULT err;

    switch (enumID)
    {
    default:
        DrLogW("Unknown property in vertex command message enumID %u", (UINT32) enumID);
        err = reader->SkipNextPropertyOrAggregate();
        break;

    case DrProp_VertexCommand:
        UINT32 marshaledCommand;
        err = reader->ReadNextProperty(enumID, marshaledCommand);
        if (err == S_OK)
        {
            if (marshaledCommand < DrVC_Max)
            {
                m_command = (DrVertexCommand) marshaledCommand;
            }
            else
            {
                DrLogW("Unknown vertex command %u", marshaledCommand);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
        }
        break;

    case DrProp_VertexArgumentCount:
        UINT32 nArguments;
        err = reader->ReadNextProperty(enumID, nArguments);
        if (err == S_OK)
        {
            if (nArguments < 0x80000000)
            {
                SetArgumentCount((int) nArguments);
            }
            else
            {
                DrLogW("Too large argument count %u", nArguments);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
        }
        break;

    case DrProp_VertexArgument:
        if (m_nextArgumentToRead >= m_argument->Allocated())
        {
            DrLogW("Too many arguments nextArgumentToRead=%d, nArguments=%d",
                   m_nextArgumentToRead, m_argument->Allocated());
            err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
        }
        else
        {
            DrString arg;
            err = reader->ReadNextProperty(enumID, arg);
            if (err == S_OK)
            {
                m_argument[m_nextArgumentToRead] = arg;
                ++m_nextArgumentToRead;
            }
        }
        break;

    case DrProp_VertexSerializedBlock:
        UINT32 blockLength;
        err = reader->PeekNextPropertyTag(&enumID, &blockLength);
        if (err == S_OK)
        {
            if (blockLength < 0x80000000)
            {
                DrByteArrayRef block = DrNew DrByteArray((int) blockLength);
                {
                    DRPIN(BYTE) data = &(block[0]);
                    err = reader->ReadNextProperty(enumID, (UINT32) blockLength, data);
                }
                if (err == S_OK)
                {
                    m_serializedBlock = block;
                }
            }
            else
            {
                DrLogW("Block too large %u", blockLength);
                err = HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
            }
        }
        break;

    case DrProp_DebugBreak:
        err = reader->ReadNextProperty(enumID, m_setBreakpointOnCommandArrival);
        break;

    case DrProp_BeginTag:
        UINT16 tagValue;
        err = reader->PeekNextAggregateTag(&tagValue);
        if (err != S_OK)
        {
            DrLogW("Error reading DrProp_BeginTag %d", err);
        }
        else
        {
            switch (tagValue)
            {
            case DrTag_VertexProcessStatus:
                err = reader->ReadAggregate(tagValue, m_processStatus);
                break;

            default:
                DrLogW("Unexpected tag %d", tagValue);
                err = reader->SkipNextPropertyOrAggregate();
            }
        }
        break;
    }

    return err;
}

DrString DrVertexCommandBlock::GetPropertyLabel(int vertexId, int vertexVersion)
{
    DrString s;
    s.SetF("%s-%d.%d", s_CommandPropertyLabel, vertexId, vertexVersion);
    return s;
}
