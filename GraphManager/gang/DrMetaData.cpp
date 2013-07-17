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

#include <DrGangHeaders.h>

struct DrPropertyType
{
    UINT16 m_tag;
    UINT16 m_type;
};

static DrPropertyType s_propertyType[] =
{
    { DrProp_ChannelState, DrMTT_HRESULT },
    { DrProp_ChannelURI, DrMTT_String },
    { DrProp_ChannelBufferOffset, DrMTT_UInt64 },
    { DrProp_ChannelTotalLength, DrMTT_UInt64 },
    { DrProp_ChannelProcessedLength, DrMTT_UInt64 },
    { DrProp_StreamExpireTimeWhileOpen, DrMTT_TimeInterval },
    { DrProp_StreamExpireTimeWhileClosed, DrMTT_TimeInterval },
    { DrProp_VertexState, DrMTT_HRESULT },
    { DrProp_VertexErrorCode, DrMTT_HRESULT },
    { DrProp_VertexId, DrMTT_UInt32 },
    { DrProp_VertexVersion, DrMTT_UInt32 },
    { DrProp_VertexInputChannelCount, DrMTT_UInt32 },
    { DrProp_VertexOutputChannelCount, DrMTT_UInt32 },
    { DrProp_VertexCommand, DrMTT_VertexCommand },
    { DrProp_VertexArgumentCount, DrMTT_UInt32 },
    { DrProp_VertexArgument, DrMTT_String },
    { DrProp_VertexSerializedBlock, DrMTT_Blob },
    { DrProp_DebugBreak, DrMTT_Boolean },
    { DrProp_AssertFailure, DrMTT_String },
    { DrProp_CanShareWorkQueue, DrMTT_Boolean },
    { DrProp_VertexMaxOpenInputChannelCount, DrMTT_UInt32 },
    { DrProp_VertexMaxOpenOutputChannelCount, DrMTT_UInt32 },
    { DrProp_ErrorCode, DrMTT_HRESULT },
    { DrProp_ErrorString, DrMTT_String },
    { DrProp_ItemBufferStartOffset, DrMTT_UInt64 },
    { DrProp_ItemBufferEndOffset, DrMTT_UInt64 },
    { DrProp_BufferLength, DrMTT_UInt64 },
    { DrProp_ItemStreamStartOffset, DrMTT_UInt64 },
    { DrProp_ItemStreamEndOffset, DrMTT_UInt64 },
    { DrProp_ItemDataSequenceNumber, DrMTT_UInt64 },
    { DrProp_ItemDeliverySequenceNumber, DrMTT_UInt64 },
    { DrProp_InputPortCount, DrMTT_UInt32 },
    { DrProp_OutputPortCount, DrMTT_UInt32 },
    { DrProp_NumberOfVertices, DrMTT_UInt32 },
    { DrProp_SourceVertex, DrMTT_UInt32 },
    { DrProp_SourcePort, DrMTT_UInt32 },
    { DrProp_DestinationVertex, DrMTT_UInt32 },
    { DrProp_DestinationPort, DrMTT_UInt32 },
    { DrProp_NumberOfEdges, DrMTT_UInt32 },
    { DrProp_TryToCreateChannelPath, DrMTT_Void },
    { DrProp_InitialChannelWriteSize, DrMTT_UInt64 },
    { 0xffff, 0xffff }
};

DrMetaData::DrMetaData()
{
    m_tagList = DrNew DrMTagList();
}

void DrMetaData::Append(DrMTagPtr tag)
{
    m_tagList->Add(tag);
}

DrMTagListPtr DrMetaData::GetTags()
{
    return m_tagList;
}

DrMTagPtr DrMetaData::LookUp(UINT16 enumId)
{
    int i;
    for (i=0; i<m_tagList->Size(); ++i)
    {
        if (m_tagList[i]->GetMTag() == enumId)
        {
            return m_tagList[i];
        }
    }

    return DrNull;
}

void DrMetaData::Serialize(DrPropertyWriterPtr writer)
{
    if (m_cachedSerialization == DrNull)
    {
        int i;
        for (i=0; i<m_tagList->Size(); ++i)
        {
            m_tagList[i]->Serialize(writer);
        }
    }
    else
    {
        DRPIN(BYTE) src = &(m_cachedSerialization[0]);
        writer->WriteBytes(src, m_cachedSerialization->Allocated());
    }
}

void DrMetaData::CacheSerialization()
{
    DrPropertyWriterRef writer = DrNew DrPropertyWriter();
    Serialize(writer);
    m_cachedSerialization = writer->GetBuffer();
}

HRESULT DrMetaData::ParseProperty(DrPropertyReaderPtr reader, UINT16 enumId, UINT32 dataLen)
{
    UINT16 type = DrMTT_Unknown;
    int i;
    for (i=0; s_propertyType[i].m_tag != 0xffff; ++i)
    {
        if (s_propertyType[i].m_tag == enumId)
        {
            type = s_propertyType[i].m_type;
            break;
        }
    }

    DrMTagRef tag = DrMTag::MakeTyped(enumId, type);

    HRESULT status = tag->ParseProperty(reader, enumId, dataLen);
    if (status == S_OK)
    {
        Append(tag);
    }

    return status;
}