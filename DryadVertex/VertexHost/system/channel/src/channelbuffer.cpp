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

#include <DrCommon.h>
#include <channelbuffer.h>

#pragma unmanaged


RChannelBuffer::RChannelBuffer(RChannelBufferType type)
{
    m_type = type;
    DryadMetaData::Create(&m_metaData);
}

RChannelBuffer::~RChannelBuffer()
{
}

DryadMetaData* RChannelBuffer::GetMetaData()
{
    return m_metaData.Ptr();
}

void RChannelBuffer::ReplaceMetaData(DryadMetaData* metaData)
{
    m_metaData.Set(metaData);
}

bool RChannelBuffer::IsTerminationBuffer(RChannelBufferType type)
{
    return (type == RChannelBuffer_Restart ||
            type == RChannelBuffer_Abort ||
            type == RChannelBuffer_EndOfStream);
}

RChannelBufferType RChannelBuffer::GetType()
{
    return m_type;
}

RChannelBufferData::RChannelBufferData() : RChannelBuffer(RChannelBuffer_Data)
{
}

RChannelBufferData::~RChannelBufferData()
{
}

//
// Return structure that contains item
//
RChannelBufferData* ChannelDataBufferList::CastOut(DrBListEntry* item)
{
    return DR_GET_CONTAINER(RChannelBufferData, item, m_dataListPtr);
}

//
// Return pointer to data
//
DrBListEntry* ChannelDataBufferList::CastIn(RChannelBufferData* item)
{
    return &(item->m_dataListPtr);
}

RChannelBufferDataDefault::
    RChannelBufferDataDefault(DryadLockedMemoryBuffer* dataBuffer,
                              UInt64 startOffset,
                              RChannelBufferDefaultHandler* parent)
{
    m_dataBuffer = dataBuffer;
    m_startOffset = startOffset;
    m_parent = parent;
}

RChannelBufferDataDefault::~RChannelBufferDataDefault()
{
    m_dataBuffer->DecRef();
}

RChannelBufferDataDefault*
    RChannelBufferDataDefault::Create(DryadLockedMemoryBuffer* dataBuffer,
                                      UInt64 startOffset,
                                      RChannelBufferDefaultHandler* parent)
{
    return new RChannelBufferDataDefault(dataBuffer, startOffset,
                                         parent);
}

DryadLockedMemoryBuffer* RChannelBufferDataDefault::GetData()
{
    return m_dataBuffer;
}

void RChannelBufferDataDefault::
    GetOffsetMetaData(bool isStart,
                      UInt64 offset,
                      DryadMetaDataRef* dstMetaData)
{
    DryadMetaDataRef m;
    GetMetaData()->Clone(&m);

    DryadMTagRef tag;
    tag.Attach(DryadMTagUInt64::Create((isStart) ?
                                       Prop_Dryad_ItemBufferStartOffset :
                                       Prop_Dryad_ItemBufferEndOffset,
                                       offset));
    bool brc = m->Append(tag, false);
    LogAssert(brc == true);

    UInt64 streamOffset =
        (m_startOffset == RCHANNEL_BUFFER_OFFSET_UNDEFINED) ?
        RCHANNEL_BUFFER_OFFSET_UNDEFINED : offset + m_startOffset;

    tag.Attach(DryadMTagUInt64::Create((isStart) ?
                                       Prop_Dryad_ItemStreamStartOffset :
                                       Prop_Dryad_ItemStreamEndOffset,
                                       streamOffset));
    brc = m->Append(tag, false);
    LogAssert(brc == true);

    *dstMetaData = m;
}

//
// Mark a buffer complete
//
void RChannelBufferDataDefault::ProcessingComplete(RChannelBufferPrefetchInfo*
                                                   /* unused prefetchCookie*/)
{
    m_parent->ReturnBuffer(this);
}

RChannelBufferMarker::RChannelBufferMarker(RChannelBufferType type,
                                           RChannelItem* item) :
    RChannelBuffer(type)
{
    LogAssert(item != NULL);
    m_item = item;
}

RChannelBufferMarker::~RChannelBufferMarker()
{
    m_item->DecRef();
}

RChannelItem* RChannelBufferMarker::GetItem()
{
    LogAssert(m_item != NULL);
    return m_item;
}

RChannelBufferMarkerDefault::
    RChannelBufferMarkerDefault(RChannelBufferType type,
                                RChannelItem* item,
                                RChannelBufferDefaultHandler* parent) :
        RChannelBufferMarker(type, item)
{
    m_parent = parent;
}

RChannelBufferMarkerDefault::~RChannelBufferMarkerDefault()
{
}

RChannelBufferMarkerDefault*
    RChannelBufferMarkerDefault::Create(RChannelBufferType type,
                                        RChannelItem* item,
                                        RChannelBufferDefaultHandler* parent)
{
    return new RChannelBufferMarkerDefault(type, item, parent);
}

void RChannelBufferMarkerDefault::
    ProcessingComplete(RChannelBufferPrefetchInfo*
                       /* unused prefetchCookie*/)
{
    m_parent->ReturnBuffer(this);
}
