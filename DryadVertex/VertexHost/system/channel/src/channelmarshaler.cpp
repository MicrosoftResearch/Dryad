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

#include <channelmarshaler.h>
#include <dryaderrordef.h>

#pragma unmanaged


RChannelItemMarshalerBase::RChannelItemMarshalerBase()
{
    m_maxMarshalBatchSize = 0;
}

RChannelItemMarshalerBase::~RChannelItemMarshalerBase()
{
}

void RChannelItemMarshalerBase::Reset()
{
}

void RChannelItemMarshalerBase::
    SetMaxMarshalBatchSize(UInt32 maxMarshalBatchSize)
{
    m_maxMarshalBatchSize = maxMarshalBatchSize;
}

UInt32 RChannelItemMarshalerBase::GetMaxMarshalBatchSize()
{
    return m_maxMarshalBatchSize;
}

void RChannelItemMarshalerBase::SetMarshalerIndex(UInt32 index)
{
    m_index = index;
}

UInt32 RChannelItemMarshalerBase::GetMarshalerIndex()
{
    return m_index;
}

void RChannelItemMarshalerBase::SetMarshalerContext(RChannelContext* context)
{
    m_context = context;
}

RChannelContext* RChannelItemMarshalerBase::GetMarshalerContext()
{
    return m_context;
}


RChannelItemMarshaler::~RChannelItemMarshaler()
{
}

RChannelStdItemMarshalerBase::~RChannelStdItemMarshalerBase()
{
}

DrError RChannelStdItemMarshalerBase::
    MarshalItem(ChannelMemoryBufferWriter* writer,
                RChannelItem* item,
                bool flush,
                RChannelItemRef* pFailureItem)
{
    if (item->GetType() != RChannelItem_Data)
    {
        return MarshalMarker(writer, item, flush, pFailureItem);
    }

    DrError err = item->Serialize(writer);

    if (err != DrError_OK && err != DrError_IncompleteOperation)
    {
        pFailureItem->Attach(RChannelMarkerItem::
                             CreateErrorItem(RChannelItem_MarshalError,
                                             err));
        return DryadError_ChannelAbort;
    }

    return err;
}

DrError RChannelStdItemMarshalerBase::
    MarshalMarker(ChannelMemoryBufferWriter* writer,
                  RChannelItem* item,
                  bool flush,
                  RChannelItemRef* pFailureItem)
{
    return DrError_OK;
}

RChannelStdItemMarshaler::~RChannelStdItemMarshaler()
{
}
