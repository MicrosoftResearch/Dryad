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

#include "stdafx.h"

#include <DataBlockItem.h>

DataBlockItem::DataBlockItem(DrMemoryBuffer* buf)
{
    m_buf = buf;
}

DataBlockItem::DataBlockItem(Size_t size)
{
    m_buf.Attach(new DrSimpleHeapBuffer(size));
}

DataBlockItem::~DataBlockItem()
{
    m_buf.Release();
}

Size_t DataBlockItem::GetAllocatedSize() 
{
    LogAssert(m_buf.Ptr() != NULL);
    return m_buf->GetAllocatedSize();
}


Size_t DataBlockItem::GetAvailableSize() 
{
    LogAssert(m_buf.Ptr() != NULL);
    return m_buf->GetAvailableSize();
}

void DataBlockItem::SetAvailableSize(Size_t size)
{
    LogAssert(m_buf.Ptr() != NULL);
    m_buf->SetAvailableSize(size);
}

void * DataBlockItem::GetDataAddress() 
{
    Size_t size = 0;
    LogAssert(m_buf.Ptr() != NULL);
    void *addr = m_buf->GetDataAddress(0, &size, NULL);
    return addr;
}

DrMemoryBuffer* DataBlockItem::GetData() 
{
    LogAssert(m_buf.Ptr() != NULL);
    return m_buf.Ptr();
}

UInt64 DataBlockItem::GetItemSize() const
{
    return m_buf->GetAvailableSize();
}

DataBlockParser::DataBlockParser(DObjFactoryBase* factory) : 
    RChannelItemParser()
{
    // nothing needed now 
}



RChannelItem* DataBlockParser::ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength)
{
    
    // startOffset should always be zero
    LogAssert(startOffset == 0);

    RChannelBufferData* headBuffer = 
	bufferList->CastOut(bufferList->GetHead());
    *pOutLength = headBuffer->GetData()->GetAvailableSize();

    return new DataBlockItem(headBuffer->GetData());
}

RChannelItem* DataBlockParser::ParsePartialItem(ChannelDataBufferList* bufferList,
						Size_t startOffset,
						RChannelBufferMarker*
						markerBuffer)
{

    // NYI
    return NULL;
}

DrError DataBlockMarshaler::MarshalItem(ChannelMemoryBufferWriter* writer,
                        RChannelItem* item,
                        bool flush,
                        RChannelItemRef* pFailureItem)
{
    if (item->GetType() == RChannelItem_Data)
    {
        DataBlockItem* bufferItem = (DataBlockItem*) item;

        DrMemoryBuffer* buffer = bufferItem->GetData();
        return writer->WriteBytesFromBuffer(buffer, true);
    }
    else
    {
        return DrError_OK;
    }
}
