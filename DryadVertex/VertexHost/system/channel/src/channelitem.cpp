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
#include <channelitem.h>
#include <dryadmetadata.h>
#include <dryaderrordef.h>

#pragma unmanaged


DrResettableMemoryReader::
    DrResettableMemoryReader(DrMemoryBuffer* pMemoryBuffer) :
        DrMemoryBufferReader(pMemoryBuffer)
{
}

void DrResettableMemoryReader::ResetToBufferOffset(Size_t offset)
{
    ResetMemoryReader();
    SetBufferOffset(offset);
}

bool RChannelItem::IsTerminationItem(RChannelItemType type)
{
    return (type == RChannelItem_Restart ||
            type == RChannelItem_Abort ||
            type == RChannelItem_ParseError ||
            type == RChannelItem_MarshalError ||
            type == RChannelItem_EndOfStream);
}

RChannelItem::RChannelItem(RChannelItemType type)
{
    m_type = type;
    m_dataSequenceNumber = s_invalidSequenceNumber;
    m_deliverySequenceNumber = s_invalidSequenceNumber;
}

RChannelItem::~RChannelItem()
{
}

void RChannelItem::Clone(RChannelItemRef* pClonedItem)
{
    DrLogA( "Clone method not implemented");
    *pClonedItem = NULL;
}

UInt64 RChannelItem::GetNumberOfSubItems() const
{
    /* the base class includes marker items and by default has no
       subitems */
    return 0;
}

void RChannelItem::TruncateSubItems(UInt64 numberOfSubItems)
{
    /* the base class includes marker items and by default has no
       subitems */
    LogAssert(false);
}

UInt64 RChannelItem::GetItemSize() const
{
    /* the base class includes marker items and by default marshals to
       zero size */
    return 0;
}

RChannelItemType RChannelItem::GetType()
{
    return m_type;
}

UInt64 RChannelItem::GetDataSequenceNumber()
{
    return m_dataSequenceNumber;
}

void RChannelItem::SetDataSequenceNumber(UInt64 dataSequenceNumber)
{
    m_dataSequenceNumber = dataSequenceNumber;
}

UInt64 RChannelItem::GetDeliverySequenceNumber()
{
    return m_deliverySequenceNumber;
}

void RChannelItem::SetDeliverySequenceNumber(UInt64 deliverySequenceNumber)
{
    m_deliverySequenceNumber = deliverySequenceNumber;
}

DrError RChannelItem::DeSerialize(DrResettableMemoryReader* reader,
                                  Size_t availableSize)
{
    DrLogA("Default DeSerialize method cannot be called on RChannelItem");
    return DrError_Fail;
}

DrError RChannelItem::
    DeSerializePartial(DrResettableMemoryReader* reader,
                       Size_t availableSize)
{
    /* by default, any partial buffer signifies an error */
    return DryadError_ItemParseError;
}

DrError RChannelItem::Serialize(ChannelMemoryBufferWriter* writer)
{
    DrLogA("Default Serialize method cannot be called on RChannelItem");
    return DrError_Fail;
}

DryadMetaData* RChannelItem::GetMetaData()
{
    return m_metaData.Ptr();
}

void RChannelItem::ReplaceMetaData(DryadMetaData* metaData)
{
    m_metaData.Set(metaData);
}

DrError RChannelItem::GetErrorFromItem()
{
    if (m_metaData.Ptr() != NULL)
    {
        DryadMTagDrError* tag =
            m_metaData.Ptr()->LookUpDrErrorTag(Prop_Dryad_ErrorCode);
        if (tag != NULL)
        {
            return tag->GetDrError();
        }
    }

    DrError err;
    switch (m_type)
    {
    case RChannelItem_Data:
        err = DrError_OK;
        break;

    case RChannelItem_BufferHole:
        err = DryadError_BufferHole;
        break;

    case RChannelItem_ItemHole:
        err = DryadError_ItemHole;
        break;

    case RChannelItem_EndOfStream:
        err = DrError_EndOfStream;
        break;

    case RChannelItem_Restart:
        err = DryadError_ChannelRestart;
        break;

    case RChannelItem_Abort:
        err = DryadError_ChannelAbort;
        break;

    case RChannelItem_ParseError:
        err = DryadError_ItemParseError;
        break;

    case RChannelItem_MarshalError:
        err = DryadError_ItemMarshalError;
        break;

    default:
        LogAssert(false);
        err = DrError_InvalidParameter;
    }

    return err;
}
 
RChannelMarkerItem::RChannelMarkerItem(RChannelItemType type) :
    RChannelItem(type)
{
}

RChannelMarkerItem::~RChannelMarkerItem()
{
}

RChannelMarkerItem* RChannelMarkerItem::Create(RChannelItemType type,
                                               bool withMetaData)
{
    RChannelMarkerItem* item = new RChannelMarkerItem(type);
    if (withMetaData)
    {
        DryadMetaDataRef emptyMetaData;
        DryadMetaData::Create(&emptyMetaData);
        item->ReplaceMetaData(emptyMetaData);
    }
    return item;
}

void RChannelMarkerItem::Clone(RChannelItemRef* pClonedItem)
{
    RChannelMarkerItem* clone = new RChannelMarkerItem(GetType());
    clone->ReplaceMetaData(GetMetaData());
    pClonedItem->Attach(clone);
}

//
// Create a custom error item with specified type and error code
//
RChannelItem* RChannelMarkerItem::CreateErrorItem(RChannelItemType itemType,
                                                  DrError errorCode)
{
    RChannelItem* item = Create(itemType, true);
    DryadMetaData* m = item->GetMetaData();
    DryadMTagRef tag;
    tag.Attach(DryadMTagDrError::Create(Prop_Dryad_ErrorCode, errorCode));
    m->Append(tag, false);
    return item;
}

RChannelItem* RChannelMarkerItem::
    CreateErrorItemWithDescription(RChannelItemType itemType,
                                   DrError errorCode,
                                   const char* errorDescription)
{
    RChannelItem* item = Create(itemType, true);
    DryadMetaData* m = item->GetMetaData();
    DryadMTagRef tag;

    tag.Attach(DryadMTagDrError::Create(Prop_Dryad_ErrorCode, errorCode));
    m->Append(tag, false);

    tag.Attach(DryadMTagString::Create(Prop_Dryad_ErrorString,
                                       errorDescription));
    m->Append(tag, false);

    return item;
}


RChannelDataItem::RChannelDataItem() : RChannelItem(RChannelItem_Data)
{
}

RChannelDataItem::~RChannelDataItem()
{
}

UInt64 RChannelDataItem::GetNumberOfSubItems() const
{
    /* by default an item is indivisible, i.e. has one subitem */
    return 1;
}

UInt64 RChannelDataItem::GetItemSize() const
{
    /* the base class doesn't know the size, so just return 1 */
    return 1;
}

RChannelItemArray::RChannelItemArray()
{
    m_numberOfItems = 0;
    m_baseItemArray = NULL;
    m_itemArray = NULL;
}

RChannelItemArray::~RChannelItemArray()
{
    delete [] m_baseItemArray;
}

void RChannelItemArray::SetNumberOfItems(UInt32 numberOfItems)
{
    m_numberOfItems = numberOfItems;
    delete [] m_baseItemArray;
    m_baseItemArray = new RChannelItemRef [m_numberOfItems];
    m_itemArray = m_baseItemArray;
}

void RChannelItemArray::ExtendNumberOfItems(UInt32 numberOfItems)
{
    if (m_numberOfItems < numberOfItems)
    {
        RChannelItemRef* newArray = new RChannelItemRef[numberOfItems];
        LogAssert(newArray != NULL);

        UInt32 i;
        for (i=0; i<m_numberOfItems; ++i)
        {
            newArray[i].TransferFrom(m_itemArray[i]);
        }

        delete [] m_baseItemArray;
        m_baseItemArray = newArray;
        m_itemArray = m_baseItemArray;
        m_numberOfItems = numberOfItems;
    }
}

UInt32 RChannelItemArray::GetNumberOfItems()
{
    return m_numberOfItems;
}

RChannelItemRef* RChannelItemArray::GetItemArray()
{
    return m_itemArray;
}

void RChannelItemArray::TruncateToSize(UInt32 size)
{
    LogAssert(size <= m_numberOfItems);
    m_numberOfItems = size;
}

void RChannelItemArray::DiscardPrefix(UInt32 prefix)
{
    LogAssert(prefix <= m_numberOfItems);
    m_numberOfItems -= prefix;
    m_itemArray += prefix;
}
