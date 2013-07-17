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

#include <channelparser.h>
#include <channelbuffer.h>
#include <channelmemorybuffers.h>
#include <dryadmetadata.h>
#include <dryadtagsdef.h>

#pragma unmanaged


void RChannelBufferRecord::SetData(DrMemoryBuffer* data)
{
    m_buffer = data;
}

DrMemoryBuffer* RChannelBufferRecord::GetData() const
{
    return m_buffer;
}


RChannelBufferItem::RChannelBufferItem(DrMemoryBuffer* buffer)
{
    SetNumberOfRecords(1);
    GetRecordArray()[0].SetData(buffer);
}

RChannelBufferItem* RChannelBufferItem::Create(DrMemoryBuffer* buffer)
{
    return new RChannelBufferItem(buffer);
}

DrMemoryBuffer* RChannelBufferItem::GetData() const
{
    return GetRecordArray()[0].GetData();
}

UInt64 RChannelBufferItem::GetItemSize() const
{
    return GetData()->GetAvailableSize();
}


RChannelItemParserBase::RChannelItemParserBase()
{
    m_maxParseBatchSize = 0;
    m_index = 0;
}

RChannelItemParserBase::~RChannelItemParserBase()
{
}

void RChannelItemParserBase::SetMaxParseBatchSize(UInt32 maxParseBatchSize)
{
    m_maxParseBatchSize = maxParseBatchSize;
}

UInt32 RChannelItemParserBase::GetMaxParseBatchSize()
{
    return m_maxParseBatchSize;
}

void RChannelItemParserBase::SetParserIndex(UInt32 index)
{
    m_index = index;
}

UInt32 RChannelItemParserBase::GetParserIndex()
{
    return m_index;
}

void RChannelItemParserBase::SetParserContext(RChannelContext* context)
{
    m_context = context;
}

RChannelContext* RChannelItemParserBase::GetParserContext()
{
    return m_context;
}


RChannelRawItemParser::~RChannelRawItemParser()
{
}


RChannelItemTransformerBase::~RChannelItemTransformerBase()
{
}

void RChannelItemTransformerBase::
    InitializeTransformer(RChannelItemParserBase* parent,
                          DVErrorReporter* errorReporter)
{
}

void RChannelItemTransformerBase::
    TransformItem(RChannelItemRef& inputItem,
                  SyncItemWriterBase* writer,
                  DVErrorReporter* errorReporter)
{
    writer->WriteItemSyncConsumingReference(inputItem);
}

void RChannelItemTransformerBase::
    FlushTransformer(SyncItemWriterBase* writer,
                     DVErrorReporter* errorReporter)
{
}

void RChannelItemTransformerBase::
    ReportTransformerErrorItem(RChannelItemRef& errorItem,
                               DVErrorReporter* errorReporter)
{
}


RChannelItemTransformer::~RChannelItemTransformer()
{
}


RChannelTransformerParserBase::RChannelTransformerParserBase()
{
    m_transformedAny = false;
}

RChannelTransformerParserBase::~RChannelTransformerParserBase()
{
    LogAssert(m_itemList.IsEmpty());
}

void RChannelTransformerParserBase::
    SetTransformer(RChannelItemTransformerBase* transformer)
{
    m_transformer = transformer;
}

RChannelItemTransformerBase* RChannelTransformerParserBase::GetTransformer()
{
    return m_transformer;
}

RChannelItem* RChannelTransformerParserBase::
    RawParseItem(bool restartParser,
                 RChannelBuffer* inData,
                 RChannelBufferPrefetchInfo** outPrefetchCookie)
{
    *outPrefetchCookie = NULL;

    if (restartParser)
    {
        if (m_transformedAny)
        {
            DrLogI("Flushing transformer for restart");

            m_transformer->FlushTransformer(this, this);
            /* clear any errors */
            ReportError(DrError_OK, (DryadMetaData*) NULL);
            
            RChannelItemRef item;
            while (m_itemList.IsEmpty() == false)
            {
                /* remove the saved item from the list; it will be
                   garbage collected when the refcount goes out of
                   scope */
                item.Attach(m_itemList.CastOut(m_itemList.RemoveHead()));
            }

            m_transformedAny = false;
        }
        else
        {
            LogAssert(m_itemList.IsEmpty());
            m_transformer->InitializeTransformer(this, this);
        }
    }

    if (m_itemList.IsEmpty() == false)
    {
        LogAssert(m_transformedAny);
        LogAssert(inData == NULL);
        RChannelItem* item = m_itemList.CastOut(m_itemList.RemoveHead());
        return item;
    }

    if (inData == NULL)
    {
        LogAssert(m_transformedAny);
        /* we have sent back all the items that got written from the
           last buffer, but the base class doesn't know this yet, so
           inform it by returning NULL */
        return NULL;
    }

    m_transformedAny = true;

    RChannelBufferType bType = inData->GetType();
    if (bType == RChannelBuffer_Hole ||
        bType == RChannelBuffer_EndOfStream)
    {
        RChannelBufferMarker* mBuffer =
            dynamic_cast<RChannelBufferMarker*>(inData);
        LogAssert(mBuffer != NULL);
        RChannelItemRef markerItem = mBuffer->GetItem();

        if (bType == RChannelBuffer_Hole)
        {
            m_transformer->ReportTransformerErrorItem(markerItem, this);
        }

        m_transformer->FlushTransformer(this, this);

        if (!NoError())
        {
            markerItem.
                Attach(RChannelMarkerItem::Create(RChannelItem_ParseError,
                                                  false));
            markerItem->ReplaceMetaData(GetErrorMetaData());
        }

        m_itemList.InsertAsTail(m_itemList.CastIn(markerItem.Detach()));
    }
    else
    {
        LogAssert(bType == RChannelBuffer_Data);
        RChannelBufferData* dBuffer =
            dynamic_cast<RChannelBufferData*>(inData);
        LogAssert(dBuffer != NULL);

        DryadLockedMemoryBuffer* block = dBuffer->GetData();
        LogAssert(block->GetAvailableSize() > 0);
        LogAssert(block->IsGrowable() == false);

        RChannelItemRef dataItem;
        dataItem.Attach(RChannelBufferItem::Create(block));

        m_transformer->TransformItem(dataItem, this, this);

        if (!NoError())
        {
            RChannelItem* errorItem;

            errorItem =
                RChannelMarkerItem::Create(RChannelItem_ParseError, false);
            errorItem->ReplaceMetaData(GetErrorMetaData());

            m_itemList.InsertAsTail(m_itemList.CastIn(errorItem));
        }
    }

    if (m_itemList.IsEmpty())
    {
        return NULL;
    }
    else
    {
        RChannelItem* item = m_itemList.CastOut(m_itemList.RemoveHead());
        return item;
    }
}

void RChannelTransformerParserBase::
    WriteItemSyncConsumingReference(RChannelItemRef& item)
{
    m_itemList.InsertAsTail(m_itemList.CastIn(item.Detach()));
}

DrError RChannelTransformerParserBase::GetWriterStatus()
{
    return DrError_OK;
}


RChannelItemParserNoRefImpl::RChannelItemParserNoRefImpl()
{
    m_needsReset = true;
}

RChannelItemParserNoRefImpl::~RChannelItemParserNoRefImpl()
{
    ResetParserInternal();
}

void RChannelItemParserNoRefImpl::ResetParser()
{
}

RChannelItem* RChannelItemParserNoRefImpl::
    ParsePartialItem(ChannelDataBufferList* bufferList,
                     Size_t startOffset,
                     RChannelBufferMarker*
                     markerBuffer)
{
    return NULL;
}

void RChannelItemParserNoRefImpl::ResetParserInternal()
{
    m_savedItem = NULL;

    DrBListEntry* listEntry = m_bufferList.GetHead();
    while (listEntry != NULL)
    {
        RChannelBufferData* buffer = m_bufferList.CastOut(listEntry);
        listEntry = m_bufferList.GetNext(listEntry);
        m_bufferList.Remove(m_bufferList.CastIn(buffer));
        buffer->DecRef();
    }

    m_bufferListStartOffset = 0;

    m_needsData = true;
}

void RChannelItemParserNoRefImpl::DiscardBufferPrefix(Size_t discardLength)
{
    while (discardLength > 0)
    {
        LogAssert(m_bufferList.IsEmpty() == false);
        RChannelBufferData* buffer =
            m_bufferList.CastOut(m_bufferList.GetHead());
        DryadLockedMemoryBuffer* mBuffer = buffer->GetData();

        LogAssert(mBuffer->GetAvailableSize() > m_bufferListStartOffset);
        Size_t tailLength =
            mBuffer->GetAvailableSize() - m_bufferListStartOffset;

        if (discardLength >= tailLength)
        {
            discardLength -= tailLength;
            m_bufferList.Remove(m_bufferList.CastIn(buffer));
            buffer->DecRef();
            m_bufferListStartOffset = 0;
        }
        else
        {
            m_bufferListStartOffset += discardLength;
            discardLength = 0;
        }
    }
}

RChannelItem* RChannelItemParserNoRefImpl::
    DealWithPartialBuffer(RChannelBufferMarker* mBuffer)
{
    RChannelItem* item;

    item = ParsePartialItem(&m_bufferList, m_bufferListStartOffset,
                            mBuffer);

    if (m_bufferList.IsEmpty())
    {
        LogAssert(m_bufferListStartOffset == 0);
    }
    else
    {
        DrBListEntry* listEntry = m_bufferList.GetHead();
        while (listEntry != NULL)
        {
            RChannelBufferData* buffer = m_bufferList.CastOut(listEntry);
            listEntry = m_bufferList.GetNext(listEntry);
            m_bufferList.Remove(m_bufferList.CastIn(buffer));
            buffer->DecRef();
        }

        m_bufferListStartOffset = 0;
    }

    RChannelItemRef markerItem = mBuffer->GetItem();
    LogAssert(markerItem != NULL);

    if (mBuffer->GetType() == RChannelBuffer_Hole)
    {
        LogAssert(markerItem->GetType() == RChannelItem_BufferHole);
    }
    else
    {
        LogAssert(markerItem->GetType() == RChannelItem_EndOfStream);
    }

    if (item == NULL)
    {
        item = markerItem.Detach();
    }
    else
    {
        m_savedItem = markerItem;
    }

    return item;
}

RChannelItem* RChannelItemParserNoRefImpl::
    RawParseItem(bool restartParser,
                 RChannelBuffer* inData,
                 RChannelBufferPrefetchInfo** outPrefetchCookie)
{
    *outPrefetchCookie = NULL;

    if (m_needsReset)
    {
        LogAssert(restartParser == true);
    }

    if (restartParser)
    {
        // todo: remove comment if not logging
//         DrLogD( "resetting parser");

        ResetParser();

        ResetParserInternal();
    }

    LogAssert(m_needsData == (inData != NULL));

    RChannelItem* item = NULL;

    if (inData != NULL)
    {
        LogAssert(m_savedItem == NULL);

        RChannelBufferType bType = inData->GetType();
        if (bType == RChannelBuffer_Hole ||
            bType == RChannelBuffer_EndOfStream)
        {
            item = DealWithPartialBuffer((RChannelBufferMarker *) inData);
        }
        else
        {
            LogAssert(bType == RChannelBuffer_Data);
            RChannelBufferData* dBuffer = (RChannelBufferData *) inData;
            dBuffer->IncRef();
            DryadLockedMemoryBuffer* block = dBuffer->GetData();
            LogAssert(block->GetAvailableSize() > 0);
            LogAssert(block->IsGrowable() == false);
            m_bufferList.InsertAsTail(m_bufferList.CastIn(dBuffer));
        }
    }

    if (item == NULL)
    {
        if (m_savedItem != NULL)
        {
            item = m_savedItem.Detach();
        }
        else
        {
            Size_t itemLength = 0;

            if (m_bufferList.IsEmpty() == false)
            {
                item = ParseNextItem(&m_bufferList, m_bufferListStartOffset,
                                     &itemLength);
            }

            if (item == NULL)
            {
                LogAssert(itemLength == 0);
            }
            else
            {
                DiscardBufferPrefix(itemLength);
            }
        }
    }

    if (item == NULL)
    {
        m_needsReset = false;
        m_needsData = true;
    }
    else
    {
        m_needsReset = RChannelItem::IsTerminationItem(item->GetType());
        m_needsData = false;
    }

    return item;
}

RChannelItemParser::~RChannelItemParser()
{
}


RChannelStdItemParserNoRefImpl::
    RChannelStdItemParserNoRefImpl(DObjFactoryBase* factory)
{
    m_factory = factory;
}

RChannelStdItemParserNoRefImpl::~RChannelStdItemParserNoRefImpl()
{
}

void RChannelStdItemParserNoRefImpl::ResetParser()
{
    m_pendingErrorItem = NULL;
}

RChannelItem* RChannelStdItemParserNoRefImpl::
    ParseNextItem(ChannelDataBufferList* bufferList,
                  Size_t startOffset,
                  Size_t* pOutLength)
{
    if (m_pendingErrorItem != NULL)
    {
        return m_pendingErrorItem.Detach();
    }

    LogAssert(bufferList->IsEmpty() == false);

    RChannelBufferData* tailBuffer =
        bufferList->CastOut(bufferList->GetTail());
    Size_t tailBufferSize =
        tailBuffer->GetData()->GetAvailableSize();

    DrRef<DrMemoryBuffer> buffer;
    buffer.Attach(new RChannelReaderBuffer(bufferList,
                                           startOffset,
                                           tailBufferSize));

    DrResettableMemoryReader reader(buffer);

    RChannelItem* item = (RChannelItem *) m_factory->AllocateObjectUntyped();
    DrError err = item->DeSerialize(&reader, buffer->GetAvailableSize());

    if (err == DrError_OK)
    {
        *pOutLength = reader.GetBufferOffset();
        return item;
    }

    m_factory->FreeObjectUntyped(item);

    if (err == DrError_EndOfStream)
    {
        return NULL;
    }
    else
    {
        return RChannelMarkerItem::CreateErrorItem(RChannelItem_ParseError,
                                                   err);
    }
}

RChannelItem* RChannelStdItemParserNoRefImpl::
    ParsePartialItem(ChannelDataBufferList* bufferList,
                     Size_t startOffset,
                     RChannelBufferMarker*
                     markerBuffer)
{
    if (m_pendingErrorItem != NULL)
    {
        return m_pendingErrorItem.Detach();
    }

    if (bufferList->IsEmpty())
    {
        return NULL;
    }

    RChannelBufferData* tailBuffer =
        bufferList->CastOut(bufferList->GetTail());
    Size_t tailBufferSize =
        tailBuffer->GetData()->GetAvailableSize();

    DrRef<DrMemoryBuffer> buffer;
    buffer.Attach(new RChannelReaderBuffer(bufferList,
                                           startOffset,
                                           tailBufferSize));

    DrResettableMemoryReader reader(buffer);

    RChannelItem* item = (RChannelItem *) m_factory->AllocateObjectUntyped();
    DrError err = item->DeSerializePartial(&reader,
                                           buffer->GetAvailableSize());

    if (err == DrError_OK)
    {
        return item;
    }

    m_factory->FreeObjectUntyped(item);

    if (err == DrError_EndOfStream)
    {
        return NULL;
    }
    else
    {
        return RChannelMarkerItem::CreateErrorItem(RChannelItem_ParseError,
                                                   err);
    }
}

RChannelStdItemParser::RChannelStdItemParser(DObjFactoryBase* factory) :
    RChannelStdItemParserNoRefImpl(factory)
{
}

RChannelStdItemParser::~RChannelStdItemParser()
{
}


RChannelLengthDelimitedItemParserNoRefImpl::
    RChannelLengthDelimitedItemParserNoRefImpl()
{
    ResetParser();
}

RChannelLengthDelimitedItemParserNoRefImpl::
    ~RChannelLengthDelimitedItemParserNoRefImpl()
{
}

void RChannelLengthDelimitedItemParserNoRefImpl::ResetParser()
{
    m_itemLength = 0;
    m_accumulatedLength = 0;
}

void RChannelLengthDelimitedItemParserNoRefImpl::
    AddMetaData(RChannelItem* item,
                ChannelDataBufferList*
                bufferList,
                Size_t startOffset,
                Size_t endOffset)
{
    LogAssert(bufferList->IsEmpty() == false);

    RChannelBufferData* buffer = bufferList->CastOut(bufferList->GetHead());
    DryadMetaDataRef startMetaData;
    buffer->GetOffsetMetaData(true, startOffset, &startMetaData);

    buffer = bufferList->CastOut(bufferList->GetTail());
    DryadMetaDataRef endMetaData;
    buffer->GetOffsetMetaData(false, endOffset, &endMetaData);

    DryadMetaData* m = item->GetMetaData();
    /* this is only called for items which already have metadata for
       performance reasons */
    LogAssert(m != NULL);

    DryadMTagMetaDataRef tag;
    bool brc;

    tag = m->LookUpMetaDataTag(DryadTag_ItemStart);
    if (tag == NULL)
    {
        tag.Attach(DryadMTagMetaData::Create(DryadTag_ItemStart,
                                             startMetaData, true));
        brc = m->Append(tag, false);
        LogAssert(brc == true);
    }
    else
    {
        tag->GetMetaData()->AppendMetaDataTags(startMetaData, false);
    }

    tag = m->LookUpMetaDataTag(DryadTag_ItemEnd);
    if (tag == NULL)
    {
        tag.Attach(DryadMTagMetaData::Create(DryadTag_ItemEnd,
                                             endMetaData, true));
        brc = m->Append(tag, false);
        LogAssert(brc == true);
    }
    else
    {
        tag->GetMetaData()->AppendMetaDataTags(endMetaData, false);
    }
}

RChannelItem* RChannelLengthDelimitedItemParserNoRefImpl::
    FetchItem(ChannelDataBufferList*
              bufferList,
              Size_t startOffset,
              Size_t tailBufferSize)
{
    Size_t tailGapLength = m_accumulatedLength - m_itemLength;

    LogAssert(tailBufferSize > tailGapLength);

    Size_t endOffset = tailBufferSize - tailGapLength;

    RChannelReaderBuffer* itemBuffer =
        new RChannelReaderBuffer(bufferList, startOffset, endOffset);
    LogAssert(itemBuffer->GetAvailableSize() == m_itemLength);

    RChannelItem* item = ParseItemWithLength(itemBuffer, m_itemLength);
    LogAssert(item != NULL);
    LogAssert(item->GetType() == RChannelItem_Data ||
              item->GetType() == RChannelItem_ItemHole ||
              item->GetType() == RChannelItem_ParseError);

    itemBuffer->DecRef();

    if (item->GetMetaData() != NULL)
    {
        AddMetaData(item, bufferList, startOffset, endOffset);
    }

    ResetParser();

    return item;
}

RChannelItem* RChannelLengthDelimitedItemParserNoRefImpl::
   MaybeFetchItem(ChannelDataBufferList* bufferList,
                  Size_t startOffset,
                  Size_t* pOutLength)
{
    /* make sure there are at least two items */
    LogAssert(bufferList->GetHead() != bufferList->GetTail());

    RChannelBufferData* buffer = bufferList->CastOut(bufferList->GetTail());
    Size_t tailBufferSize = buffer->GetData()->GetAvailableSize();

    LogAssert(m_accumulatedLength < m_itemLength);
    m_accumulatedLength += tailBufferSize;

    if (m_accumulatedLength >= m_itemLength)
    {
        Size_t thisLength = m_itemLength;
        RChannelItem* item = FetchItem(bufferList, startOffset,
                                       tailBufferSize);
        *pOutLength = thisLength;
        return item;
    }
    else
    {
        *pOutLength = 0;
        return NULL;
    }
}

RChannelItem* RChannelLengthDelimitedItemParserNoRefImpl::
    ParseNextItem(ChannelDataBufferList* bufferList,
                  Size_t startOffset,
                  Size_t* pOutLength)
{
    if (m_itemLength > 0)
    {
        return MaybeFetchItem(bufferList, startOffset, pOutLength);
    }
    else
    {
        LogAssert(bufferList->IsEmpty() == false);

        RChannelBufferData* buffer =
            bufferList->CastOut(bufferList->GetTail());
        Size_t tailBufferSize =
            buffer->GetData()->GetAvailableSize();
        LogAssert(bufferList->GetHead() != bufferList->GetTail() ||
                  startOffset < tailBufferSize);

        RChannelReaderBuffer* lengthCheckBuffer =
            new RChannelReaderBuffer(bufferList, startOffset, tailBufferSize);

        m_accumulatedLength = lengthCheckBuffer->GetAvailableSize();

        RChannelItem* errorItem = NULL;
        LengthStatus ls = GetNextItemLength(lengthCheckBuffer,
                                            &m_itemLength,
                                            &errorItem);

        lengthCheckBuffer->DecRef();

        switch (ls)
        {
        default:
            LogAssert(false);
            return NULL;

        case LS_ParseError:
            LogAssert(errorItem != NULL);
            LogAssert(errorItem->GetType() == RChannelItem_ParseError);
            LogAssert(errorItem->GetMetaData() != NULL);
            AddMetaData(errorItem, bufferList, startOffset, tailBufferSize);
            ResetParser();
            *pOutLength = 0;
            return errorItem;

        case LS_NeedsData:
            LogAssert(errorItem == NULL);
            m_itemLength = 0;
            *pOutLength = 0;
            return NULL;

        case LS_Ok:
            LogAssert(errorItem == NULL);
            LogAssert(m_itemLength > 0);

            if (m_accumulatedLength >= m_itemLength)
            {
                Size_t thisLength = m_itemLength;
                RChannelItem* item = FetchItem(bufferList, startOffset,
                                               tailBufferSize);
                *pOutLength = thisLength;
                return item;
            }
            else
            {
                *pOutLength = 0;
                return NULL;
            }
        }
    }
}

RChannelItem* RChannelLengthDelimitedItemParserNoRefImpl::
    ParsePartialItem(ChannelDataBufferList* bufferList,
                     Size_t startOffset,
                     RChannelBufferMarker*
                     markerBuffer)
{
    if (bufferList->IsEmpty())
    {
        return NULL;
    }
    else
    {
        RChannelBufferData* buffer =
            bufferList->CastOut(bufferList->GetTail());
        Size_t tailBufferSize =
            buffer->GetData()->GetAvailableSize();
        RChannelItem* item =
            RChannelMarkerItem::Create(RChannelItem_ItemHole, true);
        AddMetaData(item, bufferList, startOffset, tailBufferSize);
        ResetParser();
        return item;
    }
}

RChannelLengthDelimitedItemParser::~RChannelLengthDelimitedItemParser()
{
}

DryadParserFactoryBase::~DryadParserFactoryBase()
{
}

DryadParserFactory::~DryadParserFactory()
{
}

DryadMarshalerFactoryBase::~DryadMarshalerFactoryBase()
{
}

DryadMarshalerFactory::~DryadMarshalerFactory()
{
}
