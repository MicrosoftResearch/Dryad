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

#include "recorditem.h"

#pragma unmanaged

RecordArrayBase::RecordArrayBase()
{
    m_recordArray = NULL;
    m_recordArraySize = 0;
    m_numberOfRecords = 0;
    m_nextRecord = 0;
    m_serializedAny = 0;
}

RecordArrayBase::~RecordArrayBase()
{
    LogAssert(m_buffer == NULL);
    LogAssert(m_recordArray == NULL);
}

void RecordArrayBase::FreeStorage()
{
    m_buffer = NULL;
    if (m_recordArraySize > 0)
    {
        FreeTypedArray(m_recordArray);
    }
    m_recordArray = NULL;
    m_recordArraySize = 0;
    m_numberOfRecords = 0;
    m_nextRecord = 0;
}

void RecordArrayBase::PopRecord()
{
    LogAssert(m_nextRecord > 0);
    --m_nextRecord;
}

UInt32 RecordArrayBase::GetNumberOfRecords() const
{
    return m_numberOfRecords;
}

UInt64 RecordArrayBase::GetNumberOfSubItems() const
{
    return GetNumberOfRecords();
}

void RecordArrayBase::TruncateSubItems(UInt64 numberOfSubItems)
{
    LogAssert(numberOfSubItems < (UInt64) m_numberOfRecords);
    SetRecordIndex((UInt32) numberOfSubItems);
    Truncate();
    ResetRecordPointer();
}

UInt64 RecordArrayBase::GetItemSize() const
{
    return (UInt64) GetRecordSize() * (UInt64) GetNumberOfRecords();
}

void* RecordArrayBase::GetRecordArrayUntyped()
{
    return m_recordArray;
}

void RecordArrayBase::SetNumberOfRecords(UInt32 numberOfRecords)
{
    if (m_recordArraySize < numberOfRecords)
    {
        if (m_recordArraySize > 0)
        {
            LogAssert(m_buffer == NULL);
            FreeTypedArray(m_recordArray);
        }

        m_recordArraySize = numberOfRecords;
        if (m_recordArraySize > 0)
        {
            m_recordArray = MakeTypedArray(m_recordArraySize);
        }
        else
        {
            m_recordArray = NULL;
        }
    }
    else if (m_recordArraySize == 0)
    {
        LogAssert(m_buffer != NULL);
        LogAssert(m_recordArray != NULL);
        m_recordArray = NULL;
    }

    m_buffer = NULL;

    m_numberOfRecords = numberOfRecords;
    ResetRecordPointer();
}

void RecordArrayBase::ResetRecordPointer()
{
    m_nextRecord = 0;
}

void* RecordArrayBase::GetRecordUntyped(UInt32 index)
{
    LogAssert(index < m_numberOfRecords);
    char* cPtr = (char *) m_recordArray;
    return &(cPtr[GetRecordSize() * index]);
}

//
// Return pointer to next record in array
//
void* RecordArrayBase::NextRecordUntyped()
{
    if (m_nextRecord == m_numberOfRecords)
    {
        //
        // If no more records, return null
        //
        return NULL;
    }
    else
    {
        //
        // If valid record index, set return value to correct
        // offset from array start and increment "next record" index
        //
        LogAssert(m_nextRecord < m_numberOfRecords);
        char* cPtr = (char *) m_recordArray;
        void* r = &(cPtr[GetRecordSize() * m_nextRecord]);
        ++m_nextRecord;
        return r;
    }
}

//
// Return the next record index
//
UInt32 RecordArrayBase::GetRecordIndex() const
{
    return m_nextRecord;
}

void RecordArrayBase::SetRecordIndex(UInt32 index)
{
    LogAssert(index <= m_numberOfRecords);
    m_nextRecord = index;
}

//
// Return whether there are any additional records available
//
bool RecordArrayBase::AtEnd()
{
    return (m_nextRecord == m_numberOfRecords);
}

void RecordArrayBase::Truncate()
{
    if (m_nextRecord*2 < m_recordArraySize)
    {
        void* newArray = TransferTruncatedArray(m_recordArray, m_nextRecord);
        FreeTypedArray(m_recordArray);
        m_recordArray = newArray;
        m_recordArraySize = m_nextRecord;
    }

    m_numberOfRecords = m_nextRecord;
}

void RecordArrayBase::TransferRecord(void* dstRecord, void* srcRecord)
{
    TransferRecord(NULL, dstRecord, NULL, srcRecord);
}

void* RecordArrayBase::TransferTruncatedArray(void* srcArrayUntyped,
                                              UInt32 numberOfRecords)
{
    if (numberOfRecords == 0)
    {
        return NULL;
    }

    void* dstArrayUntyped = MakeTypedArray(numberOfRecords);
    LogAssert(dstArrayUntyped != NULL);

    char* srcPtr = (char *) srcArrayUntyped;
    char* dstPtr = (char *) dstArrayUntyped;
    UInt32 i;
    for (i=0; i<numberOfRecords;
         ++i, srcPtr += GetRecordSize(), dstPtr += GetRecordSize())
    {
        TransferRecord(this, dstPtr, this, srcPtr);
    }

    return dstArrayUntyped;
}

UInt32 RecordArrayBase::ReadArray(DrMemoryBuffer* buffer,
                                  Size_t startOffset, UInt32 nRecords)
{
    Size_t neededSize = nRecords * GetRecordSize();

    Size_t availableSize = buffer->GetAvailableSize();
    LogAssert(availableSize >= startOffset);

    Size_t remainingSize = buffer->GetAvailableSize() - startOffset;
    if (remainingSize < neededSize)
    {
        SetNumberOfRecords(0);
        return 0;
    }
    else
    {
        SetNumberOfRecords(nRecords);
        buffer->Read(startOffset, m_recordArray, neededSize);
        return (UInt32) neededSize;
    }
}

DrError RecordArrayBase::ReadFinalArray(DrMemoryBuffer* buffer)
{
    LogAssert(buffer->GetAvailableSize() < GetRecordSize());
    return DrError_EndOfStream;
}

UInt32 RecordArrayBase::AttachArray(DryadLockedMemoryBuffer* buffer,
                                    Size_t startOffset)
{
    Size_t remainingSize;
    void* recordArray = (void *) buffer->GetReadAddress(startOffset,
                                                        &remainingSize);
    LogAssert(remainingSize == buffer->GetAvailableSize() - startOffset);

    UInt32 numberOfRecords = (UInt32) (remainingSize / GetRecordSize());
    if (numberOfRecords == 0)
    {
        SetNumberOfRecords(0);
        return 0;
    }
    else
    {
        FreeStorage();
        m_buffer = buffer;
        m_recordArray = recordArray;
        m_numberOfRecords = numberOfRecords;
        return (UInt32) (m_numberOfRecords * GetRecordSize());
    }
}

DrError RecordArrayBase::DeSerialize(DrResettableMemoryReader* reader,
                                     Size_t availableSize)
{
    UInt32 numberOfRecords = (UInt32) (availableSize / GetRecordSize());
    if (numberOfRecords < 1)
    {
        return DrError_EndOfStream;
    }

    if (GetNumberOfRecords() == 0)
    {
        SetNumberOfRecords(numberOfRecords);
    }

    if (numberOfRecords >= GetNumberOfRecords())
    {
        numberOfRecords = GetNumberOfRecords();
    }
    else
    {
        SetNumberOfRecords(numberOfRecords);
    }

    Size_t dataLength = (Size_t) (numberOfRecords * GetRecordSize());
    DrError err = reader->ReadBytes((BYTE *) GetRecordArrayUntyped(),
                                    dataLength);
    LogAssert(err == DrError_OK);

    return DrError_OK;
}

void RecordArrayBase::StartSerializing()
{
    if (m_serializedAny == false)
    {
        ResetRecordPointer();
        m_serializedAny = true;
    }
}

DrError RecordArrayBase::Serialize(ChannelMemoryBufferWriter* writer)
{
    StartSerializing();

    void* nextRecord;
    bool filledBuffer = writer->MarkRecordBoundary();
    LogAssert(filledBuffer == false);
    while (filledBuffer == false &&
           (nextRecord = NextRecordUntyped()) != NULL)
    {
        writer->WriteBytes((BYTE *) nextRecord, GetRecordSize());
        filledBuffer = writer->MarkRecordBoundary();
    }

    if (filledBuffer)
    {
        return DrError_IncompleteOperation;
    }
    else
    {
        return DrError_OK;
    }
}

PackedRecordArrayParserBase::
    PackedRecordArrayParserBase(DObjFactoryBase* factory)
{
    m_factory = factory;
}

PackedRecordArrayParserBase::~PackedRecordArrayParserBase()
{
}

RChannelItem* PackedRecordArrayParserBase::
    ParseNextItem(ChannelDataBufferList* bufferList,
                  Size_t startOffset,
                  Size_t* pOutLength)
{
    LogAssert(bufferList->IsEmpty() == false);

    RecordArrayBase *item =
        (RecordArrayBase *) m_factory->AllocateObjectUntyped();

    RChannelBufferData* headBuffer =
        bufferList->CastOut(bufferList->GetHead());

    if (bufferList->GetHead() != bufferList->GetTail())
    {
        RChannelBufferData* tailBuffer =
            bufferList->CastOut(bufferList->GetTail());
        Size_t tailBufferSize =
            tailBuffer->GetData()->GetAvailableSize();

        DrRef<RChannelReaderBuffer> combinedBuffer;
        combinedBuffer.Attach(new RChannelReaderBuffer(bufferList,
                                                       startOffset,
                                                       tailBufferSize));

        *pOutLength = item->ReadArray(combinedBuffer, 0, 1);
    }
    else
    {
        *pOutLength = item->AttachArray(headBuffer->GetData(), startOffset);
    }

    if (*pOutLength == 0)
    {
        m_factory->FreeObjectUntyped(item);
        item = NULL;
    }

    return item;
}

RChannelItem* PackedRecordArrayParserBase::
    ParsePartialItem(ChannelDataBufferList* bufferList,
                     Size_t startOffset,
                     RChannelBufferMarker*
                     markerBuffer)
{
    if (bufferList->IsEmpty())
    {
        return NULL;
    }

    RChannelBufferData* tailBuffer =
        bufferList->CastOut(bufferList->GetTail());
    Size_t tailBufferSize =
        tailBuffer->GetData()->GetAvailableSize();

    DrRef<RChannelReaderBuffer> combinedBuffer;
    combinedBuffer.Attach(new RChannelReaderBuffer(bufferList,
                                                   startOffset,
                                                   tailBufferSize));

    RecordArrayBase *item =
        (RecordArrayBase *) m_factory->AllocateObjectUntyped();

    DrError err = item->ReadFinalArray(combinedBuffer);

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

PackedRecordArrayParser::PackedRecordArrayParser(DObjFactoryBase* factory) :
    PackedRecordArrayParserBase(factory)
{
}

PackedRecordArrayParser::~PackedRecordArrayParser()
{
}


RecordArrayReaderBase::RecordArrayReaderBase()
{
}

RecordArrayReaderBase::RecordArrayReaderBase(SyncItemReaderBase* reader)
{
    Initialize(reader);
}

RecordArrayReaderBase::~RecordArrayReaderBase()
{
    delete [] m_currentRecord;
    delete [] m_itemCache;
}

void RecordArrayReaderBase::Initialize(SyncItemReaderBase* reader)
{
    m_reader = reader;
    m_arrayItem = NULL;
    m_cacheSize = 32;
    m_currentRecord = new void* [m_cacheSize];
    m_itemCache = new RChannelItemRef [m_cacheSize];
    m_valid = 0;
    m_cachedItemCount = 0;
}

void RecordArrayReaderBase::DiscardCachedItems()
{
    UInt32 i;
    for (i=0; i<m_cachedItemCount; ++i)
    {
        m_itemCache[i] = NULL;
    }
    m_cachedItemCount = 0;
}

bool RecordArrayReaderBase::AdvanceInternal(UInt32 slotNumber)
{
    while (m_item == NULL)
    {
        LogAssert(m_arrayItem == NULL);
        DrError status = m_reader->ReadItemSync(&m_item);
        LogAssert(status == DrError_OK);
        LogAssert(m_item != NULL);
        RChannelItemType itemType = m_item->GetType();
        if (itemType == RChannelItem_Data)
        {
            m_arrayItem = (RecordArrayBase *) (m_item.Ptr());
        }
        else if (RChannelItem::IsTerminationItem(itemType) == false)
        {
            /* this is a marker; skip it and fetch the next one */
            m_item = NULL;
        }
    }

    if (m_arrayItem != NULL)
    {
        m_currentRecord[slotNumber] = m_arrayItem->NextRecordUntyped();
        LogAssert(m_currentRecord[slotNumber] != NULL);
        if (m_arrayItem->AtEnd())
        {
            m_itemCache[m_cachedItemCount].TransferFrom(m_item);
            LogAssert(m_item == NULL);
            m_arrayItem = NULL;
            ++m_cachedItemCount;
        }
        return true;
    }
    else
    {
        return false;
    }
}

void RecordArrayReaderBase::PushBack()
{
    PushBack(true);
}

void RecordArrayReaderBase::PushBack(bool pushValid)
{
    if (pushValid)
    {
        LogAssert(m_valid > 0);
    }

    if (m_arrayItem == NULL)
    {
        LogAssert(m_item == NULL);
        LogAssert(m_cachedItemCount > 0);
        --m_cachedItemCount;
        m_item.TransferFrom(m_itemCache[m_cachedItemCount]);
        m_arrayItem = (RecordArrayBase *) (m_item.Ptr());
    }

    if (m_arrayItem->GetRecordIndex() == 0)
    {
        DrLogA("Can't push back more than one record");
    }

    m_arrayItem->PopRecord();
    if (pushValid)
    {
        --m_valid;
    }
}

bool RecordArrayReaderBase::Advance()
{
    DiscardCachedItems();
    if (AdvanceInternal(0))
    {
        m_valid = 1;
        return true;
    }
    else
    {
        m_valid = 0;
        return false;
    }
}

UInt32 RecordArrayReaderBase::AdvanceBlock(UInt32 validEntriesRequested)
{
    DiscardCachedItems();
    if (validEntriesRequested > m_cacheSize)
    {
        delete [] m_currentRecord;
        delete [] m_itemCache;
        m_cacheSize = validEntriesRequested;
        m_currentRecord = new void* [m_cacheSize];
        m_itemCache = new RChannelItemRef [m_cacheSize];
    }

    UInt32 i;
    for (i=0; i<validEntriesRequested; ++i)
    {
        if (AdvanceInternal(i) == false)
        {
            break;
        }
    }

    m_valid = i;
    return i;
}

DrError RecordArrayReaderBase::GetStatus()
{
    RChannelItem* item = GetTerminationItem();
    if (item == NULL)
    {
        return DrError_OK;
    }
    else
    {
        return item->GetErrorFromItem();
    }
}

RChannelItem* RecordArrayReaderBase::GetTerminationItem()
{
    if (m_item == NULL ||
        RChannelItem::IsTerminationItem(m_item->GetType()) == false)
    {
        return NULL;
    }
    else
    {
        return m_item;
    }
}

UInt32 RecordArrayReaderBase::GetValidCount() const
{
    return m_valid;
}


RecordArrayWriterBase::RecordArrayWriterBase()
{
    m_currentRecord = NULL;
    m_itemCache = NULL;
    m_cachedItemCount = 0;
}

RecordArrayWriterBase::RecordArrayWriterBase(SyncItemWriterBase* writer,
                                             DObjFactoryBase* factory)
{
    m_currentRecord = NULL;
    m_itemCache = NULL;
    m_cachedItemCount = 0;
    Initialize(writer, factory);
}

RecordArrayWriterBase::~RecordArrayWriterBase()
{
    Destroy();
}

void RecordArrayWriterBase::Destroy()
{
    Flush();
    delete [] m_currentRecord;
    delete [] m_itemCache;
}

void RecordArrayWriterBase::Initialize(SyncItemWriterBase* writer,
                                       DObjFactoryBase* factory)
{
    Destroy();
    m_factory = factory;
    m_writer = writer;
    m_cacheSize = 32;
    m_currentRecord = new void* [m_cacheSize];
    m_itemCache = new DrRef<RecordArrayBase> [m_cacheSize];
    m_valid = 0;
    m_pushBackIndex = 0;
    m_cachedItemCount = 0;
}

void RecordArrayWriterBase::SetWriter(SyncItemWriterBase* writer)
{
    m_writer = writer;
}

DrError RecordArrayWriterBase::GetWriterStatus()
{
    return m_writer->GetWriterStatus();
}

//
// Write out any cached items
//
void RecordArrayWriterBase::SendCachedItems()
{
    UInt32 i;
    for (i=0; i<m_cachedItemCount; ++i)
    {
        //
        // ensure the record pointer is rewound in case we are sending
        // this down a FIFO 
        //
        m_itemCache[i]->ResetRecordPointer();
        m_writer->WriteItemSync(m_itemCache[i]);
        m_itemCache[i] = NULL;
    }

    m_cachedItemCount = 0;
}

//
// Move to next available object if one is available. 
//
void RecordArrayWriterBase::AdvanceInternal(UInt32 slotNumber)
{
    if (m_item == NULL)
    {
        //
        // If no current record array, generate one
        //
        RecordArrayBase *item = (RecordArrayBase *)
            m_factory->AllocateObjectUntyped();
        m_item.Attach(item);
    }

    //
    // Set current record to the next available record
    //
    m_currentRecord[slotNumber] = m_item->NextRecordUntyped();
    LogAssert(m_currentRecord[slotNumber] != NULL);

    //
    // If there are no more records in this record array, transfer the 
    // record array into the item cache to remember that all items are cached.
    //
    if (m_item->AtEnd())
    {
        m_itemCache[m_cachedItemCount].TransferFrom(m_item);
        LogAssert(m_item == NULL);
        ++m_cachedItemCount;
    }
}

//
// 
//
void RecordArrayWriterBase::MakeValid()
{
    //
    // Clear out any cached items to reset
    //
    SendCachedItems();

    //
    // Get the index of the next available record
    //
    if (m_item == NULL)
    {
        m_pushBackIndex = 0;
    }
    else
    {
        m_pushBackIndex = m_item->GetRecordIndex();
    }

    //
    // Move to next available object if one is available and mark as valid
    //
    AdvanceInternal(0);
    m_valid = 1;
}

void RecordArrayWriterBase::MakeValidBlock(UInt32 validEntriesRequested)
{
    SendCachedItems();

    if (m_item == NULL)
    {
        m_pushBackIndex = 0;
    }
    else
    {
        m_pushBackIndex = m_item->GetRecordIndex();
    }

    if (validEntriesRequested > m_cacheSize)
    {
        delete [] m_currentRecord;
        delete [] m_itemCache;
        m_cacheSize = validEntriesRequested;
        m_currentRecord = new void* [m_cacheSize];
        m_itemCache = new DrRef<RecordArrayBase> [m_cacheSize];
    }

    UInt32 i;
    for (i=0; i<validEntriesRequested; ++i)
    {
        AdvanceInternal(i);
    }

    m_valid = validEntriesRequested;
}

UInt32 RecordArrayWriterBase::GetValidCount() const
{
    return m_valid;
}

void RecordArrayWriterBase::PushBack()
{
    LogAssert(m_valid > 0);
    if (m_cachedItemCount > 0)
    {
        /* throw away back to the item we were using when we started */
        m_item.TransferFrom(m_itemCache[0]);
        UInt32 i;
        for (i=1; i<m_cachedItemCount; ++i)
        {
            m_itemCache[i] = NULL;
        }
        m_cachedItemCount = 0;
    }
    LogAssert(m_item != NULL);
    m_item->SetRecordIndex(m_pushBackIndex);
    m_pushBackIndex = 0;
    m_valid = 0;
}

void* RecordArrayWriterBase::ReadValidUntyped(UInt32 index)
{
    LogAssert(index < m_valid);
    return m_currentRecord[index];
}

void RecordArrayWriterBase::Flush()
{
    if (m_item != NULL)
    {
        m_item->Truncate();
        m_itemCache[m_cachedItemCount].TransferFrom(m_item);
        LogAssert(m_item == NULL);
        ++m_cachedItemCount;
    }
    SendCachedItems();
    m_valid = 0;
}

void RecordArrayWriterBase::Terminate()
{
    Flush();
    RChannelItemRef item;
    item.Attach(RChannelMarkerItem::Create(RChannelItem_EndOfStream, false));
    m_writer->WriteItemSync(item);
}

AlternativeRecordParserBase::
    AlternativeRecordParserBase(DObjFactoryBase* factory)
{
    m_factory = factory;
    m_function = NULL;
}

AlternativeRecordParserBase::
    AlternativeRecordParserBase(DObjFactoryBase* factory,
                                RecordDeSerializerFunction* function)
{
    m_factory = factory;
    m_function = function;
}

AlternativeRecordParserBase::~AlternativeRecordParserBase()
{
}

void AlternativeRecordParserBase::ResetParser()
{
    m_pendingErrorItem = NULL;
}

DrError AlternativeRecordParserBase::
    DeSerializeArray(RecordArrayBase* array,
                     DrResettableMemoryReader* reader,
                     Size_t availableSize)
{
    if (array->GetNumberOfRecords() == 0)
    {
        array->SetNumberOfRecords(RChannelItem::s_defaultRecordBatchSize);
    }

    Size_t sizeUsed = 0;
    Size_t remainingSize = availableSize;
    DrError err = DrError_OK;

    array->ResetRecordPointer();
    void* nextRecord;
    while (remainingSize > 0 &&
           (nextRecord = array->NextRecordUntyped()) != NULL)
    {
        err = DeSerializeUntyped(nextRecord, reader, remainingSize, false);
        if (err != DrError_OK)
        {
            array->PopRecord();
            reader->ResetToBufferOffset(sizeUsed);
            break;
        }

        sizeUsed = reader->GetBufferOffset();
        LogAssert(sizeUsed <= availableSize);
        remainingSize = availableSize - sizeUsed;
    }

    array->Truncate();
    array->ResetRecordPointer();

    if (err == DrError_EndOfStream && array->AtEnd() == false)
    {
        /* AtEnd() == false after Truncate+Reset means there's at
           least one item */
        err = DrError_OK;
    }

    return err;
}

RChannelItem* AlternativeRecordParserBase::
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

    RecordArrayBase* recordArray =
        (RecordArrayBase *) m_factory->AllocateObjectUntyped();
    DrError err = DeSerializeArray(recordArray, &reader,
                                   buffer->GetAvailableSize());

    if (err == DrError_OK)
    {
        *pOutLength = reader.GetBufferOffset();
        return recordArray;
    }

    m_factory->FreeObjectUntyped(recordArray);

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

RChannelItem* AlternativeRecordParserBase::
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

    RecordArrayBase* recordArray =
        (RecordArrayBase *) m_factory->AllocateObjectUntyped();
    recordArray->SetNumberOfRecords(1);
    void* nextRecord = recordArray->NextRecordUntyped();
    DrError err = DeSerializeUntyped(nextRecord, &reader,
                                     buffer->GetAvailableSize(), true);

    if (err == DrError_OK)
    {
        recordArray->ResetRecordPointer();
        return recordArray;
    }

    m_factory->FreeObjectUntyped(recordArray);

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

DrError AlternativeRecordParserBase::
    DeSerializeUntyped(void* record,
                       DrMemoryBufferReader* reader,
                       Size_t availableSize, bool lastRecordInStream)
{
    return (*m_function)(record, reader, availableSize, lastRecordInStream);
}

UntypedAlternativeRecordParser::
    UntypedAlternativeRecordParser(DObjFactoryBase* factory,
                                   RecordDeSerializerFunction* function) :
        AlternativeRecordParserBase(factory, function)
{
}

StdAlternativeRecordParserFactory::
    StdAlternativeRecordParserFactory(DObjFactoryBase* factory,
                                      RecordDeSerializerFunction* function)
{
    m_factory = factory;
    m_function = function;
}

void StdAlternativeRecordParserFactory::
    MakeParser(RChannelItemParserRef* pParser,
               DVErrorReporter* errorReporter)
{
    pParser->Attach(new UntypedAlternativeRecordParser(m_factory,
                                                       m_function));
}


AlternativeRecordMarshalerBase::AlternativeRecordMarshalerBase()
{
    m_function = NULL;
}

AlternativeRecordMarshalerBase::
    AlternativeRecordMarshalerBase(RecordSerializerFunction* function)
{
    m_function = function;
}

AlternativeRecordMarshalerBase::~AlternativeRecordMarshalerBase()
{
}

void AlternativeRecordMarshalerBase::
    SetFunction(RecordSerializerFunction* function)
{
    m_function = function;
}

DrError AlternativeRecordMarshalerBase::
    MarshalItem(ChannelMemoryBufferWriter* writer,
                RChannelItem* item,
                bool flush,
                RChannelItemRef* pFailureItem)
{
    DrError err = DrError_OK;

    if (item->GetType() != RChannelItem_Data)
    {
        return err;
    }

    RecordArrayBase* arrayItem = (RecordArrayBase *) item;

    arrayItem->StartSerializing();

    Size_t currentPosition = 0;

    void* nextRecord;
    bool filledBuffer = writer->MarkRecordBoundary();
    LogAssert(filledBuffer == false);
    while (err == DrError_OK &&
           filledBuffer == false &&
           (nextRecord = arrayItem->NextRecordUntyped()) != NULL)
    {
        currentPosition = writer->GetBufferOffset();
        err = SerializeUntyped(nextRecord, writer);
        if (err == DrError_OK)
        {
            filledBuffer = writer->MarkRecordBoundary();
        }
    }

    if (err != DrError_OK)
    {
        LogAssert(writer->GetStatus() == DrError_OK);
        writer->SetBufferOffset(currentPosition);

        pFailureItem->Attach(RChannelMarkerItem::
                             CreateErrorItem(RChannelItem_MarshalError,
                                             err));
        return DryadError_ChannelAbort;
    }

    if (filledBuffer)
    {
        return DrError_IncompleteOperation;
    }

    return DrError_OK;
}

DrError AlternativeRecordMarshalerBase::
    SerializeUntyped(void* record,
                     DrMemoryBufferWriter* writer)
{
    return (*m_function)(record, writer);
}

UntypedAlternativeRecordMarshaler::
    UntypedAlternativeRecordMarshaler(RecordSerializerFunction* function) :
        AlternativeRecordMarshalerBase(function)
{
}

StdAlternativeRecordMarshalerFactory::
    StdAlternativeRecordMarshalerFactory(RecordSerializerFunction* function)
{
    m_function = function;
}

void StdAlternativeRecordMarshalerFactory::
    MakeMarshaler(RChannelItemMarshalerRef* pMarshaler,
                  DVErrorReporter* errorReporter)
{
    pMarshaler->Attach(new UntypedAlternativeRecordMarshaler(m_function));
}
