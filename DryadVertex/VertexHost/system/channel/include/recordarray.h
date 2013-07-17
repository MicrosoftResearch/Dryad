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

#pragma once

#include "channelparser.h"
#include "channelmarshaler.h"
#include "channelinterface.h"
#include <dryaderrordef.h>

class RecordArrayBase : public RChannelDataItem
{
public:
    RecordArrayBase();
    virtual ~RecordArrayBase();

    /* from the RChannelItem interface */
    UInt64 GetNumberOfSubItems() const;
    void TruncateSubItems(UInt64 numberOfSubItems);
    virtual UInt64 GetItemSize() const;

    virtual size_t GetRecordSize() const = 0;

    UInt32 GetNumberOfRecords() const;
    virtual void SetNumberOfRecords(UInt32 numberOfRecords);

    void* GetRecordArrayUntyped();
    void* GetRecordUntyped(UInt32 index);
    void* NextRecordUntyped();

    UInt32 GetRecordIndex() const;
    void SetRecordIndex(UInt32 index);

    bool AtEnd();
    void Truncate();
    void ResetRecordPointer();
    void PopRecord();

    virtual DrError DeSerialize(DrResettableMemoryReader* reader,
                                Size_t availableSize);
    virtual DrError Serialize(ChannelMemoryBufferWriter* writer);

    void TransferRecord(void* dstRecord, void* srcRecord);
    virtual void TransferRecord(RecordArrayBase* dstArray, void* dstRecord,
                                RecordArrayBase* srcArray, void* srcRecord) = 0;

    void StartSerializing();

protected:
    void FreeStorage();
    void* TransferTruncatedArray(void* srcArray, UInt32 numberOfRecords);
    virtual UInt32 ReadArray(DrMemoryBuffer* buffer,
                             Size_t startOffset, UInt32 nRecords);
    virtual UInt32 AttachArray(DryadLockedMemoryBuffer* buffer,
                               Size_t startOffset);
    virtual DrError ReadFinalArray(DrMemoryBuffer* buffer);

    virtual void* MakeTypedArray(UInt32 numberOfRecords) = 0;
    virtual void FreeTypedArray(void* untypedArray) = 0;

    UInt32                       m_numberOfRecords;
    UInt32                       m_nextRecord;
    UInt32                       m_recordArraySize;
    void*                        m_recordArray;
    DrRef<DrMemoryBuffer>  m_buffer;
    bool                         m_serializedAny;

    friend class PackedRecordArrayParserBase;
    friend class AlternativeRecordMarshalerBase;
};

template< class _R > class PackedRecordArray : public RecordArrayBase
{
public:
    typedef _R RecordType;

    virtual ~PackedRecordArray()
    {
        /* call this from here rather than the base class destructor
           since it needs to call FreeTypedArray() which is
           inaccessible from the base destructor */
        FreeStorage();
    }

    size_t GetRecordSize() const
    {
        return sizeof(RecordType);
    }

    RecordType* GetRecordArray() const
    {
        return (RecordType *) m_recordArray;
    }

    RecordType* NextRecord()
    {
        return (RecordType *) NextRecordUntyped();
    }

    virtual void TransferRecord(RecordArrayBase* dstArray, void* dst,
                                RecordArrayBase* srcArray, void* src)
    {
        RecordType* dstRecord = (RecordType *) dst;
        RecordType* srcRecord = (RecordType *) src;
        *dstRecord = *srcRecord;
    }

private:
    void* MakeTypedArray(UInt32 numberOfRecords)
    {
        return new RecordType[numberOfRecords];
    }

    void FreeTypedArray(void* untypedArray)
    {
        RecordType* array = (RecordType *) untypedArray;
        delete [] array;
    }
};

template< class _R > class RecordArray : public PackedRecordArray<_R>
{
public:
    virtual DrError DeSerialize(DrResettableMemoryReader* reader,
                                Size_t availableSize)
    {
        if (GetNumberOfRecords() == 0)
        {
            SetNumberOfRecords(RChannelItem::s_defaultRecordBatchSize);
        }

        Size_t sizeUsed = 0;
        Size_t remainingSize = availableSize;
        DrError err = DrError_OK;

        ResetRecordPointer();
        RecordType* nextRecord;
        while (remainingSize > 0 && (nextRecord = NextRecord()) != NULL)
        {
            err = nextRecord->DeSerialize(reader, remainingSize, false);
            if (err != DrError_OK)
            {
                PopRecord();
                reader->ResetToBufferOffset(sizeUsed);
                break;
            }

            sizeUsed = reader->GetBufferOffset();
            LogAssert(sizeUsed <= availableSize);
            remainingSize = availableSize - sizeUsed;
        }

        Truncate();
        ResetRecordPointer();

        if (err == DrError_EndOfStream && AtEnd() == false)
        {
            /* AtEnd() == false after Truncate+Reset means there's at
               least one item */
            err = DrError_OK;
        }

        return err;
    }

    virtual DrError DeSerializePartial(DrResettableMemoryReader* reader,
                                       Size_t availableSize)
    {
        SetNumberOfRecords(1);

        ResetRecordPointer();
        RecordType* nextRecord = NextRecord();
        LogAssert(nextRecord != NULL);

        DrError err = nextRecord->DeSerialize(reader, availableSize, true);

        ResetRecordPointer();

        return err;
    }

    virtual DrError Serialize(ChannelMemoryBufferWriter* writer)
    {
        StartSerializing();

        DrError err = DrError_OK;
        Size_t currentPosition = 0;

        RecordType* nextRecord;
        bool filledBuffer = writer->MarkRecordBoundary();
        LogAssert(filledBuffer == false);
        while (err == DrError_OK &&
               filledBuffer == false &&
               (nextRecord = NextRecord()) != NULL)
        {
            currentPosition = writer->GetBufferOffset();
            err = nextRecord->Serialize(writer);
            if (err == DrError_OK)
            {
                filledBuffer = writer->MarkRecordBoundary();
            }
        }

        if (err != DrError_OK)
        {
            LogAssert(writer->GetStatus() == DrError_OK);
            writer->SetBufferOffset(currentPosition);
            return DryadError_ItemMarshalError;
        }

        if (filledBuffer)
        {
            return DrError_IncompleteOperation;
        }

        return DrError_OK;
    }

    virtual void TransferRecord(RecordArrayBase* dstArray, void* dst,
                                RecordArrayBase* srcArray, void* src)
    {
        RecordType* dstRecord = (RecordType *) dst;
        RecordType* srcRecord = (RecordType *) src;
        dstRecord->TransferFrom(*srcRecord);
    }

private:
    virtual UInt32 ReadArray(DrMemoryBuffer* buffer,
                             Size_t startOffset, UInt32 nRecords)
    {
        LogAssert(false);
        return 0;
    }

    virtual UInt32 AttachArray(DryadLockedMemoryBuffer* buffer,
                               Size_t startOffset)
    {
        LogAssert(false);
        return 0;
    }
};

template< class _R > class RecordArrayEx : public RecordArray<_R>
{
public:
    virtual DrError DeSerialize(DrResettableMemoryReader* reader,
                                Size_t availableSize)
    {
        
        if (GetNumberOfRecords() == 0)
        {
            SetNumberOfRecords(RChannelItem::s_defaultRecordBatchSize);
        }

        Size_t sizeUsed = 0;
        Size_t remainingSize = availableSize;
        DrError err = DrError_OK;

        ResetRecordPointer();
        RecordType* nextRecord;
        while (remainingSize > 0 && (nextRecord = NextRecord()) != NULL)
        {
            err = nextRecord->DeSerialize(reader, remainingSize, false);
            if (err != DrError_OK)
            {
                PopRecord();
                reader->ResetToBufferOffset(sizeUsed);
                break;
            }

            sizeUsed = reader->GetBufferOffset();
            LogAssert(sizeUsed <= availableSize);
            remainingSize = availableSize - sizeUsed;
        }

        Truncate();
        ResetRecordPointer();

        if (err == DrError_EndOfStream && AtEnd() == false)
        {
            /* AtEnd() == false after Truncate+Reset means there's at
               least one item */
            err = DrError_OK;
        }

        return err;
    }

    virtual DrError DeSerializePartial(DrResettableMemoryReader* reader,
                                       Size_t availableSize)
    {
        SetNumberOfRecords(1);

        ResetRecordPointer();
        RecordType* nextRecord = NextRecord();
        LogAssert(nextRecord != NULL);

        DrError err = nextRecord->DeSerialize(reader, availableSize, true);

        ResetRecordPointer();

        return err;
    }
};

template< class _A > class RecordArrayFactory : public DObjFactoryBase
{
public:
    typedef _A ArrayType;

    RecordArrayFactory(UInt32 arraySize)
    {
        m_arraySize = arraySize;
    }

private:
    void* AllocateObjectUntyped()
    {
        ArrayType* newArray = new ArrayType();
        newArray->SetNumberOfRecords(m_arraySize);
        return newArray;
    }

    void FreeObjectUntyped(void* object)
    {
        DrRef<ArrayType> typedObject;
        typedObject.Attach((ArrayType *) object);
        /* let typedObject go out of scope, freeing the reference */
    }

    UInt32  m_arraySize;

    DRREFCOUNTIMPL
};

class RecordArrayReaderBase
{
public:
    RecordArrayReaderBase();
    RecordArrayReaderBase(SyncItemReaderBase* reader);
    virtual ~RecordArrayReaderBase();

    void Initialize(SyncItemReaderBase* reader);

    bool Advance();
    UInt32 AdvanceBlock(UInt32 validEntriesRequested);
    UInt32 GetValidCount() const;
    void PushBack();

    DrError GetStatus();
    RChannelItem* GetTerminationItem();

protected:
    void PushBack(bool pushValid);
    void DiscardCachedItems();
    virtual bool AdvanceInternal(UInt32 slotNumber);

    void**                 m_currentRecord;
    SyncItemReaderBase*    m_reader;
    RChannelItemRef        m_item;
    RecordArrayBase*       m_arrayItem;
    UInt32                 m_cacheSize;
    RChannelItemRef*       m_itemCache;
    UInt32                 m_valid;
    UInt32                 m_cachedItemCount;
};

template< class _R > class RecordArrayReader :
    public RecordArrayReaderBase
{
public:
    typedef _R RecordType;

    RecordArrayReader() {}
    RecordArrayReader(SyncItemReaderBase* reader) :
        RecordArrayReaderBase(reader)
    {
    }

    RecordType* operator->() const
    {
        LogAssert(GetValidCount() > 0);
        return (RecordType *) m_currentRecord[0];
    }

    RecordType& operator*() const
    {
        LogAssert(GetValidCount() > 0);
        return *((RecordType *) m_currentRecord[0]);
    }

    RecordType& operator[](UInt32 index) const
    {
        LogAssert(index < GetValidCount());
        return *((RecordType *) m_currentRecord[index]);
    }
};

class RecordArrayWriterBase
{
public:
    RecordArrayWriterBase();
    RecordArrayWriterBase(SyncItemWriterBase* writer,
                          DObjFactoryBase* factory);
    virtual ~RecordArrayWriterBase();

    void Initialize(SyncItemWriterBase* writer, DObjFactoryBase* factory);
    void SetWriter(SyncItemWriterBase* writer);

    void MakeValid();
    void MakeValidBlock(UInt32 validEntriesRequested);
    UInt32 GetValidCount() const;
    void PushBack();

    void* ReadValidUntyped(UInt32 index);

    void Terminate();
    void Flush();

    DrError GetWriterStatus();

protected:
    void**                   m_currentRecord;

private:
    void Destroy();
    void SendCachedItems();
    void AdvanceInternal(UInt32 slotNumber);

    DrRef<DObjFactoryBase>   m_factory;
    SyncItemWriterBase*      m_writer;
    DrRef<RecordArrayBase>   m_item;
    UInt32                   m_cacheSize;
    DrRef<RecordArrayBase>*  m_itemCache;
    UInt32                   m_valid;
    UInt32                   m_pushBackIndex;
    UInt32                   m_cachedItemCount;
};

template< class _R > class RecordArrayWriter : public RecordArrayWriterBase
{
public:
    typedef _R RecordType;

    RecordArrayWriter() {}
    RecordArrayWriter(SyncItemWriterBase* writer, DObjFactoryBase* factory) :
        RecordArrayWriterBase(writer, factory)
    {
    }

    RecordType* operator->() const
    {
        LogAssert(GetValidCount() > 0);
        return (RecordType *) m_currentRecord[0];
    }

    RecordType& operator*() const
    {
        LogAssert(GetValidCount() > 0);
        return *((RecordType *) m_currentRecord[0]);
    }

    RecordType& operator[](UInt32 index) const
    {
        LogAssert(index < GetValidCount());
        return *((RecordType *) m_currentRecord[index]);
    }
};
