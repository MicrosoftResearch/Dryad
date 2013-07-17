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

class PackedRecordArrayParserBase : public RChannelItemParserNoRefImpl
{
public:
    PackedRecordArrayParserBase(DObjFactoryBase* factory);
    virtual ~PackedRecordArrayParserBase();

    RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength);
    RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                   Size_t startOffset,
                                   RChannelBufferMarker* markerBuffer);

private:
    DrRef<DObjFactoryBase>    m_factory;
};

class PackedRecordArrayParser : public PackedRecordArrayParserBase
{
public:
    PackedRecordArrayParser(DObjFactoryBase* factory);
    virtual ~PackedRecordArrayParser();
    DRREFCOUNTIMPL
};

typedef DrError RecordDeSerializerFunction(void* record,
                                           DrMemoryBufferReader* reader,
                                           Size_t availableSize,
                                           bool lastRecordInStream);

class AlternativeRecordParserBase : public RChannelItemParserNoRefImpl
{
public:
    AlternativeRecordParserBase(DObjFactoryBase* factory);
    AlternativeRecordParserBase(DObjFactoryBase* factory,
                                RecordDeSerializerFunction* function);
    virtual ~AlternativeRecordParserBase();

    void ResetParser();

    RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength);
    RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                   Size_t startOffset,
                                   RChannelBufferMarker*
                                   markerBuffer);

private:
    DrError DeSerializeArray(RecordArrayBase* array,
                             DrResettableMemoryReader* reader,
                             Size_t availableSize);
    virtual DrError DeSerializeUntyped(void* record,
                                       DrMemoryBufferReader* reader,
                                       Size_t availableSize,
                                       bool lastRecordInStream);

    DObjFactoryBase*              m_factory;
    RChannelItemRef               m_pendingErrorItem;
    RecordDeSerializerFunction*   m_function;
};

typedef DrRef<AlternativeRecordParserBase> AlternativeRecordParserRef;

class UntypedAlternativeRecordParser : public AlternativeRecordParserBase
{
public:
    UntypedAlternativeRecordParser(DObjFactoryBase* factory,
                                   RecordDeSerializerFunction* function);

    DRREFCOUNTIMPL
};

class StdAlternativeRecordParserFactory : public DryadParserFactoryBase
{
public:
    StdAlternativeRecordParserFactory(DObjFactoryBase* factory,
                                      RecordDeSerializerFunction* function);

    void MakeParser(RChannelItemParserRef* pParser,
                    DVErrorReporter* errorReporter);

private:
    DObjFactoryBase*              m_factory;
    RecordDeSerializerFunction*   m_function;

    DRREFCOUNTIMPL
};

template< class _R > class AlternativeRecordParser :
    public AlternativeRecordParserBase
{
public:
    typedef _R RecordType;

    AlternativeRecordParser(DObjFactoryBase* factory);

    virtual DrError DeSerialize(RecordType* record,
                                DrMemoryBufferReader* reader,
                                Size_t availableSize,
                                bool lastRecordInStream) = 0;

private:
    DrError DeSerializeUntyped(void* record,
                               DrMemoryBufferReader* reader,
                               Size_t availableSize,
                               bool lastRecordInStream)
    {
        return DeSerialize((RecordType *) record, reader,
                           availableSize, lastRecordInStream);
    }
};


typedef DrError RecordSerializerFunction(void* record,
                                         DrMemoryBufferWriter* writer);

class AlternativeRecordMarshalerBase : public RChannelItemMarshalerBase
{
public:
    AlternativeRecordMarshalerBase();
    AlternativeRecordMarshalerBase(RecordSerializerFunction* function);
    virtual ~AlternativeRecordMarshalerBase();

    void SetFunction(RecordSerializerFunction* function);

    DrError MarshalItem(ChannelMemoryBufferWriter* writer,
                        RChannelItem* item,
                        bool flush,
                        RChannelItemRef* pFailureItem);

private:
    virtual DrError SerializeUntyped(void* record,
                                     DrMemoryBufferWriter* writer);

    RecordSerializerFunction*   m_function;
};

typedef DrRef<AlternativeRecordMarshalerBase> AlternativeRecordMarshalerRef;

class UntypedAlternativeRecordMarshaler : public AlternativeRecordMarshalerBase
{
public:
    UntypedAlternativeRecordMarshaler(RecordSerializerFunction* function);

    DRREFCOUNTIMPL
};

class StdAlternativeRecordMarshalerFactory : public DryadMarshalerFactoryBase
{
public:
    StdAlternativeRecordMarshalerFactory(RecordSerializerFunction* function);

    void MakeMarshaler(RChannelItemMarshalerRef* pMarshaler,
                       DVErrorReporter* errorReporter);

private:
    RecordSerializerFunction*   m_function;

    DRREFCOUNTIMPL
};

template< class _R > class AlternativeRecordMarshaler :
    public AlternativeRecordMarshalerBase
{
public:
    typedef _R RecordType;

    virtual DrError Serialize(RecordType* record,
                              DrMemoryBufferWriter* writer) = 0;

private:
    DrError SerializeUntyped(void* record, DrMemoryBufferWriter* writer)
    {
        return Serialize((RecordType *) record, writer);
    }
};
