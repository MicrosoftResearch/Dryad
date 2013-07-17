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

#include "recordparser.h"

class RecordBundleInterfaceBase
{
public:
    RecordBundleInterfaceBase()
    {
        m_marshalerFactory.Attach(new MarshalerFactory());
    }

    virtual ~RecordBundleInterfaceBase() {}

    DryadParserFactoryBase* GetParserFactory()
    {
        return m_parserFactory;
    }

    DryadMarshalerFactoryBase* GetMarshalerFactory()
    {
        return m_marshalerFactory;
    }

    DObjFactoryBase* GetRecordFactory()
    {
        return m_factory;
    }

protected:
    class MarshalerFactory : public DryadMarshalerFactory
    {
    public:
        void MakeMarshaler(RChannelItemMarshalerRef* pMarshaler,
                           DVErrorReporter* errorReporter)
        {
            pMarshaler->Attach(new RChannelStdItemMarshaler());
        }
    };

    DrRef<DObjFactoryBase>   m_factory;
    DryadParserFactoryRef    m_parserFactory;
    DryadMarshalerFactoryRef m_marshalerFactory;
};

template< class _R > class PackedRecordBundleBase :
    public RecordBundleInterfaceBase
{
public:
    typedef _R RecordType;

    typedef PackedRecordArray<RecordType> Array;
    typedef RecordArrayReader<RecordType> Reader;
    typedef RecordArrayWriter<RecordType> WriterBase;

    class Writer : public WriterBase
    {
    public:
        Writer() {}
        Writer(PackedRecordBundleBase<_R>* bundle, SyncItemWriterBase* writer)
        {
            Initialize(bundle, writer);
        }

        void Initialize(PackedRecordBundleBase<_R>* bundle,
                        SyncItemWriterBase* writer)
        {
            bundle->InitializeWriter(this, writer);
        }
    };

    PackedRecordBundleBase() {}
    PackedRecordBundleBase(DObjFactoryBase* factory)
    {
        InitializeBase(factory);
    }
    virtual ~PackedRecordBundleBase() {}

    void InitializeBase(DObjFactoryBase* factory)
    {
        m_factory = factory;
        m_parserFactory.Attach(new ParserFactory(m_factory));
    }

    void InitializeWriter(WriterBase* writer,
                          SyncItemWriterBase* channelWriter)
    {
        writer->Initialize(channelWriter, m_factory);
    }

private:
    class ParserFactory : public DryadParserFactory
    {
    public:
        ParserFactory(DObjFactoryBase* factory)
        {
            m_factory = factory;
        }

        void MakeParser(RChannelItemParserRef* pParser,
                        DVErrorReporter* errorReporter)
        {
            pParser->Attach(new PackedRecordArrayParser(m_factory));
        }

        DObjFactoryRef m_factory;
    };
};

template< class _R > class PackedRecordBundle :
    public PackedRecordBundleBase<_R>
{
public:
    typedef RecordArrayFactory<Array> Factory;

    PackedRecordBundle()
    {
        Initialize(RChannelItem::s_defaultRecordBatchSize);
    }

    PackedRecordBundle(UInt32 maxArraySize)
    {
        Initialize(maxArraySize);
    }

    void Initialize(UInt32 maxArraySize)
    {
        DrRef<Factory> factory;
        factory.Attach(new Factory(maxArraySize));
        InitializeBase(factory);
    }
};

template< class _R > class RecordBundleBase :
    public RecordBundleInterfaceBase
{
public:
    typedef _R RecordType;

    typedef RecordArray<RecordType> Array;
    typedef RecordArrayReader<RecordType> Reader;
    typedef RecordArrayWriter<RecordType> WriterBase;

    class Writer : public WriterBase
    {
    public:
        Writer() {}
        Writer(RecordBundleBase<_R>* bundle, SyncItemWriterBase* writer)
        {
            Initialize(bundle, writer);
        }

        void Initialize(RecordBundleBase<_R>* bundle,
                        SyncItemWriterBase* writer)
        {
            bundle->InitializeWriter(this, writer);
        }
    };

    RecordBundleBase() {}
    RecordBundleBase(DObjFactoryBase* factory)
    {
        InitializeBase(factory);
    }
    virtual ~RecordBundleBase() {}

    void InitializeBase(DObjFactoryBase* factory)
    {
        m_factory = factory;
        m_parserFactory.Attach(new ParserFactory(m_factory));
    }

    void InitializeWriter(WriterBase* writer,
                          SyncItemWriterBase* channelWriter)
    {
        writer->Initialize(channelWriter, m_factory);
    }

private:
    class ParserFactory : public DryadParserFactory
    {
    public:
        ParserFactory(DObjFactoryBase* factory)
        {
            m_factory = factory;
        }

        void MakeParser(RChannelItemParserRef* pParser,
                        DVErrorReporter* errorReporter)
        {
            pParser->Attach(new RChannelStdItemParser(m_factory));
        }

        DObjFactoryRef m_factory;
    };
};

template< class _R > class RecordBundle : public RecordBundleBase<_R>
{
public:
    typedef RecordArrayFactory<Array> Factory;

    RecordBundle()
    {
        Initialize(RChannelItem::s_defaultRecordBatchSize);
    }

    RecordBundle(UInt32 maxArraySize)
    {
        Initialize(maxArraySize);
    }

    void Initialize(UInt32 maxArraySize)
    {
        DrRef<Factory> factory;
        factory.Attach(new Factory(maxArraySize));
        InitializeBase(factory);
    }
};
