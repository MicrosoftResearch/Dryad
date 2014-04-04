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

#include "concreterchannel.h"
#include "channelfifo.h"
#include <workqueue.h>

class RChannelThrottledStream
{
public:
    ~RChannelThrottledStream();
    virtual void OpenAfterThrottle() = 0;
};

class RChannelOpenThrottler
{
public:
    class Dispatch : public WorkRequest
    {
    public:
        Dispatch(RChannelThrottledStream* stream);

        void Process();
        bool ShouldAbort();

    private:
        RChannelThrottledStream*  m_stream;
    };

    RChannelOpenThrottler(UInt32 maxOpenFiles, WorkQueue* workQueue);
    ~RChannelOpenThrottler();

    bool QueueOpen(RChannelThrottledStream* stream);
    void NotifyFileCompleted();

private:
    WorkQueue*                           m_workQueue;
    UInt32                               m_maxOpenFiles;
    UInt32                               m_openFileCount;
    std::list<RChannelThrottledStream*>  m_blockedFileList;
    CRITSEC                              m_baseCS;
};

class RChannelFifoHolder
{
public:
    RChannelFifoHolder(const char* channelURI, UInt32 fifoLength,
                       bool isReader, WorkQueue* workQueue);
    ~RChannelFifoHolder();

    RChannelFifo* GetFifo();

    bool MakeReader();
    bool MakeWriter();

    bool DiscardReader();
    bool DiscardWriter();

private:
    RChannelFifo*   m_fifo;
    WorkQueue*      m_workQueue;
    bool            m_madeReader;
    bool            m_discardedReader;
    bool            m_madeWriter;
    bool            m_discardedWriter;

    CRITSEC         m_atomic;
};

class RChannelFifoReaderHolder : public RChannelReaderHolder
{
public:
    RChannelFifoReaderHolder(const char* channelURI,
                             WorkQueue* workQueue,
                             DVErrorReporter* errorReporter);
    ~RChannelFifoReaderHolder();

    RChannelReader* GetReader();
    void FillInStatus(DryadInputChannelDescription* status);
    void Close();

private:
    RChannelFifoReader* m_reader;
};

class RChannelFifoWriterHolder : public RChannelWriterHolder
{
public:
    RChannelFifoWriterHolder(const char* channelURI,
                             DVErrorReporter* errorReporter);
    ~RChannelFifoWriterHolder();

    RChannelWriter* GetWriter();
    void FillInStatus(DryadOutputChannelDescription* status);
    void Close();

private:
    RChannelFifoWriterBase* m_writer;
};

class RChannelBufferedReaderHolder : public RChannelReaderHolder
{
public:
    RChannelBufferedReaderHolder(const char* channelURI,
                                 RChannelOpenThrottler* openThrottler,
                                 DryadMetaData* metaData,
                                 RChannelItemParserBase* parser, 
                                 UInt32 numberOfReaders,
                                 UInt32 maxParseBatchSize,
                                 UInt32 maxParseUnitsInFlight,
                                 WorkQueue* workQueue,
                                 DVErrorReporter* errorReporter,
                                 LPDWORD localInputChannels);
    ~RChannelBufferedReaderHolder();

    RChannelReader* GetReader();
    void FillInStatus(DryadInputChannelDescription* status);
    void Close();

private:
    bool CreateBufferReader(UInt32 numberOfReaders,
                            RChannelOpenThrottler* openThrottler,
                            WorkQueue* workQueue,
                            const char* channelURI,
                            DryadMetaData* metaData,
                            DVErrorReporter* errorReporter,
                            LPDWORD localInputChannels);

    RChannelItemParserRef  m_parser;
    RChannelBufferReader*  m_bufferReader;
    RChannelReader*        m_reader;
};

class RChannelBufferedWriterHolder : public RChannelWriterHolder
{
public:
    RChannelBufferedWriterHolder(const char* channelURI,
                                 RChannelOpenThrottler* openThrottler,
                                 DryadMetaData* metaData,
                                 RChannelItemMarshalerBase* marshaler,
                                 UInt32 numberOfWriters,
                                 UInt32 maxMarshalBatchSize,
                                 WorkQueue* workQueue,
                                 DVErrorReporter* errorReporter);
    ~RChannelBufferedWriterHolder();

    RChannelWriter* GetWriter();
    void FillInStatus(DryadOutputChannelDescription* status);
    void Close();

private:
    void CreateBufferWriter(UInt32 numberOfWriters,
                            RChannelOpenThrottler* openThrottler,
                            const char* channelURI,
                            DryadMetaData* metaData,
                            bool* pBreakOnBufferBoundaries,
                            DVErrorReporter* errorReporter);

    RChannelItemMarshalerRef  m_marshaler;
    RChannelBufferWriter*     m_bufferWriter;
    RChannelWriter*           m_writer;
};

class RChannelNullWriterHolder : public RChannelWriterHolder
{
public:
    RChannelNullWriterHolder(const char* uri);
    ~RChannelNullWriterHolder();
    RChannelWriter* GetWriter();
    void FillInStatus(DryadOutputChannelDescription* s);
    void Close();

private:
    RChannelWriter*   m_writer;
};
