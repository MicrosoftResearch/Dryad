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

#include <errorreporter.h>
#include <channelinterface.h>
#include <channelparser.h>
#include <channelmarshaler.h>

class WorkQueue;

class RChannelOpenThrottler;

class ConcreteRChannel
{
public:
    static bool IsNTFSFile(const char* uri);
//JC    static bool IsXComputeFile(const char* uri);
//JC    static bool IsDryadStream(const char* uri);
//JC    static bool IsDryadPipe(const char* uri);
    static bool IsDscStream(const char* uri);
    static bool IsDscPartition(const char* uri);
    static bool IsHdfsFile(const char* uri);
    static bool IsHdfsPartition(const char* uri);
    static bool IsAzureBlob(const char* uri);
    static bool IsUncPath(const char* uri);
    static bool IsFifo(const char* uri);
    static bool IsNull(const char* uri);
    static bool IsNamedPipe(const char* uri);
    static bool IsTidyFSStream(const char* uri);
    static const UInt32 s_infiniteFifoBuffer = (UInt32) -1;
};

class RChannelReaderHolder : public DrRefCounter
{
public:
    virtual ~RChannelReaderHolder();

    virtual RChannelReader* GetReader() = 0;
    virtual void FillInStatus(DryadInputChannelDescription* status) = 0;
    virtual void Close() = 0;
};

typedef DrRef<RChannelReaderHolder> RChannelReaderHolderRef;

class RChannelWriterHolder : public DrRefCounter
{
public:
    virtual ~RChannelWriterHolder();

    virtual RChannelWriter* GetWriter() = 0;
    virtual void FillInStatus(DryadOutputChannelDescription* status) = 0;
    virtual void Close() = 0;
};

typedef DrRef<RChannelWriterHolder> RChannelWriterHolderRef;

class RChannelFactory
{
public:
    static DrError OpenReader(const char* channelURI,
                              DryadMetaData* metaData,
                              RChannelItemParserBase* parser,
                              UInt32 numberOfReaders,
                              RChannelOpenThrottler* openThrottler,
                              UInt32 maxParseBatchSize,
                              UInt32 maxParseUnitsInFlight,
                              WorkQueue* workQueue,
                              DVErrorReporter* errorReporter,
                              RChannelReaderHolderRef* pHolder /* out */,
                              LPDWORD localInputChannels);

    static DrError OpenWriter(const char* channelURI,
                              DryadMetaData* metaData,
                              RChannelItemMarshalerBase* marshaler,
                              UInt32 numberOfWriters,
                              RChannelOpenThrottler* openThrottler,
                              UInt32 maxMarshalBatchSize,
                              WorkQueue* workQueue,
                              DVErrorReporter* errorReporter,
                              RChannelWriterHolderRef* pHolder /* out */);

    /* this returns an ID that is guaranteed different to any other
       return value from this call in this process, and can be used
       when minting internal fifo names to ensure that two vertices
       don't accidentally use the same name */
    static UInt32 GetUniqueFifoId();

    /* this returns a throttler object that can be used to set the
       maximum number of concurrently open readers and writers from a
       set. When the throttler is no longer needed it should be passed
       back to DiscardOpenThrottler. */
    static RChannelOpenThrottler* MakeOpenThrottler(UInt32 maxOpens,
                                                    WorkQueue* workQueue);
    static void DiscardOpenThrottler(RChannelOpenThrottler* throttler);
};
