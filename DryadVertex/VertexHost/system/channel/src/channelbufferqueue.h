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

class RChannelReaderImpl;
class RChannelBuffer;
class RChannelBufferReader;
class RChannelBufferPrefetchInfo;
class RChannelUnit;
class RChannelParseRequest;

#include "channelbuffer.h"
#include <channelreader.h>
#include <channelparser.h>
#include <workqueue.h>
#include <orderedsendlatch.h>

typedef DryadBList<RChannelUnit> ChannelUnitList;

class RChannelBufferQueue :
    public RChannelReaderSupplier,
    public RChannelBufferReaderHandler
{
public:
    RChannelBufferQueue(RChannelReaderImpl* parent,
                        RChannelBufferReader* bufferReader,
                        RChannelItemParserBase* parser,
                        UInt32 maxParseBatchSize,
                        UInt32 maxOutstandingUnits,
                        WorkQueue* workQueue);
    ~RChannelBufferQueue();

    void StartSupplier(RChannelBufferPrefetchInfo* prefetchCookie);
    void InterruptSupplier();
    void DrainSupplier(RChannelItem* drainItem);

    void ProcessBuffer(RChannelBuffer* buffer);
    void NotifyUnitConsumption();

    void CloseSupplier();

    bool GetTotalLength(UInt64* pLen);

private:
    enum QueueState {
        BQ_Stopped,
        BQ_Empty,
        BQ_BlockingItem,
        BQ_InWorkQueue,
        BQ_Locked,
        BQ_Stopping
    };

    enum NextParseAction {
        NPA_RequestBuffer,
        NPA_RequestItem,
        NPA_StopParsing,
        NPA_EndOfStream
    };

    void DispatchEvents(ChannelBufferList* bufferList,
                        WorkRequest* workRequest);
    void ShutDownBufferQueue(ChannelBufferList* bufferList);
    RChannelBuffer* GetAndLockCurrentBuffer(bool* outReset);
    void ProcessAfterParsing(NextParseAction nextParseAction,
                             RChannelItemArray* itemArray,
                             UInt64 numberOfSubItemsRead,
                             UInt64 dataSizeRead,
                             RChannelBufferPrefetchInfo* prefetchCookie);

    void ParseRequest(bool useNewBuffer);
    bool ShutDownRequested();

    RChannelReaderImpl*                  m_parent;
    RChannelBufferReader*                m_bufferReader;
    RChannelItemParserRef                m_parser;
    UInt32                               m_maxParseBatchSize;
    UInt32                               m_maxOutstandingUnits;
    WorkQueue*                           m_workQueue;

    QueueState                           m_state;
    DryadOrderedSendLatch<ChannelUnitList> m_sendLatch;
    bool                                 m_shutDownRequested;
    HANDLE                               m_shutDownEvent;
    HANDLE                               m_alertCompleteEvent;

    RChannelBuffer*                      m_currentBuffer;
    bool                                 m_firstBuffer;
    ChannelBufferList                    m_pendingList;
    UInt32                               m_outstandingUnits;

    UInt64                               m_nextDataSequenceNumber;
    UInt64                               m_nextDeliverySequenceNumber;

    CRITSEC                              m_baseCS;

    friend class RChannelParseRequest;
};
