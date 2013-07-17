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
#include <channelinterface.h>
#include <channeltransform.h>
#include <dryadvertex.h>

enum FifoReaderState {
    RS_Stopped,
    RS_OutstandingHandler,
    RS_Stopping
};

class FifoChannelItemWriterHandler :    public RChannelItemWriterHandler
{
public:
    virtual ~FifoChannelItemWriterHandler();
    virtual void ProcessWriteCompleted(RChannelItemType status,
        RChannelItem* marshalFailureItem); 
};

class FifoChannel :    public RChannelItemWriterHandler,
                             public RChannelItemReaderHandlerQueued
{
public:
    FifoChannel(RChannelReader* reader, 
                RChannelWriter *writer, 
                DryadVertexProgram* vertex,
                TransformType transType);
    virtual ~FifoChannel();
    virtual void ProcessItem(RChannelItem* deliveredItem);
    virtual void ProcessWriteCompleted(RChannelItemType status,
        RChannelItem* marshalFailureItem);    
    void MaybeSendHandler();
    void Start();
    bool Stop(RChannelWriter* fifoWriter);
    void Drain(bool mustWait);
    void WriteTransformedItem(RChannelItem* transformedItem);
private:

    bool m_initialHandlerSent;
    ChannelTransform *m_transform;
    RChannelReader   *m_reader;
    RChannelWriter   *m_writer;
    CRITSEC m_critsec;
    HANDLE m_shutdownEvent;
    int m_numItemsInFlight;
    FifoReaderState m_state;
    FifoChannelItemWriterHandler *m_fifoWriterHandler;
    DryadVertexProgram *m_vertex;
};
