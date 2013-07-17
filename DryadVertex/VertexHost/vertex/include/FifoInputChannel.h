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

#include <inputchannel.h>
#include <fifochannel.h>

class FifoInputChannel :  public InputChannel
{
public:
    FifoInputChannel(UInt32 portNum, DryadVertexProgram* vertex, 
        WorkQueue *workQueue, RChannelReader* channel, 
        TransformType tType);
    void Stop();
    virtual DataBlockItem* ReadDataBlock(byte **ppDataBlock,
        Int32 *ppDataBlockSize,
        Int32 *pErrorCode);
    bool GetTotalLength(UInt64 *length);
    bool GetExpectedLength(UInt64 *length);    
    const char* GetURI();
private:
    void MakeFifo(UInt32 fifoLength, WorkQueue* workQueue);

    bool m_initialHandlerSent;
    FifoChannel *m_fifoChannel;
    RChannelReaderHolderRef   m_fifoReader;
    RChannelWriterHolderRef   m_fifoWriter;
    RChannelReader* m_origReader;
};
