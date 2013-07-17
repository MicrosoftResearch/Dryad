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

#include "stdafx.h"

#include <FifoInputChannel.h>

#pragma unmanaged 

FifoInputChannel::FifoInputChannel(UInt32 portNum, 
    DryadVertexProgram* vertex, WorkQueue *workQueue, 
    RChannelReader* channel, TransformType tType) : 
    InputChannel(portNum, vertex, channel)
{
    m_initialHandlerSent = false; 

    m_fifoReader = NULL;
    m_fifoWriter = NULL;
    m_origReader = channel;

    MakeFifo(3, workQueue);
    m_fifoChannel = new FifoChannel(m_origReader, m_fifoWriter->GetWriter(), vertex, tType);
    m_reader = m_fifoReader->GetReader();
}

void FifoInputChannel::Stop()
{ 
    bool mustWait = false;

    if (m_initialHandlerSent) { 
	/* this makes sure no more items will be sent down the fifo */
	mustWait = m_fifoChannel->Stop(m_fifoWriter->GetWriter());
    } 
    else 
    {
        // we never started the FifoChannel, so we must send a term item 	
	RChannelItemRef termination;
        termination.Attach(RChannelMarkerItem::Create(RChannelItem_EndOfStream,
			       false));
/*        
// by construction, we've never written any items to the FIFO, so a 
	//WriteItemSync call should not block 
	RChannelItemType result = m_fifoWriter->GetWriter()->WriteItemSync(
	    termination.Ptr(), false, NULL);
*/
	FifoChannelItemWriterHandler *fifoWriterHandler = new FifoChannelItemWriterHandler();
	m_fifoWriter->GetWriter()->WriteItem(termination, false, fifoWriterHandler);
	//LogAssert(result == RChannelItem_EndOfStream);
    }

    m_fifoReader->GetReader()->Drain();

    RChannelItemRef writeTermination;
    m_fifoWriter->GetWriter()->Drain(DrTimeInterval_Zero, &writeTermination);
    if (writeTermination != NULL)
    {
        if (writeTermination->GetType() != RChannelItem_EndOfStream)
        {
            // NYI: do something
        }
    }

    // as above, we've never called Start on the FifoChannel, 
    //so no need to call Drain
    if (m_initialHandlerSent) { 
	m_fifoChannel->Drain(mustWait);
    }
    delete m_fifoChannel;
    m_fifoChannel = NULL;

    m_fifoReader = NULL;
    m_fifoWriter = NULL;
}

bool FifoInputChannel::GetTotalLength(UInt64 *length)
{
    return m_origReader->GetTotalLength(length);
}

bool FifoInputChannel::GetExpectedLength(UInt64 *length)
{
    return m_origReader->GetExpectedLength(length);
}

void FifoInputChannel::MakeFifo(UInt32 fifoLength, WorkQueue* workQueue)
{
// based on DryadSubGraphVertex::EdgeInfo::MakeFifo in subgraphvertex.cpp 
    LogAssert(m_fifoReader == NULL);
    LogAssert(m_fifoWriter == NULL);
    
    UInt32 uniquifier = RChannelFactory::GetUniqueFifoId();
    
    DrStr64 fifoName;
    fifoName.SetF("fifo://%u/compressedchannel-%u",
        fifoLength, uniquifier);
    
    DVErrorReporter errorReporter;
    RChannelFactory::OpenReader(fifoName, NULL, NULL, 1, NULL, 0, 0, workQueue,
        &errorReporter, &m_fifoReader, NULL);
    LogAssert(errorReporter.NoError());
    RChannelFactory::OpenWriter(fifoName, NULL, NULL, 1, NULL, 0, NULL,
        &errorReporter, &m_fifoWriter);
    LogAssert(errorReporter.NoError());
    
    m_fifoReader->GetReader()->Start(NULL);
    m_fifoWriter->GetWriter()->Start();
}

DataBlockItem*  FifoInputChannel::ReadDataBlock(byte **ppDataBlock,
    Int32 *ppDataBlockSize,
    Int32 *pErrorCode)
{
    if (!m_initialHandlerSent)
    {
        m_initialHandlerSent = true;
        m_fifoChannel->Start();
    }
    return InputChannel::ReadDataBlock(ppDataBlock, ppDataBlockSize,
        pErrorCode);
}

const char* FifoInputChannel::GetURI()
{
    return m_origReader->GetURI();
}

