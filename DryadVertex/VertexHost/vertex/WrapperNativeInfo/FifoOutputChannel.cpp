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

#include <FifoOutputChannel.h>
#include <NullChannelTransform.h>
#include <channelinterface.h>


#pragma unmanaged 


FifoOutputChannel::FifoOutputChannel(UInt32 portNum, DryadVertexProgram* vertex, 
    WorkQueue *workQueue, RChannelWriter* outputChannel, TransformType tType):
    OutputChannel(portNum, vertex, outputChannel) 
{
    m_origWriter = outputChannel;
    m_fifoReader = NULL;
    m_fifoWriter = NULL;
    MakeFifo(3, workQueue);
    m_fifoChannel = new FifoChannel(m_fifoReader->GetReader(), m_origWriter, vertex, tType);

    m_writer = m_fifoWriter->GetWriter();    
    m_fifoChannel->Start();
}

void FifoOutputChannel::Stop()
{
    /* this makes sure no more items will be sent down the fifo */
    bool mustWait = m_fifoChannel->Stop(m_fifoWriter->GetWriter());
    m_fifoChannel->Drain(mustWait);
    delete m_fifoChannel;
    m_fifoChannel = NULL;

    m_fifoReader->GetReader()->Drain();

    RChannelItemRef writeTermination;
    m_fifoWriter->GetWriter()->Drain(DrTimeInterval_Zero, &writeTermination);
    if (writeTermination != NULL)
    {
        if (writeTermination->GetType() != RChannelItem_EndOfStream)
        {
            // do something
        }
    }
    
    m_fifoReader = NULL;
    m_fifoWriter = NULL;
}

void FifoOutputChannel::MakeFifo(UInt32 fifoLength, WorkQueue* workQueue)
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

void FifoOutputChannel::SetInitialSizeHint(UInt64 hint)
{
    m_origWriter->SetInitialSizeHint(hint);
}

const char* FifoOutputChannel::GetURI()
{
    return m_origWriter->GetURI();
}
