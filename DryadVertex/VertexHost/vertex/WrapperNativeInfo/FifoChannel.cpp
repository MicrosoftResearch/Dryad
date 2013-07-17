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

#include <fifochannel.h>
#include <nullchanneltransform.h>
#include <gzipdecompressionchanneltransform.h>
#include <gzipcompressionchanneltransform.h>

#pragma unmanaged 


FifoChannelItemWriterHandler::~FifoChannelItemWriterHandler() 
{
// nothing needed for now.    
}

void FifoChannelItemWriterHandler::ProcessWriteCompleted(RChannelItemType status,
        RChannelItem* marshalFailureItem)
{
// NYI
}



FifoChannel::FifoChannel(RChannelReader* reader, 
                         RChannelWriter *writer, 
                         DryadVertexProgram* vertex,
                         TransformType transType)
{
    m_initialHandlerSent = false;
    m_vertex = vertex;
    switch (transType) 
    {
    case TT_NullTransform:
        m_transform = new NullChannelTransform(vertex);
        break;
#ifdef LINKWITHZLIB
    case TT_GzipCompression:
        m_transform = new GzipCompressionChannelTransform(vertex, true, false);
        break;
    case TT_GzipFastCompression:
        m_transform = new GzipCompressionChannelTransform(vertex, true, true);
        break;
    case TT_GzipDecompression:
        m_transform = new GzipDecompressionChannelTransform(vertex, true);
        break;
    case TT_DeflateCompression:
        m_transform = new GzipCompressionChannelTransform(vertex, false, false);
        break;
    case TT_DeflateFastCompression:
        m_transform = new GzipCompressionChannelTransform(vertex, false, true);
        break;
    case TT_DeflateDecompression:
        m_transform = new GzipDecompressionChannelTransform(vertex, false);
        break;
#endif 
    default:
        DrLogE("Invalid compressionScheme.");
        LogAssert(false);
    }
    m_reader = reader;
    m_writer = writer;
    m_shutdownEvent = CreateEvent(NULL, false, false, NULL);  
    LogAssert(m_shutdownEvent != NULL);
    m_numItemsInFlight = 0;
    m_state = RS_Stopped;
    m_fifoWriterHandler = NULL;
}

FifoChannel::~FifoChannel()
{
    LogAssert(m_state == RS_Stopped);

    delete m_transform;
    m_transform = NULL;

    BOOL bRet = ::CloseHandle(m_shutdownEvent);
    LogAssert(bRet != 0);

    if (m_fifoWriterHandler)
    {
        delete m_fifoWriterHandler;
        m_fifoWriterHandler = NULL;
    }
}

bool FifoChannel::Stop(RChannelWriter* fifoWriter)
{
    DrLogI( "Stopping fifochannel");
    bool mustWait = false;
    bool mustTerminate = false;

    {
        AutoCriticalSection acs(&m_critsec);

        if (m_state == RS_OutstandingHandler) 
        {
            mustTerminate = true;
            mustWait = true;
        } 

        if (m_state == RS_Stopping) 
        {
            mustWait = true;
        }
    }

    if (mustTerminate)
    {
        DrLogI( "Sending fifochannel termination");
        RChannelItemRef termination;
        termination.Attach(RChannelMarkerItem::Create(RChannelItem_EndOfStream,
                                                      false));
        if (fifoWriter == m_writer) 
        {
            this->WriteTransformedItem(termination.Ptr());
        } 
        else 
        {
            m_fifoWriterHandler = new FifoChannelItemWriterHandler();
            fifoWriter->WriteItem(termination, false, m_fifoWriterHandler);
        }
    }
    return mustWait;
}

void FifoChannel::Drain(bool mustWait)
{
    DrLogI( "Waiting for fifochannel");

    if (mustWait) 
    {
        
        WaitForSingleObject(m_shutdownEvent, INFINITE);
    }

    DrLogI( "Waiting for fifochannel done");
    {
        AutoCriticalSection acs(&m_critsec);
        
        m_state = RS_Stopped;
    }
}

void FifoChannel::ProcessItem(RChannelItem* deliveredItem)
{
    {
        AutoCriticalSection acs(&m_critsec);
        m_numItemsInFlight++;
    }

    DrLogI( "In ProcessItem");
    
    RChannelItemType itemType = deliveredItem->GetType();
    if (RChannelItem::IsTerminationItem(itemType)) 
    {
        DrLogI( "Got termination item");
        m_transform->Finish(itemType == RChannelItem_EndOfStream);
        WriteTransformedItem(deliveredItem);
        {
            AutoCriticalSection acs(&m_critsec);
            m_state = RS_Stopping;
        }
    }
    else 
    {
        if (itemType == RChannelItem_Data) 
        {
            DataBlockItem* itemPtr =
                dynamic_cast<DataBlockItem*>(deliveredItem);
            LogAssert(itemPtr != NULL);
            m_transform->ProcessItem(itemPtr);
        }
    }
    
    {
        AutoCriticalSection acs(&m_critsec);
        LogAssert(m_numItemsInFlight > 0);
        m_numItemsInFlight--;
        MaybeSendHandler();
    }
}

void FifoChannel::ProcessWriteCompleted(RChannelItemType status,
    RChannelItem* marshalFailureItem)
{
    
    {
        AutoCriticalSection acs(&m_critsec);
        DrLogD( "In Process Write Complete. Status: %d, m_numItemsInFlight: %d", status, 
            m_numItemsInFlight);

        LogAssert(m_numItemsInFlight > 0);
        m_numItemsInFlight--;
        
        MaybeSendHandler();
    }
}

void FifoChannel::MaybeSendHandler() 
{
    {
        AutoCriticalSection acs(&m_critsec);
        if (m_numItemsInFlight == 0) 
        {
            if (m_state == RS_Stopping) 
            { 
                SetEvent(m_shutdownEvent);
            } 
            else 
            {
                m_reader->SupplyHandler(this, NULL);
            }
        }
    }
}

void FifoChannel::Start() 
{
    DrLogI( "Starting fifochannel");
    LogAssert(!m_initialHandlerSent);
    m_initialHandlerSent = true;
    m_transform->Start(this);
    m_state = RS_OutstandingHandler;
    MaybeSendHandler();
}


void FifoChannel::WriteTransformedItem(RChannelItem* transformedItem)
{
    {
        AutoCriticalSection acs(&m_critsec);
        m_numItemsInFlight++;
        m_writer->WriteItem(transformedItem, false, this);
    }
}
