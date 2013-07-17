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

#include <OutputChannel.h>

#pragma unmanaged 


OutputChannel::OutputChannel(UInt32 portNum,
                             DryadVertexProgram* vertex, 
                             RChannelWriter* outputChannel)
{
    m_writer = outputChannel;
    m_vertex = vertex;
    m_bytesWritten = 0L;
    m_portNum = portNum;
}

OutputChannel::~OutputChannel()
{
    m_writer = NULL;
    m_vertex = NULL;
}


void OutputChannel::Stop()
{
}

Int64 OutputChannel::GetBytesWritten()
{
    return m_bytesWritten;
}

void OutputChannel::SetInitialSizeHint(UInt64 hint)
{
    m_writer->SetInitialSizeHint(hint);
}

const char* OutputChannel::GetURI()
{
    return m_writer->GetURI();
}

RChannelWriter* OutputChannel::GetWriter()
{
    return m_writer;
}

BOOL OutputChannel::WriteDataBlock(DataBlockItem *pItem,
                                   Int32 numBytesToWrite) 
{
    if (numBytesToWrite != (Int32) pItem->GetAvailableSize())
    {
      pItem->SetAvailableSize(numBytesToWrite);
    }
    
    m_bytesWritten += numBytesToWrite;
    BOOL returnValue = true;
    RChannelItemRef marshalFailureItem;
    RChannelItemType result = m_writer->WriteItemSync(pItem, 
        false, &marshalFailureItem);
    switch (result) {
    case RChannelItem_Data:
        // successful write
        returnValue = true;
        break;
    case RChannelItem_EndOfStream:
        DrLogE("Received EndOfStream Item from channel write.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ChannelWriteError);
        }
        returnValue = false;
        break;
    case RChannelItem_Restart:
        DrLogE("Received Restart Item from channel write.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ChannelRestart);
        }
        returnValue = false;
        break;
    case RChannelItem_Abort:
        DrLogE("Received Abort Item from channel write.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ChannelAbort);
        }
        returnValue = false;
        break;
    default:
        DrLogE("Received Item of unexpected type from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_AssertFailure, 
                "Received Item of unexpected type from channel.");
        }
        returnValue = false;
        break;
    }        

    // and make sure there was not a marshalling error
    if (marshalFailureItem.Ptr() != NULL) 
    {
        DrLogE("Received MarshalError Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ItemMarshalError);
        }
        returnValue = false;
    }

#ifdef VERBOSE
    fprintf(stdout, "MEM WriteDataBlock block has addr %p.\n", pItem);
    fflush(stdout);
#endif    

    return returnValue;
}

void OutputChannel::ProcessWriteCompleted(RChannelItemType status,
                                          RChannelItem* marshalFailureItem)
{
     LogAssert(status == RChannelItem_Data);
}
