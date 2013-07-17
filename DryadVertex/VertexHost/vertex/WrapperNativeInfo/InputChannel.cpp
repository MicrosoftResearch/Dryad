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

#include <InputChannel.h>

#pragma unmanaged 


InputChannel::InputChannel(UInt32 portNum,
                           DryadVertexProgram* vertex, 
                           RChannelReader* channel)
{
    m_reader = channel;
    m_vertex = vertex;
    m_bytesRead = 0L;
    m_portNum = portNum;
    m_atEOC = false;
}

InputChannel::~InputChannel()
{
    m_reader = NULL;
    m_vertex = NULL;
}

void InputChannel::Stop()
{
}

bool InputChannel::AtEndOfChannel()
{
    return m_atEOC;
}

Int64 InputChannel::GetBytesRead() 
{
  return m_bytesRead;
}

RChannelReader* InputChannel::GetReader()
{
    return m_reader;
}

bool InputChannel::GetTotalLength(UInt64 *length)
{
    return m_reader->GetTotalLength(length);
}

bool InputChannel::GetExpectedLength(UInt64 *length)
{
    return m_reader->GetExpectedLength(length);
}

const char* InputChannel::GetURI()
{
    return m_reader->GetURI();
}

DataBlockItem* InputChannel::ReadDataBlock(byte **ppDataBlock,
                                           Int32 *ppDataBlockSize,
                                           Int32 *pErrorCode)
{
    RChannelItemRef nextItem;
    DataBlockItem *itemPtr = NULL;
    bool result = m_reader->FetchNextItem(&nextItem, 
                                                        DrTimeInterval_Infinite);
    LogAssert(result);
    LogAssert(nextItem != NULL, "FetchNextItem() returned a NULL item");
    RChannelItemType itemType = nextItem->GetType();

    switch (itemType) {
    case RChannelItem_Data:
        itemPtr = (DataBlockItem*) nextItem.Ptr();
        itemPtr->IncRef(); 
        *ppDataBlock = (byte *) itemPtr->GetDataAddress();
        *ppDataBlockSize = (Int32) itemPtr->GetAvailableSize();
#ifdef VERBOSE
        fprintf(stdout, "Read %d bytes from channel %u.\n", 
                itemPtr->GetAvailableSize(), m_portNum);
        fprintf(stdout, "MEM ReadDataBlock block has addr %p.\n", itemPtr);
        fflush(stdout);
#endif
        m_bytesRead += itemPtr->GetAvailableSize();
        LogAssert(itemPtr->GetAvailableSize() != 0);
        *pErrorCode = 0;
        break;
    case RChannelItem_BufferHole:
        DrLogE("Error: Received BufferHole Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_BufferHole);
        }
        *pErrorCode = 1;
        break;
    case RChannelItem_ItemHole:
        DrLogE("Received ItemHole Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ItemHole);
        }
        *pErrorCode = 1;
        break;
    case RChannelItem_EndOfStream:
        DrLogD("ReadDataBlock received EndOfStream");
        m_atEOC= true;
        *pErrorCode = 0;
        break;
    case RChannelItem_Restart:
        DrLogE("Received Restart Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ChannelRestart);
        }
        *pErrorCode = 1;
        break;
    case RChannelItem_Abort:
        DrLogE("Received Abort Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ChannelAbort);
        }
        *pErrorCode = 1;
        break;
    case RChannelItem_ParseError:
        DrLogE("Received ParseError Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ItemParseError);
        }
        *pErrorCode = 1;
        break;
    case RChannelItem_MarshalError:
        DrLogE("Received MarshalError Item from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_ItemMarshalError);
        }
        *pErrorCode = 1;
        break;
    default:
        DrLogE("Received Item of unknown type from channel.");
        if (m_vertex != NULL) {
            m_vertex->ReportError(DryadError_AssertFailure, 
                "Received Item of unknown type from channel.");
        }
        *pErrorCode = 1;
        break;
    }
    return itemPtr;
}
