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

#include <dryaderrordef.h>
#include <wrappernativeinfo.h>
#include <fifoinputchannel.h>
#include <fifooutputchannel.h>

#pragma unmanaged 

WrapperNativeInfoBase::~WrapperNativeInfoBase()
{
}

//#define VERBOSE 
WrapperNativeInfo::WrapperNativeInfo(UInt32 numberOfInputChannels,
    RChannelReader** inputChannel,
    UInt32 numberOfOutputChannels,
    RChannelWriter** outputChannel, 
    DryadVertexProgram* vertex,
    WorkQueue* workQueue)
{
#ifdef VERBOSE
    if (DrLogging::Enabled(LogLevel_Info))
    {         
        fprintf(stdout, "WrapperNativeInfo::WrapperNativeInfo: %p %u\n", this, numberOfInputChannels);
        fflush(stdout);
    }
#endif
    DrLogI( "Creating WrapperNativeInfo");
    m_numberOfInputChannels = numberOfInputChannels;
    m_inputChannels = new InputChannel*[m_numberOfInputChannels];

    m_numberOfOutputChannels = numberOfOutputChannels;
    m_outputChannels = new OutputChannel*[m_numberOfOutputChannels];

    m_vertex = vertex;
    m_workQueue = workQueue;

    for (UInt32 i = 0; i < m_numberOfInputChannels; i++) {
      m_inputChannels[i] = new InputChannel(i, m_vertex, inputChannel[i]);
    }

    for (UInt32 i = 0; i < m_numberOfOutputChannels; i++)
    {
      m_outputChannels[i] = new OutputChannel(i, m_vertex, outputChannel[i]);
    }
}

WrapperNativeInfo::~WrapperNativeInfo()
{
    m_vertex = NULL;
    m_workQueue = NULL;

    for (UInt32 i = 0; i < m_numberOfInputChannels; i++)
    {
        delete m_inputChannels[i];
    }
    m_inputChannels = NULL;

    for (UInt32 i = 0; i < m_numberOfOutputChannels; i++)
    {
        delete m_outputChannels[i];
    }
    m_outputChannels = NULL;
}

void WrapperNativeInfo::CleanUp()
{
    // we don't own the reader/writer reference, so we should not delete it

    DrLogI( "Cleaning up WrapperNativeInfo");

    /* first call stop on all the channels to drain any transforms
       that are still in flight */
    for (UInt32 i = 0; i < m_numberOfInputChannels; i++) {
        m_inputChannels[i]->Stop();
    }

    for (UInt32 i = 0; i < m_numberOfOutputChannels; i++)
    {
        m_outputChannels[i]->Stop();
    }

    // enumerate which channels were not completely read
    for (UInt32 i = 0; i < m_numberOfInputChannels; i++)
    {
        if (!m_inputChannels[i]->AtEndOfChannel())
        {
             DrLogI("The client did not completely read channel: %u ", i);
        }
    }

    // output read/write stats for debugging
    Int64 totalBytesRead = 0L;
    Int64 totalBytesWritten = 0L;
    for (UInt32 i = 0; i < m_numberOfInputChannels; i++)
    {
        Int64 channelBytesRead = m_inputChannels[i]->GetBytesRead();        
        DrLogI(
                "WrapperNativeInfo read %I64d bytes from channel %u.", 
                channelBytesRead, i);
        
        totalBytesRead += channelBytesRead;
    }

    for (UInt32 i = 0; i < m_numberOfOutputChannels; i++)
    {
        Int64 channelBytesWritten = m_outputChannels[i]->GetBytesWritten();
        DrLogI("WrapperNativeInfo wrote %I64d bytes to channel %u.",
                    channelBytesWritten, i);
        totalBytesWritten += channelBytesWritten;
    }
        
    DrLogI("WrapperNativeInfo read %I64d bytes from all channels.", totalBytesRead);
    DrLogI("WrapperNativeInfo wrote %I64d bytes to all channels.", totalBytesWritten);
}

Int64 WrapperNativeInfo::GetTotalLength(UInt32 portNum)
{
    UInt64 len;
    bool isKnown = m_inputChannels[portNum]->GetTotalLength(&len);
    return (isKnown) ? len : -1;
}

Int64 WrapperNativeInfo::GetExpectedLength(UInt32 portNum)
{
    UInt64 len;
    bool isKnown = m_inputChannels[portNum]->GetExpectedLength(&len);
    return (isKnown) ? len : -1;
}

Int64 WrapperNativeInfo::GetVertexId()
{
    return m_vertex->GetVertexId();
}

void WrapperNativeInfo::SetInitialSizeHint(UInt32 portNum, UInt64 hint)
{
    m_outputChannels[portNum]->SetInitialSizeHint(hint);
}

UInt32 WrapperNativeInfo::GetNumOfInputs()
{
#ifdef VERBOSE
    if (DrLogging::Enabled(LogLevel_Info))
    {         
        fprintf(stdout, "WrapperNativeInfo::GetNumOfInputs: %p %u\n", this, m_numberOfInputChannels);
        fflush(stdout);
    }
#endif
    return m_numberOfInputChannels;
}

UInt32 WrapperNativeInfo::GetNumOfOutputs()
{
    return m_numberOfOutputChannels;
}

const char* WrapperNativeInfo::GetInputChannelURI(UInt32 portNum)
{
    return m_inputChannels[portNum]->GetURI();
}

const char* WrapperNativeInfo::GetOutputChannelURI(UInt32 portNum)
{
    return m_outputChannels[portNum]->GetURI();
}

DataBlockItem* WrapperNativeInfo::AllocateDataBlock(Int32 dataBlockSize,
                                                    byte **pDataBlock)
{
    DrLogD("AllocateDataBlock(): dataBlockSize = %d", dataBlockSize);

    DataBlockItem *dbi = new DataBlockItem(dataBlockSize);
    *pDataBlock = (byte *) dbi->GetDataAddress();
#ifdef VERBOSE
    if (DrLogging::Enabled(LogLevel_Info))
    {         
        fprintf(stdout, "MEM AllocateDataBlock block has addr %p.\n", dbi);
        fflush(stdout);
    }
#endif
    return dbi;
}

void WrapperNativeInfo::ReleaseDataBlock(DataBlockItem *pItem)
{
#ifdef VERBOSE
    if (DrLogging::Enabled(LogLevel_Info))
    {         
        fprintf(stdout, "MEM ReleaseDataBlock block has addr %p.\n", pItem);
        fflush(stdout);
    }
#endif
    if (pItem != NULL) 
    {
        pItem->DecRef();
    }
}

DataBlockItem* WrapperNativeInfo::ReadDataBlock(UInt32 portNum, byte **ppDataBlock, Int32 *ppDataBlockSize, Int32* pErrorCode)
{
    LogAssert(portNum < m_numberOfInputChannels);

    DrLogD("ReadDataBlock() entering: portNum = %d", portNum);

    DataBlockItem *dbi = m_inputChannels[portNum]->ReadDataBlock(ppDataBlock, ppDataBlockSize, pErrorCode);

    DrLogD("ReadDataBlock() returning: portNum = %d, dataBlockSize = %d, errorCode = %d", portNum, *ppDataBlockSize, *pErrorCode);

    return dbi;
}

BOOL WrapperNativeInfo::WriteDataBlock(UInt32 portNum, 
                                       DataBlockItem *pItem,
                                       Int32 numBytesToWrite)
{
    DrLogD("WriteDataBlock() entering: portNum = %d", portNum);

    LogAssert(portNum < m_numberOfOutputChannels);

    BOOL retVal = m_outputChannels[portNum]->WriteDataBlock(pItem, numBytesToWrite); 

    DrLogD("WriteDataBlock() returning: portNum = %d, success = %d ", portNum, retVal);

    return retVal;
}


void WrapperNativeInfo::EnableFifoInputChannel(Int32 compressionScheme, 
    UInt32 channel)
{
    DrLogI( "Enabling fifo for input channel. Channel %u scheme %d", channel, compressionScheme);
    LogAssert(channel < m_numberOfInputChannels);
    LogAssert((compressionScheme >= 0) && ( compressionScheme <= 6));
    TransformType tType = TT_NullTransform;
    if (compressionScheme == 1)
    {
        tType = TT_GzipDecompression;
    }
    else if (compressionScheme == 2)
    {
        tType = TT_GzipDecompression;
    }
    else if (compressionScheme == 3)
    {
        tType = TT_DeflateDecompression;
    }
    else if (compressionScheme == 4)
    {
        tType = TT_DeflateDecompression;
    }

    /* Xpress removed, but left in comments as an example of 
     * supporting an alternate compression scheme.
    else if (compressionScheme == 5)
    {
        tType = TT_XpressDecompression;
    }
    else if (compressionScheme == 6)
    {
        tType = TT_XpressDecompression;
    }
    */

    InputChannel *origChannel = m_inputChannels[channel];
    m_inputChannels[channel] = new FifoInputChannel(channel, 
        m_vertex, m_workQueue, origChannel->GetReader(), tType);
    delete origChannel;
}

void WrapperNativeInfo::EnableFifoOutputChannel(Int32 compressionScheme, 
    UInt32 channel)
{
    DrLogI( "Enabling fifo for output channel. Channel %u scheme %d", channel, compressionScheme);
    LogAssert(channel < m_numberOfOutputChannels);
    LogAssert((compressionScheme >= 0) && ( compressionScheme <= 6));
    TransformType tType = TT_NullTransform;
    if (compressionScheme == 1) 
    {
        tType = TT_GzipCompression;
    }
    else if (compressionScheme == 2)
    {
        tType = TT_GzipFastCompression;
    }
    else if (compressionScheme == 3)
    {
        tType = TT_DeflateCompression;
    }
    else if (compressionScheme == 4)
    {
        tType = TT_DeflateFastCompression;
    }

    /* Xpress removed, but left in comments as an example of 
     * supporting an alternate compression scheme.
    else if (compressionScheme == 5)
    {
        tType = TT_XpressCompression;
    }
    else if (compressionScheme == 6)
    {
        tType = TT_XpressFastCompression;
    }
    */

    OutputChannel *origChannel = m_outputChannels[channel];
    m_outputChannels[channel] = new FifoOutputChannel(channel, 
        m_vertex, m_workQueue, origChannel->GetWriter(), tType);
    delete origChannel;
} 
