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

#include <DrCommon.h>
#include <managedwrapper.h>
#include <datablockitem.h>
#include <inputchannel.h>
#include <outputchannel.h>

class WrapperNativeInfoBase
{
public:
    virtual ~WrapperNativeInfoBase();

    virtual void CleanUp() = 0;

    virtual DataBlockItem* AllocateDataBlock(Int32 dataBlockSize,
                                             byte **pDataBlock) = 0;
    virtual void ReleaseDataBlock(DataBlockItem *pItem) = 0;

    virtual DataBlockItem* ReadDataBlock(UInt32 portNum,
                                         byte **ppDataBlock,
                                         Int32 *ppDataBlockSize,
                                         Int32 *pErrorCode) = 0;
    virtual BOOL WriteDataBlock(UInt32 portNum, 
                                DataBlockItem *pData,
                                Int32 numBytesToWrite) = 0;
    virtual Int64 GetTotalLength(UInt32 portNum) = 0;
    virtual Int64 GetExpectedLength(UInt32 portNum) = 0;    
    virtual Int64 GetVertexId() = 0;
    virtual void SetInitialSizeHint(UInt32 portNum, UInt64 hint) = 0;
    virtual UInt32 GetNumOfInputs() = 0;
    virtual UInt32 GetNumOfOutputs() = 0;
    virtual const char* GetInputChannelURI(UInt32 portNum) = 0;
    virtual const char* GetOutputChannelURI(UInt32 portNum) = 0;    
    virtual void EnableFifoInputChannel(int compresionScheme,
                                        UInt32 channel) = 0;
    virtual void EnableFifoOutputChannel(int compresionScheme,
                                         UInt32 channel) = 0;
};

class WrapperNativeInfo : public WrapperNativeInfoBase
{
public:
    WrapperNativeInfo(UInt32 numberOfInputChannels,
        RChannelReader** inputChannel,
        UInt32 numberOfOutputChannels,
        RChannelWriter** outputChannel,
        DryadVertexProgram* vertex,
        WorkQueue* workQueue);

    ~WrapperNativeInfo();
    void CleanUp();

    DataBlockItem* AllocateDataBlock(Int32 dataBlockSize,
                                     byte **pDataBlock);
    void ReleaseDataBlock(DataBlockItem *pItem);

    DataBlockItem* ReadDataBlock(UInt32 portNum,
                                 byte **ppDataBlock,
                                 Int32 *ppDataBlockSize,
                                 Int32 *pErrorCode);
    BOOL WriteDataBlock(UInt32 portNum, 
                        DataBlockItem *pData,
                        Int32 numBytesToWrite);
    Int64 GetTotalLength(UInt32 portNum);
    Int64 GetExpectedLength(UInt32 portNum);
    Int64 GetVertexId();
    void SetInitialSizeHint(UInt32 portNum, UInt64 hint);
    const char* GetInputChannelURI(UInt32 portNum);
    const char* GetOutputChannelURI(UInt32 portNum); 
    UInt32 GetNumOfInputs();
    UInt32 GetNumOfOutputs();
    void EnableFifoInputChannel(int compresionScheme, UInt32 channel); 
    void EnableFifoOutputChannel(int compresionScheme, UInt32 channel); 

private:
    UInt32 m_numberOfInputChannels;
    InputChannel** m_inputChannels;
    UInt32 m_numberOfOutputChannels;
    OutputChannel** m_outputChannels;
    DryadVertexProgram* m_vertex;
    WorkQueue* m_workQueue;
};

extern "C" {
    UInt32 GetNumOfInputs(WrapperNativeInfoBase *info);
    UInt32 GetNumOfOutputs(WrapperNativeInfoBase *info);
    Int64 GetTotalLength(WrapperNativeInfoBase *info, UInt32 portNum);
    Int64 GetExpectedLength(WrapperNativeInfoBase *info, UInt32 portNum);    
    Int64 GetVertexId(WrapperNativeInfoBase *info);
    void SetInitialSizeHint(WrapperNativeInfoBase *info, UInt32 portNum, UInt64 hint);
    const char* GetInputChannelURI(WrapperNativeInfoBase *info, UInt32 portNum);
    const char* GetOutputChannelURI(WrapperNativeInfoBase *info, UInt32 portNum); 
    void Flush(WrapperNativeInfoBase *info, UInt32 portNum);
    void Close(WrapperNativeInfoBase *info, UInt32 portNum);

    // The compressionScheme argument allows selection of 
    // three different tranformations of the channel data
    // The values are:
    // 0 - No transform, just passthrough
    // 1 - gzip compression or decompression
    void EnableFifoInputChannel(WrapperNativeInfoBase *info, 
        Int32 compresionScheme, UInt32 channel); 
    void EnableFifoOutputChannel(WrapperNativeInfoBase *info, 
        Int32 compresionScheme, UInt32 channel); 

    DataBlockItem* ReadDataBlock(WrapperNativeInfoBase *info, 
                                 UInt32 portNum, 
                                 byte **ppDataBlock,
                                 Int32 *ppDataBlockSize,
                                 Int32 *pErrorCode);

    // The data block should be considered read only after WriteDataBlock 
    // has been called.  The data block will not be reclaimed until the client 
    // explicitly releases it. 
    BOOL WriteDataBlock(WrapperNativeInfoBase *info, 
                        UInt32 portNum, 
                        DataBlockItem *pItem,
                        Int32 numBytesToWrite); 

    DataBlockItem* AllocateDataBlock(WrapperNativeInfoBase *info, 
                                     Int32 dataBlockSize,
                                     byte **pDataBlock); 

    void ReleaseDataBlock(WrapperNativeInfoBase *info, 
                          DataBlockItem *pItem);

    bool GetDryadStreamInfo(char *streamName, UInt32 *numExtents, UInt64 *streamLength);

}
