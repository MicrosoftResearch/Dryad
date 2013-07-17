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

#include <portmemorybuffers.h>

class RChannelBufferData;
class RChannelBufferWriter;

class ChannelDataBufferList;

class RChannelReaderBuffer : public DrMemoryBuffer
{
public:
    RChannelReaderBuffer(DryadLockedBufferList* bufferList,
                         Size_t startOffset, Size_t endOffset);
    RChannelReaderBuffer(ChannelDataBufferList* bufferList,
                         Size_t startOffset, Size_t endOffset);
    ~RChannelReaderBuffer();

    //
    // Retrieve pointer to the data stored in memory block at uOffset
    // and max size available in this block
    //
    // Returns NULL if no data at this offset, valid pointer otherwise
    //
    void *GetDataAddress(Size_t uOffset,
                         Size_t *puSize,
                         Size_t *puPriorSize);

    //
    // Preallocate enough memory buffers to fix uMaxSize bytes of data.
    //
    void IncreaseAllocatedSize(Size_t uSize);

private:
    class Current
    {
    public:
        Current(Size_t initialStartOffset,
                Size_t finalEndOffset,
                DryadLockedMemoryBuffer** bufferArray,
                size_t nBuffers,
                Size_t offset);

        void* GetDataAddress(DryadLockedMemoryBuffer** bufferArray,
                             Size_t offset,
                             Size_t *puSize,
                             Size_t *puPriorSize);

    private:
        void SetTailBufferData(DryadLockedMemoryBuffer** bufferArray,
                               size_t nBuffers,
                               Size_t finalEndOffset);

        size_t             m_currentBuffer;
        Size_t             m_currentBufferBase;
        Size_t             m_currentHeadCutLength;
        Size_t             m_currentTailOffset;
    };

    void Initialise(Size_t startOffset, Size_t endOffset);

    DryadLockedMemoryBuffer**     m_bufferArray;
    size_t                        m_nBuffers;
    Size_t                        m_initialStartOffset;
    Size_t                        m_finalEndOffset;
};

class RChannelWriterBuffer : public DrMemoryBuffer
{
public:
    RChannelWriterBuffer(RChannelBufferWriter* bufferProvider,
                         DryadFixedBufferList* bufferList);
    ~RChannelWriterBuffer();

    //
    // Retrieve pointer to the data stored in memory block at uOffset
    // and max size available in this block
    //
    // Returns NULL if no data at this offset, valid pointer otherwise
    //
    void *GetDataAddress(Size_t uOffset,
                         Size_t *puSize,
                         Size_t *puPriorSize);

    //
    // Preallocate enough memory buffers to fix uMaxSize bytes of data.
    //
    void IncreaseAllocatedSize(Size_t uSize);

protected:
    void InternalSetAvailableSize(Size_t uSize);

private:
    DryadFixedBufferList*          m_bufferList;

    Size_t                         m_currentBufferOffset;
    Size_t                         m_currentBaseOffset;
    Size_t                         m_baseBufferOffset;

    Size_t                         m_availableHighWaterMark;
    Size_t                         m_availableStartOffset;
    Size_t                         m_availableBufferOffset;
    DrBListEntry*                  m_lastAvailableBuffer;

    RChannelBufferWriter*          m_bufferProvider;
};

class ChannelMemoryBufferWriter : public DrMemoryBufferWriter
{
public:
    ChannelMemoryBufferWriter(DrMemoryBuffer* writer,
                              DryadFixedBufferList* bufferList);

    bool MarkRecordBoundary();
    Size_t GetLastRecordBoundary();

private:
    Size_t                  m_initialBoundary;
    Size_t                  m_lastRecordBoundary;
    DryadFixedBufferList*   m_bufferList;
};
