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

#include "channelmemorybuffers.h"
#include "channelreader.h"
#include "channelwriter.h"

#pragma unmanaged


RChannelReaderBuffer::Current::Current(Size_t initialStartOffset,
                                       Size_t finalEndOffset,
                                       DryadLockedMemoryBuffer**
                                       bufferArray,
                                       size_t nBuffers,
                                       Size_t offset)
{
    m_currentBuffer = 0;
    m_currentBufferBase = 0;
    m_currentHeadCutLength = initialStartOffset;
    m_currentTailOffset = 0;

    SetTailBufferData(bufferArray, nBuffers, finalEndOffset);

    while (offset >= m_currentTailOffset)
    {
        ++m_currentBuffer;
        LogAssert(m_currentBuffer < nBuffers);

        m_currentBufferBase = m_currentTailOffset;
        m_currentHeadCutLength = 0;

        SetTailBufferData(bufferArray, nBuffers, finalEndOffset);
    }
}

void* RChannelReaderBuffer::Current::
    GetDataAddress(DryadLockedMemoryBuffer** bufferArray,
                   Size_t offset,
                   Size_t *puSize,
                   Size_t *puPriorSize)
{
    LogAssert(offset >= m_currentBufferBase &&
              offset < m_currentTailOffset);

    Size_t offsetInAvailable = offset - m_currentBufferBase;
    Size_t offsetInBuffer = offsetInAvailable + m_currentHeadCutLength;
    Size_t remaining = m_currentTailOffset - offset;

    void* dataAddress =
        bufferArray[m_currentBuffer]->GetDataAddress(offsetInBuffer,
                                                     puSize,
                                                     puPriorSize);
    LogAssert(dataAddress != NULL);

    if (*puSize > remaining)
    {
        *puSize = remaining;
    }

    if (puPriorSize != NULL && *puPriorSize > offsetInAvailable)
    {
        *puPriorSize = offsetInAvailable;
    }

    return dataAddress;
}

void RChannelReaderBuffer::Current::
    SetTailBufferData(DryadLockedMemoryBuffer** bufferArray,
                      size_t nBuffers,
                      Size_t finalEndOffset)
{
    Size_t bSize = bufferArray[m_currentBuffer]->GetAvailableSize();

    if (m_currentBuffer == nBuffers-1)
    {
        LogAssert(finalEndOffset >= m_currentHeadCutLength);
        LogAssert(finalEndOffset <= bSize);

        m_currentTailOffset += (finalEndOffset - m_currentHeadCutLength);
    }
    else
    {
        m_currentTailOffset += (bSize - m_currentHeadCutLength);
    }
}


RChannelReaderBuffer::RChannelReaderBuffer(DryadLockedBufferList* bufferList,
                                           Size_t startOffset,
                                           Size_t endOffset)
{
    m_nBuffers = bufferList->CountLinks();
    LogAssert(m_nBuffers > 0);
    m_bufferArray = new DryadLockedMemoryBuffer *[m_nBuffers];

    LogAssert(m_uAllocatedSize == 0);
    size_t i;
    DrBListEntry* listEntry = bufferList->GetHead();
    for (i=0; i < m_nBuffers; ++i)
    {
        LogAssert(listEntry != NULL);
        m_bufferArray[i] = bufferList->CastOut(listEntry);
        m_bufferArray[i]->IncRef();
        m_uAllocatedSize += m_bufferArray[i]->GetAvailableSize();
        listEntry = bufferList->GetNext(listEntry);
    }
    LogAssert(listEntry == NULL);

    Initialise(startOffset, endOffset);
}

RChannelReaderBuffer::RChannelReaderBuffer(ChannelDataBufferList* bufferList,
                                           Size_t startOffset,
                                           Size_t endOffset)
{
    m_nBuffers = bufferList->CountLinks();
    LogAssert(m_nBuffers > 0);
    m_bufferArray = new DryadLockedMemoryBuffer *[m_nBuffers];

    LogAssert(m_uAllocatedSize == 0);
    size_t i;
    DrBListEntry* listEntry = bufferList->GetHead();
    for (i=0; i < m_nBuffers; ++i)
    {
        LogAssert(listEntry != NULL);
        m_bufferArray[i] = (bufferList->CastOut(listEntry))->GetData();
        m_bufferArray[i]->IncRef();
        m_uAllocatedSize += m_bufferArray[i]->GetAvailableSize();
        listEntry = bufferList->GetNext(listEntry);
    }
    LogAssert(listEntry == NULL);

    Initialise(startOffset, endOffset);
}

void RChannelReaderBuffer::Initialise(Size_t startOffset, Size_t endOffset)
{
    m_fIsGrowable = false;

    LogAssert(startOffset <= m_bufferArray[0]->GetAvailableSize());
    LogAssert(endOffset <= m_bufferArray[m_nBuffers-1]->GetAvailableSize());
    Size_t tailCutLength =
        m_bufferArray[m_nBuffers-1]->GetAvailableSize() - endOffset;
    LogAssert(m_uAllocatedSize >= (startOffset + tailCutLength));
    m_uAllocatedSize -= (startOffset + tailCutLength);

    SetAvailableSize(m_uAllocatedSize);

    m_initialStartOffset = startOffset;
    m_finalEndOffset = endOffset;
}

RChannelReaderBuffer::~RChannelReaderBuffer()
{
    size_t i;

    for (i=0; i<m_nBuffers; ++i)
    {
        m_bufferArray[i]->DecRef();
    }
    delete [] m_bufferArray;
}

void* RChannelReaderBuffer::GetDataAddress(Size_t uOffset,
                                           Size_t *puSize,
                                           Size_t *puPriorSize)
{
    if (uOffset >= m_uAllocatedSize)
    {
        return NULL;
    }

    Current current(m_initialStartOffset, m_finalEndOffset,
                    m_bufferArray, m_nBuffers, uOffset);

    void* dataAddress = current.GetDataAddress(m_bufferArray,
                                               uOffset,
                                               puSize,
                                               puPriorSize);

    return dataAddress;
}

void RChannelReaderBuffer::IncreaseAllocatedSize(Size_t uSize)
{
    LogAssert(uSize <= m_uAllocatedSize);
}


RChannelWriterBuffer::
    RChannelWriterBuffer(RChannelBufferWriter* bufferProvider,
                         DryadFixedBufferList* bufferList)
{
    m_bufferProvider = bufferProvider;
    m_bufferList = bufferList;
    if (m_bufferList->IsEmpty() == false)
    {
        LogAssert(m_bufferList->CountLinks() == 1);
        DryadFixedMemoryBuffer* currentBuffer =
            m_bufferList->CastOut(m_bufferList->GetHead());

        LogAssert(currentBuffer->GetAllocatedSize() >
                  currentBuffer->GetAvailableSize());

        m_baseBufferOffset = currentBuffer->GetAvailableSize();
        m_uAllocatedSize =
            currentBuffer->GetAllocatedSize() - m_baseBufferOffset;
    }
    else
    {
        m_baseBufferOffset = 0;
        m_uAllocatedSize = 0;
    }

    m_currentBufferOffset = 0;
    m_currentBaseOffset = m_baseBufferOffset;

    m_availableHighWaterMark = 0;
    m_availableStartOffset = m_baseBufferOffset;
    m_availableBufferOffset = 0;
    m_lastAvailableBuffer = m_bufferList->GetHead();
}

RChannelWriterBuffer::~RChannelWriterBuffer()
{
}

void* RChannelWriterBuffer::GetDataAddress(Size_t uOffset,
                                           Size_t *puSize,
                                           Size_t *puPriorSize)
{
    if (m_bufferList->IsEmpty())
    {
        return NULL;
    }
    else
    {
        LogAssert(uOffset >= m_currentBufferOffset);

        Size_t offsetInAvailable = uOffset - m_currentBufferOffset;
        Size_t offsetInBuffer = offsetInAvailable + m_currentBaseOffset;

        DryadFixedMemoryBuffer* buffer =
            m_bufferList->CastOut(m_bufferList->GetTail());
        void* dataAddress =
            buffer->GetDataAddress(offsetInBuffer, puSize, puPriorSize);

        if (puPriorSize != NULL && *puPriorSize > offsetInAvailable)
        {
            *puPriorSize = offsetInAvailable;
        }

        return dataAddress;
    }
}

void RChannelWriterBuffer::IncreaseAllocatedSize(Size_t uSize)
{
    while (m_uAllocatedSize < uSize)
    {
        DryadFixedMemoryBuffer* newBuffer =
            m_bufferProvider->GetNextWriteBuffer();
        m_bufferList->InsertAsTail(m_bufferList->CastIn(newBuffer));
        m_currentBufferOffset = m_uAllocatedSize;
        m_uAllocatedSize += newBuffer->GetAllocatedSize();
    }

    /* this is zero for all but (optionally) the first buffer in the
       list */
    m_currentBaseOffset = 0;
}

void RChannelWriterBuffer::InternalSetAvailableSize(Size_t uSize)
{
    DrMemoryBuffer::InternalSetAvailableSize(uSize);

    LogAssert(uSize >= m_availableHighWaterMark);

    if (m_lastAvailableBuffer == NULL)
    {
        LogAssert(m_availableBufferOffset == 0);
        LogAssert(m_availableHighWaterMark == 0);
        LogAssert(m_availableStartOffset == m_baseBufferOffset);

        if (m_bufferList->IsEmpty())
        {
            LogAssert(uSize == 0);
            return;
        }

        m_lastAvailableBuffer = m_bufferList->GetHead();
    }

    DrBListEntry* listEntry = m_lastAvailableBuffer;
    if (listEntry == NULL)
    {
        listEntry = m_bufferList->GetHead();
        LogAssert(listEntry != NULL);
    }

    do
    {
        DryadFixedMemoryBuffer* b = m_bufferList->CastOut(listEntry);
        listEntry = m_bufferList->GetNext(listEntry);

        if (listEntry == NULL)
        {
            LogAssert(uSize >= m_availableBufferOffset);
            Size_t thisAvailable = uSize - m_availableBufferOffset;

            LogAssert(m_availableStartOffset + thisAvailable <=
                      b->GetAllocatedSize());
            b->SetAvailableSize(m_availableStartOffset + thisAvailable);
        }
        else
        {
            b->SetAvailableSize(b->GetAllocatedSize());
            m_availableBufferOffset +=
                b->GetAllocatedSize() - m_availableStartOffset;
            LogAssert(m_availableBufferOffset < uSize);
            m_lastAvailableBuffer = listEntry;
            m_availableStartOffset = 0;
        }
    } while (listEntry != NULL);

    m_availableHighWaterMark = uSize;
}

ChannelMemoryBufferWriter::
    ChannelMemoryBufferWriter(DrMemoryBuffer* writeBuffer,
                              DryadFixedBufferList* bufferList) :
        DrMemoryBufferWriter(writeBuffer)
{
    m_bufferList = bufferList;

    if (m_bufferList->IsEmpty())
    {
        m_initialBoundary = 0;
    }
    else
    {
        LogAssert(m_bufferList->CountLinks() == 1);
        DryadFixedMemoryBuffer* buffer =
            m_bufferList->CastOut(m_bufferList->GetHead());
        m_initialBoundary = buffer->GetAvailableSize();
        LogAssert(m_initialBoundary < buffer->GetAllocatedSize());
    }

    m_lastRecordBoundary = 0;
}

bool ChannelMemoryBufferWriter::MarkRecordBoundary()
{
    if (m_bufferList->IsEmpty())
    {
        LogAssert(m_initialBoundary == 0);
        LogAssert(m_lastRecordBoundary == 0);
        return false;
    }
    else if (m_bufferList->CountLinks() == 1)
    {
        DryadFixedMemoryBuffer* buffer =
            m_bufferList->CastOut(m_bufferList->GetHead());
        Size_t offset = GetBufferOffset();
        Size_t allocated = buffer->GetAllocatedSize();

        if (m_initialBoundary + offset >= allocated)
        {
            DrError errTmp = FlushMemoryWriter();
            // sammck: should this assert here or return an error
            LogAssert(errTmp == DrError_OK);
        }

        Size_t boundary = buffer->GetAvailableSize();
        if (boundary == allocated)
        {
            return true;
        }
        else
        {
            m_lastRecordBoundary = offset;
            LogAssert(m_initialBoundary + m_lastRecordBoundary < allocated);
            return false;
        }
    }
    else
    {
        return true;
    }
}

Size_t ChannelMemoryBufferWriter::GetLastRecordBoundary()
{
    return m_initialBoundary + m_lastRecordBoundary;
}
    
