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

#include <portmemorybuffers.h>

#pragma unmanaged

DryadFixedMemoryBuffer::DryadFixedMemoryBuffer()
{
}

DryadFixedMemoryBuffer::DryadFixedMemoryBuffer(BYTE *pData,
                                               Size_t allocatedSize,
                                               Size_t availableSize) :
    DrFixedMemoryBuffer(pData, allocatedSize, availableSize)
{
}

DryadFixedMemoryBuffer::~DryadFixedMemoryBuffer()
{
}


DryadFixedMemoryBufferCopy::
    DryadFixedMemoryBufferCopy(DryadFixedMemoryBuffer* src)
{
    Size_t copySize = src->GetAvailableSize();
    Size_t availableSize;
    BYTE* data = (BYTE *) src->GetDataAddress(0, &availableSize, NULL);
    LogAssert(data != NULL && availableSize >= copySize);

    m_dataCopy = new BYTE[src->GetAvailableSize()];
    ::memcpy(m_dataCopy, data, copySize);

    Init(m_dataCopy, copySize, copySize);
}

DryadFixedMemoryBufferCopy::~DryadFixedMemoryBufferCopy()
{
    delete [] m_dataCopy;
}

DryadLockedMemoryBuffer::DryadLockedMemoryBuffer()
{
}

DryadLockedMemoryBuffer::DryadLockedMemoryBuffer(BYTE *pData,
                                                 Size_t allocatedSize) :
    DrFixedMemoryBuffer(pData, allocatedSize, allocatedSize)
{
}

DryadLockedMemoryBuffer::~DryadLockedMemoryBuffer()
{
}

//
// Initialize memory buffer with array and size
//
void DryadLockedMemoryBuffer::Init(BYTE *pData, Size_t allocatedSize)
{
    DrFixedMemoryBuffer::Init(pData, allocatedSize, allocatedSize);
}

void DryadLockedMemoryBuffer::SetAvailableSize(Size_t uSize)
{
    LogAssert(false);
}

static CRITSEC s_readPoolCS;
static size_t s_readPoolAlignment = 0;
static size_t s_readPoolBufferSize = 0;
static const size_t s_readPoolSize = 20;
static void* s_readPool[s_readPoolSize];
static size_t s_readPoolValid = 0;

//
// Create a fixed length buffer aligned to a provided 2^N alignment
//
DryadAlignedReadBlock::DryadAlignedReadBlock(size_t size,
                                             size_t alignment)
{
    //
    // If alignment set, create buffer and set start to correct alignment, otherwise just use random address
    //
    if (alignment > 0)
    {
        void* poolData = NULL;

        {
            AutoCriticalSection acs(&s_readPoolCS);
            if (s_readPoolAlignment == 0)
            {
                s_readPoolAlignment = alignment;
            }
            if (s_readPoolBufferSize == 0)
            {
                s_readPoolBufferSize = size;
            }

            if (s_readPoolAlignment == alignment &&
                s_readPoolBufferSize == size &&
                s_readPoolValid > 0)
            {
                --s_readPoolValid;
                poolData = s_readPool[s_readPoolValid];
            }
        }

        if (poolData == NULL)
        {
            // 
            // alignment must be a power of 2 (eg 1000 & 111 = 0)
            //
            LogAssert ((alignment & (alignment-1)) == 0);

            //
            // Create buffer that's big enough to hold size even if base address has to move up by (alignment - 1)
            //
            m_data = new char[size + alignment - 1];
        }
        else
        {
            m_data = poolData;
        }

        ULONG_PTR baseAddress = (ULONG_PTR) m_data;

        //
        // Round base address up to nearest multiple of alignment (=baseaddress if already aligned)
        // eg: base = 13, alignment = 8: 1101 + 111 - ((1101 + 111) & 111) = 10100 - (10100 & 111) = 10000 = 16
        //
        ULONG_PTR alignedAddress = baseAddress + alignment - 1;
        alignedAddress -= (alignedAddress & (alignment - 1));
        LogAssert(alignedAddress + size < baseAddress + size + alignment);

        m_alignedData = (void *) alignedAddress;
    }
    else
    {
        m_data = new char[size];
        m_alignedData = m_data;
    }

    //
    // Initialize a fixed length buffer
    //
    this->Init((BYTE *) m_alignedData, size);
    m_alignment = alignment;
}

DryadAlignedReadBlock::~DryadAlignedReadBlock()
{
    bool mustDelete = true;

    if (m_alignment > 0)
    {
        AutoCriticalSection acs(&s_readPoolCS);

        if (s_readPoolAlignment == m_alignment &&
            s_readPoolBufferSize == GetAllocatedSize() &&
            s_readPoolValid < s_readPoolSize)
        {
            s_readPool[s_readPoolValid] = m_data;
            ++s_readPoolValid;
            mustDelete = false;
        }
    }

    if (mustDelete)
    {
        delete [] m_data;
    }
}

void* DryadAlignedReadBlock::GetData()
{
    return m_alignedData;
}

//
// Update max data size in this buffer to supplied value
//
void DryadAlignedReadBlock::Trim(Size_t numBytes)
{
    LogAssert(numBytes <= GetAvailableSize());
    InternalSetAvailableSize(numBytes);
}


static CRITSEC s_writePoolCS;
static size_t s_writePoolAlignment = 0;
static size_t s_writePoolBufferSize = 0;
static const size_t s_writePoolSize = 20;
static void* s_writePool[s_writePoolSize];
static size_t s_writePoolValid = 0;

DryadAlignedWriteBlock::DryadAlignedWriteBlock(size_t size,
                                               size_t alignment)
{
    if (alignment > 0)
    {
        void* poolData = NULL;

        {
            AutoCriticalSection acs(&s_writePoolCS);
            if (s_writePoolAlignment == 0)
            {
                s_writePoolAlignment = alignment;
            }
            if (s_writePoolBufferSize == 0)
            {
                s_writePoolBufferSize = size;
            }

            if (s_writePoolAlignment == alignment &&
                s_writePoolBufferSize == size &&
                s_writePoolValid > 0)
            {
                --s_writePoolValid;
                poolData = s_writePool[s_writePoolValid];
            }
        }

        if (poolData == NULL)
        {
            /* alignment must be a power of 2 */
            LogAssert ((alignment & (alignment-1)) == 0);

            m_data = new char[size + alignment - 1];
        }
        else
        {
            m_data = poolData;
        }

        ULONG_PTR baseAddress = (ULONG_PTR) m_data;
        ULONG_PTR alignedAddress = baseAddress + alignment - 1;
        alignedAddress -= (alignedAddress & (alignment - 1));
        LogAssert(alignedAddress + size < baseAddress + size + alignment);

        m_alignedData = (void *) alignedAddress;
    }
    else
    {
        m_data = new char[size];
        m_alignedData = m_data;
    }

    this->Init((BYTE *) m_alignedData, size, 0);
    m_alignment = alignment;
}

DryadAlignedWriteBlock::~DryadAlignedWriteBlock()
{
    bool mustDelete = true;

    if (m_alignment > 0)
    {
        AutoCriticalSection acs(&s_writePoolCS);

        if (s_writePoolAlignment == m_alignment &&
            s_writePoolBufferSize == GetAllocatedSize() &&
            s_writePoolValid < s_writePoolSize)
        {
            s_writePool[s_writePoolValid] = m_data;
            ++s_writePoolValid;
            mustDelete = false;
        }
    }

    if (mustDelete)
    {
        delete [] m_data;
    }
}

void* DryadAlignedWriteBlock::GetData()
{
    return m_alignedData;
}
