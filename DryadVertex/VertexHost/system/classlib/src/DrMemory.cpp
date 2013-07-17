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

#include "DrCommon.h"

#pragma unmanaged

/*

   Implementation of core cosmos memory/buffer management

*/

//
// Get address and size of available memory chunk allocating more memory if necessary
//
// Note that this may return memory beyond the current available size; it is
// the caller's responsibility to SetAvailableSize() if necessary.
//
// Note also that the returned *puSize may be greater than uDataSize
//
void *DrMemoryBuffer::GetWriteAddress(
    Size_t uOffset,    // offset at which to return a write pointer
    Size_t uDataSize, // minimum number of bytes to ensure is available (not necessarily contiguous) starting at the specified offset
    /* out */ Size_t *puSize,                                // Number of contiguous bytes starting at the returned pointer
    /* out */ Size_t *puPreceedingSize   // Number of contiguous bytes that preceed the returned pointer
)
{
    LogAssert(IsWritable());
    
    Size_t uEnd = uOffset + uDataSize;
    LogAssert(uEnd >= uOffset);
    if (uEnd > m_uAllocatedSize) {
        IncreaseAllocatedSize(uEnd);
        LogAssert(uEnd <= m_uAllocatedSize);
    }

    void  *pAddress;
    Size_t uSize;
    Size_t uPrec;

    pAddress = GetDataAddress(uOffset, &uSize, &uPrec);
    LogAssert(pAddress != NULL);

    *puSize = uSize;
    if (puPreceedingSize != NULL) {
        *puPreceedingSize = uPrec;
    }
    
    return pAddress;
};

//
// Copy uDataSize bytes from pData array into the buffer at the
// specified offset.  Grows the available data size if necessary to include the written data.
//
void DrMemoryBuffer::Write(
    Size_t uOffset,         // starting offset
    const void *pData,    // data buffer
    Size_t uDataSize      // number of bytes to copy
)
{
    const BYTE *pSrc = (const BYTE *)pData;
    BYTE *pDst;
    Size_t uSize;

    while (uDataSize != 0)  {
        pDst = (BYTE *)GetWriteAddress(uOffset, uDataSize, &uSize);
        if (uSize > uDataSize) {
            uSize = uDataSize;
        }
        memcpy(pDst, pSrc, uSize);
        pSrc += uSize;
        uOffset += uSize;
        uDataSize -= uSize;
    }
    
    if (uOffset > GetAvailableSize()) {
        SetAvailableSize(uOffset);
    }
};

//
// Zero uDataSize bytes in the buffer at the
// specified offset.  Grows the available data size if necessary to include the zeroed data.
//
void DrMemoryBuffer::Zero(
    Size_t uOffset,            // starting offset
    Size_t uDataSize           // number of bytes to set to 0
)
{
    BYTE *pDst;
    Size_t uSize;

    while (uDataSize != 0) {
        pDst = (BYTE *)GetWriteAddress(uOffset, uDataSize, &uSize);
        if (uSize > uDataSize) {
            uSize = uDataSize;
        }
        memset(pDst, 0, uSize);
        uOffset   += uSize;
        uDataSize -= uSize;
    }

    if (uOffset > GetAvailableSize()) {
        SetAvailableSize(uOffset);
    }
};

//
// Get the address and size of contiguous available readable memory area at offset uOffset
//
const void *DrMemoryBuffer::GetReadAddress(
    Size_t uOffset,
    /* out */ Size_t *puSize,                   // Number of contiguous available bytes beginning at uOffset
    /* out */ Size_t *puPreceedingSize   // Number of contiguous readable bytes that preceed the returned pointer
)
{
    const void *pData;
    Size_t uSize;
    Size_t uPrec;

    LogAssert(uOffset < GetAvailableSize());

    pData = GetDataAddress(uOffset, &uSize, &uPrec);
    if (uSize + uOffset > GetAvailableSize())
        uSize = GetAvailableSize() - uOffset;

    *puSize = uSize;
    if (puPreceedingSize != NULL) {
        *puPreceedingSize = uPrec;
    }

    return pData;
};

//
// Read uDataSize bytes into pData into the buffer starting at uOffset.
//
// It is a fatal error to attempt to read beyond the available size of the buffer
//
void DrMemoryBuffer::Read(
    Size_t uOffset,
    void  *pData,
    Size_t uDataSize
)
{
    BYTE *pDst = (BYTE *)pData;

    while (uDataSize != 0) {
        Size_t uSize;
        const BYTE *pSrc = (const BYTE *)GetReadAddress(uOffset, &uSize);
        if (uSize > uDataSize)
            uSize = uDataSize;
        memcpy(pDst, pSrc, uSize);
        pDst += uSize;
        uDataSize -= uSize;
        uOffset += uSize;
    }
};

//
// Compares uDataSize bytes from pData with buffer contents starting at uOffset.
//
// Return 0 on match, < 0 if contents of the buffer is less than contents of pData, > 0 otherwise
//
// It is a fatal error to attempt to read beyond the available size of the buffer
//
int DrMemoryBuffer::Compare(
    Size_t      uOffset,
    const void *pData,
    Size_t      uDataSize
)
{
    int iResult;
    const BYTE *pDst = (const BYTE *)pData;

    iResult = 0;

    while (uDataSize != 0) {
        Size_t uSize;
        const BYTE *pSrc = (const BYTE *)GetReadAddress(uOffset, &uSize);
        if (uSize > uDataSize)
            uSize = uDataSize;
        iResult = memcmp(pSrc, pDst, uSize);
        if (iResult != 0)
            break;
        pDst += uSize;
        uDataSize -= uSize;
        uOffset += uSize;
    }

    return (iResult);
};

//
// Copy data from one buffer to another
//
void DrMemoryBuffer::CopyBuffer(
    Size_t uDstOffset,     // starting offset
    DrMemoryBuffer *pSrcBuffer,     // source buffer
    Size_t uSrcOffset,     // starting offset in source buffer
    Size_t uDataSize       // number of bytes to copy
)
{
    if (uDataSize != 0) {
        IncreaseAllocatedSize(uDstOffset + uDataSize);
        while (uDataSize != 0) {
            Size_t  uSize;
            const BYTE *pSrc = (const BYTE *)pSrcBuffer->GetReadAddress(uSrcOffset, &uSize);
            if (uSize > uDataSize)
                uSize = uDataSize;
            Write(uDstOffset, pSrc, uSize);
            uDataSize -= uSize;
            uSrcOffset += uSize;
            uDstOffset += uSize;
        }
    }
}

DrSimpleHeapBuffer::DrSimpleHeapBuffer()
{
    m_pData = NULL;
}

DrSimpleHeapBuffer::DrSimpleHeapBuffer(Size_t uSize)
{
    m_pData = NULL;
    if (uSize != 0) {
        m_pData = (BYTE *)malloc(uSize);
        LogAssert(m_pData != NULL);
        m_uAllocatedSize = uSize;
    } else {
        m_pData = NULL;
    }
}

DrSimpleHeapBuffer::~DrSimpleHeapBuffer()
{
    if (m_pData != NULL) {
        free(m_pData);
    }
}

// Detaches the underlying heap object (if any) and returns it to the caller, who
// must call free() on the memory when done with it.
//
// returns NULL if there is no underlying heap object (allocedSize = 0)
//
// After this call, the buffer is a new buffer with no data in it.
//
void *DrSimpleHeapBuffer::DetachHeapItem()
{
    void *pRet = m_pData;
    m_pData = NULL;
    m_uAllocatedSize = 0;
    SetAvailableSize(0);
    return pRet;
}

// Attaches an external heap item to the buffer. Any previous heap item is
// freed. The buffer becomes the owner of the heap item.
//
void DrSimpleHeapBuffer::AttachHeapItem(void *pHeapItem, Size_t allocedSize, Size_t dataSize)
{
    LogAssert(allocedSize >= dataSize);
    LogAssert(allocedSize == 0 || pHeapItem != NULL);
    if (m_pData != NULL) {
        free(m_pData);
    }
    m_pData = (BYTE *)pHeapItem;
    m_uAllocatedSize = allocedSize;
    SetAvailableSize(dataSize);
}

//
// Retrieve pointer to the data stored in memory block at uOffset and max size available in this block
//
// Returns NULL if no data at this offset, valid pointer otherwise
//
void *DrSimpleHeapBuffer::GetDataAddress(
    Size_t uOffset,            // starting offset
    Size_t *puSize,             // number of bytes available (0 in case of failure)
    Size_t *puPriorSize         // optional; size of contigious memory area prior to (*GetDataAddress())
)
{
    BYTE *pRet;
    Size_t prec;

    if (uOffset >= m_uAllocatedSize) {
        pRet = NULL;
        *puSize = 0;
        prec = 0;
    } else {
        pRet = m_pData + uOffset;
        *puSize = m_uAllocatedSize - uOffset;
        prec = uOffset;
    }

    if (puPriorSize != NULL) {
        *puPriorSize = prec;
    }
    
    return (void *)pRet;
}

//
// Preallocate enough memory buffers to fix uMaxSize bytes of data.
//
void DrSimpleHeapBuffer::IncreaseAllocatedSize(
    Size_t uSize              // preallocate memory blocks to fit at least uSize bytes of data
)
{
    if (uSize > m_uAllocatedSize) {
        if (uSize < 32) {
            uSize = 32;
        }
        if (uSize < 2 * m_uAllocatedSize) {
            uSize = 2 * m_uAllocatedSize;
        }
        if (m_pData == NULL) {
            m_pData = (BYTE *)malloc(uSize);
            LogAssert(m_pData != NULL);
        } else {
            BYTE *pNew = (BYTE *)realloc(m_pData, uSize);
            LogAssert(pNew != NULL);
            m_pData = pNew;
        }
        m_uAllocatedSize = uSize;
    }
}

//
// Retrieve pointer to the data stored in memory block at uOffset and max size available in this block
//
// Returns NULL if no data at this offset, valid pointer otherwise
//
void *DrFixedMemoryBuffer::GetDataAddress(
    Size_t uOffset,            // starting offset
    Size_t *puSize,             // number of bytes available (0 in case of failure)
    Size_t *puPriorSize         // optional; size of contigious memory area prior to (*GetDataAddress())
)
{
    void *pRet;
    Size_t uPrior;
    if (uOffset >= m_uAllocatedSize) {
        *puSize = 0;
        uPrior = 0;
        pRet = NULL;
    } else {
        *puSize = m_uAllocatedSize - uOffset;
        uPrior = uOffset;
        pRet = (void *)(m_pData + uOffset);
    }
    if (puPriorSize != NULL) {
        *puPriorSize = uPrior;
    }
    return pRet;
}

//
// Preallocate enough memory buffers to fix uMaxSize bytes of data.
//
void DrFixedMemoryBuffer::IncreaseAllocatedSize(
    Size_t uSize              // preallocate memory blocks to fit at least uSize bytes of data
)
{
    LogAssert(uSize <= m_uAllocatedSize);
}
