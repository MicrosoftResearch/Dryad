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

class DrMemoryBuffer: public DrRefCounter
{
private:
    Size_t m_uAvailableSize;
    
protected:
    Size_t m_uAllocatedSize;
    bool m_fIsGrowable;

    // these two are anded together to determine writability    
    bool m_fIsWritable;
    bool m_fIsUserWritable;

    void Init()
    {
        m_uAllocatedSize   = 0;
        m_uAvailableSize   = 0;
        m_fIsGrowable = true;
        m_fIsWritable = true;
        m_fIsUserWritable = true;
    };

    DrMemoryBuffer()
    {
        Init();
    };

    virtual ~DrMemoryBuffer()
    {
    };

    virtual void InternalSetAvailableSize(Size_t uSize)
    {
        m_uAvailableSize = uSize;
    }


/* --------------------- Functions that the actual implementation provides --------------------- */

public:

    //
    // Retrieve pointer to the data stored in memory block at uOffset and max size available in this block
    //
    // Returns NULL if no data at this offset, valid pointer otherwise
    //
    virtual void *GetDataAddress(
        Size_t uOffset,            // starting offset
        Size_t *puSize,             // number of bytes available (0 in case of failure)
        Size_t *puPriorSize         // optional; size of contigious memory area prior to (*GetDataAddress())
    ) = 0;

    //
    // Preallocate enough memory buffers to fix uMaxSize bytes of data.
    //
    // If the buffer is not growable, it is a fatal error to attempt to grow the allocated size
    // beyond the current value.
    virtual void IncreaseAllocatedSize(
        Size_t uSize              // preallocate memory blocks to fit at least uSize bytes of data
    ) = 0;

    //
    // Updates the available size. Grows the allocated size if necessary.
    //
    // The default implementation just does the required updating, then updates m_uAvailableSize
    //
    virtual void SetAvailableSize(Size_t uSize)
    {
        if (uSize > m_uAllocatedSize) {
            IncreaseAllocatedSize(uSize);
            LogAssert(uSize <= m_uAllocatedSize);
        }
        InternalSetAvailableSize(uSize);
    }


/* ------------------------------- The rest are predefined ----------------------------------------- */


public:

    bool IsWritable()
    {
        return m_fIsWritable && m_fIsUserWritable;
    }

    bool IsGrowable()
    {
        return m_fIsGrowable;
    }

    //
    // Get max size of data that may be stored in the buffer
    //
    Size_t GetAllocatedSize()
    {
        return m_uAllocatedSize;
    };

    //
    // Get amount of data already stored in the buffer
    //
    Size_t GetAvailableSize()
    {
        return m_uAvailableSize;
    };

    //
    // Get address and size of available memory chunk allocating more memory if necessary
    //
    // Note that this may return memory beyond the current available size; it is
    // the caller's responsibility to SetAvailableSize() if necessary.
    //
    // Note also that the returned *puSize may be greater than uDataSize
    //
    // If the buffer is not growable, it is a fatal error to ask for data beyond the allocated size.
    void *GetWriteAddress(
        Size_t uOffset,    // offset at which to return a write pointer
        Size_t uDataSize, // minimum number of bytes to ensure is available (not necessarily contiguous) starting at the specified offset
        /* out */ Size_t *puSize,                                // Number of contiguous bytes starting at the returned pointer
        /* out */ Size_t *puPreceedingSize = NULL   // Number of contiguous bytes that preceed the returned pointer
    );

    //
    // This is the same as GetWriteAddress, except it is not a fatal error if the buffer is not growable grown to accomodate; in
    // this case, NULL is returned.
    void *GetWriteAddressIfPossible(
        Size_t uOffset,    // offset at which to return a write pointer
        Size_t uDataSize, // minimum number of bytes to ensure is available (not necessarily contiguous) starting at the specified offset
        /* out */ Size_t *puSize,                                // Number of contiguous bytes starting at the returned pointer
        /* out */ Size_t *puPreceedingSize = NULL   // Number of contiguous bytes that preceed the returned pointer
        )
    {
        Size_t uEnd = uOffset + uDataSize;
        if (!IsGrowable() && uEnd > m_uAllocatedSize) {
            *puSize = 0;
            if (puPreceedingSize != NULL) {
                *puPreceedingSize = 0;
            }
            return NULL;
        }
        return GetWriteAddress(uOffset, uDataSize, puSize, puPreceedingSize);
    }

    //
    // Copy uDataSize bytes from pData array into the buffer at the
    // specified offset.  Grows the available data size if necessary to include the written data.
    //
    void Write(
        Size_t uOffset,         // starting offset
        const void *pData,    // data buffer
        Size_t uDataSize      // number of bytes to copy
    );

    void Append(
        const void *pData,
        Size_t uDataSize
    )
    {
        Write(GetAvailableSize(), pData, uDataSize);
    }

    //
    // Zero uDataSize bytes in the buffer at the
    // specified offset.  Grows the available data size if necessary to include the zeroed data.
    //
    void Zero(
        Size_t uOffset,            // starting offset
        Size_t uDataSize           // number of bytes to set to 0
    );

    //
    // Get the address and size of contiguous available readable memory area at offset uOffset
    // It is a fatal error to request readable memory at or beyond the current available buffer size.
    //
    const void *GetReadAddress(
        Size_t uOffset,
        /* out */ Size_t *puSize,                   // Number of contiguous available bytes beginning at uOffset
        /* out */ Size_t *puPreceedingSize = NULL   // Number of contiguous readable bytes that preceed the returned pointer
    );

    //
    // Read uDataSize bytes into pData into the buffer starting at uOffset.
    //
    // It is a fatal error to attempt to read beyond the available size of the buffer
    //
    void Read(
        Size_t uOffset,
        void  *pData,
        Size_t uDataSize
    );

    //
    // Compares uDataSize bytes from pData with buffer contents starting at uOffset.
    //
    // Return 0 on match, < 0 if contents of the buffer is less than contents of pData, > 0 otherwise
    //
    // It is a fatal error to attempt to read beyond the available size of the buffer
    //
    int Compare(
        Size_t      uOffset,
        const void *pData,
        Size_t      uDataSize
    );

    //
    // Copy data from one buffer to another
    //
    void CopyBuffer(
        Size_t uDstOffset,     // starting offset
        DrMemoryBuffer *pSrcBuffer,     // source buffer
        Size_t uSrcOffset,     // starting offset in source buffer
        Size_t uDataSize       // number of bytes to copy
    );
    
};


// A simple implementation of DrMemoryBuffer that is built on a single malloc'd block of memory
class DrSimpleHeapBuffer : public DrMemoryBuffer
{
private:
    BYTE *m_pData;

public:
    DrSimpleHeapBuffer();
    DrSimpleHeapBuffer(Size_t allocedSize); // pregrows allocated size
    virtual ~DrSimpleHeapBuffer();

    // Returns the underlying heap object (if any). This buffer remains
    // the owner of the heap object.
    //
    // returns NULL if there is no underlying heap object (allocedSize = 0)
    //
    void *GetHeapItem()
    {
        return m_pData;
    }

    // Detaches the underlying heap object (if any) and returns it to the caller, who
    // must call free() on the memory when done with it.
    //
    // returns NULL if there is no underlying heap object (allocedSize = 0)
    //
    // After this call, the buffer is a new buffer with no data in it.
    //
    void *DetachHeapItem();

    // Attaches an external heap item to the buffer. Any previous heap item is
    // freed. The buffer becomes the owner of the heap item.
    //
    void AttachHeapItem(void *pHeapItem, Size_t allocedSize, Size_t dataSize);

    // DrMemoryBuffer implementation:
    
public:

    //
    // Retrieve pointer to the data stored in memory block at uOffset and max size available in this block
    //
    // Returns NULL if no data at this offset, valid pointer otherwise
    //
    virtual void *GetDataAddress(
        Size_t uOffset,            // starting offset
        Size_t *puSize,             // number of bytes available (0 in case of failure)
        Size_t *puPriorSize         // optional; size of contigious memory area prior to (*GetDataAddress())
    );

    //
    // Preallocate enough memory buffers to fix uMaxSize bytes of data.
    //
    virtual void IncreaseAllocatedSize(
        Size_t uSize              // preallocate memory blocks to fit at least uSize bytes of data
    );
};

// A buffer that wraps a fixed contiguous block of memory. 
//
// The buffer allocated size is not growable. No memory is freed when the buffer is destroyed
//
class DrFixedMemoryBuffer : public DrMemoryBuffer
{
private:
    const BYTE *m_pData;

public:
    DrFixedMemoryBuffer()
    {
        m_pData = NULL;
        m_fIsGrowable = false;
        m_fIsWritable = false;
    }

    virtual ~DrFixedMemoryBuffer()
    {
    }
    
    //
    // Initialize fixed length buffer with byte array and sizes
    //
    void Init(const BYTE *pData, Size_t allocatedSize, Size_t availableSize = 0)
    {
        LogAssert(availableSize <= allocatedSize);
        m_pData = pData;
        m_uAllocatedSize = allocatedSize;
        InternalSetAvailableSize(availableSize);
        m_fIsWritable = true;
    }

    //
    // Create new fixed length buffer with byte array and sizes
    //
    DrFixedMemoryBuffer(const BYTE *pData, Size_t allocatedSize, Size_t availableSize = 0)
    {
        Init(pData, allocatedSize, availableSize);
    }

    // DrMemoryBuffer implementation:
    
public:

    //
    // Retrieve pointer to the data stored in memory block at uOffset and max size available in this block
    //
    // Returns NULL if no data at this offset, valid pointer otherwise
    //
    virtual void *GetDataAddress(
        Size_t uOffset,            // starting offset
        Size_t *puSize,             // number of bytes available (0 in case of failure)
        Size_t *puPriorSize         // optional; size of contigious memory area prior to (*GetDataAddress())
    );

    //
    // Preallocate enough memory buffers to fix uMaxSize bytes of data.
    //
    virtual void IncreaseAllocatedSize(
        Size_t uSize              // preallocate memory blocks to fit at least uSize bytes of data
    );
};

