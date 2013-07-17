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

#include <stdio.h>
#include <stdlib.h>
#include <share.h>

#pragma unmanaged

void DrMemoryStream::DiscardMemoryStreamContext()
{
    pBlockBase = NULL;
    blockLength = 0;
    pData = NULL;
    uBlockBasePhysicalStreamPosition = 0;
}

DrMemoryStream::DrMemoryStream()
{
    status = DrError_OK;
    pBlockBase = NULL;
    blockLength = 0;
    pData = NULL;
    uBlockBasePhysicalStreamPosition = 0;
}

DrMemoryStream::~DrMemoryStream()
{
    DiscardMemoryStreamContext();
    status = DrError_Impossible;
}

__declspec(deprecated) DrError DrMemoryStream::Close()
{
    DiscardMemoryStreamContext();
    return status;
}

DrMemoryWriter::DrMemoryWriter()
{
    m_pendingBufferredWriteOldAvailableSize = 0;
    m_fMemoryWriterIsClosed = false;
    m_fIgnoreMemoryWriterCloseFailureInDestructor = false;
}

void DrMemoryWriter::InternalFree()
{
    m_pPendingBufferedWriteBuffer = NULL;
    m_pendingBufferredWriteOldAvailableSize = 0;
}

// Clears all buffer context values to initial defaults (including settting the physical stream position to 0
// and discarding abandandoned temporary buffered write buffers), but does not change the current status code
// or "closed" status.
// may be overridden by a subclass if it needs to free resources controlled by the
// context pointers before delegating to this method implementation.
void DrMemoryWriter::DiscardMemoryWriterContext()
{
    DrMemoryStream::DiscardMemoryStreamContext();
    InternalFree();
}

DrError DrMemoryWriter::CloseMemoryWriter()
{
    // Note, if we got here, we were chained to by any subclass implementation of CloseMemoryWriter.

    // We only allow CloseMemoryWriter to be called once    
    if (!m_fMemoryWriterIsClosed)
    {
        // Before we close, we call FlushMemoryWriter.
        // We do not bother flushing if we are in an error condition.
        // This is a virtual method which can be implemented by a subclass.
        if (status == DrError_OK)
        {
            SetStatus(FlushMemoryWriter());
        }

        // Release any abandoned temporary buffered write buffers, and clear the current contiguous block context.
        // The current status remains unchanged.
        // We do this regardless of status because it frees unneeded resources even on failure.
        DiscardMemoryWriterContext();
        
        // We set the closed flag regardless of the status. CloseMemoryWriter can only be called once. 
        m_fMemoryWriterIsClosed = true;
    }

    // At this point the only meaningful state we maintain is the status and the closed flag.
    return status;
}

void DrMemoryWriter::MemoryWriterDestructorClose()
{
    if (!m_fMemoryWriterIsClosed)
    {
        // If status is already failing, then it is OK to fail close at destructor time, since no new error
        // is occurring, so flushing is unnecessary.
        bool fIgnoreFailures = (m_fIgnoreMemoryWriterCloseFailureInDestructor || status != DrError_OK);

        // We call CloseMemoryWriter even if status is not DrError_OK because implementations
        // use CloseMemoryWriter to free resources even in an error state.
        // NOTE that this call goes through the virtual memory chain, executing on the most-derived class first.
        SetStatus(CloseMemoryWriter());

        // if m_fMemoryWriterIsClosed is not set, it means that someone forgot to chain to their parent class's
        // CloseMemoryWriter()
        LogAssert(m_fMemoryWriterIsClosed);

        // If a new error occured on close and we are not ignoring errors, then it is a fatal error.
        LogAssert(fIgnoreFailures || status == DrError_OK);
    }
}

DrMemoryWriter::~DrMemoryWriter()
{
    //=======================================================================================================
    // NOTE: each subclass should duplicate this code if it implements CloseMemoryWriter or FlushMemoryWriter
    //
    if (!MemoryWriterIsClosed())
    {
        // Close the memory writer. We cannot tolerate new failures in CloseMemoryWriter at this point because we are in a destructor!
        // This will call FlushMemoryWriter() before closing, but because of virtual destructor unwinding, it will call the
        // current class's implementation rather than the subclass's overriding implementation. So each subclass needs to implement this call in their
        // virtual destructor if they implement CloseMemoryWriter or FlushMemoryWriter
        MemoryWriterDestructorClose();
    }
    //
    // NOTE: End dublicated destructor code
    //=======================================================================================================

    InternalFree();
}

// Write a narrow (char[]) string "pstr" as a property, with an explicit provided string length. pstr may be NULL, which is
//    properly encoded as a distinct value from a zero-length string. Typically, a non-NULL pstr points to a
//    UTF-8 string (not-necessarily '\0'-terminated) that is "length" bytes long, without any embedded '\0' bytes;
//    however, this function may be used to encode arbitrary byte blocks, including blocks that contain embedded '\0' bytes,
//    so it can be used, e.g., to encode a concatenated list of '\0'-terminated strings.
// The primary distinctions between this function and WriteBlob are:
//    a) NULL is a distinct value from a zero-length string.
//    b) The property length is 0 for NULL values, and Llength"+1 for non-NULL values.
//    b) For non-NULL values, a '\0' is appended to the string bytes as the last byte of the property data. This does not change
//       the value of the string, but allows a reader to use the serialized data as a null terminated string without copying it, and
//       allows a reader to distinguish between a NULL string and an empty string.
//
// The provided length does not include the terminating '\0' byte; however, a non-NULL pstr
//    is always '\0'-terminated in the output stream, and a NULL pstr is wriiten as an empty property.
//    The actual length of the property data will be "length" + 1 if pstr is not NULL.
//
// These semantics are consistent with round-trip encoding of a DrStr value.
//
// This vertion only works with LONGATOM properties. This is asserted, to help detect bugs where strings that may
// occasionally exceed 254 bytes are accidently tagged as SHORTATOM. If you have a string property that you know
// *must* always be less than 255 bytes long, you can use WriteShortStringPropertyWithLength.
//
// Causes an assertion failure if:
//     a) enumId is a SHORTATOM
//     b) length >= _UI32_MAX
//     c) pstr == NULL && length != 0
DrError DrMemoryWriter::WriteLongStringPropertyWithLength(UInt16 enumId, const char *pstr, Size_t length)
{
    if (pstr == NULL)
    {
        LogAssert(length == 0);
        WriteEmptyLongProperty(enumId);
    }
    else
    {
        LogAssert(length < (Size_t)_UI32_MAX);
        if (WritePropertyTagLong(enumId, length + 1) == DrError_OK)
        {
            WriteBytes(pstr, length);
            WriteChar('\0');
        }
        
    }
    return status;
}

// This version writes a SHORTATOM string value. 
// 
// This vertion only works with SHORTATOM properties. If you have a string property that you know
// *must* always be less than 255 bytes long, you can use a SHORTATOM property ID and this method.
// Otherwise, you should use WriteLongStringPropertyWithLength().
//
// Causes an assertion failure if:
//     a) enumId is a LONGATOM
//     b) length >= 255
//     c) pstr == NULL && length != 0
DrError DrMemoryWriter::WriteShortStringPropertyWithLength(UInt16 enumId, const char *pstr, Size_t length)
{
    if (pstr == NULL)
    {
        LogAssert(length == 0);
        WriteEmptyShortProperty(enumId);
    }
    else
    {
        LogAssert(length < (Size_t)_UI8_MAX);
        if (WritePropertyTagShort(enumId, length + 1) == DrError_OK)
        {
            WriteBytes(pstr, length);
            WriteChar('\0');
        }
        
    }
    return status;
}

// This method should be overridden by memory writers that know how to allocate a new block and keep writing.
// The implementation should update pData, pBlockBase, blockLength, and uBlockBasePhysicalStreamPosition to point to the new block.
// The current block will be completely filled before calling this method, since there is no way to back up.
// After this call, the old block can be disposed of in any way the underlying implementation chooses (e.g., flushing).
// Returns DrError_EndOfStream if a new block can't or shouldn't be allocated.
// The default implementation always returns DrError_EndOfStream, which is appropriate for single-block writers.
// If an error is returned, status has been set.
DrError DrMemoryWriter::AdvanceToNextBlock()
{
    return SetStatus(DrError_EndOfStream);
}

// This method checks whether an attempt to write beyond the end of the current block
// will succeed. The implementation should return true if the buffer is indefinitely growable.
// The default implementation returns false, which is appropriate for single-block writers.
// NOt that this method does not set or check status
bool DrMemoryWriter::FutureBlocksCanBeWritten(size_t length)
{
    (void)length;
    return false;
}

// This method writes bytes into the buffer, handling the case where the data
// will cross blocks.
DrError DrMemoryWriter::CrossBlockWriteBytes(const BYTE *pBytes, size_t length)
{
    if (EnsureCanBeWritten(length) != DrError_OK) {
        return status;
    }

    while (length > (size_t)0) {
        if (NumContiguousBytesRemaining() == (size_t)0) {
            if (AdvanceToNextBlock() != DrError_OK) {
                return status;
            }
        }
        size_t nr = NumContiguousBytesRemaining();
        LogAssert(nr > 0);
        size_t nb = (length > nr) ? nr : length;
        memcpy(pData, pBytes, nb);
        pData += nb;
        pBytes += nb;
        length -= nb;
    }
    return DrError_OK;
}

DrError DrMemoryWriter::WriteBytesFromReader(DrMemoryReader *pReader, Size_t length)
{
    SetStatus(pReader->ReadBytesIntoWriter(this, length));
    return status;
}

DrError DrMemoryWriter::WriteBytesFromBuffer(DrMemoryBuffer *pBuffer, bool fAllowCopyBufferByReference)
{
    if (status == DrError_OK) {
        Size_t nb = pBuffer->GetAvailableSize();
        if (nb != 0) {
            DrMemoryBufferReader reader(pBuffer, fAllowCopyBufferByReference);
            WriteBytesFromReader(&reader, nb);
        }
    }
    return status;
}


// DrMemoryReader

DrMemoryReader::DrMemoryReader()
{
    pFirstTempMemHeader = NULL;
    pFirstPeekMemHeader = NULL;
    pLastPeekMemHeader = NULL;
    fAllowCopyBufferByReference = false;
    m_fMemoryReaderIsClosed = false;
}

void DrMemoryReader::InternalFree()
{
    DiscardTemporaryResults();
    DiscardPeekBlocks();
}

// Clears all buffer context values to initial defaults (including settting the physical stream position to 0
// and discarding peekahead buffers), but does not discard temporary results and does not change the current status code
// or "closed" status.
// may be overridden by a subclass if it needs to free resources controlled by the
// context pointers before delegating to this method implementation.
void DrMemoryReader::DiscardMemoryReaderContext()
{
    DrMemoryStream::DiscardMemoryStreamContext();
    DiscardPeekBlocks();
}

// This method clears all buffer context values to initial defaults (including settting the physical stream position to 0,
// discarding peekahead buffers and temporary results, clearing the "closed" state, and setting the status to DrError_OK.
// a subclass may override this to reset its own state along with forwarding the request to this class.
// Calls through the virtual DiscardMemoryReaderContext() before clearing the status and the closed flag.
void DrMemoryReader::ResetMemoryReader()
{
    DiscardMemoryReaderContext();
    DiscardTemporaryResults();
    status = DrError_OK;
    m_fMemoryReaderIsClosed = false;
}

DrError DrMemoryReader::CloseMemoryReader()
{
    // Note, if we got here, we were chained to by any subclass implementation of CloseMemoryReader.

    // We only allow CloseMemoryReader to be called once    
    if (!m_fMemoryReaderIsClosed)
    {
        // Release any abandoned temporary buffered write buffers, and clear the current contiguous block context.
        // The current status remains unchanged.
        // We do this regardless of status because it frees unneeded resources even on failure.
        DiscardMemoryReaderContext();
        DiscardTemporaryResults();
        
        // We set the closed flag regardless of the status. CloseMemoryReader can only be called once. 
        m_fMemoryReaderIsClosed = true;
    }

    // At this point the only meaningful state we maintain is the status and the closed flag.
    return status;
}

void DrMemoryReader::MemoryReaderDestructorClose()
{
    if (!m_fMemoryReaderIsClosed)
    {
        bool fAlreadyFailing = (status != DrError_OK);

        // We call CloseMemoryReader even if status is not DrError_OK because implementations
        // use CloseMemoryReader to free resources even in an error state.
        // NOTE that this call goes through the virtual memory chain, executing on the most-derived class first.
        SetStatus(CloseMemoryReader());

        // if m_fMemoryWriterIsClosed is not set, it means that someone forgot to chain to their parent class's
        // CloseMemoryWriter()
        LogAssert(m_fMemoryReaderIsClosed);

        if (!fAlreadyFailing && (status != DrError_OK))
        {
            // CloseMemoryReader introduced a new failure. But we are in a destructor, so we can't return an error code.
            // Close errors on reader streams are generally hamless, so we will ignore the error.
            ;
        }
    }
}

void DrMemoryReader::DiscardTemporaryResults()
{
    while (pFirstTempMemHeader != NULL) {
        TempMemHeader *p = pFirstTempMemHeader;
        pFirstTempMemHeader = p->Detach();
        delete p;
    }
}

void DrMemoryReader::DiscardPeekBlocks()
{
    while (pFirstPeekMemHeader != NULL) {
        PeekMemHeader *p = pFirstPeekMemHeader;
        pFirstPeekMemHeader = p->Detach();
        delete p;
    }
    pLastPeekMemHeader = NULL;
}

// The destructor for DrMemoryReader frees all the temporary
// return values allocated since the reader was created.
DrMemoryReader::~DrMemoryReader()
{
    //=======================================================================================================
    // NOTE: each subclass should duplicate this code if it implements CloseMemoryReader and needs to have it 
    // called at destruct time
    //
    if (!MemoryReaderIsClosed())
    {
        // Close the memory reader. This will call CloseMemoryReader() before closing, but because of virtual destructor unwinding, it will call the
        // current class's implementation rather than the subclass's overriding implementation. So each subclass needs to implement this call in their
        // virtual destructor if they implement CloseMemoryReader.
        MemoryReaderDestructorClose();
    }
    //
    // NOTE: End dublicated destructor code
    //=======================================================================================================

    InternalFree();
}


// ReadNextPropertyTag. Reads the next property from the bag, along
// with its length (either 1- or 4-byte depending on the length
// bit in the property name). Returns DrError_EndOfStream if there is not enough
// data in the bag to read out the property name and length, but
// does not check that there are *pDataLen more bytes remaining.
DrError DrMemoryReader::ReadNextPropertyTag(
    /* out */ UInt16 *pEnumId,
    /* out */ UInt32 *pDataLen)
{
    if (ReadUInt16(pEnumId) != DrError_OK) {
        return status;
    }

    if (((*pEnumId) & PropLengthMask) == PropLength_Short) {
        UInt8 lengthByte;
        if (ReadUInt8(&lengthByte) == DrError_OK) {
            *pDataLen = lengthByte;
        }
    } else {
        ReadUInt32(pDataLen);
    }

    return status;
}

// PeekNextPropertyTag. Peeks at the next property from the bag, along
// with its length (either 1- or 4-byte depending on the length
// bit in the property name). Returns DrError_EndOfStream if there is not enough
// data in the bag to read out the property name and length, but
// does not check that there are *pDataLen more bytes remaining.
DrError DrMemoryReader::PeekNextPropertyTag(
    /* out */ UInt16 *pEnumId,
    /* out */ UInt32 *pDataLen)
{
    if (PeekUInt16(pEnumId) != DrError_OK) {
        return status;
    }

    BYTE tmp[sizeof(UInt16) + sizeof(UInt32)];

    if (((*pEnumId) & PropLengthMask) == PropLength_Short) {
        if (PeekBytes(tmp, sizeof(UInt16) + sizeof(UInt8)) == DrError_OK) {
            *pDataLen = tmp[sizeof(UInt16)];
        }
    } else {
        if (PeekBytes(tmp, sizeof(UInt16) + sizeof(UInt32)) == DrError_OK) {
            memcpy(pDataLen, tmp+sizeof(UInt16), sizeof(UInt32));
        }
    }

    return status;
}

// PeekNextProperty: Peeks at the next property in the bag and fills
// in its name and length to pEnumId and pDataLen respectively,
// not advancing the the read pointer.
// Returns a pointer to the contiguous property value (either in
// the buffer or copied to make it contiguous).If PeekNextProperty returns an error,
// the values of *pEnumId,
// *pDataLen and *data are undefined.
DrError DrMemoryReader::PeekNextProperty(
    /* out */ UInt16 *pEnumId,
    /* out */ UInt32 *pDataLen,
    /* out */ const void **data)
{
    if (PeekNextPropertyTag(pEnumId, pDataLen) == DrError_OK) {
        UInt32 hdrLen = sizeof(UInt16);
        if (((*pEnumId) & PropLengthMask) == PropLength_Short) {
            hdrLen += sizeof(UInt8);
        } else {
            hdrLen += sizeof(UInt32);
        }
        const BYTE *pProp;
        if (PeekBytes(hdrLen + *pDataLen, &pProp) == DrError_OK) {
            *data = pProp + hdrLen;
        }
    }

    return status;
}


// ReadNextKnownProperty: Reads the next property which is of
// known ID and length into a preallocated buffer, placing the read pointer
// after the property value. If ReadNextKnownProperty returns error,
// the position of the read pointer is undefined.
// returns DrError_InvalidProperty if the enum id or length don't match.
DrError DrMemoryReader::ReadNextKnownProperty(
    UInt16 enumId,
    UInt32 dataLen,
    void *pDest)
{
    UInt16 realEnumId;
    UInt32 realDataLen;

    if (ReadNextPropertyTag(&realEnumId, &realDataLen) == DrError_OK) {
        if (realEnumId != enumId || realDataLen != dataLen) {
            SetStatus(DrError_InvalidProperty);
        } else {
            ReadData(dataLen, pDest);
        }
    }

    return status;
}

// PeekNextKnownProperty: Peeks at the next property which is of
// known ID and length into a preallocated buffer.
// returns DrError_InvalidProperty if the enum id or length don't match.
DrError DrMemoryReader::PeekNextKnownProperty(
    UInt16 enumId,
    UInt32 dataLen,
    void *pDest)
{
    UInt16 enumIdActual;
    UInt32 dataLenActual;
    if (PeekNextPropertyTag(&enumIdActual, &dataLenActual) == DrError_OK) {
        if (enumIdActual != enumId || dataLenActual != dataLen) {
            SetStatus(DrError_InvalidProperty);
        } else {
            UInt32 hdrLen = sizeof(UInt16);
            if ((enumId & PropLengthMask) == PropLength_Short) {
                hdrLen += sizeof(UInt8);
            } else {
                hdrLen += sizeof(UInt32);
            }
            const BYTE *pProp;
            if (PeekBytes(hdrLen + dataLen, &pProp) == DrError_OK) {
                memcpy(pDest, pProp + hdrLen, dataLen);
            }
        }
    }

    return status;
}


// Reads a string property that has been encoded with WriteStringProperty.
// If the string in the stream is longer than maxLength (not including null), DrError_StringTooLong is returned
DrError DrMemoryReader::ReadNextStringProperty(UInt16 enumId, /* out */ const char **ppStr, Size_t maxLength)
{
    UInt32 length;
    UInt16 realEnumId;

    if (ReadNextPropertyTag(&realEnumId, &length) == DrError_OK) {
        if (realEnumId != enumId) {
            SetStatus(DrError_InvalidProperty);
        } else if ((Size_t)length > maxLength) {
            SetStatus(DrError_StringTooLong);
        } else {
            *ppStr = NULL;

            if (length > 0) {
                BYTE *pBytes;
                if (length > NumContiguousBytesRemaining() || pData[length-1] != (BYTE)0) {
                    // Must allocate temporary buffer wth enough room for null terminator
                    pBytes = ReserveTempMemory(length+1);
                    if (ReadBytes(pBytes, length) == DrError_OK) {
                        pBytes[length] = (BYTE)0;
                    }
                } else {
                    ReadBytes(length, (const BYTE **)&pBytes);
                }

                if (status == DrError_OK) {
                    *ppStr = (const char *)(const void *)pBytes;
                }
            }
        }
    }
    return status;
}

// Reads a string property that has been encoded with WriteStringProperty.
// If the string in the stream is longer than maxLength (not including null), DrError_StringTooLong is returned
DrError DrMemoryReader::ReadOrAppendNextStringProperty(bool fAppend, UInt16 enumId, /* out */ DrStr& strOut, Size_t maxLength)
{
    UInt32 length;
    UInt16 realEnumId;

    if (ReadNextPropertyTag(&realEnumId, &length) == DrError_OK) {
        if (realEnumId != enumId) {
            SetStatus(DrError_InvalidProperty);
        } else if (length == 0) {
            // null string
            if (fAppend) {
                strOut.EnsureNotNull();
            } else {
                strOut = NULLSTR;
            }
        } else if ((Size_t)(length) > maxLength) {
            SetStatus(DrError_StringTooLong);
        } else {
            strOut.EnsureNotNull();
            size_t oldlen = strOut.GetLength();
            char *pOut = strOut.GetWritableAppendBuffer((size_t)length);
            ReadBytes((BYTE *)pOut, (Size_t)length);

            if (status == DrError_OK) {
                strOut.UpdateLength(oldlen + strlen(pOut));
            } else {
                strOut.UpdateLength(oldlen);
            }
        }
    }
    return status;
}


/* Read a string property from the buffer into a preallocated buffer.

If the embedded string is NULL, an empty string is returned.

If the string in the stream is longer than buffLength (including null), DrError_StringTooLong is returned
*/
DrError DrMemoryReader::ReadNextStringProperty(UInt16 enumId, char *pStr, Size_t buffLength)
{
    UInt32 length;
    UInt16 realEnumId;

    if (ReadNextPropertyTag(&realEnumId, &length) == DrError_OK) {
        if (realEnumId != enumId) {
            SetStatus(DrError_InvalidProperty);
        } else if (buffLength == 0) {
            SetStatus(DrError_StringTooLong);
        } else if (length == 0) {
            // NULL string
            pStr[0] = '\0';
        } else if ((Size_t)(length) > buffLength) {
            // We allow reading 1 more byte than maxLength, hoping that the last byte is a null
            SetStatus(DrError_StringTooLong);
        } else {
            pStr[length-1] = '\0';
            ReadBytes((BYTE *)pStr, length);

            if (status == DrError_OK) {
                // If we read more than buffLength-1 bytes, the last byte has to be a null
                if ((Size_t)length >= buffLength) {
                    if (pStr[length-1] != '\0') {
                        pStr[0] = '\0';
                        SetStatus(DrError_StringTooLong);
                    }
                } else {
                    pStr[length] = '\0';
                }
            }
        }
    }
    return status;
}


void DrMemoryReader::AllocTempMemBlock(size_t minLength)
{
    if (minLength < DEFAULT_TEMP_MEM_ALLOC_SIZE) {
        minLength = DEFAULT_TEMP_MEM_ALLOC_SIZE;
    }

    pFirstTempMemHeader = TempMemHeader::Alloc(minLength, pFirstTempMemHeader);
    LogAssert(pFirstTempMemHeader != NULL);
}

// Reserves a block of temporary memory that will be valid until this DrMemoryReader
// is destroyed.
BYTE *DrMemoryReader::ReserveTempMemory(size_t length)
{
    if (pFirstTempMemHeader == NULL || pFirstTempMemHeader->GetLength() < length) {
        AllocTempMemBlock(length);
        LogAssert (pFirstTempMemHeader != NULL && pFirstTempMemHeader->GetLength() >= length);
    }
    return pFirstTempMemHeader->ReserveData(length);
}

// Reads data from blocks starting *after* the current block, without advancing the current read pointer.
// Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
// may still be written into the byte array).
DrError DrMemoryReader::FutureBlockPeekBytes(/* out */ void *pBytes, size_t length)
{
    (void)pBytes;
    (void)length;
    return SetStatus(DrError_EndOfStream);
}

// Reads data from memory without advancing the current read pointer.
// Handles cross-block cases.
// Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
// is still wriiten into the byte array).
DrError DrMemoryReader::CrossBlockPeekBytes(/* out */ BYTE *pBytes, size_t length)
{
    Size_t nb;
    PeekMemHeader *ph;
    const BYTE *pb;

    if (status == DrError_OK && length > (size_t)0) {
        if (pFirstPeekMemHeader != NULL) {
            ph = pFirstPeekMemHeader;
            pb = pData;
            nb = NumContiguousBytesRemaining();

            do {
                if (nb > length) {
                    nb = length;
                }

                if (nb > 0) {
                    memcpy(pBytes, pb, nb);
                    pBytes += nb;
                    length -= nb;
                }

                if (length == 0) {
                    break;
                }

                ph = ph->GetNext();
                if (ph != NULL) {
                    pb = ph->GetData();
                    nb = ph->GetLength();
                }
            } while (ph != NULL);
        }

        if (length > 0) {
            if (FutureBlockPeekBytes(pBytes, length) == DrError_PeekTooFar) {
                // stream doesn't support peeking
                while (length > 0 && AppendNextBlock() == DrError_OK) {
                    ph = pLastPeekMemHeader;
                    pb = ph->GetData();
                    nb = ph->GetLength();
                    if (nb > length) {
                        nb = length;
                    }

                    if (nb > 0) {
                        memcpy(pBytes, pb, nb);
                        pBytes += nb;
                        length -= nb;
                    }
                }
            }
        }

    }

    return status;
}

// Reads data from memory, advancing the current read pointer.
// Handles cross-block cases.
// Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
// is still wriiten into the byte array).
DrError DrMemoryReader::CrossBlockReadBytes(/* out */ BYTE *pBytes, size_t length)
{
    while (length > (size_t)0) {
        size_t nr = NumContiguousBytesRemaining();
        if (nr == (size_t)0) {
            DrError ret = AdvanceToNextPeekBlock();
            if (ret != DrError_OK) {
                return ret;
            }
            nr = NumContiguousBytesRemaining();
        }
        LogAssert(nr > 0);

        size_t nb = (length > nr) ? nr : length;
        memcpy(pBytes, pData, nb);
        pData += nb;
        pBytes += nb;
        length -= nb;
    }
    return DrError_OK;
}

DrError DrMemoryReader::ReadBytesIntoWriter(DrMemoryWriter *pWriter, Size_t length)
{
    DrError ret;
    while (length > (size_t)0) {
        size_t nr = NumContiguousBytesRemaining();
        if (nr == (size_t)0) {
            ret = AdvanceToNextPeekBlock();
            if (ret != DrError_OK) {
                return ret;
            }
            nr = NumContiguousBytesRemaining();
        }
        LogAssert(nr > 0);

        size_t nb = (length > nr) ? nr : length;
        ret = pWriter->WriteBytes(pData, nb);
        if (ret != DrError_OK) {
            return SetStatus(ret);
        }
        pData += nb;
        length -= nb;
    }
    return DrError_OK;
}

// Skips data in memory, advancing the current read pointer.
// Handles cross-block cases.
// Returns DrError_EndOfStream if the stream reaches the end before all data can be skipped (partial data
// is still skipped).
DrError DrMemoryReader::CrossBlockSkipBytes(size_t length)
{
    while (length > (size_t)0) {
        size_t nr = NumContiguousBytesRemaining();
        if (nr == (size_t)0) {
            DrError ret = AdvanceToNextPeekBlock();
            if (ret != DrError_OK) {
                return ret;
            }
            nr = NumContiguousBytesRemaining();
        }
        LogAssert(nr > 0);
        size_t nb = (length > nr) ? nr : length;
        pData += nb;
        length -= nb;
    }
    return DrError_OK;
}

// Appends the next block from the underlying stream to the list of peekable data blocks.
DrError DrMemoryReader::AppendNextBlock()
{
    const BYTE *pBytes;
    Size_t length;

    if (status == DrError_OK) {
        // Read the next block from the stream
        if (SetStatus(ReadNextBlock(&pBytes, &length)) == DrError_OK) {
            // We have a new block. Append it to the end of the list of peek blocks.
            PeekMemHeader *ph;
            ph = new PeekMemHeader(length, pLastPeekMemHeader, pBytes);
            LogAssert(ph != NULL);
            pLastPeekMemHeader = ph;
            if (pFirstPeekMemHeader == NULL) {
                // We are appending the very first peek block.
                pFirstPeekMemHeader = ph;
                
                // The new block is now the current block.
                // We need to update the base class current block pointers
                pData = pBlockBase = pFirstPeekMemHeader->GetData();
                blockLength = pFirstPeekMemHeader->GetLength();
                
                // Since this is the first peek block, the prior block was empty and there is
                // no effect on uBlockBasePhysicalStreamPosition
            }
        }
    }
    
    return status;
}

// Advances the current block to the next available peek block, reading a new block if necessary
DrError DrMemoryReader::AdvanceToNextPeekBlock()
{
    if (status == DrError_OK) {
        // remove current peek block, if any
        if (pFirstPeekMemHeader != NULL) {
            // We have at least one peek block. The first one is the "current" block. We need to remove it.
            PeekMemHeader *ph = pFirstPeekMemHeader;
            pFirstPeekMemHeader = ph->Detach();
            if (pFirstPeekMemHeader == NULL) {
                // we removed the last peek block. we now have no current block.
                pLastPeekMemHeader = NULL;
                pData = pBlockBase = NULL;
                blockLength = 0;
            } else {
                // the next peek block is now the current block
                pData = pBlockBase = pFirstPeekMemHeader->GetData();
                blockLength = pFirstPeekMemHeader->GetLength();
            }
            // Since we advanced to a new current block, we need to advance uBlockBasePhysicalStreamPosition
            // by the length of the previous current block
            uBlockBasePhysicalStreamPosition += ph->GetLength();
            delete ph;
        }

        if (pFirstPeekMemHeader == NULL) {
            // if no more peek blocks, add a new one
            AppendNextBlock();
        }

        if (status == DrError_OK) {
            LogAssert(pFirstPeekMemHeader != NULL);
        }
    }
    return status;
}


// This method should be overidden by memory readers that know how to advance to a new block.
// Returns DrError_EndOfStream if there are no more blocks to be read.
// The default implementation always returns DrError_EndOfStream, which is appropriate for
// single-block readers.
DrError DrMemoryReader::ReadNextBlock(/* out */ const BYTE **pBytes, /* out */ Size_t *pLength)
{
    return SetStatus(DrError_EndOfStream);
}

// This method should be overridden by memory readers that know how to advance to a new block.
// The implementation should return true if there are at least "length" readable bytes
// following the current block.
//
// The default implementation always returns false, which is appropriate for single-block readers
bool DrMemoryReader::FutureBlocksCanBeRead(size_t length)
{
    return false;
}


// Consumes the (BeginTag, desiredTagType) property and closing (EndTag, desiredTagType) property, and calls you back
// on parser->OnParseProperty() for each decoded property.  Each property it calls you back on has only been peeked,
// so you will need to read or skip over it.  If another BeginTag appears, you will be called back with that.
DrError DrMemoryReader::ReadAggregate(UInt16 desiredTagType, DrPropertyParser *parser, void *cookie)
{
    DrError err;
    UInt16  beginTagType;

    if (ReadNextKnownProperty(Prop_Dryad_BeginTag, sizeof(UInt16), &beginTagType) != DrError_OK)
        return status;

    if (beginTagType != desiredTagType)
        return SetStatus(DrError_InvalidProperty);

    while (TRUE)
    {
        UInt16 propertyType;
        UInt32 dataLen;

        if (PeekNextPropertyTag(&propertyType, &dataLen) != DrError_OK)
            return status;

        // If we find an end tag, it must be for the begin tag we consumed
        if (propertyType == Prop_Dryad_EndTag)
        {
            UInt16 endTagType;

            // Consume it
            if (ReadNextUInt16Property(Prop_Dryad_EndTag, &endTagType) != DrError_OK)
                return status;

            if (desiredTagType != endTagType)
                return SetStatus(DrError_InvalidProperty);

            // We're done
            return DrError_OK;
        }
        else
        {
            // This could be a begin tag - it's up to the caller to call ReadAggregate()
            // or SkipNextPropertyOrAggregate()
            err = parser->OnParseProperty(this, propertyType, dataLen, cookie);
            if (err != DrError_OK)
                return SetStatus(err);
        }
    }
}

// If the next property is not a BeginTag, it simply skips it.
// If the next property is a BeginTag, then it skips everything through and including the EndTag,
// and handles recursion
// @TODO Limit recursion depth
DrError DrMemoryReader::SkipNextPropertyOrAggregate()
{
    UInt32  dataLen;
    UInt16  propertyType;
    UInt16  beginTagType;

    if (PeekNextPropertyTag(&propertyType, &dataLen) != DrError_OK)
        return status;

    // If it's not a begin tag, just skip the property and return
    if (propertyType != Prop_Dryad_BeginTag)
        return SkipNextProperty();

    // Read the begin tag type
    if (ReadNextUInt16Property(Prop_Dryad_BeginTag, &beginTagType) != DrError_OK)
        return status;

    // Skip until corresponding end tag
    // If another BeginTag is encountered, recurse as appropriate
    while (TRUE)
    {
        if (PeekNextPropertyTag(&propertyType, &dataLen) != DrError_OK)
            return status;

        if (propertyType == Prop_Dryad_BeginTag)
        {
            if (SkipNextPropertyOrAggregate() != DrError_OK)
                return status;
        }
        else if (propertyType == Prop_Dryad_EndTag)
        {
            UInt16 endTagType;

            if (ReadNextUInt16Property(Prop_Dryad_EndTag, &endTagType) != DrError_OK)
                return status;

            if (endTagType != beginTagType)
                return SetStatus(DrError_InvalidProperty);

            return DrError_OK;
        }
        else
        {
            if (SkipNextProperty() != DrError_OK)
                return status;
        }
    }
}


DrError DrMemoryBufferWriter::SetBufferOffset(Size_t offset)
{
    if (status == DrError_OK && !MemoryWriterIsClosed()) {
        FlushMemoryWriter();
        if (status == DrError_OK) {
            Size_t uSize;
            BYTE *p = (BYTE *)m_pBuffer->GetWriteAddress(offset, (Size_t)1, &uSize); 
            if (p == NULL) {
                SetStatus(DrError_EndOfStream);
            } else {
                uBlockBasePhysicalStreamPosition = (UInt64)offset;
                pData = pBlockBase = p;
                blockLength = uSize;
            }
        }
    }
    
    return status;
}

DrError DrMemoryBufferWriter::FlushMemoryWriter()
{
    if (status == DrError_OK && !MemoryWriterIsClosed())
    {
        if (m_pBuffer != NULL)
        {
            Size_t newPos = GetBufferOffset();
            if (m_fTruncateOnFlush || newPos > m_pBuffer->GetAvailableSize()) {
                m_pBuffer->SetAvailableSize(newPos);
            }
        }
        // DrMemoryWriter::FlushMemoryWriter()
    }
    return status;
}

void DrMemoryBufferWriter::InternalFree()
{
    m_pBuffer = NULL;
}

DrError DrMemoryBufferWriter::CloseMemoryWriter()
{
    if (!MemoryWriterIsClosed())
    {
        DrMemoryWriter::CloseMemoryWriter(); // this will call FlushMemoryWriter
        InternalFree();
    }
    return status;
}

DrMemoryBufferWriter::~DrMemoryBufferWriter()
{
    //=======================================================================================================
    // NOTE: each subclass should duplicate this code if it implements CloseMemoryWriter or FlushMemoryWriter
    //
    if (!MemoryWriterIsClosed())
    {
        // Close the memory writer. We cannot tolerate new failures in CloseMemoryWriter at this point because we are in a destructor!
        // This will call FlushMemoryWriter() before closing, but because of virtual destructor unwinding, it will call the
        // current class's implementation rather than the subclass's overriding implementation. So each subclass needs to implement this call in their
        // virtual destructor if they implement CloseMemoryWriter or FlushMemoryWriter
        MemoryWriterDestructorClose();
    }
    //
    // NOTE: End dublicated destructor code
    //=======================================================================================================
    InternalFree();
}

DrError DrMemoryBufferWriter::AdvanceToNextBlock()
{
    if (status == DrError_OK) {
        Size_t newPos = GetBufferOffset();
        m_pBuffer->SetAvailableSize(newPos);
        Size_t uNewBlockSize;
        BYTE *p = (BYTE *)m_pBuffer->GetWriteAddressIfPossible(newPos, (Size_t)1, &uNewBlockSize); 
        if (p == NULL) {
            SetStatus(DrError_EndOfStream);
        } else {

            // Update DrMemoryWriter context for the new contiguous block            
            pBlockBase = p;
            blockLength = uNewBlockSize;
            pData = pBlockBase;
            uBlockBasePhysicalStreamPosition = (UInt64)newPos;
        }
    }
    return status;
}

bool DrMemoryBufferWriter::FutureBlocksCanBeWritten(Size_t length)
{
    (void)length;
    return true;
}

void DrMemoryBufferReader::InternalFree()
{
    m_pBuffer = NULL; // decrefs the buffer
    nextReadOffset = 0;
}

DrError DrMemoryBufferReader::CloseMemoryReader()
{
    if (!MemoryReaderIsClosed())
    {
        DrMemoryReader::CloseMemoryReader();
        InternalFree();
    }
    return status;
}

DrMemoryBufferReader::~DrMemoryBufferReader()
{
    InternalFree();
}

DrError DrMemoryBufferReader::SetBufferOffset(Size_t offset)
{
    if (status == DrError_OK && !MemoryReaderIsClosed()) {
        Size_t uStreamSize = 0;
        if (m_pBuffer != NULL) {
            uStreamSize = m_pBuffer->GetAvailableSize();
        }
        if (m_pBuffer == NULL || offset > uStreamSize) {
            SetStatus(DrError_EndOfStream);
        } else {
            DiscardMemoryReaderContext();
            const BYTE *p = NULL; 
            Size_t uSize = 0;
            if (offset < uStreamSize) {
                p = (const BYTE *)m_pBuffer->GetReadAddress(offset, &uSize); 
                LogAssert (p != NULL && uSize > 0);
            }
            uBlockBasePhysicalStreamPosition = (UInt64)offset;
            nextReadOffset = offset + uSize;
            pData = pBlockBase = (BYTE *)p;
            blockLength = uSize;
        }
    }
    return status;
}


DrError DrMemoryBufferReader::ReadNextBlock(/* out */ const BYTE **ppBytes, /* out */ Size_t *pLength)
{
    *ppBytes = NULL;
    *pLength = 0;
    if (status == DrError_OK) {
        Size_t uSize;
        if (m_pBuffer == NULL || nextReadOffset >= m_pBuffer->GetAvailableSize()) {
            SetStatus(DrError_EndOfStream);
        } else {
            const BYTE *p = (const BYTE *)m_pBuffer->GetReadAddress(nextReadOffset, &uSize); 
            LogAssert (p != NULL && uSize > 0);
            *ppBytes = p;
            *pLength = uSize;
            nextReadOffset += uSize;
        }
    }
    return status;
}

DrError DrMemoryBufferReader::FutureBlockPeekBytes(/* out */ void *pBytes, Size_t length)
{
    if (status == DrError_OK) {
        Size_t cbAvailable = m_pBuffer->GetAvailableSize();
        if (m_pBuffer == NULL || nextReadOffset > cbAvailable || cbAvailable - nextReadOffset < length) {
            SetStatus(DrError_EndOfStream);
        } else {
            m_pBuffer->Read(nextReadOffset, pBytes, length);
        }
    }
    return status;
}

bool DrMemoryBufferReader::FutureBlocksCanBeRead(Size_t length)
{
    if (status != DrError_OK) {
        return false;
    }
    Size_t cbAvailable = m_pBuffer->GetAvailableSize();
    return (m_pBuffer != NULL && nextReadOffset <= cbAvailable && cbAvailable - nextReadOffset >= length);
}
