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

#include <basic_types.h>
#include <propertyids.h>


// Abstract class used if you use the ReadAggregate function on DrMemoryReader to parse properties
class DrMemoryReader;

class DrPropertyParser
{
public:
    virtual DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID, UInt32 dataLen, void *cookie) = NULL;
};

/* Classes for marshalling data into and out of non-contiguous memory buffers */

#pragma warning (push)

// These are:
// 1) Linker complaining about a couple of functions that are never called (this is ok, function may be called in the future)
// 2) size_t issues.  Sammck is going to fix these.
#pragma warning (disable:4201)
#pragma warning (disable:4365)

class DrMemoryStream
{
    friend class DrMemoryWriter;
    friend class DrMemoryReader;
        
public:
    // Base-class implemented methods:
    
    // Sets the current status, if not already set to a failing code. Once the status
    // has been changed to something other than DrError_OK, it cannot be changed.
    // Returns the new current status, which may not be the same as the status passed in.
    __forceinline DrError SetStatus(DrError err)
    {
        if (status == DrError_OK) {
            status = err;
        }
        return status;
    }

    // Returns the current status. If this is not DrError_OK, the status is persistent and
    //    all future attempts to manipulate this stream will fail with this status code. */
    __forceinline DrError GetStatus()
    {
        return status;
    }

    // Returns the current physical byte offset from the beginning of the entire stream.
    // For readers, this is the last set physical stream position (or 0 if it has never been set) plus the number of bytes read/parsed since
    //     the physical position was last set. Does not include buffered or peeked data that has not yet been permanently read.
    // For writers, this is the last set physical stream position (or 0 if it has never been set) plus the number of bytes written since
    //     the physical position was last set, including data that had been written but has not yet been flushed.
    __forceinline UInt64 GetPhysicalStreamPosition()
    {
        // Note: both pData and pBlockBase may be NULL, which results in uBlockBasePhysicalStreamPosition
        return uBlockBasePhysicalStreamPosition + (pData - pBlockBase);
    }

public:
    // Deprecated methods:

    // BUGBUG: this method used to be void, but needs to return DrError
    // BUGBUG: changed return type to intentionally break subclass virtual override, and deprecated to encourage callers to
    // BUGBUG: check return code.
    // Deprecated --Replace with CloseMemoryReader() or CloseMemoryWriter, and check return code or writer status.
    __declspec(deprecated) virtual DrError Close() sealed;

protected:
    // subclass-visible methods

    // Returns the number of bytes remaining within the current contiguous block.
    // For readers, this is the number of contiguous bytes to be read before the read block
    //    must be advanced (or end of stream is reached). Does not include prefetched/peeked data.
    // For writers, this is the number of contiguous unflushed bytes which can be written before the current
    //    block must be flushed and a new block allocated.
    __forceinline Size_t NumContiguousBytesRemaining()
    {
        // Note: both pData and pBlockBase may be NULL, which results in blockLength (which must be 0 in that case)
        return blockLength - (pData - pBlockBase);
    }

private:
    // private friend-only methods (only usable by friend classes):

    // Clears all buffer context values to initial defaults (including settting the physical stream position to 0), but does not change the current status code.
    void DiscardMemoryStreamContext();
        
private:
    // private methods:
    // Cannot be directly instantiated; must be subclassed by a friend class
    DrMemoryStream();

    // cannot be directly destructed, must be referenced through subclass
    virtual ~DrMemoryStream();

// TODO: hide protected members, use accessors
protected:
    // sub-class visible member data
    
    /* The status starts at DrError_OK. at the first stream error, it is set to an
       error code. After this point, the current pointer is never advanced and the
       error is never reset.
       When the DrMemoryStream is closed, memory resources are freed and the
       pointers are reset, but the status remains the same as the pre-close status
     */
    // TODO: prevent subclass from changing status after it is set to an error
    DrError status;

    /* The base pointer to the current contiguous block */
    BYTE    *pBlockBase;

    /* The length of the current contiguous block */
    Size_t  blockLength;

    /* The current read or write pointer (inside the current contiguous block) */
    BYTE    *pData;

    /* The physical position of the beginning of the current contiguous block within the
       overall stream. This is advanced by blockLength when we move to a new block. It
       is provided to that the caller can determine physical stream position independent
       of buffering behavior. */
    UInt64  uBlockBasePhysicalStreamPosition;
private:
};

class DrMemoryReader;

class DrMemoryWriter : public DrMemoryStream
{
public:
    // Public subclass-overridable methods

    // Flushes output to its destination. By default, this just returns the writer status.
    // This method is called by CloseMemoryWriter if the stream is not yet closed.
    virtual DrError FlushMemoryWriter()
    {
        return status;
    }
    
    // Default implementation just frees any abandoned temporary
    // buffered write buffers, calls FlushMemoryWriter, and returns the writer status.
    // If this is the first time CloseMemoryWriter() has been called, then resources are freed regardless of
    // the current status, allowing this method to be used to free resources even on failed streams.
    // The current status is always returned.
    // Subclasses that override this method should *always* delegate to their parent class
    // after freeing their own resources, and should *always* return the current status at completion.
    virtual DrError CloseMemoryWriter();

    // Special Write methods that can be optimized by subclasses to copy buffers by reference. The default
    // implementation simply reads contiguous blocks from the buffer and writes them to the output stream.
    virtual DrError WriteBytesFromBuffer(DrMemoryBuffer *pBuffer, bool fAllowCopyBufferByReference=false);

public:
    // public base class methods:

    // Returns true if CloseMemoryWriter has been called and has run all the way down to the base class
    __forceinline bool MemoryWriterIsClosed()
    {
        return m_fMemoryWriterIsClosed;
    }

    // Verifies that a certain number of bytes can be written without error (DrError_OK is not a guarantee of
    // success, but anything else is a guarantee of failure and becomes a persistent failure). This method
    // handles the contiguous case inline, and delegates the split case to a real function.
    //
    // returns DrError_EndOfStream if the output stream cannot be extended by the required length.
    //
    virtual DrError EnsureCanBeWritten(Size_t length)
    {
        if (status != DrError_OK) {
            return status;
        } else if (length > NumContiguousBytesRemaining() && !CrossBlockCanBeWritten(length)) {
            return SetStatus(DrError_EndOfStream);
        } else {
            return DrError_OK;
        }
    }

    // This is the quick, and primary, method for stuffing bytes into the output stream. It handles
    // the contiguous case inline, and delegates the cross-block case to a real function.
    // Returns DrError_EndOfStream if the stream becomes full before all data can be written (partial data
    // may still be written).
    virtual DrError WriteBytes(const void *pBytes, Size_t length)
    {
        if (status == DrError_OK) {
            LogAssert(pBytes != NULL || length == 0);
            if (length <= NumContiguousBytesRemaining()) {
                memcpy(pData, pBytes, length);
                pData += length;
            } else {
                CrossBlockWriteBytes((const BYTE *)pBytes,   length);
            }
        }
        return status;
    }

    DrError WriteBytesFromReader(DrMemoryReader *pReader, Size_t length);
    
    /* Append values of different types. Note that once an error occurs on the stream, these
       methods always fail without having any effect. This allows the caller to call a sequence
       of these methods and only check the return code of the last one. */

    inline DrError WriteByte(BYTE val)
    {
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteChar(char val)
    {
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteInt8(Int8 val)
    {
        return WriteBytes((const BYTE *)(&val), sizeof(val));
    }

    inline DrError WriteInt16(Int16 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteInt32(Int32 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteInt64(Int64 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    // compat with netlib
    inline DrError WriteUInt8(UInt8 val)
    {
        return WriteBytes((const BYTE *)(&val), sizeof(val));
    }

    inline DrError WriteUInt16(UInt16 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteUInt32(UInt32 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteUInt64(UInt64 val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    // This will cause an assertion failure if the size is too big to fit in UInt32
    inline DrError WriteSize_tAsUInt32(Size_t val)
    {
        // WARNING: Assumes little endian
        LogAssert((Size_t)(UInt32)val == val);
        return WriteUInt32((UInt32)val);
    }

    inline DrError WriteSize_tAsUInt64(Size_t val)
    {
        // WARNING: Assumes little endian
        return WriteUInt64((UInt64)val);
    }

    inline DrError WriteFloat(float val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteDouble(double val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    /* copy length bytes verbatim into the buffer. This call does not
    write length into the buffer explicitly; if you want to precede
    the data blob with a length, call e.g. WriteUInt32() first. */
    inline DrError WriteData(UInt32 length, const void *data)
    {
        return WriteBytes(data, (Size_t)length);
    }

    inline DrError WriteGuid(const GUID& guid)
    {
        return WriteBytes(&guid, sizeof(guid));
    }

    inline DrError WriteDrError(DrError val)
    {
        // WARNING: Assumes little endian
        return WriteBytes(&val, sizeof(val));
    }

    inline DrError WriteBool(bool val)
    {
        UInt8 v = val ? 1 : 0;
        return WriteBytes(&v, sizeof(v));
    }

    inline DrError WriteTimeStamp(DrTimeStamp ts)
    {
        return WriteBytes(&ts, sizeof(ts));
    }

    inline DrError WriteTimeInterval(DrTimeInterval ti)
    {
        return WriteBytes(&ti, sizeof(ti));
    }

    /* Array versions of writes. Write an array of data elements in to the
       buffer */

    inline DrError WriteByteArray(UInt32 count, const BYTE *vals)
    {
        return WriteBytes(vals, (Size_t)count);
    }

    inline DrError WriteCharArray(UInt32 count, const char *vals)
    {
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count);
    }

    inline DrError WriteInt8Array(UInt32 count, const Int8 *vals)
    {
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count);
    }

    inline DrError WriteInt16Array(UInt32 count, const Int16 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteInt32Array(UInt32 count, const Int32 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteInt64Array(UInt32 count, const Int64 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteUInt8Array(UInt32 count, const UInt8 *vals)
    {
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count);
    }

    inline DrError WriteUInt16Array(UInt32 count, const UInt16 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteUInt32Array(UInt32 count, const UInt32 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteUInt64Array(UInt32 count, const UInt64 *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteFloatArray(UInt32 count, const float *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteDoubleArray(UInt32 count, const double *vals)
    {
        // Assumes Little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    inline DrError WriteGuidArray(UInt32 count, const GUID *vals)
    {
        // Assumes little Endian
        return WriteBytes((const BYTE *)(void *)vals, (Size_t)count * sizeof(vals[0]));
    }

    // WritePropertyTagXXX: These functions write the part of the
    // property not incuding "data". After calling WritePropertyTagXXX, you *must*
    // write exactly dataLen bytes of data.
    // WritePropertyTagShort must only be used for SHORTATOM's,
    // and WritePropertyTagLong must only be used for LONGATOM's.
    // WritePropertyTagAnySize calls either WritePropertyTagShort or WritePropertyTagLong
    // depending on the atom type.


    // WritePropertyTagShort--write a SHORTATOM property ID and length value.
    // Will cause an assertion failure if the property id is not a SHORTATOM.
    inline DrError WritePropertyTagShort(UInt16 enumId, UInt8 dataLen)
    {
        LogAssert((enumId & PropLengthMask) == PropLength_Short);
        WriteUInt16(enumId);
        return WriteUInt8(dataLen);
    }

    // WritePropertyTagShort--write a SHORTATOM property ID and length value.
    // This version takes an arbitrary Size_t dataLen
    // Will cause an assertion failure if the property id is not a SHORTATOM, or if dataLen > 255.
    inline DrError WritePropertyTagShort(UInt16 enumId, Size_t dataLen)
    {
        LogAssert((enumId & PropLengthMask) == PropLength_Short && dataLen <= (Size_t)_UI8_MAX);
        WriteUInt16(enumId);
        return WriteUInt8((UInt8)dataLen);
    }

    // WritePropertyTagLong--write a LONGATOM property ID and length value.
    // Will cause an assertion failure if the property id is not a LONGATOM, or if dataLen > _UI32_MAX.
    inline DrError WritePropertyTagLong(UInt16 enumId, Size_t dataLen)
    {
        LogAssert((enumId & PropLengthMask) == PropLength_Long && dataLen <= (Size_t)_UI32_MAX);
        WriteUInt16(enumId);
        return WriteUInt32((UInt32)dataLen);
    }

    // WritePropertyTagAnySize--write a SHORTATOM or LONGATOM property ID and length value.
    // Will cause an assertion failure if:
    //     a) The property id is a SHORTATOM and dataLen > 255
    //     b) The property id is a LONGATOM and dataLen > _UI32_MAX
    inline DrError WritePropertyTagAnySize(UInt16 enumId, Size_t dataLen)
    {
        if ((enumId & PropLengthMask) == PropLength_Short)
        {
            WritePropertyTagShort(enumId, dataLen);
        }
        else
        {
            WritePropertyTagLong(enumId, dataLen);
        }
        return status;
    }

    /* the following methods may only be called with enumId values that represent SHORTATOM's */

    // WriteProperty: these functions write a property tag followed by
    // the value of the implicitly specified type.

    // This method writes a LONGATOM or SHORTATOM property as an arbitrary sequence of bytes.
    //
    // Causes an assertion failure if:
    //     a) "pvData" is NULL but "dataLen" is not 0.
    //     b) "dataLen" > 255 for SHORTATOMs
    //     c) "dataLen" > _UI32_MAX for LONGATOMs
    inline DrError WriteAnySizeBlobProperty(UInt16 enumId, Size_t dataLen, const void *pvData)
    {
        WritePropertyTagAnySize(enumId, dataLen);
        return WriteBytes(pvData, dataLen);
    }

    // This method writes a LONGATOM property as an arbitrary sequence of bytes.
    //
    // Causes an assertion failure if:
    //     a) "pvData" is NULL but "dataLen" is not 0.
    //     b) enumId is not a LONGATOM
    //     c) "dataLen" > _UI32_MAX
    inline DrError WriteLongBlobProperty(UInt16 enumId, Size_t dataLen, const void *pvData)
    {
        WritePropertyTagLong(enumId, dataLen);
        return WriteBytes(pvData, dataLen);
    }

    // This method writes a SHORTATOM property as an arbitrary sequence of bytes.
    //
    // Causes an assertion failure if:
    //     a) "pvData" is NULL but "dataLen" is not 0.
    //     b) enumId is not a SHORTATOM
    //     c) "dataLen" > 255
    inline DrError WriteShortBlobProperty(UInt16 enumId, Size_t dataLen, const void *pvData)
    {
        WritePropertyTagShort(enumId, dataLen);
        return WriteBytes(pvData, dataLen);
    }

    // Note: formerly, this only allowed short property atoms. Now it allows either long or short properties.
    // For optimal efficiency, use WriteEmptyShortProperty or WriteEmptyLongProperty if known at design time.
    inline DrError WriteEmptyProperty(UInt16 enumId)
    {
        return WritePropertyTagAnySize(enumId, 0);
    }

    inline DrError WriteEmptyShortProperty(UInt16 enumId)
    {
        return WritePropertyTagShort(enumId, (UInt8)0);
    }

    inline DrError WriteEmptyLongProperty(UInt16 enumId)
    {
        return WritePropertyTagLong(enumId, 0);
    }

    inline DrError WriteInt8Property(UInt16 enumId, Int8 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt8(value);
    }

    inline DrError WriteInt16Property(UInt16 enumId, Int16 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt16(value);
    }

    inline DrError WriteInt32Property(UInt16 enumId, Int32 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt32(value);
    }

    inline DrError WriteInt64Property(UInt16 enumId, Int64 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt64(value);
    }

    inline DrError WriteUInt8Property(UInt16 enumId, UInt8 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt8(value);
    }

    inline DrError WriteUInt16Property(UInt16 enumId, UInt16 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt16(value);
    }

    inline DrError WriteUInt32Property(UInt16 enumId, UInt32 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt32(value);
    }

    inline DrError WriteUInt64Property(UInt16 enumId, UInt64 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt64(value);
    }

    inline DrError WriteSize_tAsUInt32Property(UInt16 enumId, Size_t value)
    {
        WritePropertyTagShort(enumId, sizeof(UInt32));
        return WriteSize_tAsUInt32(value);
    }

    inline DrError WriteSize_tAsUint64Property(UInt16 enumId, Size_t value)
    {
        WritePropertyTagShort(enumId, sizeof(UInt64));
        return WriteSize_tAsUInt64(value);
    }

    inline DrError WriteFloatProperty(UInt16 enumId, float value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteFloat(value);
    }

    inline DrError WriteDoubleProperty(UInt16 enumId, double value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteDouble(value);
    }

    inline DrError WriteGuidProperty(UInt16 enumId, const GUID& value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteGuid(value);
    }

    inline DrError WriteBoolProperty(UInt16 enumId, bool value)
    {
        WritePropertyTagShort(enumId, sizeof(UInt8));
        return WriteBool(value);
    }

    inline DrError WriteDrErrorProperty(UInt16 enumId, DrError value)
    {
        WritePropertyTagShort(enumId, sizeof(DrError));
        return WriteDrError(value);
    }

    inline DrError WriteTimeStampProperty(UInt16 enumId, DrTimeStamp value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteTimeStamp(value);
    }

    inline DrError WriteTimeIntervalProperty(UInt16 enumId, DrTimeInterval value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteTimeInterval(value);
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
    DrError WriteLongStringPropertyWithLength(UInt16 enumId, const char *pstr, Size_t length);

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
    DrError WriteShortStringPropertyWithLength(UInt16 enumId, const char *pstr, Size_t length);

    // Writes a '\0'-terminated string as a LONGATOM property
    //
    // Causes an assertion failure if:
    //     a) enumId is a SHORTATOM
    //     b) strlen(pstr) >= _UI32_MAX
    //
    // See WriteLongStringPropertyWithLength for more information.
    //
    DrError WriteLongStringProperty(UInt16 enumId, const char *pstr)
    {
        size_t len = 0;
        if (pstr != NULL)
        {
            len = strlen(pstr);
        }
        return WriteLongStringPropertyWithLength(enumId, pstr, len);
    }

    // Writes a '\0'-terminated string as a SHORTATOM property
    //
    // Causes an assertion failure if:
    //     a) enumId is a SHORTATOM
    //     b) strlen(pstr) >= _UI32_MAX
    //
    // See WriteLongStringPropertyWithLength for more information.
    //
    DrError WriteShortStringProperty(UInt16 enumId, const char *pstr)
    {
        size_t len = 0;
        if (pstr != NULL)
        {
            len = strlen(pstr);
        }
        return WriteShortStringPropertyWithLength(enumId, pstr, len);
    }

    // Writes a LONGATOM string property value from a DrStr. 
    //
    // Any DrStr value, including NULL and strings with embedded '\0' bytes can be properly round-tripped.
    //
    // Causes an assertion failure if:
    //     a) enumId is a SHORTATOM
    //     b) length >= _UI32_MAX
    //     c) pstr == NULL && length != 0
    //
    // See WriteLongStringPropertyWithLength for more information.
    //
    DrError WriteLongDrStrProperty(UInt16 enumId, const DrStr& str)
    {
        return WriteLongStringPropertyWithLength(enumId, str.GetString(), str.GetLength());
    }

    // Writes a SHORTATOM string property value from a DrStr. 
    // 
    // Any DrStr value, including NULL and strings with embedded '\0' bytes can be properly round-tripped.
    //
    // Causes an assertion failure if:
    //     a) enumId is a LONGATOM
    //     b) length >= 255
    //     c) pstr == NULL && length != 0
    //
    // See WriteLongStringPropertyWithLength for more information.
    //
    DrError WriteShortDrStrProperty(UInt16 enumId, const DrStr& str)
    {
        return WriteShortStringPropertyWithLength(enumId, str.GetString(), str.GetLength());
    }

#define DRDEPRECATED_UNTYPED __declspec(deprecated)

public:
    // Deprecated methods:
    
    // BUGBUG: this method used to be void, but needs to return DrError
    // BUGBUG: changed return type to intentionally break subclass virtual override, and deprecated to encourage callers to
    // BUGBUG: check return code.
    // Deprecated --Replace with FlushMemoryWriter(), and check return code or writer status.
    __declspec(deprecated) virtual DrError Flush() sealed
    {
        SetStatus(FlushMemoryWriter());
        return status;
    }

    // deprecated, use WriteBlobProperty
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, Size_t dataLen, const void *data)
    {
        WritePropertyTagAnySize(enumId, dataLen);
        return WriteBytes(data, dataLen);
    }

    // deprecated, use WriteEmptyShortProperty, WriteEmptyLongProperty, or WriteEmptyProperty
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId)
    {
        return WritePropertyTagAnySize(enumId, 0);
    }

    // deprecated, use WriteInt8Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, Int8 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt8(value);
    }

    // deprecated, use WriteInt16Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, Int16 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt16(value);
    }

    // deprecated, use WriteInt32Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, Int32 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt32(value);
    }

    // deprecated, use WriteInt64Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, Int64 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteInt64(value);
    }

    // deprecated, use WriteUInt8Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, UInt8 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt8(value);
    }

    // deprecated, use WriteUInt16Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, UInt16 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt16(value);
    }

    // deprecated, use WriteUInt32Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, UInt32 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt32(value);
    }

    // deprecated, use WriteUInt64Property
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, UInt64 value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteUInt64(value);
    }

    // deprecated, use WriteFloatProperty
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, float value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteFloat(value);
    }

    // deprecated, use WriteDoubleProperty
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, double value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteDouble(value);
    }

    // deprecated, use WriteGuidProperty
    inline DRDEPRECATED_UNTYPED DrError WriteProperty(UInt16 enumId, const GUID& value)
    {
        WritePropertyTagShort(enumId, sizeof(value));
        return WriteGuid(value);
    }

public:
    // virtual destructor
    
    // Note: it is the most derived subclass's responsibility to call MemoryWriterDestructorClose()
    // at destruct time if desired and necessary (since the virtual destructor cleanup order
    // makes it too late to call the virtual CloseMemoryWriter() method. However, since an error cannot be returned
    // at destruct time, this is only possible if CloseMemoryWriter() cannot fail. So MemoryWriterDestructorClose()
    // will assert on success of CloseMemoryWriter() unless status is already failing prior to MemoryWriterDestructorClose() time,
    // or MemoryWriterDestructorClose has already been called.
    // If the subclass wants to inhibit a base class from attempting to close at destruct time, it should just call
    // SetIgnoreMemoryWriterCloseFailureInDestructor().
    virtual ~DrMemoryWriter();
    
protected:
    // Protected subclass-overridable methods

    // Clears all buffer context values to initial defaults (including settting the physical stream position to 0
    // and discarding abandandoned temporary buffered write buffers), but does not change the current status code
    // or "closed" status.
    // may be overridden by a subclass if it needs to free resources controlled by the
    // context pointers before delegating to this method implementation.
    virtual void DiscardMemoryWriterContext();
        
    // This method should be overridden by memory writers that know how to allocate a new block and keep writing.
    // The implementation should update pData, pBlockBase, blockLength, and uBlockBasePhysicalStreamPosition to point to the new block.
    // The current block will be completely filled before calling this method, since there is no way to back up.
    // After this call, the old block can be disposed of in any way the underlying implementation chooses (e.g., flushing).
    // Returns SetStatus(DrError_EndOfStream) if a new block can't or shouldn't be allocated.
    // The default implementation always returns DrError_EndOfStream, which is appropriate for single-block writers.
    // If an error is returned, status has been set.
    virtual DrError AdvanceToNextBlock();

    // This method checks whether an attempt to write beyond the end of the current block
    // can possibly succeed. The implementation should return true if the buffer is indefinitely growable.
    // The default implementation returns false, which is appropriate for ungrowable single-block writers.
    // Note that this method must not set status, and will only be called when status is DrError_OK.
    virtual bool FutureBlocksCanBeWritten(Size_t length);

protected:
    // protected base class methods
    
    // Note: it is the most derived subclass's responsibility to call MemoryWriterDestructorClose()
    // at destruct time if desired and necessary (since the virtual destructor cleanup order
    // makes it too late to call the virtual FlushMemoryWriter() method from the base class. Since an error cannot be returned
    // at destruct time, this is only safe if CloseMemoryWriter() cannot fail. So MemoryWriterDestructorClose()
    // will assert on success of CloseMemoryWriter() unless status is already failing prior to MemoryWriterDestructorClose() time,
    // or CloseMemoryWriter() has already been called.
    // If the subclass wants to inhibit the base class from crashing on close failure at destruct time, it can just call
    // SetIgnoreMemoryWriterCloseFailureInDestructor().
    void MemoryWriterDestructorClose();

private:
    // Private methods:

    // This frees all temporary buffers (e.g., incomplete/abandoned buffered writes). The base class stae remains unchanged.
    void InternalFree();
    
    // This method determines if a number of bytes can successfully be written, handling
    // the case where the data will cross blocks.
    // This method is only called with status == DrError_OK
    inline bool CrossBlockCanBeWritten(Size_t length)
    {
        if (length <= NumContiguousBytesRemaining()) {
            return true;
        } else {
            return FutureBlocksCanBeWritten(length - NumContiguousBytesRemaining());
        }
    }

    // This method writes bytes into the buffer, handling the case where the data
    // will cross blocks.
    DrError CrossBlockWriteBytes(const BYTE *pBytes, Size_t length);

protected:
    // cannot be directly instantiated, must be subclassed
    DrMemoryWriter();
    
private:
    // Private member variables
    
    DrRef<DrMemoryBuffer> m_pPendingBufferedWriteBuffer; // If not NULL, a buffered write is in progress, and this is the buffer
    Size_t m_pendingBufferredWriteOldAvailableSize; // If a buffered write is in progress, the available size of the buffer at the beginning of the operation

    // true if CloseMemoryWriter has already been called or is currently running
    bool m_fMemoryWriterIsClosed;

    // true if the subclass wants us to ignore failures in MemoryWriterDestructorClose.
    bool m_fIgnoreMemoryWriterCloseFailureInDestructor;
};

class DrMemoryReader : public DrMemoryStream
{
public:
    // public subclass-overridable methods

    // Default implementation just frees any buffers and returns the reader status.
    // If this is the first time CloseMemoryReader() has been called, then resources are freed regardless of
    // the current status, allowing this method to be used to free resources even on failed streams.
    // The current status is always returned.
    // Unless MemoryStreamIsClosed(), subclasses that override this method should *always* delegate to their parent class
    // after freeing their own resources, and should *always* return the current status at completion.
    virtual DrError CloseMemoryReader();

    // This method frees any temporary buffers that have been allocated to store returned results (such as strings) from
    // selected auto-allocating ReadXXX methods (required when the result crossed non-contiguous buffer boundaries).
    // After this call is made, any result pointers from these ReadXXX methods become invalid.
    // The stream itself is still valid, and reading may continue.
    virtual void DiscardTemporaryResults();

public:
    // public base-class methods    

    // Returns true if CloseMemoryReader has been called and has run all the way down to the base class
    __forceinline bool MemoryReaderIsClosed()
    {
        return m_fMemoryReaderIsClosed;
    }

    inline Size_t NumContiguousBytesRead()
    {
        return pData - pBlockBase;
    }

    inline DrError EnsureCanBeRead(Size_t length)
    {
        if (status != DrError_OK) {
            return status;
        } else if (length > NumContiguousBytesRemaining() && !CrossBlockCanBeRead(length)) {
            return SetStatus(DrError_EndOfStream);
        } else {
            return DrError_OK;
        }
    }

    // Reads data from memory, without advancing the current read pointer. Uses the caller's buffer.
    // Handles simple case inline. Delegates cross-block cases to CrossBlockPeekBytes.
    // Returns DrError_EndOfStream (and sets status) if the stream reaches the end before all data can be read (partial data
    // is still written into the byte array).
    inline DrError PeekBytes(/* out */BYTE *pBytes, Size_t length)
    {
        if (status != DrError_OK) {
            return status;
        } else if (length > NumContiguousBytesRemaining()) {
            return CrossBlockPeekBytes(pBytes, length);
        } else {
            memcpy(pBytes, pData, length);
            return DrError_OK;
        }
    }

    // Reads data from memory, advancing the current read pointer. Uses the caller's buffer.
    // Handles simple case inline. Delegates cross-block cases to CrossBlockReadBytes.
    // Returns DrError_EndOfStream (and sets status) if the stream reaches the end before all data can be read (partial data
    // is still written into the byte array).
    inline DrError ReadBytes(/* out */BYTE *pBytes, Size_t length)
    {
        if (status != DrError_OK) {
            return status;
        } else if (length > NumContiguousBytesRemaining()) {
            return CrossBlockReadBytes(pBytes, length);
        } else {
            memcpy(pBytes, pData, length);
            pData += length;
            return DrError_OK;
        }
    }

    // reads bytes from this stream, and writes them into the destination stream.
    // Note that an error reading does not result in an error status on the writer.
    // Also, if DrError_EndOfStream is encountered, then all of the remaining data in this stream
    // has been written to the writer (may be less than the requested number of bytes).
    DrError ReadBytesIntoWriter(DrMemoryWriter *pWriter, Size_t length);
    
    // Skips data, advancing the current read pointer.
    // Handles simple case inline. Delegates cross-block cases to CrossBlockReadBytes.
    // Returns DrError_EndOfStream (and sets status) if the stream reaches the end before the specified length can be skipped (partial data
    // is still skipped).
    inline DrError SkipBytes(Size_t length)
    {
        if (status != DrError_OK) {
            return status;
        } else if (length > NumContiguousBytesRemaining()) {
            return CrossBlockSkipBytes(length);
        } else {
            pData += length;
            return DrError_OK;
        }
    }

    inline DrError AdvanceToNonemptyPeekBlock()
    {
        while (status == DrError_OK && NumContiguousBytesRemaining() == 0) {
            AdvanceToNextPeekBlock();
        }
        return status;
    }
    
    // Reads data from memory, without advancing the current read pointer. If the peek is
    // cross-block, copies the data into a temporary memory space and returns the copy; otherwise,
    // returns a pointer directly into the source block. The returned pointer is valid until
    // this DrMemoryReader is destroyed.
    // Handles simple case inline. Delegates cross-block cases to CrossBlockPeekBytes.
    // Returns NULL if the stream reaches the end before all data can be read (partial data
    // is still written into the byte array).
    inline DrError PeekBytes(Size_t length, /* out */ const BYTE **ppBytes)
    {
        *ppBytes = NULL;
        if (length > 0) {
            AdvanceToNonemptyPeekBlock();
        }
        if (status != DrError_OK) {
            return status;
        } else {
            *ppBytes = pData;
            return DrError_OK;
        }
    }

    // Reads data from memory, advancing the current read pointer. If the read is
    // cross-block, copies the data into a temporary memory space and returns the copy; otherwise,
    // returns a pointer directly into the source block. The returned pointer is valid until
    // this DrMemoryReader is destroyed.
    // Handles simple case inline. Delegates cross-block cases to CrossBlockReadBytes.
    // Returns NULL if the stream reaches the end before all data can be read (partial data
    // is still written into the byte array).
    inline DrError ReadBytes(Size_t length, /* out */ const BYTE **ppBytes)
    {
        *ppBytes = NULL;
        if (length > 0) {
            AdvanceToNonemptyPeekBlock();
        }
        if (status != DrError_OK) {
            return status;
        } else {
            *ppBytes = pData;
            pData += length;
            return DrError_OK;
        }
    }

    /* read values of different types. Note that once an error is returned, the pointer does not advance
    and all subsequent reads return an error. */

    inline DrError ReadByte(/* out */ BYTE *pVal)
    {
        return ReadBytes(pVal, sizeof(*pVal));
    }

    inline DrError ReadChar(/* out */ char *pVal)
    {
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadInt8(/* out */ Int8 *pVal)
    {
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadInt16(/* out */ Int16 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadInt32(/* out */ Int32 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadInt64(/* out */ Int64 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadUInt8(/* out */ UInt8 *pVal)
    {
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadUInt16(/* out */ UInt16 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadUInt32(/* out */ UInt32 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadUInt64(/* out */ UInt64 *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadUInt32ToSize_t(/* out */Size_t *pVal)
    {
        //assumes little endian
        UInt32 val = 0;
        DrError ret = ReadUInt32(&val);
        *pVal = (Size_t)val;
        return ret;
    }

    // This will return DrError_InvalidProperty if the encoded UInt64 will not fit in a Size_t.
    inline DrError ReadUInt64ToSize_t( /* out */ Size_t *pVal)
    {
        //assumes little endian
        *pVal = 0;
        UInt64 val;
        DrError ret = ReadUInt64(&val);
        if (ret == DrError_OK) {
            if ((UInt64)(Size_t)val == val) {
                *pVal = (Size_t)val;
            } else {
                // too big for Size_t
                ret = SetStatus(DrError_InvalidProperty);
            }
        }
        return ret;
    }

    inline DrError ReadFloat(/* out */ float *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadDouble(/* out */ double *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadGuid(/* out */ GUID *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadBool(/* out */ bool *pVal)
    {
        UInt8 v;

        if (ReadBytes((BYTE *)(void *)&v, sizeof(v)) == DrError_OK) {
            *pVal = (v != 0);
        }
        return status;
    }

    inline DrError ReadDrError(/* out */ DrError *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadTimeStamp(/* out */ DrTimeStamp *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError ReadTimeInterval(/* out */ DrTimeInterval *pVal)
    {
        // assumes little endian
        return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    // Reads data into a preallocated caller-owned buffer
    // just here for netlib naming convention
    inline DrError ReadData(UInt32 length, /* out */ void *data)
    {
        return ReadBytes((BYTE *)data, (Size_t)length);
    }

    // Returns a pointer into the buffer, or into a temporary copy.
    inline DrError ReadData(UInt32 length, /* out */ const void **ppData)
    {
        return ReadBytes((Size_t)length, (const BYTE **)ppData);
    }

    inline DrError PeekByte(/* out */ BYTE *pVal)
    {
        return PeekBytes(pVal, sizeof(*pVal));
    }

    inline DrError PeekInt8(/* out */ Int8 *pVal)
    {
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekInt16(/* out */ Int16 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekInt32(/* out */ Int32 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekInt64(/* out */ Int64 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekUInt8(/* out */ UInt8 *pVal)
    {
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekUInt16(/* out */ UInt16 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekUInt32(/* out */ UInt32 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekUInt64(/* out */ UInt64 *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekFloat(/* out */ float *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekDouble(/* out */ double *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekGuid(/* out */ GUID *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekBool(/* out */ bool *pVal)
    {
        UInt8 v;

        if (PeekBytes((BYTE *)(void *)&v, sizeof(v)) == DrError_OK) {
            *pVal = (v != 0);
        }
        return status;
    }

    inline DrError PeekDrError(/* out */ DrError *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekTimeStamp(/* out */ DrTimeStamp *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    inline DrError PeekTimeInterval(/* out */ DrTimeInterval *pVal)
    {
        // assumes little endian
        return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
    }

    // Peeks data into a preallocated caller-owned buffer
    // just here for netlib naming convention
    inline DrError PeekData(UInt32 length, /* out */ void *data)
    {
        return PeekBytes((BYTE *)data, (Size_t)length);
    }

    // Returns a pointer into the buffer, or into a temporary copy.
    inline DrError PeekData(UInt32 length, /* out */ const void **ppData)
    {
        return PeekBytes((Size_t)length, (const BYTE **)ppData);
    }

    inline DrError SkipByte()
    {
        return SkipBytes(sizeof(BYTE));
    }

    inline DrError SkipInt8()
    {
        return SkipBytes(sizeof(Int8));
    }

    inline DrError SkipInt16()
    {
        return SkipBytes(sizeof(Int16));
    }

    inline DrError SkipInt32()
    {
        return SkipBytes(sizeof(Int32));
    }

    inline DrError SkipInt64()
    {
        return SkipBytes(sizeof(Int64));
    }

    inline DrError SkipUInt8()
    {
        return SkipBytes(sizeof(UInt8));
    }

    inline DrError SkipUInt16()
    {
        return SkipBytes(sizeof(UInt16));
    }

    inline DrError SkipUInt32()
    {
        return SkipBytes(sizeof(UInt32));
    }

    inline DrError SkipUInt64()
    {
        return SkipBytes(sizeof(UInt64));
    }

    inline DrError SkipFloat()
    {
        return SkipBytes(sizeof(float));
    }

    inline DrError SkipDouble()
    {
        return SkipBytes(sizeof(double));
    }

    inline DrError SkipGuid()
    {
        // assumes little endian
        return SkipBytes(sizeof(GUID));
    }

    inline DrError SkipBool()
    {
        return SkipBytes(sizeof(UInt8));
    }

    inline DrError SkipDrError()
    {
        return SkipBytes(sizeof(DrError));
    }

    inline DrError SkipTimeStamp()
    {
        // assumes little endian
        return SkipBytes(sizeof(DrTimeStamp));
    }

    inline DrError SkipTimeInterval()
    {
        // assumes little endian
        return SkipBytes(sizeof(DrTimeInterval));
    }

    inline DrError SkipData(UInt32 length)
    {
        return SkipBytes((Size_t)length);
    }

    /*
       Property marshalling methods
    */

    // ReadNextPropertyTag. Reads the next property from the bag, along
    // with its length (either 1- or 4-byte depending on the length
    // bit in the property name). Returns DrError_EndOfStream if there is not enough
    // data in the bag to read out the property name and length, but
    // does not check that there are *pDataLen more bytes remaining.
    DrError ReadNextPropertyTag(
        /* out */ UInt16 *pEnumId,
        /* out */ UInt32 *pDataLen);
    
    // PeekNextPropertyTag. Peeks at the next property from the bag, along
    // with its length (either 1- or 4-byte depending on the length
    // bit in the property name). Returns DrError_EndOfStream if there is not enough
    // data in the bag to read out the property name and length, but
    // does not check that there are *pDataLen more bytes remaining.
    DrError PeekNextPropertyTag(
        /* out */ UInt16 *pEnumId,
        /* out */ UInt32 *pDataLen);

    // ReadNextProperty: Reads the next property in the bag and fills
    // in its name and length to pEnumId and pDataLen respectively,
    // placing the read pointer after the property value. 
    // Returns a pointer to the contiguous property value (either in
    // the buffer or copied to make it contiguous).If ReadNextProperty returns an error,
    // the position of the read pointer and values of *pEnumId,
    // *pDataLen and *data are undefined.
    inline DrError ReadNextProperty(
        /* out */ UInt16 *pEnumId,
        /* out */ UInt32 *pDataLen,
        /* out */ const void **data)
    {
        if (ReadNextPropertyTag(pEnumId, pDataLen) == DrError_OK) {
            ReadData(*pDataLen, data);
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
    DrError PeekNextProperty(
        /* out */ UInt16 *pEnumId,
        /* out */ UInt32 *pDataLen,
        /* out */ const void **data);

    // SkipNextProperty: Skips the next property.
    inline DrError SkipNextProperty()
    {
        UInt16 enumId;
        UInt32 dataLen;

        if (ReadNextPropertyTag(&enumId, &dataLen) == DrError_OK) {
            SkipData(dataLen);
        }

        return status;
    }

    // ReadNextKnownProperty: Reads the next property which is of
    // known ID and length into a preallocated buffer, placing the read pointer
    // after the property value. If ReadNextKnownProperty returns error,
    // the position of the read pointer is undefined.
    // returns DrError_InvalidProperty if the enum id or length don't match.
    DrError ReadNextKnownProperty(
        UInt16 enumId,
        UInt32 dataLen,
        void *pDest);

    // PeekNextKnownProperty: Peeks at the next property which is of
    // known ID and length into a preallocated buffer.
    // returns DrError_InvalidProperty if the enum id or length don't match.
    DrError PeekNextKnownProperty(
        UInt16 enumId,
        UInt32 dataLen,
        void *pDest);


    // ReadNextProperty: Returns DrError_OK if the next property in the bag
    // is enumId and is well-formed. If so, pDataLen is filled in with
    // its length, *data points to the contiguous property value data,
    // and the read pointer is advanced to just after the value data. If
    // ReadNextProperty returns an error, the position of the read
    // pointer and values of *pEnumId, *pDataLen and *data are
    // undefined.
    // Returns DrError_EndOfStream if there is not a full property remaining
    // Returns DrError_InvalidProperty if the tag doesn't match
    inline DrError ReadNextProperty(
        UInt16 targetId,
        /* out */ UInt32 *pDataLen,
        /* out */ const void **data)
    {
        UInt16 enumId;

        if (ReadNextProperty(&enumId, pDataLen, data) == DrError_OK) {
            if (enumId != targetId) {
                SetStatus(DrError_InvalidProperty);
            }
        }

        return status;
    }

    // The following methods each return DrError_OK if the next
    // property in the bag is enumId, is of the length of the relevant
    // type, and is well-formed, in which case the property value is
    // filled in to *pValue and the read pointer is left after the
    // property value data. If ReadNextProperty returns an error, the
    // position of the read pointer is undefined but *pValue is
    // guaranteed to be unmodified.

    
    inline DrError ReadNextEmptyProperty(UInt16 enumId)
    {
        return ReadNextKnownProperty(enumId, 0, NULL);
    }

    inline DrError ReadNextInt8Property(UInt16 enumId, /* out */ Int8 *pValue)
    {
        return ReadNextKnownProperty(enumId, sizeof(Int8), pValue);
    }

    inline DrError ReadNextInt16Property(UInt16 enumId, /* out */ Int16 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int16), pValue);
    }

    inline DrError ReadNextInt32Property(UInt16 enumId, /* out */ Int32 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int32), pValue);
    }

    inline DrError ReadNextInt64Property(UInt16 enumId, /* out */ Int64 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int64), pValue);
    }

    inline DrError ReadNextUInt8Property(UInt16 enumId, /* out */ UInt8 *pValue)
    {
        return ReadNextKnownProperty(enumId, sizeof(UInt8), pValue);
    }

    inline DrError ReadNextUInt16Property(UInt16 enumId, /* out */ UInt16 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt16), pValue);
    }

    inline DrError ReadNextUInt32Property(UInt16 enumId, /* out */ UInt32 *pValue)
    {
        //assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt32), pValue);
    }

    inline DrError ReadNextUInt64Property(UInt16 enumId, /* out */ UInt64 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt64), pValue);
    }

    inline DrError ReadNextUInt32PropertyToSize_t(UInt16 enumId, /* out */ Size_t *pValue)
    {
        //assumes little endian
        UInt32 val = 0;
        DrError ret = ReadNextUInt32Property(enumId, &val);
        *pValue = (Size_t)val;
        return ret;
    }

    // This will return DrError_InvalidProperty if the encoded UInt64 will not fit in a Size_t.
    inline DrError ReadNextUInt64PropertyToSize_t(UInt16 enumId, /* out */ Size_t *pValue)
    {
        //assumes little endian
        *pValue = 0;
        UInt64 val;
        DrError ret = ReadNextUInt64Property(enumId, &val);
        if (ret == DrError_OK) {
            if ((UInt64)(Size_t)val == val) {
                *pValue = (Size_t)val;
            } else {
                // too big for Size_t
                ret = SetStatus(DrError_InvalidProperty);
            }
        }

        return ret;
    }

    inline DrError ReadNextFloatProperty(UInt16 enumId, /* out */ float *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(float), pValue);
    }

    inline DrError ReadNextDoubleProperty(UInt16 enumId, /* out */ double *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(double), pValue);
    }

    inline DrError ReadNextGuidProperty(UInt16 enumId, /* out */ GUID *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(GUID), pValue);
    }

    inline DrError ReadNextBoolProperty(UInt16 enumId, /* out */ bool *pValue)
    {
        UInt8 v;
        if (ReadNextKnownProperty(enumId, sizeof(UInt8), &v) == DrError_OK) {
            *pValue = (v != 0);
        }
        return status;
    }

    inline DrError ReadNextDrErrorProperty(UInt16 enumId, /* out */ DrError *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(DrError), pValue);
    }

    inline DrError ReadNextTimeStampProperty(UInt16 enumId, /* out */ DrTimeStamp *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(DrTimeStamp), pValue);
    }

    inline DrError ReadNextTimeIntervalProperty(UInt16 enumId, /* out */ DrTimeInterval *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(DrTimeInterval), pValue);
    }

    // Reads a string property that has been encoded with WriteStringProperty.
    // If the string in the stream is longer than maxLength (not including null), DrError_StringTooLong is returned
    DrError ReadNextStringProperty(UInt16 enumId, /* out */ const char **ppStr, Size_t maxLength = Max_Size_t);

    /* Read a string property from the buffer into a preallocated buffer.
       If the embedded string is NULL, an empty string is returned.
       If the string in the stream is longer than buffLength (including null), DrError_StringTooLong is returned
    */
    DrError ReadNextStringProperty(UInt16 enumId, char *pStr, Size_t buffLength);
    
    // Reads a string property that has been encoded with WriteStringProperty.
    // If the string in the stream is longer than maxLength (not including null), DrError_StringTooLong is returned
    DrError ReadOrAppendNextStringProperty(bool fAppend, UInt16 enumId, /* out */ DrStr& strOut, Size_t maxLength = Max_Size_t);

    // The following methods each return DrError_OK if the next
    // property in the bag is enumId, is of the length of the relevant
    // type, and is well-formed, in which case the property value is
    // filled in to *pValue. If PeekNextProperty returns an error, *pValue is
    // guaranteed to be unmodified.

    inline DrError PeekNextEmptyProperty(UInt16 enumId)
    {
        return PeekNextKnownProperty(enumId, 0, NULL);
    }

    inline DrError PeekNextInt8Property(UInt16 enumId, /* out */ Int8 *pValue)
    {
        return PeekNextKnownProperty(enumId, sizeof(Int8), pValue);
    }

    inline DrError PeekNextInt16Property(UInt16 enumId, /* out */ Int16 *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(Int16), pValue);
    }

    inline DrError PeekNextInt32Property(UInt16 enumId, /* out */ Int32 *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(Int32), pValue);
    }

    inline DrError PeekNextInt64Property(UInt16 enumId, /* out */ Int64 *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(Int64), pValue);
    }

    inline DrError PeekNextUInt8Property(UInt16 enumId, /* out */ UInt8 *pValue)
    {
        return PeekNextKnownProperty(enumId, sizeof(UInt8), pValue);
    }

    inline DrError PeekNextUInt16Property(UInt16 enumId, /* out */ UInt16 *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(UInt16), pValue);
    }

    inline DrError PeekNextUInt32Property(UInt16 enumId, /* out */ UInt32 *pValue)
    {
        //assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(UInt32), pValue);
    }

    inline DrError PeekNextUInt64Property(UInt16 enumId, /* out */ UInt64 *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(UInt64), pValue);
    }

    inline DrError PeekNextFloatProperty(UInt16 enumId, /* out */ float *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(float), pValue);
    }

    inline DrError PeekNextDoubleProperty(UInt16 enumId, /* out */ double *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(double), pValue);
    }

    inline DrError PeekNextGuidProperty(UInt16 enumId, /* out */ GUID *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(GUID), pValue);
    }

    inline DrError PeekNextBoolProperty(UInt16 enumId, /* out */ bool *pValue)
    {
        UInt8 v;
        if (PeekNextKnownProperty(enumId, sizeof(UInt8), &v) == DrError_OK) {
            *pValue = (v != 0);
        }
        return status;
    }

    inline DrError PeekNextDrErrorProperty(UInt16 enumId, /* out */ DrError *pValue)
    {
        //assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(DrError), pValue);
    }

    inline DrError PeekNextTimeStampProperty(UInt16 enumId, /* out */ DrTimeStamp *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(DrTimeStamp), pValue);
    }

    inline DrError PeekNextTimeIntervalProperty(UInt16 enumId, /* out */ DrTimeInterval *pValue)
    {
        // assumes little endian
        return PeekNextKnownProperty(enumId, sizeof(DrTimeInterval), pValue);
    }

    // Consumes the (BeginTag, desiredTagType) property and closing (EndTag, desiredTagType) property, and calls you back
    // on parser->OnParseProperty() for each decoded property.  Each property it calls you back on has only been peeked,
    // so you will need to read or skip over it.  If another BeginTag appears, you will be called back with that.
    DrError ReadAggregate(UInt16 desiredTagType, DrPropertyParser *parser, void *cookie);

    // If the next property is not a BeginTag, it simply skips it.
    // If the next property is a BeginTag, then it skips everything through and including the EndTag,
    // and handles recursion
    DrError SkipNextPropertyOrAggregate();

public:
    // Deprecated methods
    // deprecated, use ReadNextEmptyProperty 
    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId)
    {
        return ReadNextKnownProperty(enumId, 0, NULL);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ Int8 *pValue)
    {
        return ReadNextKnownProperty(enumId, sizeof(Int8), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ Int16 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int16), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ Int32 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int32), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ Int64 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(Int64), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ UInt8 *pValue)
    {
        return ReadNextKnownProperty(enumId, sizeof(UInt8), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ UInt16 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt16), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ UInt32 *pValue)
    {
        //assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt32), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ UInt64 *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(UInt64), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ float *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(float), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ double *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(double), pValue);
    }

    inline DRDEPRECATED_UNTYPED DrError ReadNextProperty(UInt16 enumId, /* out */ GUID *pValue)
    {
        // assumes little endian
        return ReadNextKnownProperty(enumId, sizeof(GUID), pValue);
    }

    // Reads a string property that has been encoded with WriteStringProperty.
    // If the string in the stream is longer than maxLength (not including null), DrError_StringTooLong is returned
    // NOTE: may be deprecated, please use ReadNextDrStrProperty to disambiguate
    DrError ReadNextStringProperty(UInt16 enumId, /* out */ DrStr& strOut, Size_t maxLength = Max_Size_t)
    {
        return ReadOrAppendNextStringProperty(false, enumId, strOut, maxLength);
    }

protected:
    // protected base-class methods
    
    // Must be subclassed, cannot be constructed directly:
    DrMemoryReader();

    // It is the responsibility of the subclass to deal with the
    // case where the reader may not have been closed at destruct time, and clean up without crashing.
    // Calling MemoryReaderDescructorClose is usually a good way to to that
    void MemoryReaderDestructorClose();
    
    // true if the underlying data is immutable and refcounted, so that a DrMemoryBuffer can be wrapped around a portion of it.
    __forceinline void SetMemoryReaderAllowsCopyByReference(bool fMemoryReaderAllowsCopyByReference)
    {
        fAllowCopyBufferByReference = fMemoryReaderAllowsCopyByReference;
    }
    
public:
    // virtual destructor:

    // It is the responsibility of the subclass to deal with the
    // case where the reader may not have been closed at destruct time, and clean up without crashing.
    // Calling MemoryReaderDescructorClose is usually a good way to to that
    virtual ~DrMemoryReader();

protected:
    // Protected subclass-overridable methods

    // Clears all buffer context values to initial defaults (including settting the physical stream position to 0
    // and discarding peekahead buffers), but does not discard temporary results or change the current status code
    // or "closed" status.
    // may be overridden by a subclass if it needs to free resources controlled by the
    // context pointers before delegating to this method implementation.
    virtual void DiscardMemoryReaderContext();

    // This method clears all buffer context values to initial defaults (including settting the physical stream position to 0,
    // discarding peekahead buffers and temporary results, clearing the "closed" state, and setting the status to DrError_OK.
    // a subclass may override this to reset its own state along with forwarding the request to this class.
    // Calls through the virtual DiscardMemoryReaderContext() before clearing the status and the closed flag.
    virtual void ResetMemoryReader();

    // This method should be overidden by memory readers that know how to advance to a new block.
    // Returns DrError_EndOfStream if there are no more blocks to be read.
    // The default implementation always returns DrError_EndOfStream, which is appropriate for
    // single-block readers.
    virtual DrError ReadNextBlock(/* out */ const BYTE **ppBytes, /* out */ Size_t *pLength);

    // Reads data from blocks starting *after* the current block, without advancing the current read pointer.
    // Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
    // may still be written into the byte array).
    // Returns DrError_PeekTooFar if the underlying implemntation cannot peek that far forward (in this
    // case, we will directly read the data and append it to a cached peek list).
    virtual DrError FutureBlockPeekBytes(/* out */ void *pBytes, Size_t length);

    // This method should be overridden by memory readers that know how to advance to a new block.
    // The implementation should return true if there are at least "length" readable bytes
    // following the current block.
    //
    // The default implementation always returns false, which is appropriate for single-block readers
    virtual bool FutureBlocksCanBeRead(Size_t length);

private:
    // private methods

    // called by close + destructor. Discards temporary results and peek blocks.
    void InternalFree();

    // Discards any lookahead peek blocks that have been allocated.
    // This may invalidate the current block, so the current block should always be
    // reinitialized after calling this.
    void DiscardPeekBlocks();
    
    // This method determines if a number of bytes can successfully be read, handling
    // the case where the data will cross blocks. 
    inline bool CrossBlockCanBeRead(Size_t length)
    {
        Size_t nr = NumContiguousBytesRemaining();
        if (length <= nr) {
            return true;
        } else {
            return FutureBlocksCanBeRead(length - nr);
        }
    }

    void AllocTempMemBlock(Size_t minLength);

    // Reserves a block of temporary memory that will be valid until this DrMemoryReader
    // is destroyed.
    BYTE *ReserveTempMemory(Size_t length);

    // Reads data from memory without advancing the current read pointer.
    // Handles cross-block cases.
    // Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
    // is still wriiten into the byte array).
    DrError CrossBlockPeekBytes(/* out */ BYTE *pBytes, Size_t length);

    // Reads data from memory, advancing the current read pointer.
    // Handles cross-block cases.
    // Returns DrError_EndOfStream if the stream reaches the end before all data can be read (partial data
    // is still wriiten into the byte array).
    DrError CrossBlockReadBytes(/* out */ BYTE *pBytes, Size_t length);

    // Skips data in memory, advancing the current read pointer.
    // Handles cross-block cases.
    // Returns DrError_EndOfStream if the stream reaches the end before all data can be skipped (partial data
    // is still skipped).
    DrError CrossBlockSkipBytes(Size_t length);

    // Appends the next block from the underlying stream to the list of peekable data blocks.
    DrError AppendNextBlock();

    // Advances the current block to the next available peek block, reading a new block if necessary
    DrError AdvanceToNextPeekBlock();

private:
    // Private type definitions
    
    // TempMemHeader allows us to maintain a stack of temporarily allocated
    // return data (e.g., strings) that is cleaned up when the DrMemoryReader
    // is destroyed.
    class TempMemHeader
    {
    private:
        TempMemHeader *pNext;
        BYTE *pData;
        Size_t length;

        // We override "new" to allocate the header and the content
        // in a single allocation.
        inline void *operator new(Size_t headersize, Size_t blocksize)
        {
            LogAssert(headersize == sizeof(TempMemHeader));
            LogAssert(headersize + blocksize >= headersize);  // keep prefast happy
            void *p = malloc(headersize + blocksize);
            LogAssert(p != NULL);
            return p;
        }

        inline TempMemHeader(Size_t blocksize,  TempMemHeader *pOldHead)
        {
            pData = ((BYTE *)(void *)this) + sizeof(*this);
            length = blocksize;
            pNext = pOldHead;
        }

    public:
        // We have to provide a matching delete...
        inline void operator delete(void *pMem, Size_t  blocksize)
        {
            (void)blocksize;
            free(pMem);
        }

        inline static TempMemHeader *Alloc(Size_t blocksize, TempMemHeader  *pOldHead)
        {
            TempMemHeader *p = new(blocksize) TempMemHeader(blocksize, pOldHead);
            return p;
        }

        inline TempMemHeader *Detach()
        {
            TempMemHeader *p = pNext;
            pNext = NULL;
            return p;
        }

        inline ~TempMemHeader()
        {
            LogAssert(pNext == NULL);
        }

        inline BYTE *GetData()
        {
            return pData;
        }

        inline Size_t GetLength()
        {
            return length;
        }

        inline BYTE *ReserveData(Size_t len)
        {
            LogAssert(len <= length);
            BYTE *pd = pData;
            pData += len;
            length -= len;
            return pd;
        }

    };

    // PeekMemHeader allows us to maintain a queue of lookahead data
    // for streams that don't support peek
    class PeekMemHeader
    {
    private:
        PeekMemHeader *pNext;
        BYTE *pData;
        Size_t length;
        bool   fOwnMemory;       // True if the block at pData is owned by this object and should be deleted

    public:
        // If pBytes is not null, we don't own the memory block. If pBytes is NULL, a block is allocated that we own.
        inline PeekMemHeader(Size_t blocksize, PeekMemHeader *pOldTail, const void *pBytes = NULL)
        {
            fOwnMemory = (pBytes == NULL);
            if (fOwnMemory) {
                #pragma prefast(disable:419, "Don't need to check blocksize")
                pData = (BYTE *)malloc(blocksize);
                #pragma prefast(enable:419, "End prefast suppression")
                LogAssert(pData != NULL);
            } else {
                pData = (BYTE *)(void *)pBytes;
            }
            length = blocksize;
            pNext = NULL;
            if (pOldTail != NULL) {
                pOldTail->pNext = this;
            }
        }

        inline void EnsureIsOwned()
        {
            if (!fOwnMemory) {
                BYTE *pNew = (BYTE *)malloc(length);
                LogAssert(pNew != NULL);
                memcpy(pNew, pData, length);
                pData = pNew;
                fOwnMemory = true;
            }
        }

        inline PeekMemHeader *Detach()
        {
            PeekMemHeader *p = pNext;
            pNext = NULL;
            return p;
        }

        inline ~PeekMemHeader()
        {
            LogAssert(pNext == NULL);
            if (fOwnMemory) {
                free(pData);
            }
        }

        inline BYTE *GetData()
        {
            return pData;
        }

        inline Size_t GetLength()
        {
            return length;
        }

        inline PeekMemHeader *GetNext()
        {
            return pNext;
        }

    };

    // End private type definitions
    
private:
    // private member data
    static const Size_t DEFAULT_TEMP_MEM_ALLOC_SIZE = 16384;

    TempMemHeader *pFirstTempMemHeader;
    PeekMemHeader *pFirstPeekMemHeader;
    PeekMemHeader *pLastPeekMemHeader;

    // true if CloseMemoryReader has already been called or is currently running
    bool m_fMemoryReaderIsClosed;
    
    // true if the underlying data is immutable and refcounted, so that a DrMemoryBuffer can be wrapped around a portion of it.
    bool fAllowCopyBufferByReference;
};


// A simple writer that can only write into a fixed-size, ungrowable contiguous block of memory.
class DrSingleBlockWriter : public DrMemoryWriter
{
public:
    inline DrSingleBlockWriter(void *pBytes, Size_t length)
    {
        pData = pBlockBase = (BYTE *)pBytes;
        blockLength = length;
    }
};

// A simple reader that can only read from a single contiguous block of memory.
class DrSingleBlockReader : public DrMemoryReader
{
public:
    inline DrSingleBlockReader(const void *pBytes, Size_t length)
    {
        pData = pBlockBase = (BYTE *)(void *)pBytes;
        blockLength = length;
    }
};

// A simple writer that can write into a potentially growable DrMemoryBuffer
class DrMemoryBufferWriter : public DrMemoryWriter
{
private:
    DrRef<DrMemoryBuffer> m_pBuffer;   // The discontiguous memory buffer we are writing to
    bool m_fTruncateOnFlush;      // True if the memory buffer we are writing to should be truncated to our current write position when we flush

public:
    inline DrMemoryBufferWriter(DrMemoryBuffer *pMemoryBuffer, bool fTruncateOnFlush = true)
    {
        m_pBuffer = pMemoryBuffer;
        m_fTruncateOnFlush = fTruncateOnFlush;
    }

    inline DrMemoryBufferWriter()
    {
        m_fTruncateOnFlush = true;
    }

    // Set the current writing position in the buffer. Increases the current available size in the buffer if necessary.
    // The physical stream position is adjusted to be consistent relative to the current "stream origin", if possible.
    // Since the default physical stream position at the beginning of the buffer is 0, this means that the physical stream
    //   position by default becomes equal to the "offset" parameter.
    // If the change would cause the current physical stream position to become negative,
    //   then the current physical stream position is set to 0. This could happen if, e.g., you wer at
    //   buffer offset 50, then you explicitly set the physical stream position to 0, then you set the buffer offset back to 0.
    DrError SetBufferOffset(Size_t offset);

    Size_t GetBufferOffset()
    {
        return (Size_t)GetPhysicalStreamPosition();
    }
    
    virtual DrError FlushMemoryWriter();

    // NOTE: after you call this method, the internal refcount to the buffer will be released.
    virtual DrError CloseMemoryWriter();

    virtual ~DrMemoryBufferWriter();

protected:
    virtual DrError AdvanceToNextBlock();

    virtual bool FutureBlocksCanBeWritten(Size_t length);

private:
    void InternalFree();
};

// a simple reader that can read from a refcounted DrMemoryBuffer
class DrMemoryBufferReader : public DrMemoryReader
{
private:
    DrRef<DrMemoryBuffer> m_pBuffer;   // The discontiguous memory buffer we are reading from (refcounted)
    Size_t nextReadOffset;    // The  offset within the DrMemoryBuffer that should be used for the next call to ReadNextBlock

public:
    inline DrMemoryBufferReader(DrMemoryBuffer *pMemoryBuffer, bool fAllowCopyByReference = false)
    {
        SetMemoryReaderAllowsCopyByReference(fAllowCopyByReference);
        nextReadOffset = 0;
        m_pBuffer = pMemoryBuffer;
    }

    // If true, references to the provided buffer may be handled out to the reading app rather
    // than making copies.
    void SetAllowCopyByReference(bool fAllow=true)
    {
        SetMemoryReaderAllowsCopyByReference(fAllow);
    }

    // Set the current reading position in the buffer. Has no effect if there is an error status
    DrError SetBufferOffset(Size_t offset);

    Size_t GetBufferOffset()
    {
        return (Size_t)uBlockBasePhysicalStreamPosition + NumContiguousBytesRead();
    }

    // returns the total number of bytes remaining to be read in the buffer
    Size_t GetTotalAvailableBufferRemaining()
    {
        if (status == DrError_OK) {
            Size_t available = m_pBuffer->GetAvailableSize();
            Size_t used = GetBufferOffset();
            LogAssert(available >= used);
            return available - used;
        } else {
            return 0;
        }
    }

    // Default implementation just frees any temporary buffers and returns the reader status.
    // If this is the first time CloseMemoryReader() has been called, then resources are freed regardless of
    // the current status, allowing this method to be used to free resources even on failed streams.
    // The current status is always returned.
    // Unless MemoryStreamIsClosed(), subclasses that override this method should *always* delegate to their parent class
    // after freeing their own resources, and should *always* return the current status at completion.
    // NOTE: this method frees the reference to the buffer that is being read from
    virtual DrError CloseMemoryReader();
    
    virtual ~DrMemoryBufferReader();

protected:
    virtual DrError ReadNextBlock(/* out */ const BYTE **ppBytes, /* out */ Size_t *pLength);

    virtual DrError FutureBlockPeekBytes(/* out */ void *pBytes, Size_t length);
    virtual bool FutureBlocksCanBeRead(Size_t length);

private:
    void InternalFree();
    
};



#pragma warning (pop)
