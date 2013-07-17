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

#include <GzipDecompressionChannelTransform.h>
#include <wrappernativeinfo.h>

#pragma unmanaged 

#ifdef LINKWITHZLIB

GzipDecompressionChannelTransform::GzipDecompressionChannelTransform(DryadVertexProgram* vertex, bool gzipHeader)
{
    m_firstReadProcessed = false;
    m_vertex = vertex;

    // zlib structures
    memset(&m_stream, 0, sizeof(m_stream));
    m_stream.next_in = NULL;
    m_stream.avail_in = 0;
    m_stream.next_out = NULL;
    m_stream.avail_out = 0;
    m_stream.zalloc = Z_NULL;
    m_stream.zfree = Z_NULL;
    m_stream.opaque = NULL;
    
    m_crc = crc32(0L, Z_NULL, 0);
    m_crcStart = NULL;
    m_channel = NULL;
    m_writeSize = 256 * 1024;
    m_trailerBytesFilled = 0;
    m_trailerBytes = new byte[TrailerLength];

    m_gzipHeader = gzipHeader;
}

GzipDecompressionChannelTransform::~GzipDecompressionChannelTransform()
{
    // zlib structures
    memset(&m_stream, 0, sizeof(m_stream));
    m_stream.next_in = NULL;
    m_stream.avail_in = 0;
    m_stream.next_out = NULL;
    m_stream.avail_out = 0;
    m_stream.zalloc = Z_NULL;
    m_stream.zfree = Z_NULL;
    m_stream.opaque = NULL;
    
    m_crc = 0;
    m_crcStart = NULL;
    m_channel = NULL;
    m_outputBuffer = NULL;   
    delete m_trailerBytes;
}

DrError GzipDecompressionChannelTransform::AllocateOutputBuffer() 
{
    DataBlockItem * block = new DataBlockItem(m_writeSize);
    if (m_writeSize > 0 && block == NULL) {
        m_vertex->ReportError(DryadError_ChannelRestart, "Failed to allocate enough memory for decompression; corrupted input stream?");
        DecompressionError();
        return DrError_OutOfMemory;
    }
    m_outputBuffer.Attach(block);
    m_stream.next_out = (byte *) m_outputBuffer->GetDataAddress();
    m_stream.avail_out = (z_uInt) m_writeSize;
    m_crcStart = m_stream.next_out;
    return DrError_OK;
}

DrError GzipDecompressionChannelTransform::Finish(bool atEndOfStream)
{
    if (atEndOfStream)
    {
        WriteOutputBuffer();
    }
    return DrError_OK;
}

inline DrError GzipDecompressionChannelTransform::IncrementInputPosition(UInt32 increment) 
{
    if (m_stream.avail_in >= increment) {
        m_stream.avail_in -= increment;
        m_stream.next_in += increment;
        return DrError_OK;
    }
    else {
        DrLogE( "Buffer overrun during decompression; corrupted input stream?");
        m_vertex->ReportError(DryadError_ChannelRestart, "Buffer overrun during decompression; corrupted input stream?");
        DecompressionError();
        return DrError_Fail;
    }
}  

DrError GzipDecompressionChannelTransform::ProcessItem(DataBlockItem *item)
{
    m_stream.next_in = (byte *) item->GetDataAddress();
    m_stream.avail_in = (Int32) item->GetAvailableSize();
    int retVal = 0;
    DrError err = DrError_OK;

    if (m_trailerBytesFilled != 0) {  // we must have a split trailer
        int bytesToCopy = TrailerLength - m_trailerBytesFilled;
        memcpy(m_trailerBytes + m_trailerBytesFilled, 
               m_stream.next_in, bytesToCopy);
        m_trailerBytesFilled += bytesToCopy;
        err = IncrementInputPosition(bytesToCopy);
        if (err != DrError_OK) {
            return DrError_IoReadWriteError;
        }
        if (m_trailerBytesFilled == TrailerLength) {
            err = ProcessTrailer();
            if (err != DrError_OK) {
                return err;
            }
        }
    }

    while (m_stream.avail_in != 0) {
        if (!m_firstReadProcessed) {
            if (m_gzipHeader)
            {
                // need to read the gzip header
                err = ReadGzipHeader();
            }
            if (err == DrError_OK) {
                m_crcStart = m_stream.next_out;
                retVal = inflateInit2(&m_stream, -MAX_WBITS);
                if (retVal != Z_OK) {
                    DecompressionError();
                    err = DrError_IoReadWriteError;
                    m_vertex->ReportError(DryadError_ChannelRestart, "Error during stream decompression");
                }
            }
            m_firstReadProcessed = true;
        }

        retVal = inflate(&m_stream, Z_SYNC_FLUSH);
        if (m_gzipHeader)
        {
            m_crc = crc32(m_crc, m_crcStart, (UInt32)(m_stream.next_out - 
                                                      m_crcStart));
            m_crcStart = m_stream.next_out;
        }

        switch (retVal) {
        case Z_OK:
            // done with this input block wait for next one to arrive
            break;
        case Z_STREAM_END:
            if (m_gzipHeader)
            {
                if (m_stream.avail_in >= TrailerLength) {
                    memcpy(m_trailerBytes, m_stream.next_in, TrailerLength);
                    err = IncrementInputPosition(TrailerLength);
                    ProcessTrailer();
                } else {
                    memcpy(m_trailerBytes + m_trailerBytesFilled, 
                           m_stream.next_in, m_stream.avail_in);
                    m_trailerBytesFilled = m_stream.avail_in;
                    err = IncrementInputPosition(m_trailerBytesFilled);
                }
            }
            else
            {
                /* if there's more data following, things will get
                   very confused, but that's a malformed stream */
                WriteOutputBuffer();
                err = AllocateOutputBuffer();
            }
            break;
        default:
            char *errorMsg = "NULL";
            if (m_stream.msg != NULL) {
                errorMsg = m_stream.msg;
            }
            DrLogE( "Error in stream inflation: %s", errorMsg);
            err = DrError_IoReadWriteError;
            DecompressionError();
            m_vertex->ReportError(DryadError_ChannelRestart, "Error in stream inflate %s", errorMsg);
        }
                
        if (err == DrError_OK && m_stream.avail_out == 0) {
            WriteOutputBuffer();
            err = AllocateOutputBuffer();
        }

        if (err != DrError_OK)
            // out of the loop
            break;
    }
    return err;
}

DrError GzipDecompressionChannelTransform::ProcessTrailer() 
{
    DrError err = DrError_OK;

    UInt32 *intPtr = (UInt32 *)m_trailerBytes;
    if (m_crc != *intPtr) {
        DecompressionError();
        m_vertex->ReportError(DryadError_ChannelRestart, "Corrupted input stream during decompression");
        DrLogE( "Corrupted input stream during decompression");
        err = DrError_IoReadWriteError;
    }
    intPtr++;
    // NYI - check size against orig size, 
    // which is in intPtr
    //    IncrementInputPosition(8); no longer needed
    int retVal = inflateEnd(&m_stream);
    if (retVal != Z_OK) {
        DecompressionError();
        m_vertex->ReportError(DryadError_ChannelRestart, "Corrupted input stream during decompression");
        DrLogE( "Corrupted input stream during decompression");
        err = DrError_IoReadWriteError;
    }
    m_firstReadProcessed = false; //deal with multiple streams
    m_trailerBytesFilled = 0;
    // in most cases, this is the end of the stream, so 
    // write the output
    if (err == DrError_OK && m_stream.avail_out != m_writeSize) {
        WriteOutputBuffer();
        err = AllocateOutputBuffer();
    }
    return err;
}

DrError GzipDecompressionChannelTransform::ReadGzipHeader() 
{
    if (!(m_stream.avail_in > 10)) {
        goto error;
    }
    bool flagExtra = false;
    bool flagFilename = false;
    bool flagComment = false;
    bool flagFHCrc = false;
    DrError err = DrError_OK;

    // check the 2 id bytes
    if (*(m_stream.next_in) != 0x1f)
    {
        goto error;
    }
    err = IncrementInputPosition(1);
    if (err != DrError_OK)
        goto error;
    if (*(m_stream.next_in) != 0x8b) {
        goto error;
    }
    err = IncrementInputPosition(1);
    if (err != DrError_OK)
        goto error;

    // check the cm
    if (*(m_stream.next_in) != 8) {
        goto error;
    }
    err = IncrementInputPosition(1);
    if (err != DrError_OK)
        goto error;

    // check the flag bits
    if (*(m_stream.next_in) & 0x2) 
    {
        flagFHCrc = true;
    } 
    if (*(m_stream.next_in) & 0x4)
    {
        flagExtra = true;
    } 
    if (*(m_stream.next_in) & 0x8)
    {
        flagFilename = true;
    }
    if (*(m_stream.next_in) & 0x10)
    {
        flagComment = true;
    }
    // check the reserved bits (5-7)
    if (*(m_stream.next_in) & 0xE0){
        //LogAssert(false, "Flag bits 5-7 in the gzip header must be 0");
        goto error;
    }
    err = IncrementInputPosition(1);
    if (err != DrError_OK)
        goto error;
    // skip mtime(4), xfl(1), and OS(1) fields  
    err = IncrementInputPosition(6);
    if (err != DrError_OK)
        goto error;

    if (flagExtra) 
    {
        short* fieldLen = (short *) m_stream.next_in;
        err = IncrementInputPosition(UInt32(2 + *fieldLen));
        if (err != DrError_OK)
            goto error;
    }
    if (flagFilename) 
    {
        while (*(m_stream.next_in) != 0) {
            err = IncrementInputPosition(1);
            if (err != DrError_OK)
                goto error;
        }
        // and skip the terminating 0
        err = IncrementInputPosition(1);
        if (err != DrError_OK)
            goto error;
    }
    if (flagComment) 
    {
        while (*(m_stream.next_in) != 0) {
            err = IncrementInputPosition(1);
            if (err != DrError_OK)
                goto error;
        }
        // and skip the terminating 0
        err = IncrementInputPosition(1);
        if (err != DrError_OK)
            goto error;
    }
    if (flagFHCrc) 
    {
        // just skip the header crc for now
        err = IncrementInputPosition(2);
        if (err != DrError_OK)
            goto error;
    }
    return DrError_OK;

 error:
    m_vertex->ReportError(DryadError_ChannelRestart, "Compression header corrupted");
    DrLogE( "Compression header corrupted");
    DecompressionError();
    return DrError_IoReadWriteError;
}

void GzipDecompressionChannelTransform::DecompressionError()
{
    RChannelItemRef termination;
    termination.Attach(RChannelMarkerItem::Create(RChannelItem_MarshalError, false));
    m_channel->WriteTransformedItem(termination.Ptr());
}

void GzipDecompressionChannelTransform::SetOutputBufferSize(UInt32 bufferSize)
{
    WriteOutputBuffer();
    m_writeSize = bufferSize;
    AllocateOutputBuffer();
}

DrError GzipDecompressionChannelTransform::Start(FifoChannel *channel)
{
    m_channel = channel;
    AllocateOutputBuffer();
    return DrError_OK;
}

void GzipDecompressionChannelTransform::WriteOutputBuffer() 
{
    DataBlockItem *dbi = (DataBlockItem *)m_outputBuffer.Ptr();
    int bytesToWrite = (int) m_writeSize - m_stream.avail_out;
    if (bytesToWrite > 0) {
        dbi->SetAvailableSize(bytesToWrite);
        m_channel->WriteTransformedItem(dbi);
    }
    // for safety, zero the output pointers
    m_stream.next_out = NULL;
    m_stream.avail_out = 0;
    m_outputBuffer = NULL;   
}
#endif