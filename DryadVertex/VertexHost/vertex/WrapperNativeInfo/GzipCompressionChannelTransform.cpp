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

#include <gzipcompressionchanneltransform.h>
#include <wrappernativeinfo.h>

#pragma unmanaged 

#ifdef LINKWITHZLIB

static const UInt32 s_defaultWriteSize = 256 * 1024;

GzipCompressionChannelTransform::GzipCompressionChannelTransform(DryadVertexProgram* vertex,
                                                                 bool gzipHeader,
                                                                 bool optimizeForSpeed)
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
    
    m_crc = crc32(0L, Z_NULL, 0);
    m_crcStart = NULL;
    m_channel = NULL;
    m_writeSize = 0; /* try to infer this from the first item to process */
    m_zlibArg = Z_NO_FLUSH;
    m_firstReadProcessed = false;
    m_gzipHeader = gzipHeader;
    m_optimizeForSpeed = optimizeForSpeed;
    m_vertex = vertex;
}

GzipCompressionChannelTransform::~GzipCompressionChannelTransform()
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
    
    m_crc = crc32(0L, Z_NULL, 0);
    m_crcStart = NULL;
    m_channel = NULL;
}

void GzipCompressionChannelTransform::AllocateOutputBuffer() 
{
    LogAssert(m_writeSize > 0);
    m_outputBuffer.Attach(new DataBlockItem(m_writeSize));
    m_stream.next_out = (byte *) m_outputBuffer->GetDataAddress();
    LogAssert(m_outputBuffer.Ptr() != NULL);
    m_stream.avail_out = (z_uInt) m_writeSize;
}

inline void GzipCompressionChannelTransform::AppendOutputByte(byte value) 
{
    //LogAssert(m_stream.avail_out > 0);
    if (m_stream.avail_out == 0) 
    {
    WriteOutputBuffer();
    AllocateOutputBuffer();
    }
    m_stream.avail_out--;
    *(m_stream.next_out) = value;
    m_stream.next_out++;
}

inline void GzipCompressionChannelTransform::IncrementOutputPosition(UInt32 increment) 
{
    LogAssert(m_stream.avail_out >= increment);
    m_stream.avail_out -= increment;
    m_stream.next_out += increment;
}  

void GzipCompressionChannelTransform::SetOutputBufferSize(UInt32 bufferSize)
{
    WriteOutputBuffer();
    m_writeSize = bufferSize;
    AllocateOutputBuffer();
}

// Process block should be called with m_critsec held
DrError GzipCompressionChannelTransform::ProcessBlock()
{
    int retVal = -1;
    bool done = false;
    while (!done) 
    {
        // record the starting point for the crc
//         DrLogI( "About to deflate",
//             "avail_in %u total_in %u avail_out %u total_out %u arg %u",
//             m_stream.avail_in, m_stream.total_in,
//             m_stream.avail_out, m_stream.total_out, m_zlibArg);
        m_crcStart = m_stream.next_in;
        retVal = deflate(&m_stream, m_zlibArg);
//         DrLogI( "Done deflate",
//             "ret %u", retVal);
        if (m_gzipHeader)
        {
            m_crc = crc32(m_crc, m_crcStart,
                          (UInt32) (m_stream.next_in - m_crcStart));
        }
        
        UInt32 *intPtr = NULL;
        switch (retVal) {
        case Z_OK:
            // nothing needed here
            break;
        case Z_STREAM_END:
            done = true;
            if (m_gzipHeader)
            {
                if (m_stream.avail_out < 8)
                {
                    WriteOutputBuffer();
                    AllocateOutputBuffer();
                }
                intPtr = (UInt32 *)m_stream.next_out;
                *intPtr = m_crc;
                intPtr++;
                *intPtr = m_stream.total_in; 
                IncrementOutputPosition(8);
            }
            retVal = deflateEnd(&m_stream);     
            LogAssert(retVal == Z_OK);
            WriteOutputBuffer();
            AllocateOutputBuffer();
            break;
        default:
            char *errorMsg = "NULL";
            if (m_stream.msg != NULL) {
                errorMsg = m_stream.msg;
            }
            DrLogA( "Error in deflate. retVal: %d Message: %s", retVal, errorMsg);
        }
        if (m_stream.avail_out == 0) 
        {
            WriteOutputBuffer();
            AllocateOutputBuffer();
        }
        if (m_stream.avail_in == 0 && m_zlibArg != Z_FINISH)
        {
            done = true;
        }
    }
    return DrError_OK;
}

DrError GzipCompressionChannelTransform::ProcessItem(DataBlockItem *item)
{
    UInt32 inputSize = (UInt32) item->GetAvailableSize();
    if (inputSize == 0)
    {
        /* nothing to do here */
        return DrError_OK;
    }

    {   
        AutoCriticalSection acs(&m_critsec);

        if (m_firstReadProcessed == false)
        {
            if (m_writeSize == 0)
            {
                /* try to infer the write size from the size of the
                   uncompressed data coming in to the compresser,
                   i.e. what the app thought it wanted to write */
                UInt32 itemSize = (UInt32) item->GetAllocatedSize();
                UInt32 inputPages = itemSize / (4 * 1024);
                if (inputPages * 4 * 1024 == itemSize)
                {
                    m_writeSize = itemSize;
                }
                else
                {
                    /* if the block is a weird size then use the
                       default so we don't force the writer to use
                       buffered IO */
                    m_writeSize = s_defaultWriteSize;
                }

                if (m_writeSize > 4 * 1024 * 1024)
                {
                    m_writeSize = 4 * 1024 * 1024;
                }
            }

            m_zlibArg = Z_NO_FLUSH;

            AllocateOutputBuffer();

            if (m_gzipHeader)
            {
                WriteGzipHeader();
            }

            int level = (m_optimizeForSpeed) ?
                (Z_BEST_SPEED) : (Z_DEFAULT_COMPRESSION);
            DrLogI( "Set compression level. Level %s", (m_optimizeForSpeed) ? "faster" : "default");

            int retVal = deflateInit2(&m_stream, level, Z_DEFLATED, 
                                      -MAX_WBITS, 9, Z_DEFAULT_STRATEGY);
            LogAssert(retVal == Z_OK);

            m_firstReadProcessed = true;
        }

        LogAssert(m_zlibArg == Z_NO_FLUSH);
        m_stream.avail_in = (z_uInt) item->GetAvailableSize();
        m_stream.next_in = (z_Bytef *)item->GetDataAddress();
//         DrLogI( "In process item");
        return ProcessBlock();
    }
}

DrError GzipCompressionChannelTransform::Finish(bool atEndOfStream)
{
    {
        DrError retval = DrError_OK;
        AutoCriticalSection acs(&m_critsec);

        if (m_firstReadProcessed)
        {
            m_zlibArg = Z_FINISH;
//         DrLogI( "In finish");
            retval = ProcessBlock();

            m_firstReadProcessed = false;
        }
        return retval;
    }
}

DrError GzipCompressionChannelTransform::Start(FifoChannel *channel)
{
    m_channel = channel;
    return DrError_OK;
}

void GzipCompressionChannelTransform::WriteGzipHeader()
{
    AppendOutputByte(0x1f); // ID1
    AppendOutputByte(0x8b); // ID2 
    AppendOutputByte(8);    // CM
    for (int i = 0; i < 6; i++) {
        AppendOutputByte(0);  // FLAG, MTIME, and XFL
    }
    AppendOutputByte(11);   // OS
}


void GzipCompressionChannelTransform::WriteOutputBuffer() 
{
    DataBlockItem *dbi = (DataBlockItem *)m_outputBuffer.Ptr();
    int bytesToWrite = m_writeSize - m_stream.avail_out;
//     DrLogI( "Writing output buffer",
//         "bytes: %d", bytesToWrite);
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