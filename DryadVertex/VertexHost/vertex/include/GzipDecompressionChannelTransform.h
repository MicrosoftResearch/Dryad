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
#include <channeltransform.h>
#include <dryadvertex.h>
#include <fifochannel.h>
#ifndef Z_PREFIX
#define Z_PREFIX
#endif

#ifdef LINKWITHZLIB
#include "zlib.h"

class GzipDecompressionChannelTransform : public ChannelTransform
{
public: 
    GzipDecompressionChannelTransform(DryadVertexProgram * vertex, bool gzipHeader);
    virtual ~GzipDecompressionChannelTransform();
    virtual DrError Start(FifoChannel *channel);
    virtual void SetOutputBufferSize(UInt32 bufferSize);
    virtual DrError ProcessItem(DataBlockItem *item);
    virtual DrError Finish(bool atEndOfStream);

 private:
    inline DrError IncrementInputPosition(UInt32 increment); 
    DrError ReadGzipHeader();
    DrError AllocateOutputBuffer(); 
    void WriteOutputBuffer(); 
    DrError ProcessTrailer();
    void DecompressionError();

    static const int TrailerLength = 8;
    byte *m_trailerBytes;
    UInt32 m_trailerBytesFilled;
    bool m_firstReadProcessed;
    UInt32 m_crc;
    byte *m_crcStart;
    FifoChannel *m_channel;
    Size_t m_writeSize;
    z_stream m_stream;
	DrRef<DataBlockItem> m_outputBuffer;
    bool m_gzipHeader;
};
#endif
