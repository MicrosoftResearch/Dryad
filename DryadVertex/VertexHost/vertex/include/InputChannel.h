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
#include <DrCommon.h>
#include <datablockitem.h>
#include <dryadvertex.h>

class InputChannel 
{
public:
    InputChannel(UInt32 portNum,
                 DryadVertexProgram* vertex, 
                 RChannelReader* channel);
    virtual ~InputChannel();

    virtual void Stop();

    virtual DataBlockItem* ReadDataBlock(byte **ppDataBlock,
                                         Int32 *ppDataBlockSize,
                                         Int32 *pErrorCode);
    virtual bool AtEndOfChannel();
    Int64 GetBytesRead();
    RChannelReader* GetReader();
    virtual bool GetTotalLength(UInt64 *length);
    virtual bool GetExpectedLength(UInt64 *length);
    virtual const char* GetURI();

protected:
    RChannelReader* m_reader;
    DryadVertexProgram* m_vertex;
    Int64 m_bytesRead;
    UInt32 m_portNum;
    bool m_atEOC;
};
