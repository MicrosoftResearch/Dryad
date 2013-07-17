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

class OutputChannel : public RChannelItemWriterHandler
{
public:
    OutputChannel(UInt32 portNum,
                  DryadVertexProgram* vertex, 
                  RChannelWriter* outputChannel);
    virtual ~OutputChannel();

    virtual void Stop();

    BOOL WriteDataBlock(DataBlockItem *pItem,
                        Int32 numBytesToWrite);
    void ProcessWriteCompleted(RChannelItemType status,
                               RChannelItem* marshalFailureItem);
    Int64 GetBytesWritten();
    virtual void SetInitialSizeHint(UInt64 hint);
    RChannelWriter* GetWriter();
    virtual const char* GetURI();

protected:
    RChannelWriter* m_writer;
    DryadVertexProgram* m_vertex;
    Int64 m_bytesWritten;
    UInt32 m_portNum;
};

