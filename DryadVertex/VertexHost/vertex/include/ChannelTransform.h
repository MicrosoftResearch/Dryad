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

#include <channelinterface.h>
#include <DataBlockItem.h>
#include <dryadvertex.h>
class FifoChannel;

class ChannelTransform
{
public:
    virtual ~ChannelTransform() = 0;
    virtual DrError Start(FifoChannel *channel) = 0;
    virtual void SetOutputBufferSize(UInt32 bufferSize) = 0;
    virtual DrError ProcessItem(DataBlockItem *item) = 0;
    virtual DrError Finish(bool atEndOfStream) = 0;
protected:
    DryadVertexProgram *m_vertex; // used for error reporting
};

