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
#include <fifochannel.h>

class NullChannelTransform : public ChannelTransform
{
public:
    NullChannelTransform(DryadVertexProgram* vertex);
    virtual ~NullChannelTransform();
    virtual DrError Start(FifoChannel *channel);
    virtual void SetOutputBufferSize(UInt32 bufferSize);
    virtual DrError ProcessItem(DataBlockItem *item);
    virtual DrError Finish(bool atEndOfStream);
private:
    FifoChannel *m_channel;
};
