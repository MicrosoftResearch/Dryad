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

#include <nullchanneltransform.h>

#pragma unmanaged 


ChannelTransform::~ChannelTransform()
{
    // nothing needed here
}

NullChannelTransform::NullChannelTransform(DryadVertexProgram* vertex)
{
    m_vertex = vertex;
}

NullChannelTransform::~NullChannelTransform()
{
    m_channel = NULL;
}

DrError NullChannelTransform::Start(FifoChannel *channel)
{
    m_channel = channel;
    return DrError_OK;
}

void NullChannelTransform::SetOutputBufferSize(UInt32 bufferSize)
{
    // nothing needed here
}

DrError NullChannelTransform::ProcessItem(DataBlockItem *item)
{
    m_channel->WriteTransformedItem(item);
    return DrError_OK;
}

DrError NullChannelTransform::Finish(bool atEndOfStream)
{
    m_channel = NULL;
    return DrError_OK;
}
