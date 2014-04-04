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

#include <httppropertyblock.h>


DryadHttpPnProcessPropertyRequest::DryadHttpPnProcessPropertyRequest()
{
    m_block.Attach(new DrSimpleHeapBuffer());
}

void DryadHttpPnProcessPropertyRequest::
    SetPropertyLabel(const char* label, const char* /*controlLabel*/)
{
    m_label.Set(label);
}

void DryadHttpPnProcessPropertyRequest::
    SetPropertyString(const char* string)
{
    m_string.Set(string);
}

DrMemoryBuffer*
    DryadHttpPnProcessPropertyRequest::GetPropertyBlock()
{
    return m_block;
}

DryadHttpPnProcessPropertyResponse::
    DryadHttpPnProcessPropertyResponse(UInt32 length, const unsigned char* data)
{
    m_block.Attach(new DrFixedMemoryBuffer());
    m_block->Init(data, length, length);
}

DrMemoryBuffer* DryadHttpPnProcessPropertyResponse::GetPropertyBlock()
{
    return m_block;
}

void DryadHttpPnProcessPropertyResponse::RetrievePropertyLabel(const char* label)
{
}



