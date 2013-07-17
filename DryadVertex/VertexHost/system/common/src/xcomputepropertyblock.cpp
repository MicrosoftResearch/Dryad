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

#include "xcomputepropertyblock.h"

#pragma unmanaged

DryadXComputePnProcessPropertyRequest::DryadXComputePnProcessPropertyRequest()
{
    m_block.Attach(new DrSimpleHeapBuffer());
}

void DryadXComputePnProcessPropertyRequest::
    SetPropertyLabel(const char* label, const char* controlLabel)
{
    m_label.Set(label);
    m_controlLabel.Set(controlLabel);
}

void DryadXComputePnProcessPropertyRequest::
    SetPropertyString(const char* string)
{
    m_string.Set(string);
}

DrMemoryBuffer*
    DryadXComputePnProcessPropertyRequest::GetPropertyBlock()
{
    return m_block;
}


DryadXComputePnProcessPropertyResponse::
    DryadXComputePnProcessPropertyResponse(PXC_PROCESS_INFO response)
{
    m_processInfo = response;
    m_propertyInfo = NULL;
    m_block.Attach(new DrFixedMemoryBuffer());
}

void DryadXComputePnProcessPropertyResponse::
    RetrievePropertyLabel(const char* label)
{
    m_propertyInfo = NULL;

    UInt32 i;
    for (i=0; i<m_processInfo->NumberofProcessProperties; ++i)
    {
        PXC_PROCESSPROPERTY_INFO propertyInfo = m_processInfo->ppProperties[i];
        if (::strcmp(propertyInfo->pPropertyLabel, label) == 0)
        {
            m_propertyInfo = propertyInfo;
            break;
        }
    }
    LogAssert(m_propertyInfo != NULL);
}

DrMemoryBuffer* DryadXComputePnProcessPropertyResponse::
    GetPropertyBlock()
{
    m_block->Init((const BYTE *) m_propertyInfo->pPropertyBlock,
                  m_propertyInfo->PropertyBlockSize,
                  m_propertyInfo->PropertyBlockSize);

    return m_block;
}
