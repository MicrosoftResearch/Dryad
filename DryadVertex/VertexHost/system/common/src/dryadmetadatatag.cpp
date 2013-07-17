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

#include <dryadmetadata.h>
#include <DrCommon.h>

#include <strsafe.h>


#pragma unmanaged

DryadMTag::DryadMTag(UInt16 tagValue, UInt16 type)
{
    m_tag = tagValue;
    m_type = type;
}

DryadMTag::~DryadMTag()
{
}

UInt16 DryadMTag::GetTagValue()
{
    return m_tag;
}

UInt16 DryadMTag::GetType()
{
    return m_type;
}

void DryadMTag::Clone(DryadMTagRef* dstTag)
{
    *dstTag = this;
}
