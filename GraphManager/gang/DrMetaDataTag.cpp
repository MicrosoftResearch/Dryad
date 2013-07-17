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

#include <DrGangHeaders.h>

DrMTag::DrMTag(UINT16 tag, UINT16 type)
{
    m_tag = tag;
    m_type = type;
}

DrMTag::~DrMTag()
{
}

UINT16 DrMTag::GetMTag()
{
    return m_tag;
}

UINT16 DrMTag::GetMType()
{
    return m_type;
}

#define DRMTAGCASESTMT(_type) \
    case DrMTT_##_type: \
        return DrNew DrMTag##_type(tag); \
        break; \


DrMTagRef DrMTag::MakeTyped(UINT16 tag, UINT16 type)
{
    switch (type)
    {
        DRMTAGCASESTMT(Void)
        DRMTAGCASESTMT(Int16)
        DRMTAGCASESTMT(Int32)
        DRMTAGCASESTMT(Int64)
        DRMTAGCASESTMT(UInt16)
        DRMTAGCASESTMT(UInt32)
        DRMTAGCASESTMT(UInt64)
        DRMTAGCASESTMT(HRESULT)
        DRMTAGCASESTMT(String)

    case DrMTT_Unknown:
    default:
        return DrNew DrMTagUnknown(tag, type);
    }
};


DrMTagUnknown::DrMTagUnknown(UINT16 tag, UINT16 originalType) : DrMTag(tag, DrMTT_Unknown)
{
    m_originalType = originalType;
}

void DrMTagUnknown::SetData(DrByteArrayPtr data)
{
    m_data = data;
}

DrByteArrayPtr DrMTagUnknown::GetData()
{
    return m_data;
}

UINT16 DrMTagUnknown::GetOriginalType()
{
    return m_originalType;
}

HRESULT DrMTagUnknown::ParseProperty(DrPropertyReaderPtr reader, UINT16 tag, UINT32 dataLen)
{
    DrAssert(tag == GetMTag());
    DrAssert(dataLen < 0x80000000);

    m_data = DrNew DrByteArray((int) dataLen);
    HRESULT status;
    {
        DRPIN(BYTE) dst = &(m_data[0]);
        status = reader->ReadNextProperty(tag, dataLen, dst);
    }
    if (status != S_OK)
    {
        m_data = DrNull;
    }
    return status;
}

void DrMTagUnknown::Serialize(DrPropertyWriterPtr writer)
{
    DRPIN(BYTE) data = &(m_data[0]);
    writer->WriteProperty(GetMTag(), m_data->Allocated(), data);
}


DrMTagVoid::DrMTagVoid(UINT16 tag) : DrMTag(tag, DrMTT_Void)
{
}

HRESULT DrMTagVoid::ParseProperty(DrPropertyReaderPtr reader, UINT16 tag,
                                  UINT32 /* unused dataLen */)
{
    DrAssert(tag == GetMTag());
    return reader->ReadNextProperty(tag);
}

void DrMTagVoid::Serialize(DrPropertyWriterPtr writer)
{
    writer->WriteProperty(GetMTag());
}