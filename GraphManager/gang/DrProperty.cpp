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

DrPropertyReader::DrPropertyReader(DrByteArrayPtr byteArray)
{
    m_status = S_OK;
    m_byteArray = byteArray;
    m_readPtr = 0;
}

HRESULT DrPropertyReader::SetStatus(HRESULT newStatus)
{
    if (m_status == S_OK)
    {
        m_status = newStatus;
    }

    return m_status;
}

HRESULT DrPropertyReader::PeekNextPropertyTag(/* out */ UINT16 *pEnumId, /* out */ UINT32 *pDataLen)
{
    if (PeekUInt16(pEnumId) != S_OK)
    {
        return m_status;
    }

    BYTE tmp[sizeof(UINT16) + sizeof(UINT32)];

    if (((*pEnumId) & DrPropLengthMask) == DrPropLength_Short)
    {
        if (PeekBytes(tmp, sizeof(UINT16) + sizeof(UINT8)) == S_OK)
        {
            *pDataLen = tmp[sizeof(UINT16)];
        }
    }
    else
    {
        if (PeekBytes(tmp, sizeof(UINT16) + sizeof(UINT32)) == S_OK)
        {
            memcpy(pDataLen, tmp+sizeof(UINT16), sizeof(UINT32));
        }
    }

    return m_status;
}

HRESULT DrPropertyReader::PeekNextAggregateTag(/* out */ UINT16 *pValue)
{
    UINT16 enumIdActual;
    UINT32 dataLenActual;
    UINT16 enumId = DrProp_BeginTag;
    UINT32 dataLen = sizeof(UINT16);

    if (PeekNextPropertyTag(&enumIdActual, &dataLenActual) == S_OK)
    {
        if (enumIdActual != enumId || dataLenActual != dataLen)
        {
            DrLogW("Mismatched property peek %u,%u %u,%u", enumIdActual, enumId, dataLenActual, dataLen);
            SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
        }
        else
        {
            UINT32 hdrLen = sizeof(UINT16) + sizeof(UINT8);

            BYTE prop[sizeof(UINT16) + sizeof(UINT8) + sizeof(UINT16)];
            if (PeekBytes(prop, hdrLen + dataLen) == S_OK)
            {
                memcpy(pValue, prop + hdrLen, dataLen);
            }
        }
    }

    return m_status;
}

HRESULT DrPropertyReader::PeekBytes(/* out */ BYTE* pBytes, int length)
{
    if (m_status != S_OK)
    {
        return m_status;
    }
    else if (length + m_readPtr > m_byteArray->Allocated())
    {
        return SetStatus(HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER));
    }
    else
    {
        DRPIN(BYTE) srcPtr = &(m_byteArray[m_readPtr]);
        memcpy(pBytes, srcPtr, length);
        return S_OK;
    }
}

HRESULT DrPropertyReader::PeekUInt16(/* out */ UINT16 *pVal)
{
    // assumes little endian
    return PeekBytes((BYTE *)(void *)pVal, sizeof(*pVal));
}

HRESULT DrPropertyReader::SkipBytes(int length)
{
    if (m_status != S_OK)
    {
        return m_status;
    }
    else if (length + m_readPtr > m_byteArray->Allocated())
    {
        return SetStatus(HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER));
    }
    else
    {
        m_readPtr += length;
        return S_OK;
    }
}

HRESULT DrPropertyReader::ReadBytes(/* out */ BYTE* pBytes, int length)
{
    if (m_status != S_OK)
    {
        return m_status;
    }
    else if (length + m_readPtr > m_byteArray->Allocated())
    {
        return SetStatus(HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER));
    }
    else
    {
        DRPIN(BYTE) srcPtr = &(m_byteArray[m_readPtr]);
        memcpy(pBytes, srcPtr, length);
        m_readPtr += length;
        return S_OK;
    }
}

HRESULT DrPropertyReader::ReadUInt8(/* out */ UINT8 *pVal)
{
    return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
}

HRESULT DrPropertyReader::ReadUInt16(/* out */ UINT16 *pVal)
{
    // assumes little endian
    return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
}

HRESULT DrPropertyReader::ReadUInt32(/* out */ UINT32 *pVal)
{
    // assumes little endian
    return ReadBytes((BYTE *)(void *)pVal, sizeof(*pVal));
}

HRESULT DrPropertyReader::ReadNextPropertyTag(/* out */ UINT16 *pEnumId, /* out */ UINT32 *pDataLen)
{
    if (ReadUInt16(pEnumId) != S_OK)
    {
        return m_status;
    }

    if (((*pEnumId) & DrPropLengthMask) == DrPropLength_Short)
    {
        UINT8 lengthByte;
        if (ReadUInt8(&lengthByte) == S_OK)
        {
            *pDataLen = lengthByte;
        }
    }
    else
    {
        ReadUInt32(pDataLen);
    }

    return m_status;
}

HRESULT DrPropertyReader::ReadNextProperty(UINT16 enumId, UINT32 dataLen, void *pDest)
{
    UINT16 realEnumId;
    UINT32 realDataLen;

    if (ReadNextPropertyTag(&realEnumId, &realDataLen) == S_OK)
    {
        if (realEnumId != enumId || realDataLen != dataLen)
        {
            DrLogW("Mismatched property read %u,%u %u,%u", realEnumId, enumId, realDataLen, dataLen);
            SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
        }
        else
        {
            ReadBytes((BYTE*) pDest, dataLen);
        }
    }

    return m_status;
}

HRESULT DrPropertyReader::ReadNextProperty(UINT16 enumId)
{
    return ReadNextProperty(enumId, 0, NULL);
}

#ifdef _MANAGED
#define MAKEDRPROPREADER(_type) \
    HRESULT DrPropertyReader::ReadNextProperty(UINT16 enumId, /* out */ _type %pValue) \
    { \
        _type tmp; \
        if (ReadNextProperty(enumId, sizeof(_type), &tmp) == S_OK) \
        { \
            pValue = tmp; \
        } \
        return m_status; \
    }
#else
#define MAKEDRPROPREADER(_type) \
    HRESULT DrPropertyReader::ReadNextProperty(UINT16 enumId, /* out */ _type &pValue) \
    { \
        _type tmp; \
        if (ReadNextProperty(enumId, sizeof(_type), &tmp) == S_OK) \
        { \
            pValue = tmp; \
        } \
        return m_status; \
    }
#endif

MAKEDRPROPREADER(bool)
MAKEDRPROPREADER(INT8)
MAKEDRPROPREADER(INT16)
MAKEDRPROPREADER(INT32)
MAKEDRPROPREADER(INT64)
MAKEDRPROPREADER(UINT8)
MAKEDRPROPREADER(UINT16)
MAKEDRPROPREADER(UINT32)
MAKEDRPROPREADER(UINT64)
MAKEDRPROPREADER(HRESULT)
MAKEDRPROPREADER(float)
MAKEDRPROPREADER(double)
MAKEDRPROPREADER(GUID)

HRESULT DrPropertyReader::ReadNextProperty(UINT16 enumId, /* out */ DrStringR pValue)
{
    UINT32 length;
    UINT16 realEnumId;

    if (ReadNextPropertyTag(&realEnumId, &length) == S_OK)
    {
        if (realEnumId != enumId)
        {
            DrLogW("Mismatched string property read %u,%u", realEnumId, enumId);
            SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
        }
        else
        {
            if (length > 0)
            {
                DrByteArrayRef array = DrNew DrByteArray(length+1);
                {
                    DRPIN(BYTE) dst = &(array[0]);
                    if (SUCCEEDED(ReadBytes(dst, length)))
                    {
                        /* ensure there's a terminator just in case */
                        array[(int)length] = 0;
                        DrString s;
                        s.SetF("%s", (const char *) dst);
                        pValue = s;
                        return S_OK;
                    }
                }
            }
            else
            {
                pValue = DrNull;
            }
        }
    }

    return m_status;
}

HRESULT DrPropertyReader::ReadAggregate(UINT16 desiredTagType, DrPropertyParserPtr parser)
{
    HRESULT err;
    UINT16 beginTagType;

    if (ReadNextProperty(DrProp_BeginTag, sizeof(UINT16), &beginTagType) != S_OK)
    {
        return m_status;
    }

    if (beginTagType != desiredTagType)
    {
        DrLogW("Mismatched aggregate read %u,%u", beginTagType, desiredTagType);
        return SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
    }

    for (;;)
    {
        UINT16 propertyType;
        UINT32 dataLen;

        if (PeekNextPropertyTag(&propertyType, &dataLen) != S_OK)    
        {
            return m_status;
        }

        // If we find an end tag, it must be for the begin tag we consumed
        if (propertyType == DrProp_EndTag)
        {
            UINT16 endTagType;

            // Consume it
            if (ReadNextProperty(DrProp_EndTag, endTagType) != S_OK)
            {
                return m_status;
            }

            if (desiredTagType != endTagType)
            {
                DrLogW("Mismatched aggregate end read %u,%u", desiredTagType, endTagType);
                return SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
            }

            // We're done
            return S_OK;
        }
        else
        {
            // This could be a begin tag - it's up to the caller to call ReadAggregate()
            // or SkipNextPropertyOrAggregate()
            err = parser->ParseProperty(this, propertyType, dataLen);
            if (err != S_OK)
            {
                return SetStatus(err);
            }
        }
    }
}

HRESULT DrPropertyReader::SkipNextProperty()
{
    UINT16 enumId;
    UINT32 dataLen;

    if (ReadNextPropertyTag(&enumId, &dataLen) == S_OK)
    {
        SkipBytes(dataLen);
    }

    return m_status;
}

HRESULT DrPropertyReader::SkipNextPropertyOrAggregate()
{
    UINT32  dataLen;
    UINT16  propertyType;
    UINT16  beginTagType;

    if (PeekNextPropertyTag(&propertyType, &dataLen) != S_OK)
    {
        return m_status;
    }

    // If it's not a begin tag, just skip the property and return
    if (propertyType != DrProp_BeginTag)
    {
        return SkipNextProperty();
    }

    // Read the begin tag type
    if (ReadNextProperty(DrProp_BeginTag, beginTagType) != S_OK)
    {
        return m_status;
    }

    // Skip until corresponding end tag
    // If another BeginTag is encountered, recurse as appropriate
    for (;;)
    {
        if (PeekNextPropertyTag(&propertyType, &dataLen) != S_OK)
        {
            return m_status;
        }

        if (propertyType == DrProp_BeginTag)
        {
            if (SkipNextPropertyOrAggregate() != S_OK)
            {
                return m_status;
            }
        }
        else if (propertyType == DrProp_EndTag)
        {
            UINT16 endTagType;

            if (ReadNextProperty(DrProp_EndTag, endTagType) != S_OK)
            {
                return m_status;
            }

            if (endTagType != beginTagType)
            {
                DrLogW("Mismatched aggregate matchup %u,%u", endTagType, beginTagType);
                return SetStatus(HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER));
            }

            return S_OK;
        }
        else
        {
            if (SkipNextProperty() != S_OK)
            {
                return m_status;
            }
        }
    }
}


DrPropertyWriter::DrPropertyWriter()
{
    m_buffer = DrNew DrByteArrayList();
}

DrByteArrayRef DrPropertyWriter::GetBuffer()
{
    DrByteArrayRef array = DrNew DrByteArray(m_buffer->Size());
    int i;
    for (i=0; i<m_buffer->Size(); ++i)
    {
        array[i] = m_buffer[i];
    }
    return array;
}

void DrPropertyWriter::WriteByte(BYTE b)
{
    m_buffer->Add(b);
}

void DrPropertyWriter::WriteBytes(BYTE* pBytes, int length)
{
    int i;
    for (i=0; i<length; ++i)
    {
        WriteByte(pBytes[i]);
    }
}

#define MAKEDRPROPBYTEWRITER(_type) \
    void DrPropertyWriter::WriteValue(_type value) \
    { \
        WriteBytes((BYTE *) &value, sizeof(value)); \
    }

MAKEDRPROPBYTEWRITER(UINT8)
MAKEDRPROPBYTEWRITER(UINT16)
MAKEDRPROPBYTEWRITER(UINT32)

void DrPropertyWriter::WritePropertyTagShort(UINT16 enumId, UINT8 dataLen)
{
    DrAssert((enumId & DrPropLengthMask) == DrPropLength_Short);
    WriteValue(enumId);
    WriteValue(dataLen);
}

void DrPropertyWriter::WritePropertyTagLong(UINT16 enumId, UINT32 dataLen)
{
    DrAssert((enumId & DrPropLengthMask) == DrPropLength_Long);
    WriteValue(enumId);
    WriteValue(dataLen);
}

void DrPropertyWriter::WritePropertyTag(UINT16 enumId, UINT32 dataLen)
{
    if ((enumId & DrPropLengthMask) == DrPropLength_Short)
    {
        DrAssert(dataLen < 256);
        WritePropertyTagShort(enumId, (UINT8) dataLen);
    }
    else
    {
        WritePropertyTagLong(enumId, dataLen);
    }
}

void DrPropertyWriter::WriteProperty(UINT16 enumId)
{
    WritePropertyTag(enumId, 0);
}

#define MAKEDRPROPWRITER(_type) \
    void DrPropertyWriter::WriteProperty(UINT16 enumId, _type value) \
    { \
        WritePropertyTag(enumId, sizeof(value)); \
        WriteBytes((BYTE *) &value, sizeof(value)); \
    }

MAKEDRPROPWRITER(bool)
MAKEDRPROPWRITER(INT8)
MAKEDRPROPWRITER(INT16)
MAKEDRPROPWRITER(INT32)
MAKEDRPROPWRITER(INT64)
MAKEDRPROPWRITER(UINT8)
MAKEDRPROPWRITER(UINT16)
MAKEDRPROPWRITER(UINT32)
MAKEDRPROPWRITER(UINT64)
MAKEDRPROPWRITER(float)
MAKEDRPROPWRITER(double)
MAKEDRPROPWRITER(GUID)

void DrPropertyWriter::WriteProperty(UINT16 enumId, DrString string)
{
    int length;

    if (string.GetChars() == DrNull)
    {
        length = -1;
    }
    else
    {
        length = (int) strlen(string.GetChars());
    }

    WritePropertyTag(enumId, (UINT32) (length+1));

    if (length >= 0) {
        WriteBytes((BYTE *) string.GetChars(), length);
        WriteByte((BYTE)0);
    }
}

void DrPropertyWriter::WriteProperty(UINT16 enumId, UINT32 dataLen, BYTE* pDest)
{
    WritePropertyTag(enumId, dataLen);
    WriteBytes(pDest, dataLen);
}