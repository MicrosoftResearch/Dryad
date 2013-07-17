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

DRDECLARECLASS(DrPropertyReader);
DRREF(DrPropertyReader);

DRINTERFACE(DrPropertyParser)
{
public:
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader, UINT16 enumId, UINT32 dataLength) DRABSTRACT;
};
DRREF(DrPropertyParser);

/* make managed/native variants of foo& or foo% for the types we need to marshal */
DRRREF(bool);
DRRREF(INT8);
DRRREF(INT16);
DRRREF(INT32);
DRRREF(INT64);
DRRREF(UINT8);
DRRREF(UINT16);
DRRREF(UINT32);
DRRREF(UINT64);
DRRREF(HRESULT);
DRRREF(float);
DRRREF(double);
DRRREF(GUID);

DRBASECLASS(DrPropertyReader)
{
public:
    DrPropertyReader(DrByteArrayPtr byteArray);

    HRESULT SetStatus(HRESULT newStatus);

    HRESULT PeekNextPropertyTag(/* out */ UINT16 *pEnumId, /* out */ UINT32 *pDataLen);
    HRESULT PeekNextAggregateTag(/* out */ UINT16 *pValue);

    HRESULT ReadNextPropertyTag(/* out */ UINT16 *pEnumId, /* out */ UINT32 *pDataLen);
    HRESULT ReadNextProperty(UINT16 enumId);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ boolR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ INT8R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ INT16R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ INT32R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ INT64R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ UINT8R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ UINT16R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ UINT32R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ UINT64R pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ HRESULTR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ floatR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ doubleR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ GUIDR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, /* out */ DrStringR pValue);
    HRESULT ReadNextProperty(UINT16 enumId, UINT32 dataLen, /* out */ void *pDest);
    HRESULT SkipNextProperty();

    HRESULT ReadAggregate(UINT16 desiredTag, DrPropertyParserPtr parser);
    HRESULT SkipNextPropertyOrAggregate();

private:
    HRESULT PeekBytes(/* out */ BYTE *pBytes, int length);
    HRESULT PeekUInt16(/* out */ UINT16 *pVal);

    HRESULT SkipBytes(int length);

    HRESULT ReadBytes(/* out */BYTE *pBytes, int length);
    HRESULT ReadUInt8(/* out */ UINT8 *pVal);
    HRESULT ReadUInt16(/* out */ UINT16 *pVal);
    HRESULT ReadUInt32(/* out */ UINT32 *pVal);

    HRESULT          m_status;
    DrByteArrayRef   m_byteArray;
    int              m_readPtr;
};

DRBASECLASS(DrPropertyWriter)
{
public:
    DrPropertyWriter();

    DrByteArrayRef GetBuffer();

    void WritePropertyTag(UINT16 enumId, UINT32 dataLen);
    void WriteProperty(UINT16 enumId);
    void WriteProperty(UINT16 enumId, bool value);
    void WriteProperty(UINT16 enumId, INT8 value);
    void WriteProperty(UINT16 enumId, INT16 value);
    void WriteProperty(UINT16 enumId, INT32 value);
    void WriteProperty(UINT16 enumId, INT64 value);
    void WriteProperty(UINT16 enumId, UINT8 value);
    void WriteProperty(UINT16 enumId, UINT16 value);
    void WriteProperty(UINT16 enumId, UINT32 value);
    void WriteProperty(UINT16 enumId, UINT64 value);
    void WriteProperty(UINT16 enumId, float value);
    void WriteProperty(UINT16 enumId, double value);
    void WriteProperty(UINT16 enumId, GUID value);
    void WriteProperty(UINT16 enumId, DrString value);
    void WriteProperty(UINT16 enumId, UINT32 dataLen, BYTE *pDest);

    void WriteByte(BYTE b);
    void WriteBytes(BYTE* pBytes, int length);
    void WriteValue(UINT8 value);
    void WriteValue(UINT16 value);
    void WriteValue(UINT32 value);

private:
    void WritePropertyTagShort(UINT16 enumId, UINT8 dataLen);
    void WritePropertyTagLong(UINT16 enumId, UINT32 dataLen);

    DrByteArrayListRef  m_buffer;
};
DRREF(DrPropertyWriter);
