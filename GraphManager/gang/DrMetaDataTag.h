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

/* these are all the tag types used in the old Dryad: almost all of them will turn
   up as DrMTagUnknown, but individual types can be added as desired */
DRENUM(DrMTagType)
{
    DrMTT_Unknown,
    DrMTT_Boolean,
    DrMTT_Int16,
    DrMTT_Int32,
    DrMTT_Int64,
    DrMTT_UInt16,
    DrMTT_UInt32,
    DrMTT_UInt64,
    DrMTT_HexUInt16,
    DrMTT_HexUInt32,
    DrMTT_HexUInt64,
    DrMTT_String,
    DrMTT_Guid,
    DrMTT_TimeStamp,
    DrMTT_TimeInterval,
    DrMTT_BeginTag,
    DrMTT_EndTag,
    DrMTT_HRESULT,
    DrMTT_ExitCode,
    DrMTT_Blob,
    DrMTT_EnvironmentBlock,
    DrMTT_PropertyList,
    DrMTT_TagIdValue,

    DrMTT_AppendExtentOptions,
    DrMTT_SyncOptions,
    DrMTT_SyncDirectiveOptions,
    DrMTT_ReadExtentOptions,
    DrMTT_AppendStreamOptions,
    DrMTT_EnumDirectoryOptions,
    DrMTT_EnInfoBits,
    DrMTT_UpdateExtentMetadataOptions,
    DrMTT_StreamInfoBits,
    DrMTT_ExtentInfoBits,
    DrMTT_ExtentInstanceInfoBits,

    DrMTT_MetaData = 0x1000,
    DrMTT_InputChannelDescription,
    DrMTT_OutputChannelDescription,
    DrMTT_VertexProcessStatus,
    DrMTT_VertexStatus,
    DrMTT_VertexCommandBlock,

    DrMTT_Void,
    DrMTT_VertexCommand
};

DRDECLARECLASS(DrMTag);
DRREF(DrMTag);

DRBASECLASS(DrMTag abstract), public DrPropertyParser
{
public:
    /* this returns a typed DrMTag. Almost all types just give DrMTagUnknown */
    static DrMTagRef MakeTyped(UINT16 tag, UINT16 type);

    UINT16 GetMTag();
    UINT16 GetMType();

    virtual void Serialize(DrPropertyWriterPtr writer) = 0;
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader,
                                  UINT16 tag, UINT32 dataLen) = 0;

protected:
    DrMTag(UINT16 tag, UINT16 type);
    virtual ~DrMTag();

private:
    UINT16   m_tag;
    UINT16   m_type;
};

DRCLASS(DrMTagUnknown) : public DrMTag
{
public:
    DrMTagUnknown(UINT16 tag, UINT16 originalType);

    void SetData(DrByteArrayPtr data);
    DrByteArrayPtr GetData();
    UINT16 GetOriginalType();

    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader,
                                  UINT16 tag, UINT32 dataLen) DROVERRIDE;
    virtual void Serialize(DrPropertyWriterPtr writer) DROVERRIDE;

private:
    DrByteArrayRef  m_data;
    UINT16          m_originalType;
};
DRREF(DrMTagUnknown);

DRCLASS(DrMTagVoid) : public DrMTag
{
public:
    DrMTagVoid(UINT16 tag);

    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader,
                                  UINT16 tag, UINT32 dataLen) DROVERRIDE;
    virtual void Serialize(DrPropertyWriterPtr writer) DROVERRIDE;
};
DRREF(DrMTagVoid);

template <class T,UINT16 _type> DRCLASS(DrMTagBase) : public DrMTag
{
public:
    DrMTagBase(UINT16 tag) : DrMTag(tag, _type)
    {
    }

    DrMTagBase(UINT16 tag, T value) : DrMTag(tag, _type)
    {
        m_value = value;
    }

    void SetValue(T value)
    {
        m_value = value;
    }

    T GetValue()
    {
        return m_value;
    }

    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader,
                                  UINT16 tag, UINT32 /* unused dataLen*/) DROVERRIDE
    {
        DrAssert(tag == GetMTag());
        return reader->ReadNextProperty(tag, m_value);
    }

    virtual void Serialize(DrPropertyWriterPtr writer) DROVERRIDE
    {
        writer->WriteProperty(GetMTag(), m_value);
    }

private:
    T    m_value;
};

typedef DrMTagBase<INT16,DrMTT_Int16> DrMTagInt16;
DRTEMPLATE DrMTagBase<INT16,DrMTT_Int16>;
DRREF(DrMTagInt16);

typedef DrMTagBase<INT32,DrMTT_Int32> DrMTagInt32;
DRTEMPLATE DrMTagBase<INT32,DrMTT_Int32>;
DRREF(DrMTagInt32);

typedef DrMTagBase<INT64,DrMTT_Int64> DrMTagInt64;
DRTEMPLATE DrMTagBase<INT64,DrMTT_Int64>;
DRREF(DrMTagInt64);

typedef DrMTagBase<UINT16,DrMTT_UInt16> DrMTagUInt16;
DRTEMPLATE DrMTagBase<UINT16,DrMTT_UInt16>;
DRREF(DrMTagUInt16);

typedef DrMTagBase<UINT32,DrMTT_UInt32> DrMTagUInt32;
DRTEMPLATE DrMTagBase<UINT32,DrMTT_UInt32>;
DRREF(DrMTagUInt32);

typedef DrMTagBase<UINT64,DrMTT_UInt64> DrMTagUInt64;
DRTEMPLATE DrMTagBase<UINT64,DrMTT_UInt64>;
DRREF(DrMTagUInt64);

typedef DrMTagBase<HRESULT,DrMTT_HRESULT> DrMTagHRESULT;
DRTEMPLATE DrMTagBase<HRESULT,DrMTT_HRESULT>;
DRREF(DrMTagHRESULT);

typedef DrMTagBase<DrString,DrMTT_String> DrMTagString;
DRTEMPLATE DrMTagBase<DrString,DrMTT_String>;
DRREF(DrMTagString);


typedef DrArrayList<DrMTagRef> DrMTagList;
DRAREF(DrMTagList,DrMTagRef);
