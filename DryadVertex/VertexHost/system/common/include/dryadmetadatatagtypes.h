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

#include "dryadmetadatatag.h"
#include "dryadpropertiesdef.h"

class DryadInputChannelDescription;
class DryadOutputChannelDescription;
class DVertexProcessStatus;
class DVertexStatus;
class DVertexCommandBlock;

class DryadMTagUnknown;
typedef DrRef<DryadMTagUnknown> DryadMTagUnknownRef;

class DryadMTagUnknown : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagUnknown* Create(UInt16 tag, UInt32 dataLen, void* data,
                                    UInt16 originalType);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagUnknownRef* pTag);
    static DrError ReadFromStreamWithType(DrMemoryReader* reader,
                                          UInt16 enumID, UInt32 dataLen,
                                          UInt16 originalType,
                                          DryadMTagUnknownRef* outTag);
    DrError Serialize(DrMemoryWriter* writer);

    UInt32 GetDataLength();
    void* GetData();
    UInt16 GetOriginalType();

private:
    DryadMTagUnknown(UInt16 tag, UInt32 dataLen, UInt16 originalType);
    ~DryadMTagUnknown();

    UInt32    m_dataLength;
    void*     m_data;
    UInt16    m_originalType;
};

class DryadMTagVoid;
typedef DrRef<DryadMTagVoid> DryadMTagVoidRef;

class DryadMTagVoid : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagVoid* Create(UInt16 tag);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagVoidRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

private:
    DryadMTagVoid(UInt16 tag);
    ~DryadMTagVoid();
};

class DryadMTagBoolean;
typedef DrRef<DryadMTagBoolean> DryadMTagBooleanRef;

class DryadMTagBoolean : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagBoolean* Create(UInt16 tag, bool val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagBooleanRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    bool GetBoolean();

private:
    DryadMTagBoolean(UInt16 tag, bool val);
    ~DryadMTagBoolean();

    bool    m_val;
};

class DryadMTagInt16;
typedef DrRef<DryadMTagInt16> DryadMTagInt16Ref;

class DryadMTagInt16 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagInt16* Create(UInt16 tag, Int16 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagInt16Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    Int16 GetInt16();

private:
    DryadMTagInt16(UInt16 tag, Int16 val);
    ~DryadMTagInt16();

    Int16    m_i16Val;
};

class DryadMTagUInt16;
typedef DrRef<DryadMTagUInt16> DryadMTagUInt16Ref;

class DryadMTagUInt16 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagUInt16* Create(UInt16 tag, UInt16 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagUInt16Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    UInt16 GetUInt16();

private:
    DryadMTagUInt16(UInt16 tag, UInt16 val);
    ~DryadMTagUInt16();

    UInt16    m_uI16Val;
};

class DryadMTagInt32;
typedef DrRef<DryadMTagInt32> DryadMTagInt32Ref;

class DryadMTagInt32 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagInt32* Create(UInt16 tag, Int32 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagInt32Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    Int32 GetInt32();

private:
    DryadMTagInt32(UInt16 tag, Int32 val);
    ~DryadMTagInt32();

    Int32    m_i32Val;
};

class DryadMTagUInt32;
typedef DrRef<DryadMTagUInt32> DryadMTagUInt32Ref;

class DryadMTagUInt32 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagUInt32* Create(UInt16 tag, UInt32 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagUInt32Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    UInt32 GetUInt32();

private:
    DryadMTagUInt32(UInt16 tag, UInt32 val);
    ~DryadMTagUInt32();

    UInt32    m_uI32Val;
};

class DryadMTagInt64;
typedef DrRef<DryadMTagInt64> DryadMTagInt64Ref;

class DryadMTagInt64 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagInt64* Create(UInt16 tag, Int64 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagInt64Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    Int64 GetInt64();

private:
    DryadMTagInt64(UInt16 tag, Int64 val);
    ~DryadMTagInt64();

    Int64    m_i64Val;
};

class DryadMTagUInt64;
typedef DrRef<DryadMTagUInt64> DryadMTagUInt64Ref;

class DryadMTagUInt64 : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagUInt64* Create(UInt16 tag, UInt64 val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagUInt64Ref* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    UInt64 GetUInt64();

private:
    DryadMTagUInt64(UInt16 tag, UInt64 val);
    ~DryadMTagUInt64();

    UInt64    m_uI64Val;
};

class DryadMTagDouble;
typedef DrRef<DryadMTagDouble> DryadMTagDoubleRef;

class DryadMTagDouble : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagDouble* Create(UInt16 tag, double val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagDoubleRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    double GetDouble();

private:
    DryadMTagDouble(UInt16 tag, double val);
    ~DryadMTagDouble();

    double    m_doubleVal;
};

class DryadMTagString;
typedef DrRef<DryadMTagString> DryadMTagStringRef;

class DryadMTagString : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagString* Create(UInt16 tag, const char* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagStringRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    const char* GetString();

private:
    DryadMTagString(UInt16 tag, const char* val);
    ~DryadMTagString();
    char* GetWritableString(size_t dataLen);

    DrStr64  m_string;
};

class DryadMTagGuid;
typedef DrRef<DryadMTagGuid> DryadMTagGuidRef;

class DryadMTagGuid : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagGuid* Create(UInt16 tag, const DrGuid* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagGuidRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    const DrGuid* GetGuid();

private:
    DryadMTagGuid(UInt16 tag, const DrGuid* val);
    ~DryadMTagGuid();

    DrInitializedGuid  m_guid;
};

class DryadMTagTimeStamp;
typedef DrRef<DryadMTagTimeStamp> DryadMTagTimeStampRef;

class DryadMTagTimeStamp : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagTimeStamp* Create(UInt16 tag, DrTimeStamp val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagTimeStampRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DrTimeStamp GetTimeStamp();

private:
    DryadMTagTimeStamp(UInt16 tag, DrTimeStamp val);
    ~DryadMTagTimeStamp();

    DrTimeStamp  m_val;
};

class DryadMTagTimeInterval;
typedef DrRef<DryadMTagTimeInterval> DryadMTagTimeIntervalRef;

class DryadMTagTimeInterval : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagTimeInterval* Create(UInt16 tag, DrTimeInterval val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagTimeIntervalRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DrTimeInterval GetTimeInterval();

private:
    DryadMTagTimeInterval(UInt16 tag, DrTimeInterval val);
    ~DryadMTagTimeInterval();

    DrTimeInterval  m_val;
};

class DryadMTagDrError;
typedef DrRef<DryadMTagDrError> DryadMTagDrErrorRef;

class DryadMTagDrError : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagDrError* Create(UInt16 tag, DrError val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagDrErrorRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DrError GetDrError();

private:
    DryadMTagDrError(UInt16 tag, DrError val);
    ~DryadMTagDrError();

    DrError  m_val;
};

class DryadMTagMetaData;
typedef DrRef<DryadMTagMetaData> DryadMTagMetaDataRef;

class DryadMTagMetaData : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. The caller's reference to val is
       transferred to the new tag. If marshalAsAggregate is true, this
       is serialized as an aggregate with tagValue=tag, otherwise it
       is serialized as a single property with enumID=tag. */
    static DryadMTagMetaData* Create(UInt16 tag, DryadMetaData* val,
                                     bool marshalAsAggregate);
    static DrError ReadFromStreamInAggregate(DrMemoryReader* reader,
                                             DryadMTagMetaDataRef* pTag);
    static DrError ReadFromArray(UInt16 tag,
                                 const void* data, UInt32 dataLen,
                                 DryadMTagMetaDataRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    /* do a deep copy instead of just increasing the refcount */
    DryadMTag* Clone();

    /* this call does not modify the returned metadata's reference
       count */
    DryadMetaData* GetMetaData();

private:
    DryadMTagMetaData(UInt16 tag, DryadMetaData* val,
                      bool marshalAsAggregate);
    ~DryadMTagMetaData();

    DrRef<DryadMetaData>  m_val;
    bool                  m_marshalAsAggregate;
};

class DryadMTagVertexCommand;
typedef DrRef<DryadMTagVertexCommand> DryadMTagVertexCommandRef;

class DryadMTagVertexCommand : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagVertexCommand* Create(UInt16 tag, DVertexCommand val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  UInt16 tag, UInt32 dataLen,
                                  DryadMTagVertexCommandRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DVertexCommand GetVertexCommand();

private:
    DryadMTagVertexCommand(UInt16 tag, DVertexCommand val);
    ~DryadMTagVertexCommand();

    DVertexCommand    m_val;
};

class DryadMTagInputChannelDescription;
typedef DrRef<DryadMTagInputChannelDescription>
    DryadMTagInputChannelDescriptionRef;

class DryadMTagInputChannelDescription : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagInputChannelDescription*
        Create(UInt16 tag, DryadInputChannelDescription* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  DryadMTagInputChannelDescriptionRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DryadInputChannelDescription* GetInputChannelDescription();

private:
    DryadMTagInputChannelDescription(UInt16 tag,
                                     DryadInputChannelDescription* val);
    ~DryadMTagInputChannelDescription();

    DryadInputChannelDescription*    m_val;
};

class DryadMTagOutputChannelDescription;
typedef DrRef<DryadMTagOutputChannelDescription>
    DryadMTagOutputChannelDescriptionRef;

class DryadMTagOutputChannelDescription : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagOutputChannelDescription*
        Create(UInt16 tag, DryadOutputChannelDescription* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  DryadMTagOutputChannelDescriptionRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DryadOutputChannelDescription* GetOutputChannelDescription();

private:
    DryadMTagOutputChannelDescription(UInt16 tag,
                                     DryadOutputChannelDescription* val);
    ~DryadMTagOutputChannelDescription();

    DryadOutputChannelDescription*    m_val;
};

class DryadMTagVertexProcessStatus;
typedef DrRef<DryadMTagVertexProcessStatus> DryadMTagVertexProcessStatusRef;

class DryadMTagVertexProcessStatus : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagVertexProcessStatus* Create(UInt16 tag,
                                                DVertexProcessStatus* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  DryadMTagVertexProcessStatusRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DVertexProcessStatus* GetVertexProcessStatus();

private:
    DryadMTagVertexProcessStatus(UInt16 tag, DVertexProcessStatus* val);
    ~DryadMTagVertexProcessStatus();

    DrRef<DVertexProcessStatus>    m_val;
};

class DryadMTagVertexStatus;
typedef DrRef<DryadMTagVertexStatus> DryadMTagVertexStatusRef;

class DryadMTagVertexStatus : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagVertexStatus* Create(UInt16 tag,
                                         DVertexStatus* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  DryadMTagVertexStatusRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DVertexStatus* GetVertexStatus();

private:
    DryadMTagVertexStatus(UInt16 tag, DVertexStatus* val);
    ~DryadMTagVertexStatus();

    DrRef<DVertexStatus>    m_val;
};

class DryadMTagVertexCommandBlock;
typedef DrRef<DryadMTagVertexCommandBlock> DryadMTagVertexCommandBlockRef;

class DryadMTagVertexCommandBlock : public DryadMTag
{
public:
    /* the following call creates a new tag with a single reference
       owned by the caller. */
    static DryadMTagVertexCommandBlock* Create(UInt16 tag,
                                               DVertexCommandBlock* val);
    static DrError ReadFromStream(DrMemoryReader* reader,
                                  DryadMTagVertexCommandBlockRef* pTag);
    DrError Serialize(DrMemoryWriter* writer);

    DVertexCommandBlock* GetVertexCommandBlock();

private:
    DryadMTagVertexCommandBlock(UInt16 tag, DVertexCommandBlock* val);
    ~DryadMTagVertexCommandBlock();

    DrRef<DVertexCommandBlock>    m_val;
};
