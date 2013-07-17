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
#include <dvertexcommand.h>

#pragma unmanaged

DryadMTagUnknown::DryadMTagUnknown(UInt16 tag, UInt32 dataLength,
                                   UInt16 originalType) :
    DryadMTag(tag, DrPropertyTagType_Unknown)
{
    m_dataLength = dataLength;
    if ((tag & PropLengthMask) == PropLength_Short)
    {
        LogAssert(m_dataLength < 256);
    }
    m_data = new char[m_dataLength];
    m_originalType = originalType;
}

DryadMTagUnknown::~DryadMTagUnknown()
{
    delete [] m_data;
}

DryadMTagUnknown* DryadMTagUnknown::Create(UInt16 enumID, UInt32 dataLen,
                                           void* data, UInt16 originalType)
{
    DryadMTagUnknown* tag =
        new DryadMTagUnknown(enumID, dataLen, originalType);
    ::memcpy(tag->GetData(), data, dataLen);
    return tag;
}

DrError DryadMTagUnknown::ReadFromStreamWithType(DrMemoryReader* reader,
                                                 UInt16 enumID, UInt32 dataLen,
                                                 UInt16 originalType,
                                                 DryadMTagUnknownRef* outTag)
{
    DryadMTagUnknown* tag =
        new DryadMTagUnknown(enumID, dataLen, originalType);
    DrError cse = reader->ReadNextKnownProperty(enumID, dataLen,
                                                tag->GetData());
    if (cse == DrError_OK)
    {
        outTag->Attach(tag);
    }
    else
    {
        tag->DecRef();
        (*outTag) = NULL;
    }
    return cse;
}

DrError DryadMTagUnknown::ReadFromStream(DrMemoryReader* reader,
                                         UInt16 enumID, UInt32 dataLen,
                                         DryadMTagUnknownRef* outTag)
{
    return ReadFromStreamWithType(reader, enumID, dataLen,
                                  DrPropertyTagType_Unknown, outTag);
}

UInt32 DryadMTagUnknown::GetDataLength()
{
    return m_dataLength;
}

void* DryadMTagUnknown::GetData()
{
    return m_data;
}

UInt16 DryadMTagUnknown::GetOriginalType()
{
    return m_originalType;
}

DrError DryadMTagUnknown::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteAnySizeBlobProperty(GetTagValue(), m_dataLength, m_data);
}


DryadMTagVoid::DryadMTagVoid(UInt16 tag) :
    DryadMTag(tag, DryadPropertyTagType_Void)
{
}

DryadMTagVoid::~DryadMTagVoid()
{
}

DryadMTagVoid* DryadMTagVoid::Create(UInt16 tag)
{
    return new DryadMTagVoid(tag);
}

DrError DryadMTagVoid::ReadFromStream(DrMemoryReader* reader,
                                      UInt16 enumID, UInt32 dataLen,
                                      DryadMTagVoidRef* outTag)
{
    if (dataLen > 0)
    {
        return DrError_InvalidProperty;
    }

    const void* dummyData;
    DrError cse = reader->ReadNextProperty(&enumID, &dataLen, &dummyData);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

DrError DryadMTagVoid::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteAnySizeBlobProperty(GetTagValue(), 0, NULL);
}


DryadMTagBoolean::DryadMTagBoolean(UInt16 tag, bool val) :
    DryadMTag(tag, DrPropertyTagType_Boolean)
{
    m_val = val;
}

DryadMTagBoolean::~DryadMTagBoolean()
{
}

DryadMTagBoolean* DryadMTagBoolean::Create(UInt16 tag, bool val)
{
    return new DryadMTagBoolean(tag, val);
}

DrError DryadMTagBoolean::ReadFromStream(DrMemoryReader* reader,
                                         UInt16 enumID, UInt32 dataLen,
                                         DryadMTagBooleanRef* outTag)
{
    bool val;
    DrError cse = reader->ReadNextBoolProperty(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

bool DryadMTagBoolean::GetBoolean()
{
    return m_val;
}

DrError DryadMTagBoolean::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteBoolProperty(GetTagValue(), m_val);
}


DryadMTagInt16::DryadMTagInt16(UInt16 tag, Int16 val) :
    DryadMTag(tag, DrPropertyTagType_Int16)
{
    m_i16Val = val;
}

DryadMTagInt16::~DryadMTagInt16()
{
}

DryadMTagInt16* DryadMTagInt16::Create(UInt16 tag, Int16 val)
{
    return new DryadMTagInt16(tag, val);
}

DrError DryadMTagInt16::ReadFromStream(DrMemoryReader* reader,
                                       UInt16 enumID, UInt32 dataLen,
                                       DryadMTagInt16Ref* outTag)
{
    Int16 val;
    DrError cse = reader->ReadNextInt16Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

Int16 DryadMTagInt16::GetInt16()
{
    return m_i16Val;
}

DrError DryadMTagInt16::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteInt16Property(GetTagValue(), m_i16Val);
}


DryadMTagUInt16::DryadMTagUInt16(UInt16 tag, UInt16 val) :
    DryadMTag(tag, DrPropertyTagType_UInt16)
{
    m_uI16Val = val;
}

DryadMTagUInt16::~DryadMTagUInt16()
{
}

DryadMTagUInt16* DryadMTagUInt16::Create(UInt16 tag, UInt16 val)
{
    return new DryadMTagUInt16(tag, val);
}

DrError DryadMTagUInt16::ReadFromStream(DrMemoryReader* reader,
                                        UInt16 enumID, UInt32 dataLen,
                                        DryadMTagUInt16Ref* outTag)
{
    UInt16 val;
    DrError cse = reader->ReadNextUInt16Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

UInt16 DryadMTagUInt16::GetUInt16()
{
    return m_uI16Val;
}

DrError DryadMTagUInt16::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteUInt16Property(GetTagValue(), m_uI16Val);
}


DryadMTagInt32::DryadMTagInt32(UInt16 tag, Int32 val) :
    DryadMTag(tag, DrPropertyTagType_Int32)
{
    m_i32Val = val;
}

DryadMTagInt32::~DryadMTagInt32()
{
}

DryadMTagInt32* DryadMTagInt32::Create(UInt16 tag, Int32 val)
{
    return new DryadMTagInt32(tag, val);
}

DrError DryadMTagInt32::ReadFromStream(DrMemoryReader* reader,
                                       UInt16 enumID, UInt32 dataLen,
                                       DryadMTagInt32Ref* outTag)
{
    Int32 val;
    DrError cse = reader->ReadNextInt32Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

Int32 DryadMTagInt32::GetInt32()
{
    return m_i32Val;
}

DrError DryadMTagInt32::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteInt32Property(GetTagValue(), m_i32Val);
}


DryadMTagUInt32::DryadMTagUInt32(UInt16 tag, UInt32 val) :
    DryadMTag(tag, DrPropertyTagType_UInt32)
{
    m_uI32Val = val;
}

DryadMTagUInt32::~DryadMTagUInt32()
{
}

DryadMTagUInt32* DryadMTagUInt32::Create(UInt16 tag, UInt32 val)
{
    return new DryadMTagUInt32(tag, val);
}

DrError DryadMTagUInt32::ReadFromStream(DrMemoryReader* reader,
                                        UInt16 enumID, UInt32 dataLen,
                                        DryadMTagUInt32Ref* outTag)
{
    UInt32 val;
    DrError cse = reader->ReadNextUInt32Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

UInt32 DryadMTagUInt32::GetUInt32()
{
    return m_uI32Val;
}

DrError DryadMTagUInt32::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteUInt32Property(GetTagValue(), m_uI32Val);
}


DryadMTagInt64::DryadMTagInt64(UInt16 tag, Int64 val) :
    DryadMTag(tag, DrPropertyTagType_Int64)
{
    m_i64Val = val;
}

DryadMTagInt64::~DryadMTagInt64()
{
}

DryadMTagInt64* DryadMTagInt64::Create(UInt16 tag, Int64 val)
{
    return new DryadMTagInt64(tag, val);
}

DrError DryadMTagInt64::ReadFromStream(DrMemoryReader* reader,
                                       UInt16 enumID, UInt32 dataLen,
                                       DryadMTagInt64Ref* outTag)
{
    Int64 val;
    DrError cse = reader->ReadNextInt64Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

Int64 DryadMTagInt64::GetInt64()
{
    return m_i64Val;
}

DrError DryadMTagInt64::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteInt64Property(GetTagValue(), m_i64Val);
}


DryadMTagUInt64::DryadMTagUInt64(UInt16 tag, UInt64 val) :
    DryadMTag(tag, DrPropertyTagType_UInt64)
{
    m_uI64Val = val;
}

DryadMTagUInt64::~DryadMTagUInt64()
{
}

DryadMTagUInt64* DryadMTagUInt64::Create(UInt16 tag, UInt64 val)
{
    return new DryadMTagUInt64(tag, val);
}

DrError DryadMTagUInt64::ReadFromStream(DrMemoryReader* reader,
                                        UInt16 enumID, UInt32 dataLen,
                                        DryadMTagUInt64Ref* outTag)
{
    UInt64 val;
    DrError cse = reader->ReadNextUInt64Property(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

UInt64 DryadMTagUInt64::GetUInt64()
{
    return m_uI64Val;
}

DrError DryadMTagUInt64::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteUInt64Property(GetTagValue(), m_uI64Val);
}

DryadMTagDouble::DryadMTagDouble(UInt16 tag, double val) :
    DryadMTag(tag, DrPropertyTagType_Double)
{
    m_doubleVal = val;
}

DryadMTagDouble::~DryadMTagDouble()
{
}

DryadMTagDouble* DryadMTagDouble::Create(UInt16 tag, double val)
{
    return new DryadMTagDouble(tag, val);
}

DrError DryadMTagDouble::ReadFromStream(DrMemoryReader* reader,
                                        UInt16 enumID, UInt32 dataLen,
                                        DryadMTagDoubleRef* outTag)
{
    double val;
    DrError cse = reader->ReadNextDoubleProperty(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

double DryadMTagDouble::GetDouble()
{
    return m_doubleVal;
}

DrError DryadMTagDouble::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteDoubleProperty(GetTagValue(), m_doubleVal);
}

DryadMTagString::DryadMTagString(UInt16 enumID, const char* val) :
    DryadMTag(enumID, DrPropertyTagType_String)
{
    m_string.Set(val);
}

DryadMTagString::~DryadMTagString()
{
}

DryadMTagString* DryadMTagString::Create(UInt16 enumID, const char* val)
{
    return new DryadMTagString(enumID, val);
}

DrError DryadMTagString::ReadFromStream(DrMemoryReader* reader,
                                        UInt16 enumID, UInt32 dataLen,
                                        DryadMTagStringRef* outTag)
{
    DryadMTagString* tag = Create(enumID, NULL);
    DrError cse =
        reader->ReadNextStringProperty(enumID,
                                       tag->GetWritableString(dataLen),
                                       dataLen);
    if (cse == DrError_OK)
    {
        outTag->Attach(tag);
    }
    else
    {
        tag->DecRef();
        (*outTag) = NULL;
    }
    return cse;
}

const char* DryadMTagString::GetString()
{
    return m_string.GetString();
}

char* DryadMTagString::GetWritableString(size_t dataLen)
{
    return m_string.GetWritableBuffer(dataLen);
}

DrError DryadMTagString::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteLongStringProperty(GetTagValue(), m_string.GetString());
}


DryadMTagGuid::DryadMTagGuid(UInt16 enumID, const DrGuid* val) :
    DryadMTag(enumID, DrPropertyTagType_Guid)
{
    m_guid.Set(*val);
}

DryadMTagGuid::~DryadMTagGuid()
{
}

DryadMTagGuid* DryadMTagGuid::Create(UInt16 enumID, const DrGuid* val)
{
    return new DryadMTagGuid(enumID, val);
}

DrError DryadMTagGuid::ReadFromStream(DrMemoryReader* reader,
                                      UInt16 enumID, UInt32 dataLen,
                                      DryadMTagGuidRef* outTag)
{
    DrGuid guid;
    DrError cse =
        reader->ReadNextGuidProperty(enumID, &guid);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, &guid));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

const DrGuid* DryadMTagGuid::GetGuid()
{
    return &m_guid;
}

DrError DryadMTagGuid::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteGuidProperty(GetTagValue(), m_guid);
}


DryadMTagTimeStamp::DryadMTagTimeStamp(UInt16 tag, DrTimeStamp val) :
    DryadMTag(tag, DrPropertyTagType_TimeStamp)
{
    m_val = val;
}

DryadMTagTimeStamp::~DryadMTagTimeStamp()
{
}

DryadMTagTimeStamp* DryadMTagTimeStamp::Create(UInt16 tag, DrTimeStamp val)
{
    return new DryadMTagTimeStamp(tag, val);
}

DrError DryadMTagTimeStamp::ReadFromStream(DrMemoryReader* reader,
                                           UInt16 enumID, UInt32 dataLen,
                                           DryadMTagTimeStampRef* outTag)
{
    DrTimeStamp val;
    DrError cse = reader->ReadNextTimeStampProperty(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

DrTimeStamp DryadMTagTimeStamp::GetTimeStamp()
{
    return m_val;
}

DrError DryadMTagTimeStamp::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteTimeStampProperty(GetTagValue(), m_val);
}


DryadMTagTimeInterval::DryadMTagTimeInterval(UInt16 tag, DrTimeInterval val) :
    DryadMTag(tag, DrPropertyTagType_TimeInterval)
{
    m_val = val;
}

DryadMTagTimeInterval::~DryadMTagTimeInterval()
{
}

DryadMTagTimeInterval* DryadMTagTimeInterval::Create(UInt16 tag,
                                                     DrTimeInterval val)
{
    return new DryadMTagTimeInterval(tag, val);
}

DrError DryadMTagTimeInterval::ReadFromStream(DrMemoryReader* reader,
                                              UInt16 enumID, UInt32 dataLen,
                                              DryadMTagTimeIntervalRef* outTag)
{
    DrTimeInterval val;
    DrError cse = reader->ReadNextTimeIntervalProperty(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

DrTimeInterval DryadMTagTimeInterval::GetTimeInterval()
{
    return m_val;
}

DrError DryadMTagTimeInterval::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteTimeIntervalProperty(GetTagValue(), m_val);
}


DryadMTagDrError::DryadMTagDrError(UInt16 tag, DrError val) :
    DryadMTag(tag, DrPropertyTagType_DrError)
{
    m_val = val;
}

DryadMTagDrError::~DryadMTagDrError()
{
}

DryadMTagDrError* DryadMTagDrError::Create(UInt16 tag, DrError val)
{
    return new DryadMTagDrError(tag, val);
}

DrError DryadMTagDrError::ReadFromStream(DrMemoryReader* reader,
                                         UInt16 enumID, UInt32 dataLen,
                                         DryadMTagDrErrorRef* outTag)
{
    DrError val;
    DrError cse = reader->ReadNextDrErrorProperty(enumID, &val);
    if (cse == DrError_OK)
    {
        outTag->Attach(Create(enumID, val));
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

DrError DryadMTagDrError::GetDrError()
{
    return m_val;
}

DrError DryadMTagDrError::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteDrErrorProperty(GetTagValue(), m_val);
}


DryadMTagMetaData::DryadMTagMetaData(UInt16 tag, DryadMetaData* val,
                                     bool marshalAsAggregate) :
    DryadMTag(tag, DryadPropertyTagType_MetaData)
{
    m_val = val;
    m_marshalAsAggregate = marshalAsAggregate;
}

DryadMTagMetaData::~DryadMTagMetaData()
{
}

DryadMTagMetaData* DryadMTagMetaData::Create(UInt16 tag, DryadMetaData* val,
                                             bool marshalAsAggregate)
{
    return new DryadMTagMetaData(tag, val, marshalAsAggregate);
}

DrError DryadMTagMetaData::
    ReadFromStreamInAggregate(DrMemoryReader* reader,
                              DryadMTagMetaDataRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DryadMetaDataParser nestedParser;
        err = reader->ReadAggregate(tagValue, &nestedParser, NULL);
        if (err == DrError_OK)
        {
            DryadMetaData* parsed = nestedParser.GetMetaData();
            outTag->Attach(Create(tagValue, parsed, true));
        }
        else
        {
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DrError DryadMTagMetaData::ReadFromArray(UInt16 tagValue,
                                         const void* data, UInt32 dataLen,
                                         DryadMTagMetaDataRef* outTag)
{
    DryadMetaDataParser parser;
    DrError err = parser.ParseBuffer(data, dataLen);
    if (err == DrError_OK)
    {
        DryadMetaData* parsed = parser.GetMetaData();
        outTag->Attach(Create(tagValue, parsed, false));
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DryadMetaData* DryadMTagMetaData::GetMetaData()
{
    return m_val;
}

DrError DryadMTagMetaData::Serialize(DrMemoryWriter* writer)
{
    return m_val->WriteAsAggregate(writer, GetTagValue(), true);
}

DryadMTag* DryadMTagMetaData::Clone()
{
    DryadMetaDataRef clonedValue;
    m_val->Clone(&clonedValue);
    return Create(GetTagValue(), clonedValue, m_marshalAsAggregate);
}


DryadMTagVertexCommand::DryadMTagVertexCommand(UInt16 tag,
                                               DVertexCommand val) :
    DryadMTag(tag, DryadPropertyTagType_VertexCommand)
{
    m_val = val;
}

DryadMTagVertexCommand::~DryadMTagVertexCommand()
{
}

DryadMTagVertexCommand* DryadMTagVertexCommand::Create(UInt16 tag,
                                                       DVertexCommand val)
{
    return new DryadMTagVertexCommand(tag, val);
}

DrError DryadMTagVertexCommand::
    ReadFromStream(DrMemoryReader* reader,
                   UInt16 enumID, UInt32 dataLen,
                   DryadMTagVertexCommandRef* outTag)
{
    UInt32 val;
    DrError cse = reader->ReadNextUInt32Property(enumID, &val);
    if (cse == DrError_OK)
    {
        if (val < DVertexCommand_Max)
        {
            outTag->Attach(Create(enumID, (DVertexCommand) val));
        }
        else
        {
            (*outTag) = NULL;
            cse = DrError_InvalidProperty;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return cse;
}

DVertexCommand DryadMTagVertexCommand::GetVertexCommand()
{
    return m_val;
}

DrError DryadMTagVertexCommand::Serialize(DrMemoryWriter* writer)
{
    return writer->WriteUInt32Property(GetTagValue(), (UInt32) m_val);
}


DryadMTagInputChannelDescription::
    DryadMTagInputChannelDescription(UInt16 tag,
                                     DryadInputChannelDescription* val) :
    DryadMTag(tag, DryadPropertyTagType_InputChannelDescription)
{
    m_val = val;
}

DryadMTagInputChannelDescription::~DryadMTagInputChannelDescription()
{
    delete m_val;
}

DryadMTagInputChannelDescription* DryadMTagInputChannelDescription::
    Create(UInt16 tag, DryadInputChannelDescription* val)
{
    return new DryadMTagInputChannelDescription(tag, val);
}

DrError DryadMTagInputChannelDescription::
    ReadFromStream(DrMemoryReader* reader,
                   DryadMTagInputChannelDescriptionRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DryadInputChannelDescription* cDescription =
            new DryadInputChannelDescription();
        err = reader->ReadAggregate(tagValue, cDescription, NULL);
        if (err == DrError_OK)
        {
            outTag->Attach(Create(tagValue, cDescription));
        }
        else
        {
            delete cDescription;
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DryadInputChannelDescription* DryadMTagInputChannelDescription::
    GetInputChannelDescription()
{
    return m_val;
}

DrError DryadMTagInputChannelDescription::
    Serialize(DrMemoryWriter* writer)
{
    return m_val->Serialize(writer);
}


DryadMTagOutputChannelDescription::
    DryadMTagOutputChannelDescription(UInt16 tag,
                                      DryadOutputChannelDescription* val) :
    DryadMTag(tag, DryadPropertyTagType_OutputChannelDescription)
{
    m_val = val;
}

DryadMTagOutputChannelDescription::~DryadMTagOutputChannelDescription()
{
    delete m_val;
}

DryadMTagOutputChannelDescription* DryadMTagOutputChannelDescription::
    Create(UInt16 tag, DryadOutputChannelDescription* val)
{
    return new DryadMTagOutputChannelDescription(tag, val);
}

DrError DryadMTagOutputChannelDescription::
    ReadFromStream(DrMemoryReader* reader,
                   DryadMTagOutputChannelDescriptionRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DryadOutputChannelDescription* cDescription =
            new DryadOutputChannelDescription();
        err = reader->ReadAggregate(tagValue, cDescription, NULL);
        if (err == DrError_OK)
        {
            outTag->Attach(Create(tagValue, cDescription));
        }
        else
        {
            delete cDescription;
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DryadOutputChannelDescription* DryadMTagOutputChannelDescription::
    GetOutputChannelDescription()
{
    return m_val;
}

DrError DryadMTagOutputChannelDescription::
    Serialize(DrMemoryWriter* writer)
{
    return m_val->Serialize(writer);
}


DryadMTagVertexProcessStatus::
    DryadMTagVertexProcessStatus(UInt16 tag,
                                 DVertexProcessStatus* val) :
    DryadMTag(tag, DryadPropertyTagType_VertexProcessStatus)
{
    m_val = val;
}

DryadMTagVertexProcessStatus::~DryadMTagVertexProcessStatus()
{
}

DryadMTagVertexProcessStatus* DryadMTagVertexProcessStatus::
    Create(UInt16 tag, DVertexProcessStatus* val)
{
    return new DryadMTagVertexProcessStatus(tag, val);
}

DrError DryadMTagVertexProcessStatus::
    ReadFromStream(DrMemoryReader* reader,
                   DryadMTagVertexProcessStatusRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DrRef<DVertexProcessStatus> pStatus;
        pStatus.Attach(new DVertexProcessStatus());
        err = reader->ReadAggregate(tagValue, pStatus, NULL);
        if (err == DrError_OK)
        {
            outTag->Attach(Create(tagValue, pStatus));
        }
        else
        {
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DVertexProcessStatus* DryadMTagVertexProcessStatus::GetVertexProcessStatus()
{
    return m_val;
}

DrError DryadMTagVertexProcessStatus::Serialize(DrMemoryWriter* writer)
{
    return m_val->Serialize(writer);
}


DryadMTagVertexStatus::DryadMTagVertexStatus(UInt16 tag,
                                             DVertexStatus* val) :
    DryadMTag(tag, DryadPropertyTagType_VertexStatus)
{
    m_val = val;
}

DryadMTagVertexStatus::~DryadMTagVertexStatus()
{
}

DryadMTagVertexStatus* DryadMTagVertexStatus::
    Create(UInt16 tag, DVertexStatus* val)
{
    return new DryadMTagVertexStatus(tag, val);
}

DrError DryadMTagVertexStatus::
    ReadFromStream(DrMemoryReader* reader,
                   DryadMTagVertexStatusRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DrRef<DVertexStatus> vStatus;
        vStatus.Attach(new DVertexStatus());
        err = reader->ReadAggregate(tagValue, vStatus, NULL);
        if (err == DrError_OK)
        {
            outTag->Attach(Create(tagValue, vStatus));
        }
        else
        {
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DVertexStatus* DryadMTagVertexStatus::GetVertexStatus()
{
    return m_val;
}

DrError DryadMTagVertexStatus::Serialize(DrMemoryWriter* writer)
{
    return m_val->Serialize(writer);
}


DryadMTagVertexCommandBlock::
    DryadMTagVertexCommandBlock(UInt16 tag,
                                DVertexCommandBlock* val) :
    DryadMTag(tag, DryadPropertyTagType_VertexCommandBlock)
{
    m_val = val;
}

DryadMTagVertexCommandBlock::~DryadMTagVertexCommandBlock()
{
}

DryadMTagVertexCommandBlock* DryadMTagVertexCommandBlock::
    Create(UInt16 tag, DVertexCommandBlock* val)
{
    return new DryadMTagVertexCommandBlock(tag, val);
}

DrError DryadMTagVertexCommandBlock::
    ReadFromStream(DrMemoryReader* reader,
                   DryadMTagVertexCommandBlockRef* outTag)
{
    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        DrRef<DVertexCommandBlock> cBlock;
        cBlock.Attach(new DVertexCommandBlock());
        err = reader->ReadAggregate(tagValue, cBlock, NULL);
        if (err == DrError_OK)
        {
            outTag->Attach(Create(tagValue, cBlock));
        }
        else
        {
            (*outTag) = NULL;
        }
    }
    else
    {
        (*outTag) = NULL;
    }
    return err;
}

DVertexCommandBlock* DryadMTagVertexCommandBlock::GetVertexCommandBlock()
{
    return m_val;
}

DrError DryadMTagVertexCommandBlock::Serialize(DrMemoryWriter* writer)
{
    return m_val->Serialize(writer);
}
