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

#include <DrCommon.h>
#include <dryaderrordef.h>
#include <dryadtagsdef.h>
#include <dryadmetadata.h>

#ifdef DECLARE_DRPROPERTYTYPE
#undef DECLARE_DRPROPERTYTYPE
#endif

#define DECLARE_DRPROPERTYTYPE(type) DrError DrMetaDataTagFactory_##type(DrMemoryReader*,UInt16,UInt32,DryadMetaDataParser*);

#include "DrPropertyType.h"

#undef DECLARE_DRPROPERTYTYPE

#ifdef DECLARE_DRYADPROPERTYTYPE
#undef DECLARE_DRYADPROPERTYTYPE
#endif

#define DECLARE_DRYADPROPERTYTYPE(type) DrError DryadMetaDataTagFactory_##type(DrMemoryReader*,UInt16,UInt32,DryadMetaDataParser*);

#include "DryadPropertyType.h"

#undef DECLARE_DRYADPROPERTYTYPE

#ifdef DEFINE_DRYADTAG
#undef DEFINE_DRYADTAG
#endif

#define DEFINE_DRYADTAG(name,id,desc,type) \
    DrError DryadMetaDataAggregateFactory_##type(DrMemoryReader*,DryadMTagRef*);

#include "dryadtags.h"

#undef DEFINE_DRYADTAG


#pragma unmanaged

struct DryadTypeFactoryRecord
{
    UInt16                               m_typeCode;
    DryadMetaDataParser::TagFactory*     m_factory;
};

//
// todo: Understand the preprocessor magic herein contained
// todo: if cosmos specific, remove
//
static DryadTypeFactoryRecord s_cosmosTypeFactory[] = {

#ifdef DECLARE_DRPROPERTYTYPE
#undef DECLARE_DRPROPERTYTYPE
#endif

#define DECLARE_DRPROPERTYTYPE(type) \
    {DrPropertyTagType_##type,DrMetaDataTagFactory_##type},

#include "DrPropertyType.h"

#undef DECLARE_DRPROPERTYTYPE
};

//
// todo: Understand the preprocessor magic herein contained
//
static DryadTypeFactoryRecord s_dryadTypeFactory[] = {

#ifdef DECLARE_DRYADPROPERTYTYPE
#undef DECLARE_DRYADPROPERTYTYPE
#endif

#define DECLARE_DRYADPROPERTYTYPE(type) \
    {DryadPropertyTagType_##type,DryadMetaDataTagFactory_##type},

#include "DryadPropertyType.h"

#undef DECLARE_DRYADPROPERTYTYPE
};

struct DryadMetaDataTypeRecord
{
    UInt16                               m_enumId;
    UInt16                               m_typeCode;
    DryadMetaDataParser::TagFactory*     m_factory;
};

static DryadMetaDataTypeRecord s_cosmosTagTypes[] = {

#ifdef DEFINE_DRPROPERTY
#undef DEFINE_DRPROPERTY
#endif

#define DEFINE_DRPROPERTY(name,id,type,desc) \
    {name,DrPropertyTagType_##type,DrMetaDataTagFactory_##type},

#include "DrProperties.h"

};

static DryadMetaDataTypeRecord s_dryadTagTypes[] = {

#ifdef DEFINE_DRYADPROPERTY
#undef DEFINE_DRYADPROPERTY
#endif

#define DEFINE_DRYADPROPERTY(name,id,type,desc) \
    {name,DryadPropertyTagType_##type,DryadMetaDataTagFactory_##type},

#include "DryadProperties.h"

#undef DEFINE_DRYADPROPERTY
#undef DEFINE_DRPROPERTY
};

struct DryadMetaDataAggregateFactory
{
    UInt16                                     m_aggregateId;
    DryadMetaDataParser::AggregateFactory*     m_factory;
};

static DryadMetaDataAggregateFactory s_dryadAggregateFactory[] = {

#ifdef DEFINE_DRYADTAG
#undef DEFINE_DRYADTAG
#endif

#define DEFINE_DRYADTAG(name,id,desc,type) \
    {name,DryadMetaDataAggregateFactory_##type},

#include "dryadtags.h"

#undef DEFINE_DRYADTAG
};

//
// Define maps between integer ids and various value types
//
typedef std::map<UInt16,DryadMetaDataParser::TagFactory*> TagFactoryMap;
typedef std::map<UInt16,DryadMetaDataParser::AggregateFactory*>
    AggregateFactoryMap;
typedef std::map<UInt16,DryadMetaDataTypeRecord*> TagTypeRecordMap;

//
// allocate these on the heap during the DryadInitMetaDataTable
// routine for purposes of sanitation: we don't really want to have
// stl constructors/destructors being called statically if we don't
// have to 
static TagFactoryMap*         s_factoryTable;
static AggregateFactoryMap*   s_aggregateFactoryTable;
static TagTypeRecordMap*      s_propertyTypeTable;

DryadMetaData::DryadMetaData()
{
}

DryadMetaData::~DryadMetaData()
{
    TagListIter iter;
    for (iter = m_elementList.begin(); iter != m_elementList.end(); ++iter)
    {
        (*iter)->DecRef();
    }
}

void DryadMetaData::Create(DrRef<DryadMetaData> * dstMetaData)
{
    dstMetaData->Attach(new DryadMetaData());
}

bool DryadMetaData::Append(DryadMTag* tag, bool allowDuplicateNames)
{
    bool appended = true;

    {
        AutoCriticalSection acs(&m_baseCS);

        if (!allowDuplicateNames)
        {
            TagMapIter iter = m_elementMap.find(tag->GetTagValue());
            if (iter != m_elementMap.end())
            {
                appended = false;
            }
        }

        if (appended)
        {
            tag->IncRef();
            m_elementMap.insert(std::make_pair(tag->GetTagValue(), tag));
            m_elementList.push_back(tag);
        }
    }

    return appended;
}

void DryadMetaData::AppendMetaDataTags(DryadMetaData* metaData,
                                       bool allowDuplicateNames)
{
    TagListIter endIter;

    {
        AutoCriticalSection outerAcs(&(metaData->m_baseCS));

        TagListIter iter = metaData->LookUpInSequence(NULL, &endIter);

        {
            AutoCriticalSection acs(&m_baseCS);

            while (iter != endIter)
            {
                DryadMTagRef tag;
                (*iter)->Clone(&tag);
                Append(tag, allowDuplicateNames);
                ++iter;
            }
        }
    }
}

bool DryadMetaData::Replace(DryadMTag* newTag, DryadMTag* oldTag)
{
    bool replaced = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(oldTag->GetTagValue() == newTag->GetTagValue());

        TagMapIter iter = m_elementMap.find(oldTag->GetTagValue());
        while (iter != m_elementMap.end() &&
               iter->second != oldTag)
        {
            ++iter;
        }

        if (iter != m_elementMap.end())
        {
            iter->second = newTag;
            oldTag->DecRef();
            replaced = true;
        }
    }

    return replaced;
}

bool DryadMetaData::Remove(DryadMTag* tag)
{
    bool removed = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        TagMapIter mIter = m_elementMap.find(tag->GetTagValue());
        while (mIter != m_elementMap.end() &&
               mIter->second != tag)
        {
            ++mIter;
        }

        if (mIter != m_elementMap.end())
        {
            TagListIter lIter = m_elementList.begin();
            while (lIter != m_elementList.end() &&
                   (*lIter) != tag)
            {
                ++lIter;
            }
            LogAssert(lIter != m_elementList.end());
            m_elementList.erase(lIter);
            m_elementMap.erase(mIter);
            tag->DecRef();
            removed = true;
        }
    }

    return removed;
}

DryadMTag* DryadMetaData::LookUpTag(UInt16 enumId)
{
    DryadMTag* tag = NULL;

    {
        AutoCriticalSection acs(&m_baseCS);

        TagMapIter iter = m_elementMap.find(enumId);
        if (iter != m_elementMap.end())
        {
            ++iter;
            if (iter == m_elementMap.end() || iter->first != enumId)
            {
                --iter;
                tag = iter->second;
            }
        }
    }

    return tag;
}

DryadMetaData::TagMapIter
    DryadMetaData::LookUpMulti(UInt16 enumId,
                               DryadMetaData::TagMapIter* pEndIter)
{
    DryadMetaData::TagMapIter startIter = m_elementMap.find(enumId);
    DryadMetaData::TagMapIter endIter = startIter;
    while (endIter != m_elementMap.end() && enumId == endIter->first)
    {
        ++endIter;
    }

    *pEndIter = endIter;
    return startIter;
}

DryadMetaData::TagListIter
    DryadMetaData::LookUpInSequence(DryadMTag* tag,
                                    DryadMetaData::TagListIter* pEndIter)
{
    *pEndIter = m_elementList.end();

    if (tag == NULL)
    {
        return m_elementList.begin();
    }

    DryadMetaData::TagListIter iter = m_elementList.begin();
    while (iter != m_elementList.end() && (*iter) != tag)
    {
        ++iter;
    }

    return iter;
}

void DryadMetaData::Clone(DryadMetaDataRef* dstMetaData)
{
    DryadMetaData* clone = new DryadMetaData();

    TagListIter lIter;

    {
        AutoCriticalSection acs(&m_baseCS);

        for (lIter = m_elementList.begin();
             lIter != m_elementList.end();
             ++lIter)
        {
            DryadMTagRef tag;
            (*lIter)->Clone(&tag);
            DryadMTag* freeTag = tag.Detach();
            clone->m_elementList.push_back(freeTag);
            clone->m_elementMap.insert(std::make_pair(freeTag->GetTagValue(),
                                                      freeTag));
        }
    }

    dstMetaData->Attach(clone);
}

void DryadMetaData::AddError(DrError errorCode)
{
    DryadMTagRef tag;
    tag.Attach(DryadMTagDrError::Create(Prop_Dryad_ErrorCode, errorCode));
    Append(tag, false);
}

void DryadMetaData::AddErrorWithDescription(DrError errorCode,
                                            const char* errorDescription)
{
    DryadMTagRef tag;
    tag.Attach(DryadMTagDrError::Create(Prop_Dryad_ErrorCode, errorCode));
    Append(tag, false);
    tag.Attach(DryadMTagString::Create(Prop_Dryad_ErrorString,
                                       errorDescription));
    Append(tag, false);
}

bool DryadMetaData::GetErrorCode(DrError* pError)
{
    DryadMTagDrError* tag = LookUpDrErrorTag(Prop_Dryad_ErrorCode);
    if (tag != NULL)
    {
        *pError = tag->GetDrError();
        return true;
    }
    else
    {
        return false;
    }
}

const char* DryadMetaData::GetErrorString()
{
    DryadMTagString* tag = LookUpStringTag(Prop_Dryad_ErrorString);
    if (tag == NULL)
    {
        return NULL;
    }

    return tag->GetString();
}

void DryadMetaData::Serialize(DrMemoryWriter* writer)
{
    {
        AutoCriticalSection acs(&m_baseCS);

        if (m_cachedSerialization.Ptr() != NULL)
        {
            Size_t available;
            void* data =
                m_cachedSerialization->GetDataAddress(0, &available, NULL);
            LogAssert(available < 0x100000000);
            LogAssert(available >= m_cachedSerialization->GetAvailableSize());

            writer->WriteBytes(data,
                               m_cachedSerialization->GetAvailableSize());
        }
        else
        {
            DryadMetaData::TagListIter iter;
            for (iter = m_elementList.begin(); iter != m_elementList.end();
                 ++iter)
            {
                (*iter)->Serialize(writer);
            }
        }
    }
}

void DryadMetaData::CacheSerialization()
{
    m_cachedSerialization = NULL;

    DrRef<DrSimpleHeapBuffer> buffer;
    buffer.Attach(new DrSimpleHeapBuffer());
    {
        DrMemoryBufferWriter writer(buffer);
        Serialize(&writer);
    }
    /* don't assign buffer to m_cachedSerialization until after the
       call to Serialize to make sure something actually happens
       there */
    m_cachedSerialization = buffer;
}

DrMemoryBuffer* DryadMetaData::SerializeToBuffer()
{
    DrMemoryBuffer* buffer = new DrSimpleHeapBuffer();

    {
        DrMemoryBufferWriter writer(buffer);
        Serialize(&writer);
        DrError errTmp = writer.CloseMemoryWriter();
        LogAssert(errTmp == DrError_OK);
    }

    return buffer;
}

char* DryadMetaData::GetText()
{
    DrMemoryBuffer* buffer = SerializeToBuffer();
    DrMemoryBufferReader reader(buffer);
    buffer->DecRef();

    buffer = new DrSimpleHeapBuffer();
    DrMemoryBufferWriter writer(buffer);

/* JC
    DrPropertyDumper dumper;

    dumper.SetWriter(&writer);

    dumper.PutNestedPropertyList(&reader);
*/
    DrError errTmp = writer.CloseMemoryWriter();
    LogAssert(errTmp == DrError_OK);

    size_t textLength = buffer->GetAvailableSize();
    char* textBuffer = new char[textLength+1];
    LogAssert(textBuffer != NULL);
    buffer->Read(0, textBuffer, textLength);
    buffer->DecRef();
    textBuffer[textLength] = '\0';

    return textBuffer;
}

DrError DryadMetaData::WriteAsProperty(DrMemoryWriter* writer,
                                       UInt16 propertyTag,
                                       bool writeIfEmpty)
{
    DrMemoryBuffer* buffer = SerializeToBuffer();

    const BYTE* data = NULL;
    Size_t length = buffer->GetAvailableSize();

    DrError err = DrError_OK;

    if (length > 0)
    {
        LogAssert(length < 0x100000000UL);
        DrMemoryBufferReader reader(buffer);
        buffer->DecRef();

        reader.ReadBytes(length, &data);
        LogAssert(data != NULL);

        err = writer->WriteAnySizeBlobProperty(propertyTag, length, data);
    }
    else if (writeIfEmpty)
    {
        err = writer->WriteEmptyProperty(propertyTag);
    }

    return err;
}

DrError DryadMetaData::WriteAsAggregate(DrMemoryWriter* writer,
                                        UInt16 propertyTag,
                                        bool writeIfEmpty)
{
    writer->WriteUInt16Property(Prop_Dryad_BeginTag, propertyTag);
    Serialize(writer);
    return writer->WriteUInt16Property(Prop_Dryad_EndTag, propertyTag);
}

DryadMTagUnknown* DryadMetaData::LookUpUnknownTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Unknown)
        {
            tag = NULL;
        }
    }
    return (DryadMTagUnknown *) tag;
}

DryadMTagVoid* DryadMetaData::LookUpVoidTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_Void)
        {
            tag = NULL;
        }
    }
    return (DryadMTagVoid *) tag;
}

DryadMTagBoolean* DryadMetaData::LookUpBooleanTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Boolean)
        {
            tag = NULL;
        }
    }
    return (DryadMTagBoolean *) tag;
}

DryadMTagInt16* DryadMetaData::LookUpInt16Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Int16)
        {
            tag = NULL;
        }
    }
    return (DryadMTagInt16 *) tag;
}

DryadMTagUInt16* DryadMetaData::LookUpUInt16Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_UInt16)
        {
            tag = NULL;
        }
    }
    return (DryadMTagUInt16 *) tag;
}

DryadMTagInt32* DryadMetaData::LookUpInt32Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Int32)
        {
            tag = NULL;
        }
    }
    return (DryadMTagInt32 *) tag;
}

DryadMTagUInt32* DryadMetaData::LookUpUInt32Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_UInt32)
        {
            tag = NULL;
        }
    }
    return (DryadMTagUInt32 *) tag;
}

DryadMTagInt64* DryadMetaData::LookUpInt64Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Int64)
        {
            tag = NULL;
        }
    }
    return (DryadMTagInt64 *) tag;
}

DryadMTagUInt64* DryadMetaData::LookUpUInt64Tag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_UInt64)
        {
            tag = NULL;
        }
    }
    return (DryadMTagUInt64 *) tag;
}

DryadMTagString* DryadMetaData::LookUpStringTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_String)
        {
            tag = NULL;
        }
    }
    return (DryadMTagString *) tag;
}

DryadMTagGuid* DryadMetaData::LookUpGuidTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_Guid)
        {
            tag = NULL;
        }
    }
    return (DryadMTagGuid *) tag;
}

DryadMTagTimeStamp* DryadMetaData::LookUpTimeStampTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_TimeStamp)
        {
            tag = NULL;
        }
    }
    return (DryadMTagTimeStamp *) tag;
}

DryadMTagTimeInterval* DryadMetaData::LookUpTimeIntervalTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_TimeInterval)
        {
            tag = NULL;
        }
    }
    return (DryadMTagTimeInterval *) tag;
}

DryadMTagDrError* DryadMetaData::LookUpDrErrorTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DrPropertyTagType_DrError)
        {
            tag = NULL;
        }
    }
    return (DryadMTagDrError *) tag;
}

DryadMTagMetaData* DryadMetaData::LookUpMetaDataTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_MetaData)
        {
            tag = NULL;
        }
    }
    return (DryadMTagMetaData *) tag;
}

DryadMTagVertexCommand* DryadMetaData::LookUpVertexCommandTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_VertexCommand)
        {
            tag = NULL;
        }
    }
    return (DryadMTagVertexCommand *) tag;
}

DryadMTagInputChannelDescription*
    DryadMetaData::LookUpInputChannelDescriptionTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_InputChannelDescription)
        {
            tag = NULL;
        }
    }
    return (DryadMTagInputChannelDescription *) tag;
}

DryadMTagOutputChannelDescription*
    DryadMetaData::LookUpOutputChannelDescriptionTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_OutputChannelDescription)
        {
            tag = NULL;
        }
    }
    return (DryadMTagOutputChannelDescription *) tag;
}

DryadMTagVertexProcessStatus*
    DryadMetaData::LookUpVertexProcessStatusTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_VertexProcessStatus)
        {
            tag = NULL;
        }
    }
    return (DryadMTagVertexProcessStatus *) tag;
}

DryadMTagVertexStatus* DryadMetaData::LookUpVertexStatusTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_VertexStatus)
        {
            tag = NULL;
        }
    }
    return (DryadMTagVertexStatus *) tag;
}

DryadMTagVertexCommandBlock*
    DryadMetaData::LookUpVertexCommandBlockTag(UInt16 enumID)
{
    DryadMTag* tag = LookUpTag(enumID);
    if (tag != NULL)
    {
        if (tag->GetType() != DryadPropertyTagType_VertexCommandBlock)
        {
            tag = NULL;
        }
    }
    return (DryadMTagVertexCommandBlock *) tag;
}

DrError DryadMetaData::LookUpVoid(UInt16 enumID)
{
    DryadMTagVoid* tag = LookUpVoidTag(enumID);
    return (tag == NULL) ? DrError_InvalidProperty : DrError_OK;
}

DrError DryadMetaData::LookUpBoolean(UInt16 enumID, bool* pVal /* out */)
{
    DryadMTagBoolean* tag = LookUpBooleanTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetBoolean();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpUInt16(UInt16 enumID, UInt16* pVal /* out */)
{
    DryadMTagUInt16* tag = LookUpUInt16Tag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetUInt16();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpInt32(UInt16 enumID, Int32* pVal /* out */)
{
    DryadMTagInt32* tag = LookUpInt32Tag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetInt32();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpUInt32(UInt16 enumID, UInt32* pVal /* out */)
{
    DryadMTagUInt32* tag = LookUpUInt32Tag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetUInt32();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpInt64(UInt16 enumID, Int64* pVal /* out */)
{
    DryadMTagInt64* tag = LookUpInt64Tag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetInt64();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpUInt64(UInt16 enumID, UInt64* pVal /* out */)
{
    DryadMTagUInt64* tag = LookUpUInt64Tag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetUInt64();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpString(UInt16 enumID, const char** pVal /* out */)
{
    DryadMTagString* tag = LookUpStringTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetString();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpGuid(UInt16 enumID, const DrGuid** pVal /* out */)
{
    DryadMTagGuid* tag = LookUpGuidTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetGuid();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpTimeStamp(UInt16 enumID,
                                       DrTimeStamp* pVal /* out */)
{
    DryadMTagTimeStamp* tag = LookUpTimeStampTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetTimeStamp();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpTimeInterval(UInt16 enumID,
                                          DrTimeInterval* pVal /* out */)
{
    DryadMTagTimeInterval* tag = LookUpTimeIntervalTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetTimeInterval();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpDrError(UInt16 enumID, DrError* pVal /* out */)
{
    DryadMTagDrError* tag = LookUpDrErrorTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetDrError();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpMetaData(UInt16 enumID,
                                      DryadMetaDataRef* pVal /* out */)
{
    DryadMTagMetaData* tag = LookUpMetaDataTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetMetaData();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpVertexCommand(UInt16 enumID,
                                           DVertexCommand* pVal /* out */)
{
    DryadMTagVertexCommand* tag = LookUpVertexCommandTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetVertexCommand();
        return DrError_OK;
    }
}

DrError DryadMetaData::
    LookUpInputChannelDescription(UInt16 enumID,
                                  DryadInputChannelDescription** pVal)
{
    DryadMTagInputChannelDescription* tag =
        LookUpInputChannelDescriptionTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetInputChannelDescription();
        return DrError_OK;
    }
}

DrError DryadMetaData::
    LookUpOutputChannelDescription(UInt16 enumID,
                                   DryadOutputChannelDescription** pVal)
{
    DryadMTagOutputChannelDescription* tag =
        LookUpOutputChannelDescriptionTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetOutputChannelDescription();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpVertexProcessStatus(UInt16 enumID,
                                                 DVertexProcessStatus** pVal)
{
    DryadMTagVertexProcessStatus* tag = LookUpVertexProcessStatusTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetVertexProcessStatus();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpVertexStatus(UInt16 enumID, DVertexStatus** pVal)
{
    DryadMTagVertexStatus* tag = LookUpVertexStatusTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetVertexStatus();
        return DrError_OK;
    }
}

DrError DryadMetaData::LookUpVertexCommandBlock(UInt16 enumID,
                                                DVertexCommandBlock** pVal)
{
    DryadMTagVertexCommandBlock* tag = LookUpVertexCommandBlockTag(enumID);
    if (tag == NULL)
    {
        return DrError_InvalidProperty;
    }
    else
    {
        *pVal = tag->GetVertexCommandBlock();
        return DrError_OK;
    }
}

bool DryadMetaData::AppendVoid(UInt16 enumID,
                               bool allowDuplicateTags)
{
    DryadMTagVoidRef tag;
    tag.Attach(DryadMTagVoid::Create(enumID));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendBoolean(UInt16 enumID, bool value,
                                  bool allowDuplicateTags)
{
    DryadMTagBooleanRef tag;
    tag.Attach(DryadMTagBoolean::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendInt16(UInt16 enumID, Int16 value,
                                bool allowDuplicateTags)
{
    DryadMTagInt16Ref tag;
    tag.Attach(DryadMTagInt16::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendUInt16(UInt16 enumID, UInt16 value,
                                 bool allowDuplicateTags)
{
    DryadMTagUInt16Ref tag;
    tag.Attach(DryadMTagUInt16::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendInt32(UInt16 enumID, Int32 value,
                                bool allowDuplicateTags)
{
    DryadMTagInt32Ref tag;
    tag.Attach(DryadMTagInt32::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendUInt32(UInt16 enumID, UInt32 value,
                                 bool allowDuplicateTags)
{
    DryadMTagUInt32Ref tag;
    tag.Attach(DryadMTagUInt32::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendInt64(UInt16 enumID, Int64 value,
                                bool allowDuplicateTags)
{
    DryadMTagInt64Ref tag;
    tag.Attach(DryadMTagInt64::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendUInt64(UInt16 enumID, UInt64 value,
                                 bool allowDuplicateTags)
{
    DryadMTagUInt64Ref tag;
    tag.Attach(DryadMTagUInt64::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendString(UInt16 enumID, const char* value,
                                 bool allowDuplicateTags)
{
    DryadMTagStringRef tag;
    tag.Attach(DryadMTagString::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendGuid(UInt16 enumID, const DrGuid* value,
                               bool allowDuplicateTags)
{
    DryadMTagGuidRef tag;
    tag.Attach(DryadMTagGuid::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendTimeStamp(UInt16 enumID, DrTimeStamp value,
                                    bool allowDuplicateTags)
{
    DryadMTagTimeStampRef tag;
    tag.Attach(DryadMTagTimeStamp::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendTimeInterval(UInt16 enumID, DrTimeInterval value,
                                       bool allowDuplicateTags)
{
    DryadMTagTimeIntervalRef tag;
    tag.Attach(DryadMTagTimeInterval::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendDrError(UInt16 enumID, DrError value,
                                  bool allowDuplicateTags)
{
    DryadMTagDrErrorRef tag;
    tag.Attach(DryadMTagDrError::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendMetaData(UInt16 enumID, DryadMetaData* value,
                                   bool marshalAsAggregate,
                                   bool allowDuplicateTags)
{
    DryadMTagMetaDataRef tag;
    tag.Attach(DryadMTagMetaData::Create(enumID, value, marshalAsAggregate));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendVertexCommand(UInt16 enumID, DVertexCommand value,
                                        bool allowDuplicateTags)
{
    DryadMTagVertexCommandRef tag;
    tag.Attach(DryadMTagVertexCommand::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::
    AppendInputChannelDescription(UInt16 enumID,
                                  DryadInputChannelDescription* value,
                                  bool allowDuplicateTags)
{
    DryadMTagInputChannelDescriptionRef tag;
    tag.Attach(DryadMTagInputChannelDescription::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::
    AppendOutputChannelDescription(UInt16 enumID,
                                   DryadOutputChannelDescription* value,
                                   bool allowDuplicateTags)
{
    DryadMTagOutputChannelDescriptionRef tag;
    tag.Attach(DryadMTagOutputChannelDescription::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendVertexProcessStatus(UInt16 enumID,
                                              DVertexProcessStatus* value,
                                              bool allowDuplicateTags)
{
    DryadMTagVertexProcessStatusRef tag;
    tag.Attach(DryadMTagVertexProcessStatus::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendVertexStatus(UInt16 enumID,
                                       DVertexStatus* value,
                                       bool allowDuplicateTags)
{
    DryadMTagVertexStatusRef tag;
    tag.Attach(DryadMTagVertexStatus::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}

bool DryadMetaData::AppendVertexCommandBlock(UInt16 enumID,
                                             DVertexCommandBlock* value,
                                             bool allowDuplicateTags)
{
    DryadMTagVertexCommandBlockRef tag;
    tag.Attach(DryadMTagVertexCommandBlock::Create(enumID, value));
    return Append(tag, allowDuplicateTags);
}


DryadMetaDataParser::DryadMetaDataParser()
{
    DryadMetaData::Create(&m_set);
}

DryadMetaDataParser::~DryadMetaDataParser()
{
}

DryadMetaData* DryadMetaDataParser::GetMetaData()
{
    return m_set.Ptr();
}

void DryadMetaDataParser::AddTag(DryadMTag* tag)
{
    m_set.Ptr()->Append(tag, true);
}

DrError DryadMetaDataParser::OnParseProperty(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             void* cookie)
{
    TagTypeRecordMap::iterator iter = s_propertyTypeTable->find(enumId);
    TagFactory* factory;
    if (iter == s_propertyTypeTable->end())
    {
        factory = DrMetaDataTagFactory_Unknown;
    }
    else
    {
        factory = iter->second->m_factory;
    }
    return (*factory)(reader, enumId, dataLen, this);
}

DrError DryadMetaDataParser::ParseBuffer(const void* data, UInt32 dataLength)
{
    DrSingleBlockReader r((void *) data, dataLength);

    DrError err = DrError_OK;
    bool finished = false;

    while (!finished) {
        UInt16 enumId;
        UInt32 length;

        err = r.PeekNextPropertyTag(&enumId, &length);
        if (err == DrError_EndOfStream) {
            err = DrError_OK;
            finished = true;
        } else {
            err = OnParseProperty(&r, enumId, length, NULL);
            if (err != DrError_OK)
            {
                finished = true;
            }
        }
    }

    return err;
}


//
// Attempt to insert tagfactory into tagfactory map
// If unable to insert, fail because of duplicate key
//
DrError DryadAddFactoryToTypeTable(UInt16 typeCode,
                                   DryadMetaDataParser::TagFactory* factory)
{
    std::pair<TagFactoryMap::iterator, bool> retval =
        s_factoryTable->insert(std::make_pair(typeCode, factory));
    if (retval.second == false)
    {
        return HRESULT_FROM_WIN32( ERROR_ALREADY_ASSIGNED );
    }
    else
    {
        return DrError_OK;
    }
}

//
// Attempt to add factory to aggregate factory table
// If unable to insert, fail because of duplicate key
//
DrError DryadAddFactoryToAggregateTable(UInt16 typeCode,
                                        DryadMetaDataParser::
                                        AggregateFactory* factory)
{
    std::pair<AggregateFactoryMap::iterator, bool> retval =
        s_aggregateFactoryTable->insert(std::make_pair(typeCode, factory));
    if (retval.second == false)
    {
        return HRESULT_FROM_WIN32( ERROR_ALREADY_ASSIGNED );
    }
    else
    {
        return DrError_OK;
    }
}

//
// Get factory with specified typecode, create a new metadata record
// with reference to that factory and provided id, and then insert
// the metadata record into the record map
//
DrError DryadAddPropertyToMetaData(UInt16 enumId, UInt16 typeCode)
{
    //
    // Get factory associated with typeCode
    //
    TagFactoryMap::iterator iter = s_factoryTable->find(typeCode);
    if (iter == s_factoryTable->end())
    {
        return DrError_InvalidProperty;
    }

    //
    // Build Metadata record 
    //
    DryadMetaDataTypeRecord* r = new DryadMetaDataTypeRecord;
    r->m_enumId = enumId;
    r->m_typeCode = typeCode;
    r->m_factory = iter->second;

    //
    // Attempt to insert record into property type table
    // If unable to add, report duplicate
    //
    std::pair<TagTypeRecordMap::iterator, bool> retval =
        s_propertyTypeTable->insert(std::make_pair(enumId, r));
    if (retval.second == false)
    {
        return HRESULT_FROM_WIN32( ERROR_ALREADY_ASSIGNED );
    }
    else
    {
        return DrError_OK;
    }
}

//
// Build maps with all factories, aggregate factories, and property types
//
void DryadInitMetaDataTable()
{
    //
    // Create maps for factories, aggregate factories, and property types
    //
    s_factoryTable = new TagFactoryMap;
    s_aggregateFactoryTable = new AggregateFactoryMap;
    s_propertyTypeTable = new TagTypeRecordMap;

    //
    // Foreach cosmos factory, add it to factory table
    // todo: Is this cosmos specific? If so, remove it.
    //
    UInt32 n = sizeof(s_cosmosTypeFactory) / sizeof(s_cosmosTypeFactory[0]);
    UInt32 i;
    for (i=0; i<n; ++i)
    {
        DrError err =
            DryadAddFactoryToTypeTable(s_cosmosTypeFactory[i].m_typeCode,
                                       s_cosmosTypeFactory[i].m_factory);
        LogAssert(err == DrError_OK);
    }

    //
    // Foreach dryad factory, add it to factory table
    //
    n = sizeof(s_dryadTypeFactory) / sizeof(s_dryadTypeFactory[0]);
    for (i=0; i<n; ++i)
    {
        DrError err =
            DryadAddFactoryToTypeTable(s_dryadTypeFactory[i].m_typeCode,
                                       s_dryadTypeFactory[i].m_factory);
        LogAssert(err == DrError_OK);
    }

    //
    // Foreach aggregate factory, add it to the aggregate factory table
    //
    n = sizeof(s_dryadAggregateFactory) / sizeof(s_dryadAggregateFactory[0]);
    for (i=0; i<n; ++i)
    {
        DrError err =
            DryadAddFactoryToAggregateTable(s_dryadAggregateFactory[i].
                                            m_aggregateId,
                                            s_dryadAggregateFactory[i].
                                            m_factory);
        LogAssert(err == DrError_OK);
    }

    //
    // Add all the property types in cosmos to the property type map
    // todo: cosmos specific. Remove.
    //
    n = sizeof(s_cosmosTagTypes) / sizeof(s_cosmosTagTypes[0]);
    for (i=0; i<n; ++i)
    {
        DrError err =
            DryadAddPropertyToMetaData(s_cosmosTagTypes[i].m_enumId,
                                       s_cosmosTagTypes[i].m_typeCode);
        LogAssert(err == DrError_OK);
    }

    //
    // Add all the property types in dryad to the property type map
    //
    n = sizeof(s_dryadTagTypes) / sizeof(s_dryadTagTypes[0]);
    for (i=0; i<n; ++i)
    {
        DrError err =
            DryadAddPropertyToMetaData(s_dryadTagTypes[i].m_enumId,
                                       s_dryadTagTypes[i].m_typeCode);
        LogAssert(err == DrError_OK);
    }
}


DrError DrMetaDataTagFactory_Unknown(DrMemoryReader* reader,
                                     UInt16 enumId,
                                     UInt32 dataLen,
                                     DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Void(DrMemoryReader* reader,
                                  UInt16 enumId,
                                  UInt32 dataLen,
                                  DryadMetaDataParser* parent)
{
    DryadMTagVoidRef tag;
    DrError err =
        DryadMTagVoid::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Boolean(DrMemoryReader* reader,
                                     UInt16 enumId,
                                     UInt32 dataLen,
                                     DryadMetaDataParser* parent)
{
    DryadMTagBooleanRef tag;
    DrError err =
        DryadMTagBoolean::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Int16(DrMemoryReader* reader,
                                   UInt16 enumId,
                                   UInt32 dataLen,
                                   DryadMetaDataParser* parent)
{
    DryadMTagInt16Ref tag;
    DrError err =
        DryadMTagInt16::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Int32(DrMemoryReader* reader,
                                   UInt16 enumId,
                                   UInt32 dataLen,
                                   DryadMetaDataParser* parent)
{
    DryadMTagInt32Ref tag;
    DrError err =
        DryadMTagInt32::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Int64(DrMemoryReader* reader,
                                   UInt16 enumId,
                                   UInt32 dataLen,
                                   DryadMetaDataParser* parent)
{
    DryadMTagInt64Ref tag;
    DrError err =
        DryadMTagInt64::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Double(DrMemoryReader* reader,
                                   UInt16 enumId,
                                   UInt32 dataLen,
                                   DryadMetaDataParser* parent)
{
    DryadMTagDoubleRef tag;
    DrError err =
        DryadMTagDouble::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_UInt16(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagUInt16Ref tag;
    DrError err =
        DryadMTagUInt16::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_TagIdValue(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagUInt16Ref tag;
    DrError err =
        DryadMTagUInt16::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}


DrError DrMetaDataTagFactory_UInt32(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagUInt32Ref tag;
    DrError err =
        DryadMTagUInt32::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_UInt64(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagUInt64Ref tag;
    DrError err =
        DryadMTagUInt64::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_HexUInt16(DrMemoryReader* reader,
                                       UInt16 enumId,
                                       UInt32 dataLen,
                                       DryadMetaDataParser* parent)
{
    DryadMTagUInt16Ref tag;
    DrError err =
        DryadMTagUInt16::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_HexUInt32(DrMemoryReader* reader,
                                       UInt16 enumId,
                                       UInt32 dataLen,
                                       DryadMetaDataParser* parent)
{
    DryadMTagUInt32Ref tag;
    DrError err =
        DryadMTagUInt32::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_HexUInt64(DrMemoryReader* reader,
                                       UInt16 enumId,
                                       UInt32 dataLen,
                                       DryadMetaDataParser* parent)
{
    DryadMTagUInt64Ref tag;
    DrError err =
        DryadMTagUInt64::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_String(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagStringRef tag;
    DrError err =
        DryadMTagString::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Guid(DrMemoryReader* reader,
                                  UInt16 enumId,
                                  UInt32 dataLen,
                                  DryadMetaDataParser* parent)
{
    DryadMTagGuidRef tag;
    DrError err =
        DryadMTagGuid::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_TimeStamp(DrMemoryReader* reader,
                                       UInt16 enumId,
                                       UInt32 dataLen,
                                       DryadMetaDataParser* parent)
{
    DryadMTagTimeStampRef tag;
    DrError err =
        DryadMTagTimeStamp::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_TimeInterval(DrMemoryReader* reader,
                                          UInt16 enumId,
                                          UInt32 dataLen,
                                          DryadMetaDataParser* parent)
{
    DryadMTagTimeIntervalRef tag;
    DrError err =
        DryadMTagTimeInterval::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_DrExitCode(DrMemoryReader* reader,
                                     UInt16 enumId,
                                     UInt32 dataLen,
                                     DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_DrExitCode,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_DrError(DrMemoryReader* reader,
                                     UInt16 enumId,
                                     UInt32 dataLen,
                                     DryadMetaDataParser* parent)
{
    DryadMTagDrErrorRef tag;
    DrError err =
        DryadMTagDrError::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Blob(DrMemoryReader* reader,
                                  UInt16 enumId,
                                  UInt32 dataLen,
                                  DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::ReadFromStreamWithType(reader, enumId, dataLen,
                                                 DrPropertyTagType_Blob,
                                                 &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_Payload(DrMemoryReader* reader,
                                  UInt16 enumId,
                                  UInt32 dataLen,
                                  DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::ReadFromStreamWithType(reader, enumId, dataLen,
                                                 DrPropertyTagType_Payload,
                                                 &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_EnvironmentBlock(DrMemoryReader* reader,
                                  UInt16 enumId,
                                  UInt32 dataLen,
                                  DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::ReadFromStreamWithType(reader, enumId, dataLen,
                                                 DrPropertyTagType_EnvironmentBlock,
                                                 &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_AppendExtentOptions(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_AppendExtentOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_AppendBlockOptions(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_AppendBlockOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_FailureInjectionOptions(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_FailureInjectionOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_SyncDirectiveOptions(DrMemoryReader* reader,
                                              UInt16 enumId,
                                              UInt32 dataLen,
                                              DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_SyncDirectiveOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_SyncOptions(DrMemoryReader* reader,
                                     UInt16 enumId,
                                     UInt32 dataLen,
                                     DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_SyncOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_ReadExtentOptions(DrMemoryReader* reader,
                                           UInt16 enumId,
                                           UInt32 dataLen,
                                           DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_ReadExtentOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_AppendStreamOptions(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_AppendStreamOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_StreamCapabilityBits(DrMemoryReader* reader,
                                             UInt16 enumId,
                                             UInt32 dataLen,
                                             DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_StreamCapabilityBits,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_EnumDirectoryOptions(DrMemoryReader* reader,
                                              UInt16 enumId,
                                              UInt32 dataLen,
                                              DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_EnumDirectoryOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_EnInfoBits(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_EnInfoBits,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_UpdateExtentMetadataOptions(DrMemoryReader*
                                                     reader,
                                                     UInt16 enumId,
                                                     UInt32 dataLen,
                                                     DryadMetaDataParser*
                                                     parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_UpdateExtentMetadataOptions,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_StreamInfoBits(DrMemoryReader* reader,
                                        UInt16 enumId,
                                        UInt32 dataLen,
                                        DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_StreamInfoBits,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_ExtentInfoBits(DrMemoryReader* reader,
                                        UInt16 enumId,
                                        UInt32 dataLen,
                                        DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_ExtentInfoBits,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DrMetaDataTagFactory_ExtentInstanceInfoBits(DrMemoryReader* reader,
                                                UInt16 enumId,
                                                UInt32 dataLen,
                                                DryadMetaDataParser* parent)
{
    DryadMTagUnknownRef tag;
    DrError err =
        DryadMTagUnknown::
        ReadFromStreamWithType(reader, enumId, dataLen,
                               DrPropertyTagType_ExtentInstanceInfoBits,
                               &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError DrMetaDataTagFactory_BeginTag(DrMemoryReader* reader,
                                      UInt16 enumId,
                                      UInt32 dataLen,
                                      DryadMetaDataParser* parent)
{
    if (enumId != Prop_Dryad_BeginTag)
    {
        return DrError_InvalidProperty;
    }

    UInt16 tagValue;
    DrError err
        = reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagValue);
    if (err == DrError_OK)
    {
        AggregateFactoryMap::iterator iter =
            s_aggregateFactoryTable->find(tagValue);
        if (iter == s_aggregateFactoryTable->end())
        {
            err = DrError_InvalidProperty;
        }
        else
        {
            DryadMetaDataParser::AggregateFactory* factory = iter->second;
            DryadMTagRef tag;
            err = (*factory)(reader, &tag);
            {
                if (err == DrError_OK)
                {
                    parent->AddTag(tag);
                }
                else
                {
                    LogAssert(tag == NULL);
                }
            }
        }
    }

    return err;
}

DrError DrMetaDataTagFactory_EndTag(DrMemoryReader* reader,
                                    UInt16 enumId,
                                    UInt32 dataLen,
                                    DryadMetaDataParser* parent)
{
    /* this should always be consumed as a side-effect of the
       BeginTag */
    return DrError_InvalidProperty;
}

DrError DrMetaDataTagFactory_PropertyList(DrMemoryReader* reader,
                                          UInt16 enumId,
                                          UInt32 dataLen,
                                          DryadMetaDataParser* parent)
{
    const void* data;
    DrError err = reader->ReadNextProperty(&enumId, &dataLen, &data);
    if (err == DrError_OK)
    {
        DryadMTagMetaDataRef tag;
        err = DryadMTagMetaData::ReadFromArray(enumId, data, dataLen, &tag);
        if (err == DrError_OK)
        {
            parent->AddTag(tag);
        }
        else
        {
            LogAssert(tag == NULL);
        }
    }
    return err;
}

DrError
    DryadMetaDataTagFactory_Void(DrMemoryReader* reader,
                                 UInt16 enumId,
                                 UInt32 dataLen,
                                 DryadMetaDataParser* parent)
{
    DryadMTagVoidRef tag;
    DrError err =
        DryadMTagVoid::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DryadMetaDataTagFactory_VertexCommand(DrMemoryReader* reader,
                                          UInt16 enumId,
                                          UInt32 dataLen,
                                          DryadMetaDataParser* parent)
{
    DryadMTagVertexCommandRef tag;
    DrError err =
        DryadMTagVertexCommand::ReadFromStream(reader, enumId, dataLen, &tag);
    if (err == DrError_OK)
    {
        parent->AddTag(tag);
    }
    else
    {
        LogAssert(tag == NULL);
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_MetaData(DrMemoryReader* reader,
                                           DryadMTagRef* pTag)
{
    DryadMTagMetaDataRef tag;
    DrError err = DryadMTagMetaData::ReadFromStreamInAggregate(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_InputChannelDescription(DrMemoryReader* reader,
                                                          DryadMTagRef* pTag)
{
    DryadMTagInputChannelDescriptionRef tag;
    DrError err =
        DryadMTagInputChannelDescription::ReadFromStream(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_OutputChannelDescription(DrMemoryReader* reader,
                                                           DryadMTagRef* pTag)
{
    DryadMTagOutputChannelDescriptionRef tag;
    DrError err =
        DryadMTagOutputChannelDescription::ReadFromStream(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_VertexProcessStatus(DrMemoryReader* reader,
                                                      DryadMTagRef* pTag)
{
    DryadMTagVertexProcessStatusRef tag;
    DrError err = DryadMTagVertexProcessStatus::ReadFromStream(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_VertexStatus(DrMemoryReader* reader,
                                               DryadMTagRef* pTag)
{
    DryadMTagVertexStatusRef tag;
    DrError err = DryadMTagVertexStatus::ReadFromStream(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}

DrError
    DryadMetaDataAggregateFactory_VertexCommandBlock(DrMemoryReader* reader,
                                               DryadMTagRef* pTag)
{
    DryadMTagVertexCommandBlockRef tag;
    DrError err = DryadMTagVertexCommandBlock::ReadFromStream(reader, &tag);
    if (err == DrError_OK)
    {
        *pTag = tag;
    }
    return err;
}
