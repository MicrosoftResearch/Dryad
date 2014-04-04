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
#pragma warning(disable:4512)
#pragma warning(disable:4511)
#pragma warning(disable:4995)

#include <DrCommon.h>
#include "dryadmetadatatag.h"
#include "dryadmetadatatagtypes.h"
#include <map>
#include <list>

class DryadMetaDataConst;

class DryadMetaData;
typedef DrRef<DryadMetaData> DryadMetaDataRef;

class DryadMetaData : public DrRefCounter
{
public:
    typedef std::list<DryadMTag *> TagList;
    typedef TagList::iterator TagListIter;
    typedef std::multimap< UInt16,DryadMTag * > TagMap;
    typedef TagMap::iterator TagMapIter;

    /* the following method places a reference to a new, empty,
       DryadMetaData object in the caller's dstMetaData object. */
    static void Create(DryadMetaDataRef* dstMetaData);

    /* this appends tag to the end of self's tag list and transfers
       the caller's reference to tag. If allowDuplicateTags is false
       and there is a tag of the same name in self, tag is not
       appended to self and its reference count is
       decremented. Returns true if and only if tag was appended to
       self. */
    bool Append(DryadMTag* tag, bool allowDuplicateTags);

    /* this appends all the tags in metaData to the end of self's tag
       list without modifying metaData (incrementing each tag's
       refcount). If allowDuplicateTags is false then any tags in
       metaData which already exist in self are not appended. */
    void AppendMetaDataTags(DryadMetaData* metaData, bool allowDuplicateTags);

    /* this replaces oldTag with newTag in self's tag list. It is an
       error to call Replace with oldTag and newTag which do not have
       the same tag value. If oldTag is not in self's tag list this
       returns false and does not alter any reference counts,
       otherwise it returns true, transfers the caller's reference to
       newTag into self and discards self's reference to oldTag.
    */
    bool Replace(DryadMTag* newTag, DryadMTag* oldTag);

    /* this removes tag from self's tag list. If tag is not in self's
       tag list this returns false and does not alter any reference
       counts, otherwise it returns true and discards self's reference
       to tag.
    */
    bool Remove(DryadMTag* tag);

    /* this returns the tag with ID enumID if and only if there is
       a unique tag with that name in self's list, otherwise it
       returns NULL. This call does not modify the tag's reference
       count. */
    DryadMTag* LookUpTag(UInt16 enumID);

    /* the following type-safe lookup methods return non-NULL if and
       only if there is a unique tag in self with ID enumID and the
       correct type. These calls do not modify the tag's reference
       count. */
    DryadMTagUnknown* LookUpUnknownTag(UInt16 enumID);
    DryadMTagVoid* LookUpVoidTag(UInt16 enumID);
    DryadMTagBoolean* LookUpBooleanTag(UInt16 enumID);
    DryadMTagInt16* LookUpInt16Tag(UInt16 enumID);
    DryadMTagUInt16* LookUpUInt16Tag(UInt16 enumID);
    DryadMTagInt32* LookUpInt32Tag(UInt16 enumID);
    DryadMTagUInt32* LookUpUInt32Tag(UInt16 enumID);
    DryadMTagInt64* LookUpInt64Tag(UInt16 enumID);
    DryadMTagUInt64* LookUpUInt64Tag(UInt16 enumID);
    DryadMTagString* LookUpStringTag(UInt16 enumID);
    DryadMTagGuid* LookUpGuidTag(UInt16 enumID);
    DryadMTagTimeStamp* LookUpTimeStampTag(UInt16 enumID);
    DryadMTagTimeInterval* LookUpTimeIntervalTag(UInt16 enumID);
    DryadMTagDrError* LookUpDrErrorTag(UInt16 enumID);
    DryadMTagMetaData* LookUpMetaDataTag(UInt16 enumID);
    DryadMTagVertexCommand* LookUpVertexCommandTag(UInt16 enumID);
    DryadMTagInputChannelDescription*
        LookUpInputChannelDescriptionTag(UInt16 enumID);
    DryadMTagOutputChannelDescription*
        LookUpOutputChannelDescriptionTag(UInt16 enumID);
    DryadMTagVertexProcessStatus* LookUpVertexProcessStatusTag(UInt16 enumID);
    DryadMTagVertexStatus* LookUpVertexStatusTag(UInt16 enumID);
    DryadMTagVertexCommandBlock* LookUpVertexCommandBlockTag(UInt16 enumID);

    /* the following type-safe lookup methods return DrError_OK if and
       only if there is a unique tag in self with ID enumID and the
       correct type. */
    DrError LookUpVoid(UInt16 enumID);
    DrError LookUpBoolean(UInt16 enumID, bool* pVal /* out */);
    DrError LookUpInt16(UInt16 enumID, Int16* pVal /* out */);
    DrError LookUpUInt16(UInt16 enumID, UInt16* pVal /* out */);
    DrError LookUpInt32(UInt16 enumID, Int32* pVal /* out */);
    DrError LookUpUInt32(UInt16 enumID, UInt32* pVal /* out */);
    DrError LookUpInt64(UInt16 enumID, Int64* pVal /* out */);
    DrError LookUpUInt64(UInt16 enumID, UInt64* pVal /* out */);
    DrError LookUpString(UInt16 enumID, const char** pVal /* out */);
    DrError LookUpGuid(UInt16 enumID, const DrGuid** pVal /* out */);
    DrError LookUpTimeStamp(UInt16 enumID, DrTimeStamp* pVal /* out */);
    DrError LookUpTimeInterval(UInt16 enumID, DrTimeInterval* pVal /* out */);
    DrError LookUpDrError(UInt16 enumID, DrError* pVal /* out */);
    DrError LookUpMetaData(UInt16 enumID, DryadMetaDataRef* pVal /* out */);
    DrError LookUpVertexCommand(UInt16 enumID, DVertexCommand* pVal /* out */);
    DrError LookUpInputChannelDescription(UInt16 enumID,
                                          DryadInputChannelDescription**
                                          pVal /* out */);
    DrError LookUpOutputChannelDescription(UInt16 enumID,
                                           DryadOutputChannelDescription**
                                           pVal /* out */);
    DrError LookUpVertexProcessStatus(UInt16 enumID,
                                      DVertexProcessStatus** pVal /* out */);
    DrError LookUpVertexStatus(UInt16 enumID, DVertexStatus** pVal /* out */);
    DrError LookUpVertexCommandBlock(UInt16 enumID,
                                     DVertexCommandBlock** pVal /* out */);

    /* the following type-safe append methods return true if and only
       if the tag was appended. */
    bool AppendVoid(UInt16 enumID,
                    bool allowDuplicateTags);
    bool AppendBoolean(UInt16 enumID, bool value,
                       bool allowDuplicateTags);
    bool AppendInt16(UInt16 enumID, Int16 value,
                     bool allowDuplicateTags);
    bool AppendUInt16(UInt16 enumID, UInt16 value,
                      bool allowDuplicateTags);
    bool AppendInt32(UInt16 enumID, Int32 value,
                     bool allowDuplicateTags);
    bool AppendUInt32(UInt16 enumID, UInt32 value,
                      bool allowDuplicateTags);
    bool AppendInt64(UInt16 enumID, Int64 value,
                     bool allowDuplicateTags);
    bool AppendUInt64(UInt16 enumID, UInt64 value,
                      bool allowDuplicateTags);
    bool AppendString(UInt16 enumID, const char* value,
                      bool allowDuplicateTags);
    bool AppendGuid(UInt16 enumID, const DrGuid* value,
                    bool allowDuplicateTags);
    bool AppendTimeStamp(UInt16 enumID, DrTimeStamp value,
                         bool allowDuplicateTags);
    bool AppendTimeInterval(UInt16 enumID, DrTimeInterval value,
                            bool allowDuplicateTags);
    bool AppendDrError(UInt16 enumID, DrError value,
                       bool allowDuplicateTags);
    bool AppendMetaData(UInt16 enumID, DryadMetaData* value,
                        bool marshalAsAggregate,
                        bool allowDuplicateTags);
    bool AppendVertexCommand(UInt16 enumID, DVertexCommand value,
                             bool allowDuplicateTags);
    bool AppendInputChannelDescription(UInt16 enumID,
                                       DryadInputChannelDescription* value,
                                       bool allowDuplicateTags);
    bool AppendOutputChannelDescription(UInt16 enumID,
                                        DryadOutputChannelDescription* value,
                                        bool allowDuplicateTags);
    bool AppendVertexProcessStatus(UInt16 enumID, DVertexProcessStatus* value,
                                   bool allowDuplicateTags);
    bool AppendVertexStatus(UInt16 enumID, DVertexStatus* value,
                            bool allowDuplicateTags);
    bool AppendVertexCommandBlock(UInt16 enumID, DVertexCommandBlock* value,
                                  bool allowDuplicateTags);

    /* This returns an iterator which can be used to access all the
       tags with ID enumID (if any) within self. *pEndIter is
       filled in with an iterator beyond the last tag with ID
       enumID. No reference counts are modified by this call. The
       order of tags returned by this iterator is undefined. */
    DryadMetaData::TagMapIter
        LookUpMulti(UInt16 enumID,
                    DryadMetaData::TagMapIter* pEndIter);

    /* This returns an iterator which can be used to access all the
       tags within self in sequence starting from startTag. *pEndIter
       is filled in with an iterator beyond the last tag in self. If
       startTag is NULL the iterator begins at the first tag (if any)
       in self. No reference counts are modified by this call. */
    DryadMetaData::TagListIter
        LookUpInSequence(DryadMTag* startTag,
                         DryadMetaData::TagListIter* pEndIter);

    /* this returns a recursive clone of self. Leaf tags acquire a new
       reference within the cloned object, rather than being copied. The
       returned metadata has a single reference owned by the
       caller. */
    void Clone(DryadMetaDataRef* dstMetaData);

    void Serialize(DrMemoryWriter* writer);
    void CacheSerialization();
    DrMemoryBuffer* SerializeToBuffer();

    /* call 'delete []' on the buffer returned by GetText() */
    char* GetText();
    DrError WriteAsProperty(DrMemoryWriter* writer,
                            UInt16 propertyTag,
                            bool writeIfEmpty);
    DrError WriteAsAggregate(DrMemoryWriter* writer,
                             UInt16 propertyTag,
                             bool writeIfEmpty);

    /* these are convenience methods to add error codes. They each add
       a DrError tag with ID Prop_Dryad_ErrorCode, value errorCode if
       it doesn't already exist. The second also adds a
       Prop_Dryad_ErrorString containing errorDescription. */
    void AddError(DrError errorCode);
    void AddErrorWithDescription(DrError errorCode,
                                 const char* errorDescription);

    /* this is a convenience method. If the metadata contains a
       DrError tag with ID Prop_Dryad_ErrorCode then the call returns
       true and the error in that property is filled in to *pError,
       otherwise it returns false and *pError is not modified. */
    bool GetErrorCode(DrError* pError /* out */);
    /* returns NULL if no error string exists. The returned value is
       valid only as long as this instance is not changed */
    const char* GetErrorString();

private:
    DryadMetaData();
    ~DryadMetaData();

    TagList                     m_elementList;
    TagMap                      m_elementMap;
    DrRef<DrMemoryBuffer> m_cachedSerialization;

    CRITSEC                     m_baseCS;
};

class DryadMetaDataParser : public DrPropertyParser
{
public:
    typedef DrError (TagFactory)(DrMemoryReader* reader,
                                 UInt16 enumId, UInt32 dataLen,
                                 DryadMetaDataParser* parent);
    typedef DrError (AggregateFactory)(DrMemoryReader* reader,
                                       DryadMTagRef* pTag);

    DryadMetaDataParser();
    ~DryadMetaDataParser();

    DryadMetaData* GetMetaData();
    void AddTag(DryadMTag* tag);

    DrError ParseBuffer(const void* data, UInt32 dataLength);

    DrError DryadMetaDataParser::OnParseProperty(DrMemoryReader* reader,
                                                 UInt16 enumId,
                                                 UInt32 dataLen,
                                                 void* cookie);

private:
    DryadMetaDataRef                 m_set;
};

extern void DryadInitMetaDataTable();
extern DrError
    DryadAddFactoryToTypeTable(UInt16 typeCode,
                               DryadMetaDataParser::TagFactory* factory);
extern DrError
    DryadAddFactoryToAggregateTable(UInt16 typeCode,
                                    DryadMetaDataParser::AggregateFactory*
                                    factory);
extern DrError DryadAddPropertyToMetaData(UInt16 prop, UInt16 typeCode);
