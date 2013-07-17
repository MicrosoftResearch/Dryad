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

#include "DrList.h"

#pragma warning(push)
#pragma warning(disable:4995)

// A structure that defines a mapping between a particular bit range within a UInt32 and a readable name
typedef struct {
    const char *fieldName;              // The name of the bit field
    UInt32 bitMask;                         // The bits that relate to this mask
    const char **prgValueNames;   // An array of enum names for the field (of length 2**n-1, where n is the number of bits including
                                                     // the least significant and most significant 1 bit in bitMask. If an entry is NULL,
                                                     // the bitmask value will not be displayed. If prgValueNames is NULL, all values will
                                                     // be displayed as a UInt.
} DrBitFieldMap;

// A structure that defines a mapping between a UInt32 and a set of bit fields
typedef struct {
    int numFields;                          // The number of fields
    const DrBitFieldMap *prgFields; // The array of bit field mappiings
} DrBitMaskMap;

/* JC
class DrPropertyDumper
{
private:
    typedef enum {
        // We are at the beginning of a property id
        DrPropertyDumper_StartOfTag = 0,

        // we have read the first byte of the property ID and are waiting for the second byte
        DrPropertyDumper_ReadingPropertyId,

        // We have read the property ID, but are waiting on 1-4 remaining length bytes (1 byte for a shortatom, 4 bytes for a longatom)
        DrPropertyDumper_ReadingLength,

        // We have read the property ID and the length, , but are waiting on the remaining data
        DrPropertyDumper_ReadingData
    } DrPropertyDumperState;

public:

public:
    // The default maximum size that will be dumped for blob properties, unknown properties, and partial/malformed properties. Blob properties
    // that exceed this length will be truncated in the dump with an "...(n bytes remaining)" annotation.
    static const size_t k_nbDefaultMaxBlobSize = 512;

    // The default maximum size that will be dumped for 
    static const size_t k_nbDefaultMaxPayloadSize = 64;
    static const int k_nDefaultIndentSpacesPerLevel = 2;

private:
    void Construct(size_t maxBlobSize, bool fIncludeComments, int nIndentSpacesPerLevel)
    {
        m_state = DrPropertyDumper_StartOfTag;
        m_partialPropertyId = 0;
        m_partialPropertyTotalLength = 0;
        m_pReader = NULL;
        m_pWriter = NULL;
        m_fDeleteReader = false;
        m_fDeleteWriter = false;
        m_indent = 0;
        m_fAtBol = true;
        m_fOutputCrlf = true;
        m_nbMaxBlobSize = maxBlobSize;
        m_nbMaxPayloadSize = k_nbDefaultMaxPayloadSize;
        if (m_nbMaxPayloadSize > m_nbMaxBlobSize) {
            m_nbMaxPayloadSize = m_nbMaxBlobSize;
        }
        m_nIndentSpacesPerLevel = nIndentSpacesPerLevel;
        m_fIncludeComments = fIncludeComments;
    }


public:
    DrPropertyDumper()
    {
        Construct(k_nbDefaultMaxBlobSize, true, k_nDefaultIndentSpacesPerLevel);
    }

    DrPropertyDumper(bool fFullContent, bool fIncludeComments = true, int nIndentSpacesPerLevel = k_nDefaultIndentSpacesPerLevel)
    {
        Construct(fFullContent ? MAX_SIZE_T : k_nbDefaultMaxBlobSize, fIncludeComments, nIndentSpacesPerLevel);
    }

    DrPropertyDumper(size_t maxBlobSize, bool fIncludeComments = true, int nIndentSpacesPerLevel = k_nDefaultIndentSpacesPerLevel)
    {
        Construct(maxBlobSize, fIncludeComments, nIndentSpacesPerLevel);
    }

    __declspec(deprecated) void SetReader(DrMemoryReader *pReader, bool fDelete = false)
    {
        if (m_pReader != NULL && m_fDeleteReader) {
            delete m_pReader;
        }
        m_pReader = pReader;
        m_fDeleteReader = fDelete;
    }

    // If true, then the full content of the property set (including large blobs) will be encoded rather than summarized
    // If false, then blobs will be limited to the first k_nbDefaultMaxBlobSize bytes.
    void SetFullContent(bool fFullContent=true)
    {
        m_nbMaxBlobSize = fFullContent ? MAX_SIZE_T : k_nbDefaultMaxBlobSize;
    }

    // If parameter is true, causes bare '\n' to terminate lines, rather than "\r\n"
    void SetSuppressOutputCr(bool fSuppressOutputCr = true)
    {
        m_fOutputCrlf = !fSuppressOutputCr;
    }

    // Will lower, but not raise the max payload size
    void SetMaxBlobSize(size_t nbMaxBlobSize)
    {
        m_nbMaxBlobSize = nbMaxBlobSize;
        if (m_nbMaxPayloadSize > m_nbMaxBlobSize) {
            m_nbMaxPayloadSize = m_nbMaxBlobSize;
        }
    }

    // Will not raise the max payload size
    void SetUnlimitedBlobSize()
    {
        SetMaxBlobSize(MAX_SIZE_T);
    }

    size_t GetMaxBlobSize()
    {
        return m_nbMaxBlobSize;
    }

    void SetIncludeComments(bool fIncludeComments = true)
    {
        m_fIncludeComments = fIncludeComments;
    }

    bool ShouldIncludeComments()
    {
        return m_fIncludeComments;
    }

    // Does not affect the limits for non-payload blobs
    void SetMaxPayloadSize(size_t nbMaxPayloadSize)
    {
        m_nbMaxPayloadSize = nbMaxPayloadSize;
    }

    // Does not affect the limits for non-payload blobs
    void SetUnlimitedPayloadSize()
    {
        SetMaxPayloadSize(MAX_SIZE_T);
    }

    size_t GetMaxPayloadSize()
    {
        return m_nbMaxPayloadSize;
    }

    DrMemoryReader *GetReader()
    {
        return m_pReader;
    }

    void SetWriter(DrMemoryWriter *pWriter, bool fDelete = false)
    {
        if (m_pWriter != NULL && m_fDeleteWriter) {
            delete m_pWriter;
        }
        m_pWriter = pWriter;
        m_fDeleteWriter = fDelete;
    }

    DrMemoryWriter *GetWriter()
    {
        return m_pWriter;
    }

    void SetNumIndentSpacesPerLevel(int n)
    {
        m_nIndentSpacesPerLevel = n;
    }

    int GetNumIndentSpacesPerLevel()
    {
        return m_nIndentSpacesPerLevel;
    }
    
    void SetIndent(int n)
    {
        m_indent = n;
    }

    int GetIndent()
    {
        return m_indent;
    }

    void Indent()
    {
        m_indent += m_nIndentSpacesPerLevel;
    }

    void Unindent()
    {
        m_indent -= m_nIndentSpacesPerLevel;
        if (m_indent < 0) {
            m_indent = 0;
        }
    }

    DrError WriteF(const char *pszFormat, ...);
    DrError VWriteF(const char *pszFormat, va_list args);

    // Writes an XML start tag.  Does NOT affect the DrTag Nesting level
    DrError WriteStartTag(const char *pszTagName, Size_t length, bool fIncludeLength, bool fNewlineAfter)
    {
        DrError err;
        if (fIncludeLength) {
            err = WriteF("<%s length=\"%Iu\"%s", pszTagName, length, (fNewlineAfter ? ">\n" : ">"));
        } else {
            err = WriteF("<%s%s", pszTagName, (fNewlineAfter ? ">\n" : ">"));
            m_fAtBol = fNewlineAfter;
        }
        Indent();
        return err;
    }

    DrError WriteSimpleStartTag(const char *pszTagName, bool fNewlineAfter)
    {
        DrError err;
        err = WriteF("<%s%s", pszTagName, (fNewlineAfter ? ">\n" : ">"));
        m_fAtBol = fNewlineAfter;
        Indent();
        return err;
    }

    DrError FlushFileWriter()
    {
    }

    DrError WriteXmlFileHeader()
    {
        return PutStr("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    }
    
    // Writes an XML start tag.  Does NOT affect the DrTag Nesting level
    // pszAttributes is an optional XML attributes string in the form:
    //               attrib1="value" attrib2="value"
    //
    DrError WriteStartTagWithAttributes(__in const char *pszTagName, __in_opt const char *pszAttributes, bool fNewlineAfter)
    {
        DrError err;
        if (pszAttributes == NULL) {
            pszAttributes = "";
        } else {
            while(pszAttributes[0] == ' ') {
                pszAttributes++;
            }
        }
        err = WriteF("<%s%s%s%s", pszTagName, (pszAttributes[0] == '\0') ? "" : " ", pszAttributes, (fNewlineAfter ? ">\n" : ">"));
        m_fAtBol = fNewlineAfter;
        Indent();
        return err;
    }

    DrError WriteStartTagWithLengthAndAttributes(__in const char *pszTagName, Size_t length, __in_opt const char *pszAttributes, bool fNewlineAfter)
    {
        DrError err;
        if (pszAttributes == NULL) {
            pszAttributes = "";
        } else {
            while(pszAttributes[0] == ' ') {
                pszAttributes++;
            }
        }
        err = WriteF("<%s length=\"%Iu\"%s%s%s", pszTagName, length, (pszAttributes[0] == '\0') ? "" : " ", pszAttributes, (fNewlineAfter ? ">\n" : ">"));
        m_fAtBol = fNewlineAfter;
        Indent();
        return err;
    }

    // Writes an XML end tag.  DOes NOT affect the DrTag Nesting level
    DrError WriteEndTag(const char *pszTagName, bool fNewlineBefore)
    {
        Unindent();
        DrError err = WriteF("%s/%s>\n", (fNewlineBefore ? "\n<" : "<"), pszTagName);
        return err;
    }

    // The string generated by pszFormat must be XML-encoded or contain no XML delimiters
    DrError VWriteSimpleTagValue(
        const char *pszTagName,
        UInt32 length,
        bool fIncludeLength,
        bool fSeperateLine,
        const char *pszFormat,
        va_list args)
    {
        DrError err;

        WriteStartTag(pszTagName, length, fIncludeLength, fSeperateLine);
        VWriteF(pszFormat, args);
        err = WriteEndTag(pszTagName, fSeperateLine);
        return err;
    }

    // The string generated by pszFormat must be XML-encoded or contain no XML delimiters
    DrError WriteSimpleTagValue(
        const char *pszTagName,
        UInt32 length,
        bool fIncludeLength,
        bool fSeperateLine,
        const char *pszFormat,
        ...)
    {
        va_list args;
        va_start(args, pszFormat);
        return VWriteSimpleTagValue(pszTagName, length, fIncludeLength, fSeperateLine, pszFormat, args);
    }

    // The string generated by pszFormat must be XML-encoded or contain no XML delimiters
    DrError WriteSimpleTagValue(
        const char *pszTagName,
        const char *pszFormat,
        ...)
    {
        va_list args;
        va_start(args, pszFormat);
        return VWriteSimpleTagValue(pszTagName, 0, false, false, pszFormat, args);
    }

    // The string generated by pszFormat must be XML-encoded or contain no XML delimiters
    DrError WriteSimpleTagValue(
        const char *pszTagName,
        UInt32 length, 
        const char *pszFormat,
        ...)
    {
        va_list args;
        va_start(args, pszFormat);
        return VWriteSimpleTagValue(pszTagName, length, true, false, pszFormat, args);
    }

    // Puts a string to output, indenting lines as appropriate
    // Must obey XML conventions
    DrError PutStr(const char *psz, int len = -1);

    DrError PutBolLeader()
    {
        if (m_indent > 0) {
            m_fAtBol = false;
            for (int i = 0; i < m_indent; i++) {
                m_pWriter->WriteChar(' ');
            }
        }
        return m_pWriter->GetStatus();
    }

    DrError PutUInt64TagValue(const char *pszTagName, UInt64 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%016I64x" : "%I64u");
        return WriteSimpleTagValue(pszTagName, pszFormat, val);
    }

    DrError PutUInt32TagValue(const char *pszTagName, UInt32 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%08x" : "%u");
        return WriteSimpleTagValue(pszTagName, pszFormat, val);
    }

    DrError PutDoubleTagValue(const char *pszTagName, double val)
    {
        const char *pszFormat = "%lf";
        return WriteSimpleTagValue(pszTagName, pszFormat, val);
    }

    DrError PutUInt32BitMaskValue(const char *pszTagName, UInt32 val, const DrBitMaskMap *pMap);

    DrError PutUInt16TagValue(const char *pszTagName, UInt16 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%04x" : "%u");
        return WriteSimpleTagValue(pszTagName, pszFormat, (UInt32)val);
    }

    DrError PutTagIdTagValue(const char *pszTagName, UInt16 val)
    {
        const char *pszTagIdName = GetTagName(val);
        if (pszTagIdName == NULL) {
            return WriteSimpleTagValue(pszTagName, "0x%04x", (UInt32)val);
        } else {
            return WriteSimpleTagValue(pszTagName, "0x%04x <!-- %s -->", (UInt32)val, pszTagIdName);
        }
    }

    DrError PutInt64TagValue(const char *pszTagName, Int64 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%016I64x" : "%I64d");
        return WriteSimpleTagValue(pszTagName, pszFormat, val);
    }

    DrError PutInt32TagValue(const char *pszTagName, Int32 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%08x" : "%d");
        return WriteSimpleTagValue(pszTagName, pszFormat, val);
    }

    DrError PutInt16TagValue(const char *pszTagName, Int16 val, bool fHex = false)
    {
        const char *pszFormat = (fHex ? "0x%04x" : "%d");
        return WriteSimpleTagValue(pszTagName, pszFormat, (Int32)val);
    }

    DrError PutBooleanTagValue(const char *pszTagName, bool val)
    {
        return WriteSimpleTagValue(pszTagName, "%s", (val ? "true" : "false"));
    }

    // safely XML-encodes the string
    DrError PutStringTagValue(const char *pszTagName, const char *val, int len = -1)
    {
        if (val == NULL) {
            return WriteSimpleTagValue(pszTagName, "%s", "<!-- null string -->");
        } else {
            if (len < 0) {
                len = (int)strlen(val);
            }
            DrStr512 strXml;
            strXml.AppendXmlEncodedString(val, (Size_t)len, true);
            return WriteSimpleTagValue(pszTagName, "%s", strXml.GetString());
        }
    }

    DrError PutGuidTagValue(const char *pszTagName, const DrGuid& val)
    {
        char buff[40];
        return WriteSimpleTagValue(pszTagName, "%s", val.ToString(buff, false));
    }

    DrError PutTimeStampTagValue(const char *pszTagName, DrTimeStamp val)
    {
        char buff[64];
        DrError err;
        if (val == DrTimeStamp_Never) {
                err = WriteSimpleTagValue(pszTagName, "never", (UInt64)val, buff);
        } else if (val == DrTimeStamp_LongAgo) {
                err = WriteSimpleTagValue(pszTagName, "long ago", (UInt64)val, buff);
        } else {
            err = DrTimeStampToString(val, buff, sizeof(buff), false);
            if (err == DrError_OK) {
                err = WriteSimpleTagValue(pszTagName, "%I64u<!-- %s -->", (UInt64)val, buff);
            } else {
                m_pWriter->SetStatus(err);
            }
        }
        return err;
    }

    DrError PutNodeAddressTagValue(const char *pszTagName, const DrNodeAddress& val)
    {
        char buff[64];
        DrError err = val.ToAddressPortString(buff, sizeof(buff));
        if (err == DrError_OK) {
            err = WriteSimpleTagValue(pszTagName, "%s", buff);
        } else {
            m_pWriter->SetStatus(err);
        }
        return err;
    }

    DrError PutTimeIntervalTagValue(const char *pszTagName, DrTimeInterval val)
    {
        DrError err;

        char buff[k_DrTimeIntervalStringBufferSize];
        err = DrTimeIntervalToString(val, buff, sizeof(buff));
        LogAssert(err == DrError_OK);

        err = WriteSimpleTagValue(pszTagName, "%I64d<!-- %s -->", (Int64)val, buff);
        
        return err;
    }

    DrError PutDrErrorTagValue(const char *pszTagName, DrError val)
    {
        char buff[1024];
        const char *pszErr = DrGetErrorDescription(val, buff, sizeof(buff));
        // Assumes error code descriptions are XML-friendly
        DrError err = WriteSimpleTagValue(pszTagName, "0x%08x<!-- %s -->", (UInt32)val, pszErr);
        return err;
    }

    DrError PutDrExitCodeTagValue(const char *pszTagName, DrExitCode val)
    {
        // Assumes exit code descriptions are XML-friendly
        DrError err = WriteSimpleTagValue(pszTagName, "0x%08x<!-- %s -->", (UInt32)val, DREXITCODESTRING(val));
        return err;
    }

    DrError PutBlobTagValue(const char *pszTagName, DrMemoryReader *pReader, Size_t length, Size_t maxShowLength, const char *pszExtraAttributes = NULL);
    DrError PutEnvironmentBlockTagValue(const char *pszTagName, const DrEnvironmentStrings& envBlock);

    static const char *GetPropertyName(UInt16 enumId);
    static const char *GetTagName(UInt16 tagId);


    // Returns the number of nested DrTag begintags that are currently in effect.  0 means we are not inside a begin tag
    UInt32 GetCurrentTagLevel()
    {
        return m_beginTagStack.NumEntries();
    }

    // returns DrTag_InvalidTag if we are at level 0
    UInt16 GetCurrentNestedTagId()
    {
        if (m_beginTagStack.NumEntries() == 0) {
            return DrTag_InvalidTag;
        } else {
            return m_beginTagStack.TopOfStack();
        }
    }

    // returns NULL if the current nested DrTag tag name is not known
    const char *GetCurrentNestedTagName()
    {
        const char * pszTagName = GetTagName(GetCurrentNestedTagId());
        return pszTagName;
    }

    // Returns the DrTag tag level below which we are not allowed to pop, due to a balanced block being in effect
    UInt32 GetCurrentMinimumTagLevel()
    {
        if (m_balancedBlockStack.NumEntries() == 0) {
            return 0;
        } else {
            return m_balancedBlockStack.TopOfStack();
        }
    }

    // returns true if there is a partially written DrProperty waiting to be completed before it is dumped
    bool PartialPropertyIsPending()
    {
        return (m_state != DrPropertyDumper_StartOfTag);
    }
    

    // Begins a section where begin/end DrTags  must be balanced and there must not be be a partial property at the beginning or end
    // On entry, any partially written property is written out as a specially tagged "incomplete" property
    void BeginBalancedBlock();

    // If there is a partially written property pending, it is written out as a specially tagged "incomplete" property.
    // This resets the parser to expect a property ID next.
    // Also, if there are outstanding open begin-DrTag beyond the currend balanced block level, writes end DrTags for them (with XML comments
    // describing them as missing).
    // After this call, the current DrTag level will be equal to the current balanced block level.
    DrError SynchronizeToBalancedBlock();

    // endss a section where begin/end DrTags must be balanced.
    // If there is a partially written property pending, it is written out as a specially tagged "incomplete" property.
    // This resets the parser to expect a property ID next.
    // Also, if there are outstanding open begin-DrTag beyond the currend balanced block level, writes end DrTags for them (with XML comments
    // describing them as missing).
    // After this call, the current DrTag level will be equal to the current balanced block level.
    DrError EndBalancedBlock()
    {
        LogAssert(m_balancedBlockStack.NumEntries() != 0);
        SynchronizeToBalancedBlock();
        LogAssert(GetCurrentTagLevel() == GetCurrentMinimumTagLevel());
        m_balancedBlockStack.Pop();
        return m_pWriter->GetStatus();
    }

    // writes an XML equivalent to a  DrTag "begin tag", and pushes a level
    DrError WriteAndPushDrBeginTag(UInt16 tagId);

    // writes an XML equivalent to a  DrTag "end tag", and pops a level 
    DrError WriteAndPopDrEndTag(UInt16 tagId);

    // If there is a partially written property pending, it is written out as a specially tagged "incomplete" property.
    // This resets the parser to expect a property ID next.
    DrError WritePartialProperty();

    // Final flush of the dumper. must be called from balanced block level 0
    DrError WriteIncompleteEndTagsAndFlushWriter()
    {
        LogAssert(m_balancedBlockStack.NumEntries() == 0);
        SynchronizeToBalancedBlock();
        return m_pWriter->FlushMemoryWriter();
    }
    // reads all remaining bytes from the reader and generates XML output as appropriate
    // The content of the reader does not need to be balanced or complete, and it need not begin or end at a property boundary; you may call this multiple times to
    // represent a coherent property stream
    // If you want to enforce tag balancing accross a section, call BeginBalancedBlock before calling this, and EndBalancedBlock after calling this.
    // if maxLength is specified, the function will return after reading maxLength bytes even if end of stream is not reached
    DrError ParseAndWriteFromReader(DrMemoryReader *pReader, Size_t maxLength=MAX_SIZE_T);

    DrError ParseAndWriteFromBuffer(DrMemoryBuffer *pBuffer, Size_t initialOffset=0, Size_t maxLength = MAX_SIZE_T)
    {
        DrMemoryBufferReader reader(pBuffer);
        if (initialOffset != 0) {
            reader.SetBufferOffset(initialOffset);
        }
        return ParseAndWriteFromReader(&reader, maxLength);
    }

    DrError ParseAndWriteFromSingleBlock(const void *pData, Size_t length)
    {
        DrSingleBlockReader reader(pData, length);
        return ParseAndWriteFromReader(&reader, length);
    }


    // reads the next single tag&value and outputs it. Does not reads aggregates--begintag and endtag are
    // trated as independent values; however, it does increment indentation in begintag, and decrement it on endtag.
    __declspec(deprecated) DrError PutNextPropertyTagValue()
    {
        return PutNextPropertyTagValue(m_pReader);
    }

    
    DrError PutNextPropertyTagValue(DrMemoryReader *pReader);

    // Reads the remainder of the input stream (Until DrError_EndOfStream is returned) as a sequential list
    // of 0 or more properties and/or aggregates, and outputs them accordingly. Does not return until the entire stream has been read.
    // Will always write a balanced set even if the input is unbalanced (will add missing end tags, etc., as necessary).
    // returns the writer status
    __declspec(deprecated) DrError PutNestedPropertyList()
    {
        return PutNestedPropertyList(m_pReader);
    }
    DrError PutNestedPropertyList(DrMemoryReader *pReader)
    {
        BeginBalancedBlock();
        ParseAndWriteFromReader(pReader);
        return EndBalancedBlock();
    }

    bool IsAtBeginningOfLine()
    {
        return m_fAtBol;
    }

private:
    DrMemoryReader *m_pReader;
    bool m_fDeleteReader;
    DrMemoryWriter *m_pWriter;
    bool m_fDeleteWriter;
    int m_indent;
    int m_nIndentSpacesPerLevel;
    bool m_fAtBol;  // Beginning-of-line flag
    bool m_fOutputCrlf;  // True if '\r' should precede each '\n' on output.
    size_t m_nbMaxBlobSize;
    size_t m_nbMaxPayloadSize;
    bool m_fIncludeComments;

    DrPropertyDumperState m_state;

    // A stack of unmatched begintag tagids. The top of stack is the end tag we are currently looking for.
    // The number of entries in this list is the current tag "level"
    DrValList<UInt16> m_beginTagStack;

    // A stack of pushed "BeginBalancedBlock" "level" parameters.
    // The top of the stack is the current balanced block level -- the parser will not allow end tags to pop the beginTagStack
    // above this level.
    DrValList<UInt32> m_balancedBlockStack;

    // buffer to store an incomplete property we are parsing
    // Includes property tag
    DrRef<DrSimpleHeapBuffer> m_pPartialPropertyBuffer;

    UInt16 m_partialPropertyId;
    Size_t m_partialPropertyTotalLength;
    
};
*/

extern void DrInitPropertyTable();
extern void DrInitTagTable();

/* JC
typedef DrError (*DrPropertyConverter)(DrPropertyDumper *pDumper, UInt16 enumId, const char *propertyName);

// returns ERROR_ALREADY_ASSIGNED if the property has already been defined
extern DrError DrAddPropertyToDumper(UInt16 prop, const char *pszDescription, DrPropertyConverter pConverter);

// returns ERROR_ALREADY_ASSIGNED if the tag has already been defined
extern DrError DrAddTagToDumper(UInt16 tag, const char *pszDescription);
*/

#pragma warning(pop)

