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
#include <math.h>

#pragma unmanaged

//
// Add contents of provided string to the DrStr
//
DrStr& DrStr::Append(const WCHAR *psz, size_t len, UINT codePage)
{
    //
    // Grow DrStr to accomodate extra length
    //
    GrowTo(m_stringLen + len);

    //
    // If there's anything to append
    //
    if (len != 0) 
    {
        LogAssert(m_nbBuffer > m_stringLen + 1);
        Size_t n = m_nbBuffer - m_stringLen - 1;

        //
        // Convert WCHAR to multibyte string
        //
        int ret = WideCharToMultiByte(
            codePage,
            0,
            psz,
            (int)len,
            m_pBuffer + m_stringLen,
            (int)n,
            NULL,
            NULL);
        if (ret == 0) 
        {
            //
            // If failed, check that there wasn't enough room
            // todo: what if there is some other failure? This would fail the assertion
            //
            DrError err = DrGetLastError();
            LogAssert(err == DrErrorFromWin32(ERROR_INSUFFICIENT_BUFFER));

            //
            // The result wouldn't fit. Rather than iterating on heap allocation, we'll just ask windows to calculate the needed size:
            //
            int ret2 = WideCharToMultiByte(
                codePage,
                0,
                psz,
                (int)len,
                NULL,
                0,
                NULL,
                NULL);
            LogAssert(ret2 > 1);


            //
            // Grow to required size and reconvert
            //
            GrowTo(m_stringLen + ret2);
            n = m_nbBuffer - m_stringLen - 1;
            ret = WideCharToMultiByte(
                codePage,
                0,
                psz,
                (int)len,
                m_pBuffer + m_stringLen,
                (int)n,
                NULL,
                NULL);
            LogAssert(ret == ret2);
        } 
        else 
        {
            LogAssert(ret > 0);
        }

        //
        // update length of string
        //
        UpdateLength(m_stringLen+ret);
    }

    return *this;
}

#pragma warning (disable: 4995) // _vsnprintf deprecated
DrStr& DrStr::VAppendF(const char *pszFormat, va_list args)
{
    // TODO: this could be made more efficient by implementing vprintf ourselves, since we could
    // TODO: reallocate and continue without starting over
    GrowTo(64);   // Start with something

    while (true) {
        // See how many bytes are remaining in the buffer.
        Size_t n = m_nbBuffer - m_stringLen;

        // Don't even bother trying to fit it in less than 10 bytes.
        if (n > 10) {
            // BUGBUG: not sure what _vsnprintf does when n-1 >_I32_MAX, since it cannot return a correct length as a result. So, we have to cap it at _I32_MAX-1
            // BUGBUG: and crash if it does not fit
            if (n > (Size_t)_I32_MAX) {
                n = (Size_t)_I32_MAX;
            }
            int ret = _vsnprintf(m_pBuffer + m_stringLen, n - 1, pszFormat, args);
            if (ret >= 0) {
                m_stringLen += ret;
                m_pBuffer[m_stringLen] = '\0';
                break;
            }
        }

        // Wouldn't fit.

        // Throw away partial result
        m_pBuffer[m_stringLen] = '\0';

        // If we have already tried the max for _vsnprintf, we have to crash
        LogAssert(n < (Size_t)_I32_MAX);
        
        // grow buffer, and try again.
        GrowTo(m_nbBuffer +1);  // This will force size doubling
    }

    return *this;
}

//
// Append the value in an environment variable onto the end of the current string
//
DrError DrStr::AppendFromEnvironmentVariable(const char *pszVarName)
{
    const char *pszValue = NULL;
    DrError err = DrGetEnvironmentVariable(pszVarName, &pszValue);
    if (err == DrError_OK) 
    {
        Append(pszValue);
        free((char *)pszValue);
    }
    return err;
}

DrStr& DrStr::AppendXmlEncodedString(const char *pszUnencoded, Size_t len, bool fEscapeNewlines)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);
        
        char c;
        while ((c = *pszUnencoded++) != '\0')
        {
            if (c == '<') {
                *this += "&lt;";
            } else if (c == '>') {
                *this += "&gt;";
            } else if (c == '\'') {
                *this += "&#39;";
            } else if (c == '"') {
                *this += "&quot;";
            } else if (c == '&') {
                *this += "&amp;";
            } else if (!fEscapeNewlines && (c == '\n' || c == '\r')) {
                *this += c;
            } else if ((unsigned char)c < ' ') {
                AppendF("&#x%02X;", (unsigned int)(unsigned char)c);
            } else {
                *this += c;
            }
        }
    }

    return *this;
}

bool DrStr::SubstrIsEqual(size_t index, const char *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (memcmp(m_pBuffer+index, pszMatch, matchLen) == 0);
}

bool DrStr::SubstrIsEqualNoCase(size_t index, const char *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (_strnicmp(m_pBuffer+index, pszMatch, matchLen) == 0);
}

//
// Returns first index where a given character can be found starting at a certain index
// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// Uses strchr - not multibyte aware.
//
size_t DrStr::IndexOfChar(char c, size_t startIndex) const
{
    //
    // If start Index invalid, return invalid index
    //
    if(startIndex > m_stringLen) 
    {
        return DrStr_InvalidIndex;
    }

    //
    // If character not found, return invalid index
    //
    char *p = strchr(m_pBuffer+startIndex, c);
    if (p == NULL) 
    {
        return DrStr_InvalidIndex;
    }

    //
    // If character found, return offset from where character found from beginning of string
    //
    return (size_t)(p - m_pBuffer);
}

//
// Make string lower case
// Uses _strlwr (not multibyte aware)
//
DrStr& DrStr::ToLowerCase()
{
    if (m_pBuffer != NULL) 
    {
        LogAssert(m_pBuffer[m_stringLen] == '\0');
        _strlwr(m_pBuffer);
    }

    return *this;
}

//
// Make string upper case
// Uses _strupr (not multibyte aware)
//
DrStr& DrStr::ToUpperCase()
{
    if (m_pBuffer != NULL) 
    {
        LogAssert(m_pBuffer[m_stringLen] == '\0');
        _strupr(m_pBuffer);
    }

    return *this;
}

//
// Increases size of string if needed
// Ensures that there are at least maxStringLength+1 bytes available (including null terminator) for the string.
// Does not change the string length.
// If the string was NULL, it becomes an empty string.
//
void DrStr::GrowTo(size_t maxStringLength)
{
    size_t maxBuff = maxStringLength + 1;

    //
    // If there is not yet a string, make it an empty string
    //
    if (m_pBuffer == NULL) 
    {
        // switch from NULL to an empty string in the static buffer
        m_pBuffer = m_pStaticBuffer;
        m_nbBuffer = m_nbStatic;
        if (m_pBuffer != NULL) {
            LogAssert(m_nbBuffer != 0);
            m_pBuffer[0] = '\0';
        }
    }

    //
    // If requested length is greater than existing length
    //
    if (m_nbBuffer < maxBuff) {
        //
        // Grow the heap string. Minimum = 64 characters, grow by factor of 2 beyond that.
        //
        size_t nbNew = m_nbBuffer * 2;
        if (nbNew < 64) 
        {
            nbNew = 64;
        }

        //
        // If 2*old_size is still less than required, increase to requested size
        //
        if (nbNew < maxBuff) 
        {
            nbNew = maxBuff;
        }

        //
        // Allocate a new buffer of required size
        //
        char *pNew = AllocateBiggerBuffer( nbNew );
        LogAssert(pNew != NULL);

        //
        // Copy old buffer contents into new buffer
        //
        if (m_stringLen != 0) 
        {
            memcpy(pNew, m_pBuffer, m_stringLen);
        }
        pNew[m_stringLen] = '\0';

        //
        // If old buffer is valid and not static
        //
        if (m_pBuffer != NULL && m_pBuffer != m_pStaticBuffer) 
        {
            delete[] m_pBuffer;
        }

        //
        // New buffer and buffer length becomes current buffer and buffer length
        //
        m_pBuffer = pNew;
        m_nbBuffer = nbNew;
    }
}

DrInternalizedStringPool g_DrInternalizedStrings;

const char *DrInternalizedStringPool::InternalizeStringLowerCase(const char *pszString)
{
    if (pszString == NULL) {
        return NULL;
    }
    
    // Normalize the instance to lower case so it is case insensitive...
    DrStr1024 strTemp;
    strTemp = pszString;
    strTemp.ToLowerCase();
    const char *pszResult = InternalizeString(strTemp);
    return pszResult;
}

const char *DrInternalizedStringPool::InternalizeStringUpperCase(const char *pszString)
{
    if (pszString == NULL) {
        return NULL;
    }
    
    // Normalize the instance to lower case so it is case insensitive...
    DrStr1024 strTemp;
    strTemp = pszString;
    strTemp.ToUpperCase();
    const char *pszResult = InternalizeString(strTemp);
    return pszResult;
}


const char *DrInternalizedStringPool::InternalizeString(const char *pszString)
{
    if (pszString == NULL) {
        return NULL;
    }

    Size_t length;
    UInt32 hash = StringHash(pszString, &length);
    const char *pszMatch = NULL;
    {
        MUTEX_LOCK(lock, m_Mutex);
        UInt32 bucket = hash % m_hashTableSize;
        InternalizedStringHeader *pHeader = m_pBuckets[bucket];

        while (pHeader != NULL) {
            LogAssert(pHeader->magic == k_internalizedStringMagic);
            if (pHeader->hash == hash) {
                const char *pszExisting = (const char *)(const void *)(pHeader+1);
                if (strcmp(pszExisting, pszString) == 0) {
                    pszMatch = pszExisting;
                    break;
                }
            }
            pHeader = pHeader->pNext;
        }
        
        if (pszMatch == NULL) {
            pHeader = (InternalizedStringHeader *)allocMem(sizeof(InternalizedStringHeader) + length + 1);
            char *pszNew = (char *)(void *)(pHeader+1);
            memcpy(pszNew, pszString, length+1);
            pHeader->magic = k_internalizedStringMagic;
            pHeader->hash = hash;
            pHeader->pNext = m_pBuckets[bucket];
            m_pBuckets[bucket] = pHeader;
            pszMatch = pszNew;
        }
    }

    return pszMatch;
    
}

DrError DrStringToPortNumber(const char *psz, DrPortNumber *pResult)
{
    if (psz == NULL) {
        return DrError_InvalidParameter;
    }

    DrError err = DrError_OK;

    if (_stricmp(psz, "any") == 0 || _stricmp(psz, "*") == 0) {
        *pResult = DrAnyPortNumber;
    } else if (_stricmp(psz, "invalid") == 0) {
        *pResult = DrInvalidPortNumber;
    } else {
        err = DrStringToUInt16(psz, pResult);
    }
    return err;
}

//JC
#if 0
DrStr& DrStr::AppendNodeAddress(const DrNodeAddress& val)
{
    char buff[64];
    DrError err = val.ToAddressPortString(buff, sizeof(buff));
    LogAssert(err == DrError_OK);
    return Append(buff);
}



DrStr& DrStr::AppendHexBytes(const void *pData, size_t numBytes, bool fUppercase)
{
    const BYTE *pbData = (const BYTE *)pData;
    for (size_t i = 0; i < numBytes; i++) {
        AppendF(fUppercase ? "%02X" : "%02x", pbData[i]);
    }
    return *this;
}

DrStr& DrStr::AppendCQuoteEncodedString(const char *pszUnencoded, Size_t len)
{
    LogAssert (pszUnencoded != NULL || len == 0);
    if (len == 0) {
        return *this;
    }

    for (Size_t i = 0; i < len; i++) {
        char c = pszUnencoded[i];
        if (c < ' ' || c > 126) {
            if (c == '\r') {
                Append("\\r");
            } else if (c == '\n') {
                Append("\\n");
            } else if (c == '\t') {
                Append("\\t");
            } else {
                Append("\\x");
                AppendHexBytes(&c, 1, false);
            }
        } else {
            switch(c) {
                case '\\':
                    Append("\\\\");
                    break;
                case '"':
                    Append("\\\"");
                    break;
                default:
                    Append(c);
            };
        }
    }

    return *this;
}


// Encodes a string for inclusion in XML text, outside of an element. Escapes "<" and "&", and nonprinting characters.
DrStr& DrStr::AppendXmlTextEncodedString(const char *pszUnencoded, Size_t len, bool fEscapeNewlines)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);
        
        for (Size_t i = 0; i < len; i++) {
            char c = pszUnencoded[i];
            if (c == '<') {
                *this += "&lt;";
            } else if (c == '&') {
                *this += "&amp;";
            } else if (!fEscapeNewlines && (c == '\n' || c == '\r')) {
                *this += c;
            } else if ((unsigned char)c < ' ') {
                AppendF("&#x%02X;", (unsigned int)(unsigned char)c);
            } else {
                *this += c;
            }
        }
    }

    return *this;
}

// Encodes a string for inclusion in an XML double-quoted attribute value. Escapes "<", "&", and "\"".
DrStr& DrStr::AppendXmlDQuoteEncodedString(const char *pszUnencoded, Size_t len, bool fEscapeNewlines)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);
        
        for (Size_t i = 0; i < len; i++) {
            char c = pszUnencoded[i];
            if (c == '<') {
                *this += "&lt;";
            } else if (c == '"') {
                *this += "&quot;";
            } else if (c == '&') {
                *this += "&amp;";
            } else if (!fEscapeNewlines && (c == '\n' || c == '\r')) {
                *this += c;
            } else if ((unsigned char)c < ' ') {
                AppendF("&#x%02X;", (unsigned int)(unsigned char)c);
            } else {
                *this += c;
            }
        }
    }

    return *this;
}

// Encodes a string for inclusion in an XML single-quoted attribute value. Escapes "<", "&", and "'".
DrStr& DrStr::AppendXmlSQuoteEncodedString(const char *pszUnencoded, Size_t len, bool fEscapeNewlines)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);
        
        for (Size_t i = 0; i < len; i++) {
            char c = pszUnencoded[i];
            if (c == '<') {
                *this += "&lt;";
            } else if (c == '\'') {
                *this += "&apos;";
            } else if (c == '&') {
                *this += "&amp;";
            } else if (!fEscapeNewlines && (c == '\n' || c == '\r')) {
                *this += c;
            } else if ((unsigned char)c < ' ') {
                AppendF("&#x%02X;", (unsigned int)(unsigned char)c);
            } else {
                *this += c;
            }
        }
    }

    return *this;
}

// Encodes a string for inclusion in XML text, outside of an element. Wraps with CDATA. Useful for
// long strings with lots of delimiters. Handles embedded "]]>" by splitting into two CDATA sections.
DrStr& DrStr::AppendXmlTextEncodedStringAsCDATA(const char *pszUnencoded, Size_t len)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);

        *this +="<![CDATA[";
        
        for (Size_t i = 0; i < len; i++) {
            char c = pszUnencoded[i];
            if (c != ']' || i + 2 >= len || pszUnencoded[i+1] != ']' || pszUnencoded[i+2] != '>') {
                *this += c;
            } else {
                // We found an embedded "]]>". To handle this, we will output "]]", then close the current CDATA section, then open
                // a new one and output the ">".
                *this += "]]]]><![CDATA[>";
                i += 2;
            }
        }
    }

    return *this;
}


DrStr& DrStr::ParseNextCommandLineArg(const char *pszCommandLine, size_t *pNumCharsConsumedOut)
{
    size_t n = 0;
    if (pszCommandLine == NULL) {
        // null commandline
        SetToNull();
    } else {
        while (pszCommandLine[n] == ' ' || pszCommandLine[n] == '\t') {
            // skip leading blanks and tabs
            n++;
        }
        if (pszCommandLine[n] == '\0') {
            // empty commandline
            SetToNull();
        } else {
            SetToEmptyString();
            bool fInQuote = false;
            bool fTerminateQuoteAfterWord = false;
            char c;
            for (; (c = pszCommandLine[n]) != '\0'; n++) {
                if (c == ' ' || c == '\t') {
                    if (fInQuote && !fTerminateQuoteAfterWord) {
                        // quoted blank or tab
                        Append(c);
                    } else {
                        // End of quote or unquoted word
                        fInQuote = false;
                        fTerminateQuoteAfterWord = false;
                        
                        // Consume trailing blanks/tabs and terminate parsing
                        do {
                            n++;
                        } while (pszCommandLine[n] == ' ' || pszCommandLine[n] == '\t' );
                        break;
                    }
                } else if (c == '"') {
                    if (pszCommandLine[n+1] == '\"' && pszCommandLine[n+2] == '\"') {
                        // There are 3 quotes in a row -- it is an escaped quote.
                        Append('"');
                        n += 2;
                    } else {
                        // Toggle quote mode. For compatibilty, quoting always extends to the begginning/end of a word.
                        if (!fInQuote) {
                            fInQuote = true;
                            fTerminateQuoteAfterWord = false;
                        } else {
                            fTerminateQuoteAfterWord = !fTerminateQuoteAfterWord;
                        }
                    }
                } else if (c == '\\') {
                    // backslash can escape a backslash preceding a quote, or a quote
                    int nBackslashes;
                    for (nBackslashes = 1; pszCommandLine[n+nBackslashes] == '\\'; nBackslashes++) {
                        // nothing to do
                    }
                    // advance to last backslash
                    n += (nBackslashes - 1);
                    bool fAddQuote = false;
                    if (pszCommandLine[n+1] == '"') {
                        // Advance past the last backslash
                        n++;
                        // The backslashes are terminated by a quote. There are to be interpreted as an escape sequence
                        // If there are an odd number of backslashes, then the last one is an escape for the quote
                        fAddQuote = (nBackslashes & 1) != 0;
                        // each pair of backslashes becomes a single backslash
                        nBackslashes = nBackslashes >> 1;
                    }
                    for (int i = 0; i < nBackslashes; i++) {
                        Append('\\');
                    }
                    if (fAddQuote) {
                        Append('"');
                    }
                } else {
                    // normal chars are simply copied
                    Append(c);
                }
            }
        }
    }

    if (pNumCharsConsumedOut != NULL) {
        *pNumCharsConsumedOut = n;
    }

    return *this;
}

// Parses a conventionally escaped/quoted command line string into a DrStrList of argument strings, with escaping removed
DrStrList& DrParseCommandLineToList(DrStrList& argListOut, const char *pszCommandLine)
{
    argListOut.Clear();

    size_t nConsumed = 0;
    DrStr64 strArg;
    while (strArg.ParseNextCommandLineArg(pszCommandLine, &nConsumed) != NULL) {
        argListOut.AddString(strArg);
        pszCommandLine += nConsumed;
    }

    return argListOut;
}

void DrFreeParsedCommandLineArgv(char **argv)
{
    delete[] argv;
}

// Parses a conventionally escaped/quoted command line string into a UTF-8 encoded argc/argv pair.
// The returned argv value should be freed with DrFreeParsedCommandLineArgv
void DrParseCommandLineToArgv(const char *pszCommandLine, int *pargc, char ***pargv)
{
    char **newArgv = NULL;

    DrStrList argList;
    DrParseCommandLineToList(argList, pszCommandLine);
    int argc = (int)argList.GetNumStrings();
    if (argc > 0) {
        newArgv = new char *[(size_t)argc];
        LogAssert(newArgv != NULL);
        for (int i = 0; i < argc; i++) {
            if (argList[i] == NULL) {
                newArgv[i] = NULL;
            } else {
                newArgv[i] = new char[argList[i].GetLength() + 1];
                LogAssert(newArgv[i] != NULL);
                memcpy(newArgv[i], argList[i].GetString(), argList[i].GetLength() + 1);
            }
        }
    }

    *pargv = newArgv;
    *pargc = argc;
}

DrStr& DrStr::AppendEncodedCommandLine(const DrStrList& argList)
{
    EnsureNotNull();
    UInt32 n = argList.GetNumStrings();
    for (UInt32 i = 0; i < n; i++) {
        if (i != 0) {
            Append(' ');
        }
        AppendCommandLineEncodedString(argList[i]);
    }
    return *this;
}

DrStr& DrStr::AppendEncodedCommandLine(int argc, char **argv)
{
    EnsureNotNull();
    for (int i = 0; i < argc; i++) {
        if (i != 0) {
            Append(' ');
        }
        AppendCommandLineEncodedString(argv[i]);
    }
    return *this;
}


DrStr& DrStr::AppendCommandLineEncodedString(const char *pszUnencoded, Size_t len)
{
    bool fEncloseInQuotes = false;
    Size_t i;

    EnsureNotNull();

    // first determine if the string must be quoted
    for (i = 0;i < len;i++) {
        char c = pszUnencoded[i];
        if (c == '\0') {
            // no way to encode null character, so truncate the string at the null
            len = i;
            break;
        } else if ((c >= '\0' && c <= ' ') || c == (char)127 || c == '`' || c == '^' || c == '&' || c == '(' || c == ')' || c == '{' || c == '}' || c == '[' ||
            c == ']' || c == '|' || c == ';' || c == '\'' || c == '"' || c == ',' || c == '<' || c == '>') {
            // characters that are interpreted as delimters by the shell should generally be quoted to be safe
            fEncloseInQuotes = true;
            break;
        }
    }

    if (fEncloseInQuotes) {
        Append('"');
    }


    for (i = 0; i < len; i++) {
        char c = pszUnencoded[i];
        switch(c) {
            case '\0':
                // Premature null byte, can't be escaped, stop here
                goto done;
                
            case '"':
                // Escape a double quote with a backslash
                Append("\\\"", 2);
                break;

            case '\\':
                // Backslashes are escaped only if followed by a double quote
                {
                    // skip past contiguous backslashes
                    Size_t nBackslashes;
                    for (nBackslashes = 1; i + nBackslashes < len && pszUnencoded[i + nBackslashes] == '\\'; nBackslashes++) {
                        // nothing to do
                    }
                    if (i + nBackslashes < len && pszUnencoded[i + nBackslashes] == '"') {
                        // backslashes preceeding a quote must be escaped
                        for (int i = 0; i < nBackslashes; i++) {
                            Append("\\\\", 2);
                        }
                    } else {
                        // backslashes not preceding a quote need not be escaped
                        for (int i = 0; i < nBackslashes; i++) {
                            Append('\\');
                        }
                    }
                    i += (nBackslashes - 1);
                }
                
                break;

            default:
                // All other characters are not escaped
                Append(c);
                break;
        };
    }

done:    
    if (fEncloseInQuotes) {
        Append('"');
    }

    return *this;

}

DrStr& DrStr::AppendDrvEncodedString(const char *pszUnencoded, Size_t len)
{
    LogAssert (pszUnencoded != NULL || len == 0);
    if (len == 0) {
        return *this;
    }

    // determine if quotes are required
    bool fQuote = false;
    if (pszUnencoded[0] == ' ' || pszUnencoded[0] == '\t' || pszUnencoded[len-1] == ' ' || pszUnencoded[len-1] == '\t') {
        // leading or trailing whitespace always requires quotes
        fQuote = true;
    } else {
        for (Size_t i = 0; i < len; i++) {
            char c = pszUnencoded[i];
            // newlines, double-quotes, and commas anywhere in the string require quoting
            if (c == '\n' || c == '\r' || c == '"' || c == ',') {
                fQuote = true;
                break;
            }
        }
    }

    // Append the string, with optional quotes
    if (fQuote) {
        Append('"');
    }

    for (Size_t i = 0; i < len; i++) {
        char c = pszUnencoded[i];
        if (c == '"') {
            Append("\"\"", 2);
        } else {
            Append(c);
        }
    }

    if (fQuote) {
        Append('"');
    }

    return *this;
}

DrError DrStr::AppendFromEnvironmentVariable(const char *pszVarName)
{
    const char *pszValue = NULL;
    DrError err = DrGetEnvironmentVariable(pszVarName, &pszValue);
    if (err == DrError_OK) {
        Append(pszValue);
        free((char *)pszValue);
    }
    return err;
}

DrStr& DrStr::AppendFromOptionalEnvironmentVariable(const char *pszVarName, const char *pszDefault)
{
    DrError err = AppendFromEnvironmentVariable(pszVarName);
    if (err != DrError_OK && pszDefault != NULL) {
        Append(pszDefault);
    }
    return *this;
}

// returns 1 if this string is greater, 0 if they are equal, and -1 if this string is less
// NULL is less than any other value
// Uses memcmp (not multibyte aware)
int DrStr::Compare(const char *pszOther, size_t length) const
{
    if (m_pBuffer == NULL) {
        if (pszOther == NULL) {
            return 0;
        } else {
            return -1;
        }
    } else if (pszOther == NULL) {
        return 1;
    }

    size_t minlen = length;
    if (minlen > m_stringLen) {
        minlen = m_stringLen;
    }

    int ret = 0;
    if (minlen != 0) {
        ret = memcmp(m_pBuffer, pszOther, minlen);
    }

    if (ret == 0) {
        if (m_stringLen > length) {
            ret = 1;
        } else if (m_stringLen < length) {
            ret = -1;
        }
    }

    return ret;
}

// returns 1 if this string is greater, 0 if they are equal, and -1 if this string is less
// Uses case insensitive compare
// Uses _stricmp (not multibyte aware)
// NULL is less than any other value
int DrStr::CompareNoCase(const char *pszOther) const
{
    if (m_pBuffer == NULL) {
        if (pszOther == NULL) {
            return 0;
        } else {
            return -1;
        }
    } else if (pszOther == NULL) {
        return 1;
    }

     int ret = _stricmp(m_pBuffer, pszOther);
     return ret;
}

bool DrStr::SubstrIsEqual(size_t index, const char *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (memcmp(m_pBuffer+index, pszMatch, matchLen) == 0);
}

bool DrStr::SubstrIsEqualNoCase(size_t index, const char *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (_strnicmp(m_pBuffer+index, pszMatch, matchLen) == 0);
}


// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// Uses strchr - not multibyte aware.
size_t DrStr::IndexOfChar(char c, size_t startIndex) const
{
    if(startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    char *p = strchr(m_pBuffer+startIndex, c);
    if (p == NULL) {
        return DrStr_InvalidIndex;
    }
    return (size_t)(p - m_pBuffer);
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrStr::ReverseIndexOfChar(char c, size_t startLength) const
{
    if(startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    char * p = m_pBuffer + startLength;
    while (--p >= m_pBuffer) {
        if (*p == c) {
            return (size_t)(p - m_pBuffer);
        }
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// not multibyte aware.
size_t DrStr::IndexOfString(const char *psz, size_t startIndex) const
{
    if(psz == NULL || m_pBuffer == NULL || startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    char *p = strstr(m_pBuffer+startIndex, psz);
    if (p == NULL) {
        return DrStr_InvalidIndex;
    }
    return (size_t)(p - m_pBuffer);
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrStr::ReverseIndexOfString(const char *psz, size_t startLength) const
{
    if(psz == NULL || startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    size_t slen = strlen(psz);
    if (slen > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t spos = startLength;
    if (spos > m_stringLen - slen) {
        spos = m_stringLen - slen;
    }
    char * p = m_pBuffer + spos;
    while (p >= m_pBuffer) {
        if (strncmp(p, psz, slen) == 0) {
            return (size_t)(p - m_pBuffer);
        }
        p--;
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// not multibyte aware.
size_t DrStr::IndexOfStringNoCase(const char *psz, size_t startIndex) const
{
    if(psz == NULL || m_pBuffer == NULL || startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t slen = strlen(psz);
    if (slen > m_stringLen - startIndex) {
        return DrStr_InvalidIndex;
    }
    size_t epos = m_stringLen - slen;
    for (size_t i = startIndex; i <= epos; i++) {
        if (_strnicmp(m_pBuffer + i, psz, slen) == 0) {
            return i;
        }
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrStr::ReverseIndexOfStringNoCase(const char *psz, size_t startLength) const
{
    if(psz == NULL || startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    size_t slen = strlen(psz);
    if (slen > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t spos = startLength;
    if (spos > m_stringLen - slen) {
        spos = m_stringLen - slen;
    }
    char * p = m_pBuffer + spos;
    while (p >= m_pBuffer) {
        if (_strnicmp(p, psz, slen) == 0) {
            return (size_t)(p - m_pBuffer);
        }
        p--;
    }
    return DrStr_InvalidIndex;
}


DrStr& DrStr::DeleteRange(size_t startIndex, size_t numChars)
{
    size_t oldend = startIndex + numChars;
    LogAssert(oldend >= startIndex);  // overflow check
    LogAssert(m_pBuffer != NULL && oldend <= m_stringLen);
    if (numChars != 0) {
        if (oldend < m_stringLen) {
            memmove(m_pBuffer + startIndex, m_pBuffer + oldend, m_stringLen - oldend);
        }
        m_stringLen -= numChars;
        m_pBuffer[m_stringLen] = '\0';
    }
    return *this;
}

// Asserts that the startIndex is valid. If startIndex is 0 and the string is NULL,
// it is converted to an empty string before inserting.
DrStr& DrStr::Insert(size_t startIndex, const char *psz, size_t len)
{
    LogAssert(startIndex <= m_stringLen);
    size_t newlen = m_stringLen + len;
    LogAssert(newlen >= m_stringLen); // overflow check
    GrowTo(newlen);
    if (len != 0) {
        size_t newstart = startIndex + len;
        memmove(m_pBuffer + newstart, m_pBuffer + startIndex, m_stringLen -startIndex);
        memcpy(m_pBuffer + startIndex, psz, len);
        m_stringLen = newlen;
        m_pBuffer[m_stringLen] = '\0';
    }
    return *this;
}

// Asserts that the startIndex is valid. If startIndex is 0 and the string is NULL,
// it is converted to an empty string before inserting.
DrStr& DrStr::ReplaceRange(size_t startIndex, size_t oldLen, const char *psz, size_t newLen)
{
    LogAssert(startIndex <= m_stringLen);
    size_t oldend = startIndex + oldLen;
    LogAssert(oldend >= startIndex);  // overflow check
    LogAssert(oldend <= m_stringLen);
    if (newLen > oldLen) {
        size_t nInsert = newLen - oldLen;
        size_t ipos = startIndex + oldLen;
        GrowTo(m_stringLen + nInsert);
        if (ipos < m_stringLen) {
            memmove(m_pBuffer + ipos + nInsert, m_pBuffer + ipos, m_stringLen -ipos);
        }
        m_stringLen += nInsert;
        m_pBuffer[m_stringLen] = '\0';
    } else if (newLen < oldLen) {
        EnsureNotNull();
        size_t nDelete = oldLen - newLen;
        size_t ipos = startIndex + newLen;
        size_t epos = ipos + nDelete;
        if (epos < m_stringLen) {
            memmove(m_pBuffer + ipos , m_pBuffer + epos, m_stringLen -epos);
        }
        m_stringLen -= nDelete;
        m_pBuffer[m_stringLen] = '\0';
    }

    if (newLen != 0) {
        memcpy(m_pBuffer+startIndex, psz, newLen);
    }
    
    return *this;
}

__inline bool ISSPACE(char c)
{
    return isspace((unsigned char)c) != 0;
}



//
// Description:
//
//  NextUTF8 - find the next UTF8 character in a UTF8 string
//
// Arguments:
//
//       const char *psz      -  pointer to a valid UTF8 string.  
//       const char *&pszNext -  place to point to the next valid UTF8 character in the string psz
//       Size_t & cb          -  set to the size of the first UTF8 character in psz
//
// Return Value:
//
//      true - psz was a valid UTF8 character pointer, and pszNext points to the next UTF8 character in the string.
//      false - the string pointed to in psz was not valid UTF8 (as defined by the below table).
//
//
// Ref: http://en.wikipedia.org/wiki/UTF-8
//
//    With these restrictions, bytes in a UTF-8 sequence have the following meanings. The ones marked in red can never appear in a legal UTF-8 sequence. 
//    The ones in green are represented in a single byte. The ones in white must only appear as the first byte in a multi-byte sequence, 
//    and the ones in orange can only appear as the second or later byte in a multi-byte sequence:
//
//    binary            hex   decimal notes                                                                             color
//    ----------------- ----- ------- ------------------------------------------------------------------------------    -----
//    00000000-01111111 00-7F 0-127   US-ASCII (single byte)                                                            GREEN
//    10000000-10111111 80-BF 128-191 Second, third, or fourth byte of a multi-byte sequence                            ORANGE
//    11000000-11000001 C0-C1 192-193 Overlong encoding: start of a 2-byte sequence, but code point <= 127              RED
//    11000010-11011111 C2-DF 194-223 Start of 2-byte sequence                                                          WHITE
//    11100000-11101111 E0-EF 224-239 Start of 3-byte sequence                                                          WHITE
//    11110000-11110100 F0-F4 240-244 Start of 4-byte sequence                                                          WHITE
//    11110101-11110111 F5-F7 245-247 Restricted by RFC 3629: start of 4-byte sequence for codepoint above 10FFFF       RED
//    11111000-11111011 F8-FB 248-251 Restricted by RFC 3629: start of 5-byte sequence                                  RED
//    11111100-11111101 FC-FD 252-253 Restricted by RFC 3629: start of 6-byte sequence                                  RED
//    11111110-11111111 FE-FF 254-255 Invalid: not defined by original UTF-8 specification                              RED
//
//     The bits of a Unicode character are distributed into the lower bit positions inside the UTF-8 bytes, with the lowest bit going into the last bit of the last byte:
// 
//     Unicode           Byte1    Byte2    Byte3    Byte4       example 
//     ----------------- -------- -------- -------- --------    ---------------------------------------------------------------------
//     U+000000-U+00007F 0xxxxxxx                               U+0024 ? 00100100 ? 0x24 
//     U+000080-U+0007FF 110xxxxx 10xxxxxx                      U+00A2 ? 11000010,10100010 ? 0xC2,0xA2 
//     U+000800-U+00FFFF 1110xxxx 10xxxxxx 10xxxxxx             U+20AC ? 11100010,10000010,10101100 ? 0xE2,0x82,0xAC 
//     U+010000-U+10FFFF 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx    U+10ABCD ? 11110100,10001010,10101111,10001101 ? 0xf4,0x8a,0xaf,0x8d 

__inline bool DrStr::NextUTF8(const char* psz, const char* &pszNext, Size_t &cb)
{
    const unsigned char *pc = (const unsigned char *)psz;
    unsigned char  c;
    unsigned int   nExtraBytes=0;
    
    pszNext = NULL;

    c=*pc;

    if(c <= 127)
    {
        // US ASCII
        cb = 1;
        pszNext = (char *)++pc;
        return true;
    }

    if(c == 0xc0 || c == 0xc1)
    {
        cb = 0;
        return false;
        }
    else if(c >= 0xc2 && c <= 0xdf)
    {
        // 2 byte sequence.
        cb = 2;
        nExtraBytes = 1;
    }
    else if(c >= 0xe0 && c <= 0xef)
    {
        // 3 byte sequence.
        cb = 3;
        nExtraBytes = 2;
    }
    else if(c >= 0xf0 && c <= 0xf4)
    {
        // 4 bytes sequence.
        cb = 4;
        nExtraBytes = 3;
    }
    else
    {
        // not a legal UTF8 sequence
        cb = 0;
        return false;
    }

    do
    {
        ++pc;
        c=*pc;
        nExtraBytes--;

        if(!(c >= 128 && c <= 191))
        {
            // not a valid second, third or forth byte of a multi-byte sequence.
            cb = 0;
            return false;
        }
        
    } while(c != '\0' && nExtraBytes != 0);

    if(nExtraBytes != 0)
    {
        cb = 0;
        return false;
    }
    else
    {
        pszNext = (char *)pc;
        return true;
    }
}

//
// Description:
//
//  IsValidUTF8Ex - check if this is a valid UTF8 string and does not include any characters in the 
//                  specified exclusion list.
//
// Arguments:
//
//      this                             - a DrStr
//      const char *excludeUTF8CharList  - a list of UTF8 characters which are not permitted in the string.
//      Size_t maxLen                    - the maximum string length in bytes (not characters) allowed.
//      bool bVisibleASCII               - if set, any ASCII character not in the visible ASCII set is not allowed
//      bool bExcludeDoubleSlash         - if set, no double slashes are allowed in the string
//      bool bExcludeTrailingSlash       - if set, the string must not end in a slash
//
// Return Value:
//
//      true - the DrStr met the criteria for validity
//      false - the DrStr does not meet the criteria for validity
//
bool DrStr::IsValidUTF8Ex(const char *excludeUTF8CharList, Size_t maxLen, bool bVisibleASCII, bool bExcludeDoubleSlash, bool bExcludeTrailingSlash)
{
    if(excludeUTF8CharList != NULL)
    {
        if(!DrStr64(excludeUTF8CharList).IsValidUTF8())
        {
            // the exclude list isn't valid 
            return false;
        }
    }

    // variables to walk the exclusion list
    const char *pExclude     = excludeUTF8CharList;
    const char *pNextExclude = pExclude;

    // variables to walk the string being tested    
    const char *pszUTF8     = m_pBuffer;
    const char *pszNextUTF8 = pszUTF8;

    // number of bytes in the string
    Size_t count=0;

    // the last ASCII character we parsed, if the last character was not ASCII this is set to '\0'
    char lastASCII='\0';

    bool rc=true;

    while(*pszUTF8 != '\0' && count < maxLen)
    {
        Size_t cLen;

        // get a pointer to the next character, and the length of the current character
        rc = NextUTF8(pszUTF8, pszNextUTF8, cLen);

        if(rc==false)
        {
            // invalid UTF8 encountered
            return false;
        }

        // check if this character is in the exclusion list
        if(excludeUTF8CharList)
        {
            Size_t cExcludeLen;
            
            while(*pExclude != '\0')
            {
                // walk the exclusion list, and check if the current character matches any excluded character
                rc=NextUTF8(pExclude, pNextExclude, cExcludeLen); 
            
                if(rc==true)
                {
                    if(cExcludeLen==cLen && (memcmp(pExclude,pszUTF8,cLen)==0) )
                    {
                        // character is in the exclude list
                        return false;
                    }
                }
                pExclude = pNextExclude;
            }

            pExclude = excludeUTF8CharList;  // back to the beginning for the next pass.
        }

        if(cLen==1)
        {
            if(bVisibleASCII)
            {
                if(*pszUTF8 >= 1 && *pszUTF8 <=31)
                {
                    // not printable ASCII character found
                    return false;
                }
            }

            if(bExcludeDoubleSlash)
            {
                if(lastASCII == '/' && *pszUTF8 == '/')
                {
                    // double slash not allowed in this string
                    return false;
                }
            }

            lastASCII = *pszUTF8;
        }
        else
        {
            lastASCII = '\0';   
        }

        pszUTF8 = pszNextUTF8;
        count+=cLen;
        
    }

    if(count > maxLen)
    {
        // too long
        return false;
    }

    if(bExcludeTrailingSlash && (lastASCII == '/'))
    {
        // trailing slash not allowed in this string
        return false;
    }

    return true;
    
}

// Removes whitespace from the start and end of the string
DrStr& DrStr::Trim()
{
    if (m_pBuffer != NULL) {
        size_t newlen = m_stringLen;
        while (newlen != 0 && ISSPACE(m_pBuffer[newlen-1])) {
            newlen--;
        }
        size_t nleading = 0;
        while(nleading < newlen && ISSPACE(m_pBuffer[nleading])) {
            nleading++;
        }
        if (nleading != 0 && nleading != newlen) {
            memmove(m_pBuffer, m_pBuffer + nleading, newlen - nleading);
        }
        m_stringLen = newlen - nleading;
        m_pBuffer[m_stringLen] = '\0';
    }
    return *this;
}

DrStr& DrStr::Append(const WCHAR *psz, size_t len, UINT codePage)
{
    GrowTo(m_stringLen + len);
    if (len != 0) {
        LogAssert(m_nbBuffer > m_stringLen + 1);
        Size_t n = m_nbBuffer - m_stringLen - 1;
        /*
        int WideCharToMultiByte(
          UINT CodePage, 
          DWORD dwFlags, 
          LPCWSTR lpWideCharStr, 
          int cchWideChar, 
          LPSTR lpMultiByteStr, 
          int cbMultiByte, 
          LPDRTR lpDefaultChar, 
          LPBOOL lpUsedDefaultChar 
        );
        */

        int ret = WideCharToMultiByte(
            codePage,
            0,
            psz,
            (int)len,
            m_pBuffer + m_stringLen,
            (int)n,
            NULL,
            NULL);

        if (ret == 0) {
            DrError err = DrGetLastError();
            LogAssert(err == DrErrorFromWin32(ERROR_INSUFFICIENT_BUFFER));
            // The result wouldn't fit. Rather than iterating on heap allocation, we'll just ask windows to calculate the needed size:
            int ret2 = WideCharToMultiByte(
                codePage,
                0,
                psz,
                (int)len,
                NULL,
                0,
                NULL,
                NULL);
            LogAssert(ret2 > 1);
            GrowTo(m_stringLen + ret2);
            n = m_nbBuffer - m_stringLen - 1;
            ret = WideCharToMultiByte(
                codePage,
                0,
                psz,
                (int)len,
                m_pBuffer + m_stringLen,
                (int)n,
                NULL,
                NULL);
            LogAssert(ret == ret2);
        } else {
            LogAssert(ret > 0);
        }
        UpdateLength(m_stringLen+ret);
    }

    return *this;
}

DrWStr& DrWStr::Append(const char *psz, size_t len, UINT codePage)
{
    GrowTo(m_stringLen + len);
    if (len != 0) {
        LogAssert(m_nbBuffer > m_stringLen + 1);
        Size_t n = m_nbBuffer - m_stringLen - 1;
        /*
         int MultiByteToWideChar(
          UINT CodePage, 
          DWORD dwFlags, 
          LPDRTR lpMultiByteStr, 
          int cbMultiByte, 
          LPWSTR lpWideCharStr, 
          int cchWideChar 
        );
        */

        int ret = MultiByteToWideChar(
            codePage,
            0,
            psz,
            (int)len,
            m_pBuffer + m_stringLen,
            (int)n);

        if (ret == 0) {
            DrError err = DrGetLastError();
            LogAssert(err == DrErrorFromWin32(ERROR_INSUFFICIENT_BUFFER));
            // The result wouldn't fit. Rather than iterating on heap allocation, we'll just ask windows to calculate the needed size:
            int ret2 = MultiByteToWideChar(
                codePage,
                0,
                psz,
                (int)len,
                NULL,
                0);
            LogAssert(ret2 > 1);
            GrowTo(m_stringLen + ret2);
            n = m_nbBuffer - m_stringLen - 1;
            ret = MultiByteToWideChar(
                codePage,
                0,
                psz,
                (int)len,
                m_pBuffer + m_stringLen,
                (int)n);
            LogAssert(ret == ret2);
        } else {
            LogAssert(ret > 0);
        }
        UpdateLength(m_stringLen+ret);
    }

    return *this;
}




// Ensures that there are at least maxStringLength+1 bytes available (including null terminator) for the string.
// Does not change the string length.
// If the string was NULL, it becomes an empty string.
void DrWStr::GrowTo(size_t maxStringLength)
{
    size_t maxBuff = maxStringLength + 1;

    if (m_pBuffer == NULL) {
        // switch from NULL to an empty string in the static buffer
        m_pBuffer = m_pStaticBuffer;
        m_nbBuffer = m_nbStatic;
        if (m_pBuffer != NULL) {
            LogAssert(m_nbBuffer != 0);
            m_pBuffer[0] = L'\0';
        }
    }

    if (m_nbBuffer < maxBuff) {
        // Grow the heap string
        size_t nbNew =m_nbBuffer * 2;
        if (nbNew < 64) {
            nbNew = 64;
        }
        if (nbNew < maxBuff) {
            nbNew = maxBuff;
        }
        WCHAR *pNew = new WCHAR[nbNew];
        LogAssert(pNew != NULL);
        if (m_stringLen != 0) {
            memcpy(pNew, m_pBuffer, m_stringLen * sizeof(WCHAR));
        }
        pNew[m_stringLen] = L'\0';
        if (m_pBuffer != NULL && m_pBuffer != m_pStaticBuffer) {
            delete[] m_pBuffer;
        }
        m_pBuffer = pNew;
        m_nbBuffer = nbNew;
    }
}


DrWStr& DrWStr::AppendNodeAddress(const DrNodeAddress& val)
{
    WCHAR buff[64];
    DrError err = val.ToAddressPortString(buff, ELEMENTCOUNT(buff));
    LogAssert(err == DrError_OK);
    return Append(buff);
}


DrWStr& DrWStr::VAppendF(const WCHAR *pszFormat, va_list args)
{
    // TODO: this could be made more efficient by implementing vprintf ourselves, since we could
    // TODO: reallocate and continue without starting over
    GrowTo(64);   // Start with something

    while (true) {
        // See how many WCHARS are remaining in the buffer.
        Size_t n = m_nbBuffer - m_stringLen;

        // Don't even bother trying to fit it in less than 10 WCHARS.
        if (n > 10) {
            // BUGBUG: not sure what _vsnwprintf does when n-1 >_I32_MAX, since it cannot return a correct length as a result. So, we have to cap it at _I32_MAX-1
            // BUGBUG: and crash if it does not fit
            if (n > (Size_t)_I32_MAX) {
                n = (Size_t)_I32_MAX;
            }
            int ret = _vsnwprintf(m_pBuffer + m_stringLen, n - 1, pszFormat, args);
            if (ret >= 0) {
                m_stringLen += ret;
                m_pBuffer[m_stringLen] = L'\0';
                break;
            }
        }

        // Wouldn't fit.
        // Throw away partial result
        m_pBuffer[m_stringLen] = '\0';
        
        // If we have already tried the max for _vsnwprintf, we have to crash
        LogAssert(n < (Size_t)_I32_MAX);
        
        // grow buffer, and try again.
        GrowTo(m_nbBuffer +1);  // This will force size doubling
    }

    return *this;
}

DrWStr& DrWStr::AppendHexBytes(const void *pData, size_t numBytes, bool fUppercase)
{
    const BYTE *pbData = (const BYTE *)pData;
    for (size_t i = 0; i < numBytes; i++) {
        AppendF(fUppercase ? L"%02X" : L"%02x", pbData[i]);
    }
    return *this;
}

DrWStr& DrWStr::AppendCQuoteEncodedString(const WCHAR *pszUnencoded, Size_t len)
{
    LogAssert (pszUnencoded != NULL || len == 0);
    if (len == 0) {
        return *this;
    }

    for (Size_t i = 0; i < len; i++) {
        WCHAR c = pszUnencoded[i];
        if (c < L' ') {
            if (c == L'\r') {
                Append(L"\\r");
            } else if (c == L'\n') {
                Append(L"\\n");
            } else if (c == L'\t') {
                Append(L"\\t");
            } else {
                Append(L"\\x");
                AppendHexBytes(&c, 1, false);
            }
        } else {
            switch(c) {
                case L'\\':
                    Append(L"\\\\");
                    break;
                case L'"':
                    Append(L"\\\"");
                    break;
                default:
                    Append(c);
            };
        }
    }

    return *this;
}

DrWStr& DrWStr::AppendXmlEncodedString(const WCHAR *pszUnencoded, Size_t len, bool fEscapeNewlines)
{
    if (len == 0) {
        EnsureNotNull();
    } else {
        LogAssert(pszUnencoded != NULL);
        
        WCHAR c;
        while ((c = *pszUnencoded++) != L'\0')
        {
            if (c == L'<') {
                *this += L"&lt;";
            } else if (c == L'>') {
                *this += L"&gt;";
            } else if (c == L'\'') {
                *this += L"&#39;";
            } else if (c == L'"') {
                *this += L"&quot;";
            } else if (c == L'&') {
                *this += L"&amp;";
            } else if (!fEscapeNewlines && (c == L'\n' || c == L'\r')) {
                *this += c;
            } else if ((unsigned char)c < L' ') {
                AppendF(L"&#x%02X;", (UInt16)c);
            } else {
                *this += c;
            }
        }
    }

    return *this;
}


DrWStr& DrWStr::AppendDrvEncodedString(const WCHAR *pszUnencoded, Size_t len)
{
    LogAssert (pszUnencoded != NULL || len == 0);
    if (len == 0) {
        return *this;
    }

    // determine if quotes are required
    bool fQuote = false;
    if (pszUnencoded[0] == L' ' || pszUnencoded[0] == L'\t' || pszUnencoded[len-1] == L' ' || pszUnencoded[len-1] == L'\t') {
        // leading or trailing whitespace always requires quotes
        fQuote = true;
    } else {
        for (Size_t i = 0; i < len; i++) {
            WCHAR c = pszUnencoded[i];
            // newlines, double-quotes, and commas anywhere in the string require quoting
            if (c == L'\n' || c == L'\r' || c == L'"' || c == L',') {
                fQuote = true;
                break;
            }
        }
    }

    // Append the string, with optional quotes
    if (fQuote) {
        Append(L'"');
    }

    for (Size_t i = 0; i < len; i++) {
        WCHAR c = pszUnencoded[i];
        if (c == L'"') {
            Append(L"\"\"", 2);
        } else {
            Append(c);
        }
    }

    if (fQuote) {
        Append(L'"');
    }

    return *this;
}

DrError DrWStr::AppendFromEnvironmentVariable(const WCHAR *pszVarName)
{
    const WCHAR *pszValue = NULL;
    DrError err = DrGetEnvironmentVariable(pszVarName, &pszValue);
    if (err == DrError_OK) {
        Append(pszValue);
        free((WCHAR *)pszValue);
    }
    return err;
}

DrWStr& DrWStr::AppendFromOptionalEnvironmentVariable(const WCHAR *pszVarName, const WCHAR *pszDefault)
{
    DrError err = AppendFromEnvironmentVariable(pszVarName);
    if (err != DrError_OK && pszDefault != NULL) {
        Append(pszDefault);
    }
    return *this;
}

// returns 1 if this string is greater, 0 if they are equal, and -1 if this string is less
// NULL is less than any other value
// Uses memcmp (not multibyte aware)
int DrWStr::Compare(const WCHAR *pszOther, size_t length) const
{
    if (m_pBuffer == NULL) {
        if (pszOther == NULL) {
            return 0;
        } else {
            return -1;
        }
    } else if (pszOther == NULL) {
        return 1;
    }

    size_t minlen = length;
    if (minlen > m_stringLen) {
        minlen = m_stringLen;
    }

    int ret = 0;
    if (minlen != 0) {
        ret = memcmp(m_pBuffer, pszOther, minlen * sizeof(WCHAR));
    }

    if (ret == 0) {
        if (m_stringLen > length) {
            ret = 1;
        } else if (m_stringLen < length) {
            ret = -1;
        }
    }

    return ret;
}

// returns 1 if this string is greater, 0 if they are equal, and -1 if this string is less
// Uses case insensitive compare
// Uses _stricmp (not multibyte aware)
// NULL is less than any other value
int DrWStr::CompareNoCase(const WCHAR *pszOther) const
{
    if (m_pBuffer == NULL) {
        if (pszOther == NULL) {
            return 0;
        } else {
            return -1;
        }
    } else if (pszOther == NULL) {
        return 1;
    }

     int ret = _wcsicmp(m_pBuffer, pszOther);
     return ret;
}

bool DrWStr::SubstrIsEqual(size_t index, const WCHAR *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (memcmp(m_pBuffer+index, pszMatch, matchLen * sizeof(WCHAR)) == 0);
}

bool DrWStr::SubstrIsEqualNoCase(size_t index, const WCHAR *pszMatch, size_t matchLen) const
{
    if (pszMatch == NULL) {
        LogAssert(matchLen == 0);
        return (index == 0 && m_pBuffer == NULL);
    }
    if (index + matchLen > m_stringLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return (_wcsnicmp(m_pBuffer+index, pszMatch, matchLen) == 0);
}


// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// Uses strchr - not multibyte aware.
size_t DrWStr::IndexOfChar(WCHAR c, size_t startIndex) const
{
    if(startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    WCHAR *p = wcschr(m_pBuffer+startIndex, c);
    if (p == NULL) {
        return DrStr_InvalidIndex;
    }
    return (size_t)(p - m_pBuffer);
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrWStr::ReverseIndexOfChar(WCHAR c, size_t startLength) const
{
    if(startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    WCHAR * p = m_pBuffer + startLength;
    while (--p >= m_pBuffer) {
        if (*p == c) {
            return (size_t)(p - m_pBuffer);
        }
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// not multibyte aware.
size_t DrWStr::IndexOfString(const WCHAR *psz, size_t startIndex) const
{
    if(psz == NULL || m_pBuffer == NULL || startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    WCHAR *p = wcsstr(m_pBuffer+startIndex, psz);
    if (p == NULL) {
        return DrStr_InvalidIndex;
    }
    return (size_t)(p - m_pBuffer);
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrWStr::ReverseIndexOfString(const WCHAR *psz, size_t startLength) const
{
    if(psz == NULL || startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    size_t slen = wcslen(psz);
    if (slen > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t spos = startLength;
    if (spos > m_stringLen - slen) {
        spos = m_stringLen - slen;
    }
    WCHAR * p = m_pBuffer + spos;
    while (p >= m_pBuffer) {
        if (wcsncmp(p, psz, slen) == 0) {
            return (size_t)(p - m_pBuffer);
        }
        p--;
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the starting index is out of range
// returns the string length if the null terminator is matched
// not multibyte aware.
size_t DrWStr::IndexOfStringNoCase(const WCHAR *psz, size_t startIndex) const
{
    if(psz == NULL || m_pBuffer == NULL || startIndex > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t slen = wcslen(psz);
    if (slen > m_stringLen - startIndex) {
        return DrStr_InvalidIndex;
    }
    size_t epos = m_stringLen - slen;
    for (size_t i = startIndex; i <= epos; i++) {
        if (_wcsnicmp(m_pBuffer + i, psz, slen) == 0) {
            return i;
        }
    }
    return DrStr_InvalidIndex;
}

// returns DrStr_InvalidIndex if there is no match or the startLength is out of range
// The startLength should be one greater than the first posible matching index (e.g., the length of the string to search)
size_t DrWStr::ReverseIndexOfStringNoCase(const WCHAR *psz, size_t startLength) const
{
    if(psz == NULL || startLength == 0 || startLength > m_stringLen+1 || m_pBuffer == NULL) {
        return DrStr_InvalidIndex;
    }
    size_t slen = wcslen(psz);
    if (slen > m_stringLen) {
        return DrStr_InvalidIndex;
    }
    size_t spos = startLength;
    if (spos > m_stringLen - slen) {
        spos = m_stringLen - slen;
    }
    WCHAR * p = m_pBuffer + spos;
    while (p >= m_pBuffer) {
        if (_wcsnicmp(p, psz, slen) == 0) {
            return (size_t)(p - m_pBuffer);
        }
        p--;
    }
    return DrStr_InvalidIndex;
}

// Uses _strlwr (not multibyte aware)
DrWStr& DrWStr::ToLowerCase()
{
    if (m_pBuffer != NULL) {
        LogAssert(m_pBuffer[m_stringLen] == L'\0');
        _wcslwr(m_pBuffer);
    }
    return *this;
}

// Uses _strupr (not multibyte aware)
DrWStr& DrWStr::ToUpperCase()
{
    if (m_pBuffer != NULL) {
        LogAssert(m_pBuffer[m_stringLen] == L'\0');
        _wcsupr(m_pBuffer);
    }
    return *this;
}

DrWStr& DrWStr::DeleteRange(size_t startIndex, size_t numChars)
{
    size_t oldend = startIndex + numChars;
    LogAssert(oldend >= startIndex);  // overflow check
    LogAssert(m_pBuffer != NULL && oldend <= m_stringLen);
    if (numChars != 0) {
        if (oldend < m_stringLen) {
            memmove(m_pBuffer + startIndex, m_pBuffer + oldend, (m_stringLen - oldend) * sizeof(WCHAR));
        }
        m_stringLen -= numChars;
        m_pBuffer[m_stringLen] = L'\0';
    }
    return *this;
}

// Asserts that the startIndex is valid. If startIndex is 0 and the string is NULL,
// it is converted to an empty string before inserting.
DrWStr& DrWStr::Insert(size_t startIndex, const WCHAR *psz, size_t len)
{
    LogAssert(startIndex <= m_stringLen);
    size_t newlen = m_stringLen + len;
    LogAssert(newlen >= m_stringLen); // overflow check
    GrowTo(newlen);
    if (len != 0) {
        size_t newstart = startIndex + len;
        memmove(m_pBuffer + newstart, m_pBuffer + startIndex, (m_stringLen -startIndex) * sizeof(WCHAR));
        memcpy(m_pBuffer + startIndex, psz, len * sizeof(WCHAR));
        m_stringLen = newlen;
        m_pBuffer[m_stringLen] = L'\0';
    }
    return *this;
}

// Asserts that the startIndex is valid. If startIndex is 0 and the string is NULL,
// it is converted to an empty string before inserting.
DrWStr& DrWStr::ReplaceRange(size_t startIndex, size_t oldLen, const WCHAR *psz, size_t newLen)
{
    LogAssert(startIndex <= m_stringLen);
    size_t oldend = startIndex + oldLen;
    LogAssert(oldend >= startIndex);  // overflow check
    LogAssert(oldend <= m_stringLen);
    if (newLen > oldLen) {
        size_t nInsert = newLen - oldLen;
        size_t ipos = startIndex + oldLen;
        GrowTo(m_stringLen + nInsert);
        if (ipos < m_stringLen) {
            memmove(m_pBuffer + ipos + nInsert, m_pBuffer + ipos, (m_stringLen -ipos) * sizeof(WCHAR));
        }
        m_stringLen += nInsert;
        m_pBuffer[m_stringLen] = L'\0';
    } else if (newLen < oldLen) {
        EnsureNotNull();
        size_t nDelete = oldLen - newLen;
        size_t ipos = startIndex + newLen;
        size_t epos = ipos + nDelete;
        if (epos < m_stringLen) {
            memmove(m_pBuffer + ipos , m_pBuffer + epos, (m_stringLen -epos) * sizeof(WCHAR));
        }
        m_stringLen -= nDelete;
        m_pBuffer[m_stringLen] = L'\0';
    }

    if (newLen != 0) {
        memcpy(m_pBuffer+startIndex, psz, newLen * sizeof(WCHAR));
    }
    
    return *this;
}

__inline bool ISWSPACE(WCHAR c)
{
    return iswspace(c) != 0;
}

// Removes whitespace from the start and end of the string
DrWStr& DrWStr::Trim()
{
    if (m_pBuffer != NULL) {
        size_t newlen = m_stringLen;
        while (newlen != 0 && ISWSPACE(m_pBuffer[newlen-1])) {
            newlen--;
        }
        size_t nleading = 0;
        while(nleading < newlen && ISWSPACE(m_pBuffer[nleading])) {
            nleading++;
        }
        if (nleading != 0 && nleading != newlen) {
            memmove(m_pBuffer, m_pBuffer + nleading, (newlen - nleading) * sizeof(WCHAR));
        }
        m_stringLen = newlen - nleading;
        m_pBuffer[m_stringLen] = L'\0';
    }
    return *this;
}

DrError DrStringToSignedOrUnsignedInt64(const WCHAR *psz, UInt64 *pResult, bool fSigned)
{
    UInt64 v = 0;
    UInt64 vnew;
    bool neg = false;
    bool gotDig = false;
    int base = 10;
    
    while (ISWSPACE(*psz)) {
        psz++;
    }

    if (*psz == L'+') {
        psz++;
    } else if (fSigned && *psz == L'-') {
        neg = true;
        psz++;
    }

    if (*psz == L'0' && (*(psz+1) == L'x' ||*(psz+1) == L'X') ) {
        psz += 2;
        base = 16;
    }

    if (base == 16 && neg == false) {
        // we allow hex constants to set the sign bit.
        fSigned = false;
    }
    
    while (*psz != L'\0' && !ISWSPACE(*psz)) {
        int dig = -1;
        if (*psz >= L'0' && *psz <= L'9') {
            dig = *psz - L'0';
        } else if (*psz >= L'a' && *psz <= L'f') {
            dig = *psz - L'a' + 10;
        } else if (*psz >= L'A' && *psz <= L'F') {
            dig = *psz - L'A' + 10;
        }
        if (dig < 0 || dig >= base) {
            return DrError_InvalidParameter;
        }
        vnew = v * base + dig;
        if ((fSigned && (Int64)vnew < 0) || ((vnew - dig)/base != v)) {
            // overflow
            return DrError_InvalidParameter;
        }
        v = vnew;
        gotDig = true;
        psz++;
    }

    if (!gotDig) {
        return DrError_InvalidParameter;
    }
    
    while (ISWSPACE(*psz)) {
        psz++;
    }

    if (*psz != L'\0') {
        return DrError_InvalidParameter;
    }

    if (neg) {
        v= (UInt64)(-(Int64)v);
    }

    *pResult = v;
    return DrError_OK;
}

DrError DrStringToFloat(const WCHAR *psz, float *pResult)
{
    double v;
    DrError err = DrStringToDouble(psz, &v);
    if (err == DrError_OK) {
        *pResult = (float)v;
    }
    return err;
}

DrError DrStringToUInt64(const WCHAR *psz, UInt64 *pResult)
{
    return DrStringToSignedOrUnsignedInt64(psz, pResult, false);
}

DrError DrStringToInt64(const WCHAR *psz, Int64 *pResult)
{
    return DrStringToSignedOrUnsignedInt64(psz, (UInt64 *)(void *)pResult, true);
}

DrError DrStringToUInt16(const WCHAR *psz, UInt16 *pResult)
{
    UInt64 v;
    DrError err = DrStringToSignedOrUnsignedInt64(psz, &v, false);
    if (err != DrError_OK) {
        return err;
    }
    UInt16 v16 = (UInt16)v;
    if (v != (UInt64)v16) {
        return DrError_InvalidParameter;
    }
    *pResult = v16;
    return DrError_OK;
}

DrError DrStringToPortNumber(const char *psz, DrPortNumber *pResult)
{
    if (psz == NULL) {
        return DrError_InvalidParameter;
    }

    DrError err = DrError_OK;
    
    if (_wcsicmp(psz, L"any") == 0 || _wcsicmp(psz, L"*") == 0) {
        *pResult = DrAnyPortNumber;
    } else if (_wcsicmp(psz, L"invalid") == 0) {
        *pResult = DrInvalidPortNumber;
    } else {
        err = DrStringToUInt16(psz, pResult);
    }
    return err;
}

DrError DrStringToUInt32(const WCHAR *psz, UInt32 *pResult)
{
    UInt64 v;
    DrError err = DrStringToSignedOrUnsignedInt64(psz, &v, false);
    if (err != DrError_OK) {
        return err;
    }
    UInt32 v32 = (UInt32)v;
    if (v != (UInt64)v32) {
        return DrError_InvalidParameter;
    }
    *pResult = v32;
    return DrError_OK;
}

DrError DrStringToInt32(const WCHAR *psz, Int32 *pResult)
{
    Int64 v;
    DrError err = DrStringToSignedOrUnsignedInt64(psz, (UInt64 *)(void *)&v, true);
    if (err != DrError_OK) {
        return err;
    }
    Int32 v32 = (Int32)v;
    if (v != (Int64)v32) {
        return DrError_InvalidParameter;
    }
    *pResult = v32;
    return DrError_OK;
}

DrError DrStringToUInt(const WCHAR *psz, unsigned int *pResult)
{
    UInt64 v;
    DrError err = DrStringToSignedOrUnsignedInt64(psz, &v, false);
    if (err != DrError_OK) {
        return err;
    }
    unsigned int v32 = (unsigned int)v;
    if (v != (UInt64)v32) {
        return DrError_InvalidParameter;
    }
    *pResult = v32;
    return DrError_OK;
}

DrError DrStringToInt(const WCHAR *psz, int *pResult)
{
    Int64 v;
    DrError err = DrStringToSignedOrUnsignedInt64(psz, (UInt64 *)(void *)&v, true);
    if (err != DrError_OK) {
        return err;
    }
    int v32 = (int)v;
    if (v != (Int64)v32) {
        return DrError_InvalidParameter;
    }
    *pResult = v32;
    return DrError_OK;
}

DrError DrStringToDouble(const WCHAR *psz, double *pResult)
{
    double v = 0;
    int exponent = 0;
    bool neg = false;
    bool gotDig = false;
    bool gotPoint = false;
    
    while (ISWSPACE(*psz)) {
        psz++;
    }

    if (*psz == L'+') {
        psz++;
    } else if (*psz == L'-') {
        neg = true;
        psz++;
    }

    while (*psz != L'\0' && !ISWSPACE(*psz) && *psz != L'e' && *psz != L'E') {
        int dig = -1;
        if (!gotPoint && *psz == L'.') {
            gotPoint = true;
            psz++;
            continue;
        } else if (*psz >= L'0' && *psz <= L'9') {
            dig = *psz - L'0';
        }
        if (dig < 0 || dig >= 10) {
            return DrError_InvalidParameter;
        }
        v = v * 10.0 + dig;
        if (gotPoint) {
            exponent--;
        }
        gotDig = true;
        psz++;
    }

    if (!gotDig) {
        return DrError_InvalidParameter;
    }

    if (*psz == L'e' || *psz == L'E') {
        psz++;
        Int32 exp2;
        DrError err = DrStringToInt32(psz, &exp2);
        if (err != DrError_OK) {
            return err;
        }
        exponent += exp2;
    } else {
        while (ISWSPACE(*psz)) {
            psz++;
        }

        if (*psz != L'\0') {
            return DrError_InvalidParameter;
        }
    }

    if (exponent != 0) {
        v = v * pow((double) 10, exponent);
    }

    if (neg) {
        v= -v;
    }

    *pResult = v;
    return DrError_OK;
}

DrError DrStringToBool(const WCHAR *psz, bool *pResult)
{
    bool ret = false;
    WCHAR tmp[16];
    size_t length = 0;

    while (ISWSPACE(*psz)) {
        psz++;
    }

    while (length < 15 && *psz != L'\0' && !ISWSPACE(*psz)) {
        tmp[length++] = *(psz++);
    }

    tmp[length] = L'\0';
    
    while (ISWSPACE(*psz)) {
        psz++;
    }

    if (*psz != L'\0') {
        return DrError_InvalidParameter;
    }

    _wcslwr(tmp);

    length++;
    if (wcsncmp(tmp, L"true", length) == 0 ||
         wcsncmp(tmp, L"yes", length) == 0 ||
         wcsncmp(tmp, L"on", length) == 0 ||
         wcsncmp(tmp, L"1", length) == 0) {
        ret = true;
    } else if (
         wcsncmp(tmp, L"false", length) == 0 ||
         wcsncmp(tmp, L"no", length) == 0 ||
         wcsncmp(tmp, L"off", length) == 0 ||
         wcsncmp(tmp, L"0", length) == 0) {
        ret = false;
    } else {
        return DrError_InvalidParameter;
    }

    *pResult = ret;
    return DrError_OK;
}




void DrStrList::GrowTo(UInt32 nAlloced)
{
    if (nAlloced > m_numAllocedStrings) {
        if (nAlloced < 32) {
            nAlloced = 32;
        }
        if (nAlloced < 2 * m_numAllocedStrings) {
            nAlloced = 2 * m_numAllocedStrings;
        }
        DrStr **ppNew = new DrStr*[nAlloced];
        LogAssert(ppNew != NULL);
        if (m_numStrings > 0) {
            memcpy(ppNew, m_prgpStrings, sizeof(DrStr *) * m_numStrings);
        }
        memset(ppNew + m_numStrings, 0, sizeof(DrStr *) * (nAlloced - m_numStrings));
        if (m_prgpStrings != NULL) {
            delete[] m_prgpStrings;
        }
        m_prgpStrings = ppNew;
        m_numAllocedStrings = nAlloced;
    }
}
#endif // if 0
