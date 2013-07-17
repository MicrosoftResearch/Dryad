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

#define NULLSTR ((char *)NULL)

// This macro can be used to obtain a temporary "const char *" error description for an error. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
#define DRERRORSTRING(err)    (DrErrorString(err).GetString())

// This macro can be used to obtain a temporary "const char *" UTF-8 string equivalent for a unicode string. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
#define DRWSTRINGTOUTF8(s)    (DrStr64(DrStr((const WCHAR *)(s))).GetString())

// This macro can be used to obtain a temporary "const char *" description for a status value. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
// succeeded string: OK
// failed string: FAILED 80001234 'Error text'
#define DR_STATUS_STRING(status)    (DrStatusString(status).GetString())

// This macro can be used to obtain a temporary "const char *" string equivalent for a GUID. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
#define DRGUIDSTRING(guid)    (DrGuidString(guid, false).GetString())

__inline bool ISSPACE(char c)
{
    return isspace((unsigned char)c) != 0;
}

static const Size_t DrStr_InvalidIndex = Max_Size_t;

// Simple growable string classes
class DrStr
{
public:
    DrStr()
    {
        InitZero();
    }

    DrStr(const DrStr& other)
    {
        InitZero();
        Set(other);
    }

    DrStr(const char *psz)
    {
        InitZero();
        Set(psz);
    }

    DrStr(const char *psz, size_t len)
    {
        InitZero();
        Set(psz, len);
    }

    DrStr(const WCHAR *psz)
    {
        InitZero();
        Set(psz);
    }

    DrStr(const WCHAR *psz, size_t len)
    {
        InitZero();
        Set(psz, len);
    }
/*JC
    DrStr(const WCHAR *psz, size_t len, UINT codePage)
    {
        InitZero();
        Set(psz, len, codePage);
    }

    DrStr(const DrWStr& other);
*/

    virtual ~DrStr()
    {
        DiscardString();
    }

    // may be NULL
    const char *GetString() const
    {
        return m_pBuffer;
    }

    size_t GetLength() const
    {
        return m_stringLen;
    }

    size_t GetBufferLength() const
    {
        return m_nbBuffer;
    }

    operator const char *() const
    {
        return m_pBuffer;
    }

    DrStr& Set(const char *psz, size_t len)
    {
        if (psz == NULL) {
            DiscardString();
        } else {
            GrowTo(len);
            memcpy(m_pBuffer, psz, len);
            m_pBuffer[len] = '\0';
            m_stringLen = len;
        }
        return *this;
    }

    DrStr& Set(const char *psz)
    {
        size_t len = 0;
        if (psz != NULL) {
            len = strlen(psz);
        }
        return Set(psz, len);
    }

    DrStr& Set(const DrStr& other)
    {
        return Set(other.m_pBuffer, other.m_stringLen);
    }

    // Converts from unicode
    DrStr& Set(const WCHAR *psz, size_t len, UINT codePage)
    {
        if (psz == NULL) {
            DiscardString();
        } else {
            SetToEmptyString();
            Append(psz, len, codePage);
        }
        return *this;
    }

    // Converts from unicode to UTF8
    DrStr& Set(const WCHAR *psz, size_t len)
    {
        return Set(psz, len, CP_UTF8);
    }

    DrStr& Set(const WCHAR *psz)
    {
        size_t len = 0;
        if (psz != NULL) {
            len = wcslen(psz);
        }
        return Set(psz, len);
    }

    DrStr& VSetF(const char *pszFormat, va_list args)
    {
        SetToEmptyString();
        return VAppendF(pszFormat, args);
    }

#pragma warning (push)
#pragma warning (disable: 4793) // function compiled as native
    DrStr& SetF(const char *pszFormat, ...)
    {
        va_list args;
        va_start(args, pszFormat);

        return VSetF(pszFormat, args);
    }
#pragma warning (pop)

    //
    // Set string value to environment variable value
    //
    DrError SetFromEnvironmentVariable(const char *pszVarName)
    {
        SetToEmptyString(); 
        return AppendFromEnvironmentVariable(pszVarName);
    }

    DrStr& operator=(const char *psz)
    {
        return Set(psz);
    }

    DrStr& operator=(const DrStr& other)
    {
        return Set(other);
    }

    // Returns a writable buffer of >= 0 chars, but without growing if not necessary
    // The max chars to write are returned in *pLenOut
    char *GetAnyWritableBuffer(size_t *pLenOut, size_t offset = 0)
    {
        GrowTo(offset);
        *pLenOut = m_nbBuffer - offset - 1;
        return m_pBuffer + offset;
    }

    DrStr& Append(const char *psz, size_t len)
    {
        if (psz != NULL) {
            size_t oldLen = m_stringLen;
            GrowTo(oldLen + len);
            memcpy(m_pBuffer + oldLen, psz, len);
            m_stringLen = oldLen + len;
            m_pBuffer[m_stringLen] = '\0';
        }
        return *this;
    }

    DrStr& Append(const char *psz)
    {
        size_t len = 0;
        if (psz != NULL) {
            len = strlen(psz);
        }
        return Append(psz, len);
    }

    DrStr& Append(const DrStr& other)
    {
        return Append(other.m_pBuffer, other.m_stringLen);
    }

    DrStr& Append(char c)
    {
        GrowTo(m_stringLen+1);
        m_pBuffer[m_stringLen++] = c;
        m_pBuffer[m_stringLen] = '\0';
        return *this;
    }

    DrStr& Append(const WCHAR *psz, size_t len, UINT codePage);

    DrStr& Append(const WCHAR *psz, size_t len)
    {
        return Append(psz, len, CP_UTF8);
    }

    DrStr& Append(const WCHAR *psz)
    {
        size_t len = 0;
        if (psz != NULL) {
            len = wcslen(psz);
        }
        return Append(psz, len);
    }

    DrStr& VAppendF(const char *pszFormat, va_list args);

#pragma warning (push)
#pragma warning (disable: 4793) // function compiled as native
    DrStr& AppendF(const char *pszFormat, ...)
    {
        va_list args;
        va_start(args, pszFormat);

        return VAppendF(pszFormat, args);
    }
#pragma warning (pop)

    DrStr& AppendErrorString(DrError val)
    {
        return DrAppendErrorDescription(*this, val);
    }

    DrStr& AppendXmlEncodedString(const char *pszUnencoded, Size_t len, bool fEscapeNewlines=false);
    
    DrStr& AppendXmlEncodedString(const DrStr& strUnencoded, bool fEscapeNewlines=false)
    {
        return AppendXmlEncodedString(strUnencoded.GetString(), strUnencoded.GetLength(), fEscapeNewlines);
    }

    DrStr& AppendXmlEncodedString(const char *pszUnencoded, bool fEscapeNewlines=false)
    {
        return AppendXmlEncodedString(pszUnencoded, pszUnencoded == NULL ? (Size_t)0 : strlen(pszUnencoded), fEscapeNewlines);
    }

    DrStr& SetErrorString(DrError val)
    {
        SetToEmptyString(); return AppendErrorString(val);
    }

    DrStr& operator+=(const char *psz)
    {
        return Append(psz);
    }

    DrStr& operator+=(const DrStr& other)
    {
        return Append(other);
    }

    DrStr& operator+=(char c)
    {
        return Append(c);
    }

    // Asserts if not inside the string. You cannot fetch the null terminator with this
    // The return value is an lvalue, so it can be set as well as fetched
    char& operator[](size_t index)
    {
        LogAssert(m_pBuffer != NULL && index <= m_stringLen);
        return m_pBuffer[index];
    }

    char& operator[](int index)
    {
        LogAssert(m_pBuffer != NULL && index >= 0 && (size_t)index <= m_stringLen);
        return m_pBuffer[index];
    }

    char operator[](size_t index) const
    {
        LogAssert(m_pBuffer != NULL && index <= m_stringLen);
        return m_pBuffer[index];
    }

    char operator[](int index) const
    {
        LogAssert(m_pBuffer != NULL && index >= 0 && (size_t)index <= m_stringLen);
        return m_pBuffer[index];
    }

    // Uses _strlwr (not multibyte aware)
    // leaves NULL strings alone
    DrStr& ToLowerCase();

    // Uses _strupr (not multibyte aware)
    // leaves NULL strings alone
    DrStr& ToUpperCase();

    bool IsEqual(const char *pszOther, size_t len) const
    {
        if (len != m_stringLen) {
            return false;
        }
        if (pszOther == NULL || m_pBuffer == NULL) {
            return (m_pBuffer == pszOther);
        }
        if (len == 0) {
            return true;
        }
        return (memcmp(m_pBuffer, pszOther, len) == 0);
    }

    bool IsEqual(const DrStr& other) const
    {
        return IsEqual(other.GetString(), other.GetLength());
    }

    bool IsEqual(const char *pszOther) const
    {
        return IsEqual(pszOther, (pszOther == NULL) ? (size_t)0 : strlen(pszOther));
    }

    bool IsNullOrEmpty( bool ignoreWhitespace = false )
    {
        if ( m_pBuffer == NULL )
            return true;
        const unsigned char* p = (const unsigned char *)m_pBuffer;
        if ( ignoreWhitespace )
            while ( isspace( *p ) )
                p++;
        return '\0' == *p;
    }

    bool operator==(const char *pszOther) const
    {
        return IsEqual(pszOther);
    }

    bool operator==(const DrStr& other) const
    {
        return IsEqual(other.m_pBuffer, other.m_stringLen);
    }


    bool operator!=(const char *pszOther) const
    {
        return !IsEqual(pszOther);
    }

    bool operator!=(const DrStr& other) const
    {
        return !IsEqual(other.m_pBuffer, other.m_stringLen);
    }

    bool SubstrIsEqual(size_t index, const char *pszMatch, size_t matchLen) const;

    bool SubstrIsEqual(size_t index, const char *pszMatch) const
    {
        return SubstrIsEqual(index, pszMatch, strlen(pszMatch));
    }

    bool StartsWith(const char *pszMatch, size_t len) const
    {
        return SubstrIsEqual(0, pszMatch, len);
    }

    bool StartsWith(const char *pszMatch) const
    {
        return StartsWith(pszMatch, strlen(pszMatch));
    }

    // Not multibyte aware
    bool SubstrIsEqualNoCase(size_t index, const char *pszMatch, size_t matchLen) const;

    // Not multibyte aware
    bool SubstrIsEqualNoCase(size_t index, const char *pszMatch) const
    {
        return SubstrIsEqualNoCase(index, pszMatch, strlen(pszMatch));
    }

    // Not multibyte aware
    bool StartsWithNoCase(const char *pszMatch, size_t len) const
    {
        return SubstrIsEqualNoCase(0, pszMatch, len);
    }

    // Not multibyte aware
    bool StartsWithNoCase(const char *pszMatch) const
    {
        return StartsWithNoCase(pszMatch, strlen(pszMatch));
    }

    // Returns DrStr_InvalidIndex if there is no match or the starting index is out of range
    // Returns the string length if the null terminator is matched
    // Uses strchr - not multibyte aware.
    size_t IndexOfChar(char c, size_t startIndex = 0) const;

    char *GetWritableBuffer(size_t maxStringLen, size_t offset = 0)
    {
        LogAssert(offset + maxStringLen >= offset); // overflow check
        GrowTo(offset + maxStringLen);
        return m_pBuffer + offset;
    }

    char *GetWritableAppendBuffer(size_t maxStringLen)
    {
        return GetWritableBuffer(maxStringLen, m_stringLen);
    }

    DrError AppendFromEnvironmentVariable(const char *pszVarName);

    void SetToNull()
    {
        DiscardString();
    }
    
    void SetToEmptyString()
    {
        GrowTo(0);
        m_stringLen = 0;
        m_pBuffer[0] = '\0';
    }

    void EnsureNotNull()
    {
        GrowTo(0);
    }

    void UpdateLength(size_t stringLen)
    {
        if (m_pBuffer == NULL) {
            LogAssert(stringLen == 0);
            LogAssert(m_stringLen == 0);
        } else {
            LogAssert(stringLen < m_nbBuffer);
            m_pBuffer[stringLen] = '\0';
            m_stringLen = stringLen;
        }
    }

protected:
    // Ensures that there are at least maxStringLength+1 bytes available (including null terminator) for the string.
    // Does not change the string length.
    // If the string was NULL, it becomes an empty string.
    void GrowTo(size_t maxStringLength);

    void DiscardString()
    {
        if (m_pBuffer != NULL && m_pBuffer != m_pStaticBuffer) {
            delete[] m_pBuffer;
        }
        m_pBuffer = NULL;
        m_nbBuffer = 0;
        m_stringLen = 0;        
    }

protected:
    DrStr(char *pStaticBuffer, size_t nbStatic, bool isFastStr)
    {
        m_pStaticBuffer = pStaticBuffer;
        m_nbStatic = nbStatic;
        m_pBuffer = NULL;
        m_nbBuffer = 0;
        m_stringLen = 0;
    }

    void SetToStaticBuffer()
    {
        m_pBuffer = m_pStaticBuffer;
        m_nbBuffer = m_nbStatic;
        m_stringLen = m_nbStatic;
    }

    virtual char* AllocateBiggerBuffer( size_t newSize ) {
        return new char[newSize];
    }

private:
    void InitZero()
    {
        m_pStaticBuffer = NULL;
        m_nbStatic = 0;
        m_pBuffer = NULL;
        m_nbBuffer = 0;
        m_stringLen = 0;
    }

private:
    char *m_pStaticBuffer; // stack-allocated buffer in subclass, or null
    size_t m_nbStatic;  // Number of bytes in the static buffer (including null terminator)
    char *m_pBuffer; // if equal to m_pStaticBuffer, then we are using the static buffer. Otherwise a heap block.
    size_t m_nbBuffer; // size of the buffer
    size_t m_stringLen; // Number of characters in the string, not including null terminator

};

// Variable size template for growable strings
template<size_t buffSize> class DrFastStr : public DrStr
{
public:
    DrFastStr() :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
    }

    DrFastStr(const DrStr& other) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(other);
    }

    // explicitly defined copy constructor is very important; without it, C++ generates a default one that doesn't work
    DrFastStr(const DrFastStr<buffSize>& other) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(other);
    }

    DrFastStr(const char *psz) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(psz);
    }

    DrFastStr(const char *psz, size_t len) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(psz, len);
    }

/*JC
    explicit DrFastStr(const WCHAR *psz) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(psz);
    }

    DrFastStr(const WCHAR *psz, size_t len) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(psz, len);
    }

    DrFastStr(const WCHAR *psz, size_t len, UINT codePage) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(psz, len, codePage);
    }

    DrFastStr(const DrWStr& other) :
        DrStr(m_szStaticBuffer, buffSize, true)
    {
        Set(other);
    }
*/

    DrStr& operator=(const char *psz)
    {
        return Set(psz);
    }

    DrStr& operator=(const DrStr& other)
    {
        return Set(other);
    }

    // explicitly provided copy assignment operator to make sure C++ doesn't try to outsmart us
    DrStr& operator=(const DrFastStr<buffSize>& other)
    {
        return Set(other);
    }


/*JC    DrStr& operator=(const DrWStr& other)
    {
        return Set(other);
    }
*/  

protected:
    char m_szStaticBuffer[buffSize];
};

typedef DrStr DrStr0;
typedef DrFastStr<4> DrStr4;
typedef DrFastStr<8> DrStr8;
typedef DrFastStr<16> DrStr16;
typedef DrFastStr<32> DrStr32;
typedef DrFastStr<64> DrStr64;
typedef DrFastStr<128> DrStr128;
typedef DrFastStr<256> DrStr256;
typedef DrFastStr<512> DrStr512;
typedef DrFastStr<1024> DrStr1024;
typedef DrFastStr<1024> DrStr1K;
typedef DrFastStr<2048> DrStr2K;
typedef DrFastStr<4096> DrStr4K;
typedef DrFastStr<8192> DrStr8K;
typedef DrFastStr<16384> DrStr16K;
typedef DrFastStr<32768> DrStr32K;
typedef DrFastStr<65536> DrStr64K;

class DrErrorString : public DrStr128
{
public:
    DrErrorString(DrError err)
    {
        SetErrorString(err);
    }
};

class DrGuidString : public DrStr64
{
public:
    DrGuidString( const DrGuid& guid, bool fBraces )
    {
        guid.AppendToString( *this, fBraces);
    }

    DrGuidString( const GUID& guid, bool fBraces )
    {
        DrGuid g;
        g.Set(guid);
        g.AppendToString( *this, fBraces );
    }
}; 

class DrStatusString : public DrStr128
{
public:
    DrStatusString(DrError status)
    {
        if ( SUCCEEDED( status ) )
        {
            if ( status == DrError_OK )
            {
                Append( "OK" );
            }
            else
            {
                AppendF( "OK %08x", status );
            }
        }
        else
        {
            PCSTR s;
            if (status == DrErrorFromWin32(ERROR_IO_PENDING))
            {
                s = "IN_PROGRESS";
            }
            else if (status == DrError_AlreadyCompleted)
            {
                s = "ASYNC_COMPLETION";
            }
            else
            {
                s = "FAILED";
            }
                
            AppendF("%s %08x \'", s, status);
            AppendErrorString( status );
            Append( '\'' );
        }
    }
};

class DrTempStringPool
{
private:

    // TempStringBlock allows us to maintain a stack of temporarily allocated
    // return strings that is cleaned up when the stack is destroyed.
    class TempStringBlock
    {
    private:
        TempStringBlock *pNext;
        BYTE *pData;
        Size_t length;

        // We override "new" to allocate the header and the content
        // in a single allocation.
        inline void *operator new(Size_t headersize, Size_t blocksize)
        {
            LogAssert(headersize == sizeof(TempStringBlock));
            LogAssert(headersize + blocksize >= headersize);  // keep prefast happy
            void *p = malloc(headersize + blocksize);
            LogAssert(p != NULL);
            return p;
        }

        inline TempStringBlock(Size_t blocksize,  TempStringBlock *pOldHead)
        {
            pData = ((BYTE *)(void *)this) + sizeof(*this);
            length = blocksize;
            pNext = pOldHead;
        }

    public:
        // We have to provide a matching delete...
        inline void operator delete(void *pMem, Size_t  blocksize)
        {
            (void)blocksize;
            free(pMem);
        }

        inline static TempStringBlock *Alloc(Size_t blocksize, TempStringBlock  *pOldHead)
        {
            TempStringBlock *p = new(blocksize) TempStringBlock(blocksize, pOldHead);
            return p;
        }

        inline TempStringBlock *Detach()
        {
            TempStringBlock *p = pNext;
            pNext = NULL;
            return p;
        }

        inline ~TempStringBlock()
        {
            LogAssert(pNext == NULL);
        }

        inline BYTE *GetData()
        {
            return pData;
        }

        inline Size_t GetLength()
        {
            return length;
        }

        inline BYTE *ReserveData(Size_t len)
        {
            LogAssert(len <= length);
            BYTE *pd = pData;
            pData += len;
            length -= len;
            return pd;
        }

    };

public:
    DrTempStringPool(Size_t growSize = 8192)
    {
        m_growSize = growSize;
        m_pHead = NULL;
    }

    void discardAll()
    {
        while (m_pHead != NULL) {
            TempStringBlock *p = m_pHead;
            m_pHead = m_pHead->Detach();
            delete p;
        }
    }

    ~DrTempStringPool()
    {
        discardAll();
    }

    void setTempStringGrowSize(Size_t growSize)
    {
        m_growSize = growSize;
    }

    const char *dupStr(const char *pszStr)
    {
        return dupStr( pszStr, pszStr ? strlen(pszStr) : 0 );
    }

    const char *dupStr(const char *pszStr, size_t len )
    {
        if (pszStr == NULL) {
            return NULL;
        }
        char *pszOut = (char *)allocMem(len+1);
        memcpy(pszOut, pszStr, len+1);
        return pszOut;
    }

    // Not multibyte aware
    const char *dupStrLowerCase(const char *pszStr, size_t* pLength = NULL )
    {
        if (pszStr == NULL) {
            return NULL;
        }
        Size_t len = strlen(pszStr);
        if ( pLength != NULL )
            *pLength = len;
        char *pszOut = (char *)allocMem(len+1);
        memcpy(pszOut, pszStr, len+1);
        _strlwr(pszOut);
        return pszOut;
    }

    const char *dupStrUpperCase(const char *pszStr, size_t* pLength = NULL)
    {
        if (pszStr == NULL) {
            return NULL;
        }
        Size_t len = strlen(pszStr);
        if ( pLength != NULL )
            *pLength = len;
        char *pszOut = (char *)allocMem(len+1);
        memcpy(pszOut, pszStr, len+1);
        _strupr(pszOut);
        return pszOut;
    }

    const void *dupMem(const void *pMem, Size_t len)
    {
        if (pMem == NULL) {
            return NULL;
        }
        void *pOut = allocMem(len);
        memcpy(pOut, pMem, len);
        return pOut;
    }

    void *allocMem(Size_t len)
    {
        if (m_pHead == NULL || len > m_pHead->GetLength()) {
            Size_t n = m_growSize;
            if (n < len) {
                n = len;
            }
            while (m_growSize < n) {
                m_growSize = 2 * m_growSize;
            }
            m_pHead = TempStringBlock::Alloc(m_growSize, m_pHead);
        }
        return m_pHead->ReserveData(len);
    }

    inline const char *setString(const char *pszStr, bool fCopy = true)
    {
        if (fCopy) {
            pszStr = dupStr(pszStr);
        }

        return pszStr;
    }

    inline const void *setBlob(const void *pMem, Size_t len, bool fCopy = true)
    {
        if (fCopy) {
            pMem = dupMem(pMem, len);
        }

        return pMem;
    }

private:

    Size_t m_growSize;
    TempStringBlock *m_pHead;

};

// An internalized string pool manages reusable strings that can be located by their hash. Strings are put into the table, and references to the same string
// can be reused as often as desired, as long as the DrInternalizedStringPool is not deleted.
// There is no mechanism for deleting entries once they are added, so this class is only useful for string sets that don't grow unbounded (e.g., datacenter machine names,
// volume names, etc.)
//
// This class is threadsafe
class DrInternalizedStringPool : public DrTempStringPool
{
private:
    static const UInt32 k_internalizedStringMagic = (UInt32)'rtSi';

    class InternalizedStringHeader
    {
    public:
        UInt32 magic;
        UInt32 hash;
        InternalizedStringHeader *pNext;
    };

public:
    DrInternalizedStringPool(UInt32 hashTableSize = 20011, Size_t growSize = 65536) : DrTempStringPool(growSize)
    {
        m_hashTableSize = hashTableSize;
        m_pBuckets = new InternalizedStringHeader *[m_hashTableSize];
        LogAssert(m_pBuckets != NULL);
        memset(m_pBuckets, 0, m_hashTableSize * sizeof(InternalizedStringHeader *));
        m_Mutex = new MSMutex ();
    }

    ~DrInternalizedStringPool()
    {
        delete[] m_pBuckets;
        m_pBuckets = NULL;
        m_Mutex = NULL;
    }

    // Computes the hash of a string, and if pLength is supplied, its length as well
    static UInt32 StringHash(const char *pszString, /* out */ Size_t *pLength = NULL)
    {
        LogAssert(pszString != NULL);

        Size_t length = strlen( pszString );

        if (pLength != NULL) {
            *pLength = length;
        }

        return DrHash32::Compute( pszString, length );
    }

    // Returns a pointer to a duplicate of the given string that will be valid as long as this
    // pool is not deleted. It is guaranteed that the return value will be the same for any two identical strings.
    const char *InternalizeString(const char *pszString);

    // Same as InternalizeString, but normalizes the string to lower case
    // before internalizing.
    const char *InternalizeStringLowerCase(const char *pszString);

    // Same as InternalizeString, but normalizes the string to upper case
    // before internalizing.
    const char *InternalizeStringUpperCase(const char *pszString);

    // Verifies that a string is internalized. This should only be used for Assertions, since it is dangerous to do for non-internalized strings.
    static bool IsInternalized(const char *pszString)
    {
        return GetHeaderOfInternalizedString(pszString)->magic == k_internalizedStringMagic;
    }

    static UInt32 HashOfInternalizedString(const char *pszInternalizedString)
    {
        LogAssert(pszInternalizedString != NULL);
        const InternalizedStringHeader *pHeader = GetHeaderOfInternalizedString(pszInternalizedString);
        LogAssert(pHeader->magic == k_internalizedStringMagic);
        return pHeader->hash;
    }

protected:
    static const InternalizedStringHeader *GetHeaderOfInternalizedString(const char *pszInternalizedString)
    {
        return ((const InternalizedStringHeader *)(const void *)pszInternalizedString) - 1;
    }

private:
    Ptr<MSMutex> m_Mutex;
    InternalizedStringHeader **m_pBuckets;
    UInt32 m_hashTableSize;
};

extern DrInternalizedStringPool g_DrInternalizedStrings;

