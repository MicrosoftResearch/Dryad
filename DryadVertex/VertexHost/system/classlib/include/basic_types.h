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

#pragma warning (disable: 4619) // #pragma warning : there is no warning number 'NNNN'

#pragma warning (push)

//
// Disabled warnings
//

#pragma warning (disable: 4255)
#pragma warning (disable: 4668) // XXX is not defined as preprocessor macro, replacing with 0
#pragma warning (disable: 4820) // 'N' bytes padding added after data member 'XXX'
#pragma warning (disable: 4365) // conversion from 'LONG64' to 'DWORD64', signed/unsigned mismat
#pragma warning (disable: 4548) // malloc.h(245) & STL: expression before comma has no effect; expected expression with side-effect
#pragma warning (disable: 4995) // deprecated functions

#include <Winsock2.h>
#include <mswsock.h>
#include <windows.h>
#include <limits.h>
#include <malloc.h>
// include for DNS hostname support
//#include <WinDNS.h>

#pragma warning (pop)

#if defined (ELEMENTCOUNT)
#undef ELEMENTCOUNT
#endif
#define ELEMENTCOUNT(x) (sizeof(x)/sizeof(x[0]))

typedef __int8 Int8;
typedef unsigned __int8 UInt8;
typedef __int16 Int16;
typedef unsigned __int16 UInt16;
typedef __int32 Int32;
typedef unsigned __int32 UInt32;
typedef __int64 Int64;
typedef unsigned __int64 UInt64;
typedef size_t Size_t;

#ifdef _M_IX86
typedef              int  xint;
typedef unsigned     int uxint;
#else
typedef          __int64  xint;
typedef unsigned __int64 uxint;
#endif

#define MAX_UINT8  ((UInt8)-1)
#define MAX_UINT16 ((UInt16)-1)
#define MAX_UINT32 ((UInt32)-1)
#define MIN_INT32  ((Int32)0x80000000)
#define MAX_INT32  ((Int32)0x7FFFFFFF)      // 2147483647
#define MAX_UINT64 ((UInt64)-1)
#define MAX_INT64  0x7FFFFFFFFFFFFFFFi64
#define MAX_FLOAT  (3.402823466e+38F)
#define MAX_SIZE_T ((size_t)((size_t)0 - (size_t)1))

#define PF_I64D "%I64d"
#define PF_I64X "%016I64x"
#define PF_I64O "%022I64o"

// A structure to represent a fixed-size chunk of
// memory
struct SIZED_STRING
{
    union
    {
        const UInt8 *pbData;
        const char  *pcData;
    };
    size_t cbData;
};

// A helper macro for defining a SIZED_STRING as part of a constant
#define INLINE_SIZED_STRING(str) { (const UInt8 *)str, sizeof(str) - 1 }

//  Structure to store a wchar_t version of a user dictionary word in memory
struct WCHAR_SIZED_STRING
{
    wchar_t* pData;
    size_t cchData;
};

// A helper macro for defining a SIZED_STRING as part of a constant
#define INLINE_WCHAR_SIZED_STRING(wstr) { wstr, sizeof(wstr)/sizeof(wstr[0]) - 1 }

//  Structure to store a wchar_t version of a user dictionary word in memory
struct WCHAR_SIZED_STRING_CONST
{
    const wchar_t* pData;
    size_t cchData;
};

// A utility class for creating temporary SIZED_STRINGs
class CStackSizedString : public SIZED_STRING
{
public:
    // Null-terminated input
    CStackSizedString(const char *szValue)
    {
        pbData = (const UInt8 *)szValue;
        cbData = strlen(szValue);
    }

    // Name/size pair
    CStackSizedString(
        const UInt8 *pbValue,
        size_t cbValue)
    {
        pbData = pbValue;
        cbData = cbValue;
    }

    // Name/size pair
    CStackSizedString(
        const char *pcValue,
        size_t cbValue)
    {
        pcData = pcValue;
        cbData = cbValue;
    }

private:
    // prevent heap allocation
    void *operator new(size_t);
};

// A utility class for creating temporary WCHAR_SIZED_STRINGs
class CStackSizedWString : public WCHAR_SIZED_STRING_CONST
{
public:
    // Null-terminated input
    CStackSizedWString(const wchar_t *wzValue)
    {
        pData = wzValue;
        cchData = wcslen(wzValue);
    }

    // Name/size pair
    CStackSizedWString(
        const wchar_t *wzValue,
        size_t cchValue)
    {
        pData = wzValue;
        cchData = cchValue;
    }

private:
    // prevent heap allocation
    void *operator new(size_t);
};

//This is an interface to disallow heap construction.
class INoHeapInstance
{
private:
    void* operator new(size_t);
};

// DDWORD is used to easily access a 64bit number as both a 64bit
//   and as two 32bit numbers
typedef union
{
    struct
    {
        DWORD    low;
        DWORD    high;
    } dw;
    DWORD64    ddw;
} DDWORD;

// critical section wrapper
//
class CRITSEC
{
private:
    CRITICAL_SECTION m_critsec;

public:
    CRITSEC()
    {
        InitializeCriticalSection(&m_critsec);
    }

    ~CRITSEC()
    {
        DeleteCriticalSection(&m_critsec);
    }

    void    Enter()
    {
        EnterCriticalSection(&m_critsec);
    }

    BOOL TryEnter()
    {
        return TryEnterCriticalSection(&m_critsec);
    }

    void    Leave()
    {
        LeaveCriticalSection(&m_critsec);
    }

    DWORD SetSpinCount(DWORD spinCount = 4000)
    {
        return SetCriticalSectionSpinCount(&m_critsec, spinCount);
    }
};

//Smart wrapper around CRITSEC so that we dont need to call enter/leave
class AutoCriticalSection : public INoHeapInstance
{
private:
    CRITSEC* m_pCritSec;
public:
    AutoCriticalSection(CRITSEC* pCritSec) : m_pCritSec(pCritSec)
    {
        this->m_pCritSec->Enter();
    }
    ~AutoCriticalSection()
    {
        this->m_pCritSec->Leave();
    }
};

///////////////////////////////////
#pragma warning(push)
#pragma warning (disable: 4201) // nonstandard extension used : nameless struct/union

typedef union tagFAInt64
{
    UInt64  n64;
    LONG64  i64;
    UInt8   nBytes[8];
    FILETIME  ft;

    struct
    {
        UInt32 nData;
        UInt32 nCount;
    };
    struct
    {
        Int32  i32_low;
        Int32  i32_hi;
    };
    struct
    {
        UInt32  n32_low;
        UInt32  n32_hi;
    };

    tagFAInt64():n64(0){};
    tagFAInt64( UInt64 nVal ):n64(nVal){};
    tagFAInt64( Int64  iVal ):i64(iVal){};
    tagFAInt64( UInt32 nHigh, UInt32 nLow ): n32_low(nLow), n32_hi(nHigh){};
    tagFAInt64( Int32  iHigh, Int32  iLow ): i32_low(iLow), i32_hi(iHigh){};
    tagFAInt64( DWORD  nHigh, DWORD  nLow ): n32_low(nLow), n32_hi(nHigh){};
    tagFAInt64( const FILETIME&  rftSrc ): ft(rftSrc){};

} FAInt64;  // flexible access 64 bit integer

typedef union tagFAInt32
{
    UInt32  n32;
    Int32   i32;
    UInt8   nBytes[4];
    struct
    {
        Int16  i16_low;
        Int16  i16_hi;
    };
    struct
    {
        UInt16  n16_low;
        UInt16  n16_hi;
    };
    tagFAInt32():n32(0){};
    tagFAInt32( UInt32 nVal ): n32(nVal){};
    tagFAInt32( Int32  iVal ): i32(iVal){};

} FAInt32;  // flexible access 32 bit integer

#define NUMELEM(p)  (sizeof(p)/sizeof((p)[0]))

#pragma warning(pop)

//
// disabling a couple of warnings that show up with /Wall and are pretty much useless -- they come mostly from macros
//
#pragma warning (disable: 4514) // unreferenced inline function has been removed
#pragma warning (disable: 4820) // 'N' bytes padding added after data member 'XXX'
#pragma warning (disable: 4265) // class has virtual functions, but destructor is not virtual
#pragma warning (disable: 4668) // XXX is not defined as preprocessor macro, replacing with 0
#pragma warning (disable: 4711) // function XXX selected for automatic inline expansion
#pragma warning (disable: 4548) // malloc.h(245) & STL: expression before comma has no effect; expected expression with side-effect
#pragma warning (disable: 4127) // conditional expression is constant
