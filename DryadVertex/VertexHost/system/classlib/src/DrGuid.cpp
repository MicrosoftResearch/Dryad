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

#include "DrCommon.h"
#include <objbase.h>


#pragma unmanaged

const GUID g_DrInvalidGuid = { 0xFFFFFFFF, 0xFFFF, 0xFFFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
const GUID g_DrNullGuid = { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };

// Generate a new guid
void DrGuid::Generate()
{
    HRESULT success = CoCreateGuid((GUID *)this);
    if (FAILED(success))
    {
        LogAssert("Fatal error, failed to create guid");
    }
}


static
const UInt8 *
GuidWriteWord (
    char        *pDst,
    const UInt8 *pSrc,
    uxint        uBytes
)
{
    static const char Hex[] = "0123456789ABCDEF";
    uxint  c;

    pDst += uBytes * 2;
    *pDst = '-';
    do
    {
        c = *pSrc++;
        pDst -= 2;
        pDst[1] = Hex[c & 15];
        pDst[0] = Hex[c >> 4];
    }
    while (--uBytes);

    return (pSrc);
}

static
void
GuidWrite (
    char *pDst,
    const GUID *pGuid,
    bool fBraces = true
)
{
    const UInt8 *p;

    p = (const UInt8 *) pGuid;

    if (fBraces) {
        *pDst++ = '{';
    }
    
    p = GuidWriteWord (pDst, p, 4);
    pDst += 9;

    p = GuidWriteWord (pDst, p, 2);
    pDst += 5;

    p = GuidWriteWord (pDst, p, 2);
    pDst += 5;

    GuidWriteWord (pDst, p, 1);
    GuidWriteWord (pDst + 2, p + 1, 1);
    pDst += 5;
    p += 2;

    GuidWriteWord (pDst + 0*2, p + 0, 1);
    GuidWriteWord (pDst + 1*2, p + 1, 1);
    GuidWriteWord (pDst + 2*2, p + 2, 1);
    GuidWriteWord (pDst + 3*2, p + 3, 1);
    GuidWriteWord (pDst + 4*2, p + 4, 1);
    GuidWriteWord (pDst + 5*2, p + 5, 1);

    pDst += 6*2;
    if (fBraces) {
        *pDst++ = '}';
    }
    *pDst = 0;
}

static
const UInt8 *
GuidWriteWord (
    WCHAR        *pDst,
    const UInt8 *pSrc,
    uxint        uBytes
)
{
    static const WCHAR Hex[] = L"0123456789ABCDEF";
    uxint  c;

    pDst += uBytes * 2;
    *pDst = L'-';
    do
    {
        c = *pSrc++;
        pDst -= 2;
        pDst[1] = Hex[c & 15];
        pDst[0] = Hex[c >> 4];
    }
    while (--uBytes);

    return (pSrc);
}

#pragma warning (disable: 4505) // unreferenced local function
static
void
GuidWrite (
    WCHAR *pDst,
    const GUID *pGuid,
    bool fBraces = true
)
{
    const UInt8 *p;

    p = (const UInt8 *) pGuid;

    if (fBraces) {
        *pDst++ = L'{';
    }
    
    p = GuidWriteWord (pDst, p, 4);
    pDst += 9;

    p = GuidWriteWord (pDst, p, 2);
    pDst += 5;

    p = GuidWriteWord (pDst, p, 2);
    pDst += 5;

    GuidWriteWord (pDst, p, 1);
    GuidWriteWord (pDst + 2, p + 1, 1);
    pDst += 5;
    p += 2;

    GuidWriteWord (pDst + 0*2, p + 0, 1);
    GuidWriteWord (pDst + 1*2, p + 1, 1);
    GuidWriteWord (pDst + 2*2, p + 2, 1);
    GuidWriteWord (pDst + 3*2, p + 3, 1);
    GuidWriteWord (pDst + 4*2, p + 4, 1);
    GuidWriteWord (pDst + 5*2, p + 5, 1);

    pDst += 6*2;
    if (fBraces) {
        *pDst++ = L'}';
    }
    *pDst = 0;
}

static
bool
GuidReadWord (
    const char *pSrc,
    UInt8 *pDst,
    uxint  uBytes
)
{
    uxint c0, c1;

    pDst += uBytes;
    do
    {
        c0 = (uxint) *pSrc++;
        if (c0 >= '0' && c0 <= '9')
            c0 -= '0';
        else if (c0 >= 'A' && c0 <= 'F')
            c0 -= 'A' - 10;
        else if (c0 >= 'a' && c0 <= 'f')
            c0 -= 'a' - 10;
        else
            return (false);

        c1 = (uxint) *pSrc++;
        if (c1 >= '0' && c1 <= '9')
            c1 -= '0';
        else if (c1 >= 'A' && c1 <= 'F')
            c1 -= 'A' - 10;
        else if (c1 >= 'a' && c1 <= 'f')
            c1 -= 'a' - 10;
        else
            return (false);

        *--pDst = (UInt8) ((c0 << 4) + c1);
    }
    while (--uBytes);

    return (true);
}


static const char* GuidRead(GUID *pGuid, const char *pSrc, bool allowBraces, bool requireBraces, bool requireEOL)
{
    UInt8 *p;
    bool closingBrace;

    if (requireBraces && !allowBraces)
    {
        goto failed;
    }
    
    if (pSrc == NULL) 
    {
        goto failed;
    }
    
    p = (UInt8 *) pGuid;

    closingBrace = false;
    if (allowBraces)
    {
        if (*pSrc == '{')
        {
            pSrc++;
            closingBrace = true;
        }
        else if (requireBraces)
        {
            goto failed;
        }
    }

    if (!GuidReadWord (pSrc, p, 4) || pSrc[8] != '-')
    {
        goto failed;
    }
    pSrc += 9;
    p += 4;

    if (!GuidReadWord (pSrc, p, 2) || pSrc[4] != '-')
    {
        goto failed;
    }
    pSrc += 5;
    p += 2;

    if (!GuidReadWord (pSrc, p, 2) || pSrc[4] != '-')
    {
        goto failed;
    }
    pSrc += 5;
    p += 2;

    if (!GuidReadWord (pSrc, p, 1) || !GuidReadWord (pSrc + 2, p + 1, 1) || pSrc[4] != '-')
    {
        goto failed;
    }
    pSrc += 5;
    p += 2;

    if (!GuidReadWord (pSrc + 0*2, p + 0, 1) ||
         !GuidReadWord (pSrc + 1*2, p + 1, 1) ||
         !GuidReadWord (pSrc + 2*2, p + 2, 1) ||
         !GuidReadWord (pSrc + 3*2, p + 3, 1) ||
         !GuidReadWord (pSrc + 4*2, p + 4, 1) ||
         !GuidReadWord (pSrc + 5*2, p + 5, 1)) 
    {
        goto failed;
    }

    pSrc += 6*2;

    if (closingBrace)
    {
        if (*pSrc != '}')
        {
            goto failed;
        }
        ++pSrc;
    }

    if (requireEOL && *pSrc != 0)
    {
        goto failed;
    }
    
    return pSrc;

failed:
    FillMemory(pGuid, sizeof(GUID), 0xFF);
    return NULL;
}

// appends the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
// is false, the braces are omitted.
DrStr& DrGuid::AppendToString(DrStr& strOut, bool fBraces) const
{
    size_t len = strOut.GetLength();
    char *pDest = strOut.GetWritableAppendBuffer(GuidStringLength-1);
    GuidWrite(pDest, this, fBraces);
    strOut.UpdateLength(len + strlen(pDest));
    return strOut;
}

/* JC
// appends the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
// is false, the braces are omitted.
DrWStr& DrGuid::AppendToString(DrWStr& strOut, bool fBraces) const
{
    size_t len = strOut.GetLength();
    WCHAR *pDest = strOut.GetWritableAppendBuffer(GuidStringLength-1);
    GuidWrite(pDest, this, fBraces);
    strOut.UpdateLength(len + wcslen(pDest));
    return strOut;
}
*/

    // Output the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
    // is false, the braces are omitted.
    // String must be able to hold DrGuid::GuidStringLength (39 characters = 38 + null terminator)
char *DrGuid::ToString (char *string, bool fBraces) const
{
    GuidWrite (string, this, fBraces);
    return (string);
}


// Parse a guid from a string. Acceptes guids either with or without braces
// EFF6744C-7143-11cf-A51B-080036F12502
BOOL DrGuid::Parse(const char *string)
{
    return GuidRead(this, string, true, false, true) != NULL;
}

//returns pointer to the next char after guid on success, NULL on failure
const char* DrGuid::Parse(const char *string, bool allowBraces, bool requireBraces, bool requireEOL)
{
    return GuidRead(this, string, allowBraces, requireBraces, requireEOL);
}
    
