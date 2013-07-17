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

#include <objbase.h>
#include "DrHash.h"

#pragma once

class DrStr;
//JC class DrWStr;

extern const GUID g_DrInvalidGuid;
extern const GUID g_DrNullGuid;

#define DrInvalidGuid ((const DrGuid &)g_DrInvalidGuid)
#define DrNullGuid ((const DrGuid &)g_DrNullGuid)

// A class that encapsulates guids used within Dryad
//
// NOTE: This class has no constructor, so that it can be used in unions. Use
// DrInitializedGuid if you want the constructor version.
//
// This class must add no new member state since we cast from arbitrary GUIDs to this class.
class DrGuid : public GUID
{
public:
    DrGuid& Set(const GUID& other)
    {
        *(GUID *)this = other;
        return *this;
    }

    DrGuid& operator=(const GUID& other)
    {
        return Set(other);
    }

    bool operator==(const GUID& other) const
    {
        return memcmp(this, &other, sizeof(DrGuid)) == 0;
    }

    bool operator!=(const GUID& other) const
    {
        return memcmp(this, &other, sizeof(DrGuid)) != 0;
    }

    bool operator>(const DrGuid& other) const
    {
        const unsigned int *pCur = (const unsigned int*) this;
        const unsigned int *pOther = (const unsigned int*) &other;

        for(int i = 0; i < sizeof(DrGuid)/sizeof(int); i++) {
            if(pCur[i] > pOther[i])
                return true;
            else if(pCur[i] < pOther[i])
                return false;
        }

        return false;  // They are equal
    }

    // Return a 32-bit hash for this guid
    DWORD Hash() const
    {
        return DrHash32::Guid( this );
    }

    // Store the invalid guid as our guid
    void Invalidate()
    {
        Set( g_DrInvalidGuid );
    }

    // Returns whether this is a valid guid
    bool IsValid() const
    {
        return (*this != g_DrInvalidGuid) && !IsNull();
    }

    // Store the null guid as our guid
    void SetToNull()
    {
        Set( g_DrNullGuid );
    }

    // Returns whether this is a valid guid
    bool IsNull() const
    {
        return *this == g_DrNullGuid;
    }

    // Generate a guid
    void Generate();

    // Parse a guid from a string. Acceptes guids either with or without braces
    BOOL Parse(const char *string);

    // Parses guid from a string, return NULL on failure or pointer to next char after GUID in given string on success
    // allowBraces - string may contain optional braces 
    // requireBraces - braces are required (allowBraces should be true)
    // requireEOL - string should contain nothing after GUID (i.e. zero terminator should be present right after GUID)
    const char* Parse(const char *string, bool allowBraces, bool requireBraces, bool requireEOL);
    
    const static size_t GuidStringLength = 39; //38 + null terminator

    // Output the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
    // is false, the braces are omitted.
    // String must be able to hold DrGuid::GuidStringLength (39 characters = 38 + null terminator)
    char *ToString(char *string, bool fBraces=true) const;

    // appends the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
    // is false, the braces are omitted.
    DrStr& AppendToString(DrStr& strOut, bool fBraces=true) const;

    // appends the guid in string form; {EFF6744C-7143-11cf-A51B-080036F12502}. If fBraces
    // is false, the braces are omitted.
//JC    DrWStr& AppendToString(DrWStr& strOut, bool fBraces=true) const;

protected:
    // Helper function to convert a byte into 2 hex digits
    static void ByteToHex(BYTE input, char *output);
};

class DrInitializedGuid : public DrGuid
{
public:
    DrInitializedGuid()
    {
        Set(g_DrInvalidGuid);
    }

    DrInitializedGuid(const GUID& other)
    {
        Set(other);
    }
};

