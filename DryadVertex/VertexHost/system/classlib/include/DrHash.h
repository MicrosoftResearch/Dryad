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

//
// Fast, thorough hash functions returning 32 or 64 bit results.
//
// You can use hash table sizes that are a power of 2, and use & to 
// trim values, for example:
//   LOGSIZE=10;
//   Buckets *hash_table[(1<<LOGSIZE)];
//   bucket = hash_table[ hash(key) & ((1<<LOGSIZE)-1) ];
// It doesn't matter which bits of the hash you use, they are all well
// mixed.  It doesn't matter what patterns there are in your keys, the
// hash does equally well on all key formats.
//
// Usually the function you want here is DrHash32::Compute().
//


#pragma once

#include "basic_types.h"

#pragma pack (push, 8)

//JCnamespace apsdk
//JC{

class DrHash32
{
private:
    DrHash32 ();                               // no NEW allowed 
    DrHash32 ( const DrHash32& );              // no copy allowed
    DrHash32& operator= ( const DrHash32& );   // no assignment allowed

public:
 
    //
    // Mix(): cause every bit of a,b,c to affect 32 bits of a,b,c
    // both forwards and in reverse.  Same for pairs of bits in a,b,c.
    // This can be used along with Final() to hash a fixed number of
    // 4-byte integers, for example see the implementation of Guid().
    //
    inline static void Mix( UInt32& a, UInt32& b, UInt32& c)
    {
        a -= c;  a ^= _rotl(c, 4);  c += b;
        b -= a;  b ^= _rotl(a, 6);  a += c;
        c -= b;  c ^= _rotl(b, 8);  b += a;
        a -= c;  a ^= _rotl(c,16);  c += b;
        b -= a;  b ^= _rotl(a,19);  a += c;
        c -= b;  c ^= _rotl(b, 4);  b += a;
    }

    //
    // Final: cause every bit of a,b,c to affect every bit of c, only forward.
    // Same for pairs of bits in a,b,c.  It also causes b to be an OK hash.
    // This is a good way to hash 1 or 2 or 3 integers:
    //   a = k1; b = k2; c = 0;
    //   DrHash32::Final(a,b,c);
    // Use c (and maybe b) as the hash value
    //
    inline static void Final( UInt32& a, UInt32& b, UInt32& c)
    {
        c ^= b; c -= _rotl(b,14);
        a ^= c; a -= _rotl(c,11);
        b ^= a; b -= _rotl(a,25);
        c ^= b; c -= _rotl(b,16);
        a ^= c; a -= _rotl(c,4);
        b ^= a; b -= _rotl(a,14);
        c ^= b; c -= _rotl(b,24);
    }

    //
    // Compute2: Compute two hash values for a byte array of known length
    //
    static void Compute2 (
        const void *pData,    // byte array of known length
        Size_t      uSize,    // size of pData
        UInt32      uSeed1,   // first seed
        UInt32      uSeed2,   // second seed
        UInt32     *uHash1,   // OUT: first hash value (may not be null)
        UInt32     *uHash2);  // OUT: second hash value (may not be null)

    //
    // Compute: Compute a hash values for a byte array of known length
    //
    static const UInt32 Compute (
        const void *pData,      // byte array of known length
        Size_t      uSize,      // size of pData
        UInt32      uSeed = 0)  // seed for hash function
    {
        UInt32 uHash2 = 0;
        DrHash32::Compute2(pData, uSize, uSeed, uHash2, &uSeed, &uHash2);
        return uSeed;
    }

    //
    // String: hash of string of unknown length
    //
    static const UInt32 String (
        const char *pString,     // ASCII string to hash case-insensitive
        UInt32      uSeed = 0)   // optional seed for hash
    {
        UInt32      uHash2 = 0;
        Size_t      uSize;

        uSize = strlen(pString);
        DrHash32::Compute2(pString, uSize, uSeed, uHash2, &uSeed, &uHash2);
        return uSeed;
    }

    //
    // StringI2: Produce two case-insensitive 32-bit hashes of an ASCII string
    // The results are identical to Compute2() on an uppercased string
    //
    static void StringI2 (
        const char *pString,   // ASCII string to hash case-insensitive
        Size_t      uSize,     // length of string (required)
        UInt32      uSeed1,    // first seed
        UInt32      uSeed2,    // second seed
        UInt32     *uHash1,    // OUT: first hash
        UInt32     *uHash2);   // OUT: second hash

    //
    // StringI: case insensitive hash of string of unknown length
    //
    static const UInt32 StringI (
        const char *pString,     // ASCII string to hash case-insensitive
        size_t      len = (size_t)-1,
        UInt32      uSeed = 0)   // optional seed for hash
    {
        UInt32      uHash2 = 0;
        Size_t      uSize;

        uSize = (len == (size_t)-1) ? strlen(pString) : len;
        DrHash32::StringI2(pString, uSize, uSeed, uHash2, &uSeed, &uHash2);
        return uSeed;
    }

    //
    // Find a 32-bit hash of a GUID
    //
    static const UInt32 Guid ( const GUID *pGuid )
    {
        UInt32 a = ((UInt32 *)pGuid)[0];
        UInt32 b = ((UInt32 *)pGuid)[1];
        UInt32 c = ((UInt32 *)pGuid)[2];
        DrHash32::Mix(a,b,c);
        a ^= ((UInt32 *)pGuid)[3];
        DrHash32::Final(a,b,c);
        return c;
    }
};




class DrHash64
{
private:
    DrHash64 ();                               // no NEW allowed 
    DrHash64 ( const DrHash64& );              // no copy allowed
    DrHash64& operator= ( const DrHash64& );   // no assignment allowed

public:


    //
    // Compute hash for a byte array of known length.
    //
    static const UInt64 Compute ( 
        const void *pData,         // byte array to hash
        Size_t      uSize,          // length of pData
        UInt64      uSeed = 0 )     // seed to hash function; 0 is an OK value
    {
        UInt32 uHash1, uHash2;
        DrHash32::Compute2( pData, uSize, (UInt32) uSeed, (UInt32) (uSeed >> 32), &uHash1, &uHash2);
        return uHash1 | (((UInt64)uHash2) << 32);
    }

    //
    // case-insensitive hash of null terminated string
    // produce the same hash as Compute on an uppercased string
    //
    static const UInt64 StringI (
        const char *pString,
        Size_t      uSize,
        UInt64      uSeed)
    {
        UInt32 uHash1, uHash2;
        DrHash32::StringI2( pString, uSize, (UInt32) uSeed, (UInt32) (uSeed >> 32), &uHash1, &uHash2);
        return uHash1 | (((UInt64)uHash2) << 32);
    }

    // Compute a 64-bit hash of a GUID
    static const UInt64 Guid ( const GUID *pGuid )
    {
        UInt32 a = ((UInt32 *)pGuid)[0];
        UInt32 b = ((UInt32 *)pGuid)[1];
        UInt32 c = ((UInt32 *)pGuid)[2];
        DrHash32::Mix(a,b,c);
        a ^= ((UInt32 *)pGuid)[3];
        DrHash32::Final(a,b,c);
        return c | (((UInt64)b) << 32);
    }
};

#pragma pack (pop)

//JC} // namespace apsdk

#ifdef USING_APSDK_NAMESPACE
using namespace apsdk;
#endif
