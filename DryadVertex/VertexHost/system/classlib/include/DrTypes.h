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

#include "basic_types.h"

typedef HRESULT DrError;

typedef size_t Size_t;
static const Size_t Max_Size_t = (Size_t)((Size_t)0 - (Size_t)1);

//JCnamespace apsdk
//JC{

__forceinline DrError DrGetLastError()
{
    return HRESULT_FROM_WIN32( GetLastError() );
}

__forceinline DrError DrGetLastErrorEnforceFail()
{
    DrError hr = HRESULT_FROM_WIN32(GetLastError());
    return FAILED(hr) ? hr : E_UNEXPECTED;
}

__forceinline DrError DrErrorFromWin32(DWORD err)
{
    return HRESULT_FROM_WIN32(err);
}

///////////////////////////////////
#pragma warning(push)
#pragma warning (disable: 4201) // nonstandard extension used : nameless struct/union

/* 
   A cosmos timestamp is defined as the number of 100-nanosecond intervals that have elapsed
   since 12:00 A.M. January 1, 1601 (UTC). It is the representation of choice whenever an
   absolute date/time must be used.
*/
typedef unsigned __int64 DrTimeStamp;

static const DrTimeStamp DrTimeStamp_Never = (DrTimeStamp)_UI64_MAX;
static const DrTimeStamp DrTimeStamp_LongAgo = (DrTimeStamp)0;

/*
   A cosmos elapsed time is defined as the number of 100-nanosecond intervals between two
   points in time. It may be negative. It is what you get when you subtract two DrTimestamp values, and is
   the representation of choice whenever a time interval needs to be represented persistently
   or in a network protocol.
*/
typedef __int64 DrTimeInterval;

static const DrTimeInterval DrTimeInterval_Infinite = (DrTimeInterval)_I64_MAX;
static const DrTimeInterval DrTimeInterval_NegativeInfinite = (DrTimeInterval)_I64_MIN;
static const DrTimeInterval DrTimeInterval_Zero = (DrTimeInterval)0;
static const DrTimeInterval DrTimeInterval_Quantum = (DrTimeInterval)1;
static const DrTimeInterval DrTimeInterval_100ns = DrTimeInterval_Quantum;
static const DrTimeInterval DrTimeInterval_Microsecond = DrTimeInterval_100ns * 10;
static const DrTimeInterval DrTimeInterval_Millisecond = DrTimeInterval_Microsecond * 1000;
static const DrTimeInterval DrTimeInterval_Second = DrTimeInterval_Millisecond * 1000;
static const DrTimeInterval DrTimeInterval_Minute = DrTimeInterval_Second * 60;
static const DrTimeInterval DrTimeInterval_Hour = DrTimeInterval_Minute * 60;
static const DrTimeInterval DrTimeInterval_Day = DrTimeInterval_Hour * 24;
static const DrTimeInterval DrTimeInterval_Week = DrTimeInterval_Day * 7;

// A DrTimeInterval_Year is defined as 52 weeks. It is for convenience, not for computing exact years.
static const DrTimeInterval DrTimeInterval_Year = DrTimeInterval_Week * 52;

/* An IPV4 IP address in host byte order */
typedef UInt32 DrIpAddress;

static const DrIpAddress DrAnyIpAddress = 0;
static const DrIpAddress DrInvalidIpAddress = 0;
static const DrIpAddress DrLocalIpAddress = 0x7F000001;  // 127.0.0.1


/* An IP port number in host byte order */
typedef UInt16 DrPortNumber;

static const DrPortNumber DrInvalidPortNumber = 0xFFFF;
static const DrPortNumber DrAnyPortNumber = 0;

#pragma warning(pop)

//JC} // namespace apsdk
#ifdef USING_APSDK_NAMESPACE
using namespace apsdk;
#endif

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

