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
#define _ATL_CSTRING_EXPLICIT_CONSTRUCTORS      // some CString constructors will be explicit


#include <math.h>
#include <strsafe.h>

DrError DrStringToSignedOrUnsignedInt64(const char *psz, UInt64 *pResult, bool fSigned);
DrError DrStringToFloat(const char *psz, float *pResult);
DrError DrStringToInt64(const char *psz, Int64 *pResult);
DrError DrStringToInt32(const char *psz, Int32 *pResult);
DrError DrStringToUInt64(const char *psz, UInt64 *pResult);
DrError DrStringToUInt32(const char *psz, UInt32 *pResult);
DrError DrStringToUInt16(const char *psz, UInt16 *pResult);
DrError DrStringToPortNumber(const char *psz, DrPortNumber *pResult);
DrError DrStringToUInt(const char *psz, unsigned int *pResult);
DrError DrStringToInt(const char *psz, int *pResult);
DrError DrStringToDouble(const char *psz, double *pResult);
DrError DrStringToBool(const char *psz, bool *pResult);

// Convert string to size
// String may contain KB, MB, GB, TB, PB at the end, i.e. 12KB. It's case-insensitive.
// If suffix is present then fractions are allowed , i.e. 20.5MB
DrError DrStringToSize(PCSTR psz, UInt64* result);
DrError DrStringToIntegerSize(PCSTR psz, Int64* result); //parses negative sizes as well
DrError DrStringToSizeEx(PCSTR psz, UInt64* result, bool allowNegative);

/* Returns a timestamp representing the current date and time (UTC time). Note that this time may change
   suddenly due to system clock updates and may even move backwards. */
DrTimeStamp DrGetCurrentTimeStamp();

// Returns the elapsed time between two Dryad timestamps. May be negative.
inline DrTimeInterval DrGetElapsedTime(DrTimeStamp tStart, DrTimeStamp tEnd)
{
    return (DrTimeInterval)(tEnd - tStart);
}

// Reads an environment variable
// the returned value must be freed with free()
// Note that the varname and returned value are both UTF-8 encoded
DrError DrGetEnvironmentVariable(const char *pszVarName, /* out */ const char **ppszValue);

//
// Get the environment variable using WCHARs
//
DrError DrGetEnvironmentVariable(const WCHAR *pszVarName, WCHAR ppszValue[]);

// 
// Get the SID for a user
//
DrError DrGetSidForUser(LPCWSTR domainUserName, PSID* ppSid); 

//
// Get the computer name whether it's on-premises or in Azure
//
DrError DrGetComputerName(WCHAR ppszValue[]);

__inline DWORD DrGetTimerMsFromInterval(DrTimeInterval t)
{
    if (t == DrTimeInterval_Infinite) {
        return INFINITE;
    }
    LogAssert (t >= DrTimeInterval_Zero && t < (DrTimeInterval_Millisecond * 0x7fffffff));

    // round up since timers are a minimum time
    return (DWORD)((t + DrTimeInterval_Millisecond - DrTimeInterval_Quantum) / DrTimeInterval_Millisecond);
}

// Same as ExitProcess, but flushes logging and stdout/stderr first...
void DrExitProcess(UInt32 exitCode);

// Converts a Win32 SYSTEMTIME structure, either in UTC or local time, to a Dryad timestamp
DrError DrSystemTimeToTimeStamp(const SYSTEMTIME *pSystemTime, /* out */ DrTimeStamp *pTimeStamp, bool fFromLocalTimeZone = false);

// Generates a string to append to a timestamp string to identify a local time zone ("form "Z" or "L+7h")
DrError DrGenerateTimeZoneBiasSuffix(DrTimeInterval bias, char *szBuff, size_t nbBuff);

// Converts a time interval string to a DrTimeInterval.
// If len is -1, it is computed with strlen.
// Strings must include units; e.g., "105.42s" or "12d5h10m".
DrError DrStringToTimeInterval(const char *pszString, DrTimeInterval *pTimeInterval, int len = -1);

// Converts a Dryad timestamp to a human-readable string, either in UTC  (if bias is 0) or local time (according to bias). If bias is given the
// special value DrTimeInterval_Infinite, the default local bias is used.
// nFracDig Is the number of fractional second digits to include. If -1, either 0 or 3 digits will be included depending on whether the time represents an integral second.
DrError DrTimeStampToString(DrTimeStamp timeStamp, char *pBuffer, int buffLen, DrTimeInterval bias = DrTimeInterval_Zero, Int32 nFracDig=-1);
DrError DrTimeStampToString(DrTimeStamp timeStamp, char *pBuffer, int buffLen, bool fToLocalTimeZone, Int32 nFracDig=-1);

static const size_t k_DrTimeIntervalStringBufferSize = 64; // this size guarrantees successful completion of DrTimeIntervalToString

// Converts a cosmos timeinterval to a human-readable string.
// The generated string may be fed back into DrStringToTimeInterval
DrError DrTimeIntervalToString(DrTimeInterval timeInterval, char *pBuffer, size_t buffLen);

DrError DrStringToTimeStamp(const char *pszTime, DrTimeStamp *pTimeStampOut, bool fDefaultLocalTimeZone = true);
DrError DrStringToTimeStamp(const char *pszTime, DrTimeStamp *pTimeStampOut, DrTimeInterval defaultTimeZoneBias);

// Converts a Dryad timestamp to a Win32 SYSTEMTIME structure, either in UTC or local time
DrError DrTimeStampToSystemTime(DrTimeStamp timeStamp, /* out */ SYSTEMTIME *pSystemTime, bool fToLocalTimeZone = false);
