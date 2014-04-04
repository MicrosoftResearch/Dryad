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
#include <aclapi.h>
#include <Sddl.h>

#pragma unmanaged

#pragma warning (disable: 4995) // '_snprintf': name was marked as #pragma deprecated

DrError DrStringToSignedOrUnsignedInt64(const char *psz, UInt64 *pResult, bool fSigned)
{
    UInt64 v = 0;
    UInt64 vnew;
    bool neg = false;
    bool gotDig = false;
    int base = 10;

    while (ISSPACE(*psz)) {
        psz++;
    }

    if (*psz == '+') {
        psz++;
    } else if (fSigned && *psz == '-') {
        neg = true;
        psz++;
    }

    if (*psz == '0' && (*(psz+1) == 'x' ||*(psz+1) == 'X') ) {
        psz += 2;
        base = 16;
    }

    if (base == 16 && neg == false) {
        // we allow hex constants to set the sign bit.
        fSigned = false;
    }

    while (*psz != '\0' && !ISSPACE(*psz)) {
        int dig = -1;
        if (*psz >= '0' && *psz <= '9') {
            dig = *psz - '0';
        } else if (*psz >= 'a' && *psz <= 'f') {
            dig = *psz - 'a' + 10;
        } else if (*psz >= 'A' && *psz <= 'F') {
            dig = *psz - 'A' + 10;
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

    while (ISSPACE(*psz)) {
        psz++;
    }

    if (*psz != '\0') {
        return DrError_InvalidParameter;
    }

    if (neg) {
        v= (UInt64)(-(Int64)v);
    }

    *pResult = v;
    return DrError_OK;
}

DrError DrStringToFloat(const char *psz, float *pResult)
{
    double v;
    DrError err = DrStringToDouble(psz, &v);
    if (err == DrError_OK) {
        *pResult = (float)v;
    }
    return err;
}

DrError DrStringToUInt64(const char *psz, UInt64 *pResult)
{
    return DrStringToSignedOrUnsignedInt64(psz, pResult, false);
}

DrError DrStringToInt64(const char *psz, Int64 *pResult)
{
    return DrStringToSignedOrUnsignedInt64(psz, (UInt64 *)(void *)pResult, true);
}

DrError DrStringToUInt16(const char *psz, UInt16 *pResult)
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

DrError DrStringToUInt32(const char *psz, UInt32 *pResult)
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

DrError DrStringToInt32(const char *psz, Int32 *pResult)
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

DrError DrStringToUInt(const char *psz, unsigned int *pResult)
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

DrError DrStringToInt(const char *psz, int *pResult)
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

DrError DrStringToDouble(const char *psz, double *pResult)
{
    double v = 0;
    int exponent = 0;
    bool neg = false;
    bool gotDig = false;
    bool gotPoint = false;

    while (ISSPACE(*psz)) {
        psz++;
    }

    if (*psz == '+') {
        psz++;
    } else if (*psz == '-') {
        neg = true;
        psz++;
    }

    while (*psz != '\0' && !ISSPACE(*psz) && *psz != 'e' && *psz != 'E') {
        int dig = -1;
        if (!gotPoint && *psz == '.') {
            gotPoint = true;
            psz++;
            continue;
        } else if (*psz >= '0' && *psz <= '9') {
            dig = *psz - '0';
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

    if (*psz == 'e' || *psz == 'E') {
        psz++;
        Int32 exp2;
        DrError err = DrStringToInt32(psz, &exp2);
        if (err != DrError_OK) {
            return err;
        }
        exponent += exp2;
    } else {
        while (ISSPACE(*psz)) {
            psz++;
        }

        if (*psz != '\0') {
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

DrError DrStringToBool(const char *psz, bool *pResult)
{
    bool ret = false;
    char tmp[16];
    size_t length = 0;

    while (ISSPACE(*psz)) {
        psz++;
    }

    while (length < 15 && *psz != '\0' && !ISSPACE(*psz)) {
        tmp[length++] = *(psz++);
    }

    tmp[length] = '\0';

    while (ISSPACE(*psz)) {
        psz++;
    }

    if (*psz != '\0') {
        return DrError_InvalidParameter;
    }

    _strlwr(tmp);

    length++;
    if (strncmp(tmp, "true", length) == 0 ||
         strncmp(tmp, "yes", length) == 0 ||
         strncmp(tmp, "on", length) == 0 ||
         strncmp(tmp, "1", length) == 0) {
        ret = true;
    } else if (
         strncmp(tmp, "false", length) == 0 ||
         strncmp(tmp, "no", length) == 0 ||
         strncmp(tmp, "off", length) == 0 ||
         strncmp(tmp, "0", length) == 0) {
        ret = false;
    } else {
        return DrError_InvalidParameter;
    }

    *pResult = ret;
    return DrError_OK;
}

DrError DrStringToSizeEx(PCSTR psz, UInt64* result, bool allowNegative)
{
    if ( psz == NULL || result == NULL )
    {
        return DrError_InvalidParameter;
    }

    char buf[48];
    DrError err = StringCbCopyA( buf, sizeof( buf ), psz );
    if ( FAILED( err ) )
    {
        return DrError_InvalidParameter;
    }

    UInt32 shift = 0;
    Size_t len = strlen(psz);
    if ( len > 2 )
    {
        PCSTR tail = buf + len - 2;
        if ( _stricmp( tail, "KB" ) == 0 )
        {
            shift = 10;
        }
        else if ( _stricmp( tail, "MB" ) == 0 )
        {
            shift = 20;
        }
        else if ( _stricmp( tail, "GB" ) == 0 )
        {
            shift = 30;
        }
        else if ( _stricmp( tail, "TB" ) == 0 )
        {
            shift = 40;
        }
        else if ( _stricmp( tail, "PB" ) == 0 )
        {
            shift = 50;
        }
    }

    if ( shift != 0 )
    {
        buf[len - 2] = 0;
    }

    //check if dot is present
    PSTR dot = strchr( buf, '.' );

    if ( dot != NULL )
    {
        // can't have fractions if size specifier is not present
        if ( shift == 0 )
        {
            return DrError_InvalidParameter;
        }

        *dot = 0;
    }

    bool negative = false;
    UInt64 r1;
    err = DrStringToSignedOrUnsignedInt64( buf, &r1, allowNegative );
    if ( err != DrError_OK )
    {
        return err;
    }
    if ( allowNegative && ((Int64)r1 < 0ui64) )
    {
        negative = true;
        r1 = (UInt64)-(Int64)r1;
    }

    UInt64 maxVal = allowNegative ? MAX_INT64 : MAX_UINT64;
    if ( dot != NULL )
    {
        maxVal >>= 4; // reserve space for fraction
    }
    maxVal >>= shift;

    if ( r1 > maxVal )
    {
        return DrError_InvalidParameter;
    }

    r1 <<= shift;

    if ( dot != NULL )
    {
        char buf2[48];
        buf2[0] = '0';
        buf2[1] = '.';
        buf2[2] = 0;

        err = StringCbCatA( buf2, sizeof(buf2), dot + 1 );
        if ( FAILED( err ) )
        {
            return DrError_InvalidParameter;
        }

        double rd;
        err = DrStringToDouble( buf2, &rd );
        if ( err != DrError_OK )
        {
            return err;
        }

        rd *= 1ui64 << shift;
        r1 += (UInt64)rd;
    }

    if ( negative )
    {
        r1 = (UInt64)-(Int64)r1;
    }

    *result = r1;

    return DrError_OK;
}

DrError DrStringToSize(PCSTR psz, UInt64* result)
{
    return DrStringToSizeEx( psz, result, false );
}

DrError DrStringToIntegerSize(PCSTR psz, Int64* result)
{
    return DrStringToSizeEx( psz, (UInt64*)result, true );
}


//
// Close session after completing outstanding requests
//
DrError DryadShutdown();

//
// Same as ExitProcess, but flushes logging and stdout/stderr first...
//
void DrExitProcess(UInt32 exitCode)
{ 
    //
    // Close the cluster connection
    //
    DrError e = DryadShutdown();
    if (e == DrError_OK)
    {
        DrLogI("Completed uninitialize dryad");
    }
    else
    {
        DrLogE("Couldn't uninitialize dryad");
    }
    
    //
    // Flush output logs
    //
    fflush(stdout);
    DrLogging::FlushLog();

    //
    // Exit the current process
    //
    ExitProcess((UINT)exitCode);
}

DrError DrSystemTimeToTimeStamp(const SYSTEMTIME *pSystemTime, DrTimeStamp *pTimeStamp, bool fFromLocalTimeZone)
{
    union {
        FILETIME    ft;
        DrTimeStamp ts;
    };

    union {
        FILETIME    ft2;
        DrTimeStamp ts2;
    };

    if (!SystemTimeToFileTime(pSystemTime, &ft)) {
        return DrGetLastError();
    }

   if (fFromLocalTimeZone && ts != DrTimeStamp_LongAgo && ts != DrTimeStamp_Never) {
        if (!LocalFileTimeToFileTime(&ft, &ft2)) {
            return DrGetLastError();
        }
        *pTimeStamp = ts2;
    } else {
        *pTimeStamp = ts;
    }

     return DrError_OK;
}

// Returns UTC time
DrTimeStamp DrGetCurrentTimeStamp()
{
    union {
        FILETIME    ft;
        DrTimeStamp ts;
    };
    GetSystemTimeAsFileTime(&ft);
    return ts;
}

DrTimeInterval DrGetCurrentLocalTimeZoneBias()
{
    TIME_ZONE_INFORMATION tzi;
    LONG biasMinutes;

    DWORD dwRet = GetTimeZoneInformation(&tzi);
    switch(dwRet) {
        case TIME_ZONE_ID_UNKNOWN:
            biasMinutes = tzi.Bias;
            break;

        case TIME_ZONE_ID_STANDARD:
            biasMinutes = tzi.Bias + tzi.StandardBias;
            break;

        case TIME_ZONE_ID_DAYLIGHT:
            biasMinutes = tzi.Bias + tzi.DaylightBias;
            break;

        default:
            LogAssert(dwRet != dwRet);
            return DrTimeInterval_Zero;
    }

    DrTimeInterval bias = DrTimeInterval_Minute * biasMinutes;
    return bias;
}

DrError DrGenerateTimeZoneBiasSuffix(DrTimeInterval bias, char *szBuff, size_t nbBuff)
{

    if (bias == DrTimeInterval_Zero) {
        if (nbBuff < 2) {
            return HRESULT_FROM_WIN32( ERROR_INSUFFICIENT_BUFFER );
        }

        szBuff[0]= 'Z';
        szBuff[1] = '\0';
        return DrError_OK;
    } else {
        if (nbBuff < 4) {
            return HRESULT_FROM_WIN32( ERROR_INSUFFICIENT_BUFFER );
        }
        *(szBuff++)= 'L';
        nbBuff--;
        char s = '+';
        if (bias < DrTimeInterval_Zero) {
            s = '-';
            bias = -bias;
        }
        *(szBuff++)= s;
        nbBuff--;
        *szBuff = '\0';
        return DrTimeIntervalToString(bias, szBuff, nbBuff);
    }
}

__inline DrError DrMultiplyTimeInterval(DrTimeInterval *pValue, Int64 n)
{
    DrTimeInterval v2 = *pValue * n;
    if (n != 0 && v2 / n != *pValue) {
        return DrError_InvalidTimeInterval;
    }
    *pValue = v2;
    return DrError_OK;
}

static DrError DrAddDeltaUnitTimeInterval(DrTimeInterval *pValue, Int64 n, DrTimeInterval units)
{
    DrError err = DrMultiplyTimeInterval(&units, n);
    if (err != DrError_OK) {
        return err;
    }

    // if the signs of the two addends are the same, we have to check for overflow in the result
    bool checkSign = ((*pValue >= DrTimeInterval_Zero) == (units >= DrTimeInterval_Zero));

    *pValue += units;

    // if the signs of the two addends were the same, they still should be
    if (checkSign && (*pValue >= DrTimeInterval_Zero) != (units >= DrTimeInterval_Zero)) {
        return DrError_InvalidTimeInterval;
    }

    return DrError_OK;
}

// Converts a time interval string to a DrTimeInterval.
// If len is -1, it is computed with strlen.
// Strings must include units; e.g., "105.42s" or "12d5h10m".
DrError DrStringToTimeInterval(const char *pszString, DrTimeInterval *pTimeInterval, int len)
{
    DrError err = DrError_OK;
    DrTimeInterval val = 0;
    bool neg = false;
    char szBuff[32];

    if (pszString == NULL) {
        return DrError_InvalidTimeInterval;
    }

    if (pszString[0] == '+') {
        pszString++;
    } else if (pszString[0] == '-') {
        pszString++;
        neg = true;
    }

    if (len < 0) {
        len = (int)strlen(pszString);
    }

    if (len == 0 || *pszString == '\0') {
        return DrError_InvalidTimeInterval;
    }

    if (len == 8 && _strnicmp(pszString, "infinite", 8) == 0) {
        *pTimeInterval = DrTimeInterval_Infinite;
        return DrError_OK;
    }

    if (len == 16 && _strnicmp(pszString, "negativeinfinite", 16) == 0) {
        *pTimeInterval = DrTimeInterval_NegativeInfinite;
        return DrError_OK;
    }

    if (len == 1 && pszString[0] == '0') {
        *pTimeInterval = DrTimeInterval_Zero;
        return DrError_OK;
    }

    int i = 0;
    while (i < len && pszString[i] != '\0') {
        UInt64 n = 0;
        UInt64 frac = 0;
        int nFrac = 0;
        int nDig = 0;

        while (nDig < 31 && i < len && pszString[i] >= '0' && pszString[i] <= '9') {
            szBuff[nDig++] = pszString[i];
            i++;
        }

        if (nDig > 0) {
            szBuff[nDig] = '\0';
            err = DrStringToUInt64(szBuff, &n);
            if (err != DrError_OK || (Int64)n < 0) {
                return DrError_InvalidTimeInterval;
            }
        }

        if (i < len && pszString[i] == '.') {
            i++;
            while (nFrac < 31 && i < len && pszString[i] >= '0' && pszString[i] <= '9') {
                szBuff[nFrac++] = pszString[i];
                i++;
            }
            if (nFrac > 0) {
                szBuff[nFrac] = '\0';
                err = DrStringToUInt64(szBuff, &frac);
                if (err != DrError_OK || (Int64)frac < 0) {
                    return DrError_InvalidTimeInterval;
                }
            }
        }

        if (nDig == 0 && nFrac == 0) {
            return DrError_InvalidTimeInterval;
        }

        char szUnit[2];
        szUnit[0] = ' ';
        szUnit[1] = ' ';
        for (int k = 1; k >= 0; --k) {
            if (i < len && pszString[i] != '\0' && (pszString[i] < '0' || pszString[i] > '9') && pszString[i] != '.') {
                szUnit[k] = pszString[i++];
                if (szUnit[k] >= 'A' && szUnit[k] <= 'Z') {
                    szUnit[k] = szUnit[k] - 'A' + 'a';
                }
            } else {
                break;
            }
        }

        int tag = (int)*(WORD *)(void *)szUnit;
        DrTimeInterval units = 0;
        int fracKeep = 0; // # of fraction digits to keep
        DrTimeInterval fracunits;
        switch(tag) {
            default:
                return DrError_InvalidTimeInterval;

            case 'q ':
                units = DrTimeInterval_Quantum;
                fracunits = DrTimeInterval_Quantum;
                fracKeep = 0;
                break;

            case 'us':
                units = DrTimeInterval_Microsecond;
                fracunits = DrTimeInterval_Quantum;
                fracKeep = 1;  // 10 microseconds per interval
                break;

            case 'ms':
                units = DrTimeInterval_Millisecond;
                fracunits = DrTimeInterval_Quantum;
                fracKeep = 4; // 10,000
                break;

            case 's ':
                units = DrTimeInterval_Second;
                fracunits = 1;
                fracKeep = 7; // 10,000,000
                break;

            case 'm ':
                units = DrTimeInterval_Minute;
                fracunits = DrTimeInterval_Minute / 100000000;
                fracKeep = 8; // 600,000,000
                break;

            case 'h ':
                units = DrTimeInterval_Hour;
                fracunits = DrTimeInterval_Hour / 1000000000;
                fracKeep = 9; // 36,000,000,000
                break;

            case 'd ':
                units = DrTimeInterval_Day;
                fracunits = DrTimeInterval_Day / 1000000000;
                fracKeep = 9; // 864,000,000,000
                break;

            case 'w ':
                units = DrTimeInterval_Week;
                fracunits = DrTimeInterval_Week / 1000000000;
                fracKeep = 9; // 6,048,000,000,000
                break;

            case 'y ':
                units = DrTimeInterval_Year;
                fracunits = DrTimeInterval_Year / 1000000000;
                fracKeep = 9; // 314,496,000,000,000
                break;

        }

        // Normalize the fraction to the correct number of digits
        while (nFrac > fracKeep) {
            frac = frac / 10;
            nFrac--;
        }
        while (nFrac < fracKeep) {
            frac = frac * 10;
            nFrac++;
        }

        // result should be n*units + frac*fracunits

        err = DrAddDeltaUnitTimeInterval(&val, (Int64)n, units);
        if (err != DrError_OK) {
            return err;
        }
        err = DrAddDeltaUnitTimeInterval(&val, (Int64)frac, fracunits);
        if (err != DrError_OK) {
            return err;
        }
    }

    if (neg) {
        val = -val;
    }

    *pTimeInterval = val;

    return DrError_OK;
}

DrError DrTimeStampToString(DrTimeStamp timeStamp, char *pBuffer, int buffLen, DrTimeInterval bias, Int32 nFracDig)
{
    SYSTEMTIME st;

    if (timeStamp == DrTimeStamp_Never) {
        if (buffLen < 6) {
            return HRESULT_FROM_WIN32( ERROR_INSUFFICIENT_BUFFER );
        }
        strncpy(pBuffer, "never", 6);
        return DrError_OK;
    } else if (timeStamp == DrTimeStamp_LongAgo) {
        if (buffLen < 9) {
            return HRESULT_FROM_WIN32( ERROR_INSUFFICIENT_BUFFER );
        }
        strncpy(pBuffer, "longago", 9);
        return DrError_OK;
    }

    if (bias == DrTimeInterval_Infinite) {
        bias = DrGetCurrentLocalTimeZoneBias();
    }

    timeStamp -= bias;

    DrError err = DrTimeStampToSystemTime(timeStamp, &st, false);
    if (err != DrError_OK) {
        return err;
    }

    if (nFracDig < 0) {
        if (st.wMilliseconds == 0) {
            nFracDig = 0;
        } else {
            nFracDig = 3;
        }
    }

    char szFrac[16];
    if (nFracDig == 0) {
        szFrac[0] = '\0';
    } else {
        szFrac[0] = '.';
        LogAssert(st.wMilliseconds < 1000);
        sprintf(szFrac+1, "%03u", st.wMilliseconds);
        if (nFracDig > 3) {
            if (nFracDig > 7) {
                nFracDig = 7;
            }
            // Compute quantum ticks in excess of 1ms boundary
            UInt32 remainder = (UInt32)(timeStamp % 10000);
            sprintf(szFrac+4, "%04u", remainder);
        }
        szFrac[nFracDig+1] = '\0';
    }
    

    if (bias == DrTimeInterval_Zero) {
        // No local time bias -- Zulu time
        _snprintf(pBuffer, (size_t)buffLen,  "%04u-%02u-%02uT%02u:%02u:%02u%sZ",
            st.wYear,
            st.wMonth,
            st.wDay,
            st.wHour,
            st.wMinute,
            st.wSecond,
            szFrac);
    } else {
        // Local time zone bias
        char szSuffix[k_DrTimeIntervalStringBufferSize+2];
        const char *suffix;
        err = DrGenerateTimeZoneBiasSuffix(bias, szSuffix, ELEMENTCOUNT(szSuffix));
        LogAssert(err == DrError_OK);
        suffix = szSuffix;
        _snprintf(pBuffer, (size_t)buffLen, "%04u-%02u-%02uT%02u:%02u:%02u%s%s",
            st.wYear,
            st.wMonth,
            st.wDay,
            st.wHour,
            st.wMinute,
            st.wSecond,
            szFrac,
            suffix);
    }

    return DrError_OK;
}

// Converts a Dryad timeinterval to a human-readable string.
// The generated string may be fed back into DrStringToTimeInterval
DrError DrTimeIntervalToString(DrTimeInterval timeInterval, char *pBuffer, size_t buffLen)
{
    LogAssert(pBuffer != NULL);
    LogAssert(buffLen != 0);
    char tempBuff[k_DrTimeIntervalStringBufferSize];
    char *pBuff = pBuffer;
    if (buffLen < k_DrTimeIntervalStringBufferSize) {
        // use a temporary buffer if we aren't sure it will fit
        pBuff = tempBuff;
    }
    DrError err = DrError_OK;

    if (timeInterval == DrTimeInterval_Infinite) {
        strcpy(pBuff, "infinite");
    } else if (timeInterval == DrTimeInterval_NegativeInfinite) {
        strcpy(pBuff, "negativeinfinite");
    } else {
        bool neg = (timeInterval < DrTimeInterval_Zero);
        UInt64 v;
        if (neg) {
            v = (UInt64)(-timeInterval);
        } else {
            v = (UInt64)timeInterval;
        }
        UInt32 frac100ns = (UInt32)(v % DrTimeInterval_Second);
        v = v / DrTimeInterval_Second;
        UInt32 sec = (UInt32)(v % (UInt32)60);
        v = v / 60;
        UInt32 min = (UInt32)(v % (UInt32)60);
        v = v / 60;
        UInt32 hr = (UInt32)(v % (UInt32)24);
        v = v / 24;
        // v now contains days


        int i =0;
        if (neg) {
            pBuff[i++] = '-';
        }
        int ret = 0;
        bool fOutput = false;
        if (v != 0) {
            ret = _snprintf(pBuff+i, k_DrTimeIntervalStringBufferSize-i-1, "%I64ud", v);
            LogAssert(ret > 0);
            i += ret;
            fOutput = true;
        }
        if (hr != 0 || (fOutput && (min != 0 || sec != 0 || frac100ns != 0))) {
            fOutput = true;
            ret = _snprintf(pBuff+i, k_DrTimeIntervalStringBufferSize-i-1, "%uh", hr);
            LogAssert(ret > 0);
            i += ret;
        }
        if (min != 0 || (fOutput && (sec != 0 || frac100ns != 0))) {
            fOutput = true;
            ret = _snprintf(pBuff+i, k_DrTimeIntervalStringBufferSize-i-1, "%um", min);
            LogAssert(ret > 0);
            i += ret;
        }
        if (frac100ns == 0) {
            // whole number of seconds
            if (sec != 0 || !fOutput) {
                fOutput = true;
                ret = _snprintf(pBuff+i, k_DrTimeIntervalStringBufferSize - i - 1, "%us", sec);
                LogAssert(ret > 0);
                i += ret;
            }
        } else {
            // fractional seconds
            fOutput = true;
            ret = _snprintf(pBuff+i, k_DrTimeIntervalStringBufferSize - i - 1, "%u.%07u", sec, frac100ns);
            LogAssert(ret > 0);
            i += ret;

            // remove traling "0" characters
            while (i > 0 && pBuff[i-1] == '0') {
                --i;
            }

            LogAssert((size_t)i+2 < k_DrTimeIntervalStringBufferSize);
            pBuff[i++] = 's';
            pBuff[i] = '\0';
        }
    }

    if (err == DrError_OK && pBuff == tempBuff) {
        size_t n = strlen(tempBuff) + 1;
        if (n <= buffLen) {
            memcpy(pBuffer, tempBuff, n);
        } else {
            err = DrError_StringTooLong;
            pBuffer[0] = '\0';
        }
    }

    return err;
}

DrError DrTimeStampToString(DrTimeStamp timeStamp, char *pBuffer, int buffLen, bool fToLocalTimeZone, Int32 nFracDig)
{
    return DrTimeStampToString(timeStamp, pBuffer, buffLen, fToLocalTimeZone ? DrTimeInterval_Infinite : DrTimeInterval_Zero, nFracDig);
}

DrError DrStringToTimeStamp(const char *pszTime, DrTimeStamp *pTimeStampOut, DrTimeInterval defaultTimeZoneBias)
{
    // 2006-04-18 13:06:44
    // 2006-04-18T13:06:44.mmmL+8h
   // 2006-04-18T13:06:44.mmm-08:00
   // 2006-04-18T13:06:44.mmmZ
    // 01234567890123456789
    // +timeinterval
    // -timeinterval
    DrError err = DrError_OK;

    UInt32 uYear;
    UInt32 uMonth;
    UInt32 uDay;
    UInt32 uHour = 0;
    UInt32 uMinute = 0;
    UInt32 uSecond = 0;
    UInt32 uFrac = 0;
    UInt32 nFracDigs = 0;
    DrTimeInterval bias = defaultTimeZoneBias;

    // If the first character is "+" or "-", it is a relative time interval to the current time
    if (pszTime != NULL && (pszTime[0] == '-' || pszTime[0] == '+')) {
        DrTimeInterval ti;
        err = DrStringToTimeInterval(pszTime, &ti);
        if (err == DrError_OK) {
            *pTimeStampOut = DrGetCurrentTimeStamp() + ti;
        }
        return err;
    }

    // If the string consists entirely of digits, it is a simple decimal encoding of a DrTimeStamp. We optimize for this case:
    if (pszTime != NULL) {
        char c1 = *pszTime;
        // An early detector, numbers that don't start with 1 or 2 (for year) are usually simple numbers
        if ((c1 >= '0' && c1 < '1') || (c1 >= '3' && c1 <= '9')) {
            goto trySimple;
        }
        // If first 5 chars are numeric, it is probably a simple number
        for (UInt32 i = 0; i < 5; i++) {
            if (pszTime[i] == '\0') {
                break;
            } else if (pszTime[i] < '0' || pszTime[i] > '9') {
                goto notSimple;
            }
        }

trySimple:
        // might be a simple number
        err = DrStringToUInt64(pszTime, pTimeStampOut);
        if (err == DrError_OK) {
            return err;
        }

        // If not a simple number, fall through to try string forms...

    }

notSimple:

    DrStr32 strTime(pszTime);

    if (strTime.GetLength() == 7 && strTime == "longago") {
        *pTimeStampOut = DrTimeStamp_LongAgo;
        return DrError_OK;
    } else if (strTime == "never") {
        *pTimeStampOut = DrTimeStamp_Never;
        return DrError_OK;
    }

    if (strTime.GetLength() < 10 || strTime.GetLength() > 40) {
        return DrError_InvalidParameter;
    }

    char *psz = &(strTime[0]);

    if (psz[4] != '-') {
        return DrError_InvalidParameter;
    }

    psz[4] = '\0';
    err = DrStringToUInt32(psz, &uYear);
    if (err != DrError_OK) {
        return err;
    }
    if (uYear < 1600 || uYear > 9999) {
        return DrError_InvalidParameter;
    }
    psz += 5;

    if (psz[2] != '-') {
        return DrError_InvalidParameter;
    }

    psz[2] = '\0';
    err = DrStringToUInt32(psz, &uMonth);
    if (err != DrError_OK) {
        return err;
    }
    if (uMonth < 1 || uMonth > 12) {
        return DrError_InvalidParameter;
    }
    psz += 3;


    char chNext = psz[2];
    psz[2] = '\0';

    err = DrStringToUInt32(psz, &uDay);
    if (err != DrError_OK) {
        return err;
    }
    if (uDay < 1 || uDay > 31) {
        return DrError_InvalidParameter;
    }

    if (chNext == '\0') {
        psz += 2;
    } else {
        psz += 3;
    }

    if (chNext == ' ' || chNext == 'T') {
        // there is HH:MM:SS
        if (strTime.GetLength() < 19) {
            return DrError_InvalidParameter;
        }
        if (psz[2] != ':') {
            return DrError_InvalidParameter;
        }
        psz[2] = '\0';
        err = DrStringToUInt32(psz, &uHour);
        if (err != DrError_OK) {
            return err;
        }
        if (uHour > 23) {
            return DrError_InvalidParameter;
        }
        psz += 3;
        if (psz[2] != ':') {
            return DrError_InvalidParameter;
        }
        psz[2] = '\0';
        err = DrStringToUInt32(psz, &uMinute);
        if (err != DrError_OK) {
            return err;
        }
        if (uMinute > 59) {
            return DrError_InvalidParameter;
        }
        psz += 3;
        chNext = psz[2];
        psz[2] = '\0';

        err = DrStringToUInt32(psz, &uSecond);
        if (err != DrError_OK) {
            return err;
        }
        if (uSecond > 59) {
            return DrError_InvalidParameter;
        }

        if (chNext == '\0') {
            psz += 2;
        } else {
            psz += 3;
        }

        if (chNext == '.') {
            // fraction
            while (*psz >= '0' && *psz <= '9') {
                nFracDigs++;
                if (nFracDigs > 9) {
                    return DrError_InvalidParameter;
                }
                uFrac = (10 * uFrac) + (UInt32) (*psz - '0');
                psz++;
            }
            chNext = *psz;
            if (chNext != '\0') {
                psz++;
            }
        }
    }

    if (chNext == 'Z') {
        bias = 0;
    } else if (chNext == 'L') {
        bool fNeg = false;
        if (*psz == '+' || *psz == '-') {
            fNeg = (*psz == '-');
            psz++;
            err = DrStringToTimeInterval(psz, &bias);
            if (err != DrError_OK) {
                return err;
            }
            if (fNeg) {
                bias = -bias;
            }
            psz += strlen(psz);
        }
    }
    //case where time end with [-/+]HH:MM
    else if (chNext == '+' || chNext == '-')
    {
        UInt32 ubiashour = 0;
        UInt32 ubiasMinute = 0;

        while(isdigit((int)(*psz)))
        {
            ubiashour = ubiashour * 10 + (*psz-'0');
            psz++;
        }
       
        if(*psz != ':')
        {
             return DrError_InvalidParameter;
        }
        psz++;
        
        while(isdigit((int)(*psz)))
        {
            ubiasMinute= ubiasMinute* 10 + (*psz-'0');
            psz++;
        }

        bias =  ubiashour * DrTimeInterval_Hour + ubiasMinute * DrTimeInterval_Minute;

        //If timezone is -08:00, we have to add 8 hrs to find utc time.
        if(chNext == '+')
        {
            bias = -bias;
        }
    }

    if (*psz != '\0') {
        return DrError_InvalidParameter;
    }

    SYSTEMTIME st;
    st.wDayOfWeek = 0; // unknown
    st.wYear = (WORD) uYear;
    st.wMonth = (WORD) uMonth;
    st.wDay = (WORD) uDay;
    st.wHour = (WORD) uHour;
    st.wMinute = (WORD) uMinute;
    st.wSecond = (WORD) uSecond;
    st.wMilliseconds = 0; // we handle milliseconds ourselves to get better resolution...

    while (nFracDigs > 7 && uFrac != 0) {
        uFrac = uFrac / 10;
        nFracDigs --;
    }
    while (nFracDigs < 7 && uFrac != 0) {
        uFrac = 10 * uFrac;
        nFracDigs++;
    }

    DrTimeStamp ts;
    err = DrSystemTimeToTimeStamp(&st, &ts, false);
    if (err != DrError_OK) {
        return err;
    }

    ts += uFrac;
    ts += bias;

    *pTimeStampOut = ts;

    return DrError_OK;
}

DrError DrStringToTimeStamp(const char *pszTime, DrTimeStamp *pTimeStampOut, bool fDefaultLocalTimeZone)
{
    DrTimeInterval bias;
    if (fDefaultLocalTimeZone) {
        bias = DrGetCurrentLocalTimeZoneBias();
    } else {
        bias = 0;
    }
    return DrStringToTimeStamp(pszTime, pTimeStampOut, bias);
}

DrError DrTimeStampToSystemTime(DrTimeStamp timeStamp, SYSTEMTIME *pSystemTime, bool fToLocalTimeZone)
{
    union {
        FILETIME    ft;
        DrTimeStamp ts;
    };

    if (fToLocalTimeZone && timeStamp != DrTimeStamp_LongAgo && timeStamp != DrTimeStamp_Never) {
        if (!FileTimeToLocalFileTime((const FILETIME *)(const void *)&timeStamp, &ft)) {
            return DrGetLastError();
        }
    } else {
        ts = timeStamp;
    }

    if (!FileTimeToSystemTime(&ft, pSystemTime)) {
        return DrGetLastError();
    }
    return DrError_OK;
}


//
// Get an environment variable
//
DrError DrGetEnvironmentVariable(const WCHAR *pszVarName, WCHAR ppszValue[])
{
    DrError err;
    WCHAR *  psz = NULL;
    DWORD nb2 = 0;

    
    //
    // Get length of environment variable value
    // 
    DWORD nb = GetEnvironmentVariableW(pszVarName, NULL, 0);
    if (nb == 0) 
    {
        err = DrGetLastError();
        goto done;
    }

    psz = (WCHAR *)malloc(sizeof(WCHAR) * nb);
    
    //
    // Get environment variable value
    //
    nb2 = GetEnvironmentVariableW(pszVarName, psz, nb);
    if (nb2 == 0) 
    {
        err = DrGetLastError();
        goto done;
    }

    err = DrError_OK;

    // Fail if more than MAX_PATH characters
    if(MAX_PATH <= nb2)
    {
        err = DrError_Fail;
    }

done:
    if (err != DrError_OK || psz == NULL)
    {
        // GLE may return wrong results in mixed mode code. If we catch S_OK we need to return a meaningful code instead
        if (err == S_OK) err = ERROR_ENVVAR_NOT_FOUND; 

        //
        // If there has been an error, set value to null
        // and free any allocated resources
        //
        if (psz != NULL) 
        {
            free(psz);
        }

        *ppszValue = NULL;
    } 
    else 
    {
        LogAssert(psz != NULL);
        // use length + 1 to get null character ending
        wcsncpy(ppszValue, psz, nb2+1);
        free(psz);
    }

    return err;
}

//
// Get an environment variable
//
DrError DrGetEnvironmentVariable(const char *pszVarName, const char **ppszValue)
{
    DrError err;
    LPWSTR myenvname;
    LPWSTR  psz = NULL;

    int charLen = lstrlenA(pszVarName);
    int wcharLen;

    //
    // Get length of variable name
    //
    wcharLen = ::MultiByteToWideChar(CP_ACP, NULL, pszVarName, charLen, NULL, NULL);
    if (wcharLen > 0)
    {
        //
        // Get converted variable name
        //
        myenvname = ::SysAllocStringLen(0, wcharLen);
        ::MultiByteToWideChar(CP_ACP, 0, pszVarName, charLen, myenvname, wcharLen);
    }
    else
    {
        //
        // If unable to get length, fail
        //
        err = DrGetLastError();
        goto done;
    }


    //
    // Get length of environment variable value
    // 
    DWORD nb = GetEnvironmentVariableW(myenvname, NULL, 0);
    if (nb == 0) {
        err = DrGetLastError();
        goto done;
    }

    psz = (LPWSTR )malloc(sizeof(char) * nb);
    
    //
    // Get environment variable value
    //
    DWORD nb2 = GetEnvironmentVariableW(myenvname, psz, nb);
    if (nb2 == 0) {
        err = DrGetLastError();
        goto done;
    }

    err = DrError_OK;

done:
    if (err != DrError_OK) 
    {
        //
        // If there has been an error, set value to null
        // and free any allocated resources
        //
        if (psz != NULL) 
        {
            free(psz);
        }

        *ppszValue = NULL;
    } 
    else 
    {
        LogAssert(psz != NULL);
        *ppszValue = (char*) psz;
    }

    return err;
}

DrError DrGetSidForUser(LPCWSTR domainUserName, PSID* ppSid) 
{
    // Create buffers for SID and domain. If size > default, will retry with new size
    DWORD bufferSizeSid = 64;
    DWORD bufferSizeDomain = 64;
    DWORD newBufferSizeSid = 64;
    DWORD newBufferSizeDomain = 64;
    WCHAR* pDomainName = NULL;
    SID_NAME_USE sidType;

    // Check SID pointer fo null before using 
    if(ppSid == NULL)
    {
        return DrError_Fail;
    }

    // Create buffers for the SID and domain name.
    *ppSid = (PSID) new BYTE[bufferSizeSid];
    if (*ppSid == NULL)
    {
        return DrError_Fail;
    }
    memset(*ppSid, 0, bufferSizeSid);

    pDomainName = new WCHAR[bufferSizeDomain];
    if (pDomainName == NULL)
    {
        FreeSid(*ppSid);
        return DrError_Fail;
    }
    memset(pDomainName, 0, bufferSizeDomain*sizeof(WCHAR));

    // Try to get SID with default buffer size
    if (LookupAccountNameW(NULL, domainUserName, *ppSid, &newBufferSizeSid, pDomainName, &newBufferSizeDomain, &sidType))
    {
        delete [] pDomainName;
            
        if (IsValidSid(*ppSid) == FALSE)
        {
            return DrError_Fail;
        }
        
        return DrError_OK;
    }

    // If unable to get account name, check for insufficient buffer
    DWORD err = GetLastError();
    while (err == ERROR_INSUFFICIENT_BUFFER)
    {
        if (newBufferSizeSid > bufferSizeSid)
        {
            // Free and reallocate buffer for SID
            FreeSid(*ppSid);
            *ppSid = (PSID) new BYTE[newBufferSizeSid];
            if (*ppSid == NULL)
            {
                delete [] pDomainName;
                return DrError_Fail;
            }

            bufferSizeSid = newBufferSizeSid;

            memset(*ppSid, 0, bufferSizeSid);
        }

        if (newBufferSizeDomain > bufferSizeDomain)
        {
            // Free and reallocate buffer for domain
            delete [] pDomainName;
            pDomainName = new WCHAR[newBufferSizeDomain];
            if (pDomainName == NULL)
            {
                FreeSid(*ppSid);        
                return DrError_Fail;
            }

            bufferSizeDomain = newBufferSizeDomain;

            memset(pDomainName, 0, bufferSizeDomain*sizeof(WCHAR));
        }

        // Try to get SID with new buffer size
        if (LookupAccountNameW(NULL, domainUserName, *ppSid, &bufferSizeSid, pDomainName, &bufferSizeDomain, &sidType))
        {
            delete [] pDomainName;
            
            if (IsValidSid(*ppSid) == FALSE)
            {        
                return DrError_Fail;
            }
        
            return DrError_OK;
        }
    
        err = GetLastError();
    } 

    // If outside loop, failed to lookup SID 
    return DrError_Fail;
}

DrError DrGetComputerName(WCHAR ppszValue[])
{
    
    WCHAR azureFlag[MAX_PATH+1] = {0};
    DrError err = DrGetEnvironmentVariable(L"CCP_ONAZURE", azureFlag);
    if(err == DrError_OK)
    {
        // This process is running on Azure        
        err = DrGetEnvironmentVariable(L"HPC_NODE_NAME", ppszValue); 
        if(err != DrError_OK)
        {
            DrLogE( "Error retrieving HPC_NODE_NAME environment variable. Error: %s", DrGetErrorText(err));
            return err;
        }
    }
    else
    {
        // This process is not running on Azure

        // swap with current lines for DNS hostname support
        //DWORD hostLength = DNS_MAX_LABEL_BUFFER_LENGTH;
        //if (!GetComputerNameExW(ComputerNameDnsHostname, ppszValue, &hostLength)) 

        DWORD hostLength = MAX_COMPUTERNAME_LENGTH + 1;
        if (!GetComputerName(ppszValue, &hostLength)) 
        {
            DrLogE( "Error calling GetComputerName. ErrorCode: %u", GetLastError());
            return DrError_Fail;
        }
    }

    return DrError_OK;
}

//JC
#if 0

DrError DrGetEnvironmentVariable(const char *pszVarName, /* out */ const char **ppszValue)
{
    // We jump through hoops to use unicode API here and convert to UTF-8

    DrError err;
    WCHAR *psz = NULL;
    DrWStr256 wstr;
    DrWStr64 wstrVarName;
    wstrVarName = pszVarName;
    DrStr256 str;
    char *pszRet = NULL;

    DWORD nb = GetEnvironmentVariableW(wstrVarName.GetString(), NULL, 0);

    if (nb == 0) {
        err = DrGetLastError();
        goto done;
    }

    psz = wstr.GetWritableBuffer(nb);

    DWORD nb2 = GetEnvironmentVariableW(wstrVarName.GetString(), psz, nb);
    if (nb2 == 0) {
        err = DrGetLastError();
        goto done;
    }
    LogAssert(nb2 < nb);
    wstr.UpdateLength(nb2);
    jstr.Set( wstr );
    LogAssert(str.GetString() != NULL);

    pszRet = (char *)malloc(str.GetLength()+1);
    LogAssert(pszRet != NULL);
    memcpy(pszRet, str.GetString(), str.GetLength()+1);

    err = DrError_OK;

done:
    if (err != DrError_OK) {
        if (pszRet != NULL) {
            free(pszRet);
        }
        *ppszValue = NULL;
    } else {
        LogAssert(pszRet != NULL);
        *ppszValue = pszRet;
    }

    return err;
}

// If pszBaseDir is null, the current working directory is used. If pszRelDir is
// fully qualified, pszBaseDir is ignored.
DrError DrCanonicalizeFilePath(DrStr& strOut, const char *pszRelDir, const char *pszBaseDir)
{
    DrWStr128 wstrRelDir;
    DrWStr128 wstrBaseDir;
    wstrRelDir = pszRelDir;
    wstrBaseDir = pszBaseDir;

    if (pszRelDir != NULL && !PathIsRelativeW(wstrRelDir)) {
        strOut = pszRelDir;
        return DrError_OK;
    }

    if (wstrBaseDir == NULL) {
        DrStr256 strBase;
        DrGetCurrentDirectory(strBase);
        wstrBaseDir = strBase;
    }

    WCHAR szBuff1[MAX_PATH];
    WCHAR szBuff2[MAX_PATH];

    if (wstrRelDir != NULL) {
        const WCHAR *pszResult = PathCombineW(
            szBuff1,
            wstrBaseDir,
            wstrRelDir);

        if (pszResult == NULL) {
            return DrError_InvalidPathname;
        }

        BOOL fRet = PathCanonicalizeW(szBuff2, szBuff1);
        if (!fRet) {
            return DrError_InvalidPathname;
        }
    } else {
        BOOL fRet = PathCanonicalizeW(szBuff2, wstrBaseDir);
        if (!fRet) {
            return DrError_InvalidPathname;
        }
    }

    strOut.Set(szBuff2);

    return DrError_OK;
}
#endif
