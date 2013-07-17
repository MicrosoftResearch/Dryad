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

#if defined(__cplusplus)
extern "C" {
#endif

typedef HRESULT DrError;

class DrStr;

extern DrStr& DrAppendErrorDescription(DrStr& strOut, DrError err);
//JC extern DrWStr& DrAppendErrorDescription(DrWStr& strOut, DrError err);

extern void DrInitErrorTable(void);
extern DrError DrAddErrorDescription(DrError code, const char *pszDescription);

// The returned error string should be freed with free();
//
// If the error code is unknown, a generic error description is returned.
extern char *DrGetErrorText(DrError err);

// The returned error string should be freed with free();
//
// If the error code is unknown, a generic error description is returned.
//JC extern WCHAR *DrGetErrorTextW(DrError err);

// The buffer must be at least 64 bytes long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
extern const char *DrGetErrorDescription(DrError err, char *pBuffer, int buffLen);

// The buffer must be at least 64 chars long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
//JC extern const WCHAR *DrGetErrorDescription(DrError err, WCHAR *pBuffer, int buffLen);

static const int k_DrMaxErrorLength = 256;

#ifndef FACILITY_DR
#define FACILITY_DR   0x309
#endif

#ifndef DR_ERROR
#define DR_ERROR(n) MAKE_HRESULT(SEVERITY_ERROR, FACILITY_DR, n)
#endif

#ifdef DEFINE_DR_ERROR
#undef DEFINE_DR_ERROR
#endif

#define DEFINE_DR_ERROR(name, number, description) static const DrError name = number;

#include "DrError.h"

#undef DEFINE_DR_ERROR

#if defined(__cplusplus)
}
#endif
