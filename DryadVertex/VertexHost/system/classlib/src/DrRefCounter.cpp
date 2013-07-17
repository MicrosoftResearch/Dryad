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
#include <windows.h>

#pragma unmanaged

static volatile LONGLONG g_DrRefUniqueObjectCounter = 0;


#if defined(_AMD64_)

UInt64 GetUniqueObjectID()
{
    LONGLONG value = ::InterlockedIncrement64(&g_DrRefUniqueObjectCounter);
    return (UInt64) value;
}

#else

// We don't use non-x64 builds, but for completeness sake we will generate a 
// a 32 bit version. The downside is that unique object IDs would wrap around at 4 Billion if this build were used,
// but even that isn't a problem because we'll never have so many active object at a given time.
UInt64 GetUniqueObjectID()
{
    int value = ::InterlockedIncrement((volatile LONG*) &g_DrRefUniqueObjectCounter);
    return (UInt64) value;
}

#endif