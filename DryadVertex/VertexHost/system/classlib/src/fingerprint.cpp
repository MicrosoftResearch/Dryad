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

#include "fingerprint.h"


#pragma unmanaged

UInt64 FingerPrint64::FingerPrint64Init::count;
FingerPrint64* FingerPrint64::instance;

FingerPrint64::FingerPrint64Init::FingerPrint64Init()
{
    if (0 == count)
    {
        FingerPrint64::Init();
    }
    ++count;
}

FingerPrint64::FingerPrint64Init::~FingerPrint64Init()
{
//JC    assert(count > 0);
    --count;
    if (0 == count)
    {
        FingerPrint64::Dispose();
    }
}

void FingerPrint64::Init()
{
    FingerPrint64::instance = new FingerPrint64();
}

void FingerPrint64::Dispose()
{
    delete FingerPrint64::instance;
    FingerPrint64::instance = 0;
}

FingerPrint64* FingerPrint64::GetInstance()
{
    return FingerPrint64::instance;    
}

UInt64 FingerPrint64::GetFingerPrint(const void *data, const size_t length)
{
    return ms_fprint_of(this->fp, (void*) data, (size_t) length);
}

FingerPrint64::FingerPrint64(UInt64 poly)
{
    this->fp = ::ms_fprint_new(poly);
}

FingerPrint64::FingerPrint64(void)
{
    this->fp = ::ms_fprint_new();
}

FingerPrint64::~FingerPrint64(void)
{
    ::ms_fprint_destroy(this->fp);
}
