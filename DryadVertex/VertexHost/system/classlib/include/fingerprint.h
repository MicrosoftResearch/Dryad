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
#include "ms_fprint.h"

class FingerPrint64
{
public:
    static FingerPrint64* GetInstance();

    UInt64 GetFingerPrint(
        const void* data,
        const size_t length
        );

    class FingerPrint64Init 
    {
    public:
        FingerPrint64Init();
        ~FingerPrint64Init();
    private:
        static UInt64 count;
    };
    static void Init();
    static void Dispose();
    FingerPrint64(UInt64 poly);
    FingerPrint64();
    ~FingerPrint64();
private:
    ms_fprint_data_t fp;
    static FingerPrint64* instance;
};

static FingerPrint64::FingerPrint64Init fpInit;
