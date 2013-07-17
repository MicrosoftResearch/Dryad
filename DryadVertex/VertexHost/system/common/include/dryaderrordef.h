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

#include <DrCommon.h>

#define FACILITY_DRYAD   778
#define DRYAD_ERROR(n) ((HRESULT)(0x80000000 + (FACILITY_DRYAD << 16) + n))

#ifdef DEFINE_DRYAD_ERROR
#undef DEFINE_DRYAD_ERROR
#endif

#define DEFINE_DRYAD_ERROR(name, number, description) static const DrError name = number;
#include "DryadError.h"

#undef DEFINE_DRYAD_ERROR
