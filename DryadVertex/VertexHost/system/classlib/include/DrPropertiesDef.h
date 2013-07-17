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

class DrPropertyDumper;

#ifdef DECLARE_DRPROPERTYTYPE
#undef DECLARE_DRPROPERTYTYPE
#endif

#define DECLARE_DRPROPERTYTYPE(type) \
    extern DrError DrPropertyToText_##type(DrPropertyDumper *pDumper, UInt16 enumId, const char *propertyName);

#include "DrPropertyType.h"

#undef DECLARE_DRPROPERTYTYPE


#ifdef DEFINE_DRPROPERTY
#undef DEFINE_DRPROPERTY
#endif

#define DEFINE_DRPROPERTY(var, value, type, propertyName) \
    static const UInt16 var = value;

#include "DrProperties.h"

#undef DEFINE_DRPROPERTY


// This is a special value for an offset meaning unknown
const UInt64 DrStreamOffset_Unknown = 0xFFFFFFFFFFFFFFFF;

// This is a special value for an extent offset meaning unknown
// Note that -1 is used to mean an invalid offset
const UInt64 DrExtentOffset_Unknown = (UInt64 ) -2;     //$TODO(DanielD) - for consistency (and logging, etc - see SamMck) swap values to have DrExtentOffset_Unknown==-1
const UInt64 DrExtentOffset_Invalid = (UInt64 ) -1;

const UInt64 DrExtentLength_Invalid = (UInt64) -1;

//Flags for Prop_Dryad_PublishedCrc64
const UInt64 DrExtentCrc64_Suspect = (UInt64)-1;
