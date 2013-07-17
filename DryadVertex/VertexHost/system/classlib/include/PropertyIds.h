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

// property type flag
const UInt16 PropTypeMask                       = 0xc000;

// PropType_Atom is a leaf property which is an element of a list or a
// set. There may be nested properties within the leaf.
const UInt16 PropType_Atom                      = 0x0000;

// length type flag
const UInt16 PropLengthMask                     = 0x2000;

// A property with PropLength_Short has a 1-byte length field
const UInt16 PropLength_Short                   = 0x0000;
// A property with PropLength_Long has a 4-byte length field
const UInt16 PropLength_Long                    = 0x2000;

// mask for the remaining 13-bit namespace

const UInt16 PropValueMask                      = 0x1fff;

#define PROP_SHORTATOM(x_) ((x_) | PropType_Atom | PropLength_Short)
#define PROP_LONGATOM(x_) ((x_) | PropType_Atom | PropLength_Long)

// Propries for Dryad
const UInt16 Prop_Stream_BeginTag          = PROP_SHORTATOM(0x1200);
const UInt16 Prop_Stream_EndTag            = PROP_SHORTATOM(0x1201);
