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

#ifdef DECLARE_DRYADPROPERTYTYPE
#undef DECLARE_DRYADPROPERTYTYPE
#endif

#define DECLARE_DRYADPROPERTYTYPE(type) \
    extern DrError DryadPropertyToText_##type(DrPropertyDumper *pDumper, UInt16 enumId, const char *propertyName);

#include "dryadpropertytype.h"

#undef DECLARE_DRYADPROPERTYTYPE

#ifdef DEFINE_DRPROPERTY
#undef DEFINE_DRPROPERTY
#endif

#ifdef DEFINE_DRYADPROPERTY
#undef DEFINE_DRYADPROPERTY
#endif

#define DEFINE_DRPROPERTY(var, value, type, propertyName) \
    static const UInt16 var = value;

#define DEFINE_DRYADPROPERTY(var, value, type, propertyName) \
    static const UInt16 var = value;

#include "dryadproperties.h"

#undef DEFINE_DRPROPERTY
#undef DEFINE_DRYADPROPERTY



// Options for VertexCommand in a DVertexCommand message
enum DVertexCommand {
    DVertexCommand_Start = 0,
    DVertexCommand_ReOpenChannels,
    DVertexCommand_Terminate,
    DVertexCommand_Max
};
extern const char* g_dVertexCommandText[DVertexCommand_Max];
