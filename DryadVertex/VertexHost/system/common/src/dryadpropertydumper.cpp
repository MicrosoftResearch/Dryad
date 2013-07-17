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
#include <dryadpropertydumper.h>
#include <dryadpropertiesdef.h>
#include <dryadtagsdef.h>
#include <dryaderrordef.h>
#include <string.h>

#pragma unmanaged

const char* g_dVertexCommandText[DVertexCommand_Max] = {
    "Start",
    "ReOpenChannels",
    "Terminate"
};

//JC
#if 0
typedef DrError (*PropertyConverter)(DrPropertyDumper *pDumper, UInt16 enumId, const char *propertyName);

typedef struct {
    UInt16 value;
    const char *pszDescription;
    PropertyConverter pConverter;
} PropEntry;

static PropEntry g_DryadPropertyMap[] = {

#ifdef DEFINE_DRPROPERTY
#undef DEFINE_DRPROPERTY
#endif

#ifdef DEFINE_DRYADPROPERTY
#undef DEFINE_DRYADPROPERTY
#endif

#define DEFINE_DRPROPERTY(name, number, type, description) {name, description, DrPropertyToText_##type},

#define DEFINE_DRYADPROPERTY(name, number, type, description) {name, description, DryadPropertyToText_##type},

#include "dryadproperties.h"

#undef DEFINE_DRPROPERTY
#undef DEFINE_DRYADPROPERTY
};



typedef struct {
    UInt16 value;
    const char *pszDescription;
} TagEntry;

static TagEntry g_DryadTagMap[] = {

#ifdef DEFINE_DRYADTAG
#undef DEFINE_DRYADTAG
#endif

#define DEFINE_DRYADTAG(name, number, description, type) {name, description},

#include "dryadtags.h"

#undef DEFINE_DRYADTAG
};


typedef struct {
    DrError value;
    const char *pszDescription;
} ErrEntry;

static ErrEntry g_DryadErrorMap[] = {
#ifdef DEFINE_DRYAD_ERROR
#undef DEFINE_DRYAD_ERROR
#endif

#define DEFINE_DRYAD_ERROR(name, number, description) {name, description},

#include "dryaderror.h"

#undef DEFINE_DRYAD_ERROR
};


DrError DryadPropertyToText_Void(DrPropertyDumper *pDumper,
                                 UInt16 enumId,
                                 const char *propertyName)
{
    UInt16 actualEnumId;
    UInt32 length;
    DrError err = pDumper->GetReader()->ReadNextPropertyTag(&actualEnumId, &length);
    if (err == DrError_OK) {
        if (enumId != actualEnumId || length != 0) {
            err = pDumper->GetReader()->SetStatus(DrError_InvalidProperty);
        } else {
            err = pDumper->WriteSimpleTagValue(propertyName, "Void");
        }
    }
    return err;
}

DrError DryadPropertyToText_VertexCommand(DrPropertyDumper *pDumper,
                                          UInt16 enumId,
                                          const char *propertyName)
{
    char descBuffer[100];
    const char* desc;
    UInt32 val;
    DrError err = pDumper->GetReader()->ReadNextUInt32Property(enumId, &val);
    if (err == DrError_OK) {
        if (val < DVertexCommand_Max)
        {
            desc = g_dVertexCommandText[val];
        }
        else
        {
            HRESULT hr = ::StringCbPrintfA(descBuffer, sizeof(descBuffer),
                                           "Unknown state %u", val);
            LogAssert(SUCCEEDED(hr));
            desc = descBuffer;
        }

        err = pDumper->WriteSimpleTagValue(propertyName, desc);
    }
    return err;
}

void DryadInitErrorTable()
{
    UInt32 n = sizeof(g_DryadErrorMap) / sizeof(g_DryadErrorMap[0]);
    UInt32 i;
    for (i=0; i<n; ++i)
    {
        DrError err =
            DrAddErrorDescription(g_DryadErrorMap[i].value,
                                  g_DryadErrorMap[i].pszDescription);
        LogAssert(err == DrError_OK);
    }
}

void DryadInitPropertyTable()
{
    UInt32 n = sizeof(g_DryadPropertyMap) / sizeof(g_DryadPropertyMap[0]);
    UInt32 i;
    for (i=0; i<n; ++i)
    {
        DrError err =
            DrAddPropertyToDumper(g_DryadPropertyMap[i].value,
                                  g_DryadPropertyMap[i].pszDescription,
                                  g_DryadPropertyMap[i].pConverter);
        LogAssert(err == DrError_OK);
    }
}

void DryadInitTagTable()
{
    UInt32 n = sizeof(g_DryadTagMap) / sizeof(g_DryadTagMap[0]);
    UInt32 i;
    for (i=0; i<n; ++i)
    {
        DrError err =
            DrAddTagToDumper(g_DryadTagMap[i].value,
                             g_DryadTagMap[i].pszDescription);
        LogAssert(err == DrError_OK);
    }
}
#endif // if 0
