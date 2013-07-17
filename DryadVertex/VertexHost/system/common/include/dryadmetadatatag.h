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

class DryadMetaData;
class DryadMetaDataConst;
class DrMemoryWriter;

class DryadMTag;
typedef DrRef<DryadMTag> DryadMTagRef;

class DryadMTag : public DrRefCounter
{
public:
    UInt16 GetTagValue();
    UInt16 GetType();

    virtual DrError Serialize(DrMemoryWriter* writer) = 0;

    /* the default implementation is for immutable tags and simply
       increments the reference count and returns self */
    virtual void Clone(DryadMTagRef* dstTag);

protected:
    DryadMTag(UInt16 tagValue, UInt16 type);
    virtual ~DryadMTag();

private:
    UInt16             m_tag;
    UInt16             m_type;
};

enum DrPropertyTagEnum {

#ifdef DECLARE_DRPROPERTYTYPE
#undef DECLARE_DRPROPERTYTYPE
#endif

#define DECLARE_DRPROPERTYTYPE(type) DrPropertyTagType_##type,

#include "DrPropertyType.h"

#undef DECLARE_DRPROPERTYTYPE
};

enum DryadPropertyTagEnum {

    DryadPropertyTagType_MetaData = 0x1000,
    DryadPropertyTagType_InputChannelDescription,
    DryadPropertyTagType_OutputChannelDescription,
    DryadPropertyTagType_VertexProcessStatus,
    DryadPropertyTagType_VertexStatus,
    DryadPropertyTagType_VertexCommandBlock,

#ifdef DECLARE_DRYADPROPERTYTYPE
#undef DECLARE_DRYADPROPERTYTYPE
#endif

#define DECLARE_DRYADPROPERTYTYPE(type) DryadPropertyTagType_##type,

#include "DryadPropertyType.h"

#undef DECLARE_DRYADPROPERTYTYPE
};
