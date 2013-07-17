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

/* this is an opaque class that identifies a process independent of
   cluster type */
class DryadProcessIdentifier : public DrRefCounter
{
public:
    virtual ~DryadProcessIdentifier();

    virtual DrGuid* GetGuid() = 0;
    virtual const char* GetGuidString() = 0;

    virtual void MakeURIForRelativeFile(DrStr* dst,
                                        const char* baseDirectory,
                                        const char* relativeFileName) = 0;
};

/* this is an opaque class that identifies a machine independent of
   cluster type */
class DryadMachineIdentifier : public DrRefCounter
{
public:
    virtual ~DryadMachineIdentifier();
};
