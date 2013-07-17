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

#include "dvertexpncontrol.h"

class DVertexXComputePnControllerOuter;

class DVertexYarnPnController : public DVertexPnController
{
public:
    DVertexYarnPnController(DVertexPnControllerOuter* parent,
                                UInt32 vertexId, UInt32 vertexVersion);

    void SendSetStatusRequest(DryadPnProcessPropertyRequest* r);

private:
    DryadPnProcessPropertyRequest*
        MakeSetStatusRequest(UInt32 exitOnCompletion,
                             bool isAssert,
                             bool notifyWaiters);
    unsigned CommandLoop();

    //
    // Get wrapper
    //
    DVertexXComputePnControllerOuter* GetParent();
};

//
// Wrapper for PN controller and environment
// Used by dvertextmain.cpp:DryadVertexMain
//
class DVertexYarnPnControllerOuter : public DVertexPnControllerOuter
{
private:
    DVertexEnvironment* MakeEnvironment();
    DVertexPnController* MakePnController(UInt32 vertexId,
                                          UInt32 vertexVersion);
};
