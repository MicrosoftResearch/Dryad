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

#pragma managed

//
// Wrapper for PN controller and environment
// Used by dvertextmain.cpp:DryadVertexMain
//
class DVertexHttpPnControllerOuter : public DVertexPnControllerOuter
{
private:
    DVertexPnController* MakePnController(UInt32 vertexId,
                                          UInt32 vertexVersion);
};

class DVertexHttpPnController : public DVertexPnController
{
public:
    DVertexHttpPnController(DVertexPnControllerOuter* parent,
                            UInt32 vertexId, UInt32 vertexVersion, const char* serverAddress);
    ~DVertexHttpPnController();

    void SendSetStatusRequest(DryadPnProcessPropertyRequest* r);
    void ConsiderNextSendRequest(DryadPnProcessPropertyRequest* r);

    HANDLE GetCommandLoopEvent();

private:
    void SendSetStatusRequestInternal(DryadPnProcessPropertyRequest* r);
    DryadPnProcessPropertyRequest*
        MakeSetStatusRequest(UInt32 exitOnCompletion,
                             bool isAssert,
                             bool notifyWaiters);
    unsigned CommandLoop();

    bool                                  m_sending;
    DrRef<DryadPnProcessPropertyRequest>  m_nextSend;
    DrStr64                               m_serverAddress;
    HANDLE                                m_commandLoopEvent;
};
