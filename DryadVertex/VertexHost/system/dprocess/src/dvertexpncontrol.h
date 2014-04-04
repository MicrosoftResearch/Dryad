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

#include <dvertexcommand.h>
#include <dryadvertex.h>

class DVertexPnController;
class DVertexPnControllerOuter;

//
// Structure for storing controller/commandblock association
//
struct DVertexPnControllerThreadBlock
{
    DVertexPnController*          m_parent;
    DrRef<DVertexCommandBlock>    m_commandBlock;
};

class DVertexPnController : public DryadVertexController
{
public:
    DVertexPnController(DVertexPnControllerOuter* parent,
                        UInt32 vertexId, UInt32 vertexVersion);
    virtual ~DVertexPnController();

    void AssimilateNewStatus(DVertexProcessStatus* status,
                             bool sendUpdate, bool notifyWaiters);
    void LaunchCommandLoop();
    void SendAssertStatus(const char* assertString);

protected:
    void SetChannelStatus(DryadChannelDescription* dst,
                          DryadChannelDescription* src);
    void AssimilateChannelStatus(DryadChannelDescription* dst,
                                 DryadChannelDescription* src);

    void SendStatus(UInt32 exitOnCompletion, bool notifyWaiters);
    void Start(DVertexCommandBlock* commandBlock);
    void ReOpenChannels(DVertexCommandBlock* reOpenCommand);
    void Terminate(DrError vertexState, UInt32 exitCode);
    DrError ActOnCommand(DVertexCommandBlock* commandBlock);
    static unsigned ThreadFunc(void* arg);
    static unsigned CommandLoopStatic(void* arg);
    void DumpRestartCommand(DVertexCommandBlock* commandBlock);

    virtual DryadPnProcessPropertyRequest*
        MakeSetStatusRequest(UInt32 exitOnCompletion,
                             bool isAssert,
                             bool notifyWaiters) = 0;
    virtual void SendSetStatusRequest(DryadPnProcessPropertyRequest* r) = 0;
    virtual unsigned CommandLoop() = 0;

    DVertexPnControllerOuter*  m_parent;
    UInt32                     m_vertexId;
    UInt32                     m_vertexVersion;
    DryadVertexRef             m_vertex;
    UInt64                     m_currentCommandVersion;
    bool                       m_activeVertex;
    bool                       m_waitingForTermination;
    DrRef<DVertexStatus>       m_currentStatus;
    CRITSEC                    m_baseCS;
};

class DVertexPnControllerOuter
{
public:
    DVertexPnControllerOuter();

    UInt32 Run(int argc, char* argv[]);
    void VertexExiting(int exitCode);
    const char* GetRunningExePathName();

private:
    void SendAssertStatus(const char* assertString);
    static void AssertCallback(void* cookie,
                               const char* assertString);

    virtual DVertexPnController* MakePnController(UInt32 vertexId,
                                                  UInt32 vertexVersion) = 0;

    DVertexPnController**  m_controllerArray;
    volatile LONG          m_assertCounter;
    UInt32                 m_numberOfVertices;
    UInt32                 m_activeVertexCount;
    DrStr64                m_exePathName;
    CRITSEC                m_baseCS;
};
