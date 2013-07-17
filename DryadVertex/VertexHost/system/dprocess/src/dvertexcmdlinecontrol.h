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
#include <dvertexcommand.h>
#include <dryadvertex.h>
#include <vertexfactory.h>

class DVertexCmdLineController : public DryadVertexController
{
public:
    UInt32 Run(int argc, char* argv[],
               DryadVertexFactoryBase* factory,
               bool useExplicitCmdLine);
    void AssimilateNewStatus(DVertexProcessStatus* status,
                             bool sendUpdate, bool notifyWaiters);

private:
    void GetURI(DrStr* dst, const char* channel);
    int GetChannelDescriptions(int argc, char* argv[],
                               DVertexProcessStatus* initialState,
                               bool isInput);
    int GetChannelOverride(int argc, char* argv[],
                           DVertexProcessStatus* initialState,
                           bool isInput);
    int GetTextOverride(int argc, char* argv[],
                        DVertexCommandBlock* startCommand);
    int RestoreDumpedStartCommand(int argc, char* argv[],
                                  DVertexCommandBlock* startCommand);
    DrError ParseImplicitCmdLine(int argc, char* argv[],
                                 DVertexCommandBlock* startCommand);
    void ParseExplicitCmdLine(int argc, char* argv[],
                              DVertexCommandBlock* startCommand);
};
