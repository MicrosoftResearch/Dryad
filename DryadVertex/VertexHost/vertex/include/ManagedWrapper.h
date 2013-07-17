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

#include <dryadvertex.h>
#include <vertexfactory.h>
#include <DataBlockItem.h>
#include <mscoree.h>

class ManagedWrapperVertex : public DryadVertexProgram
{
public:
    ManagedWrapperVertex();
    
    void Main(WorkQueue* workQueue,
              UInt32 numberOfInputChannels,
              RChannelReader** inputChannel,
              UInt32 numberOfOutputChannels,
              RChannelWriter** outputChannel);

private:
    static ICLRRuntimeHost *pClrHost;          // we only need one per process
    static DrCriticalSection m_atomic;          // loading the CLR should be atomic
    
    // Use DryadLinqLog to log messages from the DryadLinqRuntime
    // These messages will be send both to the logging infrastructure, but also to the vertex standard error.
    void DryadLinqLog(LogLevel level, const char* title, const char* fmt, va_list args);
};

typedef StdTypedVertexFactory<ManagedWrapperVertex> FactoryMWrapper;

