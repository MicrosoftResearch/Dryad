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

#include "managedchannel.h"
#include "..\..\common\include\drstring.h"

#pragma managed

using namespace System::Collections::Generic;

using namespace Microsoft::Research::Dryad::Channel;

ref class MCritSec
{
public:
    void Enter();
    void Leave();
};


ref class MAutoCriticalSection
{
public:
    MAutoCriticalSection(MCritSec^ critSec);
    ~MAutoCriticalSection();

private:
    MCritSec^ m_critSec;
};

typedef int MCHandle;

ref class ManagedHandleStore
{
public:
    static ManagedHandleStore();

    static MCHandle Create(Object^ o);
    static Object^ Get(MCHandle h);
    static void Free(MCHandle h);

private:
    static MCritSec^                      s_cs;
    static MCHandle                       s_next;
    static Dictionary<MCHandle, Object^>^ s_store;
};

ref class ManagedChannelBridge : public IDrLogging
{
public:
    virtual void LogAssertion(System::String^ message, System::String^ file, System::String^ function, int line);
    virtual void LogError(System::String^ message, System::String^ file, System::String^ function, int line);
    virtual void LogWarning(System::String^ message, System::String^ file, System::String^ function, int line);
    virtual void LogInformation(System::String^ message, System::String^ file, System::String^ function, int line);

private:
    void LogWithType(LogLevel type, System::String^ message, System::String^ file, System::String^ function, int line);
};

class ManagedHelpers
{
public:
    static RChannelItem* MakeErrorItem(DrError errorCode, const char* description);
    static RChannelBuffer* MakeErrorBuffer(DrError errorCode, const char* description,
                                           RChannelBufferDefaultHandler* handler);
    static RChannelItem* MakeEndOfStreamItem();
    static RChannelBuffer* MakeEndOfStreamBuffer(RChannelBufferDefaultHandler* handler);
    static RChannelBufferData* MakeDataBuffer(UInt64 streamOffset, size_t blockSize,
                                              RChannelBufferDefaultHandler* handler);
};