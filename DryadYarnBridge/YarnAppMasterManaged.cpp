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

#pragma managed
#include "YarnAppMasterNative.h"
#include "YarnAppMasterManaged.h"

#include <string>
#include <iostream>
#include <vcclr.h>
using namespace System;
using namespace System::Text;
using namespace System::IO;
using namespace DryadYarn;

namespace Microsoft { namespace Research { namespace Dryad { namespace YarnBridge
{
    AMInstance::AMInstance()
    {
        AMNativeInstance *instance = new AMNativeInstance();

        if (instance->OpenInstance())
        {
            m_instance = IntPtr(instance);
        } 
        else
        {
            m_instance = IntPtr::Zero;
            throw gcnew ApplicationException("Unable to initialize Yarn Native App Master Instance");
        }
    }

    AMInstance::~AMInstance()
    {
        Close();
    }

    void AMInstance::Close()
    {
        if (m_instance != IntPtr::Zero)
        {
           AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
           delete instance;
           m_instance = IntPtr::Zero;
        }
    }

    void AMInstance::Finish()
    {
        if (m_instance != IntPtr::Zero)
        {
            AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
            instance->Shutdown();
        }
    }

    int AMInstance::GetHealthyNodeCount()
    {
        if (m_instance != IntPtr::Zero)
        {
            AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
            //NYI 
            return 1;
        }
        return -1;
    }

    void AMInstance::ScheduleProcess(int vertexId, const char* name, const char* cmdLine)
    {
        AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
        bool result = instance->ScheduleProcess(vertexId, name, cmdLine);

        if (!result) 
        {
            throw gcnew ApplicationException("Unable to schedule process");
        }
    }

    void AMInstance::ScheduleProcess(int vertexId, String^ name, String^ cmdLine)
    {
		Console::WriteLine("Scheduling process: Vertex ID: {0}, Name: '{1}', Command Line: '{2}'", vertexId, name, cmdLine);
        IntPtr namePtr = Marshal::StringToHGlobalAnsi(name);
        const char* nameString = static_cast<const char*>(namePtr.ToPointer());
        IntPtr cmdLinePtr = Marshal::StringToHGlobalAnsi(cmdLine);
        const char* cmdLineString = static_cast<const char*>(cmdLinePtr.ToPointer());
        
        AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
        bool result = instance->ScheduleProcess(vertexId, nameString, cmdLineString);
        
        Marshal::FreeHGlobal(namePtr);
        Marshal::FreeHGlobal(cmdLinePtr);


        if (!result) 
        {
            throw gcnew ApplicationException("Unable to schedule process");
        }
    }

    void AMInstance::UpdateProcess(int vertexId, int state, String^ nodeName)
    {      
		Console::Error->WriteLine("Calling GM Callback: Vertex Id: {0} State: {1} NodeName: '{2}'", vertexId, state, nodeName);
        m_gmCallback(vertexId, state, nodeName);
    }

    void AMInstance::RegisterGMCallback(UpdateProcessState^ callback)
    {
        m_gmCallback = callback;
    }

}}}}
