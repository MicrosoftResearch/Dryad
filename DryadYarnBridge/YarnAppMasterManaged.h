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

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;

namespace Microsoft { namespace Research { namespace Dryad { namespace YarnBridge
{

    public delegate void UpdateProcessState(int vertexId, int state, String^ nodeName);

    public ref class AMInstance : public IDisposable
    {
    public:
        AMInstance();
        ~AMInstance();


        void Close();
        void Finish(bool success);

        int GetHealthyNodeCount();

        void ScheduleProcess(int vertexId, const char* name, const char* cmdLine);
        void ScheduleProcess(int vertexId, String^ name, String^ cmdLine);

		static void UpdateProcess(int vertexId, int state, String^ nodeName);
        static void RegisterGMCallback(UpdateProcessState^ callback);

    private:
        IntPtr                      m_instance;
        static UpdateProcessState^  m_gmCallback;
    };
}}}}
