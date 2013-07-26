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
#pragma unmanaged

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

//---------------------------------------------------------------------------------------------------

namespace DryadYarn
{
	struct Instance
	{
		void* p;
	};
	class InstanceInternal;

	struct Env;
	bool Initialize();

	class InstanceAccessor
	{
	public:
	    InstanceAccessor(Instance* instance);
	    ~InstanceAccessor();
	    
	    void Dispose();
	    
	    char* GetExceptionMessage();
	    
	    bool ScheduleProcess(long vertexId, const char* name, const char* commandLine);
	    
	private:
	    //void* operator new( size_t );
	    //void* operator new[]( size_t );
	    
	    Env*                m_env;
	    InstanceInternal*   m_inst;
	};

	bool OpenInstance(Instance** pInstance);

    class AMNativeInstance
	{
    public:
        AMNativeInstance();
        ~AMNativeInstance();
        bool OpenInstance();
	    
	    char* GetExceptionMessage();
	    
	    bool ScheduleProcess(int vertexId, const char* name, const char* commandLine);
		bool Shutdown(bool success);

    private:
	    //void* operator new( size_t );
	    //void* operator new[]( size_t );
	    
	    Env*                m_env;
	    InstanceInternal*   m_inst;
	};

};
//---------------------------------------------------------------------------------------------------

