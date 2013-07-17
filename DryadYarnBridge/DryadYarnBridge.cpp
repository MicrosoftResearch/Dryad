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

#include "DryadYarnBridge.h"
#include "YarnAppMasterNative.h"
using namespace DryadYarn;

void * DrCreateNativeAppMaster()
{
    AMNativeInstance *instance = new AMNativeInstance();
    if (instance->OpenInstance())
    {
        return instance;
    } 
    return NULL;
}

void DrDestroyNativeAppMaster(void *ptr)
{
    AMNativeInstance *instance = (AMNativeInstance *)ptr; 
    delete instance;
}


char* DrGetExceptionMessage(void *ptr)
{
    AMNativeInstance *instance = (AMNativeInstance *)ptr; 
    return instance->GetExceptionMessage();
}

bool DrScheduleProcess(void *ptr, int vertexId, const char* name, const char* commandLine)
{
    AMNativeInstance *instance = (AMNativeInstance *)ptr; 
    return instance->ScheduleProcess(vertexId, name, commandLine);
}
