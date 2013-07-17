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

#include "DrExecution.h"

DrExecutionEnvironment *g_pDryadExecution = NULL;

//JC
#if 0
//JC#define SECURITY_WIN32
//JC#define SEC_SUCCESS(Status) ((Status) >= 0)
//JC#include <security.h>


DrError DrInitExecution()
{
    LogAssert(g_pDrExecution == NULL);
    g_pDrExecution  = new DrExecutionEnvironment();
    LogAssert(g_pDrExecution != NULL);
    DrError err = g_pDrExecution->Initialize();
    if (err != DrError_OK) {
        delete g_pDrExecution;
        g_pDrExecution = NULL;
    }
    return err;
}

DrError DrExecutionEnvironment::Initialize()
{
//JC    return DrInitPipeManager();
	return DrError_OK;
}
#endif
