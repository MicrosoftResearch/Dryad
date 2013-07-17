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

#ifndef _DrExecution_h_
#define  _DrExecution_h_

#pragma once


/*
    DrExecution.h - main header file for Dryad execution
*/

#include "DrCommon.h"

//JC#include "DrFileCache.h"
//JC#include "DrPipe.h"
//JC#include "DrPipeLoopback.h"
//JC#include "DrNativeHandlePipe.h"
//JC#include "DrTcpPipe.h"
//JC#include "DrPipeRendezvous.h"
//JC#include "DrJobStatistics.h"


//JC#define DR_ENVSTR_PN_QUOTA "PN_QUOTA"

DrError DrInitExecution();

class DrExecutionEnvironment
{
    friend DrError DrInitExecution();

public:

    void Lock()
    {
        m_cs.Enter();
    }

    void Unlock()
    {
        m_cs.Leave();
    }

	/* JC
    void GetCurrentJobDescriptor(DrJobDescriptorEx& jdOut)
    {
        Lock();
        jdOut = g_pDryadConfig->CurrentJobDescriptor();
        Unlock();
    }

    const DrProcessDescriptor& GetCurrentProcessDescriptor()
    {
        return g_pDryadConfig->GetCurrentProcessDescriptor();
    }

    const DrProcessDescriptor& GetRootProcessDescriptor()
    {
        return g_pDryadConfig->GetRootProcessDescriptor();
    }

    bool IsUnderProcessNode()
    {
        return g_pDryadConfig->IsUnderProcessNode();
    }

    bool JobWasInherited()
    {
        return g_pDryadConfig->JobWasInherited();
    }
*/
private:
    DrExecutionEnvironment()
    {
    }

    DrError Initialize();

private:
    DrCriticalSection m_cs;
};

extern DrExecutionEnvironment *g_pDryadExecution;

#endif // end if not defined  _DrExecution_h_
