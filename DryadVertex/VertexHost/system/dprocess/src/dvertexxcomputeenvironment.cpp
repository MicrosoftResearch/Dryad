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

#include <dvertexxcomputeenvironment.h>
#include <dryadxcomputeresources.h>
#include <dryadstandaloneini.h>
#include <XCompute.h>

//
// Use current environment to initialize xcompute environment
//
DrError DVertexXComputeEnvironment::InitializeFromEnvironment()
{
    //
    // Get process handle from dryad standalone ini
    //
    XCPROCESSHANDLE handle = GetProcessHandle();
    if (handle == INVALID_XCPROCESSHANDLE)
    {
        DrLogE("XCompute Process Handle is invalid"); 
        return DrError_Fail;
    }

    //
    // Build an identifier around process handle and attach to it
    //
    DryadXComputeProcessIdentifier* process = new DryadXComputeProcessIdentifier(handle);

    m_process.Attach(process);

    //
    // Get node running xcompute process. Fail if invalid
    //
    XCPROCESSNODEID node;
    XCERROR err = XcGetProcessNodeId(process->GetHandle(), &node);
    LogAssert(err == DrError_OK);
    LogAssert(node != INVALID_XCPROCESSNODEID);

    //
    // attach to machine
    //
    m_machine.Attach(new DryadXComputeMachineIdentifier(node));

    return DrError_OK;
}
