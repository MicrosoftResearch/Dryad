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

#include <dryadxcomputeresources.h>
#include <DrStringUtil.h>

#pragma unmanaged

//
// Constructor - store process handle. Set guid to process id.
//
DryadXComputeProcessIdentifier::
    DryadXComputeProcessIdentifier(XCPROCESSHANDLE handle)
{
    m_handle = handle;
    ReCacheGUID();
}

//
// Sets guid to xcompute process id
//
void DryadXComputeProcessIdentifier::ReCacheGUID()
{
    GUID id;

    //
    // Get the process ID from xcompute layer
    //
    XCERROR err = XcGetProcessId(m_handle, &id);
    if (err == DrError_OK)
    {
        //
        // If process ID found, store ID and string version of ID as guids
        //
        m_guid = id;
        m_guidString = DRGUIDSTRING(id);
    }
    else
    {
        //
        // If process ID not found, use null.
        // todo: is this fatal? If so, log and fail.
        //
        m_guid.SetToNull();
        m_guidString = "{No Guid}";
    }
}

//
// Return handle to process
//
XCPROCESSHANDLE DryadXComputeProcessIdentifier::GetHandle()
{
    return m_handle;
}

//
// Return GUID
//
DrGuid* DryadXComputeProcessIdentifier::GetGuid()
{
    return &m_guid;
}

//
// Return string version of GUID
//
const char* DryadXComputeProcessIdentifier::GetGuidString()
{
    return m_guidString;
}

//
// 
//
void DryadXComputeProcessIdentifier::
    MakeURIForRelativeFile(DrStr* dst,
                           const char* baseDirectory,
                           const char* relativeFileName)
{
    //
    // Combind base directory and relative file name unless base directory is not supplied
    //
    DrStr256 fullPath;
    if (baseDirectory == NULL)
    {
        fullPath.SetF("wd/%s",relativeFileName);
    }
    else
    {
        fullPath.SetF("wd/%s/%s", baseDirectory, relativeFileName);
    }

    //
    // Get URI relative to process following xcompute access rules
    // todo: ask why this matters one way or the other
    //
    char* uri = NULL;
    XCERROR err = XcGetProcessUri(m_handle, fullPath, &uri);
    if (err == DrError_OK)
    {
        //
        // If return successful, use the URI
        //
        dst->Set(uri);
    }
    else
    {
        //
        // If the return isn't successful, just use the full path
        //
        dst->Set(fullPath);
    }

    //
    // Free URI if created
    //
    if (uri != NULL)
    {
        XcFreeMemory(uri);
    }
}

DryadXComputeMachineIdentifier::
    DryadXComputeMachineIdentifier(XCPROCESSNODEID node)
{
    m_node = node;
}

XCPROCESSNODEID DryadXComputeMachineIdentifier::GetNodeID()
{
    return m_node;
}
