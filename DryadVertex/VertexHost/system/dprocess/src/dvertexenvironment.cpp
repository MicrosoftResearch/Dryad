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

#include <DrExecution.h>
#include <dvertexenvironment.h>

//
// Constructor - set defaults
// no process or machine info
// quota, failure before abort, and failure threshold all set to maxint
//
DVertexEnvironment::DVertexEnvironment()
{
    m_process = NULL;
    m_machine = NULL;
    m_pnQuota = (UInt32)-1;
    m_minNumberOfFailuresBeforeAbort = (UInt32)-1;
    m_maxFailureThreshold = (UInt32)-1;
}

//
// Destructor - do nothing
// todo: does m_process and m_machine need to be freed?
//
DVertexEnvironment::~DVertexEnvironment()
{
}

//
// Get the process info
//
DryadProcessIdentifier* DVertexEnvironment::GetPNProcess()
{
    return m_process;
}

//
// Get the machine info
//
DryadMachineIdentifier* DVertexEnvironment::GetPNMachine()
{
    return m_machine;
}

//
// Get the quota
//
UInt32 DVertexEnvironment::GetPNQuota() const
{
    return m_pnQuota;
}

//
// Get the failure threshold
//
UInt32 DVertexEnvironment::GetMaxFailureThreshold() const
{
    return m_maxFailureThreshold;
}

//
// Get the number of failures before abort
//
UInt32 DVertexEnvironment::GetMinNumberOfFailuresBeforeAbort() const
{
    return m_minNumberOfFailuresBeforeAbort;
}
