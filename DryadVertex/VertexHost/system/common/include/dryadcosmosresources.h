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

#include <dryadopaqueresources.h>

class DryadDryadProcessIdentifier : public DryadProcessIdentifier
{
public:
    DryadDryadProcessIdentifier(DrGuid* guid);

    DrGuid* GetGuid();
    const char* GetGuidString();

    void SetWorkingDirectory(const char* workingDirectory);
    void MakeURIForRelativeFile(DrStr* dst,
                                const char* baseDirectory,
                                const char* relativeFileName);

private:
    DrGuid                   m_guid;
    DrStr32                  m_guidString;
    DrStr128                 m_workingDirectory;
};

class DryadDryadMachineIdentifier : public DryadMachineIdentifier
{
public:
    DryadDryadMachineIdentifier(DrServiceDescriptor* desc);

    DrServiceDescriptor* GetServiceDescriptor();

private:
    DrServiceDescriptor     m_serviceDescriptor;
};
