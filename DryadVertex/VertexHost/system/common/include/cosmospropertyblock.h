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

#include <dvertexcommand.h>

class DrPnSetProcessPropertyRequest;
class DrPnGetProcessPropertyResponse;
class DrProcessPropertyInfo;

class DryadDryadPnProcessPropertyRequest :
    public DryadPnProcessPropertyRequest
{
public:
    DryadDryadPnProcessPropertyRequest(DrPnSetProcessPropertyRequest*
                                        request);

    void SetPropertyLabel(const char* label, const char* controlLabel);
    void SetPropertyString(const char* string);
    DrMemoryBuffer* GetPropertyBlock();

    DrPnSetProcessPropertyRequest* GetMessage();

private:
    DrRef<DrPnSetProcessPropertyRequest>   m_message;
};

class DryadDryadPnProcessPropertyResponse :
    public DryadPnProcessPropertyResponse
{
public:
    DryadDryadPnProcessPropertyResponse(DrPnGetProcessPropertyResponse*
                                         response);

    void RetrievePropertyLabel(const char* label);
    DrMemoryBuffer* GetPropertyBlock();

private:
    DrPnGetProcessPropertyResponse*  m_message;
    DrProcessPropertyInfo*           m_info;
};
