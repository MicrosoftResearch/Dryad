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

DRCLASS(DrArtemisLegacyReporter) : public DrIReporter
{
public:
    /* the DrProcessListener implementation */
    virtual void ReceiveMessage(DrProcessInfoRef info) DROVERRIDE;

    /* the DrVertexListener implementation */
    virtual void ReceiveMessage(DrVertexInfoRef info) DROVERRIDE;

    /* the DrVertexTopologyReporter implementation */
    virtual void ReportFinalTopology(DrVertexPtr vertex, DrResourcePtr runningMachine,
                                     DrTimeInterval runningTime) DROVERRIDE;

    virtual void ReportStart(DrDateTime startTime) DROVERRIDE;

    virtual void ReportStop(UINT exitCode, DrNativeString errorString, DrDateTime stopTime) DROVERRIDE;
};
DRREF(DrArtemisLegacyReporter);