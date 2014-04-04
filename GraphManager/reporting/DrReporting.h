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

DRCLASS(DrIReporter abstract) : public DrCritSec, public DrProcessListener, public DrVertexListener,
    public DrVertexTopologyReporter
{
public:
    /* the DrProcessListener implementation */
    virtual void ReceiveMessage(DrProcessInfoRef info) DRABSTRACT;

    /* the DrVertexListener implementation */
    virtual void ReceiveMessage(DrVertexInfoRef info) DRABSTRACT;

    /* the DrVertexTopologyReporter implementation */
    virtual void ReportFinalTopology(DrVertexPtr vertex, DrResourcePtr runningMachine,
                                     DrTimeInterval runningTime) DRABSTRACT;

    virtual void ReportStart(DrDateTime startTime) DRABSTRACT;
    virtual void ReportStop(UINT exitCode, DrNativeString errorString, DrDateTime stopTime) DRABSTRACT;
};
DRREF(DrIReporter);

DRMAKEARRAYLIST(DrIReporterRef);
