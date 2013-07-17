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

#include <DrGraphHeaders.h>


DrProcessTemplateRef DrDefaultParameters::MakeProcessTemplate(DrNativeString exeName, DrNativeString jobClass)
{
    DrProcessTemplateRef t = DrNew DrProcessTemplate();

    t->SetCommandLineBase(DrString(exeName));
    t->SetProcessClass(DrString(jobClass));

    t->SetFailedRetainAndLeaseGraceTime(DrTimeInterval_Minute * 3, DrTimeInterval_Minute * 2);
    t->SetCompletedRetainAndLeaseGraceTime(DrTimeInterval_Minute * 3, DrTimeInterval_Minute * 2);

    t->SetTimeOutBetweenProcessEndAndVertexNotification(DrTimeInterval_Second * 30);

    return t;
}

DrVertexTemplateRef DrDefaultParameters::MakeVertexTemplate()
{
    DrVertexTemplateRef t = DrNew DrVertexTemplate();

    t->SetStatusBlockTime(DrTimeInterval_Second * 10);

    return t;
}

DrGraphParametersRef DrDefaultParameters::Make(DrNativeString exeName, DrNativeString jobClass, bool enableSpeculativeDuplication)
{
    DrGraphParametersRef p = DrNew DrGraphParameters();

    p->m_processAbortTimeOut = DrTimeInterval_Second * 30;
    p->m_maxActiveFailureCount = 6;

    p->m_duplicateEverythingThreshold = 10;
    if(enableSpeculativeDuplication)
    {
        p->m_defaultOutlierThreshold = 10 * DrTimeInterval_Minute;
        /* Wait until this fraction of vertices have completed before computing outlier time estimate */
        p->m_nonParametricThresholdFraction = 0.50;
    }
    else
    {
        // to disable speculative dupilcation, set minimum time to inifinite 
        // and require all vertices complete before calculating non-parametric threshold
        p->m_defaultOutlierThreshold = DrTimeInterval_Infinite;
        p->m_nonParametricThresholdFraction = 1.0;
    }

    p->m_minOutlierThreshold = 10 * DrTimeInterval_Second;

    p->m_defaultProcessTemplate = MakeProcessTemplate(exeName, jobClass);
    p->m_defaultVertexTemplate = MakeVertexTemplate();

    return p;
}