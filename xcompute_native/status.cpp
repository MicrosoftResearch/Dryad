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

/*++
Module Name:

    file.cpp

Abstract:

    This module contains the public interface and support routines for
    the xcompute process file functionality on top of the HPC scheduler.

--*/
#include "stdafx.h"

using namespace Microsoft::Research::Dryad;


XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcResetProgress(    
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    ULONG            nTotalProgressSteps,
    IN    bool             bUpdate
)
{
    VertexScheduler::GetInstance()->JobStatus->ResetProgress(nTotalProgressSteps, bUpdate);
    return S_OK;
}

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcIncrementTotalSteps(
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    bool             bUpdate
    )
{
    VertexScheduler::GetInstance()->JobStatus->IncrementTotalSteps(bUpdate);
    return S_OK;
}

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcDecrementTotalSteps(
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    bool             bUpdate
    )
{
    VertexScheduler::GetInstance()->JobStatus->DecrementTotalSteps(bUpdate);
    return S_OK;
}

XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcSetProgress(    
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    ULONG            nCompletedProgressSteps,
    IN    PCSTR            pMessage
)
{
    VertexScheduler::GetInstance()->JobStatus->SetProgress(nCompletedProgressSteps, gcnew System::String(pMessage));
    return S_OK;
}


XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcIncrementProgress(    
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    PCSTR            pMessage
)
{
    VertexScheduler::GetInstance()->JobStatus->IncrementProgress(gcnew System::String(pMessage));
    return S_OK;
}


XCOMPUTEAPI_EXT
XCERROR
XCOMPUTEAPI
XcCompleteProgress(    
    IN    XCSESSIONHANDLE  SessionHandle,
    IN    PCSTR            pMessage
)
{
    VertexScheduler::GetInstance()->JobStatus->CompleteProgress(gcnew System::String(pMessage));
    return S_OK;
}
