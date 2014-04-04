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

DRDECLARECLASS(DrError);
DRREF(DrError);

typedef DrArrayList<DrErrorRef> DrErrorList;
DRAREF(DrErrorList,DrErrorRef);

DRBASECLASS(DrError)
{
public:
    DrError(HRESULT code, DrNativeString component, DrString explanation);
    DrError(HRESULT code, DrString component, DrString explanation);

    void AddProvenance(DrErrorPtr previousError);

    DrString ToShortText();
    DrString ToFullText();
    DrNativeString ToFullTextNative();
    static DrString ToShortText(DrErrorPtr errorOrNull);

    HRESULT           m_code;
    DrString          m_component;
    DrString          m_explanation;

    DrErrorListRef    m_errorProvenance;
};
DRREF(DrError);

#define FACILITY_COSMOS  777
#define FACILITY_DRYAD   778
#define FACILITY_DSC     779

#define DRYAD_ERROR(n) ((HRESULT)(0x80000000 + (FACILITY_DRYAD << 16) + n))
#define COSMOS_ERROR(n) ((HRESULT)(0x80000000 + (FACILITY_COSMOS << 16) + n))
#define DSC_ERROR(n) ((HRESULT)(0x80000000 + (FACILITY_DSC << 16) + n))

const HRESULT DrError_BadMetaData =               DRYAD_ERROR (0x0001);
const HRESULT DrError_InvalidCommand =            DRYAD_ERROR (0x0002);
const HRESULT DrError_VertexReceivedTermination = DRYAD_ERROR (0x0003);
const HRESULT DrError_InvalidChannelURI =         DRYAD_ERROR (0x0004);
const HRESULT DrError_ChannelOpenError =          DRYAD_ERROR (0x0005);
const HRESULT DrError_ChannelRestartError =       DRYAD_ERROR (0x0006);
const HRESULT DrError_ChannelWriteError =         DRYAD_ERROR (0x0007);
const HRESULT DrError_ChannelReadError =          DRYAD_ERROR (0x0008);
const HRESULT DrError_ItemParseError =            DRYAD_ERROR (0x0009);
const HRESULT DrError_ItemMarshalError =          DRYAD_ERROR (0x0010);
const HRESULT DrError_BufferHole =                DRYAD_ERROR (0x0011);
const HRESULT DrError_ItemHole =                  DRYAD_ERROR (0x0012);
const HRESULT DrError_ChannelRestart =            DRYAD_ERROR (0x0013);
const HRESULT DrError_ChannelAbort =              DRYAD_ERROR (0x0014);
const HRESULT DrError_VertexRunning =             DRYAD_ERROR (0x0015);
const HRESULT DrError_VertexCompleted =           DRYAD_ERROR (0x0016);
const HRESULT DrError_VertexError =               DRYAD_ERROR (0x0017);
const HRESULT DrError_ProcessingError =           DRYAD_ERROR (0x0018);
const HRESULT DrError_VertexInitialization =      DRYAD_ERROR (0x0019);
const HRESULT DrError_ProcessingInterrupted =     DRYAD_ERROR (0x001a);
const HRESULT DrError_VertexChannelClose =        DRYAD_ERROR (0x001b);
const HRESULT DrError_AssertFailure =             DRYAD_ERROR (0x001c);
const HRESULT DrError_ExternalChannel =           DRYAD_ERROR (0x001d);
const HRESULT DrError_AlreadyInitialized =        DRYAD_ERROR (0x001e);
const HRESULT DrError_DuplicateVertices =         DRYAD_ERROR (0x001f);
const HRESULT DrError_ComposeRHSNeedsInput =      DRYAD_ERROR (0x0020);
const HRESULT DrError_ComposeLHSNeedsOutput =     DRYAD_ERROR (0x0021);
const HRESULT DrError_ComposeStagesMustBeDifferent = DRYAD_ERROR (0x0022);
const HRESULT DrError_ComposeStageEmpty =         DRYAD_ERROR (0x0023);
const HRESULT DrError_VertexNotInGraph =          DRYAD_ERROR (0x0024);
const HRESULT DrError_HardConstraintCannotBeMet = DRYAD_ERROR (0x0025);
const HRESULT DrError_ClusterError =              DRYAD_ERROR (0x0026);
const HRESULT DrError_CohortShutdown =            DRYAD_ERROR (0x0027);
const HRESULT DrError_Unexpected =                DRYAD_ERROR (0x0028);
const HRESULT DrError_DependentVertexFailure =    DRYAD_ERROR (0x0029);
const HRESULT DrError_BadOutputReported =         DRYAD_ERROR (0x002a);
const HRESULT DrError_InputUnavailable =          DRYAD_ERROR (0x002b);

const HRESULT DrError_EndOfStream =               COSMOS_ERROR (0x000b);

const HRESULT DrError_CannotConnectToDsc =        DSC_ERROR (0x0100);
const HRESULT DrError_DscOperationFailed =        DSC_ERROR (0x0101);
const HRESULT DrError_FailedToDeleteFileset =     DSC_ERROR (0x0102);
const HRESULT DrError_FailedToCreateFileset =     DSC_ERROR (0x0103);
const HRESULT DrError_FailedToAddFile =           DSC_ERROR (0x0104);
const HRESULT DrError_FailedToSetMetadata =       DSC_ERROR (0x0105);
const HRESULT DrError_FailedToSealFileset =       DSC_ERROR (0x0106);
const HRESULT DrError_FailedToSetLease =          DSC_ERROR (0x0107);
const HRESULT DrError_FailedToOpenFileset =       DSC_ERROR (0x0108);
