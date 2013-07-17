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

// property type flag
const UINT16 DrPropTypeMask                       = 0xc000;

// DrPropType_Atom is a leaf property which is an element of a list or a
// set. There may be nested properties within the leaf.
const UINT16 DrPropType_Atom                      = 0x0000;

// length type flag
const UINT16 DrPropLengthMask                     = 0x2000;

// A property with DrPropLength_Short has a 1-byte length field
const UINT16 DrPropLength_Short                   = 0x0000;
// A property with DrPropLength_Long has a 4-byte length field
const UINT16 DrPropLength_Long                    = 0x2000;

// mask for the remaining 13-bit namespace

const UINT16 DrPropValueMask                      = 0x1fff;

#define DRPROP_SHORTATOM(x_) ((x_) | DrPropType_Atom | DrPropLength_Short)
#define DRPROP_LONGATOM(x_)  ((x_) | DrPropType_Atom | DrPropLength_Long)

const UINT16 DrProp_BeginTag =                  DRPROP_SHORTATOM(0x1200);
const UINT16 DrProp_EndTag =                    DRPROP_SHORTATOM(0x1201);

const UINT16 DrProp_ChannelState =              DRPROP_SHORTATOM(0x4000);
const UINT16 DrProp_ChannelURI =                DRPROP_LONGATOM(0x4003);
const UINT16 DrProp_ChannelBufferOffset =       DRPROP_SHORTATOM(0x4004);
const UINT16 DrProp_ChannelTotalLength =        DRPROP_SHORTATOM(0x4005);
const UINT16 DrProp_ChannelProcessedLength =    DRPROP_SHORTATOM(0x4006);
const UINT16 DrProp_StreamExpireTimeWhileOpen = DRPROP_SHORTATOM(0x4007);
const UINT16 DrProp_StreamExpireTimeWhileClosed = DRPROP_SHORTATOM(0x4008);
const UINT16 DrProp_VertexState =               DRPROP_SHORTATOM(0x4010);
const UINT16 DrProp_VertexErrorCode =           DRPROP_SHORTATOM(0x4011);
const UINT16 DrProp_VertexId =                  DRPROP_SHORTATOM(0x4012);
const UINT16 DrProp_VertexVersion =             DRPROP_SHORTATOM(0x4013);
const UINT16 DrProp_VertexInputChannelCount =   DRPROP_SHORTATOM(0x4015);
const UINT16 DrProp_VertexOutputChannelCount =  DRPROP_SHORTATOM(0x4016);
const UINT16 DrProp_VertexCommand =             DRPROP_SHORTATOM(0x4017);
const UINT16 DrProp_VertexArgumentCount =       DRPROP_SHORTATOM(0x4018);
const UINT16 DrProp_VertexArgument =            DRPROP_LONGATOM(0x4019);
const UINT16 DrProp_VertexSerializedBlock =     DRPROP_LONGATOM(0x401a);
const UINT16 DrProp_DebugBreak =                DRPROP_SHORTATOM(0x401b);
const UINT16 DrProp_AssertFailure =             DRPROP_LONGATOM(0x401c);
const UINT16 DrProp_CanShareWorkQueue =         DRPROP_SHORTATOM(0x401d);
const UINT16 DrProp_VertexMaxOpenInputChannelCount = DRPROP_SHORTATOM(0x401e);
const UINT16 DrProp_VertexMaxOpenOutputChannelCount = DRPROP_SHORTATOM(0x401f);
const UINT16 DrProp_ErrorCode =                 DRPROP_SHORTATOM(0x4040);
const UINT16 DrProp_ErrorString =               DRPROP_LONGATOM(0x4041);
const UINT16 DrProp_ItemBufferStartOffset =     DRPROP_SHORTATOM(0x4042);
const UINT16 DrProp_ItemBufferEndOffset =       DRPROP_SHORTATOM(0x4043);
const UINT16 DrProp_BufferLength =              DRPROP_SHORTATOM(0x4044);
const UINT16 DrProp_ItemStreamStartOffset =     DRPROP_SHORTATOM(0x4045);
const UINT16 DrProp_ItemStreamEndOffset =       DRPROP_SHORTATOM(0x4046);
const UINT16 DrProp_ItemDataSequenceNumber =    DRPROP_SHORTATOM(0x4047);
const UINT16 DrProp_ItemDeliverySequenceNumber = DRPROP_SHORTATOM(0x4048);
const UINT16 DrProp_InputPortCount =            DRPROP_SHORTATOM(0x4060);
const UINT16 DrProp_OutputPortCount =           DRPROP_SHORTATOM(0x4061);
const UINT16 DrProp_NumberOfVertices =          DRPROP_SHORTATOM(0x4062);
const UINT16 DrProp_SourceVertex =              DRPROP_SHORTATOM(0x4063);
const UINT16 DrProp_SourcePort =                DRPROP_SHORTATOM(0x4064);
const UINT16 DrProp_DestinationVertex =         DRPROP_SHORTATOM(0x4065);
const UINT16 DrProp_DestinationPort =           DRPROP_SHORTATOM(0x4066);
const UINT16 DrProp_NumberOfEdges =             DRPROP_SHORTATOM(0x4067);
const UINT16 DrProp_TryToCreateChannelPath =    DRPROP_SHORTATOM(0x4068);
const UINT16 DrProp_InitialChannelWriteSize =   DRPROP_SHORTATOM(0x4069);


const UINT16 DrTag_InputChannelDescription = 10000;
const UINT16 DrTag_OutputChannelDescription = 10001;
const UINT16 DrTag_VertexProcessStatus = 10002;
const UINT16 DrTag_VertexStatus = 10003;
const UINT16 DrTag_VertexCommand = 10004;
const UINT16 DrTag_ItemStart = 10005;
const UINT16 DrTag_ItemEnd = 10006;
const UINT16 DrTag_ChannelMetaData = 10007;
const UINT16 DrTag_VertexMetaData = 10008;
const UINT16 DrTag_ArgumentArray = 10009;
const UINT16 DrTag_VertexArray = 10010;
const UINT16 DrTag_VertexInfo = 10011;
const UINT16 DrTag_EdgeArray = 10012;
const UINT16 DrTag_EdgeInfo = 10013;
const UINT16 DrTag_GraphDescription = 10014;