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

// There must *only* be DEFINE_DRPROPERTY or DEFINE_DRYADPROPERTY
// directives in this file

DEFINE_DRPROPERTY(Prop_Dryad_ChannelState, PROP_SHORTATOM(0x4000), DrError, "ChannelState")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelURI, PROP_LONGATOM(0x4003), String, "ChannelURI")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelBufferOffset, PROP_SHORTATOM(0x4004), UInt64, "ChannelBufferOffset")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelTotalLength, PROP_SHORTATOM(0x4005), UInt64, "ChannelTotalLength")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelProcessedLength, PROP_SHORTATOM(0x4006), UInt64, "ChannelProcessedLength")
DEFINE_DRPROPERTY(Prop_Dryad_StreamExpireTimeWhileOpen, PROP_SHORTATOM(0x4007), TimeInterval, "StreamExpireTimeWhileOpen")
DEFINE_DRPROPERTY(Prop_Dryad_StreamExpireTimeWhileClosed, PROP_SHORTATOM(0x4008), TimeInterval, "StreamExpireTimeWhileClosed")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelErrorCode, PROP_SHORTATOM(0x4009), DrError, "ChannelErrorCode")
DEFINE_DRPROPERTY(Prop_Dryad_ChannelErrorString, PROP_LONGATOM(0x400a), String, "ChannelErrorString")

DEFINE_DRPROPERTY(Prop_Dryad_VertexState, PROP_SHORTATOM(0x4010), DrError, "VertexState")
DEFINE_DRPROPERTY(Prop_Dryad_VertexErrorCode, PROP_SHORTATOM(0x4011), DrError, "VertexErrorCode")
DEFINE_DRPROPERTY(Prop_Dryad_VertexId, PROP_SHORTATOM(0x4012), UInt32, "VertexId")
DEFINE_DRPROPERTY(Prop_Dryad_VertexVersion, PROP_SHORTATOM(0x4013), UInt32, "VertexVersion")
DEFINE_DRPROPERTY(Prop_Dryad_VertexInputChannelCount, PROP_SHORTATOM(0x4015), UInt32, "VertexInputChannelCount")
DEFINE_DRPROPERTY(Prop_Dryad_VertexOutputChannelCount, PROP_SHORTATOM(0x4016), UInt32, "VertexOutputChannelCount")
DEFINE_DRYADPROPERTY(Prop_Dryad_VertexCommand, PROP_SHORTATOM(0x4017), VertexCommand, "VertexCommand")
DEFINE_DRPROPERTY(Prop_Dryad_VertexArgumentCount, PROP_SHORTATOM(0x4018), UInt32, "VertexArgumentCount")
DEFINE_DRPROPERTY(Prop_Dryad_VertexArgument, PROP_LONGATOM(0x4019), String, "VertexArgument")
DEFINE_DRPROPERTY(Prop_Dryad_VertexSerializedBlock, PROP_LONGATOM(0x401a), Blob, "VertexSerializedBlock")
DEFINE_DRPROPERTY(Prop_Dryad_DebugBreak, PROP_SHORTATOM(0x401b), Boolean, "DebugBreak")
DEFINE_DRPROPERTY(Prop_Dryad_AssertFailure, PROP_LONGATOM(0x401c), String, "AssertFailure")
DEFINE_DRPROPERTY(Prop_Dryad_CanShareWorkQueue, PROP_SHORTATOM(0x401d), Boolean, "CanShareWorkQueue")
DEFINE_DRPROPERTY(Prop_Dryad_VertexMaxOpenInputChannelCount, PROP_SHORTATOM(0x401e), UInt32, "VertexMaxOpenInputChannelCount")
DEFINE_DRPROPERTY(Prop_Dryad_VertexMaxOpenOutputChannelCount, PROP_SHORTATOM(0x401f), UInt32, "VertexMaxOpenOutputChannelCount")
DEFINE_DRPROPERTY(Prop_Dryad_VertexErrorString, PROP_LONGATOM(0x4020), String, "VertexErrorString")

DEFINE_DRPROPERTY(Prop_Dryad_ErrorCode, PROP_SHORTATOM(0x4040), DrError, "ErrorCode")
DEFINE_DRPROPERTY(Prop_Dryad_ErrorString, PROP_LONGATOM(0x4041), String, "ErrorString")
DEFINE_DRPROPERTY(Prop_Dryad_ItemBufferStartOffset, PROP_SHORTATOM(0x4042), UInt64, "ItemBufferStartOffset")
DEFINE_DRPROPERTY(Prop_Dryad_ItemBufferEndOffset, PROP_SHORTATOM(0x4043), UInt64, "ItemBufferEndOffset")
DEFINE_DRPROPERTY(Prop_Dryad_BufferLength, PROP_SHORTATOM(0x4044), UInt64, "BufferLength")
DEFINE_DRPROPERTY(Prop_Dryad_ItemStreamStartOffset, PROP_SHORTATOM(0x4045), UInt64, "ItemStreamStartOffset")
DEFINE_DRPROPERTY(Prop_Dryad_ItemStreamEndOffset, PROP_SHORTATOM(0x4046), UInt64, "ItemStreamEndOffset")
DEFINE_DRPROPERTY(Prop_Dryad_ItemDataSequenceNumber, PROP_SHORTATOM(0x4047), UInt64, "ItemDataSequenceNumber")
DEFINE_DRPROPERTY(Prop_Dryad_ItemDeliverySequenceNumber, PROP_SHORTATOM(0x4048), UInt64, "ItemDeliverySequenceNumber")

DEFINE_DRPROPERTY(Prop_Dryad_InputPortCount, PROP_SHORTATOM(0x4060), UInt32, "InputPortCount")
DEFINE_DRPROPERTY(Prop_Dryad_OutputPortCount, PROP_SHORTATOM(0x4061), UInt32, "OutputPortCount")
DEFINE_DRPROPERTY(Prop_Dryad_NumberOfVertices, PROP_SHORTATOM(0x4062), UInt32, "NumberOfVertices")
DEFINE_DRPROPERTY(Prop_Dryad_SourceVertex, PROP_SHORTATOM(0x4063), UInt32, "SourceVertex")
DEFINE_DRPROPERTY(Prop_Dryad_SourcePort, PROP_SHORTATOM(0x4064), UInt32, "SourcePort")
DEFINE_DRPROPERTY(Prop_Dryad_DestinationVertex, PROP_SHORTATOM(0x4065), UInt32, "DestinationVertex")
DEFINE_DRPROPERTY(Prop_Dryad_DestinationPort, PROP_SHORTATOM(0x4066), UInt32, "DestinationPort")
DEFINE_DRPROPERTY(Prop_Dryad_NumberOfEdges, PROP_SHORTATOM(0x4067), UInt32, "NumberOfEdges")
DEFINE_DRYADPROPERTY(Prop_Dryad_TryToCreateChannelPath, PROP_SHORTATOM(0x4068), Void, "TryToCreateChannelPath")
DEFINE_DRPROPERTY(Prop_Dryad_InitialChannelWriteSize, PROP_SHORTATOM(0x4069), UInt64, "InitialChannelWriteSize")
// BUGBUG: this property used to be called Prop_Dryad_MachineName, but it was mistakenly declared with a SHORTATOM. THat property
// BUGBUG: is now deprecated, and this one should be used instead.
DEFINE_DRPROPERTY(Prop_Dryad_LongMachineName, PROP_LONGATOM(0x406a), String, "LongMachineName" )

DEFINE_DRPROPERTY(Prop_Dryad_RSRootProcessIdentifier, PROP_LONGATOM(0x4070), String, "RSRootProcessIdentifier")
DEFINE_DRPROPERTY(Prop_Dryad_RSMachineName, PROP_LONGATOM(0x4071), String, "RSMachineName")
DEFINE_DRPROPERTY(Prop_Dryad_RSCPUAllowance, PROP_SHORTATOM(0x4072), UInt32, "RSCPUAllowance")
DEFINE_DRPROPERTY(Prop_Dryad_RSDiskAllowance, PROP_SHORTATOM(0x4073), UInt32, "RSDiskAllowance")
DEFINE_DRPROPERTY(Prop_Dryad_RSMemoryAllowance, PROP_SHORTATOM(0x4074), UInt64, "RSMemoryAllowance")
DEFINE_DRPROPERTY(Prop_Dryad_RSProcessGuid, PROP_SHORTATOM(0x4075), Guid, "RSProcessGuid")
DEFINE_DRPROPERTY(Prop_Dryad_RSPodName, PROP_LONGATOM(0x4076), String, "RSPodName")
DEFINE_DRPROPERTY(Prop_Dryad_RSAffinity, PROP_SHORTATOM(0x4077), UInt32, "RSAffinity")
DEFINE_DRPROPERTY(Prop_Dryad_RSFailedMachine, PROP_LONGATOM(0x4078), String, "RSFailedMachine")
DEFINE_DRPROPERTY(Prop_Dryad_RSDiscardedProcess, PROP_SHORTATOM(0x4079), Guid, "RSDiscardedProcess")
DEFINE_DRPROPERTY(Prop_Dryad_RSReturnedProcess, PROP_SHORTATOM(0x407a), Guid, "RSReturnedProcess")
DEFINE_DRPROPERTY(Prop_Dryad_RSReplacementGuid, PROP_SHORTATOM(0x407b), Guid, "RSReplacementGuid")
DEFINE_DRPROPERTY(Prop_Dryad_RSClientStarting, PROP_SHORTATOM(0x407c), Boolean, "RSClientStarting")
DEFINE_DRPROPERTY(Prop_Dryad_RSMachineDataSize, PROP_SHORTATOM(0x407d), UInt64, "RSMachineDataSize")
DEFINE_DRPROPERTY(Prop_Dryad_RSPodDataSize, PROP_SHORTATOM(0x407e), UInt64, "RSPodDataSize")
DEFINE_DRPROPERTY(Prop_Dryad_RSRootProcessName, PROP_LONGATOM(0x407f), String, "RSRootProcessName")
DEFINE_DRPROPERTY(Prop_Dryad_RSRootProcessMachine, PROP_LONGATOM(0x4080), String, "RSRootProcessMachine")
DEFINE_DRPROPERTY(Prop_Dryad_RSSendTime, PROP_SHORTATOM(0x4081), TimeStamp, "RSSendTime")
DEFINE_DRPROPERTY(Prop_Dryad_RSProcessingTime, PROP_SHORTATOM(0x4082), TimeInterval, "RSProcessingTime")
