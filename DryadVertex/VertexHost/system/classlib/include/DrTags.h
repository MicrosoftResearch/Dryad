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

// Tags used for BeginTag/EndTag

// The must *only* be DEFINE_DRTAG directives in this file

DEFINE_DRTAG(DrTag_InvalidTag, 0, "InvalidTag")   // An invalid tag value


// DRM Tags
DEFINE_DRTAG(Drm_CreateStream, 1, "CreateStream")
DEFINE_DRTAG(Drm_CreateStreamResponse, 2, "CreateStreamResponse")
DEFINE_DRTAG(Drm_AppendStream, 3, "AppendStream")
DEFINE_DRTAG(Drm_AppendStreamResponse, 4, "AppendStreamResponse")
DEFINE_DRTAG(Drm_EnumDirectory, 5, "EnumDirectory")
DEFINE_DRTAG(Drm_EnumDirectoryResponse, 6, "EnumDirectoryResponse")
DEFINE_DRTAG(Drm_GetVolumeInfo, 7, "GetVolumeInfo")
DEFINE_DRTAG(Drm_GetVolumeInfoResponse, 8, "GetVolumeInfoResponse")
DEFINE_DRTAG(Drm_DeleteStream, 9, "DeleteStream")
DEFINE_DRTAG(Drm_DeleteStreamResponse, 10, "DeleteStreamResponse")
DEFINE_DRTAG(DrTag_AppendExtentRequest, 11, "AppendExtentRequest")
DEFINE_DRTAG(DrTag_AppendExtentResponse, 12, "AppendExtentResponse")
DEFINE_DRTAG(Drm_ReadStream, 13, "ReadStream")
DEFINE_DRTAG(Drm_ReadStreamResponse, 14, "ReadStreamResponse")
DEFINE_DRTAG(Drm_UpdateExtentMetadataEvent, 15, "UpdateExtentMetadataEvent")
DEFINE_DRTAG(Drm_UpdateExtentInstanceMetadataEvent, 16, "UpdateExtentInstanceMetadataEvent")
DEFINE_DRTAG(DrTag_ExtentNodeStatusEvent, 17, "ExtentNodeStatusEvent")
DEFINE_DRTAG(DrTag_DrmGarbageCollectEvent, 19, "DrmGarbageCollect")
DEFINE_DRTAG(DrTag_DrmSyncEnEvent, 20, "DrmSyncEN")
DEFINE_DRTAG(Drm_RenameStream, 21, "RenameStream")
DEFINE_DRTAG(Drm_RenameStreamResponse, 22, "RenameStreamResponse")
DEFINE_DRTAG(Drm_NewVolumeEvent, 23, "NewVolumeEvent")
DEFINE_DRTAG(DrTag_AppendExtentRangeRequest, 24, "AppendExtentRangeRequest")
DEFINE_DRTAG(DrTag_AppendExtentRangeResponse, 25, "AppendExtentRangeResponse")
DEFINE_DRTAG(DrTag_DrmSealExtentRequest, 26, "DrmSealExtentRequest")
DEFINE_DRTAG(DrTag_DrmSealExtentResponse, 27, "DrmSealExtentResponse")
DEFINE_DRTAG(DrTag_DrmSetStreamProperties, 28, "SetStreamProperties")
DEFINE_DRTAG(DrTag_DrmSetStreamPropertiesResponse, 29, "SetStreamPropertiesResponse")
// the following four tags are for synchronized version of metadata update
DEFINE_DRTAG(Drm_UpdateExtentMetadataRequest, 30, "UpdateExtentMetadataRequest")        
DEFINE_DRTAG(Drm_UpdateExtentMetadataResponse, 31, "UpdateExtentMetadataResponse")
DEFINE_DRTAG(Drm_UpdateExtentInstanceMetadataRequest, 32, "UpdateExtentInstanceMetadataRequest")
DEFINE_DRTAG(Drm_UpdateExtentInstanceMetadataResponse, 33, "UpdateExtentInstanceMetadataResponse")
DEFINE_DRTAG(DrTag_AppendExtentRangeRequest2, 34, "AppendExtentRangeRequest2")
DEFINE_DRTAG(DrTag_AppendExtentRangeResponse2, 35, "AppendExtentRangeResponse2")
DEFINE_DRTAG(Drm_SubstituteStreamRequest, 36, "SubstituteStreamRequest")
DEFINE_DRTAG(Drm_SubstituteStreamResponse, 37, "SubstituteStreamResponse")
DEFINE_DRTAG(DrTag_ExtentRangeList, 38, "ExtentRangeList")
DEFINE_DRTAG(DrTag_ExtentRange, 39, "ExtentRange")
DEFINE_DRTAG(Drm_ExtentList, 51, "ExtentList") // array of CEIDs

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// REDDOG DRM extensions (shared with cosmos)
DEFINE_DRTAG(Drm_MultiModifyStreamEntry, 61, "MultiModifyStreamEntry") // a single stream mod entry within a list
DEFINE_DRTAG(Drm_MultiModifyStream, 62, "MultiModifyStream")
DEFINE_DRTAG(Drm_MultiModifyStreamResponse, 63, "MultiModifyStreamResponse")
DEFINE_DRTAG(Drm_MultiModifyExtentRange, 64, "MultiModifyExtentRange") // a single concatenate extent range within a list

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// EN Tags
// First set are for requests from csm/client
DEFINE_DRTAG(DrTag_EnAppendExtentRequest, 100, "EnAppendExtentRequest")
DEFINE_DRTAG(DrTag_EnSealExtentRequest, 101, "EnSealExtentRequest")
DEFINE_DRTAG(DrTag_EnReadExtentRequest, 102, "EnReadExtentRequest")
DEFINE_DRTAG(DrTag_EnCreateExtentRequest, 103, "EnCreateExtentRequest")
DEFINE_DRTAG(DrTag_EnStartSealingExtentInstanceRequest, 104, "EnStartSealingExtentInstanceRequest")
DEFINE_DRTAG(DrTag_EnSyncRequest, 105, "EnSyncRequest")
DEFINE_DRTAG(DrTag_EnReplicateExtentRequest, 106, "EnReplicateExtentRequest")
DEFINE_DRTAG(DrTag_EnRecoverExtentRequest, 107, "EnRecoverExtentRequest")
DEFINE_DRTAG(DrTag_EnCmdRequest, 108, "EnCmdRequest" )
DEFINE_DRTAG(DrTag_EnReadaheadInfo, 109, "EnReadaheadInfo" )

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// These are the matching response tags for replies to above requests
DEFINE_DRTAG(DrTag_EnAppendExtentResponse, 200, "EnAppendExtentResponse")
DEFINE_DRTAG(DrTag_EnSealExtentResponse, 201, "EnSealExtentResponse")
DEFINE_DRTAG(DrTag_EnReadExtentResponse, 202, "EnReadExtentResponse")
DEFINE_DRTAG(DrTag_EnCreateExtentResponse, 203, "EnCreateExtentResponse")
DEFINE_DRTAG(DrTag_EnStartSealingExtentInstanceResponse, 204, "EnStartSealingExtentInstanceResponse")
DEFINE_DRTAG(DrTag_EnSyncResponse, 205, "EnSyncResponse")
DEFINE_DRTAG(DrTag_EnReplicateExtentResponse, 206, "EnReplicateExtentResponse")
DEFINE_DRTAG(DrTag_EnRecoverExtentResponse, 207, "EnRecoverExtentResponse")
DEFINE_DRTAG(DrTag_EnCmdResponse, 208, "EnCmdResponse" )

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Second set are for requests between ENs to handle replication
DEFINE_DRTAG(DrTag_EnReplicateCreateExtentRequest, 300, "EnReplicateCreateExtentRequest")
DEFINE_DRTAG(DrTag_EnReplicateAppendExtentRequest, 301, "EnReplicateAppendExtentRequest")
//DEFINE_DRTAG(DrTag_EnReplicateExtentLengthRequest, 302, "EnReplicateExtentLengthRequest")
DEFINE_DRTAG(DrTag_EnReplicateSealRequest, 303, "EnReplicateSealRequest")

// And these are the matching responses to the above requests
DEFINE_DRTAG(DrTag_EnReplicateCreateExtentResponse, 400, "EnReplicateCreateExtentResponse")
DEFINE_DRTAG(DrTag_EnReplicateAppendExtentResponse, 401, "EnReplicateAppendExtentResponse")
DEFINE_DRTAG(DrTag_EnReplicateExtentLengthResponse, 402, "EnReplicateExtentLengthResponse")
DEFINE_DRTAG(DrTag_EnReplicateSealResponse, 403, "EnReplicateSealResponse")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Generic messages
DEFINE_DRTAG(DrTag_DrMalformedMessage, 500, "DrMalformedMessage")
DEFINE_DRTAG(DrTag_DrErrorResponseMessage, 501, "DrErrorResponseMessage")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Name Resolution tags
DEFINE_DRTAG(DrTag_DrResolveNameRequest, 600, "DrResolveNameRequest")
DEFINE_DRTAG(DrTag_DrResolveNameResponse, 601, "DrResolveNameResponse")
DEFINE_DRTAG(DrTag_DrGetNameResolutionMapRequest, 602, "DrGetNameResolutionMapRequest")
DEFINE_DRTAG(DrTag_DrGetNameResolutionMapResponse, 603, "DrGetNameResolutionMapResponse")
DEFINE_DRTAG(DrTag_DrRegisterNamesRequest, 604, "DrRegisterNamesRequest")
DEFINE_DRTAG(DrTag_DrRegisterNamesResponse, 605, "DrRegisterNamesResponse")
DEFINE_DRTAG(DrTag_DrUpdateNameResolution, 606, "DrUpdateNameResolution")
DEFINE_DRTAG(DrTag_DrUpdateNameResolutionResponse, 607, "DrUpdateNameResolutionResponse")
DEFINE_DRTAG(DrTag_DrHostNameList, 608, "DrHostNameList")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// ProcessNode tags
DEFINE_DRTAG(DrTag_PnCreateProcessRequest, 			700, 	"PnCreateProcessRequest")
DEFINE_DRTAG(DrTag_PnCreateProcessResponse, 		701, 	"PnCreateProcessResponse")
DEFINE_DRTAG(DrTag_PnGetProcessStatusRequest, 		702, 	"PnGetProcessStatusRequest")
DEFINE_DRTAG(DrTag_PnGetProcessStatusResponse, 		703, 	"PnGetProcessStatusResponse")
DEFINE_DRTAG(DrTag_PnTerminateProcessRequest, 		704, 	"PnTerminateProcessRequest")
DEFINE_DRTAG(DrTag_PnTerminateProcessResponse, 		705, 	"PnTerminateProcessResponse")
DEFINE_DRTAG(DrTag_PnSetProcessPropertyRequest, 	706, 	"PnSetProcessPropertyRequest")
DEFINE_DRTAG(DrTag_PnSetProcessPropertyResponse, 	707, 	"PnSetProcessPropertyResponse")
DEFINE_DRTAG(DrTag_PnGetProcessPropertyRequest, 	708, 	"PnGetProcessPropertyRequest")
DEFINE_DRTAG(DrTag_PnGetProcessPropertyResponse, 	709, 	"PnGetProcessPropertyResponse")
DEFINE_DRTAG(DrTag_PnEnumerateProcessesRequest, 	710, 	"PnEnumerateProcessesRequest")
DEFINE_DRTAG(DrTag_PnEnumerateProcessesResponse, 	711, 	"PnEnumerateProcessesResponse")
DEFINE_DRTAG(DrTag_PnUserData, 						712, 	"PnUserData")
DEFINE_DRTAG(DrTag_PnProcessStat, 					713,	"PnProcessStat")
DEFINE_DRTAG(DrTag_PnJobStat, 					    714,	"PnJobStat")
DEFINE_DRTAG(DrTag_PnGetJobStatRequest,             715,    "PnGetJobStatRequest")
DEFINE_DRTAG(DrTag_PnGetJobStatResponse,            716,    "PnGetJobStatResponse")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Pipe protocol tags
DEFINE_DRTAG(DrTag_DrRendezvousConnect, 800, "DrRendezvousConnect")
DEFINE_DRTAG(DrTag_DrRendezvousConnected, 801, "DrRendezvousConnected")
DEFINE_DRTAG(DrTag_DrRendezvousConnectedAck, 802, "DrRendezvousConnectedAck")
DEFINE_DRTAG(DrTag_DrRendezvousConnectFailed, 803, "DrRendezvousConnectFailed")
DEFINE_DRTAG(DrTag_DrRendezvousRedirect, 804, "DrRendezvousRedirect")
DEFINE_DRTAG(DrTag_DrRendezvousWait, 805, "DrRendezvousWait")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Execution monitoring tags
DEFINE_DRTAG(DrTag_JmJobExecutionStatistics, 900, "JmJobExecutionStatistics")
DEFINE_DRTAG(DrTag_JmVertexClassExecutionStatistics, 901, "JmVertexClassExecutionStatistics")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Cache Manager request tags
DEFINE_DRTAG(DrTag_CmSeedRequest,          1000,     "CmSeedRequest")
DEFINE_DRTAG(DrTag_CmQueryFileRequest,     1001,     "CmQueryFileRequest")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Cache Manager response tags
DEFINE_DRTAG(DrTag_CmSeedResponse,         1010,     "CmSeedResponse")
DEFINE_DRTAG(DrTag_CmQueryFileResponse,    1011,     "CmQueryFileResponse")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Cache Manager event tags
DEFINE_DRTAG(DrTag_CmGossipEvent,          1020,     "CmGossipEvent")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// Dryad Task Scheduler tags
DEFINE_DRTAG(DrTag_ScheduleTaskRequest,    1100,     "ScheduleTaskRequest")
DEFINE_DRTAG(DrTag_ScheduleTaskResponse,   1101,     "ScheduleTaskResponse")
DEFINE_DRTAG(DrTag_TaskJournalStart,       1102,     "TaskJournalStart")
DEFINE_DRTAG(DrTag_TaskJournalEnd,         1103,     "TaskJournalEnd")
DEFINE_DRTAG(DrTag_GetTaskInfoRequest,     1104,     "GetTaskInfoRequest")
DEFINE_DRTAG(DrTag_GetTaskInfoResponse,    1105,     "GetTaskInfoResponse")
DEFINE_DRTAG(DrTag_GetTaskJournalRequest,  1106,     "GetTaskJournalRequest")
DEFINE_DRTAG(DrTag_GetTaskJournalResponse, 1107,     "GetTaskJournalResponse")
DEFINE_DRTAG(DrTag_DeleteTaskRequest,      1108,     "DeleteTaskRequest")
DEFINE_DRTAG(DrTag_DeleteTaskResponse,     1109,     "DeleteTaskResponse")
DEFINE_DRTAG(DrTag_EnumerateTasksRequest,  1110,     "EnumerateTasksRequest")
DEFINE_DRTAG(DrTag_EnumerateTasksResponse, 1111,     "EnumerateTasksResponse")
DEFINE_DRTAG(DrTag_EnumeratedTask,         1112,     "EnumeratedTask")
DEFINE_DRTAG(DrTag_TaskJournalEntry,       1113,     "TaskJournalEntry")
DEFINE_DRTAG(DrTag_EnumerateSchedulerLogRequest, 1114, "EnumerateSchedulerLogRequest")
DEFINE_DRTAG(DrTag_EnumerateSchedulerLogResponse,1115, "EnumerateSchedulerLogResponse")
DEFINE_DRTAG(DrTag_SetSchedulerPropertiesRequest,1116, "SetSchedulerPropertiesRequest")
DEFINE_DRTAG(DrTag_SetSchedulerPropertiesResponse,1117, "SetSchedulerPropertiesResponse")
DEFINE_DRTAG(DrTag_SetTaskPropertiesRequest,1118,    "SetTaskPropertiesRequest")
DEFINE_DRTAG(DrTag_SetTaskPropertiesResponse,1119,   "SetTaskPropertiesResponse")
DEFINE_DRTAG(DrTag_AppendJobDataRequest,1120,        "AppendJobDataRequest")
DEFINE_DRTAG(DrTag_AppendJobDataResponse,1121,       "AppendJobDataResponse")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// jforbes: Tags between 1122-1139 reserved for task scheduler

// Dryad Task scheduler log
DEFINE_DRTAG(DrTag_TaskLogTaskSubmitted,   1140,     "TaskSubmitted")
DEFINE_DRTAG(DrTag_TaskLogTaskDeleted,     1141,     "TaskDeleted")
DEFINE_DRTAG(DrTag_TaskLogTaskCompleted,   1142,     "TaskCompleted")
DEFINE_DRTAG(DrTag_TaskLogJobStarted,      1143,     "JobStarted")
DEFINE_DRTAG(DrTag_TaskLogJobEnded,        1144,     "JobEnded")
DEFINE_DRTAG(DrTag_TaskLogGainedMasterStatus, 1145,  "GainedMasterStatus")
DEFINE_DRTAG(DrTag_TaskLogSetSchedulerProperties, 1146,  "SetSchedulerProperties")
DEFINE_DRTAG(DrTag_TaskLogSetTaskProperties, 1147,   "SetTaskProperties")
DEFINE_DRTAG(DrTag_TaskLogJobData,         1148,   "JobData")

// This is not actually a log entry, but the custom property bag part of a TaskLogJobData entry
DEFINE_DRTAG(DrTag_TaskLogJobDataContents, 1149,   "JobDataContents")

// This is a tag that will be used within a DrTag_TaskLogJobDataContents
// It describes data that has been uploaded Dryad URL, typically by Dryad, that should be
// preserved.  Sub-properties will include a Dryad URI and a flag indicating whether it is
// temporary output (e.g. stdout.txt for the job) that should be cleaned up by the scheduler
// at some later point.
DEFINE_DRTAG(DrTag_JobOutput, 1150, "JobOutput")

// This entry indicates the task scheduler appended its state to the permanent storage stream
// at a particular sequence number.
DEFINE_DRTAG(DrTag_TaskLogArchive, 1151,   "Archive")

// This entry indicates an error or warning related to the scheduler
// This may include being unable to start a job
DEFINE_DRTAG(DrTag_TaskLogError, 1152,   "SchedulerError")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// jforbes: Tags between 1153-1199 reserved for task scheduler

// Dryad Async RPC Server Protocol tags
DEFINE_DRTAG(DrTag_RpcTunnelTransportRequest, 1200, "RpcTunnelTransportRequest")
DEFINE_DRTAG(DrTag_RpcTunnelTransportRequestHeader, 1201, "RpcTunnelTransportRequestHeader")
DEFINE_DRTAG(DrTag_RpcTunnelTransportRequestBody, 1202, "RpcTunnelTransportRequestBody")
DEFINE_DRTAG(DrTag_RpcTunnelTransportResponse, 1203, "RpcTunnelTransportResponse")
DEFINE_DRTAG(DrTag_RpcTunnelTransportResponseHeader, 1204, "RpcTunnelTransportResponseHeader")
DEFINE_DRTAG(DrTag_RpcTunnelTransportResponseBody, 1205, "RpcTunnelTransportResponseBody")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionOpenRequest, 1206, "RpcTunnelTransportSessionOpenRequest")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionOpenResponse, 1207, "RpcTunnelTransportSessionOpenResponse")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionCloseRequest, 1208, "RpcTunnelTransportSessionCloseRequest")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionCloseResponse, 1209, "RpcTunnelTransportSessionCloseResponse")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionEnqueueRequest, 1210, "RpcTunnelTransportSessionEnqueueRequest")
DEFINE_DRTAG(DrTag_RpcRequest, 1211, "RpcRequest")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionEnqueueResponse, 1212, "RpcTunnelTransportSessionEnqueueResponse")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionPollRequest, 1213, "RpcTunnelTransportSessionPollRequest")
DEFINE_DRTAG(DrTag_RpcTunnelTransportSessionPollResponse, 1214, "RpcTunnelTransportSessionPollResponse")
DEFINE_DRTAG(DrTag_RpcResponse, 1215, "RpcResponse")
DEFINE_DRTAG(DrTag_RpcRequestHeader, 1216, "RpcRequestHeader")
DEFINE_DRTAG(DrTag_RpcGenerateEmptyResultRequest, 1217, "RpcGenerateEmptyResultRequest")
DEFINE_DRTAG(DrTag_RpcResponseHeader, 1218, "RpcResponseHeader")
DEFINE_DRTAG(DrTag_RpcSessionKey, 1219, "RpcSessionKey")
DEFINE_DRTAG(DrTag_RpcProxySendMessageRequest, 1220, "RpcProxySendMessageRequest")
DEFINE_DRTAG(DrTag_RpcProxySendMessageResponse, 1221, "RpcProxySendMessageResponse")
DEFINE_DRTAG(DrTag_DrWrappedProtocolMessage, 1222, "DrWrappedProtocolMessage")
DEFINE_DRTAG(DrTag_RpcRequestPacketHeader, 1223, "RpcRequestPacketHeader")
DEFINE_DRTAG(DrTag_RpcResponsePacketHeader, 1224, "RpcResponsePacketHeader")
DEFINE_DRTAG(DrTag_CompoundOpaqueKey, 1225, "CompundOpaqueKey")
DEFINE_DRTAG(DrTag_RpcRequestPacket, 1226, "RpcRequestPacket")
DEFINE_DRTAG(DrTag_RpcResponsePacket, 1227, "RpcResponsePacket")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

//
// cosmos resource selector Tags
//
DEFINE_DRTAG(DrTag_GetResourceListRequest, 1300, "DrTag_GetResourceListRequest")
DEFINE_DRTAG(DrTag_GetResourceListResponse, 1301, "DrTag_GetResourceListResponse")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// base protocol tags
DEFINE_DRTAG(DrTag_DrMessage, 9000, "DrMessage")
DEFINE_DRTAG(DrTag_DrClientMessageHeader, 9001, "DrClientMessageHeader")
DEFINE_DRTAG(DrTag_DrServerMessageHeader, 9002, "DrServerMessageHeader")
DEFINE_DRTAG(DrTag_DrServiceDescriptor, 9003, "DrServiceDescriptor")
DEFINE_DRTAG(DrTag_DrExtentNodeInfo, 9004, "DrExtentNodeInfo")
DEFINE_DRTAG(DrTag_DrStreamInfo, 9005, "DrStreamInfo")
DEFINE_DRTAG(DrTag_DrExtentInfo, 9006, "DrExtentInfo")
DEFINE_DRTAG(DrTag_DrExtentInstanceInfo, 9007, "DrExtentInstanceInfo")
DEFINE_DRTAG(DrTag_DrExtentInstanceMetadata, 9008, "DrExtentInstanceMetadata")
DEFINE_DRTAG(DrTag_DrNameResolutionMapEntry, 9009, "DrNameResolutionMapEntry")
DEFINE_DRTAG(DrTag_DrEnSyncDirective, 9010, "DrEnSyncDirective")
DEFINE_DRTAG(DrTag_DrHostAndPort, 9011, "DrHostAndPort")
DEFINE_DRTAG(DrTag_DrSimpleProcessFile, 9012, "DrSimpleProcessFile")
DEFINE_DRTAG(DrTag_DrPingRequest, 9013, "DrPingRequest")
DEFINE_DRTAG(DrTag_DrPingResponse, 9014, "DrPingResponse")
DEFINE_DRTAG(DrTag_DrProcessConstraints, 9015, "DrProcessConstraints")
DEFINE_DRTAG(DrTag_DrNodeErrorInfo, 9016, "DrNodeErrorInfo")
DEFINE_DRTAG(DrTag_DrProcessInfo, 9017, "DrProcessInfo")
DEFINE_DRTAG(DrTag_DrProcessPropertyInfo, 9018, "DrProcessPropertyInfo")
DEFINE_DRTAG(DrTag_DrUserDescriptor, 9019, "DrUserDescriptor")
DEFINE_DRTAG(DrTag_DrUserTicket, 9020, "DrUserTicket")
DEFINE_DRTAG(DrTag_DrJobDescriptor, 9021, "DrJobDescriptor")
DEFINE_DRTAG(DrTag_DrJobTicket, 9022, "DrJobTicket")
DEFINE_DRTAG(DrTag_DrProcessDescriptor, 9023, "DrProcessDescriptor")
DEFINE_DRTAG(DrTag_DrProcessTicket, 9024, "DrProcessTicket")
DEFINE_DRTAG(DrTag_DrParentProcess, 9025, "DrParentProcess")
DEFINE_DRTAG(DrTag_DrRootProcess, 9026, "DrRootProcess")
DEFINE_DRTAG(DrTag_DrExtentInstanceMetadataSet, 9027, "DrExtentInstanceMetadataSet")
DEFINE_DRTAG(DrTag_DrDetailedError, 9028, "DrDetailedError")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// BUGBUG: These were added by XStore and should not be here, they will collide with Dryad integrations.
// TODO: consider breaking XStream interop to move them to the right place!

DEFINE_DRTAG(DrTag_ParamList, 9029, "ParamList" )
DEFINE_DRTAG(DrTag_ValueList, 9030, "ValueList" )
DEFINE_DRTAG(DrTag_ParamValueList, 9031, "ParamValueList")
DEFINE_DRTAG(DrTag_DrStreamPolicyInfo, 9032, "StreamPolicyInfo")

// WARNING: DO NOT INSERT REDDOG TAGS HERE!

// NOTE NOTE NOTE
//
// 10000-10999 are reserved for Dryad

// WARNING: DO NOT INSERT REDDOG TAGS HERE!


// NOTE NOTE NOTE
//
// 11000-11999 are reserved for xstore
//
//
// XStream Tags
//
// WARNING: DO NOT INSERT REDDOG TAGS BEFORE THIS POINT
// DO NOT REORDER, DELETE, OR CHANGE THE VALUE OF ANY ENTRY IN THIS FILE
DEFINE_DRTAG(XsTag_ClientMessageHeader, 11000, "ClientMessageHeader")
DEFINE_DRTAG(XsTag_ServerMessageHeader, 11001, "ServerMessageHeader")
DEFINE_DRTAG(XsTag_XStoreMessage, 11002, "XStoreMessage")
DEFINE_DRTAG(XsTag_XStoreMessageBody, 11003, "XStoreMessageBody")
DEFINE_DRTAG(DrTag_DrFailureInjectionRequest, 11004, "DrFailureInjectionRequest")
DEFINE_DRTAG(DrTag_DrFailureInjectionResponse, 11005, "DrFailureInjectionResponse")
DEFINE_DRTAG(DrTag_DrEnumFileDirRequest, 11006, "DrEnumFileDirRequest")
DEFINE_DRTAG(DrTag_DrEnumFileDirResponse, 11007, "DrEnumFileDirResponse")
DEFINE_DRTAG(DrTag_DrReadFileRequest, 11008, "DrReadFileRequest")
DEFINE_DRTAG(DrTag_DrReadFileResponse, 11009, "DrReadFileResponse")
DEFINE_DRTAG(DrTag_DrWriteFileRequest, 11010, "DrWriteFileRequest")
DEFINE_DRTAG(DrTag_DrWriteFileResponse, 11011, "DrWriteFileResponse")
DEFINE_DRTAG(DrTag_DrDirectoryEntry, 11012, "DrDirectoryEntry")
DEFINE_DRTAG(DrTag_EnAppendBlockInfo, 11013, "EnAppendBlockInfo" )
DEFINE_DRTAG(Drm_SetStreamPolicyRequest, 11014, "SetStreamPolicyRequest")
DEFINE_DRTAG(Drm_SetStreamPolicyResponse, 11015, "SetStreamPolicyResponse")

// SAMMCK: renamed from QueryStreamPolicyRequest to match the API
DEFINE_DRTAG(Drm_GetStreamPoliciesRequest, 11016, "GetStreamPolociesRequest")

// SAMMCK: renamed from QueryStreamPolicyResponse to match the API
DEFINE_DRTAG(Drm_GetStreamPoliciesResponse, 11017, "QueryStreamPolicyResponse")
DEFINE_DRTAG(Drm_PolicyList, 11018, "PolicyList") 
DEFINE_DRTAG(Drm_ENConfigChangeEvent, 11019, "ENConfigChangeEvent")
DEFINE_DRTAG(DrTag_DrCommandLineParams, 11020, "DrCommandLineParams" )
DEFINE_DRTAG(DrTag_DrCommandLineResults, 11021, "DrCommandLineResults" )
DEFINE_DRTAG(DrTag_XcPsScheduleProcessRequest, 11022, "XcPsScheduleProcessRequest")
DEFINE_DRTAG(DrTag_XcPsScheduleProcessResponse, 11023, "XcPsScheduleProcessResponse")
DEFINE_DRTAG(DrTag_DrExecuteCommandLineRequest, 11024, "DrExecuteCommandLineRequest" )
DEFINE_DRTAG(DrTag_DrExecuteCommandLineResponse, 11025, "DrExecuteCommandLineResponse" )
DEFINE_DRTAG(DrTag_DrReferencingStreamInfo, 11033, "ReferencingStreamInfo")
DEFINE_DRTAG(Drm_GetExtentInfoRequest, 11034, "GetExtentInfoRequest")
DEFINE_DRTAG(Drm_GetExtentInfoResponse, 11035, "GetExtentInfoResponse")
DEFINE_DRTAG(DrTag_EmbeddedDrmCommand, 11036, "EmbeddedDrmCommand")
DEFINE_DRTAG(DrTag_EmbeddedRslRequest, 11037, "EmbeddedRslRequest")
DEFINE_DRTAG(Drm_GetLazyReplicationInfo, 11038, "GetLazyReplicationInfo")
DEFINE_DRTAG(Drm_GetLazyReplicationInfoResponse, 11039, "GetLazyReplicationInfoResponse")
DEFINE_DRTAG(DrEnTag_RequestLatenciesDeprecated, 11040, "RequestLatenciesDeprecated")

// The following two tags are used for extent 
// recovery after DRM emergency rollback.
DEFINE_DRTAG(DrTag_RecoverAndAppendExtentsRequest, 11041, "RecoverAndAppendExtentsRequest")
DEFINE_DRTAG(DrTag_RecoverAndAppendExtentsResponse, 11042, "RecoverAndAppendExtentsResponse")

// The following two tags are used to enumerate unsealable
// extents following a rollback/recovery operation.
DEFINE_DRTAG(DrTag_EnumerateUnsealableExtentsRequest, 11043, "EnumerateUnsealableExtentsRequest")
DEFINE_DRTAG(DrTag_EnumerateUnsealableExtentsResponse, 11044, "EnumerateUnsealableExtentsResponse")

// The following two tags are deprecated and may be reused 
// once all known nodes have been upgraded.
DEFINE_DRTAG(DrTag_ProtocolMessageTimestampsDeprecated, 11045, "ProtocolMessageTimestampsDeprecated")
DEFINE_DRTAG(DrEnTag_RequestLatencies, 11046, "RequestLatencies")

// ADD ALL XSTREAM/XSTORE/REDDOG TAGS ABOVE THIS POINT!

