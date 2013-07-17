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

// There must *only* be DEFINE_DRYADTAG directives in this file

DEFINE_DRYADTAG(DryadTag_InputChannelDescription, 10000, "InputChannelDescription", InputChannelDescription)
DEFINE_DRYADTAG(DryadTag_OutputChannelDescription, 10001, "OutputChannelDescription", OutputChannelDescription)
DEFINE_DRYADTAG(DryadTag_VertexProcessStatus, 10002, "VertexProcessStatusBlock", VertexProcessStatus)
DEFINE_DRYADTAG(DryadTag_VertexStatus, 10003, "VertexStatusBlock", VertexStatus)
DEFINE_DRYADTAG(DryadTag_VertexCommand, 10004, "VertexCommandBlock", VertexCommandBlock)
DEFINE_DRYADTAG(DryadTag_ItemStart, 10005, "ItemStartBlock", MetaData)
DEFINE_DRYADTAG(DryadTag_ItemEnd, 10006, "ItemEndBlock", MetaData)
DEFINE_DRYADTAG(DryadTag_ChannelMetaData, 10007, "ChannelMetaData", MetaData)
DEFINE_DRYADTAG(DryadTag_VertexMetaData, 10008, "VertexMetaData", MetaData)
DEFINE_DRYADTAG(DryadTag_ArgumentArray, 10009, "ArgumentArray", MetaData)
DEFINE_DRYADTAG(DryadTag_VertexArray, 10010, "VertexArray", MetaData)
DEFINE_DRYADTAG(DryadTag_VertexInfo, 10011, "VertexInfo", MetaData)
DEFINE_DRYADTAG(DryadTag_EdgeArray, 10012, "EdgeArray", MetaData)
DEFINE_DRYADTAG(DryadTag_EdgeInfo, 10013, "EdgeInfo", MetaData)
DEFINE_DRYADTAG(DryadTag_GraphDescription, 10014, "GraphDescription", MetaData)
DEFINE_DRYADTAG(DryadTag_RSCAReturnMachine, 10015, "ReturnMachine", MetaData)
DEFINE_DRYADTAG(DryadTag_RSCAEnqueueProcess, 10016, "EnqueueProcess", MetaData)
DEFINE_DRYADTAG(DryadTag_RSCAReportFailedMachine, 10017, "ReportFailedMachine", MetaData)
DEFINE_DRYADTAG(DryadTag_RSCADiscardProcess, 10018, "DiscardProcess", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientResponse, 10019, "RSClientResponse", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientRootProcessRequest, 10020, "RSClientRootProcessRequest", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientRootProcessResponse, 10021, "RSClientRootProcessResponse", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientInitializeRequest, 10022, "RSClientInitializeRequest", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientActionRequest, 10023, "RSClientActionRequest", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientStatusRequest, 10024, "RSClientStatusRequest", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientMatch, 10025, "RSClientMatch", MetaData)
DEFINE_DRYADTAG(DryadTag_RSRevocation, 10026, "RSRevocation", MetaData)
DEFINE_DRYADTAG(DryadTag_RSClientCommand, 10027, "RSClientCommand", MetaData)
