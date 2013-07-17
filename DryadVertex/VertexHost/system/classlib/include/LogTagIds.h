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

//JC Check this file for unnecessary content.

//
// This file lists all the known tag values for logging.
//
// This file is included twice, with DeclareTag() defined to do different things, to initialize
// different data structures with the logging code.
//
// Add any new named log tags to this file
// The IDs MUST be sequential
// Update LogTag_End if you add a new one
//
// The value in quotes is the name under which it will appear in the log file;
// e.g. Filename="Foo"
//

// Use these as generic log tags parameters if you don't feel it makes sense to add a new
// log tag entry.  There really is no real overhead for adding more, though, so if you're
// going to log the same data item in more than a few places, you probably want to consider
// making a log tag for it, in case we want WatchDog to be able to key off it.
DeclareTag(LogTag_Int1,          "Int1",             LogTagType_Int32),
DeclareTag(LogTag_Int2,          "Int2",             LogTagType_Int32),
DeclareTag(LogTag_Int64_1,       "Int64_1",          LogTagType_Int64),
DeclareTag(LogTag_Int64_2,       "Int64_2",          LogTagType_Int64),
DeclareTag(LogTag_String1,       "String1",          LogTagType_String),
DeclareTag(LogTag_String2,       "String2",          LogTagType_String),
DeclareTag(LogTag_WString1,      "WString1",         LogTagType_WideString),
DeclareTag(LogTag_WString2,      "WString2",         LogTagType_WideString),
DeclareTag(LogTag_UInt1,         "UInt1",            LogTagType_UInt32),
DeclareTag(LogTag_UInt2,         "UInt2",            LogTagType_UInt32),
DeclareTag(LogTag_U32X1,         "Hex1",            LogTagType_Hex32),
DeclareTag(LogTag_U32X2,         "Hex2",            LogTagType_Hex32),
DeclareTag(LogTag_U64X1,         "Hex64_1",         LogTagType_Hex64),
DeclareTag(LogTag_U64X2,         "Hex64_2",         LogTagType_Hex64),
DeclareTag(LogTag_Float1,        "Float1",          LogTagType_Float),
DeclareTag(LogTag_Float2,        "Float2",          LogTagType_Float),

DeclareTag(LogTag_Filename,     "Filename",          LogTagType_String),
DeclareTag(LogTag_TargetServer, "TargetServer",      LogTagType_String),
DeclareTag(LogTag_QueryString,  "Q",                 LogTagType_String),
DeclareTag(LogTag_TraceID,      "TraceID",           LogTagType_String),
DeclareTag(LogTag_URL,          "URL",              LogTagType_String),
DeclareTag(LogTag_ErrorCode,    "ErrorCode",        LogTagType_UInt32),
DeclareTag(LogTag_ThreadID,    "ThreadID",          LogTagType_Int32),
DeclareTag(LogTag_Description, "Desc",              LogTagType_String),

// ids for ini error
DeclareTag(LogTag_Section,     "Section",           LogTagType_String),
DeclareTag(LogTag_Param1,      "Param1",            LogTagType_String),
DeclareTag(LogTag_Param2,      "Param2",            LogTagType_String),
DeclareTag(LogTag_Param3,      "Param3",            LogTagType_String),
DeclareTag(LogTag_Param4,      "Param4",            LogTagType_String),

// ids for SQL ODBC error
DeclareTag(LogTag_SQLMessage,          "SQLMessage",        LogTagType_String),
DeclareTag(LogTag_SQLErrorCode,        "SQLErrorCode",      LogTagType_Int32),
DeclareTag(LogTag_SQLState,            "SQLState",          LogTagType_String),

// ids for SQL BCP error
DeclareTag(LogTag_SQLBCPFunction,      "SQLBCPFunction",    LogTagType_String),
DeclareTag(LogTag_SQLBCPToTable,       "SQLBCPToTable",     LogTagType_String),
DeclareTag(LogTag_SQLBCPColumn,        "SQLBCPColumn",      LogTagType_Int32),
DeclareTag(LogTag_SQLBCPRowCountSum,   "SQLBCPRowCountSum", LogTagType_Int32),
DeclareTag(LogTag_SQLBCPRowCountInc,   "SQLBCPRowCountInc", LogTagType_Int32),
DeclareTag(LogTag_SQLBCPRowCountCur,   "SQLBCPRowCountCur", LogTagType_Int32),

// ids for aggregator machine status notifications
DeclareTag(LogTag_ISNName,        "ISNName", LogTagType_String),
DeclareTag(LogTag_ISNService,     "ISNService", LogTagType_String),
DeclareTag(LogTag_ISNFailureType, "ISNFailureType", LogTagType_String),

// ids for Caption Generator
DeclareTag(LogTag_DocId,            "DocId",            LogTagType_Hex64),
DeclareTag(LogTag_ContentChunkId,   "ContentChunkId",   LogTagType_Int32),

// ids for Netlib
DeclareTag(LogTag_Port,         "Port",             LogTagType_UInt32),
DeclareTag(LogTag_IP,           "IP",               LogTagType_String),
DeclareTag(LogTag_NumericIP,    "NumericIP",        LogTagType_Hex32),
DeclareTag(LogTag_LastError,    "LastError",        LogTagType_UInt32),

// ids for Fcslite
DeclareTag(LogTag_PacketId,             "PacketId",             LogTagType_UInt32),
DeclareTag(LogTag_ConfigName,           "ConfigName",           LogTagType_String),
DeclareTag(LogTag_ResultBase,           "ResultBase",           LogTagType_UInt32),
DeclareTag(LogTag_ResultsCount,         "ResultsCount",         LogTagType_UInt32),
DeclareTag(LogTag_MaxResultsPerHost,    "MaxResultsPerHost",    LogTagType_UInt32),
DeclareTag(LogTag_FcsOptions,           "FcsOptions",           LogTagType_Hex64),
DeclareTag(LogTag_AggregatorOptions,    "AggregatorOptions",    LogTagType_Hex64),
DeclareTag(LogTag_QueryOptions,         "QueryOptions",         LogTagType_Hex64),
DeclareTag(LogTag_CaptionOptions,       "CaptionOptions",       LogTagType_Hex64),
DeclareTag(LogTag_NextTierToQuery,      "NextTierToQuery",      LogTagType_UInt32),
DeclareTag(LogTag_MaxTiersInIndex,      "MaxTiersInIndex",      LogTagType_UInt32),
DeclareTag(LogTag_RowID,                "RowID",                LogTagType_UInt32),

// These should have been in the generic section but are here to be in the proper range
DeclareTag(LogTag_UInt641,       "UInt641",          LogTagType_UInt64),
DeclareTag(LogTag_UInt642,       "UInt642",          LogTagType_UInt64),

#if defined(_M_AMD64)
DeclareTag(LogTag_Sizet1,       "Size_t1",      LogTagType_UInt64),
DeclareTag(LogTag_Sizet2,       "Size_t2",      LogTagType_UInt64),
#else
DeclareTag(LogTag_Sizet1,       "Size_t1",      LogTagType_UInt32),
DeclareTag(LogTag_Sizet2,       "Size_t2",      LogTagType_UInt32),
#endif

DeclareTag(LogTag_ActiveMCP,    "Active",           LogTagType_Int32),
DeclareTag(LogTag_VoteCount,    "VoteCount",        LogTagType_Int32),

#if defined(_M_AMD64)
DeclareTag(LogTag_Ptr1,       "Ptr1",       LogTagType_Hex64),
DeclareTag(LogTag_Ptr2,       "Ptr2",       LogTagType_Hex64),
#else
DeclareTag(LogTag_Ptr1,       "Ptr1",       LogTagType_Hex32),
DeclareTag(LogTag_Ptr2,       "Ptr2",       LogTagType_Hex32),
#endif

DeclareTag(LogTag_Latitude, "Latitude",     LogTagType_Float),
DeclareTag(LogTag_Longitude, "Longitude",    LogTagType_Float),

DeclareTag(LogTag_VarString1,       "VarString1",          LogTagType_VarString),
DeclareTag(LogTag_VarString2,       "VarString2",          LogTagType_VarString),
DeclareTag(LogTag_VarWString1,      "VarWString1",         LogTagType_VarWideString),
DeclareTag(LogTag_VarWString2,      "VarWString2",         LogTagType_VarWideString),

// ids for service manager
DeclareTag(LogTag_PID,  "ProcessID",  LogTagType_UInt32),
DeclareTag(LogTag_PPID, "ParentProcessID", LogTagType_UInt32),

// ids for fex
DeclareTag(LogTag_FEXLatency,  "FEXLatency",  LogTagType_Float),
DeclareTag(LogTag_TLALatency, "TLALatency", LogTagType_Float),
DeclareTag(LogTag_CDGLatency,  "CDGLatency",  LogTagType_Float),
DeclareTag(LogTag_ISNLatency, "ISNLatency", LogTagType_Float),
DeclareTag(LogTag_FcsStatus, "FcsStatus", LogTagType_Hex64),
DeclareTag(LogTag_MachineNeeded, "MachineNeeded", LogTagType_UInt32),
DeclareTag(LogTag_MachineComplete, "MachineComplete", LogTagType_UInt32),
DeclareTag(LogTag_MachineTimedOut, "MachineTimedOut", LogTagType_UInt32),
DeclareTag(LogTag_Federator, "Federator",      LogTagType_String),
DeclareTag(LogTag_RawUrl, "RawUrl",      LogTagType_VarString),
DeclareTag(LogTag_StatusCode, "StatusCode",      LogTagType_UInt32),

// ISN Total Time
DeclareTag(LogTag_ISNMaxTotalMachine, "ISNMaxTotalMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxTotalLatency, "ISNMaxTotalLatency", LogTagType_Float),
DeclareTag(LogTag_ISNMaxTotalStartTime, "ISNMaxTotalStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgTotalLatency, "ISNAvgTotalLatency", LogTagType_Float),

// ISN Queue Time
DeclareTag(LogTag_ISNMaxQueueMachine, "ISNMaxQueueMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxQueueLatency, "ISNMaxQueueLatency", LogTagType_Float),
DeclareTag(LogTag_ISNMaxQueueStartTime, "ISNMaxQueueStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgQueueLatency, "ISNAvgQueueLatency", LogTagType_Float),

// ISN Ranker Time
DeclareTag(LogTag_ISNMaxRankerMachine, "ISNMaxRankerMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxRankerLatency, "ISNMaxRankerLatency", LogTagType_Float),
DeclareTag(LogTag_ISNMaxRankerStartTime, "ISNMaxRankerStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgRankerLatency, "ISNAvgRankerLatency", LogTagType_Float),

// ISN Pages Scored Time
DeclareTag(LogTag_ISNMaxPagesScoredMachine, "ISNMaxPagesScoredMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxPagesScored, "ISNMaxPagesScored", LogTagType_UInt64),
DeclareTag(LogTag_ISNMaxPagesScoredStartTime, "ISNMaxPagesScoredStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgPagesScored, "ISNAvgPagesScored", LogTagType_Float),

// ISN Pages Matched Time
DeclareTag(LogTag_ISNMaxPagesMatchedMachine, "ISNMaxPgsMatchedMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxPagesMatched, "ISNMaxPgsMatched", LogTagType_UInt64),
DeclareTag(LogTag_ISNMaxPagesMatchedStartTime, "ISNMaxPgsMatchedStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgPagesMatched, "ISNAvgPgsMatched", LogTagType_Float),

// ISN Pages In Corpus Time
DeclareTag(LogTag_ISNMaxPagesInCorpusMachine, "ISNMaxPgsInCorpusMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxPagesInCorpus, "ISNMaxPgsInCorpus", LogTagType_UInt64),
DeclareTag(LogTag_ISNMaxPagesInCorpusStartTime, "ISNMaxPgsInCorpusStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgPagesInCorpus, "ISNAvgPagesInCorpus", LogTagType_Float),

// ISN QueueLength Length
DeclareTag(LogTag_ISNMaxQueueLengthMachine, "ISNMaxQueueLengthMachine", LogTagType_String),
DeclareTag(LogTag_ISNMaxQueueLength, "ISNMaxQueueLength", LogTagType_Float),
DeclareTag(LogTag_ISNMaxQueueLengthStartTime, "ISNMaxQueueLengthStartTime", LogTagType_Time),
DeclareTag(LogTag_ISNAvgQueueLength, "ISNAvgQueueLength", LogTagType_Float),

// Reissue
DeclareTag(LogTag_ReissueLatency, "ReissueLatency", LogTagType_Float),
DeclareTag(LogTag_ReissueCount, "ReissueCount", LogTagType_UInt32),
DeclareTag(LogTag_MaxCDLatency, "MaxCDLatency", LogTagType_Float),
DeclareTag(LogTag_MachineAnswered, "MachineAnswered", LogTagType_UInt32),
DeclareTag(LogTag_MachineQueried, "MachineQueried", LogTagType_UInt32),
DeclareTag(LogTag_MaxTierQueried, "MaxTierQueried", LogTagType_UInt32),

// Version
DeclareTag(LogTag_CDVersion, "CDVersion", LogTagType_UInt32),
DeclareTag(LogTag_CDVersionCount, "CDVersionCount", LogTagType_UInt32),
DeclareTag(LogTag_IndexVersion, "IndexVersion", LogTagType_UInt64),
DeclareTag(LogTag_RankVersion, "RankVersion", LogTagType_UInt64),

// Watchdog
DeclareTag(LogTag_Property, "Property", LogTagType_String),
DeclareTag(LogTag_Level, "Level", LogTagType_String),
DeclareTag(LogTag_Machinename, "machinename", LogTagType_String),

// Caching
DeclareTag(LogTag_HitCount, "HitCount", LogTagType_UInt64),
DeclareTag(LogTag_TotalEstimatedMatches, "TotalEstimatedMatches", LogTagType_UInt64),

// fex filter tags
DeclareTag(LogTag_Mkt, "Mkt", LogTagType_String),
DeclareTag(LogTag_Flight, "Flight", LogTagType_String),
DeclareTag(LogTag_Brand, "Brand", LogTagType_String),
DeclareTag(LogTag_VariantID, "Variant", LogTagType_UInt32),

DeclareTag(LogTag_RequestTime, "RequestTime", LogTagType_String),
DeclareTag(LogTag_Method, "Method", LogTagType_String),
DeclareTag(LogTag_Host, "Host", LogTagType_String),
DeclareTag(LogTag_BytesRecv, "BytesRecv", LogTagType_UInt32),
DeclareTag(LogTag_BytesSent, "BytesSent", LogTagType_UInt32),
DeclareTag(LogTag_BlockedStatus, "BlockedStatus",      LogTagType_UInt32),
DeclareTag(LogTag_RequestCost, "RequestCost",      LogTagType_UInt32),
DeclareTag(LogTag_CacheHeader, "CacheHeader", LogTagType_String),
DeclareTag(LogTag_Latency, "Latency", LogTagType_Float),
DeclareTag(LogTag_UserAgent, "UserAgent", LogTagType_String),
DeclareTag(LogTag_Referer, "Referer", LogTagType_String),
DeclareTag(LogTag_Cookies, "Cookies", LogTagType_String),
DeclareTag(LogTag_GetHeaderXUpSubno, "GetHeaderXUpSubno", LogTagType_String),
DeclareTag(LogTag_GetHeaderXUpUpLink, "GetHeaderXUpUpLink", LogTagType_String),

DeclareTag(LogTag_RSLMsg, "Msg", LogTagType_String),
DeclareTag(LogTag_RSLMsgLen, "MsgLen", LogTagType_UInt32),
DeclareTag(LogTag_Offset, "Offset", LogTagType_UInt64),
DeclareTag(LogTag_RSLState, "State", LogTagType_UInt32),
DeclareTag(LogTag_RSLMemberId, "MemberId", LogTagType_UInt64),
DeclareTag(LogTag_RSLBallotId, "BallotId", LogTagType_UInt32),
DeclareTag(LogTag_RSLDecree, "Decree", LogTagType_UInt64),
DeclareTag(LogTag_RSLBallot, "Ballot", LogTagType_String),
DeclareTag(LogTag_RSLMsgVersion, "MsgVersion", LogTagType_UInt32),

DeclareTag(LogTag_OldIndexVersion, "OldIndexVersion", LogTagType_UInt64),
DeclareTag(LogTag_NewIndexVersion, "NewIndexVersion", LogTagType_UInt64),

// Clusterbuilder
DeclareTag(LogTag_NumArticlesLoaded, "NumArticlesLoaded", LogTagType_UInt32),
DeclareTag(LogTag_NumArticlesRefs, "NumArticleRefs", LogTagType_UInt32),
DeclareTag(LogTag_NumArticles, "NumArticles", LogTagType_UInt32),
DeclareTag(LogTag_NumClusterRefs, "NumClusterRefs", LogTagType_UInt32),
DeclareTag(LogTag_NumClusters, "NumClusters", LogTagType_UInt32),
DeclareTag(LogTag_NumLanguageModelRowRefs, "NumLanguageModelRowRefs", LogTagType_UInt32),
DeclareTag(LogTag_NumLanguageModelRows, "NumLanguageModelRows", LogTagType_UInt32),
DeclareTag(LogTag_NumChunksExpired, "NumChunksExpired", LogTagType_UInt32),
DeclareTag(LogTag_NumClustersExpired, "NumClustersExpired", LogTagType_UInt32),
DeclareTag(LogTag_NumArticlesExpired, "NumArticlesExpired", LogTagType_UInt32),
DeclareTag(LogTag_NumStopTokensLoaded, "NumStopTokensLoaded", LogTagType_UInt32),
DeclareTag(LogTag_NumNoClusterTokensLoaded, "NumNoClusterTokensLoaded", LogTagType_UInt32),

DeclareTag(LogTag_PrefixString, "Prefix", LogTagType_String),
DeclareTag(LogTag_PrefixOptions, "PrefixOptions", LogTagType_Hex64),

// ids for fex (latencies in ms)
DeclareTag(LogTag_FederationLatency,  "FederationLatency",  LogTagType_Float),
DeclareTag(LogTag_HttpSysLatency,  "HttpSysLatency",  LogTagType_Float),
DeclareTag(LogTag_TotalLatency,  "TotalLatency",  LogTagType_Float),

// Dryad
DeclareTag(LogTag_Cluster, "Cluster", LogTagType_String),
DeclareTag(LogTag_Namespace, "Namespace", LogTagType_String),
DeclareTag(LogTag_NodeName, "Node", LogTagType_String),
DeclareTag(LogTag_ServiceType, "SvcType", LogTagType_String),

DeclareTag(LogTag_Command, "Command", LogTagType_String),
DeclareTag(LogTag_Service, "Service", LogTagType_String),

DeclareTag(LogTag_OID, "OID", LogTagType_Hex64),
DeclareTag(LogTag_EID, "ExtentID", LogTagType_String),
DeclareTag(LogTag_RefCount, "RefCount", LogTagType_UInt32),

DeclareTag(LogTag_RemoteMachine,            "RemoteMachine",           LogTagType_String ),

// Other (non-cosmos) tags
DeclareTag(LogTag_APProxyCommandID, "ID", LogTagType_UInt64),

//Logging for Fex C Logs
DeclareTag(LogTag_QLocation, "QLoc", LogTagType_String),
DeclareTag(LogTag_QLatitude, "QLat", LogTagType_Float),
DeclareTag(LogTag_QLongitude, "QLong", LogTagType_Float),

//Windows Live Searchpane Action tracing
DeclareTag(LogTag_P4_ActionID, "ActionID", LogTagType_UInt32),
DeclareTag(LogTag_P4_SessionID, "SessionID", LogTagType_String),
DeclareTag(LogTag_P4_ActionTime, "ActionTime", LogTagType_String),
DeclareTag(LogTag_P4_ResultType, "ResultType", LogTagType_UInt32),
DeclareTag(LogTag_P4_Market, "Market", LogTagType_String),
DeclareTag(LogTag_P4_SearchSource, "SearchSource", LogTagType_UInt32),
DeclareTag(LogTag_P4_TargetPage, "TargetPage", LogTagType_UInt32),
DeclareTag(LogTag_P4_ExceptionID, "ExceptionID", LogTagType_UInt64),
DeclareTag(LogTag_P4_SearchTerm, "SearchTerm", LogTagType_String),
DeclareTag(LogTag_P4_ActiveURL, "ActiveURL", LogTagType_String),
DeclareTag(LogTag_P4_ExceptionMessage, "ExceptionMessage", LogTagType_String),
DeclareTag(LogTag_P4_QuickSearch, "QuickSearch", LogTagType_UInt32),
DeclareTag(LogTag_P4_AnswerType, "AnswerType", LogTagType_String),
DeclareTag(LogTag_P4_TutorialMode, "TutorialMode", LogTagType_UInt32),

//AppID Tracing for SOAP API
DeclareTag(LogTag_AppID, "AppID", LogTagType_String),

//Logging of Reverse IP Loc, Lat and Long for Fex C Logs
DeclareTag(LogTag_IPLocation, "IPLoc", LogTagType_String),
DeclareTag(LogTag_IPLatitude, "IPLat", LogTagType_Float),
DeclareTag(LogTag_IPLongitude, "IPLong", LogTagType_Float),

//adding one more latency type for logging
DeclareTag(LogTag_DPSLatency,  "DPSLatency",  LogTagType_Float),
DeclareTag(LogTag_RawQuery, "RawQuery", LogTagType_String),

// Answers stuff
DeclareTag(LogTag_Market,  "Market",  LogTagType_String),
DeclareTag(LogTag_Environment,  "Environment",  LogTagType_String),
DeclareTag(LogTag_QueryTokenID,  "QueryTokenID",  LogTagType_UInt64),
DeclareTag(LogTag_GrammarTokenID,  "GrammarTokenID",  LogTagType_UInt64),
DeclareTag(LogTag_AnswerRequest,  "AnsRequest", LogTagType_String),
DeclareTag(LogTag_AnswerResponse,  "AnsResponse", LogTagType_String),
DeclareTag(LogTag_AnswerServiceStatus,  "AnsServiceStatus", LogTagType_String),
DeclareTag(LogTag_AnswerLogVersion,  "LogVersion", LogTagType_UInt32),
DeclareTag(LogTag_MatchDuration,  "MatchDuration", LogTagType_UInt32),
DeclareTag(LogTag_FulfillDuration,  "FulfillDuration", LogTagType_UInt32),

// Latencies for C-logs
DeclareTag(LogTag_TotalLatency_C,      "TotLat",  LogTagType_Float),
DeclareTag(LogTag_DPSLatency_C,        "DPSLat",  LogTagType_Float),
DeclareTag(LogTag_HttpSysLatency_C,    "HttpSysLat",  LogTagType_Float),
DeclareTag(LogTag_FederationLatency_C, "FedLat",  LogTagType_Float),
DeclareTag(LogTag_FEXLatency_C,        "FEXLat",  LogTagType_Float),
DeclareTag(LogTag_Latency_C,           "Latency", LogTagType_Float),

//General CLogging
DeclareTag(LogTag_CLogVersion,     "CLogVersion", LogTagType_String),
DeclareTag(LogTag_FEXBuild,        "FexBuild", LogTagType_String),
DeclareTag(LogTag_DataCenter,      "DataCenter", LogTagType_String),

// Speller request parameters
DeclareTag(LogTag_SpellerTimeout,          "Timeout",           LogTagType_UInt32),
DeclareTag(LogTag_SpellerTargetCorrection, "TargetCorrection",  LogTagType_String),
DeclareTag(LogTag_SpellerConfig,           "Config",            LogTagType_String),
DeclareTag(LogTag_SpellerOptions,          "Options",           LogTagType_Hex64),

// Tag to identify spilling in indexserve
DeclareTag(LogTag_SpillStatus,             "SpillStatus",     LogTagType_UInt32),

// Tags for TLA Query Log
DeclareTag(LogTag_ISNSourceEnvironment, "ISNSourceEnvironment",   LogTagType_String),
DeclareTag(LogTag_CDGSourceEnvironment, "CDGSourceEnvironment",   LogTagType_String),
DeclareTag(LogTag_LocalEnvironment,     "LocalEnvironment",       LogTagType_String),
DeclareTag(LogTag_NumDocuments,         "NumDocuments",           LogTagType_UInt32),
DeclareTag(LogTag_NumDocumentsActual,   "NumDocumentsActual",     LogTagType_UInt32),
DeclareTag(LogTag_Tier1ISNLatency,      "Tier1ISNLatency",        LogTagType_Float),
DeclareTag(LogTag_Tier2ISNLatency,      "Tier2ISNLatency",        LogTagType_Float),

// tag for clusterbuilder
DeclareTag(LogTag_NumNoAutotermTokensLoaded, "NumNoAutotermTokens", LogTagType_UInt32),
DeclareTag(LogTag_NumEntityGazetteersLoaded, "NumEntityGazetteersLoaded", LogTagType_UInt32),
DeclareTag(LogTag_NumPeakTermsLoaded, "NumPeakTerms", LogTagType_UInt32),
DeclareTag(LogTag_NumHighCTRTermsLoaded, "NumHighCTRTerms", LogTagType_UInt32),
DeclareTag(LogTag_NumDailyPeakTermsLoaded, "NumDailyHighCTRTerms", LogTagType_UInt32),

// Answers Performance Monitor Results
DeclareTag(LogTag_APM_ExecutionTime,       "ExecutionTime",        LogTagType_String ),
DeclareTag(LogTag_APM_TestNumber,          "TestNumber",           LogTagType_String ),
DeclareTag(LogTag_APM_ExpectedAnswer,      "ExpectedAnswer",       LogTagType_String ),
DeclareTag(LogTag_APM_ExpectedScenario,    "ExpectedScenario",     LogTagType_String ),
DeclareTag(LogTag_APM_Environment,         "Environment",          LogTagType_String ),
DeclareTag(LogTag_APM_HostName,            "HostName",             LogTagType_String ),
DeclareTag(LogTag_APM_Port,                "Port",                 LogTagType_String ),
DeclareTag(LogTag_APM_APMQueryResult,      "APMQueryResult",       LogTagType_String ),
DeclareTag(LogTag_APM_AnswersResponseCode, "AnswersResponseCode",  LogTagType_String ),
DeclareTag(LogTag_APM_Latency,             "Latency",              LogTagType_UInt32 ),
DeclareTag(LogTag_APM_ActualAnswer,        "ActualAnswer",         LogTagType_String ),
DeclareTag(LogTag_APM_ProductionID,        "ProductionID",         LogTagType_String ),
DeclareTag(LogTag_APM_GrammarID,           "GrammarID",            LogTagType_String ),
DeclareTag(LogTag_APM_DataSet,             "DataSet",              LogTagType_String ),
DeclareTag(LogTag_APM_DataSetVersion,      "DataSetVersion",       LogTagType_String ),
DeclareTag(LogTag_APM_ActualScenario,      "ActualScenario",       LogTagType_String ),
DeclareTag(LogTag_APM_OutputVersion,       "Version",              LogTagType_UInt32 ),
DeclareTag(LogTag_APM_AlertStatus,         "AlertStatus",          LogTagType_UInt32 ),
DeclareTag(LogTag_APM_SuccessRate,         "SuccessRate",          LogTagType_UInt32 ),
// APM v2 XML blob is all one string.
DeclareTag(LogTag_APM_TestResultXML,       "TestResultXML",        LogTagType_String ),

// Speller Debugging Tags
DeclareTag(LogTag_Speller_Query,            "Query",                   LogTagType_String),
DeclareTag(LogTag_Speller_Status,           "PostWebSpellerStatus",    LogTagType_UInt32),
DeclareTag(LogTag_Speller_QueryType,        "QueryType",               LogTagType_UInt32),
DeclareTag(LogTag_Speller_Flags,            "SpellerFlags",            LogTagType_UInt32),
DeclareTag(LogTag_Speller_NumSuggestions,   "NumSuggestions",          LogTagType_UInt32),
DeclareTag(LogTag_Speller_Suggestion,       "Suggestion",              LogTagType_String),
DeclareTag(LogTag_Speller_PPLatency,        "PreprocessLatency",       LogTagType_Float),
DeclareTag(LogTag_Speller_URLDetectLatency, "URLDetectLatency",        LogTagType_Float),
DeclareTag(LogTag_Speller_CandGenLatency,   "CandGenLatency",          LogTagType_Float),
DeclareTag(LogTag_Speller_ViterbiLatency,   "ViterbiLatency",          LogTagType_Float),
DeclareTag(LogTag_Speller_LMLatency,        "LMLatency",               LogTagType_Float),
DeclareTag(LogTag_Speller_URLCheckLatency,  "URLCheckLatency",         LogTagType_Float),
DeclareTag(LogTag_Speller_ConfLatency,      "ConfLatency",             LogTagType_Float),
DeclareTag(LogTag_Speller_PreWebLatency,    "PreWebSpellerLatency",    LogTagType_Float),
DeclareTag(LogTag_Speller_PostWebLatency,   "PostWebSpellerLatency",   LogTagType_Float),

// More Tags for TLA Query Log: request info from users
DeclareTag(LogTag_RequestIP,                "RequestIP",               LogTagType_String),
DeclareTag(LogTag_RequestMethod,            "RequestMethod",           LogTagType_String),
DeclareTag(LogTag_RequestDomain,            "RequestDomain",           LogTagType_String),
DeclareTag(LogTag_RequestUrl,               "RequestUrl",              LogTagType_String),
DeclareTag(LogTag_RequestReferer,           "RequestReferer",          LogTagType_String),
DeclareTag(LogTag_RequestAppID,             "RequestAppID",            LogTagType_String),
DeclareTag(LogTag_RequestPort,              "RequestPort",             LogTagType_UInt32),

// Log Service
DeclareTag(LogTag_IG,                       "IG",                      LogTagType_String),

// Tags for TLAQueryStatJoin Log
DeclareTag(LogTag_TLATraceID,          "TLATraceID",             LogTagType_String),
DeclareTag(LogTag_CacheTraceID,        "CacheTraceID",           LogTagType_String),
// tag for cache service
DeclareTag(LogTag_CacheStatus,         "CacheStatus",            LogTagType_Hex64),
DeclareTag(LogTag_CacheID,             "CacheID",                LogTagType_String),

// Vox Populi (answers) service tags
DeclareTag(LogTag_VoxPopuli_Rank,           "Rank",                    LogTagType_Float),
DeclareTag(LogTag_VoxPopuli_Pos,            "Pos",                     LogTagType_UInt32),

DeclareTag(LogTag_VarQueryString,           "Q",                       LogTagType_VarString),
DeclareTag(LogTag_VarMarket,                "Market",                  LogTagType_VarString),
DeclareTag(LogTag_AnswerService,            "Service",                 LogTagType_VarString),
DeclareTag(LogTag_AnswerScenario,           "Scenario",                LogTagType_VarString),
DeclareTag(LogTag_AnswerFeed,               "Feed",                    LogTagType_VarString),
DeclareTag(LogTag_AnswerEffectiveConstraint,"AnswerEffectiveConstraint",LogTagType_VarString),
DeclareTag(LogTag_EffectiveConstraint,      "EffectiveConstraint",     LogTagType_VarString),
DeclareTag(LogTag_AnswerEffectiveFlight,    "AnswerEffectiveFlight",   LogTagType_VarString),
DeclareTag(LogTag_EffectiveFlight,          "EffectiveFlight",         LogTagType_VarString),
DeclareTag(LogTag_Time1,                    "Time1",                   LogTagType_Time),
DeclareTag(LogTag_Time2,                    "Time2",                   LogTagType_Time),

// TLA Query Log Tags
DeclareTag(LogTag_RemoteFcsLatency,         "RemoteFcsLatency",        LogTagType_Float),
DeclareTag(LogTag_RemoteFcsNetworkLatency,  "RemoteFcsNetworkLatency", LogTagType_Float),

// tag for tiers queried within request
DeclareTag(LogTag_TiersQueried,        "TiersQueried",                 LogTagType_String),

// IDs for alterations
DeclareTag(LogTag_AlterationName,          "AlterationName",           LogTagType_String),

DeclareTag(LogTag_Mean,                     "Mean",                    LogTagType_Float),
DeclareTag(LogTag_WeightedMean,             "WeightedMean",            LogTagType_Float),
DeclareTag(LogTag_StdDev,                   "StdDev",                  LogTagType_Float),
DeclareTag(LogTag_Samples,                  "Samples",                 LogTagType_Int32),
DeclareTag(LogTag_Metric,                   "Metric",                  LogTagType_String),
DeclareTag(LogTag_MetricParameters,         "MetricParameters",        LogTagType_String),

DeclareTag(LogTag_VarFlight,                "Flight",                  LogTagType_VarString),

DeclareTag(LogTag_FcsResultsNumber,        "FcsResultsNumber",         LogTagType_UInt64),
DeclareTag(LogTag_QueryResults,            "QueryResults",             LogTagType_String),
DeclareTag(LogTag_DuplicatedQueryType,     "DuplicatedQueryType",      LogTagType_UInt32),

//
// Additional speller latencies for experimentation
//
DeclareTag(LogTag_Speller_RemoveDupCandLat,        "RemoveDupCandLat",         LogTagType_Float),
DeclareTag(LogTag_Speller_PopCandLat,              "PopCandLat",               LogTagType_Float),
DeclareTag(LogTag_Speller_FiltCandLat,             "FiltCandLat",              LogTagType_Float),
DeclareTag(LogTag_Speller_BldLatticeLat,           "BldLatticeLat",            LogTagType_Float),
DeclareTag(LogTag_Speller_CalcBestPathLat,         "CalcBestPathLat",          LogTagType_Float),
DeclareTag(LogTag_Speller_PrnLatticeLat,           "PrnLatticeLat",            LogTagType_Float),
DeclareTag(LogTag_Speller_GetTopPathsLat,          "GenTopPathsLat",           LogTagType_Float),
DeclareTag(LogTag_Speller_ScorePathLat,            "ScorePathLat",             LogTagType_Float),
DeclareTag(LogTag_Speller_TokenizeLat,             "TokenizeLat",              LogTagType_Float),
DeclareTag(LogTag_Speller_FcsParseLat,             "FcsParseLat",              LogTagType_Float),
DeclareTag(LogTag_Speller_GenCandLat,              "GenCandLat",               LogTagType_Float),
DeclareTag(LogTag_Speller_PreCheckLat,             "PreCheckLat",              LogTagType_Float),
DeclareTag(LogTag_Speller_JaJpSpellCheckLat,       "JaJpSpellCheckLat",        LogTagType_Float),
DeclareTag(LogTag_Speller_PreSpellCheckLat,        "PreSpellCheckLat",         LogTagType_Float),
DeclareTag(LogTag_Speller_SpellCheckLat,           "SpellCheckLat",            LogTagType_Float),
DeclareTag(LogTag_Speller_PostSpellCheckLat,       "PostSpellCheckLat",        LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat1,         "PhraseCheckLat1",          LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat2,         "PhraseCheckLat2",          LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat3,         "PhraseCheckLat3",          LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat4,         "PhraseCheckLat4",          LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat5,         "PhraseCheckLat5",          LogTagType_Float),
DeclareTag(LogTag_Speller_PhraseCheckLat6,         "PhraseCheckLat6",          LogTagType_Float),

//
// Tag ids for Commerce Answer Service (CAS) metadata logging
//
DeclareTag(LogTag_CAS_ProductID,            "ProductID",                     LogTagType_VarString),
DeclareTag(LogTag_CAS_VendID,               "VendorID",                      LogTagType_VarString),
DeclareTag(LogTag_CAS_Shingle,              "Shingle",                       LogTagType_VarString),
DeclareTag(LogTag_CAS_MSNShopItemID,        "MSNShopItemID",                 LogTagType_VarString),
DeclareTag(LogTag_CAS_TraceID,              "TraceID",                       LogTagType_VarString),
DeclareTag(LogTag_CAS_PTraceID,             "PTraceID",                      LogTagType_String),
DeclareTag(LogTag_CAS_RawQuery,             "RawQuery",                      LogTagType_VarString),
DeclareTag(LogTag_CAS_Metadata,             "CASMetadata",                   LogTagType_String),
DeclareTag(LogTag_CAS_CommDocType,          "CommDocType",                   LogTagType_String),
DeclareTag(LogTag_CAS_DpID,                 "DpID",                          LogTagType_VarString),
DeclareTag(LogTag_CAS_Category,             "Category",                      LogTagType_VarString),
DeclareTag(LogTag_CAS_MCATId,               "MCATID",                        LogTagType_VarString),
DeclareTag(LogTag_CAS_Scenario,             "Scenario",                      LogTagType_String),
DeclareTag(LogTag_CAS_Brand,                "Brand",                         LogTagType_VarString),
DeclareTag(LogTag_CAS_ProductName,          "ProductName",                   LogTagType_VarString),
DeclareTag(LogTag_CAS_ProductLine,          "ProductLine",                   LogTagType_VarString),
DeclareTag(LogTag_CAS_ReviewRating,         "ReviewRating",                  LogTagType_VarString),
DeclareTag(LogTag_CAS_Title,                "Title",                         LogTagType_VarString),
DeclareTag(LogTag_CAS_ResultPosition,       "ResultPosition",                LogTagType_Int32),
DeclareTag(LogTag_CAS_FeatureName,          "FeatureName",                   LogTagType_VarString),
DeclareTag(LogTag_CAS_CRFUsedToMatch,       "CRFUsedToMatch",                LogTagType_Int32),
DeclareTag(LogTag_CAS_CRFConfidenceLevel,   "CRFConfidenceLevel",            LogTagType_Float),
DeclareTag(LogTag_CAS_QDRConfidenceLevel,   "QDRConfidenceLevel",            LogTagType_Float),
DeclareTag(LogTag_CAS_CRFAlteredRawQuery,   "CRFAlteredRawQuery",            LogTagType_String),
DeclareTag(LogTag_CAS_CRFLabeledQuery,      "CRFLabeledQuery",               LogTagType_String),

// WebPM Performance Monitor Results
DeclareTag(LogTag_WebPM_CurrentEnvironment,  "CurrentEnvironment",   LogTagType_String ),
DeclareTag(LogTag_WebPM_TargetEnvironment,   "TargetEnvironment",    LogTagType_String ),
DeclareTag(LogTag_WebPM_TargetEnvironmentName,"TargetEnvironmentName",LogTagType_String ),
DeclareTag(LogTag_WebPM_TargetPort,          "Port",                 LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_RAWQuery,            "RAWQuery",             LogTagType_String ),
DeclareTag(LogTag_WebPM_QueryResult,         "QueryResult",          LogTagType_String ),
DeclareTag(LogTag_WebPM_ResponseStatus,      "ResponseStatus",       LogTagType_UInt64 ),
DeclareTag(LogTag_WebPM_HttpResponseCode,    "HttpResponseCode",     LogTagType_String ),
DeclareTag(LogTag_WebPM_E2ELatency,          "E2ELatency",           LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_FcsCacheFindLatency, "FcsCacheFindLatency",  LogTagType_Float ),
DeclareTag(LogTag_WebPM_FcsISNLatency,       "FcsISNLatency",        LogTagType_Float ),
DeclareTag(LogTag_WebPM_FcsCDGLatency,       "FcsCDGLatency",        LogTagType_Float ),
DeclareTag(LogTag_WebPM_FcsTotalLatency,     "FcsTotalLatency",      LogTagType_Float ),
DeclareTag(LogTag_WebPM_ProductionID,        "ProductionID",         LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_FDRSourceISNEnv,     "FDRSourceISNEnv",      LogTagType_String ),
DeclareTag(LogTag_WebPM_FDRSourceCDGEnv,     "FDRSourceCDGEnv",      LogTagType_String ),
DeclareTag(LogTag_WebPM_MaxTierQueried,      "MaxTierQueried",       LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_ISNResponseFoundInCache, "ISNResponseFoundInCache", LogTagType_String ),
DeclareTag(LogTag_WebPM_CDGResponseFoundInCache, "CDGResponseFoundInCache", LogTagType_String ),
DeclareTag(LogTag_WebPM_OutputVersion,       "Version",              LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_AlertStatus,         "AlertStatus",          LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_FcsEstimatedMatches, "FcsEstimatedMatches",  LogTagType_UInt64 ),
DeclareTag(LogTag_WebPM_FcsNumberofResults,  "FcsNumberofResults",   LogTagType_UInt64 ),
DeclareTag(LogTag_WebPM_ExecutionTime,       "ExecutionTime",        LogTagType_String ),
DeclareTag(LogTag_WebPM_TestNumber,          "TestNumber",           LogTagType_UInt32 ),
DeclareTag(LogTag_WebPM_RemoteEnvironmentName,"RemoteEnvironmentName",LogTagType_String ),
DeclareTag(LogTag_WebPM_NumberOfHops,        "NumberOfHops",         LogTagType_UInt32 ),

// Moonshot information
DeclareTag(LogTag_Docs,                     "DODR",                    LogTagType_String),

DeclareTag(LogTag_FEXAnswerServiceName,        "AnswerServiceName",       LogTagType_String),
DeclareTag(LogTag_FEXAnswerScenario,           "AnswerScenario",          LogTagType_String),
DeclareTag(LogTag_FEXAnswerUXDisplayHint,      "AnswerUXDisplayHint",     LogTagType_String),

DeclareTag(LogTag_VarConstraint,               "Constraint",              LogTagType_VarString),
DeclareTag(LogTag_Constraint,                  "Constraint",              LogTagType_String),

// Count of adult documents rendered
DeclareTag(LogTag_AdultDocumentCount,		 "AdultDocumentCount",		LogTagType_UInt32),

// User state
DeclareTag(LogTag_ULS,                      "ULS",                     LogTagType_UInt32),

DeclareTag(LogTag_IndexName,               "IndexName",                LogTagType_String),
DeclareTag(LogTag_ResultSource,            "ResultSource",             LogTagType_String),
DeclareTag(LogTag_MUID,                     "MUID",                    LogTagType_String),

// ApSDk tags 
DeclareTag(LogTag_ApCommMsgVersion,         "Msg",             LogTagType_String),
DeclareTag(LogTag_ApCommSeqNo,              "SeqNo",           LogTagType_UInt64),

// Performance Tracking
DeclareTag(LogTag_LoadBalanceId,      "LoadBalanceId",     LogTagType_String),
DeclareTag(LogTag_LoadBalanceTS,      "LoadBalanceTS",     LogTagType_String),

// FEX / FrontDoor 
DeclareTag(LogTag_FrontDoorAction,    "FDAction",          LogTagType_String),
DeclareTag(LogTag_FullUrl,             "FullUrl",          LogTagType_String),

DeclareTag(LogTag_JSON,                "JSON",              LogTagType_String),

//webmaster
DeclareTag(LogTag_Email, "Email", LogTagType_String),
DeclareTag(LogTag_PUID, "PUID", LogTagType_String),
DeclareTag(LogTag_UserProfileID, "UserProfileID", LogTagType_String),

// Query Diagnostic
DeclareTag(LogTag_QueryProcessLogVersion,   "LogVersion",      LogTagType_UInt32),
DeclareTag(LogTag_QueryProcessTracking,     "QPTracking",      LogTagType_String),

// This must be the final tag, and it must have type None
DeclareTag(LogTag_End,         "End",               LogTagType_None),
