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
// This header file is designed to be included twice by the
// old autopilot logging code, once by new logging.
//
// This file meant to be used by
// non-autopilot code to define logids without having to
// modify any the autopilot SDK files (not really necessary
// with new logging).
//

#ifndef USE_DRTRACE

#if !defined(APSDK_CUSTOMIZED_LOGIDS_STRING)
#error This file is not meant to be included directly. APSDK_CUSTOMIZED_LOGIDS_STRING is not defined.
#endif

// To add a new log id, append a line below in the form of:
//      APSDK_CUSTOMIZED_LOGIDS_STRING(Foo),
// This will end up creating two things, an enum LogIDEx_Foo of enum type LogID and a string "Foo"
// as the value for g_LogIDNames[LogIDEx_Foo].

    APSDK_CUSTOMIZED_LOGIDS_STRING(SDKBasicSampleWatchdog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(SDKSample),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AnswersMatchLog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AnswersRemoteTLAPreLog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(VoxPopuliRatingLog), 
    APSDK_CUSTOMIZED_LOGIDS_STRING(DryadAudit),
    APSDK_CUSTOMIZED_LOGIDS_STRING(DryadRS),
    APSDK_CUSTOMIZED_LOGIDS_STRING(DryadCache),
    APSDK_CUSTOMIZED_LOGIDS_STRING(DryadSimulator),
    APSDK_CUSTOMIZED_LOGIDS_STRING(QUERY_PROCESSING),
    APSDK_CUSTOMIZED_LOGIDS_STRING(QUERY_PARSING),
    APSDK_CUSTOMIZED_LOGIDS_STRING(QUERY_REWRITING),
    APSDK_CUSTOMIZED_LOGIDS_STRING(QUERY_CLASSIFICATION),
    APSDK_CUSTOMIZED_LOGIDS_STRING(WatchDogClient),
    APSDK_CUSTOMIZED_LOGIDS_STRING(WatchDogServer),
    APSDK_CUSTOMIZED_LOGIDS_STRING(CacheSync),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AdServiceHttpRequestLog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AdServiceHttpResponseLog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AdServiceXMLResponseLog),
    APSDK_CUSTOMIZED_LOGIDS_STRING(AnswersFrameworkDebug),
    APSDK_CUSTOMIZED_LOGIDS_STRING(NoCodeUser),
#else

//
// With new logging, just define the value (or, better yet, do it somewhere in your project)
//

#define LogIDEx_SDKBasicSampleWatchdog          "SDKBasicSampleWatchdog"
#define LogIDEx_SDKSample                       "SDKSample"
#define LogIDEx_AnswersMatchLog                 "AnswersMatchLog"
#define LogIDEx_AnswersRemoteTLAPreLog          "AnswersRemoteTLAPreLog"
#define LogIDEx_VoxPopuliRatingLog              "VoxPopuliRatingLog" 
#define LogIDEx_DryadAudit                     "DryadAudit"
#define LogIDEx_DryadRS                        "DryadRS"
#define LogIDEx_DryadCache                     "DryadCache"
#define LogIDEx_DryadSimulator                  "DryadSimulator"
#define LogIDEx_QUERY_PROCESSING                "QUERY_PROCESSING"
#define LogIDEx_QUERY_PARSING                   "QUERY_PARSING"
#define LogIDEx_QUERY_REWRITING                 "QUERY_REWRITING"
#define LogIDEx_QUERY_CLASSIFICATION            "QUERY_CLASSIFICATION"
#define LogIDEx_AdServiceHttpRequestLog         "AdServiceHttpRequestLog"
#define LogIDEx_AdServiceHttpResponseLog        "AdServiceHttpResponseLog"
#define LogIDEx_AdServiceXMLResponseLog         "AdserviceXMLResponseLog"
#define LogIDEx_AnswersFrameworkDebug           "AnswersFrameworkDebug"
#define LogIDEx_NoCodeUser                      "NoCodeUser"

#endif

