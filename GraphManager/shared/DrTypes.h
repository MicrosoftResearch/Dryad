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

#define MAX_UINT8  ((UINT8)-1)
#define MAX_UINT16 ((UINT16)-1)
#define MAX_UINT32 ((UINT32)-1)
#define MIN_INT32  ((INT32)0x80000000)
#define MAX_INT32  ((INT32)0x7FFFFFFF)      // 2147483647
#define MAX_UINT64 ((UINT64)-1)
#define MIN_INT64  ((INT64)0x8000000000000000I64)
#define MAX_INT64  0x7FFFFFFFFFFFFFFFi64

typedef INT64 DrTimeInterval;

static const DrTimeInterval DrTimeInterval_Infinite = (DrTimeInterval)MAX_INT64;
static const DrTimeInterval DrTimeInterval_NegativeInfinite = (DrTimeInterval)MIN_INT64;
static const DrTimeInterval DrTimeInterval_Zero = (DrTimeInterval)0;
static const DrTimeInterval DrTimeInterval_Quantum = (DrTimeInterval)1;
static const DrTimeInterval DrTimeInterval_100ns = DrTimeInterval_Quantum;
static const DrTimeInterval DrTimeInterval_Microsecond = DrTimeInterval_100ns * 10;
static const DrTimeInterval DrTimeInterval_Millisecond = DrTimeInterval_Microsecond * 1000;
static const DrTimeInterval DrTimeInterval_Second = DrTimeInterval_Millisecond * 1000;
static const DrTimeInterval DrTimeInterval_Minute = DrTimeInterval_Second * 60;
static const DrTimeInterval DrTimeInterval_Hour = DrTimeInterval_Minute * 60;
static const DrTimeInterval DrTimeInterval_Day = DrTimeInterval_Hour * 24;
static const DrTimeInterval DrTimeInterval_Week = DrTimeInterval_Day * 7;

typedef UINT64 DrDateTime;

static const DrDateTime DrDateTime_Never = (DrDateTime) MAX_UINT64;
static const DrDateTime DrDateTime_LongAgo = (DrDateTime) 0;
