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

//JC Check this file for redundant information.

// Dryad properties (see propertybag.h)

// General purpose parameters

// (BeginTag, UInt16 tag) marks the beginning of a set of data
// (EndTag, UInt16 tag) marks the end of the a set of data
// See cstags.h for a description of known tag values

// This file must consist only of DEFINE_DRPROPERTY statements

// Current Allowed types are:
//
// UInt32
// UInt64
// String
// Guid
// TimeStamp
// TimeInterval
// BeginTag
// EndTag
// DrError
// DrExitCode
// Blob
// EnvironmentBlock

// ******************************************************************************************************
//
//              DO NOT RESERVE RANGES IN THIS FILE, USE NEXT AVAILABLE ID AND LEAVE NO HOLES
//
// ******************************************************************************************************

// This is a BeginTag
DEFINE_DRPROPERTY(Prop_Dryad_BeginTag, PROP_SHORTATOM(0x1200), BeginTag, "BeginTag")

// This is an EndTag
DEFINE_DRPROPERTY(Prop_Dryad_EndTag, PROP_SHORTATOM(0x1201), EndTag, "EndTag")

// This is a Blob value - a win32 environment variable block
DEFINE_DRPROPERTY(Prop_Dryad_EnvironmentBlock, PROP_LONGATOM(0x129A), EnvironmentBlock, "EnvironmentBlock")

// This is a UInt16
DEFINE_DRPROPERTY(Prop_Dryad_Port, PROP_SHORTATOM(0x1294), UInt16, "Port")

DEFINE_DRPROPERTY(Prop_Dryad_ShortHostName, PROP_SHORTATOM(0x1293), String, "ShortHostName")
DEFINE_DRPROPERTY(Prop_Dryad_LongHostName, PROP_LONGATOM(0x1037), String, "LongHostName")

// This is a String
DEFINE_DRPROPERTY(Prop_Dryad_PodName, PROP_LONGATOM(0x12C6), String, "PodName")

// This is a UInt32 - gives number of entries that follow
DEFINE_DRPROPERTY(Prop_Dryad_NumEntries, PROP_SHORTATOM(0x124C), UInt32, "NumEntries")

// This is a UInt32 value - a pointer to the primary host in a host list
DEFINE_DRPROPERTY(Prop_Dryad_PrimaryHost, PROP_SHORTATOM(0x12A8), UInt32, "PrimaryHost")

// This is a UInt32 value - a pointer to the next host to return in a host list
DEFINE_DRPROPERTY(Prop_Dryad_NextHost, PROP_SHORTATOM(0x12A9), UInt32, "NextHost")
