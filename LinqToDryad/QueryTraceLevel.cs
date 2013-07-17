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

using System;
using Microsoft.Hpc.Dryad;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Trace levels for HpcQuery runtime
    /// </summary>
    public enum HpcQueryTraceLevel : int
    {
        // Use internal constants since public type cannot be easily shared acrossed DLLs w/o resulting in ambiquities
        Off = Constants.traceOffNum,
        Critical = Constants.traceCriticalNum,
        Error = Constants.traceErrorNum,
        Warning = Constants.traceWarningNum,
        Information = Constants.traceInfoNum,
        Verbose = Constants.traceVerboseNum
    }
}
