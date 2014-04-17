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

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Trace levels for DryadLinqQuery runtime.
    /// </summary>
    public enum QueryTraceLevel : int
    {
        /// <summary>
        /// This level turns off DryadLINQ logging completely.
        /// </summary>
        Off = Constants.TraceOffLevel,

        /// <summary>
        /// This level only logs critical log entries.
        /// </summary>
        Critical = Constants.TraceCriticalLevel,

        /// <summary>
        /// This level logs error or critical log entries.
        /// </summary>
        Error = Constants.TraceErrorLevel,

        /// <summary>
        /// This level logs warning or more critical log entries.
        /// </summary>
        Warning = Constants.TraceWarningLevel,

        /// <summary>
        /// This level logs information or more critical log entries.
        /// </summary>
        Information = Constants.TraceInfoLevel,

        /// <summary>
        /// This level logs all DryadLINQ log entries. 
        /// </summary>
        Verbose = Constants.TraceVerboseLevel
    }
}
