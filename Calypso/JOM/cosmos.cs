
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

namespace Microsoft.Research.Calypso.JobObjectModel
{
    using System.Text.RegularExpressions;
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Information about a standard Cosmos log entry.
    /// </summary>
    [Serializable]
    public class CosmosLogEntry : IParse
    {
        /// <summary>
        /// Message severity.
        /// </summary>
        public char Severity { get; protected set; }
        /// <summary>
        /// Timestamp.
        /// </summary>
        public DateTime Timestamp { get; protected set; }
        /// <summary>
        /// Subsystem logging the message.
        /// </summary>
        public string Subsystem { get; protected set; }
        /// <summary>
        /// Unique message string.
        /// </summary>
        public string Message { get; protected set; }
        /// <summary>
        /// Source file containing the code logging the message.
        /// </summary>
        public string SourceFile { get; protected set; }
        /// <summary>
        /// Source code function logging the message.
        /// </summary>
        public string SourceFunction { get; protected set; }
        /// <summary>
        /// Line number in source file of the logging statement.
        /// </summary>
        public int SourceLineNumber { get; protected set; }
        /// <summary>
        /// ID of process logging the message.
        /// </summary>
        public int ProcessId { get; protected set; }
        /// <summary>
        /// Id of thread logging the message.
        /// </summary>
        public int ThreadId { get; protected set; }
        /// <summary>
        /// Extra unformatted information about the message.
        /// </summary>
        public string ExtraInfo { get; protected set; }
        /// <summary>
        /// If true the message could not be parsed.
        /// </summary>
        public bool Malformed { get; protected set; }
        /// <summary>
        /// The line which produced by parsing this message.
        /// </summary>
        public string OriginalLogLine { get; protected set; }

        static Regex loglineregex =
           new Regex(@"(\w+),                # 1 severity
                       ([^,]+),              # 2 timestamp
                       (\w*),                # 3 level
                       (.*),                 # 4 message, free form!
                       SrcFile=""(\S+)""\s+  # 5 source file 
                       SrcFunc=""(\S+)""\s+  # 6 source function
                       SrcLine=""(\d+)""\s+  # 7 line
                       Pid=""(\d+)""\s+      # 8 process
                       Tid=""(\d+)""\s+      # 9 thread
                       TS=""(\w+)""\s*       # 10 ?
                       (String1=""(.*)"")?$  # 11,12 additional arguments
                        ",
               RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Allocate an empty log entry
        /// </summary>
        public CosmosLogEntry()
        {
            this.Malformed = true;
        }

        /// <summary>
        /// Allocate a log entry from a given string.
        /// </summary>
        /// <param name="line">String to initialize the log entry.</param>
        public CosmosLogEntry(string line)
        {
            this.Malformed = true;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            this.Parse(line);
        }

        /// <summary>
        /// Return 'true' if the log entry signals an error.
        /// </summary>
        public bool IsError { get { return !this.Malformed && (this.Severity == 'a' || this.Severity == 'e'); } }

        /// <summary>
        /// Convert a log entry to a string (returns the original source line).
        /// </summary>
        /// <returns>The original log line that was parsed.</returns>
        public override string ToString()
        {
            return this.OriginalLogLine;
        }

        /// <summary>
        /// Parse a log entry line.
        /// </summary>
        /// <param name="line">Dryad log line to be parsed.</param>
        public virtual void Parse(string line)
        // i,09/09/2008 10:41:43.829,Cosmos,Found cluster alias,SrcFile="csnameresolver.cpp" SrcFunc="CsNameResolver::BuildClusterAliasMap" SrcLine="537" Pid="1936" Tid="1552" TS="0x01C912A35300C167" String1="AliasCluster=codev2 RealCluster=cosmos02-dev-co1"
        {
            this.OriginalLogLine = line;

            Match m = loglineregex.Match(line);
            if (m.Success)
            {
                this.Severity = m.Groups[1].Value.ToCharArray()[0];
                this.Timestamp = DateTime.Parse(m.Groups[2].Value);
                this.Subsystem = m.Groups[3].Value;
                this.Message = m.Groups[4].Value;
                this.SourceFile = m.Groups[5].Value;
                this.SourceFunction = m.Groups[6].Value;
                this.SourceLineNumber = Int32.Parse(m.Groups[7].Value);
                this.ProcessId = Int32.Parse(m.Groups[8].Value);
                this.ThreadId = Int32.Parse(m.Groups[9].Value);
                if (m.Groups.Count >= 12)
                    // the String1 may be missing
                    this.ExtraInfo = m.Groups[12].Value;
                else
                    this.ExtraInfo = "";
                this.Malformed = false;
            }
            else
            {
                Trace.TraceInformation("Could not parse line as cosmos log entry:" + line);
            }
        }
    }

    /// <summary>
    /// An extended log entry is like a cosmos log entry, but it has a prefix: GUID,Machine
    /// </summary>
    [Serializable]
    public class ExtendedLogEntry : CosmosLogEntry
    {
        static Regex loglineregex =
            new Regex(@"([-0-9A-F]+), # 1 guid, inserted by reader
                (\S+),                # 2 machine
                (.),                  # 3 severity
                ([^,]+),              # 4 timestamp
                (\w+),                # 5 level
                (.+),                 # 6 message, free form!
                SrcFile=""(\S+)""\s+  # 7 source file 
                SrcFunc=""(\S+)""\s+  # 8 source function
                SrcLine=""(\d+)""\s+  # 9 line
                Pid=""(\d+)""\s+      # 10 process
                Tid=""(\d+)""\s+      # 11 thread
                TS=""(\w+)""\s*       # 12 ?
                (String1=""(.*)"")?$  # 14 additional arguments
                 ",
         RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// When parsing fails, use this simple parser.
        /// </summary>
        static Regex simpleloglineregex =
            new Regex(@"([-0-9A-F]+),        # 1 guid
                        (\w+),               # 2 machine
                        (.*)                 # 3 rest of LineRecord",
                             RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// The guid of the process that logged this entry.
        /// </summary>
        public string Guid { get; protected set; }
        /// <summary>
        /// The machine where the log entry resides.
        /// </summary>
        public string Machine { get; protected set; }

        /// <summary>
        /// Parse an extended log entry line.
        /// </summary>
        /// <param name="line">Dryad log line to be parsed.</param>
        public override void Parse(string line)
        // i,09/09/2008 10:41:43.829,Cosmos,Found cluster alias,SrcFile="csnameresolver.cpp" SrcFunc="CsNameResolver::BuildClusterAliasMap" SrcLine="537" Pid="1936" Tid="1552" TS="0x01C912A35300C167" String1="AliasCluster=codev2 RealCluster=cosmos02-dev-co1"
        {
            this.OriginalLogLine = line;

            Match m = loglineregex.Match(line);
            if (m.Success)
            {
                try
                {
                    this.Guid = m.Groups[1].Value;
                    this.Machine = m.Groups[2].Value;

                    this.Severity = m.Groups[3].Value.ToCharArray()[0];
                    this.Timestamp = DateTime.Parse(m.Groups[4].Value);
                    this.Subsystem = m.Groups[5].Value;
                    this.Message = m.Groups[6].Value;
                    this.SourceFile = m.Groups[7].Value;
                    this.SourceFunction = m.Groups[8].Value;
                    this.SourceLineNumber = Int32.Parse(m.Groups[9].Value);
                    this.ProcessId = Int32.Parse(m.Groups[10].Value);
                    this.ThreadId = Int32.Parse(m.Groups[11].Value);
                    if (m.Groups.Count >= 14)
                        // the String1 may be missing
                        this.ExtraInfo = m.Groups[14].Value;
                    else
                        this.ExtraInfo = "";
                    this.Malformed = false;
                }
                catch (FormatException)
                {
                    Trace.TraceInformation("Failed to parse as extended log entry: {0}", line);
                    this.Malformed = true;
                }
            }
            else this.Malformed = true;

            if (this.Malformed)
            // much simpler parsing
            {
                m = simpleloglineregex.Match(line);
                if (m.Success)
                {
                    this.Guid = m.Groups[1].Value;
                    this.Machine = m.Groups[2].Value;
                }
                else
                {
                    this.Guid = "?";
                    this.Machine = "?";
                }

                this.Severity = '?';
                this.Timestamp = DateTime.Now;
                this.Subsystem = "";
                this.Message = "";
                this.SourceFile = "";
                this.SourceFunction = "";
                this.SourceLineNumber = 0;
                this.ProcessId = 0;
                this.ThreadId = 0;
                this.ExtraInfo = "";
            }
        }
    }

    /// <summary>
    /// Log entries have changed when switching to Argentia.
    /// </summary>
    [Serializable]
    public class ArgentiaDryadLogEntry : IParse
    {
        static Regex loglineregex =
           new Regex(@"(\w),                 # 1 severity
                       (.*),                 # 2 source function
                       (\S+):                # 3 source file 
                       (\d+),                # 4 line number
                       (\S+)                 # 5 message
                        ",
               RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Message severity.
        /// </summary>
        public char Severity { get; protected set; }
        /// <summary>
        /// Source file containing the code logging the message.
        /// </summary>
        public string SourceFile { get; protected set; }
        /// <summary>
        /// Source code function logging the message.
        /// </summary>
        public string SourceFunction { get; protected set; }
        /// <summary>
        /// If true the message could not be parsed.
        /// </summary>
        public bool Malformed { get; protected set; }
        /// <summary>
        /// The line which produced by parsing this message.
        /// </summary>
        public string OriginalLogLine { get; protected set; }
        /// <summary>
        /// Line number in source file of the logging statement.
        /// </summary>
        public int SourceLineNumber { get; protected set; }
        /// <summary>
        /// Unique message string.
        /// </summary>
        public string Message { get; protected set; }

        /// <summary>
        /// Parse a line as a log message.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        public void Parse(string line)
        {
            this.OriginalLogLine = line;

            Match m = loglineregex.Match(line);
            if (m.Success)
            {
                this.Severity = m.Groups[1].Value.ToCharArray()[0];
                this.SourceFunction = m.Groups[2].Value;
                this.SourceFile = m.Groups[3].Value;
                this.SourceLineNumber = Int32.Parse(m.Groups[4].Value);
                this.Message = m.Groups[5].Value;
                this.Malformed = false;
            }
            else
            {
                this.Malformed = true;
            }
        }
    }
}
