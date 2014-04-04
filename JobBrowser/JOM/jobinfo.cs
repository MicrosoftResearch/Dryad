
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

#undef USE_LINQ_TO_DRYAD
#undef USE_HPC

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Microsoft.Research.Calypso.Tools;
using System.Diagnostics;

namespace Microsoft.Research.Calypso.JobObjectModel
{
    /// <summary>
    /// Exception throw by Calypso when it cannot understand the structure of a Dryad/DryadLINQ job.
    /// </summary>
    public class CalypsoDryadException : Exception
    {
        /// <summary>
        /// Create a new CalypsoDryadException.
        /// </summary>
        /// <param name="message">Message conveyed by the exception.</param>
        public CalypsoDryadException(string message) : base(message) { }
    }

    /// <summary>
    /// Classes providing a parsing from string routine.
    /// </summary>
    public interface IParse
    {
        /// <summary>
        /// Parse one line.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        void Parse(string line);
    }

    /// <summary>
    /// One stage (a set of vertices) in a DryadLINQ job.
    /// </summary>
    public class DryadLinqJobStage
    {
        /// <summary>
        /// Number of vertices defined in the static plan; 0 if unknown.
        /// This field must be set explicitly, it is not computed by the constructor, since the information is not available in the set of vertices.
        /// </summary>
        public int StaticVertexCount { get; set; }
        /// <summary>
        /// Stage name.
        /// </summary>
        public string Name { get; protected set; }
        /// <summary>
        /// List of vertices in the stage.
        /// </summary>
        readonly IEnumerable<ExecutedVertexInstance> vertices;
        /// <summary>
        /// Number of executed vertices; does not include abandoned vertices.
        /// </summary>
        public int TotalInitiatedVertices { get; protected set; }
        /// <summary>
        /// Created but not yet started.
        /// </summary>
        public int CreatedVertices { get; protected set; }
        /// <summary>
        /// Vertices that have started (may still be running).
        /// </summary>
        public int StartedVertices { get; protected set; }
        /// <summary>
        /// Vertices that have completed and then have been cancelled.
        /// </summary>
        public int InvalidatedVertices { get; protected set; }
        /// <summary>
        /// Number of failed vertices.
        /// </summary>
        public int FailedVertices { get; protected set; }
        /// <summary>
        /// Vertices that have completed successfully.
        /// </summary>
        public int SuccessfulVertices { get; protected set; }
        /// <summary>
        /// Number of cancelled vertices.
        /// </summary>
        public int CancelledVertices { get; protected set; }
        /// <summary>
        /// Number of vertices abandoned before running.
        /// </summary>
        public int AbandonedVertices { get; protected set; }
        /// <summary>
        /// Number of vertices cancelled by the remote scheduler.
        /// </summary>
        public int RevokedVertices { get; protected set; }
        /// <summary>
        /// How long has this stage been running?
        /// </summary>
        public TimeSpan RunningTime { get { return this.EndTime - this.StartTime; } }
        /// <summary>
        /// Time when first vertex in stage started.
        /// </summary>
        public DateTime StartTime { get; protected set; }
        /// <summary>
        /// Time when last vertex in stage finished.
        /// </summary>
        public DateTime EndTime { get; protected set; }
        /// <summary>
        /// Amount of data read (-1 if unknown).
        /// </summary>
        public long DataRead { get; protected set; }
        /// <summary>
        /// Amount of data written (-1 if unknown).
        /// </summary>
        public long DataWritten { get; protected set; }
        /// <summary>
        /// Information about the vertices executed in this stage.
        /// </summary>
        public IEnumerable<ExecutedVertexInstance> Vertices { get { return this.vertices; } }

        /// <summary>
        /// Create a DryadLinqJobStage from a set of vertices.
        /// </summary>
        /// <param name="stagename">Name of stage.</param>
        /// <param name="vertices">Set of vertices contained in the stage.</param>
        public DryadLinqJobStage(string stagename, List<ExecutedVertexInstance> vertices)
        {
            this.StaticVertexCount = 0; // not yet known
            this.DataRead = vertices.Select(v => v.DataRead).Sum();
            this.DataWritten = vertices.Select(v => v.DataWritten).Sum();
            if (this.DataRead < 0)
                this.DataRead = -1;
            if (this.DataWritten < 0)
                this.DataWritten = -1;

            this.vertices = vertices;
            this.Name = stagename;
            this.AbandonedVertices =
                this.FailedVertices =
                this.CancelledVertices =
                this.StartedVertices =
                this.SuccessfulVertices =
                this.CreatedVertices =
                this.InvalidatedVertices =
                this.RevokedVertices = 
                this.TotalInitiatedVertices = 0;

            foreach (var vertex in this.vertices)
            {
                this.TotalInitiatedVertices++;
                switch (vertex.State)
                {
                    case ExecutedVertexInstance.VertexState.Revoked:
                        this.RevokedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Abandoned:
                        this.AbandonedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Invalidated:
                        this.InvalidatedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Cancelled:
                        this.CancelledVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Failed:
                        this.FailedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Created:
                        this.CreatedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Started:
                        this.StartedVertices++;
                        break;
                    case ExecutedVertexInstance.VertexState.Successful:
                        this.SuccessfulVertices++;
                        break;
                    default:
                        throw new CalypsoDryadException("Unexpected vertex state " + vertex.State);
                }
            }
            this.TotalInitiatedVertices -= this.AbandonedVertices;
            if (this.TotalInitiatedVertices == 0)
                return;

            List<ExecutedVertexInstance> runVertices = this.vertices.Where(v => v.Start != DateTime.MinValue).ToList();
            if (runVertices.Count > 0) this.StartTime = runVertices.Select(v => v.Start).Min();
            this.EndTime = this.vertices.Select(v => v.End).Max();
        }
    }
    
    /// <summary>
    /// This class contains all the information about a Dryad job.
    /// </summary>
    public class DryadLinqJobInfo
    {
        /// <summary>
        /// A summary of the job.
        /// </summary>
        DryadLinqJobSummary jobSummary;
        /// <summary>
        /// The start time of the job manager.
        /// </summary>
        public DateTime StartJMTime {
            get
            {
                return this.ManagerVertex.Start;
            }
        }
        /// <summary>
        /// When was the job state last updated?
        /// </summary>
        public DateTime LastUpdatetime { get; private set; }
        /// <summary>
        /// The end time of the job.
        /// </summary>
        public DateTime EndTime {
            get
            {
                return this.ManagerVertex.End;
            }
        }
        /// <summary>
        /// How long has the job been running?
        /// </summary>
        public TimeSpan RunningTime { 
            get 
            {
                return this.ManagerVertex.RunningTime;
            } 
        }
        /// <summary>
        /// All the vertices for the job.
        /// </summary>
        JobVertices jobVertices;
        /// <summary>
        /// When parsing stdout save here last vertex with failure, to attach additional 
        /// error messages to it.
        /// </summary>
        ExecutedVertexInstance lastFailedVertex;
        /// <summary>
        /// The path to the stdout of the job manager 
        /// </summary>
        IClusterResidentObject stdoutpath;
        /// <summary>
        /// The name of the Job
        /// </summary>
        public string JobName { get { return this.Summary.Name; } }
        /// <summary>
        /// Error code of the dryadlinq job.
        /// </summary>
        public string ErrorCode { get; set; }
        /// <summary>
        /// The job manager vertex for this job.
        /// </summary>
        public ExecutedVertexInstance ManagerVertex { get; set; }
        /// <summary>
        /// Is the standard output complete or truncated?
        /// </summary>
        public bool ManagerStdoutIncomplete { get; protected set; }
        /// <summary>
        /// True if the information to create this jobinfo is no longer available.
        /// </summary>
        public bool JobInfoCannotBeCollected { get; protected set; }
        /// <summary>
        /// Number of stages that have started execution.
        /// </summary>
        public int ExecutedStageCount { get { return this.jobVertices.ExecutedStageCount; } }

        /// <summary>
        /// Total data read by job.
        /// </summary>
        public long TotalDataRead { get; protected set; }
        /// <summary>
        /// Data read intra-pod.
        /// </summary>
        public long IntraPodDataRead { get; protected set; }
        /// <summary>
        /// Data read cross pod.
        /// </summary>
        public long CrossPodDataRead { get; protected set; }
        /// <summary>
        /// Data read from the same machine.
        /// </summary>
        public long LocalReadData { get; protected set; }
        /// <summary>
        /// Approximate timing information, used for vertices which have not terminated yet.
        /// </summary>
        private DateTime lastTimestampSeen;

        /// <summary>
        /// The vertices in this job
        /// </summary>
        public IEnumerable<ExecutedVertexInstance> Vertices
        {
            get { return this.jobVertices.AllVertices(); }
        }
        /// <summary>
        /// The summary of this job
        /// </summary>
        public DryadLinqJobSummary Summary
        {
            get { return this.jobSummary; }
        }
        /// <summary>
        /// Message returned by job manager when job aborts.
        /// </summary>
        public string AbortingMsg { get; private set; }
        /// <summary>
        /// The cluster where the job information resides.
        /// </summary>
        public ClusterConfiguration ClusterConfiguration { get; protected set; }
        /// <summary>
        /// Original cluster configuration (the config can be just "cache").
        /// </summary>
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        private ClusterConfiguration OriginalClusterConfiguration { get; set; }

        /// <summary>
        /// Regular expression for parsing a stdout line with vertex statistics.
        /// </summary>
        private readonly static Regex vertexStartRegex, vertexCreatedRegex, processCreatingRegex, timingInfoRegex, terminationRegex, verticesCreatedRegex, ioRegex,
            terminatedRegex, vertexAbandonedRegex, failedRegex, cancelRegex, datareadRegex, inputFailureRegex, setToFailedlRegex, revokedRegex;

        /// <summary>
        /// Useful CPU time.
        /// </summary>
        public TimeSpan UsefulCPUTime { get; protected set; }
        /// <summary>
        /// Time spent in failed vertices.
        /// </summary>
        public TimeSpan WastedCPUTime { get; protected set; }
        /// <summary>
        /// Average degree of parallelism.
        /// </summary>
        public double AverageParallelism { 
            get 
            {
// ReSharper disable CompareOfFloatsByEqualityOperator
                if (this.RunningTime.TotalSeconds != 0)
// ReSharper restore CompareOfFloatsByEqualityOperator
                    return this.UsefulCPUTime.TotalSeconds / this.RunningTime.TotalSeconds;
                else
                    return 0; 
            } 
        }

        /// <summary>
        /// Number of executed vertices.
        /// </summary>
        public int ExecutedVertexCount { get { return this.jobVertices.Count; } }

        /// <summary>
        /// Compile a bunch of constant regular expressions.
        /// </summary>
        static DryadLinqJobInfo()
        {
            // optional guid regular expression
            const string optGuidRegex = @"GUID \{?([-a-fA-F0-9]+)\}";

            // Abandoning duplicate scheduling/execution of vertex 83.1 (InputTable__26[5])
            vertexAbandonedRegex = new Regex(@"Abandoning duplicate \w+ of vertex (\d+)\.(\d+) \((.+)\)", RegexOptions.Compiled);
            // Created process execution record for vertex 33 (Super__0[0]) v.0 GUID {B0FC788F-1FFC-4D74-AFC4-3EDFF03AF11A}
            vertexCreatedRegex = new Regex(@"Created process execution record for vertex (\d+) \((.*)\) v.(\d+) " + optGuidRegex, 
                RegexOptions.Compiled);
            // Creating process for vertex 2945 (Merge__17[440]) v.0 GUID {DDC9BB35-25D9-48A9-98C6-9EC7753FFB3B} machine sherwood-022
            processCreatingRegex = new Regex(@"Creating process for vertex (\d+) \((.*)\) v.(\d+) " + optGuidRegex + @" machine (\w+)", RegexOptions.Compiled);
            // Created process execution record for vertices 192 (Merge__41[0]) 223 (Union__45[0]) v.0 GUID {0297A91C-FFEA-42EA-94AF-CD0163A04D45}
            verticesCreatedRegex = new Regex(@"Created process execution record for vertices (.*) v.(\d+) " + optGuidRegex,
                                RegexOptions.Compiled);
            // Process started for vertex 5 (Super__0[1]) v.0 GUID {73EA55E0-0326-43C4-AD61-CB0B8CF8FE49} machine sherwood-025
            // Process started for vertices 23 (Merge__29) 24 (Apply__33) v.0 GUID {E945DC5D-9AF6-4732-8770-2A6BF7FA3041} machine sherwood-237
            vertexStartRegex = new Regex(@"Process started for vert(\w+) (.*) v\.(.*) " + optGuidRegex + @" machine (\S+)",
                RegexOptions.Compiled);
            // Timing Information 5 1 Super__0[1] 128654556602334453 0.0000 0.0000 0.0000 0.0000 0.2969
            timingInfoRegex = new Regex(@"Timing Information (\d+) (\d+) (.+) (\d+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+)",
                RegexOptions.Compiled);
            // Vertex 5.0 (Super__0[1]) machine sherwood-025 guid {73EA55E0-0326-43C4-AD61-CB0B8CF8FE49} status Vertex Has Completed, 
            terminationRegex = new Regex(@"Vertex (\d+)\.(\d+) \((.+)\) machine (\S+) guid \{?([-a-fA-F0-9]+)\}? status (.*)",
                RegexOptions.Compiled);
            // Process was terminated Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 status The operation succeeded
            terminatedRegex = new Regex(@"Process was terminated Vertex (\d+)\.(\d+) \((.+)\) " + optGuidRegex + @" machine (\S+) status (.*)",
                RegexOptions.Compiled);
            // Process has failed Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 Exitcode status The operation succeeded
            failedRegex = new Regex(@"Process has failed Vertex (\d+)\.(\d+) \((.+)\) " + optGuidRegex + @" machine (\S+) Exitcode (.*)",
                RegexOptions.Compiled);
            // Canceling vertex 1461.0 (Merge__13[258]) due to dependent failure
            cancelRegex = new Regex(@"Canceling vertex (\d+)\.(\d+) \((.+)\) due to (.*)", RegexOptions.Compiled);
            // Setting vertex 1461.0 (Merge__13[258]) to failed
            setToFailedlRegex = new Regex(@"Setting vertex (\d+)\.(\d+) \((.+)\) to failed(.*)", RegexOptions.Compiled);
            // total=951722563162 local=37817665237 intrapod=189765117248 crosspod=724139780677
            datareadRegex = new Regex(@"total=(\d+) local=(\d+) intrapod=(\d+) crosspod=(\d+)", RegexOptions.Compiled);
            // Input vertex %u (%s) had %u read failure%s\n
            inputFailureRegex = new Regex(@"Input vertex (\d+) \(.*\) had (\d+) read failure", RegexOptions.Compiled);
            // Io information 23 1 Super__4[5] read 7106 wrote 933
            ioRegex = new Regex(@"Io information (\d+) (\d+) (.+) read (\d+) wrote (\d+)");
            // Cancellations by Quincy
            revokedRegex = new Regex(@"Process was revoked by remote scheduler Old " + optGuidRegex + @" New " + optGuidRegex);
        }

        /// <summary>
        /// Create information about a job run on the cluster.
        /// </summary>
        /// <param name="cf">Cluster configuration.</param>
        /// <param name="summary">Summary description of the job.</param>
        /// <returns>The Dryad job description, or null.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <param name="fill">If true, fill all the information, otherwise the user will have to call FillInformation on the result later.</param>
        /// <param name="updateProgress">Delegate used to report progress.</param>
        public static DryadLinqJobInfo CreateDryadLinqJobInfo(ClusterConfiguration cf, DryadLinqJobSummary summary, bool fill, StatusReporter reporter, Action<int> updateProgress)
        {
            try
            {
                DryadLinqJobInfo job = new DryadLinqJobInfo(cf, summary);
                if (fill)
                    job.CollectEssentialInformation(reporter, updateProgress);
                return job;
            }
            catch (Exception e)
            {
                Trace.TraceInformation(e.ToString());
                reporter("Could not collect job information for " + summary.Name + ": " + e.Message, StatusKind.Error);
                return null;
            }
        }

        /// <summary>
        /// Read the information about a job which ran the JM on the cluster
        /// </summary>
        /// <param name="cf">Configuration of the cluster.</param>
        /// <param name="summary">Summary of the job.</param>
        protected DryadLinqJobInfo(ClusterConfiguration cf, DryadLinqJobSummary summary)
        {
            this.JobInfoCannotBeCollected = true;
            this.ClusterConfiguration = cf;
            if (cf is CacheClusterConfiguration)
                this.OriginalClusterConfiguration = (cf as CacheClusterConfiguration).ActualConfig(summary);
            else
                this.OriginalClusterConfiguration = cf;
            this.Initialize(summary);
        }

        /// <summary>
        /// Initialize a job info.
        /// </summary>
        /// <param name="summary">Job to summarize.</param>
        private void Initialize(DryadLinqJobSummary summary)
        {
            this.UsefulCPUTime = TimeSpan.Zero;
            this.WastedCPUTime = TimeSpan.Zero;
            this.LastUpdatetime = DateTime.Now;
            this.ManagerStdoutIncomplete = true; // until we've seen the end
            this.ManagerVertex = null;
            this.jobSummary = summary;
            this.ErrorCode = "";
            this.AbortingMsg = "";
            this.cachedStages = new Dictionary<string, DryadLinqJobStage>();
            this.jobVertices = new JobVertices();

            bool terminated = ClusterJobInformation.JobIsFinished(summary.Status);

            IClusterResidentObject managerstdoutfile = this.ClusterConfiguration.ProcessStdoutFile(summary.ManagerProcessGuid, terminated, summary.Machine, summary);
            if (this.ClusterConfiguration is CacheClusterConfiguration)
                this.stdoutpath = managerstdoutfile;
            else
            {
                IClusterResidentObject jmdir = this.ClusterConfiguration.ProcessDirectory(summary.ManagerProcessGuid, terminated, summary.Machine, summary);
                if (this.stdoutpath == null)
                {
                    string filename = managerstdoutfile.Name;

                    //this.stdoutpath = jmdir.GetFile("stdout.txt");
                    // do this by scanning the folder; this can give additional information about the file size on some platforms
                    IEnumerable<IClusterResidentObject> files = jmdir.GetFilesAndFolders(filename);
                    foreach (var f in files)
                    {
                        if (f.Exception != null)
                        {
                            throw f.Exception;
                        }
                        if (f.RepresentsAFolder)
                            continue;
                        // there should be exactly one match
                        this.stdoutpath = f;
                        break;
                    }

                    if (this.stdoutpath == null)
                    {
                        throw new CalypsoClusterException("Could not locate JM standard output file in folder " + jmdir);
                    }
                }
            }
        }

        /// <summary>
        /// Refresh the job status.
        /// </summary>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <param name="updateProgress">Used to report progress.</param>
        public void RefreshJobStatus(StatusReporter reporter, Action<int> updateProgress)
        {
            // skip if job is finished
            if (this.Summary.Status == ClusterJobInformation.ClusterJobStatus.Failed ||
                this.Summary.Status == ClusterJobInformation.ClusterJobStatus.Cancelled ||
                this.Summary.Status == ClusterJobInformation.ClusterJobStatus.Succeeded)
                return;

            ClusterStatus status = this.ClusterConfiguration.CreateClusterStatus();
            status.RefreshStatus(this.Summary, reporter, updateProgress);
        }

        /// <summary>
        /// Fill the job info by parsing the stdout.txt.
        /// <param name="statusReporter">Delegate used to report errors.</param>
        /// <returns>True if it succeeds, false otherwise.</returns>
        /// <param name="updateProgress">Delegate used to report progress.</param>
        /// </summary>
        public bool CollectEssentialInformation(StatusReporter statusReporter, Action<int> updateProgress)
        {
            this.RefreshJobStatus(statusReporter, updateProgress);
            if (this.ManagerVertex == null)
            {
                this.ManagerVertex = new ExecutedVertexInstance(this, -1, 0, "JobManager", "", this.Summary.Date);
                this.ManagerVertex.IsManager = true;
                this.ManagerVertex.SetStartInformation(this, this.Summary.Machine, this.Summary.Date, this.Summary.ManagerProcessGuid, "");
                this.ManagerVertex.StartCommandTime = this.ManagerVertex.CreationTime = this.ManagerVertex.VertexScheduleTime = this.Summary.Date;
                ExecutedVertexInstance.VertexState jmstate = ExecutedVertexInstance.VertexState.Started;
                switch (this.Summary.Status)
                {
                    case ClusterJobInformation.ClusterJobStatus.Failed:
                        jmstate = ExecutedVertexInstance.VertexState.Failed;
                        break;
                    /*
                    case ClusterJobInformation.ClusterJobStatus.Succeeded:
                        jmstate = ExecutedVertexInstance.VertexState.Successful;
                        break;
                    */
                }
                this.ManagerVertex.SetState(jmstate);
                this.jobVertices.Add(this.ManagerVertex);
            }

            if (this.stdoutpath == null)
                return false;
            bool success = this.ParseStdout(this.stdoutpath, statusReporter, updateProgress);
            updateProgress(100);
            if (!success)
                return false;

            this.JobInfoCannotBeCollected = false;
            statusReporter("Stdout parsed", StatusKind.OK);

            this.LastUpdatetime = DateTime.Now;
            if (this.Summary.Status == ClusterJobInformation.ClusterJobStatus.Running)
            {
                foreach (var vertex in this.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Started))
                    vertex.MarkVertexWasRunning(this.LastUpdatetime);
                this.ManagerVertex.MarkVertexWasRunning(this.LastUpdatetime);
            }
            else if (this.jobSummary.Status == ClusterJobInformation.ClusterJobStatus.Failed)
            {
                if (this.ManagerVertex.State == ExecutedVertexInstance.VertexState.Started)
                    this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Failed);
                foreach (var vertex in this.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Started))
                    vertex.MarkVertexWasRunning(this.ManagerVertex.End);
            }
            
            return true;
        }

        /// <summary>
        /// Given a list of vertex (number \(name\))* pairs return the numbers.
        /// </summary>
        /// <param name="vertexlist">A string of shape (number \(name\))*.</param>
        /// <returns>The list of name-number pairs.</returns>
        private static IEnumerable<Tuple<string, int>> ParseVertices(string vertexlist)
        {
            Regex numberre = new Regex(@"(\d+) (.*)");
            while (vertexlist.Length > 0)
            {
                Match m = numberre.Match(vertexlist);
                if (!m.Success)
                    throw new CalypsoDryadException("Could not find vertex number in " + vertexlist);
                string number = m.Groups[1].Value;

                // now scan a balanced number of parantheses
                string rest = m.Groups[2].Value;
                if (rest[0] != '(')
                    throw new CalypsoDryadException("Expecting open parens after vertex number");
                int opened = 0;
                int i;
                for (i = 0; i < rest.Length; i++)
                {
                    if (rest[i] == '(')
                        opened++;
                    else if (rest[i] == ')')
                    {
                        opened--;
                        if (opened == 0)
                        {
                            i++;
                            break;
                        }
                    }
                }
                if (opened != 0 || i <= 2)
                    throw new CalypsoDryadException("did not find matched parantheses in vertex name in " + vertexlist + ", can't parse");
                string name = rest.Substring(1, i - 2); // skip first and last paranthesis
                yield return new Tuple<string, int>(name, int.Parse(number));
                vertexlist = rest.Substring(i);
            }
        }

        /// <summary>
        /// In new versions of L2H some lines start with a timestamp.  Parse this timestamp.
        /// </summary>
        /// <param name="line">Line that may start with [timestamp].</param>
        /// <returns>The timestamp at the beginning of the line, or DateTime.MinValue if none.</returns>
        static DateTime ParseLineTimestamp(string line)
        {
            int square = line.IndexOf(']');
            DateTime result = DateTime.MinValue;

            if (line.StartsWith("[") && square >= 1)
            {
                string datetime = line.Substring(1, square-1);
                DateTime.TryParse(datetime, out result);
            }
            return result;
        }

        /// <summary>
        /// Try to read a numeric value from a dictionary at a specific key.
        /// </summary>
        /// <param name="dict">Dictionary containing key-value pairs.</param>
        /// <param name="key">Key we are interested in.</param>
        /// <returns>The numeric value with that key, or 0 if some error occurs.</returns>
        private long TryGetNumeric(Dictionary<string, string> dict, string key)
        {
            if (!dict.ContainsKey(key)) return 0;
            long result;
            if (long.TryParse(dict[key], out result))
                return result;
            return 0;
        }

        /// <summary>
        /// New JM stdout parsing code, for YARN-based DryadLINQ.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <returns>False if the line terminated in a quoted string and has to be combined with the next line.</returns>
        private bool ParseStdoutLineNew(string line)
        {
            if (string.IsNullOrWhiteSpace(line)) return true;

            Dictionary<string, string> kvp = Utilities.ParseCSVKVP(line);
            if (kvp == null) return false;

            var strTs = kvp["logtimelocal"];
            int cutOff = strTs.IndexOf("UTC");
            if (cutOff >= 0)
            {
                strTs = strTs.Substring(0, cutOff);
            }
            DateTime timeStamp = DateTime.Parse(strTs, CultureInfo.InvariantCulture);
            timeStamp = timeStamp.ToLocalTime();
            this.lastTimestampSeen = timeStamp;

            if (kvp.ContainsKey("job"))
            {
                string operation = kvp["job"];
                switch (operation)
                {
                    case "start":
                        this.ManagerVertex.SetStartInformation(this, this.Summary.Machine, timeStamp, this.Summary.ManagerProcessGuid, "");
                        this.ManagerVertex.StartCommandTime = this.ManagerVertex.CreationTime = this.ManagerVertex.VertexScheduleTime = timeStamp;
                        break;
                    case "stop":
                        this.ManagerVertex.End = timeStamp;
                        string exitcode;

                        if (kvp.TryGetValue("exitcode", out exitcode))
                        {
                            this.ErrorCode = exitcode;
                            int numCode = Convert.ToInt32(exitcode, 16);
                            if (numCode == 0)
                            {
                                this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Successful);
                            }
                            else
                            {
                                this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Failed);
                            }
                        }

                        string errorstring;
                        if (kvp.TryGetValue("errorstring", out errorstring))
                        {
                            this.ManagerVertex.AddErrorString(errorstring);
                            this.AbortingMsg = errorstring;
                        }

                        break;
                }
            }
            else if (kvp.ContainsKey("vertex"))
            {
                string vertex = kvp["vertex"];
                int number;
                int version;

                int dot = vertex.IndexOf('.');
                if (dot < 0)
                {
                    number = int.Parse(vertex);
                    version = int.Parse(kvp["version"]);
                }
                else
                {
                    number = int.Parse(vertex.Substring(0, dot));
                    version = int.Parse(vertex.Substring(dot + 1));
                }

                if (kvp.ContainsKey("transition"))
                {
                    string transition = kvp["transition"];
                    switch (transition)
                    {
                        case "created":
                        {
                            string name = kvp["name"];
                            ExecutedVertexInstance vi = new ExecutedVertexInstance(this, number, version, name, "", timeStamp);
                            this.jobVertices.Add(vi);
                        }
                        break;
                        case "starting":
                        {
                            // not doing anything
                            break;
                        }
                        case "running":
                        {
                            string process;
                            kvp.TryGetValue("id", out process); // "process" is also good
                            string machine = kvp["computer"];
                            ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                            this.jobVertices.Remap(vi, process);
                            string pid = this.ClusterConfiguration.ExtractPidFromGuid(process, this.Summary);
                            DryadProcessIdentifier identifier = new DryadProcessIdentifier(pid);
                            vi.SetStartInformation(this, machine, timeStamp, identifier, process);
                        }
                        break;
                        case "completed":
                        {
                            ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                            vi.SetState(ExecutedVertexInstance.VertexState.Successful);
                            vi.End = timeStamp;
                            vi.ExitCode = "";
                            break;
                        }
                        case "failed":
                        {
                            ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                            if (vi.State != ExecutedVertexInstance.VertexState.Started)
                                vi.SetState(ExecutedVertexInstance.VertexState.Cancelled);
                            else
                                vi.SetState(ExecutedVertexInstance.VertexState.Failed);
                            if (kvp.ContainsKey("errorstring"))
                                vi.AddErrorString(kvp["errorstring"]);
                            string exitcode;
                            if (kvp.TryGetValue("errorcode", out exitcode))
                                vi.ExitCode = exitcode;
                            vi.End = timeStamp;
                            break;
                        }
                    }
                }
                else if (kvp.ContainsKey("outputChannel"))
                {
                    string chan = kvp["outputChannel"];
                    int channelNo = int.Parse(chan);
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);

                    if (!kvp.ContainsKey("errorstatus"))
                    {
                    }
                    else
                    {
                        if (kvp.ContainsKey("errorstring"))
                            vi.AddErrorString(kvp["errorstring"]);
                    }
                }
                else if (kvp.ContainsKey("inputChannel"))
                {
                    string chan = kvp["inputChannel"];
                    int channelNo = int.Parse(chan);
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);

                    if (!kvp.ContainsKey("errorstatus"))
                    {
                    }
                    else
                    {
                        vi.AddErrorString(kvp["errorstring"]);
                    }
                }
                else if (kvp.ContainsKey("io"))
                {
                    if (kvp["io"] == "starting")
                    {
                        ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                        int numberOfInputs = (int) TryGetNumeric(kvp, "numberOfInputs");
                        int numberOfOutputs = (int)TryGetNumeric(kvp, "numberOfOutputs");

                        if (vi.InputChannels == null)
                            vi.InputChannels = new Dictionary<int, ChannelEndpointDescription>();
                        for (int i = 0; i < numberOfInputs; i++)
                        {
                            string uri;
                            if (kvp.TryGetValue("uriIn." + i, out uri))
                            {
                                var ched = new ChannelEndpointDescription(false, i, uri, 0);
                                vi.InputChannels[i] = ched;
                            }
                        }

                        if (vi.OutputChannels == null)
                            vi.OutputChannels = new Dictionary<int, ChannelEndpointDescription>();
                        for (int i = 0; i < numberOfOutputs; i++)
                        {
                            string uri;
                            if (kvp.TryGetValue("uriOut." + i, out uri))
                            {
                                var ched = new ChannelEndpointDescription(false, i, uri, 0);
                                vi.OutputChannels[i] = ched;
                            }
                        }
                    }
                    else if (kvp["io"] == "total")
                    {
                        ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);

                        long totalRead = TryGetNumeric(kvp, "totalRead");
                        long tempRead = TryGetNumeric(kvp, "tempRead");
                        long tempReadInRack = TryGetNumeric(kvp, "tempReadInRack");
                        long tempReadCrossRack = TryGetNumeric(kvp, "tempReadCrossRack");
                        long localRead = TryGetNumeric(kvp, "localRead");
                        long totalWritten = TryGetNumeric(kvp, "totalWritten");

                        vi.DataRead = totalRead;
                        vi.DataWritten = totalWritten;

                        this.TotalDataRead += totalRead;
                        this.LocalReadData += localRead;
                        this.CrossPodDataRead += tempReadCrossRack;
                        this.IntraPodDataRead += tempReadInRack;
                    }
                    else if (kvp["io"] == "running")
                    {
                        ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                        
                        foreach (int ch in vi.InputChannels.Keys)
                        {
                            long bytes = TryGetNumeric(kvp, "rb." + ch);
                            vi.InputChannels[ch].Size = bytes;
                        }

                        foreach (int ch in vi.OutputChannels.Keys)
                        {
                            long bytes = TryGetNumeric(kvp, "wb." + ch);
                            vi.OutputChannels[ch].Size = bytes;
                        }

                        long totalRead = TryGetNumeric(kvp, "totalRead");
                        long totalWritten = TryGetNumeric(kvp, "totalWritten");

                        vi.DataRead = totalRead;
                        vi.DataWritten = totalWritten;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Parse one line from the JM standard output.
        /// </summary>
        /// <param name="line">The line to parse.</param>
        private void ParseStdoutLine(string line)
        {
            DateTime lineTimeStamp = DateTime.MinValue;

            if (line.Contains("Created process execution record"))
            {
                Match m = vertexCreatedRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    // Created process execution record for vertex (\d+) \((.*)\) v.(\d+) GUID \{?([-A-F0-9]+)\}?
                    int number = Int32.Parse(m.Groups[1].Value);
                    string name = m.Groups[2].Value;
                    int version = Int32.Parse(m.Groups[3].Value);
                    string guid = m.Groups[4].Value; // on some platforms, e.g. HPC, this identifier is not yet assigned properly

                    // the vertex may be already there, sometimes numbers are reused...
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi == null)
                    {
                        vi = new ExecutedVertexInstance(this, number, version, name, guid, lineTimeStamp);
                        this.jobVertices.Add(vi);
                    }
                }
                else
                {
                    m = verticesCreatedRegex.Match(line);
                    if (m.Success)
                    {
                        lineTimeStamp = ParseLineTimestamp(line);

                        // Created process execution record for vertices (.*) v.(\d+) GUID \{?([-A-F0-9]+)\}?
                        // Created process execution record for vertices 192 (Merge__41[0]) 223 (Union__45[0]) v.0 GUID {0297A91C-FFEA-42EA-94AF-CD0163A04D45}
                        int version = Int32.Parse(m.Groups[2].Value);
                        string vertices = m.Groups[1].Value;
                        string guid = m.Groups[3].Value; // on some platforms, e.g. HPC, this identifier is not yet assigned properly

                        IEnumerable<Tuple<string, int>> vertexList = DryadLinqJobInfo.ParseVertices(vertices);
                        foreach (var p in vertexList)
                        {
                            int number = p.Item2;
                            ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                            if (vi == null)
                            {
                                vi = new ExecutedVertexInstance(this, number, version, p.Item1, guid, lineTimeStamp);
                                this.jobVertices.Add(vi);
                            }
                        }
                    }
                }
            }
            else if (line.StartsWith("Creating process"))
            {
                Match m = processCreatingRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    // Creating process for vertex (\d+) \((.*)\\) v.(\d+) GUID \{?([-A-F0-9]+)\}? machine (\w+)
                    int number = Int32.Parse(m.Groups[1].Value);
                    //string name = m.Groups[2].Value;
                    int version = Int32.Parse(m.Groups[3].Value);
                    string guid = m.Groups[4].Value; 

                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi != null)
                    {
                        this.jobVertices.Remap(vi, guid);
                    }
                }
            }
            else if (line.StartsWith("Process was revoked"))
            {
                Match m = revokedRegex.Match(line);
                if (m.Success)
                {
                    string oldGuid = m.Groups[1].Value;
                    ExecutedVertexInstance vi = this.jobVertices.FindVertexByGuid(oldGuid);
                    if (vi != null)
                    {
                        vi.SetState(ExecutedVertexInstance.VertexState.Revoked);
                        string newGuid = m.Groups[2].Value;
                        this.jobVertices.Remap(vi, newGuid);
                    }
                    else
                    {
                        Trace.TraceInformation("Could not find revoked vertex with guid " + oldGuid);
                    }
                }
            }
            else if (line.StartsWith("---HiPriTime"))
            {
                // Scope-specific line which we use to get the i/o information
                // ---HiPriTime D7D51A1F-6693-4378-95FD-FC778A67C632,F52CA694-0202-411E-85E9-0C883E770A0E,SV4_Extract_Split[0],Completed,ch1sch010331112,2011-05-03 15:26:01.681 PDT,2011-05-03 15:26:01.696 PDT,2011-05-03 15:26:02.118 PDT,2011-05-03 15:26:04.286 PDT,2011-05-03 15:26:07.656 PDT,2011-05-03 15:26:01.696 PDT,97390825,1498630
                string info = line.Substring(13);
                string[] parts = info.Split(',');
                if (parts.Length >= 13)
                {
                    long read = long.Parse(parts[11]);
                    long written = long.Parse(parts[12]);
                    string guid = parts[1];

                    ExecutedVertexInstance vi = this.jobVertices.FindVertexByGuid(guid);
                    if (vi != null)
                    {
                        vi.DataRead = read;
                        vi.DataWritten = written;
                        this.TotalDataRead += read;
                    }
                }
            }
            else if (line.Contains("Io information"))
            {
                // HPC-specific line
                Match m = ioRegex.Match(line);
                if (m.Success)
                {
                    int number = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi != null)
                    {
                        vi.DataRead = long.Parse(m.Groups[4].Value);
                        vi.DataWritten = long.Parse(m.Groups[5].Value);
                        this.TotalDataRead += vi.DataRead;
                    }
                }
            }
            else if (line.Contains("Process started"))
            {
                //those vertices which are being canceled may not be here
                Match m = vertexStartRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    string version = m.Groups[3].Value;
                    string guid = m.Groups[4].Value;
                    string pid = this.ClusterConfiguration.ExtractPidFromGuid(guid, this.Summary);
                    DryadProcessIdentifier identifier = new DryadProcessIdentifier(pid);
                    string machine = m.Groups[5].Value;

                    // Process started for vertex 4 (Super__0[0]) v.0 GUID {9DDD0B00-C93F-46D2-9073-1CFD27829300} machine sherwood-255
                    // Process started for vertices 23 (Merge__29) 24 (Apply__33) v.0 GUID {E945DC5D-9AF6-4732-8770-2A6BF7FA3041} machine sherwood-237

                    string vertices = m.Groups[2].Value;
                    // This is a list of (number \(name\))* pairs
                    // we will assume that the parantheses are matched, or we can't do much

                    bool onevertex;
                    if (m.Groups[1].Value == "ex")  // one vertEX
                        onevertex = true;
                    else if (m.Groups[1].Value == "ices")
                        onevertex = false;
                    else
                        throw new CalypsoDryadException("Can't figure out if one or many vertices");

                    IEnumerable<Tuple<string, int>> vertexList = DryadLinqJobInfo.ParseVertices(vertices);

                    int vertexcount = 0;
                    int iversion = int.Parse(version);

                    if (lineTimeStamp > this.lastTimestampSeen)
                        this.lastTimestampSeen = lineTimeStamp;
                    foreach (var p in vertexList)
                    {
                        int number = p.Item2;
                        ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, iversion);
                        //new ExecutedVertexInstance(this, number, version, name, identifier, machine, this.lastTimestampSeen);
                        if (vi == null)
                            Trace.TraceInformation("Could not find information for vertex {0}.{1}", number, version);
                        else
                            vi.SetStartInformation(this, machine, this.lastTimestampSeen, identifier, guid);
                        vertexcount++;
                    }

                    if (vertexcount > 1 && onevertex)
                        throw new CalypsoDryadException("Expected one vertex, found " + vertexcount);
                }
                else
                {
                    Trace.TraceInformation("Unexpected parsing error on line {0}", line);
                }
            }
            else if (line.Contains("Abandoning"))
            {
                Match m = vertexAbandonedRegex.Match(line);
                if (m.Success)
                {
                    int number = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi != null)
                        vi.SetState(ExecutedVertexInstance.VertexState.Abandoned);
                }
            }
            else if (line.Contains("Setting"))
            {
                Match m = setToFailedlRegex.Match(line);
                if (m.Success)
                {
                    // Setting vertex 1461.0 (Merge__13[258]) to failed
                    // Setting vertex (\d+)\.(\d+) \((.+)\) to failed(.*)
                    int number = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);

                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi != null)
                    {
                        vi.SetState(ExecutedVertexInstance.VertexState.Failed);
                        //vi.ErrorString = m.Groups[4].Value;
                    }
                }
            }
            else if (line.Contains("Process was terminated"))
            {
                // terminatedRegex = new Regex(@"Process was terminated Vertex (\d+)\.(\d+) \((.+)\) GUID \{?([-A-F0-9]+)\}? machine (\S+) status (.*)",
                // Process was terminated Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 status The operation succeeded
                Match m = terminatedRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    int number = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);

                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(number, version);
                    if (vi != null)
                    {
                        // sometimes successful processes are terminated, because they don't report quickly enough being done
                        if (vi.State != ExecutedVertexInstance.VertexState.Successful)
                        {
                            vi.SetState(ExecutedVertexInstance.VertexState.Cancelled);
                        }
                        vi.ErrorString = m.Groups[6].Value;
                        if (lineTimeStamp != DateTime.MinValue)
                            vi.End = lineTimeStamp;
                    }
                }
            }
            else if (line.Contains("Timing Information Graph Start Time"))
            {
                // Cosmos-specific line
                // Timing Information Graph Start Time 128654556581866096
                Match m = Regex.Match(line, @"Timing Information Graph Start Time (\d+)");
                DateTime createTime = Utilities.Convert64time(ClusterConfiguration.GetClusterTimeZone(this.Summary), m.Groups[1].Value);
                this.ManagerVertex.SetStartInformation(this, this.Summary.Machine, createTime, this.Summary.ManagerProcessGuid, "");
                this.ManagerVertex.StartCommandTime = this.ManagerVertex.CreationTime = this.ManagerVertex.VertexScheduleTime = createTime;
                this.lastTimestampSeen = createTime;
            }
            else if (line.StartsWith("Start time: "))
            {
                // HPC L2H specific line
                // Start time: 04/05/2011 17:25:42.223
                DateTime createTime;
                bool parse = DateTime.TryParse(line.Substring("Start time: ".Length), out createTime);

                if (parse)
                {
                    this.ManagerVertex.SetStartInformation(this, this.Summary.Machine, createTime, this.Summary.ManagerProcessGuid, "");
                    this.ManagerVertex.StartCommandTime = this.ManagerVertex.CreationTime = this.ManagerVertex.VertexScheduleTime = createTime;
                    this.lastTimestampSeen = createTime;
                }
            }
            else if (line.Contains("JM Finish time:"))
            {
                // Cosmos-specific line
                // JM Finish time: 129140295499437263 2010-03-25T22:25:49.943726Z
                Match m = Regex.Match(line, @"JM Finish time: (\d+)");
                DateTime time = Utilities.Convert64time(ClusterConfiguration.GetClusterTimeZone(this.Summary), m.Groups[1].Value);
                this.lastTimestampSeen = time;
                this.ManagerVertex.End = time;
            }
            else if (line.StartsWith("Stop time "))
            {
                // HPC L2H specific line
                // Stop time (Exit code = 2148734208): 04/05/2011 17:25:46.614
                Regex regex = new Regex(@"Stop time \(Exit code = (.*)\): (.*)");
                Match m = regex.Match(line);
                if (m.Success)
                {
                    this.ManagerStdoutIncomplete = false;

                    DateTime time;
                    bool parse = DateTime.TryParse(m.Groups[2].Value, out time);
                    if (parse)
                    {
                        this.lastTimestampSeen = time;
                        this.ManagerVertex.End = time;
                    }

                    this.ErrorCode = m.Groups[1].Value;
                    if (this.ErrorCode == "0")
                    {
                        this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Successful);
                    }
                    else
                    {
                        this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Failed);
                    }
                }
            }
            else if (line.Contains("Timing Information"))
            {
                // Timing Information 4 1 Super__0[0] 128654556603428182 0.0000 0.0000 0.0000 0.0000 0.2500 
                Match m = timingInfoRegex.Match(line);
                if (m.Success)
                {
                    int vertex = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    DateTime createtime = Utilities.Convert64time(ClusterConfiguration.GetClusterTimeZone(this.Summary), m.Groups[4].Value);
                    ExecutedVertexInstance vi = jobVertices.FindVertex(vertex, version);
                    if (vi == null)
                        return; // we do not keep track of vertices with duplicate scheduling, so these won't show up here

                    if (vi.State == ExecutedVertexInstance.VertexState.Started)
                    {
                        Console.WriteLine("Timing information while vertex is still running " + vi);
                        //throw new CalypsoClusterException("Timing information for vertex still running: " + vi);
                    }
                    DateTime last = vi.SetTiming(createtime, m.Groups[5].Value, m.Groups[6].Value, m.Groups[7].Value, m.Groups[8].Value, m.Groups[9].Value);
                    if (last > this.lastTimestampSeen)
                        this.lastTimestampSeen = last;
                    this.ManagerVertex.MarkVertexWasRunning(last);

                    try
                    {
                        if (vi.State == ExecutedVertexInstance.VertexState.Successful)
                            this.UsefulCPUTime += vi.RunningTime;
                        else if (vi.RunningTime > TimeSpan.Zero)
                            this.WastedCPUTime += vi.RunningTime;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Time value exception: " + ex.Message);
                    }
                }
                else
                    throw new CalypsoDryadException("Unmatched timing information line " + line);
            }
            else if (line.Contains("Process has failed"))
            {
                // Process has failed Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 Exitcode 0 status The operation succeeded
                // failedRegex = new Regex(@"Process has failed Vertex (\d+)\.(\d+) \((.+)\) GUID \{?([-A-F0-9]+)\}? machine (\S+) Exitcode (.*)",
                Match m = failedRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    int vertex = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    string exitcode = m.Groups[6].Value;
                    //string status = m.Groups[7].Value;
                    ExecutedVertexInstance vi = jobVertices.FindVertex(vertex, version);
                    if (vi != null)
                    {
                        vi.SetState(ExecutedVertexInstance.VertexState.Failed);
                        vi.ExitCode = exitcode;
                        if (lineTimeStamp != DateTime.MinValue)
                            vi.End = lineTimeStamp;
                        //vi.ErrorString = status;
                    }
                }
            }
            else if (line.Contains("ABORTING:"))
            {
                this.AbortingMsg = line.Substring(10);
                this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Failed);
            }
            else if (line.Contains("Accurate read data"))
            {
                Match m = datareadRegex.Match(line);
                if (m.Success)
                {
                    this.TotalDataRead = long.Parse(m.Groups[1].Value);
                    this.LocalReadData = long.Parse(m.Groups[2].Value);
                    this.IntraPodDataRead = long.Parse(m.Groups[3].Value);
                    this.CrossPodDataRead = long.Parse(m.Groups[4].Value);
                }
            }
            else if (line.Contains("<ErrorString>"))
            {
                //some errors contains "Error returned from managed runtime invocation"
                //which shows the error is from application code
                Match m = Regex.Match(line, @"\<ErrorString\>(.*)\</ErrorString\>");
                if (m.Success && lastFailedVertex != null)
                {
                    lastFailedVertex.AddErrorString(System.Web.HttpUtility.HtmlDecode(m.Groups[1].Value));
                }
            }
            else if (line.Contains("Canceling"))
            {
                // Canceling vertex 1461.0 (Merge__13[258]) due to dependent failure
                Match m = cancelRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    int vertex = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    string name = m.Groups[3].Value;

                    ExecutedVertexInstance vi = jobVertices.FindVertex(vertex, version);
                    if (vi != null)
                    {
                        if (vi.State == ExecutedVertexInstance.VertexState.Successful)
                            vi.SetState(ExecutedVertexInstance.VertexState.Invalidated);
                        else 
                            vi.SetState(ExecutedVertexInstance.VertexState.Cancelled);
                        if (lineTimeStamp != DateTime.MinValue)
                            vi.End = lineTimeStamp;
                    }
                    else
                    {
                        // TODO: this should not be needed, but this is a workaround for a bug in the HPC L2H software
                        vi = new ExecutedVertexInstance(this, vertex, version, name, "", lineTimeStamp);
                        vi.SetState(ExecutedVertexInstance.VertexState.Cancelled);
                        this.jobVertices.Add(vi);
                    }
                    // Process wasn't even started, so there is nothing to cancel
                }
            }
            else if (line.Contains("Application"))
            {
                //the job ends successfully
                Regex endSuccessRegex = new Regex(@"Application completed successfully.");
                //the job failed
                Regex endFailRegex = new Regex(@"Application failed with error code (.*)");

                Match m1 = endFailRegex.Match(line);

                if (m1.Success)
                {
                    this.ErrorCode = m1.Groups[1].Value;
                    this.ManagerStdoutIncomplete = false;
                    this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Failed);
                }
                else
                {
                    Match m2 = endSuccessRegex.Match(line);
                    if (m2.Success)
                    {
                        this.ManagerVertex.SetState(ExecutedVertexInstance.VertexState.Successful);
                        this.ManagerStdoutIncomplete = false;
                    }
                }
            }
            else if (line.StartsWith("Input"))
            {
                // Input vertex %u (%s) had %u read failure%s\n
                Match m = inputFailureRegex.Match(line);
                if (m.Success)
                {
                    this.AbortingMsg = line;
                }
            }
            else if (line.Contains("Vertex"))
            {
                // terminationRegex = new Regex(@"Vertex (\d+)\.(\d+) \((.+)\) machine (\S+) guid \{?([-0-9A-F]+)\}? status (.*)"
                Match m = terminationRegex.Match(line);
                if (m.Success)
                {
                    lineTimeStamp = ParseLineTimestamp(line);

                    int vertex = Int32.Parse(m.Groups[1].Value);
                    int version = Int32.Parse(m.Groups[2].Value);
                    ExecutedVertexInstance vi = this.jobVertices.FindVertex(vertex, version);
                    if (vi == null)
                    {
                        Trace.TraceInformation("Could not find vertex {0}.{1} line {2}", vertex, version, line);
                    }
                    else
                    {
                        bool failed = vi.SetTermination(m.Groups[6].Value, lineTimeStamp);
                        if (failed)
                            this.lastFailedVertex = vi;
                    }
                }
            }

            if (lineTimeStamp != DateTime.MinValue)
                this.lastTimestampSeen = lineTimeStamp;
        }

        private Dictionary<string, DryadLinqJobStage> cachedStages;
        
        /// <summary>
        /// Get information about a particular stage.
        /// </summary>
        /// <param name="stagename">Name of stage sought.</param>
        /// <returns>A description of the stage in question, or null if there are no vertices in that stage.</returns>
        public DryadLinqJobStage GetStage(string stagename)
        {
            if (this.cachedStages.ContainsKey(stagename))
                return this.cachedStages[stagename];
            List<ExecutedVertexInstance> stageVertices = this.jobVertices.GetStageVertices(stagename);
            if (stageVertices == null)
                stageVertices = new List<ExecutedVertexInstance>();
            DryadLinqJobStage retval = new DryadLinqJobStage(stagename, stageVertices);
            this.cachedStages.Add(stagename, retval);
            return retval;
        }

        private ISharedStreamReader cachedStdoutReader = null;

        /// <summary>
        /// Remember how many lines were parsed, and skip them on a second invocation.
        /// </summary>
        private int stdoutLinesParsed;
        /// <summary>
        /// Parse the stdout.txt file from the job manager.
        /// </summary>
        /// <param name="file">File to parse.</param>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        /// <param name="updateProgress">Delegate used to report progress.</param>
        /// <returns>True if the parsing succeeds.</returns>
        private bool ParseStdout(IClusterResidentObject file, StatusReporter statusReporter, Action<int> updateProgress)
        {
            int currentLine = 0;
            if (this.stdoutLinesParsed == 0)
                // don't lose it if we are only parsing the tail.
                this.lastTimestampSeen = this.Summary.Date; // start from the job submission timestamp

            // we are reusing the stream
            this.stdoutLinesParsed = 0;

            try
            {
                long filesize = file.Size;
                long readbytes = 0;
                string message = "Scanning JM stdout " + file;
                if (filesize >= 0)
                    message += string.Format("({0:N0} bytes)", filesize);
                statusReporter(message, StatusKind.LongOp);

                if (this.cachedStdoutReader == null)
                    this.cachedStdoutReader = file.GetStream();
                if (this.cachedStdoutReader.Exception != null)
                {
                    statusReporter("Exception while opening stdout " + this.cachedStdoutReader.Exception.Message, StatusKind.Error);
                    return false;
                }
                while (!this.cachedStdoutReader.EndOfStream)
                {
                    string line = this.cachedStdoutReader.ReadLine();
                    readbytes += line.Length;
                    if (currentLine >= this.stdoutLinesParsed)
                    {
                        while (true)
                        {
                            int startLine = currentLine;
                            bool completeLine = true;
                            try
                            {
                                completeLine = this.ParseStdoutLineNew(line);
                            }
                            catch (Exception ex)
                            {
                                statusReporter(string.Format("Line {0}: Exception {1}", currentLine, ex.Message), StatusKind.Error);
                                Console.WriteLine("Line {0}: Exception {1}", currentLine, ex);
                            }
                            if (!completeLine)
                            {
                                if (this.cachedStdoutReader.EndOfStream)
                                {
                                    throw new Exception("File ended while scanning for closing quote started on line " + startLine);
                                }

                                string extraline = this.cachedStdoutReader.ReadLine();
                                line += "\n" + extraline;
                                currentLine++;
                            }
                            else break;
                        }
                    }
                    currentLine++;
                    if (currentLine % 100 == 0 && filesize > 0)
                    {
                        updateProgress(Math.Min(100, (int)(100 * readbytes / filesize)));
                    }
                }

                this.stdoutLinesParsed = currentLine;

                if (this.ManagerVertex != null)
                {
                    if (this.ManagerVertex.End == DateTime.MinValue)
                        // approximation
                        this.ManagerVertex.End = this.lastTimestampSeen;

                    // we are done with this stream
                    if (this.ManagerVertex.State == ExecutedVertexInstance.VertexState.Failed ||
                        this.ManagerVertex.State == ExecutedVertexInstance.VertexState.Successful)
                        this.cachedStdoutReader.Close();
                }
                return true;
            }
            catch (Exception e)
            {
                statusReporter("Exception while reading stdout " + e.Message, StatusKind.Error);
                Trace.TraceInformation(e.ToString());
                return false;
            }
        }

        /// <summary>
        /// How many log files were successfuly parsed.
        /// </summary>
        private int logFilesParsed;
        /// <summary>
        /// Parse the logs generated by the Job Manager and learn more information from them.
        /// This function should be called after parsing the stdout.
        /// This function is extremely slow; it may be invoked on a background thread.
        /// <param name="statusReporter">Delegate used to report errors.</param>
        /// <returns>True on success.</returns>
        /// <param name="updateProgress">Delegate used to report progress.</param>
        /// </summary>
        public bool ParseJMLogs(StatusReporter statusReporter, Action<int> updateProgress)
        {
            IClusterResidentObject dir = this.ClusterConfiguration.ProcessLogDirectory(this.Summary.ManagerProcessGuid, this.ManagerVertex.VertexIsCompleted, this.Summary.Machine, this.Summary);
            if (dir.Exception != null)
            {
                statusReporter("Exception finding logs in " + dir, StatusKind.Error);
                return false;
            }

            string pattern = this.ClusterConfiguration.JMLogFilesPattern(false, this.Summary);
            List<IClusterResidentObject> logfiles = dir.GetFilesAndFolders(pattern).ToList();
            long totalWork = 0;
            foreach (var logfile in logfiles)
            {
                if (logfile.Size >= 0 && totalWork > 0)
                    totalWork += logfile.Size;
                else
                    totalWork = -1;
            }

            bool success = true;
            statusReporter(string.Format("Parsing {0} log files", logfiles.Count - this.logFilesParsed), StatusKind.OK);

            int currentFile = 0;
            bool invalidateCache = false;
            foreach (var logfile in logfiles)
            {
                if (currentFile >= this.logFilesParsed)
                {
                    invalidateCache = true;
                    success = this.ParseJMLogFile(logfile, statusReporter);
                }
                if (!success)
                    // stop at first failure
                    break;
                currentFile++;
                updateProgress(100 * currentFile  / logfiles.Count);
            }

            updateProgress(100);
            if (invalidateCache)
                this.InvalidateCaches();
            // reparse the last one again
            this.logFilesParsed = currentFile - 1;
            return success;
        }

        /// <summary>
        /// Parse a log file of the job manager and extract useful information.
        /// </summary>
        /// <param name="logfile">Log file to parse.</param>
        /// <param name="statusReporter">Delegate used to parse errors.</param>
        /// <returns>True if parsing succeeds.</returns>
        internal bool ParseJMLogFile(IClusterResidentObject logfile, StatusReporter statusReporter)
        {
            bool success = true;

            ISharedStreamReader sr = logfile.GetStream();
            if (sr.Exception != null)
            {
                statusReporter("Exception while opening file " + logfile + ":" + sr.Exception.Message, StatusKind.Error);
                return false;
            }
            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                if (!line.Contains("DryadProfiler")) continue;

                CosmosLogEntry le = new CosmosLogEntry(line);
                if (le.Subsystem != "DryadProfiler") continue;
                if (!le.Message.EndsWith("channel status")) continue;

                Dictionary<string, string> kvp = Utilities.ParseCommaSeparatedKeyValuePair(le.ExtraInfo);
                string verver = kvp["Vertex"];
                string[] numbers = verver.Split('.');
                int vertex = int.Parse(numbers[0]);
                int version = int.Parse(numbers[1]);
                ExecutedVertexInstance vi = this.jobVertices.FindVertex(vertex, version);
                if (vi == null)
                {
                    // We have overshot the information about the vertices parsed from stdout; stop parsing here
                    success = false;
                    break;
                }

                if (le.Message == "Input channel status")
                {
                    // Vertex=69.0, Name=Merge__446[3], MachPod=sherwood-005:pod1, TotalRead=1470802, TotalReadFromMach=1470802, TotalReadCrossMach=1470802, TotalReadCrossPod=0
                    long info = long.Parse(kvp["TotalRead"]);
                    vi.DataRead = info;
                }
                else if (le.Message == "Output channel status")
                {
                    // Vertex=69.0, Name=Merge__446[3], MachPod=sherwood-005:pod1, TotalWrite=1213418
                    long info = long.Parse(kvp["TotalWrite"]);
                    vi.DataWritten = info;
                }
            }
            sr.Close();

            return success;
        }

        /// <summary>
        /// The list of all stage names in the job.
        /// </summary>
        /// <returns>An iterator over the stage names.</returns>
        public IEnumerable<string> GetStageNames()
        {
            return this.jobVertices.GetAllStageNames();
        }

        /// <summary>
        /// The list of all stages in the job.
        /// </summary>
        /// <returns>An iterator over all stages.</returns>
        public IEnumerable<DryadLinqJobStage> AllStages()
        {
            return this.GetStageNames().Select(this.GetStage);
        }

        /// <summary>
        /// Generate a layout suitable for drawing the plan.
        /// </summary>
        /// <returns>A graph layout.</returns>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        // ReSharper disable once UnusedParameter.Global
        public GraphLayout ComputePlanLayout(StatusReporter statusReporter) 
        {
            IEnumerable<DryadLinqJobStage> stages = this.AllStages().OrderBy(s => s.StartTime).ToList();
            if (!stages.Any())
                // no layout to compute
                return null;

            DateTime jobStartTime = this.StartJMTime;
            DateTime lastTime = stages.Max(s => s.EndTime);
            if (lastTime == jobStartTime)
                // avoid the degenerate case
                lastTime = jobStartTime + new TimeSpan(0, 0, 1);

            GraphLayout result = new GraphLayout((lastTime - jobStartTime).TotalSeconds, stages.Count()*2);

            int currentStage = 0;
            foreach (DryadLinqJobStage s in stages)
            {
                // node represents the schedule: horizontal position is starttime - endtime
                DateTime endTime = s.EndTime;
                DateTime startTime = s.StartTime;
                if (endTime <= jobStartTime) // unknown time?
                    endTime = lastTime;      // assume still running
                if (startTime <= jobStartTime)
                    startTime = jobStartTime;
                GraphLayout.GraphNode node = new GraphLayout.GraphNode(
                    (startTime - jobStartTime).TotalSeconds, currentStage*2, (endTime - startTime).TotalSeconds, 1);
                node.Shape = GraphLayout.GraphNode.NodeShape.Box;
                node.Label = s.Name;
                node.Stage = s.Name;

                result.Add(node);
                currentStage++;
            }

            return result;
        }

        /// <summary>
        /// Find a vertex having specified the process id.
        /// </summary>
        /// <param name="id">Process id.</param>
        /// <returns>The vertex with the specified guid.</returns>
        public ExecutedVertexInstance FindVertex(DryadProcessIdentifier id)
        {
            return this.jobVertices.FindVertexByGuid(id.ToString());
        }

        /// <summary>
        /// Invalidate the cached information.
        /// </summary>
        public void InvalidateCaches()
        {
            this.cachedStages.Clear();
        }
    }

    /// <summary>
    /// Summary information about a job plan.
    /// </summary>
    public abstract class DryadJobStaticPlan
    {
        /// <summary>
        /// Connection between two stages.
        /// </summary>
        public class Connection
        {
            /// <summary>
            /// Arity of connection.
            /// </summary>
            public enum ConnectionType
            {
                /// <summary>
                /// Point-to-point connection between two stages.
                /// </summary>
                PointToPoint,
                /// <summary>
                /// Cross-product connection between two stages.
                /// </summary>
                AllToAll
            };

            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public enum ChannelType
            {
                /// <summary>
                /// Persistent file.
                /// </summary>
                DiskFile,
                /// <summary>
                /// In-memory fifo.
                /// </summary>
                Fifo,
                /// <summary>
                /// TCP pipe.
                /// </summary>
                TCP
            }

            /// <summary>
            /// Stage originating the connection.
            /// </summary>
            public Stage From { internal set; get; }
            /// <summary>
            /// Stage terminating the connection.
            /// </summary>
            public Stage To { internal set; get; }
            /// <summary>
            /// Type of connection.
            /// </summary>
            public ConnectionType Arity { get; internal set; }
            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public ChannelType ChannelKind { get; internal set; }
            /// <summary>
            /// Dynamic manager associated with the connection.
            /// </summary>
            public string ConnectionManager { get; internal set; }

            /// <summary>
            /// Color used to represent the connection.
            /// </summary>
            /// <returns>A string describing the color.</returns>
            public string Color()
            {
                switch (this.ChannelKind)
                {
                    case ChannelType.DiskFile:
                        return "black";
                    case ChannelType.Fifo:
                        return "red";
                    case ChannelType.TCP:
                        return "yellow";
                    default:
                        throw new Exception("Unknown channel kind " + this.ChannelKind);
                }
            }
        }

        /// <summary>
        /// Per-node connection information (should be per-edge...)
        /// </summary>
        protected struct ConnectionInformation
        {
            /// <summary>
            /// Type of connection.
            /// </summary>
            public Connection.ConnectionType Arity { get; internal set; }
            /// <summary>
            /// Type of channel backing the connection.
            /// </summary>
            public Connection.ChannelType ChannelKind { get; internal set; }
            /// <summary>
            /// Dynamic manager associated with the connection.
            /// </summary>
            public string ConnectionManager { get; internal set; }
        }

        /// <summary>
        /// Information about a stage.
        /// </summary>
        public class Stage
        {
            /// <summary>
            /// Stage name.
            /// </summary>
            public string Name { get; internal set; }
            /// <summary>
            /// Code executed in the stage.
            /// </summary>
            public string [] Code { get; internal set; }
            /// <summary>
            /// DryadLINQ operator implemented by the stage.
            /// </summary>
            public string Operator { get; internal set; }
            /// <summary>
            /// Number of vertices in stage.
            /// </summary>
            public int Replication { get; internal set; }
            /// <summary>
            /// Unique identifier.
            /// </summary>
            public int Id { get; set; }

            /// <summary>
            /// True if the stage is an input.
            /// </summary>
            public bool IsInput { get; internal set; }
            /// <summary>
            /// True if the stage is an output.
            /// </summary>
            public bool IsOutput { get; internal set; }
            /// <summary>
            /// True if the stage is a tee.
            /// </summary>
            public bool IsTee { get; internal set; }
            /// <summary>
            /// True if the stage is a concatenation.
            /// </summary>
            public bool IsConcat { get; internal set; }
            /// <summary>
            /// True if the stage is virtual (no real vertices synthesized).
            /// </summary>
            public bool IsVirtual { get { return this.IsInput || this.IsOutput || this.IsTee || this.IsConcat; } }
            /// <summary>
            /// Only defined for tables.
            /// </summary>
            public string Uri { get; internal set; }
            /// <summary>
            /// Only defined for tables.
            /// </summary>
            public string UriType { get; internal set; }
        }

        /// <summary>
        /// Map from stage id to stage.
        /// </summary>
        protected readonly Dictionary<int, Stage> stages;
        /// <summary>
        /// List of inter-stage connections in the plan.
        /// </summary>
        protected readonly List<Connection> connections;
        /// <summary>
        /// Store here per-node connection information (map from node id).
        /// </summary>
        protected readonly Dictionary<int, ConnectionInformation> perNodeConnectionInfo;

        /// <summary>
        /// Stream containing the plan.
        /// </summary>
        protected readonly ISharedStreamReader planStream;

        /// <summary>
        /// Create a dryadlinq job plan starting from an xml plan file.
        /// </summary>
        /// <param name="plan">Stream containing the plan.</param>
        protected DryadJobStaticPlan(ISharedStreamReader plan)
        {
            if (plan.Exception != null)
                // don't do this
                throw plan.Exception;
            this.planStream = plan;
            this.stages = new Dictionary<int, Stage>();
            this.connections = new List<Connection>();
            this.perNodeConnectionInfo = new Dictionary<int, ConnectionInformation>();
            this.fictitiousStages = 0;
        }

        /// <summary>
        /// Parse the query plan: cluster-specific.
        /// </summary>
        protected abstract void ParseQueryPlan();
       
        int fictitiousStages;

        /// <summary>
        /// Create a fictitious node for the job manager.
        /// </summary>
        public void AddFictitiousStages()
        {
            this.fictitiousStages = 2;

            Stage stage = new Stage();
            stage.Id = -1;
            stage.Replication = 1;
            stage.Operator = "Job Manager";
            stage.Name = "JobManager";
            stage.Code = null;
            this.stages.Add(stage.Id, stage);

            stage = new Stage();
            stage.Id = -2;
            stage.Replication = 1;
            stage.Operator = "All vertices";
            stage.Name = "All vertices";
            stage.Code = null;
            this.stages.Add(stage.Id, stage);
        }

        /// <summary>
        /// Find the stage given the stage id as a string.
        /// </summary>
        /// <param name="stageId">Stage id.</param>
        /// <returns>A handle to the stage with the specified static Id.</returns>
        public Stage GetStageByStaticId(string stageId)
        {
            int id = int.Parse(stageId);
            return this.stages[id];
        }

        /// <summary>
        /// Find the stage given the stage name.
        /// </summary>
        /// <param name="name">Name of stage to return.</param>
        /// <returns>The stage with the given name or null.</returns>
        public Stage GetStageByName(string name)
        {
            foreach (Stage s in this.stages.Values)
            {
                if (s.Name.Equals(name))
                    return s;
            }
            return null;
        }

        /// <summary>
        /// The list of all stages in the plan.
        /// </summary>
        /// <returns>An iterator over the list of stages.</returns>
        public IEnumerable<Stage> GetAllStages()
        {
            return this.stages.Values;
        }

        /// <summary>
        /// The list of all connections in the plan.
        /// </summary>
        /// <returns>An iterator over a list of connections.</returns>
        public IEnumerable<Connection> GetAllConnections()
        {
            return this.connections;
        }

        /// <summary>
        /// Number of stages in static plan.
        /// </summary>
        public int StageCount
        {
            get
            {
                return this.stages.Count - this.fictitiousStages;
            }
        }

        /// <summary>
        /// Get all connections adjacent to a stage. Warning: this method is inefficient.
        /// </summary>
        /// <param name="inputs">If true return the stage inputs, else return the stage outputs.</param>
        /// <param name="stage">Stage we are looking for.</param>
        /// <returns>A list of connections.</returns>
        public IEnumerable<Connection> GetStageConnections(Stage stage, bool inputs)
        {
            foreach (Connection c in this.GetAllConnections())
            {
                if (inputs && c.To == stage)
                    yield return c;
                else if (!inputs && c.From == stage)
                    yield return c;
            }
        }

        /// <summary>
        /// Factory: create the plan for a given job.
        /// </summary>
        /// <param name="dryadLinqJobInfo">Job to create plan for.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <returns>The plan or null.</returns>
        public static DryadJobStaticPlan CreatePlan(DryadLinqJobInfo dryadLinqJobInfo, StatusReporter reporter)
        {
            reporter("Trying to build static plan", StatusKind.LongOp);
            ClusterConfiguration config = dryadLinqJobInfo.ClusterConfiguration;
            IClusterResidentObject file = config.JobQueryPlan(dryadLinqJobInfo.Summary);
            if (config is CacheClusterConfiguration)
                config = (config as CacheClusterConfiguration).ActualConfig(dryadLinqJobInfo.Summary);

            if (file.Exception == null)
            {
                DryadJobStaticPlan retval;
                {
                    retval = new DryadLinqJobStaticPlan(config, file.GetStream());
                }
                retval.ParseQueryPlan();
                return retval;
            }
            else
            {
                reporter("Exception while looking for plan " + file.Exception.Message, StatusKind.Error);
                return null;
            }
        }
    }

    /// <summary>
    /// DryadLINQ-specific job information.
    /// </summary>
    public class DryadLinqJobStaticPlan : DryadJobStaticPlan
    {
        /// <summary>
        /// Create a DryadLinqJobStaticPlan.
        /// </summary>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="planFile">Stream containing the file.</param>
        // ReSharper disable once UnusedParameter.Local
        public DryadLinqJobStaticPlan(ClusterConfiguration config, ISharedStreamReader planFile)
            : base(planFile)
        {
        }


        /// <summary>
        /// Parse an XML query plan and represent that information.
        /// </summary>
        protected override void ParseQueryPlan()
        {
            string planString = this.planStream.ReadToEnd();

            XDocument plan = XDocument.Parse(planString);
// ReSharper disable PossibleNullReferenceException
            XElement query = plan.Root.Elements().First(e => e.Name == "QueryPlan");
            IEnumerable<XElement> vertices = query.Elements().Where(e => e.Name == "Vertex");

            foreach (XElement v in vertices)
            {
                Stage stage = new Stage();
                stage.Id = int.Parse(v.Element("UniqueId").Value);
                stage.Replication = int.Parse(v.Element("Partitions").Value);
                stage.Operator = v.Element("Type").Value;
                stage.Name = v.Element("Name").Value;
                {
                    string code = v.Element("Explain").Value;
                    string[] lines = code.Split('\n');
                    if (lines.Length > 1)
                    {
                        stage.Code = lines.Skip(1). // drop stage name
                            Select(l => l.Trim()). // remove leading tab
                            ToArray();
                    }
                    else
                    {
                        stage.Code = new string[] { };
                    }
                }
                this.stages.Add(stage.Id, stage);

                {
                    // These should be connection attributes, not stage attributes.
                    string cht = v.Element("ChannelType").Value;
                    string connectionManager = v.Element("DynamicManager").Element("Type").Value;
                    string connection = v.Element("ConnectionOperator").Value;
                    ConnectionInformation info = new ConnectionInformation();
                    info.ConnectionManager = connectionManager;
                    switch (connection)
                    {
                        case "Pointwise":
                            info.Arity = Connection.ConnectionType.PointToPoint;
                            break;
                        case "CrossProduct":
                            info.Arity = Connection.ConnectionType.AllToAll;
                            break;
                        default:
                            throw new CalypsoDryadException("Don't know about connection of type " + connection);
                    }
                    switch (cht)
                    {
                        case "DiskFile":
                            info.ChannelKind = Connection.ChannelType.DiskFile;
                            break;
                        case "TCPPipe":
                            info.ChannelKind = Connection.ChannelType.TCP;
                            break;
                        case "MemoryFIFO":
                            info.ChannelKind = Connection.ChannelType.Fifo;
                            break;
                        default:
                            throw new CalypsoDryadException("Don't know about channel of type " + cht);
                    }
                    this.perNodeConnectionInfo.Add(stage.Id, info);
                }

                switch (stage.Operator)
                {
                    case "InputTable":
                        stage.IsInput = true;
                        stage.UriType = v.Element("StorageSet").Element("Type").Value;
                        stage.Uri = v.Element("StorageSet").Element("SourceURI").Value;
                        break;
                    case "OutputTable":
                        stage.IsOutput = true;
                        stage.UriType = v.Element("StorageSet").Element("Type").Value;
                        stage.Uri = v.Element("StorageSet").Element("SinkURI").Value;
                        break;
                    case "Tee":
                        stage.IsTee = true;
                        break;
                    case "Concat":
                        stage.IsConcat = true;
                        break;
                }

                if (!v.Elements("Children").Any())
                    continue;

                bool first = true;
                var children = v.Element("Children").Elements().Where(e => e.Name == "Child").ToList();
                foreach (XElement child in children)
                {
                    // This code parallels the graphbuilder.cpp for XmlExecHost
                    Connection conn = new Connection();
                    int fromid = int.Parse(child.Element("UniqueId").Value);
                    ConnectionInformation fromConnectionInformation = this.perNodeConnectionInfo[fromid];
                    Stage from = this.stages[fromid];
                    conn.From = from;
                    conn.To = stage;
                    conn.ChannelKind = fromConnectionInformation.ChannelKind;

                    ConnectionInformation thisConnectionInformation = this.perNodeConnectionInfo[stage.Id];
                    switch (thisConnectionInformation.ConnectionManager)
                    {
                        case "FullAggregator":
                        case "HashDistributor":
                        case "RangeDistributor":
                            // Ignore except first child
                            if (first)
                            {
                                first = false;
                                conn.ConnectionManager = thisConnectionInformation.ConnectionManager;
                            }
                            else
                            {
                                conn.ConnectionManager = "";
                            }
                            break;
                        case "PartialAggregator":
                        case "Broadcast":
                            // All children have the same connection manager
                            conn.ConnectionManager = thisConnectionInformation.ConnectionManager;
                            break;
                        case "Splitter":
                            // The connection manager depends on the number of children
                            if (first)
                            {
                                first = false;
                                if (children.Count() == 1)
                                    conn.ConnectionManager = thisConnectionInformation.ConnectionManager;
                                else
                                    conn.ConnectionManager = "SemiSplitter";
                            }
                            else
                            {
                                conn.ConnectionManager = "";
                            }
                            break;
                        case "None":
                        case "":
                            break;
                    }


                    conn.Arity = fromConnectionInformation.Arity;

                    this.connections.Add(conn);
                }
            }
            // ReSharper restore PossibleNullReferenceException
        }
    }

    /// <summary>
    /// Scope-specific job information.
    /// </summary>
    public class ScopeJobStaticPlan : DryadJobStaticPlan
    {
        private readonly ISharedStreamReader vertexDef;

        /// <summary>
        /// Create a ScopeJobStaticPlan.
        /// </summary>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="planFile">Stream containing the file.</param>
        /// <param name="vertexDef">File containing the vertex definition (ScopeVertexDef.xml).</param>
        // ReSharper disable once UnusedParameter.Local
        public ScopeJobStaticPlan(ClusterConfiguration config, ISharedStreamReader planFile, ISharedStreamReader vertexDef)
            : base(planFile)
        {
            this.vertexDef = vertexDef;
        }

        /// <summary>
        /// Simplify the name of an IO.
        /// </summary>
        /// <param name="ioname">Name to simplify.</param>
        /// <returns>The simplified name.</returns>
        private static string NormalizeIOName(string ioname)
        {
            // drop everything between square braces
            Regex re = new Regex(@"\[.*\]");
            string result = re.Replace(ioname, "");
            return result;
        }

        /// <summary>
        /// Parse the Algebra file.
        /// </summary>
        private void ParseAlgebra()
        {
            // TODO: this parser is not really complete, as I don't understand the semantics of all xml elements.
            Dictionary<string, string> outToStage = new Dictionary<string, string>(); // map an output to a stage name. Assume that ios have unique names.
            Dictionary<string, List<string>> inputs = new Dictionary<string, List<string>>();

            // <CsJobAlgebra> <graph> <process> ...
            string planString = this.planStream.ReadToEnd();
            XDocument plan = XDocument.Parse(planString);
// ReSharper disable PossibleNullReferenceException
            XElement graph = plan.Root.Element("graph"); // graph node, children are stages

            // add stages
            int id = 0;
            foreach (XElement child in graph.Elements())
            {
                if (child.Name == "process")
                {
                    string stageName = child.Attribute("id").Value;

                    Stage stage = new Stage();
                    stage.Name = stageName;
                    stage.Replication = 1;
                    stage.Code = new string[] { child.Attribute("command").Value };
                    stage.Id = id++;
                    this.stages.Add(stage.Id, stage);
                    List<string> stageInputs = new List<string>();
                    inputs.Add(stageName, stageInputs);

                    foreach (var io in child.Elements())
                    {
                        if (io.Name != "input" && io.Name != "output")
                            continue;

                        string cosmosStream = io.Attribute("cosmosStream") != null ? io.Attribute("cosmosStream").Value : null;
                        string structuredStream = io.Attribute("structuredStream") != null ? io.Attribute("structuredStream").Value : null;
                        string ioid = NormalizeIOName(io.Attribute("id").Value);
                        string streamname = cosmosStream ?? structuredStream;

                        if (io.Name == "input")
                        {
                            stageInputs.Add(ioid);

                            if (streamname != null)
                            {
                                Stage alreadyDone = this.GetStageByName(streamname);
                                if (alreadyDone == null)
                                {
                                    Stage input = new Stage();
                                    input.Id = id++;
                                    input.IsInput = true;
                                    input.Name = streamname;
                                    input.Code = new string[0];
                                    input.Replication = 1;
                                    input.UriType = "cosmos";
                                    input.Uri = "cosmos://" + streamname;
                                    this.stages.Add(input.Id, input);
                                }
                                outToStage.Add(ioid, streamname);
                            }
                        }
                        else if (io.Name == "output")
                        {
                            outToStage.Add(ioid, stageName);

                            if (streamname != null)
                            {
                                Stage alreadyDone = this.GetStageByName(streamname);
                                if (alreadyDone == null)
                                {
                                    Stage output = new Stage();
                                    output.IsOutput = true;
                                    output.Replication = 1;
                                    output.Name = streamname;
                                    output.Code = new string[0];
                                    output.Id = id++;
                                    output.UriType = "cosmos";
                                    output.Uri = "cosmos://" + streamname;
                                    this.stages.Add(output.Id, output);
                                    inputs.Add(streamname, new List<string> { ioid });
                                }
                            }
                        }
                    }
                }
                else if (child.Name == "dataset")
                {
                    string stageName = child.Attribute("id").Value;
                    stageName = NormalizeIOName(stageName);

                    Stage stage = new Stage();
                    stage.Name = stageName;
                    stage.Replication = 1;
                    stage.IsConcat = true;
                    stage.Code = new string[0];
                    stage.Id = id++;
                    this.stages.Add(stage.Id, stage);
                    List<string> stageInputs = new List<string>();
                    inputs.Add(stageName, stageInputs);

                    foreach (var io in child.Elements())
                    {
                        if (io.Name == "element")
                        {
                            string ioid = NormalizeIOName(io.Attribute("id").Value);
                            stageInputs.Add(ioid);
                        }
                    }

                    // implicit output with stage name
                    outToStage.Add(stage.Name, stage.Name);
                }
                else if (child.Name == "inputStreamList")
                {
                    Stage input = new Stage();
                    input.Id = id++;
                    input.IsInput = true;
                    input.Name = child.Attribute("id").Value;
                    input.Replication = 1;
                    input.UriType = "cosmos";
                    input.Uri = "inputStreamList";
                    input.Code = child.Elements().Select(e => e.Attribute("cosmosPath").Value).ToArray();
                    this.stages.Add(input.Id, input);
                    outToStage.Add(input.Name, input.Name);
                }
                else if (child.Name == "outputStreamList")
                {
                    Stage output = new Stage();
                    output.Id = id++;
                    output.IsOutput = true;
                    output.Name = child.Attribute("id").Value;
                    output.Replication = 1;
                    output.UriType = "cosmos";
                    output.Uri = "outputStreamList";
                    output.Code = child.Elements().Select(e => e.Attribute("cosmosPath").Value).ToArray();
                    this.stages.Add(output.Id, output);
                    inputs.Add(output.Name, new List<string> { output.Name });
                }
            }

            // scan the dictionaries and build the edges
            foreach (string stage in inputs.Keys)
            {
                foreach (string inputName in inputs[stage])
                {
                    string iName = inputName;
                    if (outToStage.ContainsKey(iName))
                    {
                        string sourceStage = outToStage[iName];
                        Connection conn = new Connection();
                        conn.From = this.GetStageByName(sourceStage);
                        conn.To = this.GetStageByName(stage);
                        conn.ChannelKind = Connection.ChannelType.DiskFile;
                        conn.ConnectionManager = "";
                        conn.Arity = Connection.ConnectionType.PointToPoint;
                        this.connections.Add(conn);
                    }
                    else
                    {
                        Trace.TraceInformation("Could not find stage for input {0}", iName);
                    }
                }
            }
            // ReSharper restore PossibleNullReferenceException
        }

        /// <summary>
        /// Parse the vertex definition file.
        /// </summary>
        private void ParseVertexDef()
        {
            if (this.vertexDef.Exception != null)
                return;

            // <ScopeVertices> <ScopeVertex> <operator> <input> </input> <output> </output>
            string planString = this.vertexDef.ReadToEnd();
            XDocument vxDef = XDocument.Parse(planString);

            XElement vertices = vxDef.Root;
// ReSharper disable PossibleNullReferenceException
            foreach (XElement vertex in vertices.Elements())
            {
                List<string> code = new List<string>();
                string id = vertex.Attribute("id").Value;
                Stage stage = this.GetStageByName(id);
                if (stage == null) 
                {
                    Trace.TraceInformation("Could not find stage {0}", id);
                    continue;
                }

                foreach (XElement op in vertex.Elements("operator")) 
                {
                    string className = op.Attribute("className").Value;
                    if (op.Attribute("args") != null)
                        className += " " + op.Attribute("args").Value;
                    code.Add(className);
                    foreach (XElement input in op.Elements("input"))
                    {
                        XAttribute indexatt = input.Attribute("inputIndex");
                        string index = indexatt != null ? indexatt.Value : " "; 
                        string schema = input.Attribute("schema").Value;
                        code.Add("\tI" + index + ": " + schema);
                    }

                    foreach (XElement output in op.Elements("output"))
                    {
                        XAttribute indexatt = output.Attribute("outputIndex");
                        string index = indexatt != null ? indexatt.Value : " ";
                        string schema = output.Attribute("schema").Value;
                        code.Add("\tO" + index + ": " + schema);
                    }
                }

                stage.Code = code.ToArray();
                // ReSharper restore PossibleNullReferenceException
            }
        }

        /// <summary>
        /// Parse the query plan for a Scope job.
        /// </summary>
        protected override void ParseQueryPlan()
        {
            this.ParseAlgebra();
            this.ParseVertexDef();
        }
    }

    /// <summary>
    /// A collection describing all the vertices encountered so far in a job.
    /// </summary>
    internal class JobVertices
    {
        /// <summary>
        /// Vertices indexed by numeric vertex id.  A vertex can have multiple executions.
        /// </summary>
        private readonly Dictionary<int, List<ExecutedVertexInstance>> vertices;
        /// <summary>
        /// Total number of vertices in collection.
        /// </summary>
        private int count;

        private readonly Dictionary<string, List<ExecutedVertexInstance>> jobStages;
        private readonly Dictionary<string, ExecutedVertexInstance> vertexByGuid;

        /// <summary>
        /// Create a collection representing the job vertices.
        /// </summary>
        public JobVertices()
        {
            this.count = 0;
            this.vertexByGuid = new Dictionary<string, ExecutedVertexInstance>();
            this.vertices = new Dictionary<int, List<ExecutedVertexInstance>>();
            this.jobStages = new Dictionary<string, List<ExecutedVertexInstance>>();
            this.jobStages.Add("All vertices", new List<ExecutedVertexInstance>()); // this list holds all vertices
        }

        /// <summary>
        /// Number of stages in job.
        /// </summary>
        public int ExecutedStageCount { get { return this.jobStages.Count - 2; } } // subtract GM and all vertices

        /// <summary>
        /// The list of vertices in this stage.
        /// </summary>
        /// <param name="stagename">Name of stage to return.</param>
        /// <returns>The vertices in the stage, or null if the stage is not found (e.g., it is a table).</returns>
        public List<ExecutedVertexInstance> GetStageVertices(string stagename)
        {
            if (this.jobStages.ContainsKey(stagename))
                return this.jobStages[stagename];
            return null;
        }

        /// <summary>
        /// The list of all stage names.
        /// </summary>
        /// <returns>An iterator over the stage names.</returns>
        public IEnumerable<string> GetAllStageNames()
        {
            return this.jobStages.Keys;
        }
        
        /// <summary>
        /// Number of vertices in job.
        /// </summary>
        public int Count
        {
            get { return this.count; }
        }

        /// <summary>
        /// Add a new vertex to this job.
        /// </summary>
        /// <param name="vi">Vertex description to add.</param>
        /// <returns>Stage name that the vertex belongs to.</returns>
        public void Add(ExecutedVertexInstance vi)
        {
            int id = vi.Number;
            List<ExecutedVertexInstance> l;

            if (vertices.ContainsKey(id))
                l = vertices[id];
            else
            {
                l = new List<ExecutedVertexInstance>();
                vertices.Add(id, l);
            }
            l.Add(vi);
            this.count++;
            string stage = vi.StageName;
            List<ExecutedVertexInstance> members;
            if (this.jobStages.ContainsKey(stage))
                members = this.jobStages[stage];
            else
            {
                members = new List<ExecutedVertexInstance>();
                this.jobStages.Add(stage, members);
            }
            members.Add(vi);

            if (!this.vertexByGuid.ContainsKey(vi.UniqueID))
                this.vertexByGuid.Add(vi.UniqueID, vi);

            this.jobStages["All vertices"].Add(vi);
        }

        /// <summary>
        /// Find the information associated with a given vertex and version.
        /// </summary>
        /// <param name="id">Vertex number in job.</param>
        /// <param name="version">Vertex version.</param>
        /// <returns>Matching VertexInfo or null if no vertex exists.</returns>
        public ExecutedVertexInstance FindVertex(int id, int version)
        {
            if (!this.vertices.ContainsKey(id))
                return null;

            List<ExecutedVertexInstance> l = this.vertices[id];
            foreach (ExecutedVertexInstance i in l)
                if (i.Version == version)
                    return i;
            return null;
        }

        /// <summary>
        /// Find a vertex from its guid.  Currently very slow.
        /// </summary>
        /// <param name="guid">Vertex guid.</param>
        /// <returns>The vertex with the correct guid, or null.</returns>
        public ExecutedVertexInstance FindVertexByGuid(string guid)
        {
            if (this.vertexByGuid.ContainsKey(guid))
                return this.vertexByGuid[guid];
            return null;
        }

        /// <summary>
        /// The set of all vertices in the job.
        /// </summary>
        /// <returns>An enumerator over VertexInfo objects.</returns>
        public IEnumerable<ExecutedVertexInstance> AllVertices()
        {
            foreach (int key in this.vertices.Keys)
            {
                List<ExecutedVertexInstance> l = this.vertices[key];
                foreach (ExecutedVertexInstance vi in l)
                    yield return vi;
            }
        }

        /// <summary>
        /// A vertex has received a new guid.
        /// </summary>
        /// <param name="vi">Executed vertex instance.</param>
        /// <param name="newGuid">New guid.</param>
        internal void Remap(ExecutedVertexInstance vi, string newGuid)
        {
            if (!this.vertexByGuid.ContainsKey(newGuid))
                this.vertexByGuid.Add(newGuid, vi);
        }
    }

    /// <summary>
    /// Brief description of a channel endpoint.
    /// </summary>
    public class ChannelEndpointDescription
    {
        /// <summary>
        /// Is the endpoint of this channel an input?
        /// </summary>
        public bool IsInput { get; protected set; }
        /// <summary>
        /// The input/output number.
        /// </summary>
        public int Number { get; protected set; }
        /// <summary>
        /// Type of URI.
        /// </summary>
        public string UriType { get; protected set; }
        /// <summary>
        /// Part of URI without the type to the channel contents.
        /// </summary>
        public string LocalPath { get; protected set; }
        /// <summary>
        /// How big is the channel (0 if it cannot be determined, e.g. FIFO, -1 if the channel data cannot be retrieved).
        /// </summary>
        public long Size { get; set; }

        /// <summary>
        /// String representation of the endpoint.
        /// </summary>
        public override string ToString()
        {
            string uritype = this.UriType;
            string localpath = this.LocalPath;
            return string.Format("{0,4} {1,20:N0} {2}://{3}", this.Number, this.Size, uritype, localpath);
        }

        /// <summary>
        /// Create a channel endpoint description
        /// </summary>
        /// <param name="isInput">True if the channel endpoint is an input.</param>
        /// <param name="number">The input/output number.</param>
        /// <param name="uri">URI to channel contents.</param>
        /// <param name="uripathprefix">Relative uris will need this prefix appended.</param>
        /// <param name="fast">If true the channel size is not computed (this is much faster).</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        // ReSharper disable once UnusedParameter.Local
        public ChannelEndpointDescription(bool isInput, int number, string uri, string uripathprefix, bool fast, StatusReporter reporter)
        {
            this.IsInput = isInput;
            this.Number = number;

            int sepindex = uri.IndexOf("://");
            if (sepindex < 0)
                throw new CalypsoDryadException("Channel URI " + uri + " does not contain separator ://");

            this.UriType = uri.Substring(0, sepindex);
            // some HPC URIs use the compression scheme as an "option" (not really defined for file:// uris, but...)
            // strip it here
            int option = uri.IndexOf('?');
            if (option >= 0)
            {
                uri = uri.Substring(0, option);
            }
            this.LocalPath = uri.Substring(sepindex + 3);

            if (uripathprefix != null) {
                // Unfortunately the uri is absolute, although it should be relative sometimes. We fix this here.
                this.LocalPath = Path.Combine(uripathprefix, this.LocalPath);
            }

            if (fast)
                this.Size = 0;
            else
            {
                switch (this.UriType)
                {
                    case "file":
                        if (File.Exists(this.LocalPath))
                        {
                            this.Size = new FileInfo(this.LocalPath).Length;
                        }
                        else
                        {
                            this.Size = -1;
                        }
                        break;
                    default:
                        this.Size = 0;
                        break;
                }
            }
        }

        /// <summary>
        /// Create a channel endpoint description from Scope information.
        /// </summary>
        /// <param name="isInput">True if the channel endpoint is an input.</param>
        /// <param name="number">The input/output number.</param>
        /// <param name="uri">URI to channel contents.</param>
        /// <param name="size">Size of channel if known.</param>
        public ChannelEndpointDescription(bool isInput, int number, string uri, long size)
        {
            this.IsInput = isInput;
            this.Number = number;
            int sepindex = uri.IndexOf("://");
            if (sepindex < 0)
                throw new CalypsoClusterException("Channel URI " + uri + " does not contain separator ://");
            this.UriType = uri.Substring(0, sepindex);
            this.LocalPath = uri.Substring(sepindex + 3);
            this.Size = size;
        }
    }

    /// <summary>
    /// An instance of an executed vertex (each vertex may execute multiple times).
    /// </summary>
    public class ExecutedVertexInstance
    {
        /// <summary>
        /// State that the vertex is in.
        /// </summary>
        public enum VertexState
        {
            /// <summary>
            /// Scheduled but never executed (duplicate scheduling abandoned).
            /// </summary>
            Abandoned,
            /// <summary>
            /// Vertex has been created but not started.
            /// </summary>
            Created,
            /// <summary>
            /// Vertex has started running, but has not yet completed.
            /// </summary>
            Started,
            /// <summary>
            /// Vertex has been cancelled.
            /// </summary>
            Cancelled,
            /// <summary>
            /// Vertex has been cancelled after completing successfully.
            /// </summary>
            Invalidated,
            /// <summary>
            /// Vertex has completed successfully.
            /// </summary>
            Successful,
            /// <summary>
            /// Vertex has failed.
            /// </summary>
            Failed,
            /// <summary>
            /// Vertex has been cancelled by the scheduler.
            /// </summary>
            Revoked,
            /// <summary>
            /// Vertex state is not yet known.
            /// </summary>
            Unknown,
        };

        /// <summary>
        /// State the vertex is in.
        /// </summary>
        public VertexState State { get; protected set; }
        /// <summary>
        /// The error message related to this vertex.
        /// </summary>
        string error;
        /// <summary>
        /// Directory where the vertex executed.
        /// </summary>
        public IClusterResidentObject WorkDirectory { get; protected set; }
        /// <summary>
        /// Amount of data read by vertex (may be unknown, then it's -1).
        /// </summary>
        public long DataRead { get; internal set; }
        /// <summary>
        /// Amount of data written by vertex (may be unknown, then it's -1).
        /// </summary>
        public long DataWritten { get; internal set; }
        /// <summary>
        /// On some platforms this is a guid, but not always. At least the identifier is unique per job.
        /// </summary>
        public string UniqueID { get; protected set; }
        /// <summary>
        /// String representation of the cluster configuration type.
        /// </summary>
        private readonly string ClusterConfigType;

        // <ErrorCode>0x830a0017<!-- Vertex Had Errors --></ErrorCode>
        static readonly Regex errorCodeRegex = new Regex(@"Vertex Had Errors, \<ErrorCode\>(.*)\<!--.*--\>\</ErrorCode\>", RegexOptions.Compiled);
        // Super__128[0][1] -> stage name is Super__128[0]
        static readonly Regex stageNameRegex = new Regex(@"(.*)\[(\d+)\]$", RegexOptions.Compiled);
        /// <summary>
        /// Dynamic names have really strange names: Dynamic__128[13]+[0]. We want to bundle all these into a single dynamic stage.
        /// </summary>
        static readonly Regex dynamicStageNameRegex = new Regex(@"(Dynamic__(\d+))\[\d+\](\+\[(\d+)\])*$", RegexOptions.Compiled);
        /// <summary>
        /// Some scope stages have double indices.
        /// </summary>
        static readonly Regex scopeStageNameRegex = new Regex(@"([^]]*)(\[\d+\](\[\d+\])*)$", RegexOptions.Compiled);
      
        /// <summary>
        /// Create a vertex information.
        /// </summary>
        /// <param name="job">Information about the current job.</param>
        /// <param name="number">Vertex number, unique in job.</param>
        /// <param name="version">Vertex version.</param>
        /// <param name="name">Name of vertex in graph.</param>
        /// <param name="uniqueId">Unique vertex identifier; on some platforms the value is not correct at this point.</param>
        /// <param name="timeStamp">Time when vertex was created; maybe MinValue if unknown.</param>
        public ExecutedVertexInstance(DryadLinqJobInfo job, int number, int version, string name, string uniqueId, DateTime timeStamp)
        {
            this.Number = number;
            this.Name = name;
            this.Version = version;
            this.ProcessIdentifier = new DryadProcessIdentifier();
            this.IsManager = false;
            this.DataRead = -1;
            this.DataWritten = -1;
            this.State = VertexState.Created;
            this.error = "";
            this.Machine = "";
            this.timingSet = false;
            this.UniqueID = uniqueId;
            this.ClusterConfigType = job.ClusterConfiguration.GetType().ToString();
            this.channelsAreFinal = false;
            this.ComputeStageName();

            this.CreationTime = timeStamp;
            this.Start = this.StartCommandTime = this.VertexScheduleTime = this.End = DateTime.MinValue;
        }

        /// <summary>
        /// The vertex has started.
        /// </summary>
        /// <param name="machine">Machine on which vertex is run.</param>
        /// <param name="job">Job containing the vertex.</param>
        /// <param name="approxStartTime">Approximate starting time (the real value is known when the vertex is terminated).</param>
        /// <param name="identifier">Id of process running this vertex (several vertices may share a process).</param>
        /// <param name="uniqueId">Unique identifier.</param>
        public void SetStartInformation(DryadLinqJobInfo job, string machine, DateTime approxStartTime, DryadProcessIdentifier identifier, string uniqueId)
        {

            this.Machine = machine;
            this.Start = approxStartTime;
            this.ProcessIdentifier = identifier;
            this.WorkDirectory = job.ClusterConfiguration.ProcessWorkDirectory(this.ProcessIdentifier, false, machine, job.Summary);
            this.StdoutFile = job.ClusterConfiguration.ProcessStdoutFile(this.ProcessIdentifier, false, machine, job.Summary);
            this.SetState(VertexState.Started);
            if (approxStartTime == DateTime.MinValue)
                throw new CalypsoDryadException("Unexpected small start time for vertex");
            this.LogDirectory = job.ClusterConfiguration.ProcessLogDirectory(this.ProcessIdentifier, false, machine, job.Summary);
            this.LogFilesPattern = job.ClusterConfiguration.VertexLogFilesPattern(false, job.Summary);
            this.UniqueID = uniqueId;
            
            if (this.StdoutFile != null)
                this.StdoutFile.ShouldCacheLocally = false; // don't cache until vertex proved terminated
            if (this.LogDirectory != null)
                this.LogDirectory.ShouldCacheLocally = false;
        }

        /// <summary>
        /// Is this "vertex" a job manager?
        /// </summary>
        public bool IsManager { get; internal set; }
        /// <summary>
        /// Path to file containing logged vertex standard output.
        /// </summary>
        public IClusterResidentObject StdoutFile { get; protected set; }
        /// <summary>
        /// Path to directory containing the logs generated by this vertex.
        /// </summary>
        public IClusterResidentObject LogDirectory { get; protected set; }
        /// <summary>
        /// Pattern matching the log files generated by this vertex in the log directory.
        /// </summary>
        public string LogFilesPattern { get; protected set; }
        /// <summary>
        /// Guid of the process which contained this vertex (and maybe other vertices too).
        /// </summary>
        public DryadProcessIdentifier ProcessIdentifier { get; internal set; }
        /// <summary>
        /// Vertex version (each vertex may execute multiple times.)
        /// </summary>
        public int Version { get; protected set; }
        /// <summary>
        /// Machine where vertex ran.
        /// </summary>
        public string Machine { get; protected set; }
        /// <summary>
        /// Name of vertex, such as Select__1[3].
        /// </summary>
        public string Name { get; protected set; }
        /// <summary>
        /// Return vertex number.
        /// </summary>
        public int Number { get; set; }

        /// <summary>
        /// If this is true the cached channels won't change anymore.
        /// </summary>
        private bool channelsAreFinal;
        /// <summary>
        /// The input channels of this vertex; this must be explicitly computed by invoking DiscoverChannels(), since it's expensive.
        /// </summary>
        public Dictionary<int, ChannelEndpointDescription> InputChannels { get; set; }
        /// <summary>
        /// The output channels of this vertex; this must be explicitly computed by invoking DiscoverChannels(), since it's expensive.
        /// </summary>
        public Dictionary<int, ChannelEndpointDescription> OutputChannels { get; set; }

        /// <summary>
        /// Compute the stage name and partition number.
        /// </summary>
        private void ComputeStageName()
        {
            if (this.ClusterConfigType.Contains("Scope"))
            {
                // Stage names extactred from the Scope algebra look different
                Match m = scopeStageNameRegex.Match(this.Name);
                if (m.Success)
                {
                    this.stageName = m.Groups[1].Value;
                }
                else
                {
                    this.stageName = this.Name;
                }
            }
            else
            {
                Match m = dynamicStageNameRegex.Match(this.Name);
                if (m.Success)
                {
                    this.stageName = m.Groups[1].Value;
                }
                else
                {
                    m = stageNameRegex.Match(this.Name);
                    if (m.Success)
                    {
                        this.stageName = m.Groups[1].Value;
                    }
                    else
                    {
                        this.stageName = this.Name;
                    }
                }
            }
        }

        private string stageName;
        /// <summary>
        /// Try to guess the stage name from the vertex name.  Not easy for generic Dryad jobs, doable for DryadLINQ jobs.
        /// </summary>
        public string StageName
        {
            // cache the result
            get {
                if (this.stageName == null) {
                    this.ComputeStageName();
                }
                return this.stageName;
            }
        }

        #region TIMING_INFORMATION
        /// <summary>
        /// True if the timing information has been set.
        /// </summary>
        private bool timingSet;
        /// <summary>
        /// Time when vertex was created.
        /// </summary>
        public DateTime CreationTime { get; internal set; }
        /// <summary>
        /// Time when vertex process is scheduled.
        /// </summary>
        public DateTime VertexScheduleTime { get; internal set; }
        /// <summary>
        /// Time when process start command is issued to the PN.
        /// </summary>
        public DateTime StartCommandTime { get; internal set; }
        /// <summary>
        /// Time when process is created by PN.
        /// </summary>
        public DateTime Start { get; protected set; }
        /// <summary>
        /// Time when vertex has completed
        /// </summary>
        public DateTime End { get; internal set; }

        /// <summary>
        /// How long has the vertex been running?
        /// </summary>
        public TimeSpan RunningTime
        {
            get
            {
                if (this.Start != DateTime.MinValue && this.End != DateTime.MinValue)
                    return this.End - this.Start;
                else
                    return TimeSpan.MinValue;
            }
        }

        /// <summary>
        /// Mark the fact that the vertex was still running at the specified time.
        /// </summary>
        /// <param name="when">Time when vertex was known to be running.</param>
        public void MarkVertexWasRunning(DateTime when)
        {
            if (this.Start == DateTime.MinValue)
            {
                Trace.TraceInformation("Vertex {0} which is not started is still running?", this.Name);
                return;
                //throw new CalypsoClusterException("Vertex which is not started is still running?");
            }
            if (this.Start > when)
                // This can happen if the cluster clocks are not synchronized with the local machine clocks.
                return;
            this.End = when;
        }

        /// <summary>
        /// Set the timing parameters for a vertex execution.
        /// Some times can be negative, if the respective states were actually never certainly reached.
        /// </summary>
        /// <param name="creation">Absolute time of vertex creation.</param>
        /// <param name="creatToScheduleTime">Number of seconds from creation to process being ready to run.</param>
        /// <param name="schedToStartProcessTime">Number of seconds from ready to run to process dispatch to PN.</param>
        /// <param name="startToCreatedProcessTime">Number of seconds from start to process creation on PN.</param>
        /// <param name="processToRunTime">Number of seconds from process creation to process running on PN.</param>
        /// <param name="runToCompTime">Actual running time of process.</param>
        internal DateTime SetTiming(DateTime creation,
            string creatToScheduleTime, string schedToStartProcessTime, string startToCreatedProcessTime, string processToRunTime, string runToCompTime)
        {
            if (this.timingSet)
                // a vertex may be cancelled after it's already terminated, e.g, due to a missing output.
                // but the second timing information is incorrect
                return DateTime.MinValue;

            if (this.State == VertexState.Successful || this.State == VertexState.Failed)
                // allow the timing to be set again for cancelled vertices
                this.timingSet = true;
            this.CreationTime = creation;
            double ctst = Double.Parse(creatToScheduleTime);
            double stsp = Double.Parse(schedToStartProcessTime);
            double stcp = Double.Parse(startToCreatedProcessTime);
            double ptrt = Double.Parse(processToRunTime);
            double rtct = Double.Parse(runToCompTime);
            double totSeconds = ctst + stsp + stcp + ptrt + rtct;
            TimeSpan total = TimeSpan.FromSeconds(totSeconds);
            DateTime totalTime = creation + total;
            if (totSeconds < 0)
                throw new CalypsoDryadException("Negative total time for vertex " + this.Name);

            // if the vertex has no machine just ignore the times
            if (string.IsNullOrEmpty(this.Machine))
                return totalTime;

            if (ctst >= 0)
            {
                TimeSpan creatToSchedule = TimeSpan.FromSeconds(ctst);
                this.VertexScheduleTime = this.CreationTime + creatToSchedule;
                if (stsp >= 0)
                {
                    TimeSpan schedToStartProcess = TimeSpan.FromSeconds(stsp);
                    this.StartCommandTime = this.VertexScheduleTime + schedToStartProcess;
                    if (stcp >= 0)
                    {
                        TimeSpan startToCreatedProcess = TimeSpan.FromSeconds(stcp);
                        if (ptrt >= 0)
                        {
                            TimeSpan processToRun = TimeSpan.FromSeconds(ptrt);
                            this.Start = this.StartCommandTime + startToCreatedProcess + processToRun;
                            if (rtct >= 0)
                            {
                                TimeSpan runToComp = TimeSpan.FromSeconds(rtct);
                                this.End = this.Start + runToComp;
                            }
                        }
                    }
                }
            }
            return totalTime;
        }
        #endregion

        /// <summary>
        /// Set termination status of a vertex based on the status string.
        /// </summary>
        /// <param name="status">Status reported by job manager.</param>
        /// <returns>Failure status of vertex: false if it failed with error, true if it succeeded or it was a killed duplicate.</returns>
        /// <param name="time">Time when vertex terminated.  Maybe MinValue if unknown.</param>
        internal bool SetTermination(string status, DateTime time)
        {
            if (status.ToLower().StartsWith("vertex has completed"))
            {
                this.SetState(VertexState.Successful);
            }
            else if (status.ToLower().StartsWith("killed"))
            {
                this.SetState(VertexState.Cancelled);
            }
            else if (status.ToLower().StartsWith("failed"))
            {
                this.SetState(VertexState.Failed);
            }
            else if (status.ToLower().StartsWith("vertex had errors"))
            {
                Match m = errorCodeRegex.Match(status);
                // Vertex Had Errors, <ErrorCode>0x830a0017<!-- Vertex Had Errors --></ErrorCode>
                if (m.Success)
                    this.ExitCode = m.Groups[1].Value;
                else
                    this.ExitCode = status;
                this.SetState(VertexState.Failed);
            }
            else if (status.ToLower().StartsWith("vertex received termination"))
            {
                this.SetState(VertexState.Cancelled);
            }
            else
            {
                this.SetState(VertexState.Failed);
            }

            if (time != DateTime.MinValue)
                this.End = time;
            return this.State == VertexState.Failed;
        }

        /// <summary>
        /// Additional information about the vertex error.
        /// </summary>
        /// <param name="s">Information about error.</param>
        internal void AddErrorString(string s)
        {
            if (this.error == null)
                this.error = "";
            this.error += "\n" + s;
        }

        /// <summary>
        /// Exit code of vertex.
        /// </summary>
        public string ExitCode { get; set; }

        /// <summary>
        /// Return the error string.
        /// </summary>
        /// <returns>The error string as reported to the JM.</returns>
        public string ErrorString
        {
            get
            {
                return this.error;
            }
            set
            {
                if (string.IsNullOrEmpty(this.error))
                    this.error = value;
                else
                    this.error += " " + value;
            }
        }

        /// <summary>
        /// Parse a part of the 'originalInfo.txt' file to discover a set of channel endpoints.
        /// </summary>
        /// <param name="sr">Stream reader which contains the channel information.</param>
        /// <returns>The list of channels, or null on failure.</returns>
        /// <param name="uriprefix">If the channel is an output, prefix the path with this; this is null for inputs.</param>
        /// <param name="skip">If true, do not return anything (still useful to advance the stream reader).</param>
        /// <param name="fast">If true the channel sizes are not discovered; this is much faster, since no remote machines are queried for files.</param>
        /// <param name="updateProgress">Delegate used to report progress.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        private Dictionary<int, ChannelEndpointDescription> DiscoverOriginalInfoChannels(ISharedStreamReader sr, string uriprefix, bool skip, bool fast, StatusReporter reporter, Action<int> updateProgress)
        {
            bool isInput = uriprefix == null;

            string countline = sr.ReadLine();
            if (countline == null)
                return null;
            int channelCount;
            int spaceIndex = countline.IndexOf(' ');
            if (spaceIndex > 0)
                countline = countline.Substring(0, spaceIndex);
            bool success = int.TryParse(countline, out channelCount);
            if (!success)
                return null;
            var channels = new Dictionary<int, ChannelEndpointDescription>(channelCount);
            for (int i = 0; i < channelCount; i++)
            {
                string channel = sr.ReadLine();
                if (channel == null)
                {
                    if (updateProgress != null)
                        updateProgress(100);
                    return null;
                }
                if (!skip)
                {
                    ChannelEndpointDescription desc = new ChannelEndpointDescription(isInput, i, channel, uriprefix, fast, reporter);
                    channels.Add(i, desc);
                    if (updateProgress != null)
                        updateProgress(i * 100 / channelCount);
                }
            }
            
            if (updateProgress != null)
                updateProgress(100);
            if (skip)
                return null;
            return channels;
        }

        /// <summary>
        /// Discover the vertex channels in a vertex-*-rerun file.
        /// </summary>
        /// <returns>True if the discovery was successful.</returns>
        /// <param name="inputs">If true discover the inputs.</param>
        /// <param name="outputs">If true discover the outputs.</param>
        /// <param name="fast">If true do not discover the channel sizes (much faster).</param>
        /// <param name="progress">Delegate used to report progress.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        public bool DiscoverOriginalInfoChannels(bool inputs, bool outputs, bool fast, StatusReporter reporter, Action<int> progress)
        {
            string filename = string.Format("vertex-{0}-{1}-rerun-originalInfo.txt", this.Number, this.Version);
            bool success = true;

            // The format of this file is fixed.
            if (this.InputChannels != null)
                // skip discovery
                inputs = false;
            ISharedStreamReader sr = this.WorkDirectory.GetFile(filename).GetStream();
            var channels = this.DiscoverOriginalInfoChannels(sr, null, !inputs, fast, reporter, progress);
            if (channels == null)
            {
                if (inputs)
                    success = false;
            }
            else
                this.InputChannels = channels;
            if (this.OutputChannels != null)
                // skip discovery
                outputs = false;
            channels = this.DiscoverOriginalInfoChannels(sr, this.WorkDirectory.ToString(), !outputs, fast, reporter, progress);
            if (channels == null)
            {
                if (outputs)
                    success = false;
            }
            else
                this.OutputChannels = channels;
            sr.Close();
            return success;
        }

        /// <summary>
        /// Discover the vertex channels in a Scope-generated vcmdStart*xml file.
        /// </summary>
        /// <returns>True if the discovery was successful.</returns>
        /// <param name="inputs">If true discover the inputs.</param>
        /// <param name="outputs">If true discover the outputs.</param>
        /// <param name="fast">If true do not discover the channel sizes (much faster).</param>
        /// <param name="progress">Delegate used to report progress.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        // ReSharper disable UnusedParameter.Global
        public bool DiscoverScopeChannels(bool inputs, bool outputs, bool fast, StatusReporter reporter, Action<int> progress)
            // ReSharper restore UnusedParameter.Global
        {
            // find the xml file
            var files = this.WorkDirectory.GetFilesAndFolders("vcmdStart*.xml").ToList();
            if (files.Count != 1)
            {
                reporter("Cannot locate vcmdStart*.xml file", StatusKind.Error);
                return false;
            }
            ISharedStreamReader sr = files.First().GetStream();
            if (sr.Exception != null)
            {
                reporter("Error reading vcmdStart*.xml file" + sr.Exception.Message, StatusKind.Error);
                return false;
            }
            
            // ReSharper disable PossibleNullReferenceException
            XDocument plan = XDocument.Parse(sr.ReadToEnd());
            if (inputs && this.InputChannels == null)
            {
                var channels = new Dictionary<int, ChannelEndpointDescription>();
                IEnumerable<XElement> inputsData = plan.Root.Element("inputs").Elements();
                int chno = 0;
                foreach (var e in inputsData)
                {
                    string chpath = e.Attribute("path").Value;
                    long size = long.Parse(e.Attribute("length").Value);
                    ChannelEndpointDescription desc = new ChannelEndpointDescription(true, chno, chpath, size);
                    channels.Add(chno, desc);
                    chno++;
                }
                this.InputChannels = channels;
            }

            if (outputs && this.OutputChannels == null)
            {
                var channels = new Dictionary<int, ChannelEndpointDescription>();
                IEnumerable<XElement> inputsData = plan.Root.Element("outputs").Elements();
                int chno = 0;
                foreach (var e in inputsData)
                {
                    string chpath = e.Attribute("path").Value;
                    ChannelEndpointDescription desc = new ChannelEndpointDescription(true, chno, chpath, -1);
                    channels.Add(chno, desc);
                    chno ++;
                }
                this.OutputChannels = channels;
            }
            // ReSharper restore PossibleNullReferenceException
            
            sr.Close();
            return true;
        }

        /// <summary>
        /// Discover the input and output channels of the vertex.  Populates the 'InputChannels' and 'OutputChannel' lists.
        /// </summary>
        /// <returns>True if the discovery was successful.</returns>
        /// <param name="inputs">If true discover the inputs.</param>
        /// <param name="outputs">If true discover the outputs.</param>
        /// <param name="fast">If true do not discover the channel sizes (much faster).</param>
        /// <param name="progress">Delegate used to report progress.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        public bool DiscoverChannels(bool inputs, bool outputs, bool fast, StatusReporter reporter, Action<int> progress)
        {
            // check if the result is already cached
            if ((this.InputChannels != null || !inputs) &&
                (this.OutputChannels != null || !outputs))
                return true;

            if (this.WorkDirectory == null || this.WorkDirectory.Exception != null)
                return false;

            // The format of this file is fixed.
            if (!this.channelsAreFinal)
            {
                // invalidate cache
                this.InputChannels = null;
                this.OutputChannels = null;
            }
            
            bool result;

            IClusterResidentObject wd = this.WorkDirectory;
            if (wd is FolderInCachedCluster)
            {
                wd = (wd as FolderInCachedCluster).OriginalFolder;
            }

            if (wd is UNCFile)
            {
                result = this.DiscoverOriginalInfoChannels(inputs, outputs, fast, reporter, progress);
            }
            else
            {
                result = false;
            }

            if (this.VertexIsCompleted)
                this.channelsAreFinal = true;
            return result;
        }

        /// <summary>
        /// If true the vertex is no longer running; some of its information can be cached.
        /// </summary>
        public bool VertexIsCompleted
        {
            get
            {
                switch (this.State)
                {
                    case VertexState.Cancelled:
                    case VertexState.Abandoned:
                    case VertexState.Failed:
                    case VertexState.Successful:
                    case VertexState.Invalidated:
                    case VertexState.Revoked:
                        return true;
                    default:
                        return false;
                }
            }
        }

        /// <summary>
        /// String representation of the executed vertex instance.
        /// </summary>
        /// <returns>A string briefly describing the executed vertex instance.</returns>
        public override string ToString()
        {
            return string.Format("Vertex {0}.{1} ({2}) status {3}", this.Number, this.Version, this.Name, this.State);
        }

        /// <summary>
        /// Replace the information in an executed vertex instance when a new vertex is created.
        /// This can only happen in some cases when cancelled vertex numbers are reused.
        /// </summary>
        /// <param name="name">New vertex name.</param>
        /// <param name="guid">New vertex guid.</param>
        // ReSharper disable once UnusedParameter.Global
        internal void Update(string name, string guid)
        {
            if (this.State != VertexState.Cancelled && this.State != VertexState.Abandoned)
                throw new CalypsoDryadException("Updating a non-cancelled/abandoned vertex");
            if (this.Name != name)
                throw new CalypsoDryadException("Vertex changed name");
            this.UniqueID = guid;
            this.SetState(VertexState.Created);
            // the stdoutfile is expected to change, so I don't invalidate the cache
        }

        /// <summary>
        /// Set the vertex state.
        /// </summary>
        /// <param name="state">New vertex state.</param>
        public void SetState(VertexState state)
        {
            this.State = state;
            bool cache = this.VertexIsCompleted;
            if (this.StdoutFile != null)
                this.StdoutFile.ShouldCacheLocally = cache;
            if (this.LogDirectory != null)
                this.LogDirectory.ShouldCacheLocally = cache;
            if (this.WorkDirectory != null)
                this.WorkDirectory.ShouldCacheLocally = cache;
        }

        /// <summary>
        /// A CSV header matching the AsCSV data.
        /// </summary>
        /// <returns>A string describing the CSV header for a vertex executed instance.</returns>
        public static string CSV_Header()
        {
            return "Name,Stage,Start,End,Running time,State,Data Read,Data Written,Version,Machine,Process ID";
        }

        /// <summary>
        /// CSV representation of the information about an executed vertex instance.
        /// </summary>
        /// <returns>The information in CSV format, matching the CSV_Header.</returns>
        public string AsCSV()
        {
            string start = this.Start != DateTime.MinValue ? this.Start.ToString("s") : "";
            string end = this.End != DateTime.MinValue ? this.End.ToString("s") : "";
            string running = this.RunningTime > TimeSpan.Zero ? this.RunningTime.ToString() : "";

            return string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}",
                this.Name, this.StageName, start, end, running, this.State, this.DataRead, this.DataWritten, this.Version, this.Machine, this.ProcessIdentifier);
        }
    }
}
