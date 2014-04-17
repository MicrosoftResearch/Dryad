
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
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Research.JobObjectModel;
using Microsoft.Research.Tools;

namespace Microsoft.Research.DryadAnalysis
{
    /// <summary>
    /// The result of a decision (ternary booleans?)
    /// </summary>
    public enum Decision
    {
        /// <summary>
        /// Yes.
        /// </summary>
        Yes,
        /// <summary>
        /// No.
        /// </summary>
        No,
        /// <summary>
        /// Cannot say.
        /// </summary>
        Dontknow,
        /// <summary>
        /// Not applicable.
        /// </summary>
        NA
    };

    /// <summary>
    /// A message diagnosing a problem in a system.
    /// </summary>
    public class DiagnosisMessage
    {
        /// <summary>
        /// Categorizes diagnosis messages according to their importance.
        /// </summary>
        public enum Importance
        {
            /// <summary>
            /// Traces the decision flow.
            /// </summary>
            Tracing,
            /// <summary>
            /// An error occured during the diagnostic process.
            /// </summary>
            Error,
            /// <summary>
            /// Final diagnostic message.
            /// </summary>
            Final,
            /// <summary>
            /// This is a bug_ in Dryad/DryadLINQ.
            /// </summary>
            CoreBug,
        }

        /// <summary>
        /// Importance of the message.
        /// </summary>
        public Importance MessageImportance { get; protected set; }

        /// <summary>
        /// Message itself.
        /// </summary>
        public string Message { get; protected set; }
        /// <summary>
        /// Additional details about the message.
        /// </summary>
        public string Details { get; protected set; }

        /// <summary>
        /// Create a new diagnosis message.
        /// </summary>
        /// <param name="i">Message importance.</param>
        /// <param name="message">Message attached.</param>
        /// <param name="details">Additional details about the message.</param>
        public DiagnosisMessage(Importance i, string message, string details)
        {
            this.MessageImportance = i;
            this.Message = message;
            this.Details = details;
        }

        /// <summary>
        /// String representation of the diagnosis message.
        /// </summary>
        /// <returns>A string representation.</returns>
        public override string ToString()
        {
            return string.Format("{0} {1}", this.Message, this.Details);
        }
    }

    /// <summary>
    /// A diagnostic log is a sequence of messages.
    /// </summary>
    public class DiagnosisLog
    {
        /// <summary>
        /// List of messages in the log.
        /// </summary>
        private List<DiagnosisMessage> messages;
        /// <summary>
        /// Summary of job being diagnosed.
        /// </summary>
        public DryadLinqJobSummary Summary { get; protected set; }
        /// <summary>
        /// Job being diagnosed.
        /// </summary>
        public DryadLinqJobInfo Job { get; protected set; }

        /// <summary>
        /// Create a new diagnostic log.
        /// </summary>
        public DiagnosisLog(DryadLinqJobInfo job, DryadLinqJobSummary summary)
        {
            this.messages = new List<DiagnosisMessage>();
            this.Summary = summary;
            this.Job = job;
        }

        /// <summary>
        /// Add a new message to the log.
        /// </summary>
        /// <param name="msg">Message to add to log.</param>
        public void AddMessage(DiagnosisMessage msg)
        {
            this.messages.Add(msg);
        }

        /// <summary>
        /// The part of the log with high enough severity.
        /// </summary>
        /// <param name="cutoff">Do not show messages below this severity.</param>
        /// <returns>A string representation of all suitable messages in the log.</returns>
        public IEnumerable<string> Message(DiagnosisMessage.Importance cutoff)
        {
            bool coreBugFound = false;

            IEnumerable<DiagnosisMessage> suitable = this.messages.Where(m => m.MessageImportance >= cutoff);
            // remove duplicated messages
            Dictionary<string, Tuple<int, DiagnosisMessage>> repeats = new Dictionary<string,Tuple<int,DiagnosisMessage>>();
            foreach (DiagnosisMessage s in suitable) {
                if (s.MessageImportance == DiagnosisMessage.Importance.CoreBug)
                    coreBugFound = true;
                if (repeats.ContainsKey(s.Message))
                    repeats[s.Message] = Tuple.Create(repeats[s.Message].Item1+1, repeats[s.Message].Item2);
                else
                    repeats[s.Message] = new Tuple<int,DiagnosisMessage>(1, s);
            }

            foreach (var m in repeats) {
                var count = m.Value.Item1;
                var msg = m.Value.Item2.ToString();
                if (count > 1)
                    msg += " [repeated " + count + " times]";
                yield return msg;
            }

            if (coreBugFound)
            {
                yield return "This is a bug in the underlying system (Dryad/DryadLINQ/Quincy).  You can report this diagnosis to the DryadLINQ developers at drylnqin";
            }
        }

        /// <summary>
        /// Default string representation.
        /// </summary>
        /// <returns>A string representation of the log.</returns>
        public IEnumerable<string> Message()
        {
            // ReSharper disable once IntroduceOptionalParameters.Global
            return this.Message(DiagnosisMessage.Importance.Final);
        }

        /// <summary>
        /// String representation of the log contents.
        /// </summary>
        /// <returns>A big string containing each line of the log on a separate line.</returns>
        public override string ToString()
        {
            return string.Join("\n", this.Message().ToArray());
        }
    }

    /// <summary>
    /// Base class for failure diagnoses.
    /// </summary>
    public abstract class FailureDiagnosis
    {
        /// <summary>
        /// Log used to write the diagnosis messages.
        /// </summary>
        protected DiagnosisLog diagnosisLog;
        /// <summary>
        /// Summary of the job being diagnosed.
        /// </summary>
        public DryadLinqJobSummary Summary
        {
            get;
            protected set;
        }
        /// <summary>
        /// Cluster where the job resides.
        /// </summary>
        protected readonly ClusterConfiguration cluster;

        /// <summary>
        /// Job that owns the vertex.
        /// </summary>
        public DryadLinqJobInfo Job { get; protected set; }
        /// <summary>
        /// Communication manager.
        /// </summary>
        public CommManager Manager { get; protected set; }
        /// <summary>
        /// Plan of the job.
        /// </summary>
        public DryadJobStaticPlan StaticPlan { get; protected set; }
        /// <summary>
        /// Create a FailureDiagnosis object.
        /// </summary>
        /// <param name="job">Job being diagnosed.</param>
        /// <param name="plan">Static plan of the job.</param>
        /// <param name="manager">Communication manager.</param>
        protected FailureDiagnosis(DryadLinqJobInfo job, DryadJobStaticPlan plan, CommManager manager)
        {
            this.Job = job;
            this.StaticPlan = plan;
            this.Manager = manager;
            this.Summary = job.Summary;
            this.cluster = job.ClusterConfiguration;
        }

        /// <summary>
        /// Try to find the job information from cluster and summary.
        /// </summary>
        /// <param name="manager">Communication manager.</param>
        protected void FindJobInfo(CommManager manager)
        {
            DryadLinqJobInfo jobinfo = DryadLinqJobInfo.CreateDryadLinqJobInfo(this.cluster, this.Summary, true, manager);
            if (jobinfo == null)
            {
                manager.Status("Cannot collect information for " + Summary.ShortName() + " to diagnose", StatusKind.Error);
                return;
            }

            this.Job = jobinfo;
            this.StaticPlan = JobObjectModel.DryadJobStaticPlan.CreatePlan(jobinfo, manager);
        }

        /// <summary>
        /// Create a failure diagnosis when the job info is not yet known.
        /// </summary>
        /// <param name="config">Cluster where job resides.</param>
        /// <param name="summary">Job summary.</param>
        /// <param name="manager">Communication manager.</param>
        protected FailureDiagnosis(ClusterConfiguration config, DryadLinqJobSummary summary, CommManager manager)
        {
            this.cluster = config;
            this.Summary = summary;
            this.Manager = manager;
            this.FindJobInfo(manager);
        }

        /// <summary>
        /// Write a log message to the diagnosis log.
        /// </summary>
        /// <param name="importance">Message importance.</param>
        /// <param name="message">Message to write.</param>
        /// <param name="details">Additional message details.</param>
        protected void Log(DiagnosisMessage.Importance importance, string message, string details)
        {
            this.diagnosisLog.AddMessage(new DiagnosisMessage(importance, message, details));
        }
    }

    #region COMMON_DIAGNOSIS
    // This is diagnosis which is mostly Dryad dependent, so it is independent on the cluster platform.

    /// <summary>
    /// Diagnoses the failure of a vertex.
    /// </summary>
    public class VertexFailureDiagnosis : FailureDiagnosis
    {
        /// <summary>
        /// Vertex that is being diagnosed.
        /// </summary>
        public ExecutedVertexInstance Vertex { get; protected set; }
        /// <summary>
        /// Create a class to diagnose the problems of a vertex.
        /// </summary>
        /// <param name="vertex">Vertex to diagnose.</param>
        /// <param name="job">Job containing the vertex.</param>
        /// <param name="plan">Plan of the executed job.</param>
        /// <param name="manager">Communication manager.</param>
        protected VertexFailureDiagnosis(DryadLinqJobInfo job, DryadJobStaticPlan plan, ExecutedVertexInstance vertex, CommManager manager)
            : base(job, plan, manager)
        {
            this.Job = job;
            this.Vertex = vertex;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            this.stackTraceFile = "dryadLinqStackTrace.txt";
        }

        /// <summary>
        /// Create a VertexFailureDiagnosis of the appropriate type.
        /// </summary>
        /// <param name="vertex">Vertex to diagnose.</param>
        /// <param name="job">Job containing the vertex.</param>
        /// <param name="manager">Communication manager.</param>
        /// <returns>A subclass of VertexFailureDiagnosis.</returns>
        /// <param name="plan">Plan of the executed job.</param>
        public static VertexFailureDiagnosis CreateVertexFailureDiagnosis(DryadLinqJobInfo job, 
            DryadJobStaticPlan plan, 
            ExecutedVertexInstance vertex,
            CommManager manager)
        {
            ClusterConfiguration config = job.ClusterConfiguration;
            if (config is CacheClusterConfiguration)
                config = (config as CacheClusterConfiguration).ActualConfig(job.Summary);

  
            throw new InvalidOperationException("Config of type " + config.TypeOfCluster + " not handled");
        }

        /// <summary>
        /// The main function of the diagnosis.
        /// </summary>
        /// <param name="log">Log where explanation is written.</param>
        /// <returns>The decision of the diagnosis.</returns>
        public virtual Decision Diagnose(DiagnosisLog log)
        {
            throw new InvalidOperationException("Must override this function");
        }

        /// <summary>
        /// Diagnose a vertex.
        /// </summary>
        /// <returns>The log of the diagnostic.</returns>
        public DiagnosisLog Diagnose()
        {
            DiagnosisLog log = new DiagnosisLog(this.Job, this.Summary);
            log.AddMessage(new DiagnosisMessage(DiagnosisMessage.Importance.Final, "Diagnostic for " + this.VertexName, "Vertex state is " + this.Vertex.State));
            this.Diagnose(log);
            this.Manager.Status("Vertex diagnosis complete", StatusKind.OK);
            return log;
        }

        /// <summary>
        /// Return the vertex logs.
        /// </summary>
        /// <param name="errorLogs">If true return only the error logs.</param>
        /// <returns>An iterator over all log files.</returns>
        public virtual IEnumerable<IClusterResidentObject> Logs(bool errorLogs)
        {
            IClusterResidentObject logdir = this.Job.ClusterConfiguration.ProcessLogDirectory(this.Vertex.ProcessIdentifier, this.Vertex.VertexIsCompleted, this.Vertex.Machine, this.Job.Summary);
            string pattern = this.Job.ClusterConfiguration.VertexLogFilesPattern(errorLogs, this.Job.Summary);
            if (logdir.Exception != null)
                yield break;

            IEnumerable<IClusterResidentObject> logs = logdir.GetFilesAndFolders(pattern);
            foreach (var l in logs)
            {
                if (l.Exception == null)
                    yield return l;
            }
        }

        /// <summary>
        /// Detect whether the vertex had problems reading a particular channel.
        /// </summary>
        /// <returns>The channel that cannot be read, or null if that's not the problem.</returns>
        /// <param name="manager">Communication manager.</param>
        public virtual ChannelEndpointDescription ChannelReadFailure(CommManager manager)
        {
            List<string> stack = this.StackTrace().ToList();
            if (stack.Count == 0)
                return null;
            string firstLine = stack.First();
            Regex errorMsg = new Regex(@"(.*)Exception: (.*)ailed to read from input channel at port (\d+)");
            Match m = errorMsg.Match(firstLine);
            if (!m.Success)
                return null;

            int channelNo;
            bool success = int.TryParse(m.Groups[3].Value, out channelNo);
            if (!success)
                return null;
            this.Vertex.DiscoverChannels(true, false, true, manager);
            var channels = this.Vertex.InputChannels;
            if (channels == null)
                return null;
            if (channels.Count < channelNo)
            {
                this.Log(DiagnosisMessage.Importance.Error, "Could not discover channel " + channelNo, this.VertexName);
                return null;
            }
            return channels[channelNo];
        }

        /// <summary>
        /// Detect whether vertex terminates with a stack overflow.
        /// </summary>
        /// <returns>True if this seems likely.</returns>
        protected virtual Decision StackOverflow()
        {
            IClusterResidentObject stdout = this.Job.ClusterConfiguration.ProcessStdoutFile(this.Vertex.ProcessIdentifier, this.Vertex.VertexIsCompleted, this.Vertex.Machine, this.Job.Summary);
            if (stdout.Exception != null)
                return Decision.Dontknow;
            ISharedStreamReader sr = stdout.GetStream();
            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                if (line.Contains("StackOverflowException"))
                {
                    this.Log(DiagnosisMessage.Importance.Final, "Error found in vertex stderr:", line);
                    sr.Close();
                    return Decision.Yes;
                }
            }
            sr.Close();
            return Decision.Dontknow;
        }

        /// <summary>
        /// Try to diagnose whether there's a CLR mismatch error.
        /// </summary>
        /// <returns>True if this is the problem.</returns>
        protected bool CLRStartupProblems()
        {
            IClusterResidentObject stdout = this.Job.ClusterConfiguration.ProcessStdoutFile(this.Vertex.ProcessIdentifier, this.Vertex.VertexIsCompleted, this.Vertex.Machine, this.Job.Summary);
            if (stdout.Exception != null)
                return false;
            ISharedStreamReader sr = stdout.GetStream();
            // only look for the error in the first 10 lines
            for (int i = 0; i < 10; i++)
            {
                if (sr.EndOfStream)
                {
                    sr.Close();
                    return false;
                }
                string line = sr.ReadLine();
                if (line.Contains("Error code 2148734720 (0x80131700)"))
                {
                    this.Log(DiagnosisMessage.Importance.Final, "Error found in vertex stdout:", line);
                    sr.Close();
                    return true;
                }
            }
            sr.Close();
            return false;
        }
        
        /// <summary>
        /// Name of vertex that is being diagnosed.
        /// </summary>
        public string VertexName { get { return this.Vertex.Name; } }
        /// <summary>
        /// Name of the file containing the stack trace.
        /// </summary>
        public string stackTraceFile { get; protected set;  }

        /// <summary>
        /// The stack trace of the vertex at the time of the crash.
        /// </summary>
        /// <returns>The stack trace or an empty collection.</returns>
        public virtual IEnumerable<string> StackTrace()
        {
            IClusterResidentObject logdir = this.Job.ClusterConfiguration.ProcessWorkDirectory(this.Vertex.ProcessIdentifier, this.Vertex.VertexIsCompleted, this.Vertex.Machine, this.Job.Summary);
            IClusterResidentObject stackTrace = logdir.GetFile(this.stackTraceFile);
            ISharedStreamReader sr = stackTrace.GetStream();

            if (sr.Exception == null)
            {
                while (! sr.EndOfStream)
                    yield return sr.ReadLine();
            }
            else
                yield break;
        }

        /// <summary>
        /// Check whether this vertex is reading from a job input.
        /// </summary>
        /// <returns>The list of input stages this vertex is reading from.</returns>
        protected IEnumerable<DryadJobStaticPlan.Stage> VertexIsReadingFromJobInput()
        {
            if (this.StaticPlan == null)
                yield break;

            string stage = this.Vertex.StageName;
            DryadJobStaticPlan.Stage staticStage = this.StaticPlan.GetStageByName(stage);
            if (staticStage == null)
                yield break;

            foreach (DryadJobStaticPlan.Connection connection in this.StaticPlan.GetStageConnections(staticStage, true))
            {
                var input = connection.From;
                if (input.IsInput)
                    yield return input;
            }
        }

        /// <summary>
        /// Detect whether a problem with incorrect serialization may be the reason for job failure.
        /// </summary>
        /// <returns>Yes if serialization is the issue.</returns>
        protected virtual Decision SerializationError()
        {
            // two things must have happened: 
            // - the vertex must be reading from an input file
            // - the vertex has failed with an error during reading
            var inputStages = this.VertexIsReadingFromJobInput().Select(s => s.Uri).ToArray();
            if (inputStages.Count() == 0)
                return Decision.Dontknow;

            Decision decision = this.VertexFailedWhenReading();
            if (decision != Decision.Yes)
                return decision;

            this.Log(DiagnosisMessage.Importance.Final,
                   "A vertex failed with an error that may be indicative of incorrect data serialization.",
                   "Make sure that the program uses the correct data type for the used input files (and also it is using the proper combination of serialization attributes)."
                   );
            this.Log(DiagnosisMessage.Importance.Final,
                "The failed vertex is reading from the following input(s): ",
                    string.Join(",", inputStages));

            return decision;
        }

        /// <summary>
        /// Yes if this vertex had a read error.
        /// </summary>
        /// <returns>A decision.</returns>
        protected virtual Decision VertexFailedWhenReading()
        {
            // Look for a DryadRecordReader string on the stack trace
            foreach (string s in this.StackTrace())
            {
                if (s.Contains("DryadRecordReader"))
                    return Decision.Yes;
            }

            return Decision.Dontknow;
        }

        /// <summary>
        /// If true the vertex died with an assertion failure.
        /// </summary>
        public bool DiedWithAssertion { get; protected set; }
    }

    /// <summary>
    /// Diagnose failures in a job.
    /// </summary>
    public abstract class JobFailureDiagnosis : FailureDiagnosis
    {
        /// <summary>
        /// Job manager vertex.
        /// </summary>
        protected readonly ExecutedVertexInstance jobManager;

        /// <summary>
        /// Create a class to diagnose the problems of a job.
        /// </summary>
        /// <param name="job">Job to diagnose.</param>
        /// <param name="plan">Plan of the diagnosed job.</param>
        /// <param name="manager">Communication manager.</param>
        protected JobFailureDiagnosis(DryadLinqJobInfo job, DryadJobStaticPlan plan, CommManager manager)
            : base(job, plan, manager)
        {
            this.diagnosisLog = new DiagnosisLog(job, job.Summary);
            this.jobManager = this.Job.ManagerVertex;
        }

        /// <summary>
        /// Create a class to diagnose the problems of a job.
        /// </summary>
        /// <param name="config">Cluster where job resides.</param>
        /// <param name="manager">Communication manager.</param>
        /// <param name="summary">Job summary.</param>
        protected JobFailureDiagnosis(ClusterConfiguration config, DryadLinqJobSummary summary, CommManager manager)
            : base(config, summary, manager)
        {
            this.diagnosisLog = new DiagnosisLog(this.Job, summary);
            if (this.Job != null)
                this.jobManager = this.Job.ManagerVertex;
        }

        /// <summary>
        /// Decide whether the job has finished executing.
        /// </summary>
        /// <returns>A decision.</returns>
        protected Decision IsJobFinished()
        {
            bool dec = ClusterJobInformation.JobIsFinished(this.Summary.Status);
            return dec ? Decision.Yes : Decision.No;
        }

        /// <summary>
        /// Decide whether the job has failed.
        /// </summary>
        /// <returns>A decision.</returns>
        protected Decision IsJobFailed()
        {
            switch (this.Summary.Status)
            {
                case ClusterJobInformation.ClusterJobStatus.Failed:
                    return Decision.Yes;
                case ClusterJobInformation.ClusterJobStatus.Cancelled:
                case ClusterJobInformation.ClusterJobStatus.Succeeded:
                    return Decision.No;
                case ClusterJobInformation.ClusterJobStatus.Running:
                // job may still fail.
                case ClusterJobInformation.ClusterJobStatus.Unknown:
                    return Decision.Dontknow;
                default:
                    throw new InvalidDataException("Invalid job status " + this.Summary.Status);
            }
        }

        /// <summary>
        /// Main entry point: diagnose the failures of a job.
        /// </summary>
        /// <returns>The log containing the diagnosis result.</returns>
        public virtual DiagnosisLog Diagnose()
        {
            throw new InvalidOperationException("Must override this function");
        }

        /// <summary>
        /// Discover whether the failure is caused by the inability to parse the XML plan.
        /// </summary>
        /// <returns>The decision.</returns>
        public Decision XmlPlanParseError()
        {
            if (this.jobManager == null)
            {
                this.Log(DiagnosisMessage.Importance.Tracing, "Could not find job manager vertex information", "");
                return Decision.Dontknow;
            }

            IClusterResidentObject jmstdout = this.jobManager.StdoutFile;
            if (jmstdout.Exception != null)
            {
                this.Log(DiagnosisMessage.Importance.Tracing, "Could not find job manager standard output", "");
                return Decision.Dontknow;
            }

            ISharedStreamReader sr = jmstdout.GetStream();
            if (sr.Exception != null)
            {
                this.Log(DiagnosisMessage.Importance.Tracing, "Could not read job manager standard output", sr.Exception.Message);
                return Decision.Dontknow;
            }
            string firstline = sr.ReadLine();
            if (sr.EndOfStream || firstline == null)
            {
                sr.Close();
                return Decision.No;
            }
            sr.Close();

            if (firstline.Contains("Error parsing input XML file"))
            {
                this.Log(DiagnosisMessage.Importance.Final, "The job manager cannot parse the XML plan file.\n",
                        "This means probably that the version of LinqToDryad.dll that you are using does not match the XmlExecHost.exe file from your drop.");
                return Decision.Yes;
            }
            return Decision.No;
        }

        /// <summary>
        /// Find if a vertex has had many instances failed.
        /// </summary>
        /// <returns>The vertex that failed many times.</returns>
        protected ExecutedVertexInstance LookForRepeatedVertexFailures()
        {
            IEnumerable<ExecutedVertexInstance> failures =
                this.Job.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Failed).
                Where(v => !v.IsManager).
                ToList();
            if (failures.Count() == 0)
                return null;
            var mostFailed = failures.GroupBy(v => v.Name).OrderBy(g => -g.Count()).First();
            if (mostFailed.Count() > 3)
                return mostFailed.First();
            return null;
        }

        /// <summary>
        /// Find if a vertex has had many instances failed.
        /// </summary>
        /// <returns>The vertex that failed many times.</returns>
        protected ExecutedVertexInstance LookForManyVertexFailures()
        {
            IEnumerable<ExecutedVertexInstance> failures =
                this.Job.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Failed).
                Where(v => !v.IsManager).
                ToList();
            if (failures.Count() < 5)
                return null;
            var mostFailed = failures.GroupBy(v => v.Name).OrderBy(g => -g.Count()).First();
            return mostFailed.First();
        }

        /// <summary>
        /// Find multiple failures on the same machine.
        /// </summary>
        /// <returns>Yes if there are some.</returns>
        protected Decision LookForCorrelatedMachineFailures()
        {
            // if we have more than this many failures we start to worry
            const int maxFailures = 5;
            IEnumerable<ExecutedVertexInstance> failures =
                this.Job.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Failed).
                Where(v => !v.IsManager).
                ToList();
            int totalFailures = failures.Count();
            if (totalFailures < maxFailures)
                return Decision.No;

            var mostFailures = failures.GroupBy(v => v.Machine).OrderBy(g => -g.Count()).First();
            string failMachine = mostFailures.Key;
            if (mostFailures.Count() > totalFailures / 3 || mostFailures.Count() > 4)
            {
                this.Log(DiagnosisMessage.Importance.Final,
                    "There are " + mostFailures.Count() + " failures on machine " + failMachine,
                    "Total number of failures is " + totalFailures);
                return Decision.Yes;
            }

            return Decision.Dontknow;
        }

        /// <summary>
        /// Check to see whether a vertex has failed deterministically too many times.
        /// </summary>
        /// <returns>Identify of the failed vertex, or null if no such vertex exists.</returns>
        protected virtual ExecutedVertexInstance DeterministicVertexFailure()
        {
            string abortmsg = this.Job.AbortingMsg;
            if (abortmsg == null)
                return null;

            // ABORTING: Vertex failed too many times. Vertex 2 (OrderBy__0) number of failed executions 6
            Regex manyFaileRegex = new Regex(@"Vertex failed too many times. Vertex (\d+) \((.*)\) number of failed executions (\d+)");
            Match m = manyFaileRegex.Match(this.Job.AbortingMsg);
            if (!m.Success)
                return null;

            string name = m.Groups[2].Value;
            string failures = m.Groups[3].Value;
            this.Log(DiagnosisMessage.Importance.Final, string.Format("Job was aborted because vertex {0} failed {1} times", name, failures), "");

            IEnumerable<ExecutedVertexInstance> failed = this.Job.Vertices.Where(vi => vi.Name == name && vi.State == ExecutedVertexInstance.VertexState.Failed).ToList();
            if (failed.Count() == 0)
            {
                this.Log(DiagnosisMessage.Importance.Error, "Cannot find information about failed vertex", name);
                return null;
            }
            return failed.First();
        }

        /// <summary>
        /// Yes if the job dies because a vertex fails too many times to read the main job input.
        /// </summary>
        /// <returns>A decision indicating whether an input cannot be read.</returns>
        protected virtual Decision DeterministicInputFailure()
        {
            if (string.IsNullOrEmpty(this.Job.AbortingMsg))
                return Decision.No;
            if (this.Job.AbortingMsg.Contains("read failures"))
            {
                this.Log(DiagnosisMessage.Importance.Final, "Job cannot read some input data", this.Job.AbortingMsg);
                return Decision.Yes;
            }

            return Decision.Dontknow;
        }

        /// <summary>
        /// Create a suitable Job Failure diagnosis object for the job being analyzed.
        /// </summary>
        /// <param name="job">Job to diagnose.</param>
        /// <param name="manager">Communication manager.</param>
        /// <returns>A subclass of JobFailureDiagnosis with the type appropriate for the job.</returns>
        /// <param name="plan">Plan of the job being diagnosed.</param>
        public static JobFailureDiagnosis CreateJobFailureDiagnosis(DryadLinqJobInfo job, DryadJobStaticPlan plan, CommManager manager)
        {
            ClusterConfiguration config = job.ClusterConfiguration;
            if (config is CacheClusterConfiguration)
                config = (config as CacheClusterConfiguration).ActualConfig(job.Summary);


            throw new InvalidOperationException("Configuration of type " + config.TypeOfCluster + " not supported for diagnosis");
        }

        /// <summary>
        /// Create a suitable Job Failure diagnosis object for the job being analyzed.
        /// </summary>
        /// <param name="summary">Job to diagnose.</param>
        /// <param name="config">Cluster where job resides.</param>
        /// <param name="manager">Communication manager.</param>
        /// <returns>A subclass of JobFailureDiagnosis with the type appropriate for the job.</returns>
        public static JobFailureDiagnosis CreateJobFailureDiagnosis(ClusterConfiguration config, DryadLinqJobSummary summary, CommManager manager)
        {
            if (config is CacheClusterConfiguration)
                config = (config as CacheClusterConfiguration).ActualConfig(summary);

            throw new InvalidOperationException("Configuration of type " + config.TypeOfCluster + " not supported for diagnosis");
        }

        /// <summary>
        /// Look to see whether the vertices failed reading from some common set of machines.
        /// This is incomplete: e.g., it does not work for tidyfs streams.
        /// </summary>
        /// <returns>Yes if there were correlated failures.</returns>
        /// <param name="manager">Communication manager.</param>
        protected Decision LookForCorrelatedReadFailures(CommManager manager)
        {
            // if we have more than this many failures we start to worry
            const int maxFailures = 5;
            IEnumerable<ExecutedVertexInstance> failures =
                this.Job.Vertices.Where(v => v.State == ExecutedVertexInstance.VertexState.Failed).
                Where(v => !v.IsManager).
                ToList();
            int totalFailures = failures.Count();
            if (totalFailures < maxFailures)
                return Decision.No;

            List<ChannelEndpointDescription> channelsFailed = new List<ChannelEndpointDescription>();
            int verticesDone = 0;
            foreach (ExecutedVertexInstance v in failures)
            {
                var crf = VertexFailureDiagnosis.CreateVertexFailureDiagnosis(this.Job, this.StaticPlan, v, manager).ChannelReadFailure(manager);
                if (crf != null)
                {
                    channelsFailed.Add(crf);
                }
                verticesDone++;
                manager.Progress(verticesDone * 100 / totalFailures);
            }
            if (channelsFailed.Count() < maxFailures)
                return Decision.No;
            this.Log(DiagnosisMessage.Importance.Final, "There are " + channelsFailed.Count() + " read failures in the job", "");
            var files = channelsFailed.Where(ced => ced.UriType == "file").ToList();
            if (files.Count() == 0)
            {
                this.Log(DiagnosisMessage.Importance.Final, "All channels with failures are distributed files", "No further information is available");
                return Decision.Dontknow;
            }

            Decision result = Decision.Dontknow;
            var machines = files.Select(f => new UNCPathname(f.LocalPath).Machine).GroupBy(w => w).ToList();
            foreach (var m in machines)
            {
                int failuresOnM = m.Count();
                if (failuresOnM > 3)
                {
                    this.Log(DiagnosisMessage.Importance.Final, "There are " + failuresOnM + " read failures reading from machine", m.Key);
                    result = Decision.Yes;
                }
            }

            return result;
        }
    }
#endregion

    #region COSMOS_DIAGNOSIS
            #region JOB_STILL_RUNNING
            #endregion
#endregion
    #region SCOPE_DIAGNOSIS
            #region JOB_STILL_RUNNING
            #endregion
    #endregion

    #region HPC_DIAGNOSIS
            #region JOB_STILL_RUNNING
            #endregion
    #endregion

    #region L2H_JOB_DIAGNOSIS
            #region JOB_STILL_RUNNING
            #endregion
            #region HPC_ERRORS
            #endregion
#endregion
}
