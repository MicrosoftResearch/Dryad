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

//
// � Microsoft Corporation.  All rights reserved.
//
#if REMOVE_FOR_YARN
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Security.Principal;
using System.Text;
using System.Xml;
using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;
using Microsoft.Hpc.Dryad;
using Microsoft.Research.DryadLinq.Internal;
using System.Collections.Specialized;

namespace Microsoft.Research.DryadLinq
{
    internal class HpcJobSubmission : IHpcLinqJobSubmission
    {
        private HpcLinqContext m_context;
        private DryadJobSubmission m_job;
        private JobStatus m_status;

        internal void Initialize()
        {
            this.m_job.FriendlyName = m_context.Configuration.JobFriendlyName;

            //  if the user specified MinNodes and it is less than 2, return an error. Otherwise let job run with job template which 
            //      must specify a value of 2 or higher
            if (m_context.Configuration.JobMinNodes.HasValue && m_context.Configuration.JobMinNodes < 2)
            {
                throw new HpcLinqException(HpcLinqErrorCode.HpcLinqJobMinMustBe2OrMore,
                                           SR.HpcLinqJobMinMustBe2OrMore);
            }

            this.m_job.DryadJobMinNodes = m_context.Configuration.JobMinNodes;
            this.m_job.DryadJobMaxNodes = m_context.Configuration.JobMaxNodes;
            this.m_job.DryadNodeGroup = m_context.Configuration.NodeGroup;
            this.m_job.DryadUserName = m_context.Configuration.JobUsername;
            this.m_job.DryadPassword = m_context.Configuration.JobPassword;
            this.m_job.DryadRuntime = m_context.Configuration.JobRuntimeLimit;
            this.m_job.EnableSpeculativeDuplication = m_context.Configuration.EnableSpeculativeDuplication;
            this.m_job.RuntimeTraceLevel = (int)m_context.Configuration.RuntimeTraceLevel;
            this.m_job.GraphManagerNode = m_context.Configuration.GraphManagerNode;

            System.Collections.Specialized.NameValueCollection collection = new System.Collections.Specialized.NameValueCollection();
            
            foreach (var keyValuePair in m_context.Configuration.JobEnvironmentVariables)
            {
                collection.Add(keyValuePair.Key, keyValuePair.Value);
            }

            this.m_job.JobEnvironmentVariables = collection;
        }

        internal bool LocalJM
        {
            get
            {
                return m_job.Type == DryadJobSubmission.JobType.Local;
            }
            set
            {
                if (value == true)
                {
                    m_job.Type = DryadJobSubmission.JobType.Local;
                }
                else
                {
                    m_job.Type = DryadJobSubmission.JobType.Cluster;
                }
            }
        }

        internal string CommandLine
        {
            get
            {
                return m_job.CommandLine;
            }
            set
            {
                m_job.CommandLine = value;
            }
        }

        public string ErrorMsg
        {
            get
            {
                return m_job.ErrorMessage;
            }
            private set
            {
                m_job.ErrorMessage = value;
            }
        }

        internal HpcJobSubmission(HpcLinqContext context)
        {
            this.m_context = context;
            this.m_status = JobStatus.NotSubmitted;

            //@@TODO[P0] pass the runtime to the DryadJobSubmission so that it can use the scheduler instance.
            //@@TODO: Merge DryadJobSubmission into Ms.Hpc.Linq. Until then make sure Context is not disposed before DryadJobSubmission.
            this.m_job = new DryadJobSubmission(m_context.GetIScheduler());
        }

        public void AddJobOption(string fieldName, string fieldVal)
        {
            if (fieldName == "cmdline")
            {
                m_job.CommandLine = fieldVal;
            }
            else
            {
                throw new HpcLinqException(HpcLinqErrorCode.JobOptionNotImplemented,
                                           String.Format(SR.JobOptionNotImplemented, fieldName, fieldVal));
            }
        }

        public void AddLocalFile(string fileName)
        {
            m_job.AddFileToJob(fileName);
        }

        public void AddRemoteFile(string fileName)
        {
            string msg = String.Format("HpcJobSubmission.AddRemoteFile({0}) not implemented", fileName);
        }

        public JobStatus GetStatus()
        {
            if (this.m_status == JobStatus.Success ||
                this.m_status == JobStatus.Failure )
            {
                return this.m_status;
            }

            if (this.m_job == null)
            {
                return JobStatus.NotSubmitted;
            }

            switch (this.m_job.State)
            {
                case JobState.ExternalValidation:
                case JobState.Queued:
                case JobState.Submitted:
                case JobState.Validating:
                {
                    this.m_status = JobStatus.Waiting;
                    break;
                }
                case JobState.Configuring:
                case JobState.Running:
                case JobState.Canceling:
                case JobState.Finishing:
                {
                    this.m_status = JobStatus.Running;
                    break;
                }
                case JobState.Failed:
                    // a job only fails if the job manager fails.
                {
                    ISchedulerCollection tasks = this.m_job.Job.GetTaskList(null, null, false);
                    if (tasks.Count < 1)
                    {
                        this.ErrorMsg = this.m_job.ErrorMessage;
                        this.m_status = JobStatus.Failure;
                    }
                    else
                    {
                        ISchedulerTask jm = tasks[0] as ISchedulerTask;
                        switch (jm.State)
                        {
                            case TaskState.Finished:
                                this.m_status = JobStatus.Success;
                                break;
                            default:
                                this.m_status = JobStatus.Failure;
                                this.ErrorMsg = "JM error: " + jm.ErrorMessage;
                                break;
                        }
                    }
                    break;
                }
                case JobState.Canceled:
                {
                    this.ErrorMsg = this.m_job.ErrorMessage;
                    this.m_status = JobStatus.Failure;
                    break;
                }    
                case JobState.Finished:
                {
                    this.m_status = JobStatus.Success;
                    break;
                }
            }

            return this.m_status;
        }

        public void SubmitJob()
        {
            // Verify that the head node is set
            if (m_context.Configuration.HeadNode == null)
            {
                throw new HpcLinqException(HpcLinqErrorCode.ClusterNameMustBeSpecified,
                                           SR.ClusterNameMustBeSpecified);
            }
            
            try
            {
                this.m_job.SubmitJob();
            }
            catch (Exception e)
            {
                throw new HpcLinqException(HpcLinqErrorCode.SubmissionFailure,
                                           String.Format(SR.SubmissionFailure, m_context.Configuration.HeadNode), e);
            }
        }

        public JobStatus TerminateJob()
        {
            JobStatus status = GetStatus();
            switch (status)
            {
                case JobStatus.Failure:
                case JobStatus.NotSubmitted:
                case JobStatus.Success:
                case JobStatus.Cancelled:
                    // Nothing to do.
                    return status;
                default:
                    break;
            }

            this.m_job.CancelJob();
            return JobStatus.Cancelled;
        }

        public int GetJobId()
        {
            if (m_job == null || m_job.Job == null)
            {
                throw new InvalidOperationException("(internal) GetDryadJobInfo called when no job is available");
            }
            return m_job.Job.Id;
        }
    }
}
#else
namespace Microsoft.Research.DryadLinq
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Xml.Linq;
    using System.Linq;
    using System.Text;

    internal class YarnJobSubmission : IHpcLinqJobSubmission
    {
        private HpcLinqContext m_context;
        private JobStatus m_status;
        private string m_applicationId;
        private WebClient m_wc;
        private string m_cmdLine;
        private string m_queryPlan;
        private string m_errorMsg;
        

        public YarnJobSubmission(HpcLinqContext context)
        {
            m_context = context;
            m_status = JobStatus.NotSubmitted;
            m_wc = new WebClient();
        }

        public void AddJobOption(string fieldName, string fieldVal)
        {
            if(fieldName == "cmdline")
            {
                m_cmdLine = fieldVal;
                var fields = m_cmdLine.Split();
                m_queryPlan = fields[fields.Length - 1].Trim();
                Console.WriteLine("QueryPlan: {0}", m_queryPlan);
            }
        }

        public void AddLocalFile(string fileName)
        {
            // do nothing for now
        }

        public void AddRemoteFile(string fileName)
        {
            throw new System.NotImplementedException();
        }

        public string ErrorMsg
        {
            get 
            {
                if (!String.IsNullOrEmpty(m_errorMsg))
                {
                    return m_errorMsg;
                }
                else
                {
                    return "Unknown error running YARN query.";
                }
            }
        }

        public JobStatus GetStatus()
        {
            if (m_status == JobStatus.Waiting || m_status == JobStatus.Running)
            {
                m_wc.Headers.Add("Accept", "application/xml");
                var xmlData = m_wc.DownloadString(GetRestServiceUri());
                ProcessXmlData(xmlData);
            }
            return m_status;
        }

        private string BuildExpandedClasspath(HpcLinqConfiguration config)
        {
            String classPathString = System.Environment.GetEnvironmentVariable("classpath");
            //Console.WriteLine(classPathString);

            var fields = classPathString.Split(';');

            StringBuilder sb = new StringBuilder(16384);

            var jarFiles = Directory.GetFiles(config.DryadHomeDirectory, "*.jar", System.IO.SearchOption.TopDirectoryOnly);
            foreach (String file in jarFiles)
            {
                //Console.WriteLine("\t{0}", file);
                sb.Append(file);
                sb.Append(";");
            }

            foreach (String field in fields)
            {
                //Console.WriteLine(field);
                if (!field.EndsWith("*"))
                {
                    sb.Append(field);
                    sb.Append(";");
                }
                else
                {
                    var dirField = field.Substring(0, field.Length - 1); // trim the trailing *
                    jarFiles = Directory.GetFiles(dirField, "*.jar", System.IO.SearchOption.TopDirectoryOnly);
                    foreach (String file in jarFiles)
                    {
                        //Console.WriteLine(file);
                        sb.Append(file);
                        sb.Append(";");
                    }
                }
            }
            return sb.ToString();
        }

        public void SubmitJob()
        {
            // find the xml file, then invoke the java submission process
            ProcessStartInfo psi = new ProcessStartInfo();
            
            psi.FileName = Path.Combine(m_context.Configuration.YarnHomeDirectory, "bin", "yarn.cmd");
            string jarPath = Path.Combine(m_context.Configuration.DryadHomeDirectory, "DryadYarnBridge.jar");
            psi.Arguments = string.Format(@"jar {0} {1}", jarPath, m_queryPlan);
            psi.EnvironmentVariables.Add("JNI_CLASSPATH", BuildExpandedClasspath(m_context.Configuration));

            if (!psi.EnvironmentVariables.ContainsKey("DRYAD_HOME"))  
            {
                //Console.WriteLine("Adding DRYAD_HOME env variable");
                psi.EnvironmentVariables.Add("DRYAD_HOME", m_context.Configuration.DryadHomeDirectory);
            }

            psi.UseShellExecute = false;
            psi.RedirectStandardOutput = true;
            var process = Process.Start(psi);
            
            var procOutput = process.StandardOutput;
            process.WaitForExit();
            // the java submission process will return the application id as the last line
            while (!procOutput.EndOfStream)
            {
                m_applicationId = procOutput.ReadLine();
            }
            Console.WriteLine("Application ID: {0}", m_applicationId);
            m_status = JobStatus.Waiting;
        }

        public JobStatus TerminateJob()
        {
            throw new System.NotImplementedException();
        }

        public int GetJobId()
        {
            int jobId = -1;
            if (m_status != JobStatus.NotSubmitted)
            {
                var appNumberString = m_applicationId.Substring(m_applicationId.LastIndexOf('_') + 1);
                return int.Parse(appNumberString);
            }
            return jobId;
        }

        internal void Initialize()
        {
            // nothing needed for now
        }

        internal Uri GetRestServiceUri()
        {
            UriBuilder builder = new UriBuilder();
            builder.Host = m_context.Configuration.HeadNode;
            builder.Port = m_context.Configuration.HdfsNameNodeHttpPort; 
            builder.Path = "/ws/v1/cluster/apps/" + m_applicationId;
            return builder.Uri;
        }

        private void ProcessXmlData(string xmlData)
        {
            // for now, just pull state and finalStatus out of xml response

            //State: The application state according to the ResourceManager - 
            //valid values are: NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
            //
            //finalStatus: The final status of the application if finished - 
            //reported by the application itself - valid values are: UNDEFINED, SUCCEEDED, FAILED, KILLED

            XDocument xdoc = XDocument.Parse(xmlData);
            var stateString = xdoc.Descendants("state").Single().Value;
            
            switch (stateString)
            {
                case "NEW":
                    m_status = JobStatus.NotSubmitted;
                    break;
                case "SUBMITTED":
                case "ACCEPTED":
                    m_status = JobStatus.Waiting;
                    break;
                case "RUNNING":
                    m_status = JobStatus.Running;
                    break;
                case "FINISHED":
                    var finalStatusString = xdoc.Descendants("finalStatus").Single().Value;
                    switch (finalStatusString) {
                        case "UNDEFINED":
                            m_status = JobStatus.Success;
                            break;
                        case "SUCCEEDED": 
                            m_status = JobStatus.Success;
                            break;
                        case "FAILED": 
                            m_status = JobStatus.Failure;
                            break;
                        case "KILLED":
                            m_status = JobStatus.Cancelled;
                            break;
                        default:
                            throw new DryadLinqException("Unexpected finalStatus from Resource Manager");
                    }
                    break;
                case "FAILED":
                    m_status = JobStatus.Failure;
                    if (String.IsNullOrEmpty(m_errorMsg))
                    {
                        m_errorMsg = xdoc.Descendants("diagnostics").Single().Value.Trim();
                    }
                    break;
                case "KILLED":
                    m_status = JobStatus.Cancelled;
                    break;
                default:
                    throw new DryadLinqException("Unexpected status from Resource Manager");
            }
        }

    }
}
#endif
