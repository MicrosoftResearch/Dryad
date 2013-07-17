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
using System.Text.RegularExpressions;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// Where is the JM process executed?
    /// </summary>
    internal enum ExecutionKind
    {
        /// <summary>
        /// The JM has been submitted to the job scheduler.
        /// </summary>
        JobScheduler,

        /// <summary>
        /// The JM is run on the local machine.
        /// </summary>
//        LocalJM,

        /// <summary>
        /// The query is run using local debug.
        /// </summary>
        LocalDebug
    }

    /// <summary>
    /// This class encapsulates the means to execute in the background a collection of queries.
    /// </summary>
    internal class JobExecutor
    {
        /// <summary>
        /// The jobSubmission is only used if we use a job scheduler.
        /// </summary>
        private IHpcLinqJobSubmission jobSubmission;
        /// <summary>
        /// The Process is used only if we don't use a job scheduler.
        /// </summary>
//        private Process dryadProc;
        /// <summary>
        /// Where is the query being executed?
        /// </summary>
        private ExecutionKind executionKind;
        /// <summary>
        /// Where is the query being executed?
        /// </summary>
        internal ExecutionKind ExecutionKind
        {
            get { return this.executionKind; }
        }
        /// <summary>
        /// Error message when job fails.
        /// </summary>
//        private string errorMsg;
        /// <summary>
        /// Keep status here when it no longer changes.
        /// </summary>
        private JobStatus currentStatus;
        private HpcLinqContext m_context;

        /// <summary>
        /// Create a new job executor object.
        /// </summary>
        public JobExecutor(HpcLinqContext context)
        {
            // use a new job submission object for each query
//            this.errorMsg = "";
            this.m_context = context;
            this.currentStatus = JobStatus.NotSubmitted;
            if (context.Runtime is HpcQueryRuntime)
            {
                YarnJobSubmission job = new YarnJobSubmission(context);
//                job.LocalJM = false;
                job.Initialize();
                this.executionKind = ExecutionKind.JobScheduler;
                this.jobSubmission = job;
            }
            else
            {
                throw new DryadLinqException(HpcLinqErrorCode.UnsupportedSchedulerType,
                                           String.Format(SR.UnsupportedSchedulerType, context.Runtime));
            }

#if REMOVE
            case SchedulerKind.LocalJM:
            {
                HpcJobSubmission job = new HpcJobSubmission(runtime);
                job.LocalJM = true;
                job.Initialize();
                this.executionKind = ExecutionKind.JobScheduler;
                this.jobSubmission = job;
                DryadLinq.SchedulerType = SchedulerKind.Hpc;
                break;
            }
#endif
        }

        /// <summary>
        /// Add a specified resource to the dryad computation.
        /// </summary>
        /// <param name="resource">Resource to add.</param>
        /// <returns>The pathname to the resource to use in the xml plan.</returns>
        public string AddResource(string resource)
        {
            switch (this.executionKind)
            {
                case ExecutionKind.JobScheduler:
                {
                    this.jobSubmission.AddLocalFile(resource);
                    FileInfo resourceInfo = new FileInfo(resource);
                    return resourceInfo.Name;
                }
#if REMOVE
                case ExecutionKind.LocalJM:
                {
                    return resource;
                }
#endif
                default:
                {
                    throw new DryadLinqException(HpcLinqErrorCode.UnsupportedExecutionKind,
                                               SR.UnsupportedExecutionKind);
                }
            }
        }

        /// <summary>
        /// Add a resource to a job.  Check whether there are clashes in resource names.
        /// </summary>
        /// <param name="jobSubmission">JobSubmission object which will hold the resources.</param>
        /// <param name="file">Pathname to file to add as a resource.</param>
        private void AddResource(IHpcLinqJobSubmission jobSubmission, string file)
        {
            // extract basename
            string basename = Path.GetFileName(file);
            this.jobSubmission.AddLocalFile(file);
        }

        /// <summary>
        /// Start executing the dryad job in the background.
        /// </summary>
        /// <param name="queryPlanPath">Full path to query plan XML file.</param>
        public void ExecuteAsync(string queryPlanPath)
        {
            switch (this.executionKind)
            {
                case ExecutionKind.JobScheduler:
                {
                    lock (this)
                    {
                        FileInfo xmlHostInfo = new FileInfo(StaticConfig.XmlHostPath);
                        
                        // Consturct the Graph Manager cmd line.
                        //string queryPlanFileName = Path.GetFileName(queryPlanPath);
                        this.jobSubmission.AddJobOption("cmdline", xmlHostInfo.Name + " " + queryPlanPath);
                                                
                        AddResource(this.jobSubmission, StaticConfig.XmlHostPath);
                        AddResource(this.jobSubmission, queryPlanPath);

                        // Add black and white list file, additional resources if passed as a command line
                        string[] args = StaticConfig.XmlExecHostArgs.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        for (int i = 0; i < args.Length; ++i)
                        {
                            string arg = args[i];
                            if (arg.Equals("-bw") || arg.Equals("-r"))
                            {
                                AddResource(this.jobSubmission, args[++i]);
                            }
                        }

                        this.jobSubmission.SubmitJob();
                        this.currentStatus = JobStatus.Waiting;
                    }
                    break;                    
                }
#if REMOVE
                case ExecutionKind.LocalJM:
                {
                    lock (this)
                    {
                        // Invoking Dryad as a separate process:
                        if (DryadLinq.APEnvironmentPath != null)
                        {
                            Environment.SetEnvironmentVariable("APENVIRONMENTPATH", DryadLinq.APEnvironmentPath);
                        }
                        if (DryadLinq.APConfigPath != null)
                        {
                            Environment.SetEnvironmentVariable("APCONFIGPATH", DryadLinq.APConfigPath);
                        }
                        ProcessStartInfo procStartInfo = new ProcessStartInfo();
                        procStartInfo.FileName = DryadLinq.XmlHostPath;
                        procStartInfo.Arguments = dryadProgram;
                        procStartInfo.RedirectStandardOutput = true;
                        procStartInfo.RedirectStandardError = false;
                        procStartInfo.UseShellExecute = false;

                        this.dryadProc = Process.Start(procStartInfo);
                        this.currentStatus = JobStatus.Running;
                    }
                    break;                    
                }
#endif
                default:
                {
                    throw new NotImplementedException();
                }
            }
        }

        /// <summary>
        /// Wait for job completion.
        /// </summary>
        /// <returns>The status of the job.</returns>
        public JobStatus WaitForCompletion()
        {
            if (this.Terminated())
            {
                return this.currentStatus;
            }

            JobStatus status;
            switch (this.executionKind)
            {
                case ExecutionKind.JobScheduler:
                {
                    int sleep = 3000;
                    int maxSleep = 20000;
                    int retries = 0;
                    while (true)
                    {
                        try
                        {
                            string msg = null;
                            switch (status = this.GetStatus())
                            {
                                case JobStatus.Success:
                                case JobStatus.Failure:
                                case JobStatus.Cancelled:
                                    return status;
                                case JobStatus.NotSubmitted:
                                    msg = "The job to create this table has not been submitted yet.  Waiting ...";
                                    break;
                                case JobStatus.Running:
                                    msg = "The job to create this table is still running. Waiting ...";
                                    break;
                                case JobStatus.Waiting:
                                    msg = "The job to create this table is still queued. Waiting ...";
                                    break;
                                default:
                                    throw new DryadLinqException(HpcLinqErrorCode.UnexpectedJobStatus,
                                                               String.Format(SR.UnexpectedJobStatus, status.ToString()));
                            }

                            retries = 0;
                            HpcClientSideLog.Add(msg);
                        }
                        catch (System.Net.WebException)
                        {
                            retries++;
                            sleep = maxSleep;
                            HpcClientSideLog.Add("Error contacting web server while querying job status. Waiting ...");
                        }
                        
                        if (retries > 5)
                        {
                            throw new DryadLinqException(HpcLinqErrorCode.JobStatusQueryError,
                                                       SR.JobStatusQueryError);
                        }
                        if (sleep < maxSleep)
                        {
                            sleep = Math.Min(maxSleep, (int)(sleep * 1.1));
                        }
                        System.Threading.Thread.Sleep(sleep);
                    }
                }
#if REMOVE
                case ExecutionKind.LocalJM:
                {
                    StreamReader dryadProcOut = this.dryadProc.StandardOutput;
                    StreamWriter stdout = null;
                    try
                    {
                        if (DryadLinq.LocalJMStdout != null)
                        {
                            stdout = new StreamWriter(DryadLinq.LocalJMStdout);
                        }
                        string outLine;
                        while ((outLine = dryadProcOut.ReadLine()) != null)
                        {      
                            if (DryadLinq.Verbose)
                            {
                                Console.WriteLine(outLine);
                            }
                            if (stdout != null)
                            {
                                stdout.WriteLine(outLine);
                            }
                        }
                    }
                    finally
                    {
                        if (stdout != null) stdout.Close();
                    }
                    
                    this.dryadProc.WaitForExit();
                    status = this.GetStatus();
                    this.dryadProc.Close();
                    return status;
                }
#endif
                default:
                {
                    throw new NotImplementedException();
                }
            }
        }

        /// <summary>
        /// True if the background execution has terminated.
        /// </summary>
        /// <returns></returns>
        public bool Terminated()
        {
            // First check whether the status is finalized
            switch (this.currentStatus)
            {
                case JobStatus.Cancelled:
                case JobStatus.Failure:
                case JobStatus.Success:
                case JobStatus.NotSubmitted:
                    // this status can't change any more
                    return true;
                case JobStatus.Running:
                case JobStatus.Waiting:
                    // re-evaluate status
                    return false;
                default:
                    throw new DryadLinqException(HpcLinqErrorCode.UnexpectedJobStatus,
                                               String.Format(SR.UnexpectedJobStatus, this.currentStatus.ToString()));
            }
        }

        /// <summary>
        /// Find out the status of the job.
        /// </summary>
        /// <returns>The job status.</returns>
        internal JobStatus GetStatus()
        {
            if (this.Terminated())
            {
                return this.currentStatus;
            }

            lock (this)
            {
                if (this.executionKind == ExecutionKind.JobScheduler)
                {
                    this.currentStatus = this.jobSubmission.GetStatus();
                }
#if REMOVE
                else if (this.executionKind == ExecutionKind.LocalJM)
                {
                    if (!dryadProc.HasExited)
                    {
                        this.currentStatus = JobStatus.Running;
                    }
                    else
                    {
                        if (this.dryadProc.ExitCode != 0)
                        {
                            this.currentStatus = JobStatus.Failure;
                            this.errorMsg = "HpcLinq graph manager process failed with exit code " + dryadProc.ExitCode.ToString();
                        }
                        else
                            this.currentStatus = JobStatus.Success;
                    }
                }
#endif
                else
                {
                    throw new DryadLinqException(HpcLinqErrorCode.UnsupportedExecutionKind,
                                               SR.UnsupportedExecutionKind);
                }
                return currentStatus;
            }
        }

        internal void SetStatus(JobStatus js)
        {
            this.currentStatus = js;
        }

        /// <summary>
        /// Cancel the job computation.
        /// </summary>
        /// <returns>The actual status of the job.  This may be 'Cancelled', but if the job has completed it may be 'Success' as well.</returns>
        internal JobStatus Cancel()
        {
            if (this.Terminated())
            {
                return currentStatus;
            }

            lock (this)
            {
                switch (this.executionKind)
                {
                    case ExecutionKind.JobScheduler:
                    {
                        this.currentStatus = this.jobSubmission.TerminateJob();
                        return this.currentStatus;
                    }
#if REMOVE
                    case ExecutionKind.LocalJM:
                    {
                        if (this.dryadProc == null)
                        {
                            return JobStatus.NotSubmitted;
                        }

                        if (!this.dryadProc.HasExited)
                        {
                            this.dryadProc.Kill();
                            this.errorMsg = "Job Manager was cancelled";
                            this.currentStatus = JobStatus.Cancelled;
                        }
                        return this.GetStatus();
                    }
#endif
                    default:
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.UnsupportedExecutionKind,
                                                   SR.UnsupportedExecutionKind);
                    }
                }
            }
        }

        /// <summary>
        /// For failed jobs this contains an error message.
        /// </summary>
        internal string ErrorMsg
        {
            get
            {
                switch (this.executionKind)
                {
                    case ExecutionKind.JobScheduler:
                    {
                        return this.jobSubmission.ErrorMsg;
                    }
#if REMOVE
                    case ExecutionKind.LocalJM:
                    {
                        return this.errorMsg;
                    }
#endif
                    default:
                    {
                        throw new DryadLinqException(HpcLinqErrorCode.UnsupportedExecutionKind,
                                                   SR.UnsupportedExecutionKind);
                    }
                }
            }
        }

        internal IHpcLinqJobSubmission JobSubmission
        {
            get { return this.jobSubmission; }
        }
    }
}
