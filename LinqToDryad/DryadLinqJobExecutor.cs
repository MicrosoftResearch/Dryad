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
    /// This class encapsulates the means to execute in the background a collection of queries.
    /// </summary>
    internal class DryadLinqJobExecutor
    {
        /// <summary>
        /// The jobSubmission is only used if we use a job scheduler.
        /// </summary>
        private IDryadLinqJobSubmission m_jobSubmission;

        /// <summary>
        /// Keep status here when it no longer changes.
        /// </summary>
        private JobStatus m_currentStatus;
        private DryadLinqContext m_context;

        /// <summary>
        /// Create a new job executor object.
        /// </summary>
        public DryadLinqJobExecutor(DryadLinqContext context)
        {
            // use a new job submission object for each query
            this.m_context = context;
            this.m_currentStatus = JobStatus.NotSubmitted;
            if (context.PlatformKind == PlatformKind.LOCAL)
            {
                this.m_jobSubmission = new LocalJobSubmission(context);
            }
            else
            {
                this.m_jobSubmission = new YarnJobSubmission(
                    context, context.Cluster.MakeInternalClusterUri("tmp", "staging"));
            }
        }

        /// <summary>
        /// Add a specified resource to the dryad computation.
        /// </summary>
        /// <param name="resource">Resource to add.</param>
        /// <returns>The pathname to the resource to use in the xml plan.</returns>
        public string AddResource(string resource)
        {
            this.m_jobSubmission.AddLocalFile(resource);
            FileInfo resourceInfo = new FileInfo(resource);
            return resourceInfo.Name;
        }

        /// <summary>
        /// Add a resource to a job.  Check whether there are clashes in resource names.
        /// </summary>
        /// <param name="jobSubmission">JobSubmission object which will hold the resources.</param>
        /// <param name="file">Pathname to file to add as a resource.</param>
        private void AddResource(IDryadLinqJobSubmission jobSubmission, string file)
        {
            this.m_jobSubmission.AddLocalFile(file);
        }

        /// <summary>
        /// Start executing the dryad job in the background.
        /// </summary>
        /// <param name="queryPlanPath">Full path to query plan XML file.</param>
        public void ExecuteAsync(string queryPlanPath)
        {
            lock (this)
            {
                // Construct the Graph Manager cmd line.
                // string queryPlanFileName = Path.GetFileName(queryPlanPath);
                this.m_jobSubmission.AddJobOption("cmdline", "Microsoft.Research.Dryad.GraphManager.exe" + " " + queryPlanPath);
                
                AddResource(this.m_jobSubmission, queryPlanPath);

                // Add black and white list file, additional resources if passed as a command line
                string[] args = StaticConfig.XmlExecHostArgs.Split(
                                        new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < args.Length; ++i)
                {
                    string arg = args[i];
                    if (arg.Equals("-bw") || arg.Equals("-r"))
                    {
                        AddResource(this.m_jobSubmission, args[++i]);
                    }
                }

                this.m_jobSubmission.SubmitJob();
                this.m_currentStatus = JobStatus.Waiting;
            }
        }

        /// <summary>
        /// Wait for job completion.
        /// </summary>
        /// <returns>The status of the job.</returns>
        public JobStatus WaitForCompletion()
        {
            return this.m_jobSubmission.WaitForCompletion();
        }

        /// <summary>
        /// True if the background execution has terminated.
        /// </summary>
        /// <returns>true iff the job has terminated</returns>
        public bool Terminated()
        {
            // First check whether the status is finalized
            switch (this.m_currentStatus)
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
                    throw new DryadLinqException(DryadLinqErrorCode.UnexpectedJobStatus,
                                                 String.Format(SR.UnexpectedJobStatus, this.m_currentStatus.ToString()));
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
                return this.m_currentStatus;
            }

            lock (this)
            {
                this.m_currentStatus = this.m_jobSubmission.GetStatus();
                return this.m_currentStatus;
            }
        }

        internal void SetStatus(JobStatus js)
        {
            this.m_currentStatus = js;
        }

        /// <summary>
        /// Cancel the job computation.
        /// </summary>
        /// <returns>The actual status of the job. </returns>
        internal JobStatus Cancel()
        {
            if (this.Terminated())
            {
                return this.m_currentStatus;
            }

            lock (this)
            {
                this.m_currentStatus = this.m_jobSubmission.TerminateJob();
                return this.m_currentStatus;
            }
        }

        /// <summary>
        /// For failed jobs this contains an error message.
        /// </summary>
        internal string ErrorMsg
        {
            get { return this.m_jobSubmission.ErrorMsg; }
        }

        public string GetJobId()
        {
            return this.m_jobSubmission.GetJobId();
        }

        internal IDryadLinqJobSubmission JobSubmission
        {
            get { return this.m_jobSubmission; }
        }
    }
}
