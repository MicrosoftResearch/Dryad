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
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Research.DryadLinq.Internal;

using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.DryadLinq
{
    internal class YarnJobSubmission : PeloponneseJobSubmission
    {
        private ClusterJob m_job;
        private Uri baseStagingUri;

        public YarnJobSubmission(DryadLinqContext context, Uri baseStagingUri) : base(context)
        {
            this.baseStagingUri = baseStagingUri;
        }

        private Uri PeloponneseDfs
        {
            get { return Context.Cluster.DfsClient.Combine(baseStagingUri, "peloponnese"); }
        }

        private Uri DryadDfs
        {
            get { return Context.Cluster.DfsClient.Combine(baseStagingUri, "dryad"); }
        }

        private Uri UserDfs
        {
            get { return Context.Cluster.DfsClient.Combine(baseStagingUri, Environment.UserName, "dryadJob" ); }
        }

        protected override XElement MakeJMConfig()
        {
            var environment = new Dictionary<string, string>();
            environment.Add(Constants.LoggingLevelEnvVar, Constants.LoggingStringFromLevel((int)Context.RuntimeLoggingLevel).ToString());

            var qpPath = Path.Combine("..", "..", Path.GetFileName(QueryPlan));
            var jmPath = Path.Combine("..", "..", "Microsoft.Research.Dryad.GraphManager.exe");
            string jobDirectoryTemplate =
                Context.Cluster.Client(Context).JobDirectoryTemplate.AbsoluteUri.Replace("_BASELOCATION_", "dryad-jobs");
            string logDirParam = Microsoft.Research.Peloponnese.Utils.CmdLineEncode(jobDirectoryTemplate);
            string[] jmArgs = { "--dfs=" + logDirParam, "Microsoft.Research.Dryad.VertexHost.exe", qpPath }; // +" --break";
            return ConfigHelpers.MakeProcessGroup(
                         "jm", "local", 1, 1, -1, true,
                         jmPath, jmArgs, "LOG_DIRS", "graphmanager-stdout.txt",
                         "graphmanager-stderr.txt",
                         null, environment);
        }

        protected override XElement MakeWorkerConfig(string configPath)
        {
            var waiters = new List<Task<XElement>>();

            waiters.Add(ConfigHelpers.MakePeloponneseWorkerResourceGroupAsync(Context.Cluster.DfsClient, baseStagingUri, Context.PeloponneseHomeDirectory));
            waiters.Add(ConfigHelpers.MakeResourceGroupAsync(Context.Cluster.DfsClient, PeloponneseDfs, true, m_peloponneseWorkerFiles));
            waiters.Add(ConfigHelpers.MakeResourceGroupAsync(Context.Cluster.DfsClient, DryadDfs, true, m_dryadWorkerFiles));

            // add job-local resources to each worker directory, using public versions of the standard Dryad files
            foreach (var rg in LocalResources)
            {
                IEnumerable<string> files = rg.Value.Select(x => Path.Combine(rg.Key, x));
                waiters.Add(ConfigHelpers.MakeResourceGroupAsync(Context.Cluster.DfsClient, UserDfs, false, files));
            }

            try
            {
                Task.WaitAll(waiters.ToArray());
            }
            catch (Exception e)
            {
                throw new DryadLinqException("Dfs resource make failed", e);
            }

            var resources = new List<XElement>();
            foreach (var t in waiters)
            {
                resources.Add(t.Result);
            }

            var psPath = "Microsoft.Research.Dryad.ProcessService.exe";
            string[]  psArgs = { Path.GetFileName(configPath) };
            int maxNodes = (Context.JobMaxNodes == null) ? -1 : Context.JobMaxNodes.Value;
            return ConfigHelpers.MakeProcessGroup(
                         "Worker", "yarn", -1, maxNodes, Context.ContainerMbMemory, false,
                         psPath, psArgs, "LOG_DIRS", "processservice-stdout.txt", "processservice-stderr.txt",
                         resources, null);
        }

        private string MakeProcessServiceConfig()
        {
            var configDoc = new XDocument();

            var docElement = new XElement("PeloponneseConfig");

            var psElement = new XElement("ProcessService");

            var psPortElement = new XElement("Port");
            psPortElement.Value = "8471";
            psElement.Add(psPortElement);

            var psPrefixElement = new XElement("Prefix");
            psPrefixElement.Value = "/peloponnese/dpservice/";
            psElement.Add(psPrefixElement);

            var environment = new Dictionary<string, string>();
            environment.Add(Constants.LoggingLevelEnvVar, Constants.LoggingStringFromLevel((int)Context.RuntimeLoggingLevel).ToString());
            environment.Add("DRYAD_THREADS_PER_WORKER", Context.ThreadsPerWorker.ToString());

            var envElement = new XElement("Environment");
            foreach (var e in environment)
            {
                var varElement = new XElement("Variable");
                varElement.SetAttributeValue("var", e.Key);
                varElement.Value = e.Value;
                envElement.Add(varElement);
            }
            psElement.Add(envElement);

            docElement.Add(psElement);

            configDoc.Add(docElement);

            string psConfigPath = DryadLinqCodeGen.GetPathForGeneratedFile("psConfig.xml", null);

            configDoc.Save(psConfigPath);

            return psConfigPath;
        }

        private XDocument MakeLauncherConfig(string configPath)
        {
            List<Task<XElement>> waiters = new List<Task<XElement>>();

            waiters.Add(ConfigHelpers.MakePeloponneseResourceGroupAsync(Context.Cluster.DfsClient, baseStagingUri, Context.PeloponneseHomeDirectory));
            waiters.Add(ConfigHelpers.MakeResourceGroupAsync(Context.Cluster.DfsClient, DryadDfs, true, m_dryadGMFiles));

            IEnumerable<string> userFiles = new[] { configPath, QueryPlan };
            waiters.Add(ConfigHelpers.MakeResourceGroupAsync(Context.Cluster.DfsClient, UserDfs, false, userFiles));

            try
            {
                Task.WaitAll(waiters.ToArray());
            }
            catch (Exception e)
            {
                throw new DryadLinqException("Dfs resource make failed", e);
            }

            List<XElement> resources = new List<XElement>();
            foreach (var t in waiters)
            {
                resources.Add(t.Result);
            }

            string appName = Context.JobFriendlyName;
            if (String.IsNullOrEmpty(appName))
            {
                appName = "DryadLINQ.App";
            }

            return ConfigHelpers.MakeLauncherConfig(appName, Path.GetFileName(configPath), Context.Queue, 
                Context.ApplicationMasterMbMemory, resources, JobDirectory);
        }

        private XDocument GenerateConfig()
        {
            string psConfigPath = MakeProcessServiceConfig();

            // this will cause the psConfig to be uploaded to the DFS during MakeConfig
            AddLocalFile(psConfigPath);

            var configDoc = MakeConfig(psConfigPath);

            string configPath = DryadLinqCodeGen.GetPathForGeneratedFile("ppmConfig.xml", null);
            configDoc.Save(configPath);

            return MakeLauncherConfig(configPath);
        }

        public override string ErrorMsg
        {
            get 
            {
                if (this.m_job == null)
                {
                    return null;
                }
                return this.m_job.ErrorMsg;
            }
        }

        public override JobStatus GetStatus()
        {
            if (this.m_job == null)
            {
                return JobStatus.NotSubmitted;
            }

            Peloponnese.ClusterUtils.JobStatus status = m_job.GetStatus();
            switch (status)
            {
                case Peloponnese.ClusterUtils.JobStatus.NotSubmitted:
                    return JobStatus.NotSubmitted;

                case Peloponnese.ClusterUtils.JobStatus.Waiting:
                    return JobStatus.Waiting;

                case Peloponnese.ClusterUtils.JobStatus.Running:
                    return JobStatus.Running;

                case Peloponnese.ClusterUtils.JobStatus.Success:
                    return JobStatus.Success;

                case Peloponnese.ClusterUtils.JobStatus.Failure:
                    return JobStatus.Failure;

                case Peloponnese.ClusterUtils.JobStatus.Cancelled:
                    return JobStatus.Cancelled;

                default:
                    throw new ApplicationException("Unknown status " + status);
            }
        }

        /// <summary>
        /// Waits for the job to complete
        /// </summary>
        /// <returns>The status of the job.</returns>
        public override JobStatus WaitForCompletion()
        {
            m_job.Join();
            return this.GetStatus();
        }

        private string JobDirectory
        {
            get
            {
                return Context.Cluster.Client(Context).JobDirectoryTemplate.AbsoluteUri
                    .Replace("_BASELOCATION_", "dryad-jobs");
            }
        }

        public override void SubmitJob()
        {
            if (Context.PeloponneseHomeDirectory == null)
            {
                Context.PeloponneseHomeDirectory = Context.DryadHomeDirectory;
            }
            if (Context.DryadHomeDirectory == null)
            {
                throw new ApplicationException("No Dryad home directory is set");
            }
            if (!IsValidDryadDirectory(Context.DryadHomeDirectory))
            {
                throw new ApplicationException("Dryad home directory " + Context.DryadHomeDirectory + " is missing some required files");
            }

            XDocument config = GenerateConfig();

            try
            {
                this.m_job = Context.Cluster.Client(Context).Submit(config, new Uri(JobDirectory));
            }
            catch (Exception e)
            {
                throw new DryadLinqException("Failed to launch", e);
            }
        }

        public override JobStatus TerminateJob()
        {
            this.m_job.Kill();
            return GetStatus();
        }

        public override string GetJobId()
        {
            if (this.m_job == null)
            {
                return "Unknown";
            }
            else
            {
                return this.m_job.Id;
            }
        }
    }
}
