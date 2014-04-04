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
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Xml.Linq;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.DryadLinq
{
    internal class LocalJobSubmission : PeloponneseJobSubmission
    {
        private JobStatus m_status;
        private string m_error;
        private Process m_ppmProcess;
        private int m_applicationId;
        private string m_workingDirectory;

        public LocalJobSubmission(DryadLinqContext context) : base(context)
        {
            m_status = JobStatus.NotSubmitted;
            m_error = null;
        }

        public override string ErrorMsg
        {
            get
            {
                lock (this)
                {
                    return m_error;
                }
            }
        }

        public override JobStatus GetStatus()
        {
            lock (this)
            {
                return m_status;
            }
        }

        private XElement MakeResourceGroup(string location, HashSet<string> files)
        {
            var rgElement = new XElement("ResourceGroup");
            rgElement.SetAttributeValue("type", "local");
            rgElement.SetAttributeValue("location", location);

            foreach (var r in files)
            {
                var rElement = new XElement("Resource");
                rElement.Value = r;
                rgElement.Add(rElement);
            }

            return rgElement;
        }

        protected override XElement MakeJMConfig()
        {
            var environment = new Dictionary<string, string>();
            environment.Add("PATH", Environment.GetEnvironmentVariable("PATH") + ";" + Context.PeloponneseHomeDirectory);
            var jarPath = Path.Combine(Context.PeloponneseHomeDirectory, "Peloponnese-HadoopBridge.jar");
            environment.Add("PELOPONNESE_ADDITIONAL_CLASSPATH", jarPath);

            // add the query plan to the JM directory so that job analysis tools can find it later
            string queryPlanDirectory = Path.GetDirectoryName(QueryPlan);
            string queryPlanFile = Path.GetFileName(QueryPlan);
            HashSet<string> queryPlanSet = new HashSet<string>();
            queryPlanSet.Add(queryPlanFile);
            List<XElement> resources = new List<XElement>();
            resources.Add(MakeResourceGroup(queryPlanDirectory, queryPlanSet));

            string logDirectory = Path.Combine(m_workingDirectory, "log");
            Uri logUri = new Uri("file:///" + logDirectory + "/");
            string logDirParam = Microsoft.Research.Peloponnese.Storage.AzureUtils.CmdLineEncode(logUri.AbsoluteUri);

            var jmPath = Path.Combine(Context.DryadHomeDirectory, "DryadLinqGraphManager.exe");
            var vertexPath = Path.Combine(Context.DryadHomeDirectory, "VertexHost.exe");
            string[] jmArgs = { "--dfs=" + logDirParam, vertexPath, queryPlanFile };
            return ConfigHelpers.MakeProcessGroup(
                "jm", "local", 1, 1, true,
                jmPath, jmArgs, null, "graphmanager-stdout.txt", "graphmanager-stderr.txt",
                resources, environment);
        }

        protected override XElement MakeWorkerConfig(string configPath, XElement peloponneseResource)
        {
            // add job-local resources to each worker directory, leaving out the standard Dryad files
            var resources = new List<XElement>();
            foreach (var rg in LocalResources.Where(rg => rg.Key != Context.DryadHomeDirectory))
            {
                resources.Add(MakeResourceGroup(rg.Key, rg.Value));
            }
            int numWorkerProcesses = 2;
            if (Context.JobMinNodes.HasValue)
            {
                numWorkerProcesses = Context.JobMinNodes.Value;
            }
            var psPath = Path.Combine(Context.DryadHomeDirectory, "ProcessService.exe");
            string[] psArgs = { configPath };
            return ConfigHelpers.MakeProcessGroup(
                "Worker", "local", 2, numWorkerProcesses, false,
                psPath, psArgs, null, "processservice-stdout.txt", "processservice-stderr.txt",
                resources, null);
        }

        private string MakeProcessServiceConfig()
        {
            var configDoc = new XDocument();

            var docElement = new XElement("PeloponneseConfig");

            var psElement = new XElement("ProcessService");

            var psPortElement = new XElement("Port");
            psPortElement.Value = "8472";
            psElement.Add(psPortElement);

            var psPrefixElement = new XElement("Prefix");
            psPrefixElement.Value = "/peloponnese/dpservice/";
            psElement.Add(psPrefixElement);

            var environment = new Dictionary<string, string>();
            environment.Add("PATH", Environment.GetEnvironmentVariable("PATH") + ";" + Context.PeloponneseHomeDirectory);
            var jarPath = Path.Combine(Context.PeloponneseHomeDirectory, "Peloponnese-HadoopBridge.jar");
            environment.Add("PELOPONNESE_ADDITIONAL_CLASSPATH", jarPath);

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

        private string GenerateConfig()
        {
            var psConfigPath = MakeProcessServiceConfig();
            var configPath = DryadLinqCodeGen.GetPathForGeneratedFile("ppmConfig.xml", null);

            var configDoc = MakeConfig(psConfigPath, null);
            configDoc.Save(configPath);

            return configPath;
        }

        private void CreateDirectory()
        {
            m_workingDirectory = null;

            string wdBase = Path.Combine(Context.DryadHomeDirectory, "LocalJobs");
            if (!Directory.Exists(wdBase))
            {
                Directory.CreateDirectory(wdBase);
            }

            var existingDirs = Directory.EnumerateDirectories(wdBase);
            var existingJobs = existingDirs.Select(x => Path.GetFileName(x))
                                           .Select(x => { int jobId; if (int.TryParse(x, out jobId)) return jobId; else return -1; });

            int nextJob = 0;
            if (existingJobs.Count() > 0)
            {
                nextJob = existingJobs.Max() + 1;
            }

            lock (this)
            {
                m_applicationId = nextJob;
                m_status = JobStatus.Waiting;
            }

            var wd = Path.Combine(wdBase, nextJob.ToString());

            try
            {
                Directory.CreateDirectory(wd);
            }
            catch (Exception e)
            {

                lock (this)
                {
                    m_error = "Failed to create local job directory " + wd + ": " + e.ToString();
                    m_status = JobStatus.Failure;
                }

                Console.WriteLine(m_error);

                return;
            }

            var logD = Path.Combine(wd, "log");

            try
            {
                Directory.CreateDirectory(logD);
            }
            catch (Exception e)
            {
                lock (this)
                {
                    m_error = "Failed to create local log directory " + logD + ": " + e.ToString();
                    m_status = JobStatus.Failure;
                }

                Console.WriteLine(m_error);

                return;
            }
 
            m_workingDirectory = wd;
        }

        void OnJobExited(Object obj, EventArgs args)
        {
            var exitCode = m_ppmProcess.ExitCode;
            string error = null;

            string errorPath = Path.Combine(m_workingDirectory, "log", "error.txt");
            if (File.Exists(errorPath))
            {
                try
                {
                    error = File.ReadAllText(errorPath);
                }
                catch (Exception)
                {
                }
            }

            lock (this)
            {
                if (exitCode == 0)
                {
                    m_status = JobStatus.Success;
                }
                else
                {
                    m_status = JobStatus.Failure;
                    if (error != null)
                    {
                        m_error = error;
                    }
                }
            }
        }

        public override void SubmitJob()
        {
            CreateDirectory();
            if (m_workingDirectory == null)
            {
                return;
            }

            var configLocation = GenerateConfig();

            ProcessStartInfo psi = new ProcessStartInfo();
            psi.FileName = Path.Combine(Context.PeloponneseHomeDirectory, "PersistentProcessManager.exe");
            psi.Arguments = configLocation;
            psi.UseShellExecute = false;
            psi.WorkingDirectory = m_workingDirectory;

            m_ppmProcess = new Process();
            m_ppmProcess.StartInfo = psi;
            m_ppmProcess.EnableRaisingEvents = true;
            m_ppmProcess.Exited += new EventHandler(OnJobExited);

            m_ppmProcess.Start();

            lock (this)
            {
                m_status = JobStatus.Running;
            }
        }

        public override JobStatus TerminateJob()
        {
            lock (this)
            {
                if (m_status == JobStatus.Running)
                {
                    try
                    {
                        m_ppmProcess.Kill();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to kill job: " + e.ToString());
                    }
                }

                m_status = JobStatus.Cancelled;
                return m_status;
            }
        }

        public override string GetJobId()
        {
            lock (this)
            {
                return m_applicationId.ToString();
            }
        }
    }
}
