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

namespace Microsoft.Research.DryadLinq
{
    abstract class PeloponneseJobSubmission : IDryadLinqJobSubmission
    {
        private DryadLinqContext m_context;
        protected IEnumerable<string> m_peloponneseGMFiles;
        protected IEnumerable<string> m_dryadGMFiles;
        protected IEnumerable<string> m_peloponneseWorkerFiles;
        protected IEnumerable<string> m_dryadWorkerFiles;
        private string m_cmdLine;
        private string m_queryPlan;
        private Dictionary<string, HashSet<string>> m_localResources;

        protected string QueryPlan { get { return m_queryPlan; } }
        protected DryadLinqContext Context { get { return m_context; } }
        protected Dictionary<string, HashSet<string>> LocalResources { get { return m_localResources; } }

        abstract public string ErrorMsg { get; }
        abstract public void SubmitJob();
        abstract public string GetJobId();
        abstract public JobStatus GetStatus();
        abstract public JobStatus TerminateJob();
        abstract public JobStatus WaitForCompletion();
        

        public PeloponneseJobSubmission(DryadLinqContext context)
        {
            m_context = context;
            m_localResources = new Dictionary<string, HashSet<string>>();

            m_peloponneseGMFiles = Peloponnese.ClusterUtils.ConfigHelpers
                .ListPeloponneseResources(context.PeloponneseHomeDirectory)
                .Select(r => r.ToLower());
            IEnumerable<string> graphManagerFiles = Peloponnese.Shared.DependencyLister.Lister
                .ListDependencies(Path.Combine(context.DryadHomeDirectory, "Microsoft.Research.Dryad.GraphManager.exe"))
                .Select(r => r.ToLower());
            m_dryadGMFiles = graphManagerFiles.Except(m_peloponneseGMFiles);

            string[] additionalWorkerFiles =
            {
                "Microsoft.Research.Dryadlinq.dll",
                "Microsoft.Research.Dryad.DryadLinq.NativeWrapper.dll"
            };

            IEnumerable<string> processServiceFiles = Peloponnese.Shared.DependencyLister.Lister
                .ListDependencies(Path.Combine(context.DryadHomeDirectory, "Microsoft.Research.Dryad.ProcessService.exe"))
                .Select(r => r.ToLower());
            IEnumerable<string> vertexHostFiles = Peloponnese.Shared.DependencyLister.Lister
                .ListDependencies(Path.Combine(context.DryadHomeDirectory, "Microsoft.Research.Dryad.VertexHost.exe"))
                .Concat(additionalWorkerFiles.Select(f => Path.Combine(context.DryadHomeDirectory, f)))
                .Select(r => r.ToLower());
            IEnumerable<string> workerFiles = processServiceFiles.Union(vertexHostFiles);

            m_peloponneseWorkerFiles = workerFiles.Intersect(m_peloponneseGMFiles);
            m_dryadWorkerFiles = workerFiles.Except(m_peloponneseGMFiles);
        }

        static protected bool IsValidDryadDirectory(string directory)
        {
            string[] sampleDryadFiles = 
            {
                "Microsoft.Research.Dryad.GraphManager.exe",
                "Microsoft.Research.Dryad.GraphManager.exe.config",
            };

            IEnumerable<string> filesPresent =
                Directory.EnumerateFiles(directory, "*", SearchOption.TopDirectoryOnly)
                .Select(x => Path.GetFileName(x).ToLower());

            IEnumerable<string> filesNeeded = sampleDryadFiles.Select(x => x.ToLower());

            return (filesPresent.Intersect(filesNeeded).Count() == sampleDryadFiles.Length);
        }

        private bool IsDryadWorkerFile(string fileName)
        {
            return
                m_dryadWorkerFiles.Select(f => Path.GetFileName(f)).Contains(fileName.ToLower()) ||
                m_peloponneseWorkerFiles.Select(f => Path.GetFileName(f)).Contains(fileName.ToLower());
        }

        public void AddJobOption(string fieldName, string fieldVal)
        {
            if (fieldName == "cmdline")
            {
                m_cmdLine = fieldVal;
                var fields = m_cmdLine.Split();
                m_queryPlan = fields[fields.Length - 1].Trim();
                Console.WriteLine("QueryPlan: {0}", m_queryPlan);
            }
        }

        public void AddLocalFile(string pathName)
        {
            var fileName = Path.GetFileName(pathName);
            var directory = Path.GetDirectoryName(pathName);

            if ((directory == Context.DryadHomeDirectory || directory == Context.PeloponneseHomeDirectory)
                && IsDryadWorkerFile(fileName))
            {
                // we deal with these resources elsewhere
                return;
            }

            HashSet<string> group;
            if (!m_localResources.TryGetValue(directory, out group))
            {
                group = new HashSet<string>();
                m_localResources.Add(directory, group);
            }

            if (!group.Contains(fileName))
            {
                group.Add(fileName);
            }
        }

        public void AddRemoteFile(string fileName)
        {
            throw new System.NotImplementedException();
        }

        protected abstract XElement MakeJMConfig();
        protected abstract XElement MakeWorkerConfig(string configPath);

        protected XDocument MakeConfig(string psConfigPath)
        {
            var configDoc = new XDocument();
            var docElement = new XElement("PeloponneseConfig");
            var serverElement = new XElement("PeloponneseServer");

            var portElement = new XElement("Port");
            portElement.Value = "8472";
            serverElement.Add(portElement);

            var prefixElement = new XElement("Prefix");
            prefixElement.Value = "/peloponnese/server/";
            serverElement.Add(prefixElement);

            serverElement.Add(MakeJMConfig());
            serverElement.Add(MakeWorkerConfig(psConfigPath));

            docElement.Add(serverElement);

            configDoc.Add(docElement);

            return configDoc;
        }
    }
}
