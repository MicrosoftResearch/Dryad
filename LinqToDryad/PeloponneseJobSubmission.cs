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

namespace Microsoft.Research.DryadLinq
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Xml.Linq;
    using System.Linq;
    using System.Text;
    using Microsoft.Research.DryadLinq.Internal;

    abstract class PeloponneseJobSubmission : IDryadLinqJobSubmission
    {
        private DryadLinqContext m_context;
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

        public PeloponneseJobSubmission(DryadLinqContext context)
        {
            m_context = context;
            m_localResources = new Dictionary<string, HashSet<string>>();
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

            if (directory == Context.DryadHomeDirectory)
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
        protected abstract XElement MakeWorkerConfig(string configPath, XElement peloponneseResource);

        protected XDocument MakeConfig(string psConfigPath, XElement peloponneseResource)
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
            serverElement.Add(MakeWorkerConfig(psConfigPath, peloponneseResource));

            docElement.Add(serverElement);

            configDoc.Add(docElement);

            return configDoc;
        }
    }
}
