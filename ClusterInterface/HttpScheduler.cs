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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Microsoft.Research.Dryad.ClusterInterface
{
    public class HttpCluster : ICluster
    {
        private ILogger logger;
        private IScheduler scheduler;

        public HttpCluster(ILogger l)
        {
            logger = l;

            HttpClient.Initialize(l);

            scheduler = Factory.CreateScheduler("local", l);

            l.Log("HttpCluster created");
        }

        public bool Start()
        {
            return scheduler.Start();
        }

        public void Stop()
        {
            scheduler.Stop();
        }

        public List<IComputer> GetComputers()
        {
            return scheduler.GetComputers();
        }

        public string GetLocalFilePath(IComputer computer, string directory, string fileName, int compressionMode)
        {
            UriBuilder uri = new UriBuilder("file:///");
            uri.Path = Path.Combine(computer.Directory, directory, fileName);
            uri.Query = "c=" + compressionMode;
            return uri.Uri.AbsoluteUri;
        }

        public string GetRemoteFilePath(IComputer computer, string directory, string fileName, int compressionMode)
        {
            IComputer runningComputer = scheduler.GetComputerAtHost(computer.Host);
            UriBuilder fileServer;
            if (runningComputer == null)
            {
                logger.Log("No currently known computer running at host " + computer.Host + " to read file " + directory + "/" + fileName);
                fileServer = new UriBuilder("http://noKnownProcessFor/" + computer.Host);
            }
            else
            {
                fileServer = new UriBuilder(runningComputer.FileServer);
            }

            fileServer.Path += Path.Combine(computer.Directory, directory, fileName);
            fileServer.Query = "c=" + compressionMode;

            return fileServer.Uri.AbsoluteUri;
        }

        public IProcess NewProcess(IProcessWatcher watcher, string commandLine, string commandLineArguments)
        {
            ISchedulerProcess process = scheduler.NewProcess();
            return new Process(process, watcher, commandLine, commandLineArguments, logger);
        }

        public void ScheduleProcess(IProcess ip, List<Affinity> affinities)
        {
            Process process = ip as Process;
            process.ToQueued();
            scheduler.ScheduleProcess(process.SchedulerProcess, affinities, process.Run);
        }

        public void CancelProcess(IProcess ip)
        {
            Process process = ip as Process;

            scheduler.CancelProcess(process.SchedulerProcess);
            process.Cancel();
        }

        public void GetProcessStatus(IProcess ip, IProcessKeyStatus status)
        {
            Process process = ip as Process;
            Task.Run(() => process.GetKeyStatus(status));
        }

        public void SetProcessCommand(IProcess ip, IProcessCommand command)
        {
            Process process = ip as Process;
            Task.Run(() => process.SetCommand(command));
        }
    }
}
