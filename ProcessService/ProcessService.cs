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
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Diagnostics;
using System.IO;

using Microsoft.Research.Peloponnese;

namespace Microsoft.Research.Dryad.ProcessService
{
    enum ProcessStatus
    {
        Queued,
        Running,
        Canceling,
        Completed
    };

    internal class ValueVersion
    {
        public string processStatus;
        public int exitCode;
        public Int64 startTime;
        public Int64 stopTime;
        public UInt64 version;
        public string shortStatus;
        public byte[] value;

        public ValueVersion(UInt64 ve, string ss, byte[] va, int ec, ProcessStatus status, Int64 start, Int64 stop)
        {
            version = ve;
            shortStatus = ss;
            value = va;
            exitCode = ec;
            startTime = start;
            stopTime = stop;
            switch (status)
            {
                case ProcessStatus.Queued:
                    processStatus = "Queued";
                    break;

                case ProcessStatus.Running:
                    processStatus = "Running";
                    break;

                case ProcessStatus.Canceling:
                    processStatus = "Canceling";
                    break;

                default:
                    processStatus = "Completed";
                    break;
            }
        }
    }

    class MailboxRecord
    {
        private string shortStatus;
        private byte[] value;
        private UInt64 version;
        private TaskCompletionSource<bool> waiter;

        public MailboxRecord()
        {
            version = 0;
            waiter = new TaskCompletionSource<bool>();
            value = new byte[0];
            shortStatus = "";
        }

        // add a continuation so setting the result won't synchronously call the awaiting thread
        public Task Waiter { get { return waiter.Task.ContinueWith((t) => {}); } }

        public void Unblock()
        {
            waiter.SetResult(true);

            ++version;
            waiter = new TaskCompletionSource<bool>();
        }

        public void SetValue(string status, byte[] newvalue, bool unblock)
        {
            shortStatus = status;
            value = newvalue;
            if (unblock)
            {
            // Unblock increments the version
                Unblock();
        }
        }

        public UInt64 GetValue(out string s, out byte[] v)
        {
            s = shortStatus;
            v = value;
            return version;
        }
    }

    class ProcessRecord
    {
        private ILogger logger;

        private int id;
        private Dictionary<string, MailboxRecord> mailbox;
        //private Dictionary<string, long> inProgressFile;
        private int exitCode;
        private ProcessStatus status;
        private Int64 startTime;
        private Int64 stopTime;
        private Process process;
        private Action collector;

        public ProcessRecord(int i, ILogger l, Action collect)
        {
            logger = l;
            collector = collect;
            id = i;
            status = ProcessStatus.Queued;
            exitCode = unchecked((int)Constants.WinError_StillActive);
            mailbox = new Dictionary<string, MailboxRecord>();

            // every process has a record corresponding to the 'null' key that is used for monitoring
            // the lifetime of the process
            var record = new MailboxRecord();
            mailbox.Add(Constants.NullKeyString, record);
        }

        // Always accessed when process is locked
        public int ExitCode { get { return exitCode; } }

        // Always accessed when process is locked
        public ProcessStatus Status { get { return status; } }

        // Always accessed when process is locked
        public Int64 StartTime { get { return startTime; } }

        // Always accessed when process is locked
        public Int64 StopTime { get { return stopTime; } }

        private void SetCompletedStatus()
        {
            status = ProcessStatus.Completed;
            stopTime = process.ExitTime.ToFileTimeUtc();
            // after 30 seconds, call our parent to garbage collect the record and its mailboxes
            Task.Delay(30 * 1000).ContinueWith((t) => collector());
        }

        public void OnExited(object obj, EventArgs args)
        {
            lock (this)
            {
                SetExited();
            }
        }

        private void UnblockMailboxes()
        {
            foreach (var k in mailbox)
            {
                MailboxRecord r = k.Value;
                // r.Unblock increments r's version
                r.Unblock();
                string s;
                byte[] v;
                logger.Log("mailbox", "Unblocking " + k.Key + " with version " + r.GetValue(out s, out v));
            }
        }

        private async Task FlushRegularly(Stream stream, int intervalMs, Task interrupt)
        {
            try
            {
                Task awoken;
                do
                {
                    awoken = await Task.WhenAny(Task.Delay(intervalMs), interrupt);
                    await stream.FlushAsync();
                } while (awoken != interrupt);
            }
            catch (Exception e)
            {
                logger.Log("Flusher caught exception " + e.ToString());
            }
        }

        private async void CopyStreamWithCatch(StreamReader src, string dstPath)
        {
            try
            {
                using (Stream dst = new FileStream(dstPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
                {
                    TaskCompletionSource<bool> stopFlushing = new TaskCompletionSource<bool>();
                    Task flushTask = Task.Run(() => FlushRegularly(dst, 1000, stopFlushing.Task));
                    int nRead;
                    byte[] buffer = new byte[4 * 1024];
                    do
                    {
                        nRead = await src.BaseStream.ReadAsync(buffer, 0, buffer.Length);
                        if (nRead > 0)
                        {
                            await dst.WriteAsync(buffer, 0, nRead);
                        }
                    } while (nRead > 0);
                    stopFlushing.SetResult(true);
                    await flushTask;
                }
            }
            catch (Exception e)
            {
                logger.Log("Copying stream to " + dstPath + " caught exception " + e.ToString());
            }
        }

        // Always called when process is locked
        public void Launch(string logDirectory, ProcessStartInfo startInfo)
        {
            if (status == ProcessStatus.Completed)
            {
                logger.Log("process " + id + " already canceled so not starting");
                return;
            }

            try
            {
                process = new Process();
                process.StartInfo = startInfo;
                process.EnableRaisingEvents = true;
                process.Exited += new EventHandler(OnExited);

                logger.Log("About to start process " + id);
                var started = process.Start();
                if (started)
                {
                    logger.Log("Process " + id + " started successfully");
                    status = ProcessStatus.Running;
                    startTime = process.StartTime.ToFileTimeUtc();

                    string name = "process-" + id;
                    string stdOutDest = name + "-stdout.txt";
                    string stdErrDest = name + "-stderr.txt";
                    if (logDirectory != null)
                    {
                        stdOutDest = Path.Combine(logDirectory, stdOutDest);
                        stdErrDest = Path.Combine(logDirectory, stdErrDest);
                    }
                    else
                    {
                        stdOutDest = Path.Combine(startInfo.WorkingDirectory, stdOutDest);
                        stdErrDest = Path.Combine(startInfo.WorkingDirectory, stdErrDest);
                    }

                    Task copyOutTask = Task.Run(() => CopyStreamWithCatch(process.StandardOutput, stdOutDest));
                    Task copyErrTask = Task.Run(() => CopyStreamWithCatch(process.StandardError, stdErrDest));
                }
                else
                {
                    logger.Log("Process " + id + " failed to start");
                    SetCompletedStatus();
                }
            }
            catch (Exception e)
            {
                logger.Log("Error starting process " + id + ": " + e.ToString());
                SetCompletedStatus();
            }

            UnblockMailboxes();
        }

        // Always called when process is locked
        public void Cancel()
        {
            if (status == ProcessStatus.Completed)
            {
                logger.Log("process " + id + " already canceled so not killing");
                return;
            }

            if (status == ProcessStatus.Running)
            {
                try
                {
                    logger.Log("trying to kill process " + id);
                    process.Kill();
                }
                catch (Exception e)
                {
                    logger.Log("error killing process " + id + ": " + e.ToString());
                }

                status = ProcessStatus.Canceling;

                // wait for the event telling us the process has exited before unblocking the waiters
                return;
            }

            logger.Log("setting queued process " + id + " to completed");
            exitCode = unchecked((int)Constants.DrError_VertexReceivedTermination);
            SetCompletedStatus();

            UnblockMailboxes();
        }

        // Always called when process is locked
        private void SetExited()
        {
            if (status == ProcessStatus.Queued || status == ProcessStatus.Completed)
            {
                logger.Log(String.Format("process {0} in unexpected state {1} while setting exited", id, status.ToString()));
            }
            else if (status == ProcessStatus.Running)
            {
                exitCode = process.ExitCode;
                logger.Log("setting running process " + id + " to completed exit code " + exitCode);
            }
            else
            {
                exitCode = unchecked((int)Constants.DrError_VertexReceivedTermination);
                logger.Log("setting canceling process " + id + " to completed exit code " + exitCode + " real code " + process.ExitCode);
            }

            SetCompletedStatus();

            UnblockMailboxes();
        }

        // Always called when process is locked
        public void SetValue(string s, string status, byte[] value, bool unblock)
        {
            MailboxRecord record;
            if (!mailbox.TryGetValue(s, out record))
            {
                record = new MailboxRecord();
                mailbox.Add(s, record);
            }

            record.SetValue(status, value, unblock);
        }

        // Always called when process is locked
        public Task GetValue(string s, UInt64 lastSeenVersion, out UInt64 currentVersion, out string shortStatus, out byte[] value)
        {
            MailboxRecord record;
            if (!mailbox.TryGetValue(s, out record))
            {
                record = new MailboxRecord();
                mailbox.Add(s, record);
            }

            currentVersion = record.GetValue(out shortStatus, out value);
            if (lastSeenVersion < currentVersion || status == ProcessStatus.Completed)
            {
                return null;
            }
            else
            {
                return record.Waiter;
            }
        }
    }

    internal class ProcessService : IDisposable
    {
        private ILogger logger;
        private Dictionary<string, string> environment;
        private Dictionary<int, ProcessRecord> processTable;
        private HttpServer server;
        private ProcessServer processServer;
        private FileServer fileServer;
        private ManualResetEvent finished;
        private readonly string logDirectory;

        public ProcessService(ILogger l)
        {
            logger = l;
            processTable = new Dictionary<int, ProcessRecord>();
            finished = new ManualResetEvent(false);

            string logDirEnv = Environment.GetEnvironmentVariable("LOG_DIRS");
            if (logDirEnv == null)
            {
                logDirectory = null;
            }
            else
            {
                // deal with comma-separated list
                logDirEnv = logDirEnv.Split(',').First().Trim();
                if (Directory.Exists(logDirEnv))
                {
                    logDirectory = logDirEnv;
                }
                else
                {
                    logDirectory = null;
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
            if (server != null)
            {
                server.Stop();
            }
            finished.Close();
        }
        }

        public void ShutDown()
        {
            logger.Log("stopping http server");
            if (server != null)
            {
            server.Stop();
            server = null;
            }

            logger.Log("setting event");
            finished.Set();
        }

        public void WaitForExit()
        {
            finished.WaitOne();
        }

        public string LogDirectory { get { return logDirectory; } }

        public bool Initialize(XDocument doc, out int portNumber, out string httpPrefix, out Dictionary<string, string> environment)
        {
            portNumber = 0;
            httpPrefix = null;
            environment = null;

            var outer = doc.Descendants("ProcessService").Single();

            try
            {
                // get the set of environment variables
                environment = new Dictionary<string, string>();
                foreach (var e in outer.Descendants("Environment").Single().Descendants("Variable"))
                {
                    environment.Add(e.Attribute("var").Value, e.Value);
                }
            }
            catch (Exception e)
            {
                logger.Log("Failed to get environment variables " + e.ToString());
                return false;
            }

            try
            {
                portNumber = Int32.Parse(outer.Descendants("Port").Single().Value);
                httpPrefix = outer.Descendants("Prefix").Single().Value.Trim();
            }
            catch (Exception e)
            {
                logger.Log("Failed to get port base and prefix from config: " + e.ToString());
                return false;
            }

            return true;
        }

        public bool Start(XDocument config, string groupName, string processId, out string processUri, out string fileUri)
        {
            int portNumber;
            string httpPrefix;

            processUri = null;
            fileUri = null;

            if (!Initialize(config, out portNumber, out httpPrefix, out environment))
            {
                finished.Set();
                return false;
            }

            httpPrefix = httpPrefix + groupName + "/" + processId + "/";

            server = new HttpServer(portNumber, httpPrefix, logger);

            if (!server.Start())
            {
                logger.Log("Failed to start process http server");
                finished.Set();
                return false;
            }

            processServer = new ProcessServer("process/", server, this);
            fileServer = new FileServer("file/", server, this);

            processUri = processServer.BaseURI;
            fileUri = fileServer.BaseURI;

            return true;
        }

        public void GarbageCollectProcess(int processId)
        {
            lock (processTable)
            {
                if (processTable.ContainsKey(processId))
                {
                    logger.Log("Garbage collecting process id " + processId);
                    processTable.Remove(processId);
                }
                else
                {
                    logger.Log("Unable to garbage collect unknown process id " + processId);
                }
            }
        }

        public bool Create(int processId)
        {
            ProcessRecord process = new ProcessRecord(processId, logger, () => GarbageCollectProcess(processId));

            lock (processTable)
            {
                if (processTable.ContainsKey(processId))
                {
                    return false;
                }

                processTable.Add(processId, process);
            }

            return true;
        }

        private void SplitCmdLine(string cmdLine, out string cmd, out string args)
        {
            cmd = "";
            args = "";
            int lastSpacePos = 0;
            bool rootedPath = Path.IsPathRooted(cmdLine);
            string candPath = cmdLine;

            while (true)
            {                
                if (!rootedPath)
                {
                    candPath = Path.Combine(Environment.CurrentDirectory, candPath);
                }
                if (File.Exists(candPath))
                {
                    cmd = candPath;
                    args = (lastSpacePos > 0) ? cmdLine.Substring(lastSpacePos) : "";
                    return;
                }

                int spacePos = cmdLine.IndexOf(' ', lastSpacePos);
                if (spacePos != -1)
                {
                    candPath = cmdLine.Substring(0, spacePos);
                    lastSpacePos = spacePos + 1;
                }
                else
                {
                    break;
                }
            }
            throw new ApplicationException("Couldn't split command line into command line and arguments.");
        }

        public void Launch(int processId, string commandLine, string arguments)
        {
            ProcessRecord process;
            lock (processTable)
            {
                process = processTable[processId];
            }

            ProcessStartInfo startInfo;

            try
            {
                string serviceWorkingDirectory = System.IO.Directory.GetCurrentDirectory();

                System.IO.Directory.CreateDirectory(processId.ToString());

                startInfo = new ProcessStartInfo();
                startInfo.CreateNoWindow = true;
                startInfo.UseShellExecute = false;
                startInfo.RedirectStandardOutput = true;
                startInfo.RedirectStandardError = true;
                startInfo.WorkingDirectory = Path.Combine(serviceWorkingDirectory, processId.ToString());
                logger.Log(String.Format("Working directory: '{0}'", startInfo.WorkingDirectory));
                
                // Use either FQ path or path relative to job path  
                if (Path.IsPathRooted(commandLine))
                {
                    startInfo.FileName = commandLine;
                }
                else
                {
                    startInfo.FileName = Path.Combine(serviceWorkingDirectory, commandLine);
                }
                
                startInfo.Arguments = arguments;
                logger.Log(String.Format("FileName: '{0}'", startInfo.FileName));

                logger.Log(String.Format("args: '{0}'", arguments));

                // Add environment variable to vertex host process
                Uri genericUri = new Uri(processServer.BaseURI);
                Uri localUri = new Uri(genericUri.Scheme + "://localhost:" + genericUri.Port + genericUri.PathAndQuery);
                string processUpdateURI = localUri.ToString() + processId.ToString();
                startInfo.EnvironmentVariables.Add(Constants.EnvProcessServerUri, processUpdateURI);
                logger.Log(String.Format("Added env: {0}='{1}'", Constants.EnvProcessServerUri, processUpdateURI));

                foreach (var v in environment)
                {
                    startInfo.EnvironmentVariables.Remove(v.Key);
                    startInfo.EnvironmentVariables.Add(v.Key, v.Value);
                    logger.Log(String.Format("Added env: {0}='{1}'", v.Key, v.Value));
                }
            }
            catch (Exception e)
            {
                logger.Log("failed to make startinfo: " + e.ToString());

                lock (process)
                {
                    process.Cancel();
                }

                return;
            }

            lock (process)
            {
                process.Launch(logDirectory, startInfo);
            }
        }

        public async Task<ValueVersion> BlockOnStatus(int processId, string key, UInt64 version, int timeout)
        {
            ProcessRecord process;

            lock (processTable)
            {
                if (!processTable.TryGetValue(processId, out process))
                {
                    return null;
                }
            }

            Task unblocker;
            UInt64 currentVersion;
            string status;
            byte[] value;
            lock (process)
            {
                unblocker = process.GetValue(key, version, out currentVersion, out status, out value);
                if (unblocker == null)
                {
                    return new ValueVersion(currentVersion, status, value, process.ExitCode, process.Status, process.StartTime, process.StopTime);
                }
            }

            await Task.WhenAny(unblocker, Task.Delay(timeout));

            lock (process)
            {
                // Return whatever the current version is now
                unblocker = process.GetValue(key, version, out currentVersion, out status, out value);
                return new ValueVersion(currentVersion, status, value, process.ExitCode, process.Status, process.StartTime, process.StopTime);
            }
        }

        public void Kill(int processId)
        {
            ProcessRecord process;

            lock (processTable)
            {
                if (!processTable.TryGetValue(processId, out process))
                {
                    return;
                }
            }

            lock (process)
            {
                process.Cancel();
            }
        }

        public bool SetValue(int processId, string key, string shortStatus, byte[] value, bool unblock)
        {
            ProcessRecord process;

            lock (processTable)
            {
                if (!processTable.TryGetValue(processId, out process))
                {
                    return false;
                }
            }

            lock (process)
            {
                process.SetValue(key, shortStatus, value, unblock);
            }

            return true;
        }

        public Task<string> Upload(string srcDirectory, IEnumerable<string> sources, Uri dstUri)
        {
            return Task.FromResult<string>(null);
        }
    }
}
