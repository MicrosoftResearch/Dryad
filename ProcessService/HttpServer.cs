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
using System.IO;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese;
using Microsoft.Research.Peloponnese.NotHttpServer;

namespace Microsoft.Research.Dryad.ProcessService
{
    class HttpServer
    {
        private ILogger logger;

        private IHttpServer server;
        private string prefix;
        private int portNumber;
        private string baseURI;

        public delegate Task HandlePutRequest(IHttpContext context);
        public delegate Task HandleGetRequest(IHttpContext context);
        private struct Handlers
        {
            public HandlePutRequest put;
            public HandleGetRequest get;
        }

        private Dictionary<string, Handlers> handlers;

        public HttpServer(int pn, string p, ILogger l)
        {
            logger = l;
            prefix = p;
            portNumber = pn;
            handlers = new Dictionary<string, Handlers>();
        }

        public ILogger Logger { get { return logger; } }

        public string Prefix { get { return prefix; } }

        public string BaseURI { get { return baseURI; } }

        public void Register(string p, HandlePutRequest put, HandleGetRequest get)
        {
            Handlers h = new Handlers { put = put, get = get };
            handlers.Add(p, h);
        }

        public async Task ReportError(IHttpContext context, HttpStatusCode code, string error)
        {
            logger.Log("Error " + code + ": " + error);
            context.Request.InputStream.Close();

            context.Response.StatusCode = (int)code;
            context.Response.StatusDescription = code.ToString();
            using (var sw = new StreamWriter(context.Response.OutputStream))
            {
                await sw.WriteAsync(error);
            }

            await context.Response.CloseAsync();
        }

        public async Task ReportSuccess(IHttpContext context, bool closeResponse)
        {
            logger.Log("Success");
            context.Request.InputStream.Close();

            context.Response.StatusCode = (int)HttpStatusCode.OK;
            context.Response.StatusDescription = HttpStatusCode.OK.ToString();

            if (closeResponse)
            {
                await context.Response.CloseAsync();
            }
        }

        private async Task HandleConnection(IHttpContext context)
        {
            string path = context.Request.Url.AbsolutePath;
            if (!path.StartsWith(prefix))
            {
                throw new ApplicationException("path " + path + " should start with " + prefix);
            }

            string relativePath = path.Substring(prefix.Length);

            bool found = false;
            Handlers component = new Handlers { put=null, get=null };
            foreach (var h in handlers)
            {
                if (relativePath.StartsWith(h.Key))
                {
                    found = true;
                    component = h.Value;
                    break;
                }
            }

            if (!found)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.Response.StatusDescription = "Unknown prefix";
                await context.Response.CloseAsync();
                return;
            }
            
            try
            {
                if (context.Request.HttpMethod.ToUpper() == "POST")
                {
                    await component.put(context);
                }
                else
                {
                    await component.get(context);
                }
            }
            catch (Exception e)
            {
                logger.Log("Got http exception " + e.ToString());
            }
        }

        public void Stop()
        {
            logger.Log("Stopping http server");

            if (server != null)
            {
                server.Stop();
            }

            logger.Log("Stopped http server");
        }

        public bool Start()
        {
            try
            {
                IPAddress address = IPAddress.Any;
                server = Factory.Create(address, portNumber, prefix, true, logger);

                if (server != null)
                {
                    baseURI = server.Address.AbsoluteUri;
                    server.Start(HandleConnection);
                    return true;
                }
            }
            catch (Exception e)
            {
                logger.Log("Failed to start server for " + prefix + " at port " + portNumber + ": " + e.ToString());
            }

            return false;
        }
    }

    class ProcessServer
    {
        private ILogger logger;
        private string prefix;
        private string baseUri;
        private ProcessService parent;
        private HttpServer server;

        public ProcessServer(string relativePrefix, HttpServer s, ProcessService p)
        {
            parent = p;
            server = s;
            logger = server.Logger;
            server.Register(relativePrefix, HandlePutRequest, HandleGetRequest);
            prefix = server.Prefix + relativePrefix;
            baseUri = server.BaseURI + relativePrefix;
        }

        public string BaseURI { get { return baseUri; } }

        private int ProcessIdFromURI(Uri u)
        {
            var p = u.AbsolutePath;
            if (p.StartsWith(prefix))
            {
                var processString = p.Substring(prefix.Length);
                int processId;
                if (!int.TryParse(processString, out processId))
                {
                    if (processString == "shutdown")
                    {
                        return -2;
                    }
                    else
                    {
                        return -1;
                    }
                }
                return processId;
            }
            else
            {
                // This shouldn't happen because the HttpListener was configured to
                // only accept the prefix, so we shouldn't need to handle the exception
                throw new NotImplementedException("Unknown URI " + p);
            }
        }

        private async Task CreateProcess(IHttpContext context, int processId)
        {
            logger.Log("Starting create for process " + processId);

            string error;
            try
            {
                string commandLine;
                string arguments;

                using (var sr = new System.IO.StreamReader(context.Request.InputStream))
                {
                    commandLine = sr.ReadLine();
                    arguments = sr.ReadLine();
                }
                logger.Log("Received create for process " + processId + " cmdline: " + commandLine + " arguments: " + arguments);

                if (parent.Create(processId))
                {
                    logger.Log("Added process entry for " + processId);
                    await server.ReportSuccess(context, true);
                }
                else
                {
                    logger.Log("Process entry for " + processId + " already exists");
                    await server.ReportError(context, HttpStatusCode.Conflict, "Process " + processId + " already exists");
                }

                parent.Launch(processId, commandLine, arguments);

                return;
            }
            catch (Exception e)
            {
                logger.Log("Failed reading create commandline for " + processId);
                error = "Request failed: " + e.Message;
            }

            await server.ReportError(context, HttpStatusCode.BadRequest, error);
        }

        private async Task SetValue(IHttpContext context, int processId)
        {
            logger.Log("starting setvalue");

            var req = context.Request;

            var keyString = req.QueryString["key"];
            if (keyString == null)
            {
                logger.Log("setvalue failed no key specified");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No key specified");
                return;
            }

            var shortStatusString = req.QueryString["shortstatus"];
            if (shortStatusString == null)
            {
                logger.Log("setvalue failed no shortstatus specified");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No shortstatus specified");
                return;
            }

            var notifyWaitersString = req.QueryString["notifywaiters"];
            if (notifyWaitersString == null)
            {
                logger.Log("setvalue failed no notifywaiters specified");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No notifywaiters specified");
                return;
            }

            bool notifyWaiters = (notifyWaitersString == "true");

            string error;

            try
            {
                byte[] bits = null;

                using (MemoryStream ms = new MemoryStream())
                {
                    await context.Request.InputStream.CopyToAsync(ms);
                    bits = ms.ToArray();
                }

                logger.Log("Received value for process " + processId + " length " + bits.Length);

                if (parent.SetValue(processId, keyString, shortStatusString, bits, notifyWaiters))
                {
                    logger.Log("setvalue succeeded for process " + processId + " key " + keyString);
                    await server.ReportSuccess(context, true);
                }
                else
                {
                    logger.Log("setvalue", "setvalue failed no process " + processId);
                    await server.ReportError(context, HttpStatusCode.BadRequest, "No process " + processId);
                }

                return;
            }
            catch (Exception e)
            {
                logger.Log("setvalue failed reading request: " + e.ToString());
                error = "Request failed: " + e.Message;
            }

            await server.ReportError(context, HttpStatusCode.BadRequest, error);
        }

        private async Task Kill(IHttpContext context, int processId)
        {
            logger.Log("starting kill");

            var req = context.Request;

            parent.Kill(processId);

            logger.Log("kill succeeded for process " + processId);
            
            await server.ReportSuccess(context, true);
        }

        private async Task HandlePutRequest(IHttpContext context)
        {
            logger.Log("put entered");
            var req = context.Request;

            var u = req.Url;
            int processId = ProcessIdFromURI(u);
            if (processId < 0)
            {
                if (processId == -2)
                {
                    await server.ReportSuccess(context, true);
                    logger.Log("shutting down");
                    var dontBlock = Task.Run(() => parent.ShutDown());
                    return;
                }
                else
                {
                    logger.Log("bad process id");
                    await server.ReportError(context, HttpStatusCode.BadRequest, "Malformed process ID URI");
                    return;
                }
            }

            var opString = req.QueryString["op"];
            if (opString == null)
            {
                logger.Log("no op");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No op specified");
                return;
            }

            if (opString == "create")
            {
                logger.Log("put", "starting create");
                await CreateProcess(context, processId);
            }
            else if (opString == "setstatus")
            {
                logger.Log("put", "starting setvalue");
                await SetValue(context, processId);
            }
            else if (opString == "kill")
            {
                logger.Log("put", "starting kill");
                await Kill(context, processId);
            }
            else
            {
                logger.Log("put", "bad op {0}", opString);
                await server.ReportError(context, HttpStatusCode.BadRequest, "Unknown op specified: " + opString);
            }
        }

        private async Task HandleGetRequest(IHttpContext context)
        {
            logger.Log("get entered");

            var req = context.Request;

            var u = req.Url;
            int processId = ProcessIdFromURI(u);
            if (processId < 0)
            {
                logger.Log("bad process id");
                await server.ReportError(context, HttpStatusCode.BadRequest, "Malformed process ID URI");
                return;
            }

            // this can be null, meaning we are just blocking on state change of the process
            var keyString = req.QueryString["key"];
            if (keyString == null)
            {
                keyString = Constants.NullKeyString;
            }

            var timeoutString = req.QueryString["timeout"];
            if (timeoutString == null)
            {
                logger.Log("no timeout");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No timeout specified");
                return;
            }

            int timeout;
            if (!Int32.TryParse(timeoutString, out timeout))
            {
                logger.Log("bad timeout");
                await server.ReportError(context, HttpStatusCode.BadRequest, "Malformed timeout specified");
                return;
            }

            var versionString = req.QueryString["version"];
            if (timeoutString == null)
            {
                logger.Log("no version");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No version specified");
                return;
            }

            UInt64 version;
            if (!UInt64.TryParse(versionString, out version))
            {
                logger.Log("bad version");
                await server.ReportError(context, HttpStatusCode.BadRequest, "Malformed version specified");
                return;
            }

            logger.Log("looking up version " + version + " for key '" + keyString + "' timeout " + timeout);
            ValueVersion valueState = await parent.BlockOnStatus(processId, keyString, version, timeout);
            if (valueState == null)
            {
                logger.Log("get", "no such process");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No such process " + processId);
                return;
            }
            else
            {
                logger.Log(String.Format("process {0} key {1} version {2} status {3} exitcode {4} value length {5}",
                    processId, keyString, valueState.version, valueState.processStatus, valueState.exitCode, valueState.value.Length));
                var resp = context.Response;

                resp.Headers.Add(Constants.HttpHeaderValueVersion, valueState.version.ToString());
                resp.Headers.Add(Constants.HttpHeaderValueStatus, valueState.shortStatus);
                resp.Headers.Add(Constants.HttpHeaderProcessStatus, valueState.processStatus);
                resp.Headers.Add(Constants.HttpHeaderProcessExitCode, valueState.exitCode.ToString());
                resp.Headers.Add(Constants.HttpHeaderProcessStartTime, valueState.startTime.ToString());
                resp.Headers.Add(Constants.HttpHeaderProcessStopTime, valueState.stopTime.ToString());

                try
                {
                    await server.ReportSuccess(context, false);

                    await resp.OutputStream.WriteAsync(valueState.value, 0, valueState.value.Length);

                    await context.Response.CloseAsync();
                }
                catch (IOException e)
                {
                    logger.Log(String.Format("Got IOException returning get 0x{0:X}", e.HResult));
                }
            }
        }
    }

    class FileServer
    {
        private ILogger logger;
        private string prefix;
        private string baseUri;
        private ProcessService parent;
        private HttpServer server;

        public FileServer(string relativePrefix, HttpServer s, ProcessService p)
        {
            parent = p;
            server = s;
            logger = server.Logger;
            server.Register(relativePrefix, HandlePutRequest, HandleGetRequest);
            prefix = server.Prefix + relativePrefix;
            baseUri = server.BaseURI + relativePrefix;
        }

        public string BaseURI { get { return baseUri; } }

        protected string NameFromURI(Uri u)
        {
            var p = u.AbsolutePath;
            if (p.StartsWith(prefix))
            {
                return p.Substring(prefix.Length);
            }
            else
            {
                // This shouldn't happen because the HttpListener was configured to
                // only accept the prefix, so we shouldn't need to handle the exception
                throw new NotImplementedException("Unknown URI " + p);
            }
        }

        private async Task Upload(IHttpContext context)
        {
            IHttpRequest req = context.Request;

            logger.Log("upload entered " + req.Url.AbsoluteUri);

            string srcDirectory = NameFromURI(req.Url);

            string destination = req.QueryString["dstDirectory"];
            if (destination == null)
            {
                logger.Log("Upload: no destination");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No dstDirectory specified");
                return;
            }

            Uri dstUri = null;
            try
            {
                dstUri = new Uri(destination);
            }
            catch (Exception e)
            {
                logger.Log("Upload: destination bad Uri " + destination + ": " + e.ToString());
            }

            if (dstUri == null)
            {
                await server.ReportError(context, HttpStatusCode.BadRequest, "Malformed dstDirectory specified");
                return;
            }

            string sourceLocation = req.QueryString["srcLocation"];
            if (sourceLocation != null)
            {
                if (sourceLocation == "LOG" && parent.LogDirectory != null)
                {
                    srcDirectory = parent.LogDirectory;
                }
                else
                {
                    logger.Log("Upload: unknown source location: " + sourceLocation);
                    await server.ReportError(context, HttpStatusCode.BadRequest, "Bad source location specified " + sourceLocation);
                    return;
                }
            }

            List<string> sources = new List<string>();
            using (StreamReader sr = new StreamReader(req.InputStream))
            {
                string line;
                while ((line = await sr.ReadLineAsync()) != null)
                {
                    sources.Add(line);
                }
            }

            string error = await parent.Upload(srcDirectory, sources, dstUri);

            if (error == null)
            {
                await server.ReportSuccess(context, true);
            }
            else
            {
                await server.ReportError(context, HttpStatusCode.BadRequest, error);
            }
        }

        protected async Task HandlePutRequest(IHttpContext context)
        {
            IHttpRequest req = context.Request;

            logger.Log("put entered " + req.Url.AbsoluteUri);

            string opString = req.QueryString["op"];
            if (opString == null)
            {
                logger.Log("no op");
                await server.ReportError(context, HttpStatusCode.BadRequest, "No op specified");
                return;
            }

            if (opString == "upload")
            {
                await Upload(context);
            }
        }

        private async Task GetFile(IHttpContext context)
        {
            var req = context.Request;

            var u = req.Url;
            var name = NameFromURI(u);

            logger.Log("Got GET request " + name);

            var offset = long.Parse(req.QueryString["offset"]);
            var length = int.Parse(req.QueryString["length"]);

            using (var fs = new FileStream(name, FileMode.Open, FileAccess.Read))
            {
                long totalLength = fs.Length;
                if (offset > totalLength)
                {
                    throw new ApplicationException("Offset too large: " + offset + ">" + totalLength);
                }
                context.Response.Headers["X-Dryad-StreamTotalLength"] = totalLength.ToString();

                if (totalLength - offset <= (long)length)
                {
                    length = (int)(totalLength - offset);
                    context.Response.Headers["X-Dryad-StreamEof"] = "true";
                }

                fs.Seek(offset, SeekOrigin.Begin);

                int blockSize = Math.Min(2 * 1024 * 1024, length);
                var buffer = new byte[blockSize];

                await server.ReportSuccess(context, false);

                while (length > 0)
                {
                    int blockLength = (length < blockSize) ? length : blockSize;
                    int nRead = await fs.ReadAsync(buffer, 0, blockLength);
                    if (nRead == 0)
                    {
                        throw new ApplicationException("Read returned 0 bytes");
                    }
                    await context.Response.OutputStream.WriteAsync(buffer, 0, nRead);

                    length -= nRead;
                }

                logger.Log("Finished GET request copy " + name);

                await context.Response.CloseAsync();

                logger.Log("Closed GET request " + name);
            }
        }

        protected async Task HandleGetRequest(IHttpContext context)
        {
            HttpStatusCode code = HttpStatusCode.OK;
            string error = null;
            try
            {
                await GetFile(context);
                return;
            }
            catch (FileNotFoundException e)
            {
                code = HttpStatusCode.NotFound;
                error = e.Message;
            }
            catch (Exception e)
            {
                logger.Log("Got GET request exception " + e.ToString());
                code = HttpStatusCode.BadRequest;
                error = e.Message;
            }

            try
            {
                await server.ReportError(context, code, error);
            }
            catch (Exception e)
            {
                logger.Log("Got http response exception: " + e.ToString());
            }
        }
    }
}
