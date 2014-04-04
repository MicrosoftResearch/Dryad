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
using System.Net;
using System.Text;
using System.Xml.Linq;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese;
using Microsoft.Research.Peloponnese.NotHttpClient;

namespace Microsoft.Research.Dryad.ProcessService
{
    class Service
    {
        static private Guid jobGuid;
        static private string parentAddress;
        static private string groupName;
        static private string identifier;
        static private string hostName;
        static private string rackName;

        static bool GetEnvironmentDetails(ILogger logger)
        {
            parentAddress = Environment.GetEnvironmentVariable(Constants.EnvManagerServerUri);
            if (parentAddress == null)
            {
                jobGuid = Guid.NewGuid();
                groupName = "group";
                identifier = "identifier";
                return true;
            }

            string guidString = Environment.GetEnvironmentVariable(Constants.EnvManagerJobGuid);
            groupName = Environment.GetEnvironmentVariable(Constants.EnvProcessGroup);
            identifier = Environment.GetEnvironmentVariable(Constants.EnvProcessIdentifier);
            hostName = Environment.GetEnvironmentVariable(Constants.EnvProcessHostName);
            if (guidString == null || groupName == null || identifier == null || hostName == null)
            {
                logger.Log("environment variables not present");
                return false;
            }

            try
            {
                jobGuid = Guid.Parse(guidString);
            }
            catch (Exception)
            {
                logger.Log("bad guid string " + guidString);
                return false;
            }

            rackName = Environment.GetEnvironmentVariable(Constants.EnvProcessRackName);
            if (rackName == null)
            {
                rackName = "DefaultRack";
            }

            return true;
        }

        static bool RendezvousWithParent(string processUri, string fileUri, string directory, ILogger logger)
        {
            if (parentAddress == null)
            {
                // we weren't started from Peloponnese so do nothing
                return true;
            }

            NotHttpClient httpClient = new NotHttpClient(false, 1, 10000, logger);

            string status;
            try
            {
                var processDetails = new XElement("ProcessUri");
                processDetails.Value = processUri;

                var fileDetails = new XElement("FileUri");
                fileDetails.Value = fileUri;

                var dirDetails = new XElement("LocalDirectory");
                dirDetails.Value = directory;

                var details = new XElement("ProcessDetails");
                details.SetAttributeValue("hostname", hostName);
                details.SetAttributeValue("rackname", rackName);
                details.Add(processDetails);
                details.Add(fileDetails);
                details.Add(dirDetails);

                status = details.ToString();
            }
            catch (Exception e)
            {
                logger.Log("malformed xml: " + e.ToString());
                return false;
            }

            string registration = String.Format("{0}register?guid={1}&group={2}&identifier={3}",
                parentAddress, jobGuid.ToString(), groupName, identifier);
            IHttpRequest request = httpClient.CreateRequest(registration);
            request.Timeout = 30 * 1000; // if it doesn't respond in 30 seconds we'll throw an exception and quit
            request.Method = "POST";

            try
            {
                using (Stream upload = request.GetRequestStream())
                {
                    using (StreamWriter sw = new StreamWriter(upload))
                    {
                        sw.Write(status);
                    }
                }

                using (IHttpResponse response = request.GetResponse())
                {
                    logger.Log("Server registration succeeded");
                    return true;
                }
            }
            catch (NotHttpException e)
            {
                // if this failed, there's nothing much more we can do
                logger.Log("Command put failed message " + e.Message + " status " + e.Response.StatusCode + " " + e.Response.StatusDescription);
            }
            catch (Exception e)
            {
                logger.Log("Command put failed message " + e.Message);
            }

            return false;
        }

        static void Main(string[] args)
        {
            ILogger logger = new SimpleLogger("processservice.log");
            logger.Log("startup", "-------- Starting Dryad Process Service --------");

            var directory = Directory.GetCurrentDirectory();

            if (!GetEnvironmentDetails(logger))
            {
                logger.Log("Failed to read environment variables");
                logger.Stop();
                return;
            }

            XDocument config;
            try
            {
                string configFile = args[0];
                logger.Log("Opening config file " + configFile);
                config = XDocument.Load(configFile);
                logger.Log("Opened config file " + configFile);
            }
            catch (Exception e)
            {
                logger.Log("Failed to read config file: " + e.ToString());
                logger.Stop();
                return;
            }

            using (var processService = new ProcessService(logger))
            {
                string processUri;
                string fileUri;

                logger.Log("starting process service");
                if (processService.Start(config, groupName, identifier, out processUri, out fileUri))
                {
                    logger.Log("done starting process service");

                    if (!RendezvousWithParent(processUri, fileUri, directory, logger))
                    {
                        processService.ShutDown();
                    }
                }
                else
                {
                    logger.Log("process service start failed");
                }

                logger.Log("waiting for process service to exit");
                processService.WaitForExit();
                logger.Log("process service has exited");
            }

            logger.Log("-------- Stopping Dryad Process Service --------");
            logger.Stop();
        }
    }
}
