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

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Security.Principal;
    using System.Security.AccessControl;
    using Microsoft.Research.Dryad;
    
    internal static class ExecutionHelper
    {
        /// <summary>
        /// List of known files in bin directory
        /// </summary>
        private static List<string> binFileList = new List<string>(10);

        /// <summary>
        /// %DRYAD_HOME% bin directory where installed binaries can be found
        /// </summary>
        private static string dryadHome = Environment.GetEnvironmentVariable("DRYAD_HOME");

        /// <summary>
        /// Lockable object used to help make this class thread safe.
        /// </summary>
        private static object initializeLock = new object();

        /// <summary>
        /// Used to initialize known file list
        /// </summary>
        private static void InitializeBinFileList()
        {
            if (binFileList.Count == 0)
            {
                lock (initializeLock)
                {
                    if (binFileList.Count == 0)
                    {
                        binFileList.Add("VertexHost.exe");
                        binFileList.Add("Microsoft.Research.Dryad.dll");
                        binFileList.Add("DryadLINQNativeChannels.dll");
                        binFileList.Add("YarnQueryNativeClusterAdapter.dll");
                        binFileList.Add("Microsoft.Research.Dryad.dll");
                        binFileList.Add("Microsoft.Research.DryadLINQ.dll");
                        binFileList.Add("Microsoft.Research.Dryad.Hdfs.dll");
                    }
                }
            }
        }

        /// <summary>
        /// Check for azure execution
        /// </summary>
        private static bool AzureExecution
        {
            get
            {
                string debugAzure = Environment.GetEnvironmentVariable(Constants.debugAzure);
                if (!String.IsNullOrEmpty(debugAzure))
                {
                    return true;
                }

                string schedulerType = Environment.GetEnvironmentVariable(Constants.schedulerTypeEnvVar);
                return (!String.IsNullOrEmpty(schedulerType) && schedulerType == Constants.schedulerTypeAzure);
            }
        }

        /// <summary>
        /// Copy local resources from %ccp_home%bin to working directory
        /// todo: Post SP2, we should run directly from %ccp_home%bin rather than performing a local copy.
        /// </summary>
        /// <returns>success = true</returns>
        private static bool CopyLocalBinaries()
        {
            //Console.Error.WriteLine("Copying source files from {0}", dryadHome); //DEBUG
            foreach (string localFile in binFileList)
            {
                // Get path to job working directory in \\hpctemp
                string jobFilePath = Path.Combine(ProcessPathHelper.JobPath, localFile);

                // Only copy files that do not already exist
                //  Avoids overwriting files when a vertex service task fails or finishes on one node 
                //  and a new vertex service task is scheduled on the same node within the same job
                if (!File.Exists(jobFilePath))
                {
                    // Get path to file source in CCP_HOME\bin
                    string sourceFilePath = Path.Combine(dryadHome, localFile);

                    try
                    {
                        File.Copy(sourceFilePath, jobFilePath, true);
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("[ExecutionHelper.CopyLocalResources] Exception copying '{0}' to '{1}': {2}", sourceFilePath, jobFilePath, e.Message);
                        return false;
                    }
                }
            }

            return true;
        }

        private static void GetHdfsFile(string hdfsDir, string fileName, string destFileName)
        {
            if(!hdfsDir.EndsWith("/"))
            {
                hdfsDir = hdfsDir + "/";
            }
            var hdfsDirUri = new Uri(hdfsDir, UriKind.Absolute);
            var hdfsFileUri = new Uri(hdfsDirUri, fileName);
            var builder = new UriBuilder();
            builder.Host = hdfsFileUri.DnsSafeHost;
            builder.Port = Constants.HdfsServiceDefaultHttpPort;
            builder.Path = "webhdfs/v1/" + hdfsFileUri.AbsolutePath.TrimStart('/');
            builder.Query = "op=OPEN";
            Console.WriteLine(builder.Uri);
            var wc = new WebClient();
            wc.DownloadFile(builder.Uri, destFileName);
        }

        /// <summary>
        /// Copy the resources from staging dir to working dir
        /// </summary>
        /// <param name="resources">list of resources supplied by dryadlinq</param>
        /// <returns>success = true</returns>
        private static bool CopyStagedJobResources(string resources)
        {
            if (resources != null)
            {
                if (resources[0] == '@')
                {
                    resources = File.ReadAllText(resources.Substring(1));
                }

                if (resources.EndsWith(","))
                {
                    resources = resources.Substring(0, resources.Length - 1);
                }
                string[] files = resources.Split(',');
                DryadLogger.LogInformation("CopyStagedJobResources", string.Format("Will copy {0} resource files.", files.Length));

                if (files.Length > 1)
                {
                    string source = files[0];
                    for (int i = 1; i < files.Length; i++)
                    {
                        string jobFilePath = Path.Combine(ProcessPathHelper.JobPath, files[i]);

                        //
                        // File may already exist due to local resource copying
                        //
                        if (File.Exists(jobFilePath) == false)
                        {
                            //
                            // If file doesn't exist today, get it from staging location
                            //
                            if(source.StartsWith("hdfs://", StringComparison.InvariantCultureIgnoreCase))
                            {
                                // copy from HDFS
                                DryadLogger.LogDebug("CopyStagedJobResources", string.Format(
                                       "[ExecutionHelper.CopyJobResources] Copying '{0}' to '{1}' from HDFS dir {2}",
                                       files[i], jobFilePath, source));
                                GetHdfsFile(source, files[i], jobFilePath);
                            }
                            else
                            {
                                string sourceFile = Path.Combine(source, files[i]);
                                try
                                {
                                    DryadLogger.LogDebug("CopyStagedJobResources", string.Format(
                                        "[ExecutionHelper.CopyJobResources] Copying '{0}' to '{1}'",
                                        sourceFile, jobFilePath));
                                    File.Copy(sourceFile, jobFilePath);
                                }
                                catch (Exception e)
                                {
                                    DryadLogger.LogInformation("CopyStagedJobResources", string.Format(
                                        "[ExecutionHelper.CopyJobResources] Exception copying '{0}' to '{1}': {2}",
                                        sourceFile, jobFilePath, e.Message));
                                    return false;
                                }
                            }
                        }
                    }
                }
                else
                {
                    Console.Error.WriteLine("[ExecutionHelper.CopyJobResources] invalid XC_RESOURCEFILES length = {0}", files.Length);
                    return false;
                }
            }
            else
            {
                Console.Error.WriteLine("[ExecutionHelper.CopyJobResources] resources = null");
                return false;                 
            }
            return true;
        }

        /// <summary>
        /// Create working directory for vertex
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static bool InitializeForProcessExecution(int id, string resources)
        {
            try
            {
                Directory.CreateDirectory(ProcessPathHelper.ProcessPath(id));
                Console.Error.WriteLine("Created directory: " + ProcessPathHelper.ProcessPath(id));
                return true;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("[ExecutionHelper.InitializeForProcessExecution] Exception: {0}", e.Message);
                Console.Error.WriteLine(e.StackTrace);
                return false;
            }
        }

        /// <summary>
        /// Initialize the job directory for vertex execution
        /// </summary>
        /// <param name="resources">list of DryadLINQ-requested resources</param>
        /// <returns>success/failure</returns>
        public static bool InitializeForJobExecution(string resources)
        {
            try
            {
                //
                // Update list of known local binaries if needed
                //
                InitializeBinFileList();

                ProcessPathHelper.CreateUserWorkingDirectory();

                Directory.CreateDirectory(ProcessPathHelper.JobPath);

                //
                // copy any files that already live locally and may be needed for the job
                //
                bool success = CopyLocalBinaries();

                //
                // copy any user-specified files that haven't already been copied
                //
                success &= CopyStagedJobResources(resources);
                return success;
            }
            catch (Exception e)
            {
                //
                // Write out any errors and return false on exception
                //
                Console.Error.WriteLine("[ExecutionHelper.InitializeForJobExecution] Exception: {0}", e.Message);
                Console.Error.WriteLine(e.StackTrace);
                return false;
            }
        }

        /// <summary>
        /// Checks if resource is one of the binaries already on the compute node
        /// </summary>
        /// <param name="resourceName">name of resource to check</param>
        public static bool IsLocalResource(string resourceName)
        {
            InitializeBinFileList();

            return ((from myfile in binFileList
                    where string.Compare(resourceName, myfile, StringComparison.OrdinalIgnoreCase) == 0
                    select myfile).Count() == 1);
        }
    }
}