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

//------------------------------------------------------------------------------
// <summary>
//      Helper class that builds working directories for dryad jobs
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.IO;
    using System.Security.Principal;
    using System.Security.AccessControl;
    using Microsoft.Hpc.Dsc.Internal;

    /// <summary>
    /// Helper class that builds working directories for dryad jobs
    /// </summary>
    public static class ProcessPathHelper
    {
        /// <summary>
        /// Current job directory path
        /// </summary>
        private static string jobPath = string.Empty;

        /// <summary>
        /// Lockable object used to synchronize job path creation
        /// </summary>
        private static object jobPathLock = new object();

        /// <summary>
        /// Build the current job directory path
        /// </summary>
        private static void BuildJobPath()
        {
            //
            // Get the path to the HpcTemp share on the local machine and append the current user name to build the job path.
            // If unable to get local path to HpcTemp share, or necessary environment variables, leave jobPath as string.empty.
            //
            string rootDir = RootWorkingDirectory;

            if (!string.IsNullOrEmpty(rootDir))
            {
                try
                {
                    string userName = Environment.GetEnvironmentVariable("USERNAME");
                    if (!string.IsNullOrEmpty(userName))
                    {
                        //
                        // If all goes well, assign the path to jobPath, so it can accessed.
                        //
                        ProcessPathHelper.jobPath = Path.Combine(Path.Combine(rootDir, userName), Environment.GetEnvironmentVariable("CCP_JOBID"));
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("ProcessPathHelper.BuildJobPath: Unable to get environment variable: {0}", ex.Message);
                }
            }
            else
            {
                Console.Error.WriteLine("ProcessPathHelper.BuildJobPath: Unable to get local path to {0} share.", Constants.DscTempShare);
            }
        }
        
        /// <summary>
        /// Returns the path to the specified vertex dir
        /// </summary>
        /// <param name="id">Vertex id</param>
        /// <returns>path to vertex directory</returns>
        public static string ProcessPath(int id)
        {
            return Path.Combine(JobPath, id.ToString());
        }

        /// <summary>
        /// Returns path to the specified vertex's working dir
        /// </summary>
        /// <param name="id">Vertex ID</param>
        /// <returns>path to the vertex working directory</returns>
        public static string ProcessWorkingDirectory(int id)
        {
            return Path.Combine(ProcessPath(id), "WD");
        }

        /// <summary>
        /// Gets path to directory for the current job. 
        /// If job path cannot be retrieved, string.empty may be returned.
        /// </summary>
        public static string JobPath
        {
            get
            {
                //
                // If job path hasn't been built, build it.
                // Lock to allow multiple vertices to access this property without issue.
                //
                if (string.IsNullOrEmpty(ProcessPathHelper.jobPath))
                {
                    lock (jobPathLock)
                    {
                        if (string.IsNullOrEmpty(ProcessPathHelper.jobPath))
                        {
                            ProcessPathHelper.BuildJobPath();
                        }
                    }
                }

                return ProcessPathHelper.jobPath;
            }
        }

        /// <summary>
        /// Get the root working directory
        /// </summary>
        public static string RootWorkingDirectory
        {
            get
            {
                return (NetShareWrapper.GetLocalPath("localhost", Constants.DscTempShare));
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static void CreateUserWorkingDirectory()
        {
            //Create user specific working directory if does not exist and set full control to the user and administrators only.

            string userWorkingDirectory = String.Format(@"{0}\{1}", ProcessPathHelper.RootWorkingDirectory, Environment.UserName);

            if (!Directory.Exists(userWorkingDirectory))
            {
                DirectorySecurity directorySecurity = new DirectorySecurity();

                //Add full control to user

                directorySecurity.AddAccessRule(new FileSystemAccessRule(WindowsIdentity.GetCurrent().Name, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));

                //Add full control to administrators

                SecurityIdentifier administratorGroup = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);

                directorySecurity.AddAccessRule(new FileSystemAccessRule(administratorGroup, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));

                Directory.CreateDirectory(userWorkingDirectory, directorySecurity);
            }

        }
    }
}