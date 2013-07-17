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
//      Utils used by L2H for dealing with Azure
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;

    public class AzureUtils
    {
        /// <summary>
        /// Flag to denote whether process is on Azure or not
        /// </summary>
        private static bool? isOnAzure = null;

        /// <summary>
        /// Name of HPC node
        /// </summary>
        private static string hostName = null;

        /// <summary>
        /// Determine whether current process on Azure or not
        /// </summary>
        public static bool IsOnAzure
        {
            get
            {
                if (isOnAzure == null)
                {
                    isOnAzure = Environment.GetEnvironmentVariable("CCP_ONAZURE") != null;
                }

                return (bool)isOnAzure;
            }
        }

        /// <summary>
        /// Returns name of node or HPC alias if in Azure
        /// </summary>
        /// <returns>name to use for current node</returns>
        public static string CurrentHostName
        {
            get
            {
                if(string.IsNullOrEmpty(hostName))
                {
                    if (Microsoft.Research.Dryad.AzureUtils.IsOnAzure)
                    {
                        hostName = Environment.GetEnvironmentVariable(@"HPC_NODE_NAME");
                        if (string.IsNullOrEmpty(hostName))
                        {
                            throw new Exception("Unable to get HPC_NODE_NAME environment variable");
                        }
                    }
                    else
                    {
                        hostName = Environment.MachineName;
                    }
                }

                return hostName;
            }
        }

        /// <summary>
        /// This needs to be set in the Azure bootstrapper code.
        /// </summary>

        internal static bool IsDatabaseShared
        {
            get
            {
                if (AzureUtils.IsOnAzure)
                {
                    return (Environment.GetEnvironmentVariable(@"HPC_SHARED_DATABASE", EnvironmentVariableTarget.Machine) != null);
                }

                return false;
            }
        }
    }
}