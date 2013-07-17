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
//      Auth manager used for checking if the caller identity matches with the 
//      current identity.
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.Security.Principal;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.Net;
    using System.Text;

    /// <summary>
    /// Auth manager used for checking if the caller identity matches with the current identity.
    /// </summary>
    public class DryadVertexServiceAuthorizationManager : ServiceAuthorizationManager
    {
        /// <summary>
        /// Reference Identity
        /// </summary>
        WindowsIdentity currentIdentity;

        /// <summary>
        /// Creates an instance of the DryadVertexServiceAuthorizationManager class.
        /// </summary>
        public DryadVertexServiceAuthorizationManager()
        {
            this.currentIdentity = WindowsIdentity.GetCurrent();
        }

        /// <summary>
        /// Check whether current operation context should be allowed access
        /// </summary>
        /// <param name="operationContext">Current operation context</param>
        /// <returns>true = allowed</returns>
        protected override bool CheckAccessCore(OperationContext operationContext)
        {
            //TODO: Put logging information to appropriate channels when available.

            //
            // Fail if context is annonymous
            //
            if (operationContext.ServiceSecurityContext.IsAnonymous)
            {
                DryadLogger.LogError(0, null, "Vertex authentication failed : Service security context is anonymous.");
                return false;
            }

            //
            // Get identity used in current context
            //
            WindowsIdentity callerIdentity = operationContext.ServiceSecurityContext.WindowsIdentity;
            if (callerIdentity == null)
            {
                //
                // Fail if identity is not set
                //
                DryadLogger.LogError(0, null, "Vertex authentication failed : Caller identity is null.");
                return false;
            }
            else if (callerIdentity.IsAnonymous)
            {
                //
                // Fail if identity is anonymous
                //
                DryadLogger.LogError(0, null, "Vertex authentication failed : Caller identity is anonymous.");
                return false;
            }
            else if (!callerIdentity.IsAuthenticated)
            {
                //
                // Fail if identity is not authenticated
                //
                DryadLogger.LogError(0, null, "Vertex authentication failed : Caller identity is not authenticated.");
                return false;
            }

            //
            // If operation context has same user as vertex service, then allow, otherwise fail.
            //
            if (this.currentIdentity.User == callerIdentity.User)
            {
                return true;
            }
            else
            {
                DryadLogger.LogError(0, null, "Vertex authentication failed : Current identity is {0}, caller identity is {1}", this.currentIdentity.Name, callerIdentity.Name);
            }

            return false;
        }
    }
}
