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
using System.Text;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// The exception that is thrown by DryadLINQ.
    /// </summary>
    [Serializable]
    public class DryadLinqException : Exception
    {
        private int m_errorCode;

        /// <summary>
        /// Initializes an instance of DryadLinqException with a specified message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public DryadLinqException(string message) : base(message) 
        { 
        }
        
        /// <summary>
        /// Initializes an instance of DryadLinqException with a specified message and an inner exception.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="inner">The inner exception.</param>
        public DryadLinqException(string message, Exception inner) : base(message, inner) 
        { 
        }
        
        /// <summary>
        /// Initializes an instance of DryadLinqException with serialized data.
        /// </summary>
        /// <param name="info">The SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        protected DryadLinqException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info != null)
            {
                this.m_errorCode = int.Parse(info.GetString("ErrorCode"));
            }
        }

        /// <summary>
        /// Initializes an instance of DryadLinqException with a specified error code and message.
        /// </summary>
        /// <param name="errorCode">The error code.</param>
        /// <param name="message">The exception message.</param>
        internal DryadLinqException(int errorCode, string message)
            : base(message)
        {
            this.m_errorCode = errorCode;
        }

        /// <summary>
        /// Initializes an instance of DryadLinqException with a specified error code, an exception 
        /// message, and an inner exception.
        /// </summary>
        /// <param name="errorCode">The error code.</param>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        internal DryadLinqException(int errorCode, string message, Exception innerException)
            : base(message, innerException)
        {
            this.m_errorCode = errorCode;
        }

        /// <summary>
        /// Exception's error code. Maps to values in DryadLinqErrorCode.
        /// </summary>
        public int ErrorCode 
        { 
            get { return this.m_errorCode; } 
        }

        /// <summary>
        /// Creates an instance of DryadLinqException.
        /// </summary>
        /// <param name="errorCode">The error code.</param>
        /// <param name="msg">The error message.</param>
        /// <param name="expr">The current expression.</param>
        /// <returns></returns>
        internal static Exception Create(int errorCode, string msg, Expression expr)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(msg);
            sb.Append(" Expression : ");
            sb.AppendLine(DryadLinqExpression.Summarize(expr, 1));

            return new DryadLinqException(errorCode, sb.ToString());
        }

        /// <summary>
        /// Sets the SerializationInfo with information about the exception.
        /// </summary>
        /// <param name="info">The SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (info != null)
            {
                info.AddValue("ErrorCode", this.m_errorCode);
            }
        }
    }
}
