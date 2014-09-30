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

namespace Microsoft.Research.DryadLinq.Internal
{
    /// <summary>
    /// Compile-time static parameters for DryadLinq vertex.
    /// </summary>
    public class DryadLinqVertexParams
    {
        private Int32 m_inputArity;
        private Int32 m_outputArity;
        private UInt32[] m_inputPortCounts;
        private bool[] m_keepInputPortOrder;
        private string m_vertexStageName;
        private bool m_useLargeBuffer;
        private string m_remoteArch;

        /// <summary>
        /// Initializes a new instance of the DryadLinqVertexParams class.
        /// </summary>
        /// <param name="inputArity">The number of inputs.</param>
        /// <param name="outputArity">The number of outputs.</param>
        public DryadLinqVertexParams(int inputArity, int outputArity)
        {
            this.m_inputArity = inputArity;
            this.m_outputArity = outputArity;
            this.m_inputPortCounts = new UInt32[inputArity];
            this.m_keepInputPortOrder = new bool[inputArity];
        }

        /// <summary>
        /// Sets the parameters for a specified input.
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <param name="portCount">The number of ports for the given input.</param>
        /// <param name="keepPortOrder">true to preserve the port ordering when reading.</param>
        public void SetInputParams(int index, UInt32 portCount, bool keepPortOrder)
        {
            this.m_inputPortCounts[index] = portCount;
            this.m_keepInputPortOrder[index] = keepPortOrder;
        }

        /// <summary>
        /// Gets and sets a user friendly name for a vertex stage.
        /// </summary>
        public string VertexStageName
        {
            get { return this.m_vertexStageName; }
            set { this.m_vertexStageName = value; }
        }

        /// <summary>
        /// Gets and sets the number of inputs.
        /// </summary>
        public int InputArity
        {
            get { return this.m_inputArity; }
            set { this.m_inputArity = value; }
        }

        /// <summary>
        /// Gets and sets the number of outputs.
        /// </summary>
        public int OutputArity
        {
            get { return this.m_outputArity; }
            set { this.m_outputArity = value; }
        }

        /// <summary>
        /// Gets and sets the arch flavor of the cluster node.
        /// </summary>
        public string RemoteArch
        {
            get { return this.m_remoteArch; }
            set { this.m_remoteArch = value; }
        }
        
        /// <summary>
        /// Gets and sets the buffering policy for output.
        /// </summary>
        public bool UseLargeBuffer
        {
            get { return this.m_useLargeBuffer; }
            set { this.m_useLargeBuffer = value; }
        }

        /// <summary>
        /// Determines if port ordering needs to be preserved for reading from a specified input. 
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <returns>true to preserve port order for a specified input.</returns>
        public bool KeepInputPortOrder(UInt32 index)
        {
            return this.m_keepInputPortOrder[index];
        }

        /// <summary>
        /// Returns the number of ports for a specified input.
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <returns>The number of ports.</returns>
        public UInt32 InputPortCount(UInt32 index)
        {
            return this.m_inputPortCounts[index];
        }
    }
}
