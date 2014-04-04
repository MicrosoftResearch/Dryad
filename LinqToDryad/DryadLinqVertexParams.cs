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
    public class DryadLinqVertexParams
    {
        private Int32 m_inputArity;
        private Int32 m_outputArity;
        private UInt32[] m_inputPortCounts;
        private bool[] m_keepInputPortOrder;
        private string m_vertexStageName;
        private bool m_useLargeBuffer;
        private string m_remoteArch;
        private bool m_multiThreading;

        public DryadLinqVertexParams(int inputArity, int outputArity)
        {
            this.m_inputArity = inputArity;
            this.m_outputArity = outputArity;
            this.m_inputPortCounts = new UInt32[inputArity];
            this.m_keepInputPortOrder = new bool[inputArity];
        }

        public void SetInputParams(int index, UInt32 portCount, bool keepPortOrder)
        {
            this.m_inputPortCounts[index] = portCount;
            this.m_keepInputPortOrder[index] = keepPortOrder;
        }

        public string VertexStageName
        {
            get { return this.m_vertexStageName; }
            set { this.m_vertexStageName = value; }
        }

        public int InputArity
        {
            get { return this.m_inputArity; }
            set { this.m_inputArity = value; }
        }

        public int OutputArity
        {
            get { return this.m_outputArity; }
            set { this.m_outputArity = value; }
        }

        public string RemoteArch
        {
            get { return this.m_remoteArch; }
            set { this.m_remoteArch = value; }
        }
        
        public bool UseLargeBuffer
        {
            get { return this.m_useLargeBuffer; }
            set { this.m_useLargeBuffer = value; }
        }

        public bool KeepInputPortOrder(UInt32 index)
        {
            return this.m_keepInputPortOrder[index];
        }

        public UInt32 InputPortCount(UInt32 index)
        {
            return this.m_inputPortCounts[index];
        }

        public bool MultiThreading
        {
            get { return this.m_multiThreading; }
            set { this.m_multiThreading = value; }
        }
    }
}
