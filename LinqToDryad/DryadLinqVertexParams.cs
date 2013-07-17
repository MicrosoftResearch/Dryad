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

//
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.DryadLinq.Internal
{
    public class HpcLinqVertexParams
    {
        private string m_vertexStageName;
        private bool m_useLargeBuffer;
        private bool m_keepInputPortOrder;
        private string m_remoteArch;
        private int _inputArity;
        private int _outputArity;
        private bool m_multiThreading;

        public HpcLinqVertexParams(int inputArity, int outputArity)
        {
            _inputArity = inputArity;
            _outputArity = outputArity;
        }

        public string VertexStageName
        {
            get { return this.m_vertexStageName; }
            set { this.m_vertexStageName = value; }
        }

        public int InputArity
        {
            get { return _inputArity; }
            set { _inputArity = value; }
        }

        public int OutputArity
        {
            get { return _outputArity; }
            set { _outputArity = value; }
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

        public bool KeepInputPortOrder
        {
            get { return this.m_keepInputPortOrder; }
            set { this.m_keepInputPortOrder = value; }
        }

        public bool MultiThreading
        {
            get { return m_multiThreading; }
            set { m_multiThreading = value; }
        }
    }
}
