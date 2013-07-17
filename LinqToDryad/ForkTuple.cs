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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    interface IForkValue
    {
        bool HasValue { get; }
        object Value { get; }
    }

    [Serializable]
    public struct ForkValue<T> : IForkValue
    {
        private T m_x;
        private bool m_hasX;

        public ForkValue(T x)
        {
            this.m_x = x;
            this.m_hasX = true;
        }
        
        public bool HasValue
        {
            get { return this.m_hasX; }
        }

        object IForkValue.Value
        {
            get { return this.m_x; }
        }
                
        public T Value
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }
    }

    [Serializable]    
    public struct ForkTuple<T1, T2>
    {
        private T1 m_x;
        private T2 m_y;
        private bool m_hasX;
        private bool m_hasY;        

        public ForkTuple(T1 x, T2 y)
        {
            this.m_x = x;
            this.m_y = y;
            this.m_hasX = true;
            this.m_hasY = true;
        }
        
        public bool HasFirst
        {
            get { return this.m_hasX; }
        }

        public bool HasSecond
        {
            get { return this.m_hasY; }
        }
                
        public T1 First
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }

        public T2 Second
        {
            get { return this.m_y; }
            set { this.m_y = value; this.m_hasY = true; }
        }
    }

    [Serializable]    
    public struct ForkTuple<T1, T2, T3>
    {
        private T1 m_x;
        private T2 m_y;
        private T3 m_z;
        private bool m_hasX;
        private bool m_hasY;        
        private bool m_hasZ;
        
        public ForkTuple(T1 x, T2 y, T3 z)
        {
            this.m_x = x;
            this.m_y = y;
            this.m_z = z;
            this.m_hasX = true;
            this.m_hasY = true;
            this.m_hasZ = true;
        }
        
        public bool HasFirst
        {
            get { return this.m_hasX; }
        }

        public bool HasSecond
        {
            get { return this.m_hasY; }
        }

        public bool HasThird
        {
            get { return this.m_hasZ; }
        }
        
        public T1 First
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }

        public T2 Second
        {
            get { return this.m_y; }
            set { this.m_y = value; this.m_hasY = true; }
        }

        public T3 Third
        {
            get { return this.m_z; }
            set { this.m_z = value; this.m_hasZ = true; }
        }
    }
    
    internal struct ForkTuple
    {
        private IForkValue[] m_values;

        public ForkTuple(params IForkValue[] values)
        {
            this.m_values = values;
        }
                
        public bool HasValue(int index)
        {
            return this.m_values[index].HasValue;
        }

        public object Value(int index)
        {
            return this.m_values[index].Value;
        }
    }
}
