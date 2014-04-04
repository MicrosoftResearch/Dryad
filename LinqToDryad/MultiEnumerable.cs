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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    public interface IMultiEnumerable
    {
        UInt32 NumberOfInputs { get; }
    }

    public interface IMultiEnumerable<T> : IEnumerable<T>
    {
        UInt32 NumberOfInputs { get; }
        IEnumerable<T> this[int index] { get; }
    }

    public interface IMultiEnumerable<T1, T2> : IMultiEnumerable
    {
        IEnumerable<T1> First { get; }
        IEnumerable<T2> Second { get; }
    }

    public interface IMultiEnumerable<T1, T2, T3> : IMultiEnumerable
    {
        IEnumerable<T1> First { get; }
        IEnumerable<T2> Second { get; }
        IEnumerable<T3> Third { get; }
    }

    public class MultiEnumerable<T> : IMultiEnumerable<T>
    {
        private IEnumerable<T>[] m_enumList;

        public MultiEnumerable(IEnumerable<T>[] enumList)
        {
            this.m_enumList = enumList;
        }

        public UInt32 NumberOfInputs
        {
            get { return (UInt32)this.m_enumList.Length; }
        }

        public IEnumerable<T> this[int index]
        {
            get
            {
                if (index < this.m_enumList.Length)
                {
                    return this.m_enumList[index];
                }

                //@@TODO: throw ArgumentOutOfRangeException?
                throw new DryadLinqException(DryadLinqErrorCode.IndexOutOfRange, SR.IndexOutOfRange);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var x in this.m_enumList)
            {
                foreach (var y in x)
                {
                    yield return y;
                }
            }
        }
    }

    public class MultiEnumerable<T1, T2> : IMultiEnumerable<T1, T2>
    {
        private IEnumerable<T1> m_first;
        private IEnumerable<T2> m_second;

        public MultiEnumerable(IEnumerable<T1> first, IEnumerable<T2> second)
        {
            this.m_first = first;
            this.m_second = second;
        }

        public UInt32 NumberOfInputs
        {
            get { return 2; }
        }

        public IEnumerable<T1> First
        {
            get { return this.m_first; }
        }

        public IEnumerable<T2> Second
        {
            get { return this.m_second; }
        }
    }

    public class MultiEnumerable<T1, T2, T3> : IMultiEnumerable<T1, T2, T3>
    {
        private IEnumerable<T1> m_first;
        private IEnumerable<T2> m_second;
        private IEnumerable<T3> m_third;

        public MultiEnumerable(IEnumerable<T1> first, IEnumerable<T2> second, IEnumerable<T3> third)
        {
            this.m_first = first;
            this.m_second = second;
            this.m_third = third;
        }

        public UInt32 NumberOfInputs
        {
            get { return 3; }
        }

        public IEnumerable<T1> First
        {
            get { return this.m_first; }
        }

        public IEnumerable<T2> Second
        {
            get { return this.m_second; }
        }

        public IEnumerable<T3> Third
        {
            get { return this.m_third; }
        }
    }
}
