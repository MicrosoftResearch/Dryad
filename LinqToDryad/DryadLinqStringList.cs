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
using System.Collections.Specialized;
using System.Linq;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// DryadLinq specific list-of-string that supports a readonly flag.
    /// </summary>
    internal class DryadLinqStringList : IList<string>
    {
        private List<string> _store = new List<string>();
        private bool _isReadOnly = false;
        
        /// <summary>
        /// Creatable only from DryadLinq assembly
        /// </summary>
        internal DryadLinqStringList()
        {
        }

        private void ThrowIfReadOnly()
        {
            if (_isReadOnly)
            {
                throw new NotSupportedException(SR.DryadLinqStringDictionaryReadonly); 
            }
        }

        internal DryadLinqStringList GetImmutableClone()
        {
            DryadLinqStringList clone = new DryadLinqStringList();
            clone._store.AddRange(_store);
            clone._isReadOnly = true;

            return clone;
        }

        public int IndexOf(string item)
        {
            return _store.IndexOf(item);
        }

        public void Insert(int index, string item)
        {
            ThrowIfReadOnly();
            _store.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            ThrowIfReadOnly();
            _store.RemoveAt(index);
        }

        public string this[int index]
        {
            get
            {
                return _store[index];
            }
            set
            {
                ThrowIfReadOnly();
                _store[index] = value;
            }
        }

        public void Add(string item)
        {
            ThrowIfReadOnly();
            _store.Add(item);
        }

        public void Clear()
        {
            ThrowIfReadOnly();
            _store.Clear();
        }

        public bool Contains(string item)
        {
            return _store.Contains(item);
        }

        public void CopyTo(string[] array, int arrayIndex)
        {
            _store.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _store.Count; }
        }

        public bool IsReadOnly
        {
            get { return _isReadOnly; }
        }

        public bool Remove(string item)
        {
            ThrowIfReadOnly();
            return _store.Remove(item);
        }

        public IEnumerator<string> GetEnumerator()
        {
            return _store.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
