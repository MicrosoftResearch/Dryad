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
    /// DryadLinq specific (string,string) dictionary that supports a readonly flag.
    /// </summary>
    internal class DryadLinqStringDictionary : IDictionary<string,string>
    {
        private Dictionary<string,string> _store = new Dictionary<string,string>();
        private bool _isReadOnly = false;
        
        /// <summary>
        /// Creatable only from DryadLinq assembly
        /// </summary>
        internal DryadLinqStringDictionary()
        {

        }

        //this is useful to transfer data to DryadLinqJobSubmission class
        internal Dictionary<string, string> BackingStore
        {
            get { return _store; }
        }

        private void ThrowIfReadOnly()
        {
            if (_isReadOnly)
            {
                throw new NotSupportedException(SR.DryadLinqStringDictionaryReadonly);
            }
        }
        
        public void Add(string key, string value)
        {
            ThrowIfReadOnly();
            _store.Add(key, value);
        }

        public bool ContainsKey(string key)
        {
            return _store.ContainsKey(key);
        }

        public ICollection<string> Keys
        {
            get { return _store.Keys; }
        }

        public bool Remove(string key)
        {
            ThrowIfReadOnly();
            return _store.Remove(key);
        }

        public bool TryGetValue(string key, out string value)
        {
            return _store.TryGetValue(key, out value);
        }

        public ICollection<string> Values
        {
            get { return _store.Keys; }
        }

        public string this[string key]
        {
            get
            {
                return _store[key];
            }
            set
            {
                ThrowIfReadOnly();
                _store[key] = value;
            }
        }

        public void Add(KeyValuePair<string, string> item)
        {
            ThrowIfReadOnly();
            _store.Add(item.Key, item.Value);
        }

        public void Clear()
        {
            ThrowIfReadOnly();
            _store.Clear();
        }

        public bool Contains(KeyValuePair<string, string> item)
        {
            return _store.Contains(item);
        }

        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
        {
            ((IDictionary<string, string>)_store).CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _store.Count; }
        }

        public bool IsReadOnly
        {
            get { return this._isReadOnly; }
        }

        public bool Remove(KeyValuePair<string, string> item)
        {
            ThrowIfReadOnly();
            return ((IDictionary<string, string>)_store).Remove(item);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return _store.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets or sets the collection as read only
        /// </summary>
        internal DryadLinqStringDictionary GetImmutableClone()
        {
            DryadLinqStringDictionary clone = new DryadLinqStringDictionary();
            foreach (var keyValuePair in this._store)
            {
                clone.Add(keyValuePair);
            }

            clone._isReadOnly = true;
            return clone;
        }
    }
}
