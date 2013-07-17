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

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Threading;
    using System.Collections.Generic;

    // Abstraction to facilitate moving process table to, e.g., SQL in the future
    internal class ProcessTable
    {
        private Dictionary<int, XComputeProcess> processTable;
        private object tableLock = new object();

        public ProcessTable()
        {
            processTable = new Dictionary<int, XComputeProcess>();
        }

        public void Add(int id, XComputeProcess proc)
        {
            lock (tableLock)
            {
                this.processTable.Add(id, proc);
            }
        }

        public bool ContainsKey(int id)
        {
            lock (tableLock)
            {
                return this.processTable.ContainsKey(id);
            }
        }

        public Dictionary<int, XComputeProcess>.Enumerator GetEnumerator()
        {
            return this.processTable.GetEnumerator();
        }

        public void Remove(int id)
        {
            lock (tableLock)
            {
                if (this.processTable.ContainsKey(id))
                {
                    this.processTable[id].Dispose();
                    this.processTable.Remove(id);
                }
            }
        }

        public bool TryGetValue(int id, out XComputeProcess proc)
        {
            return this.processTable.TryGetValue(id, out proc);
        }

        public XComputeProcess this[int id]
        {
            get
            {
                lock (tableLock)
                {
                    if (this.processTable.ContainsKey(id))
                    {
                        return this.processTable[id];
                    }
                    else
                    {
                        throw new ArgumentException(String.Format("Process ID {0} not found in process table", id));
                    }
                }
            }

            set
            {
                lock (tableLock)
                {
                    this.processTable[id] = value;
                }
            }
        }

        public object SyncRoot
        {
            get { return this.tableLock; }
        }


    }
}
