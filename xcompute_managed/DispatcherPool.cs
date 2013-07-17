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
    using System.Collections.Generic;
    using System.Linq;
    using System;

    internal class DispatcherPool
    {
        private List<Dispatcher> m_dispatcherTable = new List<Dispatcher>();

        private object m_syncRoot = new object();

        public DispatcherPool()
        {
        }

        public bool Add(Dispatcher d)
        {
            lock (SyncRoot)
            {
                // We never want to have two dispatchers for the same node
                // since we don't support oversubscription for vertex nodes
                Dispatcher dummy = null;
                if (!GetByNodeName(d.NodeName, out dummy))
                {
                    m_dispatcherTable.Add(d);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public void Clear()
        {
            lock (SyncRoot)
            {
                foreach (Dispatcher d in m_dispatcherTable)
                {
                    d.Dispose();
                }
                m_dispatcherTable.Clear();
            }
        }

        public bool GetByNodeName(string node, out Dispatcher d)
        {
            lock (SyncRoot)
            {
                d = m_dispatcherTable.Find(x => String.Compare(x.NodeName, node, StringComparison.OrdinalIgnoreCase) == 0);
            }
            return (d != null);
        }

        public bool GetByTaskId(int taskId, out Dispatcher d)
        {
            lock (SyncRoot)
            {
                d = m_dispatcherTable.Find(x => x.TaskId == taskId);
            }
            return (d != null);
        }

        public bool Remove(Dispatcher d)
        {
            lock (SyncRoot)
            {
                return m_dispatcherTable.Remove(d);
            }
        }

        public bool TryReserveDispatcher(string node, out Dispatcher dispatcher)
        {
            lock (SyncRoot)
            {
                Dispatcher d = null;
                if (GetByNodeName(node, out d))
                {
                    if (d.Reserve())
                    {
                        dispatcher = d;
                        return true;
                    }
                }
            }
            dispatcher = null;
            return false;
        }

        public IEnumerator<Dispatcher> GetEnumerator()
        {
            return m_dispatcherTable.GetEnumerator();
        }

        public int Count
        {
            get 
            {
                lock (SyncRoot)
                {
                    return m_dispatcherTable.Count(x => !x.Faulted);
                }
            }
        }

        public List<string> Nodes
        {
            get 
            {
                List<string> nodes = new List<string>();
                lock (SyncRoot)
                {
                    foreach (Dispatcher d in this)
                    {
                        nodes.Add(d.NodeName);
                    }
                }
                return nodes; 
            }
        }

        public object SyncRoot
        {
            get { return this.m_syncRoot; }
        }

    }
}
