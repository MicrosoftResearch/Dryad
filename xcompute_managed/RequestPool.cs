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

    internal class RequestPool
    {
        private List<ScheduleProcessRequest> m_processRequestPool = new List<ScheduleProcessRequest>();
        private object m_syncRoot = new object();

        public RequestPool()
        {
        }

        public void Add(ScheduleProcessRequest req)
        {
            lock (SyncRoot)
            {
                m_processRequestPool.Add(req);
            }
        }

        public bool Cancel(int processId)
        {
            lock (SyncRoot)
            {
                foreach (ScheduleProcessRequest r in m_processRequestPool)
                {
                    if (r.Id == processId)
                    {
                        return Remove(r);
                    }
                }
            }
            return false;
        }

        public void Clear()
        {
            lock (SyncRoot)
            {
                m_processRequestPool.Clear();
            }
        }

        public bool Remove(ScheduleProcessRequest req)
        {
            lock (SyncRoot)
            {
                return m_processRequestPool.Remove(req);
            }
        }

        public IEnumerator<ScheduleProcessRequest> GetEnumerator()
        {
            return m_processRequestPool.GetEnumerator();
        }

        public object SyncRoot
        {
            get 
            { 
                return this.m_syncRoot; 
            }
        }

        public int Count
        {
            get 
            {
                lock (SyncRoot)
                {
                    return m_processRequestPool.Count;
                }
            }
        }
    }
}
