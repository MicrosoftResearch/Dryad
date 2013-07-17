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
using System.Threading;
using System.IO;

namespace Microsoft.Research.DryadLinq.Internal
{
    public class HpcLinqCache
    {
        private const int EvictionPeriod = 10000;

        private static Dictionary<string, CacheRecord> s_cache = new Dictionary<string, CacheRecord>();
        private static bool s_isInitialized = false;
        private static Timer s_evictionTimer;

        public void Initalize()
        {
            lock (s_cache)
            {
                if (!s_isInitialized)
                {
                    s_evictionTimer = new Timer(new TimerCallback(DoEviction),
                                                null,
                                                EvictionPeriod, EvictionPeriod);
                    s_isInitialized = true;
                }
            }
        }

        public static void Add(string key, object val, Type elemType, object factory)
        {
            lock (s_cache)
            {
                if (!s_cache.ContainsKey(key))
                {
                    CacheRecord rec = CacheRecord.Create(val, elemType, factory);
                    s_cache.Add(key, rec);
                }
            }
        }

        public static bool Contains(string key)
        {
            lock (s_cache)
            {
                return s_cache.ContainsKey(key);
            }
        }

        public static bool TryGet(string key, out object val)
        {
            lock (s_cache)
            {
                val = null;
                CacheRecord rec;
                bool found = s_cache.TryGetValue(key, out rec);
                if (found)
                {
                    val = rec.Value;
                    rec.LastAccessed = DateTime.Now;
                    rec.RefCount++;
                }
                return found;
            }
        }

        public static void DecRefCount(string key)
        {
            lock (s_cache)
            {
                CacheRecord rec;
                bool found = s_cache.TryGetValue(key, out rec);
                if (!found)
                {
                    DryadLinqLog.Add("Can't find the cache entry with key {0}.", key);
                }
                else if (rec.RefCount > 0)
                {
                    rec.RefCount--;
                }
                else
                {
                    DryadLinqLog.Add("The reference count of the cache entry {0} is already 0.",
                                   key);
                }
            }
        }

        private unsafe static void DoEviction(object stateInfo)
        {
            while (true)
            {
                try
                {
                    MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
                    memStatus.dwLength = (UInt32)sizeof(MEMORYSTATUSEX);
                    HpcLinqNative.GlobalMemoryStatusEx(ref memStatus);
                    if (HpcLinqNative.GlobalMemoryStatusEx(ref memStatus) &&
                        memStatus.ullAvailPhys < 4 * 1024 * 1024 * 1024UL)
                    {
                        // Perform eviction only when feeling memory pressure
                        lock (s_cache)
                        {
                            var candidates = s_cache.Where(x => x.Value.RefCount == 0);
                            foreach (var rec in candidates)
                            {
                                s_cache.Remove(rec.Key);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    DryadLinqLog.Add("Exception occurred when performing cache eviction: {0}.",
                                   e.Message);
                }
            }
        }

        private abstract class CacheRecord
        {
            private object m_value;
            private DateTime m_lastAccessed;
            private int m_refCount;

            public CacheRecord(object value)
            {
                this.m_value = value;
                this.m_lastAccessed = DateTime.Now;
                this.m_refCount = 0;
            }

            public static CacheRecord
                Create(object value, Type elemType, object factory)
            {
                Type type = typeof(IEnumerable<>).MakeGenericType(elemType);
                return (CacheRecord)Activator.CreateInstance(
                                elemType, new object[] { value, factory });
            }

            public object Value
            {
                get { return this.m_value; }
            }

            public DateTime LastAccessed
            {
                get { return this.m_lastAccessed; }
                set { this.m_lastAccessed = value; }
            }

            public int RefCount
            {
                get { return this.m_refCount; }
                set { this.m_refCount = value; }
            }

            public abstract void Write(NativeBlockStream stream);
        }

        private class CacheRecord<T> : CacheRecord
        {
            private HpcLinqFactory<T> m_factory;

            public CacheRecord(object value, object factory)
                : base(value)
            {
                this.m_factory = (HpcLinqFactory<T>)factory;
            }

            public override void Write(NativeBlockStream stream)
            {
                HpcRecordWriter<T> writer = this.m_factory.MakeWriter(stream);
                IEnumerable<T> elems = (IEnumerable<T>)this.Value;
                foreach (var x in elems)
                {
                    writer.WriteRecordSync(x);
                }
                writer.Close();
            }
        }
    }
}
