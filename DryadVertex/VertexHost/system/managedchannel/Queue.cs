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
using System.Threading.Tasks;

namespace Microsoft.Research.Dryad.Channel
{
    internal class BufferQueue
    {
        private Queue<Buffer> queue;
        private TaskCompletionSource<Buffer> waiter;

        public BufferQueue()
        {
            queue = new Queue<Buffer>();
            waiter = null;
        }

        public int Enqueue(Buffer b)
        {
            lock (this)
            {
                if (waiter == null)
                {
                    queue.Enqueue(b);
                }
                else
                {
                    waiter.SetResult(b);
                    waiter = null;
                }
                return queue.Count;
            }
        }

        public Task<Buffer> Dequeue()
        {
            lock (this)
            {
                if (queue.Count == 0)
                {
                    waiter = new TaskCompletionSource<Buffer>();
                    // add a continuation to make sure that SetResult doesn't synchronously
                    // execute the caller awaiting the queue
                    return waiter.Task.ContinueWith((t) => t.Result);
                }
                else
                {
                    return Task.FromResult(queue.Dequeue());
                }
            }
        }

        public List<Buffer> Shutdown()
        {
            lock (this)
            {
                if (waiter != null)
                {
                    throw new ApplicationException();
                }

                var list = new List<Buffer>();
                while (queue.Count > 0)
                {
                    list.Add(queue.Dequeue());
                }
                queue = null;
                return list;
            }
        }
    }
}
