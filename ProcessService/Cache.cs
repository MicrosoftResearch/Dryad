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
using System.Threading.Tasks;

namespace Microsoft.Research.Dryad.ProcessService
{

    internal class Cache
    {
        // Callback object for appends to IDFs, implemented by the writer classes
        internal interface IAppendCompletion
        {
            // Cache calls Success once the append has been accepted. If the cache
            // is throttling writes, Success will not be called until the throttle
            // has unblocked, so this is the mechanism for throttling and
            // writers need to wait for it.
            void Success();
            // Cache calls Fail if there is an error, typically either because the entry
            // doesn't exist, or because there was a spill write error.
            void Fail(string errorString);
        }

        private class CacheBlock
        {
            // data bytes corresponding to a block of an Intermediate Data File (IDF)
            public byte[] data;
            // prefix of bytes in the array that are valid data
            public Int64 validLength;
            // offset into the IDF where the block begins
            public Int64 offset;
            // linked list pointer
            public CacheBlock next;

            public CacheBlock(Int64 size)
            {
                data = new byte[size];
            }
        }

        private class FreeList
        {
            // size of blocks in that we are cacheing
            public Int64 blockSize;
            // head of the list of unused cache blocks
            public CacheBlock head;

            public FreeList(Int64 size)
            {
                blockSize = size;
            }
        }

        private class CacheEntry
        {
            // total length of the IDF being cached
            Int64 length;
            // linked list of blocks corresponding to the IDF if it is held in memory,
            // null otherwise. These are freed as the file is read, so in general
            // correspond to a suffix of the file
            CacheBlock blockHead;
            CacheBlock blockTail;
            // name of the spill file if the IDF has been partially or completely spilled to disk, null otherwise
            string fileName;
            // offset in the disk file where this IDF begins
            Int64 fileOffset;
            // the file may only have a suffix spilled to disk, and may be in the process
            // of being spilled. The range of bytes of the IDF that can be read from the file
            // is [fileDataStart,fileDataStart+fileDataWritten). If there are bytes that have
            // not yet been written then they should still be present as blocks in the blockList.
            Int64 fileDataStart;
            Int64 fileDataWritten;
            // flag indicating if a spilling write is currently in progress, i.e. whether or not
            // there is a region of the spill file beyond fileDataWritten that is in use
            bool writingFile;

            public bool InMemory { get { return (fileName == null); } }

            // Add data to the current cache entry, using blocks from freeList if possible and allocating
            // new blocks otherwise
            public Task AppendInMemory(System.IO.Stream data, Int64 offset, Int64 dataLength, FreeList freeList)
            {
                Task<Int64> finalTask = null;

                Int64 stashed = 0;
                // we are willing to use a block from the free list if it's going to be at least 90%
                // full, i.e. data*10 >= blockSize*9
                Int64 blockSizeX9 = freeList.blockSize * 9;

                if (offset != length)
                {
                    throw new InvalidOperationException("Offset " + offset + " does not match current length " + length);
                }

                if (fileName != null)
                {
                    throw new InvalidOperationException("Can't append in memory since already spilling");
                }

                while (stashed < dataLength)
                {
                    CacheBlock b = null;
                    Int64 needed = dataLength - stashed;
                    Int64 neededX10 = needed * 10;
                    bool largeEnough = (neededX10 >= blockSizeX9);

                    if (largeEnough) // we aren't dealing with a small data fragment
                    {
                        lock (freeList)
                        {
                            if (freeList.head != null)
                            {
                                // Remove a block from the free list
                                b = freeList.head;
                                freeList.head = b.next;
                            }
                        }

                        if (b == null) // The list was empty so make a new block
                        {
                            b = new CacheBlock(freeList.blockSize);
                        }
                    }
                    else // we are dealing with a small data fragment
                    {
                        b = new CacheBlock(needed);
                    }

                    // Add the block to the tail of the cache entry
                    if (blockHead == null)
                    {
                        blockHead = b;
                        blockTail = b;
                    }
                    else
                    {
                        blockTail.next = b;
                        blockTail = b;
                    }
                    b.next = null;

                    if (finalTask == null)
                    {
                        finalTask = Task<Int64>.Factory.StartNew((obj) =>
                            {
                                var cb = (CacheBlock)obj;
                                Int64 nRead = data.ReadAsync(cb.data, 0, cb.data.Length).Result;
                                cb.validLength = nRead;
                                return nRead;
                            },
                            b,
                            TaskCreationOptions.AttachedToParent);
                    }
                    else
                    {
                        finalTask = finalTask.ContinueWith<Int64>((previousTask, obj) =>
                            {
                                var cb = (CacheBlock)obj;
                                Int64 nRead = data.ReadAsync(cb.data, 0, cb.data.Length).Result;
                                cb.validLength = nRead;
                                return previousTask.Result + nRead;
                            },
                            b,
                            TaskContinuationOptions.AttachedToParent);
                    }
                    Int64 toCopy = Math.Max(b.data.LongLength, needed);
                    stashed += toCopy;
                }

                return finalTask.ContinueWith(previousTask => length += previousTask.Result);
            }

            public Task AppendSpilling(System.IO.Stream data, Int64 offset)
            {
                return Task.Factory.StartNew(() => { });
            }
        }

        private class Entries
        {
            // all entries currently being managed. key is the URI by which an entry is referenced
            public Dictionary<string, CacheEntry> data;
            // total size of data cached in RAM
            public Int64 cachedDataSize;

            public Entries()
            {
                data = new Dictionary<string, CacheEntry>();
            }
        }

        // the dictionary of IDFs being cached, either in RAM or spill files
        Entries entries;

        // size below which it is permitted to cache more data
        Int64 cacheHighWatermark;
        // size to spill down to when spilling is required
        Int64 cacheLowWatermark;

        // all active spill files. The key is the filename, the value is a reference count of the
        // number of active IDFs in the spill file
        Dictionary<string, int> spillFile;

        // free list of unused cache blocks
        FreeList freeList;

        public Cache(Int64 lowWaterMark, Int64 highWaterMark, Int64 blockSize)
        {
            entries = new Entries();
            spillFile = new Dictionary<string, int>();

            cacheLowWatermark = lowWaterMark;
            cacheHighWatermark = highWaterMark;

            freeList = new FreeList(blockSize);
        }

        public bool CreateEntry(string name)
        {
            CacheEntry e = new CacheEntry();
            lock (entries)
            {
                if (entries.data.ContainsKey(name))
                {
                    return false;
                }
                else
                {
                    entries.data.Add(name, e);
                    return true;
                }
            }
        }

        public Task AppendEntry(string name, System.IO.Stream data, Int64 offset, Int64 length)
        {
            bool spaceExists = false;
            CacheEntry e = null;

            lock (entries)
            {
                bool exists = entries.data.TryGetValue(name, out e);
                if (exists)
                {
                    lock (e)
                    {
                        if (e.InMemory && entries.cachedDataSize + length <= cacheHighWatermark)
                        {
                            entries.cachedDataSize += length;
                            spaceExists = true;
                        }
                    }
                }
            }

            if (e != null) // the cache entry existed
            {
                lock (e)
                {
                    if (spaceExists) // there's enough space to cache it
                    {
                        return e.AppendInMemory(data, offset, length, freeList);
                    }
                    else 
                    {
                        return e.AppendSpilling(data, offset);
                    }

                }
            }
            else
            {
                throw new InvalidOperationException("Can't append to unknown IDF " + name);
            }
        }
    }
}
