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

#ifndef __DRYADHEAP_H__
#define __DRYADHEAP_H__


// Heap grows by 2*oldsize + c_heapGrowAmount
static const int c_heapGrowAmount = 100;

class DryadHeapItem
{
public:
    // This is where the item is in the index
    DWORD   m_heapIndex;

    // Returns TRUE if we are supposed to be dequeued before "other"
    virtual bool IsHigherPriorityThan(DryadHeapItem *other) = NULL;
};

// Implements a binary heap priority queue
class DryadHeap
{
public:
    DryadHeap()
    {
        m_entries = NULL;
        m_numEntries = 0;
        m_heapAllocSize = 0;
    }

    // Initialize heap with the initial count of elements
    void    Initialize(int initialCount);

    // Dequeue the heap root; returns NULL if empty
    DryadHeapItem* DequeueHeapRoot();

    // Peek at the heap root without dequeueing; returns NULL if empty
    DryadHeapItem* PeekHeapRoot();

    // Insert a new entry into the heap
    void    InsertHeapEntry(DryadHeapItem *entry);

    // Remove the heap item at the given index
    void    RemoveHeapEntry(DWORD index);

protected:
    void    DownHeapify(DWORD index);
    void    UpHeapify(DWORD index);
    void    Heapify(DWORD index);

    // Increases the size of the heap, when it needs more memory
    void    GrowHeap();

    inline void HeapSwap(DWORD index1, DWORD index2);

    // Return whether an index exists
    // Indices are 1-based, so 1 is the root and m_numEntries is the last entry
    // e.g. ParentOf(1) will return FALSE
    bool Exists(DWORD index)
    {
        return (index > 0 && index <= m_numEntries);
    }

    // Note, ParentOf(root) will return root, so your code may need to check for this
    static DWORD ParentOf(DWORD index)
        {   return ((index) >> 1);  };

    static DWORD LeftChild(DWORD index) 
        {   return ((index) << 1);  };

    static DWORD RightChild(DWORD index) 
        {   return (((index) << 1)+1);  };


public:
    DryadHeapItem**  m_entries;

    // Current # entries
    DWORD   m_numEntries;

    // # entries allocated
    size_t  m_heapAllocSize;
};


#endif  //end if not defined __DRYADHEAP_H__
