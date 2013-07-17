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

#include "DrCommon.h"


// Recursively ensure that the node is larger than its two children
void DryadHeap::DownHeapify(DWORD index)
{
    DWORD swapWith = index;

    // If we have to swap, it will be with the larger of the two children
    // Find the largest child.  The right child may not exist.
    if (Exists(RightChild(index)) && m_entries[RightChild(index)]->IsHigherPriorityThan(m_entries[LeftChild(index)]))
        swapWith = RightChild(index);
    else if (Exists(LeftChild(index)))
        swapWith = LeftChild(index);

    // Swap and recurse if necessary
    if (swapWith != index)
    {
        if (m_entries[swapWith]->IsHigherPriorityThan(m_entries[index]))
        {
            // Child is higher priority, which violates heap rule, so swap
            HeapSwap(swapWith, index);
            DownHeapify(swapWith);
        }
    }
}

void DryadHeap::GrowHeap()
{
    size_t newHeapSize = m_heapAllocSize * 2 + c_heapGrowAmount;

    DryadHeapItem **newHeap = new DryadHeapItem* [newHeapSize];
    LogAssert(newHeap != NULL);

    if (m_entries != NULL)
    {
        memcpy(newHeap, m_entries, m_heapAllocSize * sizeof(m_entries[0]));
        delete[] m_entries;
    }

    m_entries = newHeap;
    m_heapAllocSize = newHeapSize;
}

// Insert the given value into the heap
void DryadHeap::InsertHeapEntry(DryadHeapItem *entry)
{
    DWORD insertLoc = m_numEntries+1;
    DWORD parent;

    if (insertLoc >= m_heapAllocSize)
        GrowHeap();

    // While we are not the root, and inserting at our current position would make us > the parent
    // (which violates the heap rule)...
    while (Exists((parent = ParentOf(insertLoc))) && entry->IsHigherPriorityThan(m_entries[parent]))
    {
        // Copy the parent into the location we would have inserted
        // Now plan to insert in the parent's location
        m_entries[insertLoc] = m_entries[parent];
        m_entries[insertLoc]->m_heapIndex = insertLoc;
        insertLoc = parent;
    }

    m_entries[insertLoc] = entry;
    m_entries[insertLoc]->m_heapIndex = insertLoc;
    m_numEntries++;
}

// Peek at the first item in the heap (does not remove it)
// Returns NULL if the heap is empty
DryadHeapItem *DryadHeap::PeekHeapRoot()
{
    if (m_numEntries == 0)
        return NULL;

    return m_entries[1];
}

// Extract the first item from the heap
// Returns NULL if the heap is empty
DryadHeapItem *DryadHeap::DequeueHeapRoot()
{
    if (m_numEntries == 0)
        return NULL;

    DryadHeapItem *value = m_entries[1];

    // Move last node to top
    m_entries[1] = m_entries[m_numEntries];
    m_entries[1]->m_heapIndex = 1;
    m_numEntries--;
    DownHeapify(1);

    return value;
}

// Preserves heap property by moving item up or down as appropriate
void DryadHeap::Heapify(DWORD index)
{
    DWORD parent = ParentOf(index);

    // If the node has a parent, but is out of order with respect to the parent, then swap with it
    if (Exists(parent) && m_entries[index]->IsHigherPriorityThan(m_entries[parent]))
    {
        // Heap compare says node's value is such that it should be extracted before parent
        HeapSwap(index, parent);
        UpHeapify(parent);
    }
    else
    {
        // Didn't violate anything in the up direction, but may violate in the down direction
        DownHeapify(index);
    }
}

// Bubble up the entry starting at the given index, if necessary
void DryadHeap::UpHeapify(DWORD index)
{
    DWORD parent = ParentOf(index);

    // If the node has a parent, but is out of order with respect to the parent, then swap with it
    if (Exists(parent) && m_entries[index]->IsHigherPriorityThan(m_entries[parent]))
    {
        HeapSwap(index, parent);
        UpHeapify(parent);
    }
}

// Remove the entry at the given heap position
// To do this, swap the last entry in the heap into the position we want to remove
void DryadHeap::RemoveHeapEntry(DWORD position)
{
    if (position > m_numEntries)
        return;

    if (position == m_numEntries)
    {
        // Entry to remove was already the last entry in the list
        m_numEntries--;
    }
    else
    {
        HeapSwap(position, m_numEntries);
        m_numEntries--;

        // Make sure we haven't violated the heap in either the up or down direction
        Heapify(position);
    }
}


void DryadHeap::HeapSwap(DWORD index1, DWORD index2)
{
    DryadHeapItem *temp = m_entries[index1]; 
    m_entries[index1] = m_entries[index2];
    m_entries[index2] = temp; 
    m_entries[index1]->m_heapIndex = index1; 
    m_entries[index2]->m_heapIndex = index2; 
}


