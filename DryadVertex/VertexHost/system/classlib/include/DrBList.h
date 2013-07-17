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

/*
 * DrBList
 *
 * Declares a utility class for managing bi-linked listed
 */
 
#ifndef __DRYADBLIST_H__
#define __DRYADBLIST_H__


/*
 * DrBListEntry
 * The list entry class is intentionally opaque. Operations should always
 * be done using the list object. The list entry class should be embedded
 * in the object you want to store, and the DR_GET_CONTAINER macro
 * used to map back to the actual object
 */

class DrBListEntry
{
public:

    //Standard c'tor
    inline DrBListEntry();
    
    //Return TRUE if this entry is in a list.
    //N.B. Remember to use the locking conventions of the list object!
    inline BOOL IsInList();

    //Remove this entry from whatever list its currently in
    //N.B. Remember to use the locking conventions of the list object!
    inline void Remove();
        
private:
    friend class DrBList;
    DrBListEntry * m_pNext, * m_pPrev;
};

/*
 * DrBList
 * Manages a bi-linked list of DrBListEntry's
 */

class DrBList
{
public:

    /*
     * Lifecycle Management
     */

    //Standard c'tor
    inline DrBList();


    /*
     * Inserting entries to list
     */

    //Insert specified entry as head of list
    //Asserts if pEntry is already in an existing list
    inline void InsertAsHead(DrBListEntry * pEntry);
    
    //Insert specified entry as tail of list
    //Asserts if pEntry is already in an existing list
    inline void InsertAsTail(DrBListEntry * pEntry);

    //Insert pNewEntry as the next entry after pCurrentEntry
    //Asserts if pNewEntry is already in an existing list
    inline void InsertAsNext(DrBListEntry * pCurrentEntry, DrBListEntry * pNewEntry);
    
    //Insert pNewEntry as the previoud entry before pCurrentEntry
    //Asserts if pNewEntry is already in an existing list
    inline void InsertAsPrev(DrBListEntry * pCurrentEntry, DrBListEntry * pNewEntry);
    
    //Remove pEntry from a list and transition it to the head of this one
    //pEntry can either be in this list or an unrelated one
    //Asserts if pEntry is not already in an existing list
    inline void TransitionToHead(DrBListEntry * pEntry);
    
    //Remove pEntry from a list and transition it to the tail of this one
    //pEntry can either be in this list or an unrelated one
    //Asserts if pEntry is not already in an existing list
    inline void TransitionToTail(DrBListEntry * pEntry);
    
    //Remove all the entries from pList and transition them to the
    //head of this one. pList must not be this list
    inline void TransitionToHead(DrBList * pList);
    
    //Remove all the entries from pList and transition them to the
    //tail of this one. pList must not be this list
    inline void TransitionToTail(DrBList * pList);


    /*
     * Removing entries from list
     */
    
    //Remove entry from head of list and return it
    //Returns NULL if list is empty
    inline DrBListEntry * RemoveHead();
    
    //Remove entry from tail of list and return it
    //Returns NULL if list is empty
    inline DrBListEntry * RemoveTail();
    
    //Remove specified entry from list
    inline DrBListEntry * Remove(DrBListEntry * pEntry);


    /*
     * Accessing entries in the list
     */
    
    //Scan the list and if pEntry is found return it
    //Returns NULL if pEntry is not found
    inline DrBListEntry * Find(DrBListEntry * pEntry);
    
    //Return the head entry in the list or NULL if list is empty
    inline DrBListEntry * GetHead();
    
    //Return the tail entry in the list or NULL if list is empty
    inline DrBListEntry * GetTail();
    
    //Return the next entry after pEntry, or NULL if pEntry is the tail
    inline DrBListEntry * GetNext(DrBListEntry * pEntry);
    
    //Return the previous entry before pEntry, or NULL if pEntry is the head
    inline DrBListEntry * GetPrev(DrBListEntry * pEntry);
    
    
    /*
     * Retrieving state of list
     */
    
    //Scan the list and count the total number of entries
    inline DWORD CountLinks();
    
    //Return TRUE if the list is empty
    inline BOOL IsEmpty();
    
private:

    DrBListEntry m_dummyEntry;
    
};



/*
 * Inline methods for DrBListEntry 
 */
 
 
DrBListEntry::DrBListEntry()
{
    m_pNext=this;
    m_pPrev=this;
}

BOOL DrBListEntry::IsInList()
{
    return (m_pNext!=this);
}

void DrBListEntry::Remove()
{
    LogAssert(IsInList());

    m_pNext->m_pPrev = m_pPrev;
    m_pPrev->m_pNext = m_pNext;
    m_pNext=this;
    m_pPrev=this;
}

/*
 * Inline methods for DrBList 
 */
 

DrBList::DrBList()
{
    //this space intentionally left blank
}

void DrBList::InsertAsHead(DrBListEntry * pEntry)
{
    LogAssert(pEntry->IsInList()==FALSE);
    
    pEntry->m_pNext=m_dummyEntry.m_pNext;
    pEntry->m_pPrev=&m_dummyEntry;
    m_dummyEntry.m_pNext->m_pPrev=pEntry;
    m_dummyEntry.m_pNext=pEntry;
}

void DrBList::TransitionToHead(DrBListEntry * pEntry)
{
    LogAssert(pEntry->IsInList());
    
    //Pull entry from existing list
    pEntry->m_pNext->m_pPrev = pEntry->m_pPrev;
    pEntry->m_pPrev->m_pNext = pEntry->m_pNext;
    //Insert into this list
    pEntry->m_pNext=m_dummyEntry.m_pNext;
    pEntry->m_pPrev=&m_dummyEntry;
    m_dummyEntry.m_pNext->m_pPrev=pEntry;
    m_dummyEntry.m_pNext=pEntry;
}

void DrBList::TransitionToHead(DrBList * pList)
{
    LogAssert(pList != this);

    if (!pList->IsEmpty())
    {
        DrBListEntry* otherHead = pList->GetHead();
        DrBListEntry* otherTail = pList->GetTail();
        otherTail->m_pNext=m_dummyEntry.m_pNext;
        otherHead->m_pPrev=&m_dummyEntry;
        m_dummyEntry.m_pNext->m_pPrev=otherTail;
        m_dummyEntry.m_pNext=otherHead;
        pList->m_dummyEntry.m_pNext=&(pList->m_dummyEntry);
        pList->m_dummyEntry.m_pPrev=&(pList->m_dummyEntry);
    }
}

void DrBList::InsertAsTail(DrBListEntry * pEntry)
{
    LogAssert(pEntry->IsInList()==FALSE);

    pEntry->m_pNext=&m_dummyEntry;
    pEntry->m_pPrev=m_dummyEntry.m_pPrev;
    m_dummyEntry.m_pPrev->m_pNext=pEntry;
    m_dummyEntry.m_pPrev=pEntry;
}

void DrBList::TransitionToTail(DrBListEntry * pEntry)
{
    LogAssert(pEntry->IsInList());
    //Pull entry from existing list
    pEntry->m_pNext->m_pPrev = pEntry->m_pPrev;
    pEntry->m_pPrev->m_pNext = pEntry->m_pNext;
    //Insert into this list
    pEntry->m_pNext=&m_dummyEntry;
    pEntry->m_pPrev=m_dummyEntry.m_pPrev;
    m_dummyEntry.m_pPrev->m_pNext=pEntry;
    m_dummyEntry.m_pPrev=pEntry;
}

//
// Append list to existing list's tail (uses m_dummyEntry to move links)
//
void DrBList::TransitionToTail(DrBList * pList)
{
    LogAssert(pList != this);

    if (!pList->IsEmpty())
    {
        DrBListEntry* otherHead = pList->GetHead();
        DrBListEntry* otherTail = pList->GetTail();
        
        //
        // link tail's next and head's prev to placeholder's prev
        //
        otherTail->m_pNext=&m_dummyEntry;
        otherHead->m_pPrev=m_dummyEntry.m_pPrev;

        //
        // Link placeholder's prev's next to head
        //
        m_dummyEntry.m_pPrev->m_pNext=otherHead;

        //
        // Link placeholder's prev to tail
        //
        m_dummyEntry.m_pPrev=otherTail;

        //
        // Update placeholder next and previous to self
        //
        pList->m_dummyEntry.m_pNext=&(pList->m_dummyEntry);
        pList->m_dummyEntry.m_pPrev=&(pList->m_dummyEntry);
    }
}


void DrBList::InsertAsNext(DrBListEntry * pCurrentEntry, DrBListEntry * pNewEntry)
{
    LogAssert(pCurrentEntry->IsInList());
    LogAssert(pNewEntry->IsInList()==FALSE);

    pNewEntry->m_pNext=pCurrentEntry->m_pNext;
    pNewEntry->m_pPrev=pCurrentEntry;
    pCurrentEntry->m_pNext->m_pPrev=pNewEntry;
    pCurrentEntry->m_pNext=pNewEntry;
}

void DrBList::InsertAsPrev(DrBListEntry * pCurrentEntry, DrBListEntry * pNewEntry)
{
    LogAssert(pCurrentEntry->IsInList());
    LogAssert(pNewEntry->IsInList()==FALSE);
    
    pNewEntry->m_pNext=pCurrentEntry;
    pNewEntry->m_pPrev=pCurrentEntry->m_pPrev;
    pCurrentEntry->m_pPrev->m_pNext=pNewEntry;
    pCurrentEntry->m_pPrev=pNewEntry;
}

DrBListEntry * DrBList::RemoveHead()
{
    if (m_dummyEntry.m_pNext==&m_dummyEntry)
        return NULL;
    else
        return Remove(m_dummyEntry.m_pNext);
}

DrBListEntry * DrBList::RemoveTail()
{
    if (m_dummyEntry.m_pPrev==&m_dummyEntry)
        return NULL;
    else
        return Remove(m_dummyEntry.m_pPrev);
}

DrBListEntry * DrBList::Remove(DrBListEntry * pEntry)
{
    LogAssert(pEntry->IsInList());
    LogAssert(pEntry!=&m_dummyEntry);

    pEntry->m_pNext->m_pPrev = pEntry->m_pPrev;
    pEntry->m_pPrev->m_pNext = pEntry->m_pNext;
    pEntry->m_pNext=pEntry;
    pEntry->m_pPrev=pEntry;
    return pEntry;
}


DrBListEntry * DrBList::Find(DrBListEntry * pEntry)
{
    LogAssert(pEntry);
    DrBListEntry * pScan=m_dummyEntry.m_pNext;
    while (pScan!=&m_dummyEntry)
    {
        if (pScan==pEntry)
            return pEntry;
        pScan=pScan->m_pNext;
    }
    return NULL;
}

DrBListEntry * DrBList::GetHead()
{
    return (m_dummyEntry.m_pNext==&m_dummyEntry) ? NULL : m_dummyEntry.m_pNext;
}

DrBListEntry * DrBList::GetTail()
{
    return (m_dummyEntry.m_pPrev==&m_dummyEntry) ? NULL : m_dummyEntry.m_pPrev;
}

DrBListEntry * DrBList::GetNext(DrBListEntry * pEntry)
{
    return (pEntry->m_pNext==&m_dummyEntry) ? NULL : pEntry->m_pNext;
}

DrBListEntry * DrBList::GetPrev(DrBListEntry * pEntry)
{
    return (pEntry->m_pPrev==&m_dummyEntry) ? NULL : pEntry->m_pPrev;
}
    
//
// Count the number of links in the list and return count
//
DWORD DrBList::CountLinks()
{
    DWORD dwCount=0;
    DrBListEntry * pScan=m_dummyEntry.m_pNext;
    while (pScan!=&m_dummyEntry)
    {
        dwCount++;
        pScan=pScan->m_pNext;
    }
    return dwCount;
}

BOOL DrBList::IsEmpty()
{
    return (m_dummyEntry.m_pNext==&m_dummyEntry);
}

#endif //end if not defined __DRYADBLIST_H__
