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

#ifndef __DRLIST_H__
#define __DRLIST_H__

#pragma once

// Simple Growable vector of arbitrary assignable typed values
// Insertions and deletions can be performed both at the head and at the tail of the list, making it suitable for queues.
// The underlying type must either be a simple scalar, a struct/class that is clonable with memcpy, or a class that implements copy constructor and assignment operator
// TODO: This version constructs all objects on reallocation, and does not destruct unused items until the list is destroyed.
// TODO: That is OK for simple types, but can waste space for types that allocate additional storage.
// TODO: to do it correctly, fix this class to use in-place contructor/destructor; construct object when it is added to list, destruct when it is removed.
template<typename t> class DrValList sealed
{
public:    
    DrValList(UInt32 uFirstAllocSize = 20)
    {
        m_nEntries = 0;
        m_uFirstEntry = 0;
        m_nAllocated = 0;
        m_prgEntries = NULL;
        m_uFirstAllocSize = uFirstAllocSize;
    }

    ~DrValList()
    {
        if (m_prgEntries != NULL) {
            delete[] m_prgEntries;
            m_prgEntries = NULL;
        }
        m_nEntries = 0;
        m_nAllocated = 0;
    }

    // forces the buffer to be reallocated with the given size, even if it
    // is the same as the current size.
    // the requested size must be big enough to hold the valid entries.
    // On exit, the valid entries are always contiguous starting at offset 0
    // if n==0, frees the buffer
    void ForceRealloc(UInt32 n)
    {
        LogAssert(n >= m_nEntries);
        t *pnew = NULL;
        if (n != 0) {
            pnew = new t[n];
            LogAssert(pnew != NULL);
            UInt32 uFrom = m_uFirstEntry;
            for (UInt32 i = 0; i < m_nEntries; i++) {
                pnew[i]=m_prgEntries[uFrom++];
                if (uFrom >= m_nAllocated) {
                    uFrom = 0;
                }
            }
        }
        if (m_prgEntries != NULL) {
            delete[] m_prgEntries;
        }
        m_prgEntries = pnew;
        m_nAllocated = n;
        m_uFirstEntry = 0;
    }

    // reallocates the buffer if there are not at least n elements allocated    
    void GrowTo(UInt32 n)
    {
        if (n > m_nAllocated) {
            if (n < 2 * m_nAllocated) {
                n = 2 * m_nAllocated;
            }
            if (n < m_uFirstAllocSize) {
                n = m_uFirstAllocSize;
            }
            ForceRealloc(n);
        }
    }

    // Converts a potentially wrapped list (if you have moved the head) into a
    // contiguous list, and returns a pointer to the first item in the contiguous list
    // If possible, nothing is moved. If the list is wrapped, a new buffer is allocated (easier than moving everything in a full list)
    // This operation is always cheap if you never remove from or add to the head.
    t *MakeContiguous()
    {
        if (m_uFirstEntry + m_nEntries > m_nAllocated) {
            ForceRealloc(m_nEntries);
        }

        return m_prgEntries + m_uFirstEntry;
    }

    UInt32 NumEntries() const
    {
        return m_nEntries;
    }

    bool IsEmpty()
    {
        return m_nEntries == 0;
    }

    t& EntryAt(UInt32 index)
    {
        LogAssert(index < m_nEntries);
        return m_prgEntries[NormalizeEntryIndex(index)];
    }

    const t& EntryAt(UInt32 index) const
    {
        LogAssert(index < m_nEntries);
        return m_prgEntries[NormalizeEntryIndex(index)];
    }

    t& operator[](UInt32 index)
    {
        return EntryAt(index);
    }

    const t& operator[](UInt32 index) const
    {
        return EntryAt(index);
    }

    t& Head()
    {
        LogAssert(m_nEntries != 0);
        return m_prgEntries[m_uFirstEntry];
    }

    const t& Head() const
    {
        LogAssert(m_nEntries != 0);
        return m_prgEntries[m_uFirstEntry];
    }

    t& Tail()
    {
        LogAssert(m_nEntries != 0);
        return EntryAt(m_nEntries-1);
    }

    const t& Tail() const
    {
        LogAssert(m_nEntries != 0);
        return EntryAt(m_nEntries-1);
    }

    // LIFO-style top of stack
    t& TopOfStack()
    {
        return Tail();
    }

    const t& TopOfStack() const
    {
        return Tail();
    }


    // Invalidates all entry references previously returned
    // This is the typical method used to implement Enqueue for FIFO queues, or Push for stacks
    t& AddEntryToTail(const t& val)
    {
        GrowTo(m_nEntries+1);
        t& newEntry = m_prgEntries[NormalizeEntryIndex(m_nEntries++)];
        newEntry = val;
        return newEntry;
    }

    // FIFO-style queueing
    t& Enqueue(const t& val)
    {
        return AddEntryToTail(val);
    }

    // LIFO-style stack push
    t& Push(const t& val)
    {
        return AddEntryToTail(val);
    }

    // Invalidates all entry references previously returned
    t& AddEntryToHead(const t& val)
    {
        GrowTo(m_nEntries+1);
        if (m_uFirstEntry == 0) {
            m_uFirstEntry = m_nAllocated - 1;
        } else {
            m_uFirstEntry--;
        }
        m_nEntries++;
        t& newEntry = m_prgEntries[m_uFirstEntry];
        newEntry = val;
        return newEntry;
    }

    // Invalidates all entry references previously returned
    t& AddEntry(const t& val)
    {
        return AddEntryToTail(val);
    }

    // This is the typical method used to emplement Pop for stacks
    t& RemoveEntryFromTail(__out t *pValOut)
    {
        LogAssert(m_nEntries != 0);
        *pValOut = m_prgEntries[NormalizeEntryIndex(--m_nEntries)];
    }

    t RemoveEntryFromTail()
    {
        LogAssert(m_nEntries != 0);
        // NOTE: following code depends on not destructing the returned value until after we return.
        // if we add in-place destructors, this has to change
        const t& retVal = m_prgEntries[NormalizeEntryIndex(--m_nEntries)];
        return retVal;
    }

    // LIFO-style stack pop
    t Pop()
    {
        return RemoveEntryFromTail();
    }

    t& Pop(__out t *pValOut)
    {
        return RemoveEntryFromTail(pValOut);
    }

    // This is the typical method used to emplement Pop for stacks
    t& RemoveEntryFromHead(__out t *pValOut)
    {
        LogAssert(m_nEntries != 0);
        *pValOut = m_prgEntries[m_uFirstEntry++];
        if (m_uFirstEntry >= m_nAllocated) {
            m_uFirstEntry = 0;
        }
        m_nEntries--;
        return *pValOut;
    }

    t RemoveEntryFromHead()
    {
        LogAssert(m_nEntries != 0);
        // NOTE: following code depends on not destructing the returned value until after we return.
        // if we add in-place destructors, this has to change
        const t& retVal = m_prgEntries[m_uFirstEntry++];
        if (m_uFirstEntry >= m_nAllocated) {
            m_uFirstEntry = 0;
        }
        m_nEntries--;
        return retVal;
    }

    // FIFO-style dequeueing
    t Dequeue()
    {
        return RemoveEntryFromHead();
    }

    t& Dequeue(__out t *pValOut)
    {
        return RemoveEntryFromHead(pValOut);
    }


    // does not shrink the allocated list or destruct existing entries. To do that, use ForceRealloc(0).
    void Clear()
    {
        m_nEntries = 0;
        m_uFirstEntry = 0;
    }


    // returns NULL if not in the list.
    t *FindVal(const t& val)
    {
        for (UInt32 i = 0; i < m_nEntries; i++) {
            t& entry = EntryAt(i);
            if (entry == val) {
                return &entry;
            }
        }
        return NULL;
    }
    
    // returns NULL if not in the list.
    const t *FindVal(const t& val) const
    {
        for (UInt32 i = 0; i < m_nEntries; i++) {
            const t& entry = EntryAt(i);
            if (entry == val) {
                return &entry;
            }
        }
        return NULL;
    }

    bool ContainsVal(const t& val) const
    {
        return FindVal(val) != NULL;
    }
    
    static int __cdecl InternalDrValListEntryPointerCompare(void *context, const void *p1, const void *p2)
    {
        if (**( const t**)p1 > **(const t**)p2) {
            return 1;
        } else if (**( const t**)p1 ==**(const t**)p2) {
            return 0;
        }
        return -1;
    }
    
    // performs a quicksort on the list.
    // To sort, the base type must suport the ">" and "==" operators
    // As a side-effect, truncates the allocated size to the actual size.
    void Sort()
    {
        // We cannot sort directly with C's quicksort, since it uses memmove.
        // So we will sort a pointer list, and then reallocate
        if (m_nEntries != 0)
        {
            const t **rgpEntries = new const t *[m_nEntries];
            LogAssert(rgpEntries != NULL);
            for (UInt32 i = 0; i < m_nEntries; i++) {
                rgpEntries[i] = &(EntryAt(i));
            }
            qsort(rgpEntries, m_nEntries, sizeof(rgpEntries[0]), InternalDrValListEntryPointerCompare);
            // Generally, noone ever sorts when they plan to grow the list, so we can truncate to actual size.
            t *pNew = new t[m_nEntries];
            LogAssert(pNew != NULL);
            for (UInt32 i = 0; i < m_nEntries; i++)
            {
                pNew[i] = *(rgpEntries[i]);
            }
            delete[] m_prgEntries;
            m_prgEntries = pNew;
            m_nAllocated = m_nEntries;
            m_uFirstEntry = 0;
        }
        else
        {
            ForceRealloc(0);
        }
    }

protected:
    UInt32 NormalizeEntryIndex(UInt32 index) const
    {
        LogAssert(index < m_nAllocated);
        if (m_uFirstEntry != 0) {
            index += m_uFirstEntry;
            if (index >= m_nAllocated) {
                index -= m_nAllocated;
            }
        }
        return index;
    }

private:
    UInt32 m_uFirstEntry;             // Normally 0, the index of the first entry (for circular buffers)
    
    UInt32 m_uFirstAllocSize;
    UInt32 m_nEntries;
    UInt32 m_nAllocated;
    t *m_prgEntries;
};



// a List template that only grows :)
// You are recommended to use DrPtrList unless you are aware of the potential complicated issues behind it
// when copying/assigning values. . :)

template <typename T>
class DrList{
private:
    //basically not supported
    void Merge(DrList &other){
        for(unsigned int i = 0; i <other.Size(); i ++){
            Push(other[i]);
        }
    }

public:
    DrList(unsigned int numAlloc = 10){
        Init(numAlloc);
    }
    DrList(DrList &other){
        Init(other.Size());
        Merge(other);
    }
    
    
    ~DrList(){
        FreeElements();     // special processing for freeing each element
        FreeMem();
    }

    bool IsEmpty() const {
        return m_numEntries == 0;
    }

    unsigned int Size() const {
        return m_numEntries;
    }

    void Push(const T &entry){
        AddEntry(entry);
    }

    /*
    T& Pop() const{
        LogAssert(Size());
        m_numEntries --;
        return m_p[m_numEntries];
    }

    T& Last() const{
        LogAssert(Size());
        return m_p[m_numEntries - 1];
    }

    T& First() const{
        LogAssert(Size());
        return m_p[0];
    }
    */

    T& GetEntry(unsigned int i) const {
        LogAssert(i < m_numEntries);

        return m_p[i];
    }

    T& operator[](unsigned int i) const{
        return GetEntry(i);
    }

    /* sample compare function
        int __cdecl compare(const void *p1, const void *p2){
            const T &v1 = *(T *)p1;
            const T &v2 = *(T *)p2;

            return v1 - v2;
        }
      */
      
    void Sort(int (__cdecl *compare )(const void *, const void *)){

        qsort(m_p, m_numEntries, sizeof(T), compare);
    }
    
    void Clear(){
        if(m_numAllocated == m_initNumAllocated && IsEmpty()) return;

        unsigned int numAlloc = m_initNumAllocated;
        FreeElements();
        FreeMem();
        Init(numAlloc);
    }

    // don't call me !!! This function violates some design principle, will be removed shortly
    void MakeAllAllocatedElementsValid(){
        m_numEntries = m_numAllocated;
    }
    
protected:

    unsigned int m_numEntries;
    unsigned int m_numAllocated;
    unsigned int m_initNumAllocated;

    T           *m_p;

    bool IsFull() const {
        return m_numEntries == m_numAllocated;
    }

    void Grow(){
        m_numAllocated *= 2;

        T  *newp = new T[m_numAllocated];

        LogAssert(newp);

        for(unsigned int i = 0; i < m_numEntries; i ++){
            newp[i] = m_p[i];
        }

        FreeMem();      

        m_p = newp;
    }

    void Init(unsigned int numAlloc){
        m_numEntries = 0;
        m_initNumAllocated = m_numAllocated = numAlloc;
        m_p = new T[numAlloc];
        LogAssert(m_p);
    }

    void FreeMem(){
        if(m_p){
            delete[] m_p;
            m_p = 0;
        }
    }

    virtual void FreeElements(){
        /* do nothing */
    }

    void AddEntry(const T &entry){
        if(IsFull()){
            Grow();
        }
        m_p[m_numEntries] = entry;
        m_numEntries ++;
    }

    unsigned int NumAllocated() const {
        return m_numAllocated;
    }

};

template <typename T>
class DrInternalPtrList : public DrList<T *>{
public:
    DrInternalPtrList(unsigned int numAlloc) : DrList<T *>(numAlloc) {}
    // allow pointer expressions to be passed as argument
    void Push(T *ptr){
        DrList<T *>::Push(ptr);          
    }
};

// managed means the pointers give to the list are owned by the list and therefore they will be freed by the list.

template <typename T>
class DrPtrList : public DrInternalPtrList<T>{
public:
    DrPtrList(unsigned int numAlloc = 10) : DrInternalPtrList<T>(numAlloc) {}
};

template <typename T>
class DrUnmanagedPtrList : public DrInternalPtrList<T>{
public:
    DrUnmanagedPtrList(unsigned int numAlloc = 10) : DrInternalPtrList<T>(numAlloc) {}
};

template <typename T>
class DrUnmanagedArrList : public DrInternalPtrList<T>{
public:
    DrUnmanagedArrList(unsigned int numAlloc = 10) : DrInternalPtrList<T>(numAlloc) {}
};

template <typename T>
class DrManagedPtrList : public DrInternalPtrList<T>{
public:
    DrManagedPtrList(unsigned int numAlloc = 10) : DrInternalPtrList<T>(numAlloc) {}
protected:
    virtual void FreeElements(){
        if(m_p){
            for(unsigned int i = 0; i < Size(); i ++){
                delete GetEntry(i);
            }
        }
    }
};

template <typename T>
class DrManagedArrList: public DrInternalPtrList<T>{
public:
    DrManagedArrList(unsigned int numAlloc = 10) : DrInternalPtrList<T>(numAlloc) {}
protected:
    virtual void FreeElements(){
        if(m_p){
            for(unsigned int i = 0; i < Size(); i ++){
                delete[] GetEntry(i);
            }
        }
    }
};

typedef DrManagedArrList<const char> DrManagedStrList;
typedef DrUnmanagedPtrList<const char> DrUnmanagedStrList;

#endif
