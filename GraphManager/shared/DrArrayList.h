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

#pragma once

#ifdef _MANAGED


template <class T> DRBASECLASS(DrArrayList), public System::Collections::Generic::List<T>
{
public:
    DrArrayList() : System::Collections::Generic::List<T>()
    {
    }

    DrArrayList(int initialSize) : System::Collections::Generic::List<T>(initialSize)
    {
    }

    int Allocated()
    {
        return Capacity;
    }

    int Size()
    {
        return Count;
    }
};

template <class T> DRBASECLASS(DrValueWrapperArrayList)
{
public:
    DrValueWrapperArrayList()
    {
        m_list = DrNew System::Collections::Generic::List< DrValueWrapper<T> >();
    }

    DrValueWrapperArrayList(int initialSize)
    {
        m_list = DrNew System::Collections::Generic::List< DrValueWrapper<T> >(initialSize);
    }

    void Add(T t)
    {
        DrValueWrapper<T> tt;
        tt.T() = t;
        m_list->Add(tt);
    }

    void RemoveAt(int i)
    {
        m_list->RemoveAt(i);
    }

    T% operator[](int index)
    {
        return m_list[index].T();
    }

    T% Get(int index)
    {
        return m_list[index].T();
    }

    int Allocated()
    {
        return m_list->Capacity;
    }

    int Size()
    {
        return m_list->Count;
    }

private:
    System::Collections::Generic::List< DrValueWrapper<T> >^   m_list;
};

#define DRMAKEARRAYLIST(T_) typedef DrArrayList<T_> T_##List; \
    typedef T_##List^ T_##ListRef; typedef T_##List^ T_##ListPtr; typedef T_##List% T_##ListR; \
    template ref class DrArrayList<T_>

#define DRMAKEVWARRAYLIST(T_) typedef DrValueWrapperArrayList<T_> T_##List; \
    typedef T_##List^ T_##ListRef; typedef T_##List^ T_##ListPtr; typedef T_##List% T_##ListR; \
    template ref class DrValueWrapperArrayList<T_>

#else

#include <vector>
#include <algorithm>
#include <functional>

template <class T> DRBASECLASS(DrArrayList)
{
public:
    DrArrayList()
    {
        Initialize(1);
    }

    DrArrayList(int initialSize)
    {
        Initialize(initialSize);
    }

    ~DrArrayList()
    {
#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        if (m_vector.size() > 0)
        {
            size_t nRemoved = DrRefCounter::s_arrayStorage.erase(&(m_vector[0]));
            DrAssert(nRemoved == 1);
        }
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif
    }

    void Add(T t)
    {
#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        if (m_vector.size() > 0)
        {
            size_t nRemoved = DrRefCounter::s_arrayStorage.erase(&(m_vector[0]));
            DrAssert(nRemoved == 1);
        }
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif

        m_vector.push_back(t);

#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        bool inserted = DrRefCounter::s_arrayStorage.insert(std::make_pair(&(m_vector[0]),this)).second;
        DrAssert(inserted);
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif
    }

    void RemoveAt(int element)
    {
#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        size_t nRemoved = DrRefCounter::s_arrayStorage.erase(&(m_vector[0]));
        DrAssert(nRemoved == 1);
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif

        m_vector.erase(m_vector.begin() + element);

#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        if (m_vector.size() > 0)
        {
            bool inserted = DrRefCounter::s_arrayStorage.insert(std::make_pair(&(m_vector[0]),this)).second;
            DrAssert(inserted);
        }
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif
    }

    bool Remove(T t)
    {
        std::vector<T>::iterator i;
        for (i=m_vector.begin(); i!=m_vector.end(); ++i)
        {
            if (*i == t)
            {
                m_vector.erase(i);
                return true;
            }
        }
        return false;
    }

    T& operator[](int element)
    {
        return m_vector[element];
    }

    T& Get(int element)
    {
        return m_vector[element];
    }


    int Size()
    {
        return (int) m_vector.size();
    }

    int Allocated()
    {
        return (int) m_vector.capacity();
    }

    void Sort(DrComparer<T>* comparer)
    {
        Comparer c(comparer);
        std::sort(m_vector.begin(), m_vector.end(), c);
    }

private:
    class Comparer : std::binary_function<T,T,bool>
    {
    public:
        Comparer(DrComparer<T>* comparer)
        {
            m_comparer = comparer;
        }

        bool operator() (T& a, T& b)
        {
            return (m_comparer->Compare(a, b) < 0);
        }

    private:
        DrComparer<T>*  m_comparer;
    };

    void Initialize(int initialSize)
    {
        if (initialSize == 0)
        {
            initialSize = 1;
        }
        m_vector.reserve(initialSize);
#ifdef _DEBUG_DRREF
        DrAssert(m_vector.size() == 0);
#endif
    }

protected:
    std::vector<T>   m_vector;
};

#if 0
template <class T> DRBASECLASS(DrArrayList)
{
public:
    DrArrayList()
    {
        Initialize(1);
    }

    DrArrayList(int initialSize)
    {
        Initialize(initialSize);
    }

    ~DrArrayList()
    {
        delete [] m_array;
    }

    void Add(T t)
    {
        if (m_used == m_allocated)
        {
            m_allocated *= 2;
            T* newArray = new T[m_allocated];

            int i;
            for (i=0; i<m_used; ++i)
            {
                newArray[i] = m_array[i];
            }

            delete [] m_array;
            m_array = newArray;
        }

        DrAssert(m_used < m_allocated);
        m_array[m_used] = t;

        ++m_used;
    }

    void RemoveAt(int element)
    {
        DrAssert(element < m_used);
        int i;
        for (i=element+1; i<m_used; ++i)
        {
            m_array[i-1] = m_array[i];
        }
        --m_used;
        m_array[m_used] = T();
    }

    T& operator[](int element)
    {
        DrAssert(element < m_used);
        return m_array[element];
    }

    int Size()
    {
        return m_used;
    }

    int Allocated()
    {
        return m_allocated;
    }

    void Sort(DrComparer<T>* comparer)
    {
        ::qsort_s(m_array, m_used, sizeof(T), DrComparer<T>::CompareUntyped, comparer);
    }

private:
    void Initialize(int initialSize)
    {
        if (initialSize == 0)
        {
            initialSize = 1;
        }
        m_allocated = initialSize;
        m_used = 0;
        m_array = new T[m_allocated];
    }

protected:
    int     m_allocated;
    int     m_used;
    T*      m_array;
};

template <class T> DRBASECLASS(DrArrayList)
{
public:
    DrArrayList()
    {
        Initialize(1);
    }

    DrArrayList(int initialSize)
    {
        Initialize(initialSize);
    }

    ~DrArrayList()
    {
        Clear();
        delete [] m_array;
    }

    void Add(T t)
    {
        if (m_used == m_allocated)
        {
            m_allocated *= 2;
            unsigned char* newArray = new unsigned char[m_allocated * (int) sizeof(T)];

            memcpy(newArray, m_array, m_used * (int) sizeof(T));

            delete [] m_array;
            m_array = newArray;
        }

        DrAssert(m_used < m_allocated);
        void* insertLocation = &(m_array[m_used * (int) sizeof(T)]);
        ::new (insertLocation) T(t);

        ++m_used;
    }

    void RemoveAt(int element)
    {
        DrAssert(element < m_used);
        int i;
        for (i=element+1; i<m_used; ++i)
        {
            unsigned char* srcLocation = &(m_array[i * (int) sizeof(T)]);
            unsigned char* dstLocation = &(m_array[(i-1) * (int) sizeof(T)]);
            T* srcPtr = (T*) srcLocation;
            T* dstPtr = (T*) dstLocation;
            (*dstPtr) = (*srcPtr);
        }
        --m_used;
        unsigned char* removeLocation = &(m_array[m_used * (int) sizeof(T)]);
        T* removePtr = (T*) removeLocation;
        (*removePtr).T::~T();
    }

    T& operator[](int element)
    {
        DrAssert(element < m_used);
        unsigned char* location = &(m_array[element * (int) sizeof(T)]);
        T* ptr = (T*) location;
        return *ptr;
    }

    void Clear()
    {
        while (m_used > 0)
        {
            RemoveAt(m_used - 1);
        }
    }

    int Size()
    {
        return m_used;
    }

    void Sort(DrComparer<T>* comparer)
    {
        ::qsort_s(m_array, m_used, sizeof(T), DrComparer<T>::CompareUntyped, comparer);
    }

private:
    void Initialize(int initialSize)
    {
        if (initialSize == 0)
        {
            initialSize = 1;
        }
        m_allocated = initialSize;
        m_used = 0;
        m_array = new unsigned char[m_allocated * (int) sizeof(T)];
    }

protected:
    int             m_allocated;
    int             m_used;
    unsigned char*  m_array;
};
#endif

#define DRMAKEARRAYLIST(T_) typedef DrArrayList<T_> T_##List; \
    typedef DrArrayRef<T_##List,T_> T_##ListRef; typedef T_##List* T_##ListPtr; typedef T_##List& T_##ListR; \
    template class DrArrayList<T_>

#define DRMAKEVWARRAYLIST(T_) typedef DrArrayList<T_> T_##List; \
    typedef DrArrayRef<T_##List,T_> T_##ListRef; typedef T_##List* T_##ListPtr; typedef T_##List& T_##ListR; \
    template class DrArrayList<T_>

#endif

typedef DrArrayList<BYTE> DrByteArrayList;
DRAREF(DrByteArrayList,BYTE);
template DRDECLARECLASS(DrArrayList<BYTE>);

typedef DrArrayList<int> DrIntArrayList;
DRAREF(DrIntArrayList,int);
template DRDECLARECLASS(DrArrayList<int>);

DRMAKEVWARRAYLIST(DrString);
