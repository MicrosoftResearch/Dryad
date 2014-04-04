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

#include "DrRef.h"

#ifdef _MANAGED

template <class T> DRBASECLASS(DrArray)
{
public:
    DrArray()
    {
        m_array = DrNull;
    }

    DrArray(int s)
    {
        m_array = DrNew array<T>(s);
    }

    T% operator[](int element)
    {
        return m_array[element];
    }

    int Allocated()
    {
        return m_array->Length;
    }

    array<T>^ GetArray()
    {
        return m_array;
    }

    void SetArray(array<T>^ newArray)
    {
        m_array = newArray;
    }

protected:
    array<T>^     m_array;
};

template <class T> DRBASECLASS(DrValueWrapperArray)
{
public:
    DrValueWrapperArray(int s)
    {
        m_array = DrNew array< DrValueWrapper<T> >(s);
    }

    T% operator[](int element)
    {
        return m_array[element].T();
    }

    int Allocated()
    {
        return m_array->Length;
    }

    array< DrValueWrapper<T> >^ GetArray()
    {
        return m_array;
    }

protected:
    array< DrValueWrapper<T> >^     m_array;
};

#define DRMAKEARRAY(T_) typedef DrArray<T_> T_##Array; \
    typedef T_##Array^ T_##ArrayRef; typedef T_##Array^ T_##ArrayPtr; typedef T_##Array% T_##ArrayR; \
    template ref class DrArray<T_>

#define DRMAKEVWARRAY(T_) typedef DrValueWrapperArray<T_> T_##Array; \
    typedef T_##Array^ T_##ArrayRef; typedef T_##Array^ T_##ArrayPtr; typedef T_##Array% T_##ArrayR; \
    template ref class DrValueWrapperArray<T_>

#else

template <class T> DRBASECLASS(DrArray)
{
public:
    DrArray(int s)
    {
        m_allocated = s;
        m_array = new T[m_allocated];

#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        bool inserted = DrRefCounter::s_arrayStorage.insert(std::make_pair(m_array,this)).second;
        DrAssert(inserted);
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif
    }

    ~DrArray()
    {
        delete [] m_array;

#ifdef _DEBUG_DRREF
        EnterCriticalSection(&DrRefCounter::s_debugCS);
        size_t nRemoved = DrRefCounter::s_arrayStorage.erase(m_array);
        DrAssert(nRemoved == 1);
        LeaveCriticalSection(&DrRefCounter::s_debugCS);
#endif
    }

    T& operator[](int element)
    {
        DrAssert(element < m_allocated);
        return m_array[element];
    }

    int Allocated()
    {
        return m_allocated;
    }

    T* GetPtr()
    {
        return m_array;
    }

protected:
    int       m_allocated;
    T*        m_array;
};

#ifdef _MANAGED
#define DRMAKEARRAY(T_) typedef DrArray<T_> T_##Array; \
    typedef DrArrayRef<T_##Array,T_> T_##ArrayRef; typedef T_##Array* T_##ArrayPtr; typedef T_##Array& T_##ArrayR; \
    template class DrArray<T_>
#else 
#define DRMAKEARRAY(T_) typedef DrArray<T_> T_##Array; \
    typedef DrArrayRef<T_##Array,T_> T_##ArrayRef; typedef T_##Array* T_##ArrayPtr; typedef T_##Array& T_##ArrayR; 
#endif

#define DRMAKEVWARRAY(T_) typedef DrArray<T_> T_##Array; \
    typedef DrArrayRef<T_##Array,T_> T_##ArrayRef; typedef T_##Array* T_##ArrayPtr; typedef T_##Array& T_##ArrayR; \
    template class DrArray<T_>

#endif

typedef DrArray<BYTE> DrByteArray;
DRAREF(DrByteArray,BYTE);

typedef DrArray<int> DrIntArray;
DRAREF(DrIntArray,int);

typedef DrArray<UINT32> DrUINT32Array;
DRAREF(DrUINT32Array,UINT32);

typedef DrArray<UINT64> DrUINT64Array;
DRAREF(DrUINT64Array,UINT64);

typedef DrArray<float> DrFloatArray;
DRAREF(DrFloatArray,float);

DRMAKEVWARRAY(DrString);
