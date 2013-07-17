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

template <class T> public ref class DrComparer abstract : public IDrRefCounter, public System::Collections::Generic::IComparer<T>
{
public:
    virtual int Compare(T x, T y) = 0;
};

#else

template <class T> DRBASECLASS(DrComparer)
{
public:
    static int __cdecl CompareUntyped(void* context, const void* x, const void* y)
    {
        DrComparer<T>* self = (DrComparer<T> *) context;
        return self->Compare(*((T *) x), *((T *) y));
    }

    virtual int Compare(T x, T y) = 0;
};

#endif

