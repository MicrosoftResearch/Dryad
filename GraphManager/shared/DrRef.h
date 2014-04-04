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

/*

DrRef is the class that is the basis for managing object storage in Dryad.
Under both the managed and unmanaged compilation, all object references are
wrapped in a DrRef, and the object is accessed and copied using overloads on
DrRef. In the managed version DrRef simply holds a managed reference to the
object on the garbage-collected heap. In the native version the object
inherits from a basic reference-counter class, and DrRef manages incrementing
and decrementing the reference count as the object is passed around the program.

This header also includes macros for allocating and freeing objects and arrays
so that they are allocated on the managed heap or the native heap depending on
compilation type.

A base type that needs to be reference-counted/managed is declared as follows:

DRBASECLASS(Foo)
{
    Foo() { ... }
    ...
};
DRREF(Foo);

Now type FooRef (which is just DrRef<Foo>) is used to store the object
throughout the program, and FooPtr (which is a Foo^ or Foo* depending)
can be used for a 'temporary' copy that does not modify the reference count in
the native world.

If you want to inherit from one of these classes it works like this:

DRCLASS(Bar) : public Foo
{
    ...
};
DRREF(Bar);

FooRef x = DrNew Foo();
{
    FooRef y;
    y = x; // refcount is now 2
} // refcount is back to 1 when y goes out of scope

FooPtr z = x; // refcount is still 1: be careful x doesn't go out of scope!

x->Wombat(); // can access members directly through the overload of ->
z->Wombat(); // or of course on the temporary copy directly

*/

#ifdef _MANAGED

#define DRABSTRACT abstract
#define DROVERRIDE override
#define DRSEALED sealed

#define DRENUM(T_) enum T_
#define DRPUBLICENUM(T_) public enum T_
#define DRBASECLASS(T_) public ref class T_ : public IDrRefCounter
#define DRINTERNALBASECLASS(T_) ref class T_ : public IDrRefCounter
#define DRINTERFACE(T_) public interface class T_
#define DRCLASS(T_) public ref class T_
#define DRINTERNALCLASS(T_) ref class T_
#define DRDECLARECLASS(T_) ref class T_
#define DRVALUECLASS(T_) public value class T_
#define DRDECLAREVALUECLASS(T_) value class T_
#define DRINTERNALVALUECLASS(T_) value class T_
#define DRTEMPLATE template ref class
#define DRREF(T_) typedef T_^ T_##Ref; typedef T_^ T_##Ptr; typedef T_% T_##R
#define DRAREF(A_,T_) typedef A_^ A_##Ref; typedef A_^ A_##Ptr; typedef A_% A_##R
#define DRIREF(T_) typedef T_^ T_##IRef; typedef T_^ T_##Ptr; typedef T_% T_##R
#define DRRREF(T_) typedef T_% T_##R
#define DrNew gcnew
#define DrNull nullptr
#define DRPIN(T_) pin_ptr< T_ >
#define DrObjectPtr Object^

interface class IDrRefCounter
{
};

template <class T> class DrRefHolder
{
public:
    DrRefHolder()
    {
        /* The CLR may initialize System::IntPtr members to IntPtr::Zero,
           but let's err on the safe side */
        m_storedValue = System::IntPtr::Zero;
    }

    void Store(T^ obj)
    {
        System::Runtime::InteropServices::GCHandle gch =
            System::Runtime::InteropServices::GCHandle::Alloc(obj);
        m_storedValue = System::Runtime::InteropServices::GCHandle::ToIntPtr(gch);
    }

    T^ Extract()
    {
        /* In some cases, Extract may be called on a DrRefHolder for which Store was
           never called.  */
        if (m_storedValue == System::IntPtr::Zero)
        {
            return DrNull;
        }

        System::Runtime::InteropServices::GCHandle gch =
            System::Runtime::InteropServices::GCHandle::FromIntPtr(m_storedValue);
        m_storedValue = System::IntPtr::Zero;

        T^ obj = (T^) gch.Target;
        gch.Free();

        return obj;
    }

private:
    System::IntPtr   m_storedValue;
};

template <class T_> public value class DrValueWrapper
{
public:
    T_% T()
    {
        if (p == DrNull)
        {
            p = DrNew T_();
        }

        return *p;
    }

private:
    T_^ p;
};


#else

#define DRABSTRACT =0
#define DROVERRIDE
#define DRSEALED 

#define DRENUM(T_) enum T_
#define DRPUBLICENUM(T_) enum T_
#define DRBASECLASS(T_) class T_ : public DrRefCounter
#define DRINTERNALBASECLASS(T_) class T_ : public DrRefCounter
#define DRINTERFACE(T_) class T_
#define DRCLASS(T_) class T_
#define DRINTERNALCLASS(T_) class T_
#define DRDECLARECLASS(T_) class T_
#define DRVALUECLASS(T_) class T_
#define DRDECLAREVALUECLASS(T_) class T_
#define DRINTERNALVALUECLASS(T_) class T_
#define DRTEMPLATE template class
#define DRREF(T_) typedef DrRef<T_> T_##Ref; typedef T_* T_##Ptr; typedef T_& T_##R
#define DRAREF(A_,T_) typedef DrArrayRef<A_,T_> A_##Ref; typedef A_* A_##Ptr; typedef A_& A_##R
#define DRIREF(T_) typedef DrInterfaceRef<T_> T_##IRef; typedef T_* T_##Ptr; typedef T_& T_##R
#define DRRREF(T_) typedef T_& T_##R
#define DrNew new
#define DrNull NULL
#define DRPIN(T_) T_*
#define DrObjectPtr DrRefCounter*

//#define _DEBUG_DRREF

#ifdef _DEBUG_DRREF
#include <set>
#include <map>
#endif

class DrRefCounter
{
public:
#ifdef _DEBUG_DRREF
    void   IncRef(void* holder);
    void   DecRef(void* holder);
#else
    void   IncRef();
    void   DecRef();
#endif

protected:
    mutable volatile LONG m_iRefCount;

    DrRefCounter();
    virtual ~DrRefCounter();
    void FreeMemory();      // Called when the refcount becomes zero.

#ifdef _DEBUG_DRREF
public:
    static std::set<DrRefCounter*> s_refsAllocated;
    static std::map<void*,DrRefCounter*> s_arrayStorage;
    static CRITICAL_SECTION        s_debugCS;
    std::set<void*>                m_holders;
#endif
};

#ifdef _DEBUG_DRREF
#define DRINCREF(p_) p_->IncRef(this)
#else
#define DRINCREF(p_) p_->IncRef()
#endif

#ifdef _DEBUG_DRREF
#define DRDECREF(p_) p_->DecRef(this)
#else
#define DRDECREF(p_) p_->DecRef()
#endif

template <class T> class DrRef
{
public:
    DrRef()
    {
        p = NULL;
    }

    explicit DrRef(const DrRef<T>& ref)
    {
        p = ref.p;

        if (p != NULL)
        {
            DRINCREF(p);
        }
    }

    DrRef(T* lp)
    {
        p = lp;

        if (p != NULL)
        {
            DRINCREF(p);
        }
    }

    virtual ~DrRef()
    {
        if (p != NULL)
        {
            DRDECREF(p);
            p = NULL;   // Make sure we AV in case someone is using DrRef after DecRef
        }
    }

    operator T*() const
    {
        return p;
    }

    T* operator->() const
    {
        return p;
    }

    bool operator==(T* pT) const
    {
        return (p == pT);
    }

    bool operator!=(T* pT) const
    {
        return (p != pT);
    }

    bool operator<(T* pT) const
    {
        return p < pT;
    }

    DrRef<T>& Set(T* lp)
    {
        if (p != lp)
        {
            if (lp != NULL)
            {
                DRINCREF(lp);
            }

            if (p != NULL)
            {
                DRDECREF(p);
            }

            p = lp;
        }

        return *this;
    }

    DrRef<T>& operator=(const DrRef<T>& ref)
    {
        return Set(ref.p);
    }

    DrRef<T>& operator=(T* lp)
    {
        return Set(lp);
    }

private:
    T* p;
};

template <class A,class T> class DrArrayRef
{
public:
    DrArrayRef()
    {
        p = NULL;
    }

    explicit DrArrayRef(const DrArrayRef<A,T>& ref)
    {
        p = ref.p;

        if (p != NULL)
        {
            DRINCREF(p);
        }
    }

    DrArrayRef(A* lp)
    {
        p = lp;

        if (p != NULL)
        {
            DRINCREF(p);
        }
    }

    virtual ~DrArrayRef()
    {
        if (p != NULL)
        {
            DRDECREF(p);
            p = NULL;   // Make sure we AV in case someone is using DrRef after DecRef
        }
    }

    operator A*() const
    {
        return p;
    }

    A* operator->() const
    {
        return p;
    }

    T& operator[](int element)
	{
		return p->operator[](element);
	}

    bool operator==(A* pT) const
    {
        return (p == pT);
    }

    bool operator!=(A* pT) const
    {
        return (p != pT);
    }

    bool operator<(A* pT) const
    {
        return p < pT;
    }

    DrArrayRef<A,T>& Set(A* lp)
    {
        if (p != lp)
        {
            if (lp != NULL)
            {
                DRINCREF(lp);
            }

            if (p != NULL)
            {
                DRDECREF(p);
            }

            p = lp;
        }

        return *this;
    }

    DrArrayRef<A,T>& operator=(const DrArrayRef<A,T>& ref)
    {
        return Set(ref.p);
    }

    DrArrayRef<A,T>& operator=(A* lp)
    {
        return Set(lp);
    }

private:
    A* p;
};

/* the only reason for this class is so that we can get DrInterfaceRef<I> to assert using the normal
   logging interface if the type cast fails. We can't call DrLogA from this header since it hasn't
   been defined yet, but generic classes need to have their implementations in the header files... */
class DrInterfaceRefBase
{
protected:
    static void AssertTypeCast();
};

/* DrInterfaceRef<I> is a reference counter holder for an object of type I that is an interface.
   There is a runtime check that the actual object inherits from DrRefCounter. We can't check
   this statically due to the limitations on multiple inheritance imposed by allowing cross-compilation
   to managed code */
template <class I> class DrInterfaceRef : public DrInterfaceRefBase
{
public:
    DrInterfaceRef()
    {
    }

    explicit DrInterfaceRef(const DrInterfaceRef<I>& ref)
    {
        Set((I*) ref);
    }

    DrInterfaceRef(I* lp)
    {
        Set(lp);
    }

    operator I*() const
    {
        return dynamic_cast<I*>((DrRefCounter *) p);
    }

    I* operator->() const
    {
        return dynamic_cast<I*>((DrRefCounter *) p);
    }

    bool operator!() const
    {
        return (!p);
    }

    bool operator==(I* pT) const
    {
        I* pI = dynamic_cast<I*>((DrRefCounter *) p);
        return (pI == pT);
    }

    bool operator!=(I* pT) const
    {
        return ((I*) this != pT);
    }

    DrInterfaceRef<I>& Set(I* obj)
    {
        p = dynamic_cast<DrRefCounter *>(obj);
        if (obj != DrNull && p == DrNull)
        {
            AssertTypeCast();
        }
        return *this;
    }

    DrInterfaceRef<I>& operator=(const DrInterfaceRef<I>& ref)
    {
        return Set((I*) ref);
    }

    DrInterfaceRef<I>& operator=(I* lp)
    {
        return Set(lp);
    }

private:
    DrRef<DrRefCounter> p;
};

template <class T> class DrRefHolder
{
public:
    void Store(T* obj)
    {
        m_storedValue = obj;
    }

    DrRef<T> Extract()
    {
        DrRef<T> obj = m_storedValue;
        m_storedValue = NULL;
        return obj;
    }

private:
    DrRef<T>    m_storedValue;
};

template <class T_> class DrValueWrapper
{
public:
    T_& T()
    {
        return t;
    }

private:
    T_ t;
};

#endif
