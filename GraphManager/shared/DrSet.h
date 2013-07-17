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

using namespace System::Threading;

public ref class SimpleLock
{
public:
	SimpleLock(System::Object ^object)
	{
		m_object = object;
		if (Monitor::TryEnter(m_object) == false)
		{
			System::Diagnostics::Debugger::Break();
		}
	}

	~SimpleLock()
	{
		Monitor::Exit(m_object);
	}

private:
	System::Object ^m_object;
};


// this inherits from Dictionary instead of HashSet because my (and Ulfar's) install of VS08
// does not permit HashSet in C++/CLI. I don't know why. If I figure it out, this can easily
// be fixed
template <class T> DRBASECLASS(DrSet), public System::Collections::Generic::Dictionary<T,bool>
{
public:
    value class DrEnumerator
    {
    public:
        DrEnumerator(Dictionary<T,bool>^ hset)
        {
			m_iterator = hset->GetEnumerator();
        }

        T GetElement()
        {	
            return m_iterator.Current.Key;
        }

        bool MoveNext()
        {
            return m_iterator.MoveNext();
        }

    private:
        Dictionary<T,bool>::Enumerator   m_iterator;
    };

    bool Add(T key)
    {
        if (Contains(key))
        {
            return false;
        }
        else
        {
            Add(key, false);
            return true;
        }
    }

	virtual bool Remove(T element) new
    {
		bool bRet = System::Collections::Generic::Dictionary<T,bool>::Remove(element);
        return bRet; 
    }
    
    bool Contains(T key)
    {
		bool dummy;
        try
        {
            dummy = TryGetValue(key, dummy);
        }
        catch (...)
        {
            return true;
        }
		return dummy;
    }

    DrEnumerator GetDrEnumerator()
    {
		return DrEnumerator(this);
    }

    int GetSize()
    {
        return Count;
    }
};

#else

#include <set>

template <class T> DRBASECLASS(DrSet)
{
    typedef std::set<T> Set;

public:
    class DrEnumerator
    {
    public:
        DrEnumerator(Set* s)
        {
            m_iterator = s->begin();
            m_end = s->end();
            m_moved = false;
        }

        const T& GetElement()
        {
            DrAssert(m_moved);
            return *m_iterator;
        }

        bool MoveNext()
        {
            if (m_moved == false)
            {
                m_moved = true;
            }
            else if (m_iterator != m_end)
            {
                ++m_iterator;
            }

            if (m_iterator == m_end)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

    private:
        typename Set::iterator    m_iterator;
        typename Set::iterator    m_end;
        bool                      m_moved;
    };

    void Add(T element)
    {
        bool inserted =
            m_set.insert(element).second;
        DrAssert(inserted);
    }

    bool Contains(T element)
    {
        Set::const_iterator i = m_set.find(element);
        return (i != m_set.end());
    }

    bool Remove(T element)
    {
        size_t nRemoved = m_set.erase(element);
        if (nRemoved == 1)
        {
            return true;
        }
        else
        {
            DrAssert(nRemoved == 0);
            return false;
        }
    }

    DrEnumerator GetDrEnumerator()
    {
        return DrEnumerator(&m_set);
    }

    int GetSize()
    {
        return (int) m_set.size();
    }

protected:
    Set   m_set;
};

#endif

typedef DrSet<int> DrIntSet;
DRREF(DrIntSet);

typedef DrSet<UINT64> DrUInt64Set;
DRREF(DrUInt64Set);