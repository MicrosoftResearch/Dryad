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

template <class K, class V> DRBASECLASS(DrDictionary), public System::Collections::Generic::Dictionary<K,V>
{
public:
    value class DrEnumerator
    {
    public:
        DrEnumerator(Dictionary<K,V>^ dict)
        {
            m_iterator = dict->GetEnumerator();
        }

        K GetKey()
        {
            return m_iterator.Current.Key;
        }

        V GetValue()
        {
            return m_iterator.Current.Value;
        }

        bool MoveNext()
        {
            return m_iterator.MoveNext();
        }

    private:
        Dictionary<K,V>::Enumerator   m_iterator;
    };

    DrEnumerator GetDrEnumerator()
    {
        return DrEnumerator(this);
    }

    int GetSize()
    {
        return Count;
    }

    void Replace(K key, V value)
    {
        bool removed = Remove(key);
        DrAssert(removed);
        Add(key, value);
    }
};

template <class V> DRCLASS(DrStringDictionary) : public DrDictionary<System::String^,V>
{
};

#if 0
/* unfortunately this crashes the compiler with an internal error */
template <class K,class V> DRCLASS(DrDictionaryForValueWrapper)
    : public System::Collections::Generic::Dictionary< K, DrValueWrapper<V> >
{
public:
    value class DrEnumerator
    {
    public:
        DrEnumerator(Dictionary< K, DrValueWrapper<V> >^ dict)
        {
            m_iterator = dict->GetEnumerator();
        }

        K GetKey()
        {
            return m_iterator.Current.Key;
        }

        V GetValue()
        {
            return m_iterator.Current.Value.T();
        }

        bool MoveNext()
        {
            return m_iterator.MoveNext();
        }

    private:
        Dictionary< K, DrValueWrapper<V> >::Enumerator   m_iterator;
    };

    DrEnumerator GetDrEnumerator()
    {
        return DrEnumerator(this);
    }

    int GetSize()
    {
        return Count;
    }

    void Add(K key, V value)
    {
        DrValueWrapper<V> v;
        v.T() = value;
        Add(key, v);
    }

    bool TryGetValue(K key, /*out*/ V% value)
    {
        DrValueWrapper<V> v;
        bool found = TryGetValue(key, v);
        if (found)
        {
            value = v.T();
        }
        return found;
    }

    void Replace(K key, V value)
    {
        bool removed = Remove(key);
        DrAssert(removed);
        Add(key, value);
    }
};

typedef DrDictionaryForValueWrapper<System::String^,DrString> DrStringStringDictionary;
DRREF(DrStringStringDictionary);
#endif

template <class K> DRCLASS(DrDictionaryForString)
    : public System::Collections::Generic::Dictionary< K, DrValueWrapper<DrString> >
{
public:
    value class DrEnumerator
    {
    public:
        DrEnumerator(Dictionary< K, DrValueWrapper<DrString> >^ dict)
        {
            m_iterator = dict->GetEnumerator();
        }

        K GetKey()
        {
            return m_iterator.Current.Key;
        }

        DrString GetValue()
        {
            return m_iterator.Current.Value.T();
        }

        bool MoveNext()
        {
            return m_iterator.MoveNext();
        }

    private:
        Dictionary< K, DrValueWrapper<DrString> >::Enumerator   m_iterator;
    };

    DrEnumerator GetDrEnumerator()
    {
        return DrEnumerator(this);
    }

    int GetSize()
    {
        return Count;
    }

    void Add(K key, DrString value)
    {
        DrValueWrapper<DrString> v;
        v.T() = value;
        Add(key, v);
    }

    bool TryGetValue(K key, /*out*/ DrString% value)
    {
        DrValueWrapper<DrString> v;
        bool found = TryGetValue(key, v);
        if (found)
        {
            value = v.T();
        }
        return found;
    }

    void Replace(K key, DrString value)
    {
        bool removed = Remove(key);
        DrAssert(removed);
        Add(key, value);
    }
};

typedef DrDictionaryForString<System::String^> DrStringStringDictionary;
DRREF(DrStringStringDictionary);
template ref class DrDictionaryForString<System::String^>;


#else

#include <map>
#include <string>

template <class K, class V> DRBASECLASS(DrDictionary)
{
    typedef std::map<K,V> Map;

public:
    class DrEnumerator
    {
    public:
        DrEnumerator(Map* m)
        {
            m_iterator = m->begin();
            m_end = m->end();
            m_moved = false;
        }

        const K& GetKey()
        {
            DrAssert(m_moved);
            return m_iterator->first;
        }

        V& GetValue()
        {
            DrAssert(m_moved);
            return m_iterator->second;
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
        typename Map::iterator    m_iterator;
        typename Map::iterator    m_end;
        bool                      m_moved;
    };

    void Add(K key, V value)
    {
        bool inserted =
            m_map.insert(std::make_pair(key, value)).second;
        DrAssert(inserted);
    }

    bool TryGetValue(K key, /*out*/ V& value)
    {
        Map::const_iterator i = m_map.find(key);
        if (i == m_map.end())
        {
            return false;
        }
        else
        {
            value = i->second;
            return true;
        }
    }

    bool Remove(K key)
    {
        size_t nRemoved = m_map.erase(key);
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

    void Replace(K key, V value)
    {
        Map::iterator i = m_map.find(key);
        DrAssert(i != m_map.end());
        i->second = value;
    }

    DrEnumerator GetDrEnumerator()
    {
        return DrEnumerator(&m_map);
    }

    int GetSize()
    {
        return (int) m_map.size();
    }

protected:
    Map   m_map;
};

template <class V> DRBASECLASS(DrStringDictionary)
{
    typedef std::map<std::string,V> Map;

public:
    class DrEnumerator
    {
    public:
        DrEnumerator(Map* m)
        {
            m_iterator = m->begin();
            m_end = m->end();
            m_moved = false;
        }

        const char* GetKey()
        {
            DrAssert(m_moved);
            return m_iterator->first.c_str();
        }

        V& GetValue()
        {
            DrAssert(m_moved);
            return m_iterator->second;
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
        typename Map::iterator    m_iterator;
        typename Map::iterator    m_end;
        bool                      m_moved;
    };

    void Add(const char* key, V value)
    {
        bool inserted =
            m_map.insert(std::make_pair(std::string(key), value)).second;
        DrAssert(inserted);
    }

    bool TryGetValue(const char* key, /*out*/ V& value)
    {
        Map::const_iterator i = m_map.find(std::string(key));
        if (i == m_map.end())
        {
            return false;
        }
        else
        {
            value = i->second;
            return true;
        }
    }

    bool Remove(const char* key)
    {
        size_t nRemoved = m_map.erase(std::string(key));
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
        return DrEnumerator(&m_map);
    }

    int GetSize()
    {
        return (int) m_map.size();
    }

protected:
    Map   m_map;
};

template <class K,class V> DRCLASS(DrDictionaryForValueWrapper) : public DrDictionary<K,V>
{
};

template <class K> DRCLASS(DrDictionaryForString) : public DrDictionary<K,DrString>
{
};

typedef DrStringDictionary<DrString> DrStringStringDictionary;
DRREF(DrStringStringDictionary);

#endif
