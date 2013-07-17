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

#include <cliext/map>

template <class K, class V> DRBASECLASS(DrMultiMap)
{
    typedef cliext::multimap<K,V> Map;
#else

#include <map>

template <class K, class V> DRBASECLASS(DrMultiMap)
{
    typedef std::multimap<K,V> Map;
#endif

public:
    typedef typename Map::iterator Iter;

    Iter Insert(K key, V value)
    {
#ifdef _MANAGED
        return m_map.insert(Map::make_value(key, value));
#else
        return m_map.insert(std::make_pair(key, value));
#endif
    }

    Iter Find(K key)
    {
        return m_map.find(key);
    }

    Iter Erase(Iter i)
    {
        return m_map.erase(i);
    }

    int Erase(K key)
    {
        return (int) m_map.erase(key);
    }

    Iter Begin()
    {
        return m_map.begin();
    }

    Iter End()
    {
        return m_map.end();
    }

    int GetSize()
    {
        return (int) m_map.size();
    }

    void Clear()
    {
        m_map.clear();
    }

protected:
    Map     m_map;
};
