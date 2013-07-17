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

DRENUM(DrResourceLevel)
{
    DRL_Core,
    DRL_Socket,
    DRL_Computer,
    DRL_Rack,
    DRL_Cluster
};

DRDECLARECLASS(DrResource);
DRREF(DrResource);

typedef DrArrayList<DrResourceRef> DrResourceList;
DRAREF(DrResourceList,DrResourceRef);

DRBASECLASS(DrResource)
{
public:
    DrResource(DrResourceLevel level, DrString name, DrString locality, DrResourcePtr parent);

    void Discard();

    DrResourceLevel GetLevel();
    DrString GetName();
    DrString GetLocality();
    DrResourcePtr GetParent();
    DrResourceListRef GetChildren();

    bool Contains(DrResourcePtr resource);

private:
    DrResourceLevel          m_level;
    DrString                 m_name;
    DrString                 m_locality;
    DrResourcePtr            m_parent; /* does not hold a reference to its parent */
    DrResourceListRef        m_children;
};

typedef DrStringDictionary<DrResourceRef> DrResourceDictionary;
DRREF(DrResourceDictionary);
/* the following exercises the template machinery to avoid a spurious compiler error */
template DRDECLARECLASS(DrStringDictionary<DrResourceRef>);

typedef DrArray<DrResourceRef> DrResourceArray;
DRAREF(DrResourceArray,DrResourceRef);

DRBASECLASS(DrUniverse)
{
public:
    DrUniverse();

    void Discard();

    DrCritSecPtr GetResourceLock();

    void AddResource(DrResourcePtr resource);

    DrResourceListRef GetResources(DrResourceLevel level);
    DrResourcePtr LookUpResource(DrNativeString name);
    DrResourcePtr LookUpResourceInternal(DrString name);

private:
    typedef DrArray<DrResourceListRef> RAArray;
    DRAREF(RAArray,DrResourceListRef);

    DrCritSecRef             m_resourceLock;
    RAArrayRef               m_resourceAtLevel;
    DrResourceDictionaryRef  m_resource;
};
DRREF(DrUniverse);

DRBASECLASS(DrAffinity)
{
public:
    DrAffinity();

    void SetHardConstraint(bool isHardConstraint);
    bool GetHardConstraint();

    void SetWeight(UINT64 weight);
    UINT64 GetWeight();

    void AddLocality(DrResourcePtr locality);
    DrResourceListRef GetLocalityArray();

private:
    bool               m_isHardConstraint;
    UINT64             m_weight;
    DrResourceListRef  m_locality;
};
DRREF(DrAffinity);

typedef DrArray<DrAffinityRef> DrAffinityArray;
DRAREF(DrAffinityArray,DrAffinityRef);

typedef DrArrayList<DrAffinityRef> DrAffinityList;
DRAREF(DrAffinityList,DrAffinityRef);

typedef DrArrayList<DrAffinityListRef> DrAffinityListList;
DRAREF(DrAffinityListList,DrAffinityListRef);

class DrAffinityIntersector
{
public:
    static DrAffinityRef IntersectHardConstraints(DrAffinityPtr existingConstraints,
                                                  DrAffinityListRef newAffinities);
};

DRBASECLASS(DrAffinityMerger)
{
public:
    DrAffinityMerger();

    void AccumulateWeights(DrAffinityPtr affinity);
    void AccumulateWeights(DrAffinityListRef affinityList);

    DrAffinityListRef GetMergedAffinities(DrFloatArrayRef levelThreshold);

private:
    typedef DrDictionary<DrResourcePtr,UINT64> ResourceWeightDictionary;
    DRREF(ResourceWeightDictionary);

    ResourceWeightDictionaryRef m_dictionary;
};
DRREF(DrAffinityMerger);

