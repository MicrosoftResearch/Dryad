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

#include "DrKernel.h"

template DRDECLARECLASS(DrStringDictionary<DrResourceRef>);

DrResource::DrResource(DrClusterPtr cluster, DrResourceLevel level, DrString name, DrString locality, DrResourcePtr parent)
{
    m_cluster = cluster;
    m_level = level;
    m_name = name;
    m_locality = locality;
    m_parent = parent;
    m_children = DrNew DrResourceList();

    if (m_parent != DrNull)
    {
        m_parent->GetChildren()->Add(this);
    }
}

void DrResource::Discard()
{
    m_parent = DrNull;
    if (m_children != DrNull)
    {
        int i;
        for (i=0; i<m_children->Size(); ++i)
        {
            m_children[i]->Discard();
        }
    }
    m_children = DrNull;
}

DrClusterPtr DrResource::GetCluster()
{
    return m_cluster;
}

DrResourceLevel DrResource::GetLevel()
{
    return m_level;
}

DrString DrResource::GetName()
{
    return m_name;
}

DrString DrResource::GetLocality()
{
    return m_locality;
}

DrResourcePtr DrResource::GetParent()
{
    return m_parent;
}

DrResourceListRef DrResource::GetChildren()
{
    return m_children;
}

bool DrResource::Contains(DrResourcePtr resource)
{
    if (resource->GetLevel() > m_level)
    {
        return false;
    }
    else if (resource->GetLevel() == m_level)
    {
        return (resource == this);
    }
    else
    {
        return Contains(resource->GetParent());
    }
}


DrUniverse::DrUniverse()
{
    m_resourceLock = DrNew DrCritSec();

    m_resourceAtLevel = DrNew RAArray(DRL_Cluster+1);
    int i;
    for (i=DRL_Core; i<=DRL_Cluster; ++i)
    {
        m_resourceAtLevel[i] = DrNew DrResourceList();
    }
    m_resource = DrNew DrResourceDictionary();
}

void DrUniverse::Discard()
{
    int i;
    for (i=DRL_Core; i<=DRL_Cluster; ++i)
    {
        int j;
        for (j=0; j<m_resourceAtLevel[i]->Size(); ++j)
        {
            m_resourceAtLevel[i][j]->Discard();
        }
        m_resourceAtLevel[i] = DrNull;
    }
    m_resourceAtLevel = DrNull;
    m_resource = DrNull;
}

DrCritSecPtr DrUniverse::GetResourceLock()
{
    return m_resourceLock;
}

void DrUniverse::AddResource(DrResourcePtr resource)
{
    m_resourceAtLevel[resource->GetLevel()]->Add(resource);
    m_resource->Add(resource->GetName().GetString(), resource);
}

DrResourceListRef DrUniverse::GetResources(DrResourceLevel level)
{
    return m_resourceAtLevel[level];
}

DrResourcePtr DrUniverse::LookUpResource(DrNativeString name)
{
    return LookUpResourceInternal(DrString(name));
}

DrResourcePtr DrUniverse::LookUpResourceInternal(DrString name)
{
    DrResourceRef resource;
    if (m_resource->TryGetValue(name.GetString(), resource))
    {
        return resource;
    }
    else
    {
        return DrNull;
    }
}


DrAffinity::DrAffinity()
{
    m_isHardConstraint = false;
    m_weight = 0;
    m_locality = DrNew DrResourceList();
}

void DrAffinity::SetHardConstraint(bool isHardConstraint)
{
    m_isHardConstraint = isHardConstraint;
}

bool DrAffinity::GetHardConstraint()
{
    return m_isHardConstraint;
}

void DrAffinity::SetWeight(UINT64 weight)
{
    m_weight = weight;
}

UINT64 DrAffinity::GetWeight()
{
    return m_weight;
}

void DrAffinity::AddLocality(DrResourcePtr locality)
{
    m_locality->Add(locality);
}

DrResourceListRef DrAffinity::GetLocalityArray()
{
    return m_locality;
}

DrAffinityRef DrAffinityIntersector::IntersectHardConstraints(DrAffinityPtr existingConstraints,
                                                              DrAffinityListRef newAffinities)
{
    DrAffinityRef constraints = existingConstraints;

    int i;
    for (i=0; i<newAffinities->Size(); ++i)
    {
        DrAffinityPtr a = newAffinities[i];
        if (a->GetHardConstraint())
        {
            if (constraints == DrNull)
            {
                constraints = DrNew DrAffinity();
                constraints->SetHardConstraint(true);

                int j;
                for (j=0; j<a->GetLocalityArray()->Size(); ++j)
                {
                    constraints->GetLocalityArray()->Add(a->GetLocalityArray()[j]);
                }
            }
            else
            {
                int j = 0;
                while (j<constraints->GetLocalityArray()->Size())
                {
                    DrResourcePtr c = constraints->GetLocalityArray()[j];

                    int k;
                    for (k=0; k<a->GetLocalityArray()->Size(); ++k)
                    {
                        DrResourcePtr r = a->GetLocalityArray()[k];
                        if (c->Contains(r) || r->Contains(c))
                        {
                            if (r->GetLevel() < c->GetLevel())
                            {
                                /* narrow the constraint */
                                constraints->GetLocalityArray()[j] = r;
                            }
                            break;
                        }
                    }
                    if (k == a->GetLocalityArray()->Size())
                    {
                        /* we can't keep this constraint */
                        constraints->GetLocalityArray()->RemoveAt(j);
                    }
                    else
                    {
                        ++j;
                    }
                }
            }
        }
    }

    return constraints;
}


DrAffinityMerger::DrAffinityMerger()
{
    m_dictionary = DrNew ResourceWeightDictionary();
}

void DrAffinityMerger::AccumulateWeights(DrAffinityListRef affinityList)
{
    int i;
    for (i=0; i<affinityList->Size(); ++i)
    {
        AccumulateWeights(affinityList[i]);
    }
}

void DrAffinityMerger::AccumulateWeights(DrAffinityPtr affinity)
{
    DrAssert(affinity->GetHardConstraint() == false);

    DrResourceListRef list = affinity->GetLocalityArray();
    int i;
    for (i=0; i<list->Size(); ++i)
    {
        DrResourcePtr r = list[i];
        do
        {
            UINT64 weight;
            if (m_dictionary->TryGetValue(r, weight))
            {
                weight += affinity->GetWeight();
                m_dictionary->Replace(r, weight);
            }
            else
            {
                m_dictionary->Add(r, affinity->GetWeight());
            }

            /* also accumulate the coarser-level information */
            r = r->GetParent();
        } while (r != DrNull);
    }
}

DrAffinityListRef DrAffinityMerger::GetMergedAffinities(DrFloatArrayRef levelThreshold)
{
    DrUINT64ArrayRef levelTotal = DrNew DrUINT64Array(DRL_Cluster + 1);

    int level;
    for (level = DRL_Core; level <= DRL_Cluster; ++level)
    {
        levelTotal[level] = 0;
    }

    ResourceWeightDictionary::DrEnumerator eSum = m_dictionary->GetDrEnumerator();
    while (eSum.MoveNext())
    {
        levelTotal[eSum.GetKey()->GetLevel()] += eSum.GetValue();
    }

    for (level = DRL_Core; level <= DRL_Cluster; ++level)
    {
        levelTotal[level] = (UINT64) ((float) levelTotal[level] * levelThreshold[level]);
    }

    DrAffinityListRef l = DrNew DrAffinityList();

    ResourceWeightDictionary::DrEnumerator eFilter = m_dictionary->GetDrEnumerator();
    while (eFilter.MoveNext())
    {
        DrResourcePtr resource = eFilter.GetKey();
        UINT64 weight = eFilter.GetValue();
        level = resource->GetLevel();
        if (weight >= levelTotal[level])
        {
            DrAffinityRef a = DrNew DrAffinity();
            a->SetWeight(weight);
            a->AddLocality(resource);
            l->Add(a);
        }
    }

    return l;
}
