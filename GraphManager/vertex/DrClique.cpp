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

#include <DrVertexHeaders.h>

DrStartClique::DrStartClique(DrActiveVertexPtr initialMember)
{
    m_list = DrNew DrActiveVertexList();
    m_list->Add(initialMember);

    m_version = 0;
	m_externalInputsRemaining = 0;
}

void DrStartClique::Discard()
{
    m_list = DrNull;

    if (m_gang != DrNull)
    {
        m_gang->Discard();
    }
    m_gang = DrNull;
}

int DrStartClique::CountExternalInputs(DrActiveVertexPtr vertex)
{
    int numberOfExternalInputs = 0;
    int i;
    for (i=0; i<vertex->GetInputs()->GetNumberOfEdges(); ++i)
    {
        DrEdge e = vertex->GetInputs()->GetEdge(i);
        if (e.IsStartCliqueEdge() == false)
        {
            ++numberOfExternalInputs;
        }
    }

    return numberOfExternalInputs;
}

void DrStartClique::SetGang(DrGangPtr gang)
{
    m_gang = gang;
}

DrGangPtr DrStartClique::GetGang()
{
    return m_gang;
}

DrActiveVertexListPtr DrStartClique::GetMembers()
{
    return m_list;
}

void DrStartClique::AssimilateOther(DrStartCliquePtr other)
{
    DrActiveVertexListRef otherList = other->GetMembers();
    int i;
    for (i=0; i<otherList->Size(); ++i)
    {
        DrActiveVertexPtr otherMember = otherList[i];
        DrAssert(otherMember->GetStartClique() == other);
        otherMember->SetStartClique(this);
        m_list->Add(otherMember);
    }

    DrGangPtr otherGang = other->GetGang();
    otherGang->RemoveStartClique(other);

    DrGang::Merge(otherGang, m_gang);

}

void DrStartClique::Merge(DrStartCliqueRef s1, DrStartCliqueRef s2)
{
    if (s1 == s2)
    {
        return;
    }

    if (s1->GetMembers()->Size() > s2->GetMembers()->Size())
    {
        s1->AssimilateOther(s2);
    }
    else
    {
        s2->AssimilateOther(s1);
    }
}

void DrStartClique::InstantiateVersion(int version)
{
	DrAssert(m_version == 0);
	m_version = version;
    m_externalInputsRemaining = 0;

	int i;
    for (i=0; i<m_list->Size(); ++i)
    {
        m_externalInputsRemaining += CountExternalInputs(m_list[i]);
    }

    for (i=0; i<m_list->Size(); ++i)
    {
        m_list[i]->InstantiateVersion(version);
    }
}

void DrStartClique::NotifyExternalInputsReady(int version, int numberOfInputs)
{
	DrAssert(m_version == version);
    DrAssert(m_externalInputsRemaining >= numberOfInputs);

    m_externalInputsRemaining -= numberOfInputs;
}

void DrStartClique::GrowExternalInputs(int numberOfInputs)
{
	if (m_version != 0)
	{
		m_externalInputsRemaining += numberOfInputs;
    }
}

bool DrStartClique::StartVersionIfReady(int version)
{
	DrAssert(m_version == version);

    if (m_externalInputsRemaining > 0)
    {
        /* there are still inputs needed before we can start */
        return false;
    }

    m_version = 0;

    /* ok all the inputs are ready: now make sure there's a process running for everyone
       in the clique and move their records from pending to running */
	int i;
    for (i=0; i<m_list->Size(); ++i)
    {
        m_list[i]->StartProcess(version);
    }

    /* then make sure they start connecting to each other if there was already a process
       running in the cohort */
    for (i=0; i<m_list->Size(); ++i)
    {
        m_list[i]->CheckForProcessAlreadyStarted(version);
    }

    return true;
}

void DrStartClique::NotifyVersionRevoked(int version)
{
	if (m_version == version)
	{
		m_version = 0;
	}
}
