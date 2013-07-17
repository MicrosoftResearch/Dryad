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

DRDECLARECLASS(DrGraph);
DRREF(DrGraph);

DRDECLARECLASS(DrGang);
DRREF(DrGang);

DRBASECLASS(DrStartClique)
{
public:
    DrStartClique(DrActiveVertexPtr initialMember);
    void Discard();

    void SetGang(DrGangPtr gang);
    DrGangPtr GetGang();

    int CountExternalInputs(DrActiveVertexPtr vertex);

    DrActiveVertexListPtr GetMembers();

    static void Merge(DrStartCliqueRef s1, DrStartCliqueRef s2);

    void InstantiateVersion(int version);
    void NotifyExternalInputsReady(int version, int numberOfInputs);
    void GrowExternalInputs(int numberOfInputs);
    bool StartVersionIfReady(int version);
    void NotifyVersionRevoked(int version);

private:
    void AssimilateOther(DrStartCliquePtr other);

    DrActiveVertexListRef    m_list;
    DrGangRef                m_gang;

	/* If we are currently preparing a new version to start then m_version is non-zero and
	   m_externalInputsRemaining gives the number of external inputs for that version that we
	   are waiting for */
	int                      m_version;
	int                      m_externalInputsRemaining;
};

typedef DrArrayList<DrStartCliqueRef> DrStartCliqueList;
DRAREF(DrStartCliqueList,DrStartCliqueRef);
