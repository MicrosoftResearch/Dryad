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

DRCLASS(DrCohortProcess) : public DrSharedCritSec, public DrProcessListener
{
public:
    DrCohortProcess(DrGraphPtr graph, DrCohortPtr cohort,
                    int version, int numberOfVertices, DrTimeInterval timeout);

    void Discard();
    void DiscardParent();

    int GetVersion();

    DrLockBox<DrProcess> GetProcess();
    bool ProcessHasStarted();

    void NotifyVertexCompletion();

    /* DrProcessListener implementation */
    virtual void ReceiveMessage(DrProcessInfoRef message);

private:
    bool                     m_receivedProcess;
    DrMessagePumpRef         m_messagePump;
    DrCohortRef              m_parent;
    int                      m_version;
    DrLockBox<DrProcess>     m_process;
    int                      m_numberOfVerticesLeftToComplete;
    DrProcessHandleRef       m_processHandle;
    DrTimeInterval           m_timeout;
};
DRREF(DrCohortProcess);


DRVALUECLASS(VersionProcess)
{
public:
    int                         m_version;
    DrCohortProcessRef          m_process;
};
typedef DrArrayList<VersionProcess> VPList;
DRAREF(VPList,VersionProcess);


DRBASECLASS(DrCohort)
{
public:
    DrCohort(DrProcessTemplatePtr processTemplate, DrActiveVertexPtr initialMember);
    void Discard();

    DrProcessTemplatePtr GetProcessTemplate();

    void SetGang(DrGangPtr gang);
    DrGangPtr GetGang();

    DrString GetDescription();

    DrActiveVertexListPtr GetMembers();

    DrCohortProcessPtr GetProcessForVersion(int version);
    DrCohortProcessPtr EnsureProcess(DrGraphPtr graph, int version);
    void StartProcess(DrGraphPtr graph, int version);
    void NotifyProcessHasStarted(int version);
    void CancelVertices(int version, DrErrorPtr originalReason);
    void NotifyProcessComplete(int version);

    static void Merge(DrCohortRef c1, DrCohortRef c2);

	DrGraphPtr GetGraph();

private:
    void AssimilateOther(DrCohortPtr other);
    void PrepareDescription();

    DrProcessTemplateRef     m_processTemplate;
    DrActiveVertexListRef    m_list;
    DrGangRef                m_gang;
    DrString                 m_description;

    VPListRef                m_versionList;
};

typedef DrArrayList<DrCohortRef> DrCohortList;
DRAREF(DrCohortList,DrCohortRef);

DRINTERNALVALUECLASS(DrRunningGang)
{
public:
	int   m_version;
	int   m_verticesLeftToComplete;
};

typedef DrArrayList<DrRunningGang> DrRunningGangList;
DRAREF(DrRunningGangList, DrRunningGang);


DRBASECLASS(DrGang)
{
public:
    DrGang(DrCohortPtr initialCohort, DrStartCliquePtr initialStartClique);
    void Discard();

    void IncrementUnreadyVertices();
    void DecrementUnreadyVertices();
    bool VerticesAreReady();

    void StartVersion(DrGraphPtr graph, int version);
    void CancelVersion(int version, DrErrorPtr error);
	void CancelAllVersions(DrErrorPtr error);
	void EnsurePendingVersion(int duplicateVersion);
	void ReactToCompletedVertex(int version);

    DrCohortListPtr GetCohorts();
    DrStartCliqueListPtr GetStartCliques();

    void RemoveCohort(DrCohortPtr cohort);
    void RemoveStartClique(DrStartCliquePtr runClique);

    static void Merge(DrGangRef g1, DrGangRef g2);

private:
	void AssimilateOther(DrGangPtr other);
	DrGraphPtr GetGraph();

    DrCohortListRef        m_cohort;
    DrStartCliqueListRef   m_clique;

	/* If non-zero, this version number is complete and present in every vertex in the gang.
	   If this is non-zero then there are no pending versions or running versions of any
	   vertex in the gang, since we cancel any duplicate executions as soon as there is a
	   consistent completed version available. If this is zero then some vertices in the
	   gang may still have one or more completed versions, however there is no consistent
	   version that is complete in every vertex. */
	int                    m_completedVersion;

	/* If non-zero, this version number is pending in every vertex in the gang. If there is
	   already a pending version (i.e. if this is non-zero) then we will not create any more
	   pending version---in other words we will not 'queue' multiple duplicates to be
	   started. */
	int                    m_pendingVersion;

	/* This list contains an entry for every version that has been started for this gang,
	   for which not all vertices have completed (or failed). There can be more than one
	   entry in the list in the case that there are duplicates running. */
	DrRunningGangListRef   m_runningVersion;

	/* This is the version number that will be handed out to the next pending version. */
    int                    m_nextVersion;
    int                    m_unreadyVertexCount;
};
