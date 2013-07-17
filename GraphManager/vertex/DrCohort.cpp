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

DrCohortProcess::DrCohortProcess(DrGraphPtr graph, DrCohortPtr parent, int version,
                                 int numberOfVertices, DrTimeInterval timeout)
    : DrSharedCritSec(graph)
{
    m_receivedProcess = false;
    m_messagePump = graph->GetXCompute()->GetMessagePump();
    m_parent = parent;
    m_version = version;
    m_numberOfVerticesLeftToComplete = numberOfVertices;
    m_timeout = timeout;
}

void DrCohortProcess::DiscardParent()
{
    m_parent = DrNull;
}

int DrCohortProcess::GetVersion()
{
    return m_version;
}

DrLockBox<DrProcess> DrCohortProcess::GetProcess()
{
    return m_process;
}

void DrCohortProcess::ReceiveMessage(DrProcessInfoRef message)
{
    if (m_receivedProcess == false)
    {
        /* when the CohortStarter helper below actually scheduled the process, it
           wasn't holding our lock. So the first thing it does is send us a
           message containing our DrProcess. Because of the ordering guarantees
           of the message queue, that is the first message we will receive. There
           are two cases: either it sent the process to be scheduled, in which
           case we will hear from a subsequent message whether it succeeded or not,
           or there was an unsatisfiable hard constraint in which case we hear with
           an error right now. */

        m_receivedProcess = true;

        /* this can only be the fake message from the Cohort Starter */
        DrAssert(message->m_state->m_state == DPS_NotStarted);
        
        if (message->m_process.IsNull())
        {
            /* there was an error scheduling */
            DrAssert(message->m_state->m_status != DrNull);

            DrString msg = DrError::ToShortText(message->m_state->m_status);
            DrLogI("Cohort %s v.%d got scheduling error message on cohort startup %s",
                   m_parent->GetDescription().GetChars(), m_version, msg.GetChars());

			m_parent->GetGang()->CancelVersion(m_version, message->m_state->m_status);
        }
        else
        {
            DrAssert(message->m_state->m_status == DrNull);
            m_process = message->m_process;
            DrLogI("Cohort %s v.%d got startup message",
                   m_parent->GetDescription().GetChars(), m_version);
        }

        return;
    }

    if (m_process.IsNull())
    {
        /* we have already finished so do nothing */
        return;
    }

    DrLogI("Cohort %s v.%d got message state %d",
           m_parent->GetDescription().GetChars(), m_version,
           message->m_state->m_state);

    DrProcessStateRecordPtr state = message->m_state;
    if (state->m_state > DPS_Running)
    {
        /* in the normal course of affairs, we should have already seen the process
           start running, in which case the vertices have all initiated their own
           message sends to the process which will also return informing them that
           it has finished, at which point we will be notified cleanly via NotifyVertexCompletion.
           The DrProcess machinery is supposed to have delayed the message we are now
           receiving in order to give the vertex messages a chance to arrive. So if we
           ever get here, something has gone wrong: either the process never started or
           the vertex messages didn't get sent.

           We are going to call Cancel below on the gang, which will result in all our vertices
           calling NotifyVertexCompletion and eventually us cleaning up once they all report.
        */

        DrErrorRef error;

        if (state->m_state == DPS_Completed)
        {
            DrString reason;
            if (m_processHandle == DrNull)
            {
                if (state->m_status == DrNull)
                {
                    reason.SetF("Process completed with no error without starting");
                    error = DrNew DrError(DrError_VertexError, "DrCohortProcess", reason);
                }
                else
                {
                    reason.SetF("Process completed with code %s without starting",
                                DRERRORSTRING(state->m_status->m_code));
                    error = DrNew DrError(state->m_status->m_code, "DrCohortProcess", reason);
                    error->AddProvenance(state->m_status);
                }
            }
            else
            {
                if (state->m_status == DrNull)
                {
                    reason.SetF("Process completed with no error but vertex message was never delivered");
                    error = DrNew DrError(DrError_VertexError, "DrCohortProcess", reason);
                }
                else
                {
                    reason.SetF("Process completed with code %s but vertex message was never delivered",
                                DRERRORSTRING(state->m_status->m_code));
                    error = DrNew DrError(state->m_status->m_code, "DrCohortProcess", reason);
                    error->AddProvenance(state->m_status);
                }
            }
        }
        else
        {
            if (state->m_status == DrNull)
            {
                DrLogW("Empty status delivered with process info state %u", state->m_state);
                DrString reason;
                reason.SetF("Empty status with failed process state %u", state->m_state);
                error = DrNew DrError(DrError_Unexpected, "DrCohortProcess", reason);
            }
            else
            {
                DrString reason;
                reason.SetF("%s process code %s",
                            (state->m_state == DPS_Failed) ? "Failed" : "Zombie",
                            DRERRORSTRING(state->m_status->m_code));
                error = DrNew DrError(state->m_status->m_code, "DrCohortProcess", reason);
                error->AddProvenance(state->m_status);
            }
        }

        DrString eString = DrError::ToShortText(error);
        DrLogI("Cohort %s v.%d cancelling gang %s %s",
               m_parent->GetDescription().GetChars(), m_version, eString.GetChars(),
               error->m_explanation.GetChars());

		m_parent->GetGang()->CancelVersion(m_version, error);
    }
    else if (m_processHandle == DrNull && state->m_state == DPS_Running)
    {
        /* the process has started so tell everyone about it */
        m_processHandle = state->m_process;
        DrAssert(m_processHandle != DrNull);

        DrLogI("Cohort %s v.%d starting process",
               m_parent->GetDescription().GetChars(), m_version);

        m_parent->NotifyProcessHasStarted(m_version);
    }
}

bool DrCohortProcess::ProcessHasStarted()
{
    return (m_processHandle != DrNull);
}

void DrCohortProcess::NotifyVertexCompletion()
{
    DrLogI("Enter with m_numberOfVerticesLeftToComplete %d", m_numberOfVerticesLeftToComplete);
    DrAssert(m_numberOfVerticesLeftToComplete > 0);
    --m_numberOfVerticesLeftToComplete;

    if (m_numberOfVerticesLeftToComplete > 0)
    {
        DrLogI("Still have %d vertices to complete", m_numberOfVerticesLeftToComplete);
        return;
    }

    bool scheduleTermination = false;
    if (m_process.IsEmpty() == false)
    {
        DrLockBoxKey<DrProcess> process(m_process);

        if (process->GetInfo()->m_state->m_state <= DPS_Running)
        {
            /* the process hasn't exited yet even though all the vertices are done. It will
               probably exit soon of its own accord, but in case it doesn't we'll schedule
               a message that will terminate it after a while */
            DrLogI("Process %s has not exited, scheduling termination", process->GetName().GetChars());
            scheduleTermination = true;
        }
        else
        {
            /* The process completed and we're done with it, call Terminate to clean up */
            DrLogI("Process %s completed, calling Terminate to clean up", process->GetName().GetChars());
            DrAssert(process->GetInfo()->m_state->m_process != DrNull);
            process->Terminate();
        }
    }

    if (scheduleTermination)
    {
        /* The listener for the state message (DrProcess::ReceiveMessage(DrProcessState message))
           will call Terminate to clean up in response to this message. */
        DrPStateMessageRef message = DrNew DrPStateMessage(m_process, DPS_Failed);
        m_messagePump->EnQueueDelayed(m_timeout, message);
    }

	DrLogI("Notifying cohort of vertex completion");
    m_parent->NotifyProcessComplete(m_version);

	DrLogI("Discarding cohort process");
    Discard();
}

void DrCohortProcess::Discard()
{
    m_parent = DrNull;

    if (m_process.IsEmpty() == false)
    {
        {
            DrLockBoxKey<DrProcess> process(m_process);

            process->CancelListener(this);

        }

        m_process.Set(DrNull);
    }

    /* make sure that if we were cancelled just as we were starting up, we
       ignore the startup message containing the process when it arrives
       instead of getting confused and setting m_process to be non-null again */
    m_receivedProcess = true;
}


DRBASECLASS(DrCohortStartInfo)
{
public:
    DrCohortStartInfo(DrXComputeRef xcompute, DrCohortProcessRef cohort,
                      DrString processName, DrString commandLine,
                      DrProcessTemplateRef processTemplate,
                      DrAffinityListRef affinityList)
    {
        m_xcompute = xcompute;
        m_cohort = cohort;
        m_processName = processName;
        m_commandLine = commandLine;
        m_processTemplate = processTemplate;
        m_affinityList = affinityList;
    }

    DrXComputeRef                m_xcompute;
    DrCohortProcessRef           m_cohort;
    DrString                     m_processName;
    DrString                     m_commandLine;
    DrProcessTemplateRef         m_processTemplate;
    DrAffinityListRef            m_affinityList;
};
DRREF(DrCohortStartInfo);

typedef DrListener<DrCohortStartInfoRef> DrCohortStartListener;
typedef DrMessage<DrCohortStartInfoRef> DrCohortStartMessage;
DRREF(DrCohortStartMessage);

DRCLASS(DrCohortStarter) : public DrCritSec, public DrCohortStartListener
{
public:
    /* implements the DrCohortStartListener interface */
    virtual void ReceiveMessage(DrCohortStartInfoRef message)
    {
        DrAffinityRef hardConstraint = DrAffinityIntersector::IntersectHardConstraints(DrNull,
                                                                                       message->m_affinityList);

        DrAffinityListRef affinityList;
        if (hardConstraint == DrNull)
        {
            DrAffinityMergerRef merger = DrNew DrAffinityMerger();
            merger->AccumulateWeights(message->m_affinityList);
            affinityList = merger->GetMergedAffinities(message->m_processTemplate->GetAffinityLevelThresholds());
        }
        else
        {
            if (hardConstraint->GetLocalityArray()->Size() == 0)
            {
                DrString reason = "Unsatisfiable hard constraint for starting vertex";
                DrErrorRef error = DrNew DrError(DrError_HardConstraintCannotBeMet, "DrCohortStarter", reason);

                DrProcessInfoRef failureNotification = DrNew DrProcessInfo();
                failureNotification->m_state = DrNew DrProcessStateRecord();
                failureNotification->m_state->m_state = DPS_Failed;
                failureNotification->m_state->m_status = error;

                DrProcessMessageRef failureMessage = DrNew DrProcessMessage(message->m_cohort,
                                                                            failureNotification);

                message->m_xcompute->GetMessagePump()->EnQueue(failureMessage);

                return;
            }

            affinityList = DrNew DrAffinityList();
            affinityList->Add(hardConstraint);
        }

        DrProcessRef process = DrNew DrProcess(message->m_xcompute, message->m_processName,
                                               message->m_commandLine, message->m_processTemplate);

        /* make a message to the cohort that will get delivered before the first message from its
           process, including the process as payload. We rely on the ordering of the message queue
           to ensure this message will arrive before anything from the process. We do it this way
           so we can avoid acquiring the cohort's lock during scheduling, since the cohort shares
           its lock with the global graph lock and we want to be able to start scheduling the
           processes in parallel with graph state machine actions, particularly when a large stage
           is starting up. */
        DrProcessInfoRef startNotification = DrNew DrProcessInfo();
        startNotification->m_process = process;
        startNotification->m_state = DrNew DrProcessStateRecord();
        startNotification->m_state->m_state = DPS_NotStarted;
        DrProcessMessageRef startMessage = DrNew DrProcessMessage(message->m_cohort,
                                                                  startNotification);
        message->m_xcompute->GetMessagePump()->EnQueue(startMessage);

        /* now actually schedule the process */
        process->SetAffinityList(affinityList);
        process->AddListener(message->m_cohort);

        process->Schedule();
    }
};
DRREF(DrCohortStarter);


DrCohort::DrCohort(DrProcessTemplatePtr processTemplate, DrActiveVertexPtr initialMember)
{
    m_processTemplate = processTemplate;

    m_list = DrNew DrActiveVertexList();
    m_list->Add(initialMember);

    m_versionList = DrNew VPList();

    PrepareDescription();
}

void DrCohort::Discard()
{
    m_list = DrNull;

    if (m_gang != DrNull)
    {
        m_gang->Discard();
    }
    m_gang = DrNull;

    m_versionList = DrNull;
}

DrProcessTemplatePtr DrCohort::GetProcessTemplate()
{
    return m_processTemplate;
}

void DrCohort::SetGang(DrGangPtr gang)
{
    m_gang = gang;
}

DrGangPtr DrCohort::GetGang()
{
    return m_gang;
}

DrString DrCohort::GetDescription()
{
    return m_description;
}

DrActiveVertexListPtr DrCohort::GetMembers()
{
    return m_list;
}

DrCohortProcessPtr DrCohort::GetProcessForVersion(int version)
{
    int i;
    for (i=0; i<m_versionList->Size(); ++i)
    {
        if (m_versionList[i].m_version == version)
        {
            return m_versionList[i].m_process;
        }
    }
    return DrNull;
}

DrCohortProcessPtr DrCohort::EnsureProcess(DrGraphPtr graph, int version)
{
    DrCohortProcessPtr process = GetProcessForVersion(version);

    if (process == DrNull)
    {
        m_gang->StartVersion(graph, version);
        process = GetProcessForVersion(version);
    }

    DrAssert(process != DrNull);
    return process;
}

void DrCohort::PrepareDescription()
{
    if (m_list->Size() > 1)
    {
        m_description = "vertices";
    }
    else
    {
        m_description = "vertex";
    }

    int i;
    for (i=0; i<m_list->Size(); ++i)
    {
        m_description = m_description.AppendF(" %s", m_list[i]->GetDescription().GetChars());
    }
}

void DrCohort::StartProcess(DrGraphPtr graph, int version)
{
    int i;
    for (i=0; i<m_versionList->Size(); ++i)
    {
        DrAssert(m_versionList[i].m_version != version);
    }

    DrString processName;
    processName.SetF("%s v.%d", m_description.GetChars(), version);

    DrString commandLine;
    commandLine.SetF("%s --vertex --startfrompn %d",
                     m_processTemplate->GetCommandLineBase().GetChars(), m_list->Size());

    for (i=0; i<m_list->Size(); ++i)
    {
        commandLine = commandLine.AppendF(" %d %d", m_list[i]->GetId(), version);
    }

    DrCohortProcessRef process =
        DrNew DrCohortProcess(graph, this, version, m_list->Size(),
                              m_processTemplate->GetTimeOutBetweenProcessEndAndVertexNotification());

    VersionProcess vp;
    vp.m_version = version;
    vp.m_process = process;
    m_versionList->Add(vp);

    DrAffinityListRef affinity = DrNew DrAffinityList();
    for (i=0; i<m_list->Size(); ++i)
    {
        m_list[i]->AddCurrentAffinitiesToList(version, affinity);
    }

	graph->IncrementInFlightProcesses();

    /* hand off the computation to merge the affinities (which can be slow) and the actual call to
       start the process onto the work queue */
    DrCohortStartInfoRef info = DrNew DrCohortStartInfo(graph->GetXCompute(), process,
                                                        processName, commandLine,
                                                        m_processTemplate, affinity);
    DrCohortStarterRef starter = DrNew DrCohortStarter();
    DrCohortStartMessageRef message = DrNew DrCohortStartMessage(starter, info);
    graph->GetXCompute()->GetMessagePump()->EnQueue(message);
}

void DrCohort::NotifyProcessHasStarted(int version)
{
    int i;
    for (i=0; i<m_versionList->Size(); ++i)
    {
        if (m_versionList[i].m_version == version)
        {
            break;
        }
    }
    DrAssert(i < m_versionList->Size());
    DrLockBox<DrProcess> process = m_versionList[i].m_process->GetProcess();

    for (i=0; i<m_list->Size(); ++i)
    {
        m_list[i]->ReactToStartedProcess(version, process);
    }
}

void DrCohort::NotifyProcessComplete(int version)
{
    int i;
    for (i=0; i<m_versionList->Size(); ++i)
    {
        if (m_versionList[i].m_version == version)
        {
            break;
        }
    }
    DrAssert(i < m_versionList->Size());
    m_versionList->RemoveAt(i);

	GetGraph()->DecrementInFlightProcesses();

    /* we don't need to tell the member vertices: the only reason we got here
       was that they all called NotifyVertexCompletion on the DrCohortProcess
       that is now calling us */
}

DrGraphPtr DrCohort::GetGraph()
{
	DrAssert(m_list != DrNull && m_list->Size() > 0);
	return m_list[0]->GetStageManager()->GetGraph();
}

void DrCohort::CancelVertices(int version, DrErrorPtr error)
{
    DrLogI("Cancelling cohort vertices for version %d", version);
    DrCohortProcessRef cohortProcess;

    int i;
    for (i=0; i<m_versionList->Size(); ++i)
    {
        if (m_versionList[i].m_version == version)
        {
            cohortProcess = m_versionList[i].m_process;
            break;
        }
    }

    for (i=0; i<m_list->Size(); ++i)
    {
        DrLogI("Cancelling cohort vertex %d.%d", m_list[i]->GetId(), version);
        m_list[i]->CancelVersion(version, error, cohortProcess);
    }

    /* if there was an entry in m_versionList above (so cohortProcess is non-NULL) then the vertices
       will all have terminateed and told the cohortProcess so, and the cohortProcess will have
       called us back on NotifyVersionComplete, and so it will have been removed from the list */
    for (i=0; i<m_versionList->Size(); ++i)
    {
        DrAssert(m_versionList[i].m_version != version);
    }
}

void DrCohort::AssimilateOther(DrCohortPtr other)
{
    DrAssert(m_processTemplate == other->m_processTemplate);

    DrActiveVertexListRef otherList = other->GetMembers();
    int i;
    for (i=0; i<otherList->Size(); ++i)
    {
        DrActiveVertexPtr otherMember = otherList[i];
        DrAssert(otherMember->GetCohort() == other);
        otherMember->SetCohort(this);
        m_list->Add(otherMember);
    }

    PrepareDescription();

    DrGangPtr otherGang = other->GetGang();
    otherGang->RemoveCohort(other);

    DrGang::Merge(otherGang, m_gang);
}

void DrCohort::Merge(DrCohortRef c1, DrCohortRef c2)
{
    if (c1->GetMembers()->Size() > c2->GetMembers()->Size())
    {
        c1->AssimilateOther(c2);
    }
    else
    {
        c2->AssimilateOther(c1);
    }
}


DrGang::DrGang(DrCohortPtr initialCohort, DrStartCliquePtr initialStartClique)
{
    m_cohort = DrNew DrCohortList();
    m_cohort->Add(initialCohort);

    m_clique = DrNew DrStartCliqueList();
    m_clique->Add(initialStartClique);

	m_pendingVersion = 0;
	m_runningVersion = DrNew DrRunningGangList();
	m_completedVersion = 0;

    m_nextVersion = 1;
}

void DrGang::Discard()
{
    m_cohort = DrNull;
    m_clique = DrNull;
}

void DrGang::IncrementUnreadyVertices()
{
    ++m_unreadyVertexCount;
}

void DrGang::DecrementUnreadyVertices()
{
    DrAssert(m_unreadyVertexCount > 0);
    --m_unreadyVertexCount;

    if (m_unreadyVertexCount == 0)
    {
		EnsurePendingVersion(0);
    }
}

bool DrGang::VerticesAreReady()
{
    return (m_unreadyVertexCount == 0);
}

DrCohortListPtr DrGang::GetCohorts()
{
    return m_cohort;
}

DrStartCliqueListPtr DrGang::GetStartCliques()
{
    return m_clique;
}

void DrGang::RemoveCohort(DrCohortPtr cohort)
{
    bool removed = m_cohort->Remove(cohort);
    DrAssert(removed);
}

void DrGang::RemoveStartClique(DrStartCliquePtr startClique)
{
    bool removed = m_clique->Remove(startClique);
    DrAssert(removed);
}

void DrGang::Merge(DrGangRef g1, DrGangRef g2)
{
    if (g1 == g2)
    {
        return;
    }

    if (g1->GetCohorts()->Size() + g1->GetStartCliques()->Size() >
        g2->GetCohorts()->Size() + g2->GetStartCliques()->Size())
    {
        g1->AssimilateOther(g2);
    }
    else
    {
        g2->AssimilateOther(g1);
    }
}

void DrGang::AssimilateOther(DrGangPtr other)
{
    int i;

    DrCohortListRef otherCohort = other->GetCohorts();
    for (i=0; i<otherCohort->Size(); ++i)
    {
        DrCohortRef c = otherCohort[i];
        c->SetGang(this);
        m_cohort->Add(c);
    }

    DrStartCliqueListRef otherClique = other->GetStartCliques();
    for (i=0; i<otherClique->Size(); ++i)
    {
        DrStartCliqueRef c = otherClique[i];
        c->SetGang(this);
        m_clique->Add(c);
    }
}

void DrGang::StartVersion(DrGraphPtr graph, int version)
{
    DrAssert(version < m_nextVersion);

	DrAssert(version == m_pendingVersion);
	m_pendingVersion = 0;

	DrRunningGang rv;
	rv.m_version = version;
	rv.m_verticesLeftToComplete = 0;

	/* count the number of vertices in the gang */
    int i;
    for (i=0; i<m_cohort->Size(); ++i)
    {
		rv.m_verticesLeftToComplete += m_cohort[i]->GetMembers()->Size();
	}

	m_runningVersion->Add(rv);

    /* if we had a gang-scheduling interface to XCompute we would be calling it here */
    for (i=0; i<m_cohort->Size(); ++i)
    {
        m_cohort[i]->StartProcess(graph, version);
    }
}

void DrGang::CancelAllVersions(DrErrorPtr error)
{
    DrLogI("Canceling all versions for gang");
	if (m_pendingVersion > 0)
	{
        DrLogI("Canceling pending version %d", m_pendingVersion);
		CancelVersion(m_pendingVersion, error);
	}

	while (m_runningVersion->Size() > 0)
	{
        DrLogI("Canceling running version %d", m_runningVersion[0].m_version);
		CancelVersion(m_runningVersion[0].m_version, error);
	}

	/* make sure we didn't try to schedule another version. The
	   graph should be shutting down if this was called, which
	   should be blocking new pending versions from being made
	   since GetGraph()->IsRunning() should be false */
	DrAssert(m_pendingVersion == 0);
}

void DrGang::CancelVersion(int version, DrErrorPtr error)
{
    DrLogI("Canceling version %d for gang", version);
    DrAssert(version < m_nextVersion);

    int i;
    for (i=0; i<m_cohort->Size(); ++i)
    {
        DrLogI("Canceling m_cohort[%d] version %d", i, version);
        m_cohort[i]->CancelVertices(version, error);
    }

    for (i=0; i<m_clique->Size(); ++i)
    {
        m_clique[i]->NotifyVersionRevoked(version);
    }

	if (m_pendingVersion == version)
	{
		m_pendingVersion = 0;

		/* check our invariant holds */
		for (i=0; i<m_cohort->Size(); ++i)
		{
			int j;
			for (j=0; j < m_cohort[i]->GetMembers()->Size(); ++j)
			{
#ifdef _MANAGED
				DrAssert(m_cohort[i]->GetMembers()[j]->HasPendingVersion() == false);
#else
				DrAssert(m_cohort[i]->GetMembers()->Get(j)->HasPendingVersion() == false);
#endif
			}
		}
	}

	for (i=0; i<m_runningVersion->Size(); ++i)
	{
		if (m_runningVersion[i].m_version == version)
		{
			m_runningVersion->RemoveAt(i);

			/* check our invariant holds */
			int j;
			for (j=0; j<m_cohort->Size(); ++j)
			{
				int k;
				for (k=0; k < m_cohort[j]->GetMembers()->Size(); ++k)
				{
#ifdef _MANAGED
					DrAssert(m_cohort[j]->GetMembers()[k]->HasRunningVersion(version) == false);
#else
					DrAssert(m_cohort[j]->GetMembers()->Get(k)->HasRunningVersion(version) == false);
#endif
				}
			}

			break;
		}
	}

	if (m_completedVersion == version)
	{
		m_completedVersion = 0;

		/* check our invariant holds */
		for (i=0; i<m_cohort->Size(); ++i)
		{
			int j;
			for (j=0; j < m_cohort[i]->GetMembers()->Size(); ++j)
			{
#ifdef _MANAGED
				DrAssert((m_cohort[i]->GetMembers()[j])->HasCompletedVersion(version) == false);
#else
				DrAssert((m_cohort[i]->GetMembers()->Get(j))->HasCompletedVersion(version) == false);
#endif
			}
		}
	}

	EnsurePendingVersion(0);
}

void DrGang::EnsurePendingVersion(int duplicateVersion)
{
    int i;

	if (GetGraph()->IsRunning() == false)
	{
		/* we are shutting down so don't do any more */
		return;
	}

	if (VerticesAreReady() == false)
	{
		/* some vertex is blocked by its stage from starting, so wait until it
		   unblocks */
		return;
	}

	if (m_completedVersion != 0)
	{
		/* there is already a consistent completed version held by every vertex in
		   the gang, so no need to start a new one */
		return;
	}

	if (m_pendingVersion != 0)
	{
		/* there is already a version pending, i.e. waiting for inputs, so no need
		   to start a new one */
		return;
	}

	if (m_runningVersion->Size() > 0)
	{
		/* there is a version running. Only start a new one if we've been told to */
		if (duplicateVersion == 0)
		{
			/* we aren't been told to duplicate, so don't */
			return;
		}

		if (m_runningVersion->Size() > 2)
		{
			/* don't have more than three copies running at a given time: maybe this vertex
			   is just slow... */
			return;
		}

		for (i=0; i<m_runningVersion->Size(); ++i)
		{
			if (m_runningVersion[i].m_version >= duplicateVersion)
			{
				/* a duplicate has already been started for this version or a subsequent one,
				   so no need to do so again */
				return;
			}
		}
	}

	/* OK, we should set up a new pending version in every vertex in the gang */
    int newVersion = m_nextVersion;
    ++m_nextVersion;
	m_pendingVersion = newVersion;

    for (i=0; i<m_clique->Size(); ++i)
    {
        m_clique[i]->InstantiateVersion(newVersion);
    }

	/* and then start anyone that is ready */
    for (i=0; i<m_clique->Size(); ++i)
    {
		/* StartVersionIfReady may actually kick off a process if all the inputs
		   are ready, and that would reset m_pendingVersion to 0 and add the version
		   to m_runningVersion, so it's important to use the local variable newVersion
		   in this call instead of just passing in m_pendingVersion */
        m_clique[i]->StartVersionIfReady(newVersion);
    }
}

void DrGang::ReactToCompletedVertex(int version)
{
	bool completed = false;
	bool foundMatch = false;

	int i;
	for (i=0; i<m_runningVersion->Size(); ++i)
	{
		if (m_runningVersion[i].m_version == version)
		{
			DrRunningGang rv = m_runningVersion[i];
			DrAssert(rv.m_verticesLeftToComplete > 0);
			--rv.m_verticesLeftToComplete;
			m_runningVersion[i] = rv;

			if (rv.m_verticesLeftToComplete == 0)
			{
				m_runningVersion->RemoveAt(i);

				DrAssert(m_completedVersion == 0);
				m_completedVersion = version;

				completed = true;
			}

			foundMatch = true;
			break;
		}
	}

    if (!foundMatch)
    {
        DrLogE("Failed to find match for running version %d", version);
        DrLogE("m_runningVersion->Size() = %d", m_runningVersion->Size());
        DrLogE("m_completedVersion %d", m_completedVersion);
        DrLogE("m_pendingVersion %d", m_pendingVersion);
        DrLogE("m_nextVersion %d", m_nextVersion);
        DrLogE("m_unreadyVertexCount %d", m_unreadyVertexCount);
    }
	DrAssert(foundMatch);

	if (completed)
	{
		if (m_pendingVersion != 0)
		{
	        DrString reason = "Pending cohort being cancelled because a duplicate gang completed";
		    DrErrorRef error = DrNew DrError(DrError_CohortShutdown, "DrGang", reason);

			DrLogI("Canceling pending version %d", m_pendingVersion);
			CancelVersion(m_pendingVersion, error);
		}

		while (m_runningVersion->Size() > 0)
		{
	        DrString reason = "Running cohort being cancelled because a duplicate gang completed";
		    DrErrorRef error = DrNew DrError(DrError_CohortShutdown, "DrGang", reason);

			/* CancelVersion removes the relevant record from the running version list,
			   hence using a while loop here instead of a for loop */
			DrLogI("Canceling running version %d", m_runningVersion[0].m_version);
			CancelVersion(m_runningVersion[0].m_version, error);
		}
	}
}

DrGraphPtr DrGang::GetGraph()
{
	DrAssert(m_cohort != DrNull && m_cohort->Size() > 0);
	return m_cohort[0]->GetGraph();
}