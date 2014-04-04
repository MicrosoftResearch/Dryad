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

DrGraphParameters::DrGraphParameters()
{
    m_reporters = DrNew DrIReporterRefList();
}

DrFailureInfo::DrFailureInfo()
{
    m_numberOfFailures = 0;
}

DrGraph::DrGraph(DrClusterPtr cluster, DrGraphParametersPtr parameters)
    : DrErrorNotifier(cluster->GetMessagePump())
{
    m_cluster = cluster;
    m_parameters = parameters;

    m_dictionary = DrNew DrFailureDictionary();
    m_stageList = DrNew DrStageList();
    m_partitionGeneratorList = DrNew DrPartitionGeneratorList();

    m_state = DGS_NotStarted;
    m_activeVertexCount = 0;
    m_activeVertexCompleteCount = 0;

    DrActiveVertexOutputGenerator::s_intermediateCompressionMode = parameters->m_intermediateCompressionMode;
}

void DrGraph::Discard()
{
    m_cluster->Shutdown();
    m_cluster = DrNull;
    m_dictionary = DrNull;

    int i;
    for (i=0; i<m_stageList->Size(); ++i)
    {
        m_stageList[i]->Discard();
    }
    m_stageList = DrNull;

    m_partitionGeneratorList = DrNull;
}

void DrGraph::AddStage(DrStageManagerPtr stage)
{
    m_stageList->Add(stage);
}

DrStageListPtr DrGraph::GetStages()
{
    return m_stageList;
}

void DrGraph::AddPartitionGenerator(DrIOutputPartitionGeneratorPtr partitionGenerator)
{
    m_partitionGeneratorList->Add(partitionGenerator);
}

bool DrGraph::IsRunning()
{
    return (m_state == DGS_Running);
}

void DrGraph::StartRunning()
{
    DrAssert(m_state == DGS_NotStarted);

    m_state = DGS_Running;
    m_activeVertexCount = 0;
    m_activeVertexCompleteCount = 0;

    // Send a delayed message to renew the temporary output stream lease
    // before it expires
    DrLeaseMessageRef leaseMessage = DrNew DrLeaseMessage(this, true);
    m_cluster->GetMessagePump()->EnQueueDelayed(23 * DrTimeInterval_Hour, leaseMessage);

    // Send a delayed message to check for duplicate vertices
    DrDuplicateMessageRef duplicateMessage = DrNew DrDuplicateMessage(this, 0);
    m_cluster->GetMessagePump()->EnQueueDelayed(DrTimeInterval_Second, duplicateMessage);

    int i;
    for (i=0; i<m_stageList->Size(); ++i)
    {
        m_stageList[i]->InitializeForGraphExecution();
    }

    m_cluster->IncrementTotalSteps(false);  // Add a step for initialization
    m_cluster->IncrementProgress("initialization complete");

    for (i=0; i<m_stageList->Size(); ++i)
    {
        m_stageList[i]->KickStateMachine();
    }
}

void DrGraph::TriggerShutdown(DrErrorRef status)
{
    HRESULT exitCode = 0;

    DrLogI("Triggering shutdown in state %d", m_state);
    if (m_state == DGS_Running)
    {
        m_exitStatus = status;

        /* Write error, if any, to stderr so it will appear in HPC console */
        if (status != DrNull && status->m_code != 0)
        {
            exitCode = status->m_code;

            /* Write the proximate error */
            if (status->m_explanation.GetChars() != DrNull)
            {
                fprintf(stderr, status->m_explanation.GetChars());
                fprintf(stderr, "\n");
            }
            else
            {
                fprintf(stderr, "Error (0x%08X): %s\n", status->m_code, DRERRORSTRING(status->m_code));
            }
            
            /* Look at the provenance and write any previous errors, for additional detail */
            if (status->m_errorProvenance != DrNull)
            {
                for (int i = 0; i < status->m_errorProvenance->Size(); ++i)
                {
#ifdef _MANAGED
                    DrErrorRef previousError = status->m_errorProvenance[i];
#else     
                    DrErrorRef previousError = status->m_errorProvenance->Get(i);
#endif
                    if (previousError != DrNull)
                    {
                        if (previousError->m_explanation.GetChars() != DrNull)
                        {
                            fprintf(stderr, previousError->m_explanation.GetChars());
                            fprintf(stderr, "\n");
                        }
                        else
                        {
                            fprintf(stderr, "Previous error (0x%08X): %s\n", previousError->m_code, DRERRORSTRING(previousError->m_code));
                        } 
                    }
                }
                /* OMC: Old Managed code.
                array<DrErrorRef> ^previousErrors = status->m_errorProvenance->ToArray();
                for (int i = 0; i < previousErrors->Length; i++)
                {
                    if (previousErrors[i] != DrNull)
                    {
                        if (previousErrors[i]->m_explanation.GetChars() != DrNull)
                        {
                            fprintf(stderr, previousErrors[i]->m_explanation.GetChars());
                            fprintf(stderr, "\n");
                        }
                        else
                        {
                            fprintf(stderr, "Previous error (0x%08X): %s\n", previousErrors[i]->m_code, DRERRORSTRING(previousErrors[i]->m_code));
                        }
                    }
                }
                */
            }
            fflush(stderr);
        }

        /* Send ourself a message to do the actual shutdown */
        DrShutdownMessageRef message = DrNew DrShutdownMessage(this, exitCode);
        m_cluster->GetMessagePump()->EnQueue(message);
    }
}

void DrGraph::ReceiveMessage(DrErrorRef /* unused abortError*/)
{
	if (m_state == DGS_Stopping)
	{
		DrLogI("Received process shutdown timeout");

		FinalizeGraph();
	}
}

void DrGraph::FinalizeGraph()
{
    DrAssert(m_state == DGS_Stopping);

    m_state = DGS_Stopped;

	if (m_exitStatus == DrNull || SUCCEEDED(m_exitStatus->m_code))
	{
		HRESULT err = S_OK;
		int i;
		for (i=0; SUCCEEDED(err) && i<m_partitionGeneratorList->Size(); ++i)
		{
            DrString errorText;
			err = m_partitionGeneratorList[i]->FinalizeSuccessfulParts(true, errorText);
			if (!SUCCEEDED(err))
			{
				DrString reason;
                reason.SetF("Failed to finalize outputs: %s", errorText.GetChars());
				m_exitStatus = DrNew DrError(err, "DrGraph", reason);
			}
		}
	}

	if (m_exitStatus != DrNull && !SUCCEEDED(m_exitStatus->m_code))
    {
		int i;
		for (i=0; i<m_partitionGeneratorList->Size(); ++i)
		{
            // don't change the exit status if we fail to delete zombie outputs
            DrString errorText;
			m_partitionGeneratorList[i]->FinalizeSuccessfulParts(false, errorText);
		}
    }

    for (int r=0; r<m_parameters->m_reporters->Size(); ++r)
	{
        DrIReporterPtr reporter = m_parameters->m_reporters[r];
		int i;
		for (i=0; i<m_stageList->Size(); ++i)
		{
			DrVertexListRef vList = m_stageList[i]->GetVertexVector();
			int j;
			for (j=0; j<vList->Size(); ++j)
			{
				vList[j]->ReportFinalTopology(reporter);
			}
		}
	}

	DeliverNotification(m_exitStatus);
}

void DrGraph::ReceiveMessage(DrLeaseExtender /* unused leaseMessage */)
{
    int i;
    for (i=0; i<m_partitionGeneratorList->Size(); ++i)
    {
        m_partitionGeneratorList[i]->ExtendLease(DrTimeInterval_Day);
    }
    DrLeaseMessageRef message = DrNew DrLeaseMessage(this, true);
    m_cluster->GetMessagePump()->EnQueueDelayed(23 * DrTimeInterval_Hour, message);
}

void DrGraph::ReceiveMessage(DrDuplicateChecker /* unused checkDuplicate */)
{
    int i;
	for (i=0; i<m_stageList->Size(); ++i)
    {
		m_stageList[i]->CheckForDuplicates();
    }

	DrDuplicateMessageRef message = DrNew DrDuplicateMessage(this, 0);
    m_cluster->GetMessagePump()->EnQueueDelayed(DrTimeInterval_Second, message);
}

void DrGraph::ReceiveMessage(DrExitStatus /* unused exitStatus */)
{
    DrLogI("Receiving shutdown message in state %d", m_state);
    if (m_state == DGS_Running)
    {
        m_state = DGS_Stopping;

		if (m_inFlightProcessCount == 0)
		{
            DrLogI("No processes in flight, finalizing graph");
			FinalizeGraph();
		}
		else
		{
			/* tell all the outstanding versions to cancel themselves */
			int i;
			for (i=0; i<m_stageList->Size(); ++i)
			{
                DrLogI("Cancelling all vertices in stage %d", i);
                DrString abortReason = "Job is being canceled";
                DrErrorRef error =
                    DrNew DrError(DrError_CohortShutdown, "DrGraph", abortReason);
				m_stageList[i]->CancelAllVertices(error);
			}

			/* now we will wait until the final outstanding process calls DecrementInFlightProcesses
			   at which point FinalizeGraph will be called */

			if (m_parameters->m_processAbortTimeOut < DrTimeInterval_Infinite)
            {
				/* set a timeout in case some processes don't abort; if this fires then ReceiveMessage
				   will get the error */
                DrString reason = "Process abort timed out";
                DrErrorRef error =
                    DrNew DrError(HRESULT_FROM_WIN32(ERROR_TIMEOUT), "DrGraph", reason);
				DrErrorMessageRef message = DrNew DrErrorMessage(this, error);
				m_cluster->GetMessagePump()->EnQueueDelayed(m_parameters->m_processAbortTimeOut, message);
            }
		}
    }
}

void DrGraph::IncrementInFlightProcesses()
{
	DrAssert(m_state == DGS_Running);
	++m_inFlightProcessCount;
}

void DrGraph::DecrementInFlightProcesses()
{
	DrAssert(m_inFlightProcessCount > 0);
	--m_inFlightProcessCount;

	if (m_state == DGS_Stopping)
	{
		DrLogI("Stopping: waiting for %d processes", m_inFlightProcessCount);

		if (m_inFlightProcessCount == 0)
		{
			FinalizeGraph();
		}
	}
}

void DrGraph::IncrementActiveVertexCount()
{
    ++m_activeVertexCount;
    m_cluster->IncrementTotalSteps(false);
}

void DrGraph::DecrementActiveVertexCount()
{
    --m_activeVertexCount;
    m_cluster->DecrementTotalSteps(false);
}

void DrGraph::NotifyActiveVertexComplete()
{
    DrAssert(m_activeVertexCompleteCount < m_activeVertexCount);
    ++m_activeVertexCompleteCount;

    DrLogI("Got %d/%d complete active vertices", m_activeVertexCompleteCount, m_activeVertexCount);

    if (m_activeVertexCompleteCount == m_activeVertexCount)
    {
        DrLogI("Triggering shutdown");
        TriggerShutdown(DrNull);
    }
}

void DrGraph::NotifyActiveVertexRevoked()
{
    DrAssert(m_activeVertexCompleteCount > 0);
    --m_activeVertexCompleteCount;

    DrLogI("Revoking: got %d/%d complete active vertices", m_activeVertexCompleteCount, m_activeVertexCount);

    if (m_activeVertexCompleteCount == m_activeVertexCount)
    {
        TriggerShutdown(DrNull);
    }
}

DrClusterPtr DrGraph::GetCluster()
{
    return m_cluster;
}

DrGraphParametersPtr DrGraph::GetParameters()
{
    return m_parameters;
}

int DrGraph::ReportFailure(DrActiveVertexPtr vertex, int version,
                           DrVertexProcessStatusPtr status, DrErrorPtr error)
{
    /* TODO much more sophisticated here */

    if (status != DrNull)
    {
        DrInputChannelArrayRef inputs = status->GetInputChannels();
        int i;
        for (i=0; i<inputs->Allocated(); ++i)
        {
            DrChannelDescriptionPtr c = inputs[i];
            HRESULT err = c->GetChannelState();
            if ((err != S_OK) &&
                (err != DrError_EndOfStream) &&
                (err != DrError_ProcessingInterrupted))
            {
                DrLogI("Reporting read error %s for vertex %d.%d input channel %d %s",
                    DRERRORSTRING(err), vertex->GetId(), version, i, c->GetChannelURI().GetChars());
                return i;
            }
        }
    }

    if (error->m_code == DrError_CohortShutdown)
    {
        /* not our fault */
        return -1;
    }

    DrFailureInfoRef info;
    if (m_dictionary->TryGetValue(vertex, info) == false)
    {
        info = DrNew DrFailureInfo();
        m_dictionary->Add(vertex, info);
    }
    ++(info->m_numberOfFailures);
    if (info->m_numberOfFailures == m_parameters->m_maxActiveFailureCount)
    {
        DrLogI("Triggering graph abort because vertex %d failed %d times", vertex->GetId(), info->m_numberOfFailures);

        DrString reason;

        if (status != DrNull &&
            status->GetVertexErrorString().GetString() != DrNull)
		{
			reason.SetF("Graph abort because vertex failed %d times: vertex %d in stage %s\n\nVERTEX FAILURE DETAILS:\n%s", info->m_numberOfFailures, 
				vertex->GetId(), 
				vertex->GetStageManager()->GetStageName().GetChars(),
				status->GetVertexErrorString().GetChars());
		}
		else
		{
			reason.SetF("Graph abort because vertex failed %d times: vertex %d, part %d in stage %s\n", info->m_numberOfFailures, 
				vertex->GetId(), vertex->GetPartitionId(),
				vertex->GetStageManager()->GetStageName().GetChars());
		}

        DrErrorRef newError = DrNew DrError(DrError_VertexError, "DrGraph", reason);
        newError->AddProvenance(error);
        TriggerShutdown(newError);
    }

    return -1;
}

void DrGraph::ReportStorageFailure(DrStorageVertexPtr vertex, DrErrorPtr originalError)
{
    DrFailureInfoRef info;
    if (m_dictionary->TryGetValue(vertex, info) == false)
    {
        info = DrNew DrFailureInfo();
        m_dictionary->Add(vertex, info);
    }
    ++(info->m_numberOfFailures);
    if (info->m_numberOfFailures == m_parameters->m_maxActiveFailureCount)
    {
		DrLogI("Triggering abort because input read failed %d times vertex %d",
			   info->m_numberOfFailures, vertex->GetId());

		DrString reason;
        reason.SetF("Graph abort because input read failed %d times: vertex %d, part %d in stage %s",
            info->m_numberOfFailures, vertex->GetId(), vertex->GetPartitionId(), vertex->GetStageManager()->GetStageName().GetChars());
        DrErrorRef error = DrNew DrError(DrError_InputUnavailable, "DrGraph", reason);
        error->AddProvenance(originalError);

        DrLogI("Triggering shutdown with error: %s", error->ToFullText().GetChars());

        TriggerShutdown(error);
    }
}
