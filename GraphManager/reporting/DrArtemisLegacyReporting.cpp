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

#include <DrReporting.h>

//
// Prints a timestamp as MM/DD/YYYY HH:MM:SS.MS
// 
static void PrintTimestamp()
{
    SYSTEMTIME utc, local;
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    FileTimeToSystemTime(&ft, &utc);
    SystemTimeToTzSpecificLocalTime(NULL, &utc, &local);

    printf("[%02d/%02d/%04d %02d:%02d:%02d.%03u] ",
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds);
}

/*

// Abandoning duplicate scheduling of vertex 83.1 (InputTable__26[5])
vertexAbandonedRegex = new Regex(@"Abandoning duplicate scheduling of vertex (\d+)\.(\d+) \((.+)\)", RegexOptions.Compiled);

// BasicAggregate__10(1),(Super__3[0].0*Super__3[1].0*Super__3[2].0*Super__3[3].0*Super__3[4].0*Super__3[5].0*Super__3[6].0),(sherwood-068),0.1718728s
topologyRegex = new Regex(@"(.*),\((.*)\),\((\S+)\),([.0-9]+)s$",
                RegexOptions.Compiled);

// total=951722563162 local=37817665237 intrapod=189765117248 crosspod=724139780677
datareadRegex = new Regex(@"total=(\d+) local=(\d+) intrapod=(\d+) crosspod=(\d+)", RegexOptions.Compiled);

Also:

Timing Information Graph Start Time (\d+)
JM Finish Time: (\d+)
ABORTING:
\<ErrorString\>(.*)\</ErrorString\>
Application completed successfully.
Application failed with error code (.*)
Total running time in vertices successful/failed: ([.0-9]+)s\/([.0-9]+)s
Average job parallelism
*/

/*

// Created process execution record for vertex 33 (Super__0[0]) v.0 GUID {B0FC788F-1FFC-4D74-AFC4-3EDFF03AF11A}
vertexCreatedRegex = new Regex(@"\[(.*)\] Created process execution record for vertex (\d+) \((.*)\) v.(\d+) GUID \{?([-A-F0-9]+)\}?", 
                RegexOptions.Compiled);

// Process started for vertex 5 (Super__0[1]) v.0 GUID {73EA55E0-0326-43C4-AD61-CB0B8CF8FE49} machine sherwood-025
// Process started for vertices 23 (Merge__29) 24 (Apply__33) v.0 GUID {E945DC5D-9AF6-4732-8770-2A6BF7FA3041} machine sherwood-237
vertexStartRegex = new Regex(@"\[(.*)\] Process started for vert(\w+) (.*) v\.(.*) GUID \{?([-A-F0-9]+)\}? machine (\S+)",
                RegexOptions.Compiled);

*/

void DrArtemisLegacyReporter::ReceiveMessage(DrProcessInfoRef info)
{
    DrString processName;
    {
        DrLockBoxKey<DrProcess> process(info->m_process);
        processName = process->GetName();
    }

    if (info->m_state->m_state == DPS_Starting)
    {
        PrintTimestamp();
        printf("Created process execution record for %s GUID {%s}\n",
               processName.GetChars(), info->m_state->m_process->GetHandleIdAsString().GetChars());
        PrintTimestamp();
        printf("Process started for %s GUID {%s} machine %s\n",
               processName.GetChars(), info->m_state->m_process->GetHandleIdAsString().GetChars(),
               info->m_state->m_process->GetAssignedNode()->GetName().GetChars());
        fflush(stdout);
    }
}

/*

// Vertex 5.0 (Super__0[1]) machine sherwood-025 guid {73EA55E0-0326-43C4-AD61-CB0B8CF8FE49} status Vertex Has Completed, 
terminationRegex = new Regex(@"Vertex (\d+)\.(\d+) \((.+)\) machine (\S+) guid \{?([-0-9A-F]+)\}? status (.*)",
                RegexOptions.Compiled);

// Canceling vertex 1461.0 (Merge__13[258]) due to dependent failure
cancelRegex = new Regex(@"\[(.*)\] Canceling vertex (\d+)\.(\d+) \((.+)\) due to (.*)", RegexOptions.Compiled);

// Process was terminated Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 status The operation succeeded
terminatedRegex = new Regex(@"\[(.*)\] Process was terminated Vertex (\d+)\.(\d+) \((.+)\) GUID \{?([-A-F0-9]+)\}? machine (\S+) status (.*)",
                RegexOptions.Compiled);

// Process has failed Vertex 11.0 (Select__6[1]) GUID {C1E35A88-F5AD-4A26-BE5F-46B6D515623F} machine sherwood-118 status The operation succeeded
failedRegex = new Regex(@"\[(.*)\] Process has failed Vertex (\d+)\.(\d+) \((.+)\) GUID \{?([-A-F0-9]+)\}? machine (\S+) Exitcode (.*)",
                RegexOptions.Compiled);

// Timing Information 5 1 Super__0[1] 128654556602334453 0.0000 0.0000 0.0000 0.0000 0.2969
timingInfoRegex = new Regex(@"Timing Information (\d+) (\d+) (.+) (\d+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+) ([-.0-9]+)",
                RegexOptions.Compiled);

*/

void DrArtemisLegacyReporter::ReceiveMessage(DrVertexInfoRef info)
{
    DrString processGuid = "(no guid)";
    DrString machineName = "(no computer)";
 
    DrDateTime processSchedule = DrDateTime_Never;
    DrDateTime processStartCreate = DrDateTime_Never;
    DrDateTime processFinishCreate = DrDateTime_Never;

    if (info->m_process.IsEmpty() == false)
    {
        DrLockBoxKey<DrProcess> process(info->m_process);
        DrProcessHandlePtr handle = process->GetInfo()->m_state->m_process;
        if (handle != DrNull)
        {
            processGuid = handle->GetHandleIdAsString();
            if (handle->GetAssignedNode() != DrNull)
            {
                machineName = handle->GetAssignedNode()->GetName();
            }
        }

        processSchedule = process->GetInfo()->m_jmProcessScheduledTime;
        processStartCreate = process->GetInfo()->m_statistics->m_createdTime;
        processFinishCreate = process->GetInfo()->m_statistics->m_beginExecutionTime;
    }

    if (info->m_state == DVS_Completed)
    {
        PrintTimestamp();
        printf("Vertex %d.%d (%s) machine %s guid {%s} status Vertex Has Completed\n",
               info->m_info->GetProcessStatus()->GetVertexId(),
               info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
               info->m_name.GetChars(), machineName.GetChars(), processGuid.GetChars());
    }
    else if (info->m_state == DVS_Failed)
    {
        if (info->m_statistics->m_exitStatus == DrError_CohortShutdown)
        {
            PrintTimestamp();
            printf("Canceling vertex %d.%d (%s) due to dependent failure\n",
                   info->m_info->GetProcessStatus()->GetVertexId(),
                   info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
                   info->m_name.GetChars());
        }

        if (info->m_statistics->m_exitStatus == DrError_Unexpected)
        {
            PrintTimestamp();
            printf("Process was terminated Vertex %d.%d (%s) GUID {%s} machine %s status The operation succeeded\n",
                   info->m_info->GetProcessStatus()->GetVertexId(),
                   info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
                   info->m_name.GetChars(), processGuid.GetChars(), machineName.GetChars());
        }
        else
        {
            PrintTimestamp();
            printf("Process has failed Vertex %d.%d (%s) GUID {%s} machine %s Exitcode %x\n",
                   info->m_info->GetProcessStatus()->GetVertexId(),
                   info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
                   info->m_name.GetChars(), processGuid.GetChars(), machineName.GetChars(),
                   info->m_statistics->m_exitStatus);
        }
    }

    if (info->m_state == DVS_Completed || info->m_state == DVS_Failed)
    {
        if ((info->m_statistics != DrNull) 
            && (info->m_statistics->m_totalInputData != DrNull) 
            && (info->m_statistics->m_totalOutputData != DrNull))
        {
            printf("Io information %d %d %s read %I64u wrote %I64u\n",
                info->m_info->GetProcessStatus()->GetVertexId(),
                info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
                info->m_name.GetChars(),
                info->m_statistics->m_totalInputData->m_dataRead,
                info->m_statistics->m_totalOutputData->m_dataWritten);

            printf("Io locality information %d %d %s read %I64u ( %I64u local )\n",
                info->m_info->GetProcessStatus()->GetVertexId(),
                info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
                info->m_name.GetChars(),
                info->m_statistics->m_totalInputData->m_dataRead,
                info->m_statistics->m_totalLocalInputData);
        }
    }

    if (info->m_state >= DVS_Completed)
    {
        DrVertexExecutionStatisticsPtr eStats = info->m_statistics;
        DrDateTime execRunning = eStats->m_runningTime;
        DrDateTime completion = eStats->m_completionTime;

        if ((processSchedule == DrDateTime_Never) || (processSchedule < eStats->m_creationTime))
        {
            /* this vertex is part of a cohort, and was created after the
               process had already started */
            processSchedule = eStats->m_creationTime;
        }

        if ((processStartCreate == DrDateTime_Never) || (processStartCreate < processSchedule))
        {
            /* this vertex is part of a cohort, and was created after the
               process had already started, or clock skew means it appears to have started before it
               was scheduled */
            processStartCreate = processSchedule;
        }

        if ((processFinishCreate == DrDateTime_Never) || (processFinishCreate < processStartCreate))
        {
            /* this vertex is part of a cohort, and was created after the
               process creation had already completed */
            processFinishCreate = processStartCreate;
        }

        if ((execRunning == DrDateTime_Never) || (execRunning < processFinishCreate))
        {
            /* this vertex had never run */
            execRunning = processFinishCreate;
        }

        if ((completion == DrDateTime_Never) || (completion < execRunning))
        {
            /* clock skew??? */
            completion = execRunning;
        }

        double creatToScheduleTime =
            (double) (processSchedule - eStats->m_creationTime) / (double) DrTimeInterval_Second;
        double schedToStartProcessTime =
            (double) (processStartCreate - processSchedule) / (double) DrTimeInterval_Second;
        double pStartToCreatedProcessTime =
            (double) (processFinishCreate - processStartCreate) / (double) DrTimeInterval_Second;
        double cProcessToRunTime =
            (double) (execRunning - processFinishCreate) / (double) DrTimeInterval_Second;
        double runToCompTime =
            (double) (completion - execRunning) / (double) DrTimeInterval_Second;

        // No need to print timestamp for timing report
        printf("Timing Information %u %u %s %I64u %.4f %.4f %.4f %.4f %.4f\n",
               info->m_info->GetProcessStatus()->GetVertexId(),
               info->m_info->GetProcessStatus()->GetVertexInstanceVersion(),
               info->m_name.GetChars(),
               eStats->m_creationTime,
               creatToScheduleTime, schedToStartProcessTime, pStartToCreatedProcessTime,
               cProcessToRunTime, runToCompTime);
        fflush(stdout);
    }
}

void DrArtemisLegacyReporter::ReportFinalTopology(DrVertexPtr vertex, DrResourcePtr runningMachine,
                                                  DrTimeInterval runningTime)
{
    DrString machineName = "nowhere";
    if (runningMachine != DrNull)
    {
        machineName = runningMachine->GetName();
    }

    // No need to print timestamp for topology reporting
    printf("%s(%d),(", vertex->GetName().GetChars(), vertex->GetOutputs()->GetNumberOfEdges());

    int i;
    for (i=0; i<vertex->GetInputs()->GetNumberOfEdges(); ++i)
    {
        DrEdge e = vertex->GetInputs()->GetEdge(i);

        if (e.m_remoteVertex == DrNull)
        {
            printf("%sNULL", (i > 0) ? "*" : "");
        }
        else
        {
            printf("%s%s.%d", (i > 0) ? "*" : "", e.m_remoteVertex->GetName().GetChars(), e.m_remotePort);
        }
    }

    printf("),(%s),%lfs\n", machineName.GetChars(), (double) runningTime / (double) DrTimeInterval_Second);
    fflush(stdout);
}


void DrArtemisLegacyReporter::ReportStart(DrDateTime startTime)
{
    union {
        FILETIME    ft;
        DrDateTime  ts;
    };
    ts = startTime;
    SYSTEMTIME utc, local;
    FileTimeToSystemTime(&ft, &utc);
    SystemTimeToTzSpecificLocalTime(NULL, &utc, &local);

    printf("Start time: %02d/%02d/%04d %02d:%02d:%02d.%03u\n",
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds);

    fflush(stdout);
}

void DrArtemisLegacyReporter::ReportStop(UINT exitCode)
{
    SYSTEMTIME utc, local;
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    FileTimeToSystemTime(&ft, &utc);
    SystemTimeToTzSpecificLocalTime(NULL, &utc, &local);

    printf("Stop time (Exit code = %u): %02d/%02d/%04d %02d:%02d:%02d.%03u\n",
        exitCode,
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds
        );

    fflush(stdout);
}