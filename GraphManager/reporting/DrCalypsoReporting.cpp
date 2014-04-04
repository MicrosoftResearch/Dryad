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

#include <DrStageHeaders.h>
#include <DrCalypsoReporting.h>

#include <msclr\lock.h>

using namespace System::IO;
using namespace Microsoft::Research::Peloponnese::Storage;
using namespace msclr;

//
// Prints a timestamp as MM/DD/YYYY HH:MM:SS.MS
// 
static System::String^ RecordTimeInternal()
{
    DYNAMIC_TIME_ZONE_INFORMATION zone;
    SYSTEMTIME local;
    int firstTimeZone = GetDynamicTimeZoneInformation(&zone);
    GetLocalTime(&local);
    int timeZone = GetDynamicTimeZoneInformation(&zone);
    if (timeZone != firstTimeZone)
    {
        // yes, this is checking for the race that daylight savings time started or ended between the two calls. Sorry
        GetLocalTime(&local);
    }

    int bias;
    if (timeZone == TIME_ZONE_ID_STANDARD)
    {
        bias = -(zone.Bias + zone.StandardBias);
    }
    else if (timeZone == TIME_ZONE_ID_DAYLIGHT)
    {
        bias = -(zone.Bias + zone.DaylightBias);
    }
    int hourBias = bias / 60;
    int minuteBias = abs(bias) % 60;

    return System::String::Format("logtimelocal={0,2:D2}/{1,2:D2}/{2,4:D4} {3,2:D2}:{4,2:D2}:{5,2:D2}.{6,3:D3}UTC{7}{8}:{9,2:D2}",
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds,
        (hourBias >= 0) ? "+" : "", hourBias, minuteBias);
}

//
// Prints a timestamp as MM/DD/YYYY HH:MM:SS.MS
// 
static void WriteTimestamp(TextWriter^ writer, System::String^ key, DrDateTime timestamp)
{
    union {
        FILETIME    ft;
        DrDateTime  ts;
    };
    ts = timestamp;
    SYSTEMTIME utc;
    GetSystemTimeAsFileTime(&ft);
    FileTimeToSystemTime(&ft, &utc);

    writer->Write(",{0}={1,2:D2}/{2,2:D2}/{3,4:D4} {4,2:D2}:{5,2:D2}:{6,2:D2}.{7,3:D3}",
        key,
        utc.wMonth,
        utc.wDay,
        utc.wYear,
        utc.wHour,
        utc.wMinute,
        utc.wSecond,
        utc.wMilliseconds);
}

static void WriteKeyValue(TextWriter^ writer, System::String^ key, System::String^ value)
{
    value = value->Replace('"', '\'');
    if (value->IndexOf(',') >= 0 || value->IndexOf('\n') >= 0)
    {
        writer->Write(",{0}=\"{1}\"", key, value);
    }
    else
    {
        writer->Write(",{0}={1}", key, value);
    }
}

ref class PeloponneseLogger : public Microsoft::Research::Peloponnese::ILogger
{
public:
    virtual void Log(System::String^ message, System::String^ file, System::String^ function, int line)
    {
        if (DrLogging::Enabled(DrLog_Info))
        {
            DrString sMessage(message);
            DrString sFile(file);
            DrString sFunction(function);
            DrLogHelper(DrLog_Info, sFile.GetChars(), sFunction.GetChars(), line)("%s", sMessage.GetChars());
        }
    }

    virtual void Stop()
    {
    }
};

DrCalypsoReporter::DrCalypsoReporter(DrNativeString uriString)
{
    System::Uri^ uri = DrNew System::Uri(uriString);
    if (uri->Scheme == AzureUtils::BlobScheme)
    {
        m_logStream = DrNew AzureLogAppendStream(uri, 0x20, false, false, gcnew PeloponneseLogger());
        m_flushInterval = 1000;
    }
    else if (uri->Scheme == "hdfs")
    {
        m_logStream = DrNew HdfsLogAppendStream(uri, false);
        // flushing hdfs logs is very expensive
        m_flushInterval = 2 * 60 * 1000;
    }
    else if (uri->Scheme == "file")
    {
        m_logStream = DrNew FileStream(uri->UnescapeDataString(uri->AbsolutePath), FileMode::Create, FileAccess::Write);
        m_flushInterval = 1000;
    }
    // use a big write buffer so we don't write tons of tiny pages
    m_writer = DrNew StreamWriter(m_logStream, System::Text::Encoding::UTF8, 4 * 1024 * 1024);
    m_stop = DrNew System::Threading::Tasks::TaskCompletionSource<bool>();
    m_flusher = DrNew System::Threading::Tasks::Task(DrNew System::Action(this, &DrCalypsoReporter::Flusher));
    m_flusher->Start();
}

DrCalypsoReporter::DrCalypsoReporter()
{
    m_logStream = DrNull;
    m_flushInterval = 1000;
    m_writer = System::Console::Out;
    m_stop = DrNew System::Threading::Tasks::TaskCompletionSource<bool>();
    m_flusher = DrNew System::Threading::Tasks::Task(DrNew System::Action(this, &DrCalypsoReporter::Flusher));
    m_flusher->Start();
}

void DrCalypsoReporter::Flusher()
{
    try
    {
        while (true)
        {
            bool stopped = m_stop->Task->Wait(m_flushInterval);
            if (stopped)
            {
                return;
            }

            {
                lock l(this);

                if (m_writer == DrNull)
                {
                    // discarded
                    return;
                }

                m_writer->Flush();
                if (m_logStream != DrNull)
                {
                    m_logStream->Flush();
                }
            }
        }
    }
    catch (System::Exception^ e)
    {
        DrString reason(e->ToString());
        DrLogE("Calypso flusher got exception: %s", reason.GetChars());
    }
}

System::String^ DrCalypsoReporter::RecordTime()
{
    return RecordTimeInternal();
}

void DrCalypsoReporter::BeginEntry(System::String^ entryTime, TextWriter^ writer)
{
    writer->Write(entryTime);
}

void DrCalypsoReporter::Discard()
{
    m_stop->TrySetResult(true);

    m_flusher->Wait();

    {
        lock l(this);

        if (m_logStream != DrNull)
        {
            try
            {
                delete m_writer;
                delete m_logStream;
            }
            catch (System::Exception^ e)
            {
                DrString reason(e->ToString());
                DrLogE("Calypso discard got exception: %s", reason.GetChars());
            }

            m_logStream = DrNull;
        }

        m_writer = DrNull;
    }
}

void DrCalypsoReporter::ReportStart(DrDateTime startTime)
{
    try
    {
        lock l(this);

        if (m_writer == DrNull)
        {
            // discarded
            return;
        }

        System::String^ entryTime = RecordTime();
        TextWriter^ writer = gcnew StringWriter();
        BeginEntry(entryTime, writer);
        WriteKeyValue(writer, "job", "start");
        WriteTimestamp(writer, "utctime", startTime);
        writer->WriteLine("");

        m_writer->Write(writer->ToString());
    }
    catch (System::Exception^ e)
    {
        DrString reason(e->ToString());
        DrLogE("Calypso got exception: %s", reason.GetChars());
        Discard();
    }
}

void DrCalypsoReporter::ReportStop(UINT exitCode, DrNativeString errorString, DrDateTime stopTime)
{
    try
    {
        lock l(this);

        if (m_writer == DrNull)
        {
            // discarded
            return;
        }

        System::String^ entryTime = RecordTime();
        TextWriter^ writer = gcnew StringWriter();

        BeginEntry(entryTime, writer);
        WriteKeyValue(writer, "job", "stop");
        WriteKeyValue(writer, "exitcode", exitCode.ToString("X8"));
        if (errorString != DrNull)
        {
            WriteKeyValue(writer, "errorstring", errorString);
        }
        WriteTimestamp(writer, "utctime", stopTime);
        writer->WriteLine("");

        m_writer->Write(writer->ToString());
    }
    catch (System::Exception^ e)
    {
        DrString reason(e->ToString());
        DrLogE("Calypso got exception: %s", reason.GetChars());
    }

    Discard();
}

static void WriteError(TextWriter^ writer, DrErrorPtr status)
{
    WriteKeyValue(writer, "statuscode", status->m_code.ToString("X8"));
    WriteKeyValue(writer, "statuscomponent", status->m_component.GetString());
    WriteKeyValue(writer, "statusdescription", status->m_explanation.GetString());
}

void DrCalypsoReporter::ReceiveMessage(DrProcessInfoRef info)
{
    DrDateTime processCreated = DrDateTime_Never;
    DrDateTime processScheduled = DrDateTime_Never;

    DrString processName;
    {
        DrLockBoxKey<DrProcess> process(info->m_process);
        processName = process->GetName();
        processCreated = process->GetInfo()->m_jmProcessCreatedTime;
        processScheduled = process->GetInfo()->m_jmProcessScheduledTime;
    }

    try
    {
        lock l(this);

        if (m_writer == DrNull)
        {
            // discarded
            return;
        }

        System::String^ entryTime = RecordTime();
        TextWriter^ writer = gcnew StringWriter();

        switch (info->m_state->m_state)
        {
        case DPS_Starting:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "process", info->m_state->m_process->GetHandleIdAsString().GetString());
            WriteKeyValue(writer, "transition", "starting");
            WriteKeyValue(writer, "machine", info->m_state->m_process->GetAssignedNode()->GetLocality().GetString());
            WriteKeyValue(writer, "id", info->m_state->m_process->GetDirectory().GetString());
            WriteKeyValue(writer, "name", processName.GetString());
            WriteTimestamp(writer, "utctime", info->m_state->m_creatingTime);
            writer->WriteLine("");
            break;

        case DPS_Created:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "process", info->m_state->m_process->GetHandleIdAsString().GetString());
            WriteKeyValue(writer, "transition", "created");
            WriteTimestamp(writer, "utctime", info->m_state->m_createdTime);
            writer->WriteLine("");
            break;

        case DPS_Running:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "process", info->m_state->m_process->GetHandleIdAsString().GetString());
            WriteKeyValue(writer, "transition", "running");
            WriteTimestamp(writer, "utctime", info->m_state->m_beginExecutionTime);
            writer->WriteLine("");
            break;

        case DPS_Completed:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "process", info->m_state->m_process->GetHandleIdAsString().GetString());
            WriteKeyValue(writer, "transition", "completed");
            WriteTimestamp(writer, "utctime", info->m_state->m_terminatedTime);
            writer->WriteLine("");
            break;

        case DPS_Failed:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "process", info->m_state->m_process->GetHandleIdAsString().GetString());
            WriteKeyValue(writer, "transition", "failed");
            WriteKeyValue(writer, "exitcode", info->m_state->m_exitCode.ToString());
            WriteError(writer, info->m_state->m_status);
            WriteTimestamp(writer, "utctime", info->m_state->m_terminatedTime);
            writer->WriteLine("");
            break;

        default:
            break;
        }

        m_writer->Write(writer->ToString());
    }
    catch (System::Exception^ e)
    {
        DrString reason(e->ToString());
        DrLogE("Calypso got exception: %s", reason.GetChars());
        Discard();
    }
}

void DrCalypsoReporter::ReceiveMessage(DrVertexInfoRef info)
{
    System::String^ vertexId = info->m_info->GetProcessStatus()->GetVertexId().ToString();
    System::String^ vertexVersion = info->m_info->GetProcessStatus()->GetVertexInstanceVersion().ToString();

    System::String^ processGuid = "(no guid)";
    System::String^ machineName = "(no computer)";
    System::String^ idOnComputer = "(no id)";

    if (info->m_process.IsEmpty() == false)
    {
        DrLockBoxKey<DrProcess> process(info->m_process);
        DrProcessHandlePtr handle = process->GetInfo()->m_state->m_process;
        if (handle != DrNull)
        {
            idOnComputer = handle->GetAssignedNode()->GetName().GetString() + ":" + handle->GetDirectory().GetString();
            processGuid = handle->GetHandleIdAsString().GetString();
            if (handle->GetAssignedNode() != DrNull)
            {
                machineName = handle->GetAssignedNode()->GetLocality().GetString();
            }
        }
    }

    try
    {
        lock l(this);

        if (m_writer == DrNull)
        {
            // discarded
            return;
        }

        System::String^ entryTime = RecordTime();
        TextWriter^ writer = gcnew StringWriter();

        DrVertexProcessStatusPtr status = DrNull;
        if (info->m_info != DrNull)
        {
            status = info->m_info->GetProcessStatus();
        }

        switch (info->m_state)
        {
        case DVS_NotStarted:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "name", info->m_name.GetString());
            if (info->m_partInStage >= 0)
            {
                WriteKeyValue(writer, "stagename", info->m_stageName.GetString());
                WriteKeyValue(writer, "partInStage", info->m_partInStage.ToString());
            }
            WriteKeyValue(writer, "transition", "created");
            WriteTimestamp(writer, "utctime", info->m_statistics->m_creationTime);
            writer->WriteLine("");
            break;

        case DVS_Starting:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "transition", "starting");
            WriteTimestamp(writer, "utctime", info->m_statistics->m_startTime);
            writer->WriteLine("");
            break;

        case DVS_Running:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "transition", "running");
            WriteKeyValue(writer, "process", processGuid);
            WriteKeyValue(writer, "id", idOnComputer);
            WriteKeyValue(writer, "computer", machineName);
            WriteTimestamp(writer, "utctime", info->m_statistics->m_runningTime);
            writer->WriteLine("");
            break;

        case DVS_Completed:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "transition", "completed");
            WriteTimestamp(writer, "utctime", info->m_statistics->m_completionTime);
            writer->WriteLine("");
            break;

        case DVS_Failed:
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "transition", "failed");
            if (info->m_statistics->m_exitStatus == DrError_CohortShutdown)
            {
                WriteKeyValue(writer, "reason", "dependent failure");
            }
            else if (info->m_statistics->m_exitStatus == DrError_Unexpected)
            {
                WriteKeyValue(writer, "reason", "process terminated");
            }
            else
            {
                WriteKeyValue(writer, "reason", info->m_statistics->m_exitStatus.ToString("X8"));
            }
            if (status != DrNull)
            {
                if (status->GetVertexErrorCode() != S_OK)
                {
                    WriteKeyValue(writer, "errorcode", status->GetVertexErrorCode().ToString("X8"));
                }
                if (status->GetVertexErrorString().GetString() != DrNull)
                {
                    WriteKeyValue(writer, "errorstring", status->GetVertexErrorString().GetString());
                }
            }
            WriteTimestamp(writer, "utctime", info->m_statistics->m_completionTime);
            writer->WriteLine("");
            break;

        default:
            break;
        };

        if (info->m_state == DVS_Running && status != DrNull)
        {
            BeginEntry(entryTime, writer);
            WriteKeyValue(writer, "vertex", vertexId);
            WriteKeyValue(writer, "version", vertexVersion);
            WriteKeyValue(writer, "io", "starting");

            DrInputChannelArrayRef inputs = status->GetInputChannels();
            WriteKeyValue(writer, "numberOfInputs", inputs->Allocated().ToString());
            for (int iChan=0; iChan<inputs->Allocated(); ++iChan)
            {
                DrChannelDescriptionPtr c = inputs[iChan];
                HRESULT err = c->GetChannelState();
                WriteKeyValue(writer, "uriIn." + iChan.ToString(), c->GetChannelURI().GetString());
            }

            DrOutputChannelArrayRef outputs = status->GetOutputChannels();
            WriteKeyValue(writer, "numberOfOutputs", outputs->Allocated().ToString());
            for (int oChan=0; oChan<outputs->Allocated(); ++oChan)
            {
                DrChannelDescriptionPtr c = outputs[oChan];
                WriteKeyValue(writer, "uriOut." + oChan.ToString(), c->GetChannelURI().GetString());
            }

            writer->WriteLine("");
        }

        if (info->m_state == DVS_RunningStatus || info->m_state == DVS_Completed || info->m_state == DVS_Failed)
        {
            if (info->m_statistics->m_totalInputData != DrNull && info->m_statistics->m_totalOutputData != DrNull)
            {
                BeginEntry(entryTime, writer);
                WriteKeyValue(writer, "vertex", vertexId);
                WriteKeyValue(writer, "version", vertexVersion);
                if (info->m_state == DVS_RunningStatus)
                {
                    WriteKeyValue(writer, "io", "running");
                }
                else
                {
                    WriteKeyValue(writer, "io", "total");
                }
                WriteKeyValue(writer, "totalRead", info->m_statistics->m_totalInputData->m_dataRead.ToString());
                WriteKeyValue(writer, "tempRead", info->m_statistics->m_totalInputData->m_tempDataRead.ToString());
                WriteKeyValue(writer, "tempReadInRack", info->m_statistics->m_totalInputData->m_tempDataReadCrossMachine.ToString());
                WriteKeyValue(writer, "tempReadCrossRack", info->m_statistics->m_totalInputData->m_tempDataReadCrossPod.ToString());
                WriteKeyValue(writer, "localRead", info->m_statistics->m_totalLocalInputData.ToString());
                WriteKeyValue(writer, "totalWritten", info->m_statistics->m_totalOutputData->m_dataWritten.ToString());

                if (info->m_statistics->m_inputData != DrNull)
                {
                    DrInputChannelArrayRef inputs = DrNull;
                    if (status != DrNull)
                    {
                        inputs = status->GetInputChannels();
                    }
                    if (info->m_state == DVS_RunningStatus)
                    {
                        for (int iChan=0; iChan<info->m_statistics->m_inputData->Allocated(); ++iChan)
                        {
                            DrInputChannelExecutionStatisticsPtr s = info->m_statistics->m_inputData[iChan];
                            WriteKeyValue(writer, "rb." + iChan.ToString(), s->m_dataRead.ToString());
                            if (inputs != DrNull)
                            {
                                DrChannelDescriptionPtr c = inputs[iChan];
                                WriteKeyValue(writer, "tb." + iChan.ToString(), c->GetChannelTotalLength().ToString());
                            }
                        }
                    }
                    else
                    {
                        for (int iChan=0; iChan<info->m_statistics->m_inputData->Allocated(); ++iChan)
                        {
                            DrInputChannelExecutionStatisticsPtr s = info->m_statistics->m_inputData[iChan];
                            WriteKeyValue(writer, "rb." + iChan.ToString(), s->m_dataRead.ToString());
                            if (inputs != DrNull)
                            {
                                DrChannelDescriptionPtr c = inputs[iChan];
                                WriteKeyValue(writer, "tb." + iChan.ToString(), c->GetChannelTotalLength().ToString());
                            }
                            if (s->m_remoteMachine != DrNull)
                            {
                                WriteKeyValue(writer, "rC." + iChan.ToString(), s->m_remoteMachine->GetLocality().GetString());
                                WriteKeyValue(writer, "rR." + iChan.ToString(), s->m_tempDataReadCrossMachine.ToString());
                                WriteKeyValue(writer, "rCR." + iChan.ToString(), s->m_tempDataReadCrossPod.ToString());
                            }
                        }
                    }
                }

                if (info->m_statistics->m_outputData != DrNull)
                {
                    for (int oChan=0; oChan<info->m_statistics->m_outputData->Allocated(); ++oChan)
                    {
                        DrOutputChannelExecutionStatisticsPtr s = info->m_statistics->m_outputData[oChan];
                        WriteKeyValue(writer, "wb." + oChan.ToString(), s->m_dataWritten.ToString());
                    }
                }

                writer->WriteLine("");
            }

            if ((info->m_state == DVS_Completed || info->m_state == DVS_Failed) && status != DrNull)
            {
                DrInputChannelArrayRef inputs = status->GetInputChannels();
                for (int iChan=0; iChan<inputs->Allocated(); ++iChan)
                {
                    DrChannelDescriptionPtr c = inputs[iChan];
                    HRESULT err = c->GetChannelState();
                    if (err != S_OK && err != DrError_EndOfStream)
                    {
                        HRESULT channelErr = c->GetChannelErrorCode();
                        DrString channelErrString = c->GetChannelErrorString();

                        BeginEntry(entryTime, writer);
                        WriteKeyValue(writer, "vertex", vertexId);
                        WriteKeyValue(writer, "version", vertexVersion);
                        WriteKeyValue(writer, "inputChannel", iChan.ToString());
                        WriteKeyValue(writer, "uri", c->GetChannelURI().GetString());
                        WriteKeyValue(writer, "errorstatus", err.ToString("X8"));
                        if (channelErr != S_OK)
                        {
                            WriteKeyValue(writer, "errorcode", channelErr.ToString("X8"));
                        }
                        if (channelErrString.GetString() != DrNull)
                        {
                            WriteKeyValue(writer, "errorstring", channelErrString.GetString());
                        }
                        writer->WriteLine("");
                    }
                }

                DrOutputChannelArrayRef outputs = status->GetOutputChannels();
                for (int oChan=0; oChan<outputs->Allocated(); ++oChan)
                {
                    DrChannelDescriptionPtr c = outputs[oChan];
                    HRESULT err = c->GetChannelState();
                    if (err != S_OK && err != DrError_EndOfStream)
                    {
                        HRESULT channelErr = c->GetChannelErrorCode();
                        DrString channelErrString = c->GetChannelErrorString();

                        BeginEntry(entryTime, writer);
                        WriteKeyValue(writer, "vertex", vertexId);
                        WriteKeyValue(writer, "version", vertexVersion);
                        WriteKeyValue(writer, "outputChannel", oChan.ToString());
                        WriteKeyValue(writer, "uri", c->GetChannelURI().GetString());
                        WriteKeyValue(writer, "errorstatus", err.ToString("X8"));
                        if (channelErr != S_OK)
                        {
                            WriteKeyValue(writer, "errorcode", channelErr.ToString("X8"));
                        }
                        if (channelErrString.GetString() != DrNull)
                        {
                            WriteKeyValue(writer, "errorstring", channelErrString.GetString());
                        }
                        writer->WriteLine("");
                    }
                }
            }
        }

        m_writer->Write(writer->ToString());
    }
    catch (System::Exception^ e)
    {
        DrString reason(e->ToString());
        DrLogE("Calypso got exception: %s", reason.GetChars());
        Discard();
    }
}

void DrCalypsoReporter::ReportFinalTopology(DrVertexPtr vertex, DrResourcePtr runningMachine, DrTimeInterval runningTime)
{
    // doing nothing here
}
