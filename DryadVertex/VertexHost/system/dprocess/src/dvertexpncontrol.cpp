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

#include "dvertexpncontrol.h"
#include <dryadvertex.h>
#include <dvertexenvironment.h>
#include <dryadpropertiesdef.h>
#include <dryaderrordef.h>
#include <process.h>

#pragma managed

//
// Constructor for controller associated with single vertex process
// 
//
DVertexPnController::DVertexPnController(DVertexPnControllerOuter* parent,
                                         UInt32 vertexId, UInt32 vertexVersion)
{
    m_currentCommandVersion = 0;
    m_activeVertex = false;
    m_waitingForTermination = false;

    m_parent = parent;
    m_vertexId = vertexId;
    m_vertexVersion = vertexVersion;

    //
    // Create a new vertex 
    //
    m_vertex.Attach(new DryadSimpleChannelVertex());
    DrLogI( "PN controller creating simple channel vertex. Vertex %u.%u", m_vertexId, m_vertexVersion);

    //
    // Initialize the vertex and current vertex status
    //
    m_vertex->Initialize(this);
    m_currentStatus.Attach(new DVertexStatus());
    m_currentStatus->SetVertexState(DrError_OK);

    DrLogI( "PN controller managing vertex. Vertex %u.%u", m_vertexId, m_vertexVersion);
}

DVertexPnController::~DVertexPnController()
{
}

//
// Send updated status to GM
//
void DVertexPnController::SendStatus(UInt32 exitOnCompletion,
                                     bool notifyWaiters)
{
    DrRef<DryadPnProcessPropertyRequest> request;

    //
    // Enter critical section
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        //
        // If waiting for termination, don't send more status updates. 
        // Must report exiting as next status.
        // todo: make sure m_waitingForTermination is set false when done
        //
        if (m_waitingForTermination)
        {
            DrLogI( "Skipping status update");
            return;
        }
        else if (exitOnCompletion != DrExitCode_StillActive)
        {
            //
            // If not waiting for termination and still running, wait for termination
            //
            m_waitingForTermination = true;
        }

        //
        // Notify GM that we're done
        //
        request.Attach(MakeSetStatusRequest(exitOnCompletion, false,
                                            notifyWaiters));
        m_currentStatus->StoreInRequestMessage(request);
    }

    //
    // Send status update
    //
    DrLogI( "Sending status update. Vertex %u.%u notifyWaiters %s",
        m_currentStatus->GetProcessStatus()->GetVertexId(),
        m_currentStatus->GetProcessStatus()->GetVertexInstanceVersion(),
        (notifyWaiters) ? "true" : "false");
    SendSetStatusRequest(request);
}

void DVertexPnController::SendAssertStatus(const char* assertString)
{
    DrRef<DryadPnProcessPropertyRequest> request;

    if (m_baseDR.TryEnter())
    {
        /* we got the lock, so we can keep the current status and
           just add info that there's an assert failure. */
        m_waitingForTermination = true;

        DVertexProcessStatus* pStatus =
            m_currentStatus->GetProcessStatus();

        DryadMetaDataRef metaData = pStatus->GetVertexMetaData();
        if (metaData.Ptr() == NULL)
        {
            DryadMetaData::Create(&metaData);
            pStatus->SetVertexMetaData(metaData);
        }

        metaData->AppendString(Prop_Dryad_AssertFailure,
                               assertString, true);

        m_currentStatus->SetVertexState(DryadError_AssertFailure);

        request.Attach(MakeSetStatusRequest(DrExitCode_StillActive,
                                            true, true));
        if (request != NULL)
        {
            m_currentStatus->StoreInRequestMessage(request);
        }
    }
    else
    {
        /* we don't want to risk deadlock, so just send the assert
           failure as a "raw" status message */
        DrRef<DVertexStatus> rawStatus;
        rawStatus.Attach(new DVertexStatus());

        DVertexProcessStatus* pStatus = rawStatus->GetProcessStatus();

        DryadMetaDataRef metaData;
        DryadMetaData::Create(&metaData);
        pStatus->SetVertexMetaData(metaData);

        metaData->AppendString(Prop_Dryad_AssertFailure,
                               assertString, true);

        rawStatus->SetVertexState(DryadError_AssertFailure);

        request.Attach(MakeSetStatusRequest(DrExitCode_StillActive,
                                            true, true));
        if (request != NULL)
        {
            rawStatus->StoreInRequestMessage(request);
        }
    }

    if (request != NULL)
    {
        DrLogI( "Sending notification. AssertString=%s", assertString);

        SendSetStatusRequest(request);

        DrLogI( "Sent notification. AssertString=%s", assertString);
    }
}

void DVertexPnController::
    AssimilateChannelStatus(DryadChannelDescription* dst,
                            DryadChannelDescription* src)
{
    dst->SetChannelState(src->GetChannelState());
    dst->SetChannelMetaData(src->GetChannelMetaData());
    dst->SetChannelProcessedLength(src->GetChannelProcessedLength());
    dst->SetChannelTotalLength(src->GetChannelTotalLength());
}

void DVertexPnController::AssimilateNewStatus(DVertexProcessStatus* status,
                                              bool sendUpdate,
                                              bool notifyWaiters)
{
    {
        AutoCriticalSection acs(&m_baseDR);

        UInt32 i;

        LogAssert(m_activeVertex == true);

        DVertexProcessStatus* currentPStatus =
            m_currentStatus->GetProcessStatus();
        LogAssert(status->GetVertexId() ==
                  currentPStatus->GetVertexId());
        LogAssert(status->GetVertexInstanceVersion() ==
                  currentPStatus->GetVertexInstanceVersion());

        currentPStatus->SetVertexMetaData(status->GetVertexMetaData());

        LogAssert(status->GetInputChannelCount() ==
                  currentPStatus->GetInputChannelCount());
        for (i=0; i<status->GetInputChannelCount(); ++i)
        {
            AssimilateChannelStatus(&(currentPStatus->GetInputChannels()[i]),
                                    &(status->GetInputChannels()[i]));
        }

        LogAssert(status->GetOutputChannelCount() ==
                  currentPStatus->GetOutputChannelCount());
        for (i=0; i<status->GetOutputChannelCount(); ++i)
        {
            AssimilateChannelStatus(&(currentPStatus->GetOutputChannels()[i]),
                                    &(status->GetOutputChannels()[i]));
        }
    }

    if (sendUpdate)
    {
        SendStatus(DrExitCode_StillActive, notifyWaiters);
    }
}

//
// Notify GM of completed vertex
//
void DVertexPnController::Terminate(DrError vertexState,
                                    UInt32 exitCode)
{
    LogAssert(vertexState != DryadError_VertexRunning);

    if (vertexState == DrError_OK)
    {
        LogAssert(exitCode == DrExitCode_StillActive);
    }

    //
    // take Critical section to update vertex state
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        m_currentStatus->SetVertexState(vertexState);
    }

    //
    // Log vertex termination
    //
    DrLogI( "Terminating vertex. Vertex %u.%u exitCode %s vertexState %s",
        m_currentStatus->GetProcessStatus()->GetVertexId(),
        m_currentStatus->GetProcessStatus()->GetVertexInstanceVersion(),
        DREXITCODESTRING(exitCode), DRERRORSTRING(vertexState));

    //
    // Send status to GM
    //
    SendStatus(exitCode, true);
}

//
// Runs in new thread created and started by DVertexPnController::Start
// Executes vertex and terminates to report final status
//
unsigned DVertexPnController::ThreadFunc(void* arg)
{
    //
    // Get Controller Reference, and then clean up unnecessary thread block
    //
    DVertexPnControllerThreadBlock* threadBlock =
        (DVertexPnControllerThreadBlock *) arg;
    DVertexPnController* self = threadBlock->m_parent;
    DrRef<DVertexCommandBlock> startCommand = threadBlock->m_commandBlock;

    LogAssert(self != NULL,"Received NULL DVertexPnController pointer.");
	
    if(startCommand == NULL)
    {
        DrLogE("Received NULL DVertexCommandBlock pointer");
        self->Terminate(DrError_Fail, DrError_Fail);
		return DrExitCode_Fail;
    }


    delete threadBlock;
    threadBlock = NULL;

    //
    // Execute dryad vertex - blocking
    //
    DrError vertexState =
        self->m_vertex->RunDryadVertex(startCommand->GetProcessStatus(),
                                       startCommand->GetArgumentCount(),
                                       startCommand->GetArgumentVector());

    //
    // If vertex completed, then success, otherwise failure
    //
    UInt32 exitCode;
    if (vertexState == DryadError_VertexCompleted)
    {
        exitCode = DrExitCode_OK;
    }
    else
    {
        exitCode = DrExitCode_Fail;
    }

    //
    // Enter critical section to turn off active vertex
    //
    {
        AutoCriticalSection acs(&(self->m_baseDR));
        LogAssert(self->m_activeVertex == true);
        self->m_activeVertex = false;
    }

    //
    // Notify GM of completed vertex
    //
    DrLogD( "About to terminate");
    self->Terminate(vertexState, exitCode);

    return DrExitCode_OK;
}

//
// Create files which contain information used to restart the upcoming vertex command
// Used for post-mortem debugging.
//
void DVertexPnController::DumpRestartCommand(DVertexCommandBlock* commandBlock)
{
    DrError err;

    //
    // Create temporary buffer
    //
    DrRef<DrSimpleHeapBuffer> buf;
    buf.Attach(new DrSimpleHeapBuffer());

    //
    // Write command block into buffer
    //
    {
        DrMemoryBufferWriter writer(buf);
        err = commandBlock->Serialize(&writer);
    }

    //
    // If write fails, log failure and return
    //
    if (err != DrError_OK)
    {
        DrLogE("Can't serialize command block for restart --- %s",
            DRERRORSTRING(err));
        return;
    }

    //
    // Get data reference and byte count
    //
    const void* serializedData;
    Size_t availableToRead;
    serializedData = buf->GetReadAddress(0, &availableToRead);
    LogAssert(availableToRead >= buf->GetAvailableSize());

    //
    // Get the process information 
    //
    DVertexProcessStatus* ps = commandBlock->GetProcessStatus();

    //
    // Build file for data required for rerun, open it
    //
    DrStr64 restartBlockName;
    restartBlockName.SetF("vertex-%u-%u-rerun-data.dat",
                          ps->GetVertexId(), ps->GetVertexInstanceVersion());
    FILE* fData = fopen(restartBlockName, "wb");
    if (fData == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run command block file '%s' --- %s",
            restartBlockName.GetString(), DRERRORSTRING(err));
        return;
    }

    //
    // Build file for original information required for rerun, open it
    //
    DrStr64 originalInfoName;
    originalInfoName.SetF("vertex-%u-%u-rerun-originalInfo.txt",
                          ps->GetVertexId(), ps->GetVertexInstanceVersion());
    FILE* fOriginalText = fopen(originalInfoName, "w");
    if (fOriginalText == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run original info file '%s' --- %s",
            originalInfoName.GetString(), DRERRORSTRING(err));

        //
        // Close data file
        //
        fclose(fData);
        return;
    }

    //
    // Build file for rerun command line, open it
    //
    DrStr64 originalRestartCommand;
    originalRestartCommand.SetF("vertex-%u-%u-rerun.cmd",
                                ps->GetVertexId(),
                                ps->GetVertexInstanceVersion());
    FILE* fOriginalRestart = fopen(originalRestartCommand, "w");
    if (fOriginalRestart == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run original command file '%s' --- %s",
            originalRestartCommand.GetString(), DRERRORSTRING(err));

        //
        // Close data and original text files
        //
        fclose(fData);
        fclose(fOriginalText);
        return;
    }

    //
    // Open file for local info
    //

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    DrStr64 localInfoName;
    localInfoName.SetF("vertex-%u-%u-rerun-localInfo.txt",
                       ps->GetVertexId(), ps->GetVertexInstanceVersion());
    FILE* fLocalText = fopen(localInfoName, "w");
    if (fLocalText == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run local info file '%s' --- %s",
            localInfoName.GetString(), DRERRORSTRING(err));

        //
        // Close data, cmd, and original text files
        //
        fclose(fData);
        fclose(fOriginalText);
        fclose(fOriginalRestart);
        return;
    }
    */


    //
    // Open file for rerun with local inputs
    //

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    DrStr64 localRestartCommand;
    localRestartCommand.SetF("vertex-%u-%u-rerun-local-inputs.cmd",
                             ps->GetVertexId(),
                             ps->GetVertexInstanceVersion());
    FILE* fLocalRestart = fopen(localRestartCommand, "w");
    if (fLocalRestart == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run local command file '%s' --- %s",
            localRestartCommand.GetString(), DRERRORSTRING(err));

        //
        // Close data, cmd, original, and local text files
        //
        fclose(fData);
        fclose(fOriginalText);
        fclose(fOriginalRestart);
        fclose(fLocalText);
        return;
    }
    */

    //
    // Open file for fetching inputs
    //
    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    DrStr64 copyCommand;
    copyCommand.SetF("vertex-%u-%u-rerun-fetch-inputs.cmd",
                     ps->GetVertexId(), ps->GetVertexInstanceVersion());
    FILE* fCopyCommand = fopen(copyCommand, "w");
    if (fCopyCommand == NULL)
    {
        //
        // If failed to open file, log and return
        //
        err = DrGetLastError();
        DrLogE(
            "Can't open re-run copy command file '%s' --- %s",
            localRestartCommand.GetString(), DRERRORSTRING(err));

        //
        // Close data, original and localcmd, and original and local text files
        //
        fclose(fData);
        fclose(fOriginalText);
        fclose(fOriginalRestart);
        fclose(fLocalText);
        fclose(fLocalRestart);
        return;
    }
    */

    //
    // Write out data to data file, then close it.
    //
    size_t written = fwrite(serializedData, 1, buf->GetAvailableSize(), fData);
    fclose(fData);
    if (written != buf->GetAvailableSize())
    {
        //
        // If failed to write all the data, log failure
        //
        err = DrGetLastError();
        DrLogE(
            "Failed to write re-run command block file '%s': only %Iu of %Iu bytes written --- %s",
            restartBlockName.GetString(),
            written, (size_t) (buf->GetAvailableSize()),
            DRERRORSTRING(err));
    }

    //
    // Write original restart command
    //
    fprintf(fOriginalRestart,
            "%s --vertex --cmd -dump %s -overridetext %s\n",
            m_parent->GetRunningExePathName(),
            restartBlockName.GetString(),
            originalInfoName.GetString());

    //
    // Write local restart command
    //
    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    fprintf(fLocalRestart,
            "%s --vertex --cmd -dump %s -overridetext %s\n",
            m_parent->GetRunningExePathName(),
            restartBlockName.GetString(),
            localInfoName.GetString());
    */

    //
    // Record number of input files
    //
    fprintf(fOriginalText, "%u # input files\n", ps->GetInputChannelCount());

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    fprintf(fLocalText, "%u # input files\n", ps->GetInputChannelCount());
    */

    //
    // Get the input channels and foreach channel, add copy command to copy script
    //
    DryadInputChannelDescription* inputs = ps->GetInputChannels();
    for (UInt32 i=0; i<ps->GetInputChannelCount(); ++i)
    {
        const char* uri = inputs[i].GetChannelURI();

        /* BUG 16322: Do not create this for SP3, since it is currently broken.
           Consider fixing for v4.
        if (::_strnicmp(uri, "file://", 7) == 0)
        {
            //
            // If reading from file, copy command doesn't want "file://" prefix
            // todo: remove reference to cosmos
            //
            fprintf(fCopyCommand, "cosmos.exe copy %s v%u.%u-i%u\n",
                    uri+7,
                    ps->GetVertexId(), ps->GetVertexInstanceVersion(), i);
        }
        else if (::_strnicmp(uri, "cosmos://", 9) == 0)
        {
            //
            // If reading from cosmos path, copy directly
            // todo: remove cosmos code
            //
            fprintf(fCopyCommand, "cosmos.exe copy %s v%u.%u-i%u\n",
                    uri,
                    ps->GetVertexId(), ps->GetVertexInstanceVersion(), i);
        }
        else
        {
            //
            // Otherwise, unable to copy
            //
            fprintf(fCopyCommand, "echo can't copy URI %s to v%u.%u-i%u\n",
                    uri,
                    ps->GetVertexId(), ps->GetVertexInstanceVersion(), i);
        }
        */

        // 
        // At reference to this URI to original and relative reference to local
        //
        fprintf(fOriginalText, "%s\n", uri);

        /* BUG 16322: Do not create this for SP3, since it is currently broken.
           Consider fixing for v4.
        fprintf(fLocalText, "file://v%u.%u-i%u\n",
                ps->GetVertexId(), ps->GetVertexInstanceVersion(), i);
        */
    }

    //
    // Record number of output files
    //
    fprintf(fOriginalText, "%u # output files\n", ps->GetOutputChannelCount());

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    fprintf(fLocalText, "%u # output files\n", ps->GetOutputChannelCount());
    */

    //
    // Get the output channels and record each one
    //
    DryadOutputChannelDescription* outputs = ps->GetOutputChannels();
    for (UInt32 i=0; i<ps->GetOutputChannelCount(); ++i)
    {
        const char* uri = outputs[i].GetChannelURI();

        //
        // Check if uri is writting to DSC partition. 
        // If it is, redirect to local temp file to avoid writing to sealed stream
        // 
        DrStr uriMod("");
        if(ConcreteRChannel::IsDscPartition(uri))
        {
            uriMod.AppendF("file://hpcdscpt_redirect_%d.dtf", i);
            uri = uriMod.GetString();
        }

        fprintf(fOriginalText, "%s\n", uri);

        /* BUG 16322: Do not create this for SP3, since it is currently broken.
           Consider fixing for v4.
        fprintf(fLocalText, "%s\n", uri);
        */
    }

    //
    // Record number of arguments
    //
    fprintf(fOriginalText, "%u # arguments\n",
            commandBlock->GetArgumentCount());

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    fprintf(fLocalText, "%u # arguments\n", commandBlock->GetArgumentCount());
    */

    //
    // Foreach argument, record its value
    //
    for (UInt32 i=0; i<commandBlock->GetArgumentCount(); ++i)
    {
        DrStr64 arg = commandBlock->GetArgumentVector()[i];
        fprintf(fOriginalText, "%s\n", arg.GetString());

        /* BUG 16322: Do not create this for SP3, since it is currently broken.
           Consider fixing for v4.
        fprintf(fLocalText, "%s\n", arg.GetString());
        */
    }

    //
    // Close all files
    // todo: fData closed above, remove duplicate
    //
    fclose(fData);
    fclose(fOriginalText);
    fclose(fOriginalRestart);

    /* BUG 16322: Do not create this for SP3, since it is currently broken.
       Consider fixing for v4.
    fclose(fLocalText);
    fclose(fLocalRestart);
    fclose(fCopyCommand);
    */
}

//
// Prepare to start user vertex code. Dump restart command, verify I/O channel health
// and spin up new thread to handle vertex execution
// Called from ActOnCommand when command is start.
//
void DVertexPnController::Start(DVertexCommandBlock* commandBlock)
{
    //
    // Associate controller and command block
    //
    DVertexPnControllerThreadBlock* threadBlock =
        new DVertexPnControllerThreadBlock();

    threadBlock->m_parent = this;
    threadBlock->m_commandBlock = commandBlock;

    //
    // Write out all files needed to restart this vertex later
    //
    DumpRestartCommand(commandBlock);

    //
    // Enter critical section for updating vertex status and getting channels' state
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        //
        // Update vertex state to "running" after verifying vertex ok
        //
        LogAssert(m_currentStatus->GetVertexState() == DrError_OK);
        m_currentStatus->SetVertexState(DryadError_VertexRunning);

        //
        // Get current vertex process status ref and update with command parameters
        //
        DVertexProcessStatus* currentPStatus = m_currentStatus->GetProcessStatus();
        currentPStatus->CopyFrom(commandBlock->GetProcessStatus(), false);

        //
        // Get input channels and foreach make sure state is ok
        //
        DryadInputChannelDescription* inputs = currentPStatus->GetInputChannels();
        UInt32 i;
        for (i=0; i<currentPStatus->GetInputChannelCount(); ++i)
        {
            LogAssert(inputs[i].GetChannelState() == DrError_OK);
        }

        
        //
        // Get output channels and foreach make sure state is ok
        //
        DryadOutputChannelDescription* outputs = currentPStatus->GetOutputChannels();
        for (i=0; i<currentPStatus->GetOutputChannelCount(); ++i)
        {
            LogAssert(outputs[i].GetChannelState() == DrError_OK);
        }

        //
        // Set vertex to active
        //
        LogAssert(m_activeVertex == false);
        m_activeVertex = true;
    }

    //
    // Update vertex service with current, active, status
    //
    SendStatus(DrExitCode_StillActive, true);

    //
    // Prepare the vertex for execution
    //
    m_vertex->PrepareDryadVertex(commandBlock->GetProcessStatus(),
                                 NULL,
                                 commandBlock->GetArgumentCount(),
                                 commandBlock->GetArgumentVector(),
                                 commandBlock->GetRawSerializedBlockLength(),
                                 commandBlock->GetRawSerializedBlock());

    //
    // Start thread to execute vertex code
    //
    unsigned threadAddr;
    HANDLE threadHandle =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  DVertexPnController::ThreadFunc,
                                  threadBlock,
                                  0,
                                  &threadAddr);
    
    //
    // Verify thread creation and exit
    //
    LogAssert(threadHandle != 0);
    BOOL bRet = ::CloseHandle(threadHandle);
    LogAssert(bRet != 0);
}

void DVertexPnController::ReOpenChannels(DVertexCommandBlock* reOpenCommand)
{
    DVertexProcessStatus* newStatus = reOpenCommand->GetProcessStatus();

    {
        AutoCriticalSection acs(&m_baseDR);

        DVertexProcessStatus* currentPStatus =
            m_currentStatus->GetProcessStatus();

        LogAssert(newStatus->GetVertexId() ==
                  currentPStatus->GetVertexId());
        LogAssert(newStatus->GetVertexInstanceVersion() ==
                  currentPStatus->GetVertexInstanceVersion());

        LogAssert(newStatus->GetInputChannelCount() ==
                  currentPStatus->GetInputChannelCount());
        LogAssert(newStatus->GetOutputChannelCount() ==
                  currentPStatus->GetOutputChannelCount());
    }

    m_vertex->ReOpenChannels(newStatus);
}

//
// Interpret command received from GM
//
DrError DVertexPnController::ActOnCommand(DVertexCommandBlock* commandBlock)
{
    // 
    // Break into debugger if command asks for it
    //
    if (commandBlock->GetDebugBreak())
    {
        ::DebugBreak();
    }

    DVertexCommand command = commandBlock->GetVertexCommand();
    DrError err = DrError_OK;

    //
    // Critical section to issue commands
     //
    {
        AutoCriticalSection acs(&m_baseDR);

        switch (command)
        {
        case DVertexCommand_Start:
            //
            // Command is to start a new vertex
            //
            if (m_currentStatus->GetVertexState() != DrError_OK)
            {
                //
                // If vertex is in an error state, can't start it
                //
                err = DryadError_InvalidCommand;
            }
            else
            {
                //
                // If vertex ok, then start with command from GM
                // this is non-blocking and will return after creating new thread 
                //
                DrLogI("Start command received.");
                Start(commandBlock);
            }
            break;

        case DVertexCommand_ReOpenChannels:
            //
            // If reopen channels command, then reopen channels
            // todo: find out if this is ever used
            //
            DrLogI("Reopen Channels command received.");
            ReOpenChannels(commandBlock);
            break;

        case DVertexCommand_Terminate:
            //
            // Terminate command
            // todo: find out from Victor if this is ever used
            //
            DrError currentState;
            currentState = m_currentStatus->GetVertexState();
            DrLogI("Terminate command received.");
            if (m_waitingForTermination == false)
            {
                //
                // If not waiting for termination already, terminate.
                // 
                DrLogD( "About to terminate");
                if (currentState == DryadError_VertexCompleted)
                {
                    //
                    // If already done, report cause of finish
                    //
                    Terminate(m_currentStatus->GetVertexState(),
                              DrExitCode_OK);
                }
                else
                {
                    //
                    // If not yet done, terminate because of this
                    //
                    Terminate(DryadError_VertexReceivedTermination,
                              DrExitCode_Killed);
                }
            }

            //
            // if waiting for termination, we can just fall through to the command loop again
            // here: the process will exit as soon as the sendstatus completes 
            //
            break;

        default:
            //
            // Invalid command
            //
            DrLogE("Unknown command received.");
            err = DryadError_InvalidCommand;
            break;
        }
    }

    return err;
}

//
// Starts thread for vertex command loop
// called from DVertexPnControllerOuter.run
//
void DVertexPnController::LaunchCommandLoop()
{
    unsigned threadAddr;

    //
    // Create new thread executing the DVertexPnController::CommandLoopStatic function
    //
    HANDLE threadHandle =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  DVertexPnController::CommandLoopStatic,
                                  this,
                                  0,
                                  &threadAddr);

    // todo: handle errors in thread creation better
    LogAssert(threadHandle != 0);

    //
    // Close handle to this thread
    //
    BOOL bRet = ::CloseHandle(threadHandle);
    LogAssert(bRet != 0);
}

//
// Started in new thread per vertex to process commands
//
unsigned DVertexPnController::CommandLoopStatic(void* arg)
{
    //
    // Get provided controller
    //
    DVertexPnController* self = (DVertexPnController *) arg;

    //
    // Run command loop on controller
    //
    return self->CommandLoop();
}

//
// Constructor. Initializes without environment or controller list
//
DVertexPnControllerOuter::DVertexPnControllerOuter()
{
    m_controllerArray = NULL;
    m_environment = NULL;
}

void DVertexPnControllerOuter::AssertCallback(void* cookie, const char* assertString)
{
    DVertexPnControllerOuter* self = (DVertexPnControllerOuter *) cookie;

    if (assertString[0] == 'a' || assertString[0] == 'A')
    {
        self->SendAssertStatus(assertString);
    }
}

void DVertexPnControllerOuter::SendAssertStatus(const char* assertString)
{
    LONG postIncrement = ::InterlockedIncrement(&m_assertCounter);

    if (postIncrement == 1)
    {
        /* this is the first assert we've seen, so try to do something
           useful with it */

        UInt32 i;
        for (i=0; i<m_numberOfVertices; ++i)
        {
            m_controllerArray[i]->SendAssertStatus(assertString);
        }

        DrLogI( "Sleeping");

        /* wait to give the PN a chance to receive the messages if
           they are in a send queue */
        ::Sleep(2000);

        DrLogI( "Sleeping done");

        /* once we return the assert failure will be processed and the
           program will terminate */
    }
}

//
// Decrement number of active verticies and close process if done or failed
//
void DVertexPnControllerOuter::VertexExiting(int exitCode)
{
    //
    // Take critical section to update the number of active verticies 
    // and exit the process if all verticies are complete or a failure occurred
    //
    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_activeVertexCount > 0);
        --m_activeVertexCount;
        if (m_activeVertexCount == 0 || exitCode != 0)
        {
            DrExitProcess(exitCode);
        }
    }
}

//
// Return current exe path
//
const char* DVertexPnControllerOuter::GetRunningExePathName()
{
    return m_exePathName;
}

//
// Return current environment
//
DVertexEnvironment* DVertexPnControllerOuter::GetEnvironment()
{
    return m_environment;
}

//
// called from dvertexmain.cpp
//
UInt32 DVertexPnControllerOuter::Run(int argc, char* argv[])
{
    //
    // Create virtex environment and initialize it from current environment
    //
    m_environment = MakeEnvironment();
    DrError cse = m_environment->InitializeFromEnvironment();
    if (cse != DrError_OK)
    {
        DrLogE("Couldn't initialise environment");
        return 1;
    }

    //
    // Make sure there are at least two arguments
    //
    if (argc < 2)
    {
        DrLogE("No vertex arguments specified to the PN controller");
        return 1;
    }

    //
    // Get path and num verticies
    //
    m_exePathName = argv[0];
    m_numberOfVertices = atoi(argv[1]);

    //
    // Fail if number of verticies cannot be converted
    // todo: also fail if INT_MAX or INT_MIN returned denoting invalid range
    //
    if (m_numberOfVertices == 0)
    {
        DrLogE("No vertices specified to the PN controller");
        return 1;
    }

    //
    // If number of arguments isn't 2*numVerticies + 2, then it doesn't make sense
    //
    if ((UInt32) argc != (2 + 2*m_numberOfVertices))
    {
        DrLogE( "%u vertices specified to the PN controller need "
                "%u not %d arguments to describe them",
                m_numberOfVertices, 2 + 2*m_numberOfVertices, argc);
        return 1;
    }

    //
    // Set up array for controllers for each vertex
    //
    LogAssert(m_controllerArray == NULL);
    m_controllerArray = new DVertexPnController* [m_numberOfVertices];
    LogAssert(m_controllerArray != NULL);

    //
    // Critical section to update the number of active verticies
    //
    {
        AutoCriticalSection acs(&m_baseDR);
        m_assertCounter = 0;
        m_activeVertexCount = m_numberOfVertices;
    }

    //
    // Foreach vertex, get command line arguments and make a controller
    // 
    UInt32 i;
    for (i=0; i<m_numberOfVertices; ++i)
    {
        //
        // cmdline has each vertex info in for <vertexID, vertexVersion>
        //
        UInt32 vertexId = atoi(argv[2 + i*2]);
        UInt32 vertexVersion = atoi(argv[2 + i*2 + 1]);

        //
        // Make a new controller for each vertex
        //
        m_controllerArray[i] = MakePnController(vertexId, vertexVersion);
    }

    // todo: not sure if this matters for us
    /* disable assertion notification for now until logging deadlock
       is fixed */
//     Logger::AddApplicationLogCallback(AssertCallback, this);

    //
    // foreach vertex, launch the command loop
    // todo: code cleanup: any reason not to launch command loop as soon as it's created?
    //
    for (i=0; i<m_numberOfVertices; ++i)
    {
        m_controllerArray[i]->LaunchCommandLoop();
    }

    //
    // Sleep forever - commandloop will take down process when instructed to do so
    //
    ::Sleep(INFINITE);

    return 0;
}
