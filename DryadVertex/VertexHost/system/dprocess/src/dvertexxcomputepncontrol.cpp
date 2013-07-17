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

#include "dvertexxcomputepncontrol.h"
#include <dryadvertex.h>
#include <dvertexenvironment.h>
#include <dryadpropertiesdef.h>
#include <dryaderrordef.h>
#include <dryadnativeport.h>
#include <dryadstandaloneini.h>
#include <xcomputepropertyblock.h>
#include <dvertexxcomputeenvironment.h>

class DVertexXComputeSetStatus : public DryadXComputePnProcessPropertyRequest
{
public:
    DVertexXComputeSetStatus(DVertexXComputePnController* parent,
                             DVertexPnControllerOuter* parentOuter,
                             UInt32 exitOnCompletion,
                             bool isAssert,
                             bool notifyWaiters);

    PXC_SETANDGETPROCESSINFO_REQINPUT MarshalProperty();
    PXC_SETANDGETPROCESSINFO_REQRESULTS* GetResults();

    bool IsAssert();

    void IncrementSendCount();
    UInt32 GetSendCount();

    void Process(DrError err);

private:
    XC_SETANDGETPROCESSINFO_REQINPUT     m_info;

    PXC_PROCESSPROPERTY_INFO             m_propertyArray[2];
    XC_PROCESSPROPERTY_INFO              m_payloadProperty;
    XC_PROCESSPROPERTY_INFO              m_controlProperty;
    PXC_SETANDGETPROCESSINFO_REQRESULTS  m_results;

    DVertexXComputePnController*         m_parent;
    DVertexPnControllerOuter*            m_parentOuter;
    UInt32                               m_exitOnCompletion;
    bool                                 m_notifyWaiters;
    bool                                 m_isAssert;

    UInt32                               m_sendCount;
};

//
// Constructor. Create Set Status request.
//
DVertexXComputeSetStatus::
    DVertexXComputeSetStatus(DVertexXComputePnController* parent,
                             DVertexPnControllerOuter* parentOuter,
                             UInt32 exitOnCompletion,
                             bool isAssert,
                             bool notifyWaiters)
{
    //
    // Build m_info property
    //
    memset(&m_info, 0, sizeof(m_info));
    m_info.Size = sizeof(m_info);
    m_info.NumberOfProcessPropertiesToSet = (notifyWaiters) ? 2 : 1;
    m_info.ppPropertiesToSet = m_propertyArray;

    //
    // Assign payload and control properties to array
    //
    m_propertyArray[0] = &m_payloadProperty;
    m_propertyArray[1] = &m_controlProperty;

    memset(&m_payloadProperty, 0, sizeof(m_payloadProperty));
    m_payloadProperty.Size = sizeof(m_payloadProperty);
    m_payloadProperty.PropertyVersion = MAX_UINT64;

    memset(&m_controlProperty, 0, sizeof(m_controlProperty));
    m_controlProperty.Size = sizeof(m_controlProperty);
    m_controlProperty.PropertyVersion = MAX_UINT64;

    //
    // Save parameters
    //
    m_parent = parent;
    m_parentOuter = parentOuter;
    m_exitOnCompletion = exitOnCompletion;
    m_notifyWaiters = notifyWaiters;
    m_isAssert = isAssert;

    m_sendCount = 0;
}

PXC_SETANDGETPROCESSINFO_REQINPUT
    DVertexXComputeSetStatus::MarshalProperty()
{
    DrLogI( "Marshaling property. Payload %s notify waiters %s",
        m_label.GetString(), m_notifyWaiters ? "true" : "false");

    m_payloadProperty.pPropertyLabel = m_label.GetString();
    m_payloadProperty.pPropertyString = m_string.GetString();

    if (m_notifyWaiters)
    {
        m_controlProperty.pPropertyLabel = m_controlLabel.GetString();
        m_controlProperty.pPropertyString = "(Empty)";
    }

    Size_t propertySize = m_block->GetAvailableSize();

    Size_t blockSize;
    void* propertyData = m_block->GetDataAddress(0, &blockSize, NULL);

    LogAssert(blockSize >= propertySize);

    m_payloadProperty.PropertyBlockSize = propertySize;
    m_payloadProperty.pPropertyBlock = propertyData;

    return &m_info;
}

PXC_SETANDGETPROCESSINFO_REQRESULTS*
    DVertexXComputeSetStatus::GetResults()
{
    return &m_results;
}

//
// Get assertion level
//
bool DVertexXComputeSetStatus::IsAssert()
{
    return m_isAssert;
}

//
// Increment the number of retries attempted 
//
void DVertexXComputeSetStatus::IncrementSendCount()
{
    ++m_sendCount;
}

//
// Return the number of retries attempted
//
UInt32 DVertexXComputeSetStatus::GetSendCount()
{
    return m_sendCount;
}

//
// Handle response from xcompute
//
void DVertexXComputeSetStatus::Process(DrError err)
{
    //
    // If status successfully sent, log success and check on vertex status
    //
    if (err == DrError_OK)
    {
        DrLogI( "PN send succeeded. label %s", m_label.GetString());

        if (m_exitOnCompletion != DrExitCode_StillActive)
        {
            //
            // If vertex is not still active, report that it is exiting
            // this may kill this process if all verticies are complete
            //
            m_parentOuter->VertexExiting(m_exitOnCompletion);
        }

        return;
    }

    //
    // If there was a communication failure, retry up to 4 times
    //
    if (err == DrError_RemoteDisconnected ||
        err == DrError_LocalDisconnected ||
        err == DrError_ConnectionFailed)
    {
        if (m_sendCount < 4)
        {
            DrLogW( "Retrying PN send. error %s", DRERRORSTRING(err));

            m_parent->SendSetStatusRequest(this);

            return;
        }
    }

    //
    // If m_isAssert, just report warning, otherwise log and fail
    // todo: this seems backwards. I don't understand how m_isAssert is set.
    //
    if (m_isAssert)
    {
        DrLogW(
            "Send to PN failed: not asserting again. done %u sends, error %s",
            m_sendCount, DRERRORSTRING(err));
    }
    else
    {
        DrLogA(
            "Send to PN failed. done %u sends, error %s",
            m_sendCount, DRERRORSTRING(err));
    }
}

//
// Type of xcompute request handler that deals with "set status" operations
//
class XComputeSetStatusOverlapped : public DryadNativePort::HandlerBase
{
public:
    //
    // Constructor
    //
    XComputeSetStatusOverlapped(DVertexXComputeSetStatus* request);

    //
    // Property returning operation state of request
    //
    DrError* GetOperationState();

private:
    //
    // Process a set status request
    //
    void ProcessIO(DrError retval, UInt32 numBytes);

    //
    // Set Status request and state
    //
    DrError                           m_operationState;
    DrRef<DVertexXComputeSetStatus>   m_request;
};

//
// Assign associated request
//
XComputeSetStatusOverlapped::
    XComputeSetStatusOverlapped(DVertexXComputeSetStatus* request)
{
    m_request = request;
}

//
// Returns a pointer to the operation state of the request
//
DrError* XComputeSetStatusOverlapped::GetOperationState()
{
    return &m_operationState;
}

//
// Process a set status request
//
void XComputeSetStatusOverlapped::ProcessIO(DrError retval, UInt32 numBytes)
{
    //
    // If there was an error, log and fail
    //
    if (retval != DrError_OK)
    {
        DrLogA( "Completion port returned error. error %s %u bytes", DRERRORSTRING(retval), numBytes);
    }

    //
    // If there is no data, log and fail
    //
    if (numBytes != 0)
    {
        DrLogA( "Completion port returned non-zero. %u bytes", numBytes);
    }

    //
    // Handle other outcomes based on the operation state
    //
    m_request->Process(m_operationState);

    delete this;
}

//
// Constructor. Calls parent constructor only.
//
DVertexXComputePnController::
    DVertexXComputePnController(DVertexPnControllerOuter* parent,
                                UInt32 vertexId, UInt32 vertexVersion) :
        DVertexPnController(parent, vertexId, vertexVersion)
{
}

//
// Create a new set status request
//
DryadPnProcessPropertyRequest*
    DVertexXComputePnController::MakeSetStatusRequest(UInt32 exitOnCompletion,
                                                      bool isAssert,
                                                      bool notifyWaiters)
{
    return new DVertexXComputeSetStatus(this, m_parent,
                                        exitOnCompletion, isAssert,
                                        notifyWaiters);
}

//
// Send updated status to vertex service
//
void DVertexXComputePnController::
    SendSetStatusRequest(DryadPnProcessPropertyRequest* r)
{
    //
    // Cast request to required type and make sure it's valid
    //
    DVertexXComputeSetStatus* request =
        dynamic_cast<DVertexXComputeSetStatus*>(r);
    LogAssert(request != NULL);

    //
    // Wrap request in XComputeSetStatusOverlapped
    //
    XComputeSetStatusOverlapped* overlapped =
        new XComputeSetStatusOverlapped(request);

    //
    // Create asynchronous execution information
    //
    XC_ASYNC_INFO asyncInfo;
    memset(&asyncInfo, 0, sizeof(asyncInfo));
    asyncInfo.cbSize = sizeof(asyncInfo);
    asyncInfo.pOperationState = overlapped->GetOperationState();
    asyncInfo.IOCP = g_dryadNativePort->GetCompletionPort();
    asyncInfo.pOverlapped = overlapped->GetOverlapped();

    //
    // Update request counters
    //
    request->IncrementSendCount();
    g_dryadNativePort->IncrementOutstandingRequests();

    //
    // Update process info
    //
    XCERROR err =
        XcSetAndGetProcessInfo(GetProcessHandle(),
                               request->MarshalProperty(),
                               request->GetResults(),
                               &asyncInfo);

    LogAssert(err != DrError_OK);

    if (err != HRESULT_FROM_WIN32(ERROR_IO_PENDING))
    {
        //
        // If failed (other than due to pending IO) log failure and update request counter
        //
        g_dryadNativePort->DecrementOutstandingRequests();

        //
        // If request assertion true, report errors as warnings, otherwise report as error and fail
        // todo: this still seems backwards - need to figure out rational 
        // request handles retries itself
        //
        if (request->IsAssert())
        {
            DrLogW(
                "Status request send failed synchronously during assert: not asserting again. done %u send tries, error %s",
                request->GetSendCount(), DRERRORSTRING(err));
        }
        else
        {
            DrLogA(
                "Status request send failed synchronously. done %u send tries, error %s",
                request->GetSendCount(), DRERRORSTRING(err));
        }

        delete overlapped;
    }
}

//
// Run in thread for each vertex
//
unsigned DVertexXComputePnController::CommandLoop()
{
    DrError err;
    UInt32 retries = 0;

    //
    // Get the vertex label
    //
    DrStr64 label;
    DVertexCommandBlock::GetPnPropertyLabel(&label,
                                            m_vertexId,
                                            m_vertexVersion);

    //
    // Wait for communication until error
    //
    do 
    {
        //
        // Create request to get vertex version property
        //
        XC_SETANDGETPROCESSINFO_REQINPUT request;
        memset(&request, 0, sizeof(request));
        request.Size = sizeof(request);
        request.pBlockOnPropertyLabel = label.GetString();
        request.BlockOnPropertyversionLastSeen = m_currentCommandVersion;
        request.MaxBlockTime = XCTIMEINTERVAL_MINUTE;
        // XXXX
        request.pPropertyFetchTemplate = (char *) label.GetString();
 
        //
        // Send the request and check for errors
        //
        PXC_SETANDGETPROCESSINFO_REQRESULTS pResults = NULL;
        err = XcSetAndGetProcessInfo(GetProcessHandle(),
                                     &request,
                                     &pResults,
                                     NULL);
        if (err == DrError_OK)
        {
            //
            // If request successfully sent, store process status and exit code
            //
            DrLogI( "Got command property");
            retries = 0;
            DrError processStatus = pResults->pProcessInfo->ProcessStatus;
            DrExitCode exitCode = pResults->pProcessInfo->ExitCode;

            if (processStatus == DrError_OK || exitCode != DrExitCode_StillActive)
            {
                //
                // If the PN thinks we have exited, so better make it so
                //
                err = DrError_Fail;
            }
        }

        //
        // If request was successful and other process doesn't think we're done
        //
        if (err == DrError_OK)
        {
            if (pResults->pProcessInfo->NumberofProcessProperties != 0)
            {
                //
                // Make sure there's only one property and it's the version
                //
                LogAssert(pResults->pProcessInfo->
                          NumberofProcessProperties == 1);
                PXC_PROCESSPROPERTY_INFO property =
                    pResults->pProcessInfo->ppProperties[0];
                LogAssert(::strcmp(property->pPropertyLabel, label) == 0);

                //
                // Update vertex version
                //
                UInt64 newVersion = property->PropertyVersion;
                if (newVersion < m_currentCommandVersion)
                {
                    //
                    // If vertex version is less than the current version, fail (logic error)
                    //
                    DrLogE(
                        "Property version went back in time. Property %s old version %I64u new version %I64u",
                        label.GetString(),
                        m_currentCommandVersion, newVersion);
                    err = DrError_ProcessPropertyVersionMismatch;
                }
                else if (newVersion == m_currentCommandVersion)
                {
                    //
                    // If version the same, report version the same 
                    //
                    DrLogI(
                        "Command timeout with same version. Property %s version %I64u",
                        label.GetString(), m_currentCommandVersion);
                }
                else if (newVersion > m_currentCommandVersion)
                {
                    //
                    // If new vertex version, let GM know what process is handling it
                    //
                    DrLogI(
                        "Property got new version. Property %s old version %I64u new version %I64u",
                        label.GetString(),
                        m_currentCommandVersion, newVersion);

                    m_currentCommandVersion = newVersion;

                    DrRef<DVertexCommandBlock> newCommand;
                    newCommand.Attach(new DVertexCommandBlock());

                    DrRef<DryadXComputePnProcessPropertyResponse> response;
                    response.Attach(new DryadXComputePnProcessPropertyResponse(pResults->pProcessInfo));

                    //
                    // Get new vertex command
                    //
                    err = newCommand->ReadFromResponseMessage(response, m_vertexId, m_vertexVersion);

                    //
                    // If no errors in getting command, act on it. Log any failures below
                    //
                    if (err == DrError_OK)
                    {
                        err = ActOnCommand(newCommand);
                    }
                }
            }
        }
        else
        {
            //
            // Log error and continue
            //
            DrLogE( "XcSetAndGetProcessInfo got error: %s", DRERRORSTRING(err));
        }

        //
        // If the error is related to disconnection, retry up to 4 times
        //
        if (err == DrError_RemoteDisconnected ||
            err == DrError_LocalDisconnected ||
            err == DrError_ConnectionFailed ||
            err == DrError_ResponseDisconnect)
        {
            ++retries;
            // todo: move 4 to global
            if (retries < 4)
            {
                DrLogW( "Retrying get");
                err = DrError_OK;
            }
        }

        //
        // If result was allocated, free it before next iteration
        //
        if (pResults != NULL)
        {
            XCERROR freeError = XcFreeMemory(pResults);
            LogAssert(freeError == DrError_OK);
        }
    } while (err == DrError_OK);

    //
    // Close this controller and take no more requests
    //
    DrLogD( "About to terminate");
    Terminate(err, DrExitCode_Fail);

    //
    // Sleep forever, waiting for verticies to complete and take down the process
    //
    Sleep(INFINITE);

    return 0;
}

//
// Create and return an XCompute environment
//
DVertexEnvironment* DVertexXComputePnControllerOuter::MakeEnvironment()
{
    return new DVertexXComputeEnvironment();
}

DVertexPnController* DVertexXComputePnControllerOuter::
    MakePnController(UInt32 vertexId,
                     UInt32 vertexVersion)
{
    return new DVertexXComputePnController(this, vertexId, vertexVersion);
}
