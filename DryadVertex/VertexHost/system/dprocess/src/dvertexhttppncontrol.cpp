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

#include <dryadvertex.h>
#include <dryadpropertiesdef.h>
#include <dryaderrordef.h>
#include <dryadnativeport.h>
#include <dryadstandaloneini.h>
#include <httppropertyblock.h>

#include "dvertexhttppncontrol.h"

#include "DrString.h"

using namespace System::IO;
using namespace System::Net;
using namespace System::Runtime::InteropServices;
using namespace System::Threading::Tasks;

using namespace Microsoft::Research::Peloponnese;

ref class DVertexLogger : public ILogger
{
public:
    virtual void Log(System::String^ message, System::String^ file, System::String^ function, int line)
    {
        if (DrLogging::Enabled(LogLevel_Info))
        {
            DrString sMessage(message);
            DrString sFile(file);
            DrString sFunction(function);
            DrLogHelper(LogLevel_Info, sFile.GetChars(), sFunction.GetChars(), line)("%s", sMessage.GetChars());
        }
    }

    virtual void Stop()
    {
    }
};

ref class HttpClientHolder
{
public:
    static HttpClientHolder()
    {
        s_client = gcnew NotHttpClient::NotHttpClient(true, 1, 10000, gcnew DVertexLogger());
    }

    static NotHttpClient::IHttpRequest^ Create(System::Uri^ uri)
    {
        return s_client->CreateRequest(uri);
    }

private:
    static NotHttpClient::NotHttpClient^ s_client;
};

class DVertexHttpSetStatus : public DryadHttpPnProcessPropertyRequest
{
public:
    DVertexHttpSetStatus(DVertexHttpPnController* parent,
                         DVertexPnControllerOuter* parentOuter,
                         UInt32 exitOnCompletion,
                         bool isAssert,
                         bool notifyWaiters);

    const char* GetPropertyLabel();
    const char* GetPropertyString();
    bool GetNotifyWaiters();

    bool IsAssert();

    void IncrementSendCount();
    UInt32 GetSendCount();
    UInt32 ExitOnCompletion();

    void Process(DrError err);

private:
    DVertexHttpPnController*             m_parent;
    DVertexPnControllerOuter*            m_parentOuter;
    UInt32                               m_exitOnCompletion;
    bool                                 m_notifyWaiters;
    bool                                 m_isAssert;

    UInt32                               m_sendCount;
};

//
// Constructor. Create Set Status request.
//
DVertexHttpSetStatus::
    DVertexHttpSetStatus(DVertexHttpPnController* parent,
                         DVertexPnControllerOuter* parentOuter,
                         UInt32 exitOnCompletion,
                         bool isAssert,
                         bool notifyWaiters)
{
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

const char* DVertexHttpSetStatus::GetPropertyLabel()
{
    return m_label.GetString();
}

const char* DVertexHttpSetStatus::GetPropertyString()
{
    return m_string.GetString();
}

bool DVertexHttpSetStatus::GetNotifyWaiters()
{
    return m_notifyWaiters;
}

//
// Get assertion level
//
bool DVertexHttpSetStatus::IsAssert()
{
    return m_isAssert;
}

UInt32 DVertexHttpSetStatus::ExitOnCompletion()
{
    return m_exitOnCompletion;
}

//
// Increment the number of retries attempted 
//
void DVertexHttpSetStatus::IncrementSendCount()
{
    ++m_sendCount;
}

//
// Return the number of retries attempted
//
UInt32 DVertexHttpSetStatus::GetSendCount()
{
    return m_sendCount;
}

//
// Handle response from cluster infrastructure
//
void DVertexHttpSetStatus::Process(DrError err)
{
    //
    // If status successfully sent, log success and check on vertex status
    //
    if (err == DrError_OK)
    {
        DrLogI( "PN send succeeded. label %s", m_label.GetString());

        if (m_exitOnCompletion != DrExitCode_StillActive)
        {
/*
            DrLogI("Exiting after status is set, but first waiting for command loop to exit");

            // wait two seconds and then exit anyway
            DWORD dRet = ::WaitForSingleObject(m_parent->GetCommandLoopEvent(), 2000);
            if (dRet == WAIT_OBJECT_0)
            {
                DrLogI("Command loop has exited");
            }
            else
            {
                LogAssert(dRet == WAIT_TIMEOUT);
                DrLogI("Timed out waiting for command loop to exit");
            }
*/
            DrLogI("Exiting after status is set");

            //
            // If vertex is not still active, report that it is exiting
            // this may kill this process if all verticies are complete
            //
            m_parentOuter->VertexExiting(m_exitOnCompletion);
        }

        m_parent->ConsiderNextSendRequest(NULL);

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

            m_parent->ConsiderNextSendRequest(this);

            return;
        }
    }

    //
    // If m_isAssert this send was generated by an assertion that already happened, in which case
    // just report a warning. Otherwise we should assert now, since we can't send
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
// Constructor. Calls parent constructor only.
//
DVertexHttpPnController::
    DVertexHttpPnController(DVertexPnControllerOuter* parent,
                            UInt32 vertexId, UInt32 vertexVersion,
                            const char* serverAddress) :
        DVertexPnController(parent, vertexId, vertexVersion)
{
    m_serverAddress.Set(serverAddress);
    m_commandLoopEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    m_sending = false;
    m_nextSend = NULL;
}

DVertexHttpPnController::~DVertexHttpPnController()
{
    BOOL bRetval = CloseHandle(m_commandLoopEvent);
    LogAssert(bRetval != 0);
}

HANDLE DVertexHttpPnController::GetCommandLoopEvent()
{
    return m_commandLoopEvent;
}

//
// Create a new set status request
//
DryadPnProcessPropertyRequest*
    DVertexHttpPnController::MakeSetStatusRequest(UInt32 exitOnCompletion,
                                                  bool isAssert,
                                                  bool notifyWaiters)
{
    return new DVertexHttpSetStatus(this, m_parent,
                                    exitOnCompletion, isAssert,
                                    notifyWaiters);
}

ref class SetStatusWrapper
{
public:
    SetStatusWrapper(System::String^ a, System::String^ l, System::String^ s, array<unsigned char>^ d, bool n, DVertexHttpSetStatus* p)
    {
        address = a;
        label = l;
        status = s;
        payload = d;
        notifyWaiters = n;
        parent = p;
        parent->IncRef();
    }

    System::String^ address;
    System::String^ label;
    System::String^ status;
    bool            notifyWaiters;
    array<unsigned char>^ payload;

    DVertexHttpSetStatus* parent;
};

static void SetStatusDataCallback(System::Object^ arg)
{
    SetStatusWrapper^ info = (SetStatusWrapper^) arg;

    System::String^ uri = System::String::Format(
        "{0}?op=setstatus&key={1}&shortstatus={2}&notifywaiters={3}",
        info->address, info->label, info->status, info->notifyWaiters ? "true" : "false");
    NotHttpClient::IHttpRequest^ request = HttpClientHolder::Create(gcnew System::Uri(uri));
    request->Timeout = 15 * 1000; // if it doesn't respond in 15 seconds, this tears down the connection so we can try again
    request->Method = "POST";
    Stream^ rs = nullptr;

    DrError err = DrError_OK;
    try
    {
        rs = request->GetRequestStream();
        rs->Write(info->payload, 0, info->payload->Length);
        rs->Close();

        NotHttpClient::IHttpResponse^ response = request->GetResponse();
        delete response;
    }
    catch (NotHttpClient::NotHttpException^ e)
    {
        System::IntPtr ePtr = Marshal::StringToHGlobalAnsi(e->Response->StatusDescription->ToString());
        char* eStr = (char *)(void *)ePtr;
        DrLogW("Got SetStatus error %d:%s", (int)e->Response->StatusCode, eStr);
        Marshal::FreeHGlobal(ePtr);
        err = DrError_ConnectionFailed;
    }
    catch (System::Exception^ e)
    {
        System::IntPtr ePtr = Marshal::StringToHGlobalAnsi(e->ToString());
        char* eStr = (char *)(void *)ePtr;
        DrLogW("Got SetStatus exception %s", eStr);
        Marshal::FreeHGlobal(ePtr);
        err = DrError_ConnectionFailed;
    }
    finally
    {
        delete rs;
    }

    info->parent->Process(err);
    info->parent->DecRef();
}

//
// Send updated status to vertex service
//
void DVertexHttpPnController::
    SendSetStatusRequest(DryadPnProcessPropertyRequest* r)
{
    bool mustSend = false;

    {
        AutoCriticalSection acs(&m_baseCS);

        if (m_sending)
        {
            if (m_nextSend == NULL)
            {
                DrLogI("Queueing send for later");
            }
            else
            {
                DrLogI("Overwriting queued send with newer version");
            }
            m_nextSend = r;
        }
        else
        {
            DrLogI("Sending now");
            m_sending = true;
            LogAssert(m_nextSend == NULL);
            mustSend = true;
        }
    }

    if (mustSend)
    {
        SendSetStatusRequestInternal(r);
    }
}

void DVertexHttpPnController::ConsiderNextSendRequest(DryadPnProcessPropertyRequest* queuedRequestIn)
{
    DrRef<DryadPnProcessPropertyRequest> queuedRequest = queuedRequestIn;

    {
        AutoCriticalSection acs(&m_baseCS);

        LogAssert(m_sending);

        if (queuedRequest == NULL)
        {
            if (m_nextSend == NULL)
            {
                DrLogI("No more sends");
                m_sending = false;
            }
            else
            {
                DrLogI("Queueing next send");
                queuedRequest = m_nextSend;
                m_nextSend = NULL;
            }
        }
        else
        {
            DrLogI("Requeueing send");
        }
    }

    if (queuedRequest != NULL)
    {
        SendSetStatusRequestInternal(queuedRequest);
    }
}

void DVertexHttpPnController::SendSetStatusRequestInternal(DryadPnProcessPropertyRequest* r)
{
    //
    // Cast request to required type and make sure it's valid
    //
    DVertexHttpSetStatus* request;
    try
    {
        request = dynamic_cast<DVertexHttpSetStatus*>(r);
    }
    catch (System::Exception^ e)
    {
        DrString msg(e->ToString());
        DrLogA("Got cast exception %s", msg.GetChars());
    }
    LogAssert(request != NULL);

    request->IncrementSendCount();

    DrMemoryBuffer* buffer = request->GetPropertyBlock();

    SetStatusWrapper^ wrapper;
    try
    {
        System::String^ address = gcnew System::String(m_serverAddress.GetString());
        System::String^ label = gcnew System::String(request->GetPropertyLabel());
        System::String^ status = gcnew System::String(request->GetPropertyString());
        bool notifyWaiters = request->GetNotifyWaiters();

        Size_t payloadSize = buffer->GetAvailableSize();

        LogAssert(payloadSize < System::Int32::MaxValue);
        array<unsigned char>^ payload = gcnew array<unsigned char>((int)payloadSize);
        Size_t bufferSize;
        void* rawPtr = buffer->GetDataAddress(0, &bufferSize, NULL);
        System::IntPtr bufferPtr(rawPtr);

        Marshal::Copy(bufferPtr, payload, 0, (int)payloadSize);

        wrapper = gcnew SetStatusWrapper(address, label, status, payload, notifyWaiters, request);
    }
    catch (System::Exception^ e)
    {
        DrString msg(e->ToString());
        DrLogA("Failed to make wrapper: ", msg.GetChars());
    }

    Task^ t = gcnew Task(gcnew System::Action<System::Object^>(SetStatusDataCallback), wrapper);
    t->Start();
}

//
// Run in thread for each vertex
//
unsigned DVertexHttpPnController::CommandLoop()
{
    DrError err;
    UInt32 retries = 0;
    UInt64 commandVersion = 0;
    UInt32 blockTimeout = 15 * 1000;
    UInt32 requestTimeout = 60 * 1000;

    //
    // Get the vertex label
    //
    DrStr64 label;
    DVertexCommandBlock::GetPnPropertyLabel(&label,
                                            m_vertexId,
                                            m_vertexVersion);

    System::String^ labelString = Marshal::PtrToStringAnsi((System::IntPtr)(void*)label.GetString());
    System::String^ serverAddress = Marshal::PtrToStringAnsi((System::IntPtr)(void*)m_serverAddress.GetString());
    System::String^ uriBase = serverAddress + "?timeout=" + blockTimeout + "&key=" + labelString;

    //
    // Wait for communication until error
    //
    do 
    {
        System::String^ uriString = uriBase + System::String::Format("&version={0}", commandVersion);

		System::Uri^ uri;
		try
		{
			uri = gcnew System::Uri(uriString);
		}
		catch (System::Exception^ e)
		{
			DrLogE("Got exception creating Uri %s: %s", DrString(uriString).GetChars(), DrString(e->ToString()).GetChars());
		}
        NotHttpClient::IHttpRequest^ request = HttpClientHolder::Create(uri);
        request->Timeout = requestTimeout;

        NotHttpClient::IHttpResponse^ response = nullptr;
        Stream^ data = nullptr;
        MemoryStream^ bytes = nullptr;

        try
        {
            DrLogI("Connecting to Process Service");

            response = request->GetResponse();

            System::String^ processStatus = response->Headers["X-Dryad-ProcessStatus"];
            DrExitCode exitCode = System::UInt32::Parse(response->Headers["X-Dryad-ProcessExitCode"]);
            UInt64 newVersion = System::UInt64::Parse(response->Headers["X-Dryad-ValueVersion"]);

            data = response->GetResponseStream();
            bytes = gcnew MemoryStream();
            data->CopyTo(bytes);
            array<unsigned char>^ responseData = bytes->ToArray();

            // if we got here without throwing an exception, the web get succeeded
            retries = 0;

            {
                System::IntPtr sPtr = Marshal::StringToHGlobalAnsi(processStatus);
                char* sStr = (char *)(void *)sPtr;
                DrLogI("Got status %s exitCode 0x%x", sStr, exitCode);
                Marshal::FreeHGlobal(sPtr);
            }

            if (processStatus != "Running" || exitCode != DrExitCode_StillActive)
            {
                //
                // If the server thinks we have exited, so better make it so
                //
                err = DrError_Fail;
            }
            else
            {
                //
                // Update vertex version
                //
                if (newVersion < commandVersion)
                {
                    //
                    // If vertex version is less than the current version, fail (logic error)
                    //
                    DrLogE(
                        "Property version went back in time. Property %s old version %I64u new version %I64u",
                        label.GetString(),
                        commandVersion, newVersion);
                    err = DrError_ProcessPropertyVersionMismatch;
                }
                else if (newVersion == commandVersion)
                {
                    //
                    // If version the same, report version the same 
                    //
                    DrLogI(
                        "Command timeout with same version. Property %s version %I64u",
                        label.GetString(), commandVersion);
                }
                else if (newVersion > m_currentCommandVersion)
                {
                    //
                    // If new vertex version, let GM know what process is handling it
                    //
                    DrLogI(
                        "Property got new version. Property %s old version %I64u new version %I64u",
                        label.GetString(),
                        commandVersion, newVersion);

                    commandVersion = newVersion;

                    DrRef<DVertexCommandBlock> newCommand;
                    newCommand.Attach(new DVertexCommandBlock());

                    DrRef<DryadHttpPnProcessPropertyResponse> response;
                    {
                        pin_ptr<unsigned char> nativeData = &(responseData[0]);
                        response.Attach(new DryadHttpPnProcessPropertyResponse(responseData->Length, nativeData));
                    }

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
                        if (err != DrError_OK)
                        {
                            DrLogW("Command loop got bad vertex command %s", DRERRORSTRING(err));
                        }
                    }
                }
            }
        }
        catch (System::Exception^ e)
        {
            ++retries;
            System::IntPtr ePtr = Marshal::StringToHGlobalAnsi(e->Message);
            char* eStr = (char *)(void *)ePtr;
            DrLogE("Command loop http request failed try %d with exception %s", retries, eStr);
            Marshal::FreeHGlobal(ePtr);

            if (retries >= 4)
            {
                err = DrError_ConnectionFailed;
            }
            else
            {
                DrLogI("Retrying connection");
            }
        }
        finally
        {
            delete response;
            delete data;
            delete bytes;
        }
    } while (err == DrError_OK);

    DrLogI("Command loop exiting");
    BOOL bRet = ::SetEvent(m_commandLoopEvent);
    LogAssert(bRet != 0);

    if (err != DryadError_VertexReceivedTermination)
    {
        //
        // Close this controller and take no more requests
        //
        DrLogD( "About to terminate");
        Terminate(err, DrExitCode_Fail);
    }

    //
    // Sleep forever, waiting for vertices to complete and take down the process
    //
    Sleep(INFINITE);

    return 0;
}

//
// This is just a factory method to generate controllers of the correct concrete type
//
DVertexPnController* DVertexHttpPnControllerOuter::
    MakePnController(UInt32 vertexId,
                     UInt32 vertexVersion)
{
    DVertexPnController* controller = NULL;

    System::String^ serverAddress = System::Environment::GetEnvironmentVariable("DRYAD_PROCESS_SERVER_URI");
    if (serverAddress == nullptr)
    {
        DrLogA("Can't get environment string DRYAD_PROCESS_SERVER_URI");
    }
    else
    {
        System::IntPtr strPtr = Marshal::StringToHGlobalAnsi(serverAddress);
        char* serverStr = (char *)(void *)strPtr;
        DrLogI("Got environment DRYAD_PROCESS_SERVER_URI=%s", serverStr);
        controller = new DVertexHttpPnController(this, vertexId, vertexVersion, serverStr);
        Marshal::FreeHGlobal(strPtr);
    }

    return controller;
}
