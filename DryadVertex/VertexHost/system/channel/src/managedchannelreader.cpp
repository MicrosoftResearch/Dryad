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

#include "managedchannelhelpers.h"

#pragma managed

ref class ManagedReaderBridge;

enum MCRState
{
    MCR_WaitingOpen,
    MCR_Opened,
    MCR_Draining,
    MCR_WaitingDrain,
    MCR_Drained
};

class RChannelManagedReader :
    public RChannelBufferReader, public RChannelBufferDefaultHandler, public RChannelThrottledStream
{
public:
    virtual ~RChannelManagedReader();

    void Initialize(IManagedReader^ managed, RChannelOpenThrottler* throttler);

    void Start(RChannelBufferPrefetchInfo* prefetchCookie,
               RChannelBufferReaderHandler* handler);
    void OpenAfterThrottle();
    void Interrupt();
    void Drain(RChannelItem* drainItem);
    bool GetTotalLength(UInt64* pTotalLength);
    void FillInStatus(DryadChannelDescription* status);
    void Close();

    void ReceiveData(RChannelBufferData* buffer, bool eof);
    void SignalError(int code, const char* error);
    void ReturnBuffer(RChannelBuffer* buffer);
    void UpdateTotalLength(long long totalLength);

private:
    void SupplyBuffer();
    IManagedReader^ Managed();

    MCHandle                       m_managed;
    int                            m_bufferSize;
    long long                      m_totalLength;
    long long                      m_processedLength;
    MCRState                       m_state;
    int                            m_outstandingBuffers;

    RChannelOpenThrottler*         m_throttler;

    RChannelBuffer*                m_finalBuffer;
    HANDLE                         m_finalEvent;

    CRITSEC                        m_cs;
    RChannelBufferReaderHandler*   m_handler;
};


ref class ManagedReaderBridge : public IReaderClient, public ManagedChannelBridge
{
public:
    ManagedReaderBridge(RChannelManagedReader* n)
    {
        native = n;
    }

    virtual void ReceiveData(Buffer^ b, bool eof)
    {
        RChannelBufferDataSettableOffset* buffer = static_cast<RChannelBufferDataSettableOffset*>(b->handle.ToPointer());
        DryadAlignedReadBlock* block = dynamic_cast<DryadAlignedReadBlock*>(buffer->GetData());
        block->Trim(b->size);
        buffer->SetOffset(b->offset);
        native->ReceiveData(buffer, eof);
    }

    virtual void SignalError(ErrorType type, System::String^ reason)
    {
        DrError code;
        switch (type)
    {
        case ErrorType::Open:
            code = DryadError_ChannelOpenError;
            break;

        default:
            code = DryadError_ChannelReadError;
            break;
        }

        DrString r(reason);
        native->SignalError(code, r.GetChars());
    }

    virtual void DiscardBuffer(Buffer^ b)
    {
        RChannelBufferData* buffer = static_cast<RChannelBufferData*>(b->handle.ToPointer());
        native->ReturnBuffer(buffer);
    }

    virtual void UpdateTotalLength(long long length)
    {
        native->UpdateTotalLength(length);
    }

private:
    RChannelManagedReader* native;
};


RChannelManagedReader::~RChannelManagedReader()
{
    ManagedHandleStore::Free(m_managed);
    CloseHandle(m_finalEvent);
}

void RChannelManagedReader::Initialize(IManagedReader^ managed, RChannelOpenThrottler* throttler)
{
    m_managed = ManagedHandleStore::Create(managed);
    m_outstandingBuffers = 0;
    m_bufferSize = managed->BufferSize;
    m_totalLength = managed->TotalLength;
    m_processedLength = 0;
    m_finalBuffer = NULL;
    m_state = MCR_WaitingOpen;

    m_throttler = throttler;

    m_finalEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
    LogAssert(m_finalEvent != NULL);
}

void RChannelManagedReader::Start(RChannelBufferPrefetchInfo* prefetchCookie,
                                  RChannelBufferReaderHandler* handler)
{
    m_handler = handler;

    if (m_throttler == NULL || m_throttler->QueueOpen(this))
    {
        OpenAfterThrottle();
    }
}

void RChannelManagedReader::OpenAfterThrottle()
{
    {
        AutoCriticalSection acs(&m_cs);

        if (m_state == MCR_Drained)
        {
            // we were canceled before we got here
            m_throttler->NotifyFileCompleted();
            return;
        }
        else
        {
            LogAssert(m_state == MCR_WaitingOpen);
            DrLogI("Setting state to opened");
            m_state = MCR_Opened;
        }

        Managed()->Start();

        for (int i=0; i<Managed()->OutstandingBuffers; ++i)
        {
            SupplyBuffer();
        }
    }
}

void RChannelManagedReader::Interrupt()
{
    {
        AutoCriticalSection acs(&m_cs);

        if (m_state == MCR_WaitingOpen)
        {
            DrLogI("Setting state to drained");
            m_state = MCR_Drained;
            return;
        }
        else if (m_state == MCR_Opened)
        {
            DrLogI("Setting state to draining");
            m_state = MCR_Draining;
        }
    }

    Managed()->Interrupt();
}

void RChannelManagedReader::Drain(RChannelItem* /*unused drainItem*/)
{
    {
        AutoCriticalSection acs(&m_cs);

        if (m_state == MCR_WaitingOpen || m_state == MCR_Drained)
        {
            DrLogI("Setting state to drained");
            m_state = MCR_Drained;
            return;
        }
        else if (m_state == MCR_Opened)
        {
            DrLogI("Setting state to draining");
            m_state = MCR_Draining;
        }
    }

    Managed()->WaitForDrain();

    DWORD dRet = ::WaitForSingleObject(m_finalEvent, INFINITE);
    LogAssert(dRet == WAIT_OBJECT_0);

    if (m_throttler != NULL)
    {
        m_throttler->NotifyFileCompleted();
    }

    {
        AutoCriticalSection acs(&m_cs);

        LogAssert(m_outstandingBuffers == 0);
        LogAssert(m_state == MCR_WaitingDrain);
        DrLogI("Setting state to drained");
        m_state = MCR_Drained;
    }
}

bool RChannelManagedReader::GetTotalLength(UInt64* pTotalLength)
{
    if (m_totalLength < 0)
    {
        *pTotalLength = 0;
        return false;
    }
    else
    {
        *pTotalLength = (UInt64)m_totalLength;
        return true;
    }
}

void RChannelManagedReader::FillInStatus(DryadChannelDescription* status)
{
    AutoCriticalSection acs(&m_cs);

    if (m_totalLength < 0)
    {
        status->SetChannelTotalLength(m_processedLength);
    }
    else
    {
        status->SetChannelTotalLength(m_totalLength);
    }

    status->SetChannelProcessedLength(m_processedLength);
}

void RChannelManagedReader::Close()
{
    Managed()->Close();
}

void RChannelManagedReader::ReceiveData(RChannelBufferData* buffer, bool eof)
{
    DrLogI("Receiving data");
    if (eof)
    {
        AutoCriticalSection acs(&m_cs);

        if (m_state == MCR_Opened)
        {
            DrLogI("Setting state to draining");
            m_state = MCR_Draining;

            LogAssert(m_finalBuffer == NULL);
            m_finalBuffer = ManagedHelpers::MakeEndOfStreamBuffer(this);
        }
        else
        {
            LogAssert(m_state == MCR_Draining);
        }
    }

    if (buffer->GetData()->GetAvailableSize() > 0)
    {
        {
            AutoCriticalSection acs(&m_cs);

            RChannelBufferDataSettableOffset* ob = dynamic_cast<RChannelBufferDataSettableOffset*>(buffer);
            LogAssert(ob != NULL);

            Int64 newProcessed = (Int64)(ob->GetOffset() + ob->GetData()->GetAvailableSize());
            if (newProcessed > m_processedLength)
            {
                m_processedLength = newProcessed;
            }
        }

        m_handler->ProcessBuffer(buffer);
    }
    else
    {
        ReturnBuffer(buffer);
    }
}

void RChannelManagedReader::SignalError(int code, const char* error)
{
    AutoCriticalSection acs(&m_cs);

    DrLogE("Got managed reader error %x: %s", code, error);

    LogAssert(m_state == MCR_Opened || m_state == MCR_Draining);
    DrLogI("Setting state to draining");
    m_state = MCR_Draining;

    if (m_finalBuffer == NULL)
    {
        m_finalBuffer = ManagedHelpers::MakeErrorBuffer(code, error, this);
    }
}

void RChannelManagedReader::ReturnBuffer(RChannelBuffer* buffer)
{
    DrLogI("Receiving returned buffer");
    /* discard buffer */
    buffer->DecRef();

    RChannelBuffer* toProcess = NULL;

    {
        AutoCriticalSection acs(&m_cs);

        LogAssert(m_outstandingBuffers > 0);
        --m_outstandingBuffers;

        if (m_state == MCR_Draining)
        {
            if (m_outstandingBuffers == 0)
            {
                if (m_finalBuffer == NULL)
                {
                    DrLogI("Setting state to waiting drain");
                    m_state = MCR_WaitingDrain;
                    ::SetEvent(m_finalEvent);
                }
                else
                {
                    toProcess = m_finalBuffer;
                    ++m_outstandingBuffers;
                    m_finalBuffer = NULL;
                }
            }
        }
        else
        {
            LogAssert(m_state == MCR_Opened);
            DrLogI("Supplying buffer");
            SupplyBuffer();
            DrLogI("Done supplying buffer");
        }
    }

    if (toProcess != NULL)
    {
        m_handler->ProcessBuffer(toProcess);
    }
}

void RChannelManagedReader::UpdateTotalLength(long long totalLength)
{
    AutoCriticalSection acs(&m_cs);

    m_totalLength = totalLength;
}

void RChannelManagedReader::SupplyBuffer()
{
    RChannelBufferData* buffer;

    buffer = ManagedHelpers::MakeDataBuffer(RCHANNEL_BUFFER_OFFSET_UNDEFINED, m_bufferSize, this);
    ++m_outstandingBuffers;

    Buffer^ b = gcnew Buffer();
    b->offset = RCHANNEL_BUFFER_OFFSET_UNDEFINED;
    b->size = m_bufferSize;
    Size_t s;
    b->storage = System::IntPtr(buffer->GetData()->GetDataAddress(0, &s, NULL));
    b->handle = System::IntPtr(buffer);

    Managed()->SupplyBuffer(b);
}

IManagedReader^ RChannelManagedReader::Managed()
{
    return (IManagedReader^) ManagedHandleStore::Get(m_managed);
}


bool ManagedChannelFactory::RecognizesReaderUri(const char* uriChars)
{
    System::String^ uri = gcnew System::String(uriChars);
    return Factory::RecognizesReaderUri(uri);
}

RChannelBufferReader* ManagedChannelFactory::OpenReader(const char* uriChars, int numberOfReaders, LPDWORD localInputChannels,
                                                        RChannelOpenThrottler* throttler)
{
    System::String^ uri = gcnew System::String(uriChars);

    RChannelManagedReader* native = new RChannelManagedReader();

    ManagedReaderBridge^ client = gcnew ManagedReaderBridge(native);
    IManagedReader^ managed = Factory::OpenReader(uri, numberOfReaders, client, client);

    if (localInputChannels != NULL && managed->IsLocal)
    {
        ++(*localInputChannels);
    }

    native->Initialize(managed, throttler);

    return native;
}