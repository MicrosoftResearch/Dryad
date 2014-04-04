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
#include "channelwriter.h"

#pragma managed

using namespace Microsoft::Research::Dryad::Channel;

ref class ManagedWriterBridge;

class ManagedWriterHolder
{
public:
    ManagedWriterHolder(DryadFixedMemoryBuffer* buffer, RChannelBufferWriterHandler* handler)
    {
        m_buffer = buffer;
        m_handler = handler;
    }

    DryadFixedMemoryBuffer*       m_buffer;
    RChannelBufferWriterHandler*  m_handler;
};

class RChannelManagedWriter :
    public RChannelBufferWriter, public RChannelThrottledStream
{
public:
    virtual ~RChannelManagedWriter();

    void Initialize(IManagedWriter^ managed, RChannelOpenThrottler* throttler);

    void Start();

    DryadFixedMemoryBuffer* GetNextWriteBuffer();
    DryadFixedMemoryBuffer* GetCustomWriteBuffer(Size_t bufferSize);

    bool WriteBuffer(DryadFixedMemoryBuffer* buffer,
                     bool flushAfter,
                     RChannelBufferWriterHandler* handler);

    void ReturnUnusedBuffer(DryadFixedMemoryBuffer* buffer);

    void WriteTermination(RChannelItemType reasonCode,
                          RChannelBufferWriterHandler* handler);

    void FillInStatus(DryadChannelDescription* status);

    void Drain(RChannelItemRef* pReturnItem);

    /* shut down the channel. After Close returns no further calls can
       be made to this interface.
    */
    void Close();

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);

    void ReturnBuffer(Buffer^ b, DrError code, const char* errorMessage);

    void OpenAfterThrottle();

private:
    IManagedWriter^ Managed();

    MCHandle                       m_managed;
    int                            m_bufferSize;
    Size_t                         m_bufferAlignment;
    long long                      m_nextOffset;
    long long                      m_processedLength;

    RChannelOpenThrottler*         m_throttler;

    RChannelItemRef                m_completionItem;

    CRITSEC                        m_cs;
};


ref class ManagedWriterBridge : public IWriterClient, public ManagedChannelBridge
{
public:
    ManagedWriterBridge(RChannelManagedWriter* n)
    {
        native = n;
    }

    virtual void ReturnBuffer(Buffer^ b, ErrorType type, System::String^ errorMessage)
    {
        if (errorMessage == nullptr)
        {
            native->ReturnBuffer(b, S_OK, NULL);
        }
        else
        {
            DrError code;
            switch (type)
            {
            case ErrorType::Open:
                code = DryadError_ChannelOpenError;
                break;

            default:
                code = DryadError_ChannelWriteError;
                break;
            }

            DrString msg(errorMessage);
            native->ReturnBuffer(b, code, msg.GetChars());
        }
    }

private:
    RChannelManagedWriter* native;
};


RChannelManagedWriter::~RChannelManagedWriter()
{
    ManagedHandleStore::Free(m_managed);
}

void RChannelManagedWriter::Initialize(IManagedWriter^ managed, RChannelOpenThrottler* throttler)
{
    m_managed = ManagedHandleStore::Create(managed);
    m_bufferSize = managed->BufferSize;
    m_bufferAlignment = managed->BufferAlignment;
    m_nextOffset = 0;
    m_processedLength = 0;

    m_throttler = throttler;
}

void RChannelManagedWriter::Start()
{
    if (m_throttler == NULL || m_throttler->QueueOpen(this))
    {
        OpenAfterThrottle();
    }
}

void RChannelManagedWriter::OpenAfterThrottle()
{
    DrLogI("Starting managed writer channel");
    Managed()->Start();
    DrLogI("Managed writer channel started");
}

DryadFixedMemoryBuffer* RChannelManagedWriter::GetNextWriteBuffer()
{
    return GetCustomWriteBuffer(m_bufferSize);
}

DryadFixedMemoryBuffer* RChannelManagedWriter::GetCustomWriteBuffer(Size_t bufferSize)
{
    return new DryadAlignedWriteBlock(bufferSize, m_bufferAlignment);
}

void RChannelManagedWriter::ReturnUnusedBuffer(DryadFixedMemoryBuffer* buffer)
{
    buffer->DecRef();
}

bool RChannelManagedWriter::WriteBuffer(DryadFixedMemoryBuffer* buffer,
                                        bool flushAfter,
                                        RChannelBufferWriterHandler* handler)
{
    RChannelItemType status = RChannelItem_Data;

    {
        AutoCriticalSection acs(&m_cs);

        if (m_completionItem != NULL)
        {
            status = m_completionItem->GetType();
            LogAssert(status != RChannelItem_Data);
        }
    }

    if (status != RChannelItem_Data)
    {
        // there was already an error, so signal it and do nothing with the write
        handler->ProcessWriteCompleted(status);
        return false;
    }

    size_t dataSize;
    void *dataAddr = buffer->GetDataAddress(0, &dataSize, NULL);
    Size_t dataToWrite = buffer->GetAvailableSize();
    LogAssert(dataToWrite <= dataSize);
    LogAssert(dataToWrite < 0x80000000);

    ManagedWriterHolder* holder = new ManagedWriterHolder(buffer, handler);

    Buffer^ b = gcnew Buffer();

    {
        AutoCriticalSection acs(&m_cs);

        b->offset = m_nextOffset;
        b->size = (int) dataToWrite;
        b->storage = System::IntPtr(dataAddr);
        b->handle = System::IntPtr(holder);

        m_nextOffset += dataToWrite;
    }

    // the return code specifies whether the producer should block until outstanding
    // writes complete
    return Managed()->Write(b);
}

void RChannelManagedWriter::WriteTermination(RChannelItemType reasonCode,
                                             RChannelBufferWriterHandler* handler)
{
    RChannelItemType status = RChannelItem_Data;

    {
        AutoCriticalSection acs(&m_cs);

        if (m_completionItem != NULL)
        {
            status = m_completionItem->GetType();
            LogAssert(status != RChannelItem_Data);
        }
    }

    if (status != RChannelItem_Data)
    {
        // there was already an error, so signal it and do nothing with the termination
        handler->ProcessWriteCompleted(status);
        return;
    }

    ManagedWriterHolder* holder = new ManagedWriterHolder(NULL, handler);

    Buffer^ b = gcnew Buffer();
    b->offset = -1;
    b->size = -1;
    b->storage = System::IntPtr(NULL);
    b->handle = System::IntPtr(holder);

    Managed()->Write(b);
}

void RChannelManagedWriter::ReturnBuffer(Buffer^ b, DrError code, const char* errorMessage)
{
    ManagedWriterHolder* holder = static_cast<ManagedWriterHolder*>(b->handle.ToPointer());

    DryadFixedMemoryBuffer* buffer = holder->m_buffer;
    RChannelBufferWriterHandler* handler = holder->m_handler;

    delete holder;

    DrLogI("Returning buffer offset %I64d size %d code %08x error %s", b->offset, b->size, code, errorMessage);

    RChannelItemType status = RChannelItem_Data;
    {
        AutoCriticalSection acs(&m_cs);

        if (errorMessage != NULL && m_completionItem == NULL)
        {
            m_completionItem.Attach(ManagedHelpers::MakeErrorItem(code, errorMessage));
        }

        if (buffer == NULL && m_completionItem == NULL)
        {
            m_completionItem.Attach(ManagedHelpers::MakeEndOfStreamItem());
        }

        if (m_completionItem != NULL)
        {
            status = m_completionItem->GetType();
            LogAssert(status != RChannelItem_Data);
        }

        if (buffer != NULL)
        {
            LogAssert(m_processedLength == b->offset);
            m_processedLength += b->size;
            buffer->DecRef();
        }
    }

    if (status != RChannelItem_Data && m_throttler != NULL)
    {
        m_throttler->NotifyFileCompleted();
    }

    handler->ProcessWriteCompleted(status);
}

void RChannelManagedWriter::FillInStatus(DryadChannelDescription* status)
{
    AutoCriticalSection acs(&m_cs);

    status->SetChannelTotalLength(0);
    status->SetChannelProcessedLength(m_processedLength);
}

void RChannelManagedWriter::Drain(RChannelItemRef* pReturnItem)
{
    /* Drain shouldn't have been called unless a termination item has
       been sent, so eventually the writer will exit... */
    Managed()->WaitForClose();

    {
        AutoCriticalSection acs(&m_cs);

        /* and that's it, nothing more to do */
        LogAssert(m_completionItem != NULL);
        *pReturnItem = m_completionItem;
        m_completionItem = NULL;
    }
}

void RChannelManagedWriter::Close()
{
    LogAssert(m_completionItem == NULL);
}

UInt64 RChannelManagedWriter::GetInitialSizeHint()
{
    return 0;
}

void RChannelManagedWriter::SetInitialSizeHint(UInt64 /*hint*/)
{
}

IManagedWriter^ RChannelManagedWriter::Managed()
{
    return (IManagedWriter^) ManagedHandleStore::Get(m_managed);
}


bool ManagedChannelFactory::RecognizesWriterUri(const char* uriChars)
{
    System::String^ uri = gcnew System::String(uriChars);
    return Factory::RecognizesWriterUri(uri);
}

RChannelBufferWriter* ManagedChannelFactory::OpenWriter(const char* uriChars, int numberOfWriters, bool* pBreakOnRecordBoundaries,
                                                        RChannelOpenThrottler* throttler)
{
    System::String^ uri = gcnew System::String(uriChars);

    RChannelManagedWriter* native = new RChannelManagedWriter();

    ManagedWriterBridge^ client = gcnew ManagedWriterBridge(native);
    IManagedWriter^ managed = Factory::OpenWriter(uri, numberOfWriters, client, client);

    if (managed->BreakOnRecordBoundaries)
    {
        *pBreakOnRecordBoundaries = true;
    }

    native->Initialize(managed, throttler);

    return native;
}
