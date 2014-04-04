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

#include "dryadnativeport.h"
#include "channelreader.h"
#include "concreterchannelhelpers.h"
#include <dvertexcommand.h>

#pragma warning(disable:4995)
#include <map>

class RChannelBufferReaderNative :
    public RChannelBufferReader, public RChannelBufferDefaultHandler,
    public RChannelThrottledStream
{
public:
    class ReadHandler : public DryadNativePort::Handler
    {
    public:
        ReadHandler(UInt64 requestOffset, UInt64 streamOffset,
                    UInt32 bufferSize,
                    size_t bufferAlignment);
        virtual ~ReadHandler();

        UInt64 GetStreamOffset();
        void* GetData();
        DryadAlignedReadBlock* GetBlock();
        RChannelBuffer* TransferChannelBuffer();
        bool IsLastDataBuffer();

        virtual void QueueRead(DryadNativePort* port) = 0;

    protected:
        void SetChannelBuffer(RChannelBuffer* buffer);
        void SignalLastDataBuffer();

    private:
        UInt64                       m_streamOffset;
        DryadAlignedReadBlock*       m_block;
        RChannelBuffer*              m_buffer;
        bool                         m_isLastDataBuffer;
        DrBListEntry                 m_listPtr;
        friend class DryadBList<ReadHandler>;
    };

    RChannelBufferReaderNative(UInt32 prefetchBuffers,
                               DryadNativePort* port,
                               WorkQueue* workQueue,
                               RChannelOpenThrottler* openThrottler,
                               bool supportsLazyOpen);
    virtual ~RChannelBufferReaderNative();

    void Interrupt();

    void ReturnBuffer(RChannelBuffer* buffer);

    virtual void FillInStatus(DryadChannelDescription* status);

    bool EnsureOpenForRead(ReadHandler* handler);

    void OpenAfterThrottle();

protected:
    void StartNativeReader(RChannelBufferReaderHandler* handler);
    void DrainNativeReader();

    void SetPrefetchBufferCount(UInt32 numberOfBuffers);
    UInt32 GetPrefetchBufferCount();

    void AssociateHandleWithPort(HANDLE h);
    void SetTotalLength(UInt64 totalLength);
    bool GetTotalLength(UInt64* pLen);

    /* called with baseDR held */
    void OpenNativeReader();
    /* called with baseDR held */
    RChannelBuffer* CloseNativeReader();

    void SetErrorBuffer(RChannelBuffer* buffer);

    void DispatchBuffer(ReadHandler* handler, bool makeNewHandler,
                        ReadHandler* fillInReadHandler);

    RChannelBuffer* MakeEndOfStreamBuffer(UInt64 streamOffset);
    RChannelBuffer* MakeErrorBuffer(UInt64 streamOffset,
                                    DrError errorCode);
    RChannelBuffer* MakeOpenErrorBuffer(DrError errorCode,
                                        const char* description);
    RChannelBuffer* MakeDataBuffer(UInt64 streamOffset,
                                   DryadLockedMemoryBuffer* block);
    virtual void MakeFileMetaData(DryadMetaData* metaData,
                                  UInt64 streamOffset) = 0;
    virtual bool LazyOpenFile();
    virtual void FillInOpenedDetails(ReadHandler* handler);
    virtual void EagerCloseFile();

    CRITSEC* GetBaseDR();

private:
    enum State {
        S_Closed,
        S_Stopped,
        S_Running,
        S_Stopping
    };

    enum OpenState {
        OS_Closed,
        OS_NotOpened,
        OS_Waiting,
        OS_Opened,
        OS_OpenError,
        OS_Stopped
    };

    typedef DryadBList<ReadHandler> HandlerList;
    typedef std::map<UInt64, ReadHandler *> OffsetHandlerMap;

    bool FinishUsingFile();
    virtual ReadHandler* GetNextReadHandler(bool lazyOpenDone) = 0;


    UInt32                                   m_prefetchBuffers;

    RChannelBufferReaderHandler*             m_handler;
    DryadNativePort*                         m_port;
    WorkQueue*                               m_workQueue;
    RChannelOpenThrottler*                   m_openThrottler;
    bool                                     m_supportsLazyOpen;

    State                                    m_state;
    OpenState                                m_openState;
    bool                                     m_fetching;
    bool                                     m_sentLastBuffer;
    UInt32                                   m_outstandingHandlers;
    UInt32                                   m_outstandingBuffers;
    UInt64                                   m_nextStreamOffsetToProcess;
    UInt64                                   m_totalLength;
    OffsetHandlerMap                         m_reorderMap;
    DrRef<RChannelBuffer>                    m_errorBuffer;
    HANDLE                                   m_handlerReturnEvent;
    HANDLE                                   m_bufferReturnEvent;
    HandlerList                              m_openWaitingList;
    bool                                     m_drainingOpenQueue;

    DryadOrderedSendLatch<ChannelBufferList>  m_latch;

protected:
    CRITSEC                                  m_baseCS;

    friend class ReadHandler;
};

class RChannelBufferReaderNativeFile : public RChannelBufferReaderNative
{
public:
    class FileReadHandler : public RChannelBufferReaderNative::ReadHandler
    {
    public:
        FileReadHandler(HANDLE fileHandle,
                        bool detailsPresent,
                        UInt64 streamOffset,
                        UInt32 dataSize, size_t dataAlignment,
                        RChannelBufferReaderNativeFile* parent);

        void ProcessIO(DrError cse, UInt32 numBytes);
        void SetFileHandle(HANDLE h);
        HANDLE GetFileHandle();
        void QueueRead(DryadNativePort* port);

    private:
        HANDLE                             m_fileHandle;
        bool                               m_detailsPresent;
        RChannelBufferReaderNativeFile*    m_parent;
    };

    RChannelBufferReaderNativeFile(UInt32 bufferSize,
                                   size_t bufferAlignment,
                                   UInt32 prefetchBuffers,
                                   DryadNativePort* port,
                                   WorkQueue* workQueue,
                                   RChannelOpenThrottler* openThrottler);
    ~RChannelBufferReaderNativeFile();

    virtual void FillInStatus(DryadChannelDescription* status);

    bool OpenA(const char* pathName);
//JC    bool OpenW(const wchar_t* pathName);

    void Start(RChannelBufferPrefetchInfo* prefetchCookie,
               RChannelBufferReaderHandler* handler);

    void Drain(RChannelItem* drainItem);

    void Close();

protected:
    void EagerCloseFile();
    bool LazyOpenFile();
    void ResetFileNameAndErrorStateA(const char *fileName);

private:
    void MakeFileMetaData(DryadMetaData* metaData, UInt64 streamOffset);
    RChannelBuffer* MakeFinalBuffer(UInt64 streamOffset);
    void FillInOpenedDetails(ReadHandler* handler);

    ReadHandler* GetNextReadHandler(bool lazyOpenDone);

    UInt32                       m_bufferSize;
    size_t                       m_bufferAlignment;
    UInt64                       m_nextOffsetToRequest;
    HANDLE                       m_fileHandle;
    char*                        m_fileNameA;
    size_t                       m_fileNameLength;
    bool                         m_fileIsPipe;
    wchar_t*                     m_fileNameW;
    bool                         m_wideFileName;
    DrRef<RChannelBuffer>        m_openErrorBuffer;

    friend class FileReadHandler;
};
