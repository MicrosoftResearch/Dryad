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
#include "channelwriter.h"
#include "concreterchannelhelpers.h"
#ifdef TIDYFS
#include <mdclient.h>
#endif
#include "DrFPrint.h"

#pragma warning(disable:4995)
#include <set>

class RChannelBufferWriterNative :
    public RChannelBufferWriter, public RChannelThrottledStream
{
public:
    class WriteHandler : public DryadNativePort::Handler, public DrRefCounter
    {
    public:
        WriteHandler(DryadFixedMemoryBuffer* block,
                     UInt64 streamOffset,
                     bool flushAfter,
                     RChannelBufferWriterHandler* handler,
                     RChannelBufferWriterNative* parent);
        ~WriteHandler();

        void ProcessingComplete(RChannelItemType statusCode);

        UInt32 GetWriteLength();
        UInt64 GetStreamOffset();
        void* GetData();
        DryadFixedMemoryBuffer* GetBlock();
        bool IsFlush();

        virtual void QueueWrite(DryadNativePort* port) = 0;

    protected:
        RChannelBufferWriterNative* GetParent();

    private:
        DryadFixedMemoryBuffer*            m_block;
        UInt32                             m_writeLength;
        bool                               m_flush;
        RChannelBufferWriterHandler*       m_handler;
        RChannelBufferWriterNative*        m_parent;
        UInt64                             m_streamOffset;
        void*                              m_data;
        DrBListEntry                       m_listPtr;
        friend class DryadBList<WriteHandler>;
    };

    typedef DryadBList<WriteHandler> WriteHandlerList;

    RChannelBufferWriterNative(UInt32 outstandingWritesLowWatermark,
                               UInt32 outstandingWritesHighWatermark,
                               DryadNativePort* port,
                               RChannelOpenThrottler* openThrottler,
                               bool supportsLazyOpen);
    ~RChannelBufferWriterNative();

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

    bool EnsureOpenForWrite(WriteHandler* handler);

    void Drain(RChannelItemRef* returnItem);

    void Close();

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);

    void OpenAfterThrottle();

protected:
    CRITSEC* GetBaseDR();
    DryadNativePort* GetPort();
    void OpenInternal();
    void ReceiveBufferInternal(WriteHandler* writeHandler, DrError errorCode);
    void DecrementOutstandingHandlers();
    bool FinishUsingFile();
    /* called with baseDR held */
    void SetProcessedLength(UInt64 processedLength);
    void SetLowAndHighWaterMark(UInt32 outstandingWritesLowWatermark,
                                UInt32 outstandingWritesHighWatermark);
    void SetOpenErrorItem(RChannelItem* errorItem);
    bool OpenError();

private:
    enum State {
        S_Closed,
        S_Running,
        S_Stopping,
        S_Stopped
    };

    enum OpenState {
        OS_Closed,
        OS_NotOpened,
        OS_Waiting,
        OS_Opened,
        OS_OpenError,
        OS_Stopped
    };

    typedef std::set<WriteHandler *> WriteHandlerSet;

    virtual bool LazyOpenFile();
    virtual void FillInOpenedDetails(WriteHandler* handler);
    virtual void EagerCloseFile();
    virtual void FinalCloseFile();
    virtual DryadFixedMemoryBuffer* GetNextWriteBufferInternal() = 0;
    virtual DryadFixedMemoryBuffer*
        GetCustomWriteBufferInternal(Size_t bufferSize) = 0;
    virtual void ReturnUnusedBufferInternal(DryadFixedMemoryBuffer*
                                            buffer) = 0;
    virtual WriteHandler* MakeWriteHandler(DryadFixedMemoryBuffer* block,
                                           bool flushAfter,
                                           RChannelBufferWriterHandler*
                                           handler,
                                           bool detailsPresent,
                                           bool* extendFile) = 0;
    virtual void ExtendFileValidLength() = 0;
    virtual void StartConcreteWriter(RChannelItemRef* pCompletionItem) = 0;
    virtual void DrainConcreteWriter() = 0;

    bool AddToWriteQueue(WriteHandler* writeRequest,
                         WriteHandlerList* writeQueue,
                         bool extendFile);
    void Unblock(WriteHandlerList* returnList,
                 WriteHandlerList* processList);
    void ConsumeErrorBuffer(DrError errorCode,
                            WriteHandlerList* pReturnList,
                            WriteHandlerList* pProcessList);
    RChannelItemType ConsumeBufferCompletion(WriteHandler* writeHandler,
                                             DrError errorCode,
                                             WriteHandlerList* pReturnList,
                                             WriteHandlerList* pProcessList);
    RChannelItemType DealWithReturnHandlers(WriteHandler* writeHandler,
                                            DrError errorCode);

    UInt32                       m_lowWatermark;
    UInt32                       m_highWatermark;

    UInt32                       m_outstandingBuffers;
    UInt32                       m_outstandingIOs;
    UInt32                       m_outstandingHandlerSends;
    bool                         m_outstandingTermination;
    bool                         m_blocking;
    bool                         m_flushing;
    bool                         m_blockingForFileExtension;
    WriteHandler*                m_terminationHandler;
    WriteHandlerList             m_blockedList;
    WriteHandlerSet              m_pendingWriteSet;
    WriteHandlerSet              m_pendingUnblockSet;
    RChannelItemRef              m_completionItem;
    RChannelItemRef              m_openErrorItem;
    WriteHandlerList             m_openWaitingList;
    RChannelOpenThrottler*       m_openThrottler;

    UInt64                       m_processedLength;
    UInt64                       m_initialSizeHint;

    HANDLE                       m_terminationEvent;

    DryadNativePort*             m_port;

    OpenState                    m_openState;
    State                        m_state;
    bool                         m_supportsLazyOpen;
    bool                         m_drainingOpenQueue;

    CRITSEC                      m_baseCS;
};

class RChannelBufferWriterNativeFile : public RChannelBufferWriterNative
{
public:
    class FileWriteHandler :
        public RChannelBufferWriterNative::WriteHandler
    {
    public:
        FileWriteHandler(HANDLE fileHandle,
                         bool detailsPresent,
                         bool isBuffered,
                         DryadFixedMemoryBuffer* block,
                         UInt64 streamOffset,
                         bool flushAfter,
                         RChannelBufferWriterHandler* handler,
                         RChannelBufferWriterNativeFile* parent);

        void SetFileHandle(HANDLE h);
        HANDLE GetFileHandle();
        bool IsBuffered();

        void ProcessIO(DrError errorCode, UInt32 numBytes);

        void QueueWrite(DryadNativePort* port);

    private:
        HANDLE                             m_fileHandle;
        bool                               m_detailsPresent;
        bool                               m_isBuffered;
    };

    RChannelBufferWriterNativeFile(UInt32 bufferSize,
                                   size_t bufferAlignment,
                                   UInt32 outstandingWritesLowWatermark,
                                   UInt32 outstandingWritesHighWatermark,
                                   DryadNativePort* port,
                                   RChannelOpenThrottler* openThrottler);
    ~RChannelBufferWriterNativeFile();

    DrError SetMetaData(DryadMetaData* metaData);

    bool OpenA(const char* pathName);
//JC    bool OpenW(const wchar_t* pathName);

protected:
    UInt64                       m_fpDataLength;
    bool                         m_calcFP;
    Dryad_dupelim_fprint_data_t    m_fpo;
    Dryad_dupelim_fprint_uint64_t  m_fp;

    
private:
    DrError TryToCreatePathA();
//JC    DrError TryToCreatePathW();
    HANDLE CreateFileAndPath(DrError* pErr, SECURITY_ATTRIBUTES *sa);
    bool LazyOpenFile();
    void FillInOpenedDetails(WriteHandler* handler);
    void EagerCloseFile();
    DryadFixedMemoryBuffer* GetNextWriteBufferInternal();
    DryadFixedMemoryBuffer* GetCustomWriteBufferInternal(Size_t bufferSize);
    void ReturnUnusedBufferInternal(DryadFixedMemoryBuffer* buffer);
    WriteHandler* MakeWriteHandler(DryadFixedMemoryBuffer* block,
                                   bool flushAfter,
                                   RChannelBufferWriterHandler*
                                   handler,
                                   bool detailsPresent,
                                   bool* extendFile);
    void StartConcreteWriter(RChannelItemRef* pCompletionItem);
    void DrainConcreteWriter();
    void ExtendFileValidLength();

    bool IsAligned(UInt64 offset);
    UInt32 AlignmentGap(UInt64 offset);
    void ReceiveBuffer(WriteHandler* writeHandler, DrError errorCode);

    static bool TryToSetPrivilege();

    UInt32                       m_bufferSize;
    size_t                       m_bufferAlignment;

    HANDLE                       m_rawFileHandle;
    HANDLE                       m_bufferedFileHandle;

    bool                         m_fileIsPipe;

    UInt64                       m_nextOffsetToWrite;
    UInt32                       m_realignmentSize;

    char*                        m_fileNameA;
//JC    wchar_t*                     m_fileNameW;
    size_t                       m_fileNameLength;
//JC    bool                         m_wideFileName;

    bool                         m_tryToCreatePath;
    bool                         m_canExtendFileLength;
    UInt64                       m_fileLengthSet;

    static bool                  s_triedToSetPrivilege;
    static bool                  s_setPrivilege;
    static CRITSEC               s_privilegeDR;

    friend class FileWriteHandler;
};

#ifdef TIDYFS
class RChannelBufferWriterNativeTidyFSStream : public RChannelBufferWriterNativeFile
{
public:
    RChannelBufferWriterNativeTidyFSStream(UInt32 bufferSize,
                                           size_t bufferAlignment,
                                           UInt32 outstandingWritesLowWatermark,
                                           UInt32 outstandingWritesHighWatermark,
                                           DryadNativePort* port,
                                           RChannelOpenThrottler* openThrottler);
    ~RChannelBufferWriterNativeTidyFSStream();
    DrError OpenA(const char* streamName, DryadMetaData *metaData);
private:
    void Close();
    MDClient *m_client;
    UInt64 m_partId;
    char *m_hostname;

};
#endif
