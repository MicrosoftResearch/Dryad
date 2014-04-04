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

#pragma once
#pragma warning(disable:4512)

#include <channelinterface.h>
#include <channelbuffer.h>
#include <channelmarshaler.h>
#include <orderedsendlatch.h>
#include <dryadeventcache.h>

class RChannelMarshalRequest;
class RChannelBufferWriter;
class WorkQueue;

class RChannelBufferWriterHandler
{
public:
    virtual ~RChannelBufferWriterHandler();

    virtual void ProcessWriteCompleted(RChannelItemType status) = 0;
};

/*
  The RChannelWriter is the primary mechanism for writing
  application-specific structured items to an underlying byte-oriented
  channel.

  The application first calls Start, then sends a series of items,
  primarily of type RChannelItem_Data, though markers may be
  interspersed, followed by a termination item of type
  RChannelItem_Restart, RChannelItem_Abort or
  RChannelItem_EndOfStream. In the case of a pipe which has not
  broken, the termination item will be sent to the remote end so that
  the consuming process learns the reason for the pipe closure.

  After sending a termination item no further items will be sent on
  the channel until a call to Drain has completed. After this point
  the channel may call Start again in the case of a restart, and start
  sending items again.

  Each item is marshaled into the byte-stream using an
  application-specific marshaler object which must co-operate with the
  application so that bare RChannelItem objects can be cast by it into
  objects containing meaningful data.
 */
class RChannelSerializedWriter :
    public RChannelWriter,
    public RChannelBufferWriterHandler
{
public:
    RChannelSerializedWriter(RChannelBufferWriter* writer,
                             RChannelItemMarshalerBase* marshaler,
                             bool breakBufferOnRecordBoundaries,
                             UInt32 maxMarshalBatchSize,
                             WorkQueue* workQueue);
    ~RChannelSerializedWriter();

    /* a client must call Start before writing items to the channel
       the first time, or after a Drain has completed before writing
       to a restarted channel.
    */
    void Start();

    void WriteItemArray(RChannelItemArrayRef& itemArray,
                        bool flushAfter,
                        RChannelItemArrayWriterHandler* handler);

    RChannelItemType WriteItemArraySync(RChannelItemArrayRef& itemArray,
                                        bool flush,
                                        RChannelItemArrayRef* pFailureArray);

    virtual void Drain(DrTimeInterval timeOut,
                       RChannelItemRef* pRemoteStatus);

    void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                             RChannelItemRef* pReaderDrainItem);

    /* Close may not be called unless Start has never been called or
       Drain has completed since the last call to Start. After Close
       is called the channel may not be restarted. */
    void Close();

    /* this is the implementation of the RChannelBufferWriterHandler
       interface and must not be called by clients. */
    void ProcessWriteCompleted(RChannelItemType status);

    /* Get/Set the URI of the channel. */
    const char* GetURI();
    void SetURI(const char* uri);

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);

private:
    class DummyItemHandler : public RChannelItemArrayWriterHandler
    {
    public:
        void ProcessWriteArrayCompleted(RChannelItemType returnCode,
                                        RChannelItemArray* failureArray);
    };

    class WriteRequest
    {
    public:
        WriteRequest(RChannelItemArray* itemArray,
                     bool flushAfter,
                     RChannelItemArrayWriterHandler* handler);
        ~WriteRequest();

        void SetHandler(RChannelItemArrayWriterHandler* handler);
        bool ShouldFlush();

        RChannelItem* GetNextItem();
        void SetSuccessItem();
        void SetFailureItem(RChannelItem* marshalFailureItem, bool abort);
        bool LastItem();
        bool Completed();

        void ProcessMarshalCompleted(RChannelItemType returnType);

    private:
        RChannelItemArrayRef             m_itemArray;
        bool                             m_flushAfter;
        RChannelItemArrayWriterHandler*  m_handler;
        UInt32                           m_currentItem;
        RChannelItemArrayRef             m_failureArray;
        bool                             m_aborted;
        DrBListEntry                     m_listPtr;
        friend class DryadBList<WriteRequest>;
    };

    typedef DryadBList<WriteRequest> WriteRequestList;

    class SyncHandler : public RChannelItemArrayWriterHandler
    {
    public:
        SyncHandler();
        ~SyncHandler();

        void ProcessWriteArrayCompleted(RChannelItemType returnType,
                                        RChannelItemArray* failureArray);

        RChannelItemType GetStatusCode();
        void GetFailureItemArray(RChannelItemArrayRef* pFailureArray);

        void UseEvent(DryadHandleListEntry* event);
        DryadHandleListEntry* GetEvent();

        bool UsingEvent();
        void Wait();

    private:
        LONG                        m_usingEvent;
        DryadHandleListEntry*       m_event;
        RChannelItemType            m_statusCode;
        RChannelItemArrayRef        m_failureArray;
    };

    enum CWState {
        CW_Closed,
        CW_Empty,
        CW_InWorkQueue,
        CW_Marshaling,
        CW_Blocking,
        CW_Stopping,
        CW_Stopped
    };

    void MakeCachedWriter();
    Size_t DisposeOfCachedWriter();
    void ReturnHandlers(WriteRequestList* completedList,
                        RChannelItemType returnCode);
    bool CheckForTerminationItem(RChannelItemArray* itemArray);
    void RestorePreMarshalBuffers(Size_t preMarshalAvailableSize);
    void CollapseToSingleBuffer();
    void ShuffleBuffersOnRecordBoundaries(Size_t preMarshalAvailableSize);
    RChannelItemType PerformSingleMarshal(WriteRequest* writeRequest);
    bool PerformMarshal(WriteRequestList* pendingRequestList,
                        WriteRequestList* completedRequestList);
    void PostMarshal(WriteRequestList* pendingRequestList,
                     WriteRequestList* completedRequestList,
                     bool shouldBlock,
                     SyncHandler* syncHandler);
    bool SendCompletedBuffers(bool shouldFlush,
                              RChannelItemType terminationType);
    void MarshalItems();
    void AcceptReturningHandlers(UInt32 handlerCount);

    bool                                       m_breakBufferOnRecordBoundaries;
    RChannelBufferWriter*                      m_writer;
    RChannelItemMarshalerRef                   m_marshaler;
    WorkQueue*                                 m_workQueue;

    UInt32                                     m_maxMarshalBatchSize;

    CWState                                    m_state;
    /* the pending list is the writes which have been submitted but
       not yet queued for the marshaler. */
    WriteRequestList                           m_pendingList;
    /* the blocked handler list is the writes which have been
       marshaled and sent to the buffer writer and are waiting for the
       channel to unblock */
    WriteRequestList                           m_blockedHandlerList;
    /* outstandingBuffers is the count of buffers which have been sent
       to the buffer writer and not yet completed */
    UInt32                                     m_outstandingBuffers;
    /* outstandingBuffers is the count of handlers which have been
       received from the client and not yet returned. */
    UInt32                                     m_outstandingHandlers;
    HANDLE                                     m_handlerReturnEvent;
    bool                                       m_marshaledTerminationItem;
    HANDLE                                     m_marshaledLastItemEvent;

    DryadFixedBufferList                       m_bufferList;
    ChannelMemoryBufferWriter*                 m_cachedWriter;
    DryadOrderedSendLatch<WriteRequestList>    m_returnLatch;

    DryadEventCache                            m_eventCache;

    RChannelItemType                           m_channelTermination;
    RChannelItemRef                            m_writerTerminationItem;
    RChannelItemRef                            m_readerTerminationItem;

    DrStr128                                   m_uri;

    CRITSEC                                    m_baseCS;

    friend class RChannelMarshalRequest;
};

class RChannelBufferWriter
{
public:
    virtual ~RChannelBufferWriter();

    virtual void Start() = 0;

    virtual DryadFixedMemoryBuffer* GetNextWriteBuffer() = 0;
    virtual DryadFixedMemoryBuffer* GetCustomWriteBuffer(Size_t bufferSize) = 0;

    virtual bool WriteBuffer(DryadFixedMemoryBuffer* buffer,
                             bool flushAfter,
                             RChannelBufferWriterHandler* handler) = 0;

    virtual void ReturnUnusedBuffer(DryadFixedMemoryBuffer* buffer) = 0;

    virtual void WriteTermination(RChannelItemType reasonCode,
                                  RChannelBufferWriterHandler* handler) = 0;

    virtual void FillInStatus(DryadChannelDescription* status);

    virtual void Drain(RChannelItemRef* pReturnItem) = 0;

    /* shut down the channel. After Close returns no further calls can
       be made to this interface.
    */
    virtual void Close() = 0;

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    virtual UInt64 GetInitialSizeHint() = 0;
    virtual void SetInitialSizeHint(UInt64 hint) = 0;
};

class RChannelNullWriter : public RChannelWriter
{
public:
    RChannelNullWriter(const char* uri);
    void Start();
    void WriteItemArray(RChannelItemArrayRef& itemArray,
                        bool flushAfter,
                        RChannelItemArrayWriterHandler* handler);
    RChannelItemType WriteItemArraySync(RChannelItemArrayRef& itemArray,
                                        bool flush,
                                        RChannelItemArrayRef* pFailureArray);
    void Drain(DrTimeInterval timeOut,
               RChannelItemRef* pRemoteStatus);
    void GetTerminationItems(RChannelItemRef* pWriterDrainItem,
                             RChannelItemRef* pReaderDrainItem);
    void Close();

    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);
    const char* GetURI();

private:
    bool              m_started;
    RChannelItemRef   m_writeTerminationItem;
    DrStr128          m_uri;
    CRITSEC           m_baseCS;
};
