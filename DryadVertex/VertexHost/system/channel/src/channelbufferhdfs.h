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

#include "channelreader.h"
#include "channelwriter.h"

#include <HdfsBridgeNative.h>

using namespace Microsoft::Research::Peloponnese;

class RChannelBufferHdfsReader
    : public RChannelBufferReader, public RChannelBufferDefaultHandler
{
public:
    static const char* s_hdfsPartitionPrefix;
    static const char* s_wasbPartitionPrefix;

    RChannelBufferHdfsReader(const char* uri);
    virtual ~RChannelBufferHdfsReader();

    void Start(RChannelBufferPrefetchInfo* prefetchCookie,
               RChannelBufferReaderHandler* handler);

    void Interrupt();

    void FillInStatus(DryadChannelDescription* status);

    void Drain(RChannelItem* drainItem);

    void Close();

    bool GetTotalLength(UInt64* pLen);

    /* the RChannelBufferDefaultHandler interface */
    void ReturnBuffer(RChannelBuffer* buffer);

private:
    virtual Int64 ScanForSync(Hdfs::Reader* reader,
                              const char* fileName,
                              Int64 startOffset, Int64 endOffset,
                              RChannelBuffer** pErrorBuffer) = 0;

    static unsigned __stdcall ThreadFunc(void* a);
    void SendBuffer(RChannelBuffer* buffer, bool getSemaphore);
    Int64 AdjustStartOffset(Hdfs::Reader* reader,
                            const char* fileName,
                            Int64 startOffset,
                            Int64 endOffset);
    Int64 AdjustEndOffset(Hdfs::Reader* reader,
                          const char* fileName,
                          Int64 endOffset);
    Int64 ReadDataBuffer(Hdfs::ReaderAccessor& ra,
                         const char* fileName,
                         Int64 offset,
                         Int64 endOffset);
    void ReadThread();

    DrStr64                        m_uri;
    RChannelBufferReaderHandler*   m_handler;
    HANDLE                         m_readThread;
    HANDLE                         m_blockSemaphore;
    HANDLE                         m_abortHandle;

    UInt64                         m_totalLength;
    UInt64                         m_processedLength;
    UInt32                         m_buffersOut;
    CRITSEC                        m_cs;
};

class RChannelBufferHdfsReaderLineRecord : public RChannelBufferHdfsReader
{
public:
    RChannelBufferHdfsReaderLineRecord(const char* uri);

private:
    Int64 ScanForSync(Hdfs::Reader* reader,
                      const char* fileName,
                      Int64 startOffset, Int64 endOffset,
                      RChannelBuffer** pErrorBuffer);
};


class RChannelBufferHdfsWriter : public RChannelBufferWriter
{
public:
    static const char* s_hdfsFilePrefix;
    static const char* s_wasbFilePrefix;

    RChannelBufferHdfsWriter(const char* uri);

    DryadFixedMemoryBuffer* GetNextWriteBuffer();
    DryadFixedMemoryBuffer* GetCustomWriteBuffer(Size_t bufferSize);

    void Start();

    bool WriteBuffer(DryadFixedMemoryBuffer* buffer,
                     bool flushAfter,
                     RChannelBufferWriterHandler* handler);

    void ReturnUnusedBuffer(DryadFixedMemoryBuffer* buffer);

    void WriteTermination(RChannelItemType reasonCode,
                          RChannelBufferWriterHandler* handler);

    void FillInStatus(DryadChannelDescription* status);

    void Drain(RChannelItemRef* pReturnItem);

    void Close();

    /* Get/set a hint about the total length the channel is expected
       to be. Some channel implementations can use this to improve
       write performance and decrease disk fragmentation. A value of 0
       (the default) means that the size is unknown. */
    UInt64 GetInitialSizeHint();
    void SetInitialSizeHint(UInt64 hint);

private:
    struct WriteEntry
    {
        DrRef<DryadFixedMemoryBuffer>  m_buffer;
        bool                           m_flush;
        RChannelItemType               m_type;
        RChannelBufferWriterHandler*   m_handler;
        DrBListEntry                   m_listPtr;
    };

    static unsigned __stdcall ThreadFunc(void* arg);
    void WriteThread();
    bool Open(Hdfs::Instance** pInstance, Hdfs::Writer** pWriter);
    bool AddToQueue(WriteEntry* entry);

    DrStr64                        m_uri;
    DryadBList<WriteEntry>         m_queue;
    UInt32                         m_queueLength;
    HANDLE                         m_queueHandle;
    HANDLE                         m_writeThread;
    RChannelItemRef                m_completionItem;
    UInt64                         m_processedLength;
    CRITSEC                        m_cs;
};
