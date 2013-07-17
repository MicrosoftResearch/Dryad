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

#include <DrCommon.h>
#include <channelinterface.h>
#include <channelbuffer.h>
#include <errorreporter.h>

/* The RChannelItemMarshaler is used to supply application-specifc
   marshaling code to convert an RChannelItem object to a byte stream.

   It is expected that most marshalers will be stateless, but the
   marshaler is called sequentially with each item in turn, so it is
   possible to implement marshalers with state if desired.

   If an RChannelItemMarshaler object is passed to only one
   RChannelWriter object then calls to its methods will never be
   overlapped. Methods are called on worker threads and should compute
   for as long as necessary before returning. The worker threads are
   used for item parsing, item marshaling, item processing and other
   actions.
 */
class RChannelItemMarshalerBase : public IDrRefCounter
{
public:
    RChannelItemMarshalerBase();
    virtual ~RChannelItemMarshalerBase();

    void SetMaxMarshalBatchSize(UInt32 maxMarshalBatchSize);
    UInt32 GetMaxMarshalBatchSize();

    void SetMarshalerContext(RChannelContext* context);
    RChannelContext* GetMarshalerContext();

    /* this indicates, e.g., which channel on a vertex the marshaler
       is attached to. It may not be unique in a given process if
       there are e.g. multiple vertices. */
    void SetMarshalerIndex(UInt32 index);
    UInt32 GetMarshalerIndex();

    /* Reset is called before any items are marshaled to a stream. If
       a channel is restarted, Reset will be called before starting to
       write items again. */
    virtual void Reset();

    /* MarshalItem is called whenever a new item should be written to
       the underlying stream. The item should be marshaled using the
       DrMemoryWriter writer in such a way that a matching
       RChannelItemParser object can unmarshal it subsequently.

       If the marshaler succeeds in writing the item, it should return
       DrError_OK and *pFailureItem will be ignored. It is legal to
       write no data for some items or item types if the application
       semantics permit this.

       If the marshaler encounters an error when parsing it should
       attempt to recover and return a descriptive item of type
       RChannelItem_MarshalError in *pFailureItem rather than, for
       example, asserting. In this case, any data written to writer
       will be discarded, and self will immediately be called back
       with the RChannelItem_MarshalError item. Any failure item
       returned by this second call will be ignored.

       If MarshalItem returns DryadError_ChannelAbort or
       DryadError_ChannelRestart then the appropriate termination item
       will be sent to the channel preventing subsequent items from
       being marshaled. Any other error code will cause the channel to
       continue marshaling items past the error.

       flush is set if the channel will be flushed after this item is
       marshaled. This can be ignored by most marshalers.
    */
    virtual DrError MarshalItem(ChannelMemoryBufferWriter* writer,
                                RChannelItem* item,
                                bool flush,
                                RChannelItemRef* pFailureItem) = 0;

private:
    UInt32                       m_maxMarshalBatchSize;
    UInt32                       m_index;
    RChannelContextRef           m_context;
};

typedef DrRef<RChannelItemMarshalerBase> RChannelItemMarshalerRef;

class RChannelItemMarshaler : public RChannelItemMarshalerBase
{
public:
    ~RChannelItemMarshaler();
    DRREFCOUNTIMPL
};

class RChannelStdItemMarshalerBase : public RChannelItemMarshalerBase
{
public:
    virtual ~RChannelStdItemMarshalerBase();

    DrError MarshalItem(ChannelMemoryBufferWriter* writer,
                        RChannelItem* item,
                        bool flush,
                        RChannelItemRef* pFailureItem);

    virtual DrError MarshalMarker(ChannelMemoryBufferWriter* writer,
                                  RChannelItem* item,
                                  bool flush,
                                  RChannelItemRef* pFailureItem);
};

class RChannelStdItemMarshaler : public RChannelStdItemMarshalerBase
{
public:
    virtual ~RChannelStdItemMarshaler();
    DRREFCOUNTIMPL
};

class DryadMarshalerFactoryBase : public IDrRefCounter
{
public:
    virtual ~DryadMarshalerFactoryBase();
    virtual void MakeMarshaler(RChannelItemMarshalerRef* pMarshaler,
                               DVErrorReporter* errorReporter) = 0;
};

typedef DrRef<DryadMarshalerFactoryBase> DryadMarshalerFactoryRef;

class DryadMarshalerFactory : public DryadMarshalerFactoryBase
{
public:
    virtual ~DryadMarshalerFactory();
    DRREFCOUNTIMPL
};

template<class _T> class StdMarshalerFactory : public DryadMarshalerFactory
{
public:
    typedef _T Marshaler;
    void MakeMarshaler(RChannelItemMarshalerRef* pMarshaler,
                       DVErrorReporter* errorReporter)
    {
        pMarshaler->Attach(new Marshaler());
    }
};
