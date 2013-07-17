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

#include "channelbuffer.h"
#include "dobjpool.h"
#include <errorreporter.h>
#include "recordarray.h"

class RChannelRawItemParser;

/* The classes in this header file must be overridden to implement an
   application-specific parser which can take application-specific
   data from a byte-oriented channel and parse it into RChannelItem
   objects.

   At the start of the header is the RChannelRawItemParser class which
   may be overridden directly in the unlikely event that an
   application needs full control over the parsing process. Almost all
   applications will be able to derive from the RChannelItemParser or
   RChannelLengthDelimitedItemParser classes which follow.
   RChannelLengthDelimitedItemParser is somewhat simpler to use in the
   case that the length of items in the channel can be determined by
   reading a prefix of the item.

   Assuming that a parser object is only passed to a single
   RChannelReader object, no method calls on the parser will ever be
   overlapped. Methods are called on worker threads and should compute
   for as long as necessary before returning. The worker threads are
   used for item parsing, item marshaling, item processing and other
   actions.
*/

/* RChannelRawItemParser is the raw interface for unmarshaling
   structured items from a byte-stream, however the convenience
   classes below will be more suitable for most applications. */
class RChannelItemParserBase : public IDrRefCounter
{
public:
    RChannelItemParserBase();
    virtual ~RChannelItemParserBase();

    void SetMaxParseBatchSize(UInt32 maxParseBatchSize);
    UInt32 GetMaxParseBatchSize();

    void SetParserContext(RChannelContext* context);
    RChannelContext* GetParserContext();

    /* this indicates, e.g., which channel on a vertex the parser is
       attached to. It may not be unique in a given process if there
       are e.g. multiple vertices. */
    void SetParserIndex(UInt32 index);
    UInt32 GetParserIndex();

    /* The Item Parser takes a sequence of data buffers from the
       underlying Channel and returns a sequence of items parsed out
       of the data.

       A single method call, RawParseItem, implements the API between
       the Channel and the parser. In the common case, when
       RawParseItem is called the parser returns the next item from
       whatever buffer data it has cached: it must maintain enough
       state to be able to keep track of which the next item is.

       If the parser does not have enough cached data to return the
       next item (e.g. an item straddles two or more data buffers) it
       signals this on return from the method, and the next call to
       RawParseItem will supply the next buffer in sequence.

       The buffers are supplied as a sequence of RChannelBuffer_Data
       and RChannelBuffer_Hole buffers followed by a single
       RChannelBuffer_EndOfStream though there may be a restart at any
       time. The returned items are of type RChannelItem_Data,
       RChannelItem_ItemHole, RChannelItem_BufferHole,
       RChannelItem_EndOfStream or RChannelItem_ParseError and between
       restarts they obey the pattern sequence

         (Data | ItemHole | BufferHole)* (EndOfStream | ParseError)

       The detailed method description follows:

       The code calling the parser behaves as though it stores a
       parserState variable which can take on the values NeedsData,
       DataReady and Stopped; this variable is updated after every
       call to RawParseItem. The rules for calling ItemParser are as
       follows:

         1) At any time RawParseItem may be called with
         resetParser=true. This signifies that the entire channel has
         been restarted. In this case inData contains a pointer to a
         buffer and the parser should discard all saved state and
         restart parsing from the beginning of inData.

         2) Otherwise resetParser=false and the call to RawParseItem
         depends on the value of parserState:

           2a) parserState = NeedsData: *inData contains the next buffer
           in sequence
           2b) parserState = DataReady: inData=NULL
           2c) parserState = Stopped: RawParseItem will never be called

       RawParseItem must set *outPrefetchCookie before returning.

       If RawParseItem returns NULL, then parserState is set to
       NeedsData and *outPrefetchCookie is delivered to the buffer
       reader when the current buffer (the most recent to have been
       passed to RawParseItem) is returned. For now *outPrefetchCookie
       should always be NULL. The return value may not be NULL if the
       current buffer is of type RChannelBuffer_EndOfStream.

       Otherwise *outPrefetchCookie must be NULL and the returned item
       must be of type RChannelItem_Data, RChannelItem_ItemHole,
       RChannelItem_BufferHole, RChannelItem_ParseError or
       RChannelItem_EndOfStream.

       The type of the returned item should be as follows:

         RChannelItem_Data: the item is the next item successfully
         parsed from the stream.

         RChannelItem_ItemHole: the parser encountered malformed data
         but believes it has successfully skipped over it and
         resynchronized on the start of the next well-formed item. The
         returned item contains metadata describing the error and
         e.g. the amount of data skipped.

         RChannelItem_BufferHole: the parser has been given a buffer
         of type RChannelBuffer_Hole. After it has returned any
         partial data in the preceding buffers as RChannelItem_Data or
         RChannelItem_ItemHole items, it will return the
         RChannelItem_BufferHole which is stored in the
         RChannelBuffer_Hole object. This contains the buffer
         provider's explanation for the hole.

         RChannelItem_EndOfStream: all the data in the stream has been
         consumed. The returned item may only have type
         RChannelItem_EndOfStream if the current buffer has type
         RChannelBuffer_EndOfStream.

         RChannelItem_ParseError: the parser encountered malformed
         data, in unable to recover, and will not be able to return
         any more items from the stream. The returned item contains
         metadata describing the error.

       If the returned item has type RChannelItem_Data,
       RChannelItem_BufferHole or RChannelItem_ItemHole then
       parserState is set to DataReady before the next call to
       RawParseItem, otherwise parserState is set to Stopped.
    */
    virtual RChannelItem* RawParseItem(bool restartParser,
                                       RChannelBuffer* inData,
                                       RChannelBufferPrefetchInfo**
                                       outPrefetchCookie /* out */) = 0;

private:
    UInt32                       m_maxParseBatchSize;
    UInt32                       m_index;
    RChannelContextRef           m_context;
};

typedef DrRef<RChannelItemParserBase> RChannelItemParserRef;

class RChannelRawItemParser : public RChannelItemParserBase
{
public:
    virtual ~RChannelRawItemParser();
    DRREFCOUNTIMPL
};

class RChannelItemTransformerBase : public IDrRefCounter
{
public:
    virtual ~RChannelItemTransformerBase();

    /* this is called once before the first use of the other
       transformer methods. The default implementation does
       nothing. Initialize can retrieve the parser context, etc. from
       the base class. */
    virtual void InitializeTransformer(RChannelItemParserBase* parent,
                                       DVErrorReporter* errorReporter);

    /* this is called once for each item in the input. It should write
       zero or more transformed items to writer. The default
       implementation writes each input directly back to writer. */
    virtual void TransformItem(RChannelItemRef& inputItem,
                               SyncItemWriterBase* writer,
                               DVErrorReporter* errorReporter);

    /* this is called when the input is exhausted. In some cases,
       input may be broken up into fragments (e.g. a cosmos stream
       with missing extents). In this case, Flush will be called at
       the end of each fragment, and will be followed by another
       sequence of Map calls to process the next fragment, followed by
       another Flush, until the end of the input is reached. The
       default implementation does nothing. */
    virtual void FlushTransformer(SyncItemWriterBase* writer,
                                  DVErrorReporter* errorReporter);

    /* this is called before Flush if the input encounters an error,
       with the item describing the error (e.g. a corrupt extent in a
       cosmos stream). The default implementation does nothing. */
    virtual void ReportTransformerErrorItem(RChannelItemRef& errorItem,
                                            DVErrorReporter* errorReporter);
};

typedef DrRef<RChannelItemTransformerBase> RChannelItemTransformerRef;

class RChannelItemTransformer : public RChannelItemTransformerBase
{
public:
    virtual ~RChannelItemTransformer();

    DRREFCOUNTIMPL
};


/* this record contains a single buffer. */
class RChannelBufferRecord
{
public:
    void SetData(DrMemoryBuffer* data);
    DrMemoryBuffer* GetData() const;

private:
    DrRef<DrMemoryBuffer>  m_buffer;
};

/* this item contains a single buffer, however it is also a record
   array that contains a single record holding the same buffer. That
   way it can be used in both item-reading and record-reading
   interfaces. */
class RChannelBufferItem : public PackedRecordArray<RChannelBufferRecord>
{
public:
    static RChannelBufferItem* Create(DrMemoryBuffer* buffer);
    DrMemoryBuffer* GetData() const;
    virtual UInt64 GetItemSize() const;

private:
    RChannelBufferItem(DrMemoryBuffer* buffer);
};

class RChannelTransformerParserBase :
    public RChannelItemParserBase,
    public DVErrorReporter,
    public SyncItemWriterBase
{
public:
    RChannelTransformerParserBase();
    virtual ~RChannelTransformerParserBase();

    void SetTransformer(RChannelItemTransformerBase* transformer);
    RChannelItemTransformerBase* GetTransformer();

    /* the implementation of the base parser interface */
    RChannelItem* RawParseItem(bool restartParser,
                               RChannelBuffer* inData,
                               RChannelBufferPrefetchInfo**
                               outPrefetchCookie /* out */);

    /* the implementation of the item writer interface */
    void WriteItemSyncConsumingReference(RChannelItemRef& item);
    DrError GetWriterStatus();

private:
    RChannelItemTransformerRef        m_transformer;
    bool                              m_transformedAny;
    RChannelItemList                  m_itemList;
};

class RChannelTransformerParser : public RChannelTransformerParserBase
{
    DRREFCOUNTIMPL
};

class RChannelItemParserNoRefImpl : public RChannelItemParserBase
{
public:
    RChannelItemParserNoRefImpl();
    virtual ~RChannelItemParserNoRefImpl();

    /* ResetParser is called whenever the underlying channel restarts,
       and before the first buffer is passed to the parser. On
       receiving a call to ResetParser the parser should discard all
       internal state and return to the start-of-stream ready
       condition. */
    virtual void ResetParser();

    /* ParseNextItem is called whenever another item is needed, and
       the item should be parsed from the buffers in bufferList,
       starting at startOffset in the first buffer in the list. The
       list will never be empty. It may be convenient to wrap the
       bufferList in a DrMemoryBuffer using the RChannelReaderBuffer
       class in channelmemorybuffers.h, e.g.
         wrapper = new RChannelReaderBuffer(bufferList,
                                            startOffset,
                                            bufferList->back()->GetData()->
                                            GetAvailableSize());

       If there is not enough data to parse the next item,
       ParseNextItem should return NULL. On the next call the parser
       is guaranteed to receive a longer list of buffers starting from
       the same point in the stream (with bufferList as a prefix)
       unless ResetParser or ParsePartialItem has been called, so it
       can cache information about partially parsed items if desired.

       If an item is successfully parsed from availableData it should
       be returned with type RChannelItem_Data, and the number of
       bytes consumed should be stored in *pOutLength which must be <=
       the total available size of the buffers in bufferList -
       startOffset. The parser is allowed to behave as if the
       application now "owns" this prefix of the available data,
       e.g. it can increase the refcounts of buffers in bufferList,
       store them inside the returned item, and modify the data within
       this prefix.

       If a parse error occurs but the parser can resynchronize at the
       start of the next item, ParseNextItem should return an item of
       type RChannelItem_ItemHole describing the error and set
       *pOutLength to the number of bytes to be skipped before the
       start of the next item.

       If a parse error occurs and the parser cannot determine the
       start of the next item, ParseNextItem should return an item of
       type RChannelItem_ParseError describing the error. In this case
       ResetParser will be called before the next call to
       ParseNextItem or ParsePartialItem.
    */
    virtual RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                        Size_t startOffset,
                                        Size_t* pOutLength) = 0;

    /* ParsePartialItem is called whenever a buffer of type
       RChannelBuffer_Hole or RChannelBuffer_EndOfStream arrives on
       the channel and there is unparsed data remaining before this
       marker buffer. The remaining data is passed as bufferList and
       the buffer is passed as markerBuffer. bufferList may be empty.

       If there is no useful data remaining in bufferList,
       ParsePartialItem should return NULL. If the data in bufferList
       can be interpreted as a valid item, this item should be
       returned as an item of type RChannelItem_Data. If the data is
       malformed, ParsePartialItem should return an item of type
       RChannelItem_ItemHole describing the malformed data.

       After a call to ParsePartialItem, the parser should probably
       not be holding on to any state. If the marker was a buffer
       hole, the next call to ParseNextItem will be called with
       bufferList starting after the hole in the stream. If the
       marker was an end of stream buffer, ResetParser will be called
       before the next call to ParseNextItem or ParsePartialItem.
    */
    virtual RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                           Size_t startOffset,
                                           RChannelBufferMarker*
                                           markerBuffer);



    /* this implements the RChannelRawItemParser interface */
    RChannelItem* RawParseItem(bool restartParser,
                               RChannelBuffer* inData,
                               RChannelBufferPrefetchInfo** outPrefetchCookie);

protected:
    void ResetParserInternal();
    void DiscardBufferPrefix(Size_t discardLength);
    RChannelItem* DealWithPartialBuffer(RChannelBufferMarker* mBuffer);

private:
    bool                    m_needsReset;
    bool                    m_needsData;
    RChannelItemRef         m_savedItem;

    ChannelDataBufferList   m_bufferList;

    Size_t                  m_bufferListStartOffset;
};

class RChannelItemParser : public RChannelItemParserNoRefImpl
{
public:
    virtual ~RChannelItemParser();
    DRREFCOUNTIMPL
};


class RChannelStdItemParserNoRefImpl : public RChannelItemParserNoRefImpl
{
public:
    RChannelStdItemParserNoRefImpl(DObjFactoryBase* factory);
    virtual ~RChannelStdItemParserNoRefImpl();

    void ResetParser();

    RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength);
    RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                   Size_t startOffset,
                                   RChannelBufferMarker*
                                   markerBuffer);

private:
    DObjFactoryBase*    m_factory;
    RChannelItemRef     m_pendingErrorItem;
};

class RChannelStdItemParser : public RChannelStdItemParserNoRefImpl
{
public:
    RChannelStdItemParser(DObjFactoryBase* factory);
    virtual ~RChannelStdItemParser();
    DRREFCOUNTIMPL
};


/* many items are stored with an explicit length available near the
   start of the item. The RChannelParserExplicitLength convenience
   class is the simplest way to write a parser to deal with such
   items.
*/
class RChannelLengthDelimitedItemParserNoRefImpl :
    public RChannelItemParserNoRefImpl
{
public:
    enum LengthStatus {
        LS_ParseError,
        LS_NeedsData,
        LS_Ok
    };

    RChannelLengthDelimitedItemParserNoRefImpl();
    virtual ~RChannelLengthDelimitedItemParserNoRefImpl();

    /* GetNextItemLength should attempt to read the length of the next
       item from availableData, assuming the next item starts at the
       beginning of availableData. availableData is not growable.

       If buffer does not contain enough data to read out the next
       item length, GetNextItemLength should return LS_NeedsData and
       *pErrorItem should be set to NULL.

       If the prefix of buffer is malformed (does not contain a valid
       prefix of an item), GetNextItemLength should return
       LS_ParseError and *pErrorItem should contain an item of type
       RChannelItem_ParseError describing the error.

       Otherwise, GetNextItemLength should read the item length from
       buffer, store it in *pOutLength, return LS_Ok and set
       *pErrorItem to NULL.
    */
    virtual LengthStatus GetNextItemLength(DrMemoryBuffer* availableData,
                                           Size_t* pOutLength,
                                           RChannelItem** pErrorItem) = 0;

    /* ParseItemWithLength is called after a successful call to
       GetNextItemLength, and after cached data of the required length
       has been accumulated from the channel. itemData is not growable
       and has its AllocatedSize and AvailableSize set to itemLength,
       which is the length returned by the previous call to
       GetNextItemLength.

       ParseItemWithLength should parse itemData into an RChannelItem
       object of type RChannelItem_Data and return it. The parser is
       allowed to behave as if the application now "owns" itemData,
       e.g. it can increase itemData's refcount and store it inside
       the returned item, or modify the data within itemData.

       If a parse error occurs, ParseItemWithLength should return an
       item of type RChannelItem_ItemHole describing the error.

       The RChannelLengthDelimitedItemParser class will automatically
       add entries to the returned item's metadata indicating what
       range of the underlying channel the item was generated from.
     */
    virtual RChannelItem* ParseItemWithLength(DrMemoryBuffer* itemData,
                                              Size_t itemLength) = 0;

    /* these implement the RChannelItemParser interface */
    void ResetParser();
    RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength);
    RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                   Size_t startOffset,
                                   RChannelBufferMarker*
                                   markerBuffer);

private:
    void AddMetaData(RChannelItem* item,
                     ChannelDataBufferList* bufferList,
                     Size_t startOffset,
                     Size_t endOffset);
    RChannelItem* FetchItem(ChannelDataBufferList* bufferList,
                            Size_t startOffset,
                            Size_t tailBufferSize);
    RChannelItem* MaybeFetchItem(ChannelDataBufferList* bufferList,
                                 Size_t startOffset,
                                 Size_t* pOutLength);

    Size_t  m_itemLength;
    Size_t  m_accumulatedLength;
};

class RChannelLengthDelimitedItemParser :
    public RChannelLengthDelimitedItemParserNoRefImpl
{
public:
    virtual ~RChannelLengthDelimitedItemParser();
    DRREFCOUNTIMPL
};

class DryadParserFactoryBase : public IDrRefCounter
{
public:
    virtual ~DryadParserFactoryBase();
    virtual void MakeParser(RChannelItemParserRef* pParser,
                            DVErrorReporter* errorReporter) = 0;
};


typedef DrRef<DryadParserFactoryBase> DryadParserFactoryRef;

class DryadParserFactory : public DryadParserFactoryBase
{
public:
    virtual ~DryadParserFactory();
    DRREFCOUNTIMPL
};

template<class _T> class StdParserFactory : public DryadParserFactory
{
public:
    typedef _T Parser;
    void MakeParser(RChannelItemParserRef* pParser,
                    DVErrorReporter* errorReporter)
    {
        pParser->Attach(new Parser());
    }
};

