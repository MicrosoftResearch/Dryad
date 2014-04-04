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

#include "DrCommon.h"
#include <channelinterface.h>
#include <channelparser.h>
#include <channelmarshaler.h>

class DataBlockItem : public RChannelDataItem
{
public: 
    DataBlockItem(DrMemoryBuffer* buf);
    DataBlockItem(Size_t size);
    ~DataBlockItem();
    Size_t GetAllocatedSize();
    Size_t GetAvailableSize();
    void SetAvailableSize(Size_t size);
    void * GetDataAddress();
    DrMemoryBuffer * GetData();

    virtual UInt64 GetItemSize() const;

private:
    DrRef<DrMemoryBuffer> m_buf;  
};

class DataBlockParser : public RChannelItemParser
{
public:
    DataBlockParser() {};

    DataBlockParser(DObjFactoryBase* factory);

    RChannelItem* ParseNextItem(ChannelDataBufferList* bufferList,
                                Size_t startOffset,
                                Size_t* pOutLength);
    RChannelItem* ParsePartialItem(ChannelDataBufferList* bufferList,
                                   Size_t startOffset,
                                   RChannelBufferMarker*
                                   markerBuffer);

};

typedef StdParserFactory<DataBlockParser> DataBlockParserFactory;

class DataBlockMarshaler : public RChannelItemMarshaler
{
public:
    DrError MarshalItem(ChannelMemoryBufferWriter* writer,
                        RChannelItem* item,
                        bool flush,
                        RChannelItemRef* pFailureItem);
};
typedef StdMarshalerFactory<DataBlockMarshaler> DataBlockMarshalerFactory;


