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
#include <dryadmetadata.h>

/* these are wrapper classes to access properties that abstracts the
   different mechanisms for getting and setting properties in xcompute 
   and potentially other clusters */
class DryadPnProcessPropertyRequest : public DrRefCounter
{
public:
    virtual ~DryadPnProcessPropertyRequest();

    virtual void SetPropertyLabel(const char* label,
                                  const char* controlLabel) = 0;
    virtual void SetPropertyString(const char* string) = 0;
    virtual DrMemoryBuffer* GetPropertyBlock() = 0;
};

class DryadPnProcessPropertyResponse : public DrRefCounter
{
public:
    virtual ~DryadPnProcessPropertyResponse();

    virtual void RetrievePropertyLabel(const char* label) = 0;
    virtual DrMemoryBuffer* GetPropertyBlock() = 0;
};

class DryadChannelDescription : public DrPropertyParser
{
public:
    DryadChannelDescription(bool isInputChannel);
    virtual ~DryadChannelDescription();

    DrError GetChannelState() const;
    void SetChannelState(DrError state);

    const char* GetChannelURI() const;
    void SetChannelURI(const char* channelURI);

    DryadMetaData* GetChannelMetaData() const;
    void SetChannelMetaData(DryadMetaData* metaData);

    UInt64 GetChannelTotalLength() const;
    void SetChannelTotalLength(UInt64 totalLength);

    UInt64 GetChannelProcessedLength() const;
    void SetChannelProcessedLength(UInt64 processedLength);

    DrError Serialize(DrMemoryWriter* writer);
    DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID,
                            UInt32 dataLen, void *cookie);

    void CopyFrom(DryadChannelDescription* src, bool includeLengths);

private:
    DrError               m_state;
    DrStr64               m_URI;
    DryadMetaDataRef      m_metaData;
    UInt64                m_totalLength;
    UInt64                m_processedLength;
    bool                  m_isInputChannel;
};

class DryadInputChannelDescription : public DryadChannelDescription
{
public:
    DryadInputChannelDescription();
};

class DryadOutputChannelDescription : public DryadChannelDescription
{
public:
    DryadOutputChannelDescription();
};

class DVertexProcessStatus : public DrPropertyParser, public DrRefCounter
{
public:
    DVertexProcessStatus();
    ~DVertexProcessStatus();

    UInt32 GetVertexId();
    void SetVertexId(UInt32 vertexId);

    UInt32 GetVertexInstanceVersion();
    void SetVertexInstanceVersion(UInt32 instanceVersion);

    DryadMetaData* GetVertexMetaData();
    void SetVertexMetaData(DryadMetaData* metaData);

    UInt32 GetInputChannelCount();
    void SetInputChannelCount(UInt32 channelCount);

    UInt32 GetMaxOpenInputChannelCount();
    void SetMaxOpenInputChannelCount(UInt32 channelCount);

    DryadInputChannelDescription* GetInputChannels();

    UInt32 GetOutputChannelCount();
    void SetOutputChannelCount(UInt32 channelCount);

    UInt32 GetMaxOpenOutputChannelCount();
    void SetMaxOpenOutputChannelCount(UInt32 channelCount);

    DryadOutputChannelDescription* GetOutputChannels();

    bool GetCanShareWorkQueue();
    void SetCanShareWorkQueue(bool canShareWorkQueue);

    DrError Serialize(DrMemoryWriter* writer);
    DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID,
                            UInt32 dataLen, void *cookie);

    void CopyFrom(DVertexProcessStatus* src, bool includeLengths);

private:
    UInt32                            m_id;
    UInt32                            m_version;
    DryadMetaDataRef                  m_metaData;
    UInt32                            m_nInputChannels;
    UInt32                            m_maxInputChannels;
    DryadInputChannelDescription*     m_inputChannel;
    UInt32                            m_nOutputChannels;
    UInt32                            m_maxOutputChannels;
    DryadOutputChannelDescription*    m_outputChannel;
    bool                              m_canShareWorkQueue;

    UInt32                            m_nextInputChannelToRead;
    UInt32                            m_nextOutputChannelToRead;
};
   

class DVertexStatus : public DrPropertyParser, public DrRefCounter
{
public:
    DVertexStatus();

    DrError GetVertexState();
    void SetVertexState(DrError state);

    DVertexProcessStatus* GetProcessStatus();
    void SetProcessStatus(DVertexProcessStatus* status);

    DrError Serialize(DrMemoryWriter* writer);
    DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID,
                            UInt32 dataLen, void *cookie);

    void StoreInRequestMessage(DryadPnProcessPropertyRequest* request);
    DrError ReadFromResponseMessage(DryadPnProcessPropertyResponse* response,
                                    UInt32 vertexId, UInt32 vertexVersion);

    static void GetPnPropertyLabel(DrStr* pDstString,
                                   UInt32 vertexId, UInt32 vertexVersion,
                                   bool notifyWaiters);

private:
    DrError                           m_state;
    DrRef<DVertexProcessStatus>       m_processStatus;
};


class DVertexCommandBlock : public DrPropertyParser, public DrRefCounter
{
public:
    DVertexCommandBlock();
    ~DVertexCommandBlock();

    DVertexCommand GetVertexCommand();
    void SetVertexCommand(DVertexCommand command);

    DVertexProcessStatus* GetProcessStatus();
    void SetProcessStatus(DVertexProcessStatus* status);

    UInt32 GetArgumentCount();
    void SetArgumentCount(UInt32 nArguments);
    DrStr64* GetArgumentVector();
    void SetArgument(UInt32 argumentIndex, const char* argument);

    void* GetRawSerializedBlock();
    UInt32 GetRawSerializedBlockLength();
    void SetRawSerializedBlock(UInt32 length, const void* data);

    void SetDebugBreak(bool setBreakpointOnCommandArrival);
    bool GetDebugBreak();

    DrError Serialize(DrMemoryWriter* writer);
    DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID,
                            UInt32 dataLen, void *cookie);

    void StoreInRequestMessage(DryadPnProcessPropertyRequest* request);
    DrError ReadFromResponseMessage(DryadPnProcessPropertyResponse* response,
                                    UInt32 vertexId, UInt32 vertexVersion);

    static void GetPnPropertyLabel(DrStr* pDstString,
                                   UInt32 vertexId, UInt32 vertexVersion);

private:
    DVertexCommand                    m_command;
    DrRef<DVertexProcessStatus>       m_processStatus;
    UInt32                            m_nArguments;
    DrStr64*                          m_argument;
    UInt32                            m_serializedBlockLength;
    char*                             m_serializedBlock;
    bool                              m_setBreakpointOnCommandArrival;
    UInt32                            m_nextArgumentToRead;
};
