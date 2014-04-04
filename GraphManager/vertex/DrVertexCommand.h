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

DRDECLARECLASS(DrChannelDescription);
DRREF(DrChannelDescription);

DRBASECLASS(DrChannelDescription), public DrPropertyParser
{
public:
    DrChannelDescription(bool isInputChannel);
    virtual ~DrChannelDescription();

    HRESULT GetChannelState();
    void SetChannelState(HRESULT state);

    DrString GetChannelURI();
    void SetChannelURI(DrString uri);

    DrMetaDataPtr GetChannelMetaData();
    void SetChannelMetaData(DrMetaDataPtr metaData);

    HRESULT GetChannelErrorCode();
    void SetChannelErrorCode(HRESULT errorCode);

    DrString GetChannelErrorString();
    void SetChannelErrorString(DrString errorString);

    UINT64 GetChannelTotalLength();
    void SetChannelTotalLength(UINT64 totalLength);

    UINT64 GetChannelProcessedLength();
    void SetChannelProcessedLength(UINT64 processedLength);

    void Serialize(DrPropertyWriterPtr writer);
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID, UINT32 dataLen);

    void CopyFrom(DrChannelDescriptionPtr src, bool includeLengths);

private:
    HRESULT               m_state;
    DrString              m_URI;
    DrMetaDataRef         m_metaData;
    HRESULT               m_errorCode;
    DrString              m_errorString;
    UINT64                m_totalLength;
    UINT64                m_processedLength;
    bool                  m_isInputChannel;
};

DRCLASS(DrInputChannelDescription) : public DrChannelDescription
{
public:
    DrInputChannelDescription();
};
DRREF(DrInputChannelDescription);

DRCLASS(DrOutputChannelDescription) : public DrChannelDescription
{
public:
    DrOutputChannelDescription();
};
DRREF(DrOutputChannelDescription);

typedef DrArray<DrInputChannelDescriptionRef> DrInputChannelArray;
DRAREF(DrInputChannelArray,DrInputChannelDescriptionRef);

typedef DrArray<DrOutputChannelDescriptionRef> DrOutputChannelArray;
DRAREF(DrOutputChannelArray,DrOutputChannelDescriptionRef);

DRDECLARECLASS(DrVertexProcessStatus);
DRREF(DrVertexProcessStatus);

DRBASECLASS(DrVertexProcessStatus), public DrPropertyParser
{
public:
    DrVertexProcessStatus();

    int GetVertexId();
    void SetVertexId(int vertexId);

    int GetVertexInstanceVersion();
    void SetVertexInstanceVersion(int instanceVersion);

    DrMetaDataPtr GetVertexMetaData();
    void SetVertexMetaData(DrMetaDataPtr metaData);

    HRESULT GetVertexErrorCode();
    void SetVertexErrorCode(HRESULT errorCode);

    DrString GetVertexErrorString();
    void SetVertexErrorString(DrString errorString);

    void SetInputChannelCount(int channelCount);
    DrInputChannelArrayRef GetInputChannels();

    int GetMaxOpenInputChannelCount();
    void SetMaxOpenInputChannelCount(int channelCount);

    void SetOutputChannelCount(int channelCount);
    DrOutputChannelArrayRef GetOutputChannels();

    int GetMaxOpenOutputChannelCount();
    void SetMaxOpenOutputChannelCount(int channelCount);

    bool GetCanShareWorkQueue();
    void SetCanShareWorkQueue(bool canShareWorkQueue);

    void Serialize(DrPropertyWriterPtr writer);
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID, UINT32 dataLen);

    void CopyFrom(DrVertexProcessStatusPtr src, bool includeLengths);

private:
    int                               m_id;
    int                               m_version;
    DrMetaDataRef                     m_metaData;
    HRESULT                           m_errorCode;
    DrString                          m_errorString;
    int                               m_maxInputChannels;
    DrInputChannelArrayRef            m_inputChannel;
    int                               m_maxOutputChannels;
    DrOutputChannelArrayRef           m_outputChannel;
    bool                              m_canShareWorkQueue;

    int                               m_nextInputChannelToRead;
    int                               m_nextOutputChannelToRead;
};


DRBASECLASS(DrVertexStatus), public DrPropertyParser
{
public:
    DrVertexStatus();

    HRESULT GetVertexState();
    void SetVertexState(HRESULT state);

    DrVertexProcessStatusPtr GetProcessStatus();
    void SetProcessStatus(DrVertexProcessStatusPtr status);

    void Serialize(DrPropertyWriterPtr writer);
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID, UINT32 dataLen);

    static DrString GetPropertyLabel(int vertexId, int vertexVersion);

private:
    HRESULT                           m_state;
    DrVertexProcessStatusRef          m_processStatus;
};
DRREF(DrVertexStatus);

DRENUM(DrVertexCommand)
{
    DrVC_Start = 0,
    DrVC_ReOpenChannels,
    DrVC_Terminate,
    DrVC_Max
};

DRBASECLASS(DrVertexCommandBlock) , public DrPropertyParser
{
public:
    DrVertexCommandBlock();

    DrVertexCommand GetVertexCommand();
    void SetVertexCommand(DrVertexCommand command);

    DrVertexProcessStatusPtr GetProcessStatus();
    void SetProcessStatus(DrVertexProcessStatusPtr status);

    void SetArgumentCount(int nArguments);
    DrStringArrayRef GetArgumentVector();

    DrByteArrayPtr GetRawSerializedBlock();
    void SetRawSerializedBlock(DrByteArrayPtr block);

    void SetDebugBreak(bool setBreakpointOnCommandArrival);
    bool GetDebugBreak();

    void Serialize(DrPropertyWriterPtr writer);
    virtual HRESULT ParseProperty(DrPropertyReaderPtr reader, UINT16 enumID, UINT32 dataLen);

    static DrString GetPropertyLabel(int vertexId, int vertexVersion);

private:
    DrVertexCommand                   m_command;
    DrVertexProcessStatusRef          m_processStatus;
    DrStringArrayRef                  m_argument;
    DrByteArrayRef                    m_serializedBlock;
    bool                              m_setBreakpointOnCommandArrival;

    int                               m_nextArgumentToRead;
};
DRREF(DrVertexCommandBlock);