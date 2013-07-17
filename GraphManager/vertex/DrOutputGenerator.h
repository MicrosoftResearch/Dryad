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

DRPUBLICENUM(DrConnectorType)
{
    DCT_File,
    DCT_Output,
    DCT_Pipe,
    DCT_Fifo,
    DCT_FifoNonBlocking,
    DCT_Tombstone
};

DRDECLARECLASS(DrVertex);
DRREF(DrVertex);

DRDECLAREVALUECLASS(DrEdge);
DRRREF(DrEdge);

DRVALUECLASS(DrEdge)
{
public:
    bool IsStartCliqueEdge();
    bool IsGangEdge();

    bool operator==(DrEdgeR other);

    DrConnectorType   m_type;
    DrVertexRef       m_remoteVertex;
    int               m_remotePort;
};

DRMAKEARRAYLIST(DrEdge);

DRBASECLASS(DrEdgeHolder)
{
public:
    DrEdgeHolder(bool edgeHolderIsInput);

    void SetNumberOfEdges(int numberOfEdges);
    void GrowNumberOfEdges(int newNumberOfEdges);

    int GetNumberOfEdges();

    void Compact(DrVertexPtr thisVertex);
    
    void SetEdge(int edgeIndex, DrEdge edge);
    DrEdge GetEdge(int edgeIndex);

private:
    DrEdgeListRef     m_edge;
    bool              m_inputEdges;
};
DRREF(DrEdgeHolder);

DRBASECLASS(DrVertexOutputGenerator abstract)
{
public:
    virtual DrResourcePtr GetResource() DRABSTRACT;
    virtual int GetVersion() DRABSTRACT;
    virtual DrAffinityRef GetOutputAffinity(int output) DRABSTRACT;
    virtual DrString GetURIForRead(int output, DrConnectorType type, DrResourcePtr runningResource) DRABSTRACT;
};
DRREF(DrVertexOutputGenerator);

typedef DrArray<DrVertexOutputGeneratorRef> DrGeneratorArray;
DRAREF(DrGeneratorArray,DrVertexOutputGeneratorRef);

DRCLASS(DrActiveVertexOutputGenerator) : public DrVertexOutputGenerator
{
public:
    void StoreOutputLengths(DrVertexProcessStatusPtr status, DrTimeInterval runningTime);
    void SetProcess(DrProcessHandlePtr process, int vertexId, int version);

    virtual DrResourcePtr GetResource() DROVERRIDE;
    virtual int GetVersion() DROVERRIDE;
    virtual DrAffinityRef GetOutputAffinity(int output) DROVERRIDE;
    virtual DrString GetURIForRead(int output, DrConnectorType type, DrResourcePtr runningResource) DROVERRIDE;
    static DrString GetURIForWrite(DrEdgeHolderPtr outputEdges, DrResourcePtr runningResource,
                                   int id, int version,
                                   int output, DrConnectorType type,
                                   DrMetaDataRef metaData);

    DrTimeInterval GetRunningTime();

    static int s_intermediateCompressionMode;

private:
    int                   m_vertexId;
    int                   m_version;
    DrUINT64ArrayRef      m_lengthArray;
    DrTimeInterval        m_runningTime;
    DrString              m_uriBase;
    DrResourcePtr         m_assignedNode;
    int                   m_compression;
};
DRREF(DrActiveVertexOutputGenerator);

DRINTERFACE(DrIInputPartitionReader)
{
public:
    virtual DrAffinityRef GetAffinity(int partitionIndex) DRABSTRACT;
    virtual DrString GetURIForRead(int partitionIndex, DrResourcePtr runningResource) DRABSTRACT;
};
DRIREF(DrIInputPartitionReader);

DRCLASS(DrStorageVertexOutputGenerator) : public DrVertexOutputGenerator
{
public:
    DrStorageVertexOutputGenerator(int partitionIndex, DrIInputPartitionReaderPtr reader);

    virtual DrResourcePtr GetResource() DROVERRIDE;
    virtual int GetVersion() DROVERRIDE;
    virtual DrAffinityRef GetOutputAffinity(int output) DROVERRIDE;
    virtual DrString GetURIForRead(int output, DrConnectorType type, DrResourcePtr runningResource) DROVERRIDE;

private:
    int                          m_partitionIndex;
    DrIInputPartitionReaderIRef  m_reader;
};
DRREF(DrStorageVertexOutputGenerator);

DRCLASS(DrTeeVertexOutputGenerator) : public DrVertexOutputGenerator
{
public:
    DrTeeVertexOutputGenerator(DrVertexOutputGeneratorPtr wrappedGenerator);

    DrVertexOutputGeneratorPtr GetWrappedGenerator();

    virtual DrResourcePtr GetResource() DROVERRIDE;
    virtual int GetVersion() DROVERRIDE;
    virtual DrAffinityRef GetOutputAffinity(int output) DROVERRIDE;
    virtual DrString GetURIForRead(int output, DrConnectorType type, DrResourcePtr runningResource) DROVERRIDE;

private:
    DrVertexOutputGeneratorRef m_wrappedGenerator;
};
DRREF(DrTeeVertexOutputGenerator);
