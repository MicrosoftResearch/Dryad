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

#include <DrVertexHeaders.h>

bool DrEdge::IsStartCliqueEdge()
{
    return (m_type == DCT_Pipe || m_type == DCT_Fifo);
}

bool DrEdge::IsGangEdge()
{
    return (m_type == DCT_Pipe || m_type == DCT_Fifo || m_type == DCT_FifoNonBlocking);
}

bool DrEdge::operator==(DrEdgeR other)
{
    return
        (m_remoteVertex == other.m_remoteVertex &&
         m_remotePort == other.m_remotePort &&
         m_type == other.m_type);
}

DrEdgeHolder::DrEdgeHolder(bool edgeHolderIsInput)
{
    m_inputEdges = edgeHolderIsInput;
    SetNumberOfEdges(0);    
}

void DrEdgeHolder::SetNumberOfEdges(int numberOfEdges)
{
    m_edge = DrNew DrEdgeList();
    int i;
    for (i=0; i<numberOfEdges; ++i)
    {
        DrEdge e;
        e.m_type = DCT_Tombstone;
        m_edge->Add(e);
    }
}

void DrEdgeHolder::GrowNumberOfEdges(int newNumberOfEdges)
{
    DrAssert(newNumberOfEdges >= m_edge->Size());

    int newCount = newNumberOfEdges - m_edge->Size();
    int i;
    for (i=0; i<newCount; ++i)
    {
        DrEdge e;
        e.m_type = DCT_Tombstone;
        m_edge->Add(e);
    }
}

int DrEdgeHolder::GetNumberOfEdges()
{
    return m_edge->Size();
}

void DrEdgeHolder::SetEdge(int edgeIndex, DrEdge edge)
{
    if (edgeIndex < m_edge->Size())
    {
        m_edge[edgeIndex] = edge;
    }
    else
    {
        DrAssert(edgeIndex == m_edge->Size());
        m_edge->Add(edge);
    }
}

void DrEdgeHolder::Compact(DrVertexPtr thisVertex)
{
    DrEdgeListRef newList = DrNew DrEdgeList();

    int remoteCount = 0;
    int i;

    for (i=0; i<m_edge->Size(); ++i)
    {
        if (m_edge[i].m_type != DCT_Tombstone)
        {
            newList->Add(m_edge[i]);

            int remotePort = m_edge[i].m_remotePort;

            DrVertexPtr remoteVertex = m_edge[i].m_remoteVertex;
            DrEdgeHolderRef remoteHolder;
            if (m_inputEdges)
            {
                remoteHolder = remoteVertex->GetOutputs();
            }
            else
            {
                remoteHolder = remoteVertex->GetInputs();
            }
                
            DrEdge remoteEdge = remoteHolder->GetEdge(remotePort);                    
            if (remoteEdge.m_type != DCT_Tombstone)
            {
                DrAssert(remoteEdge.m_remotePort == i);
                remoteEdge.m_remotePort = remoteCount;
                remoteHolder->SetEdge(remotePort, remoteEdge);
            }

            ++remoteCount; // Incrememt for each one added
        }
    }

    if (thisVertex && m_inputEdges)
    {
        thisVertex->CompactPendingVersion(this, remoteCount);
    }    
    m_edge = newList;

}

DrEdge DrEdgeHolder::GetEdge(int edgeIndex)
{
    return m_edge[edgeIndex];
}


void DrActiveVertexOutputGenerator::StoreOutputLengths(DrVertexProcessStatusPtr status, DrTimeInterval runningTime)
{
    DrOutputChannelArrayRef outputs = status->GetOutputChannels();

    m_lengthArray = DrNew DrUINT64Array(outputs->Allocated());

    int i;
    for (i=0; i<outputs->Allocated(); ++i)
    {
        m_lengthArray[i] = outputs[i]->GetChannelProcessedLength();
    }

    m_runningTime = runningTime;
}

DrTimeInterval DrActiveVertexOutputGenerator::GetRunningTime()
{
    return m_runningTime;
}

#ifndef _MANAGED
int DrActiveVertexOutputGenerator::s_intermediateCompressionMode = 0;
#endif

void DrActiveVertexOutputGenerator::SetProcess(DrProcessHandlePtr process,
                                               int vertexId, int version)
{
    m_vertexId = vertexId;
    m_version = version;
    /* There are failure cases where SetProcess is called with process == DrNull,
       so check for that */
    if (process != DrNull)
    {
        /* Cache some state, because process gets closed in process termination, 
           before GetURIForRead is called by downstream vertices */

        m_directory = process->GetDirectory();

        /* Assigned node */
        m_assignedNode = process->GetAssignedNode();
    }
}

int DrActiveVertexOutputGenerator::GetVersion()
{
    return m_version;
}

DrAffinityRef DrActiveVertexOutputGenerator::GetOutputAffinity(int output)
{
    DrAffinityRef a = DrNew DrAffinity();
    if (m_lengthArray != DrNull)
    {
        a->SetWeight(m_lengthArray[output]);
    }
    a->AddLocality(m_assignedNode);
    return a;
}

DrString DrActiveVertexOutputGenerator::GetURIForWrite(DrEdgeHolderPtr outputEdges,
                                                       int output, DrConnectorType type,
                                                       DrMetaDataRef metaData)
{
    DrString uri;

    DrEdge e;

    if (m_assignedNode == DrNull)
    {
        /* This should never happen - but just in case it does, let's assert so we can debug */
        DrLogA("Active vertex output generator was asked for a write URI when no assigned node is available vertex %d.%d", m_vertexId, m_version);
    }

    switch (type)
    {
    case DCT_File:
        {
            DrString leafName;
            leafName.SetF("%d_%d_%d.tmp", m_vertexId, output, m_version, DrActiveVertexOutputGenerator::s_intermediateCompressionMode);
            uri = m_assignedNode->GetCluster()->TranslateFileToURI(leafName, m_directory, m_assignedNode, m_assignedNode, DrActiveVertexOutputGenerator::s_intermediateCompressionMode);
        }
        break;

    case DCT_Output:
        e = outputEdges->GetEdge(output);
        uri = e.m_remoteVertex->GetURIForWrite(e.m_remotePort, m_vertexId, m_version, output, m_assignedNode, metaData);
        break;

    case DCT_Pipe:
        /* TODO implement pipes */
        DrLogA("Pipes not implemented");
        break;

    case DCT_Fifo:
        uri.SetF("fifo://%u/%d_%d_%d", 32, m_vertexId, output, m_version);
        break;

    case DCT_FifoNonBlocking:
        uri.SetF("fifo://%u/%d_%d_%d", (UINT32) -1, m_vertexId, output, m_version);
        break;
    }

    return uri;
}

DrString DrActiveVertexOutputGenerator::GetURIForRead(int output, DrConnectorType type,
                                                      DrResourcePtr runningResource)
{
    DrString uri;

    switch (type)
    {
    case DCT_File:
        if (m_assignedNode != DrNull)
        {
            DrString leafName;
            leafName.SetF("%d_%d_%d.tmp", m_vertexId, output, m_version, DrActiveVertexOutputGenerator::s_intermediateCompressionMode);
            uri = runningResource->GetCluster()->TranslateFileToURI(leafName, m_directory, m_assignedNode, runningResource, DrActiveVertexOutputGenerator::s_intermediateCompressionMode);
        }
        else
        {
            /* This should never happen - but just in case it does, let's assert so we can debug */
            DrLogA("Active vertex output generator was asked for a read URI when no assigned node is available vertex %d.%d", m_vertexId, m_version);
        }
        break;

    case DCT_Output:
        /* can't be reading from an edge that leads to an output vertex */
        DrLogA("Active vertex output generator was asked for a read URI on output edge type %d", output);
        break;

    case DCT_Pipe:
        /* TODO implement pipes */
        DrLogA("Pipes not implemented");
        break;

    case DCT_Fifo:
        uri.SetF("fifo://%u/%d_%d_%d", 32, m_vertexId, output, m_version);
        break;

    case DCT_FifoNonBlocking:
        uri.SetF("fifo://%u/%d_%d_%d", (UINT32) -1, m_vertexId, output, m_version);
        break;
    }

    return uri;
}

DrResourcePtr DrActiveVertexOutputGenerator::GetResource()
{
    return m_assignedNode;
}


DrStorageVertexOutputGenerator::DrStorageVertexOutputGenerator(int partitionIndex,
                                                               DrIInputPartitionReaderPtr reader)
{
    m_partitionIndex = partitionIndex;
    m_reader = reader;
}

DrResourcePtr DrStorageVertexOutputGenerator::GetResource()
{
    return DrNull;
}

int DrStorageVertexOutputGenerator::GetVersion()
{
    return 0;
}

int DrStorageVertexOutputGenerator::GetPartitionIndex()
{
    return m_partitionIndex;
}

DrAffinityRef DrStorageVertexOutputGenerator::GetOutputAffinity(int /* unused output */)
{
    return m_reader->GetAffinity(m_partitionIndex);
}

DrString DrStorageVertexOutputGenerator::GetURIForRead(int /* unused output */,
                                                       DrConnectorType type,
                                                       DrResourcePtr runningResource)
{
    DrAssert(type == DCT_File);
    return m_reader->GetURIForRead(m_partitionIndex, runningResource);
}


DrTeeVertexOutputGenerator::DrTeeVertexOutputGenerator(DrVertexOutputGeneratorPtr wrappedGenerator)
{
    m_wrappedGenerator = wrappedGenerator;
}

DrVertexOutputGeneratorPtr DrTeeVertexOutputGenerator::GetWrappedGenerator()
{
    return m_wrappedGenerator;
}

DrResourcePtr DrTeeVertexOutputGenerator::GetResource()
{
    return m_wrappedGenerator->GetResource();
}

int DrTeeVertexOutputGenerator::GetVersion()
{
    return m_wrappedGenerator->GetVersion();
}

DrAffinityRef DrTeeVertexOutputGenerator::GetOutputAffinity(int /* unused output */)
{
    return m_wrappedGenerator->GetOutputAffinity(0);
}

DrString DrTeeVertexOutputGenerator::GetURIForRead(int /* unused output */,
                                                      DrConnectorType type,
                                                      DrResourcePtr runningResource)
{
    return m_wrappedGenerator->GetURIForRead(0, type, runningResource);
}
