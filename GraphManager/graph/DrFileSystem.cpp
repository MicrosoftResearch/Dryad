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

#include <DrGraphHeaders.h>

DrInputStreamManager::DrInputStreamManager(DrInputStreamPtr stream, DrStageManagerPtr stage)
{
    m_stage = stage;
    m_vertices = DrNew DrStorageVertexList();
    int numberOfParts = stream->GetNumberOfParts();

    for (int i = 0; i < numberOfParts; ++i)
    {
        m_vertices->Add(DrNew DrStorageVertex(m_stage, i, stream));
    }

    SetName(m_stage->GetStageName());
}

void DrInputStreamManager::Discard()
{
    m_stage->Discard();
    m_stage = DrNull;

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        m_vertices[i]->Discard();
    }
    m_vertices = DrNull;
}

DrStageManagerPtr DrInputStreamManager::GetStageManager()
{
    return m_stage;
}

void DrInputStreamManager::SetName(DrString name)
{
    m_name = name;

    DrString nameBase;
    if (m_name.GetString() == DrNull)
    {
        nameBase = "In";
    }
    else
    {
        nameBase = m_name;
    }

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        DrString vertexName;
        vertexName.SetF("%s[%d]", nameBase.GetChars(), i);
        m_vertices[i]->SetName(vertexName);
    }
}

DrString DrInputStreamManager::GetName()
{
    return m_name;
}

DrStorageVertexListPtr DrInputStreamManager::GetVertices()
{
    return m_vertices;
}

DrOutputStreamManager::DrOutputStreamManager(DrOutputStreamPtr stream, DrStageManagerPtr stage)
{
    m_stream = stream;
    m_stage = stage;
    m_vertices = DrNew DrOutputVertexList();

    SetName(stage->GetStageName());
}

void DrOutputStreamManager::Discard()
{
    m_stream = DrNull;

    m_stage->Discard();
    m_stage = DrNull;

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        m_vertices[i]->Discard();
    }
    m_vertices = DrNull;
}

DrStageManagerPtr DrOutputStreamManager::GetStageManager()
{
    return m_stage;
}

void DrOutputStreamManager::SetName(DrString name)
{
    m_name = name;

    DrString nameBase;
    if (m_name.GetString() == DrNull)
    {
        nameBase = "Out";
    }
    else
    {
        nameBase = m_name;
    }

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        DrString vertexName;
        vertexName.SetF("%s[%d]", nameBase.GetChars(), i);
        m_vertices[i]->SetName(vertexName);
    }
}

DrString DrOutputStreamManager::GetName()
{
    return m_name;
}

void DrOutputStreamManager::SetNumberOfParts(int numberOfParts)
{
    m_vertices = DrNew DrOutputVertexList(numberOfParts);

    int i;
    for (i=0; i<numberOfParts; ++i)
    {
        DrOutputVertexRef v = DrNew DrOutputVertex(m_stage, i, this);
        m_vertices->Add(v);
    }
    
    SetName(m_name);
    m_stream->SetNumberOfParts(numberOfParts);
    
}

DrOutputVertexListPtr DrOutputStreamManager::GetVertices()
{
    return m_vertices;
}

void DrOutputStreamManager::AddDynamicSplitVertex(DrOutputVertexPtr newVertex)
{
    if (m_startedSplitting == false)
    {
        DrAssert(m_vertices->Size() == 1);
        SetNumberOfParts(0);
        m_startedSplitting = true;
    }

    m_vertices->Add(newVertex);
}

HRESULT DrOutputStreamManager::FinalizeSuccessfulParts(bool jobSuccess, DrStringR errorText)
{
    DrOutputPartitionArrayRef partitionArray = DrNew DrOutputPartitionArray(m_vertices->Size());

    int i;
    for (i=0; i<m_vertices->Size(); ++i)
    {
        // this call abandons all non-successful version. If jobSuccess==false, it will abandon
        // all versions
        partitionArray[i] = m_vertices[i]->FinalizeVersions(jobSuccess);
    }

    if (jobSuccess)
    {
        return m_stream->FinalizeSuccessfulParts(partitionArray, errorText);
    }
    else
    {
        return m_stream->DiscardFailedStream(errorText);
    }
}

DrString DrOutputStreamManager::GetURIForWrite(int partitionIndex, int id, int version, int outputPort,
                                               DrResourcePtr runningResource, DrMetaDataRef metaData)
{
    return m_stream->GetURIForWrite(partitionIndex, id, version, outputPort, runningResource, metaData);
}

void DrOutputStreamManager::AbandonVersion(int partitionIndex, int id, int version, int outputPort,
                                           DrResourcePtr runningResource, bool jobSuccess)
{
    m_stream->DiscardUnusedPart(partitionIndex, id, version, outputPort, runningResource, jobSuccess);
}

void DrOutputStreamManager::ExtendLease(DrTimeInterval lease)
{
    m_stream->ExtendLease(lease);
}