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

DRBASECLASS(DrInputStream abstract), public DrIInputPartitionReader
{
public:
    virtual DrString GetStreamName() DRABSTRACT;
    virtual int GetNumberOfPartitions() DRABSTRACT;
    virtual DrAffinityRef GetAffinity(int partitionIndex) DRABSTRACT;
    virtual DrString GetURIForRead(int partitionIndex, DrResourcePtr runningResource) DRABSTRACT;
};
DRREF(DrInputStream);

/* This class is needed for DrOutputStream. It is defined in DrVertex.h but repeated here for
   convenience
DRVALUECLASS(DrOutputPartition)
{
public:
    int             m_id;
    int             m_version;
    int             m_outputPort;
    DrResourceRef   m_resource;
    UINT64          m_size;
};
*/

DRBASECLASS(DrOutputStream abstract)
{
public:
    virtual void SetNumberOfPartitions(int numberOfPartitions) DRABSTRACT;
    virtual DrString GetURIForWrite(int partitionIndex, int id, int version, int outputPort,
                                    DrResourcePtr runningResource,
                                    DrMetaDataRef metaData) DRABSTRACT;
    virtual void DiscardUnusedPartition(int partitionIndex, int id, int version, int outputPort,
                                        DrResourcePtr runningResource) DRABSTRACT;
    virtual HRESULT FinalizeSuccessfulPartitions(DrOutputPartitionArrayRef partitionArray) DRABSTRACT;
    virtual void ExtendLease(DrTimeInterval) DRABSTRACT;
};
DRREF(DrOutputStream);


DRBASECLASS(DrInputStreamManager)
{
public:
    DrInputStreamManager(DrInputStreamPtr stream, DrStageManagerPtr stage);

    void Discard();

    void SetName(DrString name);
    DrString GetName();

    DrStageManagerPtr GetStageManager();

    DrStorageVertexListPtr GetVertices();

private:
    DrString                  m_name;
    DrStageManagerRef         m_stage;
    DrStorageVertexListRef    m_vertices;
};
DRREF(DrInputStreamManager);


DRBASECLASS(DrOutputStreamManager), public DrIOutputPartitionGenerator
{
public:
    DrOutputStreamManager(DrOutputStreamPtr stream, DrStageManagerPtr stage);

    void Discard();

    void SetName(DrString name);
    DrString GetName();

    DrStageManagerPtr GetStageManager();

    void SetNumberOfPartitions(int numberOfPartitions);
    DrOutputVertexListPtr GetVertices();

    /* the DrIOutputPartitionGenerator implementation */
    virtual void AddDynamicSplitVertex(DrOutputVertexPtr newVertex);
    virtual HRESULT FinalizeSuccessfulPartitions();
    virtual DrString GetURIForWrite(int partitionIndex, int id, int version, int outputPort,
                                    DrResourcePtr runningResource, DrMetaDataRef metaData);
    virtual void AbandonVersion(int partitionIndex, int id, int version, int outputPort,
                                DrResourcePtr runningResource);
    virtual void ExtendLease(DrTimeInterval);

private:
    DrString                m_name;
    DrOutputStreamRef       m_stream;
    DrStageManagerRef       m_stage;
    DrOutputVertexListRef   m_vertices;
    bool                    m_startedSplitting;
};
DRREF(DrOutputStreamManager);
