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

#ifdef _MANAGED
using namespace Microsoft::Research::Dryad::Hdfs;
#else 
#include "HdfsBridgeNative.h"
#endif

DRCLASS(DrHdfsInputStream) : public DrInputStream
{
public:   
    DrHdfsInputStream();
    HRESULT Open(DrUniversePtr universe, DrNativeString streamUri);
    HRESULT OpenInternal(DrUniversePtr universe, DrString streamUri);

    virtual DrString GetStreamName() DROVERRIDE;
    virtual int GetNumberOfPartitions() DROVERRIDE;
    virtual DrAffinityRef GetAffinity(int partitionIndex) DROVERRIDE;
    virtual DrString GetURIForRead(int partitionIndex, DrResourcePtr runningResource) DROVERRIDE;

private:
    DrString                m_streamUri;
    DrAffinityArrayRef      m_affinity;
    DrUINT64ArrayRef        m_partIds;
    DrUINT64ArrayRef        m_partOffsets;
    DrUINT32ArrayRef        m_partFileIds;
#ifdef _MANAGED
    array<System::String^>^ m_fileNameArray;
    HdfsInstance^           m_hdfsInstance;
#else 
    HdfsBridgeNative::Instance* m_hdfsInstance;
    DrString                m_hostname;
    int                     m_portNum;
    const char**            m_fileNameArray;
    
#endif
};
DRREF(DrHdfsInputStream);

DRCLASS(DrHdfsOutputStream) : public DrOutputStream
{
public:
    DrHdfsOutputStream();

    HRESULT Open(DrNativeString streamUri);

    virtual void SetNumberOfPartitions(int numberOfPartitions) DROVERRIDE;
    virtual DrString GetURIForWrite(
        int partitionIndex, 
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource,
        DrMetaDataRef metaData) DROVERRIDE;

    virtual void DiscardUnusedPartition(
        int partitionIndex,
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource) DROVERRIDE;

    virtual HRESULT FinalizeSuccessfulPartitions(
        DrOutputPartitionArrayRef partitionArray) DROVERRIDE;

    virtual void ExtendLease(DrTimeInterval) DROVERRIDE;

private:
    int                m_numParts;
#ifdef _MANAGED
    System::String^    m_baseUri;
    HdfsInstance^      m_hdfsInstance;
#else 
    HdfsBridgeNative::Instance* m_hdfsInstance;
    DrString           m_baseUri;
#endif
};
DRREF(DrPartitionOutputStream);
