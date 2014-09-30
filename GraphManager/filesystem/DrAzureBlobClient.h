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

using namespace Microsoft::Research::Peloponnese::Azure;

DRCLASS(DrAzureInputStream) : public DrInputStream
{
public:   
    DrAzureInputStream();
    void Open(DrNativeString streamUri);

    virtual DrString GetStreamName() DROVERRIDE;
    virtual int GetNumberOfParts() DROVERRIDE;
    virtual DrAffinityRef GetAffinity(int partitionIndex) DROVERRIDE;
    virtual DrString GetURIForRead(int partitionIndex, DrResourcePtr runningResource) DROVERRIDE;

private:
    AzureCollectionPartition^    m_partition;
    array<AzureCollectionPart^>^ m_partArray;
};
DRREF(DrAzureInputStream);

DRCLASS(DrAzureOutputStream) : public DrOutputStream
{
public:
    DrAzureOutputStream();

    void Open(DrNativeString streamUri);

    virtual void SetNumberOfParts(int numberOfParts) DROVERRIDE;
    virtual DrString GetURIForWrite(
        int partitionIndex, 
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource,
        DrMetaDataRef metaData) DROVERRIDE;

    virtual void DiscardUnusedPart(
        int partitionIndex,
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource,
        bool jobSuccess) DROVERRIDE;

    virtual HRESULT FinalizeSuccessfulParts(
        DrOutputPartitionArrayRef partitionArray,
        DrStringR errorText) DROVERRIDE;

    virtual HRESULT DiscardFailedStream(DrStringR errorText) DROVERRIDE;

    virtual void ExtendLease(DrTimeInterval) DROVERRIDE;

private:
    AzureCollectionPartition^  m_partition;
    int                        m_numParts;
};
DRREF(DrAzureOutputStream);
