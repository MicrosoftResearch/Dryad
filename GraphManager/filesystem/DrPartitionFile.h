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

DRCLASS(DrPartitionInputStream) : public DrInputStream
{
public:
    HRESULT Open(DrUniversePtr universe, DrNativeString streamName);
    HRESULT OpenInternal(DrUniversePtr universe, DrString streamName);

    virtual DrString GetStreamName() DROVERRIDE;
    virtual int GetNumberOfParts() DROVERRIDE;
    virtual DrAffinityRef GetAffinity(int partitionIndex) DROVERRIDE;
    virtual DrString GetURIForRead(int partitionIndex, DrResourcePtr runningResource) DROVERRIDE;

    typedef DrStringStringDictionary Override;
    DRREF(Override);

    typedef DrArray<OverrideRef> OverrideArray;
    DRAREF(OverrideArray,OverrideRef);

private:
    DrString             m_streamName;
    DrString             m_pathNameOnComputer;
    DrAffinityArrayRef   m_affinity;
    DrStringArrayRef     m_remoteName;
    OverrideArrayRef     m_override;
};
DRREF(DrPartitionInputStream);


DRCLASS(DrPartitionOutputStream) : public DrOutputStream
{
public:
    HRESULT Open(DrNativeString streamName, DrNativeString pathBase);
    HRESULT OpenInternal(DrString streamName, DrString pathBase);

    virtual void SetNumberOfParts(int numberOfParts) DROVERRIDE;
    virtual DrString GetURIForWrite(int partitionIndex, int id, int version, int outputPort,
                                    DrResourcePtr runningResource,
                                    DrMetaDataRef metaData) DROVERRIDE;
    virtual void DiscardUnusedPart(int partitionIndex, int id, int version, int outputPort,
                                   DrResourcePtr runningResource, bool jobSuccess) DROVERRIDE;
    virtual HRESULT FinalizeSuccessfulParts(
        DrOutputPartitionArrayRef partitionArray,
        DrStringR errorText) DROVERRIDE;
    virtual HRESULT DiscardFailedStream(DrStringR errorText) DROVERRIDE;
    virtual void ExtendLease(DrTimeInterval) DROVERRIDE;

private:
    HRESULT RenameSuccessfulPart(int partitionIndex, DrOutputPartition p);

    DrString                    m_streamName;
    DrString                    m_pathBase;
	bool						m_isPathBaseRooted;
    DrOutputPartitionArrayRef   m_successfulParts;
};
DRREF(DrPartitionOutputStream);
