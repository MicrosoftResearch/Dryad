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

#include <DrFileSystems.h>

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Collections::Specialized;
using namespace System::Diagnostics;
using namespace System::IO;
using namespace System::Runtime::InteropServices;
using namespace System::Text;
using namespace System::Linq;

DrAzureInputStream::DrAzureInputStream()
{
}

void DrAzureInputStream::Open(DrNativeString streamUri)
{
    try
    {
        System::Uri^ uri = DrNew System::Uri(streamUri);

        m_partition = gcnew AzureCollectionPartition(uri, false);
        m_partArray = Enumerable::ToArray(m_partition->GetPartition());
    }
    catch (System::Exception^ e)
    {
        System::String^ message = "Failed to open Azure stream " + streamUri + ": " + e->ToString();
        DrString m(message);
        DrLogW("%s", m.GetChars());
        throw e;
    }
}

DrString DrAzureInputStream::GetStreamName()
{
    return DrString(m_partition->BaseUri->AbsoluteUri);
}

int DrAzureInputStream::GetNumberOfParts()
{
    return m_partArray->Length;
}

DrAffinityRef DrAzureInputStream::GetAffinity(int partitionIndex)
{
    return DrNew DrAffinity();
}

DrString DrAzureInputStream::GetURIForRead(int partitionIndex, DrResourcePtr runningResource)
{
    try
    {
        return DrString(m_partArray[partitionIndex]->ToUri(m_partition)->AbsoluteUri);
    }
    catch (System::Exception^ e)
    {
        DrString m(e->ToString());
        DrString uri(m_partition->BaseUri->AbsoluteUri);
        DrString part(m_partArray[partitionIndex]->ToString());
        DrLogA("Failed to get URI for read %s part %d=%s: %s", uri.GetChars(), partitionIndex, part.GetChars(), m.GetChars()); 
        return DrString("error");
    }
}


DrAzureOutputStream::DrAzureOutputStream()
{
    m_numParts = -1;
    m_partition = DrNull;
}

void DrAzureOutputStream::Open(DrNativeString streamUri)
{
    System::Uri^ baseUri = gcnew System::Uri(streamUri);
    m_partition = gcnew AzureCollectionPartition(baseUri, false);
    if (m_partition->IsCollectionExists())
    {
        throw gcnew System::ApplicationException("Won't open " + streamUri + " to write since it already exists");
    }
}

void DrAzureOutputStream::SetNumberOfParts(int numberOfParts)
{
    // For now, assume that the number of parts cannot change
    DrAssert(m_numParts == -1);
    DrAssert(m_partition != DrNull);

    m_numParts = numberOfParts;
}

DrString DrAzureOutputStream::GetURIForWrite(
        int partitionIndex, 
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource,
        DrMetaDataRef metaData)
{
    System::UriBuilder^ b = gcnew System::UriBuilder(m_partition->BaseUri);
    b->Path += "/part-" + partitionIndex.ToString("D8") + "-" + version.ToString("D4");
    return DrString(b->Uri->AbsoluteUri);
}

void DrAzureOutputStream::DiscardUnusedPart(
        int partitionIndex,
        int id, 
        int version, 
        int outputPort,
        DrResourcePtr runningResource,
        bool jobSuccess)
{
    if (!jobSuccess)
    {
        // if the job has failed we will delete all the parts in one go when discarding
        // the stream, so nothing to do here
        return;
    }

    /* delete the partition if it has been created */
    DrString uriString = GetURIForWrite(
        partitionIndex,
        id,
        version,
        outputPort,
        runningResource,
        DrNull);

    try
    {
        m_partition->DeleteMatchingParts(gcnew Uri(uriString.GetString()));
    }
    catch (System::Exception^ e)
    {
        DrString msg(e->ToString());
        DrString base(m_partition->BaseUri->AbsoluteUri);

        DrLogW("Got exception deleting part %s of %s: %s", uriString.GetChars(), base.GetChars(), msg.GetChars());
    }
}

HRESULT DrAzureOutputStream::FinalizeSuccessfulParts(DrOutputPartitionArrayRef, DrStringR errorText)
{
    // nothing to do since blobs cannot be renamed
    return S_OK;
}

HRESULT DrAzureOutputStream::DiscardFailedStream(DrStringR errorText)
{
    DrAssert(m_partition != DrNull);
    try
    {
        m_partition->DeleteCollection();
    }
    catch (System::Exception^ e)
    {
        errorText.SetF("Exception deleting Azure directory %s: %s", DrString(m_partition->BaseUri->AbsoluteUri).GetChars(), DrString(e->ToString()).GetChars());
        return DrError_FailedToDeleteFileset;
    }

    return S_OK;
}

void DrAzureOutputStream::ExtendLease(DrTimeInterval)
{
}
