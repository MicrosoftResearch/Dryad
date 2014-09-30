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

#ifdef _MANAGED
using namespace System;
using namespace System::Collections::Generic;
using namespace System::Collections::Specialized;
using namespace System::Diagnostics;
using namespace System::IO;
using namespace System::Runtime::InteropServices;
using namespace System::Text;
#else
using namespace HdfsBridgeNative;
#include <Wininet.h>
#endif

#ifdef _MANAGED

/* Returns 'name' from a stream URI of the form hdfs://server:port/name */

/* Returns 'host' from a UNC path of the form \\host\dir\file.ext */
String ^HdfsStorageNodeFromReadPath(String ^readPath)
{
    String ^storageNode = String::Empty;

    if (readPath->StartsWith("\\\\"))
    {
        String ^temp = readPath->TrimStart('\\');
        int serverEnd = temp->IndexOf('\\');
        if (serverEnd > 0)
        {
            storageNode = temp->Substring(0, serverEnd);
        }
    }

    return storageNode;
}

HdfsInstance ^GetHdfsServiceInstance(String ^HdfsUri)
{
    return DrNew HdfsInstance(DrNew Uri(HdfsUri));
}

HdfsInstance ^GetHdfsServiceInstance(DrString DrHdfsUri)
{
    return GetHdfsServiceInstance(DrNew String(DrHdfsUri.GetString()));
}
#else

HdfsBridgeNative::Instance* GetHdfsServiceInstance(DrString DrHdfsUri)
{
    URL_COMPONENTSA UrlComponents = {0};
    UrlComponents.dwStructSize = sizeof(UrlComponents);  
    UrlComponents.dwHostNameLength  = 1;

    BOOL fOK = InternetCrackUrlA(DrHdfsUri.GetChars(), DrHdfsUri.GetCharsLength(), 0, &UrlComponents);
    if (!fOK)
    {
        return NULL;
    }

    HdfsBridgeNative::Instance* instancePtr = NULL;
    bool ret = OpenInstance(UrlComponents.lpszHostName, UrlComponents.nPort, &instancePtr);
    if (ret) 
    {
        return instancePtr;
    } 
    else 
    {
        return NULL;
    }
}

DrString FromInternalUri(DrString baseUri, DrString inputString)
{
    URL_COMPONENTSA UrlComponents = {0};
    UrlComponents.dwStructSize = sizeof(UrlComponents);  
    UrlComponents.dwSchemeLength   = 1;
    UrlComponents.dwHostNameLength = 1;

    BOOL fOK = InternetCrackUrlA(baseUri.GetChars(), baseUri.GetCharsLength(), 0, &UrlComponents);
    if (!fOK)
    {
        DrLogA("Error getting stream path from HDFS URI.");
        return DrNull;
    }
    DrString serviceUri;
    serviceUri.AppendF("%s://%s:%d/", UrlComponents.lpszScheme, UrlComponents.lpszHostName, UrlComponents.nPort);

    if (inputString.Compare(serviceUri.GetChars(), serviceUri.GetCharsLength(), false) == 0)
    {
        return DrString(inputString.GetChars() + serviceUri.GetCharsLength());//inputString->Substring(m_serviceUri->Length);
    }
    else
    {
        return DrNull;
    }
}

DrString ToInternalUri(DrString serviceUri, DrString inputString)
{
    DrString resultString(serviceUri);
    return resultString.AppendF("%s", inputString.GetChars());
}

#endif

DrHdfsInputStream::DrHdfsInputStream()
{
    m_hdfsInstance = DrNull;
}

HRESULT DrHdfsInputStream::Open(DrUniversePtr universe, DrNativeString streamUri, DrNativeString recordType)
{
    DrString uri = DrString(streamUri);
    DrString record = DrString(recordType);

    DrLogI("Opening instance for %s record type %s", uri.GetChars(), record.GetChars());

    return OpenInternal(universe, uri, record);
}


#ifdef _MANAGED
HRESULT DrHdfsInputStream::OpenInternal(DrUniversePtr universe, DrString streamUri, DrString recordType)
{
    m_streamUri = streamUri;
    HRESULT err = S_OK;

    try 
    {
        DrLogI("Opening instance for %s: %s", streamUri.GetChars(), recordType.GetChars());
        m_hdfsInstance = GetHdfsServiceInstance(streamUri);

        HdfsFileInfo^ stream = m_hdfsInstance->GetFileInfo(streamUri.GetString(), true);
        m_fileNameArray = stream->fileNameArray;
        UInt32 totalPartitionCount;

        if (recordType.Compare("Microsoft.Research.DryadLinq.LineRecord") == 0)
        {
            DrLogI("Getting block-level file info for %s", streamUri.GetChars());
            totalPartitionCount = static_cast<UInt32>(stream->blockArray->Length);
        }
        else
        {
            DrLogI("Getting file info for %s", streamUri.GetChars());
            totalPartitionCount = m_fileNameArray->Length;
        }
      
        DrLogI("Partition count %d", totalPartitionCount);

        /* Allocate these arrays even if they're size 0, to avoid
        NullReferenceException later */
        m_affinity = DrNew DrAffinityArray(totalPartitionCount); 
        m_partOffsets = DrNew DrUINT64Array(totalPartitionCount);
        m_partFileIds = DrNew DrUINT32Array(totalPartitionCount);

        if (recordType.Compare("Microsoft.Research.DryadLinq.LineRecord") == 0)
        {
            for (UINT32 i = 0; i < totalPartitionCount; ++i)
            {
                HdfsBlockInfo^ partition = stream->blockArray[i];
                m_affinity[i] = DrNew DrAffinity();
                m_affinity[i]->SetWeight(partition->Size);
                m_partOffsets[i] = partition->Offset;
                m_partFileIds[i] = partition->fileIndex;

                for (int j = 0; j < partition->Hosts->Length; ++j)
                {
                    DrResourceRef location = universe->LookUpResource(partition->Hosts[j]);
                    if (location != DrNull)
                    {
                        m_affinity[i]->AddLocality(location);
                    }
                }
            }
        }
        else
        {
            int fileBlockIndex = 0;
            for (UINT32 i = 0; i < totalPartitionCount; ++i)
            {
                m_partOffsets[i] = 0;
                m_partFileIds[i] = i;

                HdfsBlockInfo^ partition = stream->blockArray[fileBlockIndex];
                DrAssert(partition->fileIndex == i);

                long long fileSize = partition->Size;

                HashSet<DrResourceRef>^ locations = DrNew HashSet<DrResourceRef>();
                for (int j = 0; j < partition->Hosts->Length; ++j)
                {
                    DrResourceRef location = universe->LookUpResource(partition->Hosts[j]);
                    if (location != DrNull)
                    {
                        locations->Add(location);
                    }
                }

                ++fileBlockIndex;
                
                while (fileBlockIndex < stream->blockArray->Length && stream->blockArray[fileBlockIndex]->fileIndex == i)
                {
                    partition = stream->blockArray[fileBlockIndex];
                    fileSize += partition->Size;

                    if (locations->Count > 0)
                    {
                        HashSet<DrResourceRef>^ newLocations = DrNew HashSet<DrResourceRef>();
                        for (int j = 0; j < partition->Hosts->Length; ++j)
                        {
                            DrResourceRef location = universe->LookUpResource(partition->Hosts[j]);
                            if (location != DrNull)
                            {
                                newLocations->Add(location);
                            }
                        }

                        locations->IntersectWith(newLocations);
                    }
                }

                m_affinity[i] = DrNew DrAffinity();
                m_affinity[i]->SetWeight(fileSize);

                System::Text::StringBuilder^ locationText;
                if (locations->Count > 0)
                {
                    locationText = gcnew System::Text::StringBuilder("File " + m_fileNameArray[i] + " merged locations:");
                }
                else
                {
                    locationText = gcnew System::Text::StringBuilder("File " + m_fileNameArray[i] + " no shared locations");
                }

                HashSet<DrResourceRef>::Enumerator^ enumerator = locations->GetEnumerator();
                while (enumerator->MoveNext())
                {
                    m_affinity[i]->AddLocality(enumerator->Current);
                    locationText->Append(" ");
                    locationText->Append(enumerator->Current->GetName().GetString());
                }

                DrString locationLog(locationText->ToString());
                DrLogI("%s", locationLog.GetChars());
            }
        }
    }
    catch (System::Exception ^e)
    {
        m_error = e->ToString();
        DrString msg(m_error);
        DrLogE("Got HDFS exception %s", msg.GetChars());
        err = System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }
    finally
    {
        // TODO: How do we clean this up?
        //hdfsInstance->Discard();
    }

    return err;
}
#else
HRESULT DrHdfsInputStream::OpenInternal(DrUniversePtr universe, DrString streamUri, DrString recordType)
{
    m_streamUri = streamUri;
    HRESULT err = S_OK;

      
    DrLogI("Opening instance for %s", streamUri.GetChars());
    m_hdfsInstance = GetHdfsServiceInstance(streamUri);
        
    bool ret = HdfsBridgeNative::Initialize();
    if (!ret)
    {
        DrLogE("Error calling HdfsBridgeNative::Initialize()");
        return E_FAIL;
    }

    if (m_hdfsInstance == NULL)
    {
        DrLogE("Error calling GetHdfsServiceInstance(streamUri)");
        return E_FAIL;
    }
    URL_COMPONENTSA UrlComponents = {0};
    UrlComponents.dwStructSize = sizeof(UrlComponents);  
    UrlComponents.dwUrlPathLength  = 1;
    UrlComponents.dwHostNameLength = 1;

    BOOL fOK = InternetCrackUrlA(streamUri.GetChars(), streamUri.GetCharsLength(), 0, &UrlComponents);
    if (!fOK)
    {
        DrLogE("Error getting stream path from HDFS URI.");
        return E_FAIL;
    }

    m_hostname.Set(UrlComponents.lpszHostName);
    m_portNum = UrlComponents.nPort;

    InstanceAccessor ia(m_hdfsInstance);
    FileStat* fileStat = NULL;
    ia.OpenFileStat(UrlComponents.lpszUrlPath, true, &fileStat);
    UINT32 totalPartitionCount = 0;
    HdfsBridgeNative::FileStatAccessor fs(fileStat);
    totalPartitionCount = fs.GetNumberOfBlocks();

    m_fileNameArray = (const char **)fs.GetFileNameArray();
      
    DrLogI("Partition count %d", totalPartitionCount);

    /* Allocate these arrays even if they're size 0, to avoid
    NullReferenceException later */
    m_affinity = DrNew DrAffinityArray(totalPartitionCount); 
    m_partOffsets = DrNew DrUINT64Array(totalPartitionCount);
    m_partFileIds = DrNew DrUINT32Array(totalPartitionCount);

    for (UINT32 i=0; i<totalPartitionCount; ++i)
    {
        HdfsBridgeNative::HdfsBlockLocInfo* partition = fs.GetBlockInfo(i);
        m_affinity[i] = DrNew DrAffinity();
        m_affinity[i]->SetWeight(partition->Size);
        m_partOffsets[i] = partition->Offset;
        m_partFileIds[i] = partition->fileIndex;

        for (int j = 0; j < partition->numberOfHosts; ++j)
        {
            DrResourceRef location = universe->LookUpResource(partition->Hosts[j]);
            if (location != DrNull)
            {
                m_affinity[i]->AddLocality(location);
            }
        }
        delete partition;
    }   

    return err;
}
#endif

DrNativeString DrHdfsInputStream::GetError()
{
#ifdef _MANAGED
    return m_error;
#else
    return m_error.GetString();
#endif
}

DrString DrHdfsInputStream::GetStreamName() 
{
    return m_streamUri;
}

int DrHdfsInputStream::GetNumberOfParts() 
{
    return m_affinity->Allocated();
}

DrAffinityRef DrHdfsInputStream::GetAffinity(int partitionIndex) 
{
    return m_affinity[partitionIndex];
}

DrString DrHdfsInputStream::GetURIForRead(int partitionIndex,
                                          DrResourcePtr /* unused runningResource*/)
{
    DrString uri;
    //Put HDFS service host and port in the input partition URI

#ifdef _MANAGED
    String ^HdfsStreamUri = DrNew String(m_streamUri.GetString());
    UriBuilder ^HdfsServiceUri = DrNew UriBuilder(HdfsStreamUri);

    // rewrite the scheme to be our private scheme including partition information
    HdfsServiceUri->Scheme = HdfsServiceUri->Scheme + "pt";

    // the easiest way to get an empty query collection is to parse an empty string
    // this is a subtype of NameValueCollection that implements ToString to render
    // an http query
    NameValueCollection ^query = System::Web::HttpUtility::ParseQueryString("");
    query->Add("offset", m_partOffsets[partitionIndex].ToString());
    query->Add("length", m_affinity[partitionIndex]->GetWeight().ToString());

    HdfsServiceUri->Query = query->ToString();

    uri.Set(HdfsServiceUri->Uri->AbsoluteUri);
#else
        uri.SetF("hdfspt://%s:%d/%s?%I64u?%I64u", m_hostname, m_portNum, 
            m_fileNameArray[m_partFileIds[partitionIndex]],  m_partOffsets[partitionIndex],
            m_affinity[partitionIndex]->GetWeight());
#endif

        return uri;
}


DrHdfsOutputStream::DrHdfsOutputStream()
{
    m_hdfsInstance = DrNull;
}

HRESULT DrHdfsOutputStream::Open(DrNativeString streamUri)
{
    m_baseUri = streamUri;
    m_numParts = -1;


#ifdef _MANAGED
    try
    {
        DrLogI("Opening Hdfs for output node");
        m_hdfsInstance = GetHdfsServiceInstance(m_baseUri);

        DrLogI("Checking for Hdfs existence");
        if (m_hdfsInstance->IsFileExists(m_baseUri))
        {
            DrString u(m_baseUri);
            DrLogW("Won't open %s to write since it already exists", u.GetChars());
            m_error = "Fileset exists: " + m_baseUri;
            return DrError_FailedToCreateFileset;
        }

        String^ tmpDir = m_baseUri + "-tmp";
        DrLogI("Trying to delete Hdfs temp directory");
        m_hdfsInstance->DeleteFile(tmpDir, true);
    }
    catch (System::Exception ^e)
    {
        m_error = e->ToString();
        return System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }
#else 
    bool ret = HdfsBridgeNative::Initialize();
    if (!ret)
    {
        DrLogE("Error calling HdfsBridgeNative::Initialize()");
        return E_FAIL;
    }

    m_hdfsInstance = GetHdfsServiceInstance(streamUri);
    if (m_hdfsInstance == NULL)
    {
        DrLogE("Error calling GetHdfsServiceInstance(streamUri)");
        return E_FAIL;
    }
#endif

    return S_OK;
}

DrNativeString DrHdfsOutputStream::GetError()
{
#ifdef _MANAGED
    return m_error;
#else
    return m_error.GetString();
#endif
}

void DrHdfsOutputStream::SetNumberOfParts(int numberOfParts)
{
    // For now, assume that the number of parts cannot change
    DrAssert(m_numParts == -1);
    DrAssert(m_hdfsInstance != DrNull);

    m_numParts = numberOfParts;
}

DrString DrHdfsOutputStream::GetURIForWrite(int partitionIndex, 
                                            int /* id*/, 
                                            int version, 
                                            int /* outputPort*/,
                                            DrResourcePtr /*runningResource*/,
                                            DrMetaDataRef /*metaData */)
{
    DrAssert(m_hdfsInstance != DrNull);
    String^ fileName = m_baseUri + "-tmp/part-" + partitionIndex.ToString("D8") + "." + version;
	
	//DrLogI("HDFS GetURIForWrite returning '%s'", fileName);  // DCF HDFS debug

    return DrString(fileName);
}

void DrHdfsOutputStream::DiscardUnusedPart(int partitionIndex, 
                                           int id, 
                                           int version, 
                                           int outputPort,
                                           DrResourcePtr runningResource,
                                           bool jobSuccess)
{
    DrAssert(m_hdfsInstance != DrNull);

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

#ifdef _MANAGED
    bool deleted = m_hdfsInstance->DeleteFile(uriString.GetString(), false);
#else 
  
    DrString path = FromInternalUri(m_baseUri, uriString);
    InstanceAccessor ia(m_hdfsInstance);
    bool deleted = false;
    ia.DeleteFileOrDir((char *) path.GetChars(), false, &deleted); 
#endif

    DrLogI(
        "HDFS deleting failed version %s: %s",
        uriString.GetChars(), (deleted) ? "succeeded" : "failed"
        );
}

HRESULT DrHdfsOutputStream::FinalizeSuccessfulParts(DrOutputPartitionArrayRef partitionArray, DrStringR errorText)
{
    DrAssert(m_numParts == partitionArray->Allocated());
    DrAssert(m_hdfsInstance != DrNull);

#ifdef _MANAGED
    String^ srcUri = m_baseUri + "-tmp";
    HdfsFileInfo^ directoryInfo = m_hdfsInstance->GetFileInfo(srcUri, false);

    if (directoryInfo == DrNull)
    {
        DrString drSrc(srcUri);
        DrLogE("Can't read %s finalizing HDFS output", drSrc.GetChars());
        m_error = "Can't read " + srcUri + " finalizing HDFS output";
        errorText.SetF("%s", DrString(m_error).GetChars());
        return E_FAIL;
    }

    if (directoryInfo->fileNameArray->Length == m_numParts)
    {
        bool renamed = m_hdfsInstance->RenameFile(m_baseUri, srcUri);
        if (!renamed)
        {
            DrString drSrc(srcUri);
            DrString drDst(m_baseUri);
            DrLogE("Can't rename %s to %s finalizing HDFS output",
                drSrc.GetChars(), drDst.GetChars());
            m_error = "Can't rename " + srcUri + " to " + m_baseUri + " finalizing HDFS output";
            errorText.SetF("%s", DrString(m_error).GetChars());
            return E_FAIL;
        }

        String^ userName = Environment::GetEnvironmentVariable("USER");
        if (userName == nullptr)
        {
            userName = Environment::UserName;
        }
        try
        {
            m_hdfsInstance->SetOwnerAndPermission(m_baseUri, userName, nullptr, Convert::ToInt16("0755", 8));
        }
        catch (Exception^ e)
        {
            DrString drDst(m_baseUri);
            DrString err(e->ToString());
            DrLogE("Can't set %s permissions finalizing HDFS output: %s", drDst.GetChars(), err.GetChars());
            m_error = "Can't set " + m_baseUri + " permissions finalizing HDFS output: " + e->ToString();
            errorText.SetF("%s", DrString(m_error).GetChars());
            return E_FAIL;
        }
    }
    else
    {
        DrString drSrc(srcUri);
        DrLogE("Won't rename %s: should contain %d files, but has %d",
            drSrc.GetChars(), m_numParts, directoryInfo->fileNameArray->Length);
        m_error = "Won't rename " + srcUri + ": should contain " + m_numParts + " files but has " + directoryInfo->fileNameArray->Length;
        errorText.SetF("%s", DrString(m_error).GetChars());
        return E_FAIL;
    }
#else 
    DrString srcUri(m_baseUri);
    srcUri.AppendF("-tmp");
    DrString srcPath = FromInternalUri(m_baseUri, srcUri);

    InstanceAccessor ia(m_hdfsInstance);
    FileStat* fs;
    bool ret = ia.OpenFileStat(srcPath.GetChars(), false, &fs);
    if (!ret)
    {
        char* msg = ia.GetExceptionMessage();
        DrLogE(msg);
        free(msg);
        return E_FAIL;
    }
    FileStatAccessor directoryInfo(fs);
    if (directoryInfo.GetNumberOfFiles() == m_numParts)
    {
        DrString dstPath = FromInternalUri(m_baseUri, m_baseUri);

        bool renamed = false;
        ia.RenameFileOrDir((char *)dstPath.GetChars(), (char *)srcPath.GetChars(), &renamed);
        if (!renamed)
        {
            DrString drSrc(srcPath);
            DrString drDst(dstPath);
            DrLogE("Can't rename %s to %s finalizing HDFS output",
                drSrc.GetChars(), drDst.GetChars());
            return E_FAIL;
        }
    } 
    else 
    {
        DrString drSrc(srcPath);
        DrLogE("Won't rename %s: should contain %d files, but has %d",
            drSrc.GetChars(), m_numParts, directoryInfo.GetNumberOfFiles());
        return E_FAIL;
    }
#endif

    return S_OK;
}

HRESULT DrHdfsOutputStream::DiscardFailedStream(DrStringR errorText)
{
    DrAssert(m_hdfsInstance != DrNull);
    String^ fileName = m_baseUri + "-tmp";
    try
    {
        bool deleted = m_hdfsInstance->DeleteFile(fileName, true);
        if (deleted)
        {
            return S_OK;
        }
        else
        {
            errorText.SetF("Unknown error deleting HDFS directory %s", DrString(fileName).GetChars());
            return DrError_FailedToDeleteFileset;
        }
    }
    catch (System::Exception^ e)
    {
        errorText.SetF("Exception deleting HDFS directory %s: %s", DrString(fileName).GetChars(), DrString(e->ToString()).GetChars());
        return DrError_FailedToDeleteFileset;
    }
}

void DrHdfsOutputStream::ExtendLease(DrTimeInterval /*lease*/) 
{
    /* nothing to do here */
}
