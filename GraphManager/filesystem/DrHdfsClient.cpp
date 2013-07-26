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
using namespace System::Runtime::InteropServices;
#else
using namespace HdfsBridgeNative;
#include <Wininet.h>
#endif

/* Returns 'name' from a stream URI of the form hpchdfs://server:port/name */

#ifdef _MANAGED
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
    return DrNew HdfsInstance(HdfsUri);
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

HRESULT DrHdfsInputStream::Open(DrUniversePtr universe, DrNativeString streamUri)
{
    DrString uri = DrString(streamUri);

    return OpenInternal(universe, uri);
}


HRESULT DrHdfsInputStream::OpenInternal(DrUniversePtr universe, DrString streamUri)
{
    m_streamUri = streamUri;
    HRESULT err = S_OK;

#ifdef _MANAGED

    try 
    {
#endif
      

        m_hdfsInstance = GetHdfsServiceInstance(streamUri);
        
#ifdef _MANAGED
        String ^StreamName = m_hdfsInstance->FromInternalUri(streamUri.GetString());
        HdfsFileInfo^ stream = m_hdfsInstance->GetFileInfo(StreamName, true);
        m_fileNameArray = stream->fileNameArray;
        UInt32 totalPartitionCount = static_cast<UInt32>(stream->blockArray->Length);

#else 
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
#endif
      
        /* Allocate these arrays even if they're size 0, to avoid
        NullReferenceException later */
        m_affinity = DrNew DrAffinityArray(totalPartitionCount); 
        m_partOffsets = DrNew DrUINT64Array(totalPartitionCount);
        m_partFileIds = DrNew DrUINT32Array(totalPartitionCount);

        for (UINT32 i=0; i<totalPartitionCount; ++i)
        {
#ifdef _MANAGED
            HdfsBlockInfo^ partition = stream->blockArray[i];
#else 
            HdfsBridgeNative::HdfsBlockLocInfo* partition = fs.GetBlockInfo(i);
#endif
            m_affinity[i] = DrNew DrAffinity();
            m_affinity[i]->SetWeight(partition->Size);
            m_partOffsets[i] = partition->Offset;
            m_partFileIds[i] = partition->fileIndex;

#ifdef _MANAGED
            for (int j = 0; j < partition->Hosts->Length; ++j)
#else 
            for (int j = 0; j < partition->numberOfHosts; ++j)
#endif
            {
                DrResourceRef location = universe->LookUpResource(partition->Hosts[j]);
                if (location != DrNull)
                {
                    m_affinity[i]->AddLocality(location);
                }
            }
#ifndef _MANAGED
            delete partition;
#endif
        }   
#ifdef _MANAGED
    }
    catch (System::Exception ^e)
    {
        err = System::Runtime::InteropServices::Marshal::GetHRForException(e);
    }
    finally
    {
        // TODO: How do we clean this up?
        //hdfsInstance->Dispose();
    }
#endif

    return err;
}

DrString DrHdfsInputStream::GetStreamName() 
{
    return m_streamUri;
}

int DrHdfsInputStream::GetNumberOfPartitions() 
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
    Uri ^HdfsServiceUri = DrNew Uri(HdfsStreamUri);
    String ^HdfsPartitionUri =
        String::Format("hpchdfspt://{0}:{1}/{2}?{3}?{4}",
        HdfsServiceUri->Host,
        HdfsServiceUri->Port,
        m_fileNameArray[m_partFileIds[partitionIndex]],
        m_partOffsets[partitionIndex],
        m_affinity[partitionIndex]->GetWeight());
    uri.Set(HdfsPartitionUri);
#else
        uri.SetF("hpchdfspt://%s:%d/%s?%I64u?%I64u", m_hostname, m_portNum, 
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
        m_hdfsInstance = GetHdfsServiceInstance(m_baseUri);
    }
    catch (System::Exception ^e)
    {
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

void DrHdfsOutputStream::SetNumberOfPartitions(int numberOfPartitions)
{
    // For now, assume that the number of partitions cannot change
    DrAssert(m_numParts == -1);
    DrAssert(m_hdfsInstance != DrNull);

    m_numParts = numberOfPartitions;
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

void DrHdfsOutputStream::DiscardUnusedPartition(int partitionIndex, 
                                                int id, 
                                                int version, 
                                                int outputPort,
                                                DrResourcePtr runningResource)
{
    DrAssert(m_hdfsInstance != DrNull);

    /* delete the partition if it has been created */
    DrString uriString = GetURIForWrite(
        partitionIndex,
        id,
        version,
        outputPort,
        runningResource,
        DrNull);

#ifdef _MANAGED
    String^ path = m_hdfsInstance->FromInternalUri(uriString.GetString());
    bool deleted = m_hdfsInstance->DeleteFile(path, false);
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

HRESULT DrHdfsOutputStream::FinalizeSuccessfulPartitions(DrOutputPartitionArrayRef partitionArray)
{
    DrAssert(m_numParts == partitionArray->Allocated());
    DrAssert(m_hdfsInstance != DrNull);

#ifdef _MANAGED
    String^ srcUri = m_baseUri + "-tmp";
    String^ srcPath = m_hdfsInstance->FromInternalUri(srcUri);
    HdfsFileInfo^ directoryInfo = m_hdfsInstance->GetFileInfo(srcPath, false);

    if (directoryInfo == DrNull)
    {
        DrString drSrc(srcPath);
        DrLogE("Can't read %s finalizing HDFS output",
            drSrc.GetChars());
        return E_FAIL;
    }

    if (directoryInfo->fileNameArray->Length == m_numParts)
    {
        String^ dstPath = m_hdfsInstance->FromInternalUri(m_baseUri);

        bool renamed = m_hdfsInstance->RenameFile(dstPath, srcPath);
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
            drSrc.GetChars(), m_numParts, directoryInfo->fileNameArray->Length);
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

void DrHdfsOutputStream::ExtendLease(DrTimeInterval /*lease*/) 
{
    /* nothing to do here */
}
