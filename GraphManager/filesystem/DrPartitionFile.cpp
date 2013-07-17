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

static DrString ReadLineFromFile(FILE* f)
{
    DrString line("");

    char buf[1024];
    char* s;
    bool found = false;
    bool foundEndOfLine = false;

    while ((s = fgets(buf, sizeof(buf)-1, f)) != NULL)
    {
        found = true;
        size_t sLen;

        buf[sizeof(buf)-1] = '\0';

        sLen = ::strlen(buf);

        if (sLen > 0 && buf[sLen-1] == '\n')
        {
            --sLen;
            buf[sLen] = '\0';
            foundEndOfLine = true;
        }
        if (sLen > 0 && buf[sLen-1] == '\r')
        {
            --sLen;
            buf[sLen] = '\0';
            foundEndOfLine = true;
        }

        line = line.AppendF("%s", buf);
        if (foundEndOfLine)
        {
            break;
        }
    }

    if (found)
    {
        return line;
    }
    else
    {
        DrString nullString;
        return nullString;
    }
}

static bool ParseReplicatedFromPartitionLine(int partitionNumber,
                                             DrAffinityPtr affinity,
                                             DrStringR remoteName,
                                             DrPartitionInputStream::OverridePtr over,
                                             bool mustOverride,
                                             DrString line,
                                             DrUniversePtr universe)
{
    DrString lineCopy = line;

    int sep = lineCopy.IndexOfChar(',');
    if (sep == DrStr_InvalidIndex)
    {
        return false;
    }
    else
    {
        DrString partitionNumberString;
        partitionNumberString.SetSubString(lineCopy.GetChars(), sep);
        DrString shorter;
        shorter.SetF("%s", lineCopy.GetChars() + sep + 1);
        lineCopy = shorter;

        int parsedNumber;
        int n = sscanf_s(partitionNumberString.GetChars(), "%d", &parsedNumber);
        if (n != 1 || parsedNumber != partitionNumber)
        {
            DrLogW("Mismatched partition numbers in line %s: Expected %d got %d",
                   line.GetChars(), partitionNumber, parsedNumber);
            return false;
        }
    }

    UINT64 parsedSize = 0;

    sep = lineCopy.IndexOfChar(',');
    if (sep == DrStr_InvalidIndex)
    {
        DrLogW("Malformed line %s: no list of machines", line.GetChars());
        return false;
    }
    else
    {
        DrString partitionSizeString;
        partitionSizeString.SetSubString(lineCopy.GetChars(), sep);
        DrString shorter;
        shorter.SetF("%s", lineCopy.GetChars() + sep + 1);
        lineCopy = shorter;

        int n = sscanf_s(partitionSizeString.GetChars(), "%I64u", &parsedSize);
        if (n != 1)
        {
            DrLogW("Malformed line %s: can't parse size", line.GetChars());
            return false;
        }

        affinity->SetWeight(parsedSize);
    }

    if (lineCopy.GetCharsLength() == 0)
    {
        DrLogW("Malformed line %s: no partition machines", line.GetChars());
        return false;
    }

    int numberOfReplicas = 0;
    while (lineCopy.GetCharsLength() > 0)
    {
        DrString thisMachineName;

        sep = lineCopy.IndexOfChar(',');
        if (sep == DrStr_InvalidIndex)
        {
            thisMachineName = lineCopy;
            lineCopy = DrString("");
        }
        else
        {
            thisMachineName.SetSubString(lineCopy.GetChars(), sep);
            DrString shorter;
            shorter.SetF("%s", lineCopy.GetChars() + sep + 1);
            lineCopy = shorter;
        }

        sep = thisMachineName.IndexOfChar(':');
        if (sep == DrStr_InvalidIndex)
        {
            thisMachineName = thisMachineName.ToUpperCase();
        }
        else
        {
            DrString overrideFile;
            overrideFile.SetF("%s", thisMachineName.GetChars() + sep + 1);
            DrString shorter;
            shorter.SetSubString(thisMachineName.GetChars(), sep);
            thisMachineName = shorter.ToUpperCase();

            over->Add(thisMachineName.GetString(), overrideFile);
        }

        DrResourceRef location = universe->LookUpResourceInternal(thisMachineName);
        if (location == DrNull)
        {
            remoteName.Set(thisMachineName);
        }
        else
        {
            affinity->AddLocality(location);
        }

        ++numberOfReplicas;
    }

    if (mustOverride && over->GetSize() == 0)
    {
        DrLogW("Malformed partition file: All filenames must be overrides when path is empty");
        return false;
    }

    return true;
}

HRESULT DrPartitionInputStream::Open(DrUniversePtr universe, DrNativeString streamName)
{
    return OpenInternal(universe, DrString(streamName));
}

HRESULT DrPartitionInputStream::OpenInternal(DrUniversePtr universe, DrString streamName)
{
    HRESULT err = S_OK;

    FILE* f;
    errno_t ferr = fopen_s(&f, streamName.GetChars(), "rb");
    if (ferr != 0)
    {
        err = HRESULT_FROM_WIN32(GetLastError());
        DrLogW("Failed to open input file %s error %s", streamName.GetChars(), DRERRORSTRING(err));
        return err;
    }

    m_pathNameOnComputer = ReadLineFromFile(f);
    DrString partitionSizeLine = ReadLineFromFile(f);

    if (partitionSizeLine.GetString() == DrNull)
    {
        err = DrError_EndOfStream;
        DrLogW("Failed to read pathname and partition size from input file %s", streamName.GetChars());
        fclose(f);
        return err;
    }

    bool mustOverride = false;
    if (m_pathNameOnComputer.GetCharsLength() == 0)
    {
        mustOverride = true;
    }

    int numberOfPartitions;
    int n = sscanf_s(partitionSizeLine.GetChars(), "%d", &numberOfPartitions);
    if (n != 1)
    {
        DrLogW("Unable to read partition size from line '%s' Filename %s",
            partitionSizeLine.GetChars(), streamName.GetChars());
        fclose(f);
        return DrError_Unexpected;
    }

    if (numberOfPartitions == 0)
    {
        DrLogI("Read empty partitioned file details PathName: '%s'",
               m_pathNameOnComputer.GetChars());
        fclose(f);
        return S_OK;
    }

    m_affinity = DrNew DrAffinityArray(numberOfPartitions);
    m_remoteName = DrNew DrStringArray(numberOfPartitions);
    m_override = DrNew OverrideArray(numberOfPartitions);

    DrLogI("Reading partitioned file details PathName: '%s' NumberOfPartitions=%d",
           m_pathNameOnComputer.GetChars(), numberOfPartitions);

    int i;
    for (i=0; i<numberOfPartitions; ++i)
    {
        DrString partitionLine = ReadLineFromFile(f);
        if (partitionLine.GetString() == DrNull)
        {
            DrLogW("Malformed partition file %s: got %d partition lines; expected %d",
                   streamName.GetChars(), i, numberOfPartitions);
            m_affinity = DrNull;
            m_remoteName = DrNull;
            m_override = DrNull;
            fclose(f);
            return DrError_Unexpected;
        }


        m_affinity[i] = DrNew DrAffinity();
        m_override[i] = DrNew Override();
        DrString remoteName;
        if (ParseReplicatedFromPartitionLine(i,
                                             m_affinity[i],
                                             remoteName,
                                             m_override[i],
                                             mustOverride,
                                             partitionLine,
                                             universe) == false)
        {
            DrLogW("Failed to parse partition file line from file %s, partition %d, line='%s'",
                   streamName.GetChars(), i, partitionLine.GetChars());
            m_affinity = DrNull;
            m_remoteName = DrNull;
            m_override = DrNull;
            fclose(f);
            return DrError_Unexpected;
        }

        m_remoteName[i] = remoteName;
    }

    fclose(f);

    m_streamName = streamName;

    return S_OK;
}

DrString DrPartitionInputStream::GetStreamName()
{
    return m_streamName;
}

int DrPartitionInputStream::GetNumberOfPartitions()
{
    return m_affinity->Allocated();
}

DrAffinityRef DrPartitionInputStream::GetAffinity(int partitionIndex)
{
    return m_affinity[partitionIndex];
}

DrString DrPartitionInputStream::GetURIForRead(int partitionIndex, DrResourcePtr runningResource)
{
    OverridePtr over = m_override[partitionIndex];
    DrAffinityPtr affinity = m_affinity[partitionIndex];
    DrResourceListRef location = affinity->GetLocalityArray();

    DrString computerName;

    if (location->Size() == 0)
    {
        computerName = m_remoteName[partitionIndex];
    }
    else
    {
        DrResourcePtr resource = DrNull;

        int i;
        for (i=0; i<location->Size(); ++i)
        {
            if (location[i] == runningResource)
            {
                resource = location[i];
                break;
            }
        }

        if (resource == DrNull)
        {
            for (i=0; i<location->Size(); ++i)
            {
                if (location[i]->GetParent() == runningResource->GetParent())
                {
                    resource = location[i];
                    break;
                }
            }

            if (resource == DrNull)
            {
                resource = location[rand() % location->Size()];
            }
        }

        computerName = resource->GetName();
    }

    DrString uri;

    DrString overrideString;
    if (over->TryGetValue(computerName.GetString(), overrideString))
    {
        uri.SetF("file://\\\\%s\\%s", computerName.GetChars(), overrideString.GetChars());
    }
    else
    {
        uri.SetF("file://\\\\%s\\%s.%08x",
                  computerName.GetChars(), m_pathNameOnComputer.GetChars(), partitionIndex);
    }

    return uri;
}

HRESULT DrPartitionOutputStream::Open(DrNativeString streamName, DrNativeString pathBase)
{
    return OpenInternal(DrString(streamName), DrString(pathBase));
}

HRESULT DrPartitionOutputStream::OpenInternal(DrString streamName, DrString pathBase)
{
    FILE* f;
    errno_t ferr = fopen_s(&f, streamName.GetChars(), "w");
    if (ferr != 0)
    {
        HRESULT err = HRESULT_FROM_WIN32(GetLastError());
        DrLogW("Failed to open output file %s error %s", streamName.GetChars(), DRERRORSTRING(err));
        return err;
    }
    fclose(f);

    m_streamName = streamName;
    m_pathBase = pathBase;

    return S_OK;
}

void DrPartitionOutputStream::SetNumberOfPartitions(int /* unused numberOfPartitions*/)
{
}

DrString DrPartitionOutputStream::GetURIForWrite(int partitionIndex,
                                                 int id, int version, int outputPort,
                                                 DrResourcePtr runningResource,
                                                 DrMetaDataRef metaData)
{
    DrString uri;
    uri.SetF("file://\\\\%s\\%s.%08x---%d_%d_%d.tmp",
              runningResource->GetName().GetChars(),
              m_pathBase.GetChars(), partitionIndex, id, outputPort, version);

    DrMTagVoidRef tag = DrNew DrMTagVoid(DrProp_TryToCreateChannelPath);
    metaData->Append(tag);

    return uri;
}

void DrPartitionOutputStream::DiscardUnusedPartition(int partitionIndex,
                                                     int id, int version, int outputPort,
                                                     DrResourcePtr runningResource)
{
    DrMetaDataRef metaData = DrNew DrMetaData();
    DrString uri = GetURIForWrite(partitionIndex, id, version, outputPort, runningResource, metaData);
    BOOL bRet = ::DeleteFileA(uri.GetChars() + 7);
    if (!bRet)
    {
        HRESULT err = HRESULT_FROM_WIN32(GetLastError());
        DrAssert(err != S_OK);

        if (err == HRESULT_FROM_WIN32(ERROR_FILE_NOT_FOUND))
        {
            DrLogI("Delete ignoring nonexistent URI %s", uri.GetChars());
        }
        else
        {
            DrLogW("DeleteFile(%s), error %s", uri.GetChars() + 7, DRERRORSTRING(err));
        }
    }
    else
    {
        DrLogI("Deleted URI %s", uri.GetChars());
    }
}

HRESULT DrPartitionOutputStream::RenameSuccessfulPartition(int partitionIndex, DrOutputPartition p)
{
    DrMetaDataRef metaData = DrNew DrMetaData();
    DrString uri = GetURIForWrite(partitionIndex, p.m_id,
                                  p.m_version, p.m_outputPort, p.m_resource,
                                  metaData);

    DrString finalName;
    finalName.SetF("\\\\%s\\%s.%08x",
                   p.m_resource->GetName().GetChars(),
                   m_pathBase.GetChars(), partitionIndex);

    HRESULT err = S_OK;

    BOOL bRet = ::MoveFileA(uri.GetChars()+7, finalName.GetChars());
    if (!bRet)
    {
        err = HRESULT_FROM_WIN32(GetLastError());
        DrAssert(err != S_OK);

        DrLogW("MoveFile(%s, %s), error %s", uri.GetChars()+7, finalName.GetChars(), DRERRORSTRING(err));
    }
    else
    {
        DrLogI("Renamed Native URI %s -> %s", uri.GetChars(), finalName.GetChars());
    }

    return err;
}

HRESULT DrPartitionOutputStream::FinalizeSuccessfulPartitions(DrOutputPartitionArrayRef partitionArray)
{
    HRESULT err = S_OK;

    if (!SUCCEEDED(err))
    {
        return err;
    }

    FILE* f;
    errno_t ferr = fopen_s(&f, m_streamName.GetChars(), "w");
    if (ferr != 0)
    {
        HRESULT err = HRESULT_FROM_WIN32(GetLastError());
        DrLogW("Failed to open output file %s error %s", m_streamName.GetChars(), DRERRORSTRING(err));
        return err;
    }

    fprintf(f, "%s\n%d\n", m_pathBase.GetChars(), partitionArray->Allocated());
    int i;
    for (i=0; i<partitionArray->Allocated(); ++i)
    {
        DrOutputPartition p = partitionArray[i];

        HRESULT thisErr = RenameSuccessfulPartition(i, p);
        if (err == S_OK && !SUCCEEDED(thisErr))
        {
            err = thisErr;
        }

        fprintf(f, "%d,%I64u,%s\n", i, p.m_size, p.m_resource->GetName().GetChars());
    }

    fclose(f);

    return err;
}

void DrPartitionOutputStream::ExtendLease(DrTimeInterval)
{
}

