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

#include <concreterchannel.h>
#include <channelfifo.h>
#include <channelbuffernativereader.h>
#include <channelbuffernativewriter.h>
#include <dryadstandaloneini.h>
#include <dryadmetadata.h>
#include <workqueue.h>
#include <concreterchannelhelpers.h>
#include <channelbufferhdfs.h>
#ifdef TIDYFS
#include <mdclient.h>
#endif

#pragma once
#pragma warning(disable:4512)
#pragma warning(disable:4511)
#include <string>
#include <map>

#pragma unmanaged


static const char* s_filePrefix = "file://";
static const char* s_fifoPrefix = "fifo://";
static const char* s_nullPrefix = "null://";
static const char* s_tidyfsPrefix = "tidyfs://";
static const char* s_dscStreamPrefix = "hpcdsc://";
static const char* s_dscPartitionPrefix = "hpcdscpt://";
static const char* s_azureBlobPrefix = "http://"; 

//
// Use 6 to match up with retry count used between GM and VS
//
static const int s_dscRetryMax = 6;

//
// Check if vertex host running on Azure node
//
static bool IsAzure()
{
    WCHAR buf[MAX_PATH];
    ZeroMemory(buf, sizeof(buf));

    int nSchedulerTypeLen = ::GetEnvironmentVariable(L"CCP_SCHEDULERTYPE", buf, _countof(buf));
    if (_wcsicmp(buf,L"AZURE") == 0)
    {
        return true;
    }

    ZeroMemory(buf, sizeof(buf));
    ::GetEnvironmentVariable(L"DEBUG_AZURE", buf, _countof(buf));
    if (nSchedulerTypeLen != 0 && _wcsicmp(buf, L"0") != 0)
    {
        return true;
    }
    return false;
}

//
// Check if channel URI is reading from an on-premise NTFS file
//
bool ConcreteRChannel::IsNTFSFile(const char* uri)
{
    size_t prefixLen = ::strlen(s_filePrefix);
    size_t uriLen = ::strlen(uri);

    if (uriLen < prefixLen + 2)
    {
        return false;
    }

    if (_strnicmp(uri, s_filePrefix, ::strlen(s_filePrefix)) == 0)
    {
        if (!IsAzure() || uri[prefixLen] != '\\' || uri[prefixLen+1] != '\\')
        {
            return true;
        }
    }
    return false;

}


//
// Check if channel URI is a DSC partition by comparing the prefix to hpcdscpt://
//
bool ConcreteRChannel::IsDscPartition(const char* uri)
{
    return (_strnicmp(uri, s_dscPartitionPrefix, ::strlen(s_dscPartitionPrefix)) == 0);
}

//
// Check if channel URI is a HDFS file by comparing the prefix to hpchdfs://
//
bool ConcreteRChannel::IsHdfsFile(const char* uri)
{
    return (_strnicmp(uri,
                      RChannelBufferHdfsWriter::s_hdfsFilePrefix,
                      ::strlen(RChannelBufferHdfsWriter::
                               s_hdfsFilePrefix)) == 0);
}

//
// Check if channel URI is a HDFS partition by comparing the prefix to hpchdfspt://
//
bool ConcreteRChannel::IsHdfsPartition(const char* uri)
{
    return (_strnicmp(uri,
                      RChannelBufferHdfsReader::s_hdfsPartitionPrefix,
                      ::strlen(RChannelBufferHdfsReader::
                               s_hdfsPartitionPrefix)) == 0);
}

//
// Check if the channel URI is an Azure blob by comparing the prefix to http://
//
bool ConcreteRChannel::IsAzureBlob(const char* uri)
{
    return (_strnicmp(uri, s_azureBlobPrefix, ::strlen(s_azureBlobPrefix)) == 0);
}

//
// Check if the channel URI is a UNC path in Azure
//
bool ConcreteRChannel::IsUncPath(const char* uri)
{
    size_t prefixLen = ::strlen(s_filePrefix);
    size_t uriLen = ::strlen(uri);

    if (uriLen < prefixLen + 2)
    {
        return false;
    }

    if (_strnicmp(uri, s_filePrefix, ::strlen(s_filePrefix)) == 0)
    {
        if (IsAzure() && uri[prefixLen] == '\\' && uri[prefixLen+1] == '\\')
        {
            return true;
        }
    }
    return false;
}
//
// Check if the channel URI is a DSC stream by comparing prefix to hpcdsc://
//
bool ConcreteRChannel::IsDscStream(const char* uri)
{
    return (_strnicmp(uri, s_dscStreamPrefix, ::strlen(s_dscStreamPrefix)) == 0);
}

//
// Check if the channel URI is a fifo by comparing prefix to fifo://
//
bool ConcreteRChannel::IsFifo(const char* uri)
{
    return (_strnicmp(uri, s_fifoPrefix, ::strlen(s_fifoPrefix)) == 0);
}

//
// Check if the channel URI is a null channel by comparing prefix to null://
//
bool ConcreteRChannel::IsNull(const char* uri)
{
    return (_strnicmp(uri, s_nullPrefix, ::strlen(s_nullPrefix)) == 0);
}

//
// Check if channel URI is a named pipe by comparing prefix to \pipe\
//
bool ConcreteRChannel::IsNamedPipe(const char* uri)
{
    // uri is of form <\\CompName\pipe\>
    const char* pipe = ::strstr(uri, "\\pipe\\");
    if(pipe)
    {
        while(pipe != uri)
        {
            --pipe;
            if(*pipe == '\\')
            {
                return (--pipe == uri);
            }
        }
    }

    return false;
}

//
// Check if channel URI is a tidyfs stream by comparing prefix to tidyfs://
//
bool ConcreteRChannel::IsTidyFSStream(const char* uri)
{
    return (_strnicmp(uri, s_tidyfsPrefix, ::strlen(s_tidyfsPrefix)) == 0);
}

//
// Nothing to dispose
// todo: ensure this is correct
//
RChannelThrottledStream::~RChannelThrottledStream()
{
}

//
// Create a new dispatch with provided stream as source
//
RChannelOpenThrottler::Dispatch::Dispatch(RChannelThrottledStream* stream)
{
    m_stream = stream;
}

void RChannelOpenThrottler::Dispatch::Process()
{
    m_stream->OpenAfterThrottle();
}

bool RChannelOpenThrottler::Dispatch::ShouldAbort()
{
    return false;
}

//
// Constructor - Initialize properties (workqueue and max input)
//
RChannelOpenThrottler::RChannelOpenThrottler(UInt32 maxOpenFiles,
                                             WorkQueue* workQueue)
{
    LogAssert(maxOpenFiles > 0);
    m_maxOpenFiles = maxOpenFiles;
    m_workQueue = workQueue;
    m_openFileCount = 0;
}

RChannelOpenThrottler::~RChannelOpenThrottler()
{
    LogAssert(m_openFileCount == 0);
    LogAssert(m_blockedFileList.empty());
}

bool RChannelOpenThrottler::QueueOpen(RChannelThrottledStream* stream)
{
    bool openImmediately = false;

    {
        AutoCriticalSection acs(&m_baseDR);

        if (m_openFileCount < m_maxOpenFiles)
        {
            ++m_openFileCount;
            openImmediately = true;
        }
        else
        {
            LogAssert(m_openFileCount == m_maxOpenFiles);
            m_blockedFileList.push_back(stream);
            DrLogI( "Queueing open. %u files open, %Iu files blocked",
                m_openFileCount, m_blockedFileList.size());
        }
    }

    return openImmediately;
}

//
// Notify throttler that a file is complete and a new one can be opened
//
void RChannelOpenThrottler::NotifyFileCompleted()
{
    Dispatch* dispatch = NULL;

    {
        AutoCriticalSection acs(&m_baseDR);

        LogAssert(m_openFileCount > 0);
        if (m_openFileCount == m_maxOpenFiles &&
            m_blockedFileList.empty() == false)
        {
            //
            // If current throttle is full and there's more files ready to be opened,
            // Open first blocked file
            //
            dispatch = new Dispatch(m_blockedFileList.front());
            m_blockedFileList.pop_front();
            DrLogI( "Unblocking open. %u files open, %Iu files still blocked",
                m_openFileCount, m_blockedFileList.size());
        }
        else
        {
            --m_openFileCount;
        }
    }

    if (dispatch != NULL)
    {
        //
        // If a new file can be opened, add it to the work queue
        //
        bool b = m_workQueue->EnQueue(dispatch);
        LogAssert(b);
    }
}

//
// Create a file reader. Every type of file path eventually ends up here.
//
static RChannelBufferReader*
    CreateNativeFileReader(UInt32 numberOfReaders,
                           RChannelOpenThrottler* openThrottler,
                           WorkQueue* workQueue,
                           const char* fileName,
                           DryadMetaData* metaData,
                           DVErrorReporter* errorReporter,
                           LPDWORD localInputChannels)
{
    UInt32 blockSize = 4*1024;
    UInt32 numberOfBlocksPerBuffer = 64 / numberOfReaders;
    if (numberOfBlocksPerBuffer < 16)
    {
        numberOfBlocksPerBuffer = 16;
    }

    RChannelBufferReaderNativeFile* fileReader =
        new RChannelBufferReaderNativeFile(numberOfBlocksPerBuffer*blockSize,
                                           blockSize, 4,
                                           g_dryadNativePort,
                                           workQueue, openThrottler);
    if (fileReader == NULL)
    {
        return NULL;
    }

    //
    // Open the specified file
    //
    if (!fileReader->OpenA(fileName))
    {
        delete fileReader;

        DrError errorCode = DrGetLastError();
        errorReporter->ReportError(errorCode,
                                   "Can't open native file '%s' to read",
                                   fileName);

        return NULL;
    }

    //
    // If counting local input channels, proceed with testing
    // this is set to null in the case of Azure and in-memory fifo sources
    //
    if(localInputChannels != NULL)
    {
        DrStr myFilePath(fileName);
        if(!myFilePath.StartsWith("\\\\"))
        {
            //
            // If path doesnt start with "\\", then it must be local
            //
            (*localInputChannels)++;
        }
        else
        {
            //
            // Otherwise, check host name to see if it's local
            //

            // replace below to enable DNS hostname support
            //DWORD hostLength = DNS_MAX_LABEL_BUFFER_LENGTH;
            //char hostname[DNS_MAX_LABEL_BUFFER_LENGTH];
            //if (!GetComputerNameExA(ComputerNameDnsHostname, hostname, &hostLength)) 

            DWORD hostLength = MAX_COMPUTERNAME_LENGTH + 1;
            char hostname[MAX_COMPUTERNAME_LENGTH + 1];
            if (!GetComputerNameA(hostname, &hostLength)) 
            {
                DrLogE( "Error calling GetComputerName. File: %s ErrorCode: %u",

                    fileName, GetLastError());
                return NULL;
            }

            DrStr myHostName(hostname);
            
            //
            // Find first index of \ after \\ prefix
            // this should find the end of machine name 
            // should always find an instance of the \ character in valid path (bug if not)
            //
            size_t firstIndexOfSlash = myFilePath.IndexOfChar('\\', 2);
            LogAssert(firstIndexOfSlash != DrStr_InvalidIndex);

            if(firstIndexOfSlash - 2 == myHostName.GetLength())
            {
                if(myFilePath.SubstrIsEqualNoCase(2, myHostName, myHostName.GetLength()))
                {
                    //
                    // If length of host name is same and strings match, then file is local
                    //
                    (*localInputChannels)++;
                }
            }
        }
    }

    return fileReader;
}

//
// Create a HDFS block reader. HDFS read path eventually ends up here.
//
static RChannelBufferReader* CreateHdfsBlockReader(const char* uri)
{
    return new RChannelBufferHdfsReaderLineRecord(uri);
}


static RChannelBufferWriter* CreateHdfsFileWriter(const char* uri)
{
    return new RChannelBufferHdfsWriter(uri);
}


// hide azure related
#if 0
static RChannelBufferReader*
    CreateUncFileReader(UInt32 numberOfReaders,
                        RChannelOpenThrottler* openThrottler,
                        WorkQueue* workQueue,
                        const char* streamName,
                        DryadMetaData* metaData,
                        DVErrorReporter* errorReporter,
                        LPDWORD localInputChannels)
{
    // Ensure that the stream name passed in is really a Azure Blob URL
    LogAssert(ConcreteRChannel::IsUncPath(streamName));

    //
    // Copy the blob locally to a temp file and call
    // CreateNativeFileReader on the temp file.
    //
    char path[MAX_PATH];
    if (GetTempFileNameA(".", "UNC", 0, path) == 0)
    {
        errorReporter->ReportError(E_FAIL, "Error calling GetTempFileName for '%s': %u",
                                   streamName, GetLastError());
        return NULL;
    }
    
    HRESULT err = DscGetNetworkFile(streamName + ::strlen(s_filePrefix), path);
    LogAssert(err == S_OK);
    if (err != S_OK)
    {
        errorReporter->ReportError(err, "Error calling GetTempFileName for '%s': %u",
                                   streamName, GetLastError());
        return NULL;
    }
    
    return CreateNativeFileReader(numberOfReaders, openThrottler, workQueue, path, metaData, errorReporter, localInputChannels);
}

#endif

#ifdef TIDYFS
static RChannelBufferReader*
CreateTidyFSStreamReader(UInt32 numberOfReaders,
                         RChannelOpenThrottler* openThrottler,
                         WorkQueue* workQueue,
                         const char* streamName,
                         DryadMetaData* metaData,
                         DVErrorReporter* errorReporter)
{
    MDClient *client = new MDClient();
    LogAssert(client != NULL);
    DrError result = client->Initialize("rsl.ini");
    if (result != DrError_OK)
    {
        errorReporter->ReportError(result,
            "Error initializing TidyFS client: %u: %s",
            result, GetDrErrorDescription(result));

        return NULL;
    } 
    const char *hostname = Configuration::GetRawMachineName();
    
    char path[2048]; 
    result = client->GetReadPath(path, 2048, streamName, hostname);
    DrLogI( "CreateTidyFSStreamReader", "Stream: %s, Host: %s", streamName, hostname);
    if (result != DrError_OK)
    {
        errorReporter->ReportError(result,
            "Error calling GetReadPath on '%s': %u: %s",
            streamName, result, GetDrErrorDescription(result));
        delete client;
        return NULL;
    } 
    DrLogI( "CreateTidyFSStreamReader", "Path: %s", path);
    delete client;
    return CreateNativeFileReader(numberOfReaders, openThrottler, workQueue, path, metaData, errorReporter);
}
#endif

/* JC
static RChannelBufferReader*
    CreateXComputeFileReader(UInt32 numberOfReaders,
                           RChannelOpenThrottler* openThrottler,
                           WorkQueue* workQueue,
                           const char* fileName,
                           DryadMetaData* metaData,
                           DVErrorReporter* errorReporter)
{
    UInt32 blockSize = 4*1024;
    UInt32 numberOfBlocksPerBuffer = 64 / numberOfReaders;
    if (numberOfBlocksPerBuffer < 16)
    {
        numberOfBlocksPerBuffer = 16;
    }

    RChannelBufferReaderNativeXComputeFile* fileReader =
        new RChannelBufferReaderNativeXComputeFile(numberOfBlocksPerBuffer*blockSize*64,
                                           blockSize, 2,
                                           g_dryadNativePort,
                                           workQueue, openThrottler);
    if (fileReader == NULL)
    {
        return NULL;
    }

    if (!fileReader->OpenA(fileName))
    {
        delete fileReader;

        DrError errorCode = DrGetLastError();
        errorReporter->ReportError(errorCode,
                                   "Can't open xcompute file '%s' to read",
                                   fileName);

        return NULL;
    }

    return fileReader;
}

static RChannelBufferReader*
    CreateDryadStreamReader(UInt32 numberOfReaders,
                             RChannelOpenThrottler* openThrottler,
                             WorkQueue* workQueue,
                             const char* streamName,
                             DryadMetaData* metaData,
                             DVErrorReporter* errorReporter)
{
    RChannelBufferReaderDryadStream* streamReader =
        new RChannelBufferReaderDryadStream(2*1024*1024, 1,
                                             g_dryadNativePort,
                                             workQueue, openThrottler);
    if (streamReader == NULL)
    {
        return NULL;
    }

    DrError cse = streamReader->OpenA(streamName);
    if (cse != DrError_OK)
    {
        delete streamReader;

        errorReporter->ReportError(cse,
                                   "Can't open cosmos stream '%s' to read"
                                   " --- %s",
                                   streamName, DRERRORSTRING(cse));

        return NULL;
    }

    return streamReader;
}

static RChannelBufferReader*
    CreateDryadPipeReader(UInt32 numberOfReaders,
                           const char* pipeName,
                           DryadMetaData* metaData,
                           DVErrorReporter* errorReporter)
{
    RChannelBufferReaderDryadPipe* pipeReader =
        new RChannelBufferReaderDryadPipe(256*1024, 1,
                                           g_dryadNativePort);
    if (pipeReader == NULL)
    {
        return NULL;
    }

    DrError cse = pipeReader->OpenA(pipeName);
    if (cse != DrError_OK)
    {
        delete pipeReader;

        errorReporter->ReportError(cse,
                                   "Can't open cosmos pipe '%s' to read"
                                   " --- %s",
                                   pipeName, DRERRORSTRING(cse));

        return NULL;
    }

    return pipeReader;
}
*/

//
// todo: figure out what and why
//
static RChannelBufferReader*
    CreateNullReader(const char* uri,
                     DryadMetaData* metaData,
                     DVErrorReporter* errorReporter)
{
    return new RChannelNullBufferReader();
}

static RChannelBufferWriter*
    CreateNativeFileWriter(UInt32 numberOfWriters,
                           RChannelOpenThrottler* openThrottler,
                           const char* fileName,
                           DryadMetaData* metaData,
                           bool* pBreakOnBufferBoundaries,
                           DVErrorReporter* errorReporter)
{
    UInt32 blockSize = 4*1024;
    UInt32 numberOfBlocksPerBuffer = 8*64 / numberOfWriters;
    if (numberOfBlocksPerBuffer < 16)
    {
        numberOfBlocksPerBuffer = 16;
    }

    RChannelBufferWriterNativeFile* fileWriter =
        new RChannelBufferWriterNativeFile(numberOfBlocksPerBuffer*blockSize,
                                           blockSize, 2, 6,
                                           g_dryadNativePort, openThrottler);
    if (fileWriter == NULL)
    {
        return NULL;
    }

    DrError cse = fileWriter->SetMetaData(metaData);
    if (cse != DrError_OK)
    {
        delete fileWriter;

        const char* text = metaData->GetText();

        errorReporter->ReportError(cse,
                                   "Can't read native file metadata %s for '%s' to write"
                                   " --- %s",
                                   text, fileName, DRERRORSTRING(cse));

        delete [] text;

        return NULL;
    }

    if (!fileWriter->OpenA(fileName))
    {
        delete fileWriter;

        DrError errorCode = DrGetLastError();
        errorReporter->ReportError(errorCode,
                                   "Can't open native file '%s' to write",
                                   fileName);

        return NULL;
    }

    return fileWriter;
}

/* JC
static RChannelBufferWriter*
    CreateDryadStreamWriter(UInt32 numberOfWriters,
                             RChannelOpenThrottler* openThrottler,
                             const char* streamName,
                             DryadMetaData* metaData,
                             bool* pBreakOnBufferBoundaries,
                             DVErrorReporter* errorReporter)
{
    RChannelBufferWriterDryadStream* streamWriter =
        new RChannelBufferWriterDryadStream(2*1024*1024, 0, 1,
                                             g_dryadNativePort, openThrottler);
    if (streamWriter == NULL)
    {
        return NULL;
    }

    DrError cse = streamWriter->SetMetaData(metaData);
    if (cse != DrError_OK)
    {
        delete streamWriter;

        const char* text = metaData->GetText();

        errorReporter->ReportError(cse,
                                   "Can't read cosmos stream metadata %s for '%s' to write"
                                   " --- %s",
                                   text, streamName, DRERRORSTRING(cse));

        delete [] text;

        return NULL;
    }

    cse = streamWriter->OpenA(streamName);
    if (cse != DrError_OK)
    {
        delete streamWriter;

        errorReporter->ReportError(cse,
                                   "Can't open cosmos stream '%s' to write"
                                   " --- %s",
                                   streamName, DRERRORSTRING(cse));

        return NULL;
    }

    *pBreakOnBufferBoundaries = true;

    return streamWriter;
}
*/


#ifdef TIDYFS
static RChannelBufferWriter*
CreateTidyFSStreamWriter(UInt32 numberOfWriters,
                         RChannelOpenThrottler* openThrottler,
                         const char* streamName,
                         DryadMetaData* metaData,
                         bool* pBreakOnBufferBoundaries,
                         DVErrorReporter* errorReporter)
{
    UInt32 blockSize = 4*1024;
    UInt32 numberOfBlocksPerBuffer = 64 / numberOfWriters;
    if (numberOfBlocksPerBuffer < 16)
    {
        numberOfBlocksPerBuffer = 16;
    }

    RChannelBufferWriterNativeTidyFSStream *tidyFSWriter = 
        new RChannelBufferWriterNativeTidyFSStream(numberOfBlocksPerBuffer*blockSize,
        blockSize, 2, 6,
        g_dryadNativePort, openThrottler);
    if (tidyFSWriter == NULL)
    {
        return NULL;
    }

    DrError cse = tidyFSWriter->OpenA(streamName, metaData);
    if (cse != DrError_OK)
    {
        delete tidyFSWriter;

        errorReporter->ReportError(cse,
            "Can't open TidyFS stream '%s' to write"
            " --- %s",
            streamName, DRERRORSTRING(cse));

        return NULL;
    }

    return tidyFSWriter;
}
#endif

/* JC
static RChannelBufferWriter*
    CreateDryadPipeWriter(UInt32 numberOfWriters,
                           const char* pipeName,
                           DryadMetaData* metaData,
                           bool* pBreakOnBufferBoundaries,
                           DVErrorReporter* errorReporter)
{
    RChannelBufferWriterDryadPipe* pipeWriter =
        new RChannelBufferWriterDryadPipe(256*1024, 0, 1,
                                           g_dryadNativePort);
    if (pipeWriter == NULL)
    {
        return NULL;
    }

    DrError cse = pipeWriter->OpenA(pipeName);
    if (cse != DrError_OK)
    {
        delete pipeWriter;

        errorReporter->ReportError(cse,
                                   "Can't open cosmos pipe '%s' to write"
                                   " --- %s",
                                   pipeName, DRERRORSTRING(cse));

        return NULL;
    }

    return pipeWriter;
}
*/

//
// Creates a new fifo holder
//
RChannelFifoHolder::RChannelFifoHolder(const char* channelURI,
                                       UInt32 fifoLength,
                                       bool isReader,
                                       WorkQueue* workQueue)
{
    if (workQueue == NULL)
    {
        //
        // create a work queue if needed
        //
        m_workQueue = new WorkQueue(4, 2);
        m_workQueue->Start();
        workQueue = m_workQueue;
    }
    else
    {
        //
        // If there is a global work queue, don't create a local one
        //
        m_workQueue = NULL;
    }

    m_fifo = new RChannelFifo(channelURI, fifoLength, workQueue);

    m_discardedReader = false;
    m_discardedWriter = false;
    m_madeReader = isReader;
    m_madeWriter = !isReader;
}

RChannelFifoHolder::~RChannelFifoHolder()
{
    if (m_workQueue != NULL)
    {
        m_workQueue->Stop();
    }
    delete m_fifo;
    delete m_workQueue;
}

RChannelFifo* RChannelFifoHolder::GetFifo()
{
    return m_fifo;
}

bool RChannelFifoHolder::MakeReader()
{
    {
        AutoCriticalSection acs(&m_atomic);

        if (m_madeReader == false)
        {
            m_madeReader = true;
            return true;
        }
        else
        {
            return false;
        }
    }
}

bool RChannelFifoHolder::MakeWriter()
{
    {
        AutoCriticalSection acs(&m_atomic);

        if (m_madeWriter == false)
        {
            m_madeWriter = true;
            return true;
        }
        else
        {
            return false;
        }
    }
}

bool RChannelFifoHolder::DiscardReader()
{
    {
        AutoCriticalSection acs(&m_atomic);

        LogAssert(m_discardedReader == false);
        m_discardedReader = true;
        if (m_discardedWriter)
        {
            m_fifo->GetReader()->Close();
            m_fifo->GetWriter()->Close();
            return true;
        }
        else
        {
            return false;
        }
    }
}

bool RChannelFifoHolder::DiscardWriter()
{
    {
        AutoCriticalSection acs(&m_atomic);

        LogAssert(m_discardedWriter == false);
        m_discardedWriter = true;
        if (m_discardedReader)
        {
            m_fifo->GetReader()->Close();
            m_fifo->GetWriter()->Close();
            return true;
        }
        else
        {
            return false;
        }
    }
}

typedef std::map< std::string, RChannelFifoHolder*, std::less<std::string> >
    FifoMap;

static FifoMap s_fifoMap;
static CRITSEC s_fifoAtomic;
static UInt32 s_fifoUniqueId = 0;

static UInt32 GetFifoLength(const char* channelURI)
{
    DrStr256 uri( channelURI );
    LogAssert(uri.StartsWithNoCase("fifo://", strlen("fifo://")));
    // BUGBUG: sammck: this code is inefficient, copies entire string when only the first word is used.
    DrStr256 lengthField( channelURI + strlen("fifo://") );
    size_t endOfField = lengthField.IndexOfChar('/');
    if (endOfField == DrStr_InvalidIndex)
    {
        DrLogW( "fifo URI malformed. URI %s", channelURI);
        return 0;
    }

    lengthField.UpdateLength(endOfField);

    UInt32 length;
    DrError err = DrStringToUInt32(lengthField, &length);
    if (err == DrError_OK)
    {
        return length;
    }
    else
    {
        DrLogW( "fifo URI malformed. URI %s lengthField %s getlength returned %s",
            channelURI, lengthField.GetString(), DRERRORSTRING(err));
        return 0;
    }
}


RChannelReaderHolder::~RChannelReaderHolder()
{
}

RChannelWriterHolder::~RChannelWriterHolder()
{
}

//
// Create a new fifo reader holder
//
RChannelFifoReaderHolder::
    RChannelFifoReaderHolder(const char* channelURI,
                             WorkQueue* workQueue,
                             DVErrorReporter* errorReporter)
{
    m_reader = NULL;

    UInt32 fifoLength = GetFifoLength(channelURI);

    if (fifoLength == 0)
    {
        //
        // Report an error if any inputs are empty
        //
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "fifo %s has invalid length",
                                   channelURI);
        return;
    }

    {
        AutoCriticalSection acs(&s_fifoAtomic);

        FifoMap::iterator existing = s_fifoMap.find(channelURI);
        if (existing == s_fifoMap.end())
        {
            //
            // If fifo doesn't already exist, create a new reader
            //
            DrLogI( "Creating new fifo reader. Name %s length %u", channelURI, fifoLength);
            RChannelFifoHolder* holder =
                new RChannelFifoHolder(channelURI, fifoLength, true,
                                       workQueue);
            s_fifoMap.insert(std::make_pair(channelURI, holder));
            m_reader = holder->GetFifo()->GetReader();
        }
        else
        {
            DrLogI(
                "Looked up existing fifo for reader. Name %s length %u", channelURI, fifoLength);
            if (existing->second->MakeReader())
            {
                RChannelFifo* fifo = existing->second->GetFifo();
                m_reader = fifo->GetReader();
            }
            else
            {
                errorReporter->ReportError(DryadError_InvalidChannelURI,
                                           "Duplicate fifo name %s passed "
                                           "to reader create",
                                           channelURI);
            }
        }
    }
}

RChannelFifoReaderHolder::~RChannelFifoReaderHolder()
{
    Close();
}

RChannelReader* RChannelFifoReaderHolder::GetReader()
{
    return m_reader;
}

void RChannelFifoReaderHolder::FillInStatus(DryadInputChannelDescription* s)
{
    LogAssert(m_reader != NULL);

    RChannelFifoWriterBase* writer =
        m_reader->GetParent()->GetWriter();
    LogAssert(writer != NULL);

    s->SetChannelProcessedLength(m_reader->GetDataSizeRead());
    s->SetChannelTotalLength(writer->GetDataSizeWritten());
}

void RChannelFifoReaderHolder::Close()
{
    if (m_reader != NULL)
    {
        RChannelFifo* fifo = m_reader->GetParent();
        const char* name = fifo->GetName();

        {
            AutoCriticalSection acs(&s_fifoAtomic);

            FifoMap::iterator holder = s_fifoMap.find(name);
            LogAssert(holder != s_fifoMap.end());

            if (holder->second->DiscardReader())
            {
                DrLogI( "Discarding fifo (reader). Name %s", name);

                delete holder->second;
                s_fifoMap.erase(holder);
            }
        }

        m_reader = NULL;
    }
}


RChannelFifoWriterHolder::
    RChannelFifoWriterHolder(const char* channelURI,
                             DVErrorReporter* errorReporter)
{
    m_writer = NULL;

    UInt32 fifoLength = GetFifoLength(channelURI);

    if (fifoLength == 0)
    {
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "fifo %s has invalid length",
                                   channelURI);
        return;
    }

    {
        AutoCriticalSection acs(&s_fifoAtomic);

        FifoMap::iterator existing = s_fifoMap.find(channelURI);
        if (existing == s_fifoMap.end())
        {
            DrLogI( "Creating new fifo writer. Name %s length %u", channelURI, fifoLength);
            RChannelFifoHolder* holder =
                new RChannelFifoHolder(channelURI, fifoLength, false, NULL);
            s_fifoMap.insert(std::make_pair(channelURI, holder));
            m_writer = holder->GetFifo()->GetWriter();
        }
        else
        {
            DrLogI(
                "Looked up existing fifo for writer. Name %s length %u", channelURI, fifoLength);
            if (existing->second->MakeWriter())
            {
                RChannelFifo* fifo = existing->second->GetFifo();
                m_writer = fifo->GetWriter();
            }
            else
            {
                errorReporter->ReportError(DryadError_InvalidChannelURI,
                                           "Duplicate fifo name %s passed "
                                           "to writer create",
                                           channelURI);
            }
        }
    }
}

RChannelFifoWriterHolder::~RChannelFifoWriterHolder()
{
    Close();
}

RChannelWriter* RChannelFifoWriterHolder::GetWriter()
{
    return m_writer;
}

void RChannelFifoWriterHolder::FillInStatus(DryadOutputChannelDescription* s)
{
    LogAssert(m_writer != NULL);
    s->SetChannelProcessedLength(m_writer->GetDataSizeWritten());
}

void RChannelFifoWriterHolder::Close()
{
    if (m_writer != NULL)
    {
        RChannelFifo* fifo = m_writer->GetParent();
        const char* name = fifo->GetName();

        {
            AutoCriticalSection acs(&s_fifoAtomic);

            FifoMap::iterator holder = s_fifoMap.find(name);
            LogAssert(holder != s_fifoMap.end());

            if (holder->second->DiscardWriter())
            {
                DrLogI( "Discarding fifo (writer). Name %s", name);

                delete holder->second;
                s_fifoMap.erase(holder);
            }
        }

        m_writer = NULL;
    }
}

RChannelNullWriterHolder::RChannelNullWriterHolder(const char* uri)
{
    m_writer = new RChannelNullWriter(uri);
}

RChannelNullWriterHolder::~RChannelNullWriterHolder()
{
    Close();
}

RChannelWriter* RChannelNullWriterHolder::GetWriter()
{
    return m_writer;
}

void RChannelNullWriterHolder::FillInStatus(DryadOutputChannelDescription* s)
{
    LogAssert(m_writer != NULL);
    s->SetChannelProcessedLength(0);
}

void RChannelNullWriterHolder::Close()
{
    delete m_writer;
    m_writer = NULL;
}


//
// Create a buffer reader for the incoming channel
//
RChannelBufferedReaderHolder::
    RChannelBufferedReaderHolder(const char* channelURI,
                                 RChannelOpenThrottler* openThrottler,
                                 DryadMetaData* metaData,
                                 RChannelItemParserBase* parser,
                                 UInt32 numberOfReaders,
                                 UInt32 maxParseBatchSize,
                                 UInt32 maxParseUnitsInFlight,
                                 WorkQueue* workQueue,
                                 DVErrorReporter* errorReporter,
                                 LPDWORD localInputChannels)
{
    m_parser = parser;
    m_reader = NULL;
    m_bufferReader = NULL;

    //
    // Create buffer reader
    //
    bool lazyStart =
        CreateBufferReader(numberOfReaders, openThrottler, workQueue,
                           channelURI, metaData, errorReporter, localInputChannels);

    //
    // If error, just return. Caller will see same error and act on it
    //
    if (errorReporter->GetErrorCode() != DrError_OK)
    {
        return;
    }

    LogAssert(m_bufferReader != NULL);

    //
    // Create a serial reader wrapper around the buffer reader
    //
    RChannelSerializedReader* r =
        new RChannelSerializedReader(m_bufferReader,
                                     parser,
                                     maxParseBatchSize,
                                     maxParseUnitsInFlight,
                                     lazyStart,
                                     workQueue);
    r->SetURI(channelURI);

    m_reader = r;
}

//
// Call close on destruction, which handles cleanup
//
RChannelBufferedReaderHolder::~RChannelBufferedReaderHolder()
{
    Close();
}

//
// Create a buffer reader, taking the type of input into account
//
bool RChannelBufferedReaderHolder::
    CreateBufferReader(UInt32 numberOfReaders,
                       RChannelOpenThrottler* openThrottler,
                       WorkQueue* workQueue,
                       const char* channelURI,
                       DryadMetaData* metaData,
                       DVErrorReporter* errorReporter,
                       LPDWORD localInputChannels)
{
    bool lazyStart = false;

    if (ConcreteRChannel::IsNTFSFile(channelURI))
    {
        //
        // If URI is on-premise NTFS file, create the file reader right away
        //
        m_bufferReader =
            CreateNativeFileReader(numberOfReaders, openThrottler,
                                   workQueue,
                                   channelURI + ::strlen(s_filePrefix),
                                   metaData, errorReporter, localInputChannels);
        lazyStart = true;
    }
    else if (ConcreteRChannel::IsHdfsPartition(channelURI))
    {
        m_bufferReader = CreateHdfsBlockReader(channelURI);
        lazyStart = false;
    }
    else if (ConcreteRChannel::IsFifo(channelURI))
    {
        //
        // If URI is a fifo, report it as an error
        //
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "RChannelBufferReaderFactory passed "
                                   "fifo URI %s in error",
                                   channelURI);
    }
    else if (ConcreteRChannel::IsNull(channelURI))
    {
        //
        // If URI is a null reader, create a specialized reader for it
        // todo: figure out when this is used
        //
        m_bufferReader =
            CreateNullReader(channelURI,
                             metaData, errorReporter);
    }
    // hide azure related
#if 0
    else if (ConcreteRChannel::IsUncPath(channelURI))
    {
        //
        // If URI is a UNC path in azure, copy the file locally and then create reader
        // 
        m_bufferReader = CreateUncFileReader(numberOfReaders, openThrottler,
                                             workQueue, channelURI, metaData, errorReporter, NULL);
    }
#endif
    else
    {
        //
        // If any other channel URI, give up and report error
        //
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "Can't open channel '%s' to read --- "
                                   "unknown prefix (must be %s, %s, %s, %s, %s or %s)",
                                   channelURI,
                                   s_filePrefix, s_tidyfsPrefix, s_fifoPrefix, 
                                   s_dscPartitionPrefix,
                                   RChannelBufferHdfsReader::
                                   s_hdfsPartitionPrefix,
                                   s_nullPrefix);
    }

    //
    // If creating the reader failed, report the error
    //
    if (m_bufferReader == NULL && errorReporter->NoError())
    {
        errorReporter->ReportError(DryadError_ChannelOpenError,
                                   "Can't open channel '%s' to read",
                                   channelURI);
    }

    return lazyStart;
}

//
// Return the reader
//
RChannelReader* RChannelBufferedReaderHolder::GetReader()
{
    return m_reader;
}

void RChannelBufferedReaderHolder::
    FillInStatus(DryadInputChannelDescription* s)
{
    LogAssert(m_bufferReader != NULL);
    m_bufferReader->FillInStatus(s);
}

//
// Clean up the reader and the buffered reader wrapper
//
void RChannelBufferedReaderHolder::Close()
{
    if (m_reader == NULL)
    {
        LogAssert(m_bufferReader == NULL);
    }
    else
    {
        m_reader->Close();
        delete m_reader;
        m_reader = NULL;
        LogAssert(m_bufferReader != NULL);
        delete m_bufferReader;
        m_bufferReader = NULL;
    }
}

RChannelBufferedWriterHolder::
    RChannelBufferedWriterHolder(const char* channelURI,
                                 RChannelOpenThrottler* openThrottler,
                                 DryadMetaData* metaData,
                                 RChannelItemMarshalerBase* marshaler,
                                 UInt32 numberOfWriters,
                                 UInt32 maxMarshalBatchSize,
                                 WorkQueue* workQueue,
                                 DVErrorReporter* errorReporter)
{
    m_marshaler = marshaler;
    m_writer = NULL;
    m_bufferWriter = NULL;

    bool breakOnBufferBoundaries;
    CreateBufferWriter(numberOfWriters, openThrottler,
                       channelURI, metaData, &breakOnBufferBoundaries,
                       errorReporter);

    if (errorReporter->GetErrorCode() != DrError_OK)
    {
        return;
    }

    LogAssert(m_bufferWriter != NULL);

    RChannelSerializedWriter* w =
        new RChannelSerializedWriter(m_bufferWriter,
                                     marshaler,
                                     breakOnBufferBoundaries,
                                     maxMarshalBatchSize,
                                     workQueue);
    w->SetURI(channelURI);

    m_writer = w;
}

RChannelBufferedWriterHolder::~RChannelBufferedWriterHolder()
{
    Close();
}

void RChannelBufferedWriterHolder::
    CreateBufferWriter(UInt32 numberOfWriters,
                       RChannelOpenThrottler* openThrottler,
                       const char* channelURI,
                       DryadMetaData* metaData,
                       bool* pBreakOnBufferBoundaries,
                       DVErrorReporter* errorReporter)
{
    *pBreakOnBufferBoundaries = false;

    if (ConcreteRChannel::IsNTFSFile(channelURI))
    {
        m_bufferWriter =
            CreateNativeFileWriter(numberOfWriters, openThrottler,
                                   channelURI + ::strlen(s_filePrefix),
                                   metaData, pBreakOnBufferBoundaries,
                                   errorReporter);
    }
    else if (ConcreteRChannel::IsHdfsFile(channelURI))
    {
        m_bufferWriter = CreateHdfsFileWriter(channelURI);
    }
    else if (ConcreteRChannel::IsFifo(channelURI))
    {
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "RChannelBufferWriterFactory passed "
                                   "fifo URI %s in error",
                                   channelURI);
    }
    else
    {
        errorReporter->ReportError(DryadError_InvalidChannelURI,
                                   "Can't open channel '%s' to write --- "
                                   "unknown prefix (must be %s, %s or %s)",
                                   channelURI, s_filePrefix,
                                   s_tidyfsPrefix, s_fifoPrefix);
    }

    if (m_bufferWriter == NULL && errorReporter->NoError())
    {
        errorReporter->ReportError(DryadError_ChannelOpenError,
                                   "Can't open channel '%s' to write",
                                   channelURI);
    }
}

RChannelWriter* RChannelBufferedWriterHolder::GetWriter()
{
    return m_writer;
}

void RChannelBufferedWriterHolder::
    FillInStatus(DryadOutputChannelDescription* s)
{
    LogAssert(m_bufferWriter != NULL);
    m_bufferWriter->FillInStatus(s);
}

void RChannelBufferedWriterHolder::Close()
{
    if (m_writer == NULL)
    {
        LogAssert(m_bufferWriter == NULL);
    }
    else
    {
        m_writer->Close();
        delete m_writer;
        m_writer = NULL;
        LogAssert(m_bufferWriter != NULL);
        delete m_bufferWriter;
        m_bufferWriter = NULL;
    }
}

//
// Open a reader on an input channel
//
DrError RChannelFactory::OpenReader(const char* channelURI,
                                    DryadMetaData* metaData,
                                    RChannelItemParserBase* parser,
                                    UInt32 numberOfReaders,
                                    RChannelOpenThrottler* openThrottler,
                                    UInt32 maxParseBatchSize,
                                    UInt32 maxParseUnitsInFlight,
                                    WorkQueue* workQueue,
                                    DVErrorReporter* errorReporter,
                                    RChannelReaderHolderRef* pHolder,
                                    LPDWORD localInputChannels)
{
    LogAssert(errorReporter->NoError());
    LogAssert(numberOfReaders > 0);

    if (ConcreteRChannel::IsFifo(channelURI))
    {
        //
        // If the input channel is a FIFO, open a FIFO channel reader
        //
        pHolder->Attach(new RChannelFifoReaderHolder(channelURI, workQueue,
                                                     errorReporter));
    }
    else
    {
        //
        // If the input channel is a file or DSC buffer, open a generic buffer reader
        //
        pHolder->Attach(new RChannelBufferedReaderHolder(channelURI,
                                                         openThrottler,
                                                         metaData,
                                                         parser,
                                                         numberOfReaders,
                                                         maxParseBatchSize,
                                                         maxParseUnitsInFlight,
                                                         workQueue,
                                                         errorReporter,
                                                         localInputChannels));
    }

    return errorReporter->GetErrorCode();
}

//
// Open writer for provided output channel
//
DrError RChannelFactory::OpenWriter(const char* channelURI,
                                    DryadMetaData* metaData,
                                    RChannelItemMarshalerBase* marshaler,
                                    UInt32 numberOfWriters,
                                    RChannelOpenThrottler* openThrottler,
                                    UInt32 maxMarshalBatchSize,
                                    WorkQueue* workQueue,
                                    DVErrorReporter* errorReporter,
                                    RChannelWriterHolderRef* pHolder)
{
    LogAssert(errorReporter->NoError());
    LogAssert(numberOfWriters > 0);

    if (ConcreteRChannel::IsFifo(channelURI))
    {
        //
        // If the output channel is a FIFO, open a FIFO channel writer
        //
        pHolder->Attach(new RChannelFifoWriterHolder(channelURI,
                                                     errorReporter));
    }
    else if (ConcreteRChannel::IsNull(channelURI))
    {
        //
        // If the output channel is a null channel, open a null channel writer
        //
        pHolder->Attach(new RChannelNullWriterHolder(channelURI));
    }
    else
    {
        //
        // If the output channel is a file buffer, open a file buffer writer
        //
        pHolder->Attach(new RChannelBufferedWriterHolder(channelURI,
                                                         openThrottler,
                                                         metaData,
                                                         marshaler,
                                                         numberOfWriters,
                                                         maxMarshalBatchSize,
                                                         workQueue,
                                                         errorReporter));
    }

    return errorReporter->GetErrorCode();
}

UInt32 RChannelFactory::GetUniqueFifoId()
{
    UInt32 id;

    {
        AutoCriticalSection acs(&s_fifoAtomic);

        id = s_fifoUniqueId;
        ++s_fifoUniqueId;
    }

    return id;
}

//
// Create a new Throttler
//
RChannelOpenThrottler* RChannelFactory::MakeOpenThrottler(UInt32 maxOpens,
                                                          WorkQueue* workQueue)
{
    return new RChannelOpenThrottler(maxOpens, workQueue);
}

//
// Delete referenced throttler
//
void RChannelFactory::DiscardOpenThrottler(RChannelOpenThrottler* throttler)
{
    delete throttler;
}

