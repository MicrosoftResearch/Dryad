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

#include <DrShared.h>


DrFileWriter::DrFileWriter()
{
    m_fileHandle = INVALID_HANDLE_VALUE;
    m_dataBufferSize = 64 * 1024;
    m_bufferedData = new char[m_dataBufferSize];
    m_bufferedDataLength = 0;

}

#ifdef _MANAGED
DrFileWriter::~DrFileWriter()
{
    this->!DrFileWriter();
}

DrFileWriter::!DrFileWriter()
{
    if (m_fileHandle != INVALID_HANDLE_VALUE)
    {
        FlushInternal();
        CloseHandle(m_fileHandle);
    }
    delete [] m_bufferedData;

}
#else
DrFileWriter::~DrFileWriter()
{
    if (m_fileHandle != INVALID_HANDLE_VALUE)
    {
        FlushInternal();
        CloseHandle(m_fileHandle);
    }
    delete [] m_bufferedData;

}
#endif

bool DrFileWriter::ReOpen(DrString fileName)
{
    DrAutoCriticalSection acs(this);

    DrAssert(m_fileHandle == INVALID_HANDLE_VALUE);

    m_fileHandle = CreateFileA(
        fileName.GetChars(),
        GENERIC_WRITE,
        FILE_SHARE_READ,
        NULL,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (m_fileHandle == INVALID_HANDLE_VALUE)
    {
        DrLogW("File open failed for %s with error %s", fileName.GetChars(),
               DRERRORSTRING(HRESULT_FROM_WIN32(GetLastError())));
        return false;
    }

    return true;
}

bool DrFileWriter::Open(DrString fileName)
{
    DrAutoCriticalSection acs(this);

    DrAssert(m_fileHandle == INVALID_HANDLE_VALUE);

    m_fileHandle = CreateFileA(
        fileName.GetChars(),
        GENERIC_WRITE,
        FILE_SHARE_READ,
        NULL,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (m_fileHandle == INVALID_HANDLE_VALUE)
    {
        DrLogW("File open failed for %s with error %s", fileName.GetChars(),
               DRERRORSTRING(HRESULT_FROM_WIN32(GetLastError())));
        return false;
    }

    return true;
}

void DrFileWriter::FlushInternal()
{
    if (m_fileHandle != INVALID_HANDLE_VALUE && m_bufferedDataLength > 0)
    {
        DWORD bytesWritten;
        WriteFile(m_fileHandle, m_bufferedData, m_bufferedDataLength, &bytesWritten, NULL);
        m_bufferedDataLength = 0;
    }
}

void DrFileWriter::Flush()
{
    DrAutoCriticalSection acs(this);

    FlushInternal();
}

void DrFileWriter::Close()
{
    DrAutoCriticalSection acs(this);

    if (m_fileHandle != INVALID_HANDLE_VALUE)
    {
        FlushInternal();
        CloseHandle(m_fileHandle);
        m_fileHandle = INVALID_HANDLE_VALUE;
    }
}

void DrFileWriter::Append(const char *data, int dataLength)
{
    DrAutoCriticalSection acs(this);

    if (m_fileHandle != INVALID_HANDLE_VALUE)
    {
        if (m_bufferedDataLength + dataLength > m_dataBufferSize)
        {
            FlushInternal();
        }

        if (m_bufferedDataLength + dataLength > m_dataBufferSize)
        {
            DrAssert(m_bufferedDataLength == 0);
            m_dataBufferSize = dataLength;
            delete [] m_bufferedData;
            m_bufferedData = new char[m_dataBufferSize];
        }

        memcpy(m_bufferedData + m_bufferedDataLength, data, dataLength);
        m_bufferedDataLength += dataLength;
    }
}

typedef DrArrayList<DrFileWriterRef> DrFWArray;
DRAREF(DrFWArray,DrFileWriterRef);

DRCLASS(DrSFWInternal)
{
public:
    static DrCritSecRef s_cs;
    static DrFWArrayRef s_file;
};

#ifndef _MANAGED
DrCritSecRef DrSFWInternal::s_cs;
DrFWArrayRef DrSFWInternal::s_file;
#endif

void DrStaticFileWriters::Initialize()
{
    DrSFWInternal::s_cs = DrNew DrCritSec();
    DrSFWInternal::s_file = DrNew DrFWArray();
}

void DrStaticFileWriters::AddWriter(DrFileWriterPtr writer)
{
    DrAutoCriticalSection acs(DrSFWInternal::s_cs);

    DrSFWInternal::s_file->Add(writer);
}

void DrStaticFileWriters::FlushWriters()
{
    DrAutoCriticalSection acs(DrSFWInternal::s_cs);

    int i;
    for (i=0; i<DrSFWInternal::s_file->Size(); ++i)
    {
        DrSFWInternal::s_file[i]->Flush();
    }
}

void DrStaticFileWriters::Discard()
{
    DrSFWInternal::s_cs = DrNull;
    DrSFWInternal::s_file = DrNull;
}