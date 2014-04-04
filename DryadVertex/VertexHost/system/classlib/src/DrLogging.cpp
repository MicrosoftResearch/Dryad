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

#include "DrCommon.h"

#pragma unmanaged

//
// there are race conditions in the accessor functions for these global bools, however
// we are ignoring them since their effects will merely be to change the number of log
// statements printed if the logging level is dynamically changed during a program run,
// which is benign compared to the overhead of acquiring a lock while testing for logging
// being enabled 
//

//
// Default level is warning
//
static LogLevel s_loggingType = LogLevel_Warning;

void DrLogging::Initialize(const WCHAR* logFileName)
{
    m_logFile = CreateLogFile(logFileName);
}

//
// Update the logging level to all at supplied level and more severe
//
void DrLogging::SetLoggingLevel(LogLevel type)
{
    s_loggingType = type;
}

//
// Check whether logging is enabled at a certain event level
//
bool DrLogging::Enabled(LogLevel type)
{
    return ((s_loggingType & type) == type);
}

//
// Flush the log
//
void DrLogging::FlushLog()
{
    fflush(m_logFile);
}

//
// Return log file - used by drlogginghelper
//
FILE* DrLogging::GetLogFile()
{
    return m_logFile;
}

//
// Create the vertex host log file
//
FILE* DrLogging::CreateLogFile(const WCHAR* logFileName)
{
    FILE * logFile = _wfsopen(logFileName, L"w", _SH_DENYWR);
    if(logFile != NULL)
    {
        // If log file created successfully, use it
        return logFile;
    }

    // if there is an error creating the log file, fall back to stderr
    return stderr;
}

//
// Initialize log file 
//
FILE* DrLogging::m_logFile = stderr;

static DrLogAssertCallback* s_assertCallback = NULL;
static void*                s_assertCookie = NULL;

void DrLogging::SetAssertCallback(DrLogAssertCallback callback, void* cookie)
{
    s_assertCallback = callback;
    s_assertCookie = cookie;
}

//
// Log the provided string
//
void DrLogHelper::operator()(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    SYSTEMTIME utc, local;
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    FileTimeToSystemTime(&ft, &utc);
    SystemTimeToTzSpecificLocalTime(NULL, &utc, &local);

    //
    // Get character for event logging level
    //
    char initial = 0;
    switch (m_type)
    {
    case LogLevel_Assert:
        initial = 'a';
        break;
    case LogLevel_Error:
        initial = 'e';
        break;
    case LogLevel_Warning:
        initial = 'w';
        break;
    case LogLevel_Info:
        initial = 'i';
        break;
    case LogLevel_Debug:
        initial = 'd';
        break;
    }


    //
    // Get formatted message
    //
    DrStr128 s = "";
	if (format != NULL)
	{
		s.VSetF(format, args);
	}

    //
    // Strip path from filename, if present.
    //
    const char * filename =m_file;
    const char * lastBackslash = strrchr(filename, '\\');
    if (lastBackslash != NULL)
    {
        filename = lastBackslash + 1;
    }

    //
    // Print out message to stderr
    //
    fprintf(DrLogging::GetLogFile(),
        "%c, %02d/%02d/%04d %02d:%02d:%02d.%03d, TID=%u,%s,%s:%d,%s\n",
        initial,
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds,
        GetCurrentThreadId(),
        m_function, 
        m_file, 
        m_line,
        s.GetString()
    );


    va_end(args);

    DrLogging::FlushLog();
    //
    // If assert level, assert(false) after logging and flushing the stream
    //
    if (m_type == LogLevel_Assert)
    {
        DrLogging::FlushLog();

        if (IsDebuggerPresent())
        {
            ::DebugBreak();
        }

        if (s_assertCallback != NULL)
        {
            DrStr128 assertString;
            assertString.SetF("%s,%s:%d,%s", m_function, m_file, m_line, s.GetString());
            (*s_assertCallback)(s_assertCookie, assertString.GetString());
        }

        TerminateProcess(GetCurrentProcess(), DrError_Fail);
    }
}
