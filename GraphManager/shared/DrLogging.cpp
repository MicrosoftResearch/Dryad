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

#include "DrShared.h"

#include "DrErrorInternal.h"

#include <assert.h>

#ifdef _MANAGED
QueryAssertException::QueryAssertException(System::String ^message, System::Diagnostics::StackTrace ^st) : System::Exception(message)
{
    HResult = DrError_AssertFailure;
    m_stackTrace = st;
}
#endif

/* there are race conditions in the accessor functions for these global bools, however
we are ignoring them since their effects will merely be to change the number of log
statements printed if the logging level is dynamically changed during a program run,
which is benign compared to the overhead of acquiring a lock while testing for logging
eing enabled */

static DrLogType s_loggingType = DrLog_Warning;

void DrLogging::SetLoggingLevel(DrLogType type)
{
    s_loggingType = type;
}

bool DrLogging::Enabled(DrLogType type)
{
    return ((s_loggingType & type) == type);
}

DRCLASS(DrLoggingInternal)
{
public:
    static void Initialize(DrString logPath, bool redirectStdStreams);
    static void FlushLogs();
    DrLoggingInternal();
  
#ifdef _MANAGED
    static __declspec(noreturn) void Terminate(UINT exitCode, QueryAssertException ^e);
#else
    static __declspec(noreturn) void Terminate(UINT exitCode);
#endif
    static void Stop();

    static void Append(const char* data, int dataLength);

    static void RolloverLog();

private:

    static void FlushThread();
    static void ForkFlushThread();
    static void WaitForFlushThread();

    static DrFileWriterRef             s_logFile;
    static volatile int                s_flag;
    static const DWORD                 s_maxLogFileBytes = 10 * 1024 * 1024; // 10 MBytes
    static volatile DWORD              s_currentLogFileBytes;
    static int                         s_currentLogFileCounter;
    static DrString                    s_logFileName;
    static DrString                    s_archiveLogFileFormat;

    static DrCritSecRef                m_rolloverLock;
#ifdef _MANAGED
    static System::Threading::Thread^  s_flushThread;
#else
    static unsigned __stdcall FlushThreadFunc(void* arg);
    static HANDLE                      s_flushThread;
#endif
};

#ifndef _MANAGED
DrFileWriterRef DrLoggingInternal::s_logFile;
volatile int DrLoggingInternal::s_flag;
volatile DWORD DrLoggingInternal::s_currentLogFileBytes = 0;
int DrLoggingInternal::s_currentLogFileCounter = 0;
DrString DrLoggingInternal::s_logFileName = "default.log";
DrString DrLoggingInternal::s_archiveLogFileFormat = "default%03d.log";
DrCritSecRef DrLoggingInternal::m_rolloverLock = DrNull;
HANDLE DrLoggingInternal::s_flushThread;
#endif

DrLoggingInternal::DrLoggingInternal()
{
    // Nothing needed
}

#pragma warning (push)
// _wfreopen_s tries to open the file with exclusive access, which fails since HpcQueryGraphManager.exe already has it open
#pragma warning (disable: 4996 ) // _wfreopen : This function or variable may be unsafe. Consider using _wfreopen_s instead.
void DrLoggingInternal::Initialize(DrString logPath, bool redirectStdStreams)
{
    WCHAR szOut[MAX_PATH + 1] = {0};
    WCHAR szDir[MAX_PATH + 1] = {0};

    s_currentLogFileBytes = 0;
    s_currentLogFileCounter = 0;
    s_logFileName.SetF("%s.log", logPath.GetChars());
    s_archiveLogFileFormat.SetF("%s%%03d.log", logPath.GetChars());
    m_rolloverLock = DrNew DrCritSec();

    if (redirectStdStreams && GetCurrentDirectory(MAX_PATH, szDir) > 0)
    {
        FILE *f = 0;
        if (_snwprintf_s(szOut, MAX_PATH, MAX_PATH, L"%s\\stdout.txt", szDir) != -1)
        {
            f = _wfreopen(szOut, L"a", stdout);
        }
        FILE *fe = 0;
        if (_snwprintf_s(szOut, MAX_PATH, MAX_PATH, L"%s\\stderr.txt", szDir) != -1)
        {
            fe = _wfreopen(szOut, L"a", stderr);
        }
    }

    s_flag = 0;
    s_logFile = DrNew DrFileWriter();
    if (s_logFile->Open(s_logFileName))
    {
        DrStaticFileWriters::AddWriter(s_logFile);
    }
    else
    {
        fprintf(stderr, "Failed to open log file %s: no logging!\n", s_logFileName.GetChars());
    }

    ForkFlushThread();

    DrLogWithType(DrLog_Info)("Logging started");
}
#pragma warning(pop)

void DrLoggingInternal::FlushThread()
{
    do
    {
        Sleep(1000);
        FlushLogs();
    } while (s_flag == 0);
}

void DrLoggingInternal::FlushLogs()
{
    // We don't want to flush if we're rolling over the logs
    DrAutoCriticalSection acs(m_rolloverLock);

    fflush(stdout);
    fflush(stderr);
    DrStaticFileWriters::FlushWriters();
}

#ifdef _MANAGED
__declspec(noreturn) void DrLoggingInternal::Terminate(UINT exitCode, QueryAssertException ^e)
{
    DrLogWithType(DrLog_Info)("------------- Terminating the process ------------ ExitCode=%u", exitCode);

    Stop();

    if (exitCode == 0)
    {
        //
        // application says that everything completed normally; do clean shutdown
        //
        exit(0);
    }
    else if (e != DrNull)
    {
        //
        // something went wrong; we flushed the logs and must commit a suicide now
        // as we cannot do clean shutdown as it may hang (e.g. because data structures were corrupted,
        // a thread failed while holding a lock, etc.)
        //

        throw e;
    }
    else
    {
        //
        // We weren't passed an exception, so just terminate the process
        //

        TerminateProcess(GetCurrentProcess(), exitCode);
    }
}
#else
__declspec(noreturn) void DrLoggingInternal::Terminate(UINT exitCode)
{
    DrLogI("------------- Terminating the process ------------ ExitCode=%u", exitCode);

    Stop();

    if (exitCode == 0)
    {
        //
        // application says that everything completed normally; do clean shutdown
        //
        exit(0);
    }
    else
    {
        //
        // something went wrong; we flushed the logs and must commit a suicide now
        // as we cannot do clean shutdown as it may hung (e.g. because data structures were corrupted,
        // a thread failed while holding a lock, etc.)
        //
        TerminateProcess(GetCurrentProcess(), exitCode);
    }

}
#endif 

void DrLoggingInternal::Stop()
{
    FlushLogs();

    {
        DrAutoCriticalSection acs(m_rolloverLock);

        s_flag = 1;
    }
    WaitForFlushThread();
    s_logFile = DrNull;
}

void DrLoggingInternal::RolloverLog()
{
    DrAutoCriticalSection acs(m_rolloverLock);

    ++s_currentLogFileCounter;
    s_logFile->Close();
    
    s_currentLogFileBytes = 0;
    DrString archiveFileName;
    archiveFileName.SetF(s_archiveLogFileFormat.GetChars(), s_currentLogFileCounter);
    if (MoveFileA(s_logFileName.GetChars(), archiveFileName.GetChars()))
    {
        if (!s_logFile->Open(s_logFileName))
        {
            fprintf(stderr, "Failed to open log file %s: no logging!\n", s_logFileName.GetChars());
        }
    }
    else 
    {
        fprintf(stderr, "Failed to archive log file %s to %s with error %s\n", s_logFileName.GetChars(), archiveFileName.GetChars(), 
            DRERRORSTRING(HRESULT_FROM_WIN32(GetLastError())));
        if (!s_logFile->ReOpen(s_logFileName))
        {
            fprintf(stderr, "Failed to reopen log file %s: no logging!\n", s_logFileName.GetChars());
        }
    }
}

void DrLoggingInternal::Append(const char *data, int dataLength)
{
    // TODO: this might slow logging down, but we don't want to miss any log stms
    DrAutoCriticalSection acs(m_rolloverLock);

    if (s_flag == 1) return;

    if (s_currentLogFileBytes + dataLength > s_maxLogFileBytes)
    {
        RolloverLog();
    }
    s_logFile->Append(data, dataLength);
    s_currentLogFileBytes += dataLength;
}

#include <dbghelp.h>
static int s_miniDumpTimeoutMilliseconds = 15 * 60 * 1000;
static LONG volatile s_startedMiniDump = 0;

//
// Only include data sections for our assemblies and ntdll
//

const WCHAR* c_szIncludeModules[] =
{
    L"HpcQueryGraphManager",
    L"Microsoft.Hpc.Query.GraphManager",
    L"Microsoft.Hpc.Query.ClusterAdapter",
    L"HpcQueryNativeClusterAdapter",
    L"Microsoft.Hpc.Dsc",
    L"HpcDscNativeClient"
};

static BOOL IncludeDataSection(const WCHAR* pModule)
{
    if (pModule == NULL)
    {
        return FALSE;
    }

    WCHAR szFileName[_MAX_FNAME] = L"";
    _wsplitpath_s(pModule, NULL, 0, NULL, 0, szFileName, _MAX_FNAME, NULL, 0);

    DWORD numMods = sizeof(c_szIncludeModules) / sizeof(c_szIncludeModules[0]);
    for (DWORD i = 0; i < numMods; i++)
    {
        if (_wcsicmp(c_szIncludeModules[i], szFileName) == 0)
        {
            return TRUE;
        }
    }

    return FALSE;
}

//
// Callback for MiniDumpWriteDump
//
static BOOL MiniDumpCallback(
	PVOID, 
	const PMINIDUMP_CALLBACK_INPUT   pInput, 
	PMINIDUMP_CALLBACK_OUTPUT        pOutput 
) 
{
	BOOL bRet = FALSE; 


	// Check parameters 
	if( pInput == 0 ) 
		return FALSE; 

	if( pOutput == 0 ) 
		return FALSE; 


	// Process the callbacks 
	switch( pInput->CallbackType ) 
	{
		case IncludeModuleCallback: 
		{
			// Include the module into the dump 
			bRet = TRUE; 
		}
		break; 

		case IncludeThreadCallback: 
		{
			// Skip the minidump thread 
            if (pInput->Thread.ThreadId == ::GetCurrentThreadId())
            {
                bRet = FALSE;
            }
            else
            {
                bRet = TRUE;  
            }
		}
		break; 

		case ModuleCallback: 
		{
			if( pOutput->ModuleWriteFlags & ModuleWriteDataSeg ) 
			{
				if( !IncludeDataSection( pInput->Module.FullPath ) ) 
				{
					pOutput->ModuleWriteFlags &= (~ModuleWriteDataSeg); 
				}
			}
            bRet = TRUE;
		}
		break; 

		case ThreadCallback: 
		{
			// Include all thread information into the minidump 
            bRet = TRUE;
		}
		break; 

		case ThreadExCallback: 
		{
			// Include this information 
			bRet = TRUE;  
		}
		break; 

		case MemoryCallback: 
		{
			// We do not include any information here -> return FALSE 
			bRet = FALSE; 
		}
		break; 

		case CancelCallback: 
			break; 
	}

	return bRet; 

}


//
// Write out a mini dump
//
void DrLogging::WriteMiniDumpImpl()
{
    // If this function actually causes exceptions itself, we will be
    // doomed with deadlock. So, we catch all the possible exceptions
    // generated by this function and do nothing.
    try
    {
        CHAR szDumpFile[MAX_PATH + 1] = {0};
        if (!GetCurrentDirectoryA(MAX_PATH, szDumpFile))
        {
            fprintf(stderr, "Failed to get current directory: %s\n", DRERRORSTRING(GetLastError()));
            return;
        }

        strcat_s(szDumpFile, MAX_PATH, "\\minidump.dmp");

        // write the actual dump
        HANDLE hMiniDumpFile = CreateFileA (
            szDumpFile,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_WRITE | FILE_SHARE_READ,
            0,
            CREATE_ALWAYS,
            0,
            0
            );

        if (hMiniDumpFile != INVALID_HANDLE_VALUE)
        {
            /* don't log here in case it's logging that's causing the problem */

            MINIDUMP_CALLBACK_INFORMATION mci = {0}; 
            mci.CallbackRoutine     = (MINIDUMP_CALLBACK_ROUTINE)MiniDumpCallback; 
            mci.CallbackParam       = NULL; 

            MINIDUMP_TYPE mdt = (MINIDUMP_TYPE) (
                MiniDumpWithFullMemory |
                MiniDumpWithHandleData |
                MiniDumpWithUnloadedModules |
                MiniDumpWithThreadInfo |
                MiniDumpWithDataSegs );

            BOOL err = MiniDumpWriteDump(
                GetCurrentProcess(),
                GetCurrentProcessId(),
                hMiniDumpFile,
                mdt,
                NULL,
                NULL,
                &mci
                );
            if (!err)
            {
                fprintf(stderr, "Failed to write minidump last Error %s\n", DRERRORSTRING(GetLastError()));
            }
            else
            {
                CHAR szComputer[MAX_COMPUTERNAME_LENGTH + 1] = {0};
                DWORD cchSize = MAX_COMPUTERNAME_LENGTH + 1;

                // enable for DNS hostnames
                //CHAR szComputer[DNS_MAX_LABEL_BUFFER_LENGTH] = {0};
                //DWORD cchSize = DNS_MAX_LABEL_BUFFER_LENGTH;

                fprintf(stderr, "Wrote minidump to %s", szDumpFile);
                if (GetComputerNameA(szComputer, &cchSize))
                //if (GetComputerNameExA(ComputerNameDnsHostname, szComputer, &cchSize))
                {
                    fprintf(stderr, " on node %s", szComputer);
                }
                fprintf(stderr, "\n");
            }

            CloseHandle(hMiniDumpFile);
        }
        else
        {
            fprintf(stderr, "Failed to open dump file %s error %s\n", szDumpFile, DRERRORSTRING(GetLastError()));
        }
    }
    catch (...)
    {
        // do nothing
    }

    fflush(stderr);

    return;
}

#ifdef _MANAGED

void DrLoggingInternal::ForkFlushThread()
{
    s_flushThread = DrNew System::Threading::Thread(
        DrNew System::Threading::ThreadStart(&FlushThread));
    s_flushThread->Start();
}

void DrLoggingInternal::WaitForFlushThread()
{
    s_flushThread->Join();
}

void DrLogging::MiniDumpThread()
{
    DrLogging::WriteMiniDumpImpl();
}

#undef GetEnvironmentVariable

bool DrLogging::WriteMiniDump()
{
    //Since writing a dump is a one time occurrence dont write it if its already
    //started.
    if (InterlockedExchange(&s_startedMiniDump, 1) != 0)
    {
        // The dump is already started
        return false;
    }

    // Check to see whether we are even supposed to write a minidump
    System::String ^dumpEnvVal = System::Environment::GetEnvironmentVariable(L"HPCQUERY_GM_CREATEDUMP");
    if (System::String::IsNullOrEmpty(dumpEnvVal) || dumpEnvVal->Equals(L"0"))
        return false;

    // Write the minidump in a new thread so that we capture the state of the
    // current thread correctly
    System::Threading::Thread ^dumpThread =
        DrNew System::Threading::Thread(
        DrNew System::Threading::ThreadStart(&MiniDumpThread));
    dumpThread->Start();
    if (!dumpThread->Join(s_miniDumpTimeoutMilliseconds))
    {
        // Timed out waiting for minidump thread
        return false;
    }

    return true;
}


#else

#include <process.h>

static unsigned __stdcall MiniDumpThreadFunc(void* /* unused arg */)
{
//    WriteMiniDumpImpl(NULL);
    return 0;
}

unsigned __stdcall DrLoggingInternal::FlushThreadFunc(void* /* unused arg */)
{
    DrLoggingInternal::FlushThread();
    return 0;
}

#pragma warning (push)
#pragma warning (disable: 4505) // Unreferenced local function parameter
static void ForkMiniDumpThread()
{
    fprintf(stderr, "About to create dump thread\n");
    unsigned threadAddr;
    HANDLE handle =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  MiniDumpThreadFunc,
                                  NULL,
                                  0,
                                  &threadAddr);
    assert(handle != 0);
    fprintf(stderr, "Waiting for dump thread\n");
    ::WaitForSingleObject(handle, INFINITE);
    
    ::CloseHandle(handle);
    fprintf(stderr, "Finished waiting for dump thread\n");
}
#pragma warning (pop)

void DrLoggingInternal::ForkFlushThread()
{
    unsigned threadAddr;
    s_flushThread =
        (HANDLE) ::_beginthreadex(NULL,
                                  0,
                                  FlushThreadFunc,
                                  NULL,
                                  0,
                                  &threadAddr);
    assert(s_flushThread != 0);
}

void DrLoggingInternal::WaitForFlushThread()
{
    ::WaitForSingleObject(s_flushThread, INFINITE);
    ::CloseHandle(s_flushThread);
}

#endif

#ifndef _MANAGED
#pragma warning (push)
#pragma warning (disable: 4715 ) // 'Logger::LogAndExitProcess' : not all control paths return a value
#pragma warning (disable: 4702 ) // unreachable code
#pragma warning (disable: 4100 ) // unreferenced formal parameter
static LONG WINAPI LogAndExitProcess(EXCEPTION_POINTERS *exceptionPointers)
{
    fprintf(stderr, "An unhandled exception was thrown -- exiting process\n");
    DrLoggingInternal::FlushLogs();

    //if (WriteMiniDumpImpl(exceptionPointers))
    {
        DrLoggingInternal::Terminate(1);
    }

    return (EXCEPTION_CONTINUE_SEARCH);
}
#pragma warning (pop)
#endif

void DrLogHelper::operator()(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    DrString s;
    s.VSetF(format, args);

    SYSTEMTIME utc, local;
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    FileTimeToSystemTime(&ft, &utc);
    SystemTimeToTzSpecificLocalTime(NULL, &utc, &local);

    // For DrLog_Assert and DrLog_Error, write the message to stderr
    // so that it is displayed in task's output
    bool logToConsole = false;

    char initial = 0;
    switch (m_type)
    {
    case DrLog_Assert:
        initial = 'a';
        logToConsole = true;
        break;
    case DrLog_Error:
        initial = 'e';
        logToConsole = true;
        break;
    case DrLog_Warning:
        initial = 'w';
        break;
    case DrLog_Info:
        initial = 'i';
        break;
    case DrLog_Debug:
        initial = 'd';
        break;
    }

    DrString logEntry;
    logEntry.SetF(
        "%c,"
        "%02d/%02d/%04d %02d:%02d:%02d.%03u,"
        "TID=%d,%s,%s:%d,%s\r\n",
        initial,
        local.wMonth,
        local.wDay,
        local.wYear,
        local.wHour,
        local.wMinute,
        local.wSecond,
        local.wMilliseconds,
        GetCurrentThreadId(),
        m_function, m_file, m_line,
        s.GetChars()
    );


    if (logToConsole)
    {
        fprintf(stderr, logEntry.GetChars());
        fflush(stderr);
    }

    if (m_type != DrLog_Assert)
    {
        DrLoggingInternal::Append(logEntry.GetChars(), logEntry.GetCharsLength());
    }
    else
    {
#ifdef _MANAGED
        // Get a stack trace, excluding the current frame
        System::Diagnostics::StackTrace ^st = gcnew System::Diagnostics::StackTrace(1, true);
        char *pszStackTrace = (char*)(void*)System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(st->ToString()); 

        // Log it
        logEntry = logEntry.AppendF("%s", pszStackTrace);
        DrLoggingInternal::Append(logEntry.GetChars(), logEntry.GetCharsLength());

        // Terminate the graph manager
        DrLoggingInternal::Terminate((UINT)DrError_AssertFailure, gcnew QueryAssertException(s.GetString(), st));
#else

#endif
    }
}

#ifdef _MANAGED
void DrLogging::Initialize(System::String^ logPathManaged, bool redirectStdStreams)
{
#ifdef _DEBUG_DRREF
    InitializeCriticalSection(&DrRefCounter::s_debugCS);
#endif

    DrString logPath(logPathManaged);
    DrStaticFileWriters::Initialize();
    DrLoggingInternal::Initialize(logPath, redirectStdStreams);
    DrErrorText::Initialize();
}
#else
void DrLogging::Initialize(DrString logPath, bool redirectStdStreams)
{
#ifdef _DEBUG_DRREF
    InitializeCriticalSection(&DrRefCounter::s_debugCS);
#endif

    DrStaticFileWriters::Initialize();
    DrLoggingInternal::Initialize(logPath, redirectStdStreams);
    DrErrorText::Initialize();

    ::SetUnhandledExceptionFilter(LogAndExitProcess);
}
#endif


void DrLogging::ShutDown(UINT code)
{
    DrLogWithType(DrLog_Info)("------------- Shutting down logging ------------ ExitCode=%u", code);
    DrLoggingInternal::Stop();
    DrStaticFileWriters::Discard();
    DrErrorText::Discard();
}

void DrLogging::ShutDown(int code)
{
    DrLogWithType(DrLog_Info)("------------- Shutting down logging ------------ ExitCode=%d", code);
    DrLoggingInternal::Stop();
    DrStaticFileWriters::Discard();
    DrErrorText::Discard();
}

bool DrLogging::DebuggerIsPresent()
{
#ifdef _MANAGED
    return System::Diagnostics::Debugger::IsAttached;
#else
    return (::IsDebuggerPresent()) ? true : false;
#endif
}

#ifdef _MANAGED

void DrLogging::SetLoggingLevel(DrLogTypeManaged type)
{
    SetLoggingLevel((DrLogType)type);
}

static void LogFromManaged(DrLogType type,
                           System::String^ message,
                           System::String^ file,
                           System::String^ function,
                           int line)
{
    DrString sMessage(message);
    DrString sFile(file);
    DrString sFunction(function);
    DrLogHelper(type, sFile.GetChars(), sFunction.GetChars(), line)("%s", sMessage.GetChars());
}

void DrLogging::LogCritical(System::String^ message,
                            System::String^ file,
                            System::String^ function,
                            int line)
{
    LogFromManaged(DrLog_Error, message, file, function, line);
}

void DrLogging::LogWarning(System::String^ message,
                           System::String^ file,
                           System::String^ function,
                           int line)
{
    LogFromManaged(DrLog_Warning, message, file, function, line);
}

void DrLogging::LogInformation(System::String^ message,
                               System::String^ file,
                               System::String^ function,
                               int line)
{
    LogFromManaged(DrLog_Info, message, file, function, line);
}

#endif