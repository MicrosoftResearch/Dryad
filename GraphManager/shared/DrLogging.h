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

#define DrLogWithType(_x) DrLogHelper(_x,__FILE__,__FUNCTION__,__LINE__)

#define DRMAKELOGTYPE(_type,_initial) \
    

#define DrLogD if (DrLogging::Enabled(DrLog_Debug)) DrLogWithType(DrLog_Debug)
#define DrLogI if (DrLogging::Enabled(DrLog_Info)) DrLogWithType(DrLog_Info)
#define DrLogW if (DrLogging::Enabled(DrLog_Warning)) DrLogWithType(DrLog_Warning)
#define DrLogE if (DrLogging::Enabled(DrLog_Error)) DrLogWithType(DrLog_Error)
#define DrLogA if (DrLogging::Enabled(DrLog_Assert)) DrLogWithType(DrLog_Assert)

DRPUBLICENUM(DrLogType)
{
    DrLog_Off = 0,
    DrLog_Assert = 1,
    DrLog_Error = 3,
    DrLog_Warning = 7,
    DrLog_Info = 15,
    DrLog_Debug = 31
};

#ifdef _MANAGED
public enum class DrLogTypeManaged
{
    Off = DrLog_Off,
    Assert = DrLog_Assert,
    Error = DrLog_Error,
    Warning = DrLog_Warning,
    Info = DrLog_Info,
    Debug = DrLog_Debug
};
#endif

DRCLASS(DrLogging)
{
public:
#ifdef _MANAGED
    static void Initialize(System::String^ logPath, bool redirectStdStreams);
#else
    static void Initialize(DrString logPath, bool redirectStdStreams);
#endif
    static void ShutDown(UINT code);
    static void ShutDown(int code);
    static void SetLoggingLevel(DrLogType type);
    static bool Enabled(DrLogType type);

    static bool DebuggerIsPresent();

    static bool WriteMiniDump();

#ifdef _MANAGED
    static void SetLoggingLevel(DrLogTypeManaged type);

    static void LogInformation(System::String^ message,
                               System::String^ file,
                               System::String^ function,
                               int line);
    static void LogWarning(System::String^ message,
                           System::String^ file,
                           System::String^ function,
                           int line);
    static void LogCritical(System::String^ message,
                            System::String^ file,
                            System::String^ function,
                            int line);
#endif

private:
    static void MiniDumpThread();
    static void WriteMiniDumpImpl();

};

DRCLASS(DrLogHelper)
{
public:
    DrLogHelper(DrLogType type, const char* file, const char* function, int line)
    {
        m_type = type;
        m_file = file;
        m_function = function;
        m_line = line;
    }

    void operator()(const char* format, ...);

private:
    DrLogType    m_type;
    const char*  m_file;
    const char*  m_function;
    int          m_line;
};


#ifdef _MANAGED

DRCLASS(QueryAssertException) : System::Exception
{
public:
    QueryAssertException(System::String ^message, System::Diagnostics::StackTrace ^st);

    virtual property System::String^ StackTrace
    {
        System::String^ get() override {return m_stackTrace->ToString(); }
    }

private:
    System::Diagnostics::StackTrace ^m_stackTrace;
};
#endif