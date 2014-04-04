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

#include <assert.h>
#include <stdio.h>
#include <share.h>

#define DrLogWithType(_x) DrLogHelper(_x,__FILE__,__FUNCTION__,__LINE__)

#define DRMAKELOGTYPE(_type,_initial) \
    

//
// Create logging calls at various levels and allow them to complete iff logging is enabled
// at the selected level
//
#define DrLogD if (DrLogging::Enabled(LogLevel_Debug)) DrLogWithType(LogLevel_Debug)
#define DrLogI if (DrLogging::Enabled(LogLevel_Info)) DrLogWithType(LogLevel_Info)
#define DrLogW if (DrLogging::Enabled(LogLevel_Warning)) DrLogWithType(LogLevel_Warning)
#define DrLogE if (DrLogging::Enabled(LogLevel_Error)) DrLogWithType(LogLevel_Error)
#define DrLogA if (DrLogging::Enabled(LogLevel_Assert)) DrLogWithType(LogLevel_Assert)

//
// Define the logging levels
//
typedef enum 
{
    LogLevel_Off = 0,
    LogLevel_Assert = 1,
    LogLevel_Error = 3,
    LogLevel_Warning = 7,
    LogLevel_Info = 15,
    LogLevel_Debug = 31
} LogLevel;

typedef void DrLogAssertCallback(void* cookie, const char* assertString);

//
// Expose functions to set the logging level, check the logging level, and flush the log
//
class DrLogging
{   
public:
    static void Initialize(const WCHAR* logFileName);
    static void SetLoggingLevel(LogLevel type);
    static bool Enabled(LogLevel type);
    static void FlushLog();
    static FILE* GetLogFile();

    static void SetAssertCallback(DrLogAssertCallback callback, void* cookie);

private:   
    static FILE* CreateLogFile(const WCHAR* logFileName);
    static FILE* m_logFile;
};

//
// Class that defines the logging context for a particular log call
//
class DrLogHelper
{
public:
    DrLogHelper(LogLevel type, const char* file, const char* function, int line)
    {
        m_type = type;
        m_file = file;
        m_function = function;
        m_line = line;
    }

    void operator()(const char* format, ...);

private:
    LogLevel    m_type;
    const char*  m_file;
    const char*  m_function;
    int          m_line;
};

//
// Define a helper that logs at the assert level if any conditional fails 
//
#define LogAssert(exp, ...) \
    do { \
        if (!(exp)) { \
            printf("Assert -- %s, %d: %s\n", __FILE__, __LINE__, #exp); \
            DrLogA(#exp, __VA_ARGS__ ); \
        } \
    } while (0)

#define DebugLogAssert(exp, ...)	LogAssert(#exp, __VA_ARGS__ )
