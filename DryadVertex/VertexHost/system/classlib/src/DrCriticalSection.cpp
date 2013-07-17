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

void DrCriticalSectionBase::Init(
    __in PCSTR name,
    DWORD spinCount,
    bool logUsage,
    DrTimeInterval logHeldTooLongTimeout
    )
{
    _name = (name != NULL) ? name : "UnknownCritSec";
    _logUsage = logUsage;
    _logHeldTooLongTimeoutMs = DrGetTimerMsFromInterval(logHeldTooLongTimeout);
    _lastFunctionName = NULL;
    _lastFileName = NULL;
    _lastLineNumber = 0;
    _enterTimeMs = 0;
    LogAssert( InitializeCriticalSectionAndSpinCount( this, spinCount ) );
}

void DrCriticalSectionBase::SetCriticalSectionLoggingParameters(
    bool logUsage,
    DrTimeInterval logHeldTooLongTimeout
)
{
    _logHeldTooLongTimeoutMs = DrGetTimerMsFromInterval(logHeldTooLongTimeout);
    _logUsage = logUsage;
}

void DrCriticalSectionBase::SetCriticalSectionLogging(
    bool logUsage
)
{
    _logUsage = logUsage;
}

void DrCriticalSectionBase::SetCriticalSectionLogHeldTooLongTimeout(
    DrTimeInterval logHeldTooLongTimeout
)
{
    _logHeldTooLongTimeoutMs = DrGetTimerMsFromInterval(logHeldTooLongTimeout);
}

void DrCriticalSectionBase::Enter( PCSTR functionName, PCSTR fileName, UINT lineNumber )
{
    UInt32 tms = GetTickCount();

    EnterCriticalSection( this );

    _enterTimeMs = GetTickCount();

    tms = _enterTimeMs - tms;

    Int32 entryCount = -1;
    Int32 contentionCount = -1;
    if ( DebugInfo != NULL )
    {
        entryCount = (Int32)DebugInfo->EntryCount;
        contentionCount = (Int32)DebugInfo->ContentionCount;
    }

    _lastFunctionName = functionName;
    _lastFileName = fileName;
    _lastLineNumber = lineNumber;

    if ( tms > _logHeldTooLongTimeoutMs )
    {
        if (_lastFileName != NULL) {
            DrLogW( "CritSect WAITED TO ENTER TOO LONG %s at %s %s(%u), entryCount=%d, contentionCount=%d, waited for %ums, addr=%08Ix",
                _name, _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
        }
        else
        {
            DrLogW( "CritSect WAITED TO ENTER TOO LONG %s, entryCount=%d, contentionCount=%d, waited for %ums, addr=%08Ix",
                _name, entryCount, contentionCount, tms, this );
        }
    }

    if ( _logUsage )
    {
        if (_lastFileName != NULL) {
            DrLogD( "CritSect ENTER %s at %s %s(%u), entryCount=%d, contentionCount=%d, waited for %ums, addr=%08Ix",
                _name, _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
        }
        else
        {
            DrLogD( "CritSect ENTER %s at %s, entryCount=%d, contentionCount=%d, waited for %ums, addr=%08Ix",
                _name, _lastFunctionName, entryCount, contentionCount, tms, this );
        }
    }
}

void DrCriticalSectionBase::Leave( PCSTR functionName, PCSTR fileName, UINT lineNumber )
{
    DebugLogAssert( Aquired() );

    UInt32 tms = 0;
    if ( _enterTimeMs != 0 )        // technically wrong, but it will fail to complain on 1 out of 4 billion slow locks
    {
        tms = GetTickCount() - _enterTimeMs;
        _enterTimeMs = 0;
    }

    Int32 entryCount = -1;
    Int32 contentionCount = -1;
    if ( DebugInfo != NULL )
    {
        entryCount = (Int32)DebugInfo->EntryCount;
        contentionCount = (Int32)DebugInfo->ContentionCount;
    }

    if ( tms > _logHeldTooLongTimeoutMs )
    {
        if (fileName != NULL)
        {
            if (_lastFileName != NULL)
            {
                DrLogW( "CritSect LEAVE, HELD TOO LONG %s at %s %s(%u) entered at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name,  functionName, fileName, lineNumber, _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
            }
            else
            {
                DrLogW( "CritSect LEAVE, HELD TOO LONG %s at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name,  functionName, fileName, lineNumber, entryCount, contentionCount, tms, this );
            }
        }
        else
        {
            if (_lastFileName != NULL)
            {
                DrLogW( "CritSect LEAVE, HELD TOO LONG %s entered at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name,  _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
            }
            else
            {
                DrLogW( "CritSect LEAVE, HELD TOO LONG %s, entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name, entryCount, contentionCount, tms, this );
            }
        }
    }

    if ( _logUsage )
    {
        if (fileName != NULL)
        {
            if (_lastFileName != NULL)
            {
                DrLogD( "CritSect LEAVE %s at %s %s(%u) entered at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name,  functionName, fileName, lineNumber, _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
            }
            else
            {
                DrLogD( "CritSect LEAVE %s at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name,  functionName, fileName, lineNumber, entryCount, contentionCount, tms, this );
            }
        }
        else
        {
            if (_lastFileName != NULL)
            {
                DrLogD( "CritSect LEAVE %s entered at %s %s(%u), entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name, _lastFunctionName, _lastFileName, _lastLineNumber, entryCount, contentionCount, tms, this );
            }
            else
            {
                DrLogD( "CritSect LEAVE %s, entryCount=%d, contentionCount=%d, time held=%ums, addr=%08Ix",
                    _name, entryCount, contentionCount, tms, this );
            }
        }
    }

    LeaveCriticalSection( this );
}


