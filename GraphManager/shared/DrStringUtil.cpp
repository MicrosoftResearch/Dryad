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

#include <lmerr.h>
#include <winhttp.h>

struct DrErrorDescription
{
    HRESULT       m_error;
    const char*   m_text;
};

static const DrErrorDescription s_errorTable[] =
{
    { DrError_BadMetaData, "Bad MetaData XML" },
    { DrError_InvalidCommand, "Invalid Command" },
    { DrError_VertexReceivedTermination, "Vertex Received Termination" },
    { DrError_InvalidChannelURI, "Invalid Channel URI syntax" },
    { DrError_ChannelOpenError, "Channel Open Error" },
    { DrError_ChannelRestartError, "Channel Restart Error" },
    { DrError_ChannelWriteError, "Channel Write Error" },
    { DrError_ChannelReadError, "Channel Read Error" },
    { DrError_ItemParseError, "Item Parse Error" },
    { DrError_ItemMarshalError, "Item Marshal Error" },
    { DrError_BufferHole, "Buffer Hole" },
    { DrError_ItemHole, "Item Hole" },
    { DrError_ChannelRestart, "Channel Sent Restart" },
    { DrError_ChannelAbort, "Channel Sent Abort" },
    { DrError_VertexRunning, "Vertex Is Running" },
    { DrError_VertexCompleted, "Vertex Has Completed" },
    { DrError_VertexError, "Vertex Had Errors" },
    { DrError_ProcessingError, "Error While Processing" },
    { DrError_VertexInitialization, "Vertex Could Not Initialize" },
    { DrError_ProcessingInterrupted, "Processing was interrupted before completion" },
    { DrError_VertexChannelClose, "Errors during channel close" },
    { DrError_AssertFailure, "Assertion Failure" },
    { DrError_ExternalChannel, "External Channel" },
    { DrError_AlreadyInitialized, "Dryad Already Initialized" },
    { DrError_DuplicateVertices, "Duplicate Vertices" },
    { DrError_ComposeRHSNeedsInput, "RHS of composition must have at least one input" },
    { DrError_ComposeLHSNeedsOutput, "LHS of composition must have at least one output" },
    { DrError_ComposeStagesMustBeDifferent, "Stages for composition must be different" },
    { DrError_ComposeStageEmpty, "Stage for composition is empty" },
    { DrError_VertexNotInGraph, "Vertex not in graph" },
    { DrError_HardConstraintCannotBeMet, "Hard constraint cannot be met" },
    { DrError_ClusterError, "Cluster error" },
    { DrError_CohortShutdown, "Cohort shutdown" },
    { DrError_Unexpected, "Unexpected" },
    { DrError_DependentVertexFailure, "Dependent vertex failure" },
    { DrError_BadOutputReported, "Bad output reported" },
    { DrError_InputUnavailable, "Input unavailable" },

    { DrError_EndOfStream, "End of stream" },

    { DrError_CannotConnectToDsc, "Failed to connect to DSC" },
    { DrError_DscOperationFailed, "DSC operation failed" },
    { DrError_FailedToDeleteFileset, "Failed to delete DSC fileset" },
    { DrError_FailedToCreateFileset, "Failed to create DSC fileset" },
    { DrError_FailedToAddFile, "Failed to add file to DSC fileset" },
    { DrError_FailedToSetMetadata, "Failed to set metadata for DSC fileset" },
    { DrError_FailedToSealFileset, "Failed to seal DSC fileset" },
    { DrError_FailedToSetLease, "Failed to set lease for DSC fileset" },
    { DrError_FailedToOpenFileset, "Failed to open DSC fileset" },

};

#ifdef _MANAGED

DRCLASS(DrSystemErrorText)
{
public:
    static void Initialize()
    {
    }

    static void Discard()
    {
    }

    static DrString GetSystemErrorText(DWORD dwError)
    {
        HRESULT hr = HRESULT_FROM_WIN32(dwError);
        System::Exception ^e = System::Runtime::InteropServices::Marshal::GetExceptionForHR(hr);
        return DrString(e->Message);
    }
};

#else

DRCLASS(DrSystemErrorText)
{
public:
    static void Initialize()
    {
        s_cs = DrNew DrCritSec();
        s_hModuleNetMsg = NULL;
        s_hModuleWinHttp = NULL;
    }

    static void Discard()
    {
        s_cs = DrNull;
    }

    static DrString GetSystemErrorText(DWORD dwError)
    {
        LPSTR MessageBuffer;
        DWORD dwBufferLength;
        HANDLE hModule = NULL;

        DWORD dwUse = dwError;
        DWORD dwNormalized = dwError;
        if ((dwNormalized & 0xFFFF0000) == ((FACILITY_WIN32 << 16) | 0x80000000)) {
            dwNormalized = dwNormalized & 0xFFFF;
        }

        DWORD dwFormatFlags = FORMAT_MESSAGE_ALLOCATE_BUFFER |
            FORMAT_MESSAGE_IGNORE_INSERTS |
            FORMAT_MESSAGE_FROM_SYSTEM ;

        //
        // If dwLastError is in the network range, 
        //  load the message source.
        //

        if (dwNormalized >= NERR_BASE && dwNormalized <= MAX_NERR) {
            {
                DrAutoCriticalSection((DrCritSecPtr)s_cs);

                if (s_hModuleNetMsg == NULL) {
                    s_hModuleNetMsg = LoadLibraryEx(
                        TEXT("netmsg.dll"),
                        NULL,
                        LOAD_LIBRARY_AS_DATAFILE
                        );
                }

                if(s_hModuleNetMsg != NULL) {
                    dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
                    hModule = s_hModuleNetMsg;
                }
            }
        } else if (dwNormalized >= WINHTTP_ERROR_BASE && dwNormalized <= WINHTTP_ERROR_LAST) {
            {
                DrAutoCriticalSection((DrCritSecPtr)s_cs);

                if (s_hModuleWinHttp == NULL) {
                    s_hModuleWinHttp = LoadLibraryEx(
                        TEXT("winhttp.dll"),
                        NULL,
                        LOAD_LIBRARY_AS_DATAFILE
                        );
                }

                if(s_hModuleWinHttp != NULL) {
                    dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
                    hModule = s_hModuleWinHttp;
                    dwUse = dwNormalized;
                }
            }
        }

        //
        // Call FormatMessage() to allow for message 
        //  text to be acquired from the system 
        //  or from the supplied module handle.
        //
        // For perf, we assume here that all ANSI error messages are also valid UTF-8. If
        // this turns out not to be true, we'll have to get the unicode message and convert to UTF-8
        if ((dwBufferLength = FormatMessageA(
            dwFormatFlags,
            hModule, 
            dwUse,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // default language
            (LPSTR) &MessageBuffer,
            0,
            NULL
            )) != 0) {
                char *pszBuffer = (char *)malloc(dwBufferLength + 1);
                DrAssert(pszBuffer != NULL);
                memcpy(pszBuffer, MessageBuffer, dwBufferLength+1);
                LocalFree(MessageBuffer);

                DWORD i;
                for (i = 0; i < dwBufferLength; ++i)
                    if (pszBuffer[i] == '\r' || pszBuffer[i] == '\n')
                        pszBuffer[i] = ' ';
                DrString s = DrString(pszBuffer);
                free((void*)pszBuffer);
                return s;
        }

        DrString s = DrString();
        s.SetF("Error code %u (0x%08x)", dwError, dwError);
        return s;
    }

private:
    static DrCritSecRef          s_cs;
    static HMODULE               s_hModuleNetMsg;
    static HMODULE               s_hModuleWinHttp;
};

DrCritSecRef DrSystemErrorText::s_cs;
HMODULE DrSystemErrorText::s_hModuleNetMsg;
HMODULE DrSystemErrorText::s_hModuleWinHttp;

DrErrorDictionaryRef DrErrorText::s_dictionary;

#endif

void DrErrorText::Initialize()
{
    DrSystemErrorText::Initialize();

    s_dictionary = DrNew DrErrorDictionary();
    int i;
    for (i=0; i<sizeof(s_errorTable)/sizeof(DrErrorDescription); ++i)
    {
        DrString s;
        s.SetF("%s", s_errorTable[i].m_text);
        s_dictionary->Add(s_errorTable[i].m_error, s);
    }
}

void DrErrorText::Discard()
{
    DrSystemErrorText::Discard();
    s_dictionary = DrNull;
}

DrString DrErrorText::GetErrorText(HRESULT err)
{
    DrString text;
    if (s_dictionary->TryGetValue(err, text))
    {
        return text;
    }
    else
    {
        return DrSystemErrorText::GetSystemErrorText(err);
    }
}

const char* DrErrorString::GetChars(HRESULT err)
{
    if (err == S_OK)
    {
        /* common case */
        m_string.T() = "No Error";
    }
    else
    {
        m_string.T() = DrErrorText::GetErrorText(err);
    }

    return m_string.T().GetChars();
}
