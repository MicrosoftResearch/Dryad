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
#include <lmerr.h>
#include <winhttp.h>

#pragma unmanaged

typedef struct {
    DrError value;
    const char *pszDescription;
} ErrEntry;


typedef struct _ErrHashEntry {
    DrError value;
    const char *pszDescription;
    struct _ErrHashEntry *pNext;
} ErrHashEntry;


#define FACILITY_DRYAD   778
#define DRYAD_ERROR(n) ((HRESULT)(0x80000000 + (FACILITY_DRYAD << 16) + n))

#ifdef DEFINE_DRYAD_ERROR
#undef DEFINE_DRYAD_ERROR
#endif

#define DEFINE_DRYAD_ERROR(name, number, description) const DrError name = number;

#include "..\..\common\include\dryaderror.h"

#undef DEFINE_DRYAD_ERROR

static ErrEntry g_DryadErrorMap[] = {

#undef DEFINE_DR_ERROR
#undef COMMON_DR_ERRORS_DEFINED

#define DEFINE_DR_ERROR(name, number, description) {name, description},

#include "DrError.h"

#undef DEFINE_DR_ERROR

#ifdef DEFINE_DRYAD_ERROR
#undef DEFINE_DRYAD_ERROR
#endif

#define DEFINE_DRYAD_ERROR(name, number, description) {name, description},

#include "..\..\common\include\dryaderror.h"

#undef DEFINE_DRYAD_ERROR
};


class DrErrorTable : public DrTempStringPool
{
public:
    static const DWORD k_hashTableSize = 111;
    
public:
    DrErrorTable()
    {
        m_fInitialized = false;
        for (int i = 0; i < k_hashTableSize; i++) {
            m_pBucket[i] = NULL;
        }
        m_hModuleNetMsg = NULL;
        m_hModuleWinHttp = NULL;
    }

    ~DrErrorTable()
    {
        ErrHashEntry *p;

        for (int i = 0; i < k_hashTableSize; i++) {
            while ((p = m_pBucket[i]) != NULL) {
                m_pBucket[i] = p->pNext;
                delete p;
            }
            
        }
        
        if (m_hModuleNetMsg != NULL) {
            FreeLibrary(m_hModuleNetMsg);
        }

        if (m_hModuleWinHttp != NULL) {
            FreeLibrary(m_hModuleWinHttp);
        }
    }

    void Lock()
    {
        m_lock.Enter();
    }

    void Unlock()
    {
        m_lock.Leave();
    }
    
    // The returned error string should be freed with free();
    //
    // If the error code is unknown, a generic error description is returned.
    char *GetSystemErrorText(DWORD dwError)
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
            Lock();
            if (m_hModuleNetMsg == NULL) {
                m_hModuleNetMsg = LoadLibraryEx(
                    TEXT("netmsg.dll"),
                    NULL,
                    LOAD_LIBRARY_AS_DATAFILE
                    );
            }
            Unlock();

            if(m_hModuleNetMsg != NULL) {
                dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
                hModule = m_hModuleNetMsg;
            }
        } else if (dwNormalized >= WINHTTP_ERROR_BASE && dwNormalized <= WINHTTP_ERROR_LAST) {
            Lock();
            if (m_hModuleWinHttp == NULL) {
                m_hModuleWinHttp = LoadLibraryEx(
                    TEXT("winhttp.dll"),
                    NULL,
                    LOAD_LIBRARY_AS_DATAFILE
                    );
            }
            Unlock();

            if(m_hModuleWinHttp != NULL) {
                dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
                hModule = m_hModuleWinHttp;
                dwUse = dwNormalized;
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
            LogAssert(pszBuffer != NULL);
            memcpy(pszBuffer, MessageBuffer, dwBufferLength+1);
            LocalFree(MessageBuffer);

            DWORD i;
            for (i = 0; i < dwBufferLength; ++i)
                if (pszBuffer[i] == '\r' || pszBuffer[i] == '\n')
                    pszBuffer[i] = ' ';

            return pszBuffer;
        }


        char *pszBuffer = (char *)malloc(64);
        LogAssert(pszBuffer != NULL);
        _snprintf(pszBuffer, 64, "Error code %u (0x%08x)", dwError, dwError);
        return pszBuffer;
    }

    // The returned error string should be freed with free();
    //
    // If the error code is unknown, a generic error description is returned.
/*JC
    WCHAR *GetSystemErrorTextW(DWORD dwError)
    {
        LPWSTR MessageBuffer;
        DWORD dwBufferLength;

        DWORD dwFormatFlags = FORMAT_MESSAGE_ALLOCATE_BUFFER |
            FORMAT_MESSAGE_IGNORE_INSERTS |
            FORMAT_MESSAGE_FROM_SYSTEM ;

        //
        // If dwLastError is in the network range, 
        //  load the message source.
        //

        if (dwError >= NERR_BASE && dwError <= MAX_NERR) {
            Lock();
            if (m_hModuleNetMsg == NULL) {
                m_hModuleNetMsg = LoadLibraryEx(
                    TEXT("netmsg.dll"),
                    NULL,
                    LOAD_LIBRARY_AS_DATAFILE
                    );
            }
            Unlock();

            if(m_hModuleNetMsg != NULL) {
                dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
            }
    }

        //
        // Call FormatMessage() to allow for message 
        //  text to be acquired from the system 
        //  or from the supplied module handle.
        //

        if ((dwBufferLength = FormatMessageW(
                    dwFormatFlags,
                    m_hModuleNetMsg, 
                    dwError,
                    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // default language
                    (LPWSTR) &MessageBuffer,
                    0,
                    NULL
                )) != 0) {

            WCHAR *pszBuffer = (WCHAR *)malloc((dwBufferLength + 1) * sizeof(WCHAR));
            LogAssert(pszBuffer != NULL);
            memcpy(pszBuffer, MessageBuffer, (dwBufferLength+1) * sizeof(WCHAR));
            LocalFree(MessageBuffer);
            return pszBuffer;
        }


        WCHAR*pszBuffer = (WCHAR *)malloc(64 * sizeof(WCHAR));
        LogAssert(pszBuffer != NULL);
        _snwprintf(pszBuffer, 64, L"Error code %u (0x%08x)", dwError, dwError);
        return pszBuffer;
    }
*/

    static DWORD IndexOf(DrError err) {
        return ((DWORD)err % k_hashTableSize);
    }

    // Returns NULL if not found
    // Must be called under lock
    ErrHashEntry *FindEntry(DrError err)
    {
        DWORD dw = IndexOf(err);
        ErrHashEntry *p = m_pBucket[dw];
        while (p != NULL) {
            if (p->value == err) {
                return p;
            }
            p = p->pNext;
        }
        return NULL;
    }

    // returns ERROR_ALREADY_ASSIGNED if the error has already been defined
    DrError AddEntry(DrError code, const char *pszDescription)
    {
        DrError err = DrError_Fail;
        Lock();

        if (FindEntry(code) != NULL) {
            err = HRESULT_FROM_WIN32( ERROR_ALREADY_ASSIGNED );
            goto done;
        }
        DWORD dw = IndexOf(code);
        ErrHashEntry *p = new ErrHashEntry;
        LogAssert(p != NULL);
        p->value = code;
        p->pszDescription = dupStr(pszDescription);
        p->pNext = m_pBucket[dw];
        m_pBucket[dw] = p;
        err = DrError_OK;
        
    done:
        Unlock();
        return err;
    }

    // The returned error string should be freed with free();
    //
    // If the error code is unknown, a generic error description is returned.
    char *GetErrorText(DrError err)
    {
        char *pszText = NULL;
        Lock();
        Initialize();
        ErrHashEntry *p = FindEntry(err);
        if (p != NULL) {
            const char *pszOrig = p->pszDescription;
            if (pszOrig != NULL) {
                Size_t len = strlen(pszOrig)+1;
                pszText = (char *)malloc(len);
                memcpy(pszText, pszOrig, len);
            }
        }
        Unlock();
        if (pszText == NULL) {
            pszText = GetSystemErrorText((DWORD) err);
        }
        LogAssert(pszText != NULL);
        return pszText;
    }

/*JC
    WCHAR *GetErrorTextW(DrError err)
    {
        WCHAR *pszText = NULL;
        Lock();
        Initialize();
        ErrHashEntry *p = FindEntry(err);
        if (p != NULL) {
            const char *pszOrig = p->pszDescription;
            if (pszOrig != NULL) {
                DrWStr128 wstrText(pszOrig);
                Size_t len = wstrText.GetLength()+1;
                pszText = (WCHAR *)malloc(len * sizeof(WCHAR));
                memcpy(pszText, wstrText.GetString(), len * sizeof(WCHAR));
            }
        }
        Unlock();
        if (pszText == NULL) {
            pszText = GetSystemErrorTextW((DWORD) err);
        }
        LogAssert(pszText != NULL);
        return pszText;
    }
*/
    inline void Initialize()
    {
        if (!m_fInitialized) {
            Lock();
            AddStaticCodes();
            m_fInitialized = true;
            Unlock();
        }
    }
    
    void AddStaticCodes()
    {
        int n = sizeof(g_DryadErrorMap) / sizeof(g_DryadErrorMap[0]);
        for (int i = 0; i < n; i++) {
            DrError err = AddEntry(g_DryadErrorMap[i].value, g_DryadErrorMap[i].pszDescription);
            if (err != DrError_OK) {
                DrLogE( "DrErrorTable::AddStaticCodes - Could not add error code 0x%08x: [%s] to table, error: 0x%08x", g_DryadErrorMap[i].value, g_DryadErrorMap[i].pszDescription, err);
            }
            LogAssert(err == DrError_OK);
        }
}
    
private:
    ErrHashEntry *m_pBucket[k_hashTableSize];
    HMODULE m_hModuleNetMsg;
    HMODULE m_hModuleWinHttp;
    DrCriticalSection m_lock;
    bool m_fInitialized;
};


DrErrorTable g_csErrorTable;

// The returned error string should be freed with free();
//
// If the error code is unknown, a generic error description is returned.
char *DrGetErrorText(DrError err)
{
    return g_csErrorTable.GetErrorText(err);
}

// The returned error string should be freed with free();
//
// If the error code is unknown, a generic error description is returned.
/*JC
WCHAR *DrGetErrorTextW(DrError err)
{
    return g_csErrorTable.GetErrorTextW(err);
}
*/

void DrInitErrorTable(void)
{
    g_csErrorTable.Initialize();
}

extern DrError DrAddErrorDescription(DrError code, const char *pszDescription)
{
    return g_csErrorTable.AddEntry(code, pszDescription);
}

// The buffer must be at least 64 bytes long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
const char *DrGetErrorDescription(DrError err, char *pBuffer, int buffLen)
{
    if (buffLen < 64) {
        return "Error Description Buffer too short!";
    }
    char *pszText = DrGetErrorText(err);
    LogAssert(pszText != NULL);
    Size_t len = strlen(pszText)+1;
    if ((int) len <= buffLen) {         // AKadatch: don't use "int". // TODO: fix this hack.
        memcpy(pBuffer, pszText, len);
    } else {
        _snprintf(pBuffer, (size_t) (buffLen-1), "Error code %u (0x%08x)", err, err);
    }

    free(pszText);
    
    return pBuffer;
}

// The buffer must be at least 64 chars long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
/* JC
const WCHAR *DrGetErrorDescription(DrError err, WCHAR *pBuffer, int buffLen)
{
    if (buffLen < 64) {
        return L"Error Description Buffer too short!";
    }
    WCHAR *pszText = DrGetErrorTextW(err);
    LogAssert(pszText != NULL);
    Size_t len = wcslen(pszText)+1;
    if ((int) len <= buffLen) {         // AKadatch: don't use "int". // TODO: fix this hack.
        memcpy(pBuffer, pszText, len * sizeof(WCHAR));
    } else {
        _snwprintf(pBuffer, (size_t) (buffLen-1), L"Error code %u (0x%08x)", err, err);
    }

    free(pszText);
    
    return pBuffer;
}
*/

// The buffer must be at least 64 bytes long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
DrStr& DrAppendErrorDescription(DrStr& strOut, DrError err)
{
    char *pszText = DrGetErrorText(err);
    LogAssert(pszText != NULL);
    strOut.Append(pszText);
    free(pszText);
    return strOut;
}

// The buffer must be at least 64 chars long to guarantee a result. If the result won't fit in the buffer, a generic
// error message is generated.
/* JC
DrWStr& DrAppendErrorDescription(DrWStr& strOut, DrError err)
{
    WCHAR *pszText = DrGetErrorTextW(err);
    LogAssert(pszText != NULL);
    strOut.Append(pszText);
    free(pszText);
    return strOut;
}
*/


