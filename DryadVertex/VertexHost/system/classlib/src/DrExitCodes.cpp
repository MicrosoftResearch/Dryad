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

struct ExitCodeEntry {
    DrExitCode value;
    const char *pszDescription;
};


struct ExitCodeHashEntry {
    DrExitCode value;
    const char *pszDescription;
    struct ExitCodeHashEntry *pNext;
};


static ExitCodeEntry g_DryadExitCodeMap[] = {
    
#ifdef DEFINE_DREXITCODE
#undef DEFINE_DREXITCODE
#endif
#ifdef DECLARE_DREXITCODE
#undef DECLARE_DREXITCODE
#endif
#define DEFINE_DREXITCODE(name, value, description) {(DrExitCode)(value), description},
#define DECLARE_DREXITCODE(valname, description) {(DrExitCode)(valname), description},

#include "DrExitCodes.h"

#undef DEFINE_DREXITCODE
#undef DECLARE_DREXITCODE
};


class DrExitCodeTable : public DrTempStringPool
{
public:
    static const int k_hashTableSize = 111;
    
public:
    DrExitCodeTable()
    {
        m_fInitialized = false;
        for (int i = 0; i < k_hashTableSize; i++) {
            m_pBucket[i] = NULL;
        }
    }

    ~DrExitCodeTable()
    {
        ExitCodeHashEntry *p;

        for (int i = 0; i < k_hashTableSize; i++) {
            while ((p = m_pBucket[i]) != NULL) {
                m_pBucket[i] = p->pNext;
                delete p;
            }
            
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
    
    static int IndexOf(DrExitCode code) {
        return (int)((DWORD)code % (DWORD)k_hashTableSize);
    }

    // Returns NULL if not found
    // Must be called under lock
    ExitCodeHashEntry *FindEntry(DrExitCode code)
    {
        int i = IndexOf(code);
        ExitCodeHashEntry *p = m_pBucket[i];
        while (p != NULL) {
            if (p->value == code) {
                return p;
            }
            p = p->pNext;
        }
        return NULL;
    }

    // returns HRESULT(ERROR_ALREADY_ASSIGNED) if the error has already been defined
    DrError AddEntry(DrExitCode code, const char *pszDescription)
    {
        DrError err = DrError_Fail;
        Lock();

        if (FindEntry(code) != NULL) {
            err = HRESULT_FROM_WIN32( ERROR_ALREADY_ASSIGNED );
            goto done;
        }

        int i = IndexOf(code);
        ExitCodeHashEntry *p = new ExitCodeHashEntry;
        LogAssert(p != NULL);
        p->value = code;
        p->pszDescription = dupStr(pszDescription);
        p->pNext = m_pBucket[i];
        m_pBucket[i] = p;
        err = DrError_OK;
        
    done:
        Unlock();
        return err;
    }

    inline void Initialize()
    {
        if (!m_fInitialized) {
            Lock();
            AddStaticExitCodes();
            m_fInitialized = true;
            Unlock();
        }
    }
    
    void AddStaticExitCodes()
    {
        int n = sizeof(g_DryadExitCodeMap) / sizeof(g_DryadExitCodeMap[0]);
        for (int i = 0; i < n; i++) {
            DrError err = AddEntry(g_DryadExitCodeMap[i].value, g_DryadExitCodeMap[i].pszDescription);
            LogAssert(err == DrError_OK);
        }
    }

    DrStr& AppendExitCodeDescription(DrStr& strOut, DrExitCode code)
    {
        Lock();
        Initialize();
        ExitCodeHashEntry *p = FindEntry(code);
        if (p != NULL) {
            strOut.Append(p->pszDescription);
        }
        Unlock();
        if (p == NULL) {
            if (code < 256) {
                strOut.AppendF("%u", code);
            } else {
                strOut.AppendF("0x%08x (%u)", code, code);
            }
        }
        return strOut;
    }
    
    DrStr& GetExitCodeDescription(DrStr& strOut, DrExitCode code)
    {
        strOut = "";
        return AppendExitCodeDescription(strOut, code);
    }
    
private:
    ExitCodeHashEntry *m_pBucket[k_hashTableSize];
    DrCriticalSection m_lock;
    bool m_fInitialized;
};


DrExitCodeTable g_csExitCodeTable;

void DrInitExitCodeTable()
{
    g_csExitCodeTable.Initialize();
}

DrError DrAddExitCodeDescription(DrExitCode code, const char *pszDescription)
{
    return g_csExitCodeTable.AddEntry(code, pszDescription);
}

DrStr& DrAppendExitCodeDescription(DrStr& strOut, DrExitCode code)
{
    return g_csExitCodeTable.AppendExitCodeDescription(strOut, code);
}

DrStr& DrGetExitCodeDescription(DrStr& strOut, DrExitCode code)
{
    return g_csExitCodeTable.GetExitCodeDescription(strOut, code);
}

