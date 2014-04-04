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
#include "dryadstandaloneini.h"
#include "dryadnativeport.h"
#include "dryaderrordef.h"
#include "dryadmetadata.h"

#pragma unmanaged

DryadNativePort* g_dryadNativePort;

static bool g_initialized = false;

//
// Output stream files
//
FILE* g_oldStdout = NULL;
FILE* g_oldStderr = NULL;

//
// Attempts to open stdout.txt and stderr.txt in current directory
// If successful, redirects output streams to these files. 
// If unsucessful, no error, just retains original stdout/stderr streams
// _wfreopen_s locks the logs so that they cannot be read while the vertex is running.
//
#pragma warning (disable: 4996) // _wfreopen : This function may be unsafe, consider using _wfreopen_s
static void RedirectOutputStreams()
{
    WCHAR szCurrentDir[MAX_PATH + 1] = {0};
    WCHAR szStdout[MAX_PATH + 1] = {0};
    WCHAR szStderr[MAX_PATH + 1] = {0};

    if (GetCurrentDirectoryW(MAX_PATH, szCurrentDir) != 0)
    {
        if (S_OK == StringCchPrintfW(szStdout, MAX_PATH, L"%s\\stdout.txt", szCurrentDir))
        {
            g_oldStdout = _wfreopen(szStdout, L"w", stdout);
        }
        if (S_OK == StringCchPrintfW(szStderr, MAX_PATH, L"%s\\stderr.txt", szCurrentDir))
        {
            g_oldStderr = _wfreopen(szStderr, L"w", stderr);
        }
    }
}

//
// Initialize
//
DrError DryadInitialize(int argc, char* argv[], int* pNOpts)
{
    DrError err = DrError_OK; 
    *pNOpts = 0;

    //
    // Only initialize cluster once
    //
    if (g_initialized)
    {
        return DryadError_AlreadyInitialized;
    }

    g_initialized = true;

    if (argc > 1 && strcmp(argv[1], "--noredirect") == 0)
    {
        *pNOpts = 1;
    }
    else
    {
        //
        // Set up std.out and std.err files in current directory and redirect to them
        //
        RedirectOutputStreams();
    }

    //
    // Initialize the tables defining the metadata
    //
    DryadInitMetaDataTable();

    //
    // Create a port and start it
    //
    g_dryadNativePort = new DryadNativePort(4, 2);
    g_dryadNativePort->Start();

    return err;
}

//
// Shut down the completion port and cluster session
//
DrError DryadShutdown()
{
    DrError err = DrError_OK;

    //
    // Get the number of outstanding requests remaining
    // todo: do we want this log? if so, use logging framework 
    //
    UInt32 n = g_dryadNativePort->GetOutstandingRequests();
    fprintf(stdout, "DryadShutdown:  %u outstanding requests\n", n);
    while (n > 0)
    {
        //
        // While there are requests remaining, wait 10 seconds and try again
        // todo: do we want this log? probably not since this could be indefinite
        // todo: is 10 seconds appropriate?
        //
        Sleep(10);
        n = g_dryadNativePort->GetOutstandingRequests();
        fprintf(stdout, "DryadShutdown:  %u outstanding requests\n", n);
    }
    
    return err;
}