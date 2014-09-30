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

#include "stdafx.h"

#include <lm.h>
#include <lmshare.h>

#include <dryaderrordef.h>
#include <ManagedWrapper.h>
#include <mscoree.h>
#include <wrappernativeinfo.h>

#pragma managed

#pragma warning(disable:4947) // so that we can use Assembly::LoadWithPartialName()

static DataBlockParserFactory s_FactoryDataBlockParser;
static DataBlockMarshalerFactory s_FactoryDataBlockMarshaler;

DrCriticalSection ManagedWrapperVertex::m_atomic = DrCriticalSection("ManagedWrapperVertex");


ManagedWrapperVertex::ManagedWrapperVertex()
{
    SetCommonParserFactory(&s_FactoryDataBlockParser);
    SetCommonMarshalerFactory(&s_FactoryDataBlockMarshaler);
}

FactoryMWrapper s_factoryHWrapper("MW");

//
// Convert ANSI string to WCHAR*
//
LPCWSTR makeWStr(const char * ansiStr)
{
    int lenA = lstrlenA(ansiStr);
    int lenW;
    LPWSTR unicodeStr;

    //
    // Call MultiByteToWideChar once with 0 for last arg, to get wchar length of converted string.
    //
    lenW = ::MultiByteToWideChar(CP_UTF8, 0, ansiStr, lenA, 0, 0);

    //
    // Check conversion was successful.
    //
    LogAssert(lenW > 0);
    unicodeStr = ::SysAllocStringLen(0, lenW);
    ::MultiByteToWideChar(CP_UTF8, 0, ansiStr, lenA, unicodeStr, lenW);
    return unicodeStr;
}

//
// Converts the current local directory to a UNC path by correlating it to the actual nw share
// Assumes the current directory is under a NW share, specified by pwszNWShareName.
// 
// Examples for conversion:
//  pwszNWShareName = "HPCTEMP" (shared from c:\HPCTEMP)
//  actual CWD = "c:\HPCTEMP\USERNAME\1234\56"
//  returned value "\\hostname\HPCTEMP\USERNAME\1234\56"
//
//or
//  pwszNWShareName = L"HPCTEMP" (shared from c:\FOO\BAR)
//  actual CWD = "c:\FOO\BAR\USERNAME\1234\56"
//  returned value "\\hostname\HPCTEMP\USERNAME\1234\56"
//
BOOL ConvertCurrentDirToUNCPath(WCHAR *pwszNWShareName, WCHAR *pwszUncPath, DWORD dwUncPathLen)
{
    BOOL bSuccess = FALSE;
    PSHARE_INFO_2 pShareInfoBuf = NULL;
    do
    {
        // First get the local path from which \\hostname\HPCTEMP is shared from
        if( NetShareGetInfo(NULL, pwszNWShareName, 2, (LPBYTE*) &pShareInfoBuf) != ERROR_SUCCESS)
            break;

        WCHAR *pwszShareLocalPath = pShareInfoBuf->shi2_path;

        WCHAR wszCurrentDir[MAX_PATH+1];
        ZeroMemory(wszCurrentDir, sizeof(wszCurrentDir));
        DWORD dwCurrentDirLen = GetCurrentDirectory(_countof(wszCurrentDir), wszCurrentDir);			// this should give us something like "c:\hpctemp\<USER>\<JOBID>\<VERTEXID>"
        if (dwCurrentDirLen == 0)
            break;

        DWORD dwShareLocalPathLen = (DWORD) wcslen(pwszShareLocalPath);
        
        if( dwShareLocalPathLen >= dwCurrentDirLen)     // current directory must be longer than share local path, otherwise something is off.
            break;
        
        // convert everything to upper case before comparisons
        _wcsupr(pwszShareLocalPath);
        _wcsupr(wszCurrentDir);

        // search for the share local path as a substring of current dir
        WCHAR *pPos = wcsstr(wszCurrentDir, pwszShareLocalPath);
        if( pPos != wszCurrentDir )     // current directory must contain share local path at its starting position, if not cwd is not under the share. 
            break;

        // now everything checks out, we can truncate the current dir to get the part that goes after the UNC share.
        WCHAR *pwszTruncatedCurrentDir = wszCurrentDir + dwShareLocalPathLen;

        // uncomment for DNS hostname support
        //WCHAR wszComputerName[DNS_MAX_LABEL_BUFFER_LENGTH];
        WCHAR wszComputerName[MAX_COMPUTERNAME_LENGTH+1];
        DrError drErr = DrGetComputerName(wszComputerName) ;
        if(drErr != DrError_OK)
        {
            break;
        }

        // and do the final formatting to produce the UNC path
        if (_snwprintf(pwszUncPath, dwUncPathLen, L"\\\\%s\\%s%s", wszComputerName, pwszNWShareName, pwszTruncatedCurrentDir) <= 0)
            break;

        bSuccess = TRUE;
    }
    while(FALSE);

    // we need to free all buffers returned by Net* APIs
    if (pShareInfoBuf != NULL)
    {
        NetApiBufferFree(pShareInfoBuf);
    }

    return bSuccess;
}



//
// Invoke user vertex code
//
void ManagedWrapperVertex::Main(WorkQueue* workQueue,
                                UInt32 numberOfInputChannels,
                                RChannelReader** inputChannel,
                                UInt32 numberOfOutputChannels,
                                RChannelWriter** outputChannel)
{
    DrLogI("Starting ManagedWrapperVertex Main with %u arguments", GetArgumentCount());
    LogAssert(GetArgumentCount() >= 4);

    //
    // Create an object encapsulating all the native stuff:
    //
    WrapperNativeInfo *nativeInfo =
        new WrapperNativeInfo(numberOfInputChannels,
                              inputChannel,
                              numberOfOutputChannels,
                              outputChannel,
                              this, workQueue);

    //
    // Set the compression mode for all the channels
    //
    for (UInt32 i = 0; i < numberOfInputChannels; i++)
    {
        int tt = static_cast<int>(inputChannel[i]->GetTransformType());
        if (tt != 0) // only enable the FIFO channels if we actually have a transform
        {
            nativeInfo->EnableFifoInputChannel(tt, i);
        }
    }

    for (UInt32 i = 0; i < numberOfOutputChannels; i++)
    {
        int tt = static_cast<int>(outputChannel[i]->GetTransformType());
        if (tt != 0) // only enable the FIFO channels if we actually have a transform
        {
           nativeInfo->EnableFifoOutputChannel(tt, i);
        }
    }

    System::String^ logName = System::String::Format("vertex-{0}-{1}-LinqLog.txt", GetVertexId(), GetVertexVersion());
    System::String^ logDirectory = System::Environment::GetEnvironmentVariable("LOG_DIRS");
    if (logDirectory != nullptr)
    {
        logDirectory = logDirectory->Split(',')[0]->Trim();
        logName = System::IO::Path::Combine(logDirectory, logName);
    }

	int threadsPerWorker = 1;
	System::String^ threadsPerWorkerStr = System::Environment::GetEnvironmentVariable("DRYAD_THREADS_PER_WORKER");
    if (threadsPerWorkerStr != nullptr)
    {
		threadsPerWorker = Int32::Parse(threadsPerWorkerStr);
    }
    DrLogI("ManagedWrapperVertex: threadsPerWorker %u", threadsPerWorker);

    DrLogI("ManagedWrapperVertex: %p %u %u", nativeInfo, numberOfInputChannels, numberOfOutputChannels);
    DrLogI("ManagedWrapperVertex: Calling %s.%s", GetArgument(2), GetArgument(3));
    DrLogging::FlushLog();

    DrStr128 errorMsg;
    DrError error;

    {
        //
        // Instead of invoking the vertex entry point directly from here, we delegate it to the bridge method in the Microsoft.Research.DryadLinq assembly, specifically:
        //     static int Microsoft.Research.DryadLinq.Internal.VertexEnv.VertexBridge(string vertexBridgeArgs)
        //
        // This indirect method of invoking the vertex entry point is used so that any type load / assembly load problems coming from user code 
        // can be caught and reported with full details using the same mechanism that other vertex failures go through (exception dumped into vertexexception.txt etc.)
        //
        // The format of vertexBridgeArgs is simply a comma separated string packing vertex assembly, class, method name, and the *actual* vertex method args (==the native channel string)
        //     L"<vertexAssembly>,<vertexClassName>,<vertexMethodName>,<vertexMethodArgs>"
        //
        System::String^ classFullName = gcnew System::String(GetArgument(2));
        System::String^ assemblyName = classFullName->Substring(0, classFullName->LastIndexOf('.'));
        System::String ^bridgeAssemblyName = gcnew System::String(assemblyName);
        System::String ^bridgeClassName = gcnew System::String(assemblyName + ".Internal.VertexEnv");
        System::String ^bridgeMethodName = gcnew System::String(L"VertexBridge");

        //
        // Construct the actual vertex methods args from the native information (the "native channel string")
        //
        System::Text::StringBuilder ^vertexMethodArgs = gcnew System::Text::StringBuilder();
        System::IntPtr ^nativeInfoIntPtr = gcnew System::IntPtr((void*) nativeInfo);
        
        vertexMethodArgs->Append(nativeInfoIntPtr->ToString("X")); //use hex format, because that's what the vertex env uses when converting it back to a handle
        for (UInt32 i = 4; i < GetArgumentCount(); i++)
        {
            vertexMethodArgs->Append(L"|");
            DrStr64 arg(GetArgument(i));
            vertexMethodArgs->Append(gcnew System::String(arg.GetString()));
        }

        //
        // Get assembly path, class name, and method name, and construct the vertex bridge args with the following format:
        //     "<vertexAssembly>,<vertexClassName>,<vertexMethodName>,<vertexMethodArgs>"
        //        
        System::Text::StringBuilder ^vertexBridgeArg = gcnew System::Text::StringBuilder();
        vertexBridgeArg->Append(gcnew System::String(GetArgument(1)));   // path to vertex DLL as passed to the vertex host, e.g. L"\\HpcTemp\\user\\jobID\\Microsoft.Research.DryadLinq0.dll";
        vertexBridgeArg->Append(",");
        vertexBridgeArg->Append(gcnew System::String(GetArgument(2)));   // full name of class that contains vertex entry method, e.g. L"Microsoft.Research.DryadLinq.DryadLinq__Vertex";
        vertexBridgeArg->Append(",");
        vertexBridgeArg->Append(gcnew System::String(GetArgument(3)));   // vertex entry method name L"Select__1";
        vertexBridgeArg->Append(",");		
        vertexBridgeArg->Append(vertexMethodArgs->ToString());

        DrLogI("ManagedWrapperVertex: Calling into Vertex Bridge to invoke Vertex Entry: %s", GetArgument(3));
        DrLogging::FlushLog();

        HRESULT hr = S_OK;

        //
        // Now that we have everything ready, we can invoke vertex bridge using reflection
        //                
        try
        {
            System::Reflection::Assembly ^vertexBridgeAsm;
            try
            {
                vertexBridgeAsm = System::Reflection::Assembly::Load(bridgeAssemblyName);
            }
            catch (System::Exception ^ie)
            {
                DrLogI("ManagedWrapperVertex: Failed to load assembly %s: %s", bridgeAssemblyName, ie->ToString());
                System::String^ asmLoc = System::IO::Path::Combine("..", bridgeAssemblyName + ".dll");
                vertexBridgeAsm = System::Reflection::Assembly::LoadFrom(asmLoc);
            }
            System::Type ^vertexBridgeType = vertexBridgeAsm->GetType(bridgeClassName);
            System::Reflection::MethodInfo ^vertexBridgeMethod
                = vertexBridgeType->GetMethod(bridgeMethodName, 
                                              static_cast<System::Reflection::BindingFlags>(System::Reflection::BindingFlags::NonPublic | 
                                                                                            System::Reflection::BindingFlags::Static));

            cli::array<System::Object^> ^invokeArgs = gcnew array<System::Object^>(2);
            invokeArgs[0] = logName;
            invokeArgs[1] = vertexBridgeArg->ToString();

            vertexBridgeMethod->Invoke(nullptr, invokeArgs);
        }
        catch(System::Exception ^ex)
        {
            System::Console::WriteLine(ex->ToString());
            hr = System::Runtime::InteropServices::Marshal::GetHRForException(ex);
            
            if (hr == S_OK) 
            {
                // if for some reason GetHRForException() mistakenly returned S_OK we want to make sure we don't skip the failure handling path below
                hr = E_FAIL; 
            }
        }

        //
        // Flush stdout to make sure all LINQ logs are written out
        //
        fflush(stdout);

        if (hr != S_OK)
        {
            //
            // Log errors. 
            //
            DrLogE("ManagedWrapperVertex: Assembly path = %s", GetArgument(1));
            DrLogE("ManagedWrapperVertex: Class name = %s", GetArgument(2));
            DrLogE("ManagedWrapperVertex: Method name = %s", GetArgument(3));

            error = (DrError)hr;
            errorMsg.Set("Error returned from managed runtime invocation, ");
            errorMsg.Append(DRERRORSTRING(error));
            errorMsg.Append("\n");
            DrLogE( "Error returned from managed runtime invocation. %s (%d)", DRERRORSTRING(error), error);

            //
            // Prepare the error message that will be sent over to the GM, and eventually displayed in the HPC console if this is the last vertex to fail the job
            //
            {
                WCHAR *pwszHpcTempShare = L"HPCTEMP";

                WCHAR wszUncPath[MAX_PATH+1];
                ZeroMemory(wszUncPath, sizeof(wszUncPath));

                if(ConvertCurrentDirToUNCPath(pwszHpcTempShare, wszUncPath, _countof(wszUncPath)) == TRUE)
                {
                    errorMsg.Append("For vertex logs, exception dump and rerun batch files see working directory for failed vertex:\n\n");
                    errorMsg.Append(wszUncPath);
                    errorMsg.Append("\n\n");
                }

                //
                // Open file containing the exception dump produced by vertex code.
                // The reason we read it out of the file instead of extracting straight from the excetpion we caught above is that 
                // the HPCLINQ runtime (vertexbridge or other code paths in Microsoft.Hpc.Linq.DLL) will report the inner exception in some cases. 
                // So we'll trust it to find the exception layer which is most relevant for a user looking at the HPC console for initial diagnosis.
                //
                FILE* errorFile = fopen("VertexException.txt", "r");
                if (errorFile)
                {
                    errorMsg.Append("The following callstack was reported as cause of failure:\n");
                    char line[1024];                    
                    while (fgets(line, _countof(line), errorFile) != NULL)
                    {
                        errorMsg.Append(line);
                    }
					
                    fclose(errorFile);
                    errorMsg.Append("\n");
                }
                else 
                {
                    //
                    // If no error exists, report that
                    //
                    errorMsg.Append("No error stack trace reported by managed code. See VertexHostLog.txt in vertex working directory for failure information.\n");
                }
            }

            //
            // Record error information
            // todo: make sure nativeInfo cleaned up
            //
            ReportError(error, "%s", errorMsg.GetString());
            return;
        }
    }

    //
    // Report success, clean up native Info 
    //
    DrLogI("ManagedWrapperVertex: Cleaning up NativeInfo at %p", nativeInfo);
    nativeInfo->CleanUp();
    delete nativeInfo;
    return;
}
