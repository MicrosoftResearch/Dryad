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

//
// Includes
//
#include "dvertexmain.h"
#include "managedwrapper.h"
#include "recorditem.h"
#include "DrString.h"

#pragma managed

//
// Managed Wrapper vertex factory. 
//
extern FactoryMWrapper s_factoryHWrapper;

//
// Eliminates arguments used by previous operations. 
// Arguments left are those not yet used.
//
static void EliminateArguments(int* pArgc, char* argv[],
                               int startingLocation, int numberToRemove)
{
    //
    // Get total number of arguments. Check if there number to remove makes sense.
    //
    int argc = *pArgc;
    LogAssert(argc >= startingLocation+numberToRemove);

    int i;
    for (i=startingLocation+numberToRemove; i<argc; ++i)
    {
        //
        // Starting from first unused argument, move it to the beginning of the array
        //
        argv[i - numberToRemove] = argv[i];
    }

    //
    // Update the argument count so that it reflects the number of valid arguments remaining
    //
    *pArgc = argc - numberToRemove;
}

//
// Gets command line arguments
//
void DrGetUtf8CommandArgs(int argc, wchar_t *wargv[], char ***pargv)
{
    DrStr128 strArg;
    char **newArgv = NULL;

    //
    // increment count to account for tailing NULL
    //
    ++argc;

    //
    // If there are any command line args
    //
    if (argc > 0) 
    {
        //
        // Create argument storage and verify that it's correctly created
        //
        newArgv = new char *[(size_t)argc];
        LogAssert(newArgv != NULL);

        //
        // Foreach argument
        //
        for (int i = 0; i < argc; i++) 
        {
            if (wargv[i] == NULL) 
            {
                //
                // If NULL argument, store NULL
                //
                newArgv[i] = NULL;
            } 
            else 
            {
                // 
                // Get argument value and verify that it's a valid string
                //
                strArg.Set( wargv[i] );
                LogAssert(strArg.GetString() != NULL);

                //
                // Copy argument value into list 
                //
                newArgv[i] = new char[strArg.GetLength() + 1];
                LogAssert(newArgv[i] != NULL);
                memcpy(newArgv[i], strArg.GetString(), strArg.GetLength() + 1);
            }
        }
    }

    //
    // Store argument list (can be null if no arguments)
    //
    *pargv = newArgv;
}

void GetLoggingFileName(WCHAR* fileName)
{
    WCHAR* logDir = NULL;

    WCHAR currentDir[MAX_PATH + 1];

    if (GetCurrentDirectory(MAX_PATH, currentDir) != 0)
    {
        WCHAR logDirectory[MAX_PATH+1];
        HRESULT hr = DrGetEnvironmentVariable(L"LOG_DIRS", logDirectory);
        if(SUCCEEDED(hr))
        {
            // deal with comma-separated list of directories
            WCHAR* firstComma = wcschr(logDirectory, ',');
            if (firstComma != NULL)
            {
                *firstComma = '\0';
                WCHAR* firstSpace = wcschr(logDirectory, ' ');
                if (firstSpace != NULL)
                {
                    *firstSpace = '\0';
                }
            }
            logDir = logDirectory;
        }
        else
        {
            logDir = currentDir;
        }

        WCHAR* dirLoc = wcsrchr(currentDir, L'\\');
        if (dirLoc != NULL)
        {
            ++dirLoc;
            if (S_OK == StringCchPrintf(fileName, MAX_PATH, L"%s\\process-%s-vertexhost.log", logDir, dirLoc))
            {
                return;
            }
        }
    }

    wcscpy_s(fileName, MAX_PATH, L"vertexhost.log");
}

//
// Sets the logging level based on the environment variable
//
void SetLoggingLevel()
{
    WCHAR logFileName[MAX_PATH + 1];
    GetLoggingFileName(logFileName);
    DrLogging::Initialize(logFileName);

    WCHAR traceLevel [MAX_PATH];
    HRESULT hr = DrGetEnvironmentVariable(L"DRYAD_TRACE_LEVEL", traceLevel);
    if(hr == DrError_OK)
    {
        if(wcscmp(traceLevel, L"OFF") == 0)
        {
            DrLogging::SetLoggingLevel(LogLevel_Off);
        }
        else if(wcscmp(traceLevel, L"CRITICAL") == 0)
        {
            DrLogging::SetLoggingLevel(LogLevel_Assert);
        }
        else if(wcscmp(traceLevel, L"ERROR") == 0)
        {
            DrLogging::SetLoggingLevel(LogLevel_Error);
        }
        else if(wcscmp(traceLevel, L"WARN") == 0)
        {
            DrLogging::SetLoggingLevel(LogLevel_Warning);
        }
        else if(wcscmp(traceLevel, L"INFO") == 0)
        {
            DrLogging::SetLoggingLevel(LogLevel_Info);
        }
        else
        {
            DrLogging::SetLoggingLevel(LogLevel_Debug);
        }
    }
    else
    {
        DrLogging::SetLoggingLevel(LogLevel_Debug);
    }
}

//
// if $HPCQUERY_DEBUGVERTEXHOST is defined, break into the debugger
//
void BreakForDebugger()
{
    WCHAR strDebugBreak [MAX_PATH];
    HRESULT hr = DrGetEnvironmentVariable(L"HPCQUERY_DEBUGVERTEXHOST", strDebugBreak);
    if(hr == DrError_OK)
    {
            DrLogE("Waiting for debugger ");
            DrLogging::FlushLog(); 
            
            while (!IsDebuggerPresent()) 
            {
                Sleep(2000);
            }

            DebugBreak();
    }
}

[System::Security::SecurityCriticalAttribute]
[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptionsAttribute]
static void ExceptionHandler(System::Object^ sender, System::UnhandledExceptionEventArgs^ args)
{
    DrLogI("In exception handler");
    HRESULT result = E_FAIL;
    System::Exception^ e = dynamic_cast<System::Exception^>(args->ExceptionObject);
    System::String^ errorString = "Unknown exception";
    if (e != nullptr)
    {
        result = System::Runtime::InteropServices::Marshal::GetHRForException(e);
        errorString = e->ToString();
        DrLogA("Unhandled exception: %s", DrString(errorString).GetChars());
    }
}

//
// Start up vertex host
//
[System::Security::SecurityCriticalAttribute]
[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptionsAttribute]
#if defined(_AMD64_)
int wmain(int argc, wchar_t* wargv[])
#else
int __cdecl wmain(int argc, wchar_t* wargv[])
#endif
{
    try
    {
        //
        // Enable logging based on environment variable
        //
        SetLoggingLevel();

        DrInitErrorTable();
        DrInitExitCodeTable();
        DrInitLastAccessTable();

        // Set unhandled exception handler to catch anything thrown from 
        // managed code
        System::AppDomain^ currentDomain = System::AppDomain::CurrentDomain;
        currentDomain->UnhandledException += gcnew System::UnhandledExceptionEventHandler(ExceptionHandler);

        //
        // trace for startup
        //
        DrLogI("Vertex Host starting");

        //
        // Get environment variable to know whether to break into debugger
        //
        BreakForDebugger();

        //
        // We call Register on the Managed Wrapper vertex factory to force its library to be linked.
        // Registration actually occurs during static initialization.
        //
        s_factoryHWrapper.Register();

        //
        // Get command line arguments
        //
        char** argv;
        DrGetUtf8CommandArgs(argc, wargv, &argv);

        //
        // Initialize the dryad communication layer with the command line arguments
        //
        int nOpts;
        DrError e;
        e = DryadInitialize(argc, argv, &nOpts);
        if (e != DrError_OK)
        {
            //
            // Report error in initializing cluster layer
            //
            DrLogE("Couldn't initialise Cluster");
            return 1;
        }

        //
        // Update the argument list to just those parameters that weren't used by cluster init
        //
        EliminateArguments(&argc, argv, 1, nOpts);

        //
        // Call main function to continue execution of vertex
        //
        int exitCode = DryadVertexMain(argc, argv, NULL);

        //
        // Close the cluster connection after dryadvertexmain returns
        //
        e = DryadShutdown();
        if (e == DrError_OK)
        {
            //
            // Report success
            //
            DrLogI("Completed uninitialise cluster");
        }
        else
        {
            //
            // Report failure
            //
            DrLogE("Couldn't uninitialise cluster");
        }

        return exitCode;
    }
    catch (System::Exception^ e)
    {
        DrLogA("Unhandled exception: %s", DrString(e->ToString()).GetChars());
        return 1;
    }
}

//
// Simple data class which contains the byte array and its length.
//
class DummyRecord  {
    /// used to copy arbitrary-sized items
    size_t m_dummySize;
    BYTE* m_dummyStuff;

public:
    //
    // Create an empty record
    //
    DummyRecord()
    {
        m_dummySize = 0;
        m_dummyStuff = NULL;
    }

    //
    // Clean up record
    //
    ~DummyRecord()
    {
        if (m_dummyStuff)
            delete [] m_dummyStuff;
    }

    //
    // Copy constructor for record
    //
    DummyRecord(const DummyRecord& other) 
    {
        m_dummySize = other.m_dummySize;
        if (m_dummySize) 
        {
            m_dummyStuff = new BYTE[m_dummySize];
            memcpy(m_dummyStuff, other.m_dummyStuff, m_dummySize);
        }
    }
    
    //
    // Assignment operator overload to copy existing record
    //
    DummyRecord& operator=(const DummyRecord& other) 
    {
        //
        // Clean up existing record
        //
        if (m_dummyStuff) 
        {
            // todo: why not 'delete []' like in destructor
            delete m_dummyStuff;
        }

        //
        // Copy other record contents into this record
        //
        m_dummySize = other.m_dummySize;
        if (m_dummySize) 
        {
            m_dummyStuff = new BYTE[m_dummySize];
            memcpy(m_dummyStuff, other.m_dummyStuff, m_dummySize);
        }
        else
        {
            //
            // If nothing in other record, set local record contents to NULL
            // todo: use NULL rather than 0
            //
            m_dummyStuff = 0;
        }

        return *this;
    }

    //
    // Define deserialization of record
    //
    DrError DeSerialize(DrMemoryBufferReader* reader,
                        Size_t availableSize, bool lastRecordInStream)
    {
        //
        // Read n bytes into record from memory buffer
        //
        m_dummySize = availableSize;
        m_dummyStuff = new BYTE[m_dummySize];
        return reader->ReadBytes(m_dummyStuff, m_dummySize);
    }

    //
    // Define serialization of record
    //
    DrError Serialize(DrMemoryBufferWriter* writer)
    {
        //
        // Write record into memory buffer
        //
        return writer->WriteBytes(m_dummyStuff, m_dummySize);
    }

    //
    // Move contents of another record into this record
    //
    void TransferFrom(DummyRecord& src)
    {
        //
        // Get size and data
        //
        m_dummySize = src.m_dummySize;
        m_dummyStuff = src.m_dummyStuff;

        //
        // Clear other record's size and data 
        //
        src.m_dummySize = 0;
        src.m_dummyStuff = NULL;
    }

    //
    // Return the data size of this record
    //
    size_t GetSize() const 
    { 
        return m_dummySize; 
    }

    //
    // Return a pointer to the data in this record
    //
    BYTE* GetData() const 
    { 
        return m_dummyStuff; 
    }
};

//
// Define a type for multiple records and create an instance of that type
//
typedef RecordBundle<DummyRecord> DummyBundle;
DummyBundle s_packedBundle;

//
// Copy Vertex ('CP')
// Copies input to output. Used for broadcast.
//
class CopyVertex : public DryadVertexProgram
{
public:
    //
    // Constructor - Ensures that parser factory is the factory associated with the global record bundle
    //
    CopyVertex()
    {
        SetCommonParserFactory(s_packedBundle.GetParserFactory());
    }

    //
    // Run vertex which involves copying input channel to output channel
    //
    void Main(WorkQueue* workQueue,
              UInt32 numberOfInputChannels,
              RChannelReader** inputChannel,
              UInt32 numberOfOutputChannels,
              RChannelWriter** outputChannel)
    {
        //
        // Ensure exactly one input and one output
        //
        LogAssert(numberOfInputChannels == 1 && numberOfOutputChannels == 1);

        //
        // Associates reader with input channel and writer with output channel
        //
        DummyBundle::Reader input(inputChannel[0]);
        DummyBundle::Writer output(&s_packedBundle, outputChannel[0]);
        
        //
        // Log start of main. DrLogging will add reference to CopyVertex.main
        //
        DrLogI("Started");

        //
        // Reads from input channel until nothing left
        //
        while (input.Advance())
        {
            //todo: Decide whether to remove this or log it
            //printf("Transferring\n");

            //
            // Prepare output record array writer for additional input
            //
            output.MakeValid();

            //
            // Transfer contents of input into output
            //
            output->TransferFrom(*input);
        }
    }
};

//
// Factory for copy verticies
//
StdTypedVertexFactory<CopyVertex> s_factoryCopy("CP");
