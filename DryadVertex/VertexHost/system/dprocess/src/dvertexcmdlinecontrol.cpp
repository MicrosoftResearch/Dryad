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

#include <dvertexcmdlinecontrol.h>
#include <dryadmetadata.h>
#include <dryadtagsdef.h>
#include <dryaderrordef.h>
#include <vertexfactory.h>

//
// todo: just have one of these - it's redefined a few places
//
static void EliminateArguments(int* pArgc, char* argv[],
                               int startingLocation, int numberToRemove)
{
    int argc = *pArgc;

    LogAssert(argc >= startingLocation+numberToRemove);

    int i;
    for (i=startingLocation+numberToRemove; i<argc; ++i)
    {
        argv[i - numberToRemove] = argv[i];
    }

    *pArgc = argc - numberToRemove;
}

//
// Record channel URIs and count
//
int DVertexCmdLineController::
    GetChannelDescriptions(int argc, char* argv[],
                           DVertexProcessStatus* initialState,
                           bool isInput)
{
    DrError cse;
    LogAssert(argc > 1);
    UInt32 nChannels;
    
    //
    // Parse number of channels and ensure it is valid
    //
    cse = DrStringToUInt32(argv[1], &nChannels);
    LogAssert(cse == DrError_OK);
    LogAssert((UInt32) argc > nChannels+1);

    //
    // Update the number of channels of the specified type
    //
    if (isInput)
    {
        initialState->SetInputChannelCount(nChannels);
    }
    else
    {
        initialState->SetOutputChannelCount(nChannels);
    }

    //
    // Set the channel URI to explicitly what is provided in next
    // nChannels arguments
    //
    UInt32 i;
    for (i = 0; i < nChannels; ++i)
    {
        DryadChannelDescription* channel;
        if (isInput)
        {
            channel = &(initialState->GetInputChannels()[i]);
        }
        else
        {
            channel = &(initialState->GetOutputChannels()[i]);
        }

        channel->SetChannelState(DrError_OK);
        channel->SetChannelURI(argv[i+2]);
    }

    return nChannels+1;
}

int DVertexCmdLineController::
    GetChannelOverride(int argc, char* argv[],
                       DVertexProcessStatus* initialState,
                       bool isInput)
{
    DrError cse;
    LogAssert(argc > 2);
    UInt32 whichChannel;
    cse = DrStringToUInt32(argv[1], &whichChannel);
    LogAssert(cse == DrError_OK);

    DryadChannelDescription* channel;
    if (isInput)
    {
        LogAssert(whichChannel < initialState->GetInputChannelCount());
        channel = &(initialState->GetInputChannels()[whichChannel]);
    }
    else
    {
        LogAssert(whichChannel < initialState->GetOutputChannelCount());
        channel = &(initialState->GetOutputChannels()[whichChannel]);
    }

    channel->SetChannelURI(argv[2]);

    return 2;
}

int DVertexCmdLineController::
    GetTextOverride(int argc, char* argv[],
                    DVertexCommandBlock* startCommand)
{
    DVertexProcessStatus* initialState = startCommand->GetProcessStatus();

    DrError err = DrError_OK;
    LogAssert(argc > 1);

    FILE* fText = fopen(argv[1], "r");
    if (fText == NULL)
    {
        err = DrGetLastError();
        DrLogE("Failed to open dump text file '%s' --- %s",
                argv[1], DRERRORSTRING(err));
        return 1;
    }

    static const UInt32 bufSize = 32 * 1024;
    char* buf = new char[bufSize];

    char* s = fgets(buf, bufSize, fText);
    if (s == NULL)
    {
        DrLogE("Couldn't read dump text input count");
        err = DrError_Fail;
    }
    else
    {
        UInt32 nInputs;
        int nRead = sscanf(buf, "%p", &nInputs);
        if (nRead == 1)
        {
            if (nInputs != initialState->GetInputChannelCount())
            {
                DrLogE("Dump input count %u differs from override count %u",
                        initialState->GetInputChannelCount(),
                        nInputs);
                err = DrError_Fail;
            }
            else
            {
                UInt32 i;
                for (i=0; i<nInputs && err == DrError_OK; ++i)
                {
                    s = fgets(buf, bufSize, fText);
                    if (s == NULL)
                    {
                        DrLogE("Couldn't read dump text input line %u",
                                i);
                        err = DrError_Fail;
                    }
                    else
                    {
                        size_t length = strlen(buf);
                        while (length > 0 &&
                               (buf[length-1] == '\n' ||
                                buf[length-1] == '\r'))
                        {
                            --length;
                            buf[length] = '\0';
                        }
                        DryadChannelDescription* channel;
                        channel = &(initialState->GetInputChannels()[i]);
                        channel->SetChannelURI(buf);
                    }
                }
            }
        }
        else
        {
            DrLogE("Malformed dump text output count %s", buf);
            err = DrError_Fail;
        }
    }

    if (err != DrError_OK)
    {
        delete [] buf;
        fclose(fText);
        return 1;
    }

    s = fgets(buf, bufSize, fText);
    if (s == NULL)
    {
        DrLogE("Couldn't read dump text output count");
        err = DrError_Fail;
    }
    else
    {
        UInt32 nOutputs;
        int nRead = sscanf(buf, "%p", &nOutputs);
        if (nRead == 1)
        {
            if (nOutputs != initialState->GetOutputChannelCount())
            {
                DrLogE("Dump output count %u differs from override count %u",
                        initialState->GetOutputChannelCount(),
                        nOutputs);
                err = DrError_Fail;
            }
            else
            {
                UInt32 i;
                for (i=0; i<nOutputs && err == DrError_OK; ++i)
                {
                    s = fgets(buf, bufSize, fText);
                    if (s == NULL)
                    {
                        DrLogE("Couldn't read dump text output line %u",
                                i);
                        err = DrError_Fail;
                    }
                    else
                    {
                        size_t length = strlen(buf);
                        while (length > 0 &&
                               (buf[length-1] == '\n' ||
                                buf[length-1] == '\r'))
                        {
                            --length;
                            buf[length] = '\0';
                        }
                        DryadChannelDescription* channel;
                        channel = &(initialState->GetOutputChannels()[i]);
                        channel->SetChannelURI(buf);
                    }
                }
            }
        }
        else
        {
            DrLogE("Malformed dump text output count %s", buf);
            err = DrError_Fail;
        }
    }

    s = fgets(buf, bufSize, fText);
    if (s == NULL)
    {
        DrLogE("Couldn't read dump text argument count");
        err = DrError_Fail;
    }
    else
    {
        UInt32 nArguments;
        int nRead = sscanf(buf, "%u", &nArguments);
        if (nRead == 1)
        {
            if (nArguments != startCommand->GetArgumentCount())
            {
                DrLogE("Dump argument count %u differs from override count %u",
                        startCommand->GetArgumentCount(),
                        nArguments);
                err = DrError_Fail;
            }
            else
            {
                UInt32 i;
                for (i=0; i<nArguments && err == DrError_OK; ++i)
                {
                    s = fgets(buf, bufSize, fText);
                    if (s == NULL)
                    {
                        DrLogE("Couldn't read dump text argument %u", i);
                        err = DrError_Fail;
                    }
                    else
                    {
                        size_t length = strlen(buf);
                        while (length > 0 &&
                               (buf[length-1] == '\n' ||
                                buf[length-1] == '\r'))
                        {
                            --length;
                            buf[length] = '\0';
                        }
                        startCommand->SetArgument(i, buf);
                    }
                }
            }
        }
        else
        {
            DrLogE("Malformed dump text argument count %s", buf);
            err = DrError_Fail;
        }
    }

    delete [] buf;
    fclose(fText);
    return 1;
}

int DVertexCmdLineController::
    RestoreDumpedStartCommand(int argc, char* argv[],
                              DVertexCommandBlock* startCommand)
{
    LogAssert(argc > 1);

    DrError err = DrError_OK;

    HANDLE h = CreateFileA(argv[1],
                           GENERIC_READ,
                           FILE_SHARE_READ,
                           NULL,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_NORMAL,
                           NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        err = DrGetLastError();
        DrLogE("Failed to open dump file '%s' --- %s",
                argv[1], DRERRORSTRING(err));
    }
    else
    {
        LARGE_INTEGER filelen;
        ::memset(&filelen, 0, sizeof(filelen));

        BOOL fSuccess = GetFileSizeEx(h, &filelen);
        if (!fSuccess)
        {
            err = DrGetLastError();
            DrLogE("Failed to get dump file size for '%s' --- %s",
                    argv[1], DRERRORSTRING(err));
        }
        else
        {
            LogAssert(filelen.HighPart == 0);
            DWORD nToRead = (DWORD)filelen.LowPart;
            BYTE* pBuff = new BYTE[nToRead+1];
            LogAssert(pBuff != NULL);
            BYTE *pRead = pBuff;
            while (err == DrError_OK && nToRead > 0)
            {
                DWORD nRead = 0;
                fSuccess = ReadFile(h, pRead, nToRead, &nRead, NULL);
                if (fSuccess)
                {
                    if (nRead == 0)
                    {
                        break;
                    }
                    pRead += nRead;
                    nToRead -= nRead;
                }
                else
                {
                    err = DrGetLastError();
                }
            }

            if (err == DrError_OK)
            {
                Size_t bufSize = (Size_t) filelen.LowPart;
                DrRef<DrFixedMemoryBuffer> buf;
                buf.Attach(new DrFixedMemoryBuffer(pBuff,
                                                         bufSize, bufSize));
                DrMemoryBufferReader reader(buf);
                err = reader.ReadAggregate(DryadTag_VertexCommand,
                                           startCommand, NULL);
                if (err != DrError_OK)
                {
                    DrLogE("Failed to deserialize dump file '%s' --- %s",
                            argv[1], DRERRORSTRING(err));
                }
            }
            else
            {
                DrLogE("Failed to read dump file '%s' --- %s",
                        argv[1], DRERRORSTRING(err));
            }

            delete [] pBuff;
        }
    }

    LogAssert(err == DrError_OK);

    return 1;
}

//
// Parses remaining command line arguments
//
void DVertexCmdLineController::
    ParseExplicitCmdLine(int argc, char* argv[],
                         DVertexCommandBlock* startCommand)
{
    DVertexProcessStatus* initialState = startCommand->GetProcessStatus();
    bool restoredDump = false;

    //
    // For each argument starting with '-'
    // todo: ensure dump is first if it's used at all
    //
    while (argc > 1 && argv[1][0] == '-')
    {
        int consumed = 0;

        if (::_stricmp(argv[1], "-dump") == 0)
        {
            --argc;
            ++argv;
            consumed = RestoreDumpedStartCommand(argc, argv, startCommand);
            restoredDump = true;
        }
        else if (::_stricmp(argv[1], "-overrideinput") == 0)
        {
            //
            // Only allowed when restored dump
            //
            LogAssert(restoredDump == true);
            --argc;
            ++argv;
            consumed = GetChannelOverride(argc, argv,
                                          initialState, true);
        }
        else if (::_stricmp(argv[1], "-overrideoutput") == 0)
        {
            //
            // Only allowed when restored dump
            //
            LogAssert(restoredDump == true);
            --argc;
            ++argv;
            consumed = GetChannelOverride(argc, argv,
                                          initialState, false);
        }
        else if (::_stricmp(argv[1], "-overridetext") == 0)
        {
            //
            // Only allowed when restored dump
            //
            LogAssert(restoredDump == true);
            --argc;
            ++argv;
            consumed = GetTextOverride(argc, argv, startCommand);
        }
        else if (::strcmp(argv[1], "-i") == 0)
        {
            //
            // Only allowed when not restored dump
            //
            LogAssert(restoredDump == false);
            --argc;
            ++argv;

            //
            // Get channel descriptions for input
            //
            consumed = GetChannelDescriptions(argc, argv,
                                              initialState, true);
        }
        else if (::strcmp(argv[1], "-o") == 0)
        {
            //
            // Only allowed when not restored dump
            //
            LogAssert(restoredDump == false);
            --argc;
            ++argv;

            //
            // Get channel descriptions for output
            //
            consumed = GetChannelDescriptions(argc, argv,
                                              initialState, false);
        }
        else
        {
            DrLogA("Unknown argument '%s'", argv[1]);
        }

        //
        // Update the argument count and pointer based on used parameters
        //
        LogAssert(argc > consumed);
        argc -= consumed;
        argv += consumed;
    }

    if (restoredDump)
    {
        //
        // If restoring from dump file, all arguments should have been handled
        //
        LogAssert(argc == 1);
    }
    else
    {
        //
        // Otherwise, remaining arguments are arguments to start command
        // copy remaining arguments into arg vector
        //
        startCommand->SetArgumentCount(argc-1);
        DrStr64* argVector = startCommand->GetArgumentVector();
        int i;
        for (i=1; i<argc; ++i)
        {
            argVector[i-1] = argv[i];
        }
    }
}

//
// Get a URI associated with specified channel
//
void DVertexCmdLineController::GetURI(DrStr* dst, const char* channel)
{
    
    if (::_stricmp(channel, "cosmos://") == 0)
    {
        //
        // If cosmos source, use channel name
        // todo: remove this case
        //
        *dst = channel;
    }
    else if (::_stricmp(channel, "file://") == 0)
    {
        //
        // If file source, use channel name
        //
        *dst = channel;
    }
    else
    {
        //
        // If file prefix not specified, prepend it
        //
        dst->SetF("file://%s", channel);
    }
}

//
// Parse command line arguments
//
DrError DVertexCmdLineController::
    ParseImplicitCmdLine(int argc, char* argv[],
                         DVertexCommandBlock* startCommand)
{
    DrStr128 prefix;
    bool expandInputs = false;
    bool expandOutputs = false;
    DVertexProcessStatus* initialState = startCommand->GetProcessStatus();

    //
    // todo: why 1024?
    //
    static UInt32 s_maxChannels = 1024;//2

    DrStr128* inputChannels = new DrStr128[s_maxChannels];
    DrStr128* outputChannels = new DrStr128[s_maxChannels];

    UInt32 numberOfInputChannels = 0;
    UInt32 numberOfOutputChannels = 0;

    //
    // foreach argument, check for well-known designators (-?, -i, -o, etc)
    // and interpret according to designator rules
    //
    int i = 1;
    while (i<argc)
    {
        if (::_stricmp(argv[i], "-?") == 0)
        {
            //
            // If asking for help with -?, look for executable name to report usage
            // Get executable name by looking for last instance of "\" and taking the
            // rest of the string or using the whole argument if not qualified
            //
            const char* leafName = ::strrchr(argv[0], '\\');
            if (leafName == NULL)
            {
                leafName = argv[0];
            }
            else
            {
                ++leafName;
            }

            DrLogE(
                    "\n"
                    "usage: %s [--debugbreak] [--popup] <vertexName> [<vertexOptions>]\\\n"
                    "           [-i input1 [-i input2] ...] [-o output1 [-o output2] ...]\n\n"
                    "Vertices that are registered in this executable:\n",
                    leafName);

            //
            // Also, show and errors and usage messages for verticies
            //
            VertexFactoryRegistry::ShowAllVertexUsageMessages(stderr);
            return DrError_Fail;
        }
        else if (::_stricmp(argv[i], "-i") == 0)
        {
            //
            // If input specified, make sure parameter is valid, and then store input URI
            //
            if (expandInputs)
            {
                //
                // If already specified --inputs, fail on duplicate method of specifying inputs
                //
                DrLogE("Cannot use -i together with --inputs");
                return DrError_Fail;
            }
            if (i+1 >= argc)
            {
                //
                // If -i is the last parameter, there is an error, because no URI could be specified
                //
                DrLogE("-i flag with no input channel");
                return DrError_Fail;
            }
            if (numberOfInputChannels == s_maxChannels)
            {
                //
                // If all input channels are already set, fail due to too many input channels
                //
                DrLogE("Too many input channels. Can't have more than %u inputs", s_maxChannels);

                return DrError_Fail;
            }

            //
            // Get the URI for the input channel
            //
            GetURI(&(inputChannels[numberOfInputChannels]), argv[i+1]);
            ++numberOfInputChannels;

            //
            // Remove the used arguments from the list
            //
            EliminateArguments(&argc, argv, i, 2);
        }
        else if (::_stricmp(argv[i], "-o") == 0)
        {
            //
            // If input specified, make sure parameter is valid, and then store output URI
            //
            if (expandOutputs)
            {
                //
                // If already specified --outputs, fail on duplicate method of specifying outputs
                //
                DrLogE("Cannot use -o together with --outputs");
                return DrError_Fail;
            }
            if (i+1 >= argc)
            {
                //
                // If -o is the last parameter, there is an error, because no URI could be specified
                //
                DrLogE("-o flag with no output channel");
                return DrError_Fail;
            }
            if (numberOfOutputChannels == s_maxChannels)
            {
                //
                // If all output channels are already set, fail due to too many output channels
                //
                DrLogE("Too many output channels. Can't have more than %u outputs", s_maxChannels);
                return DrError_Fail;
            }

            //
            // Get the URI for the output channel
            //
            GetURI(&(outputChannels[numberOfOutputChannels]), argv[i+1]);
            ++numberOfOutputChannels;

            //
            // Remove the used arguments from the list
            //
            EliminateArguments(&argc, argv, i, 2);
        }
        else if (::_stricmp(argv[i], "--prefix") == 0)
        {
            //
            // If I/O prefix specified, make sure parameters are valid and store the prefix URI
            //
            if (i+1 >= argc)
            {
                //
                // If --prefix is the last parameter, there is an error, because no prefix string could be specified
                // todo: if prefix specified multiple times, that should be logged at least, if not error
                //
                DrLogE("--prefix flag without prefix string");
                return DrError_Fail;
            }

            //
            // Get the URI for the prefix
            //
            GetURI(&prefix, argv[i+1]);

            //
            // Remove the used arguments from the list
            //
            EliminateArguments(&argc, argv, i, 2);
        }
        else if (::_stricmp(argv[i], "--inputs") == 0)
        {
            //
            // If --inputs specified, make sure parameters are valid and store number of channels
            //
            if (i+1 >= argc)
            {
                //
                // If --inputs is the last parameter, there is an error, because no count can be specified
                //
                DrLogE("--inputs flag without a number");
                return DrError_Fail;
            }

            if (numberOfInputChannels > 0)
            {
                //
                // If input channels are already set, fail due to duplicate method for specifying inputs
                // todo: Can also fail in this code path if --inputs set twice. Need to update error message.
                //
                DrLogE("Cannot use -i together with --inputs");
                return DrError_Fail;
            }
            
            //
            // Try to parse the next parameter as a count
            // If this fails, numberOfInputChannels = 0
            //
            numberOfInputChannels = atoi(argv[i + 1]);
            if (numberOfInputChannels >= s_maxChannels)
            {
                //
                // If more than max channels, fail
                //
                DrLogE("Too many input channels. Can't have more than %u inputs", s_maxChannels);

                return DrError_Fail;
            }
            
            //
            // Remember to expand the input list
            //
            expandInputs = true;

            //
            // Remove the used arguments from the list
            //
            EliminateArguments(&argc, argv, i, 2);
        }
        else if (::_stricmp(argv[i], "--outputs") == 0)
        {
            //
            // If --outputs specified, make sure parameters are valid and store number of channels
            //
            if (i+1 >= argc)
            {
                //
                // If --outputs is the last parameter, there is an error, because no count can be specified
                //
                DrLogE("--outputs flag without a number");
                return DrError_Fail;
            }

            if (numberOfOutputChannels > 0)
            {
                //
                // If output channels are already set, fail due to duplicate method for specifying outputs
                // todo: Can also fail in this code path if --outputs set twice. Need to update error message.
                //
                DrLogE("Cannot use -o together with --outputs");
                return DrError_Fail;
            }

            //
            // Try to parse the next parameter as a count
            // If this fails, numberOfInputChannels = 0
            //
            numberOfOutputChannels = atoi(argv[i + 1]);
            if (numberOfOutputChannels >= s_maxChannels)
            {
                DrLogE("Too many output channels. Can't have more than %u outputs", s_maxChannels);
                return DrError_Fail;
            }
            
            //
            // Remember to expand the output list
            //
            expandOutputs = true;

            //
            // Remove the used arguments from the list
            //
            EliminateArguments(&argc, argv, i, 2);
        }
        else
        {
            //
            // If parameter isn't well-known or removed while processing well-known
            // arguments, just increment index and continue
            //
            ++i;
        }
    }

    UInt32 c;
    DrStr128 channelURI;
    
    //
    // If prefix not specified, and --inputs or --outputs used, fail 
    //
    if (prefix.GetLength() == 0)
    {
        if (expandInputs)
        {
            DrLogE("--inputs flag without --prefix flag");
            return DrError_Fail;
        }
        if (expandOutputs)
        {
            DrLogE("--outputs flag without --prefix flag");
            return DrError_Fail;
        }
    }

    //
    // Foreach input channel, set the channel URI
    //
    initialState->SetInputChannelCount(numberOfInputChannels);
    for (c=0; c<numberOfInputChannels; ++c)
    {
        DryadChannelDescription* channel =
            &(initialState->GetInputChannels()[c]);
        channel->SetChannelState(DrError_OK);
        if (!expandInputs)
        {
            //
            // If using explicit names, just copy the temp name built in argument processing
            //
            channel->SetChannelURI(inputChannels[c]);
        }
        else
        {
            //
            // If using implicit names, format the URI as [prefix]i[input index] 
            //
            channelURI.SetF("%si%u", prefix.GetString(), c);
            channel->SetChannelURI(channelURI);
        }
    }
        
    //
    // Foreach output channel, set the channel URI
    //
    initialState->SetOutputChannelCount(numberOfOutputChannels);
    for (c=0; c<numberOfOutputChannels; ++c)
    {
        DryadChannelDescription* channel =
            &(initialState->GetOutputChannels()[c]);
        channel->SetChannelState(DrError_OK);
        if (!expandOutputs)
        {
            //
            // If using explicit names, just copy the temp name built in argument processing
            //
            channel->SetChannelURI(outputChannels[c]);
        }
        else
        {
            //
            // If using implicit names, format the URI as [prefix]o[output index] 
            //
            channelURI.SetF("%so%u", prefix.GetString(), c);
            channel->SetChannelURI(channelURI);
        }
    }

    //
    // Use all arguments not used here as arguments to start command
    //
    startCommand->SetArgumentCount(argc-1);
    DrStr64* argVector = startCommand->GetArgumentVector();
    for (i=1; i<argc; ++i)
    {
        argVector[i-1] = argv[i];
    }

    return DrError_OK;
}

//
// Entry point for starting command line controller
//
UInt32 DVertexCmdLineController::Run(int argc, char* argv[],
                                     DryadVertexFactoryBase* factory,
                                     bool useExplicitCmdLine)
{
    UInt32 exitCode = 0;;

    //
    // Create new command block reference
    //
    DrRef<DVertexCommandBlock> startCommand;
    startCommand.Attach(new DVertexCommandBlock());

    {
        // todo: make sure the vertex gets deleted before the command block 

        //
        // Create vertex reference
        //
        DrRef<DryadSimpleChannelVertex> vertex;
        vertex.Attach(new DryadSimpleChannelVertex());

        
        if (useExplicitCmdLine)
        {
            //
            // Use explicit command line if --cmd or --cmdwait option used
            // allows for dump restore or explicit -i/-o usage
            //
            ParseExplicitCmdLine(argc, argv, startCommand);
        }
        else
        {
            //
            // Use implicit command line if no cmd option used
            // allows for explicit -i/-o usage (different from above) or implicit --inputs/--outputs usage
            //
            DrError err =
                ParseImplicitCmdLine(argc, argv, startCommand);
            if (err != DrError_OK)
            {
                exitCode = 1;
            }
        }

        //
        // If everything successful, initialize and run the vertex
        //
        if (exitCode == 0)
        {
            vertex->Initialize(this);
            vertex->PrepareDryadVertex(startCommand->GetProcessStatus(),
                                       factory,
                                       startCommand->GetArgumentCount(),
                                       startCommand->GetArgumentVector(),
                                       startCommand->GetRawSerializedBlockLength(),
                                       startCommand->GetRawSerializedBlock());

            //
            // Run the vertex - blocking
            //
            DrError vertexState =
                vertex->RunDryadVertex(startCommand->GetProcessStatus(),
                                       startCommand->GetArgumentCount(),
                                       startCommand->GetArgumentVector());

            //
            // Log completion status
            //
            if (vertexState == DryadError_VertexCompleted)
            {
                DrLogI("Vertex exited without error");
            }
            else
            {
                DrLogE("Vertex exited with error: %s",
                        DRERRORSTRING(vertexState));
                exitCode = 1;
            }
        }
    }

    return exitCode;
}

static void ShowChannelError(const char* channelType,
                             DrError err,
                             const char* channelURI, DryadMetaData* errorData)
{
    char* cosmosErrorString = DrGetErrorText(err);
    if (errorData == NULL)
    {
        DrLogE("%s channel %s aborted %s, no error data",
                channelType, channelURI, cosmosErrorString);
    }
    else
    {
        char* errorText = errorData->GetText();
        DrLogE("%s channel %s aborted %s: %s",
                channelType, channelURI, cosmosErrorString, errorText);
        delete [] errorText;
    }
    free(cosmosErrorString);
}       

void DVertexCmdLineController::
    AssimilateNewStatus(DVertexProcessStatus* status,
                        bool /*sendUpdate*/, bool /*notifyWaiters*/)
{
    UInt32 i;

    for (i=0; i<status->GetInputChannelCount(); ++i)
    {
        DryadChannelDescription* c = &(status->GetInputChannels()[i]);
        if (c->GetChannelState() != DrError_OK &&
            c->GetChannelState() != DryadError_ProcessingInterrupted &&
            c->GetChannelState() != DrError_EndOfStream)
        {
            LogAssert(c->GetChannelState() != DryadError_ChannelRestart);
            ShowChannelError("Input",
                             c->GetChannelState(),
                             c->GetChannelURI(),
                             c->GetChannelMetaData());
        }
    }

    for (i=0; i<status->GetOutputChannelCount(); ++i)
    {
        DryadChannelDescription* c = &(status->GetOutputChannels()[i]);
        if (c->GetChannelState() != DrError_OK &&
            c->GetChannelState() != DryadError_ProcessingInterrupted &&
            c->GetChannelState() != DrError_EndOfStream)
        {
            LogAssert(c->GetChannelState() != DryadError_ChannelRestart);
            ShowChannelError("Output",
                             c->GetChannelState(),
                             c->GetChannelURI(),
                             c->GetChannelMetaData());
        }
    }

    DryadMetaData* metaData = status->GetVertexMetaData();
    if (metaData != NULL)
    {
        const char* metaDataText = metaData->GetText();
        DrLogI("Vertex output status: %s", metaDataText);
        delete [] metaDataText;
    }
}
