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

#include <dryadstandaloneini.h>
#include <dvertexmain.h>
#include <dvertexpncontrol.h>
#include <dvertexcmdlinecontrol.h>
#include <dryadvertex.h>
#include <subgraphvertex.h>
#include <dvertexhttppncontrol.h>

#pragma managed

//
// Eliminates arguments used already. leaves remaining arguments
// todo: should only have one copy of this and export it around. Already defined in vertexHost.cpp
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
// Looks for indicators about what kind of controller to use to drive
// vertex loading
// Called from vertexHost.cpp:main with NULL factory
//
extern int DryadVertexMain(int argc, char** argv,
                                   DryadVertexFactoryBase* factory)
{
    bool useCmdLineController = true;
    bool useExplicitCmdLine = false;
    bool waitForInput = false;

    // 
    // Determine controller behavior and then remove this argument
    // --cmd uses command line
    // --cmdwait uses command line and waits for user input
    // --startfrompn doesn't use command line
    //
    if (argc > 1 &&
        ::strcmp(argv[1], "--cmd") == 0)
    {
        useExplicitCmdLine = true;
        EliminateArguments(&argc, argv, 1, 1);
    } 
    else if (argc > 1 &&
             ::strcmp(argv[1], "--cmdwait") == 0)
    {
        useExplicitCmdLine = true;
        waitForInput = true;
        EliminateArguments(&argc, argv, 1, 1);
    }
    else if (argc > 1 &&
             ::strcmp(argv[1], "--startfrompn") == 0)
    {
        useCmdLineController = false;
        EliminateArguments(&argc, argv, 1, 1);
    }

    UInt32 exitCode;

    //
    // If --cmdwait was used, wait for user to press a key before continuing
    //
    if (waitForInput)
    {
        fprintf(stdout, "Press any key\n"); 
        fflush(stdout);
        char buf[100];
        char* s = fgets(buf, 99, stdin);
        LogAssert(s != NULL);
    }

    //
    // Run controller
    //
    {
        if (useCmdLineController)
        {
            //
            // If --cmd or --cmdwait supplied, create a cmd line controller and run
            //
            DVertexCmdLineController controller;
            exitCode = controller.Run(argc, argv,
                                      factory, useExplicitCmdLine);
        }
        else
        {
            //
            // If --startfrompn used, create a Vertex Service controller and run
            //
            DVertexHttpPnControllerOuter controller;
            exitCode = controller.Run(argc, argv);
        }
    }

    //
    // After controller terminates, wait for user input again if --cmdwait
    //
    if (waitForInput)
    {
        fprintf(stdout, "Press any key\n"); fflush(stdout);
        char buf[100];
        char* s = fgets(buf, 99, stdin);
        LogAssert(s != NULL);
    }

    return exitCode;
}
