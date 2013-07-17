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

typedef UInt32 DrExitCode;

void DrInitExitCodeTable();
DrError DrAddExitCodeDescription(DrExitCode code, const char *pszDescription);
DrStr& DrAppendExitCodeDescription(DrStr& strOut, DrExitCode code);
DrStr& DrGetExitCodeDescription(DrStr& strOut, DrExitCode code);

class DrExitCodeString : public DrStr128
{
public:
    DrExitCodeString(DrExitCode code)
    {
        DrGetExitCodeDescription(*this, code);
    }
};

// This macro can be used to obtain a temporary "const char *" error description for an exit code. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
#define DREXITCODESTRING(code)    (DrExitCodeString(code).GetString())
#define DREXITCODEWSTRING(code)    (DRUTF8TOWSTRING(DREXITCODESTRING(code)))



#define DEFINE_DREXITCODE_NO_DESC(name, value) DEFINE_DREXITCODE(name, (DrExitCode)(value), #name)
#define DECLARE_DREXITCODE_NO_DESC(valname) DECLARE_DREXITCODE(valname, #valname)

#ifdef DEFINE_DREXITCODE
#undef DEFINE_DREXITCODE
#endif
#ifdef DECLARE_DREXITCODE
#undef DECLARE_DREXITCODE
#endif
#define DEFINE_DREXITCODE(name, value, description) const DrExitCode name = (DrExitCode)(value);
#define DECLARE_DREXITCODE(valname, description)

#include "DrExitCodes.h"

#undef DEFINE_DREXITCODE
#undef DECLARE_DREXITCODE



