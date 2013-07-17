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

/* TODO understand public classes */
#pragma warning( disable: 4677 )

#define _HAS_ITERATOR_DEBUGGING 0

//#define _CRTDBG_MAP_ALLOC
//#include <stdlib.h>
//#include <crtdbg.h>

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#include <windows.h>

#include "DrTypes.h"

#include "DrAssert.h"

#include "DrRef.h"

#include "DrCritSec.h"

#include "DrString.h"
#include "DrFileWriter.h"
#include "DrLogging.h"

#include "DrSort.h"
#include "DrArray.h"
#include "DrArrayList.h"
#include "DrDictionary.h"
#include "DrSet.h"
#include "DrMultiMap.h"

#include "DrError.h"

#include "DrStringUtil.h"