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

/* when there's time, I will write a high-performance file writer so we can get decent logging performance,
   but for now we are using regular WriteFile */

#include <stdio.h>

DRCLASS(DrFileWriter) : public DrCritSec
{
public:
    DrFileWriter();
    ~DrFileWriter();
#ifdef _MANAGED
    !DrFileWriter();
#endif
    
    bool Open(DrString fileName);
    bool ReOpen(DrString fileName);
    void Flush();
    void Close();

    void Append(const char* data, int dataLength);

private:
    void FlushInternal();

    HANDLE         m_fileHandle;
    int            m_dataBufferSize;
    char*          m_bufferedData;
    int            m_bufferedDataLength;

};
DRREF(DrFileWriter);

DRCLASS(DrStaticFileWriters)
{
public:
    static void Initialize();
    static void AddWriter(DrFileWriterPtr writer);
    static void FlushWriters();
    static void Discard();
};