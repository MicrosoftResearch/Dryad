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

#include <dryadmetadata.h>

class DVErrorReporter
{
public:
    DVErrorReporter();

    bool NoError();
    DrError GetErrorCode();
    DryadMetaData* GetErrorMetaData();

    void ReportError(DrError errorStatus);
    void ReportError(const char* errorFormat, ...);
    void ReportError(DrError errorStatus,
                     const char* errorFormat, ...);
    void ReportError(DrError errorStatus, DryadMetaData* metaData);

    void InterruptProcessing();

private:
    void ReportFormattedErrorInternal(DrError errorStatus,
                                      const char *formatString,
                                      va_list args);

    DrError            m_errorCode;
    DryadMetaDataRef   m_metaData;
};
