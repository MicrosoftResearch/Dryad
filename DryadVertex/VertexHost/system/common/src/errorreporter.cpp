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

#include <errorreporter.h>
#include <dryaderrordef.h>

#pragma unmanaged

DVErrorReporter::DVErrorReporter()
{
    m_errorCode = DrError_OK;
}

bool DVErrorReporter::NoError()
{
    return m_errorCode == DrError_OK;
}

DrError DVErrorReporter::GetErrorCode()
{
    return m_errorCode;
}

//
// Return any error metadata accumulated
//
DryadMetaData* DVErrorReporter::GetErrorMetaData()
{
    return m_metaData;
}

void DVErrorReporter::InterruptProcessing()
{
    ReportError(DryadError_ProcessingInterrupted);
}

void DVErrorReporter::ReportError(DrError errorStatus)
{
    m_errorCode = errorStatus;
}

void DVErrorReporter::ReportError(const char* errorFormat, ...)
{
    va_list ptr; va_start(ptr, errorFormat);
    ReportFormattedErrorInternal(DryadError_VertexError, errorFormat, ptr);
}

void DVErrorReporter::ReportError(DrError errorStatus,
                                  const char* errorFormat, ...)
{
    va_list ptr; va_start(ptr, errorFormat);
    ReportFormattedErrorInternal(errorStatus, errorFormat, ptr);
}

void DVErrorReporter::ReportError(DrError errorStatus,
                                  DryadMetaData* metaData)
{
    m_metaData = metaData;
    m_errorCode = errorStatus;
}

void DVErrorReporter::ReportFormattedErrorInternal(DrError errorStatus,
                                                   const char* errorFormat,
                                                   va_list args)
{
    DryadMetaData::Create(&m_metaData);
    DrStr128 errorString;
    errorString.VSetF(errorFormat, args);
    m_metaData->AddErrorWithDescription(errorStatus, errorString);
    m_errorCode = errorStatus;
}
