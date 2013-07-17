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

#include <stdio.h>

static const int DrStr_InvalidIndex = -1;

DRDECLARECLASS(DrString);
DRRREF(DrString);

#ifdef _MANAGED

#include <msclr\marshal.h>

#define DrNullString ((System::String^) nullptr)
#define DrNativeString System::String^

ref class DrStringBuffer
{
public:
    DrStringBuffer();
    ~DrStringBuffer();
    !DrStringBuffer();

    void SetString(System::String^ string);
    System::String^ GetString();

    const char* GetChars();
    int GetCharsLength();

private:
    System::String^     m_string;
    char*               m_buffer;

};

public ref class DrString
{
public:
    DrString();
    DrString(System::String^ newString);
    DrString(DrString% otherString);

    DrString% operator=(System::String^ other);
    DrString% operator=(DrString% other);

    bool operator==(DrString% other);

    int GetCharsLength();

    System::String^ GetString();

    const char* GetChars();

    void Set(System::String^ newString);
    void Set(DrString otherString);

    void SetSubString(const char* otherString, int length);

    int Compare(DrString otherString);
    int Compare(System::String^ otherString);
    int Compare(System::String^ otherString, int charsToCompare);

    void SetF(const char *format, ...);
    void VSetF(const char* format, va_list args);

    DrString AppendF(const char *format, ...);
    DrString ToUpperCase();

    int IndexOfChar(char c);
    int IndexOfChar(char c, int startPos);
    int ReverseIndexOfChar(char c);

private:
    DrStringBuffer^     m_buffer;
};
DRRREF(DrString);

#else

#define DrNullString ((const char*) NULL)
#define DrNativeString const char*

DRBASECLASS(DrStringBuffer)
{
public:
    DrStringBuffer(int length);
    ~DrStringBuffer();

    char* GetString();

    int GetLength();
    void SetLength(int length);

private:
    char*       m_buffer;
    int         m_length;
};
DRREF(DrStringBuffer);

DRVALUECLASS(DrString)
{
public:
    DrString();
    DrString(const char* newString);
    DrString(const DrString& otherString);

    DrString& operator=(const DrString& other);

    bool operator==(DrString& other);

    int GetCharsLength();

    const char* GetString();

    const char* GetChars();

    void Set(const char* newString);
    void Set(DrString otherString);

    void SetSubString(const char* otherString, int length);

    int Compare(DrString otherString);
    int Compare(const char* otherString);
    int Compare(const char* otherString, int charsToCompare, bool caseSensitive=true);

    void SetF(const char *format, ...);
    void VSetF(const char* format, va_list args);

    DrString AppendF(const char *format, ...);
    DrString ToUpperCase();

    int IndexOfChar(char c, int startPos=0);
    int ReverseIndexOfChar(char c);

private:
    DrStringBufferRef    m_string;
};

#endif

DRRREF(DrString);