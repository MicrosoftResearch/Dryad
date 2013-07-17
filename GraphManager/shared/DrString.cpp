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

#include <DrShared.h>

static int ReverseIndexOfCharInBuf(char c, const char* buf, int length)
{
    while (length > 0)
    {
        --length;
        if (buf[length] == c)
        {
            return length;
        }
    }
    return DrStr_InvalidIndex;
};

static int IndexOfCharInBuf(char c, const char* buf, int length, int startPos)
{
    int i;
    for (i=startPos; i<length; ++i)
    {
        if (buf[i] == c)
        {
            return i;
        }
    }
    return DrStr_InvalidIndex;
};

#ifdef _MANAGED

DrStringBuffer::DrStringBuffer()
{
    m_buffer = nullptr;
}

DrStringBuffer::~DrStringBuffer()
{
    this->!DrStringBuffer();
}

DrStringBuffer::!DrStringBuffer()
{
    delete [] m_buffer;
}

void DrStringBuffer::SetString(System::String^ string)
{
    DrAssert(m_string == nullptr);
    DrAssert(m_buffer == nullptr);

    m_string = string;
}

System::String^ DrStringBuffer::GetString()
{
    return m_string;
}

const char* DrStringBuffer::GetChars()
{
    System::Threading::Monitor::Enter(this);

    if (m_string == nullptr)
    {
        System::Threading::Monitor::Exit(this);
        return NULL;
    }

    if (m_buffer == NULL)
    {
        /* cache a copy of the UTF8 char* representation */
        pin_ptr<const wchar_t> c = PtrToStringChars(m_string);

        int newSize = ::WideCharToMultiByte(CP_UTF8, 0, c, -1, NULL, 0, NULL, NULL);
        m_buffer = new char[newSize];

        int convertedChars = ::WideCharToMultiByte(CP_UTF8, 0, c, -1, m_buffer, newSize, NULL, NULL);
        if (convertedChars == 0)
        {
            delete[] m_buffer;
            m_buffer = NULL;
            DrLogA("Failed to convert %S to UTF8: %d", c, GetLastError());
        }
    }

    const char* buffer = m_buffer;
    System::Threading::Monitor::Exit(this);
    return buffer;
}

int DrStringBuffer::GetCharsLength()
{
    const char* chars = GetChars();
    if (chars == nullptr)
    {
        return 0;
    }
    else
    {
        return (int) strlen(chars);
    }
}

DrString::DrString()
{
    m_buffer = DrNew DrStringBuffer();
}

DrString::DrString(System::String^ s)
{
    Set(s);
}

DrString::DrString(DrString% s)
{
    m_buffer = s.m_buffer;
}

int DrString::GetCharsLength()
{
    return m_buffer->GetCharsLength();
}

System::String^ DrString::GetString()
{
    return m_buffer->GetString();
}

DrString% DrString::operator=(System::String^ s)
{
    Set(s);
    return *this;
}

DrString% DrString::operator=(DrString% s)
{
    Set(s);
    return *this;
}

void DrString::Set(System::String^ newString)
{
    m_buffer = DrNew DrStringBuffer();
    m_buffer->SetString(newString);
}

void DrString::Set(DrString otherString)
{
    m_buffer = otherString.m_buffer;
}

int DrString::Compare(DrString otherString)
{
    if (m_buffer == otherString.m_buffer)
    {
        return 0;
    }
    else
    {
        return Compare(otherString.GetString());
    }
}

int DrString::Compare(System::String^ otherString)
{
    System::String^ thisString = m_buffer->GetString();

    if (otherString == nullptr)
    {
        if (thisString == nullptr)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    else if (thisString == nullptr)
    {
        return -1;
    }

    return System::String::Compare(thisString, otherString);
}

int DrString::Compare(System::String^ otherString, int charsToCompare)
{
    System::String^ thisString = m_buffer->GetString();

    if (otherString == nullptr)
    {
        if (thisString == nullptr)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    else if (thisString == nullptr)
    {
        return -1;
    }

    return System::String::Compare(thisString, 0, otherString, 0, charsToCompare);
}


void DrString::SetF(const char *format, ...)
{
    va_list args;
    va_start(args, format);

    VSetF(format, args);
}

const char* DrString::GetChars()
{
    return m_buffer->GetChars();
}

void DrString::VSetF(const char* format, va_list args)
{
    size_t bufferLength = 2 * strlen(format) + 1;

    for ( ; ; )
    {
        char* buffer = new char[bufferLength];
        int ret = _vsnprintf_s(buffer, bufferLength, bufferLength-1, format, args);
        if (ret >= 0)
        {
            System::String^ s = gcnew System::String(buffer);
            delete [] buffer;
            m_buffer = DrNew DrStringBuffer();
            m_buffer->SetString(s);
            break;
        }

        delete [] buffer;
        bufferLength *= 2;
    }
}

int DrString::IndexOfChar(char c)
{
    return IndexOfChar(c, 0);
}

int DrString::IndexOfChar(char c, int startPos)
{
    const char* buf = GetChars();
    if (buf == nullptr)
    {
        return DrStr_InvalidIndex;
    }

    int length = (int) strlen(buf);
    return IndexOfCharInBuf(c, buf, length, startPos);
}


int DrString::ReverseIndexOfChar(char c)
{
    const char* buf = GetChars();
    if (buf == nullptr)
    {
        return DrStr_InvalidIndex;
    }

    int length = (int) strlen(buf);
    return ReverseIndexOfCharInBuf(c, buf, length);
}

#else

DrStringBuffer::DrStringBuffer(int length)
{
    m_buffer = new char[length+1];
    m_length = length;
}

DrStringBuffer::~DrStringBuffer()
{
    delete [] m_buffer;
}

char* DrStringBuffer::GetString()
{
    return m_buffer;
}

void DrStringBuffer::SetLength(int length)
{
    m_length = length;
}

int DrStringBuffer::GetLength()
{
    return m_length;
}


DrString::DrString()
{
}

DrString::DrString(const char* s)
{
    Set(s);
}

DrString::DrString(const DrString& s)
{
    m_string = s.m_string;
}

int DrString::GetCharsLength()
{
    if (m_string == NULL)
    {
        return 0;
    }
    else
    {
        return m_string->GetLength();
    }
}

const char* DrString::GetString()
{
    if (m_string == NULL)
    {
        return NULL;
    }
    else
    {
        return m_string->GetString();
    }
}

const char* DrString::GetChars()
{
    return GetString();
}

void DrString::Set(const char* newString)
{
    if (newString != NULL)
    {
        int newLength = (int) strlen(newString);
        DrStringBufferRef newBuffer = DrNew DrStringBuffer(newLength);
        errno_t err = strcpy_s(newBuffer->GetString(), newLength+1, newString);
        DrAssert(err == 0);
        m_string = newBuffer;
    }
}

void DrString::Set(DrString otherString)
{
    m_string = otherString.m_string;
}

DrString& DrString::operator=(const DrString& other)
{
    m_string = other.m_string;
    return *this;
}

int DrString::Compare(DrString otherString)
{
    if (m_string == otherString.m_string)
    {
        return 0;
    }
    else
    {
        return Compare(otherString.GetString());
    }
}

int DrString::Compare(const char* otherString)
{
    if (otherString == NULL)
    {
        if (m_string == NULL)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    else if (m_string == NULL)
    {
        return -1;
    }

    return strcmp(m_string->GetString(), otherString);
}

int DrString::Compare(const char* otherString, int charsToCompare, bool caseSensitive)
{
    if (otherString == NULL)
    {
        if (m_string == NULL)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    else if (m_string == NULL)
    {
        return -1;
    }

    if (caseSensitive)
    {
        return strncmp(m_string->GetString(), otherString, charsToCompare);
    }
    else 
    {
        return _strnicmp(m_string->GetString(), otherString, charsToCompare);
    }
}

void DrString::SetF(const char *format, ...)
{
    va_list args;
    va_start(args, format);

    VSetF(format, args);
}

void DrString::VSetF(const char* format, va_list args)
{
    int bufferLength = 2 * (int) strlen(format);

    for ( ; ; ) 
    {
        m_string = DrNew DrStringBuffer(bufferLength);
        int ret = _vsnprintf_s(m_string->GetString(), bufferLength+1, bufferLength,
                               format, args);
        if (ret >= 0)
        {
            m_string->SetLength(ret);
            break;
        }

        bufferLength *= 2;
    }
}

int DrString::IndexOfChar(char c, int startPos)
{
    return IndexOfCharInBuf(c, m_string->GetString(), m_string->GetLength(), startPos);
}

int DrString::ReverseIndexOfChar(char c)
{
    return ReverseIndexOfCharInBuf(c, m_string->GetString(), m_string->GetLength());
}

#endif

bool DrString::operator==(DrStringR other)
{
    return (Compare(other) == 0);
}

DrString DrString::AppendF(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    DrString newSubString;
    newSubString.VSetF(format, args);

    DrString newString;
    newString.SetF("%s%s", GetChars(), newSubString.GetChars());

    return newString;
}

#include <string.h>

void DrString::SetSubString(const char* otherString, int length)
{
    DrAssert((int)strlen(otherString) >= length);
    char* copy = _strdup(otherString);
    copy[length] = '\0';

    SetF("%s", copy);

    free(copy);
}

DrString DrString::ToUpperCase()
{
    char* copy = _strdup(GetChars());
    char* s = copy;
    while (*s != '\0')
    {
        if (__isascii(*s) && islower(*s))
        {
            *s = (char)toupper(*s);
        }
        ++s;
    }

    DrString upcased;
    upcased.SetF("%s", copy);
    return upcased;
}