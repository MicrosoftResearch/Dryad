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

#include <DrCommon.h>

#pragma managed

#include <vcclr.h>

class DrString
{
public:
    DrString(System::String^ s)
    {
        /* cache a copy of the UTF8 char* representation */
        pin_ptr<const wchar_t> c = PtrToStringChars(s);

        int newSize = ::WideCharToMultiByte(CP_UTF8, 0, c, -1, NULL, 0, NULL, NULL);
        char* chars = new char[newSize];

        int convertedChars = ::WideCharToMultiByte(CP_UTF8, 0, c, -1, chars, newSize, NULL, NULL);
        if (convertedChars == 0)
        {
            delete[] chars;
            DrLogA("Failed to convert %S to UTF8: %d", c, GetLastError());
        }

        m_chars = chars;
    }

    ~DrString()
    {
        delete [] m_chars;
    }

    const char* GetChars()
    {
        return m_chars;
    }

private:
    const char* m_chars;
};
