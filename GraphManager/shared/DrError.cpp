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

DrError::DrError(HRESULT code, DrNativeString component, DrString explanation)
{
    m_code = code;
    m_component = component;
    m_explanation = explanation;
}

DrError::DrError(HRESULT code, DrString component, DrString explanation)
{
    m_code = code;
    m_component = component;
    m_explanation = explanation;
}

DrString DrError::ToShortText()
{
    DrString s;
    if (m_explanation.GetString() == DrNull)
    {
        s.SetF("%s:%x:%s", m_component.GetChars(), m_code, DRERRORSTRING(m_code));
    }
    else
    {
        s.SetF("%s:%x:%s. %s", m_component.GetChars(), m_code, DRERRORSTRING(m_code), m_explanation.GetChars());
    }
    return s;
}

void DrError::AddProvenance(DrErrorPtr previousError)
{
    if (m_errorProvenance == DrNull)
    {
        m_errorProvenance = DrNew DrErrorList();
    }
    m_errorProvenance->Add(previousError);
}

DrString DrError::ToShortText(DrErrorPtr errorOrNull)
{
    DrString s;
    if (errorOrNull == DrNull)
    {
        s = "No error";
    }
    else
    {
        s = errorOrNull->ToShortText();
    }
    return s;
}
