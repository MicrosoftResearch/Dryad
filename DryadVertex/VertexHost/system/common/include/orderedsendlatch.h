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
#pragma warning(disable:4512)  // KLUDGE -- build for now, fix later.

#include <DrCommon.h>

template< class _T > class DryadOrderedSendLatch
{
public:
    typedef _T ListType;

    DryadOrderedSendLatch()
    {
        m_sendState = SS_Empty;
        m_event = ::CreateEvent(NULL, TRUE, FALSE, NULL);
        LogAssert(m_event != NULL);
    }

    ~DryadOrderedSendLatch()
    {
        LogAssert(m_sendState == SS_Empty);
        LogAssert(m_pendingList.IsEmpty());
        BOOL bRet = ::CloseHandle(m_event);
        LogAssert(bRet != 0);
    }


    void Start()
    {
        LogAssert(m_sendState == SS_Empty);
        LogAssert(m_pendingList.IsEmpty());
    }

    void Stop()
    {
        LogAssert(m_sendState == SS_Empty);
        LogAssert(m_pendingList.IsEmpty());
    }

    //
    // If the send latch is sending or blocked, add list to pending 
    //
    void AcceptList(ListType* src)
    {
        if (src->IsEmpty() == false)
        {
            if (m_sendState == SS_Empty)
            {
                //
                // If send state says latch is empty, verify nothing in the pending list
                // and set state to sending
                // todo: shouldn't the m_pendingList append the src parameter so that there's something to send?
                //
                LogAssert(m_pendingList.IsEmpty());
                m_sendState = SS_Sending;
            }
            else
            {
                //
                // If sending or blocking, add list to pending 
                //
                m_pendingList.TransitionToTail(src);
            }
        }
    }

    //
    // If there is currently a pending list, put the supplied list on the end
    // otherwise, stop blocking
    //
    void TransferList(ListType* dst)
    {
        if (m_pendingList.IsEmpty() == false)
        {
            LogAssert(m_sendState != SS_Empty);
            dst->TransitionToTail(&m_pendingList);
        }
        else
        {
            // todo: why isn't dst used in this case?
            if (m_sendState == SS_Blocking)
            {
                BOOL bRet = ::SetEvent(m_event);
                LogAssert(bRet != 0);
            }
            m_sendState = SS_Empty;
        }
    }

    //
    // Block and further sends. Return true if need to wait for blocking to occur, false otherwise.
    //
    bool Interrupt()
    {
        bool mustWait = false;

        //
        // If currently sending, set to blocking and return true
        //
        if (m_sendState == SS_Sending)
        {
            BOOL bRet = ::ResetEvent(m_event);
            LogAssert(bRet != 0);
            m_sendState = SS_Blocking;
            mustWait = true;
        }

        return mustWait;
    }

    //
    // Blocking wait for reset event
    //
    void Wait()
    {
        DWORD dRet = ::WaitForSingleObject(m_event, INFINITE);
        LogAssert(dRet == WAIT_OBJECT_0);
    }

private:
    enum SendState {
        SS_Empty,
        SS_Sending,
        SS_Blocking
    };

    SendState      m_sendState;
    ListType       m_pendingList;
    HANDLE         m_event;
};

