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

#include <dryadlisthelper.h>

class DryadFixedMemoryBuffer : public DrFixedMemoryBuffer
{
public:
    DryadFixedMemoryBuffer();
    DryadFixedMemoryBuffer(BYTE *pData,
                           Size_t allocatedSize, Size_t availableSize = 0);
    virtual ~DryadFixedMemoryBuffer();

private:
    DrBListEntry   m_listPtr;
    friend class DryadBList<DryadFixedMemoryBuffer>;
};

typedef DryadBList<DryadFixedMemoryBuffer> DryadFixedBufferList;

class DryadFixedMemoryBufferCopy : public DryadFixedMemoryBuffer
{
public:
    DryadFixedMemoryBufferCopy(DryadFixedMemoryBuffer* src);
    ~DryadFixedMemoryBufferCopy();

private:
    BYTE* m_dataCopy;
};

class DryadLockedMemoryBuffer : public DrFixedMemoryBuffer
{
public:
    DryadLockedMemoryBuffer();
    DryadLockedMemoryBuffer(BYTE *pData, Size_t allocatedSize);
    virtual ~DryadLockedMemoryBuffer();

    void Init(BYTE *pData, Size_t allocatedSize);

    void SetAvailableSize(Size_t uSize);

private:
    DrBListEntry   m_listPtr;
    friend class DryadBList<DryadLockedMemoryBuffer>;
};

typedef DryadBList<DryadLockedMemoryBuffer> DryadLockedBufferList;

class DryadAlignedReadBlock : public DryadLockedMemoryBuffer
{
public:
    DryadAlignedReadBlock(size_t size, size_t alignment);
    ~DryadAlignedReadBlock();

    void Trim(Size_t numBytes);

    void* GetData();

private:
    void*     m_data;
    void*     m_alignedData;
    size_t    m_alignment;
};

class DryadAlignedWriteBlock : public DryadFixedMemoryBuffer
{
public:
    DryadAlignedWriteBlock(size_t size, size_t alignment);
    ~DryadAlignedWriteBlock();

    void* GetData();

private:
    void*     m_data;
    void*     m_alignedData;
    size_t    m_alignment;
};

