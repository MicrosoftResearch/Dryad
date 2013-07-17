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
#include <string.h>
#include <stdlib.h>

//---------------------------------------------------------------------------------------------------

namespace HdfsBridgeNative
{
	struct Instance
	{
		void* p;
	};
	class InstanceInternal;

	struct FileStat
	{
		void* p;
	};
	class FileStatInternal;

	struct Reader
	{
		void* p;
	};
	class ReaderInternal;

	struct Writer
	{
		void* p;
	};
	class WriterInternal;

	struct Env;

	bool Initialize();
	void DisposeString(char* str);

	class HdfsBlockLocInfo
	{
	public:
		HdfsBlockLocInfo();
		~HdfsBlockLocInfo();

		long numberOfHosts; /* length of the hosts array */
		char** Hosts;       /* hosts storing block replicas, freed by destructor */
		long long Size;     /* the size of the block in bytes */
		long long Offset;   /* start offset of file associated with this block */
		int fileIndex;      /* which file in a directory this block is part of */
	};

	class FileStatAccessor
	{
	public:
		FileStatAccessor(FileStat* fileStat);
		~FileStatAccessor();

		void Dispose();

		char* GetExceptionMessage();
		char* GetBlockExceptionMessage();

		long long GetFileLength();
		bool IsDir();
		long long GetFileLastModified();
		short GetFileReplication();
		long long GetFileBlockSize();

		long long GetTotalFileLength();
		long GetNumberOfBlocks();
		HdfsBlockLocInfo* GetBlockInfo(long blockId);
		void DisposeBlockInfo(HdfsBlockLocInfo* blockInfo);
		long GetNumberOfFiles();
		char** GetFileNameArray();
		void DisposeFileNameArray(long length, char** array);

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*                m_env;
		FileStatInternal*   m_stat;
	};

	class ReaderAccessor
	{
	public:
		ReaderAccessor(Reader* reader);
		~ReaderAccessor();

		void Dispose();

		char* GetExceptionMessage();

		long ReadBlock(long long offset, char* buffer, long bufferSize);

		bool Close();

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*              m_env;
		ReaderInternal*   m_rdr;
	};

	class WriterAccessor
	{
	public:
		WriterAccessor(Writer* writer);
		~WriterAccessor();

		void Dispose();

		char* GetExceptionMessage();

		bool WriteBlock(char* buffer, long bufferSize, bool flushAfter);

		bool Close();

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*              m_env;
		WriterInternal*   m_wtr;
	};

	class InstanceAccessor
	{
	public:
		InstanceAccessor(Instance* instance);
		~InstanceAccessor();

		void Dispose();

		char* GetExceptionMessage();

		bool IsFileExists(char* fileName, bool* pExists);

		bool DeleteFileOrDir(char* fileName, bool recursive, bool* pDeleted);

		bool RenameFileOrDir(char* dstFileName, char* srcFileName, bool* pRenamed);

		bool OpenFileStat(const char* fileName, bool getBlockArray, FileStat** pFileStat);

		bool OpenReader(const char* fileName, Reader** pReader);

		bool OpenWriter(const char* fileName, Writer** pWriter);

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*                m_env;
		InstanceInternal*   m_inst;
	};

	bool OpenInstance(const char* headNode, long portNumber, Instance** pInstance);
};
//---------------------------------------------------------------------------------------------------

