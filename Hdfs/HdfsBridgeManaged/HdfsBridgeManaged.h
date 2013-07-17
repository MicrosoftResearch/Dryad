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


//#pragma once
//#include <vcclr.h>

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;

namespace Microsoft { namespace Research { namespace Dryad { namespace Hdfs
{
	//---------------------------------------------------------------------------------------------------

	public ref class HdfsBlockInfo
	{
	public:
		array<String^>^ Hosts;
		long long Size;
		long long Offset;
		int fileIndex;
	};

	//---------------------------------------------------------------------------------------------------

	public ref class HdfsFileInfo
	{
	public:
		String^ Name;
		bool IsDirectory;
		long long Size;
		long long LastModified;
		short Replication;
		long long BlockSize;

		long long totalSize;

		array<HdfsBlockInfo^>^ blockArray;
		array<String^>^ fileNameArray;
	};

	//---------------------------------------------------------------------------------------------------

	public ref class HdfsInstance : public IDisposable
	{
	public:
		static bool Initialize();

		HdfsInstance(String^ hdfsUri);
		~HdfsInstance();

		void Close();

		bool IsFileExists(String^ fileName);

		HdfsFileInfo^ GetFileInfo(String^ fileName, bool getBlockArray);

		bool DeleteFile(String^ fileName, bool recursive);

		bool RenameFile(String^ dstFileName, String^ srcFileName);

		String^ ToInternalUri(String^ fileName);

		String^ FromInternalUri(String^ fileName);

	private:
		bool Open(String^ headNode, long hdfsPort);

		IntPtr   m_instance;
		String^  m_serviceUri;
	};

	//---------------------------------------------------------------------------------------------------
}}}}
