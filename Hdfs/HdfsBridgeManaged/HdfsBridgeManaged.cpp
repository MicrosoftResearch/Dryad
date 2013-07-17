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

// HdfsBridgeManaged.cpp : main project file.

//#include "stdafx.h"

#pragma warning( disable: 4793 )

#include "HdfsBridgeNative.h"

#include "HdfsBridgeManaged.h"

//#include <string>
//#include <iostream>


#define SUCCESS 0
#define FAILURE -1

using namespace System;
using namespace Microsoft::Research::Dryad::Hdfs;


//---------------------------------------------------------------------------------------------------
#if 0
int main(array<System::String ^> ^args)
{
	Console::WriteLine(L"Hello World");

	Uri^ headUri = gcnew Uri("hpchdfs://svc-d1-17:9000/");
	String^ hdfsFileSetName = "/data";
	String^ hdfsFileSetName1 = "/data/inputPart1.txt";

	bool ret = HdfsInstance::Initialize();

	HdfsInstance^ managedHdfsClient1 = gcnew HdfsInstance(headUri);

	HdfsFileInfo^ managedFileInfo = managedHdfsClient1->GetFileInfo(hdfsFileSetName, true);

	HdfsInstance^ managedHdfsClient2 = gcnew HdfsInstance(headUri);
	bool exists = managedHdfsClient2->IsFileExists(hdfsFileSetName1);

	//String^ hdfsFileSetName3 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient3 = gcnew HdfsInstance(headNode, hdfsPort);
	//array<HdfsBlockInfo^>^ blocks = managedHdfsClient3->GetBlocks(hdfsFileSetName3);

	//String^ hdfsFileSetName4 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient4 = gcnew HdfsInstance(headNode, hdfsPort);
	//String^ blockContent = managedHdfsClient4->ReadBlock(hdfsFileSetName4, 0, 0);

	//String^ hdfsFileSetName3 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient6 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists2 = managedHdfsClient6->IsFileExists(hdfsFileSetName3);

	//String^ hdfsFileSetName7 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient7 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists3 = managedHdfsClient7->IsFileExists(hdfsFileSetName7);

	//String^ hdfsFileSetName8 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient8 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists4 = managedHdfsClient8->IsFileExists(hdfsFileSetName8);

	return 0;
}
#endif

namespace Microsoft { namespace Research { namespace Dryad { namespace Hdfs
{
	bool HdfsInstance::Initialize()
	{
		return HdfsBridgeNative::Initialize();
	}

	HdfsInstance::HdfsInstance(String^ hdfsString)
	{
		bool ret = HdfsInstance::Initialize();

		if (!ret)
		{
			throw gcnew ApplicationException("Unable to initialize Hdfs bridge");
		}

		Uri^ hdfsUri = gcnew Uri(hdfsString);
		ret = Open(hdfsUri->Host, hdfsUri->Port);

		if (!ret)
		{
			throw gcnew 
				ApplicationException(String::Format("Unable to connect to Hdfs at {0}:{1}", hdfsUri->Host, hdfsUri->Port));
		}

		m_serviceUri = hdfsUri->Scheme + "://" + hdfsUri->Host + ":" + hdfsUri->Port + "/";
	}

	HdfsInstance::~HdfsInstance()
	{
		Close();
	}

	bool HdfsInstance::Open(String^ headNode, long hdfsPort)
	{
		char* cHeadNode = (char *) Marshal::StringToHGlobalAnsi(headNode).ToPointer();

		HdfsBridgeNative::Instance* instance;
		bool ret = HdfsBridgeNative::OpenInstance(cHeadNode, hdfsPort, &instance);

		Marshal::FreeHGlobal(IntPtr(cHeadNode));

		if (ret)
		{
			m_instance = IntPtr(instance);
		}
		else
		{
			m_instance = IntPtr::Zero;
		}

		return ret;
	}

	void HdfsInstance::Close()
	{
		if (m_instance != IntPtr::Zero)
		{
			HdfsBridgeNative::InstanceAccessor ia((HdfsBridgeNative::Instance *) m_instance.ToPointer());

			ia.Dispose();

			m_instance = IntPtr::Zero;
		}

		m_serviceUri = nullptr;
	}

	HdfsFileInfo^ HdfsInstance::GetFileInfo(String^ fileName, bool getBlockArray)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		HdfsBridgeNative::InstanceAccessor ia((HdfsBridgeNative::Instance *) m_instance.ToPointer());

		HdfsBridgeNative::FileStat* fileStat;
		bool ret = ia.OpenFileStat(cFileName, getBlockArray, &fileStat);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HdfsBridgeNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs GetFileInfo: " + errorMsg);
		}

		HdfsBridgeNative::FileStatAccessor fs(fileStat);

		HdfsFileInfo^ fileInfo = gcnew HdfsFileInfo();

		fileInfo->Name = fileName;
		fileInfo->IsDirectory = fs.IsDir();
		fileInfo->Size = fs.GetFileLength();
		fileInfo->LastModified = fs.GetFileLastModified();
		fileInfo->Replication = fs.GetFileReplication();
		fileInfo->BlockSize = fs.GetFileBlockSize();

		long numberOfFiles = fs.GetNumberOfFiles();
		fileInfo->fileNameArray = gcnew array<String^>(numberOfFiles);

		char** cArray = fs.GetFileNameArray();
		for (long i=0; i<numberOfFiles; ++i)
		{
			fileInfo->fileNameArray[i] = Marshal::PtrToStringAnsi((IntPtr) cArray[i]);
		}

		fs.DisposeFileNameArray(numberOfFiles, cArray);

		if (getBlockArray)
		{
			fileInfo->blockArray = gcnew array<HdfsBlockInfo^>(fs.GetNumberOfBlocks());

			for (long i=0; i<fileInfo->blockArray->Length; ++i)
			{
				HdfsBridgeNative::HdfsBlockLocInfo* info = fs.GetBlockInfo(i);

				fileInfo->blockArray[i] = gcnew HdfsBlockInfo();

				fileInfo->blockArray[i]->fileIndex = info->fileIndex;
				fileInfo->blockArray[i]->Size = info->Size;
				fileInfo->blockArray[i]->Offset = info->Offset;
				fileInfo->blockArray[i]->Hosts = gcnew array<String^>(info->numberOfHosts);

				for (int j=0; j<info->numberOfHosts; ++j)
				{
					String^ h = Marshal::PtrToStringAnsi((IntPtr) info->Hosts[j]);
					fileInfo->blockArray[i]->Hosts[j] = h;
				}

				fs.DisposeBlockInfo(info);
			}

			fileInfo->totalSize = fs.GetTotalFileLength();
		}

		fs.Dispose();

		return fileInfo;
	}

	bool HdfsInstance::IsFileExists(String^ fileName)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		HdfsBridgeNative::InstanceAccessor ia((HdfsBridgeNative::Instance *) m_instance.ToPointer());

		bool exists = false;
		bool result = ia.IsFileExists(cFileName, &exists);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HdfsBridgeNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs IsFileExists: " + errorMsg);
		}

		return exists;
	}

	bool HdfsInstance::DeleteFile(String^ fileName, bool recursive)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		HdfsBridgeNative::InstanceAccessor ia((HdfsBridgeNative::Instance *) m_instance.ToPointer());

		bool deleted = false;
		bool result = ia.DeleteFileOrDir(cFileName, recursive, &deleted);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HdfsBridgeNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs DeleteFile: " + errorMsg);
		}

		return deleted;
	}

	bool HdfsInstance::RenameFile(String^ dstFileName, String^ srcFileName)
	{
		// Marshal the managed strings to unmanaged memory.
		char* cDstFileName = (char*) Marshal::StringToHGlobalAnsi(dstFileName).ToPointer();
		char* cSrcFileName = (char*) Marshal::StringToHGlobalAnsi(srcFileName).ToPointer();

		HdfsBridgeNative::InstanceAccessor ia((HdfsBridgeNative::Instance *) m_instance.ToPointer());

		bool renamed;
		bool result = ia.RenameFileOrDir(cDstFileName, cSrcFileName, &renamed);

		// free the unmanaged strings.
		Marshal::FreeHGlobal(IntPtr(cSrcFileName));
		Marshal::FreeHGlobal(IntPtr(cDstFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HdfsBridgeNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs RenameFile: " + errorMsg);
		}

		return renamed;
	}

	String^ HdfsInstance::FromInternalUri(String^ inputString)
	{
		if (inputString->StartsWith(m_serviceUri))
		{
			return inputString->Substring(m_serviceUri->Length);
		}
		else
		{
			throw gcnew ApplicationException(inputString + " doesn't start with " + m_serviceUri);
			return nullptr;
		}
	}

	String^ HdfsInstance::ToInternalUri(String^ inputString)
	{
		return m_serviceUri + inputString;
	}
}}}}
