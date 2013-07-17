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

#include "HdfsBridgeNative.h"

#include <jni.h>
#include <windows.h>
#include <assert.h>

static JavaVM* s_jvm = NULL;

static char* GetExceptionMessageLocal(JNIEnv* env, jclass cls, jobject obj)
{
	jfieldID fidMessage = env->GetFieldID(
		cls, "exceptionMessage", "Ljava/lang/String;");

	assert(fidMessage != NULL);

	jstring message = (jstring) env->GetObjectField(obj, fidMessage);

	char* msg = NULL;

	if (message == NULL)
	{
		msg = _strdup("<no message>");
	}
	else
	{
		const char* msgCopy = (const char*)(env->GetStringUTFChars(message, NULL));
		msg = _strdup(msgCopy);
		env->ReleaseStringUTFChars(message, msgCopy);
	}

	env->DeleteLocalRef(message);

	return msg;
}

static JNIEnv* AttachToJvm() 
{
	JNIEnv* env;
	int ret = s_jvm->AttachCurrentThread((void**) &env, NULL);

	assert(ret == JNI_OK);

	return env;
}

namespace HdfsBridgeNative
{
	struct Env
	{
		JNIEnv* e;
	};

	class InstanceInternal
	{
	public:
		jclass     m_clsInstance;
		jobject    m_obj;
		Instance*  m_holder;
	};

	class FileStatInternal
	{
	public:
		jclass             m_clsFileStat;
		jobject            m_fileStat;
		jclass             m_clsBlockLocations;
		jobject            m_blockLocations;
		FileStat*          m_holder;
	};

	class ReaderInternal
	{
	public:
		jclass             m_clsReader;
		jobject            m_reader;
		jclass             m_clsReaderBlock;
		Reader*            m_holder;
	};

	class WriterInternal
	{
	public:
		jclass             m_clsWriter;
		jobject            m_writer;
		Writer*            m_holder;
	};

	HdfsBlockLocInfo::HdfsBlockLocInfo()
	{
		numberOfHosts = 0;
		Hosts = NULL;
		Size = 0;
		Offset = 0;
	}

	HdfsBlockLocInfo::~HdfsBlockLocInfo()
	{
		for (long i=0; i<numberOfHosts; ++i)
		{
			free(Hosts[i]);
		}
		delete [] Hosts;
	}

	bool Initialize() 
	{
		if (s_jvm != NULL)
		{
			return true;
		}

		jsize bufLen = 1;
		jsize nVMs = -1;
		int ret = JNI_GetCreatedJavaVMs(&s_jvm, bufLen, &nVMs);
		if (ret < 0)
		{
			fprintf(stderr, "\nGetCreatedJavaVMs returned %d\n", ret);
			return false;
		}

		if (nVMs == 1)
		{
			fprintf(stderr, "\nProcess already contains %d Java VMs\n", nVMs);
			return true;
		}
        else if (nVMs > 1)
        {
            fprintf(stderr, "\nProcess already contains %d Java VMs\n", nVMs);
            return false;
        }
        
        char classPath[_MAX_ENV];
        DWORD dRet = GetEnvironmentVariableA("JNI_CLASSPATH", classPath, _MAX_ENV);
        if (dRet == 0)
        {
            fprintf(stderr, "Failed to get 'classpath' environment variable\n");
			return false;
        }

        JavaVMInitArgs vm_args;
		JNI_GetDefaultJavaVMInitArgs(&vm_args);        
        vm_args.version = JNI_VERSION_1_6;
        
		JavaVMOption options[1]; // increment when turning on verbose JNI
		vm_args.nOptions = 1;
		vm_args.options = options;
		options[0].optionString = new char[_MAX_ENV];
        sprintf_s(options[0].optionString, _MAX_ENV, "-Djava.class.path=%s", classPath); 
        //fprintf(stderr, "JNI_CLASSPATH:[%s]\n", options[0].optionString);
		//options[1].optionString = "-verbose:jni";
		/*
		vm_args.nOptions = 1;
		JavaVMOption options;
		options.optionString = "-verbose:jni";		
		vm_args.options = &options;
		*/
        vm_args.ignoreUnrecognized = 0;

		JNIEnv* env;
		ret = JNI_CreateJavaVM(&s_jvm, (void**) &env, &vm_args);

		delete [] options[0].optionString;

		if (ret < 0)
		{
			s_jvm = NULL;
			printf("\nCreateJavaVM returned %d\n", ret);     
			return false;
		}

		return true;
	}

	void DisposeString(char* str)
	{
		free(str);
	}

	bool OpenInstance(const char* headNode, long portNumber, Instance** pInstance)
	{
		JNIEnv* env = AttachToJvm();

		jclass clsHdfsBridge = env->FindClass("GSLHDFS/HdfsBridge");
		if (clsHdfsBridge == NULL)
		{
			printf("Failed to find HdfsBridge class\n");
			return false;
		}

		jmethodID midOpenInstance = env->GetStaticMethodID(
			clsHdfsBridge, "OpenInstance",
			"(Ljava/lang/String;J)LGSLHDFS/HdfsBridge$Instance;");
		assert(midOpenInstance != NULL);

		jstring jHeadNode = env->NewStringUTF(headNode);
		jlong jPortNumber = portNumber;

		jobject localInstance = env->CallStaticObjectMethod(
			clsHdfsBridge, midOpenInstance, jHeadNode, jPortNumber);
		env->DeleteLocalRef(jHeadNode);

		if (localInstance == NULL)
		{
			printf("Failed to open instance %s:%d\n", headNode, portNumber);
			return false;
		}

		InstanceInternal* instance = new InstanceInternal();

		instance->m_clsInstance = env->FindClass("GSLHDFS/HdfsBridge$Instance");
		assert(instance->m_clsInstance != NULL);

		instance->m_obj = env->NewGlobalRef(localInstance);
		env->DeleteLocalRef(localInstance);

		Instance* holder = new Instance();
		holder->p = instance;
		instance->m_holder = holder;
		*pInstance = holder;

		return true;
	};

	InstanceAccessor::InstanceAccessor(Instance* instance)
	{
		m_env = new Env;
		m_env->e = AttachToJvm();
		m_inst = (InstanceInternal* ) instance->p;
	}

	InstanceAccessor::~InstanceAccessor()
	{
		delete m_env;
	}

	void InstanceAccessor::Dispose()
	{
		if (m_inst->m_obj != NULL)
		{
			m_env->e->DeleteGlobalRef(m_inst->m_obj);
		}
		delete m_inst->m_holder;
		delete m_inst;
	}

	char* InstanceAccessor::GetExceptionMessage()
	{
		return GetExceptionMessageLocal(m_env->e, m_inst->m_clsInstance, m_inst->m_obj);
	}

	bool InstanceAccessor::IsFileExists(char* fileName, bool* pExists)
	{
		jmethodID midIsFileExist =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "IsFileExist", "(Ljava/lang/String;)I");                         
		assert(midIsFileExist != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jint jFileExists =
			m_env->e->CallIntMethod(m_inst->m_obj, midIsFileExist, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (jFileExists == -1)
		{
			return false;
		}
		else
		{
			*pExists = (jFileExists == 1) ? true : false;
			return true;
		}
	}

	bool InstanceAccessor::DeleteFileOrDir(char* fileName, bool recursive, bool* pDeleted)
	{
		jmethodID midDeleteFile =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "DeleteFile", "(Ljava/lang/String;Z)I");                         
		assert(midDeleteFile != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jboolean jRecursive = (jboolean) recursive;
		jint jFileDeleted =
			m_env->e->CallIntMethod(m_inst->m_obj, midDeleteFile, jFileName, jRecursive);
		m_env->e->DeleteLocalRef(jFileName);

		if (jFileDeleted == -1)
		{
			return false;
		}
		else
		{
			*pDeleted = (jFileDeleted == 1) ? true : false;
			return true;
		}
	}

	bool InstanceAccessor::RenameFileOrDir(char* dstFileName, char* srcFileName, bool* pRenamed)
	{
		jmethodID midRenameFile =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "RenameFile", "(Ljava/lang/String;Ljava/lang/String;)I");                         
		assert(midRenameFile != NULL);

		jstring jDstFileName = m_env->e->NewStringUTF(dstFileName);
		jstring jSrcFileName = m_env->e->NewStringUTF(srcFileName);
		jint jFileRenamed =
			m_env->e->CallIntMethod(m_inst->m_obj, midRenameFile, jDstFileName, jSrcFileName);
		m_env->e->DeleteLocalRef(jDstFileName);
		m_env->e->DeleteLocalRef(jSrcFileName);

		if (jFileRenamed == -1)
		{
			return false;
		}
		else
		{
			*pRenamed = (jFileRenamed == 1) ? true : false;
			return true;
		}
	}

	bool InstanceAccessor::OpenFileStat(
		const char* fileName,
		bool getBlockArray,
		FileStat** pFileStat)
	{
		FileStatInternal* fs = new FileStatInternal();

		fs->m_clsFileStat = m_env->e->FindClass("org/apache/hadoop/fs/FileStatus");
		assert(fs->m_clsFileStat != NULL);

		fs->m_clsBlockLocations = m_env->e->FindClass("GSLHDFS/HdfsBridge$Instance$BlockLocations");
		assert(fs->m_clsBlockLocations != NULL);

		jmethodID midOpenFileStat = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenFileStatus",
			"(Ljava/lang/String;Z)Lorg/apache/hadoop/fs/FileStatus;");  
		assert(midOpenFileStat != NULL);

		fs->m_fileStat = NULL;
		fs->m_blockLocations = NULL;

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localFileStat = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenFileStat, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localFileStat == NULL)
		{
			delete fs;
			return false;
		}
		else
		{
			fs->m_fileStat = m_env->e->NewGlobalRef(localFileStat);
			m_env->e->DeleteLocalRef(localFileStat);
		}

		jmethodID midOpenBlockLocations = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenBlockLocations",
			"(Lorg/apache/hadoop/fs/FileStatus;Z)LGSLHDFS/HdfsBridge$Instance$BlockLocations;");  
		assert(midOpenBlockLocations != NULL);

		jboolean jGetBlockArray = (jboolean) getBlockArray;
		jobject localBlockLoc =
			m_env->e->CallObjectMethod(
			m_inst->m_obj,
			midOpenBlockLocations,
			fs->m_fileStat, jGetBlockArray);

		if (localBlockLoc == NULL)
		{
			m_env->e->DeleteGlobalRef(fs->m_fileStat);
			delete fs;
			return false;
		}
		else
		{
			fs->m_blockLocations = m_env->e->NewGlobalRef(localBlockLoc);
			m_env->e->DeleteLocalRef(localBlockLoc);
		}

		FileStat* fileStat = new FileStat();
		fileStat->p = fs;
		fs->m_holder = fileStat;
		*pFileStat = fileStat;

		return true;
	}

	bool InstanceAccessor::OpenReader(const char* fileName, Reader** pReader)
	{
		ReaderInternal* r = new ReaderInternal;

		r->m_clsReader = m_env->e->FindClass("GSLHDFS/HdfsBridge$Instance$Reader");
		assert(r->m_clsReader != NULL);

		r->m_clsReaderBlock = m_env->e->FindClass("GSLHDFS/HdfsBridge$Instance$Reader$Block");
		assert(r->m_clsReaderBlock != NULL);

		jmethodID midOpenReader = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenReader",
			"(Ljava/lang/String;)LGSLHDFS/HdfsBridge$Instance$Reader;");  
		assert(midOpenReader != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localReader = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenReader, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localReader == NULL)
		{
			delete r;
			return false;
		}
		else
		{
			r->m_reader = m_env->e->NewGlobalRef(localReader);
			m_env->e->DeleteLocalRef(localReader);
		}

		Reader* reader = new Reader();
		reader->p = r;
		r->m_holder = reader;
		*pReader = reader;

		return true;
	}

	bool InstanceAccessor::OpenWriter(const char* fileName, Writer** pWriter)
	{
		WriterInternal* w = new WriterInternal;

		w->m_clsWriter = m_env->e->FindClass("GSLHDFS/HdfsBridge$Instance$Writer");
		assert(w->m_clsWriter != NULL);

		jmethodID midOpenWriter = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenWriter",
			"(Ljava/lang/String;)LGSLHDFS/HdfsBridge$Instance$Writer;");  
		assert(midOpenWriter != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localWriter = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenWriter, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localWriter == NULL)
		{
			delete w;
			return false;
		}
		else
		{
			w->m_writer = m_env->e->NewGlobalRef(localWriter);
			m_env->e->DeleteLocalRef(localWriter);
		}

		Writer* writer = new Writer();
		writer->p = w;
		w->m_holder = writer;
		*pWriter = writer;

		return true;
	}


	FileStatAccessor::FileStatAccessor(FileStat* fileStat)
	{
		m_env = new Env;
		m_env->e = AttachToJvm();
		m_stat = (FileStatInternal *) fileStat->p;
	}

	FileStatAccessor::~FileStatAccessor()
	{
		delete m_env;
	}

	void FileStatAccessor::Dispose()
	{
		if (m_stat->m_fileStat != NULL)
		{
			m_env->e->DeleteGlobalRef(m_stat->m_fileStat);
			m_stat->m_fileStat = NULL;
		}

		if (m_stat->m_blockLocations != NULL)
		{
			m_env->e->DeleteGlobalRef(m_stat->m_blockLocations);
			m_stat->m_blockLocations = NULL;
		}

		delete m_stat->m_holder;
		delete m_stat;
	}

	char* FileStatAccessor::GetExceptionMessage()
	{
		return GetExceptionMessageLocal(m_env->e, m_stat->m_clsFileStat, m_stat->m_fileStat);
	}

	char* FileStatAccessor::GetBlockExceptionMessage()
	{
		return GetExceptionMessageLocal(m_env->e, m_stat->m_clsBlockLocations, m_stat->m_blockLocations);
	}

	long long FileStatAccessor::GetFileLength()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getLen", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	bool FileStatAccessor::IsDir()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "isDir", "()Z");
		assert(mid != NULL);

		jboolean isDir = m_env->e->CallBooleanMethod(m_stat->m_fileStat, mid);
		return (isDir) ? true : false;
	}

	long long FileStatAccessor::GetFileLastModified()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getModificationTime", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	short FileStatAccessor::GetFileReplication()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getReplication", "()S");
		assert(mid != NULL);

		return m_env->e->CallShortMethod(m_stat->m_fileStat, mid);
	}

	long long FileStatAccessor::GetFileBlockSize()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getBlockSize", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	long FileStatAccessor::GetNumberOfBlocks()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetNumberOfBlocks", "()I");
		assert(mid != NULL);

		return m_env->e->CallIntMethod(m_stat->m_blockLocations, mid);
	}

	HdfsBlockLocInfo* FileStatAccessor::GetBlockInfo(long blockId)
	{
		HdfsBlockLocInfo* block = new HdfsBlockLocInfo();

		jint jId = blockId;

		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockLength", "(I)J");
		assert(mid != NULL);

		block->Size = m_env->e->CallLongMethod(m_stat->m_blockLocations, mid, jId);

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockOffset", "(I)J");
		assert(mid != NULL);

		block->Offset = m_env->e->CallLongMethod(m_stat->m_blockLocations, mid, jId);

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockHosts", "(I)[Ljava/lang/String;");
		assert(mid != NULL);

		jobjectArray hostArray = (jobjectArray) m_env->e->CallObjectMethod(m_stat->m_blockLocations, mid, jId);
		if (hostArray != NULL)
		{
			jsize jArrayLength = m_env->e->GetArrayLength(hostArray);
			block->numberOfHosts = jArrayLength;
			block->Hosts = new char* [jArrayLength];

			const char *hostName;
			for (int i=0; i<jArrayLength; ++i) 
			{
				jstring jHost = (jstring) m_env->e->GetObjectArrayElement(hostArray, i);           
				hostName = (const char*) m_env->e->GetStringUTFChars(jHost, NULL);
				block->Hosts[i] = _strdup(hostName);
				m_env->e->ReleaseStringUTFChars(jHost, hostName);
				m_env->e->DeleteLocalRef(jHost);
			}

			m_env->e->DeleteLocalRef(hostArray);
		}

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockFileId", "(I)I");
		assert(mid != NULL);

		block->fileIndex = m_env->e->CallIntMethod(m_stat->m_blockLocations, mid, jId);

		return block;
	}

	void FileStatAccessor::DisposeBlockInfo(HdfsBlockLocInfo* bi)
	{
		delete bi;
	}

	long long FileStatAccessor::GetTotalFileLength()
	{
		jfieldID fidSize = m_env->e->GetFieldID(
			m_stat->m_clsBlockLocations, "fileSize", "J");
		assert(fidSize != NULL);

		return m_env->e->GetLongField(m_stat->m_blockLocations, fidSize);
	}

	long FileStatAccessor::GetNumberOfFiles()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetNumberOfFileNames", "()I");
		assert(mid != NULL);

		return m_env->e->CallIntMethod(m_stat->m_blockLocations, mid);
	}

	char** FileStatAccessor::GetFileNameArray()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetFileNames", "()[Ljava/lang/String;");
		assert(mid != NULL);

		char** array = NULL;

		jobjectArray nameArray = (jobjectArray) m_env->e->CallObjectMethod(m_stat->m_blockLocations, mid);
		if (nameArray != NULL)
		{
			jsize jArrayLength = m_env->e->GetArrayLength(nameArray);
			array = new char* [jArrayLength];

			const char *fileName;
			for (int i=0; i<jArrayLength; ++i) 
			{
				jstring jFile = (jstring) m_env->e->GetObjectArrayElement(nameArray, i);           
				fileName = (const char*) m_env->e->GetStringUTFChars(jFile, NULL);
				array[i] = _strdup(fileName);
				m_env->e->ReleaseStringUTFChars(jFile, fileName);
				m_env->e->DeleteLocalRef(jFile);
			}

			m_env->e->DeleteLocalRef(nameArray);
		}

		return array;
	}

	void FileStatAccessor::DisposeFileNameArray(long length, char** array)
	{
		for (long i=0; i<length; ++i)
		{
			free(array[i]);
		}
		delete [] array;
	}


	ReaderAccessor::ReaderAccessor(Reader* reader)
	{
		m_env = new Env;
		m_env->e = AttachToJvm();
		m_rdr = (ReaderInternal *) reader->p;
	}

	ReaderAccessor::~ReaderAccessor()
	{
		delete m_env;
	}

	void ReaderAccessor::Dispose()
	{
		if (m_rdr->m_reader != NULL)
		{
			m_env->e->DeleteGlobalRef(m_rdr->m_reader);
		}

		delete m_rdr->m_holder;
		delete m_rdr;
	}

	char* ReaderAccessor::GetExceptionMessage()
	{
		return GetExceptionMessageLocal(m_env->e, m_rdr->m_clsReader, m_rdr->m_reader);
	}

	long ReaderAccessor::ReadBlock(long long offset, char* buffer, long bufferLength)
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_rdr->m_clsReader, "ReadBlock", "(JI)LGSLHDFS/HdfsBridge$Instance$Reader$Block;");
		assert(mid != NULL);

		jlong jOffset = offset;
		jint jLength = bufferLength;
		jobject block = m_env->e->CallObjectMethod(m_rdr->m_reader, mid, jOffset, jLength);

		jfieldID fidret = m_env->e->GetFieldID(
			m_rdr->m_clsReaderBlock, "ret", "I");
		assert(fidret != NULL);

		jint bytesRead = m_env->e->GetIntField(block, fidret);

		assert(bytesRead <= bufferLength);

		if (bytesRead > 0)
		{
			jfieldID fid = m_env->e->GetFieldID(
				m_rdr->m_clsReaderBlock, "buffer", "[B");
			assert(fid != NULL);

			jbyteArray byteArray = (jbyteArray) m_env->e->GetObjectField(block, fid);
			assert(byteArray != NULL);

			jint arrayLength = m_env->e->GetArrayLength(byteArray);
			assert(arrayLength >= bytesRead);

			m_env->e->GetByteArrayRegion(byteArray, 0, bytesRead, (jbyte *) buffer);

			m_env->e->DeleteLocalRef(byteArray);
		}

		m_env->e->DeleteLocalRef(block);

		return bytesRead;
	}

	bool ReaderAccessor::Close()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_rdr->m_clsReader, "Close", "()I");

		assert(mid != NULL);

		jint ret = m_env->e->CallIntMethod(m_rdr->m_reader, mid);

		return (ret) ? true : false;
	}


	WriterAccessor::WriterAccessor(Writer* writer)
	{
		m_env = new Env;
		m_env->e = AttachToJvm();
		m_wtr = (WriterInternal *) writer->p;
	}

	WriterAccessor::~WriterAccessor()
	{
		delete m_env;
	}

	void WriterAccessor::Dispose()
	{
		if (m_wtr->m_writer != NULL)
		{
			m_env->e->DeleteGlobalRef(m_wtr->m_writer);
		}

		delete m_wtr->m_holder;
		delete m_wtr;
	}

	char* WriterAccessor::GetExceptionMessage()
	{
		return GetExceptionMessageLocal(m_env->e, m_wtr->m_clsWriter, m_wtr->m_writer);
	}

	bool WriterAccessor::WriteBlock(char* buffer, long bufferLength, bool flushAfter)
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_wtr->m_clsWriter, "WriteBlock", "([BZ)I");
		assert(mid != NULL);

		jboolean jFlush = (jboolean) flushAfter;
		jint jLength = bufferLength;
		jbyteArray jBuffer = m_env->e->NewByteArray(jLength);
		assert(jBuffer != NULL);

		m_env->e->SetByteArrayRegion(jBuffer, 0, jLength, (jbyte *) buffer);

		jint jRet = m_env->e->CallIntMethod(m_wtr->m_writer, mid, jBuffer, jFlush);

		m_env->e->DeleteLocalRef(jBuffer);

		return (jRet) ? true : false;
	}

	bool WriterAccessor::Close()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_wtr->m_clsWriter, "Close", "()I");

		assert(mid != NULL);

		jint ret = m_env->e->CallIntMethod(m_wtr->m_writer, mid);

		return (ret) ? true : false;
	}
};

#if 0

#include <process.h>

struct ReadBlock
{
	HdfsBridgeNative::Reader* r;
	HdfsBridgeNative::Instance* i;
	long long                 o;
	const char*               f;
};

unsigned __stdcall ThreadFunc(void* arg)
{
	ReadBlock* block = (ReadBlock *) arg;

	HdfsBridgeNative::ReaderAccessor r(block->r);

	int bytesRead = 0;
	long long offset = 0;
	char* buffer = new char[256*1024];
	do
	{
		bytesRead = r.ReadBlock(offset, buffer, 256*1024);
		if (bytesRead > 0)
		{
			printf("Read from %s:%I64d:%d\n", block->f, offset, bytesRead);
			offset += bytesRead;
		}
		if (bytesRead < -1)
		{
			printf("%s: %s\n", block->f, r.GetExceptionMessage());
		}
		if (bytesRead == -1)
		{
			printf("EOF\n");
		}
	} while (bytesRead > -1);

	return 0;
}

int main(int argc, wchar_t** argv)
{
	bool ret = HdfsBridgeNative::Initialize();

	if (!ret)
	{
		printf("Failed to initialize\n");
		return 0;
	}

	HdfsBridgeNative::Instance* instance;
	ret = HdfsBridgeNative::OpenInstance("svc-d1-17", 9000, &instance);

	if (!ret)
	{
		printf("failed open\n");
		return 0;
	}

	HANDLE h[4];

	HdfsBridgeNative::InstanceAccessor bridge(instance);

	ReadBlock* r0 = new ReadBlock;
	r0->f = "/data/inputPart0.txt";

	ret = bridge.OpenReader(r0->f, &r0->r);

	if (!ret)
	{
		printf("%s failed open %s\n", r0->f, bridge.GetExceptionMessage());
		bridge.Dispose();
		return 0;
	}

	h[0] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r0,
		0,
		NULL
		);

	h[1] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r0,
		0,
		NULL
		);

	ReadBlock* r1 = new ReadBlock;
	r1->f = "/data";

	HdfsBridgeNative::FileStat* fileStat;
	ret = bridge.OpenFileStat(r1->f, true, &fileStat);

	{
		HdfsBridgeNative::FileStatAccessor fs(fileStat);

		long long ll = fs.GetFileLength();
		ll = fs.GetFileLastModified();
		ll = fs.GetFileBlockSize();
		long l = fs.GetFileReplication();
		bool b = fs.IsDir();

		long nBlocks = fs.GetNumberOfBlocks();
		for (long i=0; i<nBlocks; ++i)
		{
			HdfsBridgeNative::HdfsBlockLocInfo* block = fs.GetBlockInfo(i);
			delete block;
		}

		ll = fs.GetTotalFileLength();

		long nFiles = fs.GetNumberOfFiles();
		char** fArray = fs.GetFileNameArray();
		fs.DisposeFileNameArray(nFiles, fArray);

		fs.Dispose();
	}

	ret = bridge.OpenReader(r1->f, &r1->r);

	if (!ret)
	{
		printf("%s failed open %s\n", r1->f, bridge.GetExceptionMessage());
		bridge.Dispose();
		return 0;
	}

	h[2] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r1,
		0,
		NULL
		);

	h[3] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r1,
		0,
		NULL
		);

	WaitForMultipleObjects(4, h, TRUE, INFINITE);

	HdfsBridgeNative::ReaderAccessor ra0(r0->r);
	ret = ra0.Close();
	ra0.Dispose();

	HdfsBridgeNative::ReaderAccessor ra1(r1->r);
	ret = ra1.Close();
	ra1.Dispose();

	bridge.Dispose();

#if 0
	HdfsBridgeNative::Instance* instance;
	ret = HdfsBridgeNative::OpenInstance("svc-d1-17", 9000, &instance);

	{
		HdfsBridgeNative::InstanceAccessor bridge(instance);

		char* msg = bridge.GetExceptionMessage();
		free(msg);

		bool exists;
		ret = bridge.IsFileExists("data/foo", &exists);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.IsFileExists("/data/inputPart0.txt", &exists);
		msg = bridge.GetExceptionMessage();
		free(msg);

		HdfsBridgeNative::FileStat* fileStat;

		ret = bridge.OpenFileStat("data/foo", false, &fileStat);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.OpenFileStat("/data/inputPart0.txt", true, &fileStat);
		msg = bridge.GetExceptionMessage();
		free(msg);

		{
			HdfsBridgeNative::FileStatAccessor fs(fileStat);

			long long ll = fs.GetFileLength();
			ll = fs.GetFileLastModified();
			ll = fs.GetFileBlockSize();
			long l = fs.GetFileReplication();
			bool b = fs.IsDir();

			long nBlocks = fs.GetNumberOfBlocks();
			for (long i=0; i<nBlocks; ++i)
			{
				HdfsBlockLocInfo* block = fs.GetBlockInfo(i);
				delete block;
			}

			fs.Dispose();
		}

		HdfsBridgeNative::Reader* reader;

		ret = bridge.OpenReader("data/foo", &reader);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.OpenReader("/data/inputPart0.txt", &reader);
		msg = bridge.GetExceptionMessage();
		free(msg);

		{
			HdfsBridgeNative::ReaderAccessor r(reader);

			int bytesRead = 0;
			long long offset = 0;
			char* buffer = new char[256*1024];
			FILE* f;
			errno_t e = fopen_s(&f, "\\users\\misard\\foo.txt", "wb");
			do
			{
				bytesRead = r.ReadBlock(offset, buffer, 256*1024);
				if (bytesRead > 0)
				{
					printf("Read from %I64d:%d\n", offset, bytesRead);
					fwrite(buffer, 1, bytesRead, f);
					offset += bytesRead;
				}
				if (bytesRead < -1)
				{
					msg = r.GetExceptionMessage();
					printf("%s\n", msg);
					free(msg);
				}
				if (bytesRead == -1)
				{
					printf("EOF\n");
				}
			} while (bytesRead > -1);

			fclose(f);

			ret = r.Close();
			msg = r.GetExceptionMessage();
			free(msg);

			r.Dispose();
		}

		bridge.Dispose();
	}
#endif

	return 0;
}

#endif