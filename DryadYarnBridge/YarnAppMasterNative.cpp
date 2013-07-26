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

#pragma unmanaged
#include "YarnAppMasterNative.h"

#include <jni.h>
#include <windows.h>
#include <assert.h>
#include <Share.h>

static JavaVM* s_jvm = NULL;
//static FILE* s_logfile = NULL;

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

namespace DryadYarn
{
#define CHAR_BUFFER_SIZES 70000
    struct Env
    {
        JNIEnv* e;
    };

    class InstanceInternal
    {
    public:
        jmethodID  m_midSchProc;
		jmethodID  m_midShutdown;
        jclass     m_clsInstance;
        jobject    m_obj;
        Instance*  m_holder;
    };

    bool Initialize() 
    {
        //::DebugBreak();
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

        if (nVMs != 0)
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
            fprintf(stderr, "\nCreateJavaVM returned %d\n", ret); 
			fflush(stderr);
            return false;
        }
        fflush(stderr);
        return true;
    }
	
    AMNativeInstance::AMNativeInstance()
    {
        /*
        while (!::IsDebuggerPresent())
        {
            printf("Waiting for debugger\n");fflush(stdout);
            Sleep(1000);
        }
        ::DebugBreak();
        */
        m_inst = NULL;
        m_env = NULL;
    }

    AMNativeInstance::~AMNativeInstance()
    {
        if (m_inst != NULL && m_inst->m_obj != NULL)
        {
            m_env->e->DeleteGlobalRef(m_inst->m_obj);
        }
        delete m_inst->m_holder;
        delete m_inst;
        m_inst = NULL;
        delete m_env;
        m_env = NULL;
    }

    bool AMNativeInstance::OpenInstance()
    {

        //TODO Determine if we should detach the current thread from the jvm when exiting this call
        if (Initialize()) 
        {
            m_env = new Env;
            m_env->e = AttachToJvm();
        } 
        else 
        {
            return false;
        }

		jclass clsDryadAppMaster = m_env->e->FindClass("com/microsoft/research/DryadAppMaster");
        if (clsDryadAppMaster == NULL)
        {
			jthrowable exc;
			exc = m_env->e->ExceptionOccurred();
			if (exc) {
				 m_env->e->ExceptionDescribe();
				 m_env->e->ExceptionClear();
			}

            fprintf(stderr, "Failed to find DryadAppMaster class\n");
            fprintf(stderr, "Destroying JVM\n");
            fflush(stderr);
            s_jvm->DestroyJavaVM();
            return false;
        }

        jmethodID midAMCons = m_env->e->GetMethodID(clsDryadAppMaster, "<init>", "()V");
        assert(midAMCons != NULL);

        jobject localInstance = m_env->e->NewObject(clsDryadAppMaster, midAMCons);

        if (localInstance == NULL)
        {
			jthrowable exc;
			exc = m_env->e->ExceptionOccurred();
			if (exc) {
				 m_env->e->ExceptionDescribe();
				 m_env->e->ExceptionClear();
			}

            fprintf(stderr, "Failed to initialize DryadAppMaster\n");
            fprintf(stderr, "Destroying JVM\n");
            fflush(stderr);
            s_jvm->DestroyJavaVM();
            return false;
        }

        jmethodID midSchProc = m_env->e->GetMethodID(clsDryadAppMaster, "scheduleProcess", "(ILjava/lang/String;Ljava/lang/String;)V");
        if (midSchProc == NULL)
        {
            jthrowable exc;
            exc = m_env->e->ExceptionOccurred();
            if (exc) {
                m_env->e->ExceptionDescribe();
                m_env->e->ExceptionClear();
            }

            fprintf(stderr, "Failed to find DryadAppMaster.scheduleProcess method\n");
            fprintf(stderr, "Destroying JVM\n");
            fflush(stderr);
            s_jvm->DestroyJavaVM();
            return false;
        }

		jmethodID midShutdown = m_env->e->GetMethodID(clsDryadAppMaster, "shutdown", "(ZZ)V");
        if (midShutdown == NULL)
        {
            jthrowable exc;
            exc = m_env->e->ExceptionOccurred();
            if (exc) {
                m_env->e->ExceptionDescribe();
                m_env->e->ExceptionClear();
            }

            fprintf(stderr, "Failed to find DryadAppMaster.shutdown method\n");
            fprintf(stderr, "Destroying JVM\n");
            fflush(stderr);
            s_jvm->DestroyJavaVM();
            return false;
        }

        m_inst = new InstanceInternal();

        m_inst->m_clsInstance = clsDryadAppMaster;
        m_inst->m_obj = m_env->e->NewGlobalRef(localInstance);
        m_env->e->DeleteLocalRef(localInstance);
        m_inst->m_midSchProc = midSchProc;
		m_inst->m_midShutdown = midShutdown;
        fprintf(stderr, "Created Instance\n");
        fflush(stderr);
        return true;
    }

    char* AMNativeInstance::GetExceptionMessage()
    {
        return GetExceptionMessageLocal(m_env->e, m_inst->m_clsInstance, m_inst->m_obj);
    }

    bool AMNativeInstance::ScheduleProcess(int vertexId, const char* name, const char* commandLine)
    {
        fprintf(stderr, "Scheduling process %s\n", commandLine);
        fflush(stderr);
        JNIEnv* env = AttachToJvm(); 

        jstring jName = env->NewStringUTF(name);
        jstring jCmdLine = env->NewStringUTF(commandLine);
        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midSchProc, vertexId, jName, jCmdLine);

        env->DeleteLocalRef(jName);
        env->DeleteLocalRef(jCmdLine);

        // detach here?

        return true;
    }

	bool AMNativeInstance::Shutdown(bool success)
    {
        fprintf(stderr, "Shutting down AMNativeInstance\n");
        fflush(stderr);
        JNIEnv* env = AttachToJvm(); 
		fprintf(stderr, "Calling Shutdown\n");
        fflush(stderr);
		
		jboolean jImmedShutdown = 0;
		jboolean jSuccess = 0;
		if (success)
		{
			jSuccess = 1;
		}
        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midShutdown, jImmedShutdown, jSuccess);

        // detach here?
		fprintf(stderr, "Finished Shutdown\n");
        fflush(stderr);
        return true;
    }



}