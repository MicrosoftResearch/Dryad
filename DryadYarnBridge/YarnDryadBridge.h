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
#include <jni.h>

extern "C"
{

    //void Register(DryadYarnBridgeBase* bridge);

    void __stdcall SendVertexState(int vertexId, int state);

    JNIEXPORT void JNICALL Java_TestLib_SendVertexState(JNIEnv *env, jobject obj, jint vertexId, jint state);

    JNIEXPORT void JNICALL Java_com_microsoft_research_DryadAppMaster_SendVertexState(JNIEnv *env, jobject obj, int vertexId, jint state, jstring nodeName);

}

