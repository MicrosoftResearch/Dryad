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
#include "stdafx.h"
#include "yarndryadbridge.h"
#include "YarnAppMasterManaged.h"


// #pragma managed
// push unmanaged state on to stack and set managed state
#pragma managed(push, on)

void __stdcall SendVertexState(int vertexId, int state, const char *nodeName)
{
	String^ nodeNameString = gcnew String(nodeName);
    //s_bridge->SendVertexState(vertexId, state);
    //System::Console::Error->WriteLine("\nIn managed function.");
	//System::Console::Error->WriteLine("YarnDryadBridge: Vertex Id: {0} State: {1} NodeName: '{2}'", vertexId, state, nodeNameString);
    
    Microsoft::Research::Dryad::YarnBridge::AMInstance::UpdateProcess(vertexId, state, nodeNameString);
    //printf("Vertex Id: %I64d, State: %d\n", vertexId, state);
}
// #pragma unmanaged
#pragma managed(pop)



void JNICALL Java_com_microsoft_research_TestLib_SendVertexState(JNIEnv *env, jobject obj, jint vertexId, jint state, jstring nodeName)
{
    const char* nodeNameCopy = (const char*)(env->GetStringUTFChars(nodeName, NULL));
    SendVertexState((int) vertexId, (int) state, nodeNameCopy);
    env->ReleaseStringUTFChars(nodeName, nodeNameCopy);
    //printf("Vertex Id: %d, State: %d\n", vertexId, state);
}

void JNICALL Java_com_microsoft_research_DryadAppMaster_SendVertexState(JNIEnv *env, jobject obj, jint vertexId, jint state, jstring nodeName)
{
    const char* nodeNameCopy = (const char*)(env->GetStringUTFChars(nodeName, NULL));
    SendVertexState((int) vertexId, (int) state, nodeNameCopy);
    env->ReleaseStringUTFChars(nodeName, nodeNameCopy);
}

