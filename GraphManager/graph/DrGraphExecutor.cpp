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

#include <DrGraphHeaders.h>

DrGraphExecutor::DrGraphExecutor()
{
    m_event = ::CreateEvent(NULL, TRUE, FALSE, NULL);
}

DrGraphExecutor::~DrGraphExecutor()
{
    ::CloseHandle(m_event);
}

DrGraphPtr DrGraphExecutor::Initialize(DrGraphParametersPtr parameters)
{
    DrMessagePumpRef pump = DrNew DrMessagePump(8, 4);
    pump->Start();

    DrUniverseRef universe = DrNew DrUniverse();

    DrClusterRef cluster = DrCluster::Create();
    if (SUCCEEDED( cluster->Initialize(universe, pump, parameters->m_propertyUpdateInterval) ))
    {
        m_graph = DrNew DrGraph(cluster, parameters);
    }

    for (int i=0; i<parameters->m_reporters->Size(); ++i)
    {
        parameters->m_defaultVertexTemplate->GetListenerList()->Add(parameters->m_reporters[i]);
        parameters->m_defaultProcessTemplate->GetListenerList()->Add(parameters->m_reporters[i]);
    }

    return m_graph;
}

void DrGraphExecutor::Run()
{
    m_graph->AddListener(this);
    {
        DrAutoCriticalSection acs(m_graph);
        m_graph->StartRunning();
    }
}

void DrGraphExecutor::ReceiveMessage(DrErrorRef exitStatus)
{
    m_exitStatus = exitStatus;
    ::SetEvent(m_event);
}

DrErrorPtr DrGraphExecutor::Join()
{
    ::WaitForSingleObject(m_event, INFINITE);

    if (m_exitStatus && m_exitStatus->m_code != 0)
    {
        m_graph->GetCluster()->CompleteProgress( m_exitStatus->m_explanation.GetChars());
    }
    else
    {
        m_graph->GetCluster()->CompleteProgress( "" );
    }

    m_graph->GetCluster()->Shutdown();

    DrMessagePumpRef pump = m_graph->GetCluster()->GetMessagePump();

    pump->Stop();

    m_graph->CancelListener(this);

    DrUniverseRef cluster = m_graph->GetCluster()->GetUniverse();
    cluster->Discard();

    m_graph->Discard();

    m_graph = DrNull;

    return m_exitStatus;
}
