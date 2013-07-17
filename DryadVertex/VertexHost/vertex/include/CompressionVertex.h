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

#include <dryadvertex.h>
#include <vertexfactory.h>

/* A CompressionVertex is a vertex that will either compress or decompress it's input using 
   the gzip format.  The vertex takes one argument, which is either compress or decompress. 
   The vertex can only be called with one output channel, but may be called with multiple 
   input channels.  The requested operation is applied to each input channel in order.  */  
class CompressionVertex : public DryadVertexProgram
{
public:
    CompressionVertex();
    
    void Main(WorkQueue* workQueue,
              UInt32 numberOfInputChannels,
              RChannelReader** inputChannel,
              UInt32 numberOfOutputChannels,
              RChannelWriter** outputChannel);

private:

};

typedef StdTypedVertexFactory<CompressionVertex> FactoryCompressionVertexWrapper;

