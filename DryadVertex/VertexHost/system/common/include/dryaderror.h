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


// This file must contain *only* DEFINE_DRYAD_ERROR directives!
//
//
// It is included multiple times with different macro definitions.

// Dummy error
DEFINE_DRYAD_ERROR (DryadError_BadMetaData,               DRYAD_ERROR (0x0001), "Bad MetaData XML")
DEFINE_DRYAD_ERROR (DryadError_InvalidCommand,            DRYAD_ERROR (0x0002), "Invalid Command")
DEFINE_DRYAD_ERROR (DryadError_VertexReceivedTermination, DRYAD_ERROR (0x0003), "Vertex Received Termination")
DEFINE_DRYAD_ERROR (DryadError_InvalidChannelURI,         DRYAD_ERROR (0x0004), "Invalid Channel URI syntax")
DEFINE_DRYAD_ERROR (DryadError_ChannelOpenError,          DRYAD_ERROR (0x0005), "Channel Open Error")
DEFINE_DRYAD_ERROR (DryadError_ChannelRestartError,       DRYAD_ERROR (0x0006), "Channel Restart Error")
DEFINE_DRYAD_ERROR (DryadError_ChannelWriteError,         DRYAD_ERROR (0x0007), "Channel Write Error")
DEFINE_DRYAD_ERROR (DryadError_ChannelReadError,          DRYAD_ERROR (0x0008), "Channel Read Error")
DEFINE_DRYAD_ERROR (DryadError_ItemParseError,            DRYAD_ERROR (0x0009), "Item Parse Error")
DEFINE_DRYAD_ERROR (DryadError_ItemMarshalError,          DRYAD_ERROR (0x0010), "Item Marshal Error")
DEFINE_DRYAD_ERROR (DryadError_BufferHole,                DRYAD_ERROR (0x0011), "Buffer Hole")
DEFINE_DRYAD_ERROR (DryadError_ItemHole,                  DRYAD_ERROR (0x0012), "Item Hole")
DEFINE_DRYAD_ERROR (DryadError_ChannelRestart,            DRYAD_ERROR (0x0013), "Channel Sent Restart")
DEFINE_DRYAD_ERROR (DryadError_ChannelAbort,              DRYAD_ERROR (0x0014), "Channel Sent Abort")
DEFINE_DRYAD_ERROR (DryadError_VertexRunning,             DRYAD_ERROR (0x0015), "Vertex Is Running")
DEFINE_DRYAD_ERROR (DryadError_VertexCompleted,           DRYAD_ERROR (0x0016), "Vertex Has Completed")
DEFINE_DRYAD_ERROR (DryadError_VertexError,               DRYAD_ERROR (0x0017), "Vertex Had Errors")
DEFINE_DRYAD_ERROR (DryadError_ProcessingError,           DRYAD_ERROR (0x0018), "Error While Processing")
DEFINE_DRYAD_ERROR (DryadError_VertexInitialization,      DRYAD_ERROR (0x0019), "Vertex Could Not Initialize")
DEFINE_DRYAD_ERROR (DryadError_ProcessingInterrupted,        DRYAD_ERROR (0x001a), "Processing was interrupted before completion")
DEFINE_DRYAD_ERROR (DryadError_VertexChannelClose,        DRYAD_ERROR (0x001b), "Errors during channel close")
DEFINE_DRYAD_ERROR (DryadError_AssertFailure,             DRYAD_ERROR (0x001c), "Assertion Failure")
DEFINE_DRYAD_ERROR (DryadError_ExternalChannel,           DRYAD_ERROR (0x001d), "External Channel")
DEFINE_DRYAD_ERROR (DryadError_AlreadyInitialized,        DRYAD_ERROR (0x001e), "Dryad Already Initialized")
DEFINE_DRYAD_ERROR (DryadError_DuplicateVertices,         DRYAD_ERROR (0x001f), "Duplicate Vertices")
DEFINE_DRYAD_ERROR (DryadError_ComposeRHSNeedsInput,      DRYAD_ERROR (0x0020), "RHS of composition must have at least one input")
DEFINE_DRYAD_ERROR (DryadError_ComposeLHSNeedsOutput,     DRYAD_ERROR (0x0021), "LHS of composition must have at least one output")
DEFINE_DRYAD_ERROR (DryadError_ComposeStagesMustBeDifferent, DRYAD_ERROR (0x0022), "Stages for composition must be different")
DEFINE_DRYAD_ERROR (DryadError_ComposeStageEmpty,         DRYAD_ERROR (0x0023), "Stage for composition is empty")
DEFINE_DRYAD_ERROR (DryadError_VertexNotInGraph,          DRYAD_ERROR (0x0024), "Vertex not in graph")
DEFINE_DRYAD_ERROR (DryadError_HardConstraintCannotBeMet, DRYAD_ERROR (0x0025), "Hard constraint cannot be met")
DEFINE_DRYAD_ERROR (DryadError_MustRequeue,               DRYAD_ERROR (0x0026), "Must requeue process")
