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

//
// Includes
//
#include <DrExecution.h>
#include <vertexfactory.h>
#include <dryaderrordef.h>

//
// Create a vertex factory and register it 
//
DryadVertexFactoryBase::DryadVertexFactoryBase(const char* name)
{
    m_name = name;
    VertexFactoryRegistry::RegisterFactory(this);
}

//
// Destructor does nothing
//
DryadVertexFactoryBase::~DryadVertexFactoryBase()
{
}

const char* DryadVertexFactoryBase::GetName()
{
    return m_name;
}

//
// The Register method does nothing, but can be used to pull in static
//   factories from other compilation units. 
//
void DryadVertexFactoryBase::Register()
{
}

//
// Create reference to new program
//
DryadVertexProgramRef DryadVertexFactoryBase::MakeUntyped()
{
    DryadVertexProgramRef p;
    p.Attach(NewUntyped());
    return p;
}

//
// Have factory create new program base
//
DryadVertexProgramBase* DryadVertexFactoryBase::NewUntyped()
{
    return NewUntypedInternal();
}

/* this is guaranteed to be set to zero before anyone's constructor is
   called */
static VertexFactoryRegistry* s_dryadVertexRegistry;

//
// Create new empty factory registry
//
VertexFactoryRegistry::VertexFactoryRegistry()
{
    m_registeredNULL = false;
}

//
// Register a factory 
//
void VertexFactoryRegistry::RegisterFactory(DryadVertexFactoryBase* factory)
{
    //
    // If there is no registry, create one
    //
    if (s_dryadVertexRegistry == 0)
    {
        s_dryadVertexRegistry = new VertexFactoryRegistry();
    }

    VertexFactoryRegistry* self = s_dryadVertexRegistry;

    const char* name = factory->GetName();
    if (name == NULL)
    {
        //
        // If no factory name, this is an error.
        // RegisterFactory gets called during static initializations,
        // so just save the errors and log properly in :LookupFactory
        // or :ShowAllVertexUsageMessages
        //
        self->m_registeredNULL = true;
    }
    else
    {
        FactoryMap::iterator existing = self->m_factories.find(name);
        if (existing != self->m_factories.end())
        {
            //
            // If factory name already in registry, this is an error
            // RegisterFactory gets called during static
            // initializations, so just save the errors and log
            // properly in :LookupFactory or :ShowAllVertexUsageMessages
            //
            self->m_errorSet.insert(name);
        }
        else
        {
            //
            // If factory name doesn't exist, add it to list of registered factories (key is name).
            //
            self->m_factories.insert(std::make_pair(name, factory));
        }
    }
}

//
// Get Vertex factory from name
//
DryadVertexFactoryBase* VertexFactoryRegistry::LookUpFactory(const char* name)
{
    //
    // Create a vertex registry if none exist
    //
    if (s_dryadVertexRegistry == 0)
    {
        s_dryadVertexRegistry = new VertexFactoryRegistry();
    }

    VertexFactoryRegistry* self = s_dryadVertexRegistry;

    //
    // If NULL name was provided to the registry on initialization, log the error
    //
    if (self->m_registeredNULL)
    {
        DrLogE("Factory Registered With illegal NULL name");
    }

    //
    // If there are any elements in m_errorSet, multiple factories of the same 
    // were attempted to be registered. Log this error.
    //
    if (self->m_errorSet.empty() == false)
    {
        DuplicateSet::iterator i;
        for (i = self->m_errorSet.begin(); i != self->m_errorSet.end(); ++i)
        {
            DrLogE("Duplicate Factory Registered. Factory name: %s", i->c_str());
        }
    }

    //
    // If any errors, report that they exist at the assert level
    //
    if (self->m_registeredNULL || self->m_errorSet.empty() == false)
    {
        DrLogA("Factory Registration Errors");
    }

    //
    // Find a factory with the provided name
    //
    FactoryMap::iterator factory = self->m_factories.find(name);
    if (factory != self->m_factories.end())
    {
        //
        // If found factory, return reference to it 
        //
        return factory->second;
    }
    else
    {
        //
        // If no factory found, return null
        //
        return NULL;
    }
}

//
// Check factory registry and print out any immediate errors and usage instructions
//
void VertexFactoryRegistry::ShowAllVertexUsageMessages(FILE* f)
{
    //
    // If factory registry not yet initialized, report failure and exit.
    // todo: ensure we want to fprintf here and not write to log
    //
    if (s_dryadVertexRegistry == 0)
    {
        fprintf(f, "Factory Registry usage called before initialization\n\n");
        return;
    }

    VertexFactoryRegistry* self = s_dryadVertexRegistry;

    //
    // If NULL name was provided to the registry on initialization, log the error
    //
    if (self->m_registeredNULL)
    {
        DrLogE("Factory Registered With illegal NULL name");
    }

    //
    // If there are any elements in m_errorSet, multiple factories of the same 
    // were attempted to be registered. Log this error.
    //
    if (self->m_errorSet.empty() == false)
    {
        DuplicateSet::iterator i;
        for (i = self->m_errorSet.begin(); i != self->m_errorSet.end(); ++i)
        {
            DrLogE("Duplicate Factory Registered. Factory name: %s", i->c_str());
        }
    }

    //
    // If any errors, report that they exist at the assert level
    //
    if (self->m_registeredNULL || self->m_errorSet.empty() == false)
    {
        DrLogA("Factory Registration Errors");
    }

    //
    // Foreach factory, make sure the argument count is 1 and print out any
    // usage information
    //
    FactoryMap::iterator factory;
    for (factory = self->m_factories.begin();
         factory != self->m_factories.end(); ++factory)
    {
        DryadVertexProgramRef program = factory->second->MakeUntyped();
        LogAssert(program->GetArgumentCount() == 1);
        program->Usage(f);
    }
}

//
// Get factory and have it create a vertex program
//
DrError VertexFactoryRegistry::MakeVertex(UInt32 vertexId,
                                          UInt32 vertexVersion,
                                          UInt32 numberOfInputChannels,
                                          UInt32 numberOfOutputChannels,
                                          UInt64* expectedLength,
                                          DryadVertexFactoryBase* factory,
                                          DryadMetaData* metaData,
                                          UInt32 maxInputChannels,
                                          UInt32 maxOutputChannels,
                                          UInt32 argumentCount,
                                          DrStr64* argumentList,
                                          UInt32 serializedBlockLength,
                                          const void* serializedBlock,
                                          DryadMetaDataRef* pErrorData,
                                          DryadVertexProgramRef* pProgram)
{
    DryadVertexProgramBase* program;
    //
    // If factory is not supplied, try to get it from factory registry.
    // If still unable, fail with error.
    // 
    if (factory == NULL)
    {
        //
        // If no arguments, return vertex initialization error
        //
        if (argumentCount == 0)
        {
            DryadMetaData::Create(pErrorData);
            (*pErrorData)->AddErrorWithDescription(DryadError_VertexInitialization,
                                                   "Factory Registry called with no arguments");
            return DryadError_VertexInitialization;
        }
		

        //
        // Get vertex factory. If one cannot be found, report initialization error
        //
        factory = LookUpFactory(argumentList[0]);
        if (factory == NULL)
        {
			DrLogW("Factory Registry called with unknown factory UID %s.", argumentList[0].GetString());
            DrStr128 errorString;
            errorString.SetF("Factory Registry called with unknown factory UID %s",
                             argumentList[0].GetString());
            DryadMetaData::Create(pErrorData);
            (*pErrorData)->AddErrorWithDescription(DryadError_VertexInitialization,
                                                   errorString);
            return DryadError_VertexInitialization;
        }
		
        //
        // report new vertex creation
        //
        DrLogI( "Factory making new vertex. Vertex %s with %u arguments %u inputs %u outputs",
            argumentList[0].GetString(), argumentCount,
            numberOfInputChannels, numberOfOutputChannels);

        //
        // Make a program and ensure that program argument is first argument in list
        // todo: figure out how first argument in program is specified/what it is
        //
        *pProgram = factory->MakeUntyped();
        program = *pProgram;
        LogAssert(program->GetArgumentCount() == 1);
        LogAssert(::strcmp(program->GetArgument(0), argumentList[0]) == 0);

        //
        // adjust the argument count and list to throw away the first
        // argument naming which vertex was to be created
        //
        ++argumentList;
        --argumentCount;
    }
    else
    {
        //
        // If a factory exists, then we have been told which factory to use and are 
        // not going to read it from the argument list 
        // 
        *pProgram = factory->MakeUntyped();
        program = *pProgram;
        LogAssert(program->GetArgumentCount() == 1);
    }

    //
    // Update program with vertext id and version
    //
    program->SetVertexId(vertexId);
    program->SetVertexVersion(vertexVersion);

    //
    // Let program know how long the input channels are
    //
    if (expectedLength != NULL)
    {
        program->SetExpectedInputLength(numberOfInputChannels, expectedLength);
    }

    //
    // Add remaining arguments to arg list
    //
    UInt32 i;
    for (i=0; i<argumentCount; ++i)
    {
        program->AddArgument(argumentList[i]);

        DrLogI( "Factory adding vertex argument. Vertex %s arguments %u=%s",
            program->GetArgument(0), i, argumentList[i].GetString());
    }

    //
    // Set max number of channels and other metadata
    //
    program->SetMaxOpenInputChannelCount(maxInputChannels);
    program->SetMaxOpenOutputChannelCount(maxOutputChannels);
    program->SetMetaData(metaData);

    //
    // Copy data into program
    //
    DrRef<DrMemoryBuffer> buffer;
    buffer.Attach(new DrSimpleHeapBuffer());
    buffer->Append(serializedBlock, serializedBlockLength);
    DrMemoryBufferReader reader(buffer);
    program->DeSerialize(&reader);

    if (program->GetErrorCode() == DrError_OK)
    {
        //
        // If still ok, initialize the program with the expected number of input and output channels
        // todo: this doesn't do anything in base type. Figure out if base type or derived type.
        //
        program->Initialize(numberOfInputChannels, numberOfOutputChannels);
    }

    //
    // Report any errors
    //
    *pErrorData = program->GetErrorMetaData();
    return program->GetErrorCode();
}
