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
#pragma warning(disable:4995)
#include <map>
#include <set>
#include <string>

class DryadVertexFactoryBase
{
public:
    //
    // Constructor/Destructor
    //
    DryadVertexFactoryBase(const char* name);
    virtual ~DryadVertexFactoryBase();

    //
    // Return the name of the factory
    //
    const char* GetName();

    //
    // The Register method does nothing, but can be used to pull in
    // static factories from other compilation units.
    //
    void Register();

    //
    // Can make new typed or untyped dryad vertex. Used differently depending on 
    // child implementation
    //
    DryadVertexProgramRef MakeUntyped();
    DryadVertexProgramBase* NewUntyped();

private:
    //
    // NewUntypedInternal is not defined in factory base
    //
    virtual DryadVertexProgramBase* NewUntypedInternal() = 0;

    //
    // Name of factory
    //
    DrStr64      m_name;
};

//
// Vertex factory using defined type
//
template<class _T> class TypedVertexFactory : public DryadVertexFactoryBase
{
public:
    typedef _T VertexClass;
    typedef DrRef<_T> Vertex;

    TypedVertexFactory(const char* name) : DryadVertexFactoryBase(name) {}
    virtual ~TypedVertexFactory() {}

    //
    // Create new vertex of the defined type with name of factory
    //
    VertexClass* New()
    {
        VertexClass* v = NewInternal();
        v->AddArgument(GetName());
        return v;
    }

    //
    // Make a DrRef to a new vertex of the defined type
    //
    Vertex Make()
    {
        Vertex v;
        v.Attach(New());
        return v;
    }

private:
    //
    // Create a vertex of the defined type
    //
    DryadVertexProgramBase* NewUntypedInternal()
    {
        return New();
    }

    //
    // Don't define NewInternal
    //
    virtual VertexClass* NewInternal() = 0;
};

//
// Standard implementation of a vertex factory
//
template<class _T> class StdTypedVertexFactory :
    public TypedVertexFactory<_T>
{
public:
    StdTypedVertexFactory(const char* name) : TypedVertexFactory<_T>(name) {}

private:
    //
    // Create new vertex of the defined type
    //
    VertexClass* NewInternal()
    {
        return new VertexClass();
    }
};

//
// Registry for vertex factories 
//
class VertexFactoryRegistry
{
public:
    VertexFactoryRegistry();

    static void ShowAllVertexUsageMessages(FILE* f);

    //
    // Get reference to vertex factory in registry. Returns null if DNE.
    //
    static DryadVertexFactoryBase* LookUpFactory(const char* name);

    //
    // Place a factory in the registry
    //
    static void RegisterFactory(DryadVertexFactoryBase* factory);
    
    //
    // Create a vertex
    //
    static DrError MakeVertex(UInt32 vertexId,
                              UInt32 vertexVersion,
                              UInt32 numberOfInputChannels,
                              UInt32 numberOfOutputChannels,
                              UInt64* expectedInputLength,
                              DryadVertexFactoryBase* factory,
                              DryadMetaData* metaData,
                              UInt32 maxInputChannels,
                              UInt32 maxOutputChannels,
                              UInt32 argumentCount,
                              DrStr64* argumentList,
                              UInt32 serializedBlockLength,
                              const void* serializedBlock,
                              DryadMetaDataRef* pErrorData,
                              DryadVertexProgramRef* pProgram);

private:
    typedef std::map< std::string, DryadVertexFactoryBase*,
                      std::less<std::string> > FactoryMap;
    typedef std::set< std::string, std::less<std::string> > DuplicateSet;

    FactoryMap    m_factories;
    DuplicateSet  m_errorSet;
    bool          m_registeredNULL;
};

extern DryadVertexFactoryBase* g_subgraphFactory;
