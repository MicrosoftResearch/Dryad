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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.CodeDom;
using System.CodeDom.Compiler;
using Microsoft.CSharp;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    using CodeStmtPair = Pair<CodeStatement[], CodeStatement[]>;

    /// <summary>
    /// This class generates and compiles the managed code executed by DryadLinq.
    /// It creates a managed library (DLL) that contains the entry method for each
    /// DryadLINQ vertex.
    ///
    /// For each type of DryadLINQ vertex node, we need to call
    ///    AddDryadCodeForType(node.OutputType);
    ///    AddVertexMethod(node);
    /// </summary>
    /// <remarks>A DryadLINQ user should not use this class directly.</remarks>
    public class DryadLinqCodeGen
    {
        private const BindingFlags FieldFlags = BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic;
        private const string TargetNamespace = "Microsoft.Research.DryadLinq";
        private const string ExtensionClassName = "DryadLinq__Extension";
        private const string VertexClassName = "DryadLinq__Vertex";
        private const string TargetDllName = "Microsoft.Research.DryadLinq_.dll";
        private const string VertexSourceFile = "Microsoft.Research.DryadLinq_.cs";
        private const string DummyExtensionSourceFile = "DryadLinqExtensionBase.cs";
        private const string VertexParamName = "vertexParams";
        private const string HelperClassName = "DryadLinqHelper";
        private const string DebugHelperMethodName = "CheckVertexDebugRequest";
        private const string CopyResourcesMethodName = "CopyResources";
        internal const string VertexEnvName = "denv";
        
        private static int s_uniqueId = -1;
        private static int s_DryadLinqDllVersion = -1;
        private static Dictionary<Type, string> s_TypeToInternalName;
        private static Dictionary<Type, string> s_BuiltinTypeToReaderName;
        private static Dictionary<Type, string> s_BuiltinTypeToSerializerName;
        private static Dictionary<Type, object> s_TypeToFactory;
        private static object s_codeGenLock = new Object();

        internal readonly static CodeExpression ZeroExpr = new CodePrimitiveExpression(0);
        internal readonly static CodeExpression OneExpr = new CodePrimitiveExpression(1);
        internal readonly static CodeExpression NullExpr = new CodePrimitiveExpression(null);
        internal readonly static CodeExpression DLVTypeExpr = new CodeTypeReferenceExpression("DryadLinqVertex");

        static DryadLinqCodeGen()
        {
            // Initialize the mapping from type to its internal name
            s_TypeToInternalName = new Dictionary<Type, string>(20);
            s_TypeToInternalName.Add(typeof(byte), "Byte");
            s_TypeToInternalName.Add(typeof(sbyte), "SByte");
            s_TypeToInternalName.Add(typeof(bool), "Bool");
            s_TypeToInternalName.Add(typeof(char), "Char");
            s_TypeToInternalName.Add(typeof(short), "Short");
            s_TypeToInternalName.Add(typeof(ushort), "UShort");
            s_TypeToInternalName.Add(typeof(int), "Int32");
            s_TypeToInternalName.Add(typeof(uint), "UInt32");
            s_TypeToInternalName.Add(typeof(long), "Int64");
            s_TypeToInternalName.Add(typeof(ulong), "UInt64");
            s_TypeToInternalName.Add(typeof(float), "Float");
            s_TypeToInternalName.Add(typeof(decimal), "Decimal");
            s_TypeToInternalName.Add(typeof(double), "Double");
            s_TypeToInternalName.Add(typeof(DateTime), "DateTime");
            s_TypeToInternalName.Add(typeof(string), "String");
            s_TypeToInternalName.Add(typeof(LineRecord), "LineRecord");
            s_TypeToInternalName.Add(typeof(SqlDateTime), "SqlDateTime");
            s_TypeToInternalName.Add(typeof(Guid), "Guid");

            // Initialize the mapping from builtin type to its read method name
            s_BuiltinTypeToReaderName = new Dictionary<Type, string>(20);
            s_BuiltinTypeToReaderName.Add(typeof(bool), "ReadBool");
            s_BuiltinTypeToReaderName.Add(typeof(char), "ReadChar");
            s_BuiltinTypeToReaderName.Add(typeof(sbyte),"ReadSByte");
            s_BuiltinTypeToReaderName.Add(typeof(byte), "ReadUByte");
            s_BuiltinTypeToReaderName.Add(typeof(short), "ReadInt16");
            s_BuiltinTypeToReaderName.Add(typeof(ushort), "ReadUInt16");
            s_BuiltinTypeToReaderName.Add(typeof(int), "ReadInt32");
            s_BuiltinTypeToReaderName.Add(typeof(uint), "ReadUInt32");
            s_BuiltinTypeToReaderName.Add(typeof(long), "ReadInt64");
            s_BuiltinTypeToReaderName.Add(typeof(ulong), "ReadUInt64");
            s_BuiltinTypeToReaderName.Add(typeof(float), "ReadSingle"); 
            s_BuiltinTypeToReaderName.Add(typeof(double), "ReadDouble");
            s_BuiltinTypeToReaderName.Add(typeof(decimal), "ReadDecimal");
            s_BuiltinTypeToReaderName.Add(typeof(DateTime), "ReadDateTime");
            s_BuiltinTypeToReaderName.Add(typeof(string), "ReadString");
            s_BuiltinTypeToReaderName.Add(typeof(SqlDateTime), "ReadSqlDateTime");
            s_BuiltinTypeToReaderName.Add(typeof(Guid), "ReadGuid");

            // Initialize the mapping from builtin type to its serializer class name
            s_BuiltinTypeToSerializerName = new Dictionary<Type, string>(20);
            s_BuiltinTypeToSerializerName.Add(typeof(byte), "ByteDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(sbyte), "SByteDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(bool), "BoolDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(char), "CharDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(short), "Int16DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(ushort), "UInt16DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(int), "Int32DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(uint), "UInt32DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(long), "Int64DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(ulong), "UInt64DryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(float), "SingleDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(double), "DoubleDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(decimal), "DecimalDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(DateTime), "DateTimeDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(string), "StringDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(Guid), "GuidDryadLinqSerializer");
            s_BuiltinTypeToSerializerName.Add(typeof(SqlDateTime), "SqlDateTimeDryadLinqSerializer");

            // Initialize the mapping from type to its factory
            s_TypeToFactory = new Dictionary<Type, object>(20);
            s_TypeToFactory.Add(typeof(byte), new DryadLinqFactoryByte());
            s_TypeToFactory.Add(typeof(sbyte), new DryadLinqFactorySByte());
            s_TypeToFactory.Add(typeof(bool), new DryadLinqFactoryBool());
            s_TypeToFactory.Add(typeof(char), new DryadLinqFactoryChar());
            s_TypeToFactory.Add(typeof(short), new DryadLinqFactoryShort());
            s_TypeToFactory.Add(typeof(ushort), new DryadLinqFactoryUShort());
            s_TypeToFactory.Add(typeof(int), new DryadLinqFactoryInt32());
            s_TypeToFactory.Add(typeof(uint), new DryadLinqFactoryUInt32());
            s_TypeToFactory.Add(typeof(long), new DryadLinqFactoryInt64());
            s_TypeToFactory.Add(typeof(ulong), new DryadLinqFactoryUInt64());
            s_TypeToFactory.Add(typeof(float), new DryadLinqFactoryFloat());
            s_TypeToFactory.Add(typeof(decimal), new DryadLinqFactoryDecimal());
            s_TypeToFactory.Add(typeof(double), new DryadLinqFactoryDouble());
            s_TypeToFactory.Add(typeof(DateTime), new DryadLinqFactoryDateTime());
            s_TypeToFactory.Add(typeof(string), new DryadLinqFactoryString());
            s_TypeToFactory.Add(typeof(LineRecord), new DryadLinqFactoryLineRecord());
            s_TypeToFactory.Add(typeof(SqlDateTime), new DryadLinqFactorySqlDateTime());
            s_TypeToFactory.Add(typeof(Guid), new DryadLinqFactoryGuid());
        }
                
        private string m_generatedVertexDllPath;    // only set if vertex code and assembly were both created successfully
        private Assembly m_loadedVertexAssembly;    // only set if the caller requests a load (the only case is GetFactory, which is used by enumeration)

        private CodeCompileUnit m_dryadLinqUnit;
        private CodeNamespace m_dryadCodeSpace;   
        private CodeTypeDeclaration m_dryadExtensionClass;
        private CodeTypeConstructor m_extensionStaticCtor;
        private CodeTypeDeclaration m_dryadVertexClass;
        private HashSet<Type> m_dryadDataTypes;
        private HashSet<Type> m_serializationDatatypes;
        private Dictionary<FieldInfo, string> m_fieldToStaticName;
        private HashSet<string> m_staticFieldDefined;
        private Dictionary<Type, string> m_typeToSerializerName;
        private Dictionary<Type, string> m_anonymousTypeToName;
        private Dictionary<string, string> m_nameToAlias;
        private DryadLinqContext m_context;
        private VertexCodeGen m_vertexCodeGen;

        internal DryadLinqCodeGen(DryadLinqContext context, VertexCodeGen vertexCodeGen)
        {
            this.m_context = context;
            this.m_vertexCodeGen = vertexCodeGen;
            this.m_loadedVertexAssembly = null;
            this.m_dryadLinqUnit = new CodeCompileUnit();

            // Create a namespace
            this.m_dryadCodeSpace = new CodeNamespace(TargetNamespace);
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Collections"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Text"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Linq.Expressions"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Diagnostics"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Runtime.Serialization"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Data.SqlTypes"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Data.Linq"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("System.Data.Linq.Mapping"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("Microsoft.Research.DryadLinq"));
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport("Microsoft.Research.DryadLinq.Internal"));

            this.m_dryadLinqUnit.Namespaces.Add(this.m_dryadCodeSpace);

            // Create the class for all the DryadLinq extension methods
            this.m_dryadExtensionClass = new CodeTypeDeclaration(ExtensionClassName);
            this.m_dryadExtensionClass.IsClass = true;
            this.m_dryadExtensionClass.IsPartial = true;
            this.m_dryadExtensionClass.TypeAttributes = TypeAttributes.Public;
            this.m_dryadCodeSpace.Types.Add(this.m_dryadExtensionClass);

            // Create the static constructor for the vertex extension class
            this.m_extensionStaticCtor = new CodeTypeConstructor();
            this.m_dryadExtensionClass.Members.Add(this.m_extensionStaticCtor);

            // Create the class for all the DryadLinq vertex methods
            this.m_dryadVertexClass = new CodeTypeDeclaration(VertexClassName);
            this.m_dryadVertexClass.IsClass = true;
            this.m_dryadVertexClass.TypeAttributes = TypeAttributes.Public | TypeAttributes.Sealed;
            this.m_dryadCodeSpace.Types.Add(this.m_dryadVertexClass);
            this.AddCopyResourcesMethod();

            // The set of input/output channel datatypes
            this.m_dryadDataTypes = new HashSet<Type>();
            this.m_dryadDataTypes.Add(typeof(byte));
            this.m_dryadDataTypes.Add(typeof(sbyte));
            this.m_dryadDataTypes.Add(typeof(bool));
            this.m_dryadDataTypes.Add(typeof(char));
            this.m_dryadDataTypes.Add(typeof(short));
            this.m_dryadDataTypes.Add(typeof(ushort));
            this.m_dryadDataTypes.Add(typeof(int));
            this.m_dryadDataTypes.Add(typeof(uint));
            this.m_dryadDataTypes.Add(typeof(long));
            this.m_dryadDataTypes.Add(typeof(ulong));
            this.m_dryadDataTypes.Add(typeof(float));
            this.m_dryadDataTypes.Add(typeof(decimal));
            this.m_dryadDataTypes.Add(typeof(double));
            this.m_dryadDataTypes.Add(typeof(DateTime));      
            this.m_dryadDataTypes.Add(typeof(string));
            this.m_dryadDataTypes.Add(typeof(LineRecord));
            this.m_dryadDataTypes.Add(typeof(SqlDateTime));
            this.m_dryadDataTypes.Add(typeof(Guid)); 
            
            // The set of datatypes we have added serialization methods
            this.m_serializationDatatypes = new HashSet<Type>();

            this.m_fieldToStaticName = new Dictionary<FieldInfo, string>();
            this.m_staticFieldDefined = new HashSet<string>();
            this.m_typeToSerializerName = new Dictionary<Type, string>();
            this.m_anonymousTypeToName = new Dictionary<Type, string>();
            this.m_nameToAlias = new Dictionary<string, string>();
        }

        private static string GetBuiltinReaderName(Type type)
        {
            string readerName = null;
            s_BuiltinTypeToReaderName.TryGetValue(type, out readerName);
            return readerName;
        }

        internal VertexCodeGen VertexCodeGen
        {
            get { return this.m_vertexCodeGen; }
            set { this.m_vertexCodeGen = value; }
        }

        internal static string VertexClassFullName
        {
            get { return TargetNamespace + "." + VertexClassName; }
        }
        
        internal static string MakeUniqueName(string name)
        {
            return name + "__" + Interlocked.Increment(ref s_uniqueId);
        }

        internal static string MakeName(Type type)
        {
            if (!s_TypeToInternalName.ContainsKey(type))
            {
                string name = MakeUniqueName("Type");
                s_TypeToInternalName.Add(type, name);
                return name;
            }
            return s_TypeToInternalName[type];
        }

        internal static string DryadReaderClassName(Type type)
        {
            return "DryadLinqRecordReader" + MakeName(type);
        }

        internal static string DryadWriterClassName(Type type)
        {
            return "DryadLinqRecordWriter" + MakeName(type);
        }

        internal static string DryadLinqFactoryClassName(Type type)
        {
            return "DryadLinqFactory" + MakeName(type);
        }

        internal static string AnonymousClassName(Type type)
        {
            return "Anonymous" + MakeName(type);
        }

        internal static string GetBuiltInDryadLinqSerializer(Type type)
        {
            if (s_BuiltinTypeToSerializerName.ContainsKey(type))
            {
                return s_BuiltinTypeToSerializerName[type];
            }
            return null;
        }

        internal Dictionary<Type, string> AnonymousTypeToName
        {
            get { return this.m_anonymousTypeToName; }
        }
                
        // Converts long type names into an alias in order to make vertex code more readable
        private string MakeTypeNameAlias(string fullProcessedTypeName)
        {
            // no change necessary if the full type name is short enough
            if (fullProcessedTypeName.Length <= 60)
            {
                return fullProcessedTypeName;
            }

            int aliasLen = fullProcessedTypeName.Length;
            while (aliasLen > 2 &&
                   fullProcessedTypeName[aliasLen - 1] == ']' && 
                   fullProcessedTypeName[aliasLen - 2] == '[')
            {
                aliasLen -= 2;
            }
            string typeNamePrefix = fullProcessedTypeName;
            if (aliasLen < fullProcessedTypeName.Length)
            {
                typeNamePrefix = fullProcessedTypeName.Substring(0, aliasLen);
            }
            string typeNameAlias;
            if (this.m_nameToAlias.ContainsKey(typeNamePrefix))
            {
                typeNameAlias = this.m_nameToAlias[typeNamePrefix];
            }
            else
            {
                typeNameAlias = MakeUniqueName("Alias");
                this.m_nameToAlias[typeNamePrefix] = typeNameAlias;
            }
            this.m_dryadCodeSpace.Imports.Add(new CodeNamespaceImport(typeNameAlias + " = " + typeNamePrefix));

            string newTypeName = typeNameAlias;
            if (aliasLen < fullProcessedTypeName.Length)
            {
                newTypeName += fullProcessedTypeName.Substring(aliasLen);
            }
            return typeNameAlias;
        }
        
        internal string GetStaticFactoryName(Type type)
        {
            string fieldName = "Factory" + MakeName(type);
            if (!this.m_staticFieldDefined.Contains(fieldName))
            {
                this.m_staticFieldDefined.Add(fieldName);
                string factoryName = DryadLinqFactoryClassName(type);
                CodeMemberField factoryField = new CodeMemberField(factoryName, fieldName);
                factoryField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
                factoryField.InitExpression = new CodeObjectCreateExpression(factoryName);
                this.m_dryadExtensionClass.Members.Add(factoryField);
            }
            return ExtensionClassName + "." + fieldName;
        }

        internal string GetStaticSerializerName(Type type)
        {
            string fieldName = "Serializer" + MakeName(type);
            if (!this.m_staticFieldDefined.Contains(fieldName))
            {
                this.m_staticFieldDefined.Add(fieldName);
                string serializerName = this.AddSerializerClass(type);
                CodeMemberField serializerField = new CodeMemberField(serializerName, fieldName);
                serializerField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
                serializerField.InitExpression = new CodeObjectCreateExpression(serializerName);
                this.m_dryadExtensionClass.Members.Add(serializerField);
            }
            return ExtensionClassName + "." + fieldName;
        }

        private string GetterFieldName(FieldInfo finfo)
        {
            this.AddFieldAccessDelegates(finfo);
            return "get_" + this.m_fieldToStaticName[finfo];
        }

        private string SetterFieldName(FieldInfo finfo)
        {
            this.AddFieldAccessDelegates(finfo);
            return "set_" + this.m_fieldToStaticName[finfo];
        }

        private void AddFieldAccessDelegates(FieldInfo finfo)
        {
            if (!this.m_fieldToStaticName.ContainsKey(finfo))
            {
                string fieldName = DryadLinqUtil.MakeValidId(TypeSystem.FieldName(finfo.Name));
                this.m_fieldToStaticName[finfo] = MakeUniqueName(fieldName);
                CodeTypeReferenceExpression typeExpr = new CodeTypeReferenceExpression("CodeGenHelper");
                CodeTypeReference cRef = new CodeTypeReference(finfo.DeclaringType);
                CodeTypeReference fRef = new CodeTypeReference(finfo.FieldType);
                string getterName, setterName;
                Type getterType, setterType;

                if (finfo.DeclaringType.IsValueType)
                {
                    getterName = "GetStructField";
                    setterName = "SetStructField";
                    getterType = typeof(GetStructFieldDelegate<,>);
                    setterType = typeof(SetStructFieldDelegate<,>);
                }
                else
                {
                    getterName = "GetObjField";
                    setterName = "SetObjField";
                    getterType = typeof(GetObjFieldDelegate<,>);
                    setterType = typeof(SetObjFieldDelegate<,>);
                }
                getterType = getterType.MakeGenericType(finfo.DeclaringType, finfo.FieldType);
                setterType = setterType.MakeGenericType(finfo.DeclaringType, finfo.FieldType);
                
                CodeMemberField getField = new CodeMemberField(getterType, this.GetterFieldName(finfo));
                getField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
                getField.InitExpression = new CodeMethodInvokeExpression(
                                                  new CodeMethodReferenceExpression(typeExpr, getterName, cRef, fRef),
                                                  new CodePrimitiveExpression(finfo.Name));
                this.m_dryadExtensionClass.Members.Add(getField);
                
                CodeMemberField setField = new CodeMemberField(setterType, this.SetterFieldName(finfo));
                setField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
                setField.InitExpression = new CodeMethodInvokeExpression(
                                                  new CodeMethodReferenceExpression(typeExpr, setterName, cRef, fRef),
                                                  new CodePrimitiveExpression(finfo.Name));
                this.m_dryadExtensionClass.Members.Add(setField);
            }
        }

        // Copy user resources to the vertex working directory
        private void AddCopyResourcesMethod()
        {
            CodeMemberMethod copyResourcesMethod = new CodeMemberMethod();
            copyResourcesMethod.Name = CopyResourcesMethodName;
            copyResourcesMethod.Attributes = MemberAttributes.Public | MemberAttributes.Static;

            IEnumerable<string> resourcesToExclude = this.m_context.ResourcesToRemove;
            foreach (string res in this.m_context.ResourcesToAdd)
            {
                if (!resourcesToExclude.Contains(res))
                {
                    string fname = Path.GetFileName(res);
                    string stmt = @"System.IO.File.Copy(@""" + Path.Combine("..", fname) + "\", @\"" + fname + "\")";
                    CodeExpression stmtExpr = new CodeSnippetExpression(stmt);
                    copyResourcesMethod.Statements.Add(new CodeExpressionStatement(stmtExpr));
                }
            }
            this.m_dryadVertexClass.Members.Add(copyResourcesMethod);
        }

        internal string AddDecompositionInitializer(Type decomposerType, Expression stateExpr)
        {
            string decomposerTypeName = TypeSystem.TypeName(decomposerType);
            string decomposerFieldName = MakeUniqueName("decomposer");
            CodeMemberField decomposerField = new CodeMemberField(decomposerTypeName, decomposerFieldName);
            decomposerField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
            this.m_dryadExtensionClass.Members.Add(decomposerField);

            CodeStatement initStmt1 = new CodeAssignStatement(
                                              new CodeVariableReferenceExpression(decomposerFieldName),
                                              new CodeObjectCreateExpression(decomposerTypeName));
            this.m_extensionStaticCtor.Statements.Add(initStmt1);
            
            MethodInfo initInfo = decomposerType.GetMethod("Initialize");
            ParameterExpression decomposer = Expression.Parameter(decomposerType, decomposerFieldName);
            Expression initExpr = Expression.Call(decomposer, initInfo, stateExpr);
            CodeStatement initStmt2 = new CodeExpressionStatement(this.MakeExpression(initExpr));
            this.m_extensionStaticCtor.Statements.Add(initStmt2);

            return ExtensionClassName + "." + decomposerFieldName;
        }

        internal void AddDryadCodeForType(Type type)
        {
            if (!this.m_dryadDataTypes.Contains(type))
            {
                this.m_dryadDataTypes.Add(type);
                this.AddAnonymousClass(type);
                this.AddSerializerClass(type);
                this.AddReaderClass(type);
                this.AddWriterClass(type);
                this.AddFactoryClass(type);
            }
        }

        // Add a new DryadLinqRecordReader subclass for a type
        internal void AddReaderClass(Type type)
        {
            Type baseClass = typeof(DryadLinqRecordBinaryReader<>).MakeGenericType(type);
            string baseClassName = TypeSystem.TypeName(baseClass, this.AnonymousTypeToName);
            string className = DryadReaderClassName(type);
            CodeTypeDeclaration readerClass = new CodeTypeDeclaration(className + " : " + baseClassName);
            this.m_dryadCodeSpace.Types.Add(readerClass);
            readerClass.IsClass = true;
            readerClass.TypeAttributes = TypeAttributes.Public | TypeAttributes.Sealed;

            // Add constructors:
            string conString = "        public " + className + "(DryadLinqBinaryReader reader) : base(reader) { }";
            CodeTypeMember con = new CodeSnippetTypeMember(conString);
            readerClass.Members.Add(con);

            // Add method ReadRecord:
            string serializerName = GetStaticSerializerName(type);
            string typeName = TypeSystem.TypeName(type, this.AnonymousTypeToName);
            StringBuilder methodBuilder = new StringBuilder();
            methodBuilder.AppendLine("        protected override bool ReadRecord(ref " + typeName + " rec)");
            methodBuilder.AppendLine("        {");
            methodBuilder.AppendLine("            if (!this.IsReaderAtEndOfStream())");
            methodBuilder.AppendLine("            {");
            if (AttributeSystem.RecordCanBeNull(this.m_context, type))
            {
                methodBuilder.AppendLine("                if (!this.m_reader.ReadBool())");
                methodBuilder.AppendLine("                {");
                methodBuilder.AppendLine("                    rec = " + serializerName + ".Read(this.m_reader);");
                methodBuilder.AppendLine("                }");                
            }
            else
            {
                methodBuilder.AppendLine("                rec = " + serializerName + ".Read(this.m_reader);");
            }
            methodBuilder.AppendLine("                return true;");
            methodBuilder.AppendLine("            }");
            methodBuilder.AppendLine("            return false;");
            methodBuilder.AppendLine("        }");
            CodeTypeMember readRecordMethod = new CodeSnippetTypeMember(methodBuilder.ToString());
            readerClass.Members.Add(readRecordMethod);
        }

        // Add a new DryadLinqRecordWriter subclass for a type
        internal void AddWriterClass(Type type)
        {
            Type baseClass = typeof(DryadLinqRecordBinaryWriter<>).MakeGenericType(type);
            string baseClassName = TypeSystem.TypeName(baseClass, this.AnonymousTypeToName);
            string className = DryadWriterClassName(type);
            CodeTypeDeclaration writerClass = new CodeTypeDeclaration(className + " : " + baseClassName);
            this.m_dryadCodeSpace.Types.Add(writerClass);
            writerClass.IsClass = true;
            writerClass.TypeAttributes = TypeAttributes.Public | TypeAttributes.Sealed;
            
            // Add constructors:
            string conString = "        public " + className + "(DryadLinqBinaryWriter writer) : base(writer) { }";
            CodeTypeMember con = new CodeSnippetTypeMember(conString);
            writerClass.Members.Add(con);

            // Add method WriteRecord:
            string serializerName = GetStaticSerializerName(type);
            string typeName = TypeSystem.TypeName(type, this.AnonymousTypeToName);
            StringBuilder methodBuilder = new StringBuilder();
            methodBuilder.AppendLine("        protected override void WriteRecord(" + typeName + " rec)");
            methodBuilder.AppendLine("        {");
            if (AttributeSystem.RecordCanBeNull(m_context, type))
            {
                methodBuilder.AppendLine("            bool isNull = Object.ReferenceEquals(rec, null);");
                methodBuilder.AppendLine("            this.m_writer.Write(isNull);");
                methodBuilder.AppendLine("            if (!isNull)");
                methodBuilder.AppendLine("            {");
                methodBuilder.AppendLine("            " + serializerName + ".Write(this.m_writer, rec);");                
                methodBuilder.AppendLine("            }");
            }
            else
            {
                methodBuilder.AppendLine("            " + serializerName + ".Write(this.m_writer, rec);");
            }
            methodBuilder.AppendLine("            this.CompleteWriteRecord();");
            methodBuilder.AppendLine("        }");
            CodeTypeMember writeRecordMethod = new CodeSnippetTypeMember(methodBuilder.ToString());
            writerClass.Members.Add(writeRecordMethod);
        }

        // Add a new DryadLinqFactory subclass for a type
        internal void AddFactoryClass(Type type)
        {
            Type baseClass = typeof(DryadLinqFactory<>).MakeGenericType(type);
            string baseClassName = TypeSystem.TypeName(baseClass, this.AnonymousTypeToName);
            CodeTypeDeclaration factoryClass = new CodeTypeDeclaration(DryadLinqFactoryClassName(type) + " : " + baseClassName);
            this.m_dryadCodeSpace.Types.Add(factoryClass);
            factoryClass.IsClass = true;
            factoryClass.TypeAttributes = TypeAttributes.Public;

            // Add method MakeReader(IntPtr handle, UInt32 port):
            Type returnType = typeof(DryadLinqRecordReader<>).MakeGenericType(type);
            string returnTypeName = TypeSystem.TypeName(returnType, this.AnonymousTypeToName);
            StringBuilder mb1 = new StringBuilder();
            mb1.AppendLine("        public override " + returnTypeName + " MakeReader(System.IntPtr handle, uint port)");
            mb1.AppendLine("        {");
            mb1.AppendLine("            return new " + DryadReaderClassName(type) +
                           "(Microsoft.Research.DryadLinq.Internal.VertexEnv.MakeBinaryReader(handle, port));");
            mb1.AppendLine("        }");
            CodeTypeMember readerMethod1 = new CodeSnippetTypeMember(mb1.ToString());
            factoryClass.Members.Add(readerMethod1);

            // Add method MakeReader(NativeBlockStream stream):
            StringBuilder mb3 = new StringBuilder();
            mb3.AppendLine("        public override " + returnTypeName +
                           " MakeReader(Microsoft.Research.DryadLinq.Internal.NativeBlockStream stream)");
            mb3.AppendLine("        {");
            mb3.AppendLine("            return new " + DryadReaderClassName(type) +
                           "(Microsoft.Research.DryadLinq.Internal.VertexEnv.MakeBinaryReader(stream));");
            mb3.AppendLine("        }");
            CodeTypeMember readerMethod3 = new CodeSnippetTypeMember(mb3.ToString());
            factoryClass.Members.Add(readerMethod3);
            
            // Add method MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize):
            returnType = typeof(DryadLinqRecordWriter<>).MakeGenericType(type);
            returnTypeName = TypeSystem.TypeName(returnType, this.AnonymousTypeToName);
            StringBuilder mb4 = new StringBuilder();
            mb4.AppendLine("        public override " + returnTypeName + " MakeWriter(System.IntPtr handle, uint port, int buffSize)");
            mb4.AppendLine("        {");
            mb4.AppendLine("            return new " + DryadWriterClassName(type) +
                           "(Microsoft.Research.DryadLinq.Internal.VertexEnv.MakeBinaryWriter(handle, port, buffSize));");
            mb4.AppendLine("        }");
            CodeTypeMember writerMethod1 = new CodeSnippetTypeMember(mb4.ToString());
            factoryClass.Members.Add(writerMethod1);
            
            // Add method MakeWriter(NativeBlockStream stream):
            StringBuilder mb6 = new StringBuilder();
            mb6.AppendLine("        public override " + returnTypeName + " MakeWriter(Microsoft.Research.DryadLinq.Internal.NativeBlockStream stream)");
            mb6.AppendLine("        {");
            mb6.AppendLine("            return new " + DryadWriterClassName(type) +
                           "(Microsoft.Research.DryadLinq.Internal.VertexEnv.MakeBinaryWriter(stream));");
            mb6.AppendLine("        }");
            CodeTypeMember writerMethod3 = new CodeSnippetTypeMember(mb6.ToString());
            factoryClass.Members.Add(writerMethod3);
        }

        // Add an anonymous class
        internal bool AddAnonymousClass(Type type)
        {
            if (!TypeSystem.IsAnonymousType(type)) return false;
            if (this.m_anonymousTypeToName.ContainsKey(type)) return true;

            string className = AnonymousClassName(type);
            this.m_anonymousTypeToName.Add(type, className);

            CodeTypeDeclaration anonymousClass = new CodeTypeDeclaration(className);
            anonymousClass.IsClass = true;
            anonymousClass.TypeAttributes = TypeAttributes.Public;

            // Add the fields, the constructor, and properties:
            CodeConstructor con = new CodeConstructor();
            con.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            PropertyInfo[] props = type.GetProperties();
            System.Array.Sort(props, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));
            string[] fieldNames = new string[props.Length];
            for (int i = 0; i < props.Length; i++)
            {
                fieldNames[i] = "_" + props[i].Name;
                CodeParameterDeclarationExpression paramExpr;
                CodeMemberField memberField;
                if (this.AddAnonymousClass(props[i].PropertyType))
                {
                    string typeName = this.AnonymousTypeToName[props[i].PropertyType];
                    memberField = new CodeMemberField(typeName, fieldNames[i]);
                    paramExpr = new CodeParameterDeclarationExpression(typeName, props[i].Name);
                }
                else
                {
                    memberField = new CodeMemberField(props[i].PropertyType, fieldNames[i]);
                    paramExpr = new CodeParameterDeclarationExpression(props[i].PropertyType, props[i].Name);
                }
                memberField.Attributes = MemberAttributes.Public;
                anonymousClass.Members.Add(memberField);
                con.Parameters.Add(paramExpr);
                CodeExpression fieldExpr = new CodeFieldReferenceExpression(
                                                   new CodeThisReferenceExpression(), fieldNames[i]);
                con.Statements.Add(new CodeAssignStatement(
                                           fieldExpr, new CodeVariableReferenceExpression(paramExpr.Name)));

                CodeMemberProperty p = new CodeMemberProperty();
                p.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                p.Name = props[i].Name;
                p.Type = paramExpr.Type;
                p.GetStatements.Add(new CodeMethodReturnStatement(fieldExpr));
                anonymousClass.Members.Add(p);
            }
            anonymousClass.Members.Add(con);

            // Add Equals method:
            CodeMemberMethod equalsMethod = new CodeMemberMethod();
            equalsMethod.Attributes = MemberAttributes.Public | MemberAttributes.Override;
            equalsMethod.Name = "Equals";
            equalsMethod.Parameters.Add(new CodeParameterDeclarationExpression("Object", "obj"));
            equalsMethod.ReturnType = new CodeTypeReference(typeof(bool));

            CodeExpression initExpr = new CodeSnippetExpression("obj as " + className);
            equalsMethod.Statements.Add(
                  new CodeVariableDeclarationStatement(className, "myObj", initExpr));
            CodeStatement ifStmt = new CodeConditionStatement(
                                           new CodeSnippetExpression("myObj == null"),
                                           new CodeMethodReturnStatement(new CodePrimitiveExpression(false)));
            equalsMethod.Statements.Add(ifStmt);
            string equalsCode = "";
            for (int i = 0; i < props.Length; i++)
            {
                string fieldTypeName;
                // we must use the proxy-type for anonymous-types.
                if (m_anonymousTypeToName.ContainsKey(props[i].PropertyType))
                {
                    fieldTypeName = m_anonymousTypeToName[props[i].PropertyType];
                }
                else
                {
                    fieldTypeName = TypeSystem.TypeName(props[i].PropertyType);
                }

                if (i > 0) equalsCode += " && ";
                equalsCode += String.Format("EqualityComparer<{0}>.Default.Equals(this.{1}, myObj.{1})",
                                            fieldTypeName, props[i].Name);
            }
            CodeExpression returnExpr = new CodeSnippetExpression(equalsCode);
            equalsMethod.Statements.Add(new CodeMethodReturnStatement(returnExpr));
            anonymousClass.Members.Add(equalsMethod);

            // Add GetHashCode method:
            CodeMemberMethod getHashCodeMethod = new CodeMemberMethod();
            getHashCodeMethod.Attributes = MemberAttributes.Public | MemberAttributes.Override;
            getHashCodeMethod.Name = "GetHashCode";
            getHashCodeMethod.ReturnType = new CodeTypeReference(typeof(int));

            CodeVariableDeclarationStatement
                hashDecl = new CodeVariableDeclarationStatement(typeof(int), "num", ZeroExpr);
            getHashCodeMethod.Statements.Add(hashDecl);

            CodeExpression numExpr = new CodeArgumentReferenceExpression(hashDecl.Name);
            for (int i = 0; i < props.Length; i++)
            {
                if (props[i].PropertyType.IsValueType)
                {
                    CodeExpression hashExpr = new CodeSnippetExpression(
                                                      "(-1521134295 * num) + this." + props[i].Name + ".GetHashCode()");
                    getHashCodeMethod.Statements.Add(new CodeAssignStatement(numExpr, hashExpr));
                }
                else
                {
                    CodeExpression hashExpr = new CodeSnippetExpression(
                                                      String.Format("(-1521134295 * num) + (this.{0} != null ? this.{0}.GetHashCode() : 0)",
                                                                    props[i].Name));
                    getHashCodeMethod.Statements.Add(new CodeAssignStatement(numExpr, hashExpr));
                }
            }
            getHashCodeMethod.Statements.Add(new CodeMethodReturnStatement(numExpr));
            anonymousClass.Members.Add(getHashCodeMethod);

            // Add ToString method:
            CodeMemberMethod toStringMethod = new CodeMemberMethod();
            toStringMethod.Attributes = MemberAttributes.Public | MemberAttributes.Override;
            toStringMethod.Name = "ToString";
            toStringMethod.ReturnType = new CodeTypeReference(typeof(string));
            StringBuilder toStringCode = new StringBuilder();
            toStringCode.Append("\"{ \"");
            for (int i = 0; i < props.Length; i++)
            {
                if (i > 0) toStringCode.Append(" + \", \"");
                toStringCode.Append(" + \"");
                toStringCode.Append(props[i].Name);
                toStringCode.Append(" = \" + this.");
                toStringCode.Append(props[i].Name);
                toStringCode.Append(".ToString()");
            }
            toStringCode.Append(" + \" }\"");
            returnExpr = new CodeSnippetExpression(toStringCode.ToString());
            toStringMethod.Statements.Add(new CodeMethodReturnStatement(returnExpr));
            anonymousClass.Members.Add(toStringMethod);

            this.m_dryadCodeSpace.Types.Add(anonymousClass);
            return true;
        }
        
        private CodeStatement[] MakeReadMethodBody(Type type)
        {
            List<CodeStatement> statements = new List<CodeStatement>();
            CodeExpression objExpr = new CodeArgumentReferenceExpression("obj");
            if (type.IsArray)
            {
                if (type.GetElementType() == typeof(object))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CannotHandleObjectFields,
                                                 String.Format(SR.CannotHandleObjectFields, type.FullName));
                }

                // Generate obj = new MyType[reader.ReadInt32()]
                int rank = type.GetArrayRank();
                Type baseType = type.GetElementType();
                while (baseType.IsArray)
                {
                    baseType = baseType.GetElementType();
                }
                string[] lenNames = new string[rank];
                for (int i = 0; i < rank; i++)
                {
                    lenNames[i] = MakeUniqueName("len");
                    CodeExpression lenExpr = new CodeSnippetExpression("reader.ReadInt32()");
                    var lenStmt = new CodeVariableDeclarationStatement(typeof(int), lenNames[i], lenExpr);
                    statements.Add(lenStmt);
                }
                string newCallString = "new " + TypeSystem.TypeName(baseType, this.AnonymousTypeToName);
                newCallString += "[";
                for (int i = 0; i < rank; i++)
                {
                    if (i != 0)
                    {
                        newCallString += ",";
                    }
                    newCallString += lenNames[i];
                }
                newCallString += "]";

                Type elemType = type.GetElementType();
                while (elemType.IsArray)
                {
                    int elemRank = elemType.GetArrayRank();
                    newCallString += "[";
                    for (int i = 1; i < elemRank; i++)
                    {
                        newCallString += ',';
                    }
                    newCallString += "]";
                    elemType = elemType.GetElementType();
                }
                CodeExpression newCall = new CodeSnippetExpression(newCallString);
                statements.Add(new CodeVariableDeclarationStatement(type, "obj", newCall));

                // Generate reading code
                if (type.GetElementType().IsPrimitive)
                {
                    // Use a single ReadRawBytes for primitive array
                    string lenStr = "sizeof(" + type.GetElementType() + ")";
                    for (int i = 0; i < rank; i++)
                    {
                        lenStr += "*obj.GetLength(" + i + ")";
                    }
                    string readBytes = "            unsafe { fixed (void *p = obj) reader.ReadRawBytes((byte*)p, " + lenStr + "); }";
                    statements.Add(new CodeSnippetStatement(readBytes));
                }
                else
                {
                    if (StaticConfig.AllowNullArrayElements && !type.GetElementType().IsValueType)
                    {
                        CodeExpression bvReadExpr = new CodeSnippetExpression("BitVector.Read(reader)");
                        CodeStatement stmt = new CodeVariableDeclarationStatement(typeof(BitVector), "bv", bvReadExpr);
                        statements.Add(stmt);
                    }

                    CodeVariableReferenceExpression[] indexExprs = new CodeVariableReferenceExpression[lenNames.Length];
                    for (int i = 0; i < lenNames.Length; i++)
                    {
                        indexExprs[i] = new CodeVariableReferenceExpression("i" + i);
                    }
                    CodeStatement[] readStmts = this.MakeReadFieldStatements(type.GetElementType(), objExpr, null, indexExprs);
                    for (int i = lenNames.Length - 1; i >= 0; i--)
                    {
                        CodeVariableDeclarationStatement
                            initStmt = new CodeVariableDeclarationStatement(
                                               typeof(int), indexExprs[i].VariableName, ZeroExpr);
                        CodeExpression
                            testExpr = new CodeBinaryOperatorExpression(indexExprs[i],
                                                                        CodeBinaryOperatorType.LessThan,
                                                                        new CodeVariableReferenceExpression(lenNames[i]));
                        CodeStatement
                            incStmt = new CodeAssignStatement(
                                              indexExprs[i],
                                              new CodeBinaryOperatorExpression(indexExprs[i],
                                                                               CodeBinaryOperatorType.Add,
                                                                               OneExpr));
                        readStmts = new CodeStatement[] { new CodeIterationStatement(initStmt, testExpr, incStmt, readStmts) };
                    }
                    statements.AddRange(readStmts);
                }
            }
            else
            {
                CodeExpression newObjectCall;
                if (type.IsValueType)
                {
                    // default(type)
                    newObjectCall = new CodeObjectCreateExpression(type);
                }
                else
                {
                    // FormatterServices.GetUninitializedObject(type)
                    newObjectCall = new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("FormatterServices"),
                                                                   "GetUninitializedObject",
                                                                   new CodeTypeOfExpression(type));
                    newObjectCall = new CodeCastExpression(type, newObjectCall);
                }
                statements.Add(new CodeVariableDeclarationStatement(type, "obj", newObjectCall));

                // For each field of type, generate its deserialization code.
                FieldInfo[] fields = TypeSystem.GetAllFields(type);
                System.Array.Sort(fields, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));

                bool canBeNull = fields.Any(x => !x.FieldType.IsValueType && AttributeSystem.FieldCanBeNull(x));
                if (canBeNull)
                {
                    CodeExpression bvReadExpr = new CodeSnippetExpression("BitVector.Read(reader)");
                    CodeStatement stmt = new CodeVariableDeclarationStatement(typeof(BitVector), "bv", bvReadExpr);
                    statements.Add(stmt);
                }
                for (int i = 0; i < fields.Length; i++)
                {
                    FieldInfo finfo = fields[i];
                    if (TypeSystem.IsFieldSerialized(finfo))
                    {
                        if (finfo.FieldType == typeof(object))
                        {
                            throw new DryadLinqException(DryadLinqErrorCode.CannotHandleObjectFields,
                                                         String.Format(SR.CannotHandleObjectFields, type.FullName));
                        }

                        CodeVariableReferenceExpression[]
                            indexExprs = new CodeVariableReferenceExpression[] { new CodeVariableReferenceExpression(i.ToString()) };
                        CodeStatement[] stmts = this.MakeReadFieldStatements(finfo.FieldType, objExpr, finfo, indexExprs);
                        statements.AddRange(stmts);
                    }
                }
            }
            statements.Add(new CodeMethodReturnStatement(objExpr));
            return statements.ToArray();
        }

        private CodeStatement[] MakeWriteMethodBody(Type type)
        {
            List<CodeStatement> statements = new List<CodeStatement>();
            CodeExpression objExpr = new CodeArgumentReferenceExpression("obj");
            CodeExpression writerExpr = new CodeArgumentReferenceExpression("writer");

            if (type.IsArray)
            {
                if (type.GetElementType() == typeof(object))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CannotHandleObjectFields,
                                                 String.Format(SR.CannotHandleObjectFields, type.FullName));
                }

                int rank = type.GetArrayRank();
                for (int i = 0; i < rank; i++)
                {
                    CodeExpression lenExpr = new CodeMethodInvokeExpression(objExpr, "GetLength", new CodePrimitiveExpression(i));
                    CodeExpression lenCall = new CodeMethodInvokeExpression(writerExpr, "Write", lenExpr);
                    statements.Add(new CodeExpressionStatement(lenCall));
                }

                // Generate the writing code
                if (type.GetElementType().IsPrimitive)
                {
                    // Use a single WriteRawBytes for primitive array
                    string lenStr = "sizeof(" + type.GetElementType() + ")";
                    for (int i = 0; i < rank; i++)
                    {
                        lenStr += "*obj.GetLength(" + i + ")";
                    }
                    string writeBytes = "            unsafe { fixed (void *p = obj) writer.WriteRawBytes((byte*)p, " + lenStr + "); }";
                    statements.Add(new CodeSnippetStatement(writeBytes));
                }
                else
                {
                    CodeVariableReferenceExpression[] indexExprs = new CodeVariableReferenceExpression[rank];
                    for (int i = 0; i < rank; i++)
                    {
                        indexExprs[i] = new CodeVariableReferenceExpression("i" + i);
                    }
                    bool canBeNull = StaticConfig.AllowNullArrayElements && !type.GetElementType().IsValueType;
                    if (canBeNull)
                    {
                        string lenString = "obj.GetLength(0)";
                        for (int i = 1; i < rank; i++)
                        {
                            lenString += "*obj.GetLength(" + i + ")";
                        }
                        CodeExpression lenExpr = new CodeSnippetExpression(lenString);
                        CodeExpression bvExpr = new CodeObjectCreateExpression(typeof(BitVector), lenExpr);
                        CodeStatement bvStmt = new CodeVariableDeclarationStatement("BitVector", "bv", bvExpr);
                        statements.Add(bvStmt);
                    }
                    CodeStmtPair pair = this.MakeWriteFieldStatements(type.GetElementType(), objExpr, null, indexExprs);

                    CodeStatement[] writeStmts = pair.Key;
                    if (writeStmts != null)
                    {
                        for (int i = rank - 1; i >= 0; i--)
                        {
                            CodeVariableDeclarationStatement
                                initStmt = new CodeVariableDeclarationStatement(
                                                   typeof(int), indexExprs[i].VariableName, ZeroExpr);
                            CodeExpression lenExpr = new CodeMethodInvokeExpression(
                                                             objExpr, "GetLength", new CodePrimitiveExpression(i));
                            CodeExpression testExpr = new CodeBinaryOperatorExpression(
                                                              indexExprs[i],
                                                              CodeBinaryOperatorType.LessThan,
                                                              lenExpr);
                            CodeStatement incStmt = new CodeAssignStatement(
                                                            indexExprs[i],
                                                            new CodeBinaryOperatorExpression(
                                                                    indexExprs[i], CodeBinaryOperatorType.Add, OneExpr));
                            writeStmts = new CodeStatement[] { new CodeIterationStatement(initStmt, testExpr, incStmt, writeStmts) };
                        }
                        statements.AddRange(writeStmts);
                    }

                    if (canBeNull)
                    {
                        CodeExpression bvWriteExpr = new CodeSnippetExpression("BitVector.Write(writer, bv)");
                        statements.Add(new CodeExpressionStatement(bvWriteExpr));
                    }

                    writeStmts = pair.Value;
                    for (int i = rank - 1; i >= 0; i--)
                    {
                        CodeVariableDeclarationStatement
                            initStmt = new CodeVariableDeclarationStatement(
                                               typeof(int), indexExprs[i].VariableName, ZeroExpr);
                        CodeExpression lenExpr = new CodeMethodInvokeExpression(
                                                         objExpr, "GetLength", new CodePrimitiveExpression(i));
                        CodeExpression testExpr = new CodeBinaryOperatorExpression(
                                                          indexExprs[i], CodeBinaryOperatorType.LessThan, lenExpr);
                        CodeStatement incStmt = new CodeAssignStatement(
                                                        indexExprs[i],
                                                        new CodeBinaryOperatorExpression(
                                                                indexExprs[i], CodeBinaryOperatorType.Add, OneExpr));
                        writeStmts = new CodeStatement[] { new CodeIterationStatement(initStmt, testExpr, incStmt, writeStmts) };
                    }
                    statements.AddRange(writeStmts);
                }
            }
            else
            {
                FieldInfo[] fields = TypeSystem.GetAllFields(type);
                System.Array.Sort(fields, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));

                bool canBeNull = fields.Any(x => !x.FieldType.IsValueType && AttributeSystem.FieldCanBeNull(x));
                if (canBeNull)
                {
                    CodeExpression lenExpr = new CodePrimitiveExpression(fields.Length);
                    CodeExpression bvExpr = new CodeObjectCreateExpression(typeof(BitVector), lenExpr);
                    CodeStatement bvStmt = new CodeVariableDeclarationStatement("BitVector", "bv", bvExpr);
                    statements.Add(bvStmt);
                }
                    
                // For each field of type, generate its serialization code
                CodeStatement[][] stmtArray = new CodeStatement[fields.Length][];
                for (int i = 0; i < fields.Length; i++)
                {
                    FieldInfo finfo = fields[i];
                    if (TypeSystem.IsFieldSerialized(finfo))
                    {
                        if (finfo.FieldType == typeof(object))
                        {
                            throw new DryadLinqException(DryadLinqErrorCode.CannotHandleObjectFields,
                                                         String.Format(SR.CannotHandleObjectFields, type.FullName));
                        }

                        CodeVariableReferenceExpression[]
                            indexExprs = new CodeVariableReferenceExpression[] { new CodeVariableReferenceExpression(i.ToString()) };
                        CodeStmtPair pair = this.MakeWriteFieldStatements(finfo.FieldType, objExpr, finfo, indexExprs);
                        stmtArray[i] = pair.Value;
                        if (pair.Key != null)
                        {
                            statements.AddRange(pair.Key);
                        }
                    }
                }
                if (canBeNull)
                {
                    CodeExpression bvWriteExpr = new CodeSnippetExpression("BitVector.Write(writer, bv)");
                    statements.Add(new CodeExpressionStatement(bvWriteExpr));
                }
                for (int i = 0; i < stmtArray.Length; i++)
                {
                    if (stmtArray[i] != null)
                    {
                        statements.AddRange(stmtArray[i]);
                    }
                }
            }

            return statements.ToArray();
        }
        
        private CodeStatement[]
            MakeReadFieldStatements(Type type,
                                    CodeExpression objExpr,
                                    FieldInfo finfo,
                                    CodeVariableReferenceExpression[] indexExprs)
        {
            CodeStatement[] stmts;
            CodeExpression readerExpr = new CodeArgumentReferenceExpression("reader");
            string readerName = GetBuiltinReaderName(type);
            if (readerName == null)
            {
                // For non-builtin types
                string serializerName = GetStaticSerializerName(type);
                CodeVariableReferenceExpression serializerExpr = new CodeVariableReferenceExpression(serializerName);                
                CodeVariableDeclarationStatement tempDecl = null;
                CodeExpression setterExpr = null;

                CodeExpression fieldExpr;
                if (finfo == null)
                {
                    fieldExpr = new CodeArrayIndexerExpression(objExpr, indexExprs);
                }
                else if (finfo.IsPublic && !finfo.IsInitOnly)
                {
                    fieldExpr = new CodeFieldReferenceExpression(objExpr, finfo.Name);
                }
                else
                {
                    string fieldName = TypeSystem.FieldName(finfo.Name);
                    if (!TypeSystem.IsBackingField(finfo.Name) ||
                        finfo.DeclaringType.GetProperty(fieldName, FieldFlags).GetSetMethod() == null)
                    {
                        setterExpr = new CodeVariableReferenceExpression(ExtensionClassName + "." + this.SetterFieldName(finfo));
                        fieldName = this.m_fieldToStaticName[finfo];
                    }
                    tempDecl = new CodeVariableDeclarationStatement(type, fieldName);
                    fieldExpr = new CodeVariableReferenceExpression(tempDecl.Name);
                }

                CodeExpression fieldValExpr = new CodeMethodInvokeExpression(serializerExpr, "Read", readerExpr);
                CodeStatement readCall = new CodeAssignStatement(fieldExpr, fieldValExpr);
                if (tempDecl == null)
                {
                    stmts = new CodeStatement[] { readCall };
                }
                else
                {
                    CodeStatement setCall;
                    if (setterExpr == null)
                    {
                        CodeExpression propExpr = new CodePropertyReferenceExpression(objExpr, tempDecl.Name);
                        setCall = new CodeAssignStatement(propExpr, fieldExpr);
                    }
                    else
                    {
                        if (finfo.DeclaringType.IsValueType)
                        {
                            objExpr = new CodeDirectionExpression(FieldDirection.Out, objExpr);
                        }
                        CodeExpression setExpr = new CodeDelegateInvokeExpression(setterExpr, objExpr, fieldExpr);
                        setCall = new CodeExpressionStatement(setExpr);
                    }
                    stmts = new CodeStatement[] { tempDecl, readCall, setCall };
                }
            }
            else
            {
                // for builtin types
                CodeExpression readCall = new CodeMethodInvokeExpression(readerExpr, readerName);
                if (finfo == null)
                {
                    CodeExpression fieldExpr = new CodeArrayIndexerExpression(objExpr, indexExprs);
                    stmts = new CodeStatement[] { new CodeAssignStatement(fieldExpr, readCall) };
                }
                else
                {
                    string fieldName = TypeSystem.FieldName(finfo.Name);
                    if ((finfo.IsPublic && !finfo.IsInitOnly) ||
                        (TypeSystem.IsBackingField(finfo.Name) &&
                         finfo.DeclaringType.GetProperty(fieldName, FieldFlags).GetSetMethod() != null))
                    {
                        CodeExpression fieldExpr = new CodeFieldReferenceExpression(objExpr, fieldName);
                        stmts = new CodeStatement[] { new CodeAssignStatement(fieldExpr, readCall) };
                    }
                    else
                    {
                        CodeExpression setterExpr = new CodeVariableReferenceExpression(
                                                            ExtensionClassName + "." + this.SetterFieldName(finfo));
                        if (finfo.DeclaringType.IsValueType)
                        {
                            objExpr = new CodeDirectionExpression(FieldDirection.Out, objExpr);
                        }
                        CodeExpression setExpr = new CodeDelegateInvokeExpression(setterExpr, objExpr, readCall);
                        stmts = new CodeStatement[] { new CodeExpressionStatement(setExpr) };
                    }
                }
            }

            if (!type.IsValueType &&
                (finfo != null || StaticConfig.AllowNullArrayElements) &&
                (finfo == null || AttributeSystem.FieldCanBeNull(finfo)))
            {
                CodeExpression bvIndex = indexExprs[0];
                if (finfo == null)
                {
                    string bvIndexString = indexExprs[0].VariableName;
                    for (int i = 1; i < indexExprs.Length; i++)
                    {
                        bvIndexString += "*" + indexExprs[i].VariableName;
                    }
                    bvIndex = new CodeSnippetExpression(bvIndexString);
                }
                CodeExpression bvExpr = new CodeArgumentReferenceExpression("bv");
                CodeExpression ifExpr = new CodeBinaryOperatorExpression(
                                                new CodeIndexerExpression(bvExpr, bvIndex),
                                                CodeBinaryOperatorType.IdentityEquality,
                                                new CodePrimitiveExpression(false));
                CodeStatement stmt = new CodeConditionStatement(ifExpr, stmts);
                stmts = new CodeStatement[] { stmt };
            }
            return stmts;
        }

        private CodeStmtPair MakeWriteFieldStatements(Type type,
                                                      CodeExpression objExpr,
                                                      FieldInfo finfo,
                                                      CodeVariableReferenceExpression[] indexExprs)
        {
            CodeExpression writerExpr = new CodeArgumentReferenceExpression("writer");
            CodeExpression fieldExpr;
            if (finfo == null)
            {
                fieldExpr = new CodeArrayIndexerExpression(objExpr, indexExprs);
            }
            else
            {
                string fieldName = TypeSystem.FieldName(finfo.Name);
                if (finfo.IsPublic ||
                    (TypeSystem.IsBackingField(finfo.Name) &&
                     finfo.DeclaringType.GetProperty(fieldName, FieldFlags).GetGetMethod() != null))
                {
                    fieldExpr = new CodeFieldReferenceExpression(objExpr, fieldName);
                }
                else
                {
                    CodeExpression getterExpr = new CodeVariableReferenceExpression(
                                                        ExtensionClassName + "." + this.GetterFieldName(finfo));
                    if (finfo.DeclaringType.IsValueType)
                    {
                        objExpr = new CodeDirectionExpression(FieldDirection.Out, objExpr);
                    }
                    fieldExpr = new CodeDelegateInvokeExpression(getterExpr, objExpr);
                }
            }

            CodeExpression writeCall;
            if (GetBuiltinReaderName(type) == null)
            {
                // for non-builtin types
                string serializerName = GetStaticSerializerName(type);
                CodeVariableReferenceExpression serializerExpr = new CodeVariableReferenceExpression(serializerName);
                writeCall = new CodeMethodInvokeExpression(serializerExpr, "Write", writerExpr, fieldExpr);
            }
            else
            {
                // for builtin types
                writeCall = new CodeMethodInvokeExpression(writerExpr, "Write", fieldExpr);
            }
            CodeStatement stmt1 = new CodeExpressionStatement(writeCall);
            
            if (type.IsValueType)
            {
                return new CodeStmtPair(null, new CodeStatement[] { stmt1 });
            }
            else if (finfo == null)
            {
                if (StaticConfig.AllowNullArrayElements)
                {
                    string bvIndexString = indexExprs[0].VariableName;
                    for (int i = 1; i < indexExprs.Length; i++)
                    {
                        bvIndexString += "*" + indexExprs[i].VariableName;
                    }
                    CodeExpression bvIndex = new CodeSnippetExpression(bvIndexString);
                    CodeExpression nullExpr = new CodeMethodInvokeExpression(
                                                      new CodeTypeReferenceExpression("Object"),
                                                      "ReferenceEquals",
                                                      fieldExpr,
                                                      NullExpr);
                    CodeExpression bvExpr = new CodeArgumentReferenceExpression("bv");
                    CodeStatement stmt0 = new CodeExpressionStatement(
                                                  new CodeMethodInvokeExpression(bvExpr, "Set", bvIndex));
                    stmt0 = new CodeConditionStatement(nullExpr, stmt0);

                    CodeExpression notNullExpr = new CodeBinaryOperatorExpression(
                                                         new CodeIndexerExpression(bvExpr, bvIndex),
                                                         CodeBinaryOperatorType.IdentityEquality,
                                                         new CodePrimitiveExpression(false));
                    stmt1 = new CodeConditionStatement(notNullExpr, stmt1);
                    return new CodeStmtPair(new CodeStatement[] { stmt0 }, new CodeStatement[] { stmt1 });      
                }
                else
                {
                    return new CodeStmtPair(null, new CodeStatement[] { stmt1 });
                }
            }
            else
            {
                CodeExpression nullExpr = new CodeMethodInvokeExpression(
                                                  new CodeTypeReferenceExpression("Object"),
                                                  "ReferenceEquals",
                                                  fieldExpr,
                                                  NullExpr);
                if (AttributeSystem.FieldCanBeNull(finfo))
                {
                    CodeExpression bvExpr = new CodeArgumentReferenceExpression("bv");
                    CodeStatement stmt0 = new CodeExpressionStatement(
                                                  new CodeMethodInvokeExpression(bvExpr, "Set", indexExprs[0]));
                    stmt0 = new CodeConditionStatement(nullExpr, stmt0);
                    
                    CodeExpression notNullExpr = new CodeBinaryOperatorExpression(
                                                         new CodeIndexerExpression(bvExpr, indexExprs[0]),
                                                         CodeBinaryOperatorType.IdentityEquality,
                                                         new CodePrimitiveExpression(false));
                    stmt1 = new CodeConditionStatement(notNullExpr, stmt1);
                    return new CodeStmtPair(new CodeStatement[] { stmt0 }, new CodeStatement[] { stmt1 });
                }
                else
                {
                    // YY: For now we always check null
                    string msg = "Field " + finfo.DeclaringType.Name + "." + finfo.Name + " is null.";
                    CodeExpression msgExpr = new CodePrimitiveExpression(msg);
                    CodeExpression throwExpr = new CodeObjectCreateExpression(typeof(ArgumentNullException), msgExpr);
                    CodeStatement stmt0 = new CodeConditionStatement(nullExpr, new CodeThrowExceptionStatement(throwExpr));
                    return new CodeStmtPair(null, new CodeStatement[] { stmt0, stmt1 });                    
                }
            }
        }

        private static Type FindCustomSerializerType(Type type)
        {
            // Look for [CustomDryadLinqSerializer] on the UDT. 
            // Skip inheritance hieararchy, we don't want CustomDryadLinqSerializer declarations
            // on the UDT's parent types to take effect.
            object[] attributes = type.GetCustomAttributes(typeof(CustomDryadLinqSerializerAttribute), false);
            if (attributes.Length == 1)
            {
                CustomDryadLinqSerializerAttribute attr = (CustomDryadLinqSerializerAttribute)attributes[0];
                Type serializerType = attr.SerializerType;

                // make sure the serializer type specified in the attribute isn't null
                if (serializerType == null)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.SerializerTypeMustBeNonNull,
                                                 String.Format(SR.SerializerTypeMustBeNonNull, type.FullName));
                }

                // Make sure the serializer type specified in the attribute implements IDryadLinqSerializer<T>
                bool found = false;
                if (type.IsGenericType)
                {
                    Type type1 = type.GetGenericTypeDefinition();
                    foreach (var intf in serializerType.GetInterfaces())
                    {
                        if (intf.GetGenericTypeDefinition() == typeof(IDryadLinqSerializer<>) &&
                            intf.GetGenericArguments()[0].GetGenericTypeDefinition() == type1)
                        {
                            found = true;
                            break;
                        }
                    }
                }
                else
                {
                    Type expectedSerializerInterfaceType = typeof(IDryadLinqSerializer<>).MakeGenericType(type);
                    found = expectedSerializerInterfaceType.IsAssignableFrom(serializerType);
                }
                if (!found)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.SerializerTypeMustSupportIDryadLinqSerializer,
                                                 String.Format(SR.SerializerTypeMustSupportIDryadLinqSerializer,
                                                               serializerType.FullName, type.FullName));
                }
                return serializerType;
            }

            return null;
        }

        // Returns true if the DryadLinqSerialization classes define
        // the type's Read and Write methods. For now we only support
        // generic types with at most 3 type parameters.
        private static string GetGenericSerializationClassName(Type type)
        {
            Type[] genericArgs = type.GetGenericArguments();
            if (genericArgs.Length > 2)
            {
                return null;
            }

            Type refType = type.MakeByRefType();
            if (genericArgs.Length == 1)
            {
                Type[] typeArgs = new Type[] { genericArgs[0], 
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[0]) };
                Type dsType = typeof(DryadLinqSerialization<,>).MakeGenericType(typeArgs);
                MethodInfo readMethod = TypeSystem.FindStaticMethod(dsType, "Read", new Type[]{ typeof(DryadLinqBinaryReader), refType });
                MethodInfo writeMethod = TypeSystem.FindStaticMethod(dsType, "Write", new Type[]{ typeof(DryadLinqBinaryWriter), type });
                if (readMethod != null && writeMethod != null)
                {
                    return "DryadLinqSerialization";
                }
                if (typeArgs[0].IsValueType)
                {
                    dsType = typeof(StructDryadLinqSerialization<,>).MakeGenericType(typeArgs);
                    readMethod = TypeSystem.FindStaticMethod(dsType, "Read", new Type[] { typeof(DryadLinqBinaryReader), refType });
                    writeMethod = TypeSystem.FindStaticMethod(dsType, "Write", new Type[] { typeof(DryadLinqBinaryWriter), type });
                    if (readMethod != null && writeMethod != null)
                    {
                        return "StructDryadLinqSerialization";
                    }
                }
            }
            else if (genericArgs.Length == 2)
            {
                Type[] typeArgs = new Type[] { genericArgs[0],
                                               genericArgs[1],
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[0]),
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[1]) };
                Type dsType = typeof(DryadLinqSerialization<,,,>).MakeGenericType(typeArgs);
                MethodInfo readMethod = TypeSystem.FindStaticMethod(dsType, "Read", new Type[]{ typeof(DryadLinqBinaryReader), refType });
                MethodInfo writeMethod = TypeSystem.FindStaticMethod(dsType, "Write", new Type[]{ typeof(DryadLinqBinaryWriter), type });
                if (readMethod != null && writeMethod != null)
                {
                    return "DryadLinqSerialization";
                }
                if (typeArgs[0].IsValueType && typeArgs[1].IsValueType)
                {
                    dsType = typeof(StructDryadLinqSerialization<,,,>).MakeGenericType(typeArgs);
                    readMethod = TypeSystem.FindStaticMethod(dsType, "Read", new Type[] { typeof(DryadLinqBinaryReader), refType });
                    writeMethod = TypeSystem.FindStaticMethod(dsType, "Write", new Type[] { typeof(DryadLinqBinaryWriter), type });
                    if (readMethod != null && writeMethod != null)
                    {
                        return "StructDryadLinqSerialization";
                    }
                }
            }
            else if (genericArgs.Length == 3)
            {
                Type[] typeArgs = new Type[] { genericArgs[0],
                                               genericArgs[1],
                                               genericArgs[2],
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[0]),
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[1]),
                                               typeof(DryadLinqSerializer<>).MakeGenericType(genericArgs[2]) };
                Type dsType = typeof(DryadLinqSerialization<,,,,,>).MakeGenericType(typeArgs);
                MethodInfo readMethod = TypeSystem.FindStaticMethod(dsType, "Read", new Type[] { typeof(DryadLinqBinaryReader), refType });
                MethodInfo writeMethod = TypeSystem.FindStaticMethod(dsType, "Write", new Type[] { typeof(DryadLinqBinaryWriter), type });
                if (readMethod != null && writeMethod != null)
                {
                    return "DryadLinqSerialization";
                }
            }
            return null;
        }

        private static bool IsObject(Type type)
        {
            Type elemType = type;
            while (elemType.IsArray)
            {
                elemType = elemType.GetElementType();
            }
            return elemType == typeof(object);
        }

        // Add the serializer class
        internal string AddSerializerClass(Type type)
        {
            // Check if the serializer class is built-in
            string serializerName = GetBuiltInDryadLinqSerializer(type);
            if (serializerName != null)
            {
                return serializerName;
            }

            // Check if the serializer class is already generated
            if (this.m_typeToSerializerName.TryGetValue(type, out serializerName))
            {
                return serializerName;
            }

            // Check for custom serialization
            Type customSerializerType = FindCustomSerializerType(type);
            if (customSerializerType != null)
            {
                serializerName = TypeSystem.TypeName(customSerializerType, this.AnonymousTypeToName);
                if (type.IsGenericType)
                {
                    Type[] argTypes = type.GetGenericArguments();
                    int len = argTypes.Length;
                    for (int i = 0; i < len; i++)
                    {
                        this.AddAnonymousClass(argTypes[i]);
                    }
                    if (customSerializerType.IsGenericTypeDefinition)
                    {
                        if (customSerializerType.GetGenericArguments().Length != len * 2)
                        {
                            throw new DryadLinqException("The custom serializer " + customSerializerType +
                                                         " must have " + (len*2) + " generic type parameters.");
                        }

                        int cnt = 1;
                        int matchIdx = serializerName.Length - 2;
                        while (matchIdx >= 0)
                        {
                            if (serializerName[matchIdx] == '>') cnt++;
                            if (serializerName[matchIdx] == '<') cnt--;
                            if (cnt == 0) break;
                            matchIdx--;
                        }
                        serializerName = serializerName.Substring(0, matchIdx);
                        serializerName += "<";
                        for (int i = 0; i < len; i++)
                        {
                            serializerName += this.MakeTypeNameAlias(
                                                      TypeSystem.TypeName(argTypes[i], this.m_anonymousTypeToName));
                            serializerName += ",";
                        }
                        for (int i = 0; i < len; i++)
                        {
                            serializerName += this.MakeTypeNameAlias(this.AddSerializerClass(argTypes[i]));
                            if (i < (len-1)) serializerName += ",";
                        }
                        serializerName += ">";
                    }
                }
                return serializerName;
            }

            if (!TypeSystem.IsAnonymousType(type))
            {
                if (!type.IsPublic && !type.IsNestedPublic)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.TypeRequiredToBePublic,
                                                 String.Format(SR.TypeRequiredToBePublic, type));
                }
                if (IsObject(type))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CannotHandleObjectFields,
                                                 String.Format(SR.CannotHandleObjectFields, type.FullName));
                }

                // The serializer has troubles if a data type has no data-members, so we outlaw these.
                // Abstract classes don't admit such an easy test. 
                if (!type.IsAbstract && TypeSystem.GetSize(type) == 0)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.TypeMustHaveDataMembers,
                                                 String.Format(SR.TypeMustHaveDataMembers, type));
                }
            }

            bool isReal = TypeSystem.IsRealType(type);
            this.AddAnonymousClass(type);
            bool isTypeSerializable = TypeSystem.IsTypeSerializable(type);

            // Check for builtin serialization
            CodeExpression serializationTypeExpr = null;
            if (type.IsGenericType)
            {
                string serializationClassName = GetGenericSerializationClassName(type);
                if (serializationClassName != null)
                {
                    // Add anonymous classes for type arguments
                    Type[] argTypes = type.GetGenericArguments();
                    int len = argTypes.Length;
                    for (int i = 0; i < len; i++)
                    {
                        this.AddAnonymousClass(argTypes[i]);
                    }
                    CodeTypeReference[] argRefs = new CodeTypeReference[len * 2];
                    for (int i = 0; i < len; i++)
                    {
                        argRefs[i] = new CodeTypeReference(this.MakeTypeNameAlias(TypeSystem.TypeName(argTypes[i], this.m_anonymousTypeToName)));
                        argRefs[len + i] = new CodeTypeReference(this.MakeTypeNameAlias(this.AddSerializerClass(argTypes[i])));
                    }
                    CodeTypeReference typeRef = new CodeTypeReference(serializationClassName, argRefs);
                    serializationTypeExpr = new CodeTypeReferenceExpression(typeRef);
                }
            }

            // We now add the serializer class
            serializerName = "DryadLinqSerializer" + MakeName(type);
            this.m_typeToSerializerName[type] = serializerName;
            string typeName = TypeSystem.TypeName(type, this.m_anonymousTypeToName);
            string baseClassName = "DryadLinqSerializer<" + typeName + ">";
            CodeTypeDeclaration serializerClass = new CodeTypeDeclaration(serializerName + " : " + baseClassName);
            this.m_dryadCodeSpace.Types.Add(serializerClass);
            serializerClass.IsClass = true;
            serializerClass.TypeAttributes = TypeAttributes.Public | TypeAttributes.Sealed;

            // Add the Read method
            CodeMemberMethod readMethod = new CodeMemberMethod();
            serializerClass.Members.Add(readMethod);
            readMethod.Attributes = MemberAttributes.Public | MemberAttributes.Override;
            readMethod.Name = "Read";
            readMethod.Parameters.Add(new CodeParameterDeclarationExpression(typeof(DryadLinqBinaryReader), "reader"));
            typeName = this.MakeTypeNameAlias(typeName);
            readMethod.ReturnType = new CodeTypeReference(typeName);

            CodeExpression objExpr = new CodeArgumentReferenceExpression("obj");
            CodeExpression readerExpr = new CodeArgumentReferenceExpression("reader");
            if (type.IsEnum)
            {
                string readerName = GetBuiltinReaderName(type.GetFields()[0].FieldType);
                CodeExpression valExpr = new CodeMethodInvokeExpression(readerExpr, readerName);
                valExpr = new CodeCastExpression(type, valExpr);
                readMethod.Statements.Add(new CodeMethodReturnStatement(valExpr));                
            }
            else if (serializationTypeExpr != null)
            {
                CodeExpression outObjExpr = new CodeDirectionExpression(FieldDirection.Out, objExpr);
                CodeExpression readCallExpr = new CodeMethodInvokeExpression(
                                                      serializationTypeExpr, "Read", readerExpr, outObjExpr);
                readMethod.Statements.Add(new CodeVariableDeclarationStatement(typeName, "obj"));
                readMethod.Statements.Add(new CodeExpressionStatement(readCallExpr));
                readMethod.Statements.Add(new CodeMethodReturnStatement(objExpr));
            }
            else if (TypeSystem.IsAnonymousType(type))
            {
                string className = this.m_anonymousTypeToName[type];
                CodeExpression newObjectCall = new CodeMethodInvokeExpression(
                                                       new CodeTypeReferenceExpression("FormatterServices"),
                                                       "GetUninitializedObject",
                                                       new CodeTypeOfExpression(className));
                newObjectCall = new CodeCastExpression(className, newObjectCall);
                readMethod.Statements.Add(new CodeVariableDeclarationStatement(className, "obj", newObjectCall));

                PropertyInfo[] props = type.GetProperties();
                System.Array.Sort(props, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));
                for (int i = 0; i < props.Length; i++)
                {
                    string fieldName = "_" + props[i].Name;
                    CodeExpression fieldExpr = new CodeFieldReferenceExpression(objExpr, fieldName);
                    string readerName = GetBuiltinReaderName(props[i].PropertyType);
                    CodeStatement stmt;
                    if (readerName == null)
                    {
                        string fieldSerializerName = GetStaticSerializerName(props[i].PropertyType);
                        CodeVariableReferenceExpression
                            serializerExpr = new CodeVariableReferenceExpression(fieldSerializerName);
                        CodeExpression
                            readCallExpr = new CodeMethodInvokeExpression(serializerExpr, "Read", readerExpr);
                        stmt = new CodeAssignStatement(fieldExpr, readCallExpr);
                    }
                    else
                    {
                        CodeExpression readCallExpr = new CodeMethodInvokeExpression(readerExpr, readerName);
                        stmt = new CodeAssignStatement(fieldExpr, readCallExpr);
                    }
                    if (!props[i].PropertyType.IsValueType)
                    {
                        CodeExpression ifExpr = new CodeMethodInvokeExpression(readerExpr, "ReadBool");
                        stmt = new CodeConditionStatement(ifExpr, stmt);
                    }
                    readMethod.Statements.Add(stmt);
                }
                readMethod.Statements.Add(new CodeMethodReturnStatement(objExpr));
            }
            else if (!isReal)
            {
                throw new DryadLinqException(DryadLinqErrorCode.UDTMustBeConcreteType,
                                             String.Format(SR.UDTMustBeConcreteType, type.FullName));
            }
            else if (TypeSystem.HasFieldOfNonPublicType(type))
            {
                throw new DryadLinqException(DryadLinqErrorCode.UDTHasFieldOfNonPublicType,
                                             String.Format(SR.UDTHasFieldOfNonPublicType, type.FullName));
            }
            else if (typeof(System.Delegate).IsAssignableFrom(type))
            {
                throw new DryadLinqException(DryadLinqErrorCode.UDTIsDelegateType,
                                             String.Format(SR.UDTIsDelegateType, type.FullName));
            }
            else if (!type.IsSealed && TypeSystem.HasSubtypes(type))
            {
                throw new DryadLinqException(DryadLinqErrorCode.CannotHandleSubtypes,
                                             String.Format(SR.CannotHandleSubtypes, type.FullName));
            }
            else if (isTypeSerializable)   // The only choice we have left is to add the auto generated Read method body. 
            {
                // make sure we aren't trying to auto-serialize a circular type
                if (TypeSystem.IsCircularType(type))
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CannotHandleCircularTypes,
                                                 String.Format(SR.CannotHandleCircularTypes, type.FullName));
                }
                readMethod.Statements.AddRange(this.MakeReadMethodBody(type));
            }
            else
            {
                // tell the user we could do this automatically for them, but they just need to ask explicitly
                throw new DryadLinqException(DryadLinqErrorCode.TypeNotSerializable,
                                             String.Format(SR.TypeNotSerializable, type.FullName));
            }
            
            // Add the Write method
            CodeMemberMethod writeMethod = new CodeMemberMethod();
            serializerClass.Members.Add(writeMethod);
            writeMethod.Attributes = MemberAttributes.Public | MemberAttributes.Override;
            writeMethod.Name = "Write";
            writeMethod.Parameters.Add(new CodeParameterDeclarationExpression(typeof(DryadLinqBinaryWriter), "writer"));
            writeMethod.Parameters.Add(new CodeParameterDeclarationExpression(typeName, "obj"));
            writeMethod.ReturnType = new CodeTypeReference(typeof(void));

            CodeExpression writerExpr = new CodeArgumentReferenceExpression("writer");
            if (type.IsEnum)
            {
                Type intType = type.GetFields()[0].FieldType;
                CodeExpression valExpr = new CodeCastExpression(intType, objExpr);
                CodeExpression writeCallExpr = new CodeMethodInvokeExpression(writerExpr, "Write", valExpr);
                writeMethod.Statements.Add(new CodeExpressionStatement(writeCallExpr));
            }
            else if (serializationTypeExpr != null)
            {
                CodeExpression writeCallExpr = new CodeMethodInvokeExpression(
                                                       serializationTypeExpr, "Write", writerExpr, objExpr);
                writeMethod.Statements.Add(new CodeExpressionStatement(writeCallExpr));
            }
            else if (TypeSystem.IsAnonymousType(type))
            {
                PropertyInfo[] props = type.GetProperties();
                System.Array.Sort(props, (x, y) => x.MetadataToken.CompareTo(y.MetadataToken));
                for (int i = 0; i < props.Length; i++)
                {
                    Type fieldType = props[i].PropertyType;
                    string fieldName = "_" + props[i].Name;
                    CodeExpression fieldExpr = new CodeFieldReferenceExpression(objExpr, fieldName);
                    CodeExpression writeCall;
                    if (GetBuiltinReaderName(type) == null)
                    {
                        string fieldSerializerName = GetStaticSerializerName(fieldType);
                        CodeVariableReferenceExpression
                            serializerExpr = new CodeVariableReferenceExpression(fieldSerializerName);
                        writeCall = new CodeMethodInvokeExpression(serializerExpr, "Write", writerExpr, fieldExpr);
                    }
                    else
                    {
                        writeCall = new CodeMethodInvokeExpression(writerExpr, "Write", fieldExpr);
                    }
                    CodeStatement stmt = new CodeExpressionStatement(writeCall);
                    if (!fieldType.IsValueType)
                    {
                        CodeExpression nullExpr = new CodeMethodInvokeExpression(
                                                          new CodeTypeReferenceExpression("Object"),
                                                          "ReferenceEquals",
                                                          fieldExpr,
                                                          NullExpr);
                        CodeExpression notNullExpr = new CodeBinaryOperatorExpression(
                                                             nullExpr,
                                                             CodeBinaryOperatorType.IdentityEquality,
                                                             new CodePrimitiveExpression(false));
                        writeCall = new CodeMethodInvokeExpression(writerExpr, "Write", notNullExpr);
                        writeMethod.Statements.Add(new CodeExpressionStatement(writeCall));
                        stmt = new CodeConditionStatement(notNullExpr, stmt);
                    }
                    writeMethod.Statements.Add(stmt);
                }
            }
            else
            {
                writeMethod.Statements.AddRange(this.MakeWriteMethodBody(type));
            }

            return serializerName;
        }

        private CodeMemberField AddCustomSerializerStaticField(Type type, Type customSerializerType)
        {
            // create unique name for the static instance
            string customSerializerInstanceName = String.Format("customSerializer_{0}", MakeName(type));
            CodeMemberField customSerializerField = new CodeMemberField(customSerializerType, customSerializerInstanceName);
            customSerializerField.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
            
            // Now we need to add the init expression for the serializer instance
            if (customSerializerType.IsClass && !customSerializerType.IsByRef)
            {
                // if the serializer type is a CLASS, this expression will be the default ctor of the custom serializer type
                // i.e. "internal static CustomSerializerType customSerializerInstance = new CustomSerializerType();"
                customSerializerField.InitExpression = new CodeObjectCreateExpression(customSerializerType);

                // make sure the custom serialzier type has a default constructor because we need to instantiate a static copy
                var ctorInfo = customSerializerType.GetConstructor(Type.EmptyTypes);
                if (ctorInfo == null)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.CustomSerializerMustSupportDefaultCtor,
                                                 String.Format(SR.CustomSerializerMustSupportDefaultCtor,
                                                               customSerializerType.FullName));
                }
            }
            else if (customSerializerType.IsValueType && !customSerializerType.IsByRef)
            {
                // if the serializer type is a VALUE TYPE, this expression will be the default value of the custom serializer type
                // i.e. "internal static CustomSerializerType customSerializerInstance = default(CustomSerializerType);"
                customSerializerField.InitExpression = new CodeDefaultValueExpression(new CodeTypeReference(customSerializerType));
            }
            else
            {
                // neither class, nor value type means they either passed in an interface or a byref type, none of which we support
                throw new DryadLinqException(DryadLinqErrorCode.CustomSerializerMustBeClassOrStruct,
                                             String.Format(SR.CustomSerializerMustBeClassOrStruct,
                                                           customSerializerType.FullName, type.FullName));
            }


            // We don't need to ensure uniqueness of "customSerializer_Type_XX" fields
            // here because the caller of this method runs only once per UDT
            m_dryadExtensionClass.Members.Add(customSerializerField);

            return customSerializerField;
        }

        internal CodeVariableDeclarationStatement
            MakeVarDeclStatement(string typeName, string varName, CodeExpression expr)
        {
            return new CodeVariableDeclarationStatement(typeName, MakeUniqueName(varName), expr);
        }
        
        internal CodeVariableDeclarationStatement
            MakeVarDeclStatement(Type type, string varName, CodeExpression expr)
        {
            string typeName = TypeSystem.TypeName(type, this.AnonymousTypeToName);
            return new CodeVariableDeclarationStatement(typeName, MakeUniqueName(varName), expr);
        }

        internal CodeExpression MakeExpression(Expression expr)
        {
            string exprString = DryadLinqExpression.ToCSharpString(expr, this.AnonymousTypeToName);
            return new CodeSnippetExpression(exprString);
        }
    
        internal CodeVariableDeclarationStatement MakeFactoryDecl(Type type)
        {
            CodeExpression factoryInitExpr = new CodeObjectCreateExpression(DryadLinqFactoryClassName(type));
            return this.MakeVarDeclStatement(DryadLinqFactoryClassName(type), "factory", factoryInitExpr);
        }

        internal CodeVariableDeclarationStatement MakeVertexReaderDecl(Type type, string factoryName)
        {
            CodeExpression readerInitExpr = new CodeMethodInvokeExpression(
                                                    new CodeArgumentReferenceExpression(VertexEnvName),
                                                    "MakeReader",
                                                    new CodeArgumentReferenceExpression(factoryName));
            return this.MakeVarDeclStatement("var", "dreader", readerInitExpr);
        }

        internal CodeVariableDeclarationStatement MakeVertexWriterDecl(Type type, string factoryName)
        {
            CodeExpression writerInitExpr = new CodeMethodInvokeExpression(
                                                    new CodeArgumentReferenceExpression(VertexEnvName),
                                                    "MakeWriter",
                                                    new CodeArgumentReferenceExpression(factoryName));
            return this.MakeVarDeclStatement("var", "dwriter", writerInitExpr);
        }

        internal CodeVariableDeclarationStatement MakeSourceDecl(string methodName, string denvName)
        {
            CodeExpression sourceInitExpr = new CodeMethodInvokeExpression(
                                                    new CodeVariableReferenceExpression(denvName),
                                                    methodName,
                                                    new CodePrimitiveExpression(true));
            return this.MakeVarDeclStatement("var", "source", sourceInitExpr);
        }

        internal static CodeVariableDeclarationStatement MakeVertexParamsDecl(DLinqQueryNode node)
        {
            int inputArity = node.InputArity + node.GetReferencedQueries().Count;
            int outputArity = node.OutputArity;
            
            CodeExpression arg1 = new CodePrimitiveExpression(inputArity);
            CodeExpression arg2 = new CodePrimitiveExpression(outputArity);
            CodeExpression vertexParamsInitExpr = new CodeObjectCreateExpression("DryadLinqVertexParams", arg1, arg2);
            CodeVariableDeclarationStatement
                vertexParamsDecl = new CodeVariableDeclarationStatement("DryadLinqVertexParams",
                                                                        VertexParamName,
                                                                        vertexParamsInitExpr);
            return vertexParamsDecl;
        }

        internal static CodeAssignStatement SetVertexParamField(string fieldName, object value)
        {
            CodeExpression vertexParam = new CodeArgumentReferenceExpression(VertexParamName);
            CodeExpression left = new CodeFieldReferenceExpression(vertexParam, fieldName);
            CodeExpression right = new CodePrimitiveExpression(value);
            return new CodeAssignStatement(left, right);
        }

        internal static CodeVariableDeclarationStatement MakeVertexEnvDecl(DLinqQueryNode node)
        {
            CodeExpression arg1 = new CodeArgumentReferenceExpression("args");
            CodeExpression arg2 = new CodeArgumentReferenceExpression(VertexParamName);
            
            CodeExpression 
                denvInitExpr = new CodeObjectCreateExpression("VertexEnv", arg1, arg2);
            return new CodeVariableDeclarationStatement("VertexEnv", VertexEnvName, denvInitExpr);
        }

        // Add a new vertex method to the DryadLinq vertex class
        internal CodeMemberMethod AddVertexMethod(DLinqQueryNode node)
        {
            CodeMemberMethod vertexMethod = new CodeMemberMethod();
            vertexMethod.Attributes = MemberAttributes.Public | MemberAttributes.Static;
            vertexMethod.ReturnType = new CodeTypeReference(typeof(int));
            vertexMethod.Parameters.Add(new CodeParameterDeclarationExpression(typeof(string), "args"));
            vertexMethod.Name = MakeUniqueName(node.NodeType.ToString());

            CodeTryCatchFinallyStatement tryBlock = new CodeTryCatchFinallyStatement();

            string startedMsg = "DryadLinqLog.AddInfo(\"Vertex " + vertexMethod.Name + 
                " started at {0}\", DateTime.Now.ToString(\"MM/dd/yyyy HH:mm:ss.fff\"))";
            vertexMethod.Statements.Add(new CodeSnippetExpression(startedMsg));

            // We need to add a call to CopyResources()
            vertexMethod.Statements.Add(new CodeSnippetExpression("CopyResources()"));
            
            if (StaticConfig.LaunchDebugger)
            {
                // If static config requests it, we do an unconditional Debugger.Launch() at vertex entry.
                // Currently this isn't used because StaticConfig.LaunchDebugger is hardcoded to false
                System.Console.WriteLine("Launch debugger: may block application");

                CodeExpression launchExpr = new CodeSnippetExpression("System.Diagnostics.Debugger.Launch()");
                vertexMethod.Statements.Add(new CodeExpressionStatement(launchExpr));
            }
            else
            {
                // Otherwise (the default behavior), we check an environment variable to decide whether
                // to launch the debugger, wait for a manual attach or simply skip straigt into vertex code.
                CodeMethodInvokeExpression debuggerCheckExpr = new CodeMethodInvokeExpression(
                        new CodeMethodReferenceExpression(new CodeTypeReferenceExpression(HelperClassName), 
                                                          DebugHelperMethodName));

                vertexMethod.Statements.Add(new CodeExpressionStatement(debuggerCheckExpr));
            }
            
            vertexMethod.Statements.Add(MakeVertexParamsDecl(node));
            vertexMethod.Statements.Add(SetVertexParamField("VertexStageName", vertexMethod.Name));
            vertexMethod.Statements.Add(SetVertexParamField("UseLargeBuffer", node.UseLargeWriteBuffer));
            Int32[] portCountArray = node.InputPortCounts();
            bool[] keepPortOrderArray = node.KeepInputPortOrders();
            for (int i = 0; i < node.InputArity; i++)
            {
                CodeExpression setParamsExpr = new CodeMethodInvokeExpression(
                                                       new CodeVariableReferenceExpression(VertexParamName),
                                                       "SetInputParams",
                                                       new CodePrimitiveExpression(i),
                                                       new CodePrimitiveExpression(portCountArray[i]),
                                                       new CodePrimitiveExpression(keepPortOrderArray[i]));
                vertexMethod.Statements.Add(new CodeExpressionStatement(setParamsExpr));
            }
            // YY: We could probably do better here.
            for (int i = 0; i < node.GetReferencedQueries().Count; i++)
            {
                CodeExpression setParamsExpr = new CodeMethodInvokeExpression(
                                                       new CodeVariableReferenceExpression(VertexParamName),
                                                       "SetInputParams",
                                                       new CodePrimitiveExpression(i + node.InputArity),
                                                       new CodePrimitiveExpression(1),
                                                       new CodePrimitiveExpression(false));
                vertexMethod.Statements.Add(new CodeExpressionStatement(setParamsExpr));
            }
            
            // Push the parallel-code settings into DryadLinqVertex
            bool multiThreading = this.m_context.EnableMultiThreadingInVertex;
            vertexMethod.Statements.Add(SetVertexParamField("MultiThreading", multiThreading));
            vertexMethod.Statements.Add(
                  new CodeAssignStatement(
                     new CodeFieldReferenceExpression(DLVTypeExpr, "s_multiThreading"),
                     new CodePrimitiveExpression(multiThreading)));
                                                                 
            vertexMethod.Statements.Add(MakeVertexEnvDecl(node));

            Type[] outputTypes = node.OutputTypes;
            string[] writerNames = new string[outputTypes.Length];
            for (int i = 0; i < outputTypes.Length; i++)
            {
                CodeVariableDeclarationStatement
                    writerDecl = MakeVertexWriterDecl(outputTypes[i], this.GetStaticFactoryName(outputTypes[i]));
                vertexMethod.Statements.Add(writerDecl);
                writerNames[i] = writerDecl.Name;
            }

            // Add side readers:
            node.AddSideReaders(vertexMethod);

            // Generate code based on the node type:
            switch (node.NodeType)
            {
                case QueryNodeType.Where:
                case QueryNodeType.OrderBy:
                case QueryNodeType.Distinct:
                case QueryNodeType.Skip:
                case QueryNodeType.SkipWhile:
                case QueryNodeType.Take:
                case QueryNodeType.TakeWhile:
                case QueryNodeType.Merge:
                case QueryNodeType.Select:
                case QueryNodeType.SelectMany:
                case QueryNodeType.Zip:
                case QueryNodeType.GroupBy:
                case QueryNodeType.BasicAggregate:
                case QueryNodeType.Aggregate:                
                case QueryNodeType.Contains:
                case QueryNodeType.Join:
                case QueryNodeType.GroupJoin:
                case QueryNodeType.Union:
                case QueryNodeType.Intersect:
                case QueryNodeType.Except:
                case QueryNodeType.RangePartition:
                case QueryNodeType.HashPartition:
                case QueryNodeType.Apply:
                case QueryNodeType.Fork:
                case QueryNodeType.Dynamic:
                {
                    Type[] inputTypes = node.InputTypes;
                    string[] sourceNames = new string[inputTypes.Length];
                    for (int i = 0; i < inputTypes.Length; i++)
                    {
                        CodeVariableDeclarationStatement 
                            readerDecl = MakeVertexReaderDecl(inputTypes[i], this.GetStaticFactoryName(inputTypes[i]));
                        vertexMethod.Statements.Add(readerDecl);
                        sourceNames[i] = readerDecl.Name;
                    }
                    string sourceToSink = this.m_vertexCodeGen.AddVertexCode(node, vertexMethod, sourceNames, writerNames);
                    if (sourceToSink != null)
                    {
                        CodeExpression sinkExpr = new CodeMethodInvokeExpression(
                                                           new CodeVariableReferenceExpression(writerNames[0]),
                                                           "WriteItemSequence",
                                                           new CodeVariableReferenceExpression(sourceToSink));
                        vertexMethod.Statements.Add(sinkExpr);
                    }
                    break;
                }
                case QueryNodeType.Super:
                {
                    string sourceToSink = this.m_vertexCodeGen.AddVertexCode(node, vertexMethod, null, writerNames);
                    if (sourceToSink != null)
                    {
                        CodeExpression sinkExpr = new CodeMethodInvokeExpression(
                                                           new CodeVariableReferenceExpression(writerNames[0]),
                                                           "WriteItemSequence",
                                                           new CodeVariableReferenceExpression(sourceToSink));
                        vertexMethod.Statements.Add(sinkExpr);
                    }
                    break;
                }
                default:
                {
                    //@@TODO: this should not be reachable. could change to Assert/InvalidOpEx
                    throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                                 String.Format(SR.AddVertexNotHandled, node.NodeType));
                }
            }

            string completedMsg = "DryadLinqLog.AddInfo(\"Vertex " + vertexMethod.Name + 
                " completed at {0}\", DateTime.Now.ToString(\"MM/dd/yyyy HH:mm:ss.fff\"))";
            vertexMethod.Statements.Add(new CodeSnippetExpression(completedMsg));
            
            // add a catch block
            CodeCatchClause catchBlock = new CodeCatchClause("e");
            CodeTypeReferenceExpression errorReportClass = new CodeTypeReferenceExpression("VertexEnv");
            CodeMethodReferenceExpression
                errorReportMethod = new CodeMethodReferenceExpression(errorReportClass, "ReportVertexError");
            CodeVariableReferenceExpression exRef = new CodeVariableReferenceExpression(catchBlock.LocalName);
            catchBlock.Statements.Add(new CodeMethodInvokeExpression(errorReportMethod, exRef));
            tryBlock.CatchClauses.Add(catchBlock);
            
            // wrap the entire vertex method in a try/catch block
            tryBlock.TryStatements.AddRange(vertexMethod.Statements);
            vertexMethod.Statements.Clear();
            vertexMethod.Statements.Add(tryBlock);
            
            // Always add "return 0", to make CLR hosting happy...
            vertexMethod.Statements.Add(new CodeMethodReturnStatement(ZeroExpr));

            this.m_dryadVertexClass.Members.Add(vertexMethod);
            return vertexMethod;
        }

        internal string AddVertexCode(CodeMemberMethod vertexMethod, Pipeline pipeline)
        {
            if (pipeline.Length == 0)
            {
                //@@TODO: this should not be reachable. could change to Assert/InvalidOpEx
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.CannotBeEmpty);
            }
            DLinqQueryNode firstNode = pipeline[0];
            if (firstNode.CanAttachPipeline)
            {
                firstNode.AttachedPipeline = pipeline;
                return this.m_vertexCodeGen.AddVertexCode(
                               firstNode, vertexMethod, pipeline.ReaderNames, pipeline.WriterNames);
            }
            
            int startIndex = 0;
            string applySource = pipeline.ReaderNames[0];
            if (!firstNode.IsHomomorphic)
            {
                applySource = this.m_vertexCodeGen.AddVertexCode(
                                      firstNode, vertexMethod, pipeline.ReaderNames, pipeline.WriterNames);
                if (pipeline.Length == 1) return applySource;
                startIndex = 1;
            }

            // The vertex code
            Type paramType = pipeline[startIndex].InputTypes[0].MakeArrayType();
            ParameterExpression param = Expression.Parameter(paramType, MakeUniqueName("x"));
            CodeExpression pipelineArg = pipeline.BuildExpression(startIndex, param, param);
            bool orderPreserving = (m_context.SelectOrderPreserving ||
                                    pipeline[pipeline.Length - 1].OutputDataSetInfo.orderByInfo.IsOrdered);

            CodeExpression applyExpr;
            if (this.m_context.EnableMultiThreadingInVertex)
            {
                applyExpr = new CodeMethodInvokeExpression(
                                          DryadLinqCodeGen.DLVTypeExpr,
                                          "PApply",
                                          new CodeVariableReferenceExpression(applySource),
                                          pipelineArg,
                                          new CodePrimitiveExpression(orderPreserving));
            }
            else
            {
                applyExpr = new CodeMethodInvokeExpression(
                                          DryadLinqCodeGen.DLVTypeExpr,
                                          "Apply",
                                          new CodeVariableReferenceExpression(applySource),
                                          pipelineArg);
            }
            CodeVariableDeclarationStatement
                sourceDecl = this.MakeVarDeclStatement("var", "source", applyExpr);
            vertexMethod.Statements.Add(sourceDecl);
            return sourceDecl.Name;
        }

        private void GenerateCode(string dummyFile, string srcFile)
        {
            // Add a source file containing the following line:
            string dummyClass = "namespace Microsoft.Research.DryadLinq { public static partial class " + ExtensionClassName + " { } }";
            using (StreamWriter srcWriter = new StreamWriter(dummyFile))
            {
                srcWriter.Write(dummyClass);
                srcWriter.Close();
            }
            
            // Generate code for dryadUnit and store in srcFile:
            CSharpCodeProvider provider = new CSharpCodeProvider();
            CodeGeneratorOptions options = new CodeGeneratorOptions();
            options.BracingStyle = "C";
            using (IndentedTextWriter srcWriter = new IndentedTextWriter(new StreamWriter(srcFile), "    "))
            {
                provider.GenerateCodeFromCompileUnit(this.m_dryadLinqUnit, srcWriter, options);
                srcWriter.Close();
            }
        }
            
        // Compile this compilation unit. Generate source code if asked
        // loadGeneratedAssembly specifies whether the generated assembly
        // should get loaded after compilation (into m_loadedVertexAssembly)
        private void GenerateCodeAndCompile(string dummyFile,
                                            string srcFile,
                                            string binFile,
                                            bool loadGeneratedAssembly)
        {
            // Generate the sources:
            this.GenerateCode(dummyFile, srcFile);

            // Build the parameters for source compilation.
            CompilerParameters cp = new CompilerParameters();

            // Add assembly references.
            HashSet<string> assemblySet = new HashSet<string>();
            assemblySet.Add("System");
            assemblySet.Add("System.Core");
            assemblySet.Add("System.Data");
            assemblySet.Add("System.Data.Linq");
            assemblySet.Add("System.Xml");
            foreach (string name in this.m_vertexCodeGen.GetReferencedAssemblies())
            {
                assemblySet.Add(name);
            }            
            foreach (string name in assemblySet)
            {
                cp.ReferencedAssemblies.Add(name + ".dll");
            }

            // Add references to assemblies referenced by entry assembly
            foreach (Assembly asm in TypeSystem.GetAllAssemblies())
            {
                if (!asm.IsDynamic)
                {
                    string name = asm.GetName().Name;
                    if (name != "mscorlib" &&
                        !assemblySet.Contains(name) &&
                        !String.IsNullOrEmpty(asm.Location))
                    {
                        cp.ReferencedAssemblies.Add(asm.Location);
                    }
                }
            }

            // Compiler options.
            cp.CompilerOptions = @"/unsafe";
            if (! m_context.CompileForVertexDebugging)
            {
                cp.CompilerOptions += @" /optimize+";
            }

            // Generate PDB.
            cp.IncludeDebugInformation = m_context.CompileForVertexDebugging;
            if (m_context.CompileForVertexDebugging) cp.CompilerOptions += @" /debug:full";

            // Generate an executable instead of a class library.
            cp.GenerateExecutable = false;

            // Set the assembly file name to generate.
            cp.OutputAssembly = binFile;

            // Save the assembly as a physical file.
            cp.GenerateInMemory = false;

            // Invoke compilation.
            IDictionary<string, string> providerOptions = new Dictionary<string, string>();
            
            // If the user hasn't requested "MatchClientNetFrameworkVersion" then we will force
            // the compiler version to be 3.5 (or the same as the .NET version L2H is compiled against)
            // This is to make sure we satisfy the minimun guaranteed .NET version on the cluster nodes.
            //
            // However if the user set config.MatchClientNetFrameworkVersion to true, it means
            // they know the cluster nodes match the client's (possibly higher) .NET version, and 
            // they request compilation to match that. In that case, we will use whatever the default
            // compiler version is (== matching client's .NET runtime)
            //
            // We also explicitly set the string to "v3.5" if the client is a v3.5 process (regardless of the MCNFV flag).
            // This is because the default compiler version on .NET 3.5 is actually v2.
            if (!m_context.MatchClientNetFrameworkVersion || Environment.Version.Major < 4)
            {
                // Hardcode the compiler version to 3.5, which is what L2H is built against.
                // NOTE: if we ever build L2H against a newer .NET version, we need to update this string.
                providerOptions["CompilerVersion"] = "v3.5";
            }

            IEnumerable<string> additionalSources = this.m_vertexCodeGen.GetGeneratedSources();
            string[] sourceArray = new string[additionalSources.Count() + 2];
            sourceArray[0] = dummyFile;
            sourceArray[1] = srcFile;
            int idx = 2;
            foreach (string src in additionalSources)
            {
                sourceArray[idx++] = src;
            }
            CSharpCodeProvider provider = new CSharpCodeProvider(providerOptions);
            CompilerResults cr = provider.CompileAssemblyFromFile(cp, dummyFile, srcFile);

            if (cr.Errors.Count > 0)
            {
                // Display compilation errors.
                Console.Error.WriteLine("Errors building {0}", cr.PathToAssembly);
                foreach (CompilerError ce in cr.Errors)
                {
                    DryadLinqClientLog.Add("  {0}\n\r", ce.ToString());
                }
                throw new DryadLinqException(DryadLinqErrorCode.FailedToBuild,
                                             String.Format(SR.FailedToBuild, binFile, DryadLinqClientLog.CLIENT_LOG_FILENAME));
            }
            else
            {                
                DryadLinqClientLog.Add("{0} was built successfully.", cr.PathToAssembly);                                
                this.m_generatedVertexDllPath = Path.Combine(Directory.GetCurrentDirectory(), binFile);
                if (loadGeneratedAssembly)
                {
                    this.m_loadedVertexAssembly = cr.CompiledAssembly;
                }

                // @TODO: should we lock the generated DLL if a load wasn't requested? 
            }
        }

        private void BuildAssembly(bool loadGeneratedAssembly)
        {
            // there's nothing to do if we previously built *and* loaded the vertex assembly
            if (this.m_loadedVertexAssembly != null) return;

            // if we previously built a vertex DLL without loading it, and someone is
            // requesting the loaded copy now, just load it and return currently there's
            // no scenario that would hit this case, but adding it here for completeness sake.
            if (loadGeneratedAssembly && this.m_generatedVertexDllPath != null)
            {
                this.m_loadedVertexAssembly = Assembly.LoadFrom(this.m_generatedVertexDllPath);
                return;
            }

            int inProcessVertexInstanceID = Interlocked.Increment(ref s_DryadLinqDllVersion);

            string dummyFile = DryadLinqCodeGen.GetPathForGeneratedFile(DummyExtensionSourceFile, null);
            string srcFile = DryadLinqCodeGen.GetPathForGeneratedFile(VertexSourceFile, inProcessVertexInstanceID);
            string targetName = DryadLinqCodeGen.GetPathForGeneratedFile(TargetDllName, inProcessVertexInstanceID);
            
            this.GenerateCodeAndCompile(dummyFile, srcFile, targetName, loadGeneratedAssembly);
        }

        /// <summary>
        /// Gets the <see cref="DryadLinqFactory{T}"/> for a specified type. If a factory doesn't exist,
        /// the method generates the serialization code and creates a new factory for the type.
        /// </summary>
        /// <param name="context">An instnance of <see cref="DryadLinqContext"/></param>
        /// <param name="type">A specified type</param>
        /// <returns>A <see cref="DryadLinqFactory{T}"/> for the type</returns>
        public static object GetFactory(DryadLinqContext context, Type type)
        {
            lock (s_codeGenLock)
            {
                if (s_TypeToFactory.ContainsKey(type))
                {
                    return s_TypeToFactory[type];
                }
                
                DryadLinqCodeGen codeGen = new DryadLinqCodeGen(context, new VertexCodeGen(context));
                codeGen.AddDryadCodeForType(type);
                
                // build assembly, and load into memory, because we'll next instantiate
                // the factory type out of the generated assembly.
                codeGen.BuildAssembly(true);

                string factoryTypeFullName = TargetNamespace + "." + DryadLinqFactoryClassName(type);
                object factory = codeGen.m_loadedVertexAssembly.CreateInstance(factoryTypeFullName);
                s_TypeToFactory.Add(type, factory);
                return factory;
            }
        }

        internal void BuildDryadLinqAssembly(DryadLinqQueryGen queryGen)
        {
            lock (s_codeGenLock)
            {
                // this method only gets called from DryadLinqQueryGen.GenerateDryadProgram() before job submission. 
                // Since we don't load the generated vertex DLL after that, the check for
                // "should re-gen?" below is based on m_generatedVertexDllPath being set
                if (this.m_generatedVertexDllPath == null)
                {
                    queryGen.CodeGenVisit();
                    this.BuildAssembly(false);
                }
            }
        }

        internal string GetDryadLinqDllName()
        {
            if (this.m_generatedVertexDllPath == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.AutogeneratedAssemblyMissing);
            }
            return Path.GetFileName(this.m_generatedVertexDllPath);
        }

        internal string GetTargetLocation()
        {
            if (this.m_generatedVertexDllPath== null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal, SR.AutogeneratedAssemblyMissing);
            }
            return this.m_generatedVertexDllPath;
        }

        // Utility method for creating a unique path for generated files (vertex source, DLL, object store, query plan XML etc.)
        // Each process gets its own directory under the temp path (formatted as APPNAME_<PID>), and all generated files
        // go under that directory
        internal static string GetPathForGeneratedFile(string fileNameTemplate, int? inProcessInstanceID)
        {
            Process process = System.Diagnostics.Process.GetCurrentProcess();

            // The temp folder format is:
            //      <SystemDrive>:\Users\<username>\AppData\Local\Temp\DRYADLINQ\<ExeName>_<PID>
            //
            string artifactsPath = Path.Combine(System.IO.Path.GetTempPath(),
                                                String.Format("DRYADLINQ\\{0}_{1}",
                                                              Path.GetFileNameWithoutExtension(process.MainModule.ModuleName),
                                                              process.Id));
            if (!Directory.Exists(artifactsPath))
            {
                Directory.CreateDirectory(artifactsPath);
            }

            string fileName = null;
            if (inProcessInstanceID != null)
            {
                // If an in-process instance ID is provided we format the filename template to
                //      <basename><ID>.<ext>
                //
                string baseFileName = Path.GetFileNameWithoutExtension(fileNameTemplate);
                string fileExtension = Path.GetExtension(fileNameTemplate);
                fileName = String.Format("{0}{1}{2}", baseFileName, inProcessInstanceID.Value, fileExtension);
            }
            else
            {
                // otherwise use the filename as is
                fileName = fileNameTemplate;
            }

            return Path.Combine(artifactsPath, fileName);
        }
    }   
}
