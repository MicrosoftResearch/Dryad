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
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization;
using System.Runtime.CompilerServices;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    internal class TypeHelper<T>
    {
        private static T instanceHandle = (T)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(T));

        // Return the instance of type T created as a handle
        internal static T InstanceHandle
        {
            get { return instanceHandle; }
        }

        // Create a new fresh instance of type T
        internal static T Instance
        {
            get { return (T)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(T)); }
        }
    }
    
    internal class IdentityFunction
    {
        internal static LambdaExpression Instance(Type type, string paramName)
        {
            ParameterExpression param = Expression.Parameter(type, paramName);
            Type delegateType = typeof(Func<,>).MakeGenericType(type, type);
            return Expression.Lambda(delegateType, param, param);
        }

        internal static LambdaExpression Instance(Type type)
        {
            return IdentityFunction.Instance(type, "x");
        }

        internal static bool IsIdentity(LambdaExpression expr)
        {
            return (expr.Parameters.Count == 1 &&
                    expr.Parameters[0] == expr.Body);
        }
    }

    internal static class TypeSystem
    {
        private static Dictionary<Type, int> s_sizeOfKnownTypes;
        private static HashSet<string> s_systemAssemblies;

        static TypeSystem()
        {
            //
            // Add the sizes of built in types into s_sizeOfKnownTypes. Entries in this dictionary will be
            // used by TypeSystem.GetSize() to compute the fixed size of a TRecord.
            // TypeSystem.GetSize() returns -1 if the type's size isn't fixed due to a variable size field it contains.
            // To support that, we add -1 entries in s_sizeOfKnownTypes for string and object.
            // 
            s_sizeOfKnownTypes = new Dictionary<Type, int>(20);
            s_sizeOfKnownTypes.Add(typeof(bool), sizeof(bool));
            s_sizeOfKnownTypes.Add(typeof(char), sizeof(char));
            s_sizeOfKnownTypes.Add(typeof(sbyte), sizeof(sbyte));
            s_sizeOfKnownTypes.Add(typeof(byte),  sizeof(byte));
            s_sizeOfKnownTypes.Add(typeof(short), sizeof(Int16));
            s_sizeOfKnownTypes.Add(typeof(ushort), sizeof(UInt16));
            s_sizeOfKnownTypes.Add(typeof(int), sizeof(Int32));
            s_sizeOfKnownTypes.Add(typeof(uint), sizeof(UInt32));
            s_sizeOfKnownTypes.Add(typeof(long), sizeof(Int64));
            s_sizeOfKnownTypes.Add(typeof(ulong),sizeof(UInt64));
            s_sizeOfKnownTypes.Add(typeof(float), sizeof(float));
            s_sizeOfKnownTypes.Add(typeof(double), sizeof(double));
            s_sizeOfKnownTypes.Add(typeof(decimal), sizeof(decimal));
            s_sizeOfKnownTypes.Add(typeof(DateTime), sizeof(Int64));
            s_sizeOfKnownTypes.Add(typeof(SqlDateTime), sizeof(Int64));
            s_sizeOfKnownTypes.Add(typeof(string), -1);
            s_sizeOfKnownTypes.Add(typeof(object), -1);

            s_systemAssemblies = new HashSet<string>();
            s_systemAssemblies.Add("mscorlib");
            s_systemAssemblies.Add("System");
            s_systemAssemblies.Add("Accessibility");
            s_systemAssemblies.Add("SMDiagnostics");
        }

        internal static IEnumerable<string> GetLoadedNonSystemAssemblyPaths()
        {
            List<string> names = new List<string>();
            foreach (Assembly asm in TypeSystem.GetAllAssemblies())
            {
                if (!TypeSystem.IsSystemAssembly(asm))
                {
                    names.Add(asm.Location);
                }
            }
            return names.ToArray();
        }
        
        internal static bool IsSystemAssembly(Assembly asm)
        {
            string name = asm.GetName().Name;
            return (s_systemAssemblies.Contains(name) ||
                    name.StartsWith("Microsoft.", StringComparison.Ordinal) ||
                    name.StartsWith("System.", StringComparison.Ordinal) ||
                    name == "WindowsBase");
        }

        internal static Type FindGenericType(Type definition, Type type)
        {
            while (type != null && type != typeof(object))
            {
                if (type.IsGenericType && type.GetGenericTypeDefinition() == definition)
                {
                    return type;
                }
                if (definition.IsInterface)
                {
                    foreach (Type itype in type.GetInterfaces())
                    {
                        Type found = FindGenericType(definition, itype);
                        if (found != null)
                        {
                            return found;
                        }
                    }
                }
                type = type.BaseType;
            }
            return null;
        }

        internal static Type GetElementType(Type seqType)
        {
            Type ienumType = FindGenericType(typeof(IEnumerable<>), seqType);
            if (ienumType == null)
            {
                return seqType;
            }
            return ienumType.GetGenericArguments()[0];
        }

        internal static bool IsSameOrSubclass(Type type, Type subType)
        {
            return (type == subType) || subType.IsSubclassOf(type);
        }
        
        // This should be the only method used to find out the real type.
        internal static bool IsRealType(Type type)
        {
            if (type.IsAbstract)
            {
                if (type.IsGenericType)
                {
                    Type typedef = type.GetGenericTypeDefinition();
                    if (typedef == typeof(IGrouping<,>) ||
                        typedef == typeof(IEnumerable<>) ||
                        typedef == typeof(IList<>))
                    {
                        return type.GetGenericArguments().All(x => IsRealType(x));
                    }
                }
                return false;
            }
            
            return (!type.IsGenericType ||
                    type.GetGenericArguments().All(x => IsRealType(x)));

        }

        internal static int GetInMemSize(Type type)
        {
            if (!type.IsValueType) return 8;

            int size = 0;
            bool found = s_sizeOfKnownTypes.TryGetValue(type, out size);
            if (found) return size;

            FieldInfo[] fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (FieldInfo field in fields)
            {
                size += GetInMemSize(field.FieldType);
            }
            return size;
        }

        // Returns the size of the type if it has a fixed layout.
        // Returns -1 if the type has a variable size (due to fields of string, object or arrays)
        // Note that this is the number of bytes used by DryadLINQ serialization
        internal static int GetSize(Type type)
        {
            return GetSize(type, new Dictionary<Type, int>());
        }
        
        private static int GetSize(Type type, Dictionary<Type, int> typeSizeMap)
        {
            int size = 0;
            bool found = s_sizeOfKnownTypes.TryGetValue(type, out size);
            if (found) return size;
            found = typeSizeMap.TryGetValue(type, out size);
            if (found) return size;

            if (type.IsArray) return -1;

            typeSizeMap[type] = -1;
            size = 0;
            FieldInfo[] fields = type.GetFields(BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            foreach (FieldInfo field in fields)
            {
                int fsize = GetSize(field.FieldType, typeSizeMap);
                if (fsize == -1) return -1;
                size += fsize;
            }
            typeSizeMap[type] = size;
            return size;
        }

        internal static bool StructContainsNoReference(Type type)
        {
            if (!type.IsValueType) return false;

            int size = 0;
            bool found = s_sizeOfKnownTypes.TryGetValue(type, out size);
            if (found) return true;

            FieldInfo[] fields = type.GetFields(BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            foreach (FieldInfo finfo in fields)
            {
                if (!StructContainsNoReference(finfo.FieldType)) return false;
            }
            return true;
        }

        internal static bool ContainsNoLazyValue(Type type)
        {
            return ContainsNoLazyValue(type, new HashSet<Type>());
        }

        internal static bool ContainsNoLazyValue(Type type, HashSet<Type> seen)
        {
            while (type.IsArray)
            {
                type = type.GetElementType();
            }
            if (type.IsPrimitive || 
                seen.Contains(type) ||
                type == typeof(System.Reflection.Pointer))
            {
                return true;
            }

            if (typeof(IEnumerable).IsAssignableFrom(type) &&
                !typeof(IList).IsAssignableFrom(type))
            {
                return false;
            }

            seen.Add(type);
            FieldInfo[] finfos = TypeSystem.GetAllFields(type);
            foreach (FieldInfo finfo in finfos)
            {
                if (!ContainsNoLazyValue(finfo.FieldType, seen))
                {
                    return false;
                }
            }
            return true;
        }
        
        // Returns true iff the method is a property.
        internal static bool IsProperty(MethodInfo minfo)
        {
            if (minfo.Name.StartsWith("get_", StringComparison.Ordinal))
            {
                Type declType = minfo.DeclaringType;
                ParameterInfo[] paramInfos = minfo.GetParameters();

                PropertyInfo[] pinfos = declType.GetProperties();
                foreach (var pinfo in pinfos)
                {
                    MethodInfo[] accessors = pinfo.GetAccessors();
                    foreach (MethodInfo ac in accessors)
                    {
                        if (ac.Name == minfo.Name)
                        {
                            ParameterInfo[] paramInfos1 = ac.GetParameters();
                            if (paramInfos.Length != paramInfos1.Length) break;
                            for (int i = 0; i < paramInfos.Length; i++)
                            {
                                if (paramInfos[i].ParameterType != paramInfos1[i].ParameterType) break;
                            }
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Compare two assemblies for equality.
        /// </summary>
        internal class AssemblyComparer : IEqualityComparer<Assembly>
        {
            public bool Equals(Assembly x, Assembly y)
            {
                // some Assembly objects loaded as reflection only may have different pointers
                // what matters is that their fully qualified names match

                return ReferenceEquals(x, y) || x.FullName == y.FullName; 
            }

            public int GetHashCode(Assembly obj)
            {
                return obj.FullName.GetHashCode();
            }
        }

        private static HashSet<Assembly> s_allReferencedAssemblies = null;

        /// <summary>
        /// Compute all referenced assemblies (transitive closure) and cache them.
        /// </summary>
        /// <returns>List of all referenced assemblies.</returns>
        internal static HashSet<Assembly> GetAllAssemblies()
        {
            if (s_allReferencedAssemblies == null)
            {
                // compute transitive closure
                HashSet<Assembly> referencedAssemblies = new HashSet<Assembly>(new AssemblyComparer());
                Queue<Assembly> toscan = new Queue<Assembly>(10);
                
                Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
                foreach (Assembly asm in assemblies)
                {
                    if (!referencedAssemblies.Contains(asm) && !asm.IsDynamic)
                    {
                        toscan.Enqueue(asm);
                        referencedAssemblies.Add(asm);
                    }
                }

                while (toscan.Count > 0)
                {
                    Assembly asm = toscan.Dequeue();
                    AssemblyName[] names = asm.GetReferencedAssemblies();
                    foreach (AssemblyName asmName in names)
                    {
                        try
                        {
                            Assembly refAssembly = Assembly.ReflectionOnlyLoad(asmName.FullName);
                            if (!referencedAssemblies.Contains(refAssembly))
                            {
                                toscan.Enqueue(refAssembly);
                                referencedAssemblies.Add(refAssembly);
                            }
                        }
                        catch (Exception)
                        {
                            // Console.WriteLine("Warning: Could not load referenced assembly " + asmName);
                        }
                    }
                }
                
                // Due to the use of FullName in the ReflectionOnlyLoad call, we may end up with multiple versions of the same assembly in the list 
                // if the client is running against a newer .NET version than what Microsoft.Research.DryadLinq.DLL is compiled against
                // We need to filter the list by selecting the newest version of each assembly
                var newestAssemblies = referencedAssemblies.GroupBy(asm => asm.GetName().Name).Select(grp => grp.OrderByDescending(asm => asm.GetName().Version).First());
                s_allReferencedAssemblies = new HashSet<Assembly>(new AssemblyComparer());
                foreach (var asm in newestAssemblies)
                {
                    s_allReferencedAssemblies.Add(asm);
                }
            }
            return s_allReferencedAssemblies;
        }

        private static Dictionary<Type, Type[]> s_typeMap = null;

        internal static Dictionary<Type, Type[]> BuildTypeHierarchy()
        {
            if (s_typeMap != null) return s_typeMap;

            Dictionary<Type, Type[]> typeMap = new Dictionary<Type, Type[]>();
            HashSet<Assembly> assemblies = TypeSystem.GetAllAssemblies();
            foreach (Assembly asm in assemblies)
            {
                foreach (Type type in asm.GetTypes())
                {
                    if (!type.IsInterface)
                    {
                        Type[] baseTypes = type.GetInterfaces();
                        if (type.BaseType != null && type.BaseType != typeof(object))
                        {
                            Type[] newBaseTypes = new Type[baseTypes.Length + 1];
                            Array.Copy(baseTypes, newBaseTypes, baseTypes.Length);
                            newBaseTypes[baseTypes.Length] = type.BaseType;
                            baseTypes = newBaseTypes;
                        }
                        for (int i = 0; i < baseTypes.Length; i++)
                        {
                            Type baseType = baseTypes[i];
                            if (baseType.IsGenericType)
                            {
                                baseType = baseType.GetGenericTypeDefinition();
                                baseTypes[i] = baseType;
                            }
                            
                            bool isNew = true;
                            for (int j = 0; j < i; j++)
                            {
                                if (baseTypes[j] == baseType)
                                {
                                    isNew = false;
                                    break;
                                }
                            }
                            if (isNew)
                            {
                                Type[] deriveds = null;
                                if (typeMap.TryGetValue(baseType, out deriveds))
                                {
                                    Type[] newDeriveds = new Type[deriveds.Length + 1];
                                    Array.Copy(deriveds, newDeriveds, deriveds.Length);
                                    newDeriveds[deriveds.Length] = type;
                                    deriveds = newDeriveds;
                                }
                                else
                                {
                                    deriveds = new Type[1] { type };
                                }
                                typeMap[baseType] = deriveds;
                            }
                        }
                    }
                }
            }
            s_typeMap = typeMap;
            return s_typeMap;
        }

        internal static bool HasSubtypes(Type type)
        {
            Type typeDef = type;
            if (type.IsGenericType)
            {
                typeDef = type.GetGenericTypeDefinition();
            }
            Dictionary<Type, Type[]> typeMap = TypeSystem.BuildTypeHierarchy();
            return typeMap.ContainsKey(typeDef);
        }

        internal static bool IsASubType(Type type)
        {
            return (type.BaseType != null &&
                    type.BaseType != typeof(object) &&
                    !(typeof(System.ValueType).IsAssignableFrom(type.BaseType)) && 
                    !type.IsArray);
        }

        internal static Type GetType(string name)
        {
            Type type = null;
            Assembly callingAssembly = Assembly.GetCallingAssembly();
            if (callingAssembly != null)
            {
                type = callingAssembly.GetType(name);
                if (type != null) return type;
            }
            Assembly executingAssembly = Assembly.GetExecutingAssembly();
            if (executingAssembly != null)
            {
                type = executingAssembly.GetType(name);
                if (type != null) return type;
            }
            Assembly entryAssembly = Assembly.GetEntryAssembly();
            if (entryAssembly != null)
            {
                type = entryAssembly.GetType(name);
                if (type != null) return type;
            }

            foreach (Assembly asm in GetAllAssemblies())
            {
                type = asm.GetType(name);
                if (type != null) return type;
            }
            return type;
        }
        
        // Get the current value of a static field
        internal static object GetFieldValue(string name)
        {
            int idx = name.LastIndexOf('.');
            if (idx <= 0)
            {
                throw new ArgumentException("Internal: The argument is not a reference to static field");
            }
            string tname = name.Substring(0, idx);
            string fname = name.Substring(idx+1);

            Type type = TypeSystem.GetType(tname);
            if (type == null)
            {
                throw new ArgumentException("Internal: The argument is not a reference to static field");
            }
            FieldInfo finfo = type.GetField(fname);
            if (finfo == null || !finfo.IsStatic)
            {
                throw new ArgumentException("Internal: The argument is not a reference to static field");
            }
            return finfo.GetValue(null);
        }

        internal static LambdaExpression GetExpression(string name)
        {
            if (name == null) return null;
            object val = TypeSystem.GetFieldValue(name);
            if (val == null)
            {
                throw new ArgumentException("Internal: The argument is not defined");
            }
            if (!(val is LambdaExpression))
            {
                throw new ArgumentException("Internal: The argument is not a lambda expression");
            }
            return (LambdaExpression)val;
        }

        internal static MethodInfo FindStaticMethod(Type type, string name, Type[] paramTypes, params Type[] genericTypeArgs)
        {
            MethodInfo[] methods = type.GetMethods(BindingFlags.Static | BindingFlags.Public);
            foreach (MethodInfo minfo in methods)
            {
                if (minfo.Name == name)
                {
                    MethodInfo matchedInfo = MatchArgs(minfo, paramTypes, genericTypeArgs);
                    if (matchedInfo != null)
                    {
                        return matchedInfo;
                    }
                }
            }
            return null;
        }

        internal static MethodInfo FindStaticMethod(string methodName, Type[] paramTypes)
        {
            int index = methodName.LastIndexOf('.');
            if (index == -1) return null;

            string className = methodName.Substring(0, index);
            methodName = methodName.Substring(index + 1);
            Type classType = TypeSystem.GetType(className);
            if (classType == null) return null;
            return TypeSystem.FindStaticMethod(classType, methodName, paramTypes);
        }

        private static MethodInfo MatchArgs(MethodInfo minfo, Type[] paramTypes, Type[] genericTypeArgs)
        {
            ParameterInfo[] mParams = minfo.GetParameters();
            if (mParams.Length != paramTypes.Length)
            {
                return null;
            }
            if (!minfo.IsGenericMethodDefinition && minfo.IsGenericMethod && minfo.ContainsGenericParameters)
            {
                minfo = minfo.GetGenericMethodDefinition();
            }
            if (minfo.IsGenericMethodDefinition)
            {
                if (genericTypeArgs == null ||
                    genericTypeArgs.Length == 0 ||
                    minfo.GetGenericArguments().Length != genericTypeArgs.Length)
                {
                    return null;
                }
                minfo = minfo.MakeGenericMethod(genericTypeArgs);
                mParams = minfo.GetParameters();
            }
            else if (genericTypeArgs != null && genericTypeArgs.Length > 0)
            {
                return null;
            }
            for (int i = 0; i < paramTypes.Length; i++)
            {
                Type parameterType = mParams[i].ParameterType;
                if (parameterType == null ||
                    (paramTypes[i].IsByRef && parameterType != paramTypes[i]) ||
                    !parameterType.IsAssignableFrom(paramTypes[i]))
                {
                    return null;
                }
            }
            return minfo;
        }

        internal static string TypeName(Type type)
        {
            return TypeName(type, new Dictionary<Type, string>());
        }

        internal static string TypeName(Type type, Dictionary<Type, string> typeToName)
        {
            string name = null;
            if (typeToName.TryGetValue(type, out name))
            {
                return name;
            }

            if (type.IsGenericParameter)
            {
                return type.Name;
            }

            if (type.IsArray)
            {
                Type baseType = type.GetElementType();
                while (baseType.IsArray)
                {
                    baseType = baseType.GetElementType();
                }
                string tname = TypeName(baseType, typeToName);

                Type elemType = type;
                do
                {
                    string ranks = new string(',', (elemType.GetArrayRank() - 1));
                    tname += "[" + ranks + "]";
                    elemType = elemType.GetElementType();
                }
                while (elemType.IsArray);
                return tname;
            }

            List<Type> nestedTypes = new List<Type>();
            nestedTypes.Add(type);
            Type declaringType = type;
            while (declaringType.IsNested)
            {
                declaringType = declaringType.DeclaringType;
                nestedTypes.Add(declaringType);
            }

            StringBuilder processedName = new StringBuilder();
            Type[] typeArgs = type.GetGenericArguments();
            int typeArgIndex = 0;
            bool isFirst = true;
            for (int i = nestedTypes.Count - 1; i >= 0; i--)
            {
                Type curType = nestedTypes[i];
                name = (curType.IsNested) ? curType.Name : curType.FullName;
            
                int lastIndex = name.IndexOf('`');
                if (lastIndex != -1)
                {
                    name = name.Substring(0, lastIndex);
                }
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    processedName.Append('.');
                }
                processedName.Append(name);

                if (curType.IsGenericType)
                {
                    int len = curType.GetGenericArguments().Length;
                    if (typeArgIndex < len)
                    {
                        processedName.Append('<');
                        processedName.Append(TypeName(typeArgs[typeArgIndex], typeToName));
                        typeArgIndex++;
                        while (typeArgIndex < len)
                        {
                            processedName.Append(',');
                            processedName.Append(TypeName(typeArgs[typeArgIndex], typeToName));
                            typeArgIndex++;
                        }
                        processedName.Append('>');
                    }
                }
            }

            return processedName.ToString();
        }

        internal static IComparer<T> GetComparer<T>(object comparer)
        {
            IComparer<T> res = comparer as IComparer<T>;
            if (res == null && HasDefaultComparer(typeof(T)))
            {
                if (typeof(T) == typeof(string))
                {
                    res = (IComparer<T>)StringComparer.Ordinal;
                }
                else
                {
                    res = Comparer<T>.Default;
                }
            }
            return res;
        }

        internal static bool IsComparer(object comparer, Type type)
        {
            Type comparerType = comparer.GetType();
            return typeof(IComparer<>).MakeGenericType(type).IsAssignableFrom(comparerType);
        }

        internal static IEqualityComparer<T> GetEqualityComparer<T>(object comparer)
        {
            IEqualityComparer<T> res = comparer as IEqualityComparer<T>;
            if (res == null && HasDefaultEqualityComparer(typeof(T)))
            {
                res = EqualityComparer<T>.Default;
            }
            return res;
        }

        internal static bool IsEqualityComparer(object comparer, Type type)
        {
            Type comparerType = comparer.GetType();
            return typeof(IEqualityComparer<>).MakeGenericType(type).IsAssignableFrom(comparerType);
        }

        internal static bool HasDefaultComparer(Type type)
        {
            // true if T implements IComparable<T>
            if (typeof(IComparable<>).MakeGenericType(type).IsAssignableFrom(type))
            {
                return true;
            }
            // true if T is a Nullable<U> where U implements IComparable<U>
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                Type u = type.GetGenericArguments()[0];
                if (typeof(IComparable<>).MakeGenericType(u).IsAssignableFrom(u))
                {
                    return true;
                }
            }
            // true if T implements IComparable
            return typeof(IComparable).IsAssignableFrom(type);
        }

        internal static bool HasDefaultEqualityComparer(Type type)
        {
            // true if T implements IEquatable<T>
            if (typeof(IEquatable<>).MakeGenericType(type).IsAssignableFrom(type))
            {
                return HasOverrideHashCode(type);
            }
            // true if T is a Nullable<U> where U implements IEquatable<U>
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                Type u = type.GetGenericArguments()[0];
                if (typeof(IEquatable<>).MakeGenericType(u).IsAssignableFrom(u))
                {
                    return HasOverrideHashCode(type);
                }
            }
            // true if Equals is overridden in T
            if (type.IsAbstract)
            {
                Dictionary<Type, Type[]> typeMap = BuildTypeHierarchy();
                Type[] subtypes;                
                if (typeMap.TryGetValue(type, out subtypes))
                {
                    foreach (Type subtype in subtypes)
                    {
                        if (!HasDefaultEqualityComparer(subtype))
                        {
                            return false;
                        }
                    }
                }
                return true;
            }
            MethodInfo minfo = type.GetMethod("Equals", new Type[] { typeof(object) });
            if (minfo.DeclaringType != type)
            {
                return false;
            }
            minfo = type.GetMethod("GetHashCode");
            if (minfo.DeclaringType != type)
            {
                return false;
            }
            if (type.IsGenericType)
            {
                Type[] typeArgs = type.GetGenericArguments();
                foreach (Type targ in typeArgs)
                {
                    if (!HasDefaultEqualityComparer(targ))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        private static bool HasOverrideHashCode(Type type)
        {
            if (type.IsAbstract)
            {
                Dictionary<Type, Type[]> typeMap = BuildTypeHierarchy();
                Type[] subtypes;                
                if (typeMap.TryGetValue(type, out subtypes))
                {
                    foreach (Type subtype in subtypes)
                    {
                        if (!HasOverrideHashCode(subtype))
                        {
                            return false;
                        }
                    }
                }
            }
            MethodInfo minfo = type.GetMethod("GetHashCode");
            if (minfo.DeclaringType != type)
            {
                return false;
            }
            if (type.IsGenericType)
            {
                Type[] typeArgs = type.GetGenericArguments();
                foreach (Type targ in typeArgs)
                {
                    if (!HasOverrideHashCode(targ))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        internal static bool IsTypeSerializable(Type type)
        {
            return (!type.IsPointer &&
                    type != typeof(IntPtr) &&
                    !typeof(System.Delegate).IsAssignableFrom(type));
        }

        internal static bool IsFieldSerialized(FieldInfo finfo)
        {
            return (finfo.Attributes & FieldAttributes.NotSerialized) != FieldAttributes.NotSerialized;
        }
        
        internal static Type GetNonserializable(object obj)
        {
            return GetNonserializable(obj, new HashSet<object>(new ReferenceEqualityComparer<object>()));
        }
        
        internal static Type GetNonserializable(object obj, HashSet<object> seen)
        {
            if (obj == null || seen.Contains(obj))
            {
                return null;
            }

            Type type = obj.GetType();
            if (type.IsPrimitive)
            {
                return null;
            }
            if (!type.IsSerializable)
            {
                return type;
            }
            Type[] argTypes = new Type[] { typeof(SerializationInfo), typeof(StreamingContext) };
            if (type.GetMethod("GetObjectData", argTypes) != null)
            {
                return null;
            }
            if (type.IsArray)
            {
                return GetTypeNonserializable(type);
            }
            seen.Add(obj);
            FieldInfo[] fields = GetAllFields(type);
            foreach (FieldInfo finfo in fields)
            {
                if ((finfo.Attributes & FieldAttributes.NotSerialized) != FieldAttributes.NotSerialized)
                {
                    object fval = finfo.GetValue(obj);
                    if (!(fval is Pointer))
                    {
                        type = GetNonserializable(fval, seen);
                        if (type != null)
                        {
                            return type;
                        }
                    }
                }
            }
            return null;
        }

        internal static Type GetTypeNonserializable(Type type)
        {
            return GetTypeNonserializable(type, new HashSet<Type>());
        }

        internal static Type GetTypeNonserializable(Type type, HashSet<Type> seen)
        {
            while (type.IsArray)
            {
                type = type.GetElementType();
            }
            if (type.IsPrimitive || seen.Contains(type))
            {
                return null;
            }
            if (!type.IsSerializable)
            {
                return type;
            }
            Type[] argTypes = new Type[] { typeof(SerializationInfo), typeof(StreamingContext) };
            if (type.GetMethod("GetObjectData", argTypes) != null)
            {
                return null;
            }

            seen.Add(type);
            FieldInfo[] fields = GetAllFields(type);
            foreach (FieldInfo finfo in fields)
            {
                if ((finfo.Attributes & FieldAttributes.NotSerialized) != FieldAttributes.NotSerialized)
                {
                    if (finfo.FieldType != typeof(System.Reflection.Pointer))
                    {
                        type = GetTypeNonserializable(finfo.FieldType, seen);
                        if (type != null)
                        {
                            return type;
                        }
                    }
                }
            }
            return null;
        }

        internal static bool IsQueryOperatorCall(MethodCallExpression expression)
        {
            Type declType = expression.Method.DeclaringType;
            return (declType == typeof(System.Linq.Enumerable) ||
                    declType == typeof(System.Linq.Queryable) ||
                    declType == typeof(Microsoft.Research.DryadLinq.DryadLinqQueryable));
        }

        internal static FieldInfo[] GetAllFields(Type type)
        {
            List<FieldInfo> res = new List<FieldInfo>();
            while (type != typeof(object) && type != null)
            {
                FieldInfo[] fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                foreach (FieldInfo finfo in fields)
                {
                    if (finfo.DeclaringType == type)
                    {
                        res.Add(finfo);
                    }
                }
                type = type.BaseType;
            }
            return res.ToArray();
        }

        internal static bool HasFieldOfNonPublicType(Type type)
        {
            FieldInfo[] fields = GetAllFields(type);
            foreach (FieldInfo finfo in fields)
            {
                if (!finfo.FieldType.IsPublic && !finfo.FieldType.IsNestedPublic)
                {
                    return true;
                }
            }
            return false;
        }


        // Checks whether the given type contains a field that refers to itself. Fields will be traversed recursively, and any field that referes back to 
        // the original type (either as a direct reference, or as an array reference) will cause the check to return true. Static fields are skipped.
        //
        // However types of the fields traversed will not be checked for circularity themselves. This is meant to allow the user to handle a circular UDT
        // with a custom serializer, and still rely on autoserialization for types that contain it. 
        // See last example below for this case.. B is circular, and A contains B. If B has a custom serializer, A can still be autoserialized.
        //
        // But such "contained circularity" (e.g. B or C) doesn't escape the check in case there are no custom serializers involved, 
        // because codegen has to call IsCircular for each of those contained circular types before generating autoserializaton code for them.
        // 
        // Here are some examples:
        //
        //  class A { A m_f1;}      => returns TRUE for class A
        //  class A { A[] m_f1;}    => returns TRUE for class A
        //  class A { B m_f1;} class B { A m_f2;}                       => returns TRUE for both class A and class B (both are second level circular)
        //  class A { B m_f1;} class B { C m_f2;} class C { B m_f3;}    => returns FALSE for class A (as neither B nor C contain references to A)
        //                                                              => but returns TRUE for both class B and class C (both are second level circular)
        internal static bool IsCircularType(Type type)
        {
            return DoCircularTypeCheck(type, type, null);
        }

        // Returns true if "typeToCheck" contains a direct reference to "parentType", or an array of it
        // The hashset visitedTypes is meant to prevent multiple visits to the same type within this recursion (allocated on demand)
        private static bool DoCircularTypeCheck(Type typeToCheck, Type parentType, HashSet<Type> visitedTypes)
        {
            if (visitedTypes == null)
            {
                visitedTypes = new HashSet<Type>();
            }

            // if typeToCheck is in visitedTypes it means either we visited this type and haven't encountered the parent, or a visit is in progress
            // In both cases it's OK to return false. If a visit is already in progress and typeToCheck is actually an offender, it will be caught in that callstack level which is performing the visit.
            if (visitedTypes.Contains(typeToCheck))
            {
                return false;
            }

            // Before starting to visit all fields of "typeToCheck", we need to mark it as already visited to guard against infinite recursion.
            visitedTypes.Add(typeToCheck);

            foreach (FieldInfo fi in typeToCheck.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            {
                Type fieldType = fi.FieldType;

                // if this field is a reference to the parentType it means parentType is circular. Return true and unwind everyone.
                if (fieldType == parentType ||
                    (fieldType.IsArray && fieldType.GetElementType() == parentType) ||
                    DoCircularTypeCheck(fieldType, parentType, visitedTypes) )    // Otherwise recurse for this field's type, keeping the same parentType because
                                                                                  // we are only interested in whether fieldType is circular wrt parentType (we don't want to check whether fieldType is circular wrt itself)
                {
                    return true;
                }
            }

            return false;
        }
                
        // Assume: t1.GetGenericTypeDefinition() == t2.GetGenericTypeDefinition()
        internal static bool TypeMatch(Type t1, Type t2, Dictionary<Type, Type> map)
        {
            Type[] typeArgs1 = t1.GetGenericArguments();
            Type[] typeArgs2 = t2.GetGenericArguments();

            if (typeArgs1.Length != typeArgs2.Length)
            {
                return false;
            }
            for (int i = 0; i < typeArgs1.Length; i++)
            {
                if (typeArgs1[i].IsGenericParameter)
                {
                    Type t;
                    if (map.TryGetValue(typeArgs1[i], out t))
                    {
                        if (t != typeArgs2[i]) return false;
                    }
                    else
                    {
                        map[typeArgs1[i]] = typeArgs2[i];
                    }
                }
                else if (typeArgs1[i].ContainsGenericParameters)
                {
                    bool matched = TypeMatch(typeArgs1[i], typeArgs2[i], map);
                    if (!matched) return false;
                }
                else if (typeArgs1[i] != typeArgs2[i])
                {
                    return false;
                }
            }
            return true;
        }

        // Assume: type.IsGenericType
        internal static Type MakeGenericInstance(Type type, Dictionary<Type, Type> map)
        {
            Type[] typeArgs = type.GetGenericArguments();
            Type[] args = new Type[typeArgs.Length];
            for (int i = 0; i < args.Length; i++)
            {
                if (typeArgs[i].IsGenericParameter)
                {
                    Type t;
                    if (map.TryGetValue(typeArgs[i], out t))
                    {
                        args[i] = t;
                    }
                    else
                    {
                        args[i] = typeArgs[i];
                    }
                }
                else if (typeArgs[i].ContainsGenericParameters)
                {
                    args[i] = MakeGenericInstance(typeArgs[i], map);
                }
                else
                {
                    args[i] = typeArgs[i];
                }
            }

            // kluggy, need to rewrite.
            try
            {
                return type.GetGenericTypeDefinition().MakeGenericType(args);
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        internal static MethodBase GetBaseMethod(Type baseType, MethodBase method)
        {
            if (method.IsStatic || method.IsConstructor)
            {
                return (method.DeclaringType == baseType) ? method : null;
            }
            Type[] argTypes = new Type[method.GetParameters().Length];
            for (int i = 0; i < argTypes.Length; i++)
            {
                argTypes[i] = method.GetParameters()[i].ParameterType;
            }

            MethodInfo[] minfos = baseType.GetMethods(BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance);
            foreach (MethodInfo minfo in minfos)
            {
                ParameterInfo[] mparams = minfo.GetParameters();
                if (minfo.Name == method.Name && mparams.Length == argTypes.Length)
                {
                    bool found = true;
                    for (int i = 0; i < argTypes.Length; i++)
                    {
                        if (argTypes[i].IsGenericParameter)
                        {
                            if (!mparams[i].ParameterType.IsGenericParameter) return null;
                        }
                        else if (argTypes[i] != mparams[i].ParameterType)
                        {
                            found = false;
                            break;
                        }
                    }
                    if (found) return minfo;
                }
            }
            return null;
        }

        internal static List<MethodBase> GetAllOverrides(Type baseType, MethodBase method, Dictionary<Type, Type[]> typeMap)
        {
            List<MethodBase> methods = new List<MethodBase>();
            GetAllOverrides(baseType, method, typeMap, methods);
            return methods;
        }

        private static void GetAllOverrides(Type baseType, MethodBase method, Dictionary<Type, Type[]> typeMap, List<MethodBase> methods)
        {
            if (method.IsStatic || method.IsFinal || method.IsConstructor)
            {
                return;
            }

            Type type = baseType;
            if (type.IsGenericType)
            {
                type = type.GetGenericTypeDefinition();
            }
            Type[] subtypes = null;
            if (typeMap.TryGetValue(type, out subtypes))
            {
                Type[] argTypes = new Type[method.GetParameters().Length];
                for (int i = 0; i < argTypes.Length; i++)
                {
                    argTypes[i] = method.GetParameters()[i].ParameterType;
                }
                Type[] genericArgTypes = method.GetGenericArguments();
                foreach (Type subtype in subtypes)
                {
                    // TBD: We could make subtype more precise by unifying with baseType
                    MethodInfo[] minfos = subtype.GetMethods(BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance|BindingFlags.DeclaredOnly);
                    foreach (MethodInfo minfo in minfos)
                    {
                        if (minfo.Name == method.Name && minfo.IsVirtual)
                        {
                            MethodInfo minfo1 = MatchAndInstantiate(minfo, argTypes, genericArgTypes);
                            if (minfo1 != null)
                            {
                                methods.Add(minfo1);
                                GetAllOverrides(subtype, minfo1, typeMap, methods);
                            }
                        }
                    }
                }
            }
        }

        private static MethodInfo MatchAndInstantiate(MethodInfo minfo, Type[] argTypes, Type[] genericArgTypes)
        {
            ParameterInfo[] mParams = minfo.GetParameters();
            if (mParams.Length != argTypes.Length)
            {
                return null;
            }
            if (minfo.IsGenericMethod)
            {
                Type[] genericArgs = minfo.GetGenericArguments();
                if (genericArgs.Length != genericArgTypes.Length)
                {
                    return null;
                }
                if (!minfo.IsGenericMethodDefinition)
                {
                    bool hasGenericParameter = false;
                    foreach (Type arg in genericArgs)
                    {
                        if (arg.IsGenericParameter)
                        {
                            hasGenericParameter = true;
                            break;
                        }
                    }
                    if (hasGenericParameter)
                    {
                        minfo = minfo.GetGenericMethodDefinition();
                    }
                }

                // kluggy, need to rewrite.
                try
                {
                    minfo = minfo.MakeGenericMethod(genericArgTypes);
                }
                catch (ArgumentException)
                {
                    return null;
                }
                mParams = minfo.GetParameters();
            }
            for (int i = 0; i < argTypes.Length; i++)
            {
                Type paramType = mParams[i].ParameterType;
                if (paramType.IsGenericParameter)
                {
                    if (!argTypes[i].IsGenericParameter)
                    {
                        return null;
                    }
                }
                else if (paramType != argTypes[i])
                {
                    return null;
                }
            }
            return minfo;
        }

        internal static Type FindConstrainedType(Type type)
        {
            if (type.IsInterface) return null;
            if (type.IsGenericParameter)
            {
                Type[] constraints = type.GetGenericParameterConstraints();
                foreach (Type ctype in constraints)
                {
                    Type constrainedType = FindConstrainedType(ctype);
                    if (constrainedType != null) return constrainedType;
                }
            }
            return type;
        }

        public static bool IsBlittable(Type type)
        {
            if (type.IsPrimitive) return true;
            if (!type.IsValueType || !type.IsLayoutSequential || type.IsGenericType)
            {
                return false;
            }

            FieldInfo[] finfos = TypeSystem.GetAllFields(type);
            foreach (FieldInfo finfo in finfos)
            {
                if (!IsBlittable(finfo.FieldType))
                {
                    return false;
                }
            }
            return true;
        }

        // A hack to detect anonymous types
        internal static bool IsAnonymousType(Type type)
        {
            return (Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false) &&
                    type.IsGenericType && type.Name.Contains("AnonymousType") &&
                    (type.Name.StartsWith("<>", StringComparison.Ordinal) || type.Name.StartsWith("VB$", StringComparison.Ordinal)) &&
                    (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic);
        }

        // A hack to detect transparent identifiers
        internal static bool IsTransparentIdentifier(string name)
        {
            return name.StartsWith("<>h__TransparentIdentifier", StringComparison.Ordinal);
        }

        // A hack to detect backing field names
        internal static bool IsBackingField(string fieldName)
        {
            return fieldName.StartsWith("<", StringComparison.Ordinal) && fieldName.Contains("BackingField");
        }

        internal static bool ContainsAnonymousType(IEnumerable<Type> types)
        {
            foreach (Type type in types)
            {
                if (IsAnonymousType(type)) return true;
                if (type.IsGenericType &&
                    ContainsAnonymousType(type.GetGenericArguments()))
                {
                    return true;
                }
            }
            return false;
        }

        internal static bool IsTypeOrAnyGenericParamsAnonymous(Type type)
        {
            if (IsAnonymousType(type))
            {
                return true;
            }
            if (type.IsGenericType)
            {
                foreach (Type typeArg in type.GetGenericArguments())
                {
                    if (IsTypeOrAnyGenericParamsAnonymous(typeArg)) return true;
                }
            }
            return false;
        }

        internal static string FieldName(string fieldName)
        {
            if (!IsBackingField(fieldName))
            {
                return fieldName;
            }
            int idx = fieldName.IndexOf(">", StringComparison.Ordinal);
            if (idx == -1) idx = fieldName.Length;
            return fieldName.Substring(1, idx - 1);
        }
    }
}
