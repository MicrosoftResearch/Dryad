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
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    /// <summary>
    /// The Nullable attribute specifies if a field is nullable. The information is used by DryadLINQ 
    /// serialization. DryadLINQ serialization by default treats all fields not nullable. 
    /// </summary>
    [AttributeUsage(AttributeTargets.Field|AttributeTargets.Property|AttributeTargets.Class|AttributeTargets.Method, AllowMultiple = false)]
    public sealed class NullableAttribute : Attribute
    {
        private bool m_canBeNull;

        /// <summary>
        /// Initializes an instance of NullableAttribute.
        /// </summary>
        /// <param name="canBeNull">true iff the target of the attribute is nullable</param>
        public NullableAttribute(bool canBeNull)
        {
            this.m_canBeNull = canBeNull;
        }

        /// <summary>
        /// Determines if the target of this attribute is nullable.
        /// </summary>
        public bool CanBeNull
        {
            get { return this.m_canBeNull; }
        }
    }

    [AttributeUsage(AttributeTargets.Method|AttributeTargets.Constructor, AllowMultiple = true)]
    internal sealed class FieldMappingAttribute : Attribute
    {
        private string m_source;
        private string m_destination;

        public FieldMappingAttribute(string src, string dest)
        {
            this.m_source = src;
            this.m_destination = dest;
        }

        public string Source
        {
            get { return this.m_source; }
        }
        
        public string Destination
        {
            get { return this.m_destination; }
        }
    }

    [AttributeUsage(AttributeTargets.Class|AttributeTargets.Interface, AllowMultiple = false)]
    internal sealed class AutoTypeInferenceAttribute : Attribute
    {
        public AutoTypeInferenceAttribute()
        {
        }
    }

    [AttributeUsage(AttributeTargets.Method|AttributeTargets.Constructor, AllowMultiple = false)]
    internal sealed class DistinctAttribute : Attribute
    {
        private bool m_mustBeDistinct;
        private string m_comparer;
        
        public DistinctAttribute()
        {
            this.m_mustBeDistinct = false;
            this.m_comparer = null;
        }

        public DistinctAttribute(string comparer)
        {
            this.m_mustBeDistinct = false;
            this.m_comparer = comparer;
        }

        public bool MustBeDistinct
        {
            get { return this.m_mustBeDistinct; }
            set { this.m_mustBeDistinct = value; }
        }

        public object Comparer
        {
            get {
                if (this.m_comparer == null) return null;
                object val = TypeSystem.GetFieldValue(this.m_comparer);
                if (val == null)
                {
                    throw new DryadLinqException(DryadLinqErrorCode.DistinctAttributeComparerNotDefined,
                                                 String.Format(SR.DistinctAttributeComparerNotDefined, this.m_comparer));
                }
                return val;
            }
        }
    }

    /// <summary>
    /// The Resource attribute is used to specify the computation cost of a user defined 
    /// function (UDF). IsStateful asserts that the function is stateful; IsExpensive 
    /// asserts that the function is expensive to compute. The information is useful in 
    /// generating better execution plan. For example, expensive associative aggregation
    /// functions enables the use of multiple aggregation layers.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class ResourceAttribute : Attribute
    {
        private bool m_isStateful;
        private bool m_isExpensive;

        /// <summary>
        /// Initializes an instance of the Resource attribute. The default value of
        /// IsStateful is true; the default value of IsExpensive is false.
        /// </summary>
        public ResourceAttribute()
        {
            this.m_isStateful = true;
            this.m_isExpensive = false;
        }

        /// <summary>
        /// Gets and sets the IsStateful flag.
        /// </summary>
        public bool IsStateful
        {
            get { return this.m_isStateful; }
            set { this.m_isStateful = value; }
        }

        /// <summary>
        /// Gets and sets the IsExpensive flag.
        /// </summary>
        public bool IsExpensive
        {
            get { return this.m_isExpensive; }
            set { this.m_isExpensive = value; }
        }        
    }

    /// <summary>
    /// Indicates that a method can be decomposed to multiple methods. The argument to the 
    /// constructor must be of type IDecomposable. The computation of the method annotated
    /// by this attribute can be decomposed to a sequence of calls to the Seed, Accumulate,
    /// RecursiveAccumulate methods and a FinalReduce.
    /// </summary>
    /// <remarks>
    /// If a method is decomposable, a user can annotate it with this attribute. This enables
    /// DryadLINQ to perform a generalized "combiner" optimization.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class DecomposableAttribute : Attribute
    {
        private Type m_decompositionType;

        /// <summary>
        /// Initializes an instance of DecomposableAttribute. The argument is a type that implements
        /// <see cref="IDecomposable{TSource, TAccumulate, TResult}"/>.
        /// </summary>
        /// <param name="decompositionType">A type that implements IDecomposable{TSource, TAccumulate, TResult}</param>
        public DecomposableAttribute(Type decompositionType)
        {
            m_decompositionType = decompositionType;
        }

        /// <summary>
        /// A type that implements IDecomposable{TSource, TAccumulate, TResult} where 
        /// TSource is the element type of the input, TAccumulate is the element type
        /// of an intermediate dataset, and TResult is the output type of the method
        /// annotated by this attribute.
        /// </summary>
        public Type DecompositionType
        {
            get { return m_decompositionType; }
        }
    }

    /// <summary>
    /// Indicates that a method is an associative aggregation method. The argument to the 
    /// constructor must be of type IAssociative. The computation of the method annotated
    /// by this attribute can be decomposed to a sequence of calls to the Seed and 
    /// RecursiveAccumulate methods.
    /// </summary>
    /// <remarks>
    /// If a method is associative, a user can annotate it with this attribute. This enables
    /// DryadLINQ to perform the "combiner" optimization.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class AssociativeAttribute : Attribute
    {
        private Type m_associativeType;

        /// <summary>
        /// Initializes an instance of AssociativeAttribute. The argument is a type that implements
        /// <see cref="IAssociative{T}"/>.
        /// </summary>
        /// <remarks>
        /// During aggregation, the recursiveAccumulator will be used to aggregate items arising 
        /// from the main aggregation.
        /// </remarks>
        /// <param name="associativeType">A type that implements IAssociative{T}, where T
        /// is the output type of the method annotated by this attribute.</param>
        public AssociativeAttribute(Type associativeType)
        {
            this.m_associativeType = associativeType;
        }

        /// <summary>
        /// A type that implements IAssociative{T} where T is the output type of methods
        /// that are decorated with this attribute.
        /// </summary>
        public Type AssociativeType
        {
            get { return this.m_associativeType; }
        }
    }

    /// <summary>
    /// Provides a user-defined serialization method for a .NET type. 
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited=false)]
    public sealed class CustomDryadLinqSerializerAttribute : Attribute
    {
        /// <summary>
        /// Initializes an instance of CustomDryadLinqSerializer attribute.
        /// </summary>
        /// <param name="serializerType">A type that implements IDryadLinqSerializer{T}, where T
        /// is the .NET type to be serialized.</param>
        public CustomDryadLinqSerializerAttribute(Type serializerType)
        {
            this.SerializerType = serializerType;

            // We need to make sure serializerType implements IDryadLinqSerializer<T>
            // However we will defer that check until DryadCodeGen.FindCustomSerializerType(), because
            //  1) we don't have access to <T> here but it's available at code gen time, and 
            //  2) because an exception coming from the attribute ctor leads to an obscure failure.
        }

        /// <summary>
        /// Gets and sets the type object for serialization.
        /// </summary>
        public Type SerializerType { get; private set; }
    }

    internal static class AttributeSystem
    {
        private static Dictionary<Expression, Attribute[]> attribMap = new Dictionary<Expression, Attribute[]>();

        internal static void Add(LambdaExpression func, Attribute attrib)
        {
            Attribute[] attribs;
            if (attribMap.TryGetValue(func, out attribs))
            {
                Attribute[] oldAttribs = attribs;
                attribs = new Attribute[oldAttribs.Length+1];
                Array.Copy(oldAttribs, attribs, oldAttribs.Length);
                attribs[oldAttribs.Length] = attrib;
                attribMap.Remove(func);
            }
            else
            {
                attribs = new Attribute[] { attrib };
            }
            attribMap[func] = attribs;
        }

        private static Attribute[] Get(LambdaExpression func, Type attribType)
        {
            Attribute[] attribs;
            attribMap.TryGetValue(func, out attribs);
            if (attribs != null)
            {
                ArrayList alist = new ArrayList();
                foreach (var x in attribs)
                {
                    if (x.GetType() == attribType)
                    {
                        alist.Add(x);
                    }
                }
                attribs = (Attribute[])alist.ToArray(attribType);
            }
            return attribs;
        }

        internal static Attribute[] GetAttribs(LambdaExpression func, Type attribType)
        {
            Attribute[] attribs1 = AttributeSystem.Get(func, attribType);
            Attribute[] attribs2 = null;
            if (func.Body is MethodCallExpression)
            {
                MethodCallExpression expr = (MethodCallExpression)func.Body;
                attribs2 = Attribute.GetCustomAttributes(expr.Method, attribType);
            }
            else if (func.Body is NewExpression && ((NewExpression)func.Body).Constructor != null)
            {
                NewExpression expr = (NewExpression)func.Body;
                attribs2 = Attribute.GetCustomAttributes(expr.Constructor, attribType);
            }
            else if (func.Body is BinaryExpression)
            {
                BinaryExpression expr = (BinaryExpression)func.Body;
                if (expr.Method != null)
                {
                    attribs2 = Attribute.GetCustomAttributes(expr.Method, attribType);
                }
            }
            else if (func.Body is InvocationExpression)
            {
                InvocationExpression expr = (InvocationExpression)func.Body;
                if (expr.Expression is LambdaExpression)
                {
                    attribs2 = GetAttribs((LambdaExpression)expr.Expression, attribType);
                }
            }
            if (attribs1 == null)
            {
                return attribs2;
            }
            if (attribs2 == null)
            {
                return attribs1;
            }
            ArrayList alist = new ArrayList();
            foreach (var x in attribs1)
            {
                alist.Add(x);
            }
            foreach (var x in attribs2)
            {
                alist.Add(x);
            }
            Attribute[] attribs = (Attribute[])alist.ToArray(attribType);
            return attribs;
        }

        internal static Attribute GetAttrib(Expression expr, Type attribType)
        {
            Attribute[] attribs = null;
            if (expr is MethodCallExpression)
            {
                attribs = Attribute.GetCustomAttributes(((MethodCallExpression)expr).Method, attribType);
            }
            else if (expr is NewExpression && ((NewExpression) expr).Constructor != null)
            {
                attribs = Attribute.GetCustomAttributes(((NewExpression)expr).Constructor, attribType);
            }
            else if (expr is LambdaExpression)
            {
                attribs = GetAttribs((LambdaExpression)expr, attribType);
            }

            if (attribs == null || attribs.Length == 0) return null;
            return attribs[0];
        }
        
        internal static DecomposableAttribute GetDecomposableAttrib(Expression expr)
        {
            return (DecomposableAttribute)GetAttrib(expr, typeof(DecomposableAttribute));
        }

        internal static AssociativeAttribute GetAssociativeAttrib(Expression expr)
        {
            return (AssociativeAttribute)GetAttrib(expr, typeof(AssociativeAttribute));
        }
        
        internal static ResourceAttribute GetResourceAttrib(LambdaExpression func)
        {
            return (ResourceAttribute)GetAttrib(func, typeof(ResourceAttribute));
        }

        internal static FieldMappingAttribute[] GetFieldMappingAttribs(LambdaExpression func)
        {
            Attribute[] a = GetAttribs(func, typeof(FieldMappingAttribute));
            if (a == null || a.Length == 0) return null;
            return (FieldMappingAttribute[])a;
        }

        internal static bool DoAutoTypeInference(DryadLinqContext context, Type type)
        {
            if (!StaticConfig.AllowAutoTypeInference) return false;
            object[] a = type.GetCustomAttributes(typeof(AutoTypeInferenceAttribute), true);
            return (a.Length != 0);
        }

        internal static DistinctAttribute GetDistinctAttrib(LambdaExpression func)
        {
            return (DistinctAttribute)GetAttrib(func, typeof(DistinctAttribute));
        }

        internal static bool FieldCanBeNull(FieldInfo finfo)
        {
            if (finfo == null || finfo.FieldType.IsValueType) return false;

            object[] attribs = finfo.GetCustomAttributes(typeof(NullableAttribute), true);
            if (attribs.Length == 0)
            {
                return StaticConfig.AllowNullFields;
            }
            return ((NullableAttribute)attribs[0]).CanBeNull;
        }

        internal static bool RecordCanBeNull(DryadLinqContext context, Type type)
        {
            if (type == null || type.IsValueType) return false;

            object[] attribs = type.GetCustomAttributes(typeof(NullableAttribute), true);
            if (attribs.Length == 0)
            {
                return StaticConfig.AllowNullRecords;
            }
            return ((NullableAttribute)attribs[0]).CanBeNull;
        }
    }
}
