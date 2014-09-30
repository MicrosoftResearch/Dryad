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
using System.IO;
using System.Reflection;
using System.Reflection.Emit;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

#pragma warning disable 1591

namespace Microsoft.Research.DryadLinq.Internal
{
    public delegate S GetObjFieldDelegate<T, S>(T obj);
    public delegate void SetObjFieldDelegate<T, S>(T obj, S value);

    public delegate S GetStructFieldDelegate<T, S>(out T obj);
    public delegate void SetStructFieldDelegate<T, S>(out T obj, S value);

    //this class is internal-public for Get ObjFieldDelegate etc.
    public static class CodeGenHelper
    {
        public static GetObjFieldDelegate<T, S> GetObjField<T, S>(string fname)
        {
            Type typeT = typeof(T);
            if (typeT.IsValueType)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             SR.Internal_CannotBeUsedForValueType);
            }
            FieldInfo finfo = typeT.GetField(fname, BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            if (finfo == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             String.Format(SR.TypeDoesNotContainRequestedField, typeT.Name, fname));
            }
            DynamicMethod dm = new DynamicMethod("GetObjField",
                                                 typeof(S),
                                                 new Type[] { typeT },
                                                 typeT,
                                                 true);
            ILGenerator ilgen = dm.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldfld, finfo);
            ilgen.Emit(OpCodes.Ret);

            return (GetObjFieldDelegate<T, S>)dm.CreateDelegate(typeof(GetObjFieldDelegate<T, S>));
        }

        public static SetObjFieldDelegate<T, S> SetObjField<T, S>(string fname)
        {
            Type typeT = typeof(T);
            if (typeT.IsValueType)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             SR.Internal_CannotBeUsedForValueType);
            }
            FieldInfo finfo = typeT.GetField(fname, BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            if (finfo == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             String.Format(SR.TypeDoesNotContainRequestedField, typeT.Name, fname));
            }            
            DynamicMethod dm = new DynamicMethod("SetObjField",
                                                 typeof(void),
                                                 new Type[] { typeT, typeof(S) },
                                                 typeT,
                                                 true);
            ILGenerator ilgen = dm.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldarg_1);
            ilgen.Emit(OpCodes.Stfld, finfo);
            ilgen.Emit(OpCodes.Ret);

            return (SetObjFieldDelegate<T, S>)dm.CreateDelegate(typeof(SetObjFieldDelegate<T, S>));
        }

        public static GetStructFieldDelegate<T, S> GetStructField<T, S>(string fname)
        {
            Type typeT = typeof(T);
            if (!typeT.IsValueType)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             SR.CannotBeUsedForReferenceType);
            }
            FieldInfo finfo = typeT.GetField(fname, BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            if (finfo == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             String.Format(SR.TypeDoesNotContainRequestedField, typeT.Name, fname));
            }            
            DynamicMethod dm = new DynamicMethod("GetStructField",
                                                 typeof(S),
                                                 new Type[] { typeT.MakeByRefType() },
                                                 typeT,
                                                 true);
            ILGenerator ilgen = dm.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldfld, finfo);
            ilgen.Emit(OpCodes.Ret);

            return (GetStructFieldDelegate<T, S>)dm.CreateDelegate(typeof(GetStructFieldDelegate<T, S>));
        }

        public static SetStructFieldDelegate<T, S> SetStructField<T, S>(string fname)
        {
            Type typeT = typeof(T);
            if (!typeT.IsValueType)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             SR.CannotBeUsedForReferenceType);
            }
            FieldInfo finfo = typeT.GetField(fname, BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic);
            if (finfo == null)
            {
                throw new DryadLinqException(DryadLinqErrorCode.Internal,
                                             String.Format(SR.TypeDoesNotContainRequestedField, typeT.Name, fname));
            }            
            DynamicMethod dm = new DynamicMethod("SetStructField",
                                                 typeof(void),
                                                 new Type[] { typeT.MakeByRefType(), typeof(S) },
                                                 typeT,
                                                 true);
            ILGenerator ilgen = dm.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldarg_1);
            ilgen.Emit(OpCodes.Stfld, finfo);
            ilgen.Emit(OpCodes.Ret);

            return (SetStructFieldDelegate<T, S>)dm.CreateDelegate(typeof(SetStructFieldDelegate<T, S>));
        }
    }
}
