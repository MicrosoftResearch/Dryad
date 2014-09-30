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
using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;


namespace DryadLinqTests
{
    public class SerializationTests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" SerializationTests ");
            TestLog.Message(" **********************");

            context.LocalDebug = false;

            IQueryable<int> source = DataGenerator.GetSimpleFileSets(context);

            var tests = new Dictionary<string, Action>()
              {
                  {"UDT_Undecorated", () => 
                    SerializationTests.TestUDT<UDT_Undecorated>(context, source, "Basic UDT with no attribute. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeNotSerializable")) },

                  {"UDT_MarkedSerializable", () => 
                    SerializationTests.TestUDT<UDT_MarkedSerializable>(context, source, "Basic UDT which is marked as serializable.", null, 0)},
                    
                  {"UDT_StaticSerializer", () => 
                    SerializationTests.TestUDT<UDT_StaticSerializer>(context, source, "Basic UDT with old stype static serializers. Should be rejected.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeNotSerializable")) },

                  {"UDT_SelfCustomSerializer", () => 
                    SerializationTests.TestUDT<UDT_SelfCustomSerializer>(context, source, "UDT implementing custom serializer for itself.", null, 0)},

                  {"UDT_ExternalCustomSerializer", () => 
                    SerializationTests.TestUDT<UDT_ExternalCustomSerializer>(context, source, "UDT with a custom serializer", null, 0)},

                  {"UDT_BadCustomSerializerType1", () => 
                    SerializationTests.TestUDT<UDT_BadCustomSerializerType1>(context, source, "UDT decorated with an invalid custom serializer type.", typeof(DryadLinqException), 0)},

                  {"UDT_BadCustomSerializerType2", () => 
                    SerializationTests.TestUDT<UDT_BadCustomSerializerType2>(context, source, "UDT decorated with another UDT's custom serializer.", typeof(DryadLinqException), 0)},

                  {"UDT_BaseType", () => 
                    SerializationTests.TestUDT<UDT_BaseType>(context, source, "UDT with subtypes. Should be rejected", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleSubtypes"))},

                  {"UDT_DerivedType", () => 
                    SerializationTests.TestUDT<UDT_DerivedType>(context, source, "UDT with a base type. Should be rejected", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleDerivedtypes"))},

                  {"UDT_FieldOfNonPublicType", () => 
                    SerializationTests.TestUDT<UDT_FieldOfNonPublicType>(context, source, "UDT with a field of a non-public type. Should be rejected", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("UDTHasFieldOfNonPublicType"))},

                  {"UDT_Nested_OuterSerializableInnerNotSerializable", () => 
                    SerializationTests.TestUDT<UDT_Nested_OuterSerializableInnerNotSerializable>(context, source, "Nested UDT, outer type Serializable, inner type not Serializable. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeNotSerializable"))},

                  {"UDT_Nested_OuterNotSerializableInnerSerializable", () => 
                    SerializationTests.TestUDT<UDT_Nested_OuterNotSerializableInnerSerializable>(context, source, "Nested UDT, outer type not Serializable, inner type Serializable. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeNotSerializable"))},

                  {"UDT_Nested_InnerAndOuterSerializable", () => 
                    SerializationTests.TestUDT<UDT_Nested_InnerAndOuterSerializable>(context, source, "Nested UDT, both outer and innter types marked Serializable.", null, 0)},

                  {"UDT_Nested_InnerEnum_InnerAndOuterSerializable", () => 
                    SerializationTests.TestUDT<UDT_Nested_InnerEnum_InnerAndOuterSerializable>(context, source, "Nested UDT, inner enum, both outer and innter types marked Serializable.", null, 0)},

                  {"int?", () => 
                    SerializationTests.TestUDT<int?>(context, source, "Nullable type as the UDT. Should be autoserialized.", null, 0)},

                  {"UDT_FirstLevelCircular", () => 
                    SerializationTests.TestUDT<UDT_FirstLevelCircular>(context, source, "Circular UDT. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleCircularTypes"))},

                  {"UDT_FirstLevelCircularArrayRef", () => 
                    SerializationTests.TestUDT<UDT_FirstLevelCircularArrayRef>(context, source, "Circular UDT with array reference to self. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleCircularTypes"))},

                  {"UDT_SecondLevelCircular", () => 
                    SerializationTests.TestUDT<UDT_SecondLevelCircular>(context, source, "Circular UDT with indirect reference to self. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleCircularTypes"))},

                  {"UDT_CircularTypeWithCustomSerializer", () => 
                    SerializationTests.TestUDT<UDT_CircularTypeWithCustomSerializer>(context, source, "Circular UDT with a custom serializer. Should not be rejected.", null, 0)},

                  {"UDT_TypeContainingCustomSerializedCircularType", () => 
                    SerializationTests.TestUDT<UDT_TypeContainingCustomSerializedCircularType>(context, source, "An UDT containing a field of a circular UDT with a custom serializer. Should be autoserialized.", null, 0)},

                  {"UDT_ObjectField", () => 
                    SerializationTests.TestUDT<UDT_ObjectField>(context, source, "UDT with a field of type System.Object. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"UDT_ObjectArrayField", () => 
                    SerializationTests.TestUDT<UDT_ObjectArrayField>(context, source, "UDT with a field of type System.Object[]. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"UDT_ObjectListField", () => 
                    SerializationTests.TestUDT<UDT_ObjectListField>(context, source, "UDT with a field of type List<object>. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"object", () => 
                    SerializationTests.TestUDT<object>(context, source, "UDT = System.Object. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"object[]", () => 
                    SerializationTests.TestUDT<object[]>(context, source, "UDT = System.Object[]. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"List<object>", () => 
                    SerializationTests.TestUDT<List<object>>(context, source, "UDT = List<object>. Should not be autoserialized.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("CannotHandleObjectFields"))},

                  {"UDT_EmptyType", () => 
                    SerializationTests.TestUDT<UDT_EmptyType>(context, source, "Empty UDT. Should be rejected.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeMustHaveDataMembers"))},

                  {"UDT_ObjectFieldAndCustomSerializer", () => 
                    SerializationTests.TestUDT<UDT_ObjectFieldAndCustomSerializer>(context, source, "UDT with an Object field, and a custom serializer. Should not be rejected.", null, 0)},

                  {"UDT_EmptyTypeWithCustomSerializer", () => 
                    SerializationTests.TestUDT<UDT_EmptyTypeWithCustomSerializer>(context, source, "Empty UDT with a custom serializer. Should be rejected.", typeof(DryadLinqException), ReflectionHelper.GetDryadLinqErrorCode("TypeMustHaveDataMembers"))},

                  {"UDT_PrivateFieldOfPublicType", () => 
                    SerializationTests.TestUDT<UDT_PrivateFieldOfPublicType>(context, source, "UDT with a private field of a public type. Should be autoserialized.", null, 0)},
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }

        }

        // the generic entry method used by most tests in this file
        public static bool TestUDT<TRecord>(DryadLinqContext context, IQueryable<int> source, string testMessage, Type expectedExceptionType, int expectedErrorCode)
        {
            string testName = testMessage;
            TestLog.TestStart(testName);

            bool passed = true;
            // first make sure SelectFunc knows how to handle the type.
            try
            {
                object o = SelectFunc<TRecord>(0);
            }
            catch
            {
                TestLog.Message(String.Format("SelectFunc doesn't support type {0}", typeof(TRecord).Name));
                passed &= false;
                goto Done;
            }

            try
            {
                var results = source.Select(x => SelectFunc<TRecord>(x));

                int count = 0;
                foreach (var r in results) count++;

                if (count == 0)
                {
                    TestLog.Message("...FAILED! No elements returned from query");
                    passed &= false;
                    goto Done;
                }
            }
            catch (Exception exp)
            {
                if (expectedExceptionType == null)
                {
                    TestLog.Message(String.Format("...FAILED! Caught {0} while none was expected: ", exp.GetType().Name));
                    passed &= false;
                    goto Done;
                }
                else if (expectedExceptionType != exp.GetType())
                {
                    TestLog.Message(String.Format("...FAILED! Caught {0} while while {1} was expected: {0}", exp.GetType().Name, expectedExceptionType.Name));
                    passed &= false;
                    goto Done;
                }
                else if (exp is DryadLinqException && expectedErrorCode != 0 && ((DryadLinqException)exp).ErrorCode != expectedErrorCode)
                {
                    TestLog.Message("...FAILED! Caught DryadLinqException but fault code is wrong.");
                    passed &= false;
                    goto Done;
                }

                TestLog.Message("....PASSED (caught correct exception)");
                goto Done;
            }

            if (expectedExceptionType == null)
            {
                TestLog.Message(".....PASSED");
            }
            else
            {
                TestLog.Message("...FAILED! No exception was thrown from query.");
                passed &= false;
            }

            Done:
            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static TRecord SelectFunc<TRecord>(int x)
        {
            if (typeof(TRecord) == typeof(object))
            {
                return (TRecord)(new object());
            }

            if (typeof(TRecord) == typeof(object[]))
            {
                return (TRecord)((object)new object[0]);
            }

            if (typeof(TRecord) == typeof(List<object>))
            {
                return (TRecord)((object)new List<object>());
            }

            if (typeof(TRecord) == typeof(UDT_Undecorated))
            {
                return (TRecord)((object)new UDT_Undecorated(x));
            }

            if (typeof(TRecord) == typeof(UDT_SelfCustomSerializer))
            {
                return (TRecord)((object)new UDT_SelfCustomSerializer(x));
            }

            if (typeof(TRecord) == typeof(UDT_MarkedSerializable))
            {
                return (TRecord)((object)new UDT_MarkedSerializable(x));
            }

            if (typeof(TRecord) == typeof(UDT_StaticSerializer))
            {
                return (TRecord)((object)new UDT_StaticSerializer(x));
            }

            if (typeof(TRecord) == typeof(UDT_ExternalCustomSerializer))
            {
                return (TRecord)((object)new UDT_ExternalCustomSerializer(x));
            }

            if (typeof(TRecord) == typeof(UDT_BadCustomSerializerType1))
            {
                return (TRecord)((object)new UDT_BadCustomSerializerType1(x));
            }

            if (typeof(TRecord) == typeof(UDT_BadCustomSerializerType2))
            {
                return (TRecord)((object)new UDT_BadCustomSerializerType2(x));
            }

            if (typeof(TRecord) == typeof(UDT_BaseType))
            {
                return (TRecord)((object)new UDT_BaseType(x));
            }

            if (typeof(TRecord) == typeof(UDT_DerivedType))
            {
                return (TRecord)((object)new UDT_DerivedType(x));
            }

            if (typeof(TRecord) == typeof(UDT_DerivedType))
            {
                return (TRecord)((object)new UDT_DerivedType(x));
            }

            if (typeof(TRecord) == typeof(UDT_FieldOfNonPublicType))
            {
                return (TRecord)((object)new UDT_FieldOfNonPublicType(x));
            }

            if (typeof(TRecord) == typeof(UDT_PrivateFieldOfPublicType))
            {
                return (TRecord)((object)new UDT_PrivateFieldOfPublicType(x));
            }

            if (typeof(TRecord) == typeof(UDT_Nested_OuterSerializableInnerNotSerializable))
            {
                return (TRecord)((object)new UDT_Nested_OuterSerializableInnerNotSerializable(x));
            }

            if (typeof(TRecord) == typeof(UDT_Nested_OuterNotSerializableInnerSerializable))
            {
                return (TRecord)((object)new UDT_Nested_OuterNotSerializableInnerSerializable(x));
            }

            if (typeof(TRecord) == typeof(UDT_Nested_InnerAndOuterSerializable))
            {
                return (TRecord)((object)new UDT_Nested_InnerAndOuterSerializable(x));
            }

            if (typeof(TRecord) == typeof(UDT_Nested_InnerEnum_InnerAndOuterSerializable))
            {
                return (TRecord)((object)new UDT_Nested_InnerEnum_InnerAndOuterSerializable(x));
            }

            if (typeof(TRecord) == typeof(int?))
            {
                return (TRecord)((object)((int?)x));
            }

            if (typeof(TRecord) == typeof(UDT_FirstLevelCircular))
            {
                return (TRecord)((object)new UDT_FirstLevelCircular(x));
            }

            if (typeof(TRecord) == typeof(UDT_FirstLevelCircularArrayRef))
            {
                return (TRecord)((object)new UDT_FirstLevelCircularArrayRef(x));
            }

            if (typeof(TRecord) == typeof(UDT_SecondLevelCircular))
            {
                return (TRecord)((object)new UDT_SecondLevelCircular(x));
            }

            if (typeof(TRecord) == typeof(UDT_CircularTypeWithCustomSerializer))
            {
                return (TRecord)((object)new UDT_CircularTypeWithCustomSerializer(x));
            }

            if (typeof(TRecord) == typeof(UDT_TypeContainingCustomSerializedCircularType))
            {
                return (TRecord)((object)new UDT_TypeContainingCustomSerializedCircularType(x));
            }

            if (typeof(TRecord) == typeof(UDT_ObjectField))
            {
                return (TRecord)((object)new UDT_ObjectField(x));
            }

            if (typeof(TRecord) == typeof(UDT_ObjectArrayField))
            {
                return (TRecord)((object)new UDT_ObjectArrayField(x));
            }

            if (typeof(TRecord) == typeof(UDT_ObjectListField))
            {
                return (TRecord)((object)new UDT_ObjectListField(x));
            }

            if (typeof(TRecord) == typeof(UDT_ObjectFieldAndCustomSerializer))
            {
                return (TRecord)((object)new UDT_ObjectFieldAndCustomSerializer(x));
            }

            if (typeof(TRecord) == typeof(UDT_EmptyType))
            {
                return (TRecord)((object)new UDT_EmptyType(x));
            }

            if (typeof(TRecord) == typeof(UDT_EmptyTypeWithCustomSerializer))
            {
                return (TRecord)((object)new UDT_EmptyTypeWithCustomSerializer(x));
            }

            throw new InvalidOperationException("Unrecognized TRecord");
        }

    }
}
