using Microsoft.Research.DryadLinq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DryadLinqTests
{
    //////////////////////////////////////////////////////////////////////
    //
    // Decorated and undecorated types
    //

    // A UDT with no attribute shouldn't autoserialize without an explicit [Serializable] attribute
    public class UDT_Undecorated
    {
        public UDT_Undecorated(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
        }

        public int m_field1;
        public int m_field2;
    }

    // UDT which is marked as serializable. This should be autoserialized
    [Serializable]
    public class UDT_MarkedSerializable
    {
        public UDT_MarkedSerializable(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
        }

        public int m_field1;
        public int m_field2;
    }

    // UDT that has the old style static serialization methods. This should be rejected.
    public class UDT_StaticSerializer
    {
        public UDT_StaticSerializer(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
        }

        public int m_field1;
        public int m_field2;

        public static UDT_StaticSerializer Read(DryadLinqBinaryReader reader)
        {
            var val = new UDT_StaticSerializer(0);
            val.m_field1 = reader.ReadInt32();
            val.m_field2 = reader.ReadInt32();

            return val;
        }

        public static void Write(DryadLinqBinaryWriter writer, UDT_StaticSerializer val)
        {
            writer.Write(val.m_field1);
            writer.Write(val.m_field2);
        }
    }

    //////////////////////////////////////////////////////////////////////
    //
    // UDTs with custom serializers
    //

    // UDT that has an attribute declaring itself as its custom serializer
    [CustomDryadLinqSerializer(typeof(UDT_SelfCustomSerializer))]
    public struct UDT_SelfCustomSerializer : IDryadLinqSerializer<UDT_SelfCustomSerializer>
    {
        public UDT_SelfCustomSerializer(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
        }

        public int m_field1;
        public int m_field2;

        public UDT_SelfCustomSerializer Read(DryadLinqBinaryReader reader)
        {
            var val = new UDT_SelfCustomSerializer(0);
            val.m_field1 = reader.ReadInt32();
            val.m_field2 = reader.ReadInt32();

            return val;
        }

        public void Write(DryadLinqBinaryWriter writer, UDT_SelfCustomSerializer val)
        {
            writer.Write(val.m_field1);
            writer.Write(val.m_field2);
        }
    }

    // UDT that declares another type as its custom serializer
    [CustomDryadLinqSerializer(typeof(CustomUDTSerializer))]
    public struct UDT_ExternalCustomSerializer
    {
        public UDT_ExternalCustomSerializer(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
        }

        public int m_field1;
        public int m_field2;
    }

    // this is the custom serializer for UDT_ExternalCustomSerializer
    public class CustomUDTSerializer : IDryadLinqSerializer<UDT_ExternalCustomSerializer>
    {
        public UDT_ExternalCustomSerializer Read(DryadLinqBinaryReader reader)
        {
            var val = new UDT_ExternalCustomSerializer(0);
            val.m_field1 = reader.ReadInt32();
            val.m_field2 = reader.ReadInt32();

            return val;
        }

        public void Write(DryadLinqBinaryWriter writer, UDT_ExternalCustomSerializer val)
        {
            writer.Write(val.m_field1);
            writer.Write(val.m_field2);
        }
    }

    // UDT with a CustomHpcSerializer attribute that points to an invalid type for the serializer
    [CustomDryadLinqSerializer(typeof(int))]
    public struct UDT_BadCustomSerializerType1
    {
        public UDT_BadCustomSerializerType1(int val)
        {
            m_field1 = val + 1;
        }

        public int m_field1;
    }

    // UDT with a CustomHpcSerializer attribute that points to a serializer type that targets a different UDT
    [CustomDryadLinqSerializer(typeof(CustomUDTSerializer))]
    public struct UDT_BadCustomSerializerType2
    {
        public UDT_BadCustomSerializerType2(int val)
        {
            m_field1 = val + 1;
        }

        public int m_field1;
    }


    //////////////////////////////////////////////////////////////////////
    //
    // Inheritance
    //

    // A UDT that has sub types. This should be rejected
    [Serializable]
    public class UDT_BaseType
    {
        public UDT_BaseType(int val)
        {
            m_baseTypeField = val + 42;
        }

        public int m_baseTypeField;
    }

    // A UDT that has derives from a type other than object. This should be rejected
    [Serializable]
    public class UDT_DerivedType : UDT_BaseType
    {
        public UDT_DerivedType(int val)
            : base(val)
        {
            m_derivedTypeField = val + 84;
        }

        public int m_derivedTypeField;
    }

    //////////////////////////////////////////////////////////////////////
    //
    // Field types
    //

    // A UDT with no data. This should be rejected
    [Serializable]
    public class UDT_EmptyType
    {
        public UDT_EmptyType(int val)
        {
        }
    }

    // A UDT with a public field of type System.Object. This should be rejected
    [Serializable]
    public class UDT_ObjectField
    {
        public UDT_ObjectField(int val)
        {
            m_intField = val + 84;
            m_objectField = null;
        }

        public int m_intField;
        public object m_objectField;
    }

    // A UDT with a public field of type System.Object[]. This should be rejected
    [Serializable]
    public class UDT_ObjectArrayField
    {
        public UDT_ObjectArrayField(int val)
        {
            m_intField = val + 84;
            m_objectArrayField = new object[1];
            m_objectArrayField[0] = null;
        }

        public int m_intField;
        public object[] m_objectArrayField;
    }

    // A UDT with a public field of type List<System.Object>. This should be rejected
    [Serializable]
    public class UDT_ObjectListField
    {
        public UDT_ObjectListField(int val)
        {
            m_intField = val + 84;
            m_objectListField = new List<object>();
            m_objectListField.Add(null);
        }

        public int m_intField;
        public List<object> m_objectListField;
    }

    // A UDT with an object field, and a custom serializer. This should not be rejected
    [CustomDryadLinqSerializer(typeof(UDT_ObjectFieldAndCustomSerializer))]
    public class UDT_ObjectFieldAndCustomSerializer : IDryadLinqSerializer<UDT_ObjectFieldAndCustomSerializer>
    {
        private object m_objectRecord = (object)"";

        // Required by CustomHpcSerializer
        public UDT_ObjectFieldAndCustomSerializer() { }

        public UDT_ObjectFieldAndCustomSerializer(int val)
        {
            m_objectRecord = (object)String.Format("{0}", val);
        }

        #region IHpcSerializer implementation

        public UDT_ObjectFieldAndCustomSerializer Read(DryadLinqBinaryReader reader)
        {
            string tmp = reader.ReadString();
            int val = Int32.Parse(tmp);

            return new UDT_ObjectFieldAndCustomSerializer(val);
        }

        public void Write(DryadLinqBinaryWriter writer, UDT_ObjectFieldAndCustomSerializer record)
        {
            writer.Write((string)record.m_objectRecord);
        }

        #endregion
    }

    // An empty UDT with a custom serializer. This should be rejected, because even though the user has control over serialization
    // we will encounter runtime problems if the custom serializer reads/writer 0 bytes. This behavior is simply to discourage empty CS code.
    [CustomDryadLinqSerializer(typeof(UDT_EmptyTypeWithCustomSerializer))]
    public class UDT_EmptyTypeWithCustomSerializer : IDryadLinqSerializer<UDT_EmptyTypeWithCustomSerializer>
    {
        // Required by CustomHpcSerializer
        public UDT_EmptyTypeWithCustomSerializer() { }

        public UDT_EmptyTypeWithCustomSerializer(int val)
        {
        }

        #region IHpcSerializer implementation

        public UDT_EmptyTypeWithCustomSerializer Read(DryadLinqBinaryReader reader)
        {
            return new UDT_EmptyTypeWithCustomSerializer(0);
        }

        public void Write(DryadLinqBinaryWriter writer, UDT_EmptyTypeWithCustomSerializer record)
        {
        }

        #endregion
    }

    //////////////////////////////////////////////////////////////////////
    //
    // Visibility
    //

    // UDT with a field of non-public type. We cannot handle these
    [Serializable]
    public class UDT_FieldOfNonPublicType
    {
        private enum SecretCodeLevel
        {
            Secret,
            SuperSecret,
        }

        public UDT_FieldOfNonPublicType(int val)
        {
            m_field = (SecretCodeLevel)val;
        }

        private SecretCodeLevel m_field;
    }

    // UDT with a private field of a public type. We do handle these using emitted IL code.
    [Serializable]
    public class UDT_PrivateFieldOfPublicType
    {
        public UDT_PrivateFieldOfPublicType(int val)
        {
            m_field = val + 1;
        }

        private int m_field;
    }

    //////////////////////////////////////////////////////////////////////
    //
    // Nesting
    //

    [Serializable]
    public class UDT_Nested_InnerAndOuterSerializable
    {
        public UDT_Nested_InnerAndOuterSerializable(int val)
        {
            m_field = val + 1;
            m_field2 = new NestedSerializable(val);
        }

        private int m_field;
        private NestedSerializable m_field2;

        [Serializable]
        public class NestedSerializable
        {
            public NestedSerializable(int val)
            {
                m_field = val / 2.0;
            }

            private double m_field;
        }
    }

    [Serializable]
    public class UDT_Nested_InnerEnum_InnerAndOuterSerializable
    {
        public UDT_Nested_InnerEnum_InnerAndOuterSerializable(int val)
        {
            m_field = val + 1;
            m_field2 = (NestedEnum)(val % 3);
        }

        private int m_field;
        private NestedEnum m_field2;

        //[Serializable]
        public enum NestedEnum
        {
            Foo = 0,
            Bar = 1,
            Baz = 2,
        }
    }

    [Serializable]
    public class UDT_Nested_OuterSerializableInnerNotSerializable
    {
        public UDT_Nested_OuterSerializableInnerNotSerializable(int val)
        {
            m_field = val + 1;
            m_field2 = new NestedNotSerializable(val);
        }

        private int m_field;
        private NestedNotSerializable m_field2;

        public class NestedNotSerializable
        {
            public NestedNotSerializable(int val)
            {
                m_field = val / 2.0;
            }

            private double m_field;
        }
    }

    public class UDT_Nested_OuterNotSerializableInnerSerializable
    {
        public UDT_Nested_OuterNotSerializableInnerSerializable(int val)
        {
            m_field = val + 1;
            m_field2 = new NestedSerializable(val);
        }

        private int m_field;
        private NestedSerializable m_field2;

        [Serializable]
        public class NestedSerializable
        {
            public NestedSerializable(int val)
            {
                m_field = val / 2.0;
            }

            private double m_field;
        }
    }

    //////////////////////////////////////////////////////////////////////
    //
    // Self reference
    //

    // First level circular type
    [Serializable]
    public class UDT_FirstLevelCircular
    {
        public UDT_FirstLevelCircular(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
            m_circularRef = null;
        }

        public int m_field1;
        public int m_field2;

        public UDT_FirstLevelCircular m_circularRef;
    }

    // First level circular type with an array reference to self
    [Serializable]
    public class UDT_FirstLevelCircularArrayRef
    {
        public UDT_FirstLevelCircularArrayRef(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
            m_circularRefArray = new UDT_FirstLevelCircular[5];
        }

        public int m_field1;
        public int m_field2;

        public UDT_FirstLevelCircular[] m_circularRefArray;
    }

    // Second level circular type
    [Serializable]
    public class UDT_SecondLevelCircular
    {
        public UDT_SecondLevelCircular(int val)
        {
            m_field1 = val + 1;
            m_field2 = val + 2;
            m_child = new UDT_CircularRefChild(this);
        }

        public int m_field1;
        public int m_field2;

        public UDT_CircularRefChild m_child;
    }

    [Serializable]
    public class UDT_CircularRefChild
    {
        public UDT_CircularRefChild(UDT_SecondLevelCircular parent)
        {
            m_parent = parent;
        }

        public UDT_SecondLevelCircular m_parent;
    }

    // Circular type with custom serializer. Should not be rejected
    [CustomDryadLinqSerializer(typeof(UDT_CircularTypeWithCustomSerializer))]
    public class UDT_CircularTypeWithCustomSerializer : IDryadLinqSerializer<UDT_CircularTypeWithCustomSerializer>
    {
        public UDT_CircularTypeWithCustomSerializer() { }
        public UDT_CircularTypeWithCustomSerializer(int val)
        {
            m_field1 = val;

            // create each new object with log2(val) self references hanging off of m_next
            if (val == 0)
                m_next = null;
            else
                m_next = new UDT_CircularTypeWithCustomSerializer(val / 2);
        }

        public int m_field1;
        public UDT_CircularTypeWithCustomSerializer m_next;

        //
        // sample recursive custom serializer
        //
        public UDT_CircularTypeWithCustomSerializer Read(DryadLinqBinaryReader reader)
        {
            UDT_CircularTypeWithCustomSerializer obj = new UDT_CircularTypeWithCustomSerializer();

            bool bHasValidNext = reader.ReadBool();
            obj.m_field1 = reader.ReadInt32();

            if (bHasValidNext)
            {
                obj.m_next = this.Read(reader); // recursively read the next 
            }
            else
            {
                obj.m_next = null;  // terminate recursion
            }

            return obj;
        }

        public void Write(DryadLinqBinaryWriter writer, UDT_CircularTypeWithCustomSerializer x)
        {
            if (x.m_next != null)
            {
                writer.Write(true);     // bHasValidNext for the reader side
                writer.Write(x.m_field1);
                this.Write(writer, x.m_next);     // write out recursively
            }
            else
            {
                writer.Write(false);   // bHasValidNext = false for the reader side, makes sure recursive reads stop
                writer.Write(x.m_field1);
            }
        }
    }

    // This type itself isn't circular, but contains a field of a circular type that is custom serialized. Autoserialization should work for this guy
    [Serializable]
    public class UDT_TypeContainingCustomSerializedCircularType
    {
        public UDT_TypeContainingCustomSerializedCircularType(int val)
        {
            m_field1 = val;
            m_circularType = new UDT_CircularTypeWithCustomSerializer(val);
        }

        public int m_field1;
        public UDT_CircularTypeWithCustomSerializer m_circularType;
    }
}
