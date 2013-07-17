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
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Data.SqlTypes;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    public abstract class HpcLinqFactory<T>
    {
        public abstract HpcRecordReader<T> MakeReader(NativeBlockStream nativeStream);
        public abstract HpcRecordReader<T> MakeReader(IntPtr handle, UInt32 port);
        public abstract HpcRecordWriter<T> MakeWriter(NativeBlockStream nativeStream);
        public abstract HpcRecordWriter<T> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize);
    }

    public sealed class HpcLinqFactoryByte : HpcLinqFactory<byte>
    {
        public override HpcRecordReader<byte> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordByteReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<byte> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordByteReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<byte> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordByteWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<byte> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordByteWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactorySByte : HpcLinqFactory<sbyte>
    {
        public override HpcRecordReader<sbyte> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordSByteReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<sbyte> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordSByteReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<sbyte> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordSByteWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<sbyte> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordSByteWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryBool : HpcLinqFactory<bool>
    {
        public override HpcRecordReader<bool> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordBoolReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<bool> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordBoolReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<bool> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordBoolWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<bool> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordBoolWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
    
    public sealed class HpcLinqFactoryChar : HpcLinqFactory<char>
    {
        public override HpcRecordReader<char> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordCharReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<char> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordCharReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<char> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordCharWriter(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<char> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordCharWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryShort : HpcLinqFactory<short>
    {
        public override HpcRecordReader<short> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordShortReader(new HpcBinaryReader(nativeStream));
        }
        
        public override HpcRecordReader<short> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordShortReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<short> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordShortWriter(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<short> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordShortWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryUShort : HpcLinqFactory<ushort>
    {
        public override HpcRecordReader<ushort> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordUShortReader(new HpcBinaryReader(nativeStream));
        }
        
        public override HpcRecordReader<ushort> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordUShortReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<ushort> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordUShortWriter(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<ushort> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordUShortWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryInt32 : HpcLinqFactory<int>
    {
        public override HpcRecordReader<int> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordInt32Reader(new HpcBinaryReader(nativeStream));
        }
        
        public override HpcRecordReader<int> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordInt32Reader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<int> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordInt32Writer(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<int> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordInt32Writer(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
    
    public sealed class HpcLinqFactoryUInt32 : HpcLinqFactory<uint>
    {
        public override HpcRecordReader<uint> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordUInt32Reader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<uint> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordUInt32Reader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<uint> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordUInt32Writer(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<uint> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordUInt32Writer(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
    
    public sealed class HpcLinqFactoryInt64 : HpcLinqFactory<long>
    {
        public override HpcRecordReader<long> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordInt64Reader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<long> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordInt64Reader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<long> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordInt64Writer(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<long> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordInt64Writer(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
    
    public sealed class HpcLinqFactoryUInt64 : HpcLinqFactory<ulong>
    {
        public override HpcRecordReader<ulong> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordUInt64Reader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<ulong> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordUInt64Reader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<ulong> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordUInt64Writer(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<ulong> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordUInt64Writer(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryFloat : HpcLinqFactory<float>
    {
        public override HpcRecordReader<float> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordFloatReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<float> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordFloatReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<float> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordFloatWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<float> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordFloatWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryDecimal : HpcLinqFactory<decimal>
    {
        public override HpcRecordReader<decimal> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordDecimalReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<decimal> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordDecimalReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<decimal> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordDecimalWriter(new HpcBinaryWriter(nativeStream));
        }
        
        public override HpcRecordWriter<decimal> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordDecimalWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryDouble : HpcLinqFactory<double>
    {
        public override HpcRecordReader<double> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordDoubleReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<double> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordDoubleReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<double> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordDoubleWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<double> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordDoubleWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryDateTime : HpcLinqFactory<DateTime>
    {
        public override HpcRecordReader<DateTime> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordDateTimeReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<DateTime> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordDateTimeReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<DateTime> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordDateTimeWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<DateTime> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordDateTimeWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
    
    public sealed class HpcLinqFactoryString : HpcLinqFactory<string>
    {
        public override HpcRecordReader<string> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordStringReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<string> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordStringReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<string> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordStringWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<string> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordStringWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryGuid : HpcLinqFactory<Guid>
    {
        public override HpcRecordReader<Guid> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordGuidReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<Guid> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordGuidReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<Guid> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordGuidWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<Guid> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordGuidWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactoryLineRecord : HpcLinqFactory<LineRecord>
    {
        public override HpcRecordReader<LineRecord> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordTextReader(new HpcTextReader(nativeStream));
        }

        public override HpcRecordReader<LineRecord> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordTextReader(new HpcTextReader(handle, port));
        }

        public override HpcRecordWriter<LineRecord> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordTextWriter(new HpcTextWriter(nativeStream));
        }

        public override HpcRecordWriter<LineRecord> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordTextWriter(new HpcTextWriter(handle, port, buffSize));
        }
    }

    public sealed class HpcLinqFactorySqlDateTime : HpcLinqFactory<SqlDateTime>
    {
        public override HpcRecordReader<SqlDateTime> MakeReader(NativeBlockStream nativeStream)
        {
            return new HpcRecordSqlDateTimeReader(new HpcBinaryReader(nativeStream));
        }

        public override HpcRecordReader<SqlDateTime> MakeReader(IntPtr handle, UInt32 port)
        {
            return new HpcRecordSqlDateTimeReader(new HpcBinaryReader(handle, port));
        }

        public override HpcRecordWriter<SqlDateTime> MakeWriter(NativeBlockStream nativeStream)
        {
            return new HpcRecordSqlDateTimeWriter(new HpcBinaryWriter(nativeStream));
        }

        public override HpcRecordWriter<SqlDateTime> MakeWriter(IntPtr handle, UInt32 port, Int32 buffSize)
        {
            return new HpcRecordSqlDateTimeWriter(new HpcBinaryWriter(handle, port, buffSize));
        }
    }
}
