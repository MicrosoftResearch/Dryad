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
using System.Reflection;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq
{
    // We override Equals and GetHashCode for better performance.
    [Serializable]
    public struct LineRecord : IComparable, IComparable<LineRecord>, IEnumerable, IEnumerable<char>, IEquatable<LineRecord>
    {
        private string _line;
        public string Line
        {
            get { return _line; }
            internal set { _line = value; }
        }

        public LineRecord(string line)
        {
            _line = line;
        }

        public override bool Equals(Object obj)
        {
            if (!(obj is LineRecord)) return false;
            return this.Line.Equals(((LineRecord)obj).Line);
        }

        public bool Equals(LineRecord val)
        {
            return this.Line.Equals(val.Line);
        }

        public static bool Equals(LineRecord a, LineRecord b)
        {
            return a.Equals(b);
        }

        public static bool operator ==(LineRecord a, LineRecord b)
        {
            return a.Equals(b);
        }

        public static bool operator !=(LineRecord a, LineRecord b)
        {
            return !a.Equals(b);
        }

        public static bool operator <(LineRecord a, LineRecord b)
        {
            return a.CompareTo(b) < 0;
        }

        public static bool operator >(LineRecord a, LineRecord b)
        {
            return a.CompareTo(b) > 0;
        }

        public override int GetHashCode()
        {
            return this.Line.GetHashCode();
        }
        
        public int CompareTo(Object val)
        {
            if (val == null) return 1;
            
            if (!(val is LineRecord))
            {
                throw new ArgumentException(SR.CompareArgIncorrect, "val");
            }

            return StringComparer.Ordinal.Compare(this.Line, ((LineRecord)val).Line);
        }
    
        public int CompareTo(LineRecord val)
        {
            return StringComparer.Ordinal.Compare(this.Line, val.Line);
        }

        IEnumerator<char> IEnumerable<char>.GetEnumerator()
        {
            return this.Line.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.Line.GetEnumerator();
        }

        public override String ToString()
        {
            return this.Line;
        }
    }
}
