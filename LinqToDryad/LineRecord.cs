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
    /// <summary>
    /// The DryadLINQ type to represent a line of text.
    /// </summary>
    [Serializable]
    public struct LineRecord : IComparable, IComparable<LineRecord>, IEnumerable, IEnumerable<char>, IEquatable<LineRecord>
    {
        private string _line;

        /// <summary>
        /// Initializes a new instance of LineRecord from a string.
        /// </summary>
        /// <param name="line">The input string.</param>
        public LineRecord(string line)
        {
            _line = line;
        }

        /// <summary>
        /// Gets the string value of a LineRecord.
        /// </summary>
        public string Line
        {
            get { return _line; }
            internal set { _line = value; }
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current LineRecord.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>true iff the specified object is equal to the current object.</returns>
        public override bool Equals(Object obj)
        {
            if (!(obj is LineRecord)) return false;
            return this.Line.Equals(((LineRecord)obj).Line);
        }

        /// <summary>
        /// Determines whether a specified LineRecord is equal to the current LineRecord.
        /// </summary>
        /// <param name="val">A LineRecord to compare with.</param>
        /// <returns>true iff the argument is equal to the current LineRecord.</returns>
        public bool Equals(LineRecord val)
        {
            return this.Line.Equals(val.Line);
        }

        /// <summary>
        /// Determines whether two specified LineRecords are equal.
        /// </summary>
        /// <param name="a">The left LineRecord.</param>
        /// <param name="b">The right LineRecord.</param>
        /// <returns>true iff two LineRecords are equal.</returns>
        public static bool operator ==(LineRecord a, LineRecord b)
        {
            return a.Equals(b);
        }

        /// <summary>
        /// Determines whether two specified LineRecords are not equal.
        /// </summary>
        /// <param name="a">The left LineRecord.</param>
        /// <param name="b">The right LineRecord.</param>
        /// <returns>true iff two LineRecords are not equal.</returns>
        public static bool operator !=(LineRecord a, LineRecord b)
        {
            return !a.Equals(b);
        }

        /// <summary>
        /// Returns true iff a LineRecord is less than another LineRecord.
        /// </summary>
        /// <param name="a">The left LineRecrod.</param>
        /// <param name="b">The right LineRecord.</param>
        /// <returns>true iff left is less than right.</returns>
        public static bool operator <(LineRecord a, LineRecord b)
        {
            return a.CompareTo(b) < 0;
        }

        /// <summary>
        /// Returns true iff a LineRecord is greater than another LineRecord.
        /// </summary>
        /// <param name="a">The left LineRecrod.</param>
        /// <param name="b">The right LineRecord.</param>
        /// <returns>true iff left is greater than right.</returns>
        public static bool operator >(LineRecord a, LineRecord b)
        {
            return a.CompareTo(b) > 0;
        }

        /// <summary>
        /// Returns the hash code of the current LineRecord.
        /// </summary>
        /// <returns>A 32-bit signed integer.</returns>
        public override int GetHashCode()
        {
            return this.Line.GetHashCode();
        }
        
        /// <summary>
        /// Compares the current LineRecord with an object.
        /// </summary>
        /// <param name="val">The value to compare.</param>
        /// <returns>An integer that indicates the order.</returns>
        public int CompareTo(Object val)
        {
            if (val == null) return 1;
            
            if (!(val is LineRecord))
            {
                throw new ArgumentException(SR.CompareArgIncorrect, "val");
            }

            return StringComparer.Ordinal.Compare(this.Line, ((LineRecord)val).Line);
        }

        /// <summary>
        /// Compares the current LineRecord with another LineRecord.
        /// </summary>
        /// <param name="val">The LineRecord to compare with.</param>
        /// <returns>An integer that indicates the order.</returns>
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

        /// <summary>
        /// Returns a string that represents the current LineRecord.
        /// </summary>
        /// <returns>A string that represents the current LineRecord.</returns>
        public override String ToString()
        {
            return this.Line;
        }
    }
}
