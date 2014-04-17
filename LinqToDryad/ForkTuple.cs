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
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    interface IForkValue
    {
        bool HasValue { get; }
        object Value { get; }
    }

    /// <summary>
    /// Represents an element that may not have valid value.
    /// </summary>
    /// <typeparam name="T">The element type</typeparam>
    [Serializable]
    public struct ForkValue<T> : IForkValue, IEquatable<ForkValue<T>>
    {
        private T m_x;
        private bool m_hasX;

        /// <summary>
        /// Initializes an instance of ForkValue{T}. Its element has valid value.
        /// </summary>
        /// <param name="x">The value of the element</param>
        public ForkValue(T x)
        {
            this.m_x = x;
            this.m_hasX = true;
        }
        
        /// <summary>
        /// true iff the value of the element is valid. 
        /// </summary>
        public bool HasValue
        {
            get { return this.m_hasX; }
        }

        object IForkValue.Value
        {
            get { return this.m_x; }
        }
                
        /// <summary>
        /// Gets the value of the element. It is valid only if HasValue returns true.
        /// </summary>
        public T Value
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }

        /// <summary>
        /// Gets the hash code of this instance of ForkValue.
        /// </summary>
        /// <returns>An integer hash code</returns>
        public override int GetHashCode()
        {
            if (!this.HasValue)
            {
                return 0;
            }
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Determines whether the current ForkValue is equal to a specified ForkValue
        /// </summary>
        /// <param name="fval">A specified ForkValue</param>
        /// <returns>true iff the current ForkValue is equal to the argument</returns>
        public bool Equals(ForkValue<T> fval)
        {
            if (!this.HasValue)
            {
                return !fval.HasValue;
            }
            return fval.HasValue && this.Value.Equals(fval.Value);
        }
    }

    /// <summary>
    /// Represents a pair of elements that may not have valid values.
    /// </summary>
    /// <typeparam name="T1">The type of the first element</typeparam>
    /// <typeparam name="T2">The type of the second element</typeparam>
    [Serializable]    
    public struct ForkTuple<T1, T2> : IEquatable<ForkTuple<T1, T2>>
    {
        private T1 m_x;
        private T2 m_y;
        private bool m_hasX;
        private bool m_hasY;        

        /// <summary>
        /// Initializes an instnace of ForkTuple of two elements. Both elements have valid values.
        /// </summary>
        /// <param name="x">The first element</param>
        /// <param name="y">The second element</param>
        public ForkTuple(T1 x, T2 y)
        {
            this.m_x = x;
            this.m_y = y;
            this.m_hasX = true;
            this.m_hasY = true;
        }
        
        /// <summary>
        /// true iff the value of the first element is valid.
        /// </summary>
        public bool HasFirst
        {
            get { return this.m_hasX; }
        }

        /// <summary>
        /// true ifff the value of the second element is valid.
        /// </summary>
        public bool HasSecond
        {
            get { return this.m_hasY; }
        }
                
        /// <summary>
        /// Gets and sets the first element. 
        /// </summary>
        public T1 First
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }

        /// <summary>
        /// Gets and sets the second element.
        /// </summary>
        public T2 Second
        {
            get { return this.m_y; }
            set { this.m_y = value; this.m_hasY = true; }
        }

        /// <summary>
        /// Gets the hash code of this instance of ForkTuple.
        /// </summary>
        /// <returns>An integer hash code</returns>
        public override int GetHashCode()
        {
            int hashCode = 0;
            if (this.HasFirst)
            {
                hashCode = (-1521134295 * hashCode) + this.First.GetHashCode();
            }
            if (this.HasSecond)
            {
                hashCode = (-1521134295 * hashCode) + this.Second.GetHashCode();
            }
            return hashCode;
        }

        /// <summary>
        /// Determines whether the current ForkTuple is equal to a specified ForkTuple.
        /// </summary>
        /// <param name="fval">A specified ForkTuple</param>
        /// <returns>true iff the current ForkTuple is equal to the argument</returns>
        public bool Equals(ForkTuple<T1, T2> fval)
        {
            if (this.HasFirst)
            {
                if (!fval.HasFirst || !this.First.Equals(fval.First))
                {
                    return false;
                }
            }
            else
            {
                if (fval.HasFirst) return false;
            }
            if (this.HasSecond)
            {
                if (!fval.HasSecond || !this.Second.Equals(fval.Second))
                {
                    return false;
                }
            }
            else
            {
                if (fval.HasSecond) return false;
            }
            return true;
        }
    }

    /// <summary>
    /// Represents a tuple of three elements that may not have valid values.
    /// </summary>
    /// <typeparam name="T1">The type of the first element</typeparam>
    /// <typeparam name="T2">The type of the second element</typeparam>
    /// <typeparam name="T3">The type of the third element</typeparam>
    [Serializable]    
    public struct ForkTuple<T1, T2, T3> : IEquatable<ForkTuple<T1, T2, T3>>
    {
        private T1 m_x;
        private T2 m_y;
        private T3 m_z;
        private bool m_hasX;
        private bool m_hasY;        
        private bool m_hasZ;
        
        /// <summary>
        /// Initializes an instance of ForkTuple of three elements. All the elements have valid values.
        /// </summary>
        /// <param name="x">The first element</param>
        /// <param name="y">The second element</param>
        /// <param name="z">The third element</param>
        public ForkTuple(T1 x, T2 y, T3 z)
        {
            this.m_x = x;
            this.m_y = y;
            this.m_z = z;
            this.m_hasX = true;
            this.m_hasY = true;
            this.m_hasZ = true;
        }
        
        /// <summary>
        /// true iff the value of the first element is valid.
        /// </summary>
        public bool HasFirst
        {
            get { return this.m_hasX; }
        }

        /// <summary>
        /// true iff the value of the second element is valid.
        /// </summary>
        public bool HasSecond
        {
            get { return this.m_hasY; }
        }

        /// <summary>
        /// true iff the value of the third element is valid.
        /// </summary>
        public bool HasThird
        {
            get { return this.m_hasZ; }
        }
        
        /// <summary>
        /// Gets and sets the first element.
        /// </summary>
        public T1 First
        {
            get { return this.m_x; }
            set { this.m_x = value; this.m_hasX = true; }
        }

        /// <summary>
        /// Gets and sets the second element.
        /// </summary>
        public T2 Second
        {
            get { return this.m_y; }
            set { this.m_y = value; this.m_hasY = true; }
        }

        /// <summary>
        /// Gets and sets the third element.
        /// </summary>
        public T3 Third
        {
            get { return this.m_z; }
            set { this.m_z = value; this.m_hasZ = true; }
        }

        /// <summary>
        /// Gets the hash code of this instance of ForkTuple.
        /// </summary>
        /// <returns>An integer hash code</returns>
        public override int GetHashCode()
        {
            int hashCode = 0;
            if (this.HasFirst)
            {
                hashCode = (-1521134295 * hashCode) + this.First.GetHashCode();
            }
            if (this.HasSecond)
            {
                hashCode = (-1521134295 * hashCode) + this.Second.GetHashCode();
            }
            if (this.HasThird)
            {
                hashCode = (-1521134295 * hashCode) + this.Third.GetHashCode();
            }
            return hashCode;
        }

        /// <summary>
        /// Determines whether the current ForkTuple is equal to a specified ForkTuple.
        /// </summary>
        /// <param name="fval">A specified ForkTuple</param>
        /// <returns>true iff the current ForkTuple is equal to the argument</returns>
        public bool Equals(ForkTuple<T1, T2, T3> fval)
        {
            if (this.HasFirst)
            {
                if (!fval.HasFirst || !this.First.Equals(fval.First))
                {
                    return false;
                }
            }
            else
            {
                if (fval.HasFirst) return false;
            }
            if (this.HasSecond)
            {
                if (!fval.HasSecond || !this.Second.Equals(fval.Second))
                {
                    return false;
                }
            }
            else
            {
                if (fval.HasSecond) return false;
            }
            if (this.HasThird)
            {
                if (!fval.HasThird || !this.Third.Equals(fval.Third))
                {
                    return false;
                }
            }
            else
            {
                if (fval.HasThird) return false;
            }
            return true;
        }
    }
}
