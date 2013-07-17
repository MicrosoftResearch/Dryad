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

namespace Microsoft.Research.Dryad
{
    using System;
    using System.ServiceModel.Channels;
    using System.Runtime.Serialization;

    [DataContract]
    public class SoftAffinity : IComparable
    {
        [DataMember]
        private string node = null;
        [DataMember]
        private ulong weight = ulong.MinValue;

        public SoftAffinity(string n, ulong w)
        {
            node = n;
            weight = w;
        }

        // Compare SoftAffinities using their weights
        int IComparable.CompareTo(object obj)
        {
            SoftAffinity other = obj as SoftAffinity;

            if (other == null)
            {
                return -1;
            }

            if (this.Weight < other.Weight)
            {
                return -1;
            }
            else if (this.Weight > other.Weight)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        [DataMember]
        public string Node
        {
            get { return node; }
            set { node = value; }
        }

        [DataMember]
        public ulong Weight
        {
            get { return weight; }
            set { weight = value; }
        }
    }
}
