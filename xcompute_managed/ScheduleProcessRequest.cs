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
    using System.Collections.Generic;
    using System.Collections.Specialized;

    internal class ScheduleProcessRequest
    {
        private int processId;
        private string commandLine;
        private string hardAffinity;
        private List<SoftAffinity> softAffinityList;
        private StringDictionary environment;

        // We want the affinity list sorted in descending order by weight,
        // so use a custom IComparer which just reverses the arguments
        internal class reverseComparer : IComparer<SoftAffinity>
        {
            int IComparer<SoftAffinity>.Compare(SoftAffinity lhs, SoftAffinity rhs)
            {
                return Comparer<SoftAffinity>.Default.Compare(rhs, lhs);
            }
        }

        public ScheduleProcessRequest(int id, string cl, List<SoftAffinity> soft, string hard, StringDictionary env)
        {
            processId = id;
            commandLine = cl;
            softAffinityList = soft;
            // Sort the affinity list in descending order by weight so that we try 
            // the highest weight affinity first, etc
            softAffinityList.Sort(new reverseComparer());
            environment = env;
            hardAffinity = hard;
        }

        public int Id
        {
            get { return processId; }
        }

        public string CommandLine
        {
            get { return commandLine; }
        }

        public StringDictionary Environment
        {
            get { return environment; }
        }

        public bool MustRunOnNode(string node)
        {
            return ((hardAffinity != null) && (String.Compare(hardAffinity, node, StringComparison.OrdinalIgnoreCase) == 0));
        }

        public bool CanRunOnNode(string node)
        {
            return (hardAffinity == null || (String.Compare(hardAffinity, node, StringComparison.OrdinalIgnoreCase) == 0));
        }

        public ulong GetAffinityWeightForNode(string node)
        {
            foreach (SoftAffinity a in softAffinityList)
            {
                if (String.Compare(a.Node, node, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    return a.Weight;
                }
            }

            return 0;
        }

        public string HardAffinity
        {
            get { return this.hardAffinity; }
        }

        public SoftAffinity AffinityAt(int i)
        {
            return softAffinityList[i];
        }

        public int AffinityCount
        {
            get { return softAffinityList.Count; }
        }
    }
}
