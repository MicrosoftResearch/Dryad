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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Reflection;
using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;

namespace DryadLinqTests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            SimpleTests.Run(args);

            /*
             * 
            Config conf = new Config("dryad-temp", "main1", @"d:\temp\TestLog\");
            DryadLinqContext context = new DryadLinqContext(Config.cluster);

            string matchPattern = @"";

            TestLog.LogInit(Config.testLogPath + "BasicAPITests.txt");
            BasicAPITests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "ApplyAndForkTests.txt");
            ApplyAndForkTests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "GroupByReduceTests.txt");
            GroupByReduceTests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "RangePartitionAPICoverageTests.txt");
            RangePartitionAPICoverageTests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "TypesInQueryTests.txt");
            TypesInQueryTests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "SerializationTests.txt");
            SerializationTests.Run(context, matchPattern);

            TestLog.LogInit(Config.testLogPath + "MiscBugFixTests.txt");
            MiscBugFixTests.Run(context, matchPattern);
             */

        }

    }
}
