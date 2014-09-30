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
using System.Text.RegularExpressions;

namespace DryadLinqTests
{
    public class ApplyAndForkTests 
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" ApplyAndForkTests ");
            TestLog.Message(" **********************");

            var tests = new Dictionary<string, Action>()
              {
                  {"NonHomomorphicUnaryApply", () => NonHomomorphicUnaryApply(context) },
                  {"HomomorphicUnaryApply", () => HomomorphicUnaryApply(context) },
                  {"NonHomomorphicBinaryApply", () => NonHomomorphicBinaryApply(context) },
                  {"LeftHomomorphicBinaryApply", () => LeftHomomorphicBinaryApply(context) },
                  {"FullHomomorphicBinaryApply_DifferentDataSets", () => FullHomomorphicBinaryApply_DifferentDataSets(context) },
                  {"FullHomomorphicBinaryApply_IdenticalDataSets", () => FullHomomorphicBinaryApply_IdenticalDataSets(context) },
                  {"Aggregate_WithCombiner", () => Aggregate_WithCombiner(context) },
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static IEnumerable<int> NonHomomorphic_Unary_Func(IEnumerable<int> input)
        {
            return input;
        }

        // [DistributiveOverConcat]
        public static IEnumerable<int> Homomorphic_Unary_Func(IEnumerable<int> input)
        {
            return input;
        }

        public static IEnumerable<int> NonHomomorphic_Binary_Func(IEnumerable<int> left, IEnumerable<int> right)
        {
            return left;
        }

        // [LeftDistributiveOverConcat]        
        public static IEnumerable<int> LeftHomomorphic_Binary_Func(IEnumerable<int> left, IEnumerable<int> right)
        {
            return left;
        }

        // Note: an apply function must only consume each enumerable once, and it must produce an enumerable
        //       So for a simple pass-through function that does a little work, we must enumerate only once.
        //       Else we get the error: "An HpcLinq channel can't be read more than once."
        // [DistributiveOverConcat]
        public static IEnumerable<int> FullHomomorphic_Binary_Func(IEnumerable<int> left, IEnumerable<int> right)
        {
            long cLeft = 0;
            foreach (int x in left)
            {
                cLeft++;
                yield return x;
            }

            long cRight = 0;
            foreach (int x in right)
            {
                cRight++;
                yield return x;
            }

            if (cLeft == 0)
                throw new Exception("a node received empty left-data");

            if (cRight == 0)
                throw new Exception("a node received empty right-data");
        }

        public static bool NonHomomorphicUnaryApply(DryadLinqContext context)
        {
            string testName = "NonHomomorphicUnaryApply";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/NonHomomorphicUnaryApply";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q1 = pt1.ApplyPerPartition(x => NonHomomorphic_Unary_Func(x));
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(x => NonHomomorphic_Unary_Func(x));
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool HomomorphicUnaryApply(DryadLinqContext context)
        {
            string testName = "HomomorphicUnaryApply";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/HomomorphicUnaryApply";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(x => Homomorphic_Unary_Func(x));
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(x => Homomorphic_Unary_Func(x));
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool NonHomomorphicBinaryApply(DryadLinqContext context)
        {
            string testName = "NonHomomorphicBinaryApply";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/NonHomomorphicBinaryApply";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Apply(pt1, (x, y) => NonHomomorphic_Binary_Func(x, y));
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Apply(pt1, (x, y) => NonHomomorphic_Binary_Func(x, y));
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool LeftHomomorphicBinaryApply(DryadLinqContext context)
        {
            string testName = "LeftHomomorphicBinaryApply";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/LeftHomomorphicBinaryApply";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt1, (x, y) => LeftHomomorphic_Binary_Func(x, y), true);
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt1, (x, y) => LeftHomomorphic_Binary_Func(x, y), true);
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool FullHomomorphicBinaryApply_DifferentDataSets(DryadLinqContext context)
        {
            string testName = "FullHomomorphicBinaryApply_DifferentDataSets";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/FullHomomorphicBinaryApply_DifferentDataSets";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt2, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt2, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool FullHomomorphicBinaryApply_IdenticalDataSets(DryadLinqContext context)
        {
            string testName = "FullHomomorphicBinaryApply_IdenticalDataSets";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/FullHomomorphicBinaryApply_2";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt1, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                    var jobInfo = q1.ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.ApplyPerPartition(pt1, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                    result[1] = q1;
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception ex)
                {
                    TestLog.Message("Error: " + ex.Message);
                    passed &= false;
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

       [Associative(typeof(AssociativeRecursive1))] 
       public static string IntToStringCSVAggregator(string agg, int next)
        {
            return agg + "," + next.ToString();
        }
       public class AssociativeRecursive1 : IAssociative<string>
       {
           public string Seed()
           {
               return "";
           }
           public string RecursiveAccumulate(string first, string second)
           {
               return first + second;
           }
       }

       public static bool Aggregate_WithCombiner(DryadLinqContext context)
       {
           string testName = "Aggregate_WithCombiner";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    string q1 = pt1.Aggregate("", (str, x) => IntToStringCSVAggregator(str, x));
                    passed &= (q1.Length == 27); // string should have numbers 1..12 separated by commas
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }
    }
}
