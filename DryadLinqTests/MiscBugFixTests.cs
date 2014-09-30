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
    public static class MiscBugFixTests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" MiscBugFixTests ");
            TestLog.Message(" **********************");

            var tests = new Dictionary<string, Action>()
              {
                  {"Bug11447_GroupByWithComparer", () => Bug11447_GroupByWithComparer(context) },
                  {"Bug12584_HashPartitionOutputCount", () => Bug12584_HashPartitionOutputCount(context) },
                  {"Bug13108_DisableSequenceEquals", () => Bug13108_DisableSequenceEquals(context) },
                  {"Bug13529_and_Bug13593_IndexedOperatorCompilation", () => Bug13529_and_Bug13593_IndexedOperatorCompilation(context) },
                  {"Bug13130_ReverseOperator", () => Bug13130_ReverseOperator(context) },
                  {"Bug13736_IndexedTakeWhile", () => Bug13736_IndexedTakeWhile(context) },
                  {"Bug13534_HashPartitionNegIndexIsError", () => Bug13534_HashPartitionNegIndexIsError(context) },
                  {"Bug13474_and_Bug13483_FromDscOnBadFileSet", () => Bug13474_and_Bug13483_FromDscOnBadFileSet(context) },
                  {"Bug13637_EmptyFilesInFilesets", () => Bug13637_EmptyFilesInFilesets(context) },
                  {"Bug13637_LocalDebugProducingZeroRecords", () => Bug13637_LocalDebugProducingZeroRecords(context) },
                  {"Bug13245_FromDsc_Submit_throws", () => Bug13245_FromDsc_Submit_throws(context) },
                  {"Bug13245_QueryUsingNonHpcLinqOperator", () => Bug13245_QueryUsingNonHpcLinqOperator(context) },
                  {"Bug13302_ConfigEnvironmentCleanup", () => Bug13302_ConfigEnvironmentCleanup(context) },
                  {"Bug13970_MismatchedDataTypes", () => Bug13970_MismatchedDataTypes(context) },
                  {"Bug14010_AlreadyDisposedContext", () => Bug14010_AlreadyDisposedContext(context) },
                  {"Bug14256_LeaseOnTempDscFileset", () => Bug14256_LeaseOnTempDscFileset(context) },
                  {"Bug14189_OrderPreservation", () => Bug14189_OrderPreservation(context) },
                  {"Bug14190_MergeJoin_DecreasingOrder", () => Bug14190_MergeJoin_DecreasingOrder(context) },
                  {"Bug14192_MultiApplySubExpressionReuse", () => Bug14192_MultiApplySubExpressionReuse(context) },
                  {"Bug14870_LongIndexTakeWhile", () => Bug14870_LongIndexTakeWhile(context) },
                  {"Bug15159_NotOperatorForNullableBool", () => Bug15159_NotOperatorForNullableBool(context) },
                  {"Bug15371_NoDataMembersForSerialization", () => Bug15371_NoDataMembersForSerialization(context) },
                  {"Bug15570_GetHashCodeAndEqualsForNullableFieldsOfAnonymousTypes", () => Bug15570_GetHashCodeAndEqualsForNullableFieldsOfAnonymousTypes(context) },
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static bool Bug11447_GroupByWithComparer(DryadLinqContext context)
        {
            string testName = "Bug11447_GroupByWithComparer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var a = pt1.GroupBy(x => x, EqualityComparer<int>.Default).SelectMany(x => x).ToList();
                    result[0] = a;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var a = pt1.GroupBy(x => x, EqualityComparer<int>.Default).SelectMany(x => x).ToList();
                    result[1] = a;
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

        public static bool Bug12584_HashPartitionOutputCount(DryadLinqContext context) // ToDo
        {
            string testName = "Bug12584_HashPartitionOutputCount";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug12584_HashPartitionOutputCount";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var info = pt1.HashPartition(x => x).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    info.Wait();

                    //// partition verification are only valid for cluster execution..

                    //// check that nOutputPartitions == nInputPartitions.
                    //// Note: this is today's behavior, but we don't strictly guarantee this and the rules may change.
                    //var inFS = context.DscService.GetFileSet(DataGenerators.SIMPLE_FILESET_NAME);
                    //var inPCount = inFS.GetFiles().Count();
                    //var outFS = context.DscService.GetFileSet("DevUnitTest/Bug12584_out");
                    //var outPCount = outFS.GetFiles().Count();
                    //passed &= TestUtils.Assert(outPCount == inPCount, "Output nPartitions should be equal to input nPartitions.");
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
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

        public static bool Bug13108_DisableSequenceEquals(DryadLinqContext context)
        {
            string testName = "Bug13108_DisableSequenceEquals";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                    bool result = pt1.Select(x => x).SequenceEqual(pt2.Select(x => x));
                    passed &= false; // NotSupportedException should have been thrown
                }
            }
            catch (Exception Ex)
            {
            }
            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug13529_and_Bug13593_IndexedOperatorCompilation(DryadLinqContext context)
        {
            string testName = "Bug13529_and_Bug13593_IndexedOperatorCompilation";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[0] = pt1.Select((x, i) => x)
                                    .LongSelect((x, i) => x)
                                    .LongSelectMany((x, i) => new[] { x })
                                    .SelectMany((x, i) => new[] { x })
                                    .Where((x, i) => true)
                                    .LongWhere((x, i) => true)
                                    .TakeWhile((x, i) => true)
                                    .LongTakeWhile((x, i) => true)
                                    .SkipWhile((x, i) => false)
                                    .LongSkipWhile((x, i) => false)
                                    .ToArray();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[1] = pt1.Select((x, i) => x)
                                    .LongSelect((x, i) => x)
                                    .LongSelectMany((x, i) => new[] { x })
                                    .SelectMany((x, i) => new[] { x })
                                    .Where((x, i) => true)
                                    .LongWhere((x, i) => true)
                                    .TakeWhile((x, i) => true)
                                    .LongTakeWhile((x, i) => true)
                                    .SkipWhile((x, i) => false)
                                    .LongSkipWhile((x, i) => false)
                                    .ToArray();
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

        public static bool Bug13130_ReverseOperator(DryadLinqContext context)
        {
            string testName = "Bug13130_ReverseOperator";
            TestLog.TestStart(testName);

            bool passed = true;
            //data set #1
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[0] = pt1.Reverse().ToArray();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[1] = pt1.Reverse().ToArray();
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

            //data set #2 ToDo
            //data set #3 ToDo

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug13736_IndexedTakeWhile(DryadLinqContext context)
        {
            string testName = "Bug13736_IndexedTakeWhile";
            TestLog.TestStart(testName);

            bool passed = true;
            // dataset #1
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var pt2 = pt1.TakeWhile((x, i) => i < 6).ToArray();
                    result[0] = pt1.AsEnumerable().TakeWhile((x, i) => i < 6).ToArray();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var pt2 = pt1.TakeWhile((x, i) => i < 6).ToArray();
                    result[1] = pt1.AsEnumerable().TakeWhile((x, i) => i < 6).ToArray();
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

            // dataset #2
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var pt2 = pt1.TakeWhile((x, i) => i < 125).ToArray();
                    result[0] = pt1.AsEnumerable().TakeWhile((x, i) => i < 125).ToArray();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    var pt2 = pt1.TakeWhile((x, i) => i < 125).ToArray();
                    result[0] = pt1.AsEnumerable().TakeWhile((x, i) => i < 125).ToArray();
                }

                // compare result
                try
                {
                    Validate.Check(result);
                }
                catch (Exception)
                {
                    passed &= false;
                }
            }
            catch (Exception)
            {
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug13534_HashPartitionNegIndexIsError(DryadLinqContext context)
        {
            string testName = "Bug13534_HashPartitionNegIndexIsError";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    pt1.HashPartition(x => x, 0); // exception should be thrown
                    passed &= false;
                }
            }
            catch (Exception)
            {
            }

            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    pt1.HashPartition(x => x, -1); // exception should be thrown
                    passed &= false;
                }
            }
            catch (Exception)
            {
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug13474_and_Bug13483_FromDscOnBadFileSet(DryadLinqContext context)
        {
            // ToDo
            return false; 
        }
        public static bool Bug13637_EmptyFilesInFilesets(DryadLinqContext context)
        {
            // ToDo
            return false;
        }

        public static bool Bug13637_LocalDebugProducingZeroRecords(DryadLinqContext context)
        {
            string testName = "Bug13637_LocalDebugProducingZeroRecords";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug13637_LocalDebugProducingZeroRecords";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> pt2 = pt1.Where(x => false).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var queryInfo2 = pt2.Submit();
                    queryInfo2.Wait();
                    int[] data2 = pt2.ToArray();
                    passed &= (data2.Length == 0);

                    // query producing no records -> anonymous output
                    IQueryable<int> pt3 = pt1.Where(x => false);
                    var queryInfo3 = pt3.Submit();
                    queryInfo3.Wait();

                    int[] data3 = pt3.ToArray();
                    passed &= (data3.Length == 0);
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

        public static bool Bug13245_FromDsc_Submit_throws(DryadLinqContext context)
        {
            string testName = "Bug13245_FromDsc_Submit_throws";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var info = pt1.Submit();
                    passed &= false; // should throw ArgEx as the input isn't well formed
                }
            }
            catch (ArgumentException)
            {
            }
            catch (Exception)
            {
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static IQueryable<int> Blah(this IQueryable<int> source)
        {
            return Enumerable.Range(1, 1).AsQueryable();
        }
        public static bool Bug13245_QueryUsingNonHpcLinqOperator(DryadLinqContext context)
        {
            string testName = "Bug13245_QueryUsingNonHpcLinqOperator";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var info = pt1.Blah().Submit();
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

        public static IEnumerable<int> CheckEnvVar(IEnumerable<int> en)
        {
            if (Environment.GetEnvironmentVariable("DummyEnvVar") == null)
                throw new Exception("the expected environment variable is not defined");
            return en;
        }
        public static bool Bug13302_ConfigEnvironmentCleanup(DryadLinqContext context)
        {
            string testName = "Bug13302_ConfigEnvironmentCleanup";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var info = pt1.Apply((en) => CheckEnvVar(en)).Submit();
                    info.Wait();
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

        public static bool Bug13970_MismatchedDataTypes(DryadLinqContext context)
        {
            string testName = "Bug13970_MismatchedDataTypes";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double? output = pt1.AverageAsQuery().Single();
                    passed &= false;
                }
            }
            catch (Exception)
            {
                passed &= true; // expected
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug14010_AlreadyDisposedContext(DryadLinqContext context)
        {
            string testName = "Bug14010_AlreadyDisposedContext";
            TestLog.TestStart(testName);

            bool passed = true;
            context.LocalDebug = false;
            try
            {
                DryadLinqContext ctx = new DryadLinqContext(Config.cluster);
                ctx.Dispose();
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(ctx);
                int output = pt1.Select(x => x).First();
                passed &= false;
            }
            catch (Exception)
            {
                passed &= true;
            }

            try
            {
                DryadLinqContext ctx = new DryadLinqContext(Config.cluster);
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(ctx);
                ctx.Dispose();
                int output = pt1.Select(x => x).First();
                passed &= false;
            }
            catch (Exception)
            {
                passed &= true;
            }

            try
            {
                DryadLinqContext ctx = new DryadLinqContext(Config.cluster);
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(ctx);
                ctx.Dispose();
                IQueryable<int> query = pt1.Select(x => x).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, "abc"), true);
                var info = DryadLinqQueryable.Submit(query);
                passed &= false;
            }
            catch (Exception)
            {
                passed &= true;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug14256_LeaseOnTempDscFileset(DryadLinqContext context) // TODO
        {
            string testName = "Bug14256_LeaseOnTempDscFileset";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
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

        public static bool Bug14189_OrderPreservation(DryadLinqContext context)
        {
            string testName = "Bug14189_OrderPreservation";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IGrouping<int, int>[] clusterSorted, localSorted;
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    var output = pt1.OrderBy(x => x % 4)
                                .Select(x => x).Select(x => x).Where(x => true)  // this pipeline was not preserving order correctly. 
                                .GroupBy(x => x % 4)
                                .ToArray();

                    passed &= (output.Count() == output.Select(x => x.Key).Distinct().Count()); // "each group should have a distinct key");
                    // sort back on the key for deterministic output.
                    clusterSorted = output.OrderBy(x => x.Key).ToArray();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    var output = pt1.OrderBy(x => x % 4)
                                .Select(x => x).Select(x => x).Where(x => true)  // this pipeline was not preserving order correctly. 
                                .GroupBy(x => x % 4)
                                .ToArray();

                    passed &= (output.Count() == output.Select(x => x.Key).Distinct().Count()); // "each group should have a distinct key");
                    // sort back on the key for deterministic output.
                    localSorted = output.OrderBy(x => x.Key).ToArray();
                }

                // check that each group of output has the same elements as the LINQ groups.
                for (int i = 0; i < 4; i++)
                {
                    var a = clusterSorted[i].OrderBy(x => x);
                    var b = localSorted[i].OrderBy(x => x);

                    passed &= a.SequenceEqual(b); //the output should match linq. Error for group: + i);
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

        public static bool Bug14190_MergeJoin_DecreasingOrder(DryadLinqContext context) // TODO
        {
            string testName = "Bug14190_MergeJoin_DecreasingOrder";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug14190_MergeJoin_DecreasingOrder";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
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

        public static bool Bug14192_MultiApplySubExpressionReuse(DryadLinqContext context)
        {
            string testName = "Bug14192_MultiApplySubExpressionReuse";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug14192_MultiApplySubExpressionReuse";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var data = pt1.Select(x => x);

                    var info = pt1.Apply(new[] { pt1, pt1 }, (sources) => new int[] { 1, 2, 3 }).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    info.Wait();

                    // ToDo
                    //string queryPlan = TestUtils.GetRecentQueryXmlAsText();
                    //int nVerticesInPlan = TestUtils.GetVertexStageCount(queryPlan);

                    //passed &= (nVerticesInPlan == 7); // "Only seven vertices should appear (before bug, there were 10 of which the last three were extraneous.");
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

        public static bool Bug14870_LongIndexTakeWhile(DryadLinqContext context)
        {
            string testName = "Bug14870_LongIndexTakeWhile";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;

                    // ToDo - move to data generator
                    int[][] data = new[] { 
                        new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, new int[] { }, new int[] { } }; 

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.LongTakeWhile((x, i) => i < 8).ToArray();
                    passed &= (output.Length == 8); // "Only eight items should be returned."
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

        public static bool Bug15159_NotOperatorForNullableBool(DryadLinqContext context)
        {
            string testName = "Bug15159_NotOperatorForNullableBool";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<bool?>[] result = new IEnumerable<bool?>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select<int, bool?>(x => x == 1 ? (bool?)null : true).Select(x => !x);
                    result[0] = output.ToArray();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select<int, bool?>(x => x == 1 ? (bool?)null : true).Select(x => !x);
                    result[1] = output.ToArray();
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

        public class NoDataMembersClass
        {

        }
        public static bool Bug15371_NoDataMembersForSerialization(DryadLinqContext context)
        {
            string testName = "Bug15371_NoDataMembersForSerialization";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug15371_NoDataMembersForSerialization";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<NoDataMembersClass> output = pt1.Select(x => new NoDataMembersClass());
                    var jobInfo = output.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true).Submit();
                    jobInfo.Wait();
                    var result = context.FromStore<NoDataMembersClass>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile)).ToArray();
                    passed &= false;
                }
            }
            catch (DryadLinqException Ex)
            {
                passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("TypeMustHaveDataMembers") ||
                           Ex.InnerException != null && ((DryadLinqException)Ex.InnerException).ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("TypeMustHaveDataMembers")); // "exception should have been thrown.
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug15570_GetHashCodeAndEqualsForNullableFieldsOfAnonymousTypes(DryadLinqContext context)
        {
            string testName = "Bug15570_GetHashCodeAndEqualsForNullableFieldsOfAnonymousTypes";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // use an anonymous type with nullable fields, and make sure we at least have some null values in the stream
                    var output = pt1.Select(x => new { FirstField = (x % 2 == 0) ? new int?(x) : default(int?), SecondField = x.ToString() })
                                    .GroupBy(x => x.FirstField, y => y.SecondField);     // use of GB ensures we exercise the emitted GetHashCode() overload
                    passed &= (output.Count() != 0); // "Query return 0 length output"
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
