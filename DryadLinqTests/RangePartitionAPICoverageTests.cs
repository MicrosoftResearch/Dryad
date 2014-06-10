using Microsoft.Research.DryadLinq;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace DryadLinqTests
{
    public class RangePartitionAPICoverageTests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" RangePartitionAPICoverageTests ");
            TestLog.Message(" **********************");

            var tests = new Dictionary<string, Action>()
              {
                  {"RP_keySelector", () => RP_keySelector(context) },
                  {"RP_keySelector_pcount", () => RP_keySelector_pcount(context) },
                  {"RP_keySelector_isDescending", () => RP_keySelector_isDescending(context) },
                  {"RP_keySelector_rangeKeys", () => RP_keySelector_rangeKeys(context) },
                  {"RP_keySelector_isDescending_pcount", () => RP_keySelector_isDescending_pcount(context) },
                  {"RP_keySelector_keyComparer_isDescending", () => RP_keySelector_keyComparer_isDescending(context) },
                  {"RP_keySelector_rangeKeys_keyComparer", () => RP_keySelector_rangeKeys_keyComparer(context) },
                  {"RP_keySelector_keyComparer_isDescending_pcount", () => RP_keySelector_keyComparer_isDescending_pcount(context) },
                  {"RP_keySelector_rangeKeys_keyComparer_isDescending", () => RP_keySelector_rangeKeys_keyComparer_isDescending(context) },
                  {"RP_singlePartition_autoSeparators", () => RP_singlePartition_autoSeparators(context) },
                  {"RP_rangeSeparators_customComparer", () => RP_rangeSeparators_customComparer(context) },
                  {"RP_rangeSeparators_nullCustomComparer", () => RP_rangeSeparators_nullCustomComparer(context) },
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static IEnumerable<int> CountPartitions<T>(IEnumerable<T> pt)
        {
            yield return 1;
        }

        public static IEnumerable<T> MaxInPartition<T>(IEnumerable<T> pt)
        {
            yield return pt.Max();
        }

        public static IEnumerable<T> MinInPartition<T>(IEnumerable<T> pt)
        {
            yield return pt.Min();
        }

        public static IEnumerable<T2> MaxCompare<T1, T2>(IEnumerable<T1> pt, int i, IEnumerable<T2> maxes)
        {
            if (0 == i)
            {
                yield return (T2)Convert.ChangeType(1, typeof(int));
            }

            int priorMax = (int)Convert.ChangeType(maxes.ToArray()[i - 1], typeof(T2));
            foreach (T1 item in pt)
            {
                int val = (int)Convert.ChangeType(item, typeof(T1));
                if (val <= priorMax)
                {
                    yield return (T2)Convert.ChangeType(1, typeof(int));
                }
            }
        }

        public static bool RP_keySelector(DryadLinqContext context)
        {
            string testName = "RP_keySelector";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();
                    
                    passed &= TestRangePartitioned(pt2, 3, false);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_pcount(DryadLinqContext context)
        {
            string testName = "RP_keySelector_pcount";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_pcount";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, 2).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 2, false);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, 2);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_isDescending(DryadLinqContext context)
        {
            string testName = "RP_keySelector_isDescending";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_isDescending";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, true).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 3, true);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, true);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_rangeKeys(DryadLinqContext context)
        {
            string testName = "RP_keySelector_rangeKeys";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                IEnumerable<int>[] result2 = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_rangeKeys_1";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    //increasing ranges keys.
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 2, 5, 8 }).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 4, false);
                    result[0] = pt2.ToList();


                    string outFile2 = "unittest/output/RP_keySelector_rangeKeys_2";
                    IQueryable<int> pt1_2 = DataGenerator.GetRangePartitionDataSet(context);
                    //decreasing ranges keys.
                    IQueryable<int> pt2_2 = pt1_2.RangePartition(x => x, new[] { 8, 5, 2 }).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile2), true);
                    var jobInfo2 = pt2_2.Submit();
                    jobInfo2.Wait();

                    passed &= TestRangePartitioned(pt2_2, 4, true);
                    result2[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    //increasing ranges keys.
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 2, 5, 8 });
                    result[1] = pt2.ToList();

                    IQueryable<int> pt1_2 = DataGenerator.GetRangePartitionDataSet(context);
                    //decreasing ranges keys.
                    IQueryable<int> pt2_2 = pt1_2.RangePartition(x => x, new[] { 8, 5, 2 });
                    result2[1] = pt2.ToList();
                }

                // compare result
                try
                {
                    Validate.Check(result);
                    Validate.Check(result2);
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

        public static bool RP_keySelector_isDescending_pcount(DryadLinqContext context)
        {
            string testName = "RP_keySelector_isDescending_pcount";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_isDescending_pcount";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, true, 2).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 2, true);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, true, 2);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_keyComparer_isDescending(DryadLinqContext context)
        {
            string testName = "RP_keySelector_keyComparer_isDescending";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_keyComparer_isDescending";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, Comparer<int>.Default, true).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 3, true);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, Comparer<int>.Default, true);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_rangeKeys_keyComparer(DryadLinqContext context)
        {
            string testName = "RP_keySelector_rangeKeys_keyComparer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_rangeKeys_keyComparer";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 2, 5, 8 }, Comparer<int>.Default)
                                     .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 4, false);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 2, 5, 8 }, Comparer<int>.Default);
                    result[1] = pt2.ToList();
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

        public static bool RP_keySelector_keyComparer_isDescending_pcount(DryadLinqContext context)
        {
            string testName = "RP_keySelector_keyComparer_isDescending_pcount";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_keyComparer_isDescending_pcount";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, Comparer<int>.Default, true, 2)
                                             .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 2, true);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, Comparer<int>.Default, true, 2);
                    result[1] = pt2.ToList();
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

            return passed;
        }

        public static bool RP_keySelector_rangeKeys_keyComparer_isDescending(DryadLinqContext context)
        {
            string testName = "RP_keySelector_rangeKeys_keyComparer_isDescending";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_keySelector_rangeKeys_keyComparer_isDescending";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 8, 5, 2 }, Comparer<int>.Default, true)
                                             .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 4, true);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new[] { 8, 5, 2 }, Comparer<int>.Default, true);
                    result[1] = pt2.ToList();
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

        public static bool RP_singlePartition_autoSeparators(DryadLinqContext context)
        {
            string testName = "RP_singlePartition_autoSeparators";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_singlePartition_autoSeparators";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.Apply(x => x) // force a merge
                                     .RangePartition(x => x)
                                     .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 1, false);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    IQueryable<int> pt2 = pt1.Apply(x => x) // force a merge
                                     .RangePartition(x => x);
                    result[1] = pt2.ToList();
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

        public static bool RP_rangeSeparators_customComparer(DryadLinqContext context)
        {
            string testName = "RP_rangeSeparators_customComparer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RP_rangeSeparators_customComparer";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);

                    //these keys should be considered not-sorted
                    try
                    {
                        var results = pt1.RangePartition(x => x, new int[] { 1, 2, 3, 4 }, new WeirdIntComparer(), false).ToArray();
                        passed &= false; // "an exception should have been thrown (non-sorted separators)."
                    }
                    catch (ArgumentException)
                    {
                        //expected
                    }

                    //these keys should also be considered not-sorted
                    try
                    {
                        var results = pt1.RangePartition(x => x, new int[] { 4, 3, 2, 1 }, new WeirdIntComparer(), false).ToArray();
                        passed &= false; // "an exception should have been thrown (non-sorted separators)."
                    }
                    catch (ArgumentException)
                    {
                        //expected
                    }

                    //these keys should work
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new int[] { 6, 6, 3, 1 }, new WeirdIntComparer(), false)
                                    .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    passed &= TestRangePartitioned(pt2, 5, new WeirdIntComparer(), false);
                    result[0] = pt2.ToList();
                }
                // local
                {
                    context.LocalDebug = true;
                    string outFile = "unittest/output/RP_rangeSeparators_customComparer";

                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);

                    //these keys should be considered not-sorted
                    try
                    {
                        var results = pt1.RangePartition(x => x, new int[] { 1, 2, 3, 4 }, new WeirdIntComparer(), false).ToArray();
                        passed &= false; // "an exception should have been thrown (non-sorted separators)."
                    }
                    catch (ArgumentException)
                    {
                        //expected
                    }

                    //these keys should also be considered not-sorted
                    try
                    {
                        var results = pt1.RangePartition(x => x, new int[] { 4, 3, 2, 1 }, new WeirdIntComparer(), false).ToArray();
                        passed &= false; // "an exception should have been thrown (non-sorted separators)."
                    }
                    catch (ArgumentException)
                    {
                        //expected
                    }

                    //these keys should work
                    IQueryable<int> pt2 = pt1.RangePartition(x => x, new int[] { 6, 6, 3, 1 }, new WeirdIntComparer(), false)
                                    .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var jobInfo = pt2.Submit();
                    jobInfo.Wait();

                    result[1] = pt2.ToList();
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

        public static bool TestRangePartitioned(IQueryable<int> pt, int expectedPCount, bool expectedIsDescending)
        {
            bool passed = true;
            try
            {
                IEnumerable<int> ptCount = pt.ApplyPerPartition(x => CountPartitions(x));
                int cPartitions = ptCount.Count();

                passed &= (expectedPCount == cPartitions);

                if (passed)
                {
                    int[] ptMax = pt.ApplyPerPartition(x => MaxInPartition(x)).ToArray();
                    int[] ptMin = pt.ApplyPerPartition(x => MinInPartition(x)).ToArray();

                    // compare mins and maxs of each partition
                    for (int i = 1; i < cPartitions; i++)
                    {
                        if (false == expectedIsDescending)
                            passed &= (ptMin[i] > ptMax[i - 1]);
                        else
                            passed &= (ptMax[i] < ptMin[i - 1]);
                    }
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }
            return passed;
        }


        [Serializable]
        public class WeirdIntComparer : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                int xx = x % 2;
                int yy = y % 2;
                return xx.CompareTo(yy);
            }
        }

        public static bool RP_rangeSeparators_nullCustomComparer(DryadLinqContext context)
        {
            string testName = "RP_rangeSeparators_nullCustomComparer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetRangePartitionDataSet(context);
                    var results1 = pt1.RangePartition(x => x, new int[] { 1, 2, 3, 4 }, null, false).ToArray();
                    var results2 = pt1.RangePartition(x => x, new int[] { 1, 2, 3, 4 }, null).ToArray();
                    //passing is not throwing.
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

        public static bool TestRangePartitioned(IQueryable<int> pt, int expectedPCount, IComparer<int> comparer, bool expectedIsDescending)
        {
            bool passed = true;
            try
            {
                IEnumerable<int> ptCount = pt.ApplyPerPartition(x => CountPartitions(x));
                int cPartitions = ptCount.Count();

                passed &= (expectedPCount == cPartitions);

                if (passed)
                {
                    int[] ptMax = pt.ApplyPerPartition(x => MaxInPartition(x)).ToArray();
                    int[] ptMin = pt.ApplyPerPartition(x => MinInPartition(x)).ToArray();

                    // compare mins and maxs of each partition
                    for (int i = 1; i < cPartitions; i++)
                    {
                        passed &= (comparer.Compare(ptMin[i], ptMax[i - 1]) == 0); // ToDo
                    }
                }
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }
            return passed;
        }


    }
}
