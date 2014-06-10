using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace DryadLinqTests
{
    public class BasicAPITests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" BasicAPITests ");
            TestLog.Message(" **********************");

            var tests = new Dictionary<string, Action>()
              {
                  {"ToStoreThrowsForNonQuery", () => ToStoreThrowsForNonQuery(context) },
                  {"ToStoreGetEnumerator", () => ToStoreGetEnumerator(context) },
                  {"GetEnumeratorNonToStoreTerminated", () => GetEnumeratorNonToStoreTerminated(context) },
                  {"ToStoreSubmitGetEnumerator", () => ToStoreSubmitGetEnumerator(context) },
                  {"SubmitNonToStoreTerminated", () => SubmitNonToStoreTerminated(context) },
                  {"MaterializeToStoreTerminated", () => MaterializeToStoreTerminated(context) },
                  {"MaterializeNonToStoreTerminated", () => MaterializeNonToStoreTerminated(context) },
                  {"EnumeratePlainData", () => EnumeratePlainData(context) }, 
                  {"CopyPlainDataViaToStoreSubmit", () => CopyPlainDataViaToStoreSubmit(context) },
                  {"CopyPlainDataViaToStoreMaterialize", () => CopyPlainDataViaToStoreMaterialize(context) },
                  {"PlainEnumerableAsDryadQueryToStoreSubmit", () => PlainEnumerableAsDryadQueryToStoreSubmit(context) },
                  {"RepeatSubmit", () => RepeatSubmit(context) }, 
                  // ToDo {"RepeatMaterialize", () => RepeatMaterialize(context) }, hangs
                  {"MaterializeMentionsSameQueryTwice", () => MaterializeMentionsSameQueryTwice(context) },
                  {"QueryOnDataBackedDLQ", () => QueryOnDataBackedDLQ(context) },
                  {"Bug11781_CountandFirstOrDefault", () => Bug11781_CountandFirstOrDefault(context) },
                  {"Bug11782_Aggregate", () => Bug11782_Aggregate(context) },
                  {"Bug11782_LowLevelQueryableManipulation", () => Bug11782_LowLevelQueryableManipulation(context) },
                  {"Bug11638_LongWhere", () => Bug11638_LongWhere(context) },
                  {"AssumeRangePartition", () => AssumeRangePartition(context) },
                  {"Bug11638_LongMethods", () => Bug11638_LongMethods(context) },
                  {"ContextConfigIsReadOnly", () => ContextConfigIsReadOnly(context) },
                  // ToDo{"ToggleSpeculativeDuplication", () => ToggleSpeculativeDuplication(context) },
                  {"Bug15068_ConfigResourcesAPI", () => Bug15068_ConfigResourcesAPI(context) },
                  {"Bug14449_ContextShouldExposeVersionIDs", () => Bug14449_ContextShouldExposeVersionIDs(context) }, // not valid anymore
                  {"Bug_16341_SubmitThrowsForDifferentContexts", () => Bug_16341_SubmitThrowsForDifferentContexts(context) },
                  {"Bug_16341_VariousTestsForSubmit", () => Bug_16341_VariousTestsForSubmit(context) },
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static bool ToStoreThrowsForNonQuery(DryadLinqContext context) 
        {
            string testName = "ToStoreThrowsForNonQuery";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                int[] data = new[] { 1, 2, 3 };
                var q1 = data.AsQueryable().Select(x => 100 + x).ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                           "dummy")).ToArray();
                //Should throw as we got into DryadLinq via AsQueryable() rather than via context.
                passed &= false;
            }
            catch (Exception Ex)
            {
                //expected
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool ToStoreGetEnumerator(DryadLinqContext context) 
        {
            string testName = "ToStoreGetEnumerator";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/ToStoreGetEnumerator";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x);
                    IQueryable<int> output = q1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var clusterOut = output.GetEnumerator();
                    result[0] = output.AsEnumerable();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x);
                    result[1] = q1.AsEnumerable();
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

        public static bool GetEnumeratorNonToStoreTerminated(DryadLinqContext context) 
        {
            string testName = "GetEnumeratorNonToStoreTerminated";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x);
                    IQueryable<int> q2 = q1.Where(x => true);
                    var clusterOut = q2.GetEnumerator();
                    result[0] = q2.AsEnumerable();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x);
                    IQueryable<int> q2 = q1.Where(x => true);
                    result[1] = q2.AsEnumerable(); ;
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

        public static bool ToStoreSubmitGetEnumerator(DryadLinqContext context)
        {
            string testName = "ToStoreSubmitGetEnumerator";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                string outFile = "unittest/output/ToStoreSubmitGetEnumerator";
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q1 = pt1.Select(x => 100 + x).HashPartition(x => x);
                    var q2 = q1.Where(x => true);
                    IQueryable<int> output = q2.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    DryadLinqJobInfo info = output.SubmitAndWait();
                    result[0] = output.AsEnumerable();

                    passed &= Validate.outFileExists(outFile);
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x).HashPartition(x => x);
                    IQueryable<int> q2 = q1.Where(x => true);
                    result[1] = q2.AsEnumerable();
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

        public static bool SubmitNonToStoreTerminated(DryadLinqContext context)
        {
            string testName = "SubmitNonToStoreTerminated";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q1 = pt1.Select(x => 100 + x);
                    var q2 = q1.Where(x => true);
                    q2.SubmitAndWait();
                    result[0] = q2.ToList();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q1 = pt1.Select(x => 100 + x);
                    IQueryable<int> q2 = q1.Where(x => true);
                    result[1] = q2.ToList();
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

        public static bool MaterializeToStoreTerminated(DryadLinqContext context)
        {
            string testName = "MaterializeToStoreTerminated";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;

                    string outFile_a = "unittest/output/MaterializeToStoreTerminated_a";
                    string outFile_b = "unittest/output/MaterializeToStoreTerminated_b";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> query = pt1.Select(x => 100 + x);
                    var q1 = query.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile_a), true); //stream name w/o prefixed slash
                    var q2 = query.Where(x => true).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile_b), true);  //stream name w/ prefixed slash
                    DryadLinqQueryable.Submit(q1, q2); //materialize 
                    var _unused2 = q1.Select(x => x); // Legal call, but BLOCKS 

                    passed &= Validate.outFileExists(outFile_a);
                    passed &= Validate.outFileExists(outFile_b);
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IEnumerable<int> q1 = pt1.Select(x => 100 + x);
                    IEnumerable<int> q2 = q1.Where(x => true);
                    var _unused2 = q1.Select(x => x); // Legal call, but BLOCKS 
                    // ToDo - how to verify that it blocks
                }

                //@TODO: assert that only one query execution occurred. 
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool MaterializeNonToStoreTerminated(DryadLinqContext context)
        {
            string testName = "MaterializeNonToStoreTerminated";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> query = pt1.Select(x => 100 + x);
                    DryadLinqQueryable.Submit(query);
                    result[0] = query;
                    //@TODO: assert that only one query execution occurred.
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> query = pt1.Select(x => 100 + x);
                    result[1] = query;
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

        public static bool EnumeratePlainData(DryadLinqContext context)
        {
            string testName = "EnumeratePlainData";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[0] = pt1;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[1] = pt1;
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

        public static bool CopyPlainDataViaToStoreSubmit(DryadLinqContext context)
        {
            string testName = "CopyPlainDataViaToStoreSubmit";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/CopyPlainDataViaToStoreSubmit";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    DryadLinqJobInfo info = q.Submit();
                    info.Wait();

                    result[0] = q.AsEnumerable();
                    passed &= Validate.outFileExists(outFile);
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[1] = pt1.AsEnumerable();
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

        public static bool CopyPlainDataViaToStoreMaterialize(DryadLinqContext context)
        {
            string testName = "CopyPlainDataViaToStoreMaterialize";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/CopyPlainDataViaToStoreMaterialize";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    DryadLinqJobInfo info = DryadLinqQueryable.Submit(q);
                    info.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    result[1] = pt1;
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

        public static bool PlainEnumerableAsDryadQueryToStoreSubmit(DryadLinqContext context) 
        {
            string testName = "PlainEnumerableAsDryadQueryToStoreSubmit";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                context.LocalDebug = false;
                string outFile = "unittest/output/PlainEnumerableAsDryadQueryToStoreSubmit";

                int[] plainData = { 5, 6, 7 };

                var q = context.FromEnumerable(plainData)
                               .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile));
                DryadLinqJobInfo info = q.Submit();
                info.Wait();

                passed &= Validate.outFileExists(outFile);
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool RepeatSubmit(DryadLinqContext context)
        {
            string testName = "RepeatSubmit";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/RepeatSubmit";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    DryadLinqJobInfo info1 = null;
                    DryadLinqJobInfo info2 = null;
                    try
                    {
                        info1 = q.Submit();
                        info2 = q.Submit(); // should throw

                        TestLog.Message("Error: Did not throw an exception");
                        passed &= false;
                    }
                    catch (ArgumentException)
                    {
                        passed &= true;
                    }

                    //wait for any jobs to complete.
                    if (info1 != null)
                    {
                        info1.Wait();
                    }

                    if (info2 != null)
                    {
                        info2.Wait();
                    }
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

        public static bool RepeatMaterialize(DryadLinqContext context)
        {
            string testName = "RepeatMaterialize";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                context.LocalDebug = false;
                string outFile = "unittest/output/RepeatMaterialize";

                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                DryadLinqJobInfo info1 = null;
                DryadLinqJobInfo info2 = null;
                try
                {
                    info1 = DryadLinqQueryable.Submit(new[] { q }); // materialize
                    info2 = DryadLinqQueryable.Submit(new[] { q }); // should throw

                    TestLog.Message("Error: Did not throw an exception");
                    passed &= false;
                }
                catch (ArgumentException Ex)
                {
                    TestLog.Message("Error: " + Ex.Message);
                    passed &= true;
                }

                //wait for any jobs to complete.
                if (info1 != null)
                {
                    info1.Wait();
                }

                if (info2 != null)
                {
                    info2.Wait();
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

        public static bool MaterializeMentionsSameQueryTwice(DryadLinqContext context)
        {
            string testName = "MaterializeMentionsSameQueryTwice";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                context.LocalDebug = false;
                string outFile = "unittest/output/MaterializeMentionsSameQueryTwice";

                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                DryadLinqJobInfo info1 = null;
                try
                {
                    info1 = DryadLinqQueryable.Submit(q, q); // materialize
                    passed &= false; // for Config.cluster execution, second materialize should throw;
                }
                catch (ArgumentException)
                {
                    passed &= true;
                }

                //wait for any jobs to complete.
                if (info1 != null)
                {
                    info1.Wait();
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

        public static bool QueryOnDataBackedDLQ(DryadLinqContext context)
        {
            string testName = "QueryOnDataBackedDLQ";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/QueryOnDataBackedDLQ";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.Select(x => 100 + x);
                    IQueryable<int> outPT = q.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    outPT.Submit();

                    IQueryable<int> outPT2_dummy_notUsed = outPT.Select(x => x);  //BLOCKS HERE until the input is concrete ???
                    // source.Expression returns an expression for the backingDataDLQ 
                    // CheckAndInitialize() on the backingData will block.

                    passed &= Validate.outFileExists(outFile);
                    result[0] = outPT;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);

                    IQueryable<int> q = pt1.Select(x => 100 + x);
                    IQueryable<int> outPT2_dummy_notUsed = q.Select(x => x);
                    result[1] = outPT2_dummy_notUsed;
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

        public static bool Bug11781_CountandFirstOrDefault(DryadLinqContext context)
        {
            string testName = "Bug11781_CountandFirstOrDefault";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                int y_cluster = 10;
                int y_local =  100;
                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug11781";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var c = pt1.Count();

                    //Test CountAsQuery()
                    var q = pt1.CountAsQuery().ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    DryadLinqJobInfo info = q.Submit();
                    info.Wait();

                    passed &= Validate.outFileExists(outFile);

                    // Also test FirstOrDefault
                    // the affected code for dlq.Execute() also has a branch for FirstOrDefault() and friends.
                    y_cluster = pt1.FirstOrDefault();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    //Test Count()
                    var c = pt1.Count();

                    //Test CountAsQuery()
                    IQueryable<int> q = pt1.AsQueryable().CountAsQuery();

                    // Also test FirstOrDefault
                    // the affected code for dlq.Execute() also has a branch for FirstOrDefault() and friends.
                    y_local = pt1.FirstOrDefault();
                }

                // compare result
                if (y_cluster != y_local)
                {
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

        public static bool Bug11782_Aggregate(DryadLinqContext context)
        {
            string testName = "Bug11782_Aggregate";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug11782_Aggregate";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    //test Aggregate()
                    int c = pt1.Select(x => x).Aggregate((x, y) => x + y);

                    //test AggregateAsQuery()
                    var q = pt1.Select(x => x).AggregateAsQuery((x, y) => x + y).ToStore(outFile);
                    DryadLinqJobInfo info = DryadLinqQueryable.Submit(q);
                    info.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);

                    //test Aggregate()
                    int c = pt1.Select(x => x).Aggregate((x, y) => x + y);

                    //test AggregateAsQuery()
                    IQueryable<int> q = pt1.AsQueryable().Select(x => x).AggregateAsQuery((x, y) => x + y);
                    result[1] = q;
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

        public static bool Bug11782_LowLevelQueryableManipulation(DryadLinqContext context)
        {
            string testName = "Bug11782_LowLevelQueryableManipulation";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    Expression lambda = Expression.Lambda<Func<int, int>>(
                            Expression.Constant(1),
                            new[] { Expression.Parameter(typeof(int), "x") });
                    var z = pt1.Provider.CreateQuery(
                        Expression.Call(
                            typeof(Queryable), "Select",
                            new Type[] { pt1.ElementType, pt1.ElementType },
                            pt1.Expression, Expression.Quote(lambda)));

                    passed &= false; // the use of non-generic Provider.CreateQuery() should have thrown
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

        public static bool Bug11638_LongWhere(DryadLinqContext context)
        {
            string testName = "Bug11638_LongWhere";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/BasicAPITests_LongWhere";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.Select(x => 100 + x);
                    IQueryable<int> outPT = q.LongWhere((x, i) => true).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var info = outPT.Submit();
                    info.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = outPT.AsEnumerable();
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.Select(x => 100 + x);
                    IQueryable<int> outPT = q.LongWhere((x, i) => true);

                    result[1] = outPT;
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

        public static bool AssumeRangePartition(DryadLinqContext context)
        {
            string testName = "AssumeRangePartition";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/BasicAPITests_AssumeRangePartition";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q = pt1.AssumeRangePartition(x => x, false)
                               .Select(x => 100 + x).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var info = q.Submit();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.AssumeRangePartition(x => x, false)
                                           .Select(x => 100 + x);
                    result[1] = q;
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

        public static bool Bug11638_LongMethods(DryadLinqContext context)
        {
            string testName = "Bug11638_LongMethods";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    string outFile = "unittest/output/Bug11638_LongMethods";

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var q = pt1.LongSelect((x, i) => x)
                                .LongWhere((x, i) => true)
                                .LongSelectMany((x, i) => new[] { x })
                                .LongSelectMany((x, i) => new[] { x }, (i, seq) => seq)  //overload#2
                                .LongTakeWhile((x, i) => true)
                                .LongSkipWhile((x, i) => false)
                                .ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                    var info = q.Submit();
                    info.Wait();

                    passed &= Validate.outFileExists(outFile);
                    result[0] = q;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> q = pt1.LongSelect((x, i) => x)
                                            .LongWhere((x, i) => true)
                                            .LongSelectMany((x, i) => new[] { x })
                                            .LongSelectMany((x, i) => new[] { x }, (i, seq) => seq)  //overload#2
                                            .LongTakeWhile((x, i) => true)
                                            .LongSkipWhile((x, i) => false);

                    result[1] = q;
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

        public static bool ContextConfigIsReadOnly(DryadLinqContext context)
        {
            string testName = "ContextConfigIsReadOnly";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                string jobName = context.JobFriendlyName;
                context.JobFriendlyName = "bob";
                context.JobFriendlyName = jobName;
            }
            catch (NotSupportedException ex)
            {
                TestLog.Message("Error: " + ex.Message);
                passed &= false; // "an exception should not thrown";
            }

            try
            {
                context.JobMinNodes = 120;
                passed &= false; 
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.ResourcesToAdd.Add("blah");
                passed &= false; 
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.ResourcesToRemove.Add("blah");
                passed &= false; 
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.JobEnvironmentVariables.Add("bob", "bob");
                passed &= false; 
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.EnableSpeculativeDuplication = false;
                passed &= false; 
            }
            catch (NotSupportedException)
            {
                //expected
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool ToggleSpeculativeDuplication()
        {
            string testName = "ContextConfigIsReadOnly";
            TestLog.TestStart(testName);

            var context = Utils.MakeBasicConfig(Config.cluster);
            context.LocalDebug = false;
            bool passed = true;
            try
            {
                passed &= context.EnableSpeculativeDuplication; // "Speculative Duplication enabled by default"
                context.EnableSpeculativeDuplication = false;
                passed &= !context.EnableSpeculativeDuplication; // "Failed to disable speculative duplication"
                context.EnableSpeculativeDuplication = true;
                passed &= context.EnableSpeculativeDuplication; // "Failed to ensable speculative duplication"
                context.EnableSpeculativeDuplication = true;
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false; // "Enabling and disabling speculative duplication should not throw"
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug15068_ConfigResourcesAPI(DryadLinqContext context)
        {
            string testName = "Bug15068_ConfigResourcesAPI";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    context.HeadNode = "MIKELID7"; // ToDo - hard coded
                    passed &= (context.ResourcesToAdd.IsReadOnly == false); // "isReadOnly should be false"
                    passed &= (context.ResourcesToRemove.IsReadOnly == false); // "isReadOnly should be false"

                    //clear
                    context.ResourcesToAdd.Clear();
                    context.ResourcesToRemove.Clear();

                    //add
                    context.ResourcesToAdd.Add("abc");
                    context.ResourcesToRemove.Add("def");
                    context.ResourcesToRemove.Add("ghi");

                    //index, count, getEnumerator
                    passed &= (context.ResourcesToAdd[0] == "abc"); // "wrong value"
                    passed &= (context.ResourcesToAdd.Count == 1); // "wrong value"

                    passed &= (context.ResourcesToRemove[0] == "def"); // "wrong value"
                    passed &= (context.ResourcesToRemove.Where((x, i) => (i == 1)).First() == "ghi"); // "wrong value"
                    passed &= (context.ResourcesToRemove.Count == 2); // "wrong value"

                    // clone was taken.
                    context.ResourcesToAdd.Clear();
                    context.ResourcesToRemove.Clear();
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

        public static bool Bug14449_ContextShouldExposeVersionIDs(DryadLinqContext context)
        {
            string testName = "Bug14449_ContextShouldExposeVersionIDs";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                //  NorSupported
                //passed &= (context.Major >= 3); // "problem with HpcLinq client version"
                //passed &= (context.Major >= 3); // "problem with HpcLinq server version"

                //passed &= (context.ClientVersion.Major >= 3); // "problem with Dsc client version"
                //passed &= (context.ServerVersion.Major >= 3); // "problem with Dsc server version"

            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug_16341_SubmitThrowsForDifferentContexts(DryadLinqContext context)
        {
            string testName = "Bug_16341_SubmitThrowsForDifferentContexts";
            TestLog.TestStart(testName);
            var context2 = new DryadLinqContext(Config.cluster);
            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    context2.LocalDebug = false;

                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context2);
                    DryadLinqQueryable.Submit(pt1, pt2);
                    passed &= false;
                }
            }
            catch (Exception)
            {
            }

            try
            {
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context2);
                DryadLinqQueryable.SubmitAndWait(pt1, pt2);
                passed &= false;
            }
            catch (Exception)
            {
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool Bug_16341_VariousTestsForSubmit(DryadLinqContext context)
        {
            string testName = "Bug_16341_VariousTestsForSubmit";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                int[] data = new[] { 1, 2, 3 };
                var badQ1 = data.AsQueryable().Select(x => 100 + x);
                var badQ2 = data.AsQueryable().Select(x => 100 + x);

                IQueryable<int> goodQ1 = DataGenerator.GetSimpleFileSets(context);

                try
                {
                    badQ1.Submit();
                    passed &= false; // "should throw as input isn't a L2H query"
                }
                catch (ArgumentException)
                {
                }

                try
                {
                    DryadLinqQueryable.Submit((IQueryable)null); //this-Query overload
                    passed &= false; // "should throw ArgNull as input is null"
                }
                catch (ArgumentException)
                {
                    //although we pass null, it goes to params[] overload which creates an actual array[1] containing one null
                    //hence we throw ArgumentException rather than ArgumentNullException.
                }

                try
                {
                    DryadLinqQueryable.Submit((IQueryable[])null); //multi-query overload
                    passed &= false; // "should throw ArgNull as input is null"
                }
                catch (ArgumentNullException)
                {
                }

                try
                {
                    DryadLinqQueryable.Submit(goodQ1, null); //multi-query overload
                    passed &= false; // "should throw ArgEx as one of the inputs is null"
                }
                catch (ArgumentException)
                {
                }

                try
                {
                    DryadLinqQueryable.Submit(goodQ1, badQ1); //multi-query overload
                    passed &= false; // "should throw ArgEx as one of the inputs is not a L2H"
                }
                catch (ArgumentException)
                {
                }

                //----------
                // same tests again for SubmitAndWait

                try
                {
                    badQ1.SubmitAndWait();
                    passed &= false; // "should throw as input isn't a L2H query"
                }
                catch (ArgumentException)
                {
                }

                try
                {
                    DryadLinqQueryable.SubmitAndWait((IQueryable)null); //this-Query overload
                    passed &= false; // "should throw ArgNull as input is null"
                }
                catch (ArgumentException)
                {
                    //although we pass null, it goes to params[] overload which creates an actual array[1] containing one null
                    //hence we throw ArgumentException rather than ArgumentNullException.
                }

                try
                {
                    DryadLinqQueryable.SubmitAndWait((IQueryable[])null); //multi-query overload
                    passed &= false; // "should throw ArgNull as input is null"
                }
                catch (ArgumentNullException)
                {
                }

                try
                {
                    DryadLinqQueryable.SubmitAndWait(goodQ1, null); //multi-query overload
                    passed &= false; // "should throw ArgEx as one of the inputs is null"
                }
                catch (ArgumentException)
                {
                }

                try
                {
                    DryadLinqQueryable.SubmitAndWait(goodQ1, badQ1); //multi-query overload
                    passed &= false; // "should throw ArgEx as one of the inputs is not a L2H"
                }
                catch (ArgumentException)
                {
                }

            }
            catch (Exception)
            {
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

    }

}
