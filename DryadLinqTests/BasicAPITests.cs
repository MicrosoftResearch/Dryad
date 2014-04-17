using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DryadLinqTests
{
    public class BasicAPITests
    {
        public static bool ToStoreThrowsForNonQuery()
        {
            bool passed = true;
            try
            {
                int[] data = new[] { 1, 2, 3 };
                var q1 = data.AsQueryable().Select(x => 100 + x).ToStore<int>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                           "dummy")).ToArray();
                //Should throw as we got into DryadLinq via AsQueryable() rather than via context.
                passed &= false;
            }
            catch (ArgumentException)
            {
                //expected
            }
            return passed;
        }

        public static bool ToStoreGetEnumeratorThrows() // pass
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/ToStoreGetEnumeratorThrows.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                                         "unittest/inputdata/SimpleFile.txt"));
                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());

                IQueryable<int> pt1 = simple.Select(x => x.First());
                IQueryable<int> q1 = pt1.Select(x => 100 + x);

                var output = q1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                output.GetEnumerator();
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool GetEnumeratorNonToStoreTerminated()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                IQueryable<int> q1 = pt1.Select(x => 100 + x);
                IQueryable<int> q2 = q1.Where(x => true);
                foreach (int x in q2) // throws here
                {
                    //Console.WriteLine(x);
                }
                //@TODO: perform a sequence-equals test.

                //IQueryable<LineRecord> format = q2.Select(x => new LineRecord(String.Format("{0}", x)));
                //DryadLinqJobInfo output = format.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                //           "unittest/output/test2.txt")).SubmitAndWait();
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool ToStoreSubmitGetEnumerator() // pass
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/ToStoreSubmitGetEnumerator.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                var q1 = pt1.Select(x => 100 + x).HashPartition(x => x);
                var q2 = q1.Where(x => true);
                IQueryable<int> output = q2.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info = output.SubmitAndWait();

                foreach (int x in output) // should not run a new dryad job.
                {
                    //Console.WriteLine(x);
                }
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool SubmitNonToStoreTerminated()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                                         "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q1 = pt1.Select(x => 100 + x);
                var q2 = q1.Where(x => true);
                q2.SubmitAndWait(); // throws here
                var outPT = q2.ToList();
                foreach (int x in outPT)
                {
                    //Console.WriteLine(x);
                }
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool MaterializeToStoreTerminated()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile_a = "unittest/output/MaterializeToStoreTerminated_a.txt";
                string outFile_b = "unittest/output/MaterializeToStoreTerminated_b.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                IQueryable<int> query = pt1.Select(x => 100 + x);

                var q1 = query.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile_a), true); //stream name w/o prefixed slash

                var q2 = query.Where(x => true).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile_b), true);  //stream name w/ prefixed slash

                DryadLinqQueryable.Submit(q1, q2); //materialize  // throws

                var __unused2 = q1.Select(x => x); // Legal call, but BLOCKS 
                foreach (int x in q2)
                {
                    //Console.WriteLine(x);
                }

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile_a);
                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile_b);

                //@TODO: assert that only one query execution occurred.
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool MaterializeNonToStoreTerminated()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                IQueryable<int> query = pt1.Select(x => 100 + x);

                DryadLinqQueryable.Submit(query); //materialize // throws

                foreach (int x in query)
                {
                    //Console.WriteLine(x);
                }

                //@TODO: assert that only one query execution occurred.
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool EnumeratePlainData()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                foreach (int x in pt1) // throws
                {
                    //Console.WriteLine(x);
                }
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
        
        public static bool CopyPlainDataViaToStoreSubmit()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/CopyPlainDataViaToStoreSubmit.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info = q.Submit();
                info.Wait();

                foreach (int x in q)
                {
                    //Console.WriteLine(x);
                }

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
        
        public static bool CopyPlainDataViaToStoreMaterialize()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/CopyPlainDataViaToStoreMaterialize.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info = DryadLinqQueryable.Submit(q);
                info.Wait();

                foreach (int x in q)
                {
                    //Console.WriteLine(x);
                }

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
        /*
        public static bool PlainEnumerableAsDryadQueryToStoreSubmit()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/PlainEnumerableAsDryadQueryToStoreSubmit.txt";

                int[] plainData = { 5, 6, 7 };

                var q = context.AsDryadQuery(plainData, CompressionScheme.None).ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile);
                DryadLinqJobInfo info = q.Submit();
                info.Wait();

                foreach (int x in q)
                {
                    //Console.WriteLine(x);
                }

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException e)
            {
                passed &= false;
            }
            return passed;
        }
        */
        public static bool RepeatSubmit()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/RepeatSubmit.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());


                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info1 = null;
                DryadLinqJobInfo info2 = null;
                try
                {
                    info1 = q.Submit();
                    info2 = q.Submit(); // does not throw

                    if (!context.LocalDebug)
                    {
                        passed &= false;
                    }
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
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool RepeatMaterialize()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/RepeatMaterialize.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info1 = null;
                DryadLinqJobInfo info2 = null;
                try
                {
                    info1 = DryadLinqQueryable.Submit(new[] { q }); //materialize
                    info2 = DryadLinqQueryable.Submit(new[] { q }); //materialize // does not throw

                    if (!context.LocalDebug)
                    {
                        passed &= false;
                    }
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
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool MaterializeMentionsSameQueryTwice() // pass
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/MaterializeMentionsSameQueryTwice.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q = pt1.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName, outFile), true);
                DryadLinqJobInfo info1 = null;
                try
                {
                    info1 = DryadLinqQueryable.Submit(q, q); //materialize // throws
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
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool QueryOnDataBackedDLQ()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/QueryOnDataBackedDLQ.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                var q = pt1.Select(x => 100 + x);
                var outPT = q.ToStore(AzureUtils.ToAzureUri(Config.accountName, Config.containerName, outFile), true);
                outPT.Submit();

                var outPT2_dummy_notUsed = outPT.Select(x => x);  //BLOCKS HERE until the input is concrete 
                // source.Expression returns an expression for the backingDataDLQ 
                // CheckAndInitialize() on the backingData will block.

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

                foreach (int x in outPT)
                {
                    //Console.WriteLine(x);
                }
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug11781_CountandFirstOrDefault()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/Bug11781.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                //Test Count()
                var c = pt1.Count();

                //Test CountAsQuery()
                var q = pt1.CountAsQuery().ToStore(outFile);
                DryadLinqJobInfo info = q.Submit();
                info.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

                // Also test FirstOrDefault
                // the affected code for dlq.Execute() also has a branch for FirstOrDefault() and friends.
                int y = pt1.FirstOrDefault();
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug11782_Aggregate()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/Bug11782_Aggregate.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                //test Aggregate()
                var c = pt1.Select(x => x).Aggregate((x, y) => x + y);

                //test AggregateAsQuery()
                var q = pt1.Select(x => x).AggregateAsQuery((x, y) => x + y).ToStore(outFile);
                DryadLinqJobInfo info = DryadLinqQueryable.Submit(q);
                info.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug11782_LowLevelQueryableManipulation()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

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
            catch (DryadLinqException)
            {
                passed &= true;
            }
            return passed;
        }

        public static bool Bug11638_LongWhere()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/BasicAPITests_LongWhere.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q = pt1.Select(x => 100 + x);
                var outPT = q.LongWhere((x, i) => true).ToStore(outFile);
                var info = outPT.Submit();
                info.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool AssumeRangePartition()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/BasicAPITests_AssumeRangePartition.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q =
                    pt1
                    .AssumeRangePartition(x => x, false)
                    .Select(x => 100 + x).ToStore(outFile);
                var info = q.Submit();
                info.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug11638_LongMethods()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/Bug11638_LongMethods.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q =
                    pt1
                    .LongSelect((x, i) => x)
                    .LongWhere((x, i) => true)
                    .LongSelectMany((x, i) => new[] { x })
                    .LongSelectMany((x, i) => new[] { x }, (i, seq) => seq)  //overload#2
                    .LongTakeWhile((x, i) => true)
                    .LongSkipWhile((x, i) => false)
                    .ToStore(outFile);
                var info = q.Submit();
                info.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool ContextConfigIsReadOnly()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;

            try
            {
                string jobName = context.JobFriendlyName;
                context.JobFriendlyName = "bob";
                context.JobFriendlyName = jobName;
            }
            catch (NotSupportedException)
            {
                passed &= false; // "an exception should not thrown";
            }

            try
            {
                context.JobMinNodes = 120;
                passed &= false; // "an exception should not thrown";
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.ResourcesToAdd.Add("blah");
                passed &= false; // "an exception should not thrown";
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.ResourcesToRemove.Add("blah");
                passed &= false; // "an exception should not thrown";
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.JobEnvironmentVariables.Add("bob", "bob");
                passed &= false; // "an exception should not thrown";
            }
            catch (NotSupportedException)
            {
                //expected
            }

            try
            {
                context.EnableSpeculativeDuplication = false;
                passed &= false; // "an exception should not thrown";
            }
            catch (NotSupportedException)
            {
                //expected
            }

            return passed;
        }

        public static bool ToggleSpeculativeDuplication()
        {
            var context = Utils.MakeBasicConfig(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                passed &= !context.EnableSpeculativeDuplication; // "Speculative Duplication enabled by default"
                context.EnableSpeculativeDuplication = true;
                passed &= context.EnableSpeculativeDuplication; // "Failed to enable speculative duplication"
                context.EnableSpeculativeDuplication = false;
                passed &= !context.EnableSpeculativeDuplication; // "Failed to disable speculative duplication"
                context.EnableSpeculativeDuplication = false;
                // ??? DryadLinqContext testContext = new DryadLinqContext(context);
                // ??? passed &= !testContext.EnableSpeculativeDuplication; // "Speculative Duplication enabled after copy"

            }
            catch (DryadLinqException)
            {
                passed &= false; // "Enabling and disabling speculative duplication should not throw"
            }
            return passed;
        }

        public static bool Bug15068_ConfigResourcesAPI()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                context.HeadNode = "MIKELID7"; // ???
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

                // ???
                //// read-only.
                //DryadLinqContext ctx = new DryadLinqContext(context);
                //passed &= (ctx.ResourcesToAdd.IsReadOnly == true); // "isReadOnly should be true"
                //passed &= (ctx.ResourcesToRemove.IsReadOnly == true); // "isReadOnly should be true"

                // clone was taken.
                context.ResourcesToAdd.Clear();
                context.ResourcesToRemove.Clear();
                // ???
                //passed &= (ctx.ResourcesToAdd.Count == 1); // "should be unaffected"
                //passed &= (ctx.ResourcesToRemove.Count == 2); // "should be unaffected"
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug14449_ContextShouldExposeVersionIDs()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                // ???
                //passed &= (context.Major >= 3); // "problem with HpcLinq client version"
                //passed &= (context.Major >= 3); // "problem with HpcLinq server version"

                //passed &= (context.ClientVersion.Major >= 3); // "problem with Dsc client version"
                //passed &= (context.ServerVersion.Major >= 3); // "problem with Dsc server version"

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug_16341_SubmitThrowsForDifferentContexts()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            var context2 = new DryadLinqContext(Config.cluster);
            context2.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                IQueryable<LineRecord> input2 = context2.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple2 = input2.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt2 = simple2.Select(x => x.First());

                DryadLinqQueryable.Submit(pt1, pt2);
                passed &= false;
            }
            catch (DryadLinqException)
            {
            }

            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                IQueryable<LineRecord> input2 = context2.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.storageKey, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple2 = input2.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt2 = simple2.Select(x => x.First());

                DryadLinqQueryable.SubmitAndWait(pt1, pt2);
                passed &= false;
            }
            catch (DryadLinqException)
            {
            }

            return passed;
        }

        public static bool Bug_16341_VariousTestsForSubmit()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                int[] data = new[] { 1, 2, 3 };
                var badQ1 = data.AsQueryable().Select(x => 100 + x);
                var badQ2 = data.AsQueryable().Select(x => 100 + x);

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> goodQ1 = simple.Select(x => x.First());

                IQueryable<LineRecord> input_copy = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple_copy = input_copy.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> goodQ2 = simple_copy.Select(x => x.First());


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
            catch (DryadLinqException)
            {
            }
            return passed;
        }


        public static bool template()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/x.txt";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

    }

}
