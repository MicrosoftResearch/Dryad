using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DryadLinqTests
{
    public static class GroupByReduceTests
    {
        public static bool Decomposition_Average()
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

                double[] aggregates = pt1.GroupBy(x => x % 2).Select(g => g.Average()).ToArray();
                //int[] expected = new[] { 1 + 3 + 5 + 7 + 9 + 11, 2 + 4 + 6 + 8 + 10 + 12 };

                ////note the order of the result elements is not guaranteed, so order them before testing
                //int[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                //int[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                //passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool DistributiveResultSelector_1()
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

                // this result selector satisfies "DistributiveOverConcat"
                int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Sum()).ToArray();
                int[] expected = new[] { 1 + 3 + 5 + 7 + 9 + 11, 2 + 4 + 6 + 8 + 10 + 12 };

                //note the order of the result elements is not guaranteed, so order them before testing
                int[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                int[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool DistributiveSelect_1()
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

                // this result selector satisfies "DistributiveOverConcat"
                int[] aggregates = pt1.GroupBy(x => x % 2).Select(group => group.Sum()).ToArray();
                int[] expected = new[] { 1 + 3 + 5 + 7 + 9 + 11, 2 + 4 + 6 + 8 + 10 + 12 };

                //note the order of the result elements is not guaranteed, so order them before testing
                int[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                int[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool BuiltInCountIsDistributable()
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

                // Built in Count is Distributable as built-in logic knows to use Sum() as the combiner function.
                // Count(a,b,c,d) = Sum(Count(a,b), Count(c,d))
                int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Count()).ToArray();
                int[] expected = new[] { 6, 6 }; // six elements in each full group.

                //note the order of the result elements is not guaranteed, so order them before testing
                int[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                int[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool Bug12078_GroupByReduceWithResultSelectingAggregate()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                double[] aggregates = data
                                        .Select(x => (double)x)
                                        .GroupBy(x => 0, (key, seq) => seq.Aggregate((double)0, (acc, item) => acc + item, val => val / 100)).ToArray();
                double[] expected = new[] { Enumerable.Range(1, 200).Sum() / 100.0 };

                //note the order of the result elements is not guaranteed, so order them before testing
                double[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                double[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        #region GroupByReduceWithCustomDecomposableFunction_DistributableCombiner

        [Decomposable(typeof(Decomposer_1))]
        public static double DecomposableFunc(IEnumerable<double> seq)
        {
            // hard to test with context system.. TestUtils.Assert(HpcLinq.LocalDebug, "This method should only be called during LocalDebug");
            return seq.Aggregate((double)0, (acc, item) => acc + item, val => val / 100);
        }
        public class Decomposer_1 : IDecomposable<double, double, double>
        {
            public void Initialize(object state) { }

            public double Seed(double source)
            {
                return source;
            }

            public double Accumulate(double a, double x)
            {
                return a + x;
            }

            public double RecursiveAccumulate(double a, double x)
            {
                return a + x;
            }

            public double FinalReduce(double a)
            {
                return a / 100;
            }
        }

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                double[] aggregates = data
                                         .Select(x => (double)x)
                                         .GroupBy(x => 0, (k, g) => DecomposableFunc(g))
                                         .ToArray();
                double[] expected = new[] { Enumerable.Range(1, 200).Sum() / 100.0 };

                //note the order of the result elements is not guaranteed, so order them before testing
                double[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                double[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
        
        #endregion GroupByReduceWithCustomDecomposableFunction_DistributableCombiner

        #region GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes
        // Tests a fully decomposed function whose reducer changes types.
        [Decomposable(typeof(Decomposer_2))]
        public static string DecomposableFunc2(IEnumerable<double> seq)
        {
            //TestUtils.Assert(HpcLinq.LocalDebug, "This method should only be called during LocalDebug");
            return seq.Aggregate((double)0, (acc, item) => acc + item, val => ("hello:" + val.ToString()));
        }
        public class Decomposer_2 : IDecomposable<double, double, string>
        {
            public void Initialize(object state) { }

            public double Seed(double source)
            {
                return source;
            }

            public double Accumulate(double a, double x)
            {
                return a + x;
            }

            public double RecursiveAccumulate(double a, double x)
            {
                return a + x;
            }

            public string FinalReduce(double a)
            {
                return ("hello:" + a.ToString());
            }
        }

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                string[] aggregates = data
                                         .Select(x => (double)x)
                                         .GroupBy(x => 0, (key, seq) => DecomposableFunc2(seq)).ToArray();
                string[] expected = new[] { "hello:" + Enumerable.Range(1, 200).Sum() };

                //note the order of the result elements is not guaranteed, so order them before testing
                string[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                string[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        #endregion GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes

        #region GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer
        // Tests a decomposed function with no need for a particular reduce.  
        // The combiner changes type, and the recursive-combiner operators on the altered type
        // The reducer just calls combiner again.
        [Decomposable(typeof(Decomposer_3))]
        public static string DecomposableFunc3(IEnumerable<double> seq)
        {
            // TestUtils.Assert(HpcLinq.LocalDebug, "This method should only be called during LocalDebug");
            return seq.Aggregate("0", (acc, item) => (double.Parse(acc) + item).ToString());
        }
        public class Decomposer_3 : IDecomposable<double, string, string>
        {
            public void Initialize(object state) { }

            public string Seed(double source)
            {
                return source.ToString();
            }

            public string Accumulate(string a, double x)
            {
                return (double.Parse(a) + x).ToString();
            }

            public string RecursiveAccumulate(string a, string x)
            {
                return (double.Parse(a) + double.Parse(x)).ToString();
            }

            public string FinalReduce(string a)
            {
                return a;
            }
        }

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                string[] aggregates = data
                                         .Select(x => (double)x)
                                         .GroupBy(x => 0, (key, seq) => DecomposableFunc3(seq)).ToArray();
                string[] expected = new[] { Enumerable.Range(1, 200).Sum().ToString() };

                //note the order of the result elements is not guaranteed, so order them before testing
                string[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                string[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        #endregion GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer

        #region GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner
        // Tests simplified pattern where the Combiner is not recursively applied.
        // Note: Func4 can be represented as a decomposable with distributive-combiner and a finalizer.. but here we choose not to.
        //       Because of the form of the Combiner, it is critical that it not be used recursively.
        [Decomposable(typeof(Decomposer_4))]
        public static double DecomposableFunc4(IEnumerable<double> seq)
        {
            // TestUtils.Assert(HpcLinq.LocalDebug, "This method should only be called during LocalDebug");
            return seq.Aggregate(0.0, (acc, item) => acc + item, acc => acc / 100);
        }
        public class Decomposer_4 : IDecomposable<double, double, double>
        {
            public void Initialize(object state) { }

            public double Seed(double source)
            {
                return source;
            }

            public double Accumulate(double a, double x)
            {
                return a + x;
            }

            public double RecursiveAccumulate(double a, double x)
            {
                return a + x;
            }

            public double FinalReduce(double a)
            {
                return a / 100;
            }
        }

        public static bool GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                double[] aggregates = data
                                         .Select(x => (double)x)
                                         .GroupBy(x => 0, (key, seq) => DecomposableFunc4(seq)).ToArray();
                double[] expected = new[] { Enumerable.Range(1, 200).Sum() / 100.0 };

                //note the order of the result elements is not guaranteed, so order them before testing
                double[] aggregatesOrdered = aggregates.OrderBy(x => x).ToArray();
                double[] expectedOrdered = expected.OrderBy(x => x).ToArray();

                passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        #endregion GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner

        public static bool GroupByReduce_BuiltIn_First()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                int[] aggregates = data
                                     .GroupBy(x => 0, (key, seq) => seq.First())
                                     .ToArray();

                // the output of First can be the first item of either partition.
                passed &= aggregates.SequenceEqual(new[] { 1 }) || aggregates.SequenceEqual(new[] { 101 });
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool GroupByReduce_ResultSelector_ComplexNewExpression()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                var aggregates = data.GroupBy(x => 0, (key, seq) => new KeyValuePair<int, KeyValuePair<double, double>>(key, new KeyValuePair<double, double>(seq.Average(), seq.Average()))).ToArray();

                var expected = new KeyValuePair<int, KeyValuePair<double, double>>[] { new KeyValuePair<int, KeyValuePair<double, double>>(0, new KeyValuePair<double, double>(100.5, 100.5)) };

                passed &= aggregates.SequenceEqual(expected);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        #region GroupByReduce_ProgrammingManualExample

        public static bool GroupByReduce_ProgrammingManualExample()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string filesetName = "DevUnitTest/0to999integers"; 
                Utils.DeleteFile(Config.accountName, Config.storageKey, Config.containerName, filesetName, true);

                IEnumerable<IEnumerable<int>> rawdata = new[] { Enumerable.Range(0, 334), Enumerable.Range(334, 333), Enumerable.Range(667, 333) };
                // ??? DscIngressHelpers.AsDryadQueryPartitions(context, rawdata, filesetName, DscCompressionScheme.None);
                var data = context.FromStore<int>(filesetName);

                var count = data.AsEnumerable().Count();
                var sum = data.AsEnumerable().Sum();
                var min = data.AsEnumerable().Min();
                var max = data.AsEnumerable().Max();
                var uniques = data.AsEnumerable().Distinct().Count();

                //Console.WriteLine("DATA:: count:{0} uniques:{1} sum:{2}, min:{3}, max:{4}", count, uniques, sum, min, max);

                // ???
                //var results = data
                //                 .GroupBy(x => x % 10, (key, seq) => new KeyValuePair<int, double>(key, seq.MyAverage()))
                //                 .OrderBy(y => y.Key)
                //                 .ToArray();

                ////foreach (var result in results)
                ////    Console.WriteLine("For group {0} the average is {1}", result.Key, result.Value);

                //passed &= (results.Count() == 10);
                //passed &= (results[0].Key == 0); // "first element should be key=0");
                //passed &= (results[0].Value == 495); // "first element should be value=495 ie avg(0,10,20,..,990)");

            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        [Decomposable(typeof(Decomposer_5))]
        public static double MyAverage(this IEnumerable<int> recordSequence)
        {
            int count = 0, sum = 0;
            foreach (var r in recordSequence)
            {
                sum += r;
                count++;
            }
            if (count == 0) throw new Exception("Can't average empty sequence");
            return (double)sum / (double)count;
        }

        [Serializable]
        public struct Partial
        {
            public int PartialSum;
            public int PartialCount;
        }

        public class Decomposer_5 : IDecomposable<int, Partial, double>
        {
            public void Initialize(object state) { }

            public Partial Seed(int x)
            {
                Partial p = new Partial();
                p.PartialSum = x;
                p.PartialCount = 1;
                return p;
            }

            public Partial Accumulate(Partial a, int x)
            {
                Partial p = new Partial();
                p.PartialSum = a.PartialSum + x;
                p.PartialCount = a.PartialCount + 1;
                return p;
            }

            public Partial RecursiveAccumulate(Partial a, Partial x)
            {
                Partial p = new Partial();
                p.PartialSum = a.PartialSum + x.PartialSum;
                p.PartialCount = a.PartialCount + x.PartialCount;
                return p;
            }

            public double FinalReduce(Partial a)
            {
                if (a.PartialCount == 0) throw new Exception("Can't average empty sequence");
                return (double)a.PartialSum / (double)a.PartialCount;
            }
        }
        
        #endregion GroupByReduce_ProgrammingManualExample


        #region GroupByReduce_SameDecomposableUsedTwice

        public static bool GroupByReduce_SameDecomposableUsedTwice()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var results = pt1.GroupBy(x => x % 2, (k, g) => MyFunc(k, DecomposableFunc5(g), DecomposableFunc5(g), g.Average())).ToArray();

                //key0: count = 6, av = av(2,4,6,8,10,12) = 7
                //key1: count = 6, av = av(1,3,5,7,9,11) = 6

                //local sort.. so that keys are in order.
                var results_sorted = results.OrderBy(x => x.Key).ToArray();

                passed &= (results_sorted.Length == 2); // "wrong results"

                passed &= (results_sorted[0].Key == 0); // "wrong results"
                passed &= (results_sorted[0].A == 6); // "wrong results"
                passed &= (results_sorted[0].B == 6); // "wrong results"
                passed &= (results_sorted[0].Av == 7.0); // "wrong results"

                passed &= (results_sorted[1].Key == 1); // "wrong results"
                passed &= (results_sorted[1].A == 6); // "wrong results"
                passed &= (results_sorted[1].B == 6); // "wrong results"
                passed &= (results_sorted[1].Av == 6.0); // "wrong results"
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static MyStruct3 MyFunc(int key, int a, int b, double av)
        {
            return new MyStruct3(key, a, b, av);
        }
        [Decomposable(typeof(Decomposer_6))]
        private static int DecomposableFunc5(IEnumerable<int> g)
        {
            return g.Count();
        }
        public class Decomposer_6 : IDecomposable<int, int, int>
        {
            public void Initialize(object state) { }

            public int Seed(int source) { return 1; }

            public int Accumulate(int a, int x)
            {
                return a + 1;
            }

            public int RecursiveAccumulate(int a, int x)
            {
                return a + x;
            }

            public int FinalReduce(int a)
            {
                return a;
            }
        }
        [Serializable]
        public struct MyStruct3
        {
            public int Key;
            public int A;
            public int B;
            public double Av;

            public MyStruct3(int key, int a, int b, double av)
            {
                Key = key; A = a; B = b; Av = av;
            }
        }

        #endregion GroupByReduce_SameDecomposableUsedTwice

        #region API_Misuse
        internal static bool GroupByReduce_APIMisuse()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                if (context.LocalDebug)
                {
                    // "decomposition logic doesn't run in LocalDebug.. skipping";
                    return true;
                }

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                // internal-visibility decomposable type should fail.
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable1(g)).ToArray();
                    passed &= false; // "exception should be thrown"
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypeMustBePublic); // "error code is wrong"
                }

                // decomposable type doesn't implement IDecomposable or IDecomposableRecursive
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable2(g)).ToArray();
                    passed &= false; //"exception should be thrown");
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypeDoesNotImplementInterface);
                }

                // decomposable type implements more than one IDecomposable or IDecomposableRecursive
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable3(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypeImplementsTooManyInterfaces);
                }

                // decomposable type doesn't have public default ctor
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable4(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypeDoesNotHavePublicDefaultCtor);
                }

                // decomposable type input type doesn't match
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable5(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypesDoNotMatch);
                }

                // decomposable type output type doesn't match
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable6(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException)
                {
                    //??? passed &= (Ex.ErrorCode == DryadLinqErrorCode.DecomposerTypesDoNotMatch);
                }
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        [Decomposable(typeof(BadDecomposerType1))]
        private static int BadDecomposable1(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        internal class BadDecomposerType1 : IDecomposable<int, int, int>
        {
            public void Initialize(object state) { }
            public int Seed(int x) { return x; }
            public int Accumulate(int a, int x) { throw new NotImplementedException(); }
            public int RecursiveAccumulate(int a, int x) { throw new NotImplementedException(); }
            public int FinalReduce(int a) { throw new NotImplementedException(); }
        }
        [Decomposable(typeof(BadDecomposerType2))]
        private static int BadDecomposable2(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        public class BadDecomposerType2
        {
        }
        [Decomposable(typeof(BadDecomposerType3))]
        private static int BadDecomposable3(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        public class BadDecomposerType3 : IDecomposable<int, int, int>
        {
            public void Initialize(object state) { }
            public int Seed(int x) { return x; }
            public int Accumulate(int a, int x) { throw new NotImplementedException(); }
            public int RecursiveAccumulate(int a, int x) { throw new NotImplementedException(); }
            public int FinalReduce(int a) { throw new NotImplementedException(); }
        }
        [Decomposable(typeof(BadDecomposerType4))]
        private static int BadDecomposable4(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        public class BadDecomposerType4 : IDecomposable<int, int, int>
        {
            internal BadDecomposerType4() { }
            public BadDecomposerType4(int x) { }
            public void Initialize(object state) { }
            public int Seed(int x) { return x; }
            public int Accumulate(int a, int x) { throw new NotImplementedException(); }
            public int RecursiveAccumulate(int a, int x) { throw new NotImplementedException(); }
            public int FinalReduce(int a) { throw new NotImplementedException(); }
        }
        [Decomposable(typeof(BadDecomposerType5))]
        private static int BadDecomposable5(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        public class BadDecomposerType5 : IDecomposable<double, int, int>
        {
            public void Initialize(object state) { }
            public int Seed(double s) { throw new NotImplementedException(); }
            public int Accumulate(int a, double x) { throw new NotImplementedException(); }
            public int RecursiveAccumulate(int a, int x) { throw new NotImplementedException(); }
            public int FinalReduce(int a) { throw new NotImplementedException(); }
        }
        [Decomposable(typeof(BadDecomposerType6))]
        private static int BadDecomposable6(IEnumerable<int> g)
        {
            throw new NotImplementedException();
        }
        public class BadDecomposerType6 : IDecomposable<int, int, double>
        {
            public void Initialize(object state) { }
            public int Seed(int s) { throw new NotImplementedException(); }
            public int Accumulate(int a, int x) { throw new NotImplementedException(); }
            public int RecursiveAccumulate(int a, int x) { throw new NotImplementedException(); }
            public double FinalReduce(int a) { throw new NotImplementedException(); }
        }

        #endregion API_Misuse

        public static bool GroupByReduce_ListInitializerReducer()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var results = pt1.GroupBy(x => x % 2, (k, g) => new List<int>() { k, g.Count(), g.Sum() }).ToArray();

                //local sort.. so that keys are in order.
                var resultsSorted = results.OrderBy(list => list[0]).ToArray();

                //key0: count = 6, sum = 42
                //key1: count = 6, sum = 36

                passed &= (resultsSorted[0][0] == 0); // "incorrect results.1"
                passed &= (resultsSorted[0][1] == 6); // "incorrect results.2"
                passed &= (resultsSorted[0][2] == 42); // "incorrect results.3"

                passed &= (resultsSorted[1][0] == 1); // "incorrect results.4"
                passed &= (resultsSorted[1][1] == 6); // "incorrect results.5"
                passed &= (resultsSorted[1][2] == 36); // "incorrect results.6"
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool GroupByReduce_CustomListInitializerReducer()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var results = pt1.GroupBy(x => x % 2, (k, g) => new MultiParamInitializerClass() { 
                                                                                    {k, g.Count(), g.Sum()} ,  // one item, comprising three components
                                                                                    }).ToArray();
                //local sort.. so that keys are in order.
                var resultsSorted = results.OrderBy(list => list.Key).ToArray();

                //key0: count = 6, sum = 42
                //key1: count = 6, sum = 36

                passed &= (resultsSorted[0].Key == 0); // "incorrect results.1"
                passed &= (resultsSorted[0].Count() == 6); // "incorrect results.2"
                passed &= (resultsSorted[0].Sum() == 42); // "incorrect results.3"

                passed &= (resultsSorted[1].Key == 1); // "incorrect results.4"
                passed &= (resultsSorted[1].Count() == 6); // "incorrect results.5"
                passed &= (resultsSorted[1].Sum() == 36); // "incorrect results.6"
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        // note: must be IEnumerable<> to be allowed to participate in list-initializer syntax.
        // we are cheating here and only supporting one "add" call, just as an example.
        [Serializable]
        public class MultiParamInitializerClass : IEnumerable<int>
        {
            public int Key;
            public int Sum;
            public int Count;

            public void Add(int key, int count, int sum)
            {
                Key = key;
                Count = count;
                Sum = sum;
            }
            public IEnumerator<int> GetEnumerator()
            {
                yield return Key;
                yield return Count;
                yield return Sum;
            }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public static bool GroupByReduce_BitwiseNegationOperator()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var results = pt1.GroupBy(x => x % 2, (k, g) => new KeyValuePair<int, int>(k, ~g.Sum())).ToArray();

                //local sort.. so that keys are in order.
                var resultsSorted = results.OrderBy(list => list.Key).ToArray();

                //key0: count = 6, sum = 42
                //key1: count = 6, sum = 36

                passed &= (resultsSorted[0].Key == 0); // "incorrect results.1"
                passed &= (resultsSorted[0].Value == ~42); // "incorrect results.2"

                passed &= (resultsSorted[1].Key == 1); // "incorrect results.3"
                passed &= (resultsSorted[1].Value == ~36); // "incorrect results.4"
            }
            catch (DryadLinqException)
            {
                passed &= false;
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
                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateGroupByReduceDataSet());
                IQueryable<int> data = simple.Select(x => x.First());

                //passed &= aggregatesOrdered.SequenceEqual(expectedOrdered);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
    }
}
