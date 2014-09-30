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
    public static class GroupByReduceTests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" GroupByReduceTests ");
            TestLog.Message(" **********************");


            var tests = new Dictionary<string, Action>()
              {
                  {"Decomposition_Average", () => Decomposition_Average(context) },
                  {"DistributiveResultSelector_1", () => DistributiveResultSelector_1(context) },
                  {"DistributiveSelect_1", () => DistributiveSelect_1(context) },
                  {"BuiltInCountIsDistributable", () => BuiltInCountIsDistributable(context) },
                  {"Bug12078_GroupByReduceWithResultSelectingAggregate", () => Bug12078_GroupByReduceWithResultSelectingAggregate(context) },
                  {"GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner", () => GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner(context) },
                  {"GroupByReduceWithCustomDecomposableFunction_DistributableCombiner", () => GroupByReduceWithCustomDecomposableFunction_DistributableCombiner(context) },
                  {"GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes", () => GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes(context) },
                  {"GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer", () => GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer(context) },
                  {"GroupByReduce_UseAllInternalDecomposables", () => GroupByReduce_UseAllInternalDecomposables(context) },
                  {"GroupByReduce_BuiltIn_First", () => GroupByReduce_BuiltIn_First(context) },
                  {"GroupByReduce_ResultSelector_ComplexNewExpression", () => GroupByReduce_ResultSelector_ComplexNewExpression(context) },
                  // ToDo {"GroupByReduce_ProgrammingManualExample", () => GroupByReduce_ProgrammingManualExample(context) },
                  {"GroupByReduce_SameDecomposableUsedTwice", () => GroupByReduce_SameDecomposableUsedTwice(context) },
                  {"GroupByReduce_APIMisuse", () => GroupByReduce_APIMisuse(context) },
                  {"GroupByReduce_ListInitializerReducer", () => GroupByReduce_ListInitializerReducer(context) },
                  {"GroupByReduce_CustomListInitializerReducer", () => GroupByReduce_CustomListInitializerReducer(context) },
                  {"GroupByReduce_BitwiseNegationOperator", () => GroupByReduce_BitwiseNegationOperator(context) },
              };


            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static bool Decomposition_Average(DryadLinqContext context)
        {
            string testName = "Decomposition_Average";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<double>[] result = new IEnumerable<double>[2];
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    double[] aggregates = pt1.GroupBy(x => x % 2).Select(g => g.Average()).ToArray(); 
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    double[] aggregates = pt1.GroupBy(x => x % 2).Select(g => g.Average()).ToArray(); 
                    result[1] = aggregates;
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

        public static bool DistributiveResultSelector_1(DryadLinqContext context)
        {
            string testName = "DistributiveResultSelector_1";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // this result selector satisfies "DistributiveOverConcat"
                    int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Sum()).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // this result selector satisfies "DistributiveOverConcat"
                    int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Sum()).ToArray();
                    result[1] = aggregates;
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

        public static bool DistributiveSelect_1(DryadLinqContext context)
        {
            string testName = "DistributiveSelect_1";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // this result selector satisfies "DistributiveOverConcat"
                    int[] aggregates = pt1.GroupBy(x => x % 2).Select(group => group.Sum()).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // this result selector satisfies "DistributiveOverConcat"
                    int[] aggregates = pt1.GroupBy(x => x % 2).Select(group => group.Sum()).ToArray();
                    result[1] = aggregates;
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

        public static bool BuiltInCountIsDistributable(DryadLinqContext context)
        {
            string testName = "BuiltInCountIsDistributable";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // Built in Count is Distributable as built-in logic knows to use Sum() as the combiner function.
                    // Count(a,b,c,d) = Sum(Count(a,b), Count(c,d))
                    int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Count()).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    // Built in Count is Distributable as built-in logic knows to use Sum() as the combiner function.
                    // Count(a,b,c,d) = Sum(Count(a,b), Count(c,d))
                    int[] aggregates = pt1.GroupBy(x => x % 2, (key, seq) => seq.Count()).ToArray();
                    result[1] = aggregates;
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

        public static bool Bug12078_GroupByReduceWithResultSelectingAggregate(DryadLinqContext context)
        {
            string testName = "Bug12078_GroupByReduceWithResultSelectingAggregate";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<double>[]result = new IEnumerable<double>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => seq.Aggregate((double)0, (acc, item) => acc + item, val => val / 100)).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => seq.Aggregate((double)0, (acc, item) => acc + item, val => val / 100)).ToArray();
                    result[1] = aggregates;
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

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner(DryadLinqContext context)
        {
            string testName = "GroupByReduceWithCustomDecomposableFunction_DistributableCombiner";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<double>[] result = new IEnumerable<double>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (k, g) => DecomposableFunc(g))
                                            .ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (k, g) => DecomposableFunc(g))
                                            .ToArray();
                    result[1] = aggregates;
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

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes(DryadLinqContext context)
        {
            string testName = "GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_DifferingTypes";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<string>[] result = new IEnumerable<string>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    string[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => DecomposableFunc2(seq)).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    string[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => DecomposableFunc2(seq)).ToArray();
                    result[1] = aggregates;
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

        public static bool GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer(DryadLinqContext context)
        {
            string testName = "GroupByReduceWithCustomDecomposableFunction_DistributableCombiner_NoFinalizer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<string>[] result = new IEnumerable<string>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    string[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => DecomposableFunc3(seq)).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    string[] aggregates = pt1.Select(x => (double)x)
                                            .GroupBy(x => 0, (key, seq) => DecomposableFunc3(seq)).ToArray();
                    result[1] = aggregates;
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

        public static bool GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner(DryadLinqContext context)
        {
            string testName = "GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<double>[] result = new IEnumerable<double>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                              .GroupBy(x => 0, (key, seq) => DecomposableFunc4(seq)).ToArray();
                    result[0] = aggregates;
                }
                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    double[] aggregates = pt1.Select(x => (double)x)
                                              .GroupBy(x => 0, (key, seq) => DecomposableFunc4(seq)).ToArray();
                    result[1] = aggregates;
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

        #endregion GroupByReduceWithCustomDecomposableFunction_NonDistributableCombiner

        public static bool GroupByReduce_UseAllInternalDecomposables(DryadLinqContext context)
        {
            string testName = "GroupByReduce_UseAllInternalDecomposables";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    var aggregates = pt1.Select(x => (double)x)
                                         .GroupBy(x => 0, (key, seq) => seq.Count())
                                         .GroupBy(x => 0, (key, seq) => seq.LongCount())
                                         .GroupBy(x => 0, (key, seq) => seq.Max())
                                         .GroupBy(x => 0, (key, seq) => seq.Min())
                                         .GroupBy(x => 0, (key, seq) => seq.Sum())
                                         .GroupBy(x => 0, (key, seq) => seq.Average())
                                         .GroupBy(x => 0, (key, seq) => seq.Aggregate((x, y) => x + y))
                                         .GroupBy(x => 0, (key, seq) => seq.Any(x => true))
                                         .SelectMany(x => new[] { x })
                                         .GroupBy(x => 0, (key, seq) => seq.All(x => true))
                                         .SelectMany(x => new[] { x })
                                         .GroupBy(x => 0, (key, seq) => seq.Contains(true))
                                         .SelectMany(x => new[] { x })
                                         .GroupBy(x => 0, (key, seq) => seq.Distinct())
                                         .SelectMany(x => new[] { x })
                                         .GroupBy(x => 0, (key, seq) => seq.First())
                                         .ToArray();
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

        public static bool GroupByReduce_BuiltIn_First(DryadLinqContext context)
        {
            string testName = "GroupByReduce_BuiltIn_First";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                    int[] aggregates = pt1.GroupBy(x => 0, (key, seq) => seq.First()).ToArray();
                    // the output of First can be the first item of either partition.
                    passed &= aggregates.SequenceEqual(new[] { 1 }) || aggregates.SequenceEqual(new[] { 101 }); // ToDo: remove hard coded
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

        public static bool GroupByReduce_ResultSelector_ComplexNewExpression(DryadLinqContext context)
        {
            string testName = "GroupByReduce_ResultSelector_ComplexNewExpression";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<int>[] result = new IEnumerable<int>[2];

                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetGroupByReduceDataSet(context);
                var aggregates = pt1.GroupBy(x => 0, (key, seq) => new KeyValuePair<int, KeyValuePair<double, double>>(key, new KeyValuePair<double, double>(seq.Average(), seq.Average()))).ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var expected = pt2.GroupBy(x => 0, (key, seq) => new KeyValuePair<int, KeyValuePair<double, double>>(key, new KeyValuePair<double, double>(seq.Average(), seq.Average()))).ToArray();

                passed &= aggregates.SequenceEqual(expected);
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        #region GroupByReduce_ProgrammingManualExample

        // ToDo:
        //public static bool GroupByReduce_ProgrammingManualExample(DryadLinqContext context)
        //{
        //    string testName = "GroupByReduce_ProgrammingManualExample";
        //    TestLog.TestStart(testName);

        //    bool passed = true;
        //    try
        //    {
        //        // cluster
        //        {
        //            context.LocalDebug = false;
        //            IEnumerable<IEnumerable<int>> rawdata = new[] { Enumerable.Range(0, 334), Enumerable.Range(334, 333), Enumerable.Range(667, 333) };
        //            IQueryable<IEnumerable<int>> data = context.FromEnumerable(rawdata);

        //            var count = data.Count();
        //            //decimal sum = data.Sum();
        //            var min = data.Min();
        //            var max = data.Max();
        //            var uniques = data.Distinct().Count();

        //            var results = data.GroupBy(x => x % 10, (key, seq) => new KeyValuePair<int, double>(key, seq.MyAverage()))
        //                              .OrderBy(y => y.Key)
        //                              .ToArray();

        //            passed &= (results.Count() == 10);
        //            passed &= (results[0].Key == 0); // "first element should be key=0");
        //            passed &= (results[0].Value == 495); // "first element should be value=495 ie avg(0,10,20,..,990)");
        //        }
        //    }
        //    catch (Exception)
        //    {
        //        passed &= false;
        //    }

        //    TestLog.LogResult(new TestResult(testName, context, passed));
        //    return passed;
        //}

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

        public static bool GroupByReduce_SameDecomposableUsedTwice(DryadLinqContext context)
        {
            string testName = "GroupByReduce_SameDecomposableUsedTwice";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var results0 = pt1.GroupBy(x => x % 2, (k, g) => MyFunc(k, DecomposableFunc5(g), DecomposableFunc5(g), g.Average())).ToArray();
                var results0_sorted = results0.OrderBy(x => x.Key).ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var results1 = pt2.GroupBy(x => x % 2, (k, g) => MyFunc(k, DecomposableFunc5(g), DecomposableFunc5(g), g.Average())).ToArray();
                var results1_sorted = results1.OrderBy(x => x.Key).ToArray();

                passed &= (results0_sorted.Length == results1_sorted.Length);
                passed &= (results0_sorted[0].Key == results1_sorted[0].Key);
                passed &= (results0_sorted[0].A == results1_sorted[0].A); 
                passed &= (results0_sorted[0].B == results1_sorted[0].B);
                passed &= (results0_sorted[0].Av == results1_sorted[0].Av);

                passed &= (results0_sorted[1].Key == results1_sorted[1].Key);
                passed &= (results0_sorted[1].A == results1_sorted[1].A);
                passed &= (results0_sorted[1].B == results1_sorted[1].B);
                passed &= (results0_sorted[1].Av == results1_sorted[1].Av);
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
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
        internal static bool GroupByReduce_APIMisuse(DryadLinqContext context)
        {
            string testName = "GroupByReduce_APIMisuse";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);

                // internal-visibility decomposable type should fail.
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable1(g)).ToArray();
                    passed &= false; // "exception should be thrown"
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypeMustBePublic"));
                }

                // decomposable type doesn't implement IDecomposable or IDecomposableRecursive
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable2(g)).ToArray();
                    passed &= false; //"exception should be thrown");
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypeDoesNotImplementInterface"));
                }

                // decomposable type implements more than one IDecomposable or IDecomposableRecursive
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable3(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypeImplementsTooManyInterfaces"));
                }

                // decomposable type doesn't have public default ctor
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable4(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypeDoesNotHavePublicDefaultCtor"));
                }

                // decomposable type input type doesn't match
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable5(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypesDoNotMatch"));
                }

                // decomposable type output type doesn't match
                try
                {
                    pt1.GroupBy(x => x, (k, g) => BadDecomposable6(g)).ToArray();
                    passed &= false;
                }
                catch (DryadLinqException Ex)
                {
                    passed &= (Ex.ErrorCode == ReflectionHelper.GetDryadLinqErrorCode("DecomposerTypesDoNotMatch"));
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

        public static bool GroupByReduce_ListInitializerReducer(DryadLinqContext context)
        {
            string testName = "GroupByReduce_ListInitializerReducer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var results0 = pt1.GroupBy(x => x % 2, (k, g) => new List<int>() { k, g.Count(), g.Sum() }).ToArray();
                var resultsSorted0 = results0.OrderBy(list => list[0]).ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var results1 = pt2.GroupBy(x => x % 2, (k, g) => new List<int>() { k, g.Count(), g.Sum() }).ToArray();
                var resultsSorted1 = results1.OrderBy(list => list[0]).ToArray();

                passed &= (resultsSorted0[0][0] == resultsSorted1[0][0]); 
                passed &= (resultsSorted0[0][1] == resultsSorted1[0][1]);
                passed &= (resultsSorted0[0][2] == resultsSorted1[0][2]); 

                passed &= (resultsSorted0[1][0] == resultsSorted1[1][0]);
                passed &= (resultsSorted0[1][1] == resultsSorted1[1][1]); 
                passed &= (resultsSorted0[1][2] == resultsSorted1[1][2]); 
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool GroupByReduce_CustomListInitializerReducer(DryadLinqContext context)
        {
            string testName = "GroupByReduce_CustomListInitializerReducer";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var results0 = pt1.GroupBy(x => x % 2, (k, g) => new MultiParamInitializerClass() { 
                                                                                    {k, g.Count(), g.Sum()} ,  // one item, comprising three components
                                                                                    }).ToArray();
                var resultsSorted0 = results0.OrderBy(list => list.Key).ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var results1 = pt2.GroupBy(x => x % 2, (k, g) => new MultiParamInitializerClass() { 
                                                                                    {k, g.Count(), g.Sum()} ,  // one item, comprising three components
                                                                                    }).ToArray();
                var resultsSorted1 = results1.OrderBy(list => list.Key).ToArray();

                passed &= (resultsSorted0[0].Key == resultsSorted1[0].Key); 
                passed &= (resultsSorted0[0].Count() == resultsSorted1[0].Count());
                passed &= (resultsSorted0[0].Sum() == resultsSorted1[0].Sum()); 

                passed &= (resultsSorted0[1].Key == resultsSorted1[1].Key); 
                passed &= (resultsSorted0[1].Count() == resultsSorted1[1].Count()); 
                passed &= (resultsSorted0[1].Sum() == resultsSorted1[1].Sum()); 
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
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

        public static bool GroupByReduce_BitwiseNegationOperator(DryadLinqContext context)
        {
            string testName = "GroupByReduce_BitwiseNegationOperator";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var results0 = pt1.GroupBy(x => x % 2, (k, g) => new KeyValuePair<int, int>(k, ~g.Sum())).ToArray();
                var resultsSorted0 = results0.OrderBy(list => list.Key).ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var results1 = pt2.GroupBy(x => x % 2, (k, g) => new KeyValuePair<int, int>(k, ~g.Sum())).ToArray();
                var resultsSorted1 = results1.OrderBy(list => list.Key).ToArray();

                passed &= (resultsSorted0[0].Key == resultsSorted1[0].Key);
                passed &= (resultsSorted0[0].Value == resultsSorted1[0].Value);

                passed &= (resultsSorted0[1].Key == resultsSorted1[1].Key);
                passed &= (resultsSorted0[1].Value == resultsSorted1[1].Value);
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
