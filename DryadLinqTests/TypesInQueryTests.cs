using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace DryadLinqTests
{
    public class TypesInQueryTests
    {
        public static void Run(DryadLinqContext context, string matchPattern)
        {
            TestLog.Message(" **********************");
            TestLog.Message(" TypesInQueryTests ");
            TestLog.Message(" **********************");

            var tests = new Dictionary<string, Action>()
              {
                  {"NonSealedTypeRecords", () => NonSealedTypeRecords(context) },
                  {"DerivedTypeRecords", () => DerivedTypeRecords(context) },
                  {"ObjectRecords", () => ObjectRecords(context) },
                  {"BadRecordsNotSerialized", () => BadRecordsNotSerialized(context) },
                  {"GroupByWithAnonymousTypes_Bug15675", () => GroupByWithAnonymousTypes_Bug15675(context) },
                  {"GroupByWithAnonymousTypes_Pipeline", () => GroupByWithAnonymousTypes_Pipeline(context) },
                  {"GroupByWithAnonymousTypes_MultipleAnonymousTypes", () => GroupByWithAnonymousTypes_MultipleAnonymousTypes(context) },
                  {"GroupByWithAnonymousTypes_GenericWithAnonTypeParam", () => GroupByWithAnonymousTypes_GenericWithAnonTypeParam(context) },
                  {"GroupByWithAnonymousTypes_ArrayOfAnon", () => GroupByWithAnonymousTypes_ArrayOfAnon(context) },
                  {"GroupByWithAnonymousTypes_NestedAnonTypes", () => GroupByWithAnonymousTypes_NestedAnonTypes(context) },
              };

            foreach (var test in tests)
            {
                if (Regex.IsMatch(test.Key, matchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    test.Value.Invoke();
                }
            }
        }

        public static bool NonSealedTypeRecords(DryadLinqContext context)
        {
            string testName = "NonSealedTypeRecords";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<NonSealedClass>[] result = new IEnumerable<NonSealedClass>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => new NonSealedClass()).ToArray();
                    result[0] = output;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => new NonSealedClass()).ToArray();
                    result[1] = output;
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

        public static bool DerivedTypeRecords(DryadLinqContext context)
        {
            string testName = "DerivedTypeRecords";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<ParentClass>[] result1 = new IEnumerable<ParentClass>[2];
                IEnumerable<ChildClass>[] result2 = new IEnumerable<ChildClass>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output1 = pt1.Select(x => new ParentClass()).ToArray();
                    var output2 = pt1.Select(x => new ChildClass()).ToArray();
                    result1[0] = output1;
                    result2[0] = output2;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output1 = pt1.Select(x => new ParentClass()).ToArray();
                    var output2 = pt1.Select(x => new ChildClass()).ToArray();
                    result1[1] = output1;
                    result2[1] = output2;
                }

                // compare result
                try
                {
                    Validate.Check(result1);
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

        public static bool ObjectRecords(DryadLinqContext context)
        {
            string testName = "ObjectRecords";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<object>[] result = new IEnumerable<object>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => new object()).ToArray();
                    result[0] = output;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => new object()).ToArray();
                    result[1] = output;
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

        public static bool BadRecordsNotSerialized(DryadLinqContext context)
        {
            string testName = "BadRecordsNotSerialized";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                IEnumerable<string>[] result = new IEnumerable<string>[2];

                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => (x % 2 == 0 ? new ChildClass() : new ParentClass())).Select(x => x is ChildClass ? "child" : "parent").ToArray();
                    result[0] = output;
                }

                // local
                {
                    context.LocalDebug = true;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var output = pt1.Select(x => (x % 2 == 0 ? new ChildClass() : new ParentClass())).Select(x => x is ChildClass ? "child" : "parent").ToArray();
                    result[1] = output;
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

        //Bug 15675 -- GroupBy creates a DryadOrderByNode which is confused by the anonymous type.
        //             specifically, the call to DryadSort incorrectly referenced both the anonymous type and the code-gen proxy for the anonymous type.
        //          -- the fix was to clean up how & when we process the types involved in the query so that only the proxy would be referenced.
        public static bool GroupByWithAnonymousTypes_Bug15675(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_Bug15675";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var result1 = pt1.Select(i => new { Num = i % 10 })
                                    .GroupBy(x => x.Num, x => x.Num)
                                    .ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var result2 = pt2.Select(i => new { Num = i % 10 })
                                    .GroupBy(x => x.Num, x => x.Num)
                                    .ToArray();

                passed &= (result1.Count() == result2.Count()); 
                passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count());
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool GroupByWithAnonymousTypes_Pipeline(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_Pipeline";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var result1 = pt1.Select(i => new { Num = i % 10 })
                                    .Where(x => true)
                                    .GroupBy(x => x.Num, x => x.Num)
                                    .ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var result2 = pt2.Select(i => new { Num = i % 10 })
                                    .Where(x => true)
                                    .GroupBy(x => x.Num, x => x.Num)
                                    .ToArray();

                passed &= (result1.Count() == result2.Count());
                passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count()); 
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool GroupByWithAnonymousTypes_MultipleAnonymousTypes(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_MultipleAnonymousTypes";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var result1 = pt1.Select(i => new { Num = i % 10 })
                                    .Where(x => true)
                                    .Select(i => new { Num2 = i.Num })
                                    .GroupBy(x => x.Num2, x => x.Num2)
                                    .ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var result2 = pt2.Select(i => new { Num = i % 10 })
                                    .Where(x => true)
                                    .Select(i => new { Num2 = i.Num })
                                    .GroupBy(x => x.Num2, x => x.Num2)
                                    .ToArray();

                passed &= (result1.Count() == result2.Count());
                passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count());
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool GroupByWithAnonymousTypes_GenericWithAnonTypeParam(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_GenericWithAnonTypeParam";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                {
                    context.LocalDebug = false;
                    IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                    var result1 = pt1.Select(i => MakeNewMyGenericType(new { Num = i % 10 }))
                                    .GroupBy(x => x.Field.Num, x => x.Field.Num)
                                    .ToArray();

                    // local
                    context.LocalDebug = true;
                    IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                    var result2 = pt2.Select(i => MakeNewMyGenericType(new { Num = i % 10 }))
                                    .GroupBy(x => x.Field.Num, x => x.Field.Num)
                                    .ToArray();

                    passed &= (result1.Count() == result2.Count());
                    passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count());
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

        public static bool GroupByWithAnonymousTypes_ArrayOfAnon(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_ArrayOfAnon";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var result1 = pt1.Select(i => new[] { new { Num = i % 10 } })
                                    .GroupBy(x => x[0].Num, x => x[0].Num)
                                    .ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var result2 = pt2.Select(i => new[] { new { Num = i % 10 } })
                                    .GroupBy(x => x[0].Num, x => x[0].Num)
                                    .ToArray();

                passed &= (result1.Count() == result2.Count());
                passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count());
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        public static bool GroupByWithAnonymousTypes_NestedAnonTypes(DryadLinqContext context)
        {
            string testName = "GroupByWithAnonymousTypes_NestedAnonTypes";
            TestLog.TestStart(testName);

            bool passed = true;
            try
            {
                // cluster
                context.LocalDebug = false;
                IQueryable<int> pt1 = DataGenerator.GetSimpleFileSets(context);
                var result1 = pt1.Select(i => new { Num = new { NumInner = new int?(i % 10) } }) // nullable-fields present particular challenge.
                                .GroupBy(x => x.Num.NumInner, x => x.Num.NumInner)
                                .ToArray();

                // local
                context.LocalDebug = true;
                IQueryable<int> pt2 = DataGenerator.GetSimpleFileSets(context);
                var result2 = pt2.Select(i => new { Num = new { NumInner = new int?(i % 10) } }) // nullable-fields present particular challenge.
                                .GroupBy(x => x.Num.NumInner, x => x.Num.NumInner)
                                .ToArray();


                passed &= (result1.Count() == result2.Count());
                passed &= (result1.Where(g => g.Key == 1).SelectMany(g => g).Count() == result2.Where(g => g.Key == 1).SelectMany(g => g).Count()); 
            }
            catch (Exception Ex)
            {
                TestLog.Message("Error: " + Ex.Message);
                passed &= false;
            }

            TestLog.LogResult(new TestResult(testName, context, passed));
            return passed;
        }

        // This is a helper method to allow 'naming the anonymous type'
        // -- there is no name to give 'anonymous' inline to the original query,
        //    but a generic-method only has to call it by pseudonym T, which is a useful workaround.
        public static MyGenericType<T> MakeNewMyGenericType<T>(T item)
        {
            return new MyGenericType<T>(item);
        }

    }

    [Serializable]
    public class MyGenericType<T>
    {
        public T Field;

        public MyGenericType(T data)
        {
            Field = data;
        }
    }

    [Serializable]
    public class NonSealedClass
    {
        public int X;
    }

    [Serializable]
    public class ParentClass
    {
        public int X;
    }

    [Serializable]
    public class ChildClass : ParentClass
    {
        public int Y;
    }
}
