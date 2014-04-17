using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DryadLinqTests
{
    public class ApplyAndForkTests 
    {
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


        public static bool NonHomomorphicUnaryApply()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/NonHomomorphicUnaryApply.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q1 = pt1.ApplyPerPartition(x => NonHomomorphic_Unary_Func(x));
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool HomomorphicUnaryApply()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/HomomorphicUnaryApply.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q1 = pt1.ApplyPerPartition(x => Homomorphic_Unary_Func(x));
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool NonHomomorphicBinaryApply()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/NonHomomorphicBinaryApply.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q1 = pt1.Apply(pt1, (x, y) => NonHomomorphic_Binary_Func(x, y));
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool LeftHomomorphicBinaryApply()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/LeftHomomorphicBinaryApply.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());

                var q1 = pt1.ApplyPerPartition(pt1, (x, y) => LeftHomomorphic_Binary_Func(x, y), true);
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool FullHomomorphicBinaryApply_DifferentDataSets()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/FullHomomorphicBinaryApply_DifferentDataSets.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                IQueryable<int> pt2 = simple.Select(x => x.First());

                var q1 = pt1.ApplyPerPartition(pt2, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }

        public static bool FullHomomorphicBinaryApply_IdenticalDataSets()
        {
            var context = new DryadLinqContext(Config.cluster);
            context.LocalExecution = false;
            bool passed = true;
            try
            {
                string outFile = "unittest/output/FullHomomorphicBinaryApply_2.out";

                IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
                                             "unittest/inputdata/SimpleFile.txt"));

                IQueryable<IEnumerable<int>> simple = input.Apply(x => DataGenerator.CreateSimpleFileSets());
                IQueryable<int> pt1 = simple.Select(x => x.First());
                var q1 = pt1.ApplyPerPartition(pt1, (x, y) => FullHomomorphic_Binary_Func(x, y), false);
                var jobInfo = q1.ToStore<int>(outFile).Submit();
                jobInfo.Wait();

                passed &= Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
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

        public static bool Aggregate_WithCombiner()
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

                string q1 = pt1.Aggregate("", (str, x) => IntToStringCSVAggregator(str, x));

                passed &= (q1.Length == 27); // string should have numbers 1..12 separated by commas
            }
            catch (DryadLinqException)
            {
                passed &= false;
            }
            return passed;
        }
    }
}
