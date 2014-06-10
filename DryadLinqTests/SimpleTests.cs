using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;

namespace DryadLinqTests
{
    public class C1
    {
        public int x;
        public float y;

        public C1(int a, float b)
        {
            this.x = a;
            this.y = b;
        }
    }

    public class SimpleTests
    {
        public static void Run(string[] args)
        {
            Test1(args);
            //Test2(args);
            //Test3(args);
            //Test4(args);
            //Test5(args);
        }

        public static void Test1(string[] args)
        {
            DryadLinqContext context = new DryadLinqContext(1, "partfile");
            //context.PartitionUncPath = "DryadLinqTemp/PartFiles";
            var input = context.FromStore<LineRecord>("partfile:///d:/DryadLinqTemp/PartFiles/foo.pt");
            var lines = input.Where(x => x.Line.Contains("white"));
            foreach (var x in lines) Console.WriteLine(x);
            //var result = lines.ToStore("partfile://svc-yuanbyu-3/DryadLinqTemp/PartFiles/res1.pt", true);
            //result.SubmitAndWait();
        }

        public static void Test2(string[] args)
        {
            DryadLinqContext context = new DryadLinqContext(1, "partfile");
            var input = context.FromStore<LineRecord>("partfile:///d:/DryadLinqTemp/PartFiles/foo.pt");
            var q1 = input.Where(x => x.Line.Contains("white"));
            var q2 = input.Where(x => x.Line.Contains("the"));
            var res1 = q1.ToStore("partfile:///d:/DryadLinqTemp/PartFiles/res1.pt", true);
            var res2 = q2.ToStore("partfile:///d:/DryadLinqTemp/PartFiles/res2.pt", true);
            DryadLinqQueryable.SubmitAndWait(res1, res2);
        }

        public static void Test3(string[] args)
        {
            DryadLinqContext context = new DryadLinqContext(1, "partfile");
            var input = context.FromStore<LineRecord>("partfile:///d:/DryadLinqTemp/PartFiles/foo.pt");
            var words = input.SelectMany(x => x.Line.Split(' '));
            var groups = words.GroupBy(x => x);
            var counts = groups.Select(x => new KeyValuePair<string, int>(x.Key, x.Count()));
            var toOutput = counts.Select(x => new LineRecord(String.Format("{0}: {1}", x.Key, x.Value)));
            var result = toOutput.ToStore("partfile:///d:/DryadLinqTemp/PartFiles/res2.pt", true);
            result.SubmitAndWait();
        }

        public static void Test4(string[] args)
        {
            DryadLinqContext context = new DryadLinqContext("svc-d2-01");
            var input = context.FromStore<LineRecord>("hdfs://svc-d2-01:8033/user/misard/foo.txt");
            var lines = input.Where(x => x.Line.Contains("white"));
            var result = lines.ToStore("hdfs://svc-d2-01:8033/user/yuanbyu/foo.txt", true);
            result.SubmitAndWait();
        }

        public static void Test5(string[] args)
        {
            DryadLinqContext context = new DryadLinqContext(1);
            Uri dataUri = AzureUtils.ToAzureUri("msrsvc", "test", "testwrite");
            IEnumerable<LineRecord> lines = DataProvider.ReadData<LineRecord>(context, dataUri);
            foreach (var x in lines)
            {
                Console.WriteLine(x);
            }
        }

    }
}
