using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

namespace DryadLinqTests
{
    public class TestResult
    {
        public DryadLinqContext Context { get; private set; }
        public string TestName { get; private set; }
        public bool Passed { get; private set; }
        //public string Error { get; private set; }

        public TestResult(string testName, DryadLinqContext context, bool passed)
        {
            TestName = testName;
            Passed = passed;
            Context = context;
        }
    }

    public class TestLog
    {
        static internal int nTestsRun;
        static internal int nTestsPassed;
        static internal string fileName = "";

        internal static void LogInit(string path)
        {
            fileName = path;

            // create empty file
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(fileName))
            {
            }
        }

        // NOTE: QA test harnesses may rely on the formatting
        internal static void TestStart(string testName)
        {
            Message(" --- Starting: [" + testName + "] --- ");
        }

        // NOTE: QA tests may rely on the formatting
        internal static void LogResult(TestResult result)
        {
            nTestsRun++;
            if (result.Passed)
            {
                nTestsPassed++;
            }

            Message(" * " + (result.Passed ? "Pass" : "FAIL") );
            Message(" --- Completed: [" + result.TestName + "] --- ");
            NewLine();
        }
        internal static void LogResult(bool result)
        {
            nTestsRun++;
            if (result)
            {
                nTestsPassed++;
            }
            NewLine();
        }

        internal static void NewLine()
        {
            if (fileName.Length > 0)
            {
                // append data
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(fileName, true))
                {
                    file.WriteLine();
                }
            }
            Console.WriteLine();
        }
        internal static void Message(string msg)
        {
            if (fileName.Length > 0)
            {
                // append data
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(fileName, true))
                {
                    file.WriteLine(msg);
                }
            }
            Console.WriteLine(msg);
        }

    }


}
