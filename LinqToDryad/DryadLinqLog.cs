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

//
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Microsoft.Research.DryadLinq.Internal
{
    // The client-log is a file produced in the current working directly (next to generated .cs/.dll and queryPlan.xml)
    // 
    // Note: concurrent queries in one AppDomain will get interleaved logs
    //       concurrent queries from different AppDomains will not all be able to write logs.
    //
    // Once the log file is opened, it generally will remain open for the duration of the AppDomain.
    internal static class HpcClientSideLog
    {
        private static bool _enabled = true; // whether to log or not.

        internal const string CLIENT_LOG_FILENAME  = "HpcLinq.log";
        private static bool s_IOErrorOccurred = false;
        private static StreamWriter s_writer;

        public static void Add(string msg)
        {
            if (!_enabled)
                return;

            Add(msg, null);
        }

        public static void Add(string msg, params object[] args)
        {
            if (!_enabled)
                return;

            if(s_IOErrorOccurred)
                return;

            try{
                if (s_writer == null)
                {
                    string path = HpcLinqCodeGen.GetPathForGeneratedFile(CLIENT_LOG_FILENAME, null);
                    s_writer = new StreamWriter(path);
                }

                if (args == null)
                {
                    s_writer.WriteLine(msg);
                }
                else
                {
                    s_writer.WriteLine(msg, args);
                }
                s_writer.Flush();
            }
            catch (IOException)
            {
                s_IOErrorOccurred = true;
                try
                {
                    s_writer.Close();
                    s_writer.Dispose();
                    s_writer = null;
                }
                catch
                {
                    // supress exceptions that occur during cleanup.
                }
                return;
            }
        }  
    }
    
    public static class DryadLinqLog
    {
        public static bool IsOn { get; set; }
        private static StreamWriter sw = new StreamWriter("LinqLog.txt", true);

        static DryadLinqLog()
        {
            sw.AutoFlush = true;
        }

        public static void Add(bool isOn, string msg, params object[] args)
        {
            if (isOn || IsOn)
            {
                Console.WriteLine("DryadLinq: " + msg, args);
                sw.WriteLine("DryadLinq: " + msg, args);
            }
        }

        public static void Add(string msg, params object[] args)
        {
            Add(true, msg, args);  //Debug - was false
        }  
    }
}
