
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

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Drawing; // for color
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Security.Cryptography;

// Implement here generally-useful tools.
namespace Microsoft.Research.Calypso.Tools
{
    /// <summary>
    /// An error handling function.
    /// </summary>
    /// <param name="message">Message to display.</param>
    /// <param name="messageKind">Kind of message.</param>
    public delegate void StatusReporter(string message, StatusKind messageKind);

    /// <summary>
    /// Kind of status displayed.
    /// </summary>
    public enum StatusKind
    {
        /// <summary>
        /// Everything is fine.
        /// </summary>
        OK,
        /// <summary>
        /// Some error occurred.
        /// </summary>
        Error,
        /// <summary>
        /// A new long-term operation is initiated.
        /// </summary>
        LongOp,
    };

    /// <summary>
    /// Untyped version of work item.
    /// </summary>
    public interface IBackgroundWorkItem 
    {
        /// <summary>
        /// Description of the work item.
        /// </summary>
        string Description { get; }
        /// <summary>
        /// Perform the background work.
        /// </summary>
        /// <param name="queue">Queue for work.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <param name="progressReporter">Delegate used to report progress.</param>
        /// <param name="cancel">If true for an item in the queue, cancel it.</param>
        void Queue(BackgroundWorkQueue queue, StatusReporter reporter, Action<int> progressReporter, Func<IBackgroundWorkItem, bool> cancel);
        /// <summary>
        /// True if the item has been cancelled.
        /// </summary>
        bool Cancelled { get; }
        /// <summary>
        /// Cancel this item.
        /// </summary>
        void Cancel();
        /// <summary>
        /// Run the computation (called on a background thread).
        /// </summary>
        void Run();
        /// <summary>
        /// Run the continuation (called on the foreground thread).
        /// <param name="ex">Exception that occurred during background work (or null).</param>
        /// </summary>
        void RunContinuation(Exception ex);
    }

    /// <summary>
    /// A piece of work to be performed in the background.
    /// </summary>
    /// <typeparam name="T">Type of result from computation.</typeparam>
    public class BackgroundWorkItem<T> : IBackgroundWorkItem
    {
        /// <summary>
        /// Computation to invoke.  If the computation is not cancelled the result is passed as the second argument to the continuation.
        /// </summary>
        public Func<StatusReporter, Action<int>, T> Computation { get; protected set; }

        /// <summary>
        /// Function to call when the work is completed.  The first argument is 'true' if the computation was not cancelled.  The second argument is the result of the computation.
        /// </summary>
        public Action<bool, T> Continuation { get; protected set; }
        /// <summary>
        /// Structure used to signal to the Computation.
        /// </summary>
        StatusReporter reporter;
        /// <summary>
        /// Progress reporter.
        /// </summary>
        private Action<int> progress;
        /// <summary>
        /// Result of background computation.
        /// </summary>
        T Result;
        /// <summary>
        /// Description of the background work.
        /// </summary>
        public string Description { get; protected set; }
        /// <summary>
        /// True if item has been cancelled.
        /// </summary>
        public bool Cancelled { get; protected set; }
        /// <summary>
        /// Queue containing item.
        /// </summary>
        private BackgroundWorkQueue queue;

        // ReSharper disable ConvertToConstant.Local
        bool TraceAsync =
            // ReSharper restore ConvertToConstant.Local
#if DEBUG_WORKQUEUE
#else
            false;
#endif

        // ReSharper disable once StaticFieldInGenericType
        private static int crtid;

        /// <summary>
        /// Create a background work item.
        /// </summary>
        /// <param name="computation">Computation to perform on a background thread.  Ideally this should always be a static method.</param>
        /// <param name="continuation">Continuation to invoke on the foreground thread when work is done.</param>
        /// <param name="description">Description of the background work.</param>
        public BackgroundWorkItem(Func<StatusReporter, Action<int>, T> computation, Action<bool, T> continuation, string description)
        {
            this.Description = description;
            this.Computation = computation;
            this.Continuation = continuation;
            this.reporter = null;
            this.queue = null;
            this.Id = crtid++;
        }

        /// <summary>
        /// Perform the background work.
        /// </summary>
        /// <param name="queue">Worker which does the work.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <param name="progressReporter">Delegate used to report progress.</param>
        /// <param name="cancel">If true for an item, cancel it.</param>
        // ReSharper disable ParameterHidesMember
        public void Queue(BackgroundWorkQueue queue, StatusReporter reporter, Action<int> progressReporter, Func<IBackgroundWorkItem, bool> cancel)
        // ReSharper restore ParameterHidesMember
        {
            if (TraceAsync)
                Console.WriteLine("{0} Queueing {1}", Utilities.PreciseTime, this.Description);
            this.queue = queue;
            this.reporter = reporter;
            this.progress = progressReporter;
            this.queue.CancelMatching(cancel);
            this.queue.Enqueue(this);
        }

        /// <summary>
        /// Run the computation; called on the background thread.
        /// </summary>
        public void Run()
        {
            DateTime startTime = DateTime.Now;

            if (TraceAsync)
                Console.WriteLine("{0} Running function {1}", Utilities.PreciseTime, this.Description);
            try
            {
                this.Result = this.Computation(this.reporter, this.progress);
            }
            catch (Exception ex)
            {
                this.reporter(this.Description + " failed with " + ex.Message, StatusKind.Error);
                Console.WriteLine(ex);
                this.Cancelled = true;
            }

            DateTime endTime = DateTime.Now;
            TimeSpan duration = endTime - startTime;
            Console.WriteLine("Operation <" + this.Description + "> took " + duration);
        }

        /// <summary>
        /// Run the continuation; called on the foreground thread.
        /// </summary>
        /// <param name="ex">Exception that occurred during background work.</param>
        public void RunContinuation(Exception ex)
        {
            if (ex != null)
                this.reporter(this.Id + ": Exception during background work: " + ex.Message, StatusKind.Error);
            if (TraceAsync)
                Console.WriteLine("{0} Running continuation of {1}", Utilities.PreciseTime, this.Description);
            this.Continuation(this.Cancelled, this.Result);
        }

        /// <summary>
        /// Cancel the work.
        /// </summary>
        public void Cancel()
        {
            if (TraceAsync)
                Console.WriteLine("{1}/{0}: Cancelling", this.Description, this.Id);
            this.Cancelled = true;
            this.queue.CancelMe(this);
        }

        /// <summary>
        /// Unique id of this work item.
        /// </summary>
        public int Id
        {
            get;
            protected set;
        }
    }

    /// <summary>
    /// Maintains a queue of tasks for a background worker.
    /// </summary>
    public class BackgroundWorkQueue
    {
        /// <summary>
        /// Worker performing the work.
        /// </summary>
        public BackgroundWorker BackgroundWorker { get; protected set; }
        /// <summary>
        /// Queue of items waiting for worker.
        /// </summary>
        readonly List<IBackgroundWorkItem> queue;
        /// <summary>
        /// Currently executing work item.
        /// </summary>
        IBackgroundWorkItem current;

        /// <summary>
        /// Create a background work queue servicing a specified worker.
        /// </summary>
        /// <param name="worker">Worker to use.</param>
        public BackgroundWorkQueue(BackgroundWorker worker)
        {
            if (worker == null)
                throw new ArgumentNullException("worker");
            this.BackgroundWorker = worker;
            this.BackgroundWorker.WorkerSupportsCancellation = true;
            this.BackgroundWorker.RunWorkerCompleted += this.worker_RunWorkerCompleted;
            this.BackgroundWorker.DoWork += this.worker_DoWork;
            this.queue = new List<IBackgroundWorkItem>();
            this.current = null;
        }

        /// <summary>
        /// Called on background thread to do the work specified by the 'current' item.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void worker_DoWork(object sender, DoWorkEventArgs e)
        {
            if (this.current == null)
                return;
#if DEBUG_WORKQUEUE
#endif
            if (!this.current.Cancelled)
            {
                this.current.Run();
            }
            else
            {
#if DEBUG_WORKQUEUE
#endif
            }
            if (this.BackgroundWorker.CancellationPending)
                e.Cancel = true;
        }

        /// <summary>
        /// Called when the worker is completed.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Event describing completion.</param>
        void worker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (this.current != null)
            {
#if DEBUG_WORKQUEUE
#endif
                IBackgroundWorkItem crt = this.current;
                this.current = null;
                crt.RunContinuation(e.Error);
            }
            this.Kick();
        }

        /// <summary>
        /// Add an item to the work queue.
        /// </summary>
        /// <param name="item">Item to add.</param>
        internal void Enqueue(IBackgroundWorkItem item)
        {
            this.queue.Add(item);
            this.Kick();
        }

        /// <summary>
        /// Try to run one more item from the queue.
        /// </summary>
        public void Kick()
        {
            if (this.BackgroundWorker.IsBusy)
                return;

            if (this.queue.Count == 0)
                return;

            if (this.current != null)
                throw new Exception("current is not null");
            this.current = this.queue[0];
            this.queue.RemoveAt(0);
            this.Start();
        }

        /// <summary>
        /// Start execution of the current item.
        /// </summary>
        private void Start()
        {
            if (this.BackgroundWorker.IsBusy)
                throw new Exception("Worker is busy");
            this.BackgroundWorker.RunWorkerAsync();
        }

        /// <summary>
        /// Cancel everything matching the filter from the queue.
        /// </summary>
        /// <param name="filter">Filter: the items where the filter is true will be cancelled.</param>
        internal void CancelMatching(Func<IBackgroundWorkItem, bool> filter)
        {
            if (filter == null)
                return;

            foreach (IBackgroundWorkItem item in this.queue)
            {
                if (filter(item))
                {
#if DEBUG_WORKQUEUE
#endif
                    item.Cancel();
                }
            }
            if (this.current != null && filter(this.current))
            {
                this.Cancel(this.current);
            }
        }

        /// <summary>
        /// Cancel the specified item.
        /// </summary>
        /// <param name="backgroundWorkItem">Item to cancel.</param>
        internal void Cancel(IBackgroundWorkItem backgroundWorkItem)
        {
            if (this.current == backgroundWorkItem)
            {
                this.BackgroundWorker.CancelAsync();
                this.current.Cancel();
            }
        }

        /// <summary>
        /// An item asks to be cancelled.
        /// </summary>
        /// <param name="backgroundWorkItem">Item to cancel.</param>
        internal void CancelMe(IBackgroundWorkItem backgroundWorkItem)
        {
            if (this.current == backgroundWorkItem)
            {
                this.BackgroundWorker.CancelAsync();
            }
        }

        public void Stop()
        {
            // TODO
        }
    }


    /// <summary>
    /// Useful static methods for all applications.
    /// </summary>
    public static class Utilities
    {
        private static DateTime firstTime = DateTime.MinValue;
        /// <summary>
        /// Current time precise enough for logging.
        /// </summary>
        public static string PreciseTime
        {
            get
            {
                if (Utilities.firstTime == DateTime.MinValue)
                {
                    Utilities.firstTime = DateTime.Now;
                }
                TimeSpan elapsed = (DateTime.Now - Utilities.firstTime);
                return elapsed + "\t";
            }
        }

        /// <summary>
        /// Read an unquoted word from the given line starting at index 'currentIndex'.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <param name="currentIndex">Start index of word.</param>
        /// <param name="word">Found word.</param>
        /// <returns>Index of separator after word (or of end of line).</returns>
        private static int ParseUnquotedWord(string line, int currentIndex, out string word)
        {
            int separatorIndex = line.IndexOf(',', currentIndex);
            if (separatorIndex == -1)
            {
                word = line.Substring(currentIndex);
                return line.Length;
            }
            else
            {
                word = line.Substring(currentIndex, separatorIndex - currentIndex);
                return separatorIndex;
            }
        }

        /// <summary>
        /// Read a quoted word from the given line starting at index 'currentIndex'.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <param name="currentIndex">Start index of word.</param>
        /// <param name="word">Found word.</param>
        /// <returns>Index of separator after word (or of end of line).  Returns -1 if the end quote is missing.</returns>
        private static int ParseQuotedWord(string line, int currentIndex, out string word)
        {
            int endIndex = currentIndex + 1;
            while (true)
            {
                endIndex = line.IndexOf('\"', endIndex);
                if (endIndex == -1)
                {
                    word = "";
                    return -1;
                }
                if (endIndex == line.Length - 1)
                {
                    // last word on line
                    break;
                }
                else if (line[endIndex + 1] == '\"')
                {
                    // quoted quote, continue
                    endIndex += 2;
                    continue;
                }
                else
                {
                    // end of quoted word
                    break;
                }
            }

            word = line.Substring(currentIndex + 1, endIndex - currentIndex - 1); // drop the start and end quotes
            word = word.Replace("\"\"", "\""); // fix quoted quotes
            return endIndex + 1;
        }

        /// <summary>
        /// Split a comma-separated value line into fields.  Properly parses quoted fields.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <returns>A list of the fields parsed.</returns>
        public static List<string> SplitCSVLine(string line)
        {
            List<string> results = new List<string>();
            int currentIndex = 0;

            while (currentIndex < line.Length)
            {
                string word;
                if (line[currentIndex] == '\"')
                {
                    currentIndex = ParseQuotedWord(line, currentIndex, out word);
                    if (currentIndex < 0)
                        // end-of-line in quoted word; need more data
                        return null;
                }
                else
                {
                    currentIndex = ParseUnquotedWord(line, currentIndex, out word);
                }

                results.Add(word);
                if (currentIndex < line.Length)
                {
                    // expect a separator
                    if (line[currentIndex] != ',')
                        throw new ArgumentException("Not found expected separator character after quoted field");
                    currentIndex++;
                }
            }

            return results;
        }

        /// <summary>
        /// Regular expression matching a GUID.
        /// </summary>
        public static readonly Regex GuidRegex = new Regex(@"([0-9A-F]{8})-([0-9A-F]{4})-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", RegexOptions.Compiled);

        /// <summary>
        /// Format string to use to represent datetimes in high precision as a string.
        /// </summary>
        public static string HighPrecisionDateFormat = "MM/dd/yyyy HH:mm:ss.fff tt";

        static MD5 MD5Cached;

        /// <summary>
        /// A string encoding of the MD5 checksum of the input string.
        /// </summary>
        /// <param name="s">String to encode using MD5.</param>
        /// <returns>A string containing the MD5 encoding.</returns>
        public static string MD5(string s)
        {
            if (MD5Cached == null)
                MD5Cached = System.Security.Cryptography.MD5.Create();
            byte [] code = MD5Cached.ComputeHash(Encoding.UTF8.GetBytes(s));
            StringBuilder result = new StringBuilder();
            foreach (byte b in code)
                result.Append(b.ToString("X2"));
            return result.ToString();
        }

        /// <summary>
        /// Move or rename a file.
        /// </summary>
        /// <param name="lpExistingFileName">Existing file.</param>
        /// <param name="lpNewFileName">New file.</param>
        /// <returns>True on success.</returns>
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool MoveFile(string lpExistingFileName, string lpNewFileName);

        /// <summary>
        /// Move or rename a file.  Throws an exception on failure.
        /// </summary>
        /// <param name="from">Existing file.</param>
        /// <param name="to">New file.</param>
        public static void Move(string from, string to)
        {
            bool success = MoveFile(from, to);
            if (success)
                return;
            throw new Win32Exception();
        }

        /// <summary>
        /// Given a string of the form [k=v](,[k=v]*), parse it into a dictionary.
        /// Keys and values cannot contain commas or equal signs.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <returns>A dictionary mapping the keys to values.</returns>
        public static Dictionary<string, string> ParseCommaSeparatedKeyValuePair(string line)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            if (line.Length == 0)
                return result;

            string[] pieces = line.Split(',');
            foreach (string piece in pieces)
            {
                if (piece.Length == 0) continue;
                string[] parts = piece.Split('=');
                if (parts.Length != 2)
                    throw new ArgumentException("Element `" + piece + "' not in k=v form");
                result.Add(parts[0].Trim(), parts[1].Trim());
            }
            return result;
        }

        /// <summary>
        /// True if this file name seems to indicate a text file.
        /// </summary>
        /// <param name="filename">File whose name is tested.</param>
        /// <returns>True if this name indicates a text file.</returns>
        public static bool FileNameIndicatesTextFile(string filename)
        {
            string[] textSuffixes = {
                                         ".txt", ".bat", ".cmd", ".log", ".config", ".xml", ".html"
                                     };
            foreach (string suffix in textSuffixes)
            {
                if (filename.EndsWith(suffix))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Create the directory if it does not exist; it retries a few times.
        /// </summary>
        /// <param name="filename">Path for file; directory name is extracted and created.</param>
        public static void EnsureDirectoryExistsForFile(string filename)
        {
            string dir = Path.GetDirectoryName(filename);

            bool ok = false;
            int count = 0;
            while (!ok && count < 5)
            {
                try
                {
                    if (dir != null && !Directory.Exists(dir))
                        Directory.CreateDirectory(dir);
                    ok = true;
                }
                catch (IOException)
                {
                    Thread.Sleep(200);
                }
                count++;
            }
        }

       /// <summary>
        /// Add an item in front of a list; the list is mutated.
        /// </summary>
        /// <param name="list">List to add item to.</param>
        /// <param name="item">Item to add in front.</param>
        /// <param name="maxlen">Maximum list lenght.</param>
        /// <returns>The new list.</returns>
        public static IList<T> AddItemInFront<T>(IList<T> list, int maxlen, T item)
        {
            if (list.Contains(item))
                // move to front
                list.Remove(item);
            else if (list.Count >= maxlen)
                // drop last
                list.RemoveAt(list.Count - 1);
            list.Insert(0, item);
            return list;
        }

        /// <summary>
        /// The file name may contain illegal characters; replace them with something legal.
        /// Does not guarantee that the file name will be unique.
        /// </summary>
        /// <param name="filename">Filename to legalize.</param>
        /// <returns>A file name which is legal.</returns>
        public static string LegalizeFileName(string filename)
        {
            HashSet<char> illegal = new HashSet<char>(Path.GetInvalidFileNameChars());
            StringBuilder result = new StringBuilder();
            foreach (char c in filename)
            {
                if (illegal.Contains(c))
                    // replace illegal characters with a dash
                    result.Append('-');
                else
                    result.Append(c);
            }
            return result.ToString();
        }

        /// <summary>
        /// Truncate x to a given number of decimals after decimal point.
        /// </summary>
        /// <param name="x">Value to truncate.</param>
        /// <param name="decimals">Number of decimal values.</param>
        /// <returns>The input with at most the indicated number of decimal places.</returns>
        public static string Round(double x, int decimals)
        {
            if (decimals < 0)
                decimals = 0;
            double rounded = Math.Round(x, decimals);
            return rounded.ToString();
        }

        private static Random jitterRandom = new Random();
        /// <summary>
        /// Generate a random value in the interval -max .. max with a distribution skewed towards the center
        /// </summary>
        /// <param name="max">Maximum amount of jitter.</param>
        /// <returns>The jitter value.</returns>
        public static double Jitter(double max)
        {
            double rand = 100 * jitterRandom.NextDouble() - 50;
            return rand * max / 50;
        }

        /// <summary>
        /// Check if two strings represent the same machine.
        /// </summary>
        /// <param name="m1">First machine.</param>
        /// <param name="m2">Second machine.</param>
        /// <returns>'true' if the two names point to the same machine really.</returns>
        public static bool SameMachine(string m1, string m2)
        {
            return m1.ToLower() == m2.ToLower();
        }

        /// <summary>
        /// Extract the executable embedded in the assembly, then store it in the disk.
        /// </summary>
        /// <param name="filename">The name of the file in the assembly</param>
        /// <returns>the path to the instantiated file on disk(a local path)</returns>
        public static string InstantiateExecutableFromRes(string filename)
        {
            if (File.Exists(filename))
                return (new FileInfo(filename)).FullName;

            // Get Current Assembly refrence
            Assembly currentAssembly = Assembly.GetExecutingAssembly();
            // Get all embedded resources
            string[] arrResources = currentAssembly.GetManifestResourceNames();

            foreach (string resourceName in arrResources)
            {
                if (resourceName.EndsWith(filename))
                { //or other extension desired
                    //Name of the file saved on disk
                    string saveAsName = filename;
                    //just save in the current directory
                    FileInfo fileInfoOutputFile = new FileInfo(saveAsName);
                    if (fileInfoOutputFile.Exists)
                    {
                        //overwrite if desired  (depending on your needs)
                        fileInfoOutputFile.Delete();
                    }
                    FileStream streamToOutputFile = fileInfoOutputFile.OpenWrite();
                    Stream streamToResourceFile = currentAssembly.GetManifestResourceStream(resourceName);

                    const int size = 4096;
                    byte[] bytes = new byte[4096];
                    int numBytes;
                    while ((numBytes = streamToResourceFile.Read(bytes, 0, size)) > 0)
                    {
                        streamToOutputFile.Write(bytes, 0, numBytes);
                    }

                    streamToOutputFile.Close();
                    streamToResourceFile.Close();
                    return fileInfoOutputFile.FullName;
                }
            }

            return null;
        }

        /// <summary>
        /// Copy the source directory to the target directory.
        /// </summary>
        /// <param name="sourceDirectory">The path to the source directory.</param>
        /// <param name="targetDirectory">The path to the target directory.</param>
        /// <param name="pattern">Only filenames and subdirectories matching the pattern will be copied.</param>
        public static void CopyDirectory(string sourceDirectory, string targetDirectory, string pattern)
        {
            DirectoryInfo diSource = new DirectoryInfo(sourceDirectory);
            DirectoryInfo diTarget = new DirectoryInfo(targetDirectory);

            CopyAll(diSource, diTarget, pattern);
        }

        /// <summary>
        /// Copy all the stuff from source dir to target dir. Just a private version of CopyDirectory
        /// </summary>
        /// <param name="source">The directory information of the source directory.</param>
        /// <param name="target">The directory information of thetarget directory.</param>
        /// <param name="pattern">Only files and subdirectories matching the pattern will be copied.</param>
        private static void CopyAll(DirectoryInfo source, DirectoryInfo target, string pattern)
        {
            // Check if the target directory exists, if not, create it.
            if (Directory.Exists(target.FullName) == false)
            {
                Directory.CreateDirectory(target.FullName);
            }

            // Copy each file into it's new directory.
            foreach (FileInfo fi in source.GetFiles(pattern))
            {
                //Trace.TraceInformation(@"Copying {0}\{1}", target.FullName, fi.Name);
                fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
            }

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories(pattern))
            {
                DirectoryInfo nextTargetSubDir =
                    target.CreateSubdirectory(diSourceSubDir.Name);
                CopyAll(diSourceSubDir, nextTargetSubDir, pattern);
            }
        }

        /// <summary>
        /// Convert a time value printed by the dryad job manager into a datetime object.
        /// </summary>
        /// <param name="tzone">Configuration describing the local time zone.</param>
        /// <param name="cosmosTime">CsTime value (A Cosmos time implementation).</param>
        /// <returns>The absolute time represented by the value.</returns>
        public static DateTime Convert64time(TimeZoneInfo tzone, string cosmosTime)
        {
            DateTime time = new DateTime(Convert.ToInt64(cosmosTime), DateTimeKind.Unspecified).AddYears(1600);
            time = System.TimeZoneInfo.ConvertTimeFromUtc(time, tzone);
            return time;
        }

        /// <summary>
        /// Add one more key-value pair to a stringbuilder in the form k=v.  If the builder is not empty, add a comma in front too.
        /// </summary>
        /// <param name="builder">Stringbuilder where the string is built.</param>
        /// <param name="key">Key.</param>
        /// <param name="value">Value.</param>
        public static void AddKVP(StringBuilder builder, string key, object value)
        {
            if (builder.Length > 0)
                builder.Append(",");
            // remove commas from key and value, otherwise it won't work
            key = key.Replace(',', '-');
            builder.Append(key);
            builder.Append("=");
            string val = value.ToString().Replace(',', '-');
            builder.Append(val);
        }

        /// <summary>
        /// CreateHardLink will utilize PInvoke to call the Win32 function CreateHardLink.
        /// </summary>
        /// <param name="lpFileName">Source FileName</param>
        /// <param name="lpExistingFileName">Destination FileName</param>
        /// <param name="lpSecurityAttributes">Security Attributes - Should be IntPtr.Zero</param>
        /// <returns>True if the function succeeds.</returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern public bool CreateHardLink(string lpFileName, string lpExistingFileName,
           IntPtr lpSecurityAttributes);

        /// <summary>
        /// Create a hard link, and throw an exception if this fails.
        /// </summary>
        /// <param name="to">Destination file.</param>
        /// <param name="from">Source file.</param>
        public static void CreateHardLink(string to, string from)
        {
            bool status = CreateHardLink(to, from, new IntPtr(0));
            if (!status)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        /// <summary>
        /// Copy a file, but try creating a hard link first.
        /// </summary>
        /// <param name="destination">Destination file.</param>
        /// <param name="source">Source file.</param>
        /// <returns>False if the copy failed. May throw exceptions.</returns>
        public static bool LinkOrCopy(string destination, string source)
        {
            if (!File.Exists(source))
                return false;

            if (File.Exists(destination))
            {
                // check if the source and destination may be the same file
                FileInfo si = new FileInfo(source);
                FileInfo di = new FileInfo(destination);
                if (si.LastWriteTime == di.LastWriteTime && si.Length == di.Length)
                    return true;

                File.Delete(destination);
            }

            // no exceptions thrown here
            bool success = Utilities.CreateHardLink(destination, source, new IntPtr(0));
            if (success) return true;

            File.Copy(source, destination);
            return true;
        }

        /// <summary>
        /// Run a process, return exit code if waiting for completion.
        /// </summary>
        /// <param name="process">Command-line to invoke.</param>
        /// <param name="workDirectory">If not null, used to set the work directory of the process.</param>
        /// <param name="quote">If true, add quotes around parameters.</param>
        /// <param name="wait">If true, wait for completion.</param>
        /// <param name="useshell">Do we use shell for executing?</param>
        /// <param name="runAsAdmin">Do we need admin privileges? (if true, usually also requires useshell to be true)</param>
        /// <param name="arguments">Arguments to pass.</param>
        /// <returns>Exit code of the process if waiting for the process, zero otherwise.</returns>
        // ReSharper disable once UnusedMethodReturnValue.Global
        public static int RunProcess(string process, string workDirectory, bool quote, bool wait, bool useshell, bool runAsAdmin, params string[] arguments)
        {
            StringBuilder args = new StringBuilder();
            string q = quote ? @"""" : "";
            foreach (string arg in arguments)
            {
                args.Append(@" " + q + arg + q);
            }
            Trace.TraceInformation("Running: " + process + " " + args);

            ProcessStartInfo processStartInfo = new ProcessStartInfo(process, args.ToString());
            processStartInfo.CreateNoWindow = true;
            processStartInfo.UseShellExecute = useshell;
            processStartInfo.RedirectStandardOutput = !useshell;
            processStartInfo.RedirectStandardError = !useshell;
            if (workDirectory != null)
            {
                processStartInfo.WorkingDirectory = workDirectory;
            }
            if (runAsAdmin)
            {
                processStartInfo.Verb = "runas";
            }

            Process p = new Process();
            p.StartInfo = processStartInfo;
            int exitcode;
            try
            {
                p.Start();
                if (!wait)
                    // no waiting
                    return 0;

                if (!useshell)
                {
                    StreamReader procout = p.StandardOutput;
                    if (!p.HasExited)
                    {
                        string outLine;
                        while ((outLine = procout.ReadLine()) != null)
                        {
                            Trace.TraceInformation(outLine);
                        }
                    }
                }
                p.WaitForExit();
                exitcode = p.ExitCode;
                if (exitcode != 0)
                    Trace.TraceInformation("Process has exited with exitcode " + exitcode);
            }
            catch (InvalidOperationException e)
            {
                exitcode = -1;
                Trace.TraceInformation("Could not start process " + process + " exception " + e);
            }
            catch (Win32Exception e)
            {
                exitcode = -1;
                Trace.TraceInformation("Could not start process " + process + " exception " + e);
            }
            return exitcode;
        }

        /// <summary>
        /// Private representation wrapping process and delegate to invoke on exit.
        /// </summary>
        class BackgroundProcessHandle
        {
            Process process;
            Action<int> onExit;

            /// <summary>
            /// Create a handle for a process to run in the background.
            /// </summary>
            /// <param name="p">Process to run.</param>
            public BackgroundProcessHandle(Process p)
            {
                this.process = p;
            }

            /// <summary>
            /// Run the process and invoke this delegate on exit.
            /// </summary>
            /// <param name="oe">Delegate to invoke on process exit; arg is process exit code.</param>
            /// <returns>True if launching the process succeeds.</returns>
            public bool Run(Action<int> oe)
            {
                this.onExit = oe;
                try
                {
                    this.process.EnableRaisingEvents = true;
                    this.process.Exited += this.processExited;
                    this.process.Start();
                }
                catch (InvalidOperationException e)
                {
                    Trace.TraceInformation("Could not start process " + this.process.ProcessName + " exception " + e);
                    return false;
                }
                catch (Win32Exception e)
                {
                    Trace.TraceInformation("Could not start process " + this.process.ProcessName + " exception " + e);
                    return false;
                }
                return true;
            }

            /// <summary>
            /// Event handler when the process exits.
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void processExited(object sender, EventArgs e)
            {
                if (this.onExit != null)
                    this.onExit(this.process.ExitCode);
            }
        }

        /// <summary>
        /// Run a process, return exit code if waiting for completion.
        /// </summary>
        /// <param name="process">Command-line to invoke.</param>
        /// <param name="workDirectory">If not null, used to set the work directory of the process.</param>
        /// <param name="onExit">Delegate to invoke on exit; passed exit code.</param>
        /// <param name="arguments">Arguments to pass.</param>
        /// <returns>True if running succeeded.</returns>
        public static bool RunProcessAsync(string process, string workDirectory, Action<int> onExit, params string[] arguments)
        {
            string args = string.Join(" ", arguments);
            Trace.TraceInformation("Running: " + process + " " + args);

            ProcessStartInfo processStartInfo = new ProcessStartInfo(process, args);
            processStartInfo.CreateNoWindow = true;
            if (workDirectory != null)
            {
                processStartInfo.WorkingDirectory = workDirectory;
            }

            Process p = new Process();
            p.StartInfo = processStartInfo;
            BackgroundProcessHandle bph = new BackgroundProcessHandle(p);
            bool started = bph.Run(onExit);
            return started;
        }

        /// <summary>
        /// Save an object as xml.
        /// </summary>
        /// <typeparam name="T">Type of object to save.</typeparam>
        /// <param name="filename">File to save object description to.</param>
        /// <param name="obj">Object to save.</param>
        public static void SaveAsXml<T>(string filename, T obj)
        {
            using (StreamWriter sw = new StreamWriter(filename))
            {
                XmlSerializer s = new XmlSerializer(typeof(T));
                s.Serialize(sw, obj);
            }
        }

        /// <summary>
        /// Load the description of an object from a file.
        /// </summary>
        /// <typeparam name="T">Type of object to read from file.</typeparam>
        /// <param name="filename">File containing the description.</param>
        public static T LoadXml<T>(string filename)
        {
            using (StreamReader sr = new StreamReader(filename))
            {
                XmlSerializer s = new XmlSerializer(typeof(T));
                Object o = s.Deserialize(sr);
                return (T)o;
            }
        }

        /// <summary>
        /// Convert a hsv color value to a rgb color value.
        /// </summary>
        /// <param name="h">Hue, between 0 and 1.</param>
        /// <param name="s">Saturation, between 0 and 1.</param>
        /// <param name="v">Value, between 0 and 1.</param>
        /// <param name="r">Red component, between 0 and 1.</param>
        /// <param name="g">Green component, between 0 and 1.</param>
        /// <param name="b">Blue component, between 0 and 1.</param>
        public static void HSVtoRGB(double h, double s, double v, out double r, out double g, out double b)
        {
            h *= 6;
            int i = (int)Math.Floor(h);
            double f = h - i;
            if ((i & 1) == 0) f = 1 - f; // if i is even  
            double m = v * (1 - s);
            double n = v * (1 - s * f);
            r = 0;
            g = 0;
            b = 0;
            switch (i)
            {
                case 6: throw new ArgumentOutOfRangeException("h");
                case 0: r = v; g = n; b = m; break;
                case 1: r = n; g = v; b = m; break;
                case 2: r = m; g = v; b = n; break;
                case 3: r = m; g = n; b = v; break;
                case 4: r = n; g = m; b = v; break;
                case 5: r = v; g = m; b = n; break;
            }
        }

        /// <summary>
        /// If the xpath expression matches return the first Xml node matching.
        /// Else return null.
        /// </summary>
        /// <param name="node">Node where matching starts.</param>
        /// <param name="xpath">Xpath expression to match.</param>
        /// <returns>First matching node or null.</returns>
        public static XmlNode FirstIfAnyXmlMatch(XmlNode node, string xpath)
        {
            XmlNodeList matching = node.SelectNodes(xpath);
            if (matching.Count == 0)
                return null;
            return matching[0];
        }

        /// <summary>
        /// Get the single expected matching node in an XML document.
        /// </summary>
        /// <param name="node">Search starting from this node.</param>
        /// <param name="xpath">Xpath expression to search for nodes.</param>
        /// <returns>The single matching node.</returns>
        public static XmlNode SingleXmlMatch(XmlNode node, string xpath)
        {
            XmlNode child = Utilities.FirstIfAnyXmlMatch(node, xpath);
            if (child == null)
                throw new XmlException("Node " + node.Name + " does not have exactly 1 child with path " + xpath + " in xml document");
            return child;
        }

        /// <summary>
        /// Read the web page with the specified URL.
        /// </summary>
        /// <param name="url">Url to reach.</param>
        /// <returns>A streamreader that returns the loaded web page.</returns>
        /// <param name="credentials">Credentials to use.</param>
        public static StreamReader Navigate(string url, ICredentials credentials)
        {
            CookieContainer cookiejar = new CookieContainer();
            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
            req.CookieContainer = cookiejar;
            req.ServicePoint.ConnectionLimit = 1;
            if (credentials == null)
                req.UseDefaultCredentials = true;
            else
                req.Credentials = credentials;
            req.PreAuthenticate = false;
            req.Method = "GET";

            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
            System.Diagnostics.Trace.Assert(resp.StatusCode == HttpStatusCode.OK);
            Trace.TraceInformation("Received response");

            // ReSharper disable once AssignNullToNotNullAttribute
            StreamReader respReader = new StreamReader(resp.GetResponseStream());
            return respReader;
        }

        /// <summary>
        /// Convert Hexadecimal number to integer.
        /// </summary>
        /// <param name="hexString">A string representing a hex number.</param>
        /// <returns>The equivalent integer value.</returns>
        public static int HexToInt(string hexString)
        {
            return int.Parse(hexString, System.Globalization.NumberStyles.HexNumber, null);
        }

        /// <summary>
        /// Convert integer to hexadecimal.
        /// </summary>
        /// <param name="number">Number to convert.</param>
        /// <returns>A string which is the hexadecimal representation.</returns>
        public static string IntToHex(int number)
        {
            return String.Format("{0:x}", number);
        }

        /// <summary>
        /// This is like Path.Combine, but with multiple arguments.
        /// </summary>
        /// <param name="paths">Paths to combine.</param>
        /// <returns>A single path composed of all segments.</returns>
        public static string PathCombine(params string[] paths)
        {
            if (paths.Length == 0)
                return "";
            string result = paths[0];
            for (int i = 1; i < paths.Length; i++)
                result = Path.Combine(result, paths[i]);
            return result;
        }

        /// <summary>
        /// Compute the average of a set of colors.
        /// </summary>
        /// <param name="colors">Colors to average.</param>
        /// <returns>A new color, which is the average of all other colors.</returns>
        public static Color AverageColor(IEnumerable<Color> colors)
        {
            int count = 0;
            int totalA = 0, totalR = 0, totalG = 0, totalB = 0;

            foreach (Color c in colors)
            {
                totalA += c.A;
                totalR += c.R;
                totalG += c.G;
                totalB += c.B;
                count++;
            }

            if (count == 0)
                return Color.Black;

            return Color.FromArgb(totalA / count, totalR / count, totalG / count, totalB / count);
        }

        /// <summary>
        /// Find a color contrasting with the given one (e.g., to use as foreground when the other is background).
        /// </summary>
        /// <param name="color">Color to contrast with.</param>
        /// <returns>A contrasting color.</returns>
        public static Color VisibleColor(Color color)
        {
            if (color.GetBrightness() < 0.5)
                return Color.White;
            else
                return Color.Black;
        }

        /// <summary>
        /// Copy a file which may be already opened for writing.
        /// </summary>
        /// <param name="from">Original name.</param>
        /// <param name="to">New name.</param>
        public static void CopyFile(string from, string to)
        {
            Stream rd = new FileStream(from, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Stream wr = new FileStream(to, FileMode.Create, FileAccess.Write);
            byte[] buf = new byte[1 << 13];
            for (; ; )
            {
                int len = rd.Read(buf, 0, buf.Length);
                if (len == 0) break;
                wr.Write(buf, 0, len);
            }
            rd.Close();
            wr.Close();
        }

        static char[] FolderSeparators = { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar };

        /// <summary>
        /// Split a pathname into a list of directories.
        /// </summary>
        /// <param name="path">Path to split.</param>
        /// <returns>The complete list of directories on the path.</returns>
        public static string[] SplitPathname(string path)
        {
            string[] subpaths = path.Split(FolderSeparators, StringSplitOptions.RemoveEmptyEntries);
            return subpaths;
        }

        /// <summary>
        /// Check whether a given string could be the current user.
        /// </summary>
        /// <param name="username">Name of user.</param>
        /// <returns>True if it matches the login name.</returns>
        public static bool IsThisUser(string username)
        {
            System.Security.Principal.WindowsIdentity id = System.Security.Principal.WindowsIdentity.GetCurrent();
            return (id.Name == username);
        }

        /// <summary>
        /// Given a file with records serialized by DryadLINQ create the metadata to morph it into a single-partition file.
        /// </summary>
        /// <param name="file">File containing DryadLINQ-serialized records.</param>
        /// <returns>A uri pointing to to partitioned file table whose body is the specified file.</returns>
        public static string TransformToPartitionedTable(UNCPathname file)
        {
            string prefix = file.DirectoryAndFilename;

            PartitionedFileMetadata md = new PartitionedFileMetadata();
            PartitionedFileMetadata.Partition part = new PartitionedFileMetadata.Partition(0, 0, file.Machine, prefix);
            string partFilename = part.Replica(0).ToString();
            md.Add(part);

            // partitions have to have a very stylized name
            Utilities.CreateHardLink(partFilename, file.ToString());

            // Create the metadata for the file we are returning
            UNCPathname metadatafile = file;
            metadatafile.Filename = "metadata-" + file.Filename;
            string uri = md.CreateMetadataFile(metadatafile);
            return uri;
        }

        /// <summary>
        /// Generate a regular expression corresponding to a search pattern.
        /// This replaces ? with .? and * with .*
        /// </summary>
        /// <param name="match"></param>
        /// <returns></returns>
        public static Regex RegexFromSearchPattern(string match)
        {
            if (string.IsNullOrEmpty(match))
                return new Regex("");
            match = match.Replace("?", ".?");
            match = match.Replace("*", ".*");
            return new Regex(match);
        }

        /// <summary>
        /// Parse a line of the form k=v,k=v. Values may be quoted.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <returns>A dictionary with all key=value parts, or null if parsing fails because of an end-of-line in quoted value.</returns>
        public static Dictionary<string, string> ParseCSVKVP(string line)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();

            while (!string.IsNullOrWhiteSpace(line))
            {
                int eq = line.IndexOf('=');
                if (eq < 0)
                    throw new ArgumentException("Could not find equal sign in " + line);

                string key = line.Substring(0, eq).Trim();
                string value;
                int cont;

                if (line.Length > eq && line[eq+1] == '"')
                {
                    cont = ParseQuotedWord(line, eq+1, out value);
                }
                else
                {
                    cont = ParseUnquotedWord(line, eq+1, out value);
                }

                if (cont < 0) return null; // end of line in quoted value
                if (cont >= line.Length)
                    line = "";
                else
                    line = line.Substring(cont + 1); // skip next comma

                result.Add(key, value);
            }

            return result;
        }
    }

    /// <summary>
    /// A binding list implementation which permits sorting.
    /// </summary>
    /// <typeparam name="T">Type of elements in list.</typeparam>
    public class BindingListSortable<T> : BindingList<T>
    {
        private bool isSorted;
        private ListSortDirection direction = ListSortDirection.Ascending;
        private PropertyDescriptor sortProperty;

        /// <summary>
        /// Create an empty binding list.
        /// </summary>
        public BindingListSortable()
        {
            this.isSorted = false;
        }

        /// <summary>
        /// Initialize a sortable binding list with a set of values.
        /// </summary>
        /// <param name="values">Values to insert in list.</param>
        public BindingListSortable(IList<T> values)
            : base(values)
        {
            this.isSorted = false;
        }

        /// <summary>
        /// What is the list sorted on?
        /// </summary>
        public string SortedOn { get; set; }

        /// <summary>
        /// Sort the list on the indicated property.
        /// </summary>
        /// <param name="property">Property of T to sort on.</param>
        public void Sort(string property)
        {
            this.sortProperty = this.FindPropertyDescriptor(property);
            this.ApplySortCore(sortProperty, direction);
        }

        /// <summary>
        /// Redo the last sorting operation.
        /// <returns>True if there was a last sorting operation.</returns>
        /// </summary>
        public bool RedoSort()
        {
            if (this.SortedOn == null)
                return false;
            this.Sort(this.SortedOn);
            return true;
        }

        /// <summary>
        /// Override indicating that the list supports sorting.
        /// </summary>
        protected override bool SupportsSortingCore
        {
            get { return true; }
        }

        /// <summary>
        /// Apply the sorting operation.
        /// </summary>
        /// <param name="property">Property to sort on.</param>
        /// <param name="dir">Direction of sort.</param>
        protected override void ApplySortCore(PropertyDescriptor property, ListSortDirection dir)
        {
            this.SortedOn = property.Name;
            this.direction = dir;
            List<T> items = this.Items as List<T>;

            if (null != items)
            {
                PropertyComparer<T> pc = new PropertyComparer<T>(property, dir);
                items.Sort(pc);

                /* Set sorted */
                this.isSorted = true;
            }
            else
            {
                /* Set sorted */
                this.isSorted = false;
            }
        }

        /// <summary>
        /// Is the list sorted?
        /// </summary>
        protected override bool IsSortedCore
        {
            get { return this.isSorted; }
        }

        /// <summary>
        /// "Unsort" the list.
        /// </summary>
        protected override void RemoveSortCore()
        {
            this.isSorted = false;
        }

        private PropertyDescriptor FindPropertyDescriptor(string property)
        {
            PropertyDescriptorCollection pdc = TypeDescriptor.GetProperties(typeof(T));
            PropertyDescriptor prop = pdc.Find(property, true);
            return prop;
        }

        internal class PropertyComparer<TKey> : System.Collections.Generic.IComparer<TKey>
        {
            private PropertyDescriptor property;
            private ListSortDirection direction;

            public PropertyComparer(PropertyDescriptor property, ListSortDirection direction)
            {
                this.property = property;
                this.direction = direction;
            }

            public int Compare(TKey xVal, TKey yVal)
            {
                /* Get property values */
                object xValue;
                object yValue;
                if (property != null)
                {
                    xValue = GetPropertyValue(xVal, property.Name);
                    yValue = GetPropertyValue(yVal, property.Name);
                }
                else
                {
                    xValue = xVal;
                    yValue = yVal;
                }

                /* Determine sort order */
                int sort = CompareAscending(xValue, yValue);
                if (direction == ListSortDirection.Descending)
                {
                    sort = -sort;
                }
                return sort;
            }

            public bool Equals(TKey xVal, TKey yVal)
            {
                return xVal.Equals(yVal);
            }

            public int GetHashCode(TKey obj)
            {
                return obj.GetHashCode();
            }

            /* Compare two property values of any type */
            private int CompareAscending(object xValue, object yValue)
            {
                int result;

                /* If values implement IComparer */
                var value = xValue as IComparable;
                if (value != null)
                {
                    result = value.CompareTo(yValue);
                }
                else throw new ArgumentException("values are not comparable");

                return result;
            }

            private object GetPropertyValue(TKey value, string prop)
            {
                /* Get property */
                PropertyInfo propertyInfo = value.GetType().GetProperty(prop);

                /* Return value */
                return propertyInfo.GetValue(value, null);
            }
        }
    }

    /// <summary>
    /// A legend maps a color to an explanation.
    /// </summary>
    public class Legend
    {
        /// <summary>
        /// The name of the entity which was used to construct the legend information.
        /// </summary>
        public string LegendSourceName { get; internal set; }

        /// <summary>
        /// Explanation for the choice of a color.
        /// </summary>
        public class ColorLegend
        {
            /// <summary>
            /// Explanation for the color.
            /// </summary>
            public string label;
            /// <summary>
            /// Colors will be ordered on this index when showing the legend.
            /// </summary>
            public double orderingIndex;

            /// <summary>
            /// String representation of the color legend.
            /// </summary>
            /// <returns>A string representation.</returns>
            public override string ToString()
            {
                return label;
            }
        };

        /// <summary>
        /// Minimum color index represented.
        /// </summary>
        public double MinIndex { get; protected set; }
        /// <summary>
        /// Maximum color index represented.
        /// </summary>
        public double MaxIndex { get; protected set; }
        /// <summary>
        /// Color with minimum index.
        /// </summary>
        public Color MinIndexColor { get; protected set; }
        /// <summary>
        /// Color with maximum index.
        /// </summary>
        public Color MaxIndexColor { get; protected set; }

        /// <summary>
        /// Map from color to explanation.
        /// </summary>
        protected readonly Dictionary<Color, ColorLegend> explanations;

        /// <summary>
        /// Create an empty legend.
        /// </summary>
        /// <param name="sourcename">Name of entity used to construct the legend.</param>
        public Legend(string sourcename)
        {
            this.LegendSourceName = sourcename;
            this.explanations = new Dictionary<Color, ColorLegend>();
        }

        /// <summary>
        /// The number of colors in the legend.
        /// </summary>
        public int Size { get { return this.explanations.Count; } }

        /// <summary>
        /// Explanation for color c, if any.
        /// </summary>
        /// <param name="c">Color whose explanation is sought.</param>
        /// <returns>The meaning of color c, or null.</returns>
        public ColorLegend this[Color c]
        {
            get
            {
                if (this.explanations.ContainsKey(c))
                    return this.explanations[c];
                else
                    return null;
            }
        }

        /// <summary>
        /// Add a new explanation to the legend.
        /// </summary>
        /// <param name="c">Color to explain.</param>
        /// <param name="explanation">Explanation for the color.</param>
        /// <param name="index">Ordering index of color.</param>
        public void Add(Color c, string explanation, double index)
        {
            if (explanation != null && !this.explanations.ContainsKey(c))
            {
                if (this.explanations.Count == 0)
                {
                    // first element added
                    this.MinIndex = this.MaxIndex = index;
                    this.MinIndexColor = this.MaxIndexColor = c;
                }
                else
                {
                    if (this.MinIndex > index)
                    {
                        this.MinIndex = index;
                        this.MinIndexColor = c;
                    }
                    if (this.MaxIndex < index)
                    {
                        this.MaxIndex = index;
                        this.MaxIndexColor = c;
                    }
                }
                this.explanations.Add(c, new ColorLegend { label = explanation, orderingIndex = index });
            }
        }

        /// <summary>
        /// Select only the specified colors from the legend.
        /// </summary>
        /// <param name="restrictTo">Restrict to these colors.</param>
        /// <returns>A new legend.</returns>
        public Legend Select(IEnumerable<Color> restrictTo)
        {
            Legend result = new Legend(this.LegendSourceName);
            foreach (Color c in restrictTo)
            {
                ColorLegend legend = this[c];
                if (legend == null) continue;
                result.Add(c, legend.label, legend.orderingIndex);
            }
            return result;
        }

        /// <summary>
        /// The list of all colors in the legend.
        /// </summary>
        /// <returns>The list of all colors represented in the legend.</returns>
        public IEnumerable<Color> GetAllColors()
        {
            return this.explanations.Keys;
        }

        /// <summary>
        /// Add a new explanation for a color.
        /// </summary>
        /// <param name="col">Color to explain.</param>
        /// <param name="colorLegend">Explanation to add.</param>
        public void Add(Color col, ColorLegend colorLegend)
        {
            if (colorLegend == null)
                return;
            if (!this.explanations.ContainsKey(col))
                this.explanations.Add(col, colorLegend);
        }
    }

    /// <summary>
    /// Point on a two-dimensional surface, with double coordinates.
    /// </summary>
    public struct Point2D
    {
        /// <summary>
        /// Point coordinates.
        /// </summary>
        double x, y;

        /// <summary>
        /// Create a point on a 2-D plane surface.
        /// </summary>
        /// <param name="x">X coordinate.</param>
        /// <param name="y">Y coordinate.</param>
        public Point2D(double x, double y)
        {
            this.x = x;
            this.y = y;
        }

        /// <summary>
        /// Create a point at the specified coordinates from the origin.
        /// </summary>
        /// <param name="size">Size to encode as a point.</param>
        public Point2D(SizeF size)
        {
            this.x = size.Width;
            this.y = size.Height;
        }

        /// <summary>
        /// X coordinate of point.
        /// </summary>
        public double X { get { return this.x; } }
        /// <summary>
        /// Y Coordinate of point.
        /// </summary>
        public double Y { get { return this.y; } }

        /// <summary>
        /// Return a point having the max coordinates from two points.
        /// </summary>
        /// <param name="other">Point to compare against.</param>
        /// <returns>A new point having X = max(this.x, other.x) (similar for Y)</returns>
        public Point2D Max(Point2D other)
        {
            return new Point2D(Math.Max(this.X, other.X), Math.Max(this.Y, other.Y));
        }

        /// <summary>
        /// Return a point having the min coordinates from two points.
        /// </summary>
        /// <param name="other">Point to compare against.</param>
        /// <returns>A new point having X = min(this.x, other.x) (similar for Y)</returns>
        public Point2D Min(Point2D other)
        {
            return new Point2D(Math.Min(this.X, other.X), Math.Min(this.Y, other.Y));
        }

        /// <summary>
        /// True if a point has coordinates smaller than another one.
        /// </summary>
        /// <param name="left">Left point.</param>
        /// <param name="right">Right point.</param>
        /// <returns>True if the left point has both coordinates smaller.</returns>
        public static bool operator <(Point2D left, Point2D right)
        {
            return left.X < right.X && left.Y < right.Y;
        }

        /// <summary>
        /// True if a point has coordinates bigger than another one.
        /// </summary>
        /// <param name="left">Left point.</param>
        /// <param name="right">Right point.</param>
        /// <returns>True if the left point has both coordinates bigger.</returns>
        public static bool operator >(Point2D left, Point2D right)
        {
            return left.X > right.X && left.Y > right.Y;
        }

        /// <summary>
        /// True if a point has coordinates smaller or equal than another one.
        /// </summary>
        /// <param name="left">Left point.</param>
        /// <param name="right">Right point.</param>
        /// <returns>True if the left point has both coordinates smaller or equal.</returns>
        public static bool operator <=(Point2D left, Point2D right)
        {
            return left.X <= right.X && left.Y <= right.Y;
        }

        /// <summary>
        /// True if a point has coordinates greater or equal than another one.
        /// </summary>
        /// <param name="left">Left point.</param>
        /// <param name="right">Right point.</param>
        /// <returns>True if the left point has both coordinates greater or equal.</returns>
        public static bool operator >=(Point2D left, Point2D right)
        {
            return left.X >= right.X && left.Y >= right.Y;
        }

        /// <summary>
        /// The second point is a displacement: compute new endpoint.
        /// </summary>
        /// <param name="left">Original.</param>
        /// <param name="right">Displacement.</param>
        /// <returns>A new point, at the given displacement from the original one.</returns>
        public static Point2D operator +(Point2D left, Point2D right)
        {
            return new Point2D(left.X + right.X, left.Y + right.Y);
        }

        /// <summary>
        /// Translate a point by a given amount.
        /// </summary>
        /// <param name="xp">Amount to translate in X direction.</param>
        /// <param name="yp">Amount to translate in Y direction.</param>
        /// <returns>A new point.</returns>
        public Point2D Translate(double xp, double yp)
        {
            return new Point2D(this.X + xp, this.Y + yp);
        }

        /// <summary>
        /// String representation of the point.
        /// </summary>
        /// <returns>A string describing the point.</returns>
        public override string ToString()
        {
            return "(" + this.X + "," + this.Y + ")";
        }
    }

    /// <summary>
    /// Class describing a rectangle on a 2D plane with edges parallel to the axes.
    /// </summary>
    public class Rectangle2D
    {
        /// <summary>
        /// Two opposite corners of the rectangle.
        /// </summary>
        Point2D one, two;
        /// <summary>
        /// True if one is less than two.
        /// </summary>
        bool normalized;

        /// <summary>
        /// Create a rectangle given 4 coordinates.  Note that there is no requirement to have points in either order.
        /// </summary>
        /// <param name="lx">Left X coordinate.</param>
        /// <param name="ly">Left Y coordinate.</param>
        /// <param name="rx">Right X coordinate.</param>
        /// <param name="ry">Right Y coordinate.</param>
        public Rectangle2D(double lx, double ly, double rx, double ry)
        {
            one = new Point2D(lx, ly);
            two = new Point2D(rx, ry);
            normalized = one < two;
        }

        /// <summary>
        /// Create a rectangle from two points.
        /// </summary>
        /// <param name="one">One corner.</param>
        /// <param name="two">Second corner.</param>
        public Rectangle2D(Point2D one, Point2D two)
        {
            this.one = one;
            this.two = two;
            normalized = one < two;
        }

        /// <summary>
        /// Make the first corner to be lower and to the left of the second corner.
        /// </summary>
        /// <returns>A new rectangle.</returns>
        public Rectangle2D Normalize()
        {
            if (normalized)
                return this;

            bool swapx = one.X > two.X;
            bool swapy = one.Y > two.Y;

            return new Rectangle2D(
                swapx ? two.X : one.X,
                swapy ? two.Y : one.Y,
                swapx ? one.X : two.X,
                swapy ? one.Y : two.Y);
        }

        /// <summary>
        /// True if rectangle has null width or height.
        /// </summary>
        /// <returns>True if rectangle has zero area.</returns>
        public bool Degenerate()
        {
            // ReSharper disable CompareOfFloatsByEqualityOperator
            return (one.X == two.X) || (one.Y == two.Y);
            // ReSharper restore CompareOfFloatsByEqualityOperator
        }

        /// <summary>
        /// First corner.
        /// </summary>
        public Point2D Corner1 { get { return one; } }
        /// <summary>
        /// Second corner.
        /// </summary>
        public Point2D Corner2 { get { return two; } }

        /// <summary>
        /// Center of the rectangle.
        /// </summary>
        public Point2D Center
        {
            get
            {
                return new Point2D(one.X + this.Width / 2, one.Y + this.Height / 2);
            }
        }

        /// <summary>
        /// Compute the intersection of two rectangles.
        /// </summary>
        /// <param name="with">Rectangle to intersect with.</param>
        /// <returns>A new rectangle.  The result may be degenerate if the intersection is empty.</returns>
        public Rectangle2D Intersect(Rectangle2D with)
        {
            Point2D ul = this.one.Max(with.one);
            Point2D lr = this.two.Min(with.two);

            if (ul.X > lr.X ||
                ul.Y > lr.Y)
                return new Rectangle2D(0, 0, 0, 0);

            // We expect this is normalized
            return new Rectangle2D(ul, lr);
        }

        /// <summary>
        /// Rectangle area.
        /// </summary>
        /// <returns>The area of the rectangle.</returns>
        public double Area()
        {
            return Math.Abs((one.X - two.X) * (one.Y - two.Y));
        }

        /// <summary>
        /// If a rectangle is degenerate, stretch it a little.
        /// </summary>
        /// <returns>Always a non-degenerate, normalized rectangle.</returns>
        public Rectangle2D FixDegeneracy()
        {
            const double epsilon = 0.1;

            Rectangle2D norm = this.Normalize();
            // if the size is 0 make it slightly larger
            if (norm.Degenerate())
            {
                double xmin = norm.Corner1.X;
                double xmax = norm.Corner2.X;
                double ymin = norm.Corner1.Y;
                double ymax = norm.Corner2.Y;

                if (xmax <= xmin)
                    xmin -= xmax * 0.1 + epsilon;
                if (ymax <= ymin)
                    ymin -= ymax * 0.1 + epsilon;
                norm = new Rectangle2D(xmin, ymin, xmax, ymax);
            }
            return norm;
        }

        /// <summary>
        /// Height of rectangle.
        /// </summary>
        public double Height { get { return Math.Abs(one.Y - two.Y); } }

        /// <summary>
        /// Width of rectangle.
        /// </summary>
        public double Width { get { return Math.Abs(one.X - two.X); } }

        /// <summary>
        /// Generate a new rectangle, with the smallest integer coordinates which contains this one.
        /// </summary>
        /// <returns>A new rectangle.</returns>
        public Rectangle2D ExpandToIntegerCoordinates()
        {
            Rectangle2D norm = this.Normalize();
            return new Rectangle2D(Math.Floor(norm.one.X), Math.Floor(norm.one.Y), Math.Ceiling(norm.two.X), Math.Ceiling(norm.two.Y));
        }

        /// <summary>
        /// Create a rectangle given a corner and size.
        /// </summary>
        /// <param name="xl">Left X coordinate.</param>
        /// <param name="yl">Left Y coordinate.</param>
        /// <param name="width">Width of rectangle (may be negative).</param>
        /// <param name="height">Height of rectangle (may be negative).</param>
        /// <returns></returns>
        public static Rectangle2D MakeRectangle(double xl, double yl, double width, double height)
        {
            return new Rectangle2D(xl, yl, xl + width, yl + height);
        }

        /// <summary>
        /// Check whether a point is inside a rectangle.
        /// </summary>
        /// <param name="point">Point to check.</param>
        /// <returns>True if the point is inside the rectangle.</returns>
        public bool Inside(Point2D point)
        {
            Rectangle2D norm = this.Normalize();
            return norm.Corner1 <= point && point <= norm.Corner2;
        }

        /// <summary>
        /// Check whether a rectangle includes another one.
        /// </summary>
        /// <param name="other">The rectangle that should be inside.</param>
        /// <returns>True if other is inside this.</returns>
        public bool Includes(Rectangle2D other)
        {
            return Inside(other.Corner1) && Inside(other.Corner2);
        }

        /// <summary>
        /// Move a rectangle.
        /// </summary>
        /// <param name="x">Amount to move in x direction.</param>
        /// <param name="y">Amount to move in y direction.</param>
        /// <returns>A new rectangle.</returns>
        public Rectangle2D Translate(double x, double y)
        {
            return new Rectangle2D(this.Corner1.Translate(x, y), this.Corner2.Translate(x, y));
        }

        /// <summary>
        /// Size of rectangle.
        /// </summary>
        public Point2D Size
        {
            get
            {
                return new Point2D(this.Width, this.Height);
            }
        }
    }

    /// <summary>
    /// Represents a UNC pathname: machine, directory, file.
    /// If 'machine' is null, this is a directory on localhost.
    /// if 'file' is null, this represents a directory.
    /// The directory cannot be null or empty.
    /// </summary>
    [Serializable]
    public class UNCPathname
    {
        /// <summary>
        /// Machine hosting the path; if null, it represents 'localhost'.
        /// </summary>
        public string Machine { get; set; }

        /// <summary>
        /// Directory; may not be null; includes the share name.
        /// </summary>
        public string Directory { get; set; }

        /// <summary>
        /// Filename; may be null.
        /// </summary>
        public string Filename { get; set; }

        /// <summary>
        /// Create a UNC pathname from a machine, directory and file.
        /// </summary>
        /// <param name="machine">Machine hosting the path; if null or empty, it represents 'localhost'.</param>
        /// <param name="directory">Directory; may not be null.</param>
        /// <param name="file">Filename; if null or empty, the pathname represents a directory.</param>
        public UNCPathname(string machine, string directory, string file)
        {
            if (machine == "")
                machine = null;
            this.Machine = machine;
            if (string.IsNullOrEmpty(directory))
                throw new ArgumentException("The directory of a UNC pathname cannot be null");
            this.Directory = directory;
            if (file == "")
                file = null;
            this.Filename = file;
        }

        /// <summary>
        /// The name of the share.
        /// </summary>
        public string Sharename
        {
            get
            {
                if (string.IsNullOrEmpty(this.DirectoryAndFilename))
                    return "";
                string[] subpaths = Utilities.SplitPathname(this.DirectoryAndFilename);
                return subpaths[0];
            }
        }

        /// <summary>
        /// Path without the share.
        /// </summary>
        public string DirectoryAndFilenameNoShare
        {
            get
            {
                if (string.IsNullOrEmpty(this.DirectoryAndFilename))
                    return "";
                string[] subpaths = Utilities.SplitPathname(this.DirectoryAndFilename);
                string result = string.Join(Path.DirectorySeparatorChar.ToString(), subpaths, 1, subpaths.Length - 1);
                return result;
            }
        }

        /// <summary>
        /// Create a new uncpathname holding the same information as the other.
        /// </summary>
        /// <param name="other">UNCPathname to copy.</param>
        public UNCPathname(UNCPathname other)
        {
            this.Machine = other.Machine;
            this.Directory = other.Directory;
            this.Filename = other.Filename;
        }

        /// <summary>
        /// Create a UNC pathname from a string.
        /// </summary>
        /// <param name="path">Path to break into pieces.</param>
        public UNCPathname(string path)
        {
            // First extract machine name
            if (path.StartsWith(@"\\") ||
                path.StartsWith(@"//"))
            {
                int slash = path.IndexOf("/", 2);
                int bkslash = path.IndexOf("\\", 2);
                if (bkslash < slash || slash < 0)
                    slash = bkslash;
                if (slash >= 0)
                {
                    this.Machine = path.Substring(2, slash - 2); // length - 2
                    path = path.Substring(slash + 1);
                }
                else
                {
                    // there is just a machine
                    this.Machine = path.Substring(2);
                    path = "";
                }
            }
            else
                this.Machine = null;

            // extract the directory and file
            {
                int slash = path.LastIndexOf('/');
                int bkslash = path.LastIndexOf('\\');
                if (bkslash > slash)
                    slash = bkslash;
                if (slash >= 0)
                {
                    this.Filename = path.Substring(slash + 1);
                    this.Directory = path.Substring(0, slash);
                }
                else
                {
                    this.Filename = path;
                    this.Directory = null;
                    throw new ArgumentException("Pathname cannot contain an empty directory");
                }
            }
        }

        /// <summary>
        /// Create a pathname from a machine and a file name.
        /// </summary>
        /// <param name="machine">Machine name.</param>
        /// <param name="dirandfile">Pathname on local machine.</param>
        public UNCPathname(string machine, string dirandfile)
            : this(machine, Path.GetDirectoryName(dirandfile), Path.GetFileName(dirandfile))
        {
        }

        /// <summary>
        /// Pathname represented as a UNC pathname suitable for passing to File/Directory functions.
        /// </summary>
        /// <returns>The UNC pathname as a string.</returns>
        public override string ToString()
        {
            if (Filename == null)
                return this.UNCDirectory;
            else
                return Path.Combine(this.UNCDirectory, this.Filename);
        }

        /// <summary>
        /// The combined directory and filename path.
        /// If filename is null, only the directory is returned.
        /// </summary>
        public string DirectoryAndFilename
        {
            get
            {
                if (this.Filename == null)
                    return this.Directory.Trim('/', '\\');
                return Path.Combine(this.Directory.Trim('/', '\\'), this.Filename.Trim('/', '\\'));
            }
        }

        /// <summary>
        /// True if the pathname represents a directory, false if it represents a file.
        /// </summary>
        public bool IsDirectory
        {
            get { return this.Filename == null; }
        }

        /// <summary>
        /// True if the pathname is on the local machine (implicitly 'localhost'), false otherwise.
        /// </summary>
        public bool IsLocal
        {
            get { return this.Machine == null; }
        }

        /// <summary>
        /// Just the machine and directory, in UNC form.
        /// </summary>
        public string UNCDirectory
        {
            get
            {
                StringBuilder builder = new StringBuilder();
                if (Machine != null)
                {
                    builder.Append(@"\\");
                    builder.Append(this.Machine);
                }
                if (!Directory.StartsWith(@"\"))
                    builder.Append(@"\");
                builder.Append(this.Directory);
                return builder.ToString();
            }
        }
    }

    /// <summary>
    /// Map from objects to colors.
    /// <typeparam name="T">Type of data source providing the information in the color map.</typeparam>
    /// </summary>
    public class ColorMap<T> : IEnumerable<object>
    {
        /// <summary>
        /// Source of data used to build the color map.
        /// </summary>
        protected readonly T source;
        /// <summary>
        /// Map from object (a value in the column) to color.
        /// </summary>
        protected Dictionary<object, Color> map;
        /// <summary>
        /// Explanation for the color choices.
        /// </summary>
        protected readonly Legend legend;

        /// <summary>
        /// Create an empty color map.
        /// </summary>
        public ColorMap(T source)
        {
            this.source = source;
            this.map = new Dictionary<object, Color>();
            this.legend = new Legend("");
            this.DefaultColor = Color.Gray;
            this.IsContinuous = false;
        }

        /// <summary>
        /// Set the name of the source which originated the legend.
        /// </summary>
        /// <param name="legendSource">Name of the source which originated the legend.</param>
        public void SetLegendSourceName(string legendSource)
        {
            this.Legend.LegendSourceName = legendSource;
        }

        /// <summary>
        /// If true this is a map from continuous values, else it is from discrete values.
        /// </summary>
        public bool IsContinuous { get; protected set; }

        /// <summary>
        /// Source providing the data for the map.
        /// </summary>
        public T ColorSource { get { return this.source; } }

        /// <summary>
        /// Find color associated with a label.
        /// </summary>
        /// <param name="label">Label.</param>
        /// <returns>Color value.</returns>
        public virtual Color this[object label]
        {
            get
            {
                if (this.map.ContainsKey(label))
                    return this.map[label];
                else
                    return this.DefaultColor;
            }
            set
            {
                if (this.map.ContainsKey(label))
                    this.map[label] = value;
                else
                    this.map.Add(label, value);
            }
        }

        /// <summary>
        /// The default color, when color is not in the map.
        /// </summary>
        public Color DefaultColor { get; set; }

        /// <summary>
        /// Color scale 0..1 => 0..255
        /// </summary>
        protected static int CS(double c)
        {
            return (int)(c * 255);
        }

        /// <summary>
        /// Copy all data in a color map.
        /// </summary>
        /// <returns>A new color map which has the same information.</returns>
        public ColorMap<T> Clone()
        {
            ColorMap<T> retval = new ColorMap<T>(this.source);
            retval.DefaultColor = this.DefaultColor;
            retval.map = new Dictionary<object, Color>(this.map);
            return retval;
        }

        /// <summary>
        /// An enumerator over the map objects.
        /// </summary>
        /// <returns>All the objects stored in the map.</returns>
        public IEnumerator<object> GetEnumerator()
        {
            return this.map.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// The legend associated with this color map.
        /// </summary>
        public Legend Legend { get { return this.legend; } }

        /// <summary>
        /// Explanation for a given color.
        /// </summary>
        /// <param name="c">Color selected.</param>
        /// <returns>The legend explaining the color, if any.</returns>
        public Legend.ColorLegend Explanation(Color c)
        {
            return this.Legend[c];
        }

        /// <summary>
        /// Explanation for the color of a label.
        /// </summary>
        /// <param name="label">Label whose color is explained.</param>
        /// <returns>A Legend.ColorLegend explaining the color.</returns>
        public Legend.ColorLegend Explanation(object label)
        {
            return this.Legend[this[label]];
        }
    }

    /// <summary>
    /// Color map representing discrete data.
    /// </summary>
    /// <typeparam name="T">Type of data source for color map.</typeparam>
    public abstract class DiscreteColorMap<T> : ColorMap<T>
    {
        /// <summary>
        /// Total number of colors represented by this colormap.
        /// </summary>
        protected readonly int totalcolors;

        /// <summary>
        /// Generate a new colormap for the given number of colors.
        /// </summary>
        /// <param name="colors">Number of distinct colors expected.</param>
        /// <param name="source">Source of data.</param>
        protected DiscreteColorMap(T source, int colors)
            : base(source)
        {
            this.totalcolors = colors;
            this.IsContinuous = false;
        }

        /// <summary>
        /// Generate a color for color #index.
        /// </summary>
        /// <param name="index">Color number to generate [0..total).</param>
        /// <returns>A nice color, such that all total colors are distinguishable.</returns>
        protected abstract Color BucketColor(int index);

        /// <summary>
        /// Add a set of labels, all at the given index.
        /// </summary>
        /// <param name="labels">Labels to add.</param>
        /// <param name="index">Index between 0..totalcolors.</param>
        /// <param name="legd">Explanation for this class.</param>
        public void AddLabelClass(IEnumerable<object> labels, int index, string legd)
        {
            if (index < 0 || index >= this.totalcolors)
                throw new System.ArgumentException("Color index " + index + " out of range 0.." + totalcolors);

            Color c = this.BucketColor(index);
            int added = 0;
            foreach (object label in labels)
            {
                added++;
                this[label] = c;
            }
            if (added > 0 && legd != null)
            {
                this.legend.Add(c, legd, index);
            }
        }
    }

    /// <summary>
    /// A colormap containing some pre-computed colors.
    /// This works up to 32 colors only.
    /// </summary>
    /// <typeparam name="T">Type of data source for color map.</typeparam>
    public class PrecomputedColorMap<T> : DiscreteColorMap<T>
    {
        // ReSharper disable once StaticFieldInGenericType
        static Color[] preAssigned;

        static PrecomputedColorMap()
        {
            preAssigned = new Color[] { 
                Color.Red,        Color.Blue,        Color.Yellow,
                Color.Green,      Color.Magenta,     Color.Brown,
                
                Color.DarkRed,    Color.Indigo,      Color.Orange,
                Color.DarkGreen,  Color.DarkViolet,  Color.Chocolate, 
                
                Color.Tomato,     Color.Aqua,        Color.YellowGreen, 
                Color.SeaGreen,   Color.Plum,        Color.Wheat,       

                Color.Coral,      Color.Firebrick,   Color.Pink,       
                Color.Olive,      Color.PaleGreen,  

                Color.SaddleBrown,Color.Lime,        Color.Sienna,            
                Color.Goldenrod,  Color.PaleTurquoise, Color.Khaki,       
                Color.DarkKhaki,  Color.Purple,      Color.Peru
            };
        }

        /// <summary>
        /// Maximum number of colors that can be represented by this color map.
        /// </summary>
        public static int MaxColors { get { return preAssigned.Length; } }

        /// <summary>
        /// Allocate a pre-computed color map.
        /// </summary>
        /// <param name="source">Data source for this color map.</param>
        /// <param name="colors">Number of distinct colors.</param>
        public PrecomputedColorMap(T source, int colors)
            : base(source, colors)
        {
            if (colors > preAssigned.Length)
                throw new ArgumentOutOfRangeException("The pre-assigned color map does not support more than " + preAssigned.Length + " colors");
        }

        /// <summary>
        /// The color associated to a bucket.
        /// </summary>
        /// <param name="index">Bucket index.</param>
        /// <returns>The color.</returns>
        protected override Color BucketColor(int index)
        {
            int ix = index; //  (index * preAssigned.Length) / this.totalcolors;
            return preAssigned[ix];
        }
    }

    /// <summary>
    /// Map from objects to colors computing automatically colors for many objects mapped into a small number of buckets.
    /// </summary>
    /// <typeparam name="T">Type of data source for color map.</typeparam>
    public class HSVColorMap<T> : DiscreteColorMap<T>
    {
        /// <summary>
        /// Generate a new colormap for the given number of colors.
        /// </summary>
        /// <param name="colors">Number of distinct colors expected.</param>
        /// <param name="source">Source of data.</param>
        public HSVColorMap(T source, int colors)
            : base(source, colors)
        {
        }

        /// <summary>
        /// Generate a color for color #index.
        /// </summary>
        /// <param name="index">Color number to generate [0..total).</param>
        /// <returns>A nice color, such that all total colors are distinguishable.</returns>
        protected override Color BucketColor(int index)
        {
            return HSVColorMap<T>.OneCircleColor(index, this.totalcolors);
        }

        /// <summary>
        /// Generate a color for color #index, out of 'total' colors.
        /// </summary>
        /// <param name="index">Color number to generate [0..total).</param>
        /// <param name="total">Total number of colors expected.</param>
        /// <returns>A nice color, such that all total colors are distinguishable.</returns>
        private static Color OneCircleColor(int index, int total)
        {
            int samplesPerCircle = total;
            const int startOffset = 1; // rotate around circle
            const double fractionOfCircle = 0.8; // leave a gap to distinguish start from end
            int pointno = index + startOffset;

            double h = pointno * fractionOfCircle / samplesPerCircle;
            const double s = 1;
            double r, g, b;
            Utilities.HSVtoRGB(h, s, s, out r, out g, out b);

            Color c = Color.FromArgb(CS(r), CS(g), CS(b));
            return c;
        }
    }

    /// <summary>
    /// This class is used to turn selected properties of an object into something suitable for a databinding.
    /// </summary>
    public class PropertyEnumerator<T>
    {
        /// <summary>
        /// Skip these properties when enumerating.
        /// </summary>
        HashSet<string> skip;
        /// <summary>
        /// Expand these properties (which are probably classes themselves) into sub-fields.
        /// </summary>
        HashSet<string> expand;

        /// <summary>
        /// A property and its value.
        /// </summary>
        public class PropertyValue : IName
        {
            /// <summary>
            /// Name of property.
            /// </summary>
            public string ObjectName { get; private set; }
            /// <summary>
            /// Value of property.
            /// </summary>
            public string Value { get; private set; }

            /// <summary>
            /// Create a property value with a given name and value.
            /// </summary>
            /// <param name="name">Name of property.</param>
            /// <param name="value">Value of property.</param>
            public PropertyValue(string name, string value)
            {
                this.ObjectName = name ?? "<null>";
                this.Value = value ?? "<null>";
            }

            /// <summary>
            /// String representation of the property value.
            /// </summary>
            /// <returns>A string representing the property value.</returns>
            public override string ToString()
            {
                return this.ObjectName + "=" + this.Value;
            }
        }

        /// <summary>
        /// Type of the data item.
        /// </summary>
        // ReSharper disable once StaticFieldInGenericType
        static Type dataType = typeof(T);

        /// <summary>
        /// Common initialization code.
        /// </summary>
        private void Initialize()
        {
            this.skip = new HashSet<string>();
            this.expand = new HashSet<string>();
        }

        /// <summary>
        /// Create a propertyenumerator which looks at the properties of an 
        /// </summary>
        /// <param name="data">Object whose properties should be enumerated.</param>
        public PropertyEnumerator(T data)
        {
            this.Initialize();
            this.Data = data;
        }

        /// <summary>
        /// Create an empty property enumerator, with no object bound yet.
        /// </summary>
        public PropertyEnumerator()
        {
            this.Initialize();
            this.Data = default(T);
            this.SetExpandAllNonPrimitive = false;
            this.ValueFormatter = null;
        }

        /// <summary>
        /// Item whose properties are listed.
        /// </summary>
        public T Data { get; set; }

        /// <summary>
        /// In not null this function is used to format the values returned as strings.
        /// </summary>
        public Func<object, string> ValueFormatter { get; set; }

        /// <summary>
        /// Formats the value depending on the type.
        /// </summary>
        /// <param name="propertyValue">Value to format.</param>
        /// <returns>The formatted value.</returns>
        string FormattedValue(object propertyValue)
        {
            if (this.ValueFormatter != null)
                return this.ValueFormatter(propertyValue);
            return propertyValue.ToString();
        }

        /// <summary>
        /// If set all non-primitive properties (structs) are expanded by default.
        /// </summary>
        public bool SetExpandAllNonPrimitive
        {
            set
            {
                if (value)
                {
                    IEnumerable<PropertyInfo> properties = PropertyEnumerator<T>.dataType.GetProperties();
                    foreach (var prop in properties)
                    {
                        if (prop.PropertyType == typeof(string)) continue;
                        IEnumerable<PropertyInfo> recursiveproperties = prop.PropertyType.GetProperties();
                        if (recursiveproperties.Count() > 1)
                            this.expand.Add(prop.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Extract all properties whose values are computed.
        /// </summary>
        public IEnumerable<string> AllPropertyNames()
        {
            IEnumerable<string> properties = PropertyEnumerator<T>.dataType.GetProperties().
                 Where(prop => !this.skip.Contains(prop.Name)).
                 SelectMany(this.ExtracRecursivetProperties);
            return properties;
        }

        /// <summary>
        /// If the property has to be expanded recursively extract its properties.
        /// </summary>
        /// <param name="prop">Property to expand on.</param>
        /// <returns>The list of properties of this property.</returns>
        private IEnumerable<string> ExtracRecursivetProperties(PropertyInfo prop)
        {
            if (!this.expand.Contains(prop.Name))
                return new string [] { prop.Name };
            IEnumerable<PropertyInfo> retval = prop.GetType().GetProperties();
            return retval.Select(p => prop.Name + "." + p.Name);
        }

        /// <summary>
        /// Extract the value of a property; if the property has to be expanded, it will be a list of property values.
        /// </summary>
        /// <param name="prop">Property whose value is extracted.</param>
        /// <returns>A collection of property values with one or more elements.</returns>
        private IEnumerable<PropertyValue> ExtractPropertyValue(PropertyInfo prop)
        {
            object propvalue = prop.GetValue(this.Data, null);

            if (this.expand.Contains(prop.Name))
            {
                IEnumerable<PropertyValue> retval =
                    propvalue.GetType().GetProperties().Select(p =>
                        new PropertyValue(prop.Name + "." + p.Name, this.FormattedValue(p.GetValue(propvalue, null))));
                foreach (PropertyValue p in retval)
                    yield return p;
            }
            else
            {
                yield return new PropertyValue(prop.Name, this.FormattedValue(propvalue));
            }
        }

        /// <summary>
        /// Get the list of all interesting properties of this object.
        /// <param name="retval">Populate this list with the property values.</param>
        /// </summary>
        public void PopulateWithProperties(IList<PropertyEnumerator<T>.PropertyValue> retval)
        {
            IEnumerable<PropertyValue> properties = PropertyEnumerator<T>.dataType.GetProperties().
                Where(prop => !this.skip.Contains(prop.Name)).
                SelectMany(this.ExtractPropertyValue);
            foreach (PropertyValue v in properties)
                retval.Add(v);
        }

        /// <summary>
        /// Do not display these properties.
        /// </summary>
        /// <param name="properties">Properties to skip.</param>
        public void Skip(params string[] properties)
        {
            foreach (string p in properties)
                this.skip.Add(p);
        }

        /// <summary>
        /// Do not display these properties.
        /// </summary>
        /// <param name="properties">Properties to skip.</param>
        public void Skip(IEnumerable<string> properties)
        {
            foreach (string p in properties)
                this.skip.Add(p);
        }

        /// <summary>
        /// Expand the fields of these properties.
        /// </summary>
        /// <param name="properties">Properties to expand; each subproperty will become one result.</param>
        public void Expand(params string[] properties)
        {
            foreach (string p in properties)
                this.expand.Add(p);
        }
    }

    /// <summary>
    /// An abstract class for streaming through files piece by piece.
    /// </summary>
    public interface IFileStreamer
    {
        /// <summary>
        /// Done reading file.
        /// </summary>
        void Close();
        /// <summary>
        /// Read one "line" from the file.
        /// </summary>
        /// <returns>The line parsed into pieces.</returns>
        string[] ReadLine();
        /// <summary>
        /// Read the file header.
        /// </summary>
        /// <returns>The line parsed into pieces.</returns>
        string[] ReadHeader();
        /// <summary>
        /// Write the file header.
        /// </summary>
        /// <param name="header">Header to write.</param>
        void WriteHeader(IEnumerable<string> header);
        /// <summary>
        /// Write a line in the file.
        /// </summary>
        /// <param name="line">Line to write.</param>
        void WriteLine(IEnumerable<string> line);
        /// <summary>
        /// Start the stream from the beginning.
        /// </summary>
        void Reset();
        /// <summary>
        /// The whole contents of the file.
        /// </summary>
        /// <returns>The complete file contents.</returns>
        IEnumerable<string[]> ReadFile();

        /// <summary>
        /// Line being parsed.
        /// </summary>
        long CurrentLineNumber { get; }
    }

    /// <summary>
    /// Base implementation of file streamer.
    /// </summary>
    public abstract class BaseFileStreamer : IFileStreamer
    {
        /// <summary>
        /// True if the file is expected to contain a header.
        /// </summary>
        protected bool HasHeader { get; set; }
        /// <summary>
        /// File header.
        /// </summary>
        protected string[] header;
        /// <summary>
        /// Used to read the file.
        /// </summary>
        protected ISharedStreamReader reader;
        /// <summary>
        /// Used to write the file.
        /// </summary>
        protected StreamWriter writer;
        /// <summary>
        /// Number of fields on each line.
        /// </summary>
        protected int fields;
        /// <summary>
        /// Current line number in file.
        /// </summary>
        protected long currentLine;
        /// <summary>
        /// If true, all lines are expected to have the same length.
        /// </summary>
        public bool AllLinesSameLength { get; set; }
        /// <summary>
        /// File to access.
        /// </summary>
        protected readonly string filename;
        /// <summary>
        /// Mode the stream operates.
        /// </summary>
        FileMode mode;
        /// <summary>
        /// Delegate used to report errors.
        /// </summary>
        protected readonly StatusReporter statusReporter;

        /// <summary>
        /// Create a base streamer.
        /// </summary>
        /// <param name="filename">File to access.</param>
        /// <param name="mode">File mode.</param>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        protected BaseFileStreamer(string filename, FileMode mode, StatusReporter statusReporter)
        {
            this.reader = null;
            this.writer = null;
            this.filename = filename;
            this.mode = mode;
            this.statusReporter = statusReporter;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            this.Reset();
        }

        /// <summary>
        /// Go to the beginning.
        /// </summary>
        public virtual void Reset()
        {
            this.Close();
            switch (this.mode)
            {
                case FileMode.Open:
                    this.reader = new FileSharedStreamReader(filename);
                    if (this.reader.Exception != null)
                        throw this.reader.Exception;
                    this.writer = null;
                    break;
                case FileMode.Create:
                    this.writer = new StreamWriter(filename);
                    this.reader = null;
                    break;
                default:
                    this.Error("no support for file mode " + this.mode);
                    break;
            }
        }

        /// <summary>
        /// We are done reading/writing to the csv file.
        /// </summary>
        public virtual void Close()
        {
            if (this.reader != null)
                this.reader.Close();
            else if (this.writer != null)
                this.writer.Close();
        }

        /// <summary>
        /// Read one line from the file.  If the file has a header, the header should be read first.
        /// </summary>
        /// <returns>The line read.</returns>
        public virtual string[] ReadLine()
        {
            if (this.HasHeader && this.header == null)
            {
                this.Error("Attempt to read a line from a file before reading the header");
            }
            return this.ReadLineInternal();
        }

        /// <summary>
        /// Returns the header of the file; if the file does not have a header, this will trigger an exception. 
        /// Must be called before ReadLine().
        /// </summary>
        /// <returns>The file header.</returns>
        public virtual string[] ReadHeader()
        {
            if (!this.HasHeader)
                this.Error("Attempt to read header from a file without a header");
            this.header = ReadLineInternal();
            return this.header;
        }

        /// <summary>
        /// Line being parsed.
        /// </summary>
        public virtual long CurrentLineNumber { get { return this.currentLine; } }

        /// <summary>
        /// Must be called before WriteLine.  Writes the file header.
        /// </summary>
        /// <param name="hdr">File header.</param>
        public virtual void WriteHeader(IEnumerable<string> hdr)
        {
            if (!this.HasHeader)
                this.Error("Attempt to add a header to a file without header");
            this.header = hdr.ToArray();
            this.WriteLineInternal(this.header.ToList());
        }

        /// <summary>
        /// Writes a line in the file; if the file has a header, must be called after WriteHeader.
        /// </summary>
        /// <param name="line">Line to write to the file.</param>
        public virtual void WriteLine(IEnumerable<string> line)
        {
            if (this.HasHeader && this.header == null)
                this.Error("Attempt to write a line in file before a header");
            this.WriteLineInternal(line.ToList());
        }

        /// <summary>
        /// The contents of the file, except the header.
        /// Closes the file at the end.
        /// </summary>
        /// <returns>An iterator over the contents.</returns>
        public IEnumerable<string[]> ReadFile()
        {
            string[] line;
            if (this.HasHeader)
            {
                line = this.ReadHeader();
                if (line == null)
                    goto done;
            }
            while (true)
            {
                line = this.ReadLine();
                if (line == null)
                {
                    goto done;
                }
                yield return line;
            }

        done:
            this.reader.Close();
            this.reader = null;
            yield break;
        }

        /// <summary>
        /// Internal implementation for line reading.
        /// </summary>
        /// <returns>The line read as a sequence of strings.  Returns null when there is nothing to read.</returns>
        protected abstract string[] ReadLineInternal();

        /// <summary>
        /// Internal implementation of writing.
        /// </summary>
        /// <param name="line">Strings to be written to one file line.</param>
        protected abstract void WriteLineInternal(List<string> line);

        /// <summary>
        /// Signal an error.
        /// </summary>
        /// <param name="message">Error message.</param>
        protected abstract void Error(string message);
    }

    /// <summary>
    /// Reads a file as a set of key-value pairs.
    /// Each record is a complete line.
    /// TODO: Change this to properly parse the fields.
    /// </summary>
    public class KVPFileStreamer : BaseFileStreamer
    {
        private const string separator = ",";

        /// <summary>
        /// Create a file streamer which knows how to operate on a KVP file.
        /// It always reads and writes complete lines.
        /// </summary>
        /// <param name="filename">File to operate on.</param>
        /// <param name="mode">Mode of access.</param>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        public KVPFileStreamer(string filename, FileMode mode, StatusReporter statusReporter)
            : base(filename, mode, statusReporter)
        {
        }

        /// <summary>
        /// Internal read implementation.
        /// </summary>
        /// <returns>A line read from the file.</returns>
        protected override string[] ReadLineInternal()
        {
            string line = this.reader.ReadLine();
            if (line == null)
            {
                return null;
            }
            this.currentLine++;
            return new string[] { line };
        }

        /// <summary>
        /// Internal write implementation.
        /// </summary>
        /// <param name="line">Line to write.</param>
        protected override void WriteLineInternal(List<string> line)
        {
            StringBuilder builder = new StringBuilder();
            bool first = true;
            foreach (string w in line)
            {
                if (!first)
                    builder.Append(separator);
                builder.Append(w);
                first = false;
            }
            this.writer.WriteLine(builder.ToString());
            this.currentLine++;
        }

        /// <summary>
        /// Signal an error.
        /// </summary>
        /// <param name="message">Error message.</param>
        protected override void Error(string message)
        {
            this.statusReporter("KVP file `" + this.filename + "' format error on line " + this.currentLine + ": " + message, StatusKind.Error);
        }
    }

    /// <summary>
    /// A comma-separated list of values in a file; may be used to read or write the file (but not both at the same time).
    /// (The separator may be something else besides comma too).
    /// Values may be quoted.
    /// </summary>
    public class CSVFileStreamer : BaseFileStreamer
    {
        /// <summary>
        /// Create a CSV file.
        /// </summary>
        /// <param name="filename">File to create.</param>
        /// <param name="mode">Read or write?</param>
        /// <param name="hasHeader">True if the file contains a header.</param>
        /// <param name="statusReporter">Delegate used to report errors.</param>
        public CSVFileStreamer(string filename, FileMode mode, bool hasHeader, StatusReporter statusReporter)
            : base(filename, mode, statusReporter)
        {
            this.HasHeader = hasHeader;
            this.header = null;
            this.fields = -1; // not known yet
            this.Separator = ',';
            this.currentLine = 0;
            this.AllLinesSameLength = true;
        }

        /// <summary>
        /// Field separator; by default it's comma.
        /// </summary>
        private char separator;
        /// <summary>
        /// The field separator.
        /// </summary>
        public char Separator
        {
            set
            {
                if (this.currentLine != 0)
                    this.Error("You cannot change the file separator in the middle of the file");
                if (value == '\"')
                    throw new ArgumentException("You cannot use the quote as a field separator in a CSV file");
                this.separator = value;
            }
            get
            {
                return this.separator;
            }
        }

        private void Check(IEnumerable<string> line)
        {
            int linelen = line.Count();
            if (this.fields == -1)
                this.fields = linelen;
            else if (this.AllLinesSameLength && (this.fields != linelen))
                this.Error("Line has " + linelen + " instead of " + this.fields + " fields.");
        }

        /// <summary>
        /// Internal line reading from CSV files.
        /// </summary>
        /// <returns>A set of tokens read from a line.</returns>
        /// <remarks>True when done reading.</remarks>
        protected override string[] ReadLineInternal()
        {
            if (this.reader.EndOfStream)
            {
                return null;
            }

            string line = this.reader.ReadLine();
            if (!line.Contains("\""))
            {
                // quick case
                string[] retval = line.Split(this.Separator);
                Check(retval);
                this.currentLine++;
                return retval;
            }

        tryToSplit:
            List<string> results = new List<string>();
            int currentIndex = 0;

            while (currentIndex < line.Length)
            {
                string word;
                if (line[currentIndex] == '\"')
                {
                    currentIndex = this.ParseQuotedWord(line, currentIndex, out word);
                }
                else
                {
                    currentIndex = this.ParseUnquotedWord(line, currentIndex, out word);
                }

                if (currentIndex == -1)
                {
                    string nextOne = this.reader.ReadLine();
                    line += nextOne;
                    this.currentLine++;
                    goto tryToSplit; // retry parsing together with the next line
                }

                results.Add(word);
                if (currentIndex < line.Length)
                {
                    // expect a separator
                    if (line[currentIndex] != this.separator)
                        this.Error("Not found expected separator character");
                    currentIndex++;
                }
            }
            this.currentLine++;
            this.Check(results);
            return results.ToArray();
        }

        /// <summary>
        /// Read an unquoted word from the given line starting at index 'currentIndex'.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <param name="currentIndex">Start index of word.</param>
        /// <param name="word">Found word.</param>
        /// <returns>Index of separator after word (or of end of line).</returns>
        private int ParseUnquotedWord(string line, int currentIndex, out string word)
        {
            int separatorIndex = line.IndexOf(this.separator, currentIndex);
            if (separatorIndex == -1)
            {
                word = line.Substring(currentIndex);
                return line.Length;
            }
            else
            {
                word = line.Substring(currentIndex, separatorIndex - currentIndex);
                return separatorIndex;
            }
        }

        /// <summary>
        /// Read a quoted word from the given line starting at index 'currentIndex'.
        /// </summary>
        /// <param name="line">Line to parse.</param>
        /// <param name="currentIndex">Start index of word.</param>
        /// <param name="word">Found word.</param>
        /// <returns>Index of separator after word (or of end of line).  Returns -1 if the end quote is missing.</returns>
        private int ParseQuotedWord(string line, int currentIndex, out string word)
        {
            int endIndex = currentIndex + 1;
            while (true)
            {
                endIndex = line.IndexOf('\"', endIndex);
                if (endIndex == -1)
                {
                    this.Error("Newline in quoted string");
                    word = "";
                    return -1;
                }
                if (endIndex == line.Length - 1)
                {
                    // last word on line
                    break;
                }
                else if (line[endIndex + 1] == '\"')
                {
                    // quoted quote, continue
                    endIndex += 2;
                    continue;
                }
                else
                {
                    // end of quoted word
                    break;
                }
            }

            word = line.Substring(currentIndex + 1, endIndex - currentIndex - 1); // drop the start and end quotes
            word = word.Replace("\"\"", "\""); // fix quoted quotes
            return endIndex + 1;
        }

        /// <summary>
        /// Signal an error that occurred during file reading.
        /// </summary>
        /// <param name="message">Message describing the error.</param>
        protected override void Error(string message)
        {
            this.statusReporter("CSV file `" + Path.GetFileName(this.filename) + "' format error on line " + this.currentLine + ": " + message, StatusKind.Error);
        }

        /// <summary>
        /// Add a line to the file.
        /// </summary>
        /// <param name="line">Line to add.</param>
        protected override void WriteLineInternal(List<string> line)
        {
            Check(line);
            bool first = true;
            foreach (string word in line)
            {
                string wordToWrite = Quote(word);
                if (!first)
                    this.writer.Write("{0}", this.separator);
                else
                    first = false;
                this.writer.Write("{0}", wordToWrite);
            }
            this.writer.WriteLine();
            this.currentLine++;
        }

        private string Quote(string word)
        {
            if (word.Contains('\"'))
            {
                // double the quotes
                word = word.Replace("\"", "\"\"");
                return "\"" + word + "\"";
            }

            // quote the whole word if it contains spaces
            if (word.Contains(this.Separator))
                return "\"" + word + "\"";
            else
                return word;
        }
    }

    /// <summary>
    /// The objects in this class have each a unique id (in the class).
    /// </summary>
    public interface IUniqueId
    {
        /// <summary>
        /// Get the object's unique id.
        /// </summary>
        int Id { get; }
    }

    /// <summary>
    /// Interface for objects which have a name.
    /// </summary>
    public interface IName
    {
        /// <summary>
        /// Get the object's name.  Cannot use just 'name' since this property is often already defined.
        /// </summary>
        string ObjectName { get; }
    }

    /// <summary>
    /// A stripped-down stream reader.
    /// Never throws an exception; rather, stores the state in the 'Exception' field.
    /// </summary>
    public interface ISharedStreamReader : IDisposable
    {
        /// <summary>
        /// Exception that occurred while opening the stream.
        /// </summary>
        Exception Exception { get; }

        /// <summary>
        /// True if the reader has reached the end of stream.
        /// </summary>
        bool EndOfStream { get; }

        /// <summary>
        /// Read one line from the stream.
        /// </summary>
        /// <returns></returns>
        string ReadLine();

        /// <summary>
        /// Close the actual stream reader.
        /// </summary>
        void Close();

        /// <summary>
        /// Read the stream to the end from the current position.
        /// </summary>
        /// <returns>The contents of the stream.</returns>
        string ReadToEnd();

        /// <summary>
        /// Read all the lines remaining in the stream.
        /// </summary>
        /// <returns>An iterator over all remaining lines.</returns>
        IEnumerable<string> ReadAllLines();
    }

    /// <summary>
    /// Common functionality too all ISharedStreamReaders.
    /// </summary>
    public abstract class BaseSharedStreamReader : ISharedStreamReader
    {
        /// <summary>
        /// Exception that occurred while opening the stream.
        /// </summary>
        public Exception Exception { get; protected set; }

        /// <summary>
        /// An exception occurred while trying to build this stream.
        /// </summary>
        /// <param name="ex">Exception that occurred.</param>
        protected BaseSharedStreamReader(Exception ex)
        {
            this.Exception = ex;
        }

        /// <summary>
        /// Basic shared stream reader.
        /// </summary>
        protected BaseSharedStreamReader()
        {
            this.Exception = null;
        }

        /// <summary>
        /// True if we have reached the end of the stream.
        /// </summary>
        public abstract bool EndOfStream { get; }

        /// <summary>
        /// Read one line from the stream.
        /// </summary>
        /// <returns></returns>
        public abstract string ReadLine();

        /// <summary>
        /// Dispose of the stream.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Read the whole stream to the end.
        /// </summary>
        /// <returns>A string containing the whole contents of the stream.</returns>
        public virtual string ReadToEnd()
        {
            StringBuilder result = new StringBuilder();
            foreach (string s in this.ReadAllLines())
                result.AppendLine(s);
            return result.ToString();
        }

        /// <summary>
        /// Close the stream.
        /// </summary>
        public abstract void Close();

        /// <summary>
        /// Read all the lines remaining in the stream.
        /// </summary>
        /// <returns>An iterator over all remaining lines.</returns>
        public IEnumerable<string> ReadAllLines()
        {
            while (! this.EndOfStream)
            {
                yield return this.ReadLine();
            }
            this.Close();
        }
    }

    /// <summary>
    /// SharedStreamReader reading from another stream.
    /// </summary>
    public class SharedStreamReader : BaseSharedStreamReader
    {
        /// <summary>
        /// If not null use this stream to write to the cache.
        /// </summary>
        StreamWriter cacheWriter;
        /// <summary>
        /// Delegate to call when stream is closed if caching.
        /// </summary>
        Action onClose;

        /// <summary>
        /// A shared stream reader representing an exception.
        /// </summary>
        /// <param name="ex">Exception represented by this stream reader.</param>
        public SharedStreamReader(Exception ex)
            : base(ex)
        {
            this.cacheWriter = null;
        }

        /// <summary>
        /// Actual stream where the data is being read from.
        /// </summary>
        protected StreamReader actualReader;

        /// <summary>
        /// Create a stream reader for the specified stream.
        /// </summary>
        /// <param name="reader">Stream to read.</param>
        public SharedStreamReader(StreamReader reader)
        {
            this.actualReader = reader;
            this.cacheWriter = null;
        }

        /// <summary>
        /// Create a stream reader for the specified stream, cache the file in the specified stream.
        /// </summary>
        /// <param name="reader">Stream to read.</param>
        /// <param name="cacheWriter">Use this stream to copy the file to a cache.</param>
        /// <param name="onClose">Delegate to call when stream is completely read.</param>
        public SharedStreamReader(StreamReader reader, StreamWriter cacheWriter, Action onClose)
        {
            this.actualReader = reader;
            this.cacheWriter = cacheWriter;
            this.onClose = onClose;
        }

        /// <summary>
        /// Set the cache writer stream.
        /// </summary>
        /// <param name="cw">Stream used to write to the cache.</param>
        /// <param name="onCl">Action to invoke on close.</param>
        public void SetCacheWriter(StreamWriter cw, Action onCl)
        {
            this.cacheWriter = cw;
            this.onClose = onCl;
        }

        /// <summary>
        /// SharedStreamReader reading from a null stream.
        /// </summary>
        public SharedStreamReader()
        {
            this.actualReader = StreamReader.Null;
            this.cacheWriter = null;
        }

        /// <summary>
        /// True if the reader has reached the end of stream.
        /// </summary>
        public override bool EndOfStream
        {
            get
            {
                return this.actualReader.EndOfStream;
            }
        }

        /// <summary>
        /// Read one line from the stream.
        /// </summary>
        /// <returns></returns>
        public override string ReadLine()
        {
            string line = this.actualReader.ReadLine();
            if (this.cacheWriter != null)
                this.cacheWriter.WriteLine(line);
            return line;
        }

        /// <summary>
        /// Close the actual stream reader.
        /// </summary>
        public override void Close()
        {
            if (this.actualReader != StreamReader.Null)
                this.actualReader.Close();
            if (this.cacheWriter != null)
                this.cacheWriter.Close();
            if (this.onClose != null)
                this.onClose();
        }

        /// <summary>
        /// Done with the stream.
        /// </summary>
        public override void Dispose()
        {
            this.actualReader.Dispose();
        }

        /// <summary>
        /// Read the stream to the end from the current position.
        /// </summary>
        /// <returns>The contents of the stream.</returns>
        public override string ReadToEnd()
        {
            string result = this.actualReader.ReadToEnd();
            if (this.cacheWriter != null)
            {
                this.cacheWriter.Write(result);
                this.cacheWriter.Close();
                if (this.onClose != null)
                    this.onClose();
            }
            return result;
        }
    }

     /// <summary>
    /// SharedStreamReader reading from another stream.
    /// </summary>
    public class FileSharedStreamReader : SharedStreamReader
    {
        /// <summary>
        /// File that is being read.
        /// </summary>
        string file;

        /// <summary>
        /// Create a file stream reader representing an exception.
        /// </summary>
        /// <param name="ex">Exception.</param>
        public FileSharedStreamReader(Exception ex)
            : base(ex)
        { }

        /// <summary>
        /// Create a stream reader for the specified file.
        /// </summary>
        /// <param name="file">File to read.</param>
        public FileSharedStreamReader(string file)
        {
            try
            {
                this.file = file;
                Stream rd = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite); 
                this.actualReader = new StreamReader(rd);
            }

            catch (Exception ex)
            {
                // I don't know how to handle other exceptions.
                this.file = "Exception: " + ex.Message;
                this.Exception = ex;
                return;
            }
        }

        /// <summary>
        /// Create a file shared stream reader backed-up by a cache.
        /// </summary>
        /// <param name="file">File to read from.</param>
        /// <param name="cache">Cache here the contents read from the file.</param>
        /// <param name="onClose">Action to invoke when file is closed.</param>
        public FileSharedStreamReader(string file, StreamWriter cache, Action onClose)
        {
            try
            {
                Stream rd = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                this.actualReader = new StreamReader(rd);
                this.SetCacheWriter(cache, onClose);
            }
            catch (Exception ex)
            {
                // I don't know how to handle other exceptions.
                this.Exception = ex;
                return;
            }
        }

        /// <summary>
        /// String representation of the File reader.
        /// </summary>
        /// <returns>A string representing the stream reader.</returns>
        public override string ToString()
        {
            return this.file;
        }
    }

    /// <summary>
    /// A shared stream reader reading from a string collection.
    /// </summary>
    public class StringIteratorStreamReader : BaseSharedStreamReader
    {
        /// <summary>
        /// Read from this iterator.
        /// </summary>
        IEnumerator<string> contents;
        /// <summary>
        /// True if we have reached the end of the stream.
        /// </summary>
        bool endOfStream;

        /// <summary>
        /// End of the stream.
        /// </summary>
        public override bool EndOfStream
        {
            get { return this.endOfStream; }
        }

        /// <summary>
        /// Create a stream reader representing an exception.
        /// </summary>
        /// <param name="ex">Exception.</param>
        public StringIteratorStreamReader(Exception ex)
            : base(ex)
        {
            this.endOfStream = true;
        }

        /// <summary>
        /// A string iterator reading from this data.
        /// </summary>
        /// <param name="data"></param>
        public StringIteratorStreamReader(IEnumerable<string> data)
        {
            this.contents = data.GetEnumerator();
            this.endOfStream = !this.contents.MoveNext();
        }

        /// <summary>
        /// Read one line from the stream.
        /// </summary>
        /// <returns></returns>
        public override string ReadLine()
        {
            string line = this.contents.Current;
            this.endOfStream = !this.contents.MoveNext();
            return line;
        }

        /// <summary>
        /// Get rid of the stream.
        /// </summary>
        public override void Dispose()
        {
        }

        /// <summary>
        /// Finish reading the stream.
        /// </summary>
        public override void Close()
        {
            this.endOfStream = true;
            this.contents = null;
        }
    }
}
