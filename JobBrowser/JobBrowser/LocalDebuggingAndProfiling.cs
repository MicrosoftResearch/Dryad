
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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Research.JobObjectModel;
using Microsoft.Research.Tools;
using Microsoft.Research.UsefulForms;
using Microsoft.Win32;
using System.Diagnostics;

namespace Microsoft.Research.DryadAnalysis
{
    /// <summary>
    /// Class that encapsulates the steps required to debug a vertex locally.
    /// </summary>
    internal class LocalDebuggingAndProfiling
    {
        private UNCPathname workingDirPath;
        private string guid;
        private StatusReporter reporter;
        private int version;
        private int number;
        private ClusterConfiguration cluster;
#pragma warning disable 649
        private DirectoryInfo TempDir;
#pragma warning restore 649
        private bool cpuSampling;


        /// <summary>
        /// Creates a new instance of this class in preparation for debugging a vertex locally.
        /// </summary>
        /// <param name="guid">Guid for the vertex to debug.</param>
        /// <param name="config">Cluster where job debugged is running.</param>
        /// <param name="vertexWorkingDirPath">Path to the (remote) working directory of the vertex.</param>
        /// <param name="statusWriter">Used to display status messages nicely.</param>
        /// <param name="version">Vertex version to debug.</param>
        /// <param name="managed">If true debug managed version.</param>
        /// <param name="cpuSampling">If true perform cpu sampling based profiling.</param>
        /// <param name="number">Vertex number.</param>
        public LocalDebuggingAndProfiling(ClusterConfiguration config,
            string guid,
            int number,
            int version,
            IClusterResidentObject vertexWorkingDirPath,
            bool managed,
            bool cpuSampling,
            StatusReporter statusWriter)
        {
            this.cluster = config;
            this.workingDirPath = (vertexWorkingDirPath as UNCFile).Pathname;
            this.guid = guid;
            this.reporter = statusWriter;
            this.cpuSampling = cpuSampling;
            this.number = number;
            this.version = version;
            if (!managed)
                throw new Exception("Unmanaged debugging not supported");
        }

        private static void ReplaceAll(List<string> data, string from, string to)
        {
            for (int i = 0; i < data.Count; i++)
                data[i] = data[i].Replace(from, to);
        }

        private static void ReplaceAllRegex(List<string> data, string regex, string to)
        {
            for (int i = 0; i < data.Count; i++)
                data[i] = Regex.Replace(data[i], regex, to);
        }

        /// <summary>
        /// Update the rerun script to the local conditions.
        /// </summary>
        /// <param name="script">Script for rerunning the vertex.</param>
        /// <returns>True of success.</returns>
        private bool RewriteRerunScript(string script)
        {
            if (!File.Exists(script))
            {
                this.reporter("Could not find the script used to rerun the vertex " + script, StatusKind.Error);
                return false;
            }
            try
            {
                List<string> contents = File.ReadAllLines(script).ToList();
                ReplaceAll(contents, "call ..\\ProcessEnv.cmd", "");

                {
                    {
                        // If debugging managed code include setting the LAUNCHDEBUG env var
                        contents.Insert(0, "set LAUNCHDEBUG=yes" + Environment.NewLine);
                    }
                }

                File.WriteAllLines(script, contents.ToArray());
            }
            catch (Exception e)
            {
                this.reporter("Exception while creating script to rerun vertex: " + e.Message, StatusKind.Error);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Performs local debugging for the vertex specified by this instance
        /// </summary>
        /// <param name="stateInfo">Ignored; required to run the object on a separate thread.</param>
        public void performDebugging(object stateInfo)
        {
            // Prepare for local vertex rerun
            if (!this.prepareLocalWorkingDir())
            {
                this.reporter("Error preparing local work directory; cannot perform local debug", StatusKind.Error);
                return;
            }
            string tempDirPath = TempDir.FullName;

            // Modify rerun script to exclude call to ProcessEnv
            string rerunFile = string.Format("vertex-{0}-{1}-rerun.cmd", this.number, this.version);
            rerunFile = Path.Combine(tempDirPath, rerunFile);
            File.Copy(rerunFile, rerunFile + ".bak", true);
            bool success = this.RewriteRerunScript(rerunFile);
            if (!success)
                return;

            this.reporter("Starting debugged process", StatusKind.LongOp);
            // Execute the rerun script in a separate process, do not wait for termination
            Utilities.RunProcess(rerunFile, tempDirPath, false, false, false, false);

        }

        /// <summary>
        /// Performs local profiling for the vertex specified by this instance
        /// </summary>
        /// <param name="stateInfo">Ignored; required to run the object on a separate thread.</param>
        public void performProfiling(object stateInfo)
        {
            // Prepare for local vertex rerun
            if (!prepareLocalWorkingDir())
            {
                this.reporter("Error preparing local work directory; cannot perform local profiling", StatusKind.Error); 
                return;
            }
            string tempDirPath = TempDir.FullName;

            // Get the location of the profiling/performance tools
            object regpath = Registry.GetValue(@"HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\devenv.exe", "", null);
            if (regpath == null)
            {
                this.reporter("Cannot locate visual studio executable devenv.exe", StatusKind.Error);
                this.CleanUp();
                return;
            }

            string devenvPath = "\"" + regpath +"\""; 
            string perfToolsPath = devenvPath.ToLower().Replace(@"common7\ide\devenv.exe", @"team tools\performance tools\");
            {
                perfToolsPath += @"x64\";
            }

            // Modify rerun script to exclude call to ProcessEnv and rerun vertex with sample based profiling      
            string rerunFile = string.Format("vertex-{0}-{1}-rerun.cmd", this.number, this.version);
            rerunFile = Path.Combine(tempDirPath, rerunFile);
            IEnumerable<string> originalRerunScript = File.ReadAllLines(rerunFile).ToList();
            List<string> rerunScript = new List<string>();

            originalRerunScript = originalRerunScript.Select(s => s.Replace("call ..\\ProcessEnv.cmd", ""));
            rerunScript.Add("cd " + tempDirPath);
            const string callCmd = "call ";
            rerunScript.Add(callCmd + perfToolsPath + @"vsperfclrenv " + (cpuSampling ? "/sampleon" : "/samplegclife"));
            rerunScript.Add(callCmd + perfToolsPath + @"vsperfcmd -start:sample -output:" + Path.Combine(tempDirPath, "sample-based-profiling-data"));

            {
                string xmlexechostinvoke = 
                    originalRerunScript.First(l => l.Contains("XmlExecHost"));
                xmlexechostinvoke = xmlexechostinvoke.Replace(
                    "XmlExecHost.exe ", 
                    perfToolsPath + "vsperfcmd -launch:XmlExecHost.exe " + (cpuSampling ? "" : "-gc:lifetime ") + "-args:\"") + "\"";
                rerunScript.Add(callCmd + xmlexechostinvoke);
            }
            
            rerunScript.Add(callCmd + perfToolsPath + @"vsperfcmd -shutdown");
            rerunScript.Add(callCmd + perfToolsPath + @"vsperfclrenv /off");
            rerunScript.Add(devenvPath + " " + Path.Combine(tempDirPath, "sample-based-profiling-data.vsp"));
            File.WriteAllLines(rerunFile, rerunScript.ToArray());

            // Code for modifying the rerun script for instrumentation based profiling. Not used for now since instrumentation is really slow
            //perfScript += callCmd + perfToolsPath + @"vsperfclrenv /tracegclife" + Environment.NewLine;
            //perfScript += callCmd + perfToolsPath + @"vsinstr -excludesmallfuncs " + Path.Combine(tempDirPath, "XmlExecHost.exe") + Environment.NewLine;
            //perfScript += callCmd + perfToolsPath + @"vsperfcmd -start:trace -output:" + Path.Combine(tempDirPath, "sample-based-profiling-data") + Environment.NewLine;
            //rerunScript = perfScript + rerunScript + Environment.NewLine;
            //rerunScript += callCmd + perfToolsPath + @"vsperfcmd -shutdown" + Environment.NewLine;
            //rerunScript += callCmd + perfToolsPath + @"vsperfclrenv /off" + Environment.NewLine;

            // Execute the rerun script in a separate process, do not wait for termination
            Utilities.RunProcess(rerunFile, tempDirPath, false, false, true, true);
            this.reporter("Profiling the vertex locally...", StatusKind.LongOp);
        }

        /// <summary>
        /// Prepares the local (temporary) working directory required to execute the vertex locally either for debugging or profiling.
        /// </summary>
        /// <returns>True on success.</returns>
        private bool prepareLocalWorkingDir()
        {
            return false;
        }

        private void CleanUp()
        {
            // Delete the temp directory 
            if (this.TempDir != null)
            {
                try
                {
                    this.TempDir.Delete(true);
                }
                catch (Exception ex)
                {
                    this.reporter("Error deleting temp dir: " + ex.Message, StatusKind.Error);
                    Trace.TraceInformation("Error deleting temporary directory: " + ex);
                }
            }
        }

        /// <summary>
        /// Copies all the files from the 'fromDir' to the 'toDir'.
        /// </summary>
        /// <param name="fromDir">Source directory.</param>
        /// <param name="toDir">Destination directory.</param>
        /// <returns>Number of copied files, or -1 on failure.</returns>
        /// <param name="skipRegex">Skip files whose name matches this regex.</param>
        /// <param name="reporter">Delegate used to report errors.</param>
        private static int copyFiles(string fromDir, string toDir, string skipRegex, StatusReporter reporter)
        {
            return copyFiles(fromDir, toDir, null, skipRegex, reporter);
        }

        /// <summary>
        /// Copies all the files matching the 'pattern' from the 'fromDir' to the 'toDir'.
        /// </summary>
        /// <param name="fromDir">Source directory.</param>
        /// <param name="toDir">Destination directory.</param>
        /// <param name="yespattern">Cope only the files matching this shell pattern.</param>
        /// <returns>Number of copied files, or -1 on failure.</returns>
        /// <param name="reporter">Delegate used to report errors.</param>
        /// <param name="skipRegex">Skip files matching this regex; if empty string don't skip anything.</param>
        private static int copyFiles(string fromDir, string toDir, string yespattern, string skipRegex, StatusReporter reporter)
        {
            try
            {
                FileInfo[] fromFiles;
                if (yespattern == null)
                {
                    fromFiles = Directory.CreateDirectory(fromDir).GetFiles();
                }
                else
                {
                    fromFiles = Directory.CreateDirectory(fromDir).GetFiles(yespattern);
                }
                int copied = 0;
                Regex skip = new Regex(skipRegex);
                foreach (FileInfo fromFile in fromFiles)
                {
                    if (!string.IsNullOrEmpty(skipRegex) &&
                        skip.IsMatch(fromFile.Name))
                        continue;

                    copied++;
                    string destFile = Path.Combine(toDir, fromFile.Name);
                    if (File.Exists(destFile))
                    {
                        string bakfile = destFile + ".bak";
                        FileInfo destFileInfo = new FileInfo(destFile);
                        if (destFileInfo.LastWriteTime >= fromFile.LastWriteTime
                            && !File.Exists(bakfile) // file may have been modified
                            && (destFileInfo.Length == fromFile.Length))
                        {
                            reporter("Skipping copying " + fromFile.Name + " since it seems to be already present.", StatusKind.OK);
                            continue;
                        }
                    }

                    reporter("Copying " + fromFile.Name + " (" + string.Format("{0:N0}", fromFile.Length) + " bytes)", StatusKind.LongOp);
                    fromFile.CopyTo(Path.Combine(toDir, fromFile.Name), true);
                }
                return copied;
            }
            catch (Exception ex)
            {
                reporter("Error during file copy: " + ex.Message, StatusKind.Error);
                return -1;
            }
        }


        /// <summary>
        /// Check whether the debugging/profiling can be done.
        /// </summary>
        /// <returns>True if the architecture allows debugging.</returns>
        // ReSharper disable once UnusedParameter.Global
        internal bool CheckArchitecture(StatusReporter status)
        {
            return true;
        }

        /// <summary>
        /// Emit a warning about the user actions which may need to be performed.
        /// </summary>
        /// <returns>True if the operation should be performed, false if it is cancelled.</returns>
        /// <param name="debugging">If true warn about debugging, else warn about profiling.</param>
        /// <param name="doNotRepeat">Set to true of the warning should not be repeated.</param>
        internal bool EmitWarning(bool debugging, out bool doNotRepeat)
        {
            string warning;
            if (debugging)
            {
                warning = "For a better debugging experience please consider doing the folowing actions:\n";
                warning += "- set `Tool > Options... > Debugging > General > Enable Just My Code' to `false' in Visual Studio\n";
            }
            else
            {
                warning = "Profiling is only supported on 64-bit machines with Visual Studio installed.\n"
                    + "The vertex will now be executed and profiled locally. This may take a significant amount of time.\n"
                    + "When the profiling is completed the result will be displayed in a new Visual Studio instance.";
            }

            WarningBox box = new WarningBox(warning);
            box.ShowDialog();
            doNotRepeat = box.DoNotShowAgain;
            return !box.Cancelled;
        }
    }
}
