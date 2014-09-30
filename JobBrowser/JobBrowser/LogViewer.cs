
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
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;
using Microsoft.Research.JobObjectModel;
using Microsoft.Research.Tools;

namespace Microsoft.Research.DryadAnalysis
{
    /// <summary>
    /// A log viewer displays fragments of logs or other text files.
    /// </summary>
    public partial class LogViewer : Form
    {
        bool canceled;

        DGVData<TextFileLine> shownText;
        DGVData<PositionedDryadLogEntry> shownLogLines;
        StatusWriter status;
        
        /// <summary>
        /// A text file line is represented in this way.
        /// </summary>
        class TextFileLine
        {
            private long Line;
            private string Contents;

            public TextFileLine(long line, string contents)
            {
                this.Line = line;
                this.Contents = contents;
            }
        }

        /// <summary>
        /// True if the form has been closed (or work has been cancelled in some way).
        /// </summary>
        public bool Cancelled { get { return this.canceled; } }

        private void Initialize(bool textFile, string header)
        {
            this.Text = header;
            this.status = new StatusWriter(this.toolStripStatusLabel_status, this.statusStrip, this.Status);
            this.canceled = false;
            if (textFile)
            {
                this.shownText = new DGVData<TextFileLine>();
                this.filteredDataGridView.SetDataSource(this.shownText);
                // ReSharper disable PossibleNullReferenceException

                this.filteredDataGridView.DataGridView.Columns["Line"].FillWeight = 5;
                this.filteredDataGridView.DataGridView.Columns["Line"].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
                this.contextMenuStrip.Enabled = false;
            }
            else
            {
                this.shownLogLines = new DGVData<PositionedDryadLogEntry>();
                this.filteredDataGridView.SetDataSource(this.shownLogLines);
                foreach (string s in new string[] { "Malformed", "IsError", "OriginalLogLine", "File", "LineNo" })
                {
                    this.filteredDataGridView.DataGridView.Columns[s].Visible = false;
                }
                this.filteredDataGridView.DataGridView.Columns["Timestamp"].DefaultCellStyle.Format = "HH:mm:ss.fff";
                // ReSharper restore PossibleNullReferenceException
            }
            this.filteredDataGridView.DataGridView.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.DisplayedCellsExceptHeader;
            this.filteredDataGridView.DataGridView.AllowUserToResizeColumns = true;
            this.filteredDataGridView.DataGridView.Columns[this.filteredDataGridView.DataGridView.Columns.Count - 1].AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill;
        }

        /// <summary>
        /// Create a new log viewer.
        /// </summary>
        /// <param name="textFile">If true the viewer will display a text file.</param>
        /// <param name="header">Information to display in the window header.</param>
        public LogViewer(bool textFile, string header)
        {
            this.InitializeComponent();
            this.Initialize(textFile, header);
        }

        /// <summary>
        /// Create a new log viewer to display a file.
        /// </summary>
        public LogViewer()
        {
            this.InitializeComponent();
        }

        /// <summary>
        /// Load a specified file.
        /// </summary>
        /// <param name="file">File to load.</param>
        public void LoadFile(IClusterResidentObject file)
        {
            string filename = file.Name;

            bool text = true;
            string basefilename = Path.GetFileName(filename);
            if (basefilename != null && (basefilename.EndsWith(".log") && basefilename.StartsWith("cosmos")))
                text = false;

            long len = file.Size;

            this.Initialize(text, basefilename);
            //ISharedStreamReader sr = new FileSharedStreamReader(filename);
            ISharedStreamReader sr = file.GetStream(false);
            long lineno = 0;
            long bytes = 0;

            List<TextFileLine> toAddText = new List<TextFileLine>();
            List<PositionedDryadLogEntry> toAddLog = new List<PositionedDryadLogEntry>();
            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                bytes += line.Length;
                if (this.shownText != null)
                    toAddText.Add(new TextFileLine(lineno, line));
                else
                {
                    PositionedDryadLogEntry cle = new PositionedDryadLogEntry(filename, lineno, line);
                    if (cle.Malformed)
                    {
                        Trace.TraceInformation("Malformed log entry: " + cle.OriginalLogLine);
                    }
                    else
                    {
                        toAddLog.Add(cle);
                    }
                }
                if (lineno++ % 100 == 0 && len > 0)
                {
                    this.UpdateProgress((int)(bytes * 100 / len));
                }
            }

            if (this.shownText != null)
                this.shownText.SetItems(toAddText);
            else
                this.shownLogLines.SetItems(toAddLog);
            this.Status("Loaded " + lineno + " lines.", StatusKind.OK);
            this.UpdateProgress(100);
            sr.Close();
            this.filteredDataGridView.DataGridView.AutoResizeColumns(DataGridViewAutoSizeColumnsMode.DisplayedCellsExceptHeader);
        }

        /// <summary>
        /// Display a status message.
        /// </summary>
        /// <param name="message">Message to display.</param>
        /// <param name="statusKind">Message severity.</param>
        public void Status(string message, StatusKind statusKind)
        {
            if (this.canceled || this.IsDisposed)
                return;
            this.status.Status(message, statusKind);
        }

        /// <summary>
        /// Add a text line to the viewer.
        /// </summary>
        /// <param name="lineno">Line number to add.</param>
        /// <param name="text">Text of the line.</param>
        /// <param name="file">File that the information comes from.</param>
        public void AddLine(string file, long lineno, string text)
        {
            if (this.canceled || this.IsDisposed)
                return;

            if (this.InvokeRequired)
            {
                var action = new Action<LogViewer, string, long, string>( (lv, f, l, t) => lv.AddLine(f, l, t));
                this.Invoke(action, this, file, lineno, text);
            }
            else
            {
                if (this.shownText != null)
                {
                    TextFileLine tfl = new TextFileLine(lineno, text);
                    this.shownText.AddItem(tfl);
                }
                else
                {
                    PositionedDryadLogEntry cle = new PositionedDryadLogEntry(file, lineno, text);
                    if (cle.Malformed)
                        return;
                    this.shownLogLines.AddItem(cle);
                }
                return;
            }
        }

        /// <summary>
        /// Update the progress bar.
        /// </summary>
        /// <param name="percent">How much has been done.</param>
        public void UpdateProgress(int percent)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action<int>(this.UpdateProgress), percent);
            }
            else
            {
                if (this.canceled || this.IsDisposed)
                    return;
                try
                {
                    this.toolStripProgressBar.Value = percent;
                }
                catch
                { }
            }
        }

        /// <summary>
        /// Close the log viewer.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void closeToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.canceled = true;
            this.Close();
        }

        /// <summary>
        /// We are done adding information.
        /// </summary>
        internal void Done()
        {
            if (this.canceled || this.IsDisposed)
                return;
            this.UpdateProgress(100);
            this.Status("OK", StatusKind.OK);
        }

        private void LogViewer_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.canceled = true;
        }

        /// <summary>
        /// User wants to the the location of the message.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Row selected.</param>
        private void locationToolStripMenuItem_Click(object sender, EventArgs e)
        {
            string position = "";
            var rows = this.filteredDataGridView.DataGridView.SelectedRows;
            for (int i = 0; i < rows.Count; i++)
            {
                PositionedDryadLogEntry entry = ((PositionedDryadLogEntry)rows[i].DataBoundItem);
                position += entry.File + ":" + entry.LineNo + Environment.NewLine;
            }
            MessageBox.Show(position, "File containing log entries");
        }
    }

    /// <summary>
    /// A line from a file.
    /// </summary>
    /// <typeparam name="T">Type of line representation.</typeparam>
    public class PositionedLine<T>
        where T : IParse, new()
    {
        /// <summary>
        /// File containing the log entry.
        /// </summary>
        public string File { get; protected set; }
        /// <summary>
        /// Line number.
        /// </summary>
        public long LineNo { get; protected set; }
        /// <summary>
        /// Actual line from file.
        /// </summary>
        public T Line { get; protected set; }

        /// <summary>
        /// Create a positioned line.
        /// </summary>
        /// <param name="file">File containing the line.</param>
        /// <param name="lineno">Line number in file.</param>
        /// <param name="line">Actual line in file.</param>
        public PositionedLine(string file, long lineno, string line)
        {
            this.File = file;
            this.LineNo = lineno;
            this.Line = new T();
            this.Line.Parse(line);
        }
    }

    /// <summary>
    /// Cosmos log entry with position information.
    /// </summary>
    public class PositionedDryadLogEntry : DryadLogEntry
    {
        /// <summary>
        /// File containing the log entry.
        /// </summary>
        public string File { get; protected set; }
        /// <summary>
        /// Line number.
        /// </summary>
        public long LineNo { get; protected set; }

        /// <summary>
        /// Create a positioned log entry.
        /// </summary>
        /// <param name="file">File containing the log entry.</param>
        /// <param name="lineno">Line number.</param>
        /// <param name="line">Line contents.</param>
        public PositionedDryadLogEntry(string file, long lineno, string line)
            : base(line)
        {
            this.File = file;
            this.LineNo = lineno;
        }
    }
}
