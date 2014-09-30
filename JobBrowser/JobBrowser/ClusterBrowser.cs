
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
using System.ComponentModel;
using System.Configuration;
using System.Drawing;
using System.Linq;
using System.Net;
using System.Windows.Forms;
using System.Diagnostics;
using Microsoft.Research.JobObjectModel;
using Microsoft.Research.Tools;
using Microsoft.Research.UsefulForms;

namespace Microsoft.Research.DryadAnalysis
{
    /// <summary>
    /// Class to browse jobs on cluster, copy, summarize and start visualization.
    /// </summary>
    public partial class ClusterBrowser : Form
    {
        /// <summary>
        /// Configuration for the currently selected cluster.
        /// </summary>
        ClusterConfiguration Configuration { get; set; }
        /// <summary>
        /// Configuration for current cluster to analyze.
        /// </summary>
        ClusterStatus clusterStatus;
        /// <summary>
        /// Cluster selected by user.
        /// </summary>
        string cluster;
        /// <summary>
        /// Complete list of jobs on the cluster.
        /// </summary>
        List<ClusterJobInformation> completeJobsList;
        /// <summary>
        /// Used to display status messages.
        /// </summary>
        StatusWriter status;
        /// <summary>
        /// Timer invoked to refresh automatically.
        /// </summary>
        Timer refreshTimer;
        /// <summary>
        /// Settings persisting after form closure.
        /// </summary>
        ClusterBrowserSettings formSettings;
        /// <summary>
        /// File logging detailed errors.
        /// </summary>
        private TextWriterTraceListener LogFile;

        private BackgroundWorkQueue queue;

        /// <summary>
        /// Jobs from the cluster.
        /// </summary>
        DGVData<ClusterJobInformation> clusterJobs;
        /// <summary>
        /// Virtual cluster selected by the user.
        /// </summary>
        string SelectedVirtualCluster { get { return this.comboBox_virtualCluster.Text; } }

        /// <summary>
        /// Create a cluster browser object which stores the databases in the specified directory.
        /// </summary>
        public ClusterBrowser()
        {
            this.InitializeComponent();
            this.status = new StatusWriter(this.statuslabel, this.statusStrip, this.Status);

            BackgroundWorker queueWorker = new BackgroundWorker();
            this.queue = new BackgroundWorkQueue(queueWorker, null, null);

            this.completeJobsList = new List<ClusterJobInformation>();
            this.refreshTimer = new Timer();
            this.refreshTimer.Interval = 30000; // 30 seconds
            this.refreshTimer.Tick += this.refreshTimer_Tick;
            this.toolStripMenuItem_job.Enabled = false;

            this.clusterJobs = new DGVData<ClusterJobInformation>();
            this.filteredDataGridView.SetDataSource(this.clusterJobs);
            this.filteredDataGridView.DataGridView.Columns["IsUnavailable"].Visible = false;
            this.filteredDataGridView.DataGridView.Columns["Cluster"].Visible = false;
            this.filteredDataGridView.DataGridView.Columns["Name"].FillWeight = 50;
            this.filteredDataGridView.DataGridView.Columns["Status"].FillWeight = 10;
            this.filteredDataGridView.ViewChanged += this.filteredDataGridView_ViewChanged;

            #region TOOLTIPS
            //ToolTip help = new ToolTip();
            //help.SetToolTip(this.combo_cluster, "Cluster whose jobs are visualized (or a \"Cache\" cluster with previously seen data).");
            //help.SetToolTip(this.autoRefreshToolStripMenuItem, "Select to refresh the cluster view every 30 seconds.");
            #endregion

            this.Status("Please pick a cluster.", StatusKind.OK);
        }

        /// <summary>
        /// Display the current status.
        /// </summary>
        /// <param name="msg">Message to display.</param>
        /// <param name="kind">Kind of status message.</param>
        public void Status(string msg, StatusKind kind)
        {
            this.status.Status(msg, kind);
        }

        /// <summary>
        /// The used has selected a new cluster.
        /// </summary>
        /// <param name="clusterName">Cluster selected by the user.</param>
        // ReSharper disable once UnusedMethodReturnValue.Local
        private void ClusterSelected(string clusterName)
        {
            this.cluster = clusterName;

            NetworkCredential credential = null;
            try
            {
                ClusterConfiguration config = ClusterConfiguration.KnownClusterByName(clusterName);

                this.Configuration = config;
                this.diagnoseToolStripMenuItem.Visible = config.SupportsDiagnosis;
                this.diagnoseToolStripMenuItem1.Visible = config.SupportsDiagnosis;
            }
            catch (Exception ex)
            {
                this.Status("Could not connect to cluster " + clusterName + ": " + ex.Message, StatusKind.Error);
                if (ex.Message.Contains("Unauthorized"))
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (credential != null)
                        // ReSharper disable once HeuristicUnreachableCode
                        InvalidateCredentials(credential.Domain);
                }
                return;
            }

            this.Text = "Cluster: " + clusterName;
            this.clusterJobs.Clear();

            {
                this.comboBox_virtualCluster.Enabled = false;
                this.label_vc.Enabled = false;

                this.RefreshClusterJobList();
            }
        }


        /// <summary>
        /// The timer to refresh the view has ticked.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void refreshTimer_Tick(object sender, EventArgs e)
        {
            this.RefreshClusterJobList();
        }

        /// <summary>
        /// Cache here network credentials, mapping from domain to credential.
        /// </summary>
        static Dictionary<string, NetworkCredential> clusterCredentials = new Dictionary<string, NetworkCredential>();


        /// <summary>
        /// Probably password was mistyped; forget these credentials.
        /// </summary>
        /// <param name="domain">Domain whose password we have to forget.</param>
        static void InvalidateCredentials(string domain)
        {
            clusterCredentials.Remove(domain);
        }
        
        /// <summary>
        /// Queue a work item.
        /// </summary>
        /// <param name="item">Item to queue.</param>
        /// <param name="cancellationPolicy">Policy which describes which queued items to cancel.</param>
        private void Queue(IBackgroundWorkItem item, Func<IBackgroundWorkItem, bool> cancellationPolicy = null)
        {
            if (cancellationPolicy == null)
                cancellationPolicy = i => item.Description == i.Description;
            item.Queue(this.queue, this.Status, this.UpdateProgress, cancellationPolicy);
        }

        /// <summary>
        /// Query the cluster again for the list of jobs.
        /// </summary>
        /// <returns>True if the cluster could be found.</returns>
        private void RefreshClusterJobList()
        {
            if (this.cluster == null) return;

            string clus = this.cluster;
            if (!string.IsNullOrEmpty(this.SelectedVirtualCluster))
                clus += "/" + this.SelectedVirtualCluster;
            this.Status("Querying cluster " + clus, StatusKind.LongOp);

            try
            {
                    this.openFromURLToolStripMenuItem.Visible = false;
                if (
                    this.Configuration is CacheClusterConfiguration)
                {
                    this.filteredDataGridView.DataGridView.Columns["VirtualCluster"].Visible = true;
                }
                else
                {
                    this.filteredDataGridView.DataGridView.Columns["VirtualCluster"].Visible = false;
                }
                this.clusterStatus = this.Configuration.CreateClusterStatus();
                this.toolStripMenuItem_job.Enabled = true;

                var item = new BackgroundWorkItem<List<ClusterJobInformation>>(
                    m => BuildClusterJobList(m, this.clusterStatus, this.SelectedVirtualCluster),
                    this.JobListRetrieved,
                    "getJobs");
                this.Queue(item);
            }
            catch (Exception ex)
            {
                this.Status("Cannot retrieve information from cluster " + cluster + ": " + ex.Message, StatusKind.Error);                
                Trace.TraceInformation(ex.ToString());
                this.comboBox_virtualCluster.Text = "";
            }
        }

        private void JobListRetrieved(bool cancelled, List<ClusterJobInformation> jobs)
        {
            if (cancelled) return;

            this.filteredDataGridView.DataGridView.ClearSelection();
            this.completeJobsList = jobs;
            this.clusterJobs.SetItems(this.completeJobsList);
            if (!this.filteredDataGridView.RedoSort())
                // ReSharper disable once AssignNullToNotNullAttribute
                this.filteredDataGridView.DataGridView.Sort(this.filteredDataGridView.DataGridView.Columns["Date"], ListSortDirection.Descending);
            this.filteredDataGridView.DataGridView.Focus();
            this.filteredDataGridView.DataGridView.AutoResizeColumns(DataGridViewAutoSizeColumnsMode.DisplayedCells);
        }

        /// <summary>
        /// The view of the data grid has been changed.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void filteredDataGridView_ViewChanged(object sender, EventArgs e)
        {
            this.Status("Showing " + this.clusterJobs.VisibleItemCount + "/" + this.completeJobsList.Count + " jobs", StatusKind.OK);
        }

        /// <summary>
        /// Talk to the web server and build the list of clustr jobs; used it to populate the upper panel.
        /// </summary>
        /// <param name="virtualCluster">Virtual cluster selected; defined only for Scope clusters.</param>
        /// <param name="manager">Communication manager.</param>
        /// <param name="status">Cluster to scan.</param>
        private static List<ClusterJobInformation> BuildClusterJobList(CommManager manager, ClusterStatus status, string virtualCluster)
        {
            return status.GetClusterJobList(virtualCluster, manager).ToList();
        }

        /// <summary>
        /// Find cluster jobs selected in the upper pane.
        /// </summary>
        /// <returns>A reference to the cluster job information.</returns>
        private IEnumerable<ClusterJobInformation> SelectedJobs()
        {
            DataGridViewSelectedRowCollection sel = this.filteredDataGridView.DataGridView.SelectedRows;
            if (sel.Count < 1)
            {
                Status("You must select exactly some rows in the upper pane.", StatusKind.Error);
                yield break;
            }

            for (int i = 0; i < sel.Count; i++)
            {
                ClusterJobInformation ti = sel[i].DataBoundItem as ClusterJobInformation;
                yield return ti;
            }
        }

        /// <summary>
        /// Start the job browser from a job summary.
        /// </summary>
        /// <param name="js">Job summary to browse.</param>
        private void browseFromJobSummary(DryadLinqJobSummary js)
        {
            if (js == null)
                return;

            // TODO: this should run in the background
            CommManager manager = new CommManager(this.Status, delegate { }, new System.Threading.CancellationTokenSource().Token);
            DryadLinqJobInfo job = DryadLinqJobInfo.CreateDryadLinqJobInfo(this.clusterStatus.Config, js, false, manager);
            if (job != null)
            {
                JobBrowser browser = new JobBrowser(job);
                browser.Show();
                this.Status("OK", StatusKind.OK);
            }
            else
            {
                this.Status("Could not find information about job", StatusKind.Error);
            }
        }

        /// <summary>
        /// The user has double-clicked a row in the clusterJobTable.  Browse the job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Event describing position.</param>
        private void filteredDataGridView_CellMouseDoubleClick(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.RowIndex < 0)
                return;
            ClusterJobInformation task = (ClusterJobInformation)this.filteredDataGridView.DataGridView.Rows[e.RowIndex].DataBoundItem;
            DryadLinqJobSummary js = task.DiscoverDryadLinqJob(this.clusterStatus, this.Status);
            if (js == null)
            {
                this.Status("Error discovering job information on cluster.", StatusKind.Error);
            }
            else
            {
                this.Status("Starting job browser...", StatusKind.LongOp);
                this.browseFromJobSummary(js);
            }
        }

        /// <summary>
        /// Exit application.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void exitToolStripMenuItem2_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        /// <summary>
        /// Start a job browser on the specified job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void jobBrowserToolStripMenuItem_Click(object sender, EventArgs e)
        {
            IEnumerable<ClusterJobInformation> ti = this.SelectedJobs();
            this.Status("Starting job browser...", StatusKind.LongOp);
            IEnumerable<DryadLinqJobSummary> jobs = ti.Select(t => t.DiscoverDryadLinqJob(this.clusterStatus, this.Status)).ToList();

            CommManager manager = new CommManager(this.Status, delegate { }, new System.Threading.CancellationTokenSource().Token);
            IEnumerable<DryadLinqJobInfo> detailed = jobs.Select(j => DryadLinqJobInfo.CreateDryadLinqJobInfo(this.clusterStatus.Config, j, false, manager));
            foreach (DryadLinqJobInfo j in detailed)
            {
                if (j == null) continue;
                JobBrowser jb = new JobBrowser(j);
                jb.Show();
            }
            this.Status("OK", StatusKind.OK);
        }

        /// <summary>
        /// User clicked the refresh menu item.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void refreshToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            this.RefreshClusterJobList();
        }

        /// <summary>
        /// User opens a job by typing a job url.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void openFromURLToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var dialog = new CustomDialog("Job URL:");
            DialogResult result = dialog.ShowDialog();
            switch (result)
            {
                case DialogResult.Cancel:
                default:
                    return;
                case DialogResult.OK:
                    {
                        string url = dialog.UserInput;
                        if (url == "")
                            return;
                        DryadLinqJobSummary summary = null;
                        try
                        {
                            summary = this.clusterStatus.DiscoverDryadLinqJobFromURL(url, this.Status);
                        }
                        catch (Exception ex)
                        {
                            this.Status("Could not find job associated to url " + url + ": exception " + ex.Message, StatusKind.Error);
                        }

                        if (summary == null)
                        {
                            this.Status("Could not locate job associated to url " + url, StatusKind.Error);
                        }

                        this.browseFromJobSummary(summary);
                        break;
                    }
            }
        }

        /// <summary>
        /// User intends to terminate the selected job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void terminateToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var todo = this.SelectedJobs().ToList();
            if (todo.Count() != 1)
            {
                this.Status("You have to select exactly one job to terminate", StatusKind.Error);
                return;
            }
            IEnumerable<DryadLinqJobSummary> jobs = todo.Select(j => j.DiscoverDryadLinqJob(this.clusterStatus, this.Status)).Where(j => j != null);

            var item = new BackgroundWorkItem<bool>(
                m => ClusterWork.CancelJobs(jobs, this.clusterStatus, m),
                (c, b) => { },
                "cancel");
            this.Queue(item);
        }

        /// <summary>
        /// Update the progress bar.
        /// </summary>
        /// <param name="value">Value to set the progress bar to.</param>
        public void UpdateProgress(int value)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action<int>(this.UpdateProgress), value);
            }
            else
            {
                if (this.toolStripProgressBar != null)
                    // there could be overflows here
                    this.toolStripProgressBar.Value = Math.Min(value, 100);
            }
        }

        /// <summary>
        /// The user is closing the form; save application settings.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void ClusterBrowser_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.refreshTimer.Stop();
            this.queue.Stop();
            this.formSettings.Location = this.Location;
            this.formSettings.Size = this.Size;
            this.formSettings.MaximizeWindow = this.WindowState == FormWindowState.Maximized;
            this.formSettings.AutoRefresh = this.autoRefreshToolStripMenuItem.Checked;
            this.formSettings.KnownClusters = ClusterConfiguration.KnownClustersSerialization();
            this.formSettings.Save();
        }

        /// <summary>
        /// Form is being loaded; restore settings.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void ClusterBrowser_Load(object sender, EventArgs e)
        {
            this.formSettings = new ClusterBrowserSettings();
            Rectangle rect = System.Windows.Forms.Screen.PrimaryScreen.Bounds;
            // set location only if it is inside
            if (rect.Contains(this.formSettings.Location))
                this.Location = this.formSettings.Location;
            bool maximized = this.formSettings.MaximizeWindow;
            if (maximized)
                this.WindowState = FormWindowState.Maximized;
            else
                // then we care about the size
                this.Size = this.formSettings.Size;
            this.autoRefreshToolStripMenuItem.Checked = this.formSettings.AutoRefresh;

            this.AddClusterNameToMenu("<add>");
            this.AddClusterNameToMenu("<scan>");

            ClusterConfiguration.ReconstructKnownCluster(this.formSettings.KnownClusters);

            int found = 0;
            IEnumerable<string> clusters = ClusterConfiguration.GetKnownClusterNames();
            foreach (string c in clusters)
            {
                this.AddClusterNameToMenu(c);
                var config = ClusterConfiguration.KnownClusterByName(c);
                if (config is CacheClusterConfiguration)
                {
                    (config as CacheClusterConfiguration).StartCaching();
                }
                found++;
            }

            if (found == 0)
                // try to find them by scanning 
                this.ScanClusters();
        }

        /// <summary>
        /// Add a new menu item with a cluster name, if not already there.
        /// </summary>
        /// <param name="clusterName">Name to add to the cluster menu.</param>
        private void AddClusterNameToMenu(string clusterName)
        {
            int count = this.clusterToolStripMenuItem.DropDownItems.Count;
            for (int i = 0; i < count; i++)
            {
                var item = this.clusterToolStripMenuItem.DropDownItems[i];
                if (item.Text == clusterName) return;
            }

            ToolStripMenuItem newItem = new ToolStripMenuItem(clusterName);
            this.clusterToolStripMenuItem.DropDownItems.Add(newItem);

            if (clusterName == "<add>")
            {
                newItem.Click += this.AddNewCluster;
                return;
            }
            if (clusterName == "<scan>")
            {
                newItem.Click += this.ScanClusters;
                return;
            }

            var selItem = newItem.DropDownItems.Add("Select");
            var delItem = newItem.DropDownItems.Add("Delete");
            var editItem = newItem.DropDownItems.Add("Edit");
            delItem.Click += delItem_Click;
            selItem.Click += selItem_Click;
            editItem.Click += editItem_Click;
        }

        /// <summary>
        /// Scan the clusters we are subscribed to and add them to the list of known clusters.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void ScanClusters(object sender, EventArgs e)
        {
            this.ScanClusters();
        }

        /// <summary>
        /// Scan the clusters we are subscribed to and add them to the list of known clusters.
        /// </summary>
        private void ScanClusters()
        {
            this.Status("Scanning for known clusters", StatusKind.LongOp);
            foreach (var conf in ClusterConfiguration.EnumerateSubscribedClusters())
            {
                ClusterConfiguration.AddKnownCluster(conf);
                this.AddClusterNameToMenu(conf.Name);
                this.Status("Adding cluster " + conf.Name, StatusKind.OK);
            }
            this.Status("Scan completed", StatusKind.OK);
        }

        /// <summary>
        /// Edit a cluster.
        /// </summary>
        /// <param name="sender">MenuItem that is clicked.</param>
        /// <param name="e">Unused.</param>
        void editItem_Click(object sender, EventArgs e)
        {
            ToolStripItem item = sender as ToolStripItem;
            if (item == null) return;

            ToolStripItem strip = item.OwnerItem;
            string clus = strip.Text;
            var conf = this.EditCluster(clus);
            this.ConfigurationChanged(conf);
        }

        void selItem_Click(object sender, EventArgs e)
        {
            ToolStripItem item = sender as ToolStripItem;
            if (item == null) return;

            ToolStripItem strip = item.OwnerItem;
            string clus = strip.Text;
            this.ClusterSelected(clus);
        }

        private void ConfigurationChanged(ClusterConfiguration conf)
        {
            if (conf == null) return;

            // you cannot have two cache clusters at the same time
            if (conf is CacheClusterConfiguration)
            {
                foreach (var name in ClusterConfiguration.GetKnownClusterNames())
                {
                    var config = ClusterConfiguration.KnownClusterByName(name);
                    if (config is CacheClusterConfiguration)
                    {
                        DialogResult res = MessageBox.Show("You cannot have two cache clusters at once: " + conf.Name + " and " + config.Name + "\nPress OK to use " + conf.Name + " instead of " + config.Name);
                        if (res == System.Windows.Forms.DialogResult.OK)
                        {
                            ClusterConfiguration.RemoveKnownCluster(config.Name);
                            (config as CacheClusterConfiguration).StopCaching();
                            (conf as CacheClusterConfiguration).StartCaching();
                        }
                        else
                        {
                            return;
                        }
                    }
                }
            }

            ClusterConfiguration.AddKnownCluster(conf);
            this.AddClusterNameToMenu(conf.Name);
            this.Status("Added cluster " + conf.Name, StatusKind.OK);
        }

        void AddNewCluster(object sender, EventArgs e)
        {
            ClusterConfiguration conf = this.EditCluster(null);
            this.ConfigurationChanged(conf);
        }

        /// <summary>
        /// Edit the information about the specified cluster.  If null create a new cluster.
        /// </summary>
        /// <param name="clusterName">Cluster that is being edited.</param>
        /// <returns>The name of the edited cluster, or null if the operation is cancelled.</returns>
        private ClusterConfiguration EditCluster(string clusterName)
        {
            ClusterConfigEditor editor = new ClusterConfigEditor();

            try
            {
                if (clusterName != null)
                {
                    var config = ClusterConfiguration.KnownClusterByName(clusterName);
                    editor.SetConfigToEdit(config);
                }
                else
                {
                    editor.SetConfigToEdit(null);
                }
            }
            catch (Exception)
            {
                // This can happen when the cluster serialization has changed
                // and we can no longer read the saved properties
                editor.SetConfigToEdit(null);
            }
            DialogResult res = editor.ShowDialog();
            if (res == System.Windows.Forms.DialogResult.OK)
            {
                var config = editor.GetConfiguration();
                return config;
            }

            return null;
        }

        /// <summary>
        /// Delete menu item clicked.
        /// </summary>
        /// <param name="sender">Menu item clicked.</param>
        /// <param name="e">Unused.</param>
        void delItem_Click(object sender, EventArgs e)
        {
            ToolStripItem item = sender as ToolStripItem;
            if (item == null) return;

            ToolStripItem strip = item.OwnerItem;
            string clus = strip.Text;
            this.RemoveClusterName(clus);
        }

        /// <summary>
        /// Remove a menu item from the cluster menu.
        /// </summary>
        /// <param name="clusterName">Name of cluster to remove.</param>
        private void RemoveClusterName(string clusterName)
        {
            var config = ClusterConfiguration.RemoveKnownCluster(clusterName);
            if (config is CacheClusterConfiguration)
            {
                (config as CacheClusterConfiguration).StopCaching();
            }

            for (int i = 0; i < this.clusterToolStripMenuItem.DropDownItems.Count; i++)
            {
                var item = this.clusterToolStripMenuItem.DropDownItems[i];
                if (item.Text == clusterName)
                {
                    this.clusterToolStripMenuItem.DropDownItems.RemoveAt(i);
                    return;
                }
            }
            //throw new ArgumentException("Menu does not contain cluster " + clusterName);
        }

        /// <summary>
        /// Diagnose the failures of the selected jobs.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void diagnoseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            IEnumerable<ClusterJobInformation> todo = this.SelectedJobs();
            IEnumerable<DryadLinqJobSummary> jobs = todo.Select(j => j.DiscoverDryadLinqJob(this.clusterStatus, this.Status)).Where(j => j != null);
            
            var item = new BackgroundWorkItem<List<DiagnosisLog>>(
                m => ClusterWork.DiagnoseJobs(jobs, this.clusterStatus.Config, m),
                DiagnosisResult.ShowDiagnosisResult,
                "cancel");
            this.Queue(item);
        }

        private void filteredDataGridView_CellFormatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            ClusterJobInformation t = (ClusterJobInformation)this.filteredDataGridView.DataGridView.Rows[e.RowIndex].DataBoundItem;
            switch (t.Status)
            {
                case ClusterJobInformation.ClusterJobStatus.Cancelled:
                    e.CellStyle.BackColor = Color.Yellow;
                    break;
                case ClusterJobInformation.ClusterJobStatus.Succeeded:
                    e.CellStyle.BackColor = Color.LightGreen;
                    break;
                case ClusterJobInformation.ClusterJobStatus.Failed:
                    e.CellStyle.BackColor = Color.Tomato;
                    break;
                case ClusterJobInformation.ClusterJobStatus.Unknown:
                    e.CellStyle.BackColor = Color.White;
                    break;
                case ClusterJobInformation.ClusterJobStatus.Running:
                    e.CellStyle.BackColor = Color.Cyan;
                    break;
            }
            if (t.IsUnavailable)
                e.CellStyle.BackColor = Color.Gray;
        }

        /// <summary>
        /// Open a new window of the cluster browser.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void newWindowToolStripMenuItem_Click(object sender, EventArgs e)
        {
            ClusterBrowser browser = new ClusterBrowser();
            browser.Show();
        }

        /// <summary>
        /// User changed the log file.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void logFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            FileDialog file = new OpenFileDialog();
            file.CheckFileExists = false;

            DialogResult res = file.ShowDialog();
            if (res != DialogResult.OK)
                this.SetLogFile(this.LogFile != null ? this.LogFile.Name : "", false);
            else
                this.SetLogFile(file.FileName, true);
        }

        /// <summary>
        /// Change the log file and the status of logging.
        /// </summary>
        /// <param name="filename">File to log info to.</param>
        /// <param name="enabled">If true logging is enabled.</param>
        private void SetLogFile(string filename, bool enabled)
        {
            if (string.IsNullOrEmpty(filename) && enabled)
            {
                enabled = false;
            }

            // remove the current listener
            if (this.LogFile != null && Trace.Listeners.Contains(this.LogFile))
            {
                this.LogFile.Flush();
                Trace.Listeners.Remove(this.LogFile);
            }

            if (enabled)
            {
                this.LogFile = new TextWriterTraceListener(filename);
                Trace.Listeners.Add(this.LogFile);
            }

            this.logFileToolStripMenuItem.Checked = enabled;
        }

        /// <summary>
        /// The user has selected a virtual cluster.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void comboBox_virtualCluster_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.RefreshClusterJobList();
        }

        /// <summary>
        /// User pressed enter in virtual cluster combo box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Key description.</param>
        private void comboBox_virtualCluster_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == '\r')
            {
                this.RefreshClusterJobList();
            }
        }

        /// <summary>
        /// Change auto-refresh status.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void autoRefreshToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.autoRefreshToolStripMenuItem.Checked = !this.autoRefreshToolStripMenuItem.Checked;
            if (this.autoRefreshToolStripMenuItem.Checked)
                this.refreshTimer.Start();
            else
                this.refreshTimer.Stop();

        }
    }

    /// <summary>
    /// Persistent settings for the job browser.
    /// </summary>
    public class ClusterBrowserSettings : ApplicationSettingsBase
    {
        /// <summary>
        /// Form location on screen.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("0,0")]
        public Point Location
        {
            get { return (Point)this["Location"]; }
            set { this["Location"] = value; }
        }
        /// <summary>
        /// Form size.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("1186, 620")]
        public System.Drawing.Size Size
        {
            get { return (System.Drawing.Size)this["Size"]; }
            set { this["Size"] = value; }
        }
        /// <summary>
        /// Size of window.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("false")]
        public bool MaximizeWindow
        {
            get { return (bool)this["MaximizeWindow"]; }
            set { this["MaximizeWindow"] = value; }
        }

        /// <summary>
        /// Does the window auto-refresh.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("false")]
        public bool AutoRefresh
        {
            get { return (bool)this["AutoRefresh"]; }
            set { this["AutoRefresh"] = value; }
        }

        /// <summary>
        /// Does the window auto-refresh.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("true")]
        public bool ReducedFunctionality
        {
            get { return (bool)this["ReducedFunctionality"]; }
            set { this["ReducedFunctionality"] = value; }
        }

        /// <summary>
        /// List of known clusters.
        /// </summary>
        [UserScopedSetting]
        public List<ClusterConfigurationSerialization> KnownClusters
        {
            get { 
                var kc = this["KnownClusters"];
                if (kc != null)
                    return (List<ClusterConfigurationSerialization>)kc;
                else
                    return null;
            }
            set { this["KnownClusters"] = value; }
        }
    }

    /// <summary>
    /// This class contains a set of static methods which are invoked in the background to perform actions on the cluster.
    /// </summary>
    internal static class ClusterWork
    {
        /// <summary>
        /// Cancel a job.
        /// </summary>
        /// <param name="jobs">Jobs to cancel.</param>
        /// <param name="cluster">Cluster where the jobs are running.</param>
        /// <returns>True if all cancellations succeed.</returns>
        /// <param name="manager">Communicatoni manager.</param>
        // ReSharper disable once UnusedParameter.Global
        public static bool CancelJobs(IEnumerable<DryadLinqJobSummary> jobs, ClusterStatus cluster, CommManager manager)
        {
            bool done = true;
            foreach (DryadLinqJobSummary job in jobs)
            {
                manager.Token.ThrowIfCancellationRequested();
                if (job.Status != ClusterJobInformation.ClusterJobStatus.Running)
                {
                    manager.Status("Job " + job.Name + " does not appear to be running; will still try to cancel", StatusKind.Error);
                }

                bool success;
                string reason = "";
                try
                {
                    success = cluster.CancelJob(job);
                }
                catch (Exception ex)
                {
                    success = false;
                    reason = ex.Message;
                    Trace.TraceInformation(ex.ToString());
                }

                if (success)
                    manager.Status("Job " + job.Name + " cancelled", StatusKind.OK);
                else
                    manager.Status("Cancellation of " + job.Name + " failed " + reason, StatusKind.Error);
                done &= success;
            }
            return done;
        }

        /// <summary>
        /// Diagnose a list of jobs.
        /// </summary>
        /// <param name="jobs">Jobs to diagnose.</param>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="manager">Communicatino manager.</param>
        public static List<DiagnosisLog> DiagnoseJobs(IEnumerable<DryadLinqJobSummary> jobs, ClusterConfiguration config, CommManager manager)
        {
            var dryadLinqJobSummaries = jobs as DryadLinqJobSummary[] ?? jobs.ToArray();
            int jobCount = dryadLinqJobSummaries.Count();

            List<DiagnosisLog> result = new List<DiagnosisLog>();
            int done = 0;
            foreach (DryadLinqJobSummary summary in dryadLinqJobSummaries)
            {
                if (summary == null) continue;

                manager.Token.ThrowIfCancellationRequested(); 
                JobFailureDiagnosis diagnosis = JobFailureDiagnosis.CreateJobFailureDiagnosis(config, summary, manager);
                manager.Status("Diagnosing " + summary.ShortName(), StatusKind.LongOp);
                DiagnosisLog log = diagnosis.Diagnose();
                result.Add(log);

                done++;
                manager.Progress(done * 100 / jobCount);
            }
            manager.Status("Diagnosis complete", StatusKind.OK);
            return result;
        }
    }
}
