
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
namespace Microsoft.Research.DryadAnalysis
{
    partial class JobBrowser
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle1 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle2 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle3 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle4 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle5 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle6 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle7 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle8 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle9 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle10 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle11 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle12 = new System.Windows.Forms.DataGridViewCellStyle();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(JobBrowser));
            this.menu = new System.Windows.Forms.MenuStrip();
            this.jobToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.refreshToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.hideCancelledVerticesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exportToCSVToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem_terminate = new System.Windows.Forms.ToolStripMenuItem();
            this.packageCachedFilesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.diagnoseToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.closeToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cancelCurrentWorkToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.stageToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cacheLogsForAllVerticesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.vertexToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.debugLocallyManagedToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.debugLocallyUnmanagedToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.profileLocallyCPUSamplingToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.profileLocallyMemorySamplingToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.jMStdoutMentionsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.diagnoseToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.viewToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.loadFileInEditorToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.loadFileInLogViewerToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cacheAllLogsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.wordWrapToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.colorByStagestatusToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.zoomInToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.zoomOutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.zoomToFitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.rightToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.leftToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.upToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.downToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.splitContainer_jobAndRest = new System.Windows.Forms.SplitContainer();
            this.splitContainer_jobData = new System.Windows.Forms.SplitContainer();
            this.dataGridView_jobHeader = new System.Windows.Forms.DataGridView();
            this.flowLayoutPanel3 = new System.Windows.Forms.FlowLayoutPanel();
            this.label_job = new System.Windows.Forms.Label();
            this.panel_scheduleContainer = new System.Windows.Forms.Panel();
            this.panel_jobSchedule = new System.Windows.Forms.Panel();
            this.flowLayoutPanel2 = new System.Windows.Forms.FlowLayoutPanel();
            this.label_plan = new System.Windows.Forms.Label();
            this.comboBox_plan = new System.Windows.Forms.ComboBox();
            this.checkBox_refresh = new System.Windows.Forms.CheckBox();
            this.splitContainer_stageAndVertex = new System.Windows.Forms.SplitContainer();
            this.splitContainer_stageData = new System.Windows.Forms.SplitContainer();
            this.dataGridView_stageHeader = new System.Windows.Forms.DataGridView();
            this.flowLayoutPanel4 = new System.Windows.Forms.FlowLayoutPanel();
            this.label_stage = new System.Windows.Forms.Label();
            this.textBox_stageFilter = new System.Windows.Forms.TextBox();
            this.button_stageFilter = new System.Windows.Forms.Button();
            this.button_clearStageFilter = new System.Windows.Forms.Button();
            this.splitContainer_stageData1 = new System.Windows.Forms.SplitContainer();
            this.textBox_stageCode = new System.Windows.Forms.TextBox();
            this.dataGridView_stageContents = new System.Windows.Forms.DataGridView();
            this.contextMenu_stageVertex = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.menuItem_stageVertexLocalDebugManaged = new System.Windows.Forms.ToolStripMenuItem();
            this.menuItem_stageVertexLocalDebugUnmanaged = new System.Windows.Forms.ToolStripMenuItem();
            this.menuItem_stageVertexProfileLocallyCPUSampling = new System.Windows.Forms.ToolStripMenuItem();
            this.menuItem_stageVertexProfileLocallyMemorySampling = new System.Windows.Forms.ToolStripMenuItem();
            this.jMStdoutLinesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.diagnoseToolStripMenuItem2 = new System.Windows.Forms.ToolStripMenuItem();
            this.splitContainer_vertexData = new System.Windows.Forms.SplitContainer();
            this.dataGridView_vertexHeader = new System.Windows.Forms.DataGridView();
            this.flowLayoutPanel1 = new System.Windows.Forms.FlowLayoutPanel();
            this.label_Vertex = new System.Windows.Forms.Label();
            this.label_comboVertex = new System.Windows.Forms.Label();
            this.comboBox_vertexInformation = new System.Windows.Forms.ComboBox();
            this.richTextBox_file = new System.Windows.Forms.RichTextBox();
            this.label_title = new System.Windows.Forms.Label();
            this.flowLayoutPanel5 = new System.Windows.Forms.FlowLayoutPanel();
            this.label1 = new System.Windows.Forms.Label();
            this.textBox_find = new System.Windows.Forms.TextBox();
            this.button_findPrev = new System.Windows.Forms.Button();
            this.button_findNext = new System.Windows.Forms.Button();
            this.button_filter = new System.Windows.Forms.Button();
            this.button_clearFind = new System.Windows.Forms.Button();
            this.label_matches = new System.Windows.Forms.Label();
            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.toolStripStatusLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStripStatusLabel_currentWork = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStripStatusLabel_backgroundWork = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStripProgressBar = new System.Windows.Forms.ToolStripProgressBar();
            this.menu.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_jobAndRest)).BeginInit();
            this.splitContainer_jobAndRest.Panel1.SuspendLayout();
            this.splitContainer_jobAndRest.Panel2.SuspendLayout();
            this.splitContainer_jobAndRest.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_jobData)).BeginInit();
            this.splitContainer_jobData.Panel1.SuspendLayout();
            this.splitContainer_jobData.Panel2.SuspendLayout();
            this.splitContainer_jobData.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_jobHeader)).BeginInit();
            this.flowLayoutPanel3.SuspendLayout();
            this.panel_scheduleContainer.SuspendLayout();
            this.flowLayoutPanel2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageAndVertex)).BeginInit();
            this.splitContainer_stageAndVertex.Panel1.SuspendLayout();
            this.splitContainer_stageAndVertex.Panel2.SuspendLayout();
            this.splitContainer_stageAndVertex.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageData)).BeginInit();
            this.splitContainer_stageData.Panel1.SuspendLayout();
            this.splitContainer_stageData.Panel2.SuspendLayout();
            this.splitContainer_stageData.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_stageHeader)).BeginInit();
            this.flowLayoutPanel4.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageData1)).BeginInit();
            this.splitContainer_stageData1.Panel1.SuspendLayout();
            this.splitContainer_stageData1.Panel2.SuspendLayout();
            this.splitContainer_stageData1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_stageContents)).BeginInit();
            this.contextMenu_stageVertex.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_vertexData)).BeginInit();
            this.splitContainer_vertexData.Panel1.SuspendLayout();
            this.splitContainer_vertexData.Panel2.SuspendLayout();
            this.splitContainer_vertexData.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_vertexHeader)).BeginInit();
            this.flowLayoutPanel1.SuspendLayout();
            this.flowLayoutPanel5.SuspendLayout();
            this.statusStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // menu
            // 
            this.menu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.jobToolStripMenuItem,
            this.stageToolStripMenuItem,
            this.vertexToolStripMenuItem,
            this.viewToolStripMenuItem});
            this.menu.Location = new System.Drawing.Point(0, 0);
            this.menu.Name = "menu";
            this.menu.Size = new System.Drawing.Size(1262, 24);
            this.menu.TabIndex = 0;
            this.menu.Text = "menuStrip1";
            // 
            // jobToolStripMenuItem
            // 
            this.jobToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.refreshToolStripMenuItem,
            this.hideCancelledVerticesToolStripMenuItem,
            this.exportToCSVToolStripMenuItem,
            this.toolStripMenuItem_terminate,
            this.packageCachedFilesToolStripMenuItem,
            this.diagnoseToolStripMenuItem,
            this.closeToolStripMenuItem,
            this.cancelCurrentWorkToolStripMenuItem});
            this.jobToolStripMenuItem.Name = "jobToolStripMenuItem";
            this.jobToolStripMenuItem.Size = new System.Drawing.Size(37, 20);
            this.jobToolStripMenuItem.Text = "Job";
            // 
            // refreshToolStripMenuItem
            // 
            this.refreshToolStripMenuItem.Name = "refreshToolStripMenuItem";
            this.refreshToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F5;
            this.refreshToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.refreshToolStripMenuItem.Text = "Refresh";
            this.refreshToolStripMenuItem.ToolTipText = "Reloads the job information from the cluster";
            this.refreshToolStripMenuItem.Click += new System.EventHandler(this.refreshToolStripMenuItem_Click);
            // 
            // hideCancelledVerticesToolStripMenuItem
            // 
            this.hideCancelledVerticesToolStripMenuItem.Checked = true;
            this.hideCancelledVerticesToolStripMenuItem.CheckState = System.Windows.Forms.CheckState.Checked;
            this.hideCancelledVerticesToolStripMenuItem.Name = "hideCancelledVerticesToolStripMenuItem";
            this.hideCancelledVerticesToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.hideCancelledVerticesToolStripMenuItem.Text = "Hide cancelled vertices";
            this.hideCancelledVerticesToolStripMenuItem.ToolTipText = "If checked cancelled vertices are not displayed.";
            this.hideCancelledVerticesToolStripMenuItem.Click += new System.EventHandler(this.hideCancelledVerticesToolStripMenuItem_Click);
            // 
            // exportToCSVToolStripMenuItem
            // 
            this.exportToCSVToolStripMenuItem.Name = "exportToCSVToolStripMenuItem";
            this.exportToCSVToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.exportToCSVToolStripMenuItem.Text = "Export to CSV...";
            this.exportToCSVToolStripMenuItem.ToolTipText = "Save information about the job vertices to a comma-separated value file.";
            this.exportToCSVToolStripMenuItem.Click += new System.EventHandler(this.exportToCSVToolStripMenuItem_Click);
            // 
            // toolStripMenuItem_terminate
            // 
            this.toolStripMenuItem_terminate.Name = "toolStripMenuItem_terminate";
            this.toolStripMenuItem_terminate.Size = new System.Drawing.Size(195, 22);
            this.toolStripMenuItem_terminate.Text = "Terminate job";
            this.toolStripMenuItem_terminate.ToolTipText = "Requests the cluster to terminate the job execution.";
            this.toolStripMenuItem_terminate.Click += new System.EventHandler(this.toolStripMenuItem_terminate_Click);
            // 
            // packageCachedFilesToolStripMenuItem
            // 
            this.packageCachedFilesToolStripMenuItem.Name = "packageCachedFilesToolStripMenuItem";
            this.packageCachedFilesToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.packageCachedFilesToolStripMenuItem.Text = "Package cached files...";
            this.packageCachedFilesToolStripMenuItem.ToolTipText = "Create a zip file containing all cached files pertaining to the current job.";
            this.packageCachedFilesToolStripMenuItem.Visible = false;
            this.packageCachedFilesToolStripMenuItem.Click += new System.EventHandler(this.packageCachedFilesToolStripMenuItem_Click);
            // 
            // diagnoseToolStripMenuItem
            // 
            this.diagnoseToolStripMenuItem.Name = "diagnoseToolStripMenuItem";
            this.diagnoseToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.D)));
            this.diagnoseToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.diagnoseToolStripMenuItem.Text = "Diagnose";
            this.diagnoseToolStripMenuItem.ToolTipText = "Attempts to provide an explanation for the failures in this job.";
            this.diagnoseToolStripMenuItem.Visible = false;
            this.diagnoseToolStripMenuItem.Click += new System.EventHandler(this.diagnoseToolStripMenuItem_Click);
            // 
            // closeToolStripMenuItem
            // 
            this.closeToolStripMenuItem.Name = "closeToolStripMenuItem";
            this.closeToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Q)));
            this.closeToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.closeToolStripMenuItem.Text = "Close";
            this.closeToolStripMenuItem.ToolTipText = "Save the settings and close the window.";
            this.closeToolStripMenuItem.Click += new System.EventHandler(this.closeToolStripMenuItem_Click);
            // 
            // cancelCurrentWorkToolStripMenuItem
            // 
            this.cancelCurrentWorkToolStripMenuItem.Name = "cancelCurrentWorkToolStripMenuItem";
            this.cancelCurrentWorkToolStripMenuItem.Size = new System.Drawing.Size(195, 22);
            this.cancelCurrentWorkToolStripMenuItem.Text = "Cancel current work";
            this.cancelCurrentWorkToolStripMenuItem.Click += new System.EventHandler(this.cancelCurrentWorkToolStripMenuItem_Click);
            // 
            // stageToolStripMenuItem
            // 
            this.stageToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.cacheLogsForAllVerticesToolStripMenuItem});
            this.stageToolStripMenuItem.Name = "stageToolStripMenuItem";
            this.stageToolStripMenuItem.Size = new System.Drawing.Size(48, 20);
            this.stageToolStripMenuItem.Text = "Stage";
            this.stageToolStripMenuItem.Visible = false;
            // 
            // cacheLogsForAllVerticesToolStripMenuItem
            // 
            this.cacheLogsForAllVerticesToolStripMenuItem.Name = "cacheLogsForAllVerticesToolStripMenuItem";
            this.cacheLogsForAllVerticesToolStripMenuItem.Size = new System.Drawing.Size(147, 22);
            this.cacheLogsForAllVerticesToolStripMenuItem.Text = "Cache all logs";
            this.cacheLogsForAllVerticesToolStripMenuItem.ToolTipText = "Add to the local file cache the logs for all the currently displayed vertices.";
            this.cacheLogsForAllVerticesToolStripMenuItem.Click += new System.EventHandler(this.cacheLogsForAllVerticesToolStripMenuItem_Click);
            // 
            // vertexToolStripMenuItem
            // 
            this.vertexToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.debugLocallyManagedToolStripMenuItem,
            this.debugLocallyUnmanagedToolStripMenuItem,
            this.profileLocallyCPUSamplingToolStripMenuItem,
            this.profileLocallyMemorySamplingToolStripMenuItem,
            this.jMStdoutMentionsToolStripMenuItem,
            this.diagnoseToolStripMenuItem1});
            this.vertexToolStripMenuItem.Enabled = false;
            this.vertexToolStripMenuItem.Name = "vertexToolStripMenuItem";
            this.vertexToolStripMenuItem.Size = new System.Drawing.Size(51, 20);
            this.vertexToolStripMenuItem.Text = "Vertex";
            this.vertexToolStripMenuItem.Visible = false;
            // 
            // debugLocallyManagedToolStripMenuItem
            // 
            this.debugLocallyManagedToolStripMenuItem.Name = "debugLocallyManagedToolStripMenuItem";
            this.debugLocallyManagedToolStripMenuItem.Size = new System.Drawing.Size(253, 22);
            this.debugLocallyManagedToolStripMenuItem.Text = "Debug locally (managed)";
            this.debugLocallyManagedToolStripMenuItem.ToolTipText = "Start an execution of the current vertex on the local workstation in a debugger.";
            this.debugLocallyManagedToolStripMenuItem.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // debugLocallyUnmanagedToolStripMenuItem
            // 
            this.debugLocallyUnmanagedToolStripMenuItem.Name = "debugLocallyUnmanagedToolStripMenuItem";
            this.debugLocallyUnmanagedToolStripMenuItem.Size = new System.Drawing.Size(253, 22);
            this.debugLocallyUnmanagedToolStripMenuItem.Text = "Debug locally (unmanaged)";
            this.debugLocallyUnmanagedToolStripMenuItem.ToolTipText = "Start an execution of the current vertex on the local workstation in a debugger (" +
    "in the unmanaged code).";
            this.debugLocallyUnmanagedToolStripMenuItem.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // profileLocallyCPUSamplingToolStripMenuItem
            // 
            this.profileLocallyCPUSamplingToolStripMenuItem.Name = "profileLocallyCPUSamplingToolStripMenuItem";
            this.profileLocallyCPUSamplingToolStripMenuItem.Size = new System.Drawing.Size(253, 22);
            this.profileLocallyCPUSamplingToolStripMenuItem.Text = "Profile locally (cpu sampling)";
            this.profileLocallyCPUSamplingToolStripMenuItem.ToolTipText = "Run the vertex on the local workstation for CPU profiling.";
            this.profileLocallyCPUSamplingToolStripMenuItem.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // profileLocallyMemorySamplingToolStripMenuItem
            // 
            this.profileLocallyMemorySamplingToolStripMenuItem.Name = "profileLocallyMemorySamplingToolStripMenuItem";
            this.profileLocallyMemorySamplingToolStripMenuItem.Size = new System.Drawing.Size(253, 22);
            this.profileLocallyMemorySamplingToolStripMenuItem.Text = "Profile locally (memory sampling)";
            this.profileLocallyMemorySamplingToolStripMenuItem.ToolTipText = "Run the vertex on the local workstation for memory profiling.";
            this.profileLocallyMemorySamplingToolStripMenuItem.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // jMStdoutMentionsToolStripMenuItem
            // 
            this.jMStdoutMentionsToolStripMenuItem.Name = "jMStdoutMentionsToolStripMenuItem";
            this.jMStdoutMentionsToolStripMenuItem.Size = new System.Drawing.Size(253, 22);
            this.jMStdoutMentionsToolStripMenuItem.Text = "Show JM stdout mentions";
            this.jMStdoutMentionsToolStripMenuItem.ToolTipText = "Find all lines in the Job Manager standard output pertaining to this vertex.";
            this.jMStdoutMentionsToolStripMenuItem.Click += new System.EventHandler(this.jMStdoutMentionsToolStripMenuItem_Click);
            // 
            // diagnoseToolStripMenuItem1
            // 
            this.diagnoseToolStripMenuItem1.Name = "diagnoseToolStripMenuItem1";
            this.diagnoseToolStripMenuItem1.Size = new System.Drawing.Size(253, 22);
            this.diagnoseToolStripMenuItem1.Text = "Diagnose";
            this.diagnoseToolStripMenuItem1.ToolTipText = "Attempt to explain the failure of this vertex.";
            this.diagnoseToolStripMenuItem1.Click += new System.EventHandler(this.diagnoseToolStripMenuItem1_Click);
            // 
            // viewToolStripMenuItem
            // 
            this.viewToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.loadFileInEditorToolStripMenuItem,
            this.loadFileInLogViewerToolStripMenuItem,
            this.cacheAllLogsToolStripMenuItem,
            this.wordWrapToolStripMenuItem,
            this.colorByStagestatusToolStripMenuItem,
            this.toolStripSeparator1,
            this.zoomInToolStripMenuItem,
            this.zoomOutToolStripMenuItem,
            this.zoomToFitToolStripMenuItem,
            this.rightToolStripMenuItem,
            this.leftToolStripMenuItem,
            this.upToolStripMenuItem,
            this.downToolStripMenuItem});
            this.viewToolStripMenuItem.Name = "viewToolStripMenuItem";
            this.viewToolStripMenuItem.ShortcutKeyDisplayString = "";
            this.viewToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.viewToolStripMenuItem.Text = "View";
            // 
            // loadFileInEditorToolStripMenuItem
            // 
            this.loadFileInEditorToolStripMenuItem.Name = "loadFileInEditorToolStripMenuItem";
            this.loadFileInEditorToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.loadFileInEditorToolStripMenuItem.Text = "Load displayed data in editor";
            this.loadFileInEditorToolStripMenuItem.ToolTipText = "Display the information in the vertex pane in a text editor.";
            this.loadFileInEditorToolStripMenuItem.Click += new System.EventHandler(this.loadFileInEditorToolStripMenuItem_Click);
            // 
            // loadFileInLogViewerToolStripMenuItem
            // 
            this.loadFileInLogViewerToolStripMenuItem.Name = "loadFileInLogViewerToolStripMenuItem";
            this.loadFileInLogViewerToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.loadFileInLogViewerToolStripMenuItem.Text = "Load displayed file in log viewer";
            this.loadFileInLogViewerToolStripMenuItem.ToolTipText = "Display the information in the vertex pane in the log viewer.";
            this.loadFileInLogViewerToolStripMenuItem.Visible = false;
            this.loadFileInLogViewerToolStripMenuItem.Click += new System.EventHandler(this.loadFileInViewerToolStripMenuItem_Click);
            // 
            // cacheAllLogsToolStripMenuItem
            // 
            this.cacheAllLogsToolStripMenuItem.Enabled = false;
            this.cacheAllLogsToolStripMenuItem.Name = "cacheAllLogsToolStripMenuItem";
            this.cacheAllLogsToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.cacheAllLogsToolStripMenuItem.Text = "Cache all text files";
            this.cacheAllLogsToolStripMenuItem.ToolTipText = "Add all the text files from the currently displayed directory to the cache (to be" +
    " used for offline debugging).";
            this.cacheAllLogsToolStripMenuItem.Click += new System.EventHandler(this.cacheAllLogsToolStripMenuItem_Click);
            // 
            // wordWrapToolStripMenuItem
            // 
            this.wordWrapToolStripMenuItem.Name = "wordWrapToolStripMenuItem";
            this.wordWrapToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.wordWrapToolStripMenuItem.Text = "Word wrap";
            this.wordWrapToolStripMenuItem.ToolTipText = "Toggle word wrapping in the vertex display window pane.";
            this.wordWrapToolStripMenuItem.Click += new System.EventHandler(this.wordWrapToolStripMenuItem_Click);
            // 
            // colorByStagestatusToolStripMenuItem
            // 
            this.colorByStagestatusToolStripMenuItem.Checked = true;
            this.colorByStagestatusToolStripMenuItem.CheckState = System.Windows.Forms.CheckState.Checked;
            this.colorByStagestatusToolStripMenuItem.Name = "colorByStagestatusToolStripMenuItem";
            this.colorByStagestatusToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.T)));
            this.colorByStagestatusToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.colorByStagestatusToolStripMenuItem.Text = "Color by stage (status)";
            this.colorByStagestatusToolStripMenuItem.ToolTipText = "Assign colors to vertices either by their status or by the stage they belong to.";
            this.colorByStagestatusToolStripMenuItem.Click += new System.EventHandler(this.colorByStagestatusToolStripMenuItem_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(239, 6);
            // 
            // zoomInToolStripMenuItem
            // 
            this.zoomInToolStripMenuItem.Name = "zoomInToolStripMenuItem";
            this.zoomInToolStripMenuItem.ShortcutKeyDisplayString = "Ctrl +";
            this.zoomInToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Oemplus)));
            this.zoomInToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.zoomInToolStripMenuItem.Text = "Zoom in";
            this.zoomInToolStripMenuItem.ToolTipText = "Magnify the plan view.";
            this.zoomInToolStripMenuItem.Click += new System.EventHandler(this.zoomInToolStripMenuItem_Click);
            // 
            // zoomOutToolStripMenuItem
            // 
            this.zoomOutToolStripMenuItem.Name = "zoomOutToolStripMenuItem";
            this.zoomOutToolStripMenuItem.ShortcutKeyDisplayString = "Ctrl -";
            this.zoomOutToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.OemMinus)));
            this.zoomOutToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.zoomOutToolStripMenuItem.Text = "Zoom out";
            this.zoomOutToolStripMenuItem.ToolTipText = "Shrink the plan view.";
            this.zoomOutToolStripMenuItem.Click += new System.EventHandler(this.zoomOutToolStripMenuItem_Click);
            // 
            // zoomToFitToolStripMenuItem
            // 
            this.zoomToFitToolStripMenuItem.Name = "zoomToFitToolStripMenuItem";
            this.zoomToFitToolStripMenuItem.ShortcutKeyDisplayString = "Ctrl ~";
            this.zoomToFitToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Oemtilde)));
            this.zoomToFitToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.zoomToFitToolStripMenuItem.Text = "Zoom to fit";
            this.zoomToFitToolStripMenuItem.ToolTipText = "Fit the plan to the window.";
            this.zoomToFitToolStripMenuItem.Click += new System.EventHandler(this.zoomToFitToolStripMenuItem_Click);
            // 
            // rightToolStripMenuItem
            // 
            this.rightToolStripMenuItem.Name = "rightToolStripMenuItem";
            this.rightToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Right)));
            this.rightToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.rightToolStripMenuItem.Text = "Right";
            this.rightToolStripMenuItem.ToolTipText = "Pan right the plan view.";
            this.rightToolStripMenuItem.Click += new System.EventHandler(this.moveToolStripMenuItem_Click);
            // 
            // leftToolStripMenuItem
            // 
            this.leftToolStripMenuItem.Name = "leftToolStripMenuItem";
            this.leftToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Left)));
            this.leftToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.leftToolStripMenuItem.Text = "Left";
            this.leftToolStripMenuItem.ToolTipText = "Pan left the plan view.";
            this.leftToolStripMenuItem.Click += new System.EventHandler(this.moveToolStripMenuItem_Click);
            // 
            // upToolStripMenuItem
            // 
            this.upToolStripMenuItem.Name = "upToolStripMenuItem";
            this.upToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Up)));
            this.upToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.upToolStripMenuItem.Text = "Up";
            this.upToolStripMenuItem.ToolTipText = "Pan up the plan view.";
            this.upToolStripMenuItem.Click += new System.EventHandler(this.moveToolStripMenuItem_Click);
            // 
            // downToolStripMenuItem
            // 
            this.downToolStripMenuItem.Name = "downToolStripMenuItem";
            this.downToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Down)));
            this.downToolStripMenuItem.Size = new System.Drawing.Size(242, 22);
            this.downToolStripMenuItem.Text = "Down";
            this.downToolStripMenuItem.ToolTipText = "Pan down the plan view.";
            this.downToolStripMenuItem.Click += new System.EventHandler(this.moveToolStripMenuItem_Click);
            // 
            // splitContainer_jobAndRest
            // 
            this.splitContainer_jobAndRest.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_jobAndRest.Location = new System.Drawing.Point(0, 24);
            this.splitContainer_jobAndRest.Name = "splitContainer_jobAndRest";
            // 
            // splitContainer_jobAndRest.Panel1
            // 
            this.splitContainer_jobAndRest.Panel1.Controls.Add(this.splitContainer_jobData);
            // 
            // splitContainer_jobAndRest.Panel2
            // 
            this.splitContainer_jobAndRest.Panel2.Controls.Add(this.splitContainer_stageAndVertex);
            this.splitContainer_jobAndRest.Size = new System.Drawing.Size(1262, 545);
            this.splitContainer_jobAndRest.SplitterDistance = 357;
            this.splitContainer_jobAndRest.TabIndex = 1;
            // 
            // splitContainer_jobData
            // 
            this.splitContainer_jobData.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_jobData.Location = new System.Drawing.Point(0, 0);
            this.splitContainer_jobData.Name = "splitContainer_jobData";
            this.splitContainer_jobData.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer_jobData.Panel1
            // 
            this.splitContainer_jobData.Panel1.Controls.Add(this.dataGridView_jobHeader);
            this.splitContainer_jobData.Panel1.Controls.Add(this.flowLayoutPanel3);
            // 
            // splitContainer_jobData.Panel2
            // 
            this.splitContainer_jobData.Panel2.Controls.Add(this.panel_scheduleContainer);
            this.splitContainer_jobData.Panel2.Controls.Add(this.flowLayoutPanel2);
            this.splitContainer_jobData.Size = new System.Drawing.Size(357, 545);
            this.splitContainer_jobData.SplitterDistance = 128;
            this.splitContainer_jobData.TabIndex = 1;
            // 
            // dataGridView_jobHeader
            // 
            this.dataGridView_jobHeader.AllowUserToAddRows = false;
            this.dataGridView_jobHeader.AllowUserToDeleteRows = false;
            this.dataGridView_jobHeader.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
            dataGridViewCellStyle1.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle1.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle1.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle1.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle1.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle1.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_jobHeader.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle1;
            this.dataGridView_jobHeader.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridViewCellStyle2.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle2.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle2.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle2.ForeColor = System.Drawing.SystemColors.ControlText;
            dataGridViewCellStyle2.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle2.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle2.WrapMode = System.Windows.Forms.DataGridViewTriState.False;
            this.dataGridView_jobHeader.DefaultCellStyle = dataGridViewCellStyle2;
            this.dataGridView_jobHeader.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView_jobHeader.Location = new System.Drawing.Point(0, 32);
            this.dataGridView_jobHeader.Name = "dataGridView_jobHeader";
            this.dataGridView_jobHeader.ReadOnly = true;
            dataGridViewCellStyle3.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle3.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle3.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle3.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle3.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle3.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle3.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_jobHeader.RowHeadersDefaultCellStyle = dataGridViewCellStyle3;
            this.dataGridView_jobHeader.RowHeadersVisible = false;
            this.dataGridView_jobHeader.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.CellSelect;
            this.dataGridView_jobHeader.ShowEditingIcon = false;
            this.dataGridView_jobHeader.Size = new System.Drawing.Size(357, 96);
            this.dataGridView_jobHeader.TabIndex = 0;
            // 
            // flowLayoutPanel3
            // 
            this.flowLayoutPanel3.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.flowLayoutPanel3.Controls.Add(this.label_job);
            this.flowLayoutPanel3.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel3.Location = new System.Drawing.Point(0, 0);
            this.flowLayoutPanel3.Name = "flowLayoutPanel3";
            this.flowLayoutPanel3.Size = new System.Drawing.Size(357, 32);
            this.flowLayoutPanel3.TabIndex = 1;
            this.flowLayoutPanel3.WrapContents = false;
            // 
            // label_job
            // 
            this.label_job.AutoSize = true;
            this.label_job.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label_job.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_job.Location = new System.Drawing.Point(3, 3);
            this.label_job.Margin = new System.Windows.Forms.Padding(3);
            this.label_job.Name = "label_job";
            this.label_job.Padding = new System.Windows.Forms.Padding(3);
            this.label_job.Size = new System.Drawing.Size(40, 22);
            this.label_job.TabIndex = 0;
            this.label_job.Text = "Job";
            this.label_job.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // panel_scheduleContainer
            // 
            this.panel_scheduleContainer.Controls.Add(this.panel_jobSchedule);
            this.panel_scheduleContainer.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel_scheduleContainer.Location = new System.Drawing.Point(0, 32);
            this.panel_scheduleContainer.Name = "panel_scheduleContainer";
            this.panel_scheduleContainer.Size = new System.Drawing.Size(357, 381);
            this.panel_scheduleContainer.TabIndex = 2;
            // 
            // panel_jobSchedule
            // 
            this.panel_jobSchedule.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel_jobSchedule.Location = new System.Drawing.Point(0, 0);
            this.panel_jobSchedule.Name = "panel_jobSchedule";
            this.panel_jobSchedule.Size = new System.Drawing.Size(357, 381);
            this.panel_jobSchedule.TabIndex = 0;
            this.panel_jobSchedule.Paint += new System.Windows.Forms.PaintEventHandler(this.panel_jobSchedule_Paint);
            this.panel_jobSchedule.MouseDown += new System.Windows.Forms.MouseEventHandler(this.panel_jobSchedule_MouseDown);
            this.panel_jobSchedule.MouseMove += new System.Windows.Forms.MouseEventHandler(this.panel_jobSchedule_MouseMove);
            this.panel_jobSchedule.MouseUp += new System.Windows.Forms.MouseEventHandler(this.panel_jobSchedule_MouseUp);
            this.panel_jobSchedule.Resize += new System.EventHandler(this.panel_jobSchedule_Resize);
            // 
            // flowLayoutPanel2
            // 
            this.flowLayoutPanel2.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.flowLayoutPanel2.Controls.Add(this.label_plan);
            this.flowLayoutPanel2.Controls.Add(this.comboBox_plan);
            this.flowLayoutPanel2.Controls.Add(this.checkBox_refresh);
            this.flowLayoutPanel2.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel2.Location = new System.Drawing.Point(0, 0);
            this.flowLayoutPanel2.MinimumSize = new System.Drawing.Size(290, 32);
            this.flowLayoutPanel2.Name = "flowLayoutPanel2";
            this.flowLayoutPanel2.Size = new System.Drawing.Size(357, 32);
            this.flowLayoutPanel2.TabIndex = 1;
            // 
            // label_plan
            // 
            this.label_plan.AutoSize = true;
            this.label_plan.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_plan.Location = new System.Drawing.Point(0, 3);
            this.label_plan.Margin = new System.Windows.Forms.Padding(0, 3, 0, 3);
            this.label_plan.Name = "label_plan";
            this.label_plan.Padding = new System.Windows.Forms.Padding(3);
            this.label_plan.Size = new System.Drawing.Size(43, 22);
            this.label_plan.TabIndex = 6;
            this.label_plan.Text = "View";
            // 
            // comboBox_plan
            // 
            this.comboBox_plan.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.comboBox_plan.FormattingEnabled = true;
            this.comboBox_plan.Items.AddRange(new object[] {
            "Static",
            "Dynamic",
            "Schedule"});
            this.comboBox_plan.Location = new System.Drawing.Point(46, 3);
            this.comboBox_plan.Name = "comboBox_plan";
            this.comboBox_plan.Size = new System.Drawing.Size(121, 24);
            this.comboBox_plan.TabIndex = 5;
            this.comboBox_plan.Text = "Static";
            this.comboBox_plan.SelectedIndexChanged += new System.EventHandler(this.comboBox_plan_SelectedIndexChanged);
            // 
            // checkBox_refresh
            // 
            this.checkBox_refresh.AutoSize = true;
            this.checkBox_refresh.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.checkBox_refresh.Location = new System.Drawing.Point(180, 3);
            this.checkBox_refresh.Margin = new System.Windows.Forms.Padding(10, 3, 3, 3);
            this.checkBox_refresh.Name = "checkBox_refresh";
            this.checkBox_refresh.Padding = new System.Windows.Forms.Padding(3);
            this.checkBox_refresh.Size = new System.Drawing.Size(110, 26);
            this.checkBox_refresh.TabIndex = 4;
            this.checkBox_refresh.Text = "Auto Refresh";
            this.checkBox_refresh.UseVisualStyleBackColor = true;
            this.checkBox_refresh.CheckedChanged += new System.EventHandler(this.checkBox_refresh_CheckedChanged);
            // 
            // splitContainer_stageAndVertex
            // 
            this.splitContainer_stageAndVertex.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_stageAndVertex.Location = new System.Drawing.Point(0, 0);
            this.splitContainer_stageAndVertex.Name = "splitContainer_stageAndVertex";
            // 
            // splitContainer_stageAndVertex.Panel1
            // 
            this.splitContainer_stageAndVertex.Panel1.Controls.Add(this.splitContainer_stageData);
            this.splitContainer_stageAndVertex.Panel1MinSize = 120;
            // 
            // splitContainer_stageAndVertex.Panel2
            // 
            this.splitContainer_stageAndVertex.Panel2.Controls.Add(this.splitContainer_vertexData);
            this.splitContainer_stageAndVertex.Panel2MinSize = 120;
            this.splitContainer_stageAndVertex.Size = new System.Drawing.Size(901, 545);
            this.splitContainer_stageAndVertex.SplitterDistance = 409;
            this.splitContainer_stageAndVertex.TabIndex = 0;
            // 
            // splitContainer_stageData
            // 
            this.splitContainer_stageData.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_stageData.Location = new System.Drawing.Point(0, 0);
            this.splitContainer_stageData.Name = "splitContainer_stageData";
            this.splitContainer_stageData.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer_stageData.Panel1
            // 
            this.splitContainer_stageData.Panel1.Controls.Add(this.dataGridView_stageHeader);
            this.splitContainer_stageData.Panel1.Controls.Add(this.flowLayoutPanel4);
            // 
            // splitContainer_stageData.Panel2
            // 
            this.splitContainer_stageData.Panel2.Controls.Add(this.splitContainer_stageData1);
            this.splitContainer_stageData.Size = new System.Drawing.Size(409, 545);
            this.splitContainer_stageData.SplitterDistance = 128;
            this.splitContainer_stageData.TabIndex = 2;
            // 
            // dataGridView_stageHeader
            // 
            this.dataGridView_stageHeader.AllowUserToAddRows = false;
            this.dataGridView_stageHeader.AllowUserToDeleteRows = false;
            this.dataGridView_stageHeader.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
            dataGridViewCellStyle4.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle4.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle4.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle4.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle4.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle4.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle4.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_stageHeader.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle4;
            this.dataGridView_stageHeader.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridViewCellStyle5.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle5.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle5.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle5.ForeColor = System.Drawing.SystemColors.ControlText;
            dataGridViewCellStyle5.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle5.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle5.WrapMode = System.Windows.Forms.DataGridViewTriState.False;
            this.dataGridView_stageHeader.DefaultCellStyle = dataGridViewCellStyle5;
            this.dataGridView_stageHeader.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView_stageHeader.Location = new System.Drawing.Point(0, 32);
            this.dataGridView_stageHeader.Name = "dataGridView_stageHeader";
            this.dataGridView_stageHeader.ReadOnly = true;
            dataGridViewCellStyle6.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle6.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle6.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle6.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle6.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle6.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle6.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_stageHeader.RowHeadersDefaultCellStyle = dataGridViewCellStyle6;
            this.dataGridView_stageHeader.RowHeadersVisible = false;
            this.dataGridView_stageHeader.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridView_stageHeader.ShowEditingIcon = false;
            this.dataGridView_stageHeader.Size = new System.Drawing.Size(409, 96);
            this.dataGridView_stageHeader.TabIndex = 1;
            this.dataGridView_stageHeader.SelectionChanged += new System.EventHandler(this.dataGridView_stageHeader_SelectionChanged);
            this.dataGridView_stageHeader.Sorted += new System.EventHandler(this.dataGridView_stageContents_SelectionChanged);
            // 
            // flowLayoutPanel4
            // 
            this.flowLayoutPanel4.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.flowLayoutPanel4.Controls.Add(this.label_stage);
            this.flowLayoutPanel4.Controls.Add(this.textBox_stageFilter);
            this.flowLayoutPanel4.Controls.Add(this.button_stageFilter);
            this.flowLayoutPanel4.Controls.Add(this.button_clearStageFilter);
            this.flowLayoutPanel4.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel4.Location = new System.Drawing.Point(0, 0);
            this.flowLayoutPanel4.MinimumSize = new System.Drawing.Size(280, 32);
            this.flowLayoutPanel4.Name = "flowLayoutPanel4";
            this.flowLayoutPanel4.Size = new System.Drawing.Size(409, 32);
            this.flowLayoutPanel4.TabIndex = 2;
            this.flowLayoutPanel4.WrapContents = false;
            // 
            // label_stage
            // 
            this.label_stage.AutoSize = true;
            this.label_stage.Dock = System.Windows.Forms.DockStyle.Left;
            this.label_stage.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_stage.Location = new System.Drawing.Point(3, 3);
            this.label_stage.Margin = new System.Windows.Forms.Padding(3);
            this.label_stage.Name = "label_stage";
            this.label_stage.Padding = new System.Windows.Forms.Padding(3);
            this.label_stage.Size = new System.Drawing.Size(55, 22);
            this.label_stage.TabIndex = 1;
            this.label_stage.Text = "Stage";
            this.label_stage.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // textBox_stageFilter
            // 
            this.textBox_stageFilter.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBox_stageFilter.Enabled = false;
            this.textBox_stageFilter.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBox_stageFilter.Location = new System.Drawing.Point(64, 3);
            this.textBox_stageFilter.Name = "textBox_stageFilter";
            this.textBox_stageFilter.Size = new System.Drawing.Size(153, 22);
            this.textBox_stageFilter.TabIndex = 2;
            this.textBox_stageFilter.KeyDown += new System.Windows.Forms.KeyEventHandler(this.textBox_stageFilter_KeyDown);
            // 
            // button_stageFilter
            // 
            this.button_stageFilter.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_stageFilter.Enabled = false;
            this.button_stageFilter.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_stageFilter.Location = new System.Drawing.Point(221, 0);
            this.button_stageFilter.Margin = new System.Windows.Forms.Padding(1, 0, 1, 0);
            this.button_stageFilter.Name = "button_stageFilter";
            this.button_stageFilter.Size = new System.Drawing.Size(45, 28);
            this.button_stageFilter.TabIndex = 8;
            this.button_stageFilter.Text = "filter";
            this.button_stageFilter.UseVisualStyleBackColor = true;
            this.button_stageFilter.Click += new System.EventHandler(this.button_stageFilter_Click);
            // 
            // button_clearStageFilter
            // 
            this.button_clearStageFilter.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_clearStageFilter.Enabled = false;
            this.button_clearStageFilter.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_clearStageFilter.Location = new System.Drawing.Point(267, 0);
            this.button_clearStageFilter.Margin = new System.Windows.Forms.Padding(0);
            this.button_clearStageFilter.Name = "button_clearStageFilter";
            this.button_clearStageFilter.Size = new System.Drawing.Size(18, 28);
            this.button_clearStageFilter.TabIndex = 9;
            this.button_clearStageFilter.Text = "X";
            this.button_clearStageFilter.UseVisualStyleBackColor = true;
            this.button_clearStageFilter.Click += new System.EventHandler(this.button_clearStageFilter_Click);
            // 
            // splitContainer_stageData1
            // 
            this.splitContainer_stageData1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_stageData1.Location = new System.Drawing.Point(0, 0);
            this.splitContainer_stageData1.Name = "splitContainer_stageData1";
            this.splitContainer_stageData1.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer_stageData1.Panel1
            // 
            this.splitContainer_stageData1.Panel1.Controls.Add(this.textBox_stageCode);
            // 
            // splitContainer_stageData1.Panel2
            // 
            this.splitContainer_stageData1.Panel2.Controls.Add(this.dataGridView_stageContents);
            this.splitContainer_stageData1.Size = new System.Drawing.Size(409, 413);
            this.splitContainer_stageData1.SplitterDistance = 62;
            this.splitContainer_stageData1.TabIndex = 3;
            // 
            // textBox_stageCode
            // 
            this.textBox_stageCode.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBox_stageCode.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBox_stageCode.Location = new System.Drawing.Point(0, 0);
            this.textBox_stageCode.Multiline = true;
            this.textBox_stageCode.Name = "textBox_stageCode";
            this.textBox_stageCode.ReadOnly = true;
            this.textBox_stageCode.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.textBox_stageCode.Size = new System.Drawing.Size(409, 62);
            this.textBox_stageCode.TabIndex = 0;
            // 
            // dataGridView_stageContents
            // 
            this.dataGridView_stageContents.AllowUserToAddRows = false;
            this.dataGridView_stageContents.AllowUserToDeleteRows = false;
            this.dataGridView_stageContents.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
            dataGridViewCellStyle7.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle7.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle7.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle7.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle7.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle7.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle7.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_stageContents.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle7;
            this.dataGridView_stageContents.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridViewCellStyle8.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle8.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle8.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle8.ForeColor = System.Drawing.SystemColors.ControlText;
            dataGridViewCellStyle8.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle8.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle8.WrapMode = System.Windows.Forms.DataGridViewTriState.False;
            this.dataGridView_stageContents.DefaultCellStyle = dataGridViewCellStyle8;
            this.dataGridView_stageContents.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView_stageContents.Location = new System.Drawing.Point(0, 0);
            this.dataGridView_stageContents.MultiSelect = false;
            this.dataGridView_stageContents.Name = "dataGridView_stageContents";
            dataGridViewCellStyle9.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle9.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle9.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle9.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle9.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle9.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle9.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_stageContents.RowHeadersDefaultCellStyle = dataGridViewCellStyle9;
            this.dataGridView_stageContents.RowHeadersVisible = false;
            this.dataGridView_stageContents.RowTemplate.ContextMenuStrip = this.contextMenu_stageVertex;
            this.dataGridView_stageContents.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridView_stageContents.ShowEditingIcon = false;
            this.dataGridView_stageContents.Size = new System.Drawing.Size(409, 347);
            this.dataGridView_stageContents.TabIndex = 2;
            this.dataGridView_stageContents.CellFormatting += new System.Windows.Forms.DataGridViewCellFormattingEventHandler(this.dataGridView_stageContents_CellFormatting);
            this.dataGridView_stageContents.CellMouseDown += new System.Windows.Forms.DataGridViewCellMouseEventHandler(this.dataGridView_stageContents_CellMouseDown);
            this.dataGridView_stageContents.SelectionChanged += new System.EventHandler(this.dataGridView_stageContents_SelectionChanged);
            this.dataGridView_stageContents.Sorted += new System.EventHandler(this.dataGridView_stageContents_Sorted);
            // 
            // contextMenu_stageVertex
            // 
            this.contextMenu_stageVertex.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.menuItem_stageVertexLocalDebugManaged,
            this.menuItem_stageVertexLocalDebugUnmanaged,
            this.menuItem_stageVertexProfileLocallyCPUSampling,
            this.menuItem_stageVertexProfileLocallyMemorySampling,
            this.jMStdoutLinesToolStripMenuItem,
            this.diagnoseToolStripMenuItem2});
            this.contextMenu_stageVertex.Name = "vertexContextMenuStrip";
            this.contextMenu_stageVertex.Size = new System.Drawing.Size(256, 136);
            // 
            // menuItem_stageVertexLocalDebugManaged
            // 
            this.menuItem_stageVertexLocalDebugManaged.Name = "menuItem_stageVertexLocalDebugManaged";
            this.menuItem_stageVertexLocalDebugManaged.Size = new System.Drawing.Size(255, 22);
            this.menuItem_stageVertexLocalDebugManaged.Text = "Debug vertex locally (managed)";
            this.menuItem_stageVertexLocalDebugManaged.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // menuItem_stageVertexLocalDebugUnmanaged
            // 
            this.menuItem_stageVertexLocalDebugUnmanaged.Name = "menuItem_stageVertexLocalDebugUnmanaged";
            this.menuItem_stageVertexLocalDebugUnmanaged.Size = new System.Drawing.Size(255, 22);
            this.menuItem_stageVertexLocalDebugUnmanaged.Text = "Debug vertex locally (unmanaged)";
            this.menuItem_stageVertexLocalDebugUnmanaged.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // menuItem_stageVertexProfileLocallyCPUSampling
            // 
            this.menuItem_stageVertexProfileLocallyCPUSampling.Name = "menuItem_stageVertexProfileLocallyCPUSampling";
            this.menuItem_stageVertexProfileLocallyCPUSampling.Size = new System.Drawing.Size(255, 22);
            this.menuItem_stageVertexProfileLocallyCPUSampling.Text = "Profile locally (cpu sampling)";
            this.menuItem_stageVertexProfileLocallyCPUSampling.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // menuItem_stageVertexProfileLocallyMemorySampling
            // 
            this.menuItem_stageVertexProfileLocallyMemorySampling.Name = "menuItem_stageVertexProfileLocallyMemorySampling";
            this.menuItem_stageVertexProfileLocallyMemorySampling.Size = new System.Drawing.Size(255, 22);
            this.menuItem_stageVertexProfileLocallyMemorySampling.Text = "Profile locally (memory sampling)";
            this.menuItem_stageVertexProfileLocallyMemorySampling.Click += new System.EventHandler(this.performLocalVertexDebuggingOrProfiling);
            // 
            // jMStdoutLinesToolStripMenuItem
            // 
            this.jMStdoutLinesToolStripMenuItem.Name = "jMStdoutLinesToolStripMenuItem";
            this.jMStdoutLinesToolStripMenuItem.Size = new System.Drawing.Size(255, 22);
            this.jMStdoutLinesToolStripMenuItem.Text = "JM stdout mentions";
            this.jMStdoutLinesToolStripMenuItem.Click += new System.EventHandler(this.jMStdoutMentionsToolStripMenuItem_Click);
            // 
            // diagnoseToolStripMenuItem2
            // 
            this.diagnoseToolStripMenuItem2.Name = "diagnoseToolStripMenuItem2";
            this.diagnoseToolStripMenuItem2.Size = new System.Drawing.Size(255, 22);
            this.diagnoseToolStripMenuItem2.Text = "Diagnose";
            this.diagnoseToolStripMenuItem2.Click += new System.EventHandler(this.diagnoseToolStripMenuItem1_Click);
            // 
            // splitContainer_vertexData
            // 
            this.splitContainer_vertexData.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer_vertexData.Location = new System.Drawing.Point(0, 0);
            this.splitContainer_vertexData.Name = "splitContainer_vertexData";
            this.splitContainer_vertexData.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer_vertexData.Panel1
            // 
            this.splitContainer_vertexData.Panel1.Controls.Add(this.dataGridView_vertexHeader);
            this.splitContainer_vertexData.Panel1.Controls.Add(this.flowLayoutPanel1);
            // 
            // splitContainer_vertexData.Panel2
            // 
            this.splitContainer_vertexData.Panel2.Controls.Add(this.richTextBox_file);
            this.splitContainer_vertexData.Panel2.Controls.Add(this.label_title);
            this.splitContainer_vertexData.Panel2.Controls.Add(this.flowLayoutPanel5);
            this.splitContainer_vertexData.Size = new System.Drawing.Size(488, 545);
            this.splitContainer_vertexData.SplitterDistance = 128;
            this.splitContainer_vertexData.TabIndex = 2;
            // 
            // dataGridView_vertexHeader
            // 
            this.dataGridView_vertexHeader.AllowUserToAddRows = false;
            this.dataGridView_vertexHeader.AllowUserToDeleteRows = false;
            this.dataGridView_vertexHeader.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
            dataGridViewCellStyle10.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle10.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle10.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle10.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle10.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle10.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle10.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_vertexHeader.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle10;
            this.dataGridView_vertexHeader.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridViewCellStyle11.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle11.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle11.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle11.ForeColor = System.Drawing.SystemColors.ControlText;
            dataGridViewCellStyle11.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle11.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle11.WrapMode = System.Windows.Forms.DataGridViewTriState.False;
            this.dataGridView_vertexHeader.DefaultCellStyle = dataGridViewCellStyle11;
            this.dataGridView_vertexHeader.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView_vertexHeader.Location = new System.Drawing.Point(0, 32);
            this.dataGridView_vertexHeader.MultiSelect = false;
            this.dataGridView_vertexHeader.Name = "dataGridView_vertexHeader";
            this.dataGridView_vertexHeader.ReadOnly = true;
            dataGridViewCellStyle12.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle12.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle12.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle12.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle12.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle12.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle12.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView_vertexHeader.RowHeadersDefaultCellStyle = dataGridViewCellStyle12;
            this.dataGridView_vertexHeader.RowHeadersVisible = false;
            this.dataGridView_vertexHeader.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.CellSelect;
            this.dataGridView_vertexHeader.ShowEditingIcon = false;
            this.dataGridView_vertexHeader.Size = new System.Drawing.Size(488, 96);
            this.dataGridView_vertexHeader.TabIndex = 1;
            // 
            // flowLayoutPanel1
            // 
            this.flowLayoutPanel1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.flowLayoutPanel1.Controls.Add(this.label_Vertex);
            this.flowLayoutPanel1.Controls.Add(this.label_comboVertex);
            this.flowLayoutPanel1.Controls.Add(this.comboBox_vertexInformation);
            this.flowLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.flowLayoutPanel1.MinimumSize = new System.Drawing.Size(260, 32);
            this.flowLayoutPanel1.Name = "flowLayoutPanel1";
            this.flowLayoutPanel1.Size = new System.Drawing.Size(488, 32);
            this.flowLayoutPanel1.TabIndex = 2;
            this.flowLayoutPanel1.WrapContents = false;
            // 
            // label_Vertex
            // 
            this.label_Vertex.AutoSize = true;
            this.label_Vertex.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_Vertex.Location = new System.Drawing.Point(3, 3);
            this.label_Vertex.Margin = new System.Windows.Forms.Padding(3);
            this.label_Vertex.Name = "label_Vertex";
            this.label_Vertex.Padding = new System.Windows.Forms.Padding(3);
            this.label_Vertex.Size = new System.Drawing.Size(58, 22);
            this.label_Vertex.TabIndex = 0;
            this.label_Vertex.Text = "Vertex";
            // 
            // label_comboVertex
            // 
            this.label_comboVertex.AutoSize = true;
            this.label_comboVertex.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_comboVertex.Location = new System.Drawing.Point(74, 3);
            this.label_comboVertex.Margin = new System.Windows.Forms.Padding(10, 3, 0, 3);
            this.label_comboVertex.Name = "label_comboVertex";
            this.label_comboVertex.Padding = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.label_comboVertex.Size = new System.Drawing.Size(47, 22);
            this.label_comboVertex.TabIndex = 8;
            this.label_comboVertex.Text = "Show:";
            // 
            // comboBox_vertexInformation
            // 
            this.comboBox_vertexInformation.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.comboBox_vertexInformation.FormattingEnabled = true;
            this.comboBox_vertexInformation.Items.AddRange(new object[] {
            "nothing",
            "stdout",
            "stderr",
            "error",
            "logs",
            "work dir",
            "inputs",
            "outputs"});
            this.comboBox_vertexInformation.Location = new System.Drawing.Point(124, 3);
            this.comboBox_vertexInformation.Name = "comboBox_vertexInformation";
            this.comboBox_vertexInformation.Size = new System.Drawing.Size(129, 24);
            this.comboBox_vertexInformation.TabIndex = 7;
            this.comboBox_vertexInformation.SelectedIndexChanged += new System.EventHandler(this.comboBox_vertexInformation_SelectedIndexChanged);
            // 
            // richTextBox_file
            // 
            this.richTextBox_file.Dock = System.Windows.Forms.DockStyle.Fill;
            this.richTextBox_file.Font = new System.Drawing.Font("Consolas", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.richTextBox_file.HideSelection = false;
            this.richTextBox_file.Location = new System.Drawing.Point(0, 51);
            this.richTextBox_file.Name = "richTextBox_file";
            this.richTextBox_file.ReadOnly = true;
            this.richTextBox_file.Size = new System.Drawing.Size(488, 362);
            this.richTextBox_file.TabIndex = 0;
            this.richTextBox_file.Text = "";
            this.richTextBox_file.WordWrap = false;
            this.richTextBox_file.LinkClicked += new System.Windows.Forms.LinkClickedEventHandler(this.richTextBox_file_LinkClicked);
            this.richTextBox_file.SelectionChanged += new System.EventHandler(this.richTextBox_file_SelectionChanged);
            // 
            // label_title
            // 
            this.label_title.AutoSize = true;
            this.label_title.Dock = System.Windows.Forms.DockStyle.Top;
            this.label_title.Location = new System.Drawing.Point(0, 32);
            this.label_title.Margin = new System.Windows.Forms.Padding(3);
            this.label_title.Name = "label_title";
            this.label_title.Padding = new System.Windows.Forms.Padding(3);
            this.label_title.Size = new System.Drawing.Size(6, 19);
            this.label_title.TabIndex = 2;
            // 
            // flowLayoutPanel5
            // 
            this.flowLayoutPanel5.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.flowLayoutPanel5.Controls.Add(this.label1);
            this.flowLayoutPanel5.Controls.Add(this.textBox_find);
            this.flowLayoutPanel5.Controls.Add(this.button_findPrev);
            this.flowLayoutPanel5.Controls.Add(this.button_findNext);
            this.flowLayoutPanel5.Controls.Add(this.button_filter);
            this.flowLayoutPanel5.Controls.Add(this.button_clearFind);
            this.flowLayoutPanel5.Controls.Add(this.label_matches);
            this.flowLayoutPanel5.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel5.Location = new System.Drawing.Point(0, 0);
            this.flowLayoutPanel5.MinimumSize = new System.Drawing.Size(460, 32);
            this.flowLayoutPanel5.Name = "flowLayoutPanel5";
            this.flowLayoutPanel5.Size = new System.Drawing.Size(488, 32);
            this.flowLayoutPanel5.TabIndex = 1;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Left;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(0, 3);
            this.label1.Margin = new System.Windows.Forms.Padding(0, 3, 0, 3);
            this.label1.Name = "label1";
            this.label1.Padding = new System.Windows.Forms.Padding(0, 3, 0, 3);
            this.label1.Size = new System.Drawing.Size(34, 22);
            this.label1.TabIndex = 0;
            this.label1.Text = "Find";
            // 
            // textBox_find
            // 
            this.textBox_find.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBox_find.Enabled = false;
            this.textBox_find.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBox_find.Location = new System.Drawing.Point(37, 3);
            this.textBox_find.Name = "textBox_find";
            this.textBox_find.Size = new System.Drawing.Size(195, 22);
            this.textBox_find.TabIndex = 1;
            this.textBox_find.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            this.textBox_find.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.textBox_find_KeyPress);
            // 
            // button_findPrev
            // 
            this.button_findPrev.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_findPrev.Enabled = false;
            this.button_findPrev.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_findPrev.Location = new System.Drawing.Point(236, 0);
            this.button_findPrev.Margin = new System.Windows.Forms.Padding(1, 0, 1, 0);
            this.button_findPrev.Name = "button_findPrev";
            this.button_findPrev.Size = new System.Drawing.Size(45, 28);
            this.button_findPrev.TabIndex = 4;
            this.button_findPrev.Text = "prev";
            this.button_findPrev.UseVisualStyleBackColor = true;
            this.button_findPrev.Click += new System.EventHandler(this.button_findPrev_Click);
            // 
            // button_findNext
            // 
            this.button_findNext.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_findNext.Enabled = false;
            this.button_findNext.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_findNext.Location = new System.Drawing.Point(283, 0);
            this.button_findNext.Margin = new System.Windows.Forms.Padding(1, 0, 1, 0);
            this.button_findNext.Name = "button_findNext";
            this.button_findNext.Size = new System.Drawing.Size(45, 28);
            this.button_findNext.TabIndex = 5;
            this.button_findNext.Text = "next";
            this.button_findNext.UseVisualStyleBackColor = true;
            this.button_findNext.Click += new System.EventHandler(this.button_findNext_Click);
            // 
            // button_filter
            // 
            this.button_filter.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_filter.Enabled = false;
            this.button_filter.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_filter.Location = new System.Drawing.Point(330, 0);
            this.button_filter.Margin = new System.Windows.Forms.Padding(1, 0, 1, 0);
            this.button_filter.Name = "button_filter";
            this.button_filter.Size = new System.Drawing.Size(45, 28);
            this.button_filter.TabIndex = 7;
            this.button_filter.Text = "filter";
            this.button_filter.UseVisualStyleBackColor = true;
            this.button_filter.Click += new System.EventHandler(this.button_filter_Click);
            // 
            // button_clearFind
            // 
            this.button_clearFind.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_clearFind.Enabled = false;
            this.button_clearFind.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_clearFind.Location = new System.Drawing.Point(376, 0);
            this.button_clearFind.Margin = new System.Windows.Forms.Padding(0);
            this.button_clearFind.Name = "button_clearFind";
            this.button_clearFind.Size = new System.Drawing.Size(18, 28);
            this.button_clearFind.TabIndex = 2;
            this.button_clearFind.Text = "X";
            this.button_clearFind.UseVisualStyleBackColor = true;
            this.button_clearFind.Visible = false;
            this.button_clearFind.Click += new System.EventHandler(this.button_clearFind_Click);
            // 
            // label_matches
            // 
            this.label_matches.AutoSize = true;
            this.label_matches.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_matches.Location = new System.Drawing.Point(397, 3);
            this.label_matches.Margin = new System.Windows.Forms.Padding(3);
            this.label_matches.Name = "label_matches";
            this.label_matches.Padding = new System.Windows.Forms.Padding(3);
            this.label_matches.Size = new System.Drawing.Size(65, 22);
            this.label_matches.TabIndex = 6;
            this.label_matches.Text = "matches";
            // 
            // statusStrip
            // 
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripStatusLabel,
            this.toolStripStatusLabel_currentWork,
            this.toolStripStatusLabel_backgroundWork,
            this.toolStripProgressBar});
            this.statusStrip.Location = new System.Drawing.Point(0, 569);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.Size = new System.Drawing.Size(1262, 22);
            this.statusStrip.TabIndex = 2;
            this.statusStrip.Text = "statusStrip1";
            // 
            // toolStripStatusLabel
            // 
            this.toolStripStatusLabel.Name = "toolStripStatusLabel";
            this.toolStripStatusLabel.Overflow = System.Windows.Forms.ToolStripItemOverflow.Never;
            this.toolStripStatusLabel.Size = new System.Drawing.Size(946, 17);
            this.toolStripStatusLabel.Spring = true;
            this.toolStripStatusLabel.Text = "OK";
            this.toolStripStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // toolStripStatusLabel_currentWork
            // 
            this.toolStripStatusLabel_currentWork.Name = "toolStripStatusLabel_currentWork";
            this.toolStripStatusLabel_currentWork.Overflow = System.Windows.Forms.ToolStripItemOverflow.Never;
            this.toolStripStatusLabel_currentWork.Size = new System.Drawing.Size(87, 17);
            this.toolStripStatusLabel_currentWork.Text = "Doing nothing.";
            // 
            // toolStripStatusLabel_backgroundWork
            // 
            this.toolStripStatusLabel_backgroundWork.Name = "toolStripStatusLabel_backgroundWork";
            this.toolStripStatusLabel_backgroundWork.Overflow = System.Windows.Forms.ToolStripItemOverflow.Never;
            this.toolStripStatusLabel_backgroundWork.Size = new System.Drawing.Size(112, 17);
            this.toolStripStatusLabel_backgroundWork.Text = "0 pending activities.";
            // 
            // toolStripProgressBar
            // 
            this.toolStripProgressBar.Name = "toolStripProgressBar";
            this.toolStripProgressBar.Size = new System.Drawing.Size(100, 16);
            // 
            // JobBrowser
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1262, 591);
            this.Controls.Add(this.splitContainer_jobAndRest);
            this.Controls.Add(this.menu);
            this.Controls.Add(this.statusStrip);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.menu;
            this.MinimumSize = new System.Drawing.Size(100, 100);
            this.Name = "JobBrowser";
            this.Text = "JobBrowser";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.JobBrowser_FormClosing);
            this.Load += new System.EventHandler(this.JobBrowser_Load);
            this.Shown += new System.EventHandler(this.JobBrowser_Shown);
            this.menu.ResumeLayout(false);
            this.menu.PerformLayout();
            this.splitContainer_jobAndRest.Panel1.ResumeLayout(false);
            this.splitContainer_jobAndRest.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_jobAndRest)).EndInit();
            this.splitContainer_jobAndRest.ResumeLayout(false);
            this.splitContainer_jobData.Panel1.ResumeLayout(false);
            this.splitContainer_jobData.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_jobData)).EndInit();
            this.splitContainer_jobData.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_jobHeader)).EndInit();
            this.flowLayoutPanel3.ResumeLayout(false);
            this.flowLayoutPanel3.PerformLayout();
            this.panel_scheduleContainer.ResumeLayout(false);
            this.flowLayoutPanel2.ResumeLayout(false);
            this.flowLayoutPanel2.PerformLayout();
            this.splitContainer_stageAndVertex.Panel1.ResumeLayout(false);
            this.splitContainer_stageAndVertex.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageAndVertex)).EndInit();
            this.splitContainer_stageAndVertex.ResumeLayout(false);
            this.splitContainer_stageData.Panel1.ResumeLayout(false);
            this.splitContainer_stageData.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageData)).EndInit();
            this.splitContainer_stageData.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_stageHeader)).EndInit();
            this.flowLayoutPanel4.ResumeLayout(false);
            this.flowLayoutPanel4.PerformLayout();
            this.splitContainer_stageData1.Panel1.ResumeLayout(false);
            this.splitContainer_stageData1.Panel1.PerformLayout();
            this.splitContainer_stageData1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_stageData1)).EndInit();
            this.splitContainer_stageData1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_stageContents)).EndInit();
            this.contextMenu_stageVertex.ResumeLayout(false);
            this.splitContainer_vertexData.Panel1.ResumeLayout(false);
            this.splitContainer_vertexData.Panel2.ResumeLayout(false);
            this.splitContainer_vertexData.Panel2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer_vertexData)).EndInit();
            this.splitContainer_vertexData.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView_vertexHeader)).EndInit();
            this.flowLayoutPanel1.ResumeLayout(false);
            this.flowLayoutPanel1.PerformLayout();
            this.flowLayoutPanel5.ResumeLayout(false);
            this.flowLayoutPanel5.PerformLayout();
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menu;
        private System.Windows.Forms.ToolStripMenuItem jobToolStripMenuItem;
        private System.Windows.Forms.SplitContainer splitContainer_jobAndRest;
        private System.Windows.Forms.Label label_job;
        private System.Windows.Forms.SplitContainer splitContainer_stageAndVertex;
        private System.Windows.Forms.SplitContainer splitContainer_jobData;
        private System.Windows.Forms.Label label_stage;
        private System.Windows.Forms.SplitContainer splitContainer_stageData;
        private System.Windows.Forms.SplitContainer splitContainer_vertexData;
        private System.Windows.Forms.DataGridView dataGridView_jobHeader;
        private System.Windows.Forms.Panel panel_jobSchedule;
        private System.Windows.Forms.DataGridView dataGridView_stageHeader;
        private System.Windows.Forms.DataGridView dataGridView_stageContents;
        private System.Windows.Forms.DataGridView dataGridView_vertexHeader;
        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel1;
        private System.Windows.Forms.Label label_Vertex;
        private System.Windows.Forms.RichTextBox richTextBox_file;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel2;
        private System.Windows.Forms.ToolStripMenuItem refreshToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem closeToolStripMenuItem;
        private System.Windows.Forms.SplitContainer splitContainer_stageData1;
        private System.Windows.Forms.TextBox textBox_stageCode;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel3;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel4;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel5;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox textBox_find;
        private System.Windows.Forms.Button button_clearFind;
        private System.Windows.Forms.Button button_findPrev;
        private System.Windows.Forms.Button button_findNext;
        private System.Windows.Forms.Label label_matches;
        private System.Windows.Forms.CheckBox checkBox_refresh;
        private System.Windows.Forms.Button button_filter;
        private System.Windows.Forms.ComboBox comboBox_vertexInformation;
        private System.Windows.Forms.Label label_comboVertex;
        private System.Windows.Forms.ToolStripMenuItem viewToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem zoomInToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem zoomOutToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem diagnoseToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem loadFileInEditorToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip contextMenu_stageVertex;
        private System.Windows.Forms.ToolStripMenuItem menuItem_stageVertexLocalDebugManaged;
        private System.Windows.Forms.ToolStripMenuItem vertexToolStripMenuItem;
        private System.Windows.Forms.ComboBox comboBox_plan;
        private System.Windows.Forms.Label label_plan;
        private System.Windows.Forms.ToolStripMenuItem zoomToFitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem debugLocallyManagedToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem menuItem_stageVertexLocalDebugUnmanaged;
        private System.Windows.Forms.ToolStripMenuItem debugLocallyUnmanagedToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem jMStdoutMentionsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem jMStdoutLinesToolStripMenuItem;
        private System.Windows.Forms.TextBox textBox_stageFilter;
        private System.Windows.Forms.Button button_stageFilter;
        private System.Windows.Forms.Button button_clearStageFilter;
        private System.Windows.Forms.Panel panel_scheduleContainer;
        private System.Windows.Forms.ToolStripMenuItem hideCancelledVerticesToolStripMenuItem;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel_backgroundWork;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel_currentWork;
        private System.Windows.Forms.ToolStripProgressBar toolStripProgressBar;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem_terminate;
        private System.Windows.Forms.ToolStripMenuItem diagnoseToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem diagnoseToolStripMenuItem2;
        private System.Windows.Forms.ToolStripMenuItem loadFileInLogViewerToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem profileLocallyCPUSamplingToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem menuItem_stageVertexProfileLocallyCPUSampling;
        private System.Windows.Forms.ToolStripMenuItem profileLocallyMemorySamplingToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem menuItem_stageVertexProfileLocallyMemorySampling;
        private System.Windows.Forms.ToolStripMenuItem stageToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem colorByStagestatusToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem wordWrapToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStripMenuItem rightToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem leftToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem upToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem downToolStripMenuItem;
        private System.Windows.Forms.Label label_title;
        private System.Windows.Forms.ToolStripMenuItem packageCachedFilesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem cacheAllLogsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exportToCSVToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem cacheLogsForAllVerticesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem cancelCurrentWorkToolStripMenuItem;
    }
}
