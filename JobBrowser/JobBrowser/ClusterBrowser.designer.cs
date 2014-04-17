
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
using Microsoft.Research.Tools;

namespace Microsoft.Research.DryadAnalysis
{
    partial class ClusterBrowser
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ClusterBrowser));
            this.contextMenuStrip_job = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.openInJobBrowserToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.diagnoseToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.terminateToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.cancelToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.statuslabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStripProgressBar = new System.Windows.Forms.ToolStripProgressBar();
            this.flowLayoutPanel_header = new System.Windows.Forms.FlowLayoutPanel();
            this.label_vc = new System.Windows.Forms.Label();
            this.comboBox_virtualCluster = new System.Windows.Forms.ComboBox();
            this.menuStrip = new System.Windows.Forms.MenuStrip();
            this.jobToolStripMenuItem_file = new System.Windows.Forms.ToolStripMenuItem();
            this.newWindowToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.refreshToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.exitToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem_job = new System.Windows.Forms.ToolStripMenuItem();
            this.jobBrowserToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.diagnoseToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.terminateToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openFromURLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.settingsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.logFileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.autoRefreshToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.clusterToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.filteredDataGridView = new FilteredDataGridView();
            this.contextMenuStrip_job.SuspendLayout();
            this.statusStrip.SuspendLayout();
            this.flowLayoutPanel_header.SuspendLayout();
            this.menuStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // contextMenuStrip_job
            // 
            this.contextMenuStrip_job.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.openInJobBrowserToolStripMenuItem,
            this.diagnoseToolStripMenuItem1,
            this.terminateToolStripMenuItem1});
            this.contextMenuStrip_job.Name = "contextMenuStrip_job";
            this.contextMenuStrip_job.Size = new System.Drawing.Size(182, 70);
            // 
            // openInJobBrowserToolStripMenuItem
            // 
            this.openInJobBrowserToolStripMenuItem.Name = "openInJobBrowserToolStripMenuItem";
            this.openInJobBrowserToolStripMenuItem.Size = new System.Drawing.Size(181, 22);
            this.openInJobBrowserToolStripMenuItem.Text = "Open in job browser";
            this.openInJobBrowserToolStripMenuItem.Click += new System.EventHandler(this.jobBrowserToolStripMenuItem_Click);
            // 
            // diagnoseToolStripMenuItem1
            // 
            this.diagnoseToolStripMenuItem1.Name = "diagnoseToolStripMenuItem1";
            this.diagnoseToolStripMenuItem1.Size = new System.Drawing.Size(181, 22);
            this.diagnoseToolStripMenuItem1.Text = "Diagnose";
            this.diagnoseToolStripMenuItem1.Visible = false;
            this.diagnoseToolStripMenuItem1.Click += new System.EventHandler(this.diagnoseToolStripMenuItem_Click);
            // 
            // terminateToolStripMenuItem1
            // 
            this.terminateToolStripMenuItem1.Name = "terminateToolStripMenuItem1";
            this.terminateToolStripMenuItem1.Size = new System.Drawing.Size(181, 22);
            this.terminateToolStripMenuItem1.Text = "Terminate";
            this.terminateToolStripMenuItem1.Click += new System.EventHandler(this.terminateToolStripMenuItem_Click);
            // 
            // cancelToolStripMenuItem1
            // 
            this.cancelToolStripMenuItem1.Name = "cancelToolStripMenuItem1";
            this.cancelToolStripMenuItem1.Size = new System.Drawing.Size(186, 22);
            this.cancelToolStripMenuItem1.Text = "Cancel current work";
            this.cancelToolStripMenuItem1.Click += new System.EventHandler(this.cancelToolStripMenuItem_Click);
            // 
            // statusStrip
            // 
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.statuslabel,
            this.toolStripProgressBar});
            this.statusStrip.Location = new System.Drawing.Point(0, 431);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.Size = new System.Drawing.Size(1143, 22);
            this.statusStrip.TabIndex = 3;
            this.statusStrip.Text = "statusStrip";
            // 
            // statuslabel
            // 
            this.statuslabel.Name = "statuslabel";
            this.statuslabel.Overflow = System.Windows.Forms.ToolStripItemOverflow.Never;
            this.statuslabel.Size = new System.Drawing.Size(1026, 17);
            this.statuslabel.Spring = true;
            this.statuslabel.Text = "Status displayed here";
            this.statuslabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // toolStripProgressBar
            // 
            this.toolStripProgressBar.Name = "toolStripProgressBar";
            this.toolStripProgressBar.Size = new System.Drawing.Size(100, 16);
            // 
            // flowLayoutPanel_header
            // 
            this.flowLayoutPanel_header.AutoSize = true;
            this.flowLayoutPanel_header.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.flowLayoutPanel_header.Controls.Add(this.label_vc);
            this.flowLayoutPanel_header.Controls.Add(this.comboBox_virtualCluster);
            this.flowLayoutPanel_header.Dock = System.Windows.Forms.DockStyle.Top;
            this.flowLayoutPanel_header.Location = new System.Drawing.Point(0, 24);
            this.flowLayoutPanel_header.MinimumSize = new System.Drawing.Size(400, 32);
            this.flowLayoutPanel_header.Name = "flowLayoutPanel_header";
            this.flowLayoutPanel_header.Size = new System.Drawing.Size(1143, 32);
            this.flowLayoutPanel_header.TabIndex = 12;
            this.flowLayoutPanel_header.Visible = false;
            // 
            // label_vc
            // 
            this.label_vc.AutoSize = true;
            this.label_vc.Dock = System.Windows.Forms.DockStyle.Left;
            this.label_vc.Enabled = false;
            this.label_vc.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label_vc.Location = new System.Drawing.Point(3, 3);
            this.label_vc.Margin = new System.Windows.Forms.Padding(3);
            this.label_vc.Name = "label_vc";
            this.label_vc.Padding = new System.Windows.Forms.Padding(3);
            this.label_vc.Size = new System.Drawing.Size(95, 22);
            this.label_vc.TabIndex = 12;
            this.label_vc.Text = "Virtual Cluster";
            this.label_vc.Visible = false;
            // 
            // comboBox_virtualCluster
            // 
            this.comboBox_virtualCluster.Dock = System.Windows.Forms.DockStyle.Left;
            this.comboBox_virtualCluster.Enabled = false;
            this.comboBox_virtualCluster.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.comboBox_virtualCluster.FormattingEnabled = true;
            this.comboBox_virtualCluster.Location = new System.Drawing.Point(104, 3);
            this.comboBox_virtualCluster.Name = "comboBox_virtualCluster";
            this.comboBox_virtualCluster.Size = new System.Drawing.Size(218, 24);
            this.comboBox_virtualCluster.TabIndex = 13;
            this.comboBox_virtualCluster.Visible = false;
            this.comboBox_virtualCluster.SelectedIndexChanged += new System.EventHandler(this.comboBox_virtualCluster_SelectedIndexChanged);
            this.comboBox_virtualCluster.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.comboBox_virtualCluster_KeyPress);
            // 
            // menuStrip
            // 
            this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.jobToolStripMenuItem_file,
            this.toolStripMenuItem_job,
            this.settingsToolStripMenuItem,
            this.clusterToolStripMenuItem});
            this.menuStrip.Location = new System.Drawing.Point(0, 0);
            this.menuStrip.Name = "menuStrip";
            this.menuStrip.Size = new System.Drawing.Size(1143, 24);
            this.menuStrip.TabIndex = 13;
            this.menuStrip.Text = "menuStrip1";
            // 
            // jobToolStripMenuItem_file
            // 
            this.jobToolStripMenuItem_file.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.cancelToolStripMenuItem1,
            this.newWindowToolStripMenuItem,
            this.refreshToolStripMenuItem1,
            this.exitToolStripMenuItem1});
            this.jobToolStripMenuItem_file.Name = "jobToolStripMenuItem_file";
            this.jobToolStripMenuItem_file.Size = new System.Drawing.Size(44, 20);
            this.jobToolStripMenuItem_file.Text = "&View";
            // 
            // newWindowToolStripMenuItem
            // 
            this.newWindowToolStripMenuItem.Name = "newWindowToolStripMenuItem";
            this.newWindowToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.N)));
            this.newWindowToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.newWindowToolStripMenuItem.Text = "&New window";
            this.newWindowToolStripMenuItem.ToolTipText = "Open a new cluster browser window.";
            this.newWindowToolStripMenuItem.Click += new System.EventHandler(this.newWindowToolStripMenuItem_Click);
            // 
            // refreshToolStripMenuItem1
            // 
            this.refreshToolStripMenuItem1.Name = "refreshToolStripMenuItem1";
            this.refreshToolStripMenuItem1.ShortcutKeys = System.Windows.Forms.Keys.F5;
            this.refreshToolStripMenuItem1.Size = new System.Drawing.Size(186, 22);
            this.refreshToolStripMenuItem1.Text = "&Refresh";
            this.refreshToolStripMenuItem1.ToolTipText = "Refresh the list of jobs on the cluster.";
            this.refreshToolStripMenuItem1.Click += new System.EventHandler(this.refreshToolStripMenuItem1_Click);
            // 
            // exitToolStripMenuItem1
            // 
            this.exitToolStripMenuItem1.Name = "exitToolStripMenuItem1";
            this.exitToolStripMenuItem1.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Q)));
            this.exitToolStripMenuItem1.Size = new System.Drawing.Size(186, 22);
            this.exitToolStripMenuItem1.Text = "Close";
            this.exitToolStripMenuItem1.ToolTipText = "Save settings and close the window.";
            this.exitToolStripMenuItem1.Click += new System.EventHandler(this.exitToolStripMenuItem2_Click);
            // 
            // toolStripMenuItem_job
            // 
            this.toolStripMenuItem_job.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.jobBrowserToolStripMenuItem,
            this.diagnoseToolStripMenuItem,
            this.terminateToolStripMenuItem,
            this.openFromURLToolStripMenuItem});
            this.toolStripMenuItem_job.Name = "toolStripMenuItem_job";
            this.toolStripMenuItem_job.Size = new System.Drawing.Size(37, 20);
            this.toolStripMenuItem_job.Text = "&Job";
            this.toolStripMenuItem_job.ToolTipText = "View job informaotion in detail.";
            // 
            // jobBrowserToolStripMenuItem
            // 
            this.jobBrowserToolStripMenuItem.Name = "jobBrowserToolStripMenuItem";
            this.jobBrowserToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.jobBrowserToolStripMenuItem.Text = "Start job browser";
            this.jobBrowserToolStripMenuItem.Click += new System.EventHandler(this.jobBrowserToolStripMenuItem_Click);
            // 
            // diagnoseToolStripMenuItem
            // 
            this.diagnoseToolStripMenuItem.Name = "diagnoseToolStripMenuItem";
            this.diagnoseToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.diagnoseToolStripMenuItem.Text = "Diagnose";
            this.diagnoseToolStripMenuItem.ToolTipText = "Attempt to diagnose job failures.";
            this.diagnoseToolStripMenuItem.Visible = false;
            this.diagnoseToolStripMenuItem.Click += new System.EventHandler(this.diagnoseToolStripMenuItem_Click);
            // 
            // terminateToolStripMenuItem
            // 
            this.terminateToolStripMenuItem.Name = "terminateToolStripMenuItem";
            this.terminateToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.terminateToolStripMenuItem.Text = "Terminate job";
            this.terminateToolStripMenuItem.ToolTipText = "Ask cluster to terminate job execution.";
            this.terminateToolStripMenuItem.Click += new System.EventHandler(this.terminateToolStripMenuItem_Click);
            // 
            // openFromURLToolStripMenuItem
            // 
            this.openFromURLToolStripMenuItem.Name = "openFromURLToolStripMenuItem";
            this.openFromURLToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.openFromURLToolStripMenuItem.Text = "Open job URL...";
            this.openFromURLToolStripMenuItem.ToolTipText = "Open the job given a URL.";
            this.openFromURLToolStripMenuItem.Visible = false;
            this.openFromURLToolStripMenuItem.Click += new System.EventHandler(this.openFromURLToolStripMenuItem_Click);
            // 
            // settingsToolStripMenuItem
            // 
            this.settingsToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.logFileToolStripMenuItem,
            this.autoRefreshToolStripMenuItem});
            this.settingsToolStripMenuItem.Name = "settingsToolStripMenuItem";
            this.settingsToolStripMenuItem.Size = new System.Drawing.Size(61, 20);
            this.settingsToolStripMenuItem.Text = "Settings";
            // 
            // logFileToolStripMenuItem
            // 
            this.logFileToolStripMenuItem.Name = "logFileToolStripMenuItem";
            this.logFileToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
            this.logFileToolStripMenuItem.Text = "Log file";
            this.logFileToolStripMenuItem.ToolTipText = "When enabled logs errors in the selected file.";
            this.logFileToolStripMenuItem.Click += new System.EventHandler(this.logFileToolStripMenuItem_Click);
            // 
            // autoRefreshToolStripMenuItem
            // 
            this.autoRefreshToolStripMenuItem.Name = "autoRefreshToolStripMenuItem";
            this.autoRefreshToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
            this.autoRefreshToolStripMenuItem.Text = "Auto refresh";
            this.autoRefreshToolStripMenuItem.Click += new System.EventHandler(this.autoRefreshToolStripMenuItem_Click);
            // 
            // clusterToolStripMenuItem
            // 
            this.clusterToolStripMenuItem.Name = "clusterToolStripMenuItem";
            this.clusterToolStripMenuItem.Size = new System.Drawing.Size(56, 20);
            this.clusterToolStripMenuItem.Text = "Cluster";
            // 
            // filteredDataGridView
            // 
            this.filteredDataGridView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.filteredDataGridView.ContextMenuStrip = this.contextMenuStrip_job;
            this.filteredDataGridView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.filteredDataGridView.Location = new System.Drawing.Point(0, 24);
            this.filteredDataGridView.Name = "filteredDataGridView";
            this.filteredDataGridView.Size = new System.Drawing.Size(1143, 429);
            this.filteredDataGridView.TabIndex = 15;
            this.filteredDataGridView.CellFormatting += new System.Windows.Forms.DataGridViewCellFormattingEventHandler(this.filteredDataGridView_CellFormatting);
            this.filteredDataGridView.CellMouseDoubleClick += new System.Windows.Forms.DataGridViewCellMouseEventHandler(this.filteredDataGridView_CellMouseDoubleClick);
            // 
            // ClusterBrowser
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1143, 453);
            this.Controls.Add(this.statusStrip);
            this.Controls.Add(this.flowLayoutPanel_header);
            this.Controls.Add(this.filteredDataGridView);
            this.Controls.Add(this.menuStrip);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "ClusterBrowser";
            this.Text = "Cluster Browser";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.ClusterBrowser_FormClosing);
            this.Load += new System.EventHandler(this.ClusterBrowser_Load);
            this.contextMenuStrip_job.ResumeLayout(false);
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.flowLayoutPanel_header.ResumeLayout(false);
            this.flowLayoutPanel_header.PerformLayout();
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        private void cancelToolStripMenuItem_Click(object sender, System.EventArgs e)
        {
            this.queue.CancelCurrentWork();
        }

        #endregion

        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.ToolStripStatusLabel statuslabel;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanel_header;
        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem_job;
        private System.Windows.Forms.ToolStripMenuItem jobToolStripMenuItem_file;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem jobBrowserToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem refreshToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem openFromURLToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem terminateToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem diagnoseToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip contextMenuStrip_job;
        private System.Windows.Forms.ToolStripMenuItem openInJobBrowserToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem diagnoseToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem terminateToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem cancelToolStripMenuItem1;
        private System.Windows.Forms.ToolStripProgressBar toolStripProgressBar;
        private FilteredDataGridView filteredDataGridView;
        private System.Windows.Forms.ToolStripMenuItem newWindowToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem settingsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem logFileToolStripMenuItem;
        private System.Windows.Forms.Label label_vc;
        private System.Windows.Forms.ComboBox comboBox_virtualCluster;
        private System.Windows.Forms.ToolStripMenuItem autoRefreshToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem clusterToolStripMenuItem;
    }
}
