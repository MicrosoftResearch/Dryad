
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
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;
using Microsoft.Msagl.GraphViewerGdi;
using Microsoft.Msagl.Splines;
using Microsoft.Research.JobObjectModel;
using Microsoft.Research.Tools;

namespace Microsoft.Research.DryadAnalysis
{
    /// <summary>
    /// A form to display information about a DryadLinq job.
    /// </summary>
    public partial class JobBrowser : Form
    {
        /// <summary>
        /// Saved settings for the form.
        /// </summary>
        private JobBrowserSettings formSettings;
        /// <summary>
        /// Job that is being visualized.
        /// </summary>
        public DryadLinqJobInfo Job { get; protected set; }
        /// <summary>
        /// True the first time the form is loading.
        /// </summary>
        private bool doingStartup;

        /// <summary>
        /// Worker for the queue.
        /// </summary>
        private BackgroundWorker queueWorker;
        /// <summary>
        /// Queue for executing background work.
        /// </summary>
        private BackgroundWorkQueue queue;

       
        // window regions starting from left-top in order going down
        #region JOB_HEADER
        /// <summary>
        /// Information about the job being displayed.
        /// </summary>
        private readonly BindingListSortable<PropertyEnumerator<DryadLinqJobInfo>.PropertyValue> jobHeaderData;
        /// <summary>
        /// Enumerates properties of the selected job.
        /// </summary>
        private readonly PropertyEnumerator<DryadLinqJobInfo> jobPropertyEnumerator;
        #endregion

        #region JOB_PLAN
        // unfortunately the job graph is drawn using AGL for the static graph and the DrawingSurface2D + GraphLayout for the dynamic plan...

        /// <summary>
        /// Maps each stage to a color.
        /// </summary>
        private DiscreteColorMap<DryadLinqJobInfo> stageColorMap;

        /// <summary>
        /// AGL graph viewer.
        /// </summary>
        private readonly Msagl.GraphViewerGdi.GViewer graphViewer;
        /// <summary>
        /// AGL representation of the static graph.
        /// </summary>
        private Msagl.Drawing.Graph staticGraph;
        /// <summary>
        /// Zoom level of static graph; an integer which may be negative.
        /// </summary>
        private int staticGraphZoomLevel;

        /// <summary>
        /// Surface on which the job schedule is drawn.
        /// </summary>
        private DrawingSurface2D planDrawSurface { get; set; }
        /// <summary>
        /// Layout for displaying the dynamic plan.
        /// </summary>
        private GraphLayout dynamicPlanLayout;
        /// <summary>
        /// Job schedule information, used to draw the job schedule.
        /// </summary>
        private DryadLinqJobSchedule jobSchedule;

        /// <summary>
        /// Static plan of the job.
        /// </summary>
        private DryadJobStaticPlan staticPlan;
        /// <summary>
        /// Which plan is drawn now?
        /// </summary>
        private enum PlanVisible
        {
            None,
            Static,
            Dynamic,
            Schedule
        };
        /// <summary>
        /// Which plan is visible?
        /// </summary>
        private PlanVisible planVisible { get; set; }
        #endregion

        #region STAGE_HEADER
        // Could be a stage or a table

        /// <summary>
        /// Stage being displayed.
        /// </summary>
        DryadLinqJobStage currentStage;
        /// <summary>
        /// Table being displayed.
        /// </summary>
        StaticPartitionedTableInformation currentTable;

        /// <summary>
        /// Information about the stage being displayed.
        /// </summary>
        private readonly BindingListSortable<PropertyEnumerator<DryadLinqJobStage>.PropertyValue> stageHeaderData;
        /// <summary>
        /// Enumerates properties of the selected stage.
        /// </summary>
        private readonly PropertyEnumerator<DryadLinqJobStage> stagePropertyEnumerator;

        /// <summary>
        /// Information about the table being displayed.
        /// </summary>
        private readonly BindingListSortable<PropertyEnumerator<StaticPartitionedTableInformation>.PropertyValue> tableHeaderData;
        /// <summary>
        /// Enumerate properties of the selected table.
        /// </summary>
        private readonly PropertyEnumerator<StaticPartitionedTableInformation> tablePropertyEnumerator;
        #endregion

        #region STAGE_DATA
        // could be a set of executedvertexinstances or a set of partitions

        /// <summary>
        /// Information about the stage vertices being displayed.
        /// </summary>
        private readonly BindingListSortable<ExecutedVertexInstance> stageData;
        /// <summary>
        /// Information about the partitions of the selected table.
        /// </summary>
        private readonly BindingListSortable<StaticPartitionedTableInformation.StaticPartitionInformation> tablePartitionsData;
        #endregion

        #region VERTEX_HEADER
        /// <summary>
        /// Vertex being displayed.
        /// </summary>
        ExecutedVertexInstance currentVertex;
        /// <summary>
        /// Information about the currently selected vertex.
        /// </summary>
        private readonly BindingListSortable<PropertyEnumerator<ExecutedVertexInstance>.PropertyValue> vertexHeaderData;
        /// <summary>
        /// Enumerates properties of the selected vertex.
        /// </summary>
        private readonly PropertyEnumerator<ExecutedVertexInstance> vertexPropertyEnumerator;
        #endregion

        /// <summary>
        /// Used to display status messages.
        /// </summary>
        readonly StatusWriter status;
        /// <summary>
        /// Default background color.
        /// </summary>
        readonly System.Drawing.Color defaultBackColor;
        /// <summary>
        /// True if plan construction has ended.
        /// </summary>
        private bool plansHaveBeenBuilt;

        #region ZOOM_PLAN
        // support for zooming in the job plan
        /// <summary>
        /// Points around the selection rectangle.
        /// </summary>
        Point startPoint, endPoint;
        /// <summary>
        /// True if the mouse button is being held.
        /// </summary>
        bool mouseIsHeld;
        /// <summary>
        /// True if the mouse is being pressed and dragged.
        /// </summary>
        bool draggingMouse;
        /// <summary>
        /// Size of drawing surface (used to prevent excessive zoom).
        /// </summary>
        double drawingSurfaceSize;
        #endregion

        /// <summary>
        /// Timer invoked to refresh automatically.
        /// </summary>
        readonly System.Windows.Forms.Timer refreshTimer;

        /// <summary>
        /// If true hide the display of cancelled vertices.
        /// </summary>
        public bool HideCancelledVertices { 
            get { 
                return this.hideCancelledVerticesToolStripMenuItem.Checked; 
            }
            protected set
            {
                this.hideCancelledVerticesToolStripMenuItem.Checked = value;
            }
        }

        /// <summary>
        /// Function used for the property enumerator to format various datatypes.
        /// </summary>
        /// <param name="obj">Object to format.</param>
        /// <returns>The value of the object formatted as a string.</returns>
        private string PropertyValueFormatter(object obj)
        {
            if (obj == null)
            {
                return "null";
            }
            else if (obj is long)
            {
                string result = string.Format("{0:N0}", ((long)obj));
                return result;
            }
            else if (obj is DateTime)
            {
                if (((DateTime)obj).Equals(DateTime.MinValue))
                    return "";
            }
            else if (obj is TimeSpan)
            {
                if (((TimeSpan)obj).Equals(TimeSpan.MinValue))
                    return "";
            }

            return obj.ToString();
        }

        /// <summary>
        /// Prepare a datagrid view for visualization.
        /// </summary>
        /// <param name="view">Datagridview to prepare.</param>
        private void SetDataGridViewColumnsSize(DataGridView view)
        {
            int maxDisplayIndex = 0;
            bool any = false;
            for (int i = view.Columns.Count - 1; i >= 0; i--)
            {
                if (!view.Columns[i].Visible) continue;

                DataGridViewColumn column = view.Columns[i];
                if (column.DisplayIndex > maxDisplayIndex)
                    maxDisplayIndex = column.DisplayIndex;
                any = true;

                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }

            if (any)
            {
                DataGridViewColumn lastVisibleColumn = view.Columns[maxDisplayIndex];
                lastVisibleColumn.AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill;
                lastVisibleColumn.MinimumWidth = 20;
            }

            view.AllowUserToResizeColumns = true; // this seems to have no effect...

            // prepare the scrollbars
            VScrollBar vscrollbar = view.Controls.OfType<VScrollBar>().FirstOrDefault();
// ReSharper disable PossibleNullReferenceException
            vscrollbar.Scroll += this.dataGridView_Scroll;
// ReSharper restore PossibleNullReferenceException
        }

        /// <summary>
        /// Initialize a job browser for a specified job.
        /// </summary>
        /// <param name="job">Job to display.</param>
        public JobBrowser(DryadLinqJobInfo job)
        {
            this.doingStartup = true;
            this.InitializeComponent();

            this.queueWorker = new BackgroundWorker();
            this.queue = new BackgroundWorkQueue(this.queueWorker, this.toolStripStatusLabel_currentWork, this.toolStripStatusLabel_backgroundWork);

            this.WarnedAboutDebugging = false;
            this.status = new StatusWriter(this.toolStripStatusLabel, this.statusStrip, this.Status);

            this.refreshTimer = new System.Windows.Forms.Timer();
            this.refreshTimer.Interval = 30000; // 30 seconds
            this.refreshTimer.Tick += this.refreshTimer_Tick;

            #region SET_JOB_HEADER
            this.jobHeaderData = new BindingListSortable<PropertyEnumerator<DryadLinqJobInfo>.PropertyValue>();
            this.dataGridView_jobHeader.DataSource = this.jobHeaderData;
            this.SetDataGridViewColumnsSize(this.dataGridView_jobHeader);
            this.jobPropertyEnumerator = new PropertyEnumerator<DryadLinqJobInfo>();
            this.jobPropertyEnumerator.ValueFormatter = this.PropertyValueFormatter;
            List<string> jobPropertiesToSkip = new List<string>
            {
                "ClusterConfiguration", "Processes", "Vertices", "JM", "Name", 
                "JobManagerVertex", "JMStdoutIncomplete", "JobInfoCannotBeCollected"
            };

            this.jobPropertyEnumerator.Skip(jobPropertiesToSkip);
            this.jobPropertyEnumerator.Expand("Summary");
            #endregion

            #region SET_STAGE_HEADER
            this.stageHeaderData = new BindingListSortable<PropertyEnumerator<DryadLinqJobStage>.PropertyValue>();
            this.stagePropertyEnumerator = new PropertyEnumerator<DryadLinqJobStage>();
            this.stagePropertyEnumerator.ValueFormatter = this.PropertyValueFormatter;
            List<string> stagePropertiesToSkip = new List<string> 
            {
                "Vertices", "Name"
            };
            this.stagePropertyEnumerator.Skip(stagePropertiesToSkip);

            this.tableHeaderData = new BindingListSortable<PropertyEnumerator<StaticPartitionedTableInformation>.PropertyValue>();
            this.tablePropertyEnumerator = new PropertyEnumerator<StaticPartitionedTableInformation>();
            this.tablePropertyEnumerator.ValueFormatter = this.PropertyValueFormatter;
            this.tablePropertyEnumerator.Skip("Partitions", "Header", "Code");

            this.SetNoStageOrTable("", false);
            #endregion

            #region SET_STAGE_DATA
            this.stageData = new BindingListSortable<ExecutedVertexInstance>();
            this.tablePartitionsData = new BindingListSortable<StaticPartitionedTableInformation.StaticPartitionInformation>();
            #endregion

            #region SET_VERTEX_HEADER
            this.vertexHeaderData = new BindingListSortable<PropertyEnumerator<ExecutedVertexInstance>.PropertyValue>();
            this.dataGridView_vertexHeader.DataSource = this.vertexHeaderData;
            this.SetDataGridViewColumnsSize(this.dataGridView_vertexHeader);
            this.vertexPropertyEnumerator = new PropertyEnumerator<ExecutedVertexInstance>();
            this.vertexPropertyEnumerator.ValueFormatter = this.PropertyValueFormatter;
            List<string> vertexPropertiesToSkip = new List<string>
            {
                "JobSummary", "InputChannels", "OutputChannels", "Name", "LogFilesPattern", "IsManager"
            };
            this.vertexPropertyEnumerator.Skip(vertexPropertiesToSkip);
            #endregion

            // Disable the vertex context menu, since none of these operatios work at this point
            this.contextMenu_stageVertex.Enabled = false;

            this.plansHaveBeenBuilt = false;
            this.graphViewer = new Msagl.GraphViewerGdi.GViewer();
            this.graphViewer.Dock = DockStyle.Fill;
            this.graphViewer.NavigationVisible = false;
            this.graphViewer.ToolBarIsVisible = false;
            this.graphViewer.MouseClick += this.graphViewer_MouseClick;
            this.graphViewer.MouseDoubleClick += this.graphViewer_MouseDoubleClick;
            this.graphViewer.InsertingEdge = false;
            this.staticGraphZoomLevel = 0;

            this.planDrawSurface = new DrawingSurface2D(this.panel_jobSchedule);
            this.planDrawSurface.SetMargins(4, 4, 4, 4);
            this.panel_jobSchedule.MouseDoubleClick += this.panel_jobSchedule_MouseDoubleClick;
            this.planDrawSurface.FastDrawing = false;
            this.colorByStagestatusToolStripMenuItem.Checked = true;

            this.defaultBackColor = this.label_job.BackColor;
            this.planVisible = PlanVisible.None;

            this.linkCache = new Dictionary<string, IClusterResidentObject>();
            this.mouseIsHeld = false;
            this.draggingMouse = false;
            this.drawingSurfaceSize = 0.0;

            #region TOOLTIPS
            ToolTip help = new ToolTip();
            help.SetToolTip(this.richTextBox_file, "Click on links to follow; control-click to open in explorer; alt-click to follow an input channel to its source.");
            help.SetToolTip(this.panel_scheduleContainer, "Displays the job schedule; click to select.");
            help.SetToolTip(this.checkBox_refresh, "Refreshes the job status ever 30s.");
            help.SetToolTip(this.graphViewer, "Click to select stages; Ctrl +/- to zoom.");
            help.SetToolTip(this.dataGridView_jobHeader, "Selecting some rows filters the data.");
            help.SetToolTip(this.comboBox_plan, "Display the job plan in various forms.");
            help.SetToolTip(this.comboBox_vertexInformation, "Display more information about the vertex.");
            help.SetToolTip(this.label_job, "Global job information.");
            help.SetToolTip(this.label_stage, "Information about the selected stage/table. Select rows for filtering.");
            help.SetToolTip(this.label_Vertex, "Information about the selected vertex.");
            help.SetToolTip(this.panel_jobSchedule, "Click on the stages or tables for more information; drag to zoom.");
            help.SetToolTip(this.textBox_stageCode, "Code executed by the selected stage.");
            help.SetToolTip(this.textBox_find, "Type a string to find.");
            help.SetToolTip(this.button_clearFind, "Stop finding.");
            help.SetToolTip(this.button_filter, "Show only lines matching string to find (case-sensitive).");
            help.SetToolTip(this.button_findNext, "Find next occurence of string.");
            help.SetToolTip(this.button_findPrev, "Find previous occurence of string.");
            help.SetToolTip(this.label_title, "File currently displayed.");
            #endregion

            this.Job = job;
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
        /// True if the user has been warned about proper debugging technique.
        /// </summary>
        bool WarnedAboutDebugging { get; set; }

        /// <summary>
        /// True if the user has been warned about proper profiling technique.
        /// </summary>
        bool WarnedAboutProfiling { get; set; }

        /// <summary>
        /// If caching is enabled save information about the current job.
        /// </summary>
        /// <param name="job">Job that is being diagnosed.</param>
        void LogJobInCache(DryadLinqJobSummary job)
        {
            if (string.IsNullOrEmpty(CachedClusterResidentObject.CacheDirectory))
                return;
            // generate a unique file name
            string summarystring = job.AsIdentifyingString();
            string md5 = Utilities.MD5(summarystring);
            string path = Utilities.PathCombine(CachedClusterResidentObject.CacheDirectory, "jobs", md5 + ".xml");
            Utilities.EnsureDirectoryExistsForFile(path);
            Utilities.SaveAsXml(path, job);
            CachedClusterResidentObject.RecordCachedFile(job, path);
        }

        #region STATIC_GRAPH
        // static graph manipulation
        
        /// <summary>
        /// Double click on static job plan: zoom in.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void graphViewer_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            this.ZoomStaticPlan(true);
        }

        /// <summary>
        /// Perform zoom in the static plan.
        /// </summary>
        /// <param name="zoomin">If true, zoom in, else zoom out.</param>
        private void ZoomStaticPlan(bool zoomin)
        {
            if (zoomin)
            {
                this.staticGraphZoomLevel++;
                this.graphViewer.ZoomInPressed();
            }
            else
            {
                this.staticGraphZoomLevel--;
                this.graphViewer.ZoomOutPressed();
            }
        }

        /// <summary>
        /// User clicked on the drawn plan.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Mouse click event.</param>
        void graphViewer_MouseClick(object sender, MouseEventArgs e)
        {
            this.ClickOnJobPlan(e.X, e.Y);
        }
        #endregion

        /// <summary>
        /// The timer to refresh the view has ticked.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void refreshTimer_Tick(object sender, EventArgs e)
        {
            if (! this.IsDisposed)
                this.RefreshDisplay();
        }

        /// <summary>
        /// Loading the job information has completed.
        /// <param name="timeToLoad">Time it took to load the job information.</param>
        /// </summary>
        private void LoadJobCompleted(TimeSpan timeToLoad)
        {
            this.LogJobInCache(this.Job.Summary);

            if (this.refreshTimer.Interval < timeToLoad.TotalMilliseconds)
            {
                this.Status(string.Format("Job took {0} to load; adjusting refresh interval", timeToLoad), StatusKind.OK);
                this.refreshTimer.Interval = (int)(2 * timeToLoad.TotalMilliseconds);
            }

            this.jobPropertyEnumerator.Data = this.Job;
            this.Text = this.Job.JobName + " " + this.Job.StartJMTime.ToString("HH:MM") + " " + this.Job.Summary.ClusterJobId;
            this.label_job.Text = "Job";
            this.label_job.BackColor = VertexStateColor(this.Job.ManagerVertex.State);
            this.jobHeaderData.Clear();
            this.jobPropertyEnumerator.PopulateWithProperties(this.jobHeaderData);
            this.dataGridView_jobHeader.ClearSelection();
            this.RefreshQueryPlan();
            this.Status("OK", StatusKind.OK);
        }

        #region MSAGL_CODE
        // this code is pilfered from MSAGL
        private static PointF PointF(Msagl.Point p) { return new PointF((float)p.X, (float)p.Y); }

        const float toDegrees = 180 / (float)Math.PI;

        static float EllipseSweepAngle(Ellipse el)
        {
            float sweepAngle = (float)(el.ParEnd - el.ParStart) * toDegrees;

            if (!OrientedCounterClockwise(el))
                sweepAngle = -sweepAngle;
            return sweepAngle;
        }

        static bool OrientedCounterClockwise(Ellipse ellipse)
        {
            return ellipse.AxisA.X * ellipse.AxisB.Y - ellipse.AxisB.X * ellipse.AxisA.Y > 0;
        }

        static float EllipseStandardAngle(Ellipse ellipse, double angle)
        {
            Msagl.Point p = Math.Cos(angle) * ellipse.AxisA + Math.Sin(angle) * ellipse.AxisB;
            return (float)Math.Atan2(p.Y, p.X) * toDegrees;
        }

        static void AddEllipseSeg(GraphicsPath graphicsPath, Ellipse el)
        {
            Msagl.Rectangle box = el.BoundingBox;

            float startAngle = EllipseStandardAngle(el, el.ParStart);

            float sweepAngle = EllipseSweepAngle(el);

            graphicsPath.AddArc((float)box.Left,
                                (float)box.Bottom,
                                (float)box.Width,
                                (float)box.Height,
                                startAngle,
                                sweepAngle);
        }

        static GraphicsPath CreateGraphicsPath(ICurve iCurve)
        {
            var graphicsPath = new GraphicsPath();
            if (iCurve == null)
                return null;
            var c = iCurve as Curve;

            if (c != null)
            {
                foreach (ICurve seg in c.Segments)
                {
                    var cubic = seg as CubicBezierSegment;
                    if (cubic != null)
                        graphicsPath.AddBezier(PointF(cubic.B(0)), PointF(cubic.B(1)), PointF(cubic.B(2)),
                                               PointF(cubic.B(3)));
                    else
                    {
                        var ls = seg as LineSegment;
                        if (ls != null)
                            graphicsPath.AddLine(PointF(ls.Start), PointF(ls.End));
                        else
                        {
                            var el = seg as Ellipse;
                            AddEllipseSeg(graphicsPath, el);
                        }
                    }
                }
            }
            else
            {
                var ls = iCurve as LineSegment;
                if (ls != null)
                    graphicsPath.AddLine(PointF(ls.Start), PointF(ls.End));
                else
                {
                    var seg = iCurve as CubicBezierSegment;
                    if (seg != null)
                        graphicsPath.AddBezier(PointF(seg.B(0)), PointF(seg.B(1)), PointF(seg.B(2)), PointF(seg.B(3)));
                    else
                    {
                        AddEllipseSeg(graphicsPath, iCurve as Ellipse);
                    }
                }
            }

            return graphicsPath;
        }

        /// <summary>
        /// Draw an node using Microsoft Automatic Graph Layout.
        /// </summary>
        /// <param name="node">Node to draw.</param>
        /// <param name="graphics">Graphics context; we know it's a Graphics object from GViewer.</param>
        /// <returns>False, to invoke the default drawing as well.</returns>
        private bool ShadePlanNode(Msagl.Drawing.Node node, object graphics)
        {
            DryadJobStaticPlan.Stage stage = (DryadJobStaticPlan.Stage)node.UserData;
            DryadLinqJobStage jobStage = this.Job.GetStage(stage.Name);
            Graphics g = (Graphics)graphics;

            if (jobStage != null)
            {
                List<Tuple<double, Color>> colors;
                if (this.colorByStagestatusToolStripMenuItem.Checked)
                {
                    colors = StageStatusAsColors(jobStage, this.HideCancelledVertices, stage.Replication).ToList();
                }
                else
                {
                    colors = new List<Tuple<double, Color>> { new Tuple<double, Color>(1, this.stageColorMap[jobStage.Name]) };
                }
                if (colors.Count > 0)
                {
                    using (System.Drawing.Drawing2D.Matrix m = g.Transform)
                    {
                        using (System.Drawing.Drawing2D.Matrix saveM = m.Clone())
                        {
                            System.Drawing.Drawing2D.GraphicsPath boundary = CreateGraphicsPath(node.Attr.GeometryNode.BoundaryCurve);
                            g.SetClip(boundary);
                            using (var m2 = new System.Drawing.Drawing2D.Matrix(1, 0, 0, -1, 0, 2 * (float)node.Attr.GeometryNode.Center.Y))
                                m.Multiply(m2);

                            g.Transform = m;
                            float totalFraction = 0;
                            double top = node.Attr.GeometryNode.Center.Y - node.Attr.GeometryNode.Height / 2;
                            double left = node.Attr.GeometryNode.Center.X - node.Attr.GeometryNode.Width / 2;
                            double width = node.Attr.Width;
                            foreach (var colorInfo in colors)
                            {
                                float fraction = (float)colorInfo.Item1;
                                Brush brush = new SolidBrush(colorInfo.Item2);

                                RectangleF rect = new RectangleF(
                                    new PointF((float)(left + width * totalFraction), (float)(top)),
                                    new SizeF((float)(width * fraction), (float)node.Attr.Height));
                                totalFraction += fraction;
                                g.FillRectangle(brush, rect);
                            }

                            g.ResetClip();
                            g.Transform = saveM;
                        }
                    }
                }
            }
            return false; // still invoke the default drawing.
        }

        /// <summary>
        /// Build an AGL graph corresponding to a dryadlinq job static plan.
        /// </summary>
        /// <returns>An AGL graph.</returns>
        private Msagl.Drawing.Graph BuildAGLGraph()
        {
            Msagl.Drawing.Graph aglGraph = BuildAGLGraph(this.staticPlan);
            foreach (Msagl.Drawing.Node node in aglGraph.NodeMap.Values)
            {
                node.DrawNodeDelegate = this.ShadePlanNode;
            }

            return aglGraph;
        }

        /// <summary>
        /// Build an AGL graph corresponding to a dryadlinq job static plan.
        /// </summary>
        /// <returns>An AGL graph.</returns>
        private static Msagl.Drawing.Graph BuildAGLGraph(DryadJobStaticPlan plan)
        {
            Msagl.Drawing.Graph retval = new Msagl.Drawing.Graph();
            foreach (DryadJobStaticPlan.Stage stage in plan.GetAllStages())
            {
                Msagl.Drawing.Node node = new Msagl.Drawing.Node(stage.Id.ToString());
                node.UserData = stage;
                if (stage.IsVirtual)
                {
                    if (stage.IsTee)
                        node.Attr.Shape = Microsoft.Msagl.Drawing.Shape.Octagon;
                    else
                        node.Attr.Shape = Msagl.Drawing.Shape.InvHouse;
                }
                else
                {
                    if (stage.Name == "JobManager")
                    {
                        node.Attr.Shape = Microsoft.Msagl.Drawing.Shape.Diamond;
                    }
                    else if (stage.Name == "All vertices")
                        node.Attr.Shape = Microsoft.Msagl.Drawing.Shape.Box;
                    else
                        node.Attr.Shape = Microsoft.Msagl.Drawing.Shape.Ellipse;
                }

                string nodeName = stage.Name;
                if (stage.IsInput || stage.IsOutput)
                {
                    nodeName = string.Join(",", nodeName.Split(',').Select(Path.GetFileName).ToArray());
                    nodeName = Path.GetFileName(nodeName);

                    const int maxNodeNameLen = 40;
                    if (nodeName.Length > maxNodeNameLen)
                    {
                        nodeName = nodeName.Substring(0, maxNodeNameLen / 2) + "..." + nodeName.Substring(nodeName.Length - maxNodeNameLen / 2);
                    }
                }
                if (stage.Replication != 1)
                    node.LabelText = stage.Replication + " x " + nodeName;
                else
                    node.LabelText = nodeName;
                retval.AddNode(node);
            }

            foreach (DryadJobStaticPlan.Connection connection in plan.GetAllConnections())
            {
                Msagl.Drawing.Edge e = retval.AddEdge(connection.From.Id.ToString(), connection.To.Id.ToString());
                if (connection.Arity == DryadJobStaticPlan.Connection.ConnectionType.AllToAll)
                    e.Attr.LineWidth = 3;
                if (connection.ConnectionManager != "None")
                    e.LabelText = connection.ConnectionManager;
                e.Attr.Color = FormColorToAglColor(System.Drawing.Color.FromName(connection.Color()));
                e.UserData = connection;
            }

            return retval;
        }

        /// <summary>
        /// Convert a windows forms color to a msagl color.
        /// </summary>
        /// <param name="color">Color to convert.</param>
        /// <returns>A MSAGL color.</returns>
        private static Msagl.Drawing.Color FormColorToAglColor(System.Drawing.Color color)
        {
            return new Msagl.Drawing.Color(color.A, color.R, color.G, color.B);
        }
        #endregion

        /// <summary>
        /// Color representing the vertex state.
        /// </summary>
        /// <returns>A string naming a color.</returns>
        private static Color VertexStateColor(ExecutedVertexInstance.VertexState state)
        {
            switch (state)
            {
                case ExecutedVertexInstance.VertexState.Cancelled:
                    return Color.Yellow;
                case ExecutedVertexInstance.VertexState.Unknown:
                case ExecutedVertexInstance.VertexState.Abandoned:
                case ExecutedVertexInstance.VertexState.Created:
                    return Color.White;
                case ExecutedVertexInstance.VertexState.Started:
                    return Color.Cyan;
                case ExecutedVertexInstance.VertexState.Invalidated:
                    return Color.YellowGreen;
                case ExecutedVertexInstance.VertexState.Revoked:
                    return Color.Brown;
                case ExecutedVertexInstance.VertexState.Successful:
                    return Color.LightGreen;
                case ExecutedVertexInstance.VertexState.Failed:
                    return Color.Tomato;
                default:
                    throw new DryadException("Unexpected vertex state " + state);
            }
        }

        /// <summary>
        /// Compute the layouts for the job plans.
        /// </summary>
        private void CreatePlanLayouts()
        {
            if (this.staticPlan == null)
            {
                this.comboBox_plan.Items.Remove("Static");
            }

            this.Status("Constructing dynamic plan", StatusKind.LongOp);
            this.dynamicPlanLayout = this.Job.ComputePlanLayout(this.Status);
            if (this.dynamicPlanLayout == null)
            {
                this.comboBox_plan.Items.Remove("Dynamic");
            }

            this.Status("Constructing job schedule", StatusKind.LongOp);
            try
            {
                this.jobSchedule = new DryadLinqJobSchedule(this.Job, this.HideCancelledVertices);
            }
            catch (Exception e)
            {
                Trace.TraceInformation("Exception while building job schedule: " + e.Message);
                this.jobSchedule = null;
            }
// ReSharper disable CompareOfFloatsByEqualityOperator
            if (this.jobSchedule == null || this.jobSchedule.X == 0 || this.jobSchedule.Y == 0)
// ReSharper restore CompareOfFloatsByEqualityOperator
            {
                // degenerate plan, nothing to draw
                this.jobSchedule = null;
                this.comboBox_plan.Items.Remove("Schedule");
            }
            this.Status("Job plans constructed", StatusKind.OK);
            this.plansHaveBeenBuilt = true;
        }

        /// <summary>
        /// Compute the colors to draw in a vertex as a function of the stage status.
        /// </summary>
        private void AssignPlanColors()
        {
            // Plans can be colored in two ways: by stage or by vertex status.
            // The stageColorMap is used for the first purpose.
            int stageCount = this.Job.AllStages().Count();

            if (this.stageColorMap == null)
            {
                if (stageCount < PrecomputedColorMap<DryadLinqJobInfo>.MaxColors)
                {
                    this.stageColorMap = new PrecomputedColorMap<DryadLinqJobInfo>(this.Job, stageCount);
                }
                else
                {
                    this.stageColorMap = new HSVColorMap<DryadLinqJobInfo>(this.Job, stageCount);
                }
                this.stageColorMap.DefaultColor = Color.White;

                int index = 0;
                foreach (var stage in this.Job.AllStages().ToList())
                {
                    this.stageColorMap.AddLabelClass(new object[] { stage.Name }, index++, stage.Name);
                }
            }

            if (this.dynamicPlanLayout != null)
            {
                foreach (var node in this.dynamicPlanLayout.AllNodes)
                {
                    DryadLinqJobStage stage = this.Job.GetStage(node.Stage);
                    DryadJobStaticPlan.Stage staticPlanStage = null;
                    if (this.staticPlan != null)
                        staticPlanStage = this.staticPlan.GetStageByName(node.Stage);
                    int staticVertexCount = staticPlanStage != null ? staticPlanStage.Replication : 0;

                    if (this.colorByStagestatusToolStripMenuItem.Checked)
                        node.FillColors = StageStatusAsColors(stage, this.hideCancelledVerticesToolStripMenuItem.Checked, staticVertexCount).ToList();
                    else
                        node.FillColors = new List<Tuple<double, Color>> { new Tuple<double, Color>(1, this.stageColorMap[stage.Name]) };
                }
            }
        }

        /// <summary>
        /// Compute the colors to draw in a stage as a function of the stage status.
        /// </summary>
        /// <param name="stage">Stage status to represent.</param>
        /// <param name="hideCancelled">If true, hide the cancelled vertices.</param>
        /// <param name="staticVertexCount">Statically estimated number of vertices.</param>
        /// <returns>The colored node.</returns>
        private static IEnumerable<Tuple<double, Color>>
            StageStatusAsColors(DryadLinqJobStage stage, bool hideCancelled, int staticVertexCount)
        {
            int executedVertices = stage.TotalInitiatedVertices;
            if (hideCancelled)
                executedVertices -= stage.CancelledVertices;
            if (executedVertices == 0)
                yield break; // no colors
            double unknown = 0;

            if (staticVertexCount > executedVertices)
            {
                unknown = ((double)staticVertexCount - executedVertices) / staticVertexCount;
                executedVertices = staticVertexCount;
            }
            double succesful = stage.SuccessfulVertices / (double)executedVertices;
            double cancelled = stage.CancelledVertices / (double)executedVertices;
            double failed = stage.FailedVertices / (double)executedVertices;
            double invalidated = stage.InvalidatedVertices/(double) executedVertices;
            double started = stage.StartedVertices / (double)executedVertices;
            double created = stage.CreatedVertices / (double)executedVertices;
            double revoked = stage.RevokedVertices/(double) executedVertices;
            yield return new Tuple<double, Color>(succesful, VertexStateColor(ExecutedVertexInstance.VertexState.Successful));
            if (!hideCancelled)
                yield return new Tuple<double, Color>(cancelled, VertexStateColor(ExecutedVertexInstance.VertexState.Cancelled));
            yield return new Tuple<double, Color>(revoked, VertexStateColor(ExecutedVertexInstance.VertexState.Revoked));
            yield return new Tuple<double, Color>(invalidated, VertexStateColor(ExecutedVertexInstance.VertexState.Invalidated));
            yield return new Tuple<double, Color>(failed, VertexStateColor(ExecutedVertexInstance.VertexState.Failed));
            yield return new Tuple<double, Color>(started, VertexStateColor(ExecutedVertexInstance.VertexState.Started));
            yield return new Tuple<double, Color>(created, VertexStateColor(ExecutedVertexInstance.VertexState.Created));
            yield return new Tuple<double, Color>(unknown, VertexStateColor(ExecutedVertexInstance.VertexState.Unknown));
        }

        /// <summary>
        /// Refresh and redisplay the query plan.
        /// </summary>
        public void RefreshQueryPlan()
        {
            this.richTextBox_file.Text = "";

            var item = new BackgroundWorkItem<DryadJobStaticPlan>(
                m => JobObjectModel.DryadJobStaticPlan.CreatePlan(this.Job, m),
                this.PlanComputed,
                "refresh plan");
            this.Queue(item);
        }
        
        /// <summary>
        /// Continuation for refreshing the query plan.
        /// </summary>
        private void PlanComputed(bool cancelled, DryadJobStaticPlan plan)
        {
            if (cancelled) return;

            this.staticPlan = plan;
            if (this.staticPlan != null)
            {
                this.staticPlan.AddFictitiousStages();
                this.staticGraph = this.BuildAGLGraph();
            }
            this.CreatePlanLayouts();
            this.AssignPlanColors();
            if (this.planVisible != PlanVisible.Static)
                this.FitPlanToWindow();
            this.DrawQueryPlan();
        }

        /// <summary>
        /// Draw the query plan in the appropriate panel.
        /// </summary>
        private void DrawQueryPlan()
        {
            if (!this.plansHaveBeenBuilt)
                return;

            PlanVisible previousPlan = this.planVisible;
            this.planVisible = PlanVisible.None;

            switch (this.comboBox_plan.Text)
            {
                case "Static":
                    {
                        this.upToolStripMenuItem.Enabled = false;
                        this.downToolStripMenuItem.Enabled = false;
                        this.leftToolStripMenuItem.Enabled = false;
                        this.rightToolStripMenuItem.Enabled = false;

                        if (this.staticGraph != null)
                        {
                            this.splitContainer_jobData.SuspendLayout();
                            if (this.panel_scheduleContainer.Controls.Contains(this.panel_jobSchedule))
                            {
                                this.panel_scheduleContainer.Controls.Remove(this.panel_jobSchedule);
                                this.panel_scheduleContainer.Controls.Add(this.graphViewer);
                            }
                            this.graphViewer.Graph = this.staticGraph;
                            this.splitContainer_jobData.ResumeLayout();
                            this.planVisible = PlanVisible.Static;

                            // zoom again to the previous level
                            int zoom = Math.Abs(this.staticGraphZoomLevel);
                            if (zoom != 0)
                            {
                                bool zoomin = this.staticGraphZoomLevel >= 0;
                                this.staticGraphZoomLevel = 0;
                                for (int i = 0; i < zoom; i++)
                                {
                                    this.ZoomStaticPlan(zoomin);
                                }
                            }
                        }
                        else
                        {
                            this.Status("Could not compute static plan", StatusKind.Error);
                        }
                    }
                    break;
                case "Dynamic":
                    {
                        this.upToolStripMenuItem.Enabled = true;
                        this.downToolStripMenuItem.Enabled = true;
                        this.leftToolStripMenuItem.Enabled = true;
                        this.rightToolStripMenuItem.Enabled = true;

                        this.splitContainer_jobData.SuspendLayout();
                        if (this.panel_scheduleContainer.Controls.Contains(this.graphViewer))
                        {
                            this.panel_scheduleContainer.Controls.Remove(this.graphViewer);
                            this.panel_scheduleContainer.Controls.Add(this.panel_jobSchedule);
                            // if we are switching, clear the selected nodes
                            this.dynamicPlanLayout.ClearSelectedNodes();
                        }
                        this.planDrawSurface.Clear();
                        this.panel_jobSchedule.Invalidate();
                        this.splitContainer_jobData.ResumeLayout();
                        if (this.dynamicPlanLayout != null)
                        {
                            if (previousPlan != PlanVisible.Dynamic)
                            {
                                // new plan is shown; resize the area
                                this.planDrawSurface.SetDrawingArea(
                                    new Rectangle2D(
                                        new Point2D(0, 0),
                                        new Point2D(this.dynamicPlanLayout.Size.X, this.dynamicPlanLayout.Size.Y)));
                                this.drawingSurfaceSize = this.dynamicPlanLayout.Size.X * this.dynamicPlanLayout.Size.Y;
                            }
                            this.dynamicPlanLayout.Draw(this.planDrawSurface);
                            this.planVisible = PlanVisible.Dynamic;
                        }
                        else
                        {
                            this.Status("Could not compute dynamic plan", StatusKind.Error);
                            this.drawingSurfaceSize = 0.0;
                        }
                    }
                    break;
                case "Schedule":
                    this.upToolStripMenuItem.Enabled = true;
                    this.downToolStripMenuItem.Enabled = true;
                    this.leftToolStripMenuItem.Enabled = true;
                    this.rightToolStripMenuItem.Enabled = true;

                    this.splitContainer_jobData.SuspendLayout();
                    if (this.panel_scheduleContainer.Controls.Contains(this.graphViewer))
                    {
                        this.panel_scheduleContainer.Controls.Remove(this.graphViewer);
                        this.panel_scheduleContainer.Controls.Add(this.panel_jobSchedule);
                    }
                    this.planDrawSurface.Clear();
                    this.panel_jobSchedule.Invalidate();
                    this.splitContainer_jobData.ResumeLayout();

                    if (this.jobSchedule != null)
                    {
                        if (previousPlan != PlanVisible.Schedule)
                        {
                            // new plan is shown; resize the area
                            this.planDrawSurface.SetDrawingArea(
                                new Rectangle2D(
                                    new Point2D(0, 0),
                                    new Point2D(this.jobSchedule.X, this.jobSchedule.Y)));
                            this.drawingSurfaceSize = this.jobSchedule.X * this.jobSchedule.Y;
                        }
                        Func<ExecutedVertexInstance, Color> colorToUse;
                        if (this.colorByStagestatusToolStripMenuItem.Checked)
                        {
                            colorToUse = v => VertexStateColor(v.State);
                        }
                        else
                        {
                            colorToUse = v => this.stageColorMap[v.StageName];
                        }
                        this.jobSchedule.Draw(this.planDrawSurface, colorToUse);
                        this.planVisible = PlanVisible.Schedule;
                    }
                    else
                    {
                        this.Status("Could not compute dynamic plan", StatusKind.Error);
                        this.drawingSurfaceSize = 0.0;
                    }
                    break;
            }
        }

        /// <summary>
        /// Repaint the job schedule.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Paint event information.</param>
        private void panel_jobSchedule_Paint(object sender, PaintEventArgs e)
        {
            if (this.planDrawSurface != null)
                this.planDrawSurface.Repaint(e);
        }

        /// <summary>
        /// Panel has been resized.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void panel_jobSchedule_Resize(object sender, EventArgs e)
        {
            if (this.planDrawSurface != null && this.planDrawSurface.Resize())
                this.DrawQueryPlan();
        }

        /// <summary>
        /// User clicked on the job plan; figure out what to display.
        /// </summary>
        /// <param name="xcoord">X coordinate of click, in pixels.</param>
        /// <param name="ycoord">Y coordinate of click, in pixels.</param>
        /// <returns>If true, the event has been completely handled; else it will be handled by the default handler as well.</returns>
        private void ClickOnJobPlan(int xcoord, int ycoord)
        {
            if (this.planVisible == PlanVisible.None)
                return;

            if (this.planVisible == PlanVisible.Static)
            {
                DryadJobStaticPlan.Stage stage;
                object o = this.graphViewer.GetObjectAt(xcoord, ycoord);

                var dEdge = o as DEdge;
                if (dEdge != null)
                {
                    Msagl.Drawing.Edge edge = (dEdge).DrawingEdge;
                    DryadJobStaticPlan.Connection connection = (DryadJobStaticPlan.Connection)edge.UserData;
                    StaticPartitionedTableInformation info =
                        StaticPartitionedTableInformation.StageOutput(this.Job, this.staticPlan, connection.From, this.Status, !this.hideCancelledVerticesToolStripMenuItem.Checked);
                    this.SetTable(info);
                    return;
                }

                var dNode = o as DNode;
                if (dNode != null)
                {
                    Msagl.Drawing.Node node = dNode.DrawingNode;
                    stage = (DryadJobStaticPlan.Stage)node.UserData;
                }
                else
                {
                    this.SetNoStageOrTable("", false);
                    return;
                }

                if (stage.IsVirtual)
                {
                    if (stage.IsInput ||
                        (this.Job.Summary.Status != ClusterJobInformation.ClusterJobStatus.Running && stage.IsOutput)) // can't display output tables if the job is not ready yet
                    {
                        // some input or output tables have associated processes; if that's true, display these instead of the table.
                        DryadLinqJobStage istage = this.Job.GetStage(stage.Name);
                        if (istage != null && istage.TotalInitiatedVertices != 0)
                        {
                            this.SetStage(istage);
                        }
                        else
                        {
                            this.Status("Reading table information", StatusKind.LongOp);
                            StaticPartitionedTableInformation info = 
                                new StaticPartitionedTableInformation(this.Job.ClusterConfiguration, stage.UriType, stage.Uri, stage.Code, this.Status);
                            this.SetTable(info);
                        }
                    }
                    else if (stage.IsTee)
                    {
                        StaticPartitionedTableInformation info =
                            StaticPartitionedTableInformation.StageOutput(this.Job, this.staticPlan, stage, this.Status, !this.hideCancelledVerticesToolStripMenuItem.Checked);
                        this.SetTable(info);
                        return;
                    }
                    else
                    {
                        this.SetNoStageOrTable(stage.Name, stage.IsOutput);
                    }
                    return;
                }
                else
                {
                    DryadLinqJobStage executedstage = this.Job.GetStage(stage.Name);
                    if (executedstage != null)
                        this.SetStage(executedstage);
                }
            }
            else if (this.planVisible == PlanVisible.Dynamic)
            {
                // dynamic plan visible
                double xo, yo;
                this.planDrawSurface.ScreenToUser(xcoord, ycoord, out xo, out yo);
                GraphLayout.GraphNode node = this.dynamicPlanLayout.FindNode(xo, yo);
                this.dynamicPlanLayout.ClearSelectedNodes();
                if (node != null)
                {
                    node.Selected = true;
                    string stageName = node.Stage;
                    DryadLinqJobStage executedstage = this.Job.GetStage(stageName);
                    if (executedstage != null)
                        this.SetStage(executedstage);
                }
                else
                {
                    this.SetNoStageOrTable("", false);
                }
                this.DrawQueryPlan(); // refresh the image
            }
            else if (this.planVisible == PlanVisible.Schedule)
            {
                double xo, yo;
                this.planDrawSurface.ScreenToUser(xcoord, ycoord, out xo, out yo);
                ExecutedVertexInstance vertex = this.jobSchedule.FindVertex(xo, yo);
                this.SelectVertex(vertex);
            }
            return;
        }

        /// <summary>
        /// Move the selection to the specified vertex.
        /// </summary>
        /// <param name="vertex">Vertex to select.</param>
        private void SelectVertex(ExecutedVertexInstance vertex)
        {
            if (vertex != null)
            {
                string stageName = vertex.StageName;
                DryadLinqJobStage executedstage = this.Job.GetStage(stageName);
                if (executedstage != null && this.currentStage != executedstage && this.currentStage.Name != "All vertices")
                    this.SetStage(executedstage);

                // let us move selection to this vertex
                for (int i = 0; i < this.dataGridView_stageContents.Rows.Count; i++)
                {
                    DataGridViewRow row = this.dataGridView_stageContents.Rows[i];
                    if (row.DataBoundItem == vertex)
                    {
                        row.Selected = true;
                        this.dataGridView_stageContents.FirstDisplayedScrollingRowIndex = i;
                        break;
                    }
                }
            }
            else
            {
                this.SetNoStageOrTable("", false);
            }
        }

        /// <summary>
        /// We are not displaying either a stage or a table.
        /// <param name="name">Name of virtual stage displayed, if any.</param>
        /// <param name="table">If true we are showing a table.</param>
        /// </summary>
        private void SetNoStageOrTable(string name, bool table)
        {
            this.EnableStageFiltering(false);
            this.dataGridView_stageHeader.DataSource = null;
            this.dataGridView_stageContents.DataSource = null;
            this.textBox_stageCode.Text = null;
            if (string.IsNullOrEmpty(name))
            {
                this.label_stage.Text = "Stage/Table";
            }
            else if (!table)
            {
                this.label_stage.Text = "Stage " + name;
            }
            else
            {
                this.label_stage.Text = "Table " + name;
            }
            this.ShowingStageOrTable = KindOfStageShown.None;
        }

        /// <summary>
        /// Start displaying information about this table.
        /// </summary>
        /// <param name="tableinfo">Table information to display.</param>
        private void SetTable(StaticPartitionedTableInformation tableinfo)
        {
            this.EnableStageFiltering(false);
            this.stageHeaderData.RaiseListChangedEvents = false;

            this.currentStage = null;
            this.currentTable = tableinfo;
            this.tableHeaderData.Clear();

            if (this.ShowingStageOrTable != KindOfStageShown.Table)
            {
                this.dataGridView_stageHeader.DataSource = this.tableHeaderData;
                this.dataGridView_stageContents.DataSource = this.tablePartitionsData;
// ReSharper disable PossibleNullReferenceException
                this.dataGridView_stageContents.Columns["PartitionSize"].DefaultCellStyle.Format = "N0";
// ReSharper restore PossibleNullReferenceException
                this.SetDataGridViewColumnsSize(this.dataGridView_stageContents);
                this.ShowingStageOrTable = KindOfStageShown.Table;
            }
            if (tableinfo.Code != null)
            {
                this.textBox_stageCode.Lines = tableinfo.Code;
            }
            else
            {
                this.textBox_stageCode.Lines = null;
            }
            this.label_stage.Text = "Table: " + tableinfo.Header;

            this.tablePropertyEnumerator.Data = tableinfo;
            this.tablePropertyEnumerator.PopulateWithProperties(this.tableHeaderData);
            this.PopulateTablePartitions();

            this.stageHeaderData.RaiseListChangedEvents = true;
            this.stageHeaderData.ResetBindings();
            if (tableinfo.Error == "")
                this.Status("OK", StatusKind.OK);
        }

        /// <summary>
        /// Populate the stage data panel with table information.
        /// </summary>
        public void PopulateTablePartitions()
        {
            if (this.currentTable != null)
            {
                IEnumerable<StaticPartitionedTableInformation.StaticPartitionInformation> partitions = this.currentTable.Partitions;
                this.tablePartitionsData.RaiseListChangedEvents = false;
                this.tablePartitionsData.Clear();
                foreach (var part in partitions)
                {
                    this.tablePartitionsData.Add(part);
                }
                this.tablePartitionsData.RaiseListChangedEvents = true;
                this.tablePartitionsData.ResetBindings();
            }
        }

        /// <summary>
        /// What are we showing currently?
        /// </summary>
        private enum KindOfStageShown
        {
            None,
            Stage,
            Table
        };
        /// <summary>
        /// If true the forms are setup to display stage information, else they are setup to display table information.
        /// </summary>
        private KindOfStageShown ShowingStageOrTable { get; set; }

        private void EnableStageFiltering(bool enabled)
        {
            this.textBox_stageFilter.Text = ""; // clear filter
            this.textBox_stageFilter.Enabled = enabled;
            this.button_stageFilter.Enabled = enabled;
            this.button_clearStageFilter.Enabled = enabled;
        }

        /// <summary>
        /// Start displaying information about this stage.
        /// </summary>
        /// <param name="stage">Job stage to display.</param>
        private void SetStage(DryadLinqJobStage stage)
        {
            this.Status("Loading stage " + stage.Name + " information...", StatusKind.LongOp);
            this.EnableStageFiltering(true);
            this.stageHeaderData.RaiseListChangedEvents = false;
            this.currentStage = stage;
            this.currentTable = null;

            // stageData is populated by the selectionChanged event handler for the stageHeader
            if (this.ShowingStageOrTable != KindOfStageShown.Stage)
            {
                // if we are changing the nature of the datasource (from table to stage) we need to do some work
                this.ShowingStageOrTable = KindOfStageShown.Stage;
                this.dataGridView_stageContents.SuspendLayout();
                BindingListSortable<ExecutedVertexInstance> empty = new BindingListSortable<ExecutedVertexInstance>();
                // bind to an empty list to make the property changes fast
                this.dataGridView_stageContents.DataSource = empty;
                this.dataGridView_stageHeader.DataSource = this.stageHeaderData;
                this.SetDataGridViewColumnsSize(this.dataGridView_stageHeader);

                DataGridViewColumnCollection columns = this.dataGridView_stageContents.Columns;
                // ReSharper disable PossibleNullReferenceException
                columns["Name"].DisplayIndex = 0;
                columns["Start"].DefaultCellStyle.Format = "T";
                columns["Start"].DisplayIndex = 1;
                columns["End"].DefaultCellStyle.Format = "T";
                columns["End"].DisplayIndex = 2;
                columns["RunningTime"].DefaultCellStyle.Format = "g";
                columns["RunningTime"].DisplayIndex = 3;

                columns["DataRead"].DefaultCellStyle.Format = "N0";
                columns["DataWritten"].DefaultCellStyle.Format = "N0";

                string[] invisibleColumns =
                {
                    "IsManager", /* "ProcessIdentifier",*/ "Number", "WorkDirectory", "ErrorString",
                    "StdoutFile", "LogFilesPattern", "LogDirectory", "UniqueID", "InputChannels", "OutputChannels",
                    "CreationTime", "StartCommandTime", "VertexScheduleTime", "StageName",
                    "DataRead", "DataWritten", "ExitCode", "VertexScheduleTime", "StartCommandTime", "VertexIsCompleted"
                };
                foreach (string s in invisibleColumns)
                {
                    if (s == "DataRead" || s == "DataWritten")
                        continue;
                    columns[s].Visible = false;
                }
                columns["Version"].HeaderText = "v.";
                this.SetDataGridViewColumnsSize(this.dataGridView_stageContents);
                // bind to the actual data
                this.dataGridView_stageContents.DataSource = this.stageData;
                this.dataGridView_stageContents.ResumeLayout();
            }
            else
            {
                this.dataGridView_stageContents.Columns["DataRead"].Visible = true;
                this.dataGridView_stageContents.Columns["DataWritten"].Visible = true;
            }
            // ReSharper restore PossibleNullReferenceException

            this.stageHeaderData.Clear();
            if (this.staticPlan != null)
            {
                DryadJobStaticPlan.Stage s = this.staticPlan.GetStageByName(stage.Name);
                if (s != null)
                {
                    this.textBox_stageCode.Lines = s.Code;
                    stage.StaticVertexCount = s.Replication;
                }
                else
                {
                    this.textBox_stageCode.Lines = null;
                }
            }

            this.label_stage.Text = "Stage: " + stage.Name;
            this.stagePropertyEnumerator.Data = stage;
            this.stagePropertyEnumerator.PopulateWithProperties(this.stageHeaderData);
            this.stageHeaderData.RaiseListChangedEvents = true;
            this.stageHeaderData.ResetBindings();

            // display by default the work directory of the job manager if nothing is displayed
            if (stage.Name == "JobManager" && this.comboBox_vertexInformation.Text == "" && Environment.UserName == "jcurrey")
            {
                this.comboBox_vertexInformation.Text = "work dir";
            }

            this.Status("OK", StatusKind.OK);
        }

        /// <summary>
        /// When this flag is set, ignore an empty selection changed message.
        /// </summary>
        private bool stageDataIgnoreEmptySelectionChanged;

        /// <summary>
        /// Populate the stage data panel.
        /// </summary>
        /// <param name="filter">String describing which vertices to display.</param>
        public void PopulateStageVertices(Func<ExecutedVertexInstance, bool> filter)
        {
            bool change = true;
            int addedRows = 0;

            if (this.currentStage != null)
            {
                this.Status("Displaying " + this.currentStage.Name + " vertices...", StatusKind.LongOp);
                IEnumerable<ExecutedVertexInstance> verticesToDisplay = this.currentStage.Vertices;
                if (filter != null)
                    verticesToDisplay = verticesToDisplay.Where(filter);
                else
                    change = false;

                if (change)
                {
                    // for some reason the selectedChanged event fires twice; detect here if the first one should be ignored
                    int selectedRows = this.dataGridView_stageContents.SelectedRows.Count;

                    this.stageData.RaiseListChangedEvents = false;
                    this.stageData.Clear();
                    foreach (ExecutedVertexInstance v in verticesToDisplay)
                    {
                        this.stageData.Add(v);
                        addedRows++;
                    }
                    this.stageData.RedoSort();
                    this.stageData.RaiseListChangedEvents = true;
                    if (selectedRows > 0 && addedRows > 0)
                        this.stageDataIgnoreEmptySelectionChanged = true;
                    this.stageData.ResetBindings();
                    this.Status("Displayed " + addedRows + " vertices", StatusKind.OK);
                }
                else
                    return;
            }
        }

        /// <summary>
        /// Display information about a selected vertex in the vertex view panes.
        /// </summary>
        /// <param name="executedVertexInstance">Vertex to display.</param>
        private void DisplayVertex(ExecutedVertexInstance executedVertexInstance)
        {
            this.currentVertex = executedVertexInstance;
            this.vertexHeaderData.Clear();
            this.label_Vertex.BackColor = this.defaultBackColor;

            if (executedVertexInstance != null)
            {
                if (this.currentVertex.IsManager)
                {
                    if (!this.comboBox_vertexInformation.Items.Contains("XML Plan"))
                        this.comboBox_vertexInformation.Items.Add("XML Plan");
                    if (!this.comboBox_vertexInformation.Items.Contains("Job log"))
                        this.comboBox_vertexInformation.Items.Add("Job log");
                    if (this.comboBox_vertexInformation.Items.Contains("stdout"))
                        this.comboBox_vertexInformation.Items.Remove("stdout");
                }
                else
                {
                    if (this.comboBox_vertexInformation.Items.Contains("XML Plan"))
                        this.comboBox_vertexInformation.Items.Remove("XML Plan");
                    if (this.comboBox_vertexInformation.Items.Contains("Job log"))
                        this.comboBox_vertexInformation.Items.Remove("Job log");
                    if (!this.comboBox_vertexInformation.Items.Contains("stdout"))
                        this.comboBox_vertexInformation.Items.Add("stdout");
                }

                this.Status("Loading vertex data...", StatusKind.LongOp);
                this.textBox_find.Enabled = true;
                this.vertexPropertyEnumerator.Data = executedVertexInstance;
                this.vertexPropertyEnumerator.PopulateWithProperties(this.vertexHeaderData);
                this.label_Vertex.Text = "Vertex: " + executedVertexInstance.Name;
                this.comboBox_vertexInformation.Enabled = true;
                this.label_Vertex.BackColor = VertexStateColor(executedVertexInstance.State);

                this.vertexToolStripMenuItem.Enabled = false; //true;
            }
            else
            {
                this.vertexToolStripMenuItem.Enabled = false;

                this.textBox_find.Enabled = false; 
                this.label_Vertex.Text = "Vertex";
                this.comboBox_vertexInformation.Enabled = false;
            }
            this.vertexHeaderData.ResetBindings();
            this.ChooseVertexInformation();
            this.dataGridView_vertexHeader.ClearSelection();
            this.Status("OK", StatusKind.OK);
        }

        #region RICHTEXTBOX_MANIPULATION
        /// <summary>
        /// Name of file loaded in the rich text box.
        /// </summary>
        private IClusterResidentObject richtextBoxShownFile;

        /// <summary>
        /// Dictionary mapping the link names in the richTextBox to objects to load.
        /// </summary>
        Dictionary<string, IClusterResidentObject> linkCache;

        private class FileContents
        {
            public string fileContents;
            public string error;
            public Dictionary<string, IClusterResidentObject> links;

            public FileContents(string error)
            {
                this.fileContents = null;
                this.error = error;
                this.links = null;
            }

            public FileContents(string contents, string error, Dictionary<string, IClusterResidentObject> links)
            {
                this.fileContents = contents;
                this.error = error;
                this.links = links;
            }
        }

        /// <summary>
        /// Get the contents of a specified cluster resident object.
        /// </summary>
        /// <param name="path">Cluster object whose contents is read.</param>
        /// <param name="pattern">Pattern to filter contents, for folders.</param>
        /// <returns>The file contents.</returns>
        /// <param name="manager">Communication manager.</param>
        private static FileContents GetContents(CommManager manager, IClusterResidentObject path, string pattern)
        {
            if (path == null)
            {
                return new FileContents("Null path");
            }

            StringBuilder output = new StringBuilder();
            
            Dictionary<string, IClusterResidentObject> linkCache = new Dictionary<string, IClusterResidentObject>();
            linkCache.Add(path.ToString(), path); 

            string error = (path.RepresentsAFolder ? "Folder " : "") + path;
            if (path.Exception != null)
            {
                error += " [Error accessing: " + path.Exception.Message + "]";
                return new FileContents(error);
            }

            if (path.RepresentsAFolder)
            {
                IEnumerable<IClusterResidentObject> dirs = path.GetFilesAndFolders(pattern);
                int displayed = 0;
                foreach (IClusterResidentObject d in dirs)
                {
                    manager.Token.ThrowIfCancellationRequested();
                    if (d.Exception != null)
                    {
                        error += " [Error " + d.Exception.Message + "]";
                        return new FileContents(error);
                    }
                    if (d.RepresentsAFolder)
                    {
                        string dirdisplay = string.Format("{0:u} {1,16} file://{2}", d.CreationTime, "d", d.Name);
                        output.AppendLine(dirdisplay);
                    }
                    else
                    {
                        string filedisplay = string.Format("{0:u} {1,16:N0} file://{2}", d.CreationTime, d.Size, d.Name);
                        output.AppendLine(filedisplay);
                    }
                    linkCache.Add("file://" + d.Name, d);
                    displayed++;
                }

                if (displayed == 0)
                    error += "[empty]";
                return new FileContents(output.ToString(), error, linkCache);
            }
            else
            {
                manager.Status("Extracting contents of " + path, StatusKind.LongOp);
                ISharedStreamReader sr = path.GetStream();
                if (sr.Exception != null)
                {
                    error += " [Error " + sr.Exception.Message + "]";
                    return new FileContents(error);
                }
                else
                {
                    if (path.Size == 0)
                        error += "[empty]";
                    var contents = sr.ReadToEnd(manager.Token);
                    return new FileContents(contents, error, linkCache);
                }
            }
        }

        /// <summary>
        /// Display the contents of a cluster resident object.
        /// </summary>
        /// <param name="path">Object reference.</param>
        /// <param name="pattern">Pattern to match for its childen.</param>
        /// <returns>True if the display succeeded.</returns>
        private void DisplayContents1(IClusterResidentObject path, string pattern)
        {
            var item = new BackgroundWorkItem<FileContents>(
                m => GetContents(m, path, pattern),
                this.ShowContents,
                "Read file");
            this.Queue(item);
        }

        /// <summary>
        /// Show the contents of a cluster-resident object.
        /// </summary>
        /// <param name="contents">Contents of the object to display, and error message.</param>
        /// <param name="cancelled">If true the work was cancelled.</param>
        private void ShowContents(bool cancelled, FileContents contents)
        {
            if (cancelled) return;

            if (contents.error != null)
                this.label_title.Text = contents.error;
            else
                this.label_title.Text = "";
            if (contents.fileContents != null)
                this.richTextBox_file.Text = contents.fileContents;
            this.linkCache = contents.links;
            this.Status("OK", StatusKind.OK);
        }

        /// <summary>
        /// User pressed the 'filter' button; restrict the view to lines containing the searched expression.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_filter_Click(object sender, EventArgs e)
        {
            IEnumerable<string> newcontents = this.richTextBox_file.Lines.Where(l => l.Contains(this.textBox_find.Text));
            this.richTextBox_file.Lines = newcontents.ToArray();
            this.richtextBoxShownFile = null; // we are only displaying a subset of the file
        }

        /// <summary>
        /// Load the file in the log viewer.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void loadFileInViewerToolStripMenuItem_Click(object sender, EventArgs e)
        {
            LogViewer viewer = new LogViewer();
            viewer.Show();
            viewer.LoadFile(this.richtextBoxShownFile);
        }

        /// <summary>
        /// Load the displayed file in a text editor.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void loadFileInEditorToolStripMenuItem_Click(object sender, EventArgs e)
        {
            string pathToDisplay;
            if (!(this.richtextBoxShownFile is UNCFile))
            {
                pathToDisplay = Path.GetRandomFileName();
                this.Status("Saving contents to file " + pathToDisplay, StatusKind.LongOp);
                this.richTextBox_file.SaveFile(pathToDisplay, RichTextBoxStreamType.PlainText);
            }
            else
            {
                pathToDisplay = (this.richtextBoxShownFile as UNCFile).Pathname.ToString();
            }

            string editor = Environment.GetEnvironmentVariable("EDITOR");
            if (editor == null)
                editor = "notepad";

            this.Status("Loading " + pathToDisplay + " in editor", StatusKind.LongOp);
            Tools.Utilities.RunProcess(editor, null, false, false, false, false, pathToDisplay);
        }

        /// <summary>
        /// Choose which vertex information to display.
        /// </summary>
        private void ChooseVertexInformation()
        {
            this.richTextBox_file.Clear();
            this.StopFind(false);
            if (this.currentVertex != null)
            {
                switch (this.comboBox_vertexInformation.Text)
                {
                    case "nothing":
                        this.richTextBox_file.Text = "";
                        this.richtextBoxShownFile = null;
                        break;
                    case "error":
                        this.richTextBox_file.Text = this.currentVertex.ErrorString;
                        break;
                    case "stdout":
                    case "Job log":
                        {
                            // standard output
                            IClusterResidentObject path = this.Job.ClusterConfiguration.ProcessStdoutFile(
                                this.currentVertex.ProcessIdentifier,
                                this.currentVertex.VertexIsCompleted,
                                this.currentVertex.Machine,
                                this.Job.Summary);
                            if (path != null)
                                this.DisplayContents1(path, "*");
                            break;
                        }
                    case "stderr":
                        {
                            IClusterResidentObject path = this.Job.ClusterConfiguration.ProcessStderrFile(
                                this.currentVertex.ProcessIdentifier,
                                this.currentVertex.VertexIsCompleted,
                                this.currentVertex.Machine,
                                this.Job.Summary);
                            this.DisplayContents1(path, "*");
                            break;
                        }
                    case "XML Plan":
                        {
                            if (!this.currentVertex.IsManager)
                                throw new InvalidOperationException("Cannot show XML plan if the vertex is not the job manager");
                            IClusterResidentObject path = this.Job.ClusterConfiguration.JobQueryPlan(this.Job.Summary);
                            if (path != null)
                                this.DisplayContents1(path, "*");
                            break;
                        }
                    case "inputs":
                        {
                            if (this.currentVertex.IsManager)
                            {
                                this.label_title.Text = "Job manager does not have channels";
                            }
                            else
                            {
                                this.label_title.Text = "Inputs";
                                this.Status("Discovering vertex channel information", StatusKind.LongOp);
                                // TODO: this should run in the background
                                CommManager manager = new CommManager(this.Status, this.UpdateProgress, new CancellationTokenSource().Token);
                                bool found = this.currentVertex.DiscoverChannels(true, false, false, manager);
                                if (found)
                                {
                                    this.richTextBox_file.SuspendLayout();
                                    StringBuilder builder = new StringBuilder();
                                    foreach (ChannelEndpointDescription endpoint in this.currentVertex.InputChannels.Values)
                                    {
                                        builder.AppendLine(endpoint.ToString());
                                    }
                                    this.richTextBox_file.Text = builder.ToString();
                                    this.richTextBox_file.ResumeLayout();
                                }
                                else
                                {
                                    this.Status("Failed to discover channel information", StatusKind.Error);
                                }
                            }
                            break;
                        }

                    case "outputs":
                        {
                            if (this.currentVertex.IsManager)
                            {
                                this.label_title.Text = "Job manager does not have channels";
                            }
                            else
                            {
                                this.label_title.Text = "Outputs";
                                this.Status("Discovering vertex channel information", StatusKind.LongOp);
                                // TODO: this should run in the background
                                CommManager manager = new CommManager(this.Status, this.UpdateProgress, new CancellationTokenSource().Token);
                                bool found = this.currentVertex.DiscoverChannels(false, true, false, manager);
                                if (found)
                                {
                                    this.richTextBox_file.SuspendLayout();
                                    foreach (ChannelEndpointDescription endpoint in this.currentVertex.OutputChannels.Values)
                                    {
                                        this.richTextBox_file.AppendText(endpoint.ToString());
                                        this.richTextBox_file.AppendText(Environment.NewLine);
                                    }
                                    this.richTextBox_file.ResumeLayout();
                                }
                                else
                                {
                                    this.Status("Failed to discover channel information", StatusKind.Error);
                                }
                            }
                            break;
                        }

                    case "work dir":
                        {
                            // display contents of work directory
                            IClusterResidentObject path = this.Job.ClusterConfiguration.ProcessWorkDirectory(this.currentVertex.ProcessIdentifier, this.currentVertex.VertexIsCompleted, this.currentVertex.Machine, this.Job.Summary);
                            this.DisplayContents1(path, "*");
                            break;
                        }
                    case "logs":
                        {
                            IClusterResidentObject path = this.Job.ClusterConfiguration.ProcessLogDirectory(this.currentVertex.ProcessIdentifier, this.currentVertex.VertexIsCompleted, this.currentVertex.Machine, this.Job.Summary);
                            if (path != null)
                            {
                                string pattern;
                                if (this.currentVertex.IsManager)
                                {
                                    pattern = this.Job.ClusterConfiguration.JMLogFilesPattern(false, this.Job.Summary);
                                }
                                else
                                {
                                    pattern = this.Job.ClusterConfiguration.VertexLogFilesPattern(false, this.Job.Summary);
                                }

                                this.DisplayContents1(path, pattern);
                            }
                            break;
                        }
                }
            }
        }

        /// <summary>
        /// The user clicked in a link in the rich text box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void richTextBox_file_LinkClicked(object sender, LinkClickedEventArgs e)
        {
            string path = e.LinkText;
            IClusterResidentObject value;

            if (this.linkCache.ContainsKey(path))
            {
                value = this.linkCache[path];
            }
            else {
                if (path.StartsWith("file://"))
                {
                    path = path.Substring(7);
                    value = new UNCFile(this.Job.ClusterConfiguration, this.Job.Summary, new UNCPathname(path), false);
                }
                else
                {
                    value = new UNCFile(this.Job.ClusterConfiguration, this.Job.Summary, new UNCPathname(path), false);
                }
            }

            if (Control.ModifierKeys == Keys.Control)
            {
                Tools.Utilities.RunProcess("explorer.exe", null, false, false, false, false, value.ToString());
            }
            else if (Control.ModifierKeys == Keys.Alt && this.comboBox_vertexInformation.Text == "inputs")
            {
                DryadProcessIdentifier proc = this.Job.ClusterConfiguration.ProcessFromInputFile(value, this.Job.Summary);
                ExecutedVertexInstance vertex = this.Job.FindVertex(proc);
                this.SelectVertex(vertex);
            }
            else
            {
                this.comboBox_vertexInformation.Text = "";
                this.DisplayContents1(value, "*");
            }
        }

        /// <summary>
        /// Position where text was found.
        /// </summary>
        private int findPosition;
        private int findMatches;

        /// <summary>
        /// When the text changes search for the text within the currently displayed file.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void textBox1_TextChanged(object sender, EventArgs e)
        {
            string textToFind = this.textBox_find.Text;
            if (textToFind == "")
                this.StopFind(true);
            else
            {
                this.StartFind();
                this.findMatches = this.FindAllMatches(textToFind);
                this.ShowMatches();

                if (this.findMatches > 0)
                {
                    if (this.findPosition < 0)
                    {
                        // start at the first visible position again
                        Point pos = new Point(0, 0);
                        this.findPosition = this.richTextBox_file.GetCharIndexFromPosition(pos);
                    }

                    this.findPosition = this.Find(textToFind, this.findPosition, false);
                }
            }
        }

        /// <summary>
        /// Show the number of matches for the search.
        /// </summary>
        private void ShowMatches()
        {
            this.label_matches.Text =
                    this.findMatches == 0 ? "No matches" : (this.findMatches > 100 ? "> 100 matches" : this.findMatches + " matches");
        }

        /// <summary>
        /// Find all matches of a text; return the match count; hilight the visible ones.
        /// Stop after at 100 matches.
        /// </summary>
        /// <param name="text">Text to find.</param>
        /// <returns>The count of matches found, or 101 if more than 100.</returns>
        private int FindAllMatches(string text)
        {
            int crtpos = 0;
            int i = 0;
            string textinbox = this.richTextBox_file.Text;
            for (; i < 101; i++)
            {
                int index = textinbox.IndexOf(text, crtpos);
                if (index < 0)
                    break;
                crtpos = index + 1;
            }
            return i;
        }

        /// <summary>
        /// Search for a specified text in the richtext view.
        /// </summary>
        /// <param name="text">Text to search for.</param>
        /// <param name="startAt">Position where to start searching.</param>
        /// <returns>The position in the box where the first instance of the text is found, or -1.</returns>
        /// <param name="reverse">If true, search in reverse.</param>
        private int Find(string text, int startAt, bool reverse)
        {
            RichTextBoxFinds options = reverse ? RichTextBoxFinds.Reverse : RichTextBoxFinds.None;
            options |= RichTextBoxFinds.MatchCase;
            if (this.richTextBox_file.Text.Length <= startAt)
            {
                return -1;
            }

            int start = reverse ? 0 : startAt;
            int end = reverse ? startAt : -1;

            int pos = this.richTextBox_file.Find(text, start, end, options);
            if (pos >= 0)
            {
                this.richTextBox_file.Select(pos, text.Length);
                this.ShowMatches();
            }
            else
            {
                // hide selection
                this.label_matches.Text = "Last match";
                this.richTextBox_file.Select(0, 0);
            }
            return pos;
        }

        /// <summary>
        /// Used wants to find the previous match.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_findPrev_Click(object sender, EventArgs e)
        {
            this.findPosition = this.Find(this.textBox_find.Text, this.findPosition, true);
        }

        /// <summary>
        /// Used wants to find the next match.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_findNext_Click(object sender, EventArgs e)
        {
            this.findPosition = this.Find(this.textBox_find.Text, this.findPosition + 1, false);
        }

        /// <summary>
        /// Start finding text.
        /// </summary>
        private void StartFind()
        {
            this.button_findNext.Enabled = true;
            this.button_findPrev.Enabled = true;
            this.button_clearFind.Enabled = true;
            this.button_filter.Enabled = true;
        }

        /// <summary>
        /// Stop finding text.
        /// </summary>
        /// <param name="clear">If true clear the find box.</param>
        private void StopFind(bool clear)
        {
            this.richTextBox_file.Select(0, 0);
            this.findPosition = 0;
            if (clear)
                this.textBox_find.Text = "";
            this.label_matches.Text = "";
            if (string.IsNullOrEmpty(this.textBox_find.Text))
            {
                this.button_findNext.Enabled = false;
                this.button_findPrev.Enabled = false;
                this.button_clearFind.Enabled = false;
                this.button_filter.Enabled = false;
            }
        }

        /// <summary>
        /// User wants to clear the find box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_clearFind_Click(object sender, EventArgs e)
        {
            this.StopFind(true);
        }

        /// <summary>
        /// The user pressed a button in the text box. 
        /// Handle the return key specially.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Event describing the key pressed.</param>
        private void textBox_find_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == 13)
            {
                // same as pressing the 'Next' button
                this.findPosition = this.Find(this.textBox_find.Text, this.findPosition + 1, false);
            }
        }

        /// <summary>
        /// The user moved the cursor in the richtextbox.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void richTextBox_file_SelectionChanged(object sender, EventArgs e)
        {
            this.findPosition = this.richTextBox_file.SelectionStart;
        }
        #endregion

        /// <summary>
        /// Event intercepted to format a cell in the stage contents.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Cell information.</param>
        private void dataGridView_stageContents_CellFormatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            object item = this.dataGridView_stageContents.Rows[e.RowIndex].DataBoundItem;
            var v = item as ExecutedVertexInstance;
            if (v != null)
            {
                e.CellStyle.BackColor = VertexStateColor(v.State);

                if (e.Value is DateTime && (DateTime)e.Value == DateTime.MinValue)
                    e.Value = "";
                else if (e.Value is TimeSpan && (TimeSpan)e.Value == TimeSpan.MinValue)
                    e.Value = "";
            }
        }

        /// <summary>
        /// Display a status message.
        /// </summary>
        /// <param name="message">Message to display.</param>
        /// <param name="statusKind">Message severity.</param>
        public void Status(string message, StatusKind statusKind)
        {
            this.status.Status(message, statusKind);
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
                try
                {
                    if (this.toolStripProgressBar != null)
                        // there could be overflows here
                        this.toolStripProgressBar.Value = Math.Min(value, 100);
                }
// ReSharper disable EmptyGeneralCatchClause
                catch
// ReSharper restore EmptyGeneralCatchClause
                { }
            }
        }

        /// <summary>
        /// A new vertex has been selected.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void dataGridView_stageContents_SelectionChanged(object sender, EventArgs e)
        {
            //Trace.TraceInformation("Stage contents selection changed");
            var rows = this.dataGridView_stageContents.SelectedRows;
            if (rows.Count != 1)
            {
                if (this.stageDataIgnoreEmptySelectionChanged)
                {
                    // Ignore this event, it should have not been fired
                    this.stageDataIgnoreEmptySelectionChanged = false;
                    return;
                }

                this.DisplayVertex(null);
            }
            else
            {
                if (this.currentStage != null)
                {
                    // only display it if it is a vertex
                    this.DisplayVertex((ExecutedVertexInstance)rows[0].DataBoundItem);
                }
                else
                    this.DisplayVertex(null);
            }            
        }

        /// <summary>
        /// Grid view has been sorted.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void dataGridView_stageContents_Sorted(object sender, EventArgs e)
        {
            if (this.currentVertex != null)
            {
                this.SelectVertex(this.currentVertex);
            }
        }

        /// <summary>
        /// Close the job browser.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void closeToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        /// <summary>
        /// Refresh the view of the job with the latest information.
        /// </summary>
        private void RefreshDisplay()
        {
            this.Status("Refreshing...", StatusKind.LongOp);
            this.Job.InvalidateCaches();
            this.stageColorMap = null; // force recomputation
            this.RefreshJob();
        }

        /// <summary>
        /// User wants to refresh the job information displayed.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void refreshToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.RefreshDisplay();
        }

        /// <summary>
        /// The user is closing the form; save the application settings.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void JobBrowser_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.refreshTimer.Stop();
            this.queue.Stop();
            this.formSettings.WarnedAboutDebugging = this.WarnedAboutDebugging;
            this.formSettings.WarnedAboutProfiling = this.WarnedAboutProfiling;
            this.formSettings.Location = this.Location;
            this.formSettings.Size = this.Size;
            this.formSettings.JobSplitterPosition = this.splitContainer_jobAndRest.SplitterDistance;
            this.formSettings.StageSplitterPosition = this.splitContainer_stageAndVertex.SplitterDistance;
            this.formSettings.StageHeaderSplitterPosition = this.splitContainer_stageData.SplitterDistance;
            this.formSettings.VertexHeaderSplitterPosition = this.splitContainer_vertexData.SplitterDistance;
            this.formSettings.JobHeaderSplitterPosition = this.splitContainer_jobData.SplitterDistance;
            this.formSettings.CodeSplitterPosition = this.splitContainer_stageData1.SplitterDistance;
            this.formSettings.MaximizeWindow = this.WindowState == FormWindowState.Maximized;
            this.formSettings.AutoRefresh = this.checkBox_refresh.Checked;
            this.formSettings.HideCancelledVertices = this.HideCancelledVertices;
            this.formSettings.Save();
        }

        /// <summary>
        /// Form is being loaded: restore settings if saved.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void JobBrowser_Load(object sender, EventArgs e)
        {
            this.formSettings = new JobBrowserSettings();

            Rectangle rect = System.Windows.Forms.Screen.PrimaryScreen.Bounds;
            // set location only if it is inside
            if (rect.Contains(this.formSettings.Location))
                this.Location = this.formSettings.Location;
            bool maximized = this.formSettings.MaximizeWindow;
            if (maximized)
                this.WindowState = FormWindowState.Maximized;
            else
            {
                // then we care about the size
                this.Size = new Size(
                    Math.Max(this.formSettings.Size.Width, this.MinimumSize.Width),
                    Math.Max(this.formSettings.Size.Height, this.MinimumSize.Height));
            }

            this.WarnedAboutDebugging = this.formSettings.WarnedAboutDebugging;
            this.WarnedAboutProfiling = this.formSettings.WarnedAboutProfiling;
            this.HideCancelledVertices = this.formSettings.HideCancelledVertices;
            try
            {
                this.splitContainer_jobAndRest.SplitterDistance = this.formSettings.JobSplitterPosition;
                this.splitContainer_stageAndVertex.SplitterDistance = this.formSettings.StageSplitterPosition;
                this.splitContainer_stageData.SplitterDistance = this.formSettings.StageHeaderSplitterPosition;
                this.splitContainer_vertexData.SplitterDistance = this.formSettings.VertexHeaderSplitterPosition;
                this.splitContainer_jobData.SplitterDistance = this.formSettings.JobHeaderSplitterPosition;
                this.splitContainer_stageData1.SplitterDistance = this.formSettings.CodeSplitterPosition;
            }
// ReSharper disable EmptyGeneralCatchClause
            catch
// ReSharper restore EmptyGeneralCatchClause
            {
                // ignore exceptions during form loading
            }
            this.checkBox_refresh.Checked = this.formSettings.AutoRefresh;
        }

        private Func<ExecutedVertexInstance, bool> ConstructVertexFilter(string cellContent)
        {
            Func<ExecutedVertexInstance, bool> result = null;
            switch (cellContent)
            {
                case "FailedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Failed;
                    break;
                case "CancelledVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Cancelled;
                    break;
                case "StaticVertexCount":
                case "TotalInitiatedVertices":
                    // total does not include abandoned
                    if (this.HideCancelledVertices)
                        result = v => (v.State != ExecutedVertexInstance.VertexState.Abandoned && v.State != ExecutedVertexInstance.VertexState.Cancelled && v.State != ExecutedVertexInstance.VertexState.Revoked);
                    else
                        result = v => v.State != ExecutedVertexInstance.VertexState.Abandoned;
                    break;
                case "StartedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Started;
                    break;
                case "CreatedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Created;
                    break;
                case "SuccessfulVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Successful;
                    break;
                case "InvalidatedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Invalidated;
                    break;
                case "RevokedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Revoked;
                    break;
                case "AbandonedVertices":
                    result = v => v.State == ExecutedVertexInstance.VertexState.Abandoned;
                    break;
// ReSharper disable RedundantEmptyDefaultSwitchBranch
                default:
                    break;
// ReSharper restore RedundantEmptyDefaultSwitchBranch
            }
            return result;
        }

        private void ShowSelecteddVertices()
        {
            // find out the content of the first column
            if (this.dataGridView_stageHeader.SelectedRows.Count != 1)
                return;

            if (this.currentStage != null)
            {
                DataGridViewRow row = this.dataGridView_stageHeader.SelectedRows[0];
                DataGridViewCell cellName = row.Cells["ObjectName"];
                string cellContent = cellName.Value.ToString();
                Func<ExecutedVertexInstance, bool> filter = this.ConstructVertexFilter(cellContent);
                this.PopulateStageVertices(filter);
            }
        }

        /// <summary>
        /// User selected a different row from the stage header.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void dataGridView_stageHeader_SelectionChanged(object sender, EventArgs e)
        {
            // clear the filter selection
            this.textBox_stageFilter.Text = "";
            this.ShowSelecteddVertices();
        }

        /// <summary>
        /// The user modified the auto-refresh box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void checkBox_refresh_CheckedChanged(object sender, EventArgs e)
        {
            if (this.checkBox_refresh.Checked)
                this.refreshTimer.Start();
            else
                this.refreshTimer.Stop();
        }

        /// <summary>
        /// Form has been shown, load the actual data now.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void JobBrowser_Shown(object sender, EventArgs e)
        {
            this.RefreshJob();
        }

        /// <summary>
        /// Refresh the job details.
        /// </summary>
        private void RefreshJob()
        {
            DryadLinqJobInfo job = this.Job;
            DateTime start = DateTime.Now;
            var item = new BackgroundWorkItem<TimeSpan>(
                m =>
                {
                    job.CollectEssentialInformation(m);
                    return DateTime.Now - start;
                },
                this.JobInfoLoaded,
                "refreshJob");
            this.Queue(item);
        }

        /// <summary>
        /// Called after a job has been loaded.
        /// </summary>
        /// <param name="cancelled">If true the loading has been cancelled.</param>
        /// <param name="loadTime">Time to load job.</param>
        private void JobInfoLoaded(bool cancelled, TimeSpan loadTime)
        {
            if (cancelled) return;

            this.LoadJobCompleted(loadTime);
            string s = this.currentStage != null ? this.currentStage.Name : null;
            if (this.doingStartup && string.IsNullOrEmpty(s))
            {
                s = "JobManager";
                this.doingStartup = false;
            }
            if (!string.IsNullOrEmpty(s))
            {
                DryadLinqJobStage executedstage = this.Job.GetStage(s);
                if (executedstage != null)
                    this.SetStage(executedstage);
            }
            else if (this.currentTable != null)
            {
                this.SetTable(this.currentTable.Refresh(this.Job, this.Status, !this.hideCancelledVerticesToolStripMenuItem.Checked));
            }
        }

        #region MOUSE_DYNAMIC_VIEWS
        /// <summary>
        /// Move the plan view (dynamic plan only).
        /// </summary>
        /// <param name="sender">Menu item which caused the move.</param>
        /// <param name="e">Unused.</param>
        private void moveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (this.planVisible != PlanVisible.Dynamic && this.planVisible != PlanVisible.Schedule)
                return;

            double deltaX = 0, deltaY = 0;
            const double percent = 20; // amount of movement

            double maxX, maxY;
            if (this.planVisible == PlanVisible.Dynamic)
            {
                maxX = this.dynamicPlanLayout.Size.X;
                maxY = this.dynamicPlanLayout.Size.Y;
            }
            else
            {
                maxX = this.jobSchedule.X;
                maxY = this.jobSchedule.Y;
            }

            if (sender == this.rightToolStripMenuItem) {
                deltaX = percent * this.planDrawSurface.Width / 100;
                if (deltaX > maxX - this.planDrawSurface.xDrawMax)
                    deltaX = maxX - this.planDrawSurface.xDrawMax;
            }
            else if (sender == this.leftToolStripMenuItem) 
            {
                deltaX = - percent * this.planDrawSurface.Width / 100;
                if (- deltaX > this.planDrawSurface.xDrawMin)
                    deltaX = -this.planDrawSurface.xDrawMin;
            }
            else if (sender == this.upToolStripMenuItem) {
                deltaY = percent * this.planDrawSurface.Height / 100;
                if (deltaY > maxY - this.planDrawSurface.yDrawMax)
                    deltaY = maxY - this.planDrawSurface.yDrawMax;
            }
            else if (sender == this.downToolStripMenuItem)
            {
                deltaY = -percent * this.planDrawSurface.Height / 100;
                if (- deltaY > this.planDrawSurface.yDrawMin)
                    deltaY = -this.planDrawSurface.yDrawMin;
            }

            double leftX = this.planDrawSurface.xDrawMin + deltaX;
            double rightX = this.planDrawSurface.xDrawMax + deltaX;
            double topY = this.planDrawSurface.yDrawMax + deltaY;
            double bottomY = this.planDrawSurface.yDrawMin + deltaY;
            Rectangle2D zoomedArea = new Rectangle2D(leftX, topY, rightX, bottomY);
            this.ZoomDynamicPlanTo(zoomedArea);
        }

        /// <summary>
        /// Show only this bounding box of the dynamic plan.
        /// </summary>
        /// <param name="boundingBox">Box to zoom to.</param>
        private void ZoomDynamicPlanTo(Rectangle2D boundingBox)
        {
            boundingBox = boundingBox.Normalize();
            if (boundingBox.Degenerate())
                return;

            if (this.drawingSurfaceSize > 10000 * boundingBox.Area())
            {
                this.Status("Cannot zoom that much", StatusKind.Error);
                return;
            }

            this.planDrawSurface.SetDrawingArea(boundingBox);
            this.DrawQueryPlan();
            this.Status("OK", StatusKind.OK);
        }

        /// <summary>
        /// Mouse double click in dynamic plan panel; zoom around mouse position.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        void panel_jobSchedule_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            if (e.Button != MouseButtons.Left)
                return;

            const double percent = 20;

            // size of resulting view
            double width = this.planDrawSurface.Width * (100 - 2 * percent) / 100;
            double height = this.planDrawSurface.Height * (100 - 2 * percent) / 100;

            double mouseX, mouseY;
            this.planDrawSurface.ScreenToUser(e.X, e.Y, out mouseX, out mouseY);

            // center view around mouse position
            double leftX = mouseX - width / 2;
            double rightX = mouseX + width / 2;
            double topY = mouseY - height / 2;
            double bottomY = mouseY + height / 2;

            // adjust if out of screen
            if (leftX < this.planDrawSurface.xDrawMin)
            {
                rightX += this.planDrawSurface.xDrawMin - leftX;
                leftX = this.planDrawSurface.xDrawMin;
            }
            else if (rightX > this.planDrawSurface.xDrawMax)
            {
                leftX -= rightX - this.planDrawSurface.xDrawMax;
                rightX = this.planDrawSurface.xDrawMax;
            }

            if (topY < this.planDrawSurface.yDrawMin)
            {
                bottomY += this.planDrawSurface.yDrawMin - topY;
                topY = this.planDrawSurface.yDrawMin;
            }
            else if (bottomY > this.planDrawSurface.yDrawMax)
            {
                topY -= bottomY - this.planDrawSurface.yDrawMax;
                bottomY = this.planDrawSurface.yDrawMax;
            }

            Rectangle2D zoomedArea = new Rectangle2D(leftX, topY, rightX, bottomY);
            this.ZoomDynamicPlanTo(zoomedArea);
        }
        
        /// <summary>
        /// Zoom to display only the selected rectangle.  Only for the dynamic plan.
        /// </summary>
        /// <param name="startPt">Start point in job plan panel (coordinates in pixels).</param>
        /// <param name="endPt">End point in job plan panel (coordinates in pixels).</param>
        private void ZoomDynamicPlan(Point startPt, Point endPt)
        {
            if (this.planVisible == PlanVisible.None)
                return;

            double leftX, topY, rightX, bottomY;
            this.planDrawSurface.ScreenToUser(startPt.X, startPt.Y, out leftX, out topY);
            this.planDrawSurface.ScreenToUser(endPt.X, endPt.Y, out rightX, out bottomY);
            Rectangle2D zoomedArea = new Rectangle2D(leftX, topY, rightX, bottomY);
            this.ZoomDynamicPlanTo(zoomedArea);
        }

        /// <summary>
        /// Zoom in/out the dynamic plan by a fixed amount.
        /// <param name="zoomin">Zoom in or out?</param>
        /// </summary>
        private void ZoomDynamicPlan(bool zoomin)
        {
            if (this.planVisible == PlanVisible.None)
                return;

            double percent = zoomin ? 20 : -20; // amount of zooming
            double leftX = this.planDrawSurface.xDrawMin + percent * this.planDrawSurface.Width / 100;
            double rightX = this.planDrawSurface.xDrawMax - percent * this.planDrawSurface.Width / 100;
            double topY = this.planDrawSurface.yDrawMin + percent * this.planDrawSurface.Height / 100;
            double bottomY = this.planDrawSurface.yDrawMax - percent * this.planDrawSurface.Height / 100;
            Rectangle2D zoomedArea = new Rectangle2D(leftX, topY, rightX, bottomY);
            this.ZoomDynamicPlanTo(zoomedArea);
        }

        /// <summary>
        /// Mouse has been released in the job schedule area.
        /// </summary>
        /// <param name="senderControl">Control reporting the event.</param>
        /// <param name="e">Mouse event.</param>
        private void panel_jobSchedule_MouseUp(object senderControl, MouseEventArgs e)
        {
            if (e.Button != MouseButtons.Left)
                return;

            this.mouseIsHeld = false;
            if (!this.draggingMouse)
            {
                this.ClickOnJobPlan(e.X, e.Y);
                return;
            }

            Control sender = (Control)senderControl;
            this.draggingMouse = false;
            Rectangle rubberBand = new Rectangle(
                sender.PointToScreen(startPoint),
                new Size(sender.PointToScreen(endPoint) - new Size(sender.PointToScreen(startPoint)))
                );
            ControlPaint.DrawReversibleFrame(rubberBand, Color.Black, FrameStyle.Dashed);

            // zoom into the visible area
            this.ZoomDynamicPlan(this.startPoint, this.endPoint);
        }

        /// <summary>
        /// Mouse has been pressed in the job schedule panel.
        /// </summary>
        /// <param name="senderControl">Control reporting the moving event.</param>
        /// <param name="e">Event.</param>
        private void panel_jobSchedule_MouseDown(object senderControl, MouseEventArgs e)
        {
            if (e.Button != System.Windows.Forms.MouseButtons.Left)
                return;

            this.mouseIsHeld = true;
            if (this.draggingMouse)
                return;

            this.startPoint = new Point(e.X, e.Y);
            this.endPoint = this.startPoint;
        }

        /// <summary>
        /// Mouse has been moved.
        /// </summary>
        /// <param name="senderControl">Control reporting the moving event.</param>
        /// <param name="e">Event.</param>
        private void panel_jobSchedule_MouseMove(object senderControl, MouseEventArgs e)
        {
            Control sender = (Control)senderControl;
            if (this.mouseIsHeld)
            {
                // mouse has been pressed
                this.draggingMouse = true;
            }
            else
            {
                this.draggingMouse = false;
                return;
            }

            Rectangle rubberBand = new Rectangle(
                sender.PointToScreen(startPoint),
                new Size(sender.PointToScreen(endPoint) - new Size(sender.PointToScreen(startPoint)))
                );
            ControlPaint.DrawReversibleFrame(rubberBand, Color.Black, FrameStyle.Dashed);
            endPoint = new Point(e.X, e.Y);
            rubberBand = new Rectangle(
                sender.PointToScreen(startPoint),
                new Size(sender.PointToScreen(endPoint) - new Size(sender.PointToScreen(startPoint)))
                );
            ControlPaint.DrawReversibleFrame(rubberBand, Color.Black, FrameStyle.Dashed);
        }
        #endregion

        /// <summary>
        /// Find mentions of the current vertex in the JM stdout.
        /// </summary>
        private void FindJMStdoutMentions()
        {
            LogViewer lv = new LogViewer(true, "JM on " + this.currentVertex);
            lv.Show();

            var item = new BackgroundWorkItem<bool>(
                m => ScanJMStdout(this.currentVertex, this.Job.ManagerVertex.StdoutFile, lv),
                (c, b) => { },
                "findStdout");
            this.Queue(item);
        }

        /// <summary>
        /// The user changed the information to display about the vertex.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void comboBox_vertexInformation_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.ChooseVertexInformation();
        }

        /// <summary>
        /// Zoom in.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void zoomInToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (this.planVisible == PlanVisible.Static)
            {
                this.ZoomStaticPlan(true);
            }
            else if (this.planVisible == PlanVisible.Dynamic || this.planVisible == PlanVisible.Schedule)
            {
                this.ZoomDynamicPlan(true);
            }
        }

        /// <summary>
        /// Zoom out.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void zoomOutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (this.planVisible == PlanVisible.Static)
            {
                this.ZoomStaticPlan(false);
            }
            else if (this.planVisible == PlanVisible.Dynamic || this.planVisible == PlanVisible.Schedule)
            {
                this.ZoomDynamicPlan(false);
            }
        }

        /// <summary>
        /// Diagnose the failure of the job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void diagnoseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            CommManager manager = new CommManager(this.Status, this.UpdateProgress, new CancellationToken());
            JobFailureDiagnosis diagnosis = JobFailureDiagnosis.CreateJobFailureDiagnosis(this.Job, this.staticPlan, manager);
            DiagnosisLog log = diagnosis.Diagnose();
            this.DisplayDiagnosis(log);
        }

        /// <summary>
        /// Display the result of the diagnosis for the user.
        /// </summary>
        /// <param name="log">Log of the diagnosis.</param>
        private void DisplayDiagnosis(DiagnosisLog log)
        {
            DiagnosisResult result = new DiagnosisResult(this.Job, this.Job.Summary, log);
            result.Show();
        }

        /// <summary>
        /// Debug/profile the managed/unmanaged code of selected vertex.
        /// </summary>
        /// <param name="sender">Used to detect whether to debug or profile and whether to debug unmanaged code</param>
        /// <param name="e">Unused.</param>
        private void performLocalVertexDebuggingOrProfiling(object sender, EventArgs e)
        {
            if (dataGridView_stageContents.SelectedRows.Count != 1)
            {
                this.Status("Need to select one vertex to debug/profile", StatusKind.Error);
                return;
            }

            ExecutedVertexInstance selectedVertex = dataGridView_stageContents.SelectedRows[0].DataBoundItem as ExecutedVertexInstance;
            if (selectedVertex == null)
            {
                this.Status("The vertex selected is null!", StatusKind.Error);
                return;
            }
            
            if (selectedVertex.IsManager)
            {
                this.Status("Job manager vertex cannot be debugged/profiled locally.", StatusKind.Error);
                return;
            }
            
            if (!Utilities.IsThisUser(this.Job.Summary.User))
            {
                this.Status("Job belongs to a different user; symbols may be unavailable.", StatusKind.Error);
            }

            string menuItemName = sender.ToString().ToLower();
            if (menuItemName.Contains("debug"))
            {
                ClusterConfiguration config = this.Job.ClusterConfiguration;
                if (config is CacheClusterConfiguration)
                    config = (config as CacheClusterConfiguration).ActualConfig(this.Job.Summary);

                LocalDebuggingAndProfiling dvl = new LocalDebuggingAndProfiling(
                   config,
                   selectedVertex.UniqueID,
                   selectedVertex.Number,
                   selectedVertex.Version,
                   selectedVertex.WorkDirectory,
                   !menuItemName.Contains("unmanaged"),
                   true,
                   this.Status);

                bool checkSucceeds = dvl.CheckArchitecture(this.Status);
                if (checkSucceeds)
                {
                    bool start = true;
                    if (!this.WarnedAboutDebugging)
                    {
                        bool doNotEmitAgain;
                        start = dvl.EmitWarning(true, out doNotEmitAgain);
                        this.WarnedAboutDebugging = doNotEmitAgain;
                    }

                    if (start)
                        ThreadPool.QueueUserWorkItem(dvl.performDebugging);
                    else
                        this.Status("Debugging cancelled", StatusKind.OK);
                }
            }
            else
            {
                ClusterConfiguration config = this.Job.ClusterConfiguration;
                if (config is CacheClusterConfiguration)
                    config = (config as CacheClusterConfiguration).ActualConfig(this.Job.Summary);

                LocalDebuggingAndProfiling dvl = new LocalDebuggingAndProfiling(
                    config,
                    selectedVertex.UniqueID,
                    selectedVertex.Number,
                    selectedVertex.Version,
                    selectedVertex.WorkDirectory,
                    true,
                    menuItemName.Contains("cpu"),
                    this.Status);

                bool checkSucceeds = dvl.CheckArchitecture(this.Status);
                if (checkSucceeds)
                {
                    bool start = true;
                    if (!this.WarnedAboutProfiling)
                    {
                        bool doNotEmitAgain;
                        start = dvl.EmitWarning(false, out doNotEmitAgain);
                        this.WarnedAboutProfiling = doNotEmitAgain;
                    }

                    if (start)
                        ThreadPool.QueueUserWorkItem(dvl.performProfiling);
                    else
                        this.Status("Profiling cancelled", StatusKind.OK);
                }
            }
        }

        /// <summary>
        /// User selected to view a different plan.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void comboBox_plan_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.DrawQueryPlan();
        }

        /// <summary>
        /// Fit the visible plan to the available window space.
        /// </summary>
        private void FitPlanToWindow()
        {
            switch (this.planVisible)
            {
                case PlanVisible.Dynamic:
                    this.planDrawSurface.SetDrawingArea(
                        new Rectangle2D(
                            new Point2D(0, 0),
                            new Point2D(this.dynamicPlanLayout.Size.X, this.dynamicPlanLayout.Size.Y)));
                    break;
                case PlanVisible.Schedule:
                    this.planDrawSurface.SetDrawingArea(
                        new Rectangle2D(
                            new Point2D(0, 0),
                            new Point2D(this.jobSchedule.X, this.jobSchedule.Y)));
                    break;
                case PlanVisible.None:
                    return;
                case PlanVisible.Static:
                    {
                        this.staticGraphZoomLevel = 0;
                        this.graphViewer.FitGraphBoundingBox();
                    }
                    break;
            }
        }

        /// <summary>
        /// Fit the plan in the available space.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void zoomToFitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.FitPlanToWindow();
            this.DrawQueryPlan();
        }

        /// <summary>
        /// Scan the JM logs looking for the specified vertex; display the lines in the file view.
        /// Run in the background.
        /// </summary>
        /// <param name="vertex">Vertex to look for.</param>
        /// <returns>true if the information was found.</returns>
        /// <param name="logViewer">Viewer used to display the logs.</param>
        private bool ScanJMLogs(ExecutedVertexInstance vertex, LogViewer logViewer)
        {
            if (vertex == null || this.Job.ManagerVertex == null)
                return false;
            if (vertex == this.Job.ManagerVertex)
                return false;

            string vertexId = vertex.UniqueID;
            Regex regex = new Regex(vertexId, RegexOptions.Compiled);
            Trace.TraceInformation(regex.ToString());
            IClusterResidentObject logdir = this.Job.ManagerVertex.LogDirectory;

            if (logdir.Exception != null)
            {
                this.Status(logdir.ToString(), StatusKind.Error);
                return false;
            }

            List<IClusterResidentObject> files = logdir.GetFilesAndFolders(this.Job.ManagerVertex.LogFilesPattern).ToList();
            if (files.Count == 0)
            {
                this.Status("No log files found", StatusKind.Error);
                return false;
            }

            try
            {
                long totalWork = 0;
                foreach (var file in files)
                {
                    if (totalWork >= 0 && file.Size >= 0)
                        totalWork += file.Size;
                }

                long done = 0;
                foreach (var file in files)
                {
                    ISharedStreamReader sr = file.GetStream();
                    if (sr.Exception != null)
                    {
                        logViewer.Status("Error opening file: " + sr.Exception.Message, StatusKind.Error);
                        continue;
                    }
                    logViewer.Status("Scanning " + file, StatusKind.LongOp);
                    long lineno = 0;
                    while (!sr.EndOfStream)
                    {
                        if (logViewer.Cancelled)
                            break;
                        string line = sr.ReadLine();
                        done += line.Length;
                        if (regex.IsMatch(line))
                        {
                            logViewer.AddLine(file.Name, lineno, line);
                        }
                        lineno++;
                        logViewer.UpdateProgress((int)(100 * done / totalWork));
                    }
                    sr.Close();
                    if (logViewer.Cancelled)
                        break;
                }
            }
            finally
            {
                logViewer.Done();
            }

            return true;
        }

        /// <summary>
        /// Scan the JM stdout looking for the specified vertex; display the lines in the file view.
        /// Run in the background.
        /// </summary>
        /// <param name="vertex">Vertex to look for.</param>
        /// <returns>true if the information was found.</returns>
        /// <param name="logViewer">Viewer to use to display the logs.</param>
        /// <param name="stdout">Job standard output stream.</param>
        private static bool ScanJMStdout(ExecutedVertexInstance vertex, IClusterResidentObject stdout, LogViewer logViewer)
        {
            if (vertex == null || vertex.IsManager)
                return false;

            string vertexId = vertex.UniqueID;
            string name = string.Format(@"\s{0}.{1}\s", vertex.Number, vertex.Version); // the dot could match a space too.
            string regexstring = string.Format(@"vertex\s{0}\s(.*)\sv.{1}\s|", vertex.Number, vertex.Version);
            if (vertexId != "")
                regexstring += vertexId + "|";
            regexstring += name + "|"  + vertex.UniqueID;
            Regex regex = new Regex(regexstring, RegexOptions.Compiled);
            Trace.TraceInformation(regex.ToString());

            long length = stdout.Size;
            logViewer.Status("Looking for " + vertex.Name, StatusKind.LongOp);
            if (length == 0)
            {
                logViewer.Status("JM stdout is empty.", StatusKind.Error);
                logViewer.Done();
                return false;
            }

            ISharedStreamReader sr = stdout.GetStream();
            if (sr.Exception != null)
            {
                logViewer.Status("Error opening JM stdout: " + sr.Exception.Message, StatusKind.Error);
                logViewer.Done();
                return false;
            }

            try
            {
                long read = 0;
                long lines = 0;
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    read += line.Length;
                    if (regex.IsMatch(line))
                        logViewer.AddLine(stdout.ToString(), lines, line);
                    lines++;
                    if (lines % 100 == 0 && length > 0)
                    {
                        if (logViewer.Cancelled)
                            break;
                        logViewer.UpdateProgress(Math.Min((int)(read * 100 / length), 100)); // the length can grow while the file is being read
                    }
                }
                sr.Close();
            }
            finally
            {
                logViewer.Done();
            }
            return true;
        }

        /// <summary>
        /// Find all mentions of this vertex in the JM stdout.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void jMStdoutMentionsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.FindJMStdoutMentions();
        }

        /// <summary>
        /// Filter all vertices that match the stage filter.
        /// </summary>
        private void ApplyStageFilter()
        {
            string filter = this.textBox_stageFilter.Text;
            if (filter == "")
            {
                this.ShowSelecteddVertices();
                return;
            }

            // keep only the filtered vertices in the displayed set
            List<int> indices = DGVData<DryadLinqJobStage>.DataGridViewRowIndicesMatching(filter, this.dataGridView_stageContents, true, false);
            HashSet<ExecutedVertexInstance> toKeep = new HashSet<ExecutedVertexInstance>(
                indices.Select(i => (ExecutedVertexInstance)this.dataGridView_stageContents.Rows[i].DataBoundItem));
            this.PopulateStageVertices(toKeep.Contains);
        }

        /// <summary>
        /// Show only the vertices that match the filter in the box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_stageFilter_Click(object sender, EventArgs e)
        {
            this.ApplyStageFilter();
        }

        private void textBox_stageFilter_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
                this.ApplyStageFilter();
        }

        /// <summary>
        /// Clear the extra filter for the stage.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_clearStageFilter_Click(object sender, EventArgs e)
        {
            this.textBox_stageFilter.Text = "";
            this.ApplyStageFilter();
        }

        /// <summary>
        /// User changed the setting for hiding the cancelled vertices.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void hideCancelledVerticesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.hideCancelledVerticesToolStripMenuItem.Checked = !this.hideCancelledVerticesToolStripMenuItem.Checked;
            this.ShowSelecteddVertices();
            this.RefreshQueryPlan();
            if (this.currentTable != null)
                this.SetTable(this.currentTable.Refresh(this.Job, this.Status, !this.hideCancelledVerticesToolStripMenuItem.Checked));
        }

        /// <summary>
        /// User selected to terminate the job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void toolStripMenuItem_terminate_Click(object sender, EventArgs e)
        {
            List<DryadLinqJobSummary> job = new List<DryadLinqJobSummary>();
            job.Add(this.Job.Summary);
            ClusterStatus clusterStatus = this.Job.ClusterConfiguration.CreateClusterStatus();

            var item = new BackgroundWorkItem<bool>(
                m => ClusterWork.CancelJobs(job, clusterStatus, m),
                (c, b) => { },
                "cancel");
            this.Queue(item);
        }

        /// <summary>
        /// Diagnose the current vertex.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void diagnoseToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            if (this.currentVertex == null)
                return;

            // TODO: this should run in the background
            CommManager manager = new CommManager(this.Status, this.UpdateProgress, new CancellationToken());
            VertexFailureDiagnosis vfd = VertexFailureDiagnosis.CreateVertexFailureDiagnosis(this.Job, this.staticPlan, this.currentVertex, manager);
            DiagnosisLog log = vfd.Diagnose();
            this.DisplayDiagnosis(log);
        }

        /// <summary>
        /// Mouse clicked in a row of the datagrid.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Event describing the mouse click.</param>
        private void dataGridView_stageContents_CellMouseDown(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.ColumnIndex >= 0 && e.RowIndex >= 0)
            {
                if (this.dataGridView_stageContents.Rows[e.RowIndex].Selected == false)
                {
                    this.dataGridView_stageContents.ClearSelection();
                    this.dataGridView_stageContents.Rows[e.RowIndex].Selected = true;
                }
                this.dataGridView_stageContents.CurrentCell = this.dataGridView_stageContents.Rows[e.RowIndex].Cells[e.ColumnIndex];
            }
        }


        /// <summary>
        /// User changed the color view.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void colorByStagestatusToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.colorByStagestatusToolStripMenuItem.Checked = !this.colorByStagestatusToolStripMenuItem.Checked;
            this.AssignPlanColors(); 
            this.DrawQueryPlan();
        }

        /// <summary>
        /// Data grid view is scrolling.  React only at the end of scrolling.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void dataGridView_Scroll(object sender, ScrollEventArgs e)
        {
        }

        /// <summary>
        /// Change word wrapping in richtextbox.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void wordWrapToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.wordWrapToolStripMenuItem.Checked = !this.wordWrapToolStripMenuItem.Checked;
            this.richTextBox_file.WordWrap = this.wordWrapToolStripMenuItem.Checked;
        }

        /// <summary>
        /// Create a package containing all the files cached for the selected job.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void packageCachedFilesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Copy all log files displayed in the current folder to the cache.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void cacheAllLogsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.RefreshJob();

            IClusterResidentObject folder = this.richtextBoxShownFile;
            if (folder == null || folder.Exception != null || !folder.RepresentsAFolder)
            {
                this.Status("Cannot copy files to cache", StatusKind.Error);
                return;
            }

            int cached = 0, skipped = 0;
            foreach (IClusterResidentObject file in folder.GetFilesAndFolders("*"))
            {
                if (file.RepresentsAFolder) continue;
                if (!Utilities.FileNameIndicatesTextFile(file.Name))
                {
                    skipped++;
                    this.Status("Skipping non-log file " + file.Name, StatusKind.OK);
                    continue;
                }
                if (!file.ShouldCacheLocally)
                {
                    skipped++;
                    this.Status("File " + file.Name + " cannot be cached (perhaps it's still being written?)", StatusKind.Error);
                    continue;
                }

                this.Status("Caching " + file.Name, StatusKind.LongOp);
                ISharedStreamReader reader = file.GetStream();
// ReSharper disable UnusedVariable
                foreach (string line in reader.ReadAllLines())
// ReSharper restore UnusedVariable
                {
                    // discard; causes caching
                }
                cached++;
            }

            this.Status("Cached " + cached + " skipped " + skipped + " files", StatusKind.OK);
        }

        /// <summary>
        /// Save data about the job vertices to a CSV file.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void exportToCSVToolStripMenuItem_Click(object sender, EventArgs e)
        {
            // ask file name to save to
            SaveFileDialog dialog = new SaveFileDialog();
            dialog.Filter = "csv files (*.csv)|*.csv";
            dialog.Title = "Chose file name for csv file with vertex information.";
            dialog.RestoreDirectory = true;
            DialogResult save = dialog.ShowDialog();
            if (save != DialogResult.OK)
                return;

            string file = dialog.FileName;
            try
            {
                StreamWriter sw = new StreamWriter(file);
                sw.WriteLine(ExecutedVertexInstance.CSV_Header());
                foreach (ExecutedVertexInstance v in this.Job.Vertices) {
                    sw.WriteLine(v.AsCSV());
                }
                sw.Close();
                this.Status("Wrote data to file " + file, StatusKind.OK);
            }
            catch (Exception ex)
            {
                this.Status("Exception while writing csv file: " + ex.Message, StatusKind.Error);
            }
        }

        /// <summary>
        /// Add the log files (and other important files) for all displayed vertices to the cache.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void cacheLogsForAllVerticesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (this.ShowingStageOrTable != KindOfStageShown.Stage)
            {
                this.Status("No vertices are currently displayed", StatusKind.Error);
                return;
            }

            List<ExecutedVertexInstance> vertices = this.stageData.ToList();

            var item = new BackgroundWorkItem<bool>(
                m => CacheAllVertices(this.Job.ClusterConfiguration, this.Job.Summary, vertices, m),
                (c, b) => { },
                "cacheAll");
            this.Queue(item);
        }

        /// <summary>
        /// Cache the vertices in the list; executed on the background thread.
        /// </summary>
        /// <returns>True: success.</returns>
        /// <param name="manager">Communication manager.</param>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="summary">Job to cache.</param>
        /// <param name="vertices">Vertices to cache.</param>
        private static bool CacheAllVertices(
            ClusterConfiguration config, DryadLinqJobSummary summary, List<ExecutedVertexInstance> vertices,
            CommManager manager)
        {
            int done = 0;
            int todo = vertices.Count;
            int files = 0;
            manager.Status("Caching data for " + todo + " vertices", StatusKind.LongOp);
            foreach (ExecutedVertexInstance v in vertices)
            {
                files += CacheVertexInfo(config, summary, v);
                done++;
                manager.Progress(done / todo);
            }

            manager.Progress(100);
            manager.Status("Cached " + files + " files", StatusKind.OK);
            return true;
        }

        /// <summary>
        /// Cache the interesting files of this vertex.
        /// </summary>
        /// <param name="v">Vertex whose files should be cached.</param>
        /// <returns>Number of files cached.</returns>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="summary">Job summary.</param>
        private static int CacheVertexInfo(ClusterConfiguration config, DryadLinqJobSummary summary, ExecutedVertexInstance v)
        {
            int cached = 0;

            IClusterResidentObject folder = config.ProcessWorkDirectory(v.ProcessIdentifier, v.VertexIsCompleted, v.Machine, summary);
            if (folder == null || folder.Exception != null)
                return 0;

            foreach (IClusterResidentObject file in folder.GetFilesAndFolders("*"))
            {
                if (file.RepresentsAFolder) continue;
                if (!Utilities.FileNameIndicatesTextFile(file.Name))
                {
                    continue;
                }
                if (!file.ShouldCacheLocally)
                {
                    continue;
                }

                ISharedStreamReader reader = file.GetStream();
                // ReSharper disable once UnusedVariable
                foreach (string line in reader.ReadAllLines())
                {
                    // discard; causes caching
                }
                cached++;
            }
            return cached;
        }

        private void cancelCurrentWorkToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.queue.CancelCurrentWork();
        }
    }

    /// <summary>
    /// Persistent settings for the job browser.
    /// </summary>
    internal class JobBrowserSettings : ApplicationSettingsBase
    {
        /// <summary>
        /// Form location on screen.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("0,0")]
        public Point Location { 
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
        /// Width of job panel.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("390")]
        public int JobSplitterPosition
        {
            get { return (int)this["JobSplitterPosition"]; }
            set { this["JobSplitterPosition"] = value; }
        }
        /// <summary>
        /// Width of stage panel.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("390")]
        public int StageSplitterPosition
        {
            get { return (int)this["StageSplitterPosition"]; }
            set { this["StageSplitterPosition"] = value; }
        }
        /// <summary>
        /// Height of job header.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("100")]
        public int JobHeaderSplitterPosition
        {
            get { return (int)this["JobHeaderSplitterPosition"]; }
            set { this["JobHeaderSplitterPosition"] = value; }
        }
        /// <summary>
        /// Height of stage header.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("100")]
        public int StageHeaderSplitterPosition
        {
            get { return (int)this["StageHeaderSplitterPosition"]; }
            set { this["StageHeaderSplitterPosition"] = value; }
        }
        /// <summary>
        /// Height of vertex header.
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("100")]
        public int VertexHeaderSplitterPosition
        {
            get { return (int)this["VertexHeaderSplitterPosition"]; }
            set { this["VertexHeaderSplitterPosition"] = value; }
        }
        /// <summary>
        /// Height of code box
        /// </summary>
        [UserScopedSetting]
        [DefaultSettingValue("100")]
        public int CodeSplitterPosition
        {
            get { return (int)this["CodeSplitterPosition"]; }
            set { this["CodeSplitterPosition"] = value; }
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

        [UserScopedSetting]
        [DefaultSettingValue("false")]
        public bool HideCancelledVertices
        {
            get { return (bool)this["HideCancelledVertices"]; }
            set { this["HideCancelledVertices"] = value; }
        }

        [UserScopedSetting]
        [DefaultSettingValue("false")]
        public bool WarnedAboutDebugging
        {
            get { return (bool)this["WarnedAboutDebugging"]; }
            set { this["WarnedAboutDebugging"] = value; }
        }

        [UserScopedSetting]
        [DefaultSettingValue("false")]
        public bool WarnedAboutProfiling
        {
            get { return (bool)this["WarnedAboutProfiling"]; }
            set { this["WarnedAboutProfiling"] = value; }
        }
    }
}
