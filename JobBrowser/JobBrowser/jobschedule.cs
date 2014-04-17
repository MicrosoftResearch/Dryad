
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
using System.Drawing;
using System.Linq;
using Microsoft.Research.Tools;

namespace Microsoft.Research.JobObjectModel
{
    /// <summary>
    /// Information about the dynamic execution schedule of a job.
    /// </summary>
    public class DryadLinqJobSchedule
    {
        class MachineUtilization
        {
            class MachineInformation
            {
                List<ExecutedVertexInstance> vertices;
                public int SortedIndex;

                // ReSharper disable once UnusedParameter.Local
                public MachineInformation(string name)
                {
                    this.vertices = new List<ExecutedVertexInstance>();
                    this.SortedIndex = 0;
                }

                public void AddVertex(ExecutedVertexInstance e)
                {
                    this.vertices.Add(e);
                }

                public List<ExecutedVertexInstance> Vertices { get { return this.vertices; } }
            }

            Dictionary<string, int> machines;
            List<MachineInformation> machineInfo;
            List<string> sortedMachines;

            public MachineUtilization()
            {
                this.machines = new Dictionary<string, int>();
                this.machineInfo = new List<MachineInformation>();
            }

            public int MachineCount { get { return this.machineInfo.Count; } }

            internal void Add(string machine, ExecutedVertexInstance vertex)
            {
                int index;
                if (!this.machines.ContainsKey(machine))
                {
                    index = this.MachineCount;
                    this.machines.Add(machine, index);
                    this.machineInfo.Add(new MachineInformation(machine));
                }
                else
                {
                    index = this.machines[machine];
                }
                this.machineInfo[index].AddVertex(vertex);
            }

            internal int SortedIndex(string machine)
            {
                return this.machineInfo[this.machines[machine]].SortedIndex;
            }

            internal void Sort()
            {
                this.sortedMachines = this.machines.Keys.ToList();
                this.sortedMachines.Sort();
                for (int i = 0; i < this.sortedMachines.Count; i++)
                {
                    string machine = this.sortedMachines[i];
                    int infoindex = this.machines[machine];
                    this.machineInfo[infoindex].SortedIndex = i;
                }
            }

            internal List<ExecutedVertexInstance> Vertices(int index)
            {
                if (index < 0 || index >= this.MachineCount)
                    return null;
                string machine = this.sortedMachines[index];
                int infoIndex = this.machines[machine];
                return this.machineInfo[infoIndex].Vertices;
            }
        }

        //DryadLinqJobInfo job;
        List<ExecutedVertexInstance> vertices;
        DateTime startTime;
        MachineUtilization utilization;
        const double spacing = 1.5;

        /// <summary>
        /// Create a DryadLinqJobSchedule object starting from the job information.
        /// </summary>
        /// <param name="jobInfo">Job whose schedule is computed.</param>
        /// <param name="hideCancelledVertices">If true do not show the cancelled vertices.</param>
        public DryadLinqJobSchedule(DryadLinqJobInfo jobInfo, bool hideCancelledVertices)
        {
            this.utilization = new MachineUtilization();

            this.startTime = jobInfo.StartJMTime;
            this.X = jobInfo.RunningTime.TotalSeconds;
            
            this.vertices = new List<ExecutedVertexInstance>();
            foreach (var stage in jobInfo.AllStages().ToList())
            {
                foreach (var vertex in stage.Vertices)
                {
                    if (hideCancelledVertices && vertex.State == ExecutedVertexInstance.VertexState.Cancelled)
                        continue;
                    string m = vertex.Machine;
                    this.utilization.Add(m, vertex);
                    this.vertices.Add(vertex);
                }
            }

            this.utilization.Sort();
            this.Y = spacing * this.utilization.MachineCount;
        }

        /// <summary>
        /// X dimension of schedule.
        /// </summary>
        public double X { get; protected set; }
        /// <summary>
        /// Y dimension of schedule.
        /// </summary>
        public double Y { get; protected set; }
        /// <summary>
        /// Draw the plan.
        /// </summary>
        /// <param name="drawingSurface2D">Surface where the plan is to be drawn.</param>
        /// <param name="colorToUse">Function deciding the color to use for each vertex.</param>
        public void Draw(DrawingSurface2D drawingSurface2D, Func<ExecutedVertexInstance, Color> colorToUse)
        {
            foreach (var vertex in this.vertices)
            {
                DateTime start = vertex.Start;
                string machine = vertex.Machine;
                if (string.IsNullOrEmpty(machine) || start == DateTime.MinValue)
                    continue;

                TimeSpan runningTime = vertex.RunningTime;
                // This can happen if there is nothing known about this vertex at the point when the schedule is drawn.
                if (runningTime == TimeSpan.MinValue)
                    continue;

                DateTime end = start + runningTime;
                int index = this.utilization.SortedIndex(machine);
                double y = spacing * index;
                double left = (start - this.startTime).TotalSeconds;
                double right = (end - this.startTime).TotalSeconds;
                Color color = colorToUse(vertex);
                Pen pen = new Pen(color, 2);

                drawingSurface2D.DrawLine(pen, new Point2D(left, y), new Point2D(right, y), false);
            }
        }

        /// <summary>
        /// Find the vertex that is being clicked on.
        /// </summary>
        /// <param name="xo">X coordinate on screen.</param>
        /// <param name="yo">Y coordinate on screen.</param>
        /// <returns>The vertex at these coordinates, or null.</returns>
        internal ExecutedVertexInstance FindVertex(double xo, double yo)
        {
            int index = (int)Math.Round(yo / spacing);
            List<ExecutedVertexInstance> candidates = this.utilization.Vertices(index);
            if (candidates == null)
                return null;

            foreach (var v in candidates)
            {
                if (v.Start == DateTime.MinValue || v.RunningTime == TimeSpan.MinValue) continue;

                DateTime start = v.Start;
                DateTime end = start + v.RunningTime;
                double left = (start - this.startTime).TotalSeconds;
                double right = (end - this.startTime).TotalSeconds;

                if (left < xo && right > xo)
                    return v;
            }
            return null;
        }
    }
}
