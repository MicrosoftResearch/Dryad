
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
using System.Drawing.Drawing2D;

namespace Microsoft.Research.Calypso.Tools
{
    /// <summary>
    /// Deals with the plane representation of a graph.
    /// </summary>
    public class GraphLayout
    {
        /// <summary>
        /// Drawing style.
        /// </summary>
        public enum Style
        {
            /// <summary>
            /// Normal style.
            /// </summary>
            Normal,
            /// <summary>
            /// Bold drawing.
            /// </summary>
            Bold,
            /// <summary>
            /// Fill with color, mostly for nodes.
            /// </summary>
            Filled,
            /// <summary>
            /// Dotted line.
            /// </summary>
            Dotted,
            /// <summary>
            /// Solid edge.
            /// </summary>
            Solid,
        }

        /// <summary>
        /// A node in the graph.
        /// </summary>
        public class GraphNode : IName
        {
            /// <summary>
            /// One of the node shapes.
            /// </summary>
            public enum NodeShape
            {
                /// <summary>
                /// Rectangle.
                /// </summary>
                Box,
                /// <summary>
                /// Ellipse.
                /// </summary>
                Ellipse,
                /// <summary>
                /// House upside-down.
                /// </summary>
                Invhouse,
            };

            /// <summary>
            /// Node name.
            /// </summary>
            public string ObjectName { get; set; }
            /// <summary>
            /// Node position and size.
            /// </summary>
            public Rectangle2D Position { get; protected set; }
            /// <summary>
            /// Line color.
            /// </summary>
            public Color LineColor { get; set; }
            /// <summary>
            /// Node style.
            /// </summary>
            public Style Style { get; set; }
            /// <summary>
            /// Node shape.
            /// </summary>
            public NodeShape Shape { get; set; }
            /// <summary>
            /// Node label.
            /// </summary>
            public string Label { get; set; }
            /// <summary>
            /// Name of stage described by the plan node.
            /// </summary>
            public string Stage { get; set; }
            /// <summary>
            /// For each color a percentage, showing how much of the node to fill with that color.
            /// </summary>
            public List<Tuple<double, Color>> FillColors { get; set; }
            private static Pen blackPen = new Pen(Color.Black);
            private static Pen thickBlackPen = new Pen(Color.Black, 4);

            /// <summary>
            /// Create a graph node at a specified position.
            /// </summary>
            /// <param name="x">X coordinate.</param>
            /// <param name="y">Y coordinate.</param>
            /// <param name="width">Width.</param>
            /// <param name="height">Height.</param>
            public GraphNode(double x, double y, double width, double height)
            {
                this.Position = new Rectangle2D(x, y, x + width, y + height);
                this.FillColors = new List<Tuple<double, Color>>();
                this.LineColor = Color.Black;
            }

            /// <summary>
            /// True if the node is selected on the plot.
            /// </summary>
            public bool Selected { get; set; }

            /// <summary>
            /// Draw the node on a panel surface.
            /// </summary>
            /// <param name="surface">Surface to draw on.</param>
            internal void Draw(DrawingSurface2D surface)
            {
                if (this.Position.Width > 0)
                {
                    // Fill colors in bands from left to right
                    double offset = 0;
                    foreach (Tuple<double, Color> band in this.FillColors)
                    {
                        if (band.Item1 <= 0)
                            continue;

                        Color c = band.Item2;
                        var brush = new SolidBrush(c);

                        Point2D left = new Point2D(this.Position.Corner1.X + offset * this.Position.Width, this.Position.Corner1.Y);
                        offset += band.Item1;
                        Point2D right = new Point2D(this.Position.Corner1.X + offset * this.Position.Width, this.Position.Corner2.Y);
                        surface.FillRectangle(brush, new Rectangle2D(left, right));
                    }

                    Font font = new Font("Arial", 12, FontStyle.Regular);
                    // let's shrink the position for the text to leave some border

                    if (this.Label != null)
                    {
                        const double borderFraction = 8; // reserve 1/8 of the rectangle for border
                        Rectangle2D box = new Rectangle2D(
                            this.Position.Corner1.Translate(this.Position.Width / borderFraction, this.Position.Height / borderFraction),
                            this.Position.Corner2.Translate(-this.Position.Width / borderFraction, -this.Position.Height / borderFraction));
                        surface.DrawTextInRectangle(this.Label, Brushes.Black, font, box);
                    }
                }

                Pen pen = this.Selected ? thickBlackPen : blackPen;
                switch (this.Shape)
                {
                    default:
                    case NodeShape.Box:
                        surface.DrawRectangle(this.Position, pen);
                        break;
                    case NodeShape.Ellipse:
                        surface.DrawEllipse(this.LineColor, this.Position, pen, false);
                        break;
                    case NodeShape.Invhouse:
                        {
                            Point2D[] points = new Point2D[5];
                            points[0] = new Point2D(this.Position.Corner1.X, this.Position.Corner1.Y + this.Position.Height / 3);
                            points[1] = new Point2D(this.Position.Corner1.X, this.Position.Corner2.Y - this.Position.Height / 8);
                            points[2] = new Point2D(this.Position.Corner2.X, this.Position.Corner2.Y - this.Position.Height / 8);
                            points[3] = new Point2D(this.Position.Corner2.X, this.Position.Corner1.Y + this.Position.Height / 3);
                            points[4] = new Point2D(this.Position.Corner1.X + this.Position.Width / 2, this.Position.Corner1.Y);
                            surface.DrawPolygon(this.LineColor, points, pen, false);
                            break;
                        }
                }
            }
        }

        /// <summary>
        /// An edge in the graph.
        /// </summary>
        public class GraphEdge
        {
            /// <summary>
            /// Name of tail node of edge.
            /// </summary>
            public string Tail { get; protected set; }
            /// <summary>
            /// Name of head node of edge.
            /// </summary>
            public string Head { get; protected set; }
            /// <summary>
            /// List of points.
            /// </summary>
            public List<Point2D> Spline { get; protected set; }
            /// <summary>
            /// Edge style.
            /// </summary>
            public Style Style { get; protected set; }
            /// <summary>
            /// Line color.
            /// </summary>
            public Color Color { get; protected set; }

            /// <summary>
            /// Create a simple one-segment edge.
            /// </summary>
            /// <param name="xstart">Start of edge x.</param>
            /// <param name="ystart">Start of edge y.</param>
            /// <param name="xend">End of edge x.</param>
            /// <param name="yend">End of edge y.</param>
            public GraphEdge(double xstart, double ystart, double xend, double yend)
            {
                this.Spline = new List<Point2D>();
                this.Spline.Add(new Point2D(xstart, ystart));
                this.Spline.Add(new Point2D(xend, yend));
                this.Color = Color.Black;
            }

            /// <summary>
            /// Draw the edge on the specified surface.
            /// </summary>
            /// <param name="surface">Surface to draw on.</param>
            internal void Draw(DrawingSurface2D surface)
            {
                var previous = Spline[0];
                int width = this.Style == Style.Bold ? 3 : 1;
                Pen pen = new Pen(this.Color);
                pen.Width = width;
                for (int i = 1; i < Spline.Count; i++)
                {
                    Point2D point = Spline[i];
                    if (i == Spline.Count - 1)
                    {
                        // adjust for arrow size at the end of the edge.
                        const double absoluteArrowSize = 0.15;
                        double dx = point.X - previous.X;
                        double dy = point.Y - previous.Y;
                        double len = Math.Sqrt(dx * dx + dy * dy);
                        double adx = absoluteArrowSize * dx / len;
                        double ady = absoluteArrowSize * dy / len;

                        point = new Point2D(point.X + adx, point.Y + ady);
                        //pen.EndCap = LineCap.ArrowAnchor;
                        AdjustableArrowCap cap = new AdjustableArrowCap(4, 6);
                        pen.CustomEndCap = cap;
                    }
                    surface.DrawLine(pen, previous, point, false);
                    previous = point;
                }
            }
        }

        /// <summary>
        /// The nodes in the graph.
        /// </summary>
        List<GraphNode> nodes;
        /// <summary>
        /// The edges in the graph.
        /// </summary>
        List<GraphEdge> edges;
        /// <summary>
        /// Size of box containing graph layout.
        /// </summary>
        Point2D size;

        /// <summary>
        /// The list of all nodes in the graph.
        /// </summary>
        public IEnumerable<GraphNode> AllNodes { get { return this.nodes; } }

        /// <summary>
        /// Create an empty graph layout.
        /// <param name="height">Height of graph to draw, in some arbitrary unit of measure; node and edge coordinates are relative to these.</param>
        /// <param name="width">Width of graph to draw, in some arbitrary unit of measure; node and edge coordinates are relative to these.</param>
        /// </summary>
        public GraphLayout(double width, double height)
        {
            this.size = new Point2D(width, height);
            this.edges = new List<GraphEdge>();
            this.nodes = new List<GraphNode>();
        }

        /// <summary>
        /// Draw the graph on the specified surface.
        /// </summary>
        /// <param name="surface">Surface to draw graph on.</param>
        public void Draw(DrawingSurface2D surface)
        {            
            foreach (GraphNode n in this.nodes)
                n.Draw(surface);
            foreach (GraphEdge e in this.edges)
                e.Draw(surface);
        }

        /// <summary>
        /// Coordinates of lower-right point for the layout.
        /// </summary>
        public Point2D Size { get { return this.size; } }

        /// <summary>
        /// Add a node to the graph.
        /// </summary>
        /// <param name="node">Node to add to the graph.</param>
        public void Add(GraphNode node)
        {
            this.nodes.Add(node);
        }

        /// <summary>
        /// Add an edge to the graph.
        /// </summary>
        /// <param name="edge">Edge to add.</param>
        public void Add(GraphEdge edge)
        {
            this.edges.Add(edge);
        }

        /// <summary>
        /// Find node which contains the given coordinates.  Return null if none.
        /// </summary>
        /// <param name="xo">X coordinate where mouse was clicked.</param>
        /// <param name="yo">Y coordinate where mouse was clicked.</param>
        /// <returns>Node which contains specified coordinates, or null.</returns>
        public GraphNode FindNode(double xo, double yo)
        {
            Point2D p = new Point2D(xo, yo);
            foreach (GraphNode node in this.nodes)
                if (node.Position.Inside(p))
                    return node;
            return null;
        }

        /// <summary>
        /// Clear the selection on all nodes.
        /// </summary>
        public void ClearSelectedNodes()
        {
            foreach (GraphNode n in this.nodes)
                n.Selected = false;
        }
    }
}
