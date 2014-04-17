
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

namespace Microsoft.Research.Tools
{
    using System;
    using System.Drawing;
    using System.Drawing.Drawing2D;
    using System.Windows.Forms;

    /// <summary>
    /// A 2D drawing surface with FP coordinates.
    /// </summary>
    public class DrawingSurface2D
    {
        /// <summary>
        /// Panel where the drawing is done.
        /// </summary>
        Panel drawingpanel;
        /// <summary>
        /// Graphics context used for drawing.
        /// </summary>
        Graphics grph;
        /// <summary>
        /// Screen graphics context (used for some transient pictures).
        /// </summary>
        Graphics screenGrph;
        /// <summary>
        /// Margins around the axes of the drawing area, in pixels.
        /// </summary>
        public int rightMargin, leftMargin; // public because other areas need to know about them
        /// <summary>
        /// Drawing area border coordinates, must be set before drawing anything.
        /// </summary>
        Rectangle2D boundingBox;
        /// <summary>
        /// Double-buffering support; by setting this to null in the constructor you can disable double-buffering.
        /// </summary>
        Bitmap buffer;
        /// <summary>
        /// Old size of canvas, before resizing.
        /// </summary>
        Size oldsize;

        /// <summary>
        /// Width of drawing area on screen in pixels, excluding margins.
        /// </summary>
        private int drawingAreaWidth;
        /// <summary>
        /// Height of drawing area on screen in pixels, excluding margins.
        /// </summary>
        private int drawingAreaHeight;
        /// <summary>
        /// Scaling coefficient on x (from drawing to screen coordinates).
        /// </summary>
        private double xScale;
        /// <summary>
        /// Scaling coefficient on y (from drawing to screen coordinates).
        /// </summary>
        private double yScale;
        /// <summary>
        /// Width of the drawing area.
        /// </summary>
        public double Width { get; protected set; }
        /// <summary>
        /// Height of the drawing area.
        /// </summary>
        public double Height { get; protected set; }

        /// <summary>
        /// Instantiate a 2D drawing surface.
        /// </summary>
        /// <param name="panel">Panel where the image ends up being drawn.</param>
        public DrawingSurface2D(Panel panel)
        {
            // Comment-out the assignment to buffer to disable double-buffering.
            buffer = new Bitmap(panel.Width, panel.Height);
            this.drawingpanel = panel;
            screenGrph = this.drawingpanel.CreateGraphics();
            if (buffer != null)
                grph = Graphics.FromImage(buffer);
            else
                grph = screenGrph;
            this.oldsize = panel.Size;
            grph.SmoothingMode = SmoothingMode.AntiAlias;
            this.RecomputeCachedValues();
        }

        /// <summary>
        /// Things may have changed, recompute some cached values.
        /// </summary>
        private void RecomputeCachedValues()
        {
            this.drawingAreaWidth = this.drawingpanel.Size.Width - this.leftMargin - this.rightMargin;
            this.drawingAreaHeight = this.drawingpanel.Size.Height - this.TopMargin - this.BottomMargin;

            if (this.boundingBox != null)
            {
                this.Width = this.xDrawMax - this.xDrawMin;
                this.Height = this.yDrawMax - this.yDrawMin;
                this.yScale = this.drawingAreaHeight / this.Height;
                this.xScale = this.drawingAreaWidth / this.Width;
            }
        }

        /// <summary>
        /// Set the margins in pixels around the area which is mapped to.
        /// </summary>
        /// <param name="top">Distance in pixels from top of panel to top of drawing area.</param>
        /// <param name="bottom">Distance in pixels from bottom of panel to bottom of drawing area.</param>
        /// <param name="right">Distance in pixels from right of panel to right of drawing area.</param>
        /// <param name="left">Distance in pixels from left of panel to left of drawing area.</param>
        public void SetMargins(int top, int bottom, int right, int left)
        {
            this.TopMargin = top;
            this.BottomMargin = bottom;
            this.rightMargin = right;
            this.leftMargin = left;
            this.RecomputeCachedValues();
        }

        private bool fastDrawing;
        /// <summary>
        /// If true try to draw faster.
        /// </summary>
        public bool FastDrawing
        {
            get
            {
                return this.fastDrawing;
            }
            set
            {
                this.fastDrawing = value;
                if (!value)
                    this.grph.SmoothingMode = SmoothingMode.AntiAlias;
                else
                    this.grph.SmoothingMode = SmoothingMode.HighSpeed;
            }
        }

        /// <summary>
        /// How big is the rectangle containing a string in this window?
        /// </summary>
        /// <param name="text">String to measure.</param>
        /// <param name="font">Font to draw the text in.</param>
        /// <returns>The size of a rectangle in pixels.</returns>
        public Size MeasureString(string text, Font font)
        {
            return this.grph.MeasureString(text, font).ToSize();
        }
        /// <summary>
        /// The bounding box of the drawing area.
        /// </summary>
        public Rectangle2D BoundingBox { get { return this.boundingBox; } }

        /// <summary>
        /// The size of the drawing area in pixels.
        /// </summary>
        /// <returns>Size of the drawing area for plots (excluding margins).</returns>
        public Size DrawingAreaSize()
        {
            return new Size(this.drawingAreaWidth, this.drawingAreaHeight);
        }

        /// <summary>
        /// Maximum X coordinate of drawing area.
        /// </summary>
        public double xDrawMax { get { return boundingBox.Corner2.X; } }
        /// <summary>
        /// Maximum Y coordinate of drawing area.
        /// </summary>
        public double yDrawMax { get { return boundingBox.Corner2.Y; } }
        /// <summary>
        /// Minimum X coordinate of drawing area.
        /// </summary>
        public double xDrawMin { get { return boundingBox.Corner1.X; } }
        /// <summary>
        /// Minimum Y coordinate of drawing area.
        /// </summary>
        public double yDrawMin { get { return boundingBox.Corner1.Y; } }
        /// <summary>
        /// Number of pixels above graphics plot.
        /// </summary>
        public int TopMargin { get; protected set; }
        /// <summary>
        /// Number of pixels below graphics plot.
        /// </summary>
        public int BottomMargin { get; protected set; }
        /// <summary>
        /// Size in pixels of the drawing panel including all margins.
        /// </summary>
        public Size SurfaceSize { get { return this.drawingpanel.Size; } }

        /// <summary>
        /// Before drawing anything one has to set the drawing area size.  
        /// This maps the four specified coordinates to the corners of the drawing panel.
        /// </summary>
        /// <param name="bbox">Perimeter of area.</param>
        public void SetDrawingArea(Rectangle2D bbox)
        {
            if (bbox.Degenerate())
                throw new ArgumentException("Cannot draw in a degenerate bounding box");
            this.boundingBox = bbox;
            this.RecomputeCachedValues();
        }

        /// <summary>
        /// Scale one point position and compute actual panel pixel coordinates.
        /// </summary>
        /// <param name="x">X coordinate to scale relative to the drawing area.</param>
        /// <param name="y">Y coordinate to scale relative to the drawing area.</param>
        /// <param name="xo">Resulting X coordinate after scaling (in panel coordinates).</param>
        /// <param name="yo">Resulting Y coordinate after scaling (in panel coordinates).</param>
        public void Scale(double x, double y, out int xo, out int yo)
        {
            double ynew = (y - this.yDrawMin) * this.yScale;
            double xnew = (x - this.xDrawMin) * this.xScale;

            xo = this.leftMargin + (int)xnew;
            yo = this.TopMargin + this.drawingAreaHeight - (int)ynew;
        }

        /// <summary>
        /// Scale one point position and compute actual panel pixel coordinates.
        /// </summary>
        /// <param name="point">Point to scale.</param>
        /// <param name="xo">Resulting X coordinate after scaling (in panel coordinates).</param>
        /// <param name="yo">Resulting Y coordinate after scaling (in panel coordinates).</param>
        public void Scale(Point2D point, out int xo, out int yo)
        {
            this.Scale(point.X, point.Y, out xo, out yo);
        }

        /// <summary>
        /// Scale one point position and compute actual panel pixel coordinates.
        /// </summary>
        /// <param name="point">Point to scale.</param>
        /// <returns>Point containing pixel coordinates.</returns>
        public Point Scale(Point2D point)
        {
            int xo, yo;
            this.Scale(point.X, point.Y, out xo, out yo);
            return new Point(xo, yo);
        }

        /// <summary>
        /// Convert a line length on the screen to user coordinates.
        /// </summary>
        /// <param name="line">Line length.</param>
        /// <returns>The plotting coordinates.</returns>
        public Point2D LengthScreenToUser(Point line)
        {
            Point2D res = new Point2D(line.X / this.xScale, line.Y / this.yScale);
            return res;
        }

        /// <summary>
        /// Invert the scaling operation: convert from screen coordinates to plotting coordinates.
        /// </summary>
        /// <param name="x">X coordinate in pixels in window.</param>
        /// <param name="y">Y coordinate in pixels in window.</param>
        /// <param name="xo">Original x coordinate, in drawing area.</param>
        /// <param name="yo">Original y coordinate, in drawing area.</param>
        public void ScreenToUser(int x, int y, out double xo, out double yo)
        {
            // remove margins and reflect
            x -= this.leftMargin;
            y = this.TopMargin + this.drawingAreaHeight - y;

            yo = y / this.yScale + this.yDrawMin;
            xo = x / this.xScale + this.xDrawMin;
        }

        /// <summary>
        /// Convert a dimension in screen coordinates to a dimension in plotting coordinates.
        /// </summary>
        /// <param name="size">Size to convert.</param>
        /// <returns>The result is really a size, encoded as a Point2D.</returns>
        public Point2D Unscale(Point2D size)
        {
            return new Point2D(size.X / this.xScale, size.Y / this.yScale);
        }

        /// <summary>
        /// The graphics context used for drawing.
        /// </summary>
        /// <param name="onscreen">If true returns the screen graphics context, else the backing buffer.</param>
        /// <returns>The graphics context used for drawing.</returns>
        internal Graphics GetGraphics(bool onscreen)
        {
            return onscreen ? this.screenGrph : this.grph;
        }

        /// <summary>
        /// Draw a line in screen coordinates.
        /// </summary>
        /// <param name="c">Line color.</param>
        /// <param name="left">Left endpoint.</param>
        /// <param name="right">Right endpoint.</param>
        /// <param name="lineWidth">Line width.</param>
        /// <param name="arrow">Cap line with arrow?</param>
        /// <param name="onscreen">Draw the line on the screen?</param>
        public void DrawLine(Color c, Point left, Point right, int lineWidth, bool arrow, bool onscreen)
        {
            System.Drawing.Pen linePen = new System.Drawing.Pen(c);
            linePen.Width = lineWidth;
            if (arrow)
            {
                linePen.EndCap = LineCap.ArrowAnchor;
            }

            this.DrawLine(linePen, left, right, onscreen);
        }

        /// <summary>
        /// Draw a line in screen coordinates.
        /// </summary>
        /// <param name="pen">Pen used to draw the line.</param>
        /// <param name="left">Left endpoint.</param>
        /// <param name="right">Right endpoint.</param>
        /// <param name="onscreen">Draw the line on the screen?</param>
        public void DrawLine(Pen pen, Point left, Point right, bool onscreen)
        {
            Graphics g = this.GetGraphics(onscreen);
            g.DrawLine(pen, left, right);
        }

        /// <summary>
        /// Draw a line in draw coordinates between two given endpoints.
        /// </summary>
        /// <param name="pen">Pen to use.</param>
        /// <param name="left">Left endpoint, relative to the DrawingArea.</param>
        /// <param name="right">Right endpoint, relative to the DrawingArea.</param>
        /// <param name="onscreen">Should the drawing be made just on screen (transient), 
        ///     or on the backing storage (persistent)?</param>
        /// <returns>True if line is visible.</returns>
        public void DrawLine(Pen pen, Point2D left, Point2D right, bool onscreen)
        {
            if (!Visible(left, right))
                return;

            Point l = Scale(left);
            Point r = Scale(right);
            this.DrawLine(pen, l, r, onscreen);
            return;
        }

        /// <summary>
        /// Draw a rectangle in screen coordinates.
        /// <param name="left">Left corner.</param>
        /// <param name="right">Right corner.</param>
        /// <param name="pen">Pen to draw.</param>
        /// </summary>
        public void DrawRectangle(Point left, Point right, Pen pen)
        {
            this.grph.DrawRectangle(pen, left.X, left.Y, right.X - left.X, right.Y - left.Y);
        }

        /// <summary>
        /// Check if a point is visible.
        /// </summary>
        /// <param name="x">Point x coordinate (unscaled).</param>
        /// <param name="y">Point y coordinate (unscaled).</param>
        /// <returns>True if the point is visible.</returns>
        public bool Visible(double x, double y)
        {
            if (x < this.xDrawMin || x > this.xDrawMax)
                return false;
            if (y < this.yDrawMin || y > this.yDrawMax)
                return false;
            return true;
        }

        /// <summary>
        /// True if a point is visible on canvas.
        /// </summary>
        /// <param name="p">Point to test.</param>
        /// <returns>True if point is visible.</returns>
        public bool Visible(Point2D p)
        {
            return this.Visible(p.X, p.Y);
        }

        /// <summary>
        /// Is any of these two points visible?
        /// </summary>
        /// <param name="left">Left endpoint.</param>
        /// <param name="right">Right endpoint.</param>
        /// <returns>True if any endpoint is on the screen.</returns>
        private bool Visible(Point2D left, Point2D right)
        {
            return this.Visible(left) || this.Visible(right);
        }

        /// <summary>
        /// Draw a filled rectangle with the given color in the drawing area.
        /// </summary>
        /// <param name="b">Brush to use for filling.</param>
        /// <param name="rect">Rectangle, relative to the DrawingArea.</param>
        public void FillRectangle(Brush b, Rectangle2D rect)
        {
            Rectangle scaledRect = this.Scale(rect);
            this.grph.FillRectangle(b, scaledRect);
        }

        /// <summary>
        /// Draw a rectangle with the given color in the drawing area.
        /// </summary>
        /// <param name="rect">Rectangle, relative to the DrawingArea.</param>
        /// <param name="pen">Pen used to draw rectangle.</param>
        public void DrawRectangle(Rectangle2D rect, Pen pen)
        {
            Rectangle scaledRect = this.Scale(rect);
            this.grph.DrawRectangle(pen, scaledRect);
        }

        /// <summary>
        /// Draw a rectangle filled with a specific brush.
        /// </summary>
        /// <param name="b">Brush to use to fill.</param>
        /// <param name="rect">Rectangle to draw.</param>
        public void DrawRectangle(Brush b, Rectangle2D rect)
        {
            Rectangle scaledRect = this.Scale(rect);
            this.grph.FillRectangle(b, scaledRect);
        }

        /// <summary>
        /// Draw a rectangle with the given color in the drawing area.
        /// </summary>
        /// <param name="c">Color to use.</param>
        /// <param name="vertices">Polygon vertices relative to the DrawingArea.</param>
        /// <param name="filled">Fill the polygon with the given color.</param>
        /// <param name="pen">Pen used to draw polygon.</param>
        public void DrawPolygon(Color c, Point2D [] vertices, Pen pen, bool filled)
        {
            Point[] points = new Point[vertices.Length];
            for (int i = 0; i < vertices.Length; i++)
            {
                points[i] = this.Scale(vertices[i]);
            }

            if (filled)
            {
                Brush b = new System.Drawing.SolidBrush(c);
                this.grph.FillPolygon(b, points);
            }
            else
            {
                this.grph.DrawPolygon(pen, points);
            }
        }

        /// <summary>
        /// Write some text in the drawing area, in absolute coordinates.
        /// </summary>
        /// <param name="c">Text color.</param>
        /// <param name="text">Text string.</param>
        /// <param name="font">Font used to draw text.</param>
        /// <param name="x">Upper-left corner x coordinate, in pixels.</param>
        /// <param name="y">Upper-left corner y coordinate, in pixels.</param>
        public void DrawTextAbsoluteCoordinates(Color c, Font font, string text, int x, int y)
        {
            this.grph.DrawString(text, font, new System.Drawing.SolidBrush(c), x, y);
        }

        /// <summary>
        /// Draw text rotated with 90 degrees.
        /// </summary>
        /// <param name="c">Text color.</param>
        /// <param name="text">Text to draw.</param>
        /// <param name="font">Font to use for text.</param>
        /// <param name="x">X coordinate.</param>
        /// <param name="y">Y coordinate.</param>
        public void DrawRotatedTextAbsoluteCoordinates(Color c, string text, Font font, int x, int y)
        {
            // bring the origin in the center
            this.grph.TranslateTransform(x, y);
            this.grph.RotateTransform(-90);
            this.grph.TranslateTransform(-x, -y);

            this.grph.DrawString(text, font, new System.Drawing.SolidBrush(c), x, y);
            this.grph.ResetTransform();
        }

        /// <summary>
        /// Draw text rotated with 90 degrees, right-justified.
        /// </summary>
        /// <param name="text">Text to draw.</param>
        /// <param name="x">X coordinate.</param>
        /// <param name="y">Y coordinate.</param>
        /// <param name="brush">Brush to use for text.</param>
        /// <param name="font">Font to use for text.</param>
        /// <param name="angleDegrees">Angle of rotation in degrees.</param>
        public void DrawRotatedRightJustifiedTextAbsoluteCoordinates(string text, Font font, Brush brush, int x, int y, int angleDegrees)
        {
            // bring the origin in the center
            this.grph.TranslateTransform(x, y);
            this.grph.RotateTransform(angleDegrees);
            this.grph.TranslateTransform(-x, -y);

            Size textsize = this.MeasureString(text, font);
            Rectangle box = new Rectangle(x - textsize.Width, y, textsize.Width + 2, textsize.Height);
            this.grph.DrawString(text, font, brush, box, new StringFormat());
            this.grph.ResetTransform();
        }

        /// <summary>
        /// Write some text in the drawing area, in absolute coordinates.
        /// </summary>
        /// <param name="text">Text to write.</param>
        /// <param name="brush">Brush to use for text.</param>
        /// <param name="font">Font to use for text.</param>
        /// <param name="x">Upper-right corner x coordinate, in pixels.</param>
        /// <param name="y">y coordinate of center, in pixels.</param>
        public void DrawRightJustifiedTextAbsoluteCoordinates(string text, Font font, Brush brush, int x, int y)
        {
            Size textsize = this.MeasureString(text, font);
            Rectangle box = new Rectangle(x - textsize.Width, y - textsize.Height / 2, textsize.Width + 2, textsize.Height);
            this.grph.DrawString(text, font, brush, box, new StringFormat());
        }

        /// <summary>
        /// Chose an appropriate font to fit the text in a given box.
        /// </summary>
        /// <param name="graph">Graphics context where the string will be drawn.</param>
        /// <param name="baseFont">Font to enlarge/shrink.</param>
        /// <param name="spaceAvailable">How much space is available for the string, in pixels.</param>
        /// <param name="text">String to write.</param>
        /// <returns>A suitably-sized font.</returns>
        public static Font ChooseFont(Graphics graph, Font baseFont, Size spaceAvailable, string text)
        {
            SizeF size = graph.MeasureString(text, baseFont);
            double scaling = Math.Min(spaceAvailable.Width / size.Width, spaceAvailable.Height / size.Height);
            double newSize = baseFont.Size * scaling;
            if (newSize < 2)
                // too small
                return null;
            Font retval = new Font(baseFont.FontFamily, (float)newSize, baseFont.Style);
            return retval;
        }

        /// <summary>
        /// Draw a one-line string to fit in the specified rectangle.
        /// </summary>
        /// <param name="text">Text to draw.</param>
        /// <param name="baseFont">Start from this font, but resize it.</param>
        /// <param name="brush">Brush to use for drawing.</param>
        /// <param name="place">Where to draw it.</param>
        public void DrawTextInRectangle(string text, Brush brush, Font baseFont, Rectangle2D place)
        {
            Rectangle dest = this.Scale(place);
            Font font = DrawingSurface2D.ChooseFont(this.grph, baseFont, dest.Size, text);
            if (font == null || font.Size < 3)
                // too small anyway
                return;
            SizeF actualSize = this.grph.MeasureString(text, font);
            int adjustX = (int)(dest.Width - actualSize.Width) / 2;
            int adjustY = dest.Height / 2;
            this.DrawRightJustifiedTextAbsoluteCoordinates(text, font, brush, dest.Right - adjustX, dest.Top + adjustY);
        }

        /// <summary>
        /// Write some text in the drawing area.  Font is not scaled.
        /// </summary>
        /// <param name="text">Text to write.</param>
        /// <param name="brush">Brush to use for text.</param>
        /// <param name="font">Font to use for text.</param>
        /// <param name="x">Upper-right corner x coordinate.</param>
        /// <param name="y">Upper-right corner y coordinate.</param>
        public void DrawRightJustifiedText(string text, Font font, Brush brush, double x, double y)
        {
            int xo, yo;
            this.Scale(x, y, out xo, out yo);
            Size textsize = this.MeasureString(text, font);
            Rectangle box = new Rectangle(xo - textsize.Width, yo, textsize.Width + 2, textsize.Height);
            this.grph.DrawString(text, font, brush, box, new StringFormat());
        }

        /// <summary>
        /// Draw a point in screen pixel coordinates (really a small circle).
        /// </summary>
        /// <param name="brush">Brush used to draw the point.</param>
        /// <param name="x">Point x coordinate.</param>
        /// <param name="y">Point y coordinate.</param>
        /// <param name="dotSize">Size of dot to draw.</param>
        /// <param name="onscreen">If true draw the point directly on the screen, else on the backing buffer.</param>
        public void DrawPointAbsoluteCoordinates(Brush brush, int x, int y, int dotSize, bool onscreen)
        {
            Graphics g = this.GetGraphics(onscreen);
            g.FillEllipse(brush, x - dotSize / 2, y - dotSize / 2, dotSize, dotSize);
        }

        /// <summary>
        /// Draw a "fat" point in the drawing area.
        /// </summary>
        /// <param name="brush">Brush used to draw point.</param>
        /// <param name="x">Point x coordinate.</param>
        /// <param name="y">Point y coordinate.</param>
        /// <param name="dotSize">Size of dot to draw.</param>
        /// <param name="onscreen">If true draw the point directly on screen, otherwise to the backing buffer.</param>
        /// <returns>True if point is visible.</returns>
        public bool DrawPoint(Brush brush, double x, double y, int dotSize, bool onscreen)
        {
            if (!this.Visible(x, y))
                return false;

            int x1, y1;
            this.Scale(x, y, out x1, out y1);
            this.DrawPointAbsoluteCoordinates(brush, x1, y1, dotSize, onscreen);
            return true;
        }

        /// <summary>
        /// Clear the drawing area.
        /// </summary>
        public void Clear()
        {
            this.grph.Clear(drawingpanel.BackColor);
        }

        /// <summary>
        /// Invoked when the panel has been resized.
        /// <returns>True if the size has changed and is not zero, false otherwise.</returns>
        /// </summary>
        public bool Resize()
        {
            if (this.oldsize == this.drawingpanel.Size)
                return false;

            if (this.drawingpanel.Size.Width == 0 &&
                this.drawingpanel.Size.Height == 0)
                // window has been minimized; do nothing
                return false;

            this.oldsize = this.drawingpanel.Size;
            this.RecomputeCachedValues();
            if (this.screenGrph != null)
                this.screenGrph.Dispose();
            this.screenGrph = this.drawingpanel.CreateGraphics();
            if (this.buffer != null)
            {
                if (this.grph != null)
                    this.grph.Dispose();
                if (this.drawingpanel.Width != 0 &&
                    this.drawingpanel.Height != 0)
                {
                    // when minimizing the window the size of the panel can be 0
                    this.buffer.Dispose();
                    this.buffer = new Bitmap(this.drawingpanel.Width, this.drawingpanel.Height);
                    this.grph = Graphics.FromImage(buffer);
                }
            }
            else
                this.grph = this.screenGrph;
            this.grph.SmoothingMode = SmoothingMode.AntiAlias;
            return true;
        }

        /// <summary>
        /// Paint the graphics area from the bitmap.
        /// </summary>
        public void Repaint(PaintEventArgs e)
        {
            e.Graphics.DrawImageUnscaled(buffer, 0, 0);
        }

        /// <summary>
        /// Scale and normalize a rectangle.
        /// </summary>
        /// <param name="rectangle">Rectangle to normalize.</param>
        /// <returns>Corresponding rectangle in pixels.</returns>
        private Rectangle Scale(Rectangle2D rectangle)
        {
            int xl1, yl1, xr1, yr1;

            this.Scale(rectangle.Corner1, out xl1, out yl1);
            this.Scale(rectangle.Corner2, out xr1, out yr1);
            int deltax = xr1 - xl1;
            int deltay = yr1 - yl1;
            if (deltax < 0)
            {
                xl1 += deltax;
                deltax = -deltax;
            }
            if (deltax == 0)
            {
                // zero is not shown, make it at least 1.
                deltax = 1;
            }
            if (deltay < 0)
            {
                yl1 += deltay;
                deltay = -deltay;
            }
            if (deltay == 0)
            {
                // zero is not shown, make it at least 1.
                deltay = 1;
            }

            Rectangle retval = new Rectangle(new Point(xl1, yl1), new Size(deltax, deltay));
            return retval;
        }

        /// <summary>
        /// Draw an ellipse.
        /// </summary>
        /// <param name="color">Fill color.</param>
        /// <param name="rect">Rectangle enclosing ellipse.</param>
        /// <param name="pen">Pen to use.</param>
        /// <param name="filled">If true, fill the ellipse.</param>
        internal void DrawEllipse(Color color, Rectangle2D rect, Pen pen, bool filled)
        {
            Rectangle scaledRect = this.Scale(rect);

            if (filled)
            {
                Brush b = new System.Drawing.SolidBrush(color);
                this.grph.FillEllipse(b, scaledRect);
            }
            else
            {
                this.grph.DrawEllipse(pen, scaledRect);
            }
        }

        /// <summary>
        /// The image plotted.
        /// </summary>
        /// <returns>A bitmap.</returns>
        public Bitmap PlottedImage()
        {
            return this.buffer;
        }
    }
}
