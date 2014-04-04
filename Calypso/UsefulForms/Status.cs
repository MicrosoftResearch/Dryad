
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

using System.Drawing;
using System;
using System.Windows.Forms;
    
namespace Microsoft.Research.Calypso.Tools
{
    /// <summary>
    /// Delegate used for invoking status messages from accross threads.
    /// </summary>
    /// <param name="message">Message to display.</param>
    /// <param name="statusKind">Severity of status.</param>
    public delegate void StatusDelegate(string message, StatusKind statusKind);   

    /// <summary>
    /// A class which writes status messages.
    /// It remembers the default color of the status strip.
    /// </summary>
    public class StatusWriter
    {
        /// <summary>
        /// The default color of the status strip; assumes that all status strips have the same color.
        /// </summary>
        private Color defaultStatusStripBackColor;
        /// <summary>
        /// Status strip used to display messages.
        /// </summary>
        private ToolStripStatusLabel label;
        /// <summary>
        /// The parent of the status strip, invoked when doing cross-thread operations.
        /// </summary>
        private Control parentControl;
        private StatusDelegate crossInvoke;

        /// <summary>
        /// Initialize a status writer.
        /// </summary>
        /// <param name="strip">Status strip holding the messages.</param>
        /// <param name="parent">Parent which contains the status writer.  
        /// (May be invoked recursively across threads).</param>
        /// <param name="crossInvoke">Delegate to invoke cross-threads in parent.</param>
        public StatusWriter(ToolStripStatusLabel strip, Control parent, StatusDelegate crossInvoke)
        {
            this.label = strip;
            this.parentControl = parent;
            this.defaultStatusStripBackColor = strip.BackColor;
            this.crossInvoke = crossInvoke;
        }

        /// <summary>
        /// Write a message on the status strip, with a given severity.
        /// </summary>
        /// <param name="message">Message to display.</param>
        /// <param name="kind">Severity of message.</param>
        public void Status(string message, StatusKind kind)
        {
            if (parentControl.InvokeRequired)
            {
                parentControl.Invoke(this.crossInvoke, new object[] { message, kind });
            }
            else
            {
                if (kind == StatusKind.Error)
                    this.label.BackColor = Color.Red;
                else
                    this.label.BackColor = defaultStatusStripBackColor;

                if (kind == StatusKind.LongOp)
                    System.Windows.Forms.Cursor.Current = System.Windows.Forms.Cursors.WaitCursor;
                else
                    System.Windows.Forms.Cursor.Current = System.Windows.Forms.Cursors.Default;

                this.label.Text = message;
                this.parentControl.Refresh();
                Console.WriteLine("{0}: {1}, {2}", DateTime.Now.ToString("hh:mm:ss.ff"), kind, message);
            }
        }
    }
}
