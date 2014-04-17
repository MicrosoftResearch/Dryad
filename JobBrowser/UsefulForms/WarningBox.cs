
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
using System.Windows.Forms;

namespace Microsoft.Research.UsefulForms
{
    /// <summary>
    /// A box displaying a warning, which can be turned off.
    /// </summary>
    public partial class WarningBox : Form
    {
        /// <summary>
        /// If true the action has been cancelled.
        /// </summary>
        public bool Cancelled { get; protected set; }

        /// <summary>
        /// Create a warning box for a specified message.
        /// </summary>
        /// <param name="message">Message to display in warning box.</param>
        public WarningBox(string message)
        {
            this.InitializeComponent();
            this.label_message.Text = message;
            this.Cancelled = false;
        }

        private void button_OK_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        /// <summary>
        /// If true this dialog should not be shown again.
        /// </summary>
        public bool DoNotShowAgain { get { return this.checkBox_doNotShowAgain.Checked; } }

        /// <summary>
        /// The cancel button has been clicked.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_Cancel_Click(object sender, EventArgs e)
        {
            this.Cancelled = true;
        }
     }
}
