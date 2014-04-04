
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

namespace Microsoft.Research.Calypso.UsefulForms
{
    using System.Windows.Forms;

    /// <summary>
    /// Custom dialog class.
    /// </summary>
    public partial class CustomDialog : Form
    {
        /// <summary>
        /// Create a custom dialog with the specified label.
        /// </summary>
        public CustomDialog(string label)
        {
            this.InitializeComponent();
            this.label_prompt.Text = label;
        }

        /// <summary>
        /// The input typed by the user.
        /// </summary>
        public string UserInput { get { return this.textBox_userInput.Text; } }
    }
}
