
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

using System.Windows.Forms;

namespace Microsoft.Research.UsefulForms
{
    /// <summary>
    /// Prompt user for password.
    /// </summary>
    public partial class PasswordDialog : Form
    {
        /// <summary>
        /// User name.
        /// </summary>
        public string Domain 
        {
            get
            {
                int indexOfBackslash = this.textBox_username.Text.IndexOf('\\');
                if (indexOfBackslash >= 0)
                    return this.textBox_username.Text.Substring(0, indexOfBackslash);
                else
                    return this.textBox_username.Text;
            }
        }

        /// <summary>
        /// Domain name.
        /// </summary>
        public string User
        {
            get
            {
                int indexOfBackslash = this.textBox_username.Text.IndexOf('\\');
                if (indexOfBackslash >= 0)
                    return this.textBox_username.Text.Substring(indexOfBackslash+1);
                else
                    return "";
            }
        }

        /// <summary>
        /// User password.
        /// </summary>
        public string Password
        {
            get
            {
                return this.textBox_password.Text;
            }
        }

        /// <summary>
        /// Show a dialog asking for a password.
        /// </summary>
        /// <param name="cluster">Cluster to authenticate to.</param>
        /// <param name="domain">Domain to authenticate to.</param>
        /// <param name="user">User to authenticate as.</param>
        public PasswordDialog(string cluster, string domain, string user)
        {
            InitializeComponent();
            this.Text = "Please authenticate to " + cluster;
            this.textBox_username.Text = domain + "\\" + user;
        }

        /// <summary>
        /// User pressed key in text box.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void textBox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == '\r')
            {
                this.DialogResult = DialogResult.OK;
            }
        }
    }
}
