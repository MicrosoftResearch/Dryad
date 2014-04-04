
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

using System.Collections.Generic;
using Microsoft.Research.Calypso.JobObjectModel;
using System;
using System.Windows.Forms;

namespace Microsoft.Research.Calypso.DryadAnalysis
{
    /// <summary>
    /// Display the diagnosis results.
    /// </summary>
    public partial class DiagnosisResult : Form
    {
        DryadLinqJobInfo job;

        /// <summary>
        /// Create a form to show the diagnosis result.
        /// </summary>
        /// <param name="job">Job diagnosed; may be null.</param>
        /// <param name="log">Diagnosis log.</param>
        /// <param name="summary">Job summary.</param>
        public DiagnosisResult(DryadLinqJobInfo job, DryadLinqJobSummary summary, DiagnosisLog log)
        {
            this.InitializeComponent();
            this.job = job;
            if (this.job == null)
                this.button_job.Enabled = false;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            this.Text = "Diagnosis results for " + summary.Name + " " + summary.Date;
            this.textBox_job.Text = "Job being diangosed: " + summary.AsIdentifyingString();
            foreach (string s in log.Message())
            {
                this.textBox_message.AppendText(s);
                this.textBox_message.AppendText(Environment.NewLine);
            }
        }

        /// <summary>
        /// Show the result of the diagnosis in a separate form.
        /// </summary>
        /// <param name="logs">List of diagnosis logs.</param>
        /// <param name="cancelled">True if diagnosis was cancelled.</param>
        public static void ShowDiagnosisResult(bool cancelled, List<DiagnosisLog> logs)
        {
            if (cancelled) return;

            foreach (var log in logs)
            {
                DiagnosisResult result = new DiagnosisResult(log.Job, log.Summary, log);
                result.Show();
            }
        }
        
        /// <summary>
        /// OK has been clicked.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_ok_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        /// <summary>
        /// User pressed the 'job' button.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_job_Click(object sender, EventArgs e)
        {
            JobBrowser jb = new JobBrowser(this.job);
            jb.Show();
        }
    }
}
