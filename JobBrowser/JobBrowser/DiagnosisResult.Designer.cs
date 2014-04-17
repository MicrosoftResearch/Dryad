
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
namespace Microsoft.Research.DryadAnalysis
{
    using System;

    partial class DiagnosisResult
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
            this.button_ok = new System.Windows.Forms.Button();
            this.textBox_message = new System.Windows.Forms.TextBox();
            this.textBox_job = new System.Windows.Forms.TextBox();
            this.button_job = new System.Windows.Forms.Button();
            this.panel_buttons = new System.Windows.Forms.Panel();
            this.panel_buttons.SuspendLayout();
            this.SuspendLayout();
            // 
            // button_ok
            // 
            this.button_ok.Dock = System.Windows.Forms.DockStyle.Fill;
            this.button_ok.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_ok.Location = new System.Drawing.Point(0, 0);
            this.button_ok.Name = "button_ok";
            this.button_ok.Size = new System.Drawing.Size(542, 30);
            this.button_ok.TabIndex = 0;
            this.button_ok.Text = "OK";
            this.button_ok.UseVisualStyleBackColor = true;
            this.button_ok.Click += new System.EventHandler(this.button_ok_Click);
            // 
            // textBox_message
            // 
            this.textBox_message.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBox_message.Location = new System.Drawing.Point(0, 33);
            this.textBox_message.Multiline = true;
            this.textBox_message.Name = "textBox_message";
            this.textBox_message.ScrollBars = System.Windows.Forms.ScrollBars.Both;
            this.textBox_message.Size = new System.Drawing.Size(632, 236);
            this.textBox_message.TabIndex = 1;
            // 
            // textBox_job
            // 
            this.textBox_job.Dock = System.Windows.Forms.DockStyle.Top;
            this.textBox_job.Location = new System.Drawing.Point(0, 0);
            this.textBox_job.Multiline = true;
            this.textBox_job.Name = "textBox_job";
            this.textBox_job.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.textBox_job.Size = new System.Drawing.Size(632, 33);
            this.textBox_job.TabIndex = 2;
            // 
            // button_job
            // 
            this.button_job.Dock = System.Windows.Forms.DockStyle.Right;
            this.button_job.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_job.Location = new System.Drawing.Point(542, 0);
            this.button_job.Name = "button_job";
            this.button_job.Size = new System.Drawing.Size(90, 30);
            this.button_job.TabIndex = 3;
            this.button_job.Text = "Job browser";
            this.button_job.UseVisualStyleBackColor = true;
            this.button_job.Click += new System.EventHandler(this.button_job_Click);
            // 
            // panel_buttons
            // 
            this.panel_buttons.Controls.Add(this.button_ok);
            this.panel_buttons.Controls.Add(this.button_job);
            this.panel_buttons.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel_buttons.Location = new System.Drawing.Point(0, 269);
            this.panel_buttons.Name = "panel_buttons";
            this.panel_buttons.Size = new System.Drawing.Size(632, 30);
            this.panel_buttons.TabIndex = 4;
            // 
            // DiagnosisResult
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(632, 299);
            this.Controls.Add(this.textBox_message);
            this.Controls.Add(this.textBox_job);
            this.Controls.Add(this.panel_buttons);
            this.Name = "DiagnosisResult";
            this.Text = "Diagnosis Result";
            this.panel_buttons.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button button_ok;
        private System.Windows.Forms.TextBox textBox_message;
        private System.Windows.Forms.TextBox textBox_job;
        private System.Windows.Forms.Button button_job;
        private System.Windows.Forms.Panel panel_buttons;
    }
}
