
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
namespace Microsoft.Research.JobObjectModel
{
    partial class ClusterConfigEditor
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
            this.tableLayoutPanel_buttons = new System.Windows.Forms.TableLayoutPanel();
            this.buttonOK = new System.Windows.Forms.Button();
            this.buttonCancel = new System.Windows.Forms.Button();
            this.tableLayoutPanel_properties = new System.Windows.Forms.TableLayoutPanel();
            this.textBox_name = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.comboBox_clusterType = new System.Windows.Forms.ComboBox();
            this.panel1 = new System.Windows.Forms.Panel();
            this.tableLayoutPanel_buttons.SuspendLayout();
            this.tableLayoutPanel_properties.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel_buttons
            // 
            this.tableLayoutPanel_buttons.ColumnCount = 2;
            this.tableLayoutPanel_buttons.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel_buttons.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel_buttons.Controls.Add(this.buttonCancel, 1, 0);
            this.tableLayoutPanel_buttons.Controls.Add(this.buttonOK, 0, 0);
            this.tableLayoutPanel_buttons.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.tableLayoutPanel_buttons.Location = new System.Drawing.Point(0, 143);
            this.tableLayoutPanel_buttons.Name = "tableLayoutPanel_buttons";
            this.tableLayoutPanel_buttons.RowCount = 1;
            this.tableLayoutPanel_buttons.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel_buttons.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 20F));
            this.tableLayoutPanel_buttons.Size = new System.Drawing.Size(381, 33);
            this.tableLayoutPanel_buttons.TabIndex = 0;
            // 
            // buttonOK
            // 
            this.buttonOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.buttonOK.Dock = System.Windows.Forms.DockStyle.Fill;
            this.buttonOK.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.buttonOK.Location = new System.Drawing.Point(3, 3);
            this.buttonOK.Name = "buttonOK";
            this.buttonOK.Size = new System.Drawing.Size(184, 27);
            this.buttonOK.TabIndex = 0;
            this.buttonOK.Text = "OK";
            this.buttonOK.UseVisualStyleBackColor = true;
            // 
            // buttonCancel
            // 
            this.buttonCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.buttonCancel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.buttonCancel.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.buttonCancel.Location = new System.Drawing.Point(193, 3);
            this.buttonCancel.Name = "buttonCancel";
            this.buttonCancel.Size = new System.Drawing.Size(185, 27);
            this.buttonCancel.TabIndex = 1;
            this.buttonCancel.Text = "Cancel";
            this.buttonCancel.UseVisualStyleBackColor = true;
            // 
            // tableLayoutPanel_properties
            // 
            this.tableLayoutPanel_properties.AutoSize = true;
            this.tableLayoutPanel_properties.ColumnCount = 2;
            this.tableLayoutPanel_properties.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle());
            this.tableLayoutPanel_properties.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel_properties.Controls.Add(this.label2, 0, 0);
            this.tableLayoutPanel_properties.Controls.Add(this.textBox_name, 1, 0);
            this.tableLayoutPanel_properties.Controls.Add(this.label1, 0, 1);
            this.tableLayoutPanel_properties.Controls.Add(this.comboBox_clusterType, 1, 1);
            this.tableLayoutPanel_properties.Dock = System.Windows.Forms.DockStyle.Top;
            this.tableLayoutPanel_properties.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel_properties.Name = "tableLayoutPanel_properties";
            this.tableLayoutPanel_properties.RowCount = 2;
            this.tableLayoutPanel_properties.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel_properties.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel_properties.Size = new System.Drawing.Size(381, 54);
            this.tableLayoutPanel_properties.TabIndex = 1;
            // 
            // textBox_name
            // 
            this.textBox_name.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBox_name.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBox_name.Location = new System.Drawing.Point(89, 3);
            this.textBox_name.Name = "textBox_name";
            this.textBox_name.Size = new System.Drawing.Size(289, 21);
            this.textBox_name.TabIndex = 3;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(3, 27);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(80, 27);
            this.label1.TabIndex = 4;
            this.label1.Text = "Cluster type";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(3, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(80, 27);
            this.label2.TabIndex = 5;
            this.label2.Text = "Cluster name";
            this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // comboBox_clusterType
            // 
            this.comboBox_clusterType.Dock = System.Windows.Forms.DockStyle.Fill;
            this.comboBox_clusterType.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.comboBox_clusterType.FormattingEnabled = true;
            this.comboBox_clusterType.Location = new System.Drawing.Point(89, 30);
            this.comboBox_clusterType.Name = "comboBox_clusterType";
            this.comboBox_clusterType.Size = new System.Drawing.Size(289, 23);
            this.comboBox_clusterType.TabIndex = 6;
            this.comboBox_clusterType.SelectedIndexChanged += new System.EventHandler(this.comboBox_clusterType_SelectedIndexChanged);
            // 
            // panel1
            // 
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(0, 54);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(381, 89);
            this.panel1.TabIndex = 2;
            // 
            // ClusterConfigEditor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(381, 176);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.tableLayoutPanel_properties);
            this.Controls.Add(this.tableLayoutPanel_buttons);
            this.Name = "ClusterConfigEditor";
            this.Text = "Cluster Editor";
            this.tableLayoutPanel_buttons.ResumeLayout(false);
            this.tableLayoutPanel_properties.ResumeLayout(false);
            this.tableLayoutPanel_properties.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TableLayoutPanel tableLayoutPanel_buttons;
        private System.Windows.Forms.Button buttonCancel;
        private System.Windows.Forms.Button buttonOK;
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanel_properties;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBox_name;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ComboBox comboBox_clusterType;
        private System.Windows.Forms.Panel panel1;

    }
}
