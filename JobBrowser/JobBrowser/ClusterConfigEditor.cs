
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
using System.Windows.Forms;
using Microsoft.Research.Tools;

namespace Microsoft.Research.JobObjectModel
{
    /// <summary>
    /// Editor for cluster configuration.
    /// </summary>
    public partial class ClusterConfigEditor : Form
    {
        /// <summary>
        /// Create a cluster configuration editor.
        /// </summary>
        public ClusterConfigEditor()
        {
            InitializeComponent();
            this.tableLayoutPanel_properties.GrowStyle = TableLayoutPanelGrowStyle.AddRows;
            this.config = null;
            this.addedControls = new List<Control>();
            this.propEditor = new Dictionary<string, TextBox>();

            for (var i = ClusterConfiguration.ClusterType.Unknown+1; i < ClusterConfiguration.ClusterType.MaxUnused; i++)
            {
                this.comboBox_clusterType.Items.Add(i.ToString());
            }
        }

        private ClusterConfiguration config;
        private bool canChangeType;

        /// <summary>
        /// Set the configuration to edit.  If null a new configuration will be created.
        /// </summary>
        /// <param name="configToEdit">Configuration to edit.</param>
        public void SetConfigToEdit(ClusterConfiguration configToEdit)
        {
            this.config = configToEdit;
            this.canChangeType = configToEdit == null;
            this.BindProperties();
        }

        private void BindProperties()
        {
            this.comboBox_clusterType.Enabled = this.canChangeType;
            if (this.config == null) return;
            var props = this.config.ExtractData();

            this.textBox_name.Text = props.Name;
            this.comboBox_clusterType.SelectedItem = props.Type.ToString();

            this.AddPropertiesToEdit(props.Properties);
        }

        private List<Control> addedControls;
        private Dictionary<string, TextBox> propEditor;

        /// <summary>
        /// Add some new controls to the table layout panel.
        /// </summary>
        /// <param name="props">Properties to add controls for.</param>
        private void AddPropertiesToEdit(List<PropertySetting> props)
        {
            foreach (var prop in props)
            {
                Label labl = new Label();
                labl.Text = prop.Property;
                this.tableLayoutPanel_properties.Controls.Add(labl);
                labl.Dock = DockStyle.Fill;
                labl.TextAlign = ContentAlignment.MiddleRight;

                TextBox tb = new TextBox();
                tb.Name = prop.Property;
                this.tableLayoutPanel_properties.Controls.Add(tb);
                tb.Dock = DockStyle.Fill;

                var crtValue = prop.Value;
                tb.Text = crtValue;

                this.addedControls.Add(labl);
                this.addedControls.Add(tb);
                this.propEditor.Add(prop.Property, tb);
            }
        }

        /// <summary>
        /// Remove all added properties.
        /// </summary>
        private void RemoveAddedProperties()
        {
            foreach (var ctrl in this.addedControls)
                this.tableLayoutPanel_properties.Controls.Remove(ctrl);
            this.addedControls.Clear();
            this.propEditor.Clear();
        }

        /// <summary>
        /// Get the edited configuration.
        /// </summary>
        /// <returns>The edited configuration.</returns>
        public ClusterConfiguration GetConfiguration()
        {
            var type = (ClusterConfiguration.ClusterType)Enum.Parse(typeof(ClusterConfiguration.ClusterType), this.comboBox_clusterType.Text);

            ClusterConfigurationSerialization ser = new ClusterConfigurationSerialization()
            {
                Name = this.textBox_name.Text,
                Type = type,
                Properties = new List<PropertySetting>()
            };

            foreach (var ctrl in this.propEditor)
            {
                ser.Properties.Add(new PropertySetting(ctrl.Key, ctrl.Value.Text));
            }

            ClusterConfiguration result = ser.Create();
            string error = result.Initialize();
            if (error == null)
                return result;
            else
            {
                MessageBox.Show("Error while initializing cluster configuration: " + error);
                return null;
            }
        }

        private void comboBox_clusterType_SelectedIndexChanged(object sender, EventArgs e)
        {
            // initial setting for an existing configuration
            if (this.config != null) return;

            this.RemoveAddedProperties();
            var type = (ClusterConfiguration.ClusterType)Enum.Parse(typeof(ClusterConfiguration.ClusterType), this.comboBox_clusterType.Text);
            this.config = ClusterConfiguration.CreateConfiguration(type);
            this.AddPropertiesToEdit(this.config.ExtractData().Properties);
        }
    }
}
