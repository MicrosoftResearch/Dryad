
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
using System.Windows.Forms;
    
namespace Microsoft.Research.Calypso.Tools
{
    /// <summary>
    /// A data grid that filters the contents.
    /// </summary>
    public partial class FilteredDataGridView : UserControl
    {
        IDGVData data;
        /// <summary>
        /// If true apply the complement of the current filter.
        /// </summary>
        bool complementFilter;

        /// <summary>
        /// Create a filtered data 
        /// </summary>
        public FilteredDataGridView()
        {
            this.complementFilter = false;
            this.InitializeComponent();
            ToolTip tip = new ToolTip();
            tip.SetToolTip(this.button_drop, "Remove the rows matching the filter string");
            tip.SetToolTip(this.button_filter, "Keep the rows matching the filter string");
            tip.SetToolTip(this.button_X, "Remove all applied filters");
            tip.SetToolTip(this.textBox_filter, "String to perform filtering on");
        }

        /// <summary>
        /// Bind to the data source.
        /// </summary>
        /// <param name="dataSource">Value to set data to.</param>
        public void SetDataSource(IDGVData dataSource)
        {
            this.data = dataSource;
            this.dataGridView.DataSource = dataSource.VisibleItems;
            this.ApplyFilter(this.complementFilter, false);
            this.data.ItemsChanged += this.ItemsChanged;
            this.data.ItemAppended += this.ItemAdded;
        }

        /// <summary>
        /// Delegate called by DGV then a new item is added.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        public void ItemAdded(object sender, EventArgs e)
        {
            this.ApplyFilter(this.complementFilter, true);
        }

        /// <summary>
        /// Delegate called by DGV when the items change.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        public void ItemsChanged(object sender, EventArgs e)
        {
            this.ApplyFilter(this.complementFilter, false);
        }

        /// <summary>
        /// Handle to the datagridview in the form.
        /// </summary>
        public DataGridView DataGridView { get { return this.dataGridView; } }

        /// <summary>
        /// Used clicked the button to filter the data displayed.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_filter_Click(object sender, EventArgs e)
        {
            this.complementFilter = false;
            this.ApplyFilter(false, false);
        }

        /// <summary>
        /// Used clicked button to remove filter.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_X_Click(object sender, EventArgs e)
        {
            this.textBox_filter.Text = "";
            this.complementFilter = false;
            this.data.RemoveFilter();
            if (this.ViewChanged != null)
                this.ViewChanged(this, EventArgs.Empty);
        }

        /// <summary>
        /// Apply the current filter.
        /// <param name="complement">If true, complement the selection.</param>
        /// <param name="lastOnly">Only filter the last value.</param>
        /// </summary>
        public void ApplyFilter(bool complement, bool lastOnly)
        {
            int firstIndex = lastOnly ? this.DataGridView.Rows.Count - 1 : 0;
            int lastIndex = this.DataGridView.Rows.Count;
            this.data.Filter(this.textBox_filter.Text, this.DataGridView, firstIndex, lastIndex, complement);
            if (this.ViewChanged != null)
                this.ViewChanged(this, EventArgs.Empty);
        }

        /// <summary>
        /// User pressed a key in the filter textbox.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Key description.</param>
        private void textBox_filter_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
                this.ApplyFilter(false, false);
        }

        /// <summary>
        /// Invoked when a cell needs to be formatted.
        /// </summary>
        public event DataGridViewCellFormattingEventHandler CellFormatting;
        /// <summary>
        /// Invoked when a cell has been double-clicked.
        /// </summary>
        public event DataGridViewCellMouseEventHandler CellMouseDoubleClick;
        /// <summary>
        /// Invoked when the view has been changed.
        /// </summary>
        public event EventHandler ViewChanged;

        private void dataGridView_CellFormatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            if (this.CellFormatting != null)
                this.CellFormatting(sender, e);
        }

        private void dataGridView_CellMouseDoubleClick(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (this.CellMouseDoubleClick != null)
                this.CellMouseDoubleClick(sender, e);
        }

        /// <summary>
        /// Right mouse button clicked in a row of the datagrid.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Event describing the mouse click.</param>
        private void dataGridView_CellMouseDown(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.Button == MouseButtons.Right && e.ColumnIndex >= 0 && e.RowIndex >= 0)
            {
                if (this.DataGridView.Rows[e.RowIndex].Selected == false)
                {
                    this.DataGridView.ClearSelection();
                    this.DataGridView.Rows[e.RowIndex].Selected = true;
                }
                this.DataGridView.CurrentCell = this.DataGridView.Rows[e.RowIndex].Cells[e.ColumnIndex];
            }
        }

        /// <summary>
        /// Redo the last sort; return true if there was a last sort.
        /// </summary>
        /// <returns>False if the data was never sorted.</returns>
        public bool RedoSort()
        {
            bool sorted = this.data.RedoSort();
            if (this.ViewChanged != null)
                this.ViewChanged(this, EventArgs.Empty);
            return sorted;
        }

        /// <summary>
        /// Drop the text from the grid.
        /// </summary>
        /// <param name="sender">Unused.</param>
        /// <param name="e">Unused.</param>
        private void button_drop_Click(object sender, EventArgs e)
        {
            this.complementFilter = true;
            this.ApplyFilter(true, false);
        }
    }

    /// <summary>
    /// Untyped interface for data bound to a filtered data grid view.
    /// </summary>
    public interface IDGVData
    {
        /// <summary>
        /// Items to display; should be a BindingList.
        /// </summary>
        object VisibleItems { get; }
        /// <summary>
        /// Perform filtering with this text.
        /// </summary>
        /// <param name="text">Text to find.</param>
        /// <param name="grid">Data grid where text is sought.</param>
        /// <returns>Number of matches.</returns>
        /// <param name="complement">If true then complement the selection.</param>
        /// <param name="endIndex">Last index to filter.</param>
        /// <param name="startIndex">First index to filter.</param>
        int Filter(string text, DataGridView grid, int startIndex, int endIndex, bool complement);
        /// <summary>
        /// Remove any filter applied.
        /// </summary>
        /// <returns>The number of items.</returns>
        int RemoveFilter();
        /// <summary>
        /// Event raised when the list changes.
        /// </summary>
        event EventHandler ItemsChanged;
        /// <summary>
        /// An item has been added.
        /// </summary>
        event EventHandler ItemAppended;
        /// <summary>
        /// Redo the sort as done the last time.
        /// Return true if there was a previous sort.
        /// </summary>
        bool RedoSort();
    }

    /// <summary>
    /// Data for a filtered data grid view; can't have a type parameter in a form, so we segregate it here.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DGVData<T> : IDGVData
    {
        /// <summary>
        /// Data items which are filtered.
        /// </summary>
        List<T> dataItems;
        /// <summary>
        /// Items displayed.
        /// </summary>
        BindingListSortable<T> visibleItems;

        /// <summary>
        /// If true filtering is case sensitive.
        /// </summary>
        public bool CaseSensitive { get; set; }

        /// <summary>
        /// Number of visible items.
        /// </summary>
        public int VisibleItemCount { get { return this.visibleItems.Count; } }

        /// <summary>
        /// Items that are visible: a binding list sortable.
        /// </summary>
        public object VisibleItems { get { return this.visibleItems;  } }

        /// <summary>
        /// Clear the contents.
        /// </summary>
        public void Clear()
        {
            this.visibleItems.Clear();
            this.dataItems.Clear();
        }

        /// <summary>
        /// Delegate called when the items change.
        /// </summary>
        public event EventHandler ItemsChanged;
        /// <summary>
        /// Delegate called when an item is added.
        /// </summary>
        public event EventHandler ItemAppended;

        /// <summary>
        /// Raise the changed event if required.
        /// </summary>
        private void OnChanged()
        {
            if (this.ItemsChanged != null)
                this.ItemsChanged(this, EventArgs.Empty);
        }

        /// <summary>
        /// Raise the changed event if required.
        /// </summary>
        private void OnItemAppended()
        {
            if (this.ItemAppended != null)
                this.ItemAppended(this, EventArgs.Empty);
        }

        /// <summary>
        /// Create an empty DGVData object.
        /// </summary>
        public DGVData()
        {
            this.visibleItems = new BindingListSortable<T>();
            this.dataItems = new List<T>();
            this.OnChanged();
        }

        /// <summary>
        /// Redo the last sorting operation.
        /// </summary>
        public bool RedoSort()
        {
            return this.visibleItems.RedoSort();
        }

        /// <summary>
        /// Set the items to display.
        /// </summary>
        /// <param name="items">Collection of items to display.</param>
        public void SetItems(IEnumerable<T> items)
        {
            this.Clear();
            this.dataItems.AddRange(items);
            this.RemoveFilter();
            this.OnChanged();
        }

        /// <summary>
        /// True if the text is found in any cell of the row.
        /// </summary>
        /// <param name="tofind">Text to search.</param>
        /// <param name="row">A datagridview row with arbitrary contents.</param>
        /// <returns>True if any cell contains the text.</returns>
        /// <param name="caseSensitive">If true, do a case-sensitive match.</param>
        private static bool DataGridViewRowMatches(string tofind, DataGridViewRow row, bool caseSensitive)
        {
            for (int i = 0; i < row.Cells.Count; i++)
            {
                var cell = row.Cells[i];
                if (!cell.Visible) continue;
                object inside = cell.FormattedValue;
                string text = inside as string;
                if (text == null)
                    continue;
                if (!caseSensitive)
                    text = text.ToLower();
                if (text.Contains(tofind))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Return the list of all indices of rows matching a specified string.
        /// </summary>
        /// <param name="tofind">String to find in a datagridview.</param>
        /// <param name="grid">Datagrid view to search.</param>
        /// <returns>A list with all matching row indices.</returns>
        /// <param name="caseSensitive">If true do a case-sensitive match.</param>
        /// <param name="exclusive">If true, return the complement of the set.</param>
        public static List<int> DataGridViewRowIndicesMatching(string tofind, DataGridView grid, bool caseSensitive, bool exclusive, int startIndex, int endIndex)
        {
            if (!caseSensitive)
                tofind = tofind.ToLower();

            List<int> retval = new List<int>();
            for (int i = Math.Max(0, startIndex); i < Math.Min(grid.Rows.Count, endIndex); i++)
            {
                var row = grid.Rows[i];
                bool matches = DataGridViewRowMatches(tofind, row, caseSensitive);
                if (matches ^ exclusive)
                    retval.Add(i);
            }
            return retval;
        }

        /// <summary>
        /// Return the list of all indices of rows matching a specified string.
        /// </summary>
        /// <param name="tofind">String to find in a datagridview.</param>
        /// <param name="grid">Datagrid view to search.</param>
        /// <returns>A list with all matching row indices.</returns>
        /// <param name="caseSensitive">If true do a case-sensitive match.</param>
        /// <param name="exclusive">If true, return the complement of the set.</param>
        public static List<int> DataGridViewRowIndicesMatching(string tofind, DataGridView grid, bool caseSensitive, bool exclusive)
        {
            return DataGridViewRowIndicesMatching(tofind, grid, caseSensitive, exclusive, 0, grid.Rows.Count);
        }

        /// <summary>
        /// Stop filtering jobs; return number of jobs.
        /// </summary>
        public int RemoveFilter()
        {
            if (this.dataItems == null)
                return 0;
            this.visibleItems.RaiseListChangedEvents = false;
            this.visibleItems.Clear();
            foreach (var ji in this.dataItems)
                this.visibleItems.Add(ji);
            this.visibleItems.RaiseListChangedEvents = true;
            this.visibleItems.RedoSort();
            this.visibleItems.ResetBindings();
            return this.dataItems.Count;
        }

        /// <summary>
        /// Apply the filter.  Return the number of matches.  If the filter is empty do not do anything.
        /// <param name="datagrid">Data grid to filter.</param>
        /// <param name="filter">Text to fine.</param>
        /// <param name="complement">If true complement the selection.</param>
        /// <param name="startIndex">First index to filter.</param>
        /// <param name="endIndex">Last index to filter.</param>
        /// </summary>
        public int Filter(string filter, DataGridView datagrid, int startIndex, int endIndex, bool complement)
        {
            int matches = datagrid.Rows.Count;
            if (filter == "")
                this.RemoveFilter();
             
            List<int> toRemove = DataGridViewRowIndicesMatching(filter, datagrid, this.CaseSensitive, !complement, startIndex, endIndex);
            matches -= toRemove.Count;
            this.visibleItems.RaiseListChangedEvents = false;
            toRemove.Sort();
            toRemove.Reverse(); // do it backwards to keep indices unchanged
            foreach (int index in toRemove)
            {
                this.visibleItems.RemoveAt(index);
            }
            this.visibleItems.RaiseListChangedEvents = true;
            this.visibleItems.ResetBindings();
            return matches;
        }

        /// <summary>
        /// Add a new item to the data grid view.
        /// </summary>
        /// <param name="item">Item to add.</param>
        public void AddItem(T item)
        {
            this.dataItems.Add(item);
            this.OnItemAppended();
        }
    }
}
