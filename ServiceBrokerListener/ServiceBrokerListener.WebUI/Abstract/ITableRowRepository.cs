namespace ServiceBrokerListener.WebUI.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using ServiceBrokerListener.WebUI.Models;

    public interface ITableRowRepository
    {
        IEnumerable<TableRow> Rows { get; }

        void UpdateRow(int rowNumber, int colNumber, string value);

        event EventHandler<TableChangedEventArgs> TableChanged;
    }

    public class TableChangedEventArgs : EventArgs
    {
        public class SingleChange
        {
            public int Row { get; set; }
            public int Column { get; set; }
            public string NewValue { get; set; }

            public string ToJson()
            {
                return string.Format("[{0}, {1}, \"\", \"{2}\"]", Row, Column, NewValue);
            }
        }

        public TableChangedEventArgs(IEnumerable<SingleChange> changes)
        {
            Changes = changes.ToArray();
        }

        public IEnumerable<SingleChange> Changes { get; private set; }

        public string GetChangesAsJson()
        {
            return string.Format(
                "[{0}]",
                Changes.Aggregate<SingleChange, string>(
                    null,
                    (p1, p2) =>
                    p1 == null ? p2.ToJson() : string.Format("{0}, {1}", p1, p2.ToJson())));
        }
    }
}
