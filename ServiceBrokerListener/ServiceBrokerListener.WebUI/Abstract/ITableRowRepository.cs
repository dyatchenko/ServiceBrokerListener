namespace ServiceBrokerListener.WebUI.Abstract
{
    using System.Collections.Generic;
    using ServiceBrokerListener.WebUI.Models;

    public interface ITableRowRepository
    {
        IEnumerable<TableRow> Rows { get; }

        void UpdateRow(int rowNumber, int colNumber, string value);
    }
}
