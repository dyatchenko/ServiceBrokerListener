namespace ServiceBrokerListener.WebUI.Helpers
{
    using ServiceBrokerListener.WebUI.Models;
    using System.Collections.Generic;
    using System.Linq;

    public static class TableRowsCollectionExtension
    {
        public static string ToJsArray(this IEnumerable<TableRow> rows)
        {
            return string.Format(
                "{0}]",
                rows.Aggregate(
                    string.Empty,
                    (s, tr) =>
                    string.IsNullOrWhiteSpace(s)
                        ? string.Format("[{0}", tr.ToJsArray())
                        : string.Format("{0}, {1}", s, tr.ToJsArray())));
        }
    }
}