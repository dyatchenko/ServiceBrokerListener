namespace ServiceBrokerListener.WebUI.Models
{
    public class TableRow
    {
        public string A { get; set; }
        public string B { get; set; }
        public string C { get; set; }
        public string D { get; set; }
        public string E { get; set; }
        public string F { get; set; }

        public string ToJsArray()
        {
            return string.Format("['{0}','{1}','{2}','{3}','{4}','{5}']", A, B, C, D, E, F);
        }
    }
}