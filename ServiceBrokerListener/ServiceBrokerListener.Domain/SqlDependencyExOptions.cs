namespace ServiceBrokerListener.Domain
{
	public class SqlDependencyExOptions
	{
		public string ConnectionString { get; set; }
		public string DatabaseName { get; set; }
		public string TableName { get; set; }
		public string SchemaName { get; set; }
	}
}