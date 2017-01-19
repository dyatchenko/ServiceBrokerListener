using CommandLine;

namespace ServiceBrokerListener.TableListener.Core
{
  public class Options
  {
    [Option('h', "help", HelpText = "Prints options")]
    public bool Help { get; set; }
    [Option('v', "verbose", HelpText = "Prints all messages to standard output.")]
    public bool Verbose { get; set; }
    [Option('c', "connectionstring", Required = true, HelpText = "connection string to server (e.g. Data Source=TheServer;Initial Catalog=TheDatabaseName;Integrated Security=True)")]
    public string ConnectionString { get; set; }
    [Option('d', "database", Required = true, HelpText = "database (e.g. TheDatabaseName)")]
    public string Database { get; set; }
    [Option('t', "table", Required = true, HelpText = "table (e.g. TheTableName)")]
    public string Table { get; set; }
  }
}