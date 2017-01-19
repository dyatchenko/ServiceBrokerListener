using CommandLine;
using ServiceBrokerListener.Domain;
using System;
using System.Diagnostics;
using System.Threading;

namespace ServiceBrokerListener.TableListener.Core
{
  public class Program
  {
    public static void Main(string[] args)
    {
      var options = new Options();
      var result = Parser.Default.ParseArguments<Options>(args)
          .WithParsed(opts => options = opts);
      if (result.Tag == ParserResultType.Parsed)
      {
        Run(options);
      }
      DebugMode();
    }

    private static void DisplayPrompt(Options args)
    {
      Console.Write("Connected to ");
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine("{0}", args.ConnectionString);

      Console.ResetColor();
      Console.Write("Database ");
      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine("{0}", args.Database);

      Console.ResetColor();
      Console.Write("Watching changes on table ");
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("{0}", args.Table);
      Console.ResetColor();
      Console.WriteLine("Waiting for 'x' to end or data to be received");
    }

    private static void Run(Options args)
    {
      var listener = new SqlDependencyEx(args.ConnectionString, args.Database, args.Table);
      listener.TableChanged += ListenerOnTableChanged;
      listener.Start();
      DisplayPrompt(args);
      do
      {
        Thread.Sleep(100);
      } while (Console.ReadKey().KeyChar != 'x');
      listener.Stop();
    }

    private static void ListenerOnTableChanged(object sender, SqlDependencyEx.TableChangedEventArgs tableChangedEventArgs)
    {
      Console.WriteLine("EventType: {0}", tableChangedEventArgs.NotificationType);
      Console.WriteLine("Data: {0}", tableChangedEventArgs.Data);
    }

    private static void DebugMode()
    {
      if (!Debugger.IsAttached)
      {
        return;
      }
      Console.WriteLine($"{Environment.NewLine}Waiting because debugger is attached");
      Console.WriteLine("Press enter key to end");
      Console.ReadLine();
    }
  }
}
