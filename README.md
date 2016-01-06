# ServiceBrokerListener
Component which receives SQL Server table changes to your .net code.

You can test it by yourself: http://sbl.azurewebsites.net

# How To Use

1. Copy the `SqlDependecyEx` class from the `ServiceBrokerListener.Domain` project to your solution.
2. Make sure that the Service Broker feature is enabled for your database.
    
    ```
    ALTER DATABASE test SET ENABLE_BROKER
    ```
    
3. Use the class as in example below:

    ```
    // See constructor optional parameters to configure it according to your needs
    var listener = new SqlDependencyEx(connectionString, "YourDatabase", "YourTable");
    
    // e.Data contains actual changed data in the XML format
    listener.TableChanged += (o, e) => Console.WriteLine("Your table was changed!");
    
    // After you call the Start method you will receive table notifications with 
    // the actual changed data in the XML format
    listener.Start();
    
    // Don't forget to stop the listener somewhere!
    listener.Stop();
    ```

4. Enjoy!

# How to use for multiple tables

All you need to do is to create multiple listeners with different identities as shown below:

    var listener1 = new SqlDependencyEx(connectionString, "YourDatabase", "YourTable1", identity: 1);
    var listener2 = new SqlDependencyEx(connectionString, "YourDatabase", "YourTable2", identity: 2);
    
# How to track UPDATEs only

    var listener = new SqlDependencyEx(connectionString, "YourDatabase",
                 "YourTable1", listenerType: SqlDependencyEx.NotificationTypes.Update);
    
# Licence

[GNU GENERAL PUBLIC LICENSE](LICENSE)
