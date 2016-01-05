# ServiceBrokerListener
Component which helps to receive SQL Server table changes in your .net code.

You can test it by yourself: http://sbl.azurewebsites.net

# How To Use

1. Copy the `SqlDependecyEx` class from the ServiceBrokerListener.Domain project to your solution.
2. Make sure that the Service Broker feature is enabled for your database.
    
    ALTER DATABASE test SET ENABLE_BROKER

3. Enjoy!

# Licence

[MIT](LICENSE)
