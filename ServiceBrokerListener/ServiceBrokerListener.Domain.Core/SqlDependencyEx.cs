using System.IO;
using System.Xml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml.Linq;

namespace ServiceBrokerListener.Domain.Core
{

  public sealed class SqlDependencyEx : IDisposable
  {
    [Flags]
    public enum NotificationTypes
    {
      None = 0,
      Insert = 1 << 1,
      Update = 1 << 2,
      Delete = 1 << 3
    }

    public class TableChangedEventArgs : EventArgs
    {
      private readonly string _notificationMessage;

      private const string InsertedTag = "inserted";

      private const string DeletedTag = "deleted";

      public TableChangedEventArgs(string notificationMessage)
      {
        _notificationMessage = notificationMessage;
      }

      public XElement Data => string.IsNullOrWhiteSpace(_notificationMessage) ? null : ReadXDocumentWithInvalidCharacters(_notificationMessage);

      public NotificationTypes NotificationType => Data?.Element(InsertedTag) != null
        ? Data?.Element(DeletedTag) != null
          ? NotificationTypes.Update
          : NotificationTypes.Insert
        : Data?.Element(DeletedTag) != null
          ? NotificationTypes.Delete
          : NotificationTypes.None;

      /// <summary>
      /// Converts an xml string into XElement with no invalid characters check.
      /// https://paulselles.wordpress.com/2013/07/03/parsing-xml-with-invalid-characters-in-c-2/
      /// </summary>
      /// <param name="xml">The input string.</param>
      /// <returns>The result XElement.</returns>
      private static XElement ReadXDocumentWithInvalidCharacters(string xml)
      {
        XDocument xDocument;

        var xmlReaderSettings = new XmlReaderSettings {CheckCharacters = false};

        using (var stream = new StringReader(xml))
        using (var xmlReader = XmlReader.Create(stream, xmlReaderSettings))
        {
          // Load our XDocument
          xmlReader.MoveToContent();
          xDocument = XDocument.Load(xmlReader);
        }

        return xDocument.Root;
      }
    }

    #region Scripts

    #region Procedures

    private const string SqlPermissionsInfo = @"
                    DECLARE @msg VARCHAR(MAX)
                    DECLARE @crlf CHAR(1)
                    SET @crlf = CHAR(10)
                    SET @msg = 'Current user must have following permissions: '
                    SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
                    SET @msg = @msg + 'that are required to start query notifications. '
                    SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
                    SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'
                    
                    PRINT @msg
                ";

    /// <summary>
    /// T-SQL script-template which creates notification setup procedure.
    /// {0} - database name.
    /// {1} - setup procedure name.
    /// {2} - service broker configuration statement.
    /// {3} - notification trigger configuration statement.
    /// {4} - notification trigger check statement.
    /// {5} - table name.
    /// {6} - schema name.
    /// </summary>
    private const string SqlFormatCreateInstallationProcedure = @"
                USE [{0}]
                " + SqlPermissionsInfo + @"
                IF OBJECT_ID ('{6}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {6}.{1}
                        AS
                        BEGIN
                            -- Service Broker configuration statement.
                            {2}

                            -- Notification Trigger check statement.
                            {4}

                            -- Notification Trigger configuration statement.
                            DECLARE @triggerStatement NVARCHAR(MAX)
                            DECLARE @select NVARCHAR(MAX)
                            DECLARE @sqlInserted NVARCHAR(MAX)
                            DECLARE @sqlDeleted NVARCHAR(MAX)
                            
                            SET @triggerStatement = N''{3}''
                            
                            SET @select = STUFF((SELECT '','' + ''['' + COLUMN_NAME + '']''
						                         FROM INFORMATION_SCHEMA.COLUMNS
						                         WHERE DATA_TYPE NOT IN  (''text'',''ntext'',''image'',''geometry'',''geography'') AND TABLE_NAME = ''{5}'' AND TABLE_CATALOG = ''{0}''
						                         FOR XML PATH ('''')
						                         ), 1, 1, '''')
                            SET @sqlInserted = 
                                N''SET @retvalOUT = (SELECT '' + @select + N'' 
                                                     FROM INSERTED 
                                                     FOR XML PATH(''''row''''), ROOT (''''inserted''''))''
                            SET @sqlDeleted = 
                                N''SET @retvalOUT = (SELECT '' + @select + N'' 
                                                     FROM DELETED 
                                                     FOR XML PATH(''''row''''), ROOT (''''deleted''''))''                            
                            SET @triggerStatement = REPLACE(@triggerStatement
                                                     , ''%inserted_select_statement%'', @sqlInserted)
                            SET @triggerStatement = REPLACE(@triggerStatement
                                                     , ''%deleted_select_statement%'', @sqlDeleted)

                            EXEC sp_executesql @triggerStatement
                        END
                        ')
                END
            ";

    /// <summary>
    /// T-SQL script-template which creates notification uninstall procedure.
    /// {0} - database name.
    /// {1} - uninstall procedure name.
    /// {2} - notification trigger drop statement.
    /// {3} - service broker uninstall statement.
    /// {4} - schema name.
    /// {5} - install procedure name.
    /// </summary>
    private const string SqlFormatCreateUninstallationProcedure = @"
                USE [{0}]
                " + SqlPermissionsInfo + @"
                IF OBJECT_ID ('{4}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {4}.{1}
                        AS
                        BEGIN
                            -- Notification Trigger drop statement.
                            {3}

                            -- Service Broker uninstall statement.
                            {2}

                            IF OBJECT_ID (''{4}.{5}'', ''P'') IS NOT NULL
                                DROP PROCEDURE {4}.{5}
                            
                            DROP PROCEDURE {4}.{1}
                        END
                        ')
                END
            ";

    #endregion

    #region ServiceBroker notification

    /// <summary>
    /// T-SQL script-template which prepares database for ServiceBroker notification.
    /// {0} - database name;
    /// {1} - conversation queue name.
    /// {2} - conversation service name.
    /// {3} - schema name.
    /// </summary>
    private const string SqlFormatInstallSeviceBrokerNotification = @"
                -- Setup Service Broker
                IF EXISTS (SELECT * FROM sys.databases 
                                    WHERE name = '{0}' AND (is_broker_enabled = 0 OR is_trustworthy_on = 0)) 
                BEGIN
                     IF (NOT EXISTS(SELECT * FROM sys.fn_my_permissions(NULL, 'SERVER')
                                             WHERE permission_name = 'CONTROL SERVER'))
                     BEGIN
                        DECLARE @msg VARCHAR(MAX)
                        SET @msg = 'Current user doesn''t have CONTROL SERVER permission to enable service broker. '
                        SET @msg = @msg + 'Grant sufficient permissions to current user or '
                        SET @msg = @msg + 'execute ALTER DATABASE [<dbname>] SET ENABLE_BROKER with admin rights.'

                        RAISERROR (@msg, 16, 1)
                     END
                     ELSE 
                     BEGIN
                        ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                        ALTER DATABASE [{0}] SET ENABLE_BROKER; 
                        ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE

                        -- FOR SQL Express
                        ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
                        ALTER DATABASE [{0}] SET TRUSTWORTHY ON;               
                     END
                END

                -- Create a queue which will hold the tracked information 
                IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{1}')
	                CREATE QUEUE {3}.[{1}]
                -- Create a service on which tracked information will be sent 
                IF NOT EXISTS(SELECT * FROM sys.services WHERE name = '{2}')
	                CREATE SERVICE [{2}] ON QUEUE {3}.[{1}] ([DEFAULT]) 
            ";

    /// <summary>
    /// T-SQL script-template which removes database notification.
    /// {0} - conversation queue name.
    /// {1} - conversation service name.
    /// {2} - schema name.
    /// </summary>
    private const string SqlFormatUninstallServiceBrokerNotification = @"
                DECLARE @serviceId INT
                SELECT @serviceId = service_id FROM sys.services 
                WHERE sys.services.name = '{1}'

                DECLARE @ConvHandle uniqueidentifier
                DECLARE Conv CURSOR FOR
                SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
                WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)

                OPEN Conv;
                FETCH NEXT FROM Conv INTO @ConvHandle;
                WHILE (@@FETCH_STATUS = 0) BEGIN
    	            END CONVERSATION @ConvHandle WITH CLEANUP;
                    FETCH NEXT FROM Conv INTO @ConvHandle;
                END
                CLOSE Conv;
                DEALLOCATE Conv;

                -- Droping service and queue.
                DROP SERVICE [{1}];
                IF OBJECT_ID ('{2}.{0}', 'SQ') IS NOT NULL
	                DROP QUEUE {2}.[{0}];
            ";

    #endregion

    #region Notification Trigger

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - notification trigger name. 
    /// {1} - schema name.
    /// </summary>
    private const string SqlFormatDeleteNotificationTrigger = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    DROP TRIGGER {1}.[{0}];
            ";

    private const string SqlFormatCheckNotificationTrigger = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    RETURN;
            ";

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - monitorable table name.
    /// {1} - notification trigger name.
    /// {2} - event data (INSERT, DELETE, UPDATE...).
    /// {3} - conversation service name. 
    /// {4} - detailed changes tracking mode.
    /// {5} - schema name.
    /// %inserted_select_statement% - sql code which sets trigger "inserted" value to @retvalOUT variable.
    /// %deleted_select_statement% - sql code which sets trigger "deleted" value to @retvalOUT variable.
    /// </summary>
    private const string SqlFormatCreateNotificationTrigger = @"
                CREATE TRIGGER [{1}]
                ON {5}.[{0}]
                AFTER {2} 
                AS

                SET NOCOUNT ON;

                --Trigger {0} is rising...
                IF EXISTS (SELECT * FROM sys.services WHERE name = '{3}')
                BEGIN
                    DECLARE @message NVARCHAR(MAX)
                    SET @message = N'<root/>'

                    IF ({4} EXISTS(SELECT 1))
                    BEGIN
                        DECLARE @retvalOUT NVARCHAR(MAX)

                        %inserted_select_statement%

                        IF (@retvalOUT IS NOT NULL)
                        BEGIN SET @message = N'<root>' + @retvalOUT END                        

                        %deleted_select_statement%

                        IF (@retvalOUT IS NOT NULL)
                        BEGIN
                            IF (@message = N'<root/>') BEGIN SET @message = N'<root>' + @retvalOUT END
                            ELSE BEGIN SET @message = @message + @retvalOUT END
                        END 

                        IF (@message != N'<root/>') BEGIN SET @message = @message + N'</root>' END
                    END

                	--Beginning of dialog...
                	DECLARE @ConvHandle UNIQUEIDENTIFIER
                	--Determine the Initiator Service, Target Service and the Contract 
                	BEGIN DIALOG @ConvHandle 
                        FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 60; 
	                --Send the Message
	                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
	                --End conversation
	                END CONVERSATION @ConvHandle;
                END
            ";

    #endregion

    #region Miscellaneous

    /// <summary>
    /// T-SQL script-template which helps to receive changed data in monitorable table.
    /// {0} - database name.
    /// {1} - conversation queue name.
    /// {2} - timeout.
    /// {3} - schema name.
    /// </summary>
    private const string SqlFormatReceiveEvent = @"
                DECLARE @ConvHandle UNIQUEIDENTIFIER
                DECLARE @message VARBINARY(MAX)
                USE [{0}]
                WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle
                            , @message=message_body FROM {3}.[{1}]), TIMEOUT {2};
	            BEGIN TRY END CONVERSATION @ConvHandle; END TRY BEGIN CATCH END CATCH

                SELECT CAST(@message AS NVARCHAR(MAX)) 
            ";

    /// <summary>
    /// T-SQL script-template which executes stored procedure.
    /// {0} - database name.
    /// {1} - procedure name.
    /// {2} - schema name.
    /// </summary>
    private const string SqlFormatExecuteProcedure = @"
                USE [{0}]
                IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
                    EXEC {2}.{1}
            ";

    /// <summary>
    /// T-SQL script-template which returns all dependency identities in the database.
    /// {0} - database name.
    /// </summary>
    private const string SqlFormatGetDependencyIdentities = @"
                USE [{0}]
                
                SELECT REPLACE(name , 'ListenerService_' , '') 
                FROM sys.services 
                WHERE [name] like 'ListenerService_%';
            ";

    #endregion

    #region Forced cleaning of database

    /// <summary>
    /// T-SQL script-template which cleans database from notifications.
    /// {0} - database name.
    /// </summary>
    private const string SqlFormatForcedDatabaseCleaning = @"
                USE [{0}]

                DECLARE @db_name VARCHAR(MAX)
                SET @db_name = '{0}' -- provide your own db name                

                DECLARE @proc_name VARCHAR(MAX)
                DECLARE procedures CURSOR
                FOR
                SELECT   sys.schemas.name + '.' + sys.objects.name
                FROM    sys.objects 
                INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_UninstallListenerNotification_%'

                OPEN procedures;
                FETCH NEXT FROM procedures INTO @proc_name

                WHILE (@@FETCH_STATUS = 0)
                BEGIN
                EXEC ('USE [' + @db_name + '] EXEC ' + @proc_name + ' IF (OBJECT_ID (''' 
                                + @proc_name + ''', ''P'') IS NOT NULL) DROP PROCEDURE '
                                + @proc_name)

                FETCH NEXT FROM procedures INTO @proc_name
                END

                CLOSE procedures;
                DEALLOCATE procedures;

                DECLARE procedures CURSOR
                FOR
                SELECT   sys.schemas.name + '.' + sys.objects.name
                FROM    sys.objects 
                INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_InstallListenerNotification_%'

                OPEN procedures;
                FETCH NEXT FROM procedures INTO @proc_name

                WHILE (@@FETCH_STATUS = 0)
                BEGIN
                EXEC ('USE [' + @db_name + '] DROP PROCEDURE '
                                + @proc_name)

                FETCH NEXT FROM procedures INTO @proc_name
                END

                CLOSE procedures;
                DEALLOCATE procedures;
            ";

    #endregion

    #endregion

    private const int CommandTimeout = 60000;

    private static readonly List<int> ActiveEntities = new List<int>();

    private CancellationTokenSource _threadSource;

    public string ConversationQueueName => string.Format("ListenerQueue_{0}", Identity);

    public string ConversationServiceName => string.Format("ListenerService_{0}", Identity);

    public string ConversationTriggerName => string.Format("tr_Listener_{0}", Identity);

    public string InstallListenerProcedureName => string.Format("sp_InstallListenerNotification_{0}", Identity);

    public string UninstallListenerProcedureName => string.Format("sp_UninstallListenerNotification_{0}", Identity);

    public string ConnectionString { get; private set; }

    public string DatabaseName { get; private set; }

    public string TableName { get; private set; }

    public string SchemaName { get; private set; }

    public NotificationTypes NotificaionTypes { get; private set; }

    public bool DetailsIncluded { get; private set; }

    public int Identity { get; private set; }

    public bool Active { get; private set; }

    public event EventHandler<TableChangedEventArgs> TableChanged;

    public event EventHandler NotificationProcessStopped;

    public SqlDependencyEx(
      string connectionString,
      string databaseName,
      string tableName,
      string schemaName = "dbo",
      NotificationTypes listenerType =
        NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,
      bool receiveDetails = true, int identity = 1)
    {
      ConnectionString = connectionString;
      DatabaseName = databaseName;
      TableName = tableName;
      SchemaName = schemaName;
      NotificaionTypes = listenerType;
      DetailsIncluded = receiveDetails;
      Identity = identity;
    }

    public void Start()
    {
      lock (ActiveEntities)
      {
        if (ActiveEntities.Contains(Identity))
          throw new InvalidOperationException("An object with the same identity has already been started.");
        ActiveEntities.Add(Identity);
      }

      InstallNotification();
      _threadSource = new CancellationTokenSource();

      // Pass the token to the cancelable operation.
      ThreadPool.QueueUserWorkItem(NotificationLoop, _threadSource.Token);
      //var thread = new Thread(threadLoop)
      //{
      //  Name = string.Format("{0}_Thread_{1}", GetType().FullName, Identity),
      //  IsBackground = true
      //};
      //thread.Start();
      //_listenerThread = thread;
    }

    private void NotificationLoop(object input)
    {
      try
      {
        while (true)
        {
          var message = ReceiveEvent();
          Active = true;
          if (!string.IsNullOrWhiteSpace(message))
          {
            OnTableChanged(message);
          }
        }
      }
      catch
      {
        // ignored
      }
      finally
      {
        Active = false;
        OnNotificationProcessStopped();
      }
    }

    public void Stop()
    {
      UninstallNotification();

      lock (ActiveEntities)
      {
        if (ActiveEntities.Contains(Identity))
        {
          ActiveEntities.Remove(Identity);
        }
      }

      if ((_threadSource == null) || (_threadSource.Token.IsCancellationRequested))
      {
        return;
      }
      if (!_threadSource.Token.CanBeCanceled)
      {
        return;
      }
      _threadSource.Cancel();
      _threadSource.Dispose();
    }

    public void Dispose()
    {
      Stop();
    }

    public static int[] GetDependencyDbIdentities(string connectionString, string database)
    {
      if (connectionString == null)
      {
        throw new ArgumentNullException("connectionString");
      }

      if (database == null)
      {
        throw new ArgumentNullException("database");
      }

      var result = new List<string>();

      using (var connection = new SqlConnection(connectionString))
      using (var command = connection.CreateCommand())
      {
        connection.Open();
        command.CommandText = string.Format(SqlFormatGetDependencyIdentities, database);
        command.CommandType = CommandType.Text;
        using (var reader = command.ExecuteReader())
          while (reader.Read())
            result.Add(reader.GetString(0));
      }

      int temp;
      return
        result.Select(p => int.TryParse(p, out temp) ? temp : -1)
          .Where(p => p != -1)
          .ToArray();
    }

    public static void CleanDatabase(string connectionString, string database)
    {
      ExecuteNonQuery(
        string.Format(SqlFormatForcedDatabaseCleaning, database),
        connectionString);
    }

    private static void ExecuteNonQuery(string commandText, string connectionString)
    {
      using (var conn = new SqlConnection(connectionString))
      using (var command = new SqlCommand(commandText, conn))
      {
        conn.Open();
        command.CommandType = CommandType.Text;
        command.ExecuteNonQuery();
      }
    }

    private string ReceiveEvent()
    {
      var commandText = string.Format(
        SqlFormatReceiveEvent,
        DatabaseName,
        ConversationQueueName,
        CommandTimeout / 2,
        SchemaName);

      using (var conn = new SqlConnection(ConnectionString))
      using (var command = new SqlCommand(commandText, conn))
      {
        conn.Open();
        command.CommandType = CommandType.Text;
        command.CommandTimeout = CommandTimeout;
        using (var reader = command.ExecuteReader())
        {
          if (!reader.Read() || reader.IsDBNull(0)) return string.Empty;

          return reader.GetString(0);
        }
      }
    }

    private string GetUninstallNotificationProcedureScript()
    {
      var uninstallServiceBrokerNotificationScript = string.Format(
        SqlFormatUninstallServiceBrokerNotification,
        ConversationQueueName,
        ConversationServiceName,
        SchemaName);
      var uninstallNotificationTriggerScript = string.Format(
        SqlFormatDeleteNotificationTrigger,
        ConversationTriggerName,
        SchemaName);
      var uninstallationProcedureScript =
        string.Format(
          SqlFormatCreateUninstallationProcedure,
          DatabaseName,
          UninstallListenerProcedureName,
          uninstallServiceBrokerNotificationScript.Replace("'", "''"),
          uninstallNotificationTriggerScript.Replace("'", "''"),
          SchemaName,
          InstallListenerProcedureName);
      return uninstallationProcedureScript;
    }

    private string GetInstallNotificationProcedureScript()
    {
      var installServiceBrokerNotificationScript = string.Format(
        SqlFormatInstallSeviceBrokerNotification,
        DatabaseName,
        ConversationQueueName,
        ConversationServiceName,
        SchemaName);
      var installNotificationTriggerScript =
        string.Format(
          SqlFormatCreateNotificationTrigger,
          TableName,
          ConversationTriggerName,
          GetTriggerTypeByListenerType(),
          ConversationServiceName,
          DetailsIncluded ? string.Empty : @"NOT",
          SchemaName);
      var uninstallNotificationTriggerScript =
        string.Format(
          SqlFormatCheckNotificationTrigger,
          ConversationTriggerName,
          SchemaName);
      var installationProcedureScript =
        string.Format(
          SqlFormatCreateInstallationProcedure,
          DatabaseName,
          InstallListenerProcedureName,
          installServiceBrokerNotificationScript.Replace("'", "''"),
          installNotificationTriggerScript.Replace("'", "''''"),
          uninstallNotificationTriggerScript.Replace("'", "''"),
          TableName,
          SchemaName);
      return installationProcedureScript;
    }

    private string GetTriggerTypeByListenerType()
    {
      var result = new StringBuilder();
      if (NotificaionTypes.HasFlag(NotificationTypes.Insert))
        result.Append("INSERT");
      if (NotificaionTypes.HasFlag(NotificationTypes.Update))
        result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
      if (NotificaionTypes.HasFlag(NotificationTypes.Delete))
        result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
      if (result.Length == 0) result.Append("INSERT");

      return result.ToString();
    }

    private void UninstallNotification()
    {
      var execUninstallationProcedureScript = string.Format(
        SqlFormatExecuteProcedure,
        DatabaseName,
        UninstallListenerProcedureName,
        SchemaName);
      ExecuteNonQuery(execUninstallationProcedureScript, ConnectionString);
    }

    private void InstallNotification()
    {
      var execInstallationProcedureScript = string.Format(
        SqlFormatExecuteProcedure,
        DatabaseName,
        InstallListenerProcedureName,
        SchemaName);
      ExecuteNonQuery(GetInstallNotificationProcedureScript(), ConnectionString);
      ExecuteNonQuery(GetUninstallNotificationProcedureScript(), ConnectionString);
      ExecuteNonQuery(execInstallationProcedureScript, ConnectionString);
    }

    private void OnTableChanged(string message)
    {
      var evnt = TableChanged;
      if (evnt == null) return;

      evnt.Invoke(this, new TableChangedEventArgs(message));
    }

    private void OnNotificationProcessStopped()
    {
      var evnt = NotificationProcessStopped;
      if (evnt == null) return;

      evnt.BeginInvoke(this, EventArgs.Empty, null, null);
    }
  }
}
