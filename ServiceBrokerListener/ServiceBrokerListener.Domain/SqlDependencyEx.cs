using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;

namespace ServiceBrokerListener.Domain
{
	public sealed class SqlDependencyEx : IDisposable
	{
		#region Scripts

		#region Procedures

		private const string SQL_PERMISSIONS_INFO = @"
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
		private const string SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
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
						                         WHERE DATA_TYPE NOT IN  (''text'',''ntext'',''image'',''geometry'',''geography'') AND TABLE_SCHEMA = ''{6}'' AND TABLE_NAME = ''{5}'' AND TABLE_CATALOG = ''{0}''
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
		private const string SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
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
		private const string SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION = @"
                -- Setup Service Broker
                IF EXISTS (SELECT * FROM sys.databases 
                                    WHERE name = '{0}' AND (is_broker_enabled = 0 OR is_trustworthy_on = 0)) 
                BEGIN

                    ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    ALTER DATABASE [{0}] SET ENABLE_BROKER; 
                    ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE

                    -- FOR SQL Express
                    ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
                    ALTER DATABASE [{0}] SET TRUSTWORTHY ON;

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
		private const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION = @"
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
		private const string SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    DROP TRIGGER {1}.[{0}];
            ";

		private const string SQL_FORMAT_CHECK_NOTIFICATION_TRIGGER = @"
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
		private const string SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER = @"
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
		private const string SQL_FORMAT_RECEIVE_EVENT = @"
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
		private const string SQL_FORMAT_EXECUTE_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
                    EXEC {2}.{1}
            ";

		/// <summary>
		/// T-SQL script-template which returns all dependency identities in the database.
		/// {0} - database name.
		/// </summary>
		private const string SQL_FORMAT_GET_DEPENDENCY_IDENTITIES = @"
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
		private const string SQL_FORMAT_FORCED_DATABASE_CLEANING = @"
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

		public string ConversationQueueName => $"ListenerQueue_{Identity}";

		public string ConversationServiceName => $"ListenerService_{Identity}";

		public string ConversationTriggerName => $"tr_Listener_{Identity}";

		public string InstallListenerProcedureName => $"sp_InstallListenerNotification_{Identity}";

		public string UninstallListenerProcedureName => $"sp_UninstallListenerNotification_{Identity}";

		public NotificationTypes Notifications { get; private set; }

		public bool DetailsIncluded { get; private set; }

		public int Identity { get; private set; }

		public bool Active { get; private set; }

		public event EventHandler<TableChangedEventArgs> TableChanged;

		public event EventHandler NotificationProcessStopped;

		private readonly SqlDependencyExOptions ConnectionOptions;

		public SqlDependencyEx(SqlDependencyExOptions opts,
			NotificationTypes listenerType = NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,
			bool receiveDetails = true,
			int identity = 1)
		{
			ConnectionOptions = opts;
			ConnectionOptions.SchemaName = opts.SchemaName ?? "dbo";
			Notifications = listenerType;
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

			// ASP.NET fix 
			// IIS is not usually restarted when a new website version is deployed
			// This situation leads to notification absence in some cases
			Stop();

			InstallNotification();

			_threadSource = new CancellationTokenSource();

			// Use thead pool to start a background thread
			ThreadPool.QueueUserWorkItem(NotificationListener, _threadSource.Token);
		}

		public void Stop()
		{
			UninstallNotification();
			lock (ActiveEntities)
				if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

			if (_threadSource == null || _threadSource.Token.IsCancellationRequested)
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
				throw new ArgumentNullException(nameof(connectionString));
			}

			if (database == null)
			{
				throw new ArgumentNullException(nameof(database));
			}

			var result = new List<string>();

			using (var connection = new SqlConnection(connectionString))
			using (var command = connection.CreateCommand())
			{
				connection.Open();
				command.CommandText = string.Format(SQL_FORMAT_GET_DEPENDENCY_IDENTITIES, database);
				command.CommandType = CommandType.Text;
				using (var reader = command.ExecuteReader())
					while (reader.Read())
						result.Add(reader.GetString(0));
			}

			return
				result.Select(p => int.TryParse(p, out int temp) ? temp : -1)
					.Where(p => p != -1)
					.ToArray();
		}

		public static void CleanDatabase(string connectionString, string database)
		{
			ExecuteNonQuery(
				string.Format(SQL_FORMAT_FORCED_DATABASE_CLEANING, database),
				connectionString);
		}

		private void NotificationListener(object cancelToken)
		{
			var threadParams = StartNotificationLoop((CancellationToken) cancelToken);
			var newThread = new Thread(threadParams)
			{
				IsBackground = true,
				Name = $"{this.GetType().Name}_{Guid.NewGuid()}"
			};
			newThread.Start(cancelToken);
		}

		private ParameterizedThreadStart StartNotificationLoop(CancellationToken token)
		{
			var threadParams = new ParameterizedThreadStart(o =>
			{
				NotificationControl(token);
			});
			return threadParams;
		}

		private void NotificationControl(CancellationToken token)
		{
			try
			{
				NotificationLoop(token);
			}
			catch (Exception e)
			{
				// ignored because why the fuck not
			}
			finally
			{
				Active = false;
				OnNotificationProcessStopped();
			}
		}

		private void NotificationLoop(CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				var message = ReceiveEvent(); // This blocks until event received
				Active = true;
				if (!string.IsNullOrWhiteSpace(message))
				{
					OnTableChanged(message);
				}
			}
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
			using (var conn = new SqlConnection(ConnectionOptions.ConnectionString))
			using (var command = new SqlCommand(SqlCommand(ConnectionOptions), conn))
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

		private string SqlCommand(SqlDependencyExOptions options)
		{
			return string.Format(
				SQL_FORMAT_RECEIVE_EVENT,
				options.DatabaseName,
				ConversationQueueName,
				CommandTimeout / 2,
				options.SchemaName);
		}

		private string GetUninstallNotificationProcedureScript()
		{
			var uninstallServiceBrokerNotificationScript = string.Format(
				SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION,
				ConversationQueueName,
				ConversationServiceName,
				ConnectionOptions.SchemaName);
			var uninstallNotificationTriggerScript = string.Format(
				SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER,
				ConversationTriggerName,
				ConnectionOptions.SchemaName);
			var uninstallationProcedureScript =
				string.Format(
					SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE,
					ConnectionOptions.DatabaseName,
					UninstallListenerProcedureName,
					uninstallServiceBrokerNotificationScript.Replace("'", "''"),
					uninstallNotificationTriggerScript.Replace("'", "''"),
					ConnectionOptions.SchemaName,
					InstallListenerProcedureName);
			return uninstallationProcedureScript;
		}

		private string GetInstallNotificationProcedureScript()
		{
			var installServiceBrokerNotificationScript = string.Format(
				SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
				ConnectionOptions.DatabaseName,
				ConversationQueueName,
				ConversationServiceName,
				ConnectionOptions.SchemaName);
			var installNotificationTriggerScript =
				string.Format(
					SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER,
					ConnectionOptions.TableName,
					ConversationTriggerName,
					GetTriggerTypeByListenerType(),
					ConversationServiceName,
					DetailsIncluded ? string.Empty : @"NOT",
					ConnectionOptions.SchemaName);
			var uninstallNotificationTriggerScript =
				string.Format(
					SQL_FORMAT_CHECK_NOTIFICATION_TRIGGER,
					ConversationTriggerName,
					ConnectionOptions.SchemaName);
			var installationProcedureScript =
				string.Format(
					SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE,
					ConnectionOptions.DatabaseName,
					InstallListenerProcedureName,
					installServiceBrokerNotificationScript.Replace("'", "''"),
					installNotificationTriggerScript.Replace("'", "''''"),
					uninstallNotificationTriggerScript.Replace("'", "''"),
					ConnectionOptions.TableName,
					ConnectionOptions.SchemaName);
			return installationProcedureScript;
		}

		private string GetTriggerTypeByListenerType()
		{
			var result = new StringBuilder();
			if (Notifications.HasFlag(NotificationTypes.Insert))
				result.Append("INSERT");
			if (Notifications.HasFlag(NotificationTypes.Update))
				result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
			if (Notifications.HasFlag(NotificationTypes.Delete))
				result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
			if (result.Length == 0) result.Append("INSERT");

			return result.ToString();
		}

		private void UninstallNotification()
		{
			var execUninstallationProcedureScript = string.Format(
				SQL_FORMAT_EXECUTE_PROCEDURE,
				ConnectionOptions.DatabaseName,
				UninstallListenerProcedureName,
				ConnectionOptions.SchemaName);
			ExecuteNonQuery(execUninstallationProcedureScript, ConnectionOptions.ConnectionString);
		}

		private void InstallNotification()
		{
			var execInstallationProcedureScript = string.Format(
				SQL_FORMAT_EXECUTE_PROCEDURE,
				ConnectionOptions.DatabaseName,
				InstallListenerProcedureName,
				ConnectionOptions.SchemaName);
			ExecuteNonQuery(GetInstallNotificationProcedureScript(), ConnectionOptions.ConnectionString);
			ExecuteNonQuery(GetUninstallNotificationProcedureScript(), ConnectionOptions.ConnectionString);
			ExecuteNonQuery(execInstallationProcedureScript, ConnectionOptions.ConnectionString);
		}

		private void OnTableChanged(string message)
		{
			var evnt = TableChanged;
			evnt?.Invoke(this, new TableChangedEventArgs(message));
		}

		private void OnNotificationProcessStopped()
		{
			var evnt = NotificationProcessStopped;
			evnt?.BeginInvoke(this, EventArgs.Empty, null, null);
		}
	}
}