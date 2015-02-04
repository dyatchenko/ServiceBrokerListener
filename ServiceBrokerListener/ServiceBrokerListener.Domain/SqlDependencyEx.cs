namespace ServiceBrokerListener.Domain
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Text;
    using System.Threading;
    using System.Xml.Linq;

    public sealed class SqlDependencyEx : IDisposable
    {
        [Flags]
        public enum NotificationTypes
        {
            None     = 0,
            Insert   = 1 << 1,
            Update   = 1 << 2,
            Delete   = 1 << 3
        }

        public class TableChangedEventArgs : EventArgs
        {
            private readonly string notificationMessage;

            private const string INSERTED_TAG = "inserted";

            private const string DELETED_TAG = "deleted";

            public TableChangedEventArgs(string notificationMessage)
            {
                this.notificationMessage = notificationMessage;
            }

            public XElement Data
            {
                get
                {
                    if (string.IsNullOrWhiteSpace(notificationMessage)) return null;

                    return XElement.Parse(notificationMessage);
                }
            }

            public NotificationTypes NotificationType
            {
                get
                {
                    return Data.Element(INSERTED_TAG) != null
                               ? Data.Element(DELETED_TAG) != null
                                     ? NotificationTypes.Update
                                     : NotificationTypes.Insert
                               : Data.Element(DELETED_TAG) != null
                                     ? NotificationTypes.Delete
                                     : NotificationTypes.None;
                }
            }
        }

        #region Scripts

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
        /// {4} - notification trigger drop statement.
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

                            -- Notification Trigger drop statement.
                            {4}

                            -- Notification Trigger configuration statement.
                            DECLARE @triggerStatement NVARCHAR(MAX)
                            DECLARE @select NVARCHAR(MAX)
                            DECLARE @sqlInserted NVARCHAR(MAX)
                            DECLARE @sqlDeleted NVARCHAR(MAX)
                            
                            SET @triggerStatement = N''{3}''
                            
                            SET @select = STUFF((SELECT '','' + COLUMN_NAME
						                         FROM INFORMATION_SCHEMA.COLUMNS
						                         WHERE TABLE_NAME = ''{5}'' AND TABLE_CATALOG = ''{0}''
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

                            EXEC sp_executeSql @triggerStatement
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
                        END
                        ')
                END
            ";

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
        private const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION = @"
                BEGIN TRY
                    -- Release all unised conversation handlers.

                    DECLARE @serviceId INT
                    SELECT @serviceId = service_id FROM sys.services 
                    WHERE sys.services.name = '{1}'

                    DECLARE @ConvHandle uniqueidentifier
                    DECLARE Conv CURSOR FOR
                    SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
                    WHERE CEP.service_id = @serviceId

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
                END TRY
                BEGIN CATCH END CATCH
            ";

        /// <summary>
        /// T-SQL script-template which creates notification trigger.
        /// {0} - notification trigger name. 
        /// {1} - schema name.
        /// </summary>
        private const string SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    DROP TRIGGER {1}.[{0}];
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
                        FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF; 
	                --Send the Message
	                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
	                --End conversation
	                END CONVERSATION @ConvHandle WITH CLEANUP;
                END
            ";

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
                BEGIN TRY
	                END CONVERSATION @ConvHandle WITH CLEANUP;
                END TRY BEGIN CATCH END CATCH

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
        /// T-SQL script-template which deletes stored procedure.
        /// {0} - database name.
        /// {1} - stored procedure name.
        /// {2} - schema name.
        /// </summary>
        private const string SQL_FORMAT_DROP_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
                    DROP PROCEDURE temp.[{1}]
            ";

        #endregion

        private const int COMMAND_TIMEOUT = 60000;

        private readonly Guid uniqueNameIdentifier = Guid.NewGuid();

        private Thread listenerThread;

        public string ConversationQueueName
        {
            get
            {
                return string.Format("ListenerQueue-{0}", this.uniqueNameIdentifier);
            }
        }

        public string ConversationServiceName
        {
            get
            {
                return string.Format("ListenerService-{0}", this.uniqueNameIdentifier);
            }
        }

        public string ConversationTriggerName
        {
            get
            {
                return string.Format("tr_Listener_{0}", this.uniqueNameIdentifier.ToString("N"));
            }
        }

        public string InstallListenerProcedureName
        {
            get
            {
                return string.Format(
                    "sp_InstallListenerNotification_{0}",
                    this.uniqueNameIdentifier.ToString("N"));
            }
        }

        public string UninstallListenerProcedureName
        {
            get
            {
                return string.Format(
                    "sp_UninstallListenerNotification_{0}",
                    this.uniqueNameIdentifier.ToString("N"));
            }
        }

        public string ConnectionString { get; private set; }

        public string DatabaseName { get; private set; }

        public string TableName { get; private set; }

        public string SchemaName { get; private set; }

        public NotificationTypes NotificaionTypes { get; private set; }

        public bool DetailsIncluded { get; private set; }

        public SqlDependencyEx(
            string connectionString,
            string databaseName,
            string tableName,
            string schemaName = "dbo",
            NotificationTypes listenerType =
                NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,
            bool receiveDetails = true)
        {
            this.ConnectionString = connectionString;
            this.DatabaseName = databaseName;
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.NotificaionTypes = listenerType;
            this.DetailsIncluded = receiveDetails;
        }

        public event EventHandler<TableChangedEventArgs> TableChanged;

        public void Start()
        {
            this.Stop();
            this.InstallNotification();

            ThreadStart threadLoop = () =>
                {
                    try
                    {
                        while (true)
                        {
                            string message = ReceiveEvent();
                            if (!string.IsNullOrWhiteSpace(message))
                                OnTableChanged(message);
                        }
                    }
                    finally { UninstallNotification(); }
                };

            var thread = new Thread(threadLoop)
                             {
                                 Name =
                                     string.Format(
                                         "{0}_Thread_{1}",
                                         this.GetType().FullName,
                                         this.uniqueNameIdentifier.ToString("N")),
                                 IsBackground = true
                             };
            thread.Start();
            this.listenerThread = thread;
        }

        public void Stop()
        {
            UninstallNotification();

            var thread = this.listenerThread;
            if ((thread == null) || (!thread.IsAlive))
                return;
            
            thread.Abort();
            thread.Join();
        }

        public void Dispose()
        {
            Stop();
        }

        private string ReceiveEvent()
        {
            var commandText = string.Format(
                SQL_FORMAT_RECEIVE_EVENT,
                this.DatabaseName,
                this.ConversationQueueName,
                COMMAND_TIMEOUT / 2,
                this.SchemaName);

            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                conn.Open();
                command.CommandType = CommandType.Text;
                command.CommandTimeout = COMMAND_TIMEOUT;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read() || reader.IsDBNull(0)) return string.Empty;

                    return reader.GetString(0);
                }
            }
        }

        private string GetUninstallNotificationProcedureScript()
        {
            string uninstallServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);
            string uninstallNotificationTriggerScript = string.Format(
                SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER,
                this.ConversationTriggerName,
                this.SchemaName);
            string uninstallationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.UninstallListenerProcedureName,
                    uninstallServiceBrokerNotificationScript.Replace("'", "''"),
                    uninstallNotificationTriggerScript.Replace("'", "''"),
                    this.SchemaName);
            return uninstallationProcedureScript;
        }

        private string GetInstallNotificationProcedureScript()
        {
            string installServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
                this.DatabaseName,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);
            string installNotificationTriggerScript =
                string.Format(
                    SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER,
                    this.TableName,
                    this.ConversationTriggerName,
                    GetTriggerTypeByListenerType(),
                    this.ConversationServiceName,
                    this.DetailsIncluded ? string.Empty : @"NOT",
                    this.SchemaName);
            string uninstallNotificationTriggerScript =
                string.Format(
                    SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER,
                    this.ConversationTriggerName,
                    this.SchemaName);
            string installationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.InstallListenerProcedureName,
                    installServiceBrokerNotificationScript.Replace("'", "''"),
                    installNotificationTriggerScript.Replace("'", "''''"),
                    uninstallNotificationTriggerScript.Replace("'", "''"),
                    this.TableName,
                    this.SchemaName);
            return installationProcedureScript;
        }

        private string GetTriggerTypeByListenerType()
        {
            StringBuilder result = new StringBuilder();
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Insert)) 
                result.Append("INSERT");
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Update)) 
                result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Delete)) 
                result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
            if (result.Length == 0) result.Append("INSERT");

            return result.ToString();
        }

        private void UninstallNotification()
        {
            string execUninstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.UninstallListenerProcedureName,
                this.SchemaName);
            string dropUsedProcedures = string.Format(
                "{0}\r\n{1}",
                string.Format(
                    SQL_FORMAT_DROP_PROCEDURE,
                    this.DatabaseName,
                    this.InstallListenerProcedureName,
                    this.SchemaName),
                string.Format(
                    SQL_FORMAT_DROP_PROCEDURE,
                    this.DatabaseName,
                    this.UninstallListenerProcedureName,
                    this.SchemaName));
            ExecuteNonQuery(execUninstallationProcedureScript, this.ConnectionString);
            ExecuteNonQuery(dropUsedProcedures, this.ConnectionString);
        }

        private void InstallNotification()
        {
            string execInstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.InstallListenerProcedureName,
                this.SchemaName);
            ExecuteNonQuery(GetInstallNotificationProcedureScript(), this.ConnectionString);
            ExecuteNonQuery(GetUninstallNotificationProcedureScript(), this.ConnectionString);
            ExecuteNonQuery(execInstallationProcedureScript, this.ConnectionString);
        }

        private void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                conn.Open();
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private void OnTableChanged(string message)
        {
            var evnt = this.TableChanged;
            if (evnt == null) return;

            evnt.Invoke(this, new TableChangedEventArgs(message));
        }
    }
}