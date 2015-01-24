namespace ServiceBrokerListener.Domain
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// TODO: 
    /// 1. Remove installation/deinstallation procedures from database after dispose.
    /// 2. Clear all conversation endpoints after dispose.
    /// 3. Add helpful procedures to clean database.
    /// 4. Use SqlConnection instead of ConnectionString.
    /// 5. Use Tasks.
    /// </summary>
    public sealed class ServiceBrokerListener : IDisposable
    {
        [Flags]
        public enum ListenerTypes
        {
            None     = 0,
            OnInsert = 1 << 1,
            OnUpdate = 1 << 2,
            OnDelete = 1 << 3
        }

        #region Scripts

        /// <summary>
        /// T-SQL script-template which creates notification setup procedure.
        /// {0} - database name.
        /// {1} - setup procedure name.
        /// {2} - service broker configuration statement.
        /// {3} - notification trigger configuration statement.
        /// </summary>
        private const string SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {1}
                        AS
                        BEGIN
                            -- Service Broker configuration statement.
                            {2}

                            -- Notification Trigger configuration statement.
                            DECLARE @triggerStatement NVARCHAR(MAX)
                            SET @triggerStatement = ''{3}''
                            
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
        /// </summary>
        private const string SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {1}
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
        /// T-SQL script-template which clears all unused conversation handlers in the database.
        /// {0} - database name.
        /// /// </summary>
        private const string SQL_FORMAT_RELEASE_ALL_UNUSED_CONVERSATION_HANDLERS = @"
                USE [{0}]
                DECLARE @ConvHandle uniqueidentifier
                DECLARE Conv CURSOR FOR
                SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
                WHERE CEP.state = 'DI' or CEP.state = 'CD'
                OPEN Conv;
                FETCH NEXT FROM Conv INTO @ConvHandle;
                WHILE (@@FETCH_STATUS = 0) BEGIN
	                END CONVERSATION @ConvHandle WITH CLEANUP;
                    FETCH NEXT FROM Conv INTO @ConvHandle;
                END
                CLOSE Conv;
                DEALLOCATE Conv;
            ";

        /// <summary>
        /// T-SQL script-template which prepares database for ServiceBroker notification.
        /// {0} - database name;
        /// {1} - conversation queue name.
        /// {2} - conversation service name.
        /// </summary>
        private const string SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION = @"
                -- Setup Service Broker
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{0}' 
                                AND is_broker_enabled = 0) 
                BEGIN
                     ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                     ALTER DATABASE [{0}] SET ENABLE_BROKER; 
                     ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE
                END
                
                -- Setup authorization
                ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
                ALTER DATABASE [{0}] SET TRUSTWORTHY ON;
                
                -- Create a queue which will hold the tracked information 
                IF NOT EXISTS (SELECT * FROM sys.service_queues 
		                WHERE name = '{1}')
	                CREATE QUEUE dbo.[{1}]
                -- Create a service on which tracked information will be sent 
                IF NOT EXISTS(SELECT * FROM sys.services 
                WHERE name = '{2}')
	                CREATE SERVICE [{2}] 
	                ON QUEUE dbo.[{1}] 
	                ([DEFAULT]) 
            ";

        /// <summary>
        /// T-SQL script-template which removes database notification.
        /// {0} - notification trigger name.
        /// {1} - conversation queue name.
        /// {2} - conversation service name.
        /// </summary>
        private const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION = @"
                IF OBJECT_ID ('{0}', 'TR') IS NOT NULL
                    DROP TRIGGER [{0}];
                BEGIN TRY
                    DROP SERVICE [{2}];
                    IF OBJECT_ID ('{1}', 'SQ') IS NOT NULL
	                    DROP QUEUE [{1}];
                END TRY
                BEGIN CATCH END CATCH
            ";

        /// <summary>
        /// T-SQL script-template which creates notification trigger.
        /// {0} - notification trigger name. 
        /// </summary>
        private const string SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER = @"
                IF OBJECT_ID ('{0}', 'TR') IS NOT NULL
                    DROP TRIGGER [{0}];
            ";

        /// <summary>
        /// T-SQL script-template which creates notification trigger.
        /// {0} - monitorable table name.
        /// {1} - notification trigger name.
        /// {2} - event data (INSERT, DELETE, UPDATE...).
        /// {3} - conversation service name. 
        /// </summary>
        private const string SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER = @"
                CREATE TRIGGER [{1}]
                ON dbo.[{0}]
                AFTER {2} 
                AS

                SET NOCOUNT ON;

                --Trigger {0} is rising...
                IF EXISTS (SELECT * FROM sys.services WHERE name = '{3}')
                BEGIN
                	--Beginning of dialog...
                	DECLARE @ConvHandle UNIQUEIDENTIFIER
                	--Determine the Initiator Service, Target Service and the Contract 
                	BEGIN DIALOG @ConvHandle 
                        FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF; 
	                --Send the Message
	                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT];
	                --End conversation
	                END CONVERSATION @ConvHandle WITH CLEANUP;
                END
            ";

        /// <summary>
        /// T-SQL script-template which helps to receive changed data in monitorable table.
        /// {0} - database name.
        /// {1} - conversation queue name.
        /// {2} - timeout.
        /// </summary>
        private const string SQL_FORMAT_RECEIVE_EVENT = @"
                DECLARE @ConvHandle UNIQUEIDENTIFIER
                USE [{0}]
                WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle FROM dbo.[{1}]), TIMEOUT {2};
                BEGIN TRY
	                END CONVERSATION @ConvHandle WITH CLEANUP;
                END TRY BEGIN CATCH END CATCH
            ";

        /// <summary>
        /// T-SQL script-template which executes stored procedure.
        /// {0} - database name.
        /// {1} - procedure name.
        /// </summary>
        private const string SQL_FORMAT_EXECUTE_PROCEDURE = @"
                USE [{0}]
                EXEC {1}
            ";

        #endregion

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
                return string.Format("ListenerTrigger-{0}", this.uniqueNameIdentifier);
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

        public ListenerTypes ListenerType { get; private set; }

        public int ListenerTimeout { get; private set; }

        public ServiceBrokerListener(
            string connectionString,
            string databaseName,
            string tableName,
            ListenerTypes listenerType,
            int listenerTimeout)
        {
            this.ConnectionString = connectionString;
            this.DatabaseName = databaseName;
            this.TableName = tableName;
            this.ListenerType = listenerType;
            this.ListenerTimeout = listenerTimeout;
        }

        public event EventHandler TableChanged;

        public void StartListen()
        {
            this.StopListen();
            this.InstallNotification();

            this.listenerThread = new Thread(this.Loop)
                                      {
                                          Name =
                                              string.Format("{0}_Thread", this.GetType().FullName),
                                          IsBackground = true
                                      };
            this.listenerThread.Start();
        }

        public void StopListen()
        {
            if ((this.listenerThread == null) || (!this.listenerThread.IsAlive))
                return;

            this.listenerThread.Abort();
            this.listenerThread.Join();
        }

        public string GetReleaseAllUnusedConversationHandlersScript()
        {
            return string.Format(
                SQL_FORMAT_RELEASE_ALL_UNUSED_CONVERSATION_HANDLERS,
                this.DatabaseName);
        }

        public void ReleaseAllUnusedConversationHandlers()
        {
            ExecuteNonQuery(GetReleaseAllUnusedConversationHandlersScript(), this.ConnectionString);
        }

        public void Dispose()
        {
            StopListen();
        }

        private void Loop()
        {
            try
            {
                while (true)
                {
                    ReceiveEvent();
                    OnTableChanged();
                }
            }
            finally
            {
                UninstallNotification();
            }
        }

        private string GetUninstallNotificationProcedureScript()
        {
            string uninstallServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION,
                this.ConversationTriggerName,
                this.ConversationQueueName,
                this.ConversationServiceName);
            string uninstallNotificationTriggerScript = string.Format(
                SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER,
                this.ConversationTriggerName);
            string uninstallationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.UninstallListenerProcedureName,
                    uninstallServiceBrokerNotificationScript.Replace("'", "''"),
                    uninstallNotificationTriggerScript.Replace("'", "''"));
            return uninstallationProcedureScript;
        }

        private string GetInstallNotificationProcedureScript()
        {
            string installServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
                this.DatabaseName,
                this.ConversationQueueName,
                this.ConversationServiceName);
            string installNotificationTriggerScript = string.Format(
                SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER,
                this.TableName,
                this.ConversationTriggerName,
                GetTriggerTypeByListenerType(),
                this.ConversationServiceName);
            string installationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.InstallListenerProcedureName,
                    installServiceBrokerNotificationScript.Replace("'", "''"),
                    installNotificationTriggerScript.Replace("'", "''''"));
            return installationProcedureScript;
        }

        private string GetTriggerTypeByListenerType()
        {
            StringBuilder result = new StringBuilder();
            if (this.ListenerType.HasFlag(ListenerTypes.OnInsert)) result.Append("INSERT");
            if (this.ListenerType.HasFlag(ListenerTypes.OnUpdate)) result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
            if (this.ListenerType.HasFlag(ListenerTypes.OnDelete)) result.Append(result.Length == 0 ? "DELETE" : ", DELETE");

            return result.ToString();
        }

        private void ReceiveEvent()
        {
            ExecuteNonQuery(
                string.Format(
                    SQL_FORMAT_RECEIVE_EVENT,
                    this.DatabaseName,
                    this.ConversationQueueName,
                    this.ListenerTimeout),
                this.ConnectionString);
        }

        private void UninstallNotification()
        {
            string execUninstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.UninstallListenerProcedureName);
            ExecuteNonQuery(execUninstallationProcedureScript, this.ConnectionString);
        }

        private void InstallNotification()
        {
            string execInstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.InstallListenerProcedureName);
            ExecuteNonQuery(GetInstallNotificationProcedureScript(), this.ConnectionString);
            ExecuteNonQuery(GetUninstallNotificationProcedureScript(), this.ConnectionString);
            ExecuteNonQuery(execInstallationProcedureScript, this.ConnectionString);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities")]
        private static void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                conn.Open();
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private void OnTableChanged()
        {
            var evnt = this.TableChanged;
            if (evnt == null) return;

            evnt.Invoke(this, EventArgs.Empty);
        }
    }
}