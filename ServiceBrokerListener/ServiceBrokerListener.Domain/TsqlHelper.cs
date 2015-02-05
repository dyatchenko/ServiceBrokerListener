namespace ServiceBrokerListener.Domain
{
    using System.Data;
    using System.Data.SqlClient;

    public static class TsqlHelper
    {
        public static int GetTriggersCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.objects WHERE [type] = 'TR'";

            return GetIntValueFromCommand(connection, CommandText);
        }

        public static int GetProceduresCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.objects WHERE [type] = 'P'";

            return GetIntValueFromCommand(connection, CommandText);
        }

        public static int GetConversationEndpointsCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.conversation_endpoints";

            return GetIntValueFromCommand(connection, CommandText);
        }

        public static int GetServiceQueuesCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.service_queues";

            return GetIntValueFromCommand(connection, CommandText);
        }

        public static int GetConversationGroupsCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.conversation_groups";

            return GetIntValueFromCommand(connection, CommandText);
        }

        public static int GetServicesCount(this SqlConnection connection)
        {
            const string CommandText = "SELECT COUNT(*) FROM sys.services";

            return GetIntValueFromCommand(connection, CommandText);
        }

        private static int GetIntValueFromCommand(SqlConnection connection, string commandText)
        {
            if (connection.State != ConnectionState.Open) return -1;

            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                command.CommandType = CommandType.Text;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (!reader.Read()) return -1;

                    return reader.GetInt32(0);
                }
            }
        }
    }
}
