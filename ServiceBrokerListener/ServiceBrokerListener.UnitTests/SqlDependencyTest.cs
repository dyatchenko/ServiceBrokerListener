namespace ServiceBrokerListener.UnitTests
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using ServiceBrokerListener.Domain;

    using Assert = NUnit.Framework.Assert;

    [TestClass]
    public class SqlDependencyTest
    {
        private const string TEST_DATABASE_NAME = "TestDatabase";

        private const string TEST_TABLE_NAME = "TestTable";

        private const string TEST_TABLE_NAME_2 = "TestTable2";

        private const string MASTER_CONNECTION_STRING =
            "Data Source=(local);Initial Catalog=master;Integrated Security=True";

        private const string TEST_CONNECTION_STRING =
            "Data Source=(local);Initial Catalog=TestDatabase;Integrated Security=True";

        private const string INSERT_FORMAT =
            "USE [" + TEST_DATABASE_NAME + "] INSERT INTO [{1}] VALUES({0})";

        [TestInitialize]
        public void TestSetup()
        {
            const string CreateDatabaseScript = @"
                CREATE DATABASE " + TEST_DATABASE_NAME;
            const string CreateTableScript = @"                
                USE [" + TEST_DATABASE_NAME + @"]
                CREATE TABLE " + TEST_TABLE_NAME + @" (TestField int)   
                CREATE TABLE " + TEST_TABLE_NAME_2 + @" (TestField int)   
                ALTER DATABASE [" + TEST_DATABASE_NAME + @"] SET ENABLE_BROKER;   
                ALTER AUTHORIZATION ON DATABASE::[" + TEST_DATABASE_NAME + @"] TO [sa]
                ALTER DATABASE [" + TEST_DATABASE_NAME + @"] SET TRUSTWORTHY ON;    
                ";
            TestCleanup();
            ExecuteNonQuery(CreateDatabaseScript, MASTER_CONNECTION_STRING);
            ExecuteNonQuery(CreateTableScript, MASTER_CONNECTION_STRING);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            const string DropTestDatabaseScript = @"
                IF EXISTS(select * from sys.databases where name='" + TEST_DATABASE_NAME + @"')
                BEGIN
                    ALTER DATABASE [" + TEST_DATABASE_NAME
                                                  + @"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [" + TEST_DATABASE_NAME + @"]
                END
                ";
            ExecuteNonQuery(DropTestDatabaseScript, MASTER_CONNECTION_STRING);
        }

        [TestMethod]
        public void NotificationTestWith1Change()
        {
            NotificationTest(1);
        }

        [TestMethod]
        public void NotificationTestWith5Changes()
        {
            NotificationTest(5);
        }

        [TestMethod]
        public void NotificationTestWith10Changes()
        {
            NotificationTest(10);
        }

        [TestMethod]
        public void ResourcesReleasabilityTestWith1Changes_PROOF_OF_FAIL()
        {
            ResourcesReleasabilityTest(1);
        }

        [TestMethod]
        public void ResourcesReleasabilityTestWith5Changes_PROOF_OF_FAIL()
        {
            ResourcesReleasabilityTest(5);
        }

        [TestMethod]
        public void ResourcesReleasabilityTestWith10Changes_PROOF_OF_FAIL()
        {
            ResourcesReleasabilityTest(1);
        }

        [TestMethod]
        public void TwoTablesNotificationTest()
        {
            const int ChangesCountFirstTable = 5;
            const int ChangesCountSecondTable = 7;

            int changesReceived1 = 0;
            int changesReceived2 = 0;

            OnChangeEventHandler onChange1 = null;
            onChange1 = (s, e) =>
                {
                    if (e != null && e.Info == SqlNotificationInfo.Insert) changesReceived1++;

                    using (SqlConnection connection = new SqlConnection(TEST_CONNECTION_STRING))
                    using (var command1 = new SqlCommand("SELECT TestField FROM dbo.TestTable", connection))
                    {
                        connection.Open();

                        SqlDependency dep = (SqlDependency)s;
                        if (dep != null) dep.OnChange -= onChange1;
                        command1.Notification = null;
                        dep = new SqlDependency(command1);
                        dep.OnChange += onChange1;
                        command1.ExecuteReader().Close();
                    }
                };

            OnChangeEventHandler onChange2 = null;
            onChange2 = (s, e) =>
            {
                if (e != null && e.Info == SqlNotificationInfo.Insert) changesReceived2++;

                using (SqlConnection connection = new SqlConnection(TEST_CONNECTION_STRING))
                using (SqlCommand command2 = new SqlCommand("SELECT TestField FROM dbo.TestTable2", connection))
                {
                    connection.Open();

                    SqlDependency dep = (SqlDependency)s;
                    if (dep != null) dep.OnChange -= onChange2;
                    command2.Notification = null;
                    dep = new SqlDependency(command2);
                    dep.OnChange += onChange2;
                    command2.ExecuteReader().Close();
                }
            };

            SqlDependency.Start(TEST_CONNECTION_STRING);

            onChange1(null, null);

            SqlDependency.Start(TEST_CONNECTION_STRING);

            onChange2(null, null);

            MakeTableInsertChange(ChangesCountFirstTable);
            MakeTableInsertChange(ChangesCountSecondTable, TEST_TABLE_NAME_2);

            Assert.AreEqual(ChangesCountFirstTable, changesReceived1);
            Assert.AreEqual(ChangesCountSecondTable, changesReceived2);

            SqlDependency.Stop(TEST_CONNECTION_STRING);
        }

        public void ResourcesReleasabilityTest(int changesCount)
        {
            using (var sqlConnection = new SqlConnection(TEST_CONNECTION_STRING))
            {
                sqlConnection.Open();

                int sqlConversationEndpointsCount = sqlConnection.GetUnclosedConversationEndpointsCount();
                int sqlConversationGroupsCount = sqlConnection.GetConversationGroupsCount();
                int sqlServiceQueuesCount = sqlConnection.GetServiceQueuesCount();
                int sqlServicesCount = sqlConnection.GetServicesCount();

                NotificationTest(changesCount, false);

                // Microsoft SqlDependency REMOVES queue and service after use.
                Assert.AreEqual(sqlServicesCount, sqlConnection.GetServicesCount());
                Assert.AreEqual(
                    sqlServiceQueuesCount,
                    sqlConnection.GetServiceQueuesCount());

                // Microsoft SqlDependency KEEPS conversation group and endpoint in DB after use.
                // This behavior leads to GIANT memory leaks in SQL Server.
                Assert.AreNotEqual(
                    sqlConversationGroupsCount,
                    sqlConnection.GetConversationGroupsCount());
                Assert.AreNotEqual(
                    sqlConversationEndpointsCount,
                    sqlConnection.GetUnclosedConversationEndpointsCount());
            }
        }

        public void NotificationTest(int changesCount, bool useAssert = true)
        {
            SqlDependency.Stop(TEST_CONNECTION_STRING);
            SqlDependency.Start(TEST_CONNECTION_STRING);

            using (SqlConnection connection = new SqlConnection(TEST_CONNECTION_STRING))
            using (SqlCommand command = new SqlCommand("SELECT TestField FROM dbo.TestTable", connection))
            {
                connection.Open();
                int changesReceived = 0;

                OnChangeEventHandler onChange = null;
                onChange = (s, e) =>
                    {
                        if (e.Info == SqlNotificationInfo.Insert) changesReceived++;

                        // SqlDependency magic to receive events consequentially.
                        SqlDependency dep = (SqlDependency)s;
                        dep.OnChange -= onChange;
                        command.Notification = null;
                        dep = new SqlDependency(command);
                        dep.OnChange += onChange;
                        command.ExecuteReader().Close();
                    };

                // Create a dependency and associate it with the SqlCommand.
                SqlDependency dependency = new SqlDependency(command);
                // Subscribe to the SqlDependency event.
                dependency.OnChange += onChange;
                // Execute the command.
                command.ExecuteReader().Close();

                MakeTableInsertChange(changesCount);

                if (useAssert) Assert.AreEqual(changesCount, changesReceived);
            }

            SqlDependency.Stop(TEST_CONNECTION_STRING);
        }

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

        private static void MakeTableInsertChange(int changesCount, string tableName = TEST_TABLE_NAME)
        {
            for (int i = 0; i < changesCount; i++)
            {
                ExecuteNonQuery(string.Format(INSERT_FORMAT, i, tableName), TEST_CONNECTION_STRING);
                // It is one of weaknesses of Microsoft SqlDependency:
                // you must subscribe on OnChange again after every event firing.
                // Thus you may loose many table changes.
                // In this case we should wait a little bit to give enough time for resubscription.
                Thread.Sleep(500);
            }
        }
    }
}
