namespace ServiceBrokerListener.UnitTests
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;

    using NUnit.Framework;

    using ServiceBrokerListener.Domain;

    /// <summary>
    /// TODO: 
    /// 1. Stress test.
    /// 2. Performance test.
    /// </summary>
    [TestFixture]
    public class SqlDependencyExTest
    {
        private const string TEST_DATABASE_NAME = "TestDatabase";

        private const string TEST_TABLE_NAME = "TestTable";

        private const string MASTER_CONNECTION_STRING =
            "Data Source=DEVSERVER;Initial Catalog=master;Integrated Security=True";

        private const string TEST_CONNECTION_STRING =
            "Data Source=DEVSERVER;Initial Catalog=TestDatabase;Integrated Security=True";

        private const string INSERT_FORMAT =
            "USE [" + TEST_DATABASE_NAME + "] INSERT INTO [" + TEST_TABLE_NAME + "] VALUES({0})";

        private const string REMOVE_FORMAT =
            "USE [" + TEST_DATABASE_NAME + "] DELETE FROM [" + TEST_TABLE_NAME
            + "] WHERE TestField = {0}";

        [SetUp]
        public void TestSetup()
        {
            const string CreateDatabaseScript = @"
                CREATE DATABASE " + TEST_DATABASE_NAME;
            const string CreateTableScript = @"                
                USE [" + TEST_DATABASE_NAME + @"]
                CREATE TABLE " + TEST_TABLE_NAME + @" (TestField int)               
                ";
            TestCleanup();
            ExecuteNonQuery(CreateDatabaseScript, MASTER_CONNECTION_STRING);
            ExecuteNonQuery(CreateTableScript, MASTER_CONNECTION_STRING);
        }

        [TearDown]
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

        [Test]
        public void NotificationTestWith10Changes()
        {
            NotificationTest(10);
        }

        [Test]
        public void NotificationTestWith100Changes()
        {
            NotificationTest(100);
        }

        [Test]
        public void NotificationTestWith1000Changes()
        {
            NotificationTest(100);
        }

        [Test]
        public void ResourcesReleasabilityTestWith1000Changes()
        {
            ResourcesReleasabilityTest(100);
        }

        [Test]
        public void ResourcesReleasabilityTestWith100Changes()
        {
            ResourcesReleasabilityTest(100);
        }

        [Test]
        public void ResourcesReleasabilityTestWith10Changes()
        {
            ResourcesReleasabilityTest(10);
        }

        public void ResourcesReleasabilityTest(int changesCount)
        {
            using (var sqlConnection = new SqlConnection(TEST_CONNECTION_STRING))
            {
                sqlConnection.Open();

                int sqlConversationEndpointsCount = sqlConnection.GetConversationEndpointsCount();
                int sqlConversationGroupsCount = sqlConnection.GetConversationGroupsCount();
                int sqlServiceQueuesCount = sqlConnection.GetServiceQueuesCount();
                int sqlServicesCount = sqlConnection.GetServicesCount();

                using (var sqlDependency = sqlConnection.GetSqlDependencyEx(TEST_TABLE_NAME))
                {
                    sqlDependency.Start();

                    // Make sure we've created one queue and sevice.
                    Assert.AreEqual(sqlServicesCount + 1, sqlConnection.GetServicesCount());
                    Assert.AreEqual(
                        sqlServiceQueuesCount + 1,
                        sqlConnection.GetServiceQueuesCount());

                    MakeTableInsertDeleteChanges(changesCount);

                    // Wait a little bit to process all changes.
                    Thread.Sleep(1000);
                }

                // Make sure we've released all resources.
                Assert.AreEqual(sqlServicesCount, sqlConnection.GetServicesCount());
                Assert.AreEqual(
                    sqlConversationGroupsCount,
                    sqlConnection.GetConversationGroupsCount());
                Assert.AreEqual(
                    sqlServiceQueuesCount,
                    sqlConnection.GetServiceQueuesCount());
                Assert.AreEqual(
                    sqlConversationEndpointsCount,
                    sqlConnection.GetConversationEndpointsCount());
            }
        }

        private void NotificationTest(int changesCount)
        {
            using (var sqlConnection = new SqlConnection(TEST_CONNECTION_STRING))
            {
                sqlConnection.Open();

                int changesReceived = 0;

                using (var sqlDependency = sqlConnection.GetSqlDependencyEx(TEST_TABLE_NAME))
                {
                    sqlDependency.TableChanged += (o, e) => changesReceived++;
                    sqlDependency.Start();

                    MakeTableInsertDeleteChanges(changesCount);

                    // Wait a little bit to receive all changes.
                    Thread.Sleep(1000);
                }
                
                Assert.AreEqual(changesCount, changesReceived);
            }
        }

        private static void MakeTableInsertDeleteChanges(int changesCount)
        {
            for (int i = 0; i < changesCount / 2; i++)
            {
                ExecuteNonQuery(string.Format(INSERT_FORMAT, i), TEST_CONNECTION_STRING);
                ExecuteNonQuery(string.Format(REMOVE_FORMAT, i), TEST_CONNECTION_STRING);
            }
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
    }
}
