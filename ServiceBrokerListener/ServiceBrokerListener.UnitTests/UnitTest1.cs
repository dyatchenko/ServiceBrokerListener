namespace ServiceBrokerListener.UnitTests
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using ServiceBrokerListener = ServiceBrokerListener.Domain.ServiceBrokerListener;

    /// <summary>
    /// TODO:
    /// 1. 
    /// </summary>
    [TestClass]
    public class UnitTest1
    {
        private const string TEST_DATABASE_NAME = "TestDatabase";

        private const string TEST_TABLE_NAME = "TestTable";

        private const string TEST_CONNECTION_STRING =
            "Data Source=DEVSERVER;Initial Catalog=master;Integrated Security=True";

        [TestInitialize]
        public void TestSetup()
        {
            const string CreateDatabaseScript = @"
                CREATE DATABASE " + TEST_DATABASE_NAME;
            const string CreateTableScript = @"                
                USE [" + TEST_DATABASE_NAME + @"]
                CREATE TABLE " + TEST_TABLE_NAME + @" (TestField int)               
                ";
            TestCleanup();
            ExecuteNonQuery(CreateDatabaseScript, TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateTableScript, TEST_CONNECTION_STRING);
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
            ExecuteNonQuery(DropTestDatabaseScript, TEST_CONNECTION_STRING);
        }

        /// <summary>
        /// TODO:
        /// 1. Add remove notification checking.
        /// </summary>
        [TestMethod]
        public void NotificationTest()
        {
            const string InsertFormat =
                "USE [" + TEST_DATABASE_NAME + "] INSERT INTO [" + TEST_TABLE_NAME + "] VALUES({0})";
            const string RemoveFormat =
                "USE [" + TEST_DATABASE_NAME + "] DELETE FROM [" + TEST_TABLE_NAME
                + "] WHERE TestField = {0}";

            using (ServiceBrokerListener testListener = new ServiceBrokerListener(
                TEST_CONNECTION_STRING,
                TEST_DATABASE_NAME,
                TEST_TABLE_NAME,
                ServiceBrokerListener.ListenerTypes.OnInsert
                | ServiceBrokerListener.ListenerTypes.OnDelete,
                60000))
            {
                int changesMade = 0;
                int changesReceived = 0;

                testListener.TableChanged += (o, e) =>
                    {
                        changesReceived++;
                    };
                testListener.StartListen();

                for (int i = 0; i < 10; i++)
                {
                    ExecuteNonQuery(string.Format(InsertFormat, i), TEST_CONNECTION_STRING);
                    changesMade++;
                }

                Thread.Sleep(1000);
                Assert.AreEqual(changesMade, changesReceived);
            }
        }

        [TestMethod]
        public void ResourcesReleasabilityTest()
        {
            //TORO
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
