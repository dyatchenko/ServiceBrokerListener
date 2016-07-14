namespace ServiceBrokerListener.UnitTests
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Text;
    using System.Threading;

    using NUnit.Framework;

    using ServiceBrokerListener.Domain;

    /// <summary>
    /// TODO: 
    /// 1. Performance test.
    /// 2. Check strange behavior.
    /// </summary>
    [TestFixture]
    public class SqlDependencyExTest
    {
        private const string MASTER_CONNECTION_STRING =
            "Data Source=(local);Initial Catalog=master;Integrated Security=True";

        private const string TEST_CONNECTION_STRING =
            "Data Source=(local);Initial Catalog=TestDatabase;User Id=TempLogin;Password=8fdKJl3$nlNv3049jsKK;";

        private const string ADMIN_TEST_CONNECTION_STRING =
            "Data Source=(local);Initial Catalog=TestDatabase;Integrated Security=True";

        private const string INSERT_FORMAT =
            "USE [TestDatabase] INSERT INTO temp.[TestTable] (TestField) VALUES({0})";

        private const string REMOVE_FORMAT =
            "USE [TestDatabase] DELETE FROM temp.[TestTable] WHERE TestField = {0}";

        private const string TEST_DATABASE_NAME = "TestDatabase";

        private const string TEST_TABLE_NAME = "TestTable";

        private const string TEST_TABLE_1_FULL_NAME = "temp.TestTable";

        private const string TEST_TABLE_2_FULL_NAME = "temp2.TestTable";

        private const string TEST_TABLE_3_FULL_NAME = "temp.Order";

        [SetUp]
        public void TestSetup()
        {
            const string CreateDatabaseScript = @"
                CREATE DATABASE TestDatabase;

                ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                ALTER DATABASE [TestDatabase] SET ENABLE_BROKER; 
                ALTER DATABASE [TestDatabase] SET MULTI_USER WITH ROLLBACK IMMEDIATE

                -- FOR SQL Express
                ALTER AUTHORIZATION ON DATABASE::[TestDatabase] TO [sa]
                ALTER DATABASE [TestDatabase] SET TRUSTWORTHY ON; ";
            const string CreateUserScript = @"
                CREATE LOGIN TempLogin 
                WITH PASSWORD = '8fdKJl3$nlNv3049jsKK', DEFAULT_DATABASE=TestDatabase;
                
                USE [TestDatabase];
                CREATE USER TempUser FOR LOGIN TempLogin;

                GRANT CREATE PROCEDURE TO [TempUser];
                GRANT CREATE SERVICE TO [TempUser];
                GRANT CREATE QUEUE  TO [TempUser];
                GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [TempUser]
                GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [TempUser];
                GRANT CONTROL ON SCHEMA::[temp] TO [TempUser]
                GRANT CONTROL ON SCHEMA::[temp2] TO [TempUser];  
                ";
            const string CreateTable1Script = @"
                CREATE SCHEMA Temp
                    CREATE TABLE TestTable (TestField int, StrField NVARCHAR(MAX));
                ";
            const string CreateTable2Script = @"
                CREATE SCHEMA Temp2
                    CREATE TABLE TestTable (TestField int, StrField NVARCHAR(MAX));";
            const string CreateTable3Script = @"
                CREATE TABLE [temp].[Order] (TestField int, StrField NVARCHAR(MAX));
                ";
            const string CreateTable4Script = @"
                CREATE TABLE [temp].[Order2] ([Order] int, StrField NVARCHAR(MAX));
                ";
            const string CreateTable5Script = @"
                CREATE TABLE [temp].[Order3] (TestField int, StrField text);
                ";

            TestCleanup();

            ExecuteNonQuery(CreateDatabaseScript, MASTER_CONNECTION_STRING);
            ExecuteNonQuery(CreateTable1Script, ADMIN_TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateTable2Script, ADMIN_TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateTable3Script, ADMIN_TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateTable4Script, ADMIN_TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateTable5Script, ADMIN_TEST_CONNECTION_STRING);
            ExecuteNonQuery(CreateUserScript, MASTER_CONNECTION_STRING);
        }

        [TearDown]
        public void TestCleanup()
        {
            const string DropTestDatabaseScript = @"
                IF (EXISTS(select * from sys.databases where name='TestDatabase'))
                BEGIN
                    ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [TestDatabase]
                END
                IF (EXISTS(select * from master.dbo.syslogins where name = 'TempLogin'))
                BEGIN
                    DECLARE @loginNameToDrop sysname
                    SET @loginNameToDrop = 'TempLogin';

                    DECLARE sessionsToKill CURSOR FAST_FORWARD FOR
                        SELECT session_id
                        FROM sys.dm_exec_sessions
                        WHERE login_name = @loginNameToDrop
                    OPEN sessionsToKill
                    
                    DECLARE @sessionId INT
                    DECLARE @statement NVARCHAR(200)
                    
                    FETCH NEXT FROM sessionsToKill INTO @sessionId
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        PRINT 'Killing session ' + CAST(@sessionId AS NVARCHAR(20)) + ' for login ' + @loginNameToDrop
                    
                        SET @statement = 'KILL ' + CAST(@sessionId AS NVARCHAR(20))
                        EXEC sp_executesql @statement
                    
                        FETCH NEXT FROM sessionsToKill INTO @sessionId
                    END
                    
                    CLOSE sessionsToKill
                    DEALLOCATE sessionsToKill

                    PRINT 'Dropping login ' + @loginNameToDrop
                    SET @statement = 'DROP LOGIN [' + @loginNameToDrop + ']'
                    EXEC sp_executesql @statement
                END
                ";
            ExecuteNonQuery(DropTestDatabaseScript, MASTER_CONNECTION_STRING);
        }

        [Test]
        public void NotificationTestWith10ChangesAnd10SecDelay()
        {
            NotificationTest(10, 10);
        }

        [Test]
        public void NotificationTestWith10ChangesAnd60SecDelay()
        {
            NotificationTest(10, 60);
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

        [Test]
        public void DetailsTestWith10ChunkInserts()
        {
            DetailsTest(10);
        }

        [Test]
        public void DetailsTestWith100ChunkInserts()
        {
            DetailsTest(100);
        }

        [Test]
        public void DetailsTestWith1000ChunkInserts()
        {
            DetailsTest(1000);
        }

        [Test]
        public void NotificationTypeTestWith10ChunkInserts()
        {
            NotificationTypeTest(10);
        }

        [Test]
        public void NotificationTypeTestWith100ChunkInserts()
        {
            NotificationTypeTest(100);
        }

        [Test]
        public void NotificationTypeTestWith1000ChunkInserts()
        {
            NotificationTypeTest(1000);
        }

        [Test]
        public void MainPermissionExceptionCheckTest()
        {
            ExecuteNonQuery("USE [TestDatabase] DENY CREATE PROCEDURE TO [TempUser];", MASTER_CONNECTION_STRING);
            bool errorReceived = false;
            try
            {
                using (SqlDependencyEx test = new SqlDependencyEx(
                        TEST_CONNECTION_STRING,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME,
                        "temp")) test.Start();
            }
            catch (SqlException) { errorReceived = true; }

            Assert.AreEqual(true, errorReceived);

            // It is impossible to start notification without CREATE PROCEDURE permission.
            ExecuteNonQuery("USE [TestDatabase] GRANT CREATE PROCEDURE TO [TempUser];", MASTER_CONNECTION_STRING);
            errorReceived = false;
            try
            {
                using (SqlDependencyEx test = new SqlDependencyEx(
                        TEST_CONNECTION_STRING,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME,
                        "temp")) test.Start();
            }
            catch (SqlException) { errorReceived = true; }

            Assert.AreEqual(false, errorReceived);

            // There is supposed to be no exceptions with admin rights.
            using (SqlDependencyEx test = new SqlDependencyEx(
                    MASTER_CONNECTION_STRING,
                    TEST_DATABASE_NAME,
                    TEST_TABLE_NAME,
                    "temp")) test.Start();
        }

        [Test]
        public void AdminPermissionExceptionCheckTest()
        {
            const string ScriptDisableBroker = @"
                ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                ALTER DATABASE [TestDatabase] SET DISABLE_BROKER; 
                ALTER DATABASE [TestDatabase] SET MULTI_USER WITH ROLLBACK IMMEDIATE";

            // It is impossible to start notification without configured service broker.
            ExecuteNonQuery(ScriptDisableBroker, MASTER_CONNECTION_STRING);
            bool errorReceived = false;
            try
            {
                using (SqlDependencyEx test = new SqlDependencyEx(
                        TEST_CONNECTION_STRING,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME,
                        "temp")) test.Start();
            }
            catch (SqlException) { errorReceived = true; }

            Assert.AreEqual(true, errorReceived);

            // Service broker supposed to be configured automatically with MASTER connection string.
            NotificationTest(10, connStr: MASTER_CONNECTION_STRING);
        }

        [Test]
        public void GetActiveDbListenersTest()
        {
            Func<int> getDbDepCount =
                () =>
                SqlDependencyEx.GetDependencyDbIdentities(
                    TEST_CONNECTION_STRING,
                    TEST_DATABASE_NAME).Length;

            using (var dep1 = new SqlDependencyEx(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"
                , identity: 4))
            using(var dep2 = new SqlDependencyEx(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"
                , identity: 5))
            {
                dep1.Start();
                
                // Make sure db has been got 1 dependency object.
                Assert.AreEqual(1, getDbDepCount());

                dep2.Start();

                // Make sure db has been got 2 dependency object.
                Assert.AreEqual(2, getDbDepCount());
            }

            // Make sure db has no any dependency objects.
            Assert.AreEqual(0, getDbDepCount());
        }

        [Test]
        public void ClearDatabaseTest()
        {
            Func<int> getDbDepCount =
                () =>
                SqlDependencyEx.GetDependencyDbIdentities(
                    TEST_CONNECTION_STRING,
                    TEST_DATABASE_NAME).Length;

            var dep1 = new SqlDependencyEx(
                TEST_CONNECTION_STRING,
                TEST_DATABASE_NAME,
                TEST_TABLE_NAME,
                "temp",
                identity: 4);
            var dep2 = new SqlDependencyEx(
                TEST_CONNECTION_STRING,
                TEST_DATABASE_NAME,
                TEST_TABLE_NAME,
                "temp",
                identity: 5);

            dep1.Start();
            // Make sure db has been got 1 dependency object.
            Assert.AreEqual(1, getDbDepCount());
            dep2.Start();
            // Make sure db has been got 2 dependency object.
            Assert.AreEqual(2, getDbDepCount());

            // Forced db cleaning
            SqlDependencyEx.CleanDatabase(TEST_CONNECTION_STRING, TEST_DATABASE_NAME);

            // Make sure db has no any dependency objects.
            Assert.AreEqual(0, getDbDepCount());

            dep1.Dispose();
            dep2.Dispose();
        }

        [Test]
        public void TwoTablesNotificationsTest()
        {
            int table1InsertsReceived = 0;
            int table1DeletesReceived = 0;
            int table1TotalNotifications = 0;
            int table1TotalDeleted = 0;

            int table2InsertsReceived = 0;
            int table2DeletesReceived = 0;
            int table2TotalNotifications = 0;
            int table2TotalInserted = 0;

            using (var sqlDependencyFirstTable = new SqlDependencyEx(
                           TEST_CONNECTION_STRING,
                           "TestDatabase",
                           "TestTable",
                           "temp",
                           SqlDependencyEx.NotificationTypes.Delete,
                           true,
                           0))
            {

                sqlDependencyFirstTable.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                    {
                        table1DeletesReceived++;
                        table1TotalDeleted += args.Data.Element("deleted").Elements("row").Count();
                    }

                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                    {
                        table1InsertsReceived++;
                    }

                    table1TotalNotifications++;
                };

                if (!sqlDependencyFirstTable.Active)
                    sqlDependencyFirstTable.Start();

                using (var sqlDependencySecondTable = new SqlDependencyEx(
                                                   TEST_CONNECTION_STRING,
                                                   "TestDatabase",
                                                   "TestTable",
                                                   "temp2",
                                                   SqlDependencyEx.NotificationTypes.Insert,
                                                   true,
                                                   1))
                {

                    sqlDependencySecondTable.TableChanged += (sender, args) =>
                    {
                        if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                        {
                            table2DeletesReceived++;
                        }

                        if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                        {
                            table2InsertsReceived++;
                            table2TotalInserted += args.Data.Element("inserted").Elements("row").Count();
                        }

                        table2TotalNotifications++;
                    };

                    if (!sqlDependencySecondTable.Active)
                        sqlDependencySecondTable.Start();

                    MakeChunkedInsert(5, TEST_TABLE_1_FULL_NAME);
                    MakeChunkedInsert(3, TEST_TABLE_2_FULL_NAME);
                    MakeChunkedInsert(8, TEST_TABLE_2_FULL_NAME);

                    DeleteFirstRow(TEST_TABLE_1_FULL_NAME);
                    DeleteFirstRow(TEST_TABLE_1_FULL_NAME);
                    DeleteFirstRow(TEST_TABLE_1_FULL_NAME);
                    DeleteFirstRow(TEST_TABLE_1_FULL_NAME);

                    DeleteFirstRow(TEST_TABLE_2_FULL_NAME);
                    DeleteFirstRow(TEST_TABLE_2_FULL_NAME);

                    MakeChunkedInsert(1, TEST_TABLE_2_FULL_NAME);
                    MakeChunkedInsert(1, TEST_TABLE_1_FULL_NAME);

                    DeleteFirstRow(TEST_TABLE_1_FULL_NAME);
                    DeleteFirstRow(TEST_TABLE_2_FULL_NAME);

                    // Wait for notification to complete
                    Thread.Sleep(3000);
                }
            }

            Assert.AreEqual(5, table1DeletesReceived);
            Assert.AreEqual(0, table1InsertsReceived);
            Assert.AreEqual(5, table1TotalNotifications);
            Assert.AreEqual(5, table1TotalDeleted);

            Assert.AreEqual(3, table2InsertsReceived);
            Assert.AreEqual(0, table2DeletesReceived);
            Assert.AreEqual(3, table2TotalNotifications);
            Assert.AreEqual(12, table2TotalInserted);
        }

        [Test]
        public void NullCharacterInsertTest()
        {
            int table1InsertsReceived = 0;
            int table1DeletesReceived = 0;
            int table1TotalNotifications = 0;
            int table1TotalDeleted = 0;

            using (var sqlDependencyFirstTable = new SqlDependencyEx(
                           TEST_CONNECTION_STRING,
                           "TestDatabase",
                           "TestTable",
                           "temp",
                           SqlDependencyEx.NotificationTypes.Insert,
                           true,
                           0))
            {

                sqlDependencyFirstTable.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                    {
                        table1DeletesReceived++;
                    }

                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                    {
                        table1InsertsReceived++;
                    }

                    table1TotalNotifications++;
                };

                if (!sqlDependencyFirstTable.Active)
                    sqlDependencyFirstTable.Start();

                MakeNullCharacterInsert();
                MakeNullCharacterInsert();
                MakeNullCharacterInsert();

                // Wait for notification to complete
                Thread.Sleep(3000);
            }

            Assert.AreEqual(0, table1DeletesReceived);
            Assert.AreEqual(3, table1InsertsReceived);
            Assert.AreEqual(3, table1TotalNotifications);
            Assert.AreEqual(0, table1TotalDeleted);
        }

        [Test]
        public void SpecialTableNameWithoutSquareBracketsTest()
        {
            int table1InsertsReceived = 0;
            int table1DeletesReceived = 0;
            int table1TotalNotifications = 0;
            int table1TotalDeleted = 0;

            using (var sqlDependencyFirstTable = new SqlDependencyEx(
                           TEST_CONNECTION_STRING,
                           "TestDatabase",
                           "Order",
                           "temp",
                           SqlDependencyEx.NotificationTypes.Insert,
                           true,
                           0))
            {

                sqlDependencyFirstTable.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                    {
                        table1DeletesReceived++;
                    }

                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                    {
                        table1InsertsReceived++;
                    }

                    table1TotalNotifications++;
                };

                if (!sqlDependencyFirstTable.Active)
                    sqlDependencyFirstTable.Start();

                MakeNullCharacterInsert("[temp].[Order]");
                MakeNullCharacterInsert("[temp].[Order]");
                MakeNullCharacterInsert("[temp].[Order]");

                // Wait for notification to complete
                Thread.Sleep(3000);
            }

            Assert.AreEqual(0, table1DeletesReceived);
            Assert.AreEqual(3, table1InsertsReceived);
            Assert.AreEqual(3, table1TotalNotifications);
            Assert.AreEqual(0, table1TotalDeleted);
        }

        [Test]
        public void SpecialFieldNameWithoutSquareBracketsTest()
        {
            int table1InsertsReceived = 0;
            int table1DeletesReceived = 0;
            int table1TotalNotifications = 0;
            int table1TotalDeleted = 0;

            using (var sqlDependencyFirstTable = new SqlDependencyEx(
                           TEST_CONNECTION_STRING,
                           "TestDatabase",
                           "Order2",
                           "temp",
                           SqlDependencyEx.NotificationTypes.Insert,
                           true,
                           0))
            {

                sqlDependencyFirstTable.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                    {
                        table1DeletesReceived++;
                    }

                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                    {
                        table1InsertsReceived++;
                    }

                    table1TotalNotifications++;
                };

                if (!sqlDependencyFirstTable.Active)
                    sqlDependencyFirstTable.Start();

                MakeNullCharacterInsert("[temp].[Order2]", "[Order]");
                MakeNullCharacterInsert("[temp].[Order2]", "[Order]");
                MakeNullCharacterInsert("[temp].[Order2]", "[Order]");

                // Wait for notification to complete
                Thread.Sleep(3000);
            }

            Assert.AreEqual(0, table1DeletesReceived);
            Assert.AreEqual(3, table1InsertsReceived);
            Assert.AreEqual(3, table1TotalNotifications);
            Assert.AreEqual(0, table1TotalDeleted);
        }

        [Test]
        public void UnsupportedFieldTypeTest()
        {
            int table1InsertsReceived = 0;
            int table1DeletesReceived = 0;
            int table1TotalNotifications = 0;
            int table1TotalDeleted = 0;

            using (var sqlDependencyFirstTable = new SqlDependencyEx(
                           TEST_CONNECTION_STRING,
                           "TestDatabase",
                           "Order3",
                           "temp",
                           SqlDependencyEx.NotificationTypes.Insert,
                           true,
                           0))
            {

                sqlDependencyFirstTable.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Delete)
                    {
                        table1DeletesReceived++;
                    }

                    if (args.NotificationType == SqlDependencyEx.NotificationTypes.Insert)
                    {
                        table1InsertsReceived++;
                    }

                    table1TotalNotifications++;
                };

                if (!sqlDependencyFirstTable.Active)
                    sqlDependencyFirstTable.Start();

                MakeNullCharacterInsert("[temp].[Order3]");
                MakeNullCharacterInsert("[temp].[Order3]");
                MakeNullCharacterInsert("[temp].[Order3]");

                // Wait for notification to complete
                Thread.Sleep(3000);
            }

            Assert.AreEqual(0, table1DeletesReceived);
            Assert.AreEqual(3, table1InsertsReceived);
            Assert.AreEqual(3, table1TotalNotifications);
            Assert.AreEqual(0, table1TotalDeleted);
        }

        [Test]
        public void NotificationWithoutDetailsTest()
        {
            NoDetailsTest(10);
        }

        public void ResourcesReleasabilityTest(int changesCount)
        {
            using (var sqlConnection = new SqlConnection(ADMIN_TEST_CONNECTION_STRING))
            {
                sqlConnection.Open();

                int sqlConversationEndpointsCount = sqlConnection.GetUnclosedConversationEndpointsCount();
                int sqlConversationGroupsCount = sqlConnection.GetConversationGroupsCount();
                int sqlServiceQueuesCount = sqlConnection.GetServiceQueuesCount();
                int sqlServicesCount = sqlConnection.GetServicesCount();
                int sqlTriggersCount = sqlConnection.GetTriggersCount();
                int sqlProceduresCount = sqlConnection.GetProceduresCount();

                using (SqlDependencyEx sqlDependency = new SqlDependencyEx(
                            TEST_CONNECTION_STRING,
                            TEST_DATABASE_NAME,
                            TEST_TABLE_NAME, "temp"))
                {
                    sqlDependency.Start();

                    // Make sure we've created one queue, sevice, trigger and two procedures.
                    Assert.AreEqual(sqlServicesCount + 1, sqlConnection.GetServicesCount());
                    Assert.AreEqual(
                        sqlServiceQueuesCount + 1,
                        sqlConnection.GetServiceQueuesCount());
                    Assert.AreEqual(sqlTriggersCount + 1, sqlConnection.GetTriggersCount());
                    Assert.AreEqual(sqlProceduresCount + 2, sqlConnection.GetProceduresCount());

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
                    sqlConnection.GetUnclosedConversationEndpointsCount());
                Assert.AreEqual(sqlTriggersCount, sqlConnection.GetTriggersCount());
                Assert.AreEqual(sqlProceduresCount, sqlConnection.GetProceduresCount());
            }
        }

        private void NotificationTest(
            int changesCount,
            int changesDelayInSec = 0,
            string connStr = TEST_CONNECTION_STRING)
        {
            int changesReceived = 0;

            using (SqlDependencyEx sqlDependency = new SqlDependencyEx(
                        connStr,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME, "temp")) 
            {
                sqlDependency.TableChanged += (o, e) => changesReceived++;
                sqlDependency.Start();

                Thread.Sleep(changesDelayInSec * 1000);
                MakeTableInsertDeleteChanges(changesCount);

                // Wait a little bit to receive all changes.
                Thread.Sleep(1000);
            }

            Assert.AreEqual(changesCount, changesReceived);
        }

        private static void NotificationTypeTest(int insertsCount)
        {
            NotificationTypeTest(insertsCount, SqlDependencyEx.NotificationTypes.Insert);
            NotificationTypeTest(insertsCount, SqlDependencyEx.NotificationTypes.Delete);
            NotificationTypeTest(insertsCount, SqlDependencyEx.NotificationTypes.Update);
            NotificationTypeTest(
                insertsCount,
                SqlDependencyEx.NotificationTypes.Insert | SqlDependencyEx.NotificationTypes.Delete);
            NotificationTypeTest(
                insertsCount,
                SqlDependencyEx.NotificationTypes.Insert | SqlDependencyEx.NotificationTypes.Update);
            NotificationTypeTest(
                insertsCount,
                SqlDependencyEx.NotificationTypes.Delete | SqlDependencyEx.NotificationTypes.Update);
        }

        private static void NotificationTypeTest(int insertsCount, SqlDependencyEx.NotificationTypes testType)
        {
            int elementsInDetailsCount = 0;
            int changesReceived = 0;
            int expectedElementsInDetails = 0;

            var notificationTypes = GetMembers(testType);
            foreach (var temp in notificationTypes)
            switch (temp)
            {
                case SqlDependencyEx.NotificationTypes.Insert:
                    expectedElementsInDetails += insertsCount / 2;
                    break;
                case SqlDependencyEx.NotificationTypes.Update:
                    expectedElementsInDetails += insertsCount;
                    break;
                case SqlDependencyEx.NotificationTypes.Delete:
                    expectedElementsInDetails += insertsCount / 2;
                    break;
            }

            using (SqlDependencyEx sqlDependency = new SqlDependencyEx(
                        TEST_CONNECTION_STRING,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME, "temp", testType))
            {
                sqlDependency.TableChanged += (o, e) =>
                {
                    changesReceived++;

                    if (e.Data == null) return;

                    var inserted = e.Data.Element("inserted");
                    var deleted = e.Data.Element("deleted");

                    elementsInDetailsCount += inserted != null
                                                  ? inserted.Elements("row").Count()
                                                  : 0;
                    elementsInDetailsCount += deleted != null
                                                  ? deleted.Elements("row").Count()
                                                  : 0;
                };
                sqlDependency.Start();

                MakeChunkedInsertDeleteUpdate(insertsCount);

                // Wait a little bit to receive all changes.
                Thread.Sleep(1000);
            }

            Assert.AreEqual(expectedElementsInDetails, elementsInDetailsCount);
            Assert.AreEqual(notificationTypes.Length, changesReceived);
        }

        private static void DetailsTest(int insertsCount)
        {
            int elementsInDetailsCount = 0;
            int changesReceived = 0;

            using (SqlDependencyEx sqlDependency = new SqlDependencyEx(
                        TEST_CONNECTION_STRING,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME, "temp"))
            {
                sqlDependency.TableChanged += (o, e) =>
                    {
                        changesReceived++;

                        if (e.Data == null) return;

                        var inserted = e.Data.Element("inserted");
                        var deleted = e.Data.Element("deleted");

                        elementsInDetailsCount += inserted != null
                                                      ? inserted.Elements("row").Count()
                                                      : 0;
                        elementsInDetailsCount += deleted != null
                                                      ? deleted.Elements("row").Count()
                                                      : 0;
                    };
                sqlDependency.Start();

                MakeChunkedInsertDeleteUpdate(insertsCount);

                // Wait a little bit to receive all changes.
                Thread.Sleep(1000);
            }

            Assert.AreEqual(insertsCount * 2, elementsInDetailsCount);
            Assert.AreEqual(3, changesReceived);
        }

        private void NoDetailsTest(
            int changesCount,
            int changesDelayInSec = 0,
            string connStr = TEST_CONNECTION_STRING)
        {
            int changesReceived = 0;

            using (SqlDependencyEx sqlDependency = new SqlDependencyEx(
                        connStr,
                        TEST_DATABASE_NAME,
                        TEST_TABLE_NAME, "temp", receiveDetails: false))
            {
                sqlDependency.TableChanged += (o, e) =>
                {
                    Assert.AreEqual(SqlDependencyEx.NotificationTypes.None, e.NotificationType);
                    Assert.AreEqual(0, e.Data.Elements().Count());

                    changesReceived++;
                };
                sqlDependency.Start();

                Thread.Sleep(changesDelayInSec * 1000);
                MakeTableInsertDeleteChanges(changesCount);

                // Wait a little bit to receive all changes.
                Thread.Sleep(1000);
            }

            Assert.AreEqual(changesCount, changesReceived);
        }

        private static void MakeChunkedInsertDeleteUpdate(int changesCount)
        {
            const string ScriptFormat = "INSERT INTO #TmpTbl VALUES({0}, N'{1}')\r\n";

            // insert unicode statement
            StringBuilder scriptResult = new StringBuilder("SELECT 0 AS Number, N'юникод<>_1000001' AS Str INTO #TmpTbl\r\n");
            for (int i = 1; i < changesCount / 2; i++) scriptResult.Append(string.Format(ScriptFormat, i, "юникод<>_" + i));

            scriptResult.Append(@"INSERT INTO temp.TestTable (TestField, StrField)   
                                            SELECT * FROM #TmpTbl");
            ExecuteNonQuery(scriptResult.ToString(), TEST_CONNECTION_STRING);
            ExecuteNonQuery("UPDATE temp.TestTable SET StrField = NULL", TEST_CONNECTION_STRING);
            ExecuteNonQuery("DELETE FROM temp.TestTable", TEST_CONNECTION_STRING);
        }

        private static void MakeChunkedInsert(int chunkSize, string tableName = "temp.TestTable")
        {
            const string ScriptFormat = "INSERT INTO #TmpTbl VALUES({0}, N'{1}')\r\n";

            // insert a unicode statement
            StringBuilder scriptResult = new StringBuilder("SELECT 0 AS Number, N'юникод<>_1000001' AS Str INTO #TmpTbl\r\n");
            for (int i = 1; i < chunkSize; i++) scriptResult.Append(string.Format(ScriptFormat, i, "юникод<>_" + i));

            scriptResult.Append($"INSERT INTO {tableName} (TestField, StrField) SELECT * FROM #TmpTbl");
            ExecuteNonQuery(scriptResult.ToString(), TEST_CONNECTION_STRING);
        }

        private static void MakeNullCharacterInsert(string tableName = "temp.TestTable", string firstFieldName = "TestField", string secondFieldName = "StrField")
        {
            // insert a unicode statement
            StringBuilder scriptResult = new StringBuilder("SELECT 0 AS Number, CONVERT(VARCHAR(MAX), 0x00) AS Str INTO #TmpTbl\r\n");

            scriptResult.Append($"INSERT INTO {tableName} ({firstFieldName}, {secondFieldName}) SELECT * FROM #TmpTbl");
            ExecuteNonQuery(scriptResult.ToString(), TEST_CONNECTION_STRING);
        }

        private static void DeleteFirstRow(string tableName = "temp.TestTable")
        {
            string script = $@"
                WITH q AS
                (
                    SELECT TOP 1 *
                    FROM {tableName}
                )
                DELETE FROM q";

            ExecuteNonQuery(script, TEST_CONNECTION_STRING);
        }

        private static void MakeTableInsertDeleteChanges(int changesCount)
        {
            for (int i = 0; i < changesCount / 2; i++)
            {
                ExecuteNonQuery(string.Format(INSERT_FORMAT, i), MASTER_CONNECTION_STRING);
                ExecuteNonQuery(string.Format(REMOVE_FORMAT, i), MASTER_CONNECTION_STRING);
            }
        }

        private static void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                conn.Open();
                command.CommandType = CommandType.Text;
                command.CommandTimeout = 60000;
                command.ExecuteNonQuery();
            }
        }

        private static SqlDependencyEx.NotificationTypes[] GetMembers(SqlDependencyEx.NotificationTypes value)
        {
            return
                Enum.GetValues(typeof(SqlDependencyEx.NotificationTypes))
                    .Cast<int>()
                    .Where(enumValue => enumValue != 0 && (enumValue & (int)value) == enumValue)
                    .Cast<SqlDependencyEx.NotificationTypes>()
                    .ToArray();
        }
    }
}
