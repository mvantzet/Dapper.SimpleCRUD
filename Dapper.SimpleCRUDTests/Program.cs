using System;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Dapper.SimpleCRUDTests
{
    class Program
    {
        private const bool RunPostgreSQLTests = true;
        private const bool RunSqLiteTests = false;
        private const bool RunMySQLTests = false;
        private const bool RunSQLServerTests = false;

        public static readonly string PostgreSQLConnectionString = string.Format(
            "Server={0};Port={1};User Id={2};Password={3};Database={4};", 
            "localhost", "5432", "postgres", "postgrespass", "testdb");

        public static readonly string MySqlConnectionString =
            string.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};",
                "localhost", "3306", "root", "admin", "testdb");

        public static readonly string SqlConnectionString =
            @"Data Source = .\sqlexpress;Initial Catalog=testdb;Integrated Security=True;MultipleActiveResultSets=true;";

        static void Main()
        {
            // Don't warn about unused code
#pragma warning disable CS0162
            if (RunSQLServerTests)
            {
                SetupSqlServer();
                RunTestsSqlServer();
            }

            if (RunSqLiteTests)
            {
                SetupSqLite();
                RunTestsSqLite();
            }

            if (RunPostgreSQLTests)
            {
                SetupPostgreSQL();
                RunTestsPostgreSQL();
            }

            if (RunMySQLTests)
            {
                SetupMySQL();
                RunTestsMySQL();
            }
#pragma warning restore CS0162

            // Run twice to test cache
            // RunTestsPg();

            RunNonDbTests();

            Console.ReadKey();
        }

        private static void SetupSqlServer()
        {
            using (var connection = new SqlConnection(SqlConnectionString.Replace("testdb", "Master")))
            {
                connection.Open();
                try
                {
                    connection.Execute(@" DROP DATABASE testdb; ");
                }
                catch (Exception)
                { }

                connection.Execute(@" CREATE DATABASE testdb; ");
            }

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();
                connection.Execute(@" create table Users (Id int IDENTITY(1,1) not null, Name nvarchar(100) not null, Age int not null, ScheduledDayOff nvarchar(100) null, CreatedDate datetime DEFAULT(getdate())) ");
                connection.Execute(@" create table Posts (Id int PRIMARY KEY AUTOINCREMENT, Text nvarchar(100) not null, UserId int NOT NULL, ModeratorId int NOT NULL ) ");
                connection.Execute(@" create table Car (CarId int IDENTITY(1,1) not null, Id int null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId bigint IDENTITY(2147483650,1) not null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE SCHEMA Log; ");
                connection.Execute(@" create table Log.CarLog (Id int IDENTITY(1,1) not null, LogNotes nvarchar(100) NOT NULL) ");
                connection.Execute(@" CREATE TABLE [dbo].[GUIDTest]([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY CLUSTERED ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId int IDENTITY(1,1) not null Primary Key, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null)");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id int not null Primary Key, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id int IDENTITY(1,1) not null Primary Key, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE GradingScale ([ScaleID] [int] IDENTITY(1,1) NOT NULL, [AppID] [int] NULL, [ScaleName] [nvarchar](50) NOT NULL, [IsDefault] [bit] NOT NULL)");
                connection.Execute(@" CREATE TABLE KeyMaster ([Key1] [int] NOT NULL, [Key2] [int] NOT NULL, CONSTRAINT [PK_KeyMaster] PRIMARY KEY CLUSTERED ([Key1] ASC, [Key2] ASC))");
                connection.Execute(@" CREATE TABLE [dbo].[stringtest]([stringkey] [varchar](50) NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_stringkey] PRIMARY KEY CLUSTERED ([stringkey] ASC))");
            }
            Console.WriteLine("Created database");
        }

        private static void SetupPostgreSQL()
        {
            using (var connection = new NpgsqlConnection(PostgreSQLConnectionString.Replace("testdb", "postgres")))
            {
                connection.Open();
                // drop  database 
                connection.Execute("DROP DATABASE IF EXISTS testdb;");
                connection.Execute("CREATE DATABASE testdb WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1;");
            }

            using (var connection = new NpgsqlConnection(PostgreSQLConnectionString))
            {
                connection.Open();
                connection.Execute(@" CREATE SEQUENCE bigcar_carid_seq START 2147483650");
                connection.Execute(@" CREATE TABLE ""Users"" (""Id"" SERIAL PRIMARY KEY, ""Name"" varchar not null, ""Age"" int not null, ""ScheduledDayOff"" TEXT null, ""CreatedDate"" date not null default CURRENT_DATE) ");
                connection.Execute(@" CREATE TABLE ""Posts"" (""Id"" SERIAL PRIMARY KEY, ""Text"" varchar not null, ""UserId"" INTEGER NOT NULL, ""ModeratorId"" INTEGER NOT NULL ) ");
                connection.Execute(@" CREATE TABLE ""Car"" (""CarId"" SERIAL PRIMARY KEY, ""Id"" INTEGER null, ""Make"" varchar not null, ""Model"" varchar not null) ");
                connection.Execute(@" CREATE TABLE ""BigCar"" (""CarId"" BIGINT NOT NULL DEFAULT nextval('bigcar_carid_seq'::regclass), ""Make"" varchar not null, ""Model"" varchar not null) ");
                connection.Execute(@" CREATE TABLE ""City"" (""Name"" VARCHAR NOT NULL, ""Population"" INT NOT NULL) ");
                connection.Execute(@" CREATE SCHEMA ""Log""; ");
                connection.Execute(@" CREATE TABLE ""Log"".""CarLog"" (""Id"" SERIAL PRIMARY KEY, ""LogNotes"" varchar NOT NULL) ");
                connection.Execute(@" CREATE TABLE ""GUIDTest"" (""Id"" uuid PRIMARY KEY, ""Name"" varchar NOT NULL)");
                connection.Execute(@" CREATE TABLE ""StrangeColumnNames"" (""ItemId"" Serial PRIMARY KEY, ""Word"" varchar not null, colstringstrangeword varchar, ""KeywordedProperty"" varchar) ");
                connection.Execute(@" CREATE TABLE ""UserWithoutAutoIdentity"" (""Id"" int PRIMARY KEY, ""Name"" varchar not null, ""Age"" int not null) ");
                connection.Execute(@" CREATE TABLE ""IgnoreColumns"" (""Id"" SERIAL PRIMARY KEY not null, ""IgnoreInsert"" varchar(100) null, ""IgnoreUpdate"" varchar(100) null, ""IgnoreSelect"" varchar(100)  null, ""IgnoreAll"" varchar(100) null) ");
                connection.Execute(@" CREATE TABLE ""GradingScale"" (""ScaleID"" SERIAL PRIMARY KEY NOT NULL, ""AppID"" int, ""ScaleName"" varchar(50) NOT NULL, ""IsDefault"" bit NOT NULL)");
                connection.Execute(@" CREATE TABLE ""KeyMaster"" (""Key1"" int NOT NULL, ""Key2"" int NOT NULL, CONSTRAINT keymaster_pk PRIMARY KEY (""Key1"", ""Key2""))");
                connection.Execute(@" CREATE TABLE ""StringTest"" (stringkey varchar(50) NOT NULL, name varchar(50) NOT NULL, CONSTRAINT stringkey_pk PRIMARY KEY (stringkey))");

                // Pg specific test
                connection.Execute(@" CREATE TABLE user2 (id SERIAL PRIMARY KEY, firstname TEXT, lastname TEXT, scheduleddayoff TEXT NULL)");
            }
        }

        private static void SetupSqLite()
        {
            File.Delete(Directory.GetCurrentDirectory() + "\\MyDatabase.sqlite");
            SQLiteConnection.CreateFile("MyDatabase.sqlite");
            var connection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            using (connection)
            {
                connection.Open();
                connection.Execute(@" create table Users (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name nvarchar(100) not null, Age int not null, ScheduledDayOff nvarchar(100) null, CreatedDate datetime default current_timestamp ) ");
                connection.Execute(@" create table Posts (Id INTEGER PRIMARY KEY AUTOINCREMENT, Text nvarchar(100) not null, UserId INTEGER NOT NULL, ModeratorId INTEGER NOT NULL ) ");
                connection.Execute(@" create table Car (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Id INTEGER null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" insert into BigCar (CarId,Make,Model) Values (2147483649,'car','car') ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE TABLE GUIDTest([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY  ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId INTEGER PRIMARY KEY AUTOINCREMENT, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null) ");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id INTEGER PRIMARY KEY, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id INTEGER PRIMARY KEY AUTOINCREMENT, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE KeyMaster (Key1 INTEGER NOT NULL, Key2 INTEGER NOT NULL, PRIMARY KEY ([Key1], [Key2]))");
                connection.Execute(@" CREATE TABLE stringtest (stringkey nvarchar(50) NOT NULL,name nvarchar(50) NOT NULL, PRIMARY KEY ([stringkey] ASC))");
            }
        }

        private static void SetupMySQL()
        {
            using (var connection = new MySqlConnection(MySqlConnectionString.Replace("testdb", "sys")))
            {
                connection.Open();
                // drop  database 
                connection.Execute("DROP DATABASE IF EXISTS testdb;");
                connection.Execute("CREATE DATABASE testdb;");
            }
            Thread.Sleep(1000);

            using (var connection = new MySqlConnection(MySqlConnectionString))
            {
                connection.Open();
                connection.Execute(@" create table Users (Id INTEGER PRIMARY KEY AUTO_INCREMENT, Name nvarchar(100) not null, Age int not null, ScheduledDayOff nvarchar(100) null, CreatedDate datetime default current_timestamp ) ");
                connection.Execute(@" create table Posts (Id INTEGER PRIMARY KEY AUTO_INCREMENT, Text nvarchar(100) not null, UserId int NOT NULL, ModeratorId int NOT NULL ) ");
                connection.Execute(@" create table Car (CarId INTEGER PRIMARY KEY AUTO_INCREMENT, Id INTEGER null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId BIGINT PRIMARY KEY AUTO_INCREMENT, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" insert into BigCar (CarId,Make,Model) Values (2147483649,'car','car') ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE TABLE GUIDTest(Id CHAR(38) NOT NULL,name varchar(50) NOT NULL, CONSTRAINT PK_GUIDTest PRIMARY KEY (Id ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId INTEGER PRIMARY KEY AUTO_INCREMENT, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null) ");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id INTEGER PRIMARY KEY, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id INTEGER PRIMARY KEY AUTO_INCREMENT, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE table KeyMaster (Key1 INTEGER NOT NULL, Key2 INTEGER NOT NULL, CONSTRAINT PK_KeyMaster PRIMARY KEY CLUSTERED (Key1 ASC, Key2 ASC))");
            }
        }

        private static void RunTests(SimpleCRUD.Dialect dialect, params string[] excludeTestsContaining)
        {
            RunTests(new Tests(dialect), dialect, excludeTestsContaining);
        }

        private static void RunTests(object tester, SimpleCRUD.Dialect dialect, params string[] excludeTestsContaining)
        {
            RunTests(tester, " in " + dialect, excludeTestsContaining);
        }

        private static void RunTests(object tester, string logPostfix = "", params string[] excludeTestsContaining)
        {
            foreach (var method in tester.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                //skip schema tests
                if (excludeTestsContaining.Any(t => method.Name.Contains(t))) continue;
                var stopwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + logPostfix);
                method.Invoke(tester, null);
                Console.WriteLine(" - OK! {0}ms", stopwatch.ElapsedMilliseconds);
            }
        }

        private static void RunNonDbTests()
        {
            var stopwatch2 = Stopwatch.StartNew();
            RunTests(new NonDbTests());
            stopwatch2.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch2.Elapsed);
        }

        private static void RunTestsSqlServer()
        {
            var stopwatch = Stopwatch.StartNew();
            RunTests(SimpleCRUD.Dialect.SQLServer);
            stopwatch.Stop();

            // Write result
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            using (var connection = new SqlConnection(@"Data Source=.\sqlexpress;Initial Catalog=Master;Integrated Security=True"))
            {
                connection.Open();
                try
                {
                    //drop any remaining connections, then drop the db.
                    connection.Execute(@" alter database DapperSimpleCrudTestDb set single_user with rollback immediate; DROP DATABASE DapperSimpleCrudTestDb; ");
                }
                catch (Exception)
                { }
            }
            Console.Write("SQL Server testing complete.");
        }

        private static void RunTestsPostgreSQL()
        {
            var stopwatch = Stopwatch.StartNew();
            RunTests(SimpleCRUD.Dialect.PostgreSQL);
            RunTests(new PgTests(), SimpleCRUD.Dialect.PostgreSQL);
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
            Console.Write("PostgreSQL testing complete.");
        }

        private static void RunTestsSqLite()
        {
            var stopwatch = Stopwatch.StartNew();
            RunTests(SimpleCRUD.Dialect.SQLite, "Schema");
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
            Console.Write("SQLite testing complete.");
        }

        private static void RunTestsMySQL()
        {
            var stopwatch = Stopwatch.StartNew();
            RunTests(SimpleCRUD.Dialect.MySQL, "Schema", "Guid");
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            Console.Write("MySQL testing complete.");
        }
    }
}
