using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using JetBrains.Annotations;
using Microsoft.CSharp.RuntimeBinder;

namespace Dapper
{
    /// <summary>
    /// Main class for Dapper.SimpleCRUD extensions
    /// </summary>
    public static partial class SimpleCRUD
    {

        static SimpleCRUD()
        {
            SetDialect(_dialect);
        }

        private static Dialect _dialect = Dialect.PostgreSQL;
        private static string _encapsulation;
        private static string _getIdentitySql;
        private static string _getPagedListSql;

        private static readonly ConcurrentDictionary<Type, string> TableNames = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<string, string> ColumnNames = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> IdProperties = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<string, PropertyInfo[]> ScaffoldableProperties = new ConcurrentDictionary<string, PropertyInfo[]>();

        private static readonly ConcurrentDictionary<string, string> StringBuilderCacheDict = new ConcurrentDictionary<string, string>();
        private static bool StringBuilderCacheEnabled = true;

        private static ITableNameResolver _tableNameResolver = new TableNameResolver();
        private static IColumnNameResolver _columnNameResolver = new ColumnNameResolver();
        private static ISequentialGuidGenerator _sequentialGuidGenerator = new SequentialGuidGenerator();

        /// <summary>
        /// Append a Cached version of a strinbBuilderAction result based on a cacheKey
        /// </summary>
        /// <param name="sb"></param>
        /// <param name="cacheKey"></param>
        /// <param name="stringBuilderAction"></param>
        private static void StringBuilderCache(StringBuilder sb, string cacheKey, Action<StringBuilder> stringBuilderAction)
        {
            if (StringBuilderCacheEnabled && StringBuilderCacheDict.TryGetValue(cacheKey, out string value))
            {
                sb.Append(value);
                return;
            }

            StringBuilder newSb = new StringBuilder();
            stringBuilderAction(newSb);
            value = newSb.ToString();
            StringBuilderCacheDict.AddOrUpdate(cacheKey, value, (t, v) => value);
            sb.Append(value);
        }
        
        /// <summary>
        /// Returns the current dialect name
        /// </summary>
        /// <returns></returns>
        public static string GetDialect()
        {
            return _dialect.ToString();
        }

        /// <summary>
        /// Sets the database dialect 
        /// </summary>
        /// <param name="dialect"></param>
        public static void SetDialect(Dialect dialect)
        {
            switch (dialect)
            {
                case Dialect.PostgreSQL:
                    _dialect = Dialect.PostgreSQL;
                    _encapsulation = "\"{0}\"";
                    _getIdentitySql = string.Format("SELECT LASTVAL() AS id");
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})";
                    break;
                case Dialect.SQLite:
                    _dialect = Dialect.SQLite;
                    _encapsulation = "\"{0}\"";
                    _getIdentitySql = string.Format("SELECT LAST_INSERT_ROWID() AS id");
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})";
                    break;
                case Dialect.MySQL:
                    _dialect = Dialect.MySQL;
                    _encapsulation = "`{0}`";
                    _getIdentitySql = string.Format("SELECT LAST_INSERT_ID() AS id");
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {Offset},{RowsPerPage}";
                    break;
                case Dialect.SQLServer:
                    _dialect = Dialect.SQLServer;
                    _encapsulation = "[{0}]";
                    _getIdentitySql = string.Format("SELECT CAST(SCOPE_IDENTITY()  AS BIGINT) AS [id]");
                    _getPagedListSql = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY {OrderBy}) AS PagedNumber, {SelectColumns} FROM {TableName} {WhereClause}) AS u WHERE PagedNumber BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dialect));
            }
        }

        /// <summary>
        /// Sets the table name resolver
        /// </summary>
        /// <param name="resolver">The resolver to use when requesting the format of a table name</param>
        public static void SetTableNameResolver([NotNull] ITableNameResolver resolver)
        {
            _tableNameResolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        }

        /// <summary>
        /// Sets the column name resolver
        /// </summary>
        /// <param name="resolver">The resolver to use when requesting the format of a column name</param>
        public static void SetColumnNameResolver([NotNull] IColumnNameResolver resolver)
        {
            _columnNameResolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        }

        /// <summary>
        /// Sets the sequential GUID generator
        /// </summary>
        /// <param name="generator">The generator to use for generating a new sequential GUID</param>
        public static void SetSequentialGuidGenerator([NotNull] ISequentialGuidGenerator generator)
        {
            _sequentialGuidGenerator = generator ?? throw new ArgumentNullException(nameof(generator));
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>By default filters on the Id column</para>
        /// <para>-Id column name can be overridden by adding an attribute on your primary key property [Key]</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a single entity by a single id from table T</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a single entity by a single id from table T.</returns>
        [CanBeNull]
        public static T Get<T>(this IDbConnection connection, [NotNull] object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            var currentType = typeof(T);
            var idProps = GetIdProperties(currentType);

            if (!idProps.Any())
                throw new ArgumentException("Get<T> only supports an entity with a [Key] or Id property");

            var name = GetTableName(currentType);
            var sb = new StringBuilder();
            sb.Append("SELECT ");
            //create a new empty instance of the type to get the base properties
            BuildSelect(sb, currentType);
            sb.AppendFormat(" FROM {0} WHERE ", name);

            for (var i = 0; i < idProps.Length; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.AppendFormat("{0} = @{1}", GetColumnName(idProps[i]), idProps[i].Name);
            }

            var dynParms = new DynamicParameters();
            if (idProps.Length == 1)
                dynParms.Add("@" + idProps.First().Name, id);
            else
            {
                foreach (var prop in idProps)
                    dynParms.Add("@" + prop.Name, id.GetType().GetProperty(prop.Name).GetValue(id, null));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("Get<{0}>: {1} with Id: {2}", currentType, sb, id));

            return connection.Query<T>(sb.ToString(), dynParms, transaction, true, commandTimeout).FirstOrDefault();
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var currentType = typeof(T);
            var name = GetTableName(currentType);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions).ToArray();
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect(sb, currentType);
            sb.AppendFormat(" from {0}", name);

            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere<T>(sb, whereprops, whereConditions);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetList<{0}>: {1}", currentType, sb));

            return connection.Query<T>(sb.ToString(), whereConditions, transaction, true, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause and/or order by clause ex: "where name='bob'" or "where age>=@Age"</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a list of entities with optional SQL where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var currentType = typeof(T);
            var name = GetTableName(currentType);

            var sb = new StringBuilder();
            sb.Append("SELECT ");

            BuildSelect(sb, currentType);

            sb.AppendFormat(" FROM {0}", name);

            sb.Append(" " + conditions);

            if (Debugger.IsAttached)
            {
                Trace.WriteLine($"GetList<{currentType}>: {sb}");
            }

            return connection.Query<T>(sb.ToString(), parameters, transaction, true, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a list of all entities</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <returns>Gets a list of all entities</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection)
        {
            return connection.GetList<T>(new { });
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>orderby is a column or list of columns to order by ex: "lastname, age desc" - not required - default is by primary key</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="pageNumber"></param>
        /// <param name="rowsPerPage"></param>
        /// <param name="conditions"></param>
        /// <param name="orderby"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a paged list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection, int pageNumber, int rowsPerPage, string conditions, string orderby, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            if (string.IsNullOrEmpty(_getPagedListSql))
                throw new Exception("GetListPage is not supported with the current SQL Dialect");

            if (pageNumber < 1)
                throw new Exception("Page must be greater than 0");

            var currentType = typeof(T);
            var idProps = GetIdProperties(currentType);
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(currentType);
            var sb = new StringBuilder();
            var query = _getPagedListSql;
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = GetColumnName(idProps.First());
            }

            //create a new empty instance of the type to get the base properties
            BuildSelect(sb, currentType);
            query = query.Replace("{SelectColumns}", sb.ToString());
            query = query.Replace("{TableName}", name);
            query = query.Replace("{PageNumber}", pageNumber.ToString());
            query = query.Replace("{RowsPerPage}", rowsPerPage.ToString());
            query = query.Replace("{OrderBy}", orderby);
            query = query.Replace("{WhereClause}", conditions);
            query = query.Replace("{Offset}", ((pageNumber - 1) * rowsPerPage).ToString());

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetListPaged<{0}>: {1}", currentType, query));

            return connection.Query<T>(query, parameters, transaction, true, commandTimeout);
        }

        private struct Key
        {
            private Guid? _guid;
            private string _string;
            private int _int;
            private uint _uInt;
            private long _long;
            private ulong _uLong;
            private short _short;
            private ushort _uShort;

            public bool IsDefined;
            public Type Type;
            
            public bool IsNumeric { get; private set; }
            public object AsObject { get; private set; }

            public Guid? Guid
            {
                get => _guid;
                set {
                    AsObject = _guid = value;
                    IsNumeric = false;
                }
            }
            
            public string String
            {
                get => _string;
                set
                {
                    AsObject = _string = value;
                    IsNumeric = false;
                }
            }

            public int Int
            {
                get => _int;
                set { AsObject = _int = value; IsNumeric = true; }
            }

            public uint UInt
            {
                get => _uInt;
                set { AsObject = _uInt = value; IsNumeric = true; }
            }

            public long Long
            {
                get => _long;
                set { AsObject = _long = value; IsNumeric = true; }
            }

            public ulong ULong
            {
                get => _uLong;
                set { AsObject = _uLong = value; IsNumeric = true; }
            }

            public short Short
            {
                get => _short;
                set { AsObject = _short = value; IsNumeric = true; }
            }

            public ushort UShort
            {
                get => _uShort;
                set { AsObject = _uShort = value; IsNumeric = true; }
            }
        }

        private static Key GetKey<TEntity>(PropertyInfo[] idProperties, TEntity entityToInsert)
        {
            var keyType = idProperties[0].PropertyType;
            Key key;

            if (keyType == typeof(int))
            {
                key = new Key
                {
                    Type = keyType,
                    Int = (int) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.Int != 0;
            }
            else if (keyType == typeof(long))
            {
                key = new Key
                {
                    Type = keyType,
                    Long = (long) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.Long != 0;
            }
            else if (keyType == typeof(Guid))
            {
                key = new Key
                {
                    Type = keyType, 
                    Guid = (Guid) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.Guid != Guid.Empty;
            }
            else if (keyType == typeof(string))
            {
                key = new Key
                {
                    Type = keyType,
                    String = (string) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.String != null;
            }
            else if (keyType == typeof(uint))
            {
                key = new Key
                {
                    Type = keyType,
                    UInt = (uint) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.UInt != 0;
            }
            else if (keyType == typeof(ulong))
            {
                key = new Key
                {
                    Type = keyType,
                    ULong = (ulong) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.ULong != 0;
            }
            else if (keyType == typeof(short))
            {
                key = new Key
                {
                    Type = keyType,
                    Short = (short) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.Short != 0;
            }
            else if (keyType == typeof(ushort))
            {
                key = new Key
                {
                    Type = keyType,
                    UShort = (ushort) idProperties.First().GetValue(entityToInsert, null),
                };
                key.IsDefined = key.UShort != 0;
            }
            else
            {
                throw new NotSupportedException("Key type " + keyType + " not supported");
            }
            return key;
        }

        /// <summary>
        /// Inserts the entity if the ID is not defined, updates it otherwise.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The (generated) ID</returns>
        public static object InsertOrUpdate<TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            if (entityToInsert == null) throw new ArgumentNullException(nameof(entityToInsert));

            var idProperties = GetIdProperties(typeof(TEntity));
            var key = GetKey(idProperties, entityToInsert);
            if (!key.IsDefined)
            {
                var id = Insert(connection, entityToInsert, idProperties, key, transaction, commandTimeout);
                idProperties.First().SetValue(entityToInsert, id);
            }
            else
            {
                Update(connection, entityToInsert, idProperties, transaction, commandTimeout);
            }
            return key.AsObject;
        }

        /// <summary>
        /// Inserts the entity if the ID is not defined, updates it otherwise.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The (generated) ID</returns>
        public static TKey InsertOrUpdate<TKey, TEntity>(this IDbConnection connection, TEntity entityToInsert,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return (TKey) InsertOrUpdate<TEntity>(connection, entityToInsert, transaction, commandTimeout);
        }

        private const string DefaultMultiQueryDelimiter = "|||";

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TReturn>(sql2, (first, second) =>
                {
                    var objects = SetToNullIfMissing(first, second);
                    return map((TFirst) objects[0], (TSecond) objects[1]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TThird, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TThird, TReturn>(sql2, (first, second, third) => {
                    var objects = SetToNullIfMissing(first, second, third);
                    return map((TFirst) objects[0], (TSecond) objects[1], (TThird) objects[2]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TThird, TFourth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TThird, TFourth, TReturn>(sql2, (first, second, third, fourth) => {
                    var objects = SetToNullIfMissing(first, second, third, fourth);
                    return map((TFirst) objects[0], (TSecond) objects[1], (TThird) objects[2], (TFourth) objects[3]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(sql2, (first, second, third, fourth, fifth) => {
                    var objects = SetToNullIfMissing(first, second, third, fourth, fifth);
                    return map((TFirst) objects[0], (TSecond) objects[1], (TThird) objects[2], (TFourth) objects[3], (TFifth) objects[4]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(sql2, (first, second, third, fourth, fifth, sixth) => {
                    var objects = SetToNullIfMissing(first, second, third, fourth, fifth, sixth);
                    return map((TFirst) objects[0], (TSecond) objects[1], (TThird) objects[2], (TFourth) objects[3], (TFifth) objects[4], (TSixth) objects[5]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        public static IEnumerable<TReturn> MultiQuery<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string delimiter = DefaultMultiQueryDelimiter,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            GetSplittableSql(sql, param, delimiter, out var sql2, out var splitOn);
            return cnn.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(sql2, (first, second, third, fourth, fifth, sixth, theSeventhOne) => {
                    var objects = SetToNullIfMissing(first, second, third, fourth, fifth, sixth, theSeventhOne);
                    return map((TFirst) objects[0], (TSecond) objects[1], (TThird) objects[2], (TFourth) objects[3], (TFifth) objects[4], (TSixth) objects[5], (TSeventh) objects[6]);
                }, param, transaction,
                buffered, splitOn, commandTimeout, commandType);
        }

        private static void GetSplittableSql(string sql, object param, string delimiter, out string splittableSql,
            out string splitOn)
        {
            sql = TemplateEngine.Evaluate(sql, param);

            int i = 0, j;
            var sb = new StringBuilder();
            var sb2 = new StringBuilder();
            
            while ((j = sql.IndexOf(delimiter, i, StringComparison.Ordinal)) >= 0)
            {
                sb.Append(sql.Substring(i, j - i));
                sb.AppendFormat(", 1 as " + _encapsulation + ",", "_split");
                i = j + delimiter.Length;
                if (sb2.Length > 0) sb2.Append(',');
                sb2.Append("_split");
            }
            sb.Append(sql.Substring(i));
            splittableSql = sb.ToString();
            splitOn = sb2.ToString();
        }

        private static object[] SetToNullIfMissing(params object[] objects)
        {
            var result = new object[objects.Length];
            for (var index = 0; index < objects.Length; index++)
            {
                var obj = objects[index];
                var idProperties = GetIdProperties(obj.GetType());
                var key = GetKey(idProperties, obj);
                result[index] = !key.IsDefined ? null : obj;
            }
            return result;
        }

        /// <summary>
        /// <para>Inserts a row into the database, using ONLY the properties defined by TEntity</para>
        /// <para>By default inserts into the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Insert filters out Id column and any columns with the [Key] attribute</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</returns>
        public static TKey Insert<TKey, TEntity>(this IDbConnection connection, [NotNull] TEntity entityToInsert,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return (TKey)Insert(connection, entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Inserts a row into the database, using ONLY the properties defined by TEntity</para>
        /// <para>By default inserts into the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Insert filters out Id column and any columns with the [Key] attribute</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</returns>
        public static object Insert<TEntity>(this IDbConnection connection, [NotNull] TEntity entityToInsert,
                IDbTransaction transaction = null, int? commandTimeout = null)
        {
            if (entityToInsert == null) throw new ArgumentNullException(nameof(entityToInsert));
            var idProps = GetIdProperties(entityToInsert);
            var key = GetKey(idProps, entityToInsert);
            return Insert(connection, entityToInsert, idProps, key, transaction, commandTimeout);
        }

        /// <summary>
        /// Internal method that re-uses id properties and the key that has been looked up already
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="idProps"></param>
        /// <param name="key"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        private static object Insert<TEntity>(this IDbConnection connection, TEntity entityToInsert, PropertyInfo[] idProps, Key key, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var name = GetTableName(entityToInsert);
            var sb = new StringBuilder();
            sb.AppendFormat("insert into {0}", name);
            sb.Append(" (");
            BuildInsertParameters<TEntity>(sb);
            sb.Append(") ");
            sb.Append("values");
            sb.Append(" (");
            BuildInsertValues<TEntity>(sb);
            sb.Append(")");

            if (!key.IsDefined)
            {
                if (idProps.Length > 1)
                {
                    throw new Exception("Cannot auto-generate a composite key");
                }
                if (key.Type == typeof(Guid))
                {
                    key.Guid = SequentialGuid();
                    idProps[0].SetValue(entityToInsert, key.Guid, null);
                    key.IsDefined = true;
                }
                else if (key.IsNumeric)
                {
                    sb.Append(";" + _getIdentitySql);
                }
                else
                {
                    throw new Exception("Cannot auto-generate key type " + key.Type);
                }
            }

            if (Debugger.IsAttached)
            {
                Trace.WriteLine(String.Format("Insert: {0}", sb));
            }

            var result = connection.Query(sb.ToString(), entityToInsert, transaction, true, commandTimeout);

            object id;
            if (key.IsDefined)
            {
                id = key.AsObject;
            }
            else
            {
                // Use Convert.ChangeType to change e.g. Int64 to Int32 where necessary
                id = Convert.ChangeType(result.First().id, idProps[0].PropertyType);
                idProps[0].SetValue(entityToInsert, id);
            }
            return id;
        }

        /// <summary>
        /// <para>Updates a record or records in the database with only the properties of TEntity</para>
        /// <para>By default updates records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Updates records where the Id property and properties with the [Key] attribute match those in the database.</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns number of rows affected</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToUpdate"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of affected records</returns>
        public static int Update<TEntity>(this IDbConnection connection, TEntity entityToUpdate,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {

            var idProps = GetIdProperties(entityToUpdate);
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] or Id property");

            return Update<TEntity>(connection, entityToUpdate, idProps, transaction, commandTimeout);
        }

        private static int Update<TEntity>(this IDbConnection connection, TEntity entityToUpdate, PropertyInfo[] idProps,
            IDbTransaction transaction = null, int? commandTimeout = null) {

            var masterSb = new StringBuilder();
            StringBuilderCache(masterSb, $"{typeof(TEntity).FullName}_Update", sb =>
            {
                var name = GetTableName(entityToUpdate);

                sb.AppendFormat("update {0}", name);

                sb.AppendFormat(" set ");
                BuildUpdateSet(entityToUpdate, sb);
                sb.Append(" where ");
                BuildWhere<TEntity>(sb, idProps, entityToUpdate);

                if (Debugger.IsAttached)
                    Trace.WriteLine(String.Format("Update: {0}", sb));
            });
            return connection.Execute(masterSb.ToString(), entityToUpdate, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a record or records in the database that match the object passed in</para>
        /// <para>-By default deletes records in the table matching the class name</para>
        /// <para>Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the number of records affected</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToDelete"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var masterSb = new StringBuilder();
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_Delete", sb =>
            {

                var idProps = GetIdProperties(entityToDelete);

                if (!idProps.Any())
                    throw new ArgumentException("Entity must have at least one [Key] or Id property");

                var name = GetTableName(entityToDelete);

                sb.AppendFormat("delete from {0}", name);

                sb.Append(" where ");
                BuildWhere<T>(sb, idProps, entityToDelete);

                if (Debugger.IsAttached)
                    Trace.WriteLine(String.Format("Delete: {0}", sb));
            });
            return connection.Execute(masterSb.ToString(), entityToDelete, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a record or records in the database by ID</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where the Id property and properties with the [Key] attribute match those in the database</para>
        /// <para>The number of records affected</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int Delete<T>(this IDbConnection connection, object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var currentType = typeof(T);
            var idProps = GetIdProperties(currentType);
            if (!idProps.Any())
                throw new ArgumentException("Delete<T> only supports an entity with a [Key] or Id property");

            var name = GetTableName(currentType);

            var sb = new StringBuilder();
            sb.AppendFormat("DELETE FROM {0} WHERE ", name);

            for (var i = 0; i < idProps.Length; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.AppendFormat("{0} = @{1}", GetColumnName(idProps[i]), idProps[i].Name);
            }

            var dynParms = new DynamicParameters();
            if (idProps.Length == 1)
                dynParms.Add("@" + idProps.First().Name, id);
            else
            {
                foreach (var prop in idProps)
                    dynParms.Add("@" + prop.Name, id.GetType().GetProperty(prop.Name).GetValue(id, null));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("Delete<{0}> {1}", currentType, sb));

            return connection.Execute(sb.ToString(), dynParms, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a list of records in the database</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where that match the where clause</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// <para>The number of records affected</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int DeleteList<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var masterSb = new StringBuilder();
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_DeleteWhere{whereConditions?.GetType()?.FullName}", sb =>
            {
                var currentType = typeof(T);
                var name = GetTableName(currentType);

                var whereprops = GetAllProperties(whereConditions).ToArray();
                sb.AppendFormat("Delete from {0}", name);
                if (whereprops.Any())
                {
                    sb.Append(" where ");
                    BuildWhere<T>(sb, whereprops);
                }

                if (Debugger.IsAttached)
                    Trace.WriteLine(String.Format("DeleteList<{0}> {1}", currentType, sb));
            });
            return connection.Execute(masterSb.ToString(), whereConditions, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a list of records in the database</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where that match the where clause</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age"</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int DeleteList<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var masterSb = new StringBuilder();
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_DeleteWhere{conditions}", sb =>
            {
                if (string.IsNullOrEmpty(conditions))
                    throw new ArgumentException("DeleteList<T> requires a where clause");
                if (!conditions.ToLower().Contains("where"))
                    throw new ArgumentException("DeleteList<T> requires a where clause and must contain the WHERE keyword");

                var currentType = typeof(T);
                var name = GetTableName(currentType);

                sb.AppendFormat("Delete from {0}", name);
                sb.Append(" " + conditions);

                if (Debugger.IsAttached)
                    Trace.WriteLine(String.Format("DeleteList<{0}> {1}", currentType, sb));
            });
            return connection.Execute(masterSb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a number of records entity by a single id from table T</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a count of records.</returns>
        public static int RecordCount<T>(this IDbConnection connection, string conditions = "", object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var currentType = typeof(T);
            var name = GetTableName(currentType);
            var sb = new StringBuilder();
            sb.Append("Select count(1)");
            sb.AppendFormat(" from {0}", name);
            sb.Append(" " + conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("RecordCount<{0}>: {1}", currentType, sb));

            return connection.ExecuteScalar<int>(sb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a number of records entity by a single id from table T</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a count of records.</returns>
        public static int RecordCount<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var currentType = typeof(T);
            var name = GetTableName(currentType);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions).ToArray();
            sb.Append("Select count(1)");
            sb.AppendFormat(" from {0}", name);
            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere<T>(sb, whereprops);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("RecordCount<{0}>: {1}", currentType, sb));

            return connection.ExecuteScalar<int>(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        //build update statement based on list on an entity
        private static void BuildUpdateSet<T>(T entityToUpdate, StringBuilder masterSb)
        {
            var currentType = typeof(T);
            StringBuilderCache(masterSb, $"{currentType.FullName}_BuildUpdateSet", sb =>
            {
                var nonIdProps = GetUpdateableProperties<T>();

                var addedAny = false;
                foreach (var property in nonIdProps)
                {
                    if (addedAny) sb.Append(", ");
                    sb.AppendFormat("{0} = @{1}", GetColumnName(property), property.Name);
                    addedAny = true;
                }
            });
        }

        /// <summary>
        /// build select clause based on list of properties skipping ones with the IgnoreSelect and NotMapped attribute 
        /// </summary>
        /// <param name="masterSb"></param>
        /// <param name="currentType"></param>
        private static void BuildSelect(StringBuilder masterSb, Type currentType)
        {
            var propertyInfos = GetScaffoldableProperties(currentType);
            StringBuilderCache(masterSb, $"{currentType.FullName}_BuildSelect", sb =>
            {
                var addedAny = false;
                foreach (var property in propertyInfos)
                {
                    if (TryGetAttributeNamed(property, nameof(IgnoreSelectAttribute), out _))
                    {
                        continue;
                    }
                    if (addedAny)
                    {
                        sb.Append(",");
                    }
                    var columnName = GetColumnName(property);
                    sb.Append(columnName);
                    if (columnName != Encapsulate(property.Name))
                    {
                        sb.Append(" AS " + Encapsulate(property.Name));
                    }
                    addedAny = true;
                }
            });
        }

        private static void BuildWhere<TEntity>(StringBuilder sb, PropertyInfo[] whereConditionProperties, object whereConditions = null)
        {
            var currentType = typeof(TEntity);
            var sourceProperties = GetScaffoldableProperties(currentType);
            var addedAny = false;

            foreach (var whereProp in whereConditionProperties)
            {
                var useIsNull = false;

                // Match up generic properties to source entity properties to allow fetching of the column attribute
                // The anonymous object used for search doesn't have the custom attributes attached to them so this allows us to build the correct where clause
                // by converting the model type to the database column name via the column attribute
                PropertyInfo propertyToUse = null;
                foreach (var sourceProperty in sourceProperties)
                {
                    if (sourceProperty.Name != whereProp.Name) continue;

                    propertyToUse = sourceProperty;

                    if (whereConditions != null 
                        && whereProp.CanRead 
                        && (whereProp.GetValue(whereConditions, null) == null || whereProp.GetValue(whereConditions, null) == DBNull.Value))
                    {
                        useIsNull = true;
                    }
                    break;
                }

                if (propertyToUse == null)
                {
                    throw new ArgumentOutOfRangeException($"Unknown or unmapped property '{whereProp.Name}'");
                }
                if (addedAny)
                {
                    sb.AppendFormat(" and ");
                }
                sb.AppendFormat(
                    useIsNull ? "{0} IS NULL" : "{0} = @{1}",
                    GetColumnName(propertyToUse),
                    propertyToUse.Name);
                addedAny = true;
            }
        }

        //build insert values which include all properties in the class that are:
        //Not named Id
        //Not marked with the Editable(false) attribute
        //Not marked with the [Key] attribute (without required attribute)
        //Not marked with [IgnoreInsert]
        //Not marked with [NotMapped]
        internal static void BuildInsertValues<T>(StringBuilder masterSb)
        {
            var type = typeof(T);
            StringBuilderCache(masterSb, $"{type.FullName}_BuildInsertValues", sb =>
            {
                var props = GetScaffoldableProperties(type).ToArray();
                var addedAny = false;
                foreach (var property in props)
                {
                    if (!IsPropertyToUpdateOrInsert(property))
                    {
                        continue;
                    }
                    if (addedAny) sb.Append(", ");
                    sb.AppendFormat("@{0}", property.Name);
                    addedAny = true;
                }
            });
        }

        //build insert parameters which include all properties in the class that are not:
        //marked with the Editable(false) attribute
        //marked with the [Key] attribute
        //marked with [IgnoreInsert]
        //named Id
        //marked with [NotMapped]
        private static void BuildInsertParameters<T>(StringBuilder masterSb)
        {
            var currentType = typeof(T);
            StringBuilderCache(masterSb, $"{currentType.FullName}_BuildInsertParameters", sb =>
            {
                var props = GetScaffoldableProperties(currentType).ToArray();
                var addedAny = false;
                foreach (var property in props)
                {
                    if (!IsPropertyToUpdateOrInsert(property)) continue;
                    if (addedAny)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(GetColumnName(property));
                    addedAny = true;
                }
            });
        }

        // Get all properties in an entity
        private static IEnumerable<PropertyInfo> GetAllProperties<T>(T entity) where T : class
        {
            if (entity == null) return new PropertyInfo[0];
            return entity.GetType().GetProperties();
        }

        // Get all properties that are:
        // - Not decorated with Editable(false) or NotMapped
        // - Simple (int, bool, string, etc.)
        // - Or not simple but decorated with the Editable(true) attribute
        internal static PropertyInfo[] GetScaffoldableProperties(Type type, bool ignoreInherited = false)
        {
            var cacheKey = $"{type.FullName}{(ignoreInherited ? "." : "")}";
            if (ScaffoldableProperties.TryGetValue(cacheKey, out var result)) return result;

            var properties = ignoreInherited
                ? type.GetProperties(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public)
                : type.GetProperties(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public);

            result = properties
                .Where(p =>
                {
                    var isEditable = IsEditableProperty(p);
                    if (isEditable == false) return false;
                    if (TryGetAttributeNamed(p, nameof(NotMappedAttribute), out _)) return false;
                    if (p.PropertyType.IsSimpleType()) return true;
                    if (IsInstanceOfGenericType(p.PropertyType, typeof(EnumString<>)))
                    {
                        RegisterEnumStringHandler(p.PropertyType);
                        return true;
                    }
                    return isEditable == true;
                })
                .ToArray();
            ScaffoldableProperties.AddOrUpdate(cacheKey, result, (t, p) => p);
            return result;
        }

        /// <summary>
        /// Adapted from https://stackoverflow.com/a/982540
        /// </summary>
        /// <param name="genericType"></param>
        /// <param name="instanceType"></param>
        /// <returns></returns>
        static bool IsInstanceOfGenericType(Type type, Type genericType)
        {
            while (type != null)
            {
                if (type.IsGenericType &&
                    type.GetGenericTypeDefinition() == genericType)
                {
                    return true;
                }
                type = type.BaseType;
            }
            return false;
        }

        /// <summary>
        /// Get all properties that are:
        /// - Not named Id
        /// - Not marked with the Key attribute
        /// - Not marked ReadOnly
        /// - Not marked IgnoreInsert
        /// - Not marked NotMapped
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static PropertyInfo[] GetUpdateableProperties<T>()
        {
            var updateableProperties = GetScaffoldableProperties(typeof(T))
                .Where(p => !IsIdProperty(p)
                            && !IsKeyProperty(p)
                            && !IsReadOnlyProperty(p)
                            && !TryGetAttributeNamed(p, nameof(IgnoreUpdateAttribute), out _))
                .ToArray();
            return updateableProperties;
        }

        /// <summary>
        /// Get all properties that are named Id or have the Key attribute
        /// For Inserts and updates we have a whole entity so this method is used 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private static PropertyInfo[] GetIdProperties(object entity)
        {
            var type = entity.GetType();
            return GetIdProperties(type);
        }

        /// <summary>
        /// Gets the table name for this entity
        /// For Inserts and updates we have a whole entity so this method is used
        /// Uses class name by default and overrides if the class has a Table attribute 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private static string GetTableName(object entity)
        {
            var type = entity.GetType();
            return GetTableName(type);
        }

        /// <summary>
        /// Get all properties that are named Id or have the Key attribute
        /// For Get(id) and Delete(id) we don't have an entity, just the type so this method is used
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static PropertyInfo[] GetIdProperties(Type type)
        {
            if (IdProperties.TryGetValue(type, out var idProperties))
            {
                return idProperties;
            }
            PropertyInfo idPropertyInfo = null;
            var keyProperties = new List<PropertyInfo>();
            foreach (var prop in type.GetProperties())
            {
                if (IsIdProperty(prop))
                {
                    idPropertyInfo = prop;
                }
                if (IsKeyProperty(prop))
                {
                    keyProperties.Add(prop);
                }
            }
            if (idPropertyInfo == null && keyProperties.Count == 0)
            {
                throw new Exception($"Cannot find a primary key for type {type.Name}, please use [Key] attribute!");
            }
            if (keyProperties.Count == 0) keyProperties.Add(idPropertyInfo);

            idProperties = keyProperties.ToArray();
            IdProperties.AddOrUpdate(type, idProperties, (t, v) => idProperties);
            return idProperties;
        }

        private static bool IsIdProperty(PropertyInfo property)
        {
            return property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsKeyProperty(PropertyInfo property)
        {
            return TryGetAttributeNamed(property, nameof(KeyAttribute), out _);
        }

        private static bool IsReadOnlyProperty(PropertyInfo property)
        {
            return TryGetAttributeNamed(property, nameof(ReadOnlyAttribute), out dynamic attr)
                   && attr.IsReadOnly;
        }

        private static bool? IsEditableProperty(PropertyInfo property)
        {
            if (TryGetAttributeNamed(property, nameof(EditableAttribute), out dynamic attr))
            {
                return attr.AllowEdit;
            }
            return null;
        }

        private static bool TryGetAttribute<T>(PropertyInfo property, out T attribute) where T : Attribute
        {
            foreach (var attr in property.GetCustomAttributes(true))
            {
                if (attr is T attr1)
                {
                    attribute = attr1;
                    return true;
                }
            }
            attribute = null;
            return false;
        }

        private static bool TryGetAttributeNamed(Type type, string name, out dynamic attribute)
        {
            foreach (var attr in type.GetCustomAttributes(true))
            {
                if (attr.GetType().Name == name)
                {
                    attribute = attr;
                    return true;
                }
            }
            attribute = null;
            return false;
        }

        private static bool TryGetAttributeNamed(PropertyInfo property, string name, out dynamic attribute)
        {
            foreach (var attr in property.GetCustomAttributes(true))
            {
                if (attr.GetType().Name == name)
                {
                    attribute = attr;
                    return true;
                }
            }
            attribute = null;
            return false;
        }

        private static bool IsPropertyToUpdateOrInsert(PropertyInfo property)
        {
            if (TryGetAttributeNamed(property, nameof(IgnoreInsertAttribute), out _)
                || IsReadOnlyProperty(property))
                return false;

            if (property.PropertyType != typeof(Guid)
                && property.PropertyType != typeof(string)
                && (IsIdProperty(property) || TryGetAttributeNamed(property, nameof(KeyAttribute), out _))
                && !TryGetAttributeNamed(property, nameof(RequiredAttribute), out _))
                return false;

            return true;
        }

        /// <summary>
        /// Gets the table name for this type
        /// For Get(id) and Delete(id) we don't have an entity, just the type so this method is used
        /// Use dynamic type to be able to handle both our Table-attribute and the DataAnnotation
        /// Uses class name by default and overrides if the class has a Table attribute
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static string GetTableName(Type type)
        {
            if (TableNames.TryGetValue(type, out var tableName))
                return tableName;

            tableName = _tableNameResolver.ResolveTableName(type);

            TableNames.AddOrUpdate(type, tableName, (t, v) => tableName);

            return tableName;
        }

        private static string GetColumnName(PropertyInfo propertyInfo)
        {
            string columnName, key = $"{propertyInfo.DeclaringType}.{propertyInfo.Name}";

            if (ColumnNames.TryGetValue(key, out columnName))
                return columnName;

            columnName = _columnNameResolver.ResolveColumnName(propertyInfo);

            ColumnNames.AddOrUpdate(key, columnName, (t, v) => columnName);

            return columnName;
        }

        private static string Encapsulate(string databaseWord)
        {
            return string.Format(_encapsulation, databaseWord);
        }

        /// <summary>
        /// Generates a GUID based on the current date/time
        /// http://stackoverflow.com/questions/1752004/sequential-guid-generator-c-sharp
        /// </summary>
        /// <returns></returns>
        public static Guid SequentialGuid()
        {
            return _sequentialGuidGenerator.NewGuid();
        }

        public interface ISequentialGuidGenerator
        {
            Guid NewGuid();
        }

        /// <summary>
        /// TODO: Is this specific to SQL Server or does it also work for PostgreSQL?
        /// </summary>
        public class SequentialGuidGenerator : ISequentialGuidGenerator
        {
            public Guid NewGuid()
            {
                var tempGuid = Guid.NewGuid();
                var bytes = tempGuid.ToByteArray();
                var time = DateTime.Now;
                bytes[3] = (byte) time.Year;
                bytes[2] = (byte) time.Month;
                bytes[1] = (byte) time.Day;
                bytes[0] = (byte) time.Hour;
                bytes[5] = (byte) time.Minute;
                bytes[4] = (byte) time.Second;
                return new Guid(bytes);
            }
        }

        /// <summary>
        /// Database server dialects
        /// </summary>
        public enum Dialect
        {
            SQLServer,
            PostgreSQL,
            SQLite,
            MySQL,
        }

        public interface INameFormatter
        {
            string Format(string name);
        }

        public class DefaultNameFormatter : INameFormatter
        {
            public string Format(string name)
            {
                return name;
            }
        }

        public class LowercaseFormatter : SimpleCRUD.INameFormatter
        {
            public string Format(string name)
            {
                return name.ToLowerInvariant();
            }
        }

        /// <summary>
        /// Transforms e.g. "MyProperty" into "my_property"
        /// </summary>
        public class LowercaseUnderscoreFormatter : SimpleCRUD.INameFormatter
        {
            public string Format(string name)
            {
                StringBuilder sb = new StringBuilder();
                bool addUnderscore = false;
                foreach (var c in name)
                {
                    if (char.IsDigit(c))
                    {
                        if (addUnderscore) sb.Append('_');
                        sb.Append(c);
                        addUnderscore = false;
                    }
                    else if (char.IsUpper(c))
                    {
                        if (sb.Length > 0 || addUnderscore)
                        {
                            sb.Append("_");
                        }

                        sb.Append(char.ToLowerInvariant(c));
                        addUnderscore = false;
                    }
                    else if (char.IsLetter(c))
                    {
                        if (addUnderscore) sb.Append('_');
                        sb.Append(c);
                        addUnderscore = false;
                    }
                    else
                    {
                        addUnderscore = true;
                    }
                }
                return sb.ToString();
            }
        }

        public interface ITableNameResolver
        {
            string ResolveTableName(Type type);
        }

        public interface IColumnNameResolver
        {
            string ResolveColumnName(PropertyInfo propertyInfo);
        }

        public class TableNameResolver : ITableNameResolver
        {
            private readonly INameFormatter _nameFormatter;

            public TableNameResolver(INameFormatter nameFormatter)
            {
                _nameFormatter = nameFormatter;
            }

            public TableNameResolver() : this(new DefaultNameFormatter())
            {
            }

            public virtual string ResolveTableName(Type type)
            {
                string tableName;
                if (TryGetAttributeNamed(type, nameof(TableAttribute), out dynamic tableattr))
                {
                    tableName = Encapsulate(tableattr.Name);
                    try
                    {
                        if (!String.IsNullOrEmpty(tableattr.Schema))
                        {
                            string schemaName = Encapsulate(tableattr.Schema);
                            tableName = $"{schemaName}.{tableName}";
                        }
                    }
                    catch (RuntimeBinderException)
                    {
                        //Schema doesn't exist on this attribute.
                    }
                }
                else
                {
                    tableName = Encapsulate(_nameFormatter.Format(type.Name));
                }
                return tableName;
            }
        }

        public class ColumnNameResolver : IColumnNameResolver
        {
            private readonly INameFormatter _nameFormatter;

            public ColumnNameResolver(INameFormatter nameFormatter)
            {
                 _nameFormatter = nameFormatter;
            }

            public ColumnNameResolver() : this(new DefaultNameFormatter())
            {
            }

            public virtual string ResolveColumnName(PropertyInfo propertyInfo)
            {
                string columnName;

                if (TryGetAttributeNamed(propertyInfo, nameof(ForeignKeyAttribute), out dynamic foreignKeyAttribute))
                {
                    columnName = foreignKeyAttribute.Name;
                }
                else if (TryGetAttributeNamed(propertyInfo, nameof(ColumnAttribute), out dynamic columnAttr))
                {
                    columnName = columnAttr.Name;
                    if (Debugger.IsAttached)
                        Trace.WriteLine(String.Format("Column name for type overridden from {0} to {1}", propertyInfo.Name, columnName));
                }
                else
                {
                    columnName = _nameFormatter.Format(propertyInfo.Name);
                }
                return Encapsulate(columnName);
            }
        }

        private static readonly ConcurrentDictionary<string, object> _enumStringHandlers = new ConcurrentDictionary<string, object>();

        private static void RegisterEnumStringHandler(Type t)
        {
            _enumStringHandlers.AddOrUpdate(t.FullName, (type) =>
            {
                var enumType = t.GetGenericArguments()[0];
                var handler = (SqlMapper.ITypeHandler)Activator.CreateInstance(typeof(EnumStringHandler<>).MakeGenericType(enumType));
                SqlMapper.AddTypeHandler(t, handler);
                return handler;
            }, (type, handler) => handler);
        }

        private class EnumStringHandler<T> : SqlMapper.TypeHandler<EnumString<T>>
        {
            public override void SetValue(IDbDataParameter parameter, EnumString<T> value)
            {
                parameter.Value = value.ToString();
            }

            public override EnumString<T> Parse(object value)
            {
                return new EnumString<T>((string)value);
            }
        }

        public static class TemplateEngine
        {
            private static readonly Regex OptionalPartRegex = new Regex(@"(?<!@)@(?'Name'[a-z][a-z0-9_]*)\?\{", RegexOptions.IgnoreCase | RegexOptions.Compiled);
            private static readonly Regex InsertRegex = new Regex(@"@@(?'Name'[a-z][a-z0-9_]*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            public static string Evaluate(string sql, dynamic parameters)
            {
                var sb = new StringBuilder();
                var searchFrom = 0;
                Match m;
                Dictionary<string, object> parameterLookup = null; // Lazy initialization

                // First pass: fill in @Param?{ ... }
                while ((m = OptionalPartRegex.Match(sql, searchFrom)).Success)
                {
                    // Find closing brace after "@Param?{"
                    var start = m.Index + m.Length;
                    if (!TryFindIfElse(sql, start, out int end, out string ifText, out string elseText))
                    {
                        throw new FormatException("Could not find closing brace for expression at index " + m.Index);
                    }

                    // If we did not initialize the parameter lookup yet, do it here
                    if (parameterLookup == null)
                    {
                        parameterLookup = GetParameterLookup(parameters);
                    }

                    // Add everything up to where we found the opening "?@Prop{"
                    sb.Append(sql.Substring(searchFrom, m.Index - searchFrom));

                    bool condition;
                    var parameterName = m.Groups["Name"].Value;
                    if (!parameterLookup.TryGetValue(parameterName, out object val) || val == null)
                    {
                        // There is no property with the extracted name, or it is null, do not include
                        condition = false;
                    }
                    else if (val is bool b)
                    {
                        condition = b;
                    }
                    else
                    {
                        condition = true;
                    }

                    if (condition)
                    {
                        sb.Append(ifText);
                    }
                    else if (elseText != null)
                    {
                        sb.Append(elseText);
                    }
                    // Resume search after "}"
                    searchFrom = end + 1;
                }

                if (searchFrom > 0)
                {
                    sb.Append(sql.Substring(searchFrom));
                    sql = sb.ToString();
                }

                // Second pass: fill in @@Name 
                sb.Clear();
                searchFrom = 0;
                while ((m = InsertRegex.Match(sql, searchFrom)).Success)
                {
                    // Add everything up to where we found the opening "@@Prop"
                    sb.Append(sql.Substring(searchFrom, m.Index - searchFrom));

                    // If we did not initialize the parameter lookup yet, do it here
                    if (parameterLookup == null)
                    {
                        parameterLookup = GetParameterLookup(parameters);
                    }
                    var parameterName = m.Groups["Name"].Value;
                    sb.Append(parameterLookup.TryGetValue(parameterName, out var val) ? val : "");

                    // Resume search after match 
                    searchFrom = m.Index + m.Length;
                }

                if (searchFrom > 0)
                {
                    sb.Append(sql.Substring(searchFrom));
                    sql = sb.ToString();
                }

                return sql;
            }

            private static Dictionary<string, object> GetParameterLookup(dynamic parameters)
            {
                PropertyInfo[] props =
                    parameters == null
                        ? Array.Empty<PropertyInfo>()
                        : (PropertyInfo[]) parameters
                            .GetType()
                            .GetProperties(BindingFlags.Instance | BindingFlags.Public);
                return props.ToDictionary(p => p.Name, p => p.GetValue(parameters));
            }

            /// <summary>
            /// Finds if/else strings in format { .. if .. } or { .. if ... }:{ .. else .. }
            /// </summary>
            /// <param name="s">string to search</param>
            /// <param name="start">index of first character after '{'</param>
            /// <param name="end">index of last '}'</param>
            /// <param name="ifText">text inside 'if' section if found</param>
            /// <param name="elseText">text inside 'else' section if found</param>
            /// <returns>true if 'if' section (optionally also 'else' section) can be found</returns>
            private static bool TryFindIfElse(string s, int start, out int end, out string ifText,
                out string elseText)
            {
                ifText = null;
                elseText = null;
                if (!TryFindClosingBrace(s, start, out end, out ifText))
                {
                    return false;
                }
                // See if we can find an ":{...}" else-block after this one.
                start = end + 1;
                if (start < s.Length - 2
                    && s[start++] == ':'
                    && s[start++] == '{'
                    && TryFindClosingBrace(s, start, out var elseEnd, out elseText))
                {
                    end = elseEnd;
                }
                else
                {
                    elseText = null;
                }
                return true;
            }

            /// <summary>
            /// Find a closing '}' brace that is not escaped (not preceded by a backslash)
            /// </summary>
            /// <param name="s">String to search</param>
            /// <param name="index">Index where to start search (first character after '{')</param>
            /// <param name="end">Index where closing brace '}' is located</param>
            /// <param name="innerText">Unescaped text (backslashes removed)</param>
            /// <returns></returns>
            private static bool TryFindClosingBrace(string s, int index, out int end, out string innerText)
            {
                bool escape = false;
                StringBuilder innerTextBuilder = new StringBuilder();
                while (index < s.Length)
                {
                    var c = s[index];
                    if (c == '\\')
                    {
                        escape = true;
                    }
                    else
                    {
                        if (c == '}' && !escape)
                        {
                            end = index;
                            innerText = innerTextBuilder.ToString();
                            return true;
                        }
                        innerTextBuilder.Append(c);
                        escape = false;
                    }
                    index++;
                }
                end = -1;
                innerText = null;
                return false;
            }
        }
    }
    
    /// <summary>
    /// Optional Table attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the table name of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        /// <summary>
        /// Optional Table attribute.
        /// </summary>
        /// <param name="tableName"></param>
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }
        /// <summary>
        /// Name of the table
        /// </summary>
        public string Name { get; private set; }
        /// <summary>
        /// Name of the schema
        /// </summary>
        public string Schema { get; set; }
    }

    /// <summary>
    /// Optional Column attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the table name of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        /// <summary>
        /// Optional Column attribute.
        /// </summary>
        /// <param name="columnName"></param>
        public ColumnAttribute(string columnName)
        {
            Name = columnName;
        }
        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; private set; }
    }

    /// <summary>
    /// Optional Key attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the Primary Key of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional NotMapped attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify that the property is not mapped
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NotMappedAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional Key attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify a required property of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class RequiredAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional Editable attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the properties that are editable
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class EditableAttribute : Attribute
    {
        /// <summary>
        /// Optional Editable attribute.
        /// </summary>
        /// <param name="iseditable"></param>
        public EditableAttribute(bool iseditable)
        {
            AllowEdit = iseditable;
        }
        /// <summary>
        /// Does this property persist to the database?
        /// </summary>
        public bool AllowEdit { get; private set; }
    }

    /// <summary>
    /// Optional Readonly attribute.
    /// You can use the System.ComponentModel version in its place to specify the properties that are editable
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ReadOnlyAttribute : Attribute
    {
        /// <summary>
        /// Optional ReadOnly attribute.
        /// </summary>
        /// <param name="isReadOnly"></param>
        public ReadOnlyAttribute(bool isReadOnly)
        {
            IsReadOnly = isReadOnly;
        }
        /// <summary>
        /// Does this property persist to the database?
        /// </summary>
        public bool IsReadOnly { get; private set; }
    }

    /// <summary>
    /// Optional IgnoreSelect attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Select methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreSelectAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional IgnoreInsert attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Insert methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreInsertAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional IgnoreUpdate attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Update methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreUpdateAttribute : Attribute
    {
    }

}

internal static class TypeExtension
{
    //You can't insert or update complex types. Lets filter them out.
    public static bool IsSimpleType(this Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type);
        type = underlyingType ?? type;
        var simpleTypes = new List<Type>
                               {
                                   typeof(byte),
                                   typeof(sbyte),
                                   typeof(short),
                                   typeof(ushort),
                                   typeof(int),
                                   typeof(uint),
                                   typeof(long),
                                   typeof(ulong),
                                   typeof(float),
                                   typeof(double),
                                   typeof(decimal),
                                   typeof(bool),
                                   typeof(string),
                                   typeof(char),
                                   typeof(Guid),
                                   typeof(DateTime),
                                   typeof(DateTimeOffset),
                                   typeof(TimeSpan),
                                   typeof(byte[])
                               };
        return simpleTypes.Contains(type) || type.IsEnum;
    }

    public static string CacheKey(this IEnumerable<PropertyInfo> props)
    {
        return string.Join(",",props.Select(p=> p.DeclaringType.FullName + "." + p.Name).ToArray());
    }
}

public class EnumString<T>
{
    private readonly T _enum;

    public EnumString(T val)
    {
        _enum = val;
    }

    public EnumString(string value)
    {
        _enum = (T)Enum.Parse(typeof(T), value);
    }

    public T Value => _enum;

    public override string ToString()
    {
        return _enum.ToString();
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(this, obj)) return true;
        if (!(obj is EnumString<T> t)) return false;
        return EqualityComparer<T>.Default.Equals(_enum, t._enum);
    }

    public override int GetHashCode()
    {
        return EqualityComparer<T>.Default.GetHashCode(_enum);
    }

    // Add various implicit casts to make it easier to work with EnumString, e.g. user.DayOfWeek = DayOfWeek.Friday, if (user.DayOfWeek == DayOfWeek.Friday) etc.
    public static implicit operator string(EnumString<T> value) => value?.ToString();

    public static implicit operator EnumString<T>(string value) => string.IsNullOrEmpty(value) ? null : new EnumString<T>((T)Enum.Parse(typeof(T), value));

    public static implicit operator EnumString<T>(T value) => new EnumString<T>(value);

    public static implicit operator T(EnumString<T> e) => e is default(EnumString<T>) ? default(T) : e._enum;
}
