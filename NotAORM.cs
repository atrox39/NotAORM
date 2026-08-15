using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Collections.Concurrent;
using System.Configuration;
using System.Diagnostics;
using System.Text;

namespace NotAORM
{
    /// <summary>
    /// Allows custom mapping of a property to a specific database column name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; }
        public ColumnAttribute(string name) => Name = name;
    }

    /// <summary>
    /// (Obsolete) Specifies the database connection string via attribute.
    /// Use app.config connection strings instead.
    /// </summary>
    [Obsolete("Use app.config connection strings instead. Will be removed in future versions.")]
    [AttributeUsage(AttributeTargets.Class)]
    public class Database : Attribute
    {
        public string DbConnectionString { get; set; }
    }

    /// <summary>
    /// Base class for data access. Provides simple, non-ORM database operations with caching, logging, and transaction support.
    /// </summary>
    /// <typeparam name="T">The entity type used for convention-based connection string naming.</typeparam>
    public class NotAORMBase<T>
    {
        /// <summary>
        /// Static logger action. Assign to capture SQL execution logs, errors, and timing.
        /// </summary>
        public static Action<string> Logger { get; set; }

        private static readonly ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>> _propertyCache
            = new ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly SqlConnection _sqlConnection;

        /// <summary>
        /// (Obsolete) Gets the underlying SqlConnection instance. Use the Connection property instead.
        /// </summary>
        [Obsolete("This property is deprecated and will be removed in a future version. Use the 'Connection' property or the 'GetConnection()' method instead.")]
        public SqlConnection Instance => _sqlConnection;

        /// <summary>
        /// Gets the underlying SqlConnection instance.
        /// </summary>
        public SqlConnection Connection => _sqlConnection;

        /// <summary>
        /// Initializes a new instance using the [Database] attribute (obsolete).
        /// </summary>
        [Obsolete("Use NotAORMBase(string connectionStringNameOrString) instead. [Database] attribute is deprecated.")]
        public NotAORMBase()
        {
            string dbConnectionString = null;
            try
            {
                dbConnectionString = (Attribute.GetCustomAttributes(typeof(T))
                    .Where(val => val is Database).First() as Database).DbConnectionString;
                _sqlConnection = new SqlConnection(dbConnectionString);
#if DEBUG
                _sqlConnection.InfoMessage += (sender, e) => Console.WriteLine("DB-LOGGER: {0}", e.Message);
#endif
            }
            catch (Exception ex)
            {
                throw new Exception("NotAORMBase - Creating Instance of DatabaseConnection", ex);
            }
        }

        /// <summary>
        /// Initializes a new instance using a connection string from app.config or a direct connection string.
        /// </summary>
        /// <param name="connectionStringNameOrString">
        /// Either a name of a connection string in app.config, or a full connection string.
        /// If null, tries to use the entity type name or "DefaultConnection" as key.
        /// </param>
        public NotAORMBase(string connectionStringNameOrString = null)
        {
            string connectionString = null;
            if (!string.IsNullOrEmpty(connectionStringNameOrString))
            {
                if (connectionStringNameOrString.Contains(";"))
                {
                    connectionString = connectionStringNameOrString;
                }
                else
                {
                    var connSettings = ConfigurationManager.ConnectionStrings[connectionStringNameOrString];
                    if (connSettings != null)
                        connectionString = connSettings.ConnectionString;
                    else
                        throw new Exception($"Connection string '{connectionStringNameOrString}' not found in app.config");
                }
            }
            else
            {
                string defaultName = typeof(T).Name;
                var connSettings = ConfigurationManager.ConnectionStrings[defaultName]
                                   ?? ConfigurationManager.ConnectionStrings["DefaultConnection"];
                if (connSettings != null)
                {
                    connectionString = connSettings.ConnectionString;
                }
                else
                {
                    // Fallback to [Database] attribute (obsolete)
                    var dbAttr = Attribute.GetCustomAttributes(typeof(T))
                        .OfType<Database>().FirstOrDefault();
                    if (dbAttr != null && !string.IsNullOrEmpty(dbAttr.DbConnectionString))
                        connectionString = dbAttr.DbConnectionString;
                    else
                        throw new Exception("No connection string provided via app.config or [Database] attribute");
                }
            }

            if (string.IsNullOrEmpty(connectionString))
                throw new Exception("Connection string is empty or not configured.");
            _sqlConnection = new SqlConnection(connectionString);
#if DEBUG
            _sqlConnection.InfoMessage += (sender, e) => Console.WriteLine("DB-LOGGER: {0}", e.Message);
#endif
        }

        // ====================== Property Mapping with Convention ======================

        private static Dictionary<string, PropertyInfo> GetPropertyMap(Type type)
        {
            return _propertyCache.GetOrAdd(type, t =>
            {
                var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.CanWrite);
                var dict = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
                foreach (var prop in props)
                {
                    // 1. If [Column] attribute is present, use its name
                    var colAttr = Attribute.GetCustomAttribute(prop, typeof(ColumnAttribute)) as ColumnAttribute;
                    string columnName = colAttr?.Name;
                    // 2. Otherwise, apply convention: PascalCase -> snake_case
                    if (string.IsNullOrEmpty(columnName))
                        columnName = ConvertToSnakeCase(prop.Name);
                    dict[columnName] = prop;
                }
                return dict;
            });
        }

        private static string ConvertToSnakeCase(string name)
        {
            if (string.IsNullOrEmpty(name)) return name;
            var sb = new StringBuilder();
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                if (i > 0 && char.IsUpper(c))
                    sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            return sb.ToString();
        }

        // ====================== Logging Helper ======================

        private void Log(string message)
        {
            Logger?.Invoke(message);
        }

        // ====================== Internal Execution ======================

        private void ExecuteSql(Action<SqlDataAdapter> execute, string query,
            CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            bool wasClosed = (_sqlConnection.State == ConnectionState.Closed);
            bool ownsConnection = wasClosed && transaction == null;
            var sw = Stopwatch.StartNew();
            try
            {
                if (wasClosed) _sqlConnection.Open();
                using (SqlCommand cmd = _sqlConnection.CreateCommand())
                {
                    cmd.CommandType = type;
                    cmd.CommandText = query;
                    if (parameters != null) cmd.Parameters.AddRange(parameters.ToArray());
                    if (transaction != null) cmd.Transaction = transaction;
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        execute(adapter);
                    }
                    cmd.Parameters.Clear();
                }
                Log($"SQL executed in {sw.ElapsedMilliseconds}ms: {query}");
            }
            catch (Exception ex)
            {
                Log($"Error executing SQL: {query}. Error: {ex.Message}");
                throw;
            }
            finally
            {
                if (ownsConnection && _sqlConnection.State == ConnectionState.Open)
                    _sqlConnection.Close();
            }
        }

        // ====================== Obsolete Raw Methods ======================

        /// <summary>
        /// (Obsolete) Executes a query and provides a SqlDataReader to the caller.
        /// </summary>
        [Obsolete("Use ExecuteScalar, GetDataTable, GetDataSet, GetList<T>, or GetSingle<T> instead.")]
        public void Raw(Action<SqlDataReader> execute, string query,
            CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            bool wasClosed = (_sqlConnection.State == ConnectionState.Closed);
            bool ownsConnection = wasClosed && transaction == null;
            var sw = Stopwatch.StartNew();
            try
            {
                if (wasClosed) _sqlConnection.Open();
                using (SqlCommand cmd = _sqlConnection.CreateCommand())
                {
                    cmd.CommandType = type;
                    cmd.CommandText = query;
                    if (parameters != null) cmd.Parameters.AddRange(parameters.ToArray());
                    if (transaction != null) cmd.Transaction = transaction;
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        execute(reader);
                    }
                    cmd.Parameters.Clear();
                }
                Log($"SQL (Raw reader) executed in {sw.ElapsedMilliseconds}ms: {query}");
            }
            catch (Exception ex)
            {
                Log($"Error executing Raw reader: {query}. Error: {ex.Message}");
                throw new Exception("Raw - SqlDataReader", ex);
            }
            finally
            {
                if (ownsConnection && _sqlConnection.State == ConnectionState.Open)
                    _sqlConnection.Close();
            }
        }

        /// <summary>
        /// (Obsolete) Returns a DataTable or DataSet.
        /// </summary>
        [Obsolete("Use GetDataTable or GetDataSet instead.")]
        public TA Raw<TA>(string query, bool isDataTable,
            CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            DataSet ds = new DataSet();
            try
            {
                ExecuteSql((adapter) =>
                {
                    if (isDataTable) adapter.Fill(dt);
                    else adapter.Fill(ds);
                }, query, type, parameters, transaction);
            }
            catch (Exception ex)
            {
                string msg = isDataTable ? "DataTable" : "DataSet";
                throw new Exception($"Raw - {msg}", ex);
            }
            return isDataTable ? (TA)Convert.ChangeType(dt, typeof(TA)) : (TA)Convert.ChangeType(ds, typeof(TA));
        }

        /// <summary>
        /// (Obsolete) Maps the result to a single object or list of objects.
        /// </summary>
        [Obsolete("Use GetList<T> or GetSingle<T> instead.")]
        public TA Raw<TA>(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            bool isList = typeof(TA).IsGenericType && typeof(TA).GetGenericTypeDefinition() == typeof(List<>);
            Type itemType = isList ? typeof(TA).GetGenericArguments()[0] : typeof(TA);

            var propMap = GetPropertyMap(itemType);

            IList list = isList ? (IList)Activator.CreateInstance(typeof(TA)) : null;
            TA result = isList ? default(TA) : Activator.CreateInstance<TA>();

            try
            {
                ExecuteSql((adapter) =>
                {
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);
                    if (isList)
                    {
                        foreach (DataRow row in dt.Rows)
                        {
                            object item = Activator.CreateInstance(itemType);
                            foreach (DataColumn col in dt.Columns)
                            {
                                AddValue(item, col.ColumnName, row[col.ColumnName], propMap);
                            }
                            list.Add(item);
                        }
                    }
                    else
                    {
                        if (dt.Rows.Count > 0)
                        {
                            DataRow row = dt.Rows[0];
                            foreach (DataColumn col in dt.Columns)
                            {
                                AddValue(result, col.ColumnName, row[col.ColumnName], propMap);
                            }
                        }
                    }
                }, query, type, parameters, transaction);
            }
            catch (Exception ex)
            {
                throw new Exception("An error occurred while executing Raw<TA>", ex);
            }

            return isList ? (TA)list : result;
        }

        /// <summary>
        /// (Obsolete) Executes a non-query command.
        /// </summary>
        [Obsolete("Use ExecuteNonQuery instead.")]
        public int Execute(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            return ExecuteNonQuery(query, type, parameters, transaction);
        }

        // ====================== New Public API ======================

        /// <summary>
        /// Returns a list of objects of type TItem.
        /// </summary>
        public List<TItem> GetList<TItem>(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            return Raw<List<TItem>>(query, type, parameters, transaction);
        }

        /// <summary>
        /// Returns a single object of type TItem (first row). If throwIfEmpty is true, throws when no rows are returned.
        /// </summary>
        public TItem GetSingle<TItem>(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null,
            bool throwIfEmpty = false)
        {
            // For empty detection, we use a separate COUNT query to avoid ambiguity with default values.
            if (throwIfEmpty)
            {
                // Use a COUNT subquery to check existence efficiently.
                long count = Convert.ToInt64(ExecuteScalar($"SELECT COUNT(*) FROM ({query}) AS T", type, parameters, transaction));
                if (count == 0)
                    throw new InvalidOperationException("No rows returned.");
            }
            return Raw<TItem>(query, type, parameters, transaction);
        }

        /// <summary>
        /// Returns a DataTable.
        /// </summary>
        public DataTable GetDataTable(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            return Raw<DataTable>(query, true, type, parameters, transaction);
        }

        /// <summary>
        /// Returns a DataSet.
        /// </summary>
        public DataSet GetDataSet(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            return Raw<DataSet>(query, false, type, parameters, transaction);
        }

        /// <summary>
        /// Executes a scalar query and returns the first column of the first row. Converts DBNull to null.
        /// </summary>
        public object ExecuteScalar(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            object result = null;
            bool wasClosed = (_sqlConnection.State == ConnectionState.Closed);
            bool ownsConnection = wasClosed && transaction == null;
            var sw = Stopwatch.StartNew();
            try
            {
                if (wasClosed) _sqlConnection.Open();
                using (SqlCommand cmd = _sqlConnection.CreateCommand())
                {
                    cmd.CommandType = type;
                    cmd.CommandText = query;
                    if (parameters != null) cmd.Parameters.AddRange(parameters.ToArray());
                    if (transaction != null) cmd.Transaction = transaction;
                    result = cmd.ExecuteScalar();
                    if (result == DBNull.Value) result = null;
                    cmd.Parameters.Clear();
                }
                Log($"ExecuteScalar executed in {sw.ElapsedMilliseconds}ms: {query}");
            }
            catch (Exception ex)
            {
                Log($"Error in ExecuteScalar: {query}. Error: {ex.Message}");
                throw new Exception("ExecuteScalar failed", ex);
            }
            finally
            {
                if (ownsConnection && _sqlConnection.State == ConnectionState.Open)
                    _sqlConnection.Close();
            }
            return result;
        }

        /// <summary>
        /// Executes a non-query command (INSERT, UPDATE, DELETE) and returns the number of affected rows.
        /// </summary>
        public int ExecuteNonQuery(string query, CommandType type = CommandType.Text,
            List<SqlParameter> parameters = null,
            SqlTransaction transaction = null)
        {
            int result = 0;
            bool wasClosed = (_sqlConnection.State == ConnectionState.Closed);
            bool ownsConnection = wasClosed && transaction == null;
            var sw = Stopwatch.StartNew();
            try
            {
                if (wasClosed) _sqlConnection.Open();
                using (SqlCommand cmd = _sqlConnection.CreateCommand())
                {
                    cmd.CommandType = type;
                    cmd.CommandText = query;
                    if (parameters != null) cmd.Parameters.AddRange(parameters.ToArray());
                    if (transaction != null) cmd.Transaction = transaction;
                    result = cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }
                Log($"ExecuteNonQuery executed in {sw.ElapsedMilliseconds}ms: {query}");
            }
            catch (Exception ex)
            {
                Log($"Error in ExecuteNonQuery: {query}. Error: {ex.Message}");
                throw new Exception("ExecuteNonQuery failed", ex);
            }
            finally
            {
                if (ownsConnection && _sqlConnection.State == ConnectionState.Open)
                    _sqlConnection.Close();
            }
            return result;
        }

        // ====================== Batch Execution ======================

        /// <summary>
        /// Represents a command to be executed in a batch.
        /// </summary>
        public class BatchCommand
        {
            public string Query { get; set; }
            public CommandType Type { get; set; }
            public List<SqlParameter> Parameters { get; set; }
        }

        /// <summary>
        /// Executes multiple commands in a single batch. If a transaction is provided, all commands are enlisted.
        /// </summary>
        /// <param name="commands">A list of BatchCommand objects.</param>
        /// <param name="transaction">Optional transaction to enlist all commands.</param>
        /// <returns>An array of the number of rows affected by each command.</returns>
        public int[] ExecuteBatch(IEnumerable<BatchCommand> commands,
            SqlTransaction transaction = null)
        {
            var results = new List<int>();
            bool wasClosed = (_sqlConnection.State == ConnectionState.Closed);
            bool ownsConnection = wasClosed && transaction == null;
            var sw = Stopwatch.StartNew();
            try
            {
                if (wasClosed) _sqlConnection.Open();
                using (SqlCommand cmd = _sqlConnection.CreateCommand())
                {
                    if (transaction != null) cmd.Transaction = transaction;
                    foreach (var command in commands)
                    {
                        cmd.CommandText = command.Query;
                        cmd.CommandType = command.Type;
                        cmd.Parameters.Clear();
                        if (command.Parameters != null) cmd.Parameters.AddRange(command.Parameters.ToArray());
                        results.Add(cmd.ExecuteNonQuery());
                    }
                }
                Log($"Batch executed with {results.Count} commands in {sw.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                Log($"Error in ExecuteBatch: {ex.Message}");
                throw;
            }
            finally
            {
                if (ownsConnection && _sqlConnection.State == ConnectionState.Open)
                    _sqlConnection.Close();
            }
            return results.ToArray();
        }

        // ====================== Mapping Helper ======================

        private void AddValue<TA>(TA target, string propertyName, object value, Dictionary<string, PropertyInfo> propMap)
        {
            if (propMap.TryGetValue(propertyName, out PropertyInfo prop))
            {
                if (value == DBNull.Value)
                {
                    Type propType = prop.PropertyType;
                    object defaultValue = propType.IsValueType ? Activator.CreateInstance(propType) : null;
                    prop.SetValue(target, defaultValue, null);
                }
                else
                {
                    try
                    {
                        object converted = Convert.ChangeType(value, prop.PropertyType);
                        prop.SetValue(target, converted, null);
                    }
                    catch
                    {
                        throw new InvalidCastException(
                            $"Cannot convert '{value}' (type {value.GetType()}) to {prop.PropertyType} on property {propertyName}");
                    }
                }
            }
        }
    }
}
