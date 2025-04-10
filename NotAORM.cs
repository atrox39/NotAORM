using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace NotAORM
{
  [AttributeUsage(AttributeTargets.Class)]
  public class Database : Attribute
  {
    public string DbConnectionString { get; set; }
  }

  public class NotAORMBase<T>
  {
    private readonly SqlConnection _sqlConnection;

    public SqlConnection Intance
    {
      get
      {
        return _sqlConnection;
      }
    }

    public NotAORMBase()
    {
      string dbConnectionString = null;
      try
      {
        dbConnectionString = (Attribute.GetCustomAttributes(typeof(T)).Where((val) => (val as Database) != null).First() as Database).DbConnectionString;
        _sqlConnection = new SqlConnection(dbConnectionString);
#if DEBUG
        _sqlConnection.InfoMessage += delegate (object sender, SqlInfoMessageEventArgs e)
        {
          Console.WriteLine("DB-LOGGER: {0}", e.Message);
        };
#endif
      }
      catch (Exception ex)
      {
        throw new Exception("NotAORMBase - Creating Intance of DatabaseConnection", ex);
      }
    }

    private void CloseOnCommandExecution(SqlConnection conn)
    {
      if (conn.State == ConnectionState.Open)
      {
        conn.Close();
        Thread.Sleep(10);
        conn.Open();
      }
      else if (conn.State == ConnectionState.Closed)
      {
        conn.Open();
      }
    }

    private void ExecuteSql(Action<SqlDataAdapter> Execute, string query, CommandType type = CommandType.Text, List<SqlParameter> parameters = null)
    {
      bool active = false;
      try
      {
        CloseOnCommandExecution(_sqlConnection);
        active = true;
        using (SqlCommand cmd = _sqlConnection.CreateCommand())
        {
          cmd.CommandType = type;
          cmd.CommandText = query;
          if (parameters != null)
          {
            cmd.Parameters.AddRange(parameters.ToArray());
          }
          SqlDataAdapter adapter = new SqlDataAdapter(cmd);
          Execute(adapter);
          cmd.Parameters.Clear();
        }
      }
      catch (Exception ex)
      {
        throw ex;
      }
      finally
      {
        if (active) _sqlConnection.Close();
      }
    }

    private PropertyInfo GetPropInfo<TA>(TA t, string name)
    {
      return t.GetType().GetProperty(name);
    }

    private void AddValue<TA>(TA t, string Name, dynamic Value, Type TypeOf)
    {
      var prop = GetPropInfo(t, Name);
      if (prop != null && prop.CanWrite)
      {
        prop.SetValue(t, TypeOf.IsInstanceOfType(DBNull.Value) ? null : Value, null);
      }
    }

    public void Raw(Action<SqlDataReader> Execute, string Query, CommandType type = CommandType.Text, List<SqlParameter> parameters = null)
    {
      bool active = false;
      try
      {
        CloseOnCommandExecution(_sqlConnection);
        active = true;
        using (SqlCommand cmd = _sqlConnection.CreateCommand())
        {
          cmd.CommandType = type;
          cmd.CommandText = Query;
          if (parameters != null)
          {
            cmd.Parameters.AddRange(parameters.ToArray());
          }
          SqlDataReader reader = cmd.ExecuteReader();
          Execute(reader);
          reader.Close();
          cmd.Parameters.Clear();
        }
      }
      catch (Exception ex)
      {
        throw new Exception("Raw - SqlDataReader", ex);
      }
      finally
      {
        if (active) _sqlConnection.Close();
      }
    }

    public TA Raw<TA>(string Query, bool isDataTable, CommandType type = CommandType.Text, List<SqlParameter> parameters = null)
    {
      DataTable dt = new DataTable();
      DataSet ds = new DataSet();

      try
      {
        ExecuteSql((adapter) =>
        {
          if (isDataTable) adapter.Fill(dt);
          else adapter.Fill(ds);
        }, Query, type, parameters);
      }
      catch (Exception ex)
      {
        string msg = isDataTable ? "DataTable" : "DataSet";
        throw new Exception($"Raw - {msg}", ex);
      }
      return isDataTable ? (TA) Convert.ChangeType(dt, typeof(TA)) : (TA) Convert.ChangeType(ds, typeof(TA));
    }

    public TA Raw<TA>(string Query, CommandType type = CommandType.Text, List<SqlParameter> parameters = null)
    {
      TA result = Activator.CreateInstance<TA>();
      bool isList = typeof(TA).IsGenericType && typeof(TA).GetGenericTypeDefinition() == typeof(List<>);
      IList list = isList ? (IList)Activator.CreateInstance(typeof(TA)) : null;
      try
      {
        ExecuteSql((adapter) =>
        {
          DataTable dt = new DataTable();
          adapter.Fill(dt);

          foreach (DataRow row in dt.Rows)
          {
            if (isList)
            {
              Type itemType = typeof(TA).GetGenericArguments()[0];
              object item = Activator.CreateInstance(itemType);

              foreach (DataColumn col in dt.Columns)
              {
                AddValue(item, col.ColumnName, row[col.ColumnName], row[col.ColumnName].GetType());
              }
              list.Add(item);
            }
            else
            {
              foreach (DataColumn col in dt.Columns)
              {
                AddValue(result, col.ColumnName, row[col.ColumnName], row[col.ColumnName].GetType());
              }
            }
          }
        }, Query, type, parameters);
      }
      catch (Exception ex)
      {
        throw new Exception("An error occurred while executing Raw<TA>", ex);
      }
      return isList ? (TA)list : result;
    }

    public int Execute(string Query, CommandType type = CommandType.Text, List<SqlParameter> parameters = null)
    {
      bool active = false;
      int result = 0;
      try
      {
        CloseOnCommandExecution(_sqlConnection);
        active = true;
        using (SqlCommand cmd = _sqlConnection.CreateCommand())
        {
          cmd.CommandType = type;
          cmd.CommandText = Query;
          if (parameters != null)
          {
            cmd.Parameters.AddRange(parameters.ToArray());
          }
          result = cmd.ExecuteNonQuery();
          cmd.Parameters.Clear();
        }
      }
      catch (Exception ex)
      {
        throw new Exception("Raw - SqlDataReader", ex);
      }
      finally
      {
        if (active) _sqlConnection.Close();
      }
      return result;
    }
  }
}
