# NotAORM

**NotAORM** is a lightweight, non-ORM data access helper for .NET Framework 4.0 and above. It provides a simple, consistent API for executing SQL queries, mapping results to objects, and managing database connections without the overhead of a full ORM.

---

## Features

- ✅ **No ORM complexity** – just raw ADO.NET with helpers.
- ✅ **Connection management** – automatic open/close with pooling.
- ✅ **Configuration** – via `app.config` (connection strings).
- ✅ **Transaction support** – pass `SqlTransaction` to any method.
- ✅ **Property mapping** – automatic mapping with `snake_case` convention and `[Column]` attribute.
- ✅ **Caching** – property reflection is cached for performance.
- ✅ **Logging** – pluggable logger (`Action<string>`) with execution timing.
- ✅ **Batch execution** – run multiple commands in one go.
- ✅ **Compatible** – works with .NET Framework 4.0, 4.5, 4.6, 4.7, 4.8.

---

## Installation

### Option 1: Include the source file

Copy `NotAORMBase.cs` into your project. No NuGet package needed.

### Option 2: Add as a reference

Compile the class library and reference the `.dll` in your project.

### Requirements

- .NET Framework 4.0 or higher.
- Reference to `System.Configuration` (for `app.config` support).

---

## Quick Start

### 1. Define your entity

```csharp
public class Product
{
    // Maps to "product_id" (snake_case automatically)
    public int ProductId { get; set; }

    // Explicit mapping using [Column]
    [Column("product_name")]
    public string Name { get; set; }

    // Maps to "price"
    public decimal Price { get; set; }
}

### 2. Configure connection string (app.config)

```xml
<configuration>
  <connectionStrings>
    <add name="DefaultConnection" connectionString="Server=localhost;Database=MyDB;Integrated Security=true;" />
    <!-- Or use the entity type name: -->
    <add name="Product" connectionString="..." />
  </connectionStrings>
</configuration>
```

### 3. Create repository instance

```csharp
// Uses "DefaultConnection" or entity name (Product)
var repo = new NotAORMBase<Product>();

// Or specify a connection string name
var repo = new NotAORMBase<Product>("MyConnectionStringName");

// Or pass a direct connection string
var repo = new NotAORMBase<Product>("Server=...");
```

### 4. Execute queries

```csharp
// Get a list of products
List<Product> products = repo.GetList<Product>("SELECT * FROM Products WHERE Price > @minPrice",
    parameters: new List<SqlParameter> { new SqlParameter("@minPrice", 100) });

// Get a single product
Product product = repo.GetSingle<Product>("SELECT TOP 1 * FROM Products WHERE Id = @id",
    parameters: new List<SqlParameter> { new SqlParameter("@id", 1) },
    throwIfEmpty: true); // Throws if no rows

// Get a DataTable
DataTable table = repo.GetDataTable("SELECT * FROM Products");

// Execute non-query
int rowsAffected = repo.ExecuteNonQuery("UPDATE Products SET Price = Price * 1.1");

// Scalar
int count = Convert.ToInt32(repo.ExecuteScalar("SELECT COUNT(*) FROM Products"));

// Batch execution
var commands = new[]
{
    new NotAORMBase<Product>.BatchCommand
    {
        Query = "UPDATE Products SET Price = Price * 1.1 WHERE Id = 1",
        Type = CommandType.Text
    },
    new NotAORMBase<Product>.BatchCommand
    {
        Query = "UPDATE Products SET Price = Price * 0.9 WHERE Id = 2",
        Type = CommandType.Text
    }
};
int[] results = repo.ExecuteBatch(commands);
```

### 5. Transactions
```csharp
using (var conn = repo.Instance)
{
    conn.Open();
    using (var trans = conn.BeginTransaction())
    {
        try
        {
            repo.ExecuteNonQuery("UPDATE Products SET Stock = Stock - 10 WHERE Id = 1",
                transaction: trans);
            repo.ExecuteNonQuery("INSERT INTO Orders (ProductId, Quantity) VALUES (1, 10)",
                transaction: trans);
            trans.Commit();
        }
        catch
        {
            trans.Rollback();
            throw;
        }
    }
}
```

### 6. Logging
```csharp
// Enable logging
NotAORMBase<Product>.Logger = msg => Console.WriteLine($"[LOG] {msg}");

// All SQL operations will now be logged with timing
```

## API Reference

| Method | Description |
|--------|-------------|
| `GetList<TItem>(...)` | Returns a `List<TItem>` mapped from the query result. |
| `GetSingle<TItem>(...)` | Returns a single object (first row). |
| `GetDataTable(...)` | Returns a `DataTable`. |
| `GetDataSet(...)` | Returns a `DataSet`. |
| `ExecuteScalar(...)` | Returns the first column of the first row. |
| `ExecuteNonQuery(...)` | Executes INSERT/UPDATE/DELETE; returns affected rows. |
| `ExecuteBatch(...)` | Executes multiple commands in one batch. |

All methods accept optional `SqlTransaction` and `List<SqlParameter>` parameters.

---

## Migration from Obsolete Methods

| Obsolete Method | New Method |
|-----------------|------------|
| `Raw(Action<SqlDataReader>)` | Use `GetReader` (planned) or `GetList<T>` / `GetSingle<T>` if applicable. |
| `Raw<DataTable>(..., true)` | `GetDataTable(...)` |
| `Raw<DataSet>(..., false)` | `GetDataSet(...)` |
| `Raw<List<T>>(...)` | `GetList<T>(...)` |
| `Raw<T>(...)` (single) | `GetSingle<T>(...)` |
| `Execute(...)` | `ExecuteNonQuery(...)` |

---

## Roadmap (Next Versions)

- **1.1.0** – Custom exception, `GetReader`, output parameters, multiple result sets, pagination, logging levels.

---

## License

This library is provided as-is, free to use in commercial and open-source projects.

---

## Contributing

Feel free to fork and submit pull requests. For bug reports, please include a minimal reproducible example.

---

**Happy coding!**
