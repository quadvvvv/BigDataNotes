## Section 4: Spark SQL, DataFrames, and Datasets

### Introduction to Spark SQL

Spark SQL is a module in Apache Spark that integrates relational processing with Spark's functional programming API. It became a prominent feature starting with Spark 2.x, offering several advantages over traditional RDDs:

- Improved performance through advanced optimizations
- Seamless integration with various data sources
- Enhanced support for structured and semi-structured data

### DataFrames: The Foundation of Spark SQL

DataFrames are distributed collections of data organized into named columns. They are conceptually equivalent to tables in a relational database or data frames in R/Python.

#### Key Features of DataFrames

- **Schema-defined**: Each DataFrame has a defined structure.
- **Optimized**: Spark SQL's Catalyst optimizer can automatically optimize operations on DataFrames.
- **Language Support**: Available in Scala, Java, Python, and R.

#### Working with DataFrames

Here's a basic example of creating and using a DataFrame in Scala:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("DataFrame Example").getOrCreate()

// Create a DataFrame from a CSV file
val df = spark.read.format("csv")
  .option("header", "true")
  .load("path/to/your/file.csv")

// Show the first few rows
df.show()

// Select specific columns
df.select("name", "age").show()

// Filter rows
df.filter($"age" > 30).show()

// Group by and aggregate
df.groupBy("department").agg(avg("salary").alias("avg_salary")).show()
```

### Datasets: Type-Safe DataFrames

Datasets are an extension of DataFrames that provide type-safety for strongly typed languages like Scala and Java.

#### Key Features of Datasets

- **Type-safe**: Errors are caught at compile time.
- **Object-oriented**: Each row in a Dataset is an object of a user-defined class.
- **Performance**: Offer the benefits of DataFrames with additional compile-time optimizations.

#### Working with Datasets

Here's an example of using Datasets in Scala:

```scala
case class Person(name: String, age: Int)

val peopleDS = spark.read
  .format("json")
  .load("people.json")
  .as[Person]

// Now we can use type-safe operations
peopleDS.filter(_.age > 30).show()
```

### Transitioning from RDDs to DataFrames/Datasets

While RDDs are still supported, there's a strong trend towards using DataFrames and Datasets due to:

1. **Better Performance**: The Catalyst optimizer can make better decisions with the structured data in DataFrames/Datasets.
2. **Ease of Use**: Higher-level APIs are more intuitive for most data processing tasks.
3. **Interoperability**: Better integration with other Spark components and external tools.

To convert between RDDs and DataFrames/Datasets:

```scala
// RDD to DataFrame
val rdd = spark.sparkContext.parallelize(Seq((1, "Alice"), (2, "Bob")))
val df = rdd.toDF("id", "name")

// DataFrame to RDD
val rdd = df.rdd

// RDD to Dataset (requires a case class)
case class Person(id: Int, name: String)
val ds = spark.createDataset(rdd.map { case (id, name) => Person(id, name) })
```

### Spark SQL Operations

Spark SQL allows you to query structured data using SQL or the DataFrame API.

#### SQL Queries

```scala
// Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

// Run SQL query
val results = spark.sql("SELECT name, age FROM people WHERE age > 30")
results.show()
```

#### DataFrame API

```scala
import org.apache.spark.sql.functions._

// Equivalent operation using DataFrame API
val results = df.select("name", "age").filter($"age" > 30)
results.show()
```

### User-Defined Functions (UDFs)

UDFs allow you to extend the capabilities of Spark SQL by defining custom operations.

```scala
import org.apache.spark.sql.functions.udf

// Define a UDF
val upperCase = udf((s: String) => s.toUpperCase)

// Use the UDF in a DataFrame operation
df.withColumn("upper_name", upperCase($"name")).show()
```

### Best Practices and Performance Tips

1. **Use DataFrames/Datasets over RDDs** when possible for better performance.
2. **Leverage Catalyst Optimizer** by using DataFrame/Dataset operations instead of UDFs when possible.
3. **Partition your data** appropriately for distributed processing.
4. **Cache frequently used DataFrames/Datasets** to improve query performance.
5. **Use appropriate data formats** like Parquet for better read/write performance.

### Conclusion

Spark SQL, with its DataFrame and Dataset APIs, provides a powerful and efficient way to process structured data in Spark. By understanding and leveraging these APIs, you can write more concise, performant, and maintainable Spark applications.

### Deep Dive: DataFrames vs Datasets

#### Relationship between DataFrames and Datasets

DataFrames and Datasets are closely related concepts in Spark SQL:

1. **DataFrame is a Dataset**: Technically, a DataFrame is a type alias for Dataset[Row]. This means a DataFrame is actually a Dataset of Row objects.

2. **Untyped vs Typed**: DataFrames are considered "untyped" because they don't have type information at compile time, while Datasets are "typed".

3. **API Consistency**: Both share a common API for most operations, making it easy to switch between them.

#### Comparison of DataFrames and Datasets

| Feature | DataFrame | Dataset |
|---------|-----------|---------|
| Type Safety | Runtime type-checking | Compile-time type-checking |
| Supported Languages | Scala, Java, Python, R | Scala, Java (Not available in Python or R) |
| Ease of Use | Easier for ad-hoc analysis | Requires defining case classes for custom types |
| Performance | Good performance due to Catalyst optimizer | Potentially better performance for complex processing due to compile-time optimizations |
| Schema | Inferred at runtime | Known at compile time |
| API | High-level DSL and SQL | High-level DSL, SQL, and ability to use lambda functions |

### Popularity and Use Scenarios

#### DataFrames

**Popularity**: DataFrames are generally more popular, especially among data scientists and analysts.

**Use Scenarios**:
1. **Ad-hoc Analysis**: When you need to quickly explore and analyze data without defining strict schemas.
2. **SQL-like Operations**: For users comfortable with SQL who want to perform operations on structured data.
3. **Interoperability**: When working with various data sources and sinks that don't require strict typing.
4. **Python and R Users**: As Datasets are not available in these languages, DataFrames are the go-to option.

Example use case:
```python
# Python example using DataFrame for quick data analysis
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
top_products = df.groupBy("product_name").agg(sum("sales").alias("total_sales")).orderBy(desc("total_sales")).limit(10)
top_products.show()
```

#### Datasets

**Popularity**: Datasets are popular in production environments where type safety is crucial, particularly in Scala and Java codebases.

**Use Scenarios**:
1. **Complex ETL Jobs**: When you need to ensure type safety for complex data processing pipelines.
2. **Domain Object Manipulation**: When working with domain-specific objects and you want to leverage compile-time type checking.
3. **Performance-Critical Applications**: For applications where you need the best possible performance and can benefit from compile-time optimizations.
4. **Large-Scale Data Pipelines**: In production environments where catching errors at compile-time is crucial for reliability.

Example use case:
```scala
// Scala example using Dataset for type-safe data processing
case class SalesRecord(id: Int, product: String, sales: Double)

val salesDS: Dataset[SalesRecord] = spark.read
  .schema(Encoders.product[SalesRecord].schema)
  .csv("sales_data.csv")
  .as[SalesRecord]

val topProducts = salesDS
  .groupByKey(_.product)
  .agg(sum($"sales").as[Double])
  .map { case (product, totalSales) => (product, totalSales) }
  .orderBy($"_2".desc)
  .limit(10)

topProducts.show()
```

### When to Choose DataFrames vs Datasets

- Choose DataFrames when:
  1. You're working in Python or R
  2. You need quick, iterative data exploration
  3. Your data doesn't have a fixed schema
  4. You're comfortable with SQL-like operations

- Choose Datasets when:
  1. You're working in Scala or Java
  2. Type safety is a priority
  3. You're building complex, large-scale data pipelines
  4. You want to work with domain-specific objects
  5. You need the absolute best performance and can benefit from compile-time optimizations

### Conclusion

Both DataFrames and Datasets have their place in the Spark ecosystem. DataFrames offer flexibility and ease of use, making them popular for data analysis and exploration. Datasets provide strong typing and better performance for complex operations, making them ideal for large-scale, production-grade data processing applications. The choice between them often depends on the specific requirements of your project, the programming language you're using, and the balance you need between development speed and type safety.

### Spark Code Comparison and Examples with Datasets

#### Examples:

### 1. SparkSQLDataset.scala - Using SQL Commands

This example demonstrates how to use SQL commands with Datasets in Spark:

```scala
import org.apache.spark.sql.SparkSession

object SparkSQLDataset {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQL").getOrCreate()
    import spark.implicits._

    val people = spark.read.json("people.json").as[Person]
    people.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    teenagersDF.show()

    spark.stop()
  }
}
```

### 2. DataFramesDataset.scala - Using SQL-like Functions / API

This example shows how to use SQL-like functions and API with Datasets:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFramesDataset {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFramesDataset").getOrCreate()
    import spark.implicits._

    val people = spark.read.json("people.json").as[Person]

    val teenagersDF = people.filter($"age" >= 13 && $"age" <= 19).select("name")
    teenagersDF.show()

    spark.stop()
  }
}
```

### Exercise:

#### FriendsByAgeExample.scala - Using Datasets

Convert the following RDD-based example to use Datasets:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAgeDataset {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FriendsByAgeDataset").getOrCreate()
    import spark.implicits._

    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("fakefriends.csv")
      .as[Person]

    val friendsByAge = people
      .groupBy("age")
      .agg(round(avg("friends"), 2).alias("avg_friends"))
      .sort("age")

    friendsByAge.show()

    spark.stop()
  }
}
```

### Examples:

#### 1. Word Count Example Using Datasets

#### WordCountDataset.scala

This example demonstrates how to perform a word count using Datasets:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDataset {
  case class Word(value: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCountDataset").getOrCreate()
    import spark.implicits._

    val input = spark.read.text("book.txt").as[String]
    val words = input.flatMap(_.split("\\W+")).map(Word)

    val wordCounts = words.groupBy("value").count()
    wordCounts.show()

    spark.stop()
  }
}
```

Note:
- Datasets work best with structured data
- In this case, we're working with a dataset of rows (strings)
- Sometimes, you might load data as an RDD and convert to a Dataset later on

#### WordCountBetterSortedDataset.scala

This example builds on the previous one, adding better sorting:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountBetterSortedDataset {
  case class Word(value: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCountBetterSortedDataset").getOrCreate()
    import spark.implicits._

    val input = spark.read.text("book.txt").as[String]
    val words = input.flatMap(_.split("\\W+")).map(Word)

    val wordCounts = words.groupBy("value").count().sort(desc("count"))
    wordCounts.show()

    spark.stop()
  }
}
```

#### 2. Min Temperature Example with Datasets

This example shows how to find minimum temperatures using Datasets:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MinTemperatureDataset {
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MinTemperatureDataset").getOrCreate()
    import spark.implicits._

    // Providing a schema to SparkSession.read()
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    val temperatures = spark.read
      .schema(temperatureSchema)
      .csv("temperature_data.csv")
      .as[Temperature]

    val minTemps = temperatures
      .filter($"measure_type" === "TMIN")
      .groupBy("stationID")
      .agg(min("temperature").alias("min_temperature"))
      // Using withColumn() to create a new column with the given columns
      .withColumn("min_temperature_celsius", ($"min_temperature" / 10) * (9.0 / 5.0) + 32)

    minTemps.show()

    spark.stop()
  }
}
```

These examples demonstrate various ways to use Datasets in Spark, including:
- Using SQL commands and SQL-like API
- Converting RDD-based code to use Datasets
- Working with unstructured data (word count)
- Providing schemas and using `withColumn()` for data transformation

Remember that Datasets provide type-safety and are generally preferred in modern Spark applications when working with structured or semi-structured data.

### Exercise: Spark Code Comparison: DS vs CustomerDS Approach

Here's the code comparison with added **tnotes** in raw markdown, along with **pros and cons** for each version.

### First Version (`ds` approach)

```scala
import spark.implicits._

val ds = spark.read
  .schema(orderSchema)
  .csv("data/customer-orders.csv")
  .as[Order]

val orderGroupById = ds.groupBy("custID").sum("amount")

val finalOrder = orderGroupById
  .withColumn("totalAmount", round($"sum(amount)", 2))
  // tnote: Adding a new column "totalAmount" after rounding off "sum(amount)".
  .select("custId", "totalAmount")
  // tnote: Selecting the columns we need: "custId" and the newly created "totalAmount".
  .sort("totalAmount")
  // tnote: Sorting by "totalAmount" to get ordered results.
```

### Pros:
1. **Clear Step-by-Step Logic**: Breaks down each stage, making it easier to follow.
2. **Flexible**: The use of `withColumn` allows for additional transformations before selecting.

### Cons:
1. **Less Efficient**: Using `.withColumn` for transformations introduces an extra step that could be avoided.
2. **Verbosity**: The extra step could make the code harder to read and maintain when handling large transformations.
3. **Potential Confusion**: Column renaming happens later, making it less clear which columns are being processed upfront.

### Second Version (`customerDS` approach)

```scala
val customerDS = spark.read
  .schema(customerOrdersSchema)
  .csv("data/customer-orders.csv")
  .as[CustomerOrders]

val totalByCustomer = customerDS
  .groupBy("cust_id")
  .agg(round(sum("amount_spent"), 2)
  // tnote: Summing "amount_spent" and rounding it directly within aggregation.
  .alias("total_spent"))
  // tnote: Giving the rounded sum a new alias "total_spent".

val totalByCustomerSorted = totalByCustomer.sort("total_spent")
// tnote: Sorting by "total_spent" to get ordered results.
```

### Pros:
1. **Concise**: Combines the summing, rounding, and aliasing in a single step with `.agg`, reducing the number of transformations.
2. **Efficient**: Avoids the need for `.withColumn`, simplifying the code and avoiding unnecessary steps.
3. **Readable**: Clear column aliasing within the same operation, making it easier to understand the intent.

### Cons:
1. **Slightly less flexible**: If more complex transformations are needed, additional steps may still need to be added.

### Overall Summary
* **First Version**: Easier to follow for beginners due to the step-by-step breakdown, but less efficient with the use of `.withColumn`.
* **Second Version**: More concise and efficient by combining transformations in the aggregation step, better for clean and maintainable code.

### Quiz notes:

1. What is the relationship between datasets and dataframes?
   - Answer: Datasets and DataFrames are closely related in Spark. A DataFrame is actually a Dataset of Row objects. In Scala, DataFrame is an alias for Dataset[Row]. Datasets provide type-safety at compile time, while DataFrames are more flexible but less type-safe.

2. Which API is modern Spark with Scala converging upon?
   - Answer: Modern Spark with Scala is converging upon the Dataset API. This API combines the benefits of RDD (strong typing and powerful lambdas) with the benefits of Spark SQL's optimized execution engine.

3. Which object must be first created by your script to use datasets and SparkSQL?
   - Answer: To use datasets and SparkSQL, you must first create a SparkSession object. This is the entry point for all functionality in Spark 2.0+. It subsumes the previous SQLContext and HiveContext.

4. What is a case where you might use an RDD instead of a dataset?
   - Answer: While datasets are preferred in most cases, you might use an RDD in the following scenarios:
     - When you need fine-grained control over physical distribution of data for performance optimization.
     - When working with unstructured data that doesn't fit well into columns.
     - When using third-party libraries that require RDDs.
     - When performing low-level transformations and actions that aren't available in the Dataset API.

5. What does the explode() function do on a dataset?
   - Answer: The explode() function in Spark is used to transform a column that contains an array or map into multiple rows, creating a new row for each element in the array or each key-value pair in the map. This is useful when you want to "flatten" nested structures in your dataset.

These answers provide a good starting point for understanding some key concepts in Spark. Remember that Spark is a rapidly evolving technology, so it's always a good idea to refer to the latest official documentation for the most up-to-date information.

