## Section 3 

### Introduction to RDD

**RDD** - Resilient Distributed Dataset

The original API for Spark.

RDDs are divided into partitions, and these partitions can be distributed across multiple machines.

Spark ensures RDDs are resilient, meaning that if a node goes down, Spark can recover from failures and rebuild the lost data.

### Spark Context

The **SparkContext** (`sc`) is automatically created by your driver program.

It is responsible for managing the creation, distribution, and resilience of RDDs.

#### How to Load Data into Spark RDD?

- `sc.textFile(local_file_path)` to load local files.
- `s3n://` for Amazon S3.
- `hdfs://` for Hadoop HDFS.

You can also create RDDs from distributed services like:
- JDBC, Cassandra, HBase, ElasticSearch.
- File formats like JSON, CSV, etc.

### Transform RDDs

**Transformations** create a new RDD from an existing one without executing immediately.

- **Map**: One-to-one mapping between elements (e.g., transforming one row to another).
- **FlatMap**: One-to-many mapping (e.g., splitting one row into many).
- **Filter**: Evaluates each row and retains only those that return `true`.
- **Distinct**: Removes duplicates.
- **Sample**: Returns a random sample of the RDD.

Other transformations include:
- **Union**, **Intersection**, **Subtract**, **Cartesian**.

#### Example of `map`:

```scala
val rdd = sc.parallelize(List(1, 2, 3, 4)) // This RDD can be distributed across multiple nodes.
val squares = rdd.map(x => x * x) // Functional programming with an anonymous function.

This can also be written as: 

```scala
def squareIt(x: Int): Int = {
  x * x
}

rdd.map(squareIt)
```

Scala is designed to use this shorthand syntax, and it's generally recommended.

### RDD Actions

Actions trigger the execution of RDD transformations and return results to the driver program.

Some common actions include:

- **collect**: Retrieve the entire RDD to the driver, which is not normally used.
- **count**: Count the number of elements.
- **countByValue**: Count occurrences of each value.
- **take**: Take the first `n` elements.
- **top**: Take the top `n` elements.
- **reduce**: Combine the elements of the RDD using a function.

### Lazy Evaluation

Spark uses **lazy evaluation**, meaning that transformations on RDDs are not executed immediately but are instead recorded as a lineage of operations. The actual execution occurs only when an **action** is called.

- Once an action is invoked, Spark optimizes the job and constructs a Directed Acyclic Graph (DAG) to efficiently process the data.

#### Pro:

- **Optimization**: By deferring execution until an action is triggered, Spark can optimize the entire data flow, reducing unnecessary computations and improving performance.

#### Cons:

- **Debugging Difficulty**: Since nothing happens until an action is called, it can be challenging to trace issues in the transformation steps during development.
- **Memory Usage**: If not managed properly, the accumulation of transformations can lead to high memory usage, as Spark keeps track of the entire transformation lineage.

### Rating Histogram Example

In this example, we'll build a Spark application to count movie ratings from a dataset and plot a histogram of the results.

### Step 1: Set Up the Environment

First, check out the `RatingsCounter.scala` file, which contains the following code.

Start by importing the necessary libraries:

```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
```

### Step 2: Create a Spark Context

In this step, we set up the **SparkContext**, which is the entry point to interact with Spark. The `SparkContext` object is essential because it allows your program to connect to a Spark cluster and run parallel tasks.

#### Code:

```scala
val sc = new SparkContext("local[*]", "RatingCounter")
```

#### Explanation:

- local[*]: This tells Spark to run in local mode, which means the job will run on your local machine. The [*] means it will use all available CPU cores to parallelize the job. This is helpful for development and testing purposes.

- "RatingCounter": This is the name of the Spark application, which appears in the Spark UI or logs when monitoring the job's execution.

### Step 3: Load the Data

In this step, we load the dataset containing movie ratings using the `textFile` method, which reads data from a specified path.

#### Code:

```scala
val lines = sc.textFile("path_to_data")
```

#### Explanation:

- sc.textFile("path_to_data"): This method loads data from a text file into an RDD (Resilient Distributed Dataset). The path_to_data should be replaced with the actual file path, either local or from distributed storage like HDFS, S3, etc.

Each line in the file is treated as a string in the RDD, and this RDD (lines) will be processed in the next steps.

#### Dataset Format:

The dataset contains movie rating data, formatted as follows:

```sql
userId    movieId    rating    timestamp
196       242        3         881250949
```

Each field is separated by a tab (\t). The third field (rating) is what we are interested in for the histogram.

### Step 4: Extract the Rating Field

In this step, we use the `map` transformation to extract the third field (the rating) from each line of the dataset.

#### Code:

```scala
val ratings = lines.map(x => x.toString().split("\t")(2))
```

#### Explanation:

- **`lines.map(x => ...)`**: The `map` transformation applies a function to each element (line) in the RDD. It processes each line to extract the desired field.

- **`x.toString().split("\t")(2)`**: 
  - `x.toString()`: Ensures each element is treated as a string.
  - `split("\t")`: Splits the string based on the tab (`\t`) delimiter, creating an array.
  - `(2)`: Accesses the third element (index 2) in the array, which corresponds to the **rating**.

This results in an RDD (`ratings`) that contains only the rating values from the original dataset.

### Step 5: Count the Ratings

In this step, we count how many times each rating appears in the dataset using the `countByValue()` action.

#### Code:

```scala
val results = ratings.countByValue()
```
#### Explanation:

- **`ratings.countByValue()`**: This action counts the occurrences of each unique rating in the `ratings` RDD. It returns a local `map` of `(rating, count)` pairs, where:
  - **`rating`**: The unique rating value.
  - **`count`**: The number of times that rating appears in the dataset.

The `results` variable now holds the count of each rating, making it easy to analyze how many users gave each rating to the movies.

### Step 6: Sort the Results

In this step, we sort the results by the rating value to prepare for displaying the histogram.

#### Code:

```scala
val sortedResults = results.toSeq.sortBy(_._1)
```

#### Explanation:

- **`results.toSeq`**: Converts the `results` map into a `sequence`, allowing us to perform sorting operations.

- **`sortBy(_._1)`**: Sorts the sequence based on the _1 element of each tuple (the rating value). The underscore `_` is a placeholder for each element in the sequence, and `._1` refers to the _1 item in the tuple.

The `sortedResults` variable now contains the counts of each rating in ascending order, making it easier to visualize the distribution of ratings.

### What is a Sequence?

A sequence is a data structure that represents an ordered collection of elements. In Scala, sequences are a type of collection that allows you to store and manipulate a list of items. Here are some key characteristics and features of sequences:

- **Ordered**: The elements in a sequence maintain a specific order, meaning you can access elements by their index.

- **Indexed Access**: You can retrieve elements using their index position, starting from 0 for the first element.

- **Various Implementations**: Scala provides several types of sequences, including:
  - **Lists**: Immutable collections that allow duplicates.
  - **Arrays**: Mutable collections with fixed size and indexed access.
  - **Vectors**: Immutable collections optimized for both reading and writing.
  - **Range**: Represents a sequence of numbers with a start and end.

- **Functional Operations**: Sequences support various operations like `map`, `filter`, `reduce`, and `fold`, allowing you to perform transformations and aggregations on the data.

- **Conversion**: You can easily convert other collections, such as arrays or maps, into a sequence using methods like `toSeq`.

Sequences are particularly useful when you need to maintain order and perform sequential operations on a collection of data.

### Step 7: Print the Sorted Results

In this step, we will print the sorted results to the console to see the count of each rating.

#### Code:

```scala
sortedResults.foreach(println)
```

#### Explanation:

- **`sortedResults.foreach(println)`**: The `foreach` method iterates over each element in the `sortedResults` sequence and applies the `println` function to it.

This will output each `(rating, count)` pair to the console, allowing you to visualize how many times each rating was given in the dataset. This step is essential for confirming that the data processing has been done correctly and for observing the distribution of ratings.

### Spark Internals 

In Spark, operations such as `map` can be executed in parallel across multiple machines, leveraging distributed computing for efficiency and speed. Here are some key points about Spark internals:

- **Node Communication**: Nodes in a Spark cluster must communicate with each other to share data and coordinate tasks. This communication is essential for maintaining data consistency and managing distributed processing.

- **Stages**: Spark jobs are divided into stages based on transformations and actions. Each stage is determined by the lineage of RDD transformations.

  - **Stage 1**: In this stage, operations like `textFile()` and `map()` are executed. The `textFile()` operation reads the data from a source and creates an RDD, while `map()` applies a transformation to each element in the RDD.

  - **Stage 2**: This stage involves actions like `countByValue()`, which triggers the execution of the preceding transformations and collects results.

- **Task Distribution**: Each stage is further broken down into smaller tasks, which can be distributed across the cluster. Each task processes a partition of the data, allowing for parallel execution.

- **Executors**: Tasks are scheduled and sent to different executors in the cluster. Executors are responsible for running the tasks and storing the resulting data.

- **Scheduling and Execution**: Spark's cluster manager schedules tasks across the available nodes in the cluster, ensuring efficient resource utilization and minimizing data transfer. Each task executes independently, contributing to the overall job completion.

### K-V RDDs and the Average Friends by Age Example

In Spark, key-value (K-V) RDDs allow each row to be a tuple, enabling powerful data processing capabilities. Here's how to work with K-V RDDs and calculate the average number of friends by age:

#### Creating K-V RDDs

Each row of the RDD can be represented as a tuple, where the first element is the key and the second is the value. For example:

```scala
val totalsByAge = rdd.map(x => (x.age, 1))
```

In this example, x.age is the key, and 1 is the value representing a count.

#### Operations on K-V RDDs

Key-value (K-V) RDDs in Spark provide several specialized operations that enable efficient data manipulation and aggregation. Here are some of the common operations you can perform on K-V RDDs:

1. **reduceByKey()**: Combines values with the same key using a specified function. This operation is useful for aggregating data.

```scala
val reducedRDD = rdd.reduceByKey((x, y) => x + y)
```

2. **groupByKey()**: Groups all values with the same key into a collection. This operation can be memory-intensive, so use it with caution.

```scala
val groupedRDD = rdd.groupByKey()
```

3. **sortByKey()**: Sorts the RDD by key values, returning a new RDD sorted by the keys.

```scala
val sortedRDD = rdd.sortByKey()
```

4. **keys()**: Creates a new RDD containing just the keys from the original K-V RDD.

```scala
val keysRDD = rdd.keys()
```

5. **values()**: Creates a new RDD containing just the values from the original K-V RDD.

```scala
val valuesRDD = rdd.values()
```

6. **SQL-Style Joins**: You can perform SQL-style joins on two K-V RDDs, including:

   - **join()**: Combines two K-V RDDs by matching keys.
     ```scala
     val joinedRDD = rdd1.join(rdd2)
     ```
   - **leftOuterJoin()**: Combines two K-V RDDs and includes all keys from the first RDD.
     ```scala
     val leftJoinedRDD = rdd1.leftOuterJoin(rdd2)
     ```
   - **rightOuterJoin()**: Combines two K-V RDDs and includes all keys from the second RDD.
     ```scala
     val rightJoinedRDD = rdd1.rightOuterJoin(rdd2)
     ```
   - **cogroup()**: Groups the values for each key in two K-V RDDs.
     ```scala
     val cogroupedRDD = rdd1.cogroup(rdd2)
     ```

7. **subtractByKey()**: Removes keys from one K-V RDD that are present in another.

```scala
val subtractedRDD = rdd1.subtractByKey(rdd2)
```

8. **Mapping Values**: You can also manipulate the values in a K-V RDD using:

   - **mapValues()**: Applies a function to the values of a K-V RDD while keeping the keys unchanged.
     ```scala
     val mappedValuesRDD = rdd.mapValues(value => value + 1)
     ```
   - **flatMapValues()**: Similar to mapValues(), but allows the function to return multiple values.
     ```scala
     val flatMappedValuesRDD = rdd.flatMapValues(value => List(value, value + 1))
     ```

These operations provide powerful tools for data aggregation and transformation in Spark, making it easier to analyze large datasets efficiently.

### Average Friends by Age Example

To calculate the average number of friends by age, we can follow these steps:

1. **Data Structure**: The dataset might have fields like ID, name, age, and number of friends. For example:

```
0, Will, 33, 385
```

2. **Parse the Data**: We define a function to parse each line of the dataset:

```scala
def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
}
```

3. **Load the Data**: Load the dataset and create an RDD:

```scala
val lines = sc.textFile("path_to_csv")
val rdd = lines.map(parseLine)
```

4. **Aggregate Data**: Use mapValues() and reduceByKey() to calculate totals:

```scala
val totalsByAge = rdd.mapValues(numFriends => (numFriends, 1))
                      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
```

5. **Calculate Averages**: Finally, compute the average number of friends by age:

```scala
val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
```

6. **Collect Results**: Collect and print the results:

```scala
val results = averagesByAge.collect()
results.sorted.foreach(println)
```

This example demonstrates how to work with K-V RDDs in Spark, illustrating both the operations available and a practical application for calculating averages.

### Note on Partitioning in Spark RDDs

- **Partition Preservation:**
  - Many "narrow" transformations (e.g., `mapValues`) preserve the original RDD's partitioning.
  - New RDDs maintain the same number of partitions and key distribution.

- **Partition Objects:**
  - While partitioning scheme is preserved, each RDD has its own set of partition objects.
  - RDDs don't literally "share" partitions, but maintain the same partitioning strategy.

- **Performance Benefits:**
  - Preserving partitions can significantly improve performance.
  - Reduces data shuffling in operations like `reduceByKey`.

- **Example Operations:**
  - `mapValues`: Preserves partitioning, doesn't require shuffling.
  - `reduceByKey`: Uses original partitioner if known, can avoid full shuffle.

- **Partitioner Consistency:**
  ```scala

  val originalRDD = sc.parallelize(List((1, "a"), (2, "b")), 4).partitionBy(new HashPartitioner(4))
  val mappedRDD = originalRDD.mapValues(_.toUpperCase)
  val reducedRDD = mappedRDD.reduceByKey(_ + _)

  // All these will show the same partitioner
  println(originalRDD.partitioner)
  println(mappedRDD.partitioner)
  println(reducedRDD.partitioner)
  ```

- **Key Takeaway:**
  - Partitioning preservation is a crucial optimization in Spark.
  - Enables efficient execution of operation sequences on key-value data.
  - Important to consider when designing Spark jobs for optimal performance.

### Filtering RDDs, and the Minimum Temperature by Location Example

This example demonstrates how to process weather data to find the minimum temperature for each location.

#### Input Schema

The input data has a peculiar format with the following fields:
- stationID
- date (YYYYMMDD)
- observation type (TMAX/TMIN/PRCP)
- temperature (Celsius * 10) or precipitation amount
- (several empty fields)
- quality code

Example row:
```
ITE00100554,18000101,TMAX,-75,,,E,
```

#### Data Processing Steps

1. **Parsing and Filtering**

   ```scala
   val parsedLines = lines.map(parseLine)  // Assume parseLine function exists
   val minTemps = parsedLines.filter(x => x._2 == "TMIN")

   ```

   - Schema after parsing: (stationID: String, observationType: String, temperature: Float)
   - Schema after filtering: (stationID: String, "TMIN", temperature: Float)

2. **Mapping to Key-Value Pairs**

```scala
   val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
```

   - Schema: (stationID: String, temperature: Float)

3. **Reducing to Find Minimum Temperatures**

   ```scala
   // we are comparing the values because we are reducing by key!
   val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))
   ```

   - Schema remains: (stationID: String, minTemperature: Float)
   - This step processes row by row for each unique key (stationID)

4. **Collecting and Formatting Results**

   ```scala
   val results = minTempsByStation.collect()

   for (result <- results.sorted) {
     val station = result._1
     val temp = result._2
     val formattedTemp = f"$temp%.2f F"
     println(s"$station minimum temperature: $formattedTemp")
   }
   ```

### Key Points to Remember

1. **Filtering RDDs**: 
   - Use `filter()` to select elements based on a condition.
   - Filtered RDDs maintain the structure of the original RDD.

2. **Transformations vs. Actions**:
   - `filter()`, `map()`, and `reduceByKey()` are transformations (lazy evaluation).
   - `collect()` is an action that triggers the actual computation.

3. **Key-Value RDDs**:
   - Created naturally when mapping to tuples.
   - Enable powerful operations like `reduceByKey()`.

4. **Data Flow and Schemas**:
   - Pay attention to how data structure changes through transformations.
   - Ensure compatibility between operations and current data schema.

5. **Performance Considerations**:
   - `reduceByKey()` is more efficient than `groupByKey()` followed by reduction.
   - `collect()` brings all data to the driver; use with caution on large datasets.

6. **Sorting and Formatting**:
   - Sorting can be done on the collected results for small datasets.
   - For large datasets, consider using `sortByKey()` before collecting.

This example showcases a typical Spark workflow: parsing, filtering, transforming to key-value pairs, aggregating, and then collecting results for final processing or display.

### Counting Word Occurrences using FlatMap()

The `flatMap()` operation is a powerful transformation that can create multiple new elements from each input element. This makes it particularly useful for tasks like word counting in text analysis.

#### Basic Word Count

```scala
val sc = new SparkContext()
val input = sc.textFile(pathToTxtFile)

// Split each line into words
val words = input.flatMap(x => x.split(" "))

// At this point, every row of the words RDD is a single word from the book (including duplicates)

// Count occurrences of each word
val wordCounts = words.countByValue()
```

Key Points:
- `flatMap()` creates a new RDD by applying the function to each element and flattening the results.
- `countByValue()` is an action that returns a collection of (word, count) pairs.

#### Improving the Word Count Script with Regular Expressions

```scala
// Use regular expression to split on any non-word character
val words = input.flatMap(x => x.split("\\W+"))

// Convert all words to lowercase
val lowerCaseWords = words.map(x => x.toLowerCase())

// Count occurrences
val wordCounts = lowerCaseWords.countByValue()
```

Improvements:
- `\\W+` splits on one or more non-word characters, effectively removing punctuation.
- Converting to lowercase ensures consistent counting (e.g., "The" and "the" are counted as the same word).

#### Sorting the Word Count Results

To sort the results by count, we need to swap the key-value pairs and use `sortByKey()`:

```scala
// Count words using reduceByKey
val wordCounts = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

// Flip (word, count) tuples to (count, word) and then sort by key (the counts)
val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey(ascending = false)

// Collect and print the results
wordCountsSorted.collect().foreach(println)
```

Key Points:
- `reduceByKey()` is used instead of `countByValue()` for more control over the process.
- `sortByKey(ascending = false)` sorts in descending order (most frequent words first).
- The final result is a sorted list of (count, word) pairs.

#### Performance Considerations

1. **Memory Usage**: `countByValue()` collects all results to the driver. For very large datasets, consider using `reduceByKey()` and `take()` or `top()` instead.

2. **Efficiency**: `reduceByKey()` is more efficient than `groupByKey()` followed by a reduce operation, especially for large datasets.

3. **Caching**: If you plan to reuse the `words` or `lowerCaseWords` RDD multiple times, consider caching it with `cache()` or `persist()`.

#### Further Improvements

1. **Stop Words**: Remove common words (like "the", "a", "an") that don't add much meaning.

2. **Stemming/Lemmatization**: Reduce words to their root form for more accurate counting.

3. **N-grams**: Instead of single words, count occurrences of word pairs or triplets for more context.

4. **Partitioning**: If working with a very large dataset, consider repartitioning after the `flatMap()` operation to balance the load across your cluster.

This word count example demonstrates the power of `flatMap()` in conjunction with other Spark transformations and actions, showcasing how to perform text analysis tasks efficiently on large datasets.

### Find the Total Amount Spent by Customer

customer-order.csv

```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object CustomerTotalAmountSpentExercise {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentAmount")
    val input = sc.textFile("data/customer-orders.csv")

    // Split each line, extract customer ID and amount spent, and convert amount to Float
    val amounts = input.map(x => x.split(",")).map(x => (x(0), x(2).toFloat))

    // Reduce by customer ID to get total amount spent by each customer
    val customers = amounts.reduceByKey((x, y) => x + y)

    // Sort the results by customer ID (the key)
    val resultsSorted = customers.sortByKey()

    // Iterate over the sorted results and print each customer ID with total amount spent
    // No need to call collect() here because iterating over an RDD
    // automatically fetches the data as needed. 
    // Spark provides an 'iterator' that pulls data from distributed nodes, leveraging lazy evaluation.
    for (result <- resultsSorted) {
      println(f"CustomerID: ${result._1}, Total Amount Spent: ${result._2};")
    }
  }
}
```
