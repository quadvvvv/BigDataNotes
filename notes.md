# Section 1: Intro to Spark and Scala

## Spark and Scala Setup in IntelliJ 

### 1. Initial Setup 
- Installed **Java SDK 11**.
- Installed **Scala SDK 2.13.14**.
- Added the **Scala Plugin** in IntelliJ.

### 2. Project Build 
- **Apache Spark** was automatically added as a dependency when building the project. This was likely configured in `build.sbt` or `pom.xml`.

## Spark Overview

### Spark's Role
- Spark handles parallel processing across a cluster of machines.
- Manages the distribution of tasks across the cluster using a **Cluster Manager**:
  - Could be a Hadoop cluster (managed by YARN).
  - Could be Spark's built-in cluster manager (requires Spark installed on each machine).
  - The **Cluster Manager** coordinates with **Executors** to process data.
  - Spark can run on top of Hadoop clusters, taking advantage of the distributed file system and YARN cluster manager. However, Spark also has its own built-in cluster manager for use outside of Hadoop.
  - Spark and Hadoop can *Co-Exist*, they are not directly inter-replaceable.

### Comparison with Hadoop MapReduce
- Spark is a replacement for Hadoop MapReduce.
  - **100x faster** in memory.
  - **10x faster** on disk.
  - This is due to Spark being a **DAG (Directed Acyclic Graph)** engine, which optimizes workflows compared to the linear structure of MapReduce.

### The Driver Program
- The script that controls your Spark job is called the **Driver Program**.



### Comparison with Hadoop MapReduce
- Spark is a replacement for Hadoop MapReduce.
  - **100x faster** in memory.
  - **10x faster** on disk.
  - This is due to Spark being a **DAG (Directed Acyclic Graph)** engine, which optimizes workflows compared to the linear structure of MapReduce.

### Supported Languages
- **High-Level APIs**: Python, Java, Scala, SparkSQL.
- **Low-Level APIs**: RDD (Resilient Distributed Datasets).

## Spark Components
- **Spark Core**: RDDs for distributed data processing.
- **Spark Streaming**: For real-time or near real-time data processing.
- **Spark SQL**: Treats Spark as a distributed database, allowing SQL queries at scale.
- **MLLib**: Distributed machine learning library.
- **GraphX**: For graph processing.

## Why Scala?
- Spark is written in Scala, which is well-suited for distributed processing due to its functional programming model.
- Scala provides:
  - Fast performance (compiles to Java bytecode).
  - Less boilerplate code than Java.
- Python is slower since it’s also compiled to Java bytecode, making Scala a preferred choice for optimal performance.

# Section 2: Scala Crash Course

## Scala Overview

### Scala Features

- **Runs on the Java Virtual Machine (JVM)**: Scala is designed to be compatible with Java, enabling seamless integration with Java libraries and frameworks.
- **Functional Programming**: Scala supports functional programming paradigms, allowing developers to treat functions as first-class citizens, utilize higher-order functions, and leverage immutability.
  
### Functional Programming Overview

#### Core Concepts of Functional Programming

1. **First-Class Functions**: 
   - Functions are treated as first-class citizens, meaning they can be assigned to variables, passed as arguments, and returned from other functions.

2. **Pure Functions**:
   - A pure function's output depends only on its input parameters. It doesn't cause any side effects (like modifying global variables or performing I/O operations), making them predictable and easier to test.

3. **Immutability**:
   - Data is immutable by default. Once created, data cannot be changed. Instead of modifying data, you create new data structures based on existing ones, preventing bugs and simplifying reasoning about code.

4. **Higher-Order Functions**:
   - Functions that can take other functions as arguments or return them as results, allowing for powerful abstractions and code reuse.

5. **Function Composition**:
   - Building complex functions by combining simpler ones, promoting modularity and separation of concerns.

#### Differences from Java and Python

1. **Mutability**:
   - **Java**: Primarily an object-oriented language, where mutable objects are common, and state management often relies on mutable objects.
   - **Python**: Supports functional programming features but is heavily object-oriented, often using mutable data structures.
   - **Scala**: Emphasizes immutability and encourages the use of immutable collections by default.

2. **Function Types**:
   - **Java**: Traditionally object-oriented, but Java 8 introduced lambdas, allowing some functional programming features.
   - **Python**: Supports functional programming (e.g., `map`, `filter`, and `lambda`), but also embraces object-oriented and imperative styles.
   - **Scala**: Fully supports functional programming, allowing for pure function usage.

3. **Side Effects**:
   - **Java**: Side effects are common, leading to mutable state and complicating change tracking.
   - **Python**: Functions may also have side effects, with state management being a common practice.
   - **Scala**: Encourages writing pure functions with no side effects, leading to clearer, maintainable code.

4. **Concurrency**:
   - **Java**: Uses threads and locks, which can cause issues like race conditions with mutable state.
   - **Python**: Similar concerns with threads; often uses `asyncio` for asynchronous programming.
   - **Scala**: Utilizes immutability and functional programming constructs to simplify concurrency, reducing errors related to shared state.

#### Conclusion

Functional programming offers a different approach to coding, focusing on functions, immutability, and side-effect-free computations. While Java and Python incorporate some functional programming features, they are primarily object-oriented, leading to different design patterns. Scala fully embraces both functional and object-oriented paradigms, allowing developers to choose the best approach for their needs.

### Scala Syntax

#### Key Concepts

1. **Variables**:
   - **`val` (Immutable)**: Used to declare immutable variables (cannot be reassigned).
   - **`var` (Mutable)**: Used to declare mutable variables (can be reassigned).

2. **Data Types**:
   - Common types include `Int`, `Boolean`, `Char`, `Double`, `Float`, `Long`, and `Byte`.

3. **Control Structures**:
   - Includes `if`, `else`, `match` for conditional statements and `for`, `while` for loops.

4. **Functions**:
   - Defined using the `def` keyword, allowing for parameterized reusable code.

### What is Functional Programming?

Functional programming is a programming paradigm that treats computation as the evaluation of mathematical functions and avoids changing state or mutable data. Key characteristics include:

- **First-Class Functions**: Functions can be assigned to variables, passed as arguments, and returned from other functions.
- **Higher-Order Functions**: Functions that can take other functions as parameters or return them as results.
- **Immutability**: Emphasis on using immutable data structures to prevent side effects and improve code safety.

### What is a Scala Worksheet?

A Scala worksheet is a tool used for interactive programming, typically within IDEs like IntelliJ IDEA with the Scala plugin. Key features include:

- **Immediate Feedback**: Automatically compiles and evaluates code snippets, displaying results alongside the code.
- **Testing and Experimenting**: Ideal for trying out new ideas, testing code sections, or learning Scala concepts interactively.
- **Integration with IDE**: Provides syntax highlighting, code completion, and error checking within the IDE environment.

In essence, a Scala worksheet functions similarly to a REPL (Read-Eval-Print-Loop), offering a convenient way to write and test Scala code interactively.

### Scala Variable Declarations and Types

#### Variable Declarations in Scala

Both `val` and `var` are used to declare variables.

#### 1. `val` (Immutable):

- Short for "value". Declares an **immutable** variable, meaning that once assigned, the value cannot be changed (similar to `final` in Java).
- Reassigning a `val` will cause a compile-time error.
- Using `val` is safer and promotes functional programming practices.

#### 2. `var` (Mutable):

- Short for "variable". Declares a **mutable** variable that can be reassigned.
- Using `var` can lead to more side effects and make code harder to maintain.
- It’s good practice to use `val` unless mutability is explicitly required.

#### Example:

```scala
// 'val' - Immutable value.
val hello: String = "hello!"
// Avoids thread-safety issues and race conditions.

// 'var' - Mutable variable.
var helloThere: String = hello
helloThere = hello + " There!"
println(helloThere)

// Immutable value concatenation.
val immutableHelloThere = hello + " There!"
println(immutableHelloThere)
```

#### Data Types in Scala

Scala Supports various data types like: 

``` scala
// Declaring data types.
val numberOne: Int = 1
val truth: Boolean = true
val letterA: Char = 'a'
val pi: Double = 3.1415926
val piSinglePrecision: Float = 3.14159265f // 7 decimal places of precision
val bigNumber: Long = 123456789L // Large numbers for up to 15 digits of precision
val smallNumber: Byte = 126

```

### String Concatenation and Formatting

#### String Concatenation

Scala will *implicitly* convert variables to String when printing.

```scala
println("Here is a mess: " + numberOne + truth + letterA + bigNumber)
```

#### String Formatting with f Inerpolator

The 'f' string interpolator formats floating-point numbers to a specific precision.

```scala
// Here, '%3f' restricts the output to 3 decimal places.
println(f"Pi is about $piSinglePrecision%.3f")
```

#### Padding Numbers

You can pad numbers with spaces(by default) or zeros.

```scala
// '%15d' pads with spaces by default, '%015d' pads with zeros.
println(f"Zero padding on the left: $numberOne%05d")
```

#### Value Substitution with 's' Interpolator

The 's' string interpolator allows direct variable substitution in Strings.

```scala
println(s"I can use the 's' prefix to include variables like $numberOne $truth $letterA")
```

You can also include expressions within the 's' interpolator using curly braces.

```scala
// The value will be calculated
println(s"The 's' prefix is not limited to variables; I can include expressions like ${1 + 2}")
```

#### Regular Expression (Regex)

You can use regex to match aptters in Strings.

```scala
val theUltimateAnswer: String = "To life, the universe, and everything is 42."
val pattern = """.* ([\d]+).*""".r

// Capturing the matched number into 'answerString' using regex.

val pattern(answerString) = theUltimateAnswer
val answer = answerString.toInt
println(answer)
```

Note: The answerString is a variable declared to capture the value match the pattern.

### Boolean Operations

```scala
val isGreater = 1 > 2
val isLesser = 1 < 2
val impossible = isGreater & isLesser // Bitwise AND
val anotherWay = isGreater && isLesser // Logical AND (commonly used)

val picard: String = "picard"
val bestCaptain: String = "picard"
val isBest: Boolean = picard == bestCaptain
```

### Exercise 1

```scala
// Double the value of 'pi' and print it with three decimal places of precision.
println(f"Exercise's Answer: ${pi * 2}%.3f")
```

### Flow Control in Scala

#### If / Else

Standard if-else statements. The result of the expression can be returned implicitly.

```scala
if (1 > 3) println("Impossible!") else println("The world makes sense.")

if (1 > 3) {
  println("Impossible!")
  println("Really?")
} else {
  println("The world makes sense.")
  println("Still.")
}
```

#### Matching (Pattern Matching)

Similar to Java's switch statement, but more powerful.

```scala
val number = 2
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  
  // Default case, just like in Java's 'default'
  case _ => println("Something else")
}
```

#### For Loops

The syntax uses '<-' which is not a comparison, but a generator for a range.

```scala
for (x <- 1 to 4) {
  val squared = x * x
  println(squared)
}
```

#### While Loop

While loops are less common in Scala due to immutability and functional programming style.

```scala
var x = 10
while (x >= 0) {
  println(x)
  x -= 1
}
```

#### Do While Loop

Executes the block at least once, then checks the condition.

```scala
x = 0
do {
  println(x)
  x += 1
} while (x <= 10)
```

#### Expressions

Blocks return the value of the last expression implicitly.

```scala
// This block will return 30.
{ val x = 10; x + 20 }

// We can also print the result of an expression.
println({ val x = 10; x + 20 })
```

#### EXERCISE 2

Write code that prints the first 10 values of the Fibonacci sequence.
The sequence is defined by each number being the sum of the two previous numbers.
Expected output: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34

```scala
var x_1 = 0
var x_2 = 1
var ct = 2

println(x_1) // First Fibonacci number
println(x_2) // Second Fibonacci number

while (ct < 10) {
  val temp = x_1 + x_2  // Calculate the next Fibonacci number
  println(temp)          // Print the Fibonacci number
  x_1 = x_2              // Update x_1 to x_2
  x_2 = temp             // Update x_2 to temp
  ct += 1                // Increment the counter
}
```

### Functions in Scala

#### Format: def <function_name>(parameter_name: Type, ...) : ReturnType = { ... }

```scala
// The implicit return of the last evaluated expression
def squareIt(x: Int): Int = {
  x * x
}

def cubeIt(x: Int): Int = {
  x * x * x
}

println(squareIt(2))  // Output: 4
println(cubeIt(3))    // Output: 27
```

#### Function can take other functions as parameters

```scala
// f: Int => Int is another function that takes an integer and transforms it to some integer
def transformInt(x: Int, f: Int => Int): Int = {
  f(x)
}

val result = transformInt(2, cubeIt)
println(result)  // Output: 8
```


#### Lambda function / anonymous function

```scala
// Define the function inline
transformInt(3, x => x * x * x)  // Output: 27
transformInt(10, x => x / 2)      // Output: 5

// You can use multi-line expressions
// "{}" is a coding block
transformInt(2, x => { 
  val y = x * 2 
  y * y 
})  // Output: 16

```

#### EXERCISE 3

Strings have a built-in .toUpperCase method. For example, "foo".toUpperCase gives you back "FOO".
Write a function that converts a string to upper-case and use that function on a few test strings.

```scala
def upperCaseString(x: String, f: String => String): String = {
  f(x)
}

println(upperCaseString("abc", x => x.toUpperCase))  // Output: ABC
println(upperCaseString("hello", x => x.toUpperCase)) // Output: HELLO
println(upperCaseString("scala", x => x.toUpperCase)) // Output: SCALA

```

### Data Structure in Scala

#### Tuples - very common in Spark

Immutable collections, similar to database fields like columns for data

The index for tuples is **ONE-BASED**!!!

```scala
val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

// Refer to individual fields with a ONE-BASED index
// This is unusual but important
println(captainStuff._1) // "Picard"
println(captainStuff._2) // "Enterprise-D"
println(captainStuff._3) // "NCC-1701-D"

// Key-value pairs
// The type is (String, String), still a tuple
val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip._2) // "Enterprise-D"

// You can mix different types within a tuple
val aBunchOfStuff = ("Kirk", 1964, true)
```

#### Lists - Collection Object

Underlying implementation is a singly linked list
Unlike tuples, lists can have more functionality
All elements must be of the same type

```scala
// List[String]
val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")

// Accessing elements is zero-based, which is different from tuples
println(shipList(1)) // "Defiant"

// Because it's a linked list under the hood
println(shipList.head) // First element: "Enterprise"
println(shipList.tail) // Sublist excluding the first item: List("Defiant", "Voyager", "Deep Space Nine")

// For each loop to iterate through the list
for (ship <- shipList) {
  println(ship)
}
```

#### Using the map function to apply a transformation to each item

This is crucial in distributed computing

```scala
val backwardShips = shipList.map((ship: String) => ship.reverse)
for (ship <- backwardShips) {
  println(ship)
}
```

#### Reduce function to combine items in a collection using a specified function

```scala
val numberList = List(1, 2, 3, 4, 5)
// Combines the values into a single result
val sum = numberList.reduce((x: Int, y: Int) => x + y)
println(sum) // 15
```

#### Filter function to remove elements that do not match a condition

```scala
// Retains values that meet the condition (true)
val iHateFives = numberList.filter((x: Int) => x != 5)

// Shorthand using the underscore placeholder for every element in the list
val iHateThrees = numberList.filter(_ != 3)
```

#### List Operations

```scala
val moreNumbers = List(6, 7, 8)
// Use double plus for concatenating lists
val lotsOfNumbers = numberList ++ moreNumbers ++ moreNumbers

// Common operations
val reversed = numberList.reverse // Reverses the list
val sorted = reversed.sorted // Sorts in ascending order by default
val lotsOfDuplicates = numberList ++ numberList // Can have duplicates
val distinctValues = lotsOfDuplicates.distinct // Removes duplicates
val maxValue = numberList.max // Finds the maximum value
val total = numberList.sum // Sums the values
val hasThree = iHateThrees.contains(3) // Checks if 3 is present in the list

```

#### Maps - an important data structure

```scala
// Key-value pairs where the data types need to be the same
val shipMap = Map(
  "Kirk" -> "Enterprise",
  "Picard" -> "Enterprise-D",
  "Sisko" -> "Deep Space Nine",
  "Janeway" -> "Voyager"
)
println(shipMap("Janeway")) // "Voyager"
println(shipMap.contains("Archer")) // Checks if "Archer" is in the map

// Try block for exception handling
val archersShip = util.Try(shipMap("Archer")).getOrElse("Unknown")
println(archersShip) // "Unknown"
```

#### EXERCISE 4

Create a list of numbers 1-20; print out numbers that are evenly divisible by three.
The modulus operator in Scala is %, which gives you the remainder after division.

```scala
for (x <- 1 to 20) {
  if (x % 3 == 0) println(x) // Prints numbers divisible by 3
}

// Create a list of candidates from 1 to 20
val candidates = List.range(1, 21) // Generates List(1, 2, ..., 20)

// Alternatively, convert a range to a list
val otherCandidates = (1 to 20).toList

// Use filter to find numbers divisible by 3
val finalAnswer = candidates.filter(_ % 3 == 0) // List of numbers divisible by 3
println(finalAnswer) // Prints the final result
```

# Section 3 

## Introduction to RDD

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

## Rating Histogram Example

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

## Spark Internals 

In Spark, operations such as `map` can be executed in parallel across multiple machines, leveraging distributed computing for efficiency and speed. Here are some key points about Spark internals:

- **Node Communication**: Nodes in a Spark cluster must communicate with each other to share data and coordinate tasks. This communication is essential for maintaining data consistency and managing distributed processing.

- **Stages**: Spark jobs are divided into stages based on transformations and actions. Each stage is determined by the lineage of RDD transformations.

  - **Stage 1**: In this stage, operations like `textFile()` and `map()` are executed. The `textFile()` operation reads the data from a source and creates an RDD, while `map()` applies a transformation to each element in the RDD.

  - **Stage 2**: This stage involves actions like `countByValue()`, which triggers the execution of the preceding transformations and collects results.

- **Task Distribution**: Each stage is further broken down into smaller tasks, which can be distributed across the cluster. Each task processes a partition of the data, allowing for parallel execution.

- **Executors**: Tasks are scheduled and sent to different executors in the cluster. Executors are responsible for running the tasks and storing the resulting data.

- **Scheduling and Execution**: Spark's cluster manager schedules tasks across the available nodes in the cluster, ensuring efficient resource utilization and minimizing data transfer. Each task executes independently, contributing to the overall job completion.

## K-V RDDs and the Average Friends by Age Example

In Spark, key-value (K-V) RDDs allow each row to be a tuple, enabling powerful data processing capabilities. Here's how to work with K-V RDDs and calculate the average number of friends by age:

### Creating K-V RDDs

Each row of the RDD can be represented as a tuple, where the first element is the key and the second is the value. For example:

```scala
val totalsByAge = rdd.map(x => (x.age, 1))
```

In this example, x.age is the key, and 1 is the value representing a count.

### Operations on K-V RDDs

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

## Filtering RDDs, and the Minimum Temperature by Location Example

This example demonstrates how to process weather data to find the minimum temperature for each location.

### Input Schema

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

### Data Processing Steps

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

## Counting Word Occurrences using FlatMap()

The `flatMap()` operation is a powerful transformation that can create multiple new elements from each input element. This makes it particularly useful for tasks like word counting in text analysis.

### Basic Word Count

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

### Improving the Word Count Script with Regular Expressions

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

### Sorting the Word Count Results

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

### Performance Considerations

1. **Memory Usage**: `countByValue()` collects all results to the driver. For very large datasets, consider using `reduceByKey()` and `take()` or `top()` instead.

2. **Efficiency**: `reduceByKey()` is more efficient than `groupByKey()` followed by a reduce operation, especially for large datasets.

3. **Caching**: If you plan to reuse the `words` or `lowerCaseWords` RDD multiple times, consider caching it with `cache()` or `persist()`.

### Further Improvements

1. **Stop Words**: Remove common words (like "the", "a", "an") that don't add much meaning.

2. **Stemming/Lemmatization**: Reduce words to their root form for more accurate counting.

3. **N-grams**: Instead of single words, count occurrences of word pairs or triplets for more context.

4. **Partitioning**: If working with a very large dataset, consider repartitioning after the `flatMap()` operation to balance the load across your cluster.

This word count example demonstrates the power of `flatMap()` in conjunction with other Spark transformations and actions, showcasing how to perform text analysis tasks efficiently on large datasets.

## Find the Total Amount Spent by Customer

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



# Section 4
