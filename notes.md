## Section 1: Intro to Spark and Scala

### Spark and Scala Setup in IntelliJ 

#### 1. Initial Setup 
- Installed **Java SDK 11**.
- Installed **Scala SDK 2.13.14**.
- Added the **Scala Plugin** in IntelliJ.

#### 2. Project Build 
- **Apache Spark** was automatically added as a dependency when building the project. This was likely configured in `build.sbt` or `pom.xml`.

### Spark Overview

#### Spark's Role
- Spark handles parallel processing across a cluster of machines.
- Manages the distribution of tasks across the cluster using a **Cluster Manager**:
  - Could be a Hadoop cluster (managed by YARN).
  - Could be Spark's built-in cluster manager (requires Spark installed on each machine).
  - The **Cluster Manager** coordinates with **Executors** to process data.
  - Spark can run on top of Hadoop clusters, taking advantage of the distributed file system and YARN cluster manager. However, Spark also has its own built-in cluster manager for use outside of Hadoop.
  - Spark and Hadoop can *Co-Exist*, they are not directly inter-replaceable.

#### Comparison with Hadoop MapReduce
- Spark is a replacement for Hadoop MapReduce.
  - **100x faster** in memory.
  - **10x faster** on disk.
  - This is due to Spark being a **DAG (Directed Acyclic Graph)** engine, which optimizes workflows compared to the linear structure of MapReduce.

#### The Driver Program
- The script that controls your Spark job is called the **Driver Program**.



#### Comparison with Hadoop MapReduce
- Spark is a replacement for Hadoop MapReduce.
  - **100x faster** in memory.
  - **10x faster** on disk.
  - This is due to Spark being a **DAG (Directed Acyclic Graph)** engine, which optimizes workflows compared to the linear structure of MapReduce.

#### Supported Languages
- **High-Level APIs**: Python, Java, Scala, SparkSQL.
- **Low-Level APIs**: RDD (Resilient Distributed Datasets).

### Spark Components
- **Spark Core**: RDDs for distributed data processing.
- **Spark Streaming**: For real-time or near real-time data processing.
- **Spark SQL**: Treats Spark as a distributed database, allowing SQL queries at scale.
- **MLLib**: Distributed machine learning library.
- **GraphX**: For graph processing.

### Why Scala?
- Spark is written in Scala, which is well-suited for distributed processing due to its functional programming model.
- Scala provides:
  - Fast performance (compiles to Java bytecode).
  - Less boilerplate code than Java.
- Python is slower since it’s also compiled to Java bytecode, making Scala a preferred choice for optimal performance.

## Section 2: Scala Crash Course

### Scala Overview

#### Scala Features

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

##### Conclusion

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


## Quiz notes:

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


## Section 5 Advanced Examples of Apache Spark

### Table of Contents
1. [MovieLens Data Format](#movielens-data-format)
2. [Popular Movies Dataset](#popular-movies-dataset)
3. [Broadcast Variables and UDFs](#broadcast-variables-and-udfs)
4. [Superhero Social Networks](#superhero-social-networks)
5. [Superhero Degrees of Separation](#superhero-degrees-of-separation)
6. [Item-Based Collaborative Filtering](#item-based-collaborative-filtering)

### MovieLens Data Format

The MovieLens dataset has the following format:
```
UserID, MovieID, Rating, Timestamp
```

### Popular Movies Dataset

#### Joining with Other Data

There are three options for joining the ratings dataset with movie names:

1. Create a Dataset to map IDs to names, then join with the ratings dataset
   - Drawback: Too much unnecessary overhead due to distributed datasets

2. Keep a table (Map) loaded in the "driver program"

3. Let Spark automatically forward it to each executor when needed
   - Solution: Use broadcast variables

#### Implementation

Refer to `PopularMoviesDataset.scala` and `PopularMoviesNicerDataset.scala` for implementation details.

### Broadcast Variables and UDFs

- Use `sc.broadcast()` to distribute objects to executors
- Access the broadcast variable using `.value()`
- Can be used for map functions, UDFs, etc.

### Superhero Social Networks

#### Data Format

- `Marvel-graph.txt`: hero ID, IDs of the hero's connections
- `Marvel-names.txt`: hero ID, quotation mark-enclosed names

#### Challenge: Find the Most Popular Superhero

1. Split off hero ID from the beginning of each line
2. Count space-separated numbers in the line (connections)
3. Subtract one to get the total number of connections
4. Group by hero IDs to sum up connections split into multiple lines
5. Sort by total connections
6. Look up the name of the most popular hero

Refer to `MostPopularSuperHeroDataset.scala` and `MostPopularSuperHero.scala` for implementation details.

Note: The RDD version (MostPopularSuperHero.scala) is more straightforward and simple compared to the Dataset version.

### Superhero Degrees of Separation

#### Introducing Breadth-First Search (BFS)

- Iterative BFS implementation in Spark
- Introduction to accumulators

#### Implementation Steps

1. Map: Convert each line into a BFS format node with connections, distances, and color
   - Initialize every node as white with distance 9999

2. BFS Iteration (Map and Reduce):
   - Mapper:
     - Create new nodes for each connection of gray nodes
     - Color processed gray nodes black
     - Copy the node itself into the results
   - Reducer:
     - Combine nodes for the same hero ID
     - Preserve the shortest distance and darkest color
     - Preserve the list of connections from the original node

3. Use an accumulator to determine when the process is complete

Refer to `DegreesOfSeparation.scala` for the implementation.

### Item-Based Collaborative Filtering

#### Finding Similar Movies

1. Select userID, movieID, and rating columns
2. Find every movie pair rated by the same user (self-join operation)
3. Filter out duplicate pairs
4. Compute cosine similarity scores for every pair
5. Group by movie pairs and compute similarity scores
6. Filter, sort, and display results

#### Caching Dataset

- Use `.cache()` or `.persist()` when performing more than one action on a dataset
- `.persist()` allows caching to disk in case of node failures

Refer to `MovieSimilarities1MDataset.scala` for the implementation.

### Historical Context

- MapReduce paradigm: Invented by Google for dealing with large amounts of data
- Hadoop: Built on top of MapReduce
- Spark: Optimized Hadoop's MapReduce
- Spark 2.0: Introduction of Datasets, inspired by SQL

Note: Using RDDs is often more straightforward than Datasets for certain tasks.

## Section 6 Running Apache Spark on a Cluster: A Comprehensive Guide

### Table of Contents
1. [Packaging and Deploying Your Application](#packaging-and-deploying-your-application)
2. [Using SBT (Simple Build Tool)](#using-sbt-simple-build-tool)
3. [Amazon Elastic MapReduce (EMR)](#amazon-elastic-mapreduce-emr)
4. [Spark-Submit Parameters](#spark-submit-parameters)
5. [Partitioning](#partitioning)
6. [Best Practices for Running on a Cluster](#best-practices-for-running-on-a-cluster)
7. [Troubleshooting Cluster Jobs](#troubleshooting-cluster-jobs)
8. [Managing Dependencies](#managing-dependencies)
9. [Key Points to Remember](#key-points-to-remember)

### Packaging and Deploying Your Application

When running Spark applications on a cluster, you need to package and deploy them properly. Here's how:

#### Prepare your application:
- Ensure no paths point to your local filesystem
- Use distributed file systems like HDFS or S3

#### Use spark-submit:
- Command: `spark-submit`
- Options:
  - `--class`: Specifies the class containing your main function
  - `--jars`: Paths to any dependencies
  - `--files`: Files you want placed alongside your application (e.g., small lookup files)

Example:
```bash
spark-submit --class com.example.MainClass --jars deps.jar --files lookup.txt myapp.jar
```

### Using SBT (Simple Build Tool)

SBT is a powerful build tool for Scala projects:

#### Benefits:
- Manages library dependency tree
- Packages dependencies into a self-contained JAR

#### Setup:
- Create directory structure: `src/main/scala`
- In `project` folder, create `assembly.sbt`:
  ```scala
  addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
  ```
- Create `build.sbt` in the root folder:
  ```scala
  name := "MySparkProject"
  version := "1.0"
  scalaVersion := "2.12.10"
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.0.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
  )
  ```

#### Build:
- Run `sbt assembly` from the root folder
- Find the JAR in `target/scala-<version>/`

### Amazon Elastic MapReduce (EMR)

EMR provides a pre-configured Spark environment:

#### Setup:
- Create a cluster with Spark, Hadoop, and YARN pre-installed
- Use the AWS console to spin up an EMR cluster

#### Deployment:
- Copy your JAR and data to S3
- Log into the master node using the "hadoop" user account
- Copy files from S3 to the master node:
  ```bash
  aws s3 cp s3://bucket-name/file-name ./
  ```
- Run `spark-submit` on the master node

#### Important Note: 
- Spark and Hadoop are not mutually exclusive
- Spark often runs on Hadoop's YARN cluster manager

### Spark-Submit Parameters

Key parameters for `spark-submit`:

- `--master`: Set to the corresponding cluster manager (e.g., `yarn` for EMR)
- `--num-executors`: Number of executors to launch
- `--executor-memory`: Memory per executor
- `--total-executor-cores`: Total cores for all executors

Example:
```bash
spark-submit --master yarn --num-executors 5 --executor-memory 4g --total-executor-cores 20 myapp.jar
```

### Partitioning

Partitioning is crucial for performance:

#### Purpose: 
- Tells Spark how to distribute tasks across the cluster

#### When to use:
- For operations like `join`, `cogroup`, `groupWith`, `join`, `groupByKey`, `reduceByKey`, `lookup()`

#### How to use:
- On DataFrame: `.repartition()`
- On RDD: `.partitionBy()`

#### Choosing partitions:
- Rule of thumb: At least as many partitions as cores/executors
- Start with `partitionBy(100)` for large operations

Example:
```scala
val repartitionedDF = myDataFrame.repartition(100)
```

### Best Practices for Running on a Cluster

#### Configuration:
- Avoid specifying Spark configs in your driver script
- Use EMR defaults and command-line options

#### Memory Management:
- If executors start failing, adjust memory allocation

#### Data Access:
- Store data in easily accessible locations (e.g., S3 for EMR)

#### Testing:
- Test with a reasonable amount of data locally before moving to the cluster

### Troubleshooting Cluster Jobs

#### Spark UI:
- Available on port 4040 of the master node
- View jobs, stages, DAG visualization, and task timelines

#### Logs:
- For YARN: Use `yarn logs -applicationId <app_id>`
- Watch for errors like executor failures or heartbeat issues

#### Common Issues:
- Out of memory errors: Consider repartitioning or adding more hardware
- Slow jobs: Check for data skew or inefficient operations

### Managing Dependencies

#### Broadcast Variables:
- Use to share data outside of RDDs or DataFrames

#### External Libraries:
- Bundle into your JAR with SBT assembly
- Or use `--jars` with spark-submit (but be cautious of dependency conflicts)

#### Best Practice:
- Avoid using obscure packages when possible

### Key Points to Remember

1. `spark-submit` is used to execute your compiled script across the entire cluster.

2. In `build.sbt`, list Spark core as a provided dependency:
   ```scala
   "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"
   ```

3. Use `--executor-memory` to specify memory for each executor.

4. If a large join is running out of memory, try using `repartition()` first.

5. On EMR, Spark is pre-configured to use cluster resources efficiently. Avoid manual configuration unless necessary.

Remember to always terminate your cluster when you're done to avoid unnecessary costs!

## Section 7: Machine Learning with Spark ML

### Introduction to Spark ML Library

#### ML Capabilities

- Feature Extraction
  - Term Frequency / Inverse Document Frequency (TF-IDF): Useful for search and text analysis
- Basic Statistics
- Linear Regression
- Logistic Regression
- Support Vector Machines (SVM)
- Naive Bayes Classifier
- Decision Trees
- K-Means Clustering
- Principal Component Analysis (PCA)
- Singular Value Decomposition (SVD)
- Recommendation using Alternating Least Squares (ALS)

#### ML Uses DataFrames

- Previous API called MLLib used RDDs and some specialized data structures
- MLLib is deprecated in Spark 3
- The newer ML library uses DataFrames for everything
  - DataFrames can be fed to other components seamlessly

### Activity: Using ML to Produce Movie Recommendations

```scala
val data = spark.read.textFile("path/to/file")
val ratings = data.map(x => x.split('\t')).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble))

val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

val model = als.fit(ratings)
```

Notes:
- This process involves trial-and-error with hyperparameter tuning
- Schema for u.data: userId, movieId, rating, timestamp
- Schema for movie names: movieId, movieTitle

File: MovieRecommendationsALSDataset.scala

Tip: To change arguments passed in, use "Modify Run Configuration" in IntelliJ

### Linear Regression with MLLib

- Fits a line to a dataset of observations
- Used to predict unobserved values (future, past, etc.)
- Usually uses least squares method to minimize the squared error between each point and the line
- Equation: y = mx + b
  - Slope (m) = correlation between variables * (standard deviation in y / standard deviation in x)
  - Intercept (b) = mean of y - (slope * mean of x)

Spark uses Stochastic Gradient Descent (SGD):
- More complex technique
- Friendlier to multi-dimensional data
- Iteratively finds the best fit line over higher-dimensional space

### Activity: Running a Linear Regression with Spark

File: LinearRegressionDataFrameDataset.scala

Schema:
- Amount of money spent (normalized)
- Pay speed (normalized and scaled down)

Goal: Predict how much people spent based on pay speed

### Exercise: Predict Real Estate Values with Decision Trees in Spark

Schema: HouseId, TransactionDate, HouseAge, DistanceToMRT, NumberConvenienceStores, Latitude, Longitude, PriceOfUnitArea

Goal: Predict the price per unit area based on house age, distance to MRT, and number of nearby convenience stores

Strategy:
1. Use DecisionTreeRegressor instead of LinearRegression (handles different scales better)
2. Start with a copy of LinearRegressionDataFrameDataset.scala
3. Note the header row - no need to hard-code a schema for reading the data file

Tips:
- VectorAssembler can have multiple input columns: 
  ```scala
  .setInputCols(Array("col1", "col2"))
  ```
- Use options when reading the data:
  ```scala
  .option("header", "true").option("inferSchema", "true")
  ```
- Match case class field names to the header names
- For DecisionTreeRegressor:
  - Can be used without hyperparameters
  - Use `.setLabelCol()` to specify a label column if its name is not "label"

### Quiz: Spark ML

1. Q: Linear Regression is implemented via SGD in SparkML. What is SGD?
   A: Stochastic Gradient Descent - an iterative optimization algorithm for finding the minimum of a function.

2. Q: Spark's recommender engine is implemented with ALS. What is ALS?
   A: Alternating Least Squares - a matrix factorization algorithm used for collaborative filtering.

3. Q: Spark doesn't support deep learning yet. Why?
   A: As of the last update, Spark focuses on distributed computing for large-scale data processing. Deep learning often requires specialized hardware (GPUs) and libraries, which are not natively supported in Spark. However, there are efforts to integrate deep learning frameworks with Spark.

4. Q: What properties of your data are assumed by Spark's linear regression algorithm?
   A: 
   - The data follows a linear relationship
   - The residuals (errors) are normally distributed
   - There's no or little multicollinearity between independent variables
   - Homoscedasticity (constant variance of residuals)

   Note: The y-intercept is not assumed to be 0 by default in Spark's implementation. You can specify whether to fit an intercept or not.

Remember: Spark's linear regression doesn't automatically scale the data to fit standard deviations of a normal distribution. If needed, you should perform feature scaling as a preprocessing step.


## Section 8: Intro to Spark Streaming

### The DStream API for Spark Streaming

Spark Streaming is designed to analyze streams of data in real-time as it's created. For example, it can process log data from a website or server.

#### Key Concepts:

- Continuous monitoring of incoming data
- Data is aggregated and analyzed at given intervals
- Can take input from various sources: TCP/IP port, Amazon Kinesis, HDFS, Kafka, Flume, and others
- Uses checkpointing to store state periodically for fault tolerance

#### DStream (Discretized Stream)

- Breaks up the stream into distinct RDDs (micro-batches)
- Historical API, not necessarily real-time (row by row)
- Suitable for most applications where millisecond differences are not critical

#### Simple Example:

```scala
val stream = new StreamingContext(conf, Seconds(1))
val lines = stream.socketTextStream("localhost", 8888)
val errors = lines.filter(_.contains("error"))
errors.print()

stream.start()
stream.awaitTermination()
```

This example listens to log data sent to port 8888, processes it in 1-second intervals, and prints out error lines.

#### Advanced Operations:

- Windowed Operations: Combine results from multiple batches over a sliding time window
  - Methods: `window()`, `reduceByWindow()`, `reduceByKeyAndWindow()`
- `updateStateByKey()`: Maintains state across many batches (e.g., running counts of events)

### Activity: Real-time Monitoring of Popular Hashtags on Twitter

Goal: Track the most popular hashtags from tweets in real-time.

Steps:

1. Get Twitter stream and extract messages:
   ```scala
   val tweets = TwitterUtils.createStream(ssc, None)
   val statuses = tweets.map(status => status.getText())
   ```

2. Split tweets into words:
   ```scala
   val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
   ```

3. Filter for hashtags:
   ```scala
   val hashtags = tweetwords.filter(word => word.startsWith("#"))
   ```

4. Prepare for counting:
   ```scala
   val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
   ```

5. Count hashtags within a time window:
   ```scala
   val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(
     (x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1)
   )
   ```
   - 300 seconds window, 1 second slide

6. Sort and output results:
   ```scala
   val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
   sortedResults.print
   ```

7. Start the streaming context:
   ```scala
   ssc.checkpoint("checkpoint_dir")
   ssc.start()
   ssc.awaitTermination()
   ```

Note: Remember to set up library dependencies in `build.sbt`, including Twitter libraries.

### Structured Streaming

Introduced in Spark 2, Structured Streaming is a new way of handling streaming data.

#### Key Features:

- Uses DataSets as its primary API
- Treats streaming data as an ever-appending DataSet
- Real-time processing, not based on micro-batches

#### Example:

```scala
val inputDF = spark.readStream.json("input_path")
inputDF.groupBy("$action", window("$time", "1 hour"))
  .count()
  .writeStream
  .format("jdbc")
  .start("jdbc:mysql//...")
```

### Activity: Using Structured Streaming for Real-Time Log Analysis

Goal: Analyze streaming log files in real-time.

Steps:

1. Read streaming data:
   ```scala
   val accessLines = spark.readStream.text("data/logs")
   ```

2. Parse log lines using regex:
   ```scala
   val logsDF = accessLines.select(
     regexp_extract($"value", contentSizeExp, 1).cast("int").alias("contentSize"),
     // ... other fields
   )
   ```

3. Perform real-time analysis:
   ```scala
   val statusCountsDF = logsDF.groupBy("status").count()
   ```

4. Output results:
   ```scala
   val query = statusCountsDF.writeStream
     .outputMode("complete")
     .format("console")
     .queryName("counts")
     .start()

   query.awaitTermination()
   ```

### Exercise: Windowed Operations with Structured Streaming

Goal: Stream top URLs in the past 30 seconds from Apache access logs.

#### Windowed Operations:

- Look back over a period of time (window)
- Slide interval defines how often we evaluate a window

Example:
```scala
dataset.groupBy(
  window(col("timestampColumnName"), 
    windowDuration = "10 minutes",
    slideDuration = "5 minutes"),
  col("columnWeAreGroupingBy")
)
```

#### Challenge:

Modify the StructuredStreaming script to track the most viewed URLs (endpoints) in the logs with a 30-second window and 10-second slide.

Useful snippets:
```scala
logsDF.groupBy(window(col("eventTime"), "30 seconds", "10 seconds"), col("endpoint"))
logsDF.withColumn("eventTime", current_timestamp())
endpointCounts.orderBy(col("count").desc)
```

### Quiz: Spark Streaming

1. Q: How do DStream objects present streaming data to your driver script?
   A: As a series of RDDs, each containing data received during the time interval you specify.

2. Q: How does structured streaming present streaming data to your driver script?
   A: As a single, ever-expanding DataSet object you can query with time windows.

## Section 9: Intro to GraphX

### GraphX, Pregel, and BFS with Pregel

GraphX is Spark's library for graph-parallel computation. As of 2024, while GraphX is still available, it's worth noting that GraphFrames, built on top of DataFrames, is becoming increasingly popular for graph processing in Spark.

#### Key Capabilities:

- Measure graph properties:
  - Connectedness
  - Degree distribution
  - Average path length
  - Triangle counts
- Apply algorithms:
  - PageRank
  - Connected Components
- Join and transform graphs quickly
- Support for the Pregel API for graph traversal

#### Core Concepts:

1. VertexRDD: Distributed collection of vertices
2. EdgeRDD: Distributed collection of edges
3. Edge: Data type representing connections between vertices

### Creating a Graph

#### Creating Vertex RDD:

```scala
def parseNames(line: String): Option[(VertexId, String)] = {
  val fields = line.split('\t')
  if (fields.length > 1) {
    val heroId: Long = fields(0).trim.toLong
    if (heroId < 6487) {
      Some((heroId, fields(1)))
    } else {
      None
    }
  } else {
    None
  }
}

val names = spark.sparkContext.textFile("../marvel-names.txt")
val verts = names.flatMap(parseNames)
```

#### Creating Edge RDD:

```scala
def makeEdges(line: String): List[Edge[Int]] = {
  val fields = line.split(" ")
  val origin = fields(0).toLong
  fields.tail.map(x => Edge(origin, x.toLong, 0)).toList
}

val lines = spark.sparkContext.textFile("../marvel-graph.txt")
val edges = lines.flatMap(makeEdges)
```

#### Building the Graph:

```scala
import org.apache.spark.graphx._

val default = "Nobody"
val graph = Graph(verts, edges, default).cache()
```

#### Example Operation:

```scala
// Find top 10 characters with most connections
graph.degrees.join(verts).sortBy(_._2._1, ascending = false).take(10).foreach(println)
```

### Using the Pregel API with Spark GraphX

Pregel is a bulk-synchronous message-passing abstraction for graph processing.

#### How Pregel Works:

1. Vertices send messages to other vertices (their neighbors)
2. Processing occurs in iterations called "supersteps"
3. Each superstep:
   - Receives messages from the previous iteration
   - Runs a program to transform itself
   - Sends messages to other vertices

### Activity: Superhero Degrees of Separation using GraphX

This activity demonstrates how to use Pregel to find the shortest path between two nodes in a graph.

#### Initialization:

```scala
val initialGraph = graph.mapVertices((id, _) => 
  if (id == startingCharacterId) 0.0 else Double.PositiveInfinity
)
```

#### Message Sending:

```scala
def sendMessage(edge: EdgeTriplet[Double, Int]): Iterator[(VertexId, Double)] = {
  if (edge.srcAttr != Double.PositiveInfinity) {
    Iterator((edge.dstId, edge.srcAttr + 1))
  } else {
    Iterator.empty
  }
}
```

#### Vertex Program:

```scala
def vertexProgram(id: VertexId, attr: Double, msg: Double): Double = {
  math.min(attr, msg)
}
```

#### Putting it all together:

```scala
val result = initialGraph.pregel(
  Double.PositiveInfinity,
  Int.MaxValue,
  EdgeDirection.Out
)(
  vertexProgram,
  sendMessage,
  (a, b) => math.min(a, b)
)
```

### GraphFrames: The Future of Graph Processing in Spark

As of 2024, GraphFrames has become the recommended library for graph processing in Spark. It's built on top of DataFrames and provides a more intuitive API.

#### Key Advantages of GraphFrames:

1. Integration with Spark SQL and DataFrames
2. Support for both attribute and structural queries
3. Motif finding for complex structural patterns
4. More efficient implementation of common algorithms

#### Example Usage:

```scala
import org.graphframes._

// Assuming vertices and edges are DataFrames
val g = GraphFrame(vertices, edges)

// PageRank
val results = g.pageRank.resetProbability(0.15).maxIter(10).run()

// Shortest paths
val paths = g.shortestPaths.landmarks(Seq("A", "D")).run()
```

### Best Practices for Graph Processing in Spark (2024):

1. Use GraphFrames for new projects when possible
2. Optimize graph partitioning for large-scale graphs
3. Use caching judiciously, especially for iterative algorithms
4. Consider using cluster managers like Kubernetes for resource allocation
5. Utilize Spark 3.x features like adaptive query execution for better performance

### Quiz: Spark GraphX

1. Q: What is the primary advantage of using GraphFrames over GraphX?
   A: GraphFrames integrates better with Spark SQL and DataFrames, providing a more unified and efficient API for graph processing.

2. Q: In Pregel, what is a "superstep"?
   A: A superstep is a single iteration in the Pregel computation model where vertices receive messages, compute, and send new messages.

3. Q: How does caching affect GraphX performance?
   A: Caching can significantly improve performance for iterative algorithms by keeping the graph structure in memory, reducing I/O operations.