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

