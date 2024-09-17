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
