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

