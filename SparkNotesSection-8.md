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

