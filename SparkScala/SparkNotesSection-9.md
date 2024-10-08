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