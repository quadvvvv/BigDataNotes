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

