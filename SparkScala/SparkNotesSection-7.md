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


