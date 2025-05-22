# Spark Compile-time

A compile-time validation library for Apache Spark SQL that enables type-safe SQL operations, schema validation, and optimized encoders for Scala 3 applications.

## Features

### 1. Compile-time SQL Validation

Validate your Spark SQL queries during compilation, eliminating runtime SQL syntax or schema errors:
- Catches schema mismatches and column type errors at compile time
- Validates column existence and join compatibility 
- Prevents common SQL errors before your code even runs

### 2. Type-safe Table and Catalog Definitions

Define your tables with schema information that travels with your code:
- Create table definitions directly from SQL CREATE TABLE statements
- Build catalogs of multiple tables to validate complex queries
- Leverage the Scala 3 type system to ensure schema consistency

### 3. Optimized Encoders

Generate efficient Spark encoders without runtime reflection:
- Achieve better performance with compile-time generated encoders
- Support for a wide range of Scala types (primitives, collections, case classes)
- Fully compatible with Spark's Dataset API

## Getting Started

### Installation

Add the following dependency to your `build.sbt`:

```scala
libraryDependencies += "org.spark-compiletime" %% "spark-compiletime" % "0.1.0-SNAPSHOT"
```

### Basic Usage

```scala
import spark.compiletime._

// Define tables with their schemas
val products = table("""
  CREATE TABLE products (
    id INT,
    name STRING,
    category STRING,
    price DECIMAL(10,2),
    in_stock BOOLEAN
  )
""")

val reviews = table("""
  CREATE TABLE reviews (
    id INT,
    product_id INT,
    rating INT,
    comment STRING,
    user_id INT
  )
""")

// Create a catalog with all your tables
val db = catalog(products, reviews)

// Write SQL queries that will be validated at compile time
val validQuery = db.sql("""
  SELECT p.name, AVG(r.rating) as avg_rating
  FROM products p
  JOIN reviews r ON p.id = r.product_id
  GROUP BY p.name
  HAVING AVG(r.rating) > 4.0
""")

// This would fail at compile time with a helpful error:
// db.sql("SELECT nonexistent_column FROM products")
```

## Examples

### Example 1: Weather Data Analysis

In this example, we'll set up a simple weather data analysis system:

```scala
import spark.compiletime._
import org.apache.spark.sql.{SparkSession, Dataset}
import spark.compiletime.encoders._

// Define our data model with case classes
case class WeatherStation(id: Int, name: String, latitude: Double, longitude: Double)
case class WeatherReading(station_id: Int, timestamp: java.sql.Timestamp, temperature: Double, humidity: Double, pressure: Double)

// Define our tables
val stations = table("""
  CREATE TABLE weather_stations (
    id INT, 
    name STRING,
    latitude DOUBLE,
    longitude DOUBLE
  )
""")

val readings = table("""
  CREATE TABLE weather_readings (
    station_id INT,
    timestamp TIMESTAMP,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE
  )
""")

// Create a catalog with all tables
val weatherDb = catalog(stations, readings)

// Initialize Spark
val spark = SparkSession.builder().appName("Weather Analysis").getOrCreate()
import spark.implicits._

// Use the SQL query that's validated at compile-time
val hotDaysQuery = weatherDb.sql("""
  SELECT s.name, r.timestamp, r.temperature
  FROM weather_stations s
  JOIN weather_readings r ON s.id = r.station_id
  WHERE r.temperature > 30.0
  ORDER BY r.temperature DESC
""")

// Create datasets with our compile-time generated encoders
val stationsDS: Dataset[WeatherStation] = spark.read.parquet("path/to/stations").as(encoderOf[WeatherStation])
val readingsDS: Dataset[WeatherReading] = spark.read.parquet("path/to/readings").as(encoderOf[WeatherReading])

// Execute the query
val hotDaysResults = spark.sql(hotDaysQuery)
```

### Example 2: Advanced Gaming Analytics Platform

Here's a more complex example with multiple tables:

```scala
import spark.compiletime._
import org.apache.spark.sql.SparkSession

// Define tables for a gaming analytics platform
val games = table("""
  CREATE TABLE games (
    game_id INT,
    title STRING,
    genre STRING,
    release_date DATE,
    developer STRING,
    publisher STRING
  )
""")

val players = table("""
  CREATE TABLE players (
    player_id INT,
    username STRING,
    signup_date DATE,
    country STRING,
    platform STRING
  )
""")

val sessions = table("""
  CREATE TABLE game_sessions (
    session_id STRING,
    game_id INT,
    player_id INT,
    start_time TIMESTAMP,
    duration_minutes INT,
    completed BOOLEAN
  )
""")

val purchases = table("""
  CREATE TABLE purchases (
    purchase_id STRING,
    player_id INT,
    game_id INT,
    amount DECIMAL(10,2),
    currency STRING,
    purchase_date TIMESTAMP
  )
""")

val items = table("""
  CREATE TABLE in_game_items (
    item_id INT,
    game_id INT,
    name STRING,
    type STRING,
    rarity STRING,
    price DECIMAL(10,2)
  )
""")

val itemPurchases = table("""
  CREATE TABLE item_purchases (
    transaction_id STRING,
    player_id INT,
    item_id INT,
    purchase_date TIMESTAMP,
    price_paid DECIMAL(10,2)
  )
""")

// Create a catalog with all tables
val gameDb = catalog(games, players, sessions, purchases, items, itemPurchases)

// Complex query with multiple joins, aggregations, and window functions that's validated at compile time
val complexAnalysisQuery = gameDb.sql("""
  SELECT 
    g.title,
    g.genre,
    COUNT(DISTINCT p.player_id) as unique_players,
    SUM(s.duration_minutes) as total_playtime,
    AVG(s.duration_minutes) as avg_session_length,
    SUM(pu.amount) as total_revenue,
    COUNT(ip.transaction_id) as total_item_purchases,
    AVG(ip.price_paid) as avg_item_price
  FROM games g
  LEFT JOIN game_sessions s ON g.game_id = s.game_id
  LEFT JOIN players p ON s.player_id = p.player_id
  LEFT JOIN purchases pu ON g.game_id = pu.game_id
  LEFT JOIN item_purchases ip ON p.player_id = ip.player_id
  WHERE g.release_date > DATE '2022-01-01'
  AND p.country IN ('US', 'CA', 'UK', 'FR', 'DE')
  GROUP BY g.title, g.genre
  HAVING COUNT(DISTINCT p.player_id) > 1000
  ORDER BY total_revenue DESC, unique_players DESC
""")

// Execute the query
val spark = SparkSession.builder().appName("Gaming Analytics").getOrCreate()
val results = spark.sql(complexAnalysisQuery)
```

