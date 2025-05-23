package exemples

import spark.compiletime.*
import org.apache.spark.sql.{SparkSession, Dataset}

// Example 1: Weather Data Analysis

// In this example, we'll set up a simple weather data analysis system:

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

// Use the SQL query that's validated at compile-time
val hotDaysQuery = weatherDb.sql("""
  SELECT s.name, r.timestamp, r.temperature
  FROM weather_stations s
  JOIN weather_readings r ON s.id = r.station_id
  WHERE r.temperature > 30.0
  ORDER BY r.temperature DESC
""")
