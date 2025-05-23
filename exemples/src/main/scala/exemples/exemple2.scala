package exemples

import spark.compiletime.*

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
