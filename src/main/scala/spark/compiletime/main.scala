package spark.compiletime

@main
def run =

  val users     = table("create table if not exists users (name STRING, age ARRAY<INT>)")
  val employees = table("create table if not exists employees (name STRING, age INT)")

  val db = catalog(users)

  db.sql("select name from users")
