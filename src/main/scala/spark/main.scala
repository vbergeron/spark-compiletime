package spark

import org.apache.spark.sql.compiletime.*
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

@main
def run =

  val users = table("create table if not exists users (name STRING, age ARRAY<INT>)")

  val db = oneTable(users)

  db.sql("select age from default.users")
