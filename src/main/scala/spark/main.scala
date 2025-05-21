package spark

import org.apache.spark.sql.compiletime.CompiletimeTable
import org.apache.spark.sql.types.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.compiletime.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

@main
def run =
  inline def users = CompiletimeTable("default", "users", "name STRING, age INT")
  checkSQL(users, "select name from default.users")
