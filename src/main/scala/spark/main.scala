package spark

import org.apache.spark.sql.compiletime.CompiletimeTable
import org.apache.spark.sql.types.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.compiletime.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

@main
def run =
  inline def users   = CompiletimeTable("default", "users", "name STRING, age INT")
  inline def foobar  = CompiletimeTable("default", "foobar", "foo INT, age INT")
  inline def default = CompiletimeDatabase(users, foobar)

  default.sql("select name, age from default.users")
