package spark.compiletime
import scala.quoted.*
import mirrors.TableMirror
import mirrors.CatalogMirror
import org.apache.spark.sql.*
import org.apache.spark.sql.streaming.*

transparent inline def table(inline sql: String): TableMirror =
  ${ macros.createTable[CatalogMirror.Empty]('sql) }

object catalog:
  val empty: CatalogMirror =
    new CatalogMirror { type Tables = EmptyTuple }

  private transparent inline def varargs(inline tables: TableMirror*): CatalogMirror =
    ${ macros.createCatalogVarargs('tables) }

  transparent inline def apply(inline table: TableMirror): CatalogMirror =
    varargs(table)

  transparent inline def apply(inline tables: TableMirror*): CatalogMirror =
    varargs(tables*)

extension (db: CatalogMirror)
  inline def sql(inline sql: String): String =
    ${ macros.checkSQL[db.type]('sql) }

  transparent inline def table(inline sql: String): TableMirror =
    ${ macros.createTable[db.type]('sql) }

  transparent inline def add(table: TableMirror): CatalogMirror =
    db.asInstanceOf[CatalogMirror { type Tables = table.type *: db.Tables }]

  transparent inline def add(inline sql: String): CatalogMirror =
    db.add(table(sql))

inline def parseSQL(inline sql: String): Unit =
  ${ macros.parseSQL('sql) }

extension (spark: SparkSession)
  inline def reader(table: TableMirror): DataFrameReader =
    spark.read.schema(table.schema)

  inline def streamReader(table: TableMirror): DataStreamReader =
    spark.readStream.schema(table.schema)

  inline def sql(catalog: CatalogMirror)(inline sql: String): DataFrame =
    spark.sql(catalog.sql(sql))
