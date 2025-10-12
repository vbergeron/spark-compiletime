package spark.compiletime
import scala.quoted.*
import mirrors.TableMirror
import mirrors.CatalogMirror
import org.apache.spark.sql.*
import org.apache.spark.sql.streaming.*
import spark.compiletime.macros.createTable

// Table creation helpers
private transparent inline def createTableImpl[C <: CatalogMirror](inline sql: String): TableMirror =
  ${ macros.createTable[C]('sql) }

private transparent inline def createTableVerboseImpl[C <: CatalogMirror](inline sql: String): TableMirror =
  ${ macros.createTableVrebose[C]('sql) }

private transparent inline def createTable[C <: CatalogMirror](inline sql: String)(using inline logPlan: LogPlan): TableMirror =
  inline logPlan match
    case LogPlan.Yes => createTableVerboseImpl[C](sql)
    case LogPlan.No  => createTableImpl[C](sql)

transparent inline def table(inline sql: String)(using inline logPlan: LogPlan): TableMirror =
  createTable[CatalogMirror.Empty](sql)(using logPlan)

// SQL check helpers
private transparent inline def checkSQLImpl[C <: CatalogMirror](inline sql: String): String =
  ${ macros.checkSQL[C]('sql) }

private transparent inline def checkSQLVerboseImpl[C <: CatalogMirror](inline sql: String): String =
  ${ macros.checkSQLVerbose[C]('sql) }

private transparent inline def checkSQL[C <: CatalogMirror](inline sql: String)(using inline logPlan: LogPlan): String =
  inline logPlan match
    case LogPlan.Yes => checkSQLVerboseImpl[C](sql)
    case LogPlan.No  => checkSQLImpl[C](sql)

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
  inline def sql(inline sql: String)(using inline logPlan: LogPlan): String =
    checkSQL[db.type](sql)(using logPlan)

  transparent inline def table(inline sql: String)(using inline logPlan: LogPlan): TableMirror =
    createTable[db.type](sql)(using logPlan)

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
