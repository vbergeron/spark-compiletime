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

private transparent inline def createTable[C <: CatalogMirror](inline sql: String)(using inline showPlans: ShowPlans): TableMirror =
  inline showPlans match
    case ShowPlans.Yes => createTableVerboseImpl[C](sql)
    case ShowPlans.No  => createTableImpl[C](sql)

// SQL check helpers
private transparent inline def checkSQLImpl[C <: CatalogMirror](inline sql: String): String =
  ${ macros.checkSQL[C]('sql) }

private transparent inline def checkSQLVerboseImpl[C <: CatalogMirror](inline sql: String): String =
  ${ macros.checkSQLVerbose[C]('sql) }

private transparent inline def checkSQL[C <: CatalogMirror](inline sql: String)(using inline showPlans: ShowPlans): String =
  inline showPlans match
    case ShowPlans.Yes => checkSQLVerboseImpl[C](sql)
    case ShowPlans.No  => checkSQLImpl[C](sql)

// SQL parse helpers
private transparent inline def parseSQLImpl(inline sql: String): Unit =
  ${ macros.parseSQL('sql) }

private transparent inline def parseSQLVerboseImpl(inline sql: String): Unit =
  ${ macros.parseSQL('sql) }

private transparent inline def parseSQL(inline sql: String)(using inline showPlans: ShowPlans): Unit =
  inline showPlans match
    case ShowPlans.Yes => parseSQLVerboseImpl(sql)
    case ShowPlans.No  => parseSQLImpl(sql)

// Public API
transparent inline def table(inline sql: String)(using inline showPlans: ShowPlans): TableMirror =
  createTable[CatalogMirror.Empty](sql)(using showPlans)

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
  inline def sql(inline sql: String)(using inline showPlans: ShowPlans): String =
    checkSQL[db.type](sql)(using showPlans)

  transparent inline def table(inline sql: String)(using inline showPlans: ShowPlans): TableMirror =
    createTable[db.type](sql)(using showPlans)

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
