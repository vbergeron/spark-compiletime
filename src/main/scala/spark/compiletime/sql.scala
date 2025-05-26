package spark.compiletime
import scala.quoted.*
import mirrors.TableMirror
import mirrors.CatalogMirror
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.compiletime.CompiletimeCatalog
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.Analyzer
import spark.compiletime.utils.typeFromString

private def createTableMirrorImpl(sqlExpr: Expr[String])(using Quotes): Expr[TableMirror] =
  import quotes.reflect.*
  val sql  = sqlExpr.valueOrAbort
  val plan =
    try CatalystSqlParser.parsePlan(sql)
    catch case error => report.errorAndAbort(error.getMessage)

  val create = plan match
    case node: CreateTable => node
    case unexpected        =>
      report.errorAndAbort(s"Not a CreateTable statement, got $unexpected")

  val names = create.name match
    case node: UnresolvedIdentifier => node
    case unexpected                 =>
      report.errorAndAbort(s"Expected the table name to not be resolved, got $unexpected")

  val name = names.nameParts match
    case Seq(table) => table
    case unexpected =>
      report.errorAndAbort(s"Only non-namespaced table name are supporte, got $unexpected")

  val nameType   = typeFromString(name)
  val schemaType = typeFromString(create.tableSchema.toDDL)
  val queryType  = typeFromString(sql)

  (nameType, schemaType, queryType) match
    case ('[name], '[schema], '[query]) =>
      '{
        new TableMirror {
          type DB     = "default"
          type Name   = name & String
          type Schema = schema & String
          type Query  = query & String
        }
      }
    case unreachable                    =>
      report.errorAndAbort(s"Unexpected types: $unreachable")

transparent inline def table(inline sql: String): TableMirror =
  ${ createTableMirrorImpl('sql) }

def createCatalogMirrorImpl[T <: Tuple](using Quotes, Type[T]): Expr[CatalogMirror] =
  '{ new CatalogMirror { type Tables = T } }

transparent inline def catalog(table: TableMirror): CatalogMirror =
  ${ createCatalogMirrorImpl[table.type *: EmptyTuple] }

transparent inline def catalog[tables <: Tuple](tables: tables): CatalogMirror =
  ${ createCatalogMirrorImpl[tables] }

def checkSQLImpl[DB <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[DB]): Expr[String] =
  import quotes.reflect.*
  val sql = sqlExpr.valueOrAbort

  val plan =
    try CatalystSqlParser.parsePlan(sql)
    catch case error => report.errorAndAbort(error.getMessage)

  val catalog = new CompiletimeCatalog()
  catalog.initialize("compiletime", CaseInsensitiveStringMap.empty())

  val tables = spark.compiletime.mirrors.macros.tablesImpl[DB].valueOrAbort

  tables.foreach: (db, table, schema) =>
    val parsed =
      try StructType.fromDDL(schema)
      catch case error => report.errorAndAbort(error.getMessage)

    catalog.addTable(db, table, parsed)

  val tracker = QueryPlanningTracker()

  val manager = catalog.manager
  manager.setCurrentNamespace(Array("default"))

  val analyzer = Analyzer(manager)

  try
    val resolved = analyzer.executeAndCheck(plan, tracker)
    report.info(resolved.toString)
  catch case error => report.errorAndAbort(error.getMessage)

  sqlExpr

inline def checkSQL[DB <: CatalogMirror](inline sql: String): String =
  ${ checkSQLImpl[DB]('sql) }

extension [T <: CatalogMirror](db: T)
  inline def sql(inline sql: String): String =
    ${ checkSQLImpl[T]('sql) }

private def parseImpl(sql: Expr[String])(using Quotes): Expr[Unit] =
  import quotes.reflect.*
  val sqlString = sql.valueOrAbort
  val plan      =
    try CatalystSqlParser.parsePlan(sqlString)
    catch case error => report.errorAndAbort(error.getMessage)
  report.info(plan.toString)
  '{ () }

inline def parse(inline sql: String): Unit =
  ${ parseImpl('sql) }
