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

private def createTableMirrorImpl(sqlExpr: Expr[String])(using Quotes): Expr[TableMirror] =
  import quotes.reflect.*
  val sql  = sqlExpr.valueOrAbort
  val plan =
    try CatalystSqlParser.parsePlan(sql)
    catch case error => report.errorAndAbort(error.getMessage)

  val create = plan match
    case node: CreateTable => node
    case _                 => report.errorAndAbort("Not a CreateTable statement")

  val names = create.name match
    case node: UnresolvedIdentifier => node
    case _                          => report.errorAndAbort("Not a UnresolvedIdentifier")

  val name = names.nameParts match
    case Seq(table) => table
    case _          => report.errorAndAbort("Only non-namespaced table name are supported")

  val nameType   = ConstantType(StringConstant(name)).asType
  val schemaType = ConstantType(StringConstant(create.tableSchema.toDDL)).asType

  (nameType, schemaType) match
    case ('[name], '[schema]) =>
      '{
        new TableMirror {
          type DB     = "default"
          type Name   = name & String
          type Schema = schema & String
        }
      }
    case unreachable          =>
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

private def showparseImpl(sql: Expr[String])(using Quotes): Expr[Unit] =
  import quotes.reflect.*
  val sqlString = sql.valueOrAbort
  val plan      =
    try CatalystSqlParser.parsePlan(sqlString)
    catch case error => report.errorAndAbort(error.getMessage)
  report.info(plan.toString)
  '{ () }

inline def showparse(inline sql: String): Unit =
  ${ showparseImpl('sql) }
