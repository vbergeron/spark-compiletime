package spark.compiletime
package macros
import mirrors.*
import scala.quoted.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.compiletime.CompiletimeCatalog
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.catalyst.plans.logical.V2CreateTablePlan
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameWriterV2
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.m
import spark.compiletime.encoders.encoderOf
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.compiletime.CompiletimeOptimizer

private def parsePlan(sqlExpr: Expr[String])(using Quotes): LogicalPlan =
  import quotes.reflect.*
  val sql = sqlExpr.valueOrAbort
  try CatalystSqlParser.parsePlan(sql)
  catch
    case error =>
      report.errorAndAbort("[Parser] : " + error.getMessage)

private def parseAndAnalysePlan[DB <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[DB]): LogicalPlan =
  import quotes.reflect.*
  val plan = parsePlan(sqlExpr)

  val catalog = CompiletimeCatalog("compiletime")

  val tables = spark.compiletime.mirrors.macros.tablesImpl[DB].valueOrAbort

  tables.foreach: (db, table, schema) =>
    val parsed =
      try StructType.fromDDL(schema)
      catch case error => report.errorAndAbort("[StructType] : " + error.getMessage)

    catalog.addTable(db, table, parsed)

  val tracker = QueryPlanningTracker()

  val manager = catalog.manager
  manager.setCurrentNamespace(Array("default"))

  val analyzer = Analyzer(manager)

  val analyzed =
    try analyzer.executeAndCheck(plan, tracker)
    catch case error => report.errorAndAbort("[Analyzer] : " + error.getMessage)

  val optimizer = CompiletimeOptimizer(manager)

  try optimizer.execute(analyzed)
  catch case error => report.errorAndAbort("[Optimizer] : " + error.getStackTrace().toList)

def parseSQL(sqlExpr: Expr[String])(using Quotes): Expr[Unit] =
  import quotes.reflect.*
  val plan = parsePlan(sqlExpr)
  report.info(plan.toString)
  '{ () }

def checkSQL[Catalog <: CatalogMirror](sqlExpr: Expr[String], shouldLogPlan: Boolean)(using Quotes, Type[Catalog]): Expr[String] =
  import quotes.reflect.*
  val resolved = parseAndAnalysePlan[Catalog](sqlExpr)
  if shouldLogPlan then report.info(resolved.toString)
  sqlExpr

def checkSQLVerbose[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[String] =
  checkSQL[Catalog](sqlExpr, true)

def checkSQL[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[String] =
  checkSQL[Catalog](sqlExpr, false)

def createTableVrebose[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[TableMirror] =
  createTable[Catalog](sqlExpr, true)

def createTable[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[TableMirror] =
  createTable[Catalog](sqlExpr, false)

def createTable[Catalog <: CatalogMirror](sqlExpr: Expr[String], shouldLogPlan: Boolean)(using Quotes, Type[Catalog]): Expr[TableMirror] =
  import quotes.reflect.*

  val sql  = sqlExpr.valueOrAbort
  val plan = parseAndAnalysePlan[Catalog](sqlExpr)
  if shouldLogPlan then report.info(plan.toString)

  val (tableName, tableSchema) = plan match
    case node: V2CreateTablePlan => (node.tableName, node.tableSchema)
    case unexpected              =>
      report.errorAndAbort(s"Not a table creation statement, got $unexpected")

  val dbType     = utils.typeFromString(tableName.namespace().mkString("."))
  val nameType   = utils.typeFromString(tableName.name())
  val schemaType = utils.typeFromString(tableSchema.toDDL)
  val queryType  = utils.typeFromString(sql)

  (dbType, nameType, schemaType, queryType) match
    case ('[db], '[name], '[schema], '[query]) =>
      '{
        new TableMirror {
          type DB     = db & String
          type Name   = name & String
          type Schema = schema & String
          type Query  = query & String
        }
      }
    case unreachable                           =>
      report.errorAndAbort(s"Unexpected types: $unreachable")

def createCatalog[T <: Tuple](using Quotes, Type[T]): Expr[CatalogMirror] =
  import quotes.reflect.*
  val types = utils.typesFromTuple[T]
  types.foreach:
    case '[t] if utils.subtypeOf[t, TableMirror] => ()
    case '[t]                                    =>
      report.errorAndAbort(s"Expected all catalog member to be an instance of TableMirror type but got ${Type.show[t]}")
  '{ new CatalogMirror { type Tables = T } }

def createCatalogVarargs(using Quotes)(tablesExpr: Expr[Seq[TableMirror]]): Expr[CatalogMirror] =
  import quotes.reflect.*

  val types = utils.varargsOf(tablesExpr).map(_.asTerm.tpe.asType)

  utils.tupleFromTypes(types) match
    case '[tables] => '{ new CatalogMirror { type Tables = tables & Tuple } }
