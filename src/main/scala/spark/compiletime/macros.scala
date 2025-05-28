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

private def parsePlan(sqlExpr: Expr[String])(using Quotes): LogicalPlan =
  import quotes.reflect.*
  val sql = sqlExpr.valueOrAbort
  try CatalystSqlParser.parsePlan(sql)
  catch
    case error =>
      report.errorAndAbort(error.getMessage)

private def parseAndAnalysePlan[DB <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[DB]): LogicalPlan =
  import quotes.reflect.*
  val plan = parsePlan(sqlExpr)

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

  try analyzer.executeAndCheck(plan, tracker)
  catch case error => report.errorAndAbort(error.getMessage)

def parseSQL(sqlExpr: Expr[String])(using Quotes): Expr[Unit] =
  import quotes.reflect.*
  val plan = parsePlan(sqlExpr)
  report.info(plan.toString)
  '{ () }

def checkSQL[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[String] =
  import quotes.reflect.*
  val resolved = parseAndAnalysePlan[Catalog](sqlExpr)
  report.info(resolved.toString)
  sqlExpr

def createTable[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[TableMirror] =
  import quotes.reflect.*
  val sql  = sqlExpr.valueOrAbort
  val plan = parseAndAnalysePlan[Catalog](sqlExpr)
  report.info(plan.toString)

  val create = plan match
    case node: CreateTable         => node
    case node: CreateTableAsSelect => node
    case unexpected                =>
      report.errorAndAbort(s"Not a CreateTable statement, got $unexpected")

  val identifier = create.name match
    case node: UnresolvedIdentifier => Identifier.of(node.nameParts.init.toArray, node.nameParts.last)
    case node: ResolvedIdentifier   => node.identifier
    case unexpected                 =>
      report.errorAndAbort(s"Expected identifier, got $unexpected")

  val name = identifier.name()
  // names.nameParts match
  // case Seq(table) => table
  // case unexpected =>
  //  report.errorAndAbort(s"Only non-namespaced table name are supporte, got $unexpected")

  val nameType   = utils.typeFromString(name)
  val schemaType = utils.typeFromString(create.tableSchema.toDDL)
  val queryType  = utils.typeFromString(sql)

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

def createCatalog[T <: Tuple](using Quotes, Type[T]): Expr[CatalogMirror] =
  import quotes.reflect.*
  val types = utils.typesFromTuple[T]
  types.foreach:
    case '[t] if utils.subtypeOf[t, TableMirror] => ()
    case '[t]                                    =>
      report.errorAndAbort(s"Expected all catalog member to be an instance of TableMirror type but got ${Type.show[t]}")
  '{ new CatalogMirror { type Tables = T } }

def appendTableToCatalog[Catalog <: CatalogMirror](table: Expr[TableMirror])(using Quotes, Type[Catalog]): Expr[CatalogMirror] =
  table match
    case '{ $m: TableMirror } =>
      Type.of[Catalog] match
        case '[Catalog { type Tables = tables }] =>
          '{
            val mi = $m
            new CatalogMirror {
              type Tables = tables *: mi.type *: EmptyTuple
            }
          }

def appendTableSQLToCatalog[Catalog <: CatalogMirror](sqlExpr: Expr[String])(using Quotes, Type[Catalog]): Expr[CatalogMirror] =
  appendTableToCatalog(createTable[Catalog](sqlExpr))
