package org.apache.spark.sql.compiletime
import scala.quoted.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.catalog.*
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.*
import java.net.URI
import org.apache.spark.sql.catalyst.analysis.*
import org.apache.spark.sql.connector.catalog.*
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.SparkSession
import org.apache.parquet.format.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.m

class CompiletimeCatalog extends TableCatalog with SupportsNamespaces {

  // Storage
  private var catalogName: String = compiletime.uninitialized
  private var namespaces          = Set(Array("default"))
  private var views               = Map.empty[Identifier, StructType]

  // Custom part
  def addTable(db: String, name: String, schema: StructType): Unit = {
    val ident = Identifier.of(Array(db), name)
    views += (ident -> schema)
    namespaces += Array(db)
  }

  // implements TableCatalog & SupportsNamespaces
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] =
    views.keys.filter(_.namespace().sameElements(namespace)).toArray

  override def loadTable(ident: Identifier): Table = {
    views
      .get(ident)
      .map { s =>
        new Table {
          override def name(): String = ident.name()

          override def schema(): StructType = s

          override def capabilities(): java.util.Set[TableCapability] = java.util.Collections.emptySet()
        }
      }
      .getOrElse(throw new NoSuchTableException(ident.toString))
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
    if (views.contains(ident)) throw new TableAlreadyExistsException(ident.toString)
    views += (ident -> schema)
    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("alterTable not supported")

  override def dropTable(ident: Identifier): Boolean = {
    val existed = views.contains(ident)
    views -= ident
    existed
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (!views.contains(oldIdent)) throw new NoSuchTableException(oldIdent.toString)
    if (views.contains(newIdent)) throw new TableAlreadyExistsException(newIdent.toString)
    views += (newIdent -> views(oldIdent))
    views -= oldIdent
  }

  // --- Namespace operations ---

  override def listNamespaces(): Array[Array[String]] =
    namespaces.toArray

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    if (namespaces.exists(_.sameElements(namespace))) Array.empty
    else throw new NoSuchNamespaceException(namespace.mkString("."))

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] =
    if (namespaces.exists(_.sameElements(namespace))) new java.util.HashMap[String, String]()
    else throw new NoSuchNamespaceException(namespace.mkString("."))

  override def createNamespace(namespace: Array[String], metadata: java.util.Map[String, String]): Unit =
    namespaces += namespace

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException("alterNamespace not supported")

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    val existed = namespaces.exists(_.sameElements(namespace))
    namespaces = namespaces.filterNot(_.sameElements(namespace))
    views = views.filterNot(_._1.namespace().sameElements(namespace))
    existed
  }
}

trait TableMirror:
  type DB <: String
  type Name <: String
  type Schema <: String

object TableMirror:

  inline def db[T]: String =
    ${ spark.macros.dbImpl[T] }

  inline def name[T]: String =
    ${ spark.macros.nameImpl[T] }

  inline def schema[T]: String =
    ${ spark.macros.schemaImpl[T] }

  inline def table[T]: (String, String, String) =
    ${ spark.macros.tableImpl[T] }

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

transparent inline def table(inline sql: String): TableMirror =
  ${ createTableMirrorImpl('sql) }

trait DatabaseMirror:
  type Tables <: Tuple

object DatabaseMirror:

  inline def tables[T <: DatabaseMirror]: List[(String, String, String)] =
    ${ spark.macros.tablesImpl[T] }

def createDatabaseMirrorImpl[T <: Tuple](using Quotes, Type[T]): Expr[DatabaseMirror] =
  '{ new DatabaseMirror { type Tables = T } }

transparent inline def oneTable(table: TableMirror): DatabaseMirror =
  ${ createDatabaseMirrorImpl[table.type *: EmptyTuple] }

transparent inline def database[tables <: Tuple](tables: tables): DatabaseMirror =
  ${ createDatabaseMirrorImpl[tables] }

def checkSQLImpl[DB <: DatabaseMirror](sqlExpr: Expr[String])(using Quotes, Type[DB]): Expr[String] =
  import quotes.reflect.*
  val sql = sqlExpr.valueOrAbort

  val plan =
    try CatalystSqlParser.parsePlan(sql)
    catch case error => report.errorAndAbort(error.getMessage)

  val catalog = new CompiletimeCatalog()
  catalog.initialize("compiletime", CaseInsensitiveStringMap.empty())

  val tables = spark.macros.tablesImpl[DB].valueOrAbort

  tables.foreach: (db, table, schema) =>
    val parsed =
      try StructType.fromDDL(schema)
      catch case error => report.errorAndAbort(error.getMessage)

    catalog.addTable(db, table, parsed)

  val analyser = Analyzer(CatalogManager(catalog, SessionCatalog(InMemoryCatalog())))

  val tracker = QueryPlanningTracker()

  val resolved =
    try analyser.executeAndCheck(plan, tracker)
    catch case error => report.errorAndAbort(error.getMessage)

  report.info(resolved.toString)

  sqlExpr

inline def checkSQL[DB <: DatabaseMirror](inline sql: String): String =
  ${ checkSQLImpl[DB]('sql) }

extension [T <: DatabaseMirror](db: T)
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
