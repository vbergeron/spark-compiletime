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

class CompiletimeCatalog extends TableCatalog with SupportsNamespaces {

  // Storage
  private var catalogName: String = _
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

case class CompiletimeTable(db: String, name: String, schema: String)

object CompiletimeTable {
  given FromExpr[CompiletimeTable] = new FromExpr[CompiletimeTable] {
    def unapply(x: Expr[CompiletimeTable])(using Quotes): Option[CompiletimeTable] =
      x match
        case '{ CompiletimeTable($db, $name, $schema) } =>
          Some(CompiletimeTable(db.valueOrAbort, name.valueOrAbort, schema.valueOrAbort))

        case _ => None
  }
}

case class CompiletimeDatabase(tables: CompiletimeTable*)

object CompiletimeDatabase {
  given FromExpr[CompiletimeDatabase] = new FromExpr[CompiletimeDatabase] {
    def unapply(x: Expr[CompiletimeDatabase])(using Quotes): Option[CompiletimeDatabase] =
      x match
        case '{ CompiletimeDatabase(${ Varargs[CompiletimeTable](tables) }*) } =>
          Some(CompiletimeDatabase(tables.map(_.valueOrAbort)*))

        case _ => None
  }

}

def checkSQLImpl(databaseExpr: Expr[CompiletimeDatabase], sqlExpr: Expr[String])(using Quotes): Expr[String] =
  import quotes.reflect.*
  val sql      = sqlExpr.valueOrAbort
  val database = databaseExpr.valueOrAbort

  val plan =
    try CatalystSqlParser.parsePlan(sql)
    catch case error => report.errorAndAbort(error.getMessage)

  val catalog = new CompiletimeCatalog()
  catalog.initialize("compiletime", CaseInsensitiveStringMap.empty())

  database.tables.foreach: table =>
    val schema =
      try StructType.fromDDL(table.schema)
      catch case error => report.errorAndAbort(error.getMessage)

    catalog.addTable(table.db, table.name, schema)

  val analyser = Analyzer(CatalogManager(catalog, SessionCatalog(InMemoryCatalog())))

  val tracker = QueryPlanningTracker()

  try
    val resolved = analyser.executeAndCheck(plan, tracker)
    report.info(resolved.toString)
  catch case error => report.errorAndAbort(error.getMessage)

  sqlExpr

inline def checkSQL(inline database: CompiletimeDatabase, inline query: String): String =
  ${ checkSQLImpl('database, 'query) }

extension (inline database: CompiletimeDatabase) //
  inline def sql(inline query: String): String = checkSQL(database, query)
