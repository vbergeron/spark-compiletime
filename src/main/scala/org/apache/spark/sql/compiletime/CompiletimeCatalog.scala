package org.apache.spark.sql.compiletime

import org.apache.spark.sql.connector.catalog.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.BuiltInFunctionCatalog
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.internal.connector.V1Function
import java.net.URI

class CompiletimeCatalog(val name: String) extends TableCatalog, SupportsNamespaces {

  // Storage
  private var namespaces = Set(Array("default"))
  private var views      = Map.empty[Identifier, StructType]

  // Custom part
  def addTable(db: String, name: String, schema: StructType): Unit = {
    val ident = Identifier.of(Array(db), name)
    views += (ident -> schema)
    namespaces += Array(db)
  }

  def addTable(table: CompiletimeTable): Unit = {
    val ident = table.ident
    views = views.updated(ident, table.schema())
    namespaces += ident.namespace()
  }

  def manager = CatalogManager(this, SessionCatalog(InMemoryCatalog(), FunctionRegistry.builtin, TableFunctionRegistry.builtin))

  // implements TableCatalog & SupportsNamespaces
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] =
    views.keys.filter(_.namespace().sameElements(namespace)).toArray

  override def loadTable(ident: Identifier): Table = {
    views
      .get(ident)
      .map(schema => new CompiletimeTable(ident, schema))
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
