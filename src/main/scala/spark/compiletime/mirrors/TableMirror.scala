package spark.compiletime.mirrors

import scala.quoted.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier

trait TableMirror:
  type DB <: String
  type Name <: String
  type Schema <: String

  inline def db: String           = TableMirror.db[this.type]
  inline def name: String         = TableMirror.name[this.type]
  inline def schema: StructType   = TableMirror.schema[this.type]
  inline def schemaString: String = TableMirror.schemaString[this.type]

object TableMirror:

  inline def db[T]: String =
    ${ macros.dbImpl[T] }

  inline def name[T]: String =
    ${ macros.nameImpl[T] }

  inline def schema[T]: StructType =
    ${ macros.schema2Impl[T] }

  inline def schemaString[T]: String =
    ${ macros.schemaImpl[T] }

  inline def table[T]: (String, String, String) =
    ${ macros.tableImpl[T] }
