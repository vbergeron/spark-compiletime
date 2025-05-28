package spark.compiletime
import scala.quoted.*
import mirrors.TableMirror
import mirrors.CatalogMirror

transparent inline def table(inline sql: String): TableMirror =
  ${ macros.createTableMirrorImpl('sql) }

object catalog:
  transparent inline def empty: CatalogMirror =
    ${ macros.createCatalogMirrorImpl[EmptyTuple] }

  transparent inline def apply(table: TableMirror): CatalogMirror =
    ${ macros.createCatalogMirrorImpl[table.type *: EmptyTuple] }

  transparent inline def apply[tables <: Tuple](tables: tables): CatalogMirror =
    ${ macros.createCatalogMirrorImpl[tables] }

extension [T <: CatalogMirror](db: T)
  inline def sql(inline sql: String): String =
    ${ macros.checkSQLImpl[T]('sql) }
