package spark.compiletime
import scala.quoted.*
import mirrors.TableMirror
import mirrors.CatalogMirror

transparent inline def table(inline sql: String): TableMirror =
  ${ macros.createTable[CatalogMirror.Empty]('sql) }

object catalog:
  transparent inline def empty: CatalogMirror =
    ${ macros.createCatalog[EmptyTuple] }

  transparent inline def apply(table: TableMirror): CatalogMirror =
    ${ macros.createCatalog[table.type *: EmptyTuple] }

  transparent inline def apply[tables <: Tuple](tables: tables): CatalogMirror =
    ${ macros.createCatalog[tables] }

extension (db: CatalogMirror)
  inline def sql(inline sql: String): String =
    ${ macros.checkSQL[db.type]('sql) }

  transparent inline def table(inline sql: String): TableMirror =
    ${ macros.createTable[db.type]('sql) }

  transparent inline def add(table: TableMirror): CatalogMirror =
    db.asInstanceOf[CatalogMirror { type Tables = table.type *: db.Tables }]

  transparent inline def add(inline sql: String): CatalogMirror =
    db.add(table(sql))

inline def parseSQL(inline sql: String): Unit =
  ${ macros.parseSQL('sql) }
