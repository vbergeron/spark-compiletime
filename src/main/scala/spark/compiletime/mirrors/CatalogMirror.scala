package spark.compiletime.mirrors

trait CatalogMirror:
  type Tables <: Tuple

  inline def tables: List[(String, String, String)] =
    CatalogMirror.tables[this.type]

object CatalogMirror:

  type Empty = CatalogMirror { type Tables = EmptyTuple }

  inline def tables[T <: CatalogMirror]: List[(String, String, String)] =
    ${ macros.tablesImpl[T] }
