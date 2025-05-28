package spark.compiletime.mirrors

trait CatalogMirror:
  type Tables <: Tuple

object CatalogMirror:

  type Empty = CatalogMirror { type Tables = EmptyTuple }

  inline def tables[T <: CatalogMirror]: List[(String, String, String)] =
    ${ macros.tablesImpl[T] }
