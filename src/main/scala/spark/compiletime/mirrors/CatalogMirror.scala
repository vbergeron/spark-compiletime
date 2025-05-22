package spark.compiletime.mirrors

trait CatalogMirror:
  type Tables <: Tuple

object CatalogMirror:

  inline def tables[T <: CatalogMirror]: List[(String, String, String)] =
    ${ macros.tablesImpl[T] }
