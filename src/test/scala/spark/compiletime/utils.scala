package spark.compiletime

import scala.quoted.*

def typeNameImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[t] =>
      val name = Type.show[t]
      Expr(name)
    case _    =>
      report.errorAndAbort("Not a type")

inline def typeName[T]: String =
  ${ typeNameImpl[T] }
