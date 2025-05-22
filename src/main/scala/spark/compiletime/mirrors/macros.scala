package spark.compiletime
package mirrors
package macros

import scala.quoted.*
import org.apache.spark.sql.types.StructType

def dbImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type DB = db & String }] =>
      TypeRepr.of[db] match
        case ConstantType(StringConstant(db)) => Expr(db)

def nameImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Name = name & String }] =>
      TypeRepr.of[name] match
        case ConstantType(StringConstant(name)) => Expr(name)

def schemaImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Schema = schema & String }] =>
      TypeRepr.of[schema] match
        case ConstantType(StringConstant(schema)) => Expr(schema)

def schema2Impl[T: Type](using Quotes): Expr[StructType] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Schema = schema & String }] =>
      TypeRepr.of[schema] match
        case ConstantType(StringConstant(schema)) =>
          '{ StructType.fromDDL(${ Expr(schema) }) }

def tableImpl[T: Type](using Quotes): Expr[(String, String, String)] =
  Expr.ofTuple(dbImpl[T], nameImpl[T], schemaImpl[T])

def tablesImpl[T <: CatalogMirror](using Quotes, Type[T]): Expr[List[(String, String, String)]] =
  import quotes.reflect.*
  Type.of[T] match
    case '[CatalogMirror { type Tables = tables }] =>
      val data = utils.typesFromTuple[tables].map { case '[t] => tableImpl[t] }
      Expr.ofList(data)
