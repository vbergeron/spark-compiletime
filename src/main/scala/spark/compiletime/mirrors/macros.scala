package spark.compiletime
package mirrors
package macros

import scala.quoted.*
import spark.compiletime.utils
import org.apache.spark.sql.types.StructType

def dbImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type DB = db & String }] =>
      Expr(utils.stringFromType[db])

def nameImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Name = name & String }] =>
      Expr(utils.stringFromType[name])

def schemaImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Schema = schema & String }] =>
      Expr(utils.stringFromType[schema])

def schema2Impl[T: Type](using Quotes): Expr[StructType] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Schema = schema & String }] =>
      val schema = utils.stringFromType[schema]
      '{ StructType.fromDDL(${ Expr(schema) }) }

def queryImpl[T: Type](using Quotes): Expr[String] =
  import quotes.reflect.*
  Type.of[T] match
    case '[TableMirror { type Query = query & String }] =>
      Expr(utils.stringFromType[query])

def tableImpl[T: Type](using Quotes): Expr[(String, String, String)] =
  Expr.ofTuple(dbImpl[T], nameImpl[T], schemaImpl[T])

def tablesImpl[T <: CatalogMirror](using Quotes, Type[T]): Expr[List[(String, String, String)]] =
  import quotes.reflect.*
  Type.of[T] match
    case '[CatalogMirror { type Tables = tables }] =>
      val data = utils.typesFromTuple[tables].map { case '[t] => tableImpl[t] }
      Expr.ofList(data)
