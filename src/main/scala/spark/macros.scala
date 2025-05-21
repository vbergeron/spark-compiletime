package spark.macros
import scala.quoted.*
import org.apache.spark.sql.compiletime.TableMirror
import org.apache.spark.sql.compiletime.DatabaseMirror

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

def tableImpl[T: Type](using Quotes): Expr[(String, String, String)] =
  Expr.ofTuple(dbImpl[T], nameImpl[T], schemaImpl[T])

def tablesImpl[T <: DatabaseMirror](using Quotes, Type[T]): Expr[List[(String, String, String)]] =
  import quotes.reflect.*
  Type.of[T] match
    case '[DatabaseMirror { type Tables = tables }] =>
      val data = spark.utils.typesFromTuple[tables].map { case '[t] => tableImpl[t] }
      Expr.ofList(data)
