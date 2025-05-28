package spark.compiletime.utils

import scala.quoted.*
import scala.reflect.ClassTag

def typesFromTuple[Ts: Type](using Quotes): List[Type[?]] =
  Type.of[Ts] match
    case '[t *: ts]    => Type.of[t] :: typesFromTuple[ts]
    case '[EmptyTuple] => Nil

def tupleFromTypes(types: List[Type[?]])(using Quotes): Type[?] =
  import quotes.reflect.*
  types match
    case Nil             => TypeRepr.of[EmptyTuple].asType
    case '[head] :: tail =>
      tupleFromTypes(tail) match
        case '[tail] => TypeRepr.of[head *: (tail & Tuple)].asType

    case _ => report.errorAndAbort(s"expected a list of types")

def stringsFromTuple[Ts: Type](using Quotes): List[String] =
  typesFromTuple[Ts].map:
    case '[t] => stringFromType[t]

def stringFromType[T: Type](using Quotes): String =
  import quotes.reflect.*
  TypeRepr.of[T] match
    case ConstantType(StringConstant(label)) => label
    case _                                   =>
      report.errorAndAbort(s"expected a constant string, got ${TypeRepr.of[T]}")

def typeFromString(name: String)(using Quotes): Type[?] =
  import quotes.reflect.*
  ConstantType(StringConstant(name)).asType

def classOf[A](using Quotes, Type[A]): Expr[Class[A]] =
  import quotes.reflect.*
  Literal(ClassOfConstant(TypeRepr.of[A])).asExprOf[Class[A]]

def classTagOf[A](using Quotes, Type[A]): Expr[ClassTag[A]] =
  import quotes.reflect.*
  '{ ClassTag(${ classOf[A] }) }

def parentTypeOf[A](using Quotes, Type[A]): Type[?] =
  import quotes.reflect.*
  TypeRepr.of[A] match
    case TypeRef(parent, _) => parent.asType
    case _                  => report.errorAndAbort(s"expected a type reference, got ${TypeRepr.of[A]}")

def subtypeOf[A, B](using Quotes, Type[A], Type[B]): Boolean =
  import quotes.reflect.*
  TypeRepr.of[A] <:< TypeRepr.of[B]

def hasAnnotation[A, T](using Quotes, Type[A], Type[T]): Boolean =
  import quotes.reflect.*
  TypeRepr.of[A].classSymbol.get.annotations.exists(_.tpe =:= TypeRepr.of[T])

def varargsOf(argsExpr: Expr[Seq[?]])(using Quotes): List[Expr[?]] =
  import quotes.reflect.*
  argsExpr match
    case Varargs(args) => args.toList
    case _             => report.errorAndAbort("Expected a varargs expression")
