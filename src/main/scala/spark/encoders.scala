package spark.encoders

import scala.quoted.*
import scala.deriving.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*
import org.apache.spark.unsafe.types.*
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.OuterScopes

object utils:

  def typesFromTuple[Ts: Type](using Quotes): List[Type[?]] =
    Type.of[Ts] match
      case '[t *: ts]    => Type.of[t] :: typesFromTuple[ts]
      case '[EmptyTuple] => Nil

  def stringsFromTuple[Ts: Type](using Quotes): List[String] =
    typesFromTuple[Ts].map:
      case '[t] => stringFromType[t]

  def stringFromType[T: Type](using Quotes): String =
    import quotes.reflect.*
    TypeRepr.of[T] match
      case ConstantType(StringConstant(label)) => label
      case _                                   =>
        report.errorAndAbort(s"expected a constant string, got ${TypeRepr.of[T]}")

  def classOf[A](using Quotes, Type[A]): Expr[Class[A]] =
    import quotes.reflect.*
    Literal(ClassOfConstant(TypeRepr.of[A])).asExprOf[Class[A]]

  def classTagOf[A](using Quotes, Type[A]): Expr[ClassTag[A]] =
    import quotes.reflect.*
    '{ ClassTag(${ utils.classOf[A] }) }

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

// A reimplementation of the very special derivation in ScalaReflection.encoderFor
private def encoderForImpl[A](using Quotes, Type[A]): Expr[AgnosticEncoder[?]] = {
  import quotes.reflect.*

  val productMirror = Expr.summon[Mirror.ProductOf[A]]

  Type.of[A] match
    case '[Null]              => '{ NullEncoder }
    case '[Boolean]           => '{ PrimitiveBooleanEncoder }
    case '[Byte]              => '{ PrimitiveByteEncoder }
    case '[Short]             => '{ PrimitiveShortEncoder }
    case '[Int]               => '{ PrimitiveIntEncoder }
    case '[Long]              => '{ PrimitiveLongEncoder }
    case '[Float]             => '{ PrimitiveFloatEncoder }
    case '[Double]            => '{ PrimitiveDoubleEncoder }
    case '[java.lang.Boolean] => '{ BoxedBooleanEncoder }
    case '[java.lang.Byte]    => '{ BoxedByteEncoder }
    case '[java.lang.Short]   => '{ BoxedShortEncoder }
    case '[java.lang.Integer] => '{ BoxedIntEncoder }
    case '[java.lang.Long]    => '{ BoxedLongEncoder }
    case '[java.lang.Float]   => '{ BoxedFloatEncoder }
    case '[java.lang.Double]  => '{ BoxedDoubleEncoder }
    case '[Array[Byte]]       => '{ BinaryEncoder }

    // Enums
    case '[t] if utils.subtypeOf[t, java.lang.Enum[?]] =>
      '{ JavaEnumEncoder(ClassTag(${ utils.classOf[A] })) }

    case '[t] if utils.subtypeOf[t, Enumeration#Value] =>
      utils.parentTypeOf[t] match
        case '[parent] =>
          '{ ScalaEnumEncoder(${ utils.classOf[parent] }, ${ utils.classTagOf[t] }) }

    // Leaf encoders
    case '[String]                  => '{ StringEncoder }
    case '[Decimal]                 => '{ DEFAULT_SPARK_DECIMAL_ENCODER }
    case '[BigDecimal]              => '{ DEFAULT_SCALA_DECIMAL_ENCODER }
    case '[java.math.BigDecimal]    => '{ DEFAULT_JAVA_DECIMAL_ENCODER }
    case '[BigInt]                  => '{ ScalaBigIntEncoder }
    case '[java.math.BigInteger]    => '{ JavaBigIntEncoder }
    case '[CalendarInterval]        => '{ CalendarIntervalEncoder }
    case '[java.time.Duration]      => '{ DayTimeIntervalEncoder }
    case '[java.time.Period]        => '{ YearMonthIntervalEncoder }
    case '[java.sql.Date]           => '{ STRICT_DATE_ENCODER }
    case '[java.time.LocalDate]     => '{ STRICT_LOCAL_DATE_ENCODER }
    case '[java.sql.Timestamp]      => '{ STRICT_TIMESTAMP_ENCODER }
    case '[java.time.Instant]       => '{ STRICT_INSTANT_ENCODER }
    case '[java.time.LocalDateTime] => '{ LocalDateTimeEncoder }
    case '[Row]                     => '{ UnboundRowEncoder }
    //
    //      // UDT encoders
    //      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
    //        val udt = getClassFromType(t).getAnnotation(classOf[SQLUserDefinedType]).udt().
    //          getConstructor().newInstance().asInstanceOf[UserDefinedType[Any]]
    //        val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
    //        UDTEncoder(udt, udtClass)
    //
    // case '[t] if UDTRegistration.exists(TypeRepr.of[t].classSymbol.get.fullName) =>
    //  val name = TypeRepr.of[t].classSymbol.get.fullName
    //  '{
    //    val udtClass = UDTRegistration.getUDTFor(${ Expr(name) }).get.asInstanceOf[Class[UserDefinedType[?]]]
    //    val udt      = udtClass.getConstructor().newInstance().asInstanceOf[UserDefinedType[Any]]
    //    UDTEncoder(udt, udtClass)

    //  }
    //          newInstance().asInstanceOf[UserDefinedType[Any]]
    //        UDTEncoder(udt, udt.getClass)
    //
    // Complex encoders
    case '[Option[t]]               =>
      '{
        val encoder = ${ encoderForImpl[t] }
        OptionEncoder(encoder)
      }

    case '[Array[t]] =>
      '{
        val encoder = ${ encoderForImpl[t] }
        ArrayEncoder(encoder, encoder.nullable)
      }

    case '[t] if utils.subtypeOf[t, Seq[?]] =>
      TypeRepr.of[t].typeArgs.head.asType match
        case '[e] =>
          val companion = TypeRepr.of[t].typeSymbol.companionModule
          if companion.methodMember("newBuilder").nonEmpty
          then
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[A] }, encoder, encoder.nullable, false)
            }
          else
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[Seq[?]] }, encoder, encoder.nullable, false)
            }

    case '[t] if utils.subtypeOf[t, Set[?]] =>
      TypeRepr.of[t].typeArgs.head.asType match
        case '[e] =>
          val companion = TypeRepr.of[t].typeSymbol.companionModule
          if companion.methodMember("newBuilder").nonEmpty
          then
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[A] }, encoder, encoder.nullable, false)
            }
          else
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[Set[?]] }, encoder, encoder.nullable, false)
            }

    case '[Map[k, v]] =>
      '{
        val key   = ${ encoderForImpl[k] }
        val value = ${ encoderForImpl[v] }
        MapEncoder(${ utils.classTagOf[A] }, key, value, value.nullable)
      }

    case '[t] if productMirror.isDefined =>
      productMirror.get match
        case '{ $m: Mirror.ProductOf[A] { type MirroredElemLabels = labels; type MirroredElemTypes = types } } =>
          val labels = utils.stringsFromTuple[labels]
          val types  = utils.typesFromTuple[types]

          val tpe = Type.of[A]

          if types.contains(tpe)
          then report.errorAndAbort(s"circular dependency: $tpe in $types")

          val params = (labels zip types).map:
            case label -> '[t] =>
              '{
                val encoder = ${ encoderForImpl[t] }
                EncoderField(${ Expr(label) }, encoder, encoder.nullable, Metadata.empty)
              }
            case unreachable   =>
              report.errorAndAbort(s"Unexpected type ${Type.show[A]}")
          '{
            ProductEncoder(
              ${ utils.classTagOf[A] },
              ${ Expr.ofList(params) },
              Option(OuterScopes.getOuterScope(${ utils.classOf[A] }))
            )
          }

    case unexpected =>
      report.errorAndAbort(s"Unexpected type ${Type.show[A]}")
}

private inline def agnosticEncoderOf[A]: AgnosticEncoder[?] =
  ${ encoderForImpl[A] }

inline def encoderOf[A] =
  ExpressionEncoder[A](agnosticEncoderOf[A].asInstanceOf)
